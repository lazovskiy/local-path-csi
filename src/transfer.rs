//! How the data of a volume travels from one node to another.
//!
//! The node that receives runs `rsync`, the node that holds the data runs `rsync --server
//! --sender`, and the two talk through a TLS connection between the agents instead of a remote
//! shell:
//!
//! ```text
//! receiving node                                         node with the data
//! rsync -e "local-path-csi rsh" ── unix socket ── agent ══ TLS ══ agent ── rsync --server --sender
//! ```
//!
//! `local-path-csi rsh` is the "remote shell" rsync starts: it only joins its standard streams to a
//! socket of the agent on the same node. The keys never leave the agents.
//!
//! Between the agents the data of the volume is compressed, when the node that holds it is set
//! to and the receiving one can unpack it: see [`crate::compression`].
//!
//! The serving side takes nothing on trust: who asks is settled by the certificate, what is
//! given out by the [`Gate`], and the path of the request is replaced by the one the gate names.

use std::ffi::OsString;
use std::fmt;
use std::io;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rustix::fs::OFlags;
use rustix::process::{Pid, Signal};
use rustls::pki_types::ServerName;
use rustls::{ClientConfig, ServerConfig};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::process::Command;
use tokio_rustls::{TlsAcceptor, TlsConnector};

use crate::compression::{self, Traffic, ZSTD};
use crate::tls::AGENT_NAME;

/// Environment variable that tells `local-path-csi rsh` the socket of its agent.
pub const SOCKET_ENV: &str = "LOCAL_PATH_CSI_RSH_SOCKET";

/// A message before the stream of rsync: nothing in it is large.
const FRAME_LIMIT: u32 = 64 << 10;

/// How long one side waits for the other to say who it is and what it wants.
const HANDSHAKE: Duration = Duration::from_secs(20);

/// Seconds without a byte after which rsync gives up on its peer: a node that went dark must not
/// hold a copy forever.
const IO_TIMEOUT_SECONDS: u64 = 300;

/// How long a connection between two agents may be silent before the kernel asks the other
/// side whether it is still there, how often it asks again, and how many questions without an
/// answer end the connection. A node that went dark sends no FIN: without the questions the
/// side that only reads would wait for the timeout of rsync, five minutes; with them, a minute
/// and a half.
const KEEPALIVE_IDLE: Duration = Duration::from_secs(60);
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);
const KEEPALIVE_PROBES: u32 = 3;

/// Has the kernel watch over `stream`, a connection to the agent of another node.
fn keep_alive(stream: &TcpStream) -> io::Result<()> {
    let keepalive = socket2::TcpKeepalive::new()
        .with_time(KEEPALIVE_IDLE)
        .with_interval(KEEPALIVE_INTERVAL)
        .with_retries(KEEPALIVE_PROBES);
    socket2::SockRef::from(stream).set_tcp_keepalive(&keepalive)
}

/// How long a sender may go on after the node it sends to has closed the connection: at the end
/// of a copy it has nothing left but to end.
const LAST_WORDS: Duration = Duration::from_secs(5);

/// A copy is complete when a pass over it finds nothing to transfer. More passes than this over
/// a volume that is not in use mean that it is in use.
const PASSES: u32 = 5;

/// rsync exit code "some files vanished before they could be transferred".
const VANISHED: i32 = 24;

/// What the receiving agent asks of the agent that holds the data.
#[derive(Serialize, Deserialize, Debug)]
struct Request {
    /// Node that asks.
    node: String,
    volume: String,
    /// The command rsync wants to run on the far side.
    args: Vec<String>,
    /// The ways of compression the receiving agent can unpack.
    #[serde(default)]
    compression: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug)]
struct Reply {
    /// Why the volume is not given out.
    error: Option<String>,
    /// How the stream that follows is compressed: one of the ways the request named, or none.
    #[serde(default)]
    compression: Option<String>,
}

async fn write_frame<W: AsyncWrite + Unpin>(writer: &mut W, message: &impl Serialize) -> io::Result<()> {
    let body = serde_json::to_vec(message)?;
    writer.write_all(&(body.len() as u32).to_be_bytes()).await?;
    writer.write_all(&body).await?;
    writer.flush().await
}

async fn read_frame<R: AsyncRead + Unpin, T: DeserializeOwned>(reader: &mut R) -> io::Result<T> {
    let mut length = [0u8; 4];
    reader.read_exact(&mut length).await?;
    let length = u32::from_be_bytes(length);
    if length > FRAME_LIMIT {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "the message is too large"));
    }
    let mut body = vec![0u8; length as usize];
    reader.read_exact(&mut body).await?;
    Ok(serde_json::from_slice(&body)?)
}

/// The short options a receiver of this driver sends to the far side: what `-aHAXS --whole-file`
/// of [`fetch_options`] come to there. None of them takes a value, and none of them makes the
/// sender read anything but the directory it is given. Not among them, and never to be:
/// `s` (the sender would read its real arguments, the path included, from the connection),
/// `L`, `k` and `K` (it would follow symbolic links out of the volume).
pub const SENDER_FLAGS: &str = "lWHogDtpAXrS";

/// Whether `option` is a cluster of short options a volume is fetched with. `e` takes the rest
/// of the cluster as its value: the capabilities of the receiver, which are not options.
fn known_cluster(option: &str) -> bool {
    let Some(cluster) = option.strip_prefix('-').filter(|cluster| !cluster.is_empty()) else { return false };
    let (flags, capabilities) = match cluster.split_once('e') {
        Some((flags, capabilities)) => (flags, capabilities),
        None => (cluster, ""),
    };
    flags.bytes().all(|flag| SENDER_FLAGS.as_bytes().contains(&flag))
        && capabilities.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'.')
}

/// Why the command a node asks for is not run.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BadCommand {
    #[error("the request is not an rsync sender command")]
    NotASender,
    #[error("rsync option {0:?} is not one a volume is fetched with")]
    Option(String),
}

/// Turns the command a receiving rsync asks for into the one that is run. Only a sender is ever
/// started, only with options a receiver of this driver sends, and the directory is not the
/// one of the request but `directory`.
pub fn sender_command(args: &[String], directory: &Path) -> Result<Vec<OsString>, BadCommand> {
    let [program, server, sender, options @ .., dot, _path] = args else {
        return Err(BadCommand::NotASender);
    };
    if program != "rsync" || server != "--server" || sender != "--sender" || dot != "." {
        return Err(BadCommand::NotASender);
    }
    let number = |text: &str| !text.is_empty() && text.bytes().all(|b| b.is_ascii_digit());
    for option in options {
        let known = match option.strip_prefix("--") {
            Some(long) => {
                long == "numeric-ids"
                    || long.strip_prefix("bwlimit=").is_some_and(number)
                    || long.strip_prefix("timeout=").is_some_and(number)
            }
            None => known_cluster(option),
        };
        if !known {
            return Err(BadCommand::Option(option.clone()));
        }
    }
    let mut command: Vec<OsString> = vec!["rsync".into(), "--server".into(), "--sender".into()];
    command.extend(options.iter().map(OsString::from));
    command.push(".".into());
    // The trailing slash: the contents of the directory, not the directory.
    let mut source = directory.as_os_str().to_os_string();
    source.push("/");
    command.push(source);
    Ok(command)
}

/// Decides who may fetch what.
pub trait Gate: Send + Sync + 'static {
    /// Why a volume is not given out. It is told, as a text, to the node that asked.
    type Refusal: fmt::Display + Send;

    /// `node` presented `certificate` (DER) and asks for `volume`. Returns the directory to give
    /// out, or the reason for the refusal.
    fn admit(
        &self,
        node: &str,
        certificate: &[u8],
        volume: &str,
    ) -> impl Future<Output = Result<PathBuf, Self::Refusal>> + Send;
}

/// Serves volumes to the agents of other nodes, until the process is stopped. `zstd_level` is
/// the level the data is compressed with on its way; `None` sends it as it is.
pub async fn serve<G: Gate>(
    listener: TcpListener,
    tls: Arc<ServerConfig>,
    gate: Arc<G>,
    rsync: PathBuf,
    zstd_level: Option<i32>,
) {
    let acceptor = TlsAcceptor::from(tls);
    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(e) => {
                tracing::warn!(error = %e, "cannot accept a connection");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };
        // Without it the transfer still works, and ends on the timeout of rsync as before.
        if let Err(e) = keep_alive(&stream) {
            tracing::warn!(%peer, error = %e, "cannot have the connection watched over");
        }
        let (acceptor, gate, rsync) = (acceptor.clone(), gate.clone(), rsync.clone());
        tokio::spawn(async move {
            if let Err(e) = give_out(stream, acceptor, gate, rsync, zstd_level).await {
                tracing::warn!(%peer, error = %e, "serving a volume failed");
            }
        });
    }
}

async fn give_out<G: Gate>(
    stream: TcpStream,
    acceptor: TlsAcceptor,
    gate: Arc<G>,
    rsync: PathBuf,
    zstd_level: Option<i32>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (request, mut stream) = tokio::time::timeout(HANDSHAKE, async {
        let mut stream = acceptor.accept(stream).await?;
        let request: Request = read_frame(&mut stream).await?;
        io::Result::Ok((request, stream))
    })
    .await
    .map_err(|_| "the peer did not say what it wants")??;
    let certificate = stream.get_ref().1.peer_certificates().and_then(|chain| chain.first()).map(|c| c.to_vec());
    let certificate = certificate.ok_or("the peer presented no certificate")?;

    // A refusal crosses the wire as a text, whatever it was here.
    let admitted = match gate.admit(&request.node, &certificate, &request.volume).await {
        Ok(directory) => sender_command(&request.args, &directory).map_err(|bad| bad.to_string()),
        Err(refusal) => Err(refusal.to_string()),
    };
    let started = admitted.and_then(|command| {
        Command::new(&rsync)
            .args(&command[1..])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| format!("cannot start {}: {e}", rsync.display()))
    });
    let mut child = match started {
        Ok(child) => child,
        Err(refusal) => {
            tracing::warn!(node = %request.node, volume = %request.volume, %refusal, "volume not given out");
            write_frame(&mut stream, &Reply { error: Some(refusal), compression: None }).await?;
            stream.shutdown().await?;
            return Ok(());
        }
    };
    // An agent that does not name zstd is one that cannot unpack it.
    let zstd_level = zstd_level.filter(|_| request.compression.iter().any(|way| way == ZSTD));
    write_frame(&mut stream, &Reply { error: None, compression: zstd_level.map(|_| ZSTD.to_string()) }).await?;
    tracing::info!(node = %request.node, volume = %request.volume, ?zstd_level, "giving out the volume");

    let (mut stdin, mut stdout) = (pipe(child.stdin.take())?, pipe(child.stdout.take())?);
    let complaints = tokio::spawn(tail(pipe(child.stderr.take())?));
    let (mut from_peer, mut to_peer) = tokio::io::split(stream);
    // Ends when the peer closes its side, or with the sender.
    let mut upstream = tokio::spawn(async move {
        let _ = tokio::io::copy(&mut from_peer, &mut stdin).await;
    });
    let traffic = Traffic::default();
    let sent = {
        let passing = async {
            match zstd_level {
                Some(level) => compression::pack(&mut stdout, &mut to_peer, level, &traffic).await,
                None => compression::relay(&mut stdout, &mut to_peer, &traffic).await,
            }
        };
        tokio::pin!(passing);
        tokio::select! {
            sent = &mut passing => sent,
            // The peer has closed its side. At the end of a copy the sender ends at the last
            // word of the receiver, before that happens or right after; a sender that goes on
            // sends to nobody. Writing to the peer may not even fail: a pod that is gone
            // neither takes what it is sent nor refuses it.
            _ = &mut upstream => match tokio::time::timeout(LAST_WORDS, &mut passing).await {
                Ok(sent) => sent,
                Err(_) => Err(io::Error::other("the other node closed the connection in the middle of the volume")),
            },
        }
    };
    match sent {
        Ok(()) => {
            let _ = to_peer.shutdown().await;
        }
        // Nobody takes what the sender writes. Left alone it would wait for room in a pipe that
        // is not read any more, until its timeout, with the files of the volume open: one that
        // is replaced or removed meanwhile would stay on the disk, charged to the volume, for
        // as long.
        Err(_) => {
            let _ = child.start_kill();
        }
    }
    let status = child.wait().await?;
    upstream.abort();
    let complaints = complaints.await.unwrap_or_default();
    let (uncompressed, compressed) = traffic.totals();
    let (node, volume) = (&request.node, &request.volume);
    match (status.success(), sent) {
        (true, Ok(())) => tracing::info!(%node, %volume, uncompressed, compressed, "the volume was read to the end"),
        (true, Err(error)) => tracing::warn!(%node, %volume, %error, "the stream of the volume broke"),
        (false, _) => tracing::warn!(%node, %volume, %status, %complaints, "the sender failed"),
    }
    Ok(())
}

/// The last lines of what a process wrote to its standard error, read to its end. A line that
/// is no UTF-8 - a complaint about a file names the file as it is - is kept with what cannot be
/// shown replaced, and does not end the reading.
async fn tail(stream: impl AsyncRead + Unpin) -> String {
    const KEEP: usize = 5;
    let mut stream = BufReader::new(stream);
    let mut last: Vec<String> = Vec::new();
    let mut line = Vec::new();
    while let Ok(1..) = stream.read_until(b'\n', &mut line).await {
        if last.len() == KEEP {
            last.remove(0);
        }
        last.push(String::from_utf8_lossy(&line).trim_end_matches(['\n', '\r']).to_string());
        line.clear();
    }
    last.join("; ")
}

/// Where a volume is fetched from.
#[derive(Clone)]
pub struct Source {
    /// `host:port` of the agent that holds the data.
    pub address: String,
    /// TLS that accepts only that agent.
    pub tls: Arc<ClientConfig>,
}

/// What is fetched and how.
#[derive(Clone, Debug)]
pub struct Pull {
    /// The rsync binary.
    pub rsync: PathBuf,
    /// This binary: rsync starts `<rsh> rsh` as its remote shell.
    pub rsh: PathBuf,
    /// The node that fetches.
    pub node: String,
    pub volume: String,
    /// The directory the volume is copied into.
    pub destination: PathBuf,
    /// KiB per second the copy may take of the network; zero is no limit.
    pub bwlimit_kib: u64,
    /// Counts what arrives, while it arrives.
    pub traffic: Arc<Traffic>,
}

/// An end of a child that was asked for when the child was started.
fn pipe<T>(end: Option<T>) -> io::Result<T> {
    end.ok_or_else(|| io::Error::other("the process was started without the pipe"))
}

fn random_name() -> io::Result<String> {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes).map_err(io::Error::other)?;
    Ok(bytes.iter().map(|b| format!("{b:02x}")).collect())
}

/// A socket in the abstract namespace: it needs no file, and only the containers of this pod
/// share the namespace.
fn listen(name: &str) -> io::Result<tokio::net::UnixListener> {
    use std::os::linux::net::SocketAddrExt;
    let address = std::os::unix::net::SocketAddr::from_abstract_name(name)?;
    let listener = std::os::unix::net::UnixListener::bind_addr(&address)?;
    listener.set_nonblocking(true)?;
    tokio::net::UnixListener::from_std(listener)
}

/// Why a volume could not be fetched.
#[derive(Debug, thiserror::Error)]
pub enum PullError {
    #[error("cannot name a socket: {0}")]
    SocketName(#[source] io::Error),
    #[error("cannot open a socket for rsync: {0}")]
    Socket(#[source] io::Error),
    #[error("cannot start {}: {source}", .rsync.display())]
    Start { rsync: PathBuf, source: io::Error },
    #[error("rsync: {0}")]
    Rsync(#[source] io::Error),
    #[error("rsync: {status}: {complaints}")]
    Failed { status: std::process::ExitStatus, complaints: String },
    #[error("the data keeps changing on the node it is copied from: {0} passes each found something new")]
    KeepsChanging(u32),
    #[error("rsync did not start its transport: {0}")]
    NoTransport(#[source] io::Error),
    #[error("transport of rsync: {0}")]
    Transport(#[source] io::Error),
    #[error("cannot connect to {address}: {source}")]
    Connect { address: String, source: io::Error },
    #[error("{address} is not the agent its LocalPathNode describes: {source}")]
    Stranger { address: String, source: io::Error },
    #[error("{address}: {source}")]
    Exchange { address: String, source: io::Error },
    #[error("{0} does not answer")]
    Silent(String),
    #[error("{address} compresses with {way}, which this agent did not offer")]
    Compression { address: String, way: String },
    /// What the agent that holds the volume had against giving it out, in its words.
    #[error("{0}")]
    Refused(String),
}

/// Joins the rsync of this node to the agent that holds the data. `Err` is what that agent, or
/// the network, had against it.
async fn bridge(
    listener: tokio::net::UnixListener,
    source: Source,
    node: String,
    volume: String,
    traffic: Arc<Traffic>,
) -> Result<(), PullError> {
    let (mut local, _) = listener.accept().await.map_err(PullError::NoTransport)?;
    drop(listener);
    let hello = async {
        let args: Vec<String> = read_frame(&mut local).await.map_err(PullError::Transport)?;
        let address = &source.address;
        let at = || address.clone();
        let stream =
            TcpStream::connect(address).await.map_err(|source| PullError::Connect { address: at(), source })?;
        if let Err(e) = keep_alive(&stream) {
            tracing::warn!(%address, error = %e, "cannot have the connection watched over");
        }
        let name = ServerName::try_from(AGENT_NAME)
            .map_err(|e| PullError::Stranger { address: at(), source: io::Error::other(e) })?;
        let mut stream = TlsConnector::from(source.tls.clone())
            .connect(name, stream)
            .await
            .map_err(|source| PullError::Stranger { address: at(), source })?;
        let request = Request { node, volume: volume.clone(), args, compression: vec![ZSTD.to_string()] };
        let exchange = |source| PullError::Exchange { address: at(), source };
        write_frame(&mut stream, &request).await.map_err(exchange)?;
        let reply: Reply = read_frame(&mut stream).await.map_err(exchange)?;
        if let Some(refusal) = reply.error {
            return Err(PullError::Refused(refusal));
        }
        match reply.compression.as_deref() {
            None => Ok((stream, false)),
            Some(ZSTD) => Ok((stream, true)),
            Some(other) => Err(PullError::Compression { address: at(), way: other.to_string() }),
        }
    };
    let (remote, packed) =
        tokio::time::timeout(HANDSHAKE, hello).await.map_err(|_| PullError::Silent(source.address.clone()))??;
    let (mut from_rsync, mut to_rsync) = local.into_split();
    let (mut from_holder, mut to_holder) = tokio::io::split(remote);
    let requests = async {
        let copied = tokio::io::copy(&mut from_rsync, &mut to_holder).await;
        let _ = to_holder.shutdown().await;
        copied.map(|_| ())
    };
    let data = async {
        let received = match packed {
            true => compression::unpack(&mut from_holder, &mut to_rsync, &traffic).await,
            false => compression::relay(&mut from_holder, &mut to_rsync, &traffic).await,
        };
        let _ = to_rsync.shutdown().await;
        received
    };
    // rsync notices a stream that broke and says so itself; this is why it broke.
    if let Err(error) = tokio::try_join!(requests, data) {
        tracing::warn!(%volume, source = %source.address, %error, "the stream of the volume broke");
    }
    Ok(())
}

/// The options the receiving rsync is run with. What it asks of the far side for them is what
/// [`sender_command`] lets through, and nothing else.
pub fn fetch_options(bwlimit_kib: u64) -> Vec<String> {
    // Everything of the files: owners by number, hard links, ACLs, extended attributes, holes.
    // Whole files: on a local network the delta algorithm only costs CPU.
    let mut options: Vec<String> =
        ["-aHAXS", "--numeric-ids", "--delete", "--whole-file", "--itemize-changes"].map(String::from).into();
    options.push(format!("--timeout={IO_TIMEOUT_SECONDS}"));
    if bwlimit_kib > 0 {
        options.push(format!("--bwlimit={bwlimit_kib}"));
    }
    options
}

/// Reads `output` to its end and returns how many lines it had. Bytes are counted, not text: a
/// line that is no UTF-8 - rsync prints the names of files as they are - is a line all the same.
/// Taken for the end of the output, it would leave the rest unread and uncounted: rsync would
/// stop at a full pipe until its timeout, or a pass that had brought files in line would count
/// as one that found nothing to do.
async fn count_lines(mut output: impl AsyncRead + Unpin) -> u64 {
    let mut buffer = vec![0u8; 64 << 10];
    let (mut lines, mut last) = (0u64, b'\n');
    loop {
        match output.read(&mut buffer).await {
            Ok(0) | Err(_) => break,
            Ok(read) => {
                lines += buffer[..read].iter().filter(|byte| **byte == b'\n').count() as u64;
                last = buffer[read - 1];
            }
        }
    }
    // A last line that was not ended is a line too.
    lines + u64::from(last != b'\n')
}

/// The processes of one run of rsync: rsync itself, the receiver it splits off, the remote shell
/// it starts. Dropped while they run - the copy is given up - it kills them all. Killing rsync
/// alone would leave the receiver writing into the directory of the copy until the node with
/// the data ended the connection, with the directory being removed under it.
struct Group(Option<Pid>);

impl Drop for Group {
    fn drop(&mut self) {
        if let Some(leader) = self.0 {
            // The leader has not been waited for: the number is not given to anything else yet.
            let _ = rustix::process::kill_process_group(leader, Signal::KILL);
        }
    }
}

/// One run of rsync over the volume; returns how many entries it had to bring in line.
async fn pass(pull: &Pull, source: &Source) -> Result<u64, PullError> {
    let socket = random_name().map_err(PullError::SocketName)?;
    let listener = listen(&socket).map_err(PullError::Socket)?;
    let mut remote_shell = pull.rsh.as_os_str().to_os_string();
    remote_shell.push(" rsh");
    let mut destination = pull.destination.as_os_str().to_os_string();
    destination.push("/");
    let mut command = Command::new(&pull.rsync);
    command
        .args(fetch_options(pull.bwlimit_kib))
        .arg("-e")
        .arg(remote_shell)
        // The far side decides what it gives out: host and path are placeholders.
        .arg("source:volume/")
        .arg(destination)
        .env(SOCKET_ENV, &socket)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        // A group of its own: rsync splits off a receiver and starts the remote shell, and a
        // signal to rsync alone leaves those two running.
        .process_group(0)
        .kill_on_drop(true);
    let mut child = command.spawn().map_err(|source| PullError::Start { rsync: pull.rsync.clone(), source })?;
    // After `child`, so that it goes first: the group is killed while its number is still taken.
    let mut group = Group(child.id().and_then(|leader| Pid::from_raw(leader as i32)));

    let refusal: Arc<Mutex<Option<PullError>>> = Arc::default();
    let transport = tokio::spawn({
        let (source, node, volume, refusal) = (source.clone(), pull.node.clone(), pull.volume.clone(), refusal.clone());
        let traffic = pull.traffic.clone();
        async move {
            if let Err(e) = bridge(listener, source, node, volume, traffic).await {
                *refusal.lock().unwrap() = Some(e);
            }
        }
    });
    let complaints = tokio::spawn(tail(pipe(child.stderr.take()).map_err(PullError::Rsync)?));
    // One line per entry that differed: counted, not kept - a volume may hold millions of files.
    let changed = count_lines(pipe(child.stdout.take()).map_err(PullError::Rsync)?).await;
    let status = child.wait().await.map_err(PullError::Rsync)?;
    // An rsync that ended by itself has waited for what it had started.
    group.0 = None;
    transport.abort();
    let _ = transport.await;
    let complaints = complaints.await.unwrap_or_default();
    if let Some(refusal) = refusal.lock().unwrap().take() {
        return Err(refusal);
    }
    match status.code() {
        Some(0) => Ok(changed),
        // Files that vanished while they were being copied: the volume is being written to.
        Some(VANISHED) => Ok(changed.max(1)),
        _ => Err(PullError::Failed { status, complaints }),
    }
}

/// Copies the volume into `pull.destination`, and makes sure that the copy is complete: the
/// last pass over the data must find nothing left to transfer. Returns the number of passes.
///
/// Stops when the future is dropped: the rsync it started is killed.
pub async fn pull(pull: &Pull, source: &Source) -> Result<u32, PullError> {
    for number in 1..=PASSES {
        let changed = pass(pull, source).await?;
        tracing::info!(volume = %pull.volume, pass = number, changed, "pass over the volume");
        if changed == 0 {
            return Ok(number);
        }
    }
    Err(PullError::KeepsChanging(PASSES))
}

/// `local-path-csi rsh <host> <command...>`: what rsync runs as its remote shell. Hands the command
/// to the agent and joins standard input and output to it. Returns the exit code.
pub fn rsh(args: &[String]) -> i32 {
    use std::fs::File;
    use std::io::Write;
    use std::os::fd::AsFd;
    use std::os::linux::net::SocketAddrExt;
    use std::os::unix::net::{SocketAddr, UnixStream};

    let run = || -> io::Result<()> {
        let name = std::env::var(SOCKET_ENV)
            .map_err(|_| io::Error::other(format!("{SOCKET_ENV} is not set: rsh is started by the agent only")))?;
        let mut socket = UnixStream::connect_addr(&SocketAddr::from_abstract_name(name)?)?;
        // The first argument is the host rsync was told, a placeholder.
        let body = serde_json::to_vec(&args.iter().skip(1).collect::<Vec<_>>())?;
        socket.write_all(&(body.len() as u32).to_be_bytes())?;
        socket.write_all(&body)?;
        // The descriptors themselves, by a second number each: the standard output of the
        // library holds bytes back until a line ends, and the stream of rsync has no lines.
        let mut input = File::from(io::stdin().as_fd().try_clone_to_owned()?);
        let mut output = File::from(io::stdout().as_fd().try_clone_to_owned()?);
        // rsync hands its remote shell non-blocking pipes: a write to a full one would fail
        // instead of waiting for rsync to read.
        for descriptor in [&input, &output] {
            let flags = rustix::fs::fcntl_getfl(descriptor)?;
            rustix::fs::fcntl_setfl(descriptor, flags - OFlags::NONBLOCK)?;
        }
        let mut upstream = socket.try_clone()?;
        std::thread::spawn(move || {
            let _ = io::copy(&mut input, &mut upstream);
            let _ = upstream.shutdown(std::net::Shutdown::Write);
        });
        io::copy(&mut socket, &mut output)?;
        output.flush()
    };
    match run() {
        Ok(()) => 0,
        Err(e) => {
            eprintln!("local-path-csi rsh: {e}");
            1
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(text: &str) -> Vec<String> {
        text.split(' ').map(str::to_string).collect()
    }

    #[test]
    fn sender_is_started_with_the_directory_of_the_gate_not_of_the_request() {
        let asked =
            args("rsync --server --sender -lWHogDtpAXrSe.iLsfxCIvu --bwlimit=1000 --timeout=300 --numeric-ids . /etc/");
        let command = sender_command(&asked, Path::new("/mnt/pool/volumes/pv-1/data")).unwrap();
        let expected = args(
            "rsync --server --sender -lWHogDtpAXrSe.iLsfxCIvu --bwlimit=1000 --timeout=300 --numeric-ids . /mnt/pool/volumes/pv-1/data/",
        );
        assert_eq!(command, expected.iter().map(OsString::from).collect::<Vec<_>>());
    }

    #[test]
    fn anything_but_a_plain_sender_is_refused() {
        let directory = Path::new("/mnt/pool/volumes/pv-1/data");
        for asked in [
            // A receiver: it would write into the volume.
            "rsync --server -lWHogDtpAXrSe.iLsfxCIvu --numeric-ids . /x/",
            // Another program.
            "sh --server --sender -c . /x/",
            // Options a fetch never uses.
            "rsync --server --sender -logDtpr --remove-source-files . /x/",
            "rsync --server --sender -logDtpr --files-from=/etc/shadow . /x/",
            "rsync --server --sender -logDtpr --bwlimit=1;reboot . /x/",
            "rsync --server --sender -logDtpr - . /x/",
            // The sender would take its real arguments, path and all, from the connection.
            "rsync --server --sender -s . /x/",
            "rsync --server --sender -se.LsfxCIvu . /x/",
            "rsync --server --sender -logDtprse.iLsfxCIvu . /x/",
            "rsync --server --sender -logDtpre.iLsfxCIvu -s . /x/",
            // It would follow symbolic links out of the volume.
            "rsync --server --sender -L . /x/",
            "rsync --server --sender -logDtprLe.iLsfxCIvu . /x/",
            "rsync --server --sender -logDtprk . /x/",
            "rsync --server --sender -logDtprK . /x/",
            // Any other short option a fetch never uses.
            "rsync --server --sender -logDtprz . /x/",
            "rsync --server --sender -e.iLsfxCIvu;reboot . /x/",
            // No options at all, and not even a command.
            "rsync --server --sender",
            "",
        ] {
            assert!(sender_command(&args(asked), directory).is_err(), "{asked}");
        }
        for flag in ['s', 'L', 'k', 'K'] {
            assert!(!SENDER_FLAGS.contains(flag), "{flag}");
        }
    }

    #[tokio::test]
    async fn lines_are_counted_whatever_bytes_they_are_made_of() {
        assert_eq!(count_lines(&b""[..]).await, 0);
        assert_eq!(count_lines(&b">f+++++++++ a\n>f+++++++++ b\n"[..]).await, 2);
        // A file name in Latin-1 between two in ASCII: not UTF-8, and still one line of three.
        assert_eq!(count_lines(&b">f+++++++++ a\n>f+++++++++ caf\xe9\xff\n>f+++++++++ b\n"[..]).await, 3);
        assert_eq!(count_lines(&b"a\nb"[..]).await, 2, "a last line without its end");
        // More than is read at once.
        let many = b"cd+++++++++ directory/\n".repeat(100_000);
        assert_eq!(count_lines(&many[..]).await, 100_000);
    }

    #[tokio::test]
    async fn complaints_are_read_to_the_end_whatever_bytes_they_are_made_of() {
        assert_eq!(tail(&b""[..]).await, "");
        assert_eq!(tail(&b"one\ntwo\r\nthree"[..]).await, "one; two; three");
        // The name of the file is in Latin-1: the complaint after it must not be lost.
        let complaints = &b"rsync: open \"caf\xe9\" failed: Permission denied (13)\nrsync error: some files could not be transferred (code 23)\n"[..];
        assert_eq!(
            tail(complaints).await,
            "rsync: open \"caf\u{fffd}\" failed: Permission denied (13); rsync error: some files could not be transferred (code 23)"
        );
        let many: Vec<u8> = (1..=9).flat_map(|n| format!("line {n}\n").into_bytes()).collect();
        assert_eq!(tail(&many[..]).await, "line 5; line 6; line 7; line 8; line 9", "the last five");
    }

    #[tokio::test]
    async fn connection_between_two_agents_is_asked_whether_the_other_side_is_there() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let connecting = TcpStream::connect(listener.local_addr().unwrap()).await.unwrap();
        let (accepted, _) = listener.accept().await.unwrap();
        for stream in [&connecting, &accepted] {
            let socket = socket2::SockRef::from(stream);
            assert!(!socket.keepalive().unwrap(), "a socket is not kept alive unless asked");
            keep_alive(stream).unwrap();
            assert!(socket.keepalive().unwrap());
            // A peer that is gone is noticed after 60 + 3 * 10 seconds of silence.
            assert_eq!(socket.tcp_keepalive_time().unwrap(), Duration::from_secs(60));
            assert_eq!(socket.tcp_keepalive_interval().unwrap(), Duration::from_secs(10));
            assert_eq!(socket.tcp_keepalive_retries().unwrap(), 3);
        }
    }

    #[test]
    fn capabilities_of_the_receiver_are_not_options() {
        let directory = Path::new("/mnt/pool/volumes/pv-1/data");
        // After `e` the letters `s` and `L` say what the receiver can do, and ask for nothing.
        for asked in [
            "rsync --server --sender -lWHogDtpAXrSe.iLsfxCIvu . /x/",
            "rsync --server --sender -logDtpre.LsfxC . /x/",
            "rsync --server --sender -e31.14 --numeric-ids . /x/",
        ] {
            assert!(sender_command(&args(asked), directory).is_ok(), "{asked}");
        }
    }
}
