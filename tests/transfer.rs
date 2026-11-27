//! A volume travels between two agents: a real rsync on both ends, TLS in between.
//!
//! Needs the `rsync` binary; `LOCAL_PATH_CSI_RSYNC` names it when it is not on the PATH.

use std::fs;
use std::os::unix::fs::{MetadataExt, PermissionsExt, symlink};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use local_path_csi::tls::{self, Identity};
use local_path_csi::transfer::{Gate, Pull, Source, pull, serve};
use tokio::net::TcpListener;

fn rsync() -> PathBuf {
    std::env::var_os("LOCAL_PATH_CSI_RSYNC").map(PathBuf::from).unwrap_or_else(|| "rsync".into())
}

fn scratch(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!("local-path-csi-transfer-{}-{name}", std::process::id()));
    let _ = fs::remove_dir_all(&path);
    fs::create_dir_all(&path).unwrap();
    path
}

/// Gives `directory` out as volume `pv-1` to node `b`, when it presents `certificate`.
struct OneVolume {
    directory: PathBuf,
    certificate: Vec<u8>,
}

impl Gate for OneVolume {
    type Refusal = String;

    async fn admit(&self, node: &str, certificate: &[u8], volume: &str) -> Result<PathBuf, String> {
        if node != "b" || certificate != self.certificate.as_slice() {
            return Err(format!("node {node} is not the one its LocalPathNode describes"));
        }
        if volume != "pv-1" {
            return Err(format!("volume {volume} is not on its way to node {node}"));
        }
        Ok(self.directory.clone())
    }
}

struct Nodes {
    source: Source,
    /// The certificate of the node with the data.
    holder_certificate: String,
    receiver: Identity,
}

/// Starts the agent of the node with the data, giving out `directory`.
async fn nodes(directory: &Path) -> Nodes {
    nodes_compressing(directory, Some(1)).await
}

/// The same, with the level the node with the data compresses at; `None` is not at all.
async fn nodes_compressing(directory: &Path, zstd_level: Option<i32>) -> Nodes {
    let (holder, receiver) = (Identity::generate().unwrap(), Identity::generate().unwrap());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let gate = Arc::new(OneVolume {
        directory: directory.to_path_buf(),
        certificate: tls::certificate(receiver.certificate_pem()).unwrap().to_vec(),
    });
    // Any certificate passes the handshake: these tests are about what the gate decides.
    let tls = holder.server_config(Arc::new(|_| true)).unwrap();
    tokio::spawn(serve(listener, tls, gate, rsync(), zstd_level));
    let tls = receiver.client_config(holder.certificate_pem()).unwrap();
    Nodes { source: Source { address, tls }, holder_certificate: holder.certificate_pem().to_string(), receiver }
}

fn request(volume: &str, destination: &Path) -> Pull {
    Pull {
        rsync: rsync(),
        rsh: env!("CARGO_BIN_EXE_local-path-csi").into(),
        node: "b".into(),
        volume: volume.into(),
        destination: destination.to_path_buf(),
        bwlimit_kib: 0,
        traffic: Arc::default(),
    }
}

#[tokio::test]
async fn volume_arrives_whole_and_a_second_fetch_brings_only_the_changes() {
    let root = scratch("whole");
    let (from, to) = (root.join("from"), root.join("to"));
    fs::create_dir_all(from.join("dir/deep")).unwrap();
    fs::create_dir(&to).unwrap();
    fs::write(from.join("file"), "payload").unwrap();
    fs::write(from.join("dir/deep/big"), vec![7u8; 3 << 20]).unwrap();
    fs::set_permissions(from.join("file"), fs::Permissions::from_mode(0o640)).unwrap();
    fs::set_permissions(&from, fs::Permissions::from_mode(0o2770)).unwrap();
    symlink("file", from.join("link")).unwrap();
    fs::hard_link(from.join("file"), from.join("dir/same-file")).unwrap();

    let nodes = nodes(&from).await;
    let passes = pull(&request("pv-1", &to), &nodes.source).await.unwrap();
    assert_eq!(passes, 2, "one pass copies, the next one finds nothing left");

    assert_eq!(fs::read_to_string(to.join("file")).unwrap(), "payload");
    assert_eq!(fs::read(to.join("dir/deep/big")).unwrap(), vec![7u8; 3 << 20]);
    assert_eq!(fs::read_link(to.join("link")).unwrap(), Path::new("file"));
    let (file, same) = (fs::metadata(to.join("file")).unwrap(), fs::metadata(to.join("dir/same-file")).unwrap());
    assert_eq!(file.permissions().mode() & 0o7777, 0o640);
    assert_eq!(file.ino(), same.ino(), "hard links stay hard links");
    assert_eq!(file.mtime(), fs::metadata(from.join("file")).unwrap().mtime());
    assert_eq!(fs::metadata(&to).unwrap().permissions().mode() & 0o7777, 0o2770, "the volume directory itself");

    // The volume lived on: a file changed, one appeared, one was removed.
    fs::write(from.join("file"), "changed payload").unwrap();
    fs::write(from.join("new"), "new").unwrap();
    fs::remove_file(from.join("dir/deep/big")).unwrap();
    pull(&request("pv-1", &to), &nodes.source).await.unwrap();
    assert_eq!(fs::read_to_string(to.join("file")).unwrap(), "changed payload");
    assert_eq!(fs::read_to_string(to.join("new")).unwrap(), "new");
    assert!(!to.join("dir/deep/big").exists(), "what is gone from the volume is gone from the copy");

    // A copy that is already complete takes a single pass.
    assert_eq!(pull(&request("pv-1", &to), &nodes.source).await.unwrap(), 1);
    fs::remove_dir_all(&root).unwrap();
}

/// What the rsync of this machine asks of the far side when a volume is fetched.
fn asked_of_the_far_side(root: &Path, bwlimit_kib: u64) -> Vec<String> {
    // A remote shell that only writes down the command it is given, one argument on a line.
    let (shell, asked) = (root.join("shell"), root.join("asked"));
    fs::write(&shell, format!("#!/bin/sh\nshift\nprintf '%s\\n' \"$@\" > '{}'\nexit 1\n", asked.display())).unwrap();
    fs::set_permissions(&shell, fs::Permissions::from_mode(0o755)).unwrap();
    fs::create_dir(root.join("to")).unwrap();
    let run = std::process::Command::new(rsync())
        .args(local_path_csi::transfer::fetch_options(bwlimit_kib))
        .arg("-e")
        .arg(&shell)
        .arg("source:volume/")
        .arg(root.join("to"))
        .output()
        .unwrap();
    assert!(!run.status.success(), "the far side never answered");
    fs::read_to_string(asked).unwrap().lines().map(str::to_string).collect()
}

#[test]
fn receiver_asks_the_far_side_for_nothing_but_what_the_holder_lets_through() {
    use local_path_csi::transfer::{SENDER_FLAGS, sender_command};
    for bwlimit_kib in [0, 1000] {
        let root = scratch(&format!("asked-{bwlimit_kib}"));
        let asked = asked_of_the_far_side(&root, bwlimit_kib);
        let command = sender_command(&asked, Path::new("/pool/volumes/pv-1/data")).unwrap_or_else(|refusal| {
            panic!("the holder would refuse what {} asks for: {asked:?}: {refusal}", rsync().display())
        });
        assert_eq!(command.last().unwrap(), "/pool/volumes/pv-1/data/");
        // The list is not wider than what is asked for: every letter of it is a way in.
        let clusters = asked.iter().filter(|option| option.starts_with('-') && !option.starts_with("--"));
        let flags: String = clusters.map(|cluster| cluster[1..].split('e').next().unwrap()).collect();
        let (mut sent, mut allowed): (Vec<char>, Vec<char>) = (flags.chars().collect(), SENDER_FLAGS.chars().collect());
        sent.sort_unstable();
        allowed.sort_unstable();
        assert_eq!(sent, allowed, "the short options {} sends: {asked:?}", rsync().display());
        fs::remove_dir_all(&root).unwrap();
    }
}

#[tokio::test]
async fn refusal_of_the_holder_is_what_the_receiver_reports() {
    let root = scratch("refused");
    let (from, to) = (root.join("from"), root.join("to"));
    fs::create_dir_all(&from).unwrap();
    fs::create_dir(&to).unwrap();
    fs::write(from.join("file"), "payload").unwrap();

    let nodes = nodes(&from).await;
    let error = pull(&request("pv-2", &to), &nodes.source).await.unwrap_err().to_string();
    assert_eq!(error, "volume pv-2 is not on its way to node b");
    assert_eq!(fs::read_dir(&to).unwrap().count(), 0, "nothing was copied");
    fs::remove_dir_all(&root).unwrap();
}

#[tokio::test]
async fn node_with_another_certificate_gets_nothing() {
    let root = scratch("stranger");
    let (from, to) = (root.join("from"), root.join("to"));
    fs::create_dir_all(&from).unwrap();
    fs::create_dir(&to).unwrap();
    fs::write(from.join("file"), "payload").unwrap();

    let nodes = nodes(&from).await;
    // An agent that says it is node b, with a key of its own.
    let stranger = Identity::generate().unwrap();
    let source = Source {
        address: nodes.source.address.clone(),
        tls: stranger.client_config(&nodes.holder_certificate).unwrap(),
    };
    let error = pull(&request("pv-1", &to), &source).await.unwrap_err().to_string();
    assert_eq!(error, "node b is not the one its LocalPathNode describes");
    assert_eq!(fs::read_dir(&to).unwrap().count(), 0);
    fs::remove_dir_all(&root).unwrap();
}

#[tokio::test]
async fn holder_that_is_not_the_one_expected_is_not_talked_to() {
    let root = scratch("impostor");
    let (from, to) = (root.join("from"), root.join("to"));
    fs::create_dir_all(&from).unwrap();
    fs::create_dir(&to).unwrap();

    let nodes = nodes(&from).await;
    // The receiver was told another certificate for the node with the data.
    let other = Identity::generate().unwrap();
    let source = Source {
        address: nodes.source.address.clone(),
        tls: nodes.receiver.client_config(other.certificate_pem()).unwrap(),
    };
    let error = pull(&request("pv-1", &to), &source).await.unwrap_err().to_string();
    assert!(error.contains("is not the agent its LocalPathNode describes"), "{error}");
    fs::remove_dir_all(&root).unwrap();
}

/// 8 MiB of text that repeats.
fn text() -> Vec<u8> {
    b"the quick brown fox jumps over the lazy dog\n".repeat((8 << 20) / 44)
}

#[tokio::test]
async fn data_that_repeats_crosses_the_network_compressed() {
    let root = scratch("compressed");
    let (from, to) = (root.join("from"), root.join("to"));
    fs::create_dir_all(&from).unwrap();
    fs::create_dir(&to).unwrap();
    fs::write(from.join("text"), text()).unwrap();

    let nodes = nodes_compressing(&from, Some(1)).await;
    let request = request("pv-1", &to);
    pull(&request, &nodes.source).await.unwrap();
    assert!(fs::read(to.join("text")).unwrap() == text(), "the file differs");

    let (uncompressed, compressed) = request.traffic.totals();
    let size = text().len() as u64;
    assert!(uncompressed >= size, "rsync sent the whole file: {uncompressed} of {size}");
    assert!(uncompressed < size + (1 << 20), "and little besides it: {uncompressed} of {size}");
    assert!(compressed > 0 && compressed * 10 < uncompressed, "{compressed} of {uncompressed}");
    fs::remove_dir_all(&root).unwrap();
}

#[tokio::test]
async fn holder_told_not_to_compress_sends_the_data_as_it_is() {
    let root = scratch("plain");
    let (from, to) = (root.join("from"), root.join("to"));
    fs::create_dir_all(&from).unwrap();
    fs::create_dir(&to).unwrap();
    fs::write(from.join("text"), text()).unwrap();

    let nodes = nodes_compressing(&from, None).await;
    let request = request("pv-1", &to);
    pull(&request, &nodes.source).await.unwrap();
    assert!(fs::read(to.join("text")).unwrap() == text(), "the file differs");

    let (uncompressed, compressed) = request.traffic.totals();
    assert!(uncompressed >= text().len() as u64, "rsync sent the whole file: {uncompressed}");
    assert_eq!(compressed, uncompressed, "what crossed the network is what rsync sent");
    fs::remove_dir_all(&root).unwrap();
}

/// Gives `directory` out to whoever asks, and counts how often it was asked.
struct Counting {
    directory: PathBuf,
    asked: std::sync::atomic::AtomicU32,
}

impl Gate for Counting {
    type Refusal = String;

    async fn admit(&self, _node: &str, _certificate: &[u8], _volume: &str) -> Result<PathBuf, String> {
        self.asked.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(self.directory.clone())
    }
}

#[tokio::test]
async fn stranger_is_turned_away_in_the_handshake_before_anything_is_done_for_it() {
    let root = scratch("handshake");
    let (from, to) = (root.join("from"), root.join("to"));
    fs::create_dir_all(&from).unwrap();
    fs::create_dir(&to).unwrap();
    fs::write(from.join("file"), "payload").unwrap();

    let (holder, receiver, stranger) =
        (Identity::generate().unwrap(), Identity::generate().unwrap(), Identity::generate().unwrap());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let gate = Arc::new(Counting { directory: from.clone(), asked: Default::default() });
    // The cluster knows one agent besides the holder.
    let known = tls::certificate(receiver.certificate_pem()).unwrap().to_vec();
    let tls = holder.server_config(Arc::new(move |certificate| certificate == known.as_slice())).unwrap();
    tokio::spawn(serve(listener, tls, gate.clone(), rsync(), Some(1)));
    let asked = || gate.asked.load(std::sync::atomic::Ordering::SeqCst);

    let source = Source { address: address.clone(), tls: stranger.client_config(holder.certificate_pem()).unwrap() };
    let error = pull(&request("pv-1", &to), &source).await.unwrap_err().to_string();
    assert!(error.contains(&address), "the failure is one of the connection: {error}");
    assert_eq!(fs::read_dir(&to).unwrap().count(), 0, "nothing was copied");

    // An agent the cluster knows gets as far as the gate, once for every pass.
    let source = Source { address, tls: receiver.client_config(holder.certificate_pem()).unwrap() };
    let passes = pull(&request("pv-1", &to), &source).await.unwrap();
    assert_eq!(fs::read_to_string(to.join("file")).unwrap(), "payload");
    assert_eq!(asked(), passes, "the gate was asked by the known agent only");
    fs::remove_dir_all(&root).unwrap();
}

/// The senders this process has started for `directory`: `rsync --server --sender` among its children.
fn senders_of(directory: &Path) -> Vec<u32> {
    let mine = std::process::id().to_string();
    let mut found = Vec::new();
    for entry in fs::read_dir("/proc").unwrap().flatten() {
        let Some(pid) = entry.file_name().to_str().and_then(|name| name.parse::<u32>().ok()) else { continue };
        let stat = fs::read_to_string(entry.path().join("stat")).unwrap_or_default();
        // pid (comm) state ppid ...
        let parent = stat.rsplit_once(") ").and_then(|(_, rest)| rest.split(' ').nth(1).map(str::to_string));
        let command = fs::read(entry.path().join("cmdline")).unwrap_or_default();
        let command = String::from_utf8_lossy(&command).replace('\0', " ");
        if parent.as_deref() == Some(mine.as_str())
            && command.contains("--sender")
            && command.contains(directory.to_str().unwrap())
        {
            found.push(pid);
        }
    }
    found
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sender_does_not_outlive_the_node_it_was_sending_to() {
    let root = scratch("gone");
    let (from, to) = (root.join("from"), root.join("to"));
    fs::create_dir(&from).unwrap();
    fs::create_dir(&to).unwrap();
    // Random, so that it does not shrink on its way, and slow enough to be caught in the middle.
    let mut blob = vec![0u8; 48 << 20];
    let mut state = 0x9E37_79B9_7F4A_7C15_u64;
    for chunk in blob.chunks_mut(8) {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        chunk.copy_from_slice(&state.to_le_bytes()[..chunk.len()]);
    }
    fs::write(from.join("blob"), &blob).unwrap();

    // The connection goes through a relay that can play a node whose agent was killed: its
    // side of the connection is closed, and what is still sent to it is neither taken nor
    // refused - the pod is gone, with its address.
    let nodes = nodes(&from).await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let through = listener.local_addr().unwrap().to_string();
    let holder = nodes.source.address.clone();
    let killed = Arc::new(tokio::sync::Notify::new());
    let relay = tokio::spawn({
        let killed = killed.clone();
        async move {
            use tokio::io::AsyncWriteExt;
            let (mut near, _) = listener.accept().await.unwrap();
            let mut far = tokio::net::TcpStream::connect(holder).await.unwrap();
            tokio::select! {
                _ = tokio::io::copy_bidirectional(&mut near, &mut far) => {}
                _ = killed.notified() => {}
            }
            drop(near);
            let _ = far.shutdown().await;
            std::future::pending::<()>().await;
        }
    });
    let slow = Pull { bwlimit_kib: 1024, ..request("pv-1", &to) };
    let traffic = slow.traffic.clone();
    let source = Source { address: through, tls: nodes.source.tls.clone() };
    let copy = tokio::spawn(async move { pull(&slow, &source).await });
    for _ in 0..200 {
        if traffic.totals().0 > 2 << 20 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(traffic.totals().0 > 2 << 20, "the copy never got under way");
    assert_eq!(senders_of(&from).len(), 1, "the node with the data runs a sender for the copy");

    killed.notify_one();
    // The file may be replaced or removed now, and what the sender has open would stay on the
    // disk, charged to the volume, for as long as the sender lives.
    let mut left = senders_of(&from);
    for _ in 0..150 {
        if left.is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        left = senders_of(&from);
    }
    assert!(left.is_empty(), "the node it was sending to is long gone and the sender is still there: {left:?}");
    assert!(copy.await.unwrap().is_err(), "the copy fails on the node that was fetching");
    relay.abort();
    let _ = fs::remove_dir_all(&root);
}

/// The processes of the copy into `directory` that still run: the rsync that fetches, the
/// receiver it splits off - both name the directory - and what they have started, the remote
/// shell. A process that has ended and waits to be collected is not one of them.
fn fetching_into(directory: &Path) -> Vec<u32> {
    let mut running = Vec::new();
    for entry in fs::read_dir("/proc").unwrap().flatten() {
        let Some(pid) = entry.file_name().to_str().and_then(|name| name.parse::<u32>().ok()) else { continue };
        let stat = fs::read_to_string(entry.path().join("stat")).unwrap_or_default();
        // pid (comm) state ppid ...
        let Some((_, rest)) = stat.rsplit_once(") ") else { continue };
        let mut fields = rest.split(' ');
        let (state, parent) = (fields.next().unwrap_or_default(), fields.next().unwrap_or_default());
        let command = fs::read(entry.path().join("cmdline")).unwrap_or_default();
        let command = String::from_utf8_lossy(&command).replace('\0', " ");
        if state != "Z" {
            running.push((pid, parent.parse::<u32>().unwrap_or_default(), command));
        }
    }
    let names = |command: &str| command.contains(directory.to_str().unwrap());
    let fetching: Vec<u32> = running.iter().filter(|(_, _, command)| names(command)).map(|(pid, ..)| *pid).collect();
    let started = running.iter().filter(|(pid, parent, _)| fetching.contains(parent) && !fetching.contains(pid));
    fetching.iter().copied().chain(started.map(|(pid, ..)| *pid)).collect()
}

/// Those of `processes` that still run: neither gone nor ended and waiting to be collected.
fn still_running(processes: &[u32]) -> Vec<u32> {
    let runs = |pid: &&u32| {
        let stat = fs::read_to_string(format!("/proc/{pid}/stat")).unwrap_or_default();
        stat.rsplit_once(") ").is_some_and(|(_, rest)| !rest.starts_with('Z'))
    };
    processes.iter().filter(runs).copied().collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn copy_that_is_given_up_leaves_nothing_running() {
    let root = scratch("given-up");
    let (from, to) = (root.join("from"), root.join("to"));
    fs::create_dir(&from).unwrap();
    fs::create_dir(&to).unwrap();
    // Random, so that it does not shrink on its way, and slow enough to be caught in the middle.
    let mut blob = vec![0u8; 48 << 20];
    let mut state = 0x9E37_79B9_7F4A_7C15_u64;
    for chunk in blob.chunks_mut(8) {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        chunk.copy_from_slice(&state.to_le_bytes()[..chunk.len()]);
    }
    fs::write(from.join("blob"), &blob).unwrap();

    let nodes = nodes(&from).await;
    let slow = Pull { bwlimit_kib: 1024, ..request("pv-1", &to) };
    let traffic = slow.traffic.clone();
    let source = nodes.source.clone();
    let copy = tokio::spawn(async move { pull(&slow, &source).await });
    for _ in 0..200 {
        if traffic.totals().0 > 2 << 20 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(traffic.totals().0 > 2 << 20, "the copy never got under way");
    let processes = fetching_into(&to);
    assert!(processes.len() >= 3, "rsync, its receiver and its remote shell carry the copy: {processes:?}");

    // What the agent does when the volume is not sent to its node any more.
    copy.abort();
    let _ = copy.await;
    // Left alone, the receiver goes on writing until the node with the data gives the copy up:
    // five seconds, in which the directory of the copy cannot be removed for good.
    let given_up = std::time::Instant::now();
    let mut left = still_running(&processes);
    while !left.is_empty() && given_up.elapsed() < std::time::Duration::from_secs(3) {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        left = still_running(&processes);
    }
    assert!(left.is_empty(), "the copy was given up and its processes run on: {left:?}");
    let _ = fs::remove_dir_all(&root);
}
