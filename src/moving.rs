//! The part of the agents in moving a volume to another node.
//!
//! The controller only says where a volume has to be (`spec.node`) and, once a complete copy is
//! there, where it is (`status.node`). The agent of the node a volume was sent to fetches it into
//! `incoming` and reports in `status.transfer`; the agent of the node that holds the data gives
//! it out, to that node only and only while nothing uses the volume.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use futures_util::{FutureExt, StreamExt};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;
use kube::api::{Api, Patch, PatchParams};
use kube::runtime::{WatchStreamExt, reflector, watcher};
use serde_json::json;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;

use crate::agent::pool_status;
use crate::api::{self, LocalPathNode, LocalPathVolume, Mobility, Passport, PoolState, TransferState};
use crate::config::{Config, Pool};
use crate::metrics::{AgentCounters, count};
use crate::tls::{self, Identity};
use crate::transfer::{Gate, Pull, PullError, Source, pull};
use crate::{blocking, pool, unix_now, volumes};

/// How often the receiving node reports how much of a volume has arrived.
const PROGRESS: Duration = Duration::from_secs(10);

/// How long the receiving node waits after a failed attempt.
const RETRY: Duration = Duration::from_secs(15);

/// The node `volume` has to be fetched from, when it is on its way to `node`.
pub fn wanted<'a>(volume: &'a LocalPathVolume, node: &str) -> Option<&'a str> {
    let moving = volume.spec.mobility == Mobility::Floating && volume.metadata.deletion_timestamp.is_none();
    (moving && volume.destination() == Some(node)).then(|| volume.data_node())
}

/// Why a node does not give a volume out. The node that asked is told it as a text, and
/// writes it into the status of the volume.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Refusal {
    #[error("{0:?} is not a volume name")]
    NotAName(String),
    /// The API server could not be asked who may have the volume; `error` is what it said.
    #[error("node {holder}: Kubernetes API: {error}")]
    Api { holder: String, error: String },
    #[error("the certificate presented is not the one of the agent of node {0}")]
    Stranger(String),
    #[error("volume {0} does not exist")]
    NoVolume(String),
    #[error("volume {volume} is not on node {holder}")]
    NotHere { volume: String, holder: String },
    #[error("volume {volume} is not on its way to node {asker}")]
    NotSent { volume: String, asker: String },
    #[error("volume {volume} is still attached to node {node}")]
    Attached { volume: String, node: String },
    #[error("node {holder} has no pool {pool}")]
    NoPool { holder: String, pool: String },
    #[error("mountinfo: {0}")]
    Mountinfo(String),
    #[error("pool {pool} on node {holder} is {state:?}")]
    PoolNotReady { holder: String, pool: String, state: PoolState },
    #[error("volume {volume} has no directory on node {holder}")]
    NoDirectory { volume: String, holder: String },
    #[error("volume {volume} is still mounted on node {holder} at {}", .at.display())]
    Mounted { volume: String, holder: String, at: PathBuf },
}

/// The volume, when the node `holder` may give it out to the node `asker`; `Err` is the refusal.
pub fn may_fetch<'a>(
    volume: Option<&'a LocalPathVolume>,
    name: &str,
    holder: &str,
    asker: &str,
) -> Result<&'a LocalPathVolume, Refusal> {
    let (name, holder) = (name.to_string(), holder.to_string());
    let Some(volume) = volume else { return Err(Refusal::NoVolume(name)) };
    if volume.data_node() != holder {
        return Err(Refusal::NotHere { volume: name, holder });
    }
    if wanted(volume, asker).is_none() {
        return Err(Refusal::NotSent { volume: name, asker: asker.to_string() });
    }
    // A pod that still writes would make the copy differ from the volume.
    if let Some(attached) = volume.status.as_ref().and_then(|s| s.attached_node.as_deref()) {
        return Err(Refusal::Attached { volume: name, node: attached.to_string() });
    }
    Ok(volume)
}

/// Whether `certificate` (DER) is the one the agent of `node` has published.
pub fn presents(node: &LocalPathNode, certificate: &[u8]) -> bool {
    let published = node.status.as_ref().and_then(|s| s.transfer_certificate.as_deref()).and_then(tls::certificate);
    published.is_some_and(|published| published.as_ref() == certificate)
}

/// Follows the LocalPathNodes of the cluster, and tells whether a certificate is the one of some
/// agent: what the serving side asks in the handshake, before it reads or does anything for the
/// peer. Until the LocalPathNodes have been read it knows nobody.
pub fn known_agents(client: kube::Client) -> tls::Admits {
    let (store, writer) = reflector::store::<LocalPathNode>();
    tokio::spawn(async move {
        let api: Api<LocalPathNode> = Api::all(client);
        let stream = reflector(writer, watcher(api, watcher::Config::default()).default_backoff());
        let mut stream = std::pin::pin!(stream);
        while let Some(event) = stream.next().await {
            if let Err(e) = event {
                tracing::warn!(error = %e, "watch of LocalPathNodes failed, retrying");
            }
        }
    });
    Arc::new(move |certificate: &[u8]| store.state().iter().any(|node| presents(node, certificate)))
}

/// The serving side: gives the volumes of this node out to the nodes they are sent to.
pub struct Holder {
    node: String,
    config: Arc<Config>,
    volumes: Api<LocalPathVolume>,
    nodes: Api<LocalPathNode>,
}

impl Holder {
    pub fn new(client: kube::Client, node: String, config: Arc<Config>) -> Self {
        Self { node, config, volumes: Api::all(client.clone()), nodes: Api::all(client) }
    }
}

impl Gate for Holder {
    type Refusal = Refusal;

    async fn admit(&self, node: &str, certificate: &[u8], volume: &str) -> Result<PathBuf, Refusal> {
        if !volumes::valid_name(volume) {
            return Err(Refusal::NotAName(volume.to_string()));
        }
        let api_error = |e: kube::Error| Refusal::Api { holder: self.node.clone(), error: e.to_string() };
        // Read anew: who may have the volume is decided by what the cluster says now.
        let asker = self.nodes.get_opt(node).await.map_err(api_error)?;
        if !asker.is_some_and(|asker| presents(&asker, certificate)) {
            return Err(Refusal::Stranger(node.to_string()));
        }
        let record = self.volumes.get_opt(volume).await.map_err(api_error)?;
        let record = may_fetch(record.as_ref(), volume, &self.node, node)?;

        let (config, holder, pool_name, volume) =
            (self.config.clone(), self.node.clone(), record.spec.pool.clone(), volume.to_string());
        blocking(move || directory_at_rest(&config, &holder, &pool_name, &volume)).await
    }
}

/// The directory of `volume` on this node, `holder`, when it is there and nothing has it
/// mounted; `Err` is the refusal. Everything here waits for a disk.
fn directory_at_rest(config: &Config, holder: &str, pool_name: &str, volume: &str) -> Result<PathBuf, Refusal> {
    let (holder, volume_name) = (holder.to_string(), volume.to_string());
    let Some(pool) = config.pool(pool_name) else {
        return Err(Refusal::NoPool { holder, pool: pool_name.to_string() });
    };
    let mountinfo = std::fs::read_to_string("/proc/self/mountinfo").map_err(|e| Refusal::Mountinfo(e.to_string()))?;
    let state = pool_status(pool, &mountinfo).state;
    if state != PoolState::Ready {
        return Err(Refusal::PoolNotReady { holder, pool: pool_name.to_string(), state });
    }
    if !volumes::exists(pool, volume) {
        return Err(Refusal::NoDirectory { volume: volume_name, holder });
    }
    let directory = volumes::data_dir(pool, volume);
    if let Some(mounted) = pool::binds_of(&mountinfo, &directory).first() {
        return Err(Refusal::Mounted { volume: volume_name, holder, at: mounted.clone() });
    }
    Ok(directory)
}

struct Running {
    source: String,
    uid: Option<String>,
    /// Returns the generation of the volume its complete copy was reported for.
    task: JoinHandle<Option<i64>>,
}

/// A complete copy that waits for the controller.
struct Complete {
    source: String,
    uid: Option<String>,
    /// The generation of the volume the copy was reported for.
    generation: i64,
}

/// What a receiving task needs.
struct Context {
    node: String,
    config: Arc<Config>,
    volumes: Api<LocalPathVolume>,
    nodes: Api<LocalPathNode>,
    identity: Arc<Identity>,
    counters: Arc<AgentCounters>,
    slots: Arc<Semaphore>,
    rsync: PathBuf,
    rsh: PathBuf,
}

/// The receiving side: fetches the volumes that are sent to this node.
pub struct Receiver {
    context: Arc<Context>,
    running: HashMap<String, Running>,
    /// Volumes whose copy is complete and waits for the controller.
    copied: HashMap<String, Complete>,
}

impl Receiver {
    pub fn new(
        client: kube::Client,
        node: String,
        config: Arc<Config>,
        identity: Arc<Identity>,
        counters: Arc<AgentCounters>,
        rsync: PathBuf,
    ) -> std::io::Result<Self> {
        let context = Context {
            slots: Arc::new(Semaphore::new(config.transfer.concurrency)),
            rsh: std::env::current_exe()?,
            volumes: Api::all(client.clone()),
            nodes: Api::all(client),
            node,
            config,
            identity,
            counters,
            rsync,
        };
        Ok(Self { context: Arc::new(context), running: HashMap::new(), copied: HashMap::new() })
    }

    /// Whether the directory of the received copy of `volume` is in use: a copy is being made, or
    /// its task is still winding down.
    pub fn busy(&self, volume: &str) -> bool {
        // A task that has finished touches nothing any more, even before `sync` has noticed.
        self.running.get(volume).is_some_and(|running| !running.task.is_finished())
    }

    /// Starts the copies this node is asked for, and stops the ones nobody waits for any more.
    /// `volumes` is everything the agent knows of.
    pub async fn sync(&mut self, volumes: &[Arc<LocalPathVolume>]) {
        let node = self.context.node.clone();
        let asked: HashMap<&str, (&LocalPathVolume, &str)> = volumes
            .iter()
            .filter_map(|v| Some((v.metadata.name.as_deref()?, (v.as_ref(), wanted(v, &node)?))))
            .collect();
        let same = |name: &str, source: &str, uid: &Option<String>| {
            asked.get(name).is_some_and(|(volume, from)| *from == source && volume.metadata.uid == *uid)
        };
        for (name, running) in &self.running {
            if !running.task.is_finished() && !same(name, &running.source, &running.uid) {
                tracing::info!(volume = %name, "the volume is not sent to this node any more: the copy is stopped");
                // Dropping the task kills its rsync.
                running.task.abort();
            }
        }
        for (name, running) in self.running.extract_if(|_, running| running.task.is_finished()) {
            let reported = running.task.now_or_never().and_then(Result::ok).flatten();
            if let Some(generation) = reported
                && same(&name, &running.source, &running.uid)
            {
                self.copied.insert(name, Complete { source: running.source, uid: running.uid, generation });
            }
        }
        // A volume with a newer generation was called back or resized since: the report about
        // the copy does not count any more, and the copy is gone over once again. An older one
        // is the cache lagging behind what the task has read.
        self.copied.retain(|name, copy| {
            let newer = |(volume, _): &(&LocalPathVolume, &str)| volume.metadata.generation > Some(copy.generation);
            same(name, &copy.source, &copy.uid) && !asked.get(name.as_str()).is_some_and(newer)
        });

        // Which of the volumes asked for have a complete copy here already: read from the disk.
        let idle = asked.iter().filter(|(name, _)| !self.running.contains_key(**name));
        let places: Vec<(String, Pool)> = idle
            .filter_map(|(name, (volume, _))| {
                Some((name.to_string(), self.context.config.pool(&volume.spec.pool)?.clone()))
            })
            .collect();
        let arrived: HashSet<String> = blocking(move || {
            places.into_iter().filter(|(name, pool)| volumes::arrived(pool, name)).map(|(name, _)| name).collect()
        })
        .await;
        for (name, (volume, source)) in asked {
            if self.running.contains_key(name) {
                continue;
            }
            let complete = arrived.contains(name);
            let reported = volume.transfer().is_some_and(|t| t.state == TransferState::Copied);
            if complete && (reported || self.copied.contains_key(name)) {
                continue;
            }
            let fetch = Fetch {
                name: name.to_string(),
                source: source.to_string(),
                uid: volume.metadata.uid.clone(),
                started: api::time(unix_now()),
            };
            let running = Running {
                source: fetch.source.clone(),
                uid: fetch.uid.clone(),
                task: tokio::spawn(receive(self.context.clone(), fetch)),
            };
            self.running.insert(name.to_string(), running);
        }
        self.context.counters.transfers_running.store(self.running.len() as u64, Ordering::Relaxed);
        // The traffic of a volume is shown for as long as the volume is here or on its way here.
        let here: HashSet<&str> = volumes
            .iter()
            .filter(|v| v.data_node() == node || v.destination() == Some(node.as_str()))
            .filter_map(|v| v.metadata.name.as_deref())
            .collect();
        self.context.counters.retain_traffic(|volume| here.contains(volume));
    }
}

/// A volume this node fetches.
struct Fetch {
    name: String,
    /// The node that holds the data.
    source: String,
    /// The volume the copy is made of: another one may get its name.
    uid: Option<String>,
    /// When the node started on the volume.
    started: Time,
}

/// How an attempt that did not fail ended.
enum Outcome {
    /// The volume is not sent here any more.
    Unwanted,
    /// The volume was called back or resized while it was being copied: what was verified is
    /// not the volume as it is now.
    Changed,
    /// The copy is complete, sealed and reported for this generation of the volume.
    Copied { generation: Option<i64>, received_bytes: Option<u64> },
}

/// Fetches the volume, trying again after every failure, until the copy is complete or the task
/// is stopped. Returns the generation of the volume the complete copy was reported for.
async fn receive(context: Arc<Context>, fetch: Fetch) -> Option<i64> {
    let (name, source) = (&fetch.name, &fetch.source);
    let started = Instant::now();
    loop {
        match attempt(&context, &fetch).await {
            Ok(Outcome::Unwanted) => return None,
            Ok(Outcome::Changed) => {
                tracing::info!(volume = %name, %source, "the volume has changed while it was being copied: the copy is gone over again");
            }
            Ok(Outcome::Copied { generation, received_bytes }) => {
                count(&context.counters.transfers_completed);
                context.counters.transfer_seconds.fetch_add(started.elapsed().as_secs(), Ordering::Relaxed);
                context.counters.transfer_bytes.fetch_add(received_bytes.unwrap_or(0), Ordering::Relaxed);
                let (uncompressed, compressed) = context.counters.traffic(name).totals();
                let seconds = started.elapsed().as_secs();
                tracing::info!(volume = %name, %source, ?received_bytes, uncompressed, compressed, seconds, "the copy of the volume is complete");
                return generation;
            }
            Err(Failure { generation, error }) => {
                count(&context.counters.transfers_failed);
                tracing::warn!(volume = %name, %source, %error, "cannot fetch the volume, trying again");
                let message = Some(error.to_string());
                let failed = Progress { state: TransferState::Failed, received_bytes: None, message };
                if let Err(e) = report(&context, &fetch, generation, failed).await {
                    tracing::warn!(volume = %name, error = %e, "cannot report the failure");
                }
                tokio::time::sleep(RETRY).await;
            }
        }
    }
}

struct Progress {
    state: TransferState,
    received_bytes: Option<u64>,
    message: Option<String>,
}

/// Why an attempt to fetch a volume failed: what the node writes into the status of the volume.
#[derive(Debug, thiserror::Error)]
enum CopyError {
    #[error("Kubernetes API: {0}")]
    Api(#[from] kube::Error),
    #[error("node {node} has no pool {pool}")]
    NoPool { node: String, pool: String },
    #[error("mountinfo: {0}")]
    Mountinfo(#[source] std::io::Error),
    #[error("pool {pool} on node {node} is {state:?}")]
    PoolNotReady { node: String, pool: String, state: PoolState },
    #[error("dirMode {0:?} is not an octal mode")]
    DirMode(String),
    #[error("the agent of node {0} has not said where it gives volumes out")]
    NoAddress(String),
    #[error("node {node}: {source}")]
    Identity { node: String, source: tls::Error },
    #[error("cannot prepare the directory for the copy: {0}")]
    Prepare(#[source] std::io::Error),
    #[error("node {node}: {source}")]
    Pull { node: String, source: PullError },
    #[error("cannot flush the copy to the disk: {0}")]
    Seal(#[source] std::io::Error),
    #[error("volume without a name")]
    Unnamed,
    /// A task that works on the disk has panicked, or the agent is stopping.
    #[error("{0}")]
    Task(String),
}

/// Why an attempt failed, and the generation of the volume it was made for.
struct Failure {
    generation: Option<i64>,
    error: Box<CopyError>,
}

async fn report(
    context: &Context,
    fetch: &Fetch,
    generation: Option<i64>,
    progress: Progress,
) -> Result<(), kube::Error> {
    // All the fields, also the empty ones: a merge patch keeps what it does not mention.
    let patch = json!({"status": {"transfer": {
        "source": fetch.source,
        "target": context.node,
        "state": progress.state,
        "observedGeneration": generation,
        "receivedBytes": progress.received_bytes,
        "startedAt": fetch.started,
        "message": progress.message,
    }}});
    let written = context.volumes.patch_status(&fetch.name, &PatchParams::default(), &Patch::Merge(&patch)).await;
    written.map(|_| ())
}

/// The volume as the API server has it, when it is still the one this node is to fetch.
async fn still_wanted(context: &Context, fetch: &Fetch) -> Result<Option<LocalPathVolume>, kube::Error> {
    let volume = context.volumes.get_opt(&fetch.name).await?;
    let wanted = |volume: &LocalPathVolume| {
        volume.metadata.uid == fetch.uid && wanted(volume, &context.node) == Some(fetch.source.as_str())
    };
    Ok(volume.filter(wanted))
}

/// One attempt.
async fn attempt(context: &Context, fetch: &Fetch) -> Result<Outcome, Failure> {
    let unknown = |error: kube::Error| Failure { generation: None, error: Box::new(error.into()) };
    let Some(volume) = still_wanted(context, fetch).await.map_err(unknown)? else { return Ok(Outcome::Unwanted) };
    // Read before the data is gone over: a copy that a pass found complete is a copy of the
    // volume as it was sent here then, and of nothing later.
    let generation = volume.metadata.generation;
    let outcome = copy(context, fetch, &volume).await;
    outcome.map_err(|error| Failure { generation, error: Box::new(error) })
}

async fn copy(context: &Context, fetch: &Fetch, volume: &LocalPathVolume) -> Result<Outcome, CopyError> {
    let (name, source) = (fetch.name.as_str(), fetch.source.as_str());
    let generation = volume.metadata.generation;
    let task = |e: tokio::task::JoinError| CopyError::Task(e.to_string());
    let _slot = match context.slots.clone().try_acquire_owned() {
        Ok(slot) => slot,
        Err(_) => {
            let message = format!("node {} is receiving other volumes", context.node);
            let queued = Progress { state: TransferState::Queued, received_bytes: None, message: Some(message) };
            report(context, fetch, generation, queued).await?;
            context.slots.clone().acquire_owned().await.map_err(|e| CopyError::Task(e.to_string()))?
        }
    };

    let pool_name = &volume.spec.pool;
    let here = || context.node.clone();
    let Some(pool) = context.config.pool(pool_name) else {
        return Err(CopyError::NoPool { node: here(), pool: pool_name.clone() });
    };
    let state = {
        let pool = pool.clone();
        blocking(move || {
            let mountinfo = std::fs::read_to_string("/proc/self/mountinfo")?;
            std::io::Result::Ok(pool_status(&pool, &mountinfo).state)
        })
        .await
        .map_err(CopyError::Mountinfo)?
    };
    if state != PoolState::Ready {
        return Err(CopyError::PoolNotReady { node: here(), pool: pool_name.clone(), state });
    }
    let mode = volume.spec.mode().ok_or_else(|| CopyError::DirMode(volume.spec.dir_mode.clone()))?;

    let holder = context.nodes.get_opt(source).await?.and_then(|n| n.status).unwrap_or_default();
    let (Some(address), Some(certificate)) = (holder.transfer_address, holder.transfer_certificate) else {
        return Err(CopyError::NoAddress(source.to_string()));
    };
    let tls = context
        .identity
        .client_config(&certificate)
        .map_err(|e| CopyError::Identity { node: source.to_string(), source: e })?;

    let destination = {
        let (config, pool, name) = (context.config.clone(), pool.clone(), name.to_string());
        let prepared = tokio::task::spawn_blocking(move || {
            volumes::prepare_incoming(&config.pools, config.project_id_base, &pool, &name, mode)
        });
        prepared.await.map_err(task)?.map_err(CopyError::Prepare)?
    };
    let received = || {
        let (pool, name) = (pool.clone(), name.to_string());
        blocking(move || volumes::received_bytes(&pool, &name).ok().flatten())
    };
    let copying = |received_bytes| Progress { state: TransferState::Copying, received_bytes, message: None };
    report(context, fetch, generation, copying(received().await)).await?;

    let request = Pull {
        rsync: context.rsync.clone(),
        rsh: context.rsh.clone(),
        node: context.node.clone(),
        volume: name.to_string(),
        destination,
        bwlimit_kib: context.config.transfer.bandwidth_limit_kib,
        traffic: context.counters.traffic(name),
    };
    let from = Source { address, tls };
    let copy = pull(&request, &from);
    tokio::pin!(copy);
    let mut tick = tokio::time::interval(PROGRESS);
    tick.tick().await;
    loop {
        tokio::select! {
            result = &mut copy => {
                result.map_err(|e| CopyError::Pull { node: source.to_string(), source: e })?;
                break;
            }
            _ = tick.tick() => {
                // Progress is a courtesy: a failed report does not fail the copy.
                let _ = report(context, fetch, generation, copying(received().await)).await;
            }
        }
    }

    let passport = Passport::of(volume).ok_or(CopyError::Unnamed)?.to_json();
    let sealed = {
        let (pool, name) = (pool.clone(), name.to_string());
        tokio::task::spawn_blocking(move || volumes::seal_incoming(&pool, &name, &passport))
    };
    sealed.await.map_err(task)?.map_err(CopyError::Seal)?;
    // A report for a generation that is gone would only be ignored, and the controller would
    // wait for the next pass of the agent to learn that the copy has to be gone over again.
    match still_wanted(context, fetch).await? {
        None => return Ok(Outcome::Unwanted),
        Some(now) if now.metadata.generation != generation => return Ok(Outcome::Changed),
        Some(_) => {}
    }
    let received_bytes = received().await;
    let copied = Progress { state: TransferState::Copied, received_bytes, message: None };
    report(context, fetch, generation, copied).await?;
    Ok(Outcome::Copied { generation, received_bytes })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::{LocalPathVolumeSpec, LocalPathVolumeStatus};

    /// A volume with its data on node `a`, sent to node `target`.
    fn volume(mobility: Mobility, target: &str) -> LocalPathVolume {
        let spec = LocalPathVolumeSpec {
            pool: "default".into(),
            size_bytes: 1 << 30,
            mobility,
            node: target.into(),
            inodes_per_gib: 0,
            dir_mode: "0770".into(),
            delete_grace_seconds: 0,
            access_mode: Default::default(),
            storage_class: None,
            claim: None,
        };
        let mut volume = LocalPathVolume::new("pv-1", spec);
        volume.status = Some(LocalPathVolumeStatus { node: Some("a".into()), ..Default::default() });
        volume
    }

    #[test]
    fn only_the_node_a_floating_volume_is_sent_to_fetches_it() {
        let moving = volume(Mobility::Floating, "b");
        assert_eq!(wanted(&moving, "b"), Some("a"));
        assert_eq!(wanted(&moving, "c"), None);
        assert_eq!(wanted(&moving, "a"), None, "the node that holds the data fetches nothing");
        assert_eq!(wanted(&volume(Mobility::Floating, "a"), "a"), None, "a volume at rest");
        assert_eq!(wanted(&volume(Mobility::Pinned, "b"), "b"), None, "a pinned volume never travels");
        let mut deleted = moving.clone();
        let epoch = k8s_openapi::jiff::Timestamp::UNIX_EPOCH;
        deleted.metadata.deletion_timestamp = Some(k8s_openapi::apimachinery::pkg::apis::meta::v1::Time(epoch));
        assert_eq!(wanted(&deleted, "b"), None, "a volume that is being deleted");
    }

    #[test]
    fn agent_is_known_by_the_certificate_its_local_node_shows() {
        use crate::api::{LocalPathNodeSpec, LocalPathNodeStatus};
        let (agent, stranger) = (Identity::generate().unwrap(), Identity::generate().unwrap());
        let der = |identity: &Identity| tls::certificate(identity.certificate_pem()).unwrap().to_vec();
        let mut node = LocalPathNode::new("b", LocalPathNodeSpec::default());
        assert!(!presents(&node, &der(&agent)), "a node that has not reported");
        node.status = Some(LocalPathNodeStatus::default());
        assert!(!presents(&node, &der(&agent)), "a node that gives no volumes out");
        let published = Some(agent.certificate_pem().to_string());
        node.status = Some(LocalPathNodeStatus { transfer_certificate: published, ..Default::default() });
        assert!(presents(&node, &der(&agent)));
        assert!(!presents(&node, &der(&stranger)), "another key under the same name");
        assert!(!presents(&node, b""));
    }

    #[test]
    fn volume_is_given_out_only_by_its_node_to_its_destination_and_only_at_rest() {
        let moving = volume(Mobility::Floating, "b");
        assert_eq!(may_fetch(Some(&moving), "pv-1", "a", "b"), Ok(&moving));
        let refusal = |volume: Option<&LocalPathVolume>, holder: &str, asker: &str| {
            may_fetch(volume, "pv-1", holder, asker).unwrap_err().to_string()
        };
        assert_eq!(refusal(None, "a", "b"), "volume pv-1 does not exist");
        assert_eq!(refusal(Some(&moving), "c", "b"), "volume pv-1 is not on node c");
        assert_eq!(refusal(Some(&moving), "a", "c"), "volume pv-1 is not on its way to node c");
        assert_eq!(refusal(Some(&volume(Mobility::Pinned, "b")), "a", "b"), "volume pv-1 is not on its way to node b");
        let mut attached = moving.clone();
        attached.status.as_mut().unwrap().attached_node = Some("a".into());
        assert_eq!(refusal(Some(&attached), "a", "b"), "volume pv-1 is still attached to node a");
    }
}
