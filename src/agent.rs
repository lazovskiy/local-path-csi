//! The agent of a storage node: makes the directories of the pools match the LocalPathVolumes whose
//! data belongs on this node, and reports the pools in the LocalPathNode of the node.
//!
//! Data is never dropped on a doubt. A volume directory without a LocalPathVolume is brought back
//! as a volume, and a volume whose directory vanished is marked lost instead of getting a new one.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::StreamExt;
use k8s_openapi::api::core::v1::Node;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{Condition, Time};
use kube::ResourceExt;
use kube::api::{Api, Patch, PatchParams, PostParams};
use kube::runtime::{WatchStreamExt, reflector, watcher};
use serde_json::json;
use tokio::sync::Notify;

use crate::api::{
    LocalPathNode, LocalPathNodeStatus, LocalPathVolume, Passport, Phase, PoolState, PoolStatus, TransferState,
};
use crate::config::{Config, Pool};
use crate::events::{Events, Notice};
use crate::metrics::{AgentCounters, TrashSample, VolumeSample, count};
use crate::moving::{Receiver, wanted};
use crate::pool::{self, Unusable};
use crate::quota::Limits;
use crate::tls::Identity;
use crate::volumes::{self, Params};
use crate::{api, blocking, unix_now};

/// How often pools are re-examined and usage is refreshed when nothing changes.
const RESYNC: Duration = Duration::from_secs(30);

/// How often the node is reported when nothing changes: the report is also the sign of life the
/// controller goes by.
const REPORT: Duration = Duration::from_secs(30);

/// What the agent does about one volume.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// Create the directory if needed and apply limits.
    Ensure { limits: Limits, mode: u32 },
    /// Remove the directory and release the volume.
    Remove,
    /// Move the directory to the trash for this many seconds and release the volume.
    Trash { grace_seconds: u64 },
    /// The directory of a volume that was ready is gone: an empty one must not take its place.
    MarkLost,
    /// Nothing can be done now; the reason goes into the status.
    Wait(String),
}

/// Decides what to do about `volume`, whose data belongs on this node. `pool` is the pool of
/// the volume with its state, `exists` tells whether the volume directory is there.
pub fn plan(volume: &LocalPathVolume, pool: Option<&PoolStatus>, exists: bool) -> Action {
    let Some(pool) = pool else {
        return Action::Wait(format!("this node has no pool {}", volume.spec.pool));
    };
    if pool.state != PoolState::Ready {
        // A path that failed its checks is never written to, not even to delete.
        return Action::Wait(format!("pool {} at {} is {:?}", pool.name, pool.path, pool.state));
    }
    if volume.metadata.deletion_timestamp.is_some() {
        return match volume.spec.delete_grace_seconds {
            0 => Action::Remove,
            grace_seconds => Action::Trash { grace_seconds },
        };
    }
    // A volume that has moved between nodes has data somewhere, whatever its phase says.
    let had_data = matches!(volume.phase(), Some(Phase::Ready | Phase::Orphaned | Phase::Lost)) || volume.moves() > 0;
    if !exists && had_data {
        return Action::MarkLost;
    }
    let Some(mode) = volume.spec.mode() else {
        return Action::Wait(format!("dirMode {:?} is not an octal mode", volume.spec.dir_mode));
    };
    let limits = Limits { bytes: volume.spec.size_bytes, inodes: volume.spec.inode_limit() };
    Action::Ensure { limits, mode }
}

/// Whether `fresh`, just read from the API server, is the volume the agent has in its cache.
///
/// The cache may be behind: a volume it shows as marked for deletion may have been released
/// already (its node was taken for gone), or replaced by a new volume of the same name. Acting
/// on such a view would remove data that is to be kept.
pub fn still_the_same(cached: &LocalPathVolume, fresh: Option<&LocalPathVolume>) -> bool {
    fresh.is_some_and(|fresh| fresh.metadata.uid.is_some() && fresh.metadata.uid == cached.metadata.uid)
}

/// Bytes the volumes may still write within their sizes. Usage that is not known (a volume not
/// measured yet) counts as none.
pub fn unclaimed_bytes<'a>(volumes: impl IntoIterator<Item = &'a LocalPathVolume>) -> u64 {
    volumes
        .into_iter()
        .map(|v| v.spec.size_bytes.saturating_sub(v.status.as_ref().and_then(|s| s.used_bytes).unwrap_or(0)))
        .sum()
}

/// Bytes the trash of `pool` has to leave free for the volumes of the pool: what they may still
/// write. A pool without quota accounts nothing, neither what a volume has written nor what an
/// entry of the trash holds: every volume would count as empty, and with less free space than
/// the sizes add up to the whole trash would go, entry after entry, each freeing nothing that
/// is known. Nothing there says that the trash is in the way: it is kept until it expires.
pub fn room_owed<'a>(pool: &Pool, volumes: impl IntoIterator<Item = &'a LocalPathVolume>) -> u64 {
    if pool.quota { unclaimed_bytes(volumes) } else { 0 }
}

/// The phase of a volume whose directory is in place with its limits, and what is said with
/// it. A volume an agent brought back from a directory is `Orphaned` for as long as it carries
/// the mark of that: until an administrator has approved it.
pub fn standing(volume: &LocalPathVolume) -> (Phase, Option<String>) {
    if !volume.brought_back() {
        return (Phase::Ready, None);
    }
    let was = match &volume.spec.claim {
        Some(claim) => format!("its directory says it was the volume of claim {}/{}", claim.namespace, claim.name),
        None => "its directory names no claim".to_string(),
    };
    let message = format!(
        "the volume was brought back from its directory, and {was}: it is given to no pod, and has no PersistentVolume made for it, until an administrator approves it by annotating the volume {}=true",
        crate::restore_approval()
    );
    (Phase::Orphaned, Some(message))
}

/// The condition `Ready` an agent writes with the phase of a volume: the same in the form the
/// tools of Kubernetes read, with the reason as a word and with the generation of the volume it
/// is about.
///
/// `waiting` says that the agent could do nothing about the volume this time, and `message` why.
/// The phase is then the one the volume had: of a volume that was ready the agent cannot say
/// whether it still is, and the condition is `Unknown`.
pub fn readiness(
    volume: &LocalPathVolume,
    pool: Option<&PoolStatus>,
    phase: Phase,
    waiting: bool,
    message: Option<&str>,
    now: Time,
) -> Condition {
    let reason = match (waiting, phase) {
        (false, Phase::Ready) => api::REASON_PROVISIONED,
        (false, Phase::Orphaned) => api::REASON_AWAITING_APPROVAL,
        (false, Phase::Lost) => api::REASON_DIRECTORY_LOST,
        // The agent waits: a volume is `Pending` in no other case.
        _ if pool.is_none_or(|pool| pool.state != PoolState::Ready) => api::REASON_POOL_NOT_READY,
        _ if volume.spec.mode().is_none() => api::REASON_INVALID_DIR_MODE,
        _ => api::REASON_COPY_INCOMPLETE,
    };
    let status = match (waiting, phase) {
        (false, Phase::Ready) => "True",
        (true, Phase::Ready | Phase::Orphaned | Phase::Lost) => "Unknown",
        _ => "False",
    };
    // The time of the last change of the status, not of the last report.
    let since = volume.condition(api::READY).filter(|before| before.status == status);
    Condition {
        type_: api::READY.to_string(),
        status: status.to_string(),
        reason: reason.to_string(),
        message: message.unwrap_or_default().to_string(),
        observed_generation: volume.metadata.generation,
        last_transition_time: since.map(|before| before.last_transition_time.clone()).unwrap_or(now),
    }
}

/// What of a volume the agents act on: where it has to be and where it is, whether it is being
/// deleted, and how far its copy has come. What an agent reports of its own volumes - the
/// phase, the applied size, the usage - is not among it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Concern {
    uid: Option<String>,
    /// Changes with every change of the spec.
    generation: Option<i64>,
    deleting: bool,
    /// Brought back from a directory and not approved yet: the phase says so.
    orphaned: bool,
    node: Option<String>,
    moves: u64,
    transfer: Option<(String, String, TransferState, Option<i64>)>,
}

impl Concern {
    pub fn of(volume: &LocalPathVolume) -> Self {
        let status = volume.status.as_ref();
        let transfer = status.and_then(|s| s.transfer.as_ref());
        Self {
            uid: volume.metadata.uid.clone(),
            generation: volume.metadata.generation,
            deleting: volume.metadata.deletion_timestamp.is_some(),
            orphaned: volume.brought_back(),
            node: status.and_then(|s| s.node.clone()),
            moves: volume.moves(),
            // Not the bytes received so far: they change all the time a copy is made.
            transfer: transfer.map(|t| (t.source.clone(), t.target.clone(), t.state, t.observed_generation)),
        }
    }
}

/// Whether a change of a LocalPathVolume is one the agent has something to do about. `seen` is what
/// it knew of the volumes before.
///
/// Every agent watches every volume, and an agent reports the usage of the volumes of its node:
/// were each report a reason for a pass, a pod that writes would keep the agents of all nodes
/// reading their disks, and its own agent reporting again what the pass before had changed.
pub fn worth_a_pass(seen: &mut HashMap<String, Concern>, event: &watcher::Event<LocalPathVolume>) -> bool {
    match event {
        watcher::Event::Apply(volume) | watcher::Event::InitApply(volume) => {
            let concern = Concern::of(volume);
            seen.insert(volume.name_any(), concern.clone()) != Some(concern)
        }
        watcher::Event::Delete(volume) => {
            seen.remove(&volume.name_any());
            true
        }
        // The volumes are listed anew: what is not listed again is gone.
        watcher::Event::Init => {
            seen.clear();
            false
        }
        watcher::Event::InitDone => true,
    }
}

/// Examines a pool of the configuration on this node.
pub fn pool_status(pool: &Pool, mountinfo: &str) -> PoolStatus {
    let mut status = PoolStatus {
        name: pool.name.clone(),
        path: pool.path.display().to_string(),
        state: PoolState::Missing,
        quota: pool.quota,
        fs_type: None,
        capacity_bytes: 0,
        allocatable_bytes: 0,
        available_bytes: 0,
    };
    if !pool.path.is_dir() {
        return status;
    }
    status.state = match pool::check(mountinfo, &pool.path, pool.requirements()) {
        Ok(mount) => {
            status.fs_type = mount.map(|m| m.fs_type);
            PoolState::Ready
        }
        Err(Unusable::NotMountpoint) => PoolState::NotMountpoint,
        Err(Unusable::NoQuota) => PoolState::NoQuota,
    };
    if let Ok(stats) = pool::stats(&pool.path) {
        status.capacity_bytes = stats.total_bytes;
        status.available_bytes = stats.available_bytes;
        // A pool that failed its checks holds no volumes: there is nothing to allocate in it.
        if status.state == PoolState::Ready {
            status.allocatable_bytes = (stats.total_bytes as f64 * pool.overcommit) as u64;
        }
    }
    status
}

/// Why the agent could not do its part for a volume, or report.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Api(#[from] kube::Error),
    #[error(transparent)]
    Disk(#[from] std::io::Error),
    /// A task that works on the disk has panicked.
    #[error(transparent)]
    Task(#[from] tokio::task::JoinError),
    #[error("volume without a name")]
    Unnamed,
    #[error("volume {volume}: its pool {pool} is not on this node")]
    NoPool { volume: String, pool: String },
    #[error("the cluster has no Node {0}: its LocalPathNode is not created")]
    NoNode(String),
}

/// The event that tells the state of a pool: what is wrong and what to do about it.
pub fn pool_event(pool: &PoolStatus) -> Notice {
    let (name, path) = (&pool.name, &pool.path);
    let not_ready = |note: String| Notice::warning("PoolNotReady", note);
    match pool.state {
        PoolState::Ready => Notice::normal("PoolReady", format!("pool {name} at {path} holds volumes")),
        PoolState::Missing => not_ready(format!("pool {name}: {path} does not exist on the node")),
        PoolState::NotMountpoint => not_ready(format!(
            "pool {name}: {path} is not the root of a mounted filesystem; mount the disk of the pool there, or set requireMountpoint: false to use a plain directory"
        )),
        PoolState::NoQuota => not_ready(format!(
            "pool {name}: the filesystem at {path} is mounted without project quota; unmount it and mount it with the prjquota option, or set quota: false to give up limits"
        )),
    }
}

/// What is on the disks of the node, for the metrics page: pools, volume directories, trash.
pub fn samples(config: &Config) -> (Vec<PoolStatus>, Vec<VolumeSample>, Vec<TrashSample>) {
    let statuses = pools(config);
    let (mut volume_samples, mut trash_samples) = (Vec::new(), Vec::new());
    for status in statuses.iter().filter(|p| p.state == PoolState::Ready) {
        let Some(pool) = config.pool(&status.name) else { continue };
        for volume in volumes::list(pool).unwrap_or_default() {
            let passport = volumes::passport(pool, &volume).ok().and_then(|json| Passport::parse(&json));
            volume_samples.push(VolumeSample {
                pool: pool.name.clone(),
                claim: passport
                    .as_ref()
                    .and_then(|p| p.spec.claim.as_ref())
                    .map(|c| (c.namespace.clone(), c.name.clone())),
                size_bytes: passport.as_ref().map(|p| p.spec.size_bytes).unwrap_or(0),
                inode_limit: passport.as_ref().map(|p| p.spec.inode_limit()).unwrap_or(0),
                usage: volumes::usage(pool, &volume).ok().flatten(),
                volume,
            });
        }
        let trashed = volumes::trashed(pool).unwrap_or_default();
        let sizes: Vec<Option<u64>> =
            trashed.iter().map(|entry| volumes::trashed_bytes(pool, entry).ok().flatten()).collect();
        trash_samples.push(TrashSample {
            pool: pool.name.clone(),
            volumes: trashed.len() as u64,
            bytes: pool.quota.then(|| sizes.iter().flatten().sum()),
        });
    }
    (statuses, volume_samples, trash_samples)
}

fn pools(config: &Config) -> Vec<PoolStatus> {
    let mountinfo = std::fs::read_to_string("/proc/self/mountinfo").unwrap_or_default();
    config.pools.iter().map(|pool| pool_status(pool, &mountinfo)).collect()
}

/// [`pools`], read off the threads of the runtime: a pool on a disk that hangs must not stop
/// the report of the node, the server of the kubelet and the copies under way.
async fn examine_pools(config: &Arc<Config>) -> Vec<PoolStatus> {
    let config = config.clone();
    blocking(move || pools(&config)).await
}

/// What `read` finds on the disk about a volume of `pool`, read off the threads of the runtime.
async fn on_disk<T: Send + 'static>(pool: &Pool, volume: &str, read: fn(&Pool, &str) -> T) -> T {
    let (pool, volume) = (pool.clone(), volume.to_string());
    blocking(move || read(&pool, &volume)).await
}

/// The names `list` finds in `pool`, read off the threads of the runtime.
async fn listed(pool: &Pool, list: fn(&Pool) -> std::io::Result<Vec<String>>) -> std::io::Result<Vec<String>> {
    let pool = pool.clone();
    blocking(move || list(&pool)).await
}

/// Keeps the LocalPathNode of the node up to date, until the process is stopped. A change of a pool
/// is reported at once, free space and the time of the report every `REPORT`.
async fn report_node(
    node: String,
    config: Arc<Config>,
    nodes: Api<LocalPathNode>,
    cluster_nodes: Api<Node>,
    events: Arc<Events>,
    counters: Arc<AgentCounters>,
    transport: Arc<Transport>,
) {
    let mut reported: Option<(Vec<PoolStatus>, Instant)> = None;
    loop {
        let pools = examine_pools(&config).await;
        let changed = reported.as_ref().is_none_or(|(last, _)| !same_but_free_space(last, &pools));
        if changed || reported.as_ref().is_some_and(|(_, at)| at.elapsed() >= REPORT) {
            let status = LocalPathNodeStatus {
                pools: pools.clone(),
                reported_at: Some(api::time(unix_now())),
                transfer_address: transport.address.clone(),
                // Without an address nobody connects: the certificate would only mislead.
                transfer_certificate: transport
                    .address
                    .as_ref()
                    .map(|_| transport.identity.certificate_pem().to_string()),
            };
            match report(&node, &nodes, &cluster_nodes, &status).await {
                Ok(()) => {
                    for pool in pools.iter().filter(|_| changed) {
                        tracing::info!(pool = %pool.name, path = %pool.path, state = ?pool.state, allocatable_bytes = pool.allocatable_bytes, "pool");
                        events.node(&node, &pool_event(pool)).await;
                    }
                    reported = Some((pools, Instant::now()));
                }
                Err(e) => {
                    count(&counters.report_errors);
                    tracing::warn!(error = %e, "cannot report the pools of the node");
                }
            }
        }
        tokio::time::sleep(REPORT / 3).await;
    }
}

async fn report(
    node: &str,
    nodes: &Api<LocalPathNode>,
    cluster_nodes: &Api<Node>,
    status: &LocalPathNodeStatus,
) -> Result<(), Error> {
    let existing = nodes.get_opt(node).await?;
    let owned = existing.as_ref().is_some_and(|n| n.metadata.owner_references.as_ref().is_some_and(|o| !o.is_empty()));
    if !owned {
        // The uid is read every time: a Node that was deleted and registered again has a new
        // one, and a LocalPathNode owned by the old one would be garbage-collected over and over.
        let owner = cluster_nodes.get_opt(node).await?.as_ref().and_then(owner_reference);
        let owner = owner.ok_or_else(|| Error::NoNode(node.to_string()))?;
        match existing {
            None => {
                let mut local_node = LocalPathNode::new(node, Default::default());
                local_node.metadata.owner_references = Some(vec![owner]);
                nodes.create(&PostParams::default(), &local_node).await?;
            }
            Some(_) => {
                let patch = json!({"metadata": {"ownerReferences": [owner]}});
                nodes.patch(node, &PatchParams::default(), &Patch::Merge(&patch)).await?;
            }
        }
    }
    nodes.patch_status(node, &PatchParams::default(), &Patch::Merge(&report_patch(status))).await?;
    Ok(())
}

/// The report of a node as a merge patch. All the fields, also the empty ones: a merge patch
/// keeps what it does not mention, and the status itself leaves out what it has not. An agent
/// that gives volumes out no more would leave its former address and certificate in the object,
/// for the other agents to go by.
fn report_patch(status: &LocalPathNodeStatus) -> serde_json::Value {
    json!({"status": {
        "pools": status.pools,
        "reportedAt": status.reported_at,
        "transferAddress": status.transfer_address,
        "transferCertificate": status.transfer_certificate,
    }})
}

/// Whether two sets of pools differ in nothing but their free space.
fn same_but_free_space(a: &[PoolStatus], b: &[PoolStatus]) -> bool {
    let without_free_space = |pools: &[PoolStatus]| -> Vec<PoolStatus> {
        pools.iter().cloned().map(|pool| PoolStatus { available_bytes: 0, ..pool }).collect()
    };
    without_free_space(a) == without_free_space(b)
}

/// Makes the Node the owner of its LocalPathNode: when the Node is deleted, Kubernetes removes the
/// report of a node that is no more. `None` for a Node the API server has not given a uid.
pub fn owner_reference(node: &Node) -> Option<OwnerReference> {
    Some(OwnerReference {
        api_version: "v1".into(),
        kind: "Node".into(),
        name: node.metadata.name.clone()?,
        uid: node.metadata.uid.clone()?,
        // Not a controller, and the deletion of the Node does not wait for the report.
        controller: None,
        block_owner_deletion: None,
    })
}

/// How the agent exchanges volumes with the agents of other nodes.
pub struct Transport {
    /// Where this agent gives volumes out, `host:port`; `None` when it does not.
    pub address: Option<String>,
    pub identity: Arc<Identity>,
    /// The rsync binary.
    pub rsync: PathBuf,
}

pub struct Agent {
    client: kube::Client,
    node: String,
    config: Arc<Config>,
    volumes: Api<LocalPathVolume>,
    nodes: Api<LocalPathNode>,
    cluster_nodes: Api<Node>,
    events: Arc<Events>,
    counters: Arc<AgentCounters>,
    transport: Arc<Transport>,
}

impl Agent {
    pub fn new(
        client: kube::Client,
        node: String,
        config: Arc<Config>,
        counters: Arc<AgentCounters>,
        transport: Arc<Transport>,
    ) -> Self {
        Self {
            events: Arc::new(Events::new(client.clone(), "local-path-csi-agent", Some(node.clone()))),
            counters,
            transport,
            node,
            config,
            volumes: Api::all(client.clone()),
            nodes: Api::all(client.clone()),
            cluster_nodes: Api::all(client.clone()),
            client,
        }
    }

    /// Runs until the process is stopped.
    pub async fn run(self) {
        let (store, writer) = reflector::store();
        let changed = Arc::new(Notify::new());
        tokio::spawn({
            let api = self.volumes.clone();
            let changed = changed.clone();
            async move {
                let stream = reflector(writer, watcher(api, watcher::Config::default()).default_backoff());
                let mut stream = std::pin::pin!(stream);
                let mut seen = HashMap::new();
                while let Some(event) = stream.next().await {
                    match event {
                        Ok(event) if worth_a_pass(&mut seen, &event) => changed.notify_one(),
                        Ok(_) => {}
                        Err(e) => tracing::warn!(error = %e, "watch of LocalPathVolumes failed, retrying"),
                    }
                }
            }
        });
        // The writer lives in a task that never ends.
        let _ = store.wait_until_ready().await;
        // On its own task: a long removal of a volume must not make the node look dead.
        tokio::spawn(report_node(
            self.node.clone(),
            self.config.clone(),
            self.nodes.clone(),
            self.cluster_nodes.clone(),
            self.events.clone(),
            self.counters.clone(),
            self.transport.clone(),
        ));
        let mut receiver = match Receiver::new(
            self.client.clone(),
            self.node.clone(),
            self.config.clone(),
            self.transport.identity.clone(),
            self.counters.clone(),
            self.transport.rsync.clone(),
        ) {
            Ok(receiver) => Some(receiver),
            Err(e) => {
                tracing::error!(error = %e, "this node cannot receive volumes: the path of the agent binary is unknown");
                None
            }
        };
        // Directories that could not be brought back or put away: each is complained about once.
        let mut unreadable: HashSet<String> = HashSet::new();
        let mut measured: Option<Instant> = None;
        loop {
            // Usage changes with every write of a pod. It is read and reported once in `RESYNC`,
            // not on every pass: a pass may follow another at once.
            let measure = measured.is_none_or(|at| at.elapsed() >= RESYNC);
            if measure {
                measured = Some(Instant::now());
            }
            let pools = examine_pools(&self.config).await;
            let all = store.state();
            let busy = |name: &str| receiver.as_ref().is_some_and(|r| r.busy(name));
            for volume in all.iter().filter(|v| v.data_node() == self.node) {
                let name = volume.metadata.name.as_deref().unwrap_or_default();
                // A copy of the volume is still being written here: its directories are not
                // touched until that has stopped.
                if busy(name) {
                    continue;
                }
                if let Err(e) = self.reconcile(volume, &pools, measure).await {
                    count(&self.counters.reconcile_errors);
                    tracing::warn!(volume = name, error = %e, "cannot reconcile the volume");
                }
            }
            let known: HashSet<&str> = all.iter().filter_map(|v| v.metadata.name.as_deref()).collect();
            let ready: Vec<(&PoolStatus, &Pool)> = pools
                .iter()
                .filter(|p| p.state == PoolState::Ready)
                .filter_map(|status| Some((status, self.config.pool(&status.name)?)))
                .collect();
            for (_, pool) in &ready {
                if let Err(e) = self.adopt_orphans(pool, &known, &mut unreadable).await {
                    count(&self.counters.reconcile_errors);
                    tracing::warn!(pool = %pool.name, error = %e, "cannot look for volumes without a LocalPathVolume");
                }
                // Before copies are started: a copy this node kept of a volume that comes back is
                // what the new copy starts from.
                if let Err(e) = self.retire_copies(pool, &all, &mut unreadable).await {
                    count(&self.counters.reconcile_errors);
                    tracing::warn!(pool = %pool.name, error = %e, "cannot put away the copies of volumes that have moved");
                }
            }
            if let Some(receiver) = receiver.as_mut() {
                receiver.sync(&all).await;
            }
            let busy = |name: &str| receiver.as_ref().is_some_and(|r| r.busy(name));
            for (status, pool) in ready {
                if let Err(e) = self.clear_incoming(pool, &all, &busy).await {
                    count(&self.counters.reconcile_errors);
                    tracing::warn!(pool = %pool.name, error = %e, "cannot remove the copies nobody waits for");
                }
                // Room for the volumes that live here and for the ones on their way here.
                let mine = all.iter().filter(|v| v.spec.pool == pool.name);
                let mine = mine.filter(|v| v.data_node() == self.node || wanted(v, &self.node).is_some());
                let owed = room_owed(pool, mine.map(|v| v.as_ref()));
                if let Err(e) = self.empty_trash(pool, status.available_bytes, owed).await {
                    count(&self.counters.reconcile_errors);
                    tracing::warn!(pool = %pool.name, error = %e, "cannot empty the trash");
                }
            }
            // The node reports on a task of its own: a pass that does not end shows only here.
            self.counters.last_pass.mark();
            let _ = tokio::time::timeout(RESYNC, changed.notified()).await;
        }
    }

    /// Lets a volume go whose data is off the node: takes the finalizer of the driver off it.
    /// The volume is the one just read from the API server; when its finalizers have changed
    /// since, the patch fails and the next pass comes here again.
    async fn release(&self, volume: &LocalPathVolume) -> Result<(), kube::Error> {
        let Some(patch) = volume.without_finalizer() else { return Ok(()) };
        self.volumes.patch(&volume.name_any(), &PatchParams::default(), &Patch::Json::<()>(patch)).await?;
        Ok(())
    }

    /// Brings the directory of `volume` in line with it and reports. `measure` says whether
    /// this is a pass that reads the usage anew.
    async fn reconcile(&self, volume: &LocalPathVolume, pools: &[PoolStatus], measure: bool) -> Result<(), Error> {
        let name = volume.metadata.name.clone().ok_or(Error::Unnamed)?;
        let pool = self.config.pool(&volume.spec.pool).filter(|_| volumes::valid_name(&name));
        let pool_state = pools.iter().find(|p| p.name == volume.spec.pool);
        let mut exists = match pool {
            Some(pool) => on_disk(pool, &name, volumes::exists).await,
            None => false,
        };
        let usable = pool.filter(|_| pool_state.is_some_and(|p| p.state == PoolState::Ready));
        if let Some(pool) = usable.filter(|_| !exists && volume.metadata.deletion_timestamp.is_none())
            && on_disk(pool, &name, volumes::arrived).await
        {
            // The volume was handed over to this node: its copy becomes the volume.
            let read = self.volumes.get_opt(&name).await?;
            if !still_the_same(volume, read.as_ref()) || read.is_some_and(|read| read.data_node() != self.node) {
                return Ok(());
            }
            let (owned_pool, volume_name) = (pool.clone(), name.clone());
            tokio::task::spawn_blocking(move || volumes::accept(&owned_pool, &volume_name)).await??;
            count(&self.counters.accepted);
            tracing::info!(volume = %name, pool = %pool.name, "the copy that arrived is the volume now");
            exists = true;
        }
        let mut action = plan(volume, pool_state.filter(|_| pool.is_some()), exists);
        let unfinished = |pool: &Pool, name: &str| volumes::incoming(pool).is_ok_and(|i| i.iter().any(|n| n == name));
        if action == Action::MarkLost
            && let Some(pool) = usable
            && on_disk(pool, &name, unfinished).await
        {
            // Never seen in practice: the volume was handed over while its copy was being redone.
            action = Action::Wait(format!(
                "the copy of the volume on node {} was not completed: look into incoming/{name} of pool {}",
                self.node, volume.spec.pool
            ));
        }
        // Before data is removed or a directory is created, the cache is checked against the
        // API server.
        let fresh;
        let mut volume = volume;
        if matches!(action, Action::Remove | Action::Trash { .. })
            || (matches!(action, Action::Ensure { .. }) && !exists)
        {
            match self.volumes.get_opt(&name).await? {
                Some(read) if still_the_same(volume, Some(&read)) => {
                    fresh = read;
                    volume = &fresh;
                    action = plan(volume, pool_state.filter(|_| pool.is_some()), exists);
                }
                _ => {
                    tracing::info!(volume = %name, "the volume is gone from the API server: its directory is left alone");
                    return Ok(());
                }
            }
        }
        let nothing = api::LocalPathVolumeStatus::default();
        let current = volume.status.as_ref().unwrap_or(&nothing);

        let (phase, message) = match (&action, pool) {
            (Action::Ensure { limits, mode }, Some(pool)) => {
                let passport = Passport::of(volume).ok_or(Error::Unnamed)?.to_json();
                let (config, pool, volume_name, limits, mode) =
                    (self.config.clone(), pool.clone(), name.clone(), *limits, *mode);
                tokio::task::spawn_blocking(move || {
                    let params = Params { limits, mode, passport: &passport };
                    volumes::ensure(&config.pools, config.project_id_base, &pool, &volume_name, &params)
                })
                .await??;
                standing(volume)
            }
            (Action::Remove, Some(pool)) => {
                let (owned_pool, volume_name) = (pool.clone(), name.clone());
                tokio::task::spawn_blocking(move || volumes::remove(&owned_pool, &volume_name)).await??;
                self.release(volume).await?;
                count(&self.counters.removed);
                tracing::info!(volume = %name, pool = %pool.name, "volume removed");
                return Ok(());
            }
            (Action::Trash { grace_seconds }, Some(pool)) => {
                let (owned_pool, volume_name, expires) = (pool.clone(), name.clone(), unix_now() + grace_seconds);
                tokio::task::spawn_blocking(move || volumes::trash(&owned_pool, &volume_name, expires)).await??;
                self.release(volume).await?;
                count(&self.counters.trashed);
                tracing::info!(volume = %name, pool = %pool.name, grace_seconds, "volume moved to the trash");
                let until = api::timestamp(expires);
                let note =
                    format!("the data is kept in the trash of pool {} on node {} until {until}", pool.name, self.node);
                self.events.volume(volume, &Notice::normal("MovedToTrash", &note)).await;
                return Ok(());
            }
            (Action::MarkLost, _) => {
                (Phase::Lost, Some(format!("the directory of the volume is gone from node {}", self.node)))
            }
            (Action::Wait(reason), _) => (current.phase.unwrap_or(Phase::Pending), Some(reason.clone())),
            // `plan` waits with a volume whose pool is not on the node.
            (Action::Ensure { .. } | Action::Remove | Action::Trash { .. }, None) => {
                return Err(Error::NoPool { volume: name, pool: volume.spec.pool.clone() });
            }
        };

        let in_place = matches!(phase, Phase::Ready | Phase::Orphaned);
        let capacity_bytes = if in_place { Some(volume.spec.size_bytes) } else { current.capacity_bytes };
        let used_bytes = match pool.filter(|_| in_place) {
            Some(pool) if measure || current.used_bytes.is_none() => on_disk(pool, &name, volumes::used_bytes).await?,
            _ => current.used_bytes,
        };
        let waiting = matches!(action, Action::Wait(_));
        let now = Time(k8s_openapi::jiff::Timestamp::now());
        let ready = readiness(volume, pool_state.filter(|_| pool.is_some()), phase, waiting, message.as_deref(), now);
        let changed = current.phase != Some(phase)
            || current.capacity_bytes != capacity_bytes
            || current.used_bytes != used_bytes
            || current.message != message
            || current.reported_by.as_deref() != Some(self.node.as_str())
            || volume.condition(api::READY) != Some(&ready);
        if changed {
            // The list is written whole: the conditions of other types stay as they are.
            let others = current.conditions.iter().filter(|condition| condition.type_ != api::READY);
            let conditions: Vec<&Condition> = others.chain([&ready]).collect();
            let patch = json!({"status": {
                "phase": phase, "capacityBytes": capacity_bytes, "usedBytes": used_bytes, "message": message,
                "reportedBy": self.node, "conditions": conditions,
            }});
            self.volumes.patch_status(&name, &PatchParams::default(), &Patch::Merge(&patch)).await?;
            match (&action, &message) {
                (Action::MarkLost, Some(note)) if current.phase != Some(Phase::Lost) => {
                    self.events.volume(volume, &Notice::warning("VolumeLost", note)).await;
                }
                (Action::Wait(_), Some(note)) if current.message.as_ref() != Some(note) => {
                    self.events.volume(volume, &Notice::warning("VolumeWaiting", note)).await;
                }
                _ => {}
            }
            if current.phase != Some(phase) || current.capacity_bytes != capacity_bytes {
                tracing::info!(volume = %name, ?phase, ?capacity_bytes, message = message.as_deref().unwrap_or(""), "volume");
            }
        }
        Ok(())
    }
}

impl Agent {
    /// Brings back volumes whose directory is in the pool while their LocalPathVolume is gone: the
    /// node was away when the volume was deleted, the object was removed by force, etcd was
    /// restored from an older backup.
    async fn adopt_orphans(
        &self,
        pool: &Pool,
        known: &HashSet<&str>,
        unreadable: &mut HashSet<String>,
    ) -> Result<(), Error> {
        for name in listed(pool, volumes::list).await?.into_iter().filter(|n| !known.contains(n.as_str())) {
            // The watch may lag behind: only the API server knows that the volume does not exist.
            if self.volumes.get_opt(&name).await?.is_some() {
                continue;
            }
            let passport = on_disk(pool, &name, volumes::passport).await.ok().and_then(|json| Passport::parse(&json));
            let Some(passport) = passport.filter(|p| p.name == name && volumes::valid_name(&name)) else {
                if unreadable.insert(format!("{}/{name}", pool.name)) {
                    tracing::warn!(volume = %name, pool = %pool.name, "a volume directory has neither a LocalPathVolume nor a readable volume.json: it is left alone");
                }
                continue;
            };
            let mut volume = LocalPathVolume::new(&name, passport.spec);
            volume.spec.pool = pool.name.clone();
            volume.spec.node = self.node.clone();
            volume.metadata.finalizers = Some(vec![crate::finalizer().to_string()]);
            volume.metadata.labels = Some([(crate::restore_label().to_string(), "true".to_string())].into());
            let created = self.volumes.create(&PostParams::default(), &volume).await?;
            if passport.moves > 0 {
                // What tells this copy from an older one that another node may still have.
                let patch = json!({"status": {"moves": passport.moves}});
                self.volumes.patch_status(&name, &PatchParams::default(), &Patch::Merge(&patch)).await?;
            }
            count(&self.counters.restored);
            tracing::info!(volume = %name, pool = %pool.name, "volume brought back from its directory");
            let note = format!(
                "a directory of pool {} on node {} had no LocalPathVolume: the volume is brought back, and waits for an administrator to approve it before any pod is given it",
                pool.name, self.node
            );
            self.events.volume(&created, &Notice::normal("VolumeRestored", &note)).await;
        }
        Ok(())
    }

    /// Puts away the copies of volumes whose data is on another node now: a volume that has
    /// moved leaves its old copy behind, and it stays in the trash for a while - as a way back,
    /// and as the start of the next copy should the volume return.
    async fn retire_copies(
        &self,
        pool: &Pool,
        all: &[Arc<LocalPathVolume>],
        complained: &mut HashSet<String>,
    ) -> Result<(), Error> {
        let elsewhere = |volume: &LocalPathVolume| {
            let holder = volume.status.as_ref().and_then(|s| s.node.as_deref());
            volume.spec.pool == pool.name && holder.is_some_and(|holder| holder != self.node)
        };
        for name in listed(pool, volumes::list).await? {
            let cached = all.iter().find(|v| v.metadata.name.as_deref() == Some(name.as_str()));
            let Some(cached) = cached.filter(|v| elsewhere(v)) else { continue };
            let Some(fresh) = self.volumes.get_opt(&name).await? else { continue };
            if !still_the_same(cached, Some(&fresh)) || !elsewhere(&fresh) {
                continue;
            }
            let holder = fresh.data_node().to_string();
            // Of two copies the one that has seen fewer moves is the older. A copy that is not
            // older than what the cluster believes in is evidence of a rollback: it stays.
            let passport = on_disk(pool, &name, volumes::passport).await.ok().and_then(|json| Passport::parse(&json));
            if passport.is_none_or(|passport| passport.moves >= fresh.moves()) {
                if complained.insert(format!("copy:{}/{name}", pool.name)) {
                    let note = format!(
                        "pool {} on node {} has a copy of the volume that is not older than the one on node {holder}: it is left alone",
                        pool.name, self.node
                    );
                    tracing::warn!(volume = %name, "{note}");
                    self.events.volume(&fresh, &Notice::warning("VolumeCopyKept", &note)).await;
                }
                continue;
            }
            let keep = self.config.transfer.keep_source_seconds;
            let (owned_pool, volume_name, expires) = (pool.clone(), name.clone(), unix_now() + keep);
            tokio::task::spawn_blocking(move || match keep {
                0 => volumes::remove(&owned_pool, &volume_name),
                _ => volumes::trash(&owned_pool, &volume_name, expires),
            })
            .await??;
            tracing::info!(volume = %name, pool = %pool.name, %holder, keep_seconds = keep, "the volume has moved: its copy here is put away");
            if keep == 0 {
                count(&self.counters.removed);
                continue;
            }
            count(&self.counters.trashed);
            let note = format!(
                "the volume is on node {holder} now: the copy it left in pool {} on node {} is kept in the trash until {}",
                pool.name,
                self.node,
                api::timestamp(expires)
            );
            self.events.volume(&fresh, &Notice::normal("MovedToTrash", &note)).await;
        }
        Ok(())
    }

    /// Removes received copies nobody waits for: the volume was sent elsewhere, went back to the
    /// node it came from, or is gone.
    async fn clear_incoming(
        &self,
        pool: &Pool,
        all: &[Arc<LocalPathVolume>],
        busy: &impl Fn(&str) -> bool,
    ) -> Result<(), Error> {
        // A copy is needed while the volume is on its way here, and from the moment it is handed
        // over until it has become the volume.
        let needed = |volume: &LocalPathVolume| {
            let here = volume.data_node() == self.node && volume.metadata.deletion_timestamp.is_none();
            volume.spec.pool == pool.name && (here || wanted(volume, &self.node).is_some())
        };
        for name in listed(pool, volumes::incoming).await? {
            if busy(&name) {
                continue;
            }
            let cached = all.iter().find(|v| v.metadata.name.as_deref() == Some(name.as_str()));
            if cached.is_some_and(|v| needed(v)) {
                continue;
            }
            // The cache may be behind: the API server has the last word before data is removed.
            if self.volumes.get_opt(&name).await?.is_some_and(|fresh| needed(&fresh)) {
                continue;
            }
            let (owned_pool, volume_name) = (pool.clone(), name.clone());
            tokio::task::spawn_blocking(move || volumes::discard_incoming(&owned_pool, &volume_name)).await??;
            tracing::info!(volume = %name, pool = %pool.name, "a copy nobody waits for is removed");
        }
        Ok(())
    }

    /// Removes what has expired in the trash of the pool, and more when the volumes need the room.
    async fn empty_trash(&self, pool: &Pool, available_bytes: u64, unclaimed_bytes: u64) -> Result<(), Error> {
        let (pool, counters) = (pool.clone(), self.counters.clone());
        tokio::task::spawn_blocking(move || -> std::io::Result<()> {
            let mut trashed = Vec::new();
            for entry in volumes::trashed(&pool)? {
                let bytes = volumes::trashed_bytes(&pool, &entry)?;
                trashed.push((entry, bytes));
            }
            for entry in volumes::purge_candidates(&trashed, unix_now(), available_bytes, unclaimed_bytes) {
                volumes::purge(&pool, &entry)?;
                count(&counters.purged);
                tracing::info!(volume = %entry.volume, pool = %pool.name, expires = entry.expires, "removed from the trash");
            }
            Ok(())
        })
        .await??;
        Ok(())
    }
}

/// The path of a volume the kubelet asks to publish, or `None` when this node has no such pool
/// or the name is not a directory name.
pub fn volume_path(config: &Config, pool: &str, volume: &str) -> Option<std::path::PathBuf> {
    let pool = config.pool(pool).filter(|_| volumes::valid_name(volume))?;
    Some(volumes::data_dir(pool, volume)).filter(|path| Path::new(path).is_dir())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::{LocalPathVolumeSpec, LocalPathVolumeStatus, Mobility};

    const GIB: u64 = 1 << 30;

    fn volume(phase: Option<Phase>) -> LocalPathVolume {
        let spec = LocalPathVolumeSpec {
            pool: "default".into(),
            size_bytes: 10 * GIB,
            mobility: Mobility::Pinned,
            node: "a".into(),
            inodes_per_gib: 100,
            dir_mode: "0750".into(),
            delete_grace_seconds: 0,
            access_mode: Default::default(),
            storage_class: None,
            claim: None,
        };
        let mut volume = LocalPathVolume::new("pv-1", spec);
        volume.status = Some(LocalPathVolumeStatus { node: Some("a".into()), phase, ..Default::default() });
        volume
    }

    fn deleted(mut volume: LocalPathVolume) -> LocalPathVolume {
        let epoch = k8s_openapi::jiff::Timestamp::UNIX_EPOCH;
        volume.metadata.deletion_timestamp = Some(k8s_openapi::apimachinery::pkg::apis::meta::v1::Time(epoch));
        volume
    }

    fn pool(state: PoolState) -> PoolStatus {
        PoolStatus {
            name: "default".into(),
            path: "/mnt/pool".into(),
            state,
            quota: true,
            fs_type: None,
            capacity_bytes: 100 * GIB,
            allocatable_bytes: 100 * GIB,
            available_bytes: 100 * GIB,
        }
    }

    #[test]
    fn new_volume_gets_a_directory_with_its_size_inode_limit_and_mode() {
        let action = plan(&volume(None), Some(&pool(PoolState::Ready)), false);
        assert_eq!(action, Action::Ensure { limits: Limits { bytes: 10 * GIB, inodes: 1000 }, mode: 0o750 });
    }

    #[test]
    fn ready_volume_keeps_being_brought_to_its_spec() {
        // This is how a resize reaches the quota: the directory exists, the size in the spec changed.
        let mut resized = volume(Some(Phase::Ready));
        resized.spec.size_bytes = 20 * GIB;
        let action = plan(&resized, Some(&pool(PoolState::Ready)), true);
        assert_eq!(action, Action::Ensure { limits: Limits { bytes: 20 * GIB, inodes: 2000 }, mode: 0o750 });
    }

    #[test]
    fn vanished_directory_of_a_ready_volume_is_not_replaced_by_an_empty_one() {
        assert_eq!(plan(&volume(Some(Phase::Ready)), Some(&pool(PoolState::Ready)), false), Action::MarkLost);
        assert_eq!(plan(&volume(Some(Phase::Lost)), Some(&pool(PoolState::Ready)), false), Action::MarkLost);
    }

    #[test]
    fn volume_marked_for_deletion_is_removed_whatever_its_phase() {
        for phase in [None, Some(Phase::Pending), Some(Phase::Ready), Some(Phase::Lost)] {
            for exists in [true, false] {
                assert_eq!(plan(&deleted(volume(phase)), Some(&pool(PoolState::Ready)), exists), Action::Remove);
            }
        }
    }

    #[test]
    fn cached_volume_is_trusted_only_while_the_api_server_has_the_same_one() {
        let mut cached = deleted(volume(Some(Phase::Ready)));
        cached.metadata.uid = Some("one".into());
        assert!(still_the_same(&cached, Some(&cached.clone())));
        // Released while the agent was away: the object is gone.
        assert!(!still_the_same(&cached, None));
        // A new volume got the name.
        let mut replacement = volume(None);
        replacement.metadata.uid = Some("two".into());
        assert!(!still_the_same(&cached, Some(&replacement)));
        // Objects that never reached the API server have no uid and prove nothing.
        assert!(!still_the_same(&volume(None), Some(&volume(None))));
    }

    #[test]
    fn agent_makes_a_pass_for_what_it_acts_on_and_not_for_reports_of_usage() {
        use crate::api::Transfer;
        let mut seen = HashMap::new();
        let mut volume = volume(Some(Phase::Ready));
        (volume.metadata.uid, volume.metadata.generation) = (Some("one".into()), Some(1));
        fn news(seen: &mut HashMap<String, Concern>, volume: &LocalPathVolume) -> bool {
            worth_a_pass(seen, &watcher::Event::Apply(volume.clone()))
        }
        let mut news = |volume: &LocalPathVolume| news(&mut seen, volume);
        assert!(news(&volume), "a volume the agent has not seen");
        assert!(!news(&volume));

        // What the agent of the volume reports while a pod writes.
        let status = volume.status.as_mut().unwrap();
        (status.used_bytes, status.capacity_bytes, status.reported_by) = (Some(GIB), Some(10 * GIB), Some("a".into()));
        assert!(!news(&volume));
        volume.status.as_mut().unwrap().used_bytes = Some(2 * GIB);
        assert!(!news(&volume));
        volume.status.as_mut().unwrap().attached_node = Some("a".into());
        assert!(!news(&volume), "which node uses the volume changes nothing on the disks");

        // The spec has changed: a resize, or the volume is sent to another node.
        volume.metadata.generation = Some(2);
        assert!(news(&volume));
        let report = |state, received_bytes| Transfer {
            source: "a".into(),
            target: "b".into(),
            state,
            observed_generation: Some(2),
            received_bytes,
            started_at: None,
            message: None,
        };
        volume.status.as_mut().unwrap().transfer = Some(report(TransferState::Copying, Some(GIB)));
        assert!(news(&volume), "a copy has started");
        volume.status.as_mut().unwrap().transfer = Some(report(TransferState::Copying, Some(2 * GIB)));
        assert!(!news(&volume), "the progress of a copy");
        volume.status.as_mut().unwrap().transfer = Some(report(TransferState::Copied, Some(3 * GIB)));
        assert!(news(&volume), "the copy is complete");
        let status = volume.status.as_mut().unwrap();
        (status.node, status.moves, status.transfer) = (Some("b".into()), 1, None);
        assert!(news(&volume), "the volume is handed over");
        let volume = deleted(volume);
        assert!(news(&volume), "the volume is marked for deletion");

        assert!(worth_a_pass(&mut seen, &watcher::Event::Delete(volume.clone())));
        assert!(worth_a_pass(&mut seen, &watcher::Event::Apply(volume.clone())), "a volume that was gone is new");
        // After a break of the watch everything is listed anew, and what is gone is not.
        assert!(!worth_a_pass(&mut seen, &watcher::Event::Init));
        assert!(worth_a_pass(&mut seen, &watcher::Event::InitApply(volume.clone())));
        assert!(worth_a_pass(&mut seen, &watcher::Event::InitDone));
    }

    #[test]
    fn pool_event_names_the_problem_and_the_way_out() {
        assert!(!pool_event(&pool(PoolState::Ready)).is_warning());
        let notice = pool_event(&pool(PoolState::NoQuota));
        let (warning, reason, note) = (notice.is_warning(), notice.reason, notice.note);
        assert!(
            warning && reason == "PoolNotReady" && note.contains("prjquota") && note.contains("quota: false"),
            "{note}"
        );
        let notice = pool_event(&pool(PoolState::NotMountpoint));
        let (warning, note) = (notice.is_warning(), notice.note);
        assert!(warning && note.contains("/mnt/pool") && note.contains("requireMountpoint: false"), "{note}");
        assert!(pool_event(&pool(PoolState::Missing)).note.contains("does not exist"));
    }

    #[test]
    fn samples_describe_the_volume_directories_of_a_pool() {
        let path = std::env::temp_dir().join(format!("local-path-csi-samples-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).unwrap();
        let plain =
            Pool { name: "plain".into(), path: path.clone(), quota: false, require_mountpoint: false, overcommit: 1.0 };
        let config = Config { pools: vec![plain.clone()], project_id_base: 1000, transfer: Default::default() };
        let mut with_claim = volume(Some(Phase::Ready));
        with_claim.spec.claim = Some(api::Claim { namespace: "infra".into(), name: "data".into() });
        let passport = Passport::of(&with_claim).unwrap().to_json();
        let params = Params { limits: Limits::default(), mode: 0o770, passport: &passport };
        volumes::ensure(&config.pools, 1000, &plain, "pv-1", &params).unwrap();
        volumes::ensure(&config.pools, 1000, &plain, "pv-old", &params).unwrap();
        volumes::trash(&plain, "pv-old", 100).unwrap();

        let (pools, volume_samples, trash) = samples(&config);
        assert_eq!(pools.len(), 1);
        assert_eq!(
            volume_samples,
            [VolumeSample {
                pool: "plain".into(),
                volume: "pv-1".into(),
                claim: Some(("infra".into(), "data".into())),
                size_bytes: 10 * GIB,
                inode_limit: 1000,
                usage: None,
            }]
        );
        assert_eq!(trash, [TrashSample { pool: "plain".into(), volumes: 1, bytes: None }]);
        std::fs::remove_dir_all(&path).unwrap();
    }

    #[test]
    fn node_owns_its_report_without_being_held_by_it() {
        let mut node = Node::default();
        node.metadata.name = Some("a".into());
        assert_eq!(owner_reference(&node), None);
        node.metadata.uid = Some("0f6b0d9e".into());
        let owner = owner_reference(&node).unwrap();
        assert_eq!(
            (owner.api_version.as_str(), owner.kind.as_str(), owner.name.as_str(), owner.uid.as_str()),
            ("v1", "Node", "a", "0f6b0d9e")
        );
        assert_eq!((owner.controller, owner.block_owner_deletion), (None, None));
    }

    #[test]
    fn report_of_a_node_takes_back_what_the_node_has_no_more() {
        let serving = LocalPathNodeStatus {
            pools: vec![pool(PoolState::Ready)],
            reported_at: Some(api::time(1_795_000_000)),
            transfer_address: Some("10.0.0.7:9754".into()),
            transfer_certificate: Some("-----BEGIN CERTIFICATE-----".into()),
        };
        // Every field the status has is in the patch: one that is added and forgotten here
        // would be reported once and never taken back.
        let patch = report_patch(&serving);
        assert_eq!(patch["status"], serde_json::to_value(&serving).unwrap());

        // The agent was started again without an address: null is what removes a field.
        let silent = LocalPathNodeStatus { transfer_address: None, transfer_certificate: None, ..serving };
        let patch = report_patch(&silent);
        let status = patch["status"].as_object().unwrap();
        for field in ["transferAddress", "transferCertificate"] {
            assert_eq!(status.get(field), Some(&serde_json::Value::Null), "{field}");
        }
        assert_eq!(status["reportedAt"], "2026-11-18T11:06:40Z");
        assert_eq!(status["pools"][0]["name"], "default");
    }

    #[test]
    fn free_space_alone_is_not_a_change_of_a_pool() {
        let ready = pool(PoolState::Ready);
        let ready = [ready];
        let less_free = [PoolStatus { available_bytes: GIB, ..ready[0].clone() }];
        assert!(same_but_free_space(&ready, &less_free));
        assert!(!same_but_free_space(&ready, &[pool(PoolState::NoQuota)]));
        assert!(!same_but_free_space(&ready, &[]));
    }

    #[test]
    fn deleted_volume_with_a_grace_period_goes_to_the_trash() {
        let mut kept = deleted(volume(Some(Phase::Ready)));
        kept.spec.delete_grace_seconds = 3600;
        assert_eq!(plan(&kept, Some(&pool(PoolState::Ready)), true), Action::Trash { grace_seconds: 3600 });
        // A volume that is not deleted is not touched by its grace period.
        let mut alive = volume(Some(Phase::Ready));
        alive.spec.delete_grace_seconds = 3600;
        assert!(matches!(plan(&alive, Some(&pool(PoolState::Ready)), true), Action::Ensure { .. }));
    }

    #[test]
    fn unclaimed_bytes_are_what_volumes_may_still_write() {
        let mut half_full = volume(Some(Phase::Ready));
        half_full.status.as_mut().unwrap().used_bytes = Some(4 * GIB);
        let unmeasured = volume(Some(Phase::Ready));
        let mut overfull = volume(Some(Phase::Ready));
        overfull.status.as_mut().unwrap().used_bytes = Some(11 * GIB);
        assert_eq!(unclaimed_bytes([&half_full, &unmeasured, &overfull]), 6 * GIB + 10 * GIB);
        assert_eq!(unclaimed_bytes([]), 0);
    }

    #[test]
    fn trash_of_a_pool_without_quota_goes_only_when_it_expires() {
        let path = PathBuf::from("/var/lib/plain");
        let plain = Pool { name: "default".into(), path, quota: false, require_mountpoint: false, overcommit: 1.0 };
        let accounted = Pool { quota: true, ..plain.clone() };
        // Nobody knows what a volume of a pool without quota has written.
        let unmeasured = [volume(Some(Phase::Ready)), volume(Some(Phase::Ready))];
        assert_eq!(room_owed(&accounted, &unmeasured), 20 * GIB);
        assert_eq!(room_owed(&plain, &unmeasured), 0);

        // The filesystem has less free space than the sizes of the volumes add up to: with
        // quota that is the trash in the way of the volumes. Without, nothing says so, and an
        // entry that has not expired stays.
        let entry = |volume: &str, expires| volumes::Trashed {
            path: plain.path.join(format!("trash/{volume}.{expires}")),
            volume: volume.into(),
            expires,
        };
        let trash = [(entry("pv-old", 100), None), (entry("pv-kept", 5000), None)];
        let purged = |room| volumes::purge_candidates(&trash, 1000, GIB, room);
        assert_eq!(purged(room_owed(&plain, &unmeasured)), [entry("pv-old", 100)]);
    }

    #[test]
    fn nothing_is_written_to_a_pool_that_failed_its_checks() {
        for state in [PoolState::Missing, PoolState::NotMountpoint, PoolState::NoQuota] {
            for candidate in [volume(None), volume(Some(Phase::Ready)), deleted(volume(Some(Phase::Ready)))] {
                assert!(matches!(plan(&candidate, Some(&pool(state)), true), Action::Wait(_)), "{state:?}");
            }
        }
        assert!(matches!(plan(&volume(None), None, false), Action::Wait(_)));
    }

    #[test]
    fn malformed_mode_keeps_the_volume_waiting() {
        let mut broken = volume(None);
        broken.spec.dir_mode = "rwx".into();
        assert!(matches!(plan(&broken, Some(&pool(PoolState::Ready)), false), Action::Wait(_)));
    }

    #[test]
    fn pool_path_that_is_not_there_is_missing() {
        let pool = Pool {
            name: "default".into(),
            path: "/nonexistent/local-path-csi-pool".into(),
            quota: true,
            require_mountpoint: true,
            overcommit: 1.0,
        };
        let status = pool_status(&pool, "");
        assert_eq!((status.state, status.allocatable_bytes), (PoolState::Missing, 0));
    }

    #[test]
    fn plain_directory_is_a_pool_only_when_both_checks_are_off_and_overcommit_scales_it() {
        let path = std::env::temp_dir();
        let strict =
            Pool { name: "p".into(), path: path.clone(), quota: true, require_mountpoint: true, overcommit: 1.0 };
        let refused = pool_status(&strict, "");
        assert_eq!(refused.state, PoolState::NotMountpoint);
        // The filesystem is still measured, but nothing may be allocated in a refused pool.
        assert!(refused.capacity_bytes > 0);
        assert_eq!(refused.allocatable_bytes, 0);
        let no_mount_check = Pool { require_mountpoint: false, ..strict.clone() };
        assert_eq!(pool_status(&no_mount_check, "").state, PoolState::NoQuota);
        let relaxed = Pool { quota: false, require_mountpoint: false, overcommit: 2.0, ..strict };
        let status = pool_status(&relaxed, "");
        assert_eq!(status.state, PoolState::Ready);
        assert!(status.capacity_bytes > 0);
        assert_eq!(status.allocatable_bytes, status.capacity_bytes * 2);
    }

    fn brought_back(mut volume: LocalPathVolume) -> LocalPathVolume {
        volume.metadata.labels = Some([(crate::restore_label().to_string(), "true".to_string())].into());
        volume
    }

    #[test]
    fn volume_brought_back_from_a_directory_is_orphaned_until_an_administrator_approves_it() {
        let mut orphan = brought_back(volume(None));
        orphan.spec.claim = Some(api::Claim { namespace: "infra".into(), name: "storage-postgres-0".into() });
        let (phase, message) = standing(&orphan);
        assert_eq!(phase, Phase::Orphaned);
        // Whoever looks at the volume is told what it waits for, and whose it says it was.
        let message = message.expect("an orphaned volume says what it waits for");
        assert!(message.contains("infra/storage-postgres-0"), "{message}");
        assert!(message.contains(&format!("{}=true", crate::restore_approval())), "{message}");
        // Its claim cannot ask for it: an administrator is the only one to approve.
        assert!(!message.contains("spec.volumeName"), "{message}");

        // The controller takes the mark off when the volume is approved.
        orphan.metadata.labels = None;
        assert_eq!(standing(&orphan), (Phase::Ready, None));
    }

    #[test]
    fn orphaned_volume_with_no_claim_on_record_waits_for_an_administrator_only() {
        let (phase, message) = standing(&brought_back(volume(None)));
        let message = message.unwrap();
        assert_eq!(phase, Phase::Orphaned);
        assert!(!message.contains("spec.volumeName"), "{message}");
        assert!(message.contains(&format!("{}=true", crate::restore_approval())), "{message}");
    }

    #[test]
    fn orphaned_volume_whose_directory_is_gone_is_lost_not_given_an_empty_one() {
        let orphan = brought_back(volume(Some(Phase::Orphaned)));
        assert_eq!(plan(&orphan, Some(&pool(PoolState::Ready)), false), Action::MarkLost);
    }

    #[test]
    fn agent_makes_a_pass_when_a_volume_it_brought_back_gets_its_persistent_volume() {
        let mut seen = HashMap::new();
        let mut orphan = brought_back(volume(Some(Phase::Orphaned)));
        assert!(worth_a_pass(&mut seen, &watcher::Event::Apply(orphan.clone())));
        assert!(!worth_a_pass(&mut seen, &watcher::Event::Apply(orphan.clone())));
        // The mark is gone: the phase is for the agent to write anew.
        orphan.metadata.labels = None;
        assert!(worth_a_pass(&mut seen, &watcher::Event::Apply(orphan.clone())));
    }

    fn at(text: &str) -> Time {
        Time(text.parse().unwrap())
    }

    /// The condition an agent writes of `volume` when it has brought it to `phase`, or could
    /// not look at it (`waiting`).
    fn condition(volume: &LocalPathVolume, pool: PoolState, phase: Phase, waiting: bool) -> (String, String) {
        let ready = readiness(volume, Some(&pool_in(pool)), phase, waiting, None, at("2026-11-27T10:00:00Z"));
        assert_eq!(ready.type_, "Ready");
        (ready.status, ready.reason)
    }

    fn pool_in(state: PoolState) -> PoolStatus {
        PoolStatus { state, ..pool(PoolState::Ready) }
    }

    fn told(status: &str, reason: &str) -> (String, String) {
        (status.to_string(), reason.to_string())
    }

    #[test]
    fn condition_says_in_a_word_what_the_phase_and_its_message_say() {
        let plain = volume(None);
        assert_eq!(condition(&plain, PoolState::Ready, Phase::Ready, false), told("True", "Provisioned"));
        assert_eq!(condition(&plain, PoolState::Ready, Phase::Lost, false), told("False", "DirectoryLost"));
        assert_eq!(condition(&plain, PoolState::Ready, Phase::Orphaned, false), told("False", "AwaitingApproval"));
        // The agent could not make the directory.
        assert_eq!(condition(&plain, PoolState::Missing, Phase::Pending, true), told("False", "PoolNotReady"));
        let mut malformed = volume(None);
        malformed.spec.dir_mode = "rwx".into();
        assert_eq!(condition(&malformed, PoolState::Ready, Phase::Pending, true), told("False", "InvalidDirMode"));
        assert_eq!(condition(&plain, PoolState::Ready, Phase::Pending, true), told("False", "CopyIncomplete"));
    }

    #[test]
    fn condition_of_a_volume_the_agent_cannot_look_at_is_unknown() {
        // The pool has failed its checks: the phase stays what it was, and that is all that is known.
        for phase in [Phase::Ready, Phase::Lost, Phase::Orphaned] {
            let was = volume(Some(phase));
            assert_eq!(condition(&was, PoolState::NoQuota, phase, true), told("Unknown", "PoolNotReady"), "{phase:?}");
        }
        let node_without_the_pool =
            readiness(&volume(None), None, Phase::Pending, true, None, at("2026-11-27T10:00:00Z"));
        assert_eq!(
            (node_without_the_pool.status.as_str(), node_without_the_pool.reason.as_str()),
            ("False", "PoolNotReady")
        );
    }

    #[test]
    fn condition_names_the_generation_it_is_about_and_carries_the_message() {
        let mut resized = volume(Some(Phase::Ready));
        resized.metadata.generation = Some(7);
        let ready =
            readiness(&resized, Some(&pool(PoolState::Ready)), Phase::Ready, false, None, at("2026-11-27T10:00:00Z"));
        assert_eq!((ready.observed_generation, ready.message.as_str()), (Some(7), ""));
        let why = "pool default at /mnt/default is Missing";
        let waiting = readiness(
            &resized,
            Some(&pool_in(PoolState::Missing)),
            Phase::Ready,
            true,
            Some(why),
            at("2026-11-27T10:00:00Z"),
        );
        assert_eq!((waiting.observed_generation, waiting.message.as_str()), (Some(7), why));
    }

    #[test]
    fn condition_keeps_the_time_of_its_last_change_while_its_status_stays() {
        let (first, later, much_later) =
            (at("2026-11-27T10:00:00Z"), at("2026-11-27T10:05:00Z"), at("2026-11-27T11:00:00Z"));
        let ready_pool = pool(PoolState::Ready);
        let mut seen = volume(Some(Phase::Ready));
        let report = |seen: &LocalPathVolume, phase: Phase, waiting: bool, now: &Time| {
            readiness(seen, Some(&ready_pool), phase, waiting, None, now.clone())
        };
        let became_ready = report(&seen, Phase::Ready, false, &first);
        assert_eq!(became_ready.last_transition_time, first);
        seen.status.as_mut().unwrap().conditions = vec![became_ready];

        // Resized: another generation, the same status.
        seen.metadata.generation = Some(2);
        let still_ready = report(&seen, Phase::Ready, false, &later);
        assert_eq!((still_ready.observed_generation, &still_ready.last_transition_time), (Some(2), &first));
        seen.status.as_mut().unwrap().conditions = vec![still_ready];

        let lost = report(&seen, Phase::Lost, false, &much_later);
        assert_eq!((lost.status.as_str(), &lost.last_transition_time), ("False", &much_later));
    }
}
