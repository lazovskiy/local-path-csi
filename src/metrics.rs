//! Metrics of the driver, in the Prometheus text format. Nothing is cached: a page is built from
//! the disk (agent) or from the custom resources (controller) when it is scraped.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::api::{LocalPathNode, LocalPathVolume, Mobility, PoolState, PoolStatus};
use crate::compression::Traffic;
use crate::controller::{allocated_in_pool, free_in_pool};
use crate::quota::Usage;

type Labels = Vec<(&'static str, String)>;

#[derive(Default)]
struct Page(String);

impl Page {
    fn family(&mut self, name: &str, kind: &str, help: &str, samples: impl IntoIterator<Item = (Labels, u64)>) {
        let _ = writeln!(self.0, "# HELP {name} {help}\n# TYPE {name} {kind}");
        for (labels, value) in samples {
            let _ = write!(self.0, "{name}");
            for (i, (label, text)) in labels.iter().enumerate() {
                let escaped = text.replace('\\', "\\\\").replace('"', "\\\"").replace('\n', "\\n");
                let _ = write!(self.0, "{}{label}=\"{escaped}\"", if i == 0 { '{' } else { ',' });
            }
            let _ = writeln!(self.0, "{} {value}", if labels.is_empty() { "" } else { "}" });
        }
    }
}

/// A volume directory as the agent finds it on the disk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VolumeSample {
    pub pool: String,
    pub volume: String,
    /// The claim of the volume, from its passport.
    pub claim: Option<(String, String)>,
    pub size_bytes: u64,
    pub inode_limit: u64,
    /// `None` in a pool without quota.
    pub usage: Option<Usage>,
}

/// The trash of a pool.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrashSample {
    pub pool: String,
    pub volumes: u64,
    /// `None` in a pool without quota.
    pub bytes: Option<u64>,
}

/// When the agent last went over its volumes to the end; when it started, until it has.
pub struct LastPass(Mutex<Instant>);

impl Default for LastPass {
    fn default() -> Self {
        Self(Mutex::new(Instant::now()))
    }
}

impl LastPass {
    /// A pass has come to its end.
    pub fn mark(&self) {
        self.set(Instant::now());
    }

    pub fn set(&self, at: Instant) {
        *self.0.lock().unwrap() = at;
    }

    /// By a clock that only moves forward: a time of day would have to be compared with the
    /// clock of whoever reads the metric.
    pub fn ago(&self) -> Duration {
        self.0.lock().unwrap().elapsed()
    }
}

/// What the agent has done since it started.
#[derive(Default)]
pub struct AgentCounters {
    pub last_pass: LastPass,
    pub reconcile_errors: AtomicU64,
    pub report_errors: AtomicU64,
    pub removed: AtomicU64,
    pub trashed: AtomicU64,
    pub purged: AtomicU64,
    pub restored: AtomicU64,
    /// Copies that became volumes of this node.
    pub accepted: AtomicU64,
    /// Volumes being received right now, the ones that wait for their turn included.
    pub transfers_running: AtomicU64,
    pub transfers_completed: AtomicU64,
    /// Failed attempts: each is tried again.
    pub transfers_failed: AtomicU64,
    /// Seconds from the moment a volume was sent here to its complete copy, summed up.
    pub transfer_seconds: AtomicU64,
    /// Bytes of the complete copies, summed up; pools without quota add nothing.
    pub transfer_bytes: AtomicU64,
    /// What came through the network for each volume this node has fetched.
    transfer_traffic: Mutex<BTreeMap<String, Arc<Traffic>>>,
}

impl AgentCounters {
    /// The traffic of `volume`: every attempt to fetch it adds to the same counters.
    pub fn traffic(&self, volume: &str) -> Arc<Traffic> {
        self.transfer_traffic.lock().unwrap().entry(volume.to_string()).or_default().clone()
    }

    /// Drops the traffic of the volumes `keep` does not hold on to: a volume that is gone, or
    /// went on to another node, must not stay on the metrics page for the life of the agent.
    pub fn retain_traffic(&self, keep: impl Fn(&str) -> bool) {
        self.transfer_traffic.lock().unwrap().retain(|volume, _| keep(volume));
    }
}

pub fn count(counter: &AtomicU64) {
    counter.fetch_add(1, Ordering::Relaxed);
}

fn state_name(state: PoolState) -> &'static str {
    match state {
        PoolState::Ready => "Ready",
        PoolState::Missing => "Missing",
        PoolState::NotMountpoint => "NotMountpoint",
        PoolState::NoQuota => "NoQuota",
    }
}

/// The metrics page of an agent.
pub fn agent(
    node: &str,
    pools: &[PoolStatus],
    volumes: &[VolumeSample],
    trash: &[TrashSample],
    counters: &AgentCounters,
) -> String {
    let mut page = Page::default();
    let pool = |p: &PoolStatus| -> Labels { vec![("node", node.to_string()), ("pool", p.name.clone())] };
    page.family(
        "local_path_csi_pool_ready",
        "gauge",
        "1 when the pool passed its checks and holds volumes; the label state names the reason otherwise.",
        pools.iter().map(|p| {
            let mut labels = pool(p);
            labels.push(("state", state_name(p.state).to_string()));
            (labels, u64::from(p.state == PoolState::Ready))
        }),
    );
    page.family(
        "local_path_csi_pool_capacity_bytes",
        "gauge",
        "Size of the filesystem of the pool.",
        pools.iter().map(|p| (pool(p), p.capacity_bytes)),
    );
    page.family(
        "local_path_csi_pool_allocatable_bytes",
        "gauge",
        "What volume sizes may add up to: the size of the filesystem times the overcommit ratio.",
        pools.iter().map(|p| (pool(p), p.allocatable_bytes)),
    );
    page.family(
        "local_path_csi_pool_available_bytes",
        "gauge",
        "Free space of the filesystem of the pool.",
        pools.iter().map(|p| (pool(p), p.available_bytes)),
    );
    let trashed = |t: &TrashSample| -> Labels { vec![("node", node.to_string()), ("pool", t.pool.clone())] };
    page.family(
        "local_path_csi_pool_trash_volumes",
        "gauge",
        "Removed volumes kept in the trash of the pool.",
        trash.iter().map(|t| (trashed(t), t.volumes)),
    );
    page.family(
        "local_path_csi_pool_trash_bytes",
        "gauge",
        "Space the trash of the pool occupies; absent for a pool without quota.",
        trash.iter().filter_map(|t| Some((trashed(t), t.bytes?))),
    );

    let volume = |v: &VolumeSample| -> Labels {
        let (namespace, claim) = v.claim.clone().unwrap_or_default();
        vec![
            ("node", node.to_string()),
            ("pool", v.pool.clone()),
            ("persistentvolume", v.volume.clone()),
            ("namespace", namespace),
            ("persistentvolumeclaim", claim),
        ]
    };
    page.family(
        "local_path_csi_volume_size_bytes",
        "gauge",
        "Size of the volume: its quota limit, or its reservation in a pool without quota.",
        volumes.iter().map(|v| (volume(v), v.size_bytes)),
    );
    page.family(
        "local_path_csi_volume_used_bytes",
        "gauge",
        "Space the volume occupies, from its quota; absent in a pool without quota.",
        volumes.iter().filter_map(|v| Some((volume(v), v.usage?.used_bytes))),
    );
    page.family(
        "local_path_csi_volume_inodes_used",
        "gauge",
        "Files and directories of the volume, from its quota; absent in a pool without quota.",
        volumes.iter().filter_map(|v| Some((volume(v), v.usage?.used_inodes))),
    );
    page.family(
        "local_path_csi_volume_inodes_limit",
        "gauge",
        "Limit on the files and directories of the volume; absent when there is none.",
        volumes.iter().filter(|v| v.inode_limit > 0).map(|v| (volume(v), v.inode_limit)),
    );

    let load = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
    let labelled = |label: &'static str, value: &str, counter: &AtomicU64| -> (Labels, u64) {
        (vec![("node", node.to_string()), (label, value.to_string())], load(counter))
    };
    page.family(
        "local_path_csi_agent_seconds_since_last_pass",
        "gauge",
        "Seconds since the agent went over the volumes of its node to the end, which it does at least every 30 seconds; counted from its start until it has.",
        [(vec![("node", node.to_string())], counters.last_pass.ago().as_secs())],
    );
    page.family(
        "local_path_csi_agent_errors_total",
        "counter",
        "Failures of the agent: reconcile is bringing a volume directory to its LocalPathVolume, report is writing the LocalPathNode.",
        [labelled("operation", "reconcile", &counters.reconcile_errors), labelled("operation", "report", &counters.report_errors)],
    );
    page.family(
        "local_path_csi_agent_volumes_total",
        "counter",
        "Volume directories the agent removed, moved to the trash, purged from the trash, or brought back as volumes.",
        [
            labelled("action", "removed", &counters.removed),
            labelled("action", "trashed", &counters.trashed),
            labelled("action", "purged", &counters.purged),
            labelled("action", "restored", &counters.restored),
            labelled("action", "received", &counters.accepted),
        ],
    );
    let plain = |counter: &AtomicU64| -> [(Labels, u64); 1] { [(vec![("node", node.to_string())], load(counter))] };
    page.family(
        "local_path_csi_transfers_running",
        "gauge",
        "Volumes the node is receiving from other nodes right now, the ones that wait for their turn included.",
        plain(&counters.transfers_running),
    );
    page.family(
        "local_path_csi_transfers_total",
        "counter",
        "Volumes the node has received (completed) and attempts that failed and were tried again (failed).",
        [
            labelled("result", "completed", &counters.transfers_completed),
            labelled("result", "failed", &counters.transfers_failed),
        ],
    );
    page.family(
        "local_path_csi_transfer_seconds_total",
        "counter",
        "Time the received volumes took, from the moment a volume was sent to the node to its complete copy.",
        plain(&counters.transfer_seconds),
    );
    page.family(
        "local_path_csi_transfer_received_bytes_total",
        "counter",
        "Size of the received volumes; pools without quota add nothing.",
        plain(&counters.transfer_bytes),
    );
    let traffic: Vec<(Labels, (u64, u64))> = {
        let traffic = counters.transfer_traffic.lock().unwrap();
        let labels = |volume: &String| vec![("node", node.to_string()), ("persistentvolume", volume.clone())];
        traffic.iter().map(|(volume, traffic)| (labels(volume), traffic.totals())).collect()
    };
    page.family(
        "local_path_csi_transfer_uncompressed_bytes_total",
        "counter",
        "Bytes rsync sent to the node for the volume: what its copies would have taken of the network without compression.",
        traffic.iter().map(|(labels, (uncompressed, _))| (labels.clone(), *uncompressed)),
    );
    page.family(
        "local_path_csi_transfer_compressed_bytes_total",
        "counter",
        "Bytes that crossed the network to the node for the volume; the same as uncompressed when compression is off.",
        traffic.iter().map(|(labels, (_, compressed))| (labels.clone(), *compressed)),
    );
    page.0
}

/// The metrics page of the controller: the cluster-wide view, from the custom resources.
pub fn controller(
    volumes: &[LocalPathVolume],
    nodes: &[LocalPathNode],
    unix_now: u64,
    agent_timeout_seconds: u64,
) -> String {
    let mut page = Page::default();

    let mut counts: BTreeMap<(String, String, &str, String), u64> = BTreeMap::new();
    for volume in volumes {
        let mobility = match volume.spec.mobility {
            Mobility::Pinned => "Pinned",
            Mobility::Floating => "Floating",
        };
        let phase = volume.phase().map(|p| format!("{p:?}")).unwrap_or_else(|| "Pending".into());
        *counts.entry((volume.data_node().to_string(), volume.spec.pool.clone(), mobility, phase)).or_default() += 1;
    }
    page.family(
        "local_path_csi_volumes",
        "gauge",
        "Volumes by the node that holds their data, pool, mobility and phase.",
        counts.iter().map(|((node, pool, mobility, phase), n)| {
            let labels: Labels = vec![
                ("node", node.clone()),
                ("pool", pool.clone()),
                ("mobility", mobility.to_string()),
                ("phase", phase.clone()),
            ];
            (labels, *n)
        }),
    );

    // Every node that reports, and every node that holds volumes whether it reports or not.
    let reports: BTreeMap<&str, &LocalPathNode> =
        nodes.iter().filter_map(|n| Some((n.metadata.name.as_deref()?, n))).collect();
    let mut names: BTreeSet<&str> = reports.keys().copied().collect();
    names.extend(volumes.iter().map(|v| v.data_node()));
    let age = |node: &str| reports.get(node).and_then(|n| n.status.as_ref()).and_then(|s| s.report_age(unix_now));
    let alive = |node: &str| age(node).is_some_and(|age| age <= agent_timeout_seconds);
    let node_label = |node: &str| -> Labels { vec![("node", node.to_string())] };
    page.family(
        "local_path_csi_node_agent_alive",
        "gauge",
        "1 when the agent of the node has reported within the timeout of the controller.",
        names.iter().map(|n| (node_label(n), u64::from(alive(n)))),
    );
    page.family(
        "local_path_csi_node_report_age_seconds",
        "gauge",
        "Time since the agent of the node reported; absent for a node that has never reported.",
        names.iter().filter_map(|n| Some((node_label(n), age(n)?))),
    );
    page.family(
        "local_path_csi_node_volumes_without_agent",
        "gauge",
        "Volumes whose data is on the node while its agent is not reporting: they cannot be created, resized, deleted or brought to a pod.",
        names.iter().map(|n| {
            let stranded = if alive(n) { 0 } else { volumes.iter().filter(|v| v.data_node() == *n).count() as u64 };
            (node_label(n), stranded)
        }),
    );

    let mut pools: BTreeSet<(&str, &str)> = volumes.iter().map(|v| (v.data_node(), v.spec.pool.as_str())).collect();
    for (name, node) in &reports {
        pools.extend(node.status.iter().flat_map(|s| s.pools.iter()).map(|p| (*name, p.name.as_str())));
    }
    let pool_label =
        |node: &str, pool: &str| -> Labels { vec![("node", node.to_string()), ("pool", pool.to_string())] };
    page.family(
        "local_path_csi_pool_allocated_bytes",
        "gauge",
        "Sum of the sizes of the volumes of the pool, the ones on their way to the node included.",
        pools.iter().map(|(node, pool)| (pool_label(node, pool), allocated_in_pool(volumes, node, pool))),
    );
    // A volume that is sent to another node than the one that holds its data.
    let moving: Vec<(&LocalPathVolume, &str)> = volumes.iter().filter_map(|v| Some((v, v.destination()?))).collect();
    let move_labels = |volume: &LocalPathVolume, target: &str| -> Labels {
        let claim = volume.spec.claim.as_ref();
        vec![
            ("persistentvolume", volume.metadata.name.clone().unwrap_or_default()),
            ("namespace", claim.map(|c| c.namespace.clone()).unwrap_or_default()),
            ("persistentvolumeclaim", claim.map(|c| c.name.clone()).unwrap_or_default()),
            ("source", volume.data_node().to_string()),
            ("target", target.to_string()),
        ]
    };
    page.family(
        "local_path_csi_volume_moving",
        "gauge",
        "1 for a volume on its way to another node; state is what the receiving node reports: Pending (nothing yet), Queued, Copying, Copied or Failed.",
        moving.iter().map(|(volume, target)| {
            let state = volume.transfer().map(|t| format!("{:?}", t.state)).unwrap_or_else(|| "Pending".into());
            let mut labels = move_labels(volume, target);
            labels.push(("state", state));
            (labels, 1)
        }),
    );
    page.family(
        "local_path_csi_volume_moving_received_bytes",
        "gauge",
        "What has arrived of a volume on its way to another node; absent in a pool without quota.",
        moving
            .iter()
            .filter_map(|(volume, target)| Some((move_labels(volume, target), volume.transfer()?.received_bytes?))),
    );
    page.family(
        "local_path_csi_pool_free_bytes",
        "gauge",
        "What new volumes and resizes may still take in the pool, as the scheduler is told: 0 for a pool that is not ready or whose agent is not reporting.",
        pools.iter().map(|(node, pool)| {
            let free = free_in_pool(reports.get(node).copied(), volumes, node, pool, unix_now, agent_timeout_seconds).unwrap_or(0);
            (pool_label(node, pool), free)
        }),
    );
    page.0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::{Claim, LocalPathNodeStatus, LocalPathVolumeSpec, LocalPathVolumeStatus, Phase, time};

    const GIB: u64 = 1 << 30;

    fn pool_status(name: &str, state: PoolState) -> PoolStatus {
        PoolStatus {
            name: name.into(),
            path: format!("/mnt/{name}"),
            state,
            quota: true,
            fs_type: Some("xfs".into()),
            capacity_bytes: 100 * GIB,
            allocatable_bytes: 150 * GIB,
            available_bytes: 60 * GIB,
        }
    }

    fn has(page: &str, line: &str) {
        assert!(page.lines().any(|l| l == line), "no line {line:?} in:\n{page}");
    }

    fn lacks(page: &str, prefix: &str) {
        assert!(!page.lines().any(|l| l.starts_with(prefix)), "a line starts with {prefix:?} in:\n{page}");
    }

    #[test]
    fn agent_page_counts_the_traffic_of_each_volume_it_fetched_until_told_to_forget_it() {
        let counters = AgentCounters::default();
        counters.traffic("pv-1").uncompressed.fetch_add(3000, Ordering::Relaxed);
        counters.traffic("pv-1").compressed.fetch_add(1000, Ordering::Relaxed);
        // The next attempt at the same volume adds to the same counters.
        counters.traffic("pv-1").uncompressed.fetch_add(500, Ordering::Relaxed);
        counters.traffic("pv-2").compressed.fetch_add(7, Ordering::Relaxed);
        let page = agent("n1", &[], &[], &[], &counters);
        has(&page, r#"local_path_csi_transfer_uncompressed_bytes_total{node="n1",persistentvolume="pv-1"} 3500"#);
        has(&page, r#"local_path_csi_transfer_compressed_bytes_total{node="n1",persistentvolume="pv-1"} 1000"#);
        has(&page, r#"local_path_csi_transfer_uncompressed_bytes_total{node="n1",persistentvolume="pv-2"} 0"#);
        has(&page, r#"local_path_csi_transfer_compressed_bytes_total{node="n1",persistentvolume="pv-2"} 7"#);

        counters.retain_traffic(|volume| volume == "pv-2");
        let page = agent("n1", &[], &[], &[], &counters);
        lacks(&page, r#"local_path_csi_transfer_uncompressed_bytes_total{node="n1",persistentvolume="pv-1""#);
        lacks(&page, r#"local_path_csi_transfer_compressed_bytes_total{node="n1",persistentvolume="pv-1""#);
        has(&page, r#"local_path_csi_transfer_compressed_bytes_total{node="n1",persistentvolume="pv-2"} 7"#);
    }

    #[test]
    fn agent_page_describes_pools_volumes_and_trash_of_the_node() {
        let pools = [pool_status("default", PoolState::Ready), pool_status("broken", PoolState::NoQuota)];
        let volumes = [
            VolumeSample {
                pool: "default".into(),
                volume: "pvc-1".into(),
                claim: Some(("infra".into(), "data".into())),
                size_bytes: 10 * GIB,
                inode_limit: 1000,
                usage: Some(Usage { limit_bytes: 10 * GIB, used_bytes: 4 * GIB, limit_inodes: 1000, used_inodes: 37 }),
            },
            VolumeSample {
                pool: "plain".into(),
                volume: "pvc-2".into(),
                claim: None,
                size_bytes: GIB,
                inode_limit: 0,
                usage: None,
            },
        ];
        let trash = [
            TrashSample { pool: "default".into(), volumes: 2, bytes: Some(3 * GIB) },
            TrashSample { pool: "plain".into(), volumes: 1, bytes: None },
        ];
        let counters = AgentCounters::default();
        count(&counters.reconcile_errors);
        count(&counters.trashed);
        count(&counters.trashed);
        let page = agent("a", &pools, &volumes, &trash, &counters);

        has(&page, r#"local_path_csi_pool_ready{node="a",pool="default",state="Ready"} 1"#);
        has(&page, r#"local_path_csi_pool_ready{node="a",pool="broken",state="NoQuota"} 0"#);
        has(&page, r#"local_path_csi_pool_capacity_bytes{node="a",pool="default"} 107374182400"#);
        has(&page, r#"local_path_csi_pool_allocatable_bytes{node="a",pool="default"} 161061273600"#);
        has(&page, r#"local_path_csi_pool_available_bytes{node="a",pool="default"} 64424509440"#);
        has(&page, r#"local_path_csi_pool_trash_volumes{node="a",pool="default"} 2"#);
        has(&page, r#"local_path_csi_pool_trash_bytes{node="a",pool="default"} 3221225472"#);
        has(&page, r#"local_path_csi_pool_trash_volumes{node="a",pool="plain"} 1"#);
        lacks(&page, r#"local_path_csi_pool_trash_bytes{node="a",pool="plain"}"#);

        let pvc1 =
            r#"{node="a",pool="default",persistentvolume="pvc-1",namespace="infra",persistentvolumeclaim="data"}"#;
        has(&page, &format!("local_path_csi_volume_size_bytes{pvc1} 10737418240"));
        has(&page, &format!("local_path_csi_volume_used_bytes{pvc1} 4294967296"));
        has(&page, &format!("local_path_csi_volume_inodes_used{pvc1} 37"));
        has(&page, &format!("local_path_csi_volume_inodes_limit{pvc1} 1000"));
        // A volume without quota has a size and nothing measured; one without a claim has empty labels.
        let pvc2 = r#"{node="a",pool="plain",persistentvolume="pvc-2",namespace="",persistentvolumeclaim=""}"#;
        has(&page, &format!("local_path_csi_volume_size_bytes{pvc2} 1073741824"));
        lacks(&page, &format!("local_path_csi_volume_used_bytes{pvc2}"));
        lacks(&page, &format!("local_path_csi_volume_inodes_limit{pvc2}"));

        has(&page, r#"local_path_csi_agent_errors_total{node="a",operation="reconcile"} 1"#);
        has(&page, r#"local_path_csi_agent_errors_total{node="a",operation="report"} 0"#);
        has(&page, r#"local_path_csi_agent_volumes_total{node="a",action="trashed"} 2"#);
        has(&page, "# TYPE local_path_csi_agent_volumes_total counter");
        has(&page, "# TYPE local_path_csi_volume_used_bytes gauge");
    }

    /// The node reports apart from the passes, so that a long removal does not make it look dead:
    /// a pass that never ends shows here and nowhere else.
    #[test]
    fn agent_page_tells_how_long_ago_the_agent_went_over_its_volumes() {
        let counters = AgentCounters::default();
        counters.last_pass.set(Instant::now() - Duration::from_secs(100));
        let page = agent("a", &[], &[], &[], &counters);
        has(&page, r#"local_path_csi_agent_seconds_since_last_pass{node="a"} 100"#);
        has(&page, "# TYPE local_path_csi_agent_seconds_since_last_pass gauge");

        counters.last_pass.mark();
        has(&agent("a", &[], &[], &[], &counters), r#"local_path_csi_agent_seconds_since_last_pass{node="a"} 0"#);
    }

    #[test]
    fn agent_that_has_finished_no_pass_yet_counts_from_its_start() {
        let counters = AgentCounters::default();
        has(&agent("a", &[], &[], &[], &counters), r#"local_path_csi_agent_seconds_since_last_pass{node="a"} 0"#);
    }

    #[test]
    fn label_values_are_escaped() {
        let mut page = Page::default();
        page.family("m", "gauge", "h", [(vec![("l", "a\"b\\c\nd".to_string())], 1)]);
        has(&page.0, r#"m{l="a\"b\\c\nd"} 1"#);
    }

    fn volume(name: &str, node: &str, size: u64, mobility: Mobility, phase: Option<Phase>) -> LocalPathVolume {
        let spec = LocalPathVolumeSpec {
            pool: "default".into(),
            size_bytes: size,
            mobility,
            node: node.into(),
            inodes_per_gib: 0,
            dir_mode: "0770".into(),
            delete_grace_seconds: 0,
            access_mode: Default::default(),
            storage_class: None,
            claim: Some(Claim { namespace: "infra".into(), name: name.into() }),
        };
        let mut volume = LocalPathVolume::new(name, spec);
        volume.status = Some(LocalPathVolumeStatus { node: Some(node.into()), phase, ..Default::default() });
        volume
    }

    fn node(name: &str, reported_seconds_ago: Option<u64>, now: u64) -> LocalPathNode {
        let mut node = LocalPathNode::new(name, Default::default());
        node.status = Some(LocalPathNodeStatus {
            pools: vec![pool_status("default", PoolState::Ready)],
            reported_at: reported_seconds_ago.map(|ago| time(now - ago)),
            ..Default::default()
        });
        node
    }

    #[test]
    fn controller_page_shows_what_the_scheduler_is_told_and_which_volumes_are_stranded() {
        let now = 1_795_000_000;
        let volumes = [
            volume("pv-1", "a", 30 * GIB, Mobility::Pinned, Some(Phase::Ready)),
            volume("pv-2", "a", 20 * GIB, Mobility::Pinned, Some(Phase::Ready)),
            volume("pv-3", "a", 10 * GIB, Mobility::Floating, None),
            volume("pv-4", "silent", 40 * GIB, Mobility::Pinned, Some(Phase::Ready)),
            volume("pv-5", "silent", 5 * GIB, Mobility::Pinned, Some(Phase::Lost)),
            volume("pv-6", "vanished", GIB, Mobility::Pinned, Some(Phase::Ready)),
        ];
        let nodes = [node("a", Some(10), now), node("silent", Some(500), now), node("idle", Some(1), now)];
        let page = controller(&volumes, &nodes, now, 120);

        has(&page, r#"local_path_csi_volumes{node="a",pool="default",mobility="Pinned",phase="Ready"} 2"#);
        has(&page, r#"local_path_csi_volumes{node="a",pool="default",mobility="Floating",phase="Pending"} 1"#);
        has(&page, r#"local_path_csi_volumes{node="silent",pool="default",mobility="Pinned",phase="Lost"} 1"#);

        has(&page, r#"local_path_csi_node_agent_alive{node="a"} 1"#);
        has(&page, r#"local_path_csi_node_agent_alive{node="silent"} 0"#);
        has(&page, r#"local_path_csi_node_agent_alive{node="idle"} 1"#);
        // A node that holds volumes and has no LocalPathNode at all is listed as well.
        has(&page, r#"local_path_csi_node_agent_alive{node="vanished"} 0"#);
        has(&page, r#"local_path_csi_node_report_age_seconds{node="silent"} 500"#);
        lacks(&page, r#"local_path_csi_node_report_age_seconds{node="vanished"}"#);

        has(&page, r#"local_path_csi_node_volumes_without_agent{node="a"} 0"#);
        has(&page, r#"local_path_csi_node_volumes_without_agent{node="silent"} 2"#);
        has(&page, r#"local_path_csi_node_volumes_without_agent{node="vanished"} 1"#);
        has(&page, r#"local_path_csi_node_volumes_without_agent{node="idle"} 0"#);

        has(&page, r#"local_path_csi_pool_allocated_bytes{node="a",pool="default"} 64424509440"#);
        // 150 GiB allocatable minus 60 GiB of volumes.
        has(&page, r#"local_path_csi_pool_free_bytes{node="a",pool="default"} 96636764160"#);
        has(&page, r#"local_path_csi_pool_free_bytes{node="idle",pool="default"} 161061273600"#);
        // Nothing is placed on a node whose agent is silent, whatever its last report said.
        has(&page, r#"local_path_csi_pool_free_bytes{node="silent",pool="default"} 0"#);
        has(&page, r#"local_path_csi_pool_allocated_bytes{node="silent",pool="default"} 48318382080"#);
        has(&page, r#"local_path_csi_pool_free_bytes{node="vanished",pool="default"} 0"#);
    }
}
