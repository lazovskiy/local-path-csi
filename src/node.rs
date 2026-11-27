//! CSI Node service: makes a volume appear at the path the kubelet hands to the pod.

use std::collections::HashMap;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tonic::{Request, Response, Status};

use crate::agent::volume_path;
use crate::config::Config;
use crate::controller::{CONTEXT_POOL, check_access_type};
use crate::csi::node_server::Node;
use crate::csi::node_service_capability::{Rpc, Type, rpc};
use crate::csi::volume_usage::Unit;
use crate::csi::{
    NodeExpandVolumeRequest, NodeExpandVolumeResponse, NodeGetCapabilitiesRequest, NodeGetCapabilitiesResponse,
    NodeGetInfoRequest, NodeGetInfoResponse, NodeGetVolumeStatsRequest, NodeGetVolumeStatsResponse,
    NodePublishVolumeRequest, NodePublishVolumeResponse, NodeServiceCapability, NodeStageVolumeRequest,
    NodeStageVolumeResponse, NodeUnpublishVolumeRequest, NodeUnpublishVolumeResponse, NodeUnstageVolumeRequest,
    NodeUnstageVolumeResponse, Topology, VolumeUsage,
};
use crate::{blocking, mount, pool, topology_node, topology_storage, volumes};

pub struct NodeService {
    node_name: String,
    config: Arc<Config>,
}

impl NodeService {
    pub fn new(node_name: String, config: Arc<Config>) -> Self {
        Self { node_name, config }
    }
}

fn internal(what: &str, path: &str, error: std::io::Error) -> Status {
    Status::internal(format!("{what} {path}: {error}"))
}

/// What it takes for `target` to show a volume.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Publish {
    /// Nothing is mounted there.
    Bind,
    /// The volume is mounted there as asked.
    Nothing,
    /// The volume is mounted there, writable when it has to be read-only or the other way round.
    SetReadOnly(bool),
}

/// Something else is mounted where a volume has to appear.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{} is mounted and does not show {}", .target.display(), .volume.display())]
struct Occupied {
    target: PathBuf,
    /// The directory of the volume.
    volume: PathBuf,
}

/// Decides what to do about `target`, where the directory `source` of a volume has to appear.
/// The kubelet repeats its call after a restart of the agent, and a mount made by an earlier
/// call is still there; but a path that is mounted is not yet a path that shows the volume, the
/// way it was asked for. `Err` when something else is mounted there: that is not touched.
fn publish_plan(mountinfo: &str, source: &Path, target: &Path, read_only: bool) -> Result<Publish, Occupied> {
    let Some(mount) = pool::find_mount(mountinfo, target) else { return Ok(Publish::Bind) };
    if !pool::binds_of(mountinfo, source).iter().any(|at| at == target) {
        return Err(Occupied { target: target.to_path_buf(), volume: source.to_path_buf() });
    }
    Ok(if mount.read_only == read_only { Publish::Nothing } else { Publish::SetReadOnly(read_only) })
}

/// Whether `path` shows volume `volume` of this node: its directory in one of the pools is
/// mounted there.
fn published_at(config: &Config, mountinfo: &str, volume: &str, path: &Path) -> bool {
    let shows = |pool| pool::binds_of(mountinfo, &volumes::data_dir(pool, volume)).iter().any(|at| at == path);
    volumes::valid_name(volume) && config.pools.iter().any(shows)
}

/// The statistics of the filesystem at the path of `request`, which is the volume as its pod
/// sees it. A path the volume is not mounted at - it was unpublished, or never was there - is a
/// plain directory, and its statistics would be the ones of the disk of the node, given out
/// for the volume.
fn statistics(config: &Config, request: &NodeGetVolumeStatsRequest) -> Result<pool::Stats, Status> {
    let missing = |what: &'static str| {
        move |e: std::io::Error| match e.kind() {
            ErrorKind::NotFound => Status::not_found(format!("{} does not exist", request.volume_path)),
            _ => internal(what, &request.volume_path, e),
        }
    };
    // The path as the kernel names it: mountinfo has no symbolic links in it.
    let path = std::fs::canonicalize(&request.volume_path).map_err(missing("resolve"))?;
    let mountinfo = std::fs::read_to_string("/proc/self/mountinfo").map_err(|e| internal("read", "mountinfo", e))?;
    if !published_at(config, &mountinfo, &request.volume_id, &path) {
        return Err(Status::not_found(format!(
            "volume {} is not published at {}",
            request.volume_id, request.volume_path
        )));
    }
    pool::stats(&path).map_err(missing("statfs"))
}

/// Makes the volume of `request` appear at its target path. Everything here waits for a disk.
fn publish(config: &Config, node_name: &str, request: &NodePublishVolumeRequest) -> Result<(), Status> {
    let pool_name = request.volume_context.get(CONTEXT_POOL).map(String::as_str).unwrap_or_default();
    let source = volume_path(config, pool_name, &request.volume_id).ok_or_else(|| {
        Status::not_found(format!(
            "volume {} has no directory in pool {pool_name:?} on node {node_name}",
            request.volume_id
        ))
    })?;
    let target = Path::new(&request.target_path);
    std::fs::create_dir_all(target).map_err(|e| internal("create", &request.target_path, e))?;
    let mountinfo = std::fs::read_to_string("/proc/self/mountinfo").map_err(|e| internal("read", "mountinfo", e))?;
    let plan = publish_plan(&mountinfo, &source, target, request.readonly)
        .map_err(|occupied| Status::failed_precondition(occupied.to_string()))?;
    match plan {
        Publish::Nothing => {}
        Publish::Bind => {
            mount::bind(&source, target, request.readonly).map_err(|e| internal("mount", &request.target_path, e))?;
            tracing::info!(volume = %request.volume_id, target = %request.target_path, "published");
        }
        Publish::SetReadOnly(read_only) => {
            mount::set_read_only(target, read_only).map_err(|e| internal("remount", &request.target_path, e))?;
            tracing::info!(volume = %request.volume_id, target = %request.target_path, read_only, "the mount is brought in line with the request");
        }
    }
    Ok(())
}

/// Takes the volume away from the target path of `request`, and the path with it.
fn unpublish(request: &NodeUnpublishVolumeRequest) -> Result<(), Status> {
    let target = Path::new(&request.target_path);
    mount::unmount(target).map_err(|e| internal("unmount", &request.target_path, e))?;
    // remove_dir, not remove_dir_all: if the unmount silently left the volume in place, its
    // data must survive.
    match std::fs::remove_dir(target) {
        Ok(()) => {}
        Err(e) if e.kind() == ErrorKind::NotFound => {}
        Err(e) => return Err(internal("remove", &request.target_path, e)),
    }
    tracing::info!(volume = %request.volume_id, target = %request.target_path, "unpublished");
    Ok(())
}

#[tonic::async_trait]
impl Node for NodeService {
    async fn node_get_info(&self, _: Request<NodeGetInfoRequest>) -> Result<Response<NodeGetInfoResponse>, Status> {
        Ok(Response::new(NodeGetInfoResponse {
            node_id: self.node_name.clone(),
            max_volumes_per_node: 0,
            accessible_topology: Some(Topology {
                segments: HashMap::from([
                    (topology_node().to_string(), self.node_name.clone()),
                    (topology_storage().to_string(), "true".to_string()),
                ]),
            }),
        }))
    }

    async fn node_get_capabilities(
        &self,
        _: Request<NodeGetCapabilitiesRequest>,
    ) -> Result<Response<NodeGetCapabilitiesResponse>, Status> {
        let capability =
            |kind: rpc::Type| NodeServiceCapability { r#type: Some(Type::Rpc(Rpc { r#type: kind.into() })) };
        // The second one is what the controller declares too: the kubelet asks the node how to
        // name the access mode of a ReadWriteOncePod claim.
        let capabilities = vec![capability(rpc::Type::GetVolumeStats), capability(rpc::Type::SingleNodeMultiWriter)];
        Ok(Response::new(NodeGetCapabilitiesResponse { capabilities }))
    }

    async fn node_publish_volume(
        &self,
        request: Request<NodePublishVolumeRequest>,
    ) -> Result<Response<NodePublishVolumeResponse>, Status> {
        let request = request.into_inner();
        if request.volume_id.is_empty() || request.target_path.is_empty() {
            return Err(Status::invalid_argument("volume id and target path are required"));
        }
        // The mount options of a PersistentVolume are not looked at here: a volume made before
        // they were refused at its creation was published without them and still is.
        if let Some(capability) = &request.volume_capability {
            check_access_type(capability)?;
        }
        let (config, node_name) = (self.config.clone(), self.node_name.clone());
        blocking(move || publish(&config, &node_name, &request)).await?;
        Ok(Response::new(NodePublishVolumeResponse {}))
    }

    async fn node_unpublish_volume(
        &self,
        request: Request<NodeUnpublishVolumeRequest>,
    ) -> Result<Response<NodeUnpublishVolumeResponse>, Status> {
        let request = request.into_inner();
        if request.target_path.is_empty() {
            return Err(Status::invalid_argument("target path is required"));
        }
        blocking(move || unpublish(&request)).await?;
        Ok(Response::new(NodeUnpublishVolumeResponse {}))
    }

    async fn node_get_volume_stats(
        &self,
        request: Request<NodeGetVolumeStatsRequest>,
    ) -> Result<Response<NodeGetVolumeStatsResponse>, Status> {
        let request = request.into_inner();
        if request.volume_id.is_empty() || request.volume_path.is_empty() {
            return Err(Status::invalid_argument("volume id and volume path are required"));
        }
        let config = self.config.clone();
        let stats = blocking(move || statistics(&config, &request)).await?;
        let usage = |unit: Unit, total: u64, available: u64, used: u64| VolumeUsage {
            unit: unit.into(),
            total: total as i64,
            available: available as i64,
            used: used as i64,
        };
        Ok(Response::new(NodeGetVolumeStatsResponse {
            usage: vec![
                usage(Unit::Bytes, stats.total_bytes, stats.available_bytes, stats.used_bytes),
                usage(Unit::Inodes, stats.total_inodes, stats.free_inodes, stats.used_inodes),
            ],
            volume_condition: None,
        }))
    }

    async fn node_stage_volume(
        &self,
        _: Request<NodeStageVolumeRequest>,
    ) -> Result<Response<NodeStageVolumeResponse>, Status> {
        Err(Status::unimplemented("NodeStageVolume"))
    }

    async fn node_unstage_volume(
        &self,
        _: Request<NodeUnstageVolumeRequest>,
    ) -> Result<Response<NodeUnstageVolumeResponse>, Status> {
        Err(Status::unimplemented("NodeUnstageVolume"))
    }

    async fn node_expand_volume(
        &self,
        _: Request<NodeExpandVolumeRequest>,
    ) -> Result<Response<NodeExpandVolumeResponse>, Status> {
        Err(Status::unimplemented("NodeExpandVolume"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::csi::{VolumeCapability, volume_capability};

    fn service() -> NodeService {
        let config = Config { pools: Vec::new(), project_id_base: 1000, transfer: Default::default() };
        NodeService::new("a".into(), Arc::new(config))
    }

    #[test]
    fn path_that_is_mounted_is_checked_for_what_it_shows_and_how() {
        let mountinfo = "\
22 1 259:2 / / rw,relatime shared:1 - ext4 /dev/mapper/main-root rw
901 22 253:3 / /mnt/pool rw,relatime shared:40 - xfs /dev/mapper/nvme-pool rw,prjquota
950 22 253:3 /volumes/pv-1/data /pods/uid-1/mount rw,relatime - xfs /dev/mapper/nvme-pool rw,prjquota
951 22 253:3 /volumes/pv-1/data /pods/uid-2/mount ro,relatime - xfs /dev/mapper/nvme-pool rw,prjquota
952 22 253:3 /volumes/pv-2/data /pods/uid-3/mount rw,relatime - xfs /dev/mapper/nvme-pool rw,prjquota
953 22 259:2 /var/lib/other /pods/uid-4/mount rw,relatime - ext4 /dev/mapper/main-root rw";
        let source = Path::new("/mnt/pool/volumes/pv-1/data");
        let plan = |target: &str, read_only| publish_plan(mountinfo, source, Path::new(target), read_only);
        assert_eq!(plan("/pods/uid-9/mount", false), Ok(Publish::Bind));
        assert_eq!(plan("/pods/uid-9/mount", true), Ok(Publish::Bind));
        // The call is repeated: the mount of the first one is there.
        assert_eq!(plan("/pods/uid-1/mount", false), Ok(Publish::Nothing));
        assert_eq!(plan("/pods/uid-2/mount", true), Ok(Publish::Nothing));
        // A mount that was to be read-only and was left writable: the agent died between the
        // two calls a read-only bind takes.
        assert_eq!(plan("/pods/uid-1/mount", true), Ok(Publish::SetReadOnly(true)));
        assert_eq!(plan("/pods/uid-2/mount", false), Ok(Publish::SetReadOnly(false)));
        // Another volume, and something that is no volume at all.
        for target in ["/pods/uid-3/mount", "/pods/uid-4/mount"] {
            let refusal = plan(target, false).unwrap_err().to_string();
            assert!(refusal.contains("does not show /mnt/pool/volumes/pv-1/data"), "{refusal}");
        }
    }

    /// A path nothing is at, different for every test.
    fn nowhere(test: &str) -> String {
        std::env::temp_dir()
            .join(format!("local-path-csi-node-{}-{test}/missing", std::process::id()))
            .display()
            .to_string()
    }

    #[tokio::test]
    async fn volume_without_a_directory_on_the_node_is_not_published() {
        let request = NodePublishVolumeRequest {
            volume_id: "pv-1".into(),
            target_path: nowhere("publish"),
            volume_context: HashMap::from([(CONTEXT_POOL.to_string(), "default".to_string())]),
            ..Default::default()
        };
        let target = request.target_path.clone();
        let error = service().node_publish_volume(Request::new(request)).await.unwrap_err();
        assert_eq!(error.code(), tonic::Code::NotFound, "{}", error.message());
        assert!(!Path::new(&target).exists(), "nothing is made for a volume that is not there");
    }

    #[tokio::test]
    async fn statistics_of_a_path_that_is_not_there_are_not_found() {
        let request =
            NodeGetVolumeStatsRequest { volume_id: "pv-1".into(), volume_path: nowhere("stats"), ..Default::default() };
        let error = service().node_get_volume_stats(Request::new(request)).await.unwrap_err();
        assert_eq!(error.code(), tonic::Code::NotFound, "{}", error.message());
    }

    #[test]
    fn statistics_are_given_for_a_path_that_shows_the_volume() {
        use crate::config::Pool;
        let pool = |name: &str, path: &str| Pool {
            name: name.into(),
            path: path.into(),
            quota: true,
            require_mountpoint: true,
            overcommit: 1.0,
        };
        let config = Config {
            pools: vec![pool("default", "/mnt/pool"), pool("fast", "/mnt/fast")],
            project_id_base: 1000,
            transfer: Default::default(),
        };
        let mountinfo = "\
22 1 259:2 / / rw,relatime shared:1 - ext4 /dev/mapper/main-root rw
901 22 253:3 / /mnt/pool rw,relatime shared:40 - xfs /dev/mapper/nvme-pool rw,prjquota
902 22 253:4 / /mnt/fast rw,relatime shared:41 - xfs /dev/mapper/nvme-fast rw,prjquota
950 22 253:3 /volumes/pv-1/data /pods/uid-1/mount rw,relatime - xfs /dev/mapper/nvme-pool rw,prjquota
951 22 253:3 /volumes/pv-2/data /pods/uid-2/mount rw,relatime - xfs /dev/mapper/nvme-pool rw,prjquota
952 22 253:4 /volumes/pv-3/data /pods/uid-3/mount rw,relatime - xfs /dev/mapper/nvme-fast rw,prjquota";
        let published = |volume: &str, path: &str| published_at(&config, mountinfo, volume, Path::new(path));
        assert!(published("pv-1", "/pods/uid-1/mount"));
        assert!(published("pv-3", "/pods/uid-3/mount"), "a volume of another pool");
        // A path where the volume was and is no more is a directory of the root filesystem:
        // its statistics would be the ones of the disk of the node.
        assert!(!published("pv-1", "/pods/uid-9/mount"));
        assert!(!published("pv-1", "/pods/uid-2/mount"), "the path shows another volume");
        assert!(!published("pv-1", "/mnt/pool"), "the pool itself is no volume");
        assert!(!published("pv-9", "/pods/uid-1/mount"));
        assert!(!published("../pv-1", "/pods/uid-1/mount"));
    }

    #[tokio::test]
    async fn block_volume_is_refused_before_anything_is_looked_for_on_the_disk() {
        use volume_capability::{AccessType, BlockVolume};
        let request = NodePublishVolumeRequest {
            volume_id: "pv-1".into(),
            target_path: "/nonexistent/local-path-csi/target".into(),
            volume_capability: Some(VolumeCapability {
                access_type: Some(AccessType::Block(BlockVolume {})),
                access_mode: None,
            }),
            ..Default::default()
        };
        let error = service().node_publish_volume(Request::new(request)).await.unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument, "{}", error.message());
        assert!(error.message().contains("Block"), "{}", error.message());
    }

    #[tokio::test]
    async fn node_tells_the_kubelet_that_it_tells_one_writer_from_many() {
        // The kubelet asks the node, not the controller: without the capability it publishes a
        // volume of a ReadWriteOncePod claim as SINGLE_NODE_WRITER.
        let request = Request::new(NodeGetCapabilitiesRequest {});
        let capabilities = service().node_get_capabilities(request).await.unwrap().into_inner().capabilities;
        let declared: Vec<rpc::Type> =
            capabilities.iter().filter_map(|c| c.r#type.as_ref().map(|Type::Rpc(rpc)| rpc.r#type())).collect();
        assert!(declared.contains(&rpc::Type::SingleNodeMultiWriter), "{declared:?}");
        assert!(declared.contains(&rpc::Type::GetVolumeStats), "{declared:?}");
    }
}
