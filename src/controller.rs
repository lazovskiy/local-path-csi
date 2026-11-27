//! CSI Controller service: decides where a volume lives and on which node it may be used.
//!
//! The controller never touches a disk. It records what it wants in a LocalPathVolume and waits for
//! the agent of the node to make it true.

use std::sync::Arc;
use std::time::Duration;

use tonic::{Request, Response, Status};

use crate::csi::controller_server::Controller;
use crate::csi::controller_service_capability::{Rpc, Type, rpc};
use crate::csi::{
    ControllerExpandVolumeRequest, ControllerExpandVolumeResponse, ControllerGetCapabilitiesRequest,
    ControllerGetCapabilitiesResponse, ControllerGetVolumeRequest, ControllerGetVolumeResponse,
    ControllerModifyVolumeRequest, ControllerModifyVolumeResponse, ControllerPublishVolumeRequest,
    ControllerPublishVolumeResponse, ControllerServiceCapability, ControllerUnpublishVolumeRequest,
    ControllerUnpublishVolumeResponse, CreateSnapshotRequest, CreateSnapshotResponse, CreateVolumeRequest,
    CreateVolumeResponse, DeleteSnapshotRequest, DeleteSnapshotResponse, DeleteVolumeRequest, DeleteVolumeResponse,
    GetCapacityRequest, GetCapacityResponse, GetSnapshotRequest, GetSnapshotResponse, ListSnapshotsRequest,
    ListSnapshotsResponse, ListVolumesRequest, ListVolumesResponse, ValidateVolumeCapabilitiesRequest,
    ValidateVolumeCapabilitiesResponse, validate_volume_capabilities_response,
};
use crate::events::Events;
use crate::store::{Inventory, Store, StoreError};
use crate::topology_node;

mod capacity;
mod provision;
mod publish;
mod relocation;
mod request;

pub use self::capacity::{Closed, allocated_in_pool, free_in_pool};
pub use self::request::CONTEXT_POOL;
pub(crate) use self::request::check_access_type;
use self::request::{PARAM_POOL, check_capabilities, csi_volume, requested_size};

/// What the tests at the end of this file name, and nothing above them does any more: they were
/// written when the whole controller was in this file.
#[cfg(test)]
use {
    self::provision::ready,
    self::relocation::{human, progress},
    self::request::{PARAM_CLAIM_NAME, PARAM_CLAIM_NAMESPACE},
    crate::api::{AccessMode, Claim, LocalPathVolume, LocalPathVolumeSpec, Mobility, Phase, PoolState, TransferState},
    crate::csi::volume_capability::access_mode::Mode,
    crate::csi::{CapacityRange, Topology, TopologyRequirement, VolumeCapability, volume_capability},
    crate::events::Notice,
    crate::store::Volumes,
    crate::{parse_duration, topology_storage},
    std::collections::HashMap,
};

pub struct ControllerService<S> {
    store: Arc<S>,
    /// How long one call waits for an agent before it asks the sidecar to retry.
    wait: Duration,
    poll: Duration,
    /// A node whose agent has not reported for this long has no agent.
    agent_timeout: Duration,
    /// How often a call that waits for a volume to move looks whether the cluster still wants
    /// the volume there, and how often it tells in an event how far the copy is.
    attachment_check: Duration,
    progress_note: Duration,
    events: Option<Arc<Events>>,
    /// Held from the look at what a pool has left to the write that takes of it: two calls that
    /// looked at the same time would both find the room and both take it. One lock for all
    /// pools: what it guards is a read of the node, a list of the volumes and one write.
    ///
    /// It is a lock of this process. The sidecars elect their leaders one by one, so a volume
    /// may be created by one replica while another resizes or moves one: those two still do
    /// not see each other.
    placing: tokio::sync::Mutex<()>,
    /// Becomes `true` when the process is told to stop. A server that stops waits for the calls
    /// it is answering, and a call that waits for a volume to arrive may take hours: the calls
    /// that wait are answered then, and their sidecars repeat them to the next controller.
    stopping: tokio::sync::watch::Receiver<bool>,
}

/// Default of `agent_timeout`: four reports of an agent.
pub const DEFAULT_AGENT_TIMEOUT: Duration = Duration::from_secs(120);

impl<S: Store> ControllerService<S> {
    pub fn new(store: Arc<S>) -> Self {
        Self {
            store,
            wait: Duration::from_secs(20),
            poll: Duration::from_millis(250),
            agent_timeout: DEFAULT_AGENT_TIMEOUT,
            attachment_check: Duration::from_secs(2),
            progress_note: Duration::from_secs(30),
            events: None,
            placing: tokio::sync::Mutex::new(()),
            stopping: tokio::sync::watch::channel(false).1,
        }
    }

    pub fn with_stopping(mut self, stopping: tokio::sync::watch::Receiver<bool>) -> Self {
        self.stopping = stopping;
        self
    }

    /// Waits for `time`, unless the process is told to stop meanwhile.
    async fn pause(&self, time: Duration) -> Result<(), Status> {
        let mut stopping = self.stopping.clone();
        let told = async move {
            // Nobody is left to tell: the process is not stopped through this.
            if stopping.wait_for(|stopping| *stopping).await.is_err() {
                std::future::pending::<()>().await;
            }
        };
        tokio::select! {
            _ = tokio::time::sleep(time) => Ok(()),
            _ = told => Err(Status::unavailable("the controller is stopping: repeat the call")),
        }
    }

    pub fn with_events(mut self, events: Arc<Events>) -> Self {
        self.events = Some(events);
        self
    }

    pub fn with_agent_timeout(mut self, agent_timeout: Duration) -> Self {
        self.agent_timeout = agent_timeout;
        self
    }
}

/// The answer to a CSI call that failed at the store. The code is what its sidecar goes by:
/// whether to repeat the call, and whether anything may have been done.
impl From<StoreError> for Status {
    fn from(error: StoreError) -> Self {
        let text = format!("Kubernetes API: {error}");
        match error {
            // Another writer was first, or the object is there already or gone: the call is
            // to be repeated, and reads anew.
            StoreError::Refused { code: 404 | 409, .. } => Status::aborted(text),
            // No repeat helps until the rights of the service account are put right.
            StoreError::Refused { code: 401 | 403, .. } => Status::permission_denied(text),
            // The API server does not take what the driver wrote, or the driver wrote by a
            // volume that never came from the API server: a fault of the driver.
            StoreError::Refused { code: 400 | 422, .. } | StoreError::Unversioned { .. } | StoreError::Unnamed => {
                Status::internal(text)
            }
            // An API server that is busy or failing, a network that is, a watch that is behind:
            // the next attempt may get through.
            StoreError::Refused { .. } | StoreError::Client(_) | StoreError::NotShown { .. } => {
                Status::unavailable(text)
            }
        }
    }
}

fn capability(kind: rpc::Type) -> ControllerServiceCapability {
    ControllerServiceCapability { r#type: Some(Type::Rpc(Rpc { r#type: kind.into() })) }
}

impl<S: Inventory> ControllerService<S> {
    /// The metrics page of the controller.
    pub async fn metrics(&self) -> String {
        match (self.store.volumes().await, self.store.nodes().await) {
            (Ok(volumes), Ok(nodes)) => {
                crate::metrics::controller(&volumes, &nodes, crate::unix_now(), self.agent_timeout.as_secs())
            }
            (Err(e), _) | (_, Err(e)) => {
                tracing::warn!(error = %e, "cannot read the custom resources for the metrics");
                String::new()
            }
        }
    }
}

#[tonic::async_trait]
impl<S: Store> Controller for ControllerService<S> {
    async fn create_volume(
        &self,
        request: Request<CreateVolumeRequest>,
    ) -> Result<Response<CreateVolumeResponse>, Status> {
        let (name, volume) = self.create(request.into_inner()).await?;
        Ok(Response::new(CreateVolumeResponse { volume: Some(csi_volume(&name, &volume)) }))
    }

    async fn delete_volume(
        &self,
        request: Request<DeleteVolumeRequest>,
    ) -> Result<Response<DeleteVolumeResponse>, Status> {
        self.delete(&request.into_inner().volume_id).await?;
        Ok(Response::new(DeleteVolumeResponse {}))
    }

    async fn controller_publish_volume(
        &self,
        request: Request<ControllerPublishVolumeRequest>,
    ) -> Result<Response<ControllerPublishVolumeResponse>, Status> {
        let ControllerPublishVolumeRequest { volume_id, node_id, .. } = request.into_inner();
        self.publish(&volume_id, &node_id).await?;
        Ok(Response::new(ControllerPublishVolumeResponse::default()))
    }

    async fn controller_unpublish_volume(
        &self,
        request: Request<ControllerUnpublishVolumeRequest>,
    ) -> Result<Response<ControllerUnpublishVolumeResponse>, Status> {
        let ControllerUnpublishVolumeRequest { volume_id, node_id, .. } = request.into_inner();
        self.unpublish(&volume_id, &node_id).await?;
        Ok(Response::new(ControllerUnpublishVolumeResponse {}))
    }

    async fn validate_volume_capabilities(
        &self,
        request: Request<ValidateVolumeCapabilitiesRequest>,
    ) -> Result<Response<ValidateVolumeCapabilitiesResponse>, Status> {
        let request = request.into_inner();
        if self.store.volume(&request.volume_id).await?.is_none() {
            return Err(Status::not_found(format!("volume {} does not exist", request.volume_id)));
        }
        let (confirmed, message) = match check_capabilities(&request.volume_capabilities) {
            Ok(()) => {
                let volume_capabilities = request.volume_capabilities;
                let confirmed =
                    validate_volume_capabilities_response::Confirmed { volume_capabilities, ..Default::default() };
                (Some(confirmed), String::new())
            }
            Err(refusal) => (None, refusal.message().to_string()),
        };
        Ok(Response::new(ValidateVolumeCapabilitiesResponse { confirmed, message }))
    }

    async fn get_capacity(
        &self,
        request: Request<GetCapacityRequest>,
    ) -> Result<Response<GetCapacityResponse>, Status> {
        let request = request.into_inner();
        let node = request.accessible_topology.as_ref().and_then(|t| t.segments.get(topology_node()));
        let pool = request.parameters.get(PARAM_POOL);
        let available = match (node, pool) {
            (Some(node), Some(pool)) => self.free_bytes(node, pool).await?.unwrap_or(0),
            _ => 0,
        };
        Ok(Response::new(GetCapacityResponse { available_capacity: available as i64, ..Default::default() }))
    }

    async fn controller_expand_volume(
        &self,
        request: Request<ControllerExpandVolumeRequest>,
    ) -> Result<Response<ControllerExpandVolumeResponse>, Status> {
        let request = request.into_inner();
        let name = request.volume_id;
        let requested = requested_size(request.capacity_range.as_ref())?;
        let volume = self.expand(&name, requested).await?;
        Ok(Response::new(ControllerExpandVolumeResponse {
            capacity_bytes: volume.spec.size_bytes as i64,
            // The limit of a mounted volume changes in place.
            node_expansion_required: false,
        }))
    }

    async fn controller_get_capabilities(
        &self,
        _: Request<ControllerGetCapabilitiesRequest>,
    ) -> Result<Response<ControllerGetCapabilitiesResponse>, Status> {
        Ok(Response::new(ControllerGetCapabilitiesResponse {
            capabilities: vec![
                capability(rpc::Type::CreateDeleteVolume),
                capability(rpc::Type::PublishUnpublishVolume),
                capability(rpc::Type::GetCapacity),
                capability(rpc::Type::ExpandVolume),
                // Without it the sidecars send ReadWriteOnce and ReadWriteOncePod alike, as
                // SINGLE_NODE_WRITER; with it, as SINGLE_NODE_MULTI_WRITER and
                // SINGLE_NODE_SINGLE_WRITER, and `access_mode` can tell a claim for one pod.
                capability(rpc::Type::SingleNodeMultiWriter),
            ],
        }))
    }

    async fn list_volumes(&self, _: Request<ListVolumesRequest>) -> Result<Response<ListVolumesResponse>, Status> {
        Err(Status::unimplemented("ListVolumes"))
    }

    async fn create_snapshot(
        &self,
        _: Request<CreateSnapshotRequest>,
    ) -> Result<Response<CreateSnapshotResponse>, Status> {
        Err(Status::unimplemented("CreateSnapshot"))
    }

    async fn delete_snapshot(
        &self,
        _: Request<DeleteSnapshotRequest>,
    ) -> Result<Response<DeleteSnapshotResponse>, Status> {
        Err(Status::unimplemented("DeleteSnapshot"))
    }

    async fn list_snapshots(
        &self,
        _: Request<ListSnapshotsRequest>,
    ) -> Result<Response<ListSnapshotsResponse>, Status> {
        Err(Status::unimplemented("ListSnapshots"))
    }

    async fn get_snapshot(&self, _: Request<GetSnapshotRequest>) -> Result<Response<GetSnapshotResponse>, Status> {
        Err(Status::unimplemented("GetSnapshot"))
    }

    async fn controller_get_volume(
        &self,
        _: Request<ControllerGetVolumeRequest>,
    ) -> Result<Response<ControllerGetVolumeResponse>, Status> {
        Err(Status::unimplemented("ControllerGetVolume"))
    }

    async fn controller_modify_volume(
        &self,
        _: Request<ControllerModifyVolumeRequest>,
    ) -> Result<Response<ControllerModifyVolumeResponse>, Status> {
        Err(Status::unimplemented("ControllerModifyVolume"))
    }
}

#[cfg(test)]
mod tests {
    use tonic::Code;

    use super::*;

    #[test]
    fn volume_that_was_brought_back_is_in_place_as_soon_as_it_has_a_persistent_volume() {
        // The agent writes `Ready` when it sees that the mark of the restore is gone; a pod of
        // the claim may ask for the volume before that. The directory is there, with its limits.
        let mut volume = LocalPathVolume::new("pv-1", restored_spec());
        let applied = volume.spec.size_bytes;
        volume.status = Some(crate::api::LocalPathVolumeStatus {
            phase: Some(Phase::Orphaned),
            capacity_bytes: Some(applied),
            ..Default::default()
        });
        assert!(ready(Some(&volume)).unwrap());
        // As with any volume: not before the limits it has are the ones it is to have.
        volume.status.as_mut().unwrap().capacity_bytes = Some(applied - 1);
        assert!(!ready(Some(&volume)).unwrap());
    }

    fn restored_spec() -> crate::api::LocalPathVolumeSpec {
        crate::api::LocalPathVolumeSpec {
            pool: "default".into(),
            size_bytes: 10 << 30,
            mobility: crate::api::Mobility::Pinned,
            node: "a".into(),
            inodes_per_gib: 0,
            dir_mode: "0770".into(),
            delete_grace_seconds: 0,
            access_mode: crate::api::AccessMode::ReadWriteOnce,
            storage_class: Some("local".into()),
            claim: None,
        }
    }
    use crate::store::memory::MemoryStore;

    const GIB: u64 = 1 << 30;

    fn capability(mode: Mode) -> VolumeCapability {
        VolumeCapability { access_mode: Some(volume_capability::AccessMode { mode: mode.into() }), access_type: None }
    }

    fn topology(key: &str, value: &str) -> Topology {
        Topology { segments: HashMap::from([(key.to_string(), value.to_string())]) }
    }

    fn create(name: &str, node: &str, size: u64, parameters: &[(&str, &str)]) -> Request<CreateVolumeRequest> {
        let mut all = HashMap::from([(PARAM_POOL.to_string(), "default".to_string())]);
        all.extend(parameters.iter().map(|(k, v)| (k.to_string(), v.to_string())));
        Request::new(CreateVolumeRequest {
            name: name.into(),
            capacity_range: Some(CapacityRange { required_bytes: size as i64, limit_bytes: 0 }),
            volume_capabilities: vec![capability(Mode::SingleNodeWriter)],
            parameters: all,
            accessibility_requirements: Some(TopologyRequirement {
                requisite: vec![topology(topology_node(), "other"), topology(topology_node(), node)],
                preferred: vec![topology(topology_node(), node)],
            }),
            ..Default::default()
        })
    }

    fn publish(volume: &str, node: &str) -> Request<ControllerPublishVolumeRequest> {
        Request::new(ControllerPublishVolumeRequest {
            volume_id: volume.into(),
            node_id: node.into(),
            ..Default::default()
        })
    }

    fn unpublish(volume: &str, node: &str) -> Request<ControllerUnpublishVolumeRequest> {
        Request::new(ControllerUnpublishVolumeRequest {
            volume_id: volume.into(),
            node_id: node.into(),
            ..Default::default()
        })
    }

    fn expand(volume: &str, size: u64) -> Request<ControllerExpandVolumeRequest> {
        Request::new(ControllerExpandVolumeRequest {
            volume_id: volume.into(),
            capacity_range: Some(CapacityRange { required_bytes: size as i64, limit_bytes: 0 }),
            ..Default::default()
        })
    }

    fn capacity(node: &str, pool: &str) -> Request<GetCapacityRequest> {
        Request::new(GetCapacityRequest {
            parameters: HashMap::from([(PARAM_POOL.to_string(), pool.to_string())]),
            accessible_topology: Some(topology(topology_node(), node)),
            ..Default::default()
        })
    }

    /// Nodes a and b, each with a ready 100 GiB pool `default`, and agents that act at once.
    fn service() -> (ControllerService<MemoryStore>, Arc<MemoryStore>) {
        let store = Arc::new(MemoryStore::with_agent());
        store.add_pool("a", "default", 100 * GIB);
        store.add_pool("b", "default", 100 * GIB);
        (ControllerService::new(store.clone()), store)
    }

    async fn free(service: &ControllerService<MemoryStore>, node: &str) -> u64 {
        service.get_capacity(capacity(node, "default")).await.unwrap().into_inner().available_capacity as u64
    }

    #[tokio::test]
    async fn shared_access_modes_are_rejected() {
        let (service, _) = service();
        let mut request = create("pv-1", "a", GIB, &[]).into_inner();
        request.volume_capabilities = vec![capability(Mode::MultiNodeMultiWriter)];
        assert_eq!(service.create_volume(Request::new(request)).await.unwrap_err().code(), Code::InvalidArgument);
    }

    /// A capability as the sidecars send it: with the way the volume is used.
    fn typed(access_type: volume_capability::AccessType) -> VolumeCapability {
        VolumeCapability { access_type: Some(access_type), ..capability(Mode::SingleNodeWriter) }
    }

    fn mounted(mount_flags: &[&str]) -> VolumeCapability {
        typed(volume_capability::AccessType::Mount(volume_capability::MountVolume {
            mount_flags: mount_flags.iter().map(|flag| flag.to_string()).collect(),
            ..Default::default()
        }))
    }

    fn block() -> VolumeCapability {
        typed(volume_capability::AccessType::Block(volume_capability::BlockVolume {}))
    }

    #[tokio::test]
    async fn block_volumes_and_mount_options_are_refused_with_a_reason() {
        let (service, store) = service();
        for (refused, word) in [(block(), "Block"), (mounted(&["noatime", "nodev"]), "noatime,nodev")] {
            let mut request = create("pv-1", "a", GIB, &[]).into_inner();
            request.volume_capabilities = vec![refused];
            let error = service.create_volume(Request::new(request)).await.unwrap_err();
            assert_eq!(error.code(), Code::InvalidArgument, "{word}");
            assert!(error.message().contains(word), "{}", error.message());
            assert!(store.get("pv-1").is_none(), "{word}: nothing is recorded");
        }
        // What the sidecars send for a claim with volumeMode Filesystem of a class without mountOptions.
        let mut request = create("pv-1", "a", GIB, &[]).into_inner();
        request.volume_capabilities = vec![mounted(&[])];
        service.create_volume(Request::new(request)).await.unwrap();
    }

    #[tokio::test]
    async fn only_what_a_volume_can_do_is_confirmed() {
        let (service, _) = service();
        service.create_volume(create("pv-1", "a", GIB, &[])).await.unwrap();
        let validate = |volume_capabilities: Vec<VolumeCapability>| {
            let request = ValidateVolumeCapabilitiesRequest {
                volume_id: "pv-1".into(),
                volume_capabilities,
                ..Default::default()
            };
            service.validate_volume_capabilities(Request::new(request))
        };
        let confirmed = validate(vec![mounted(&[])]).await.unwrap().into_inner();
        assert_eq!(confirmed.confirmed.map(|c| c.volume_capabilities), Some(vec![mounted(&[])]));
        let refused = validate(vec![mounted(&[]), block()]).await.unwrap().into_inner();
        assert_eq!(refused.confirmed, None);
        assert!(refused.message.contains("Block"), "{}", refused.message);
        let shared = validate(vec![capability(Mode::MultiNodeMultiWriter)]).await.unwrap().into_inner();
        assert_eq!(shared.confirmed, None);
        assert!(shared.message.contains("MULTI_NODE_MULTI_WRITER"), "{}", shared.message);
    }

    #[tokio::test]
    async fn misspelled_and_malformed_parameters_are_rejected() {
        let (service, _) = service();
        for parameters in [
            [("mobilty", "Floating")],
            [("mobility", "Sticky")],
            [("dirMode", "rwxrwx---")],
            [("inodesPerGiB", "many")],
            [("deleteGracePeriod", "soon")],
            [("deleteGracePeriod", "24")],
        ] {
            let error = service.create_volume(create("pv-1", "a", GIB, &parameters)).await.unwrap_err();
            assert_eq!(error.code(), Code::InvalidArgument, "{parameters:?}");
        }
        let mut without_pool = create("pv-1", "a", GIB, &[]).into_inner();
        without_pool.parameters.clear();
        assert_eq!(service.create_volume(Request::new(without_pool)).await.unwrap_err().code(), Code::InvalidArgument);
    }

    #[tokio::test]
    async fn volume_name_must_not_escape_the_pool() {
        let (service, _) = service();
        let error = service.create_volume(create("../etc", "a", GIB, &[])).await.unwrap_err();
        assert_eq!(error.code(), Code::InvalidArgument);
    }

    #[tokio::test]
    async fn volume_is_recorded_with_everything_the_agent_needs() {
        let (service, store) = service();
        store.claims.lock().unwrap().insert(("infra".into(), "storage-postgres-0".into()), "local-kept".into());
        let parameters = [
            ("mobility", "Floating"),
            ("dirMode", "0750"),
            ("inodesPerGiB", "1000"),
            ("deleteGracePeriod", "24h"),
            (PARAM_CLAIM_NAMESPACE, "infra"),
            (PARAM_CLAIM_NAME, "storage-postgres-0"),
            ("csi.storage.k8s.io/pv/name", "pv-1"),
        ];
        service.create_volume(create("pv-1", "a", 10 * GIB, &parameters)).await.unwrap();
        let volume = store.get("pv-1").unwrap();
        assert_eq!(
            volume.spec,
            LocalPathVolumeSpec {
                pool: "default".into(),
                size_bytes: 10 * GIB,
                mobility: Mobility::Floating,
                node: "a".into(),
                inodes_per_gib: 1000,
                dir_mode: "0750".into(),
                delete_grace_seconds: 86400,
                access_mode: AccessMode::ReadWriteOnce,
                storage_class: Some("local-kept".into()),
                claim: Some(Claim { namespace: "infra".into(), name: "storage-postgres-0".into() }),
            }
        );
        assert_eq!(volume.spec.inode_limit(), 10_000);
        assert_eq!(volume.data_node(), "a");
        assert_eq!(volume.metadata.finalizers, Some(vec![crate::finalizer().to_string()]));
    }

    #[tokio::test]
    async fn pinned_volume_is_reachable_from_its_node_only() {
        let (service, _) = service();
        let volume = service.create_volume(create("pv-1", "a", GIB, &[])).await.unwrap().into_inner().volume.unwrap();
        assert_eq!(volume.accessible_topology, vec![topology(topology_node(), "a")]);
        assert_eq!(volume.capacity_bytes as u64, GIB);
        assert_eq!(volume.volume_context.get(CONTEXT_POOL).map(String::as_str), Some("default"));
    }

    #[tokio::test]
    async fn floating_volume_is_reachable_from_every_storage_node() {
        let (service, _) = service();
        let request = create("pv-1", "a", GIB, &[("mobility", "Floating")]);
        let volume = service.create_volume(request).await.unwrap().into_inner().volume.unwrap();
        assert_eq!(volume.accessible_topology, vec![topology(topology_storage(), "true")]);
    }

    #[tokio::test]
    async fn repeated_create_returns_the_same_volume_and_a_different_one_conflicts() {
        let (service, store) = service();
        service.create_volume(create("pv-1", "a", GIB, &[])).await.unwrap();
        // The retry may come after the scheduler has moved on to another node: the volume stays where it is.
        let again = service.create_volume(create("pv-1", "b", GIB, &[])).await.unwrap().into_inner().volume.unwrap();
        assert_eq!(again.accessible_topology, vec![topology(topology_node(), "a")]);
        assert_eq!(store.volumes.lock().unwrap().len(), 1);
        let error = service.create_volume(create("pv-1", "a", 2 * GIB, &[])).await.unwrap_err();
        assert_eq!(error.code(), Code::AlreadyExists);
    }

    #[tokio::test(start_paused = true)]
    async fn create_asks_for_a_retry_while_the_agent_has_not_made_the_directory() {
        let store = Arc::new(MemoryStore::default());
        store.add_pool("a", "default", 100 * GIB);
        let service = ControllerService::new(store.clone());
        let error = service.create_volume(create("pv-1", "a", GIB, &[])).await.unwrap_err();
        assert_eq!(error.code(), Code::Aborted);
        // The request stays recorded, with its node: the retry picks it up.
        assert_eq!(store.get("pv-1").unwrap().status.unwrap().node.as_deref(), Some("a"));
    }

    #[tokio::test]
    async fn volume_is_not_created_where_it_does_not_fit() {
        let (service, store) = service();
        store.add_pool_in_state("c", "default", 100 * GIB, PoolState::NoQuota);
        service.create_volume(create("pv-1", "a", 60 * GIB, &[])).await.unwrap();
        for (name, node, pool) in
            [("pv-2", "a", "default"), ("pv-3", "c", "default"), ("pv-4", "a", "fast"), ("pv-5", "nowhere", "default")]
        {
            let error = service.create_volume(create(name, node, 60 * GIB, &[("pool", pool)])).await.unwrap_err();
            assert_eq!(error.code(), Code::ResourceExhausted, "{name}: {}", error.message());
            assert!(store.get(name).is_none());
        }
        // The same size fits on a node whose pool is empty.
        service.create_volume(create("pv-2", "b", 60 * GIB, &[])).await.unwrap();
    }

    #[tokio::test]
    async fn node_whose_agent_stopped_reporting_takes_nothing_new() {
        let (service, store) = service();
        service.create_volume(create("pv-1", "a", 10 * GIB, &[])).await.unwrap();
        for silence in [Some(121), None] {
            store.age_report("a", silence);
            assert_eq!(free(&service, "a").await, 0, "{silence:?}");
            let error = service.create_volume(create("pv-2", "a", GIB, &[])).await.unwrap_err();
            assert_eq!(error.code(), Code::ResourceExhausted);
            assert!(error.message().contains("has not reported"), "{}", error.message());
            assert!(store.get("pv-2").is_none());
            // The size in the spec is what the agent applies: it is not raised with nobody to apply it.
            // UNAVAILABLE is what the resizer keeps retrying; FAILED_PRECONDITION would tell it
            // that the volume has to be expanded offline.
            let error = service.controller_expand_volume(expand("pv-1", 20 * GIB)).await.unwrap_err();
            assert_eq!(error.code(), Code::Unavailable);
            assert!(error.message().contains("has not reported"), "{}", error.message());
            assert_eq!(store.get("pv-1").unwrap().spec.size_bytes, 10 * GIB);
        }
        // A report within the timeout is a live agent, and other nodes are not affected meanwhile.
        store.age_report("a", Some(121));
        assert_eq!(free(&service, "b").await, 100 * GIB);
        store.age_report("a", Some(100));
        assert_eq!(free(&service, "a").await, 90 * GIB);
        service.controller_expand_volume(expand("pv-1", 20 * GIB)).await.unwrap();
    }

    #[tokio::test]
    async fn agent_timeout_is_configurable() {
        let (service, store) = service();
        let service = service.with_agent_timeout(Duration::from_secs(30));
        store.age_report("a", Some(31));
        assert_eq!(free(&service, "a").await, 0);
        store.age_report("a", Some(29));
        assert_eq!(free(&service, "a").await, 100 * GIB);
    }

    #[tokio::test]
    async fn capacity_is_what_volume_sizes_leave_of_the_pool() {
        let (service, store) = service();
        store.add_pool_in_state("c", "default", 100 * GIB, PoolState::NotMountpoint);
        service.create_volume(create("pv-1", "a", 30 * GIB, &[])).await.unwrap();
        service.create_volume(create("pv-2", "a", 20 * GIB, &[])).await.unwrap();
        assert_eq!(free(&service, "a").await, 50 * GIB);
        assert_eq!(free(&service, "b").await, 100 * GIB);
        assert_eq!(free(&service, "c").await, 0);
        assert_eq!(free(&service, "nowhere").await, 0);
        service
            .delete_volume(Request::new(DeleteVolumeRequest { volume_id: "pv-1".into(), ..Default::default() }))
            .await
            .unwrap();
        assert_eq!(free(&service, "a").await, 80 * GIB);
    }

    #[tokio::test]
    async fn expansion_grows_the_volume_within_the_pool_and_never_shrinks_it() {
        let (service, store) = service();
        service.create_volume(create("pv-1", "a", 10 * GIB, &[])).await.unwrap();
        let grown = service.controller_expand_volume(expand("pv-1", 40 * GIB)).await.unwrap().into_inner();
        assert_eq!(grown.capacity_bytes as u64, 40 * GIB);
        assert!(!grown.node_expansion_required);
        assert_eq!(free(&service, "a").await, 60 * GIB);

        let smaller = service.controller_expand_volume(expand("pv-1", 5 * GIB)).await.unwrap().into_inner();
        assert_eq!(smaller.capacity_bytes as u64, 40 * GIB);

        let error = service.controller_expand_volume(expand("pv-1", 101 * GIB)).await.unwrap_err();
        assert_eq!(error.code(), Code::OutOfRange);
        assert_eq!(store.get("pv-1").unwrap().spec.size_bytes, 40 * GIB);
        // Growing to exactly the size of the pool is allowed.
        service.controller_expand_volume(expand("pv-1", 100 * GIB)).await.unwrap();
    }

    #[tokio::test]
    async fn expansion_that_comes_after_a_larger_one_does_not_shrink_the_volume() {
        let (service, store) = service();
        service.create_volume(create("pv-1", "a", 10 * GIB, &[])).await.unwrap();
        // A request for 12 GiB has read the size of the volume. Before it writes, a request for
        // 20 GiB is done.
        *store.meanwhile.lock().unwrap() = Some(Box::new(|store: &MemoryStore| {
            store.change("pv-1", |v| v.spec.size_bytes = 20 * GIB);
        }));
        let grown = service.controller_expand_volume(expand("pv-1", 12 * GIB)).await.unwrap().into_inner();
        assert_eq!(store.get("pv-1").unwrap().spec.size_bytes, 20 * GIB);
        assert_eq!(grown.capacity_bytes as u64, 20 * GIB, "the answer is the size the volume has");
    }

    #[tokio::test]
    async fn expansion_goes_through_when_the_volume_changes_in_something_else() {
        let (service, store) = service();
        service.create_volume(create("pv-1", "a", 10 * GIB, &[])).await.unwrap();
        // The agent reports the usage between the read and the write.
        *store.meanwhile.lock().unwrap() = Some(Box::new(|store: &MemoryStore| {
            store.change("pv-1", |v| v.status.as_mut().unwrap().used_bytes = Some(GIB));
        }));
        let grown = service.controller_expand_volume(expand("pv-1", 40 * GIB)).await.unwrap().into_inner();
        assert_eq!(grown.capacity_bytes as u64, 40 * GIB);
        assert_eq!(store.get("pv-1").unwrap().status.unwrap().used_bytes, Some(GIB));
    }

    #[tokio::test]
    async fn volume_is_published_on_its_node_and_never_on_two() {
        let (service, store) = service();
        service.create_volume(create("pv-1", "a", GIB, &[("mobility", "Floating")])).await.unwrap();
        service.controller_publish_volume(publish("pv-1", "a")).await.unwrap();
        service.controller_publish_volume(publish("pv-1", "a")).await.unwrap();
        assert_eq!(store.get("pv-1").unwrap().status.unwrap().attached_node.as_deref(), Some("a"));
        let error = service.controller_publish_volume(publish("pv-1", "b")).await.unwrap_err();
        assert_eq!(error.code(), Code::FailedPrecondition);

        // Unpublishing from a node the volume is not on changes nothing.
        service.controller_unpublish_volume(unpublish("pv-1", "b")).await.unwrap();
        assert_eq!(store.get("pv-1").unwrap().status.unwrap().attached_node.as_deref(), Some("a"));
        service.controller_unpublish_volume(unpublish("pv-1", "a")).await.unwrap();
        assert_eq!(store.get("pv-1").unwrap().status.unwrap().attached_node, None);
    }

    #[tokio::test]
    async fn unpublish_that_comes_late_does_not_take_away_what_was_published_since() {
        let (service, store) = service();
        service.create_volume(create("pv-1", "a", GIB, &[("mobility", "Floating")])).await.unwrap();
        service.controller_publish_volume(publish("pv-1", "a")).await.unwrap();
        // A repeated call for node a has read the volume. Before it writes, the call it repeats
        // is done and the volume is published to node b.
        *store.meanwhile.lock().unwrap() = Some(Box::new(|store: &MemoryStore| {
            store.change("pv-1", |v| v.status.as_mut().unwrap().attached_node = Some("b".into()));
        }));
        service.controller_unpublish_volume(unpublish("pv-1", "a")).await.unwrap();
        assert_eq!(store.get("pv-1").unwrap().status.unwrap().attached_node.as_deref(), Some("b"));
    }

    #[tokio::test]
    async fn unpublish_goes_through_when_the_volume_changes_in_something_else() {
        let (service, store) = service();
        service.create_volume(create("pv-1", "a", GIB, &[])).await.unwrap();
        service.controller_publish_volume(publish("pv-1", "a")).await.unwrap();
        // The agent reports the usage between the read and the write.
        *store.meanwhile.lock().unwrap() = Some(Box::new(|store: &MemoryStore| {
            store.change("pv-1", |v| v.status.as_mut().unwrap().used_bytes = Some(GIB / 2));
        }));
        service.controller_unpublish_volume(unpublish("pv-1", "a")).await.unwrap();
        let status = store.get("pv-1").unwrap().status.unwrap();
        assert_eq!((status.attached_node, status.used_bytes), (None, Some(GIB / 2)));
    }

    #[tokio::test]
    async fn volume_that_was_brought_back_is_given_to_no_pod_until_it_is_approved() {
        let (service, store) = service();
        service.create_volume(create("pv-1", "a", GIB, &[])).await.unwrap();
        // An agent recreated it from a directory: whose data that is, only the agent has said.
        let mark = |volume: &mut LocalPathVolume| {
            volume.metadata.labels = Some([(crate::restore_label().to_string(), "true".to_string())].into());
            volume.status.as_mut().unwrap().phase = Some(Phase::Orphaned);
        };
        store.change("pv-1", mark);
        let refused = service.controller_publish_volume(publish("pv-1", "a")).await.unwrap_err();
        assert_eq!(refused.code(), Code::FailedPrecondition);
        // Whoever looks at the pod that waits is told what to do.
        assert!(refused.message().contains(&format!("{}=true", crate::restore_approval())), "{}", refused.message());
        assert_eq!(store.get("pv-1").unwrap().status.unwrap().attached_node, None);

        // Approved: the controller takes the mark off. The agent writes `Ready` after that, and
        // a pod may ask before it has.
        store.change("pv-1", |volume| volume.metadata.labels = None);
        service.controller_publish_volume(publish("pv-1", "a")).await.unwrap();
        assert_eq!(store.get("pv-1").unwrap().status.unwrap().attached_node.as_deref(), Some("a"));
    }

    #[tokio::test]
    async fn publish_of_an_unknown_volume_is_not_found() {
        let (service, _) = service();
        assert_eq!(service.controller_publish_volume(publish("pv-9", "a")).await.unwrap_err().code(), Code::NotFound);
        // Unpublish of a volume that is gone has nothing left to do.
        service.controller_unpublish_volume(unpublish("pv-9", "a")).await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn delete_returns_when_the_agent_has_removed_the_data() {
        let (service, store) = service();
        let delete = |name: &str| Request::new(DeleteVolumeRequest { volume_id: name.into(), ..Default::default() });
        service.create_volume(create("pv-1", "a", GIB, &[])).await.unwrap();
        service.delete_volume(delete("pv-1")).await.unwrap();
        assert!(store.get("pv-1").is_none());
        // Deleting what is not there succeeds: the sidecar retries deletes.
        service.delete_volume(delete("pv-1")).await.unwrap();

        // Without an agent the volume stays, marked for deletion, and the sidecar is asked to retry.
        let store = Arc::new(MemoryStore::default());
        store.add_pool("a", "default", 100 * GIB);
        store.create_volume("pv-2", store_spec()).await.unwrap();
        let service = ControllerService::new(store.clone());
        assert_eq!(service.delete_volume(delete("pv-2")).await.unwrap_err().code(), Code::Aborted);
        assert!(store.get("pv-2").unwrap().metadata.deletion_timestamp.is_some());
    }

    #[tokio::test(start_paused = true)]
    async fn volume_of_a_node_that_left_the_cluster_is_deleted_without_its_agent() {
        let store = Arc::new(MemoryStore::default());
        store.add_pool("a", "default", 100 * GIB);
        store.create_volume("pv-1", store_spec()).await.unwrap();
        store.cluster_nodes.lock().unwrap().remove("a");
        let service = ControllerService::new(store.clone());
        service
            .delete_volume(Request::new(DeleteVolumeRequest { volume_id: "pv-1".into(), ..Default::default() }))
            .await
            .unwrap();
        assert!(store.get("pv-1").is_none());
    }

    #[tokio::test]
    async fn claim_for_a_single_pod_is_recorded_as_such() {
        let (service, store) = service();
        let mut request = create("pv-1", "a", GIB, &[]).into_inner();
        request.volume_capabilities = vec![capability(Mode::SingleNodeSingleWriter)];
        service.create_volume(Request::new(request)).await.unwrap();
        assert_eq!(store.get("pv-1").unwrap().spec.access_mode, AccessMode::ReadWriteOncePod);
        service.create_volume(create("pv-2", "a", GIB, &[])).await.unwrap();
        assert_eq!(store.get("pv-2").unwrap().spec.access_mode, AccessMode::ReadWriteOnce);
    }

    #[tokio::test]
    async fn controller_tells_the_sidecars_that_it_tells_one_writer_from_many() {
        // Without this capability the sidecars send ReadWriteOnce and ReadWriteOncePod as one mode,
        // SINGLE_NODE_WRITER, and a claim for a single pod cannot be told from any other.
        let (service, _) = service();
        let request = Request::new(ControllerGetCapabilitiesRequest {});
        let capabilities = service.controller_get_capabilities(request).await.unwrap().into_inner().capabilities;
        assert!(capabilities.contains(&super::capability(rpc::Type::SingleNodeMultiWriter)), "{capabilities:?}");
    }

    #[tokio::test]
    async fn claim_for_many_pods_of_a_node_is_recorded_as_read_write_once() {
        // What the sidecars send for ReadWriteOnce once the capability is declared.
        let (service, store) = service();
        let mut request = create("pv-1", "a", GIB, &[]).into_inner();
        request.volume_capabilities = vec![capability(Mode::SingleNodeMultiWriter)];
        service.create_volume(Request::new(request)).await.unwrap();
        assert_eq!(store.get("pv-1").unwrap().spec.access_mode, AccessMode::ReadWriteOnce);
    }

    #[test]
    fn durations_are_seconds_minutes_hours_or_days() {
        for (text, seconds) in
            [("0", Some(0)), ("90s", Some(90)), ("30m", Some(1800)), ("24h", Some(86400)), ("7d", Some(604800))]
        {
            assert_eq!(parse_duration(text), seconds, "{text}");
        }
        for text in ["", "24", "h", "-1h", "+1h", "1.5h", "1w"] {
            assert_eq!(parse_duration(text), None, "{text}");
        }
    }

    fn store_spec() -> LocalPathVolumeSpec {
        LocalPathVolumeSpec {
            pool: "default".into(),
            size_bytes: GIB,
            mobility: Mobility::Pinned,
            node: "a".into(),
            inodes_per_gib: 0,
            dir_mode: "0770".into(),
            delete_grace_seconds: 0,
            access_mode: AccessMode::ReadWriteOnce,
            storage_class: None,
            claim: None,
        }
    }

    /// Nodes a and b with 100 GiB pools and no agents: the test plays them. A floating volume
    /// of `size` is ready on node a.
    async fn floating_at_rest(size: u64) -> (Arc<ControllerService<MemoryStore>>, Arc<MemoryStore>) {
        let store = Arc::new(MemoryStore::default());
        store.add_pool("a", "default", 100 * GIB);
        store.add_pool("b", "default", 100 * GIB);
        let spec = LocalPathVolumeSpec { mobility: Mobility::Floating, size_bytes: size, ..store_spec() };
        store.create_volume("pv-1", spec).await.unwrap();
        store.set_data_node("pv-1", "a").await.unwrap();
        store.report_ready("pv-1", "a");
        let mut service = ControllerService::new(store.clone());
        (service.poll, service.attachment_check) = (Duration::from_millis(2), Duration::from_millis(2));
        (Arc::new(service), store)
    }

    type Publishing = tokio::task::JoinHandle<Result<Response<ControllerPublishVolumeResponse>, Status>>;

    fn publishing(service: &Arc<ControllerService<MemoryStore>>, node: &'static str) -> Publishing {
        let service = service.clone();
        tokio::spawn(async move { service.controller_publish_volume(publish("pv-1", node)).await })
    }

    /// Waits until the volume looks as `expected` says. The tests that wait run with the clock
    /// of the runtime paused: it moves on only when every task is waiting for it, so a sleep
    /// is a number of turns the controller gets, not a guess at how long a busy machine takes.
    async fn until(store: &MemoryStore, what: &str, expected: impl Fn(&LocalPathVolume) -> bool) {
        for _ in 0..500 {
            if expected(&store.get("pv-1").unwrap()) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        panic!("never happened: {what}; the volume: {:?}", store.get("pv-1"));
    }

    #[tokio::test]
    async fn floating_volume_follows_its_pod_to_another_node() {
        let (service, store) = service();
        service.create_volume(create("pv-1", "a", GIB, &[("mobility", "Floating")])).await.unwrap();
        service.controller_publish_volume(publish("pv-1", "a")).await.unwrap();
        service.controller_unpublish_volume(unpublish("pv-1", "a")).await.unwrap();

        service.controller_publish_volume(publish("pv-1", "b")).await.unwrap();
        let volume = store.get("pv-1").unwrap();
        let status = volume.status.clone().unwrap();
        assert_eq!((volume.spec.node.as_str(), status.node.as_deref()), ("b", Some("b")));
        assert_eq!(status.attached_node.as_deref(), Some("b"));
        assert_eq!(status.moves, 1);
        assert_eq!(volume.destination(), None, "the volume is at rest again");
        // The node it left has its room back, the node it is on has less.
        assert_eq!((free(&service, "a").await, free(&service, "b").await), (100 * GIB, 99 * GIB));
    }

    #[tokio::test]
    async fn pinned_volume_is_never_moved() {
        let (service, store) = service();
        service.create_volume(create("pv-1", "a", GIB, &[])).await.unwrap();
        let error = service.controller_publish_volume(publish("pv-1", "b")).await.unwrap_err();
        assert_eq!(error.code(), Code::FailedPrecondition);
        assert!(error.message().contains("pinned to node a"), "{}", error.message());
        let volume = store.get("pv-1").unwrap();
        assert_eq!((volume.spec.node.as_str(), volume.data_node()), ("a", "a"));
    }

    #[tokio::test]
    async fn volume_does_not_move_to_a_node_without_room_or_without_an_agent() {
        let (service, store) = service();
        service.create_volume(create("pv-1", "a", 60 * GIB, &[("mobility", "Floating")])).await.unwrap();
        service.create_volume(create("pv-2", "b", 60 * GIB, &[])).await.unwrap();
        let error = service.controller_publish_volume(publish("pv-1", "b")).await.unwrap_err();
        assert_eq!(error.code(), Code::ResourceExhausted);
        assert_eq!(store.get("pv-1").unwrap().spec.node, "a", "the volume is not sent anywhere");

        service
            .delete_volume(Request::new(DeleteVolumeRequest { volume_id: "pv-2".into(), ..Default::default() }))
            .await
            .unwrap();
        store.age_report("b", Some(600));
        let error = service.controller_publish_volume(publish("pv-1", "b")).await.unwrap_err();
        assert_eq!(error.code(), Code::ResourceExhausted);
        assert!(error.message().contains("has not reported"), "{}", error.message());
        assert_eq!(store.get("pv-1").unwrap().spec.node, "a");
    }

    #[tokio::test(start_paused = true)]
    async fn volume_is_handed_over_when_its_copy_is_complete_and_published_when_the_node_has_it() {
        let (service, store) = floating_at_rest(60 * GIB).await;
        let call = publishing(&service, "b");
        until(&store, "the volume is sent to node b", |v| v.spec.node == "b").await;
        // On its way the volume takes room on both nodes.
        assert_eq!((free(&service, "a").await, free(&service, "b").await), (40 * GIB, 40 * GIB));

        for state in [TransferState::Queued, TransferState::Copying, TransferState::Failed] {
            store.report_transfer("pv-1", state, Some("details"));
            tokio::time::sleep(Duration::from_millis(30)).await;
            let volume = store.get("pv-1").unwrap();
            assert_eq!(volume.data_node(), "a", "{state:?}: the data is still on node a");
            assert!(!call.is_finished(), "{state:?}: the pod must not start yet");
        }

        store.report_transfer("pv-1", TransferState::Copied, None);
        until(&store, "the volume is handed over to node b", |v| v.data_node() == "b").await;
        let status = store.get("pv-1").unwrap().status.unwrap();
        assert_eq!((status.moves, status.phase, status.transfer), (1, Some(Phase::Pending), None));
        assert_eq!(free(&service, "a").await, 100 * GIB, "node a has its room back");

        // The agent of the former node writes its last report late: it must not count.
        store.report_ready("pv-1", "a");
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(!call.is_finished(), "node b has not made the copy the volume yet");
        assert_eq!(store.get("pv-1").unwrap().status.unwrap().attached_node, None);

        store.report_ready("pv-1", "b");
        call.await.unwrap().unwrap();
        assert_eq!(store.get("pv-1").unwrap().status.unwrap().attached_node.as_deref(), Some("b"));
    }

    #[tokio::test(start_paused = true)]
    async fn move_is_cancelled_when_nothing_waits_for_the_volume_on_the_node() {
        let (service, store) = floating_at_rest(GIB).await;
        let call = publishing(&service, "b");
        until(&store, "the volume is sent to node b", |v| v.spec.node == "b").await;
        // The pod was deleted: Kubernetes gives up on the attachment.
        store.unwanted.lock().unwrap().insert(("pv-1".into(), "b".into()));
        assert_eq!(call.await.unwrap().unwrap_err().code(), Code::Aborted);
        let volume = store.get("pv-1").unwrap();
        assert_eq!((volume.spec.node.as_str(), volume.data_node()), ("a", "a"), "the volume stays on node a");
        assert_eq!(volume.moves(), 0);
        assert_eq!(free(&service, "b").await, 100 * GIB, "node b keeps no room for it");
    }

    #[tokio::test]
    async fn volume_does_not_set_out_for_a_node_where_nothing_waits_for_it() {
        // Kubernetes goes on asking for the attachment of a pod that is gone already.
        let (service, store) = floating_at_rest(GIB).await;
        store.unwanted.lock().unwrap().insert(("pv-1".into(), "b".into()));
        let before = store.get("pv-1").unwrap().metadata.resource_version;
        let error = service.controller_publish_volume(publish("pv-1", "b")).await.unwrap_err();
        assert_eq!(error.code(), Code::Aborted);
        let volume = store.get("pv-1").unwrap();
        assert_eq!(volume.spec.node, "a");
        assert_eq!(volume.metadata.resource_version, before, "nothing is written to the volume");
    }

    #[tokio::test]
    async fn pod_that_comes_back_before_the_volume_left_gets_it_where_it_is() {
        let (service, store) = floating_at_rest(GIB).await;
        store.set_target(&store.get("pv-1").unwrap(), "b").await.unwrap();
        store.report_transfer("pv-1", TransferState::Copying, None);

        service.controller_publish_volume(publish("pv-1", "a")).await.unwrap();
        let volume = store.get("pv-1").unwrap();
        assert_eq!((volume.spec.node.as_str(), volume.data_node()), ("a", "a"));
        assert_eq!(volume.status.as_ref().unwrap().attached_node.as_deref(), Some("a"));
        assert_eq!(volume.moves(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn volume_sent_to_one_node_goes_to_another_when_the_pod_does() {
        let (service, store) = floating_at_rest(GIB).await;
        store.add_pool("c", "default", 100 * GIB);
        let to_b = publishing(&service, "b");
        until(&store, "the volume is sent to node b", |v| v.spec.node == "b").await;
        let to_c = publishing(&service, "c");
        until(&store, "the volume is sent to node c", |v| v.spec.node == "c").await;
        assert_eq!(to_b.await.unwrap().unwrap_err().code(), Code::Aborted, "the call for node b gives up");

        // What node b had reported was about a copy nobody waits for any more.
        assert_eq!(store.get("pv-1").unwrap().transfer(), None);
        assert_eq!(store.get("pv-1").unwrap().data_node(), "a");
        store.report_transfer("pv-1", TransferState::Copied, None);
        until(&store, "the volume is handed over to node c", |v| v.data_node() == "c").await;
        store.report_ready("pv-1", "c");
        to_c.await.unwrap().unwrap();
        assert_eq!(store.get("pv-1").unwrap().status.unwrap().attached_node.as_deref(), Some("c"));
    }

    #[tokio::test(start_paused = true)]
    async fn report_about_a_cancelled_move_does_not_hand_the_volume_over() {
        let (service, store) = floating_at_rest(GIB).await;
        // The copy on node b was complete when its pod went away, and no call was there to see it.
        store.set_target(&store.get("pv-1").unwrap(), "b").await.unwrap();
        store.report_transfer("pv-1", TransferState::Copied, None);
        service.controller_unpublish_volume(unpublish("pv-1", "b")).await.unwrap();
        assert_eq!(store.get("pv-1").unwrap().spec.node, "a");
        assert_eq!(store.get("pv-1").unwrap().transfer(), None);

        // The pod comes back to node b: what was copied then is not the volume as it is now.
        let call = publishing(&service, "b");
        until(&store, "the volume is sent to node b again", |v| v.spec.node == "b").await;
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert_eq!(store.get("pv-1").unwrap().data_node(), "a", "the report of the cancelled move must not count");
        assert!(!call.is_finished());

        store.report_transfer("pv-1", TransferState::Copied, None);
        until(&store, "the volume is handed over to node b", |v| v.data_node() == "b").await;
        store.report_ready("pv-1", "b");
        call.await.unwrap().unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn volume_on_its_way_grows_by_what_both_nodes_have() {
        let (service, store) = floating_at_rest(10 * GIB).await;
        let neighbour = LocalPathVolumeSpec { node: "b".into(), size_bytes: 85 * GIB, ..store_spec() };
        store.create_volume("pv-2", neighbour).await.unwrap();
        store.set_target(&store.get("pv-1").unwrap(), "b").await.unwrap();
        // Node a, where the data is, has 90 GiB left; node b, where the volume is sent, has 5.
        assert_eq!((free(&service, "a").await, free(&service, "b").await), (90 * GIB, 5 * GIB));

        let error = service.controller_expand_volume(expand("pv-1", 20 * GIB)).await.unwrap_err();
        assert_eq!(error.code(), Code::OutOfRange, "{}", error.message());
        assert!(error.message().contains("node b"), "{}", error.message());
        assert_eq!(store.get("pv-1").unwrap().spec.size_bytes, 10 * GIB);

        // What fits on both is taken; no agent is there to apply it, so the call asks for a retry.
        let error = service.controller_expand_volume(expand("pv-1", 15 * GIB)).await.unwrap_err();
        assert_eq!(error.code(), Code::Aborted, "{}", error.message());
        assert_eq!(store.get("pv-1").unwrap().spec.size_bytes, 15 * GIB);
        assert_eq!((free(&service, "a").await, free(&service, "b").await), (85 * GIB, 0));
    }

    #[tokio::test]
    async fn unpublish_from_the_node_a_volume_was_sent_to_keeps_it_where_it_is() {
        let (service, store) = floating_at_rest(GIB).await;
        store.set_target(&store.get("pv-1").unwrap(), "b").await.unwrap();
        service.controller_unpublish_volume(unpublish("pv-1", "b")).await.unwrap();
        let volume = store.get("pv-1").unwrap();
        assert_eq!((volume.spec.node.as_str(), volume.data_node()), ("a", "a"));
    }

    #[tokio::test]
    async fn volume_being_deleted_is_not_moved() {
        let (service, store) = floating_at_rest(GIB).await;
        store.delete_volume("pv-1").await.unwrap();
        let error = service.controller_publish_volume(publish("pv-1", "b")).await.unwrap_err();
        assert_eq!(error.code(), Code::FailedPrecondition);
        assert_eq!(store.get("pv-1").unwrap().spec.node, "a");
    }

    /// Bytes the volumes take of pool `default` on `node`.
    fn taken(store: &MemoryStore, node: &str) -> u64 {
        let volumes: Vec<LocalPathVolume> = store.volumes.lock().unwrap().values().cloned().collect();
        allocated_in_pool(&volumes, node, "default")
    }

    #[tokio::test]
    async fn volumes_created_at_the_same_time_do_not_take_the_same_room() {
        let (service, store) = service();
        let (one, two) = tokio::join!(
            service.create_volume(create("pv-1", "a", 60 * GIB, &[])),
            service.create_volume(create("pv-2", "a", 60 * GIB, &[])),
        );
        let refused: Vec<Code> = [one, two].into_iter().filter_map(Result::err).map(|e| e.code()).collect();
        assert_eq!(refused, [Code::ResourceExhausted], "one of the two fits, the other does not");
        assert_eq!(taken(&store, "a"), 60 * GIB);
    }

    #[tokio::test]
    async fn volume_created_while_another_grows_does_not_take_the_same_room() {
        let (service, store) = service();
        service.create_volume(create("pv-1", "a", 10 * GIB, &[])).await.unwrap();
        let (grown, created) = tokio::join!(
            service.controller_expand_volume(expand("pv-1", 60 * GIB)),
            service.create_volume(create("pv-2", "a", 60 * GIB, &[])),
        );
        assert!(grown.is_ok() != created.is_ok(), "one of the two fits: {grown:?}, {created:?}");
        assert!(taken(&store, "a") <= 100 * GIB, "{} bytes are taken of a pool of 100 GiB", taken(&store, "a"));
    }

    #[tokio::test]
    async fn volume_created_while_another_arrives_does_not_take_the_same_room() {
        let (service, store) = service();
        service.create_volume(create("pv-1", "a", 60 * GIB, &[("mobility", "Floating")])).await.unwrap();
        let (moved, created) = tokio::join!(
            service.controller_publish_volume(publish("pv-1", "b")),
            service.create_volume(create("pv-2", "b", 60 * GIB, &[])),
        );
        assert!(moved.is_ok() != created.is_ok(), "one of the two fits: {moved:?}, {created:?}");
        assert!(taken(&store, "b") <= 100 * GIB, "{} bytes are taken of a pool of 100 GiB", taken(&store, "b"));
    }

    #[test]
    fn progress_of_a_move_is_told_in_words() {
        let mut volume = LocalPathVolume::new("pv-1", LocalPathVolumeSpec { node: "b".into(), ..store_spec() });
        volume.metadata.generation = Some(3);
        volume.status = Some(crate::api::LocalPathVolumeStatus {
            node: Some("a".into()),
            used_bytes: Some(3 * GIB),
            ..Default::default()
        });
        let transfer = |state, received_bytes, message: Option<&str>| crate::api::Transfer {
            source: "a".into(),
            target: "b".into(),
            state,
            observed_generation: Some(3),
            received_bytes,
            started_at: None,
            message: message.map(str::to_string),
        };
        let mut told = |transfer: Option<crate::api::Transfer>| {
            volume.status.as_mut().unwrap().transfer = transfer;
            progress(&volume, "b")
        };
        assert_eq!(
            told(None),
            Notice::normal("VolumeMoving", "waiting for the agent of node b to start copying from node a")
        );
        assert_eq!(
            told(Some(transfer(TransferState::Copying, Some(GIB + GIB / 2), None))),
            Notice::normal("VolumeMoving", "copying from node a to node b: 1.5 GiB of about 3.0 GiB")
        );
        assert_eq!(
            told(Some(transfer(TransferState::Copying, None, None))),
            Notice::normal("VolumeMoving", "copying from node a to node b")
        );
        assert_eq!(
            told(Some(transfer(TransferState::Queued, None, Some("node b is receiving other volumes")))),
            Notice::normal("VolumeMoving", "waiting for its turn: node b is receiving other volumes")
        );
        assert_eq!(
            told(Some(transfer(TransferState::Failed, None, Some("node a: volume pv-1 is still mounted")))),
            Notice::warning("VolumeMoveFailed", "node a: volume pv-1 is still mounted; node b tries again")
        );
        // A report about a copy from another node is not about this move.
        let mut other = transfer(TransferState::Copied, None, None);
        other.source = "c".into();
        assert_eq!(told(Some(other)).note, "waiting for the agent of node b to start copying from node a");
        // Nor is one about a copy made before the volume was called back and sent here again.
        for generation in [Some(2), None] {
            let mut earlier = transfer(TransferState::Copied, None, None);
            earlier.observed_generation = generation;
            assert_eq!(told(Some(earlier)).note, "waiting for the agent of node b to start copying from node a");
        }
    }

    #[test]
    fn sizes_are_told_in_binary_units() {
        assert_eq!(human(0), "0 B");
        assert_eq!(human(1023), "1023 B");
        assert_eq!(human(1024), "1.0 KiB");
        assert_eq!(human(1536 * 1024), "1.5 MiB");
        assert_eq!(human(100 * GIB), "100.0 GiB");
        assert_eq!(human(5000 * GIB), "4.9 TiB");
    }

    /// The reconciler of a controller that leads, or does not, and what tells it which.
    fn relocating(
        service: &Arc<ControllerService<MemoryStore>>,
        leads: bool,
    ) -> (tokio::task::JoinHandle<()>, tokio::sync::watch::Sender<bool>) {
        let (leader, leading) = tokio::sync::watch::channel(leads);
        (tokio::spawn(service.clone().relocate(leading)), leader)
    }

    /// The controller was restarted while a volume was on its way, or the attacher waits between
    /// two attempts: nobody has called, and the volume must not wait for a call.
    #[tokio::test(start_paused = true)]
    async fn volume_is_handed_over_with_no_call_waiting_for_it() {
        let (service, store) = floating_at_rest(GIB).await;
        store.set_target(&store.get("pv-1").unwrap(), "b").await.unwrap();
        let _reconciler = relocating(&service, true);
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert_eq!(store.get("pv-1").unwrap().data_node(), "a", "the copy is not complete yet");

        store.report_transfer("pv-1", TransferState::Copied, None);
        until(&store, "the volume is handed over to node b", |v| v.data_node() == "b").await;
        let status = store.get("pv-1").unwrap().status.unwrap();
        assert_eq!((status.moves, status.phase, status.attached_node), (1, Some(Phase::Pending), None));
    }

    #[tokio::test(start_paused = true)]
    async fn move_is_called_off_with_no_call_waiting_for_it() {
        let (service, store) = floating_at_rest(GIB).await;
        store.set_target(&store.get("pv-1").unwrap(), "b").await.unwrap();
        store.report_transfer("pv-1", TransferState::Copying, None);
        // The pod is gone, and so is the call that had sent the volume.
        store.unwanted.lock().unwrap().insert(("pv-1".into(), "b".into()));
        let _reconciler = relocating(&service, true);
        until(&store, "the volume stays on node a", |v| v.spec.node == "a").await;
        let volume = store.get("pv-1").unwrap();
        assert_eq!((volume.data_node(), volume.moves()), ("a", 0));
        assert_eq!(free(&service, "b").await, 100 * GIB, "node b keeps no room for it");
    }

    #[tokio::test(start_paused = true)]
    async fn replica_that_does_not_lead_moves_nothing_until_it_does() {
        let (service, store) = floating_at_rest(GIB).await;
        store.set_target(&store.get("pv-1").unwrap(), "b").await.unwrap();
        store.report_transfer("pv-1", TransferState::Copied, None);
        let before = store.get("pv-1").unwrap().metadata.resource_version;
        let (_reconciler, leader) = relocating(&service, false);
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(store.get("pv-1").unwrap().metadata.resource_version, before, "nothing is written to the volume");

        leader.send_replace(true);
        until(&store, "the volume is handed over to node b", |v| v.data_node() == "b").await;
    }

    #[tokio::test(start_paused = true)]
    async fn call_and_reconciler_hand_a_volume_over_once() {
        let (service, store) = floating_at_rest(GIB).await;
        let _reconciler = relocating(&service, true);
        let call = publishing(&service, "b");
        until(&store, "the volume is sent to node b", |v| v.spec.node == "b").await;
        store.report_transfer("pv-1", TransferState::Copied, None);
        until(&store, "the volume is handed over to node b", |v| v.data_node() == "b").await;
        store.report_ready("pv-1", "b");
        call.await.unwrap().unwrap();
        let status = store.get("pv-1").unwrap().status.unwrap();
        assert_eq!((status.moves, status.attached_node.as_deref()), (1, Some("b")));
    }

    /// The volume was handed over, and the call that waited for it is gone with the controller
    /// it was made to. The next call finds the data on the node already - and the agent of that
    /// node still has to make the copy the volume.
    #[tokio::test(start_paused = true)]
    async fn volume_that_was_handed_over_is_published_when_its_node_has_made_it_the_volume() {
        let (service, store) = floating_at_rest(GIB).await;
        store.set_target(&store.get("pv-1").unwrap(), "b").await.unwrap();
        store.report_transfer("pv-1", TransferState::Copied, None);
        store.commit_move(&store.get("pv-1").unwrap(), "b").await.unwrap();

        let call = publishing(&service, "b");
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(!call.is_finished(), "node b has not made the copy the volume yet");
        assert_eq!(store.get("pv-1").unwrap().status.unwrap().attached_node, None);
        // The agent of the former node writes its last report late: it must not count.
        store.report_ready("pv-1", "a");
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(!call.is_finished());

        store.report_ready("pv-1", "b");
        call.await.unwrap().unwrap();
        assert_eq!(store.get("pv-1").unwrap().status.unwrap().attached_node.as_deref(), Some("b"));
    }

    /// A server that is told to stop waits for the calls it is answering, and a call that waits
    /// for a volume to arrive may take hours.
    #[tokio::test(start_paused = true)]
    async fn call_that_waits_for_a_move_is_answered_when_the_controller_stops() {
        let store = Arc::new(MemoryStore::default());
        store.add_pool("a", "default", 100 * GIB);
        store.add_pool("b", "default", 100 * GIB);
        let spec = LocalPathVolumeSpec { mobility: Mobility::Floating, size_bytes: GIB, ..store_spec() };
        store.create_volume("pv-1", spec).await.unwrap();
        store.set_data_node("pv-1", "a").await.unwrap();
        store.report_ready("pv-1", "a");
        let (stop, stopping) = tokio::sync::watch::channel(false);
        let mut service = ControllerService::new(store.clone()).with_stopping(stopping);
        (service.poll, service.attachment_check) = (Duration::from_millis(2), Duration::from_millis(2));
        let service = Arc::new(service);

        let call = publishing(&service, "b");
        until(&store, "the volume is sent to node b", |v| v.spec.node == "b").await;
        stop.send_replace(true);
        let refusal = tokio::time::timeout(Duration::from_secs(1), call).await.expect("the call is answered");
        assert_eq!(refusal.unwrap().unwrap_err().code(), Code::Unavailable);
        assert_eq!(store.get("pv-1").unwrap().spec.node, "b", "the volume goes on: the next call finds it on its way");
    }

    #[tokio::test(start_paused = true)]
    async fn call_that_waits_for_an_agent_is_answered_when_the_controller_stops() {
        // No agent: the directory of a new volume is never made.
        let store = Arc::new(MemoryStore::default());
        store.add_pool("a", "default", 100 * GIB);
        let (stop, stopping) = tokio::sync::watch::channel(false);
        let service = Arc::new(ControllerService::new(store.clone()).with_stopping(stopping));
        let call = tokio::spawn({
            let service = service.clone();
            async move { service.create_volume(create("pv-1", "a", GIB, &[])).await }
        });
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert!(!call.is_finished());
        stop.send_replace(true);
        let refusal = tokio::time::timeout(Duration::from_secs(1), call).await.expect("the call is answered");
        assert_eq!(refusal.unwrap().unwrap_err().code(), Code::Unavailable);
    }

    /// A volume of 10 GiB whose agent has made it so, on a store without an agent: the test
    /// writes what the agent would.
    async fn reported_ready() -> (ControllerService<MemoryStore>, Arc<MemoryStore>) {
        let store = Arc::new(MemoryStore::default());
        store.add_pool("a", "default", 100 * GIB);
        store.create_volume("pv-1", LocalPathVolumeSpec { size_bytes: 10 * GIB, ..store_spec() }).await.unwrap();
        store.set_data_node("pv-1", "a").await.unwrap();
        store.report_ready("pv-1", "a");
        let mut service = ControllerService::new(store.clone());
        (service.wait, service.poll) = (Duration::from_millis(50), Duration::from_millis(2));
        (service, store)
    }

    /// What an agent has said of a volume holds for the spec the volume had then. That the size
    /// it has applied is large enough says so of the size alone.
    #[tokio::test]
    async fn volume_is_ready_when_its_agent_says_so_of_the_spec_it_has_now() {
        let (_, store) = reported_ready().await;
        store.report_condition("pv-1", "True", "Provisioned", 0);
        assert!(ready(store.get("pv-1").as_ref()).unwrap());

        store.report_condition("pv-1", "True", "Provisioned", 1);
        assert!(!ready(store.get("pv-1").as_ref()).unwrap(), "said of the spec before the last change");
        let status = store.get("pv-1").unwrap().status.unwrap();
        assert_eq!((status.phase, status.capacity_bytes), (Some(Phase::Ready), Some(10 * GIB)), "by these it would be");
    }

    #[tokio::test]
    async fn condition_of_the_agent_tells_a_volume_that_is_not_ready_from_one_that_is_lost() {
        let (_, store) = reported_ready().await;
        for (status, reason) in [("False", "PoolNotReady"), ("False", "InvalidDirMode"), ("Unknown", "PoolNotReady")] {
            store.report_condition("pv-1", status, reason, 0);
            assert!(!ready(store.get("pv-1").as_ref()).unwrap(), "{status} {reason}");
        }
        store.report_condition("pv-1", "False", "DirectoryLost", 0);
        let lost = ready(store.get("pv-1").as_ref()).unwrap_err();
        assert_eq!((lost.code(), lost.message()), (Code::DataLoss, "DirectoryLost in words"));

        // Brought back and waiting for its approval: in place, as before.
        store.report_condition("pv-1", "False", "AwaitingApproval", 0);
        assert!(ready(store.get("pv-1").as_ref()).unwrap());
        store.report_condition("pv-1", "False", "AwaitingApproval", 1);
        assert!(!ready(store.get("pv-1").as_ref()).unwrap());
    }

    #[tokio::test]
    async fn expansion_waits_for_the_agent_to_say_that_it_has_applied_the_new_size() {
        let (service, store) = reported_ready().await;
        store.report_condition("pv-1", "True", "Provisioned", 0);
        // Nobody plays the agent: the size is written, and what the agent had said is of the old one.
        let refusal = service.controller_expand_volume(expand("pv-1", 20 * GIB)).await.unwrap_err();
        assert_eq!(refusal.code(), Code::Aborted, "{}", refusal.message());
        assert_eq!(store.get("pv-1").unwrap().spec.size_bytes, 20 * GIB);

        store.report_ready("pv-1", "a");
        store.report_condition("pv-1", "True", "Provisioned", 0);
        service.controller_expand_volume(expand("pv-1", 20 * GIB)).await.unwrap();
    }

    #[tokio::test]
    async fn handover_takes_back_what_the_former_node_said_of_the_volume() {
        let (_, store) = floating_at_rest(GIB).await;
        store.report_condition("pv-1", "True", "Provisioned", 0);
        store.set_target(&store.get("pv-1").unwrap(), "b").await.unwrap();
        store.commit_move(&store.get("pv-1").unwrap(), "b").await.unwrap();
        let volume = store.get("pv-1").unwrap();
        assert_eq!(volume.condition("Ready"), None);
        assert!(!ready(Some(&volume)).unwrap());
    }

    #[test]
    fn failure_of_the_store_is_answered_by_what_the_sidecar_is_to_do_about_it() {
        let refused = |code: u16| {
            let message = "leases is forbidden".to_string();
            Status::from(StoreError::Refused { code, reason: "Reason".into(), message })
        };
        // Another writer was first, the object is there already, or it is gone: repeat and read anew.
        assert_eq!(refused(409).code(), Code::Aborted);
        assert_eq!(refused(404).code(), Code::Aborted);
        // The rights of the service account: no repeat helps.
        assert_eq!(refused(403).code(), Code::PermissionDenied);
        assert_eq!(refused(401).code(), Code::PermissionDenied);
        // What the driver wrote is not valid.
        assert_eq!(refused(422).code(), Code::Internal);
        assert_eq!(refused(400).code(), Code::Internal);
        // A server that is busy or failing.
        for code in [429, 500, 503, 504] {
            assert_eq!(refused(code).code(), Code::Unavailable, "{code}");
        }
        assert!(refused(403).message().contains("leases is forbidden"), "{}", refused(403).message());

        let behind = StoreError::NotShown { volume: "pv-1".into(), waited: Duration::from_secs(5) };
        assert_eq!(Status::from(behind).code(), Code::Unavailable);
        let unversioned = StoreError::Unversioned { volume: "pv-1".into() };
        assert_eq!(Status::from(unversioned).code(), Code::Internal);
    }

    #[tokio::test]
    async fn volume_that_is_there_already_is_answered_with_a_call_to_repeat() {
        let (_, store) = reported_ready().await;
        // What a second replica gets when the first has created the volume and its watch has
        // not shown that yet.
        let spec = store.get("pv-1").unwrap().spec;
        let refusal: Status = store.create_volume("pv-1", spec).await.unwrap_err().into();
        assert_eq!(refusal.code(), Code::Aborted, "{}", refusal.message());
    }

    #[tokio::test]
    async fn pool_that_takes_no_volumes_says_which_of_the_reasons_it_is() {
        let (_, store) = service();
        let node = store.node("a").await.unwrap();
        let now = crate::unix_now();
        let free = |node: Option<&crate::api::LocalPathNode>, name: &str, pool: &str, now: u64| {
            free_in_pool(node, &[], name, pool, now, 120)
        };
        assert_eq!(free(node.as_ref(), "a", "default", now), Ok(100 * GIB));
        assert_eq!(free(None, "z", "default", now), Err(Closed::NoAgent { node: "z".into() }));
        let no_pool = Closed::NoPool { node: "a".into(), pool: "fast".into() };
        assert_eq!(free(node.as_ref(), "a", "fast", now), Err(no_pool));
        let silent = free(node.as_ref(), "a", "default", now + 500).unwrap_err();
        assert!(matches!(&silent, Closed::Silent { node, age } if node == "a" && *age >= 500), "{silent:?}");
        assert!(silent.to_string().starts_with("the agent on node a has not reported for 50"), "{silent}");

        store.age_report("a", None);
        let node = store.node("a").await.unwrap();
        assert_eq!(free(node.as_ref(), "a", "default", now), Err(Closed::NeverReported { node: "a".into() }));
    }
}
