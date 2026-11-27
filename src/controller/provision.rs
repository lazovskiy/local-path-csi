//! Creating and deleting a volume: the controller records what it wants and waits for the agent.

use tokio::time::Instant;
use tonic::Status;

use super::ControllerService;
use super::request::{access_mode, check_capabilities, parse_spec, requested_size, selected_node};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition;

use crate::api::{LocalPathVolume, LocalPathVolumeSpec, Phase, READY, REASON_AWAITING_APPROVAL, REASON_DIRECTORY_LOST};
use crate::csi::CreateVolumeRequest;
use crate::events::Notice;
use crate::store::Store;
use crate::volumes::valid_name;

/// Whether the agent of the node that holds the volume has made it what its spec says.
pub(super) fn ready(volume: Option<&LocalPathVolume>) -> Result<bool, Status> {
    let Some(volume) = volume else { return Err(Status::aborted("the volume was deleted while it was being created")) };
    if let Some(condition) = volume.condition(READY) {
        return ready_by(volume, condition);
    }
    // An agent of an earlier version writes no condition: its phase and the size it has applied
    // are what there is.
    let Some(status) = &volume.status else { return Ok(false) };
    match status.phase {
        // A volume that was brought back is in place as well: its agent writes `Ready` once it
        // sees that the volume is approved, and a pod may ask before that.
        Some(Phase::Ready | Phase::Orphaned) => {
            Ok(status.capacity_bytes.is_some_and(|applied| applied >= volume.spec.size_bytes))
        }
        Some(Phase::Lost) => {
            Err(Status::data_loss(status.message.as_deref().unwrap_or("the volume directory is gone")))
        }
        Some(Phase::Pending) | None => Ok(false),
    }
}

/// `ready` by the condition of the agent. The condition names the generation of the volume it
/// is about: what an agent said of a volume before its spec was changed says nothing of the
/// volume as it has to be now.
fn ready_by(volume: &LocalPathVolume, condition: &Condition) -> Result<bool, Status> {
    let of_this_spec = condition.observed_generation.is_some_and(|seen| Some(seen) == volume.metadata.generation);
    match (condition.status.as_str(), condition.reason.as_str()) {
        ("False", REASON_DIRECTORY_LOST) => {
            let gone = Some(condition.message.clone()).filter(|message| !message.is_empty());
            Err(Status::data_loss(gone.unwrap_or_else(|| "the volume directory is gone".into())))
        }
        // A volume that was brought back is in place as well, as above.
        ("True", _) | ("False", REASON_AWAITING_APPROVAL) => Ok(of_this_spec),
        _ => Ok(false),
    }
}

/// The volume, once the agent of its node has made it what its spec says: what `wait_for` waits
/// for when a volume is created or resized.
pub(super) fn when_ready(volume: Option<&LocalPathVolume>) -> Result<Option<LocalPathVolume>, Status> {
    Ok(if ready(volume)? { volume.cloned() } else { None })
}

impl<S: Store> ControllerService<S> {
    /// Polls the volume until `done` has something to return, for at most `self.wait`.
    pub(super) async fn wait_for<T>(
        &self,
        name: &str,
        waiting_for: &str,
        done: impl Fn(Option<&LocalPathVolume>) -> Result<Option<T>, Status>,
    ) -> Result<T, Status> {
        let deadline = Instant::now() + self.wait;
        loop {
            let volume = self.store.volume(name).await?;
            if let Some(done) = done(volume.as_ref())? {
                return Ok(done);
            }
            if Instant::now() >= deadline {
                let node = volume.as_ref().map(|v| v.data_node().to_string()).unwrap_or_default();
                return Err(Status::aborted(format!(
                    "volume {name}: still waiting for the agent on node {node} to {waiting_for}"
                )));
            }
            self.pause(self.poll).await?;
        }
    }

    /// Records the volume a request asks for, unless it is recorded already, and waits for the
    /// agent of its node to make the directory. Returns the volume under its name.
    pub(super) async fn create(&self, request: CreateVolumeRequest) -> Result<(String, LocalPathVolume), Status> {
        if !valid_name(&request.name) {
            return Err(Status::invalid_argument(format!("volume name {:?} cannot be a directory name", request.name)));
        }
        check_capabilities(&request.volume_capabilities)?;
        let size_bytes = requested_size(request.capacity_range.as_ref())?;
        let node = selected_node(request.accessibility_requirements.as_ref())?;
        let mut spec = parse_spec(&request.parameters, node, size_bytes, access_mode(&request.volume_capabilities))?;
        if let Some(claim) = &spec.claim {
            spec.storage_class = self.store.claim_storage_class(&claim.namespace, &claim.name).await?;
        }
        let name = request.name;

        match self.store.volume(&name).await? {
            Some(existing) => {
                // A retry of the same request; anything else under this name is a conflict.
                let same = LocalPathVolumeSpec { node: existing.spec.node.clone(), ..spec.clone() } == existing.spec;
                if !same {
                    return Err(Status::already_exists(format!("volume {name} exists with other parameters")));
                }
            }
            None => {
                let _placing = self.placing.lock().await;
                match self.free_bytes(&spec.node, &spec.pool).await? {
                    Ok(free) if free >= size_bytes => {}
                    Ok(free) => {
                        return Err(Status::resource_exhausted(format!(
                            "pool {} on node {} has {free} bytes left, {size_bytes} requested",
                            spec.pool, spec.node
                        )));
                    }
                    Err(closed) => return Err(Status::resource_exhausted(closed.to_string())),
                }
                tracing::info!(volume = %name, node = %spec.node, pool = %spec.pool, size_bytes, "creating volume");
                self.store.create_volume(&name, spec.clone()).await?;
            }
        }
        // Also repairs a volume whose creation was interrupted before its node was recorded.
        let current = self.store.volume(&name).await?;
        if let Some(current) = current.filter(|v| v.status.as_ref().and_then(|s| s.node.as_ref()).is_none()) {
            self.store.set_data_node(&name, &current.spec.node).await?;
        }

        let volume = self.wait_for(&name, "create the directory", when_ready).await?;
        Ok((name, volume))
    }

    /// Asks for the removal of a volume and waits until its data is gone.
    pub(super) async fn delete(&self, name: &str) -> Result<(), Status> {
        let Some(volume) = self.store.volume(name).await? else {
            return Ok(());
        };
        tracing::info!(volume = %name, "deleting volume");
        self.store.delete_volume(name).await?;
        // A node that left the cluster for good has no agent to remove the data. Should the node
        // come back, its agent finds the directory and brings the volume back.
        let node = volume.data_node();
        if !self.store.node_exists(node).await? {
            tracing::warn!(volume = %name, %node, "the node is gone from the cluster: the volume is deleted without removing its data");
            if let Some(events) = &self.events {
                let note = format!(
                    "node {node} is gone from the cluster: the volume is deleted, its data stays on the disk of that node"
                );
                events.volume(&volume, &Notice::warning("DeletedWithoutNode", &note)).await;
            }
            self.store.release_volume(name).await?;
        }
        self.wait_for(name, "remove the directory", |volume| Ok(volume.is_none().then_some(()))).await?;
        Ok(())
    }
}
