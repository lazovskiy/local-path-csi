//! Publishing a volume to a node, which for a volume that follows its pod means moving it there
//! first, and taking the publication back.

use tokio::time::Instant;
use tonic::Status;

use super::ControllerService;
use super::provision::ready;
use super::relocation::{copied, progress};
use crate::api::{LocalPathVolume, Mobility};
use crate::events::Notice;
use crate::store::Store;

/// Whether the agent of `node`, which the volume was handed over to, has made its copy the volume.
fn settled(volume: &LocalPathVolume, node: &str) -> Result<bool, Status> {
    // What the agent of the former node wrote last does not count.
    if volume.status.as_ref().and_then(|status| status.reported_by.as_deref()) != Some(node) {
        return Ok(false);
    }
    ready(Some(volume))
}

/// Whether the volume was handed over to `node` and the agent there has not said yet that it has
/// made its copy the volume: what the agent of the former node wrote is taken back with the
/// handover, and nothing has been written since.
fn arriving(volume: &LocalPathVolume, node: &str) -> bool {
    let reported_by = volume.status.as_ref().and_then(|status| status.reported_by.as_deref());
    volume.data_node() == node && volume.destination().is_none() && volume.moves() > 0 && reported_by != Some(node)
}

impl<S: Store> ControllerService<S> {
    /// Publishes a volume to the node its data is on.
    async fn publish_in_place(&self, name: &str, node: &str) -> Result<(), Status> {
        // Each step is taken only if nobody has changed the volume meanwhile.
        for _ in 0..10 {
            let volume = self.store.volume(name).await?;
            let volume = volume.ok_or_else(|| Status::not_found(format!("volume {name} does not exist")))?;
            let attached = volume.status.as_ref().and_then(|s| s.attached_node.as_deref());
            if attached == Some(node) && volume.destination().is_none() {
                return Ok(());
            }
            if let Some(other) = attached.filter(|other| *other != node) {
                return Err(Status::failed_precondition(format!("volume {name} is still published to node {other}")));
            }
            if volume.data_node() != node {
                return Err(Status::aborted(format!("volume {name} was handed over to node {}", volume.data_node())));
            }
            if let Some(other) = volume.destination() {
                // The volume was sent to another node and the pod came back before it left.
                tracing::info!(volume = %name, %node, abandoned = %other, "the volume stays where it is");
                self.store.set_target(&volume, node).await?;
                continue;
            }
            if self.store.attach(&volume, node).await? {
                tracing::info!(volume = %name, %node, "published");
                return Ok(());
            }
        }
        Err(Status::aborted(format!("volume {name} keeps changing")))
    }

    /// Moves a volume to `node` and publishes it there. Returns when the data is on the node,
    /// however long that takes: an error would make Kubernetes wait ever longer between its
    /// attempts, and a pod would start long after its volume has arrived.
    ///
    /// The move does not hang on this call. What it takes - handing the volume over, calling the
    /// move off - the controller that leads does as well, for every volume on its way
    /// (`relocate`): the call may be gone, with the controller it was made to or with an
    /// attacher that waits between two attempts. Both write on condition that nobody has
    /// changed the volume, so the one that comes second only looks again.
    async fn publish_after_move(&self, volume: LocalPathVolume, node: &str) -> Result<(), Status> {
        let name = volume.metadata.name.clone().unwrap_or_default();
        let source = volume.data_node().to_string();
        let started = Instant::now();
        let unwanted = || Status::aborted(format!("volume {name}: no pod waits for it on node {node} any more"));
        if volume.spec.node != node {
            // Kubernetes repeats the call for a pod that is gone until its own attach has timed
            // out: the volume must not set out again each time.
            if !self.store.attachment_wanted(&name, node).await? {
                return Err(unwanted());
            }
            let (pool, size) = (&volume.spec.pool, volume.spec.size_bytes);
            let sent = {
                let _placing = self.placing.lock().await;
                match self.free_bytes(node, pool).await? {
                    Ok(free) if free >= size => Ok(self.store.set_target(&volume, node).await?),
                    Ok(free) => Err(format!("pool {pool} there has {free} bytes left, the volume takes {size}")),
                    Err(closed) => Err(closed.to_string()),
                }
            };
            match sent {
                Ok(true) => {}
                Ok(false) => return Err(Status::aborted(format!("volume {name} has changed, try again"))),
                Err(reason) => {
                    let reason = format!("volume {name} cannot move to node {node}: {reason}");
                    self.tell(&volume, node, &Notice::warning("VolumeMoveFailed", &reason)).await;
                    return Err(Status::resource_exhausted(reason));
                }
            }
            tracing::info!(volume = %name, from = %source, to = %node, "moving the volume");
            let note =
                format!("the volume moves from node {source} to node {node}: the pod starts when its data has arrived");
            self.tell(&volume, node, &Notice::normal("VolumeMoving", &note)).await;
        }

        let mut next_check = Instant::now();
        let mut next_note = Instant::now() + self.progress_note;
        let mut told_failure: Option<String> = None;
        loop {
            let volume = self.store.volume(&name).await?;
            let volume = volume.ok_or_else(|| Status::not_found(format!("volume {name} does not exist")))?;
            if volume.metadata.deletion_timestamp.is_some() {
                return Err(Status::aborted(format!("volume {name} is being deleted")));
            }
            if volume.spec.node != node {
                return Err(Status::aborted(format!("volume {name} was sent to node {} meanwhile", volume.spec.node)));
            }
            if volume.data_node() == node {
                // Handed over; the agent of the node makes the copy the volume.
                if settled(&volume, node)? && self.store.attach(&volume, node).await? {
                    let seconds = started.elapsed().as_secs();
                    tracing::info!(volume = %name, from = %source, to = %node, seconds, "moved and published");
                    let note = format!("the volume is on node {node} now");
                    self.tell(&volume, node, &Notice::normal("VolumeMoved", &note)).await;
                    return Ok(());
                }
            } else if copied(&volume) {
                // A lost race with another change only means another look at the volume.
                self.hand_over(&volume, node).await?;
                continue;
            } else {
                let notice = progress(&volume, node);
                let news = notice.is_warning() && told_failure.as_ref() != Some(&notice.note);
                if news || Instant::now() >= next_note {
                    self.tell(&volume, node, &notice).await;
                    told_failure = notice.is_warning().then_some(notice.note);
                    next_note = Instant::now() + self.progress_note;
                }
            }
            if Instant::now() >= next_check {
                next_check = Instant::now() + self.attachment_check;
                // The pod is gone or was placed elsewhere: nothing waits for the volume here.
                if !self.still_wanted(&volume, node).await? {
                    return Err(unwanted());
                }
            }
            self.pause(self.poll).await?;
        }
    }

    /// Publishes a volume to `node`: at once when its data is there, after a move when the volume
    /// follows its pod.
    pub(super) async fn publish(&self, volume_id: &str, node_id: &str) -> Result<(), Status> {
        let volume = self.store.volume(volume_id).await?;
        let volume = volume.ok_or_else(|| Status::not_found(format!("volume {volume_id} does not exist")))?;
        // By its mark, not by its phase: the controller takes the mark off when the volume is
        // approved, and the agent writes the phase after that.
        if volume.brought_back() {
            return Err(Status::failed_precondition(format!(
                "volume {volume_id} was brought back from a directory on node {}: that it is the volume of this name is the word of that node alone, and it is given to pods once an administrator has annotated the LocalPathVolume {}=true",
                volume.data_node(),
                crate::restore_approval()
            )));
        }
        let attached = volume.status.as_ref().and_then(|s| s.attached_node.clone());
        if let Some(attached) = attached.filter(|a| *a != node_id) {
            return Err(Status::failed_precondition(format!(
                "volume {volume_id} is still published to node {attached}"
            )));
        }
        if arriving(&volume, node_id) {
            // The call that had moved the volume is gone, with the controller it was made to.
            self.publish_after_move(volume, node_id).await?;
        } else if volume.data_node() == node_id {
            self.publish_in_place(volume_id, node_id).await?;
        } else if volume.spec.mobility == Mobility::Pinned {
            return Err(Status::failed_precondition(format!(
                "volume {volume_id} is pinned to node {}: only a volume of a StorageClass with mobility Floating follows its pod",
                volume.data_node()
            )));
        } else if volume.metadata.deletion_timestamp.is_some() {
            return Err(Status::failed_precondition(format!("volume {volume_id} is being deleted")));
        } else {
            self.publish_after_move(volume, node_id).await?;
        }
        Ok(())
    }

    /// Takes back the publication of a volume to `node`, or its way there.
    pub(super) async fn unpublish(&self, volume_id: &str, node_id: &str) -> Result<(), Status> {
        // Each step is taken only if nobody has changed the volume since it was read. A call that
        // comes late - Kubernetes repeats them - finds a volume another call has unpublished
        // and published elsewhere meanwhile: what it read says "published to this node", and
        // written without a condition its answer would take the newer attachment away.
        for _ in 0..10 {
            let Some(volume) = self.store.volume(volume_id).await? else {
                return Ok(());
            };
            let attached = volume.status.as_ref().and_then(|s| s.attached_node.as_deref());
            if attached == Some(node_id) {
                if self.store.detach(&volume).await? {
                    tracing::info!(volume = %volume_id, node = %node_id, "unpublished");
                    return Ok(());
                }
            } else if volume.destination() == Some(node_id) {
                // The volume was on its way to a node that needs it no more: it stays where it is.
                let source = volume.data_node().to_string();
                if self.store.set_target(&volume, &source).await? {
                    tracing::info!(volume = %volume_id, node = %node_id, "the move to the node is cancelled");
                    let note = format!("the move to node {node_id} is cancelled: no pod waits for the volume there");
                    self.tell(&volume, node_id, &Notice::normal("VolumeMoveCancelled", &note)).await;
                    return Ok(());
                }
            } else {
                // Published elsewhere or nowhere: nothing of this node is left to take away.
                return Ok(());
            }
        }
        Err(Status::aborted(format!("volume {volume_id} keeps changing")))
    }
}
