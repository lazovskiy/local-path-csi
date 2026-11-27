//! A volume on its way to another node: the steps that take it there or leave it where it is,
//! and what is told about it while it travels.
//!
//! The steps are taken by the call that waits for the volume (`publish`) and, with no call
//! there, by the controller that leads (`relocate`).

use std::sync::Arc;

use tokio::sync::watch;
use tonic::Status;

use super::ControllerService;
use crate::api::{LocalPathVolume, TransferState};
use crate::events::Notice;
use crate::store::Store;

/// Whether the node the volume was sent to reports a complete copy of it.
pub(super) fn copied(volume: &LocalPathVolume) -> bool {
    volume.transfer().is_some_and(|transfer| transfer.state == TransferState::Copied)
}

/// A size for people: `1.5 GiB`.
pub(super) fn human(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 { format!("{bytes} B") } else { format!("{value:.1} {}", UNITS[unit]) }
}

/// What to tell about a volume that is on its way to `node`.
pub(super) fn progress(volume: &LocalPathVolume, node: &str) -> Notice {
    let source = volume.data_node();
    let moving = |note: String| Notice::normal("VolumeMoving", note);
    let Some(transfer) = volume.transfer() else {
        return moving(format!("waiting for the agent of node {node} to start copying from node {source}"));
    };
    let detail = transfer.message.as_deref().unwrap_or_default();
    match transfer.state {
        TransferState::Queued => moving(format!("waiting for its turn: {detail}")),
        TransferState::Failed => Notice::warning("VolumeMoveFailed", format!("{detail}; node {node} tries again")),
        TransferState::Copied => moving(format!("the copy on node {node} is complete")),
        TransferState::Copying => {
            let total = volume.status.as_ref().and_then(|s| s.used_bytes);
            let note = match (transfer.received_bytes, total) {
                (Some(received), Some(total)) => {
                    format!("copying from node {source} to node {node}: {} of about {}", human(received), human(total))
                }
                (Some(received), None) => format!("copying from node {source} to node {node}: {}", human(received)),
                (None, _) => format!("copying from node {source} to node {node}"),
            };
            moving(note)
        }
    }
}

impl<S: Store> ControllerService<S> {
    pub(super) async fn tell(&self, volume: &LocalPathVolume, node: &str, notice: &Notice) {
        if let Some(events) = &self.events {
            events.volume_and_pods(volume, node, notice).await;
        }
    }

    /// Hands the volume over to `node`, which has a complete copy. Lost to another change of
    /// the volume this does nothing: whoever looks at the volume next decides anew.
    pub(super) async fn hand_over(&self, volume: &LocalPathVolume, node: &str) -> Result<(), Status> {
        if self.store.commit_move(volume, node).await? {
            let name = volume.metadata.name.as_deref().unwrap_or_default();
            tracing::info!(volume = %name, from = %volume.data_node(), to = %node, "the volume is handed over");
        }
        Ok(())
    }

    /// Whether something still waits for the volume on `node`. When nothing does, a volume on
    /// its way there is left where it is.
    pub(super) async fn still_wanted(&self, volume: &LocalPathVolume, node: &str) -> Result<bool, Status> {
        let name = volume.metadata.name.as_deref().unwrap_or_default();
        if self.store.attachment_wanted(name, node).await? {
            return Ok(true);
        }
        if volume.destination() == Some(node) {
            let source = volume.data_node();
            // Told by the one whose write went through: the call and the controller that leads
            // may both come here.
            if self.store.set_target(volume, source).await? {
                tracing::info!(volume = %name, to = %node, "nobody waits for the volume on the node: the move is cancelled");
                let note = format!("the move to node {node} is cancelled: no pod waits for the volume there");
                self.tell(volume, node, &Notice::normal("VolumeMoveCancelled", &note)).await;
            }
        }
        Ok(false)
    }

    /// Takes every volume on its way one step further, again and again, while this controller
    /// leads and until the process is told to stop: a volume whose copy is complete is handed
    /// over, a move nobody waits for is called off.
    pub async fn relocate(self: Arc<Self>, mut leading: watch::Receiver<bool>) {
        loop {
            if leading.wait_for(|leads| *leads).await.is_err() {
                return;
            }
            match self.store.volumes().await {
                Ok(volumes) => {
                    // A volume that is being deleted goes nowhere: its agent removes the data.
                    for volume in volumes.iter().filter(|volume| volume.metadata.deletion_timestamp.is_none()) {
                        let Some(node) = volume.destination() else { continue };
                        let step = async {
                            match copied(volume) {
                                true => self.hand_over(volume, node).await,
                                false => self.still_wanted(volume, node).await.map(|_| ()),
                            }
                        };
                        if let Err(e) = step.await {
                            let name = volume.metadata.name.as_deref().unwrap_or_default();
                            tracing::warn!(volume = %name, error = %e.message(), "cannot take the volume further on its way");
                        }
                    }
                }
                Err(e) => tracing::warn!(error = %e, "cannot read the volumes on their way"),
            }
            if self.pause(self.attachment_check).await.is_err() {
                return;
            }
        }
    }
}
