//! The room of a pool: what its volumes leave of it, and who takes of what is left.

use tonic::Status;

use super::ControllerService;
use super::provision::when_ready;
use crate::api::{LocalPathNode, LocalPathVolume, PoolState};
use crate::store::Store;

/// Why a pool of a node takes no volumes.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Closed {
    #[error("node {node} runs no agent")]
    NoAgent { node: String },
    /// The report outlives the agent that wrote it: without a fresh one nobody is there to
    /// create a directory or to change a limit.
    #[error("the agent on node {node} has not reported for {age} seconds")]
    Silent { node: String, age: u64 },
    #[error("the agent on node {node} has not reported yet")]
    NeverReported { node: String },
    #[error("node {node} has no pool {pool}")]
    NoPool { node: String, pool: String },
    #[error("pool {pool} on node {node} is {state:?}")]
    NotReady { node: String, pool: String, state: PoolState },
}

/// Bytes volumes of `pool` on `node` may still take: what the agent reports as allocatable minus
/// the sizes of the volumes placed there. `Err` says why the pool takes no volumes.
pub fn free_in_pool(
    local_node: Option<&LocalPathNode>,
    volumes: &[LocalPathVolume],
    node: &str,
    pool: &str,
    unix_now: u64,
    agent_timeout_seconds: u64,
) -> Result<u64, Closed> {
    let (node, pool) = (node.to_string(), pool.to_string());
    let Some(local_node) = local_node else { return Err(Closed::NoAgent { node }) };
    let reported = local_node.status.as_ref();
    match reported.and_then(|reported| reported.report_age(unix_now)) {
        Some(age) if age <= agent_timeout_seconds => {}
        Some(age) => return Err(Closed::Silent { node, age }),
        None => return Err(Closed::NeverReported { node }),
    }
    let Some(status) = reported.and_then(|reported| reported.pools.iter().find(|p| p.name == pool)) else {
        return Err(Closed::NoPool { node, pool });
    };
    if status.state != PoolState::Ready {
        return Err(Closed::NotReady { node, pool, state: status.state });
    }
    let (node, pool) = (node.as_str(), pool.as_str());
    Ok(status.allocatable_bytes.saturating_sub(allocated_in_pool(volumes, node, pool)))
}

/// Bytes of `pool` on `node` that are spoken for: the sizes of the volumes whose data is there
/// and of the ones on their way there.
pub fn allocated_in_pool(volumes: &[LocalPathVolume], node: &str, pool: &str) -> u64 {
    let there = |v: &&LocalPathVolume| v.spec.pool == pool && (v.data_node() == node || v.spec.node == node);
    volumes.iter().filter(there).map(|v| v.spec.size_bytes).sum()
}

impl<S: Store> ControllerService<S> {
    /// See [`free_in_pool`]; an error of the inner result says why the pool takes no volumes.
    pub(super) async fn free_bytes(&self, node: &str, pool: &str) -> Result<Result<u64, Closed>, Status> {
        let local_node = self.store.node(node).await?;
        let volumes = self.store.volumes().await?;
        Ok(free_in_pool(local_node.as_ref(), &volumes, node, pool, crate::unix_now(), self.agent_timeout.as_secs()))
    }

    /// Makes `requested` the size of the volume, unless the volume is that large already.
    async fn grow(&self, name: &str, requested: u64) -> Result<(), Status> {
        // The size the volume has is read under the lock: what it grows by is taken of the pool.
        let _placing = self.placing.lock().await;
        // And the new one is written only if nobody has changed the volume since: of two requests
        // that read the same size the one that writes last would otherwise win, also when it
        // asks for less than the other has got.
        for _ in 0..10 {
            let volume = self.store.volume(name).await?;
            let volume = volume.ok_or_else(|| Status::not_found(format!("volume {name} does not exist")))?;
            let current = volume.spec.size_bytes;
            if requested <= current {
                return Ok(());
            }
            // A refusal here is UNAVAILABLE, not FAILED_PRECONDITION: for an expansion that one
            // means "the volume is in use, expand it offline", and the resizer stops trying
            // while a pod has the volume.
            // A volume on its way takes its size of two pools: where its data is, and where it is sent.
            let (pool, more) = (&volume.spec.pool, requested - current);
            for node in [Some(volume.data_node()), volume.destination()].into_iter().flatten() {
                let free =
                    self.free_bytes(node, pool).await?.map_err(|closed| Status::unavailable(closed.to_string()))?;
                if more > free {
                    return Err(Status::out_of_range(format!(
                        "pool {pool} on node {node} has {free} bytes left, {more} more requested"
                    )));
                }
            }
            if self.store.set_size(&volume, requested).await? {
                tracing::info!(volume = %name, from = current, to = requested, "expanding volume");
                return Ok(());
            }
        }
        Err(Status::aborted(format!("volume {name} keeps changing")))
    }

    /// Makes `requested` the size of the volume and waits for the agent of its node to apply it.
    pub(super) async fn expand(&self, name: &str, requested: u64) -> Result<LocalPathVolume, Status> {
        self.grow(name, requested).await?;
        self.wait_for(name, "apply the new size", when_ready).await
    }
}
