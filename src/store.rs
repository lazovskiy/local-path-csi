//! Where the controller keeps volumes: custom resources in the cluster, a map in tests.

use std::fmt;
use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;

use futures_util::{StreamExt, TryStreamExt};
use k8s_openapi::api::storage::v1::VolumeAttachment;
use kube::Resource;
use kube::api::{Api, DeleteParams, ListParams, Patch, PatchParams, PostParams};
use kube::runtime::reflector::store::Writer;
use kube::runtime::reflector::{self, ObjectRef, reflector};
use kube::runtime::{WatchStreamExt, watcher};
use serde::de::DeserializeOwned;
use serde_json::json;
use tokio::time::Instant;

use crate::api::{LocalPathNode, LocalPathVolume, LocalPathVolumeSpec};
use crate::sightings::Sightings;

/// Why the store could not do what it was asked. What whoever asked is to do about it differs
/// by the kind: see `From<StoreError>` for the answer to a CSI call.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// The API server answered, and with a refusal: `code` is the HTTP status of the answer.
    #[error("{reason} ({code}): {message}")]
    Refused { code: u16, reason: String, message: String },
    /// The API server could not be asked, or its answer could not be read.
    #[error(transparent)]
    Client(kube::Error),
    /// The write went through, and the watch has not shown it in time: what is read next may
    /// be older than what was written.
    #[error("volume {volume} is written, but the watch has not shown that for {waited:?}")]
    NotShown { volume: String, waited: Duration },
    /// A conditional write of a volume that carries no version to be the condition.
    #[error("volume {volume} was read without a resource version")]
    Unversioned { volume: String },
    #[error("volume without a name")]
    Unnamed,
}

impl From<kube::Error> for StoreError {
    fn from(error: kube::Error) -> Self {
        match error {
            kube::Error::Api(status) => {
                Self::Refused { code: status.code, reason: status.reason.clone(), message: status.message.clone() }
            }
            other => Self::Client(other),
        }
    }
}

/// The volumes and the nodes as they are: what everything that reads them needs, and all the
/// metrics page does.
pub trait Inventory: Send + Sync + 'static {
    fn volume(&self, name: &str) -> impl Future<Output = Result<Option<LocalPathVolume>, StoreError>> + Send;
    fn volumes(&self) -> impl Future<Output = Result<Vec<LocalPathVolume>, StoreError>> + Send;
    fn node(&self, name: &str) -> impl Future<Output = Result<Option<LocalPathNode>, StoreError>> + Send;
    fn nodes(&self) -> impl Future<Output = Result<Vec<LocalPathNode>, StoreError>> + Send;
}

/// What the controller decides about a volume.
pub trait Volumes: Send + Sync + 'static {
    /// Creates the volume with the data finalizer; its data is to appear on `spec.node`. A volume
    /// of this name that is there already is left as it is, and the call fails.
    fn create_volume(
        &self,
        name: &str,
        spec: LocalPathVolumeSpec,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;
    /// Records the node that holds the data of the volume.
    fn set_data_node(&self, name: &str, node: &str) -> impl Future<Output = Result<(), StoreError>> + Send;
    /// Asks for removal of the volume; a volume that is not there is removed already.
    fn delete_volume(&self, name: &str) -> impl Future<Output = Result<(), StoreError>> + Send;
    /// Lets a volume marked for deletion go although no agent has removed its data.
    fn release_volume(&self, name: &str) -> impl Future<Output = Result<(), StoreError>> + Send;
    /// Sets the size the volume has to have. Like the four calls below it changes the volume
    /// only if nothing has changed it since `volume` was read, and returns `false` otherwise:
    /// two calls that decide about one volume must not undo each other.
    fn set_size(
        &self,
        volume: &LocalPathVolume,
        size_bytes: u64,
    ) -> impl Future<Output = Result<bool, StoreError>> + Send;
    /// Sends the volume to `node`: the agent there starts fetching its data.
    fn set_target(&self, volume: &LocalPathVolume, node: &str)
    -> impl Future<Output = Result<bool, StoreError>> + Send;
    /// Hands the volume over to `node`, which has a complete copy: from now on the data is there.
    fn commit_move(
        &self,
        volume: &LocalPathVolume,
        node: &str,
    ) -> impl Future<Output = Result<bool, StoreError>> + Send;
    /// Records that the volume is published to `node`.
    fn attach(&self, volume: &LocalPathVolume, node: &str) -> impl Future<Output = Result<bool, StoreError>> + Send;
    /// Records that the volume is published nowhere.
    fn detach(&self, volume: &LocalPathVolume) -> impl Future<Output = Result<bool, StoreError>> + Send;
}

/// What the rest of the cluster says: its nodes, its claims, and who waits for a volume.
pub trait Cluster: Send + Sync + 'static {
    /// Whether something waits for the volume on `node`: the cluster wants it attached there,
    /// and a pod that uses its claim is placed there and is not on its way out. Kubernetes gives
    /// up on an attachment only when the attach in flight has timed out, minutes after its pod
    /// is gone; the pods tell at once.
    fn attachment_wanted(&self, volume: &str, node: &str) -> impl Future<Output = Result<bool, StoreError>> + Send;
    /// Whether the cluster has a Node of this name.
    fn node_exists(&self, name: &str) -> impl Future<Output = Result<bool, StoreError>> + Send;
    /// StorageClass of a PersistentVolumeClaim; `None` when the claim is gone or names no class.
    fn claim_storage_class(
        &self,
        namespace: &str,
        name: &str,
    ) -> impl Future<Output = Result<Option<String>, StoreError>> + Send;
    /// PersistentVolume a PersistentVolumeClaim is bound to.
    fn claim_volume(
        &self,
        namespace: &str,
        name: &str,
    ) -> impl Future<Output = Result<Option<String>, StoreError>> + Send;
}

/// All of the above: what the CSI controller works with.
pub trait Store: Inventory + Volumes + Cluster {}

impl<T: Inventory + Volumes + Cluster> Store for T {}

/// Whether `pod` is alive and mounts the claim `claim` of its namespace.
pub fn pod_waits_for(pod: &k8s_openapi::api::core::v1::Pod, claim: &str) -> bool {
    let phase = pod.status.as_ref().and_then(|status| status.phase.as_deref());
    if pod.metadata.deletion_timestamp.is_some() || matches!(phase, Some("Succeeded" | "Failed")) {
        return false;
    }
    let pod_name = pod.metadata.name.as_deref().unwrap_or_default();
    let volumes = pod.spec.iter().flat_map(|spec| spec.volumes.iter().flatten());
    volumes.into_iter().any(|volume| match (&volume.persistent_volume_claim, &volume.ephemeral) {
        (Some(source), _) => source.claim_name == claim,
        // The claim of a generic ephemeral volume is named after the pod and the volume.
        (None, Some(_)) => format!("{pod_name}-{}", volume.name) == claim,
        _ => false,
    })
}

/// What the watches of the controller have seen: every read of a volume or a node is a look in
/// here, and no request.
pub struct Caches {
    pub volumes: reflector::Store<LocalPathVolume>,
    pub nodes: reflector::Store<LocalPathNode>,
    /// The attachments of this driver only.
    pub attachments: reflector::Store<VolumeAttachment>,
    /// When the agents of the nodes were seen to report.
    sightings: Arc<Sightings>,
}

/// What fills the caches: the watches of `KubeStore::start`, or a test.
pub struct Feeds {
    pub volumes: Writer<LocalPathVolume>,
    pub nodes: Writer<LocalPathNode>,
    pub attachments: Writer<VolumeAttachment>,
    /// Told of every node before the node is put into the cache.
    pub sightings: Arc<Sightings>,
}

pub fn caches() -> (Caches, Feeds) {
    let (volumes, volume_feed) = reflector::store();
    let (nodes, node_feed) = reflector::store();
    let (attachments, attachment_feed) = reflector::store();
    let sightings = Arc::new(Sightings::default());
    let feeds =
        Feeds { volumes: volume_feed, nodes: node_feed, attachments: attachment_feed, sightings: sightings.clone() };
    (Caches { volumes, nodes, attachments, sightings }, feeds)
}

/// Watches the objects of `api` into the cache behind `feed`, until the process is stopped.
/// Only what `kept` lets through is kept, and `seen` is told of it before the cache has it.
fn follow<K>(
    api: Api<K>,
    feed: Writer<K>,
    what: &'static str,
    kept: fn(&K) -> bool,
    seen: impl Fn(&watcher::Event<K>) + Send + 'static,
) where
    K: Resource + Clone + DeserializeOwned + fmt::Debug + Send + Sync + 'static,
    K::DynamicType: Eq + Hash + Clone + Default + Send + Sync,
{
    tokio::spawn(async move {
        let events = watcher(api, watcher::Config::default()).default_backoff();
        // Who has written which field is a large part of an object, and nothing reads it here.
        let events = events.modify(|object| object.meta_mut().managed_fields = None);
        let events = events.try_filter(move |event| {
            std::future::ready(match event {
                watcher::Event::Apply(object) | watcher::Event::InitApply(object) | watcher::Event::Delete(object) => {
                    kept(object)
                }
                watcher::Event::Init | watcher::Event::InitDone => true,
            })
        });
        let events = events.inspect_ok(seen);
        let mut events = std::pin::pin!(reflector(feed, events));
        while let Some(event) = events.next().await {
            if let Err(e) = event {
                tracing::warn!(error = %e, "watch of {what} failed, retrying");
            }
        }
    });
}

/// Whether a cache that holds a volume at version `cached` has caught up with a write that left
/// the volume at `written`. Versions grow with every change in the cluster; one that is not a
/// number can only be compared for being the same.
fn caught_up(cached: Option<&str>, written: Option<&str>) -> bool {
    match (cached, written) {
        (Some(cached), Some(written)) => match (cached.parse::<u64>(), written.parse::<u64>()) {
            (Ok(cached), Ok(written)) => cached >= written,
            _ => cached == written,
        },
        _ => false,
    }
}

/// Volumes and nodes as custom resources. They are read from the caches of watches and written
/// to the API server.
///
/// A call reads, decides and writes, and the next call must read what the last has written:
/// the room of a pool is counted from the volumes that are read. So a write returns only when
/// the cache shows it.
pub struct KubeStore {
    client: kube::Client,
    volumes: Api<LocalPathVolume>,
    caches: Caches,
    /// How long a write waits for the watch to show it.
    shown_within: Duration,
    /// How long a write that lost to another waits for the watch to show that other.
    moved_within: Duration,
}

impl KubeStore {
    pub fn new(client: kube::Client, caches: Caches) -> Self {
        Self {
            volumes: Api::all(client.clone()),
            client,
            caches,
            shown_within: Duration::from_secs(5),
            moved_within: Duration::from_secs(1),
        }
    }

    pub fn with_patience(mut self, shown_within: Duration, moved_within: Duration) -> Self {
        (self.shown_within, self.moved_within) = (shown_within, moved_within);
        self
    }

    /// Starts the watches and returns when each has listed what there is.
    pub async fn start(client: kube::Client) -> Self {
        let (caches, feeds) = caches();
        follow(Api::<LocalPathVolume>::all(client.clone()), feeds.volumes, "LocalPathVolumes", |_| true, |_| {});
        let sightings = feeds.sightings;
        let report = move |event: &watcher::Event<LocalPathNode>| match event {
            watcher::Event::Apply(node) | watcher::Event::InitApply(node) => {
                sightings.observe(node, Instant::now().into_std(), crate::unix_now());
            }
            watcher::Event::Delete(node) => sightings.forget(node.metadata.name.as_deref().unwrap_or_default()),
            watcher::Event::Init | watcher::Event::InitDone => {}
        };
        follow(Api::<LocalPathNode>::all(client.clone()), feeds.nodes, "LocalPathNodes", |_| true, report);
        // Every driver of the cluster has its attachments among them.
        let own = |attachment: &VolumeAttachment| attachment.spec.attacher == crate::driver_name();
        follow(Api::<VolumeAttachment>::all(client.clone()), feeds.attachments, "VolumeAttachments", own, |_| {});
        // The feeds live in tasks that never end.
        let _ = caches.volumes.wait_until_ready().await;
        let _ = caches.nodes.wait_until_ready().await;
        let _ = caches.attachments.wait_until_ready().await;
        Self::new(client, caches)
    }

    /// The node with the time of its report as this process saw it: when the report was seen to
    /// change, in the clock of this process. The agent wrote it by the clock of its node, and
    /// what asks how old a report is must not compare the two.
    fn as_seen(&self, node: &LocalPathNode) -> LocalPathNode {
        let mut node = node.clone();
        let name = node.metadata.name.as_deref().unwrap_or_default();
        let age = self.caches.sightings.age(name, Instant::now().into_std());
        if let Some(status) = node.status.as_mut() {
            status.reported_at = age.map(|age| crate::api::time(crate::unix_now().saturating_sub(age)));
        }
        node
    }

    fn cached(&self, name: &str) -> Option<Arc<LocalPathVolume>> {
        self.caches.volumes.get(&ObjectRef::new(name))
    }

    /// Waits until the cache holds what `seen` accepts, for at most `within`.
    async fn until_cached(
        &self,
        name: &str,
        within: Duration,
        seen: impl Fn(Option<&LocalPathVolume>) -> bool,
    ) -> bool {
        let deadline = Instant::now() + within;
        loop {
            if seen(self.cached(name).as_deref()) {
                return true;
            }
            if Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// Returns when the cache shows the write that left the volume as `written`.
    async fn shown(&self, written: &LocalPathVolume, write: Write) -> Result<(), StoreError> {
        let name = written.metadata.name.as_deref().unwrap_or_default();
        let version = written.metadata.resource_version.as_deref();
        let seen = |cached: Option<&LocalPathVolume>| match cached {
            Some(cached) => caught_up(cached.metadata.resource_version.as_deref(), version),
            // A volume that was there before the write and is gone from the cache has been
            // deleted since: the cache is past the write then.
            None => matches!(write, Write::Change),
        };
        if self.until_cached(name, self.shown_within, seen).await {
            return Ok(());
        }
        Err(StoreError::NotShown { volume: name.to_string(), waited: self.shown_within })
    }
}

/// What a write did to a volume.
#[derive(Clone, Copy)]
enum Write {
    Create,
    Change,
}

/// The part of a volume a patch is for: the status is written through a request of its own.
#[derive(Clone, Copy)]
enum Part {
    Spec,
    Status,
}

fn not_found(error: &kube::Error) -> bool {
    matches!(error, kube::Error::Api(response) if response.code == 404)
}

/// Adds to a merge patch the condition that the volume is still as `volume` was read: the
/// resource version in a patch is a condition to the API server. A volume without one cannot
/// be patched this way: `null` in its place is no condition at all, and the patch would be
/// applied whatever has happened to the volume.
fn unless_changed(volume: &LocalPathVolume, mut patch: serde_json::Value) -> Result<serde_json::Value, StoreError> {
    let name = volume.metadata.name.as_deref().unwrap_or_default();
    let version = volume.metadata.resource_version.as_deref().filter(|version| !version.is_empty());
    let version = version.ok_or_else(|| StoreError::Unversioned { volume: name.to_string() })?;
    patch["metadata"] = json!({"resourceVersion": version});
    Ok(patch)
}

impl KubeStore {
    /// Applies a merge patch to the volume or to its status, unless the volume has changed since
    /// `volume` was read.
    async fn patch_unchanged(
        &self,
        volume: &LocalPathVolume,
        part: Part,
        patch: serde_json::Value,
    ) -> Result<bool, StoreError> {
        let name = volume.metadata.name.as_deref().ok_or(StoreError::Unnamed)?;
        let patch = unless_changed(volume, patch)?;
        let (params, patch) = (PatchParams::default(), Patch::Merge(&patch));
        let written = match part {
            Part::Status => self.volumes.patch_status(name, &params, &patch).await,
            Part::Spec => self.volumes.patch(name, &params, &patch).await,
        };
        match written {
            Ok(written) => self.shown(&written, Write::Change).await.map(|()| true),
            Err(kube::Error::Api(response)) if response.code == 409 || response.code == 404 => {
                // The caller reads the volume again and decides anew: it must not read the one it
                // has just lost with. When the watch is slow it does, and loses once more.
                let read = volume.metadata.resource_version.as_deref();
                let moved = |cached: Option<&LocalPathVolume>| {
                    cached.is_none_or(|cached| cached.metadata.resource_version.as_deref() != read)
                };
                self.until_cached(name, self.moved_within, moved).await;
                Ok(false)
            }
            Err(e) => Err(e.into()),
        }
    }
}

impl Inventory for KubeStore {
    async fn volume(&self, name: &str) -> Result<Option<LocalPathVolume>, StoreError> {
        Ok(self.cached(name).map(|volume| volume.as_ref().clone()))
    }

    async fn volumes(&self) -> Result<Vec<LocalPathVolume>, StoreError> {
        Ok(self.caches.volumes.state().iter().map(|volume| volume.as_ref().clone()).collect())
    }

    async fn node(&self, name: &str) -> Result<Option<LocalPathNode>, StoreError> {
        Ok(self.caches.nodes.get(&ObjectRef::new(name)).map(|node| self.as_seen(&node)))
    }

    async fn nodes(&self) -> Result<Vec<LocalPathNode>, StoreError> {
        Ok(self.caches.nodes.state().iter().map(|node| self.as_seen(node)).collect())
    }
}

impl Volumes for KubeStore {
    async fn create_volume(&self, name: &str, spec: LocalPathVolumeSpec) -> Result<(), StoreError> {
        let mut volume = LocalPathVolume::new(name, spec);
        volume.metadata.finalizers = Some(vec![crate::finalizer().to_string()]);
        let created = self.volumes.create(&PostParams::default(), &volume).await?;
        self.shown(&created, Write::Create).await
    }

    async fn set_data_node(&self, name: &str, node: &str) -> Result<(), StoreError> {
        let patch = json!({"status": {"node": node}});
        let written = self.volumes.patch_status(name, &PatchParams::default(), &Patch::Merge(&patch)).await?;
        self.shown(&written, Write::Change).await
    }

    async fn delete_volume(&self, name: &str) -> Result<(), StoreError> {
        match self.volumes.delete(name, &DeleteParams::default()).await {
            Ok(_) => Ok(()),
            Err(e) if not_found(&e) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    async fn release_volume(&self, name: &str) -> Result<(), StoreError> {
        let Some(volume) = self.volumes.get_opt(name).await? else { return Ok(()) };
        // Fails when the finalizers have changed since the read: the sidecar repeats the call.
        let Some(patch) = volume.without_finalizer() else { return Ok(()) };
        match self.volumes.patch(name, &PatchParams::default(), &Patch::Json::<()>(patch)).await {
            Ok(_) => Ok(()),
            Err(e) if not_found(&e) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    async fn set_size(&self, volume: &LocalPathVolume, size_bytes: u64) -> Result<bool, StoreError> {
        self.patch_unchanged(volume, Part::Spec, json!({"spec": {"sizeBytes": size_bytes}})).await
    }

    async fn set_target(&self, volume: &LocalPathVolume, node: &str) -> Result<bool, StoreError> {
        self.patch_unchanged(volume, Part::Spec, json!({"spec": {"node": node}})).await
    }

    async fn commit_move(&self, volume: &LocalPathVolume, node: &str) -> Result<bool, StoreError> {
        // What the agent of the former node reported does not describe the copy on the new one.
        let status = json!({
            "node": node, "moves": volume.moves() + 1, "transfer": null,
            "phase": "Pending", "capacityBytes": null, "usedBytes": null, "message": null, "reportedBy": null,
            "conditions": null,
        });
        self.patch_unchanged(volume, Part::Status, json!({"status": status})).await
    }

    async fn attach(&self, volume: &LocalPathVolume, node: &str) -> Result<bool, StoreError> {
        self.patch_unchanged(volume, Part::Status, json!({"status": {"attachedNode": node}})).await
    }

    async fn detach(&self, volume: &LocalPathVolume) -> Result<bool, StoreError> {
        self.patch_unchanged(volume, Part::Status, json!({"status": {"attachedNode": null}})).await
    }
}

impl Cluster for KubeStore {
    async fn attachment_wanted(&self, volume: &str, node: &str) -> Result<bool, StoreError> {
        let wanted = |a: &VolumeAttachment| {
            a.metadata.deletion_timestamp.is_none()
                && a.spec.attacher == crate::driver_name()
                && a.spec.node_name == node
                && a.spec.source.persistent_volume_name.as_deref() == Some(volume)
        };
        if self.caches.attachments.find(|a| wanted(a)).is_none() {
            // The attacher calls for an attachment it has seen, and this watch may be behind
            // its: before a move is given up for an attachment that is not there, the API
            // server is asked.
            let attachments: Api<VolumeAttachment> = Api::all(self.client.clone());
            if !attachments.list(&ListParams::default()).await?.items.iter().any(wanted) {
                return Ok(false);
            }
        }
        // The claim as the PersistentVolume has it now: a volume may have been bound anew.
        let volumes: Api<k8s_openapi::api::core::v1::PersistentVolume> = Api::all(self.client.clone());
        let claim = volumes.get_opt(volume).await?.and_then(|pv| pv.spec).and_then(|spec| spec.claim_ref);
        let Some((namespace, claim)) = claim.and_then(|c| Some((c.namespace?, c.name?))) else { return Ok(true) };
        let pods: Api<k8s_openapi::api::core::v1::Pod> = Api::namespaced(self.client.clone(), &namespace);
        let on_node = ListParams::default().fields(&format!("spec.nodeName={node}"));
        Ok(pods.list(&on_node).await?.items.iter().any(|pod| pod_waits_for(pod, &claim)))
    }

    async fn node_exists(&self, name: &str) -> Result<bool, StoreError> {
        let nodes: Api<k8s_openapi::api::core::v1::Node> = Api::all(self.client.clone());
        Ok(nodes.get_opt(name).await?.is_some())
    }

    async fn claim_storage_class(&self, namespace: &str, name: &str) -> Result<Option<String>, StoreError> {
        let claims: Api<k8s_openapi::api::core::v1::PersistentVolumeClaim> =
            Api::namespaced(self.client.clone(), namespace);
        Ok(claims.get_opt(name).await?.and_then(|claim| claim.spec).and_then(|spec| spec.storage_class_name))
    }

    async fn claim_volume(&self, namespace: &str, name: &str) -> Result<Option<String>, StoreError> {
        let claims: Api<k8s_openapi::api::core::v1::PersistentVolumeClaim> =
            Api::namespaced(self.client.clone(), namespace);
        Ok(claims.get_opt(name).await?.and_then(|claim| claim.spec).and_then(|spec| spec.volume_name))
    }
}

#[cfg(test)]
mod tests {
    use k8s_openapi::api::core::v1::Pod;
    use serde_json::json;

    use super::{pod_waits_for, unless_changed};
    use crate::api::LocalPathVolume;

    fn pod(extra: serde_json::Value) -> Pod {
        let mut pod = json!({
            "metadata": {"name": "app-0"},
            "spec": {"containers": [], "volumes": [
                {"name": "data", "persistentVolumeClaim": {"claimName": "data"}},
                {"name": "scratch", "ephemeral": {"volumeClaimTemplate": {"spec": {}}}},
                {"name": "config", "configMap": {"name": "data"}},
            ]},
        });
        for (key, value) in extra.as_object().unwrap() {
            pod[key.as_str()] = value.clone();
        }
        serde_json::from_value(pod).unwrap()
    }

    #[test]
    fn patch_with_a_condition_is_not_sent_without_one() {
        let spec = serde_json::from_value(json!({"pool": "default", "sizeBytes": 1, "node": "a", "dirMode": "0770"}));
        let mut volume = LocalPathVolume::new("pv-1", spec.unwrap());
        let patch = || json!({"status": {"attachedNode": "b"}});
        // An object that did not come from the API server, or came without its version.
        for missing in [None, Some(String::new())] {
            volume.metadata.resource_version = missing;
            let refusal = unless_changed(&volume, patch()).unwrap_err();
            assert!(
                refusal.to_string().contains("pv-1") && refusal.to_string().contains("resource version"),
                "{refusal}"
            );
        }
        volume.metadata.resource_version = Some("41".into());
        assert_eq!(
            unless_changed(&volume, patch()).unwrap(),
            json!({"status": {"attachedNode": "b"}, "metadata": {"resourceVersion": "41"}})
        );
    }

    #[test]
    fn pod_waits_for_the_claims_it_mounts_while_it_is_alive() {
        let alive = pod(json!({}));
        assert!(pod_waits_for(&alive, "data"));
        assert!(pod_waits_for(&alive, "app-0-scratch"), "the claim of a generic ephemeral volume");
        assert!(!pod_waits_for(&alive, "other"));
        assert!(!pod_waits_for(&alive, "scratch"));
        assert!(!pod_waits_for(&alive, "app-0-config"), "a ConfigMap is not a claim");

        let pending = pod(json!({"status": {"phase": "Pending"}}));
        assert!(pod_waits_for(&pending, "data"));
        for phase in ["Succeeded", "Failed"] {
            assert!(!pod_waits_for(&pod(json!({"status": {"phase": phase}})), "data"), "{phase}");
        }
        let leaving = pod(json!({"metadata": {"name": "app-0", "deletionTimestamp": "2026-11-23T20:00:00Z"}}));
        assert!(!pod_waits_for(&leaving, "data"), "a pod that is being deleted waits for nothing");
    }
}

/// A store in memory. With `agent` it also plays the agents of all nodes: a created volume
/// becomes ready, a resized one gets its new size, a deleted one disappears, a volume sent to
/// another node is copied there and, once handed over, becomes ready there - all at once.
#[cfg(test)]
pub mod memory {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::Mutex;

    use super::*;
    use crate::api::{
        LocalPathNodeStatus, LocalPathVolumeStatus, Phase, PoolState, PoolStatus, Transfer, TransferState,
    };

    /// A refusal as the API server words one.
    fn refusal(code: u16, reason: &str, message: String) -> StoreError {
        StoreError::Refused { code, reason: reason.to_string(), message }
    }

    #[derive(Default)]
    pub struct MemoryStore {
        pub agent: bool,
        pub volumes: Mutex<BTreeMap<String, LocalPathVolume>>,
        pub nodes: Mutex<BTreeMap<String, LocalPathNode>>,
        /// Names of the Node objects of the cluster.
        pub cluster_nodes: Mutex<BTreeSet<String>>,
        /// StorageClass of claims, by (namespace, name).
        pub claims: Mutex<BTreeMap<(String, String), String>>,
        /// PersistentVolume of bound claims, by (namespace, name).
        pub bound: Mutex<BTreeMap<(String, String), String>>,
        /// Attachments the cluster has given up on, as (volume, node).
        pub unwanted: Mutex<BTreeSet<(String, String)>>,
        /// Counts the changes: every one gives the volume a new resource version.
        pub version: Mutex<u64>,
        /// What somebody else does between a read of a call and the write that follows it: run
        /// once, right before the next write.
        pub meanwhile: Mutex<Option<Interference>>,
    }

    pub type Interference = Box<dyn FnOnce(&MemoryStore) + Send>;

    impl MemoryStore {
        pub fn with_agent() -> Self {
            Self { agent: true, ..Default::default() }
        }

        /// Adds a node with one ready pool of `allocatable_bytes`.
        pub fn add_pool(&self, node: &str, pool: &str, allocatable_bytes: u64) {
            self.add_pool_in_state(node, pool, allocatable_bytes, PoolState::Ready);
        }

        pub fn add_pool_in_state(&self, node: &str, pool: &str, allocatable_bytes: u64, state: PoolState) {
            self.cluster_nodes.lock().unwrap().insert(node.to_string());
            let mut nodes = self.nodes.lock().unwrap();
            let entry = nodes.entry(node.to_string()).or_insert_with(|| LocalPathNode::new(node, Default::default()));
            let status = entry.status.get_or_insert_with(LocalPathNodeStatus::default);
            status.reported_at = Some(crate::api::time(crate::unix_now()));
            status.pools.push(PoolStatus {
                name: pool.to_string(),
                path: format!("/mnt/{pool}"),
                state,
                quota: true,
                fs_type: Some("xfs".into()),
                capacity_bytes: allocatable_bytes,
                allocatable_bytes,
                available_bytes: allocatable_bytes,
            });
        }

        /// Makes the last report of the agent of `node` this old; `None` erases it.
        pub fn age_report(&self, node: &str, seconds: Option<u64>) {
            let mut nodes = self.nodes.lock().unwrap();
            let status = nodes.get_mut(node).and_then(|n| n.status.as_mut()).expect("node with a report");
            status.reported_at = seconds.map(|s| crate::api::time(crate::unix_now() - s));
        }

        pub fn get(&self, name: &str) -> Option<LocalPathVolume> {
            self.volumes.lock().unwrap().get(name).cloned()
        }

        /// Changes a volume the way another writer would: with a new resource version.
        pub fn change(&self, name: &str, change: impl FnOnce(&mut LocalPathVolume)) {
            self.update(name, change).expect("the volume");
        }

        /// Lets `meanwhile` happen, when something is to.
        fn interfere(&self) {
            let meanwhile = self.meanwhile.lock().unwrap().take();
            if let Some(meanwhile) = meanwhile {
                meanwhile(self);
            }
        }

        fn update(&self, name: &str, change: impl FnOnce(&mut LocalPathVolume)) -> Result<(), StoreError> {
            self.update_held(&mut self.volumes.lock().unwrap(), name, change)
        }

        /// `update` by a caller that holds the volumes.
        fn update_held(
            &self,
            volumes: &mut BTreeMap<String, LocalPathVolume>,
            name: &str,
            change: impl FnOnce(&mut LocalPathVolume),
        ) -> Result<(), StoreError> {
            let volume = volumes
                .get_mut(name)
                .ok_or_else(|| refusal(404, "NotFound", format!("localpathvolumes {name:?} not found")))?;
            let spec = volume.spec.clone();
            change(volume);
            if volume.spec != spec {
                // As the API server does: a new generation with every change of the spec.
                volume.metadata.generation = Some(volume.metadata.generation.unwrap_or(0) + 1);
            }
            if self.agent {
                let (size, holder) = (volume.spec.size_bytes, volume.data_node().to_string());
                let transfer = volume.destination().map(|target| Transfer {
                    source: holder.clone(),
                    target: target.to_string(),
                    state: TransferState::Copied,
                    observed_generation: volume.metadata.generation,
                    received_bytes: None,
                    started_at: None,
                    message: None,
                });
                let status = volume.status.get_or_insert_with(LocalPathVolumeStatus::default);
                status.phase = Some(Phase::Ready);
                status.capacity_bytes = Some(size);
                status.reported_by = Some(holder);
                status.transfer = transfer;
            }
            self.stamp(volume);
            Ok(())
        }

        fn stamp(&self, volume: &mut LocalPathVolume) {
            let mut version = self.version.lock().unwrap();
            *version += 1;
            volume.metadata.resource_version = Some(version.to_string());
        }

        /// `update`, unless the volume has changed since `read` was read.
        fn update_unchanged(
            &self,
            read: &LocalPathVolume,
            change: impl FnOnce(&mut LocalPathVolume),
        ) -> Result<bool, StoreError> {
            let name = read.metadata.name.as_deref().unwrap_or_default();
            // As the API server does: the version is compared and the volume written in one step.
            let mut volumes = self.volumes.lock().unwrap();
            let current = volumes.get(name).map(|v| v.metadata.resource_version.clone());
            if current != Some(read.metadata.resource_version.clone()) {
                return Ok(false);
            }
            self.update_held(&mut volumes, name, change).map(|()| true)
        }

        /// What the agent of the node a volume is sent to reports about its copy.
        pub fn report_transfer(&self, name: &str, state: TransferState, message: Option<&str>) {
            let mut volumes = self.volumes.lock().unwrap();
            let volume = volumes.get_mut(name).expect("the volume");
            let (source, target) = (volume.data_node().to_string(), volume.spec.node.clone());
            let message = message.map(str::to_string);
            let observed_generation = volume.metadata.generation;
            let transfer = Transfer {
                source,
                target,
                state,
                observed_generation,
                received_bytes: None,
                started_at: None,
                message,
            };
            volume.status.get_or_insert_with(LocalPathVolumeStatus::default).transfer = Some(transfer);
            self.stamp(volume);
        }

        /// What an agent of today writes with the phase: the condition `Ready`, about the
        /// generation the volume has now less `behind`.
        pub fn report_condition(&self, name: &str, status: &str, reason: &str, behind: i64) {
            let mut volumes = self.volumes.lock().unwrap();
            let volume = volumes.get_mut(name).expect("the volume");
            let observed_generation = volume.metadata.generation.map(|generation| generation - behind);
            let condition = k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition {
                type_: crate::api::READY.to_string(),
                status: status.to_string(),
                reason: reason.to_string(),
                message: format!("{reason} in words"),
                observed_generation,
                last_transition_time: k8s_openapi::apimachinery::pkg::apis::meta::v1::Time(
                    k8s_openapi::jiff::Timestamp::UNIX_EPOCH,
                ),
            };
            volume.status.get_or_insert_with(LocalPathVolumeStatus::default).conditions = vec![condition];
            self.stamp(volume);
        }

        /// What the agent of `node` does once the volume is handed over to it.
        pub fn report_ready(&self, name: &str, node: &str) {
            let mut volumes = self.volumes.lock().unwrap();
            let volume = volumes.get_mut(name).expect("the volume");
            let size = volume.spec.size_bytes;
            let status = volume.status.get_or_insert_with(LocalPathVolumeStatus::default);
            (status.phase, status.capacity_bytes, status.reported_by) =
                (Some(Phase::Ready), Some(size), Some(node.to_string()));
            self.stamp(volume);
        }
    }

    /// Every call to the API server is a round trip, and other calls run meanwhile: without it
    /// two calls made at the same time in a test would run one after the other.
    async fn round_trip() {
        tokio::task::yield_now().await;
    }

    impl Inventory for MemoryStore {
        async fn volume(&self, name: &str) -> Result<Option<LocalPathVolume>, StoreError> {
            round_trip().await;
            Ok(self.get(name))
        }

        async fn volumes(&self) -> Result<Vec<LocalPathVolume>, StoreError> {
            round_trip().await;
            Ok(self.volumes.lock().unwrap().values().cloned().collect())
        }

        async fn node(&self, name: &str) -> Result<Option<LocalPathNode>, StoreError> {
            round_trip().await;
            Ok(self.nodes.lock().unwrap().get(name).cloned())
        }

        async fn nodes(&self) -> Result<Vec<LocalPathNode>, StoreError> {
            round_trip().await;
            Ok(self.nodes.lock().unwrap().values().cloned().collect())
        }
    }

    impl Volumes for MemoryStore {
        async fn create_volume(&self, name: &str, spec: LocalPathVolumeSpec) -> Result<(), StoreError> {
            round_trip().await;
            self.interfere();
            let mut volumes = self.volumes.lock().unwrap();
            // As the API server does: 409 AlreadyExists, and the volume stays as it was.
            if volumes.contains_key(name) {
                return Err(refusal(409, "AlreadyExists", format!("localpathvolumes {name:?} already exists")));
            }
            let mut volume = LocalPathVolume::new(name, spec);
            volume.metadata.finalizers = Some(vec![crate::finalizer().to_string()]);
            volume.metadata.generation = Some(1);
            self.stamp(&mut volume);
            volumes.insert(name.to_string(), volume);
            Ok(())
        }

        async fn set_data_node(&self, name: &str, node: &str) -> Result<(), StoreError> {
            round_trip().await;
            self.interfere();
            self.update(name, |v| {
                v.status.get_or_insert_with(LocalPathVolumeStatus::default).node = Some(node.to_string())
            })
        }

        async fn delete_volume(&self, name: &str) -> Result<(), StoreError> {
            round_trip().await;
            self.interfere();
            let mut volumes = self.volumes.lock().unwrap();
            if self.agent {
                volumes.remove(name);
            } else if let Some(volume) = volumes.get_mut(name) {
                // Without an agent the finalizer keeps the object: it is only marked for deletion.
                let epoch = k8s_openapi::jiff::Timestamp::UNIX_EPOCH;
                volume.metadata.deletion_timestamp = Some(k8s_openapi::apimachinery::pkg::apis::meta::v1::Time(epoch));
            }
            Ok(())
        }

        async fn release_volume(&self, name: &str) -> Result<(), StoreError> {
            round_trip().await;
            self.interfere();
            let mut volumes = self.volumes.lock().unwrap();
            if volumes.get(name).is_some_and(|v| v.metadata.deletion_timestamp.is_some()) {
                volumes.remove(name);
            }
            Ok(())
        }

        async fn set_size(&self, volume: &LocalPathVolume, size_bytes: u64) -> Result<bool, StoreError> {
            round_trip().await;
            self.interfere();
            self.update_unchanged(volume, |v| v.spec.size_bytes = size_bytes)
        }

        async fn set_target(&self, volume: &LocalPathVolume, node: &str) -> Result<bool, StoreError> {
            round_trip().await;
            self.interfere();
            self.update_unchanged(volume, |v| v.spec.node = node.to_string())
        }

        async fn commit_move(&self, volume: &LocalPathVolume, node: &str) -> Result<bool, StoreError> {
            round_trip().await;
            self.interfere();
            self.update_unchanged(volume, |v| {
                let status = v.status.get_or_insert_with(LocalPathVolumeStatus::default);
                *status = LocalPathVolumeStatus {
                    node: Some(node.to_string()),
                    attached_node: status.attached_node.clone(),
                    phase: Some(Phase::Pending),
                    moves: status.moves + 1,
                    ..Default::default()
                };
            })
        }

        async fn attach(&self, volume: &LocalPathVolume, node: &str) -> Result<bool, StoreError> {
            round_trip().await;
            self.interfere();
            let node = Some(node.to_string());
            self.update_unchanged(volume, |v| {
                v.status.get_or_insert_with(LocalPathVolumeStatus::default).attached_node = node
            })
        }

        async fn detach(&self, volume: &LocalPathVolume) -> Result<bool, StoreError> {
            round_trip().await;
            self.interfere();
            self.update_unchanged(volume, |v| {
                v.status.get_or_insert_with(LocalPathVolumeStatus::default).attached_node = None
            })
        }
    }

    impl Cluster for MemoryStore {
        async fn attachment_wanted(&self, volume: &str, node: &str) -> Result<bool, StoreError> {
            round_trip().await;
            Ok(!self.unwanted.lock().unwrap().contains(&(volume.to_string(), node.to_string())))
        }

        async fn node_exists(&self, name: &str) -> Result<bool, StoreError> {
            round_trip().await;
            Ok(self.cluster_nodes.lock().unwrap().contains(name))
        }

        async fn claim_storage_class(&self, namespace: &str, name: &str) -> Result<Option<String>, StoreError> {
            round_trip().await;
            Ok(self.claims.lock().unwrap().get(&(namespace.to_string(), name.to_string())).cloned())
        }

        async fn claim_volume(&self, namespace: &str, name: &str) -> Result<Option<String>, StoreError> {
            round_trip().await;
            Ok(self.bound.lock().unwrap().get(&(namespace.to_string(), name.to_string())).cloned())
        }
    }

    #[cfg(test)]
    mod tests {
        use std::sync::Barrier;

        use super::*;

        fn spec(size_bytes: u64) -> LocalPathVolumeSpec {
            let spec = serde_json::json!({"pool": "default", "sizeBytes": size_bytes, "node": "a", "dirMode": "0770"});
            serde_json::from_value(spec).unwrap()
        }

        /// The API server answers 409 AlreadyExists, and the object stays as it was.
        #[tokio::test]
        async fn volume_that_exists_is_not_created_again() {
            let store = MemoryStore::default();
            store.create_volume("pv-1", spec(1)).await.unwrap();
            let first = store.get("pv-1").unwrap();
            let refusal = store.create_volume("pv-1", spec(2)).await.unwrap_err();
            assert!(
                refusal.to_string().contains("pv-1") && refusal.to_string().contains("already exists"),
                "{refusal}"
            );
            assert_eq!(store.get("pv-1").unwrap(), first, "the volume is as the first call made it");
        }

        /// The API server compares the version and writes in one step: of the writes that were
        /// decided on one read, one goes through.
        #[test]
        fn of_the_writes_based_on_one_read_one_goes_through() {
            const WRITERS: usize = 8;
            let store = MemoryStore::default();
            let mut volume = LocalPathVolume::new("pv-1", spec(0));
            store.stamp(&mut volume);
            store.volumes.lock().unwrap().insert("pv-1".into(), volume);
            for round in 0..2000 {
                let read = store.get("pv-1").unwrap();
                let start = Barrier::new(WRITERS);
                let written = std::thread::scope(|scope| {
                    let writers: Vec<_> = (0..WRITERS)
                        .map(|_| {
                            scope.spawn(|| {
                                start.wait();
                                store.update_unchanged(&read, |v| v.spec.size_bytes += 1).unwrap()
                            })
                        })
                        .collect();
                    writers.into_iter().map(|w| w.join().unwrap()).filter(|w| *w).count()
                });
                assert_eq!(written, 1, "round {round}");
                assert_eq!(store.get("pv-1").unwrap().spec.size_bytes, round + 1, "round {round}");
            }
        }
    }
}
