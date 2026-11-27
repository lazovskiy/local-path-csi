//! Kubernetes Events: what happened to a volume or a node, where `kubectl describe` shows it.

use k8s_openapi::api::core::v1::{ObjectReference, PersistentVolumeClaim, Pod};
use kube::api::{Api, ListParams};
pub use kube::runtime::events::EventType;
use kube::runtime::events::{Event, Recorder, Reporter};
use kube::{Resource, ResourceExt};

use crate::api::{LocalPathNode, LocalPathVolume};
use crate::store::pod_waits_for;

/// What there is to tell in an event: whether it asks for attention, the reason in a word and
/// the text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Notice {
    pub kind: EventType,
    pub reason: &'static str,
    pub note: String,
}

impl Notice {
    pub fn normal(reason: &'static str, note: impl Into<String>) -> Self {
        Self { kind: EventType::Normal, reason, note: note.into() }
    }

    pub fn warning(reason: &'static str, note: impl Into<String>) -> Self {
        Self { kind: EventType::Warning, reason, note: note.into() }
    }

    pub fn is_warning(&self) -> bool {
        self.kind == EventType::Warning
    }
}

pub struct Events {
    client: kube::Client,
    recorder: Recorder,
}

impl Events {
    /// `component` is `local-path-csi-controller` or `local-path-csi-agent`, `instance` tells replicas and
    /// nodes apart.
    pub fn new(client: kube::Client, component: &str, instance: Option<String>) -> Self {
        let recorder = Recorder::new(client.clone(), Reporter { controller: component.into(), instance });
        Self { client, recorder }
    }

    async fn publish(&self, object: &ObjectReference, notice: &Notice) {
        let event = Event {
            type_: notice.kind,
            reason: notice.reason.into(),
            note: Some(notice.note.chars().take(1000).collect()),
            action: notice.reason.into(),
            secondary: None,
        };
        // An event is a courtesy: failing to write one must not fail what it describes.
        if let Err(e) = self.recorder.publish(&event, object).await {
            tracing::debug!(error = %e, reason = notice.reason, "cannot publish the event");
        }
    }

    /// The claim of the volume as it is now. `kubectl describe` shows an event only when it names
    /// the object with its uid, so a claim that is gone gets none.
    async fn claim(&self, volume: &LocalPathVolume) -> Option<PersistentVolumeClaim> {
        let claim = volume.spec.claim.as_ref()?;
        let claims: Api<PersistentVolumeClaim> = Api::namespaced(self.client.clone(), &claim.namespace);
        claims.get_opt(&claim.name).await.ok().flatten()
    }

    /// An event of a volume: on its LocalPathVolume and, when the volume has a claim, on the claim.
    pub async fn volume(&self, volume: &LocalPathVolume, notice: &Notice) {
        self.publish(&volume.object_ref(&()), notice).await;
        if let Some(claim) = self.claim(volume).await {
            self.publish(&claim.object_ref(&()), notice).await;
        }
    }

    /// An event of a volume that also concerns the pods waiting for it on `node`: on the
    /// LocalPathVolume, on its claim and on those pods.
    pub async fn volume_and_pods(&self, volume: &LocalPathVolume, node: &str, notice: &Notice) {
        self.volume(volume, notice).await;
        let Some(claim) = &volume.spec.claim else { return };
        let pods: Api<Pod> = Api::namespaced(self.client.clone(), &claim.namespace);
        let on_node = ListParams::default().fields(&format!("spec.nodeName={node}"));
        let Ok(pods) = pods.list(&on_node).await else { return };
        for pod in pods.items.iter().filter(|pod| pod_waits_for(pod, &claim.name)) {
            tracing::debug!(pod = %pod.name_any(), reason = notice.reason, "event for the pod of the volume");
            self.publish(&pod.object_ref(&()), notice).await;
        }
    }

    /// An event of the storage of a node, on its LocalPathNode.
    pub async fn node(&self, node: &str, notice: &Notice) {
        let nodes: Api<LocalPathNode> = Api::all(self.client.clone());
        // Read for its uid; a LocalPathNode that cannot be read still gets the event by name.
        let object =
            nodes.get_opt(node).await.ok().flatten().unwrap_or_else(|| LocalPathNode::new(node, Default::default()));
        self.publish(&object.object_ref(&()), notice).await;
    }
}
