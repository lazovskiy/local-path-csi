//! The pod webhook: tells the scheduler what the driver knows about the volumes of a new pod.
//!
//! A volume that follows its pod is reachable from every storage node, so the scheduler sees no
//! difference between the node that holds the data and a node the data would have to be copied
//! to, nor does it know whether the volume fits there. The webhook adds both to the affinity of
//! the pod: a preference for the node with the data, and a ban on the nodes without room.
//!
//! It is an optimisation, never a gate: a pod it has not seen starts all the same, only its
//! volume may have to travel, and a volume that does not fit is refused when it is attached.

use std::collections::BTreeSet;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http::{Method, Request, Response, StatusCode, header};
use http_body_util::{BodyExt, Full, Limited};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::{TokioIo, TokioTimer};
use k8s_openapi::ByteString;
use k8s_openapi::api::admissionregistration::v1::MutatingWebhookConfiguration;
use k8s_openapi::api::core::v1::{
    Affinity, NodeAffinity, NodeSelector, NodeSelectorRequirement, NodeSelectorTerm, Pod, PreferredSchedulingTerm,
    Secret,
};
use kube::api::{Api, Patch, PatchParams, PostParams};
use serde_json::json;
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;

use crate::api::{LocalPathNode, LocalPathVolume, Mobility};
use crate::controller::free_in_pool;
use crate::store::{Cluster, Inventory, StoreError};
use crate::tls::Serving;

/// What the scheduler is asked for on behalf of one volume.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Advice {
    /// The node that holds the data: a pod placed there starts at once.
    pub prefer: String,
    /// Storage nodes the volume cannot move to: no room, pool not ready, agent not reporting.
    pub avoid: Vec<String>,
}

/// What to ask for `volume`; `None` for a volume the scheduler needs no help with.
pub fn advice(
    volume: &LocalPathVolume,
    nodes: &[LocalPathNode],
    volumes: &[LocalPathVolume],
    unix_now: u64,
    agent_timeout_seconds: u64,
) -> Option<Advice> {
    if volume.spec.mobility != Mobility::Floating || volume.metadata.deletion_timestamp.is_some() {
        return None;
    }
    let holder = volume.data_node();
    // The volume itself must not count against the node it may already be on its way to.
    let others: Vec<LocalPathVolume> =
        volumes.iter().filter(|v| v.metadata.name != volume.metadata.name).cloned().collect();
    let fits = |node: &LocalPathNode, name: &str| {
        let free = free_in_pool(Some(node), &others, name, &volume.spec.pool, unix_now, agent_timeout_seconds);
        free.is_ok_and(|free| free >= volume.spec.size_bytes)
    };
    let avoid: BTreeSet<String> = nodes
        .iter()
        .filter_map(|node| Some((node, node.metadata.name.as_deref()?)))
        .filter(|(node, name)| *name != holder && !fits(node, name))
        .map(|(_, name)| name.to_string())
        .collect();
    Some(Advice { prefer: holder.to_string(), avoid: avoid.into_iter().collect() })
}

/// A requirement on the name of a node. The API takes one value per requirement of a field.
fn node_name(operator: &str, node: &str) -> NodeSelectorRequirement {
    NodeSelectorRequirement {
        key: "metadata.name".into(),
        operator: operator.into(),
        values: Some(vec![node.to_string()]),
    }
}

/// Adds `advice` to the affinity of a pod; `None` when there is nothing to add.
pub fn with_advice(affinity: Option<&Affinity>, advice: &[Advice]) -> Option<Affinity> {
    let original = affinity.cloned().unwrap_or_default();
    let mut result = original.clone();
    let node_affinity = result.node_affinity.get_or_insert_with(NodeAffinity::default);

    let preferred = node_affinity.preferred_during_scheduling_ignored_during_execution.get_or_insert_with(Vec::new);
    for node in advice.iter().map(|a| &a.prefer).collect::<BTreeSet<_>>() {
        let term = PreferredSchedulingTerm {
            weight: 100,
            preference: NodeSelectorTerm { match_fields: Some(vec![node_name("In", node)]), match_expressions: None },
        };
        if !preferred.contains(&term) {
            preferred.push(term);
        }
    }
    if preferred.is_empty() {
        node_affinity.preferred_during_scheduling_ignored_during_execution = None;
    }

    let avoid: BTreeSet<&String> = advice.iter().flat_map(|a| a.avoid.iter()).collect();
    if !avoid.is_empty() {
        let required = node_affinity
            .required_during_scheduling_ignored_during_execution
            .get_or_insert_with(|| NodeSelector { node_selector_terms: Vec::new() });
        // The terms are alternatives: each of them has to keep the pod off the nodes.
        if required.node_selector_terms.is_empty() {
            required.node_selector_terms.push(NodeSelectorTerm::default());
        }
        for term in &mut required.node_selector_terms {
            let fields = term.match_fields.get_or_insert_with(Vec::new);
            for node in &avoid {
                let requirement = node_name("NotIn", node);
                if !fields.contains(&requirement) {
                    fields.push(requirement);
                }
            }
        }
    }
    if node_affinity == &NodeAffinity::default() {
        result.node_affinity = None;
    }
    (result != original).then_some(result)
}

/// Claims a pod mounts: the ones that exist already. A claim made for the pod from a template
/// gets a new volume, and where that fits the scheduler knows from the capacity objects.
fn claims(pod: &Pod) -> Vec<&str> {
    let volumes = pod.spec.iter().flat_map(|spec| spec.volumes.iter().flatten());
    volumes.filter_map(|v| Some(v.persistent_volume_claim.as_ref()?.claim_name.as_str())).collect()
}

/// The affinity to give a pod created in `namespace`, or `None` to leave it as it is.
pub async fn affinity_for<S: Inventory + Cluster>(
    store: &S,
    namespace: &str,
    pod: &Pod,
    agent_timeout_seconds: u64,
) -> Result<Option<Affinity>, StoreError> {
    let spec = pod.spec.as_ref();
    // A pod that names its node is not placed by the scheduler.
    if spec.is_none_or(|spec| spec.node_name.as_ref().is_some_and(|node| !node.is_empty())) {
        return Ok(None);
    }
    let mut floating = Vec::new();
    for claim in claims(pod) {
        let Some(name) = store.claim_volume(namespace, claim).await? else { continue };
        if let Some(volume) = store.volume(&name).await?.filter(|v| v.spec.mobility == Mobility::Floating) {
            floating.push(volume);
        }
    }
    if floating.is_empty() {
        return Ok(None);
    }
    let (nodes, volumes) = (store.nodes().await?, store.volumes().await?);
    let now = crate::unix_now();
    let advice: Vec<Advice> =
        floating.iter().filter_map(|v| advice(v, &nodes, &volumes, now, agent_timeout_seconds)).collect();
    Ok(with_advice(spec.and_then(|spec| spec.affinity.as_ref()), &advice))
}

/// The part of an `admission.k8s.io/v1` AdmissionReview the webhook reads.
#[derive(serde::Deserialize)]
struct Review {
    request: Asked,
}

#[derive(serde::Deserialize)]
struct Asked {
    uid: String,
    #[serde(default)]
    namespace: String,
    #[serde(default)]
    name: String,
    object: Option<Pod>,
}

/// Why a request to the webhook got no review for an answer.
#[derive(Debug, thiserror::Error)]
pub enum ReviewError {
    #[error("not an admission review: {0}")]
    NotAReview(#[source] serde_json::Error),
    #[error(transparent)]
    Answer(#[from] serde_json::Error),
}

/// Answers an admission review of a pod. Whatever goes wrong, the pod is let through unchanged.
pub async fn review<S: Inventory + Cluster>(
    store: &S,
    body: &[u8],
    agent_timeout_seconds: u64,
) -> Result<Vec<u8>, ReviewError> {
    let Review { request } = serde_json::from_slice(body).map_err(ReviewError::NotAReview)?;
    let (namespace, name) = (&request.namespace, &request.name);
    let mut response = json!({"uid": request.uid, "allowed": true});
    if let Some(pod) = &request.object {
        match affinity_for(store, namespace, pod, agent_timeout_seconds).await {
            Ok(None) => {}
            Ok(Some(affinity)) => {
                // `add` replaces a member that exists.
                let patch = json!([{"op": "add", "path": "/spec/affinity", "value": affinity}]);
                let patch = serde_json::to_vec(&patch)?;
                response["patchType"] = json!("JSONPatch");
                // Bytes travel base64-encoded.
                response["patch"] = json!(ByteString(patch));
                tracing::info!(%namespace, pod = %name, "the pod is steered to the data of its volumes");
            }
            Err(e) => tracing::warn!(%namespace, pod = %name, error = %e, "the pod is let through as it is"),
        }
    }
    let review = json!({"apiVersion": "admission.k8s.io/v1", "kind": "AdmissionReview", "response": response});
    Ok(serde_json::to_vec(&review)?)
}

/// Admission reviews are small; a pod with everything in it stays far below this.
const BODY_LIMIT: usize = 8 << 20;

async fn respond<S: Inventory + Cluster>(
    store: &S,
    request: Request<Incoming>,
    agent_timeout_seconds: u64,
) -> Response<Full<Bytes>> {
    let plain = |status: StatusCode, text: String| {
        let mut response = Response::new(Full::new(Bytes::from(text)));
        *response.status_mut() = status;
        response
    };
    if request.method() != Method::POST || request.uri().path() != "/mutate" {
        return plain(StatusCode::NOT_FOUND, "404 page not found\n".into());
    }
    let body = match Limited::new(request.into_body(), BODY_LIMIT).collect().await {
        Ok(body) => body.to_bytes(),
        Err(e) => return plain(StatusCode::BAD_REQUEST, format!("{e}\n")),
    };
    match review(store, &body, agent_timeout_seconds).await {
        Ok(answer) => {
            let mut response = Response::new(Full::new(Bytes::from(answer)));
            response.headers_mut().insert(header::CONTENT_TYPE, header::HeaderValue::from_static("application/json"));
            response
        }
        Err(e) => plain(StatusCode::BAD_REQUEST, format!("{e}\n")),
    }
}

/// Serves the webhook over TLS until the process is stopped.
pub async fn serve<S: Inventory + Cluster>(
    address: SocketAddr,
    tls: Arc<rustls::ServerConfig>,
    store: Arc<S>,
    agent_timeout_seconds: u64,
) -> std::io::Result<()> {
    let listener = TcpListener::bind(address).await?;
    let acceptor = TlsAcceptor::from(tls);
    tracing::info!(%address, "serving the pod webhook");
    loop {
        let (stream, _) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(e) => {
                tracing::warn!(error = %e, "accept failed");
                continue;
            }
        };
        let (acceptor, store) = (acceptor.clone(), store.clone());
        tokio::spawn(async move {
            let Ok(Ok(stream)) = tokio::time::timeout(Duration::from_secs(10), acceptor.accept(stream)).await else {
                return;
            };
            let service = service_fn(move |request: Request<Incoming>| {
                let store = store.clone();
                async move { Ok::<_, Infallible>(respond(store.as_ref(), request, agent_timeout_seconds).await) }
            });
            let connection = http1::Builder::new()
                .timer(TokioTimer::new())
                .header_read_timeout(Duration::from_secs(30))
                .serve_connection(TokioIo::new(stream), service);
            let _ = connection.await;
        });
    }
}

/// Where the certificate of the webhook lives and who has to trust it.
pub struct Registration {
    pub namespace: String,
    /// Secret that keeps the certificate: all replicas of the controller serve the same one.
    pub secret: String,
    /// Service the API server calls the webhook through.
    pub service: String,
    /// MutatingWebhookConfiguration that is told the authority of the certificate.
    pub configuration: String,
}

const KEY_AUTHORITY: &str = "ca.crt";
const KEY_CERTIFICATE: &str = "tls.crt";
const KEY_KEY: &str = "tls.key";

type Error = Box<dyn std::error::Error + Send + Sync>;

/// The certificate of the webhook: the one in the Secret, made and stored when there is none.
pub async fn certificate(client: &kube::Client, registration: &Registration) -> Result<Serving, Error> {
    let secrets: Api<Secret> = Api::namespaced(client.clone(), &registration.namespace);
    let stored = |secret: Secret| -> Option<Serving> {
        let data = secret.data?;
        let text = |key: &str| String::from_utf8(data.get(key)?.0.clone()).ok();
        Some(Serving { authority: text(KEY_AUTHORITY)?, certificate: text(KEY_CERTIFICATE)?, key: text(KEY_KEY)? })
    };
    // Two replicas may start together: the one that loses the race to create reads again.
    for _ in 0..5 {
        if let Some(serving) = secrets.get_opt(&registration.secret).await?.and_then(stored) {
            return Ok(serving);
        }
        let service = format!("{}.{}.svc", registration.service, registration.namespace);
        let serving = Serving::generate(&[service.clone(), format!("{service}.cluster.local")])?;
        let secret: Secret = serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": registration.secret, "namespace": registration.namespace},
            "type": "kubernetes.io/tls",
            "stringData": {KEY_AUTHORITY: serving.authority, KEY_CERTIFICATE: serving.certificate, KEY_KEY: serving.key},
        }))?;
        match secrets.create(&PostParams::default(), &secret).await {
            Ok(_) => {
                tracing::info!(secret = %registration.secret, "made a certificate for the webhook");
                return Ok(serving);
            }
            Err(kube::Error::Api(response)) if response.code == 409 => {
                // Exists by now, or exists without the keys: the latter is for a person to fix.
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Err(e) => return Err(e.into()),
        }
    }
    Err(format!("Secret {} exists and holds no certificate: delete it", registration.secret).into())
}

/// Keeps the webhook configuration trusting `authority`, until the process is stopped: an
/// upgrade of the chart may reset what was written there.
pub async fn keep_trusted(client: kube::Client, registration: Registration, authority: String) {
    let configurations: Api<MutatingWebhookConfiguration> = Api::all(client);
    let bundle = ByteString(authority.into_bytes());
    loop {
        let trust = async {
            let Some(current) = configurations.get_opt(&registration.configuration).await? else {
                return Err(
                    format!("MutatingWebhookConfiguration {} does not exist", registration.configuration).into()
                );
            };
            let webhooks = current.webhooks.unwrap_or_default();
            let stale: Vec<usize> = (0..webhooks.len())
                .filter(|i| webhooks[*i].client_config.ca_bundle.as_ref() != Some(&bundle))
                .collect();
            if stale.is_empty() {
                return Ok(());
            }
            let operations: Vec<_> = stale
                .iter()
                .map(|i| json!({"op": "add", "path": format!("/webhooks/{i}/clientConfig/caBundle"), "value": bundle}))
                .collect();
            let patch: json_patch::Patch = serde_json::from_value(json!(operations))?;
            let patch = Patch::Json::<()>(patch);
            configurations.patch(&registration.configuration, &PatchParams::default(), &patch).await?;
            tracing::info!(configuration = %registration.configuration, "the API server is told the certificate of the webhook");
            Ok::<(), Error>(())
        };
        if let Err(e) = trust.await {
            tracing::warn!(error = %e, "cannot register the certificate of the webhook");
        }
        tokio::time::sleep(Duration::from_secs(60)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::{LocalPathVolumeSpec, LocalPathVolumeStatus};
    use crate::store::memory::MemoryStore;

    const GIB: u64 = 1 << 30;

    fn volume(name: &str, mobility: Mobility, node: &str, size: u64) -> LocalPathVolume {
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
            claim: None,
        };
        let mut volume = LocalPathVolume::new(name, spec);
        volume.status = Some(LocalPathVolumeStatus { node: Some(node.into()), ..Default::default() });
        volume
    }

    /// Nodes a, b and c with 100 GiB pools; `volumes` are in the store, claims named like them.
    fn cluster(volumes: &[LocalPathVolume]) -> MemoryStore {
        let store = MemoryStore::default();
        for node in ["a", "b", "c"] {
            store.add_pool(node, "default", 100 * GIB);
        }
        for volume in volumes {
            let name = volume.metadata.name.clone().unwrap();
            store.bound.lock().unwrap().insert(("apps".into(), name.clone()), name.clone());
            store.volumes.lock().unwrap().insert(name, volume.clone());
        }
        store
    }

    fn pod(claims: &[&str]) -> Pod {
        let volumes: Vec<_> =
            claims.iter().map(|c| json!({"name": c, "persistentVolumeClaim": {"claimName": c}})).collect();
        serde_json::from_value(json!({"metadata": {"name": "app"}, "spec": {"containers": [], "volumes": volumes}}))
            .unwrap()
    }

    fn as_json(affinity: &Affinity) -> serde_json::Value {
        serde_json::to_value(affinity).unwrap()
    }

    fn prefer(node: &str) -> serde_json::Value {
        json!({"weight": 100, "preference": {"matchFields": [{"key": "metadata.name", "operator": "In", "values": [node]}]}})
    }

    fn not(node: &str) -> serde_json::Value {
        json!({"key": "metadata.name", "operator": "NotIn", "values": [node]})
    }

    #[tokio::test]
    async fn pod_is_drawn_to_the_node_that_holds_its_floating_volume() {
        let store = cluster(&[volume("data", Mobility::Floating, "b", GIB)]);
        let affinity = affinity_for(&store, "apps", &pod(&["data"]), 120).await.unwrap().unwrap();
        assert_eq!(
            as_json(&affinity),
            json!({"nodeAffinity": {"preferredDuringSchedulingIgnoredDuringExecution": [prefer("b")]}})
        );
    }

    #[tokio::test]
    async fn pod_is_kept_off_the_nodes_its_volume_cannot_move_to() {
        let store = cluster(&[
            volume("data", Mobility::Floating, "a", 60 * GIB),
            // Node b is nearly full; node c has room but its agent is silent.
            volume("other", Mobility::Pinned, "b", 70 * GIB),
        ]);
        store.age_report("c", Some(600));
        let affinity = affinity_for(&store, "apps", &pod(&["data"]), 120).await.unwrap().unwrap();
        assert_eq!(
            as_json(&affinity),
            json!({"nodeAffinity": {
                "preferredDuringSchedulingIgnoredDuringExecution": [prefer("a")],
                "requiredDuringSchedulingIgnoredDuringExecution": {"nodeSelectorTerms": [{"matchFields": [not("b"), not("c")]}]},
            }})
        );
    }

    #[tokio::test]
    async fn node_that_holds_the_data_is_never_avoided() {
        // The pool of node a is overbooked (it shrank): the volume is there all the same.
        let store = cluster(&[
            volume("data", Mobility::Floating, "a", 80 * GIB),
            volume("other", Mobility::Pinned, "a", 80 * GIB),
        ]);
        let affinity = affinity_for(&store, "apps", &pod(&["data"]), 120).await.unwrap().unwrap();
        assert_eq!(
            as_json(&affinity),
            json!({"nodeAffinity": {"preferredDuringSchedulingIgnoredDuringExecution": [prefer("a")]}})
        );
    }

    #[tokio::test]
    async fn volume_on_its_way_to_a_node_fits_there() {
        // 60 of the 100 GiB of node b are spoken for by this very volume.
        let mut moving = volume("data", Mobility::Floating, "a", 60 * GIB);
        moving.spec.node = "b".into();
        let store = cluster(&[moving]);
        let affinity = affinity_for(&store, "apps", &pod(&["data"]), 120).await.unwrap().unwrap();
        assert_eq!(
            as_json(&affinity),
            json!({"nodeAffinity": {"preferredDuringSchedulingIgnoredDuringExecution": [prefer("a")]}})
        );
    }

    #[tokio::test]
    async fn pods_the_webhook_has_nothing_to_say_about_are_left_alone() {
        let store =
            cluster(&[volume("pinned", Mobility::Pinned, "a", GIB), volume("data", Mobility::Floating, "a", GIB)]);
        // No volumes, a pinned volume, a claim that is not bound, a claim of another driver.
        for claims in [&[][..], &["pinned"], &["unbound"]] {
            assert_eq!(affinity_for(&store, "apps", &pod(claims), 120).await.unwrap(), None, "{claims:?}");
        }
        store.bound.lock().unwrap().insert(("apps".into(), "foreign".into()), "pv-of-another-driver".into());
        assert_eq!(affinity_for(&store, "apps", &pod(&["foreign"]), 120).await.unwrap(), None);
        // The same claim name in another namespace is another claim.
        assert_eq!(affinity_for(&store, "other", &pod(&["data"]), 120).await.unwrap(), None);
        // A pod that names its node is not scheduled at all.
        let mut placed = pod(&["data"]);
        placed.spec.as_mut().unwrap().node_name = Some("c".into());
        assert_eq!(affinity_for(&store, "apps", &placed, 120).await.unwrap(), None);
    }

    #[test]
    fn affinity_of_the_pod_is_extended_not_replaced() {
        let own: Affinity = serde_json::from_value(json!({
            "podAntiAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": [{"topologyKey": "kubernetes.io/hostname"}]},
            "nodeAffinity": {
                "preferredDuringSchedulingIgnoredDuringExecution": [
                    {"weight": 10, "preference": {"matchExpressions": [{"key": "disk", "operator": "In", "values": ["ssd"]}]}},
                ],
                "requiredDuringSchedulingIgnoredDuringExecution": {"nodeSelectorTerms": [
                    {"matchExpressions": [{"key": "zone", "operator": "In", "values": ["x"]}]},
                    {"matchFields": [{"key": "metadata.name", "operator": "In", "values": ["c"]}]},
                ]},
            },
        }))
        .unwrap();
        let advice = [
            Advice { prefer: "a".into(), avoid: vec!["b".into()] },
            Advice { prefer: "a".into(), avoid: vec!["b".into(), "d".into()] },
        ];
        let result = with_advice(Some(&own), &advice).unwrap();
        assert_eq!(
            as_json(&result),
            json!({
                "podAntiAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": [{"topologyKey": "kubernetes.io/hostname"}]},
                "nodeAffinity": {
                    "preferredDuringSchedulingIgnoredDuringExecution": [
                        {"weight": 10, "preference": {"matchExpressions": [{"key": "disk", "operator": "In", "values": ["ssd"]}]}},
                        prefer("a"),
                    ],
                    // The terms are alternatives: the ban goes into each of them.
                    "requiredDuringSchedulingIgnoredDuringExecution": {"nodeSelectorTerms": [
                        {"matchExpressions": [{"key": "zone", "operator": "In", "values": ["x"]}], "matchFields": [not("b"), not("d")]},
                        {"matchFields": [{"key": "metadata.name", "operator": "In", "values": ["c"]}, not("b"), not("d")]},
                    ]},
                },
            })
        );
        // Asked again about its own answer, the webhook adds nothing.
        assert_eq!(with_advice(Some(&result), &advice), None);
        assert_eq!(with_advice(Some(&own), &[]), None);
        assert_eq!(with_advice(None, &[]), None);
    }

    #[tokio::test]
    async fn review_answers_with_a_patch_of_the_affinity_and_lets_everything_else_through() {
        let store = cluster(&[volume("data", Mobility::Floating, "b", GIB)]);
        let ask = |pod: &Pod| {
            serde_json::to_vec(&json!({
                "apiVersion": "admission.k8s.io/v1",
                "kind": "AdmissionReview",
                "request": {
                    "uid": "705ab4f5-6393-11e8-b7cc-42010a800002",
                    "kind": {"group": "", "version": "v1", "kind": "Pod"},
                    "resource": {"group": "", "version": "v1", "resource": "pods"},
                    "name": "app",
                    "namespace": "apps",
                    "operation": "CREATE",
                    "userInfo": {},
                    "object": pod,
                },
            }))
            .unwrap()
        };
        let answer = |body: Vec<u8>| -> serde_json::Value { serde_json::from_slice(&body).unwrap() };

        let steered = answer(review(&store, &ask(&pod(&["data"])), 120).await.unwrap());
        let response = &steered["response"];
        assert_eq!(response["uid"], "705ab4f5-6393-11e8-b7cc-42010a800002");
        assert_eq!(response["allowed"], true);
        assert_eq!(response["patchType"], "JSONPatch");
        let patch: ByteString = serde_json::from_value(response["patch"].clone()).unwrap();
        let patch: serde_json::Value = serde_json::from_slice(&patch.0).unwrap();
        let expected = json!({"nodeAffinity": {"preferredDuringSchedulingIgnoredDuringExecution": [prefer("b")]}});
        assert_eq!(patch, json!([{"op": "add", "path": "/spec/affinity", "value": expected}]));

        let untouched = answer(review(&store, &ask(&pod(&[])), 120).await.unwrap());
        assert_eq!(untouched["response"]["allowed"], true);
        assert!(untouched["response"].get("patch").is_none(), "{untouched}");
        assert!(review(&store, b"not a review", 120).await.is_err());
    }

    #[test]
    fn webhook_certificate_is_issued_for_the_service_by_an_authority_of_its_own() {
        let names = ["local-path-csi-webhook.local-path-csi.svc".to_string()];
        let serving = Serving::generate(&names).unwrap();
        assert!(serving.server_config().is_ok());
        assert_ne!(serving.authority, serving.certificate);
        for pem in [&serving.authority, &serving.certificate] {
            assert!(pem.starts_with("-----BEGIN CERTIFICATE-----"), "{pem}");
        }
        assert!(serving.key.contains("PRIVATE KEY"));
    }
}
