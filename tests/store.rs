//! The store of the controller against a mock API server: what it reads comes from the caches of
//! its watches, and what it writes is in them when the write returns.
//!
//! The caches are filled by the test, as the watches would fill them. The server answers the
//! writes and the few reads that still go to it, and remembers every request.

use std::convert::Infallible;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use http::{Method, Request, Response, StatusCode};
use http_body_util::{BodyExt as _, Full};
use hyper::body::{Bytes, Incoming};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use k8s_openapi::api::storage::v1::VolumeAttachment;
use kube::runtime::watcher::Event;
use local_path_csi::api::{LocalPathNode, LocalPathVolume, timestamp};
use local_path_csi::store::{Cluster, Feeds, Inventory, KubeStore, StoreError, Volumes, caches};
use serde_json::{Value, json};
use tokio::net::TcpListener;

const VOLUMES: &str = "/apis/local-path-csi.domik.fi/v1alpha1/localpathvolumes";
const ATTACHMENTS: &str = "/apis/storage.k8s.io/v1/volumeattachments";
const DRIVER: &str = "local-path.csi.domik.fi";

#[derive(Default)]
struct Server {
    /// `(method, path)` of every request.
    log: Mutex<Vec<(Method, String)>>,
    /// Every write with a condition is answered 409 while set.
    conflict: AtomicBool,
    /// Nothing may be created while set: 403.
    forbidden: AtomicBool,
    /// What a list of VolumeAttachments answers.
    attachments: Mutex<Vec<Value>>,
    /// What the last patch of a volume said.
    patch: Mutex<Value>,
}

fn failure(code: u16, reason: &str) -> (StatusCode, Value) {
    let status = json!({"kind": "Status", "apiVersion": "v1", "metadata": {}, "status": "Failure", "reason": reason, "code": code});
    (StatusCode::from_u16(code).unwrap(), status)
}

fn volume_json(name: &str, version: &str, size: u64) -> Value {
    json!({
        "apiVersion": "local-path-csi.domik.fi/v1alpha1", "kind": "LocalPathVolume",
        "metadata": {"name": name, "resourceVersion": version},
        "spec": {"pool": "default", "sizeBytes": size, "node": "a", "dirMode": "0770"},
    })
}

fn volume(name: &str, version: &str, size: u64) -> LocalPathVolume {
    serde_json::from_value(volume_json(name, version, size)).unwrap()
}

fn attachment(volume: &str, node: &str) -> Value {
    json!({
        "apiVersion": "storage.k8s.io/v1", "kind": "VolumeAttachment",
        "metadata": {"name": format!("csi-{volume}-{node}"), "resourceVersion": "3"},
        "spec": {"attacher": DRIVER, "nodeName": node, "source": {"persistentVolumeName": volume}},
    })
}

impl Server {
    fn answer(&self, method: &Method, path: &str, body: &[u8]) -> (StatusCode, Value) {
        match (method, path) {
            // A created volume gets version 7.
            (&Method::POST, VOLUMES) if self.forbidden.load(Ordering::SeqCst) => failure(403, "Forbidden"),
            (&Method::POST, VOLUMES) => {
                let mut created: Value = serde_json::from_slice(body).unwrap();
                created["metadata"]["resourceVersion"] = json!("7");
                (StatusCode::CREATED, created)
            }
            // A patched one is pv-1 of size 2 at version 9.
            (&Method::PATCH, path) if path.starts_with(VOLUMES) => {
                *self.patch.lock().unwrap() = serde_json::from_slice(body).unwrap();
                if self.conflict.load(Ordering::SeqCst) {
                    return failure(409, "Conflict");
                }
                (StatusCode::OK, volume_json("pv-1", "9", 2))
            }
            (&Method::GET, ATTACHMENTS) => {
                let items = self.attachments.lock().unwrap().clone();
                let list = json!({"apiVersion": "storage.k8s.io/v1", "kind": "VolumeAttachmentList", "metadata": {"resourceVersion": "3"}, "items": items});
                (StatusCode::OK, list)
            }
            // A PersistentVolume that names no claim: nothing more is asked about its pods.
            (&Method::GET, "/api/v1/persistentvolumes/pv-1") => {
                let volume =
                    json!({"apiVersion": "v1", "kind": "PersistentVolume", "metadata": {"name": "pv-1"}, "spec": {}});
                (StatusCode::OK, volume)
            }
            _ => failure(404, "NotFound"),
        }
    }

    fn requests(&self) -> Vec<(Method, String)> {
        self.log.lock().unwrap().clone()
    }
}

async fn serve(server: Arc<Server>) -> kube::Client {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else { return };
            let server = server.clone();
            tokio::spawn(async move {
                let service = service_fn(move |request: Request<Incoming>| {
                    let server = server.clone();
                    async move {
                        let (parts, body) = request.into_parts();
                        let body = body.collect().await.unwrap().to_bytes();
                        let path = parts.uri.path().to_string();
                        let (status, value) = server.answer(&parts.method, &path, &body);
                        server.log.lock().unwrap().push((parts.method, path));
                        let mut response = Response::new(Full::new(Bytes::from(value.to_string())));
                        *response.status_mut() = status;
                        response.headers_mut().insert("content-type", "application/json".parse().unwrap());
                        Ok::<_, Infallible>(response)
                    }
                });
                let _ = http1::Builder::new().serve_connection(TokioIo::new(stream), service).await;
            });
        }
    });
    kube::Client::try_from(kube::Config::new(format!("http://{address}").parse().unwrap())).unwrap()
}

/// A store on a server of its own, and what fills its caches.
async fn store() -> (KubeStore, Feeds, Arc<Server>) {
    let server = Arc::new(Server::default());
    let (caches, feeds) = caches();
    (KubeStore::new(serve(server.clone()).await, caches), feeds, server)
}

/// How long the watch of a test takes to show what was written.
const WATCH_LAG: Duration = Duration::from_millis(300);

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn volumes_and_nodes_are_read_without_a_request() {
    let (store, mut feeds, server) = store().await;
    feeds.volumes.apply_watcher_event(&Event::Apply(volume("pv-1", "4", 1)));
    let node: LocalPathNode =
        serde_json::from_value(json!({"metadata": {"name": "a", "resourceVersion": "2"}, "spec": {}})).unwrap();
    feeds.nodes.apply_watcher_event(&Event::Apply(node));

    assert_eq!(store.volume("pv-1").await.unwrap().unwrap().spec.size_bytes, 1);
    assert!(store.volume("pv-2").await.unwrap().is_none());
    assert_eq!(store.volumes().await.unwrap().len(), 1);
    assert_eq!(store.node("a").await.unwrap().unwrap().metadata.name.as_deref(), Some("a"));
    assert!(store.node("b").await.unwrap().is_none());
    assert_eq!(store.nodes().await.unwrap().len(), 1);
    assert_eq!(server.requests(), vec![], "a read is a look into the cache");
}

/// The room of a pool is counted from the volumes the store lists: a volume a call has just
/// created must be among them for the next call, or both take the same room.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn volume_is_listed_as_soon_as_its_creation_returns() {
    let (store, mut feeds, _) = store().await;
    let watch = tokio::spawn(async move {
        tokio::time::sleep(WATCH_LAG).await;
        feeds.volumes.apply_watcher_event(&Event::Apply(volume("pv-1", "7", 1)));
        feeds
    });
    store.create_volume("pv-1", volume("pv-1", "", 1).spec).await.unwrap();
    let listed = store.volumes().await.unwrap();
    assert_eq!(listed.iter().filter_map(|v| v.metadata.name.as_deref()).collect::<Vec<_>>(), vec!["pv-1"]);
    watch.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_is_read_back_as_soon_as_it_returns() {
    let (store, mut feeds, _) = store().await;
    let read = volume("pv-1", "4", 1);
    feeds.volumes.apply_watcher_event(&Event::Apply(read.clone()));
    let watch = tokio::spawn(async move {
        tokio::time::sleep(WATCH_LAG).await;
        feeds.volumes.apply_watcher_event(&Event::Apply(volume("pv-1", "9", 2)));
        feeds
    });
    assert!(store.set_size(&read, 2).await.unwrap());
    assert_eq!(store.volume("pv-1").await.unwrap().unwrap().spec.size_bytes, 2);
    watch.await.unwrap();
}

/// A write the watch never shows went through all the same: the call fails, and what repeats
/// it finds the volume written.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_the_watch_does_not_show_fails_instead_of_waiting_for_ever() {
    let (store, mut feeds, _) = store().await;
    let store = store.with_patience(Duration::from_millis(200), Duration::from_millis(200));
    let read = volume("pv-1", "4", 1);
    feeds.volumes.apply_watcher_event(&Event::Apply(read.clone()));
    let refusal = store.set_size(&read, 2).await.unwrap_err();
    assert!(refusal.to_string().contains("pv-1") && refusal.to_string().contains("watch"), "{refusal}");
}

/// A call that lost to another writer reads the volume again and decides anew: what it reads
/// then must be the newer volume, not the one it has just lost with.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_that_lost_to_another_is_followed_by_a_read_of_the_newer_volume() {
    let (store, mut feeds, server) = store().await;
    server.conflict.store(true, Ordering::SeqCst);
    let read = volume("pv-1", "4", 1);
    feeds.volumes.apply_watcher_event(&Event::Apply(read.clone()));
    let watch = tokio::spawn(async move {
        tokio::time::sleep(WATCH_LAG).await;
        feeds.volumes.apply_watcher_event(&Event::Apply(volume("pv-1", "5", 3)));
        feeds
    });
    assert!(!store.set_size(&read, 2).await.unwrap(), "the other writer was first");
    let again = store.volume("pv-1").await.unwrap().unwrap();
    assert_eq!((again.metadata.resource_version.as_deref(), again.spec.size_bytes), (Some("5"), 3));
    watch.await.unwrap();
}

fn listed_attachments(server: &Server) -> usize {
    server.requests().iter().filter(|(method, path)| *method == Method::GET && path == ATTACHMENTS).count()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn attachment_in_the_cache_is_not_asked_for() {
    let (store, mut feeds, server) = store().await;
    let cached: VolumeAttachment = serde_json::from_value(attachment("pv-1", "b")).unwrap();
    feeds.attachments.apply_watcher_event(&Event::Apply(cached));
    assert!(store.attachment_wanted("pv-1", "b").await.unwrap());
    assert_eq!(listed_attachments(&server), 0);
}

/// The attacher calls for an attachment it has seen, and the watch of the controller may be
/// behind it: that the cache has no attachment is no reason to cancel a move.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn attachment_the_cache_has_not_seen_yet_is_asked_for_before_it_is_taken_for_gone() {
    let (store, _feeds, server) = store().await;
    server.attachments.lock().unwrap().push(attachment("pv-1", "b"));
    assert!(store.attachment_wanted("pv-1", "b").await.unwrap(), "the API server has it");
    assert_eq!(listed_attachments(&server), 1);

    assert!(!store.attachment_wanted("pv-1", "c").await.unwrap(), "nobody has one for node c");
    server.attachments.lock().unwrap().clear();
    assert!(!store.attachment_wanted("pv-1", "b").await.unwrap(), "and now nobody has one for node b");
}

fn node_reported_at(unix_seconds: u64) -> LocalPathNode {
    let node = json!({"metadata": {"name": "a", "resourceVersion": "2"}, "spec": {}, "status": {"reportedAt": timestamp(unix_seconds)}});
    serde_json::from_value(node).unwrap()
}

/// Everything that asks how old a report is reads `status.reportedAt` of the node the store
/// gives it. That is the time this process saw the report change, not the time the agent wrote
/// by the clock of its node.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_is_read_with_the_time_its_report_was_seen_to_change() {
    let (store, mut feeds, _) = store().await;
    let age =
        |node: Option<LocalPathNode>| node.unwrap().status.unwrap().report_age(local_path_csi::unix_now()).unwrap();
    // The clock of the node is ten minutes behind.
    let now = local_path_csi::unix_now();
    let first = node_reported_at(now - 600);
    feeds.sightings.observe(&first, Instant::now(), now);
    feeds.nodes.apply_watcher_event(&Event::Apply(first));
    let seen_first = age(store.node("a").await.unwrap());
    assert!(
        (600..=602).contains(&seen_first),
        "seen for the first time, the written time is all there is: {seen_first}"
    );

    let next = node_reported_at(now - 570);
    feeds.sightings.observe(&next, Instant::now(), now);
    feeds.nodes.apply_watcher_event(&Event::Apply(next));
    let seen_next = age(store.node("a").await.unwrap());
    assert!(seen_next <= 2, "the agent has just reported: {seen_next}");
    let listed = age(store.nodes().await.unwrap().pop());
    assert!(listed <= 2, "{listed}");
}

/// What the agent of the former node said of a volume does not describe the copy on the new one:
/// the handover takes it back, the condition with the phase.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn handover_takes_back_the_report_of_the_former_node() {
    let (store, mut feeds, server) = store().await;
    let read = volume("pv-1", "4", 1);
    feeds.volumes.apply_watcher_event(&Event::Apply(read.clone()));
    feeds.volumes.apply_watcher_event(&Event::Apply(volume("pv-1", "9", 2)));
    assert!(store.commit_move(&read, "b").await.unwrap());
    let status = server.patch.lock().unwrap()["status"].clone();
    assert_eq!((&status["node"], &status["phase"]), (&json!("b"), &json!("Pending")));
    for taken_back in ["conditions", "reportedBy", "capacityBytes", "usedBytes", "message", "transfer"] {
        let field = status.as_object().unwrap().get(taken_back);
        assert_eq!(field, Some(&Value::Null), "{taken_back}");
    }
}

/// What the API server refused with is what decides the answer to the CSI call: it must not be
/// lost on the way.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refusal_of_the_api_server_keeps_its_code() {
    let (store, _feeds, server) = store().await;
    server.forbidden.store(true, Ordering::SeqCst);
    let spec = volume("pv-1", "1", 1).spec;
    let refusal = store.create_volume("pv-1", spec).await.unwrap_err();
    assert!(matches!(&refusal, StoreError::Refused { code: 403, reason, .. } if reason == "Forbidden"), "{refusal:?}");
    assert!(store.volume("pv-1").await.unwrap().is_none());

    // A server that cannot be reached is no refusal.
    let nobody = kube::Client::try_from(kube::Config::new("http://127.0.0.1:1".parse().unwrap())).unwrap();
    let unreachable = KubeStore::new(nobody, caches().0);
    let failure = unreachable.create_volume("pv-1", volume("pv-1", "1", 1).spec).await.unwrap_err();
    assert!(matches!(failure, StoreError::Client(_)), "{failure:?}");
}
