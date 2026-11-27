//! Leader election against a mock API server that serves one Lease: a Lease that is not there,
//! one another holder keeps, one that has expired, leadership that is lost and won back, a lost
//! race for the Lease, and a leader that stops.
//!
//! What is expected follows the `leaderelection` package of client-go; the timings are short
//! copies of its defaults (lease duration 15 s, renew deadline 10 s, retry period 2 s).

use std::convert::Infallible;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use http::{Method, Request, Response, StatusCode};
use http_body_util::{BodyExt as _, Full};
use hyper::body::{Bytes, Incoming};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use local_path_csi::leader::{LeaseRef, Timings, run};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::watch;

/// The collection (POST) and the named Lease (GET, PUT).
const LEASES_PATH: &str = "/apis/coordination.k8s.io/v1/namespaces/ns/leases";
const LEASE_PATH: &str = "/apis/coordination.k8s.io/v1/namespaces/ns/leases/local-path-csi-controller";

fn timings() -> Timings {
    Timings {
        lease_duration: Duration::from_secs(1),
        renew_deadline: Duration::from_millis(600),
        retry_period: Duration::from_millis(100),
    }
}

fn lease_ref(identity: &str) -> LeaseRef {
    LeaseRef { name: "local-path-csi-controller".into(), namespace: "ns".into(), identity: identity.into() }
}

/// One Lease in memory, with the apiserver's semantics for GET, POST and PUT.
#[derive(Default)]
struct MockLease {
    lease: Mutex<Option<Value>>,
    rv: AtomicU64,
    /// `(method, status)` of every request.
    log: Mutex<Vec<(Method, StatusCode)>>,
    /// Every PUT fails with this status while set.
    fail_put: Mutex<Option<StatusCode>>,
    /// The next PUT answers 409 Conflict regardless of the resourceVersion.
    conflict_once: Mutex<bool>,
}

impl MockLease {
    /// Writes the Lease as another process would (through the apiserver, so the
    /// resourceVersion moves).
    fn set(&self, holder: &str, renew_time: &str, lease_duration: i64, transitions: i64) {
        let rv = self.rv.fetch_add(1, Ordering::SeqCst) + 1;
        *self.lease.lock().unwrap() = Some(json!({
            "apiVersion": "coordination.k8s.io/v1", "kind": "Lease",
            "metadata": {"name": "local-path-csi-controller", "namespace": "ns", "resourceVersion": rv.to_string()},
            "spec": {"holderIdentity": holder, "leaseDurationSeconds": lease_duration, "acquireTime": renew_time,
                     "renewTime": renew_time, "leaseTransitions": transitions}
        }));
    }

    fn spec(&self) -> Value {
        self.lease.lock().unwrap().as_ref().map(|l| l["spec"].clone()).unwrap_or(Value::Null)
    }

    fn writes(&self) -> Vec<(Method, StatusCode)> {
        self.log.lock().unwrap().iter().filter(|(m, _)| *m != Method::GET).cloned().collect()
    }

    fn status(code: u16, reason: &str) -> (StatusCode, Value) {
        (
            StatusCode::from_u16(code).unwrap(),
            json!({"kind": "Status", "apiVersion": "v1", "metadata": {}, "status": "Failure", "reason": reason, "code": code}),
        )
    }

    fn handle(&self, method: &Method, body: &[u8]) -> (StatusCode, Value) {
        let mut lease = self.lease.lock().unwrap();
        match *method {
            Method::GET => match lease.as_ref() {
                Some(l) => (StatusCode::OK, l.clone()),
                None => Self::status(404, "NotFound"),
            },
            Method::POST => {
                if lease.is_some() {
                    return Self::status(409, "AlreadyExists");
                }
                let mut obj: Value = serde_json::from_slice(body).unwrap();
                let rv = self.rv.fetch_add(1, Ordering::SeqCst) + 1;
                obj["metadata"]["resourceVersion"] = Value::String(rv.to_string());
                *lease = Some(obj.clone());
                (StatusCode::CREATED, obj)
            }
            Method::PUT => {
                if let Some(code) = *self.fail_put.lock().unwrap() {
                    return Self::status(code.as_u16(), "InternalError");
                }
                if std::mem::take(&mut *self.conflict_once.lock().unwrap()) {
                    return Self::status(409, "Conflict");
                }
                let Some(cur) = lease.as_ref() else { return Self::status(404, "NotFound") };
                let mut obj: Value = serde_json::from_slice(body).unwrap();
                if obj["metadata"]["resourceVersion"] != cur["metadata"]["resourceVersion"] {
                    return Self::status(409, "Conflict");
                }
                let rv = self.rv.fetch_add(1, Ordering::SeqCst) + 1;
                obj["metadata"]["resourceVersion"] = Value::String(rv.to_string());
                *lease = Some(obj.clone());
                (StatusCode::OK, obj)
            }
            _ => Self::status(405, "MethodNotAllowed"),
        }
    }
}

async fn serve(mock: Arc<MockLease>) -> kube::Client {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else { return };
            let mock = mock.clone();
            tokio::spawn(async move {
                let svc = service_fn(move |req: Request<Incoming>| {
                    let mock = mock.clone();
                    async move {
                        let (parts, body) = req.into_parts();
                        let body = body.collect().await.unwrap().to_bytes();
                        let path = parts.uri.path();
                        let routed = (parts.method == Method::POST && path == LEASES_PATH)
                            || (parts.method != Method::POST && path == LEASE_PATH);
                        let (status, value) =
                            if routed { mock.handle(&parts.method, &body) } else { MockLease::status(404, "NotFound") };
                        mock.log.lock().unwrap().push((parts.method, status));
                        let mut r = Response::new(Full::new(Bytes::from(value.to_string())));
                        *r.status_mut() = status;
                        r.headers_mut().insert("content-type", "application/json".parse().unwrap());
                        Ok::<_, Infallible>(r)
                    }
                });
                let _ = http1::Builder::new().serve_connection(TokioIo::new(stream), svc).await;
            });
        }
    });
    kube::Client::try_from(kube::Config::new(format!("http://{addr}").parse().unwrap())).unwrap()
}

fn now_micro() -> String {
    k8s_openapi::jiff::fmt::strtime::format("%Y-%m-%dT%H:%M:%S%.6fZ", k8s_openapi::jiff::Timestamp::now()).unwrap()
}

/// Polls `cond` for up to `timeout`.
async fn wait_for(timeout: Duration, mut cond: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if cond() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    cond()
}

/// A candidate that is never told to stop.
fn candidate(client: kube::Client, identity: &str, timings: Timings, leading: watch::Sender<bool>) {
    tokio::spawn(run(client, lease_ref(identity), timings, leading, std::future::pending()));
}

async fn leads(leading: &mut watch::Receiver<bool>) {
    tokio::time::timeout(Duration::from_secs(5), leading.wait_for(|leads| *leads))
        .await
        .expect("became leader")
        .unwrap();
}

// No Lease: the candidate creates it with itself as the holder, a fresh acquire time and zero
// transitions, and becomes the leader right away.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn creates_missing_lease_and_leads() {
    let mock = Arc::new(MockLease::default());
    let client = serve(mock.clone()).await;
    let (tx, mut rx) = watch::channel(false);
    candidate(client, "a", timings(), tx);

    leads(&mut rx).await;
    let spec = mock.spec();
    assert_eq!(spec["holderIdentity"], "a");
    assert_eq!(spec["leaseDurationSeconds"], 1);
    assert_eq!(spec["leaseTransitions"], 0);
    assert_eq!(spec["acquireTime"], spec["renewTime"]);
    assert_eq!(mock.writes()[0], (Method::POST, StatusCode::CREATED));
    // The leader keeps renewing: renewTime moves, acquireTime and the transitions do not.
    let acquired = spec["acquireTime"].clone();
    let renew = spec["renewTime"].clone();
    assert!(wait_for(Duration::from_secs(2), || mock.spec()["renewTime"] != renew).await, "no renewal");
    assert_eq!(mock.spec()["acquireTime"], acquired);
    assert_eq!(mock.spec()["leaseTransitions"], 0);
    assert!(*rx.borrow());
}

// Another holder keeps renewing its Lease: the candidate stands by and never writes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_lease_of_another_holder_is_respected() {
    let mock = Arc::new(MockLease::default());
    mock.set("b", &now_micro(), 1, 3);
    let client = serve(mock.clone()).await;
    let (tx, rx) = watch::channel(false);
    candidate(client, "a", timings(), tx);
    // The other holder renews every 300 ms, well within its 1 s lease.
    let renewer = mock.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(300)).await;
            renewer.set("b", &now_micro(), 1, 3);
        }
    });

    tokio::time::sleep(Duration::from_secs(3)).await;
    assert!(!*rx.borrow());
    assert_eq!(mock.spec()["holderIdentity"], "b");
    assert!(mock.writes().is_empty(), "the candidate wrote the lease: {:?}", mock.writes());
    assert!(mock.log.lock().unwrap().len() >= 10, "the candidate keeps polling");
}

// A Lease its holder has stopped renewing is taken over once a lease duration has passed by the
// clock of the candidate since it first saw the record, however old renewTime is: acquireTime is
// set, the transitions are counted up.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn expired_lease_is_taken_over_after_local_observation() {
    let mock = Arc::new(MockLease::default());
    mock.set("b", "2020-01-01T00:00:00.000000Z", 1, 3);
    let client = serve(mock.clone()).await;
    let (tx, mut rx) = watch::channel(false);
    let started = Instant::now();
    candidate(client, "a", timings(), tx);

    leads(&mut rx).await;
    assert!(
        started.elapsed() >= Duration::from_secs(1),
        "took over before the lease duration: {:?}",
        started.elapsed()
    );
    let spec = mock.spec();
    assert_eq!(spec["holderIdentity"], "a");
    assert_eq!(spec["leaseTransitions"], 4);
    assert_ne!(spec["acquireTime"], "2020-01-01T00:00:00.000000Z");
    assert_eq!(spec["acquireTime"], spec["renewTime"]);
    assert_eq!(mock.writes()[0], (Method::PUT, StatusCode::OK));
}

// The leader cannot renew (every PUT fails) for the renew deadline: it says that it leads no
// more, not before the deadline and not much after it. The process goes on - it serves CSI calls
// as well - and leads again when the Lease can be written.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn leadership_that_cannot_be_renewed_is_lost_and_won_back() {
    let mock = Arc::new(MockLease::default());
    let client = serve(mock.clone()).await;
    let (tx, mut rx) = watch::channel(false);
    let t = timings();
    candidate(client, "a", t.clone(), tx);
    leads(&mut rx).await;

    *mock.fail_put.lock().unwrap() = Some(StatusCode::INTERNAL_SERVER_ERROR);
    let failing_since = Instant::now();
    tokio::time::timeout(Duration::from_secs(5), rx.wait_for(|leads| !*leads)).await.expect("lost").unwrap();
    let took = failing_since.elapsed();
    assert!(took >= t.renew_deadline - t.retry_period, "gave up too early: {took:?}");
    assert!(took <= t.renew_deadline + 2 * t.retry_period + Duration::from_millis(100), "gave up too late: {took:?}");
    assert!(mock.writes().contains(&(Method::PUT, StatusCode::INTERNAL_SERVER_ERROR)));

    *mock.fail_put.lock().unwrap() = None;
    leads(&mut rx).await;
    assert_eq!(mock.spec()["holderIdentity"], "a");
}

// A lost race: the PUT of the takeover answers 409, so the candidate is not the leader on that
// attempt and tries again on the next one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conflict_is_retried_on_the_next_attempt() {
    let mock = Arc::new(MockLease::default());
    mock.set("", "2020-01-01T00:00:00.000000Z", 1, 0);
    *mock.conflict_once.lock().unwrap() = true;
    let client = serve(mock.clone()).await;
    let (tx, mut rx) = watch::channel(false);
    candidate(client, "a", timings(), tx);

    leads(&mut rx).await;
    let writes = mock.writes();
    assert_eq!(writes[0], (Method::PUT, StatusCode::CONFLICT));
    assert_eq!(writes[1], (Method::PUT, StatusCode::OK));
    assert_eq!(mock.spec()["holderIdentity"], "a");
    assert_eq!(mock.spec()["leaseTransitions"], 1);
}

// A leader that is told to stop gives the Lease up, and the next candidate has it at once: it
// does not wait for a lease duration to pass, as it would for a holder that just went silent.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn leader_that_stops_hands_the_lease_to_the_next_at_once() {
    let mock = Arc::new(MockLease::default());
    let client = serve(mock.clone()).await;
    let long = Timings { lease_duration: Duration::from_secs(5), ..timings() };
    let (tx, mut rx) = watch::channel(false);
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let leader = tokio::spawn(run(client.clone(), lease_ref("a"), long.clone(), tx, async move {
        let _ = stopped.await;
    }));
    leads(&mut rx).await;

    stop.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(2), leader).await.expect("the leader has stopped").unwrap();
    assert!(!*rx.borrow());
    assert_eq!(mock.spec()["holderIdentity"], "", "the lease names no holder");

    let (tx, mut next) = watch::channel(false);
    let released = Instant::now();
    candidate(client, "b", long, tx);
    leads(&mut next).await;
    assert!(released.elapsed() < Duration::from_secs(2), "waited for the lease to expire: {:?}", released.elapsed());
    assert_eq!(mock.spec()["holderIdentity"], "b");
    assert_eq!(mock.spec()["leaseTransitions"], 1);
}

// A candidate that is told to stop before it leads leaves the Lease of the other holder alone.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn candidate_that_stops_does_not_touch_the_lease_of_another() {
    let mock = Arc::new(MockLease::default());
    mock.set("b", &now_micro(), 5, 3);
    let client = serve(mock.clone()).await;
    let (tx, _rx) = watch::channel(false);
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let standby = tokio::spawn(run(client, lease_ref("a"), timings(), tx, async move {
        let _ = stopped.await;
    }));
    tokio::time::sleep(Duration::from_millis(400)).await;
    stop.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(2), standby).await.expect("the candidate has stopped").unwrap();
    assert_eq!(mock.spec()["holderIdentity"], "b");
    assert!(mock.writes().is_empty(), "{:?}", mock.writes());
}
