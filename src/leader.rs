//! Leader election through a `coordination.k8s.io` Lease, after the `leaderelection` package of
//! client-go: the candidates poll the Lease every retry period; a Lease whose holder has not
//! renewed it for its lease duration (measured by the local clock, from the moment the record
//! was last seen to change) can be taken over; a leader that fails to renew for the renew
//! deadline gives up. Unlike client-go there is no optimistic fast path: every attempt reads
//! the Lease and writes it with the read `resourceVersion`, so a lost race ends in 409 and is
//! retried on the next tick.
//!
//! What the leader does it does with writes that carry a condition, so two replicas that both
//! take themselves for the leader for a moment undo nothing of each other. That is why a leader
//! that has lost the Lease stands for it again instead of ending the process, which serves CSI
//! calls and the pod webhook as well.

use std::future::Future;
use std::time::{Duration, Instant};

use k8s_openapi::api::coordination::v1::{Lease, LeaseSpec};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{MicroTime, ObjectMeta};
use k8s_openapi::jiff::Timestamp;
use kube::Client;
use kube::api::{Api, PostParams};
use tokio::sync::watch;

/// client-go defaults, as used by the Kubernetes controllers.
#[derive(Clone, Debug)]
pub struct Timings {
    /// How long a candidate waits after the last change of the Lease it has seen before it takes
    /// the Lease over. Written to the Lease as `leaseDurationSeconds` (whole seconds).
    pub lease_duration: Duration,
    /// How long the leader keeps trying to renew before it gives up leadership.
    pub renew_deadline: Duration,
    /// Interval between attempts.
    pub retry_period: Duration,
}

impl Default for Timings {
    fn default() -> Self {
        Timings {
            lease_duration: Duration::from_secs(15),
            renew_deadline: Duration::from_secs(10),
            retry_period: Duration::from_secs(2),
        }
    }
}

#[derive(Clone, Debug)]
pub struct LeaseRef {
    pub name: String,
    pub namespace: String,
    /// `holderIdentity` of this process.
    pub identity: String,
}

pub struct Elector {
    api: Api<Lease>,
    lease: LeaseRef,
    timings: Timings,
    /// The last Lease spec seen, and when it was first seen in this form.
    observed: Option<LeaseSpec>,
    observed_at: Instant,
}

impl Elector {
    pub fn new(client: Client, lease: LeaseRef, timings: Timings) -> Self {
        let api = Api::namespaced(client, &lease.namespace);
        Elector { api, lease, timings, observed: None, observed_at: Instant::now() }
    }

    /// Whether the last observed record names this process as the holder.
    pub fn is_leader(&self) -> bool {
        self.observed.as_ref().and_then(|s| s.holder_identity.as_deref()) == Some(self.lease.identity.as_str())
    }

    /// `isLeaseValid`: the observed record has not expired by the local clock. The duration is
    /// the one written by its holder.
    fn observed_lease_valid(&self, now: Instant) -> bool {
        let secs = self.observed.as_ref().and_then(|s| s.lease_duration_seconds).unwrap_or(0).max(0) as u64;
        self.observed_at + Duration::from_secs(secs) > now
    }

    /// A record read from the API server: the observation time moves only when it changed.
    fn observe_remote(&mut self, spec: LeaseSpec) {
        if self.observed.as_ref() != Some(&spec) {
            self.observed = Some(spec);
            self.observed_at = Instant::now();
        }
    }

    /// A record this process has just written.
    fn observe_own(&mut self, spec: LeaseSpec) {
        self.observed = Some(spec);
        self.observed_at = Instant::now();
    }

    fn lease(&self, resource_version: Option<String>, spec: LeaseSpec) -> Lease {
        Lease {
            metadata: ObjectMeta {
                name: Some(self.lease.name.clone()),
                namespace: Some(self.lease.namespace.clone()),
                resource_version,
                ..Default::default()
            },
            spec: Some(spec),
        }
    }

    /// `tryAcquireOrRenew`: one attempt; `true` when this process holds the Lease afterwards.
    pub async fn try_acquire_or_renew(&mut self) -> bool {
        let name = self.lease.name.clone();
        let now = MicroTime(Timestamp::now());
        let mut desired = LeaseSpec {
            holder_identity: Some(self.lease.identity.clone()),
            lease_duration_seconds: Some(self.timings.lease_duration.as_secs().min(i32::MAX as u64) as i32),
            acquire_time: Some(now.clone()),
            renew_time: Some(now),
            lease_transitions: Some(0),
            ..Default::default()
        };
        let current = match self.api.get(&name).await {
            Ok(lease) => lease,
            Err(kube::Error::Api(status)) if status.code == 404 => {
                return match self.api.create(&PostParams::default(), &self.lease(None, desired.clone())).await {
                    Ok(_) => {
                        self.observe_own(desired);
                        true
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "creating the lease failed");
                        false
                    }
                };
            }
            Err(e) => {
                tracing::warn!(error = %e, "reading the lease failed");
                return false;
            }
        };
        let spec = current.spec.unwrap_or_default();
        self.observe_remote(spec.clone());
        let held = spec.holder_identity.as_deref().is_some_and(|holder| !holder.is_empty());
        if held && !self.is_leader() && self.observed_lease_valid(Instant::now()) {
            let holder = spec.holder_identity.as_deref().unwrap_or_default();
            tracing::debug!(holder, "lease is held and has not expired");
            return false;
        }
        if self.is_leader() {
            desired.acquire_time = spec.acquire_time;
            desired.lease_transitions = spec.lease_transitions;
        } else {
            desired.lease_transitions = Some(spec.lease_transitions.unwrap_or(0).saturating_add(1));
        }
        let lease = self.lease(current.metadata.resource_version, desired.clone());
        match self.api.replace(&name, &PostParams::default(), &lease).await {
            Ok(_) => {
                self.observe_own(desired);
                true
            }
            Err(e) => {
                tracing::warn!(error = %e, "updating the lease failed");
                false
            }
        }
    }

    /// `release`: a leader that stops writes the Lease without a holder, so the next candidate
    /// has it at its next attempt and not a lease duration later. A Lease that names another
    /// holder by now is left alone.
    pub async fn release(&mut self) {
        if !self.is_leader() {
            return;
        }
        let name = self.lease.name.clone();
        let current = match self.api.get(&name).await {
            Ok(lease) => lease,
            Err(e) => {
                tracing::warn!(error = %e, "reading the lease to give it up failed");
                return;
            }
        };
        let spec = current.spec.unwrap_or_default();
        if spec.holder_identity.as_deref() != Some(self.lease.identity.as_str()) {
            return;
        }
        let now = MicroTime(Timestamp::now());
        let released = LeaseSpec {
            holder_identity: Some(String::new()),
            lease_duration_seconds: Some(1),
            acquire_time: Some(now.clone()),
            renew_time: Some(now),
            lease_transitions: spec.lease_transitions,
            ..Default::default()
        };
        let lease = self.lease(current.metadata.resource_version, released.clone());
        match self.api.replace(&name, &PostParams::default(), &lease).await {
            Ok(_) => self.observe_own(released),
            Err(e) => tracing::warn!(error = %e, "giving the lease up failed"),
        }
    }
}

/// Stands for the Lease until `stopping` ends. `leading` is `true` while this process holds the
/// Lease: from the attempt that got it until one that came a renew deadline after the last
/// that renewed it. A leader that is told to stop gives the Lease up before this returns.
pub async fn run(
    client: Client,
    lease: LeaseRef,
    timings: Timings,
    leading: watch::Sender<bool>,
    stopping: impl Future<Output = ()>,
) {
    let mut elector = Elector::new(client, lease.clone(), timings.clone());
    let mut tick = tokio::time::interval(timings.retry_period);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut stopping = std::pin::pin!(stopping);
    loop {
        loop {
            tokio::select! {
                _ = &mut stopping => return,
                _ = tick.tick() => {}
            }
            let attempt = tokio::time::timeout(timings.renew_deadline, elector.try_acquire_or_renew());
            if matches!(attempt.await, Ok(true)) {
                break;
            }
        }
        tracing::info!(lease = %lease.name, namespace = %lease.namespace, identity = %lease.identity, "became leader");
        leading.send_replace(true);
        let mut renewed = Instant::now();
        loop {
            tokio::select! {
                _ = &mut stopping => {
                    leading.send_replace(false);
                    elector.release().await;
                    return;
                }
                _ = tick.tick() => {}
            }
            // Not past the deadline: one attempt may hang for the rest of it, no longer.
            let remaining = timings.renew_deadline.saturating_sub(renewed.elapsed());
            if !remaining.is_zero()
                && matches!(tokio::time::timeout(remaining, elector.try_acquire_or_renew()).await, Ok(true))
            {
                renewed = Instant::now();
                continue;
            }
            if renewed.elapsed() >= timings.renew_deadline {
                tracing::error!(lease = %lease.name, identity = %lease.identity, "failed to renew the lease within the renew deadline: standing for it again");
                leading.send_replace(false);
                break;
            }
        }
    }
}
