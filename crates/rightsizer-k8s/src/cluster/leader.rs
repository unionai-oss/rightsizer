//! Lease-based leader election (`coordination.k8s.io/v1` Lease), for the
//! watcher (kubernetes.md §6.1: one writer of outcomes per cluster).
//!
//! Standard client-go semantics, simplified: the holder renews every
//! `renew`; another candidate takes over when `renewTime + leaseDuration`
//! has passed. Updates use `resourceVersion` (optimistic concurrency), so
//! two candidates never both win a takeover.

use std::time::Duration;

use chrono::Utc;
use k8s_openapi::api::coordination::v1::{Lease, LeaseSpec};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{MicroTime, ObjectMeta};
use kube::Api;
use kube::api::PostParams;
use tokio::sync::watch;

use super::Client;

/// Leader election settings.
#[derive(Clone, Debug)]
pub struct LeaseConfig {
    /// Namespace of the Lease.
    pub namespace: String,
    /// Lease name.
    pub name: String,
    /// This candidate.
    pub identity: String,
    /// Lease duration.
    pub duration: Duration,
    /// Renew period.
    pub renew: Duration,
}

impl LeaseConfig {
    /// Defaults: 15 s lease, renewed every 5 s; identity = `POD_NAME` or host-pid.
    pub fn new(namespace: impl Into<String>, name: impl Into<String>) -> Self {
        let identity = std::env::var("POD_NAME").ok().filter(|s| !s.is_empty()).unwrap_or_else(|| format!("{}-{}", hostname(), std::process::id()));
        LeaseConfig { namespace: namespace.into(), name: name.into(), identity, duration: Duration::from_secs(15), renew: Duration::from_secs(5) }
    }
}

fn hostname() -> String {
    std::env::var("HOSTNAME").ok().filter(|s| !s.is_empty()).unwrap_or_else(|| "rsz".into())
}

/// Try once to acquire or renew; `Ok(true)` when we hold the lease.
pub async fn try_acquire(api: &Api<Lease>, cfg: &LeaseConfig) -> Result<bool, kube::Error> {
    let now = MicroTime(k8s_openapi::jiff::Timestamp::now());
    let secs = cfg.duration.as_secs() as i32;
    match api.get_opt(&cfg.name).await? {
        None => {
            let l = Lease {
                metadata: ObjectMeta { name: Some(cfg.name.clone()), namespace: Some(cfg.namespace.clone()), ..Default::default() },
                spec: Some(LeaseSpec {
                    holder_identity: Some(cfg.identity.clone()),
                    lease_duration_seconds: Some(secs),
                    acquire_time: Some(now.clone()),
                    renew_time: Some(now),
                    lease_transitions: Some(0),
                    ..Default::default()
                }),
            };
            match api.create(&PostParams::default(), &l).await {
                Ok(_) => Ok(true),
                Err(kube::Error::Api(e)) if e.code == 409 => Ok(false),
                Err(e) => Err(e),
            }
        }
        Some(mut l) => {
            let spec = l.spec.clone().unwrap_or_default();
            let holder = spec.holder_identity.clone().unwrap_or_default();
            let expired = spec
                .renew_time
                .as_ref()
                .map(|t| Utc::now().timestamp_millis() > t.0.as_millisecond() + 1000 * spec.lease_duration_seconds.unwrap_or(secs) as i64)
                .unwrap_or(true);
            if holder != cfg.identity && !expired && !holder.is_empty() {
                return Ok(false);
            }
            let mut s = spec;
            if holder != cfg.identity {
                s.acquire_time = Some(now.clone());
                s.lease_transitions = Some(s.lease_transitions.unwrap_or(0) + 1);
            }
            s.holder_identity = Some(cfg.identity.clone());
            s.lease_duration_seconds = Some(secs);
            s.renew_time = Some(now);
            l.spec = Some(s);
            match api.replace(&cfg.name, &PostParams::default(), &l).await {
                Ok(_) => Ok(true),
                Err(kube::Error::Api(e)) if e.code == 409 => Ok(false),
                Err(e) => Err(e),
            }
        }
    }
}

/// Run the election until the process exits; the receiver says whether we
/// lead. Losing the API for longer than the lease duration steps down.
pub fn spawn(client: Client, cfg: LeaseConfig) -> watch::Receiver<bool> {
    let (tx, rx) = watch::channel(false);
    tokio::spawn(async move {
        let api: Api<Lease> = Api::namespaced(client, &cfg.namespace);
        let mut last_ok = std::time::Instant::now();
        loop {
            let leading = *tx.borrow();
            match try_acquire(&api, &cfg).await {
                Ok(held) => {
                    last_ok = std::time::Instant::now();
                    if held != leading {
                        tracing::info!(identity = %cfg.identity, lease = %cfg.name, leading = held, "leader election");
                    }
                    let _ = tx.send(held);
                }
                Err(e) => {
                    tracing::warn!(error = %e, "lease renew failed");
                    if leading && last_ok.elapsed() > cfg.duration {
                        let _ = tx.send(false);
                    }
                }
            }
            tokio::time::sleep(if *tx.borrow() { cfg.renew } else { cfg.renew.max(Duration::from_secs(2)) }).await;
        }
    });
    rx
}
