//! The node agent (kubernetes.md §6.2, M3): an optional DaemonSet that polls
//! the cgroup v2 files of tracked containers on its node every second and
//! keeps the last values after the container exits (cgroups are removed
//! shortly after exit, so reading only at termination would miss them).
//!
//! **Handoff: pull, not push.** The agent serves its samples on a small
//! JSON endpoint (`GET /v1/containers/{pod_uid}/{container}`); the
//! watcher, when it records a terminated container, asks the agent on that
//! pod's node (found by label + `spec.nodeName`, or a URL template). Why:
//! - the watcher is leader-elected behind a Service with 2+ replicas: a push
//!   would land on any replica, which would then have to forward or store
//!   it; a pull is made by the one replica that writes the record;
//! - the agent needs no credentials to the history store and holds no
//!   state anyone else depends on (it keeps exited containers for 15 min,
//!   far longer than the watcher's termination → record delay);
//! - no ConfigMap/Lease churn on the API server (one write per container
//!   would be heavy at fan-out), and no gRPC dependency.
//!
//! **Authentication.** The samples are other pods' memory and CPU, so the
//! `/v1/*` routes require `Authorization: Bearer <token>` (a shared secret
//! the Helm chart generates, mounts into the agents and the watcher, and
//! fences with a NetworkPolicy); a missing or wrong token gets 401. The
//! token is compared in constant time (SHA-256 of both sides, then a
//! branch-free compare). `/healthz` and `/metrics` (no per-pod data) stay
//! open for probes and scrapes. Running without a token needs the explicit
//! `--allow-unauthenticated` (development only).
//!
//! Tracked containers: pods on this node with the `rightsizer.io/sized`
//! label (listed every 5 s). cgroup paths are found by searching the cgroup
//! root for a directory whose name contains the container id (works for
//! the cgroupfs and systemd drivers and for containerd/CRI-O naming).

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::Router;
use axum::extract::{Path as UrlPath, Query, Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::IntoResponse;
use axum::routing::get;
use chrono::Utc;
use kube::Api;
use prometheus::{IntGauge, Registry};

use super::watcher::AgentSample;
use super::{Client, Pod};
use crate::render::SIZED_LABEL;

/// Agent options.
#[derive(Clone, Debug)]
pub struct AgentOptions {
    /// This node.
    pub node: String,
    /// cgroup v2 mount.
    pub cgroup_root: PathBuf,
    /// Poll period (1 s).
    pub interval: Duration,
    /// Keep exited containers this long.
    pub retain: Duration,
    /// Listen address.
    pub listen: SocketAddr,
    /// Bearer token required on `/v1/*` (`None`: unauthenticated, dev only).
    pub token: Option<String>,
}

#[derive(Debug, Clone)]
struct Track {
    sample: AgentSample,
    path: Option<PathBuf>,
    last_usage_usec: Option<(u64, Instant)>,
    first_usage_usec: Option<u64>,
    first_at: Instant,
    /// Container start (from the pod status), for the mean over its whole life.
    started: Option<chrono::DateTime<Utc>>,
    exited_at: Option<Instant>,
}

/// Shared agent state.
#[derive(Clone, Default)]
pub struct AgentState {
    tracks: Arc<Mutex<HashMap<String, Track>>>,
    registry: Registry,
}

fn read_u64(p: &Path) -> Option<u64> {
    std::fs::read_to_string(p).ok()?.trim().parse().ok()
}

fn usage_usec(dir: &Path) -> Option<u64> {
    let s = std::fs::read_to_string(dir.join("cpu.stat")).ok()?;
    s.lines().find_map(|l| l.strip_prefix("usage_usec ").and_then(|v| v.trim().parse().ok()))
}

/// Find the cgroup directory of a container id under `root` (depth ≤ 7).
pub fn find_cgroup(root: &Path, id: &str) -> Option<PathBuf> {
    fn walk(d: &Path, id: &str, depth: usize) -> Option<PathBuf> {
        if depth > 7 {
            return None;
        }
        let rd = std::fs::read_dir(d).ok()?;
        let mut subs = Vec::new();
        for e in rd.flatten() {
            if !e.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                continue;
            }
            let name = e.file_name().to_string_lossy().to_string();
            if name.contains(id) {
                return Some(e.path());
            }
            subs.push(e.path());
        }
        subs.into_iter().find_map(|s| walk(&s, id, depth + 1))
    }
    // Kubelet's trees first (cheap), then everything.
    for top in ["kubepods", "kubepods.slice", "kubelet.slice"] {
        let p = root.join(top);
        if p.is_dir()
            && let Some(x) = walk(&p, id, 0)
        {
            return Some(x);
        }
    }
    walk(root, id, 0)
}

impl AgentState {
    /// Poll once: update every tracked container's values.
    pub fn poll(&self, opts: &AgentOptions) {
        let Ok(mut t) = self.tracks.lock() else { return };
        let now = Instant::now();
        for tr in t.values_mut() {
            if tr.exited_at.is_some() {
                continue;
            }
            if tr.path.as_ref().is_none_or(|p| !p.exists()) {
                tr.path = find_cgroup(&opts.cgroup_root, &tr.sample.container_id);
            }
            let Some(p) = tr.path.clone() else {
                if tr.sample.samples > 0 {
                    tr.exited_at = Some(now);
                    tr.sample.exited = true;
                }
                continue;
            };
            if !p.exists() {
                tr.exited_at = Some(now);
                tr.sample.exited = true;
                continue;
            }
            let peak = read_u64(&p.join("memory.peak"));
            let cur = read_u64(&p.join("memory.current"));
            match peak {
                Some(pk) => {
                    tr.sample.memory_peak_bytes = tr.sample.memory_peak_bytes.max(pk);
                    tr.sample.memory_peak_exact = true;
                }
                None => {
                    if let Some(c) = cur {
                        tr.sample.memory_peak_bytes = tr.sample.memory_peak_bytes.max(c);
                    }
                }
            }
            if let Some(u) = usage_usec(&p) {
                tr.first_usage_usec.get_or_insert(u);
                if let Some((pu, pt)) = tr.last_usage_usec {
                    let dt = now.duration_since(pt).as_secs_f64();
                    if dt > 0.2 && u >= pu {
                        let mc = (u - pu) as f64 / dt / 1000.0;
                        tr.sample.cpu_peak_millicores = tr.sample.cpu_peak_millicores.max(mc);
                    }
                }
                tr.last_usage_usec = Some((u, now));
                // Mean over the container's life: cpu.stat counts from the cgroup's creation.
                let life = match tr.started {
                    Some(t0) => ((Utc::now() - t0).num_milliseconds() as f64 / 1000.0).max(opts.interval.as_secs_f64()),
                    None => now.duration_since(tr.first_at).as_secs_f64().max(opts.interval.as_secs_f64()),
                };
                let used = if tr.started.is_some() { u } else { u - tr.first_usage_usec.unwrap_or(u) };
                tr.sample.cpu_mean_millicores = used as f64 / life / 1000.0;
            }
            tr.sample.samples += 1;
            tr.sample.last_seen = Utc::now();
        }
        t.retain(|_, tr| tr.exited_at.is_none_or(|e| now.duration_since(e) < opts.retain));
    }

    /// Track the containers of `pods` (running ones get a track).
    pub fn track(&self, pods: &[Pod], interval: Duration) {
        let Ok(mut t) = self.tracks.lock() else { return };
        for p in pods {
            let uid = p.metadata.uid.clone().unwrap_or_default();
            let st = p.status.as_ref();
            let all = st.map(|s| s.container_statuses.clone().unwrap_or_default().into_iter().chain(s.init_container_statuses.clone().unwrap_or_default())).into_iter().flatten();
            for cs in all {
                let Some(cid) = cs.container_id.as_ref().map(|c| c.rsplit("://").next().unwrap_or(c).to_string()) else { continue };
                let Some(started) = cs.state.as_ref().and_then(|s| s.running.as_ref()) else { continue };
                let started = started.started_at.as_ref().and_then(|t| chrono::DateTime::from_timestamp_millis(t.0.as_millisecond()));
                t.entry(cid.clone()).or_insert_with(|| Track {
                    sample: AgentSample {
                        pod_uid: uid.clone(),
                        container: cs.name.clone(),
                        container_id: cid.clone(),
                        first_seen: Utc::now(),
                        last_seen: Utc::now(),
                        resolution_s: interval.as_secs_f32(),
                        ..Default::default()
                    },
                    path: None,
                    last_usage_usec: None,
                    first_usage_usec: None,
                    first_at: Instant::now(),
                    started,
                    exited_at: None,
                });
            }
        }
    }

    /// The sample of a container (by container id when given, else the newest for the name).
    pub fn get(&self, pod_uid: &str, container: &str, container_id: Option<&str>) -> Option<AgentSample> {
        let t = self.tracks.lock().ok()?;
        if let Some(id) = container_id
            && let Some(tr) = t.get(id)
        {
            return Some(tr.sample.clone());
        }
        t.values().filter(|tr| tr.sample.pod_uid == pod_uid && tr.sample.container == container).max_by_key(|tr| tr.sample.first_seen).map(|tr| tr.sample.clone())
    }

    /// All samples.
    pub fn all(&self) -> Vec<AgentSample> {
        self.tracks.lock().map(|t| t.values().map(|x| x.sample.clone()).collect()).unwrap_or_default()
    }
}

#[derive(serde::Deserialize)]
struct Q {
    container_id: Option<String>,
}

/// Constant-time equality of two secrets (SHA-256 both, then a branch-free
/// compare of the digests: neither content nor length leaks through timing).
pub fn secret_eq(a: &[u8], b: &[u8]) -> bool {
    use sha2::{Digest, Sha256};
    let (x, y) = (Sha256::digest(a), Sha256::digest(b));
    x.iter().zip(y.iter()).fold(0u8, |acc, (p, q)| acc | (p ^ q)) == 0
}

/// Whether `headers` carry `Authorization: Bearer <token>`.
pub fn authorized(headers: &axum::http::HeaderMap, token: &str) -> bool {
    let Some(v) = headers.get(axum::http::header::AUTHORIZATION).and_then(|v| v.to_str().ok()) else { return false };
    let Some((scheme, cred)) = v.trim().split_once(' ') else { return false };
    scheme.eq_ignore_ascii_case("bearer") && secret_eq(cred.trim().as_bytes(), token.as_bytes())
}

/// Read a token file (trimmed; an empty file is an error).
pub fn read_token(path: &Path) -> std::io::Result<String> {
    let t = std::fs::read_to_string(path)?.trim().to_string();
    if t.is_empty() {
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, format!("{}: empty token", path.display())));
    }
    Ok(t)
}

/// The agent's HTTP routes; `/v1/*` require `token` when given (401 otherwise).
pub fn router(state: AgentState, token: Option<String>) -> Router {
    let v1 = Router::new()
        .route(
            "/v1/containers/{pod_uid}/{container}",
            get(|State(s): State<AgentState>, UrlPath((uid, c)): UrlPath<(String, String)>, Query(q): Query<Q>| async move {
                match s.get(&uid, &c, q.container_id.as_deref()) {
                    Some(x) => axum::Json(x).into_response(),
                    None => StatusCode::NOT_FOUND.into_response(),
                }
            }),
        )
        .route("/v1/containers", get(|State(s): State<AgentState>| async move { axum::Json(s.all()) }));
    let v1 = match token {
        Some(t) => {
            let t: Arc<str> = Arc::from(t);
            v1.route_layer(axum::middleware::from_fn(move |req: Request, next: Next| {
                let t = t.clone();
                async move {
                    if authorized(req.headers(), &t) {
                        next.run(req).await
                    } else {
                        (StatusCode::UNAUTHORIZED, [(axum::http::header::WWW_AUTHENTICATE, "Bearer")]).into_response()
                    }
                }
            }))
        }
        None => v1,
    };
    v1.route("/healthz", get(|| async { "ok" }))
        .route(
            "/metrics",
            get(|State(s): State<AgentState>| async move {
                let mut buf = Vec::new();
                let _ = prometheus::Encoder::encode(&prometheus::TextEncoder::new(), &s.registry.gather(), &mut buf);
                String::from_utf8(buf).unwrap_or_default()
            }),
        )
        .with_state(state)
}

/// Run the agent until the process exits.
pub async fn run(client: Client, opts: AgentOptions) -> std::io::Result<()> {
    let state = AgentState::default();
    let tracked = IntGauge::new("rightsizer_node_agent_tracked_containers", "Containers tracked.").expect("metric");
    let _ = state.registry.register(Box::new(tracked.clone()));
    if !opts.cgroup_root.join("cgroup.controllers").exists() {
        tracing::warn!(root = %opts.cgroup_root.display(), "no cgroup v2 hierarchy at the cgroup root; samples will be empty");
    }
    // Tracked pods: a watch restricted to this node and the sized label, so a
    // container is tracked within a second of starting (short OOM runs too).
    {
        let st = state.clone();
        let o = opts.clone();
        tokio::spawn(async move {
            use futures::StreamExt as _;
            let api: Api<Pod> = Api::all(client);
            let cfg = kube::runtime::watcher::Config::default().labels(SIZED_LABEL).fields(&format!("spec.nodeName={}", o.node));
            let mut stream = kube::runtime::watcher(api, cfg).boxed();
            while let Some(ev) = stream.next().await {
                match ev {
                    Ok(kube::runtime::watcher::Event::Apply(p)) | Ok(kube::runtime::watcher::Event::InitApply(p)) => st.track(std::slice::from_ref(&p), o.interval),
                    Ok(_) => {}
                    Err(e) => {
                        tracing::warn!(error = %e, "node agent: pod watch error");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        });
    }
    // Poll loop.
    {
        let st = state.clone();
        let o = opts.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(o.interval);
            loop {
                tick.tick().await;
                let s2 = st.clone();
                let o2 = o.clone();
                let _ = tokio::task::spawn_blocking(move || s2.poll(&o2)).await;
                tracked.set(st.all().iter().filter(|x| !x.exited).count() as i64);
            }
        });
    }
    if opts.token.is_none() {
        tracing::warn!("node agent: serving container samples WITHOUT authentication (--allow-unauthenticated)");
    }
    crate::webhook::serve_http(opts.listen, router(state, opts.token.clone())).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finds_and_polls_a_fake_cgroup() {
        let root = std::env::temp_dir().join(format!("rsz-agent-{}", std::process::id()));
        let dir = root.join("kubepods/burstable/podabc/cri-containerd-0123abcd.scope");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("memory.peak"), "1048576\n").unwrap();
        std::fs::write(dir.join("cpu.stat"), "usage_usec 1000\nuser_usec 900\n").unwrap();
        assert_eq!(find_cgroup(&root, "0123abcd"), Some(dir.clone()));
        let st = AgentState::default();
        st.tracks.lock().unwrap().insert(
            "0123abcd".into(),
            Track {
                sample: AgentSample { pod_uid: "u".into(), container: "main".into(), container_id: "0123abcd".into(), ..Default::default() },
                path: None,
                last_usage_usec: None,
                first_usage_usec: None,
                first_at: Instant::now(),
                started: None,
                exited_at: None,
            },
        );
        let o = AgentOptions { node: "n".into(), cgroup_root: root.clone(), interval: Duration::from_secs(1), retain: Duration::from_secs(60), listen: "127.0.0.1:0".parse().unwrap(), token: None };
        st.poll(&o);
        let s = st.get("u", "main", None).unwrap();
        assert_eq!(s.memory_peak_bytes, 1048576);
        assert!(s.memory_peak_exact);
        std::fs::remove_dir_all(&dir).unwrap();
        st.poll(&o);
        assert!(st.get("u", "main", Some("0123abcd")).unwrap().exited);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn samples_need_the_bearer_token() {
        use tower::ServiceExt as _;
        let st = AgentState::default();
        st.tracks.lock().unwrap().insert(
            "c1".into(),
            Track {
                sample: AgentSample { pod_uid: "u".into(), container: "main".into(), container_id: "c1".into(), memory_peak_bytes: 42, ..Default::default() },
                path: None,
                last_usage_usec: None,
                first_usage_usec: None,
                first_at: Instant::now(),
                started: None,
                exited_at: None,
            },
        );
        let app = router(st, Some("t0ken".into()));
        let call = |path: &str, auth: Option<&str>| {
            let mut b = axum::http::Request::builder().uri(path);
            if let Some(a) = auth {
                b = b.header("authorization", a);
            }
            app.clone().oneshot(b.body(axum::body::Body::empty()).unwrap())
        };
        for (auth, want) in [
            (None, StatusCode::UNAUTHORIZED),
            (Some("Bearer wrong"), StatusCode::UNAUTHORIZED),
            (Some("Bearer t0ke"), StatusCode::UNAUTHORIZED),
            (Some("Basic t0ken"), StatusCode::UNAUTHORIZED),
            (Some("t0ken"), StatusCode::UNAUTHORIZED),
            (Some("Bearer t0ken"), StatusCode::OK),
            (Some("bearer  t0ken "), StatusCode::OK),
        ] {
            assert_eq!(call("/v1/containers/u/main", auth).await.unwrap().status(), want, "{auth:?}");
            assert_eq!(call("/v1/containers", auth).await.unwrap().status(), want, "{auth:?}");
        }
        let r = call("/v1/containers/u/main", Some("Bearer t0ken")).await.unwrap();
        let body = axum::body::to_bytes(r.into_body(), 1 << 20).await.unwrap();
        assert_eq!(serde_json::from_slice::<AgentSample>(&body).unwrap().memory_peak_bytes, 42);
        // Probes and scrapes stay open; unknown containers are 404 behind auth.
        assert_eq!(call("/healthz", None).await.unwrap().status(), StatusCode::OK);
        assert_eq!(call("/metrics", None).await.unwrap().status(), StatusCode::OK);
        assert_eq!(call("/v1/containers/x/y", None).await.unwrap().status(), StatusCode::UNAUTHORIZED);
        assert_eq!(call("/v1/containers/x/y", Some("Bearer t0ken")).await.unwrap().status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn constant_time_compare_and_token_files() {
        assert!(secret_eq(b"abc", b"abc"));
        assert!(!secret_eq(b"abc", b"abd") && !secret_eq(b"abc", b"abcd") && !secret_eq(b"", b"x"));
        let d = std::env::temp_dir().join(format!("rsz-token-{}", std::process::id()));
        std::fs::write(&d, "  s3cret\n").unwrap();
        assert_eq!(read_token(&d).unwrap(), "s3cret");
        std::fs::write(&d, "\n").unwrap();
        assert!(read_token(&d).is_err());
        let _ = std::fs::remove_file(&d);
    }
}
