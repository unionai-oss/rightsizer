//! The watcher (kubernetes.md §6.1): a leader-elected controller in the
//! webhook deployment that writes one `RunRecord` per terminated container
//! of sized or annotated pods (label `rightsizer.io/sized`, written at
//! admission) in enabled namespaces, in every mode.
//!
//! - **Status**: `state.terminated.reason` (`OOMKilled`, `Completed`,
//!   `Error`), the owning Job's `DeadlineExceeded`, the pod's `Evicted`
//!   reason or a `DisruptionTarget` preemption condition.
//! - **Peaks**: the node agent on the pod's node when configured and it
//!   has the container; otherwise Prometheus (cAdvisor), queried two scrape
//!   intervals after termination so the last samples are in.
//! - **Trust**: `Verified` (tenant from the namespace; status from the apiserver).
//! - **Join**: the unit's decision id from the owning object's
//!   `rightsizer.io/recommendation` (per unit) or `rightsizer.io/decision-id`;
//!   otherwise the newest decision of (key, unit) logged since the pod's
//!   creation (the (tenant, owner, unit) fallback).
//! - **Dedup**: in-process on (pod uid, container, restart count); the store
//!   dedups on (key, invocation, attempt, source).
//! - After each outcome the unit is re-decided (dry run) so the memo the
//!   webhook reads is warm with the new history.

use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Utc};
use futures::StreamExt as _;
use kube::Api;
use kube::api::ListParams;
use kube::runtime::watcher;
use prometheus::{Gauge, IntCounterVec, IntGauge, Opts};
use rightsizer_core::{Censor, Confidence, Measurement, Mode, OutcomeSource, RunRecord, RunStatus, Trust};
use rightsizer_engine::{Engine, Policy};
use rightsizer_history::DecisionFilter;
use rightsizer_plugin::{AdapterEnv, Native, PlatformModel, Unit};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use ulid::Ulid;

use super::prom::{self, Prom};
use super::{Client, KubeView, Pod, get_json};
use crate::K8sAdapter;
use crate::objects::{Kind, string_map};
use crate::render::SIZED_LABEL;
use crate::view::ClusterView;

/// Annotation set by `rsz apply --watch` on every Job of one ladder.
pub const INVOCATION_ANNOTATION: &str = "rightsizer.io/invocation";
/// Annotation set by `rsz apply --watch`: the attempt number.
pub const ATTEMPT_ANNOTATION: &str = "rightsizer.io/attempt";

/// What the node agent knows about one container (its JSON endpoint).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct AgentSample {
    /// Pod UID.
    pub pod_uid: String,
    /// Container name.
    pub container: String,
    /// Container id (`containerd://…` stripped).
    pub container_id: String,
    /// cgroup v2 `memory.peak` (or the max of `memory.current` samples), bytes.
    pub memory_peak_bytes: u64,
    /// Whether `memory.peak` was available (else sampled `memory.current`).
    pub memory_peak_exact: bool,
    /// Mean CPU over the observed life, millicores (`cpu.stat usage_usec`).
    pub cpu_mean_millicores: f64,
    /// Busiest 1 s window, millicores.
    pub cpu_peak_millicores: f64,
    /// Polls taken.
    pub samples: u64,
    /// First and last poll.
    pub first_seen: DateTime<Utc>,
    /// Last poll.
    pub last_seen: DateTime<Utc>,
    /// The cgroup is gone.
    pub exited: bool,
    /// Sampling period, s.
    pub resolution_s: f32,
}

/// Reads [`AgentSample`]s from the node agent on a node.
#[derive(Clone)]
pub struct NodeAgentClient {
    http: reqwest::Client,
    client: Client,
    /// Namespace of the agent pods.
    pub namespace: String,
    /// Label selector of the agent pods.
    pub selector: String,
    /// Agent port.
    pub port: u16,
    /// Fixed URL template (`http://{node}:9102`), instead of pod-IP discovery.
    pub url_template: Option<String>,
}

impl NodeAgentClient {
    /// A client discovering agent pods by label in `namespace`.
    pub fn new(client: Client, namespace: &str, selector: &str, port: u16, url_template: Option<String>) -> Result<Self, String> {
        let http = reqwest::Client::builder().timeout(Duration::from_secs(3)).build().map_err(|e| e.to_string())?;
        Ok(NodeAgentClient { http, client, namespace: namespace.into(), selector: selector.into(), port, url_template })
    }

    async fn base(&self, node: &str) -> Option<String> {
        if let Some(t) = &self.url_template {
            return Some(t.replace("{node}", node));
        }
        let api: Api<Pod> = Api::namespaced(self.client.clone(), &self.namespace);
        let lp = ListParams::default().labels(&self.selector).fields(&format!("spec.nodeName={node}"));
        let pods = api.list(&lp).await.ok()?;
        let ip = pods.items.iter().find_map(|p| p.status.as_ref().and_then(|s| s.pod_ip.clone()))?;
        Some(format!("http://{ip}:{}", self.port))
    }

    /// The agent's sample for a container, if it has one.
    pub async fn sample(&self, node: &str, pod_uid: &str, container: &str, container_id: Option<&str>) -> Option<AgentSample> {
        let base = self.base(node).await?;
        let mut url = format!("{base}/v1/containers/{pod_uid}/{container}");
        if let Some(id) = container_id {
            url.push_str(&format!("?container_id={id}"));
        }
        let r = self.http.get(url).send().await.ok()?;
        if !r.status().is_success() {
            return None;
        }
        r.json().await.ok()
    }
}

/// Watcher settings.
#[derive(Clone)]
pub struct WatcherConfig {
    /// Prometheus (peaks when the node agent has none).
    pub prom: Option<Prom>,
    /// Scrape interval of the cAdvisor job, s.
    pub scrape_s: u32,
    /// Node agent.
    pub node_agent: Option<NodeAgentClient>,
    /// Wait after termination before reading peaks.
    pub delay: Duration,
    /// Adapter settings map (`[k8s]`).
    pub base: Map<String, Value>,
}

/// Watcher metrics.
#[derive(Clone)]
pub struct WatcherMetrics {
    /// `rightsizer_watcher_lag_seconds`: termination → record written, last record.
    pub lag: Gauge,
    /// `rightsizer_watcher_records_total{status, peak_source}`.
    pub records: IntCounterVec,
    /// `rightsizer_watcher_leader`.
    pub leader: IntGauge,
}

impl WatcherMetrics {
    /// Register in `registry`.
    pub fn new(registry: &prometheus::Registry) -> Self {
        let lag = Gauge::new("rightsizer_watcher_lag_seconds", "Seconds from container termination to its RunRecord (last record).").expect("metric");
        let records = IntCounterVec::new(Opts::new("rightsizer_watcher_records_total", "RunRecords written by the watcher."), &["status", "peak_source"]).expect("metric");
        let leader = IntGauge::new("rightsizer_watcher_leader", "1 while this replica leads the watcher.").expect("metric");
        let _ = registry.register(Box::new(lag.clone()));
        let _ = registry.register(Box::new(records.clone()));
        let _ = registry.register(Box::new(leader.clone()));
        WatcherMetrics { lag, records, leader }
    }
}

/// The watcher.
pub struct Watcher {
    client: Client,
    engine: Engine,
    adapter: Arc<dyn Fn() -> Arc<K8sAdapter> + Send + Sync>,
    view: KubeView,
    cfg: WatcherConfig,
    metrics: WatcherMetrics,
    seen: Mutex<(HashSet<String>, VecDeque<String>)>,
}

/// One terminated container to record.
#[derive(Clone, Debug)]
pub struct Term {
    /// The pod.
    pub pod: Value,
    /// Container name.
    pub container: String,
    /// Restart count of the run that terminated.
    pub restart: u32,
    /// `state.terminated` (or `lastState.terminated`).
    pub term: Value,
    /// Status known from the pod (eviction).
    pub status_hint: Option<RunStatus>,
}

fn ts(v: Option<&Value>) -> Option<DateTime<Utc>> {
    v.and_then(Value::as_str).and_then(|s| DateTime::parse_from_rfc3339(s).ok()).map(|d| d.with_timezone(&Utc))
}

/// Status of a terminated container (kubernetes.md §6.1).
pub fn status_of(term: &Value, pod: &Value, owner: Option<&Value>) -> RunStatus {
    let reason = term.get("reason").and_then(Value::as_str).unwrap_or("");
    let exit = term.get("exitCode").and_then(Value::as_i64);
    if reason == "OOMKilled" {
        return RunStatus::OomKilled;
    }
    if pod.pointer("/status/reason").and_then(Value::as_str) == Some("Evicted") {
        return RunStatus::Evicted;
    }
    let conds = pod.pointer("/status/conditions").and_then(Value::as_array).cloned().unwrap_or_default();
    if conds.iter().any(|c| c.get("type").and_then(Value::as_str) == Some("DisruptionTarget") && c.get("reason").and_then(Value::as_str).is_some_and(|r| r.contains("Preemption"))) {
        return RunStatus::Preempted;
    }
    if reason == "Completed" || exit == Some(0) {
        return RunStatus::Succeeded;
    }
    if let Some(o) = owner {
        let oc = o.pointer("/status/conditions").and_then(Value::as_array).cloned().unwrap_or_default();
        if oc.iter().any(|c| c.get("reason").and_then(Value::as_str) == Some("DeadlineExceeded") && c.get("status").and_then(Value::as_str) == Some("True")) {
            return RunStatus::Timeout;
        }
    }
    if reason == "DeadlineExceeded" {
        return RunStatus::Timeout;
    }
    RunStatus::Failed
}

/// The unit of `top` that container `c` of `pod` belongs to.
pub fn match_unit<'a>(units: &'a [Unit], pod: &Value, c: &str) -> Option<&'a Unit> {
    let tag = format!("[{c}]");
    let cands: Vec<&Unit> = units.iter().filter(|u| u.id.0.ends_with(&tag) || u.id.0.contains(&format!("{tag}."))).collect();
    if cands.len() <= 1 {
        return cands.into_iter().next();
    }
    let labels = string_map(pod, "/metadata/labels");
    let hint = labels
        .get("jobset.sigs.k8s.io/replicatedjob-name")
        .map(|g| format!("[{g}]"))
        .or_else(|| labels.get("training.kubeflow.org/replica-type").map(|t| format!(".{}.", t.to_lowercase())))
        .or_else(|| labels.get("ray.io/group").map(|g| if labels.get("ray.io/node-type").map(String::as_str) == Some("head") { "headGroupSpec".into() } else { format!("[{g}]") }))
        .or_else(|| labels.get("volcano.sh/task-spec").map(|t| format!("[{t}]")));
    match hint {
        Some(h) => cands.iter().copied().find(|u| u.id.0.to_lowercase().contains(&h.to_lowercase())).or(cands.first().copied()),
        None => cands.first().copied(),
    }
}

impl Watcher {
    /// A watcher. `adapter` returns the current adapter (keys rotate).
    pub fn new(client: Client, engine: Engine, adapter: Arc<dyn Fn() -> Arc<K8sAdapter> + Send + Sync>, view: KubeView, cfg: WatcherConfig) -> Arc<Self> {
        let metrics = WatcherMetrics::new(&engine.metrics().registry);
        Arc::new(Watcher { client, engine, adapter, view, cfg, metrics, seen: Mutex::new((HashSet::new(), VecDeque::new())) })
    }

    fn first_time(&self, id: &str) -> bool {
        let Ok(mut s) = self.seen.lock() else { return false };
        if s.0.contains(id) {
            return false;
        }
        s.0.insert(id.to_string());
        s.1.push_back(id.to_string());
        while s.1.len() > 50_000 {
            if let Some(old) = s.1.pop_front() {
                s.0.remove(&old);
            }
        }
        true
    }

    /// Run while `leading` says so (forever).
    pub async fn run(self: Arc<Self>, mut leading: tokio::sync::watch::Receiver<bool>) {
        loop {
            while !*leading.borrow() {
                self.metrics.leader.set(0);
                if leading.changed().await.is_err() {
                    return;
                }
            }
            self.metrics.leader.set(1);
            tracing::info!("watcher: leading; watching sized pods");
            let me = self.clone();
            let work = tokio::spawn(async move { me.watch_pods().await });
            loop {
                if leading.changed().await.is_err() || !*leading.borrow() {
                    break;
                }
            }
            work.abort();
            tracing::info!("watcher: lost leadership");
        }
    }

    /// Run without leader election (tests, single replica).
    pub async fn watch_pods(self: Arc<Self>) {
        let api: Api<Pod> = Api::all(self.client.clone());
        let mut stream = watcher(api, watcher::Config::default().labels(SIZED_LABEL)).boxed();
        while let Some(ev) = stream.next().await {
            let pods = match ev {
                Ok(watcher::Event::Apply(p)) | Ok(watcher::Event::InitApply(p)) => vec![(p, false)],
                Ok(watcher::Event::Delete(p)) => vec![(p, true)],
                Ok(_) => vec![],
                Err(e) => {
                    tracing::warn!(error = %e, "pod watch error");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    vec![]
                }
            };
            for (p, deleted) in pods {
                let Ok(v) = serde_json::to_value(&p) else { continue };
                for t in self.terminations(&v, deleted) {
                    let me = self.clone();
                    tokio::spawn(async move { me.process(t).await });
                }
            }
        }
    }

    fn terminations(&self, pod: &Value, deleted: bool) -> Vec<Term> {
        let uid = pod.pointer("/metadata/uid").and_then(Value::as_str).unwrap_or("").to_string();
        let mut out = Vec::new();
        let evicted = pod.pointer("/status/reason").and_then(Value::as_str) == Some("Evicted");
        for list in ["initContainerStatuses", "containerStatuses"] {
            for cs in pod.pointer(&format!("/status/{list}")).and_then(Value::as_array).into_iter().flatten() {
                let name = cs.get("name").and_then(Value::as_str).unwrap_or("").to_string();
                let restarts = cs.get("restartCount").and_then(Value::as_u64).unwrap_or(0) as u32;
                let mut push = |term: &Value, restart: u32, hint: Option<RunStatus>| {
                    let id = format!("{uid}/{name}/{restart}");
                    if self.first_time(&id) {
                        out.push(Term { pod: pod.clone(), container: name.clone(), restart, term: term.clone(), status_hint: hint });
                    }
                };
                if let Some(t) = cs.pointer("/state/terminated") {
                    push(t, restarts, None);
                } else if evicted || (deleted && pod.pointer("/status/phase").and_then(Value::as_str) == Some("Running")) {
                    // Evicted or deleted while running: no terminated state.
                    let hint = if evicted { Some(RunStatus::Evicted) } else { None };
                    if hint.is_some() {
                        push(&serde_json::json!({"reason": "Evicted", "finishedAt": Utc::now().to_rfc3339()}), restarts, hint);
                    }
                }
                if restarts > 0
                    && let Some(t) = cs.pointer("/lastState/terminated")
                {
                    push(t, restarts - 1, None);
                }
            }
        }
        out
    }

    /// The topmost known workload owning `pod` (Job → CronJob, Job → JobSet, …), or the pod.
    async fn top_owner(&self, pod: &Value, ns: &str) -> (Value, Option<Value>) {
        let mut cur = pod.clone();
        let mut direct: Option<Value> = None;
        for _ in 0..4 {
            let owner = cur.pointer("/metadata/ownerReferences").and_then(Value::as_array).and_then(|os| {
                os.iter().find(|o| o.get("controller").and_then(Value::as_bool) == Some(true)).or(os.first()).cloned()
            });
            let Some(o) = owner else { break };
            let (Some(av), Some(kind), Some(name)) = (o.get("apiVersion").and_then(Value::as_str), o.get("kind").and_then(Value::as_str), o.get("name").and_then(Value::as_str)) else { break };
            let probe = serde_json::json!({"apiVersion": av, "kind": kind});
            if Kind::of(&probe).is_none() {
                break;
            }
            match get_json(&self.client, av, kind, Some(ns), name).await {
                Ok(Some(v)) => {
                    if direct.is_none() {
                        direct = Some(v.clone());
                    }
                    cur = v;
                }
                _ => break,
            }
        }
        (cur, direct)
    }

    async fn process(self: Arc<Self>, t: Term) {
        let finished = ts(t.term.get("finishedAt")).unwrap_or_else(Utc::now);
        let wait = if self.cfg.node_agent.is_some() && self.cfg.prom.is_none() { Duration::from_secs(1) } else { self.cfg.delay };
        let due = finished + chrono::Duration::from_std(wait).unwrap_or_default();
        let now = Utc::now();
        if due > now {
            tokio::time::sleep((due - now).to_std().unwrap_or_default()).await;
        }
        match self.record(&t).await {
            Ok(Some(rec)) => {
                let lag = (Utc::now() - finished).num_milliseconds() as f64 / 1000.0;
                self.metrics.lag.set(lag);
                tracing::info!(key = %rec.key, status = ?rec.status, attempt = rec.attempt, decision = ?rec.decision_id, lag_s = lag, "watcher: outcome recorded");
            }
            Ok(None) => {}
            Err(e) => tracing::warn!(error = %e, pod = ?t.pod.pointer("/metadata/name"), container = %t.container, "watcher: outcome not recorded"),
        }
    }

    /// Build and write the RunRecord of one terminated container.
    pub async fn record(&self, t: &Term) -> Result<Option<RunRecord>, String> {
        let pod = &t.pod;
        let ns = pod.pointer("/metadata/namespace").and_then(Value::as_str).ok_or("pod without namespace")?.to_string();
        let inputs = self.view.inputs(&ns).await?;
        if !inputs.enabled() {
            return Ok(None);
        }
        let (top, direct) = self.top_owner(pod, &ns).await;
        let mut settings = self.cfg.base.clone();
        inputs.apply_to(&mut settings);
        let adapter = (self.adapter)();
        let env = AdapterEnv { cluster: adapter.settings().cluster.clone(), settings, namespace: Some(ns.clone()), ..Default::default() };
        let native = Native::Object { value: top.clone() };
        let ex = adapter.extract_all(&native, &env).map_err(|e| e.to_string())?.pop().ok_or("nothing extracted")?;
        let unit = match_unit(&ex.units, pod, &t.container).ok_or_else(|| format!("no unit for container {}", t.container))?.clone();
        let key = unit.workload_key("k8s", &ex.tenant).map_err(|e| e.to_string())?;
        // Join: per-unit decision id, then the object's, then the pod's.
        let (obj_id, per) = crate::runtime::decision_ids(&top);
        let pod_id = string_map(pod, "/metadata/annotations").get(crate::extract::DECISION_ID).cloned();
        let mut decision_id: Option<Ulid> = per.get(&unit.id.0).cloned().or(obj_id).or(pod_id).and_then(|s| s.parse().ok());
        let created = ts(pod.pointer("/metadata/creationTimestamp")).unwrap_or_else(Utc::now);
        if decision_id.is_none() {
            let f = DecisionFilter { key: Some(key.clone()), since: Some(created - chrono::Duration::minutes(10)), limit: Some(16), ..Default::default() };
            decision_id = self.engine.store().decisions(&f).await.ok().and_then(|ds| ds.into_iter().filter(|d| d.unit == unit.id).max_by_key(|d| d.id)).map(|d| d.id);
        }
        // What actually ran.
        let spec = pod.pointer("/spec").cloned().unwrap_or(Value::Null);
        let cdef = ["containers", "initContainers"]
            .iter()
            .flat_map(|l| spec.get(*l).and_then(Value::as_array).cloned().unwrap_or_default())
            .find(|c| c.get("name").and_then(Value::as_str) == Some(t.container.as_str()));
        let requested = cdef.as_ref().and_then(|c| crate::resources::spec_of(c.get("resources"), rightsizer_core::AcceleratorId::UnknownGpu).ok());
        let mem_limit = requested.as_ref().and_then(|r| r.limits.memory).map(|b| b.0 as f64);
        let status = t.status_hint.unwrap_or_else(|| status_of(&t.term, pod, direct.as_ref()));
        let started = ts(t.term.get("startedAt")).or(ts(pod.pointer("/status/startTime"))).unwrap_or(created);
        let finished = ts(t.term.get("finishedAt")).unwrap_or_else(Utc::now);
        let duration_s = ((finished - started).num_milliseconds().max(0) as f64) / 1000.0;
        // Peaks.
        let oom = status == RunStatus::OomKilled;
        let mut rec_mem = None;
        let mut cpu_mean = None;
        let mut cpu_peak = None;
        let mut throttled = None;
        let mut peak_source = "none";
        let node = pod.pointer("/spec/nodeName").and_then(Value::as_str).unwrap_or("");
        let pod_uid = pod.pointer("/metadata/uid").and_then(Value::as_str).unwrap_or("");
        let cid = t.term.get("containerID").and_then(Value::as_str).map(|s| s.rsplit("://").next().unwrap_or(s).to_string());
        if let Some(na) = &self.cfg.node_agent
            && let Some(s) = na.sample(node, pod_uid, &t.container, cid.as_deref()).await
            && s.samples > 0
        {
            let mut m = prom::memory_measurement(s.memory_peak_bytes as f64, mem_limit, oom, s.resolution_s);
            if !s.memory_peak_exact && !oom {
                // Sampled memory.current: a lower bound of the true peak.
                m.censor = Censor::AtLeast;
            }
            rec_mem = Some(m);
            // RunRecord CPU measurements are in cores.
            cpu_mean = Some(Measurement { value: s.cpu_mean_millicores / 1000.0, censor: Censor::Exact, resolution_s: Some(s.resolution_s) });
            cpu_peak = Some(Measurement { value: s.cpu_peak_millicores / 1000.0, censor: Censor::Exact, resolution_s: Some(s.resolution_s) });
            peak_source = "node_agent";
        }
        if rec_mem.is_none()
            && let Some(p) = &self.cfg.prom
        {
            let pod_name = pod.pointer("/metadata/name").and_then(Value::as_str).unwrap_or("");
            match prom::container_peaks(p, &ns, pod_name, &t.container, started, finished, self.cfg.scrape_s).await {
                Ok(pk) => {
                    rec_mem = pk.memory.map(|m| prom::memory_measurement(m, mem_limit, oom, self.cfg.scrape_s as f32));
                    cpu_mean = pk.cpu_mean.map(|c| Measurement { value: c, censor: Censor::Exact, resolution_s: Some(self.cfg.scrape_s as f32) });
                    cpu_peak = pk.cpu_peak_window.map(|c| Measurement { value: c, censor: Censor::Exact, resolution_s: Some(pk.cpu_window_s) });
                    throttled = pk.throttled;
                    if rec_mem.is_some() {
                        peak_source = "prometheus";
                    }
                }
                Err(e) => tracing::debug!(error = %e, "prometheus peaks unavailable"),
            }
        }
        if rec_mem.is_none() && oom {
            rec_mem = mem_limit.map(|l| Measurement { value: l, censor: Censor::AtLeast, resolution_s: None });
            peak_source = "limit";
        }
        // Invocation and attempt.
        let top_ann = string_map(&top, "/metadata/annotations");
        let top_uid = top.pointer("/metadata/uid").and_then(Value::as_str).unwrap_or(pod_uid).to_string();
        let invocation_id = top_ann.get(INVOCATION_ANNOTATION).cloned().unwrap_or(top_uid.clone());
        let attempt = match top_ann.get(ATTEMPT_ANNOTATION).and_then(|a| a.parse::<u32>().ok()) {
            Some(a) => a + t.restart,
            None => self.attempt_of(pod, &top, &ns).await + t.restart,
        };
        let rec = RunRecord {
            v: rightsizer_core::WIRE_VERSION,
            key: key.clone(),
            cluster: adapter.settings().cluster.clone(),
            invocation_id,
            attempt,
            decision_id,
            started_at: started,
            inputs: vec![],
            code_digest: None,
            requested,
            origin_prior: Some(unit.prior.spec.requests.clone()),
            status,
            status_confidence: Confidence::Observed,
            memory_peak: rec_mem,
            cpu_mean,
            cpu_peak_window: cpu_peak,
            cpu_throttled_frac: throttled,
            gpu_memory_peak: None,
            gpu_util_mean: None,
            t_peak_frac: None,
            duration_s: Some(duration_s),
            staged_bytes: None,
            source: OutcomeSource::Watcher,
            trust: Trust::Verified,
        };
        let rep = self.engine.outcome(rec.clone()).await.map_err(|e| e.to_string())?;
        self.metrics.records.with_label_values(&[&format!("{status:?}").to_lowercase(), peak_source]).inc();
        if !rep.tripped.is_empty() {
            tracing::warn!(key = %key, tripped = ?rep.tripped, "watcher: breaker tripped");
        }
        // Warm the memo for the next admission of this workload.
        let state = adapter.state(&ex.tenant, &env).await.unwrap_or_default();
        let policy = Policy { mode: Some(Mode::Recommend), ..Default::default() };
        if let Ok(groups) = crate::runtime::requests(&adapter, &rightsizer_plugin::Extracted { tenant: ex.tenant.clone(), units: vec![unit.clone()], skip: None }, &state, &policy, Some(500), true) {
            let _ = crate::runtime::decide_all(&self.engine, groups).await;
        }
        Ok(Some(rec))
    }

    /// 1-based attempt of a pod of a Job: its rank among the Job's pods by creation.
    async fn attempt_of(&self, pod: &Value, top: &Value, ns: &str) -> u32 {
        let Some(uid) = pod.pointer("/metadata/labels").and_then(|l| l.get("batch.kubernetes.io/controller-uid").or(l.get("controller-uid"))).and_then(Value::as_str) else {
            return 1;
        };
        let _ = top;
        let api: Api<Pod> = Api::namespaced(self.client.clone(), ns);
        let Ok(list) = api.list(&ListParams::default().labels(&format!("batch.kubernetes.io/controller-uid={uid}"))).await else { return 1 };
        let mut pods: Vec<(String, String)> = list
            .items
            .iter()
            .map(|p| (p.metadata.creation_timestamp.as_ref().map(|t| format!("{:020}", t.0.as_millisecond())).unwrap_or_default(), p.metadata.uid.clone().unwrap_or_default()))
            .collect();
        pods.sort();
        let me = pod.pointer("/metadata/uid").and_then(Value::as_str).unwrap_or("");
        pods.iter().position(|(_, u)| u == me).map(|i| i as u32 + 1).unwrap_or(1)
    }
}
