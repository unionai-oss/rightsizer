//! The admission request path (kubernetes.md §3), feature `engine`:
//! `AdmissionReview` in, `AdmissionReview` out, never an error (S1).
//!
//! Per request, within an internal deadline (1 s, below the apiserver's
//! `timeoutSeconds: 2`): cluster inputs from the cached [`ClusterView`];
//! extract; mode (namespace label lowered by the object, then the engine's
//! `mode_ceiling` / breakers); decide with in-process estimators and the
//! memo only (inputs hash-only; metadata the prefetcher already read is
//! filled from the [`InputCache`]); render; validate before patching (S2);
//! reserve the increase in the ledger; record the decisions (not on
//! `dryRun`). Any error, panic or timeout → `allowed: true` with no patch,
//! counted in `rightsizer_webhook_fail_open_total`.

use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use base64::Engine as _;
use chrono::Utc;
use futures::FutureExt as _;
use prometheus::{HistogramOpts, HistogramVec, IntCounterVec, Opts};
use rightsizer_core::{Decision, DecisionSource, InputMeta, InputRef, Mode, PriorReason};
use rightsizer_engine::store::Pending;
use rightsizer_engine::{DecideRequest, Engine, Policy};
use rightsizer_plugin::{AdapterEnv, Native, Patch, PlatformModel, Unit};
use serde_json::{Map, Value, json};

use crate::K8sAdapter;
use crate::render::{Output, RenderRequest};
use crate::runtime;
use crate::view::ClusterView;

/// Webhook metrics (registered in the engine's registry, so `/metrics`
/// serves SPEC §13.1 and these together).
#[derive(Clone)]
pub struct WebhookMetrics {
    /// `rightsizer_webhook_requests_total{webhook, result}`.
    pub requests: IntCounterVec,
    /// `rightsizer_webhook_admission_duration_seconds{webhook}`: the full request path.
    pub duration: HistogramVec,
    /// `rightsizer_webhook_fail_open_total{webhook, reason}`: errors, panics, timeouts.
    pub fail_open: IntCounterVec,
}

impl WebhookMetrics {
    /// Register in `registry` (idempotent per registry: a second call reuses nothing,
    /// so call once per engine).
    pub fn new(registry: &prometheus::Registry) -> Self {
        let requests = IntCounterVec::new(Opts::new("rightsizer_webhook_requests_total", "Admission requests by result."), &["webhook", "result"]).expect("metric");
        let duration = HistogramVec::new(
            HistogramOpts::new("rightsizer_webhook_admission_duration_seconds", "Admission request latency (webhook side).").buckets(vec![
                0.0005, 0.001, 0.002, 0.003, 0.005, 0.0075, 0.01, 0.015, 0.02, 0.03, 0.05, 0.075, 0.1, 0.15, 0.25, 0.5, 1.0, 2.0,
            ]),
            &["webhook"],
        )
        .expect("metric");
        let fail_open = IntCounterVec::new(Opts::new("rightsizer_webhook_fail_open_total", "Requests admitted unchanged because of an error (S1)."), &["webhook", "reason"]).expect("metric");
        let _ = registry.register(Box::new(requests.clone()));
        let _ = registry.register(Box::new(duration.clone()));
        let _ = registry.register(Box::new(fail_open.clone()));
        WebhookMetrics { requests, duration, fail_open }
    }
}

/// Input metadata read by the prefetcher, by URI (kubernetes.md §4.3).
#[derive(Debug, Default)]
pub struct InputCache {
    items: Mutex<HashMap<String, (InputMeta, Instant)>>,
}

impl InputCache {
    const TTL: Duration = Duration::from_secs(3600);
    const CAP: usize = 10_000;

    /// Metadata for `uri`, if read within the TTL.
    pub fn get(&self, uri: &str) -> Option<InputMeta> {
        let m = self.items.lock().ok()?;
        m.get(uri).filter(|(_, at)| at.elapsed() < Self::TTL).map(|(x, _)| x.clone())
    }
    /// Store metadata.
    pub fn put(&self, uri: &str, meta: InputMeta) {
        if let Ok(mut m) = self.items.lock() {
            if m.len() >= Self::CAP {
                m.retain(|_, (_, at)| at.elapsed() < Self::TTL);
                if m.len() >= Self::CAP {
                    m.clear();
                }
            }
            m.insert(uri.to_string(), (meta, Instant::now()));
        }
    }
    /// Fill sizes and formats of refs whose URI was prefetched (no I/O).
    pub fn fill(&self, refs: &mut [InputRef]) -> usize {
        let mut n = 0;
        for r in refs.iter_mut() {
            if let Some(m) = r.uri.as_deref().and_then(|u| self.get(u)) {
                r.bytes = r.bytes.or(m.bytes);
                r.format = r.format.or(Some(m.format));
                n += 1;
            }
        }
        n
    }
    /// Entries.
    pub fn len(&self) -> usize {
        self.items.lock().map(|m| m.len()).unwrap_or(0)
    }
    /// Whether empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Work for the prefetcher: inputs to read after admission and the
/// requests to re-decide (dry-run) once they are read, to warm the memo.
#[derive(Clone, Debug)]
pub struct PrefetchJob {
    /// Namespace (selects `[k8s.inputs.allow]` prefixes).
    pub namespace: String,
    /// Input refs with URIs.
    pub refs: Vec<InputRef>,
    /// Requests to re-decide (dry run) with the metadata filled.
    pub requests: Vec<Vec<DecideRequest>>,
}

/// Admission options.
#[derive(Clone, Debug)]
pub struct AdmitOptions {
    /// Internal deadline for the whole request.
    pub deadline: Duration,
    /// Namespaces never sized.
    pub exclude_namespaces: Vec<String>,
    /// Only these namespaces (empty: all not excluded).
    pub namespaces: Vec<String>,
    /// Require `rightsizer.io/enabled=true` on the namespace (the webhook
    /// configuration's `namespaceSelector` already does; this is a second check).
    pub require_enabled_label: bool,
}

impl Default for AdmitOptions {
    fn default() -> Self {
        AdmitOptions { deadline: Duration::from_millis(1000), exclude_namespaces: vec!["kube-system".into()], namespaces: vec![], require_enabled_label: true }
    }
}

/// What one admission did.
#[derive(Clone, Debug, Default)]
pub struct Admitted {
    /// RFC 6902 ops (empty: none).
    pub patch: Vec<rightsizer_plugin::JsonPatchOp>,
    /// Result label: `patched`, `annotated`, `skipped:<why>`, `rejected:<check>`.
    pub result: String,
    /// The decisions (for tests and logs).
    pub decisions: Vec<Decision>,
    /// Warnings returned to the client.
    pub warnings: Vec<String>,
}

/// The admission handler.
pub struct Admitter {
    engine: Engine,
    adapter: RwLock<Arc<K8sAdapter>>,
    view: Arc<dyn ClusterView>,
    base: Map<String, Value>,
    cluster: Option<String>,
    opts: AdmitOptions,
    metrics: WebhookMetrics,
    inputs: Arc<InputCache>,
    prefetch: Option<tokio::sync::mpsc::Sender<PrefetchJob>>,
    shared_ledger: Option<Arc<crate::ledger_shared::SharedLedger>>,
}

fn skip(why: &str) -> Admitted {
    Admitted { result: format!("skipped:{why}"), ..Default::default() }
}

impl Admitter {
    /// A handler over `engine`, an adapter built from `[k8s]` (settings map
    /// `base`), and a cluster view.
    pub fn new(engine: Engine, adapter: K8sAdapter, base: Map<String, Value>, view: Arc<dyn ClusterView>, opts: AdmitOptions) -> Self {
        let metrics = WebhookMetrics::new(&engine.metrics().registry);
        let cluster = adapter.settings().cluster.clone();
        Admitter {
            engine,
            adapter: RwLock::new(Arc::new(adapter)),
            view,
            base,
            cluster,
            opts,
            metrics,
            inputs: Arc::new(InputCache::default()),
            prefetch: None,
            shared_ledger: None,
        }
    }

    /// Send prefetch work to `tx`.
    pub fn with_prefetch(mut self, tx: tokio::sync::mpsc::Sender<PrefetchJob>, cache: Arc<InputCache>) -> Self {
        self.prefetch = Some(tx);
        self.inputs = cache;
        self
    }

    /// Publish reservations through the decision log for other replicas.
    pub fn with_shared_ledger(mut self, l: Arc<crate::ledger_shared::SharedLedger>) -> Self {
        self.shared_ledger = Some(l);
        self
    }

    /// The input cache.
    pub fn input_cache(&self) -> &Arc<InputCache> {
        &self.inputs
    }

    /// Replace the adapter (signing-key rotation).
    pub fn set_adapter(&self, a: K8sAdapter) {
        if let Ok(mut g) = self.adapter.write() {
            *g = Arc::new(a);
        }
    }

    /// The current adapter.
    pub fn adapter(&self) -> Arc<K8sAdapter> {
        self.adapter.read().map(|g| g.clone()).unwrap_or_else(|p| p.into_inner().clone())
    }

    /// The engine.
    pub fn engine(&self) -> &Engine {
        &self.engine
    }

    /// Metrics.
    pub fn metrics(&self) -> &WebhookMetrics {
        &self.metrics
    }

    /// Handle one `AdmissionReview` for webhook `name` (metrics label).
    /// Always returns a response that admits the object (S1).
    pub async fn review(&self, name: &str, review: &Value) -> Value {
        let t0 = Instant::now();
        let fut = AssertUnwindSafe(self.admit(review, t0)).catch_unwind();
        let out = match tokio::time::timeout(self.opts.deadline, fut).await {
            Ok(Ok(Ok(a))) => a,
            Ok(Ok(Err(e))) => {
                tracing::warn!(error = %e, "admission error: admitted unchanged (S1)");
                self.metrics.fail_open.with_label_values(&[name, "error"]).inc();
                Admitted { result: "error".into(), ..Default::default() }
            }
            Ok(Err(_)) => {
                tracing::error!("admission panicked: admitted unchanged (S1)");
                self.metrics.fail_open.with_label_values(&[name, "panic"]).inc();
                Admitted { result: "error".into(), ..Default::default() }
            }
            Err(_) => {
                tracing::warn!(deadline_ms = self.opts.deadline.as_millis() as u64, "admission deadline: admitted unchanged (S1)");
                self.metrics.fail_open.with_label_values(&[name, "timeout"]).inc();
                Admitted { result: "timeout".into(), ..Default::default() }
            }
        };
        let label = out.result.split(':').next().unwrap_or("other").to_string();
        self.metrics.requests.with_label_values(&[name, &label]).inc();
        self.metrics.duration.with_label_values(&[name]).observe(t0.elapsed().as_secs_f64());
        response(review, &out)
    }

    /// The admission logic (errors become fail-open in [`Admitter::review`]).
    pub async fn admit(&self, review: &Value, t0: Instant) -> Result<Admitted, String> {
        let req = review.get("request").ok_or("AdmissionReview without a request")?;
        let ns = req.get("namespace").and_then(Value::as_str).ok_or("request without a namespace")?.to_string();
        let dry_run = req.get("dryRun").and_then(Value::as_bool).unwrap_or(false);
        if self.opts.exclude_namespaces.iter().any(|n| n == &ns) || (!self.opts.namespaces.is_empty() && !self.opts.namespaces.iter().any(|n| n == &ns)) {
            return Ok(skip("excluded"));
        }
        let inputs = self.view.inputs(&ns).await?;
        if self.opts.require_enabled_label && !inputs.enabled() {
            return Ok(skip("not_enabled"));
        }
        let mut settings = self.base.clone();
        inputs.apply_to(&mut settings);
        settings.insert("dry_run".into(), Value::Bool(dry_run));
        let env = AdapterEnv { cluster: self.cluster.clone(), settings, ..Default::default() };
        let adapter = self.adapter();
        let native = Native::Object { value: review.clone() };
        let mut all = adapter.extract_documents(&native, &env).map_err(|e| e.to_string())?;
        let Some(mut ex) = all.pop() else { return Ok(skip("no_object")) };
        if let Some(s) = &ex.skip {
            let why = serde_json::to_value(s).ok().and_then(|v| v.get("kind").and_then(Value::as_str).map(str::to_string)).unwrap_or_else(|| "skip".into());
            return Ok(skip(&why));
        }
        if ex.units.is_empty() {
            return Ok(skip("no_units"));
        }
        let mode = adapter.mode(&native, &env).map_err(|e| e.to_string())?;
        if mode == Mode::Off {
            return Ok(skip("off"));
        }
        // Inputs are hash-only on the request path; prefetched metadata fills sizes.
        let mut missing: Vec<InputRef> = Vec::new();
        for u in ex.units.iter_mut() {
            self.inputs.fill(&mut u.seed.inputs);
            missing.extend(u.seed.inputs.iter().filter(|r| r.uri.is_some() && r.bytes.is_none()).cloned());
        }
        let state = adapter.state(&ex.tenant, &env).await.unwrap_or_default();
        let policy = Policy { mode: Some(mode), can_classify: Some(true), ..Default::default() };
        let left = self.opts.deadline.saturating_sub(t0.elapsed());
        let budget = (left.as_millis() as u32).saturating_sub(250).clamp(20, 800);
        let groups = runtime::requests(&adapter, &ex, &state, &policy, Some(budget), true)?;
        let prefetch_reqs: Vec<Vec<DecideRequest>> = if missing.is_empty() { vec![] } else { groups.iter().map(|g| g.iter().map(|(_, r)| r.clone()).collect()).collect() };
        let mut decisions = runtime::decide_all(&self.engine, groups).await;
        let mut rmode = runtime::render_mode(&decisions);
        if rmode == Mode::Off {
            return Ok(skip("off"));
        }
        if rmode == Mode::Enforce {
            self.finalize(&adapter, &native, &env, &mut decisions);
        }
        let object_id = decisions.first().map(|(_, d)| d.id.to_string());
        let render = |m: Mode, ds: &[(Unit, Decision)]| {
            adapter.render_with(&native, &RenderRequest { mode: m, decision_id: object_id.clone(), units: runtime::render_units(ds, m), annotate: true, output: Output::JsonPatch, namespace: None })
        };
        let mut warnings = Vec::new();
        let mut result = String::new();
        let mut rendered = match render(rmode, &decisions) {
            Ok(r) => r,
            Err(e) if rmode == Mode::Enforce => {
                // Enforce cannot render (e.g. no signing key): annotation-only (S1).
                demote(&mut decisions, PriorReason::Internal, &format!("render: {e}"));
                rmode = Mode::Recommend;
                result = "error:render".into();
                render(rmode, &decisions).map_err(|e| e.to_string())?
            }
            Err(e) => return Err(e.to_string()),
        };
        // Validate every enforced patch before it is emitted (plugins.md §2),
        // not only when a decision differs from the prior: render may still
        // write resources (S2 limits, Ray params, GPU type terms).
        let changed = rmode == Mode::Enforce;
        if changed && !rendered.patch.is_none() {
            let now = Utc::now();
            match adapter.validate_at(&native, &rendered.patch, &env, now) {
                Err(rej) => {
                    let reason = crate::validate::prior_reason(&rej);
                    tracing::info!(namespace = %ns, %reason, detail = ?rej.detail, "validation failed: annotation-only patch (S2)");
                    demote(&mut decisions, PriorReason::Rejected, &format!("{reason}{}", rej.detail.as_deref().map(|d| format!(": {d}")).unwrap_or_default()));
                    warnings.push(format!("rightsizer: resources unchanged ({reason})"));
                    result = format!("rejected:{}", rej.reason);
                    rendered = render(Mode::Recommend, &decisions).map_err(|e| e.to_string())?;
                }
                Ok(()) => {
                    if !dry_run && let Some(id) = &object_id {
                        let _ = adapter.reserve(&native, &rendered.patch, &env, id, now);
                        // On an enforced decision: other replicas refresh enforce decisions only.
                        if let (Some(sl), Some((_, d))) = (&self.shared_ledger, decisions.iter_mut().find(|(_, d)| d.mode == Mode::Enforce))
                            && let Some(r) = sl.local_reservation(&ns, id)
                        {
                            d.explanation.push(crate::ledger_shared::encode(&r));
                        }
                    }
                    result = "patched".into();
                }
            }
        }
        // S1: a patch the apiserver cannot apply or would reject fails the
        // user's request whatever the failurePolicy; leave such objects alone.
        if let Patch::JsonPatch { ops } = &rendered.patch
            && !annotations_fit(&adapter, &native, ops)
        {
            tracing::warn!(namespace = %ns, "patch would exceed the apiserver's annotation size limit: admitted unchanged");
            if let Some(id) = &object_id {
                adapter.ledger().release(&ns, id);
            }
            return Ok(skip("annotation_size"));
        }
        if result.is_empty() {
            result = if rendered.patch.is_none() { "skipped:unchanged".into() } else { "annotated".into() };
        }
        if !dry_run {
            let engine = self.engine.clone();
            let ds: Vec<Decision> = decisions.iter().map(|(_, d)| d.clone()).collect();
            tokio::spawn(async move {
                for d in ds {
                    if engine.view().put(Pending::Decision(Box::new(d))).await {
                        count_recorded(&engine);
                    }
                }
            });
            if let Some(tx) = &self.prefetch
                && !missing.is_empty()
            {
                let _ = tx.try_send(PrefetchJob { namespace: ns.clone(), refs: missing, requests: prefetch_reqs });
            }
        }
        let patch = match rendered.patch {
            Patch::JsonPatch { ops } => ops,
            _ => vec![],
        };
        Ok(Admitted { patch, result, decisions: decisions.into_iter().map(|(_, d)| d).collect(), warnings })
    }

    /// GKE Autopilot normalization of enforced units (no-op elsewhere).
    fn finalize(&self, adapter: &K8sAdapter, native: &Native, env: &AdapterEnv, decisions: &mut [(Unit, Decision)]) {
        if !env.settings.get("autopilot").is_some_and(|v| !v.is_null()) && adapter.settings().autopilot.is_none() {
            return;
        }
        let pairs: Vec<_> = decisions.iter().filter(|(_, d)| d.mode == Mode::Enforce).map(|(u, d)| (u.id.clone(), d.applied.clone())).collect();
        match adapter.finalize(native, &pairs, env) {
            Ok(fs) => {
                for f in fs {
                    if let Some((_, d)) = decisions.iter_mut().find(|(u, _)| u.id == f.unit) {
                        d.applied = f.spec;
                        d.held.extend(f.holds);
                        d.explanation.extend(f.notes);
                    }
                }
            }
            Err(e) => demote(decisions, PriorReason::Internal, &format!("finalize: {e}")),
        }
    }
}

/// The apiserver's limit on an object's annotations (keys + values), bytes.
pub const ANNOTATION_LIMIT_BYTES: usize = 256 * 1024;

fn annotation_bytes(meta: &Value) -> usize {
    meta.get("annotations").and_then(Value::as_object).map(|m| m.iter().map(|(k, v)| k.len() + v.as_str().map(str::len).unwrap_or(0)).sum()).unwrap_or(0)
}

/// Whether the patched object's annotations (its own and every pod
/// template's) stay within [`ANNOTATION_LIMIT_BYTES`]; false when the patch
/// does not apply.
fn annotations_fit(adapter: &K8sAdapter, native: &Native, ops: &[rightsizer_plugin::JsonPatchOp]) -> bool {
    let Ok(sub) = adapter.parse(native) else { return false };
    let Ok(patched) = crate::render::apply_patch(&sub, &Patch::JsonPatch { ops: ops.to_vec() }) else { return false };
    patched.iter().all(|v| {
        let top = v.get("metadata").map(annotation_bytes).unwrap_or(0) <= ANNOTATION_LIMIT_BYTES;
        let templates = crate::objects::workload(v)
            .map(|w| w.sites.iter().all(|s| v.pointer(&crate::edit::pointer(&s.meta_path)).map(annotation_bytes).unwrap_or(0) <= ANNOTATION_LIMIT_BYTES))
            .unwrap_or(true);
        top && templates
    })
}

/// Count a decision this module wrote to the log (the denominator of
/// `rightsizer_outcome_join_ratio`; the engine counts its own writes).
fn count_recorded(engine: &Engine) {
    let m = engine.metrics();
    m.recorded.with_label_values(&["k8s"]).inc();
    let d = m.recorded.with_label_values(&["k8s"]).get();
    let j = m.joined.with_label_values(&["k8s"]).get();
    if d > 0 {
        m.join_ratio.with_label_values(&["k8s"]).set((j as f64 / d as f64).min(1.0));
    }
}

/// Enforced decisions fall back to their prior with `reason` (the
/// annotation-only patch still records the recommendation).
fn demote(decisions: &mut [(Unit, Decision)], reason: PriorReason, detail: &str) {
    for (u, d) in decisions.iter_mut() {
        if d.mode == Mode::Enforce && runtime::changes(u, d) {
            d.applied = u.prior.spec.clone();
            d.source = DecisionSource::Prior { reason, detail: Some(detail.to_string()) };
            d.explanation.push(format!("kept the prior: {detail}"));
        }
    }
}

/// The `AdmissionReview` response for `out` (allowed, with the patch if any).
pub fn response(review: &Value, out: &Admitted) -> Value {
    let uid = review.pointer("/request/uid").cloned().unwrap_or(Value::String(String::new()));
    let api = review.get("apiVersion").cloned().unwrap_or_else(|| json!("admission.k8s.io/v1"));
    let mut r = json!({"uid": uid, "allowed": true});
    if !out.patch.is_empty()
        && let Ok(bytes) = serde_json::to_vec(&out.patch)
    {
        r["patchType"] = json!("JSONPatch");
        r["patch"] = json!(base64::engine::general_purpose::STANDARD.encode(bytes));
    }
    if !out.warnings.is_empty() {
        r["warnings"] = json!(out.warnings);
    }
    json!({"apiVersion": api, "kind": "AdmissionReview", "response": r})
}

/// The reservation ledger behind `adapter` (for tests).
pub fn reserved(adapter: &K8sAdapter, ns: &str) -> (crate::resources::Totals, crate::resources::Totals) {
    adapter.ledger().reserved(ns, Utc::now())
}

#[allow(dead_code)]
fn assert_send(a: Arc<Admitter>, v: Value) {
    fn is_send<T: Send>(_: T) {}
    is_send(async move { a.review("x", &v).await });
}
