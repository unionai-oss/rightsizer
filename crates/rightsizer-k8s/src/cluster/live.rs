//! Live-object operations with the invoking user's credentials
//! (kubernetes.md §7: the only writes to user objects):
//!
//! - [`restore_live`]: `rsz k8s restore KIND/NAME`: GET, restore patch from
//!   the signed prior, PUT (optimistic concurrency on `resourceVersion`).
//!   An unsuspended `batch/v1` Job's template is immutable: the apiserver
//!   refuses, and the error says so (restore from the manifest instead).
//! - [`apply_watch`]: `rsz apply --watch`: create the sized Job, watch it;
//!   on `OOMKilled` delete it and resubmit a renamed Job one ladder step up
//!   (`ladder::step`, at least the engine's attempt decision), with attempt
//!   records and leases through `Engine::attempt_decide` and outcomes
//!   through `Engine::outcome`.

use std::time::Duration;

use chrono::{DateTime, Utc};
use kube::Api;
use kube::api::{DeleteParams, ListParams, PostParams, PropagationPolicy};
use rightsizer_core::{AttemptContext, Censor, Confidence, Decision, Measurement, Mode, OutcomeSource, ResourceSpec, RunRecord, RunStatus, Trust};
use rightsizer_engine::{ContextInput, DecideRequest, Engine, Policy};
use rightsizer_plugin::{AdapterEnv, Native, Patch, PlatformModel, Unit};
use serde::Serialize;
use serde_json::{Value, json};
use ulid::Ulid;

use super::watcher::{ATTEMPT_ANNOTATION, INVOCATION_ANNOTATION};
use super::{Client, Pod, dynamic, get_json, plural};
use crate::K8sAdapter;
use crate::ladder::{self, JobEvent, LadderAction, LadderState};
use crate::render::{Output, RenderRequest, UnitRender};

/// Apply RFC 6902 ops to a value.
pub fn apply_ops(v: &Value, ops: &[rightsizer_plugin::JsonPatchOp]) -> Result<Value, String> {
    let mut doc = v.clone();
    let p: json_patch::Patch = serde_json::from_value(serde_json::to_value(ops).map_err(|e| e.to_string())?).map_err(|e| e.to_string())?;
    json_patch::patch(&mut doc, &p).map_err(|e| e.to_string())?;
    Ok(doc)
}

/// The restored object (pure part + local apply).
pub fn restored(adapter: &K8sAdapter, object: &Value) -> Result<(Value, crate::restore::Restore), String> {
    let r = adapter.restore(&Native::Object { value: object.clone() }).map_err(|e| e.to_string())?;
    Ok((apply_ops(object, &r.ops)?, r))
}

/// Restore a live object: `api_version`/`kind`/`name` in `ns`.
pub async fn restore_live(client: &Client, adapter: &K8sAdapter, api_version: &str, kind: &str, ns: &str, name: &str) -> Result<Value, String> {
    let obj = get_json(client, api_version, kind, Some(ns), name).await.map_err(|e| e.to_string())?.ok_or_else(|| format!("{kind}/{name} not found in {ns}"))?;
    let (new, r) = restored(adapter, &obj)?;
    if r.ops.is_empty() {
        return Ok(new);
    }
    let api = dynamic(client, api_version, kind, Some(ns));
    let dobj: kube::api::DynamicObject = serde_json::from_value(new.clone()).map_err(|e| e.to_string())?;
    match api.replace(name, &PostParams::default(), &dobj).await {
        Ok(o) => Ok(super::to_json(&o, api_version, kind)),
        Err(kube::Error::Api(e)) if kind == "Job" && e.code == 422 => {
            let m: String = e.message.chars().take(160).collect();
            Err(format!("{kind}/{name}: an unsuspended Job's pod template is immutable, the apiserver refused the change ({m}…): restore the manifest (`rsz k8s restore -f`) and recreate the Job"))
        }
        Err(e) => Err(format!("{kind}/{name}: {e}")),
    }
}

/// `KIND/NAME` → (apiVersion, Kind).
pub fn resolve_kind(s: &str) -> Option<(&'static str, &'static str)> {
    Some(match s.to_lowercase().as_str() {
        "pod" | "pods" | "po" => ("v1", "Pod"),
        "job" | "jobs" => ("batch/v1", "Job"),
        "cronjob" | "cronjobs" | "cj" => ("batch/v1", "CronJob"),
        "jobset" | "jobsets" => ("jobset.x-k8s.io/v1alpha2", "JobSet"),
        "pytorchjob" | "pytorchjobs" => ("kubeflow.org/v1", "PyTorchJob"),
        "trainjob" | "trainjobs" => ("trainer.kubeflow.org/v1alpha1", "TrainJob"),
        "raycluster" | "rayclusters" => ("ray.io/v1", "RayCluster"),
        "rayjob" | "rayjobs" => ("ray.io/v1", "RayJob"),
        "vcjob" | "vcjobs" => ("batch.volcano.sh/v1alpha1", "Job"),
        _ => return None,
    })
}

/// One attempt of `apply --watch`.
#[derive(Clone, Debug, Serialize)]
pub struct AttemptReport {
    /// Attempt number.
    pub attempt: u32,
    /// Job name.
    pub job: String,
    /// Memory request per unit.
    pub memory: Vec<(String, Option<String>)>,
    /// Outcome.
    pub status: RunStatus,
    /// Decision ids.
    pub decisions: Vec<String>,
}

/// The `apply --watch` report.
#[derive(Clone, Debug, Serialize)]
pub struct WatchReport {
    /// Invocation id (all attempts).
    pub invocation_id: String,
    /// Attempts in order.
    pub attempts: Vec<AttemptReport>,
    /// Whether the last attempt succeeded.
    pub succeeded: bool,
    /// Why the ladder stopped.
    pub reason: String,
}

/// Options of `apply --watch`.
#[derive(Clone, Debug)]
pub struct WatchOptions {
    /// Namespace.
    pub namespace: String,
    /// Give up after.
    pub timeout: Duration,
    /// Poll period.
    pub poll: Duration,
}

async fn job_event(client: &Client, ns: &str, name: &str, uid: &str) -> Result<Option<(JobEvent, Option<Value>)>, String> {
    let pods: Api<Pod> = Api::namespaced(client.clone(), ns);
    let list = pods.list(&ListParams::default().labels(&format!("batch.kubernetes.io/controller-uid={uid}"))).await.map_err(|e| e.to_string())?;
    for p in &list.items {
        let v = serde_json::to_value(p).unwrap_or_default();
        for cs in v.pointer("/status/containerStatuses").and_then(Value::as_array).into_iter().flatten() {
            let t = cs.pointer("/state/terminated").or(cs.pointer("/lastState/terminated"));
            if t.and_then(|t| t.get("reason")).and_then(Value::as_str) == Some("OOMKilled") {
                return Ok(Some((JobEvent::OomKilled, Some(v.clone()))));
            }
        }
    }
    let Some(job) = get_json(client, "batch/v1", "Job", Some(ns), name).await.map_err(|e| e.to_string())? else {
        return Ok(Some((JobEvent::Failed { reason: "job deleted".into() }, None)));
    };
    for c in job.pointer("/status/conditions").and_then(Value::as_array).into_iter().flatten() {
        if c.get("status").and_then(Value::as_str) != Some("True") {
            continue;
        }
        let pod = list.items.first().map(|p| serde_json::to_value(p).unwrap_or_default());
        match (c.get("type").and_then(Value::as_str), c.get("reason").and_then(Value::as_str)) {
            (Some("Complete"), _) => return Ok(Some((JobEvent::Succeeded, pod))),
            (Some("Failed"), Some("DeadlineExceeded")) => return Ok(Some((JobEvent::DeadlineExceeded, pod))),
            (Some("Failed"), r) => return Ok(Some((JobEvent::Failed { reason: r.unwrap_or("failed").to_string() }, pod))),
            _ => {}
        }
    }
    Ok(None)
}

fn ts(v: Option<&Value>) -> Option<DateTime<Utc>> {
    v.and_then(Value::as_str).and_then(|s| DateTime::parse_from_rfc3339(s).ok()).map(|d| d.with_timezone(&Utc))
}

/// Create the sized Job, watch it, and climb the ladder on OOM.
#[allow(clippy::too_many_arguments)]
pub async fn apply_watch(client: &Client, engine: &Engine, adapter: &K8sAdapter, env: &AdapterEnv, job: &Value, units: &[Unit], tenant: &rightsizer_core::Tenant, o: &WatchOptions) -> Result<WatchReport, String> {
    let state = adapter.state(tenant, env).await.unwrap_or_default();
    let base = job.pointer("/metadata/name").and_then(Value::as_str).ok_or("the Job needs metadata.name")?.to_string();
    let invocation = Ulid::generate().to_string();
    let policy = Policy { mode: Some(Mode::Enforce), can_classify: Some(true), ..Default::default() };
    let mut ladders: Vec<LadderState> = Vec::new();
    let mut report = WatchReport { invocation_id: invocation.clone(), attempts: vec![], succeeded: false, reason: String::new() };
    let api = dynamic(client, "batch/v1", "Job", Some(&o.namespace));
    let mut name = base.clone();
    let deadline = std::time::Instant::now() + o.timeout;
    for attempt in 1u32.. {
        // Decide each unit for this attempt (attempt records + leases in the engine).
        let mut ds: Vec<(Unit, Decision)> = Vec::new();
        for u in units {
            let key = u.workload_key("k8s", tenant).map_err(|e| e.to_string())?;
            let req = DecideRequest {
                context: ContextInput::Seed { key, unit: u.id.clone(), group: u.group.clone(), seed: u.seed.clone(), prior: u.prior.clone(), platform: adapter.reference(u) },
                constraints: u.constraints.clone(),
                state: state.clone(),
                policy: policy.clone(),
                budget_ms: Some(5000),
                dry_run: false,
            };
            let d = engine.attempt_decide(req, AttemptContext { v: rightsizer_core::WIRE_VERSION, invocation_id: invocation.clone(), attempt, max_attempts: Some(3) }).await;
            ds.push((u.clone(), d));
        }
        if attempt == 1 {
            ladders = ds.iter().map(|(u, d)| LadderState::new(base.clone(), u.prior.spec.requests.clone(), d.applied.requests.clone())).collect();
        }
        // The spec of this attempt: the ladder's step, at least the engine's.
        let renders: Vec<UnitRender> = ds
            .iter()
            .zip(&ladders)
            .map(|((u, d), l)| {
                let mut spec: ResourceSpec = if d.mode == Mode::Enforce { d.applied.clone() } else { u.prior.spec.clone() };
                if attempt > 1 {
                    spec.requests.memory = spec.requests.memory.max(l.current.memory);
                }
                UnitRender { unit: u.id.clone(), spec, defaulted: u.prior.defaulted, decision_id: Some(d.id.to_string()) }
            })
            .collect();
        let mut obj = if attempt == 1 { job.clone() } else { ladder::renamed_job(job, &name) };
        obj.pointer_mut("/metadata").and_then(Value::as_object_mut).map(|m| m.entry("namespace").or_insert(json!(o.namespace)));
        let native = Native::Object { value: obj.clone() };
        let rendered = adapter
            .render_with(&native, &RenderRequest { mode: Mode::Enforce, decision_id: ds.first().map(|(_, d)| d.id.to_string()), units: renders.clone(), annotate: true, output: Output::JsonPatch, namespace: Some(o.namespace.clone()) })
            .map_err(|e| e.to_string())?;
        let mut sized = match &rendered.patch {
            Patch::JsonPatch { ops } => match adapter.validate_at(&native, &rendered.patch, env, Utc::now()) {
                Ok(()) => apply_ops(&obj, ops)?,
                Err(rej) => {
                    tracing::warn!(reason = %crate::validate::prior_reason(&rej), "apply --watch: validation failed; submitting at the prior");
                    obj.clone()
                }
            },
            _ => obj.clone(),
        };
        let ann = sized.pointer_mut("/metadata").and_then(Value::as_object_mut).ok_or("metadata")?;
        let anns = ann.entry("annotations").or_insert_with(|| json!({}));
        anns[INVOCATION_ANNOTATION] = json!(invocation);
        anns[ATTEMPT_ANNOTATION] = json!(attempt.to_string());
        // The pods carry the marker label already (render); be explicit for the watcher.
        let dobj: kube::api::DynamicObject = serde_json::from_value(sized.clone()).map_err(|e| e.to_string())?;
        let created = api.create(&PostParams::default(), &dobj).await.map_err(|e| format!("create Job/{name}: {e}"))?;
        let uid = created.metadata.uid.clone().unwrap_or_default();
        tracing::info!(job = %name, attempt, "apply --watch: created");
        let mem: Vec<(String, Option<String>)> = renders.iter().map(|r| (r.unit.0.clone(), r.spec.requests.memory.map(|b| b.to_string()))).collect();
        // Watch.
        let (ev, pod) = loop {
            if std::time::Instant::now() > deadline {
                return Err(format!("timed out watching Job/{name}"));
            }
            if let Some(x) = job_event(client, &o.namespace, &name, &uid).await? {
                break x;
            }
            tokio::time::sleep(o.poll).await;
        };
        let status = match &ev {
            JobEvent::Succeeded => RunStatus::Succeeded,
            JobEvent::OomKilled => RunStatus::OomKilled,
            JobEvent::DeadlineExceeded => RunStatus::Timeout,
            JobEvent::Failed { .. } => RunStatus::Failed,
        };
        // Outcome per unit (dedups with the watcher's on key/invocation/attempt/source).
        for ((u, d), r) in ds.iter().zip(&renders) {
            let key = u.workload_key("k8s", tenant).map_err(|e| e.to_string())?;
            let cname = u.id.0.rsplit('[').next().unwrap_or("").trim_end_matches(']').to_string();
            let cs = pod.as_ref().and_then(|p| {
                p.pointer("/status/containerStatuses").and_then(Value::as_array).and_then(|l| l.iter().find(|c| c.get("name").and_then(Value::as_str) == Some(cname.as_str())).cloned())
            });
            let term = cs.as_ref().and_then(|c| c.pointer("/state/terminated").or(c.pointer("/lastState/terminated")).cloned());
            let started = ts(term.as_ref().and_then(|t| t.get("startedAt"))).unwrap_or_else(Utc::now);
            let finished = ts(term.as_ref().and_then(|t| t.get("finishedAt"))).unwrap_or_else(Utc::now);
            let limit = r.spec.limits.memory.or(u.prior.spec.limits.memory);
            let rec = RunRecord {
                v: rightsizer_core::WIRE_VERSION,
                key,
                cluster: adapter.settings().cluster.clone(),
                invocation_id: invocation.clone(),
                attempt,
                decision_id: Some(d.id),
                started_at: started,
                inputs: vec![],
                code_digest: None,
                requested: Some(r.spec.clone()),
                origin_prior: Some(u.prior.spec.requests.clone()),
                status,
                status_confidence: Confidence::Observed,
                memory_peak: (status == RunStatus::OomKilled).then(|| limit.map(|l| Measurement { value: l.0 as f64, censor: Censor::AtLeast, resolution_s: None })).flatten(),
                cpu_mean: None,
                cpu_peak_window: None,
                cpu_throttled_frac: None,
                gpu_memory_peak: None,
                gpu_util_mean: None,
                t_peak_frac: None,
                duration_s: Some(((finished - started).num_milliseconds().max(0) as f64) / 1000.0),
                staged_bytes: None,
            source: OutcomeSource::Watcher,
                trust: Trust::Verified,
            };
            if let Err(e) = engine.outcome(rec).await {
                tracing::warn!(error = %e, "apply --watch: outcome not recorded");
            }
        }
        report.attempts.push(AttemptReport { attempt, job: name.clone(), memory: mem, status, decisions: ds.iter().map(|(_, d)| d.id.to_string()).collect() });
        // Next step.
        let mut next: Option<String> = None;
        let mut done_reason = String::new();
        for l in ladders.iter_mut() {
            let (nl, act) = ladder::step(l, &ev);
            *l = nl;
            match act {
                LadderAction::Resubmit { name: n, .. } => next = Some(n),
                LadderAction::Done { succeeded, reason } => {
                    report.succeeded = succeeded;
                    done_reason = reason;
                }
            }
        }
        if ev == JobEvent::OomKilled {
            let _ = api.delete(&name, &DeleteParams { propagation_policy: Some(PropagationPolicy::Background), ..Default::default() }).await;
        }
        match next {
            Some(n) if ev == JobEvent::OomKilled => {
                tracing::info!(next = %n, "apply --watch: OOMKilled; resubmitting one step up");
                name = n;
                report.succeeded = false;
            }
            _ => {
                if report.reason.is_empty() {
                    report.reason = if done_reason.is_empty() { format!("{status:?}") } else { done_reason };
                }
                break;
            }
        }
    }
    let _ = plural("Job");
    Ok(report)
}
