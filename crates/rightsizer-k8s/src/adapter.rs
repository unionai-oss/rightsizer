//! The pure K8s adapter (kubernetes.md §2–§5): [`Adapter`] and
//! [`PlatformModel`] for kind `"k8s"` over `Native::Document` (YAML/JSON
//! manifests, multi-document) and `Native::Object` (admission requests).
//!
//! The trait methods are thin wrappers over richer inherent methods the
//! webhook and CLI use directly: [`K8sAdapter::extract_documents`],
//! [`K8sAdapter::render_with`], [`K8sAdapter::validate_at`],
//! [`K8sAdapter::finalize`], [`K8sAdapter::reserve`], [`K8sAdapter::restore`]
//! and [`K8sAdapter::mode`].

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rightsizer_core::{
    AcceleratorId, Axis, Bytes, GpuShape, Hold, HoldReason, Ladder, MilliCpu, Mode, NodeShape, PlatformRef, PlatformRetry, PlatformState,
    Quota, ResourceSpec, ScalarValue, Tenant, UnitId,
};
use rightsizer_plugin::{AcceleratorMap, Adapter, AdapterEnv, Extracted, Native, Patch, PlatformModel, PluginError, Rejection, Result, SkipReason, Unit};
use serde_json::Value;

use crate::autopilot;
use crate::edit::pointer;
use crate::extract::{self, ExtractCtx};
use crate::gpu;
use crate::ledger::{MemoryLedger, Reservation, ReservationLedger};
use crate::limitrange::LimitRanges;
use crate::mode;
use crate::objects::{self, Admission, Submission};
use crate::prior::SigningKeys;
use crate::render::{self, RenderCtx, RenderRequest, Rendered};
use crate::restore::{self, Restore};
use crate::settings::K8sSettings;
use crate::validate::{self, ValidateCtx};

/// The Kubernetes adapter.
#[derive(Clone)]
pub struct K8sAdapter {
    settings: K8sSettings,
    keys: Option<SigningKeys>,
    ledger: Arc<dyn ReservationLedger>,
    accel: AcceleratorMap,
}

impl std::fmt::Debug for K8sAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("K8sAdapter").field("cluster", &self.settings.cluster).field("keys", &self.keys).finish()
    }
}

impl Default for K8sAdapter {
    fn default() -> Self {
        K8sAdapter::new(K8sSettings::default())
    }
}

/// A unit's decision after platform normalization (GKE Autopilot).
#[derive(Clone, Debug, PartialEq)]
pub struct Finalized {
    /// Unit.
    pub unit: UnitId,
    /// What will actually run.
    pub spec: ResourceSpec,
    /// Axes held at the prior because the platform would undo the change.
    pub holds: Vec<Hold>,
    /// Notes (rules that changed something).
    pub notes: Vec<String>,
}

impl K8sAdapter {
    /// An adapter with static settings (the `[k8s]` config); per-request
    /// settings in [`AdapterEnv::settings`] are layered over them.
    pub fn new(settings: K8sSettings) -> Self {
        let accel = gpu::accelerator_map(&settings.accelerators);
        K8sAdapter { settings, keys: None, ledger: Arc::new(MemoryLedger::default()), accel }
    }

    /// With HMAC keys for the signed prior (required to enforce).
    pub fn with_keys(mut self, keys: SigningKeys) -> Self {
        self.keys = Some(keys);
        self
    }

    /// With a reservation ledger (default: in-process, 2 minutes).
    pub fn with_ledger(mut self, ledger: Arc<dyn ReservationLedger>) -> Self {
        self.ledger = ledger;
        self
    }

    /// Static settings.
    pub fn settings(&self) -> &K8sSettings {
        &self.settings
    }

    /// The ledger.
    pub fn ledger(&self) -> &Arc<dyn ReservationLedger> {
        &self.ledger
    }

    /// Effective settings: static settings overlaid with `env.settings` and `env.cluster`.
    pub fn settings_for(&self, env: &AdapterEnv) -> Result<K8sSettings> {
        let Value::Object(mut base) = serde_json::to_value(&self.settings).map_err(|e| PluginError::Internal(e.to_string()))? else {
            return Err(PluginError::Internal("settings".into()));
        };
        for (k, v) in &env.settings {
            base.insert(k.clone(), v.clone());
        }
        let merged = AdapterEnv { settings: base, ..env.clone() };
        K8sSettings::from_env(&merged)
    }

    fn accel_for(&self, s: &K8sSettings) -> AcceleratorMap {
        if s.accelerators == self.settings.accelerators { self.accel.clone() } else { gpu::accelerator_map(&s.accelerators) }
    }

    /// Parse a native submission.
    pub fn parse(&self, native: &Native) -> Result<Submission> {
        objects::parse_native(native)
    }

    /// The admission request around a `Native::Object`, if any.
    pub fn admission(&self, native: &Native) -> Option<Admission> {
        objects::parse_native(native).ok().and_then(|s| s.admission)
    }

    /// The trusted namespace: the admission request's; otherwise
    /// `env.namespace`; for documents, the object's `metadata.namespace`; else `default`.
    pub fn namespace_of(sub: &Submission, obj: &Value, env: &AdapterEnv) -> Result<String> {
        if let Some(a) = &sub.admission {
            return a.namespace.clone().ok_or_else(|| PluginError::InvalidNative("admission request without a namespace".into()));
        }
        if let Some(n) = &env.namespace {
            return Ok(n.clone());
        }
        Ok(obj.pointer("/metadata/namespace").and_then(Value::as_str).unwrap_or("default").to_string())
    }

    /// Extract every object of a submission separately (documents may span namespaces).
    pub fn extract_documents(&self, native: &Native, env: &AdapterEnv) -> Result<Vec<Extracted>> {
        self.extract_opts(native, env, false)
    }

    /// Extract including units that are already sized (watcher, restore, `rsz explain`).
    pub fn extract_all(&self, native: &Native, env: &AdapterEnv) -> Result<Vec<Extracted>> {
        self.extract_opts(native, env, true)
    }

    fn extract_opts(&self, native: &Native, env: &AdapterEnv, include_sized: bool) -> Result<Vec<Extracted>> {
        let sub = objects::parse_native(native)?;
        let settings = self.settings_for(env)?;
        let accel = self.accel_for(&settings);
        let mut out = Vec::new();
        for o in &sub.objects {
            let ns = Self::namespace_of(&sub, &o.value, env)?;
            if let Some(op) = sub.admission.as_ref().and_then(|a| a.operation.as_deref())
                && op != "CREATE"
            {
                let cluster = settings.cluster.clone().unwrap_or_default();
                let t = crate::keys::tenant(&cluster, &ns).ok_or_else(|| PluginError::InvalidNative("invalid tenant".into()))?;
                out.push(Extracted { tenant: t, units: vec![], skip: Some(SkipReason::Unsupported { detail: format!("operation {op}") }) });
                continue;
            }
            let cx = ExtractCtx { settings: &settings, accel: &accel, keys: self.keys.as_ref(), namespace: &ns, admission: sub.admission.is_some(), multi: sub.multi(), include_sized };
            out.push(extract::extract_object(&o.value, o.doc_index, &cx)?);
        }
        Ok(out)
    }

    /// The requested mode for an admission object: the namespace label (from
    /// `[k8s] namespace_labels` in the env), lowered by the object's label or annotation.
    pub fn mode(&self, native: &Native, env: &AdapterEnv) -> Result<Mode> {
        let sub = objects::parse_native(native)?;
        let settings = self.settings_for(env)?;
        let obj = sub.objects.first().map(|o| o.value.clone()).unwrap_or(Value::Null);
        Ok(mode::effective_mode(&settings.namespace_labels, &obj))
    }

    /// Render with full control (mode, decision id, output form).
    pub fn render_with(&self, native: &Native, req: &RenderRequest) -> Result<Rendered> {
        let sub = objects::parse_native(native)?;
        let cx = RenderCtx { accel: &self.accel, keys: self.keys.as_ref(), settings: &self.settings };
        render::render(&sub, req, &cx)
    }

    /// Validate at a given time (ledger expiry).
    pub fn validate_at(&self, native: &Native, patch: &Patch, env: &AdapterEnv, now: DateTime<Utc>) -> Result<(), Rejection> {
        let sub = objects::parse_native(native).map_err(|e| Rejection { reason: validate::check::POD_SPEC.into(), detail: Some(e.to_string()), field: None })?;
        let settings = self.settings_for(env).map_err(|e| Rejection { reason: "config".into(), detail: Some(e.to_string()), field: None })?;
        let obj = sub.objects.first().map(|o| o.value.clone()).unwrap_or(Value::Null);
        let ns = Self::namespace_of(&sub, &obj, env).map_err(|e| Rejection { reason: validate::check::POD_SPEC.into(), detail: Some(e.to_string()), field: None })?;
        let cx = ValidateCtx { settings: &settings, ledger: self.ledger.as_ref(), namespace: &ns, now };
        validate::validate(&sub, patch, &cx)
    }

    /// Record the increase an emitted patch admits (kubernetes.md §3.4 ledger).
    pub fn reserve(&self, native: &Native, patch: &Patch, env: &AdapterEnv, id: &str, now: DateTime<Utc>) -> Result<()> {
        let sub = objects::parse_native(native)?;
        let settings = self.settings_for(env)?;
        let obj = sub.objects.first().map(|o| o.value.clone()).unwrap_or(Value::Null);
        let ns = Self::namespace_of(&sub, &obj, env)?;
        if let Some((r, l)) = validate::increase(&sub, patch, &settings, &ns)
            && (r.cpu > 0 || r.memory > 0 || l.cpu > 0 || l.memory > 0)
        {
            self.ledger.reserve(Reservation { namespace: ns, id: id.to_string(), requests: r, limits: l, at: now });
        }
        Ok(())
    }

    /// Normalize candidate decisions by the platform's rules before they are
    /// final (GKE Autopilot): what Autopilot would raise is raised; a
    /// reduction Autopilot would undo is held at the prior; an increase
    /// above Autopilot's maximum is held.
    pub fn finalize(&self, native: &Native, decisions: &[(UnitId, ResourceSpec)], env: &AdapterEnv) -> Result<Vec<Finalized>> {
        let settings = self.settings_for(env)?;
        let sub = objects::parse_native(native)?;
        let cx = RenderCtx { accel: &self.accel, keys: None, settings: &settings };
        let req = RenderRequest { annotate: false, ..RenderRequest::enforce(decisions) };
        let rendered = render::render(&sub, &req, &cx)?;
        let mut out: Vec<Finalized> = decisions
            .iter()
            .map(|(u, s)| Finalized { unit: u.clone(), spec: rendered.applied.get(u).cloned().unwrap_or_else(|| s.clone()), holds: vec![], notes: vec![] })
            .collect();
        let Some(ap) = &settings.autopilot else { return Ok(out) };
        // Autopilot normalization works on one object (admission requests).
        if sub.objects.len() != 1 {
            return Ok(out);
        }
        let single = Submission { objects: sub.objects.clone(), admission: sub.admission.clone(), docs: None };
        let rendered = render::render(&single, &RenderRequest { output: render::Output::JsonPatch, ..req.clone() }, &cx)?;
        let patched = render::apply_patch(&single, &rendered.patch)?;
        let v0 = &single.objects[0].value;
        let v1 = &patched[0];
        let Some(w) = objects::workload(v1) else { return Ok(out) };
        let ns = Self::namespace_of(&single, v0, env)?;
        let lr = LimitRanges::parse(&settings.limit_ranges, Some(&ns));
        let locs = extract::locate(v1, &w);
        for f in out.iter_mut() {
            let (_, local) = extract::split_unit_id(&f.unit.0, single.multi());
            let Some(loc) = locs.iter().find(|l| l.local_id == local) else { continue };
            let (Some(si), extract::Target::Container(c)) = (loc.site, &loc.target) else { continue };
            let Some(spec1) = v1.pointer(&pointer(&w.sites[si].spec_path)) else { continue };
            let spec1 = if w.kind.is_template() { crate::limitrange::default_pod_spec(spec1, &lr).0 } else { spec1.clone() };
            let prior = extract::declared_spec(v0, &w, loc, &self.accel).unwrap_or_default();
            let prior_cpu = prior.requests.cpu.or(prior.limits.cpu);
            let prior_mem = prior.requests.memory.or(prior.limits.memory);
            match autopilot::normalize_pod_spec(&spec1, ap) {
                Err(e) => {
                    for (axis, p, n) in [(Axis::Cpu, prior_cpu.map(|c| c.0 as u64), f.spec.requests.cpu.map(|c| c.0 as u64)), (Axis::Memory, prior_mem.map(|b| b.0), f.spec.requests.memory.map(|b| b.0))] {
                        if n > p {
                            f.holds.push(Hold::with(axis, HoldReason::Platform, format!("autopilot: {}", e.detail)));
                            match axis {
                                Axis::Cpu => f.spec.requests.cpu = prior_cpu,
                                _ => f.spec.requests.memory = prior_mem,
                            }
                        }
                    }
                    f.notes.push(e.detail);
                }
                Ok(n) => {
                    let res = n.spec.get(c.list).and_then(|l| l.get(c.index)).and_then(|x| x.get("resources"));
                    let ns_spec = crate::resources::spec_of(res, AcceleratorId::UnknownGpu).unwrap_or_default();
                    let ncpu = ns_spec.requests.cpu.or(ns_spec.limits.cpu);
                    let nmem = ns_spec.requests.memory.or(ns_spec.limits.memory);
                    let cand_cpu = f.spec.requests.cpu;
                    let cand_mem = f.spec.requests.memory;
                    if ncpu != cand_cpu {
                        if cand_cpu < prior_cpu && ncpu >= prior_cpu {
                            f.holds.push(Hold::with(Axis::Cpu, HoldReason::Platform, "autopilot would undo the reduction"));
                            f.spec.requests.cpu = prior_cpu;
                        } else {
                            f.spec.requests.cpu = ncpu;
                        }
                    }
                    if nmem != cand_mem {
                        if cand_mem < prior_mem && nmem >= prior_mem {
                            f.holds.push(Hold::with(Axis::Memory, HoldReason::Platform, "autopilot would undo the reduction"));
                            f.spec.requests.memory = prior_mem;
                        } else {
                            f.spec.requests.memory = nmem;
                        }
                    }
                    if !ap.bursting {
                        f.spec.limits = ns_spec.limits.clone();
                        f.spec.limits.ephemeral_storage = None;
                    }
                    f.notes.extend(n.notes);
                }
            }
        }
        Ok(out)
    }

    /// The restore patch for an object with a valid signed prior.
    pub fn restore(&self, native: &Native) -> Result<Restore> {
        let keys = self.keys.as_ref().ok_or_else(|| PluginError::Internal("restore needs the signing keys".into()))?;
        let sub = objects::parse_native(native)?;
        let o = sub.objects.first().ok_or_else(|| PluginError::InvalidNative("empty submission".into()))?;
        restore::restore(&o.value, keys)
    }

    fn platform_ref(&self, unit: &Unit) -> PlatformRef {
        let x = &unit.seed.manifest.extra;
        let int = |k: &str| match x.get(k) {
            Some(ScalarValue::Int(i)) if *i >= 0 => Some(*i as u64),
            _ => None,
        };
        let flag = |k: &str| matches!(x.get(k), Some(ScalarValue::Bool(true)));
        let mut r = PlatformRef::new("k8s");
        r.retry = PlatformRetry { same_size: int("backoff_limit").unwrap_or(0).min(u32::MAX as u64) as u32, ladder: None };
        if flag("apply_watch") {
            let mut l = Ladder::caller();
            l.final_floor = rightsizer_core::Requests { memory: unit.prior.spec.requests.memory, ..Default::default() };
            r.retry.ladder = Some(l);
        }
        r.walltime_s = int("active_deadline_seconds");
        r.memory_enforced = match x.get("memory_limit") {
            Some(ScalarValue::Bool(b)) => *b,
            _ => unit.prior.spec.limits.memory.is_some(),
        };
        r.cpu_window_s = self.settings.cpu_window_s;
        if let Some(ScalarValue::Str(img)) = x.get("image") {
            r.overhead = self.settings.overhead_for(img);
        }
        r
    }
}

#[async_trait]
impl Adapter for K8sAdapter {
    fn kind(&self) -> &str {
        "k8s"
    }

    /// Extract a submission. Documents whose objects span namespaces need
    /// [`K8sAdapter::extract_documents`].
    async fn extract(&self, native: &Native, env: &AdapterEnv) -> Result<Extracted> {
        let mut all = self.extract_documents(native, env)?;
        if all.len() == 1 {
            return Ok(all.remove(0));
        }
        let tenant = all.first().map(|e| e.tenant.clone()).ok_or_else(|| PluginError::InvalidNative("no objects".into()))?;
        if all.iter().any(|e| e.tenant != tenant) {
            return Err(PluginError::Unsupported("objects in several namespaces: use extract_documents".into()));
        }
        let mut units = Vec::new();
        let mut skips = Vec::new();
        for e in all {
            units.extend(e.units);
            if let Some(s) = e.skip {
                skips.push(s);
            }
        }
        let skip = if units.is_empty() { skips.into_iter().next().or(Some(SkipReason::NoUnits)) } else { None };
        Ok(Extracted { tenant, units, skip })
    }

    async fn render(&self, native: &Native, decisions: &[(UnitId, ResourceSpec)]) -> Result<Patch> {
        Ok(self.render_with(native, &RenderRequest::enforce(decisions))?.patch)
    }

    async fn validate(&self, native: &Native, patch: &Patch, env: &AdapterEnv) -> Result<(), Rejection> {
        self.validate_at(native, patch, env, Utc::now())
    }
}

/// Node objects → shapes (allocatable, GPU labels, price by instance type).
pub fn node_shapes(settings: &K8sSettings, accel: &AcceleratorMap) -> Vec<NodeShape> {
    let mut out: Vec<NodeShape> = settings.shapes.clone();
    for n in &settings.nodes {
        let a = n.pointer("/status/allocatable");
        let Some(cpu) = a.and_then(|a| a.get("cpu")).and_then(|v| crate::resources::cpu_of(v).ok()) else { continue };
        let Some(mem) = a.and_then(|a| a.get("memory")).and_then(|v| crate::resources::bytes_of(v).ok()) else { continue };
        let count: u32 = crate::resources::GPU_RESOURCES.iter().filter_map(|g| a.and_then(|a| a.get(*g)).and_then(|v| crate::resources::count_of(v).ok())).sum();
        let labels = objects::string_map(n, "/metadata/labels");
        let gpu = (count > 0).then(|| {
            let id = gpu::GPU_LABELS.iter().find_map(|l| labels.get(*l)).map(|v| accel.canonical(v)).unwrap_or(AcceleratorId::UnknownGpu);
            GpuShape { accel: id, count }
        });
        let itype = labels.get("node.kubernetes.io/instance-type").cloned();
        let price = itype.as_ref().and_then(|t| settings.prices.get(t).copied()).unwrap_or_else(|| fallback_price(cpu, mem, gpu));
        let shape = NodeShape { name: itype, cpu, memory: mem, gpu, price_per_hour: price, host_per_gpu: None };
        if !out.iter().any(|s| s.cpu == shape.cpu && s.memory == shape.memory && s.gpu == shape.gpu && s.name == shape.name) {
            out.push(shape);
        }
    }
    out
}

/// A rough on-demand price when none is configured: m6i-like CPU and memory
/// rates plus the core's placeholder GPU prices.
pub fn fallback_price(cpu: MilliCpu, mem: Bytes, gpu: Option<GpuShape>) -> f64 {
    let base = 0.0316 * cpu.as_cores() + 0.0042 * mem.as_gib();
    base + gpu.map(|g| g.count as f64 * rightsizer_core::cost::default_gpu_price(g.accel)).unwrap_or(0.0)
}

#[async_trait]
impl PlatformModel for K8sAdapter {
    fn reference(&self, unit: &Unit) -> PlatformRef {
        self.platform_ref(unit)
    }

    async fn state(&self, tenant: &Tenant, env: &AdapterEnv) -> Result<PlatformState> {
        let settings = self.settings_for(env)?;
        let accel = self.accel_for(&settings);
        let ns = tenant.as_str().split_once('/').map(|(_, n)| n.to_string()).unwrap_or_default();
        let mut state = PlatformState { shapes: node_shapes(&settings, &accel), ..Default::default() };
        if !settings.resource_quotas.is_empty() {
            let rem = validate::quota_remaining(&settings.resource_quotas, &ns);
            let (rr, _) = self.ledger.reserved(&ns, Utc::now());
            let left = |k: &str, res: u64| rem.get(k).map(|r| (*r - res as i128).max(0) as u64);
            let gpu = crate::resources::GPU_RESOURCES.iter().filter_map(|g| left(&format!("requests.{g}"), rr.gpu.get(*g).copied().unwrap_or(0))).min();
            state.quota = Some(Quota {
                cpu: left("requests.cpu", rr.cpu).map(|c| MilliCpu(c.min(u32::MAX as u64) as u32)),
                memory: left("requests.memory", rr.memory).map(Bytes),
                gpu_count: gpu.map(|g| g.min(u32::MAX as u64) as u32),
                ephemeral_storage: left("requests.ephemeral-storage", rr.ephemeral).map(Bytes),
            });
        }
        Ok(state)
    }

    fn accelerators(&self) -> &AcceleratorMap {
        &self.accel
    }
}

