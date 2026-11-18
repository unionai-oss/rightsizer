# Platform plugin interface

Status: **draft v2**. Core concepts: [SPEC.md](SPEC.md).

A **platform plugin** teaches rightsizer one platform: how to find the
resource-bearing units in a native submission, what they currently ask for,
what the platform's constraints and semantics are, how to write a decision
back, and where outcomes come from. Everything else — features, estimators,
the decision rule, constraints, breakers, history — is the core's.

Built-in plugins: **Kubernetes** ([kubernetes.md](kubernetes.md)), **Flyte**
([flyte.md](flyte.md), Python-side), **Slurm** ([slurm.md](slurm.md)) and
**Ray** ([ray.md](ray.md)).

## 1. Shape of an integration

```
native submission ──extract──► Units ──core: features, history, estimate, decide──► Decisions ──render──► Patch ──► platform
                                                                                                         │
platform outcomes ──OutcomeSource──► RunRecords ──► HistoryStore ◄───────────────────────────────────────┘ (decision log)
```

A plugin implements three things, **none of which touch estimators or the
decision rule**:

1. **`Adapter`**: native submission ⇄ units and patches.
2. **`PlatformModel`**: static platform facts (`PlatformRef`, part of the
   context) and live state (`PlatformState`: shapes, prices, quota — given to
   the decision rule, never put in the context).
3. **`OutcomeSource`**: where run records come from (optional; some
   platforms only get self-reports).

## 2. In-process interface (Rust, `rightsizer-plugin`)

```rust
/// A native submission, as the plugin received it.
pub enum Native {
    Document { media_type: String, bytes: Vec<u8> },     // YAML/JSON manifests, sbatch scripts
    Object(serde_json::Value),                            // admission request objects, Slurm option tables, Ray option dicts
}

pub struct Unit {
    pub id: UnitId,                                       // stable within the submission, e.g. "spec.template.spec.containers[trainer]"
    pub group: Option<GroupId>,                           // units sized identically (replicas of one replica type, array tasks)
    pub key_suffix: String,                               // user-overridable part of the workload key
    pub prior: Prior,
    pub seed: ContextSeed,                                // input refs (URIs/ObjectRef sizes), code ref, manifest features, labels
    pub constraints: Constraints,                         // SPEC §7.4: pins, platform holds, floor, ceiling, allowed shapes
    pub distributed: bool,                                // SPEC §5.4: GPU count held
}
pub struct ContextSeed {
    pub inputs: Vec<InputRef>,                            // the core reads metadata within its budget
    pub code: Option<CodeRef>,                            // source text, or a reference the core may resolve
    pub manifest: ManifestFeatures,
    pub labels: BTreeMap<String, String>,
}

pub struct Extracted {
    pub tenant: Tenant,                                   // derived by the plugin from the trusted request, never from user fields
    pub units: Vec<Unit>,
    pub skip: Option<SkipReason>,                         // e.g. "already sized (decision-id present)", "queue-managed pod"
}

#[async_trait]
pub trait Adapter: Send + Sync {
    fn kind(&self) -> &str;                               // "k8s", "slurm", "ray", ...
    async fn extract(&self, native: &Native, env: &AdapterEnv) -> Result<Extracted>;
    async fn render(&self, native: &Native, decisions: &[(UnitId, ResourceSpec)]) -> Result<Patch>;
    async fn validate(&self, native: &Native, patch: &Patch, env: &AdapterEnv) -> Result<(), Rejection>;
}

pub enum Patch {
    None,                                                  // nothing to change
    JsonPatch(Vec<JsonPatchOp>),                           // K8s admission responses
    TextEdits(Vec<TextEdit>),                              // byte-range edits: comments and formatting preserved (YAML, sbatch)
    Options(serde_json::Map<String, Value>),               // option tables: Slurm cli_filter/job_submit, Ray .options(), Flyte Resources
}

#[async_trait]
pub trait PlatformModel: Send + Sync {
    fn reference(&self, unit: &Unit) -> PlatformRef;      // SPEC §5.2: static, part of the context digest
    async fn state(&self, tenant: &Tenant, env: &AdapterEnv) -> Result<PlatformState>;
    fn accelerators(&self) -> &AcceleratorMap;            // canonical ⇄ native names (SPEC §5.1.1)
}
pub struct PlatformState {                                 // live; given to the decision rule only
    pub shapes: Vec<NodeShape>,                            // { cpu, memory, gpu: Option<GpuShape>, price_per_hour, host_per_gpu }
    pub price: PriceModel,                                 // Packed | Linear { cpu_core_hr, mem_gib_hr, gpu_hr: map }
    pub quota: Option<Quota>,                              // remaining, minus the adapter's reservation ledger
    pub floors: Requests,                                  // platform minimums
}

#[async_trait]
pub trait OutcomeSource: Send + Sync {
    async fn fetch(&self, since: DateTime<Utc>, keys: Option<&[WorkloadKey]>)
        -> Result<BoxStream<'_, Result<RunRecord>>>;        // classification and censoring happen inside the source
}
```

Rules every plugin follows:

- **Tenant is trusted, suffix is not.** `Extracted.tenant` comes from data the
  platform authenticated (admission `userInfo`/namespace, Slurm `submit_uid`,
  Flyte action id). User-supplied keys only set `Unit.key_suffix`.
- **Groups are sized once.** The core makes one decision per group from the
  union of the group's history (demand = max across members), and every
  member gets it. Adapters size groups where the whole group is visible at once
  (a job template, a job array, a het job); none re-decides a group per member.
- **`validate` before a patch is emitted.** The plugin checks the patch
  against everything the platform will check after it (S2: no patch the
  platform will reject), including group totals (e.g. the effective pod
  request against the largest node shape). A `Rejection` makes the core drop
  the patch and record `Prior{reason}`.
- **`render` is minimal.** Only the resource fields (and the resource-adjacent
  fields the plugin documents, e.g. a node selector for a GPU type) change.

## 3. Out-of-process plugins

For platforms whose hook point is in another language (Lua in Slurm,
Go for Nomad or Armada, Python for Ray or Flyte), rightsizer exposes **one**
protocol, served two ways.

### 3.1 Protocol: `rightsizer-plugin/1`

JSON-RPC 2.0, newline-delimited JSON over **stdio** (rightsizer spawns the
plugin) or over a **Unix socket / HTTP** (the daemon serves it). Schemas:
`schemas/plugin/1/`.

Two directions:

| direction | who calls | methods |
|---|---|---|
| **core → plugin** (a plugin *is* an adapter) | `rightsize` spawns `rightsize-plugin-<kind>` | `initialize`, `extract`, `render`, `validate`, `reference`, `state`, `fetch_outcomes`, `shutdown` |
| **shim → core** (a shim *uses* rightsizer) | a Lua/Go/Python shim calls the daemon | `initialize`, `decide`, `outcome`, `status` |

`decide` is the composite most shims need: `{ kind, native, mode }` →
`{ decisions: [Decision], patch: Patch, mode, summary, token? }`. `mode` is the
effective mode, `summary` is a one-line human explanation, and `token` is the
signed attribution token (slurm.md §4.1), present only for Slurm batch
submissions. The daemon runs the named adapter
(built-in or plugin), the estimators and the decision rule.

### 3.2 Handshake

```json
→ {"jsonrpc":"2.0","id":1,"method":"initialize",
   "params":{"protocol":"rightsizer-plugin/1","schemas":{"context":"1","decision":"1","run":"1"},"features":"features-v1"}}
← {"jsonrpc":"2.0","id":1,"result":{"protocol":"rightsizer-plugin/1","kind":"nomad","version":"0.3.0",
   "methods":["extract","render","validate","reference","state"],"native_media_types":["application/hcl","application/json"]}}
```

Compatibility follows SPEC §13.2: the core speaks the current and previous
major protocol versions; on no overlap the plugin is not used and submissions
pass through unchanged (S1).

### 3.3 Lifetime, deadlines, errors

- Plugins are long-lived: spawned once, reused, restarted on crash with
  backoff. One request at a time per stdio pipe unless the plugin advertises
  `"concurrency": n`.
- Every request carries `"deadline_ms"`; the core cancels (`$/cancel`) and
  falls back to the prior when it expires.
- Errors use JSON-RPC error objects with `data: {code, retryable}`;
  `code ∈ {parse_error, unsupported, invalid_native, platform_unavailable, internal}`.

### 3.4 Discovery and trust

- **Interactive CLI:** plugins are found as `rightsize-plugin-<kind>` on `PATH`
  (like `kubectl` and `cargo` plugins) **and** in `[plugins]` config.
- **Server modes** (`rightsize serve`, `rightsize webhook serve`): **only**
  plugins listed in config with an **absolute path**, optionally pinned by
  `sha256`. No `PATH` discovery.

```toml
[plugins.nomad]
path = "/opt/rightsizer/plugins/rightsize-plugin-nomad"
sha256 = "9f2c…"
```

- The daemon's Unix socket authenticates callers with `SO_PEERCRED`; a caller
  may only request decisions and report outcomes for tenants it maps to
  (uid → Slurm accounts / namespaces, configured).

### 3.5 Python plugins

`rightsizer.plugin.Adapter` mirrors the Rust traits (`extract`, `render`,
`validate`, `reference`, `state`); a Python adapter registered with
`rs.register_adapter(...)` runs in-process. Python-only platforms (Ray,
Flyte) render `Patch.Options` and never touch bytes.

## 4. Writing a plugin: checklist

1. **Units and groups.** Which objects carry resources? Which must be equal
   (replicas, gang members)? What is the stable key suffix?
2. **Prior and defaults.** Which values did the author write, and which did
   the platform fill (`Prior.defaulted`)?
3. **Translation.** Units, unit conventions, GPU naming (`AcceleratorMap`), the
   limit/request semantics, QoS-like classes.
4. **Validation.** Everything that runs after your hook and can reject the
   result.
5. **Platform model.** Node shapes and prices, live quota, retry semantics,
   walltime, whether memory is enforced at all, CPU measurement window,
   runtime overhead.
6. **Outcomes.** Where peaks come from, at what resolution, how OOM is
   reported, how a run maps to a key, censoring.
7. **Trust.** What identifies the tenant, and what a user can forge.
8. **Double sizing.** Mark what you sized (`decision-id`) so other layers skip it.

A conformance suite (`rightsize plugin check NAME`) runs a plugin against
fixture submissions and checks: handshake, determinism of `extract`,
idempotence of `render` (rendering an already-rendered submission is
`Patch::None`), that `validate` rejects the fixture's invalid cases, and
that a crash or timeout leaves submissions unchanged.

## 5. Platform survey

Which platforms besides Kubernetes and Flyte would benefit, and which get
plugins in v1. Stars are GitHub stars as of 2026-11-18.

| platform | license | AI relevance | integration point | v1 |
|---|---|---|---|---|
| **Slurm** | GPL-2.0 | the standard scheduler for AI training on bare metal and GPU clouds; more than half of the TOP500's top 100; NVIDIA acquired SchedMD (Dec 2025) and committed to keep it open | `cli_filter`/`job_submit` Lua, `sbatch` wrapper, `sacct` | **yes** ([slurm.md](slurm.md)) |
| **Ray** (+ KubeRay) | Apache-2.0 | the most-used open-source distributed AI runtime (≈44k stars): training, batch inference, data prep; runs on VMs or K8s | Python `.options()`, Ray Data `ray_remote_args`, Ray Jobs, KubeRay CRDs | **yes** ([ray.md](ray.md)) |
| SkyPilot | Apache-2.0 | ≈11k stars; launches AI jobs across clouds, K8s and Slurm with `resources: {accelerators, cpus, memory}` | task YAML rewrite (an adapter over its YAML) | later |
| Armada | Apache-2.0 | multi-cluster batch scheduler on K8s, built for large AI/HPC batch | job-submission API; K8s pods underneath | later |
| HTCondor | Apache-2.0 | high-throughput batch, used in research GPU pools | submit-file rewrite; `condor_history` | later |
| dstack | MPL-2.0 | GPU-native AI orchestrator (≈2k stars) | YAML rewrite | later |
| Argo Workflows | Apache-2.0 | K8s workflow engine for ML pipelines | its templates' `resources`; pods also reach the K8s webhook | via K8s (template path in M2+) |
| Kueue / Volcano / JobSet / Kubeflow Trainer | Apache-2.0 | K8s-native batch layers | covered by the K8s plugin, each with its own unit/group rules | via K8s ([kubernetes.md](kubernetes.md) §3) |
| OpenShift/OKD, k3s | Apache-2.0 | K8s distributions | — | via K8s |
| HashiCorp Nomad | **BUSL-1.1** (since Aug 2023; not OSI open source) | real GPU support, small AI footprint; no maintained open fork | no admission hook: jobspec rewrite + `nomad job plan`, alloc stats API, `oom_killed` task events | no — buildable as an out-of-process plugin |
| YARN, Docker Swarm | Apache-2.0 | declining for AI; weak GPU support | — | no |
| Apache Mesos | retired (Attic, 2025) | — | — | no |
| OpenPBS | AGPL-3.0 | HPC | — | no |

**Why Slurm and Ray.** Slurm is the most widely used non-K8s scheduler for AI
training. Ray is the most widely used open-source AI compute platform, and AI
code declares its resources inside Ray programs. Nomad is the best-known
"Kubernetes alternative" but is not open source and runs few AI workloads.
Ray is a runtime rather than a cluster OS. Half of its integration is the
Python SDK; the other half reuses the Kubernetes plugin for KubeRay.
