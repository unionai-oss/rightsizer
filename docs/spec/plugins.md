# Platform plugin interface

Status: **draft v3**. Core concepts: [SPEC.md](SPEC.md).

A **platform plugin** teaches rightsizer one platform: how to find the
resource-bearing units in a native submission, what they currently ask for,
what the platform's constraints and semantics are, how to write a decision
back, and where outcomes come from. Everything else — features, estimators,
the decision rule, constraints, breakers, history — is the core's.

Built-in plugins:

| category | plugins |
|---|---|
| cluster schedulers | **Kubernetes** ([kubernetes.md](kubernetes.md)), **Slurm** ([slurm.md](slurm.md)) |
| AI runtimes | **Ray** ([ray.md](ray.md)) |
| orchestrators | **Flyte** ([flyte.md](flyte.md)), **Metaflow**, **Airflow** ([orchestrators.md](orchestrators.md)) |
| GPU launchers | **SkyPilot**, **dstack** ([launchers.md](launchers.md)) |
| managed services | **SageMaker AI**, **AWS Batch** ([aws.md](aws.md)), **Modal** ([modal.md](modal.md)); Vertex AI, Azure ML, Databricks in M7 ([managed-ml.md](managed-ml.md)) |

§5 is the survey behind these choices.

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

### 1.1 Integration tiers

Platforms differ in how far an integration can go. Each plugin declares, per
surface, the highest tier it reaches:

| tier | what it does | needs |
|---|---|---|
| **A**udit | imports history, reports waste, risk and advisories (`rightsize audit`, SPEC §12.3) | an `OutcomeSource`, read-only credentials |
| **R**ecommend | decides at submit time and reports, changing nothing | an `Adapter` with `extract` and a point that sees submissions |
| **E**nforce | rewrites the submission under the safety contract | `render` and `validate`, at a point that can change the submission |
| **L**adder | escalates after an OOM-class failure, so reductions are S3(a)-recoverable | a `Caller` or `Orchestrator` ladder driver (SPEC §7.2) |

Tiers are cumulative per surface, and coverage is planned tier by tier: A
reaches every platform with an importer, so it ships first (milestone A).

### 1.2 Where a plugin can hook in

| hook point | examples | can enforce | covers |
|---|---|---|---|
| **server-side mutation** | K8s mutating webhook, Slurm `job_submit.lua`, SkyPilot `AdminPolicy`, dstack `ApplyPolicy` | yes | every submission to that server |
| **client-side submit hook** | botocore `before-parameter-build` (aws.md §2.1), Slurm `cli_filter`, Airflow `pod_mutation_hook` | yes | processes or hosts that load the hook |
| **SDK wrapper or subclass** | Flyte `RightsizedTask`, Ray `RightsizedRemoteFunction`, Modal `rs.modal.sized`, `rs.azureml.sized` | yes | calls that go through the wrapper |
| **compiled spec or IaC** | `rightsize apply` on manifests and pipeline specs; `rightsize suggest` on code | yes, at deploy time (increases only unless reconciled) or through review | definitions in files |
| **none** (accounting only) | AWS console, Step Functions service integrations, Replicate | no | audit only |

**Deny-only controls are not hook points.** IAM and SCP conditions, Google Org
Policy and Azure Policy can reject requests but never change them. They are
useful as a backstop (for example, an IAM allow-list of instance types) and
as validation input, never as an enforce point.

## 2. In-process interface (Rust, `rightsizer-plugin`)

```rust
/// A native submission, as the plugin received it.
pub enum Native {
    Document { media_type: String, bytes: Vec<u8> },     // YAML/JSON manifests, sbatch scripts, pipeline definitions
    Object(serde_json::Value),                            // admission request objects, Slurm option tables, Ray option dicts
    ApiCall { service: String, operation: String, params: serde_json::Value },   // a cloud API request before signing: "sagemaker"/"CreateTrainingJob"
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
    pub request_model: RequestModel,                       // Free | Catalog | Combos | Snap (SPEC §5.8)
    pub price: PriceModel,                                 // Packed | Linear { cpu_core_hr, mem_gib_hr, gpu_hr: map } | Instance | UsageFloor { rates } (SPEC §7.2)
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
- **One layer decides (double sizing).** Every sized submission carries a
  decision marker — the `rightsizer.io/decision-id` annotation, a
  `RIGHTSIZER_DECISION` environment variable in the container or task, a
  Slurm token — and every layer skips a submission that already has one. A
  layer that cannot write its marker does not change the submission. An
  integration that decides a unit before handing it to another hooked layer
  (Metaflow before the botocore hook, SkyPilot before the webhook or Slurm)
  tells that layer explicitly (`RIGHTSIZER_DECIDED`, the annotation,
  `presized_submitters`). Otherwise the second layer would size from the
  first layer's output as if it were the author's prior.
- **Hosts that fail closed are guarded.** When the host fails the submission
  on a hook error or timeout (SkyPilot, dstack, Airflow, botocore), the plugin
  catches every error and enforces a deadline shorter than the host's (SPEC
  §2.3 S1). A plugin never rejects a submission.
- **Client-side tenants select history; the store protects it.** A hook that
  runs in the user's own process (botocore, Modal, Metaflow's local runtime,
  SkyPilot's client-side policy) derives the tenant from the caller's
  authenticated identity (STS, the Modal token), but the user controls that
  process. The tenant then only chooses which history is read. Records are
  `Authenticated` only when written with credentials scoped to the tenant's
  prefix (SPEC §8.2). A client-side surface may enforce for its own user; it
  never writes `Verified` records.

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
5. **Platform model.** Node shapes and prices, the request model (free
   quantities, a catalog of machine types, legal combinations, or a snap
   function), the billing model (does an under-ask save anything?), live
   quota, retry semantics, walltime, whether memory is enforced at all, CPU
   measurement window, runtime overhead.
6. **Outcomes.** Where peaks come from, at what resolution, how OOM is
   reported, how a run maps to a key, censoring.
7. **Trust.** What identifies the tenant, and what a user can forge.
8. **Double sizing.** Mark what you sized (`decision-id`) so other layers skip it.
9. **Hook failure semantics.** Does the host fail open or closed on an error
   or a timeout? What is its deadline? Guard accordingly.
10. **Ladder driver.** Who sees a failure first: the platform, an
    orchestrator that retries through your hook, or a caller you can wrap? No
    driver means reductions need S3(b).
11. **Tier per surface** (§1.1), stated in the plugin's document.

A conformance suite (`rightsize plugin check NAME`) runs a plugin against
fixture submissions and checks: handshake, determinism of `extract`,
idempotence of `render` (rendering an already-rendered submission is
`Patch::None`), that `validate` rejects the fixture's invalid cases, and
that a crash or timeout leaves submissions unchanged.

## 5. Platform survey

Which platforms would benefit, how deep an integration each allows, and in
which milestone it ships. The goal is the widest coverage for one model and
one history. Stars are GitHub stars as of 2026-11-18. "Tier" is the highest
tier reachable (§1.1); "via K8s" means the platform runs pods that the
Kubernetes webhook already sizes.

### 5.1 Cluster schedulers and runtimes

| platform | license | AI relevance | hook point | tier | milestone |
|---|---|---|---|---|---|
| **Kubernetes** (incl. EKS, GKE, AKS, OpenShift, CoreWeave, HyperPod on EKS) | Apache-2.0 | the default substrate for AI platforms | mutating webhook | A R E L | M2 ([kubernetes.md](kubernetes.md)) |
| **Slurm** (incl. HyperPod on Slurm, CoreWeave SUNK) | GPL-2.0 | the standard scheduler for AI training on bare metal and GPU clouds; more than half of the TOP500's top 100; NVIDIA acquired SchedMD (Dec 2025) and committed to keep it open | `cli_filter`/`job_submit` Lua, `sbatch` wrapper, `sacct` | A R E L | M3 ([slurm.md](slurm.md)) |
| **Ray** (+ KubeRay, Anyscale) | Apache-2.0 | the most-used open-source distributed AI runtime (≈44k stars) | `RemoteFunction` subclass, Ray Data args, Ray Jobs; KubeRay CRDs via K8s | A R E | M3 ([ray.md](ray.md)) |
| Kueue / Volcano / JobSet / Kubeflow Trainer | Apache-2.0 | K8s-native batch layers | unit and group rules in the webhook | via K8s | M2–M3 |
| Armada | Apache-2.0 | multi-cluster batch on K8s | job-submission API; pods underneath | via K8s | later |
| HTCondor | Apache-2.0 | high-throughput research GPU pools | submit-file rewrite; `condor_history` | A R E (out-of-process plugin) | later |
| HashiCorp Nomad | **BUSL-1.1** (since Aug 2023) | real GPU support, small AI footprint | no admission hook: jobspec rewrite, alloc stats, `oom_killed` events | buildable as an out-of-process plugin | no |
| YARN, Docker Swarm, OpenPBS, Mesos (retired) | — | declining or HPC-only | — | — | no |

### 5.2 Orchestrators

Most reach Kubernetes, so the webhook covers them from M2. A native
integration adds orchestrator keys, non-K8s backends and orchestrator retries
as ladders ([orchestrators.md](orchestrators.md) intro).

| platform | stars | hook point | can mutate | tier | milestone |
|---|---|---|---|---|---|
| **Flyte v2** | — | task subclass, per-call override, run context | yes, per call | A R E L | M1 ([flyte.md](flyte.md)) |
| **Airflow** | 47.5k | `pod_mutation_hook` + `try_number`; botocore hook for AWS operators | yes, per attempt | A R E L | M6 |
| **Metaflow** | 10.4k | `@rightsize` extension; `METAFLOW_DEFAULT_DECOSPECS`; `runtime_step_cli(retry_count)` | yes, per attempt (local runtime) or per deploy | A R E L | M6 |
| Prefect | 24.2k | `job_variables` (per flow run, not per task) | yes, per flow run | via K8s; native later | M7 |
| Argo Workflows | 17.1k | template resources | via K8s | via K8s | M2 |
| Dagster | 16.4k | `K8sStepHandler` subclass (private API) | yes, per step | via K8s; native later | M7 |
| Kubeflow Pipelines v2 | 4.3k | resources from `PipelineChannel` at run time | yes, per run | via K8s; native later | M7 |
| ZenML | 5.6k | `ResourceSettings` through a flavor | via backend | via backend | not planned |
| Kedro, MLflow Projects, Hamilton | — | no per-step resources | — | — | no |

### 5.3 GPU launchers

| platform | stars | hook point | can mutate | native outcomes | tier | milestone |
|---|---|---|---|---|---|---|
| **SkyPilot** | 10.9k | `sky.AdminPolicy.validate_and_mutate`, server side | yes (fails closed: guarded) | status only | A R E | M6 ([launchers.md](launchers.md)) |
| **dstack** | 2.3k | `ApplyPolicy.on_run_apply`, server side | yes | CPU, memory, GPU memory every 10 s; exit status | A R E | M6 |

### 5.4 Managed ML job services

None has a mutating server-side hook (§1.2). The enforce point is the client.

| service | request model | enforce point | outcomes (resolution) | ladder | tier | milestone |
|---|---|---|---|---|---|---|
| **SageMaker AI** (training, processing, transform, Pipelines) | catalog of `ml.*` types | botocore hook; SDK-agnostic | CloudWatch utilization (60 s), `FailureReason` | caller (`rs.aws.sagemaker.run`), orchestrator | A R E L | M5 ([aws.md](aws.md)) |
| **AWS Batch** (EC2, Fargate, EKS) | free (EC2), combos (Fargate) | botocore hook on `SubmitJob` overrides | `DescribeJobs` exit reasons; Container Insights | caller, orchestrator (Metaflow, Airflow) | A R E L | M5 |
| **Vertex AI** custom jobs, Pipelines | catalog; snap (KFP limits) | SDK wrapper; compiled IR rewrite | Cloud Monitoring (60 s) | none in v1 | A R E | M7 ([managed-ml.md](managed-ml.md)) |
| **Azure ML** (clusters, serverless, Kubernetes instance types) | catalog | `MLClient.jobs` proxy; job YAML | Azure Monitor (60 s, MB, per GPU device) | none in v1 | A R E | M7 |
| **Databricks** (single-node and GPU job clusters) | catalog | bundle and Jobs JSON rewrite; cluster-policy validation | system tables (60 s, per node) | repair run (verify) | A R E | M7 |
| Google Cloud Batch, Cloud Run jobs | free within machine / caps | job JSON rewrite | per VM (Ops Agent); container metrics | — | R (A once an importer exists) | later |
| Snowflake SPCS / ML Jobs | instance families | — | event table | — | R (A once an importer exists) | later |
| Azure Batch | fixed pool VM size | — | none per task | — | — | no |

### 5.5 Serverless GPU platforms

| platform | resources set | per call? | outcomes | tier | milestone |
|---|---|---|---|---|---|
| **Modal** | `cpu`, `memory` (request or request+limit), `gpu` on the decorator; billed on max(request, use) | yes: `Function.with_options` variants | per-input status incl. OOM kill; container p50/p90/p99; OTel export; billing API | A R E | M5 ([modal.md](modal.md)) |
| Beam | `cpu`, `memory`, `gpu`, `gpu_count` on the decorator | unverified | unverified | A R (via `suggest`) | later |
| RunPod | Serverless: GPU type list per endpoint; Pods: GPU type, count and RAM per pod | Pods only | none documented | A R (Pods E later) | later |
| Baseten | Truss `resources` / `instance_type` per deployment; training configs | no (per deployment) | none documented | R (via `suggest`) | later |
| fal, Replicate/Cog, Together, Lambda | machine type per app or deployment, or no resource choice | no | none documented | — | no |

### 5.6 Why these, in this order

- **Slurm and Ray** (M3) are the most widely used non-Kubernetes schedulers
  for AI training and the most widely used open-source AI runtime. Nomad is
  the best-known "Kubernetes alternative" but is not open source and runs few
  AI workloads.
- **Audit first, everywhere** (milestone A). It needs only an importer and
  read-only credentials, gives a dollar figure in minutes, and is how most
  users will first meet rightsizer.
- **AWS and Modal** (M5). SageMaker and AWS Batch hold a large share of
  managed AI training and batch jobs, and one botocore hook covers every
  Python SDK and the orchestrators that call them. Modal is the
  serverless-GPU platform whose SDK makes per-call sizing possible
  (`with_options`) and whose billing rewards sizing GPU type and
  over-requests.
- **SkyPilot, dstack, Metaflow, Airflow** (M6). SkyPilot and dstack have
  official server-side mutation hooks and reach clouds and neoclouds that no
  cluster hook sees. Metaflow brings AWS Batch users and the best keys
  (`flow/step`). Airflow has the largest installed base, and one settings
  module gives it per-attempt enforcement and a ladder.
- **Vertex AI, Azure ML, Databricks** (M7). Each needs its own catalog,
  importer and client wrapper, and the Vertex Pipelines machine matching must
  first be derived from observation.
- Every platform not listed with a plugin can still be added as an
  out-of-process plugin (§3) or reached through `rightsize suggest` when its
  resources live in code or config.
