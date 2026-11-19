# rightsizer — core specification

Status: **draft v3** (after review rounds 1–4; see [REVIEW-LOG.md](REVIEW-LOG.md))
Prior art: [`model-factory-resource-tuner`](https://github.com/unionai-oss/model-factory/tree/main/model-factory-resource-tuner)
(the "resource tuner"), rounds 1–23. Citations `tuner:path:line` point into it.

| document | covers |
|---|---|
| **SPEC.md** (this) | goals, safety contract, data model, estimators, Estimator Protocol, decision rule, history, features, packaging, CLI, operations, milestones |
| [flyte.md](flyte.md) | Flyte v2: task, action, run and environment levels |
| [kubernetes.md](kubernetes.md) | CLI, admission webhook, outcome watcher; Kueue, JobSet, Kubeflow, KubeRay |
| [plugins.md](plugins.md) | platform plugin interface (in-process and out-of-process); platform survey |
| [slurm.md](slurm.md) | Slurm plugin |
| [ray.md](ray.md) | Ray plugin |
| [aws.md](aws.md) | AWS: SageMaker AI and AWS Batch |
| [modal.md](modal.md) | Modal |
| [launchers.md](launchers.md) | SkyPilot and dstack |
| [orchestrators.md](orchestrators.md) | Metaflow, Airflow; notes on Kubeflow Pipelines, Dagster, Prefect, Argo |
| [managed-ml.md](managed-ml.md) | Vertex AI, Azure ML, Databricks, Cloud Batch/Run, Snowflake |
| [models.md](models.md) | serving model-factory models (Hugging Face) through vLLM, SGLang or transformers behind the Estimator Protocol |
| [REVIEW-LOG.md](REVIEW-LOG.md) | design and review rounds |

Section references like "kubernetes.md §3.4" point into the sibling
documents; a bare "§n" is this document.

---

## 1. Summary

`rightsizer` chooses the resource request for an AI workload — memory, CPU,
GPU, and (later) ephemeral storage and shared memory — **before the workload
runs**, from what a scheduler can see at submit time: the workload's code, its
inputs' metadata, its platform, and its own run history. It can **recommend**
(report what it would do) or **enforce** (rewrite the submission), under a
safety contract (§2.3).

It ships as Rust crates (data model, features, estimators, decision rule,
history, platform adapters), Python packages (`rightsizer`, native; and
`rightsizer-runtime`, pure Python) with integrations for Flyte v2, Ray,
Metaflow, Airflow, SkyPilot, dstack, Modal and AWS (SageMaker AI, Batch), a
**`rsz` CLI** that also runs the Kubernetes admission webhook and a local
daemon, and the **Estimator Protocol**, so the model can be hosted anywhere.
Platforms without any hook point still get a read-only **audit** (§12.3) and
resource edits proposed as code changes (§12.4).

### 1.1 What to expect, honestly

- **Increases are immediate.** A first run whose code and inputs predict more
  memory than declared is sized up before it can OOM.
- **Reductions are earned.** Most savings are reductions, and a reduction on
  memory or GPU memory is enforced only when the platform can recover from a
  wrong guess (an escalation ladder) or history proves it safe (§2.3 S3). Where
  memory is enforced *and* a ladder exists — Flyte tasks whose memory has a limit,
  `rsz apply --watch` and `rsz slurm sbatch --watch` on clusters that
  enforce memory, Metaflow and Airflow tasks with enough retries on backends
  that enforce memory, and jobs
  launched through `rs.aws.sagemaker.run` — savings start on the first run.
  Elsewhere (Ray, Modal, SkyPilot, request-only memory) they start after a few
  measured runs.
- **Managed services enforce on the client.** SageMaker, Vertex AI, Azure ML
  and Modal have no hook that can rewrite a job on the server. There,
  rightsizer enforces only in processes that load its hook or wrapper
  (aws.md §2, modal.md §2), and everything else gets audit and
  recommendations.
- **Shadow mode first.** `recommend` changes nothing and records what would
  have happened; `rsz shadow` reports savings, would-be failures and a
  per-workload readiness verdict (§12.2) before anyone enforces.

### 1.2 Positioning

VPA, Goldilocks, KRR, Kubecost, StormForge, CAST AI and PerfectScale size
**long-running services from usage history**. Batch jobs never accumulate that
history, and none of these tools choose GPU shapes. Rightsizer targets
**batch-shaped AI work**: training, fine-tuning, batch inference, data
preparation, evaluation. It predicts **before the first run** and covers
cluster schedulers (Kubernetes, Slurm), AI runtimes (Ray), orchestrators
(Flyte, Metaflow, Airflow), GPU launchers (SkyPilot, dstack) and managed
services (SageMaker AI, AWS Batch, Modal; Vertex AI, Azure ML and Databricks
in M7) with one model and one history. The platform coverage plan is in
plugins.md §5.

## 2. Goals, non-goals, safety contract

### 2.1 Goals

1. Right-size the first run; improve with history.
2. Price asymmetric risk explicitly. Memory and GPU memory are incompressible
   (an under-ask kills the attempt). CPU is compressible: an under-ask stretches
   the run, and fails it at a walltime. One knob, `oom_penalty`, trades waste
   against failures.
3. One model, many platforms: decisions over a platform-neutral context;
   adapters own translation, overheads, retry semantics and failure signals.
4. Pluggable estimators: classical ones in-process; learned ones over the
   Estimator Protocol.
5. Auditable and learnable: every decision is logged with its estimate,
   propensity, constraints and model version; every outcome with its censoring.

### 2.2 Non-goals (v1)

- Horizontal or node autoscaling and bin-packing.
- In-place resize of running pods.
- Rightsizing long-running services.
- Training models (the model factory's job; rightsizer feeds it data, §14.1).
- Changing distributed-training topology: GPU count for distributed units,
  replicas, world size (§5.4). GPU *advisories* are reported (§7.7).
- Tuning program parameters (batch size, worker counts, precision).

### 2.3 Safety contract

- **S1 Inertness.** Any rightsizer error leaves the submission byte-identical
  to what the user submitted. Errors include estimator failures, protocol errors,
  config errors, panics, a history store that cannot be reached past its cache
  TTL (§8.4), a breaker state that cannot be read (treated as *open* for
  reductions), and an expired decision budget. Errors are logged and counted,
  never raised into user code. The one exception is `strict = true`, meant for
  CI, which fails on configuration errors.

  **Fail-closed hosts.** Several hook points fail the user's submission when
  the hook raises or times out: SkyPilot admin policies, dstack apply
  policies, Airflow's `pod_mutation_hook`, botocore event handlers
  (launchers.md §1.1, orchestrators.md §3.2, aws.md §2.1). Rightsizer code at
  such a point catches every error itself and enforces its own deadline,
  shorter than the host's. It never relies on the host to fail open, and it
  never rejects a submission.
- **S2 No new failure modes.** Rightsizer never:
  - adds a limit that did not exist;
  - lowers an existing limit except under S3;
  - changes GPU count (never, in v1), GPU type or QoS class unless that axis is
    opted in;
  - emits a patch a platform validator would reject (kubernetes.md §3.4).
- **S3 Recoverable reductions only.** A request below the prior on an
  incompressible axis (memory, GPU memory) is enforced only if one of these holds:
  - **(a) Ladder.** The integration resubmits on failure (`PlatformRetry.ladder`,
    §7.2), and the ladder's final step reaches at least
    max(prior, largest observed successful request) within the ceiling.
  - **(b) Evidence.** At least `min_history` (default 3) measured successes
    (`Exact` or `AtMost`, trust ≥ `Authenticated`, §8.2) exist whose peak ×
    `scale` (§7.4) × headroom fits under the proposed request. Headroom is 1.05,
    raised to 1.25 when the peak was sampled coarser than 1 s (`resolution_s > 1`)
    or comes from another code digest of the same key, and to 1.5 when both.
    Peaks with `resolution_s ≥ 10` (dstack every 10 s; CloudWatch, Cloud
    Monitoring, Azure Monitor and system tables every 60 s) are **coarse**:
    headroom 1.5, or 1.75 with another digest, and S3(b) needs
    `2 × min_history` of them.

  Where memory is **not enforced** by the platform (Ray's logical resources;
  Slurm without `ConstrainRAMSpace`; a Kubernetes container with a memory
  request and no limit), an under-ask does not deterministically fail the unit
  itself. It can harm co-located work (the kernel OOM killer or Ray's memory
  monitor may pick a neighbour), so only (b) applies, and the cost model adds a
  neighbour-risk premium (§7.2). The test is **where the failure lands**: a
  platform whose memory manager evicts exactly the over-request unit (Modal
  with an integer `memory`, modal.md §1) counts as enforced, because the
  failure is the unit's own. Otherwise the axis is held at the prior, and
  the decision reports the reduction as a recommendation.
- **S4 Schedulability.** An increase is enforced only if a node shape and the
  quota (kubernetes.md §5, slurm.md §7) can admit it. Otherwise it is clamped,
  counted (`rightsizer_clamped_total{reason}`) and explained. It never causes a
  silent `Pending`.
- **S5 Attribution and breakers.**
  - **Definition.** A failure is **rightsizer-caused** if it is an OOM-class
    failure or a timeout on an axis where the enforced request was below the
    prior. The failure is re-attributed as *not* rightsizer-caused when a retry
    at or above the prior also fails, or when the run's input exceeded every
    input size in its history. Evictions due to node memory pressure of a unit
    whose memory was reduced count as rightsizer-caused.
  - **Unclassified failures.** A failed attempt whose enforced request was
    below the prior on any incompressible axis, and whose failure is not
    classified within `classification_window` (default 1 h), counts as
    rightsizer-caused for breakers until an importer classifies it otherwise.
    A surface that cannot get failures classified at all (no importer, no
    status signal) may not enforce reductions; it runs them in `recommend`.
  - **The prior is the author's.** "Below the prior" always means below the
    **origin prior**: what the author wrote before any rightsizer layer
    changed it. Every value rightsizer persists — webhook patches (the signed
    `rightsizer.io/prior` annotation), deploy-time sizing, edits merged from
    `rsz suggest` (a `rightsizer: prior=… decision=…` marker next to the
    edited value, §12.4) — carries the origin prior, and attribution, ladders'
    `final_floor` and the re-attribution rule use it.
  - **Breakers.** Breakers demote from `enforce` to `recommend`. Their state
    lives in the history store (§8.1) and processes cache it for 60 s.

    | breaker | trips when | scope |
    |---|---|---|
    | per-workload | one rightsizer-caused failure that escalation did not recover | the workload key; or the input-size bucket, if the run was beyond its history's sizes |
    | per-estimator-version | rightsizer-caused failure rate > 2% over ≥ 50 enforced decisions from that version | every decision from that estimator version; the chain skips it |
    | fleet fast-burn | > 5% of enforced invocations fail rightsizer-caused within 1 h, with ≥ 20 such failures | the whole store prefix |
    | fleet slow-burn | > 0.5% over 30 days (the SLO) | the whole store prefix |

  - **Recovery.** A per-workload breaker goes half-open after
    `recovery_shadow_fits` (default 5) shadow decisions whose measured outcomes
    fit. The others need `rsz breaker reset`. Any breaker can be tripped by
    hand: `rsz breaker trip [--fleet | KEY]`.
- **Global ceiling.** `mode_ceiling` (off | observe | recommend | enforce) is
  read dynamically by every client from the store (`{prefix}/control/mode.json`,
  60 s cache). It caps every policy at every level, and is the fleet kill switch.

## 3. Evidence carried over from the resource tuner

| finding | source | consequence |
|---|---|---|
| **Information boundary.** Every arm may see the source text; input format metadata (bytes always; parquet rows and schema from the footer; npy shape and dtype; the CSV header only; the first JSONL record's keys only); and the workload's own history. Nothing measured on the run being decided. | `tuner:resource_tuner/bench/features.py:1-20` | `WorkloadContext` is that boundary (§5.2). |
| **Requests are grid cells.** Memory is on a half-power-of-two ladder (256Mi … 64Gi, ≈1.4× steps). CPU has 16 steps (0.25 … 8). GPU memory uses cells aligned to machine shapes' capacities (1 … 176 GiB), each landing on the cheapest shape that holds it. | `bench/grid.py:18`, `bench_cpu/grid.py:11`, `bench_gpu/grid.py:29-52` | `Grid` is first-class and versioned (§5.3). |
| **Decode by expected cost.** Estimating a distribution and decoding by expected cost beats argmax. The decode-time knobs are `OOM_PENALTY` (multiples of the oracle run's cost per retry; default 1, swept 0–16) and `DELAY_PENALTY` (the oracle's hourly price per hour late; default 1). The 2/12/40 `under_penalty` values were a *training-loss* weight, with one trained model per value. | `bench/scoring.py:41-47`; `bench_all/scoring.py:128-136`; `training/decision_model.py:161-205` | The decision rule lives in rightsizer (§7). |
| **k8s-2 leaderboard** (joint memory+CPU+GPU regret on Job pods, test / unseen code, 95% task-clustered CIs): static 89.3 / 115.1; rules 42.4 / 49.8; **rules-sized 37.7 / 39.2**; zero-shot 27B 48.8 / 42.8; trees 11.5 / 69.1; decision model 12.6 / 47.8; RL 14.6 / 39.3; **pool × decision 6.6** / 56.8; **gate 8.1 / 39.3**. | `research_log/2026-11-08-round-23-review-real-pods.md` §7 | The learned pools win on familiar code and lose on new code; the gate and the size-scaled rule hold up on both. The default chain starts from `rules-sized` (M0); gate(pool, rules-sized) arrives at M4 (§6.5). |
| At an OOM penalty of 5–20 (a raw Job with `backoffLimit: 0`, resubmitted by hand), `rules-sized` is the best arm on unseen code. | round 23, "Retry cost" | Platform retry semantics enter the decision (§7.2). |
| The **novelty gate** is the kNN distance in the trees' feature space, thresholded at the 0.95 quantile on in-distribution validation data. AUROC is 0.999 with code tokens and 0.67 without. | `bench_all/gating.py:1-60`; round 23 | Novelty is defined as an ECDF and ships with the model (§6.5). Without code features, the gate routes to the robust arm. |
| The pool is **log-linear**, with weight 0.7 on the trees. | `bench/hybrid.py:95` | §6.5. |
| **Runtime overhead is a distribution**: Flyte median ≈70 MiB, p90 ≈242 MiB. Memory-backed `emptyDir` charges staged bytes to the pod's memory cgroup. A LimitRange can turn a missing CPU limit into 0. | `bench_all/k8s.py:40-75`; round 23 | Adapters own overhead accounting and manifest features. |
| The tune service never breaks a call, caches per context digest, and keeps an append-only ledger of immutable shards. | `tune.py`, `tune_service.py`, `tune_store.py` | S1; §6.3; §8.1. |
| A zero-shot LLM needs about 8k output tokens to answer reliably. It then roughly matches the rules, at about 90 s per decision. | rounds 17 and 23 | No built-in LLM estimator (§6.4). |

## 4. Concepts

| term | meaning |
|---|---|
| **Workload** | A repeatable unit with a stable identity: a Flyte task, a K8s Job replica type, a Slurm job script, a Ray remote function. |
| **Tenant** | The security principal that owns workloads, **derived by the integration from authenticated data**: K8s `{cluster}/{namespace}`; Flyte `{org}/{project}/{domain}`; Slurm `{cluster}/{account}`; Ray `{deployment-configured tenant}` or the KubeRay namespace. |
| **Workload key** | `{platform}://{tenant}/{suffix}`. Users choose only the suffix. |
| **Unit** | One resource-bearing thing in a submission: a container, a replica type, a het-job component, a Ray function. Units in a **group** are sized identically. |
| **Invocation** | One submission with concrete inputs. |
| **Context** | What may be known before the run (§5.2). |
| **Prior** | The request the author declared, plus what platform defaults filled (`defaulted`). |
| **Grid** | The finite set of requests per axis. |
| **Estimate** | Per-axis distributions over **demand**, or, for policy models, over **actions** (§5.5). |
| **Decision** | The applied spec, the unconstrained recommendation, provenance, cost, propensity (§5.6). |
| **Outcome** | Status, censored measurements, duration, attempts (§5.7). |
| **Mode** | `off` < `observe` (record outcomes) < `recommend` (decide and record, change nothing) < `enforce` (apply under §2.3). |
| **Adapter** | A platform plugin (plugins.md). |

## 5. Data model (`rightsizer-core`)

Rust structs with `serde` and stable `snake_case` JSON. JSON Schemas are
generated into `schemas/` and are normative for every wire format. Every
serialized artifact carries `"v"`. That covers wire objects, store records,
`custom_context` payloads, K8s annotations, Slurm tokens and decision tables.
Compatibility is in §13.2.

### 5.1 Axes, quantities, requests

```rust
pub enum Axis { Memory, Cpu, GpuCount, GpuType, EphemeralStorage, Shm }
pub type AxisSet = enumflags2::BitFlags<Axis>;
// Aliases accepted at every API surface: "gpu" = {GpuCount, GpuType}; "disk" = EphemeralStorage.

pub struct Bytes(pub u64);
pub struct MilliCpu(pub u32);                     // 1000 = one core

pub struct GpuShape { pub accel: AcceleratorId, pub count: u32 }

pub struct Requests {
    pub cpu: Option<MilliCpu>,
    pub memory: Option<Bytes>,
    pub gpu: Option<GpuShape>,
    pub ephemeral_storage: Option<Bytes>,
    pub shm: Option<Bytes>,
}
pub struct Limits {                               // GPU and shm have no separate limit
    pub cpu: Option<MilliCpu>,
    pub memory: Option<Bytes>,
    pub ephemeral_storage: Option<Bytes>,
}
pub struct ResourceSpec { pub requests: Requests, pub limits: Limits }

pub struct Prior {
    pub spec: ResourceSpec,
    pub defaulted: AxisSet,                       // filled by platform defaults, not the author
    pub qos: Option<QosClass>,                    // Guaranteed | Burstable | BestEffort (K8s)
}
```

`rightsizer-core::units` parses and formats every platform's quantities:

- K8s: `1.5Gi`, `500m`, `1e9`;
- Slurm: `--mem` default unit MiB; suffixes K, M, G and T are powers of 1024;
- Ray: bytes;
- Flyte: K8s quantity strings.

All are covered by round-trip property tests.

#### 5.1.1 Accelerator catalog

`AcceleratorId` is a canonical id with per-device memory and family:

```
T4-16G  L4-24G  A10G-24G  A10-24G  L40S-48G  A100-40G  A100-80G  H100-80G  H200-141G
B200-180G  MI300X-192G  TPU-V5E  TPU-V5P  TPU-V6E  TRN1  TRN2  INF2  unknown-gpu
```

Each adapter maps the catalog to its platform's names:

| canonical | Flyte | K8s node label (configurable) | Slurm gres | Ray |
|---|---|---|---|---|
| `A100-80G` | `flyte.GPU("A100_80G", n)` | `nvidia.com/gpu.product=NVIDIA-A100-SXM4-80GB` | site table, e.g. `gpu:a100_80g` | `A100` (auto-labelled); `A100-80G` only if nodes are labelled so |
| `L4-24G` | `"L4:n"` | `nvidia.com/gpu.product=NVIDIA-L4` | `gpu:l4` | `L4` |
| `unknown-gpu` | `gpu=<int>` | `nvidia.com/gpu` with no product selector | untyped `gpu:n` | `num_gpus` with no accelerator type |

The type of an `unknown-gpu` prior is never changed.

### 5.2 `WorkloadContext`: the information boundary

```rust
pub struct WorkloadContext {
    pub v: u16,                                   // 1
    pub key: WorkloadKey,
    pub unit: UnitId,
    pub platform: PlatformRef,                    // static facts only; see below
    pub code: Option<CodeInfo>,
    pub inputs: Vec<InputMeta>,                   // ≤ 64, deterministic truncation (largest first, then name)
    pub manifest: ManifestFeatures,               // adapter-derived; e.g. memory-backed volume bytes, sidecars
    pub prior: Prior,
    pub history: Vec<RunRecord>,                  // newest first, ≤ history_limit (32), same key
    pub labels: BTreeMap<String, String>,         // allowlisted (§11)
}
pub struct PlatformRef {                          // part of the context digest: must be static
    pub kind: String, pub version: String,
    pub overhead: OverheadModel,                  // runtime overhead distribution (quantiles, MiB)
    pub retry: PlatformRetry,                     // §7.2
    pub walltime_s: Option<u64>,
    pub cpu_window_s: u32,
    pub memory_enforced: bool,
}
pub struct CodeInfo {
    pub digest: String,                           // normalized (comments, whitespace, resource directives stripped)
    pub tokens: Vec<String>,                      // features-v1 code tokens
    pub source: Option<String>,                   // only if privacy allows; truncated at 64 KiB
}
pub struct InputMeta {
    pub name: String,
    pub uri_hash: Option<String>,                 // salted; URIs never leave the process
    pub format: InputFormat,                      // Parquet | Csv | Jsonl | Npy | Safetensors | Dir | Blob | Scalar | ModelRef
    pub bytes: Option<u64>,
    pub rows: Option<u64>,                        // parquet footer only
    pub columns: Option<ColumnsInfo>,             // counts by kind; names only if allowed
    pub shape: Option<Vec<u64>>,
    pub dtype: Option<String>,
    pub params: Option<u64>,                      // model parameter count
    pub scalar: Option<ScalarValue>,              // numbers and bools; strings only if allowed
}
```

Node shapes, prices and **live quota are not in the context**. They change
minute to minute and would churn the digest. The decision rule receives them
separately as `PlatformState` (plugins.md §2).

Rules:

- **No peeking.** Readers read bounded byte ranges: headers and footers, ≤ 1 MiB
  per input. A larger footer is skipped and `columns = None` is recorded.
  `test_context_cannot_see_the_answer` pins this.
- **Deterministic truncation.** The same invocation always yields the same
  context and digest.
- **Demand excludes overhead.** Estimators predict *workload demand*: what the
  code uses. The decision converts it to *platform need*: demand + runtime
  overhead + staged in-memory bytes, then × headroom. History stores container
  peaks as measured, and estimators subtract the overhead model's median when
  learning demand. Nothing is counted twice.

### 5.3 Grids

```rust
pub struct Grid {
    pub id: String,                               // "rs-grid-v1"
    pub memory: Vec<Bytes>,
    pub cpu: Vec<MilliCpu>,
    pub gpu_memory: Vec<Bytes>,                   // total GPU memory demand; index 0 means "no GPU"
}
```

`rs-grid-v1`:

| axis | cells |
|---|---|
| memory (27) | the tuner's ladder 256Mi … 64Gi (17 cells), then 96Gi, 128Gi, 192Gi, 256Gi, 384Gi, 512Gi, 768Gi, 1Ti, 1.5Ti, 2Ti |
| cpu (25) | the tuner's 16 steps 0.25 … 8, then 10, 12, 16, 24, 32, 48, 64, 96, 128 |
| gpu_memory (25 + none) | the tuner's 17 cells 1 … 176 GiB, then 256, 352, 512, 640, 768, 1024, 1152, 1536 GiB |

The tuner's grids are prefixes of these, so its models decode unchanged. A
foreign `Cells` estimate is **rebinned conservatively**: each source cell's
mass moves to the smallest target cell covering the source cell's upper edge.
It is never rejected. Ephemeral storage and shm have no grid in v1; they are
held at the prior (§7.4).

### 5.4 GPU shapes

- The adapter's `PlatformState.shapes` lists the schedulable **node shapes**:
  CPU, memory, GPU shape (accelerator × count), price, and the host CPU and
  memory bundled per GPU.
- Estimators predict **total GPU memory demand** (`gpu_memory`; index 0 is no
  GPU). Optionally they also predict `parallelism ∈ {single, replicated, sharded}`
  with an imbalance factor (default 1.25, `tuner:bench_gpu/grid.py:52`).
- A shape fits when the busiest device fits:
  - single or replicated: `need = demand`;
  - sharded: `need = (demand − 0.5 GiB) · imbalance / count + 0.5 GiB`.
- **GPU count is held for every unit in v1** (lower counts appear as
  advisories, §7.7). It is *permanently* held for distributed units — a group
  with more than one replica, or code or a manifest showing `torchrun`,
  `nproc_per_node`, `WORLD_SIZE`, DeepSpeed, FSDP or `accelerate launch` —
  because the count changes program semantics.
- **GPU type changes are off by default** (`allow_gpu_type_change`), and never
  happen for queue-managed K8s jobs. When enabled, they happen within
  `allowed_accelerators`.
- With count and type held, the GPU decision reduces to a fit check. What
  remains decidable is host memory and host CPU, priced at the GPU node's
  per-GPU share. GPU findings are reported as **advisories** (§7.7).

### 5.5 Estimates

```rust
pub struct Estimate {
    pub v: u16,
    pub kind: EstimateKind,                       // Demand | Action
    pub memory: Option<AxisEstimate>,             // workload demand, bytes
    pub cpu_mean: Option<AxisEstimate>,           // mean cores over the run: drives stretch
    pub cpu_peak: Option<AxisEstimate>,           // busiest-window cores: reported and used for throttling diagnostics, not cost
    pub gpu_memory: Option<AxisEstimate>,         // total; Cells index 0 = no GPU
    pub parallelism: Option<Parallelism>,         // { mode: Single|Replicated|Sharded, imbalance: f32 }
    pub duration_s: Option<AxisEstimate>,         // Quantiles or Point only
    pub t_peak_frac: Option<f32>,                 // fraction of the run elapsed at the memory peak
    pub novelty: Option<f32>,                     // ECDF value, 0..1 (§6.5)
    pub provenance: Provenance,                   // { estimator, version, grid, features, latency_ms }
}
pub enum AxisEstimate {
    Cells { grid: String, probs: Vec<f32> },      // sums to 1
    Quantiles { q: Vec<(f32, f64)> },             // ≥ 3 points
    Point { value: f64 },
}
```

Normalization to cells, applied before deciding:

- **Quantiles.** Crossed quantiles are sorted first. The CDF is interpolated
  log-linearly between the given points. Mass below q_min goes to q_min's
  covering cell. Mass above q_max follows a log-normal tail fitted to the top two
  points, with the remainder at the top cell.
- **Point.** A log-normal centered on the value with σ = `point_sigma` (0.35).
  The `prior` estimator is the exception: it emits a degenerate `Cells` estimate
  at the prior's cell.
- **`kind = Action`.** A policy over cells, from RL-trained or stacked models.
  It is decoded by argmax over the feasible set, not re-decoded by expected
  cost, which would count the cost model twice.
- **Duration.** Missing → the median duration of same-key history; else 1 h,
  with timeout modeling disabled.
- **`t_peak_frac`.** Missing → the median from history; else 0.5.

### 5.6 Decisions

```rust
pub struct Decision {
    pub v: u16,
    pub id: Ulid,
    pub key: WorkloadKey, pub unit: UnitId, pub group: Option<GroupId>,
    pub mode: Mode,                               // effective mode
    pub recommended: ResourceSpec,                // the best spec within hard constraints only (pins, ceiling, S2)
    pub applied: ResourceSpec,                    // what is (or, below enforce, would be) applied: recommended with held axes reset
    pub prior: Prior,
    pub held: Vec<(Axis, HoldReason)>,            // S3-not-met, S2-limit, pinned, not-enforced, breaker, …
    pub source: DecisionSource,                   // Estimator{id, version} | Memo | Prior{reason}
    pub estimate: Option<Estimate>,               // logged for audit and off-policy learning
    pub propensity: f32,                          // P(applied | context) under the decision policy: 1, or 1−ε/ε when exploring
    pub expected_cost: Option<f64>, pub p_fail: Option<f32>,   // of `applied`
    pub advisories: Vec<Advisory>,                // §7.7
    pub explanation: Vec<String>,
    pub context_digest: String,
}
```

### 5.7 Outcomes

```rust
pub struct RunRecord {
    pub v: u16,
    pub key: WorkloadKey, pub cluster: Option<String>,
    pub invocation_id: String, pub attempt: u32,  // dedup key: (key, invocation_id, attempt, source)
    pub decision_id: Option<Ulid>,
    pub started_at: DateTime<Utc>,
    pub inputs: Vec<InputMeta>, pub code_digest: Option<String>,
    pub requested: Option<ResourceSpec>,          // None for local runs
    pub status: RunStatus,                        // Succeeded | OomKilled | GpuOom | Evicted | Timeout | Preempted | Failed | Cached | Unknown
    pub status_confidence: Confidence,            // Observed (the platform said so) | Inferred (from metrics, e.g. memory at 95% before a failure)
    pub memory_peak: Option<Measurement>,
    pub cpu_mean: Option<Measurement>, pub cpu_peak_window: Option<Measurement>,
    pub cpu_throttled_frac: Option<f32>,
    pub gpu_memory_peak: Option<Measurement>,     // busiest device
    pub gpu_util_mean: Option<f32>,
    pub t_peak_frac: Option<f32>, pub duration_s: Option<f64>,
    pub source: OutcomeSource,                    // SelfReport | Cgroup | Prometheus | Sacct | FlyteRemote | RayState | Watcher | Importer { platform }
    pub trust: Trust,                             // Verified | Authenticated | Unverified (§8.2)
}
pub struct Measurement { pub value: f64, pub censor: Censor, pub resolution_s: Option<f32> }
pub enum Censor { Exact, AtLeast, AtMost }
```

Censoring rules, applied by every importer:

- An OOM-class failure records `memory_peak = AtLeast(limit or last sample)`.
- A success within 5% of its limit records `AtLeast(peak)`, since reclaim
  deflates the working set near a limit.
- CPU at a limit with throttling above 5% records `AtLeast`.
- Sampled peaks are `Exact` with `resolution_s` set to the sampling interval;
  S3(b) and the floor use the resolution headroom (§2.3, §7.4).
- Importers declare whether a source's samples are **instantaneous** or
  **period averages**. An average is not a peak and is recorded as `AtLeast`.
  A source whose kind is not yet verified is treated as averages.
- `Inferred` statuses are used for learning and count for the unclassified
  rule (S5), never as proof of an OOM.
- When a source's bounds conflict — an unverified sample kind (so `AtLeast`)
  from a metric that may include other processes (so `AtMost`) — the record
  carries no peak; it still carries status and duration.
- A **percentile** statistic (Modal's per-container p99) is not a peak and is
  recorded as `AtLeast`.
- **Utilization percentages** (CloudWatch `MemoryUtilization`, Cloud
  Monitoring, Databricks `mem_used_percent`) are converted to bytes with the
  shape catalog's memory for the instance that ran (§5.8). When the instance
  type is unknown, the record has no peak.
- On shared GPUs (time-slicing, MPS), device memory includes other tenants and
  is recorded as `AtMost`.
- `Cached` runs carry no measurements and are excluded from learning.

`history-peak`, `rules-sized` and every trainer treat these as censored. An
`AtLeast` value shifts mass upward and is never used as a point.

### 5.8 Request models

Platforms differ in what a request *is*. The adapter's `PlatformState`
carries one of four request models, and the decision rule's candidate set is
built from it:

```rust
pub enum RequestModel {
    Free,                                   // any grid cell within floor and ceiling: K8s, Slurm, Ray, Flyte, Batch on EC2, Modal CPU/memory
    Catalog(Vec<Shape>),                    // discrete machine types: SageMaker, Vertex AI custom jobs, Azure ML, Databricks
    Combos(Vec<(MilliCpu, Vec<Bytes>)>),    // legal CPU/memory pairs only: Fargate
    Snap(Box<dyn SnapFn>),                  // the platform turns a lower bound into a shape itself: SkyPilot, dstack, KFP on Vertex
}
pub struct Shape {
    pub name: String,                       // "ml.g5.2xlarge", "n1-highmem-8", "Standard_NC24ads_A100_v4"
    pub cpu: MilliCpu,
    pub memory: Bytes,                      // installed
    pub reserved: Bytes,                    // taken by the platform's agents and OS before the job starts (per catalog); usable = memory − reserved
    pub gpu: Option<GpuShape>,              // accelerator × count, device memory
    pub price: BTreeMap<String, f64>,       // per hour, by usage type ("training", "processing", "spot", …)
    pub family: String,
}
pub trait SnapFn: Send + Sync { fn snap(&self, ask: &Requests) -> Option<Shape>; }   // None: infeasible
```

- **Catalog.** Candidates are shapes, not cells. A shape's memory cell is its
  usable memory (installed − `reserved`); its CPU cell is its vCPU count; its
  cost is its price. The memory failure probability is
  `P(demand + runtime overhead > usable)`: `reserved` is the platform's share
  and the runtime overhead model (§5.2) is the workload runtime's, so nothing
  is counted twice. The decision renders the shape's name. Holds (§7.4)
  restrict the catalog: same GPU count, same family unless families are
  configured, and so on per adapter.
- **Effective memory.** For Catalog and Snap units, the trust region, the
  floor, the anchor and S3(b)'s "fits under the proposed request" are all
  measured on the **effective usable memory** of the shape that will run, not
  on the ask: a `32+` ask that lands on a 64 GB instance has an effective
  64 GB. `max_step_down = 2` cells means the effective memory may fall to no
  less than half the anchor's, whatever the number of shapes in between.
- **Combos.** Candidates are cells snapped up to the smallest legal pair.
- **Snap.** Candidates are cells. Each is mapped through `snap` to the shape
  the platform would choose; candidates that snap to the same shape collapse
  to the smallest ask. Failure probability uses the snapped shape's usable
  memory when that platform's memory is instance-bounded, otherwise the ask.
  When the snap function is not known exactly (KFP on Vertex, managed-ml.md
  §1.2), the unit is recommend-only.
- Catalogs are data: they ship as versioned JSON (`catalog/*.json`), are
  refreshed from the provider's price and instance APIs
  (`rsz <platform> catalog refresh`), and are part of `PlatformState`,
  never of the context digest.

## 6. Estimators

### 6.1 Trait

```rust
#[async_trait]
pub trait Estimator: Send + Sync {
    fn info(&self) -> &EstimatorInfo;   // id, version, grids, axes, kind, needs{code, history, input_bytes}, features, novelty
    async fn estimate(&self, ctx: &WorkloadContext) -> Result<Estimate, EstimateError>;
    async fn estimate_batch(&self, ctxs: &[WorkloadContext]) -> Vec<Result<Estimate, EstimateError>>;
}
```

`EstimateError::Insufficient` ("I can't answer this context") passes to the
next estimator in the chain. It is not a failure.

### 6.2 Built-in estimators (in-process, no network)

| id | behavior | declines when | tuner arm |
|---|---|---|---|
| `rules-sized` | per-format power law in input bytes, anchored on the nearest-size same-key run and scaled to this invocation (§7.4 `scale`); cold-start constants per format | no input bytes and no history | `rules-sized` |
| `history-peak` | censored max over the last K same-key runs (same digest preferred) × margin; OOM bump max(×1.2, +100Mi) (VPA's rule) | no history | `vpa` |
| `prior` | degenerate at the prior | never | `static` |
| `gbt` | quantile GBTs over `features-v1`; trees from a `rightsizer-trees/1` file | no model configured | `gbt-dist` |

**Default chain (M0–M3):** `rules-sized → history-peak → prior`. `gbt` is
added in front when a model file is configured. No bundled learned model is
claimed until a `features-v1` model passes the bench (M4).

**`rightsizer-trees/1`.** A minimal JSON tree-ensemble format: feature names,
per-quantile trees, splits with a missing-value direction, and leaves. A small
Rust evaluator reads it. Exporters for sklearn
`HistGradientBoostingRegressor(loss="quantile")` (what the tuner trains,
`tuner:bench/arms.py:168`) and LightGBM live in `rightsizer.export`, with
parity tests (≤ 1e-6 relative). An optional companion file,
**`rightsizer-novelty/1`**, carries a standardized reference feature matrix,
k, and the ECDF knots of held-out kNN distances (§6.5).

### 6.3 Estimator Protocol v1 ("hosted anywhere")

HTTP with JSON. Schemas are in `schemas/estimator/1/`.

```
GET  /v1/info             -> { protocol: "estimator/1", id, version, grids, axes, kind,
                               needs: {code, history, input_bytes}, features, novelty: {needs_code} | null, max_batch }
POST /v1/estimate         { context, axes }          -> { estimate }
POST /v1/estimate:batch   { contexts, axes }         -> { results: [ {estimate} | {error} ] }
GET  /v1/health           -> 200 | 503
```

- **Errors.** Every error body is `{error: {code, message, retryable}}`.
  - `400`: malformed request.
  - `409`: unsupported grid or features version.
  - `422`: insufficient context, which is treated like `EstimateError::Insufficient`.
  - `429` / `503`: back off.
  - Any other error moves to the next estimator in the chain (S1).
- **Auth.** A bearer token from an env var or file, or mTLS. Contexts go only to
  explicitly configured URLs.
- **Client.**
  - Per-call deadline: 10 s by default; integrations pass tighter budgets.
  - One jittered retry on `retryable` errors.
  - A circuit breaker: opens after 5 consecutive failures, half-opens after 30 s.
  - **Single-flight:** concurrent identical requests share one in-flight call.
  - **Coalescing:** distinct requests within 20 ms become one batch of up to
    `max_batch`.
- **Cache.** Two layers:
  - **Exact.** Key = (estimator id, version, grid, axes, features,
    `canonical(context with history replaced by the history watermark)`), where
    watermark = sha256 of the sorted ids of the included history records. TTL is 24 h.
  - **Memo.** Stores the **normalized estimate only**; constraints, S3, breakers
    and the mode are re-evaluated on every hit. Key = (workload key, code digest,
    feature bucket, policy hash), everywhere.
    The feature bucket puts each input's bytes on the memory ladder and keeps
    numeric scalars as given. A 10,000-way fan-out over similar inputs costs a
    handful of estimates. The webhook's pre-warmed cache is a memo
    (kubernetes.md §3.2).
- **Reference server.** `rightsizer.serve` wraps any Python callable
  `ctx -> Estimate` (or a batch callable) as a protocol server. A Flyte app
  template serves the tuner's models. Serving the published model-factory models
  through vLLM, SGLang or `transformers` is in [models.md](models.md) (pulled
  forward from M4 to M3).

### 6.4 LLM estimators

None is built in (§3). Anyone may host one behind the protocol.

### 6.5 Chains, pools, gates (M4)

```toml
[estimator]
chain = ["gate", "rules-sized", "history-peak", "prior"]   # top-down; first non-declining answer wins

[estimator.members.decision]
url = "https://rightsizer-model.internal/v1"

[estimator.members.pool]
op = "log-linear"                 # p ∝ Π p_i^{w_i}, renormalized per axis
members = ["gbt", "decision"]
weights = [0.7, 0.3]

[estimator.members.gate]
signal_from = "gbt"               # needs gbt's rightsizer-novelty/1 companion
threshold = 0.95                  # an ECDF value, not a distance
familiar = "pool"
novel = "rules-sized"
on_missing_signal = "novel"       # no code tokens → novel
```

- A pool needs members with the same `kind`, on the same grid after rebinning.
- The gate evaluates `signal_from` once per context. The pool reuses that result
  through the exact cache.
- Novelty is the ECDF of the context's mean kNN distance to the reference set,
  computed over held-out in-distribution distances. When code tokens are absent
  and the novelty file says `needs_code`, the gate routes to `novel`.

## 7. The decision rule

### 7.1 Inputs

The normalized estimate, the grid, `PlatformRef` (from the context),
`PlatformState` (shapes, prices, live quota; from the adapter), the risk
profile, constraints (§7.4) and the prior.

### 7.2 Cost model

**Retry semantics** come from the adapter:

```rust
pub struct PlatformRetry {
    pub same_size: u32,                           // retries the platform performs at the same size (K8s backoffLimit, Flyte retries, Ray max_retries)
    pub ladder: Option<Ladder>,                   // resubmission at a larger size
}
pub struct Ladder {
    pub memory_factor: f32 /*2.0*/, pub max_steps: u32 /*2*/, pub final_floor: Requests,
    pub driver: LadderDriver,
}
pub enum LadderDriver {
    Caller,          // a rightsizer wrapper owns the wait and returns the final attempt: Flyte parent (`on_oom`), `apply --watch`, `slurm sbatch --watch`, `rs.aws.sagemaker.run`, `rsz aws batch submit --wait`
    Orchestrator,    // the orchestrator retries the unit through a rightsizer hook that sees the attempt number (escalate-on-retry, below): Metaflow, Airflow
}
```

**Which ladders count.** A ladder satisfies S3(a) only when the failed attempt
is never the consumer's final answer:

- `Caller`: the consumer is the wrapper's caller, and the wrapper returns only
  the last attempt. Other observers of the job (a `kubectl wait`, an
  EventBridge rule) are not consumers rightsizer protects; integrations that
  have such consumers do not claim a `Caller` ladder. A Caller ladder
  escalates only on OOM-class failures; an unclassified failure is returned to
  the caller and handled by S5's unclassified rule. Before each reduced
  attempt the wrapper writes a **ladder lease** (below); if the wrapper dies
  with the lease open and the attempt failed, the failure is unrecovered and
  trips the per-workload breaker. Every Caller ladder writes leases — the
  Flyte parent (flyte.md §9), `apply --watch`, `slurm sbatch --watch` and the
  AWS helpers. The component that classifies the failure (the watcher, `sacct`
  or a cloud importer) closes the lease: a failed attempt whose lease is still
  open after `lease_ttl` (default 15 min past the failure) is unrecovered.
- `Orchestrator`: see escalate-on-retry.

A detached process that resubmits failed jobs on its own is **not** a ladder:
whoever launched the original job has already seen it fail.

**Attempt records and leases.** Before submitting a reduced attempt, the
integration writes `{prefix}/v1/attempts/{tenant}/{key-hash}/{invocation-hash}.json`
(conditional PUT, synchronous, inside the decision budget) holding the
origin prior and attempt 1's applied request. If that write fails, the
attempt is not reduced. The record is the ladder lease and is what later
attempts read; it is never looked up by listing decisions.

**Escalate-on-retry and `AttemptContext`.** Orchestrator integrations publish
the current attempt to the processes that submit work:

```rust
pub struct AttemptContext {
    pub invocation_id: String,                   // stable across attempts of one task instance
    pub attempt: u32,                            // 1-based
    pub max_attempts: Option<u32>,               // None: unknown → no ladder
}
```

Attempt records are written before **every** attempt rightsizer changes, up
or down, and one per member for groups (each Airflow map index, each Metaflow
foreach split), holding the group's decision.

In Python it is the `rs.attempt_context` context variable, mirrored into the
`RIGHTSIZER_ATTEMPT` environment variable (JSON) so subprocesses that submit
(Metaflow's `batch step`, Airflow's task runner) see it (orchestrators.md §1).
On attempt k ≥ 2:

- the attempt record is read; if it is missing or unreadable, the attempt runs
  at the larger of the origin prior and a fresh decision with reductions
  disabled (never at a fresh, possibly reduced, decision, and never below an
  increase attempt 1 may have had);
- if attempt 1 was below the origin prior, the request is step
  `min(k − 1, max_steps)` of attempt 1's decision, or a fresh decision if
  that is larger;
- otherwise the fresh decision, never below attempt 1's request.

A retry after a reduced attempt always moves up, whatever the failure was, so
no outcome lookup is on the critical path. The ladder counts only when
`max_attempts` is known and `max_attempts − 1 ≥ max_steps`. Attempts beyond
`max_attempts` (a cleared and rerun task) stay at the last step.

Each ladder step multiplies memory by `memory_factor`. On the GPU axis a step
moves to the next shape with more GPU memory (`tuner:bench_gpu/grid.py:92`)
only within `allowed_shapes` when GPU type changes are opted in; otherwise GPU
is held and a GPU OOM is not escalated. The last step is raised to at least
`final_floor` = max(origin prior, largest observed success). If `final_floor`
exceeds the ceiling, S3(a) does not hold.

**Independence.** Axis demands are treated as independent given the context.
Estimators that model correlation can express it only through `kind = Action`.

**Expected cost of a candidate** *a* = (memory cell m, CPU cell c, shape s):

- *Per attempt.* Price × resource-hours, under the `PlatformState` price model:
  - `packed`: the dominant share of the node shape;
  - `linear`: per-resource rates;
  - `instance`: the shape's price × instance count (Catalog and Snap
    platforms, §5.8);
  - `usage-floor`: billed on `max(request, use)` per resource-second (Modal).
    The per-attempt cost is `E[max(request, demand)] × rate`. Asking below
    demand saves nothing, so an under-ask only adds failure risk and the
    penalties push the argmin to the demand's upper quantile. `shadow` and
    `audit` savings on such platforms are
    `E[max(prior, D)] − E[max(decision, D)]`, not `prior − decision`.

  On a GPU shape, host CPU and memory up to the per-GPU share are free; beyond
  that share, they strand GPUs and pay for them.
- *Stretch.* `stretch(c) = E[max(1, cpu_mean / c)]` over the `cpu_mean`
  distribution. On GPU shapes the stretch also multiplies GPU-hours.
- *Timeout.* If `stretch × duration > walltime`, the attempt is a timeout
  failure, priced like an OOM.
- *A failing attempt* burns `max(t_peak_frac, 0.05) × stretch × duration` plus
  `attempt_overhead` (0.02 of the nominal run).
- *Penalties.*
  - `oom_penalty × oracle_cost` for every failed attempt;
  - `delay_penalty × oracle_hourly_price × (total_hours − oracle_hours)` for
    lateness;
  - when memory is not enforced, a neighbour-risk premium:
    `neighbour_penalty × P(demand > request) × oracle_cost`.

  Here `oracle_cost` is the cost of the cheapest candidate that succeeds on its
  first attempt with stretch 1, for the demand's median scenario.
- *Missing or needless GPU.* A missing GPU costs `no_gpu_fail_hours`; a needless
  GPU costs its price.

**Computation.** The retry process is a Markov chain over ladder states:
memory step × GPU step, at most (max_steps+1)² states, with same-size retries
folded in as a geometric term. Each axis's per-cell failure probability
(1 − CDF) and the per-CPU-cell stretch are precomputed once per decision.
Expected cost is then closed-form per candidate, at O(|candidates| ·
(max_steps+1)²). Candidates are first pruned to the feasible set (§7.4),
typically a few hundred. The budget is under 5 ms per decision on `rs-grid-v1`
(a benchmark in CI). The tuner's scorer simulates *known* demands; rightsizer's
M0 parity test compares the two on the tuner's k8s-2 rows.

### 7.3 Risk profiles

| profile | `oom_penalty` | `delay_penalty` | `neighbour_penalty` |
|---|---|---|---|
| `lean` | 1 | 1 | 1 |
| `balanced` (default) | 4 | 1 | 2 |
| `averse` | 16 | 1 | 4 |

`oom_penalty` is in multiples of `oracle_cost`, the tuner's unit. Integrations
with no ladder and `same_size = 0` default to `averse`.

### 7.4 Constraints

The feasible set is defined before the argmin:

```rust
pub struct Constraints {
    pub pinned: AxisSet,                          // user pins and explicit call-site resources
    pub held: Vec<(Axis, HoldReason)>,            // S2/S3 and platform holds (computed)
    pub floor: Requests,                          // platform minimums ∨ size-scaled floor
    pub ceiling: Requests,                        // largest shape ∧ quota ∧ user ceiling
    pub allowed_shapes: Option<Vec<GpuShape>>,
}
```

In order of authority:

1. **Pinned axes** stay at their pinned values.
2. **Held axes** stay at the prior. These include:
   - GPU count for distributed units, and GPU type unless allowed;
   - QoS class, and limits that S2 forbids changing;
   - ephemeral storage and shm (no grid in v1);
   - any incompressible-axis reduction S3 does not permit.

   Axes in `Prior.defaulted` are **not pins**, so they may be raised freely. S3
   still governs reductions below them.
3. **Ceiling (S4).** The largest node shape, intersected with the remaining quota
   (via the adapter's reservation ledger, kubernetes.md §3.4) and any user ceiling.
   The total over the pod, job or group is checked, not just each unit.
4. **Floor.** Platform minimums, plus the **size-scaled floor**: the largest
   measured peak among the last K (5) successful same-digest runs, × `scale`,
   × that record's S3(b) headroom (§2.3: by resolution and digest).
5. **Trust region** (memory and GPU memory only). At most `max_step_down`
   (default 2) cells below the **anchor**. The anchor is the smallest
   `requested` among the last K (5) same-digest `Succeeded` records with trust
   ≥ Authenticated, or the prior if there are none. Shadow-mode runs execute at
   the prior, so they never move the anchor. Under enforce, convergence from a
   padded prior takes about ⌈cells / 2⌉ enforced runs.

**`scale`.** There is one definition, used by the floor, S3(b), `rules-sized`
and deploy-time sizing:

```
b_now  = total input bytes of this invocation; for input-independent decisions (deploy-time, Slurm table),
         the p90 of the last K same-key invocations' input bytes
b_hist = total input bytes of the history run being scaled
scale  = max(1, b_now / b_hist)   if both known
       = 1                        otherwise
```

Scaling never goes below 1. Fixed costs, such as model weights, don't shrink
with inputs.

**Floor above ceiling.** The ceiling wins for schedulability, and the decision is
flagged `infeasible_floor`. The unit is never sent back to a prior that history
shows OOMing; it gets the ceiling.

**Limits policy (S2).**

- Memory:
  - the prior has a limit: the new limit preserves the prior's limit/request
    ratio, and is ≥ the new request;
  - the prior has no limit: none is added.
- CPU: the same rule. Where a LimitRange requires a CPU limit, the adapter
  supplies the required form.
- QoS: a Guaranteed prior keeps limits = requests on every axis. Whole CPUs are
  kept if the prior used whole CPUs, for the static CPU manager.

**No prior** (BestEffort pods, Ray defaults). Recommend-only by default.
`enforce_without_prior = true` opts in; even then, no limit is added.

### 7.5 Failure semantics

All errors follow S1. All rightsizer-caused failures follow S5. Every held axis
and every fallback to the prior is recorded with its reason.

### 7.6 Exploration

Off by default. With `exploration = ε` (≤ 0.05), incompressible axes go one cell
*above* the decision with probability ε (never below). The propensity is logged.

### 7.7 Advisories (recommend-only, every platform)

- **GPU:**
  - a lower count would fit (busiest-device need);
  - a cheaper shape would fit;
  - mean utilization was below 30% over the last K runs;
  - **predicted GPU OOM**: P(need > device memory) > 0.2 at the prior.
- **Walltime:** a timeout is predicted (P(duration × stretch > walltime) > 0.1),
  or the walltime is over 10× the p95 duration. Enforcement may only *raise* a
  walltime that is predicted to be hit, and only where the integration allows it
  (Flyte `timeout`, K8s `activeDeadlineSeconds`; Slurm `--time` is
  recommend-only in v1).
- **Prior quality:** the prior is more than 4× the size-scaled p95 demand.

## 8. History

### 8.1 Stores

| backend | use |
|---|---|
| `object-store` | default for Flyte, Ray, Slurm and multi-host use |
| `postgres` | the K8s webhook and watcher deployment; any site that prefers a database |
| `sqlite` | single-user CLI on one machine; refused on network filesystems |

`HistoryStore` (async trait): `put_run`, `put_decision`, `runs(key, limit)`,
`decisions(filter)`, `breaker_get/trip/reset`, `control_get` (for
`mode_ceiling`), `compact`.

**Object-store layout and lifecycle:**

```
{prefix}/v1/runs/{tenant}/{key-hash}/{ulid}.json        immutable shards (one per record)
{prefix}/v1/decisions/{tenant}/{date}/{ulid}.json
{prefix}/v1/attempts/{tenant}/{key-hash}/{invocation-hash}.json   attempt records and ladder leases (§7.2); expire after 7 d
{prefix}/v1/latest/{tenant}/{key-hash}.json             pointer: last K records inline + watermark ULID
{prefix}/v1/breakers/{scope}.json
{prefix}/v1/control/mode.json
{prefix}/v1/snapshots/{tenant}/{date}.parquet           compacted, for analytics and export
```

`{tenant}` in these paths is the workload key's `{platform}://{tenant}` with `://`
replaced by `/` (for example `flyte/acme/ml/development`), so platforms never
share a prefix; `{key-hash}` is the first 32 hex characters of the SHA-256 of
the full key string.

- **Reads.** GET the `latest` pointer, then LIST only shards newer than its
  watermark (`start-after`). That is one GET plus one small LIST per key, and
  the result is cached per process (§8.4).
- **Compaction.** `rsz history compact` runs as a **singleton**: a
  scheduled job (CronJob, systemd timer or Flyte schedule) holding a lease
  object created with a conditional PUT (`If-None-Match: *`), with expiry. It
  does three things:
  - rewrites `latest` pointers, with conditional PUTs on the pointer's ETag;
  - writes daily Parquet snapshots;
  - deletes shards only when they are older than both the pointer watermark and
    a 24 h grace period, which exceeds any reader's lifetime.
- **Retention.** `keep_last = 64` records per key in pointers. Shards and
  snapshots are kept for `max_age = 90d`. Decision shards are kept for 30 d,
  then only in snapshots.
- **Load.** At 1M invocations a day the store sees about 2–3M PUTs a day. The
  prefix is partitioned by tenant and key hash, so request-rate limits per
  prefix are not hit.
- **Metrics.** `rightsizer_store_compaction_lag_seconds`,
  `rightsizer_store_shards`, `rightsizer_store_errors_total`.

**Control objects are operator-written.** `control/` and breaker *resets*
require operator credentials (a separate prefix policy or postgres role).
Clients can only create **trip** objects (`breakers/{scope}/trips/{ulid}.json`,
`If-None-Match: *`); a breaker is open while any trip is newer than its latest
operator-written reset. A tenant with shared credentials can therefore trip a
breaker (which only makes rightsizer more conservative) but never reset one or
raise `mode_ceiling`.

**Enforce needs a durable, shared store.** A store that is local to an ephemeral
or per-host location demotes enforce to recommend, with a warning.

### 8.2 Trust

| level | meaning | used for |
|---|---|---|
| `Verified` | written by a trusted component: the watcher, an importer reading platform accounting (`sacct`, Prometheus, `flyte.remote`), **when the record's tenant comes from the platform's authenticated identity** (namespace, `submit_uid`, CloudTrail `userIdentity`, Modal workspace) | everything |
| `Authenticated` | written with credentials whose write access is limited to that tenant's prefix (per-tenant IAM on the object-store prefix, or a per-tenant postgres role) | everything |
| `Unverified` | anything else: self-reports with shared credentials | stored and shown; excluded from floors, trust-region anchors and S3(b), unless the operator sets `history.allow_unverified_for_reductions = true` |

An importer that can attribute a record to a tenant only through
caller-chosen data (job names, tags, an account-wide default) writes it as
`Unverified`: any principal in the account could otherwise plant low peaks
under another team's key (aws.md §8).

Cross-tenant poisoning needs write access to another tenant's prefix, which
per-tenant credentials prevent. Within a tenant, a workload's history is as
trustworthy as the tenant's own code. With shared credentials (common in open
source Flyte), self-reports are `Unverified`. Flyte enforce then relies on the
escalation ladder (S3a) and on `flyte.remote` backfill, until the operator either
scopes credentials or opts in (flyte.md §8).

### 8.3 Sources

Each platform document gives its aggregation formulas:

- kubernetes.md §6: watcher and node agent; Prometheus with owner joins.
- slurm.md §5: per-node demand from `sacct`.
- flyte.md §8: child-side cgroup measurement; parent-side status;
  `flyte.remote` backfill.
- ray.md §6.
- aws.md §5, modal.md §5, launchers.md §1.5 and §2.3, orchestrators.md §2.6
  and §3.5, managed-ml.md.

All records are deduplicated on `(key, invocation_id, attempt, source)`.

### 8.4 Availability

Each process keeps a per-key history cache (an LRU, TTL `history_cache_ttl`,
default 10 min).

- **Store unreachable.** Decisions use cached history up to the TTL. After that,
  and for keys never cached, the effective mode demotes to `recommend`. Writes
  are buffered in memory (bounded, 10k records) and retried. Overflow is dropped
  and counted.
- **Breaker or control state unreadable.** Treated as open for reductions.

### 8.5 Workload identity

- Tenants are derived as in §4. Users set only suffixes: the
  `rightsizer.io/workload-key` annotation, `#RIGHTSIZER key=`, or `key=` in
  Python.
- Code changes keep the key. Records carry the code digest, and estimators
  prefer same-digest history.
- History is **per cluster** unless `history.share = ["cluster-a", "cluster-b"]`
  merges keys across clusters. Records always carry `cluster`.
- A Flyte task sized by the K8s webhook is keyed `flyte://…`, derived from Flyte's
  pod labels, so it shares history with Flyte-side sizing (kubernetes.md §2).

## 9. Feature extraction (`rightsizer-features`)

- **Readers (M0):** Parquet footer (range read), NPY header, safetensors header
  (shapes → parameter count), CSV first line, JSONL first record, directory
  listings (count + total bytes, ≤ 10,000 entries). **(M3):** Arrow IPC schema,
  Hugging Face model refs (`hf://`: `config.json` plus the safetensors index).
- **I/O.** The `object_store` crate (S3, GCS, Azure, HTTP(S), local), with
  per-input byte budgets and a per-context time budget: 1 s by default, with
  integrations passing tighter ones. HTTP(S) readers refuse private, loopback and
  link-local addresses unless allowlisted. Each integration states whose
  credentials read inputs.
- **Code features:** the tuner's token set (`tuner:bench/features.py:23`), plus
  AI tokens:
  - model loading: `from_pretrained`, `torch_dtype`, `bfloat16`, `load_in_4bit`, `device_map`;
  - training: `per_device_train_batch_size`, `gradient_accumulation_steps`, `gradient_checkpointing`;
  - inference: `vllm`, `max_model_len`, `tensor_parallel_size`;
  - distribution: `deepspeed`, `fsdp`, `torchrun`, `nproc_per_node`.

  Also numeric literals near those tokens, and code length.
- **Versioning.** The schema is `features-v1`, recorded in provenance. Models
  trained on another version get it, or a `409`.

## 10. Packaging

### 10.1 Crates

```
rightsizer-core        types, units, grids, normalization, cost model, constraints, decision, breakers logic
rightsizer-features    readers, code tokens, feature vector
rightsizer-estimators  rules-sized, history-peak, prior, gbt (trees/1, novelty/1), remote client, chain/pool/gate
rightsizer-history     HistoryStore; object-store, sqlite; postgres behind a feature flag
rightsizer-plugin      adapter traits, plugin protocol types and client (no server)
rightsizer-k8s         K8s adapter, webhook, watcher, Prometheus importer (kube-rs)
rightsizer-slurm       Slurm adapter, sbatch wrapper, tokens, sacct importer
rightsizer-cloud       shape catalogs and price importers; AWS, GCP, Azure, Modal, Databricks history importers (features: aws, gcp, azure, modal, databricks)
rightsizer-cli         `rsz`: CLI, daemon (plugin protocol server), all adapters
rightsizer-py          PyO3 module: core + features + estimators + history(object-store, sqlite) — no kube-rs, no postgres
```

The Ray, Modal, SkyPilot, dstack, Metaflow and Airflow adapters are Python
(they run inside those platforms' Python processes) and call `_core` for
decisions. The AWS adapter's decision logic is Rust (`rightsizer-cloud`), with
the botocore hook in Python. KubeRay is part of `rightsizer-k8s`.

### 10.2 Python distributions

| distribution | contents | wheels |
|---|---|---|
| `rightsizer-runtime` | pure Python: policy model, measurement (cgroup, NVML via `pynvml` if present), `RunRecord` writer (through `flyte.storage` or `fsspec`), Flyte and Ray child-side code | `py3-none-any` |
| `rightsizer` | depends on `rightsizer-runtime`; native module `rightsizer._core` (decisions, features, estimators, store reads); `rightsizer.flyte`, `rightsizer.ray`, `rightsizer.serve`, `rightsizer.export` | abi3 for CPython ≥ 3.10: manylinux2014 x86_64 and aarch64, macOS arm64 and x86_64. Later: musllinux, Windows, free-threaded `cp313t` |
| `rightsizer-cli` | the `rsz` binary as a wheel (maturin `bindings = "bin"`) | per platform |
| `metaflow-rightsizer` | the `metaflow_extensions.rightsizer` namespace package (Metaflow requires extensions to be their own distribution); depends on `rightsizer` | `py3-none-any` |

Integration modules ship inside `rightsizer` (`rightsizer.aws`, `.modal`,
`.skypilot`, `.dstack`, `.airflow`, `.gcp`, `.azureml`), with extras that pull
in only the client libraries they need (`rightsizer[aws]` → `boto3`,
`rightsizer[modal]` → `modal`, …). `rightsizer-runtime` carries the
`rightsizer_aws.pth` start-up hook, which does nothing unless
`RIGHTSIZER_AWS_INSTALL=1` (aws.md §2.1).

- Any process that **makes decisions** needs `rightsizer`, with a wheel for its
  platform. In Flyte that is every task that calls a rightsizable task. Leaf
  tasks need only `rightsizer-runtime`.
- `rightsizer/__init__` imports `_core` lazily (module `__getattr__`), so an
  image lacking a matching wheel fails at the first decision, not at import.
  That failure is caught and logged under S1, and the call runs at the prior.

### 10.3 Python runtime details

- One process-wide tokio runtime is created lazily and re-created after `fork()`,
  guarded by a pid check.
- `rs.recommend` (sync) releases the GIL while Rust works. `rs.arecommend`
  returns an awaitable via `pyo3-async-runtimes` `future_into_py`.
- Python estimators: `class E(rs.Estimator)`.
  - Sync `estimate` runs on a blocking pool, with the GIL reacquired.
  - `async def estimate` runs on the caller's loop.

  Register with `rs.register_estimator`.
- Rust store reads and Python store writes use the same credentials, from the
  environment or from the integration's storage configuration (the Python layer
  passes Flyte's resolved storage options into Rust).

```python
import rightsizer as rs

d = rs.recommend(rs.Context(
    key="llama-ft",
    prior=rs.Resources(cpu=8, memory="64Gi", gpu="A100-80G:1"),   # canonical accelerator ids (§5.1.1)
    inputs=[rs.Input.from_uri("s3://bucket/train.parquet")],
    code=rs.Code.from_callable(train_fn),
))
print(d.applied.requests.memory, d.held, d.explanation)
rs.outcome(d, status="succeeded", memory_peak="41Gi")
```

## 11. Privacy

Remote estimators receive only what `[privacy]` allows. In-process estimators
see everything.

| field | default (remote) | override |
|---|---|---|
| code digest, code tokens | sent | — |
| full source | **not sent** | `send_source = true` |
| input URIs | never; salted hash only | — |
| column names, string scalars, labels | **not sent**; counts and kinds only | `send = ["columns", "labels:team"]` |
| numeric scalars, sizes, shapes, dtypes, parameter counts | sent | `redact = [...]` |
| history (peaks, statuses, sizes) | sent | `history_limit` |

## 12. CLI (`rsz`)

One static binary (musl on Linux), also published as the `rightsizer-cli` wheel
and installed as `kubectl-rsz`.

### 12.1 Commands

```
rsz recommend  [-p PLATFORM] FILE|-                decisions for a submission (mode=recommend)
rsz apply      FILE|- [-o FILE] [--diff] [--watch] rewrite (mode=enforce under §2.3); --watch resubmits on OOM (Ladder)
rsz explain    FILE|- | --decision ID              distributions, cost table, constraints, holds, advisories
rsz shadow     [--tenant T|-n NS|--key K] [--since 14d] [--format html]   savings / would-fail / readiness (§12.2)
rsz history    import k8s|slurm|flyte|ray|aws|modal|dstack|vertex|azureml|databricks [...] | show KEY | export --parquet | compact
rsz audit      PLATFORM [--since 30d] [--format text|json|html]    read-only waste and risk report from platform history (§12.3)
rsz suggest    PATH... [--write | --pr]             resource edits to code and config from history (§12.4)
rsz mcp                                             MCP server for coding agents (§12.5)
rsz outcome    KEY --status S [--memory-peak Q]    manual outcome record
rsz breaker    status | trip [--fleet|KEY] | reset [KEY]
rsz control    mode-ceiling [off|observe|recommend|enforce]
rsz serve      [--socket PATH | --listen ADDR]     daemon (plugins.md §3)
rsz webhook    serve | manifests | rotate-key       kubernetes.md
rsz k8s        restore -f FILE|-                    reverse webhook mutations from the signed prior annotation
rsz slurm      sbatch [--watch] ARGS... | install [--disable] | keygen | rotate-key
rsz ray        submit -- ENTRYPOINT
rsz aws        run -- CMD | batch submit [--wait] ARGS... | catalog refresh | reconcile
rsz metaflow   reconcile
rsz databricks reconcile | catalog refresh          (M7)
rsz vertex     reconcile | catalog refresh          (M7)
rsz <platform> catalog refresh                      rebuild a shape catalog from provider APIs (§5.8)
rsz flyte      run ... | deploy [--restore] ... | reconcile
rsz plugin     list | check NAME
rsz config     show | validate
```

- Output is `text` (default), `json` or `yaml`. Manifests are edited with
  **minimal text edits** that preserve comments and ordering (plugins.md `Patch`).
- Exit codes:
  - 0: success, including "kept the prior";
  - 2: config error;
  - 3: input parse error;
  - 4: a wrapped submitter failed (its own exit code is printed).
- Config file: the first `rightsizer.toml` found among `$RIGHTSIZER_CONFIG`,
  `./rightsizer.toml`, `$XDG_CONFIG_HOME/rightsizer/config.toml` and
  `/etc/rightsizer/config.toml`. Any key can be overridden with an env var
  `RIGHTSIZER__SECTION__KEY`.

### 12.2 `rsz shadow`

For each key, the report gives:

- decisions vs priors;
- **would-save**: resource-hours and dollars, with a 95% bootstrap interval over
  runs;
- **would-fail**: runs whose measured peak exceeds the decision's request. This
  is censoring-aware: a run whose peak was only `AtLeast` at the prior is counted
  as "unknown", not "fit";
- held axes, with reasons and counts;
- advisories;
- a **readiness** verdict: `ready` (S3(b) holds now), `ladder-only`
  (enforceable only with escalation), or `not-ready` (needs N more measured runs).

`--format html` writes a single shareable page.

```
$ rsz shadow --tenant flyte://acme/ml/development --since 14d
KEY                       RUNS  PRIOR mem   DECIDED mem (p50)  WOULD SAVE           WOULD FAIL  READINESS
etl.clean                  412  16Gi        3Gi                  41.2k GiB·h ($1.9k)  0 / 412     ready
train.finetune              38  64Gi        48Gi                 2.1k GiB·h ($310)    1 / 38 (?)  ladder-only
embed.batch               9,870 8Gi         4Gi                  88.0k GiB·h ($4.0k)  0 / 9,870   ready
```

### 12.3 `rsz audit`

The first thing a new user runs, on any platform, with read-only credentials
and nothing installed on the platform:

```
$ rsz audit aws --since 30d --format html
```

- It runs the platform's importer (`history import`) into a temporary local
  store, then replays decisions **counterfactually**: each past run is decided
  as of its start time, using only runs that **completed** before it started,
  by the in-process estimators (no data leaves the machine unless a remote
  estimator is configured, §11).
- The replay is a **recommend-mode steady state**, not a simulation of
  enforcement: history was produced at the prior, so the trust-region anchor
  stays at the prior and convergence below it is not credited. Breakers are
  simulated (a would-fail run stops later would-saves for that key until the
  recovery rule would have reset it), ladder retries are priced, and savings
  are reported as an interval. On the same history snapshot and
  configuration, audit and `shadow` give the same decisions.
- It reports what `shadow` reports (§12.2): would-save in dollars at the
  platform's prices, would-fail, holds, advisories (GPU type and count,
  walltime, prior quality) and readiness per key, plus the **integration tier**
  each key could reach on that platform (plugins.md §1.1).
- Platforms: everything with an importer (`history import` list in §12.1);
  for platforms without per-run memory metrics, the report covers status,
  duration and GPU advisories only and says so.

### 12.4 `rsz suggest`

Resource edits proposed as code changes, for platforms where resources live
in code or config and for teams that want human review before anything
changes:

- **Finds literal resource values** with real parsers and edits them as
  minimal text edits (plugins.md `Patch::TextEdits`):
  - Python decorators and calls: Flyte `Resources`/`TaskEnvironment`, Modal
    `@app.function`, Ray `@ray.remote`, Metaflow `@resources`/`@kubernetes`/`@batch`;
  - YAML: Kubernetes manifests and Helm values, SkyPilot and dstack tasks,
    Databricks bundles, Azure ML jobs, compiled KFP IR;
  - JSON: SageMaker Pipelines definitions, Step Functions, CloudFormation.
  Non-literal values (variables, parameters, templates) are never edited; they
  get a comment in the report instead.
- **Matches each site to a workload key** through the platform's key rules
  (for example `flow/step`, `{app}.{function}`, a SageMaker `base_job_name`).
- **Proposes** increases where history shows OOMs or predictions exceed the
  prior, and reductions only where S3(b) holds, with `scale` from the p90 of
  recent input sizes (§7.4, input-independent decisions). A merged edit
  outlives breakers and the mode ceiling, so:
  - reductions are proposed only while `mode_ceiling` is `enforce` and no
    breaker covers the key;
  - each edited value carries a marker comment (or, in JSON, a sibling
    annotation where the format allows one) with the origin prior and the
    decision id, so attribution stays relative to what the author wrote
    (§2.3 S5) and later runs never treat the edit as the author's prior;
  - when a breaker opens for a key with a merged reduction, the next scheduled
    run opens a **revert** pull request restoring the origin prior.
- `--pr` opens a pull request with the explanation table; the
  `rightsizer-action` GitHub Action runs it on a schedule.

### 12.5 MCP server

`rsz mcp` serves the Model Context Protocol over stdio so coding agents
can size workloads while they write them. Tools: `estimate` (code, input URIs
or sizes, platform → resources and explanation), `explain` (a decision id or
key), `audit` (platform, window → summary). It is read-only: it never writes
history and never enforces, and it uses the local configuration and store
credentials.

Agents write the answer into code, where it persists, so `estimate` returns
what **enforce** would apply (the `applied` spec with S3 holds), not
`recommended`. Reductions that S3 does not yet allow are returned separately,
marked "not earned: do not hard-code".

## 13. Operations

### 13.1 Metrics and alerts

Every component exports these metrics (Prometheus):

| metric | labels |
|---|---|
| `rightsizer_decisions_total` | mode, source, platform |
| `rightsizer_decision_latency_seconds` | platform |
| `rightsizer_budget_expired_total` | platform |
| `rightsizer_held_total` | axis, reason |
| `rightsizer_clamped_total` | reason |
| `rightsizer_errors_total` | stage, reason |
| `rightsizer_caused_failures_total` | platform |
| `rightsizer_outcome_join_ratio` | platform |
| `rightsizer_saved_resource_hours_total` | axis |
| `rightsizer_breaker_open` | scope |
| `rightsizer_estimator_circuit_open` | estimator |
| `rightsizer_store_compaction_lag_seconds` | — |
| `rightsizer_watcher_lag_seconds` | — |

`rightsizer_outcome_join_ratio` is decisions with an outcome over decisions.
Losing outcomes means losing safety, so it is alerted on.

A `PrometheusRule` and a Grafana dashboard ship in `deploy/`. They alert on:

- the fast-burn breaker;
- webhook fail-open, measured from the apiserver's own
  `apiserver_admission_webhook_fail_open_count`;
- join ratio below 0.9;
- compaction lag above 6 h;
- an estimator circuit stuck open.

### 13.2 Versions and upgrades

- **N/N-1 contract.** Every reader accepts the current and the previous major
  version of every artifact. Writers write the version set by
  `history.write_version`. The operator raises it fleet-wide only after every
  reader has been upgraded.
- **Upgrade order:** readers (webhook, daemon, Python clients), then writers,
  then raise `write_version`.
- **Keys** (K8s prior signing, Slurm tokens) carry a key id. Two keys are active
  during rotation (`rsz slurm rotate-key`, `rsz webhook rotate-key`).

### 13.3 Turning it off and reverting

- **Off everywhere:** `rsz control mode-ceiling recommend` (or `off`),
  read by every client within 60 s.
- **K8s:** `rsz k8s restore` rewrites objects from the signed
  `rightsizer.io/prior` annotation. New objects are unaffected once the mode
  ceiling drops.
- **Flyte:** `rsz flyte deploy --restore` redeploys from source with
  sizing disabled. Deploy-time *reductions* exist only where
  `rsz flyte reconcile` is scheduled (flyte.md §6.3), which redeploys
  affected tasks at the prior within 10 minutes of a breaker opening or
  `mode_ceiling` dropping below `enforce`; without it, deploy-time sizing is
  increases-only, so the kill switch always holds.
- **Slurm:** `rsz slurm install --disable` removes the table and
  short-circuits both Lua scripts.
- **Client-side hooks** (AWS, Modal, SkyPilot, dstack, Airflow, Metaflow) read
  `mode_ceiling` like every other client. Edits merged from `rsz suggest`
  are ordinary code changes and are reverted with the version control system.

## 14. Security

1. **History poisoning.** Mitigated by server-derived tenants (§4, §8.5) and trust
   levels (§8.2).
2. **Confused deputy on input reads.** Inputs are read with the submitter's
   credentials, or from an admin allowlist (kubernetes.md §4.3).
3. **Forged enforcement tokens.** Mitigated by HMAC with key ids (slurm.md §4).
4. **SSRF** via input URIs (§9).
5. **A broken or compromised estimator** can cause waste and mass OOMs.
   Mitigated by S3, floors, trust regions, and the per-version and fast-burn
   breakers (S5).
6. **Plugin code execution.** Server modes load only configured absolute paths,
   optionally pinned by sha256 (plugins.md §3.4).
7. **Webhook credentials.** Least-privilege RBAC (kubernetes.md §7).

Every decision and patch is logged with its decision id.

### 14.1 Data for the model factory

`rsz history export --parquet` writes the tuner's corpus columns from
production outcomes and decision logs: inputs, code tokens, requests, censored
peaks, statuses and propensities. The model factory trains on them, and the
tuner's scorer replays rightsizer's decisions.

## 15. Milestones

| | scope | exit criterion |
|---|---|---|
| **M0** core | types, units, grids, normalization, cost model, constraints, `rules-sized` / `history-peak` / `prior`, trees/1 evaluator and exporters, object-store and sqlite stores (compaction, pointers), breakers, mode ceiling, CLI `recommend` / `explain` / `shadow` on JSON contexts | rightsizer's decision rule, fed the tuner's oracle demands, reproduces the tuner's scorer costs on the k8s-2 rows within 1%; `rules-sized` reproduces the tuner's `rules-sized` regret within 1 point; < 5 ms per decision |
| **A** adoption (after M0, in parallel with M1) | the importer and catalog half of `rightsizer-cloud` (AWS and Modal catalogs, prices, importers); `rsz audit` for Kubernetes (Prometheus), Slurm (`sacct`), AWS (SageMaker, Batch, CloudWatch) and Modal; HTML report; `rsz suggest` for Kubernetes YAML and Flyte/Ray literals (other key rules arrive with their milestones); `rsz mcp` | a 30-day audit of a real AWS account finishes in < 10 min with read-only credentials; counterfactual decisions equal `shadow`'s on the same history |
| **M1** Flyte | `rightsizer-runtime`, `rightsizer` wheels; Flyte task, action, run and environment levels; child-side measurement; `flyte.remote` backfill; escalation; deploy-time sizing and `reconcile`; the request-only-memory integration test (§16, open question 5) | demo workflows in `enforce` with 0 rightsizer-caused failures over 1,000 invocations; 10k-way `flyte.map` overhead < 2 ms per call once memoized |
| **M2** K8s | CLI, webhook (Pods, Jobs, JobSets, PyTorchJob), watcher, Prometheus importer, postgres store, Helm chart, alerts, `k8s restore` | 2 weeks shadow on a real cluster with join ratio ≥ 0.95; webhook p99 < 100 ms |
| **M3** plugins | plugin protocol and daemon; Slurm (wrapper, `cli_filter`, `job_submit` token verification); Ray (functions, actors, Ray Data, Jobs); KubeRay, TrainJob, Volcano; node agent; Arrow/HF readers; model-factory models served through vLLM/SGLang/transformers (models.md) | Slurm test cluster with HMAC verification and rotation; Ray 10k-task fan-out < 5 ms per task |
| **M4** models | Estimator Protocol reference server; tuner models served; pool and gate with novelty/1; GPU type changes (opt-in); Slurm decision table | the gate beats `rules-sized` on the tuner's unseen split under rightsizer's decoder |
| **M5** managed clouds I | AWS botocore hook, SageMaker (training, processing, transform, Pipelines deploy-time) and Batch (EC2, Fargate, EKS) adapters, caller ladders; `rightsizer_aws.pth` start-up hook, `rsz aws reconcile`; Modal proxy and `measure` | SDK v2, SDK v3 and boto3 fixtures sized identically; 0 rightsizer-caused failures over 500 enforced SageMaker and Batch jobs; a 10k-input Modal `.map` stays within the variant budget |
| **M6** launchers and orchestrators | `AttemptContext` and escalate-on-retry; SkyPilot policy; dstack plugin and importer; `metaflow-rightsizer` and `rsz metaflow reconcile`; Airflow hooks and listener | fault injection (raise, hang, missing `_core`) leaves every SkyPilot, dstack and Airflow submission unchanged; an orchestrator retry after a reduced attempt escalates |
| **M7** managed clouds II | Vertex AI custom jobs and KFP-on-Vertex (snap function derived from observed machine types), Azure ML (including Kubernetes instance types), Databricks single-node and GPU jobs; native KFP, Dagster and Prefect integrations | each service: audit on a real account, then 2 weeks of recommend with join ratio ≥ 0.9 |

## 16. Open questions

1. GPU count changes for distributed jobs need coherent rewrites of world size
   (torchrun args, replicas, sharding config). Out of scope for v1.
2. DRA GPU claims: recommend-only. Rewriting `ResourceClaimTemplate`s needs a
   device-class catalog.
3. Exploration in production: is the learning value worth the waste?
4. Per-site calibration of `rules-sized` constants from local history, and how to
   version it.
5. ~~How the Flyte v2 backend treats a request-only memory spec~~ **Settled
   (M1, Flyte v2 devbox, SDK 2.13.6):** the SDK serializes a scalar
   `memory=` as a request only, and the backend sets limit = request on the
   pod, so scalar memory **is** enforced (an over-use is OOM-killed, the
   unit's own failure). Flyte therefore uses `memory_enforced = true` for
   scalar and `(req, lim)` memory alike, and both get ladders (S3a). The
   switch `[flyte] scalar_memory_enforced` (default `true`) is for
   deployments whose platform defaults leave the limit unset (recheck Union
   deployments); with `false`, scalar-memory tasks get S3(b) reductions only
   (flyte.md §4.2).
6. **Modal ladder.** A caller ladder needs a per-input OOM status on the
   client; today only the CLI-side `FunctionCallFetch` exposes it
   (modal.md §6).
7. **Vertex AI Pipelines machine matching** is undocumented; M7 derives it
   from observed machine types (managed-ml.md §1.2).
8. **Instance family moves** change CPU architecture, network and storage
   (for example x86 to Graviton). v1 moves only within configured families;
   whether a model can learn per-family speed factors is open.
9. **Upstream asks** that would add ladders where there are none: resize on
   recovery (SkyPilot managed jobs), a distinct OOM exception (Modal), retry
   on algorithm errors with a different instance type (SageMaker), and a
   memory-aware `retryStrategy` (AWS Batch).
