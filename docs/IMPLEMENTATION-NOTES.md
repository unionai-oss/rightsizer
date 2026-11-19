# Implementation notes

## rightsizer-runtime

Pure-Python package at `python/rightsizer-runtime/` (`rightsizer_runtime`,
hatchling, Python ≥ 3.10, zero required deps; extras `fsspec`, `nvml`).
`import rightsizer_runtime` loads only the standard library; `fsspec` and
`pynvml` are imported lazily where used.

Modules: `wire` (wire dataclasses, ULID, RFC 3339), `policy` (definition /
invocation policy, contextvar stack, resolution), `measure` (sampler scope,
cgroup v1/v2, rusage, RSS, NVML, censoring), `record` (shard writer), `env`
(env-var contracts), `child` (`measured_execution`).

Tests: `cd python/rightsizer-runtime && uv run --python 3.12 --with pytest --with fsspec pytest -q`.

### Wire format: aligned to `rightsizer-core` (cross-checked against `crates/rightsizer-core/src/types/`)

Matched: field names and order of `RunRecord`, including Rust's extension
`origin_prior: Option<Requests>`; `RunStatus`, `Confidence`, `Censor` and
`Trust` spellings; `OutcomeSource` as internally tagged `{"kind": "self_report" | "cgroup" | "prometheus" | "sacct" | "flyte_remote" | "ray_state" | "watcher"}` and
`{"kind": "importer", "platform": …}`; `InputFormat`, including `arrow_ipc`;
`ColumnsInfo {total, numeric, string, nested, other, names?}`; `ScalarValue`
adjacently tagged `{"kind": "int"|"float"|"bool"|"str", "value": …}`;
`GpuShape {accel, count}` with the catalog's ids; `Requests`/`Limits` with
`skip_serializing_if = None`; `ResourceSpec {requests, limits}`, both always
written and both defaulted on read; `Mode` spellings and order.

Python accepts the same serde defaults on read: a missing `v` reads as 1,
`status_confidence` as `observed`, `censor` as `exact`, and `inputs` as `[]`.

Workload keys follow `WorkloadKey::parse`/`Display`: the per-platform
`tenant_arity` table (copied as `wire.TENANT_ARITY`), the explicit `//`
separator, the tenant charset `[A-Za-z0-9._@+=,-]`, and the suffix rules.
`wire.canonical_key` gives Rust's `Display` form.

### Open items to reconcile with Rust

1. **Unknown fields.** Python `from_dict`/`from_json` reject unknown keys by
   default (strict). Pass `allow_unknown=True` to accept them, which matches
   serde's default. `DecisionEnv` and `AttemptContext` read from the
   environment with `allow_unknown=True`. Rust has no `deny_unknown_fields`.
2. **Stricter numeric checks in Python.** `Measurement.value ≥ 0`, all
   values finite, and fractions (`cpu_throttled_frac`, `gpu_util_mean`,
   `t_peak_frac`) in `[0, 1]`. Rust's serde does not check these.
3. **Measurement units.** `memory_peak` and `gpu_memory_peak` are bytes
   (f64); `cpu_mean` and `cpu_peak_window` are **cores**, not millicores.
   `resolution_s` is the sampling interval in seconds; `None` means an exact
   high-water mark (kernel `memory.peak` / `max_usage_in_bytes`).
4. **Timestamps** are written as `YYYY-MM-DDTHH:MM:SS.ffffffZ`, always with
   microseconds. Any fractional precision and any `±HH:MM` offset are
   accepted on read.
5. **`AttemptContext`** is not in Rust yet. Python writes
   `{"v":1,"invocation_id":…,"attempt":…,"max_attempts"?}`, where `attempt`
   ≥ 1 and `max_attempts` is omitted when unknown.
6. **`RIGHTSIZER_DECISION` payload** is not in Rust yet:
   `{"v":1,"decision_id":<ULID>,"key":…,"mode":…,"requested"?:ResourceSpec,"measured":bool}`.
   The field names `decision_id` (not `id`) and `requested` are assumptions.
7. **`RIGHTSIZER_DECIDED`** holds a bare ULID string, not JSON.
8. **Policy stack** (`custom_context["rightsizer"]`) is
   `{"v":1,"s":[{"mode"?,"risk"?,"estimator"?},…]}`, outermost first and
   innermost last. `risk` is a profile name or a bare number (an
   `oom_penalty`, flyte.md §3.2). `policy.risk_to_wire` converts it to Rust's
   `RiskProfile` (`"balanced"` or `{"oom_penalty": x}`). The cap is 1 KiB.
   Over the cap, the stack is collapsed to one equivalent entry (each field
   takes its innermost value), and if it is still too large the estimator is
   dropped. Readers reject payloads over 1 KiB.
9. **`RIGHTSIZER__SECTION__KEY`** names are lower-cased and split on `__`,
   with unlimited nesting. Values are parsed TOML-like: `true`/`false`,
   integers, floats, and JSON arrays, objects or quoted strings; anything
   else stays a string. `rightsizer-engine::config` should parse the same
   way, or Python should pass raw strings (`parse_values=False`).

### Object-store layout contract (for `rightsizer-history`)

```
{prefix}/v1/runs/{platform}/{tenant}/{key-hash}/{ulid}.json
```

- **key-hash = `sha256(canonical_key)` as lower-case hex, first 32 chars**,
  i.e. exactly `rightsizer_core::WorkloadKey::hash()`. Pinned test vector:
  `flyte://acme/proj/dev/env.train` → `eef92d58436c80afb2670dc464a4b4f2`.
  *Deviation from the brief, which proposed 16 hex chars: core already
  defines `hash()` as 32, and Rust is the source of truth.*
- **tenant path = the qualified tenant `{platform}://{tenant}` with `://`
  replaced by `/`**: `flyte/acme/proj/dev`, `k8s/prod/ml`, `ray/team-a`. The
  tenant is what `WorkloadKey::parse` extracts. Its charset needs no
  escaping; `.` and `..` segments are rejected.
  *Note:* flyte.md §8.1 describes per-tenant IAM scoping as
  `{prefix}/v1/runs/{org}/{project}/{domain}/`, without the platform
  segment. With this layout the scoped prefix is
  `{prefix}/v1/runs/flyte/{org}/{project}/{domain}/`. Update flyte.md (or
  the layout) when the history crate lands.
- `ulid` is a fresh ULID per shard, not the decision id. It is generated
  once per record and reused on retry, so a retried PUT rewrites the same
  object. ULIDs are monotonic within a millisecond and across clock steps
  backwards, and they reset after `fork()`.
- Local backend: temp file `.rs-*.tmp` in the target directory, then
  `fsync` and `os.replace`. Readers must LIST only `*.json`. fsspec
  backends make one `pipe_file` call per shard.

### Measurement semantics (SPEC §5.7 as implemented)

- `container` attribution (Flyte/K8s/Slurm, 1 s): memory comes from the
  first of these that is available:
  1. `memory.peak` (v2), as max(kernel peak, sampled max): `Exact`, no
     resolution;
  2. `memory.max_usage_in_bytes` (v1): `Exact`, no resolution;
  3. the sampled max of `memory.current` / `usage_in_bytes`: `Exact`,
     `resolution_s = interval`;
  4. max(`ru_maxrss` self, children, sampled RSS): `AtLeast`.

  CPU comes from `cpu.stat usage_usec` / `cpuacct.usage`. Without a cgroup,
  process CPU time is recorded as `AtLeast`.
- `process` attribution (Ray, 0.5 s): memory is the max sampled RSS minus
  the RSS at scope start, `Exact` with `resolution_s = interval`. Probes:
  Linux `/proc/self/statm`; macOS Mach `task_info`. There is no rusage
  fallback, because rusage covers the worker's lifetime and so says nothing
  about the task. CPU is `process_time` plus the CPU of reaped children.
- Censoring:
  - success with peak ≥ 95% of the memory limit → `AtLeast`;
  - throttled-period fraction > 5% → `AtLeast` for both CPU measurements;
  - `OomKilled` → `AtLeast(max(limit or last sample, peak))`;
  - `GpuOom` → `AtLeast(device total)`;
  - `Cached` → no measurements;
  - a Python `MemoryError` is recorded as `Failed`, with the memory peak
    downgraded to `AtLeast`.
- `cpu_peak_window`: the max over sliding windows of `cpu_window_s`
  (default 60 s; PlatformRef's value should be passed in). For runs shorter
  than one window it equals the run mean.
- NVML: the busiest device's peak; `CUDA_VISIBLE_DEVICES` is honoured
  (indices or UUID prefixes). `gpu_shared=True` records `AtMost`.
  `process` attribution uses per-PID device memory.
- `OutcomeSource`: `cgroup` when memory or CPU came from the cgroup,
  otherwise `self_report`. `trust` defaults to `unverified`; the integration
  sets `authenticated` when credentials are tenant-scoped (SPEC §8.2).

## Test infrastructure (`tests/infra`)

How to use it is in `tests/infra/README.md`. Findings from bringing it up
on 2026-11-18 (macOS arm64, Docker Desktop 29.7, cgroup v2):

### Flyte v2 (devbox image `flyte-devbox:latest`, SDK 2.13.6): request-only memory gets limit = request (SPEC open question 5)

| `flyte.Resources(...)` | pod `requests` | pod `limits` |
|---|---|---|
| `cpu="100m", memory="200Mi"` | cpu 100m, mem 200Mi | cpu 100m, **mem 200Mi** |
| `cpu="100m", memory=("200Mi","400Mi")` | cpu 100m, mem 200Mi | cpu 100m, mem 400Mi |
| `cpu=("100m","500m"), memory="300Mi"` | cpu 100m, mem 300Mi | cpu 500m, mem 300Mi |

The SDK serializes a scalar as a request only (no limit in the task
template). The backend sets limit = request when it builds the pod. So on
this backend a scalar-memory task **is** memory-enforced (`memory_enforced`
should be true for scalar memory as well as tuples). Recheck against Union
deployments, which may configure other platform defaults.

OOM as Flyte reports it (task with `memory=("128Mi","128Mi")` allocating 512 MiB):
- pod: `status.containerStatuses[0].state.terminated.reason == "OOMKilled"`, `exitCode == 137`.
- `flyte.remote`: run phase `ActionPhase.FAILED`.
  `Run.get(name).details().action_details.error_info` has `kind=KIND_USER`,
  `code="OOMKilled"` and a message containing `Reason [OOMKilled]`. Match on
  `error_info.code == "OOMKilled"`, not on the message.
- Listing: `flyte.remote.Run.listall(in_phase=..., task_name=..., created_at=..., limit=...)`,
  `flyte.remote.Action.listall(for_run_name=..., in_phase=..., parent_name=...)`,
  `Run.get(name)`, `Run.details()` -> `RunDetails.action_details: ActionDetails`
  (`.phase`, `.error_info`, `.attempts`, `.pb2.attempts[i].cluster_events`,
  `.phase_transitions`, `.runtime`). `run.wait()`, `run.watch()`.
- The devbox runs task pods in namespace `flyte`, named `<run>-<action>-<n>`,
  labelled `flyte.org/run=<run>`, `flyte.org/action=a0`, `task-name=<env>.<fn>`.
- Gotcha: tasks must be module-level attributes. The default resolver loads
  them with `getattr(module, name)`, so tasks built in a factory fail at
  load time.

### Slurm 26.05.5 in Docker
- Built with Lua: `job_submit_lua.so`, `cli_filter_lua.so` (plus
  `burst_buffer_lua`, `jobcomp_lua`). A smoke test confirms both scripts run.
  `cli_filter`'s `options["comment"]` reaches `job_submit` as
  `job_desc.comment`. `job_desc.admin_comment` set in `job_submit` shows up in
  `sacct --json` as `.comment.administrator`.
- cgroup v2 memory enforcement works on Docker Desktop with privileged
  workers that use a private cgroupns. The entrypoint moves processes into
  `/init` and delegates controllers, with `IgnoreSystemd=yes`. A job using
  400 MB under `--mem=100M` ends `OUT_OF_MEMORY`. In `sacct --json`:
  `state.current == ["OUT_OF_MEMORY"]`, `exit_code.status == ["SIGNALED"]`,
  signal 125, and the batch step's `tres.requested.max` mem is about 100 MiB
  (MaxRSS 102056K). `required.memory_per_node` is in MiB.

### Ray 2.60.0 (local head)
- Token auth is on by default; clients need `RAY_AUTH_MODE=token` and
  `RAY_AUTH_TOKEN_PATH`/`RAY_AUTH_TOKEN`.
- The State API needs the dashboard. Use `ray.util.state.get_task(task_id_hex)`
  and `list_tasks(filters=[("state","=","FAILED")], detail=True)`.
  `TaskState` has `state` (`FINISHED`/`FAILED`/...), `error_type`,
  `error_message`, `required_resources` (`memory=` appears there in bytes),
  `worker_pid`, `node_id`, `attempt_number`, `start_time_ms`/`end_time_ms`.
  Task id comes from `ref.task_id().hex()`.
- `error_type` values: `TASK_EXECUTION_EXCEPTION` (user raise), `WORKER_DIED`
  (SIGKILL, including a kernel OOM kill), `OUT_OF_MEMORY` (only from Ray's
  memory monitor, which is Linux-only; it cannot be produced on macOS).
  Exceptions: `RayTaskError`, `WorkerCrashedError`, `OutOfMemoryError`.
- `memory=` is a logical scheduling resource only. Ray does not enforce it
  per task. There is no per-task peak memory in the State API. Node metrics
  on `/metrics` (`ray_node_mem_used`, `ray_component_rss_bytes`, `ray_tasks`)
  are per node and per component.

### Linux `rsz` binary
- A `rust:1.91-bookworm` build with toolchain 1.99.1 from `rust-toolchain.toml`
  links against glibc symbols up to `GLIBC_2.34`. It runs on Rocky 9 (Slurm
  image) and bookworm (k8s image). `--musl` is the fallback.

## rightsizer-core

Spec ambiguities resolved in `crates/rightsizer-core` (one bullet each):

- **Workspace.** Toolchain 1.99, edition 2024, resolver 3. YAML is `serde_norway` (the maintained fork of the deprecated `serde_yaml`). Postgres is `tokio-postgres` + `tokio-postgres-rustls`, not sqlx, because sqlx's sqlite feature would clash with rusqlite's bundled `libsqlite3-sys`. rustls is 0.23 (0.24 is still a dev release) with the `ring` provider. `kube` 5.1 is paired with `k8s-openapi` 0.29 (`v1_33`).
- **`AxisSet`** is a newtype over `BitFlags<Axis>`, not a type alias, so it serializes as `["memory","cpu"]`. It also accepts a comma-separated string and the aliases `gpu`, `disk` and `mem`.
- **Holds** are `Vec<Hold {axis, reason, detail?}>` rather than `Vec<(Axis, HoldReason)>`, so a hold can carry detail. `HoldReason::is_hard()` splits them: hard holds (pins, S2, GPU count/type, no grid, no estimate, platform) bind `recommended`; soft holds (S3, breaker, no prior, unclassifiable, store, inexact snap) reset only `applied`.
- **WorkloadKey string form.** Tenants and suffixes can both contain `/` (K8s suffixes look like `pod/{name}/{container}`), so parsing uses a per-platform tenant arity: k8s, slurm, ray, sky, dstack, aws, gcp, modal and databricks take 2 segments; flyte and azure take 3; airflow and metaflow take 1. Any other arity, or an unknown platform, is written `{platform}://{tenant}//{suffix}`, and parsing always accepts `//`. Suffixes may not contain `//`, whitespace, or a leading or trailing `/`. `hash()` is the first 32 hex characters of sha256 of the canonical string.
- **Accelerator usable memory** comes from the tuner's NVML numbers where it has them (T4 15, L4/A10G 22, L40S 44 GiB). Elsewhere it is nominal minus about 1 GiB. Parsing is case-insensitive and strict: an unknown id is an error, not a silent `unknown-gpu`.
- **`RequestModel` serde.** Every variant is a struct variant under `{"kind": …}` (`catalog{shapes}`, `combos{combos:[{cpu, memory:[…]}]}`, `snap{shapes, exact, instance_bounded}`). **`Snap` is data**: the platform snaps an ask to the cheapest listed shape that covers it. An exact in-process `&dyn SnapFn` can be passed to `DecisionInput.snap`; it is never serialized. `exact = false` with no `SnapFn` makes memory and CPU recommend-only (`inexact_snap`).
- **`Decision.shape: Option<String>`** (an extension) carries the catalog or snap shape name that adapters render (SPEC §5.8 says the decision renders it).
- **`RunRecord.origin_prior: Option<Requests>`** (an extension) lets attribution work without joining to the decision.
- **`gpu_util_mean` is a fraction in [0, 1].** The low-utilization advisory fires below 0.30.
- **Within-cell demand** is spread log-uniformly over `(v[j-1], v[j]]`. Cell 0 starts at half its value. On the GPU axis, cell 0 is a point mass at zero ("none"). `cdf`/`sf` therefore work at any request value, on or off the grid.
- **Quantiles.** Levels and values are sorted independently (rearrangement). A value ≤ 0 is mass at zero. With fewer than two distinct positive top points there is no tail: the remainder goes to the top cell. A single quantile point is accepted, not rejected.
- **Unknown `Cells` grid ids** are read as a prefix of the target axis and never rejected. The four tuner grids are registered: `tuner-v1`, `tuner-mem-v1` (7-step CPU: a *subset*, not a prefix, of the 16-step ladder; rebinning handles it), `tuner-gpu-v1` (no "none" cell) and `rs-grid-v1`.
- **Duration** from history is a log-normal (σ = `point_sigma`) around the median, as five equally weighted scenarios at the 0.1/0.3/0.5/0.7/0.9 quantiles. Timeouts are modelled only when the walltime is set and the duration is not the 1 h default.
- **Overhead distribution** becomes discrete points: each quantile holds the mass between the midpoints of its neighbouring levels, and the top quantile takes the tail. Need = (demand + overhead + staged) × `need_headroom` (1.05, the tuner's `LABEL_HEADROOM`). Failure probability is `Σ_o w_o · P(D > r/h − o − staged)`.
- **Retry chain.** Demand is fixed for a run, so the path through ladder states is determined by the first step at which each axis fits. The expected cost sums over those cases exactly. Same-size retries repeat a deterministic failure (geometric repeat probability 1), so a failing state burns `(1 + same_size)` attempts.
- **Terminal failure** (the ladder is exhausted, or there is none): the author resubmits at the origin prior, priced as a successful run at the prior, plus `oom_penalty × oracle_cost` for every failed attempt.
- **Timeouts** are never escalated by a ladder (a Caller ladder escalates only on OOM). A timeout burns the walltime on each of `1 + same_size` attempts, then fails terminally.
- **Packed pricing** uses the cheapest compatible node shape the request fits, at price × max(cpu share, memory share, GPU share). When the platform lists no shapes it falls back to the tuner's m6i.xlarge ($0.192/h for 4 CPU and 16 GiB). A GPU request with no shapes is priced on a synthetic node with 8 cores and 32 GiB per GPU, using a placeholder on-demand GPU price table (`cost::default_gpu_price`).
- **Oracle** = the cheapest hard-feasible candidate whose effective memory ≥ the median need and whose CPU ≥ the median `cpu_mean`. If nothing fits, the largest candidate is used. Penalties use `oracle_cost(d) = oracle_rate × d × 1.02` per duration scenario.
- **The `prior` estimator** is recognised by provenance id `"prior"`: the decision keeps the prior and the source is `Estimator{prior}`. Without this, adding overhead and headroom to a degenerate prior-cell demand would raise every prior.
- **S3 is a per-candidate filter**, not a post-hoc check. A reduction that S3(b) permits at a larger size than the argmin is still taken. Reasons are checked in this order: blocked (breaker or store), then unclassifiable, then S3 (`s3_not_met`, or `not_enforced` when memory is not enforced), then floor, then trust region.
- **S3(b) evidence.** It counts the successes that fit (the literal reading; non-fitting successes are bounded by the size-scaled floor instead). Each record needs `Observed` status, `Exact` or `AtMost` censoring and enough trust. Coarse records (resolution ≥ 10 s) count ½, so an all-coarse history needs 2 × `min_history`. A peak with no resolution is an exact high-water mark (fine).
- **Size-scaled floor** = the largest peak among the last 5 trusted, same-digest successes × scale × resolution headroom, rounded up to a MiB. Any censoring is accepted; an `AtMost` peak only makes the floor more conservative.
- **Trust region.** On grids (Free, Combos) the floor is `max_step_down` cells below the anchor's covering cell. On catalog and snap platforms it is `anchor × 2^(−max_step_down/2)`. It is clamped to the ceiling.
- **S4 clamping.** When a grid cell above the ceiling would be cheaper than the best in-bounds candidate, the decision takes the ceiling and adds a `Clamped` advisory. Under heavy penalties the cost argmin alone would otherwise "give up" and pick the smallest failing size.
- **Ties** (within 1e-9 relative cost) go to the prior, then to less memory, then to less CPU. A GPU node's free host share otherwise drags memory up to the node's size.
- **S2 CPU limits** follow the same limit/request ratio rule as memory, so a CPU limit can drop with its request: CPU is compressible, and S3 covers incompressible axes only. Whole CPUs are enforced only for a Guaranteed prior with whole CPUs. A limit with no request (K8s request = limit) becomes limit = new request.
- **Exploration** moves memory to the next feasible larger candidate with the same CPU and GPU, with probability ε clamped to ≤ 0.05. The RNG is SplitMix64, seeded from `params.seed` mixed with the key and unit. Propensity is ε or 1 − ε, and 1 when exploration is impossible or off.
- **GPU type changes**, when allowed, choose among `allowed_shapes` with the same count. A smaller device counts as an incompressible reduction and needs S3(b) evidence from `gpu_memory_peak`. The GPU ladder steps through larger allowed shapes.
- **Attribution.** "Unclassified" means status `Unknown`, or any failure whose status was `Inferred`. A `Timeout` counts as caused when CPU *or* memory was below the prior. `Evicted` counts as caused when memory was below the prior; the eviction reason is not modelled.
- **Breakers.** A caused failure beyond the history's input sizes trips the input-bucket breaker, not the workload breaker, and is left out of rates. Slow-burn needs at least 200 enforced invocations in the window, so a single failure in a small fleet does not trip it (the spec gives no minimum). Only per-workload or input-bucket breakers that were not tripped by hand go half-open. A half-open breaker allows a trial enforce.
- **Policy.** An unreadable `mode_ceiling` or breaker keeps the mode but blocks reductions (`breaker`). History unavailable past its TTL demotes enforce to recommend and blocks reductions (`store_unreachable`). A non-durable store demotes enforce. An unclassifiable surface keeps enforce for increases and blocks reductions.
- **`AttemptContext`** carries `"v"` (default 1), to match the Python writer.
- **Config env overrides** (`rightsizer-engine`) parse values as TOML literals, so a JSON-style `["a","b"]` works but JSON objects do not. A bare comma list becomes an array of strings, which differs from Python's leave-as-string. Typed sections use `deny_unknown_fields`, so typos are errors.
- **Performance.** `decide` takes about 0.33 ms on rs-grid-v1 with 32 history records, a ladder, a walltime and 10 node shapes (`cargo bench -p rightsizer-core --bench decide`). Computing the context digest inside `decide` adds about 0.1 ms; pass `DecisionInput.context_digest` to skip it.
