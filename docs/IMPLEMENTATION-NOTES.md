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

## rightsizer-features

Decisions taken in `crates/rightsizer-features` (one bullet each):

- **Column names and string scalars are read in-process.** SPEC §11 says in-process estimators see everything, so readers collect names (`ReadOptions.column_names`, default on) and `redact_for_remote(ctx, &Privacy)` drops them unless `send` has `"columns"` / `"strings"`. `Privacy` mirrors `[privacy]`: `send_source`, `send` (`columns`, `strings`, `labels`, `labels:<key>`), `redact` (`scalars`, `sizes`, `rows`, `shapes`, `dtypes`, `params`, `columns`), `history_limit`, `labels` (the allowlist into contexts: `"*"` = all, empty = none) and a resolved `salt`. Redaction applies to history records' inputs as well.
- **`CodeInfo.source` is kept only when `privacy.send_source`.** It is truncated to 64 KiB on a character boundary. Contexts may be logged, and no built-in estimator reads source (SPEC §6.4). Tokens and the digest always use the full text.
- **`uri_hash`** = `sha256:` + hex(SHA-256(`"rightsizer-uri/1\0"` ‖ salt ‖ `"\0"` ‖ uri)). The salt comes from `Privacy.salt`, otherwise from `RIGHTSIZER_URI_SALT`. With no salt the hash is still computed, with an empty salt (deterministic but guessable).
- **Digest normalization** (language from the `CodeRef` hint or extension, otherwise detected):
  - Python: `#` comments outside strings, per-line indentation, whitespace runs and blank lines are stripped. `Resources(...)` argument lists and the values of `cpu=`, `cpus=`, `memory=`, `mem=`, `gpu=`, `gpus=`, `num_cpus=`, `num_gpus=`, `ephemeral_storage=`, `accelerator=` and `shm=` inside calls are masked. Docstrings and string literals are kept: they are code, and stripping them needs a real parser.
  - Shell: comments are stripped, including `#!`, `#SBATCH`, `#PBS`, `#BSUB`, `#$` and `#FLUX:` lines. On `srun`/`sbatch`/`salloc` lines, `--mem*`, `--cpus-per-task`/`-c`, `--gres`, `--gpus*`, `--time`/`-t` and `-G` are removed.
  - YAML: comments and every `resources:` block are stripped; indentation is kept.
  - Other: full-line `#` and `//` comments are stripped.

  `limits=` (threadpoolctl) and `memory_limit` (duckdb) are *not* masked, because they change usage. A change to normalization is a features-version change.
- **Token format** (`features-v1`), sorted and de-duplicated:
  - `tok:<t>` for every token of the union of the tuner's memory, CPU, GPU, CUDA and K8s sets plus the §9 AI tokens. Matching is a raw case-sensitive substring test, as in the tuner (comments included).
  - `num:<t>=<n>` for 21 parameter-like tokens: the first integer literal that starts ≤ 32 bytes after the token on the same line, max over occurrences.
  - `max_literal=<n>`: the tuner's `log_max_literal` input, before the log.
  - `code_lines_log2=<⌊log2(lines+1)⌋>`: the length bucket.

  Integer literals follow the tuner's `_NUM` regex exactly. The GPU bench's `Linear(a, b)` / tuple-assignment features are not ported. *Note for estimators:* tokens carry the `tok:` prefix (the gbt tests currently use bare names).
- **Directory inputs** report `bytes` = total size and `shape = [entry count]`; there is no `entries` field in `InputMeta`, and I did not add one to keep the wire stable. A listing truncated at `max_dir_entries` reports both as `None` rather than a lower bound.
- **CSV** reports `columns.total` only (the kind counts are 0: kinds are unknown from a header). Its first-line read goes 4 KiB, then 64 KiB; JSONL goes 4 KiB, 64 KiB, then the whole per-input budget. These windows may include bytes past the first line, which are discarded. Binary readers never read past their header or footer: Parquet reads the 8-byte tail, then exactly the footer; NPY the 12-byte prefix, then the header; safetensors 8 bytes, then the JSON header; Arrow the 8-byte magic, the 8-byte message prefix, then the schema message. A format is sniffed from 16 bytes only when neither a hint nor a known extension decides it.
- **Parquet column kinds** count top-level fields:
  - groups and repeated fields → nested;
  - String/Enum/JSON/BSON/UUID and un-annotated (fixed) byte arrays → string;
  - ints/floats/decimals/Float16 → numeric;
  - bool, temporal and INT96 → other.

  Arrow uses the same kinds by `Type` union tag. Arrow IPC is decoded by a small bounds-checked FlatBuffers reader, so it needs no `arrow-ipc` dependency. `rows` stays `None` for Arrow (counting rows would need every batch header).
- **Hugging Face** (`hf://org/model[@rev]`, feature `http`):
  - `config.json` gives `dtype` (`torch_dtype`, then `dtype`).
  - `model.safetensors.index.json` gives `bytes` = `metadata.total_size`. `params` comes from `metadata.total_parameters` when present; otherwise it is the sum over each shard's header, within the 1 MiB budget; otherwise `total_size / bytes-per-dtype`.
  - Without an index, `model.safetensors` gives `bytes` from HEAD and `params` from its header.
  - `HF_ENDPOINT` and `HF_TOKEN` (bearer) are honoured.

  Live tests (ignored by default) pass for `openai-community/gpt2` and the sharded `Qwen/Qwen2.5-3B`.
- **SSRF guard.** `ReadOptions.guard` pre-checks every HTTP(S)/`hf://` URI before the resolver is called. It checks literal IPs, and every address a host name resolves to: one internal address refuses the host. The default resolver's HTTP client enforces the guard again: a custom `reqwest` DNS resolver covers rebinding, a redirect policy re-checks each hop (redirects to literal private IPs), and proxies are off. Refused ranges are listed in `ssrf.rs` (loopback, RFC 1918, CGNAT, link-local incl. 169.254.169.254, ULA incl. `fd00:ec2::254`, site-local, multicast, reserved, and IPv4-mapped/NAT64/6to4 forms). The allowlist (`[features] allow_hosts`) takes CIDRs, addresses, host names and `*.suffix`. S3/GCS/Azure endpoints come from environment config, not from authors, and are not guarded.
- **Time budget.** One deadline per context covers the code-file read and all input reads. Reads run up to 16 at a time. When the deadline passes, the in-flight futures are dropped, and each input keeps whatever was already published to its slot (format and size from HEAD first, then the extracted fields). The context is never failed.
- **Truncation with more than 64 refs.** Pass 1 reads sizes only (HEAD or listing) for all refs. The 64 largest (then by name) get a full read in pass 2, so truncation is deterministic and the full read is bounded.
- **Core issue (not changed):** `WorkloadContext::canonicalize` sorts by `(bytes.unwrap_or(0), name)` only. Two entries that tie there keep their input order: same name with `Some(0)` vs `None`, or the same name and size with different metadata. A proptest found this. `rightsizer_features::canonical_inputs` sorts with a total order first (size, name, known-before-unknown, canonical JSON), and `build_context` uses it; `canonicalize`'s stable sort then leaves that order alone. The core owner may want to adopt the same tiebreak.
- **Metadata cache.** Process-wide LRU of 4096 entries, keyed by (SHA-256 of the URI, ETag or `size:mtime_ms`). `hf://` refs are keyed by URI alone: a moving branch like `main` is served from the cache until eviction, and a pinned `@rev` is exact. Only successful reads are cached. `clear_metadata_cache()` resets it.
- **`build_context`** keeps the brief's positional signature (`#[allow(clippy::too_many_arguments)]`). `build_context_with` takes `&ReadOptions` (guard, concurrency, cache) instead of a `ReadBudget`. Labels are the seed's, overlaid by the `labels` argument, then filtered by the allowlist. History is the same key only, newest first by `started_at`, then `invocation_id`, then `attempt`. Distribution markers found in the source are merged into `manifest.distributed_markers` and set `distributed`.
- **Fixtures.** `tests/fixtures/{pyarrow.parquet, table.arrow, table.arrows}` (≤ 15 KB each) come from `tests/fixtures/generate.py` (pyarrow 26). Every other fixture is written by the tests themselves: Parquet through the `parquet` crate's writer with statistics off, so data values never reach the footer; NPY and safetensors by hand.

## rightsizer-history

Backends: object store (`ObjectHistoryStore`: local FS, `object_store::memory::InMemory`, S3 and S3-compatible behind `s3`, GCS behind `gcs`, Azure behind `azure`), sqlite (`SqliteStore`, feature `sqlite`), postgres (`PostgresStore`, feature `postgres`), and `MemoryStore`. Wrappers: `CachedStore` (SPEC §8.4 cache and unreachable rules, §8.5 cluster scope) and `BufferedWriter` (bounded buffer with retry). `open(&StoreConfig)` maps a URI to a backend. `export::export_parquet` writes the SPEC §14.1 columns.

Tests: `cargo test -p rightsizer-history --all-features`. One conformance suite runs the same 10 cases on every backend (`tests/conformance.rs`). Postgres runs when `RSZ_IT_POSTGRES_URL` is set, for example `postgres://postgres:pw@127.0.0.1:55432/postgres` with `docker run --rm -d --name rsz-it-pg -p 127.0.0.1:55432:5432 -e POSTGRES_PASSWORD=pw postgres:16-alpine`; each test gets its own schema. S3 runs when `RSZ_IT_S3_URL` is set, for example `s3://rsz-it/conformance?endpoint=http://127.0.0.1:59000&region=us-east-1` plus `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`, against `minio/minio` with a bucket made by `mc mb`. Both gated suites passed on 2026-11-19 (postgres 16, MinIO latest). The Python cross-language test runs `.venv/bin/python` with `PYTHONPATH=python/rightsizer-runtime/src`, and is skipped when there is no venv.

### API additions (all additive; the engine can keep coding against the existing trait)

- `HistoryStore` gained **default methods** `durable()`, `shared()` (both default to `info().durable_shared`), `role()` (default `Operator`), `keys()` (default `Unimplemented`) and `stats()` (default empty). Enforce needs `durable() && shared()`. sqlite is durable and not shared. A local directory is durable unless it is under the temp dir, and shared only if it sits on a network filesystem (statfs). Use `StoreConfig.{durable, shared}` to override, for example for a cluster filesystem the detector does not recognize. `memory://` is neither durable nor shared.
- `Role { Client (default), Operator }`. A client store refuses `breaker_reset` and `control_set` with `PermissionDenied`, and may still create trips. `MemoryStore.client_credentials` maps onto the same role. `StoreConfig::default().role` is `Client`, so `rsz breaker reset`, `rsz control` and `rsz history compact` must open the store with `Role::Operator`.
- `StoreConfig { uri, role, write_version, durable, shared, allow_network_fs, check_duplicates_on_write, holder }`. The engine maps `[history]` onto it. `cache_ttl`, `cluster` and `share` go to `CacheOptions { history_ttl, control_ttl, capacity, depth, scope: ClusterScope }`, and `buffer` goes to `BufferOptions.capacity`.
- `CachedStore::history(key, limit) -> HistoryRead::{Fresh, Cached{age}, Unavailable}`, `breaker(scope) -> BreakerRead::{Known, Unknown}` (`blocks_reductions()` is true for `Unknown`), and `control() -> ControlRead::{Known, Unknown}`. `CachedStore` also implements `HistoryStore`: an unavailable read becomes `Err(Unreachable)`.
- `BufferedWriter::{put_run, put_decision} -> WriteOutcome::{Written, Buffered, Dropped}`, plus `flush()`, `spawn_retry()` (exponential backoff from 250 ms to 30 s) and `stats()` with written, retried, buffered, dropped, rejected and errors. Permanent errors (permission, corrupt, config) are dropped and counted as `rejected`, never retried.
- N/N-1 (SPEC §13.2): `decode_versioned` accepts `v` in `WIRE_VERSION-1 ..= WIRE_VERSION`. A missing `v` reads as 1. Unknown fields are ignored and missing optional fields take their defaults. A newer `v` is **skipped and counted** (`StoreStats.skipped_on_read`), not fatal, and so is a corrupt object. Writers stamp `write_version`, and `check_write_version` refuses any version this build cannot read.
- `CompactOptions` and `CompactReport` are unchanged. `deleted` counts run shards (or rows). Per-kind counts, lag and shard count go to `StoreStats.last_compaction: CompactionStatus` (`rightsizer_store_shards`, `rightsizer_store_compaction_lag_seconds`).
- No change to `rightsizer-core`.

### Object store behaviour

- **Layout** is exactly the runtime contract above. Two objects were added: `{prefix}/v1/compaction/lease.json` (the singleton lease) and `{prefix}/v1/compaction/status.json` (the last report, for metrics). A breaker reset is one `breakers/{scope}.json` object holding the newest `Reset`; readers also accept an array of resets. Reads LIST only `*.json` direct children of the key directory, so Python's `.rs-*.tmp` staging files and the `.lock` objects are ignored.
- **Dedup.** The Python writer mints a fresh ULID for every attempt, retries included, so duplicates are removed **on read and in compaction** (first written wins). `put_run` also reads the key before writing so it can return `Duplicate` (one GET and one LIST per write). Set `check_duplicates_on_write = false` to skip that read.
- **Watermark.** Compaction folds a shard into the pointer watermark only when the shard is older than `settle` (5 min). A PUT whose ULID was minted before compaction's LIST but that lands after it is then not hidden from readers. A shard that lands later still, below the watermark (for example a Python retry hours afterwards), is hidden from readers until the next compaction merges it into the pointer. Compaction always lists every shard.
- **Deletion** removes a shard only when both hold: its ULID ≤ the watermark, and its ULID time is older than `grace` (24 h). The record goes into its day's snapshot first. As a result, readers see the pointer's `keep_last` (64) plus newer shards, and older records live only in snapshots.
- **Snapshots** are `snapshots/{tenant}/{started_at date}.parquet`. Runs are added when their shard is deleted. Decisions are added once they pass the grace period (tracked by a decision watermark in `status.json`) and again when they are deleted at 30 d, so late decisions are not lost. A day's file is read back (from the `record_json` column) and merged when new rows arrive. Snapshots older than `max_age` (90 d) are deleted, and pointer records older than `max_age` are dropped. Attempt records whose last write is older than 7 d are deleted. `attempt_get` hides expired records, and `attempt_put_if_absent` replaces an expired record with a conditional update.
- **Conditional writes.** Shards, decisions, trips, attempt records and lease creation use `PutMode::Create`. Pointer rewrites, lease closes and lease takeover use `PutMode::Update` on the ETag, retried on conflict. S3 uses `S3ConditionalPut::ETagMatch` and path-style requests when an endpoint is given, and MinIO passes the whole suite.
- **Local-FS emulation (deviation).** `LocalFileSystem` supports `Create` (hard link) but returns `NotImplemented` for `Update`. Local stores therefore emulate it. The writer takes a create-only lock object `{path}.lock`, checks the ETag (inode, size and mtime) under the lock, overwrites, then releases the lock. A lock older than `lock_stale_after` (30 s) is broken. The emulation is exact among rightsizer processes on the same directory. It does not protect against outside writers that ignore the lock. Any backend that returns `NotImplemented` for `Update` falls back to the same path.
- `decision(id)` LISTs the whole `decisions/` tree, because the tenant is unknown. That is fine for `rsz explain`, but it is not a hot path.

### sqlite / postgres

- Both store full JSON (`body`: TEXT in sqlite, JSONB in postgres) next to the indexed columns, and decode it under the N/N-1 rule. Dedup comes from a unique `(key, invocation_id, attempt, source)` index with `INSERT OR IGNORE` / `ON CONFLICT DO NOTHING`. Trips and attempt records are create-only. Expired attempts are replaced with `ON CONFLICT … DO UPDATE … WHERE written_at < cutoff`. The compaction lease is a `leases` row, taken over only when expired. Postgres closes leases with a compare-and-swap on the old body. There are no pointers or snapshot files: compaction applies retention (runs older than `max_age`, decisions older than 30 d, attempts older than 7 d), never deletes rows written within `grace`, and records its status.
- **sqlite** opens in WAL mode (`synchronous=NORMAL`, 5 s busy timeout). Migrations are ordered and recorded in `schema_version`, and a database from a newer build is refused. Network filesystems are refused by checking statfs: the type name on macOS and BSD (nfs, smbfs, afpfs, webdav, *fuse*, …), and the magic number on Linux (NFS, SMB, CIFS, SMB2, FUSE, Ceph, Lustre, GPFS, …). FUSE counts as network. `fsdetect::classify` and `sqlite::check_filesystem` are tested with injected types, and `allow_network_fs` overrides the check.
- **postgres** keeps its tables in a schema (`?schema=`, default `rightsizer`). Operators run migrations under an advisory lock. Clients only check the schema version and are refused on an unmigrated or newer schema. TLS is enabled when `sslmode=require|verify-*`, using rustls (ring) with a CA bundle from `sslrootcert` or `$SSL_CERT_FILE`. The CA bundle is required, because there is no bundled root store. The connection reconnects after it closes. The `GRANT`s documented on `postgres::MIGRATIONS` were checked against a live server: a `rightsizer_client` login could insert trips, and was denied resets, control writes and deletes.

### Deviations and decisions

- **Parquet without arrow.** Snapshots and exports use the `parquet` crate's low-level column writer (already a workspace dependency), with Snappy compression. That adds only `snap` (about 0.45 MB rlib), next to the existing parquet rlib (about 23 MB). The `arrow` feature would add arrow-array, arrow-buffer, arrow-data, arrow-schema, arrow-select and arrow-ipc for no gain, because the schema is flat. Snapshot files are real Parquet, not JSONL.
- **§14.1 columns** (`snapshot::COLUMNS`): kind, id, key, platform, tenant, at, cluster, invocation_id, attempt, decision_id, code_digest, context_digest, inputs_json, input_bytes, req_/lim_/prior_ cpu_m/memory/gpu, status, status_confidence, each peak with its censor (and `memory_peak_resolution_s`), gpu_util_mean, cpu_throttled_frac, t_peak_frac, duration_s, source, trust, mode, decision_source, estimator_id/version, recommended_/applied_ cpu_m/memory/gpu, shape, held_json, propensity, expected_cost, p_fail, record_json. **Code tokens are not exported**, because no stored artifact carries them: `RunRecord` has only `code_digest`, and `Decision` only `context_digest`. Exporting them would need the decision log to keep the context's code tokens.
- **Per-cluster history (§8.5)** is applied in `CachedStore` (`ClusterScope`), not in the backends. Records with no `cluster` are always visible.
- `breaker_list` returns only scopes that have at least one trip, each with its resets.
- `BreakerScope::path()` for workload scopes is `workload/{tenant}/{hash}`, without the platform. Keys cannot collide, because the hash covers the platform. A per-tenant IAM policy on breakers, though, cannot tell platforms apart. Raise this with the core owner if that matters.

## rightsizer-estimators

Tests: `cargo test -p rightsizer-estimators [--features remote]`; exporters:
`cd python/rightsizer && ../../.venv/bin/python -m pytest tests/export`
(`RIGHTSIZER_RUST_PARITY=1` also runs the Rust evaluator on fresh exports).

### Estimator behavior (all emit *demand*: container peak − overhead median, SPEC §5.2)

| id | output | declines when |
|---|---|---|
| `rules-sized` | `Cells` on rs-grid-v1 (log-normal, σ 0.25 warm / 0.5 cold, truncated at lower bounds) | no input bytes and no learnable record with a memory peak |
| `history-peak` | `Cells` (σ 0.1 around censored max × 1.15) | no learnable record with any measurement |
| `prior` | degenerate `Cells` (unchanged; the core recognizes provenance `"prior"`) | never |
| `gbt` | `Quantiles` per trained level (natural units) + `novelty` | no model configured |

Durations are `Quantiles` (p10/p50/p90, ≥ 3 successes) or a `Point`; `t_peak_frac` is the history median.

- **History usable for estimation.** `Cached` runs are excluded. `Unverified` records **are** used: trust gates floors, anchors and S3(b) only (SPEC §8.2; consistent with core's `trusted()` being applied only there). `Inferred` statuses are used (SPEC §5.7: "used for learning").
- **Censoring.** `OomKilled` peaks are lower bounds whatever the importer wrote (`GpuOom` for GPU memory). A lower bound `v` enters a max as the VPA bump `max(1.2 v, v + 100Mi)` (CPU: `1.2 v`) and the emitted distribution has **no mass below `v`** (truncated log-normal; cells whose upper edge is below `v` are 0). `AtMost` values are used as values (a conservative upper bound). Truncation is exact at cell edges; within a cell, mass is log-uniform as in the core.
- **Same digest preferred**: when any learnable record has the context's code digest, only those are used.
- **`rules-sized` port.** Warm: the nearest-size candidate (`|ln(b_hist/b_now)|`, ties → newest; without sizes, the newest), demand × `scale` (SPEC §7.4, never < 1) × margin. Other `AtLeast` candidates whose inputs were no larger than this invocation's also bound demand from below (scaled). Cold: per-format `ColdStart { base_mib, k, exponent }`, `demand_mib = base + k × input_mib^exponent`; the tuner's bench power law `2^(a + b·log2 bytes)` is `power_law(a, b)` and `bench_all`'s `base + k × MiB` is `linear(base, k)`. **Defaults** (the tuner never recorded its tuned winners): parquet 256 + 4×, csv 256 + 3×, jsonl 256 + 6×, npy 128 + 1.25×, safetensors 512 + 1.25×, arrow_ipc 256 + 1.5×, dir/blob 256 + 2×, model_ref 512 + 1.25× (MiB), inside the tuner's tuning box (base 64–512, k 0.5–8). CPU: censored max of `cpu_mean` × `cpu_margin`, cold 1 core (the tuner's `cpu_cell(1.0)`); `declared_pools` is not ported (needs source, often withheld).
- **Deviation from the tuner** (by the spec): the tuner scales the nearest run *down* for smaller inputs; `scale` never goes below 1.
- **`history-peak` defaults**: K = 10, margin 1.15 (VPA's default `recommendation-margin-fraction`; the tuner tuned it per split), no input scaling. GPU demand = busiest-device peak × requested device count.
- **Parity with the tuner** (`tests/rules_parity.rs`, numbers produced by running the tuner's `_sized_one` / `_size_one` via `tests/rules_parity_gen.py`): 8 of 9 cases match exactly (need within 1e-9, same memory and CPU cells): k8s and Flyte cold starts, warm exact-size, 2× and 1.5× scaling, nearest of three, margin 1.25, two bench power laws. The 9th (smaller invocation) is the documented scale-down deviation. Flyte cases feed rightsizer container peaks = tuner peak + 70 MiB with a 70 MiB overhead median; the bridge to cells is `(demand + overhead) × 1.05`.

### `rightsizer-trees/1`, `rightsizer-novelty/1`, `features-v1`

- Formats are documented in `src/trees.rs` and `src/novelty.rs`; the `features-v1` table is in `src/features_v1.rs` (names like `inputs.log2_bytes_total`, `fmt.parquet.log2_bytes`, `tok.<token>`, `scalar.<name>`, `hist.log2_peak_max_mib`). Features map **by name**; unknown names are NaN (missing). `tok.*` features exist only for the model's vocabulary; without code they are missing, not 0.
- **sklearn thresholds.** `HistGradientBoostingRegressor` predictors store `num_threshold` in raw feature space (bin upper edge) and `predict` on raw X compares `x <= num_threshold`, NaN → `missing_go_to_left` (set even for features without NaN in training). `bin_threshold` is for binned data only. Raw score = `_baseline_prediction + Σ leaves` (leaves include the learning rate). Categorical features (`is_categorical_`, `_preprocessor`) and non-identity links are refused.
- **LightGBM.** `dump_model()` thresholds; `missing_type` None → `"none"` (NaN read as 0, compared), Zero → `"zero"` (NaN→0, |x| ≤ 1e-35 → default), NaN → `"nan"`; base 0 (init score is in the first tree). Categorical (`decision_type "=="`), linear trees, multi-class and `average_output` are refused.
- **Parity**: Rust vs sklearn and LightGBM own predictions on 22 probe rows × 4 ensembles each, including all-NaN, zeros and NaN in features without training NaN: exact to ~1e-15 (asserted ≤ 1e-6 relative). All three LightGBM missing types are exercised. Fixtures: 80 KiB, regenerated with `.venv/bin/python python/rightsizer/tests/export/make_fixtures.py`.
- **Novelty** follows the tuner's `novelty_knn`: standardize on the reference rows (sd ≤ 1e-6 → scale 1), missing → raw 0, mean Euclidean distance to the k nearest, ECDF = fraction of knots ≤ d. `needs_code` + no code tokens → `novelty = None`.

### Remote client (feature `remote`)

- Order per call: redact (`rightsizer_features::redact_for_remote`) → exact cache (key = `memo::exact_key` of the redacted context) → memo (optional, shared) → single-flight → one jittered retry (50–250 ms) → coalescer (20 ms, ≤ `max_batch` from `/v1/info` capped by config; none without `max_batch > 1`) → breaker → HTTP.
- Breaker counts transport errors, timeouts, 429, 5xx and unexpected statuses as failures; 400/409/422 are deliberate answers and do not count. Half-open allows one trial.
- `/v1/info` unreachable at build time → a placeholder info (id = member name, version `unknown`); the chain still builds (S1).
- Per-item batch errors map by `code` (`"422"`/`"insufficient"`, …) and never count against the breaker.
- The caller budget (`Estimator::estimate_within`) replaces the 10 s default.

### API (additive; existing signatures kept)

- `Estimator::estimate_within(ctx, budget)` — default method (timeout around `estimate`).
- `EstimatorChain::estimate_with(ctx, &impl SkipPolicy, budget)`; `SkipPolicy` is implemented for closures `Fn(&EstimatorInfo) -> bool` and `[String]` (`id` or `id@version`). `ChainAnswer` gains `answered_by: (index, id, version)`, `failures` (non-`Insufficient` errors), `skipped_by_policy`; `skipped` is unchanged.
- `build_chain(&ChainSpec, &Privacy)` (async): `ChainSpec` deserializes from the engine's `EstimatorConfig` JSON (unknown pool/gate keys ignored; `path`/`novelty` with aliases `model_path`/`novelty_path`). Default chain gets `gbt` in front when `members.gbt.path` is set. M4 kinds are skipped with a warning; a broken model file is an error.
- `memo::{Memo, MemoKey, TtlCache, exact_key, normalized_estimate}`. `MemoKey` adds the **unit** to the spec's (workload key, code digest, feature bucket, policy hash): a pod's containers share a key, not a demand.

### Requests for the engine

- `MemberConfig` (`deny_unknown_fields`) needs `client_cert`, `client_key`, `ca_cert`, `max_batch`, `coalesce_ms` and `params` for mTLS, batching and built-in parameter overrides to be configurable from TOML; `ChainSpec` already reads them.
- No changes were made to `rightsizer-core`.

## rightsizer-k8s

Pure adapter (no `kube`, `k8s-openapi` or `axum`; builds with
`--no-default-features`). Cluster I/O (fetching LimitRanges, quotas, Nodes,
Kueue objects, ConfigMaps), the webhook server, the watcher and the
Prometheus importer come later and feed this code through `AdapterEnv`.

### Inputs and API

- **`AdapterEnv.settings`** is the `[k8s]` table *plus* cluster inputs, all
  optional, unknown keys ignored (`K8sSettings`): `cluster`, `flyte.*`,
  `accelerators` (extra native → canonical names), `gpu_product_label`,
  `allow_gpu_type_change` (false), `apply_watch`, `cpu_window_s` (15),
  `overhead` (image prefix → `OverheadModel`), `shapes`, `prices`
  (instance type → $/h), `autopilot`, `pod_skip_owner_kinds`,
  `namespace_labels`, `limit_ranges`, `resource_quotas`, `nodes`,
  `cluster_queues`, `local_queues`, `resource_flavors`, `runtime_classes`
  (raw K8s objects as JSON), `dry_run`. Settings given to `K8sAdapter::new`
  are the base; `env.settings` keys override them; `env.cluster` overrides
  `cluster`. The engine's `K8sConfig` (`deny_unknown_fields`) needs the
  extra keys (or a pass-through table) to be configurable from TOML.
- **Trait methods are thin wrappers.** The webhook and CLI use inherent
  methods: `extract_documents` (per object; documents may span namespaces),
  `extract_all` (includes already-sized units with their origin prior),
  `render_with(RenderRequest{mode, decision_id, units, annotate, output})`
  → `Rendered{patch, applied, held, decision_id}`, `validate_at(now)`,
  `finalize` (Autopilot normalization before the decision is final),
  `reserve` (ledger), `restore`, `mode`. Trait `render` = enforce with a
  fresh ULID; trait `validate` uses `Utc::now()`.
- **Shared-crate change (additive):** `rightsizer_plugin::Unit` gained
  `key: Option<WorkloadKey>` (full key when it is not
  `{kind}://{tenant}/{key_suffix}`; set only for Flyte task pods) and
  `Unit::workload_key(kind, tenant)`; `Unit` now derives `Default`. The
  engine should build keys with `unit.workload_key(adapter.kind(), &tenant)`.

### Decisions

- **YAML.** `saphyr-parser` 0.3 (maintained, YAML 1.2, spans per event)
  drives a small locator (`yaml.rs`) that builds a span tree and the JSON
  value (core schema; merge keys and anchors resolved). Parser spans are
  character indices and run over trailing comments/whitespace; the locator
  converts to bytes and recomputes scalar ends (closing quote; plain scalar
  up to ` #`). Empty values are marked at the `:`. Edits through anchors or
  aliases are refused (an edit would change every alias); block scalars are
  never edited. Replaced scalars keep their quoting style; a plain string
  scalar that would turn into a number (`rayStartParams`, annotations) is
  double-quoted; new numeric-looking strings are quoted (`cpu: "2"`).
  Missing keys are inserted after the mapping's last line at the mapping's
  column; the indentation step is inferred from the document (default 2);
  flow mappings get `, key: value` before `}`; an empty `resources:` gets a
  block. `kind: List` documents are refused (split them with `---`).
- **Unit ids** are name-based (`spec.template.spec.containers[main]`,
  `spec.replicatedJobs[workers].template.spec.template.spec.containers[t]`,
  `spec.pytorchReplicaSpecs.Worker.template.spec.containers[pytorch]`,
  `spec.workerGroupSpecs[gpu].template.spec.containers[ray-worker]`,
  `spec.tasks[worker].template.spec.containers[mpi]`,
  `spec.trainer.resourcesPerNode`, `….spec.resources` for pod-level), so a
  sidecar injected at index 0 does not shift them. Multi-document
  submissions prefix `{doc index}:`. The signed prior uses the object-local
  id. **Groups**: every template unit has `group = unit id` (one decision for
  all replicas of that container); bare pods have none.
- **Keys.** `{pod|job|cronjob|jobset|pytorchjob|trainjob|ray|volcano}/{name}`
  + group + container. Ray and Volcano use `ray/{name}/{group}` /
  `volcano/{name}/{task}` for the first app container and append
  `/{container}` for others; pod-level resources use `/@pod` (`@` cannot be
  in a container name). `rightsizer.io/workload-key` replaces the
  `{tag}/{name}` base only (unit parts are appended; invalid values are
  ignored). Name stripping: `generateName` prefix; else a trailing 5-char
  K8s-alphabet suffix containing a digit (`-x7k2p`; `-train` is kept); then
  a CronJob `-<8–10 digit unix minutes>` suffix. A Job owned by a CronJob is
  keyed `cronjob/{owner}`. Flyte: labels `organization`/`project`/`domain`/
  `task-name` (configurable; `default_org` when the org label is absent,
  else not Flyte-keyed); the key goes on the first app container only.
- **Skip rules.** Pods owned by `Job`, `CronJob`, `JobSet`, `PyTorchJob`,
  `TrainJob`, `RayCluster`, `RayJob` → `AlreadySized`; owned by
  `ReplicaSet`/`StatefulSet`/`DaemonSet`/`ReplicationController`/`Workload`/
  `PodGroup` → `Unsupported` (a decision: long-running controllers recreate
  pods from templates; configurable). Pods with `kueue.x-k8s.io/queue-name`,
  `kueue.x-k8s.io/managed`, `kueue.x-k8s.io/pod-group-name` or Volcano's
  `scheduling.k8s.io/group-name` → `QueueManaged`. Template objects owned by
  another template kind (a Job of a JobSet, a JobSet of a TrainJob, a
  RayCluster of a RayJob) → `AlreadySized`; **a Job owned by a CronJob is
  sized** (keyed by the CronJob) unless it carries the decision id that the
  CronJob's render wrote into `jobTemplate.metadata.annotations`.
  `decision-id` without a valid signed prior → another layer's marker →
  skip; with a valid prior → resume units not in it. Non-`CREATE`
  operations → `Unsupported`.
- **Priors.** Declared resources, then (templates) simulated LimitRanger
  defaulting, then K8s API defaulting (request = limit). LimitRange objects
  get API defaulting too (`default ← max`, `defaultRequest ← default`).
  `defaulted` marks an axis only when its **request** was filled; a filled
  limit alone does not. For pods: from the signed prior's `defaulted` when
  present, else request (and limit, if a default limit exists) equal to the
  LimitRange defaults. `qos` is the pod's QoS (post-defaulting). `/dev/shm`
  memory emptyDir `sizeLimit` → `requests.shm`. Pod-level unit: GPUs are
  summed from containers.
- **Seed.** Code ref = `CodeRef::Info{digest = sha256(canonical {image,
  command, args})}`. `rightsizer.io/inputs` (JSON array → `input{i}`, or
  `{name: uri}`; object or pod-template annotation; ≤ 64). On the admission
  path `extra.inputs_hash_only = true` (the engine must not read them
  inline — **engine to honour**). `rightsizer.io/code-configmap` →
  `extra.code_configmap = "{namespace}/{name/key}"` (resolution later).
  `memory_backed_bytes` is set only for the init-tmpfs staging pattern (an
  init container and the unit share a memory emptyDir, not `/dev/shm`), to
  its `sizeLimit`; otherwise the limit is `extra.memory_volume_limit_bytes`.
  `extra` also carries `kind`, `replicas`, `backoff_limit`,
  `active_deadline_seconds`, `memory_limit`, `image`, `role`,
  `queue_managed`, `gpu_selector`, `gpu_toleration`, `init_*`,
  `ray_num_cpus|num_gpus|memory`, `apply_watch` — `PlatformModel::reference`
  reads its inputs from these.
- **Distributed**: group replicas > 1 (`min(parallelism, completions)`,
  JobSet `replicas × job pods`, Ray `replicas × numOfHosts`), or any pod of
  a JobSet/PyTorchJob/TrainJob/Volcano job when the job has > 1 pod, or
  `torchrun`, `torch.distributed.run|launch`, `--nproc_per_node`,
  `WORLD_SIZE`, `deepspeed`, `accelerate launch` in command/args/env.
- **Retries** (`same_size`): Pod 0; Job/CronJob `backoffLimit` (default 6);
  JobSet `failurePolicy.maxRestarts` (0); PyTorchJob `runPolicy.backoffLimit`;
  RayJob `backoffLimit`; Volcano `maxRetry` (3). Ladder only when
  `apply_watch` and not admission. `memory_enforced` = effective prior has a
  memory limit (LimitRange defaults included).
- **Render / S2.** Limits are recomputed by `apply_limits` against the
  **declared** spec (with API defaulting), not the LimitRange-simulated
  prior, so a template never gains a limit; validate then checks the
  patched template *as the pods will be* (a request above a defaulted limit
  is rejected). Whole CPUs: `ceil_whole` when the declared pod is Guaranteed
  with whole CPUs. GPU count and ephemeral/shm are never written (held
  reported in `Rendered.held`). Only fields whose quantity changes are
  written (`4096Mi` = `4Gi`). KubeRay: `num-cpus` (whole cores of CPU limit
  else request) and `memory` (bytes of memory limit else request) are
  updated **only if present and consistent with the original resources**, so
  `num-cpus: "0"` on a head stays. Originals go into the signed prior's
  `ray` map; restore puts them back.
- **Annotations.** Enforce: `decision-id`, `prior` (merged with an existing
  valid prior: first-seen origin wins), `recommendation` (compact JSON, ≤ 8
  KiB else a count). Admission (JSON patch) also writes `decision-id` into
  every pod template's metadata and a CronJob's `jobTemplate.metadata`
  (children skip; the watcher joins). Files (text edits) get top-level
  annotations only. Observe/recommend: `decision-id` + `recommendation`,
  once. Idempotence: enforce with no resource change and a valid prior
  covering the decided units → `Patch::None`. Enforce requires a signing
  key; without one render errors (S1: caller leaves the object unchanged).
- **Signed prior**: `base64url(JSON{v:1, kid, prior, defaulted?, ray?, sig})`,
  `sig = base64url(HMAC-SHA256(key[kid], canonical_json({v, kid, prior,
  defaulted?, ray?})))`. Key file: lines `kid:base64` (first is active), or
  one bare base64 key (kid `k0`); keys ≥ 16 bytes. Not bound to object
  identity (copying a genuine prior only makes the target look sized).
- **Validation** (`Rejection.reason` = check; caller records
  `validation:<check>`, `validate::prior_reason`): `pod_spec` (unique
  container/volume names and mount paths, one `/dev/shm`, request ≤ limit,
  pod-level ≥ containers), `limitrange` (Container and Pod items; min/max/
  ratio; templates defaulted first), `qos`, `autopilot` (requests the
  normalizer would change, or a maximum), `node_shape` (effective pod
  request incl. sidecar rule and RuntimeClass/`spec.overhead` vs configured
  shapes and Node allocatable), `resource_quota` (Σ replicas × effective
  pod, requests and limits, `hard − used − ledger`, minimum over quotas;
  quota scopes are not evaluated — conservative), `kueue` (LocalQueue →
  ClusterQueue; per resource max over flavors of nominal + borrowingLimit,
  unbounded in a cohort without a borrowing limit; plus every pod must fit
  a Node matching some flavor's `nodeLabels`). Capacity checks reject only
  when the patched value exceeds capacity **and** exceeds the original.
- **Autopilot** tables (overridable): general-purpose 1–6.5 GiB/vCPU, min
  250m/512Mi (bursting 50m/52Mi), max 30 vCPU/110 GiB; balanced 1–8, max
  222/851; scale-out 4:4, max 54/216; GPU per-count CPU/memory maximums for
  T4, L4, A100-40G/80G, H100 (from the GKE docs; check before relying on
  them). Pod-total adjustments go to the first app container. `finalize`:
  a reduction Autopilot would raise back to ≥ the prior → hold at prior
  (`HoldReason::Platform`); other adjustments become the decision; an
  increase above a maximum → hold at prior; non-bursting → limits =
  requests in the finalized spec. Only for single-object submissions.
- **Ledger**: `ReservationLedger` trait, `MemoryLedger` (2-minute TTL,
  expiry on read), `NoLedger`. `reserve` records the positive increase of
  requests/limits; the store-backed shared ledger comes with the webhook.
- **Restore** (pure): sets each prior unit's CPU/memory requests/limits
  back (removing fields the prior lacked, and maps that restoring empties),
  restores Ray params, removes `rightsizer.io/{prior,decision-id,
  recommendation}` everywhere (and annotation maps / template metadata that
  only rightsizer filled). JSON patch only.
- **`apply --watch` ladder** (`ladder.rs`): pure `step(state, event)`;
  escalates only on `OOMKilled` while the current memory is below
  max(origin, final floor); ×factor per step, the last step raised to the
  floor; a ceiling never cuts below the floor; names `{base}-rsz{n}` (≤ 63
  chars); `renamed_job` strips server fields and controller selector
  labels.

### Not yet / follow-ups

- Karpenter NodePool requirements in the GPU-type intersection; tolerations.
- ConfigMap code resolution, input prefetch, webhook, watcher, importer,
  restore I/O (feature `cluster` / `webhook`).
- Kueue: no fair-sharing/preemption modelling; `coveredResources` across
  several resource groups are treated independently.

## rightsizer-engine / rsz

Tests: `cargo test -p rightsizer-engine -p rightsizer-cli` (safety, engine,
replay, parity, CLI). Bench: `cargo bench -p rightsizer-engine --bench engine`.
Parity fixture: `PYTHONPATH=<tuner checkout> uv run --no-project --with numpy python crates/rightsizer-engine/tests/parity/gen_k8s2_fixture.py crates/rightsizer-engine/tests/fixtures/parity_k8s2.json`.

### Engine behaviour (SPEC as implemented)

- **Construction.** `Engine::builder(config).store(..).chain(..).clock(..).role(..).build().await`; `Engine::new(config).await` opens `history.uri` and builds `[estimator]`. Both are async (the history factory and the chain builder are). A config error (validation, store open, chain build) fails `build()` only with `strict = true`; otherwise the engine is **inert**: every decision is `Prior{config_error}` with the message as detail.
- **`decide` never fails (S1).** One wrapper applies the hard budget (`tokio::time::timeout`, `budget_ms` or `policy.budget_ms`), catches panics (`catch_unwind` on the whole future, estimator calls included) and counts both. Budget/panic fallbacks are *deferred* into the write buffer, never written inside a spent budget. Normal decisions are written with 80 % of what is left, else deferred.
- **Mode resolution** (`core::policy::effective_mode`): requested (request ∨ config) ∧ `min(request.max_mode, config.max_mode)` ∧ `mode_ceiling` (60 s cache; no control object = `enforce`; unreadable = keep mode, block reductions) ∧ breakers (workload ∨ input-bucket, fleet; estimator-version breakers make the chain skip that member, and join the memo's policy hash) ∧ history availability ∧ store durability. `off` reads and records nothing. `observe` does not estimate (prior, reason `mode`) but is recorded (outcomes join to it). Every demotion is appended to the explanation.
- **Half-open recovery** is computed at read time: after a non-manual trip, the key's runs since the trip whose decision was below enforce and whose measured (`Exact`/`AtMost`, `Observed`) peak fit `applied` count as shadow fits (at most 4 × `recovery_shadow_fits` decision lookups).
- **Memo** (`memo.rs`): key = (`key#unit`, code digest, `features::feature_bucket`, policy hash = sha256(grid, member id@version, skipped members)[..16]). Stores the chain's estimate (normalization is re-run, cheap and deterministic); constraints, S3, breakers and mode are re-evaluated on every hit; source becomes `Memo{id, version}`. Errors are never memoized; clean declines are. `outcome()` invalidates every entry of its key. TTL `decision.memo_ttl` (10 min), capacity `decision.memo_capacity` (10k, oldest evicted).
- **Single-flight + limiter.** Concurrent misses on one memo key share a `tokio::sync::OnceCell`; only the flight that runs the chain takes a permit of `decision.max_concurrent_decisions` (64), so identical fan-out costs one permit.
- **History and availability** (`store::StoreView`): per-key cache with TTL `history.cache_ttl` on the injected clock; past the TTL (or never cached) an unreachable store demotes enforce and blocks reductions (`store_unreachable`). Per-cluster filter (`history.cluster` / `share`; records without `cluster` always pass). Writes are buffered (bounded `history.buffer`, oldest dropped and counted) and flushed before the next successful read or write.
- **Groups** (`decide_group`): every member's context gets the union of the members' history (deduplicated on `(key, invocation, attempt, source)`); each member is decided, and the decision with the largest (memory, CPU) — demand = max across members — is given to every member with the same id, the group id and the lowest member mode. The group decision is recorded once.
- **Attempts** (`attempt_decide(req, AttemptContext)`): attempt 1 is a normal decision (the attempt context goes to the rule, so Orchestrator ladders count only with known `max_attempts`); under enforce, when `applied ≠ origin prior`, the attempt record is written first (`attempt_put_if_absent`, within half the remaining budget) with `reduced` and, for a reduced attempt behind a Caller ladder, an open lease. If the write fails, a reduced attempt is re-decided with reductions blocked. Attempt k ≥ 2 reads the record: missing/unreadable → max(origin prior, a fresh decision with reductions blocked); `reduced` → `applied₁ × factor^min(k−1, max_steps)` (the last step raised to `final_floor`), or the fresh decision if larger; otherwise the fresh decision, never below attempt 1 (memory and CPU). No ladder in `PlatformRef` → `Ladder::caller()` defaults.
- **Outcomes** (`outcome(rec) -> OutcomeReport`): buffered write; memo and history invalidation; join to the decision (`joined` counter → `rightsizer_outcome_join_ratio`); origin prior = record's, else the decision's, else the attempt record's. Attribution uses `core::safety::attribute` with `input_beyond_history = false` (so beyond-history failures still count as caused and trip the **input-bucket** breaker, as `core::breaker` expects) and `retry_at_or_above_prior_failed` from the invocation's stored attempts. A caused failure trips the workload (or bucket) breaker unless a later attempt succeeded or a ladder lease is still live (open and within `lease_ttl` of the failure's end). A classified outcome (not `unknown`, not an inferred failure) closes an open lease. Fleet and per-estimator-version breakers are evaluated (`core::breaker::evaluate`) over a process-local 31-day window of attributed attempts; trips are written only when the scope is not already open. Saved resource-hours are counted for enforced successes.
- **Replay** (`Engine::replay(records, &ReplayOptions)`, `replay.rs`): the one code path for `rsz shadow` and `rsz audit`. Records may come in any order and any keys (shadow = audit on the same snapshot is a test). Per run: context as of its start with only runs that *completed* before it (`started + duration ≤ start`, other invocations), prior = the run's origin prior or request, mode recommend, no memo; censoring-aware fit (`Exact` above → fail; `AtLeast` not above, `AtMost` above, or no peak → unknown; a non-reduction that succeeded → fit); a simulated breaker per key (first would-fail opens it, `recovery_shadow_fits` fits close it, no savings while open); priced failures (burn `t_peak_frac` (0.5 if unknown, ≥ 0.05) of the run at the decided size, then the ladder step that fits, or the rerun at the prior). Would-save in GiB·h, core·h and dollars (`price_per_hour`: packed on the given shapes, else the tuner's m6i.xlarge; linear; usage floor bills `max(request, peak)`; instance = cheapest covering shape), 95 % bootstrap of the per-run sum (SplitMix64, 1000 resamples, seed per key). Readiness from the decision as of now: `ready` (no reduction recommended, or S3(b) holds at the recommendation), `ladder-only` (memory-enforced platform with a ladder), else `not-ready` with ⌈(min_history − weight) / per-run weight⌉ more runs (½ per run when any peak is coarse).
- **Metrics** (`metrics.rs`, one `prometheus::Registry` per engine, `Metrics::render()`): every §13.1 metric the engine produces (`decisions_total`, `decision_latency_seconds`, `budget_expired_total`, `held_total`, `clamped_total`, `errors_total`, `caused_failures_total`, `outcome_join_ratio`, `saved_resource_hours_total`, `breaker_open`, `estimator_circuit_open` (registered; remote clients set it), `store_compaction_lag_seconds` (registered; compaction sets it)), plus `rightsizer_store_errors_total{op}`, `rightsizer_outcomes_dropped_total{kind}`, and the join ratio's numerator/denominator counters. Not here: `watcher_lag_seconds` (K8s watcher).
- **Config additions** (engine-owned `config.rs`): `decision.max_concurrent_decisions` (64), `decision.memo_ttl` (10 min), `decision.memo_capacity` (10k); `[estimator.members.*]` gained `client_cert`, `client_key`, `ca_cert`, `max_batch`, `coalesce_ms`, `params` (the estimators crate's request). Under `strict`, estimator-chain warnings (unknown or M4 member kinds) are config errors; otherwise they are logged and the builder's S1 fallback (`prior`) is used.

### `rsz` (M0)

- `recommend [-p PLATFORM] [--dry-run] FILE|-`: input is a `DecideRequest`, a JSON array of them, or a bare `WorkloadContext` (JSON or YAML). Mode and `max_mode` are forced to `recommend`. `-p k8s` / `-p slurm` dispatch through `rightsizer_plugin::Adapter` (+ `PlatformModel` where it exists) behind the `k8s` / `slurm` features; units with a group go through `decide_group`. `explain FILE|- | --decision ID` prints the decision plus estimate quantiles, explanation lines and context digest.
- `shadow [--tenant T|-n|--key K] [--since 14d] [--format text|json|html] [--per-run]`: runs from the store (`--tenant` needs key listing: the `json://` store has it; other stores fall back to keys with logged decisions), replayed for the window, everything older is history. HTML is one page with inline CSS, no scripts, no external assets.
- `history import jsonl FILE|-` (dedup counts), `show KEY`, `export --jsonl OUT` (key listing needed) / `--parquet OUT` (`rightsizer_history::export`), `compact [--dry-run]` (operator role). `outcome KEY --status S [--memory-peak Q] [--decision ID] [--invocation I] [--attempt N] [--duration D]` writes a `self_report`, `unverified` record through `Engine::outcome`. `breaker status | trip [--fleet|KEY] [--reason] | reset [KEY]` (no KEY = fleet; reset with the operator role). `control mode-ceiling [MODE]` (set with the operator role).
- Global flags: `--config PATH` (= `RIGHTSIZER_CONFIG`), `--store URI` (overrides `history.uri`; `json://PATH` is an interim single-file store with key listing, every other scheme goes to `rightsizer_history::open`), `-o text|json|yaml`. Exit codes: 0, 1 other, 2 config (load/validate errors; `strict` engine-build errors), 3 input parse (bad JSON/YAML, key, quantity, status, unknown platform). Without `strict`, a store that cannot be opened is a warning and decisions keep the prior.

### Parity (M0 exit criterion)

The k8s-2 rows are not reachable (Union run outputs `rt-paper-k2s1-rows`; no repo copy, release or CI artifact). `tests/parity/gen_k8s2_fixture.py` imports the tuner's `bench_all.scoring` / `arms` directly and scores a deterministic synthetic k8s-2-shaped set (120 memory-bench Job pods, 110 CPU-bench pods with per-second series, 60 GPU pods, 30 % sharded; 6–7 candidate requests each, OOM penalty 1/5/20, delay 1) into `tests/fixtures/parity_k8s2.json` (410 KB).

- **Costs** (`tests/parity.rs`): rightsizer's `CostModel`, fed the tuner's oracle demands as point masses (memory: a zero-width cell; CPU: the per-second series as an empirical distribution whose `expect` is the series mean; GPU: a point peak with the run's sharding/imbalance), the tuner's grids, m6i.xlarge and device nodes, ladders (memory ×2 to 8 attempts; next device up to 4) and the oracle as the only pool member: **5,532 / 5,532 costs within 1 %, max relative error 4.8e-12** (mem 2,424, cpu 2,223, gpu 885). This needs `CostSemantics::Tuner` (core change below). Under SPEC §7.2 semantics only 2,921 / 5,532 are within 1 % (mean 8.3 %, max 200 %). Out of scope and reported only: a GPU task asked with **no GPU** (180 costs, mean diff 54 %): the tuner retries it on a T4, rightsizer prices a failed start plus one OOM penalty and does not model the platform's retry.
- **rules-sized regret**: rightsizer's `rules-sized` with the tuner's tuned knobs (`margin` 1.5, cold `256…512 + k×MiB` as fitted, `margin_c`), decoded the tuner's way (cell covering demand × 1.05), on the fixture's 120 test rows: **50.7 %** vs the tuner's **68.4 %**. The gap is the deliberate SPEC §7.4 deviation `scale = max(1, b_now/b_hist)`: on the 100 warm rows the tuner scales the nearest run *down* in 57; with that one rule changed in the tuner's own `_sized_one` the tuner gets 33.10 % on warm rows and rightsizer 33.10 % (98 / 100 identical picks), asserted within 1 point. Cold rows differ by design (rightsizer adds neither boto3's 48 MiB nor parsed thread pools): tuner-with-scale-floor 52.5 % overall vs 50.7 %. The test also checks the tuner's own picks through rightsizer's cost path reproduce 68.40 %.

### Performance

`decide` on a memo hit, end to end on `MemoryStore` (seed → cached 32-record history → control/breaker reads (cached) → memo → decision rule on rs-grid-v1 with a ladder → decision log): **≈ 355 µs dry-run, ≈ 360 µs recorded** (criterion, M-series laptop). Almost all of it is the decision rule itself (core's bench: ≈ 0.33 ms); passing a precomputed `context_digest` would save ≈ 0.1 ms.

### Changes to shared crates

- `rightsizer-core::cost`: **additive** `CostParams.semantics: CostSemantics { Rightsizer (default), Tuner }`. `Tuner` changes three things only: `oracle_cost` includes the oracle's stretch; lateness is `delay × oracle_cost × (stretch − 1)`; an exhausted ladder ends failed (no resubmission at the prior; OOM penalty counts attempts − 1). `decide.rs` passes `Rightsizer`. The oracle search now also returns the oracle's stretch.
- Workspace `Cargo.toml`: `assert_cmd = "2.2.2"` (dev-dependency of `rightsizer-cli`). `rightsizer-engine` depends on `prometheus` and dev-depends on `criterion`.

### Integration points left (TODO in code)

1. **`rightsizer_features::build_context`** (`engine.rs` `context_of`, `context.rs`): not called yet because its future is not provably `Send` for callers that `tokio::spawn` a decision (`readers::read_pass`: `which.iter().filter_map(|&i| Some(async move { .. }))` into `buffer_unordered` trips rustc's higher-ranked inference: "closure ... must implement `FnOnce<(&'1 usize,)>`"). Collecting the futures into a `Vec` first (or iterating owned indices) should fix it. Until then the engine builds contexts itself: known input sizes, salted URI hashes, `features::code_info`, distribution markers, allowlisted labels; no reads. The SSRF-guarded `DefaultResolver` is already constructed for it.
2. **`rightsizer_history::CachedStore` / `BufferedWriter`**: `StoreView` keeps its own TTL caches and buffer because the engine's TTLs run on the injected `Clock` (the safety tests move time by hand; `CachedStore` uses `Instant`). Swap when `CachedStore` takes a clock; `StoreView::history()`'s `available` flag is what mode resolution reads.
3. **Key listing** (`store::KeyLister`): `HistoryStore::keys()` exists now with a default `Unimplemented`; `rsz shadow --tenant` and `history export --jsonl` should use it for factory stores (today: `json://` lists keys, others fall back to keys with logged decisions).
4. **Estimators' memo** (`rightsizer_estimators::memo`): the engine has its own (`memo.rs`, same key incl. unit, plus single-flight); merge when convenient.
5. **Slurm `PlatformModel`** for `rsz recommend -p slurm` (units get `PlatformRef::new("slurm")` and default state until it exists); `rsz history import k8s|slurm|…` importers; lease-expiry sweep (a failed attempt whose lease stays open past `lease_ttl` is unrecovered: trip on the next outcome or a compaction-time sweep).

### Findings for other crates

- **estimators / core (staged bytes)**: `rules-sized` and `history-peak` learn demand from *container* peaks (minus the overhead median), and container peaks already include tmpfs pages written by the unit; the decision then adds `manifest.memory_backed_bytes` again (`need = (demand + overhead + staged) × headroom`). Either the estimators subtract staged bytes from history peaks (like the overhead), or adapters set `memory_backed_bytes` only when history lacks it. The parity test leaves it unset.
- **estimators**: `build_chain` skips unknown member kinds with a warning (S1); the engine turns those warnings into config errors only under `strict`.

## rightsizer.models / serve

Code: `python/rightsizer/src/rightsizer/{models,serve}/`; tests:
`python/rightsizer/tests/{models,serve}/`; extras and entry points:
`python/rightsizer/src/rightsizer/models/DEPENDENCIES.md`. Every module
imports with the standard library alone (a test blocks torch, transformers,
vllm, sglang, numpy, flyte, ray, ...).

### Modules

| module | public API |
|---|---|
| `models.prompting` | the tuner's prompt, ported byte for byte (`SYSTEM`, `KEYS`, `OPTIONS`, `messages(row)`, `user_prompt`, `format_history`, `describe_inputs`); the mapping `row_from_context(ctx)`, `messages_from_context(ctx)` |
| `models.chat` | `ChatTokenizer` (`tokenizers` + `jinja2`, torch-free; `prefer_transformers=True` uses `AutoTokenizer`), `option_offsets` |
| `models.pointer` | `load_release(repo_or_dir, revision)` → `Release` (path, repo, **commit sha**, config, head tensors); `PointerHeads` (NumPy TriPointer); `PointerModel(release, backend)`: `probabilities[_batch]`, `logits_batch`, `estimate_batch(ctxs, axes)`, `info()`, `ready()` |
| `models.estimate` | `build_estimate(probs, options, ...)` → Estimate dict (serde shape), `need_to_demand`, `argmax_request` |
| `models.backends` | `TransformersBackend` (reference), `VLLMBackend`, `SGLangBackend`, `OpenAIBackend`; `hidden_states(prompts, positions) -> [float32 [k, hidden]]`, `generate(messages, n)`, `ready()`; `vllm_command`, `sglang_command`, `Launcher` (subprocess in its own process group, health wait, SIGTERM→SIGKILL teardown) |
| `models.generate` | `parse_answer`, `empirical_estimate`, `GenerativeModel` (n samples → empirical cells) |
| `models.merge` | `causal_lm_adapter` (re-layout the LoRA for `Qwen3ForCausalLM`), `merge` (peft `merge_and_unload` → full CausalLM checkpoint) |
| `models.trees` | `convert(repo_or_dir, revision, out)`, `feature_mapping`, `load_skops` (explicit type allowlist `TRUSTED_TYPES`) |
| `models.load` | `ServeOptions`, `build_estimator(opts)` (what `rightsizer serve` builds) |
| `models.cli` | `rightsizer models convert|merge|adapter|prompt`; `rightsizer(argv)` dispatches `serve` / `models` |
| `serve` | `EstimatorServer(estimator | factory=...)`, `wrap(fn | batch=...)`, `Estimator`, `Coalescer`, errors (`BadRequest` 400, `Unauthorized` 401, `Unsupported` 409, `Insufficient` 422, `RateLimited` 429, `Internal` 500, `Unavailable` 503); `python -m rightsizer.serve` |

### Server

- **Standard library** (`ThreadingHTTPServer`), not ASGI: the server parses
  JSON and queues work; the cost is the backend (vLLM/SGLang over HTTP, or an
  in-process model), so starlette/uvicorn would add dependencies to `serve`
  without buying throughput.
- **Batching.** A coalescer thread gathers jobs for `batch_window_ms` (10 ms
  default) or until `max_batch` contexts and makes one `estimate_batch` call
  per distinct `axes`; a whole `/v1/estimate:batch` is one job. A batch
  larger than `max_batch` is a 400. More than `max_pending` queued contexts is
  a 429.
- **Errors.** Codes are the ones the Rust client maps per batch item
  (`malformed`, `unsupported`, `insufficient`, `rate_limited`,
  `unavailable`, `internal`). 409 for: a request `grid`/`grids` or `features`
  the estimator does not serve, context `v > 1`, or `axes` with nothing the
  estimator estimates. Unknown axis names are 400. A retryable backend error
  is 503; any other exception is 500 (`internal`, retryable).
- **Health.** `/v1/health` is 503 until the factory returns and the backend's
  `ready()` is true (vLLM/SGLang: `GET /health`), and again whenever the
  backend stops answering; `/v1/info` and `/v1/estimate*` are 503 meanwhile.
  A load failure is reported in the 503 message.
- **Auth.** Bearer token (`--token-env VAR`, `--token-file PATH`, or
  `RIGHTSIZER_SERVE_TOKEN`), compared with `hmac.compare_digest`; every route
  but `/v1/health` (liveness probes) needs it. 401 `unauthorized` otherwise.
- **Privacy.** Contexts are never logged (only method, path, status,
  latency) unless `--log-contexts`.
- `/v1/info` adds `backend` and `base_model` to `EstimatorInfo` (serde ignores
  unknown fields). `features` is `features-v1` (the context schema the client
  sends). `id` = repo (or the local directory's name), `version` = commit sha
  (`snapshot_download`'s `snapshots/<sha>`; `local-<sha256 of config, head,
  adapter>` for a directory).
- Request `axes` accept `duration_s` as an alias of `EstimateAxis::Duration`'s
  serde name `duration`. The Estimate is filtered to the requested axes.

### WorkloadContext → the tuner's prompt (`prompting.row_from_context`)

| tuner row field | from the context | exact? |
|---|---|---|
| `platform` | `"flyte"` if `platform.kind == "flyte"`, else `"k8s"` (the tuner's only other layout: module + manifest, container peaks) | Flyte and K8s: yes; Slurm/Ray/...: out of the training distribution |
| `task_code` | `code.source`, else `""` (the default for remote estimators, SPEC §11: the model still answers, with an empty code block) | yes when the source is sent |
| `manifest` (K8s) | `manifest.extra.manifest_yaml` (a `{"kind": "str", "value": ...}` scalar; **adapter convention, new**), else a skeleton synthesized from `ManifestFeatures` (`kind: Job`, `initContainers` per init container, `emptyDir: {medium: Memory}` when `memory_backed_bytes`) | only with `manifest_yaml` |
| `inputs_desc` | one `describe_input` line per `InputMeta`, in context order | npy (shape, dtype), csv (header names), jsonl (first-record keys): yes when names are present (in-process, or `send = ["columns"]`). **parquet: no** — the tuner wrote `schema=[name: kind, ...]` with per-column dtypes (`double`, `int64`, `string (dictionary)`, ...); `ColumnsInfo` has names and per-kind *counts*, so we write `name: numeric` when all columns share a kind, names alone otherwise, `N columns: a numeric, b string` without names. Arrow, safetensors, dir, blob, scalar, model_ref: rendered in the same style (never seen in training). Order: rightsizer's canonical order is largest first, the tuner's the task's declaration order. Sizes without a byte count: `unknown size`, as the tuner. |
| `history_json` | successful `RunRecord`s only (failures, OOMs, cached and unmeasured records dropped), oldest first (rightsizer keeps newest first); per record: `inputs_desc` from its inputs, `memory_peak` → peak memory (Flyte: stored as `peak − 70 MiB` so the rendered number, after the tuner adds `FLYTE_RUNTIME_MIB`, is the measured container peak), `cpu_peak_window` → "peak … cores (busiest 15 s)" whatever `cpu_window_s` is, `cpu_mean` → "mean", `gpu_memory_peak` + `gpu_util_mean`×100 → GPU, `duration_s` → wall | yes (golden tests) |
| `hint` | `row_from_context(..., hint=...)` (stacked arms only) | yes |

The prior and labels are not in the tuner's prompt. `max_history` caps the
history (default: all, ≤ 32). Golden tests: 60 tuner rows through the port
(Flyte, K8s twins made by the tuner's own `k8s.twin`, hints, clipping) and 80
contexts built from the same facts through the mapping are byte-for-byte equal
to `resource_tuner.bench_all.prompting.messages`; parquet cases are equal
except inside the current input's `schema=[...]`.

### Pointer read-out → Estimate

- Hidden states: chat template with `enable_thinking=False`,
  `add_generation_prompt=True`; ids without special tokens; **left-truncated
  to `max_len`**; positions = last token and `last − off` per option.
  Offsets are computed once per tokenizer from the probe prompt the tuner
  uses and **checked against the head's `off{i}` buffers** (a mismatch means
  the prompt or tokenizer drifted: load fails).
- Heads in NumPy (float64; LayerNorm eps 1e-5), softmax(z / T_axis).
- Cells: the tuner's options are prefixes of rs-grid-v1, so probabilities map
  cell to cell (zero-padded); a foreign option is rebinned conservatively.
- `kind=demand` (default): GPU uses `gpu_threshold` as
  `tuner:bench_all/scoring.decode` does: P(none) ≥ threshold → point mass at
  "none", else P(GPU cell | some GPU). `kind=action`: one-hot at each axis'
  argmax (the `-argmax` rows; `rightsize_pointer.request`).
- **Memory basis (open item).** The tuner's memory label is the *need*
  `(peak + runtime) × 1.05` (`k8s.flyte_need_mib`), while SPEC §5.2 says
  estimators emit *demand* and the decision adds overhead and headroom. Default
  `--memory-basis need` passes the cells through (models.md §4 "cell to
  cell"), which counts runtime + headroom twice (conservative, up to about a
  cell). `--memory-basis demand` converts: demand = need / 1.05 − overhead
  (70 MiB on Flyte, 0 on K8s), spread log-uniformly within each cell.
- CPU: the tuner's CPU cell is the busiest-15 s request (mean on RightSizeMem
  rows); emitted as both `cpu_mean` (so the cost model uses it) and `cpu_peak`.
- Provenance: `estimator` = repo, `version` = commit sha, `grid` =
  `rs-grid-v1`, `features` = `features-v1`, `latency_ms` = per-context share.

### Backends (sources checked at vLLM v0.34.0, 2026-11-16, and SGLang v0.5.23, 2026-11-02)

- **vLLM**: `vllm serve <base> --runner pooling --convert embed
  --pooler-config '{"task":"token_embed","pooling_type":"ALL","use_activation":false}'
  --enable-lora --lora-modules rightsize=<adapter> --max-lora-rank 16`.
  `POST /pooling` `{"model": "rightsize", "input": [[ids...], ...], "task":
  "token_embed", "use_activation": false, "encoding_format": "base64",
  "embed_dtype": "float32", "endianness": "little", "add_special_tokens":
  false}` → `{"id", "object": "list", "created", "model", "data": [{"index",
  "object": "pooling", "data": <base64 of float32 [n_tokens·hidden]> | [[...]]}],
  "usage"}`. Health `GET /health`. Sources: task split `token_embed` /
  `token_classify` (`docs/models/pooling_models/README.md:344-356`); the task
  must be set at startup (`token_embed.md:18-22`); `normalize` removed in
  favour of `use_activation` (`vllm/config/pooler.py:131-134`), on by default
  (`tokwise/poolers.py:117-132`, `tokwise/heads.py:133-141`); router
  `vllm/entrypoints/pooling/pooling/api_router.py:25-33`; response
  `pooling/protocol.py:102-114`, base64 = raw `tensor2binary` bytes
  (`entrypoints/pooling/utils.py:86-92`); states are after the final RMSNorm,
  LM head skipped (`qwen3.py:327-337`, `qwen2.py:442`, `adapters.py:153-166`);
  LoRA on pooling models: the pooling class subclasses the CausalLM and
  inherits `SupportsLoRA` (`adapters.py:153`, `qwen3.py:280-281`), adapter
  chosen by `"model"` (`serve/engine/serving.py:198-217`); prefix caching is
  skipped for `token_embed` (`pooling_params.py:198-205`). The "extract hidden
  states" speculative-decoding connector writes files and is **not**
  post-norm; not used. macOS: no wheels (build from source); Linux CPU images
  `vllm/vllm-openai-cpu:v0.34.0-{x86_64,arm64}`.
- **SGLang**: `python -m sglang.launch_server --model-path <base>
  --return-hidden-states-mode full --enable-lora --lora-paths
  rightsize=<adapter> --max-loras-per-batch 1 --disable-radix-cache
  --chunked-prefill-size -1`. `POST /generate` `{"input_ids": [[...], ...],
  "sampling_params": {"max_new_tokens": 1, "temperature": 0},
  "return_hidden_states": true, "lora_path": ["rightsize", ...]}` → a list
  (an object for one prompt) of `{"text", "output_ids", "meta_info": {...,
  "hidden_states": [[row_0, ..., row_{N-1}], <one row per decode step>...]}}`;
  element 0 is the prefill block. Sources: `--enable-return-hidden-states` =
  `--return-hidden-states-mode full` (`arg_groups/fields/exec_.py:112-124`);
  request fields `io_struct.py:177-279`; `meta_info.hidden_states`
  (`tokenizer_manager.py:2598-2601`, `batch_result_processor.py:641-700`);
  post-norm (`models/qwen3.py:536-540`, `layers/logits_processor.py:866-872`);
  test `test/registered/core/test_hidden_states.py:49-108`. A radix-cache hit
  shrinks the prefill block and chunked prefill keeps only the last chunk's
  rows, hence the two flags; the backend refuses a block whose row count is
  not the prompt length. **LoRA together with `return_hidden_states` is not
  covered by an SGLang test** (unverified). No macOS support.
- **openai** (generate only): `POST /v1/chat/completions` with `n`; servers
  that ignore `n` are asked again. Qwen3: `chat_template_kwargs:
  {enable_thinking: false}`.
- **LoRA layout.** The tuner trains on `AutoModel` (peft
  `FEATURE_EXTRACTION`), so tensors are `base_model.model.layers.N...`;
  `causal_lm_adapter` rewrites them to `base_model.model.model.layers.N...`
  and `task_type` to `CAUSAL_LM`. Real vLLM v0.34 accepted **both** layouts
  for the pooling model (it strips `model.`); `--launch` uses the re-layout,
  `--launch --merge` serves a merged checkpoint (`--served-model-name
  rightsize`, no LoRA).

### Parity and what ran for real

On a tiny local release (random 2-layer Qwen3, hidden 64, real Qwen3
tokenizer, random LoRA r=4 on all seven projections, random TriPointer heads
saved with the tuner's own `head_module` keys, real KEYS/OPTIONS, max_len 1200
so long prompts are truncated); torch 2.15.0, transformers 5.23.0, peft 0.21.3,
CPU fp32:

| check | result |
|---|---|
| tuner `RightSizePointer` vs `transformers` backend + NumPy heads, 12 contexts | max \|ΔP\| = 1.2e-6, same argmax |
| `tokenizers` + jinja chat template vs `AutoTokenizer.apply_chat_template` | identical text and ids |
| vLLM mock (base64 and float) / SGLang mock vs transformers | max \|ΔP\| < 1e-6 |
| `rightsizer serve` stack (`build_estimator` + server) over the vLLM mock | estimates equal, validate against `schemas/core/1/estimate.json` |
| **real vLLM v0.34.0 CPU** (docker `vllm/vllm-openai-cpu:v0.34.0-arm64` on Apple Silicon, `--shm-size 4g`; tiny model hidden 128, head_dim 64, LoRA via `--lora-modules`) | max \|ΔP\| = 5.2e-6, 12/12 argmax; hidden states within 6.6e-6; the base model without the adapter differs (LoRA applied) |
| real vLLM, merged checkpoint started through `Launcher` (docker prefix) | max \|ΔP\| = 6.1e-6, 12/12; container gone after `stop()` |
| SGLang | mock only (no macOS build; CUDA images) |

The image is 5.46 GB unpacked (1.31 GB compressed) and was removed after the run.
Qwen3-1.7B itself was not downloaded.

### Trees (`rightsizer models convert`)

- `model.skops` is loaded with skops after `get_untrusted_types` is checked
  against `trees.TRUSTED_TYPES` (HGB regressor/classifier, `TreePredictor`,
  `_BinMapper`, the pinball/binomial losses and links, NumPy dtype/RNG
  helpers); anything else is refused.
- Main file (`rightsizer-trees/1`, via `rightsizer.export.export_sklearn_hgb`):
  axes `memory` (target `log2`, scale `2^20 / 1.05`: the tuner's target is
  log2 of the need, so this removes the headroom; the 70 MiB Flyte runtime is
  still in it) and `cpu_mean` (`log2`, cores). Every split is rewritten to
  `missing: "none"` (missing → 0 and compared): the tuner computed absent
  features as 0, never NaN, and unmapped features are always missing.
- **GPU** goes to a companion `<out>.gpu.json` (`rightsizer-trees-gpu/1`, new):
  the classifier as a logistic ensemble (`{"link": "logistic", "base",
  "trees"}`, P(GPU) = σ(base + Σ trees)), the `gpu_memory` quantile axis
  (`log2`, scale 2^30) and `gpu_threshold`. **TODO (Rust `gbt`)**: read it;
  a `gpu_memory` axis without the classifier would put every task on a GPU,
  so it is not in the main file. `--no-gpu` skips it.
- No novelty companion: the trees repo has no reference matrix.
- Feature mapping (tuner → features-v1), 184 of 209 mapped:
  `log_total_bytes`→`inputs.log2_bytes_total`, `n_inputs`→`inputs.count`,
  `log_bytes_{fmt}`→`fmt.{fmt}.log2_bytes`, `log_rows_visible`→`inputs.log2_rows`,
  `ncols`→`cols.total`, `npy_log_n|npy_log_row|npy_itemsize`→`npy.*`,
  `{m,c,g}_code_lines`→`code.lines` (tuner counts `\n`; Rust counts lines: equal
  for sources ending in a newline), `n_hist`→`hist.n_success`,
  `log_hist_mem`→`hist.log2_peak_max_mib`, `log_hist_mem_scaled`→`hist.log2_scaled_mib`,
  `log_hist_size_ratio`→`hist.log2_size_ratio`, `hist_mean_cores`→`hist.cpu_mean_max`,
  and every token feature `{m,c,g}_tok_X`, `cuda_X`, `k8s_X` → `tok.X`
  (the K8s tokens were matched on code + manifest by the tuner; on the source
  only here). **Unmapped (25, written as `tuner.<name>` → missing → 0)**:
  `{m,c,g}_log_max_literal`, `c_max_pool`, `c_n_pool_decls`, `c_min_work_items`,
  `g_assign_c`, `g_log_assign_{a,b,d}`, `g_log_batch`, `g_log_d2_layers`,
  `g_log_max_linear`, `g_log_sum_linear`, `hist_peak_cores`, `hist_used_gpu`,
  `hist_util`, `log_hist_gpu`, `ncol_{cat,f32,f64,i32,i64,str,text}`.
  Adding `code.max_literal_log2` (from the `max_literal=` token), the pool
  and GPU-code numerics, per-dtype column counts and GPU/peak-CPU history
  features to features-v1 would close most of the gap.
- Tested on a skops file with the published structure (209 real feature
  names, 3 levels): converted values equal sklearn's `predict` (rtol 1e-6),
  the classifier's probabilities equal `predict_proba` (1e-6).

### Findings for other crates

- **features vs estimators (token spelling).** `rightsizer-features::tokens::code_tokens`
  emits `tok:<token>` (and `num:...`, `max_literal=...`), but
  `rightsizer-estimators::features_v1` looks tokens up bare (`tok.read_parquet`
  is 1 when `"read_parquet"` is in `CodeInfo.tokens`; its test uses bare
  tokens). With real `CodeInfo` from the features crate, every `tok.*` feature
  is 0. The converter writes `tok.<token>`; one side should strip/add `tok:`.
- **Adapters (K8s).** For the pointer model's K8s layout, put the rendered
  Job manifest (resources removed) in `manifest.extra.manifest_yaml` as a
  string scalar when privacy allows; without it the server synthesizes a
  skeleton.
- **Privacy.** With the default remote privacy (no source, no column names),
  the pointer prompt has an empty code block and count-only schemas: far from
  the training distribution. Serving these models usefully needs
  `send_source = true` and `send = ["columns"]` for the model's URL.

## rightsizer-slurm

Crate `crates/rightsizer-slurm` plus the Lua side in `contrib/slurm/`.
No dependency on kube/axum/pyo3 (`cargo xtask deps-check` passes; `rsz
--no-default-features --features slurm` holds).

### Modules

`sbatch` (parsing), `options` (the 26.05 option table, extracted from
`src/common/slurm_opt.c`), `keys`, `adapter` (`SlurmAdapter`: extract /
render / validate), `platform` (`SlurmPlatform`), `inventory` (scontrol /
sinfo / sacctmgr parsers), `settings` (`SlurmSettings`, read from
`AdapterEnv.settings`), `token` (tokens, key ring), `attest` (what
`job_desc` carries after a patch, and its tokens), `sacct` (importer,
`SacctSource: OutcomeSource`), `wrapper` (`rsz slurm sbatch [--watch]`),
`client` (`rsz slurm decide-client`), `runner` (`CommandRunner`, so tests
fake `sbatch`/`sacct`/`squeue`).

### Native forms and patches

- `Native::Document` (a script) → `Patch::TextEdits`.
- `Native::Object {"kind":"sbatch","argv":[…],"script":…,"env":{…}}` (the
  wrapper) → `TextEdits` if every change is in the script, `Options` (long
  option → value; `het{k}/{name}` for component k ≥ 1) if every change is in
  argv, and `Options` carrying the script edits under `_script_edits` when
  both. `adapter::apply_patch` / `apply_argv_options` apply them, keeping each
  option's form (`-c8`, `-c 8`, `--mem 64G`, `--mem=64G`).
- `Native::Object {"kind":"cli_filter","type","offset","options":<slurm.json_cli_options>,"script"}`
  → `Options` with plain long names (what cli_filter applies).
- `render` uses the adapter's own settings (the trait gives it no env), so the
  daemon constructs `SlurmAdapter::new(settings)` with the inventory.

### Parsing (checked against sbatch 26.05 source, `src/sbatch/opt.c`)

- Magic words `#SBATCH`/`#SLURM` at column 0 only. The block ends at the
  first line whose first non-blank character is not `#`. Tokenizing follows
  `get_argument`: quotes group, `\` escapes, an unescaped `#` starts a comment.
  `hetjob`/`packjob` as the first word starts a component, and the rest of
  that line belongs to the new component.
- Precedence per component: script, then `SBATCH_*` env, then argv. `:` in
  argv separates components. Long options accept unique prefixes, as
  getopt_long does.
- Two memory forms in one component is a parse *problem*. sbatch rejects it
  ("--mem, --mem-per-cpu, and --mem-per-gpu are mutually exclusive"; also
  across script and argv), so extract skips with `Unsupported`.
- Units are per job component (`job`, or `het[k]`) and quantities are per
  node. CPU = cpus-per-task × tasks per node. Memory per node is `--mem`,
  `--mem-per-cpu` × CPUs/node, or `--mem-per-gpu` × GPUs/node.
  - With no memory option, the prior is the partition default
    (DefMemPerCPU/DefMemPerNode, else the cluster's), marked `defaulted`.
  - `--mem=0` takes the smallest usable node memory as its prior and is held.
  - Options set by `SBATCH_*` variables are held: they cannot be edited in place.
  - `--tmp` is held (NoGrid). `--cpus-per-gpu` holds CPU.
  - An untyped GPU request holds the type. Multi-node or multi-task units are
    `distributed`, so the GPU count is held.
  - Ceilings come from partition MaxMemPerNode/MaxMemPerCPU, assoc/QOS
    MaxTRESPerJob/PerNode/GrpTRES and the largest fitting node.
- Extensions: `#RIGHTSIZER pin=<axes>` and `#RIGHTSIZER off`, besides `key=`.

### Keys

- Tenant segments are escaped into the core charset: bytes outside
  `[A-Za-z0-9._@+,-]` become `=XX`, and `=` becomes `=3d`. `_` needs no
  escaping. An empty account is `none`, and accounts are lower-cased.
- The suffix is `#RIGHTSIZER key=`, else `--job-name` (only explicit names
  count; the default script-basename name is never used at submit time),
  else `sha256-<32 hex>` of the normalized script. Het components append
  `/het-{k}`.
- Normalization keeps the shebang, drops comments, blank lines and
  resource-bearing `#SBATCH` options (`mem*`, `cpus-per-task`, `gres`,
  `gpus*`, `cpus-per-gpu`, `tres-per-task`, `tmp`, `time`, `time-min`), and
  rewrites the kept directives as `#SBATCH --long=value`.
- `--wrap` commands are digested with placeholders for 6+ digit runs, dates
  and times, UUIDs and long mixed hex ids.
- cli_filter cannot see argv `:` separators, so offset 0 counts as het only
  when the script has `hetjob` lines.

### Rendering (§3)

- The user's memory form is kept and a second form is never added. When the
  job had no memory option, `--mem` is added, but only if the value differs
  from the default it replaces.
- Per-CPU/per-GPU values use the *new* CPU/GPU counts and round up to MiB.
  Unsuffixed values stay in MiB (`--mem=65536` → `24576`); suffixed values
  use the largest exact unit.
- `--mem=0` never changes. `--time` and `--tmp` are never rendered.
- GPU type changes only for typed requests, mapped through `[slurm.gres]`.
- New directives go after the component's last `#SBATCH` line, or into argv
  when the component has none or for `--wrap`. Rendering is idempotent; the
  tests render every golden twice.
- `validate` rejects these patches:
  - mixed or changed memory forms;
  - `--mem=0` changed;
  - MaxMemPerNode/MaxMemPerCPU exceeded;
  - no usable node (CPUs, memory minus MemSpecLimit, GPUs of the type) for the
    node count;
  - assoc/QOS MaxTRESPerJob, GrpTRES (the job alone) or MaxTRESPerNode exceeded.

### Tokens: the job_desc mapping (evidence: Slurm 26.05.5 in Docker, 2026-11-19)

The source is `src/plugins/job_submit/lua/job_submit_lua.c` `_get_job_req_field`
at tag `slurm-26-05-5-1`. I confirmed it with a dump `job_submit.lua`
on the test cluster (fake GPU node via `GresTypes=gpu`). Every number reaches
Lua as a float (`100.0`); unset values are nil or NO_VAL.

| sbatch | job_desc field (as seen in Lua) |
|---|---|
| `--mem=100M` / `1G` / `1500K` / `0` | `min_mem_per_node` = 100 / 1024 / 2 (MiB, rounded up) / 0; `min_mem_per_cpu` nil |
| `--mem-per-cpu=300` | `min_mem_per_cpu` = 300; `min_mem_per_node` nil (`pn_min_memory` = 9.22e18: the MEM_PER_CPU flag; unusable) |
| no memory option | both nil (`pn_min_memory` = 1.84e19 NO_VAL64) |
| `-c 2` | `cpus_per_task` = 2 **and** `tres_per_task` = `cpu=2`; unset → 65534 (NO_VAL16) |
| `--gres=gpu:2` / `gpu:a100:2` | `tres_per_node` = `gres/gpu:2` / `gres/gpu:a100:2` |
| `--gpus-per-node=a100:2` | `tres_per_node` = `gres/gpu:a100:2` |
| `--gpus=3` / `a100:4` | `tres_per_job` = `gres/gpu:3` / `gres/gpu:a100:4` |
| `--gpus-per-task=1` | `tres_per_task` = `gres/gpu=1` (note `=`) |
| `--mem-per-gpu=10G` | `mem_per_tres` = `gres/gpu:10240` (MiB); `min_mem_*` nil |
| `--tmp=2G` | `pn_min_tmp_disk` = 2048 (MiB); unset → 4294967294 (NO_VAL) |
| `--time=1-02:03:04` | `time_limit` = 1564 (minutes) |
| no `-A` | `account` nil; `default_account` is available (assoc lookup, no I/O) |

- `canonical(resources)` and the NUL-separated MAC message are specified in
  `token.rs`. TRES strings are normalized (`gres/gpu:a100=2`, `cpu` entries
  dropped, entries sorted) so that Slurm's `:`/`=` spellings and the older
  `gres:` prefix all agree.
- `job_desc.script` is byte-identical to the script file (sha256 checked).
  `--wrap` scripts are `#!/bin/sh\n# This script was created by sbatch --wrap.\n\n<cmd>\n`.
- Each het component has its own `job_desc.environment`, so one
  `RIGHTSIZER_TOKEN` per component works. job_submit.lua checks
  `RIGHTSIZER_TOKEN_<het_job_offset>` first.
- Both `--export=ALL,RIGHTSIZER_TOKEN=x` and `--export=NONE,RIGHTSIZER_TOKEN=x`
  deliver the variable.
- The token format is `base64url-nopad(0x01 | kid_len | kid | ULID | u64 BE expiry | mac32)`.
  It contains no `=`, `,`, `+` or `/`, so it is safe inside `--export` lists.
  For het submissions, `DecideResult.token` joins the per-component tokens
  with `.`.
- Key file: one key per line, `rsz-key-v1 <kid> <active|verify> <created> <b64url secret>`;
  at most two keys, exactly one active. `KeyRing::rotate` has three steps:
  `Stage` adds a verify-only key, `Promote` makes it active (the old key
  still verifies), `Retire` drops the old key. `save` writes 0600 atomically.
  Verification uses `hmac::verify_slice` (constant time) in Rust and an XOR
  accumulator in Lua.
- **End to end** (`RSZ_IT_SLURM=1 cargo test -p rightsizer-slurm --test
  e2e_slurm_cluster`): tokens signed in Rust from the *parsed* submission
  were verified by `contrib/slurm/job_submit.lua` in slurmctld for:
  - `--mem`, `--mem-per-cpu=1500K`, `--gres` + `--mem-per-gpu`, `--gpus`,
    `-n 2 --gpus-per-task`, `--gpus-per-node` + `--mem=0`, and `--wrap`;
  - tampering with `--mem`, `-c` or `--gres` gave `:user`.

### sacct

- **`sacct --json -B` returns the batch scripts but no steps** (26.05). The
  importer makes two calls and merges them (`merge_scripts`). It falls back
  to `--parsable2 --format=<sacct::PARSABLE_FORMAT>` when JSON fails.
- A job that never started has `time.start` = 4294967294 (NO_VAL) and is
  skipped. `required.memory_per_node` is MiB.
- Step usage is under `steps[].tres.requested.{max,total,average}`, with mem
  in bytes. Text `ReqMem` is the job total in 26.05, and older `n`/`c`
  suffixes are honoured.
- Het component 1 of a job whose steps all ran in component 0 has no steps,
  so its record has no peak.
- **Keys**: the AdminComment decision id goes through the optional
  `ImportConfig.resolver`. Otherwise the key is re-derived from the stored
  script plus `submit_line`. The submit line loses quoting, so `--wrap` is
  rebuilt from the stored wrap script. With no script, the job name is used.
- **Trust** is `Verified` when the account is non-empty.
- **Measurements**:
  - Memory is sampled at `resolution_s` = JobAcctGatherFrequency (`task=5`
    here).
  - The `MaxRSS × tasks-per-node` path is `AtLeast`, as the spec says,
    although it is really an upper bound.
  - CPU mean is `time.total ÷ elapsed ÷ nodes`, and `AtLeast` at ≥ 95% of the
    allocation.
  - I could not verify the `gres/gpumem` units without GPUs; the importer
    assumes bytes like `mem`.
- **Statuses**: CANCELLED, FAILED, NODE_FAIL and BOOT_FAIL → `Failed`;
  DEADLINE → `Timeout`. An OOM in any step makes the job `OomKilled`.

### Wrapper (`wrapper::run`)

- It edits a temporary copy named like the original, so the default job name
  is unchanged, and deletes it after sbatch returns.
- It sets `RIGHTSIZER_DECIDED=<id>` in sbatch's environment, which makes
  cli_filter skip.
- The token rides on `--export=<effective or ALL>,RIGHTSIZER_TOKEN=…`. It is
  attached only when a patch was applied, the same as cli_filter.
- Any error submits the job unchanged. Exit 4 when sbatch fails (its code is
  in `sbatch_exit` and on stderr).
- With `--watch`:
  - It polls `squeue`, then `sacct -X -P -o JobID,State`, until every row is
    terminal; the exit code is 0 only for COMPLETED.
  - On OOM of a patched attempt it climbs ×`memory_factor`; the last step is
    raised to at least the origin prior.
  - Arrays resubmit only the OOM tasks (`--array=<ids>`).
  - Leases come from `Decider::open_lease` before each reduced attempt; if
    that fails, the attempt runs at the prior. Ladder resubmissions get
    tokens from `Decider::sign`.

### cli_filter socket approach

Slurm's Lua 5.4 in the image has neither luaposix nor luasocket. So
cli_filter writes the JSON-RPC request to an `os.tmpname()` file and runs
`rsz slurm decide-client --socket S --timeout-ms 1000 < file` through
`io.popen`. `client::run` reads the user's script (the daemon cannot),
enforces a single deadline over connect, write and read, and prints the
response line. Any failure prints nothing; cli_filter then writes the
negative-cache marker `/tmp/rightsizer-cli-filter-$USER.neg` (60 s).

### Left for the daemon round

- The CLI has to wire `rsz slurm sbatch`, `decide-client`, `keygen`,
  `rotate-key` and `history import slurm` to these library functions.
- The daemon's `decide` should run `extract → decide → render →
  validate_parsed → attest::sign_patched` (uid from SO_PEERCRED, account
  validated against associations) and refresh `SlurmSettings.inventory`
  every 5 min from the inventory parsers.
- `rightsizer-engine::config::SlurmConfig` is `deny_unknown_fields`. The
  adapter reads extra keys from `AdapterEnv.settings` (`watch`, `prices`,
  `default_account`, `presized_submitters`, `job_acct_gather_frequency`,
  `inventory`), so the config section needs to grow with them.
