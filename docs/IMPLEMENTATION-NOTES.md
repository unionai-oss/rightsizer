# Implementation notes

This file is the implementation decision log. Each agent appended a section
for its crate or round as it worked, so the body is grouped by crate and
round. It is not ordered by date, newest first or otherwise, and later rounds
sometimes correct earlier sections in place. The two summary sections below,
**Deviations from the spec** and **Known limitations / open items**, give the
current state as of 2026-11-19, after the adversarial review round (commit
`6b831e5`). In the body, items that have since been fixed are struck through
and marked **resolved**, with the place of the fix. Items tagged "(in
progress, 2026-11-19)" are being fixed by concurrent work.

## Deviations from the spec

- **Extra crate `rightsizer-engine`.** The spec lists no engine crate. The async orchestration (store, estimator chain, memo, single-flight, budgets) is a separate crate. This keeps `rightsizer-core` sync and free of I/O, and the CLI, daemon, webhook and Python module all share it. [SPEC §10.1](spec/SPEC.md#101-crates), [ARCHITECTURE §2](ARCHITECTURE.md#2-repository-layout); notes: [rightsizer-engine / rsz](#rightsizer-engine--rsz).
- **Store paths carry the platform.** The tenant path segment is the qualified tenant (`flyte/acme/proj/dev`), and breaker scopes are `workload/{platform}/{tenant}/{hash}` (the legacy path is still read). The key hash is the first 32 hex characters of the SHA-256. [SPEC §8.1](spec/SPEC.md#81-stores); notes: [object-store layout](#object-store-layout-contract-for-rightsizer-history), [history API additions](#api-additions-all-additive-the-engine-can-keep-coding-against-the-existing-trait).
- **Local-filesystem conditional updates are emulated** with create-only `.lock` objects, because `LocalFileSystem` has no `PutMode::Update`. The emulation is exact among rightsizer processes, but it does not stop outside writers. [SPEC §8.1](spec/SPEC.md#81-stores); notes: [Object store behaviour](#object-store-behaviour).
- **No code tokens in the export or the snapshots**: no stored artifact carries them. [SPEC §14.1](spec/SPEC.md#141-data-for-the-model-factory); notes: [history deviations](#deviations-and-decisions).
- **Additive wire extensions** to the §5 types: `Decision.shape`, `RunRecord.origin_prior`, `RunRecord.staged_bytes`, `Constraints.ceiling_reason` and `PlatformState.cpu_granularity`. Holds are `Vec<Hold {axis, reason, detail?}>`. [SPEC §5](spec/SPEC.md#5-data-model-rightsizer-core); notes: [rightsizer-core](#rightsizer-core), [estimators](#rightsizer-estimators), [Ray Rust changes](#rust-changes-additive).
- **Python wire readers are stricter than serde.** They reject unknown keys unless `allow_unknown=True` (environment payloads pass it), and they check ranges and finiteness. Rust's env-override parser also reads a bare comma list as an array of strings. [SPEC §13.2](spec/SPEC.md#132-versions-and-upgrades); notes: [runtime open items](#open-items-to-reconcile-with-rust).
- ~~**Python storage credentials reach Rust through environment variables.** `storage_options` are exported to the `AWS_*` variables instead of being passed into Rust.~~ — **resolved**: options are passed explicitly (`StoreOptions`, `[history.storage_options]`, `_core.Engine(storage_options=…)`); nothing is exported. [SPEC §10.3](spec/SPEC.md#103-python-runtime-details); notes: [Round 4 fixes](#round-4-store-options-node-agent-auth-flyte-backfill-dedupe-2026-11-19).
- **Ray in-task measurement.** The decision travels in a keyword-only `_rightsizer` argument, not a per-task `runtime_env`, which would get its own worker pool. Peaks on reused workers are re-based on the lowest start RSS the worker has seen. They are `AtMost`, not `Exact`, when the task started more than 16 MiB above that RSS. [ray.md §6](spec/ray.md#6-outcomes); notes: [Ray mechanism](#mechanism-ray-2600-as-installed).
- **The K8s node agent is pulled, not pushed.** The watcher leader sends `GET /v1/containers/{pod_uid}/{container}` to the agent on the node, so the agent holds no store credentials. [kubernetes.md §6.2](spec/kubernetes.md#62-node-agent-optional-m3); notes: [Cluster components](#cluster-components-m2-m3-k8s-parts).
- **The K8s webhook decides dry-run and logs the decision itself**, so the validation result is in the logged decision. Cross-replica quota reservations ride on decision explanations (`reservation:{json}`), because the store has no key-value API. [kubernetes.md §3.4](spec/kubernetes.md#34-validation-before-emitting-a-patch-s2); notes: [Cluster components](#cluster-components-m2-m3-k8s-parts).
- **`rightsizer-plugin/1` extensions:** `initialize.protocols`; `initialize` is optional for shims; `status` extras; `DecideParams.{attempt, watch, dry_run}`; the daemon-only methods `lease` and `sign`; error codes `-32003` (forbidden) and `-32004` (no common version). [plugins.md §3.1](spec/plugins.md#31-protocol-rightsizer-plugin1); notes: [`rightsizer-plugin/1` as implemented](#rightsizer-plugin1-as-implemented).
- **The pointer model's memory basis defaults to `demand`.** The tuner's need cells are converted to demand (÷ 1.05, minus the runtime overhead) instead of being mapped cell to cell. `--memory-basis need` gives the models.md behaviour. [models.md §4](spec/models.md#4-from-option-probabilities-to-an-estimate); notes: [Pointer read-out](#pointer-read-out--estimate).
- **GPU trees go in a companion file** (`rightsizer-trees-gpu/1`, `<out>.gpu.json`). This is a new format that models.md does not have. [models.md §1](spec/models.md#1-what-is-published-and-how-each-is-served); notes: [Trees](#trees-rightsizer-models-convert).

## Known limitations / open items

Checked against the code at `6b831e5` plus the working tree on 2026-11-19.

**Core / engine**
- Writes made by other processes are invisible to a running engine until `history.cache_ttl` / `decision.memo_ttl` expire (10 min). Such writes include an import, the K8s watcher and Ray workers. `_core` has no invalidation call, and the watcher's memo pre-warm can memoize a prior decision for a key that has no history yet. Freshness and revalidation are (in progress, 2026-11-19). Notes: [K8s results](#integration-results-k3d-rsz-it-k3s-v1343-2026-11-19), [Ray](#open--for-other-agents).
- The following are (in progress, 2026-11-19) in the core crates:
  - trust capping and workload-key validation;
  - the URI-hash salt: with no salt the hash uses an empty one, so it is deterministic but guessable ([features](#rightsizer-features));
  - the S3(b) veto rule (S3(b) now counts only the successes that fit; [core](#rightsizer-core));
  - the `hf://` metadata cache, which serves a moving revision like `main` until eviction;
  - local-path allowlists for input reads.
- Fleet and per-estimator-version breakers are evaluated over a 31-day window of attempts that is local to the process (`rightsizer-engine/src/engine.rs`). Each replica sees only its own outcomes. Notes: [Engine behaviour](#engine-behaviour-spec-as-implemented).
- There is no `Engine::record_decision`. The K8s webhook decides dry-run, logs the decision itself and bumps the metrics by hand. Notes: [Engine / shared changes](#engine--shared-changes-additive).
- A run record whose `origin_prior` is present but empty (`{}`), or the wrong shape (a `ResourceSpec` instead of a flat `Requests`, which deserializes to an empty `Requests`), takes precedence over the attempt record's origin in `Engine::outcome` and `Engine::sweep`. S5 attribution then finds nothing reduced and never trips (found while seeding the K8s QA lease sweep). Today's writers copy it from the decision, so an empty one means an empty origin. Treating an empty `origin_prior` as absent would be the safer default.
- Adapters never set `RunRecord.staged_bytes`: the K8s watcher, the Prometheus importer, `apply --watch` and sacct all write `None`. Estimators then assume today's staging for past runs. Notes: [engine findings](#findings-for-other-crates).
- The `RIGHTSIZER_DECISION` payload is defined only in Python (`rightsizer_runtime.env`). In Rust, K8s extract checks only that the variable is present. Notes: [runtime open items](#open-items-to-reconcile-with-rust).
- The Rust `gbt` does not read the `rightsizer-trees-gpu/1` companion, so converted trees give no GPU axis. Notes: [Trees](#trees-rightsizer-models-convert), [round 3 deferred](#integration-round-3-core-set-core-features-estimators-history-engine-rsz-m0).

**History**
- sqlite always demotes enforce: `SqliteStore::shared()` is hard-coded `false`, and the `[history] durable/shared` overrides are not applied to sqlite (`rightsizer-history/src/{sqlite,factory}.rs`). Notes: [rightsizer-py notes](#notes-for-other-crates--next-round).
- Snapshots and exports have no code tokens, because the decision log does not keep them.
- On object stores, `decision(id)` lists the whole `decisions/` tree. That is acceptable for `rsz explain` but not on a hot path.

**Kubernetes**
- ~~The node agent's HTTP endpoint is unauthenticated, and the webhook uses axum's default 2 MB body limit.~~ — **resolved**: bearer token + NetworkPolicy in the chart; 16 MiB review limit. Notes: [Round 4 fixes](#round-4-store-options-node-agent-auth-flyte-backfill-dedupe-2026-11-19).
- Karpenter NodePool requirements and tolerations are not in the GPU-type intersection.
- `rightsizer.io/code-configmap` is recorded but never resolved. `input_credentials = "pod-service-account"` (TokenRequest, [kubernetes.md §4.3](spec/kubernetes.md#43-inputs-and-whose-credentials-read-them)) is not implemented.
- Kueue: fair sharing and preemption are not modelled, and `coveredResources` in different resource groups are treated independently. Notes: [Not yet / follow-ups](#not-yet--follow-ups).
- The adapter does not set `manifest.extra.manifest_yaml`, so the pointer model sees a synthesized skeleton instead of the real manifest.

**Slurm**
- `rsz recommend -p slurm` passes no `PlatformModel` (`rightsizer-cli/src/adapters.rs`), although the daemon uses `SlurmPlatform`. Notes: [Integration points](#integration-points-status-after-round-3).
- The `gres/gpumem` units are unverified (no GPUs to test on); the importer assumes bytes.
- With whole-CPU rendering and packed prices, a 1-CPU job at or below the node's memory per core (1 GiB on the 2-CPU / 2 GiB test nodes) is never reduced: the smaller request saves nothing in the model, so the decision keeps the prior and no token is issued. QA rerun: [Slurm QA](#qa-rerun-after-review-head-f9acdbd-2026-11-19-3335).
- The first decide after a daemon start and a `scontrol reconfigure` exceeded the 1 s cli_filter deadline once on the QA cluster (1.3 s). That submission is unchanged, and the user's negative cache then skips rightsizer for 60 s. It did not reproduce in isolation.
- Each `sbatch` takes 9.4 ms longer, from spawning `decide-client`. A resident helper or a Lua socket library would remove most of that. Notes: [Slurm e2e](#end-to-end-on-the-docker-cluster-2026-11-19-slurm-26055).

**Flyte**
- ~~The Python Flyte backfill does not dedupe against the key's full history.~~ — **resolved**: `Engine.import_runs` (`Importer`). Notes: [Round 4 fixes](#round-4-store-options-node-agent-auth-flyte-backfill-dedupe-2026-11-19).
- ~~`export_flyte_storage_env` exports `AWS_*` variables instead of passing storage options to Rust.~~ — **resolved**: replaced by `flyte_storage_options()` (read-only). Notes: [Round 4 fixes](#round-4-store-options-node-agent-auth-flyte-backfill-dedupe-2026-11-19).
- The devbox end-to-end run is incomplete. Scenarios b–i, the deploy round trip and the backfill against the devbox never ran. The latest history tuning and the parent pre-warm have never run inside a task image. Notes: [Not finished](#not-finished).
- Scalar memory treated as enforced was checked on the devbox only; Union deployments may set other platform defaults.

**Ray**
- The KubeRay tenant variables (`RAY_CLUSTER_NAMESPACE`, `RAY_CLUSTER_NAME`) are assumptions that still need aligning with the K8s webhook. Notes: [Ray open items](#open--for-other-agents).
- Ray's memory-monitor `OUT_OF_MEMORY` path is verified only in a Linux container; it cannot be produced on macOS.

**Python / models**
- ~~`rs.configure(storage_options=…)` and `python/rightsizer/src/rightsizer/_storage.py` export credentials to environment variables instead of passing them into Rust ([SPEC §10.3](spec/SPEC.md#103-python-runtime-details)).~~ — **resolved**: notes [Round 4 fixes](#round-4-store-options-node-agent-auth-flyte-backfill-dedupe-2026-11-19).
- 22 tuner features are not mapped to features-v1: pool and GPU-code numerics, per-dtype column counts, and GPU and peak-CPU history. Notes: [Trees](#trees-rightsizer-models-convert).
- With the default remote privacy settings (no source, no column names), the pointer model's prompt is far from its training distribution. Serving it usefully needs `send_source = true` and `send = ["columns"]`.
- SGLang: LoRA together with `return_hidden_states` is unverified (mock only; there is no macOS build).

**Plugins / daemon**
- The systemd notify and watchdog path has not been run under a real systemd. Notes: [Lifecycle](#lifecycle).

**Test infra**
- Parity uses a synthetic fixture shaped like k8s-2, because the real k8s-2 rows are not reachable. Notes: [Parity](#parity-m0-exit-criterion).
- Real vLLM ran only on a tiny random model; Qwen3-1.7B was never downloaded, and SGLang was mocked.

## Contents

- [rightsizer-runtime](#rightsizer-runtime)
- [Test infrastructure (`tests/infra`)](#test-infrastructure-testsinfra)
- [rightsizer-core](#rightsizer-core)
- [rightsizer-features](#rightsizer-features)
- [rightsizer-history](#rightsizer-history)
- [rightsizer-estimators](#rightsizer-estimators)
- [rightsizer-k8s](#rightsizer-k8s)
- [rightsizer-engine / rsz](#rightsizer-engine--rsz)
- [rightsizer.models / serve](#rightsizermodels--serve)
- [rightsizer-plugin: protocol, daemon (`rsz serve`), plugins (M3)](#rightsizer-plugin-protocol-daemon-rsz-serve-plugins-m3)
- [rightsizer-slurm](#rightsizer-slurm)
- [Integration round 3](#integration-round-3-core-set-core-features-estimators-history-engine-rsz-m0)
- [rightsizer-py / `rightsizer` (Python API)](#rightsizer-py--rightsizer-python-api)
- [rightsizer.ray (M3)](#rightsizerray-m3)
- [rightsizer.flyte (M1)](#rightsizerflyte-m1)
- [Round 4: store options, node agent auth, Flyte backfill dedupe](#round-4-store-options-node-agent-auth-flyte-backfill-dedupe-2026-11-19)

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
5. ~~**`AttemptContext`** is not in Rust yet.~~ — **resolved**: `rightsizer_core::AttemptContext` (`crates/rightsizer-core/src/types/platform.rs`, with `v`). Python writes
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
   *Fixed (integration round 3):* Rust now tries a TOML literal, then JSON
   (objects and arrays, as Python), then a bare comma list (Rust only: an
   array of strings), then a string.

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
  `{prefix}/v1/runs/flyte/{org}/{project}/{domain}/`. ~~Update flyte.md (or
  the layout) when the history crate lands.~~ — **resolved**: flyte.md §8.1 now gives the `…/v1/runs/flyte/{org}/{project}/{domain}/` prefix.
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
this backend a scalar-memory task **is** memory-enforced ~~(`memory_enforced`
should be true for scalar memory as well as tuples)~~ — **resolved**: `[flyte] scalar_memory_enforced` defaults to `true` (`rightsizer-engine` `FlyteConfig`, `rightsizer/flyte/_engine.py`). Recheck against Union
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
- **Config env overrides** (`rightsizer-engine`) parse values as TOML literals, so a JSON-style `["a","b"]` works ~~but JSON objects do not~~ — **resolved**: JSON objects and arrays are tried after TOML (`rightsizer-engine/src/config.rs`, integration round 3). A bare comma list becomes an array of strings, which differs from Python's leave-as-string. Typed sections use `deny_unknown_fields`, so typos are errors.
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

  Integer literals follow the tuner's `_NUM` regex exactly. The GPU bench's `Linear(a, b)` / tuple-assignment features are not ported. **This module is the single definition of the token wire format** (`PRESENCE_PREFIX`, `NUMERIC_PREFIX`, `MAX_LITERAL_PREFIX`, `CODE_LINES_PREFIX`, `parse_token` → `CodeToken`); `rightsizer_estimators::features_v1` reads tokens only through `parse_token` (fixed in round 3: the vector used to look tokens up bare, so every `tok.*` was 0). A bare token still parses as presence.
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
- **Time budget.** One deadline per context covers the code-file read and all input reads. Reads run up to 16 at a time. The read futures are collected into a `Vec` before `buffer_unordered` (a lazy `filter_map` of `async move` blocks defeated rustc's higher-ranked `Send` inference), so `build_context`'s future is `Send` (compile-time `build_context_future_is_send`). `ReadOptions.hash_only` (additive) does no I/O at all: refs keep what they know and URIs are only hashed (the admission path's `inputs_hash_only`). When the deadline passes, the in-flight futures are dropped, and each input keeps whatever was already published to its slot (format and size from HEAD first, then the extracted fields). The context is never failed.
- **Truncation with more than 64 refs.** Pass 1 reads sizes only (HEAD or listing) for all refs. The 64 largest (then by name) get a full read in pass 2, so truncation is deterministic and the full read is bounded.
- **Canonical input order (fixed in core, integration round 3).** `WorkloadContext::canonicalize` used to sort by `(bytes.unwrap_or(0), name)` only, so ties kept their input order (a proptest found it). The total order (size desc, name, known-before-unknown, canonical JSON) now lives in `rightsizer_core::canonical_input_order`; `canonicalize` and `rightsizer_features::canonical_inputs` both use it (`canonicalize_is_a_total_order`).
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
- **Round 3 (additive):** `CachedStore::with_clock(inner, opts, Arc<dyn rightsizer_core::clock::Clock>)` (TTLs on an injected clock; `new` uses `TokioClock`, wall time anchored to tokio's clock so paused-time tests still work), `clock()`, `options()`, `invalidate_control()` (breakers + control only). `BufferedWriter::{defer_run, defer_decision}` (queue untried), `defer_decision_now` (sync), `buffered()` (sync counter). The engine's `StoreView` is now built on these two (its own caches and buffer are gone).
- **Breaker scope paths (round 3):** `BreakerScope::path()` is `workload/{platform}/{tenant}/{hash}` (and `bucket/{platform}/{tenant}/{hash}/{bucket}`), like runs, so per-tenant IAM can tell platforms apart. `legacy_path()` / `read_paths()` give the N-1 path; the object store reads both (trips deduplicated by id), sqlite/postgres match `scope_path IN (new, legacy)`; writers use the new path only.

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
- ~~`BreakerScope::path()` for workload scopes is `workload/{tenant}/{hash}`, without the platform.~~ Fixed in round 3 (see API additions): the platform segment is included and the old path is still read.

## rightsizer-estimators

Tests: `cargo test -p rightsizer-estimators [--features remote]`; exporters:
`cd python/rightsizer && ../../.venv/bin/python -m pytest tests/export`
(`RIGHTSIZER_RUST_PARITY=1` also runs the Rust evaluator on fresh exports).

### Estimator behavior (all emit *demand*: container peak − overhead median − staged bytes, SPEC §5.2)

**Staged tmpfs bytes (round 3).** Container peaks include memory-backed staging; the decision adds `manifest.memory_backed_bytes` to demand. To count staging once, `rules-sized` and `history-peak` learn demand = peak − overhead median − staged, where staged = the record's new `RunRecord.staged_bytes` (wire addition, optional, skipped when `None`) or, for records without it, the current context's `memory_backed_bytes` (`common::memory_demand_obs`). The fallback is safe: need = max(peak − S, 16 MiB) + S ≥ peak. Only a recorded value is exact when staging changed between runs. `features-v1` history features keep the tuner's container-peak semantics (`memory_obs`). Test: `staged_bytes_are_not_counted_twice` (engine).

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
- **The one memo (round 3, additive):** `Memo::with_clock(grid, ttl, capacity, clock)`, `with_point_sigma`, `MemoAnswer {estimate, skipped, any_error}`, `get_answer` / `put_answer` (clean declines are stored; `any_error` answers never are; estimates stored normalized), `get_or_compute(&key, f)` (single-flight: concurrent misses share one computation), `invalidate_workload(key)`, `clear`. The engine's own memo is deleted; `rightsizer_engine::memo` re-exports these (`ChainResult` = `MemoAnswer`).
- **`features-v1` code features (round 3):** `tok.<t>` reads `tok:<t>` (or bare) via `rightsizer_features::tokens::parse_token`; new `num.<t>` (literal of `num:<t>=<n>`, 0 when absent), `code.max_literal_log2` (the tuner's `log_max_literal`: log2 of the largest literal > 1, else 0), `code.lines_log2` (from `code_lines_log2=`, available without the source). Vector names use the raw token string (`tok.read_parquet(`), which is what `python/rightsizer/src/rightsizer/models/trees.py` writes; it now also maps `{m,c,g}_log_max_literal` → `code.max_literal_log2` (22 tuner features unmapped instead of 25).

### Requests for the engine

- ~~`MemberConfig` (`deny_unknown_fields`) needs `client_cert`, `client_key`, `ca_cert`, `max_batch`, `coalesce_ms` and `params`~~ — done (pinned by `config::tests::keys_other_crates_read_are_accepted`).
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
  `cluster`. ~~The engine's `K8sConfig` (`deny_unknown_fields`) needs the
  extra keys (or a pass-through table) to be configurable from TOML.~~ — **resolved**: `[k8s] adapter` pass-through table, flattened by `rightsizer_k8s::runtime::settings_map` (integration round 3, item 8).
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
  `task-name` (configurable). The org label is trusted only when
  `namespace_template` contains `{org}`; otherwise the org is
  `default_org` (required; a different org label → not Flyte-keyed). The
  key goes on the first app container only.
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
  inline — honoured since round 3: `ReadOptions.hash_only`). `rightsizer.io/code-configmap` →
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
  one bare base64 key (kid `k0`); keys ≥ 16 bytes. Bound to the object:
  `bind = {namespace}/{Kind}/{base name}` is signed; extract/render accept
  a prior only on that object (a copied prior is ignored, so it can neither
  make another object look sized nor forge the origin used for
  attribution). `rsz k8s restore` checks the signature only (it runs with
  the user's credentials). `extract_all` returns sized units with the signed
  origin as their prior (the watcher's `origin_prior`).
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

### Cluster components (M2, M3 K8s parts)

Features: `engine` (internal: the async engine, no kube/axum) ⊂ `webhook`
(axum + rustls) and `cluster` (kube-rs). `--no-default-features` stays
kube-free (`cargo xtask deps-check` passes). New pure modules:
`manifests` (`rsz webhook manifests`), `view` (`ClusterView` trait,
`ClusterInputs`, `StaticView`).

- **Request path** (`admit::Admitter::review`): AdmissionReview in, response
  out, never an error. One internal deadline (`[k8s] webhook_deadline_ms`,
  default 1000, additive engine config) wraps the whole request with
  `catch_unwind`; errors/panics/timeouts → `allowed: true`, no patch,
  `rightsizer_webhook_fail_open_total{webhook,reason}`. Steps: excluded
  namespaces → cached cluster inputs (`KubeView`, 30 s TTL,
  stale-while-revalidate; `--view-ttl`) → `rightsizer.io/enabled=true`
  re-checked → `extract_documents` (skips) → `mode` (namespace label lowered
  by the object) → seeds' inputs filled from the prefetcher's `InputCache`
  (hash-only otherwise; `inputs_hash_only` is set by extract) → engine
  `decide` with `dry_run = true` and `budget_ms = deadline − elapsed −
  250 ms` → render mode = max unit mode (units below enforce are left out of
  an enforced object) → Autopilot `finalize` → `render_with(JsonPatch)` →
  `validate_at` → on rejection: decisions get `Prior{rejected,
  "validation:<check>…"}`, applied = prior, re-render annotation-only, a
  `warnings` entry; on success `reserve` → decisions written in a spawned
  task (`StoreView::put`), never on `dryRun`. The webhook records decisions
  itself (decide is dry-run) because the validation outcome must be in the
  logged decision; it bumps `rightsizer_decisions_recorded_total` and the
  join ratio gauge for the decisions it writes.
- **Shared ledger** (`ledger_shared::SharedLedger`): the history store has no
  key-value API, and an increase is only admitted together with a logged
  decision, so the reservation rides on the first decision's explanation
  (`reservation:{json}`); every replica re-reads the last 2 minutes of
  enforce decisions every 3 s. Two replicas can admit within one refresh period, so with
  `--replicas N` (Helm passes `replicas`) each replica's not-yet-synced
  increases plus a new one must fit 1/N of the headroom (`validate`,
  `ReservationLedger::{unsynced, peers}`); a reservation is synced 6 s
  after this replica first sees it in the log. `MemoryLedger::get` added.
- **Render changes** (adapter): admission patches add the label
  `rightsizer.io/sized: "true"` to pod templates (and bare pods) so the
  watcher's informer and the node agent select server-side; the
  `rightsizer.io/recommendation` units carry `"decision"` when a unit's
  decision id differs from the object's (per-unit join). `restore` removes
  the label (and label maps it alone filled).
- **TLS**: `ReloadingCert` (`ResolvesServerCert`) polls cert/key mtimes every
  10 s; a broken new pair keeps the old one. Signing keys reload the same way
  (`spawn_key_reload`). `rsz webhook rotate-key` prepends a new random key id
  (`k<UTC timestamp>`), keeps `--keep 2` keys, on a file and/or a Secret.
  rustls needs `ring` installed explicitly (`cluster::install_crypto`): the
  graph enables both ring and aws-lc-rs. `rsz webhook certs` writes a
  self-signed CA + serving cert (rcgen; leaf with AKI, which Python ≥ 3.13's
  verifier requires).
- **Watcher** (`cluster::watcher`): Lease leader election
  (`cluster::leader`, 15 s lease, 5 s renew, `POD_NAME` identity), pod
  informer on `rightsizer.io/sized`, namespace must be enabled. One record
  per terminated container (`state` and `lastState`), dedup on (pod uid,
  container, restart). Owner chain walked to the topmost known kind (Job →
  CronJob/JobSet, …); `extract_all` on it gives the key and the origin
  prior; `match_unit` picks the unit by container name (+ JobSet / PyTorch /
  Ray / Volcano pod labels). Status: OOMKilled; pod Evicted; DisruptionTarget
  preemption; Completed/exit 0; Job DeadlineExceeded; else Failed.
  Invocation = `rightsizer.io/invocation` (apply --watch) or the top owner's
  UID; attempt = `rightsizer.io/attempt` or the pod's rank among the Job's
  pods + restarts. Peaks: node agent, else Prometheus 2 scrape intervals
  after termination (`--peak-delay`), else for OOM `AtLeast(limit)`.
  `RunRecord.cpu_*` are **cores** (I had millicores first; seeding 150
  "cores" produced an 18-CPU decision). After each outcome the unit is
  re-decided dry-run to warm the memo. Decision ids read from annotations
  are dropped when the log holds that id for another key
  (`runtime::verified_decision_id`). Attempt = `(rank−1)·1000 + restart + 1`
  (collision-free); runs already in the store are not re-sent after a
  leader change. `Engine::sweep` runs every `--sweep-interval` (300 s)
  while leading. Metrics:
  `rightsizer_watcher_lag_seconds`, `rightsizer_watcher_records_total{status,
  peak_source}`, `rightsizer_watcher_leader`. 
- **Prefetcher** (`cluster::prefetch`): bounded channel from the webhook;
  reads only URIs under `[k8s.inputs.allow]` (namespace or `"*"` → prefixes;
  additive engine config) with the features crate's readers and SSRF guard
  (`[features] allow_hosts`), stores metadata in the `InputCache` (1 h TTL,
  10k), then re-decides the object's units (dry run) so the memo has the
  same feature bucket the next admission computes.
- **Node agent** (`cluster::node_agent`, `rsz node-agent`): pod watch on
  `spec.nodeName` + the sized label (a short OOM run is tracked within a
  second), cgroup dir found by container id under the cgroup root
  (cgroupfs and systemd layouts), 1 s polls of `memory.peak` (fallback: max
  of `memory.current`, then `AtLeast`) and `cpu.stat usage_usec` (mean over
  the container's life from its start time; busiest 1 s window); exited
  containers kept 15 min. **Handoff = pull**: `GET
  /v1/containers/{pod_uid}/{container}[?container_id=]`; the leader asks the
  agent pod on the node (label + `spec.nodeName` → pod IP, or a URL
  template). A push would land on any of the replicas behind the Service;
  the pull is made by the one replica that writes the record, the agent
  holds no store credentials, and there is no per-container API write.
- **Prometheus** (`cluster::prom`): the watcher's per-container queries and
  `rsz history import k8s --namespace NS --since 30d [--dry-run]`:
  rows from cAdvisor (working set max, first/last sample time, CPU increase
  and busiest `rate[w]`, throttling) and KSM (requests/limits with
  `unit="byte"|"core"`, terminated reasons, `kube_pod_container_state_started`
  for containers cAdvisor never scraped), owners `kube_pod_owner` →
  `kube_job_owner` (CronJob keys), invocations = Job name + the latest
  `kube_job_created` ≤ pod start (a recreated Job is another invocation),
  attempts = pod order within it. Censoring: within 5 % of the limit or
  OOMKilled → `AtLeast`; memory `resolution_s` = scrape interval; CPU peak
  `resolution_s = w`; DCGM `FB_USED` → `AtMost` when a device had > 1 pod.
  Pods of long-running controllers are not imported. Trust `Verified`.
- **Live ops** (`cluster::live`): `restore_live` = GET → restore ops → PUT
  with `resourceVersion`; a Job's template is immutable (422 → explicit
  message; restore the manifest instead); CronJobs restore in place.
  `apply_watch`: per attempt `Engine::attempt_decide` (attempt records +
  leases), memory = max(ladder step, engine's decision), validate, create,
  poll pods/Job, on OOMKilled delete and resubmit `{name}-rsz{n}`; outcomes
  via `Engine::outcome` with the ladder's invocation/attempt annotations so
  the watcher's records dedup with them.
- **CLI** (`crates/rightsizer-cli/src/k8s.rs`; main.rs only dispatches):
  `rsz webhook serve|manifests|certs|rotate-key`, `rsz node-agent`, `rsz k8s
  restore -f FILE|- | KIND/NAME -n NS [--dry-run]`, `rsz apply FILE|- [-f OUT]
  [--diff] [--watch]` (text edits; enforce needs `[k8s] signing_key_file`),
  `rsz history import k8s`. `-o` is also accepted after these subcommands.
  `rsz webhook manifests` quotes YAML-1.1 booleans (`- 'off'` in the
  objectSelector; kubectl would read a bare `off` as false). `kubectl rsz`
  = a `kubectl-rsz` symlink. `rsz recommend -p k8s` now passes `[k8s.adapter]`.
- **Helm** (`deploy/helm/rightsizer`): Deployment (2 replicas, anti-affinity,
  probes on the plain HTTP port, read-only root, non-root), PDB, Service,
  MWC with a self-signed bootstrap (Helm `genCA`, kept by `lookup`) or
  cert-manager (Issuer/Certificate + CA injection), signing-key Secret
  (generated once, `resource-policy: keep`), postgres URL Secret or
  `history.uri`, optional node-agent DaemonSet (10m/40Mi, read-only
  `/sys/fs/cgroup`, no hostPID), PrometheusRule (fleet breaker, caused-failure
  burn, `apiserver_admission_webhook_fail_open_count{name=~"jobs.rightsizer.io|pods.rightsizer.io"} > 0`,
  webhook fail-open, join ratio < 0.9, compaction lag > 6 h, estimator circuit
  open 30 m, p99 > 100 ms), ServiceMonitor, Grafana dashboard JSON (12
  panels). RBAC: get/list/watch only + Leases in the release namespace.
  `tests/integration/k8s/helm_test.sh`: lint + template assertions + `rsz
  config validate` of the rendered TOML.

### Engine / shared changes (additive)

- `rightsizer-engine::config::K8sConfig`: `webhook_deadline_ms` (1000),
  `inputs: K8sInputsConfig { allow }`, `lease_namespace`, `adapter`
  (`toml::Table` pass-through to `K8sSettings`). No behaviour change.
- No other engine/core changes; the webhook uses public `Engine::view().put`,
  `metrics().recorded/joined/join_ratio`, `attempt_decide`, `outcome`.
- **Requests**: (1) `Engine::record_decision(&Decision)` so surfaces that
  decide dry-run and finish the decision themselves (validation) are counted
  like the engine's own writes; ~~(2) a lease-expiry sweep the watcher can call;~~ — **resolved**: `Engine::sweep`, called by the watcher's `sweep_loop` (`cluster/watcher.rs`);
  (3) memo/history invalidation for histories written by other processes
  (an import while a webhook runs is invisible to it for up to 10 min —
  the suite seeds before starting the webhook).

### Integration results (k3d `rsz-it`, K3s v1.34.3, 2026-11-19)

`RSZ_IT_K8S=1 tests/integration/k8s/run.sh all` (setup, a, b, c, e, f, d;
~15 min): **32/32 PASS**. Store: postgres 16 in container `rsz-it-k8s-pg`
(127.0.0.1:15432; in-cluster via `host.k3d.internal`). Dev loop: debug
`rsz webhook serve` on 127.0.0.1:18443, registered by URL
`https://host.k3d.internal:18443` with the self-signed CA.

- a: recommend → annotation-only; enforce with 6 seeded Verified runs:
  2Gi → 1Gi (trust region: 2 steps) with a signed prior, 256Mi → 1536Mi
  increase; LimitRange max → `rejected:limitrange`, annotation-only;
  requests quota → increase clamped to 400Mi (S4); limits quota →
  `rejected:resource_quota`, annotation-only; Kueue (v1beta2 queue) Job
  sized at CREATE, Workload podSet and pod = 1Gi, pod has no prior, no
  nodeSelector; JobSet and PyTorchJob patched 2Gi → 1Gi and accepted, the
  JobSet's child Job not resized; RayCluster worker 2 CPU/2Gi → 1/1.5Gi with
  `num-cpus: "1"`, `memory: "1610612736"`, head `num-cpus: "0"` kept; Flyte
  labels → `flyte://testorg/flytesnacks/development/wf.t1` in
  `flytesnacks-development`, `k8s://rsz-it/rsz-it-a/pod/wf-t1/main` elsewhere;
  `RIGHTSIZER_DECISION` pod untouched; webhook stopped → pod created,
  `apiserver_admission_webhook_fail_open_count{name="pods.rightsizer.io"}` +2.
- b: OOM Job → `oom_killed`, `AtLeast(128Mi)`, Verified, joined; 100 MiB
  Job → `exact` 104 MiB at `resolution_s` 15 from Prometheus;
  `rightsizer_outcome_join_ratio{platform="k8s"} = 1`; watcher lag 35 s
  (= the 2-scrape peak delay); importer reproduces both (OOM via KSM only:
  the 4 s container was never scraped by cAdvisor).
- c: `rsz apply --watch`: 512Mi → 384Mi (history said 60 MiB), OOMKilled,
  `ladder-rsz1` at 1Gi succeeded; attempt record + lease in postgres; no
  breaker tripped.
- e: 1000 AdmissionReviews posted directly (8 threads, ~800 rps, all
  enforce-patched) + 100 apiserver server-side dry-runs: webhook histogram
  **p50 6.7 ms, p90 9.4 ms, p99 15 ms** (debug build, laptop); client p99 24 ms.
- f: `rsz k8s restore -f` of the enforced Job → original resources, no
  `rightsizer.io/*` left; live CronJob restored in place; live Job refused
  (immutable template) with an explicit message.
- d: Helm install (image `rsz-it/rsz:dev`, self-signed certs, 2 replicas,
  node agent): one Lease holder; recommend annotation and enforce 2Gi → 1Gi
  in-cluster; OOM and success records from the in-cluster leader with node
  agent peaks (`resolution_s` 1, 104 MiB exact).

Findings: history imported by another process while a webhook runs is
invisible to it until the history-cache/memo TTLs (10 min), and the
watcher's memo pre-warm can memoize a prior decision for a key that has no
history yet — the suite seeds before starting webhooks. The rsz-it
Prometheus (1 GiB limit) was OOMKilled under this suite's churn;
`tests/infra/k8s/prometheus-values.yaml` now sets 2 GiB.

### QA rerun after review (HEAD `f9acdbd`, 2026-11-19)

Linux `rsz` and `rsz-it/rsz:dev` rebuilt from HEAD; fresh k3d `rsz-it`;
dev loop with the debug host binary. Three runs (results in
`tests/infra/.state/qa/k8s-run{1,2}-results.tsv`, `k8s-gd.log`):

| run | scope | result |
|---|---|---|
| 1 | `all` (setup a b c e f d; no new checks yet) | 26/27: only `a7.flyte-matching-namespace` failed: a harness bug (`flytesnacks-development` was never created on a fresh cluster) |
| g/d | `setup g d` with the new checks | 11/12: `g4` failed on a seed bug (`origin_prior` written as a `ResourceSpec`; it is a flat `Requests`) |
| 2 | `all` with every fix and check | 35/38. `b2`, `b4`: the laptop slept 17:09–17:45 during the run, so Prometheus had no samples for the steady job. `d7`: NetworkPolicy race (below). `a5`'s JobSet fell back to the prior (budget expired) at 17:46:05, between a wake and the next sleep |

Every check passed at least once on HEAD. Each failure above is
environmental or a harness defect, and each harness defect is fixed.

- **Latency (e):** run 1: 1000 direct AdmissionReviews + 100 apiserver
  dry-runs, webhook histogram **p50 10.3 ms, p90 14.3 ms, p99 19.2 ms**
  (target p99 < 100 ms; debug build; client p99 21.9 ms, 626 rps). In run 2
  (on battery, after sleep) p99 was 92.5 ms.
- **New checks (scenario `g`, plus d6/d7):**
  - g1, copied prior: train's genuine `rightsizer.io/prior` (bind
    `rsz-it-enf/Job/train`, origin 2Gi) is copied onto a 4Gi Job `copy`. The
    copy is enforced 4Gi → 2Gi with a new prior bound to `rsz-it-g/Job/copy`
    and origin 4Gi. The copied annotation is ignored.
  - g2, forged Flyte org: a pod labelled `organization: evilorg` in
    `flytesnacks-development` is keyed `k8s://rsz-it/flytesnacks-development/pod/wf-t1/main`.
    With `organization: testorg` (= `default_org`) the same pod gets
    `flyte://testorg/flytesnacks/development/wf.t1`.
  - g3, revalidation: a key decided with no history (2Gi kept) is seeded
    by another process while the webhook runs. 35 s later the same Job is
    enforced 2Gi → 1Gi; before, this took the 10 min TTL.
  - g4, lease sweep: an attempt row with an open lease is seeded in
    postgres, plus an inferred OOM at the reduced 512Mi. The importer's
    outcome leaves it pending. The watcher (`--sweep-interval 5`,
    `lease_ttl = "60s"`) then trips `workload k8s://rsz-it/rsz-it-g/job/swept/main`
    with reason `unrecovered_failure` and closes the lease (`outcome: oom_killed`).
  - d6/d7, node agent: from an ordinary pod in `rsz-it-d`, every request is
    dropped by the chart's NetworkPolicy (k3s/kube-router enforces it). From
    a pod in `rightsizer` that copies the webhook's labels (passes the
    policy), `/healthz` returns 200 and `/v1/containers/…` returns **401**
    without a token and with a wrong one. This holds on both nodes. The
    first request from a new pod is dropped while kube-router admits the
    pod's IP, so the probe now retries for up to 30 s.
- `helm_test.sh`: 10/10 (lint, `cluster` required, four variants
  rendered and `rsz config validate`d).
- Not re-checked here: the 16 MiB body limit (`tests/admit.rs` covers it)
  and replica-split ledger behaviour beyond d1 (one Lease holder, 2 ready
  replicas).

### Not yet / follow-ups

- Karpenter NodePool requirements in the GPU-type intersection; tolerations.
- ConfigMap code resolution (`rightsizer.io/code-configmap`); M3
  `input_credentials = "pod-service-account"` (TokenRequest).
- Kueue: no fair-sharing/preemption modelling; `coveredResources` across
  several resource groups are treated independently.
- ResourceQuota on **requests** clamps increases at decision time (S4, the
  quota is in `PlatformState`), so a requests quota never reaches
  validation; a **limits** quota does (tested).

## rightsizer-engine / rsz

Tests: `cargo test -p rightsizer-engine -p rightsizer-cli` (safety, engine,
replay, parity, CLI). Bench: `cargo bench -p rightsizer-engine --bench engine`.
Parity fixture: `PYTHONPATH=<tuner checkout> uv run --no-project --with numpy python crates/rightsizer-engine/tests/parity/gen_k8s2_fixture.py crates/rightsizer-engine/tests/fixtures/parity_k8s2.json`.

### Engine behaviour (SPEC as implemented)

- **Construction.** `Engine::builder(config).store(..).chain(..).clock(..).role(..).build().await`; `Engine::new(config).await` opens `history.uri` and builds `[estimator]`. Both are async (the history factory and the chain builder are). A config error (validation, store open, chain build) fails `build()` only with `strict = true`; otherwise the engine is **inert**: every decision is `Prior{config_error}` with the message as detail.
- **`decide` never fails (S1).** One wrapper applies the hard budget (`tokio::time::timeout`, `budget_ms` or `policy.budget_ms`), catches panics (`catch_unwind` on the whole future, estimator calls included) and counts both. Budget/panic fallbacks are *deferred* into the write buffer, never written inside a spent budget. Normal decisions are written with 80 % of what is left, else deferred.
- **Mode resolution** (`core::policy::effective_mode`): requested (request ∨ config) ∧ `min(request.max_mode, config.max_mode)` ∧ `mode_ceiling` (60 s cache; no control object = `enforce`; unreadable = keep mode, block reductions) ∧ breakers (workload ∨ input-bucket, fleet; estimator-version breakers make the chain skip that member, and join the memo's policy hash) ∧ history availability ∧ store durability. `off` reads and records nothing. `observe` does not estimate (prior, reason `mode`) but is recorded (outcomes join to it). Every demotion is appended to the explanation.
- **Half-open recovery** is computed at read time: after a non-manual trip, the key's runs since the trip whose decision was below enforce and whose measured (`Exact`/`AtMost`, `Observed`) peak fit `applied` count as shadow fits (at most 4 × `recovery_shadow_fits` decision lookups).
- **Memo** (`rightsizer_estimators::memo::Memo`, round 3; `memo.rs` re-exports it): key = (workload key, unit, code digest, `features::feature_bucket`, policy hash = sha256(grid, member id@version, skipped members)[..16]). Stores the chain's estimate **normalized** with `decision.point_sigma` (so a hit decides exactly like the miss that filled it); constraints, S3, breakers and mode are re-evaluated on every hit; source becomes `Memo{id, version}`. Errors are never memoized; clean declines are. `outcome()` invalidates every entry of its key. TTL `decision.memo_ttl` (10 min) on the injected clock, capacity `decision.memo_capacity` (10k, expired then oldest evicted).
- **Single-flight + limiter.** `Memo::get_or_compute`: concurrent misses on one memo key share a `tokio::sync::OnceCell`; only the flight that runs the chain takes a permit of `decision.max_concurrent_decisions` (64), so identical fan-out costs one permit.
- **History and availability** (`store::StoreView`, round 3: over `rightsizer_history::CachedStore` + `BufferedWriter`, both on the injected clock): per-key cache with TTL `history.cache_ttl`; past the TTL (or never cached) an unreachable store demotes enforce and blocks reductions (`store_unreachable`; no stale records are used then). Per-cluster filter (`history.cluster` / `share`; records without `cluster` always pass). Writes go through the cache (own writes visible), are buffered (bounded `history.buffer`; when full the **new** write is dropped and counted) and flushed before the next successful read or write.
- **Contexts from seeds** (round 3): `rightsizer_features::build_context_with` (bounded reads within min(`features.budget_ms`, half the decision budget left), `features.max_input_bytes` per input, the SSRF guard from `features.allow_hosts`, salted URI hashes). `manifest.extra.inputs_hash_only` (`true`/`"true"`/`1`) → no input is read, URIs are only hashed. The given history (groups: the union) is kept as is, truncated to `decision.history_limit`. `context::complete_seed` stays as the I/O-free variant.
- **Sweep** (round 3, `Engine::sweep(dry_run) -> SweepReport`): for every key, a failed invocation whose ladder lease is still open `policy.lease_ttl` past its last failure's end (and that no later attempt recovered) is unrecovered: if attributed caused or still pending (unclassified), the workload (input-bucket when beyond history) breaker trips (`UnrecoveredFailure`) if closed, and the lease is closed with the failure's status so it is swept once. `rsz history compact` runs it and reports `sweep` (keys, expired leases, tripped). The watcher can call it periodically.
- **Key listing** (round 3): `Engine::keys()` = an injected `KeyLister` if any, else `HistoryStore::keys()` (every backend). `KeyLister` / `key_lister()` are kept as compat shims.
- **Groups** (`decide_group`): every member's context gets the union of the members' history (deduplicated on `(key, invocation, attempt, source)`); each member is decided, and the decision with the largest (memory, CPU) — demand = max across members — is given to every member with the same id, the group id and the lowest member mode. The group decision is recorded once.
- **Attempts** (`attempt_decide(req, AttemptContext)`): attempt 1 is a normal decision (the attempt context goes to the rule, so Orchestrator ladders count only with known `max_attempts`); under enforce, when `applied ≠ origin prior`, the attempt record is written first (`attempt_put_if_absent`, within half the remaining budget) with `reduced` and, for a reduced attempt behind a Caller ladder, an open lease. If the write fails, a reduced attempt is re-decided with reductions blocked. Attempt k ≥ 2 reads the record: missing/unreadable → max(origin prior, a fresh decision with reductions blocked); `reduced` → `applied₁ × factor^min(k−1, max_steps)` (the last step raised to `final_floor`), or the fresh decision if larger; otherwise the fresh decision, never below attempt 1 (memory and CPU). No ladder in `PlatformRef` → `Ladder::caller()` defaults.
- **Outcomes** (`outcome(rec) -> OutcomeReport`): buffered write; memo and history invalidation; join to the decision (`joined` counter → `rightsizer_outcome_join_ratio`); origin prior = record's, else the decision's, else the attempt record's. Attribution uses `core::safety::attribute` with `input_beyond_history = false` (so beyond-history failures still count as caused and trip the **input-bucket** breaker, as `core::breaker` expects) and `retry_at_or_above_prior_failed` from the invocation's stored attempts. A caused failure trips the workload (or bucket) breaker unless a later attempt succeeded or a ladder lease is still live (open and within `lease_ttl` of the failure's end). A classified outcome (not `unknown`, not an inferred failure) closes an open lease. Fleet and per-estimator-version breakers are evaluated (`core::breaker::evaluate`) over a process-local 31-day window of attributed attempts; trips are written only when the scope is not already open. Saved resource-hours are counted for enforced successes.
- **Replay** (`Engine::replay(records, &ReplayOptions)`, `replay.rs`): the one code path for `rsz shadow` and `rsz audit`. Records may come in any order and any keys (shadow = audit on the same snapshot is a test). Per run: context as of its start with only runs that *completed* before it (`started + duration ≤ start`, other invocations), prior = the run's origin prior or request, mode recommend, no memo; censoring-aware fit (`Exact` above → fail; `AtLeast` not above, `AtMost` above, or no peak → unknown; a non-reduction that succeeded → fit); a simulated breaker per key (first would-fail opens it, `recovery_shadow_fits` fits close it, no savings while open); priced failures (burn `t_peak_frac` (0.5 if unknown, ≥ 0.05) of the run at the decided size, then the ladder step that fits, or the rerun at the prior). Would-save in GiB·h, core·h and dollars (`price_per_hour`: packed on the given shapes, else the tuner's m6i.xlarge; linear; usage floor bills `max(request, peak)`; instance = cheapest covering shape), 95 % bootstrap of the per-run sum (SplitMix64, 1000 resamples, seed per key). Readiness from the decision as of now: `ready` (no reduction recommended, or S3(b) holds at the recommendation), `ladder-only` (memory-enforced platform with a ladder), else `not-ready` with ⌈(min_history − weight) / per-run weight⌉ more runs (½ per run when any peak is coarse).
- **Metrics** (`metrics.rs`, one `prometheus::Registry` per engine, `Metrics::render()`): every §13.1 metric the engine produces (`decisions_total`, `decision_latency_seconds`, `budget_expired_total`, `held_total`, `clamped_total`, `errors_total`, `caused_failures_total`, `outcome_join_ratio`, `saved_resource_hours_total`, `breaker_open`, `estimator_circuit_open` (registered; remote clients set it), `store_compaction_lag_seconds` (registered; compaction sets it)), plus `rightsizer_store_errors_total{op}`, `rightsizer_outcomes_dropped_total{kind}`, and the join ratio's numerator/denominator counters. Not here: `watcher_lag_seconds` (K8s watcher).
- **Config additions** (engine-owned `config.rs`): `decision.max_concurrent_decisions` (64), `decision.memo_ttl` (10 min), `decision.memo_capacity` (10k); `[estimator.members.*]` gained `client_cert`, `client_key`, `ca_cert`, `max_batch`, `coalesce_ms`, `params` (the estimators crate's request). Under `strict`, estimator-chain warnings (unknown or M4 member kinds) are config errors; otherwise they are logged and the builder's S1 fallback (`prior`) is used.

### `rsz` (M0)

- `recommend [-p PLATFORM] [--dry-run] FILE|-`: input is a `DecideRequest`, a JSON array of them, or a bare `WorkloadContext` (JSON or YAML). Mode and `max_mode` are forced to `recommend`. `-p k8s` / `-p slurm` dispatch through `rightsizer_plugin::Adapter` (+ `PlatformModel` where it exists) behind the `k8s` / `slurm` features; units with a group go through `decide_group`. `explain FILE|- | --decision ID` prints the decision plus estimate quantiles, explanation lines and context digest.
- `shadow [--tenant T|-n|--key K] [--since 14d] [--format text|json|html] [--per-run]`: runs from the store (keys from `Engine::keys()`, every store), replayed for the window, everything older is history. HTML is one page with inline CSS, no scripts, no external assets.
- `history import jsonl FILE|-` (dedup counts), `show KEY`, `export --jsonl OUT` (every store) / `--parquet OUT` (`rightsizer_history::export`), `compact [--dry-run]` (operator role; also runs `Engine::sweep`). `outcome KEY --status S [--memory-peak Q] [--decision ID] [--invocation I] [--attempt N] [--duration D]` writes a `self_report`, `unverified` record through `Engine::outcome`. `breaker status | trip [--fleet|KEY] [--reason] | reset [KEY]` (no KEY = fleet; reset with the operator role). `control mode-ceiling [MODE]` (set with the operator role).
- Global flags: `--config PATH` (= `RIGHTSIZER_CONFIG`), `--store URI` (overrides `history.uri`; every scheme goes to `rightsizer_history::open`; the interim `json://` single-file store was removed in round 3 — it existed only for key listing, which every backend has now — and a `json://` URI is a configuration error naming `sqlite:///path` / `file:///dir`). `rsz` now enables the engine's `object-store` and `sqlite` features (the workspace dependency has `default-features = false`, so `rsz` could open neither before)., `-o text|json|yaml`. Exit codes: 0, 1 other, 2 config (load/validate errors; `strict` engine-build errors), 3 input parse (bad JSON/YAML, key, quantity, status, unknown platform). Without `strict`, a store that cannot be opened is a warning and decisions keep the prior.

### Parity (M0 exit criterion)

The k8s-2 rows are not reachable (Union run outputs `rt-paper-k2s1-rows`; no repo copy, release or CI artifact). `tests/parity/gen_k8s2_fixture.py` imports the tuner's `bench_all.scoring` / `arms` directly and scores a deterministic synthetic k8s-2-shaped set (120 memory-bench Job pods, 110 CPU-bench pods with per-second series, 60 GPU pods, 30 % sharded; 6–7 candidate requests each, OOM penalty 1/5/20, delay 1) into `tests/fixtures/parity_k8s2.json` (410 KB).

- **Costs** (`tests/parity.rs`): rightsizer's `CostModel`, fed the tuner's oracle demands as point masses (memory: a zero-width cell; CPU: the per-second series as an empirical distribution whose `expect` is the series mean; GPU: a point peak with the run's sharding/imbalance), the tuner's grids, m6i.xlarge and device nodes, ladders (memory ×2 to 8 attempts; next device up to 4) and the oracle as the only pool member: **5,532 / 5,532 costs within 1 %, max relative error 4.8e-12** (mem 2,424, cpu 2,223, gpu 885). This needs `CostSemantics::Tuner` (core change below). Under SPEC §7.2 semantics only 2,921 / 5,532 are within 1 % (mean 8.3 %, max 200 %). Out of scope and reported only: a GPU task asked with **no GPU** (180 costs, mean diff 54 %): the tuner retries it on a T4, rightsizer prices a failed start plus one OOM penalty and does not model the platform's retry.
- **rules-sized regret**: rightsizer's `rules-sized` with the tuner's tuned knobs (`margin` 1.5, cold `256…512 + k×MiB` as fitted, `margin_c`), decoded the tuner's way (cell covering demand × 1.05), on the fixture's 120 test rows: **50.7 %** vs the tuner's **68.4 %**. The gap is the deliberate SPEC §7.4 deviation `scale = max(1, b_now/b_hist)`: on the 100 warm rows the tuner scales the nearest run *down* in 57; with that one rule changed in the tuner's own `_sized_one` the tuner gets 33.10 % on warm rows and rightsizer 33.10 % (98 / 100 identical picks), asserted within 1 point. Cold rows differ by design (rightsizer adds neither boto3's 48 MiB nor parsed thread pools): tuner-with-scale-floor 52.5 % overall vs 50.7 %. The test also checks the tuner's own picks through rightsizer's cost path reproduce 68.40 %.

### Performance

`decide` on a memo hit, end to end on `MemoryStore` (seed → cached 32-record history → control/breaker reads (cached) → memo → decision rule on rs-grid-v1 with a ladder → decision log): **≈ 355 µs dry-run, ≈ 360 µs recorded** (criterion, M-series laptop). Almost all of it is the decision rule itself (core's bench: ≈ 0.33 ms); passing a precomputed `context_digest` would save ≈ 0.1 ms.

### Changes to shared crates

- `rightsizer-core::cost`: **additive** `CostParams.semantics: CostSemantics { Rightsizer (default), Tuner }`. `Tuner` changes three things only: `oracle_cost` includes the oracle's stretch; lateness is `delay × oracle_cost × (stretch − 1)`; an exhausted ladder ends failed (no resubmission at the prior; OOM penalty counts attempts − 1). `decide.rs` passes `Rightsizer`. The oracle search now also returns the oracle's stretch.
- Workspace `Cargo.toml`: `assert_cmd = "2.2.2"` (dev-dependency of `rightsizer-cli`). `rightsizer-engine` depends on `prometheus` and dev-depends on `criterion`.

### Integration points (status after round 3)

1. ~~`rightsizer_features::build_context` not `Send`~~ — **fixed**: futures collected into a `Vec` in `readers::run_reads`; the engine calls `build_context_with` for `ContextInput::Seed` (see "Contexts from seeds"). Tests: `build_context_future_is_send` (features, compile time), `decide_future_is_send`, `seed_inputs_are_read_through_features`, `admission_path_hashes_inputs_without_reading` (engine `tests/integration.rs`).
2. ~~Duplicate caches in `StoreView`~~ — **fixed**: `CachedStore::with_clock` + `BufferedWriter`; the engine's copies are deleted. Tests: `cache_ttls_run_on_an_injected_clock`, `deferred_writes_are_queued_untried` (history), the existing `s1_inert_on_store_unreachable_after_ttl` (manual clock).
3. ~~Key listing~~ — **fixed**: `Engine::keys()`; `rsz shadow --tenant` and `history export --jsonl` work on sqlite and `file://` (CLI test runs both); `json://` removed (`the_interim_json_store_is_gone`). Every backend's `keys()` was already implemented by the history crate.
4. ~~Two memos~~ — **fixed**: one memo, `rightsizer_estimators::memo::Memo` with single-flight (`memo::tests::ttl_on_the_clock_capacity_invalidation_and_single_flight`, plus the engine's memo/single-flight tests).
5. Lease-expiry sweep — **fixed** (`Engine::sweep`, `rsz history compact`; `sweep_trips_on_leases_that_expire_open`, `sweep_skips_recovered_invocations`). Still open, owned elsewhere: the Slurm `PlatformModel` for `rsz recommend -p slurm` (rightsizer-slurm) ~~and `rsz history import k8s|slurm|…` importers (adapter crates)~~ — **resolved** (importers): `rsz history import k8s|slurm|flyte` dispatch in `crates/rightsizer-cli/src/main.rs`; the PlatformModel part is still open.

### Findings for other crates

- ~~**estimators / core (staged bytes)**: staging counted twice~~ — **fixed** in round 3 (estimators subtract `RunRecord.staged_bytes`, falling back to the current `memory_backed_bytes`; see the estimators section). **Request for adapters (K8s watcher, importers):** set `RunRecord.staged_bytes` to the unit's memory-backed staging size at that run (the value extract puts in `manifest.memory_backed_bytes`); records without it assume today's staging.
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
- **Memory basis (settled: `demand`).** The tuner's memory label is the *need*
  `(peak + runtime) × 1.05` (`k8s.flyte_need_mib`), while SPEC §5.2 says
  estimators emit *demand* and the decision adds overhead and headroom. The
  default `--memory-basis demand` (`build_estimate`, `PointerModel`,
  `ServeOptions`, `rightsizer serve`) converts: demand = need / 1.05 − overhead
  (70 MiB on Flyte, 0 on K8s), spread log-uniformly within each cell.
  `--memory-basis need` passes the cells through (models.md §4 "cell to
  cell"), which counts runtime + headroom twice (conservative, up to about a
  cell). `rightsizer models convert --memory-basis demand|need` (default
  `demand`: scale `2^20 / 1.05`; `need`: `2^20`); the 70 MiB Flyte runtime
  cannot be expressed in a `rightsizer-trees/1` target and stays in.
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
- Feature mapping (tuner → features-v1), 187 of 209 mapped:
  `log_total_bytes`→`inputs.log2_bytes_total`, `n_inputs`→`inputs.count`,
  `log_bytes_{fmt}`→`fmt.{fmt}.log2_bytes`, `log_rows_visible`→`inputs.log2_rows`,
  `ncols`→`cols.total`, `npy_log_n|npy_log_row|npy_itemsize`→`npy.*`,
  `{m,c,g}_code_lines`→`code.lines` (tuner counts `\n`; Rust counts lines: equal
  for sources ending in a newline), `n_hist`→`hist.n_success`,
  `log_hist_mem`→`hist.log2_peak_max_mib`, `log_hist_mem_scaled`→`hist.log2_scaled_mib`,
  `log_hist_size_ratio`→`hist.log2_size_ratio`, `hist_mean_cores`→`hist.cpu_mean_max`,
  and every token feature `{m,c,g}_tok_X`, `cuda_X`, `k8s_X` → `tok.X`
  (the K8s tokens were matched on code + manifest by the tuner; on the source
  only here), `{m,c,g}_log_max_literal`→`code.max_literal_log2` (round 3).
  **Unmapped (22, written as `tuner.<name>` → missing → 0)**: `c_max_pool`, `c_n_pool_decls`, `c_min_work_items`,
  `g_assign_c`, `g_log_assign_{a,b,d}`, `g_log_batch`, `g_log_d2_layers`,
  `g_log_max_linear`, `g_log_sum_linear`, `hist_peak_cores`, `hist_used_gpu`,
  `hist_util`, `log_hist_gpu`, `ncol_{cat,f32,f64,i32,i64,str,text}`.
  Adding the pool and GPU-code numerics (`num.<token>` exists now), per-dtype column counts and GPU/peak-CPU history
  features to features-v1 would close most of the gap.
- Tested on a skops file with the published structure (209 real feature
  names, 3 levels): converted values equal sklearn's `predict` (rtol 1e-6),
  the classifier's probabilities equal `predict_proba` (1e-6).

### Findings for other crates

- ~~**features vs estimators (token spelling).**~~ **Fixed in round 3.** Exact
  format, for exporters and converters: `CodeInfo.tokens` entries are
  `tok:<raw token>`, `num:<raw token>=<n>`, `max_literal=<n>`,
  `code_lines_log2=<k>` (`rightsizer_features::tokens`); model feature names
  are `tok.<raw token>` (1/0), `num.<raw token>` (n, else 0),
  `code.max_literal_log2`, `code.lines_log2`. `<raw token>` is the token
  string verbatim, e.g. `tok.read_parquet(`, `tok."cuda"`, `tok.kind: Job`.
- **Adapters (K8s).** For the pointer model's K8s layout, put the rendered
  Job manifest (resources removed) in `manifest.extra.manifest_yaml` as a
  string scalar when privacy allows; without it the server synthesizes a
  skeleton.
- **Privacy.** With the default remote privacy (no source, no column names),
  the pointer prompt has an empty code block and count-only schemas: far from
  the training distribution. Serving these models usefully needs
  `send_source = true` and `send = ["columns"]` for the model's URL.

## rightsizer-plugin: protocol, daemon (`rsz serve`), plugins (M3)

Tests: `cargo test -p rightsizer-plugin` (unit + `tests/conformance.rs`,
needs `python3`); `tests/integration/plugin/run.sh` (host, no containers).

### `rightsizer-plugin/1` as implemented

- **Framing** (`rpc.rs`): one JSON-RPC 2.0 message per line, ≤ 16 MiB
  (longer lines are a `-32700` and the reader resynchronizes at the next
  newline); blank lines are skipped; `\r\n` accepted.
- **Versions** (N/N-1): `rightsizer-plugin/<major>`; both sides accept the
  current and the previous major (`SUPPORTED_MAJORS`, today only 1 because 0
  never shipped). `initialize` params gained `protocols: [..]` (extension:
  alternatives besides the preferred `protocol`); the answer is the highest
  common one, else `-32004` with `data.code = unsupported`. A plugin that
  answers with a version the core does not speak is killed and not used.
  `initialize` is optional for shims (cli_filter's helper sends a bare
  `decide`): a connection without it speaks the current version.
- **Errors**: always `data: {code, retryable}` with the five codes. JSON-RPC
  codes used: `-32700` parse (`parse_error`), `-32600` invalid request,
  `-32601` unknown method (`unsupported`), `-32602` bad params
  (`parse_error`), `-32000` application (code from the error), `-32001`
  `deadline_ms` expired (`internal`, retryable), `-32800` cancelled
  (`internal`, retryable), `-32003` forbidden (caller may not act for the
  tenant; `unsupported`, not retryable), `-32004` no common version.
- **Server loop** (`server.rs`): requests on one connection run concurrently
  (≤ `[serve] max_concurrent_requests`, 64); responses are written as they
  finish. `deadline_ms` (default 30 s, capped at 300 s) bounds each request;
  `$/cancel {id}` aborts it and answers `-32800` at once (a cancel for a
  finished or unknown id is a no-op; the response is never sent twice). A
  handler panic is an `internal` error. Shutdown: stop reading, let in-flight
  requests answer within the grace period, close.
- **Unix socket**: `bind_unix` replaces a stale socket file (nothing
  listening) and refuses a live one; mode `[serve] socket_mode` (default
  0666: callers are authenticated by peer credentials, not file mode). Peer
  credentials via `tokio::net::UnixStream::peer_cred` (`SO_PEERCRED` on
  Linux, `getpeereid` + `LOCAL_PEERPID` on macOS: the pid is known on both;
  checked on macOS and in the Rocky 9 container). No `unsafe`, no libc.
- **HTTP** (`--listen ADDR`, axum in `rsz`, not in the core set): `POST /`
  or `/rpc` with one message or a batch array (served through the same loop
  over in-memory pipes), `GET /healthz`, `GET /metrics` (the engine's
  registry). HTTP callers have no credentials: they get the tenants mapped
  to `"http"` in `tenants_by_uid` (default none).
- **Shim → core methods**: `initialize`, `decide`, `outcome` (trust capped at
  `authenticated`; `{accepted, written, attribution, lease_closed}`),
  `status` (extensions: `kinds`, `requests`, `decide_latency_ms {count, p50,
  p99, max}` over the last 20k decides, `caller {uid, gid, pid, user,
  tenants, transport}`, `refused`), and two extensions for `rsz slurm sbatch
  --watch` run as a user: `lease {action: open|close}` and `sign`.
  `DecideParams` gained `attempt`, `watch`, `dry_run` (all optional).
- **Composite `decide`** (`compose.rs`, used by the daemon, the in-process
  Slurm path and the conformance suite): extract → authorize the derived
  tenant → (skip?) → `reference`/`state` → one engine decision per unit or
  group (`attempt_decide` with an attempt context) → render the changed units
  → validate → `{decisions, patch, mode, summary}`. The patch is `none` below
  enforce; a render error, a rejection or a deadline keeps the prior and says
  so in `summary`. Adapter calls get ≤ 40 % of the budget each. `mode` is the
  lowest over the units (`off` when nothing was decided).

### Core → plugin (`client.rs`)

- `PluginClient` spawns `rsz-plugin-<kind>` with piped stdio (stderr goes to
  `tracing` at debug), checks the sha256 pin before **every** spawn
  (restarts included; a mismatch never spawns), handshakes within 5 s, and
  keeps the process. One request at a time unless `concurrency: n`. Every
  request carries `deadline_ms`; on expiry it sends `$/cancel` and returns
  `Timeout` (callers keep the prior); two consecutive timeouts kill the
  process. EOF fails every pending request (`Crashed`); the next call
  respawns after a backoff of 100 ms doubling to 30 s (reset after 60 s of
  healthy uptime); calls inside the window fail fast.
- `PluginAdapter: Adapter` (+ `compose::Target` for `reference`/`state`,
  which the sync `PlatformModel::reference` cannot do over RPC): transport
  errors become `PluginError::PlatformUnavailable` (extract/render) or a
  `Rejection{reason: "plugin_error"}` (validate), i.e. S1.
- Discovery (`discovery.rs`): interactive = `rsz-plugin-*` executables on
  `PATH` (first wins) + `[plugins.<kind>]` (config wins; relative paths
  resolve on `PATH`); server = config entries with absolute paths only
  (relative → refused), pins checked at discovery too. `[plugins.<kind>]`
  gained `fixtures` (additive).

### `rsz plugin list | check NAME`

`check` runs `conformance::run` against `--fixtures DIR` (else
`[plugins.NAME] fixtures`, `<exe>.fixtures/`, `fixtures/` next to it):
`valid/*.json` (`{native, env?}` or a bare Native) and `invalid/*.json`
(`{native, patch}`). Checks: handshake (version, kind, methods);
extract determinism (two calls, same JSON); render idempotence (decisions
halving memory render to a non-empty patch; the core applies it — text
edits, RFC 6902 JSON Patch, or an options merge — and re-rendering is
`Patch::None`); validate (every invalid fixture rejected, its own rendered
patches accepted); crash (SIGSTOP, decide, SIGKILL mid-request → `Patch::None`,
then a restart after the backoff); timeout (SIGSTOP, decide with a 300 ms
budget → `Patch::None` in ~120 ms, SIGCONT). Exit 1 unless all pass.

Results (`cargo test -p rightsizer-plugin --test conformance`):

| plugin | handshake | extract determinism | render idempotence | validate | crash | timeout |
|---|---|---|---|---|---|---|
| `examples/plugins/rsz-plugin-nomad-demo` | PASS | PASS | PASS | PASS (4/4 rejected) | PASS | PASS (121 ms) |
| `rsz-plugin-crashy` (exits on extract) | PASS | **FAIL** (crashed) | FAIL (nothing extracted) | PASS | PASS | PASS |
| `rsz-plugin-hangs` (never answers extract) | PASS | **FAIL** (timed out after 1.5 s) | FAIL | FAIL (validate timed out behind the hung extract) | PASS | PASS |
| `rsz-plugin-badshake` (answers `rightsizer-plugin/7`) | **FAIL** (no common version) | skipped | skipped | skipped | skipped | skipped |
| `rsz-plugin-flappy` (non-idempotent render) | PASS | PASS | **FAIL** (re-render emits `MemoryMB +1`) | PASS | PASS | PASS |

The demo (`examples/plugins/rsz-plugin-nomad-demo`, Python stdlib) adapts a
Nomad-like JSON jobspec (one unit per task, `CPU` read as millicores,
`MemoryMB` as MiB, tenant from `env.namespace` else `Job.Namespace`,
`Meta["rightsizer"] = "off"` opts out, `Meta["rightsizer.decision-id"]` is
already sized) and renders JSON Patch. The broken plugins in
`tests/integration/plugin/broken/` are the demo with `RSZ_DEMO_BREAK`.

### Plugin end to end (`tests/integration/plugin/run.sh`, macOS host): 15/15

`rsz serve --socket … --listen 127.0.0.1:…` with `[plugins.nomad-demo]` by
absolute path + sha256 pin, uid 502 → `nomad-demo://ml`, a stdlib shim
(`shim.py`): handshake; `rightsizer-plugin/9` rejected (`unsupported`);
`status.caller` = `{uid 502, gid 20, pid, user}`; recommend → decisions,
`patch: none`; after seeding 5 verified 1 GiB peaks, enforce → the plugin's
JSON Patch (`MemoryMB 8192 → 4096`, `CPU 2000 → 1000`), validated; tenant
`nomad-demo://other` → `-32003`; HTTP without an `"http"` mapping → `-32003`,
with one → a decision; pin mismatch → the plugin is refused at start (never
spawned), `decide` → `platform_unavailable` "plugin nomad-demo refused:
sha256 mismatch", `status.refused` lists it; uid mapped to nothing →
`-32003`; SIGTERM → exit 0, socket removed, buffered writes flushed.

#### QA rerun after review (HEAD `f9acdbd`, 2026-11-19): 17/17

The 15 checks above pass unchanged. Two new checks: a copy of the demo
plugin made group-writable (0775), with a matching sha256 pin, is refused
in server mode (`decide` → `platform_unavailable`, log: "unsafe to run
from a server: … is group- or world-writable"). The same copy at 0755 is
accepted (control).

### Lifecycle

`READY=1`/`STATUS=`/`MAINPID=` after binding, `WATCHDOG=1` every
`WATCHDOG_USEC/2` (only when `WATCHDOG_PID` is unset or ours),
`STOPPING=1` on SIGTERM/SIGINT, all to `$NOTIFY_SOCKET` with a plain
`UnixDatagram` (abstract `@` names on Linux). Not exercised under a real
systemd (the test containers have none); the datagram path is the protocol's.

### Changes to shared code (all additive)

- `rightsizer-engine::config`: `[history] durable`, `shared` (overrides of
  store detection, passed to `StoreConfig` by `store::open_store`: a local
  `file://` store on a single login node is durable and shared, and enforce
  is impossible without them; the e2e uses them); `[slurm]` keys above;
  `[serve] socket_mode, slurm_associations, max_concurrent_requests,
  shutdown_grace`; `[plugins.*] fixtures`. (My `store.rs` one-liner was
  overwritten once by a concurrent edit and re-applied.)
- `rightsizer-plugin`: depends on `rightsizer-history` (the conformance
  suite's in-memory store), `sha2`, `hex`; tokio `net`, `macros`, `rt`.
  `xtask deps-check` passes.
- `rsz` (`main.rs`, localized): `mod daemon; mod plugin_cmd; mod
  slurm_cmd;`, match arms for `serve`, `plugin`, `slurm`, `history import
  slurm`; `decide-client` dispatched before clap; `history import` and
  `plugin` args accept hyphenated values, and `hoist_global_flags` moves
  `-o/--output`, `--config`, `--store` given after `history import <src>` in
  front of the subcommand (otherwise the importer would swallow them; the
  K8s importer benefits too). `axum` added to `rsz`'s dependencies.

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

### Daemon round (M3): `rsz serve`, `rsz slurm …`, end to end

Tests: `cargo test -p rightsizer-slurm -p rightsizer-plugin -p rightsizer-cli`;
`RSZ_IT_SLURM=1 tests/integration/slurm/run.sh` (results in
`tests/infra/.state/slurm-e2e-results.json`).

- **Daemon side** (`service.rs`, `SlurmService`): the caller's uid comes from
  the socket's peer credentials, its name from `/etc/passwd` (else `id -nu`).
  The account is `--account`, else the user's default account (`sacctmgr show
  user`), else `[slurm] default_account`. The caller may act for
  `slurm://{cluster}/{account}` when `[serve] tenants_by_uid` maps its uid or
  name to it (exact or `prefix*`), or when it has an association with the
  account (`[serve] slurm_associations`, default on). Anything else is a
  `-32003 forbidden` error (cli_filter then submits unchanged and sets the
  negative cache). `salloc`/`srun` are capped at `recommend`. An enforced
  patch that cannot be signed is dropped ("a layer that cannot write its
  marker does not change the submission"). The inventory (`scontrol show
  node|partition|config`, `sacctmgr show assoc|qos|user`) is refreshed at
  start and every `inventory_refresh` (5 min); a failed command keeps its
  previous part. The key file is re-read when its mtime changes (checked at
  most once a second), so rotation needs no restart.
- **Prices matter.** Without `[slurm] prices` every cost is $0 and the
  decision rule picks among ties (in the first e2e run it raised CPU 1 → 2
  and kept memory). The e2e sets `prices = { default = 1.0 }` (per node-hour).
- **CPU is rendered in whole CPUs.** Decisions can say 500m; the adapter
  renders `--cpus-per-task=1`. `sign` and the import's tamper check compare
  against the decision rounded up to whole CPUs. ~~(Follow-up: give the Slurm
  `PlatformRef`/state a whole-CPU request model so decisions say what is
  rendered.)~~ — **resolved**: `PlatformState.cpu_granularity` (Slurm sets 1000m in `rightsizer-slurm/src/platform.rs`; core `decide.rs` `cpu_step`).
- **`rsz slurm sbatch`** decides in-process (`EngineDecider`: engine + the
  daemon's key ring) when `[slurm] token_key_file` is readable (the daemon
  user; `--local` forces it), else over the socket (`SocketDecider`). Over the
  socket the ladder uses two extension methods: `lease` (open/close the
  attempt record) and `sign` (the daemon signs a resubmission only if it is a
  ladder step of an enforced decision the caller may act for: memory in
  `[applied, max(prior, applied × factor^steps)]`, CPU = the decision's
  rendered CPU). The wrapper's invocation id is `rsz-sbatch:{first decision
  id}` and its lease now stays open across the ladder (closed once, with the
  final status; the OOM was not the caller's final answer).
- **sacct ladders** (`sacct::link_ladders`): jobs whose AdminComment verifies
  the same decision are attempts 1, 2, … (job id order) of
  `rsz-sbatch:{decision}` — but only when the wrapper's attempt record exists
  under that invocation (evidence-based; plain cli_filter jobs keep
  `{cluster}:{jobid}`). The CLI importer writes every new record first, then
  runs `Engine::outcome` on each, so an OOM that a later attempt recovered is
  not a trip.
- **`scontrol update` after verification.** Users may lower a pending job's
  memory (`scontrol update MinMemoryNode=…`, allowed for owners). job_submit's
  `slurm_job_modify` **cannot** rewrite `admin_comment`: slurmctld denies the
  user's whole update ("Access/permission denied") when the plugin sets it —
  checked on the cluster. So `slurm_job_modify` only logs it, and `rsz history
  import slurm` drops the decision attribution of a verified job whose
  requested memory/CPU no longer match its decision (reported as
  `detached_after_modification`).
- **`$USER` is unset** under `docker exec -u`, cron and systemd: cli_filter's
  negative-cache file now falls back to `$LOGNAME`, then the home directory's
  name, before `unknown`.
- **`rsz slurm install`** writes both scripts with a configuration header
  (`RIGHTSIZER_CLUSTER`, `_RSZ`, `_SOCKET`, `_KEY_FILE`, `_DISABLED`) and the
  vendored modules (embedded in the binary), atomically; values not given are
  kept from the previous header. `--disable` sets `RIGHTSIZER_DISABLED = true`
  (cli_filter returns at once; job_submit loads no key, writes no
  admin_comment) and removes `rightsizer-table.json`.
- **`rsz slurm rotate-key stage|promote|retire|status --key F [--copy-to F]…`**
  replaces each file atomically and keeps its owner/group (the daemon's copy
  belongs to the service user, job_submit's to SlurmUser); touch
  `job_submit.lua` afterwards so slurmctld reloads its copy.
- **`rsz slurm decide-client`** is dispatched before clap, config loading and
  the tokio runtime (it is on every `sbatch`'s path).
- **Config growth** (`rightsizer-engine::config`, additive): `[slurm]`
  `squeue, scontrol, sacctmgr, job_acct_gather_frequency, default_account,
  version, ladder_memory_factor, ladder_max_steps, prices,
  presized_submitters, inventory_refresh, token_ttl_s`.

#### End to end on the Docker cluster (2026-11-19, Slurm 26.05.5)

31/31 checks on a fresh cluster (`slurm.sh down && up`), daemon as user
`rightsizer` (uid 991) on the slurmctld container with a socket, scripts
installed by `rsz slurm install`, users alice/bob in account `mlteam`:

| step | evidence |
|---|---|
| a recommend | `sbatch` through cli_filter: job unchanged (`MinMemoryNode=1G`), stderr `rightsizer: slurm [recommend] job: keeps the request (…; slurm://rsz-it/mlteam/rsz-a; decision …)`, AdminComment `rightsizer::user` (no token below enforce) |
| b enforce | 5 seeded verified peaks of 100 MiB (`rsz history import jsonl`): `--mem=1G` → `MinMemoryNode=512M` (the trust-region floor), AdminComment `rightsizer:<id>:verified`, COMPLETED; sbatch 68 ms |
| b tampering | forged `RIGHTSIZER_TOKEN` env → cli_filter steps aside, `rightsizer:000…:user`, 1G; a real token replayed with `--mem=900M` → `:user`, with the signed `--mem=512M` → `:verified` (control); `scontrol update MinMemoryNode=200` on a verified job is allowed, logged by `slurm_job_modify`, and the import drops its attribution (`detached_after_modification: rsz-it:6`) |
| b daemon down | SIGSTOP: first sbatch 1128 ms (the 1 s deadline), second 62 ms (negative cache `/tmp/rightsizer-cli-filter-alice.neg`), both unchanged, `:user`; SIGTERM (socket removed): 68 / 58 ms, unchanged |
| c rotation | stage → daemon still signs with the old kid, verified; promote → new kid; tokens from the old and the new key both verify (overlap); retire → the old-key token is `:user`, the new one verified. `rotate-key … --copy-to /etc/slurm/rightsizer.key` keeps each file's owner, `touch job_submit.lua` reloads it |
| d ladder | `ALLOC_MB=742 rsz slurm sbatch --watch` as alice (socket decider): decided 512 MiB (watch) → job 18 `OUT_OF_MEMORY` at 512M → "resubmitting at ladder step 1/2 (1G)" → job 19 COMPLETED at 1G, exit 0, 18 s; both `rightsizer:<same id>:verified` (the resubmission signed by the daemon's `sign`); attempt record `rsz-sbatch:<id>` reduced, lease opened/closed, outcome `succeeded` |
| e import | `rsz history import slurm --since 3h`: 5 jobs → 5 records, statuses `oom_killed=1 succeeded=4`, 2 ladder attempts linked: (attempt 1, `oom_killed`, `at_least` 512 MiB, requested 512 MiB), (attempt 2, `succeeded`, `exact` 747 MiB, requested 1 GiB); attribution `caused=1` but no breaker tripped (recovered by attempt 2) |
| e shadow | `rsz shadow --tenant slurm://rsz-it/mlteam` lists rsz-a, rsz-b, rsz-c, rsz-d with readiness |
| f throughput | 1000 `sbatch --hold` through cli_filter → daemon, memoized decision, enforce (tokens signed): socket round trip idle p50 0.33 ms / p99 0.70 ms (n=1000), under a concurrent sbatch loop p50 0.43 / p99 7.1 ms; daemon-side `decide` for the 1000 sbatch calls p50 3.4 / p99 5.4 / max 6.8 ms; `decide-client` process incl. spawn p50 8.7 / p99 14.0 ms; **sbatch 14.1 ms with rightsizer vs 4.7 ms disabled: +9.4 ms per sbatch** |
| g disable | `rsz slurm install --disable`: job unchanged (1G), no AdminComment; re-install → `512M`, verified again |

The added latency per `sbatch` is the helper process (fork/exec of a 44 MB
binary + popen shell + temp file), not the daemon: a resident helper or a
Lua socket library would remove most of it. The daemon-side 3.4 ms (vs
0.2–0.3 ms for the same memoized request from a socket loop, idle gaps
included) appears only while the per-call helper spawns load the 2-vCPU
Docker VM; it is environmental, not a memo miss (checked: memo source on
every replay).

#### QA rerun after review (HEAD `f9acdbd`, 2026-11-19): 33/35

Linux `rsz` rebuilt from HEAD. The table counts c, g, h from run 2 and
setup, a, b, d, e, f from run 3 (`RSZ_IT_STEPS=a,b,d,e,f`). Logs and JSON
are in `tests/infra/.state/qa/slurm-run{2,3}*`.

- **Harness changes.**
  - Jobs now declare **2G** (`PRIOR` in `e2e.py`), not 1G. With whole CPUs
    (`cpu_granularity` 1000m) and packed pricing on these 2-CPU / 2 GiB
    nodes, a 1-CPU job at ≤ 1 GiB already sits at its per-core share. A
    smaller memory request saves nothing there, so the decision keeps the
    prior (`patch: none`, no token). That made b–g fail at 1G.
  - `setup()` wipes the suite's history store. Records from an aborted run
    had raised `rsz-d`'s demand to 1.31 GiB, so the ladder started at 2G
    and its 4G step could not be scheduled.
- **New checks (step `h`, all pass):**
  - whole CPUs: `-c 2 --mem=1G` with ~0.35-CPU history → decision
    `cpu 1000m`, patch `cpus-per-task: "1"`, `mem: 768M`; the job is
    `NumCPUs=1`, `rightsizer:<id>:verified` (the token verifies).
  - sign binding: the daemon's `sign` with an enforced `rsz-hsign` decision
    and a `rsz-hother` job → `-32000` "workload "rsz-hother" is not the
    workload of decision …". The same key (control) gets a token; uid 0
    (unmapped) → `-32003`.
  - cli_filter without a token: a fake daemon on the socket answers
    enforce with `mem=300M` and no token. The job stays at 2G,
    `rightsizer::user`.
  - negative-cache symlink: bob plants
    `/tmp/rightsizer-cli-filter-alice.neg` → `/home/alice/precious`, and
    alice's sbatch fails over (daemon down). The target's size, mtime,
    inode and sha256 are unchanged, the marker is still bob's symlink, and
    no `lua_*` temp files are left. The container has
    `fs.protected_symlinks=1`, so the kernel would also have refused to
    follow the link. This check does not separate the rename-over fix from
    that protection; the Lua unit tests do.
- **Existing steps:**
  - b: 2G → 1G, verified, COMPLETED; tampering (forged env token, replay
    at 900M, `scontrol update`) and the daemon frozen / stopped paths as
    before (1137 ms then 78 ms with the negative cache).
  - c: rotation stage / promote overlap / retire.
  - d: 1G `OUT_OF_MEMORY` → 2G COMPLETED, one verified decision, lease
    closed `succeeded`.
  - e: ladder records linked (`at_least` 1024 MiB, then `exact` 1491 MiB);
    the `scontrol update` job is detached.
  - g: `--disable` and re-enable.
- **FAIL `a.summary_on_stderr`** (runs 2 and 3): the first sbatch after
  setup took 1283 ms (6.2 s in run 2, while an orphaned sbatch loop from an
  aborted run was still submitting). decide-client hit its 1 s deadline, so
  there was no summary and the submission was unchanged (S1). slurmctld was
  reconfiguring at the time (`slurm.sh reconfigure` in `install`). It did
  not reproduce in isolation: a restarted daemon's first decide took 30 ms.
  A timed-out first decide also writes the 60 s negative cache for that
  user.
- **FAIL `f.p99_round_trip_under_20ms`** (target: socket round trip p99
  < 20 ms, idle and under sbatch load):
  - idle: p50 0.65 ms, **p99 6.3 ms**;
  - under load: p50 0.95 ms, **p99 33.5 ms**;
  - daemon-side decide for 1000 sbatch: p50 6.8 / p99 24.1 ms;
  - decide-client incl. spawn: p50 18.8 / p99 58.5 ms;
  - sbatch 30.0 ms vs 12.9 ms disabled (+17.0 ms).

  The host ran on battery, sleeping between runs, with the Flyte devbox
  in the same Docker VM. The rightsizer-free baseline was 2.7× slower than
  this morning's (12.9 vs 4.7 ms per sbatch). The morning run on the same
  cluster measured 0.70 / 7.1 ms p99 and +9.4 ms per sbatch. Re-measure on
  AC with an otherwise idle Docker VM before reading this as a regression.

## Integration round 3 (core set: core, features, estimators, history, engine, rsz M0)

All API changes are additive except where marked. Per item:

| # | issue | change | tests |
|---|---|---|---|
| 1 | `build_context` future not `Send`; engine did not call it | `readers::run_reads` collects futures into a `Vec`; `ReadOptions.hash_only`; engine `seed_context` calls `build_context_with` (budgets: min(`features.budget_ms`, ½ of the decision budget left); `inputs_hash_only` → no reads) | `build_context_future_is_send`, `hash_only_does_no_io`, `decide_future_is_send`, `seed_inputs_are_read_through_features`, `admission_path_hashes_inputs_without_reading` |
| 2 | engine kept its own caches/buffer (no clock in history) | `rightsizer_core::clock::{Clock, SystemClock, ManualClock, elapsed}` (engine re-exports at the old paths); `CachedStore::with_clock`, `TokioClock`, `invalidate_control`; `BufferedWriter::{defer_run, defer_decision, defer_decision_now, buffered}`; `StoreView` rebuilt on them (`with_clock`, `cached()`, `writer()`); engine copies deleted. **Behaviour:** a full buffer now drops the new write (history's rule) instead of the oldest; an unavailable history yields no stale records | `cache_ttls_run_on_an_injected_clock`, `deferred_writes_are_queued_untried`, safety suite |
| 3 | key listing / `json://` | `Engine::keys()`; CLI `shadow --tenant`, `history export --jsonl` use it; **removed** `SnapshotStore` and `json://` (not additive; only the CLI used them); `rsz` enables `object-store` + `sqlite` | CLI `history_import_show_export_and_shadow` (sqlite and `file://`), `the_interim_json_store_is_gone`, `keys_come_from_the_store` |
| 4 | two memos | estimators `Memo` gains clock, answers/declines, `get_or_compute` (single-flight), `invalidate_workload`; engine `memo` re-exports (`ChainResult` = `MemoAnswer`). **Not additive:** engine `MemoKey` is now the estimators' (fields `workload_key, unit, code_digest: Option, feature_bucket, policy_hash`), engine `Memo::{get, put, invalidate_key}` signatures replaced (no users outside the engine) | `ttl_on_the_clock_capacity_invalidation_and_single_flight`, engine memo/single-flight tests |
| 5 | token features always 0 | token wire format defined once in `rightsizer_features::tokens` (`parse_token`); `features_v1` reads through it; new `num.*`, `code.max_literal_log2`, `code.lines_log2`; `trees.py` maps `log_max_literal` | `every_emitted_token_parses`, `builds_named_features`, python `tests/models/test_trees.py` |
| 6 | staged bytes counted twice | `RunRecord.staged_bytes` (wire: optional; Rust + `rightsizer_runtime.wire`); estimators subtract it (fallback: current `memory_backed_bytes`) | `staged_bytes_are_not_counted_twice` |
| 7 | `canonicalize` not a total order | `rightsizer_core::canonical_input_order`, used by core and features | `canonicalize_is_a_total_order` |
| 8 | `deny_unknown_fields` blocked keys | `MemberConfig` keys were already present; `[k8s]` (`adapter` pass-through table, `rightsizer_k8s::runtime::settings_map` flattens it into `AdapterEnv.settings`) and `[slurm]` (explicit fields) were extended by their owners in this round. Explicit: anything naming a binary, key, certificate, or who is (not) sized; pass-through: adapter tunables | `keys_other_crates_read_are_accepted` |
| 9 | breaker paths without platform | `BreakerScope::path()` → `workload/{platform}/{tenant}/{hash}`; `legacy_path()`, `read_paths()`; object store, sqlite, postgres read both | `breaker_paths_carry_the_platform_and_read_the_legacy_layout`, `sqlite_reads_breaker_rows_at_the_legacy_scope_path`, core path test |
| 10 | leases expiring open | `Engine::sweep(dry_run) -> SweepReport`; `rsz history compact` runs it | `sweep_trips_on_leases_that_expire_open`, `sweep_skips_recovered_invocations`, CLI compact JSON |
| 11 | other notes TODOs | env overrides parse JSON objects/arrays (runtime note 9); `rsz recommend -p` builds keys with `Unit::workload_key` (K8s note); see "deferred" below | `env_overrides` |

**Deferred (with reason):**
- Rust `gbt` reading the `rightsizer-trees-gpu/1` companion (models section TODO): a new estimation feature (logistic classifier + `gpu_memory` axis) that needs its own Python parity fixture, not an integration fix.
- `RIGHTSIZER_DECISION` payload / `AttemptContext` env contracts in Rust (runtime notes 5–7): owned by the Flyte/Python integration.
- Slurm `PlatformModel` in `rsz recommend -p slurm`, ~~`rsz history import k8s|slurm` importers~~: adapter crates. — **resolved** (importers only): `rsz history import k8s|slurm` exist (`crates/rightsizer-cli/src/main.rs`).
- Exporting code tokens in snapshots (history note): no stored artifact carries tokens; needs the decision log to keep them.
- Requests for other agents: K8s watcher / importers set `RunRecord.staged_bytes`; wrappers implementing `HistoryStore` must forward `keys()` (a default method: the engine's test `Flaky` did not).

**Performance (criterion, M-series laptop):** core `decide` ≈ 0.32 ms (0.43 ms computing the context digest); engine memo hit ≈ 0.37 ms dry-run / 0.39 ms recorded (was 0.355 / 0.36: seeds now go through `build_context`).

## rightsizer-py / `rightsizer` (Python API)

Build: `cd python/rightsizer && VIRTUAL_ENV=$PWD/../../.venv ../../.venv/bin/maturin develop --uv`
(`PYO3_PYTHON=.venv/bin/python` for `cargo test -p rightsizer-py`). Tests:
`.venv/bin/python -m pytest python/rightsizer/tests` (core, wire, plugin,
models, serve, export); isolation: `RIGHTSIZER_ISOLATION_WHEEL=<wheel>`
(`tests/isolation`); perf: `python tests/perf/bench_recommend.py`. Wire
fixtures: `RIGHTSIZER_WRITE_FIXTURES=1 cargo test -p rightsizer-py --test wire_fixtures`
(re)writes `tests/fixtures/wire/`; without it the test checks they still round-trip.

### `_core` (JSON in / JSON out)

- `Engine(config=None, *, store=None, role="client", env_overrides=True)`:
  `None` = `Config::load()`; a path = as `RIGHTSIZER_CONFIG`; a JSON object or
  TOML text, then `RIGHTSIZER__*` (unless `env_overrides=False`). Built
  eagerly, so `strict` errors raise `RightsizerError`; otherwise inert (S1).
- `decide / adecide / decide_group / adecide_group / attempt_decide` never
  raise: an unreadable request becomes `Prior{internal}` (key from the
  request when readable, else `python://unknown/invalid-request`), a missing
  engine `Prior{config_error}`, a panic `Prior{panic}`. `outcome / aoutcome /
  flush / breaker_status / control_get / replay / metrics / config_error /
  chain` raise `RightsizerError`. Helpers: `normalize_quantity` /
  `format_quantity` (k8s, flyte, slurm, ray), `rs_grid_v1`, `grid`,
  `context_digest`, `feature_bucket`, `code_info`, `read_inputs` (SSRF-guarded),
  `parse_gpu`, `accelerators`, `canonical_key`, `key_hash`,
  `register_estimator` & co, `runtime_pid`.
- **Runtime.** One multi-thread tokio runtime (≤ 4 workers) per process,
  leaked and re-created when the pid changes (after `fork()`); engines keep
  `(pid, estimator-registry generation)` and rebuild from their config on a
  mismatch (the parent's engine is leaked, never dropped). Blocking calls
  `py.detach` and `block_on` on the calling thread; from inside the runtime
  (a Python estimator calling back) they spawn and park instead. Engine builds
  are serialized by an async gate, so a fan-out of awaitables never parks
  runtime threads on it (a std mutex there deadlocked a 2000-task fan-out
  when the blocking pool filled up). Awaitables use
  `pyo3_async_runtimes::generic` over this runtime (`PyRt`), not the crate's
  own (not fork-safe) global runtime.
- **Python estimators** go in front of the configured chain, in registration
  order (an id replaces its earlier registration). Sync `estimate` runs on
  the blocking pool with the GIL; `async def estimate` runs on the caller's
  loop (task locals from `adecide`, contextvars included); from the sync API
  it runs under `asyncio.run` on a blocking-pool thread (simplification: there
  is no caller loop to borrow). `None` / `rs.Insufficient` (or any exception
  named `Insufficient`) decline; other exceptions are estimator errors.
  Per-request `policy.chain` overrides build from config names only (no
  Python members).
- **Features.** Wheel = engine `object-store` + `sqlite` + `s3`/`gcs`/`azure`
  (`clouds`) + `http` + `remote`; no postgres, no kube (`cargo xtask
  deps-check` already checks `rightsizer-py`).

### Python API

- `rs.Context(key, prior, inputs, code, platform="python", unit, group,
  labels, manifest, history=None, tenant=None)`. A bare key becomes
  `{platform}://{tenant}/{key}` (tenant: `RIGHTSIZER_TENANT`, else the user).
  `history=None` → a seed (the engine reads inputs and the store); a list →
  a full context built in Python (`read_inputs`, `code_info`).
- `rs.Resources` parses K8s quantities in pure Python with Rust's rounding
  (tested against `normalize_quantity`); floats go through their decimal
  text (`0.1` cores = 100m, as Rust sees it).
- `rs.recommend / arecommend / decide_group / outcome / aoutcome` use a
  default engine from `rs.configure(...)` (built at the first decision;
  ~~`storage_options` are exported to the env vars `object_store` reads, so
  Python writes and Rust reads share credentials~~ — **resolved**: passed to
  Rust explicitly, see [Round 4](#round-4-store-options-node-agent-auth-flyte-backfill-dedupe-2026-11-19)). Unset `mode` / `risk` /
  `estimator` come from the innermost `rs.policy(...)`; an attempt context
  (`rs.attempt_context` / `RIGHTSIZER_ATTEMPT`) routes to `attempt_decide`.
  `rs.outcome` attaches the context's input metadata and code digest.
- `rs.plugin`: `Adapter`, `Unit`, `Extracted`, `Patch` (options only),
  `AdapterEnv`, `Rejection`, `register_adapter`, `size(adapter, native, env)`
  (extract → decide / decide_group → render only enforced, changed units →
  validate; never raises).
- `rightsizer.flyte` / `rightsizer.ray`: placeholders that import their
  platform and raise `ImportError` naming the extra.
- Console script `rightsizer = rightsizer.cli:main` (`serve`, `models`,
  `flyte` / `ray` → `rightsizer.<platform>.cli.main`, `version`).

### Measured (M-series laptop, release wheel)

- Wheel 8.9 MB (macOS arm64, abi3); `_core.abi3.so` 22.6 MB, 17.6 MB
  stripped (`strip = true` is now set for release wheels).
- Memo hit, 10k calls (Parquet input, 32 history records in `memory://`,
  walltime, 10 node shapes): `_core.Engine.decide` p50 373 µs / p99 582 µs
  recorded (361 / 602 dry-run), i.e. the engine bench itself (0.39 / 0.37 ms);
  `rs.recommend` p50 435 µs / p99 705 µs recorded (400 / 632 dry-run):
  39–62 µs of typed-API overhead (≈ 28 µs of it request building, JSON and the
  dataclass).
- Single-flight: 10,000 identical decisions from 2,000 threads → 1 call of a
  200 ms Python estimator (1.03 s wall); 10,000 `arecommend` tasks → 1 call
  of an async estimator (0.98 s). macOS caps threads per process near 4k, so
  "10k threads" is 10k calls over 2,000 threads.

### Notes for other crates / next round

- `history.durable` / `history.shared` overrides work for object stores
  (local paths included) but not for sqlite (`SqliteStore::info()` hard-codes
  `durable_shared: false`), so `sqlite://` always demotes enforce.
- `RunRecord.staged_bytes` is in `rightsizer_runtime.wire` (integration round).

## rightsizer.ray (M3)

Code: `python/rightsizer/src/rightsizer/ray/` (`_remote`, `_actor`, `data`, `_sizing`, `_settings`, `_observe`,
`_jobs`, `cli`), child side `python/rightsizer-runtime/src/rightsizer_runtime/ray_child.py`, `rsz ray`
delegation `crates/rightsizer-cli/src/ray.rs`. Unit tests: `.venv/bin/python -m pytest python/rightsizer/tests/ray`
(no cluster: Ray's base `_remote` is replaced by a recorder). Integration: `tests/infra/ray.sh up` then
`RSZ_IT_RAY=1 [RSZ_IT_RAY_LINUX=1] .venv/bin/python -m pytest tests/integration/ray`; evidence JSON in
`tests/infra/.state/ray-it/`.

### Mechanism (Ray 2.60.0 as installed)
- `RightsizedRemoteFunction(RemoteFunction)` overrides `_remote` (decide → merge → base `_remote`) **and** `options`
  (Ray merges `.options(**kw)` into the defaults before `_remote`, so `_remote` alone cannot tell pins apart; the
  override wraps Ray's `FuncWrapper` and passes the call's pins through a contextvar). Our `_remote` runs before
  the base's `@wrap_auto_init`, so it calls `auto_init_ray()` itself (autoscaler shapes need a connection).
- Written: `num_cpus`, `memory` (only when an enforced decision changed them; pinned axes never);
  `accelerator_type` only for an allowed GPU type change. Never: `num_gpus`, `resources`, `max_retries`, others.
- In-task measurement: the shipped function is `make_task_function(fn)`, whose `__signature__` is the user's plus a
  keyword-only `_rightsizer` (Ray's own tracing pattern, `_ray_trace_ctx`), so Ray still validates user args at
  submit. The driver passes the decision per call in that keyword (key, decision id, mode, applied spec, store URI,
  trust, input refs, code digest), not through a per-task `runtime_env` (which would get its own worker pool). The
  wrapper references only builtins + closure (pickled by value) and imports the runtime lazily: a worker without
  `rightsizer-runtime` runs the body unmeasured. Generators stay generators; async generators are not wrapped.
- **Reused-worker peaks (deviation from "Exact, minus the start RSS").** On macOS the second task on a worker read
  ~0 MiB: the allocator keeps freed memory resident (also `phys_footprint`, now the macOS probe), so it sits in the
  baseline. Peaks are re-based on the lowest start RSS the worker has seen; `Exact` when the task started within
  16 MiB of it, else `AtMost` (an upper bound: may include recycled memory; S3(b) accepts it, never under-states).
  On Linux glibc (container test) 200/200/50/200 MiB on one reused pid all read `Exact` and right.
- Status, driver side: `ObjectRef._on_ready` (2.60: no fetch, no deserialization) → daemon thread → object size from
  `get_local_object_locations` (> 100 KiB = plasma = success, untouched) → else the owner's inlined entry via
  `core_worker.get_objects(timeout 0)`; only Ray **error** metadata is deserialized, user values never.
  `OUT_OF_MEMORY` → `OomKilled` (observed); `WORKER_DIED` → `Failed`, upgraded only on specific evidence (State
  API `OUT_OF_MEMORY`, "running low on memory", `OOMKilled`, kernel `oom-kill:` lines; Ray's generic WORKER_DIED
  text names "the OOM killer" as a *possible* cause and is not evidence); user exceptions are recorded by the
  worker (`GpuOom` for `torch.cuda.OutOfMemoryError`). Driver records: source `ray_state`, invocation = task id.
- Platform state: autoscaler node types (bootstrap config or `get_cluster_status` node types) when an autoscaler
  with node types exists; `ray start` runs the monitor with the read-only provider whose "types" are `node_<id>`,
  ignored. Else `[ray] shapes`. Else ceiling = definition ceiling or the prior with
  `Constraints.ceiling_reason = "no_platform_state"` → `rightsizer_clamped_total{reason="no_platform_state"}`.

### Rust changes (additive)
- `rightsizer-core`: `Constraints.ceiling_reason: Option<String>` (clamp reason instead of `"user"`), used in the two
  `Advisory::Clamped` sites of `decide.rs` (`clamp_reason`); test `s4_clamp_reason_names_missing_platform_state`;
  schemas regenerated.
- `rightsizer-engine::config::RayConfig`: `shapes: Vec<NodeShape>`, `labelled_accelerators`,
  `tenant_scoped_credentials` (test `ray_section_keys`).
- `rightsizer-cli`: `ray.rs` (+2 lines in `main.rs`): `rsz ray …` execs `PYTHON -m rightsizer.ray.cli` (PYTHON =
  `[ray] python` | `$RIGHTSIZER_PYTHON` | `python3`, probed with `find_spec` without importing Ray), forwarding
  `--config`/`--store`; otherwise the install hint, exit 2.

### Ray internals vs ray.md (refs at another commit)
- `remote_function.py`: `self.remote = _remote_proxy` at 174-183 (as spec); `FuncWrapper` 335-355, returned at 357
  (spec 339-357). `_remote` is wrapped by `@wrap_auto_init @_tracing_task_invocation`.
- `actor.py`: subclassing blocked in `ActorClass.__init__`, lines 1562-1597 (spec 1591-1625).
- `experimental/locations.py`: `get_object_locations(refs, timeout_ms=-1)` waits **forever** by default; a 50 ms
  timeout is passed. `get_local_object_locations` (no RPC) exists and is tried first. `node_ids` was `[]` at the
  owner even for a 5 MB plasma object; `object_size` is reliable once the task finished, `None` before.
- `data/dataset.py`: `map_batches(num_cpus=, num_gpus=, memory=, …)` as spec; `ray_remote_args_fn` deprecation at
  215-224, "will be removed in Ray 2.64" (spec 219-226). File reads are datasource-v2: `ListFiles` →
  `ReadFiles`; paths in `ListFiles.source_paths`; `Dataset.input_files()` was `[]` before execution.
- `task_oom_retries`: only in `_raylet.so` (`RAY_task_oom_retries`); the default cannot be read from the wheel.
  The Linux test sets it to 0 (with `max_retries=0`) so the monitor kill surfaces as `OutOfMemoryError` at once.
- `num_gpus > 0` sets `max_calls = 1` (GPU workers are not reused). Apple silicon advertises `GPU: 1` and
  `accelerator_type:M5-Max` on the local head.

### Measured (M-series laptop, local head, 4 CPUs; `tests/infra/.state/ray-it/fanout.json`)
10,000 tasks, memoized decisions: plain `ray.remote` 0.029 ms per `.remote()`; `rs.ray.remote` 0.63 ms (added
0.60 mean / 0.56 p50); sizing only 0.58 ms. Decision path (decide + merge) p50 0.48 ms, p99 0.80 ms, max 10 ms.
Wall clock for 10k no-op tasks: 1.06 s plain vs 6.3 s rightsized (the driver's submit loop is the bottleneck).

### QA rerun after review (HEAD `f9acdbd`, 2026-11-19): 10/10

Extension rebuilt (`maturin develop --release`); local head
(`tests/infra/ray.sh`); `RSZ_IT_RAY=1 pytest tests/integration/ray`:
9 passed (actor, Ray Data, failures, fan-out, history → enforce,
unverified hold and opt-in, `rsz ray submit`). `RSZ_IT_RAY_LINUX=1`
memory-monitor test: 1 passed (`linux.hog` → `oom_killed`, `observed`,
`ray_state`; SIGKILL → `failed`; reused-worker peaks 200/200/50/200 MiB).
Fan-out, 10,000 memoized tasks: `rs.ray.remote` adds **0.60 ms** per
`.remote()` (mean; p50 0.53 ms; target < 5 ms). Decision path p50 0.44 ms,
p99 1.18 ms, max 14 ms. Wall clock 1.46 s plain vs 6.40 s rightsized.

### Open / for other agents
- Worker-written records bypass the driver engine's per-key history cache and memo: new evidence is seen after
  `history.cache_ttl` / `decision.memo_ttl` (10 min). `_core` has no invalidation call; the integration tests use 1 s.
- ~~Core: when the size-scaled floor exceeds a no-platform-state ceiling the core emits `infeasible_floor`, not
  `clamped` (not counted under `no_platform_state`), and its `recommended` memory fell *below* that floor
  (256Mi with floor 8064Mi, ceiling 1Gi, an ad-hoc check); worth a look in `decide.rs`.~~ — **resolved**: clamped with reason `no_platform_state` and `recommended` ≥ the floor (test `no_platform_state_floor_above_prior_is_clamped_not_below_floor`, `crates/rightsizer-core/src/decide/tests.rs`).
- KubeRay tenant: `RAY_CLUSTER_NAMESPACE` / `POD_NAMESPACE` / the service-account namespace file plus
  `RAY_CLUSTER_NAME`; the env names are assumptions to align with the K8s webhook.

## rightsizer.flyte (M1)

Code: `python/rightsizer/src/rightsizer/flyte/` (`_policy`, `_resources`,
`_task`, `_env`, `_run`, `_deploy`, `_backfill`, `_classify`, `_engine`,
`_store`, `cli`), child side `python/rightsizer-runtime/src/rightsizer_runtime/flyte_child.py`,
CLI delegation `crates/rightsizer-cli/src/flyte.rs`. Unit tests:
`.venv/bin/python -m pytest python/rightsizer/tests/flyte` (fake remote task
context + fake controller through the SDK's private `_ControllerState`, real
engine on a local store). Bench: `python/rightsizer/tests/flyte/bench_decision_path.py`.
Integration: `RSZ_IT_FLYTE=1 tests/integration/flyte/run.sh` (devbox; results in
`tests/infra/.state/flyte-it-results.json`). SDK facts as checked against
2.13.6: flyte.md §1.1.

### Decisions

- **`RightsizedTask`** is `RightsizedMixin` + the task's template class, made a
  `kw_only` dataclass once per class (`rightsized_class`). Fields: `_rs_policy`,
  `_rs_pins`, `_rs_bypass`, plus `_rs_invocation` (stack entries from
  `sized()`), `_rs_code` (`rs.Code`, captured at decoration) and
  `_rs_measured`. Pickling goes through `__reduce_ex__` → `_rebuild_rightsized(base)`,
  so a dynamic class never has to be importable. Construction from an existing
  task is a shallow copy with the class changed (`cls.__new__` + `__dict__`), not
  `cls(**init fields)`: `flyte.extras.ContainerTask.__init__` does not take its
  fields, and the generated dataclass `__init__` dropped `_cmd`/`_args`/… (review fix).
- **`aio` is a plain `def`** returning a coroutine, so the stack is captured
  when the call is made. The captured *inherited* stack is re-entered with
  `flyte.custom_context` around the submission (the SDK reads the context when
  the coroutine runs, flyte.md §1.1). `sized()` entries apply to that call only
  and are not propagated to its descendants; `policy()` blocks are.
- **Mirroring**: `policy()` pushes on the runtime contextvar stack and writes
  the whole stack into `custom_context["rightsizer"]`; a contextvar counts the
  local entries already mirrored, so the captured stack is
  `inherited + local[mirrored:]` with no duplicates.
- **Escalation** uses the engine's `attempt_decide` (attempt records, leases,
  `applied₁ × 2^k`, `final_floor`): one `AttemptContext` per call, mirrored to
  the child as `RIGHTSIZER_ATTEMPT`. Only OOM after a *reduced* attempt
  escalates (an OOM at or above the prior would repeat); GPU OOM only with
  `allow_gpu_type_change`. `on_oom="auto"` = escalate iff `retries == 0`.
- **Outcomes**: the parent writes a `self_report` record (status, duration,
  requested = the submitted spec) through `engine.aoutcome`; the child writes a
  `cgroup` record with peaks through `flyte.storage.put_stream` (the pod's own
  credentials). Both carry the same `(invocation_id, attempt)`; their sources
  differ, so both are kept. `Cached` is inferred only for cache-enabled tasks
  (one listing of the key's newest shards).
- **S4 without shapes**: `[flyte] shapes` empty → the ceiling is the
  definition's, else the prior (increases held). With shapes they go to
  `PlatformState`.
- **Limiter**: 64 decisions at once per event loop (and per process for the
  sync path). The budget starts when a call is admitted, so a 10k-way gather
  queues instead of expiring.
- **Images**: `auto_image=True` appends `rightsizer==<version>` (PyPI);
  `auto_image=<dir>` / `RIGHTSIZER_FLYTE_WHEELS` uses the SDK's private
  `PythonWheels` layer (`flyte._image`, the same layer `with_local_v2` uses).
  The task image's child lookup is by environment name, so pods may build a
  different `Image` object without harm.
- **Deploy-time sizing** writes deploy records to
  `{prefix}/v1/flyte/deploys/{tenant}/{task}/{version}.json` (origin prior,
  deployed spec, decision id) next to the engine's decision log. The deploy
  version is the caller's or Flyte's derived one (code bundle + envs with the
  sized tasks + images), read back from `flyte.deploy`'s result for the records
  (review fix: an `rsz-<digest of the sizing>` version gave a code change the
  previous deploy's version). The reconcile lease is
  `{prefix}/v1/flyte/reconcile/lease.json` (obstore conditional create /
  ETag update; local files: `O_EXCL`).
- **Backfill** writes `flyte_remote` / `verified` records through
  `engine.outcome` (joins, attribution, lease closing), `invocation_id =
  {run}/{action}`.
- **Missing `_core`**: the engine build error is remembered per process; every
  call then runs at the prior with a fresh decision id (`Decision.prior_for`),
  the marker still goes to the child, and the child still measures (runtime only).

### Changes to shared crates (all additive in API)

- `rightsizer-engine::config::FlyteConfig`: `scalar_memory_enforced` (default
  `true`), `observe_local`, `shapes: Vec<NodeShape>` (`flyte_section_keys`).
- `rightsizer-history::CachedStore::history`: **single flight** per key, run
  as its own task (a caller whose budget expires no longer cancels the read, so
  the cache fills), and **stale-while-revalidate after invalidation**
  (`invalidate_key` marks the entry stale; the next read waits up to
  `REFRESH_WAIT` = 25 ms for the refresh, else answers from the stale entry).
  New `forget_key` drops an entry. Test:
  `history_reads_are_single_flight_and_survive_a_cancelled_caller`.
- `rightsizer-history::ObjectHistoryStore::runs(key, limit)` reads the pointer
  plus only the newest `limit + limit/4` shards (was: every shard above the
  watermark), and the write-time duplicate check looks at the newest
  `DUPLICATE_CHECK_DEPTH` (8) records (was: every shard). Older duplicates are
  still removed on read and in compaction. Conformance (incl. S3 on the
  devbox's rustfs) passes.
- `rightsizer-engine::Engine`: recorded decisions are kept in a bounded
  in-process map (20k), and `outcome` joins to them before falling back to
  `HistoryStore::decision` (which lists the decision log).

Why: on the devbox, a 45-wide gather against an uncompacted key (~200 shards)
made every decision read every shard, every outcome re-read every shard (the
duplicate check) and invalidate the cache, and every expired budget cancel the
read that would have warmed it: 55 of 90 decisions fell back to the prior.

### Measured

- Decision path once memoized (plan + engine memo hit + sized copy; real
  engine, `memory://` store, readable Parquet input; `bench_decision_path.py`,
  M-series laptop): sequential p50 0.42 ms / p99 0.82 ms; a 10,000-way
  `gather` 1.53 s wall = 0.15 ms per call, 0 budget expiries, 12,000 memo hits /
  1 estimator call. With the parent-side outcome on the same path (`--full`):
  p50 1.95 ms (each outcome invalidates the key's memo, so most decisions then
  recompute).
- Devbox (run `rsz-it/run1`, before the history changes above reached the task
  image): smoke and 4 demo rounds of 90 enforced invocations each (ETL over
  Parquet, memory hogs, a tight task), all `SUCCEEDED`, 0 task errors; 54 / 63 /
  45 / 14 enforced changes per round (reductions 1Gi → 512Mi behind the ladder,
  ETL increases 1Gi → 1.5Gi from `rules-sized` cold start); 26 / 27 / 45 / 76
  of 90 decisions per round fell back to the prior on the 250 ms budget (cold
  history reads in each new driver pod, growing with uncompacted shards: the
  motivation for the history changes and the parent pre-warm).

### Not finished

The devbox run stopped after round 3 of scenario (a): the host disk filled up
(Docker Desktop's VM disk; 263 MiB left) and Docker started returning I/O
errors, so the devbox, its object store and the remaining scenarios (b–i,
deploy round trip, backfill against the devbox) could not run, and the devbox
container could not be removed. The latest Rust tuning (history read window
`limit + limit/4`, 64 concurrent GETs) and the parent pre-warm are in the
source and unit-tested but were never in a task image.

## Round 4: store options, node agent auth, Flyte backfill dedupe (2026-11-19)

### Store options instead of environment variables (SPEC §10.3)

- `rightsizer_history::factory::StoreOptions` (a `BTreeMap<String, String>`,
  keys case-insensitive) on `StoreConfig.options`, mapped onto the
  `object_store` builder of the URI's scheme: `s3_builder`, `gcs_builder`,
  `azure_builder` (public, so tests check the builder config without a
  network). Keys are anything `object_store`'s `FromStr` accepts
  (`access_key_id`/`aws_access_key_id`, `secret_access_key`, `session_token`,
  `endpoint`, `region`, `allow_http`, `virtual_hosted_style_request`,
  `skip_signature`, `service_account`, `account_name`, `account_key`,
  `sas_token`, …) plus fsspec spellings (`key`, `secret`, `token`,
  `endpoint_url`, `region_name`, `anon`). Precedence: options > URI query
  parameters > the ambient environment (`*Builder::from_env` is still the
  base; reading the environment is fine, writing it is not). A custom
  endpoint defaults to path-style requests and an `http://` endpoint to
  `allow_http` unless the options say otherwise. An unknown key is a
  configuration error naming the key, never the value. Options are ignored
  for local, sqlite, postgres and memory stores.
- **Redaction:** `Debug` and `Serialize` print `***` for every value except
  an allowlist of non-secret keys (endpoint, region, `allow_http`,
  addressing style, `skip_signature`, account name, …), so `rsz config show`,
  `Engine.config`, `_core.Engine.config_json()` and logs never carry
  credentials. Deserialization accepts strings, booleans and numbers (TOML
  `allow_http = true`). A serialized config therefore does not round-trip
  secrets — nothing in the tree re-reads a serialized `Config`.
- Engine: `[history] storage_options` (`HistoryConfig.storage_options`,
  skipped when empty; env overrides `RIGHTSIZER__HISTORY__STORAGE_OPTIONS__KEY`
  work through the generic nested-override path) → `open_store`. This is the
  only engine change (two lines in `config.rs`, one in `store.rs`).
- Python: `_core.Engine(config, *, store, role, env_overrides, storage_options)`
  (a `dict[str, str]`, merged over the config's table key by key);
  `rightsizer.Engine(..., storage_options=…)` / `rs.configure(storage_options=…)`
  normalize fsspec spellings with `object_store_options` and pass them
  through. `apply_storage_options` / `env_for` are gone (they wrote
  `os.environ`); `rightsizer.object_store_options` replaces the export.
- `rightsizer_runtime.storage` (stdlib only): `object_store_options` (any
  spelling → canonical), `fsspec_options` (canonical or fsspec → s3fs /
  gcsfs / adlfs kwargs: `key`, `secret`, `token`,
  `client_kwargs.endpoint_url`, `client_kwargs.region_name`,
  `config_kwargs.s3.addressing_style`, `anon`) and `flyte_storage_options`
  (`FLYTE_AWS_ENDPOINT`/`_ACCESS_KEY_ID`/`_SECRET_ACCESS_KEY` → options;
  `allow_http` for `http://`, `region = us-east-1` when no `AWS_REGION`;
  ambient `AWS_ENDPOINT[_URL]` / `AWS_ACCESS_KEY_ID` take precedence, as
  before). `RunRecordWriter(storage_options=…)` translates through
  `fsspec_options`; the Flyte child's fsspec fallback writer gets
  `flyte_storage_options()`.
- Flyte: `export_flyte_storage_env` is replaced by the read-only
  `flyte_storage_options()`; `get_engine()` passes it as `storage_options`;
  `flyte._store.Store` (obstore, deploy/reconcile) takes the same options.
- Tests: `factory::tests::{store_options_are_redacted,
  local_store_ignores_object_store_options, s3_options_map_onto_the_builder,
  gcs_options_map_onto_the_builder, azure_options_map_onto_the_builder}`
  (`cargo test -p rightsizer-history --features s3,gcs,azure --lib factory`);
  `python/rightsizer-runtime/tests/test_storage.py`;
  `python/rightsizer/tests/core/test_storage_import.py` (`os.environ` is
  unchanged after building an engine with options, after the Flyte engine
  with `FLYTE_AWS_*` set, and after a writer with options; the local store
  still reads and writes; options are redacted in `Engine.config`).

### K8s: node agent auth, webhook body limit

- Node agent `/v1/*` requires `Authorization: Bearer <token>`; 401 (with
  `WWW-Authenticate: Bearer`) otherwise. Compare: SHA-256 of both sides,
  then an XOR-fold over the digests (no early exit; length does not leak).
  `/healthz` and `/metrics` stay open (no per-pod data; probes). `rsz
  node-agent --token-file F` (env `RSZ_NODE_AGENT_TOKEN_FILE`) is required
  unless `--allow-unauthenticated` (dev only; logs a warning). The watcher
  sends the token (`rsz webhook serve --node-agent-token-file F`;
  `NodeAgentClient::with_token`) and logs a warning on 401.
- Helm: `<release>-node-agent-token` Secret (key `token`, 48 random
  characters, kept across upgrades with `lookup`, or
  `nodeAgent.token.existingSecret`), mounted into the DaemonSet and the
  webhook/watcher Deployment at `/etc/rightsizer/node-agent/token`. A
  NetworkPolicy (`nodeAgent.networkPolicy.enabled`, default true; extra
  peers in `nodeAgent.networkPolicy.extraFrom`) admits only this release's
  `component: webhook` pods to the agent port. Token rotation = replace the
  Secret and restart both (the token is read at start).
- Webhook: `DefaultBodyLimit::max(16 MiB)` (`webhook::MAX_REVIEW_BYTES`) on
  the router; a ~9 MiB AdmissionReview (6 MiB object + 3 MiB oldObject) is
  admitted, > 16 MiB gets 413.
- Tests: `node_agent::tests::{samples_need_the_bearer_token,
  constant_time_compare_and_token_files}`,
  `tests/admit.rs::webhook_accepts_large_admission_reviews`,
  `tests/integration/k8s/helm_test.sh` (new `agent-existing-secret` variant;
  `full` checks the Secret, mounts, flags and the NetworkPolicy).
  Not re-run on k3d (Docker unavailable this round).

### Flyte backfill dedupe

- `_core.Engine.import_runs(records_json)` → `{read, inserted, duplicates,
  joined, attributions, tripped, leases_closed}`: `rightsizer_history::Importer`
  against the key's whole stored history (same as `rsz history import
  jsonl|slurm|k8s`), then `Engine::outcome` (joins, attribution, breakers,
  lease closing) only for inserted records, after all are stored; then a
  flush. `rightsizer.Engine.import_runs(records)` wraps it.
- `rightsizer.flyte._backfill.backfill` collects every record first and
  imports in batches of 500 (`import_records`); the report adds `read`,
  `inserted`, `duplicates`, `attributions`, … (`written` = `inserted`, kept).
- Tests: `test_import_runs_dedupes_against_the_whole_history`,
  `test_flyte_backfill_reimport_is_idempotent` (re-import inserts 0).

## Refinement round (safety and trust gaps left by the adversarial review)

Core set (core, engine, features, estimators, history) plus small wiring
edits in `rightsizer-cli` (`k8s.rs`, `slurm_cmd.rs`, `main.rs`,
`daemon.rs`) and `rightsizer-k8s/src/cluster/prefetch.rs`. New engine tests:
`crates/rightsizer-engine/tests/refinement.rs` (13); the key ones were
checked against the unfixed code (the fix reverted, the test fails).

### 1. `Engine::outcome`: joins and idempotence

- **Join by key.** A `decision_id` joins only a decision whose `key` equals
  the record's (the tenant is part of the key); otherwise the record is
  unjoined (`OutcomeReport::join_refused`, metric
  `rightsizer_errors_total{component="outcome",kind="join_key_mismatch"}`).
  Before, a record could borrow another tenant's prior, mode and estimator
  (attribution, saved hours, estimator breakers), whether the decision came
  from the in-process map or the store.
- **Duplicates.** `StoreView::put_run` reports `Inserted | Duplicate |
  Buffered | Dropped`; on `Duplicate` the outcome returns at once
  (`OutcomeReport::duplicate`): no attribution, breaker trips, fleet /
  estimator window entry, lease close, saved hours or memo / history
  invalidation. A buffered write (store down) is still processed (whether it
  duplicates is unknown until the retry).
- **Importers that write first** (`rsz history import slurm`: the batch is
  inserted so attribution sees every attempt) call
  `Engine::outcome_with(rec, OutcomeOptions { prewritten: true })` for the
  records the importer *inserted*; duplicates never reach the engine. Every
  other surface (plugin daemon, K8s watcher and `apply --watch`, Flyte
  parent and backfill, Ray, Python) calls `outcome`, so a retried or
  replayed report is a no-op. The daemon's `outcome` result now carries the
  real `duplicate` flag.
- Tests: `outcome_does_not_join_a_decision_of_another_key`,
  `outcome_does_not_join_a_stored_decision_of_another_key`,
  `duplicate_outcome_runs_nothing_again` (no re-trip after an operator
  reset, memo kept), `duplicate_success_is_not_counted_twice` (saved hours),
  `importer_path_processes_inserted_records_once`.

### 2. Trust: ceilings at ingestion, a source matrix at read time

- **Matrix (core).** `OutcomeSource::max_trust()`: `self_report`, `cgroup`,
  `ray_state` → `authenticated` (written with the workload's credentials);
  `prometheus`, `sacct`, `flyte_remote`, `watcher`, `importer` → `verified`.
  `RunRecord::effective_trust() = min(trust, source.max_trust())`, and
  `safety::trusted` (floors, trust-region anchors, S3(b), GPU anchors,
  `final_floor`) reads it. `history-peak` and the other estimators do not
  gate on trust (by design: `Unverified` records are learnable), so nothing
  changed there. With today's gates (≥ `authenticated`) the cap matters for
  any `verified`-only consumer; the ingestion cap is what stops a client from
  storing `verified`.
- **Ingestion.** `history.max_ingest_trust` (`None` = by role:
  `authenticated` for client engines, `verified` for operator engines);
  `EngineBuilder::max_ingest_trust(Trust)` overrides it. `outcome` caps
  `rec.trust` at min(ceiling, source ceiling); `Engine::cap_trust` does the
  same for importers that write through `HistoryStore` directly. Wired:
  `rsz k8s serve` (the watcher) and `rsz k8s apply --watch` → `verified`;
  `rsz history import slurm` and `… import k8s` (Prometheus) → `verified`,
  capped before the `Importer` writes; `rsz history import jsonl` caps at
  the client ceiling. The plugin daemon keeps its own `authenticated` cap.
- **For the Python side (not changed here):** the `_core` engine is a client
  engine, so the Flyte `flyte.remote` backfill's `verified` records are now
  stored as `authenticated` unless its engine config sets
  `history.max_ingest_trust = "verified"` (both still pass S3(b)).
- Tests: `safety::tests::trust_matrix_caps_claims_by_source`,
  `client_engines_store_at_most_authenticated`,
  `importer_engines_keep_verified_but_not_beyond_the_source`.

### 3. Cross-process freshness

- `HistoryStore` gains `run_marker(key)`, `runs_marked(key, limit)` (marker
  taken before the read) and `runs_changed_since(key, marker)`, all with
  defaults (no marker → no revalidation):
  - object store: marker = newest shard ULID, else the pointer watermark,
    else `""`; the check is one LIST `start-after` the marker shard (stops at
    the first newer direct child). Writers whose clocks run behind the
    marker can be missed until the TTL, as with the pointer watermark.
  - sqlite: `MAX(rowid_)` (AUTOINCREMENT), check `EXISTS … rowid_ > ?`.
  - postgres: `max(id):count(*)` (a BIGSERIAL can become visible out of
    order; the count catches it; compaction only causes one extra refresh).
  - memory: a per-key write counter.
- `CachedStore`: entries keep the marker and their last check;
  `CacheOptions::revalidate` (engine: `history.revalidate_s`, default 30,
  0 = off). A hit whose last check is older claims the check (one per key per
  interval) and spawns it; the reader waits for the check and the refresh it
  triggers at most its `wait` (`history_within`), else answers from the
  cache and the refresh lands for the next read. A change marks the entry
  stale and calls the `set_on_change` hook (the engine drops the key's memo
  entries; the memo key also contains the history head, so a refreshed
  history never hits an old entry). A hit with no check due costs one clock
  read.
- Budget: the decision path waits min(25 ms, ¼ of what is left of the
  budget) for a refresh or revalidation; a slow store never expires a
  decision (S1).
- `Engine::invalidate(key)`: drops the key's memo entries and marks its
  history stale (for Python: Flyte parents after children, Ray drivers after
  workers; the binding is the other agent's).
- Tests: `conformance::*::run_markers_move_on_writes` (every backend),
  `cache_buffer::revalidation_sees_other_processes_writes` (no store call
  when no check is due; stale data within the wait; background refresh; one
  check per interval; store down keeps the entry),
  `another_processs_runs_are_seen_after_revalidation`,
  `revalidation_can_be_turned_off`.

### 4. Tenants, keys and breaker paths

- `Tenant` segments and key suffix segments may not be empty, `.` or `..`
  (`rejects_dot_and_empty_segments`).
- Breaker trips moved to their own top-level prefix:
  `{prefix}/v1/breakers-trips/{scope}/{ulid}.json`; resets stay at
  `breakers/{scope}.json` (`BreakerScope::{trip_object, reset_object,
  trip_dirs, reset_objects}`, `breaker::{TRIPS_ROOT, RESETS_ROOT}`).
  Readers also read `breakers/{scope}/trips/` (N-1, both the platform and the
  legacy platformless scope path); `breaker_list` reads a `…/trips/{ulid}.json`
  path that does not decode as a trip as a reset.
- Scope encoding is injective: free-form segments (bucket, estimator id and
  version) are escaped with `breaker::escape_segment` (`[A-Za-z0-9.-]` kept,
  a leading `.` and everything else `_HH`); the old lossy sanitizing is the
  N-1 `legacy_path` for estimator scopes (`estimator/gbt/1.0` is unchanged).
- IAM (SPEC §8.1): clients `PutObject` (create-only) on
  `{prefix}/v1/breakers-trips/*`, read on `breakers/*` and `control/*`;
  operators write `breakers/*` and `control/*`. The N-1 layout needed a
  `breakers/*/trips/*` grant that a tenant segment named `trips` matched.
- Tests: `scope_encoding_is_injective_and_trips_are_separate`,
  `object_store::breaker_trips_layout_and_n_minus_1`, updated
  `layout_matches_spec` / `breaker_paths_carry_the_platform_and_read_the_legacy_layout`.

### 5. URI salt: one source, no empty salt

- `engine::chain::resolve_salt`: the variable named by `privacy.salt_env`,
  then `RIGHTSIZER_URI_SALT`, then none. A variable that is set but empty is
  refused (configuration error under `strict`; otherwise a warning,
  `rightsizer_errors_total{component="config",kind="empty_salt"}`, and the
  next source).
- `Engine::read_options()` / `Engine::resolver()` expose the engine's
  readers; the K8s prefetcher now reads with them, so its cached
  `uri_hash`es use the engine's salt (it used `ReadOptions::new`, i.e.
  `RIGHTSIZER_URI_SALT` only, ignoring `privacy.salt_env`) and its local
  path policy. `prefetch::run` lost its `allow_hosts` parameter (the
  engine's `[features] allow_hosts` guard is used).
- Tests: `empty_salt_is_refused`, `contexts_hash_uris_with_the_engines_salt`.

### 6. S3(b) vetoes (SPEC §2.3 changed)

- `safety::s3b_evidence` / `s3b_gpu_evidence`: allowed only if (i) ≥
  `min_history` qualifying successes fit (unchanged), (ii) no qualifying
  success among the newest K = `decision.history_limit` runs needs more than
  the proposal (peak × scale × headroom), (iii) no OOM-class run among them
  proves more (`oom_memory_bound`: the peak, else the limit / request it was
  killed at; GPU: `RunRecord::gpu_oom_bound`, the GPU peak else the
  requested device's usable memory) × scale. Vetoes ignore trust (a planted
  record only makes the rule more conservative). `Evidence::veto` says which
  (`Veto::{Success, Oom}`); the hold detail says "vetoed: …".
  `S3Params::recent_k` and `DecisionParams::history_limit` carry K (32).
- Property tests (`safety::tests::props`): allowed ⇒ nothing among the last
  K needs or proves more; monotone in the proposal; a newer non-fitting run
  never unlocks a reduction. Unit tests for both vetoes and the GPU variant;
  engine: `s3b_veto_holds_a_reduction_a_newer_run_disproves`.
- `replay::would_fail_is_censoring_aware_and_trips_the_simulated_breaker`
  was reordered: after an OOM proving ≥ 8 GiB, the next decision now holds
  the prior (the intended change), so the censored-success case is placed
  before the OOM.

### 7. Features and estimators

- `hf://` metadata cache: keyed by (URI, endpoint, credential identity =
  first 16 hex of SHA-256(`HF_TOKEN`)), maximum age 1 h
  (`HF_METADATA_TTL`); expired entries are dropped. HTTP stores are cached per
  (origin, bearer identity).
- HTTP(S): a URI with a query string (presigned URL) is fetched with it (one
  uncached store per URL whose base URL carries the query); `uri_hash` and
  the metadata-cache key use `privacy::identity_uri` (query and fragment
  dropped, HTTP(S) only; other schemes keep `?`). Test:
  `ssrf::presigned_urls_keep_their_query_for_the_fetch_only` (a local
  server that 403s unsigned requests).
- Remote estimator: response bodies capped at `max_body_bytes` (8 MiB
  default, `[estimator.members.X] max_body_bytes`), checked against
  `Content-Length` and while streaming; over the cap is a protocol error
  counted by the breaker (`oversized_bodies_are_refused`).
- `Gbt::load` / `Gbt::try_new` refuse a novelty file whose `features`
  schema differs from the trees file's (unlabeled = `features-v1`)
  (`novelty_schema_must_match_the_trees`).
- `gpu_obs`: a `GpuOom` without a GPU peak is `AtLeast(usable memory of one
  requested device) × devices` when the accelerator is known
  (`gpu_oom_without_peak_is_at_least_the_device`).

### 8. Local input paths

- `[features] local_paths = ["/dir", …]` (`FeaturesConfig::local_paths`):
  bare paths and `file://` URIs open only under these directories, after
  `canonicalize` (symlinks and `..` resolved). Unset: any path for in-process
  engines (CLI, Python); none for servers (`EngineBuilder::server(true)`:
  `rsz k8s serve`, i.e. the webhook and its prefetcher, and `rsz serve`).
  `[]` denies everywhere. Enforced in `DefaultResolver` (`LocalPaths`).
- Tests: `store::tests::local_path_policy`,
  `servers_read_no_local_paths_unless_allowlisted`.

### Measured (M-series laptop, `--quick`)

| bench | before (HEAD 6b831e5) | after |
|---|---|---|
| engine `decide/memo-hit/dry-run` | 358 µs | 339 µs |
| engine `decide/memo-hit/recorded` | 355 µs | 343 µs |
| core `decide/rs-grid-v1/cpu` | 117.7 µs | 113.7 µs |
| core `…/cpu/with-digest` | 216.3 µs | 217.0 µs |
| core `…/gpu` | 169.6 µs | 166.3 µs |
| core `…/gpu/with-digest` | 277.9 µs | 269.3 µs |

A memo hit with no revalidation due adds one clock read (no measurable
change; the bench's `MemoryStore` has run markers, so revalidation is on).
The S3(b) vetoes add one pass over at most K records per evaluated
proposal (cached per proposal, as before).
