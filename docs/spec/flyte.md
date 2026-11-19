# rightsizer for Flyte v2 (`rightsizer.flyte`)

Status: **draft v2**. Core concepts: [SPEC.md](SPEC.md). SDK references are to
`flyteorg/flyte-sdk` `src/flyte/` at HEAD `2e821ef` (2026-11-18).

## 0. Day one

```python
import flyte, rightsizer as rs

env = rs.flyte.RightsizedEnvironment(          # instead of flyte.TaskEnvironment
    name="train", resources=flyte.Resources(cpu=8, memory="64Gi", gpu="A100_80G:1"),
)                                              # default mode "recommend": nothing changes yet

@env.task
async def finetune(base_model: str, data: flyte.io.DataFrame) -> flyte.io.Dir: ...
```

```bash
rsz history import flyte --project ml --domain development --since 30d   # bootstrap history
rsz shadow --tenant flyte://acme/ml/development --since 7d                # what enforcing would do
```

```python
rs.flyte.with_runcontext(rightsize="enforce").run(main, cfg)   # enforce for this run; drop the argument to stop
```

`recommend` changes nothing. Run `rsz shadow`, then turn on enforce
per run, or per environment with `rightsize="enforce"`.

## 1. How Flyte v2 constrains the design

| fact | where | consequence |
|---|---|---|
| A sub-task's spec is built **in the parent's process** when the parent calls it: `TaskTemplate.aio` → `controller.submit` → `RemoteController._submit` → `translate_task_to_wire(task)` reads `task.resources`. | `_task.py:336-356`, `_internal/controllers/remote/_controller.py:222-331`, `_internal/runtime/task_serde.py:356` | Per-invocation sizing happens **in the caller**. |
| `override(...)` returns a `dataclasses.replace` copy (same class); `resources`, `env_vars` and `pod_template` are each replaced wholesale. | `_task.py:444-451, 540-582` | Rightsizer controls its own `override` and merges fields itself. |
| No public hook between input conversion and spec building; the controller is private. | `_internal/controllers/__init__.py:24,128-140` | Integration is a `TaskTemplate` subclass, never a controller patch. |
| `with_runcontext` has no resource override; `custom_context` (str → str) **propagates to all descendant actions**; `flyte.custom_context` is a contextvar-backed sync context manager that works in async code and merges when nested. | `_run.py:1882`, `_custom_context.py` | Run-wide policy travels in one `custom_context` key. |
| One `plugin_config` per environment. | `_task_environment.py:156, 385-397` | Rightsizer never uses `plugin_config`. |
| Reusable tasks reject `override(resources=…)` unless `reusable="off"`; resources are part of the reuse pool identity. | `_task.py:509-532`, `_internal/runtime/reuse.py` | Reusable environments are observe-only per invocation and recommend-only at deploy (§6.3). |
| Resources **and env vars** are excluded from the action identity and the cache key; `custom_context` is excluded from the inputs hash. | `_internal/runtime/convert.py:1057-1068, 1101-1170` | Decisions and the decision env var are safe across recovery, replay and caching. Cache hits produce no measurement. |
| `OOMError` reaches the parent only for USER-kind errors whose code contains "OOM"; a CUDA OOM arrives as `RuntimeUnknownError` with `code="OutOfMemoryError"`. | `errors.py:98`, `_internal/runtime/convert.py:774-867` | Explicit classification (§8.2). |
| `post()` runs only after a successful return; `execute` is what runs in the pod. | `_task.py:241-254, 724-732`; `taskrunner.py:121` | Measurement wraps `execute` in `try/finally` (§8.1). |
| `flyte.map` calls `.aio` per item with unbounded concurrency by default. | `_map.py:70-80`, `_controller.py:194-198` | Decisions need their own limiter, single-flight and memoization (§7). |
| Local mode: `flyte.ctx().mode == "local"`; resources are ignored. | `models.py:305`, `_map.py:362-372` | Inert locally (§4.5). |
| Pickled bundles cloudpickle the task per child. | `_controller.py:237-243` | `RightsizedTask` holds only plain, picklable data. |
| The in-pod resolver loads `getattr(module, name)`; `env.task` registers into the private `env._tasks`; deploy serializes `env.tasks`. | `_internal/resolvers/default.py:23`, `_task_environment.py:430-463`, `_deploy.py:571` | The rightsizable object must be the module attribute *and* the registered task (§4.1). Registry writes use private API (§11). |
| `TaskEnvironment` is a non-`kw_only` dataclass; `clone_with` requires a new `name` and starts with an empty task registry. | `_environment.py:33`, `_task_environment.py:48, 154, 169-253` | Subclass fields need defaults and `kw_only`; deploy-time sizing rewrites tasks, not the environment (§6.3). |

## 2. Levels at a glance

| level | API | role |
|---|---|---|
| **task** | `@rs.flyte.rightsize(...)` | makes one task rightsizable everywhere it is called, with a **definition policy** |
| **environment** | `rs.flyte.RightsizedEnvironment(..., rightsize=...)` | makes every `@env.task` rightsizable with a shared definition policy; injects image and store configuration |
| **action** | `rs.flyte.sized(task, ...)`; `with rs.flyte.policy(...)` | an **invocation policy** for one call (or one `flyte.map`) / for every call in a block |
| **run** | `rs.flyte.with_runcontext(rightsize=...)`; `rs.flyte.run_context(...)`; `rsz flyte run` | the invocation policy for the whole run; sizes the **root** action |

Definition scope (task, environment) says **what may change**. Invocation
scope (action, run) says **how aggressively, now**. Strings are shorthand:

- At invocation scope (`with_runcontext(rightsize=…)`, `sized`, `policy`), a
  string `s` means `mode = s`. `sized()` and `policy()` take `mode=`, with
  `rightsize=` accepted as an alias.
- At definition scope (`RightsizedEnvironment(rightsize=…)`,
  `@rightsize("enforce")`), a string `s` sets `default_mode = s`, and also
  `max_mode = "off"` when `s == "off"`.

## 3. Policy model

### 3.1 Definition policy

```python
@dataclass(frozen=True)
class DefinitionPolicy:
    max_mode: Mode = "enforce"             # most invasive mode any invocation may use; "off" disables entirely
    default_mode: Mode = "recommend"       # when no invocation scope sets a mode
    pins: frozenset[str] = frozenset()     # axes never changed: "memory", "cpu", "gpu", "gpu_type", "disk", "shm"
    allow_gpu_type_change: bool = False
    ceiling: flyte.Resources | None = None
    floor: flyte.Resources | None = None
    on_oom: Literal["auto", "fail", "escalate"] = "auto"   # auto = escalate when the task's retries == 0
    max_escalations: int = 2
    key: str | None = None                 # workload-key suffix; default "{env name}.{task short name}"
    risk: Risk | None = None
```

A task's decorator policy is merged over its environment's, field by field
(the task wins).

### 3.2 Invocation policy and the stack

```python
@dataclass(frozen=True)
class InvocationPolicy:
    mode: Mode | None = None
    risk: Risk | None = None               # "lean" | "balanced" | "averse" | float oom_penalty
    estimator: str | None = None           # a configured chain name
```

Invocation policies form a stack, innermost on top:

1. the run's policy;
2. each enclosing `policy()` block;
3. `sized(...)`.

Each field resolves to its innermost value. The stack is serialized as compact
JSON (`{"v":1,"s":[…]}`, at most 1 KiB) in the `custom_context` key
`rightsizer`, so descendants inherit it.

### 3.3 Resolution

```
effective.mode = min(stack.mode ?? definition.default_mode, definition.max_mode, mode_ceiling)   # off < observe < recommend < enforce
effective.risk = stack.risk ?? definition.risk ?? config.risk
constraints    = definition pins / ceiling / floor / allow flags  +  axes set by explicit call-site resources (pins)
```

- An invocation never relaxes a definition constraint.
- `mode_ceiling` is the operator's global switch (SPEC §2.3).

## 4. Task level

```python
@rs.flyte.rightsize(pins={"gpu"}, key="llama-ft")
@env.task
async def finetune(base_model: str, data: flyte.io.DataFrame) -> flyte.io.Dir: ...
```

### 4.1 `RightsizedTask`

`rsz` must be the **outermost** decorator; it raises `TypeError` on
anything that is not a `TaskTemplate`. It returns a `RightsizedTask`: a
`@dataclass` subclass of the task's own template class. The subclass is created
once per class and cached, so plugin task classes keep working and `_rebuild_as`
still applies. `TaskTemplate` is `kw_only` (`_task.py:92`), so the extra fields
compose:

```python
_rs_policy: DefinitionPolicy = field(default_factory=DefinitionPolicy)   # plain, picklable
_rs_pins:   frozenset[str]   = frozenset()                               # axes pinned by explicit overrides
_rs_bypass: bool             = False                                     # set on the sized copy handed to the controller
```

The decorator replaces the original in its environment's registry, so
`flyte deploy`, the resolver (via the module attribute the decorator returns)
and introspection all see one object. No clients, locks or native handles live
on the task; the client is a module-level singleton, rebuilt after fork.

Methods:

- **`override(**kw)`** calls `super().override(**kw)` and adds every axis set in
  `kw["resources"]` to `_rs_pins`. Those axes are never changed; the others stay
  rightsizable.
- **`aio(*args, **kw)`** and **`__call__(*args, **kw)`**:
  1. **Delegate unchanged** to the base class if any of these hold:
     - `_rs_bypass` is set;
     - there is no task context;
     - in local mode (§4.5);
     - the task is reusable (observe-only, §6.3);
     - the effective mode is `off`.
  2. **Capture the invocation stack eagerly**, from the contextvar and
     `flyte.get_custom_context()`, before returning the coroutine. Coroutines
     created inside a `policy()` block keep that block's policy even if they are
     awaited after it exits.
  3. **Build the context** (§4.2) and **get a decision** (§7).
  4. **Build the sized copy:**
     `self._replace(resources=merged if enforce else self.resources,
     env_vars=(self.env_vars or {}) | {"RIGHTSIZER_DECISION": <compact json>},
     _rs_bypass=True)`. `_replace` (`_task.py:444`) is used so plugin classes
     rebuild correctly. Then delegate to `TaskTemplate.aio(copy, ...)`.
     `RIGHTSIZER_DECISION` carries the decision id, key, mode, the requested
     spec and `measured` (whether the child runs a `RightsizedTask`). The env
     var is added in every mode except `off`. It is not part of the cache key.
  5. **Record the parent-side outcome** (§8.2). With escalation on, catch
     OOM-class failures and re-invoke along the ladder (§9).

  For `_call_as_synchronous` tasks, `__call__` blocks and uses the sync
  decision API, exactly as the base does.
- **`execute(...)`**, which runs in the child pod, wraps the base `execute` in
  the measurement scope (§8.1).

### 4.2 Context from a Flyte invocation

| field | source |
|---|---|
| key | `flyte://{org}/{project}/{domain}/{definition.key or "env.task"}`; the tenant comes from `flyte.ctx().action` |
| code | `inspect.getsource(func)`, captured **once at decoration**. Fallback: a digest of `func.__code__` (pickled bundles). |
| inputs | per argument: `flyte.io.File`/`Dir` → `path` → reader; `flyte.io.DataFrame` → `uri` + `format` → reader; `str` that parses as a URI → reader; `int`/`float`/`bool` → scalar; otherwise the type name only. Reads are cached per (URI, `hash`) for the process. |
| prior | `task.resources` (the environment's, copied at decoration) |
| platform | `flyte`; overhead distribution (median 70 MiB, p90 242 MiB); `PlatformRetry{same_size: retries, ladder: if escalating}` (root actions: `ladder = None`, §6.1); walltime from `timeout` (`Timeout`, `int` or `timedelta`, normalized); `memory_enforced = prior memory is a (request, limit) tuple` until SPEC §16 (open question 5) is settled, so scalar-memory tasks get S3(b) reductions only |
| history | the store, by key |

### 4.3 Merging a decision into `flyte.Resources`

The merge starts from the prior and replaces only the decided axes:

- **memory:**
  - a scalar if the prior was a scalar: request only, no limit added (S2);
  - a `(request, limit)` tuple if the prior was a tuple, keeping its ratio.
- **cpu:** same rule as memory.
- **gpu:**
  - kept unless a type change is allowed and decided. Then
    `flyte.GPU(device, count)` uses the catalog's Flyte names: `A100_80G`, not
    the deprecated `"A100 80G"`.
  - A prior of `gpu=<int>` is `unknown-gpu`; its type is never changed.
- **disk, shm:** kept (held in v1).

### 4.4 Image

Every task that **calls** a rightsizable task makes decisions in its own pod,
so its image needs the `rightsizer` wheel for its platform. Leaf tasks need
only `rightsizer-runtime` (pure Python) for measurement. `RightsizedEnvironment`
has `auto_image=True` by default: for `flyte.Image` images it appends
`with_pip_packages("rightsizer==<version>")`. For string images it warns at
definition time (`strict`: it raises). If the native module is missing at the
first decision, the error is caught (S1) and the call runs at the prior.

### 4.5 Local execution

In local mode, rightsizer decides nothing and changes nothing. With
`observe_local = true` it records measured peaks with `platform = flyte-local`
and `requested = None`. Learning excludes these records.

## 5. Action level

```python
@env.task
async def driver(paths: list[str], df: flyte.io.DataFrame):
    ckpt = await rs.flyte.sized(finetune, risk="averse")("llama-8b", df)         # one call
    outs = list(flyte.map(rs.flyte.sized(preprocess, rightsize="enforce"), paths))  # one map
    with rs.flyte.policy(mode="enforce", risk="lean"):                           # a block, inherited by grandchildren
        await asyncio.gather(*(preprocess(p) for p in paths))
```

- `rs.flyte.sized(task, **invocation_policy)` works on **any** task.
  - It returns a `RightsizedTask` with the invocation policy on top of the stack.
  - An undecorated task gets its environment's definition policy, or the
    default one.
  - The child pod of an undecorated task runs the plain template, so
    `RIGHTSIZER_DECISION.measured = false`. The parent records the status with
    `memory_peak = None`, and the outcome counts as joined. Peaks come later
    from backfill (§8.3).
- `rs.flyte.policy(...)` is a sync and async context manager. It pushes onto the
  contextvar stack and enters `flyte.custom_context(rightsizer=<stack json>)`.

## 6. Run level, roots, deploy-time sizing

### 6.1 Runs

```python
# Sizes the root action and sets the run's invocation policy.
run = rs.flyte.with_runcontext(rightsize="enforce", project="ml", domain="development").run(main, cfg)

# Policy only, composable with any with_runcontext call:
flyte.with_runcontext(custom_context=rs.flyte.run_context(mode="enforce")).run(main, cfg)
```

```bash
rsz flyte run --mode enforce -- train.py main --cfg prod.yaml
# ≡ flyte run --custom-context rightsizer='{"v":1,"s":[{"mode":"enforce"}]}' train.py main --cfg prod.yaml
```

- `rs.flyte.with_runcontext(rightsize=…, **kw)` forwards every
  `flyte.with_runcontext` keyword and merges the policy into `custom_context`.
  Its `.run(task, *args)` sizes the root with
  `flyte.run(task.override(resources=merged))`.
- The run level is a **policy modifier**. It changes mode and risk for
  rightsizable tasks. It does not make undecorated tasks rightsizable; use the
  environment level for that.
- The root has no parent to catch its failure, so root decisions use
  `PlatformRetry{same_size: retries, ladder: None}`: root reductions need
  S3(b) evidence.
- `rsz flyte run` passes only the policy. It does not size the root,
  because that would mean re-implementing the module loading and argument
  parsing of `flyte run`.

### 6.2 Roots launched elsewhere

Runs started from the UI, schedules, triggers or `flyte.remote` reruns bypass
both of the above. They get **deploy-time sizing** (§6.3).

### 6.3 Deploy-time sizing

```bash
rsz flyte deploy train.py env           # or rs.flyte.deploy(env)
rsz flyte reconcile --schedule 5m       # singleton; required for deploy-time reductions
rsz flyte deploy --restore train.py env # redeploy from source with sizing disabled
```

1. For each task in the environment, compute an **input-independent**
   decision: `b_now` is the p90 of recent same-key input sizes (SPEC §7.4),
   and the decision uses S3(b) evidence only, since a deployed default has no
   ladder.
2. Rewrite each task as `t.override(resources=merged)` and replace it in the
   environment's registry (§11).
3. Log each deploy decision to `{prefix}/v1/decisions/`, with the deploy
   version and the **origin prior** (the source definition's resources). Run-time
   sizing (§4) and the Kubernetes webhook (§12) read the origin prior from that
   record by task name and deploy version, so attribution and `final_floor`
   stay relative to what the author wrote (SPEC §2.3 S5). If the record cannot
   be read, they treat the deployed value as the prior, which only forgoes
   savings: deploy-time values are never below the origin prior without
   `reconcile`.
4. Call `flyte.deploy`.

Rules:

- **The prior is the source definition.** `--restore` deploys from source with
  sizing disabled. (Task `docs` cannot hold it: `override` rejects `docs`, and
  deploy does not serialize it.)
- **Reductions need `reconcile`.** A deployed spec keeps running after a
  breaker trips or `mode_ceiling` drops. Deploy-time reductions are therefore
  applied only where `rsz flyte reconcile` is scheduled. It is a singleton
  (lease as in SPEC §8.1) that redeploys affected tasks at the prior within
  10 minutes of either event. Without it, deploy-time sizing is
  **increases-only**.
- **Reusable environments are recommend-only in v1.** `override(resources=…)`
  raises on reusable tasks, and parents submit the module attribute's resources,
  so a deploy-time size would not reach them. Instead, `rsz shadow`
  reports a suggested `resources=` for the environment.
- Tasks without enough evidence deploy unchanged.

### 6.4 Platform state

The Flyte SDK does not expose node shapes or quota. `PlatformState` comes from
`[flyte] shapes` (and optional prices) in configuration, injected into pods as
`RIGHTSIZER__*` by `RightsizedEnvironment`. Without shapes, increases are
clamped to the definition `ceiling` (or held if there is none) and counted as
`rightsizer_clamped_total{reason="no_platform_state"}` (S4).

## 7. Latency and fan-out

Per parent process:

- **Single-flight.** Concurrent decisions with the same memo key share one
  in-flight computation, so a 10,000-item `flyte.map` over similar inputs waits
  on one decision.
- **Limiter.** At most `max_concurrent_decisions` (64) *distinct* decisions run
  at once.
- **Budget.** Each call has a hard budget, default 250 ms, metadata reads
  included. On expiry the call proceeds at the prior with
  `Prior{reason="budget"}` (S1). It is counted as
  `rightsizer_budget_expired_total{platform="flyte"}`.
- **Batching.** Remote estimator calls are coalesced into `/v1/estimate:batch`.
- **Memo.** Keyed by (key, code digest, feature bucket, policy hash). Metadata
  reads are cached per (URI, hash). Source is captured once at decoration.

## 8. Outcomes

### 8.1 Child side (pod)

`RightsizedTask.execute`:

```
start a 1 s sampler: cgroup cpu.stat (usage, throttled), NVML per-device memory (if pynvml and a GPU are present)
try:     result = base.execute(...)
finally: memory peak = cgroup v2 memory.peak (kernel ≥ 5.19)
                       | cgroup v1 memory.max_usage_in_bytes
                       | max(ru_maxrss self, ru_maxrss children)           [AtLeast]
         write a RunRecord shard (flyte.storage, through rightsizer-runtime)
```

- The cgroup peak covers dataloader workers, subprocesses and `/dev/shm`.
- Reusable containers report `memory_peak = None` (not attributable).
- An OOM kill skips `finally`; the parent records that outcome.
- **Store.** `RIGHTSIZER__HISTORY__URI`, injected by `RightsizedEnvironment`.
  If it is unset, the default is `{scheme}://{bucket}/rightsizer/`, derived
  from the action's `raw_data_path` bucket.
- **Trust.** Records are `Authenticated` when the deployment gives each
  project/domain credentials scoped to its own prefix
  (`{prefix}/v1/runs/flyte/{org}/{project}/{domain}/`). Otherwise they are
  `Unverified`. In that case enforce relies on the escalation ladder (S3a,
  on by default for `retries == 0`) and on `flyte.remote` backfill, which is
  `Verified` for statuses but has no peaks in open-source Flyte. The operator can
  also opt in with `history.allow_unverified_for_reductions` (SPEC §8.2).

### 8.2 Parent side

After the child completes, the parent writes a `RunRecord` (requested spec,
duration, attempt, status):

| child result | status |
|---|---|
| returns, child record found (or `measured = false`) | `Succeeded` |
| returns, `measured = true`, no child record within 10 min | `Cached` (provisional; confirmed or corrected by backfill) |
| `flyte.errors.OOMError` | `OomKilled` |
| `RuntimeUnknownError` with code `OutOfMemoryError`, or a message matching `CUDA out of memory` / `HIP out of memory` | `GpuOom` |
| a timeout error | `Timeout` |
| other | `Failed` |

### 8.3 Backfill

`rsz history import flyte` walks `flyte.remote.Run.listall`,
`Action.listall` and `ActionDetails`. It reads:

- requested resources from the task template proto (`container.resources`,
  `extended_resources.gpu_accelerator`);
- the phase, error kind and phase durations.

When `flyteplugins-union` is installed (proprietary, Union only), it also reads
per-action metrics via `flyteplugins.union.remote.Metrics.get_for_action`.
Open-source Flyte has no metrics API; there, child records are the only peak
source.

## 9. Escalation (`on_oom`)

- `auto` (default) escalates when the task's `retries == 0`, and otherwise behaves
  like `fail`. With retries, the backend first retries at the same size and the
  cost model accounts for it (`same_size = retries`).
- **Steps:**
  - `OomKilled` → memory × 2.
  - `GpuOom` → the next GPU shape with more memory, only if GPU type changes are
    allowed. Otherwise the failure is final.
- **Limits:** at most `max_escalations` steps. The final step is at least
  max(prior, largest observed success) (S3a), and never exceeds the ceiling. If
  that floor exceeds the ceiling, S3(a) does not hold and reductions are held.
- Every attempt is its own action and its own record.

## 10. Environment level

```python
env = rs.flyte.RightsizedEnvironment(
    name="etl",
    resources=flyte.Resources(cpu=4, memory="16Gi"),
    rightsize="enforce",                       # or a DefinitionPolicy
)
```

- `RightsizedEnvironment` is a `@dataclass` subclass of `flyte.TaskEnvironment`
  with `rightsize: DefinitionPolicy | str = field(default_factory=DefinitionPolicy, kw_only=True)`
  and `auto_image: bool = field(default=True, kw_only=True)`.
- `.task` returns `RightsizedTask`s.
- `clone_with` keeps both fields (it uses `dataclasses.replace`).
- It injects `RIGHTSIZER__*` configuration into `env_vars`.
- Not covered in v1:
  - Decorators other than `.task` (orchestrator and sandbox variants). Size
    those tasks per call with `rs.flyte.sized`.
  - Converting an environment after its tasks are defined. Define it as
    `RightsizedEnvironment` instead.
- `reusable=` environments: observe-only per invocation; recommend-only at
  deploy (§6.3).

## 11. Private API and upstream asks

Writing the environment's task registry (`_tasks`), for the decorator (§4.1)
and for deploy-time sizing (§6.3), uses private API. Supported flyte versions
are pinned in CI.

Upstream asks that would remove the private-API use:

1. a public `TaskEnvironment.replace_task(name, task)`;
2. a public pre-submit hook on the controller (`(task, args) -> task`);
3. a typed resources accessor on `ActionDetails`.

## 12. Interaction with the Kubernetes webhook

Flyte task pods are Kubernetes pods. The webhook skips any pod whose containers
carry `RIGHTSIZER_DECISION`, so no pod is sized twice.

Pods of plain Flyte tasks may be sized by the webhook. They are keyed
`flyte://{org}/{project}/{domain}/{task}` from Flyte's pod labels, and so share
history with Flyte-side sizing, **only if** the pod's namespace equals the
configured `[k8s.flyte] namespace_template` (default `{project}-{domain}`)
rendered from those labels. Anyone who can create pods can set labels; the
namespace check stops a pod in one namespace from writing another project's
history. Any other pod is keyed `k8s://…`.

## 13. Tests

- **Unit:**
  - the policy-resolution table;
  - the merge rules (§4.3);
  - classification (§8.2), with a CUDA OOM fixture;
  - eager policy capture across `gather`.
- **Integration**, on a local sandbox cluster:
  - 1,000-way fan-out under `enforce` stays within budget;
  - a forced OOM escalates, and both attempts are recorded;
  - a reusable environment is observe-only;
  - pickled bundles submit;
  - local mode is inert;
  - deploy-time sizing round-trips with `--restore`;
  - a missing native module falls back to the prior.
