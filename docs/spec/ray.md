# rightsizer for Ray (`rightsizer.ray`)

Status: **draft v2**. Core concepts: [SPEC.md](SPEC.md). Plugin contract:
[plugins.md](plugins.md). Ray references are to `ray-project/ray` `python/ray/`
at commit `262f169`.

## 0. Day one

```python
import rightsizer as rs

@rs.ray.remote(num_cpus=4, memory=16 * 2**30)      # replaces @ray.remote; default mode "recommend"
def embed(shard_uri: str) -> str: ...
```

```bash
rsz ray submit -- python job.py                  # propagates store + policy via runtime_env
rsz shadow --tenant ray://team-a/cluster-1 --since 7d
RIGHTSIZER__POLICY__MODE=enforce rsz ray submit -- python job.py
```

## 1. What Ray resources mean

- `num_cpus`, `num_gpus`, `memory`, `resources` and `accelerator_type` are
  **logical**. The scheduler packs tasks and actors onto nodes by them, but Ray
  does not cap what a task actually uses.
- Memory failures come from two places:
  - the raylet's **memory monitor**, which kills workers when *node* memory
    passes `memory_usage_threshold` (default 0.95);
  - cgroup or container OOM kills (on KubeRay, a container `OOMKilled`).

  An under-ask over-packs a node, and the cost lands on whichever co-located
  worker the monitor kills.
- So the Ray platform has `memory_enforced = false`. Memory reductions need
  S3(b) evidence, and the cost model adds a neighbour-risk premium (SPEC §2.3,
  §7.2). Slurm without `ConstrainRAMSpace` follows the same rule. Over-asking
  strands node capacity and makes the autoscaler add nodes, which is what sizing
  saves.

## 2. Remote functions

```python
@rs.ray.remote(num_cpus=4, memory=16 * 2**30, key="embed")   # sizing + in-task measurement
def embed(shard_uri: str, model: str) -> str: ...

tokenize = rs.ray.rightsize(ray.remote(num_cpus=1)(tokenize_fn))   # sizing only

refs = [embed.remote(u, "bge-large") for u in shard_uris]
fast = embed.options(num_cpus=8)            # num_cpus pinned at 8; memory stays rightsizable
```

**Mechanism.** `RightsizedRemoteFunction` subclasses
`ray.remote_function.RemoteFunction`.

- `RemoteFunction.__init__` assigns `self.remote` as an instance attribute that
  proxies to `self._remote` (`remote_function.py:174-183`). The subclass
  therefore overrides **`_remote`**, not `remote`. It decides, merges the
  decision into the task options, and calls the base `_remote`.
- `.options(**kw)` returns the base's `FuncWrapper` (`:339-357`), whose
  `.remote` calls `_remote` with the options. The subclass's `_remote` sees
  them and treats the axes set in `kw` as pins.
- `rs.ray.rightsize(fn)` builds a new `RightsizedRemoteFunction` from `fn`'s
  underlying function and options. Copying the instance would keep a closure
  bound to the original.
- `.bind()` (DAGs, Serve) is not sized in v1.
- Supported Ray versions are pinned in CI, because these internals are not a
  stable extension API.

**Arguments.** Only metadata is read:

- URI strings go through the metadata readers.
- `ObjectRef` arguments are **never** fetched. Their sizes come from
  `ray.experimental.get_object_locations(refs)["object_size"]`. That API is
  experimental and may return `None`, so the call is guarded.
- Ray Data datasets contribute nothing: `size_bytes()` would execute them.

**Rendering.** `Patch::Options` sets `num_cpus`, `memory` (bytes) and
`num_gpus`. When a GPU type change is allowed (M4), it also sets
`accelerator_type`, using only constants that nodes carry automatically: `T4`,
`A10G`, `L4`, `L40S`, `A100`, `H100`, and so on. `A100-40G`/`A100-80G` are
used only when `[ray] labelled_accelerators` says the operator labelled nodes
that way (`util/accelerators/accelerators.py:60-64`). Explicit
`resources={…}` entries are never touched.

**Overhead.** A `.remote()` call with a memoized estimate targets < 5 ms
(measured in M3). The memo holds normalized estimates keyed by (key, code
digest, feature bucket, policy hash) (SPEC §6.3). Calls are single-flight per
memo key.

**Platform state.** Node shapes come from the autoscaler's node types when the
cluster has them (`ray.autoscaler` config), otherwise from `[ray] shapes` in
configuration. Without either, increases are clamped to the definition ceiling
or held, counted as `clamped_total{reason="no_platform_state"}` (S4).

## 3. Actors

`ActorClass` subclassing is blocked by Ray (`actor.py:1591-1625`), so actors
are sized through a thin proxy:

```python
Embedder = rs.ray.sized(ray.remote(num_gpus=1)(EmbedderCls), key="embedder")
e = Embedder.remote(model="bge-large")      # decides once at creation → ActorCls.options(**decided).remote(...)
```

`rs.ray.sized(A)` exposes `.remote(...)` and `.options(...)`. It is not an
`ActorClass` (`isinstance` checks fail). Actors are sized once, at creation.

## 4. Ray Data

```python
ds = ray.data.read_parquet(paths)
ds = rs.ray.data.map_batches(ds, Embedder, key="embed-op", batch_size=256, concurrency=8, num_gpus=1)
```

`rs.ray.data.map_batches` makes **one decision per operator**, from the
operator's history and the metadata of the files given to the read (parquet
footers of `paths`). It then calls
`ds.map_batches(..., num_cpus=, num_gpus=, memory=)`. Per-task variation is not
supported: `ray_remote_args_fn` takes no arguments and is deprecated
(`data/dataset.py:219-226`).

## 5. Ray Jobs and KubeRay

- **`rsz ray submit [--address …] -- python train.py`** sets `RIGHTSIZER__*`
  in `runtime_env.env_vars`, so the job's `rs.ray` functions find the store and
  the policy. It also sizes the driver (`entrypoint_num_cpus`, `_num_gpus`,
  `_memory`). Drivers are usually light, so the environment propagation is the
  main value.
- **KubeRay:** handled by the Kubernetes webhook (kubernetes.md, M3). Each worker
  group is a unit. Container resources and the matching `rayStartParams`
  change together.

## 6. Outcomes

**In-task measurement** (`rs.ray.remote` only):

- The function body runs in a sampling scope: process RSS every 0.5 s, NVML
  per-device memory, CPU time.
- Workers are reused, so lifetime counters are not attributable. The record is
  the in-task maximum of sampled RSS, `Exact` with `resolution_s = 0.5`
  (SPEC §5.7). The worker's baseline RSS before the task is subtracted when it
  is known.
- Records are written directly to the store, which the driver configures
  through `runtime_env`.

**Status**, from the driver:

- `ray.exceptions.OutOfMemoryError` → `OomKilled`.
- `WorkerCrashedError` → `Failed`, upgraded to `OomKilled` when the state API or
  the raylet logs show a memory-monitor or cgroup kill.
- A `RayTaskError` wrapping `torch.cuda.OutOfMemoryError` → `GpuOom`.

**Prometheus importer:**

- `ray_component_rss_bytes{Component="ray::<fn>"}`: per function name, no
  per-task id (its labels are ip, pid, Version, Component and SessionName);
- `ray_node_gram_used`;
- `ray_tasks{Name,State}` for counts.

## 7. Escalation (not in v1)

Ray retries memory-monitor kills of retriable tasks under `task_oom_retries`,
a counter separate from `max_retries`. Its default, -1, means retry forever
(`src/ray/common/ray_config_def.h:159-166`). Rightsizer would have to change
`max_retries` to see those failures, and then every consumer of the ref that is
not rightsizer (`ray.get`, `ray.wait`, downstream tasks) would see failures Ray
would otherwise have absorbed. That is a new failure mode (S2).

So in v1:

- `rs.ray` never changes `max_retries`;
- `PlatformRetry.ladder` is `None`;
- `on_oom` is not offered.

Ray memory is unenforced anyway (§1), so S3(b) evidence is the only path to a
reduction.

## 8. Keys and trust

- **Suffix:** `key=`, or `{module}.{qualname}`.
- **Tenant:** Ray has no authenticated submitter identity by default. The tenant
  comes from deployment config (`[ray] tenant = "team-a/cluster-1"`), or from the
  K8s namespace on KubeRay.
- **Trust:** in-task records are `Authenticated` only when written with
  credentials scoped to that tenant's prefix (SPEC §8.2). Otherwise they are
  `Unverified`.
