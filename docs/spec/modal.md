# rightsizer for Modal (`rightsizer.modal`)

Status: **draft v3**. Core concepts: [SPEC.md](SPEC.md). Plugin contract and
integration tiers: [plugins.md](plugins.md). Modal references are to
`modal-labs/modal-client` `py/modal/` at commit `a7adb1d` (1.6.4) and to
modal.com/docs as of 2026-11-18. Claims marked **(verify M5)** are
conformance-test items.

## 0. Day one

```bash
rsz audit modal --since 14d        # tier A: per-function waste and GPU advisories from Modal's stats and billing APIs
```

```python
import modal, rightsizer as rs

app = modal.App("embed")

@app.function(gpu="A100-80GB", memory=32768, cpu=8)
@rs.modal.measure                        # in-container measurement (optional, recommended)
def embed(shard_uri: str) -> str: ...

@app.local_entrypoint()
def main():
    sized = rs.modal.sized(embed)        # default mode "recommend": prints what it would choose, runs as declared
    list(sized.map(shard_uris))
```

## 1. What Modal resources mean

Modal's resource parameters on `@app.function` / `@app.cls`
(`app.py:908-932`), converted in `_resources.py:10-52`:

- `memory`: an int (MiB) is a **request with no limit** (`memory_mb_max = 0`);
  a `(request, limit)` tuple adds a hard limit ≥ the request. A container over
  its request but under any limit can be **evicted** when its worker is under
  memory pressure ("Evicted (over memory request)", `cli/function.py:78`).
  A container over its limit is OOM-killed.
- `cpu`: a float request (the limit defaults to request + 16 cores) or a
  `(request, limit)` tuple. CPU over the limit is throttled, never killed.
- `gpu`: one string such as `"H100:8"`, or a ranked fallback list. Each list
  entry becomes its own function definition with the same CPU and memory
  (`_functions.py:1131-1186`). Plain `H100` may run on H200; `H100!` forbids
  that.

**Billing changes the cost model.** CPU and memory are billed per second on
the **greater of the request and the measured use** (modal.com/docs/guide/resources
and the pricing page). CPU and memory bundled with a GPU are not billed
separately; the catalog records each GPU type's bundle, and the cost model
prices only what exceeds it. Asking for less than the workload uses saves
nothing and adds eviction risk. Savings come from requests above use, and
above all from the GPU type and count. The platform model uses
`PriceModel::UsageFloor` (SPEC §7.2): under-asks only add risk, so the
penalties push the decision to the demand's upper quantile.

So:

```
PlatformRef { kind: "modal",
              memory_enforced: true,                               # see below
              retry: { same_size: Retries.max_retries, ladder: None (§6) },
              walltime_s: timeout, cpu_window_s: 60 }
PlatformState { request_model: Free (memory and CPU on the grid; GPU held, or a catalog of GPU strings when GPU type change is opted in),
                price: UsageFloor { cpu_core_s, mem_gib_s, gpu_s by type, gpu_bundle },   # from Workspace.billing.rates()
                quota: None }
```

`memory_enforced = true` for both forms of `memory`. With a `(request, limit)`
tuple the limit OOM-kills the container. With an int, a container over its
request is **evicted** under worker memory pressure — the unit itself fails,
unlike Ray, where an under-ask harms neighbours (ray.md §1) — and S5 counts
that eviction as rightsizer-caused. Treating eviction as certain failure
overstates the risk, which errs on the safe side. There is no ladder (§6), so
reductions need S3(b).

CPU is soft: the limit defaults to request + 16 cores, so `stretch` (SPEC
§7.2) is computed against the limit, not the request, and CPU decisions
mostly move the billing floor.

## 2. Per-call sizing: `rs.modal.sized`

```python
sized = rs.modal.sized(embed, key="embed")          # or rs.modal.sized(modal.Function.from_name("embed", "embed"))
sized.remote(uri)                                   # one decision
sized.map(uris)                                     # one decision for the map (a group)
sized.spawn(uri)                                    # one decision; no ladder (§6)
rs.modal.sized(Embedder)                            # a modal.Cls: decides once per .with_options() / instance
```

**Mechanism.** `Function.with_options(cpu=, memory=, gpu=, …)`
(`_functions.py:1846-1914`, 1.4.3+) returns a **variant** of the function.
Creating one is a single `FunctionBindParams` call; the result is
deterministic per option set and cached in a 32-entry LRU on the base handle
(`_function_variants.py:351-451`). `rs.modal.sized(fn)` returns a proxy whose
`.remote`, `.map`, `.starmap`, `.spawn` and `.for_each`:

1. build the context (§4), decide, and round the decision to a **variant
   shape** (below);
2. call `fn.with_options(**decided)` and forward the call.

There is no hook inside Modal's `.remote()`/`.map()`/`.spawn()`
(`_functions.py:2098`, `:2258`), so the proxy is the integration point.

**Variant budget.** Each variant runs in its own container pool and
autoscales independently (`_functions.py:1866-1868`): a new shape pays a cold
start, and warm containers of the base function are not reused (whether
`min_containers` and memory snapshots carry over to variants: verify M5).
Therefore:

- decisions are rounded to a **coarse shape set**: memory to every other cell
  of the `rs-grid-v1` memory ladder (powers of two), CPU to the grid's powers of
  two (0.25, 0.5, 1, 2, 4, 8, 16, 32, 64), GPU to catalog entries;
- at most `max_variants` (default 4) distinct shapes per function per 24 h;
  beyond that, a decision snaps up to the nearest existing variant;
- the cold start is priced: `attempt_overhead` includes the function's
  measured cold-start time when the shape is not already warm.

**Limits of `with_options`** (`_function_variants.py:65-159`):

- no `ephemeral_disk`, and a single GPU string only. A function declared with
  a GPU **fallback list** keeps its GPU axis held (the list is the user's
  capacity preference); CPU and memory may still change only if the user
  opts in (`allow_fallback_collapse = true`), because `with_options(gpu=…)` would
  collapse the list;
- a GPU cannot be removed: a GPU → CPU-only change is impossible, so "needless
  GPU" stays an advisory;
- `secrets` and `volumes` passed to `with_options` replace the base set, so
  the proxy never passes them.

**Pins.** Axes the caller passes through `sized.with_options(...)` are pins.
Explicit `memory=(req, limit)` tuples keep their limit/request ratio (SPEC
§7.4 limits policy). The proxy always passes `memory` and `cpu` in the base
definition's form: variant options are merged into the base as protobuf, and
a zero `memory_mb_max` from an int does not clear a base limit
(`_function_variants.py:135-158`), so mixing forms could leave a stale limit
below the new request (verify M5).

**Concurrency.** Functions with `@modal.concurrent` or `@modal.batched` run
many inputs per container, so per-input sizing is meaningless. They are
**deploy-time only** (§3): the proxy forwards calls unchanged and records
container-level outcomes.

**Latency.** A memoized decision (SPEC §6.3) plus a cached variant costs no
round trip; a new variant costs one `FunctionBindParams` call. Target: < 5 ms
per call once warm (measured in M5).

## 3. Deploy-time sizing

`rsz suggest app.py` (SPEC §12.4) reads the `@app.function` /
`@app.cls` decorator arguments with a Python parser and proposes **text edits**
to `memory=`, `cpu=` and `gpu=` from history: increases always, reductions
only where S3(b) holds. It never edits a call whose arguments are not
literals. The output is a diff or a pull request; a human merges it, and the
next `modal deploy` applies it. This is the only sizing path for concurrent
and batched functions, and for deployed functions called from outside Python.

## 4. Context

- **Key suffix:** `key=`, else `{app}.{function}` from `fn.info()` or the
  decorator site.
- **Code:** the wrapped function's source when the proxy is built around a
  local function; `None` for `Function.from_name`.
- **Inputs:** URI-like string arguments and `modal.Volume` paths go through
  the metadata readers (SPEC §9). `.map` aggregates its inputs as the max per
  feature over the first 1,000 inputs, so one decision covers the batch.
- **Prior:** `fn.info()` (`_functions.py:1612`) reports `cpu`, `memory_mib`,
  `gpus`, `ephemeral_disk_mib`, `timeout` and `max_retries` of the deployed
  definition (`_function_variants.py:474-520`).

## 5. Outcomes

| source | gives | attribution | censoring |
|---|---|---|---|
| `@rs.modal.measure` (in-container) | RSS (cgroup), NVML per device, CPU time, every 0.5 s, from function entry to return | `modal.current_input_id()`, `current_function_call_id()` (`_runtime/execution_context.py:41,60`) | `Exact`, `resolution_s = 0.5`; under concurrency, container-level only |
| `Function.stats(container=…, since, until, all_variants=…)` (`_functions.py:2309-2349`) | p50/p90/p99/p99.9 of "Memory Usage (GiB)", "CPU Usage (cores)", "GPU Utilization (%)" per container (`api.proto:5282-5292`) | container; exact per input only with `single_use_containers=True`. Stats exclude variants unless `all_variants=True`; each variant has its own function id, which the importer maps back to the base function and the decided shape | a percentile is **not** a peak: `AtLeast(p99.9)` |
| OpenTelemetry / Datadog export | `modal.memory.usage`, `modal.cpu.utilization`, `modal.gpu.memory.usage` tagged by container, function id (per variant), app, environment; `modal.container.terminations{reason}` (joined by `object_id`, it has no function id) | container | max of the series: `Exact`, `resolution_s` = export interval, once M5 confirms the series are samples, not averages (SPEC §5.7) |
| `modal function calls --json` (`FunctionCallFetch`) | per input: status (`KERNEL_OOM_KILL`, `MEMORY_MANAGER_EVICTION`, `MEMORY_THRASHING_EVICTION`, …), container id, start, execution time (`api.proto:237-251, 2626-2635`) | input | — |
| `Workspace.billing.report(...)` (`_workspace.py:542`) | cost by CPU, memory and GPU type | function / tag | used by `audit` and `shadow` for dollars |

- **Status:** `KERNEL_OOM_KILL` → `OomKilled`; `MEMORY_*_EVICTION` →
  `Evicted` (rightsizer-caused when memory was reduced, SPEC §2.3 S5). On the
  client, every such failure surfaces as `modal.exception.RemoteError`
  (`_utils/function_utils.py:533-574`); the proxy records `Failed` and the
  importer upgrades it from the call record. A `RemoteError` wrapping
  `torch.OutOfMemoryError` → `GpuOom`.
- **Enforcing reductions needs the importer.** The proxy alone cannot tell an
  OOM from a user exception, so a reduced call's failure stays unclassified
  until `rsz history import modal` runs, and S5 counts it as
  rightsizer-caused meanwhile. The proxy enforces reductions only when the
  importer's last successful run is within `[modal] import_max_lag`
  (default 30 min, read from the store); otherwise reductions are
  recommend-only.
- **GPU actually used:** Modal may upgrade `H100` to `H200`. Records store the
  billed GPU type (`gpu_billing_type` in the OTel export) as `requested`, so
  history is not skewed.

## 6. Ladders

None in v1.

- `modal.Retries` retries the same input on the same variant
  (`_functions.py:311-341`), so platform retries never change size.
- A caller ladder (`sized.remote` catching an OOM and re-calling a bigger
  variant) needs the terminal status of the failed input on the client. The
  client sees only `RemoteError`; the per-input status is available through
  `FunctionCallFetch`, which the public SDK does not expose. Re-running on
  *any* `RemoteError` would re-run user code that raised its own exception,
  which S2 forbids. If M5 finds a public, per-input OOM status, `sized.remote`
  and `sized.map` gain a caller ladder; until then reductions need S3(b).

Modal's billing makes this cheap to accept: reductions below use save nothing
(§1), so the savings that wait for history are over-requests, not under-asks.

## 7. Tenant and trust

- **Tenant:** `modal://{workspace}/{environment}`, from
  `modal.Workspace.from_context()` (`_workspace.py:104`) and the client's
  environment, both resolved with the caller's token.
- In-container records are written with the store credentials given to the
  function as a `modal.Secret`. They are `Authenticated` only when those
  credentials are scoped to the tenant's prefix (SPEC §8.2). Importer records
  (stats, calls, billing) are `Verified`.

## 8. Tests (M5)

- Recorded `FunctionBindParams` requests for each decided shape; the variant
  budget holds under a 10k-input `.map`.
- A function with a GPU fallback list: GPU held, nothing else changed without
  the opt-in.
- `memory=int` and `memory=(req, limit)`: both are treated as enforced, and no
  neighbour-risk premium is applied.
- Concurrent function: the proxy forwards unchanged.
