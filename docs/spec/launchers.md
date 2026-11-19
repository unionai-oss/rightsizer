# rightsizer for GPU launchers: SkyPilot and dstack

Status: **draft v3**. Core concepts: [SPEC.md](SPEC.md). Plugin contract and
integration tiers: [plugins.md](plugins.md). References: `skypilot-org/skypilot`
`sky/` and `dstackai/dstack` `src/dstack/` at their default branches on
2026-11-18. Claims marked **(verify M6)** are conformance-test items.

SkyPilot and dstack launch the same AI job onto many backends — clouds,
Kubernetes, Slurm, GPU neoclouds. Both have a **server-side policy hook that
can mutate a request**. That makes them the cheapest way to reach GPU jobs
that never touch a cluster rightsizer already covers.

## 0. Day one

```yaml
# ~/.sky/config.yaml on the SkyPilot API server
admin_policy: rightsizer.skypilot.Policy           # default mode "recommend": logs, adds only the decision marker, resources unchanged
```

```yaml
# ~/.dstack/server/config.yml
plugins: ["rightsizer"]
```

```bash
rsz shadow --tenant sky://ml-workspace/alice --since 14d
```

## 1. SkyPilot

### 1.1 Hook

`sky.AdminPolicy.validate_and_mutate(user_request) -> MutatedUserRequest`
(`sky/admin_policy.py:190-234`). "The policy can mutate both task and
skypilot_config." It runs on the API server once per task of the DAG
(`sky/utils/admin_policy_utils.py:281-304`), for every request name in
`AdminPolicyRequestName` (`sky/server/requests/request_names.py:113-138`):
`launch`, `exec`, `jobs launch`, `serve up` and `serve update`, but also
`validate` and `optimize`, which launch nothing, and the controllers' own
launches. Managed-job **recovery** relaunches through `sdk.launch(...,
_is_launched_by_jobs_controller=True)` (`sky/jobs/recovery_strategy.py:1591`),
which becomes `JOBS_LAUNCH_CLUSTER` and runs the policy again
(`sky/execution.py:326-345`).

So the policy must be idempotent:

- **Marker.** A sized task gets `RIGHTSIZER_DECISION=<id>` in its `envs`. A
  task that already carries the marker is never re-decided: recovery and
  controller relaunches keep the original decision.
- Decisions for `validate` and `optimize` are computed (so `sky launch`'s
  plan shows them) but not logged as launches.

**SkyPilot fails closed.** Any exception a policy raises becomes
`UserRequestRejectedByPolicy` (`admin_policy_utils.py:298-304`), and the
built-in `RestfulAdminPolicy` raises `RestfulPolicyError` when its POST fails
or times out (30 s, `admin_policy.py:268-290`). A rightsizer outage would
block every launch, which S1 forbids. Therefore:

- `rightsizer.skypilot.Policy` is an **in-process** `AdminPolicy` subclass.
  Its whole body runs inside the S1 guard: any error, or the decision budget
  (`[skypilot] decide_budget_ms = 1000`) expiring, returns the request
  unchanged. It talks to the decision engine in-process (`rightsizer` wheel)
  or to a local daemon (`rsz serve`) with that deadline.
- **Import is outside the guard.** SkyPilot imports the policy module in
  `_get_policy_impl` (`admin_policy_utils.py:71-79`) before the exception
  wrapper runs, so an `ImportError` there blocks every launch. The module
  `rightsizer.skypilot` therefore imports only the standard library at top
  level; `rightsizer._core` and everything else are imported lazily inside the
  guard.
- Pointing SkyPilot's `admin_policy` at a rightsizer URL directly is
  **unsupported**; it would make rightsizer's availability a launch
  dependency.
- The policy never raises `UserRequestRejectedByPolicy` itself. Rightsizer
  does not reject requests.

**Client-side policies.** `UserRequest.at_client_side = True` means the
policy is running in the user's own CLI, with `user = None`. Rightsizer then
runs in **recommend** mode only: there is no authenticated tenant, and
enforcement belongs on the server.

### 1.2 Units and request model

- One unit per `sky.Task` in the DAG; `num_nodes > 1` → distributed: GPU count
  and node count held.
- `sky.Resources` declares `cpus`, `memory` (GB) and `accelerators`
  (`"H100:8"`). A number is an exact ask; `"N+"` is a minimum
  (`sky/resources.py:868-930`). SkyPilot's optimizer then picks the cheapest
  instance, on any enabled cloud or Kubernetes context, that satisfies the
  ask. So the decision is a **lower bound handed to an optimizer**:
  `RequestModel::Snap`, where the snap function is SkyPilot's own feasibility
  check over its catalogs.
- **Rendering preserves the form.** A `"N+"` prior is rendered as `"M+"`;
  an exact prior as an exact value that exists in the enabled catalogs; an
  unset `memory` gets a minimum only if `enforce_without_prior` is set
  (SPEC §7.4).
- **Candidate lists are held.** `any_of` and `ordered` resource lists express
  the user's preferences across clouds and GPUs; v1 does not reorder or
  rewrite them (recommend only, one recommendation per entry).
- `accelerators` changes are GPU type changes (M4 opt-in). Without the opt-in,
  the policy sizes `cpus` and `memory` only, which matters on Kubernetes and
  Slurm backends, and on clouds when CPU and memory drive the instance choice.
- `validate` re-runs `sky.Resources` validation on the mutated object and
  checks that at least one enabled cloud's catalog still has a feasible
  instance (`sky.catalog` queries) — a mutation that leaves no feasible
  instance would turn a launch into a failure (S2).

### 1.3 Platform model

```
PlatformRef { kind: "skypilot",
              memory_enforced: per backend (VM: true, instance-bounded; Kubernetes: per kubernetes.md; Slurm: per slurm.md),
              retry: { same_size: job_recovery.max_restarts_on_errors (managed jobs; restarts reuse the resources), ladder: None },
              walltime_s: None, cpu_window_s: 60 }
PlatformState { request_model: Snap(SkyPilot catalogs), price: Instance(catalog price of the cheapest feasible instance), quota: None }
```

The backend is not known before the optimizer runs, so `memory_enforced` is
the **minimum** over the backends the task may land on: if any enabled
backend leaves memory unenforced, reductions need S3(b).

### 1.4 Interaction with other integrations

- On a Kubernetes backend, SkyPilot creates pods that the rightsizer webhook
  also sees. The policy sets the `rightsizer.io/decision-id` annotation through
  `kubernetes.pod_config.metadata.annotations` in the mutated
  `skypilot_config`, so the webhook skips them (kubernetes.md §3.1). This
  works on the `kubernetes` and `ssh` clouds only
  (`sky/provision/kubernetes/utils.py:4934-5053`), which are the ones whose
  pods the webhook sees. SkyPilot
  requires every task of a DAG to end with the same config
  (`admin_policy_utils.py:306-317`), so the value is one id per request, and
  the per-task decision ids are kept in the decision log.
- On a Slurm backend, the policy's decision is final. SkyPilot jobs carry no
  rightsizer token, so `job_submit.lua` would treat them as bypassers
  (slurm.md §4) and the M4 server-side table could size them a second time,
  on top of the policy's output. The Slurm integration therefore skips
  submissions from the users listed in `[slurm] presized_submitters`, where
  the SkyPilot API server's submit user is registered.

### 1.5 Keys, tenant, outcomes

- **Suffix:** the task `name` when set; else a digest of `setup` + `run`
  (normalized like code digests, SPEC §5.2). Task names are optional, so the
  digest is common.
- **Code:** `run` and `setup` text, and the workdir's entry script when the
  `run` command names one.
- **Tenant:** `sky://{workspace}/{user}` from `UserRequest.user` and the active
  workspace, server side only.
- **Outcomes:** SkyPilot has job status but no resource usage (verify M6).
  Usage comes from the backend: the Kubernetes watcher, `sacct`, or
  in-workload self-report (`python -m rightsizer.run -- CMD` in `run:`, which
  the user adds; the policy never rewrites `run`). Status mapping:
  `FAILED` with exit 137 → `Failed`, upgraded to `OomKilled` when the backend
  record says so; `FAILED_SETUP` is not an OOM of the job.
- **VM backends classify nothing on their own.** Without the Kubernetes
  watcher, `sacct`, or in-workload self-report, a failed reduced attempt is
  never classified, so under SPEC §2.3 S5 the policy enforces reductions only
  for tasks whose candidate backends all have an outcome path; elsewhere
  reductions are recommend-only and increases are enforced.

### 1.6 Ladders

None in v1. Recovery runs the policy again (§1.1), but the marker keeps the
original decision, so a recovered job restarts at the same resources. That
re-run is the natural hook for a later `Orchestrator` ladder: on a
`JOBS_LAUNCH_CLUSTER` request for a marked task whose previous attempt was
reduced, step up as escalate-on-retry does (SPEC §7.2). It needs the
attempt record keyed by the managed job id and SkyPilot's
`max_restarts_on_errors` as `max_attempts`; both are M6 verification items.

## 2. dstack

### 2.1 Hook

`ApplyPolicy.on_run_apply(user, project, spec) -> RunSpec`
(`dstack/plugins/_base.py:8-60`), returned from a `Plugin` registered through
the `dstack.plugins` entry point and enabled in the server config
(`server/services/plugins.py:62-100`).

- It is called **twice** — for the plan and for the apply — each time with
  the original spec. The **estimate** is memoized by spec digest for 10
  minutes, so the plan the user saw is what is applied; the mode, breakers
  and `mode_ceiling` are re-evaluated on apply (SPEC §6.3), so a breaker that
  opened in between still demotes the decision.
- `ValueError` rejects the run; any other exception propagates
  (`server/services/plugins.py`, `apply_plugin_policies`). The rightsizer
  plugin therefore runs inside the S1 guard and never raises, and its module
  imports lazily as the SkyPilot policy does.

### 2.2 Units and request model

- One unit per run (task or service replica group); `nodes > 1` → distributed.
- `resources:` uses ranges (`cpu: 4..`, `memory: 16GB..64GB`, `gpu: H100:8`,
  `disk: 100GB..`; `core/models/resources.py:391`). dstack picks the cheapest
  offer within the ranges. Rightsizer writes the range's **minimum** and keeps
  the user's maximum; if the decision exceeds the maximum, it is clamped (S4).
- `RequestModel::Snap` over dstack's offers; GPU type is M4 opt-in.

### 2.3 Outcomes (native)

dstack is the one launcher with native per-job usage:

- `JobMetricsPoint`: `cpu_usage_micro`, `memory_usage_bytes`,
  `memory_working_set_bytes`, `gpus_memory_usage_bytes`
  (`server/models.py:1224-1242`), collected every 10 s
  (`background/scheduled_tasks/__init__.py:53`) and deleted after a TTL
  (`DSTACK_SERVER_METRICS_RUNNING_TTL_SECONDS`, default 3600 s). The importer
  polls every 15 minutes, well within the TTL. Peaks use the working set,
  which excludes reclaimable page cache.
- `exit_status` and `termination_reason` (`core/models/runs.py:445-455`). A
  kill appears as −9 (the shell replaced itself), 137, or −1 (older runners);
  all three map to a SIGKILL, classified as `OomKilled` only with an OOM
  termination reason or the working set at the memory limit.

Records are `Exact` with `resolution_s = 10` (coarse under SPEC §2.3), per
job, `Verified`.

### 2.4 Platform model, keys, tenant

```
PlatformRef { kind: "dstack", memory_enforced: per backend (as SkyPilot),
              retry: { same_size: 0, ladder: None } }   # dstack's retry is bounded by duration, not count; counted as 0 for the cost model
```

- **Suffix:** run name with any generated suffix stripped; else a digest of
  the configuration's `commands`.
- **Tenant:** `dstack://{project}/{user}`, from the server's authenticated
  `user` and `project` arguments.

## 3. Milestone (M6) and tests

- SkyPilot: a policy that throws, hangs, or cannot import `_core` leaves every
  launch unchanged; a policy run at client side never enforces; a managed job
  is not sized twice (decision id already present → skip).
- dstack: plan and apply see identical decisions; an exception never rejects
  a run.
- Both: rendered specs pass the platform's own validation and still have at
  least one feasible offer.
