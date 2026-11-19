# rightsizer for workflow orchestrators: Metaflow, Airflow, and others

Status: **draft v3**. Core concepts: [SPEC.md](SPEC.md). Plugin contract and
integration tiers: [plugins.md](plugins.md). References: `Netflix/metaflow`
`metaflow/` and `apache/airflow` (`airflow-core/`, `providers/cncf/kubernetes/`)
at their default branches on 2026-11-18. Claims marked **(verify M6)** are
conformance-test items.

Most orchestrators eventually create Kubernetes pods, so the webhook
([kubernetes.md](kubernetes.md)) already sizes them. A native integration adds
three things the webhook cannot get:

1. **Keys** from the orchestrator's own identity (`flow/step`,
   `dag_id/task_id`) instead of pod labels.
2. **Backends the webhook never sees**: Metaflow on AWS Batch, Airflow
   operators that call SageMaker or Batch.
3. **Attempt numbers**, which make orchestrator retries into an escalation
   ladder (SPEC §7.2, escalate-on-retry).

Flyte, the deepest orchestrator integration, has its own document
([flyte.md](flyte.md)).

## 1. Shared mechanism: the attempt context

Orchestrator integrations publish an `AttemptContext` (SPEC §7.2) **in the
process that runs the task's code and submits its work** — the Airflow
worker's task process, the Metaflow local runtime — as a
`contextvars.ContextVar`, mirrored into the `RIGHTSIZER_ATTEMPT` environment
variable so subprocesses inherit it (Metaflow submits AWS Batch jobs from a
`flow.py batch step` subprocess):

```python
rs.attempt_context.set(rs.AttemptContext(
    invocation_id="dag_id/run_id/task_id/map_index",   # stable across attempts of one task instance
    attempt=2, max_attempts=4))                        # max_attempts=None when unknown
```

Every submit hook that runs there — the botocore hook (aws.md §2.1), the
Modal proxy (modal.md §2) — reads it and applies escalate-on-retry as SPEC
§7.2 defines it: before a reduced attempt, a synchronous attempt record;
on attempt k ≥ 2, that record or else the origin prior; step
`min(k − 1, max_steps)` only if attempt 1 was reduced. A retry after a reduced
attempt always moves up, so no outcome lookup is on the critical path.

S3(a) holds for a unit only when `max_attempts` is known and
`max_attempts − 1 ≥ max_steps`. Members of a group (Airflow map indexes,
Metaflow foreach splits) each get their own attempt record, holding the
group's decision.

## 2. Metaflow (`metaflow-rightsizer`)

### 2.1 Distribution and activation

A Metaflow extension package, `metaflow-rightsizer`, providing the namespace
package `metaflow_extensions.rightsizer` (Metaflow's extension mechanism,
`metaflow/extension_support/`). It adds one step decorator, `@rightsize`.

```python
from metaflow import FlowSpec, step, resources, kubernetes, retry

class Train(FlowSpec):
    @rightsize                       # explicit, per step
    @retry(times=2)
    @kubernetes(memory=65536, cpu=8, gpu=1)
    @step
    def train(self): ...
```

Org-wide with no code change: add `rsz` to `METAFLOW_DEFAULT_DECOSPECS`
(`metaflow_config.py:350`) in the Metaflow config the platform team
distributes. Setting that variable **replaces** the decospecs that installed
extensions toggle on (`metaflow_config.py:765-767`), so the value must keep
them: `METAFLOW_DEFAULT_DECOSPECS="<existing toggles> rightsize"`. It is
applied for `run`/`resume` in `cli_components/run_cmds.py:128-133` and for
deploy commands at `cli.py:636`; steps that already carry `@rightsize` are
not decorated twice (`decorators.py:771-775`).

### 2.2 How resources are resolved, and where rightsizer writes

- `@resources(cpu=, memory=, gpu=, disk=, shared_memory=)` is
  backend-neutral. `@kubernetes(...)` and `@batch(...)` take the same keys.
  `@batch` takes the **maximum** of its own value and `@resources`
  (`compute_resource_attributes`, `plugins/aws/aws_utils.py:126-171`, called
  from `batch_decorator.py:227`). `@kubernetes` has its own merge loop
  (`kubernetes_decorator.py:366-384`): `@resources` **overrides** `gpu` and
  `shared_memory`, and the maximum applies to the rest. Writing only
  `@resources` therefore could never lower CPU or memory.
- `@rightsize` writes the decided value into **both** the `@resources`
  decorator's attributes and the compute decorator's attributes, so either
  merge yields the decision regardless of which `step_init` runs first.
- **Restoring.** The local runtime is one long-lived process, and decorator
  attributes persist across attempts. `@rightsize` snapshots the original
  attributes of both decorators at `step_init` and restores them at the start
  of every `runtime_step_cli` before deciding. On any S1 fallback (an error,
  the budget expiring, a breaker, the mode ceiling) the restored originals are
  what the command line carries, so attempt 2 never inherits attempt 1's
  reduced values.
- **One layer decides.** `@rightsize` also sets `RIGHTSIZER_DECIDED=<id>` and
  `RIGHTSIZER_ATTEMPT` in `cli_args.env` (`runtime.py:2177`), the environment
  of the `kubernetes step` / `batch step` subprocess. The botocore hook in that
  subprocess (for example under an inherited `RIGHTSIZER_AWS_INSTALL`) then
  adds only the marker to `SubmitJob` and never re-sizes it (aws.md §2.1).
- **The pod or job carries the marker.** For `@kubernetes`, `@rightsize` adds
  `rightsizer.io/decision-id` to the decorator's pod annotations, so the
  webhook skips the pod; for `@batch`, the botocore hook writes
  `RIGHTSIZER_DECISION` as above (the `@kubernetes` annotations attribute:
  verify M6; fallback: `RIGHTSIZER_DECISION` through the step's environment).
- Per attempt, `runtime_step_cli(cli_args, retry_count, max_user_code_retries,
  ubf_context)` (`decorators.py:493`) runs for every decorator of the task, in
  the step's decorator order (`runtime.py:2318-2327`). The compute decorator
  copies its `attributes` into `cli_args.command_options`
  (`kubernetes_decorator.py:474-500`). Because the order is user-controlled,
  `@rightsize` updates the compute decorator's `attributes` **and**
  `cli_args.command_options` for `cpu`, `memory` and `gpu`. Whichever runs
  last, the command line carries the decision.

### 2.3 When decisions happen

| how the flow runs | decision point | granularity | ladder |
|---|---|---|---|
| `python flow.py run` (local runtime), with `--with kubernetes`, `--with batch`, or decorators | `runtime_step_cli`, per task attempt | per task (each `foreach` split is a task) | escalate-on-retry from `retry_count` (§1); S3(a) when `@retry(times ≥ max_steps)` |
| `argo-workflows create`, `step-functions create`, `airflow create` | `step_init` during the deploy command; resources are baked into the template (`argo_workflows.py:2502`) | per deploy | none; deploy-time rules: increases only unless `rsz metaflow reconcile` is scheduled (as flyte.md §6.3) |
| Argo-deployed flows on Kubernetes, at run time | the Kubernetes webhook, keyed from Metaflow's pod annotations (`metaflow/flow_name`, `metaflow/step_name`, `metaflow/attempt`: `kubernetes.py:673-680` for jobs, `:384-390` for JobSets, `argo_workflows.py:2929-3009` for Argo, where `attempt` is `"{{retries}}"`) | per pod | webhook rules (kubernetes.md) |

`@rightsize` running at deploy time stamps the template with the
`rightsizer.io/decision-id` annotation and the origin prior, so the webhook
skips those pods. The deploy runs with the user's credentials and has no
signing key, so this copy is informational; the authoritative origin prior is
the deploy decision's record in the store, as for Flyte (flyte.md §6.3). Flows deployed **without** `@rightsize` are sized
per run by the webhook like any other pod.

### 2.4 Context

- **Key suffix:** `{flow}/{step}`, plus `project` and `branch` when
  `@project` is used. Tenant: §2.6.
- **Code:** the step function's source.
- **Inputs:** flow `Parameters` (scalars and `IncludeFile` sizes), and
  upstream artifact sizes from the task datastore metadata when it records them
  (verify M6). Artifacts are never loaded.
- **Prior:** the merged compute-decorator values. `@kubernetes` writes its
  defaults straight into its attributes (`kubernetes_decorator.py:145-148`),
  so a user who wrote `memory=4096` looks the same as one who wrote nothing.
  Values equal to Metaflow's defaults (`cpu=1`, `memory=4096`) are treated as
  `Prior.defaulted`: they may be raised freely, and reductions below them are
  still governed by S3.

### 2.5 Platform model

| backend | `memory_enforced` | notes |
|---|---|---|
| `@kubernetes`, `qos="Burstable"` (default) | **false**: memory is a request with no limit (`kube_utils.py:35-61`) | reductions need S3(b) |
| `@kubernetes(qos="Guaranteed")` | true | limits = requests; preserved |
| `@batch` | true: Batch `MEMORY` is a hard limit | with `use_tmpfs=True` and no `tmpfs_size`, tmpfs defaults to half of memory (`batch_client.py:367`); memory reductions are held in that case |

`PlatformState` comes from the backend: the Kubernetes model (kubernetes.md
§5) or the Batch model (aws.md §4.3).

### 2.6 Outcomes, tenant

- **Outcomes:** OSS Metaflow records attempts and statuses but no resource
  usage (verify M6). `@rightsize` measures in the task (`task_pre_step` starts
  the sampler, `task_finished` writes the record: RSS and NVML every 0.5 s,
  `Exact`), and the backend importer (Kubernetes watcher, Batch `DescribeJobs`)
  supplies status and OOM classification. Records join on
  `(flow, run_id, step, task_id, attempt)`.
- **Tenant:** `metaflow://{namespace}`, where the namespace comes from the
  Metaflow deployment config, not from `metaflow/user` (which the user sets).
  Records are `Authenticated` when written with store credentials scoped to
  that prefix.

## 3. Airflow (`rightsizer.airflow`)

### 3.1 Activation

Cluster policies in `airflow_local_settings.py`, installed by the platform
team on the scheduler and workers:

```python
from rightsizer.airflow import pod_mutation_hook   # noqa: F401
```

The worker-side listener (§3.2) is registered as an Airflow plugin: the
`rightsizer` wheel declares an `airflow.plugins` entry point whose plugin
lists `rightsizer.airflow.listener` in `listeners`, enabled by installing the
wheel on the workers.

### 3.2 Hooks

All in `airflow-core/src/airflow/policies.py`:

| hook | runs | can change | used for |
|---|---|---|---|
| `pod_mutation_hook(pod)` (`:96`) | where the pod is built: the worker running a `KubernetesPodOperator` (`providers/cncf/kubernetes/.../operators/pod.py:1784`), the scheduler's KubernetesExecutor (`pod_generator.py:455-458`) | the final `V1Pod` | **enforce**: one decision per pod, per attempt |
| `task_instance_mutation_hook(ti, dag_run)` (`:71`) | scheduler, at task-instance creation and on retry, inside a transaction that must not commit | the task instance | not used: it runs in the scheduler, so nothing it sets reaches the worker that submits work |
| `task_policy(task)` (`:35`) | DAG parse | the operator | not used: per-parse, not per-run, and runs on every parse |

`pod_mutation_hook` is fail-closed — an exception fails the pod launch — so
the rightsizer hook runs inside the S1 guard with a decision budget
(`[airflow] decide_budget_ms = 500`).

**Publishing the attempt context on the worker.** A listener plugin
(`rightsizer.airflow.listener`, registered through Airflow's listener
mechanism) implements `on_task_instance_running` and sets the
`AttemptContext` from `ti.try_number` and `ti.max_tries` in the task's own
process, where operators run (worker-side delivery under the Airflow 3 task
SDK: verify M6). The KPO path does not need it: KPO calls
`pod_mutation_hook` inside `execute()`, in the task process, where the same
context is set.

### 3.3 Units, keys, attempts

- One unit per pod, keyed `{dag_id}/{task_id}`, plus `map_index` as a group
  member for dynamically mapped tasks (all map indexes of one task share a
  decision per run). Identity comes from the pod labels: KubernetesExecutor
  sets `dag_id`, `task_id`, `run_id`, `try_number` and `map_index`
  (`pod_generator.py:517-531`); KPO's labels come from `_get_ti_pod_labels`
  (`operators/pod.py:607-645`), with `try_number` only when
  `include_try_number` is set, so KPO reads the attempt from the context
  instead.
- **Attempts:**
  - KPO and non-pod operators: the attempt context (§3.2) gives
    escalate-on-retry with `max_attempts = max_tries + 1`. S3(a) holds when
    `retries ≥ max_steps`.
  - KubernetesExecutor pods: the hook runs in the scheduler, where
    `try_number` is in the labels but `max_tries` is not, so `max_attempts`
    is unknown and there is no ladder (SPEC §7.2): reductions need S3(b).
- **Inputs:** `dag_run.conf` scalars and URIs (manually triggered and
  API-triggered runs), templated operator arguments after rendering when the
  hook can read them from the pod's arguments (URIs only). Otherwise `[]`.
- **Code:** for `KubernetesPodOperator`, the container's command and the image;
  for KubernetesExecutor, the task's Python callable source when importable on
  the scheduler (`None` otherwise).

### 3.4 Operators that are not pods

`SageMakerTrainingOperator`, `BatchOperator` and other AWS operators call
boto3 from the worker. With `rs.aws.install()` in `airflow_local_settings.py`,
the botocore hook (aws.md §2.1) sizes their requests, and the Airflow
integration publishes the attempt context from the running task instance, so
these operators get escalate-on-retry too, through the listener's context. Celery and local executors give
tasks no per-task resources of their own; only their outbound submissions
are sized.

### 3.5 Tenant, outcomes, double sizing

- **Tenant:** `airflow://{deployment}`, from configuration. DAG authors in one
  deployment can run arbitrary code on the same workers, so they share a
  tenant; per-team tenants need per-team deployments or per-team Kubernetes
  namespaces (then the namespace is the tenant, as kubernetes.md §2).
- **Outcomes:** none native. The Kubernetes watcher and Prometheus importer
  (kubernetes.md §6) record peaks and OOMs, joined on the pod labels.
- **Double sizing:** the hook sets the `rightsizer.io/decision-id` annotation, so the webhook
  skips the pod.

## 4. Others

| orchestrator | v1 coverage | native integration (later) |
|---|---|---|
| **Kubeflow Pipelines v2** | Kubernetes webhook (pods via Argo) | `set_memory_limit`, `set_cpu_request`, `set_accelerator_type` and friends (not `set_gpu_limit`) accept a `PipelineChannel` (`kfp/dsl/pipeline_task.py:448-730`), so a `rsz` component can compute resources at run time and feed downstream tasks. On Vertex AI Pipelines, see [managed-ml.md](managed-ml.md) §1.2. M7. |
| **Dagster** | Kubernetes webhook; step pods carry `dagster/job` and `dagster/op` labels, the latter holding the step key (`dagster_k8s/executor.py:338-342`) | a `K8sStepHandler` subclass overriding `_get_container_context` (`executor.py:254-277`, private API) for per-step enforcement with asset and partition keys. M7. |
| **Prefect** | Kubernetes webhook; resources are per flow run, not per task | `run_deployment(..., job_variables=)` wrapper (`workers/base.py:300-311` merges deployment then flow-run variables). M7. |
| **Argo Workflows** | Kubernetes webhook | template-level keys from workflow labels; nothing beyond the webhook planned. |
| **ZenML** | via the backend it runs on | `ResourceSettings(cpu_count=, gpu_count=, memory=)` through an orchestrator flavor wrapper. Not planned. |

Kedro, MLflow Projects and Hamilton declare no per-step resources and are out
of scope.

## 5. Milestone (M6) and tests

- Metaflow: `@rightsize` before and after `@kubernetes` in decorator order
  produces the same command line; with `@resources(memory=8192)` and
  `@kubernetes(memory=65536)`, a decision of 16 GiB yields `--memory 16384`;
  `retry_count = 1` after a reduced attempt escalates.
- Metaflow: an S1 fallback on attempt 2 after a reduced attempt 1 produces the
  original command line, not attempt 1's.
- Airflow: a `pod_mutation_hook` that raises or times out leaves the pod
  unchanged; `try_number = 2` escalates on KPO; KubernetesExecutor pods never
  claim a ladder; mapped tasks share a decision.
- Both: pods carry the decision id and the webhook skips them.
