# rightsizer for managed ML job services: Vertex AI, Azure ML, Databricks, and others

Status: **draft v3**, plugin depth (milestone M7). Core concepts:
[SPEC.md](SPEC.md). Plugin contract and integration tiers:
[plugins.md](plugins.md). The AWS services have their own document
([aws.md](aws.md)), and this one reuses its patterns. Claims marked
**(verify M7)** are conformance-test items.

## 0. Common shape

Every service here shares four properties with SageMaker:

1. **No mutating admission hook.** Google Org Policy custom constraints and
   Azure Policy can only allow or deny. Databricks cluster policies can fix or
   default values per policy, not per job. So enforcement happens in the
   **client** (an SDK wrapper), in a **compiled spec** before it is submitted
   (`rightsize apply`), or in **code review** (`rightsize suggest`).
2. **Discrete machine types** (`RequestModel::Catalog`), except where noted.
3. **No retry that changes size.** Ladders exist only as caller-owned waits or
   orchestrator retries (SPEC §7.2), as on AWS (aws.md §6). None of these
   services gets a caller helper before M7, so until then their reductions
   need S3(b).
4. **Minute-resolution metrics.** Peaks come from 60 s samples, so S3(b) uses
   the coarse-resolution rule (SPEC §2.3): headroom 1.5 and `2 × min_history`
   runs. Self-report (`python -m rightsizer.run -- CMD`) is the fast path.

Every service gets tier A (`rightsize audit <service>`) first.

## 1. Google Vertex AI

### 1.1 Custom jobs

- **Resource model:** `CustomJob.job_spec.worker_pool_specs[i].machine_spec`:
  `machine_type`, `accelerator_type`, `accelerator_count`,
  `gpu_partition_size` (MIG; then `accelerator_count` must be 1),
  `tpu_topology` (`google/cloud/aiplatform/v1/machine_resources.proto`,
  `custom_job.proto`). One unit per worker pool. Pools other than pool 0, or
  `replica_count > 1`, are distributed: count held, type held in v1.
- **Enforce point:** `rs.gcp.vertex.sized(job)` wraps an
  `aiplatform.CustomJob` / `CustomTrainingJob` / `CustomContainerTrainingJob`
  and edits `worker_pool_specs` before `.run()`/`.submit()`. A gRPC client
  interceptor on `JobServiceClient.create_custom_job`, which would cover raw
  GAPIC callers too, is the preferred later mechanism (verify M7).
- **Outcomes:** Cloud Monitoring, monitored resource `cloudml_job`, metrics
  `ml.googleapis.com/training/memory/utilization`, `cpu/utilization` (GA),
  `accelerator/utilization`, `accelerator/memory/utilization` (beta), labelled
  by `task_type`, `task_index` and `trial_id`; sampled every 60 s, up to 360 s
  before visible. Utilization is a fraction of the replica's **allocated**
  memory, converted with the catalog's usable memory for the machine type.
  Whether a point is a sample or a period average is not documented, so peaks
  are `AtLeast` until M7 verifies it (SPEC §5.7).
- **OOM signal:** the job's error message with exit status 137 or
  "out-of-memory" (verify M7 for exact wording) → `OomKilled`; otherwise
  `Failed`.
- **Retry:** `scheduling.restart_job_on_worker_restart` restarts at the same
  spec; `strategy` ∈ {STANDARD, SPOT, FLEX_START} selects the price model.
- **Pricing:** Cloud Billing Catalog API, Vertex AI training SKUs (which
  include a premium over Compute Engine) plus GPU SKUs.
- **Tenant:** `gcp://{project}/{principal}` from the caller's credentials;
  importer records use audit-log `principalEmail`.

### 1.2 Vertex AI Pipelines (KFP v2)

- `set_cpu_limit`, `set_memory_limit`, `set_accelerator_type` and
  `set_accelerator_limit` are **sizing hints**: Vertex picks the closest
  supported machine type (up to 96 CPUs and 624 GB; default `e2-standard-4`),
  and `set_memory_request` is not supported (Vertex "machine types" page).
  `RequestModel::Snap`, where the snap function is Vertex's machine matching.
- That matching algorithm is not documented. Until M7 derives it from observed
  `machine_type`s in pipeline job details, KFP-on-Vertex is **recommend only**.
- Enforce point (M7): rewrite the compiled pipeline IR YAML
  (`deploymentSpec.executors.*.container.resources`) with `rightsize apply`
  before `PipelineJob.submit()`; deploy-time rules (increases only without
  `reconcile`).

### 1.3 Vertex batch prediction

`dedicated_resources.machine_spec` plus replica counts. No per-job memory
metric is documented: tier A and R only.

## 2. Google Cloud Batch and Cloud Run jobs

- **Cloud Batch:** `taskSpec.computeResource.{cpuMilli, memoryMib}` per task
  (free values), packed onto `allocationPolicy.instances[].policy.machineType`
  with `accelerators`. `validate` checks that tasks fit the machine type.
  `lifecyclePolicies` with `FAIL_TASK` on exit 137 stops same-size retries
  from wasting attempts. Outcomes per VM via the Ops Agent (not per task):
  tier A/R; enforce in M7+ through job JSON rewrite.
- **Cloud Run jobs:** `resources.limits.{cpu, memory}`, one GPU per instance
  with minimums (`nvidia-l4`: 4 CPU / 16 GiB; `nvidia-rtx-pro-6000`: 20 CPU /
  80 GiB), and a regional GPU quota (deployments fail when parallelism exceeds
  it). `validate` encodes the minimums and the quota. Low priority for AI.

## 3. Azure Machine Learning

- **Resource model:**
  - command and pipeline jobs: `compute` (a cluster, or serverless) plus
    `resources.instance_type` (a VM size such as `Standard_NC24ads_A100_v4`)
    and `instance_count`;
  - **Kubernetes compute:** `instance_type` names an `InstanceType` custom
    resource (`amlarc.azureml.com/v1alpha1`) with requests and limits for CPU
    and memory and a GPU limit. Jobs can only pick a named instance type, so
    the catalog is the cluster's `InstanceType` objects
    (`RequestModel::Catalog` over admin-defined shapes). These have real
    limits, so memory is enforced;
  - **serverless:** Azure picks a VM "based on quota, cost, performance" when
    none is given; `queue_settings.job_tier` is Standard or Spot.
- **Enforce point:** `rs.azureml.sized(ml_client)` proxies
  `ml_client.jobs.create_or_update(job)` and edits `job.resources` (and each
  pipeline step's `resources`); `rightsize apply` rewrites job YAML
  (`commandJob.schema.json`). An azure-core per-call pipeline policy that
  would cover every client is a later option (verify M7).
- **Outcomes:** Azure Monitor workspace metrics at PT1M with a `RunId`
  dimension: `CpuMemoryUtilizationMegabytes`, `CpuMemoryCapacityMegabytes`,
  `CpuUtilizationMillicores`, `GpuMemoryUtilizationMegabytes` (per
  `DeviceId`), `GpuUtilizationPercentage`; the `Maximum` aggregation gives a
  per-run peak in **megabytes**, per GPU device — the best of the managed
  services, so `gpu_memory_peak` is the busiest device's maximum. CPU memory
  is measured per node and includes the node's non-job processes, so it is an
  upper bound on the job's own use. Until M7 verifies that points are samples
  rather than averages, a CPU-memory value is both possibly low and possibly
  high, and the record carries no memory peak (SPEC §5.7 conflicting bounds);
  GPU memory, measured per device, is `AtLeast` until then.
- **OOM signal:** exit 137 / `Killed` in `std_log`; on Kubernetes compute, pod
  `OOMKilled` (verify M7 for AML error codes).
- **Retry:** command jobs have no retry on failure; parallel jobs have
  `retry_settings.max_retries` (same size).
- **Pricing:** Azure Retail Prices API (`prices.azure.com/api/retail/prices`);
  AML compute bills at VM meter rates.
- **Tenant:** `azure://{subscription}/{workspace}/{principal}`; importer
  records use the job's `systemData.createdBy`.

## 4. Databricks

- **Scope in v1: single-node and GPU job clusters.** A multi-node Spark job's
  memory is spread over JVM executors and governed by Spark configuration
  (`spark.executor.memory`, partitioning), which rightsizer's per-process
  demand model does not describe. Multi-node Spark clusters get advisories
  only (e.g. "mean `mem_used_percent` below 30% over the last 5 runs").
- **Resource model:** `new_cluster.node_type_id`, `driver_node_type_id`,
  `num_workers` / `autoscale` (classic job compute,
  `job_clusters[].job_cluster_key`); serverless jobs have no resource knobs;
  serverless GPU compute ("AI Runtime") selects an accelerator
  (`hardware_accelerator`, e.g. `GPU_8xH100`; enum values verify M7).
- **Enforce point:** `rightsize apply` / `rightsize suggest` on bundle YAML
  (`databricks.yml`: `resources.jobs.*.job_clusters`,
  `tasks[].new_cluster`) and on Jobs API JSON. Both are deploy-time: increases
  only, unless a `reconcile` job is scheduled (as flyte.md §6.3), and `suggest`
  follows SPEC §12.4. `validate` evaluates the
  cluster's **cluster policy** (allow-lists, ranges, fixed values): a node
  type the policy forbids would make the job fail to start.
- **Outcomes:** `system.compute.node_timeline`: one row per node per minute
  with `mem_used_percent` (includes background processes),
  `cpu_user_percent`, `node_type`, `driver`; `system.compute.node_types` gives
  `memory_mb`, `core_count`, `gpu_count` to convert. `mem_used_percent`
  includes background processes and its sample kind is undocumented, so these
  rows give no memory peak (SPEC §5.7 conflicting bounds): Databricks memory
  decisions rest on in-workload self-report, and the system tables feed
  advisories and CPU. No GPU columns;
  serverless compute is not covered. Join runs to clusters through the
  `system.lakeflow` job tables (verify M7).
- **OOM signals:** driver-restart and `DRIVER_OUT_OF_MEMORY` termination
  codes (verify M7).
- **Retry:** task `max_retries` reruns on the same compute. A repair run after
  editing the job cluster is a possible caller ladder (verify M7 that repair
  runs pick up the edit).
- **Pricing:** `system.billing.list_prices` (DBU) plus cloud VM prices. The
  DBU rate per node type is not in system tables (verify M7); until it is, the
  cost model uses VM prices only and says so.
- **Tenant:** `databricks://{workspace}/{run_as}`.

## 5. Snowflake (Snowpark Container Services, ML Jobs)

Compute pools have an `INSTANCE_FAMILY` (`CPU_X64_S`, `GPU_NV_S`, …); a
service spec declares `containers.resources` with hard memory and GPU
requirements, and a service runs on one node. Tier A/R: recommend a family
and a memory request from the event table's container metrics (verify M7).
No enforcement planned.

## 6. Kubernetes-based managed services

GKE (including Autopilot), EKS, AKS, OpenShift, CoreWeave, and HyperPod on EKS
are Kubernetes. They use the webhook. **GKE Autopilot** silently raises
requests below its minimums and rejects requests above its maximums, enforces
CPU:memory ratios per compute class, and sets limits equal to requests on
non-bursting clusters. Those rules are part of the webhook's `validate`
(kubernetes.md §3.4); without them a reduction could be undone silently or
rejected.
