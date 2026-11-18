# rightsizer for AWS (`rightsizer.aws`): SageMaker AI and AWS Batch

Status: **draft v3**. Core concepts: [SPEC.md](SPEC.md). Plugin contract and
integration tiers: [plugins.md](plugins.md). SDK references are to
`aws/sagemaker-python-sdk` at commit `7263f35` (v3.30.0). AWS API references
are to the service API references as of 2026-11-18. Claims marked
**(verify M5)** come from community reports or untested mechanisms and are
conformance-test items, not design assumptions.

HyperPod is not a separate integration. HyperPod on EKS uses the Kubernetes
webhook ([kubernetes.md](kubernetes.md)); HyperPod on Slurm uses the Slurm
plugin ([slurm.md](slurm.md)).

## 0. Day one

```bash
rightsize audit aws --since 30d                  # tier A: read job history + CloudWatch, print waste and advisories; changes nothing
rightsize aws run -- python launch_training.py   # tier R: same program, every CreateTrainingJob/SubmitJob gets a recommendation on stderr
```

```python
import rightsizer as rs
rs.aws.install()                                  # tier R/E: botocore hook for SageMaker and Batch in this process (mode from policy)
```

## 1. Why AWS needs its own design

- **No server-side mutation hook.** Neither SageMaker nor Batch has an
  admission hook. IAM and SCP conditions can only deny: `sagemaker:InstanceTypes`
  is a condition key on about two dozen actions, including `CreateTrainingJob`,
  `CreateProcessingJob`, `CreateTransformJob`,
  `CreateHyperParameterTuningJob`, `CreateCluster` and `UpdateTrainingJob`.
  So the
  **enforce point is the client**, and coverage is limited to processes that
  load the hook. IAM allow-lists are the backstop for everyone else.
- **SageMaker sizes by instance type.** `ResourceConfig` has `InstanceType`,
  `InstanceCount`, `VolumeSizeInGB`, `KeepAlivePeriodInSeconds`,
  `InstanceGroups` and `InstancePreferences`, but no memory or CPU field. The
  job's container gets the instance. Every decision is a **shape choice**
  (SPEC §5.8, `RequestModel::Catalog`).
- **Batch sizes by quantity.** `resourceRequirements` takes `VCPU`, `MEMORY`
  (MiB) and `GPU`, and `SubmitJob.containerOverrides.resourceRequirements`
  overrides the job definition per submission. On EC2 and Fargate, `MEMORY` is
  a hard limit. Fargate only accepts fixed vCPU/memory combinations
  (`RequestModel::Combos`).
- **No platform retry changes size.** SageMaker `RetryStrategy` retries only
  `InternalServerError`, at the same size. SageMaker Pipelines retry policies
  cover service faults, throttling, internal errors and capacity, not algorithm
  errors. Batch `retryStrategy`/`evaluateOnExit` reruns at the same size. So a
  ladder exists only where a rightsizer component or an orchestrator
  resubmits (§6).

## 2. Surfaces

| surface | tier (plugins.md §1.1) | coverage |
|---|---|---|
| `rightsize audit aws` | A | any job, however it was submitted |
| `rs.aws.install()` / `rightsize aws run -- CMD` | R, E | any boto3 client in that Python process: SageMaker SDK v2 `Estimator`/`Processor`/`Transformer`, v3 `ModelTrainer`, raw boto3, Pipelines `CreatePipeline`/`UpdatePipeline`, Batch `SubmitJob`, and orchestrators that call boto3 (Airflow AWS provider, Metaflow `@batch`) |
| `rs.aws.sagemaker.run(...)`, `rightsize aws batch submit --wait` | R, E, L (caller-owned ladder) | jobs submitted through these helpers |
| `rightsize suggest` on CDK/CloudFormation/Terraform/Pipelines JSON | R (as a pull request) | jobs defined as code |

Not covered for enforcement: the AWS console, the AWS CLI, other-language SDKs,
Step Functions service integrations (`sagemaker:createTrainingJob.sync`,
`batch:submitJob.sync`) — the state machine calls the API itself. These get
tier A, and `rightsize suggest` where the definition is in a repository.

### 2.1 The botocore hook

`rs.aws.install()` registers a handler for each operation it sizes:

```
before-parameter-build.sagemaker.CreateTrainingJob
before-parameter-build.sagemaker.CreateProcessingJob
before-parameter-build.sagemaker.CreateTransformJob
before-parameter-build.sagemaker.CreatePipeline
before-parameter-build.sagemaker.UpdatePipeline
before-parameter-build.batch.SubmitJob
before-parameter-build.batch.SubmitServiceJob
```

Hyperparameter tuning jobs (`CreateHyperParameterTuningJob`) are not sized in
v1: each trial is a training job the tuner launches server side.
`SubmitServiceJob` carries its `CreateTrainingJob` request as a JSON
**string** (`serviceRequestPayload`); the handler parses it, sizes it as §3,
and re-serializes it.

`before-parameter-build` handlers receive the request `params` dict and may
change it in place before serialization and signing (boto3 "Events" guide).
Registration:

- botocore event emitters are per session and SDKs create their own sessions
  (sagemaker v3 builds a `boto3.Session` per `Session`), so registering on
  `boto3.DEFAULT_SESSION` is not enough. `install()` wraps
  `botocore.session.Session.create_client` once (idempotent, guarded) so every
  `sagemaker` and `batch` client created afterwards registers the handlers on
  `client.meta.events`. `rs.aws.instrument(client)` does the same for one
  existing client.
- **`install()` must run before the SDK creates its clients.** sagemaker-core
  caches clients (`sagemaker-core/src/sagemaker/core/utils/utils.py:420-498`),
  and a client created earlier is never hooked. `install()` warns when that
  cache is already populated. aiobotocore (async) clients are not covered.
- `rightsize aws run -- CMD` sets `RIGHTSIZER_AWS_INSTALL=1`. The
  `rightsizer-runtime` wheel ships a `.pth` file whose one line imports
  `rightsizer.aws` and calls `install()` inside a `try`/`except` when that
  variable is set (the pattern `coverage` uses with `COVERAGE_PROCESS_START`);
  a missing `rightsizer` wheel or any error leaves the interpreter as it was.
  Without the variable, the line does nothing. The variable is inherited by
  child interpreters, so subprocesses that submit jobs are hooked too.
- **Guard.** The handler runs inside the S1 guard: any exception, or the
  decision budget (`[aws] decide_budget_ms = 500`) expiring, leaves `params`
  untouched. botocore validates parameters *after* the handler returns, so an
  error there could not be caught by the guard; the handler therefore checks
  its own edits against the operation's input shape
  (`botocore.validate.validate_parameters` with the client's service model)
  and drops them if they do not validate.
- **Marker.** Every sized request carries the decision id in the container
  environment: `Environment["RIGHTSIZER_DECISION"]` (training, processing,
  transform), `containerOverrides.environment` (Batch on EC2 and Fargate),
  `nodeOverrides…containerOverrides.environment` (multi-node), or
  `eksPropertiesOverride.podProperties.containers[].env` (EKS job queues).
  None needs an extra IAM permission. If the environment map is full, the request is left
  unchanged. Every rightsizer layer skips a request that already carries the
  marker (plugins.md §2 double-sizing rule), and a process whose orchestrator
  integration has already decided the unit sets `RIGHTSIZER_DECIDED` in the
  submitting process's environment, which makes the hook add only the marker.
- **Tags** (`rightsizer:decision-id`) are **off by default**: tagging at create
  time needs `sagemaker:AddTags` / `batch:TagResource`, and a request that
  gains tags the caller may not add would fail with `AccessDenied`.
  `[aws] tag = "auto"` enables them after `iam:SimulatePrincipalPolicy`
  confirms the permission; `UpdatePipeline` never gets tags (it has no `Tags`
  parameter). Tags are never used for tenant identity (§8).

The handler sees the API request, not the SDK object, so one code path covers
SDK v2, SDK v3 and raw boto3. `_intercept_create_request` (the v2 `Session`
subclass hook) is not used: v3 `ModelTrainer.train()` does not go through it
(`model_trainer.py:994` calls `TrainingJob.create` directly).

## 3. SageMaker AI

### 3.1 Units

| request | unit | group / distributed |
|---|---|---|
| `CreateTrainingJob` with `ResourceConfig.InstanceType` | one | `InstanceCount > 1` → distributed: type and count held |
| `CreateTrainingJob` with `ResourceConfig.InstancePreferences` | one | the preference list is held (recommend only); `InstancePreferences` excludes `InstanceType`, `InstanceGroups`, `InstancePlacementConfig` and managed spot |
| `CreateTrainingJob` with `InstanceGroups` | one per group | heterogeneous clusters: every group held (recommend only) in v1 |
| `CreateProcessingJob` `ProcessingResources.ClusterConfig` | one | `InstanceCount > 1` → distributed |
| `CreateTransformJob` `TransformResources` | one | `InstanceCount > 1` is data-parallel, not gang: type may change, count held |
| `CreatePipeline`/`UpdatePipeline` | one per Training/Processing/Transform step in `PipelineDefinition` | deploy-time sizing (§3.5) |

### 3.2 Request model and shapes

`RequestModel::Catalog` over `ml.*` instance types. The catalog row for each
type carries vCPU, memory, GPU model, GPU count and GPU memory, and the
region's price per hour for each usage type (training, processing, transform,
spot training, warm pool).

- **Source.** Prices come from the AWS Price List (`AmazonSageMaker` offer;
  usage types such as `USW2-Train:ml.g5.2xlarge`, `USW2-Processing:…`,
  `USW2-Tsform:…`, `USW2-TrSpt:…`, `USW2-TrainWarmPool:…`). Spot training rows
  exist for only a few dozen types (none for g5 in us-west-2); a type without
  one is priced at its on-demand rate × the account's observed
  `BillableTimeInSeconds / TrainingTimeInSeconds` ratio, or held when there is
  no history. Hardware specs come from EC2
  `DescribeInstanceTypes` for the matching EC2 type (`ml.g5.2xlarge` →
  `g5.2xlarge`); that mapping is checked per family by a CI job
  (verify M5) and the result is shipped as `catalog/aws-sagemaker.json`,
  refreshed weekly. `rightsize aws catalog refresh` rebuilds it locally.
- **Usable memory.** The container gets the instance minus the SageMaker agent
  and OS: the shape's `reserved` (SPEC §5.8), starting at 5% of instance
  memory and calibrated per family from successful runs. The runtime overhead
  model (SPEC §5.2) is separate.
- **What may change** (SPEC §7.4 holds, defaults):
  - the **size within the family** (`ml.m5.4xlarge` → `ml.m5.2xlarge`);
  - only among sizes with the **same GPU count**. GPU families change GPU count
    with size (in `ml.g5`: `xlarge`, `2xlarge`, `4xlarge`, `8xlarge` and
    `16xlarge` have 1 GPU; `12xlarge` and `24xlarge` have 4; `48xlarge` has 8),
    so a size change that would change the GPU count is held;
  - **families** only from `[aws.sagemaker] families = [...]`. A family change
    on a GPU type is a GPU type change and needs the M4 opt-in;
  - `VolumeSizeInGB`: held (disk has no grid in v1);
  - `InstanceCount`: held (SPEC §2.3 S2).
- **Holds specific to SageMaker:**
  - `KeepAlivePeriodInSeconds > 0` (warm pools): type held. A warm pool is only
    reused by a job that matches it on instance type and count, `RoleArn`,
    `VolumeSizeInGB`, `VolumeKmsKeyId`, `VpcConfig`, and the encryption and
    network-isolation flags, so a resize would pay a cold start on every
    change.
  - `TrainingPlanArn`: type held (the plan reserves that type).
  - `InstancePreferences`: the ordered list is a capacity fallback chosen by
    the user. Held in v1; a recommendation is printed per entry.
  - `EnableManagedSpotTraining`: the spot usage type prices the candidates.
- **Quota (S4).** Service Quotas has one quota per instance type and job kind
  (for example "ml.p4d.24xlarge for training job usage"; spot training has its
  own quotas). Service Quotas returns limits, not usage; usage comes from the
  quota's CloudWatch `AWS/Usage` metric where one exists, otherwise from
  counting the account's in-progress jobs of that type (`List*Jobs` with
  `StatusEquals=InProgress`). `PlatformState.quota` caches both for 10
  minutes. An increase onto a type without headroom is clamped.
- **Moving to another type needs both checks.** The instance type changes
  only when (a) the target type is known to be allowed — listed in
  `[aws] allowed_instance_types`, or confirmed by `iam:SimulatePrincipalPolicy`
  for the caller (which does not see SCPs or permission boundaries, so
  accounts with SCPs on `sagemaker:InstanceTypes` must use the list) — and
  (b) its quota headroom is known. Otherwise the type is held and the
  recommendation is printed. A patch onto a denied or exhausted type would
  turn a working submission into `AccessDenied` or `ResourceLimitExceeded`
  (S2).

### 3.3 Prior and rendering

- The prior is the request's instance type, read as that type's catalog row.
  `Prior.defaulted` is empty: SageMaker has no default instance type. With
  `InstancePreferences` there is no `InstanceType`; the prior is the first
  preference, and importers read the type that ran from `SelectedInstanceType`
  and `SelectedInstanceCount`.
- If the instance type is a pipeline parameter reference
  (`{"Get": "Parameters.TrainInstanceType"}`), the unit is **pinned** and gets
  an advisory only.
- `render` changes `InstanceType` (or `InstanceGroups[i].InstanceType` in
  recommend output) and adds the marker (§2.1). Nothing else changes.
- `validate`:
  - the type exists in the region and supports the job kind (from the
    catalog's usage types);
  - the allow-list and quota checks of §3.2;
  - the edited request validates against the operation's input shape (§2.1).

### 3.4 Platform model

```
PlatformRef { kind: "sagemaker", memory_enforced: true,        # the instance is the limit
              retry: { same_size: 0, ladder: per §6 },
              walltime_s: StoppingCondition.MaxRuntimeInSeconds,
              cpu_window_s: 60 }
PlatformState { request_model: Catalog(ml.* rows), price: Instance, quota: Service Quotas }
```

Memory is enforced by the instance, not a cgroup: a job above usable memory is
killed by the kernel OOM killer or fails allocation. `memory_enforced = true`
therefore holds for SageMaker training, processing and transform.

### 3.5 Pipelines (deploy-time)

`CreatePipeline`/`UpdatePipeline` carry `PipelineDefinition` as JSON. The
adapter sizes each Training, Processing and Transform step's arguments, as
Flyte deploy-time sizing does (flyte.md §6.3):

- input-independent: `scale` uses the p90 of recent input bytes (SPEC §7.4);
- **increases only**, unless `rightsize aws reconcile` is scheduled (the same
  rule as flyte.md §6.3, so deploy-time reductions cannot outlive breakers and
  the mode ceiling);
- `PipelineDefinitionS3Location` is out of scope in v1: recommend only. The
  API accepts inline definitions up to 1 MiB, but the SDK uploads definitions
  over 100 KB to S3 (`sagemaker-mlops/.../workflow/pipeline.py:273`), so large
  SDK-built pipelines are recommend-only.

## 4. AWS Batch

### 4.1 Units

| request | unit | group / distributed |
|---|---|---|
| `SubmitJob` (container job) | one | — |
| `SubmitJob` with `arrayProperties.size` | one | the array is a group |
| `SubmitJob` with `nodeOverrides` (multi-node parallel) | one per node range | distributed: GPU count and node count held |
| `SubmitJob` on an EKS job queue (`eksPropertiesOverride`) | one per container | — |
| `SubmitServiceJob` (`serviceJobType = SAGEMAKER_TRAINING`) | the embedded `CreateTrainingJob` payload | sized by the SageMaker adapter (§3) |

### 4.2 Request model, prior, rendering

- **EC2 compute environments:** `RequestModel::Free` with a feasibility check
  against the compute environment's instance types. The memory an instance
  advertises to Batch is less than its installed memory (an `m4.large` offers
  about 7,985 MiB of 8 GiB, and 32 MiB is reserved), so the ceiling and the
  "fits one instance" check use **registered** memory: the catalog stores it,
  and the Batch memory-management guide's table seeds it. A request just above
  a boundary moves the job to a bigger instance; the cost model sees that
  through `Packed` pricing over the environment's types.
- **Fargate:** `RequestModel::Combos`: vCPU ∈ {0.25, 0.5, 1, 2, 4, 8, 16, 32},
  each with its allowed memory list (the Batch API reference table, shipped as
  `catalog/aws-fargate.json`). Every candidate is snapped up to a legal pair.
  GPU is not available on Fargate.
- **EKS job queues:** memory request = limit, CPU in steps of 0.25 with
  request ≤ limit, GPU request = limit (Batch on EKS guide). `validate`
  enforces these.
- **Prior.** The job definition's `resourceRequirements`, overlaid by
  `containerOverrides.resourceRequirements` in the request. The definition is
  read with `DescribeJobDefinitions` (cached by ARN and revision). The
  deprecated `memory`/`vcpus` fields are read but never written.
- **Render.** Writes `containerOverrides.resourceRequirements` (or the node
  range's or EKS container's override). The job definition is never changed.
- **`linuxParameters.sharedMemorySize` and `tmpfs`** must stay below memory.
  A memory reduction that would violate that is held.

### 4.3 Platform model

```
PlatformRef { kind: "aws-batch", memory_enforced: true,       # EC2 and Fargate MEMORY is a hard limit
              retry: { same_size: retryStrategy.attempts - 1 (OOM-matching evaluateOnExit only), ladder: per §6 },
              walltime_s: timeout.attemptDurationSeconds, cpu_window_s: 60 }
PlatformState { request_model: Free(EC2 types) | Combos(Fargate),
                price: Packed(EC2 On-Demand/Spot by CE allocation strategy) | Linear(Fargate vCPU-h, GB-h),
                quota: CE maxvCpus minus running }
```

`same_size` counts only retries that an OOM would trigger: if `evaluateOnExit`
sends `OutOfMemoryError*` to `EXIT`, OOM retries are 0.

## 5. Outcomes

### 5.1 Importers (`rightsize history import aws`, also used by `audit`)

| source | gives | resolution | censoring |
|---|---|---|---|
| `ListTrainingJobs`/`DescribeTrainingJob` (and Processing, Transform) | status, `FailureReason`, `BillableTimeInSeconds`, `TrainingTimeInSeconds`, requested type and count | per job | — |
| CloudWatch `/aws/sagemaker/TrainingJobs` (`…/ProcessingJobs`, `…/TransformJobs`): `MemoryUtilization`, `CPUUtilization`, `GPUUtilization`, `GPUMemoryUtilization`, per `Host` | peak = `Maximum` over the job's lifetime × the type's memory | 60 s | whether a datapoint is an instantaneous sample or a period average is undocumented, so `AtLeast` until M5 verifies it (SPEC §5.7); then `Exact`, `resolution_s = 60` |
| `DescribeJobs` (Batch) | `status`, `container.exitCode`, `container.reason`, `statusReason`, attempts with start and stop | per attempt | — |
| Container Insights for the compute environment (ECS `ENHANCED`, task-level `MemoryUtilized`) | peak memory per attempt | the CE's aggregation interval | `AtLeast` until M5 verifies the statistic is a sample maximum; then `Exact`, `resolution_s` = interval |
| in-container self-report (`python -m rightsizer.run -- CMD` as the entry point, or `rs.measure()`) | sampled RSS (cgroup when present) and NVML every 0.5 s | 0.5 s | `Exact`, `resolution_s = 0.5` |

- CloudWatch utilization metrics are percentages of the instance. For GPU
  memory, `GPUMemoryUtilization` is summed across GPUs (0–100 × GPUs), so the
  busiest device is unknown: the record stores `gpu_memory_peak` as
  `AtMost(max over hosts of the per-host sum)` when there is one GPU per host,
  and omits it otherwise.
- A peak sampled at 60 s misses short spikes. Even once verified as samples,
  S3(b) uses the coarse-resolution headroom for these (1.5) and requires
  `2 × min_history` runs (SPEC §2.3), so CloudWatch-only history earns
  reductions slowly. In-container self-report is the fast path, and until M5
  the only one.
- Joining: the `RIGHTSIZER_DECISION` marker (or the tag) when present;
  otherwise the job name with the SDK's suffix removed, plus the training image
  (§7). The SDK builds names as the base truncated to 38 characters plus
  `-YYYY-MM-DD-HH-MM-SS-mmm`, and v3 replaces `_` with `-`
  (`common_utils.py:135-152`, `model_trainer.py:719`), so the key compares
  against the truncated, normalized base.

### 5.2 Status classification

| signal | status |
|---|---|
| Batch `container.reason` starting `OutOfMemoryError: Container killed due to memory usage` | `OomKilled` |
| Batch `exitCode == 137` without that reason | `Failed` (137 is also a stop or timeout SIGKILL) |
| SageMaker `FailureReason` matching `Please use an instance type with more memory` (verify M5), or `exit status 137` / `out of memory` in an `AlgorithmError` | `OomKilled` |
| `FailureReason` containing `CUDA out of memory` / `torch.OutOfMemoryError` | `GpuOom` |
| `MemoryUtilization` max ≥ 95% in the last 3 samples before a failure with no other reason | `OomKilled` with `status_confidence = Inferred` (SPEC §5.7): used for learning; for breakers it counts under the unclassified rule (SPEC §2.3 S5) |
| `StoppingCondition` reached / `MaxRuntimeExceeded` | `Timeout` |
| Spot interruption / `CapacityError` | `Preempted` |

The patterns live in `rightsizer-cloud/patterns/aws.toml`, versioned, with
fixture tests. A pattern miss degrades to `Failed`, never to a false
`Succeeded`.

## 6. Ladders

SPEC §7.2 defines the two ladder drivers, `Caller` and `Orchestrator`. On
AWS:

| path | driver | S3(a) available |
|---|---|---|
| `rs.aws.sagemaker.run(trainer_or_request, wait=True)` | **caller**: rightsizer owns the wait; on an OOM-class failure it submits `<name>-rs<k>` at the next step and returns the final job. Any other failure, including an unclassified one, is returned to the caller unchanged | yes |
| `rightsize aws batch submit --wait` | caller | yes |
| `rs.aws.install()` under Airflow (the listener publishes the attempt context: orchestrators.md §3.2) | **orchestrator**: escalate-on-retry in the hook | yes, when `max_attempts` is known and the task's retries ≥ `max_steps` |
| Metaflow `@batch` | **orchestrator**: escalate-on-retry in `@rightsize` itself; the hook only adds the marker (orchestrators.md §2.2) | yes, with `@retry(times ≥ max_steps)` |
| `rs.aws.install()` alone (SDK `wait=True` or fire-and-forget) | none | no: reductions need S3(b) |
| SageMaker Pipelines, Step Functions | none | no |

- Caller ladders resubmit from the **original request**, which the helper
  holds in memory. It is never written to the history store, because it can
  contain environment variables and hyperparameters with secrets.
- `CheckpointConfig.S3Uri` makes a resubmitted training job resume. The
  helper keeps it unchanged across steps.
- Before each reduced attempt the helper writes the ladder lease (SPEC §7.2).
  If the helper's process dies (a closed notebook, a CI timeout) with a failed
  reduced attempt, the lease shows the ladder was abandoned and the failure
  counts as unrecovered (per-workload breaker).
- There is no detached supervisor that resubmits failed jobs: the caller or
  pipeline that launched the original job would still see its failure (SPEC
  §7.2, "Which ladders count").

## 7. Keys

- **Suffix:** the `rightsizer:key` tag if the caller sets it; else the SDK
  `base_job_name` (job name minus the timestamp suffix); else the entry script
  when recorded (v2 `sagemaker_program` hyperparameter; v3 equivalent
  verify M5); else a digest of the training image and entry point.
- **Code:** the entry script, when `rs.aws.install()` runs in the process that
  holds the `source_dir` (SDK v2) or `SourceCode` (v3); otherwise `None`.
- **Inputs:** `InputDataConfig[*].DataSource.S3DataSource.S3Uri` (SageMaker),
  `ProcessingInputs`, `TransformInput.DataSource`. Metadata is read with the
  caller's credentials within the SPEC §5.2 byte budget. `S3Prefix` sources
  are summed by `ListObjectsV2` up to 10,000 keys, then `bytes = AtLeast`.

## 8. Tenant and trust

- **Tenant** = `aws://{account}/{role-name}` from `sts:GetCallerIdentity`
  (cached per credentials), normalized from the assumed-role ARN. The client
  is the user's own process, so this tenant only selects which history the
  decision reads. What protects history is the store: records are
  `Authenticated` only when written with credentials scoped to the tenant's
  prefix (SPEC §8.2).
- **Importers** run with an operator role. With `[aws.audit] cloudtrail =
  true`, each job's tenant comes from the CloudTrail `userIdentity` of its
  `Create*`/`SubmitJob` call, in the same `aws://{account}/{role-name}` form as
  the live tenant, and the records are `Verified`. Without CloudTrail, jobs
  can be attributed only through caller-chosen names and an account-wide
  mapping, so the records are `Unverified`: shown in `audit`, used for
  learning, never for floors, anchors or S3(b) (SPEC §8.2). Any role in the
  account could otherwise submit tiny jobs under another team's key.
- **Tags** (`aws:RequestTag`) are set by the caller and are never used for
  tenant identity.

## 9. Configuration

```toml
[aws]
regions = ["us-west-2"]
decide_budget_ms = 500
allowed_instance_types = []            # mirror of the IAM allow-list, if any
tag = "off"                            # "off" | "auto" (after iam:SimulatePrincipalPolicy confirms AddTags/TagResource)

[aws.sagemaker]
families = ["ml.m5", "ml.c5", "ml.r5"]   # CPU families a type may move between; GPU families need the GPU type change opt-in

[aws.audit]
cloudtrail = false
```

## 10. Milestone and tests (M5)

- Recorded-request fixtures for SDK v2 `Estimator.fit`, v3 `ModelTrainer.train`,
  raw boto3, and Pipelines; the hook changes only the resource fields and the
  marker; an edit that fails input-shape validation is dropped.
- botocore versions pinned in CI; the `create_client` wrapper is checked
  against each.
- Status-pattern fixtures from real failures (OOM, GPU OOM, timeout, spot).
- A caller-ladder test: a training job that OOMs at step 0 and succeeds at
  step 1 returns success to the caller with one extra job.
