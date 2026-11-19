# rightsizer for Kubernetes

Status: **draft v2**. Core concepts: [SPEC.md](SPEC.md). Plugin contract:
[plugins.md](plugins.md).

## 0. Day one

```bash
helm install rightsizer rightsizer/rightsizer --set history.postgres.url=$PG_URL   # webhook, watcher, alerts
kubectl label ns ml rightsizer.io/enabled=true          # default mode: recommend (no resource changes)
rsz history import k8s --namespace ml --since 30d # backfill from Prometheus
kubectl rsz shadow -n ml --since 14d              # savings / would-fail / readiness
kubectl label ns ml rightsizer.io/mode=enforce --overwrite
rsz control mode-ceiling recommend                # global kill switch, if ever needed
```

## 1. Surfaces

| surface | users | modes | retry semantics |
|---|---|---|---|
| `rsz recommend` / `apply` / `explain` / `shadow` (also `kubectl rsz …`) | developers, CI, GitOps | recommend; enforce (rewrites files) | `same_size = backoffLimit`; `apply --watch` adds a ladder |
| **Mutating admission webhook** (`rsz webhook serve`) | platform teams | observe, recommend, enforce | `same_size = backoffLimit`; no ladder |
| **Watcher** (same deployment, leader-elected) | — | — | — |
| **Node agent** (optional DaemonSet, M3) | — | — | — |
| `rsz history import k8s` | backfill | — | — |

## 2. Units, groups, keys

| object | unit | group | key suffix |
|---|---|---|---|
| bare Pod | each container (init, sidecar, app) | — | `pod/{name − generated suffix}/{container}` |
| Job / CronJob | each container of the template | all pods of the Job | `{cronjob\|job}/{name}/{container}` (a CronJob's Jobs use the CronJob name) |
| JobSet | container × replicatedJob | each replicatedJob | `jobset/{name}/{replicatedJob}/{container}` |
| PyTorchJob (Kubeflow) | container × replicaType | each replicaType | `pytorchjob/{name}/{replicaType}/{container}` |
| TrainJob (Kubeflow Trainer, M3) | `spec.trainer.resourcesPerNode` | trainer nodes | `trainjob/{name}/trainer` |
| RayCluster / RayJob (M3) | head group; each worker group | each worker group | `ray/{name}/{group}` |
| Volcano Job (M3) | container × task | each task | `volcano/{name}/{task}` |
| Flyte task pod (not sized by Flyte) | the task container | — | `flyte://{org}/{project}/{domain}/{task}` from Flyte's pod labels, **only if** the pod's namespace equals `[k8s.flyte] namespace_template` (default `{project}-{domain}`) rendered from them; otherwise the pod is a bare pod (`k8s://…`). Labels are user-settable; the namespace check prevents cross-project history writes (flyte.md §12) |

- **Tenant:** `k8s://{cluster}/{namespace}`. The cluster name comes from config
  and is required to be unique per store. The namespace comes from the admission
  request.
- **Suffix:** generated name suffixes are stripped by rule. The
  `rightsizer.io/workload-key` annotation sets only the suffix.
- **Distributed units:** a group with more than one replica, or a command or env
  containing `torchrun`, `--nproc_per_node`, `WORLD_SIZE`, `deepspeed` or
  `accelerate launch`. For these the GPU count and replicas are held
  (SPEC §5.4). Group demand from history is the maximum across ranks.

## 3. The admission webhook

### 3.1 What it intercepts

| rule | operation | why |
|---|---|---|
| `batch/v1 Job`, `JobSet`, `PyTorchJob` (M2); `TrainJob`, `RayCluster`/`RayJob`, Volcano `Job` (M3) | CREATE | Jobs are sized **once, at the template**. Every replica gets the same request; gang schedulers and Kueue see the final request when they compute quota. |
| `v1 Pod` | CREATE | **Bare pods only.** Skipped: pods with an owner of a kind above; pods with `kueue.x-k8s.io/queue-name` or owned by a queue-managed workload; pods already sized by another layer (annotation `rightsizer.io/decision-id` written by the Job-level rule, or any container env `RIGHTSIZER_DECISION` from the Flyte integration). The webhook's own reinvocation is not a skip: it resumes the units it has not sized (§3.2). |

- **Kueue** creates a Workload from a job's templates after the job is created
  (suspended). Sizing at CREATE keeps its quota consistent. Pods of admitted
  workloads are never mutated, and queue-managed jobs never change GPU type,
  because flavors are Kueue's choice.
- **Kubernetes ≥ 1.36** (`MutablePodResourcesForSuspendedJobs`, beta) allows
  container resources of a *suspended* Job to change. Rightsizer still sizes at
  CREATE. The watcher records the template's resources at pod creation, so
  later edits by other actors are attributed correctly.
- **Pod-level resources** (`spec.resources`): when present, they are the unit,
  and per-container resources are held.

### 3.2 Configuration and operation

```yaml
webhooks:
  - name: jobs.rightsizer.io
    namespaceSelector: { matchLabels: { rightsizer.io/enabled: "true" } }
    objectSelector: { matchExpressions: [{ key: rightsizer.io/mode, operator: NotIn, values: ["off"] }] }
    failurePolicy: Ignore
    timeoutSeconds: 2
    reinvocationPolicy: IfNeeded
    sideEffects: NoneOnDryRun
    admissionReviewVersions: ["v1"]
```

- **Modes.**
  - Set per namespace with the label `rightsizer.io/mode`, default `recommend`.
  - Set per object with the label or annotation of the same name. Either can
    only lower the namespace's mode. Only the label (`objectSelector`) excludes
    the object at the apiserver, so the webhook is never called for it.
  - All modes are capped by `mode_ceiling`.
  - In `observe` and `recommend`, the webhook returns an **annotation-only
    patch**: `rightsizer.io/decision-id` and `rightsizer.io/recommendation` (a
    compact summary). `kubectl describe` then shows the decision, and the watcher
    joins outcomes by decision id. Resources are untouched.
- **Request path.** p99 < 100 ms; internal deadline 1 s.
  - Only in-process estimators and the **memo cache** run on the request path.
  - The memo holds normalized estimates only (SPEC §6.3), keyed by
    (key, code digest, feature bucket, policy hash), and is refreshed by the
    watcher after each outcome (§6.1). Constraints, S3, breakers and the mode
    are evaluated on every request.
  - Inputs are **hash-only** on the request path. Their metadata comes from the
    memo, populated asynchronously (§4.3).
- **Idempotence.** Patches add `rightsizer.io/decision-id` and
  `rightsizer.io/prior`, the original resources **HMAC-signed** with a key id.
  - On reinvocation (for example after a mesh sidecar injection), only units not
    yet sized are recomputed.
  - An unsigned or invalid `rightsizer.io/prior` is ignored and overwritten.
  - `rsz k8s restore` uses the signed prior to revert.
- **Dry-run.** Decisions are computed but not recorded.
- **Deployment** (Helm chart):
  - 2+ replicas, a PodDisruptionBudget, cert-manager certificates with caBundle
    injection;
  - a `PrometheusRule` with alerts, including
    `apiserver_admission_webhook_fail_open_count{name=~"jobs.rightsizer.io|pods.rightsizer.io"} > 0`
    (an expired certificate fails open silently otherwise);
  - a dashboard.

### 3.3 Prior

- **Pods.** LimitRanger (an in-tree admission plugin) runs before mutating
  webhooks, so a pod may already carry LimitRange defaults. Axes equal to the
  namespace's LimitRange default that the user's object did not set are marked
  `Prior.defaulted`. On reinvocation, "did not set" comes from the signed prior;
  otherwise it is inferred by equality.
- **Job and CRD templates.** LimitRanger fills nothing at this point; it acts
  only when the pods are created. The adapter **simulates LimitRange
  defaulting** on the raw template to fill the prior and marks those axes
  `defaulted`. With no LimitRange and no requests, the unit has no prior
  (SPEC §7.4, recommend-only by default).

### 3.4 Validation before emitting a patch (S2)

Validation, quota and pod security all run after mutating webhooks. `validate`
checks the patched object against each of the following:

- every **LimitRange**: `min`, `max`, `maxLimitRequestRatio`, per container and
  per pod. For templates, these are checked as the pods will be;
- **ResourceQuota**, which applies to pods. For a Job,
  `min(parallelism, completions) × pod request` must fit the remaining quota,
  minus the **reservation ledger**: increases admitted in the last 2 minutes
  whose pods do not exist yet, kept in-process and shared across replicas
  through the store;
- **Kueue** (queue-managed jobs): the increase must fit the ClusterQueue's
  nominal quota, including borrowing limits, and the largest shape of its
  flavors. Waiting in the queue is acceptable; never being admissible is not;
- pod-spec invariants: unique volume names and `mountPath`s. An existing
  `/dev/shm` mount is resized in place, never duplicated;
- QoS preservation (SPEC §7.4);
- **GKE Autopilot** (detected from the cluster): requests below the compute
  class minimum are raised by Autopilot, requests above its maximum are
  rejected, CPU:memory must lie within the class's ratio (for example 1:1 to
  1:6.5 for general-purpose), CPU rounds up to 0.25 vCPU without bursting,
  limits are set to requests on non-bursting clusters, and GPU pods have
  per-GPU-count CPU and memory maximums. `validate` normalizes the candidate
  by these rules first, so the decision describes what will actually run, and
  a reduction Autopilot would undo is recorded as held (managed-ml.md §6);
- the **effective pod request** against the largest node shape:
  `max(max over init containers, counting restartable sidecars started before each; sum(app) + sum(sidecars)) + RuntimeClass overhead`.

A failure produces no resource patch. The decision records
`Prior{reason="validation:<check>"}`, plus the annotation-only patch.

### 3.5 Rendering

- `resources.requests` and `resources.limits` per container, under the S2 limits
  policy.
- **No memory limit means memory is not enforced.** For a container whose prior
  has no memory limit, `memory_enforced = false`. Reductions then need S3(b)
  evidence, and the cost model adds the neighbour-risk premium. Node-pressure
  evictions of a reduced unit count as rightsizer-caused (S5).
- **GPUs:** `nvidia.com/gpu` and `amd.com/gpu` counts are held in v1
  (SPEC §5.4). Lower-count findings are advisories. A GPU type change
  (opt-in, M4, never on queue-managed jobs) adds a node-selector term:
  `nvidia.com/gpu.product`, `cloud.google.com/gke-accelerator` or
  `karpenter.k8s.aws/instance-gpu-name`, per config. The term is
  **intersected** with existing required node affinity, tolerations and known
  Karpenter NodePool requirements. If the intersection is empty, the type is
  held.
- **KubeRay (M3):** container resources and the matching `rayStartParams`
  (`num-cpus`, `num-gpus`, `memory`) change together.
- **Held in v1:** shm, ephemeral storage, DRA `ResourceClaim`s. These appear in
  advisories only.

### 3.6 GitOps

Job-level mutation makes the live object differ from Git. Two options:

- add `ignoreDifferences` on `/spec/template/spec/containers/*/resources` (and
  the CRD equivalents; the chart docs ship snippets);
- run `rsz apply` in CI and keep the webhook in `recommend` for those
  namespaces.

Self-healing an unsuspended `batch/v1` Job's template fails, because the
template is immutable.

## 4. Context extraction

### 4.1 Code

- the image (digest when resolvable), command and args;
- with `rightsizer.io/code-configmap: name/key`, a script from a ConfigMap in
  the pod's **own namespace**.

K8s contexts usually carry no code tokens, so the M4 gate routes them to the
robust estimator.

### 4.2 Manifest features

- memory-backed `emptyDir` volumes and their `sizeLimit`;
- init containers and their requests;
- sidecar count;
- `activeDeadlineSeconds` (walltime) and `backoffLimit` (retry);
- node selectors and tolerations that imply a GPU type;
- staging patterns: an init container copying into a memory-backed volume is
  the tuner's `init-tmpfs` staging (`tuner:bench_all/k8s.py:1-40`).

### 4.3 Inputs, and whose credentials read them

Inputs are declared with the annotation
`rightsizer.io/inputs: '["s3://bucket/train.parquet", …]'`.

- **When:** never on the request path. The watcher's **prefetcher** reads the
  metadata of annotated inputs after admission. It feeds the memo for this
  workload's next invocations, and the outcome record of this one.
- **Credentials (M2):** the prefetcher reads only URIs under the admin-configured
  per-namespace prefixes (`[k8s.inputs.allow]` in config). Anything else is
  recorded by hash only.
- **Credentials (M3, optional):** `input_credentials = "pod-service-account"`.
  The prefetcher requests an audience-bound, 10-minute token for the pod's
  ServiceAccount (TokenRequest) and reads through the platform's workload
  identity (IRSA, GKE Workload Identity, Azure WI). It can therefore read only
  what the pod could. Token creation is granted per opted-in namespace through
  RoleBindings.
- HTTP(S) inputs pointing at private, loopback or link-local ranges are refused.

## 5. Platform model

- **Shapes:** from config, or discovered from Nodes (allocatable, GPU labels) and
  Karpenter NodePools when present.
- **Quota:** ResourceQuota per namespace (30 s cache) and the reservation ledger
  (§3.4). Kueue ClusterQueues and ResourceFlavors.
- **Retry:** `same_size = backoffLimit`. A ladder exists only with
  `rsz apply --watch`, which resubmits a renamed Job one step up on
  `OOMKilled`.
- **CPU window:** the configured source's resolution. The node agent gives 1 s;
  Prometheus gives its scrape interval (≥ 15 s recommended).
- **Overhead:** none by default. Per-image overhead distributions come from
  config.

## 6. Outcomes

### 6.1 Watcher

A leader-elected controller in the webhook deployment. Its informers are
restricted by label and annotation selection to sized or annotated pods in
enabled namespaces, and they cover **every** enabled namespace in observe,
recommend and enforce. It writes a `RunRecord` when a container terminates,
deduplicated on `(key, invocation_id, attempt, source)`:

- **Status:**
  - from `state.terminated.reason`: `OOMKilled`, `Error`, `Completed`, plus the
    Job's `DeadlineExceeded`;
  - from pod `status.reason`: `Evicted`, preemption.
- **Peaks:**
  - from the node agent (§6.2) when present;
  - otherwise from Prometheus (§6.3).
- **Trust:** `Verified`.
- **Join:** decision id, falling back to (tenant, owner UID, unit id).

### 6.2 Node agent (optional, M3)

A DaemonSet that polls the cgroup v2 files (`memory.peak`, `cpu.stat`) of
tracked containers every 1 s and keeps the last values. Cgroups are removed
shortly after a container exits, so reading only at termination would miss
them. On container exit (detected through its poll, or NRI where available) it
hands the values to the watcher.

| resource | value |
|---|---|
| CPU request | 10m |
| memory request | 40Mi |
| `/sys/fs/cgroup` | read-only hostPath |
| hostPID | no |
| scheduling | nodeSelector and a priorityClass |

### 6.3 Prometheus importer

| measurement | query (per container, over the pod's lifetime) | notes |
|---|---|---|
| memory peak | `max_over_time(container_memory_working_set_bytes{…}[life])` | `resolution_s` = scrape interval; within 5% of the limit → `AtLeast` |
| CPU busiest window | `max_over_time(rate(container_cpu_usage_seconds_total{…}[w])[life:w])` | `w` ≥ 2 × scrape interval |
| CPU mean | `increase(container_cpu_usage_seconds_total[life]) / life` | |
| throttling | `container_cpu_cfs_throttled_periods_total / container_cpu_cfs_periods_total` | |
| OOM | `kube_pod_container_status_terminated_reason{reason="OOMKilled"}` (restartPolicy Never) or `…last_terminated_reason…` (after restarts) | |
| owner join | `kube_pod_owner` → `kube_job_owner` → CronJob | from metric history, so pods deleted by TTL still join |
| GPU memory | `DCGM_FI_DEV_FB_USED` per device, joined by pod labels | includes the framework allocator's reserved memory, which the job must be given anyway; on shared devices (time-slicing, MPS) other tenants' memory is included → `AtMost` |

## 7. RBAC

The webhook and watcher ServiceAccount is granted:

- **read:** Pods, Jobs and the listed CRDs, LimitRanges, ResourceQuotas, Nodes,
  Kueue ClusterQueues and ResourceFlavors, and Leases (for leader election);
- **ConfigMaps:** read only, through namespaced Roles in opted-in namespaces;
- **ServiceAccount tokens:** created only through RoleBindings in namespaces that
  opt in to `input_credentials = "pod-service-account"`;
- **no write access to user objects:** mutation happens through admission
  responses. The exception is `rsz k8s restore`, which runs with the
  invoking user's credentials.
