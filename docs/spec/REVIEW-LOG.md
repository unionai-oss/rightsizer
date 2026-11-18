# Review log

The spec went through rounds of design, adversarial review and refinement.
Each round had independent reviewers with separate lenses. They were given
the spec and the primary sources (the flyte-sdk source, the resource tuner's
code and research logs, platform docs), not the author's conclusions. Every
finding that changed the design was spot-checked against the source before
it was accepted.

## Round 0: research

- **Flyte v2 SDK** (flyte-sdk HEAD, 2026-11-18): where a child action's spec is
  built, what `override` and `with_runcontext` can and cannot do, plugin and
  entry-point mechanisms, OOM error propagation, `flyte.remote` history,
  metrics (proprietary, Union-only).
- **Platforms:** a ranked survey of Kubernetes alternatives for AI. Slurm and
  Ray were chosen; Nomad was rejected because it is BUSL-1.1, not OSI open
  source, and runs few AI workloads. The survey also covers each platform's
  injection points, history sources, OOM signals and GPU naming.
- **Resource tuner:** the information boundary, the grids, the scorer, the
  tune service and store, the K8s twin, and the gate.

## Round 1: draft v0 → v1

Three reviewers, about 60 findings: Flyte (19), platforms and plugins (20),
core design (22).

| # | finding (severity) | resolution in v1 |
|---|---|---|
| C1 | **Blocker.** The risk knob was misattributed. The tuner's 2/12/40 values are a *training-loss* `under_penalty`, one trained model per value. The decode-time knob is `OOM_PENALTY` in oracle-cost multiples (default 1, swept 0–16). Verified at `bench/scoring.py:41-47`. | SPEC §3 and §7.3: `oom_penalty` with presets 1/4/16. |
| C2 | Headline numbers were stale (round 17, memory only, zero-shot sample mostly unanswered). Verified against round-23 §7. | SPEC §3 cites the k8s-2 joint leaderboard. |
| C3 | The chain dropped the arm that is robust on unseen code (`rules-sized`). The pool is log-linear with w=0.7, not a 50/50 mix. | SPEC §6.2 and §6.5: default chain is `gate(pool, rules-sized)`; pool `op = "log-linear"`. |
| C4 | The novelty threshold is a validation-set *quantile* of the kNN distance, not a 0–1 score. It needs code tokens (AUROC 0.999 vs 0.67). | SPEC §5.5 defines novelty as an ECDF; the gate routes to novel when code features are missing. |
| C5 | The CPU grid did not match the tuner's 16 steps, so the "reject mismatched grids" rule would have rejected the flagship model. Verified at `bench_cpu/grid.py:11`. | SPEC §5.3: `rs-grid-v1` contains the tuner grids as prefixes; foreign grids are rebinned conservatively, not rejected. |
| C6 | **Blocker.** The GPU axes could not be decoded: per-device cells and a count grid, but no count estimate. The tuner uses total GPU memory over machine shapes. Verified at `bench_gpu/grid.py:29-126`. | SPEC §5.4: shape catalog, total demand plus parallelism, the busiest-device fit rule, and GPU count pinned for distributed units. |
| C7 | The GBT format did not match the tuner, which uses sklearn HistGradientBoosting, not LightGBM. | SPEC §6.2: `rightsizer-trees/1` JSON format, sklearn and LightGBM exporters, parity tests. |
| C8 | Runtime overhead is a distribution (p90 242 MiB), not a constant. | SPEC §3; overhead model per adapter. |
| C9 | **Blocker.** The retry model contradicted escalation: "one cell (×2)" mixed units, and a GPU step could land on the same device. Raw Jobs retry at the same size. | SPEC §7.2: `PlatformRetry` {None, SameSize, Ladder}; capacity ×2; next GPU shape up. |
| C10 | Joint decoding was unspecified. Packed pricing is not separable, and host resources on GPU nodes are bundled. | SPEC §7.2: attempt-sequence simulation per candidate; GPU-share pricing. |
| C11 | Quantile tails, point margins, `t_peak_frac`, and the role of duration were unspecified. Overhead risked being double-counted. | SPEC §5.5 normalization rules; `t_peak_frac`; walltime/timeout use; demand excludes overhead (§5.2). |
| C12 | RL policies output actions, not demand. | SPEC §5.5: `kind = Action` → argmax + constraints. |
| C13 | Clamping after the argmin is suboptimal. The floor can exceed the ceiling. The history-max floor only ratchets upward. | SPEC §7.4: constraints define the feasible set; size-scaled, same-digest, last-K floor; trust region; floor-over-ceiling rule. |
| C14 | A CPU stretch can cause a timeout, and on GPU nodes it idles the GPU. | SPEC §7.2. |
| C15 | **Blocker.** Outcomes are censored (OOM gives a lower bound; usage is capped at limits; reclaim; ru_maxrss misses children; selection bias). | SPEC §5.7: `Measurement{censor, resolution_s}`, throttling, cgroup `memory.peak`; decision logs carry action probability; ε-exploration. |
| C16 | The protocol had no versioning, error taxonomy, batching or grid negotiation, and its cache key was unusable for pre-warming. | SPEC §6.3. |
| C17 | Privacy covered only source code. | SPEC §11: per-field policy; URIs never sent, only salted hashes. |
| C18/P3/P4 | Security: confused deputy, history poisoning through user-set keys, forgeable Slurm comments, and the claim that a compromised estimator can cause only waste. | SPEC §8.3 server-derived tenants and outcome trust levels; §13; kubernetes.md §4.3; slurm.md §4. |
| C19/F7 | sqlite on ephemeral Flyte pods or NFS; double sizing of Flyte or KubeRay pods by the webhook. | SPEC §8.1: enforce requires a durable shared store; `decision-id` skip rule. |
| C20 | Python async and fork semantics; abi3 vs free-threaded builds. | SPEC §10. |
| C21 | No prior (BestEffort pods); no `features-v1` model at M0. | SPEC §7.4 "No prior"; §6.2: no bundled learned model claimed. |
| C22 | Milestone ordering. | SPEC §15. |
| **Safety** | "Never break a workload" conflated rightsizer *errors* with rightsizer-*caused* failures. | SPEC §2.3: contract S1–S5, with attribution and breakers. |
| F1 | **Blocker.** `override()` on a subclass recursed, or re-sized over a user's explicit override. Verified: `dataclasses.replace` keeps the class. | flyte.md §4.1: explicit resources become pins; the `_rs_bypass` copy goes to the controller. |
| F2 | **Blocker.** Reusable environments reject resource overrides, and resources are part of the pool identity. | flyte.md §6 and §10: reusable tasks are observe-only per invocation and sized at deploy time. |
| F3 | **Blocker.** `attach(env)` cannot rewrap tasks that are already defined. | Dropped. `RightsizedEnvironment` only; orchestrator variants are out of scope. |
| F4 | The `post` hook does not run on failure; `ru_maxrss` misses child processes; reusable containers; no store in pods. | flyte.md §8.1: `try/finally` in `execute`, cgroup `memory.peak`, object-store shards. |
| F5 | A CUDA OOM arrives as `RuntimeUnknownError`, not `OOMError`. | flyte.md §8.2. |
| F6 | Unbounded fan-out cost. | flyte.md §7: limiter, 250 ms budget, batching, feature-bucket memoization. |
| F8–F10 | Image dependency, pickled bundles, local mode. | flyte.md §4.4, §4.1, §4.5. |
| F11 | The levels overlapped, precedence was unsound, and a run could unpin an author's pin. | flyte.md §3: definition vs invocation scopes; the stack; mode lattice. |
| F12–F19 | `sized()` instead of `call(rightsize=)`; eager policy capture; root sizing limits; whole-`Resources` merge; GPU names; registration; cache-key safety; `custom_context` strings. | flyte.md §4–§6. |
| P1 | **Blocker.** `failurePolicy: Ignore` does not cover patches rejected later by LimitRange, Quota or duplicate mounts. | kubernetes.md §3.4: validate before emitting. |
| P2 | **Blocker.** `job_submit` trusted user-controlled fields; there was no transport to the controller, and I/O under controller locks. | slurm.md §4: HMAC token verified with no I/O; `admin_comment`; optional decision table loaded at script load. |
| P5 | The prior was read from forgeable annotations, and LimitRange defaults were confused with author intent. | kubernetes.md §3.3: live object, `Prior.defaulted`, signed prior, `reinvocationPolicy`. |
| P6/P7 | Kueue and gang jobs: per-pod mutation desynced quota and replica symmetry. | kubernetes.md §2–§3: size at CRD CREATE; groups; skip queue-managed pods. |
| P8/P9/P10 | GPU-type selector conflicts; QoS downgrades; effective pod request with sidecars. | kubernetes.md §3.4–§3.5. |
| P11/P12 | Slurm flag exclusivity, `--mem=0`, `srun` in an allocation, het jobs; per-node vs per-task `MaxRSS`; Prometheus resolution; `terminated_reason`. | slurm.md §3 and §5; kubernetes.md §6.2. |
| P13/P14/P19 | Ray: wrapper breaks the API; `ray.get` on arguments; Ray Data; actors; OOM classes; KubeRay `rayStartParams`; entrypoint value. | ray.md. |
| P15/P16 | Plugin trait shape; out-of-process protocol with no handshake; `PATH` discovery in server modes. | plugins.md §2–§3: `Adapter`/`PlatformModel`/`OutcomeSource`; `Patch` enum; JSON-RPC 2.0 `rightsizer-plugin/1`; absolute paths with sha256 pins. |
| P17/P18/P20 | GitOps drift; Slurm keys that never repeat; missing platforms (Armada, HTCondor, Argo). | kubernetes.md §3.6; slurm.md §6; plugins.md §5. |

## Round 2: v1 → v2

Three fresh reviewers looked at v1: an implementer (coherence, implementability,
a cut list), an operator and adopter (SRE on-call, day-one ML user), and a fact
checker (flyte-sdk HEAD `2e821ef`, ray `262f169`, Slurm/K8s sources). That gave
70 findings. The fact checker's claims about `clone_with`, Ray `_remote`,
`task_oom_retries`, the env-var exclusion from Flyte cache keys and the Slurm
reload rule were re-checked in source before being accepted.

| # | finding (severity) | resolution in v2 |
|---|---|---|
| I1 | **Blocker.** The joint argmin was undefined and intractable done naively (about 4×10⁹ simulations). | SPEC §7.2: axes independent given context; closed form over the ladder Markov chain; per-cell CDFs precomputed; < 5 ms CI benchmark. |
| I2 | The tuner's `DELAY_PENALTY` was missing, so CPU decisions came out indifferent. | SPEC §3, §7.2, §7.3: `delay_penalty`; `oracle_cost` defined. |
| I3 | Stretch could not be computed from the marginals. | `stretch(c) = E[max(1, cpu_mean/c)]`; `cpu_peak` is diagnostic only. |
| I4 | **Blocker.** The default chain was stated three ways, and `rules-sized` always answered. | One default (`rules-sized → history-peak → prior` until M4); `Insufficient` declines (SPEC §6.1–6.2). |
| I5 | The gate had no novelty artifact. | `rightsizer-novelty/1` companion file; gate deferred to M4. |
| I6 | "Scaled to input size" had three definitions and was unsafe to scale down. | One `scale = max(1, b_now/b_hist)` (SPEC §7.4). |
| I7/I8 | Trust region and defaulted-axis semantics. | Defined anchor and scope; defaulted ≠ pinned, but S3 still applies. |
| I9 | LimitRanger does not default Job/CRD templates. Verified at `limitranger/admission.go`. | kubernetes.md §3.3: simulate LimitRange defaulting for templates. |
| I10 | Shadow mode could not join decisions to outcomes. | Annotation-only patch in observe and recommend; watcher covers all enabled namespaces; join fallback. |
| I11 | **Blocker.** "The child path is pure Python" was false: Flyte parents are pods. | SPEC §10.2: `rightsizer-runtime` (pure) plus `rightsizer` (native); lazy `_core`; S1 on a missing wheel. |
| I12/O2 | **Blocker.** Child records claimed `Authenticated` with no mechanism. | SPEC §8.2: trust is defined by tenant-scoped credentials; otherwise `Unverified`; Flyte falls back to the ladder and backfill. |
| I13/I14 | Undecorated `sized()` calls were misclassified as cached; the webhook double-sized pods in recommend mode. | `RIGHTSIZER_DECISION` env var (excluded from the cache key, verified at `convert.py:1150`) in every mode but off; webhook skips it. `pod_template` merging dropped (it replaces wholesale). |
| I15 | Cache watermark unsound; pre-warm impossible. | Exact cache with a hashed watermark; memo keyed by (key, digest, bucket). |
| I16/O1 | **Blocker.** Breaker storage was undefined and the fleet breaker was far too slow (about 150k failures before tripping). | SPEC §2.3 S5: per-workload, per-estimator-version, fast-burn and slow-burn breakers; stored in the history store; manual trip; half-open recovery; re-attribution on input-size jumps. |
| I18 | Slurm table publication was wrong (touched the directory). Verified at `slurm_lua.c`. | slurm.md §4.2: tmp + rename + `touch job_submit.lua`; keyed without hashing; M4. |
| I19 | Retry enums did not compose. | `PlatformRetry { same_size, ladder: Option<Ladder{…, final_floor}> }`. |
| I20 | A decision could not express partial enforcement; `action_prob` was not a propensity. | `recommended` vs `applied`; `held`; `propensity`. |
| I21–I24 | Undefined types; limits on GPU/shm; no grid for shm and disk; live quota in the context digest. | SPEC §5.1 `Axis`, `AxisSet`, `Limits`, `Constraints`; shm and disk held; `PlatformRef` vs `PlatformState`. |
| I25–I28 | Cross-references, naming, Guaranteed QoS, webhook budget, crate boundaries, `prior` as Point. | Fixed throughout; webhook inputs are hash-only on the request path; `rightsizer-py` excludes kube-rs and postgres; Ray adapter in Python. |
| O3 | Object-store compaction and retention had no owner. | SPEC §8.1: singleton with a conditional-PUT lease; `latest` pointers; `start-after` listing; grace-period deletes; retention; metrics. |
| O4 | Version skew. | SPEC §13.2: N/N-1, `write_version`, key ids, upgrade order. |
| O5 | Quota race; Kueue quota. | kubernetes.md §3.4: reservation ledger; parallelism × pod request; ClusterQueue nominal quota. |
| O6/O7 | Breaker false positives; neighbour impact of request-only memory. | Re-attribution rules; `memory_enforced = false` for limitless memory, so S3(b) plus a neighbour premium; evictions count. |
| O8–O12 | Node agent timing, webhook operations, store outage, RBAC, metrics. | kubernetes.md §6.2, §3.2, §7; SPEC §8.4, §13.1. |
| O13 | No global off switch or revert. | `mode_ceiling`; `rightsize k8s restore`; `flyte deploy --restore`; `slurm install --disable`. |
| O14 | Slurm daemon as a single point of failure; arrays. | Negative cache, systemd watchdog, one decision per array. |
| O15 | Ray and Slurm used different rules for unenforced memory. | One rule (SPEC S3). |
| O16 | Cluster identity. | `cluster` on records; `history.share`. |
| O17 | Savings never arrive under code churn. | §1.1 "what to expect"; S3(b) counts other-digest same-key runs at 1.25× headroom; `on_oom="auto"` escalates when `retries == 0`. |
| O18 | No shadow report. | `rightsize shadow` (SPEC §12.2); `report` renamed to `outcome`. |
| O19 | Too many concepts on day one. | Day-one sections in every platform doc; `rightsize="enforce"` shorthand; `auto_image=True`; default store URI. |
| O20/O21 | GPU value was thin; walltimes. | Advisories (SPEC §7.7), including predicted GPU OOM and walltime raises only. |
| O22 | Fan-out fell back to the prior. | Single-flight per memo key. |
| F1 | **Wrong.** `env.clone_with(resources=…)` deploys nothing (new name, empty registry). | flyte.md §6.3: rewrite each task via `override` and the registry. |
| F2 | **Wrong.** Subclass fields without defaults break the non-kw_only dataclass. | Fields use `field(default…, kw_only=True)`. |
| F3 | `override(pod_template=)` replaces rather than merges. | Dropped; env var instead. |
| F5/F6 | Ray: `self.remote` is an instance attribute; `ActorClass` cannot be subclassed. | Override `_remote`; actors through a `sized()` proxy. |
| F7 | `A100-80G` is not auto-labelled. | Only auto-labelled constants unless configured. |
| F8 | `ray_remote_args_fn` takes no arguments and is deprecated. | Dropped; one decision per operator. |
| F9 | `task_oom_retries = -1` retries forever, so escalation never fires. | `max_retries=0` under escalation; verification is an M3 open item; S3(b) otherwise. |
| F10–F17 | WorkerId wording; "exactly one" non-nil Slurm field; reload rule; Job resources mutable when suspended (1.36); LimitRanger and templates; `export` semantics; "exact" cgroup claim; MiB. | Corrected in ray.md, slurm.md, kubernetes.md. |

**Cut list adopted:** milestones in SPEC §15 now gate the gate, pool and
novelty work, GPU type changes, the Slurm decision table, KubeRay, TrainJob,
Volcano, the node agent, TokenRequest input reads, and the Arrow/HF readers
(M3–M4). shm, disk and DRA are held or advisory-only in v1.

## Round 3: v2 → v2.1 (regression pass)

A final reviewer checked every cross-reference mechanically. It spot-checked 28
of the round-2 resolutions: 25 were present and 3 were incomplete. It found
5 blockers, 9 majors and 9 minors. The `docs` and reusable-override claims were
confirmed against `_task.py:514-554`.

| # | finding (severity) | resolution |
|---|---|---|
| R1 | **Blocker.** Flyte never set `memory_enforced`. A scalar memory value is a request only, so a ladder could reduce unenforced memory. | flyte.md §4.2: `memory_enforced` = the prior is a `(request, limit)` tuple, until an M1 integration test settles it (SPEC §16, open question 5). |
| R2 | **Blocker.** Root actions have no parent to catch an OOM, yet were treated as escalating. | flyte.md §6.1: roots have `ladder = None`, so reductions need S3(b). |
| R3 | **Blocker.** Ray's `max_retries=0` makes consumers other than rightsizer see failures Ray would have absorbed (S2). | ray.md §7: no Ray escalation in v1; `max_retries` is never changed. |
| R4 | **Blocker.** `flyte://` keys taken from pod labels let any pod write another project's `Verified` history. | kubernetes.md §2 and flyte.md §12: namespace-template check; otherwise the pod is keyed `k8s://`. |
| R5 | **Blocker.** Deploy-time reductions outlive breakers and the mode ceiling. | flyte.md §6.3 and SPEC §13.3: reductions only with `rightsize flyte reconcile` scheduled; otherwise increases only. |
| R6/R7 | Task `docs` cannot hold the prior; reusable tasks cannot be overridden at deploy time. | flyte.md §6.3: the prior is the source definition; reusable environments are recommend-only. |
| R8 | S3(b) ignored resolution, so Ray samples could never count. | SPEC §2.3 and §5.7: sampled peaks are `Exact` with `resolution_s`; coarse samples get headroom 1.25. |
| R9 | §1.1 overpromised first-run savings. | Reworded. |
| R10 | Flyte and Ray had no source for `PlatformState`, so S4 was impossible. | flyte.md §6.4 and ray.md §2: config or autoscaler shapes; otherwise increases are clamped and counted. |
| R11 | Memoizing whole decisions could bypass breakers. | SPEC §6.3: the memo stores estimates only; one key everywhere. |
| R12 | Shared credentials let tenants reset breakers or raise the mode ceiling. | SPEC §8.1: control objects need operator credentials; clients may only *trip* breakers. |
| R13 | Resizing `salloc`/`srun` jobs could not be attributed. | slurm.md §2: `cli_filter` enforces only for `sbatch`. |
| R14 | The `decide` result lacked the token, mode and summary fields. | plugins.md §3.1. |
| R15–R23 | Wrong cross-reference targets, old enum forms, `--tmp` vs held disk, Slurm `--time`, GPU count opt-in, missing CLI commands, mode-string shorthand semantics, `env_vars=None`, postgres milestone, env var naming. | Fixed. |

## Round 4: coverage research (v2 → v3 draft)

Goal: as much platform coverage as possible for the package and the model,
including commercial platforms. Four parallel research passes, each reading
SDK source where it exists (sagemaker-python-sdk 7263f35, modal-client
a7adb1d, skypilot, dstack, metaflow, airflow, dagster, prefect, kfp, zenml at
their default branches on 2026-11-18) and the providers' API references:

- **AWS:** SageMaker AI (training, processing, transform, Pipelines,
  HyperPod, warm pools, spot, training plans) and AWS Batch (EC2, Fargate,
  EKS, multi-node, service jobs).
- **Modal and serverless GPU:** Modal in depth; RunPod, Baseten,
  Replicate/Cog, Beam, fal, Lightning, Together, Lambda briefly.
- **Other managed services:** Vertex AI (custom jobs, KFP on Vertex, batch
  prediction), Cloud Batch, Cloud Run jobs, GKE Autopilot rules, Azure ML,
  Azure Batch, Databricks, Snowflake.
- **Orchestrators and launchers:** Airflow, Metaflow, Prefect, Dagster, KFP,
  Argo, ZenML, SkyPilot, dstack, Kedro, MLflow Projects.

Findings that shaped v3:

- **No managed ML service has a mutating server-side hook.** IAM, SCPs, Org
  Policy and Azure Policy only deny. Enforcement is therefore client-side
  (a botocore hook, SDK wrappers) or through files (`apply`, `suggest`), and
  every platform gets a read-only audit first. This became the integration
  tiers (plugins.md §1.1) and the hook-point taxonomy (§1.2).
- **Machine types are discrete** on SageMaker, Vertex, Azure ML and
  Databricks, legal pairs on Fargate, and lower bounds handed to an optimizer
  on SkyPilot and dstack. This became `RequestModel` (SPEC §5.8).
- **Modal bills max(request, use)**, so under-asks save nothing. This became
  the `usage-floor` price model (SPEC §7.2).
- **No platform retry changes size.** Ladders come only from a wrapper that
  owns the wait, or from orchestrator retries passing through a hook that sees
  the attempt number. This became `LadderDriver` and escalate-on-retry
  (SPEC §7.2).
- **SkyPilot, dstack, Airflow and botocore fail closed** on hook errors,
  which became the S1 "fail-closed hosts" rule.
- **Adoption surfaces:** `rightsize audit`, `rightsize suggest` (pull
  requests) and an MCP server (SPEC §12.3–12.5), with milestone A shipping
  audit right after M0.

Built in v3: aws.md (SageMaker AI, AWS Batch), modal.md, launchers.md
(SkyPilot, dstack), orchestrators.md (Metaflow, Airflow; notes on KFP,
Dagster, Prefect, Argo, ZenML), managed-ml.md (Vertex AI, Azure ML,
Databricks, Cloud Batch/Run, Snowflake, Autopilot rules).

## Round 5: v3 draft → v3

Two independent reviewers: platform facts (29 findings, checked against the
source clones and provider docs by the reviewer and three helpers) and design
soundness (3 blockers, 11 majors, 20 minors). The blockers were spot-checked
in source before being accepted: SkyPilot recovery re-running the policy
(`recovery_strategy.py:1591`, `execution.py:326-345`), `METAFLOW_DEFAULT_DECOSPECS`
replacing toggled decospecs (`metaflow_config.py:765-767`), and the separate
`@kubernetes` merge loop (`kubernetes_decorator.py:366-384`).

| # | finding (severity) | resolution |
|---|---|---|
| P-B1 | **Blocker.** Airflow `task_instance_mutation_hook` runs in the scheduler; a context it sets never reaches the worker. | orchestrators.md §3.2: not used; a worker-side listener publishes the attempt context; KPO reads it in `execute()`. |
| P-B2 | **Blocker.** SkyPilot managed-job recovery re-runs the admin policy. | launchers.md §1.1: normative marker in task `envs`; marked tasks are never re-decided; §1.6 notes the future ladder hook. |
| P-B3 | **Blocker.** Adding tags needs `AddTags`/`TagResource`, and `UpdatePipeline` has no `Tags`; errors occur after the guard. | aws.md §2.1: tags off by default; the marker is a container environment variable; edits are validated against the input shape before the handler returns. |
| D-B1 | **Blocker.** Escalate-on-retry read attempt 1's decision by listing the decision log, and a missing record silently allowed a fresh reduction. | SPEC §7.2 and §8.1: synchronous attempt record before any reduced attempt (no record → no reduction); a missing record on attempt ≥ 2 means the origin prior; per-member records for groups. |
| D-B2 | **Blocker.** Metaflow's long-lived runtime kept attempt 1's reduced attributes on an attempt-2 S1 fallback. | orchestrators.md §2.2: snapshot at `step_init`, restore before every `runtime_step_cli`. |
| D-B3 | **Blocker.** Unclassified failures of reduced attempts escaped S5. | SPEC §2.3 S5: they count as rightsizer-caused until classified; surfaces without classification cannot enforce reductions; Modal needs a fresh importer (modal.md §5). |
| D-M1 | Persisted edits (`suggest`, GitOps) outlived breakers and became the new prior. | SPEC §2.3 S5 "origin prior" with markers; SPEC §12.4: reductions only under an `enforce` ceiling and no breaker, revert PRs on breaker trips; Metaflow `webhook_resize` removed. |
| D-M2 | MCP returned `recommended`, which ignores S3, into code. | SPEC §12.5: returns `applied` under enforce; unearned reductions marked. |
| D-M3, P-M1 | `max_attempts` unknown on KubernetesExecutor; context did not cross processes. | `max_attempts: Option`, unknown → no ladder; `RIGHTSIZER_ATTEMPT` env mirror. |
| D-M4 | Caller ladders die with the waiting process; other observers of the job. | SPEC §7.2: Caller defined as the wrapper's caller; ladder lease; abandoned ladders trip the per-workload breaker; OOM-only escalation. |
| D-M5 | Double sizing (Metaflow + botocore; SkyPilot on Slurm). | plugins.md §2 "one layer decides"; `RIGHTSIZER_DECIDED`; `[slurm] presized_submitters`. |
| D-M6 | Catalog usable memory counted overhead twice. | SPEC §5.8: `Shape.reserved` (platform) vs the runtime overhead model. |
| D-M7 | Trust region and S3(b) were ill-defined on shapes and snapped asks. | SPEC §5.8 "effective memory": measured on the usable memory of the shape that runs; at most halving per decision. |
| D-M8 | Modal int memory treated as unenforced, though eviction kills the unit. | modal.md §1: `memory_enforced = true`, no neighbour premium. |
| D-M9 | Instance-type moves could hit SCP denies or exhausted quota. | aws.md §3.2: type moves need a known allow-list and known quota usage; else held. |
| D-M10 | Importer records attributed by job names could poison history. | SPEC §8.2 and aws.md §8: `Verified` only with authenticated attribution (CloudTrail); otherwise `Unverified`. |
| D-M11 | Audit replay underspecified. | SPEC §12.3: completed-before-start, recommend steady state, simulated breakers, priced ladders, interval. |
| P-M2/M3 | SkyPilot imports the policy outside its try; more request names than listed. | launchers.md §1.1: stdlib-only top-level imports; validate/optimize not logged as launches. |
| P-M4/M5 | `SubmitServiceJob` handler missing; `InstancePreferences` excludes `InstanceType`. | aws.md §2.1, §3.1, §3.3. |
| P-M6/M7/m17/m18 | Modal stats exclude variants, p99.9 exists, GPU-bundled resources, tuple merging, terminations join. | modal.md §1, §2, §5. |
| P-M8/M9/m14/m15 | Metaflow decospec paths, merge rules, annotation paths, defaults ambiguity. | orchestrators.md §2. |
| P-M10 | Clients created before `install()` are not hooked. | aws.md §2.1. |
| D-m12, P coarse sources | Resolution boundary; averages vs samples. | SPEC §2.3 (`resolution_s ≥ 10` coarse), §5.7 (averages are `AtLeast`; unverified sources treated as averages); CloudWatch `AtLeast` until M5. |
| others | Pipeline size limits, warm-pool match set, spot rows and quotas, name truncation, g5 sizes, Azure MB and per-node memory, Vertex allocated memory, dstack exit codes and TTL, KFP and Dagster citations, `flyte reconcile` in the CLI, ladder GPU steps only within `allowed_shapes`, step only when attempt 1 was reduced, usage-floor savings formula, floor resolution headroom, `status_confidence`, milestone A owning the importers, survey ladder and tier columns, tenant format in launchers day one, dstack memo re-evaluating the mode. | Fixed in place. |

## Round 6: regression pass

A final reviewer checked every Round 5 resolution at its cited location and
probed for contradictions the fixes introduced. Cross-references, tables,
code fences and TOML were checked mechanically and were clean. Fixed:

| # | finding (severity) | resolution |
|---|---|---|
| G1 | **Major.** Attempt records existed only for reduced attempts, so a job sized *up* that then failed would retry smaller. | SPEC §7.2: records before every changed attempt; a missing record means max(origin prior, fresh decision without reductions). |
| G2 | **Major.** Modal integer memory was "enforced" while Kubernetes request-only memory was "not enforced", with conflicting rationales. | SPEC §2.3 S3: one test — whether the failure lands on the unit itself; Kubernetes no-limit memory listed explicitly. |
| G3 | **Major.** Flyte and Metaflow deploy-time sizing did not carry the origin prior. | flyte.md §6.3 and orchestrators.md §2.3: the deploy decision record holds it and is what later layers read. |
| G4 | **Major.** SkyPilot on VM backends never classifies failures. | launchers.md §1.5: reductions only where every candidate backend has an outcome path. |
| G5 | **Major.** `presized_submitters` was missing from slurm.md; Metaflow `@kubernetes` pods carried no marker. | slurm.md §4.2; orchestrators.md §2.2. |
| G6 | Airflow snippet still imported the unused hook; listener registration unstated. | orchestrators.md §3.1. |
| G7 | Caller ladders other than the AWS helpers had no lease, and nothing closed leases. | SPEC §7.2: every Caller ladder writes leases; the classifying component closes them; `lease_ttl`. |
| G8 | Sample kind undeclared for Vertex, Azure, Databricks, Container Insights; conflicting bounds. | SPEC §5.7 conflicting-bounds rule; managed-ml.md and aws.md declare each source. |
| G9 | CLI and milestone drift (`sbatch --watch`, reconcile commands, importer ownership, Cloud Batch milestone, `.pth` hook), floor headroom restated wrongly, stale Modal test, EKS marker location, Metaflow escalation attribution, unsigned Metaflow prior, recommend-mode wording, survey ladder column. | Fixed in place. |
