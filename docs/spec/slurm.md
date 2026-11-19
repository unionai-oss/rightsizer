# rightsizer for Slurm

Status: **draft v2**. Core concepts: [SPEC.md](SPEC.md); plugin contract:
[plugins.md](plugins.md). Slurm references are to the SchedMD documentation
(`cli_filter_plugins.html`, `job_submit_plugins.html`, `sbatch.html`,
`sacct.html`, `gres.html`).

## 0. Day one

```bash
rsz slurm install --cluster gpu-a && systemctl enable --now rightsizer   # daemon on login nodes (recommend mode)
rsz history import slurm --since 30d                                     # sacct backfill
rsz slurm sbatch train.sh                                                 # prints the diff and explanation, submits unchanged
rsz shadow --tenant slurm://gpu-a/mlteam --since 14d
# then: mode = "enforce" for the account in rightsizer.toml, and CliFilterPlugins=cli_filter/lua
```

## 1. Surfaces

| surface | runs where | role | retry |
|---|---|---|---|
| `rsz slurm sbatch [sbatch args] script.sh` | login node, as the user | parse, decide, rewrite, call `sbatch`; `--watch` resubmits on `OUT_OF_MEMORY` | `same_size = 0`; a ladder with `--watch` |
| `contrib/slurm/cli_filter.lua` | inside `sbatch`/`salloc`/`srun`, as the user | transparent client-side sizing (enforce for `sbatch`; recommend for `salloc`/`srun`) | `same_size = 0` |
| `contrib/slurm/job_submit.lua` | inside `slurmctld`, as SlurmUser | verification, attribution, and (M4) history-based sizing for jobs that bypassed the client side | `same_size = 0` |
| `rsz history import slurm` | anywhere with `sacct` | outcomes | — |

The **daemon** (`rsz serve --socket /run/rightsizer.sock`) runs on each
login node as a dedicated service user. It holds the estimators, the
connection to the shared history store (postgres or object store — SPEC
§8.1), and the signing keys (with key ids, two active during rotation:
`rsz slurm rotate-key`). It runs under systemd with `WatchdogSec`. `cli_filter` and the `sbatch` wrapper talk to it;
they never hold the key.

`cli_filter` is a convenience, not a control: SchedMD documents that it
"must not be relied upon for security purposes" (users can point at their
own `slurm.conf`). Nothing that *enforces* depends on it.

## 2. Client side: `cli_filter.lua`

```lua
-- contrib/slurm/cli_filter.lua (sketch)
function slurm_cli_pre_submit(options, offset)
  if os.getenv("SLURM_JOB_ID") then return slurm.SUCCESS end   -- srun inside an allocation: never resize
  if daemon_recently_failed() then return slurm.SUCCESS end   -- negative cache: skip the daemon for 60 s after a timeout
  local ok, resp = pcall(rightsizer_decide, options, offset)   -- JSON-RPC "decide" over the daemon's Unix socket, 1 s deadline
  if not ok or resp == nil then mark_daemon_failed(); return slurm.SUCCESS end   -- S1: unchanged
  for k, v in pairs(resp.patch) do options[k] = v end          -- Patch.Options: long option names, string values, resource keys only
  -- keep the user's export semantics: nil means ALL, so never replace it with a bare variable list
  options["export"] = (options["export"] or "ALL") .. ",RIGHTSIZER_TOKEN=" .. resp.token
  if resp.mode == "recommend" then io.stderr:write(resp.summary .. "\n") end
  return slurm.SUCCESS
end
```

- `offset` identifies the component of a **heterogeneous job**; each component
  is a unit.
- A **job array** (`--array`) gets one decision, applied to every array task
  (the array is a group).
- The `options` table is a proxy (`pairs(options)` yields nothing); the shim
  reads the specific long option names it needs.
- The daemon authenticates the caller by `SO_PEERCRED` and derives the tenant
  `slurm://{cluster}/{account}` from the uid and the requested `--account`
  (validated against the user's associations).
- The token travels in the job's environment (`--export`), which `sbatch`
  forwards to the controller in `job_desc.environment`. `salloc`/`srun` jobs
  carry no environment to the controller, so their resources could not be
  attributed to rightsizer (S5). `cli_filter` therefore **enforces only for
  `sbatch`**. For `salloc` and `srun` it is recommend-only and prints the
  summary on stderr.

## 3. Rewriting rules (both the wrapper and `cli_filter`)

| axis | rule |
|---|---|
| memory | Keep the user's form. `--mem` (per node, default unit MiB, suffixes K/M/G/T = 1024ⁿ) → `--mem`; `--mem-per-cpu` → per-CPU value = decision ÷ CPUs per node; `--mem-per-gpu` → decision ÷ GPUs per node. The three are mutually exclusive; rightsizer never adds a second one. **`--mem=0` (the whole node) is never changed.** No memory option given → the partition's `DefMemPerCPU`/`DefMemPerNode` is the prior, marked `defaulted`. |
| CPU | `--cpus-per-task`, keeping `--ntasks`/`--ntasks-per-node` unchanged (those are program topology). |
| GPU | Keep the user's flag (`--gres=gpu[:type]:n`, `--gpus`, `--gpus-per-node`, `--gpus-per-task`); never mix typed and untyped forms. Count is held for multi-task or multi-node jobs (distributed). A type change (opt-in) maps through the site's gres-type table and honors `--constraint`/features. |
| ephemeral storage | `--tmp`: held in v1 (SPEC §7.4); reported in advisories. |
| walltime | `--time` is **recommend-only** in v1 (a duration estimate is reported; changing it affects backfill priority). |

The wrapper edits `#SBATCH` lines in place (`Patch::TextEdits`, comments
and order preserved) when the option came from the script, and edits argv
when it came from the command line (argv overrides the script, as in
`sbatch`).

## 4. Server side: `job_submit.lua`

Runs inside `slurmctld`, serially, under the controller's locks: it performs
**no I/O per job** and no network calls.

### 4.1 Verification and attribution

The daemon signs each client-side decision:

```
token = base64( decision_id ‖ expiry ‖ HMAC-SHA256(key, uid ‖ account ‖ sha256(script) ‖ canonical(resources) ‖ decision_id ‖ expiry) )
```

`job_submit.lua` loads the key from a file readable only by SlurmUser **when
the script is loaded** (not per job), and for each job:

1. finds `RIGHTSIZER_TOKEN` in `job_desc.environment` (batch jobs);
2. verifies the HMAC against `submit_uid`, `job_desc.account`, the script
   hash, and the job's *actual* resource fields — `min_mem_per_node` /
   `min_mem_per_cpu` (never `pn_min_memory`, which Lua mishandles as a 64-bit
   value; at most one of the two is non-nil, and both are nil when the job gave
   no memory option), `cpus_per_task`, `tres_per_node`
   / `tres_per_job` / `tres_per_task` and `mem_per_tres` (the modern GPU
   fields), `pn_min_tmp_disk`;
3. writes `rightsizer:<decision_id>:verified` (or `:user` when absent or
   invalid) into `job_desc.admin_comment` — never the user's `comment`.

A valid token means "these resources are rightsizer's decision", which is what
S5 attribution needs. An invalid or missing token never causes a rejection
and never causes resources to change on the strength of anything the user
supplied.

### 4.2 Server-side sizing for bypassers (optional, M4)

Admins who want sizing for jobs that did not go through the client side
enable a **decision table**. The daemon periodically writes
`rightsizer-table.json` next to `job_submit.lua` (write to a temp file, then
`rename`, then `touch job_submit.lua`: `slurmctld` re-reads the script when the
**script file's** mtime is newer than its load time, checked on each
submission, and keeps the previous script if the new one fails to load). The
script loads the table at load time. The table:

- holds input-independent, history-based decisions (SPEC §7.4 `scale` with the
  p90 input size), only where S3(b) holds;
- is keyed by `(account, key)` where `key` is the `#RIGHTSIZER key=` directive
  or an explicit `--job-name` — no script hashing under the controller lock;
- is capped at 10,000 entries (most-submitted keys first);
- applies only to untokenized batch jobs, under the rewriting rules of §3;
- skips jobs from the users in `[slurm] presized_submitters`: submitters such
  as a SkyPilot API server that have already sized the job through their own
  rightsizer integration (launchers.md §1.4).

## 5. Outcomes: `sacct`

```
sacct --json -S <since> --allusers \
      --format=JobID,JobName,Account,User,State,ExitCode,Elapsed,Timelimit,NNodes,NTasks,
               ReqTRES,AllocTRES,ReqMem,MaxRSS,TRESUsageInMax,TRESUsageInTot,TRESUsageInAve,AdminComment
```

- **Per-node memory demand**: Slurm's `MaxRSS` / `TRESUsageInMax` is the
  largest *single task*, while `--mem` is per node. Per-node demand is taken as
  `TRESUsageInTot(mem) / NNodes` for the step with the largest value, and as
  `MaxRSS × tasks-per-node` (`AtLeast`) when `Tot` is unavailable. The `.batch`,
  `.extern` and numbered steps are aggregated; the job's demand is the maximum
  over steps.
- **Resolution**: samples arrive every `JobAcctGatherFrequency` (default 30 s);
  `resolution_s` is set from the cluster config, and short spikes can be missed
  (whether a given Slurm version's `jobacct_gather/cgroup` reports cgroup v2
  `memory.peak` rather than sampled usage is checked per version in M3; until
  then records are treated as sampled).
- **GPU**: with `AccountingStorageTRES=gres/gpu` and NVML/RSMI, `gres/gpumem`
  and `gres/gpuutil` appear in `TRESUsageIn*` (no data for MIG devices).
- **Status**: `OUT_OF_MEMORY` → `OomKilled` (`memory_peak = AtLeast(limit)`);
  `TIMEOUT` → `Timeout`; `PREEMPTED` → `Preempted`; `NODE_FAIL` → `Failed`
  (not attributed to rightsizer).
- **Key**: from `AdminComment` (decision id → key) when present; otherwise
  from the job name or the normalized script digest (§6).

## 6. Keys

- Tenant: `slurm://{cluster}/{account}`.
- Suffix, in order: a `#RIGHTSIZER key=<name>` directive in the script;
  `--job-name` if given explicitly; otherwise the **normalized script digest**
  — comments, blank lines, and every resource-bearing `#SBATCH` directive
  removed, so resizing a job does not give it a new identity. `--wrap`
  commands are digested after replacing numbers that look like timestamps or
  run ids with placeholders.

## 7. Platform model

- **Shapes:** from `scontrol show node` / `sinfo` (CPUs, `RealMemory`, gres),
  per partition; prices from config.
- **Quota:** association/QOS limits (`GrpTRES`, `MaxTRESPerJob`) from `sacctmgr`
  (cached, 5 min); partition `MaxMemPerNode`/`MaxMemPerCPU`.
- **Enforcement:** `memory_enforced = ConstrainRAMSpace=yes` (cgroup). When
  memory is not enforced, an under-ask does not fail the job but endangers
  co-located jobs: reductions need S3(b) evidence and the cost model adds the
  neighbour-risk premium (SPEC §2.3, §7.2) — the same rule as Ray.
- **Walltime:** `--time`; CPU stretch beyond it is a `Timeout` in the cost model.
- **CPU window:** `JobAcctGatherFrequency`.

## 8. Throughput

A submission loop calling `sbatch` 10,000 times pays one daemon round trip per
call (p99 target 20 ms with a memoized decision). If the daemon is down, the
negative cache bounds the cost to one 1 s timeout per minute per login node.

## 9. Installation

```bash
rsz slurm install --cluster gpu-a \
    --cli-filter /etc/slurm/cli_filter.lua \
    --job-submit /etc/slurm/job_submit.lua    # writes the scripts, prints the slurm.conf lines:
# CliFilterPlugins=cli_filter/lua
# JobSubmitPlugins=lua
rsz slurm keygen --out /etc/slurm/rightsizer.key   # 0600 SlurmUser; the daemon's copy is 0600 rightsizer
```

The Lua scripts depend only on the Lua standard library plus a vendored
pure-Lua SHA-256/HMAC module and a small JSON module.
