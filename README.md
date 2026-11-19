# rightsizer

Right-size AI workloads before they run: choose memory, CPU and GPU requests
from a workload's code, its inputs' metadata, its platform and its history.
It recommends, or enforces under an explicit safety contract (SPEC §2.3:
never fail a submission because of rightsizer, never add a limit, reductions
only with evidence and an escalation path, breakers that trip on caused
failures).

- **Rust core** (`rightsizer-core` … `rightsizer-engine`), the **`rsz` CLI**
  (Kubernetes, Slurm, daemon), and a **Python package** (`rightsizer`, a
  native `_core` module plus a typed API) for Python-native platforms.
- One history store for every surface: a local directory, sqlite, postgres,
  or an object store (S3 and S3-compatible, GCS, Azure).

The design is in [`docs/spec/`](docs/spec/SPEC.md); the code layout and its
boundaries in [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md); decisions,
deviations and open items in
[`docs/IMPLEMENTATION-NOTES.md`](docs/IMPLEMENTATION-NOTES.md).

**Status:** pre-release (`0.1.0.dev0`, milestones M0–M3 implemented; no
published binaries or wheels yet). See [Status and limitations](#status-and-limitations).

## What works today

| platform | entry points | tested on |
|---|---|---|
| **Kubernetes** | `rsz recommend` / `explain` / `apply` on manifests (Job, CronJob, Pod, JobSet, PyTorchJob, TrainJob, RayJob/RayCluster, Volcano Job); admission webhook + leader-elected watcher (`rsz webhook serve`), Prometheus importer (`rsz history import k8s`), optional node agent, `rsz apply --watch` OOM ladder, `rsz k8s restore`; Helm chart with alerts and a dashboard | unit + golden tests; k3d (K3s v1.34) suite 32/32 |
| **Slurm** | `rsz recommend -p slurm job.sbatch`; `rsz serve` daemon + `cli_filter.lua` / `job_submit.lua` with HMAC-signed tokens (`rsz slurm install`, `keygen`, `rotate-key`); `rsz slurm sbatch` wrapper; `sacct` importer (`rsz history import slurm`) | unit + Lua cross-check tests; Slurm 26.05 in Docker (tampering, key rotation, 1,000-job throughput) |
| **Flyte v2** | `rightsizer.flyte`: task / environment / action / run levels (`RightsizedEnvironment`, `rightsize`, `sized`, `with_runcontext`), child-side measurement, deploy-time sizing and `reconcile`, `rsz flyte run|deploy|reconcile`, `rsz history import flyte` backfill | unit tests with a fake controller; local runs; Flyte devbox partially (see limitations) |
| **Ray** | `rightsizer.ray`: `remote`, `rightsize`, `sized` (actors), `data.map_batches`, job submission (`rsz ray submit`), in-task measurement | unit tests; local Ray 2.60 head (10k-task fan-out); Linux container for Ray's OOM path |
| **Any Python code** | `rightsizer.recommend` / `outcome` / `decide_group`, `rightsizer.Engine` | unit tests |
| **Other schedulers** | out-of-process plugins (`rsz-plugin-<kind>`, JSON-RPC `rightsizer-plugin/1` over stdio) and shims talking to `rsz serve` over a Unix socket or HTTP; `rsz plugin list` / `check NAME` | conformance suite; `examples/plugins/rsz-plugin-nomad-demo` end to end |
| **Model serving** | `rightsizer serve` (Estimator Protocol v1) for model-factory models via vLLM, SGLang, transformers or an OpenAI-compatible endpoint; `rightsizer.serve` to wrap any callable; `rightsizer models convert` (trees) | unit tests; vLLM on a tiny model; SGLang mocked |

Not implemented yet (spec milestones A, M4–M7): `rsz audit`, `rsz suggest`,
`rsz mcp`, managed clouds (SageMaker, Batch, Modal, Vertex AI, Azure ML,
Databricks), SkyPilot, dstack, Metaflow, Airflow. The commands exist and
report "not implemented yet".

## Install

Each platform installs only what it needs (ARCHITECTURE §1): Kubernetes and
Slurm users never need Python; Flyte users never get Ray or `kube-rs`, and
the reverse.

| you use | install |
|---|---|
| Kubernetes, Slurm, plugins | the `rsz` binary only: `cargo install --path crates/rightsizer-cli` (add `--features s3` for an S3 history store; `--no-default-features --features slurm` for a Slurm-only build without `kube-rs`) |
| Kubernetes webhook | the Helm chart `deploy/helm/rightsizer` (image `ghcr.io/unionai-oss/rsz`) |
| Python API | `pip install rightsizer` |
| Flyte v2 | `pip install 'rightsizer[flyte]'` |
| Ray | `pip install 'rightsizer[ray]'` |
| Model serving | `pip install 'rightsizer[serve]'` (NumPy, no torch); `'rightsizer[torch]'` for the in-process transformers backend; `'rightsizer[trees]'` for `rightsizer models convert` |
| Estimator Protocol client in `rsz` | `cargo install --path crates/rightsizer-cli --features remote` |
| History writes through fsspec from Python workers | `pip install 'rightsizer[fsspec]'` |

`import rightsizer` imports neither Flyte nor Ray. `rsz flyte …`,
`rsz ray …` and `rsz history import flyte` delegate to the Python package
when it is installed and print the install hint otherwise.

From source (until wheels are published):

```bash
cargo build -p rightsizer-cli                       # target/debug/rsz
python -m venv .venv && .venv/bin/pip install maturin -e python/rightsizer-runtime
cd python/rightsizer && ../../.venv/bin/maturin develop   # rightsizer + rightsizer._core
```

## Quickstarts

Every command below was run against a local sqlite or `file://` store on
2026-11-19; the output shown is from those runs (decision ids differ).

### Kubernetes from the CLI

```toml
# ./rightsizer.toml
[history]
uri = "sqlite:///tmp/rsz/history.db"     # or file:///…, postgres://…, s3://… (see Stores)

[k8s]
cluster = "dev"                          # tenant prefix k8s://dev/{namespace}
```

```yaml
# train.yaml
apiVersion: batch/v1
kind: Job
metadata: {name: train, namespace: ml}
spec:
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: main
          image: ghcr.io/acme/train:1.0
          resources:
            requests: {cpu: "4", memory: 16Gi}
            limits: {memory: 32Gi}
```

```console
$ rsz recommend -p k8s train.yaml
k8s://dev/ml/job/train/main [spec.template.spec.containers[main]]  mode recommend  estimator prior@1
                prior    recommended    applied
  memory         16Gi           16Gi       16Gi
  cpu               4              4          4

$ for p in 5Gi 5.5Gi 6Gi 5.2Gi 5.8Gi; do
    rsz outcome k8s://dev/ml/job/train/main --status succeeded --memory-peak $p --duration 20m
  done
$ rsz history show k8s://dev/ml/job/train/main
$ rsz recommend -p k8s train.yaml
k8s://dev/ml/job/train/main [spec.template.spec.containers[main]]  mode recommend  estimator rules-sized@1
                prior    recommended    applied
  memory         16Gi           12Gi       16Gi
  cpu               4              3          4
  expected $0.0657 per run, p_fail 0.000
  held memory: s3_not_met (no ladder; 0 of 0 measured successes fit (need 3))
$ rsz explain -p k8s train.yaml           # distributions, bounds, cost table, why it held
```

The reduction is recommended but held: manual outcomes are `unverified`, and
a reduction needs trusted measured successes (S3). The webhook's watcher, the
Prometheus importer and the Slurm `sacct` importer write verified records.

`rsz apply` writes the decision into the manifest (`--diff` to preview). It
signs the original requests into `rightsizer.io/prior` so `rsz k8s restore`
can undo it, so it needs a signing key:

```bash
rsz webhook rotate-key --key-file ./signing.key
RIGHTSIZER__K8S__SIGNING_KEY_FILE=$PWD/signing.key rsz apply --diff train.yaml
```

### Kubernetes webhook

```bash
helm install rightsizer deploy/helm/rightsizer -n rightsizer --create-namespace \
  --set cluster=prod-us-east-1 \
  --set history.postgres.url="$PG_URL" \
  --set watcher.prometheusUrl=http://prometheus-server.monitoring.svc
kubectl label ns ml rightsizer.io/enabled=true                 # recommend (annotations only)
kubectl label ns ml rightsizer.io/mode=enforce --overwrite     # after shadowing: rsz shadow
```

`failurePolicy: Ignore`, a 1 s internal deadline under the 2 s webhook
timeout, and fail-open alerts. `--set nodeAgent.enabled=true` adds the cgroup
v2 node agent (bearer-token auth and a NetworkPolicy). See
[the chart README](deploy/helm/rightsizer/README.md).

### Slurm

```bash
cat >> rightsizer.toml <<'EOF'
[slurm]
cluster = "hpc"
EOF
```

```console
$ rsz recommend -p slurm train.sbatch
slurm://hpc/none/train [job]  mode recommend  estimator prior@1
                prior    recommended    applied
  memory         64Gi           64Gi       64Gi
  cpu               8              8          8
```

On a cluster: `rsz slurm keygen --out /etc/slurm/rightsizer.key`, run
`rsz serve` on the login/controller host, install the Lua plugins with
`rsz slurm install --cluster C --cli-filter PATH --job-submit PATH`, and
import accounting with `rsz history import slurm --since 30d` (see
[slurm.md](docs/spec/slurm.md)).

### Python

```python
import rightsizer as rs

rs.configure(store="file:///tmp/rsz-quickstart")
ctx = rs.Context(key="train", prior=rs.Resources(cpu=4, memory="16Gi"),
                 inputs=[rs.Input.sized("train.parquet", 1 << 30, "parquet")])
d = rs.recommend(ctx)                  # never raises; falls back to the prior (S1)
print(d.key, d.mode.value, d.recommended.requests.memory / 2**30, d.applied.requests.memory / 2**30)
# python://alice/train recommend 12.0 16.0
rs.outcome(d, status="succeeded", memory_peak="5Gi", duration_s=1200)
rs.flush()
```

```console
$ rsz --store file:///tmp/rsz-quickstart history show python://alice/train
STARTED                INVOCATION                   ATT STATUS        REQ mem     PEAK mem      DUR SOURCE       TRUST
2026-11-19T18:56:08Z   01M7TKSPE3S2RDZZRPHHWSDTHB     1 succeeded        16Gi        5.0Gi    1200s self_report  unverified
```

(`alice` is `RIGHTSIZER_TENANT`, else the user name.)

### Flyte v2

```python
import flyte
import rightsizer as rs

env = rs.flyte.RightsizedEnvironment(name="train", resources=flyte.Resources(cpu=2, memory="4Gi"))

@env.task
async def train(n: int) -> int:
    return sum(range(n))

@env.task
async def main(n: int = 1000) -> int:
    return await train(n)

if __name__ == "__main__":
    flyte.init()                          # local run; flyte.init_from_config() for a cluster
    print(flyte.run(main, n=10).outputs())
```

On a cluster each child call is sized by its parent action and measured in
the child; the store defaults to `{scheme}://{bucket}/rightsizer` from the
run's raw-data path (or `RIGHTSIZER__HISTORY__URI`). Run-level policy:
`rsz flyte run --mode enforce -- …`; deploy-time sizing:
`rsz flyte deploy FILE ENV`; backfill from `flyte.remote`:
`rsz history import flyte --project P --domain D --since 30d` (re-imports are
idempotent: records already stored are reported as duplicates).

### Ray

```python
import ray
import rightsizer as rs

ray.init()

@rs.ray.remote(num_cpus=1, memory=2 * 2**30, key="embed")   # sizing + in-task measurement
def embed(n: int) -> int:
    return len(bytearray(n << 20))

ray.get([embed.remote(64) for _ in range(4)])
```

Run with `RIGHTSIZER__HISTORY__URI=file:///…` (or an object store) and the
store gets one decision and one run record per task. Jobs:
`rsz ray submit -- python job.py`.

### Plugins and the daemon

```bash
export PATH=$PWD/examples/plugins:$PATH   # plugins are found as rsz-plugin-<kind> on PATH or [plugins.<kind>]
rsz plugin list
rsz plugin check nomad-demo               # conformance: handshake, determinism, idempotence, validate, crash, timeout
rsz serve --socket /run/rightsizer.sock   # rightsizer-plugin/1 for shims
```

See [plugins.md](docs/spec/plugins.md) and `tests/integration/plugin/run.sh`.

### Model serving

```bash
pip install 'rightsizer[serve]'
rightsizer serve --model unionai/rightsize-decision-qwen3-1.7b --backend vllm --launch --port 8095
```

```toml
# rightsizer.toml: the served model at the top of the estimator chain
[estimator]
chain = ["model", "rules-sized", "history-peak", "prior"]
[estimator.members.model]
kind = "remote"
url = "http://127.0.0.1:8095"
```

The Python engine has the Estimator Protocol client built in; `rsz` needs
`--features remote`. An unreachable or slow model falls through to the next
member (the chain above ends at the prior). See [models.md](docs/spec/models.md)
and `python -m rightsizer.serve --help`.

## Stores and credentials

`history.uri` (or `--store`, or `RIGHTSIZER__HISTORY__URI`):
`file:///dir` or a path, `sqlite:///path.db`, `postgres://…` (rsz),
`s3://bucket/prefix?endpoint=…&region=…`, `gs://…`, `az://…`. Enforce needs a
durable store shared by every writer (object store or postgres).

Object-store credentials come from the ambient credential chain, or are
passed explicitly — never exported into the process environment, where your
own boto3/s3fs clients would pick them up:

```toml
[history.storage_options]           # object_store keys; values are redacted in `rsz config show`
endpoint = "http://minio:9000"
access_key_id = "…"
secret_access_key = "…"
```

```python
rs.configure(store="s3://bucket/rightsizer",
             storage_options={"key": "…", "secret": "…", "client_kwargs": {"endpoint_url": "http://minio:9000"}})
```

## Status and limitations

Milestone exit criteria (SPEC §15) as of 2026-11-19:

| milestone | exit criterion | status |
|---|---|---|
| M0 core | scorer costs within 1 % on k8s-2; `rules-sized` regret within 1 point; < 5 ms/decision | costs: 5,532/5,532 within 1 % — on a **synthetic** k8s-2-shaped fixture (the real rows were unreachable); regret within 1 point after the one deliberate scale rule; ≈ 0.36 ms/decision |
| M1 Flyte | 0 caused failures over 1,000 enforced invocations; 10k `flyte.map` < 2 ms/call memoized | memoized path 0.15 ms/call at 10k-way; devbox run **incomplete** (4 rounds × 90 enforced invocations, 0 failures, before the host disk filled) |
| M2 K8s | 2 weeks shadow on a real cluster with join ratio ≥ 0.95; webhook p99 < 100 ms | k3d suite 32/32, webhook p99 15 ms (debug build); **no real-cluster shadow yet** |
| M3 plugins | Slurm test cluster with HMAC verification and rotation; Ray 10k fan-out < 5 ms/task | Slurm in Docker: verification, tampering and rotation pass; Ray 0.6 ms/task added |

Other limitations worth knowing before use (full, current list:
[Known limitations](docs/IMPLEMENTATION-NOTES.md#known-limitations--open-items)):

- Writes from other processes (an import, the watcher, Ray workers) become
  visible to a running engine only after the history/memo TTLs (10 min).
- A sqlite store always demotes `enforce` (it is never considered shared).
- `rsz recommend -p slurm` has no platform model (node shapes) outside the
  daemon.
- GPU models: converted trees have no GPU axis in the Rust evaluator yet;
  the pointer model needs `send_source = true` to be useful.
- Integration suites need Docker (k3d, Slurm, Flyte devbox) and were last
  run on 2026-11-19 before this round's changes; see
  [ARCHITECTURE §6](docs/ARCHITECTURE.md#6-testing-strategy) for how to run
  each suite.

## Development

```bash
cargo test --workspace --exclude rightsizer-py --no-fail-fast
cargo clippy --workspace --exclude rightsizer-py --all-targets -- -D warnings
cd python/rightsizer && ../../.venv/bin/maturin develop && cd -
.venv/bin/python -m pytest -q python/rightsizer-runtime/tests
.venv/bin/python -m pytest -q python/rightsizer/tests
helm lint deploy/helm/rightsizer --set cluster=c
```

Every suite, with its environment gate: [ARCHITECTURE §6](docs/ARCHITECTURE.md#testing-commands).

License: Apache-2.0.
