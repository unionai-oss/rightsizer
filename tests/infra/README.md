# tests/infra — local integration-test environments

Scripts that bring up the platforms rightsizer's integration tests run
against (docs/ARCHITECTURE.md §6). Every script is bash with
`set -euo pipefail`, idempotent, and supports `up` / `down` / `status`.

**Isolation rules** (enforced by the scripts):

- Everything they create is named `rsz-it*` (containers, k3d cluster, docker
  volumes, compose project, buildx builder, images `rsz-it/*`).
- Every `kubectl`/`helm` call uses a kubeconfig under `tests/infra/.state/`;
  `~/.kube/config` and the current context are never read or changed. Helm
  runs with `HELM_{CACHE,CONFIG,DATA}_HOME` under `.state/helm`, so your helm
  repos are not touched either.
- Nothing is pruned globally: no `docker system/volume/image prune`, no
  `ray stop`, no `k3d cluster delete` of anything not named `rsz-it*`.
- `tests/infra/.state/` holds all generated state (kubeconfigs, tokens,
  configs, built binaries) and is gitignored.

Quick look at everything: `tests/infra/all.sh status` (`all.sh down` stops
every environment; images and build caches are kept).

## Ports and names

| env | names | host ports (all bound to 127.0.0.1) | override |
|---|---|---|---|
| k3s | k3d cluster `rsz-it` (`k3d-rsz-it-server-0`, `k3d-rsz-it-agent-0`), network `k3d-rsz-it` | 16443 (k8s API), 19090 (Prometheus NodePort 30909) | `RSZ_IT_K3S_API_PORT`, `RSZ_IT_PROM_PORT` |
| k3s webhook-check | throwaway host HTTPS server | first free port from 18443 | `RSZ_IT_WEBHOOK_PORT` |
| flyte | container + volume `rsz-it-flyte` | 30080 (Flyte API/UI), 30002 (object store), 30500 (devbox image registry, container port 30000), 26443 (k8s API) | `RSZ_IT_FLYTE_K8S_API_PORT`, `RSZ_IT_FLYTE_REGISTRY_PORT` (30080/30002 are baked into the devbox) |
| build-wheels | volume `rsz-it-cargo-target-wheel` (+ the shared registry/git/rustup volumes) | none | `RSZ_IT_WHEEL_TARGET_VOLUME`, `RSZ_IT_MATURIN_IMAGE` |
| slurm | compose project `rsz-it-slurm`: `rsz-it-slurm-{mysql,slurmdbd,slurmctld,c1,c2}` | none (use `slurm.sh exec`) | |
| ray | processes under `/tmp/rsz-it-ray` | GCS first free from 26379, dashboard/State API from 28265, client from 20001, agent from 28365, metrics from 28080, workers 21000-21999 | `RSZ_IT_RAY_*_PORT` |
| build-linux | volumes `rsz-it-cargo-{registry,git,target}`, `rsz-it-rustup`; image `rsz-it/rsz:dev` | none | `RSZ_IT_TARGET_VOLUME`, `RSZ_IT_RSZ_IMAGE` |

## python-env.sh — repo `.venv`

```
tests/infra/python-env.sh up       # uv venv (Python 3.12) + install requirements-it.txt
tests/infra/python-env.sh relock   # re-resolve the top-level set, rewrite requirements-it.txt
tests/infra/python-env.sh status | down
```

Top-level set: pytest, pytest-asyncio, hypothesis, maturin, flyte 2.13.x,
ray[default], fsspec, s3fs, pyarrow, numpy, pandas, scikit-learn, lightgbm.
Exact pins in `requirements-it.txt`. `up` installs additively, so a
`maturin develop` build in the same venv survives. ~720 MB on disk.

## k3s.sh — K3s via k3d

```
tests/infra/k3s.sh up | down | status
tests/infra/k3s.sh webhook-check           # prove pods + apiserver reach a host HTTPS server
tests/infra/k3s.sh prometheus up|down|status
tests/infra/k3s.sh kuberay up|down
tests/infra/k3s.sh kueue up|down
tests/infra/k3s.sh crds up|down [jobset|trainer|pytorchjob|volcano|kuberay|all]...
tests/infra/k3s.sh import IMAGE...         # k3d image import (e.g. rsz-it/rsz:dev)
export KUBECONFIG=$(tests/infra/k3s.sh kubeconfig)
```

- 1 server + 1 agent, `rancher/k3s:v1.34.3-k3s1`, traefik/servicelb/metrics-server
  disabled, no k3d load balancer. Up in ~20 s.
- **Webhook served from the host:** register the webhook with
  `clientConfig.url: https://host.k3d.internal:<port>/...` and a `caBundle`
  for a cert whose SAN includes `host.k3d.internal`. k3d writes
  `host.k3d.internal` (Docker Desktop's host gateway, 192.168.65.254) into the
  nodes' `/etc/hosts` and CoreDNS, and Docker Desktop forwards it to services
  bound to the Mac's `127.0.0.1`. `webhook-check` demonstrates this end to
  end with `common/webhook_echo.py` (pod `curl` + a ValidatingWebhookConfiguration
  that the host denies) and removes everything afterwards.
- **Prometheus** (`prometheus-community/prometheus` chart 29.40.3, values in
  `k8s/prometheus-values.yaml`): Prometheus v3.15.1 server + kube-state-metrics
  v2.20.0; alertmanager, pushgateway and node-exporter off; kubelet cAdvisor
  scraped by the chart's `kubernetes-nodes-cadvisor` job
  (`container_memory_working_set_bytes`, ...). At `http://127.0.0.1:19090`.
- Add-on versions (pinned in the script) and in-node image sizes (compressed):

  | add-on | what is installed | images |
  |---|---|---|
  | prometheus | server + config-reloader + kube-state-metrics | 105 + 13 + 17 MB |
  | kuberay | kuberay-operator chart 1.7.1 (CRDs + operator) | 29 MB |
  | kueue | v0.20.4 `manifests.yaml` (controller + webhooks) | 64 MB |
  | crds jobset | v0.13.0 `manifests.yaml` (CRD + controller, so JobSets create Pods) | 33 MB |
  | crds trainer | Kubeflow Trainer v2.3.0 CRDs only (TrainJob, (Cluster)TrainingRuntime) | none |
  | crds pytorchjob | training-operator v1.9.4 PyTorchJob CRD only | none |
  | crds volcano | Volcano v1.16.0 CRDs only (Job, PodGroup, Queue) | none |

## flyte.sh — Flyte v2 devbox

```
tests/infra/flyte.sh up | down | status | smoke | clean
```

Runs `cr.flyte.org/flyteorg/flyte-devbox:latest` (already present locally,
1.9 GB) as container `rsz-it-flyte`, exactly the way `flyte start devbox` does
but isolated. **Do not use `flyte start devbox` on this machine**: in SDK 2.13
it hard-codes container/volume `flyte-devbox` (a `flyte-devbox` volume already
exists here), publishes host ports 6443, 30000-30003, 30080, 30081 (30000 and
30003 belong to the existing k3d `devbox` cluster), merges its kubeconfig into
`~/.kube/config` and switches the current context to `flyte-devbox`.

- SDK config: `.state/flyte-config.yaml` (endpoint `localhost:30080`,
  insecure, project `flytesnacks`, domain `development`, local builder).
  Use `flyte --config tests/infra/.state/flyte-config.yaml ...` or
  `flyte.init_from_config("tests/infra/.state/flyte-config.yaml")`.
- Devbox kubeconfig: `.state/rsz-it-flyte.kubeconfig` (API on 127.0.0.1:26443).
- 30080 and 30002 must map 1:1 (the devbox hands out
  `http://localhost:30002` signed URLs), so this devbox and a regular
  `flyte start devbox` cannot run at the same time.
- **Image registry.** The devbox's in-cluster registry (container port 30000)
  is published on `127.0.0.1:30500` (`RSZ_IT_FLYTE_REGISTRY_PORT`), the SDK
  config sets `image.registry: localhost:30500`, and a k3s
  `registries.yaml` mirror (`.state/flyte-registries.yaml`, mounted at
  `/etc/rancher/k3s/registries.yaml`) makes the node pull `localhost:30500/…`
  from its own registry. Without a configured registry, SDK 2.13's local
  builder *guesses* `localhost:30000` for a localhost endpoint and pushes
  there — which on this machine is the k3d `devbox` cluster's registry.
- `smoke` (`smoke/flyte_smoke.py`) runs tasks with different
  `flyte.Resources(memory=...)` shapes, records the pod requests/limits, runs
  an over-limit task to see the OOM path, and lists runs/actions with
  `flyte.remote`. Results in `.state/flyte-smoke-results.json`.
- First start pulls knative/kourier/traefik images inside the devbox and the
  task image `ghcr.io/flyteorg/flyte:py3.12-v2.13.x` on first run.

## build-wheels.sh — Linux wheels for task images

```
tests/infra/build-wheels.sh [all|native|runtime|status|clean]   # -> .state/wheels/*.whl
```

- `native`: `rightsizer` (abi3, manylinux2014 aarch64) built by maturin inside
  `ghcr.io/pyo3/maturin`, with the toolchain from `rust-toolchain.toml` in the
  shared `rsz-it-rustup` volume and its own target volume
  `rsz-it-cargo-target-wheel`. The Python sources are copied without host
  build products first, so a macOS `_core*.so` from `maturin develop` never
  lands in the wheel. Incremental rebuilds take ~40 s.
- `runtime`: `rightsizer-runtime` (pure Python) with `uv build`.
- Task images use them through `RightsizedEnvironment(auto_image=<dir>)` or
  `RIGHTSIZER_FLYTE_WHEELS=<dir>` (the SDK's `PythonWheels` layer).

## slurm.sh — Slurm in Docker

```
tests/infra/slurm.sh build | up | down | status | smoke | reconfigure | clean
tests/infra/slurm.sh exec [-u alice] sbatch --wrap 'hostname'
```

- Image `rsz-it/slurm:26-05-5-1` from `slurm/Dockerfile`: Rocky Linux 9,
  Slurm 26.05.5 built from the SchedMD git tag with Lua (`job_submit_lua.so`,
  `cli_filter_lua.so`), MySQL accounting, `cgroup_v2.so`, `serializer_json`.
  Derived from giovtorres/slurm-docker-cluster but without
  apptainer/lmod/spack. Built with a private buildx builder (`rsz-it-builder`)
  whose cache is removed after the build; ~2 min build, ~1 GB image
  (234 MB compressed) + `mariadb:11.8` (484 MB).
- Cluster `rsz-it`: mariadb + slurmdbd + slurmctld + nodes `c1`, `c2`
  (2 CPUs, 2048 MB each), partition `normal`. User `alice` (uid 1000) for
  submitting as a non-root user.
- cgroup v2 enforcement works on Docker Desktop (macOS): worker containers are
  privileged with a private cgroup namespace; the entrypoint moves its
  processes into `/sys/fs/cgroup/init` and delegates controllers.
  `ConstrainRAMSpace=yes`, `ProctrackType=proctrack/cgroup`,
  `TaskPlugin=task/cgroup,task/affinity`, `JobAcctGatherType=jobacct_gather/cgroup`,
  `SelectTypeParameters=CR_Core_Memory`.
- **Host mounts for later tests:**
  - `.state/slurm-etc-extra/` -> `/etc/slurm-extra` in every Slurm container.
    Every file there is symlinked into `/etc/slurm/` at start
    (`slurm.sh reconfigure` re-links and runs `scontrol reconfigure`). Seeded with
    pass-through `job_submit.lua` and `cli_filter.lua` and an empty
    `slurm-extra.conf` (included at the end of `slurm.conf`). slurmctld re-reads
    `job_submit.lua` when its mtime changes; `cli_filter.lua` is read on every
    `sbatch`/`srun`/`salloc`. Tables next to the scripts (e.g.
    `rightsizer-table.json`) work the same way.
  - `.state/bin/` -> `/opt/rsz/bin` (read-only, on `PATH`): put the linux `rsz`
    from `build-linux.sh` there. A bookworm-built binary needs glibc <= 2.34,
    which Rocky 9 has; use `build-linux.sh --musl` if a dependency pulls newer
    symbols.
- `smoke`: (1) temporary Lua scripts tag a job via `cli_filter` (comment) and
  `job_submit` (admin_comment), then are restored; (2) a job completes and is
  `COMPLETED` in `sacct --json`; (3) a job allocating 400 MB under `--mem=100M`
  ends `OUT_OF_MEMORY`.

## ray.sh — local Ray head

```
tests/infra/ray.sh up | down | status | smoke
source tests/infra/.state/ray.env    # RAY_ADDRESS, RAY_API_SERVER_ADDRESS, auth env
```

- Ray from the repo `.venv` (2.60.0), single node, 4 CPUs, temp dir
  `/tmp/rsz-it-ray` (unix-socket paths cannot live under `.state/`).
- Ray 2.60 enables token auth by default and would otherwise reuse
  `~/.ray/auth_token`; the head gets its own token in `.state/ray-auth-token`
  (`RAY_AUTH_MODE=token`, `RAY_AUTH_TOKEN_PATH`). Clients need both env vars
  (they are in `ray.env`).
- The dashboard is enabled (bound to 127.0.0.1) because the State API is
  served by it.
- `down` kills only processes whose command line contains `/tmp/rsz-it-ray`;
  it never calls `ray stop` (which stops every Ray on the machine).
- `smoke` (`smoke/ray_smoke.py`): remote functions with `memory=`; reads status
  back through `ray.util.state` and the node's Prometheus endpoint.

## build-linux.sh — linux `rsz` binary and image

```
tests/infra/build-linux.sh [--musl] [--src DIR] [--bin rsz] [--profile release] [-- cargo args]
tests/infra/build-linux.sh image     # rsz-it/rsz:dev from .state/bin/rsz
tests/infra/k3s.sh import rsz-it/rsz:dev
tests/infra/build-linux.sh status | clean
```

- Builds inside `rust:1.91-bookworm` (`--platform linux/arm64`) with the
  toolchain from `rust-toolchain.toml` (1.99) installed by rustup into the
  `rsz-it-rustup` volume (~1.2 GB, first run only). Cargo registry/git and
  `CARGO_TARGET_DIR` are volumes too, so rebuilds are incremental and nothing
  but the binary lands on the host: `.state/bin/rsz`.
- `--musl` builds `aarch64-unknown-linux-musl` (static; installs `musl-tools`
  in the throwaway container each run).
- `image` wraps the binary in `debian:bookworm-slim` (+ ca-certificates),
  user 65532, entrypoint `rsz`.
- Verified on a hello-world crate (built, re-built incrementally in < 1 s,
  ran on bookworm and Rocky 9, imported into k3d and ran as a pod).
