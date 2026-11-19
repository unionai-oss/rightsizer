# rightsizer — implementation architecture (M0–M3)

This document turns [the spec](spec/SPEC.md) into code layout and contracts.
Where it deviates from the spec it says so and why. The spec stays
normative for behavior; this file is normative for **boundaries**.

## 1. Principle: meet each platform where it is

A user who rightsizes Kubernetes Jobs must not need Python, Flyte, Ray or a
Slurm install. A Flyte user must not need `kube-rs` or Ray. A Ray user must
not need Flyte. Concretely:

- The **core** (`rightsizer-core`, `-features`, `-estimators`, `-history`,
  `-engine`, `-plugin`) knows nothing about any platform. It never depends on
  an adapter crate. A test (`xtask deps-check`, run in CI) fails if it does.
- Each **Rust adapter** is its own crate (`rightsizer-k8s`,
  `rightsizer-slurm`). Heavy, optional pieces sit behind cargo features
  (`rightsizer-k8s/webhook`, `/cluster`).
- The **`rsz` binary** composes adapters through cargo features
  (`k8s`, `slurm`, `postgres`; all on by default). A slim build
  (`--no-default-features --features slurm`) contains no `kube-rs`.
- **Python adapters** live in the `rightsizer` wheel as subpackages
  (`rightsizer.flyte`, `rightsizer.ray`) that import their platform lazily.
  `import rightsizer` imports neither Flyte nor Ray; each platform is an
  extra (`rightsizer[flyte]`, `rightsizer[ray]`). Model tooling follows the
  same rule: `rightsizer.models` / `rightsizer.serve` need `rightsizer[serve]`
  (NumPy, no torch; `[torch]` for the reference backend), `rightsizer.export`
  needs `rightsizer[export]`. A test
  (`python/rightsizer/tests/isolation`) imports every subpackage in an
  environment without the others installed.
- **Store credentials are passed, never exported.** Every surface hands the
  history store its object-store options explicitly (`[history.storage_options]`,
  `_core.Engine(storage_options=…)`, `RunRecordWriter(storage_options=…)`);
  nothing writes `AWS_*`/`GOOGLE_*`/`AZURE_*` into the process environment,
  where the user's own clients would pick them up (SPEC §10.3).
- **Anything else** is an out-of-process plugin (`rsz-plugin-<kind>`, JSON-RPC
  `rightsizer-plugin/1` over stdio) or a shim that calls the daemon
  (`rsz serve`), so new platforms never touch the core.
- `rsz <platform> ...` subcommands for Python-only platforms (`rsz flyte`,
  `rsz ray`, `rsz history import flyte`) **delegate** to
  `python -m rightsizer.<platform>.cli` when that module is importable, and
  print an install hint otherwise. The Rust binary never links Python.

## 2. Repository layout

```
Cargo.toml                     workspace; shared dependency versions; incremental = false
rust-toolchain.toml            1.99
crates/
  rightsizer-core/             pure, sync, no I/O: data model (types/), units, grids, normalization,
                               cost model, constraints, decision rule (decide.rs, decide/), safety (S1–S5),
                               breaker/mode logic, policy, stats
  rightsizer-features/         input metadata readers (readers/: parquet, arrow IPC, safetensors, HF, …;
                               bounded range reads), code tokens, digest, context building, privacy,
                               SSRF guard; I/O via `object_store` (features: s3, gcs, azure, http)
  rightsizer-estimators/       Estimator trait, rules-sized, history-peak, prior, gbt (trees/1),
                               novelty, features-v1, memo, chain + build-from-config,
                               remote client (Estimator Protocol v1, feature `remote`)
  rightsizer-history/          HistoryStore trait; object store (local FS, S3, GCS, Azure), sqlite,
                               postgres (feature `postgres`), memory; cache, buffer, Importer (bulk
                               dedup), factory (URI + StoreOptions → backend), compaction, export
  rightsizer-engine/           orchestration: config (one `Config` for every surface), store view,
                               chain, memo + single-flight + limiter + budget, mode/breaker
                               resolution, decision logging, outcome ingestion, sweep, replay, report
  rightsizer-plugin/           Adapter / PlatformModel / OutcomeSource traits, Native, Unit, Patch,
                               rightsizer-plugin/1 protocol types, stdio plugin client, conformance suite
  rightsizer-k8s/              K8s adapter (extract/render/validate/edit: pure); feature `webhook`
                               (admit.rs, webhook.rs: axum + rustls, 16 MiB review limit), feature
                               `cluster` (cluster/: kube view, watcher, leader election, Prometheus
                               importer, node agent with bearer auth, prefetch), restore
  rightsizer-slurm/            Slurm adapter (sbatch parsing, #SBATCH text edits, option tables),
                               platform model, HMAC tokens, sacct importer, sbatch wrapper, service
  rightsizer-cli/              `rsz`: main.rs (recommend/apply/explain/shadow/history/outcome/breaker/
                               control/config), daemon.rs (`rsz serve`), k8s.rs (`webhook`, `watcher`,
                               `node-agent`, `k8s restore`, `history import k8s`), slurm_cmd.rs
                               (`slurm …`, `history import slurm`), flyte.rs / ray.rs (delegate to
                               Python), plugin_cmd.rs, adapters.rs, output.rs
  rightsizer-py/               PyO3 module `rightsizer._core` (core + features + estimators +
                               history[object-store, sqlite] + engine; features clouds, http, remote);
                               no kube-rs, no postgres
python/
  rightsizer-runtime/          pure Python, stdlib only: wire types, policy model, measurement
                               (cgroup v1/v2, rusage, NVML), RunRecord writer (local / fsspec),
                               store options (storage.py), Flyte and Ray child-side code
  rightsizer/                  maturin project: `rightsizer._core` + typed API (`rightsizer`),
                               `rightsizer.flyte` [flyte], `rightsizer.ray` [ray],
                               `rightsizer.models` + `rightsizer.serve` [serve] (models.md),
                               `rightsizer.export` [export] (trees exporter), `rightsizer.plugin`
contrib/slurm/                 cli_filter.lua, job_submit.lua, vendored sha256/hmac/json Lua
deploy/helm/rightsizer/        Helm chart (webhook + watcher, optional node agent + NetworkPolicy),
                               PrometheusRule, dashboard
examples/plugins/              rsz-plugin-nomad-demo (an out-of-process plugin + fixtures)
schemas/                       generated JSON Schemas (normative wire formats)
tests/
  fixtures/wire/               cross-language JSON fixtures
  infra/                       scripts that bring up local test platforms (k3d/K3s, Flyte devbox,
                               Slurm in Docker, Ray) and tear them down; Linux builds
  integration/                 per-platform integration suites (k8s, slurm, flyte, ray, plugin)
xtask/                         deps-check, schema generation
```

**Deviation:** the spec lists no `rightsizer-engine`. The decision *rule* stays
in `rightsizer-core` (pure, sync, < 5 ms), but something async must own the
store, estimator chain, memo, single-flight and budgets for the CLI, the
daemon, the webhook and the Python module alike. Putting that in `core` would
give the pure crate a tokio dependency; putting it in `rightsizer-cli` would
make the Python module duplicate it. Hence a separate crate.

## 3. Dependency rules

```
                         ┌──────────── history ◄─────────────┐
                         ▼                ▲                  │
core ◄── features ◄── estimators ◄── engine ◄── plugin ◄── {k8s, slurm} ◄── cli
                                        ▲          (plugin → history too; k8s → features too)
                                        └── py  (+ history[object-store, sqlite], features, estimators)
```

Edges as in the `Cargo.toml`s (every crate also depends on `core`):

| crate | depends on (workspace) |
|---|---|
| core | — |
| features | core |
| estimators | core, features |
| history | core |
| engine | core, features, estimators, history |
| plugin | core, engine, history |
| k8s | core, plugin, engine†, history†, features‡ |
| slurm | core, plugin, engine, history |
| cli | core, engine, estimators, history, plugin, k8s (feature `k8s`), slurm (feature `slurm`) |
| py | core, features, estimators, history, engine |

† feature `engine` (on with `webhook` / `cluster`); ‡ feature `cluster`.

- `core` depends only on `serde`, `serde_json`, `ulid`, `chrono`, `sha2`,
  `thiserror`, `enumflags2`, `schemars` (optional). No tokio, no I/O.
- `plugin` depends on `core`, `engine` (the conformance suite drives the
  engine) and `history` (importers write through `HistoryStore`); adapter
  crates depend on `plugin`.
- Nothing in the core set depends on `kube`, `k8s-openapi`, `axum`, `pyo3`,
  `tokio-postgres`, or an adapter crate. `cargo xtask deps-check` enforces it
  with `cargo tree`.
- Cloud object stores are cargo features end to end: `history`/`features`
  (`s3`, `gcs`, `azure`) ← `engine` ← `cli` (`s3`) / `py` (`clouds`, on by
  default in the wheel). The default `rsz` build has local FS, sqlite and
  postgres stores; `cargo build -p rightsizer-cli --features s3` adds S3.

## 4. Wire contracts

**JSON is the contract between languages.** Every struct in spec §5 is a
`serde` struct with stable `snake_case` JSON and a `"v"` field, and its JSON
Schema is generated into `schemas/` (`cargo xtask schemas`). Python never
re-implements the decision logic; it builds JSON contexts, calls `_core`, and
reads JSON decisions. Cross-language tests round-trip fixtures in
`tests/fixtures/wire/` through both sides.

Conventions:

- Quantities on the wire are integers: bytes (`u64`) and millicores (`u32`).
  Human-readable quantity strings exist only at platform boundaries
  (`rightsizer_core::units`).
- Enums are `snake_case` strings; tagged enums use `{"kind": ...}`.
- Timestamps are RFC 3339 UTC. Ids are ULIDs (Crockford base32 strings).
- Workload keys are strings `{platform}://{tenant}/{suffix}`.

### 4.1 Engine API (used by CLI, daemon, webhook, Python)

```rust
pub struct DecideRequest {
    pub context: ContextInput,        // either a full WorkloadContext or a ContextSeed + key + prior + platform
    pub constraints: Constraints,     // spec §7.4 (pins, holds, floor, ceiling, allowed shapes)
    pub state: PlatformState,         // live shapes, request model, prices, quota, floors
    pub policy: Policy,               // effective mode request, risk, estimator chain name, flags
    pub budget_ms: Option<u32>,       // hard deadline; expiry → Prior{reason="budget"}
    pub dry_run: bool,                // decide but do not record
}
impl Engine {
    pub async fn decide(&self, req: DecideRequest) -> Decision;            // never fails (S1)
    pub async fn decide_group(&self, reqs: Vec<DecideRequest>) -> Vec<Decision>;
    pub async fn outcome(&self, rec: RunRecord) -> Result<()>;            // buffered, retried
}
```

The Python module exposes the same operations JSON-in / JSON-out
(`_core.decide(request_json) -> decision_json`, `_core.outcome(record_json)`,
`_core.Engine(config_json)`), and `rightsizer` wraps them in typed dataclasses.

### 4.2 Plugin protocol

`rightsizer-plugin/1`: JSON-RPC 2.0, newline-delimited, over stdio (core →
plugin) or a Unix socket / HTTP (shim → daemon). Methods and handshake as in
plugins.md §3. Types live in `rightsizer-plugin::protocol`.

## 5. Configuration

`rightsizer.toml`, found as in spec §12.1, every key overridable with
`RIGHTSIZER__SECTION__KEY`. One `Config` struct in `rightsizer-engine::config`
is shared by every surface; the Python module receives the same TOML/env
resolution through `_core`.

## 6. Testing strategy

| layer | what | where |
|---|---|---|
| unit | every module; property tests (`proptest`) for units, grids, normalization, monotonicity of the decision rule | `crates/*/src`, `crates/*/tests` |
| safety | S1–S5 as named tests (`s1_inert_on_estimator_panic`, `s2_never_adds_limit`, …) | `crates/rightsizer-engine/tests/safety.rs` and per adapter |
| parity | decision rule vs the tuner's scorer on its oracle demands; `rules-sized` regret | `crates/rightsizer-engine/tests/parity.rs`, fixtures from the tuner |
| wire | JSON fixtures round-trip Rust ⇄ Python; schemas validate fixtures | `tests/fixtures/wire`, `python/*/tests` |
| perf | < 5 ms per decision; memoized Flyte/Ray calls; webhook p99 | `criterion` benches + integration timings |
| integration | K3s (k3d) webhook + watcher; Flyte devbox; Slurm in Docker with Lua plugins; local Ray and KubeRay | `tests/integration/<platform>`, brought up by `tests/infra` |

Integration environments are **isolated from anything already on the
machine**: they use their own cluster names (`rsz-it-*`), their own
kubeconfig files, and never touch existing contexts or containers.

### Testing: commands

Run from the repository root. Python commands use the repo venv (`.venv`,
created by `tests/infra/python-env.sh`). Suites behind an environment gate
skip (or print `skipped`) without it.

| suite | command | gate / needs |
|---|---|---|
| Rust unit + crate integration tests | `cargo test --workspace --exclude rightsizer-py --no-fail-fast` | none (postgres / S3 conformance cases skip silently) |
| object-store option mapping (S3/GCS/Azure builders, no network) | `cargo test -p rightsizer-history --features s3,gcs,azure --lib factory` | none |
| `_core` Rust tests | `cargo test -p rightsizer-py` | a Python 3.10+ to link against |
| lints | `cargo clippy --workspace --exclude rightsizer-py --all-targets -- -D warnings` and `cargo clippy -p rightsizer-py -- -D warnings` | none |
| dependency isolation | `cargo xtask deps-check` | none |
| schemas up to date | `cargo xtask schemas` (then `git diff --exit-code schemas/`) | none |
| build the Python extension | `cd python/rightsizer && ../../.venv/bin/maturin develop` | maturin in `.venv` |
| Python unit (runtime) | `.venv/bin/python -m pytest -q python/rightsizer-runtime/tests` | none (stdlib; fsspec tests skip without it) |
| Python unit (typed API, Flyte/Ray/models/serve/export units) | `.venv/bin/python -m pytest -q python/rightsizer/tests` | the extension built; Flyte/Ray/torch/skops/huggingface_hub-dependent tests skip when missing; `RIGHTSIZER_RUST_PARITY=1` adds the Rust evaluator parity check. Run the two roots separately: both have `test_wire.py` / `test_child.py`, which collide in one pytest session |
| Python import isolation | `RIGHTSIZER_ISOLATION_WHEEL=… .venv/bin/python -m pytest -q python/rightsizer/tests/isolation` | a built wheel and `uv` (throwaway venvs per extra) |
| Helm chart | `helm lint deploy/helm/rightsizer --set cluster=c` and `tests/integration/k8s/helm_test.sh` | `helm`; `pyyaml` in `.venv` |
| postgres backend | `RSZ_IT_POSTGRES_URL=postgres://… cargo test -p rightsizer-history --features postgres` | a postgres |
| S3 / MinIO backend | `RSZ_IT_S3_URL=s3://bucket/prefix?endpoint=… cargo test -p rightsizer-history --features s3 --test conformance` | an S3-compatible store |
| network readers (HF, HTTP, S3) | `cargo test -p rightsizer-features --features http,s3 -- --ignored` | network |
| plugin + daemon end to end | `tests/integration/plugin/run.sh` | host only (builds `target/debug/rsz`) |
| Kubernetes (k3d) | `RSZ_IT_K8S=1 tests/integration/k8s/run.sh setup all` | Docker, k3d, kubectl, helm |
| Slurm (Docker cluster) | `tests/infra/slurm.sh up && tests/infra/build-linux.sh -- -p rightsizer-cli && RSZ_IT_SLURM=1 tests/integration/slurm/run.sh`; Rust side: `RSZ_IT_SLURM=1 cargo test -p rightsizer-slurm --test e2e_slurm_cluster -- --nocapture` | Docker |
| Flyte (devbox) | `RSZ_IT_FLYTE=1 tests/integration/flyte/run.sh` | Docker (devbox image, Linux wheels) |
| Ray (local head) | `tests/infra/ray.sh up && RSZ_IT_RAY=1 .venv/bin/python -m pytest tests/integration/ray -v -s` | Ray in `.venv` |
| infra status / teardown | `tests/infra/all.sh status` / `tests/infra/all.sh down` | see `tests/infra/README.md` |

## 7. Milestone map

| milestone | crates / packages |
|---|---|
| M0 core | core, features, estimators (rules-sized, history-peak, prior, gbt), history (object-store, sqlite), engine, cli (`recommend`, `explain`, `shadow`, `history`, `breaker`, `control`, `config`) |
| M1 Flyte | rightsizer-py, python/rightsizer-runtime, python/rightsizer (`rightsizer.flyte`), `rsz flyte`, `rsz history import flyte` |
| M2 K8s | rightsizer-k8s (adapter, webhook, watcher, Prometheus importer, restore), history postgres, Helm chart, alerts |
| M3 plugins | rightsizer-plugin protocol + daemon, rightsizer-slurm + contrib/slurm Lua, `rightsizer.ray`, KubeRay/TrainJob/Volcano in rightsizer-k8s, node agent, Arrow IPC + HF readers |
