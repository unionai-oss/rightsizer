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
  extra (`rightsizer[flyte]`, `rightsizer[ray]`). A test imports every
  subpackage in an environment without the others installed.
- **Anything else** is an out-of-process plugin (`rsz-plugin-<kind>`, JSON-RPC
  `rightsizer-plugin/1` over stdio) or a shim that calls the daemon
  (`rsz serve`), so new platforms never touch the core.
- `rsz <platform> ...` subcommands for Python-only platforms (`rsz flyte`,
  `rsz ray`, `rsz history import flyte`) **delegate** to
  `python -m rightsizer.<platform>.cli` when that module is importable, and
  print an install hint otherwise. The Rust binary never links Python.

## 2. Repository layout

```
Cargo.toml                     workspace; shared dependency versions
rust-toolchain.toml            1.99
crates/
  rightsizer-core/             pure, sync, no I/O: data model, units, grids, normalization,
                               cost model, constraints, decision rule, breaker/mode logic, policy
  rightsizer-features/         input metadata readers (bounded range reads), code tokens, digest,
                               feature bucket; I/O via `object_store` (features: s3, gcs, azure, http)
  rightsizer-estimators/       Estimator trait, rules-sized, history-peak, prior, gbt (trees/1),
                               remote client (Estimator Protocol v1, feature `remote`), chain
  rightsizer-history/          HistoryStore trait; object-store (local FS, S3), sqlite, postgres
                               (feature `postgres`); compaction, pointers, breakers, control, attempts
  rightsizer-engine/           orchestration: config, context building from a ContextSeed, chain,
                               memo + single-flight + limiter + budget, mode/breaker resolution,
                               decision logging, outcome ingestion, shadow/audit replay
  rightsizer-plugin/           Adapter / PlatformModel / OutcomeSource traits, Native, Unit, Patch,
                               rightsizer-plugin/1 protocol types, stdio plugin client, conformance suite
  rightsizer-k8s/              K8s adapter (manifest extract/render/validate; pure);
                               features `webhook` (axum + rustls), `cluster` (kube-rs watcher,
                               restore, Prometheus importer)
  rightsizer-slurm/            Slurm adapter (sbatch parsing, #SBATCH text edits, option tables),
                               HMAC tokens, sacct importer, sbatch wrapper
  rightsizer-cli/              `rsz`: CLI, daemon (`serve`), plugin discovery
  rightsizer-py/               PyO3 module `rightsizer._core` (core + features + estimators +
                               history[object-store, sqlite] + engine); no kube-rs, no postgres
python/
  rightsizer-runtime/          pure Python: policy model, measurement (cgroup v1/v2, rusage, NVML),
                               RunRecord writer (fsspec / local), Flyte and Ray child-side code
  rightsizer/                  maturin project: `rightsizer._core`, `rightsizer.flyte`,
                               `rightsizer.ray`, `rightsizer.plugin`, `rightsizer.serve`,
                               `rightsizer.export`
contrib/slurm/                 cli_filter.lua, job_submit.lua, vendored sha256/hmac/json Lua
deploy/helm/rightsizer/        Helm chart (webhook + watcher), PrometheusRule, dashboard
schemas/                       generated JSON Schemas (normative wire formats)
tests/
  infra/                       scripts that bring up local test platforms (k3d/K3s, Flyte devbox,
                               Slurm in Docker, Ray) and tear them down
  integration/                 per-platform integration suites that use tests/infra
xtask/                         deps-check, schema generation, benches orchestration
```

**Deviation:** the spec lists no `rightsizer-engine`. The decision *rule* stays
in `rightsizer-core` (pure, sync, < 5 ms), but something async must own the
store, estimator chain, memo, single-flight and budgets for the CLI, the
daemon, the webhook and the Python module alike. Putting that in `core` would
give the pure crate a tokio dependency; putting it in `rightsizer-cli` would
make the Python module duplicate it. Hence a separate crate.

## 3. Dependency rules

```
core  ←  features  ←  estimators  ←  engine  ←  plugin  ←  {k8s, slurm}  ←  cli
  ↑         ↑            ↑            ↑                                     
  └──────── history ─────┴────────────┘            py ← engine (+ history[object-store, sqlite])
```

- `core` depends only on `serde`, `serde_json`, `ulid`, `chrono`, `sha2`,
  `thiserror`, `enumflags2`, `schemars` (optional). No tokio, no I/O.
- `plugin` depends on `core` and `engine` (the conformance suite drives the
  engine); adapter crates depend on `plugin`.
- Nothing in the core set depends on `kube`, `k8s-openapi`, `axum`, `pyo3`,
  `sqlx`/`tokio-postgres`, or an adapter crate. `xtask deps-check` enforces it
  with `cargo tree`.

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

## 7. Milestone map

| milestone | crates / packages |
|---|---|
| M0 core | core, features, estimators (rules-sized, history-peak, prior, gbt), history (object-store, sqlite), engine, cli (`recommend`, `explain`, `shadow`, `history`, `breaker`, `control`, `config`) |
| M1 Flyte | rightsizer-py, python/rightsizer-runtime, python/rightsizer (`rightsizer.flyte`), `rsz flyte`, `rsz history import flyte` |
| M2 K8s | rightsizer-k8s (adapter, webhook, watcher, Prometheus importer, restore), history postgres, Helm chart, alerts |
| M3 plugins | rightsizer-plugin protocol + daemon, rightsizer-slurm + contrib/slurm Lua, `rightsizer.ray`, KubeRay/TrainJob/Volcano in rightsizer-k8s, node agent, Arrow IPC + HF readers |
