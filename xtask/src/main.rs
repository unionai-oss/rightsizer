//! Workspace tasks (`cargo xtask <task>`).
//!
//! - `deps-check`: dependency boundaries of ARCHITECTURE.md §3. Fails if any
//!   core-set crate (core, features, estimators, history, engine, plugin)
//!   transitively depends on kube, k8s-openapi, axum, pyo3, sqlx /
//!   tokio-postgres or an adapter crate; if `rightsizer-core` depends on
//!   anything beyond its allowed direct dependencies or on tokio; if
//!   `rightsizer-k8s --no-default-features` or the slim `rsz` build has kube;
//!   if `rightsizer-py` has kube or postgres.
//! - `schemas`: writes JSON Schemas (schemars) into `schemas/`.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result, bail};
use schemars::{JsonSchema, schema_for};

const CORE_SET: [&str; 6] = [
    "rightsizer-core",
    "rightsizer-features",
    "rightsizer-estimators",
    "rightsizer-history",
    "rightsizer-engine",
    "rightsizer-plugin",
];

const KUBE: [&str; 5] = ["kube", "kube-client", "kube-core", "kube-runtime", "k8s-openapi"];
const POSTGRES: [&str; 3] = ["sqlx", "sqlx-postgres", "tokio-postgres"];
const ADAPTERS: [&str; 4] = ["rightsizer-k8s", "rightsizer-slurm", "rightsizer-cli", "rightsizer-py"];

/// Direct dependencies `rightsizer-core` may have (ARCHITECTURE.md §3).
const CORE_ALLOWED: [&str; 8] = ["serde", "serde_json", "ulid", "chrono", "sha2", "thiserror", "enumflags2", "schemars"];

fn main() -> Result<()> {
    let task = std::env::args().nth(1).unwrap_or_default();
    match task.as_str() {
        "deps-check" => deps_check(),
        "schemas" => schemas(),
        _ => {
            eprintln!("usage: cargo xtask <deps-check|schemas>");
            std::process::exit(2);
        }
    }
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).parent().expect("xtask lives in the workspace").to_path_buf()
}

/// Package names in `cargo tree` for the given arguments (normal + build edges).
fn tree(args: &[&str]) -> Result<BTreeSet<String>> {
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".into());
    let out = Command::new(cargo)
        .current_dir(workspace_root())
        .args(["tree", "--offline", "-e", "normal,build", "--prefix", "none", "-f", "{p}"])
        .args(args)
        .output()
        .context("running cargo tree")?;
    if !out.status.success() {
        bail!("cargo tree {} failed:\n{}", args.join(" "), String::from_utf8_lossy(&out.stderr));
    }
    Ok(String::from_utf8_lossy(&out.stdout).lines().filter_map(|l| l.split_whitespace().next()).map(str::to_string).collect())
}

fn check(label: &str, args: &[&str], forbidden: &[&str], failures: &mut Vec<String>) -> Result<()> {
    let names = tree(args)?;
    let bad: Vec<&str> = forbidden.iter().copied().filter(|f| names.contains(*f)).collect();
    if bad.is_empty() {
        println!("ok    {label}");
    } else {
        println!("FAIL  {label}: depends on {}", bad.join(", "));
        failures.push(format!("{label}: {}", bad.join(", ")));
    }
    Ok(())
}

fn deps_check() -> Result<()> {
    let mut failures = Vec::new();
    let mut core_forbidden: Vec<&str> = KUBE.iter().chain(&POSTGRES).chain(&ADAPTERS).copied().collect();
    core_forbidden.extend(["axum", "pyo3", "pyo3-ffi"]);
    for c in CORE_SET {
        let mut forbidden = core_forbidden.clone();
        if c == "rightsizer-core" {
            forbidden.extend(["tokio", "object_store", "reqwest", "rusqlite"]);
        }
        check(&format!("{c} (default features)"), &["-p", c], &forbidden, &mut failures)?;
    }
    // rightsizer-core's direct dependencies.
    let direct = tree(&["-p", "rightsizer-core", "--depth", "1", "--all-features"])?;
    let extra: Vec<&String> = direct.iter().filter(|d| *d != "rightsizer-core" && !CORE_ALLOWED.contains(&d.as_str())).collect();
    if extra.is_empty() {
        println!("ok    rightsizer-core direct dependencies ⊆ {{{}}}", CORE_ALLOWED.join(", "));
    } else {
        let list: Vec<&str> = extra.iter().map(|s| s.as_str()).collect();
        println!("FAIL  rightsizer-core has extra direct dependencies: {}", list.join(", "));
        failures.push("rightsizer-core direct deps".into());
    }
    check("rightsizer-k8s --no-default-features", &["-p", "rightsizer-k8s", "--no-default-features"], &KUBE, &mut failures)?;
    let mut py_forbidden: Vec<&str> = KUBE.to_vec();
    py_forbidden.extend(POSTGRES);
    py_forbidden.extend(["axum", "rightsizer-k8s", "rightsizer-slurm"]);
    check("rightsizer-py", &["-p", "rightsizer-py"], &py_forbidden, &mut failures)?;
    check(
        "rsz --no-default-features --features slurm",
        &["-p", "rightsizer-cli", "--no-default-features", "--features", "slurm"],
        &KUBE,
        &mut failures,
    )?;
    if failures.is_empty() {
        println!("deps-check: all boundaries hold");
        Ok(())
    } else {
        bail!("deps-check failed: {}", failures.join("; "))
    }
}

fn write_schema<T: JsonSchema>(dir: &Path, name: &str) -> Result<()> {
    let schema = schema_for!(T);
    let path = dir.join(format!("{name}.json"));
    let mut text = serde_json::to_string_pretty(&schema)?;
    text.push('\n');
    std::fs::write(&path, text).with_context(|| format!("writing {}", path.display()))?;
    println!("wrote {}", path.strip_prefix(workspace_root()).unwrap_or(&path).display());
    Ok(())
}

fn schemas() -> Result<()> {
    use rightsizer_core as c;
    use rightsizer_plugin as p;
    use rightsizer_plugin::protocol as pr;

    let root = workspace_root().join("schemas");
    let core = root.join("core").join("1");
    let plugin = root.join("plugin").join("1");
    std::fs::create_dir_all(&core)?;
    std::fs::create_dir_all(&plugin)?;

    write_schema::<c::WorkloadContext>(&core, "workload_context")?;
    write_schema::<c::Estimate>(&core, "estimate")?;
    write_schema::<c::Decision>(&core, "decision")?;
    write_schema::<c::RunRecord>(&core, "run_record")?;
    write_schema::<c::Prior>(&core, "prior")?;
    write_schema::<c::ResourceSpec>(&core, "resource_spec")?;
    write_schema::<c::Constraints>(&core, "constraints")?;
    write_schema::<c::PlatformRef>(&core, "platform_ref")?;
    write_schema::<c::PlatformState>(&core, "platform_state")?;
    write_schema::<c::AttemptContext>(&core, "attempt_context")?;
    write_schema::<c::AttemptRecord>(&core, "attempt_record")?;
    write_schema::<c::ContextSeed>(&core, "context_seed")?;
    write_schema::<c::Grid>(&core, "grid")?;
    write_schema::<c::breaker::BreakerScope>(&core, "breaker_scope")?;
    write_schema::<c::breaker::Trip>(&core, "breaker_trip")?;
    write_schema::<c::breaker::Reset>(&core, "breaker_reset")?;
    write_schema::<c::breaker::BreakerObjects>(&core, "breaker_objects")?;
    write_schema::<c::breaker::Control>(&core, "control")?;

    write_schema::<pr::Request>(&plugin, "request")?;
    write_schema::<pr::Response>(&plugin, "response")?;
    write_schema::<pr::InitializeParams>(&plugin, "initialize_params")?;
    write_schema::<pr::InitializeResult>(&plugin, "initialize_result")?;
    write_schema::<pr::ExtractParams>(&plugin, "extract_params")?;
    write_schema::<p::Extracted>(&plugin, "extract_result")?;
    write_schema::<pr::RenderParams>(&plugin, "render_params")?;
    write_schema::<p::Patch>(&plugin, "render_result")?;
    write_schema::<pr::ValidateParams>(&plugin, "validate_params")?;
    write_schema::<pr::ValidateResult>(&plugin, "validate_result")?;
    write_schema::<pr::ReferenceParams>(&plugin, "reference_params")?;
    write_schema::<c::PlatformRef>(&plugin, "reference_result")?;
    write_schema::<pr::StateParams>(&plugin, "state_params")?;
    write_schema::<c::PlatformState>(&plugin, "state_result")?;
    write_schema::<pr::FetchOutcomesParams>(&plugin, "fetch_outcomes_params")?;
    write_schema::<pr::FetchOutcomesResult>(&plugin, "fetch_outcomes_result")?;
    write_schema::<pr::DecideParams>(&plugin, "decide_params")?;
    write_schema::<pr::DecideResult>(&plugin, "decide_result")?;
    write_schema::<pr::OutcomeParams>(&plugin, "outcome_params")?;
    write_schema::<pr::OutcomeResult>(&plugin, "outcome_result")?;
    write_schema::<pr::StatusResult>(&plugin, "status_result")?;
    write_schema::<pr::CancelParams>(&plugin, "cancel_params")?;
    Ok(())
}
