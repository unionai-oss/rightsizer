//! The plugin conformance suite (`rsz plugin check NAME`, plugins.md §4).
//!
//! Fixtures live in a directory (see [`crate::discovery::fixtures_dir`]):
//!
//! ```text
//! valid/*.json     {"native": <Native>, "env": <AdapterEnv>?}   (or a bare Native)
//! invalid/*.json   {"native": <Native>, "patch": <Patch>}        validate must reject these
//! ```
//!
//! Checks (each pass/fail with a detail line):
//!
//! 1. `handshake`: the process starts, `initialize` negotiates a version the
//!    core speaks, the kind matches, and `extract`/`render`/`validate` are
//!    advertised.
//! 2. `extract_determinism`: `extract` of every valid fixture succeeds and
//!    returns the same JSON on two calls.
//! 3. `render_idempotence`: decisions that change every unit's memory
//!    render to a non-empty patch; the core applies it (text edits, JSON
//!    Patch or an option merge) and rendering the same decisions on the
//!    rendered submission is `Patch::None`.
//! 4. `validate`: every invalid fixture is rejected, and the plugin accepts
//!    the patches it rendered in check 3.
//! 5. `crash_leaves_unchanged`: the plugin is killed (`SIGKILL`) while a
//!    `decide` through the core waits on it; the decide returns
//!    `Patch::None` (no error escapes) and the plugin is then restarted.
//! 6. `timeout_leaves_unchanged`: the plugin is frozen (`SIGSTOP`) and a
//!    `decide` with a 300 ms budget returns `Patch::None` within the budget
//!    (+ slack); the plugin is resumed (`SIGCONT`) or restarted.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use rightsizer_core::{Bytes, ResourceSpec};
use rightsizer_engine::{Config, Engine};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::client::{PluginAdapter, PluginClient, PluginSpec};
use crate::compose::{ComposeOptions, decide_native};
use crate::{Adapter, AdapterEnv, JsonPatchOp, Native, Patch, PluginError, Result, UnitId, apply_text_edits};

/// One valid fixture.
#[derive(Clone, Debug)]
pub struct ValidFixture {
    /// File name.
    pub name: String,
    /// Submission.
    pub native: Native,
    /// Environment.
    pub env: AdapterEnv,
}

/// One invalid fixture.
#[derive(Clone, Debug)]
pub struct InvalidFixture {
    /// File name.
    pub name: String,
    /// Submission.
    pub native: Native,
    /// A patch the plugin must reject.
    pub patch: Patch,
}

/// Fixtures.
#[derive(Clone, Debug, Default)]
pub struct Fixtures {
    /// Valid submissions.
    pub valid: Vec<ValidFixture>,
    /// Invalid patches.
    pub invalid: Vec<InvalidFixture>,
}

#[derive(Deserialize)]
struct ValidFile {
    native: Native,
    #[serde(default)]
    env: AdapterEnv,
}

#[derive(Deserialize)]
struct InvalidFile {
    native: Native,
    patch: Patch,
}

fn json_files(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = std::fs::read_dir(dir).map(|rd| rd.flatten().map(|e| e.path()).filter(|p| p.extension().is_some_and(|x| x == "json")).collect()).unwrap_or_default();
    v.sort();
    v
}

/// Load a fixtures directory.
pub fn load_fixtures(dir: &Path) -> std::result::Result<Fixtures, String> {
    if !dir.is_dir() {
        return Err(format!("{}: not a directory", dir.display()));
    }
    let mut f = Fixtures::default();
    for p in json_files(&dir.join("valid")) {
        let text = std::fs::read_to_string(&p).map_err(|e| format!("{}: {e}", p.display()))?;
        let v: Value = serde_json::from_str(&text).map_err(|e| format!("{}: {e}", p.display()))?;
        let (native, env) = if v.get("native").is_some() {
            let vf: ValidFile = serde_json::from_value(v).map_err(|e| format!("{}: {e}", p.display()))?;
            (vf.native, vf.env)
        } else {
            (serde_json::from_value(v).map_err(|e| format!("{}: {e}", p.display()))?, AdapterEnv::default())
        };
        f.valid.push(ValidFixture { name: name_of(&p), native, env });
    }
    for p in json_files(&dir.join("invalid")) {
        let text = std::fs::read_to_string(&p).map_err(|e| format!("{}: {e}", p.display()))?;
        let i: InvalidFile = serde_json::from_str(&text).map_err(|e| format!("{}: {e}", p.display()))?;
        f.invalid.push(InvalidFixture { name: name_of(&p), native: i.native, patch: i.patch });
    }
    if f.valid.is_empty() {
        return Err(format!("{}: no valid/*.json fixtures", dir.display()));
    }
    Ok(f)
}

fn name_of(p: &Path) -> String {
    p.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default()
}

/// One check's result.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Check {
    /// Name.
    pub name: String,
    /// Passed.
    pub passed: bool,
    /// Detail.
    pub detail: String,
}

/// The suite's report.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Report {
    /// Plugin kind.
    pub kind: String,
    /// Executable.
    pub path: String,
    /// Checks in order.
    pub checks: Vec<Check>,
}

impl Report {
    /// Every check passed.
    pub fn passed(&self) -> bool {
        self.checks.iter().all(|c| c.passed)
    }

    /// Text form.
    pub fn text(&self) -> String {
        let mut s = format!("rsz plugin check {} ({})\n", self.kind, self.path);
        for c in &self.checks {
            s.push_str(&format!("  [{}] {:<26} {}\n", if c.passed { "PASS" } else { "FAIL" }, c.name, c.detail));
        }
        let n = self.checks.iter().filter(|c| c.passed).count();
        s.push_str(&format!("{}/{} checks passed: {}\n", n, self.checks.len(), if self.passed() { "conformant" } else { "NOT conformant" }));
        s
    }
}

fn check(name: &str, r: std::result::Result<String, String>) -> Check {
    match r {
        Ok(d) => Check { name: name.into(), passed: true, detail: d },
        Err(d) => Check { name: name.into(), passed: false, detail: d },
    }
}

// ── generic patch application ───────────────────────────────────────────

fn unescape(seg: &str) -> String {
    seg.replace("~1", "/").replace("~0", "~")
}

fn pointer_parent<'a>(root: &'a mut Value, path: &str) -> std::result::Result<(&'a mut Value, String), String> {
    if !path.starts_with('/') {
        return Err(format!("bad pointer {path:?}"));
    }
    let mut segs: Vec<String> = path[1..].split('/').map(unescape).collect();
    let last = segs.pop().unwrap_or_default();
    let mut cur = root;
    for s in segs {
        cur = match cur {
            Value::Object(m) => m.get_mut(&s).ok_or_else(|| format!("{path}: no {s:?}"))?,
            Value::Array(a) => a.get_mut(s.parse::<usize>().map_err(|_| format!("{path}: bad index {s:?}"))?).ok_or_else(|| format!("{path}: index {s} out of range"))?,
            _ => return Err(format!("{path}: not a container at {s:?}")),
        };
    }
    Ok((cur, last))
}

fn ptr_get(root: &Value, path: &str) -> std::result::Result<Value, String> {
    root.pointer(path).cloned().ok_or_else(|| format!("{path}: not found"))
}

fn ptr_add(root: &mut Value, path: &str, v: Value) -> std::result::Result<(), String> {
    if path.is_empty() {
        *root = v;
        return Ok(());
    }
    let (parent, last) = pointer_parent(root, path)?;
    match parent {
        Value::Object(m) => {
            m.insert(last, v);
            Ok(())
        }
        Value::Array(a) => {
            if last == "-" {
                a.push(v);
                return Ok(());
            }
            let i: usize = last.parse().map_err(|_| format!("{path}: bad index"))?;
            if i > a.len() {
                return Err(format!("{path}: index out of range"));
            }
            a.insert(i, v);
            Ok(())
        }
        _ => Err(format!("{path}: parent is not a container")),
    }
}

fn ptr_remove(root: &mut Value, path: &str) -> std::result::Result<Value, String> {
    let (parent, last) = pointer_parent(root, path)?;
    match parent {
        Value::Object(m) => m.remove(&last).ok_or_else(|| format!("{path}: not found")),
        Value::Array(a) => {
            let i: usize = last.parse().map_err(|_| format!("{path}: bad index"))?;
            if i >= a.len() {
                return Err(format!("{path}: index out of range"));
            }
            Ok(a.remove(i))
        }
        _ => Err(format!("{path}: parent is not a container")),
    }
}

/// Apply RFC 6902 operations.
pub fn apply_json_patch(doc: &Value, ops: &[JsonPatchOp]) -> std::result::Result<Value, String> {
    let mut d = doc.clone();
    for op in ops {
        match op {
            JsonPatchOp::Add { path, value } => ptr_add(&mut d, path, value.clone())?,
            JsonPatchOp::Remove { path } => {
                ptr_remove(&mut d, path)?;
            }
            JsonPatchOp::Replace { path, value } => {
                ptr_get(&d, path)?;
                if path.is_empty() {
                    d = value.clone();
                } else {
                    *d.pointer_mut(path).ok_or_else(|| format!("{path}: not found"))? = value.clone();
                }
            }
            JsonPatchOp::Move { from, path } => {
                let v = ptr_remove(&mut d, from)?;
                ptr_add(&mut d, path, v)?;
            }
            JsonPatchOp::Copy { from, path } => {
                let v = ptr_get(&d, from)?;
                ptr_add(&mut d, path, v)?;
            }
            JsonPatchOp::Test { path, value } => {
                if &ptr_get(&d, path)? != value {
                    return Err(format!("test failed at {path}"));
                }
            }
        }
    }
    Ok(d)
}

/// Apply a patch to a native submission, as far as the core can do so
/// generically: text edits on documents, JSON Patch on objects (and JSON
/// documents), and option tables merged into the object's `options` map
/// (or its top level).
pub fn apply_generic(native: &Native, patch: &Patch) -> Result<Native> {
    let bad = |e: String| PluginError::InvalidPatch(e);
    match (native, patch) {
        (_, p) if p.is_none() => Ok(native.clone()),
        (Native::Document { media_type, bytes }, Patch::TextEdits { edits }) => {
            let text = std::str::from_utf8(bytes).map_err(|_| bad("document is not UTF-8".into()))?;
            Ok(Native::Document { media_type: media_type.clone(), bytes: apply_text_edits(text, edits)?.into_bytes() })
        }
        (Native::Document { media_type, bytes }, Patch::JsonPatch { ops }) => {
            let v: Value = serde_json::from_slice(bytes).map_err(|e| bad(format!("document is not JSON: {e}")))?;
            let out = apply_json_patch(&v, ops).map_err(bad)?;
            Ok(Native::Document { media_type: media_type.clone(), bytes: serde_json::to_vec_pretty(&out).map_err(|e| bad(e.to_string()))? })
        }
        (Native::Object { value }, Patch::JsonPatch { ops }) => Ok(Native::Object { value: apply_json_patch(value, ops).map_err(bad)? }),
        (Native::Object { value }, Patch::Options { options }) => {
            let mut v = value.clone();
            let target = match v.get_mut("options") {
                Some(Value::Object(_)) => v.get_mut("options"),
                _ => Some(&mut v),
            };
            if let Some(Value::Object(m)) = target {
                for (k, x) in options {
                    m.insert(k.clone(), x.clone());
                }
            } else {
                return Err(bad("options patch on a non-object".into()));
            }
            Ok(Native::Object { value: v })
        }
        (Native::ApiCall { service, operation, params }, Patch::JsonPatch { ops }) => {
            Ok(Native::ApiCall { service: service.clone(), operation: operation.clone(), params: apply_json_patch(params, ops).map_err(bad)? })
        }
        (_, p) => Err(bad(format!("the core cannot apply a {} patch to this native form", serde_json::to_value(p).ok().and_then(|v| v["kind"].as_str().map(String::from)).unwrap_or_default()))),
    }
}

/// Decisions that change each unit's memory (half the prior, at least
/// 64 MiB; 512 MiB when the prior has none).
fn probe_specs(units: &[crate::Unit]) -> Vec<(UnitId, ResourceSpec)> {
    units
        .iter()
        .map(|u| {
            let mut s = u.prior.spec.clone();
            let mib = 1u64 << 20;
            let new = match s.requests.memory {
                Some(m) => Bytes(((m.0 / 2) / mib).max(64) * mib),
                None => Bytes(512 * mib),
            };
            let new = if Some(new) == s.requests.memory { Bytes(new.0 * 2) } else { new };
            s.requests.memory = Some(new);
            if s.limits.memory.is_some() {
                s.limits.memory = Some(new);
            }
            (u.id.clone(), s)
        })
        .collect()
}

fn kill(sig: &str, pid: u32) -> bool {
    std::process::Command::new("kill").arg(format!("-{sig}")).arg(pid.to_string()).status().is_ok_and(|s| s.success())
}

/// Start the plugin again, waiting out the restart backoff (≤ 3 s).
async fn restart(client: &PluginClient) -> std::result::Result<crate::protocol::InitializeResult, String> {
    let t = Instant::now();
    loop {
        match client.start().await {
            Ok(i) => return Ok(i),
            Err(crate::client::CallError::Backoff(_, w)) if t.elapsed() < Duration::from_secs(3) => tokio::time::sleep(w.min(Duration::from_millis(500))).await,
            Err(e) => return Err(e.to_string()),
        }
    }
}

async fn test_engine() -> std::result::Result<Engine, String> {
    let store = Arc::new(rightsizer_history::MemoryStore::new());
    Engine::builder(Config::default()).store(store).build().await.map_err(|e| e.to_string())
}

/// Run the suite.
pub async fn run(spec: PluginSpec, fixtures: &Fixtures) -> Report {
    let kind = spec.kind.clone();
    let path = spec.path.display().to_string();
    let mut checks = Vec::new();
    let client = Arc::new(PluginClient::new(spec));
    let adapter = PluginAdapter::new(client.clone());

    // 1. handshake
    let hs = match client.start().await {
        Ok(info) => {
            let mut problems = Vec::new();
            if info.kind != kind {
                problems.push(format!("kind {:?} != {kind:?}", info.kind));
            }
            for m in ["extract", "render", "validate"] {
                if !info.methods.iter().any(|x| x == m) {
                    problems.push(format!("does not advertise {m}"));
                }
            }
            if problems.is_empty() {
                Ok(format!("{} {} v{} methods={:?} concurrency={}", info.protocol, info.kind, info.version, info.methods, info.concurrency.unwrap_or(1)))
            } else {
                Err(problems.join("; "))
            }
        }
        Err(e) => Err(e.to_string()),
    };
    let started = hs.is_ok();
    checks.push(check("handshake", hs));
    if !started {
        for n in ["extract_determinism", "render_idempotence", "validate", "crash_leaves_unchanged", "timeout_leaves_unchanged"] {
            checks.push(Check { name: n.into(), passed: false, detail: "skipped: the plugin did not start".into() });
        }
        client.shutdown().await;
        return Report { kind, path, checks };
    }

    // 2. extract determinism
    let mut extracted = Vec::new();
    let det = async {
        for f in &fixtures.valid {
            let a = adapter.extract(&f.native, &f.env).await.map_err(|e| format!("{}: {e}", f.name))?;
            let b = adapter.extract(&f.native, &f.env).await.map_err(|e| format!("{}: second call: {e}", f.name))?;
            let (ja, jb) = (serde_json::to_value(&a).unwrap_or_default(), serde_json::to_value(&b).unwrap_or_default());
            if ja != jb {
                return Err(format!("{}: two extracts differ", f.name));
            }
            extracted.push((f.clone(), a));
        }
        let units: usize = extracted.iter().map(|(_, x)| x.units.len()).sum();
        Ok(format!("{} fixtures, {units} units, identical on repeat", fixtures.valid.len()))
    }
    .await;
    checks.push(check("extract_determinism", det));
    client.reset_backoff();

    // 3. render idempotence
    let mut rendered: Vec<(ValidFixture, Patch)> = Vec::new();
    let idem = async {
        if extracted.is_empty() {
            return Err("no fixture extracted (see extract_determinism)".to_string());
        }
        let mut n = 0;
        for (f, x) in &extracted {
            if x.units.is_empty() || x.skip.is_some() {
                continue;
            }
            let specs = probe_specs(&x.units);
            let p = adapter.render(&f.native, &specs).await.map_err(|e| format!("{}: render: {e}", f.name))?;
            if p.is_none() {
                return Err(format!("{}: decisions that change memory rendered to Patch::None", f.name));
            }
            let after = apply_generic(&f.native, &p).map_err(|e| format!("{}: {e}", f.name))?;
            let again = adapter.render(&after, &specs).await.map_err(|e| format!("{}: re-render: {e}", f.name))?;
            if !again.is_none() {
                return Err(format!("{}: re-rendering the rendered submission is not Patch::None: {}", f.name, serde_json::to_string(&again).unwrap_or_default()));
            }
            rendered.push((f.clone(), p));
            n += 1;
        }
        if n == 0 {
            return Err("no fixture has units to render".into());
        }
        Ok(format!("{n} fixtures: render(render(x)) = Patch::None"))
    }
    .await;
    checks.push(check("render_idempotence", idem));
    client.reset_backoff();

    // 4. validate
    let val = async {
        let mut rejected = 0;
        for f in &fixtures.invalid {
            match adapter.validate(&f.native, &f.patch, &AdapterEnv::default()).await {
                Ok(()) => return Err(format!("{}: invalid patch accepted", f.name)),
                Err(r) if r.reason == "plugin_error" => return Err(format!("{}: validate failed: {}", f.name, r.detail.unwrap_or_default())),
                Err(_) => rejected += 1,
            }
        }
        for (f, p) in &rendered {
            if let Err(r) = adapter.validate(&f.native, p, &f.env).await {
                return Err(format!("{}: the plugin rejects its own rendered patch ({}: {})", f.name, r.reason, r.detail.unwrap_or_default()));
            }
        }
        if fixtures.invalid.is_empty() {
            return Err("no invalid/*.json fixtures".into());
        }
        Ok(format!("{rejected}/{} invalid patches rejected; {} rendered patches accepted", fixtures.invalid.len(), rendered.len()))
    }
    .await;
    checks.push(check("validate", val));
    client.reset_backoff();

    let engine = match test_engine().await {
        Ok(e) => e,
        Err(e) => {
            checks.push(Check { name: "crash_leaves_unchanged".into(), passed: false, detail: format!("engine: {e}") });
            checks.push(Check { name: "timeout_leaves_unchanged".into(), passed: false, detail: format!("engine: {e}") });
            client.shutdown().await;
            return Report { kind, path, checks };
        }
    };
    let f0 = fixtures.valid[0].clone();
    let allow = |_: &rightsizer_core::Tenant| Ok(());

    // 5. crash (mid-request: freeze, send, kill)
    let crash = async {
        let _ = client.start().await;
        let pid = client.pid().await.ok_or("no pid")?;
        if !kill("STOP", pid) {
            return Err(format!("kill -STOP {pid} failed"));
        }
        let opts = ComposeOptions { mode: Some(rightsizer_core::Mode::Enforce), budget_ms: Some(2000), dry_run: true, ..Default::default() };
        let pending = decide_native(&engine, &adapter, &f0.native, &f0.env, &opts, &allow);
        let killer = async {
            tokio::time::sleep(Duration::from_millis(150)).await;
            kill("KILL", pid)
        };
        let (r, killed) = tokio::join!(pending, killer);
        if !killed {
            return Err(format!("kill -KILL {pid} failed"));
        }
        let c = r.map_err(|e| e.to_string())?;
        if !c.patch.is_none() {
            return Err("a patch was returned after the plugin crashed".into());
        }
        if c.error.is_none() {
            return Err(format!("the crash went unnoticed: {}", c.summary));
        }
        let restarted = restart(&client).await.map_err(|e| format!("unchanged ({}), but no restart: {e}", c.summary))?;
        let new_pid = client.pid().await;
        Ok(format!("pid {pid} killed mid-request: Patch::None ({}); restarted as pid {} ({})", c.error.unwrap_or_default(), new_pid.map(|p| p.to_string()).unwrap_or_default(), restarted.protocol))
    }
    .await;
    checks.push(check("crash_leaves_unchanged", crash));
    client.reset_backoff();

    // 6. timeout
    let to = async {
        let _ = client.start().await;
        let pid = client.pid().await.ok_or("no pid")?;
        if !kill("STOP", pid) {
            return Err(format!("kill -STOP {pid} failed"));
        }
        let t = Instant::now();
        let opts = ComposeOptions { mode: Some(rightsizer_core::Mode::Enforce), budget_ms: Some(300), dry_run: true, ..Default::default() };
        let r = decide_native(&engine, &adapter, &f0.native, &f0.env, &opts, &allow).await;
        let took = t.elapsed();
        kill("CONT", pid);
        let c = r.map_err(|e| e.to_string())?;
        if !c.patch.is_none() {
            return Err("a patch was returned from a frozen plugin".into());
        }
        if took > Duration::from_millis(1300) {
            return Err(format!("decide took {took:?} with a 300 ms budget"));
        }
        Ok(format!("pid {pid} frozen: Patch::None in {} ms ({})", took.as_millis(), c.error.unwrap_or_default()))
    }
    .await;
    checks.push(check("timeout_leaves_unchanged", to));
    client.shutdown().await;
    Report { kind, path, checks }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_patch_ops() {
        let d = serde_json::json!({"a": {"b": [1, 2]}, "c": 1});
        let out = apply_json_patch(
            &d,
            &[
                JsonPatchOp::Replace { path: "/a/b/1".into(), value: serde_json::json!(5) },
                JsonPatchOp::Add { path: "/a/b/-".into(), value: serde_json::json!(9) },
                JsonPatchOp::Remove { path: "/c".into() },
                JsonPatchOp::Copy { from: "/a/b/0".into(), path: "/d".into() },
                JsonPatchOp::Test { path: "/d".into(), value: serde_json::json!(1) },
            ],
        )
        .unwrap();
        assert_eq!(out, serde_json::json!({"a": {"b": [1, 5, 9]}, "d": 1}));
        assert!(apply_json_patch(&d, &[JsonPatchOp::Replace { path: "/x".into(), value: Value::Null }]).is_err());
    }

    #[test]
    fn generic_options_merge() {
        let n = Native::Object { value: serde_json::json!({"kind": "cli_filter", "options": {"mem": "4G"}}) };
        let mut o = serde_json::Map::new();
        o.insert("mem".into(), Value::from("1G"));
        let out = apply_generic(&n, &Patch::Options { options: o }).unwrap();
        assert_eq!(out, Native::Object { value: serde_json::json!({"kind": "cli_filter", "options": {"mem": "1G"}}) });
    }
}
