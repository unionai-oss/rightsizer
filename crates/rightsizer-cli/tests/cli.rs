//! `rsz` end to end on fixture contexts (assert_cmd). Every test runs with
//! its own config file, store and HOME, so nothing on the machine leaks in.

use std::path::PathBuf;

use assert_cmd::Command;
use serde_json::Value;

struct Sandbox {
    dir: tempfile::TempDir,
    /// `"sqlite"` (default) or `"file"` (object store on the local FS).
    backend: &'static str,
}

impl Sandbox {
    fn new() -> Self {
        Self::with_backend("sqlite")
    }
    fn with_backend(backend: &'static str) -> Self {
        let s = Sandbox { dir: tempfile::tempdir().unwrap(), backend };
        std::fs::write(s.path("rightsizer.toml"), "").unwrap();
        s
    }
    fn path(&self, p: &str) -> PathBuf {
        self.dir.path().join(p)
    }
    fn store(&self) -> String {
        match self.backend {
            "file" => format!("file://{}", self.path("history").display()),
            _ => format!("sqlite://{}", self.path("history.db").display()),
        }
    }
    fn write(&self, name: &str, body: &str) -> PathBuf {
        let p = self.path(name);
        std::fs::write(&p, body).unwrap();
        p
    }
    /// `rsz` without `--store`.
    fn bare(&self) -> Command {
        let mut c = Command::cargo_bin("rsz").unwrap();
        c.current_dir(self.dir.path()).env_clear().env("HOME", self.dir.path()).env("RIGHTSIZER_CONFIG", self.path("rightsizer.toml"));
        c
    }
    fn rsz(&self) -> Command {
        let mut c = self.bare();
        c.arg("--store").arg(self.store());
        c
    }
}

fn stdout(o: &std::process::Output) -> String {
    String::from_utf8_lossy(&o.stdout).into_owned()
}

fn context(key: &str, prior_gib: u64) -> String {
    format!(
        r#"{{"key":"{key}","unit":"main",
  "platform":{{"kind":"k8s","retry":{{"same_size":0,"ladder":{{"memory_factor":2.0,"max_steps":2,"driver":"caller"}}}}}},
  "inputs":[{{"name":"data","format":"parquet","bytes":2000000000}}],
  "prior":{{"spec":{{"requests":{{"memory":{},"cpu":2000}},"limits":{{}}}}}},
  "history":[]}}"#,
        prior_gib << 30
    )
}

fn runs_jsonl(key: &str, n: usize, prior_gib: u64, peak_gib: f64) -> String {
    let mut s = String::new();
    for i in 0..n {
        let started = chrono::Utc::now() - chrono::Duration::hours(6 * (n - i) as i64);
        let r = serde_json::json!({
            "key": key, "invocation_id": format!("inv-{i}"), "attempt": 1,
            "started_at": started.to_rfc3339(),
            "inputs": [{"name": "data", "format": "parquet", "bytes": 1_000_000_000u64}],
            "requested": {"requests": {"memory": prior_gib << 30, "cpu": 2000}, "limits": {}},
            "status": "succeeded",
            "memory_peak": {"value": peak_gib * (1u64 << 30) as f64, "censor": "exact"},
            "cpu_mean": {"value": 1.2}, "duration_s": 3600.0,
            "source": {"kind": "watcher"}, "trust": "verified"
        });
        s.push_str(&r.to_string());
        s.push('\n');
    }
    s
}

#[test]
fn recommend_a_bare_context() {
    let s = Sandbox::new();
    let f = s.write("ctx.json", &context("k8s://prod/ml/job/etl", 16));
    let o = s.rsz().args(["recommend", "--dry-run"]).arg(&f).output().unwrap();
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    let out = stdout(&o);
    assert!(out.contains("k8s://prod/ml/job/etl [main]  mode recommend"), "{out}");
    assert!(out.contains("memory"));
    let o = s.rsz().args(["recommend", "--dry-run", "-o", "json"]).arg(&f).output().unwrap();
    let d: Value = serde_json::from_slice(&o.stdout).unwrap();
    assert_eq!(d["mode"], "recommend");
    assert_eq!(d["key"], "k8s://prod/ml/job/etl");
    let o = s.rsz().args(["recommend", "--dry-run", "-o", "yaml"]).arg(&f).output().unwrap();
    assert!(stdout(&o).contains("mode: recommend"));
}

#[test]
fn recommend_a_decide_request_from_stdin_and_explain_it() {
    let s = Sandbox::new();
    let req = format!(r#"{{"context":{{"kind":"full","context":{}}},"policy":{{"mode":"enforce"}}}}"#, context("k8s://prod/ml/job/x", 8));
    let o = s.rsz().args(["recommend", "-"]).write_stdin(req.clone()).output().unwrap();
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    assert!(stdout(&o).contains("mode recommend"), "recommend never enforces");
    let id = stdout(&o).lines().find_map(|l| l.trim().strip_prefix("decision ").map(str::to_string)).unwrap();
    let o = s.rsz().args(["explain", "--decision", &id]).output().unwrap();
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    let out = stdout(&o);
    assert!(out.contains("explanation:") && out.contains("context digest"), "{out}");
    let o = s.rsz().args(["explain", "-"]).write_stdin(req).output().unwrap();
    assert!(stdout(&o).contains("explanation:"));
}

#[test]
fn exit_codes() {
    let s = Sandbox::new();
    // 3: input parse errors.
    s.rsz().args(["recommend", "-"]).write_stdin("{not json").assert().code(3);
    s.rsz().args(["history", "show", "not a key"]).assert().code(3);
    s.rsz().args(["recommend", "-p", "nomad", "-"]).write_stdin("x").assert().code(3);
    // 2: configuration errors.
    let f = s.write("ctx.json", &context("k8s://prod/ml/job/etl", 16));
    s.rsz().env("RIGHTSIZER__POLICY__MIN_HISTORY", "lots").args(["recommend"]).arg(&f).assert().code(2);
    s.write("rightsizer.toml", "[policy]\nmood = 1\n");
    s.rsz().args(["config", "validate"]).assert().code(2);
    // An unusable chain keeps the prior (exit 0) unless strict.
    s.write("rightsizer.toml", "[estimator]\nchain = [\"nope\"]\n");
    let o = s.rsz().args(["recommend", "--dry-run", "-o", "json"]).arg(&f).output().unwrap();
    assert!(o.status.success());
    let d: Value = serde_json::from_slice(&o.stdout).unwrap();
    assert_eq!(d["applied"], d["prior"]["spec"]);
    // A store that cannot be opened leaves the engine inert (prior kept, exit 0)...
    s.write("rightsizer.toml", "");
    let o = s.bare().args(["--store", "bogus://nowhere", "recommend", "--dry-run", "-o", "json"]).arg(&f).output().unwrap();
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    let d: Value = serde_json::from_slice(&o.stdout).unwrap();
    assert_eq!(d["source"]["reason"], "config_error");
    // ...and fails with `strict`.
    s.write("rightsizer.toml", "strict = true\n");
    s.bare().args(["--store", "bogus://nowhere", "recommend", "--dry-run"]).arg(&f).assert().code(2);
    // An invalid value in the file is a config error for the CLI.
    s.write("rightsizer.toml", "[decision]\nneed_headroom = 0.5\n");
    s.rsz().args(["recommend", "--dry-run"]).arg(&f).assert().code(2);
    s.write("rightsizer.toml", "strict = true\n[estimator]\nchain = [\"nope\"]\n");
    s.rsz().args(["recommend", "--dry-run"]).arg(&f).assert().code(2);
    // 1: not implemented.
    s.write("rightsizer.toml", "");
    s.rsz().args(["mcp"]).assert().code(1);
}

#[test]
fn history_import_show_export_and_shadow() {
    // Key listing (`shadow --tenant`, `export --jsonl`) works on every factory store.
    for backend in ["sqlite", "file"] {
        history_round_trip(&Sandbox::with_backend(backend));
    }
}

#[test]
fn the_interim_json_store_is_gone() {
    let s = Sandbox::new();
    let o = s.bare().args(["--store", "json:///tmp/x.json", "history", "show", "k8s://c/ns/job/x"]).output().unwrap();
    // Not strict: a warning naming the replacements, decisions keep the prior.
    assert!(String::from_utf8_lossy(&o.stderr).contains("sqlite:///"), "{}", String::from_utf8_lossy(&o.stderr));
}

fn history_round_trip(s: &Sandbox) {
    let mut jsonl = runs_jsonl("flyte://acme/ml/development/etl.clean", 10, 16, 2.0);
    jsonl.push_str(&runs_jsonl("flyte://acme/ml/development/embed.batch", 6, 8, 3.0));
    let f = s.write("runs.jsonl", &jsonl);
    let o = s.rsz().args(["history", "import", "jsonl"]).arg(&f).output().unwrap();
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    assert!(stdout(&o).contains("imported 16 run records (0 duplicates)"));
    let o = s.rsz().args(["history", "import", "jsonl"]).arg(&f).arg("-o").arg("json").output().unwrap();
    let v: Value = serde_json::from_slice(&o.stdout).unwrap();
    assert_eq!(v["duplicates"], 16);
    let o = s.rsz().args(["history", "show", "flyte://acme/ml/development/etl.clean", "--limit", "3"]).output().unwrap();
    let out = stdout(&o);
    assert_eq!(out.lines().count(), 4, "{out}");
    assert!(out.contains("inv-9") && out.contains("2.0Gi"));

    let o = s.rsz().args(["shadow", "--tenant", "flyte://acme/ml/development", "--since", "14d"]).output().unwrap();
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    let out = stdout(&o);
    for col in ["KEY", "RUNS", "PRIOR mem", "DECIDED mem (p50)", "WOULD SAVE", "WOULD FAIL", "READINESS", "etl.clean", "embed.batch"] {
        assert!(out.contains(col), "missing {col}:\n{out}");
    }
    let o = s.rsz().args(["shadow", "--key", "flyte://acme/ml/development/etl.clean", "--format", "json"]).output().unwrap();
    let r: Value = serde_json::from_slice(&o.stdout).unwrap();
    assert_eq!(r["keys"][0]["runs"], 10);
    let o = s.rsz().args(["shadow", "--format", "html"]).output().unwrap();
    let html = stdout(&o);
    assert!(html.starts_with("<!doctype html>") && !html.contains("<script") && !html.contains("https://"));

    let out = s.path("export.jsonl");
    s.rsz().args(["history", "export", "--jsonl"]).arg(&out).assert().success();
    assert_eq!(std::fs::read_to_string(&out).unwrap().lines().count(), 16);
    let o = s.rsz().args(["history", "compact", "--dry-run", "-o", "json"]).output().unwrap();
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    let v: Value = serde_json::from_slice(&o.stdout).unwrap();
    assert_eq!(v["sweep"]["keys"], 2, "the sweep runs over every key ({}): {v}", s.backend);
    assert_eq!(v["sweep"]["expired_leases"], 0);
}

#[test]
fn outcome_breakers_and_control() {
    let s = Sandbox::new();
    let key = "k8s://prod/ml/job/etl";
    let o = s.rsz().args(["outcome", key, "--status", "oom-killed", "--memory-peak", "3Gi", "-o", "json"]).output().unwrap();
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    let v: Value = serde_json::from_slice(&o.stdout).unwrap();
    assert_eq!(v["record"]["status"], "oom_killed");
    assert_eq!(v["record"]["memory_peak"]["value"], 3.0 * (1u64 << 30) as f64);
    s.rsz().args(["outcome", key, "--status", "melted"]).assert().code(3);

    s.rsz().args(["breaker", "status"]).assert().success().stdout("no breakers tripped\n");
    s.rsz().args(["breaker", "trip", key, "--reason", "drill"]).assert().success();
    let o = s.rsz().args(["breaker", "status", "-o", "json"]).output().unwrap();
    let b: Value = serde_json::from_slice(&o.stdout).unwrap();
    assert_eq!(b[0]["state"], "open");
    // An open per-workload breaker demotes; `recommend` is unaffected in mode but says why nothing changed.
    s.rsz().args(["breaker", "reset", key]).assert().success();
    let o = s.rsz().args(["breaker", "status", "-o", "json"]).output().unwrap();
    let b: Value = serde_json::from_slice(&o.stdout).unwrap();
    assert_eq!(b[0]["state"], "closed");
    s.rsz().args(["breaker", "trip"]).assert().code(3);

    s.rsz().args(["control", "mode-ceiling"]).assert().success().stdout("mode_ceiling: enforce (not set)\n");
    s.rsz().args(["control", "mode-ceiling", "observe"]).assert().success();
    let o = s.rsz().args(["control", "mode-ceiling", "-o", "json"]).output().unwrap();
    let c: Value = serde_json::from_slice(&o.stdout).unwrap();
    assert_eq!(c["mode_ceiling"], "observe");
    // The ceiling caps recommend too: observe keeps the prior.
    let f = s.write("ctx.json", &context(key, 16));
    let o = s.rsz().args(["recommend", "--dry-run", "-o", "json"]).arg(&f).output().unwrap();
    let d: Value = serde_json::from_slice(&o.stdout).unwrap();
    assert_eq!(d["mode"], "observe");
    assert_eq!(d["source"]["reason"], "mode");
}

#[test]
fn config_show_respects_overrides() {
    let s = Sandbox::new();
    let o = s.rsz().env("RIGHTSIZER__POLICY__MODE", "observe").args(["config", "show", "-o", "json"]).output().unwrap();
    let c: Value = serde_json::from_slice(&o.stdout).unwrap();
    assert_eq!(c["policy"]["mode"], "observe");
    assert_eq!(c["history"]["uri"], Value::String(s.store()));
}

#[cfg(feature = "k8s")]
#[test]
fn recommend_a_kubernetes_job_through_the_adapter() {
    let s = Sandbox::new();
    let job = "apiVersion: batch/v1\nkind: Job\nmetadata:\n  name: train\n  namespace: ml\nspec:\n  template:\n    spec:\n      restartPolicy: Never\n      containers:\n      - name: trainer\n        image: python:3.12\n        resources:\n          requests:\n            memory: 16Gi\n            cpu: \"2\"\n          limits:\n            memory: 16Gi\n";
    let f = s.write("job.yaml", job);
    let o = s.rsz().env("RIGHTSIZER__K8S__CLUSTER", "prod").args(["recommend", "-p", "k8s", "--dry-run", "-o", "json"]).arg(&f).output().unwrap();
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    let d: Value = serde_json::from_slice(&o.stdout).unwrap();
    assert!(d["key"].as_str().unwrap().starts_with("k8s://prod/ml/"), "{d}");
    assert_eq!(d["prior"]["spec"]["requests"]["memory"], 16u64 << 30);
}
