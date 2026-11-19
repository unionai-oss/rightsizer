//! `rsz slurm sbatch [--watch]` against fake `sbatch`/`sacct`/`squeue`
//! shell scripts (they log their argv, the script they were given and the
//! token they saw; `sacct` replays a list of final states).

mod common;

use std::path::{Path, PathBuf};
use std::sync::Mutex;

use async_trait::async_trait;
use common::*;
use rightsizer_core::{AttemptRecord, Bytes, Decision, Mode, RunStatus, WorkloadKey};
use rightsizer_plugin::protocol::{DecideParams, DecideResult};
use rightsizer_plugin::{Native, PluginError, Result, UnitId};
use rightsizer_slurm::attest::sign_patched;
use rightsizer_slurm::runner::TokioRunner;
use rightsizer_slurm::token::{KeyRing, Verification, VerifyInput, script_sha256};
use rightsizer_slurm::wrapper::{Decider, EXIT_SBATCH_FAILED, WrapperConfig, render_for, run};
use ulid::Ulid;

const SCRIPT: &str = "#!/bin/bash\n#SBATCH --job-name=train\n#SBATCH --mem=4G\n#SBATCH -c 2\npython train.py\n";

struct Fake {
    dir: tempfile::TempDir,
}

impl Fake {
    /// `states`: what sacct reports for job 1, 2, … (one per line).
    fn new(states: &[&str], sbatch_exit: i32) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path();
        std::fs::write(d.join("states"), states.join("\n") + "\n").unwrap();
        write_exe(
            &d.join("sbatch"),
            &format!(
                r#"#!/bin/sh
D="{d}"
n=$(( $(cat "$D/count" 2>/dev/null || echo 0) + 1 )); echo $n > "$D/count"
printf '%s\n' "$@" > "$D/argv.$n"
for a in "$@"; do case "$a" in -*|:) ;; *) [ -f "$a" ] && cp "$a" "$D/script.$n" && echo "$a" > "$D/path.$n";; esac; done
echo "${{RIGHTSIZER_DECIDED:-}}" > "$D/decided.$n"
[ {sbatch_exit} -ne 0 ] && {{ echo "sbatch: error: boom" >&2; exit {sbatch_exit}; }}
echo "Submitted batch job $n"
"#,
                d = d.display()
            ),
        );
        write_exe(&d.join("squeue"), "#!/bin/sh\nexit 0\n");
        write_exe(
            &d.join("sacct"),
            &format!(
                r#"#!/bin/sh
D="{d}"
while [ $# -gt 0 ]; do [ "$1" = "-j" ] && j="$2"; shift; done
st=$(sed -n "${{j}}p" "$D/states")
case "$st" in *=*) for t in $(echo "$st" | tr ',' ' '); do echo "${{j}}_${{t%%=*}}|${{t#*=}}"; done;; *) echo "$j|$st";; esac
"#,
                d = d.display()
            ),
        );
        Fake { dir }
    }

    fn path(&self) -> &Path {
        self.dir.path()
    }

    fn cfg(&self) -> WrapperConfig {
        let d = self.path();
        WrapperConfig {
            sbatch: d.join("sbatch"),
            sacct: d.join("sacct"),
            squeue: d.join("squeue"),
            poll_interval: std::time::Duration::from_millis(5),
            accounting_wait: std::time::Duration::from_secs(5),
            settings: settings(),
            env: env(),
            cwd: d.to_path_buf(),
            tmp_dir: Some(d.join("tmp")),
            ..Default::default()
        }
    }

    fn read(&self, name: &str) -> String {
        std::fs::read_to_string(self.path().join(name)).unwrap_or_default()
    }

    fn argv(&self, n: u32) -> Vec<String> {
        self.read(&format!("argv.{n}")).lines().map(str::to_string).collect()
    }
}

fn write_exe(p: &Path, body: &str) {
    use std::os::unix::fs::PermissionsExt;
    std::fs::write(p, body).unwrap();
    std::fs::set_permissions(p, std::fs::Permissions::from_mode(0o755)).unwrap();
}

/// A decider that halves memory and signs with a test key.
struct TestDecider {
    mode: Mode,
    mem: Bytes,
    ring: KeyRing,
    fail: bool,
    leases: Mutex<Vec<String>>,
    lease_fails: bool,
}

impl TestDecider {
    fn new(mode: Mode, mem: Bytes) -> Self {
        TestDecider { mode, mem, ring: KeyRing::generate().unwrap(), fail: false, leases: Mutex::new(vec![]), lease_fails: false }
    }
}

fn decision(unit: &str, prior_mem: Bytes, mem: Bytes, mode: Mode) -> Decision {
    serde_json::from_value(serde_json::json!({
        "id": Ulid::from_string("01JDZ7Q4W8M7C9B5V3N2X1Y0ZA").unwrap().to_string(),
        "key": "slurm://rsz-it/none/train", "unit": unit, "mode": mode,
        "recommended": {"requests": {"memory": mem.0}}, "applied": {"requests": {"memory": mem.0}},
        "prior": {"spec": {"requests": {"memory": prior_mem.0, "cpu": 2000}}},
        "source": {"kind": "estimator", "id": "test", "version": "1"}, "propensity": 1.0,
        "explanation": ["memory 4G → 2G (p99 peak 1.6G)"]
    }))
    .unwrap()
}

#[async_trait]
impl Decider for TestDecider {
    async fn decide(&self, p: DecideParams) -> Result<DecideResult> {
        if self.fail {
            return Err(PluginError::PlatformUnavailable("daemon down".into()));
        }
        assert_eq!(p.kind, "slurm");
        let d = decision("job", Bytes::gib(4), self.mem, self.mode);
        let patch = render_for(&p.native, std::slice::from_ref(&d), &settings())?;
        let token = (self.mode == Mode::Enforce).then(|| sign_patched(&p.native, &patch, 1000, "none", &[d.id], 1_900_000_000, &self.ring).unwrap());
        Ok(DecideResult { decisions: vec![d], patch, mode: self.mode, summary: "rightsizer: train memory 4G → 2G".into(), token })
    }
    async fn open_lease(&self, r: &AttemptRecord) -> Result<()> {
        if self.lease_fails {
            return Err(PluginError::PlatformUnavailable("store".into()));
        }
        self.leases.lock().unwrap().push(format!("open {} reduced={}", r.key, r.reduced));
        Ok(())
    }
    async fn close_lease(&self, _k: &WorkloadKey, _i: &str, s: RunStatus) -> Result<()> {
        self.leases.lock().unwrap().push(format!("close {s:?}"));
        Ok(())
    }
    async fn sign(&self, native: &Native, ids: &[Ulid]) -> Result<Option<String>> {
        Ok(Some(sign_patched(native, &rightsizer_plugin::Patch::None, 1000, "none", ids, 1_900_000_000, &self.ring)?))
    }
}

fn args(a: &str) -> Vec<String> {
    shlex::split(a).unwrap()
}

fn token_of(argv: &[String]) -> Option<String> {
    argv.iter().find_map(|a| a.strip_prefix("--export=ALL,RIGHTSIZER_TOKEN=").map(str::to_string))
}

#[tokio::test]
async fn enforce_edits_a_temp_copy_and_attaches_a_token() {
    let f = Fake::new(&["COMPLETED"], 0);
    std::fs::write(f.path().join("train.sh"), SCRIPT).unwrap();
    let dec = TestDecider::new(Mode::Enforce, Bytes::gib(2));
    let out = run(&args("--parsable train.sh --epochs 3"), None, &dec, &f.cfg(), &TokioRunner { cwd: Some(f.path().to_path_buf()) }).await;
    assert_eq!(out.exit_code, 0, "{:?}", out.messages);
    assert_eq!(out.job_ids, vec!["1"]);
    let sent = f.read("script.1");
    assert_eq!(sent, SCRIPT.replace("--mem=4G", "--mem=2G"));
    // Same file name as the original (default job name unchanged), in a temp dir that is gone.
    let p = PathBuf::from(f.read("path.1").trim());
    assert_eq!(p.file_name().unwrap(), "train.sh");
    assert!(!p.exists());
    assert_eq!(std::fs::read_to_string(f.path().join("train.sh")).unwrap(), SCRIPT, "the user's file is untouched");
    let argv = f.argv(1);
    assert_eq!(&argv[argv.len() - 2..], &["--epochs", "3"]);
    assert_eq!(f.read("decided.1").trim(), "01JDZ7Q4W8M7C9B5V3N2X1Y0ZA");
    // The token verifies against what job_submit.lua will see.
    let tok = token_of(&argv).unwrap();
    let res = rightsizer_slurm::token::JobDescResources { min_mem_per_node: Some(2048), cpus_per_task: Some(2), ..Default::default() };
    let v = dec.ring.verify(Some(&tok), &VerifyInput { uid: 1000, account: "none", script_sha256: &script_sha256(sent.as_bytes()), resources: &res, now: 1_800_000_000, skew_s: 0 });
    assert!(matches!(v, Verification::Verified { .. }), "{v:?}");
    assert!(out.diff.contains("-   3 #SBATCH --mem=4G\n+   3 #SBATCH --mem=2G"), "{}", out.diff);
    assert!(out.messages.iter().any(|m| m.contains("applied") && m.contains("p99 peak")));
}

#[tokio::test]
async fn recommend_prints_and_submits_unchanged() {
    let f = Fake::new(&["COMPLETED"], 0);
    std::fs::write(f.path().join("train.sh"), SCRIPT).unwrap();
    let dec = TestDecider::new(Mode::Recommend, Bytes::gib(2));
    let out = run(&args("train.sh"), None, &dec, &f.cfg(), &TokioRunner { cwd: Some(f.path().to_path_buf()) }).await;
    assert_eq!(out.exit_code, 0);
    assert_eq!(f.argv(1), args("train.sh"));
    assert_eq!(f.read("script.1"), SCRIPT);
    assert!(token_of(&f.argv(1)).is_none());
    assert!(out.messages.iter().any(|m| m.contains("recommended (not applied)") && m.contains("+   3 #SBATCH --mem=2G")), "{:?}", out.messages);
}

#[tokio::test]
async fn s1_daemon_failure_submits_unchanged() {
    let f = Fake::new(&["COMPLETED"], 0);
    std::fs::write(f.path().join("train.sh"), SCRIPT).unwrap();
    let mut dec = TestDecider::new(Mode::Enforce, Bytes::gib(2));
    dec.fail = true;
    let out = run(&args("-c 2 train.sh"), None, &dec, &f.cfg(), &TokioRunner { cwd: Some(f.path().to_path_buf()) }).await;
    assert_eq!(out.exit_code, 0);
    assert_eq!(f.argv(1), args("-c 2 train.sh"));
    assert!(out.messages[0].contains("decide failed"));
}

#[tokio::test]
async fn sbatch_failure_exits_4() {
    let f = Fake::new(&["COMPLETED"], 7);
    std::fs::write(f.path().join("train.sh"), SCRIPT).unwrap();
    let dec = TestDecider::new(Mode::Enforce, Bytes::gib(2));
    let out = run(&args("train.sh"), None, &dec, &f.cfg(), &TokioRunner { cwd: Some(f.path().to_path_buf()) }).await;
    assert_eq!(out.exit_code, EXIT_SBATCH_FAILED);
    assert_eq!(out.sbatch_exit, Some(7));
    assert!(out.messages.iter().any(|m| m.contains("sbatch: error: boom") && m.contains("exit code 7")));
}

#[tokio::test]
async fn watch_climbs_the_ladder_on_oom() {
    let f = Fake::new(&["OUT_OF_MEMORY", "OUT_OF_MEMORY", "COMPLETED"], 0);
    std::fs::write(f.path().join("train.sh"), SCRIPT.replace("--mem=4G", "--mem=8G")).unwrap();
    let mut cfg = f.cfg();
    cfg.watch = true;
    let dec = TestDecider::new(Mode::Enforce, Bytes::gib(1));
    let out = run(&args("train.sh"), None, &dec, &cfg, &TokioRunner { cwd: Some(f.path().to_path_buf()) }).await;
    assert_eq!(out.exit_code, 0, "{:?}", out.messages);
    assert_eq!(out.job_ids, vec!["1", "2", "3"]);
    assert_eq!(out.final_state.as_deref(), Some("COMPLETED"));
    assert!(f.read("script.1").contains("--mem=1G"));
    assert!(f.read("script.2").contains("--mem=2G"), "step 1: × 2");
    // Last step: at least the origin prior (4G in the decision).
    assert!(f.read("script.3").contains("--mem=4G"), "{}", f.read("script.3"));
    assert!(token_of(&f.argv(2)).is_some(), "ladder attempts are signed");
    let leases = dec.leases.lock().unwrap().clone();
    // Attempts 1 and 2 were below the prior; attempt 3 is at the prior (no lease).
    assert_eq!(leases.iter().filter(|l| l.starts_with("open")).count(), 2);
    assert_eq!(leases.last().unwrap(), "close Succeeded");
}

#[tokio::test]
async fn watch_stops_at_max_steps_and_on_other_failures() {
    let f = Fake::new(&["OUT_OF_MEMORY", "OUT_OF_MEMORY", "OUT_OF_MEMORY", "COMPLETED"], 0);
    std::fs::write(f.path().join("train.sh"), SCRIPT).unwrap();
    let mut cfg = f.cfg();
    cfg.watch = true;
    let dec = TestDecider::new(Mode::Enforce, Bytes::gib(1));
    let out = run(&args("train.sh"), None, &dec, &cfg, &TokioRunner { cwd: Some(f.path().to_path_buf()) }).await;
    assert_eq!(out.job_ids.len(), 3, "1 + max_steps attempts");
    assert_eq!(out.exit_code, 1);
    let f = Fake::new(&["FAILED"], 0);
    std::fs::write(f.path().join("train.sh"), SCRIPT).unwrap();
    let mut cfg = f.cfg();
    cfg.watch = true;
    let out = run(&args("train.sh"), None, &dec, &cfg, &TokioRunner { cwd: Some(f.path().to_path_buf()) }).await;
    assert_eq!(out.job_ids.len(), 1, "only OOM escalates");
    assert_eq!(out.exit_code, 1);
}

#[tokio::test]
async fn watch_without_a_lease_runs_at_the_prior() {
    let f = Fake::new(&["COMPLETED"], 0);
    std::fs::write(f.path().join("train.sh"), SCRIPT).unwrap();
    let mut cfg = f.cfg();
    cfg.watch = true;
    let mut dec = TestDecider::new(Mode::Enforce, Bytes::gib(1));
    dec.lease_fails = true;
    let out = run(&args("train.sh"), None, &dec, &cfg, &TokioRunner { cwd: Some(f.path().to_path_buf()) }).await;
    assert_eq!(out.exit_code, 0);
    assert_eq!(f.argv(1), args("train.sh"));
    assert!(out.messages.iter().any(|m| m.contains("lease")));
}

#[tokio::test]
async fn watch_array_resubmits_only_oom_tasks() {
    let f = Fake::new(&["1=COMPLETED,2=OUT_OF_MEMORY,3=COMPLETED,4=OUT_OF_MEMORY", "2=COMPLETED,4=COMPLETED"], 0);
    std::fs::write(f.path().join("train.sh"), SCRIPT).unwrap();
    let mut cfg = f.cfg();
    cfg.watch = true;
    let dec = TestDecider::new(Mode::Enforce, Bytes::gib(1));
    let out = run(&args("--array=1-4 train.sh"), None, &dec, &cfg, &TokioRunner { cwd: Some(f.path().to_path_buf()) }).await;
    assert_eq!(out.exit_code, 0, "{:?}", out.messages);
    assert!(f.argv(2).contains(&"--array=2,4".to_string()), "{:?}", f.argv(2));
    let _ = UnitId("job".into());
}
