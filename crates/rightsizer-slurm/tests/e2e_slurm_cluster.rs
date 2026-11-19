//! Opt-in end-to-end check on the Docker Slurm cluster (`tests/infra/slurm.sh up`):
//! `RSZ_IT_SLURM=1 cargo test -p rightsizer-slurm --test e2e_slurm_cluster -- --nocapture`.
//!
//! Installs contrib/slurm/job_submit.lua (+ modules, a fresh key) into the
//! cluster's extra dir, submits real jobs as `alice` with tokens signed in
//! Rust from the *parsed* submission (sbatch options → job_desc mapping of
//! `token.rs`), and checks the AdminComment sacct reports. The previous
//! job_submit.lua is restored afterwards. Jobs go to the `gpu` partition
//! (whose node does not exist), so they pend and are cancelled.

use std::path::PathBuf;
use std::process::Command;

use rightsizer_slurm::attest::sign_job;
use rightsizer_slurm::sbatch::parse;
use rightsizer_slurm::token::KeyRing;
use ulid::Ulid;

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn sh(cmd: &str) -> String {
    let o = Command::new("bash").arg("-c").arg(cmd).current_dir(root()).output().unwrap();
    assert!(o.status.success(), "{cmd}: {}", String::from_utf8_lossy(&o.stderr));
    String::from_utf8_lossy(&o.stdout).trim().to_string()
}

#[test]
fn job_submit_verifies_rust_tokens_on_the_cluster() {
    if std::env::var_os("RSZ_IT_SLURM").is_none() {
        eprintln!("RSZ_IT_SLURM not set: skipping");
        return;
    }
    let extra = root().join("tests/infra/.state/slurm-etc-extra");
    let backup = std::fs::read(extra.join("job_submit.lua")).ok();
    for m in ["job_submit.lua", "rightsizer_token.lua", "sha256.lua", "hmac.lua", "json.lua"] {
        std::fs::copy(root().join("contrib/slurm").join(m), extra.join(m)).unwrap();
    }
    let ring = KeyRing::generate().unwrap();
    std::fs::write(extra.join("rightsizer.key"), ring.to_text()).unwrap(); // 0644 on the bind mount (test only)
    sh("tests/infra/slurm.sh reconfigure >/dev/null 2>&1");

    let script = "#!/bin/bash\n#SBATCH --job-name=rsz-e2e\n#SBATCH -c 2\nsleep 1\n";
    sh(&format!("docker exec -i rsz-it-slurm-slurmctld bash -c 'cat > /data/rsz-e2e.sh && chmod 644 /data/rsz-e2e.sh' <<'EOF'\n{script}EOF"));
    // (argv, tamper: extra argv appended after signing)
    let cases: &[(&str, Option<&str>, &str)] = &[
        ("--mem=100M", None, "verified"),
        ("--mem-per-cpu=1500K", None, "verified"),
        ("--gres=gpu:a100:1 --mem-per-gpu=2G", None, "verified"),
        ("--gpus=a100:2 --mem=3G", None, "verified"),
        ("-n 2 --gpus-per-task=1 --mem=1G", None, "verified"),
        ("--gpus-per-node=a100:2 --mem=0", None, "verified"),
        ("--mem=100M", Some("--mem=200M"), "user"),
        ("--mem=100M", Some("-c 3"), "user"),
        ("--gres=gpu:a100:1 --mem=1G", Some("--gres=gpu:a100:2"), "user"),
    ];
    let id = Ulid::generate();
    let mut ids = Vec::new();
    for (opts, tamper, _) in cases {
        let argv: Vec<String> = shlex::split(&format!("-p gpu -A mlteam {opts} rsz-e2e.sh")).unwrap();
        let job = parse(&argv, Some(script), &Default::default()).unwrap();
        let tok = sign_job(&job, 1000, "mlteam", &[id], 1_900_000_000, &ring).unwrap().remove(0);
        let submit = format!("-p gpu -A mlteam {opts} {} /data/rsz-e2e.sh", tamper.unwrap_or(""));
        let jid = sh(&format!("tests/infra/slurm.sh exec -u alice bash -c 'RIGHTSIZER_TOKEN={tok} sbatch --parsable {submit}'"));
        ids.push(jid);
    }
    let wrap_argv = shlex::split("-p gpu -A mlteam --mem=64M --wrap 'echo hi there'").unwrap();
    let wjob = parse(&wrap_argv, None, &Default::default()).unwrap();
    let wtok = sign_job(&wjob, 1000, "mlteam", &[id], 1_900_000_000, &ring).unwrap().remove(0);
    let wid = sh(&format!("tests/infra/slurm.sh exec -u alice bash -c \"RIGHTSIZER_TOKEN={wtok} sbatch --parsable -p gpu -A mlteam --mem=64M --wrap 'echo hi there'\""));
    sh(&format!("tests/infra/slurm.sh exec scancel {} {wid}", ids.join(" ")));
    std::thread::sleep(std::time::Duration::from_secs(3));
    let mut results = Vec::new();
    for (jid, (opts, tamper, want)) in ids.iter().zip(cases).chain(std::iter::once((&wid, &("--wrap", None, "verified")))) {
        let ac = sh(&format!("docker exec rsz-it-slurm-slurmctld sacct -n -X -P -j {jid} -o AdminComment"));
        eprintln!("job {jid} [{opts} {}] -> {ac}", tamper.unwrap_or(""));
        results.push((ac.clone(), format!("rightsizer:{id}:{want}"), opts.to_string()));
    }
    match backup {
        Some(b) => std::fs::write(extra.join("job_submit.lua"), b).unwrap(),
        None => {
            let _ = std::fs::remove_file(extra.join("job_submit.lua"));
        }
    }
    for m in ["rightsizer_token.lua", "sha256.lua", "hmac.lua", "json.lua", "rightsizer.key"] {
        let _ = std::fs::remove_file(extra.join(m));
    }
    let _ = Command::new("bash").arg("-c").arg("tests/infra/slurm.sh reconfigure >/dev/null 2>&1").current_dir(root()).status();
    for (got, want, opts) in results {
        assert_eq!(got, want, "{opts}");
    }
}
