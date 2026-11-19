//! Cross-language token vectors: Rust signs, `contrib/slurm/tests` (Lua)
//! verifies. `contrib/slurm/tests/token-vectors.json` is a golden file;
//! regenerate with `RSZ_REGEN=1 cargo test -p rightsizer-slurm --test token_vectors`.

use rightsizer_slurm::token::{JobDescResources, KeyRing, TokenClaims, VerifyInput, script_sha256, wrap_script};
use serde_json::{Value, json};
use ulid::Ulid;

const KEYS: &str = "# test keys\n\
rsz-key-v1 kold verify 1700000000 b2xkLW9sZC1vbGQtb2xkLW9sZC1vbGQtb2xkLW9sZCE\n\
rsz-key-v1 knew active 1800000000 bmV3LW5ldy1uZXctbmV3LW5ldy1uZXctbmV3LW5ldyE\n";

/// A job as job_submit.lua sees it: numbers are what Slurm pushes
/// (NO_VAL16 = 65534 for an unset cpus_per_task, NO_VAL = 4294967294 for an
/// unset pn_min_tmp_disk).
fn job_desc(r: &JobDescResources, account: Option<&str>, script: &str) -> Value {
    let mut j = serde_json::Map::new();
    if let Some(a) = account {
        j.insert("account".into(), json!(a));
    }
    j.insert("script".into(), json!(script));
    if let Some(m) = r.min_mem_per_node {
        j.insert("min_mem_per_node".into(), json!(m));
    }
    if let Some(m) = r.min_mem_per_cpu {
        j.insert("min_mem_per_cpu".into(), json!(m));
    }
    j.insert("cpus_per_task".into(), json!(r.cpus_per_task.unwrap_or(65534)));
    for (k, v) in [
        ("tres_per_node", &r.tres_per_node),
        ("tres_per_job", &r.tres_per_job),
        ("tres_per_task", &r.tres_per_task),
        ("tres_per_socket", &r.tres_per_socket),
        ("mem_per_tres", &r.mem_per_tres),
    ] {
        if let Some(v) = v {
            j.insert(k.into(), json!(v));
        }
    }
    j.insert("pn_min_tmp_disk".into(), json!(r.pn_min_tmp_disk.unwrap_or(4_294_967_294)));
    Value::Object(j)
}

fn vectors() -> Value {
    let ring = KeyRing::parse(KEYS).unwrap();
    let script = "#!/bin/bash\n#SBATCH --mem=24G\n#SBATCH -c 4\npython train.py\n";
    let wrap = wrap_script("python3 -c \"print(1)\"");
    let id = Ulid::from_string("01JDZ7Q4W8M7C9B5V3N2X1Y0ZA").unwrap();
    let expiry = 1_900_000_000u64;
    let base = JobDescResources { min_mem_per_node: Some(24576), cpus_per_task: Some(4), ..Default::default() };
    let gpu = JobDescResources {
        tres_per_node: Some("gres/gpu:a100:2".into()),
        tres_per_task: Some("cpu=8".into()),
        cpus_per_task: Some(8),
        mem_per_tres: Some("gres/gpu:40960".into()),
        pn_min_tmp_disk: Some(2048),
        ..Default::default()
    };
    let mut cases = Vec::new();
    let mut add = |name: &str, signed: &JobDescResources, seen: &JobDescResources, acct_signed: &str, acct_seen: Option<&str>, default_account: Option<&str>, uid_seen: u32, script_signed: &str, script_seen: &str, now: u64, token_override: Option<String>| {
        let claims = TokenClaims {
            uid: 1000,
            account: acct_signed.into(),
            script_sha256: script_sha256(script_signed.as_bytes()),
            resources: signed.clone(),
            decision_id: id,
            expiry,
        };
        let token = token_override.unwrap_or_else(|| ring.sign(&claims).unwrap());
        let account_eff = acct_seen.or(default_account).unwrap_or("");
        let v = ring.verify(
            Some(&token),
            &VerifyInput { uid: uid_seen, account: account_eff, script_sha256: &script_sha256(script_seen.as_bytes()), resources: seen, now, skew_s: 120 },
        );
        let mut jd = job_desc(seen, acct_seen, script_seen);
        if let Some(d) = default_account {
            jd["default_account"] = json!(d);
        }
        cases.push(json!({
            "name": name,
            "token": token,
            "submit_uid": uid_seen,
            "now": now,
            "job_desc": jd,
            "canonical": seen.canonical(),
            "expected_admin_comment": v.admin_comment(),
        }));
    };
    let now = expiry - 60;
    add("valid", &base, &base, "mlteam", Some("mlteam"), None, 1000, script, script, now, None);
    add("valid_default_account", &base, &base, "mlteam", None, Some("mlteam"), 1000, script, script, now, None);
    add("valid_account_case", &base, &base, "mlteam", Some("MLteam"), None, 1000, script, script, now, None);
    add("valid_gpu_fields", &gpu, &gpu, "mlteam", Some("mlteam"), None, 1000, &wrap, &wrap, now, None);
    add("valid_within_skew", &base, &base, "mlteam", Some("mlteam"), None, 1000, script, script, expiry + 100, None);
    add("expired", &base, &base, "mlteam", Some("mlteam"), None, 1000, script, script, expiry + 121, None);
    add("tampered_uid", &base, &base, "mlteam", Some("mlteam"), None, 1001, script, script, now, None);
    add("tampered_account", &base, &base, "mlteam", Some("vision"), None, 1000, script, script, now, None);
    add("tampered_script", &base, &base, "mlteam", Some("mlteam"), None, 1000, script, &format!("{script}rm -rf /tmp/x\n"), now, None);
    let mut more_mem = base.clone();
    more_mem.min_mem_per_node = Some(65536);
    add("tampered_mem", &base, &more_mem, "mlteam", Some("mlteam"), None, 1000, script, script, now, None);
    let mut per_cpu = base.clone();
    per_cpu.min_mem_per_node = None;
    per_cpu.min_mem_per_cpu = Some(24576);
    add("tampered_mem_form", &base, &per_cpu, "mlteam", Some("mlteam"), None, 1000, script, script, now, None);
    let mut cpus = base.clone();
    cpus.cpus_per_task = Some(16);
    add("tampered_cpus", &base, &cpus, "mlteam", Some("mlteam"), None, 1000, script, script, now, None);
    let mut g2 = gpu.clone();
    g2.tres_per_node = Some("gres/gpu:a100:4".into());
    add("tampered_gpus", &gpu, &g2, "mlteam", Some("mlteam"), None, 1000, &wrap, &wrap, now, None);
    let mut g3 = gpu.clone();
    g3.mem_per_tres = Some("gres/gpu:81920".into());
    add("tampered_mem_per_gpu", &gpu, &g3, "mlteam", Some("mlteam"), None, 1000, &wrap, &wrap, now, None);
    let mut g4 = gpu.clone();
    g4.pn_min_tmp_disk = None;
    add("tampered_tmp", &gpu, &g4, "mlteam", Some("mlteam"), None, 1000, &wrap, &wrap, now, None);
    // Slurm spelling variants that must still verify.
    let mut spelled = gpu.clone();
    spelled.tres_per_node = Some("gres:gpu:A100:2".into());
    spelled.tres_per_task = None;
    add("valid_tres_spelling", &gpu, &spelled, "mlteam", Some("mlteam"), None, 1000, &wrap, &wrap, now, None);
    // Signed with the old (verify-only) key: still valid during rotation.
    let old = KeyRing::parse(&KEYS.replace("verify", "tmp").replace("active", "verify").replace("tmp", "active")).unwrap();
    let old_token = old
        .sign(&TokenClaims { uid: 1000, account: "mlteam".into(), script_sha256: script_sha256(script.as_bytes()), resources: base.clone(), decision_id: id, expiry })
        .unwrap();
    add("valid_old_key", &base, &base, "mlteam", Some("mlteam"), None, 1000, script, script, now, Some(old_token));
    add("malformed", &base, &base, "mlteam", Some("mlteam"), None, 1000, script, script, now, Some("not-a-token".into()));
    json!({
        "comment": "Generated by crates/rightsizer-slurm/tests/token_vectors.rs; RSZ_REGEN=1 to regenerate.",
        "keys": KEYS,
        "skew_s": 120,
        "cases": cases,
    })
}

#[test]
fn token_vectors_golden() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../contrib/slurm/tests/token-vectors.json");
    let want = serde_json::to_string_pretty(&vectors()).unwrap() + "\n";
    if std::env::var_os("RSZ_REGEN").is_some() || !path.exists() {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, &want).unwrap();
    }
    let have = std::fs::read_to_string(&path).unwrap();
    pretty_assertions::assert_eq!(have, want, "regenerate with RSZ_REGEN=1");
    let v: Value = serde_json::from_str(&have).unwrap();
    let verified = v["cases"].as_array().unwrap().iter().filter(|c| c["expected_admin_comment"].as_str().unwrap().ends_with(":verified")).count();
    assert_eq!(verified, 7);
}
