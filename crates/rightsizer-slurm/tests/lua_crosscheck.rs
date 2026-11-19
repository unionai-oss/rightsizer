//! Cross-language: Rust signs fresh tokens for parsed submissions (the full
//! sbatch → job_desc mapping), `contrib/slurm/rightsizer_token.lua` verifies
//! them; every tampered field must come back `:user`. Runs with `luajit` /
//! `lua` from PATH and skips (with a note) when there is none.

use std::process::Command;

use rightsizer_slurm::attest::job_desc_resources;
use rightsizer_slurm::sbatch::parse;
use rightsizer_slurm::token::{JobDescResources, KeyRing, TokenClaims, script_sha256};
use serde_json::{Value, json};
use ulid::Ulid;

fn lua() -> Option<String> {
    ["luajit", "lua", "lua5.4", "lua5.3", "lua5.1"]
        .into_iter()
        .find(|l| Command::new(l).arg("-v").output().is_ok_and(|o| o.status.success()))
        .map(str::to_string)
}

fn job_desc(r: &JobDescResources, account: &str, script: &str) -> Value {
    let mut j = json!({"account": account, "script": script,
        "cpus_per_task": r.cpus_per_task.unwrap_or(65534), "pn_min_tmp_disk": r.pn_min_tmp_disk.unwrap_or(4_294_967_294u64)});
    for (k, v) in [("min_mem_per_node", r.min_mem_per_node), ("min_mem_per_cpu", r.min_mem_per_cpu)] {
        if let Some(v) = v {
            j[k] = json!(v);
        }
    }
    for (k, v) in [("tres_per_node", &r.tres_per_node), ("tres_per_job", &r.tres_per_job), ("tres_per_task", &r.tres_per_task), ("tres_per_socket", &r.tres_per_socket), ("mem_per_tres", &r.mem_per_tres)] {
        if let Some(v) = v {
            j[k] = json!(v);
        }
    }
    j
}

const DRIVER: &str = r#"
package.path = arg[1] .. "/?.lua;" .. package.path
local json = require("json")
local tok = require("rightsizer_token")
local f = assert(io.open(arg[2], "rb")); local doc = json.decode(f:read("*a")); f:close()
local keys = assert(tok.parse_keys(doc.keys))
for _, c in ipairs(doc.cases) do
  local st, id = tok.verify(keys, c.token, c.job_desc, c.uid, c.now, 120)
  print(c.name .. " " .. tok.admin_comment(st, id))
end
"#;

#[test]
fn rust_signs_lua_verifies() {
    let Some(lua) = lua() else {
        eprintln!("no Lua interpreter on PATH: skipping (contrib/slurm/tests/run.sh also runs in the Slurm image)");
        return;
    };
    let ring = KeyRing::generate().unwrap();
    let id = Ulid::from_string("01JDZ7Q4W8M7C9B5V3N2X1Y0ZB").unwrap();
    let submissions: &[(&str, Option<&str>)] = &[
        ("x.sh", Some("#!/bin/bash\n#SBATCH --mem=24G\n#SBATCH -c 4\npython t.py\n")),
        ("--mem-per-cpu=1500K -c 3 x.sh", Some("#!/bin/sh\necho ünïcode\n")),
        ("-p gpu --gres=gpu:a100:2,shard:1 --mem-per-gpu=40G -c 8 --tmp=20G x.sh", Some("#!/bin/bash\nsrun python t.py\n")),
        ("--gpus=h100:4 --gpus-per-task=1 -n 4 --mem=0 --wrap 'python -c \"print(1)\"'", None),
        ("--gpus-per-node=2 --gpus-per-socket=1 --mem=1T x.sh", Some("#!/bin/bash\n")),
    ];
    let mut cases = Vec::new();
    let mut want = Vec::new();
    for (i, (argv, script)) in submissions.iter().enumerate() {
        let job = parse(&shlex::split(argv).unwrap(), *script, &Default::default()).unwrap();
        let res = job_desc_resources(&job.components[0]);
        let s = job.script.clone().unwrap();
        let t = ring
            .sign(&TokenClaims { uid: 1000, account: "mlteam".into(), script_sha256: script_sha256(s.as_bytes()), resources: res.clone(), decision_id: id, expiry: 1_900_000_000 })
            .unwrap();
        let mut add = |name: String, jd: Value, uid: u32, ok: bool| {
            want.push(format!("{name} rightsizer:{id}:{}", if ok { "verified" } else { "user" }));
            cases.push(json!({"name": name, "token": t, "uid": uid, "now": 1_899_999_000u64, "job_desc": jd}));
        };
        add(format!("s{i}_valid"), job_desc(&res, "mlteam", &s), 1000, true);
        add(format!("s{i}_uid"), job_desc(&res, "mlteam", &s), 0, false);
        add(format!("s{i}_account"), job_desc(&res, "root", &s), 1000, false);
        add(format!("s{i}_script"), job_desc(&res, "mlteam", &format!("{s} ")), 1000, false);
        let mut r = res.clone();
        r.min_mem_per_node = Some(r.min_mem_per_node.map_or(1, |m| m + 1));
        add(format!("s{i}_mem"), job_desc(&r, "mlteam", &s), 1000, false);
        let mut r = res.clone();
        r.cpus_per_task = Some(r.cpus_per_task.unwrap_or(1) + 1);
        add(format!("s{i}_cpus"), job_desc(&r, "mlteam", &s), 1000, false);
        let mut r = res.clone();
        r.tres_per_node = Some(format!("{},gres/gpu:9", r.tres_per_node.clone().unwrap_or_default()));
        add(format!("s{i}_gpus"), job_desc(&r, "mlteam", &s), 1000, false);
    }
    let dir = tempfile::tempdir().unwrap();
    let doc = dir.path().join("cases.json");
    std::fs::write(&doc, serde_json::to_vec(&json!({"keys": ring.to_text(), "cases": cases})).unwrap()).unwrap();
    let drv = dir.path().join("driver.lua");
    std::fs::write(&drv, DRIVER).unwrap();
    let contrib = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../contrib/slurm");
    let o = Command::new(&lua).arg(&drv).arg(&contrib).arg(&doc).output().unwrap();
    assert!(o.status.success(), "{lua}: {}", String::from_utf8_lossy(&o.stderr));
    let got: Vec<String> = String::from_utf8_lossy(&o.stdout).lines().map(str::to_string).collect();
    pretty_assertions::assert_eq!(got, want);
}
