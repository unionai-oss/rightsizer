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

const FUZZ_DRIVER: &str = r#"
package.path = arg[1] .. "/?.lua;" .. package.path
local json = require("json")
local tok = require("rightsizer_token")
local f = assert(io.open(arg[2], "rb")); local doc = json.decode(f:read("*a")); f:close()
local keys = assert(tok.parse_keys(doc.keys))
local out = {}
for i, c in ipairs(doc.cases) do
  local st = tok.verify(keys, c.token, c.job_desc, c.uid, c.now, 120)
  out[i] = { canonical = tok.canonical(c.job_desc), status = st }
end
io.write(json.encode(out))
"#;

/// Same random vectors through both implementations: Rust's canonical form
/// must be Lua's, byte for byte, and every Rust-signed token must verify in
/// Lua (spellings, sentinels, sort order, float formatting of numbers).
#[test]
fn fuzz_canonical_and_mac_agree_with_lua() {
    let Some(lua) = lua() else {
        eprintln!("no Lua interpreter on PATH: skipping");
        return;
    };
    let mut seed: u64 = 0x9e37_79b9_7f4a_7c15;
    let mut rnd = move |n: usize| {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        (seed % n as u64) as usize
    };
    const PIECES: &[&str] = &[
        "gres/", "gres:", "license/", "license:", "gpu", "GPU", "a100", "h100", "shard", "mps", "cpu", "mem", "node", "billing", "energy", "fs/disk", "bb/x", ":", ":", "=", ",", ",", " ", "1",
        "2", "8", "16", "4096", "10k", "2m", "3G", "7t", "1p", "x", "-", "_", ".", "/", "Gres/", "CPU=4",
    ];
    let tres = |r: &mut dyn FnMut(usize) -> usize| -> Option<String> {
        if r(4) == 0 {
            return None;
        }
        Some((0..r(12)).map(|_| PIECES[r(PIECES.len())]).collect())
    };
    let ring = KeyRing::generate().unwrap();
    let id = Ulid::from_string("01JDZ7Q4W8M7C9B5V3N2X1Y0ZC").unwrap();
    let mut cases = Vec::new();
    let mut want = Vec::new();
    for i in 0..400 {
        let num = |r: &mut dyn FnMut(usize) -> usize| -> Option<u64> {
            match r(5) {
                0 => None,
                1 => Some(0),
                2 => Some(r(1 << 20) as u64),
                3 => Some((1u64 << 40) + r(1000) as u64),
                _ => Some(r(100) as u64),
            }
        };
        let res = JobDescResources {
            min_mem_per_node: num(&mut rnd),
            min_mem_per_cpu: num(&mut rnd),
            cpus_per_task: match rnd(5) {
                0 => None,
                1 => Some(0),
                2 => Some(65534),
                _ => Some(rnd(512) as u32 + 1),
            },
            tres_per_node: tres(&mut rnd),
            tres_per_job: tres(&mut rnd),
            tres_per_task: tres(&mut rnd),
            tres_per_socket: tres(&mut rnd),
            mem_per_tres: tres(&mut rnd),
            pn_min_tmp_disk: if rnd(3) == 0 { Some(4_294_967_294) } else { num(&mut rnd) },
        };
        let script = format!("#!/bin/sh\necho {i}\n");
        let account = ["mlteam", "MLTeam", "", "a.b-c_d"][rnd(4)];
        let token = ring
            .sign(&TokenClaims { uid: 1000 + i, account: account.into(), script_sha256: script_sha256(script.as_bytes()), resources: res.clone(), decision_id: id, expiry: 1_900_000_000 })
            .unwrap();
        // job_desc as Lua sees it: unset numbers nil (memory) or sentinels.
        let mut jd = json!({"account": account, "script": script});
        for (k, v) in [("min_mem_per_node", res.min_mem_per_node), ("min_mem_per_cpu", res.min_mem_per_cpu), ("pn_min_tmp_disk", res.pn_min_tmp_disk)] {
            if let Some(v) = v {
                jd[k] = json!(v as f64);
            }
        }
        jd["cpus_per_task"] = json!(res.cpus_per_task.unwrap_or(65534) as f64);
        for (k, v) in [("tres_per_node", &res.tres_per_node), ("tres_per_job", &res.tres_per_job), ("tres_per_task", &res.tres_per_task), ("tres_per_socket", &res.tres_per_socket), ("mem_per_tres", &res.mem_per_tres)] {
            if let Some(v) = v {
                jd[k] = json!(v);
            }
        }
        cases.push(json!({"token": token, "uid": (1000 + i) as f64, "now": 1_899_999_000_f64, "job_desc": jd}));
        want.push(json!({"canonical": res.canonical(), "status": "verified"}));
    }
    let dir = tempfile::tempdir().unwrap();
    let doc = dir.path().join("cases.json");
    std::fs::write(&doc, serde_json::to_vec(&json!({"keys": ring.to_text(), "cases": cases})).unwrap()).unwrap();
    let drv = dir.path().join("fuzz.lua");
    std::fs::write(&drv, FUZZ_DRIVER).unwrap();
    let contrib = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../contrib/slurm");
    let o = Command::new(&lua).arg(&drv).arg(&contrib).arg(&doc).output().unwrap();
    assert!(o.status.success(), "{lua}: {}", String::from_utf8_lossy(&o.stderr));
    let got: Vec<Value> = serde_json::from_slice(&o.stdout).unwrap();
    for (i, (g, w)) in got.iter().zip(&want).enumerate() {
        pretty_assertions::assert_eq!(g, w, "case {i}: {}", cases[i]["job_desc"]);
    }
    assert_eq!(got.len(), want.len());
}
