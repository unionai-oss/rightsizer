//! Object-store specifics: layout, watermark reads after compaction,
//! snapshots, the compaction lease, the local `Update` emulation, N/N-1
//! reading, and the Python writer (`rightsizer_runtime.record`).

mod common;

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use chrono::{Duration, Utc};
use common::*;
use rightsizer_core::{Mode, RunStatus};
use rightsizer_history::object::LatestPointer;
use rightsizer_history::snapshot::{COLUMNS, SnapshotRow, column_names, read_snapshot};
use rightsizer_history::{CompactOptions, HistoryStore, ObjectHistoryStore, ObjectStoreOptions, PutOutcome, Role, StoreError};
use ulid::Ulid;

fn files(root: &Path) -> Vec<String> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).unwrap() {
            let p = e.unwrap().path();
            if p.is_dir() {
                stack.push(p);
            } else {
                out.push(p.strip_prefix(root).unwrap().to_string_lossy().replace('\\', "/"));
            }
        }
    }
    out.sort();
    out
}

fn op() -> ObjectStoreOptions {
    ObjectStoreOptions { role: Role::Operator, ..Default::default() }
}

#[tokio::test]
async fn layout_matches_spec() {
    let d = tempfile::tempdir().unwrap();
    let s = ObjectHistoryStore::local(d.path(), op()).unwrap();
    let k = k2();
    s.put_run(&rec(&k, "a", 1, t(0))).await.unwrap();
    let dec = decision(&k, t(0), Mode::Enforce, "gbt");
    s.put_decision(&dec).await.unwrap();
    s.attempt_put_if_absent(&attempt(&k, "inv", Utc::now())).await.unwrap();
    s.breaker_trip(&trip(rightsizer_core::breaker::BreakerScope::Fleet, t(1))).await.unwrap();
    s.control_set(&control(Mode::Recommend)).await.unwrap();
    let f = files(d.path());
    let hash = "eef92d58436c80afb2670dc464a4b4f2"; // pinned in IMPLEMENTATION-NOTES (runtime section)
    assert!(f.iter().any(|p| p.starts_with(&format!("v1/runs/flyte/acme/proj/dev/{hash}/")) && p.ends_with(".json")), "{f:?}");
    assert!(f.contains(&format!("v1/decisions/flyte/acme/proj/dev/2023-11-14/{}.json", dec.id)), "{f:?}");
    assert!(f.iter().any(|p| p.starts_with(&format!("v1/attempts/flyte/acme/proj/dev/{hash}/"))));
    assert!(f.iter().any(|p| p.starts_with("v1/breakers/fleet/trips/")));
    assert!(f.contains(&"v1/control/mode.json".to_string()));
    assert!(!f.iter().any(|p| p.ends_with(".lock")), "no lock files left behind");
}

#[tokio::test]
async fn watermark_pointer_snapshots_and_deletion() {
    let d = tempfile::tempdir().unwrap();
    let s = ObjectHistoryStore::local(d.path(), op()).unwrap();
    let k = k1();
    let now = Utc::now();
    for i in 0..70 {
        s.put_run(&rec(&k, &format!("r{i:02}"), 1, now - Duration::minutes(100 - i))).await.unwrap();
    }
    let dec = decision(&k, now - Duration::hours(30), Mode::Enforce, "gbt");
    s.put_decision(&dec).await.unwrap();

    // Compaction "now": nothing is past grace; pointer written, nothing deleted.
    let r = s.compact(&CompactOptions::at(now + Duration::minutes(10))).await.unwrap();
    assert_eq!((r.pointers, r.deleted), (1, 0), "{r:?}");
    let ptr_path = d.path().join(format!("v1/latest/k8s/c1/ns/{}.json", k.hash()));
    let ptr: LatestPointer = serde_json::from_slice(&std::fs::read(&ptr_path).unwrap()).unwrap();
    assert_eq!(ptr.records.len(), 64, "keep_last");
    let wm = ptr.watermark.expect("watermark");
    // Readers see the pointer's keep_last plus shards after the watermark;
    // older shards are only kept (until the grace period) for snapshots.
    assert_eq!(s.runs(&k, 100).await.unwrap().len(), 64);
    assert!(s.runs(&k, 64).await.unwrap().iter().all(|r| r.invocation_id.as_str() >= "r06"));

    // A shard written after compaction is listed after the watermark.
    s.put_run(&rec(&k, "new", 1, now)).await.unwrap();
    assert_eq!(s.runs(&k, 1).await.unwrap()[0].invocation_id, "new");
    let shards: Vec<String> = files(d.path()).into_iter().filter(|p| p.starts_with("v1/runs/")).collect();
    assert!(shards.iter().any(|p| p.ends_with(".json") && Ulid::from_string(&p[p.len() - 31..p.len() - 5]).unwrap() > wm));

    // Two days on: shards past both the watermark and the grace period are
    // deleted, after landing in the daily snapshot; the pointer keeps 64.
    let r = s.compact(&CompactOptions::at(now + Duration::days(2))).await.unwrap();
    assert_eq!(r.deleted, 71, "{r:?}");
    assert!(r.snapshots >= 1);
    let f = files(d.path());
    assert!(!f.iter().any(|p| p.starts_with("v1/runs/")), "{f:?}");
    let v = s.runs(&k, 100).await.unwrap();
    assert_eq!(v.len(), 64, "only the pointer's keep_last remain readable");
    assert_eq!(v[0].invocation_id, "new");
    let snaps: Vec<&String> = f.iter().filter(|p| p.starts_with("v1/snapshots/k8s/c1/ns/") && p.ends_with(".parquet")).collect();
    assert!(!snaps.is_empty(), "{f:?}");
    let mut rows = Vec::new();
    for p in &snaps {
        let b = std::fs::read(d.path().join(p)).unwrap();
        assert_eq!(column_names(&b).unwrap(), COLUMNS);
        rows.extend(read_snapshot(&b).unwrap());
    }
    let runs = rows.iter().filter(|r| matches!(r, SnapshotRow::Run(_))).count();
    let decs = rows.iter().filter(|r| matches!(r, SnapshotRow::Decision(_))).count();
    assert_eq!((runs, decs), (71, 1), "every deleted shard and the past-grace decision are in snapshots");
    // The decision shard stays (30 d retention); compaction status is recorded.
    assert!(s.decision(dec.id).await.unwrap().is_some());
    let st = s.stats().await.unwrap();
    let last = st.last_compaction.unwrap();
    assert_eq!(last.run_shards_deleted, 71);
    assert_eq!(st.shards, Some(0));
    assert!(st.compaction_lag_seconds.is_some());

    // A late shard for an already-snapshotted day merges into the same file.
    s.put_run(&rec(&k, "late", 1, now - Duration::minutes(100))).await.unwrap();
    s.compact(&CompactOptions::at(now + Duration::days(4))).await.unwrap();
    let mut rows = Vec::new();
    for p in files(d.path()).iter().filter(|p| p.starts_with("v1/snapshots/")) {
        rows.extend(read_snapshot(&std::fs::read(d.path().join(p)).unwrap()).unwrap());
    }
    assert_eq!(rows.iter().filter(|r| matches!(r, SnapshotRow::Run(_))).count(), 72, "merged, not replaced; decision not duplicated");
    assert_eq!(rows.iter().filter(|r| matches!(r, SnapshotRow::Decision(_))).count(), 1);

    // Retention: snapshots older than max_age are deleted.
    s.compact(&CompactOptions::at(now + Duration::days(200))).await.unwrap();
    assert!(!files(d.path()).iter().any(|p| p.starts_with("v1/snapshots/")));
    assert!(s.runs(&k, 100).await.unwrap().is_empty(), "pointer records past max_age dropped");
    assert!(s.decision(dec.id).await.unwrap().is_none(), "decision shards past 30 d deleted");
}

#[tokio::test]
async fn compaction_lease_is_a_singleton_with_takeover() {
    for local in [true, false] {
        let d = tempfile::tempdir().unwrap();
        let s = Arc::new(if local { ObjectHistoryStore::local(d.path(), op()).unwrap() } else { ObjectHistoryStore::in_memory(op()) });
        let now = Utc::now();
        let ttl = std::time::Duration::from_secs(3600);
        s.acquire_compaction_lease("A", now, ttl).await.unwrap();
        assert!(matches!(s.acquire_compaction_lease("B", now + Duration::minutes(5), ttl).await, Err(StoreError::Conflict(_))));
        assert!(matches!(s.compact(&CompactOptions::at(now)).await, Err(StoreError::Conflict(m)) if m.contains("lease")), "compact refuses while held");
        s.acquire_compaction_lease("A", now + Duration::minutes(5), ttl).await.unwrap(); // renew
        // Expired: N takers race for it, exactly one wins (conditional PUT on the ETag).
        let later = now + Duration::hours(3);
        let tasks: Vec<_> = (0..12)
            .map(|i| {
                let s = s.clone();
                tokio::spawn(async move { s.acquire_compaction_lease(&format!("T{i}"), later, ttl).await })
            })
            .collect();
        let mut wins = 0;
        for t in tasks {
            match t.await.unwrap() {
                Ok(_) => wins += 1,
                Err(StoreError::Conflict(_)) => {}
                Err(e) => panic!("{e}"),
            }
        }
        assert_eq!(wins, 1, "local={local}");
        // Released leases can be created again.
        for i in 0..12 {
            s.release_compaction_lease(&format!("T{i}")).await.unwrap();
        }
        s.acquire_compaction_lease("C", later, ttl).await.unwrap();
    }
}

#[tokio::test]
async fn stale_lock_objects_are_broken() {
    let d = tempfile::tempdir().unwrap();
    let opts = ObjectStoreOptions { lock_stale_after: std::time::Duration::from_millis(50), ..op() };
    let s = ObjectHistoryStore::local(d.path(), opts).unwrap();
    let k = k1();
    s.attempt_put_if_absent(&attempt(&k, "i", Utc::now())).await.unwrap();
    // A crashed writer left its lock behind.
    let rel = files(d.path()).into_iter().find(|p| p.starts_with("v1/attempts/")).unwrap();
    std::fs::write(d.path().join(format!("{rel}.lock")), b"{}").unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(120)).await;
    s.attempt_close_lease(&k, "i", Utc::now(), RunStatus::Succeeded).await.unwrap();
    assert_eq!(s.attempt_get(&k, "i").await.unwrap().unwrap().lease.unwrap().outcome, Some(RunStatus::Succeeded));
}

#[tokio::test]
async fn reads_n_and_n_minus_1_versions() {
    let d = tempfile::tempdir().unwrap();
    let s = ObjectHistoryStore::local(d.path(), op()).unwrap();
    let k = k1();
    let dir = d.path().join(format!("v1/runs/k8s/c1/ns/{}", k.hash()));
    std::fs::create_dir_all(&dir).unwrap();
    // v1 with an unknown field and without optional fields (`v`, `inputs`,
    // `status_confidence`, measurements are all defaulted).
    let old = serde_json::json!({
        "key": k.to_string(), "invocation_id": "old", "attempt": 1, "started_at": "2026-11-18T12:00:00.000000Z",
        "status": "succeeded", "source": {"kind": "self_report"}, "trust": "unverified",
        "from_the_future": {"nested": true}
    });
    std::fs::write(dir.join(format!("{}.json", Ulid::generate())), serde_json::to_vec(&old).unwrap()).unwrap();
    // A version this build does not read is skipped, not fatal.
    let mut future = old.clone();
    future["v"] = 99.into();
    future["invocation_id"] = "future".into();
    std::fs::write(dir.join(format!("{}.json", Ulid::generate())), serde_json::to_vec(&future).unwrap()).unwrap();
    // A corrupt shard and a writer's temp file are skipped too.
    std::fs::write(dir.join(format!("{}.json", Ulid::generate())), b"{not json").unwrap();
    std::fs::write(dir.join(".rs-abc.tmp"), b"partial").unwrap();
    let v = s.runs(&k, 10).await.unwrap();
    assert_eq!(invs(&v), ["old#1"]);
    assert_eq!(v[0].v, 1);
    assert!(v[0].inputs.is_empty() && v[0].memory_peak.is_none());
    assert!(s.stats().await.unwrap().skipped_on_read >= 2);
    // Written records carry `write_version`.
    s.put_run(&rec(&k, "new", 1, t(0))).await.unwrap();
    let newest = files(d.path()).into_iter().filter(|p| p.starts_with("v1/runs/")).max().unwrap();
    let j: serde_json::Value = serde_json::from_slice(&std::fs::read(d.path().join(newest)).unwrap()).unwrap();
    assert_eq!(j["v"], 1);
    assert!(matches!(
        rightsizer_history::check_write_version(2),
        Err(StoreError::Config(_))
    ));
}

#[tokio::test]
async fn duplicates_across_writers_are_merged_on_read() {
    // Two writers that do not check before writing (the Python writer
    // never does): both shards land; reads and compaction dedupe.
    let d = tempfile::tempdir().unwrap();
    let opts = ObjectStoreOptions { check_duplicates_on_write: false, ..op() };
    let s = ObjectHistoryStore::local(d.path(), opts).unwrap();
    let k = k1();
    let at = Utc::now();
    assert_eq!(s.put_run(&rec(&k, "a", 1, at)).await.unwrap(), PutOutcome::Inserted);
    assert_eq!(s.put_run(&rec(&k, "a", 1, at)).await.unwrap(), PutOutcome::Inserted);
    assert_eq!(files(d.path()).iter().filter(|p| p.starts_with("v1/runs/")).count(), 2);
    assert_eq!(s.runs(&k, 10).await.unwrap().len(), 1);
    s.compact(&CompactOptions::at(Utc::now() + Duration::days(2))).await.unwrap();
    assert_eq!(s.runs(&k, 10).await.unwrap().len(), 1);
}

// ── Python writer ───────────────────────────────────────────────────────

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).ancestors().nth(2).unwrap().to_path_buf()
}

fn python() -> Option<PathBuf> {
    let p = repo_root().join(".venv/bin/python");
    p.exists().then_some(p)
}

const PY_WRITE: &str = r#"
import datetime as dt, sys
from rightsizer_runtime.record import RunRecordWriter
from rightsizer_runtime.wire import Measurement, Censor, OutcomeSource, RunRecord, RunStatus, Trust

root = sys.argv[1]
T0 = dt.datetime(2026, 11, 18, 12, tzinfo=dt.timezone.utc)
w = RunRecordWriter(root)
def rec(key, inv, attempt, minutes, **kw):
    return RunRecord(key=key, invocation_id=inv, attempt=attempt, started_at=T0 + dt.timedelta(minutes=minutes),
                     status=kw.pop("status", RunStatus.SUCCEEDED), source=kw.pop("source", OutcomeSource.cgroup()), **kw)
k = "flyte://acme/proj/dev/env.train"
ok = all([
    w.write(rec(k, "inv-1", 1, 0, memory_peak=Measurement(2.5e9, Censor.EXACT, 1.0), trust=Trust.AUTHENTICATED,
                cluster="c1", code_digest="sha256:x", duration_s=3.5)),
    w.write(rec(k, "inv-1", 2, 5, status=RunStatus.OOM_KILLED, memory_peak=Measurement(4e9, Censor.AT_LEAST))),
    w.write(rec(k, "inv-1", 2, 5, status=RunStatus.OOM_KILLED, memory_peak=Measurement(4e9, Censor.AT_LEAST))),  # retried duplicate
    w.write(rec(k, "inv-2", 1, 10, source=OutcomeSource.importer("flyte"))),
    w.write(rec("ray://team-a//mod.fn", "r-1", 1, 0)),
    w.write(rec("k8s://c1/ns/pod/x/main", "p-1", 1, 0)),
])
print("ok" if ok else "fail", w.stats.written)
"#;

const PY_READ: &str = r#"
import pathlib, sys
from rightsizer_runtime.wire import RunRecord
n = 0
for p in sorted(pathlib.Path(sys.argv[1]).rglob("*.json")):
    if "/v1/runs/" in p.as_posix():
        RunRecord.from_json(p.read_text())  # strict: unknown keys rejected
        n += 1
print(n)
"#;

fn run_py(script: &str, arg: &Path) -> Option<String> {
    let py = python()?;
    let out = Command::new(py)
        .env("PYTHONPATH", repo_root().join("python/rightsizer-runtime/src"))
        .arg("-c")
        .arg(script)
        .arg(arg)
        .output()
        .unwrap();
    assert!(out.status.success(), "python failed: {}", String::from_utf8_lossy(&out.stderr));
    Some(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

#[tokio::test]
async fn reads_what_the_python_writer_writes() {
    let d = tempfile::tempdir().unwrap();
    let Some(out) = run_py(PY_WRITE, d.path()) else {
        eprintln!("skipped: no .venv/bin/python");
        return;
    };
    assert_eq!(out, "ok 6");
    // A temp file from an interrupted Python write must be ignored.
    let flyte = k2();
    let dir = d.path().join(format!("v1/runs/flyte/acme/proj/dev/{}", flyte.hash()));
    assert!(dir.is_dir(), "Python's tenant path and key hash match Rust's: {:?}", files(d.path()));
    std::fs::write(dir.join(".rs-zzzz.tmp"), b"{").unwrap();

    let s = ObjectHistoryStore::local(d.path(), op()).unwrap();
    let v = s.runs(&flyte, 10).await.unwrap();
    assert_eq!(invs(&v), ["inv-2#1", "inv-1#2", "inv-1#1"], "deduplicated, newest first");
    let first = v.iter().find(|r| r.attempt == 1 && r.invocation_id == "inv-1").unwrap();
    assert_eq!(first.memory_peak.unwrap().value, 2.5e9);
    assert_eq!(first.memory_peak.unwrap().resolution_s, Some(1.0));
    assert_eq!(first.trust, rightsizer_core::Trust::Authenticated);
    assert_eq!(first.cluster.as_deref(), Some("c1"));
    assert_eq!(v[1].status, RunStatus::OomKilled);
    assert_eq!(v[0].source, rightsizer_core::OutcomeSource::Importer { platform: "flyte".into() });
    assert_eq!(s.runs(&key("ray://team-a//mod.fn"), 10).await.unwrap().len(), 1);
    assert_eq!(s.runs(&key("k8s://c1/ns/pod/x/main"), 10).await.unwrap().len(), 1);
    assert_eq!(s.put_run(&v[1]).await.unwrap(), PutOutcome::Duplicate, "Rust dedupes against Python shards");
    let mut keys: Vec<String> = s.keys().await.unwrap().into_iter().map(|k| k.to_string()).collect();
    keys.sort();
    assert_eq!(keys.len(), 3, "{keys:?}");

    // Compaction folds Python shards into the pointer; Python keeps writing; reads see both.
    s.compact(&CompactOptions::at(Utc::now() + Duration::days(2))).await.unwrap();
    assert_eq!(s.runs(&flyte, 10).await.unwrap().len(), 3);
    run_py(PY_WRITE, d.path()).unwrap();
    assert_eq!(s.runs(&flyte, 10).await.unwrap().len(), 3, "a second Python pass is all duplicates");

    // And Python (strict) reads what Rust writes.
    s.put_run(&rec(&flyte, "from-rust", 1, Utc::now())).await.unwrap();
    let n: usize = run_py(PY_READ, d.path()).unwrap().parse().unwrap();
    assert!(n >= 1);
}

#[tokio::test]
async fn breaker_paths_carry_the_platform_and_read_the_legacy_layout() {
    use rightsizer_core::breaker::BreakerScope;
    let d = tempfile::tempdir().unwrap();
    let s = ObjectHistoryStore::local(d.path(), op()).unwrap();
    let k = k2();
    let scope = BreakerScope::Workload { key: k.clone() };
    // A trip written by an N-1 build: no platform segment.
    let old = trip(scope.clone(), t(1));
    let legacy = d.path().join(format!("v1/breakers/{}/trips/{}.json", scope.legacy_path().unwrap(), old.id));
    std::fs::create_dir_all(legacy.parent().unwrap()).unwrap();
    std::fs::write(&legacy, serde_json::to_vec(&old).unwrap()).unwrap();
    // A new trip lands under `{platform}/{tenant}`, like runs.
    let new = trip(scope.clone(), t(2));
    s.breaker_trip(&new).await.unwrap();
    let f = files(d.path());
    let hash = k.hash();
    assert!(f.contains(&format!("v1/breakers/workload/flyte/acme/proj/dev/{hash}/trips/{}.json", new.id)), "{f:?}");
    let o = s.breaker_get(&scope).await.unwrap();
    assert_eq!(o.trips.iter().map(|t| t.id).collect::<Vec<_>>(), vec![old.id, new.id], "both layouts read");
    // A reset at the new path closes both.
    s.breaker_reset(&reset(scope.clone(), t(3))).await.unwrap();
    assert!(!s.breaker_get(&scope).await.unwrap().is_open());
    assert_eq!(s.breaker_list().await.unwrap().len(), 1);
}
