//! Backend-agnostic conformance suite (SPEC §8): every case runs against
//! every backend. Gated backends (postgres via `RSZ_IT_POSTGRES_URL`, S3 /
//! MinIO via `RSZ_IT_S3_URL`) skip silently when their variable is unset.

mod common;

use std::any::Any;
use std::sync::Arc;

use chrono::{Duration, Utc};
use common::*;
use futures::future::BoxFuture;
use rightsizer_core::breaker::BreakerScope;
use rightsizer_core::{Mode, RunStatus, Tenant};
use rightsizer_history::{CompactOptions, DecisionFilter, HistoryStore, MemoryStore, ObjectHistoryStore, ObjectStoreOptions, PutOutcome, Role, StoreError};
use ulid::Ulid;

/// A store plus whatever keeps its backing resources alive.
pub struct Fx {
    pub store: Arc<dyn HistoryStore>,
    _guard: Box<dyn Any + Send>,
}

type Make = fn(Role) -> BoxFuture<'static, Option<Fx>>;

fn obj_opts(role: Role) -> ObjectStoreOptions {
    ObjectStoreOptions { role, ..Default::default() }
}

fn make_memory(role: Role) -> BoxFuture<'static, Option<Fx>> {
    Box::pin(async move {
        let mut m = MemoryStore::new();
        m.client_credentials = role == Role::Client;
        Some(Fx { store: Arc::new(m), _guard: Box::new(()) })
    })
}

fn make_object_local(role: Role) -> BoxFuture<'static, Option<Fx>> {
    Box::pin(async move {
        let d = tempfile::tempdir().unwrap();
        let s = ObjectHistoryStore::local(d.path(), obj_opts(role)).unwrap();
        Some(Fx { store: Arc::new(s), _guard: Box::new(d) })
    })
}

fn make_object_inmemory(role: Role) -> BoxFuture<'static, Option<Fx>> {
    Box::pin(async move { Some(Fx { store: Arc::new(ObjectHistoryStore::in_memory(obj_opts(role))), _guard: Box::new(()) }) })
}

#[cfg(feature = "sqlite")]
fn make_sqlite(role: Role) -> BoxFuture<'static, Option<Fx>> {
    Box::pin(async move {
        use rightsizer_history::sqlite::{SqliteOptions, SqliteStore};
        let d = tempfile::tempdir().unwrap();
        let s = SqliteStore::open_with(&d.path().join("h.db"), SqliteOptions { role, ..Default::default() }).unwrap();
        Some(Fx { store: Arc::new(s), _guard: Box::new(d) })
    })
}

#[cfg(feature = "postgres")]
fn make_postgres(role: Role) -> BoxFuture<'static, Option<Fx>> {
    Box::pin(async move {
        use rightsizer_history::postgres::{PostgresConfig, PostgresStore};
        let url = std::env::var("RSZ_IT_POSTGRES_URL").ok()?;
        let schema = format!("t_{}", Ulid::generate().to_string().to_lowercase());
        let mut cfg = PostgresConfig::from_url(&url).unwrap();
        cfg.schema = schema;
        // Operators migrate; a client connects to an already-migrated schema.
        cfg.role = Role::Operator;
        let op = PostgresStore::connect(cfg.clone()).await.unwrap();
        let store: Arc<dyn HistoryStore> = if role == Role::Operator {
            Arc::new(op)
        } else {
            cfg.role = Role::Client;
            Arc::new(PostgresStore::connect(cfg).await.unwrap())
        };
        Some(Fx { store, _guard: Box::new(()) })
    })
}

#[cfg(feature = "s3")]
fn make_s3(role: Role) -> BoxFuture<'static, Option<Fx>> {
    Box::pin(async move {
        // e.g. RSZ_IT_S3_URL="s3://rsz-it/conformance?endpoint=http://127.0.0.1:9000&region=us-east-1"
        // with AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY set.
        let base = std::env::var("RSZ_IT_S3_URL").ok()?;
        let (head, query) = base.split_once('?').map(|(h, q)| (h.to_string(), format!("?{q}"))).unwrap_or((base.clone(), String::new()));
        let uri = format!("{}/{}{query}", head.trim_end_matches('/'), Ulid::generate());
        let cfg = rightsizer_history::StoreConfig { role, ..rightsizer_history::StoreConfig::new(uri) };
        let store = rightsizer_history::open(&cfg).await.unwrap();
        Some(Fx { store, _guard: Box::new(()) })
    })
}

macro_rules! conformance {
    ($backend:ident, $make:expr) => {
        mod $backend {
            use super::*;
            const MAKE: Make = $make;
            #[tokio::test]
            async fn dedupe_order_limit() {
                cases::dedupe_order_limit(MAKE).await
            }
            #[tokio::test]
            async fn keys_isolated() {
                cases::keys_isolated(MAKE).await
            }
            #[tokio::test]
            async fn decisions_filters() {
                cases::decisions_filters(MAKE).await
            }
            #[tokio::test]
            async fn breaker_trip_reset() {
                cases::breaker_trip_reset(MAKE).await
            }
            #[tokio::test]
            async fn client_cannot_reset() {
                cases::client_cannot_reset(MAKE).await
            }
            #[tokio::test]
            async fn control_roundtrip() {
                cases::control_roundtrip(MAKE).await
            }
            #[tokio::test]
            async fn attempts_and_leases() {
                cases::attempts_and_leases(MAKE).await
            }
            #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
            async fn create_races_have_one_winner() {
                cases::create_races_have_one_winner(MAKE).await
            }
            #[tokio::test]
            async fn compaction_keeps_reads_and_applies_retention() {
                cases::compaction_keeps_reads_and_applies_retention(MAKE).await
            }
            #[tokio::test]
            async fn keys_and_export() {
                cases::keys_and_export(MAKE).await
            }
        }
    };
}

conformance!(memory, make_memory);
conformance!(object_local, make_object_local);
conformance!(object_inmemory_s3, make_object_inmemory);
#[cfg(feature = "sqlite")]
conformance!(sqlite, make_sqlite);
#[cfg(feature = "postgres")]
conformance!(postgres, make_postgres);
#[cfg(feature = "s3")]
conformance!(s3_minio, make_s3);

mod cases {
    use super::*;

    async fn fx(make: Make, role: Role) -> Option<Fx> {
        let f = make(role).await;
        if f.is_none() {
            eprintln!("skipped: backend not configured");
        }
        f
    }

    pub async fn dedupe_order_limit(make: Make) {
        let Some(f) = fx(make, Role::Operator).await else { return };
        let s = &f.store;
        let k = k1();
        assert_eq!(s.put_run(&rec(&k, "a", 1, t(0))).await.unwrap(), PutOutcome::Inserted);
        assert_eq!(s.put_run(&rec(&k, "a", 1, t(0))).await.unwrap(), PutOutcome::Duplicate, "same (key, inv, attempt, source)");
        let mut other_source = rec(&k, "a", 1, t(0));
        other_source.source = rightsizer_core::OutcomeSource::Cgroup;
        assert_eq!(s.put_run(&other_source).await.unwrap(), PutOutcome::Inserted, "another source is another record");
        s.put_run(&rec(&k, "a", 2, t(0))).await.unwrap();
        s.put_run(&rec(&k, "b", 1, t(100))).await.unwrap();
        s.put_run(&rec(&k, "c", 1, t(50))).await.unwrap();
        let v = s.runs(&k, 10).await.unwrap();
        assert_eq!(v.len(), 5);
        assert_eq!(invs(&v)[..3], ["b#1", "c#1", "a#2"], "newest first, then attempt");
        assert_eq!(s.runs(&k, 2).await.unwrap().len(), 2);
        assert!(s.runs(&k, 0).await.unwrap().is_empty());
        assert!(s.runs(&k2(), 10).await.unwrap().is_empty());
    }

    pub async fn keys_isolated(make: Make) {
        let Some(f) = fx(make, Role::Client).await else { return };
        let s = &f.store;
        let a = key("k8s://c1/ns/job/x");
        let b = key("k8s://c1/ns/job/y");
        let c = key("slurm://c1/ns/job/x");
        for (i, k) in [&a, &b, &c].iter().enumerate() {
            s.put_run(&rec(k, &format!("i{i}"), 1, t(i as i64))).await.unwrap();
        }
        for (i, k) in [&a, &b, &c].iter().enumerate() {
            let v = s.runs(k, 10).await.unwrap();
            assert_eq!(invs(&v), [format!("i{i}#1")]);
            assert_eq!(&v[0].key, *k);
        }
    }

    pub async fn decisions_filters(make: Make) {
        let Some(f) = fx(make, Role::Client).await else { return };
        let s = &f.store;
        let now = Utc::now();
        let d1 = decision(&k1(), now - Duration::days(3), Mode::Enforce, "rules-sized");
        let d2 = decision(&k1(), now - Duration::days(1), Mode::Recommend, "gbt");
        let d3 = decision(&k2(), now - Duration::hours(1), Mode::Enforce, "gbt");
        for d in [&d1, &d2, &d3] {
            s.put_decision(d).await.unwrap();
        }
        s.put_decision(&d1).await.unwrap(); // idempotent retry
        let ids = |v: Vec<rightsizer_core::Decision>| v.into_iter().map(|d| d.id).collect::<Vec<_>>();
        assert_eq!(ids(s.decisions(&DecisionFilter::default()).await.unwrap()), [d3.id, d2.id, d1.id], "newest first, no dups");
        assert_eq!(ids(s.decisions(&DecisionFilter { key: Some(k1()), ..Default::default() }).await.unwrap()), [d2.id, d1.id]);
        let tenant = DecisionFilter { tenant: Some(("flyte".into(), Tenant::new("acme/proj/dev").unwrap())), ..Default::default() };
        assert_eq!(ids(s.decisions(&tenant).await.unwrap()), [d3.id]);
        let window = DecisionFilter { since: Some(now - Duration::days(2)), until: Some(now - Duration::minutes(30)), ..Default::default() };
        assert_eq!(ids(s.decisions(&window).await.unwrap()), [d3.id, d2.id]);
        let tw = DecisionFilter { key: Some(k1()), since: Some(now - Duration::days(4)), until: Some(now), ..Default::default() };
        assert_eq!(ids(s.decisions(&tw).await.unwrap()), [d2.id, d1.id], "date partitions");
        assert_eq!(ids(s.decisions(&DecisionFilter { mode: Some(Mode::Enforce), ..Default::default() }).await.unwrap()), [d3.id, d1.id]);
        let est = DecisionFilter { estimator: Some(("gbt".into(), "1".into())), limit: Some(1), ..Default::default() };
        assert_eq!(ids(s.decisions(&est).await.unwrap()), [d3.id], "estimator + limit");
        assert_eq!(s.decision(d2.id).await.unwrap().map(|d| d.id), Some(d2.id));
        assert_eq!(s.decision(Ulid::generate()).await.unwrap(), None);
    }

    pub async fn breaker_trip_reset(make: Make) {
        let Some(f) = fx(make, Role::Operator).await else { return };
        let s = &f.store;
        let w = BreakerScope::Workload { key: k1() };
        assert!(!s.breaker_get(&w).await.unwrap().is_open());
        let t1 = trip(w.clone(), t(10));
        s.breaker_trip(&t1).await.unwrap();
        assert!(matches!(s.breaker_trip(&t1).await, Err(StoreError::Conflict(_))), "trips are create-only");
        assert!(s.breaker_get(&w).await.unwrap().is_open());
        s.breaker_reset(&reset(w.clone(), t(20))).await.unwrap();
        assert!(!s.breaker_get(&w).await.unwrap().is_open(), "reset newer than every trip");
        s.breaker_trip(&trip(w.clone(), t(15))).await.unwrap();
        assert!(!s.breaker_get(&w).await.unwrap().is_open(), "a trip older than the reset stays closed");
        s.breaker_trip(&trip(w.clone(), t(30))).await.unwrap();
        let objs = s.breaker_get(&w).await.unwrap();
        assert!(objs.is_open());
        assert_eq!(objs.trips.len(), 3);
        assert_eq!(objs.latest_reset(), Some(t(20)));
        // Other scopes are independent; fleet and estimator scopes work too.
        let fleet = BreakerScope::Fleet;
        let est = BreakerScope::EstimatorVersion { id: "gbt".into(), version: "1.2".into() };
        assert!(!s.breaker_get(&fleet).await.unwrap().is_open());
        s.breaker_trip(&trip(fleet.clone(), t(40))).await.unwrap();
        s.breaker_trip(&trip(est.clone(), t(41))).await.unwrap();
        let list = s.breaker_list().await.unwrap();
        let scopes: Vec<&BreakerScope> = list.iter().map(|(s, _)| s).collect();
        assert!(scopes.contains(&&w) && scopes.contains(&&fleet) && scopes.contains(&&est), "{scopes:?}");
        let (_, wo) = list.iter().find(|(s, _)| s == &w).unwrap();
        assert_eq!(wo.trips.len(), 3);
        assert!(!wo.resets.is_empty());
    }

    pub async fn client_cannot_reset(make: Make) {
        let Some(f) = fx(make, Role::Client).await else { return };
        let s = &f.store;
        assert_eq!(s.role(), Role::Client);
        let w = BreakerScope::Workload { key: k1() };
        s.breaker_trip(&trip(w.clone(), t(10))).await.unwrap();
        assert!(matches!(s.breaker_reset(&reset(w.clone(), t(20))).await, Err(StoreError::PermissionDenied(_))));
        assert!(s.breaker_get(&w).await.unwrap().is_open(), "still open");
        assert!(matches!(s.control_set(&control(Mode::Enforce)).await, Err(StoreError::PermissionDenied(_))));
        assert_eq!(s.control_get().await.unwrap(), None);
    }

    pub async fn control_roundtrip(make: Make) {
        let Some(f) = fx(make, Role::Operator).await else { return };
        let s = &f.store;
        assert_eq!(s.control_get().await.unwrap(), None);
        s.control_set(&control(Mode::Recommend)).await.unwrap();
        assert_eq!(s.control_get().await.unwrap().unwrap().mode_ceiling, Mode::Recommend);
        s.control_set(&control(Mode::Off)).await.unwrap();
        assert_eq!(s.control_get().await.unwrap().unwrap().mode_ceiling, Mode::Off);
    }

    pub async fn attempts_and_leases(make: Make) {
        let Some(f) = fx(make, Role::Client).await else { return };
        let s = &f.store;
        let k = k1();
        let now = Utc::now();
        assert_eq!(s.attempt_get(&k, "inv-1").await.unwrap(), None);
        let a = attempt(&k, "inv-1", now);
        assert!(s.attempt_put_if_absent(&a).await.unwrap());
        assert!(!s.attempt_put_if_absent(&a).await.unwrap(), "conditional create");
        let got = s.attempt_get(&k, "inv-1").await.unwrap().unwrap();
        assert!(got.reduced && got.lease.as_ref().unwrap().closed_at.is_none());
        assert_eq!(s.attempt_get(&k2(), "inv-1").await.unwrap(), None, "per key");
        s.attempt_close_lease(&k, "inv-1", now + Duration::seconds(5), RunStatus::OomKilled).await.unwrap();
        let l = s.attempt_get(&k, "inv-1").await.unwrap().unwrap().lease.unwrap();
        assert_eq!(l.outcome, Some(RunStatus::OomKilled));
        assert!(l.closed_at.is_some());
        assert!(s.attempt_close_lease(&k, "missing", now, RunStatus::Succeeded).await.is_err());
        if s.info().backend != rightsizer_history::Backend::Memory {
            // Records expire after 7 d: unreadable, and replaceable.
            let old = attempt(&k, "inv-old", now - Duration::days(8));
            assert!(s.attempt_put_if_absent(&old).await.unwrap());
            assert_eq!(s.attempt_get(&k, "inv-old").await.unwrap(), None, "expired");
            assert!(s.attempt_put_if_absent(&attempt(&k, "inv-old", now)).await.unwrap(), "an expired record is replaced");
            assert!(s.attempt_get(&k, "inv-old").await.unwrap().is_some());
        }
    }

    pub async fn create_races_have_one_winner(make: Make) {
        let Some(f) = fx(make, Role::Client).await else { return };
        let s = f.store.clone();
        let n = 16;
        let a = attempt(&k1(), "race", Utc::now());
        let tasks: Vec<_> = (0..n)
            .map(|_| {
                let s = s.clone();
                let a = a.clone();
                tokio::spawn(async move { s.attempt_put_if_absent(&a).await })
            })
            .collect();
        let mut wins = 0;
        for t in tasks {
            if t.await.unwrap().unwrap() {
                wins += 1;
            }
        }
        assert_eq!(wins, 1, "exactly one attempt record creator wins");

        let tr = trip(BreakerScope::Fleet, t(1));
        let tasks: Vec<_> = (0..n)
            .map(|_| {
                let s = s.clone();
                let tr = tr.clone();
                tokio::spawn(async move { s.breaker_trip(&tr).await })
            })
            .collect();
        let mut ok = 0;
        for t in tasks {
            match t.await.unwrap() {
                Ok(()) => ok += 1,
                Err(StoreError::Conflict(_)) => {}
                Err(e) => panic!("{e}"),
            }
        }
        assert_eq!(ok, 1, "exactly one trip creator wins");

        // Concurrent lease closes all land (conditional updates retry).
        let tasks: Vec<_> = (0..8)
            .map(|i| {
                let s = s.clone();
                tokio::spawn(async move { s.attempt_close_lease(&k1(), "race", Utc::now(), if i % 2 == 0 { RunStatus::Succeeded } else { RunStatus::Failed }).await })
            })
            .collect();
        for t in tasks {
            t.await.unwrap().unwrap();
        }
        assert!(s.attempt_get(&k1(), "race").await.unwrap().unwrap().lease.unwrap().closed_at.is_some());
    }

    pub async fn compaction_keeps_reads_and_applies_retention(make: Make) {
        let Some(f) = fx(make, Role::Operator).await else { return };
        let s = &f.store;
        let k = k1();
        let now = Utc::now();
        for i in 0..5 {
            s.put_run(&rec(&k, &format!("r{i}"), 1, now - Duration::hours(10 - i))).await.unwrap();
        }
        s.put_run(&rec(&k, "ancient", 1, now - Duration::days(100))).await.unwrap();
        s.put_decision(&decision(&k, now - Duration::days(40), Mode::Enforce, "gbt")).await.unwrap();
        let recent = decision(&k, now - Duration::hours(1), Mode::Enforce, "gbt");
        s.put_decision(&recent).await.unwrap();
        let before = s.runs(&k, 100).await.unwrap();
        assert_eq!(before.len(), 6);

        // Two days later: every shard is past grace and settle.
        let mut opts = CompactOptions::at(now + Duration::days(2));
        opts.decision_max_age = std::time::Duration::from_secs(30 * 86_400);
        let report = s.compact(&opts).await.unwrap();
        let after = s.runs(&k, 100).await.unwrap();
        assert_eq!(invs(&after), invs(&before[..5]), "same reads minus the record past max_age ({report:?})");
        if s.info().backend != rightsizer_history::Backend::Memory {
            assert_eq!(s.decision(recent.id).await.unwrap().map(|d| d.id), Some(recent.id));
            let all = s.decisions(&DecisionFilter::default()).await.unwrap();
            assert_eq!(all.len(), 1, "decisions past 30 d are gone from the log");
        }
        // Writes after compaction are visible (LIST after the watermark) and deduplicated.
        s.put_run(&rec(&k, "late", 1, now)).await.unwrap();
        assert_eq!(s.put_run(&rec(&k, "r0", 1, now - Duration::hours(10))).await.unwrap(), PutOutcome::Duplicate, "compacted records still dedupe");
        let v = s.runs(&k, 3).await.unwrap();
        assert_eq!(invs(&v), ["late#1", "r4#1", "r3#1"]);
        // Compacting again is stable.
        s.compact(&CompactOptions::at(now + Duration::days(3))).await.unwrap();
        assert_eq!(s.runs(&k, 100).await.unwrap().len(), 6);
        // Dry runs change nothing.
        let mut dry = CompactOptions::at(now + Duration::days(200));
        dry.dry_run = true;
        s.compact(&dry).await.unwrap();
        assert_eq!(s.runs(&k, 100).await.unwrap().len(), 6);
    }

    pub async fn keys_and_export(make: Make) {
        let Some(f) = fx(make, Role::Client).await else { return };
        let s = &f.store;
        s.put_run(&rec(&k1(), "a", 1, t(0))).await.unwrap();
        s.put_run(&rec(&k2(), "b", 1, t(0))).await.unwrap();
        s.put_decision(&decision(&k1(), Utc::now(), Mode::Enforce, "gbt")).await.unwrap();
        let mut keys: Vec<String> = s.keys().await.unwrap().into_iter().map(|k| k.to_string()).collect();
        keys.sort();
        assert_eq!(keys, [k2().to_string(), k1().to_string()]);
        let (bytes, sum) = rightsizer_history::export::export_parquet_bytes(s.as_ref(), &rightsizer_history::export::ExportFilter::all()).await.unwrap();
        assert_eq!((sum.runs, sum.decisions), (2, 1));
        let rows = rightsizer_history::snapshot::read_snapshot(&bytes).unwrap();
        assert_eq!(rows.len(), 3);
    }
}
