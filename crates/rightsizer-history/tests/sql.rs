//! sqlite and postgres specifics: N/N-1 rows, the compaction lease,
//! multi-connection races, stats. Postgres runs only with
//! `RSZ_IT_POSTGRES_URL` set.

#![cfg(any(feature = "sqlite", feature = "postgres"))]

mod common;

use std::sync::Arc;

use chrono::{Duration, Utc};
use common::*;
#[cfg(feature = "sqlite")]
use rightsizer_history::sqlite::{SqliteOptions, SqliteStore};
use rightsizer_history::{CompactOptions, HistoryStore, Role, StoreError};

fn old_row(k: &rightsizer_core::WorkloadKey) -> serde_json::Value {
    serde_json::json!({
        "key": k.to_string(), "invocation_id": "old", "attempt": 1, "started_at": "2026-11-18T12:00:00Z",
        "status": "succeeded", "source": {"kind": "sacct"}, "trust": "verified", "unknown_field": [1, 2, 3]
    })
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_reads_n_and_n_minus_1_rows() {
    let s = SqliteStore::in_memory().unwrap();
    let k = k1();
    {
        let c = s.connection().lock().unwrap();
        let ins = "INSERT INTO runs (key, tenant, invocation_id, attempt, source, started_at_ms, written_at_ms, v, body) VALUES (?1, 'k8s/c1/ns', ?2, 1, 's', ?3, 0, ?4, ?5)";
        c.execute(ins, rusqlite::params![k.to_string(), "old", 2, 1, old_row(&k).to_string()]).unwrap();
        let mut fut = old_row(&k);
        fut["v"] = 7.into();
        c.execute(ins, rusqlite::params![k.to_string(), "future", 1, 7, fut.to_string()]).unwrap();
        c.execute(ins, rusqlite::params![k.to_string(), "corrupt", 0, 1, "{oops"]).unwrap();
    }
    let v = s.runs(&k, 10).await.unwrap();
    assert_eq!(invs(&v), ["old#1"]);
    assert_eq!(v[0].source, rightsizer_core::OutcomeSource::Sacct);
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_lease_takeover_and_stats() {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("h.db");
    let s = SqliteStore::open_with(&p, SqliteOptions { role: Role::Operator, ..Default::default() }).unwrap();
    let now = Utc::now();
    let ttl = std::time::Duration::from_secs(3600);
    s.acquire_compaction_lease("A", now, ttl).await.unwrap();
    assert!(matches!(s.acquire_compaction_lease("B", now, ttl).await, Err(StoreError::Conflict(_))));
    assert!(matches!(s.compact(&CompactOptions::at(now)).await, Err(StoreError::Conflict(_))));
    s.acquire_compaction_lease("B", now + Duration::hours(2), ttl).await.unwrap();
    s.release_compaction_lease("B").await.unwrap();
    s.put_run(&rec(&k1(), "a", 1, now)).await.unwrap();
    s.compact(&CompactOptions::at(now + Duration::days(1))).await.unwrap();
    let st = s.stats().await.unwrap();
    assert_eq!(st.shards, Some(1));
    assert!(st.last_compaction.is_some() && st.compaction_lag_seconds.is_some());
}

#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_races_across_connections() {
    // Several processes' worth of connections on one WAL database.
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("h.db");
    let stores: Vec<Arc<SqliteStore>> = (0..6).map(|_| Arc::new(SqliteStore::open(&p).unwrap())).collect();
    let a = attempt(&k1(), "race", Utc::now());
    let tasks: Vec<_> = stores
        .iter()
        .cycle()
        .take(24)
        .map(|s| {
            let s = s.clone();
            let a = a.clone();
            tokio::spawn(async move { s.attempt_put_if_absent(&a).await.unwrap() })
        })
        .collect();
    let mut wins = 0;
    for t in tasks {
        wins += t.await.unwrap() as usize;
    }
    assert_eq!(wins, 1);
    let r = rec(&k1(), "dup", 1, Utc::now());
    let tasks: Vec<_> = stores.iter().map(|s| {
        let s = s.clone();
        let r = r.clone();
        tokio::spawn(async move { s.put_run(&r).await.unwrap() })
    }).collect();
    let mut ins = 0;
    for t in tasks {
        ins += (t.await.unwrap() == rightsizer_history::PutOutcome::Inserted) as usize;
    }
    assert_eq!(ins, 1);
}

#[cfg(feature = "postgres")]
mod pg {
    use super::*;
    use rightsizer_history::postgres::{PostgresConfig, PostgresStore};

    async fn store(role: Role) -> Option<(PostgresStore, PostgresConfig)> {
        let url = std::env::var("RSZ_IT_POSTGRES_URL").ok()?;
        let mut cfg = PostgresConfig::from_url(&url).unwrap();
        cfg.schema = format!("t_{}", ulid::Ulid::generate().to_string().to_lowercase());
        cfg.role = Role::Operator;
        let op = PostgresStore::connect(cfg.clone()).await.unwrap();
        if role == Role::Operator {
            return Some((op, cfg));
        }
        cfg.role = Role::Client;
        Some((PostgresStore::connect(cfg.clone()).await.unwrap(), cfg))
    }

    #[tokio::test]
    async fn postgres_reads_n_and_n_minus_1_rows() {
        let Some((s, cfg)) = store(Role::Operator).await else { return };
        let k = k1();
        let c = s.client().await.unwrap();
        let ins = format!(
            "INSERT INTO {}.runs (key, tenant, invocation_id, attempt, source, started_at_ms, written_at_ms, v, body) VALUES ($1, 'k8s/c1/ns', $2, 1, 's', $3, 0, $4, $5)",
            cfg.schema
        );
        c.execute(&ins, &[&k.to_string(), &"old", &2i64, &1i32, &old_row(&k)]).await.unwrap();
        let mut fut = old_row(&k);
        fut["v"] = 7.into();
        c.execute(&ins, &[&k.to_string(), &"future", &1i64, &7i32, &fut]).await.unwrap();
        let v = s.runs(&k, 10).await.unwrap();
        assert_eq!(invs(&v), ["old#1"]);
    }

    #[tokio::test]
    async fn postgres_lease_takeover_and_migrations() {
        let Some((s, cfg)) = store(Role::Operator).await else { return };
        let now = Utc::now();
        let ttl = std::time::Duration::from_secs(3600);
        s.acquire_compaction_lease("A", now, ttl).await.unwrap();
        assert!(matches!(s.acquire_compaction_lease("B", now, ttl).await, Err(StoreError::Conflict(_))));
        // Expired: concurrent takers, one winner.
        let s = Arc::new(s);
        let later = now + Duration::hours(2);
        let tasks: Vec<_> = (0..8)
            .map(|i| {
                let s = s.clone();
                tokio::spawn(async move { s.acquire_compaction_lease(&format!("T{i}"), later, ttl).await.is_ok() })
            })
            .collect();
        let mut wins = 0;
        for t in tasks {
            wins += t.await.unwrap() as usize;
        }
        assert_eq!(wins, 1);
        // Re-connecting re-runs no migrations; a client on an unmigrated schema is refused.
        PostgresStore::connect(cfg.clone()).await.unwrap();
        let mut c = cfg.clone();
        c.role = Role::Client;
        c.schema = "t_never_migrated".into();
        assert!(PostgresStore::connect(c).await.is_err());
        s.put_run(&rec(&k1(), "a", 1, now)).await.unwrap();
        s.release_compaction_lease("T0").await.unwrap();
        for i in 0..8 {
            s.release_compaction_lease(&format!("T{i}")).await.unwrap();
        }
        s.compact(&CompactOptions::at(now + Duration::days(1))).await.unwrap();
        assert_eq!(s.stats().await.unwrap().shards, Some(1));
    }
}
