//! Refinement round: safety and trust gaps left open by the adversarial
//! review (outcome joins and idempotence, trust ceilings at ingestion,
//! cross-process freshness, URI salt, local input paths).

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::*;
use rightsizer_core::breaker::{BreakerScope, Reset};
use rightsizer_core::*;
use rightsizer_engine::{Config, ContextInput, Engine, OutcomeOptions};
use rightsizer_estimators::EstimatorChain;
use rightsizer_history::{HistoryStore, MemoryStore, Role};

/// An enforced, reduced decision and the OOM of the attempt it sized.
async fn reduced_then_oom(r: &Rig) -> (Decision, RunRecord) {
    let d = r.engine.decide(seed_req(prior_gib(8, 2), ladder_platform(), Mode::Enforce)).await;
    assert_eq!(d.mode, Mode::Enforce);
    let applied = d.applied.requests.memory.unwrap();
    assert!(applied < Bytes::gib(8), "{:?}", d.explanation);
    let mut oom = success("inv-1", t0(), applied.as_gib(), 1);
    oom.decision_id = Some(d.id);
    oom.requested = Some(d.applied.clone());
    oom.status = RunStatus::OomKilled;
    oom.memory_peak = Some(Measurement { value: applied.0 as f64, censor: Censor::AtLeast, resolution_s: None });
    (d, oom)
}

fn other_key() -> WorkloadKey {
    WorkloadKey::parse("k8s://prod/other-team/job/train").unwrap()
}

// ── item 1(a): a decision of another key is never joined ───────────────

#[tokio::test]
async fn outcome_does_not_join_a_decision_of_another_key() {
    let r = rig(1.0).await;
    let (d, mut oom) = reduced_then_oom(&r).await;
    // The same decision id, but a record of another tenant's key: with the
    // join, the OOM would borrow the decision's 8 GiB prior and trip the
    // other key's breaker (or credit saved hours to it).
    oom.key = other_key();
    let rep = r.engine.outcome(oom).await.unwrap();
    assert!(rep.written);
    assert!(!rep.joined && rep.join_refused, "{rep:?}");
    assert_eq!(rep.attribution.as_deref(), Some("not_caused"), "no prior known for the other key");
    assert!(rep.tripped.is_empty());
    assert!(!r.store.breaker_get(&BreakerScope::Workload { key: other_key() }).await.unwrap().is_open());
    // The right key still joins.
    let mut ok = success("inv-2", t0(), 1.0, 8);
    ok.decision_id = Some(d.id);
    let rep = r.engine.outcome(ok).await.unwrap();
    assert!(rep.joined && !rep.join_refused);
}

#[tokio::test]
async fn outcome_does_not_join_a_stored_decision_of_another_key() {
    // The decision is not in this engine's recent map: the store lookup path.
    let r = rig(1.0).await;
    let (_, mut oom) = reduced_then_oom(&r).await;
    let fresh = Engine::builder(Config::default()).store(r.store.clone()).chain(EstimatorChain::new(vec![r.fixed.clone()])).clock(r.clock.clone()).build().await.unwrap();
    oom.key = other_key();
    let rep = fresh.outcome(oom).await.unwrap();
    assert!(!rep.joined && rep.join_refused);
    assert!(rep.tripped.is_empty());
}

// ── item 1(b): duplicates change nothing ───────────────────────────────

#[tokio::test]
async fn duplicate_outcome_runs_nothing_again() {
    let r = rig(1.0).await;
    let (_, oom) = reduced_then_oom(&r).await;
    let scope = BreakerScope::Workload { key: key() };
    let rep = r.engine.outcome(oom.clone()).await.unwrap();
    assert_eq!(rep.tripped, vec![scope.clone()]);
    assert!(!rep.duplicate);
    // An operator resets the breaker; a retried / replayed report of the same
    // attempt must not trip it again.
    r.store.breaker_reset(&Reset { v: 1, scope: scope.clone(), at: t0() + chrono::Duration::seconds(1), by: Some("op".into()) }).await.unwrap();
    r.engine.view().invalidate_breakers();
    let rep = r.engine.outcome(oom.clone()).await.unwrap();
    assert!(rep.duplicate && !rep.written, "{rep:?}");
    assert!(rep.tripped.is_empty() && rep.attribution.is_none() && !rep.joined);
    assert!(!r.store.breaker_get(&scope).await.unwrap().is_open(), "not re-tripped");
    // Nor does a duplicate invalidate the memo: the next decision is a memo hit.
    let a = r.engine.decide(seed_req(prior_gib(8, 2), platform(), Mode::Recommend)).await;
    let b0 = r.fixed.calls.load(std::sync::atomic::Ordering::SeqCst);
    let rep = r.engine.outcome(oom).await.unwrap();
    assert!(rep.duplicate);
    let b = r.engine.decide(seed_req(prior_gib(8, 2), platform(), Mode::Recommend)).await;
    assert!(matches!(b.source, DecisionSource::Memo { .. }), "{:?} after {:?}", b.source, a.source);
    assert_eq!(r.fixed.calls.load(std::sync::atomic::Ordering::SeqCst), b0);
}

#[tokio::test]
async fn duplicate_success_is_not_counted_twice() {
    // An enforced, reduced success: saved resource-hours are counted once,
    // however often the outcome is replayed (and a replay closes no lease and
    // feeds no breaker window).
    let r = rig(1.0).await;
    let d = r.engine.decide(seed_req(prior_gib(8, 2), ladder_platform(), Mode::Enforce)).await;
    assert_eq!(d.mode, Mode::Enforce);
    let mut ok = success("inv-ok", t0(), 1.0, 1);
    ok.decision_id = Some(d.id);
    ok.requested = Some(d.applied.clone());
    let saved = || r.engine.metrics().saved_hours.with_label_values(&["memory"]).get();
    let before = saved();
    let rep = r.engine.outcome(ok.clone()).await.unwrap();
    assert!(rep.joined && !rep.duplicate);
    let once = saved();
    assert!(once > before, "an enforced reduced success saves memory-hours");
    for _ in 0..5 {
        let rep = r.engine.outcome(ok.clone()).await.unwrap();
        assert!(rep.duplicate && !rep.joined && !rep.lease_closed && rep.tripped.is_empty());
    }
    assert_eq!(saved(), once, "replays are not counted again");
}

/// Every surface reaches `outcome` the same way: the plugin daemon, the
/// watcher, the Flyte backfill and the Python API call `outcome` (a repeat
/// is a duplicate); the Slurm importer inserts first, then calls
/// `outcome_with(prewritten)` for the records it inserted only.
#[tokio::test]
async fn importer_path_processes_inserted_records_once() {
    let r = rig(1.0).await;
    let (_, oom) = reduced_then_oom(&r).await;
    // Import round 1: the importer writes, then reports.
    assert_eq!(r.store.put_run(&oom).await.unwrap(), rightsizer_history::PutOutcome::Inserted);
    let rep = r.engine.outcome_with(oom.clone(), OutcomeOptions { prewritten: true }).await.unwrap();
    assert!(rep.written && !rep.duplicate);
    assert_eq!(rep.attribution.as_deref(), Some("caused"));
    assert_eq!(rep.tripped, vec![BreakerScope::Workload { key: key() }]);
    // Import round 2 sees a duplicate and does not report it; a plain
    // `outcome` of it (any other surface) is a duplicate too.
    assert_eq!(r.store.put_run(&oom).await.unwrap(), rightsizer_history::PutOutcome::Duplicate);
    assert!(r.engine.outcome(oom).await.unwrap().duplicate);
}

// ── item 2: trust ceilings at ingestion ────────────────────────────────

#[tokio::test]
async fn client_engines_store_at_most_authenticated() {
    let r = rig(1.0).await;
    assert_eq!(r.engine.max_ingest_trust(), Trust::Authenticated);
    let mut rec = success("inv-v", t0(), 1.0, 8);
    rec.trust = Trust::Verified; // claimed by a client with shared credentials
    r.engine.outcome(rec).await.unwrap();
    let stored = r.store.runs(&key(), 10).await.unwrap();
    assert_eq!(stored[0].trust, Trust::Authenticated, "capped at the client ceiling");
}

#[tokio::test]
async fn importer_engines_keep_verified_but_not_beyond_the_source() {
    let store = Arc::new(MemoryStore::new());
    let fixed = Arc::new(Fixed::new(1.0, 1.0));
    let e = Engine::builder(Config::default()).store(store.clone()).chain(EstimatorChain::new(vec![fixed])).max_ingest_trust(Trust::Verified).build().await.unwrap();
    let mut watcher = success("w", t0(), 1.0, 8);
    watcher.trust = Trust::Verified;
    e.outcome(watcher).await.unwrap();
    // A self-report can never be stored as verified, whatever the ceiling.
    let mut sr = success("s", t0() + chrono::Duration::seconds(1), 1.0, 8);
    sr.source = OutcomeSource::SelfReport;
    sr.trust = Trust::Verified;
    e.outcome(sr).await.unwrap();
    let v = store.runs(&key(), 10).await.unwrap();
    let by = |inv: &str| v.iter().find(|r| r.invocation_id == inv).unwrap().trust;
    assert_eq!(by("w"), Trust::Verified);
    assert_eq!(by("s"), Trust::Authenticated);
    // Operator engines default to verified; config overrides the role default.
    let op = Engine::builder(Config::default()).store(Arc::new(MemoryStore::new())).role(Role::Operator).build().await.unwrap();
    assert_eq!(op.max_ingest_trust(), Trust::Verified);
    let mut cfg = Config::default();
    cfg.history.max_ingest_trust = Some(Trust::Unverified);
    let low = Engine::builder(cfg).store(Arc::new(MemoryStore::new())).build().await.unwrap();
    assert_eq!(low.max_ingest_trust(), Trust::Unverified);
}

// ── item 3: cross-process freshness ────────────────────────────────────

/// Two processes share a store: B's outcome is visible to A after
/// `history.revalidate_s`, not the 10-minute TTL; `invalidate` makes it
/// visible at once. A hit with no revalidation due stays a memo hit.
#[tokio::test]
async fn another_processs_runs_are_seen_after_revalidation() {
    let store = Arc::new(MemoryStore::new());
    for i in 0..3 {
        store.put_run(&success(&format!("old-{i}"), t0() - chrono::Duration::hours(i + 1), 2.0, 8)).await.unwrap();
    }
    let clock = Arc::new(rightsizer_engine::ManualClock::new(t0()));
    let mk = |fixed: Arc<Fixed>| {
        Engine::builder(Config::default()).store(store.clone()).chain(EstimatorChain::new(vec![fixed])).clock(clock.clone()).build()
    };
    let fa = Arc::new(Fixed::new(1.0, 1.0));
    let a = mk(fa.clone()).await.unwrap();
    let b = mk(Arc::new(Fixed::new(1.0, 1.0))).await.unwrap();
    let req = || seed_req(prior_gib(8, 2), platform(), Mode::Recommend);
    let d1 = a.decide(req()).await;
    assert!(matches!(d1.source, DecisionSource::Estimator { .. }));
    assert!(matches!(a.decide(req()).await.source, DecisionSource::Memo { .. }));
    // Process B records a run.
    b.outcome(success("new", t0(), 6.0, 8)).await.unwrap();
    // Within the revalidation interval A still serves its memo (no store check).
    clock.advance(Duration::from_secs(10));
    assert!(matches!(a.decide(req()).await.source, DecisionSource::Memo { .. }));
    // Past it, A checks the store, sees B's run, and recomputes.
    clock.advance(Duration::from_secs(25));
    let calls = fa.calls.load(std::sync::atomic::Ordering::SeqCst);
    let d = a.decide(req()).await;
    assert!(matches!(d.source, DecisionSource::Estimator { .. }), "{:?}", d.source);
    assert_eq!(fa.calls.load(std::sync::atomic::Ordering::SeqCst), calls + 1);
    // Explicit invalidation: visible at once.
    b.outcome(success("newer", t0() + chrono::Duration::seconds(5), 6.5, 8)).await.unwrap();
    assert!(matches!(a.decide(req()).await.source, DecisionSource::Memo { .. }));
    a.invalidate(&key());
    let d = a.decide(req()).await;
    assert!(matches!(d.source, DecisionSource::Estimator { .. }), "{:?}", d.source);
}

#[tokio::test]
async fn revalidation_can_be_turned_off() {
    let store = Arc::new(MemoryStore::new());
    let clock = Arc::new(rightsizer_engine::ManualClock::new(t0()));
    let mut cfg = Config::default();
    cfg.history.revalidate_s = 0;
    let fixed = Arc::new(Fixed::new(1.0, 1.0));
    let a = Engine::builder(cfg).store(store.clone()).chain(EstimatorChain::new(vec![fixed])).clock(clock.clone()).build().await.unwrap();
    let req = || seed_req(prior_gib(8, 2), platform(), Mode::Recommend);
    a.decide(req()).await;
    store.put_run(&success("x", t0(), 6.0, 8)).await.unwrap();
    clock.advance(Duration::from_secs(120));
    assert!(matches!(a.decide(req()).await.source, DecisionSource::Memo { .. }), "TTL only");
}

// ── item 5: one URI salt ───────────────────────────────────────────────

#[tokio::test]
async fn empty_salt_is_refused() {
    // A test-private variable (env changes are process-wide).
    let var = "RSZ_TEST_REFINEMENT_EMPTY_SALT";
    // SAFETY: only this test reads this variable.
    unsafe { std::env::set_var(var, "") };
    let mut cfg = Config::default();
    cfg.privacy.salt_env = Some(var.into());
    let (salt, warn) = rightsizer_engine::chain::resolve_salt(&cfg.privacy);
    assert!(warn.unwrap().contains(var));
    assert_eq!(salt, std::env::var("RIGHTSIZER_URI_SALT").ok().filter(|s| !s.is_empty()), "falls back to the next source");
    let mut strict = cfg.clone();
    strict.strict = true;
    assert!(Engine::builder(strict).store(Arc::new(MemoryStore::new())).build().await.is_err(), "strict: configuration error");
    let e = Engine::builder(cfg.clone()).store(Arc::new(MemoryStore::new())).build().await.unwrap();
    assert!(e.config_error().is_none(), "non-strict: a warning, not inert");
    // A set salt is the engine's single source.
    unsafe { std::env::set_var(var, "pepper") };
    let e = Engine::builder(cfg).store(Arc::new(MemoryStore::new())).build().await.unwrap();
    assert_eq!(e.read_options().salt.as_deref(), Some("pepper"));
    unsafe { std::env::remove_var(var) };
}

#[tokio::test]
async fn contexts_hash_uris_with_the_engines_salt() {
    let var = "RSZ_TEST_REFINEMENT_SALT";
    // SAFETY: only this test reads this variable.
    unsafe { std::env::set_var(var, "s-engine") };
    let mut cfg = Config::default();
    cfg.privacy.salt_env = Some(var.into());
    let fixed = Arc::new(Fixed::new(1.0, 1.0));
    let e = Engine::builder(cfg).store(Arc::new(MemoryStore::new())).chain(EstimatorChain::new(vec![fixed])).build().await.unwrap();
    unsafe { std::env::remove_var(var) };
    // What the engine's readers (and the K8s prefetcher, which reads with
    // `Engine::read_options`) hash with.
    let refs = vec![InputRef { name: "d".into(), uri: Some("s3://b/x.parquet".into()), bytes: Some(10), format: None, scalar: None }];
    let mut o = e.read_options().clone();
    o.hash_only = true;
    let got = rightsizer_features::read_inputs_with(&refs, e.resolver().as_ref(), &o).await;
    assert_eq!(got[0].uri_hash.as_deref(), Some(rightsizer_features::uri_hash("s3://b/x.parquet", "s-engine").as_str()));
    // And a decision's context: the seed path hashes with the same salt.
    let mut req = seed_req(prior_gib(8, 2), platform(), Mode::Recommend);
    if let ContextInput::Seed { seed, .. } = &mut req.context {
        seed.inputs = refs;
    }
    assert!(!e.decide(req).await.id.is_nil());
}

// ── item 8: local input paths ──────────────────────────────────────────

async fn bytes_seen(e: &Engine, path: &std::path::Path) -> Option<u64> {
    let refs = vec![InputRef { name: "d".into(), uri: Some(path.display().to_string()), bytes: None, format: None, scalar: None }];
    let mut o = e.read_options().clone();
    o.use_cache = false;
    rightsizer_features::read_inputs_with(&refs, e.resolver().as_ref(), &o).await[0].bytes
}

#[tokio::test]
async fn servers_read_no_local_paths_unless_allowlisted() {
    let d = tempfile::tempdir().unwrap();
    let data = d.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    let f = data.join("x.bin");
    std::fs::write(&f, vec![0u8; 1234]).unwrap();
    let secret = d.path().join("secret.pem");
    std::fs::write(&secret, b"key").unwrap();
    let build = |cfg: Config, server: bool| Engine::builder(cfg).store(Arc::new(MemoryStore::new())).server(server).build();
    // In-process (CLI, Python): allowed by default.
    let cli = build(Config::default(), false).await.unwrap();
    assert_eq!(bytes_seen(&cli, &f).await, Some(1234));
    // Server (webhook, daemon): denied by default, bare path and file:// alike.
    let srv = build(Config::default(), true).await.unwrap();
    assert_eq!(bytes_seen(&srv, &f).await, None);
    let refs = vec![InputRef { name: "d".into(), uri: Some(url_of(&f)), bytes: None, format: None, scalar: None }];
    assert_eq!(rightsizer_features::read_inputs_with(&refs, srv.resolver().as_ref(), srv.read_options()).await[0].bytes, None);
    // Allowlisted directories only.
    let mut cfg = Config::default();
    cfg.features.local_paths = Some(vec![data.clone()]);
    let srv = build(cfg, true).await.unwrap();
    assert_eq!(bytes_seen(&srv, &f).await, Some(1234));
    assert_eq!(bytes_seen(&srv, &secret).await, None);
    assert_eq!(bytes_seen(&srv, &data.join("../secret.pem")).await, None);
    // An explicit empty list denies in-process use too.
    let mut cfg = Config::default();
    cfg.features.local_paths = Some(vec![]);
    assert_eq!(bytes_seen(&build(cfg, false).await.unwrap(), &f).await, None);
}

fn url_of(p: &std::path::Path) -> String {
    format!("file://{}", p.display())
}

// ── item 6: S3(b) vetoes, end to end ───────────────────────────────────

#[tokio::test]
async fn s3b_veto_holds_a_reduction_a_newer_run_disproves() {
    let r = rig(1.0).await;
    // No ladder: only S3(b) can permit a reduction.
    for i in 0..3 {
        r.store.put_run(&success(&format!("old-{i}"), t0() - chrono::Duration::hours(10 + i), 1.0, 8)).await.unwrap();
    }
    let d = r.engine.decide(seed_req(prior_gib(8, 2), platform(), Mode::Enforce)).await;
    let reduced = d.applied.requests.memory.unwrap();
    assert!(reduced <= Bytes::gib(4), "three fitting runs permit a reduction (to the trust region): {:?}", d.explanation);
    // A newer same-key success peaked at 6 GiB: nothing below 6 × 1.05 may be enforced.
    r.store.put_run(&success("new", t0() - chrono::Duration::hours(1), 6.0, 8)).await.unwrap();
    r.engine.invalidate(&key());
    let d = r.engine.decide(seed_req(prior_gib(8, 2), platform(), Mode::Enforce)).await;
    let m = d.applied.requests.memory.unwrap();
    assert!(m.0 as f64 >= 6.0 * 1.05 * Bytes::GIB as f64, "{m:?}: {:?}", d.explanation);
    // A newer OOM at 5 GiB (no peak) vetoes too.
    let r = rig(1.0).await;
    for i in 0..3 {
        r.store.put_run(&success(&format!("old-{i}"), t0() - chrono::Duration::hours(10 + i), 1.0, 8)).await.unwrap();
    }
    let mut oom = success("oom", t0() - chrono::Duration::hours(1), 1.0, 5);
    oom.status = RunStatus::OomKilled;
    oom.memory_peak = None;
    r.store.put_run(&oom).await.unwrap();
    let d = r.engine.decide(seed_req(prior_gib(8, 2), platform(), Mode::Enforce)).await;
    assert!(d.applied.requests.memory.unwrap() >= Bytes::gib(5), "{:?}", d.explanation);
}
