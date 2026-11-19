//! Remote client tests against an in-process mock server (axum, dev only).

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::Mutex as StdMutex;

use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use rightsizer_core::*;
use serde_json::{Value, json};

use super::*;
use crate::test_util::*;

/// A scripted response: status and JSON body.
type Scripted = (u16, Value);
/// A scripted error and a check on what it maps to.
type ErrorCase = (Scripted, fn(&EstimateError) -> bool);

#[derive(Default)]
struct Mock {
    /// `(path, body, authorization header)` of every request.
    calls: StdMutex<Vec<(String, Value, Option<String>)>>,
    /// Responses for estimate calls, consumed in order; empty → success.
    script: StdMutex<VecDeque<Scripted>>,
    /// Delay before answering estimate calls.
    delay_ms: StdMutex<u64>,
    max_batch: u32,
}

impl Mock {
    fn estimate_calls(&self) -> Vec<(String, Value, Option<String>)> {
        self.calls.lock().unwrap().iter().filter(|c| c.0.starts_with("/v1/estimate")).cloned().collect()
    }
}

/// The mock's answer for one context: a point at the first input's size
/// (so tests can match results to requests).
fn answer(ctx: &Value) -> Value {
    let bytes = ctx["inputs"][0]["bytes"].as_f64().unwrap_or(1e9);
    json!({"v": 1, "kind": "demand", "memory": {"kind": "point", "value": bytes},
           "provenance": {"estimator": "mock", "version": "3", "grid": "rs-grid-v1"}})
}

async fn handler(State(m): State<Arc<Mock>>, method: Method, uri: Uri, headers: HeaderMap, body: Bytes) -> Response {
    let path = uri.path().to_string();
    let body: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    let auth = headers.get("authorization").and_then(|v| v.to_str().ok()).map(str::to_string);
    m.calls.lock().unwrap().push((path.clone(), body.clone(), auth));
    match (method, path.as_str()) {
        (Method::GET, "/v1/info") => axum::Json(json!({
            "protocol": "estimator/1", "id": "mock", "version": "3", "grids": ["rs-grid-v1"],
            "axes": ["memory"], "kind": "demand", "needs": {"code": true, "history": true, "input_bytes": true},
            "features": "features-v1", "novelty": null, "max_batch": m.max_batch
        }))
        .into_response(),
        (Method::GET, "/v1/health") => StatusCode::OK.into_response(),
        (Method::POST, "/v1/estimate") | (Method::POST, "/v1/estimate:batch") => {
            let d = *m.delay_ms.lock().unwrap();
            if d > 0 {
                tokio::time::sleep(Duration::from_millis(d)).await;
            }
            if let Some((status, b)) = m.script.lock().unwrap().pop_front() {
                return (StatusCode::from_u16(status).unwrap(), axum::Json(b)).into_response();
            }
            if path.ends_with(":batch") {
                let results: Vec<Value> = body["contexts"].as_array().unwrap().iter().map(|c| json!({"estimate": answer(c)})).collect();
                axum::Json(json!({ "results": results })).into_response()
            } else {
                axum::Json(json!({"estimate": answer(&body["context"])})).into_response()
            }
        }
        _ => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn serve(max_batch: u32) -> (Arc<Mock>, SocketAddr) {
    let mock = Arc::new(Mock { max_batch, ..Default::default() });
    let app = Router::new().fallback(handler).with_state(mock.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (mock, addr)
}

fn cfg(addr: SocketAddr) -> RemoteConfig {
    let mut c = RemoteConfig::new(format!("http://{addr}/v1"));
    c.retry_jitter = (Duration::from_millis(1), Duration::from_millis(5));
    c
}

fn ctx(bytes: u64) -> WorkloadContext {
    ctx_with(vec![InputMeta::blob("x", bytes)], vec![])
}

fn err(status: u16, code: &str, retryable: bool) -> Scripted {
    (status, json!({"error": {"code": code, "message": "m", "retryable": retryable}}))
}

#[tokio::test]
async fn info_estimate_health_and_cache() {
    let (mock, addr) = serve(1).await;
    let r = RemoteEstimator::connect(cfg(addr)).await.unwrap();
    assert_eq!(r.info().id, "mock");
    assert_eq!(r.info().version, "3");
    assert!(r.health().await);
    let e = r.estimate(&ctx(5_000_000_000)).await.unwrap();
    assert_eq!(e.provenance.estimator, "mock");
    assert_eq!(e.memory, Some(AxisEstimate::Point { value: 5e9 }));
    // Exact cache: the same context again makes no call.
    r.estimate(&ctx(5_000_000_000)).await.unwrap();
    assert_eq!(mock.estimate_calls().len(), 1);
    // History changes the watermark, so the key.
    let mut c = ctx(5_000_000_000);
    c.history.push(record("h", RunStatus::Succeeded, None, None));
    r.estimate(&c).await.unwrap();
    assert_eq!(mock.estimate_calls().len(), 2);
    // Request body shape: {context, axes}.
    let (path, body, _) = &mock.estimate_calls()[0];
    assert_eq!(path, "/v1/estimate");
    assert_eq!(body["axes"], json!(["memory"]));
    assert!(body["context"]["key"].is_string());
}

#[tokio::test]
async fn unreachable_info_gives_a_placeholder() {
    let mut c = RemoteConfig::new("http://127.0.0.1:9/v1");
    c.name = "far".into();
    c.timeout = Duration::from_millis(300);
    let r = RemoteEstimator::connect(c).await.unwrap();
    assert_eq!((r.info().id.as_str(), r.info().version.as_str()), ("far", "unknown"));
    let e = r.estimate(&ctx(1)).await.unwrap_err();
    assert!(matches!(e, EstimateError::Failed { .. } | EstimateError::Timeout(_)), "{e:?}");
    assert!(!r.health().await);
}

#[tokio::test]
async fn error_codes_map() {
    let (mock, addr) = serve(1).await;
    // Keep the breaker out of the way: this test makes many failing calls.
    let mut c = cfg(addr);
    c.breaker_threshold = 100;
    let r = RemoteEstimator::connect(c).await.unwrap();
    let cases: Vec<ErrorCase> = vec![
        (err(400, "malformed", false), |e| matches!(e, EstimateError::Protocol(_))),
        (err(409, "unsupported", false), |e| matches!(e, EstimateError::Unsupported(_))),
        (err(422, "insufficient", false), |e| e.is_insufficient()),
        (err(500, "internal", false), |e| matches!(e, EstimateError::Failed { retryable: false, .. })),
        (err(404, "nope", false), |e| matches!(e, EstimateError::Failed { .. })),
    ];
    for (i, (resp, check)) in cases.into_iter().enumerate() {
        mock.script.lock().unwrap().push_back(resp.clone());
        let e = r.estimate(&ctx(100 + i as u64)).await.unwrap_err();
        assert!(check(&e), "{resp:?} → {e:?}");
    }
    assert_eq!(mock.estimate_calls().len(), 5, "no retries for non-retryable errors");
    // 429 and 503 back off: one jittered retry, then the error.
    for (j, status) in [429u16, 503].into_iter().enumerate() {
        let before = mock.estimate_calls().len();
        mock.script.lock().unwrap().extend([err(status, "busy", true), err(status, "busy", true)]);
        let e = r.estimate(&ctx(200 + j as u64)).await.unwrap_err();
        assert!(matches!(e, EstimateError::Unavailable(_)), "{e:?}");
        assert_eq!(mock.estimate_calls().len() - before, 2, "status {status}");
    }
    // A body without the envelope still maps by status.
    mock.script.lock().unwrap().push_back((422, json!("not an envelope")));
    assert!(r.estimate(&ctx(300)).await.unwrap_err().is_insufficient());
}

#[tokio::test]
async fn one_jittered_retry_on_retryable() {
    let (mock, addr) = serve(1).await;
    let r = RemoteEstimator::connect(cfg(addr)).await.unwrap();
    mock.script.lock().unwrap().push_back(err(503, "warming", true));
    let e = r.estimate(&ctx(7)).await.unwrap();
    assert_eq!(e.provenance.estimator, "mock");
    assert_eq!(mock.estimate_calls().len(), 2);
    // A 5xx whose body says retryable is retried too; a 503 that says not, is not.
    mock.script.lock().unwrap().push_back(err(500, "flaky", true));
    r.estimate(&ctx(8)).await.unwrap();
    assert_eq!(mock.estimate_calls().len(), 4);
    mock.script.lock().unwrap().push_back(err(503, "down", false));
    assert!(r.estimate(&ctx(9)).await.is_err());
    assert_eq!(mock.estimate_calls().len(), 5);
}

#[tokio::test]
async fn breaker_opens_and_half_opens() {
    let (mock, addr) = serve(1).await;
    let mut c = cfg(addr);
    c.breaker_cooldown = Duration::from_millis(300);
    let r = RemoteEstimator::connect(c).await.unwrap();
    for i in 0..5 {
        mock.script.lock().unwrap().push_back(err(500, "boom", false));
        assert!(r.estimate(&ctx(i)).await.is_err());
    }
    assert_eq!(mock.estimate_calls().len(), 5);
    assert!(matches!(r.breaker_state(), BreakerState::Open { .. }));
    // Open: fails fast without a call.
    let e = r.estimate(&ctx(100)).await.unwrap_err();
    assert!(matches!(e, EstimateError::Unavailable(ref m) if m.contains("circuit open")), "{e:?}");
    assert_eq!(mock.estimate_calls().len(), 5);
    // Half-open after the cooldown: a failing trial reopens it…
    tokio::time::sleep(Duration::from_millis(350)).await;
    mock.script.lock().unwrap().push_back(err(500, "boom", false));
    assert!(r.estimate(&ctx(101)).await.is_err());
    assert_eq!(mock.estimate_calls().len(), 6);
    assert!(matches!(r.breaker_state(), BreakerState::Open { .. }));
    // …a successful one closes it.
    tokio::time::sleep(Duration::from_millis(350)).await;
    r.estimate(&ctx(102)).await.unwrap();
    assert_eq!(r.breaker_state(), BreakerState::Closed { failures: 0 });
    // 422s are answers, not failures.
    for i in 0..6 {
        mock.script.lock().unwrap().push_back(err(422, "insufficient", false));
        assert!(r.estimate(&ctx(200 + i)).await.unwrap_err().is_insufficient());
    }
    assert_eq!(r.breaker_state(), BreakerState::Closed { failures: 0 });
}

#[test]
fn breaker_state_machine() {
    let b = CircuitBreaker::new(2, Duration::from_secs(30));
    let t0 = Instant::now();
    assert!(b.admit(t0));
    b.record(Some(false), t0);
    assert_eq!(b.state(), BreakerState::Closed { failures: 1 });
    b.record(Some(false), t0);
    assert!(matches!(b.state(), BreakerState::Open { .. }));
    assert!(!b.admit(t0 + Duration::from_secs(29)));
    assert!(b.admit(t0 + Duration::from_secs(30)), "half-open trial");
    assert!(!b.admit(t0 + Duration::from_secs(30)), "one trial at a time");
    b.record(None, t0 + Duration::from_secs(31));
    assert!(b.admit(t0 + Duration::from_secs(31)), "a deliberate answer frees the trial");
    b.record(Some(true), t0 + Duration::from_secs(31));
    assert_eq!(b.state(), BreakerState::Closed { failures: 0 });
}

#[tokio::test]
async fn single_flight_identical_requests() {
    let (mock, addr) = serve(1).await;
    *mock.delay_ms.lock().unwrap() = 150;
    let r = RemoteEstimator::connect(cfg(addr)).await.unwrap();
    let c = ctx(42);
    let all = futures::future::join_all((0..10).map(|_| r.estimate(&c))).await;
    assert!(all.iter().all(|x| x.is_ok()));
    assert_eq!(mock.estimate_calls().len(), 1);
}

#[tokio::test]
async fn coalesces_distinct_requests_into_batches() {
    let (mock, addr) = serve(16).await;
    let r = RemoteEstimator::connect(cfg(addr)).await.unwrap();
    assert_eq!(r.max_batch(), 16);
    let ctxs: Vec<_> = (1..=8u64).map(|i| ctx(i * 1_000_000)).collect();
    let all = futures::future::join_all(ctxs.iter().map(|c| r.estimate(c))).await;
    for (i, e) in all.into_iter().enumerate() {
        assert_eq!(e.unwrap().memory, Some(AxisEstimate::Point { value: ((i as u64 + 1) * 1_000_000) as f64 }), "results map back");
    }
    let calls = mock.estimate_calls();
    assert_eq!(calls.len(), 1, "{:?}", calls.iter().map(|c| &c.0).collect::<Vec<_>>());
    assert_eq!(calls[0].0, "/v1/estimate:batch");
    assert_eq!(calls[0].1["contexts"].as_array().unwrap().len(), 8);
    // Capped by config: 8 contexts at max 3 → 3 calls.
    let mut c = cfg(addr);
    c.max_batch = Some(3);
    let r = RemoteEstimator::connect(c).await.unwrap();
    let ctxs: Vec<_> = (1..=8u64).map(|i| ctx(i * 7_000_000)).collect();
    let before = mock.estimate_calls().len();
    let all = futures::future::join_all(ctxs.iter().map(|c| r.estimate(c))).await;
    assert!(all.iter().all(|x| x.is_ok()));
    let sizes: Vec<usize> =
        mock.estimate_calls()[before..].iter().map(|c| c.1["contexts"].as_array().map(|a| a.len()).unwrap_or(1)).collect();
    assert_eq!(sizes.iter().sum::<usize>(), 8);
    assert!(sizes.iter().all(|&s| s <= 3) && sizes.len() == 3, "{sizes:?}");
}

#[tokio::test]
async fn batch_item_errors_map_per_item() {
    let (mock, addr) = serve(4).await;
    let r = RemoteEstimator::connect(cfg(addr)).await.unwrap();
    mock.script.lock().unwrap().push_back((
        200,
        json!({"results": [
            {"estimate": answer(&json!({"inputs": [{"bytes": 1.0}]}))},
            {"error": {"code": "insufficient", "message": "no inputs", "retryable": false}}
        ]}),
    ));
    let (a, b) = (ctx(1), ctx(2));
    let (ra, rb) = tokio::join!(r.estimate(&a), r.estimate(&b));
    // Which context got which item depends on arrival order; one of each.
    let oks = [&ra, &rb].iter().filter(|x| x.is_ok()).count();
    let ins = [&ra, &rb].iter().filter(|x| x.as_ref().err().is_some_and(|e| e.is_insufficient())).count();
    assert_eq!((oks, ins), (1, 1), "{ra:?} {rb:?}");
}

#[tokio::test]
async fn deadline_caller_budget_wins() {
    let (mock, addr) = serve(1).await;
    *mock.delay_ms.lock().unwrap() = 2_000;
    let r = RemoteEstimator::connect(cfg(addr)).await.unwrap();
    let t = std::time::Instant::now();
    let e = r.estimate_within(&ctx(1), Some(Duration::from_millis(150))).await.unwrap_err();
    assert!(matches!(e, EstimateError::Timeout(_)), "{e:?}");
    assert!(t.elapsed() < Duration::from_millis(1_500), "{:?}", t.elapsed());
    // The default deadline from config applies without a budget.
    let mut c = cfg(addr);
    c.timeout = Duration::from_millis(200);
    let r = RemoteEstimator::connect(c).await.unwrap();
    let t = std::time::Instant::now();
    assert!(matches!(r.estimate(&ctx(2)).await, Err(EstimateError::Timeout(_))));
    assert!(t.elapsed() < Duration::from_millis(1_500));
}

#[tokio::test]
async fn bearer_token_and_privacy() {
    let (mock, addr) = serve(1).await;
    let dir = std::env::temp_dir().join(format!("rsz-remote-token-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let tok = dir.join("token");
    std::fs::write(&tok, "s3cret\n").unwrap();
    let mut c = cfg(addr);
    c.token = TokenSource::File(tok.clone());
    let r = RemoteEstimator::connect(c).await.unwrap();
    let mut x = ctx(1);
    x.code = Some(CodeInfo { digest: "d".into(), tokens: vec!["groupby".into()], source: Some("print('secret')".into()) });
    x.labels.insert("owner".into(), "ann".into());
    r.estimate(&x).await.unwrap();
    let (_, body, auth) = mock.estimate_calls().pop().unwrap();
    assert_eq!(auth.as_deref(), Some("Bearer s3cret"));
    assert!(body["context"]["code"].get("source").is_none(), "source never sent by default");
    assert_eq!(body["context"]["code"]["tokens"], json!(["groupby"]));
    assert_eq!(body["context"]["labels"], json!({}));
    // Env var token; a missing variable fails without a call.
    let var = format!("RSZ_TEST_TOKEN_{}", std::process::id());
    let mut c = cfg(addr);
    c.token = TokenSource::Env(var.clone());
    let r = RemoteEstimator::connect(c).await.unwrap();
    let before = mock.estimate_calls().len();
    assert!(matches!(r.estimate(&ctx(2)).await, Err(EstimateError::Failed { retryable: false, .. })));
    assert_eq!(mock.estimate_calls().len(), before);
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn memo_layer_shares_normalized_estimates() {
    let (mock, addr) = serve(1).await;
    let memo = Arc::new(Memo::new(rightsizer_core::Grid::rs_grid_v1(), Memo::DEFAULT_TTL, 100));
    let mut c = cfg(addr);
    c.memo = Some(memo.clone());
    c.policy_hash = "p1".into();
    let r = RemoteEstimator::connect(c).await.unwrap();
    r.estimate(&ctx(1_000_000_000)).await.unwrap();
    assert_eq!(memo.len(), 1);
    // Same bucket, different exact key (history differs): served from the memo, normalized.
    let mut x = ctx(1_010_000_000);
    x.history.push(record("h", RunStatus::Succeeded, None, None));
    let e = r.estimate(&x).await.unwrap();
    assert!(matches!(e.memory, Some(AxisEstimate::Cells { .. })));
    assert_eq!(mock.estimate_calls().len(), 1);
}

#[tokio::test]
async fn from_member_and_chain_build() {
    let (_mock, addr) = serve(8).await;
    let spec: crate::ChainSpec = serde_json::from_value(json!({
        "chain": ["hosted", "prior"],
        "members": {"hosted": {"url": format!("http://{addr}"), "timeout_ms": 500, "max_batch": 2}}
    }))
    .unwrap();
    let b = crate::build_chain(&spec, &RemotePrivacy::default()).await.unwrap();
    assert_eq!(b.chain.ids(), vec!["mock", "prior"]);
    let a = b.chain.estimate(&ctx(3), &[]).await;
    assert_eq!(a.answered_by.unwrap().1, "mock");
    assert!(
        RemoteConfig::from_member(
            "x",
            Some(&MemberSpec { url: Some("http://h".into()), client_cert: Some("c".into()), ..Default::default() }),
            RemotePrivacy::default()
        )
        .is_err()
    );
    assert_eq!(endpoint("http://h/v1/", "info"), "http://h/v1/info");
    assert_eq!(endpoint("http://h", "estimate:batch"), "http://h/v1/estimate:batch");
}
