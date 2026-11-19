//! Context assembly, deterministic truncation and the remote view.

mod common;

use std::collections::BTreeMap;

use common::*;
use proptest::prelude::*;
use rightsizer_core::digest::context_digest;
use rightsizer_core::{
    CodeRef, ColumnsInfo, ContextSeed, InputFormat, InputMeta, InputRef, PlatformRef, Prior, RunRecord, ScalarValue, UnitId,
    WorkloadContext, WorkloadKey,
};
use rightsizer_features::{Privacy, ReadBudget, build_context, redact_for_remote};

fn key() -> WorkloadKey {
    WorkloadKey::parse("k8s://c/ns/job/train").unwrap()
}

fn record(key: &str, id: &str, at: &str) -> RunRecord {
    serde_json::from_value(serde_json::json!({
        "key": key, "invocation_id": id, "attempt": 1, "started_at": at,
        "status": "succeeded", "source": {"kind": "cgroup"}, "trust": "unverified",
        "inputs": [{"name": "x", "format": "parquet", "bytes": 10, "columns": {"total": 2, "names": ["a", "b"]}}]
    }))
    .unwrap()
}

fn uri(name: &str) -> InputRef {
    InputRef { name: name.into(), uri: Some(format!("mem://{name}")), bytes: None, format: None, scalar: None }
}

#[tokio::test]
async fn builds_a_full_context() {
    let store = mem_store(&[("a.parquet", parquet_bytes(42, 0)), ("w.npy", npy_bytes("<f4", &[8, 8], &[0; 256]))]).await;
    let r = OneStore::new(store);
    let src = "import torch\n# torchrun --nproc_per_node=4 train.py\nmodel = AutoModel.from_pretrained(p)\nbatch_size = 16\n";
    let mut seed_labels = BTreeMap::new();
    seed_labels.insert("team".to_string(), "ml".to_string());
    seed_labels.insert("owner".to_string(), "ann".to_string());
    let seed = ContextSeed {
        inputs: vec![
            uri("a.parquet"),
            uri("w.npy"),
            InputRef { name: "lr".into(), uri: None, bytes: None, format: None, scalar: Some(ScalarValue::Float(0.001)) },
        ],
        code: Some(CodeRef::Source { text: src.into(), language: Some("python".into()) }),
        manifest: Default::default(),
        labels: seed_labels,
    };
    let mut extra = BTreeMap::new();
    extra.insert("team".to_string(), "infra".to_string());
    let history = vec![
        record("k8s://c/ns/job/train", "old", "2026-01-01T00:00:00Z"),
        record("k8s://c/ns/job/other", "foreign", "2026-06-01T00:00:00Z"),
        record("k8s://c/ns/job/train", "new", "2026-03-01T00:00:00Z"),
    ];
    let privacy = Privacy { labels: vec!["team".into()], salt: Some("s".into()), ..Privacy::default() };
    let ctx = build_context(
        &seed,
        key(),
        UnitId("main".into()),
        Prior::default(),
        PlatformRef::new("k8s"),
        history,
        &extra,
        &privacy,
        ReadBudget::default(),
        &r,
    )
    .await;
    assert_eq!(ctx.v, 1);
    let code = ctx.code.as_ref().unwrap();
    assert!(code.digest.starts_with("sha256:"));
    assert!(code.tokens.contains(&"tok:from_pretrained".to_string()));
    assert!(code.tokens.contains(&"num:batch_size=16".to_string()));
    assert_eq!(code.source, None, "source only when privacy.send_source");
    assert!(ctx.manifest.distributed);
    assert_eq!(ctx.manifest.distributed_markers, vec!["nproc_per_node", "torchrun"]);
    // Largest first, unknown sizes last.
    let names: Vec<&str> = ctx.inputs.iter().map(|i| i.name.as_str()).collect();
    assert_eq!(names[2], "lr");
    assert_eq!(ctx.inputs.iter().find(|i| i.name == "a.parquet").unwrap().rows, Some(42));
    assert_eq!(ctx.inputs.iter().find(|i| i.name == "w.npy").unwrap().shape, Some(vec![8, 8]));
    // History: same key, newest first.
    let ids: Vec<&str> = ctx.history.iter().map(|h| h.invocation_id.as_str()).collect();
    assert_eq!(ids, vec!["new", "old"]);
    // Labels: allowlisted, the argument overrides the seed.
    assert_eq!(ctx.labels.len(), 1);
    assert_eq!(ctx.labels["team"], "infra");
    // Salted hashes, no URIs.
    let json = serde_json::to_string(&ctx).unwrap();
    assert!(!json.contains("mem://"));
    assert_eq!(ctx.inputs.iter().filter(|i| i.uri_hash.is_some()).count(), 2);

    // Same inputs → same context digest.
    let again = build_context(
        &seed,
        key(),
        UnitId("main".into()),
        Prior::default(),
        PlatformRef::new("k8s"),
        vec![record("k8s://c/ns/job/train", "new", "2026-03-01T00:00:00Z"), record("k8s://c/ns/job/train", "old", "2026-01-01T00:00:00Z")],
        &extra,
        &privacy,
        ReadBudget::default(),
        &r,
    )
    .await;
    assert_eq!(context_digest(&ctx), context_digest(&again));
}

#[tokio::test]
async fn many_inputs_truncate_deterministically() {
    // 80 blobs: the 64 largest (then by name) are kept, whatever the ref order.
    let names: Vec<String> = (0..80).map(|i| format!("in/{i:03}.bin")).collect();
    let files: Vec<(&str, Vec<u8>)> = names.iter().enumerate().map(|(i, n)| (n.as_str(), vec![0u8; 10 + (i % 7) * 10])).collect();
    let store = mem_store(&files).await;
    let r = OneStore::new(store);
    let mut refs: Vec<InputRef> = names.iter().map(|n| uri(n)).collect();
    let build = |refs: Vec<InputRef>| {
        let seed = ContextSeed { inputs: refs, ..ContextSeed::default() };
        let r = &r;
        async move {
            build_context(
                &seed,
                key(),
                UnitId("main".into()),
                Prior::default(),
                PlatformRef::new("k8s"),
                vec![],
                &BTreeMap::new(),
                &Privacy::default(),
                ReadBudget::default(),
                r,
            )
            .await
        }
    };
    let a = build(refs.clone()).await;
    refs.reverse();
    let b = build(refs).await;
    assert_eq!(a.inputs.len(), 64);
    assert_eq!(a, b);
    assert_eq!(a.inputs[0].bytes, Some(70));
    let sizes: Vec<u64> = a.inputs.iter().map(|i| i.bytes.unwrap()).collect();
    assert!(sizes.windows(2).all(|w| w[0] >= w[1]));
    // The smallest (10-byte) inputs are the ones dropped.
    assert!(a.inputs.iter().all(|i| i.bytes.unwrap() >= 10));
}

fn arb_meta() -> impl Strategy<Value = InputMeta> {
    ("[a-e]{1,3}", proptest::option::of(0u64..50)).prop_map(|(n, b)| {
        let mut m = InputMeta::blob(n, 0);
        m.bytes = b;
        m
    })
}

proptest! {
    /// Truncation (SPEC §5.2) depends only on the multiset of inputs, never
    /// on their order: ≤ 64 kept, largest first, then by name.
    #[test]
    fn truncation_is_order_independent(mut inputs in proptest::collection::vec(arb_meta(), 0..150), seed in any::<u64>()) {
        let mk = |inputs: Vec<InputMeta>| {
            let mut c = WorkloadContext {
                v: 1, key: key(), unit: UnitId("u".into()), platform: PlatformRef::new("k8s"), code: None,
                inputs, manifest: Default::default(), prior: Prior::default(), history: vec![], labels: BTreeMap::new(),
            };
            rightsizer_features::canonical_inputs(&mut c.inputs);
            c.canonicalize(32);
            c
        };
        let a = mk(inputs.clone());
        // A deterministic shuffle.
        let mut s = seed | 1;
        for i in (1..inputs.len()).rev() {
            s ^= s << 13; s ^= s >> 7; s ^= s << 17;
            inputs.swap(i, (s % (i as u64 + 1)) as usize);
        }
        let b = mk(inputs);
        prop_assert!(a.inputs.len() <= 64);
        prop_assert_eq!(context_digest(&a), context_digest(&b));
        for w in a.inputs.windows(2) {
            let (x, y) = (w[0].bytes.unwrap_or(0), w[1].bytes.unwrap_or(0));
            prop_assert!(x > y || (x == y && w[0].name <= w[1].name));
        }
    }
}

#[test]
fn remote_view_follows_privacy() {
    let mut m = InputMeta::blob("t", 100);
    m.format = InputFormat::Parquet;
    m.rows = Some(5);
    m.dtype = Some("float32".into());
    m.columns = Some(ColumnsInfo { total: 2, numeric: 1, string: 1, nested: 0, other: 0, names: Some(vec!["a".into(), "b".into()]) });
    let mut s = InputMeta::blob("name", 0);
    s.bytes = None;
    s.format = InputFormat::Scalar;
    s.scalar = Some(ScalarValue::Str("secret".into()));
    let mut n = InputMeta::blob("k", 0);
    n.scalar = Some(ScalarValue::Int(3));
    let mut labels = BTreeMap::new();
    labels.insert("team".to_string(), "ml".to_string());
    labels.insert("owner".to_string(), "ann".to_string());
    let mut history = vec![record("k8s://c/ns/job/train", "a", "2026-01-01T00:00:00Z"); 40];
    history[0].invocation_id = "first".into();
    let ctx = WorkloadContext {
        v: 1,
        key: key(),
        unit: UnitId("u".into()),
        platform: PlatformRef::new("k8s"),
        code: Some(rightsizer_features::code_info("x = 1\n", None, true)),
        inputs: vec![m, s, n],
        manifest: Default::default(),
        prior: Prior::default(),
        history,
        labels,
    };
    let def = redact_for_remote(&ctx, &Privacy { history_limit: 10, ..Privacy::default() });
    let c = def.code.unwrap();
    assert!(c.source.is_none() && !c.tokens.is_empty() && c.digest.starts_with("sha256:"));
    assert_eq!(def.inputs[0].columns.as_ref().unwrap().names, None);
    assert_eq!(def.inputs[0].columns.as_ref().unwrap().total, 2);
    assert_eq!((def.inputs[0].rows, def.inputs[0].bytes), (Some(5), Some(100)));
    assert_eq!(def.inputs[1].scalar, None, "string scalars not sent");
    assert_eq!(def.inputs[2].scalar, Some(ScalarValue::Int(3)), "numeric scalars sent");
    assert!(def.labels.is_empty());
    assert_eq!(def.history.len(), 10);
    assert_eq!(def.history[0].invocation_id, "first");
    assert_eq!(def.history[0].inputs[0].columns.as_ref().unwrap().names, None);

    let open = Privacy {
        send_source: true,
        send: vec!["columns".into(), "strings".into(), "labels:team".into()],
        redact: vec!["sizes".into(), "scalars".into(), "dtypes".into()],
        ..Privacy::default()
    };
    let o = redact_for_remote(&ctx, &open);
    assert_eq!(o.code.unwrap().source.as_deref(), Some("x = 1\n"));
    assert_eq!(o.inputs[0].columns.as_ref().unwrap().names.as_ref().unwrap().len(), 2);
    assert_eq!(o.inputs[1].scalar, Some(ScalarValue::Str("secret".into())));
    assert_eq!(o.inputs[2].scalar, None);
    assert_eq!((o.inputs[0].bytes, o.inputs[0].dtype.as_deref()), (None, None));
    assert_eq!(o.labels.keys().collect::<Vec<_>>(), vec!["team"]);
    assert_eq!(o.history.len(), 32);
}

/// Compile-time check: the engine spawns decisions, so `build_context`'s
/// future (and `read_inputs`') must be `Send`.
#[allow(dead_code)]
fn build_context_future_is_send(seed: &ContextSeed, labels: &BTreeMap<String, String>, privacy: &Privacy, r: &OneStore) {
    fn assert_send<T: Send>(_: T) {}
    assert_send(build_context(
        seed,
        key(),
        UnitId("main".into()),
        Prior::default(),
        PlatformRef::new("k8s"),
        vec![],
        labels,
        privacy,
        ReadBudget::default(),
        r,
    ));
    assert_send(rightsizer_features::read_inputs(&seed.inputs, r, ReadBudget::default()));
}
