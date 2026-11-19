//! Adversarial review regressions: stores that lie about ranges, privacy of
//! manifest strings, CPU bounds of the tokenizer, special code files, and
//! SSRF/hf edge cases.

mod common;

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta, ObjectStore, ObjectStoreExt,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
};
use rightsizer_core::{CodeRef, ContextSeed, InputRef, PlatformRef, Prior, ScalarValue, UnitId, WorkloadKey};
use rightsizer_features::readers::{ReadOptions, read_inputs_with};
use rightsizer_features::{Privacy, ReadBudget, StoreResolver, build_context, redact_for_remote};

use common::{OneStore, parquet_bytes, safetensors_bytes};

/// How a [`LyingStore`] answers ranged GETs.
#[derive(Copy, Clone, Debug)]
enum Lie {
    /// Fewer bytes than asked (a truncated 206 body).
    Short,
    /// The asked range followed by `n` extra 1 KiB chunks (a 206 whose
    /// chunked body runs past its Content-Range).
    Long(u64),
}

/// Wraps a store; ranged GET bodies lie per [`Lie`]. Counts the body bytes
/// the reader actually pulled from the stream.
#[derive(Debug)]
struct LyingStore {
    inner: Arc<dyn ObjectStore>,
    lie: Lie,
    pulled: Arc<AtomicU64>,
}

impl fmt::Display for LyingStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LyingStore")
    }
}

#[async_trait]
impl ObjectStore for LyingStore {
    async fn put_opts(&self, l: &Path, p: PutPayload, o: PutOptions) -> Result<PutResult> {
        self.inner.put_opts(l, p, o).await
    }
    async fn put_multipart_opts(&self, l: &Path, o: PutMultipartOptions) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(l, o).await
    }
    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        if options.head || options.range.is_none() {
            return self.inner.get_opts(location, options).await;
        }
        let res = self.inner.get_opts(location, options).await?;
        let (meta, range, attributes) = (res.meta.clone(), res.range.clone(), res.attributes.clone());
        let data = res.bytes().await?;
        let mut chunks: Vec<Bytes> = Vec::new();
        match self.lie {
            Lie::Short => chunks.push(data.slice(..data.len() / 2)),
            Lie::Long(n) => {
                chunks.push(data);
                for _ in 0..n {
                    chunks.push(Bytes::from(vec![b'x'; 1024]));
                }
            }
        }
        let pulled = Arc::clone(&self.pulled);
        let stream = futures::stream::iter(chunks.into_iter().map(move |c| {
            pulled.fetch_add(c.len() as u64, Ordering::SeqCst);
            Ok(c)
        }));
        Ok(GetResult { payload: GetResultPayload::Stream(Box::pin(stream)), meta, range, attributes, extensions: Default::default() })
    }
    fn delete_stream(&self, l: BoxStream<'static, Result<Path>>) -> BoxStream<'static, Result<Path>> {
        self.inner.delete_stream(l)
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(&self, from: &Path, to: &Path, o: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, o).await
    }
}

async fn lying(files: &[(&str, Vec<u8>)], lie: Lie) -> (OneStore, Arc<AtomicU64>) {
    let mem: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    for (p, b) in files {
        mem.put(&Path::from(*p), PutPayload::from(b.clone())).await.unwrap();
    }
    let pulled = Arc::new(AtomicU64::new(0));
    let s: Arc<dyn ObjectStore> = Arc::new(LyingStore { inner: mem, lie, pulled: Arc::clone(&pulled) });
    (OneStore::new(s), pulled)
}

fn input(name: &str, uri: &str) -> InputRef {
    InputRef { name: name.into(), uri: Some(uri.into()), bytes: None, format: None, scalar: None }
}

fn opts() -> ReadOptions {
    ReadOptions { use_cache: false, ..ReadOptions::default() }
}

/// A store returning a short range body must not panic a reader (slicing
/// `tail[4..8]`, `pre[8]`, …): the read fails and the input keeps its size.
#[tokio::test]
async fn short_range_bodies_never_panic() {
    let files = vec![
        ("a.parquet", parquet_bytes(5, 0)),
        ("b.npy", common::npy_bytes("<f4", &[3, 4], &[0u8; 48])),
        ("c.safetensors", safetensors_bytes(&[("w", "F32", &[2, 2], 4)])),
        ("d.arrow", common::fixture("table.arrow")),
        ("e.bin", vec![0u8; 64]),
    ];
    let (r, _) = lying(&files, Lie::Short).await;
    let refs: Vec<InputRef> = files.iter().map(|(p, _)| input(p, &format!("mem://{p}"))).collect();
    let task = tokio::spawn(async move { read_inputs_with(&refs, &r, &opts()).await });
    let out = task.await.expect("a reader panicked on a short range body");
    for m in &out {
        assert!(m.bytes.is_some(), "{} keeps its HEAD size", m.name);
        assert!(m.columns.is_none() && m.shape.is_none() && m.params.is_none(), "{} took metadata from a short body", m.name);
    }
}

/// A range body longer than asked is cut at the budget: the reader stops
/// pulling (SPEC §5.2: ≤ 1 MiB per input, whatever the server sends).
#[tokio::test]
async fn overlong_range_bodies_stay_within_budget() {
    let files = vec![("a.parquet", parquet_bytes(5, 0))];
    let (r, pulled) = lying(&files, Lie::Long(64 * 1024)).await; // +64 MiB per range
    let out = read_inputs_with(&[input("x", "mem://a.parquet")], &r, &opts()).await;
    let budget = ReadBudget::default().per_input_bytes;
    let got = pulled.load(Ordering::SeqCst);
    assert!(got <= budget + 2 * 1024, "pulled {got} bytes for a 1 MiB budget");
    assert!(out[0].columns.is_none(), "an overlong body is not trusted");
}

fn key() -> WorkloadKey {
    WorkloadKey::parse("k8s://c/ns/job/train").unwrap()
}

/// String values in `manifest.extra` (e.g. the K8s container image, which
/// names private registries and repos) are string scalars: dropped for
/// remote estimators unless `send = ["strings"]` (SPEC §11).
#[tokio::test]
async fn manifest_string_extras_are_redacted_for_remote() {
    let mut seed = ContextSeed::default();
    seed.manifest.extra.insert("image".into(), ScalarValue::Str("123456789012.dkr.ecr.us-east-1.amazonaws.com/secret-team/model:v7".into()));
    seed.manifest.extra.insert("replicas".into(), ScalarValue::Int(3));
    let r = OneStore::new(Arc::new(InMemory::new()));
    let ctx = build_context(&seed, key(), UnitId("main".into()), Prior::default(), PlatformRef::new("k8s"), vec![], &Default::default(), &Privacy::default(), ReadBudget::default(), &r as &dyn StoreResolver).await;
    assert!(ctx.manifest.extra.contains_key("image"), "in-process estimators see everything");
    let out = redact_for_remote(&ctx, &Privacy::default());
    assert!(!out.manifest.extra.contains_key("image"), "image string sent to a remote estimator");
    assert_eq!(out.manifest.extra.get("replicas"), Some(&ScalarValue::Int(3)));
    let sent = redact_for_remote(&ctx, &Privacy { send: vec!["strings".into()], ..Privacy::default() });
    assert!(sent.manifest.extra.contains_key("image"));
}

/// The tokenizer is linear: a long line with many numeric-token
/// occurrences (CodeRef::Source is unbounded and is tokenized outside any
/// time budget) must not go quadratic.
#[test]
fn code_tokens_linear_on_long_lines() {
    let src = "batch_size ".repeat(60_000); // 660 KB, one line
    let t = Instant::now();
    let toks = rightsizer_features::code_tokens(&src);
    assert!(t.elapsed() < Duration::from_secs(2), "code_tokens took {:?}", t.elapsed());
    assert!(toks.iter().any(|t| t.contains("batch_size")));
    // Same results as before for the window rule.
    let toks = rightsizer_features::code_tokens("x = dict(batch_size = 16)\nper_device_train_batch_size=8");
    assert!(toks.iter().any(|t| t.ends_with("batch_size=16")), "{toks:?}");
}

/// `CodeRef::Path` naming a device (`/dev/zero` reports length 0) must not
/// read until the time budget runs out.
#[cfg(unix)]
#[tokio::test]
async fn code_path_device_is_not_read() {
    let seed = ContextSeed { code: Some(CodeRef::Path { path: "/dev/zero".into() }), ..ContextSeed::default() };
    let r = OneStore::new(Arc::new(InMemory::new()));
    let budget = ReadBudget { per_context: Duration::from_millis(400), ..ReadBudget::default() };
    let t = Instant::now();
    let ctx = build_context(&seed, key(), UnitId("main".into()), Prior::default(), PlatformRef::new("k8s"), vec![], &Default::default(), &Privacy::default(), budget, &r as &dyn StoreResolver).await;
    assert!(ctx.code.is_none());
    assert!(t.elapsed() < Duration::from_millis(200), "read /dev/zero for {:?}", t.elapsed());
}

/// `hf://` revisions `.`/`..` are refused (they would address the repo root
/// or another revision's directory on servers that normalize `%2E%2E`).
#[test]
fn hf_dot_revisions_refused() {
    use rightsizer_features::store::HfRef;
    assert!(HfRef::parse("hf://org/model@..").is_none());
    assert!(HfRef::parse("hf://org/model@.").is_none());
    assert!(HfRef::parse("hf://org/model@v1.0").is_some());
}

/// NAT64 local-use prefix (RFC 8215, `64:ff9b:1::/48`) embeds IPv4 like the
/// well-known prefix and is internal by definition.
#[test]
fn nat64_local_use_prefix_forbidden() {
    use rightsizer_features::ssrf::is_forbidden_ip;
    assert!(is_forbidden_ip("64:ff9b:1::a9fe:a9fe".parse().unwrap()));
    assert!(is_forbidden_ip("64:ff9b:1:abcd::808:808".parse().unwrap()));
}

/// Malformed Parquet footers never panic (the footer bytes are
/// attacker-controlled through any readable input).
#[tokio::test]
async fn garbage_parquet_footers_never_panic() {
    let mut files = Vec::new();
    for seed in 0u64..200 {
        let mut x = seed.wrapping_mul(0x9e3779b97f4a7c15) | 1;
        let len = 16 + (seed as usize * 13) % 400;
        let mut body: Vec<u8> = b"PAR1".to_vec();
        for _ in 0..len {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            body.push(x as u8);
        }
        body.extend_from_slice(&(len as u32).to_le_bytes());
        body.extend_from_slice(b"PAR1");
        files.push((format!("g{seed}.parquet"), body));
    }
    let mem: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    for (p, b) in &files {
        mem.put(&Path::from(p.as_str()), PutPayload::from(b.clone())).await.unwrap();
    }
    let r = OneStore::new(mem);
    let refs: Vec<InputRef> = files.iter().map(|(p, _)| input(p, &format!("mem://{p}"))).collect();
    let task = tokio::spawn(async move { read_inputs_with(&refs, &r, &ReadOptions { concurrency: 1, ..opts() }).await });
    let out = task.await.expect("parquet footer decoding panicked");
    assert_eq!(out.len(), files.len());
}

/// `build_context` with no privacy salt keeps the `RIGHTSIZER_URI_SALT` salt
/// (it used to replace it with the empty one: unsalted, guessable hashes, and
/// hashes that differ from `read_inputs` for the same URI).
#[tokio::test]
async fn build_context_keeps_env_salt() {
    // SAFETY: no other test in this binary reads or writes this variable.
    unsafe { std::env::set_var(rightsizer_features::privacy::DEFAULT_SALT_ENV, "env-salt") };
    let seed = ContextSeed { inputs: vec![InputRef { bytes: Some(10), ..input("x", "mem://nope/x.bin") }], ..ContextSeed::default() };
    let r = OneStore::new(Arc::new(InMemory::new()));
    let ctx = build_context(&seed, key(), UnitId("main".into()), Prior::default(), PlatformRef::new("k8s"), vec![], &Default::default(), &Privacy::default(), ReadBudget::default(), &r as &dyn StoreResolver).await;
    assert_eq!(ctx.inputs[0].uri_hash.as_deref(), Some(rightsizer_features::uri_hash("mem://nope/x.bin", "env-salt").as_str()));
    let direct = read_inputs_with(&seed.inputs, &r, &ReadOptions::default()).await;
    assert_eq!(direct[0].uri_hash, ctx.inputs[0].uri_hash);
}
