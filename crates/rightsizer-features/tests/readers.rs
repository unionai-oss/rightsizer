//! Per-format readers against in-memory stores and committed fixtures.

mod common;

use std::sync::Arc;
use std::sync::atomic::Ordering;

use common::*;
use rightsizer_core::{InputFormat, InputMeta, InputRef, ScalarValue};
use rightsizer_features::readers::{ReadOptions, clear_metadata_cache, read_inputs_with};
use rightsizer_features::{ReadBudget, read_inputs};

fn input(name: &str, uri: &str) -> InputRef {
    InputRef { name: name.into(), uri: Some(uri.into()), bytes: None, format: None, scalar: None }
}

fn opts() -> ReadOptions {
    ReadOptions { use_cache: false, ..ReadOptions::default().with_salt(Some("test".into())) }
}

async fn read1(files: &[(&str, Vec<u8>)], uri: &str) -> (InputMeta, Arc<CountingStore>) {
    let store = mem_store(files).await;
    let r = OneStore::new(store.clone());
    let out = read_inputs_with(&[input("x", uri)], &r, &opts()).await;
    (out.into_iter().next().unwrap(), store)
}

#[tokio::test]
async fn parquet_footer_crate_written() {
    let b = parquet_bytes(500, 0);
    let size = b.len() as u64;
    let (m, s) = read1(&[("d/a.parquet", b.clone())], "mem://d/a.parquet").await;
    assert_eq!(m.format, InputFormat::Parquet);
    assert_eq!(m.bytes, Some(size));
    assert_eq!(m.rows, Some(500));
    let c = m.columns.unwrap();
    assert_eq!((c.total, c.numeric, c.string, c.nested, c.other), (4, 2, 1, 0, 1));
    assert_eq!(c.names.unwrap(), vec!["id", "x", "name", "flag"]);
    // Exactly the tail and the footer.
    let flen = parquet_footer_len(&b);
    assert_eq!(s.ranges("d/a.parquet"), vec![size - 8..size, size - 8 - flen..size - 8]);
    assert!(m.uri_hash.unwrap().starts_with("sha256:"));
}

#[tokio::test]
async fn parquet_footer_pyarrow_nested() {
    let (m, _) = read1(&[("p.parquet", fixture("pyarrow.parquet"))], "mem://p.parquet").await;
    assert_eq!(m.rows, Some(200));
    let c = m.columns.unwrap();
    // id, score → numeric; name, cat → string; tags, meta → nested; ts, ok → other.
    assert_eq!((c.total, c.numeric, c.string, c.nested, c.other), (8, 2, 2, 2, 2));
}

#[tokio::test]
async fn parquet_huge_footer_is_skipped() {
    // 1.2 MiB of key-value metadata: the footer exceeds the 1 MiB budget.
    let b = parquet_bytes(10, 1_200_000);
    let size = b.len() as u64;
    let (m, s) = read1(&[("h.parquet", b)], "mem://h.parquet").await;
    assert_eq!(m.bytes, Some(size));
    assert_eq!(m.columns, None);
    assert_eq!(m.rows, None);
    assert_eq!(s.bytes_read(), 8, "only the 8-byte tail is read");
}

#[tokio::test]
async fn npy_header() {
    let data = vec![7u8; 3 * 4 * 4];
    let b = npy_bytes("<f4", &[3, 4], &data);
    let header_end = 10 + u16::from_le_bytes([b[8], b[9]]) as u64;
    let (m, s) = read1(&[("a.npy", b)], "mem://a.npy").await;
    assert_eq!(m.format, InputFormat::Npy);
    assert_eq!(m.shape, Some(vec![3, 4]));
    assert_eq!(m.dtype.as_deref(), Some("float32"));
    assert_eq!(s.bytes_read(), header_end, "the header only");
    let (m, _) = read1(&[("b.npy", npy_bytes("|u1", &[10], &[0; 10]))], "mem://b.npy").await;
    assert_eq!((m.shape, m.dtype.as_deref()), (Some(vec![10]), Some("uint8")));
    let (m, _) = read1(&[("c.npy", npy_bytes("<f8", &[], &[0; 8]))], "mem://c.npy").await;
    assert_eq!((m.shape, m.dtype.as_deref()), (Some(vec![]), Some("float64")));
}

#[tokio::test]
async fn safetensors_header_params() {
    let b = safetensors_bytes(&[("w", "BF16", &[64, 32], 2), ("b", "F32", &[64], 4), ("e", "BF16", &[100, 8], 2)]);
    let (m, _) = read1(&[("m.safetensors", b)], "mem://m.safetensors").await;
    assert_eq!(m.format, InputFormat::Safetensors);
    assert_eq!(m.params, Some(64 * 32 + 64 + 800));
    assert_eq!(m.dtype.as_deref(), Some("bfloat16"));
    assert_eq!(m.shape, None);
    let one = safetensors_bytes(&[("only", "F16", &[5, 6], 2)]);
    let (m, _) = read1(&[("o.safetensors", one)], "mem://o.safetensors").await;
    assert_eq!((m.params, m.shape), (Some(30), Some(vec![5, 6])));
}

#[tokio::test]
async fn csv_first_line_only() {
    let mut b = b"id,\"name, full\",score\r\n".to_vec();
    for i in 0..10_000 {
        b.extend_from_slice(format!("{i},x,{}\n", i * 2).as_bytes());
    }
    let (m, s) = read1(&[("t.csv", b)], "mem://t.csv").await;
    let c = m.columns.unwrap();
    assert_eq!(c.total, 3);
    assert_eq!(c.names.unwrap(), vec!["id", "name, full", "score"]);
    assert_eq!(m.rows, None, "no row count for CSV");
    assert!(s.bytes_read() <= 4096);
    let (m, _) = read1(&[("t.tsv", b"a\tb\tc\td\n1\t2\t3\t4\n".to_vec())], "mem://t.tsv").await;
    assert_eq!(m.columns.unwrap().total, 4);
    let (m, _) = read1(&[("one.csv", b"a;b".to_vec())], "mem://one.csv").await;
    assert_eq!(m.columns.unwrap().total, 2);
}

#[tokio::test]
async fn csv_header_longer_than_window() {
    let b = format!("{}\n1\n", "c,".repeat(70_000)).into_bytes();
    let (m, s) = read1(&[("w.csv", b.clone())], "mem://w.csv").await;
    assert_eq!(m.columns, None);
    assert_eq!(m.bytes, Some(b.len() as u64));
    assert!(s.bytes_read() <= 65536);
}

#[tokio::test]
async fn jsonl_first_record_keys() {
    let b = b"{\"a\": 1, \"b\": \"s\", \"c\": [1], \"d\": {\"x\": 1}, \"e\": true}\n{\"z\": 2}\n".to_vec();
    let (m, _) = read1(&[("r.jsonl", b)], "mem://r.jsonl").await;
    let c = m.columns.unwrap();
    assert_eq!((c.total, c.numeric, c.string, c.nested, c.other), (5, 1, 1, 2, 1));
    assert_eq!(c.names.unwrap(), vec!["a", "b", "c", "d", "e"]);
}

#[tokio::test]
async fn arrow_ipc_file_and_stream() {
    for name in ["table.arrow", "table.arrows"] {
        let b = fixture(name);
        let (m, s) = read1(&[(name, b.clone())], &format!("mem://{name}")).await;
        assert_eq!(m.format, InputFormat::ArrowIpc, "{name}");
        let c = m.columns.unwrap();
        assert_eq!((c.total, c.numeric, c.string, c.nested, c.other), (8, 2, 2, 2, 2), "{name}");
        assert_eq!(c.names.unwrap(), vec!["id", "score", "name", "tags", "meta", "ts", "ok", "cat"]);
        assert!(s.bytes_read() < b.len() as u64 / 4, "{name}: schema only, read {}", s.bytes_read());
    }
}

#[tokio::test]
async fn magic_bytes_detection() {
    let pq = parquet_bytes(5, 0);
    let npy = npy_bytes("<i8", &[2], &[0; 16]);
    let st = safetensors_bytes(&[("w", "F32", &[2, 2], 4)]);
    let arrow = fixture("table.arrow");
    let store = mem_store(&[("p", pq), ("n", npy), ("s", st), ("a", arrow), ("b", b"hello world".to_vec()), ("z.gz", b"PAR1xxxx".to_vec())])
        .await;
    let r = OneStore::new(store);
    let refs: Vec<InputRef> = ["p", "n", "s", "a", "b", "z.gz"].iter().map(|n| input(n, &format!("mem://{n}"))).collect();
    let out = read_inputs_with(&refs, &r, &opts()).await;
    let f: Vec<InputFormat> = out.iter().map(|m| m.format).collect();
    use InputFormat::*;
    assert_eq!(f, vec![Parquet, Npy, Safetensors, ArrowIpc, Blob, Blob]);
    assert_eq!(out[0].rows, Some(5));
    assert_eq!(out[4].bytes, Some(11));
}

#[tokio::test]
async fn hint_overrides_extension() {
    let store = mem_store(&[("data.bin", parquet_bytes(3, 0))]).await;
    let r = OneStore::new(store);
    let mut i = input("x", "mem://data.bin");
    i.format = Some(InputFormat::Parquet);
    let out = read_inputs_with(&[i], &r, &opts()).await;
    assert_eq!(out[0].rows, Some(3));
}

#[tokio::test]
async fn directory_listing() {
    let files: Vec<(String, Vec<u8>)> = (0..25).map(|i| (format!("dir/sub{}/f{i}", i % 3), vec![0u8; 100])).collect();
    let refs: Vec<(&str, Vec<u8>)> = files.iter().map(|(p, b)| (p.as_str(), b.clone())).collect();
    let store = mem_store(&refs).await;
    let r = OneStore::new(store.clone());
    let out = read_inputs_with(&[input("d", "mem://dir/")], &r, &opts()).await;
    assert_eq!(out[0].format, InputFormat::Dir);
    assert_eq!(out[0].bytes, Some(2500));
    assert_eq!(out[0].shape, Some(vec![25]));
    assert_eq!(store.bytes_read(), 0, "listing reads no bodies");
    // Without a trailing slash: HEAD misses, the listing finds it.
    let out = read_inputs_with(&[input("d", "mem://dir")], &r, &opts()).await;
    assert_eq!((out[0].format, out[0].bytes), (InputFormat::Dir, Some(2500)));
    // Over the entry limit: unknown, not a misleading partial sum.
    let small = ReadOptions { budget: ReadBudget { max_dir_entries: 10, ..ReadBudget::default() }, ..opts() };
    let out = read_inputs_with(&[input("d", "mem://dir/")], &r, &small).await;
    assert_eq!((out[0].format, out[0].bytes, out[0].shape.clone()), (InputFormat::Dir, None, None));
}

#[tokio::test]
async fn scalars_blobs_and_missing() {
    let store = mem_store(&[("blob.bin", vec![1; 1234])]).await;
    let r = OneStore::new(store);
    let refs = vec![
        InputRef { name: "lr".into(), uri: None, bytes: None, format: None, scalar: Some(ScalarValue::Float(0.1)) },
        InputRef { name: "tag".into(), uri: None, bytes: None, format: None, scalar: Some(ScalarValue::Str("v1".into())) },
        input("b", "mem://blob.bin"),
        input("missing", "mem://nope.parquet"),
        InputRef { name: "obj".into(), uri: None, bytes: Some(99), format: None, scalar: None },
    ];
    let out = read_inputs_with(&refs, &r, &opts()).await;
    assert_eq!(out[0].format, InputFormat::Scalar);
    assert_eq!(out[0].scalar, Some(ScalarValue::Float(0.1)));
    assert_eq!(out[1].scalar, Some(ScalarValue::Str("v1".into())));
    assert_eq!((out[2].format, out[2].bytes), (InputFormat::Blob, Some(1234)));
    assert_eq!(out[3].bytes, None);
    assert_eq!(out[3].format, InputFormat::Blob);
    assert_eq!(out[4].bytes, Some(99));
    let no_strings = ReadOptions { string_scalars: false, column_names: false, ..opts() };
    let out = read_inputs_with(&refs, &r, &no_strings).await;
    assert_eq!(out[1].scalar, None);
}

#[tokio::test]
async fn names_dropped_when_not_allowed() {
    let store = mem_store(&[("a.parquet", parquet_bytes(5, 0))]).await;
    let r = OneStore::new(store);
    let o = ReadOptions { column_names: false, ..opts() };
    let out = read_inputs_with(&[input("x", "mem://a.parquet")], &r, &o).await;
    let c = out[0].columns.clone().unwrap();
    assert_eq!((c.total, c.names), (4, None));
}

#[tokio::test]
async fn metadata_cache_skips_reread() {
    clear_metadata_cache();
    let store = mem_store(&[("cache-test/c.parquet", parquet_bytes(5, 0))]).await;
    let r = OneStore::new(store.clone());
    let o = ReadOptions { use_cache: true, ..opts() };
    let a = read_inputs_with(&[input("x", "mem://cache-test/c.parquet")], &r, &o).await;
    let first = store.bytes_read();
    assert!(first > 0);
    let b = read_inputs_with(&[input("x", "mem://cache-test/c.parquet")], &r, &o).await;
    assert_eq!(a, b);
    assert_eq!(store.bytes_read(), first, "second read served from cache (HEAD only)");
    assert!(store.heads.load(Ordering::SeqCst) >= 2);
}

#[tokio::test]
async fn hf_model_ref_offline() {
    // A fake hub: config + index with two shards.
    let s1 = safetensors_bytes(&[("a", "BF16", &[100, 10], 2)]);
    let s2 = safetensors_bytes(&[("b", "BF16", &[50], 2), ("c", "BF16", &[10, 10], 2)]);
    let index = serde_json::json!({
        "metadata": {"total_size": 2300},
        "weight_map": {"a": "model-00001-of-00002.safetensors", "b": "model-00002-of-00002.safetensors", "c": "model-00002-of-00002.safetensors"}
    });
    let base = "org/model/resolve/main";
    let files = [
        (format!("{base}/config.json"), br#"{"torch_dtype": "bfloat16", "hidden_size": 10}"#.to_vec()),
        (format!("{base}/model.safetensors.index.json"), serde_json::to_vec(&index).unwrap()),
        (format!("{base}/model-00001-of-00002.safetensors"), s1),
        (format!("{base}/model-00002-of-00002.safetensors"), s2),
    ];
    let refs: Vec<(&str, Vec<u8>)> = files.iter().map(|(p, b)| (p.as_str(), b.clone())).collect();
    let store = mem_store(&refs).await;
    let r = OneStore::new(store.clone());
    // huggingface.co is allowlisted so the guard does not need DNS.
    let o = opts().with_guard(rightsizer_features::SsrfGuard::new(&["huggingface.co"]));
    let out = read_inputs_with(&[input("model", "hf://org/model")], &r, &o).await;
    assert_eq!(out[0].format, InputFormat::ModelRef);
    assert_eq!(out[0].params, Some(1000 + 50 + 100));
    assert_eq!(out[0].dtype.as_deref(), Some("bfloat16"));
    assert_eq!(out[0].bytes, Some(2300));
    assert!(!store.saw(ANSWER), "no weights downloaded");

    // Single-file model.
    let single = safetensors_bytes(&[("w", "F16", &[4, 4], 2)]);
    let store = mem_store(&[("solo/m/resolve/v2/model.safetensors", single.clone())]).await;
    let r = OneStore::new(store.clone());
    let out = read_inputs_with(&[input("model", "hf://solo/m@v2")], &r, &o).await;
    assert_eq!((out[0].params, out[0].bytes, out[0].dtype.as_deref()), (Some(16), Some(single.len() as u64), Some("float16")));
    assert!(!store.saw(ANSWER));
}

#[tokio::test]
async fn local_files_through_default_resolver() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("a.parquet");
    std::fs::write(&p, parquet_bytes(7, 0)).unwrap();
    let npy = dir.path().join("x.npy");
    std::fs::write(&npy, npy_bytes("<f8", &[2, 3], &[0; 48])).unwrap();
    std::fs::create_dir(dir.path().join("shards")).unwrap();
    for i in 0..3 {
        std::fs::write(dir.path().join(format!("shards/{i}.bin")), vec![0u8; 10]).unwrap();
    }
    let refs = vec![
        input("a", p.to_str().unwrap()),
        input("x", url::Url::from_file_path(&npy).unwrap().as_str()),
        input("shards", dir.path().join("shards").to_str().unwrap()),
    ];
    let r = rightsizer_features::DefaultResolver::default();
    let out = read_inputs(&refs, &r, ReadBudget::default()).await;
    assert_eq!(out[0].rows, Some(7));
    assert_eq!(out[1].shape, Some(vec![2, 3]));
    assert_eq!((out[2].format, out[2].bytes, out[2].shape.clone()), (InputFormat::Dir, Some(30), Some(vec![3])));
}

#[tokio::test]
async fn hash_only_does_no_io() {
    let store = mem_store(&[("d/a.parquet", parquet_bytes(10, 0))]).await;
    let r = OneStore::new(store.clone());
    let o = ReadOptions { hash_only: true, ..opts() };
    let out = read_inputs_with(&[input("x", "mem://d/a.parquet")], &r, &o).await;
    assert_eq!(out[0].rows, None);
    assert_eq!(out[0].bytes, None);
    assert!(out[0].uri_hash.as_deref().is_some_and(|h| h.starts_with("sha256:")), "URI still hashed");
    assert_eq!(store.bytes_read(), 0);
    assert_eq!(store.heads.load(std::sync::atomic::Ordering::SeqCst), 0);
}
