//! The information boundary (SPEC §5.2): readers never read more than the
//! per-input budget, never read data bodies, and give up gracefully when the
//! per-context time budget expires.

mod common;

use std::time::{Duration, Instant};

use common::*;
use rightsizer_core::{InputFormat, InputRef};
use rightsizer_features::ReadBudget;
use rightsizer_features::readers::{ReadOptions, read_inputs_with};

fn input(name: &str, uri: &str) -> InputRef {
    InputRef { name: name.into(), uri: Some(uri.into()), bytes: None, format: None, scalar: None }
}

fn opts(budget: ReadBudget) -> ReadOptions {
    ReadOptions { use_cache: false, ..ReadOptions::new(budget) }
}

/// The answer (the data) is never visible to a reader: binary formats read
/// only their header/footer ranges, every input stays within its byte
/// budget, and the extracted metadata never contains data values.
#[tokio::test]
async fn test_context_cannot_see_the_answer() {
    let pq = parquet_bytes(20_000, 0);
    let pq_size = pq.len() as u64;
    let pq_footer = parquet_footer_len(&pq);
    let mut npy_data = Vec::new();
    while npy_data.len() < 200_000 {
        npy_data.extend_from_slice(ANSWER);
    }
    npy_data.truncate(200_000);
    let npy = npy_bytes("|u1", &[200_000], &npy_data);
    let st = safetensors_bytes(&[("w", "F32", &[256, 256], 4), ("b", "F32", &[256], 4)]);
    let st_header = u64::from_le_bytes(st[..8].try_into().unwrap());
    let mut csv = b"a,b,c\n".to_vec();
    while csv.len() < 300_000 {
        csv.extend_from_slice(ANSWER);
        csv.extend_from_slice(b",1,2\n");
    }
    let mut jsonl = b"{\"k\": 1}\n".to_vec();
    while jsonl.len() < 300_000 {
        jsonl.extend_from_slice(b"{\"k\": \"");
        jsonl.extend_from_slice(ANSWER);
        jsonl.extend_from_slice(b"\"}\n");
    }
    let files = vec![
        ("in/a.parquet", pq),
        ("in/b.npy", npy),
        ("in/c.safetensors", st.clone()),
        ("in/d.csv", csv),
        ("in/e.jsonl", jsonl),
        ("in/f.arrow", fixture("table.arrow")),
        ("in/g.bin", vec![0u8; 5_000_000]),
    ];
    let store = mem_store(&files).await;
    let r = OneStore::new(store.clone());
    let refs: Vec<InputRef> = files.iter().map(|(p, _)| input(p, &format!("mem://{p}"))).collect();
    let budget = ReadBudget::default();
    let out = read_inputs_with(&refs, &r, &opts(budget)).await;

    // Every input is within its byte budget.
    for (p, _) in &files {
        assert!(store.bytes_for(p) <= budget.per_input_bytes, "{p} read {} bytes", store.bytes_for(p));
    }
    // Binary formats never touch their data bodies.
    for r in store.ranges("in/a.parquet") {
        assert!(r.start >= pq_size - 8 - pq_footer, "parquet read {r:?} outside the footer");
    }
    for r in store.ranges("in/c.safetensors") {
        assert!(r.end <= 8 + st_header, "safetensors read {r:?} past the header");
    }
    let npy_data_start = 10 + u16::from_le_bytes([files[1].1[8], files[1].1[9]]) as u64;
    assert!(store.ranges("in/b.npy").iter().all(|r| r.end <= npy_data_start), "npy read past its header");
    assert!(store.ranges("in/g.bin").is_empty() || store.bytes_for("in/g.bin") <= 16, "blob: HEAD and ≤ 16 sniff bytes");
    // The CSV reader reads a small window; only the header line is used.
    assert!(store.bytes_for("in/d.csv") <= 4096);
    // No extracted field carries data.
    let json = serde_json::to_string(&out).unwrap();
    assert!(!json.contains(std::str::from_utf8(ANSWER).unwrap()), "metadata leaked data: {json}");
    // And the parquet/npy/safetensors/arrow readers never even saw it.
    let binary = mem_store(&files[..3].iter().cloned().chain([files[5].clone()]).collect::<Vec<_>>()).await;
    let r2 = OneStore::new(binary.clone());
    let refs2: Vec<InputRef> = ["in/a.parquet", "in/b.npy", "in/c.safetensors", "in/f.arrow"]
        .iter()
        .map(|p| input(p, &format!("mem://{p}")))
        .collect();
    let _ = read_inputs_with(&refs2, &r2, &opts(budget)).await;
    assert!(!binary.saw(ANSWER), "a binary reader read a data body");
    // URIs never appear in the metadata.
    assert!(!json.contains("mem://"));
}

/// A tight byte budget is honoured exactly: a header that does not fit is
/// skipped before any read of it.
#[tokio::test]
async fn byte_budget_is_a_hard_cap() {
    let pq = parquet_bytes(1000, 20_000);
    let st = safetensors_bytes(&[("w", "F32", &[64, 64], 4)]);
    let files = vec![("a.parquet", pq), ("b.safetensors", st)];
    let store = mem_store(&files).await;
    let r = OneStore::new(store.clone());
    let refs: Vec<InputRef> = files.iter().map(|(p, _)| input(p, &format!("mem://{p}"))).collect();
    for cap in [8u64, 64, 1024, 4096] {
        let budget = ReadBudget { per_input_bytes: cap, ..ReadBudget::default() };
        store.reads.lock().unwrap().clear();
        let out = read_inputs_with(&refs, &r, &opts(budget)).await;
        for (p, _) in &files {
            assert!(store.bytes_for(p) <= cap, "{p}: {} > {cap}", store.bytes_for(p));
        }
        assert_eq!(out[0].columns, None, "a 20 KB footer never fits {cap} bytes");
        assert!(out.iter().all(|m| m.bytes.is_some()), "sizes still come from HEAD");
    }
}

/// When the per-context time budget expires, inputs keep what was read
/// (sizes from HEAD), the rest stays unknown, and nothing errors.
#[tokio::test]
async fn budget_expiry_returns_partial_metadata() {
    let pq = parquet_bytes(100, 0);
    let size = pq.len() as u64;
    let files = vec![("s/a.parquet", pq.clone()), ("s/b.parquet", pq)];
    let store = slow_store(&files, Duration::from_secs(10)).await;
    let r = OneStore::new(store);
    let refs: Vec<InputRef> = files.iter().map(|(p, _)| input(p, &format!("mem://{p}"))).collect();
    let budget = ReadBudget { per_context: Duration::from_millis(200), ..ReadBudget::default() };
    let t = Instant::now();
    let out = read_inputs_with(&refs, &r, &opts(budget)).await;
    let took = t.elapsed();
    assert!(took < Duration::from_secs(2), "took {took:?}");
    assert_eq!(out.len(), 2);
    for m in &out {
        assert_eq!(m.bytes, Some(size), "size known from HEAD");
        assert_eq!(m.rows, None);
        assert_eq!(m.columns, None);
        assert_eq!(m.format, InputFormat::Parquet);
    }
}

/// Reads run concurrently: 8 slow inputs finish in about one delay.
#[tokio::test]
async fn reads_are_concurrent() {
    let pq = parquet_bytes(10, 0);
    let names: Vec<String> = (0..8).map(|i| format!("c/{i}.parquet")).collect();
    let files: Vec<(&str, Vec<u8>)> = names.iter().map(|n| (n.as_str(), pq.clone())).collect();
    let store = slow_store(&files, Duration::from_millis(150)).await;
    let r = OneStore::new(store);
    let refs: Vec<InputRef> = names.iter().map(|p| input(p, &format!("mem://{p}"))).collect();
    let t = Instant::now();
    let out = read_inputs_with(&refs, &r, &opts(ReadBudget { per_context: Duration::from_secs(5), ..ReadBudget::default() })).await;
    // Two sequential GETs per parquet input (tail, footer) ≈ 300 ms if parallel, 2.4 s if serial.
    assert!(t.elapsed() < Duration::from_millis(1500), "took {:?}", t.elapsed());
    assert!(out.iter().all(|m| m.rows == Some(10)));
}
