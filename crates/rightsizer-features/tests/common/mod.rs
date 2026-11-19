//! Shared test helpers: counting and slow `ObjectStore` wrappers, a
//! single-store resolver and fixture writers.
#![allow(dead_code)]

use std::fmt;
use std::ops::Range;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, ObjectStoreExt, PutMultipartOptions,
    PutOptions, PutPayload, PutResult, Result,
};
use rightsizer_features::StoreResolver;

/// Records every ranged GET (and its bytes) and HEAD.
#[derive(Debug)]
pub struct CountingStore {
    pub inner: Arc<dyn ObjectStore>,
    pub bytes: AtomicU64,
    pub heads: AtomicUsize,
    pub lists: AtomicUsize,
    pub reads: Mutex<Vec<(String, Range<u64>)>>,
    pub payloads: Mutex<Vec<Bytes>>,
    /// Delay for non-HEAD GETs (a deliberately slow store).
    pub delay: Option<Duration>,
}

impl CountingStore {
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        CountingStore {
            inner,
            bytes: AtomicU64::new(0),
            heads: AtomicUsize::new(0),
            lists: AtomicUsize::new(0),
            reads: Mutex::new(Vec::new()),
            payloads: Mutex::new(Vec::new()),
            delay: None,
        }
    }
    pub fn slow(inner: Arc<dyn ObjectStore>, delay: Duration) -> Self {
        CountingStore { delay: Some(delay), ..CountingStore::new(inner) }
    }
    pub fn bytes_read(&self) -> u64 {
        self.bytes.load(Ordering::SeqCst)
    }
    pub fn ranges(&self, path: &str) -> Vec<Range<u64>> {
        self.reads.lock().unwrap().iter().filter(|(p, _)| p == path).map(|(_, r)| r.clone()).collect()
    }
    pub fn bytes_for(&self, path: &str) -> u64 {
        self.ranges(path).iter().map(|r| r.end - r.start).sum()
    }
    pub fn saw(&self, needle: &[u8]) -> bool {
        self.payloads.lock().unwrap().iter().any(|b| b.windows(needle.len()).any(|w| w == needle))
    }
}

impl fmt::Display for CountingStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CountingStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for CountingStore {
    async fn put_opts(&self, location: &Path, payload: PutPayload, opts: PutOptions) -> Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }
    async fn put_multipart_opts(&self, location: &Path, opts: PutMultipartOptions) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }
    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        if options.head {
            self.heads.fetch_add(1, Ordering::SeqCst);
            return self.inner.get_opts(location, options).await;
        }
        if let Some(d) = self.delay {
            tokio::time::sleep(d).await;
        }
        let res = self.inner.get_opts(location, options).await?;
        let range = res.range.clone();
        let meta = res.meta.clone();
        let attributes = res.attributes.clone();
        let data = res.bytes().await?;
        self.bytes.fetch_add(data.len() as u64, Ordering::SeqCst);
        self.reads.lock().unwrap().push((location.to_string(), range.clone()));
        self.payloads.lock().unwrap().push(data.clone());
        let stream = futures::stream::once(async move { Ok(data) });
        Ok(GetResult {
            payload: object_store::GetResultPayload::Stream(Box::pin(stream)),
            meta,
            range,
            attributes,
            extensions: Default::default(),
        })
    }
    fn delete_stream(&self, locations: BoxStream<'static, Result<Path>>) -> BoxStream<'static, Result<Path>> {
        self.inner.delete_stream(locations)
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.lists.fetch_add(1, Ordering::SeqCst);
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// Resolves `mem://<path>` (and `hf://<repo>[@rev]`) to one store.
pub struct OneStore {
    pub store: Arc<dyn ObjectStore>,
    pub calls: AtomicUsize,
}

impl OneStore {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        OneStore { store, calls: AtomicUsize::new(0) }
    }
}

impl StoreResolver for OneStore {
    fn resolve(&self, uri: &str) -> Option<(Arc<dyn ObjectStore>, Path)> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if let Some(r) = rightsizer_features::store::HfRef::parse(uri) {
            return Some((Arc::clone(&self.store), r.base_path()));
        }
        let rest = uri.split_once("://").map(|(_, r)| r).unwrap_or(uri);
        Some((Arc::clone(&self.store), Path::from(rest)))
    }
}

/// An in-memory store with `files`, wrapped in a counter.
pub async fn mem_store(files: &[(&str, Vec<u8>)]) -> Arc<CountingStore> {
    let mem: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    for (p, b) in files {
        mem.put(&Path::from(*p), PutPayload::from(b.clone())).await.unwrap();
    }
    Arc::new(CountingStore::new(mem))
}

pub async fn slow_store(files: &[(&str, Vec<u8>)], delay: Duration) -> Arc<CountingStore> {
    let mem: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    for (p, b) in files {
        mem.put(&Path::from(*p), PutPayload::from(b.clone())).await.unwrap();
    }
    Arc::new(CountingStore::slow(mem, delay))
}

/// The secret a reader must never see: it lives only in data bodies.
pub const ANSWER: &[u8] = b"THE-ANSWER-PEAK-MEMORY-31337";

/// An NPY v1 file: `descr`, `shape`, then `data`.
pub fn npy_bytes(descr: &str, shape: &[u64], data: &[u8]) -> Vec<u8> {
    let shape_s = match shape.len() {
        0 => "()".to_string(),
        1 => format!("({},)", shape[0]),
        _ => format!("({})", shape.iter().map(|d| d.to_string()).collect::<Vec<_>>().join(", ")),
    };
    let mut header = format!("{{'descr': '{descr}', 'fortran_order': False, 'shape': {shape_s}, }}");
    while (10 + header.len() + 1) % 64 != 0 {
        header.push(' ');
    }
    header.push('\n');
    let mut out = b"\x93NUMPY\x01\x00".to_vec();
    out.extend_from_slice(&(header.len() as u16).to_le_bytes());
    out.extend_from_slice(header.as_bytes());
    out.extend_from_slice(data);
    out
}

/// A safetensors file with `tensors` = (name, dtype, shape, elem bytes);
/// tensor data is filled with [`ANSWER`].
pub fn safetensors_bytes(tensors: &[(&str, &str, &[u64], u64)]) -> Vec<u8> {
    let mut header = serde_json::Map::new();
    header.insert("__metadata__".into(), serde_json::json!({"format": "pt"}));
    let mut off = 0u64;
    for (name, dtype, shape, esize) in tensors {
        let n: u64 = shape.iter().product::<u64>() * esize;
        header.insert((*name).into(), serde_json::json!({"dtype": dtype, "shape": shape, "data_offsets": [off, off + n]}));
        off += n;
    }
    let mut h = serde_json::to_string(&serde_json::Value::Object(header)).unwrap();
    while !h.len().is_multiple_of(8) {
        h.push(' ');
    }
    let mut out = (h.len() as u64).to_le_bytes().to_vec();
    out.extend_from_slice(h.as_bytes());
    let mut data = Vec::with_capacity(off as usize);
    while (data.len() as u64) < off {
        data.extend_from_slice(ANSWER);
    }
    data.truncate(off as usize);
    out.extend_from_slice(&data);
    out
}

/// A Parquet file written with the `parquet` crate: `rows` rows of
/// `id: int64, x: double, name: utf8 (= ANSWER), flag: bool`, no statistics
/// (so data values never reach the footer), plus `kv_bytes` of footer
/// key-value metadata.
pub fn parquet_bytes(rows: usize, kv_bytes: usize) -> Vec<u8> {
    use parquet::data_type::{BoolType, ByteArray, ByteArrayType, DoubleType, Int64Type};
    use parquet::file::metadata::KeyValue;
    use parquet::file::properties::{EnabledStatistics, WriterProperties};
    use parquet::file::writer::SerializedFileWriter;
    use parquet::schema::parser::parse_message_type;

    let schema = Arc::new(
        parse_message_type(
            "message s { required int64 id; required double x; required binary name (UTF8); required boolean flag; }",
        )
        .unwrap(),
    );
    let mut props = WriterProperties::builder().set_statistics_enabled(EnabledStatistics::None);
    if kv_bytes > 0 {
        props = props.set_key_value_metadata(Some(vec![KeyValue::new("pad".to_string(), "p".repeat(kv_bytes))]));
    }
    let mut buf: Vec<u8> = Vec::new();
    {
        let mut w = SerializedFileWriter::new(&mut buf, schema, Arc::new(props.build())).unwrap();
        let mut rg = w.next_row_group().unwrap();
        let mut i = 0;
        while let Some(mut col) = rg.next_column().unwrap() {
            match i {
                0 => {
                    let v: Vec<i64> = (0..rows as i64).collect();
                    col.typed::<Int64Type>().write_batch(&v, None, None).unwrap();
                }
                1 => {
                    let v: Vec<f64> = (0..rows).map(|r| r as f64).collect();
                    col.typed::<DoubleType>().write_batch(&v, None, None).unwrap();
                }
                2 => {
                    let v: Vec<ByteArray> = (0..rows).map(|_| ByteArray::from(ANSWER.to_vec())).collect();
                    col.typed::<ByteArrayType>().write_batch(&v, None, None).unwrap();
                }
                _ => {
                    let v: Vec<bool> = (0..rows).map(|r| r % 2 == 0).collect();
                    col.typed::<BoolType>().write_batch(&v, None, None).unwrap();
                }
            }
            col.close().unwrap();
            i += 1;
        }
        rg.close().unwrap();
        w.close().unwrap();
    }
    buf
}

/// Footer length of a Parquet file (from its last 8 bytes).
pub fn parquet_footer_len(b: &[u8]) -> u64 {
    let n = b.len();
    u32::from_le_bytes(b[n - 8..n - 4].try_into().unwrap()) as u64
}

pub fn fixture(name: &str) -> Vec<u8> {
    std::fs::read(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures").join(name)).unwrap()
}
