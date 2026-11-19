//! Input metadata readers (SPEC §5.2 "no peeking", §9).
//!
//! Every reader works from bounded byte ranges (headers and footers only,
//! ≤ [`ReadBudget::per_input_bytes`] per input, 1 MiB by default) and never
//! reads a data body. A header or footer larger than the remaining budget is
//! skipped and its fields stay `None` (SPEC §5.2;
//! `test_context_cannot_see_the_answer`).
//!
//! | format | detected by | reads | extracts |
//! |---|---|---|---|
//! | Parquet | `.parquet` `.pq` `.parq`, magic `PAR1` | last 8 bytes, then exactly the footer | `rows`; `columns` by kind (top-level fields: groups/repeated → nested; strings/binary/UUID/JSON → string; ints/floats/decimals → numeric; bool/temporal/other → other); names |
//! | NPY | `.npy`, magic `\x93NUMPY` | first 12 bytes, then the header dict | `shape`, `dtype` (NumPy name, e.g. `float32`) |
//! | safetensors | `.safetensors`, `u64` length + `{` | first 8 bytes, then the JSON header | `params` = Σ ∏ shape; `dtype` = the dtype holding most parameters; `shape` when there is one tensor |
//! | CSV | `.csv` `.tsv` | the first line only (4 KiB, then up to 64 KiB) | `columns.total` (kinds unknown); names |
//! | JSONL | `.jsonl` `.ndjson` | the first line only (4 KiB, 64 KiB, then the budget) | `columns` by the first record's value kinds; names (keys) |
//! | Arrow IPC | `.arrow` `.feather` `.ipc` `.arrows`, magic `ARROW1` or `0xFFFFFFFF` | the 8-byte file magic, the 8-byte message prefix, the schema message | `columns` by kind; names |
//! | directory | trailing `/`, hint `dir`, or a HEAD miss with a non-empty listing | a recursive listing (≤ `max_dir_entries`) | `bytes` = total size, `shape = [entry count]`; both `None` when the listing is truncated |
//! | `hf://` model ref | scheme | `config.json`, `model.safetensors.index.json` or `model.safetensors`'s header | `params`, `dtype`, `bytes` |
//! | blob | anything else | HEAD only | `bytes` |
//! | scalar | `InputRef.scalar` | nothing | `scalar` |
//!
//! Format: the ref's hint, else the extension, else magic bytes (the first
//! 16 bytes, counted against the budget). Compressed extensions (`.gz`,
//! `.zst`, `.bz2`, `.xz`, `.zip`) are blobs.
//!
//! Budgets: bytes per input as above; one time budget per context
//! ([`ReadBudget::per_context`]). When time runs out, every input keeps what
//! was read so far (typically its size, from HEAD) and the rest stays
//! `None`; the context is never failed. Reads run concurrently, at most
//! [`ReadOptions::concurrency`] at a time. HTTP(S) and `hf://` URIs pass the
//! [`SsrfGuard`] first.
//!
//! Results are cached process-wide, keyed by (URI digest, ETag — or size and
//! modification time), LRU-bounded at [`METADATA_CACHE_CAPACITY`] entries.

mod arrow;
mod cache;
mod formats;
mod hf;

use std::ops::Range;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use futures::StreamExt;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use rightsizer_core::{ColumnsInfo, InputFormat, InputMeta, InputRef, ScalarValue};

pub use crate::store::{DefaultResolver, StoreResolver};
use crate::ssrf::SsrfGuard;
use crate::{FeatureError, ReadBudget};
pub use cache::{METADATA_CACHE_CAPACITY, clear_metadata_cache, metadata_cache_len};

/// Options for [`read_inputs_with`].
#[derive(Clone, Debug)]
pub struct ReadOptions {
    /// Byte, time and listing budgets.
    pub budget: ReadBudget,
    /// Concurrent input reads (16).
    pub concurrency: usize,
    /// SSRF guard for HTTP(S) and `hf://` URIs.
    pub guard: Arc<SsrfGuard>,
    /// URI-hash salt (`None` = empty salt).
    pub salt: Option<String>,
    /// Keep column names (in-process estimators see everything, SPEC §11;
    /// [`crate::redact_for_remote`] drops them for remote ones).
    pub column_names: bool,
    /// Keep string scalars.
    pub string_scalars: bool,
    /// Use the process-wide metadata cache.
    pub use_cache: bool,
    /// No I/O at all: inputs keep what their refs know, URIs become salted
    /// hashes (the K8s admission path, `manifest.extra.inputs_hash_only`).
    pub hash_only: bool,
}

impl Default for ReadOptions {
    fn default() -> Self {
        ReadOptions::new(ReadBudget::default())
    }
}

impl ReadOptions {
    /// Defaults with `budget`; the salt comes from `RIGHTSIZER_URI_SALT`.
    pub fn new(budget: ReadBudget) -> Self {
        ReadOptions {
            budget,
            concurrency: 16,
            guard: Arc::new(SsrfGuard::default()),
            salt: std::env::var(crate::privacy::DEFAULT_SALT_ENV).ok().filter(|s| !s.is_empty()),
            column_names: true,
            string_scalars: true,
            use_cache: true,
            hash_only: false,
        }
    }

    /// With an SSRF allowlist.
    pub fn with_guard(mut self, guard: SsrfGuard) -> Self {
        self.guard = Arc::new(guard);
        self
    }

    /// With a URI-hash salt.
    pub fn with_salt(mut self, salt: Option<String>) -> Self {
        self.salt = salt;
        self
    }
}

/// Read metadata for every input within `budget`, with default options
/// ([`ReadOptions::new`]). Deterministic: the same refs always yield the same
/// metadata, unless the time budget expires (SPEC §5.2). Errors leave an
/// input with what its ref already knew. Output order = `refs` order.
pub async fn read_inputs(refs: &[InputRef], resolver: &dyn StoreResolver, budget: ReadBudget) -> Vec<InputMeta> {
    read_inputs_with(refs, resolver, &ReadOptions::new(budget)).await
}

/// [`read_inputs`] with explicit options.
pub async fn read_inputs_with(refs: &[InputRef], resolver: &dyn StoreResolver, opts: &ReadOptions) -> Vec<InputMeta> {
    let deadline = tokio::time::Instant::now() + opts.budget.per_context;
    let slots = initial_slots(refs, opts);
    let idx: Vec<usize> = (0..refs.len()).collect();
    run_reads(refs, &idx, &slots, resolver, opts, Mode::Full, deadline).await;
    finish(slots, opts)
}

/// Read metadata for at most `max` inputs: when there are more refs, sizes
/// are read first (HEAD / listing) and only the `max` largest (then by name;
/// the context's truncation order, SPEC §5.2) get a full read. Returns the
/// `max` selected inputs, in that order. Used by [`crate::build_context`].
pub async fn read_inputs_truncated(refs: &[InputRef], resolver: &dyn StoreResolver, opts: &ReadOptions, max: usize) -> Vec<InputMeta> {
    let deadline = tokio::time::Instant::now() + opts.budget.per_context;
    let slots = initial_slots(refs, opts);
    let all: Vec<usize> = (0..refs.len()).collect();
    let selected: Vec<usize> = if refs.len() > max {
        run_reads(refs, &all, &slots, resolver, opts, Mode::SizeOnly, deadline).await;
        let mut order: Vec<(usize, Option<u64>, String)> =
            all.iter().map(|&i| (i, lock(&slots[i]).bytes, refs[i].name.clone())).collect();
        order.sort_by(|a, b| b.1.unwrap_or(0).cmp(&a.1.unwrap_or(0)).then_with(|| a.2.cmp(&b.2)));
        order.into_iter().take(max).map(|x| x.0).collect()
    } else {
        all
    };
    run_reads(refs, &selected, &slots, resolver, opts, Mode::Full, deadline).await;
    let mut out = finish(slots, opts);
    let keep: std::collections::HashSet<usize> = selected.iter().copied().collect();
    let mut i = 0;
    out.retain(|_| {
        let k = keep.contains(&i);
        i += 1;
        k
    });
    out
}

#[derive(Copy, Clone, PartialEq, Eq)]
enum Mode {
    SizeOnly,
    Full,
}

type Slot = Arc<Mutex<InputMeta>>;

fn lock(s: &Slot) -> std::sync::MutexGuard<'_, InputMeta> {
    s.lock().unwrap_or_else(|e| e.into_inner())
}

fn initial_slots(refs: &[InputRef], opts: &ReadOptions) -> Vec<Slot> {
    let salt = opts.salt.as_deref().unwrap_or("");
    crate::known_inputs(refs)
        .into_iter()
        .zip(refs)
        .map(|(mut m, r)| {
            m.uri_hash = r.uri.as_deref().map(|u| crate::privacy::uri_hash(u, salt));
            if r.scalar.is_some() && r.format.is_none() {
                m.format = InputFormat::Scalar;
            }
            Arc::new(Mutex::new(m))
        })
        .collect()
}

fn finish(slots: Vec<Slot>, opts: &ReadOptions) -> Vec<InputMeta> {
    slots
        .into_iter()
        .map(|s| {
            let mut m = lock(&s).clone();
            if !opts.column_names
                && let Some(c) = m.columns.as_mut()
            {
                c.names = None;
            }
            if !opts.string_scalars && matches!(m.scalar, Some(ScalarValue::Str(_))) {
                m.scalar = None;
            }
            m
        })
        .collect()
}

async fn run_reads(
    refs: &[InputRef],
    which: &[usize],
    slots: &[Slot],
    resolver: &dyn StoreResolver,
    opts: &ReadOptions,
    mode: Mode,
    deadline: tokio::time::Instant,
) {
    if opts.hash_only {
        return;
    }
    // Collected into a Vec first (not a lazy `filter_map` adapter): a lazy
    // iterator of `async move` blocks borrowing `&usize` defeats rustc's
    // higher-ranked `Send` inference, and the engine spawns decisions.
    let mut futs = Vec::with_capacity(which.len());
    for &i in which {
        let r = &refs[i];
        let Some(uri) = r.uri.as_deref() else { continue };
        if r.format == Some(InputFormat::Scalar) || (r.scalar.is_some() && r.format.is_none()) {
            continue;
        }
        let slot = Arc::clone(&slots[i]);
        futs.push(async move {
            if let Err(e) = read_one(r, uri, &slot, resolver, opts, mode).await {
                tracing::debug!(input = %r.name, error = %e, "input metadata unavailable");
            }
        });
    }
    let all = futures::stream::iter(futs).buffer_unordered(opts.concurrency.max(1)).collect::<Vec<()>>();
    if tokio::time::timeout_at(deadline, all).await.is_err() {
        tracing::debug!("feature time budget expired; unread inputs keep what is known");
    }
}

/// What a reader found.
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct Extracted {
    pub format: Option<InputFormat>,
    pub bytes: Option<u64>,
    pub rows: Option<u64>,
    pub columns: Option<ColumnsInfo>,
    pub shape: Option<Vec<u64>>,
    pub dtype: Option<String>,
    pub params: Option<u64>,
}

impl Extracted {
    fn apply(&self, m: &mut InputMeta) {
        if let Some(f) = self.format {
            m.format = f;
        }
        if self.bytes.is_some() {
            m.bytes = self.bytes;
        }
        m.rows = self.rows.or(m.rows);
        m.columns = self.columns.clone().or(m.columns.take());
        m.shape = self.shape.clone().or(m.shape.take());
        m.dtype = self.dtype.clone().or(m.dtype.take());
        m.params = self.params.or(m.params);
    }
}

/// Byte-budgeted range reads against one store (several paths for `hf://`).
pub(crate) struct Fetch<'a> {
    store: &'a dyn ObjectStore,
    budget: u64,
    remaining: u64,
}

impl<'a> Fetch<'a> {
    pub(crate) fn new(store: &'a dyn ObjectStore, budget: u64) -> Self {
        Fetch { store, budget, remaining: budget }
    }

    /// Bytes still allowed.
    pub(crate) fn remaining(&self) -> u64 {
        self.remaining
    }

    /// Read `range` of `path`, refusing (before any I/O) if it exceeds the
    /// remaining budget.
    pub(crate) async fn range(&mut self, path: &Path, range: Range<u64>) -> Result<Bytes, FeatureError> {
        let len = range.end.saturating_sub(range.start);
        if len > self.remaining {
            return Err(FeatureError::TooLarge(self.budget));
        }
        if len == 0 {
            return Ok(Bytes::new());
        }
        self.remaining -= len;
        // Stream the body and stop at `len`: a server may send a 206 whose
        // (chunked) body runs past its Content-Range, or a short one. Neither
        // is trusted: more bytes than asked are never buffered, and a body of
        // the wrong length is malformed (readers index into it).
        let opts = object_store::GetOptions::new().with_range(Some(range));
        let mut stream = self.store.get_opts(path, opts).await?.into_stream();
        let want = usize::try_from(len).map_err(|_| FeatureError::TooLarge(self.budget))?;
        let mut buf: Vec<u8> = Vec::with_capacity(want);
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            if chunk.len() > want - buf.len() {
                return Err(FeatureError::Malformed { format: "range", message: format!("body exceeds the {len} bytes asked") });
            }
            buf.extend_from_slice(&chunk);
        }
        if buf.len() != want {
            return Err(FeatureError::Malformed { format: "range", message: format!("short body: {} of {len} bytes", buf.len()) });
        }
        Ok(Bytes::from(buf))
    }

    /// HEAD (not counted: no body bytes).
    pub(crate) async fn head(&self, path: &Path) -> Result<object_store::ObjectMeta, FeatureError> {
        Ok(self.store.head(path).await?)
    }
}

async fn read_one(
    r: &InputRef,
    uri: &str,
    slot: &Slot,
    resolver: &dyn StoreResolver,
    opts: &ReadOptions,
    mode: Mode,
) -> Result<(), FeatureError> {
    {
        let m = lock(slot);
        if mode == Mode::Full && m.format == InputFormat::Dir && m.bytes.is_some() && m.shape.is_some() {
            return Ok(()); // listed in the size pass
        }
    }
    let is_hf = uri.starts_with("hf://");
    if let Some(u) = guard_url(uri)? {
        opts.guard.check_url(&u).await?;
    }
    let Some((store, path)) = resolver.resolve(uri) else {
        return Err(FeatureError::NotAllowed("no store for URI".into()));
    };
    let store = store.as_ref();
    let budget = opts.budget.per_input_bytes;

    let hint = r.format.or(is_hf.then_some(InputFormat::ModelRef));
    let ext = if is_hf { None } else { format_from_extension(uri) };
    let mut format = hint.or(ext.flatten());
    let trailing_slash = uri.ends_with('/');

    if format == Some(InputFormat::ModelRef) {
        let key = cache::key(uri, "hf");
        if mode == Mode::SizeOnly && lock(slot).bytes.is_some() {
            return Ok(());
        }
        if opts.use_cache
            && let Some(x) = cache::get(&key)
        {
            x.apply(&mut lock(slot));
            return Ok(());
        }
        let mut f = Fetch::new(store, budget);
        let x = hf::read(&mut f, &path, slot).await?;
        x.apply(&mut lock(slot));
        if opts.use_cache {
            cache::put(key, x);
        }
        return Ok(());
    }

    if format == Some(InputFormat::Dir) || trailing_slash {
        let x = list_dir(store, &path, opts.budget.max_dir_entries).await?;
        x.apply(&mut lock(slot));
        return Ok(());
    }

    let meta = match store.head(&path).await {
        Ok(m) => m,
        Err(e) => {
            if format.is_none()
                && let Ok(x) = list_dir(store, &path, opts.budget.max_dir_entries).await
            {
                x.apply(&mut lock(slot));
                return Ok(());
            }
            return Err(e.into());
        }
    };
    let size = meta.size;
    {
        let mut m = lock(slot);
        m.bytes = Some(size);
        if let Some(f) = format {
            m.format = f;
        }
    }
    if mode == Mode::SizeOnly {
        return Ok(());
    }
    let validator = meta.e_tag.clone().unwrap_or_else(|| format!("{}:{}", size, meta.last_modified.timestamp_millis()));
    let key = cache::key(uri, &validator);
    if opts.use_cache
        && let Some(x) = cache::get(&key)
    {
        x.apply(&mut lock(slot));
        return Ok(());
    }

    let mut f = Fetch::new(store, budget);
    if format.is_none() {
        if ext == Some(None) {
            // Known non-metadata extension (compressed): a blob.
            format = Some(InputFormat::Blob);
        } else {
            let head = f.range(&path, 0..size.min(16)).await?;
            format = Some(sniff(&head, size));
        }
    }
    let format = format.unwrap_or(InputFormat::Blob);
    let mut x = match format {
        InputFormat::Parquet => formats::parquet(&mut f, &path, size).await?,
        InputFormat::Npy => formats::npy(&mut f, &path, size).await?,
        InputFormat::Safetensors => formats::safetensors(&mut f, &path, size).await?,
        InputFormat::Csv => formats::csv(&mut f, &path, size, uri.to_ascii_lowercase().ends_with(".tsv")).await?,
        InputFormat::Jsonl => formats::jsonl(&mut f, &path, size).await?,
        InputFormat::ArrowIpc => arrow::read(&mut f, &path, size).await?,
        _ => Extracted::default(),
    };
    x.format = Some(format);
    x.bytes = Some(size);
    x.apply(&mut lock(slot));
    if opts.use_cache {
        cache::put(key, x);
    }
    Ok(())
}

/// The URL the SSRF guard must check for `uri`, if any.
fn guard_url(uri: &str) -> Result<Option<url::Url>, FeatureError> {
    if uri.starts_with("http://") || uri.starts_with("https://") {
        return url::Url::parse(uri).map(Some).map_err(|e| FeatureError::NotAllowed(format!("bad URL: {e}")));
    }
    if uri.starts_with("hf://") {
        let ep = crate::store::hf_endpoint();
        return url::Url::parse(&ep).map(Some).map_err(|e| FeatureError::NotAllowed(format!("bad HF_ENDPOINT: {e}")));
    }
    Ok(None)
}

/// Format from the URI's extension: `Some(Some(f))` = a metadata format,
/// `Some(None)` = a known opaque extension (blob, no sniffing), `None` =
/// unknown (sniff magic bytes).
pub fn format_from_extension(uri: &str) -> Option<Option<InputFormat>> {
    let no_query = uri.split(['?', '#']).next().unwrap_or(uri);
    let name = no_query.trim_end_matches('/').rsplit('/').next().unwrap_or(no_query).to_ascii_lowercase();
    let ext = name.rsplit_once('.')?.1.to_string();
    Some(match ext.as_str() {
        "parquet" | "pq" | "parq" => Some(InputFormat::Parquet),
        "csv" | "tsv" => Some(InputFormat::Csv),
        "jsonl" | "ndjson" => Some(InputFormat::Jsonl),
        "npy" => Some(InputFormat::Npy),
        "safetensors" => Some(InputFormat::Safetensors),
        "arrow" | "feather" | "ipc" | "arrows" => Some(InputFormat::ArrowIpc),
        "gz" | "zst" | "bz2" | "xz" | "zip" | "lz4" | "snappy" | "tar" | "tgz" => None,
        _ => return None,
    })
}

/// Format from the first bytes (≤ 16).
pub fn sniff(head: &[u8], size: u64) -> InputFormat {
    if head.starts_with(b"PAR1") {
        return InputFormat::Parquet;
    }
    if head.starts_with(b"\x93NUMPY") {
        return InputFormat::Npy;
    }
    if head.starts_with(b"ARROW1") || head.starts_with(&[0xff, 0xff, 0xff, 0xff]) {
        return InputFormat::ArrowIpc;
    }
    if head.len() >= 9 && head[8] == b'{' {
        let n = u64::from_le_bytes(head[0..8].try_into().unwrap_or([0; 8]));
        if n > 1 && n.saturating_add(8) <= size {
            return InputFormat::Safetensors;
        }
    }
    InputFormat::Blob
}

/// Recursive listing: entry count and total bytes, ≤ `max` entries.
async fn list_dir(store: &dyn ObjectStore, path: &Path, max: usize) -> Result<Extracted, FeatureError> {
    let prefix = if path.as_ref().is_empty() { None } else { Some(path) };
    let mut stream = store.list(prefix);
    let mut n: u64 = 0;
    let mut total: u64 = 0;
    let mut truncated = false;
    while let Some(item) = stream.next().await {
        let meta = item?;
        if n as usize >= max {
            truncated = true;
            break;
        }
        n += 1;
        total = total.saturating_add(meta.size);
    }
    if n == 0 && !truncated {
        return Err(FeatureError::Malformed { format: "dir", message: "empty or missing".into() });
    }
    Ok(Extracted {
        format: Some(InputFormat::Dir),
        bytes: (!truncated).then_some(total),
        shape: (!truncated).then(|| vec![n]),
        ..Extracted::default()
    })
}
