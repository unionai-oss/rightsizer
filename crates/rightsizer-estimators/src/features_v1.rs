//! The `features-v1` vector the `gbt` estimator (and any trainer) computes
//! from a [`WorkloadContext`] (SPEC §9). Features are **named**; a model file
//! carries the names it was trained on and the evaluator maps by name, so a
//! model may use any subset. A feature that cannot be computed is missing
//! (NaN) and follows each split's missing-value direction.
//!
//! `log2b(x)` below is `log2(max(x, 1))` (the tuner's `math.log2(total)` with
//! `total ≥ 1`). Byte sums are over inputs with a known size.
//!
//! | name | value | missing when |
//! |---|---|---|
//! | `inputs.count` | number of inputs | never |
//! | `inputs.with_bytes` | inputs with a known size | never |
//! | `inputs.log2_bytes_total` | `log2b(Σ bytes)` | no input has a size |
//! | `inputs.log2_bytes_max` | `log2b(max bytes)` | no input has a size |
//! | `fmt.{format}.count` | inputs of the format (`parquet`, `csv`, `jsonl`, `npy`, `safetensors`, `arrow_ipc`, `dir`, `blob`, `scalar`, `model_ref`) | never |
//! | `fmt.{format}.log2_bytes` | `log2b(Σ bytes of the format)`; 0 when there are none | never |
//! | `inputs.log2_rows` | `log2b(Σ rows)` | no input has rows |
//! | `cols.{total,numeric,string,nested,other}` | column counts summed over inputs with a schema | no input has columns |
//! | `inputs.log2_params` | `log2b(Σ params)` | no input has a parameter count |
//! | `npy.log2_n`, `npy.log2_row`, `npy.itemsize` | first shaped input: `log2b(shape[0])`, `log2b(Π shape[1..])`, bytes per element of `dtype` | no shaped input (itemsize: unknown dtype) |
//! | `scalar.{name}` | numeric value of scalar input `name` (bools 0/1) | no such numeric scalar |
//! | `code.present` | 1 if code info is present | never |
//! | `code.n_tokens` | distinct code tokens | no code |
//! | `tok.{token}` | 1 if `token` is among the code tokens, else 0 — one per token in the **model's** vocabulary | no code |
//! | `code.lines`, `code.log2_bytes` | lines of / `log2b` bytes of the source | no source (privacy) |
//! | `manifest.log2_memory_backed_bytes` | `log2b(bytes)`; 0 when none | never |
//! | `manifest.sidecars`, `manifest.init_containers`, `manifest.distributed` | counts, 0/1 | never |
//! | `manifest.extra.{key}` | numeric adapter extras | absent |
//! | `prior.log2_memory_mib`, `prior.cpu_cores`, `prior.gpu_count` | the prior | not set |
//! | `hist.n`, `hist.n_success`, `hist.n_oom` | learnable records; successes; OOM-class | never |
//! | `hist.log2_peak_max_mib` | `log2b` of the max demand peak (container peak − overhead median), MiB | no peaks |
//! | `hist.log2_scaled_mib` | nearest-size run's demand × `b_now / b_hist` (linear, the tuner's `log_hist_scaled`) | no peaks |
//! | `hist.log2_size_ratio` | `log2(b_now / b_hist)` for that run | sizes unknown |
//! | `hist.peak_per_byte` | that run's demand bytes / its input bytes | sizes unknown |
//! | `hist.cpu_mean_max` | max mean cores | no CPU measurements |
//! | `hist.log2_duration_median` | `log2b` median success duration, s | no durations |

use std::collections::BTreeMap;

use rightsizer_core::stats::median;
use rightsizer_core::{InputFormat, RunStatus, WorkloadContext};

use crate::common::{MIB, cpu_mean_obs, learnable, memory_obs};
use crate::rules_sized::nearest_by_size;

/// Prefix of code-token features.
pub const TOKEN_PREFIX: &str = "tok.";

fn log2b(x: f64) -> f64 {
    x.max(1.0).log2()
}

fn format_name(f: InputFormat) -> &'static str {
    match f {
        InputFormat::Parquet => "parquet",
        InputFormat::Csv => "csv",
        InputFormat::Jsonl => "jsonl",
        InputFormat::Npy => "npy",
        InputFormat::Safetensors => "safetensors",
        InputFormat::ArrowIpc => "arrow_ipc",
        InputFormat::Dir => "dir",
        InputFormat::Blob => "blob",
        InputFormat::Scalar => "scalar",
        InputFormat::ModelRef => "model_ref",
    }
}

const ALL_FORMATS: [InputFormat; 10] = [
    InputFormat::Parquet,
    InputFormat::Csv,
    InputFormat::Jsonl,
    InputFormat::Npy,
    InputFormat::Safetensors,
    InputFormat::ArrowIpc,
    InputFormat::Dir,
    InputFormat::Blob,
    InputFormat::Scalar,
    InputFormat::ModelRef,
];

/// Bytes per element of a dtype string: numpy descriptors with a byte-order
/// prefix (`<f4`, `|u1`: the digit counts bytes), names (`float32`,
/// `bfloat16`), and safetensors / Rust spellings (`F32`, `BF16`, `u8`: the
/// digit counts bits).
pub fn itemsize(dtype: &str) -> Option<f64> {
    let t = dtype.trim();
    let numpy = t.starts_with(['<', '>', '|', '=']);
    let d = t.trim_start_matches(['<', '>', '|', '=']).to_ascii_lowercase();
    if numpy {
        let (kind, n) = d.split_at(d.len().min(1));
        return matches!(kind, "b" | "i" | "u" | "f" | "c" | "m").then(|| n.parse::<f64>().ok()).flatten();
    }
    Some(match d.as_str() {
        "bool" | "uint8" | "int8" | "u8" | "i8" | "f8_e4m3" | "f8_e5m2" | "float8_e4m3fn" | "float8_e5m2" => 1.0,
        "uint16" | "int16" | "float16" | "half" | "bfloat16" | "bf16" | "f16" | "u16" | "i16" => 2.0,
        "uint32" | "int32" | "float32" | "float" | "f32" | "u32" | "i32" => 4.0,
        "uint64" | "int64" | "float64" | "double" | "f64" | "u64" | "i64" | "complex64" => 8.0,
        "complex128" => 16.0,
        _ => return None,
    })
}

/// The `features-v1` map for `ctx`. `vocabulary` lists the model's feature
/// names; it decides which `tok.*` features are emitted (absent tokens are 0).
pub fn features_v1<'a>(ctx: &WorkloadContext, vocabulary: impl IntoIterator<Item = &'a str>) -> BTreeMap<String, f64> {
    let mut f: BTreeMap<String, f64> = BTreeMap::new();
    let mut put = |k: String, v: f64| {
        f.insert(k, v);
    };
    // Inputs.
    let sized: Vec<u64> = ctx.inputs.iter().filter_map(|i| i.bytes).collect();
    put("inputs.count".into(), ctx.inputs.len() as f64);
    put("inputs.with_bytes".into(), sized.len() as f64);
    if !sized.is_empty() {
        put("inputs.log2_bytes_total".into(), log2b(sized.iter().map(|&b| b as f64).sum()));
        put("inputs.log2_bytes_max".into(), log2b(sized.iter().copied().max().unwrap_or(0) as f64));
    }
    for fmt in ALL_FORMATS {
        let of: Vec<_> = ctx.inputs.iter().filter(|i| i.format == fmt).collect();
        let b: f64 = of.iter().filter_map(|i| i.bytes).map(|b| b as f64).sum();
        put(format!("fmt.{}.count", format_name(fmt)), of.len() as f64);
        put(format!("fmt.{}.log2_bytes", format_name(fmt)), if b > 0.0 { log2b(b) } else { 0.0 });
    }
    let rows: Vec<u64> = ctx.inputs.iter().filter_map(|i| i.rows).collect();
    if !rows.is_empty() {
        put("inputs.log2_rows".into(), log2b(rows.iter().map(|&r| r as f64).sum()));
    }
    let cols: Vec<_> = ctx.inputs.iter().filter_map(|i| i.columns.as_ref()).collect();
    if !cols.is_empty() {
        put("cols.total".into(), cols.iter().map(|c| c.total as f64).sum());
        put("cols.numeric".into(), cols.iter().map(|c| c.numeric as f64).sum());
        put("cols.string".into(), cols.iter().map(|c| c.string as f64).sum());
        put("cols.nested".into(), cols.iter().map(|c| c.nested as f64).sum());
        put("cols.other".into(), cols.iter().map(|c| c.other as f64).sum());
    }
    let params: Vec<u64> = ctx.inputs.iter().filter_map(|i| i.params).collect();
    if !params.is_empty() {
        put("inputs.log2_params".into(), log2b(params.iter().map(|&p| p as f64).sum()));
    }
    if let Some(a) = ctx.inputs.iter().find(|i| i.shape.as_ref().is_some_and(|s| !s.is_empty())) {
        let s = a.shape.as_deref().unwrap_or_default();
        put("npy.log2_n".into(), log2b(s[0] as f64));
        put("npy.log2_row".into(), log2b(s[1..].iter().map(|&d| d as f64).product()));
        if let Some(sz) = a.dtype.as_deref().and_then(itemsize) {
            put("npy.itemsize".into(), sz);
        }
    }
    for i in &ctx.inputs {
        if let Some(v) = i.scalar.as_ref().and_then(|s| s.as_f64()).filter(|v| v.is_finite()) {
            put(format!("scalar.{}", i.name), v);
        }
    }
    // Code.
    put("code.present".into(), if ctx.code.is_some() { 1.0 } else { 0.0 });
    if let Some(c) = &ctx.code {
        let toks: std::collections::BTreeSet<&str> = c.tokens.iter().map(String::as_str).collect();
        put("code.n_tokens".into(), toks.len() as f64);
        for name in vocabulary {
            if let Some(t) = name.strip_prefix(TOKEN_PREFIX) {
                put(name.to_string(), if toks.contains(t) { 1.0 } else { 0.0 });
            }
        }
        if let Some(src) = &c.source {
            put("code.lines".into(), src.lines().count() as f64);
            put("code.log2_bytes".into(), log2b(src.len() as f64));
        }
    }
    // Manifest.
    let m = &ctx.manifest;
    put("manifest.log2_memory_backed_bytes".into(), m.memory_backed_bytes.map(|b| log2b(b.0 as f64)).unwrap_or(0.0));
    put("manifest.sidecars".into(), m.sidecars as f64);
    put("manifest.init_containers".into(), m.init_containers as f64);
    put("manifest.distributed".into(), if m.distributed { 1.0 } else { 0.0 });
    for (k, v) in &m.extra {
        if let Some(x) = v.as_f64().filter(|x| x.is_finite()) {
            put(format!("manifest.extra.{k}"), x);
        }
    }
    // Prior.
    let r = &ctx.prior.spec.requests;
    if let Some(mem) = r.memory {
        put("prior.log2_memory_mib".into(), log2b(mem.0 as f64 / MIB));
    }
    if let Some(c) = r.cpu {
        put("prior.cpu_cores".into(), c.as_cores());
    }
    if let Some(g) = r.gpu {
        put("prior.gpu_count".into(), g.count as f64);
    }
    // History.
    let o = ctx.platform.overhead.median_bytes();
    let hist: Vec<_> = ctx.history.iter().filter(|r| learnable(r)).collect();
    put("hist.n".into(), hist.len() as f64);
    put("hist.n_success".into(), hist.iter().filter(|r| r.status == RunStatus::Succeeded).count() as f64);
    put("hist.n_oom".into(), hist.iter().filter(|r| r.status.is_oom_class()).count() as f64);
    let peaks: Vec<_> = hist.iter().copied().filter(|r| memory_obs(r, o).is_some()).collect();
    if !peaks.is_empty() {
        let mx = peaks.iter().filter_map(|r| memory_obs(r, o)).map(|ob| ob.value).fold(0.0, f64::max);
        put("hist.log2_peak_max_mib".into(), log2b(mx / MIB));
        let b_now = ctx.input_bytes();
        if let Some(n) = nearest_by_size(&peaks, b_now) {
            let d = memory_obs(n, o).map(|ob| ob.value).unwrap_or(0.0);
            match (b_now, n.input_bytes()) {
                (Some(now), Some(h)) => {
                    let (now, h) = (now.max(1) as f64, h.max(1) as f64);
                    put("hist.log2_scaled_mib".into(), log2b(d * now / h / MIB));
                    put("hist.log2_size_ratio".into(), (now / h).log2());
                    put("hist.peak_per_byte".into(), d / h);
                }
                _ => put("hist.log2_scaled_mib".into(), log2b(d / MIB)),
            }
        }
    }
    let cpu: Vec<f64> = hist.iter().filter_map(|r| cpu_mean_obs(r, 0.0)).map(|o| o.value).collect();
    if !cpu.is_empty() {
        put("hist.cpu_mean_max".into(), cpu.iter().copied().fold(0.0, f64::max));
    }
    let durs: Vec<f64> =
        hist.iter().filter(|r| r.status == RunStatus::Succeeded).filter_map(|r| r.duration_s).filter(|d| d.is_finite()).collect();
    if let Some(m) = median(&durs) {
        put("hist.log2_duration_median".into(), log2b(m));
    }
    f
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::*;
    use rightsizer_core::*;

    #[test]
    fn builds_named_features() {
        let mut pq = InputMeta::blob("train", 1 << 30);
        pq.format = InputFormat::Parquet;
        pq.rows = Some(1000);
        pq.columns = Some(ColumnsInfo { total: 5, numeric: 3, string: 2, ..Default::default() });
        let mut arr = InputMeta::blob("emb", 1 << 20);
        arr.format = InputFormat::Npy;
        arr.shape = Some(vec![1024, 16, 16]);
        arr.dtype = Some("<f4".into());
        let mut s = InputMeta::blob("epochs", 0);
        s.bytes = None;
        s.format = InputFormat::Scalar;
        s.scalar = Some(ScalarValue::Int(3));
        let mut ctx =
            ctx_with(vec![pq, arr, s], vec![record("a", RunStatus::Succeeded, Some(Measurement::exact(gib(2.0))), Some(1 << 29))]);
        ctx.code = Some(CodeInfo { digest: "d".into(), tokens: vec!["read_parquet".into()], source: Some("a\nb\n".into()) });
        let f = features_v1(&ctx, ["tok.read_parquet", "tok.groupby", "inputs.count"]);
        assert_eq!(f["inputs.count"], 3.0);
        assert!((f["inputs.log2_bytes_total"] - ((1u64 << 30) as f64 + (1u64 << 20) as f64).log2()).abs() < 1e-12);
        assert_eq!(f["fmt.parquet.log2_bytes"], 30.0);
        assert_eq!(f["fmt.csv.log2_bytes"], 0.0);
        assert_eq!(f["cols.numeric"], 3.0);
        assert_eq!(f["npy.log2_n"], 10.0);
        assert_eq!(f["npy.log2_row"], 8.0);
        assert_eq!(f["npy.itemsize"], 4.0);
        assert_eq!(f["scalar.epochs"], 3.0);
        assert_eq!(f["tok.read_parquet"], 1.0);
        assert_eq!(f["tok.groupby"], 0.0);
        assert_eq!(f["code.lines"], 2.0);
        assert_eq!(f["hist.log2_peak_max_mib"], 11.0);
        assert!((f["hist.log2_size_ratio"] - (((1u64 << 30) as f64 + (1u64 << 20) as f64).log2() - 29.0)).abs() < 1e-9);
        assert!(!f.contains_key("prior.cpu_cores"));
        // Without code, token features are missing, not zero.
        ctx.code = None;
        let f = features_v1(&ctx, ["tok.read_parquet"]);
        assert!(!f.contains_key("tok.read_parquet"));
        assert_eq!(f["code.present"], 0.0);
    }
}
