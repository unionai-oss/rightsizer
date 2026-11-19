//! Parquet snapshots and exports (SPEC §8.1 daily snapshots, §14.1
//! `rsz history export --parquet`).
//!
//! One flat schema for run records and decisions (`kind` = `run` |
//! `decision`), written with the low-level `parquet` column writer (no
//! `arrow` dependency), Snappy-compressed. The columns are the tuner's
//! corpus columns: inputs, requests, censored peaks, statuses,
//! propensities; `record_json` keeps the full artifact so snapshots can be
//! re-read and merged losslessly (compaction rewrites a day's file when new
//! rows for that day arrive).

use std::collections::HashSet;
use std::sync::Arc;

use bytes::Bytes;
use parquet::basic::Compression;
use parquet::data_type::{ByteArray, ByteArrayType, DoubleType, Int64Type};
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::writer::SerializedFileWriter;
use parquet::record::Field;
use parquet::schema::parser::parse_message_type;
use rightsizer_core::{Decision, DecisionSource, GpuShape, Measurement, Requests, RunRecord};
use serde::Serialize;

use crate::{dedup_key, ulid_time};

/// One snapshot / export row.
#[derive(Clone, Debug, PartialEq)]
pub enum SnapshotRow {
    /// A run record.
    Run(Box<RunRecord>),
    /// A decision.
    Decision(Box<Decision>),
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum Kind {
    Str,
    I64,
    F64,
    Ts,
}

/// The columns, in file order (SPEC §14.1).
pub const COLUMNS: &[&str] = &[
    "kind",
    "id",
    "key",
    "platform",
    "tenant",
    "at",
    "cluster",
    "invocation_id",
    "attempt",
    "decision_id",
    "code_digest",
    "context_digest",
    "inputs_json",
    "input_bytes",
    "req_cpu_m",
    "req_memory",
    "req_gpu",
    "lim_cpu_m",
    "lim_memory",
    "prior_cpu_m",
    "prior_memory",
    "prior_gpu",
    "status",
    "status_confidence",
    "memory_peak",
    "memory_peak_censor",
    "memory_peak_resolution_s",
    "cpu_mean",
    "cpu_mean_censor",
    "cpu_peak_window",
    "cpu_peak_window_censor",
    "gpu_memory_peak",
    "gpu_memory_peak_censor",
    "gpu_util_mean",
    "cpu_throttled_frac",
    "t_peak_frac",
    "duration_s",
    "source",
    "trust",
    "mode",
    "decision_source",
    "estimator_id",
    "estimator_version",
    "recommended_cpu_m",
    "recommended_memory",
    "recommended_gpu",
    "applied_cpu_m",
    "applied_memory",
    "applied_gpu",
    "shape",
    "held_json",
    "propensity",
    "expected_cost",
    "p_fail",
    "record_json",
];

fn kind_of(col: &str) -> Kind {
    match col {
        "at" => Kind::Ts,
        "attempt" | "input_bytes" | "req_cpu_m" | "req_memory" | "lim_cpu_m" | "lim_memory" | "prior_cpu_m" | "prior_memory"
        | "recommended_cpu_m" | "recommended_memory" | "applied_cpu_m" | "applied_memory" => Kind::I64,
        "memory_peak" | "memory_peak_resolution_s" | "cpu_mean" | "cpu_peak_window" | "gpu_memory_peak" | "gpu_util_mean"
        | "cpu_throttled_frac" | "t_peak_frac" | "duration_s" | "propensity" | "expected_cost" | "p_fail" => Kind::F64,
        _ => Kind::Str,
    }
}

/// The Parquet message type of [`COLUMNS`].
pub fn message_type() -> String {
    let mut s = String::from("message rightsizer_history {\n");
    for c in COLUMNS {
        let t = match kind_of(c) {
            Kind::Str => "BYTE_ARRAY",
            Kind::I64 => "INT64",
            Kind::F64 => "DOUBLE",
            Kind::Ts => "INT64",
        };
        let ann = match kind_of(c) {
            Kind::Str => " (UTF8)",
            Kind::Ts => " (TIMESTAMP(MILLIS,true))",
            _ => "",
        };
        s.push_str(&format!("  OPTIONAL {t} {c}{ann};\n"));
    }
    s.push('}');
    s
}

#[derive(Clone, Debug, PartialEq)]
enum Val {
    S(Option<String>),
    I(Option<i64>),
    F(Option<f64>),
}

fn json<T: Serialize>(v: &T) -> Option<String> {
    serde_json::to_string(v).ok()
}

fn enum_str<T: Serialize>(v: &T) -> Option<String> {
    match serde_json::to_value(v).ok()? {
        serde_json::Value::String(s) => Some(s),
        other => Some(other.to_string()),
    }
}

fn gpu(g: &Option<GpuShape>) -> Option<String> {
    let g = g.as_ref()?;
    let v = serde_json::to_value(g).ok()?;
    Some(format!("{}:{}", v.get("accel").and_then(|a| a.as_str()).unwrap_or("unknown"), g.count))
}

fn source_str(s: &rightsizer_core::OutcomeSource) -> Option<String> {
    let v = serde_json::to_value(s).ok()?;
    let kind = v.get("kind")?.as_str()?.to_string();
    Some(match v.get("platform").and_then(|p| p.as_str()) {
        Some(p) => format!("{kind}:{p}"),
        None => kind,
    })
}

struct Cells(Vec<(&'static str, Val)>);

impl Cells {
    fn s(&mut self, c: &'static str, v: Option<String>) {
        self.0.push((c, Val::S(v)));
    }
    fn i(&mut self, c: &'static str, v: Option<i64>) {
        self.0.push((c, Val::I(v)));
    }
    fn f(&mut self, c: &'static str, v: Option<f64>) {
        self.0.push((c, Val::F(v)));
    }
    fn measure(&mut self, c: &'static str, censor: &'static str, m: &Option<Measurement>) {
        self.f(c, m.map(|m| m.value));
        self.s(censor, m.as_ref().and_then(|m| enum_str(&m.censor)));
    }
    fn requests(&mut self, cpu: &'static str, mem: &'static str, gpu_col: Option<&'static str>, r: Option<&Requests>) {
        self.i(cpu, r.and_then(|r| r.cpu).map(|c| c.0 as i64));
        self.i(mem, r.and_then(|r| r.memory).map(|b| b.0 as i64));
        if let Some(g) = gpu_col {
            self.s(g, r.and_then(|r| gpu(&r.gpu)));
        }
    }
    fn into_row(self) -> Vec<Val> {
        COLUMNS
            .iter()
            .map(|c| {
                self.0.iter().find(|(n, _)| n == c).map(|(_, v)| v.clone()).unwrap_or(match kind_of(c) {
                    Kind::Str => Val::S(None),
                    Kind::I64 | Kind::Ts => Val::I(None),
                    Kind::F64 => Val::F(None),
                })
            })
            .collect()
    }
}

fn cells(row: &SnapshotRow) -> Vec<Val> {
    let mut c = Cells(Vec::new());
    match row {
        SnapshotRow::Run(r) => {
            c.s("kind", Some("run".into()));
            c.s("key", Some(r.key.to_string()));
            c.s("platform", Some(r.key.platform.clone()));
            c.s("tenant", Some(r.key.tenant.to_string()));
            c.i("at", Some(r.started_at.timestamp_millis()));
            c.s("cluster", r.cluster.clone());
            c.s("invocation_id", Some(r.invocation_id.clone()));
            c.i("attempt", Some(r.attempt as i64));
            c.s("decision_id", r.decision_id.map(|d| d.to_string()));
            c.s("code_digest", r.code_digest.clone());
            c.s("inputs_json", json(&r.inputs));
            c.i("input_bytes", r.input_bytes().map(|b| b as i64));
            c.requests("req_cpu_m", "req_memory", Some("req_gpu"), r.requested.as_ref().map(|s| &s.requests));
            c.i("lim_cpu_m", r.requested.as_ref().and_then(|s| s.limits.cpu).map(|x| x.0 as i64));
            c.i("lim_memory", r.requested.as_ref().and_then(|s| s.limits.memory).map(|x| x.0 as i64));
            c.requests("prior_cpu_m", "prior_memory", Some("prior_gpu"), r.origin_prior.as_ref());
            c.s("status", enum_str(&r.status));
            c.s("status_confidence", enum_str(&r.status_confidence));
            c.measure("memory_peak", "memory_peak_censor", &r.memory_peak);
            c.f("memory_peak_resolution_s", r.memory_peak.and_then(|m| m.resolution_s).map(f64::from));
            c.measure("cpu_mean", "cpu_mean_censor", &r.cpu_mean);
            c.measure("cpu_peak_window", "cpu_peak_window_censor", &r.cpu_peak_window);
            c.measure("gpu_memory_peak", "gpu_memory_peak_censor", &r.gpu_memory_peak);
            c.f("gpu_util_mean", r.gpu_util_mean.map(f64::from));
            c.f("cpu_throttled_frac", r.cpu_throttled_frac.map(f64::from));
            c.f("t_peak_frac", r.t_peak_frac.map(f64::from));
            c.f("duration_s", r.duration_s);
            c.s("source", source_str(&r.source));
            c.s("trust", enum_str(&r.trust));
            c.s("record_json", json(r));
        }
        SnapshotRow::Decision(d) => {
            c.s("kind", Some("decision".into()));
            c.s("id", Some(d.id.to_string()));
            c.s("key", Some(d.key.to_string()));
            c.s("platform", Some(d.key.platform.clone()));
            c.s("tenant", Some(d.key.tenant.to_string()));
            c.i("at", Some(ulid_time(d.id).timestamp_millis()));
            c.s("decision_id", Some(d.id.to_string()));
            c.s("context_digest", Some(d.context_digest.clone()).filter(|s| !s.is_empty()));
            c.requests("prior_cpu_m", "prior_memory", Some("prior_gpu"), Some(&d.prior.spec.requests));
            c.s("mode", Some(d.mode.as_str().to_string()));
            let (src, id, ver) = match &d.source {
                DecisionSource::Estimator { id, version } => ("estimator", Some(id.clone()), Some(version.clone())),
                DecisionSource::Memo { id, version } => ("memo", Some(id.clone()), Some(version.clone())),
                DecisionSource::Prior { reason, .. } => ("prior", enum_str(reason), None),
            };
            c.s("decision_source", Some(src.into()));
            c.s("estimator_id", id);
            c.s("estimator_version", ver);
            c.requests("recommended_cpu_m", "recommended_memory", Some("recommended_gpu"), Some(&d.recommended.requests));
            c.requests("applied_cpu_m", "applied_memory", Some("applied_gpu"), Some(&d.applied.requests));
            c.s("shape", d.shape.clone());
            c.s("held_json", json(&d.held));
            c.f("propensity", Some(f64::from(d.propensity)));
            c.f("expected_cost", d.expected_cost);
            c.f("p_fail", d.p_fail.map(f64::from));
            c.s("record_json", json(d));
        }
    }
    c.into_row()
}

/// Deduplicate: runs on `(key, invocation_id, attempt, source)`, decisions on id (first wins).
pub fn dedup_rows(rows: Vec<SnapshotRow>) -> Vec<SnapshotRow> {
    let mut runs = HashSet::new();
    let mut decs = HashSet::new();
    rows.into_iter()
        .filter(|r| match r {
            SnapshotRow::Run(r) => runs.insert(dedup_key(r)),
            SnapshotRow::Decision(d) => decs.insert(d.id),
        })
        .collect()
}

/// Write rows (deduplicated) as one Parquet file.
pub fn write_snapshot(rows: &[SnapshotRow]) -> Result<Vec<u8>, String> {
    let rows = dedup_rows(rows.to_vec());
    let schema = Arc::new(parse_message_type(&message_type()).map_err(|e| e.to_string())?);
    let props = Arc::new(WriterProperties::builder().set_compression(Compression::SNAPPY).build());
    let mut buf = Vec::new();
    {
        let mut w = SerializedFileWriter::new(&mut buf, schema, props).map_err(|e| e.to_string())?;
        let table: Vec<Vec<Val>> = rows.iter().map(cells).collect();
        let mut rg = w.next_row_group().map_err(|e| e.to_string())?;
        let mut col = 0usize;
        while let Some(mut cw) = rg.next_column().map_err(|e| e.to_string())? {
            let mut defs = Vec::with_capacity(table.len());
            match kind_of(COLUMNS[col]) {
                Kind::Str => {
                    let mut vals = Vec::new();
                    for r in &table {
                        match &r[col] {
                            Val::S(Some(s)) => {
                                defs.push(1);
                                vals.push(ByteArray::from(s.as_bytes().to_vec()));
                            }
                            _ => defs.push(0),
                        }
                    }
                    cw.typed::<ByteArrayType>().write_batch(&vals, Some(&defs), None).map_err(|e| e.to_string())?;
                }
                Kind::I64 | Kind::Ts => {
                    let mut vals = Vec::new();
                    for r in &table {
                        match &r[col] {
                            Val::I(Some(x)) => {
                                defs.push(1);
                                vals.push(*x);
                            }
                            _ => defs.push(0),
                        }
                    }
                    cw.typed::<Int64Type>().write_batch(&vals, Some(&defs), None).map_err(|e| e.to_string())?;
                }
                Kind::F64 => {
                    let mut vals = Vec::new();
                    for r in &table {
                        match &r[col] {
                            Val::F(Some(x)) => {
                                defs.push(1);
                                vals.push(*x);
                            }
                            _ => defs.push(0),
                        }
                    }
                    cw.typed::<DoubleType>().write_batch(&vals, Some(&defs), None).map_err(|e| e.to_string())?;
                }
            }
            cw.close().map_err(|e| e.to_string())?;
            col += 1;
        }
        rg.close().map_err(|e| e.to_string())?;
        w.close().map_err(|e| e.to_string())?;
    }
    Ok(buf)
}

/// Read a snapshot back into rows (from `kind` + `record_json`). Rows whose
/// JSON does not decode under the N/N-1 rule are skipped.
pub fn read_snapshot(data: &[u8]) -> Result<Vec<SnapshotRow>, String> {
    let reader = SerializedFileReader::new(Bytes::copy_from_slice(data)).map_err(|e| e.to_string())?;
    let mut out = Vec::new();
    for row in reader.get_row_iter(None).map_err(|e| e.to_string())? {
        let row = row.map_err(|e| e.to_string())?;
        let mut kind = None;
        let mut body = None;
        for (name, f) in row.get_column_iter() {
            match (name.as_str(), f) {
                ("kind", Field::Str(s)) => kind = Some(s.clone()),
                ("record_json", Field::Str(s)) => body = Some(s.clone()),
                _ => {}
            }
        }
        let (Some(kind), Some(body)) = (kind, body) else { continue };
        match kind.as_str() {
            "run" => {
                if let Ok(Some(r)) = crate::decode_versioned::<RunRecord>(body.as_bytes(), "snapshot") {
                    out.push(SnapshotRow::Run(Box::new(r)));
                }
            }
            "decision" => {
                if let Ok(Some(d)) = crate::decode_versioned::<Decision>(body.as_bytes(), "snapshot") {
                    out.push(SnapshotRow::Decision(Box::new(d)));
                }
            }
            _ => {}
        }
    }
    Ok(out)
}

/// Column names of a Parquet file (tests, `rsz history export` checks).
pub fn column_names(data: &[u8]) -> Result<Vec<String>, String> {
    let reader = SerializedFileReader::new(Bytes::copy_from_slice(data)).map_err(|e| e.to_string())?;
    let schema = reader.metadata().file_metadata().schema_descr();
    Ok(schema.columns().iter().map(|c| c.name().to_string()).collect())
}
