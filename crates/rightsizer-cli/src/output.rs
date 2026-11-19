//! Rendering: `text`, `json` or `yaml` (SPEC §12.1).

use std::fmt::Write as _;

use rightsizer_core::normalize::normalize;
use rightsizer_core::units::{format_k8s_bytes, format_k8s_cpu};
use rightsizer_core::{Decision, DecisionSource, Grid, Requests, RunRecord};
use rightsizer_engine::BreakerStatus;
use serde::Serialize;

use crate::Format;

fn plain<T: Serialize>(v: &T) -> String {
    match serde_json::to_value(v) {
        Ok(serde_json::Value::String(s)) => s,
        Ok(serde_json::Value::Object(m)) => {
            let kind = m.get("kind").and_then(|k| k.as_str()).unwrap_or("?").to_string();
            let rest: Vec<String> = m.iter().filter(|(k, _)| *k != "kind").map(|(k, v)| format!("{k}={}", v.to_string().trim_matches('"'))).collect();
            if rest.is_empty() { kind } else { format!("{kind}({})", rest.join(", ")) }
        }
        Ok(v) => v.to_string(),
        Err(_) => "?".into(),
    }
}

fn structured<T: Serialize + ?Sized>(v: &T, out: Format) -> Option<String> {
    match out {
        Format::Json => Some(serde_json::to_string_pretty(v).unwrap_or_default() + "\n"),
        Format::Yaml => Some(serde_norway::to_string(v).unwrap_or_default()),
        Format::Text => None,
    }
}

/// Print a value as JSON/YAML, or `text()` for text.
pub fn value(v: &serde_json::Value, out: Format, text: impl FnOnce() -> String) {
    match structured(v, out) {
        Some(s) => print!("{s}"),
        None => print!("{}", text()),
    }
}

/// A measured byte count, rounded: `2.7Gi`, `410Mi`.
fn human_bytes(b: f64) -> String {
    let gib = b / (1u64 << 30) as f64;
    if gib >= 1.0 { format!("{gib:.1}Gi") } else { format!("{:.0}Mi", b / (1u64 << 20) as f64) }
}

fn mem(r: &Requests) -> String {
    r.memory.map(|b| format_k8s_bytes(b.0)).unwrap_or_else(|| "-".into())
}

fn cpu(r: &Requests) -> String {
    r.cpu.map(format_k8s_cpu).unwrap_or_else(|| "-".into())
}

fn gpu(r: &Requests) -> String {
    r.gpu.map(|g| g.to_string()).unwrap_or_else(|| "-".into())
}

fn source(s: &DecisionSource) -> String {
    match s {
        DecisionSource::Estimator { id, version } => format!("estimator {id}@{version}"),
        DecisionSource::Memo { id, version } => format!("memo {id}@{version}"),
        DecisionSource::Prior { reason, detail } => {
            format!("prior ({}{})", plain(reason), detail.as_ref().map(|d| format!(": {d}")).unwrap_or_default())
        }
    }
}

fn decision_text(d: &Decision, explain: bool) -> String {
    let mut s = String::new();
    let _ = writeln!(s, "{} [{}]  mode {}  {}", d.key, d.unit.0, d.mode, source(&d.source));
    let _ = writeln!(s, "  decision {}", d.id);
    let (p, r, a) = (&d.prior.spec.requests, &d.recommended.requests, &d.applied.requests);
    let _ = writeln!(s, "  {:<8} {:>10} {:>14} {:>10}", "", "prior", "recommended", "applied");
    let _ = writeln!(s, "  {:<8} {:>10} {:>14} {:>10}", "memory", mem(p), mem(r), mem(a));
    let _ = writeln!(s, "  {:<8} {:>10} {:>14} {:>10}", "cpu", cpu(p), cpu(r), cpu(a));
    if p.gpu.is_some() || r.gpu.is_some() {
        let _ = writeln!(s, "  {:<8} {:>10} {:>14} {:>10}", "gpu", gpu(p), gpu(r), gpu(a));
    }
    if let Some(sh) = &d.shape {
        let _ = writeln!(s, "  shape {sh}");
    }
    if let (Some(c), Some(pf)) = (d.expected_cost, d.p_fail) {
        let _ = writeln!(s, "  expected ${c:.4} per run, p_fail {pf:.3}");
    }
    for h in &d.held {
        let _ = writeln!(s, "  held {}: {}{}", h.axis, plain(&h.reason), h.detail.as_ref().map(|x| format!(" ({x})")).unwrap_or_default());
    }
    for a in &d.advisories {
        let _ = writeln!(s, "  advisory {}", plain(a));
    }
    if explain {
        if let Some(e) = &d.estimate {
            let grid = Grid::by_id(&e.provenance.grid).unwrap_or_else(Grid::rs_grid_v1);
            if let Ok(n) = normalize(e, &grid, &[], 0.35) {
                let _ = writeln!(s, "  estimate {}@{} on {}:", e.provenance.estimator, e.provenance.version, e.provenance.grid);
                if let Some(m) = &n.memory {
                    let q = |p| format_k8s_bytes(m.quantile(p).max(0.0) as u64);
                    let _ = writeln!(s, "    memory demand  p10 {}  p50 {}  p90 {}  p99 {}", q(0.1), q(0.5), q(0.9), q(0.99));
                }
                if let Some(c) = &n.cpu_mean {
                    let _ = writeln!(s, "    cpu mean       p10 {:.2}  p50 {:.2}  p90 {:.2} cores", c.quantile(0.1), c.quantile(0.5), c.quantile(0.9));
                }
                if let Some(g) = &n.gpu_memory {
                    let q = |p| format_k8s_bytes(g.quantile(p).max(0.0) as u64);
                    let _ = writeln!(s, "    gpu memory     p50 {}  p90 {}", q(0.5), q(0.9));
                }
                let _ = writeln!(s, "    duration       p50 {:.0} s; t_peak_frac {:.2}", n.duration.median_s, n.t_peak_frac);
            }
        }
        let _ = writeln!(s, "  explanation:");
        for line in &d.explanation {
            let _ = writeln!(s, "    - {line}");
        }
        let _ = writeln!(s, "  context digest {}", d.context_digest);
    }
    s
}

/// Decisions.
pub fn decisions(ds: &[Decision], out: Format, explain: bool) {
    let single = ds.len() == 1;
    let v: serde_json::Value = if single { serde_json::to_value(&ds[0]) } else { serde_json::to_value(ds) }.unwrap_or_default();
    match structured(&v, out) {
        Some(s) => print!("{s}"),
        None => {
            for (i, d) in ds.iter().enumerate() {
                if i > 0 {
                    println!();
                }
                print!("{}", decision_text(d, explain));
            }
        }
    }
}

/// A key's history.
pub fn runs(rs: &[RunRecord], out: Format) {
    if let Some(s) = structured(rs, out) {
        print!("{s}");
        return;
    }
    println!("{:<22} {:<28} {:>3} {:<11} {:>9} {:>12} {:>8} {:<12} TRUST", "STARTED", "INVOCATION", "ATT", "STATUS", "REQ mem", "PEAK mem", "DUR", "SOURCE");
    for r in rs {
        let req = r.requested.as_ref().map(|s| mem(&s.requests)).unwrap_or_else(|| "-".into());
        let peak = r
            .memory_peak
            .map(|m| {
                let c = match m.censor {
                    rightsizer_core::Censor::Exact => "",
                    rightsizer_core::Censor::AtLeast => "≥",
                    rightsizer_core::Censor::AtMost => "≤",
                };
                format!("{c}{}", human_bytes(m.value.max(0.0)))
            })
            .unwrap_or_else(|| "-".into());
        let dur = r.duration_s.map(|d| format!("{d:.0}s")).unwrap_or_else(|| "-".into());
        println!(
            "{:<22} {:<28} {:>3} {:<11} {:>9} {:>12} {:>8} {:<12} {}",
            r.started_at.format("%Y-%m-%dT%H:%M:%SZ"),
            r.invocation_id,
            r.attempt,
            plain(&r.status),
            req,
            peak,
            dur,
            plain(&r.source),
            plain(&r.trust)
        );
    }
    if rs.is_empty() {
        println!("(no history)");
    }
}

/// Breakers.
pub fn breakers(bs: &[BreakerStatus], out: Format) {
    if let Some(s) = structured(bs, out) {
        print!("{s}");
        return;
    }
    if bs.is_empty() {
        println!("no breakers tripped");
        return;
    }
    println!("{:<10} {:>5}  {:<24} SCOPE", "STATE", "OPEN", "LAST TRIP");
    for b in bs {
        let last = b.last_trip.as_ref().map(|t| format!("{} {}", t.at.format("%Y-%m-%dT%H:%M"), plain(&t.reason))).unwrap_or_else(|| "-".into());
        println!("{:<10} {:>5}  {:<24} {}", plain(&b.state), b.open_trips, last, b.scope);
    }
}
