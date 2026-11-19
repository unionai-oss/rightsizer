//! Rendering replay reports (`rsz shadow`, `rsz audit`): the text table of
//! SPEC §12.2, and a single self-contained HTML page (no external assets).

use std::fmt::Write as _;

use rightsizer_core::units::format_k8s_bytes;

use crate::replay::{KeyReport, ReplayReport};

/// `41.2k`, `1.9M`, `310`.
pub fn human(x: f64) -> String {
    let a = x.abs();
    let s = if a >= 1e6 {
        format!("{:.1}M", a / 1e6)
    } else if a >= 1e3 {
        format!("{:.1}k", a / 1e3)
    } else if a >= 10.0 {
        format!("{a:.0}")
    } else {
        format!("{a:.1}")
    };
    if x < 0.0 { format!("-{s}") } else { s }
}

/// `$1.9k`, `-$0.04`.
pub fn money(x: f64) -> String {
    let a = x.abs();
    let s = if a < 10.0 { format!("${a:.2}") } else { format!("${}", human(a)) };
    if x < 0.0 && a >= 0.005 { format!("-{s}") } else { s }
}

fn mem(b: Option<u64>) -> String {
    b.map(format_k8s_bytes).unwrap_or_else(|| "-".into())
}

fn key_label(k: &KeyReport, strip_tenant: bool) -> String {
    if strip_tenant { k.key.suffix.clone() } else { k.key.to_string() }
}

fn would_save(k: &KeyReport) -> String {
    let s = &k.would_save;
    if k.prices_known || s.dollars != 0.0 {
        format!("{} GiB·h ({})", human(s.gib_h), money(s.dollars))
    } else {
        format!("{} GiB·h", human(s.gib_h))
    }
}

fn would_fail(k: &KeyReport) -> String {
    let q = if k.unknown > 0 { " (?)" } else { "" };
    format!("{} / {}{q}", k.would_fail, k.runs)
}

/// The text table (SPEC §12.2).
pub fn text(r: &ReplayReport) -> String {
    let tenants: std::collections::BTreeSet<String> = r.keys.iter().map(|k| format!("{}://{}", k.key.platform, k.key.tenant)).collect();
    let strip = tenants.len() == 1;
    let header = ["KEY", "RUNS", "PRIOR mem", "DECIDED mem (p50)", "WOULD SAVE", "WOULD FAIL", "READINESS"];
    let rows: Vec<[String; 7]> = r
        .keys
        .iter()
        .map(|k| {
            [
                key_label(k, strip),
                k.runs.to_string(),
                mem(k.prior_memory),
                mem(k.decided_memory_p50),
                would_save(k),
                would_fail(k),
                k.readiness.to_string(),
            ]
        })
        .collect();
    let mut w = header.map(|h| h.chars().count());
    for row in &rows {
        for (i, c) in row.iter().enumerate() {
            w[i] = w[i].max(c.chars().count());
        }
    }
    let mut out = String::new();
    if strip && let Some(t) = tenants.iter().next() {
        let _ = writeln!(out, "# tenant {t}");
    }
    let line = |cells: &[String]| {
        let mut l = String::new();
        for (i, c) in cells.iter().enumerate() {
            let pad = w[i].saturating_sub(c.chars().count());
            if i == 1 {
                let _ = write!(l, "{}{c}  ", " ".repeat(pad));
            } else {
                let _ = write!(l, "{c}{}  ", " ".repeat(pad));
            }
        }
        l.trim_end().to_string()
    };
    let _ = writeln!(out, "{}", line(&header.map(String::from)));
    for row in &rows {
        let _ = writeln!(out, "{}", line(row));
    }
    let t = &r.total;
    let _ = writeln!(
        out,
        "\ntotal: {} runs, would save {} GiB·h [{}, {}] ({} [{}, {}], 95% bootstrap), would fail {} ({} unknown)",
        r.runs,
        human(t.gib_h),
        human(t.gib_h_ci.lo),
        human(t.gib_h_ci.hi),
        money(t.dollars),
        money(t.dollars_ci.lo),
        money(t.dollars_ci.hi),
        r.would_fail,
        r.unknown
    );
    for k in &r.keys {
        if k.holds.is_empty() && k.advisories.is_empty() {
            continue;
        }
        let holds: Vec<String> = k.holds.iter().map(|(h, n)| format!("{h} ×{n}")).collect();
        let adv: Vec<String> = k.advisories.iter().map(|(a, n)| format!("{a} ×{n}")).collect();
        let _ = writeln!(out, "{}: holds [{}] advisories [{}]", key_label(k, strip), holds.join(", "), adv.join(", "));
    }
    out
}

fn esc(s: &str) -> String {
    s.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;").replace('"', "&quot;")
}

/// One self-contained HTML page: inline CSS, no scripts, no external assets.
pub fn html(r: &ReplayReport, title: &str) -> String {
    let mut rows = String::new();
    for k in &r.keys {
        let holds: Vec<String> = k.holds.iter().map(|(h, n)| format!("{} ×{n}", esc(h))).collect();
        let adv: Vec<String> = k.advisories.iter().map(|(a, n)| format!("{} ×{n}", esc(a))).collect();
        let s = &k.would_save;
        let _ = writeln!(
            rows,
            "<tr><td class=k>{}</td><td class=n>{}</td><td>{}</td><td>{}</td><td>{} GiB·h <small>[{}, {}]</small><br>{} <small>[{}, {}]</small></td><td>{}</td><td class=\"r {}\">{}</td><td><small>{}</small></td><td><small>{}</small></td></tr>",
            esc(&k.key.to_string()),
            k.runs,
            esc(&mem(k.prior_memory)),
            esc(&mem(k.decided_memory_p50)),
            human(s.gib_h),
            human(s.gib_h_ci.lo),
            human(s.gib_h_ci.hi),
            money(s.dollars),
            money(s.dollars_ci.lo),
            money(s.dollars_ci.hi),
            esc(&would_fail(k)),
            match k.readiness {
                crate::replay::Readiness::Ready => "ok",
                crate::replay::Readiness::LadderOnly => "mid",
                crate::replay::Readiness::NotReady { .. } => "no",
            },
            esc(&k.readiness.to_string()),
            holds.join("<br>"),
            adv.join("<br>"),
        );
    }
    let t = &r.total;
    format!(
        r#"<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title}</title>
<style>
:root {{ --bg:#fff; --fg:#1d1d1f; --muted:#6e6e73; --line:#e5e5ea; --ok:#1a7f37; --mid:#9a6700; --no:#cf222e; }}
@media (prefers-color-scheme: dark) {{ :root {{ --bg:#161618; --fg:#f2f2f7; --muted:#a1a1a6; --line:#2c2c2e; --ok:#3fb950; --mid:#d29922; --no:#f85149; }} }}
body {{ background:var(--bg); color:var(--fg); font:14px/1.45 system-ui,-apple-system,Segoe UI,sans-serif; margin:0; padding:24px 16px; }}
main {{ max-width:1200px; margin:0 auto; }}
h1 {{ font-size:20px; margin:0 0 4px; }} p.sub {{ color:var(--muted); margin:0 0 16px; }}
.tot {{ display:flex; flex-wrap:wrap; gap:24px; margin:0 0 16px; }} .tot div b {{ display:block; font-size:22px; }}
.wrap {{ overflow-x:auto; }} table {{ border-collapse:collapse; width:100%; }}
th,td {{ text-align:left; padding:6px 8px; border-bottom:1px solid var(--line); vertical-align:top; }}
th {{ color:var(--muted); font-weight:600; font-size:12px; text-transform:uppercase; }}
td.n {{ text-align:right; font-variant-numeric:tabular-nums; }} td.k {{ font-family:ui-monospace,Menlo,monospace; font-size:12px; word-break:break-all; }}
small {{ color:var(--muted); }} .ok {{ color:var(--ok); }} .mid {{ color:var(--mid); }} .no {{ color:var(--no); }}
</style></head><body><main>
<h1>{title}</h1>
<p class=sub>Counterfactual replay: each run decided as of its start from runs completed before it (recommend-mode steady state). Intervals are 95% bootstrap over runs.</p>
<div class=tot><div><b>{runs}</b>runs</div><div><b>{gib} GiB·h</b>would save [{glo}, {ghi}]</div><div><b>{usd}</b>would save [{dlo}, {dhi}]</div><div><b>{fail}</b>would fail ({unk} unknown)</div></div>
<div class=wrap><table><thead><tr><th>key</th><th>runs</th><th>prior mem</th><th>decided mem (p50)</th><th>would save</th><th>would fail</th><th>readiness</th><th>holds</th><th>advisories</th></tr></thead>
<tbody>
{rows}</tbody></table></div>
</main></body></html>
"#,
        title = esc(title),
        runs = r.runs,
        gib = human(t.gib_h),
        glo = human(t.gib_h_ci.lo),
        ghi = human(t.gib_h_ci.hi),
        usd = money(t.dollars),
        dlo = money(t.dollars_ci.lo),
        dhi = money(t.dollars_ci.hi),
        fail = r.would_fail,
        unk = r.unknown,
    )
}
