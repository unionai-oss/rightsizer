//! Parity of the Rust `rightsizer-trees/1` / `rightsizer-novelty/1`
//! evaluators with sklearn and LightGBM (SPEC §6.2: ≤ 1e-6 relative).
//!
//! Fixtures (`tests/fixtures/*.json`) are produced by Python from real
//! models, with the libraries' own predictions. Regenerate with:
//!
//! ```text
//! .venv/bin/python python/rightsizer/tests/export/make_fixtures.py
//! ```
//!
//! `RIGHTSIZER_TREES_FIXTURES=<dir>` points this test at another fixture
//! directory (the Python test suite does that for fresh exports).

use std::collections::BTreeMap;
use std::path::PathBuf;

use rightsizer_estimators::EstimateAxis;
use rightsizer_estimators::novelty::NoveltyModel;
use rightsizer_estimators::trees::{TreeModel, axis_by_name};
use serde::Deserialize;

fn dir() -> PathBuf {
    std::env::var_os("RIGHTSIZER_TREES_FIXTURES")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures"))
}

fn read(name: &str) -> String {
    std::fs::read_to_string(dir().join(name)).unwrap_or_else(|e| panic!("{name}: {e}"))
}

#[derive(Deserialize)]
struct Cases {
    feature_names: Vec<String>,
    rows: Vec<Vec<Option<f64>>>,
    /// lib → axis → quantile (string) → raw score per row.
    expected_raw: BTreeMap<String, BTreeMap<String, BTreeMap<String, Vec<f64>>>>,
}

fn row(r: &[Option<f64>]) -> Vec<f64> {
    r.iter().map(|v| v.unwrap_or(f64::NAN)).collect()
}

fn rel(a: f64, b: f64) -> f64 {
    (a - b).abs() / b.abs().max(1e-12)
}

#[test]
fn trees_match_sklearn_and_lightgbm() {
    let cases: Cases = serde_json::from_str(&read("trees_cases.json")).unwrap();
    let mut checked = 0;
    for lib in ["sklearn", "lightgbm"] {
        let m = TreeModel::from_json(&read(&format!("trees_{lib}.json"))).unwrap();
        assert_eq!(m.feature_names, cases.feature_names);
        assert_eq!(m.features, "features-v1");
        for (axis_name, by_q) in &cases.expected_raw[lib] {
            let axis = axis_by_name(axis_name).unwrap();
            let am = m.axis(axis).unwrap();
            for (q, want) in by_q {
                let q: f64 = q.parse().unwrap();
                let ens = am.quantiles.iter().find(|e| (e.q - q).abs() < 1e-12).unwrap();
                for (r, w) in cases.rows.iter().zip(want) {
                    let x = row(r);
                    let got = ens.raw(&x);
                    assert!(rel(got, *w) <= 1e-6, "{lib} {axis_name} q={q}: {got} vs {w} (row {r:?})");
                    // The natural value is the inverse transform of the same raw score.
                    let nat = am.predict(&x).into_iter().find(|(l, _)| (*l - q).abs() < 1e-12).unwrap().1;
                    let want_nat = am.target.value(*w);
                    assert!(rel(nat, want_nat) <= 1e-6, "{lib} {axis_name} natural");
                    checked += 1;
                }
            }
        }
    }
    assert!(checked >= 2 * 4 * 10, "checked {checked}");
    // Units: memory in bytes from log2 MiB.
    let m = TreeModel::from_json(&read("trees_sklearn.json")).unwrap();
    assert_eq!(m.axis(EstimateAxis::Memory).unwrap().target.scale, 1048576.0);
}

#[derive(Deserialize)]
struct NoveltyCases {
    rows: Vec<Vec<Option<f64>>>,
    distance: Vec<f64>,
    ecdf: Vec<f64>,
}

#[test]
fn novelty_matches_numpy() {
    let n = NoveltyModel::from_json(&read("novelty.json")).unwrap();
    let c: NoveltyCases = serde_json::from_str(&read("novelty_cases.json")).unwrap();
    assert!(n.needs_code);
    for ((r, d), e) in c.rows.iter().zip(&c.distance).zip(&c.ecdf) {
        let got = n.mean_knn_distance(&row(r));
        assert!(rel(got, *d) <= 1e-6, "{got} vs {d}");
        assert_eq!(n.ecdf(got) as f64, *e as f32 as f64);
    }
}
