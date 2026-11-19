//! Property: rendering random decisions for every extracted unit of a
//! manifest never touches bytes outside the units' `resources` blocks (and
//! the KubeRay parameters that follow them); re-extracting the result sees
//! the S2 targets; and rendering again is a no-op.

mod common;

use common::*;
use proptest::prelude::*;
use rightsizer_core::{Bytes, MilliCpu};
use rightsizer_k8s::edit::{Seg, pointer};
use rightsizer_k8s::extract::{Target, locate};
use rightsizer_k8s::yaml::{self, YNode};
use rightsizer_k8s::{RenderRequest, objects};
use rightsizer_plugin::{Patch, TextEdit};

/// Byte ranges (whole lines) of every unit's `resources` entry and of the
/// KubeRay `num-cpus` / `memory` parameters.
fn resource_line_ranges(src: &str) -> Vec<(usize, usize)> {
    let docs = yaml::parse(src).unwrap();
    let mut out = Vec::new();
    let line_start = |i: usize| src[..i].rfind('\n').map(|p| p + 1).unwrap_or(0);
    for d in &docs {
        let Some(w) = objects::workload(&d.value) else { continue };
        let walk = |path: &[Seg]| -> Option<&YNode> {
            let mut n = &d.root;
            for s in path {
                n = match s {
                    Seg::Key(k) => n.get(k)?,
                    Seg::Idx(i) => n.item(*i)?,
                };
            }
            Some(n)
        };
        for loc in locate(&d.value, &w) {
            let rp = loc.resources_path(&w);
            let (last, parent) = rp.split_last().unwrap();
            let Some(pnode) = walk(parent) else { continue };
            let Seg::Key(k) = last else { continue };
            if let (Some(key), Some(val)) = (pnode.get_key(k), pnode.get(k)) {
                out.push((line_start(key.start), yaml::line_after(src, val.end.max(key.end))));
            }
            if let (Some(si), Target::Container(_)) = (loc.site, &loc.target)
                && let Some(rg) = &w.sites[si].ray
                && let Some(params) = walk(&rg.params_path)
            {
                for p in ["num-cpus", "memory"] {
                    if let (Some(key), Some(val)) = (params.get_key(p), params.get(p)) {
                        out.push((line_start(key.start), yaml::line_after(src, val.end)));
                    }
                }
            }
            let _ = pointer(&rp);
        }
    }
    out
}

fn without_ranges(src: &str, ranges: &[(usize, usize)]) -> String {
    let mut r = ranges.to_vec();
    r.sort();
    let mut out = String::new();
    let mut at = 0;
    for (s, e) in r {
        if s >= at {
            out.push_str(&src[at..s]);
            at = e;
        } else if e > at {
            at = e;
        }
    }
    out.push_str(&src[at..]);
    out
}

fn check_fixture(name: &str, cpus: &[u32], mems: &[u64]) -> Result<(), TestCaseError> {
    let a = adapter();
    let n = doc(name);
    let e = extract(&a, &n, &env());
    let decisions: Vec<_> = e.units.iter().enumerate().map(|(i, u)| decision(&u.id.0, Some(cpus[i % cpus.len()]), Some(mems[i % mems.len()]))).collect();
    let r = a.render_with(&n, &RenderRequest { annotate: false, ..RenderRequest::enforce(&decisions) }).unwrap();
    let before = text_of(&n);
    let after = text_of(&applied(&n, &r.patch));
    // 1. Every edit lies inside a resources block of the original (or is an
    //    insertion at a line boundary right after the container's last line).
    if let Patch::TextEdits { edits } = &r.patch {
        let ranges = resource_line_ranges(&before);
        for TextEdit { start, end, replacement } in edits {
            let inside = ranges.iter().any(|(s, e)| *start >= *s && *end <= *e);
            let insertion = start == end && (*start == 0 || before.as_bytes()[start - 1] == b'\n' || replacement.starts_with('\n') || before[..*start].ends_with('{') || before[*start..].starts_with('}'));
            prop_assert!(inside || insertion, "edit {start}..{end} {replacement:?} outside resources in {name}");
        }
    }
    // 2. With resource lines removed from both sides, the texts are identical.
    prop_assert_eq!(without_ranges(&before, &resource_line_ranges(&before)), without_ranges(&after, &resource_line_ranges(&after)), "{}", name);
    // 3. Re-extracting sees the applied (S2) requests.
    let e2 = extract(&a, &applied(&n, &r.patch), &env());
    for u in &e2.units {
        let want = &r.applied[&u.id];
        prop_assert_eq!(u.prior.spec.requests.memory, want.requests.memory.or(want.limits.memory), "{} {}", name, u.id);
        prop_assert_eq!(u.prior.spec.requests.cpu, want.requests.cpu.or(want.limits.cpu), "{} {}", name, u.id);
    }
    // 4. Idempotent.
    let again = a.render_with(&applied(&n, &r.patch), &RenderRequest { annotate: false, ..RenderRequest::enforce(&decisions) }).unwrap();
    prop_assert_eq!(again.patch, Patch::None, "{}", name);
    Ok(())
}

fn cpu() -> impl Strategy<Value = u32> {
    prop_oneof![1u32..64_000, (1u32..64).prop_map(|c| c * 1000)]
}

fn mem() -> impl Strategy<Value = u64> {
    prop_oneof![1u64..262_144, (1u64..256).prop_map(|g| g * 1024)]
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 48, .. ProptestConfig::default() })]

    #[test]
    fn render_touches_only_resource_bytes(cpus in prop::collection::vec(cpu(), 1..4), mems in prop::collection::vec(mem(), 1..4)) {
        for f in ["job.yaml", "job-no-resources.yaml", "pod.yaml", "cronjob.yaml", "jobset.yaml", "pytorchjob.yaml", "raycluster.yaml", "volcano-job.yaml", "multi.yaml", "pod-level.yaml", "trainjob.yaml"] {
            check_fixture(f, &cpus, &mems)?;
        }
    }
}

#[test]
fn quantities_round_trip_through_text() {
    // Spot check of the formatter on awkward values.
    for (c, m) in [(1u32, 1u64), (999, 1023), (1001, 1025), (63_999, 262_143)] {
        check_fixture("job.yaml", &[c], &[m]).unwrap();
    }
    let _ = (MilliCpu(1), Bytes(1));
}

#[test]
fn the_property_detects_non_resource_edits() {
    // Negative control: with annotations on, bytes outside resources change,
    // and the comparison used above notices.
    let a = adapter();
    let n = doc("job.yaml");
    let d = [decision("spec.template.spec.containers[build]", Some(2000), Some(4096))];
    let r = a.render_with(&n, &RenderRequest::enforce(&d)).unwrap();
    let before = text_of(&n);
    let after = text_of(&applied(&n, &r.patch));
    assert_ne!(without_ranges(&before, &resource_line_ranges(&before)), without_ranges(&after, &resource_line_ranges(&after)));
}
