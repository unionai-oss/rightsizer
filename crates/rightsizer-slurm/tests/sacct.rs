//! sacct importer on output captured from the Docker Slurm cluster
//! (jobs 27–35: completed, OOM, multi-step, two-node, failed, timeout,
//! cancelled-before-start, array, het). NODE_FAIL / PREEMPTED rows are
//! synthesized from captured rows by editing the state.

mod common;

use std::sync::Arc;

use common::*;
use futures::StreamExt;
use rightsizer_core::{Censor, RunStatus, Trust, WorkloadKey};
use rightsizer_plugin::OutcomeSource;
use rightsizer_slurm::runner::{CmdOutput, CommandRunner};
use rightsizer_slurm::sacct::*;
use ulid::Ulid;

fn cfg() -> ImportConfig {
    ImportConfig { cluster: "rsz-it".into(), resolution_s: 5.0, ..Default::default() }
}

fn by_inv(recs: &[rightsizer_core::RunRecord], id: u64) -> &rightsizer_core::RunRecord {
    recs.iter().find(|r| r.invocation_id == format!("rsz-it:{id}")).unwrap_or_else(|| panic!("job {id}"))
}

#[test]
fn json_with_scripts() {
    let with = parse_sacct_json(&fixture("sacct/jobs-with-script.json")).unwrap();
    assert!(with.iter().all(|j| j.steps.is_empty() && j.script.is_some()), "26.05: -B drops steps");
    let mut jobs = parse_sacct_json(&fixture("sacct/jobs.json")).unwrap();
    assert!(jobs.iter().all(|j| j.script.is_none()));
    merge_scripts(&mut jobs, &with);
    assert_eq!(jobs.len(), 11);
    let recs = to_run_records(&jobs, &cfg());
    assert_eq!(recs.len(), 10, "job 33 was cancelled before it started");
    let ok = by_inv(&recs, 27);
    assert_eq!(ok.key.to_string(), "slurm://rsz-it/mlteam/ok");
    assert_eq!(ok.status, RunStatus::Succeeded);
    assert_eq!(ok.decision_id, Some(Ulid::from_string("01JDZ7Q4W8M7C9B5V3N2X1Y0ZA").unwrap()));
    let m = ok.memory_peak.unwrap();
    assert_eq!((m.value, m.censor, m.resolution_s), (54432.0 * 1024.0, Censor::Exact, Some(5.0)));
    assert_eq!(ok.requested.as_ref().unwrap().requests.memory, Some(rightsizer_core::Bytes::mib(200)));
    assert_eq!(ok.trust, Trust::Verified);
    assert_eq!(ok.duration_s, Some(13.0));
    assert_eq!(ok.attempt, 1);
    assert!(ok.code_digest.as_deref().unwrap().starts_with("sha256:"));

    let oom = by_inv(&recs, 28);
    assert_eq!(oom.status, RunStatus::OomKilled);
    let m = oom.memory_peak.unwrap();
    assert_eq!((m.value, m.censor), (100.0 * 1048576.0, Censor::AtLeast), "AtLeast(max(limit, peak))");
    assert!(oom.key.suffix.starts_with("sha256-"), "no name or key directive: script digest");

    let ms = by_inv(&recs, 29);
    assert_eq!(ms.key.suffix, "train-small", "#RIGHTSIZER key=");
    let m = ms.memory_peak.unwrap();
    assert_eq!((m.value, m.censor), (128712.0 * 1024.0, Censor::Exact), "step 29.0: TRESUsageInTot / 1 node");

    let two = by_inv(&recs, 30);
    assert_eq!(two.key.tenant.as_str(), "rsz-it/vision");
    assert_eq!(two.memory_peak.unwrap().value, 173817856.0 / 2.0, "Tot / NNodes");
    assert_eq!(two.requested.as_ref().unwrap().requests.memory, Some(rightsizer_core::Bytes::mib(300)));
    assert_eq!(two.requested.as_ref().unwrap().requests.cpu, Some(rightsizer_core::MilliCpu::cores(1)));

    assert_eq!(by_inv(&recs, 31).status, RunStatus::Failed);
    assert_eq!(by_inv(&recs, 32).status, RunStatus::Timeout);

    let h0 = by_inv(&recs, 35);
    let h1 = by_inv(&recs, 36);
    assert!(h0.key.suffix.ends_with("/het-0") && h1.key.suffix.ends_with("/het-1"));
    assert_eq!(h0.key.suffix.trim_end_matches("/het-0"), h1.key.suffix.trim_end_matches("/het-1"));
    assert_eq!(h1.decision_id, Some(Ulid::from_string("01JDZ7Q4W8M7C9B5V3N2X1Y0ZC").unwrap()));
    assert_eq!(h1.requested.as_ref().unwrap().requests.memory, Some(rightsizer_core::Bytes::mib(200)));

    let a0 = by_inv(&recs, 37);
    let a1 = by_inv(&recs, 34);
    assert_eq!(a0.key, a1.key, "array tasks share a key");
    assert_eq!(a0.key.suffix, "arr");
    assert_eq!(a0.requested.as_ref().unwrap().requests.memory, Some(rightsizer_core::Bytes::mib(128)), "--mem-per-cpu × 1 CPU");
}

#[test]
fn keys_match_submit_time_derivation() {
    // The importer re-derives the key from the stored script and submit line
    // exactly as the adapter does at submit time.
    let mut jobs = parse_sacct_json(&fixture("sacct/jobs.json")).unwrap();
    merge_scripts(&mut jobs, &parse_sacct_json(&fixture("sacct/jobs-with-script.json")).unwrap());
    let j = jobs.iter().find(|j| j.job_id == 28).unwrap();
    let submit = rightsizer_slurm::sbatch::parse(&["oom.sh".into()], j.script.as_deref(), &Default::default()).unwrap();
    let want = rightsizer_slurm::keys::workload_key("rsz-it", "mlteam", &submit, 0).unwrap();
    let rec = to_run_record(j, &cfg()).unwrap();
    assert_eq!(rec.key, want);
    // --wrap: the submit line loses quoting, the stored script does not.
    let j = jobs.iter().find(|j| j.job_id == 31).unwrap();
    let submit = rightsizer_slurm::sbatch::parse(&shlex::split("--parsable -J fail --mem=100M --wrap 'exit 3'").unwrap(), None, &Default::default()).unwrap();
    assert_eq!(to_run_record(j, &cfg()).unwrap().key, rightsizer_slurm::keys::workload_key("rsz-it", "mlteam", &submit, 0).unwrap());
    let j = jobs.iter().find(|j| j.job_id == 28).unwrap();
    let mut no_name = j.clone();
    no_name.script = None;
    assert_eq!(to_run_record(&no_name, &cfg()).unwrap().key.suffix, "oom.sh", "without the script: the job name");
}

#[test]
fn resolver_maps_decision_ids_to_keys() {
    let k = WorkloadKey::parse("slurm://rsz-it/mlteam/from-decision-log").unwrap();
    let k2 = k.clone();
    let mut c = cfg();
    c.resolver = Some(Arc::new(move |id: &Ulid| (id.to_string() == "01JDZ7Q4W8M7C9B5V3N2X1Y0ZA").then(|| k2.clone())));
    let jobs = parse_sacct_json(&fixture("sacct/jobs.json")).unwrap();
    let recs = to_run_records(&jobs, &c);
    assert_eq!(by_inv(&recs, 27).key, k);
    assert_ne!(by_inv(&recs, 28).key, k);
}

#[test]
fn parsable2_fallback_matches_json() {
    let text = parse_sacct_parsable2(&fixture("sacct/jobs.parsable2.txt")).unwrap();
    let json = parse_sacct_json(&fixture("sacct/jobs.json")).unwrap();
    let (rt, rj) = (to_run_records(&text, &cfg()), to_run_records(&json, &cfg()));
    assert_eq!(rt.len(), rj.len());
    for r in &rj {
        let t = rt.iter().find(|x| x.invocation_id == r.invocation_id).unwrap_or_else(|| panic!("{}", r.invocation_id));
        assert_eq!(t.status, r.status, "{}", r.invocation_id);
        assert_eq!(t.key, r.key, "{}", r.invocation_id);
        assert_eq!(t.decision_id, r.decision_id);
        assert_eq!(t.requested.as_ref().unwrap().requests.memory, r.requested.as_ref().unwrap().requests.memory, "{}", r.invocation_id);
        let pk = |m: Option<rightsizer_core::Measurement>| m.map(|m| (m.value, m.censor));
        assert_eq!(pk(t.memory_peak), pk(r.memory_peak), "{}", r.invocation_id);
        // Het component 1 ran no step of its own: no peak.
        assert_eq!(r.memory_peak.is_none(), r.invocation_id == "rsz-it:36", "{}", r.invocation_id);
        assert_eq!(t.duration_s, r.duration_s);
    }
}

#[test]
fn spec_format_text_without_end_or_cluster() {
    // The --format of slurm.md §5 (no Cluster/End/TotalCPU columns).
    let jobs = parse_sacct_parsable2(&fixture("sacct/jobs.spec-format.parsable2.txt")).unwrap();
    let recs = to_run_records(&jobs, &cfg());
    assert_eq!(recs.len(), 10);
    assert_eq!(by_inv(&recs, 28).status, RunStatus::OomKilled);
    assert_eq!(by_inv(&recs, 30).memory_peak.unwrap().value, 169744.0 * 1024.0 / 2.0);
}

#[test]
fn statuses_including_synthesized() {
    let text = fixture("sacct/jobs.parsable2.txt");
    let mut lines: Vec<String> = text.lines().map(str::to_string).collect();
    for l in lines.iter_mut() {
        if l.starts_with("31|") {
            *l = l.replace("|FAILED|", "|NODE_FAIL|");
        }
        if l.starts_with("32|") {
            *l = l.replace("|TIMEOUT|", "|PREEMPTED|");
        }
    }
    let jobs = parse_sacct_parsable2(&lines.join("\n")).unwrap();
    let recs = to_run_records(&jobs, &cfg());
    assert_eq!(by_inv(&recs, 31).status, RunStatus::Failed, "NODE_FAIL: not attributed");
    assert_eq!(by_inv(&recs, 32).status, RunStatus::Preempted);
    // MaxRSS × tasks-per-node when TRESUsageInTot is absent: AtLeast.
    let mut j = jobs.iter().find(|j| j.job_id == 29).unwrap().clone();
    for s in &mut j.steps {
        s.tot_mem = None;
    }
    let (v, c) = memory_demand(&j).unwrap();
    assert_eq!((v, c), (64364.0 * 1024.0 * 2.0, Censor::AtLeast));
    // A success within 5% of its limit is AtLeast.
    let mut j = jobs.iter().find(|j| j.job_id == 27).unwrap().clone();
    j.mem_per_node_mib = Some(54);
    assert_eq!(to_run_record(&j, &cfg()).unwrap().memory_peak.unwrap().censor, Censor::AtLeast);
}

struct FakeSacct {
    json_ok: bool,
}

#[async_trait::async_trait]
impl CommandRunner for FakeSacct {
    async fn run_env(&self, _p: &std::path::Path, args: &[String], _e: &[(String, String)]) -> std::io::Result<CmdOutput> {
        assert!(args.contains(&"--allusers".to_string()) && args.contains(&"-S".to_string()));
        if args.contains(&"--json".to_string()) {
            let f = if args.contains(&"-B".to_string()) { "sacct/jobs-with-script.json" } else { "sacct/jobs.json" };
            return Ok(if self.json_ok {
                CmdOutput { success: true, code: Some(0), stdout: fixture(f), stderr: String::new() }
            } else {
                CmdOutput { success: false, code: Some(1), stdout: String::new(), stderr: "sacct: error: JSON plugin missing".into() }
            });
        }
        assert!(args.iter().any(|a| a.starts_with("--format=JobID,JobIDRaw")));
        Ok(CmdOutput { success: true, code: Some(0), stdout: fixture("sacct/jobs.parsable2.txt"), stderr: String::new() })
    }
}

#[tokio::test]
async fn outcome_source_json_and_text_fallback() {
    for json_ok in [true, false] {
        let mut s = SacctSource::new(cfg());
        s.runner = Arc::new(FakeSacct { json_ok });
        let since = chrono::Utc::now() - chrono::Duration::days(1);
        let all: Vec<_> = s.fetch(since, None).await.unwrap().collect().await;
        assert_eq!(all.len(), 10, "json_ok={json_ok}");
        let k = WorkloadKey::parse("slurm://rsz-it/mlteam/ok").unwrap();
        let only: Vec<_> = s.fetch(since, Some(std::slice::from_ref(&k))).await.unwrap().collect().await;
        assert_eq!(only.len(), 1);
    }
}

#[test]
fn ladder_attempts_are_linked_only_with_evidence() {
    let mut jobs = parse_sacct_json(&fixture("sacct/jobs.json")).unwrap();
    let id = Ulid::from_string("01JDZ7Q4W8M7C9B5V3N2X1Y0ZA").unwrap();
    // Make job 28 (OOM) and 27 (completed) two verified attempts of one decision.
    for j in jobs.iter_mut().filter(|j| j.job_id == 27 || j.job_id == 28) {
        j.admin_comment = format!("rightsizer:{id}:verified");
    }
    let mut recs = to_run_records(&jobs, &cfg());
    assert_eq!(link_ladders(&jobs, &mut recs, &cfg(), |_| false), 0);
    assert!(recs.iter().any(|r| r.invocation_id == "rsz-it:27"));
    let n = link_ladders(&jobs, &mut recs, &cfg(), |d| *d == id);
    assert_eq!(n, 2);
    let linked: Vec<_> = recs.iter().filter(|r| r.invocation_id == ladder_invocation(&id)).map(|r| (r.attempt, r.status)).collect();
    assert_eq!(linked, vec![(1, RunStatus::Succeeded), (2, RunStatus::OomKilled)]);
}
