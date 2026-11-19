//! Inventory parsers against output captured on the Docker Slurm cluster.

mod common;

use common::*;
use rightsizer_core::{AcceleratorId, Bytes, MilliCpu, Tenant};
use rightsizer_plugin::{PlatformModel, Unit};
use rightsizer_slurm::SlurmPlatform;
use rightsizer_slurm::inventory::*;

#[test]
fn scontrol_text_and_json_agree() {
    let text = parse_show_node(&fixture("scontrol/show-node.txt"));
    let json = parse_nodes_json(&fixture("scontrol/show-node.json")).unwrap();
    assert_eq!(text.len(), 2);
    for (t, j) in text.iter().zip(&json) {
        assert_eq!((t.name.as_str(), t.cpus, t.real_memory_mib), (j.name.as_str(), j.cpus, j.real_memory_mib));
        assert_eq!(t.partitions, j.partitions);
        assert!(t.gres.is_empty() && j.gres.is_empty());
        assert!(t.usable() && j.usable(), "{:?}", j.state);
    }
    assert_eq!(text[0].cpus, 2);
    assert_eq!(text[0].real_memory_mib, 2048);
    let sinfo = parse_nodes_json(&fixture("sinfo/sinfo.json")).unwrap();
    assert_eq!(sinfo.iter().map(|n| n.name.as_str()).collect::<Vec<_>>(), vec!["c1", "c2"]);
    assert_eq!(sinfo[0].real_memory_mib, 2048);
    let st = parse_sinfo_nodes(&fixture("sinfo/sinfo-N.txt"));
    assert_eq!(st[1].name, "c2");
    assert_eq!(st[1].partitions, vec!["normal"]);
}

#[test]
fn partitions() {
    let t = parse_show_partition(&fixture("scontrol/show-partition.txt"));
    let j = parse_partitions_json(&fixture("scontrol/show-partition.json")).unwrap();
    assert_eq!(t.len(), 2);
    assert_eq!(t[0].name, "normal");
    assert!(t[0].is_default);
    assert_eq!(t[0].nodes, vec!["c1", "c2"]);
    assert_eq!(t[0].max_mem_per_node, None);
    assert_eq!(t[1].max_mem_per_node, Some(32000));
    assert_eq!(t[0].max_time_s, None);
    let gj = j.iter().find(|p| p.name == "gpu").unwrap();
    assert_eq!(gj.max_mem_per_node, Some(32000));
    assert_eq!(gj.nodes, vec!["g1"]);
    assert_eq!(j.iter().find(|p| p.name == "normal").unwrap().nodes, vec!["c1", "c2"]);
}

#[test]
fn config_and_sacctmgr() {
    let c = parse_show_config(&fixture("scontrol/show-config.txt"));
    assert_eq!(c.cluster_name.as_deref(), Some("rsz-it"));
    assert_eq!(c.version.as_deref(), Some("26.05.5"));
    assert_eq!(c.job_acct_gather_frequency, Some(5));
    assert_eq!(c.constrain_ram_space, Some(true));
    assert_eq!(c.def_mem_per_cpu, Some(256));
    assert!(c.stores_job_script);
    let a = parse_sacctmgr_assoc(&fixture("sacctmgr/assoc.txt"));
    let alice = a.iter().find(|x| x.account == "mlteam" && x.user.as_deref() == Some("alice")).unwrap();
    assert_eq!(alice.grp_tres, TresLimits { cpu: Some(4), mem_mib: Some(4096), gpus: None, node: None });
    assert_eq!(alice.max_tres_per_job, TresLimits { cpu: Some(2), mem_mib: Some(2000), gpus: Some(2), node: None });
    assert_eq!(alice.qos, vec!["normal"]);
    let q = parse_sacctmgr_qos(&fixture("sacctmgr/qos.txt"));
    let g = q.iter().find(|x| x.name == "gpuq").unwrap();
    assert_eq!(g.max_tres_per_job, TresLimits { cpu: None, mem_mib: Some(32768), gpus: Some(4), node: None });
    let inv = inventory();
    let (job, _, grp) = inv.limits("mlteam", Some("alice"), None, Some("gpuq"));
    assert_eq!(job.gpus, Some(2));
    assert_eq!(job.mem_mib, Some(2000));
    assert_eq!(grp.gpus, Some(8));
    let (job, _, _) = inv.limits("vision", Some("alice"), None, None);
    assert!(job.is_empty());
}

#[tokio::test]
async fn platform_model() {
    let mut s = settings();
    s.prices.insert("gpu".into(), 32.0);
    s.prices.insert("default".into(), 0.2);
    let p = SlurmPlatform::new(s.clone());
    let mut unit = Unit {
        id: rightsizer_core::UnitId("job".into()),
        group: None,
        key_suffix: "x".into(),
        prior: Default::default(),
        seed: Default::default(),
        constraints: Default::default(),
        distributed: false,
        key: None,
    };
    unit.seed.manifest.extra.insert("walltime_s".into(), rightsizer_core::ScalarValue::Int(3600));
    let r = p.reference(&unit);
    assert_eq!(r.kind, "slurm");
    assert_eq!(r.retry.same_size, 0);
    assert!(r.retry.ladder.is_none(), "no ladder without --watch");
    assert_eq!(r.cpu_window_s, 5, "JobAcctGatherFrequency=task=5");
    assert!(r.memory_enforced);
    assert_eq!(r.walltime_s, Some(3600));
    assert_eq!(r.version, "26.05.5");
    let mut w = s.clone();
    w.watch = true;
    w.constrain_ram_space = Some(false);
    let r = SlurmPlatform::new(w).reference(&unit);
    assert_eq!(r.retry.ladder.unwrap().driver, rightsizer_core::LadderDriver::Caller);
    assert!(!r.memory_enforced);
    let alice = rightsizer_plugin::AdapterEnv { principal: Some("alice".into()), ..Default::default() };
    let st = p.state(&Tenant::new("rsz-it/mlteam").unwrap(), &alice).await.unwrap();
    assert_eq!(st.shapes.len(), 3, "c1/c2 collapse into one shape; g1, g2");
    let g1 = st.shapes.iter().find(|x| x.gpu.is_some_and(|g| g.accel == AcceleratorId::A100_80G)).unwrap();
    assert_eq!((g1.cpu, g1.memory, g1.price_per_hour), (MilliCpu::cores(64), Bytes::mib(507_904), 32.0));
    assert_eq!(g1.gpu.unwrap().count, 4);
    let q = st.quota.unwrap();
    assert_eq!(q.cpu, Some(MilliCpu::cores(2)));
    assert_eq!(q.memory, Some(Bytes::mib(2000)));
    assert_eq!(p.accelerators().canonical("h100"), AcceleratorId::H100_80G);
}
