//! Adapter: extract on real-world scripts, rendering (golden files),
//! conversion rules, idempotence, validation, and the S2 named tests.
//! Regenerate goldens with `RSZ_REGEN=1`.

mod common;

use common::*;
use rightsizer_core::{AcceleratorId, Axis, Bytes, GpuShape, HoldReason, MilliCpu, Requests, ResourceSpec, UnitId};
use rightsizer_plugin::{Adapter, Native, Patch, SkipReason, apply_text_edits};
use rightsizer_slurm::SlurmAdapter;
use rightsizer_slurm::adapter::{apply_patch, parse_native};

fn adapter() -> SlurmAdapter {
    SlurmAdapter::new(settings())
}

fn spec(mem: Option<Bytes>, cpu: Option<u32>) -> ResourceSpec {
    ResourceSpec { requests: Requests { memory: mem, cpu: cpu.map(MilliCpu::cores), ..Default::default() }, limits: Default::default() }
}

fn script(name: &str) -> String {
    fixture(&format!("scripts/{name}"))
}

async fn render_doc(s: &str, d: &[(&str, ResourceSpec)]) -> (Patch, String) {
    let n = document(s);
    let d: Vec<_> = d.iter().map(|(u, s)| (UnitId(u.to_string()), s.clone())).collect();
    let p = adapter().render(&n, &d).await.unwrap();
    let out = match &p {
        Patch::TextEdits { edits } => apply_text_edits(s, edits).unwrap(),
        Patch::None => s.to_string(),
        other => panic!("unexpected {other:?}"),
    };
    (p, out)
}

fn golden(name: &str, got: &str) {
    let path = fixture_path(&format!("golden/{name}"));
    if std::env::var_os("RSZ_REGEN").is_some() || !path.exists() {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, got).unwrap();
    }
    pretty_assertions::assert_eq!(std::fs::read_to_string(&path).unwrap(), got, "golden {name} (RSZ_REGEN=1)");
}

#[tokio::test]
async fn extract_real_world_scripts() {
    let a = adapter();
    let env = env();
    // name, units, prior memory, prior cpu, gpu, distributed, defaulted memory
    type Case = (&'static str, usize, Option<Bytes>, u32, Option<GpuShape>, bool, bool);
    let cases: &[Case] = &[
        ("pytorch_single_gpu.sh", 1, Some(Bytes::gib(64)), 8, Some(GpuShape { accel: AcceleratorId::A100_80G, count: 1 }), false, false),
        ("ddp_multinode.sh", 1, Some(Bytes::mib(507_904)), 96, Some(GpuShape { accel: AcceleratorId::H100_80G, count: 8 }), true, false),
        ("array_mem_per_cpu.sh", 1, Some(Bytes::mib(8000)), 2, None, false, false),
        ("mem_per_gpu.sh", 1, Some(Bytes::gib(80)), 1, Some(GpuShape { accel: AcceleratorId::UnknownGpu, count: 2 }), false, false),
        ("no_memory.sh", 1, Some(Bytes::mib(1024)), 4, None, false, true),
        ("het_job.sh", 2, Some(Bytes::gib(8)), 4, None, false, false),
        ("late_directives.sh", 1, Some(Bytes::gib(16)), 1, None, false, false),
        ("quoted_and_comments.sh", 1, Some(Bytes::gib(12)), 3, None, false, false),
        ("gres_mixed_list.sh", 1, Some(Bytes::mib(48000)), 1, Some(GpuShape { accel: AcceleratorId::A10_24G, count: 2 }), false, false),
        ("gpus_per_task.sh", 1, Some(Bytes::gib(128)), 4, Some(GpuShape { accel: AcceleratorId::UnknownGpu, count: 4 }), true, false),
    ];
    for (name, n, mem, cpu, gpu, dist, defaulted) in cases {
        let x = a.extract(&document(&script(name)), &env).await.unwrap();
        assert!(x.skip.is_none(), "{name}: {:?}", x.skip);
        assert_eq!(x.units.len(), *n, "{name}");
        let u = &x.units[0];
        let r = &u.prior.spec.requests;
        assert_eq!(r.memory, *mem, "{name} memory");
        assert_eq!(r.cpu, Some(MilliCpu::cores(*cpu)), "{name} cpu");
        assert_eq!(r.gpu, *gpu, "{name} gpu");
        assert_eq!(u.distributed, *dist, "{name} distributed");
        assert_eq!(u.prior.defaulted.contains(Axis::Memory), *defaulted, "{name} defaulted");
        assert_eq!(x.tenant.as_str(), if name.starts_with("gres_mixed") { "rsz-it/vision" } else { "rsz-it/none" }, "{name}");
    }
    // Details.
    let x = a.extract(&document(&script("pytorch_single_gpu.sh")), &env).await.unwrap();
    let u = &x.units[0];
    assert_eq!(u.key_suffix, "resnet50-train");
    assert_eq!(u.seed.manifest.extra.get("walltime_s"), Some(&rightsizer_core::ScalarValue::Int(43200)));
    assert_eq!(u.constraints.ceiling.memory, Some(Bytes::mib(32000)), "gpu partition MaxMemPerNode");
    let x = a.extract(&document(&script("array_mem_per_cpu.sh")), &env).await.unwrap();
    assert_eq!(x.units[0].key_suffix, "preprocess-shard");
    assert_eq!(x.units[0].group.as_ref().unwrap().0, "array:1-100%10");
    let x = a.extract(&document(&script("het_job.sh")), &env).await.unwrap();
    assert_eq!(x.units[1].id.0, "het[1]");
    assert_eq!(x.units[1].key_suffix, "server-client/het-1");
    assert_eq!(x.units[1].prior.spec.requests.memory, Some(Bytes::gib(2)));
    assert!(x.units[1].distributed, "4 tasks");
    let x = a.extract(&document(&script("quoted_and_comments.sh")), &env).await.unwrap();
    assert_eq!(x.units[0].key_suffix, "quoted_name");
    assert!(x.units[0].constraints.held.iter().any(|h| h.axis == Axis::EphemeralStorage && h.reason == HoldReason::NoGrid));
    let x = a.extract(&document(&script("ddp_multinode.sh")), &env).await.unwrap();
    assert!(x.units[0].constraints.held.iter().any(|h| h.axis == Axis::Memory && h.reason == HoldReason::Platform), "--mem=0 held");
    assert!(x.units[0].seed.manifest.distributed_markers.contains(&"torchrun".to_string()));
    let x = a.extract(&document(&script("mem_per_gpu.sh")), &env).await.unwrap();
    assert!(x.units[0].constraints.held.iter().any(|h| h.axis == Axis::Cpu), "--cpus-per-gpu holds CPU");
    // Authenticated account from the env wins over --account.
    let mut e2 = env.clone();
    e2.namespace = Some("mlteam".into());
    let x = a.extract(&document(&script("gres_mixed_list.sh")), &e2).await.unwrap();
    assert_eq!(x.tenant.as_str(), "rsz-it/mlteam");
}

#[tokio::test]
async fn skips() {
    let a = adapter();
    let env = env();
    let x = a.extract(&sbatch("--mem-per-cpu=100 x.sh", Some("#!/bin/sh\n#SBATCH --mem=1G\n")), &env).await.unwrap();
    assert!(matches!(x.skip, Some(SkipReason::Unsupported { .. })), "sbatch rejects mixed memory forms");
    let x = a.extract(&document("#!/bin/sh\n#RIGHTSIZER off\n#SBATCH --mem=1G\n"), &env).await.unwrap();
    assert_eq!(x.skip, Some(SkipReason::OptedOut));
    let x = a.extract(&sbatch("--export=ALL,RIGHTSIZER_TOKEN=abc --mem=1G --wrap true", None), &env).await.unwrap();
    assert!(matches!(x.skip, Some(SkipReason::AlreadySized { .. })));
    let x = a.extract(&sbatch("--mem=lots x.sh", Some("#!/bin/sh\n")), &env).await.unwrap();
    assert!(matches!(x.skip, Some(SkipReason::Unsupported { .. })));
}

#[tokio::test]
async fn render_goldens() {
    let cases: &[(&str, &[(&str, ResourceSpec)])] = &[
        ("pytorch_single_gpu.sh", &[("job", spec(Some(Bytes::gib(24)), Some(4)))]),
        ("array_mem_per_cpu.sh", &[("job", spec(Some(Bytes::mib(3000)), Some(1)))]),
        ("mem_per_gpu.sh", &[("job", spec(Some(Bytes::gib(30)), None))]),
        ("no_memory.sh", &[("job", spec(Some(Bytes::gib(3)), Some(2)))]),
        ("het_job.sh", &[("het[0]", spec(Some(Bytes::gib(6)), Some(2))), ("het[1]", spec(Some(Bytes::mib(1536)), Some(2)))]),
        ("late_directives.sh", &[("job", spec(Some(Bytes::gib(10)), Some(2)))]),
        ("quoted_and_comments.sh", &[("job", spec(Some(Bytes::gib(6)), Some(2)))]),
        ("gres_mixed_list.sh", &[("job", spec(Some(Bytes::mib(20000)), None))]),
        ("ddp_multinode.sh", &[("job", spec(Some(Bytes::gib(100)), Some(48)))]),
    ];
    for (name, d) in cases {
        let s = script(name);
        let (_, out) = render_doc(&s, d).await;
        golden(&format!("{name}.out"), &out);
        // Idempotent: rendering the rendered script is Patch::None.
        let (p2, _) = render_doc(&out, d).await;
        assert_eq!(p2, Patch::None, "{name} not idempotent");
    }
}

#[tokio::test]
async fn argv_and_mixed_rendering() {
    let a = adapter();
    let s = "#!/bin/bash\n#SBATCH --mem=64G   # big\n#SBATCH -o out.log\npython x.py\n";
    // cpus from argv, memory from the script: both edited where they came from.
    let n = sbatch("-c 8 --parsable job.sh arg", Some(s));
    let p = a.render(&n, &[(UnitId("job".into()), spec(Some(Bytes::gib(24)), Some(4)))]).await.unwrap();
    let Patch::Options { options } = &p else { panic!("{p:?}") };
    assert_eq!(options["cpus-per-task"], "4");
    assert!(options.contains_key("_script_edits"));
    let after = apply_patch(&n, &p).unwrap();
    let Native::Object { value } = &after else { panic!() };
    assert_eq!(value["argv"], serde_json::json!(["-c", "4", "--parsable", "job.sh", "arg"]));
    assert_eq!(value["script"], "#!/bin/bash\n#SBATCH --mem=24G   # big\n#SBATCH -o out.log\npython x.py\n");
    assert_eq!(a.render(&after, &[(UnitId("job".into()), spec(Some(Bytes::gib(24)), Some(4)))]).await.unwrap(), Patch::None);
    // argv overrides the script: the argv value is the one edited.
    let n = sbatch("--mem=32G job.sh", Some(s));
    let p = a.render(&n, &[(UnitId("job".into()), spec(Some(Bytes::gib(16)), None))]).await.unwrap();
    assert_eq!(p, Patch::Options { options: serde_json::json!({"mem": "16G"}).as_object().unwrap().clone() });
    // Forms are kept: -c8, --cpus-per-task 8, --mem=65536 (no suffix → MiB).
    for (argv, want) in [
        ("-c8 --wrap true", vec!["-c4", "--wrap", "true"]),
        ("--cpus-per-task 8 --wrap true", vec!["--cpus-per-task", "4", "--wrap", "true"]),
        ("--mem=65536 --wrap true", vec!["--mem=24576", "--wrap", "true"]),
    ] {
        let n = sbatch(argv, None);
        let sp = if argv.contains("mem") { spec(Some(Bytes::gib(24)), None) } else { spec(None, Some(4)) };
        let p = a.render(&n, &[(UnitId("job".into()), sp)]).await.unwrap();
        let Native::Object { value } = apply_patch(&n, &p).unwrap() else { panic!() };
        assert_eq!(value["argv"], serde_json::json!(want), "{argv}");
    }
    // Het argv components.
    let n = sbatch("--mem=1G : --mem=2G --wrap true", None);
    let p = a.render(&n, &[(UnitId("het[1]".into()), spec(Some(Bytes::gib(3)), None))]).await.unwrap();
    let Native::Object { value } = apply_patch(&n, &p).unwrap() else { panic!() };
    assert_eq!(value["argv"], serde_json::json!(["--mem=1G", ":", "--mem=3G", "--wrap", "true"]));
    // A wrap job with no memory option: --mem added to argv.
    let n = sbatch("-p gpu --wrap 'python x.py'", None);
    let p = a.render(&n, &[(UnitId("job".into()), spec(Some(Bytes::gib(5)), None))]).await.unwrap();
    let Native::Object { value } = apply_patch(&n, &p).unwrap() else { panic!() };
    assert_eq!(value["argv"], serde_json::json!(["--mem=5G", "-p", "gpu", "--wrap", "python x.py"]));
}

#[tokio::test]
async fn cli_filter_table_rendering() {
    let a = adapter();
    let n = Native::Object {
        value: serde_json::json!({"kind": "cli_filter", "type": "sbatch", "offset": 0,
            "options": {"mem": "1G", "cpus-per-task": "4", "job-name": "t", "argv": []}}),
    };
    let x = a.extract(&n, &env()).await.unwrap();
    assert_eq!(x.units[0].key_suffix, "t");
    let p = a.render(&n, &[(UnitId("job".into()), spec(Some(Bytes::mib(512)), Some(2)))]).await.unwrap();
    assert_eq!(p, Patch::Options { options: serde_json::json!({"mem": "512M", "cpus-per-task": "2"}).as_object().unwrap().clone() });
    let after = apply_patch(&n, &p).unwrap();
    assert_eq!(a.render(&after, &[(UnitId("job".into()), spec(Some(Bytes::mib(512)), Some(2)))]).await.unwrap(), Patch::None);
}

#[tokio::test]
async fn conversion_rules() {
    let a = adapter();
    let one = |s: &str| document(s);
    // --mem-per-cpu: decision ÷ CPUs per node, with the *new* cpus-per-task.
    let n = one("#!/bin/sh\n#SBATCH --ntasks-per-node=2 -c 4\n#SBATCH --mem-per-cpu=2G\n");
    let p = a.render(&n, &[(UnitId("job".into()), spec(Some(Bytes::gib(12)), Some(6)))]).await.unwrap();
    let out = parse_native(&apply_patch(&n, &p).unwrap()).unwrap();
    let c = &out.job.components[0];
    assert_eq!(c.value("cpus-per-task"), Some("3"));
    assert_eq!(c.value("mem-per-cpu"), Some("2G"), "12G / (3 × 2) = 2G: unchanged");
    let p = a.render(&n, &[(UnitId("job".into()), spec(Some(Bytes::gib(10)), None))]).await.unwrap();
    let out = parse_native(&apply_patch(&n, &p).unwrap()).unwrap();
    assert_eq!(out.job.components[0].value("mem-per-cpu"), Some("1280M"), "10G / 8 CPUs");
    // --mem-per-gpu: decision ÷ GPUs per node; rounding is up.
    let n = one("#!/bin/sh\n#SBATCH --gpus-per-node=3\n#SBATCH --mem-per-gpu=10000\n");
    let p = a.render(&n, &[(UnitId("job".into()), spec(Some(Bytes::mib(10_000)), None))]).await.unwrap();
    let out = parse_native(&apply_patch(&n, &p).unwrap()).unwrap();
    assert_eq!(out.job.components[0].value("mem-per-gpu"), Some("3334"));
    // GPU type change, typed form only, through the site gres table; count held when distributed.
    let n = one("#!/bin/sh\n#SBATCH --gres=gpu:a100:2,shard:1\n#SBATCH -p gpu\n");
    let sp = ResourceSpec { requests: Requests { gpu: Some(GpuShape { accel: AcceleratorId::H100_80G, count: 1 }), ..Default::default() }, limits: Default::default() };
    let p = a.render(&n, &[(UnitId("job".into()), sp.clone())]).await.unwrap();
    let out = parse_native(&apply_patch(&n, &p).unwrap()).unwrap();
    assert_eq!(out.job.components[0].value("gres"), Some("gpu:h100:1,shard:1"));
    let n = one("#!/bin/sh\n#SBATCH --gres=gpu:2\n");
    let p = a.render(&n, &[(UnitId("job".into()), sp.clone())]).await.unwrap();
    let out = parse_native(&apply_patch(&n, &p).unwrap()).unwrap();
    assert_eq!(out.job.components[0].value("gres"), Some("gpu:1"), "untyped stays untyped");
}

#[tokio::test]
async fn s2_mem_zero_never_changed() {
    let a = adapter();
    for s in ["#!/bin/sh\n#SBATCH --mem=0\n#SBATCH -c 4\n", "#!/bin/sh\n#SBATCH --mem=0G\n"] {
        let n = document(s);
        let p = a.render(&n, &[(UnitId("job".into()), spec(Some(Bytes::gib(3)), None))]).await.unwrap();
        assert_eq!(p, Patch::None, "{s}");
        let x = a.extract(&n, &env()).await.unwrap();
        assert!(x.units[0].constraints.held.iter().any(|h| h.axis == Axis::Memory));
    }
    // And validate refuses a patch that changes it.
    let n = document("#!/bin/sh\n#SBATCH --mem=0\n");
    let bad = Patch::TextEdits { edits: vec![rightsizer_plugin::TextEdit { start: 16, end: 17, replacement: "4G".into() }] };
    let r = a.validate(&n, &bad, &env()).await.unwrap_err();
    assert_eq!(r.reason, "mem_zero_changed");
}

#[tokio::test]
async fn s2_never_mixes_mem_forms() {
    let a = adapter();
    let forms = [
        "#!/bin/sh\n#SBATCH --mem=8G\n",
        "#!/bin/sh\n#SBATCH --mem-per-cpu=2G -c 4\n",
        "#!/bin/sh\n#SBATCH --gpus-per-node=2\n#SBATCH --mem-per-gpu=4G\n",
        "#!/bin/sh\n#SBATCH -c 2\n",
    ];
    for s in forms {
        let n = document(s);
        for mem in [Bytes::gib(1), Bytes::gib(5), Bytes::mib(777)] {
            let p = a.render(&n, &[(UnitId("job".into()), spec(Some(mem), Some(3)))]).await.unwrap();
            let before = parse_native(&n).unwrap();
            let after = parse_native(&apply_patch(&n, &p).unwrap()).unwrap();
            let fb = before.job.components[0].memory_forms();
            let fa = after.job.components[0].memory_forms();
            assert!(fa.len() <= 1, "{s}: {fa:?}");
            if !fb.is_empty() {
                assert_eq!(fa, fb, "{s}: form changed");
            }
            if let Err(r) = a.validate(&n, &p, &env()).await {
                assert!(!["mixed_memory_forms", "memory_form_changed"].contains(&r.reason.as_str()), "{s}: {r:?}");
            }
        }
    }
    // A patch adding a second form is rejected.
    let n = document("#!/bin/sh\n#SBATCH --mem=8G\n");
    let bad = Patch::TextEdits { edits: vec![rightsizer_plugin::TextEdit { start: 27, end: 27, replacement: "#SBATCH --mem-per-cpu=1G\n".into() }] };
    assert_eq!(a.validate(&n, &bad, &env()).await.unwrap_err().reason, "mixed_memory_forms");
}

#[tokio::test]
async fn s2_gpu_count_held_multinode() {
    let a = adapter();
    let more = |n| ResourceSpec { requests: Requests { gpu: Some(GpuShape { accel: AcceleratorId::H100_80G, count: n }), ..Default::default() }, limits: Default::default() };
    for s in [
        "#!/bin/sh\n#SBATCH -N 2 --gpus-per-node=h100:8\n",
        "#!/bin/sh\n#SBATCH -n 4 --gpus-per-task=h100:1\n",
        "#!/bin/sh\n#SBATCH -N 2 --gres=gpu:h100:4\n",
        "#!/bin/sh\n#SBATCH --nodes=2 --gpus=h100:16\n",
    ] {
        let n = document(s);
        let x = a.extract(&n, &env()).await.unwrap();
        assert!(x.units[0].distributed, "{s}");
        let p = a.render(&n, &[(UnitId("job".into()), more(1))]).await.unwrap();
        assert_eq!(p, Patch::None, "{s}: GPU count must be held");
    }
    // Single node, single task: the count may change (when the core decides so).
    let n = document("#!/bin/sh\n#SBATCH --gres=gpu:h100:4\n");
    assert_ne!(a.render(&n, &[(UnitId("job".into()), more(2))]).await.unwrap(), Patch::None);
}

#[tokio::test]
async fn validate_limits_and_shapes() {
    let a = adapter();
    let env = env();
    let (a, env) = (&a, &env);
    let check = |s: &'static str| async move {
        let n = document(s);
        a.validate(&n, &Patch::None, env).await
    };
    // gpu partition MaxMemPerNode=32000.
    assert_eq!(check("#!/bin/sh\n#SBATCH -p gpu --mem=40000\n").await.unwrap_err().reason, "exceeds_max_mem_per_node");
    assert!(check("#!/bin/sh\n#SBATCH -p gpu --mem=30000\n").await.is_ok());
    // normal partition nodes have 2 CPUs and 2048 MiB.
    assert_eq!(check("#!/bin/sh\n#SBATCH -c 4 --mem=100M\n").await.unwrap_err().reason, "exceeds_node_shape");
    assert_eq!(check("#!/bin/sh\n#SBATCH -N 3 --mem=100M\n").await.unwrap_err().reason, "exceeds_node_shape");
    assert_eq!(check("#!/bin/sh\n#SBATCH -p gpu --gres=gpu:h100:9\n").await.unwrap_err().reason, "exceeds_node_shape");
    assert!(check("#!/bin/sh\n#SBATCH -p gpu --gres=gpu:h100:8 --mem=30000\n").await.is_ok());
    // alice@mlteam: MaxTRESPerJob cpu=2,mem=2000M,gres/gpu=2; GrpTRES cpu=4,mem=4G.
    let mut e = env.clone();
    e.namespace = Some("mlteam".into());
    let n = document("#!/bin/sh\n#SBATCH -N 2 --mem=1500M\n");
    assert_eq!(a.validate(&n, &Patch::None, &e).await.unwrap_err().reason, "quota");
    let n = document("#!/bin/sh\n#SBATCH --mem=1500M\n");
    assert!(a.validate(&n, &Patch::None, &e).await.is_ok());
    let n = document("#!/bin/sh\n#SBATCH -p gpu --gres=gpu:a100:3 --mem=1000\n");
    assert_eq!(a.validate(&n, &Patch::None, &e).await.unwrap_err().reason, "quota");
}
