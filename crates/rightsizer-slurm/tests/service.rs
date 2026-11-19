//! The daemon side (`SlurmService`): who may get a token for what.

mod common;

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use common::*;
use rightsizer_core::{Bytes, Decision, Mode};
use rightsizer_engine::{Config, Engine};
use rightsizer_plugin::protocol::{DecideParams, SignParams};
use rightsizer_plugin::AdapterEnv;
use rightsizer_slurm::runner::{CmdOutput, CommandRunner};
use rightsizer_slurm::service::{Identity, ServiceConfig, ServiceError, SlurmService};
use rightsizer_slurm::token::KeyRing;
use ulid::Ulid;

struct NoSlurm;

#[async_trait]
impl CommandRunner for NoSlurm {
    async fn run_env(&self, _p: &Path, _a: &[String], _e: &[(String, String)]) -> std::io::Result<CmdOutput> {
        Ok(CmdOutput { success: false, code: Some(1), ..Default::default() })
    }
}

fn service(dir: &Path) -> SlurmService {
    let key = dir.join("rightsizer.key");
    KeyRing::generate().unwrap().save(&key).unwrap();
    let cfg = ServiceConfig { settings: settings(), key_file: Some(key), use_associations: false, ..Default::default() };
    SlurmService::with_runner(cfg, Arc::new(NoSlurm))
}

async fn engine() -> Engine {
    Engine::builder(Config::default()).store(Arc::new(rightsizer_history::MemoryStore::new())).build().await.unwrap()
}

fn decision(id: Ulid, gpu: Option<serde_json::Value>) -> Decision {
    let mut applied = serde_json::json!({"memory": Bytes::gib(2).0, "cpu": 2000});
    if let Some(g) = &gpu {
        applied["gpu"] = g.clone();
    }
    serde_json::from_value(serde_json::json!({
        "id": id.to_string(), "key": "slurm://rsz-it/mlteam/train", "unit": "job", "mode": Mode::Enforce,
        "recommended": {"requests": applied}, "applied": {"requests": applied},
        "prior": {"spec": {"requests": {"memory": Bytes::gib(4).0, "cpu": 2000}}},
        "source": {"kind": "estimator", "id": "test", "version": "1"}, "propensity": 1.0,
    }))
    .unwrap()
}

fn alice() -> Identity {
    Identity { uid: 1000, user: Some("alice".into()) }
}

async fn sign(svc: &SlurmService, e: &Engine, id: Ulid, argv: &str) -> Result<Option<String>, ServiceError> {
    let allow = |t: &str| t.starts_with("slurm://rsz-it/mlteam");
    let p = SignParams { kind: "slurm".into(), native: sbatch(argv, Some("#!/bin/bash\npython train.py\n")), decision_ids: vec![id] };
    svc.sign(e, p, &alice(), &allow).await.map(|r| r.token)
}

#[tokio::test]
async fn sign_only_attests_a_ladder_step_of_the_same_workload() {
    let dir = tempfile::tempdir().unwrap();
    let svc = service(dir.path());
    let e = engine().await;
    let id = Ulid::from_string("01JDZ7Q4W8M7C9B5V3N2X1Y0ZA").unwrap();
    e.store().put_decision(&decision(id, None)).await.unwrap();
    // The ladder's resubmission: same job, memory one step up.
    let ok = sign(&svc, &e, id, "--job-name=train -A mlteam --mem=4G -c 2 x.sh").await;
    assert!(ok.as_ref().is_ok_and(|t| t.is_some()), "{ok:?}");
    // Another workload of the account, borrowing the decision id.
    let other = sign(&svc, &e, id, "--job-name=other -A mlteam --mem=4G -c 2 x.sh").await;
    assert!(matches!(other, Err(ServiceError::Invalid(_))), "another workload got a token: {other:?}");
    // GPUs the decision never had.
    let gpus = sign(&svc, &e, id, "--job-name=train -A mlteam --mem=4G -c 2 -p gpu --gres=gpu:a100:4 x.sh").await;
    assert!(matches!(gpus, Err(ServiceError::Invalid(_))), "added GPUs got a token: {gpus:?}");
}

#[tokio::test]
async fn sign_keeps_the_decisions_gpus() {
    let dir = tempfile::tempdir().unwrap();
    let svc = service(dir.path());
    let e = engine().await;
    let id = Ulid::from_string("01JDZ7Q4W8M7C9B5V3N2X1Y0ZB").unwrap();
    e.store().put_decision(&decision(id, Some(serde_json::json!({"accel": "A100-80G", "count": 2})))).await.unwrap();
    let ok = sign(&svc, &e, id, "--job-name=train -A mlteam --mem=4G -c 2 -p gpu --gres=gpu:a100:2 x.sh").await;
    assert!(ok.as_ref().is_ok_and(|t| t.is_some()), "{ok:?}");
    let more = sign(&svc, &e, id, "--job-name=train -A mlteam --mem=4G -c 2 -p gpu --gres=gpu:a100:8 x.sh").await;
    assert!(matches!(more, Err(ServiceError::Invalid(_))), "{more:?}");
}

#[tokio::test]
async fn the_cluster_is_the_daemons_not_the_callers() {
    let dir = tempfile::tempdir().unwrap();
    let svc = service(dir.path());
    let e = engine().await;
    // tenants_by_uid would allow this uid on another cluster only.
    let allow = |t: &str| t.starts_with("slurm://evil/");
    let p = DecideParams {
        kind: "slurm".into(),
        native: sbatch("--job-name=train -A mlteam --mem=4G x.sh", Some("#!/bin/bash\npython train.py\n")),
        mode: None,
        env: AdapterEnv { cluster: Some("evil".into()), ..Default::default() },
        deadline_ms: Some(1000),
        attempt: None,
        watch: false,
        dry_run: false,
    };
    let r = svc.decide(&e, p, &alice(), &allow).await;
    assert!(matches!(r, Err(ServiceError::Forbidden(ref m)) if m.contains("rsz-it")), "caller-chosen cluster was used: {r:?}");
}
