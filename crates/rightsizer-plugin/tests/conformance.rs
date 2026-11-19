//! The conformance suite against `examples/plugins/rsz-plugin-nomad-demo`
//! (must pass) and the deliberately broken plugins in
//! `tests/integration/plugin/broken/` (each must fail the check that
//! targets its fault). Needs `python3` on PATH.

use std::path::PathBuf;
use std::time::Duration;

use rightsizer_plugin::client::{PluginAdapter, PluginClient, PluginSpec};
use rightsizer_plugin::conformance::{Report, load_fixtures, run};
use rightsizer_plugin::{Adapter, AdapterEnv, Native};

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn demo() -> PathBuf {
    root().join("examples/plugins/rsz-plugin-nomad-demo")
}

fn fixtures() -> rightsizer_plugin::conformance::Fixtures {
    load_fixtures(&root().join("examples/plugins/rsz-plugin-nomad-demo.fixtures")).unwrap()
}

fn python() -> bool {
    std::process::Command::new("python3").arg("--version").output().is_ok_and(|o| o.status.success())
}

async fn check(kind: &str, path: PathBuf) -> Report {
    let mut spec = PluginSpec::new(kind, path);
    spec.deadline = Duration::from_millis(1500);
    spec.handshake_deadline = Duration::from_secs(5);
    let r = run(spec, &fixtures()).await;
    eprintln!("{}", r.text());
    r
}

fn status(r: &Report, name: &str) -> bool {
    r.checks.iter().find(|c| c.name == name).unwrap_or_else(|| panic!("no check {name}")).passed
}

#[tokio::test(flavor = "multi_thread")]
async fn demo_plugin_is_conformant() {
    if !python() {
        eprintln!("python3 not found: skipping");
        return;
    }
    let r = check("nomad-demo", demo()).await;
    assert!(r.passed(), "{}", r.text());
}

#[tokio::test(flavor = "multi_thread")]
async fn broken_plugins_are_caught() {
    if !python() {
        eprintln!("python3 not found: skipping");
        return;
    }
    let b = root().join("tests/integration/plugin/broken");
    let crashy = check("crashy", b.join("rsz-plugin-crashy")).await;
    assert!(status(&crashy, "handshake"));
    assert!(!status(&crashy, "extract_determinism"), "crash on extract must fail extract_determinism");
    assert!(status(&crashy, "crash_leaves_unchanged"));
    assert!(!crashy.passed());

    let hangs = check("hangs", b.join("rsz-plugin-hangs")).await;
    assert!(!status(&hangs, "extract_determinism"), "a hanging extract must fail");
    assert!(hangs.checks.iter().any(|c| c.detail.contains("timed out")), "{}", hangs.text());
    assert!(status(&hangs, "timeout_leaves_unchanged"));

    let badshake = check("badshake", b.join("rsz-plugin-badshake")).await;
    assert!(!status(&badshake, "handshake"));
    assert!(badshake.checks[0].detail.contains("protocol"), "{}", badshake.text());

    let flappy = check("flappy", b.join("rsz-plugin-flappy")).await;
    assert!(status(&flappy, "handshake") && status(&flappy, "extract_determinism"));
    assert!(!status(&flappy, "render_idempotence"), "a non-idempotent render must fail");
}

#[tokio::test(flavor = "multi_thread")]
async fn client_restarts_after_a_crash_and_refuses_a_bad_pin() {
    if !python() {
        return;
    }
    let client = std::sync::Arc::new(PluginClient::new(PluginSpec::new("nomad-demo", demo())));
    let a = PluginAdapter::new(client.clone());
    let n: Native = serde_json::from_value(serde_json::json!({"kind":"object","value":{"Job":{"ID":"x","TaskGroups":[{"Name":"g","Tasks":[{"Name":"t","Resources":{"CPU":500,"MemoryMB":1024}}]}]}}})).unwrap();
    let x = a.extract(&n, &AdapterEnv::default()).await.unwrap();
    assert_eq!(x.units.len(), 1);
    let pid = client.pid().await.unwrap();
    std::process::Command::new("kill").arg("-9").arg(pid.to_string()).status().unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    // The first call after the crash may fail (EOF) or respawn; within the backoff it recovers.
    let mut ok = false;
    for _ in 0..20 {
        if a.extract(&n, &AdapterEnv::default()).await.is_ok() {
            ok = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(ok, "the client did not restart the plugin");
    assert_ne!(client.pid().await, Some(pid));
    assert!(client.spawn_count() >= 2);
    client.shutdown().await;

    let mut spec = PluginSpec::new("nomad-demo", demo());
    spec.sha256 = Some("ab".repeat(32));
    let c = PluginClient::new(spec);
    let e = c.start().await.unwrap_err();
    assert!(e.to_string().contains("sha256 mismatch"), "{e}");
    assert_eq!(c.spawn_count(), 0, "a mismatching binary must never be spawned");
}
