//! `rsz serve`, `rsz plugin`, `rsz slurm install|keygen|rotate-key|decide-client`.

use std::io::{BufRead, BufReader, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

fn rsz() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_rsz"))
}

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn tmp() -> PathBuf {
    // Short: sun_path is 104 bytes on macOS.
    let d = PathBuf::from(format!("/tmp/rsz-m3-{}", ulid::Ulid::generate()));
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn run(d: &std::path::Path, args: &[&str]) -> std::process::Output {
    Command::new(rsz()).args(args).env("RIGHTSIZER_CONFIG", d.join("rightsizer.toml")).current_dir(d).output().unwrap()
}

#[test]
fn keygen_and_rotation() {
    let d = tmp();
    std::fs::write(d.join("rightsizer.toml"), "").unwrap();
    let key = d.join("k");
    let o = run(&d, &["slurm", "keygen", "--out", key.to_str().unwrap()]);
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    assert_eq!(std::fs::metadata(&key).unwrap().permissions().mode() & 0o777, 0o600);
    assert!(!run(&d, &["slurm", "keygen", "--out", key.to_str().unwrap()]).status.success(), "no silent overwrite");
    let copy = d.join("copy");
    let states = |step: &str| {
        let o = run(&d, &["-o", "json", "slurm", "rotate-key", step, "--key", key.to_str().unwrap(), "--copy-to", copy.to_str().unwrap()]);
        assert!(o.status.success(), "{step}: {}", String::from_utf8_lossy(&o.stderr));
        let v: serde_json::Value = serde_json::from_slice(&o.stdout).unwrap();
        v["keys"].as_array().unwrap().iter().map(|k| (k["kid"].as_str().unwrap().to_string(), k["state"].as_str().unwrap().to_string())).collect::<std::collections::BTreeMap<_, _>>()
    };
    let staged = states("stage");
    let old = staged.iter().find(|(_, s)| *s == "active").unwrap().0.clone();
    let new = staged.iter().find(|(_, s)| *s == "verify").unwrap().0.clone();
    let promoted = states("promote");
    assert_eq!((promoted[&old].as_str(), promoted[&new].as_str()), ("verify", "active"));
    let retired = states("retire");
    assert_eq!(retired.len(), 1);
    assert_eq!(retired[&new], "active");
    assert_eq!(std::fs::read_to_string(&key).unwrap(), std::fs::read_to_string(&copy).unwrap());
    assert_eq!(std::fs::metadata(&copy).unwrap().permissions().mode() & 0o777, 0o600);
    let _ = std::fs::remove_dir_all(d);
}

#[test]
fn install_and_disable() {
    let d = tmp();
    std::fs::write(d.join("rightsizer.toml"), "").unwrap();
    let (cf, js) = (d.join("etc/cli_filter.lua"), d.join("etc/job_submit.lua"));
    let o = run(&d, &["slurm", "install", "--cluster", "c1", "--cli-filter", cf.to_str().unwrap(), "--job-submit", js.to_str().unwrap(), "--socket", "/run/r.sock"]);
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    let out = String::from_utf8_lossy(&o.stdout);
    assert!(out.contains("CliFilterPlugins=cli_filter/lua") && out.contains("JobSubmitPlugins=lua"), "{out}");
    for m in ["rightsizer_cli.lua", "rightsizer_token.lua", "sha256.lua", "hmac.lua", "json.lua"] {
        assert!(d.join("etc").join(m).exists(), "{m}");
    }
    assert!(std::fs::read_to_string(&cf).unwrap().contains("RIGHTSIZER_DISABLED = false"));
    let o = run(&d, &["slurm", "install", "--cli-filter", cf.to_str().unwrap(), "--job-submit", js.to_str().unwrap(), "--disable"]);
    assert!(o.status.success());
    let t = std::fs::read_to_string(&js).unwrap();
    assert!(t.contains("RIGHTSIZER_DISABLED = true") && t.contains("RIGHTSIZER_CLUSTER = \"c1\""));
    let _ = std::fs::remove_dir_all(d);
}

#[test]
fn plugin_list_and_check() {
    if !Command::new("python3").arg("--version").output().is_ok_and(|o| o.status.success()) {
        return;
    }
    let d = tmp();
    std::fs::write(d.join("rightsizer.toml"), "").unwrap();
    let path = format!("{}:{}", root().join("examples/plugins").display(), std::env::var("PATH").unwrap_or_default());
    let o = Command::new(rsz()).args(["plugin", "list"]).env("PATH", &path).env("RIGHTSIZER_CONFIG", d.join("rightsizer.toml")).output().unwrap();
    assert!(String::from_utf8_lossy(&o.stdout).contains("nomad-demo"), "{}", String::from_utf8_lossy(&o.stdout));
    // Server mode ignores PATH.
    let o = Command::new(rsz()).args(["plugin", "list", "--server"]).env("PATH", &path).env("RIGHTSIZER_CONFIG", d.join("rightsizer.toml")).output().unwrap();
    assert!(!String::from_utf8_lossy(&o.stdout).contains("nomad-demo"));
    let o = Command::new(rsz()).args(["plugin", "check", "nomad-demo"]).env("PATH", &path).env("RIGHTSIZER_CONFIG", d.join("rightsizer.toml")).output().unwrap();
    let out = String::from_utf8_lossy(&o.stdout);
    assert!(o.status.success() && out.contains("6/6 checks passed"), "{out}\n{}", String::from_utf8_lossy(&o.stderr));
    let _ = std::fs::remove_dir_all(d);
}

#[test]
fn serve_status_over_the_socket_and_graceful_shutdown() {
    let d = tmp();
    let me = String::from_utf8(Command::new("id").arg("-u").output().unwrap().stdout).unwrap().trim().to_string();
    std::fs::write(d.join("rightsizer.toml"), format!("[serve]\ntenants_by_uid = {{ \"{me}\" = [\"slurm://c/acct\"] }}\n")).unwrap();
    let sock = d.join("s.sock");
    let mut child = Command::new(rsz())
        .args(["serve", "--socket", sock.to_str().unwrap()])
        .env("RIGHTSIZER_CONFIG", d.join("rightsizer.toml"))
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let t = Instant::now();
    while !sock.exists() && t.elapsed() < Duration::from_secs(20) {
        std::thread::sleep(Duration::from_millis(50));
    }
    // The helper cli_filter.lua runs.
    let mut h = Command::new(rsz())
        .args(["slurm", "decide-client", "--socket", sock.to_str().unwrap(), "--timeout-ms", "3000"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    h.stdin.take().unwrap().write_all(br#"{"jsonrpc":"2.0","id":1,"method":"status","params":{}}"#).unwrap();
    let o = h.wait_with_output().unwrap();
    let v: serde_json::Value = serde_json::from_slice(&o.stdout).unwrap();
    assert_eq!(v["result"]["caller"]["uid"].to_string(), me);
    assert_eq!(v["result"]["caller"]["tenants"][0], "slurm://c/acct");
    // A plugin kind that is not served is an error, not a decision.
    let mut s = std::os::unix::net::UnixStream::connect(&sock).unwrap();
    writeln!(s, r#"{{"jsonrpc":"2.0","id":2,"method":"decide","params":{{"kind":"nope","native":{{"kind":"object","value":{{}}}}}}}}"#).unwrap();
    let mut line = String::new();
    BufReader::new(s.try_clone().unwrap()).read_line(&mut line).unwrap();
    let v: serde_json::Value = serde_json::from_str(&line).unwrap();
    assert_eq!(v["error"]["data"]["code"], "unsupported", "{line}");
    drop(s);
    // Graceful shutdown.
    Command::new("kill").arg("-TERM").arg(child.id().to_string()).status().unwrap();
    let st = child.wait().unwrap();
    assert!(st.success());
    assert!(!sock.exists(), "socket removed on shutdown");
    // A dead daemon: the helper fails fast and prints nothing.
    let t = Instant::now();
    let o = Command::new(rsz()).args(["slurm", "decide-client", "--socket", sock.to_str().unwrap()]).stdin(Stdio::null()).output().unwrap();
    assert!(!o.status.success() && o.stdout.is_empty() && t.elapsed() < Duration::from_secs(1));
    let _ = std::fs::remove_dir_all(d);
}
