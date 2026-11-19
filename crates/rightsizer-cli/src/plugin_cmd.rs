//! `rsz plugin list | check NAME [--fixtures DIR] [--deadline-ms N] [--server]`
//! (plugins.md §3.4, §4).
//!
//! - `list`: plugins found the interactive way (`rsz-plugin-*` on `PATH`
//!   plus `[plugins]` config), or with `--server` the way `rsz serve` finds
//!   them (configured absolute paths only), with pin status and refusals.
//! - `check NAME`: the conformance suite against the plugin's fixtures
//!   (`--fixtures`, `[plugins.NAME] fixtures`, `<exe>.fixtures/` or
//!   `fixtures/` next to it). Exit 0 when every check passes, 1 otherwise.

use std::path::PathBuf;
use std::time::Duration;

use rightsizer_engine::Config;
use rightsizer_plugin::conformance;
use rightsizer_plugin::discovery::{self, Mode, Source};

use crate::{Fail, Format, output};

fn usage() -> Fail {
    Fail::parse("usage: rsz plugin list [--server] | rsz plugin check NAME [--fixtures DIR] [--deadline-ms N] [--server]")
}

/// Dispatch.
pub async fn run(cfg: &Config, args: Vec<String>, out: Format) -> Result<(), Fail> {
    let mut it = args.into_iter();
    let sub = it.next().ok_or_else(usage)?;
    let rest: Vec<String> = it.collect();
    let server = rest.iter().any(|a| a == "--server");
    let mode = if server { Mode::Server } else { Mode::Interactive };
    let path = std::env::var_os("PATH");
    match sub.as_str() {
        "list" => {
            let (found, refused) = discovery::discover(&cfg.plugins, mode, path.as_deref());
            let v = serde_json::json!({
                "mode": if server { "server" } else { "interactive" },
                "plugins": found.iter().map(|f| serde_json::json!({
                    "kind": f.kind, "path": f.path, "source": if f.source == Source::Config { "config" } else { "path" },
                    "sha256": f.sha256, "pinned": f.config.as_ref().and_then(|c| c.sha256.clone()).is_some(), "pin_ok": f.pin_ok,
                })).collect::<Vec<_>>(),
                "refused": refused.iter().map(|r| serde_json::json!({"kind": r.kind, "reason": r.reason})).collect::<Vec<_>>(),
            });
            output::value(&v, out, || {
                let mut s = String::new();
                if found.is_empty() {
                    s.push_str("no plugins found\n");
                }
                for f in &found {
                    let pin = match f.pin_ok {
                        Some(true) => "pinned, sha256 ok",
                        Some(false) => "pinned, sha256 MISMATCH",
                        None => "unpinned",
                    };
                    s.push_str(&format!("{:<16} {} ({}, {pin})\n", f.kind, f.path.display(), if f.source == Source::Config { "config" } else { "PATH" }));
                }
                for r in &refused {
                    s.push_str(&format!("{:<16} REFUSED: {}\n", r.kind, r.reason));
                }
                s
            });
            Ok(())
        }
        "check" => {
            let mut name = None;
            let mut fixtures: Option<PathBuf> = None;
            let mut deadline = None;
            let mut i = 0;
            while i < rest.len() {
                match rest[i].as_str() {
                    "--fixtures" => {
                        fixtures = rest.get(i + 1).map(PathBuf::from);
                        i += 1;
                    }
                    "--deadline-ms" => {
                        deadline = rest.get(i + 1).and_then(|d| d.parse::<u64>().ok());
                        i += 1;
                    }
                    "--server" => {}
                    a if !a.starts_with('-') && name.is_none() => name = Some(a.to_string()),
                    _ => return Err(usage()),
                }
                i += 1;
            }
            let name = name.ok_or_else(usage)?;
            let found = discovery::find(&name, &cfg.plugins, mode, path.as_deref()).map_err(Fail::other)?;
            let dir = fixtures.or_else(|| discovery::fixtures_dir(&found)).ok_or_else(|| Fail::parse(format!("no fixtures for {name}: pass --fixtures DIR (valid/*.json, invalid/*.json)")))?;
            let fx = conformance::load_fixtures(&dir).map_err(Fail::parse)?;
            let mut spec = found.spec();
            if let Some(d) = deadline {
                spec.deadline = Duration::from_millis(d);
            }
            let report = conformance::run(spec, &fx).await;
            output::value(&serde_json::to_value(&report).unwrap_or_default(), out, || report.text());
            if report.passed() { Ok(()) } else { Err(Fail { code: 1, msg: format!("plugin {name} is not conformant") }) }
        }
        _ => Err(usage()),
    }
}
