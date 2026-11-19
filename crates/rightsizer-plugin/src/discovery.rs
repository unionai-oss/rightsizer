//! Plugin discovery and trust (plugins.md §3.4, SPEC §14 item 6).
//!
//! - **Interactive CLI** ([`Mode::Interactive`]): `rsz-plugin-<kind>`
//!   executables on `PATH` **and** `[plugins.<kind>]` config entries
//!   (config wins for the same kind; relative config paths resolve against
//!   `PATH` like a command name).
//! - **Server modes** ([`Mode::Server`]: `rsz serve`, `rsz webhook serve`):
//!   **only** config entries with an **absolute** path. No `PATH` lookup;
//!   a relative path is refused.
//! - A `sha256` pin is checked before every spawn ([`crate::client`]);
//!   [`Found::pin_ok`] reports it at discovery time too.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use rightsizer_engine::config::PluginConfig;

use crate::client::{PluginSpec, file_sha256};

/// Executable name prefix.
pub const PREFIX: &str = "rsz-plugin-";

/// Where discovery runs.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Mode {
    /// The interactive CLI: `PATH` + config.
    Interactive,
    /// A server: config, absolute paths only.
    Server,
}

/// Where a plugin was found.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Source {
    /// `[plugins.<kind>]`.
    Config,
    /// `rsz-plugin-<kind>` on `PATH`.
    Path,
}

/// A discovered plugin.
#[derive(Clone, Debug, PartialEq)]
pub struct Found {
    /// Kind.
    pub kind: String,
    /// Resolved executable.
    pub path: PathBuf,
    /// Source.
    pub source: Source,
    /// Config entry (pins, args, env, deadline, fixtures).
    pub config: Option<PluginConfig>,
    /// `Some(true)` pinned and matching, `Some(false)` mismatch, `None` unpinned.
    pub pin_ok: Option<bool>,
    /// The file's sha256 (when readable).
    pub sha256: Option<String>,
}

impl Found {
    /// The spec to run it with.
    pub fn spec(&self) -> PluginSpec {
        let mut s = PluginSpec::new(self.kind.clone(), self.path.clone());
        if let Some(c) = &self.config {
            s.args = c.args.clone();
            s.env = c.env.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
            s.sha256 = c.sha256.clone();
            if let Some(d) = c.deadline_ms {
                s.deadline = Duration::from_millis(d as u64);
            }
        }
        s
    }
}

/// A plugin that could not be used.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Refused {
    /// Kind.
    pub kind: String,
    /// Why.
    pub reason: String,
}

fn is_executable(p: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    std::fs::metadata(p).is_ok_and(|m| m.is_file() && m.permissions().mode() & 0o111 != 0)
}

/// Why a server-mode plugin path is unsafe to run, if it is: the
/// executable or any directory above it (before and after resolving
/// symlinks) is writable by someone other than root and this process's
/// user. The sha256 pin is checked when the file is read, and the file is
/// executed afterwards by path: whoever can replace it in between defeats
/// the pin, so only root and the daemon's own user may be able to (the
/// same rule sudo and sshd apply). Sticky directories (`/tmp`) are fine:
/// others cannot replace entries they do not own there.
pub fn insecure_path(path: &Path) -> Option<String> {
    use std::os::unix::fs::MetadataExt;
    let me = crate::server::current_uid();
    let trusted = |uid: u32| uid == 0 || Some(uid) == me;
    let mut checked: Vec<PathBuf> = Vec::new();
    let real = std::fs::canonicalize(path).ok();
    for p in std::iter::once(path.to_path_buf()).chain(real) {
        let mut cur = Some(p.as_path());
        let mut first = true;
        while let Some(c) = cur {
            if !c.as_os_str().is_empty() && !checked.iter().any(|x| x == c) {
                // The leaf may be a symlink: its target is checked through the canonical path.
                let m = if first { std::fs::symlink_metadata(c) } else { std::fs::metadata(c) };
                let Ok(m) = m else { return Some(format!("{}: cannot stat", c.display())) };
                let sticky_dir = m.is_dir() && m.mode() & 0o1000 != 0;
                if !m.file_type().is_symlink() {
                    if m.mode() & 0o022 != 0 && !sticky_dir {
                        return Some(format!("{} is group- or world-writable", c.display()));
                    }
                    if !trusted(m.uid()) {
                        return Some(format!("{} is owned by uid {}, not root or this user", c.display(), m.uid()));
                    }
                }
                checked.push(c.to_path_buf());
            }
            first = false;
            cur = c.parent();
        }
    }
    None
}

fn on_path(path_var: Option<&std::ffi::OsStr>) -> Vec<(String, PathBuf)> {
    let mut out: BTreeMap<String, PathBuf> = BTreeMap::new();
    let Some(pv) = path_var else { return vec![] };
    for dir in std::env::split_paths(pv) {
        let Ok(rd) = std::fs::read_dir(&dir) else { continue };
        let mut names: Vec<_> = rd.flatten().collect();
        names.sort_by_key(|e| e.file_name());
        for e in names {
            let name = e.file_name().to_string_lossy().into_owned();
            let Some(kind) = name.strip_prefix(PREFIX) else { continue };
            if kind.is_empty() || kind.contains('.') && kind.ends_with(".fixtures") {
                continue;
            }
            let p = e.path();
            if is_executable(&p) {
                // First match on PATH wins, like a shell.
                out.entry(kind.to_string()).or_insert(p);
            }
        }
    }
    out.into_iter().collect()
}

fn which_in(name: &Path, path_var: Option<&std::ffi::OsStr>) -> Option<PathBuf> {
    if name.components().count() > 1 {
        return Some(name.to_path_buf());
    }
    std::env::split_paths(path_var?).map(|d| d.join(name)).find(|p| is_executable(p))
}

fn pin(path: &Path, cfg: Option<&PluginConfig>) -> (Option<bool>, Option<String>) {
    let sha = file_sha256(path).ok();
    let ok = cfg.and_then(|c| c.sha256.as_ref()).map(|want| sha.as_deref().is_some_and(|s| s.eq_ignore_ascii_case(want.trim())));
    (ok, sha)
}

/// Discover plugins. `path_var` is `$PATH` (ignored in server mode).
pub fn discover(config: &BTreeMap<String, PluginConfig>, mode: Mode, path_var: Option<&std::ffi::OsStr>) -> (Vec<Found>, Vec<Refused>) {
    let mut found: BTreeMap<String, Found> = BTreeMap::new();
    let mut refused = Vec::new();
    if mode == Mode::Interactive {
        for (kind, p) in on_path(path_var) {
            let (pin_ok, sha256) = pin(&p, None);
            found.insert(kind.clone(), Found { kind, path: p, source: Source::Path, config: None, pin_ok, sha256 });
        }
    }
    for (kind, c) in config {
        if c.path.as_os_str().is_empty() {
            refused.push(Refused { kind: kind.clone(), reason: "plugins.<kind>.path is empty".into() });
            continue;
        }
        let path = if c.path.is_absolute() {
            c.path.clone()
        } else if mode == Mode::Server {
            refused.push(Refused { kind: kind.clone(), reason: format!("{}: server modes load only absolute paths", c.path.display()) });
            found.remove(kind);
            continue;
        } else {
            match which_in(&c.path, path_var) {
                Some(p) => p,
                None => {
                    refused.push(Refused { kind: kind.clone(), reason: format!("{}: not found on PATH", c.path.display()) });
                    continue;
                }
            }
        };
        if !is_executable(&path) {
            refused.push(Refused { kind: kind.clone(), reason: format!("{}: not an executable file", path.display()) });
            found.remove(kind);
            continue;
        }
        if mode == Mode::Server
            && let Some(why) = insecure_path(&path)
        {
            refused.push(Refused { kind: kind.clone(), reason: format!("{}: unsafe to run from a server: {why}", path.display()) });
            found.remove(kind);
            continue;
        }
        let (pin_ok, sha256) = pin(&path, Some(c));
        if pin_ok == Some(false) {
            refused.push(Refused {
                kind: kind.clone(),
                reason: format!("{}: sha256 mismatch (pinned {}, found {})", path.display(), c.sha256.clone().unwrap_or_default(), sha256.clone().unwrap_or_default()),
            });
            found.remove(kind);
            continue;
        }
        found.insert(kind.clone(), Found { kind: kind.clone(), path, source: Source::Config, config: Some(c.clone()), pin_ok, sha256 });
    }
    (found.into_values().collect(), refused)
}

/// Find one plugin by kind.
pub fn find(kind: &str, config: &BTreeMap<String, PluginConfig>, mode: Mode, path_var: Option<&std::ffi::OsStr>) -> Result<Found, String> {
    let (found, refused) = discover(config, mode, path_var);
    if let Some(r) = refused.iter().find(|r| r.kind == kind) {
        return Err(format!("plugin {kind} refused: {}", r.reason));
    }
    found.into_iter().find(|f| f.kind == kind).ok_or_else(|| match mode {
        Mode::Interactive => format!("no plugin {kind}: no {PREFIX}{kind} on PATH and no [plugins.{kind}] in config"),
        Mode::Server => format!("no plugin {kind}: server modes need [plugins.{kind}] path = \"/absolute/path\""),
    })
}

/// The conformance fixtures directory for a plugin: config `fixtures`,
/// else `<executable>.fixtures/`, else `fixtures/` next to it.
pub fn fixtures_dir(f: &Found) -> Option<PathBuf> {
    if let Some(d) = f.config.as_ref().and_then(|c| c.fixtures.clone()) {
        return Some(d);
    }
    let mut s = f.path.clone().into_os_string();
    s.push(".fixtures");
    let p = PathBuf::from(s);
    if p.is_dir() {
        return Some(p);
    }
    let sib = f.path.parent()?.join("fixtures");
    sib.is_dir().then_some(sib)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn exe(dir: &Path, name: &str, body: &str) -> PathBuf {
        use std::os::unix::fs::PermissionsExt;
        let p = dir.join(name);
        std::fs::write(&p, body).unwrap();
        std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o755)).unwrap();
        p
    }

    #[test]
    fn path_config_server_and_pins() {
        let d = std::env::temp_dir().join(format!("rsz-disc-{}", ulid::Ulid::generate()));
        let (a, b) = (d.join("a"), d.join("b"));
        std::fs::create_dir_all(&a).unwrap();
        std::fs::create_dir_all(&b).unwrap();
        let nomad = exe(&a, "rsz-plugin-nomad", "#!/bin/sh\n");
        exe(&b, "rsz-plugin-nomad", "#!/bin/sh\necho shadowed\n");
        exe(&a, "rsz-plugin-armada", "#!/bin/sh\n");
        std::fs::write(a.join("rsz-plugin-notexec"), "x").unwrap();
        let path = std::env::join_paths([&a, &b]).unwrap();
        let (f, r) = discover(&BTreeMap::new(), Mode::Interactive, Some(&path));
        assert_eq!(f.iter().map(|x| x.kind.as_str()).collect::<Vec<_>>(), vec!["armada", "nomad"]);
        assert_eq!(f[1].path, nomad);
        assert!(r.is_empty());
        // Server mode: nothing from PATH; relative config refused; pins checked.
        let sha = file_sha256(&nomad).unwrap();
        let mut cfg = BTreeMap::new();
        cfg.insert("nomad".to_string(), PluginConfig { path: nomad.clone(), sha256: Some(sha.clone()), ..Default::default() });
        cfg.insert("rel".to_string(), PluginConfig { path: "rsz-plugin-armada".into(), ..Default::default() });
        cfg.insert("bad".to_string(), PluginConfig { path: nomad.clone(), sha256: Some("00".repeat(32)), ..Default::default() });
        let (f, r) = discover(&cfg, Mode::Server, Some(&path));
        assert_eq!(f.iter().map(|x| x.kind.as_str()).collect::<Vec<_>>(), vec!["nomad"]);
        assert_eq!(f[0].pin_ok, Some(true));
        assert!(r.iter().any(|x| x.kind == "rel" && x.reason.contains("absolute")));
        assert!(r.iter().any(|x| x.kind == "bad" && x.reason.contains("sha256 mismatch")));
        assert!(find("bad", &cfg, Mode::Server, None).unwrap_err().contains("refused"));
        assert!(find("armada", &cfg, Mode::Server, Some(&path)).is_err());
        // Interactive: a relative config path resolves on PATH.
        let (f, _) = discover(&cfg, Mode::Interactive, Some(&path));
        assert!(f.iter().any(|x| x.kind == "rel" && x.path == a.join("rsz-plugin-armada")));
        let _ = std::fs::remove_dir_all(d);
    }

    #[test]
    fn server_mode_refuses_replaceable_executables() {
        use std::os::unix::fs::PermissionsExt;
        let d = std::env::temp_dir().join(format!("rsz-disc-{}", ulid::Ulid::generate()));
        let shared = d.join("shared");
        std::fs::create_dir_all(&shared).unwrap();
        let p = exe(&shared, "rsz-plugin-nomad", "#!/bin/sh\n");
        let sha = file_sha256(&p).unwrap();
        let mut cfg = BTreeMap::new();
        cfg.insert("nomad".to_string(), PluginConfig { path: p.clone(), sha256: Some(sha), ..Default::default() });
        assert_eq!(discover(&cfg, Mode::Server, None).0.len(), 1, "{:?}", discover(&cfg, Mode::Server, None).1);
        // A world-writable directory: anyone could swap the file after the pin check.
        std::fs::set_permissions(&shared, std::fs::Permissions::from_mode(0o777)).unwrap();
        let (f, r) = discover(&cfg, Mode::Server, None);
        assert!(f.is_empty(), "pinned plugin in a world-writable dir was accepted");
        assert!(r[0].reason.contains("writable"), "{}", r[0].reason);
        // A group-writable executable likewise.
        std::fs::set_permissions(&shared, std::fs::Permissions::from_mode(0o755)).unwrap();
        std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o775)).unwrap();
        assert!(discover(&cfg, Mode::Server, None).0.is_empty());
        // Interactive use is the user's own business.
        assert_eq!(discover(&cfg, Mode::Interactive, None).0.len(), 1);
        let _ = std::fs::remove_dir_all(d);
    }
}
