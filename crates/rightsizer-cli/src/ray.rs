//! `rsz ray …`: delegated to `python -m rightsizer.ray.cli` (ARCHITECTURE.md §1).
//!
//! The Rust binary never links Python or Ray. When the interpreter can find
//! `rightsizer.ray.cli` and `ray`, `rsz ray ARGS` replaces itself with
//! `PYTHON -m rightsizer.ray.cli ARGS` (Unix `exec`; a child process
//! elsewhere), forwarding `--config` as `RIGHTSIZER_CONFIG` and `--store` as
//! `RIGHTSIZER__HISTORY__URI`. Otherwise it prints the install hint and
//! exits 2.
//!
//! Interpreter: `[ray] python`, else `$RIGHTSIZER_PYTHON`, else `python3`.

use std::path::{Path, PathBuf};
use std::process::Command;

use rightsizer_engine::Config;

use crate::Fail;

/// Install hint printed when the module cannot be found.
pub const INSTALL_HINT: &str = "rsz ray needs the Python package with its Ray extra: pip install 'rightsizer[ray]' \
(or set [ray] python / RIGHTSIZER_PYTHON to an interpreter that has it)";

/// Checks, without importing Ray or rightsizer, that both can be found.
const PROBE: &str = "import importlib.util as u, os, sys\n\
s = u.find_spec('rightsizer')\n\
ok = bool(s and s.submodule_search_locations and u.find_spec('ray'))\n\
ok = ok and any(os.path.exists(os.path.join(p, 'ray', 'cli.py')) for p in s.submodule_search_locations)\n\
sys.exit(0 if ok else 3)\n";

/// `rsz ray …` (and `rsz ray` alone, which prints the module's usage).
pub fn handles(args: &[String]) -> bool {
    args.first().map(String::as_str) == Some("ray")
}

/// The interpreter `rsz ray` delegates to.
pub fn python(cfg: &Config) -> PathBuf {
    if let Some(p) = &cfg.ray.python {
        return p.clone();
    }
    match std::env::var_os("RIGHTSIZER_PYTHON") {
        Some(p) if !p.is_empty() => PathBuf::from(p),
        _ => PathBuf::from("python3"),
    }
}

/// Whether `python` can import `rightsizer.ray.cli` (probed without importing it).
pub fn available(python: &Path) -> bool {
    Command::new(python).args(["-c", PROBE]).output().map(|o| o.status.success()).unwrap_or(false)
}

/// Delegate `args` (`["ray", ...]`). Returns only on failure (or, off Unix,
/// with the child's exit status).
pub fn platform(cfg: &Config, config_path: Option<&Path>, store: Option<&str>, args: Vec<String>) -> Result<(), Fail> {
    let py = python(cfg);
    if !available(&py) {
        return Err(Fail { code: 2, msg: format!("{INSTALL_HINT} (interpreter: {})", py.display()) });
    }
    let mut cmd = Command::new(&py);
    cmd.args(["-m", "rightsizer.ray.cli"]).args(&args[1..]);
    if let Some(p) = config_path {
        cmd.env(rightsizer_engine::config::CONFIG_ENV, p);
    }
    if let Some(s) = store {
        cmd.env("RIGHTSIZER__HISTORY__URI", s);
    }
    exec(cmd, &py)
}

#[cfg(unix)]
fn exec(mut cmd: Command, py: &Path) -> Result<(), Fail> {
    use std::os::unix::process::CommandExt as _;
    let err = cmd.exec();
    Err(Fail::other(format!("cannot run {}: {err}", py.display())))
}

#[cfg(not(unix))]
fn exec(mut cmd: Command, py: &Path) -> Result<(), Fail> {
    let st = cmd.status().map_err(|e| Fail::other(format!("cannot run {}: {e}", py.display())))?;
    match st.code() {
        Some(0) => Ok(()),
        Some(c) => Err(Fail { code: c.clamp(1, 255) as u8, msg: format!("rightsizer.ray.cli exited with {c}") }),
        None => Err(Fail::other("rightsizer.ray.cli was terminated by a signal")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_interpreter_prints_the_install_hint() {
        let mut cfg = Config::default();
        cfg.ray.python = Some(PathBuf::from("/nonexistent/python-for-rsz-test"));
        let e = platform(&cfg, None, None, vec!["ray".into(), "submit".into()]).unwrap_err();
        assert_eq!(e.code, 2);
        assert!(e.msg.contains("rightsizer[ray]"), "{}", e.msg);
    }

    #[test]
    fn handles_only_ray() {
        assert!(handles(&["ray".into(), "submit".into()]));
        assert!(!handles(&["k8s".into()]));
    }
}
