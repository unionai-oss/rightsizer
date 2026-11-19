//! `rsz flyte …` and `rsz history import flyte …`: delegated to
//! `python -m rightsizer.flyte.cli` (ARCHITECTURE.md §1, flyte.md §6, §8.3).
//!
//! The Rust binary never links Python or Flyte. When the interpreter can find
//! `rightsizer.flyte.cli` and `flyte`, `rsz flyte ARGS` replaces itself with
//! `PYTHON -m rightsizer.flyte.cli ARGS` (Unix `exec`; a child process
//! elsewhere), forwarding `--config` as `RIGHTSIZER_CONFIG` and `--store` as
//! `RIGHTSIZER__HISTORY__URI`; `rsz history import flyte ARGS` runs
//! `… cli import ARGS`. Otherwise it prints the install hint and exits 2.
//!
//! Interpreter: `[flyte] python`, else `$RIGHTSIZER_PYTHON`, else `python3`.

use std::path::{Path, PathBuf};
use std::process::Command;

use rightsizer_engine::Config;

use crate::Fail;

/// Install hint printed when the module cannot be found.
pub const INSTALL_HINT: &str = "rsz flyte needs the Python package with its Flyte extra: pip install 'rightsizer[flyte]' \
(or set [flyte] python / RIGHTSIZER_PYTHON to an interpreter that has it)";

/// Checks, without importing Flyte or rightsizer, that both can be found.
const PROBE: &str = "import importlib.util as u, os, sys\n\
s = u.find_spec('rightsizer')\n\
ok = bool(s and s.submodule_search_locations and u.find_spec('flyte'))\n\
ok = ok and any(os.path.exists(os.path.join(p, 'flyte', 'cli.py')) for p in s.submodule_search_locations)\n\
sys.exit(0 if ok else 3)\n";

/// `rsz flyte …` (and `rsz flyte` alone, which prints the module's usage).
pub fn handles(args: &[String]) -> bool {
    args.first().map(String::as_str) == Some("flyte")
}

/// The interpreter `rsz flyte` delegates to.
pub fn python(cfg: &Config) -> PathBuf {
    if let Some(p) = &cfg.flyte.python {
        return p.clone();
    }
    match std::env::var_os("RIGHTSIZER_PYTHON") {
        Some(p) if !p.is_empty() => PathBuf::from(p),
        _ => PathBuf::from("python3"),
    }
}

/// Whether `python` can import `rightsizer.flyte.cli` (probed without importing it).
pub fn available(python: &Path) -> bool {
    Command::new(python).args(["-c", PROBE]).output().map(|o| o.status.success()).unwrap_or(false)
}

fn command(cfg: &Config, config_path: Option<&Path>, store: Option<&str>) -> Result<(Command, PathBuf), Fail> {
    let py = python(cfg);
    if !available(&py) {
        return Err(Fail { code: 2, msg: format!("{INSTALL_HINT} (interpreter: {})", py.display()) });
    }
    let mut cmd = Command::new(&py);
    cmd.args(["-m", "rightsizer.flyte.cli"]);
    if let Some(p) = config_path {
        cmd.env(rightsizer_engine::config::CONFIG_ENV, p);
    }
    if let Some(s) = store {
        cmd.env("RIGHTSIZER__HISTORY__URI", s);
    }
    Ok((cmd, py))
}

/// Delegate `args` (`["flyte", ...]`). Returns only on failure (or, off Unix,
/// with the child's exit status).
pub fn platform(cfg: &Config, config_path: Option<&Path>, store: Option<&str>, args: Vec<String>) -> Result<(), Fail> {
    let (mut cmd, py) = command(cfg, config_path, store)?;
    cmd.args(&args[1..]);
    exec(cmd, &py)
}

/// `rsz history import flyte ARGS` → `… cli import ARGS` (the store is the configured one).
pub fn import(cfg: &Config, args: Vec<String>) -> Result<(), Fail> {
    let (mut cmd, py) = command(cfg, None, cfg.history.uri.as_deref())?;
    cmd.arg("import").args(&args);
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
        Some(c) => Err(Fail { code: c.clamp(1, 255) as u8, msg: format!("rightsizer.flyte.cli exited with {c}") }),
        None => Err(Fail::other("rightsizer.flyte.cli was terminated by a signal")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_interpreter_prints_the_install_hint() {
        let mut cfg = Config::default();
        cfg.flyte.python = Some(PathBuf::from("/nonexistent/python-for-rsz-test"));
        let e = platform(&cfg, None, None, vec!["flyte".into(), "run".into()]).unwrap_err();
        assert_eq!(e.code, 2);
        assert!(e.msg.contains("rightsizer[flyte]"), "{}", e.msg);
        let e = import(&cfg, vec!["--since".into(), "1d".into()]).unwrap_err();
        assert_eq!(e.code, 2);
    }

    #[test]
    fn handles_only_flyte() {
        assert!(handles(&["flyte".into(), "deploy".into()]));
        assert!(!handles(&["ray".into()]));
    }
}
