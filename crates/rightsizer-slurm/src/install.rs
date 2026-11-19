//! `rsz slurm install` (slurm.md §9, SPEC §13.3): write `cli_filter.lua`,
//! `job_submit.lua` and their vendored modules (embedded in the binary from
//! `contrib/slurm/`), each script prefixed with a configuration header:
//!
//! ```lua
//! -- rightsizer configuration (rsz slurm install; edit with care)
//! RIGHTSIZER_CLUSTER = "gpu-a"
//! RIGHTSIZER_RSZ = "/usr/bin/rsz"
//! RIGHTSIZER_SOCKET = "/run/rightsizer.sock"
//! RIGHTSIZER_KEY_FILE = "/etc/slurm/rightsizer.key"
//! RIGHTSIZER_DISABLED = false
//! -- end rightsizer configuration
//! ```
//!
//! Files are written atomically (temp file + rename, so slurmctld sees a
//! new `job_submit.lua` mtime and re-loads it; a script that fails to load
//! keeps the previous one). `--disable` rewrites both scripts with
//! `RIGHTSIZER_DISABLED = true` (both short-circuit: jobs unchanged, no
//! admin_comment) and removes the decision table; a later install without
//! `--disable` re-enables. The previous header is kept for any value not
//! given again.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// Embedded Lua sources.
pub const CLI_FILTER: &str = include_str!("../../../contrib/slurm/cli_filter.lua");
/// Embedded Lua sources.
pub const JOB_SUBMIT: &str = include_str!("../../../contrib/slurm/job_submit.lua");
/// Vendored modules, by file name.
pub const MODULES: &[(&str, &str)] = &[
    ("rightsizer_cli.lua", include_str!("../../../contrib/slurm/rightsizer_cli.lua")),
    ("rightsizer_token.lua", include_str!("../../../contrib/slurm/rightsizer_token.lua")),
    ("sha256.lua", include_str!("../../../contrib/slurm/sha256.lua")),
    ("hmac.lua", include_str!("../../../contrib/slurm/hmac.lua")),
    ("json.lua", include_str!("../../../contrib/slurm/json.lua")),
];
/// The decision table (slurm.md §4.2, M4) removed by `--disable`.
pub const TABLE: &str = "rightsizer-table.json";

const BEGIN: &str = "-- rightsizer configuration (rsz slurm install; edit with care)";
const END: &str = "-- end rightsizer configuration";

/// What to install.
#[derive(Clone, Debug, Default)]
pub struct InstallOptions {
    /// Cluster name.
    pub cluster: Option<String>,
    /// cli_filter.lua path.
    pub cli_filter: Option<PathBuf>,
    /// job_submit.lua path.
    pub job_submit: Option<PathBuf>,
    /// `rsz` path for the decide-client helper.
    pub rsz: Option<String>,
    /// Daemon socket.
    pub socket: Option<String>,
    /// Key file job_submit.lua reads (SlurmUser, 0600).
    pub key_file: Option<String>,
    /// Short-circuit both scripts.
    pub disable: bool,
}

/// What was written.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Installed {
    /// Files written.
    pub written: Vec<PathBuf>,
    /// Files removed.
    pub removed: Vec<PathBuf>,
    /// slurm.conf lines to add.
    pub slurm_conf: Vec<String>,
    /// Effective header values.
    pub header: BTreeMap<String, String>,
}

fn lua_str(s: &str) -> String {
    let mut o = String::from("\"");
    for c in s.chars() {
        match c {
            '\\' => o.push_str("\\\\"),
            '"' => o.push_str("\\\""),
            '\n' => o.push_str("\\n"),
            c => o.push(c),
        }
    }
    o.push('"');
    o
}

/// Read the header values of an installed script (`NAME = "value"` / `true`).
pub fn read_header(text: &str) -> BTreeMap<String, String> {
    let mut m = BTreeMap::new();
    let mut inside = false;
    for l in text.lines() {
        if l == BEGIN {
            inside = true;
            continue;
        }
        if l == END {
            break;
        }
        if !inside {
            continue;
        }
        if let Some((k, v)) = l.split_once('=') {
            let k = k.trim();
            let v = v.trim();
            let v = v.strip_prefix('"').and_then(|v| v.strip_suffix('"')).map(|v| v.replace("\\\"", "\"").replace("\\\\", "\\")).unwrap_or_else(|| v.to_string());
            m.insert(k.to_string(), v);
        }
    }
    m
}

fn strip_header(text: &str) -> &str {
    match (text.find(BEGIN), text.find(END)) {
        (Some(0), Some(e)) => text[e + END.len()..].trim_start_matches('\n'),
        _ => text,
    }
}

fn header(values: &BTreeMap<String, String>) -> String {
    let mut s = format!("{BEGIN}\n");
    for (k, v) in values {
        if k == "RIGHTSIZER_DISABLED" {
            s.push_str(&format!("{k} = {}\n", if v == "true" { "true" } else { "false" }));
        } else {
            s.push_str(&format!("{k} = {}\n", lua_str(v)));
        }
    }
    s.push_str(END);
    s.push('\n');
    s
}

fn write_atomic(path: &Path, body: &str, mode: u32) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let dir = path.parent().filter(|d| !d.as_os_str().is_empty()).unwrap_or(Path::new("."));
    std::fs::create_dir_all(dir)?;
    let tmp = dir.join(format!(".{}.rsz-tmp", path.file_name().and_then(|n| n.to_str()).unwrap_or("x")));
    std::fs::write(&tmp, body)?;
    std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(mode))?;
    std::fs::rename(&tmp, path)
}

/// Install (or disable).
pub fn install(o: &InstallOptions) -> std::io::Result<Installed> {
    let mut out = Installed::default();
    let scripts: Vec<(&PathBuf, &str, &str)> = [(o.cli_filter.as_ref(), CLI_FILTER, "cli_filter"), (o.job_submit.as_ref(), JOB_SUBMIT, "job_submit")]
        .into_iter()
        .filter_map(|(p, body, n)| p.map(|p| (p, body, n)))
        .collect();
    if scripts.is_empty() {
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "nothing to install: give --cli-filter PATH and/or --job-submit PATH"));
    }
    for (path, body, name) in scripts {
        let mut values = std::fs::read_to_string(path).map(|t| read_header(&t)).unwrap_or_default();
        let mut set = |k: &str, v: &Option<String>| {
            if let Some(v) = v {
                values.insert(k.to_string(), v.clone());
            }
        };
        set("RIGHTSIZER_CLUSTER", &o.cluster);
        if name == "cli_filter" {
            set("RIGHTSIZER_RSZ", &o.rsz);
            set("RIGHTSIZER_SOCKET", &o.socket);
        } else {
            set("RIGHTSIZER_KEY_FILE", &o.key_file);
        }
        values.insert("RIGHTSIZER_DISABLED".into(), o.disable.to_string());
        let text = format!("{}{}", header(&values), strip_header(body));
        write_atomic(path, &text, 0o644)?;
        out.written.push(path.clone());
        let dir = path.parent().filter(|d| !d.as_os_str().is_empty()).unwrap_or(Path::new("."));
        for (m, src) in MODULES {
            let p = dir.join(m);
            if std::fs::read_to_string(&p).ok().as_deref() != Some(*src) {
                write_atomic(&p, src, 0o644)?;
                out.written.push(p);
            }
        }
        if name == "job_submit" && o.disable {
            let t = dir.join(TABLE);
            if t.exists() {
                std::fs::remove_file(&t)?;
                out.removed.push(t);
            }
        }
        for (k, v) in values {
            out.header.insert(k, v);
        }
    }
    if o.cli_filter.is_some() {
        out.slurm_conf.push("CliFilterPlugins=cli_filter/lua".into());
    }
    if o.job_submit.is_some() {
        out.slurm_conf.push("JobSubmitPlugins=lua".into());
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn install_disable_reenable() {
        let d = std::env::temp_dir().join(format!("rsz-install-{}", ulid::Ulid::generate()));
        let o = InstallOptions {
            cluster: Some("gpu-a".into()),
            cli_filter: Some(d.join("cli_filter.lua")),
            job_submit: Some(d.join("job_submit.lua")),
            rsz: Some("/opt/rsz/bin/rsz".into()),
            socket: Some("/run/rightsizer/rightsizer.sock".into()),
            key_file: Some("/etc/slurm/rightsizer.key".into()),
            disable: false,
        };
        let r = install(&o).unwrap();
        assert!(r.written.len() >= 7);
        assert_eq!(r.slurm_conf, vec!["CliFilterPlugins=cli_filter/lua", "JobSubmitPlugins=lua"]);
        let cf = std::fs::read_to_string(d.join("cli_filter.lua")).unwrap();
        assert!(cf.starts_with(BEGIN));
        assert!(cf.contains("RIGHTSIZER_SOCKET = \"/run/rightsizer/rightsizer.sock\"") && cf.contains("RIGHTSIZER_DISABLED = false"));
        assert!(cf.contains("function slurm_cli_pre_submit"));
        std::fs::write(d.join(TABLE), "{}").unwrap();
        // --disable alone keeps the other values.
        let r = install(&InstallOptions { cli_filter: o.cli_filter.clone(), job_submit: o.job_submit.clone(), disable: true, ..Default::default() }).unwrap();
        assert_eq!(r.removed, vec![d.join(TABLE)]);
        let cf = std::fs::read_to_string(d.join("cli_filter.lua")).unwrap();
        assert!(cf.contains("RIGHTSIZER_DISABLED = true") && cf.contains("/opt/rsz/bin/rsz"));
        assert_eq!(cf.matches(BEGIN).count(), 1);
        let js = std::fs::read_to_string(d.join("job_submit.lua")).unwrap();
        assert_eq!(read_header(&js).get("RIGHTSIZER_KEY_FILE").map(String::as_str), Some("/etc/slurm/rightsizer.key"));
        install(&InstallOptions { cli_filter: o.cli_filter.clone(), disable: false, ..Default::default() }).unwrap();
        assert!(std::fs::read_to_string(d.join("cli_filter.lua")).unwrap().contains("RIGHTSIZER_DISABLED = false"));
        let _ = std::fs::remove_dir_all(d);
    }
}
