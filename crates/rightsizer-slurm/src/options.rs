//! The `sbatch` option table (from Slurm 26.05.5 `src/common/slurm_opt.c`,
//! options with a `set_func` or `set_func_sbatch`), and getopt-compatible
//! tokenization of argv and `#SBATCH` lines.

/// Whether an option takes an argument (getopt `has_arg`).
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum HasArg {
    /// `no_argument`.
    No,
    /// `required_argument`: `--opt=v`, `--opt v`, `-ov`, `-o v`.
    Required,
    /// `optional_argument`: only `--opt=v` (and `-ov`) carry a value.
    Optional,
}

/// One option.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct OptSpec {
    /// Long name.
    pub long: &'static str,
    /// Short letter.
    pub short: Option<char>,
    /// Argument.
    pub arg: HasArg,
}

macro_rules! o {
    ($l:literal, $s:expr, R) => {
        OptSpec { long: $l, short: $s, arg: HasArg::Required }
    };
    ($l:literal, $s:expr, N) => {
        OptSpec { long: $l, short: $s, arg: HasArg::No }
    };
    ($l:literal, $s:expr, O) => {
        OptSpec { long: $l, short: $s, arg: HasArg::Optional }
    };
}

/// Every `sbatch` option.
pub static OPTIONS: &[OptSpec] = &[
    o!("account", Some('A'), R),
    o!("acctg-freq", None, R),
    o!("array", Some('a'), R),
    o!("batch", None, R),
    o!("bbf", None, R),
    o!("begin", Some('b'), R),
    o!("bb", None, R),
    o!("consolidate-segments", None, N),
    o!("cluster-constraint", None, R),
    o!("clusters", Some('M'), R),
    o!("cluster", None, R),
    o!("comment", None, R),
    o!("constraint", Some('C'), R),
    o!("container", None, R),
    o!("container-id", None, R),
    o!("container-type", None, R),
    o!("context", None, R),
    o!("contiguous", None, N),
    o!("core-spec", Some('S'), R),
    o!("cores-per-socket", None, R),
    o!("cpu-freq", None, R),
    o!("cpus-per-gpu", None, R),
    o!("cpus-per-task", Some('c'), R),
    o!("deadline", None, R),
    o!("delay-boot", None, R),
    o!("dependency", Some('d'), R),
    o!("distribution", Some('m'), R),
    o!("error", Some('e'), R),
    o!("exclude", Some('x'), R),
    o!("exclusive", None, O),
    o!("export", None, R),
    o!("export-file", None, R),
    o!("external", None, N),
    o!("extra", None, R),
    o!("extra-node-info", Some('B'), R),
    o!("get-user-env", None, O),
    o!("gid", None, R),
    o!("gpu-bind", None, R),
    o!("tres-bind", None, R),
    o!("gpu-freq", None, R),
    o!("gpus", Some('G'), R),
    o!("gpus-per-node", None, R),
    o!("gpus-per-socket", None, R),
    o!("gpus-per-task", None, R),
    o!("tres-per-task", None, R),
    o!("gres", None, R),
    o!("gres-flags", None, R),
    o!("help", Some('h'), N),
    o!("hint", None, R),
    o!("hold", Some('H'), N),
    o!("ignore-pbs", None, N),
    o!("input", Some('i'), R),
    o!("job-name", Some('J'), R),
    o!("kill-on-invalid-dep", None, R),
    o!("licenses", Some('L'), R),
    o!("mail-type", None, R),
    o!("mail-user", None, R),
    o!("mcs-label", None, R),
    o!("oom-kill-step", None, O),
    o!("mem", None, R),
    o!("mem-bind", None, R),
    o!("mem-per-cpu", None, R),
    o!("mem-per-gpu", None, R),
    o!("mem-update", None, R),
    o!("mincpus", None, R),
    o!("network", None, R),
    o!("nice", None, O),
    o!("no-kill", Some('k'), O),
    o!("no-requeue", None, N),
    o!("nodefile", Some('F'), R),
    o!("nodelist", Some('w'), R),
    o!("nodes", Some('N'), R),
    o!("ntasks", Some('n'), R),
    o!("ntasks-per-core", None, R),
    o!("ntasks-per-node", None, R),
    o!("ntasks-per-socket", None, R),
    o!("ntasks-per-tres", None, R),
    o!("ntasks-per-gpu", None, R),
    o!("open-mode", None, R),
    o!("output", Some('o'), R),
    o!("overcommit", Some('O'), N),
    o!("oversubscribe", Some('s'), N),
    o!("parsable", None, N),
    o!("partition", Some('p'), R),
    o!("prefer", None, R),
    o!("priority", None, R),
    o!("profile", None, R),
    o!("propagate", None, O),
    o!("qos", Some('q'), R),
    o!("quiet", Some('Q'), N),
    o!("reboot", None, N),
    o!("requeue", None, O),
    o!("reservation", None, R),
    o!("resources", None, R),
    o!("resv-ports", None, O),
    o!("segment", None, R),
    o!("signal", None, R),
    o!("sockets-per-node", None, R),
    o!("spread-job", None, N),
    o!("spread-segments", None, N),
    o!("stepmgr", None, N),
    o!("switches", None, R),
    o!("tasks-per-node", None, R),
    o!("test-only", None, N),
    o!("thread-spec", None, R),
    o!("threads-per-core", None, R),
    o!("time", Some('t'), R),
    o!("time-min", None, R),
    o!("tmp", None, R),
    o!("uid", None, R),
    o!("usage", None, N),
    o!("use-min-nodes", None, N),
    o!("verbose", Some('v'), N),
    o!("version", Some('V'), N),
    o!("wait", Some('W'), N),
    o!("wait-all-nodes", None, R),
    o!("wckey", None, R),
    o!("wrap", None, R),
];

/// Options rightsizer may change, plus `--time` (recommend-only) and
/// `--tmp` (held): the "resource-bearing" directives the script digest
/// ignores (slurm.md §6).
pub const RESOURCE_OPTIONS: &[&str] = &[
    "mem",
    "mem-per-cpu",
    "mem-per-gpu",
    "cpus-per-task",
    "gres",
    "gpus",
    "gpus-per-node",
    "gpus-per-task",
    "gpus-per-socket",
    "cpus-per-gpu",
    "tres-per-task",
    "tmp",
    "time",
    "time-min",
];

/// The three mutually exclusive memory options.
pub const MEMORY_OPTIONS: &[&str] = &["mem", "mem-per-cpu", "mem-per-gpu"];

/// `SBATCH_*` input environment variables for resource options (sbatch
/// `opt.c` `env_vars`): they override the script and are overridden by argv.
pub const RESOURCE_ENV: &[(&str, &str)] = &[
    ("SBATCH_MEM_PER_NODE", "mem"),
    ("SBATCH_MEM_PER_CPU", "mem-per-cpu"),
    ("SBATCH_MEM_PER_GPU", "mem-per-gpu"),
    ("SBATCH_GRES", "gres"),
    ("SBATCH_GPUS", "gpus"),
    ("SBATCH_GPUS_PER_NODE", "gpus-per-node"),
    ("SBATCH_GPUS_PER_SOCKET", "gpus-per-socket"),
    ("SBATCH_GPUS_PER_TASK", "gpus-per-task"),
    ("SBATCH_CPUS_PER_GPU", "cpus-per-gpu"),
    ("SBATCH_TRES_PER_TASK", "tres-per-task"),
    ("SBATCH_TIMELIMIT", "time"),
    ("SBATCH_PARTITION", "partition"),
    ("SBATCH_ACCOUNT", "account"),
    ("SBATCH_JOB_NAME", "job-name"),
    ("SBATCH_ARRAY_INX", "array"),
    ("SBATCH_EXPORT", "export"),
    ("SBATCH_EXCLUSIVE", "exclusive"),
    ("SBATCH_QOS", "qos"),
];

/// Whether `name` is resource-bearing.
pub fn is_resource(name: &str) -> bool {
    RESOURCE_OPTIONS.contains(&name)
}

/// Look up a long option: exact match, else a unique prefix (getopt_long).
pub fn lookup_long(name: &str) -> Option<&'static OptSpec> {
    if let Some(o) = OPTIONS.iter().find(|o| o.long == name) {
        return Some(o);
    }
    let mut it = OPTIONS.iter().filter(|o| o.long.starts_with(name));
    match (it.next(), it.next()) {
        (Some(o), None) if !name.is_empty() => Some(o),
        _ => None,
    }
}

/// Look up a short option.
pub fn lookup_short(c: char) -> Option<&'static OptSpec> {
    OPTIONS.iter().find(|o| o.short == Some(c))
}

/// How an option value was written, so a rewrite keeps the user's form.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ArgForm {
    /// `--mem=64G` (value after `=` in the same token).
    LongEq,
    /// `--mem 64G` (value in the next token).
    LongSep,
    /// `-c8` (value attached to the short letter).
    ShortAttached,
    /// `-c 8`.
    ShortSep,
    /// A flag (no value).
    Flag,
}

/// One parsed option occurrence.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParsedOpt {
    /// Long name.
    pub name: &'static str,
    /// Value (`None` for flags and optional options given without one).
    pub value: Option<String>,
    /// Index of the token that holds the value (or the option, for flags).
    pub value_token: usize,
    /// Byte offset of the value inside that token's text.
    pub value_offset: usize,
    /// Form.
    pub form: ArgForm,
}

/// Result of parsing one argv-like token list.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ParsedArgs {
    /// Options, in order.
    pub opts: Vec<ParsedOpt>,
    /// Index of the first non-option token (the script), if any.
    pub first_positional: Option<usize>,
    /// Unknown options (kept; never an error here: sbatch itself reports).
    pub unknown: Vec<String>,
}

/// Parse options from `tokens[start..end]` the way `getopt_long` with a
/// leading `+` does: stop at the first non-option or after `--`.
pub fn parse_tokens(tokens: &[String], start: usize, end: usize) -> ParsedArgs {
    let mut out = ParsedArgs::default();
    let mut i = start;
    while i < end {
        let t = &tokens[i];
        if t == "--" {
            if i + 1 < end {
                out.first_positional = Some(i + 1);
            }
            return out;
        }
        if let Some(body) = t.strip_prefix("--") {
            let (name, eq) = match body.split_once('=') {
                Some((n, v)) => (n, Some(v)),
                None => (body, None),
            };
            match lookup_long(name) {
                Some(spec) => match (spec.arg, eq) {
                    (HasArg::No, _) => out.opts.push(ParsedOpt { name: spec.long, value: None, value_token: i, value_offset: 0, form: ArgForm::Flag }),
                    (_, Some(v)) => out.opts.push(ParsedOpt {
                        name: spec.long,
                        value: Some(v.to_string()),
                        value_token: i,
                        value_offset: 2 + name.len() + 1,
                        form: ArgForm::LongEq,
                    }),
                    (HasArg::Optional, None) => out.opts.push(ParsedOpt { name: spec.long, value: None, value_token: i, value_offset: 0, form: ArgForm::Flag }),
                    (HasArg::Required, None) => {
                        if i + 1 < end {
                            out.opts.push(ParsedOpt {
                                name: spec.long,
                                value: Some(tokens[i + 1].clone()),
                                value_token: i + 1,
                                value_offset: 0,
                                form: ArgForm::LongSep,
                            });
                            i += 1;
                        } else {
                            out.unknown.push(t.clone());
                        }
                    }
                },
                None => out.unknown.push(t.clone()),
            }
            i += 1;
            continue;
        }
        if t.len() > 1 && t.starts_with('-') {
            // A cluster of short options: flags, then at most one with an argument.
            let chars: Vec<(usize, char)> = t.char_indices().skip(1).collect();
            let mut j = 0;
            while j < chars.len() {
                let (off, c) = chars[j];
                let Some(spec) = lookup_short(c) else {
                    out.unknown.push(format!("-{c}"));
                    j += 1;
                    continue;
                };
                match spec.arg {
                    HasArg::No => {
                        out.opts.push(ParsedOpt { name: spec.long, value: None, value_token: i, value_offset: 0, form: ArgForm::Flag });
                        j += 1;
                    }
                    HasArg::Optional | HasArg::Required => {
                        let rest_off = off + c.len_utf8();
                        if rest_off < t.len() {
                            out.opts.push(ParsedOpt {
                                name: spec.long,
                                value: Some(t[rest_off..].to_string()),
                                value_token: i,
                                value_offset: rest_off,
                                form: ArgForm::ShortAttached,
                            });
                        } else if spec.arg == HasArg::Required && i + 1 < end {
                            out.opts.push(ParsedOpt {
                                name: spec.long,
                                value: Some(tokens[i + 1].clone()),
                                value_token: i + 1,
                                value_offset: 0,
                                form: ArgForm::ShortSep,
                            });
                            i += 1;
                        } else {
                            out.opts.push(ParsedOpt { name: spec.long, value: None, value_token: i, value_offset: 0, form: ArgForm::Flag });
                        }
                        break;
                    }
                }
            }
            i += 1;
            continue;
        }
        out.first_positional = Some(i);
        return out;
    }
    out
}

/// A token of an `#SBATCH` line, with its byte span in the line (sbatch's
/// `get_argument`: quotes group, `\` escapes, an unescaped `#` starts a
/// comment).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LineToken {
    /// Unquoted text.
    pub text: String,
    /// Raw span start in the line.
    pub start: usize,
    /// Raw span end in the line.
    pub end: usize,
    /// Whether the raw text had quotes or escapes (its spans differ from
    /// the text).
    pub raw: bool,
}

/// Tokenize the part of a directive line after the magic word.
pub fn tokenize_directive(line: &str, from: usize) -> Result<Vec<LineToken>, String> {
    let b = line.as_bytes();
    let mut out = Vec::new();
    let mut i = from;
    loop {
        while i < b.len() && (b[i] as char).is_ascii_whitespace() {
            i += 1;
        }
        if i >= b.len() || b[i] == b'#' {
            return Ok(out);
        }
        let start = i;
        let mut text = Vec::new();
        let mut quote: Option<u8> = None;
        let mut raw = false;
        while i < b.len() {
            let c = b[i];
            if let Some(q) = quote {
                if c == q {
                    quote = None;
                    i += 1;
                    continue;
                }
                text.push(c);
                i += 1;
                continue;
            }
            if (c as char).is_ascii_whitespace() || c == b'#' {
                break;
            }
            if c == b'\\' {
                raw = true;
                i += 1;
                if i < b.len() {
                    text.push(b[i]);
                    i += 1;
                }
                continue;
            }
            if c == b'"' || c == b'\'' {
                raw = true;
                quote = Some(c);
                i += 1;
                continue;
            }
            text.push(c);
            i += 1;
        }
        if quote.is_some() {
            return Err(format!("unmatched quote in [{line}]"));
        }
        out.push(LineToken { text: String::from_utf8_lossy(&text).into_owned(), start, end: i, raw });
        if i >= b.len() || b[i] == b'#' {
            return Ok(out);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn toks(s: &[&str]) -> Vec<String> {
        s.iter().map(|x| x.to_string()).collect()
    }

    #[test]
    fn long_and_short_forms() {
        let t = toks(&["--mem=64G", "-c", "8", "--job-name", "train", "-N2", "--exclusive", "--parsable", "train.sh", "--mem=1G"]);
        let p = parse_tokens(&t, 0, t.len());
        assert_eq!(p.first_positional, Some(8));
        let names: Vec<_> = p.opts.iter().map(|o| (o.name, o.value.as_deref(), o.form)).collect();
        assert_eq!(
            names,
            vec![
                ("mem", Some("64G"), ArgForm::LongEq),
                ("cpus-per-task", Some("8"), ArgForm::ShortSep),
                ("job-name", Some("train"), ArgForm::LongSep),
                ("nodes", Some("2"), ArgForm::ShortAttached),
                ("exclusive", None, ArgForm::Flag),
                ("parsable", None, ArgForm::Flag),
            ]
        );
        assert_eq!(p.opts[0].value_offset, 6);
        assert_eq!(p.opts[3].value_offset, 2);
    }

    #[test]
    fn short_clusters_prefixes_and_double_dash() {
        let t = toks(&["-Hc4", "--cpus-per=2", "--", "x.sh"]);
        let p = parse_tokens(&t, 0, t.len());
        assert_eq!(p.opts[0].name, "hold");
        assert_eq!((p.opts[1].name, p.opts[1].value.as_deref(), p.opts[1].value_offset), ("cpus-per-task", Some("4"), 3));
        // `--cpus-per` is ambiguous (cpus-per-task, cpus-per-gpu): unknown.
        assert_eq!(p.unknown, vec!["--cpus-per=2".to_string()]);
        assert_eq!(p.first_positional, Some(3));
        assert_eq!(lookup_long("partit").unwrap().long, "partition");
    }

    #[test]
    fn directive_tokens() {
        let l = "#SBATCH --mem=4G   -J \"my job\" # comment --mem=1G";
        let t = tokenize_directive(l, 7).unwrap();
        assert_eq!(t.iter().map(|x| x.text.as_str()).collect::<Vec<_>>(), vec!["--mem=4G", "-J", "my job"]);
        assert_eq!(&l[t[0].start..t[0].end], "--mem=4G");
        assert!(t[2].raw);
        assert!(tokenize_directive("#SBATCH -J 'x", 7).is_err());
        assert_eq!(tokenize_directive("#SBATCH a\\ b", 7).unwrap()[0].text, "a b");
    }
}
