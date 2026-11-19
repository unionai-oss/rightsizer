//! Workload keys (slurm.md §6): tenant `slurm://{cluster}/{account}`; suffix
//! from `#RIGHTSIZER key=`, else an explicit `--job-name`, else the
//! normalized script digest.

use rightsizer_core::digest::sha256_hex;
use rightsizer_core::{Tenant, TypeError, WorkloadKey};

use crate::options::{ArgForm, is_resource, parse_tokens, tokenize_directive};
use crate::sbatch::{SbatchJob, directive_magic, is_command_line, lines_with_offsets};

/// Escape one tenant segment into the core's charset `[A-Za-z0-9._@+=,-]`:
/// every other byte becomes `=XX` (hex) and `=` itself becomes `=3d`, so the
/// mapping is reversible (`ml team` → `ml=20team`; `ml_team` is unchanged).
pub fn escape_segment(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || b"._@+,-".contains(&b) {
            out.push(b as char);
        } else {
            out.push_str(&format!("={b:02x}"));
        }
    }
    if out.is_empty() { "=".into() } else { out }
}

/// Reverse [`escape_segment`].
pub fn unescape_segment(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'='
            && i + 3 <= b.len()
            && let Some(Ok(v)) = s.get(i + 1..i + 3).map(|h| u8::from_str_radix(h, 16))
        {
            out.push(v);
            i += 3;
            continue;
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// `{cluster}/{account}` (account lower-cased, as Slurm stores it).
pub fn tenant(cluster: &str, account: &str) -> Result<Tenant, TypeError> {
    let acct = if account.is_empty() { "none".to_string() } else { account.to_ascii_lowercase() };
    Tenant::new(format!("{}/{}", escape_segment(cluster), escape_segment(&acct)))
}

/// A user-chosen name as a key suffix: whitespace and control characters
/// become `_`, `//` collapses, leading/trailing `/` are dropped, at most 200
/// bytes.
pub fn sanitize_suffix(s: &str) -> String {
    let mut out: String = s.trim().chars().map(|c| if c.is_whitespace() || c.is_control() { '_' } else { c }).collect();
    while out.contains("//") {
        out = out.replace("//", "/");
    }
    let mut out = out.trim_matches('/').to_string();
    if out.len() > 200 {
        let mut cut = 200;
        while !out.is_char_boundary(cut) {
            cut -= 1;
        }
        out.truncate(cut);
        out = out.trim_end_matches('/').to_string();
    }
    if out.is_empty() { "_".into() } else { out }
}

/// The normalized script digest (hex): full-line comments and blank lines
/// removed, each line trimmed; in the leading block, resource-bearing
/// `#SBATCH` options are dropped and the rest are rewritten as
/// `#SBATCH --name=value` (so `-J x` and `--job-name=x` agree); the shebang
/// is kept. For `--wrap` jobs the command is digested after replacing
/// timestamp- and run-id-like tokens with placeholders.
pub fn script_digest(job: &SbatchJob) -> String {
    if let Some(w) = job.components.first().and_then(|c| c.value("wrap")) {
        return sha256_hex(format!("wrap\n{}", placeholder_numbers(w.trim())).as_bytes());
    }
    sha256_hex(normalized_script(job.script.as_deref().unwrap_or("")).as_bytes())
}

/// The normalized text [`script_digest`] hashes.
pub fn normalized_script(script: &str) -> String {
    let mut out: Vec<String> = Vec::new();
    let mut in_block = true;
    for (i, (_, line)) in lines_with_offsets(script).into_iter().enumerate() {
        let t = line.trim();
        if i == 0 && t.starts_with("#!") {
            out.push(t.split_whitespace().collect::<Vec<_>>().join(" "));
            continue;
        }
        if in_block && let Some(m) = directive_magic(line) {
            let rest = line[m..].trim_start();
            let mut from = m + (line[m..].len() - rest.len());
            for w in ["hetjob", "packjob"] {
                if rest.len() >= w.len() && rest[..w.len()].eq_ignore_ascii_case(w) && rest[w.len()..].chars().next().is_none_or(|c| c.is_ascii_whitespace()) {
                    out.push("#SBATCH hetjob".into());
                    from += w.len();
                }
            }
            if let Ok(toks) = tokenize_directive(line, from) {
                let texts: Vec<String> = toks.into_iter().map(|t| t.text).collect();
                for o in parse_tokens(&texts, 0, texts.len()).opts {
                    if is_resource(o.name) {
                        continue;
                    }
                    out.push(match (o.form, o.value) {
                        (ArgForm::Flag, _) | (_, None) => format!("#SBATCH --{}", o.name),
                        (_, Some(v)) => format!("#SBATCH --{}={v}", o.name),
                    });
                }
            }
            continue;
        }
        if is_command_line(line) {
            in_block = false;
        }
        if t.is_empty() || t.starts_with('#') {
            continue;
        }
        out.push(t.to_string());
    }
    out.join("\n")
}

/// Replace tokens that vary per run (timestamps, dates, times, UUIDs, long
/// digit runs, long hex ids) with placeholders.
pub fn placeholder_numbers(s: &str) -> String {
    let b: Vec<char> = s.chars().collect();
    let mut out = String::with_capacity(s.len());
    let mut i = 0;
    let is_hex = |c: char| c.is_ascii_hexdigit();
    while i < b.len() {
        // A maximal run of [0-9A-Za-z-:] that we classify as a whole.
        if b[i].is_ascii_alphanumeric() {
            let mut j = i;
            while j < b.len() && (b[j].is_ascii_alphanumeric() || b[j] == '-' || b[j] == ':' || b[j] == '.' || b[j] == '_') {
                j += 1;
            }
            let word: String = b[i..j].iter().collect();
            out.push_str(&placeholder_word(&word, is_hex));
            i = j;
            continue;
        }
        out.push(b[i]);
        i += 1;
    }
    out
}

fn placeholder_word(word: &str, is_hex: impl Fn(char) -> bool + Copy) -> String {
    // Split on separators, classify each piece, and keep separators.
    let uuid = |w: &str| {
        let p: Vec<&str> = w.split('-').collect();
        p.len() == 5 && p.iter().zip([8, 4, 4, 4, 12]).all(|(x, n)| x.len() == n && x.chars().all(is_hex))
    };
    if uuid(word) {
        return "<uuid>".into();
    }
    let mut out = String::new();
    let mut piece = String::new();
    let flush = |piece: &mut String, out: &mut String| {
        if piece.is_empty() {
            return;
        }
        let digits = piece.chars().filter(char::is_ascii_digit).count();
        let all_digits = digits == piece.len();
        let hexy = piece.len() >= 12 && piece.chars().all(is_hex) && digits > 0 && digits < piece.len();
        if (all_digits && piece.len() >= 6) || hexy {
            out.push_str("<n>");
        } else {
            out.push_str(piece);
        }
        piece.clear();
    };
    for c in word.chars() {
        if c == '-' || c == ':' || c == '.' || c == '_' {
            flush(&mut piece, &mut out);
            out.push(c);
        } else {
            piece.push(c);
        }
    }
    flush(&mut piece, &mut out);
    // Dates and times: 2026-11-19, 12:34:56, 2026-11-19T12:34:56.
    let date_like = |s: &str| {
        let parts: Vec<&str> = s.split(['-', ':', 'T', '.']).collect();
        parts.len() >= 3 && parts.iter().all(|p| !p.is_empty() && p.len() <= 4 && p.chars().all(|c| c.is_ascii_digit()))
    };
    if date_like(word) {
        return "<ts>".into();
    }
    out
}

/// Where the suffix came from.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum SuffixSource {
    /// `#RIGHTSIZER key=`.
    Directive,
    /// `--job-name` given explicitly (script directive or argv).
    JobName,
    /// The normalized script digest.
    Digest,
}

/// The key suffix of component `k` (het components append `/het-{k}`).
pub fn key_suffix(job: &SbatchJob, k: usize) -> (String, SuffixSource) {
    key_suffix_het(job, k, job.components.len() > 1)
}

/// The key suffix of the component whose het offset is `k`.
pub fn key_suffix_het(job: &SbatchJob, k: usize, het: bool) -> (String, SuffixSource) {
    let comp = job.components.iter().find(|c| c.index == k);
    let first = job.components.iter().find(|c| c.index == 0);
    let (base, src) = if let Some(key) = &job.rightsizer.key {
        (sanitize_suffix(key), SuffixSource::Directive)
    } else if let Some(name) = comp
        .and_then(|c| c.value("job-name"))
        .or_else(|| first.and_then(|c| c.value("job-name")))
        .filter(|n| !n.trim().is_empty())
    {
        (sanitize_suffix(name), SuffixSource::JobName)
    } else {
        (format!("sha256-{}", &script_digest(job)[..32]), SuffixSource::Digest)
    };
    if het { (format!("{base}/het-{k}"), src) } else { (base, src) }
}

/// The full key of component `k`.
pub fn workload_key(cluster: &str, account: &str, job: &SbatchJob, k: usize) -> Result<WorkloadKey, TypeError> {
    WorkloadKey::new("slurm", tenant(cluster, account)?, key_suffix(job, k).0)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::sbatch::parse;

    fn job(argv: &str, script: Option<&str>) -> SbatchJob {
        parse(&shlex::split(argv).unwrap(), script, &BTreeMap::new()).unwrap()
    }

    #[test]
    fn tenant_escaping() {
        assert_eq!(tenant("gpu-a", "MLteam").unwrap().as_str(), "gpu-a/mlteam");
        assert_eq!(tenant("gpu_a", "ml_team").unwrap().as_str(), "gpu_a/ml_team");
        assert_eq!(tenant("c", "a=b/c").unwrap().as_str(), "c/a=3db=2fc");
        assert_eq!(unescape_segment("a=3db=2fc"), "a=b/c");
        assert_eq!(unescape_segment(&escape_segment("a=b c")), "a=b c");
        assert_eq!(tenant("c", "").unwrap().as_str(), "c/none");
        let k = workload_key("gpu_a", "ml_team", &job("-J 'my train' x.sh", Some("#!/bin/sh\n")), 0).unwrap();
        assert_eq!(k.to_string(), "slurm://gpu_a/ml_team/my_train");
        assert_eq!(WorkloadKey::parse(&k.to_string()).unwrap(), k);
    }

    #[test]
    fn suffix_order() {
        let s = "#!/bin/bash\n#RIGHTSIZER key=train-small\n#SBATCH -J named\necho\n";
        assert_eq!(key_suffix(&job("x.sh", Some(s)), 0), ("train-small".into(), SuffixSource::Directive));
        let s = "#!/bin/bash\n#SBATCH -J named\necho\n";
        assert_eq!(key_suffix(&job("x.sh", Some(s)), 0), ("named".into(), SuffixSource::JobName));
        assert_eq!(key_suffix(&job("--job-name=argv x.sh", Some(s)), 0).0, "argv");
        let (d, src) = key_suffix(&job("x.sh", Some("#!/bin/bash\necho\n")), 0);
        assert_eq!(src, SuffixSource::Digest);
        assert!(d.starts_with("sha256-") && d.len() == 39);
        let het = job("x.sh", Some("#!/bin/bash\n#SBATCH -J h\n#SBATCH hetjob\n#SBATCH -c 2\nsrun x\n"));
        assert_eq!(key_suffix(&het, 0).0, "h/het-0");
        assert_eq!(key_suffix(&het, 1).0, "h/het-1");
        assert_eq!(sanitize_suffix(" /a//b c/ "), "a/b_c");
    }

    #[test]
    fn digest_ignores_resources_comments_and_form() {
        let a = "#!/bin/bash\n#SBATCH --mem=64G\n#SBATCH -c 8\n#SBATCH -o out.log\n# train it\n\npython train.py --lr 0.1\n";
        let b = "#!/bin/bash\n#SBATCH --output=out.log\n#SBATCH --mem=24G --time=1-00:00:00\n#SBATCH --cpus-per-task=4\n  python train.py --lr 0.1   \n# trailing\n";
        assert_eq!(script_digest(&job("x.sh", Some(a))), script_digest(&job("x.sh", Some(b))));
        let c = "#!/bin/bash\n#SBATCH -o other.log\npython train.py --lr 0.1\n";
        assert_ne!(script_digest(&job("x.sh", Some(a))), script_digest(&job("x.sh", Some(c))));
        let d = "#!/bin/bash\n#SBATCH -o out.log\npython train.py --lr 0.2\n";
        assert_ne!(script_digest(&job("x.sh", Some(a))), script_digest(&job("x.sh", Some(d))));
        assert_eq!(normalized_script(a), "#!/bin/bash\n#SBATCH --output=out.log\npython train.py --lr 0.1");
    }

    #[test]
    fn wrap_placeholders() {
        let a = job("--wrap 'python run.py --run-id 1731990000 --date 2026-11-19 --out /tmp/r-3f2a9c1b7d4e5f60'", None);
        let b = job("--wrap 'python run.py --run-id 1731999999 --date 2026-12-01 --out /tmp/r-aaaa9c1b7d4e5f61'", None);
        assert_eq!(script_digest(&a), script_digest(&b));
        let c = job("--wrap 'python run.py --epochs 3'", None);
        let d = job("--wrap 'python run.py --epochs 4'", None);
        assert_ne!(script_digest(&c), script_digest(&d));
        assert_eq!(placeholder_numbers("x 123456 at 12:30:01 id 550e8400-e29b-41d4-a716-446655440000"), "x <n> at <ts> id <uuid>");
    }
}
