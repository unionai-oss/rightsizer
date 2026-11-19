//! Code identity (SPEC §5.2): the normalized digest and [`CodeInfo`].
//!
//! The digest hashes the source after a language-aware normalization, so
//! edits that cannot change what the code does to memory (comments,
//! whitespace, resource directives) keep the digest, and therefore keep the
//! memo key (SPEC §6.3) and the size-scaled floor's same-digest history.
//!
//! What is stripped, by language:
//!
//! | language | stripped |
//! |---|---|
//! | Python | `#` comments outside string literals; leading/trailing whitespace on each line; runs of spaces/tabs outside strings collapsed to one space; blank lines; the argument list of every `Resources(...)` call; the values of resource keyword arguments (`cpu=`, `cpus=`, `memory=`, `mem=`, `gpu=`, `gpus=`, `num_cpus=`, `num_gpus=`, `ephemeral_storage=`, `accelerator=`, `shm=`) inside calls. String literals and docstrings are kept verbatim. |
//! | Shell / sbatch | comments (`#` at a word start, outside quotes), which includes `#!`, `#SBATCH`, `#PBS`, `#BSUB`, `#$` and `#FLUX:` directive lines; whitespace as for Python; on `srun`/`sbatch`/`salloc` lines the resource options `--mem*`, `--cpus-per-task`/`-c`, `--gres`, `--gpus*`, `--time`/`-t` and their values. |
//! | YAML | comments (`#` at a word start, outside quotes); trailing whitespace; blank lines; every `resources:` key with its nested block. Indentation is kept (it is YAML syntax). |
//! | other | full-line `#` and `//` comments; whitespace as for Python. |
//!
//! The language comes from the [`CodeRef`](rightsizer_core::CodeRef)
//! hint (`python`, `bash`/`sh`/`shell`/`sbatch`, `yaml`/`yml`) or, without
//! one, from [`detect_language`].

use rightsizer_core::CodeInfo;
use sha2::{Digest, Sha256};

/// Maximum source kept in [`CodeInfo::source`] (SPEC §5.2): 64 KiB.
pub const MAX_SOURCE_BYTES: usize = 64 * 1024;

/// Languages the normalizer understands.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum Language {
    /// Python.
    Python,
    /// POSIX shell, bash, sbatch scripts.
    Shell,
    /// YAML (manifests, pipeline definitions).
    Yaml,
    /// Anything else.
    Other,
}

impl Language {
    /// Parse a language hint (case-insensitive). Unknown hints → `None`.
    pub fn from_hint(hint: &str) -> Option<Language> {
        match hint.trim().to_ascii_lowercase().as_str() {
            "python" | "py" | "python3" => Some(Language::Python),
            "bash" | "sh" | "shell" | "zsh" | "sbatch" | "slurm" => Some(Language::Shell),
            "yaml" | "yml" => Some(Language::Yaml),
            "other" | "text" => Some(Language::Other),
            _ => None,
        }
    }

    /// Language from a file extension (`train.py`, `job.sbatch`, `x.yaml`).
    pub fn from_path(path: &str) -> Option<Language> {
        let ext = path.rsplit_once('.')?.1.to_ascii_lowercase();
        match ext.as_str() {
            "py" => Some(Language::Python),
            "sh" | "bash" | "sbatch" | "slurm" | "zsh" => Some(Language::Shell),
            "yaml" | "yml" => Some(Language::Yaml),
            _ => None,
        }
    }
}

/// Guess the language of `source`: a shebang decides; then `#SBATCH`/`#PBS`
/// lines mean shell; a leading `---` or an `apiVersion:`/`kind:` key means
/// YAML; anything else is treated as Python (whose normalizer is the most
/// general: string-aware `#` comments).
pub fn detect_language(source: &str) -> Language {
    let mut first = true;
    for line in source.lines() {
        let t = line.trim();
        if first && t.starts_with("#!") {
            if t.contains("python") {
                return Language::Python;
            }
            if t.contains("sh") {
                return Language::Shell;
            }
        }
        if t.is_empty() {
            continue;
        }
        first = false;
        if t.starts_with("#SBATCH") || t.starts_with("#PBS") || t.starts_with("#BSUB") || t.starts_with("#FLUX:") {
            return Language::Shell;
        }
        if line.starts_with("apiVersion:") || line.starts_with("kind:") {
            return Language::Yaml;
        }
    }
    if source.trim_start().starts_with("---") {
        return Language::Yaml;
    }
    Language::Python
}

/// Normalized code digest (SPEC §5.2), language auto-detected: `sha256:` +
/// hex of the normalized text. See the module docs for what is stripped.
pub fn code_digest(source: &str) -> String {
    code_digest_with(source, None)
}

/// [`code_digest`] with an explicit language (`None` = detect).
pub fn code_digest_with(source: &str, language: Option<Language>) -> String {
    let norm = normalize(source, language.unwrap_or_else(|| detect_language(source)));
    format!("sha256:{}", hex::encode(Sha256::digest(norm.as_bytes())))
}

/// The normalized text the digest hashes (exposed for debugging and tests).
pub fn normalize(source: &str, language: Language) -> String {
    match language {
        Language::Python => mask_python_resources(&join_lines(&strip_hash_comments(source, true))),
        Language::Shell => {
            let stripped = join_lines(&strip_hash_comments(source, false));
            stripped.lines().map(strip_shell_resource_options).filter(|l| !l.is_empty()).collect::<Vec<_>>().join("\n")
        }
        Language::Yaml => normalize_yaml(source),
        Language::Other => {
            let kept: Vec<String> = source
                .lines()
                .map(collapse_ws)
                .filter(|t| !t.is_empty() && !t.starts_with('#') && !t.starts_with("//"))
                .collect();
            kept.join("\n")
        }
    }
}

/// [`CodeInfo`] for `source`: digest, `features-v1` tokens and, when
/// `keep_source`, the source truncated to 64 KiB on a character boundary.
/// Tokens are computed on the full text (as the tuner does), the digest on
/// the full normalized text.
pub fn code_info(source: &str, language: Option<Language>, keep_source: bool) -> CodeInfo {
    CodeInfo {
        digest: code_digest_with(source, language),
        tokens: crate::tokens::code_tokens(source),
        source: keep_source.then(|| truncate_utf8(source, MAX_SOURCE_BYTES).to_string()),
    }
}

/// `s` truncated to at most `max` bytes, on a character boundary.
pub fn truncate_utf8(s: &str, max: usize) -> &str {
    if s.len() <= max {
        return s;
    }
    let mut end = max;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

fn collapse_ws(line: &str) -> String {
    line.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn join_lines(text: &str) -> String {
    text.lines().map(str::trim).filter(|l| !l.is_empty()).collect::<Vec<_>>().join("\n")
}

/// Remove `#` comments outside string literals and collapse whitespace runs
/// outside strings. `python`: `'`/`"` strings with triple-quote support and
/// backslash escapes, and `#` always starts a comment outside strings.
/// Otherwise (shell): `#` starts a comment only at the start of a word,
/// single quotes are literal and double quotes honour backslash escapes.
fn strip_hash_comments(src: &str, python: bool) -> String {
    let b: Vec<char> = src.chars().collect();
    let mut out = String::with_capacity(src.len());
    let mut i = 0;
    let n = b.len();
    while i < n {
        let c = b[i];
        if c == '#' && (python || i == 0 || b[i - 1].is_whitespace() || b[i - 1] == ';') {
            while i < n && b[i] != '\n' {
                i += 1;
            }
            continue;
        }
        if c == '\'' || c == '"' {
            let triple = python && i + 2 < n && b[i + 1] == c && b[i + 2] == c;
            let q = if triple { 3 } else { 1 };
            let start = i;
            i += q;
            loop {
                if i >= n {
                    break;
                }
                let d = b[i];
                if d == '\\' && (python || c == '"') {
                    i += 2;
                    continue;
                }
                if !triple && python && d == '\n' {
                    break; // unterminated single-line string
                }
                if d == c && (!triple || (i + 2 < n && b[i + 1] == c && b[i + 2] == c)) {
                    i += q;
                    break;
                }
                i += 1;
            }
            let end = i.min(n);
            out.extend(&b[start..end]);
            continue;
        }
        if c == ' ' || c == '\t' || c == '\r' || c == '\x0c' {
            while i < n && (b[i] == ' ' || b[i] == '\t' || b[i] == '\r' || b[i] == '\x0c') {
                i += 1;
            }
            out.push(' ');
            continue;
        }
        out.push(c);
        i += 1;
    }
    out
}

/// Resource keyword arguments whose values are masked in Python calls.
const PY_RESOURCE_KWARGS: [&str; 11] =
    ["cpu", "cpus", "memory", "mem", "gpu", "gpus", "num_cpus", "num_gpus", "ephemeral_storage", "accelerator", "shm"];

/// Mask `Resources(...)` argument lists and resource keyword values inside
/// calls, string-aware.
fn mask_python_resources(text: &str) -> String {
    let b: Vec<char> = text.chars().collect();
    let n = b.len();
    let mut out = String::with_capacity(text.len());
    let mut depth = 0usize;
    let mut i = 0;
    while i < n {
        let c = b[i];
        if c == '\'' || c == '"' {
            let end = skip_string(&b, i);
            out.extend(&b[i..end]);
            i = end;
            continue;
        }
        if is_ident_start(c) && (i == 0 || !is_ident_char(b[i - 1])) {
            let mut j = i;
            while j < n && is_ident_char(b[j]) {
                j += 1;
            }
            let ident: String = b[i..j].iter().collect();
            let mut k = j;
            while k < n && b[k] == ' ' {
                k += 1;
            }
            if ident == "Resources" && k < n && b[k] == '(' {
                let close = matching_close(&b, k);
                out.push_str("Resources(…)");
                i = close;
                continue;
            }
            if depth > 0
                && PY_RESOURCE_KWARGS.contains(&ident.as_str())
                && k < n
                && b[k] == '='
                && (k + 1 >= n || b[k + 1] != '=')
            {
                // Skip the value up to `,` or the closing bracket at this depth.
                let mut m = k + 1;
                let mut d = 0usize;
                while m < n {
                    let x = b[m];
                    if x == '\'' || x == '"' {
                        m = skip_string(&b, m);
                        continue;
                    }
                    match x {
                        '(' | '[' | '{' => d += 1,
                        ')' | ']' | '}' => {
                            if d == 0 {
                                break;
                            }
                            d -= 1;
                        }
                        ',' if d == 0 => break,
                        _ => {}
                    }
                    m += 1;
                }
                out.push_str(&ident);
                out.push_str("=…");
                i = m;
                continue;
            }
            out.push_str(&ident);
            i = j;
            continue;
        }
        match c {
            '(' | '[' | '{' => depth += 1,
            ')' | ']' | '}' => depth = depth.saturating_sub(1),
            _ => {}
        }
        out.push(c);
        i += 1;
    }
    out
}

fn is_ident_start(c: char) -> bool {
    c.is_alphabetic() || c == '_'
}
fn is_ident_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

/// Index just past the Python string literal starting at `i`.
fn skip_string(b: &[char], i: usize) -> usize {
    let n = b.len();
    let c = b[i];
    let triple = i + 2 < n && b[i + 1] == c && b[i + 2] == c;
    let q = if triple { 3 } else { 1 };
    let mut j = i + q;
    while j < n {
        if b[j] == '\\' {
            j += 2;
            continue;
        }
        if b[j] == c && (!triple || (j + 2 < n && b[j + 1] == c && b[j + 2] == c)) {
            return (j + q).min(n);
        }
        if !triple && b[j] == '\n' {
            return j;
        }
        j += 1;
    }
    n
}

/// Index just past the bracket matching the opener at `open`.
fn matching_close(b: &[char], open: usize) -> usize {
    let mut d = 0usize;
    let mut j = open;
    while j < b.len() {
        let x = b[j];
        if x == '\'' || x == '"' {
            j = skip_string(b, j);
            continue;
        }
        match x {
            '(' | '[' | '{' => d += 1,
            ')' | ']' | '}' => {
                d = d.saturating_sub(1);
                if d == 0 {
                    return j + 1;
                }
            }
            _ => {}
        }
        j += 1;
    }
    b.len()
}

/// Slurm resource options removed from `srun`/`sbatch`/`salloc` lines.
fn strip_shell_resource_options(line: &str) -> String {
    let words: Vec<&str> = line.split(' ').filter(|w| !w.is_empty()).collect();
    let launcher = words.iter().position(|w| matches!(*w, "srun" | "sbatch" | "salloc"));
    let Some(start) = launcher else { return words.join(" ") };
    const LONG: [&str; 9] =
        ["--mem", "--mem-per-cpu", "--mem-per-gpu", "--cpus-per-task", "--gres", "--gpus", "--gpus-per-node", "--gpus-per-task", "--time"];
    const SHORT: [&str; 3] = ["-c", "-t", "-G"];
    let mut out: Vec<&str> = words[..=start].to_vec();
    let mut i = start + 1;
    while i < words.len() {
        let w = words[i];
        let name = w.split_once('=').map(|(a, _)| a).unwrap_or(w);
        if LONG.contains(&name) {
            i += if w.contains('=') { 1 } else { 2 };
            continue;
        }
        if SHORT.contains(&w) {
            i += 2;
            continue;
        }
        if SHORT.iter().any(|s| w.len() > 2 && w.starts_with(s) && w[2..].chars().all(|c| c.is_ascii_alphanumeric() || c == ':')) {
            i += 1;
            continue;
        }
        out.push(w);
        i += 1;
    }
    out.join(" ")
}

fn normalize_yaml(source: &str) -> String {
    let mut out: Vec<String> = Vec::new();
    let mut skip_indent: Option<usize> = None;
    for raw in source.lines() {
        let line = strip_yaml_comment(raw).trim_end().to_string();
        if line.trim().is_empty() {
            continue;
        }
        let indent = line.len() - line.trim_start().len();
        if let Some(si) = skip_indent {
            if indent > si {
                continue;
            }
            skip_indent = None;
        }
        let body = line.trim_start();
        let key = body.strip_prefix("- ").unwrap_or(body);
        if key == "resources:" || key.starts_with("resources: ") || key.starts_with("resources:{") {
            // `resources:` with a nested block (or an inline flow mapping).
            skip_indent = Some(indent + (body.len() - key.len()));
            if body.len() != key.len() {
                // `- resources:` list item: keep the dash so the item remains.
                out.push(format!("{}-", " ".repeat(indent)));
            }
            continue;
        }
        out.push(line);
    }
    out.join("\n")
}

fn strip_yaml_comment(line: &str) -> &str {
    let b = line.as_bytes();
    let mut sq = false;
    let mut dq = false;
    for (i, &c) in b.iter().enumerate() {
        match c {
            b'\'' if !dq => sq = !sq,
            b'"' if !sq => dq = !dq,
            b'#' if !sq && !dq && (i == 0 || b[i - 1] == b' ' || b[i - 1] == b'\t') => return &line[..i],
            _ => {}
        }
    }
    line
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn python_strings_keep_hashes() {
        let a = "x = '#not a comment'  # a comment\n";
        assert_eq!(normalize(a, Language::Python), "x = '#not a comment'");
        let doc = "def f():\n    \"\"\"doc # inside\n    more\"\"\"\n    return 1\n";
        assert!(normalize(doc, Language::Python).contains("doc # inside"));
    }

    #[test]
    fn python_resources_masked() {
        let a = "env = TaskEnvironment(name='t', resources=Resources(cpu=2, memory='4Gi'))\n@ray.remote(num_cpus=4, memory=8 * 2**30)\ndef f(): pass\n";
        let b = "env = TaskEnvironment(name='t', resources=Resources(cpu=(1, 8), memory=\"16Gi\", gpu='A100:1'))\n@ray.remote(num_cpus=1, memory=2**30)\ndef f(): pass\n";
        assert_eq!(normalize(a, Language::Python), normalize(b, Language::Python));
        // `limits=` (threadpool_limits) and `memory_limit` are code, not resources.
        assert_ne!(code_digest("threadpool_limits(limits=2)\n"), code_digest("threadpool_limits(limits=8)\n"));
        assert_ne!(code_digest("con.execute(memory_limit='1GB')\n"), code_digest("con.execute(memory_limit='8GB')\n"));
        // A comparison is not a keyword argument.
        assert_ne!(code_digest("f(memory == 1)\n"), code_digest("f(memory == 2)\n"));
    }

    #[test]
    fn shell_directives_stripped() {
        let a = "#!/bin/bash\n#SBATCH --mem=4G\n#SBATCH -c 2\nsrun --mem=4G -c 2 python train.py --epochs 3\n";
        let b = "#!/bin/bash\n#SBATCH --mem=64G\n#SBATCH --cpus-per-task=16\n#SBATCH --gres=gpu:a100:2\n\nsrun --mem 64G --cpus-per-task=16 --gres=gpu:2  python   train.py --epochs 3   # go\n";
        assert_eq!(detect_language(a), Language::Shell);
        assert_eq!(code_digest(a), code_digest(b));
        assert_ne!(code_digest(a), code_digest(&a.replace("--epochs 3", "--epochs 30")));
        assert_eq!(normalize("echo '# kept' # dropped", Language::Shell), "echo '# kept'");
    }

    #[test]
    fn yaml_resources_stripped() {
        let a = "apiVersion: batch/v1\nkind: Job\nspec:\n  containers:\n  - name: main\n    image: x\n    resources:\n      requests:\n        memory: 4Gi\n    command: [python, a.py]\n";
        let b = "apiVersion: batch/v1\nkind: Job  # job\nspec:\n  containers:\n  - name: main\n    image: x\n    resources:\n      limits:\n        memory: 64Gi\n      requests:\n        memory: 32Gi\n        cpu: 8\n\n    command: [python, a.py]\n";
        assert_eq!(detect_language(a), Language::Yaml);
        assert_eq!(code_digest(a), code_digest(b));
        assert_ne!(code_digest(a), code_digest(&a.replace("a.py", "b.py")));
    }

    #[test]
    fn truncation_on_char_boundary() {
        let s = "é".repeat(40_000);
        let t = truncate_utf8(&s, MAX_SOURCE_BYTES);
        assert!(t.len() <= MAX_SOURCE_BYTES && t.len() >= MAX_SOURCE_BYTES - 1);
        let ci = code_info(&s, None, true);
        assert!(ci.source.unwrap().len() <= MAX_SOURCE_BYTES);
        assert!(code_info("x=1", None, false).source.is_none());
    }
}
