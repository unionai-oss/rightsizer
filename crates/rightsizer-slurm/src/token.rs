//! Signed attribution tokens (slurm.md §4.1) and their signing keys
//! (SPEC §13.2: key ids, two keys active during rotation).
//!
//! # Wire format (v1)
//!
//! ```text
//! token  = base64url-nopad( 0x01 ‖ len(kid):u8 ‖ kid ‖ decision_id:26 ASCII ‖ expiry:u64 BE ‖ mac:32 )
//! mac    = HMAC-SHA256(key, msg)
//! msg    = "rightsizer-token-v1" NUL kid NUL uid NUL account NUL sha256hex(script)
//!          NUL canonical(resources) NUL decision_id NUL expiry
//! ```
//!
//! Fields of `msg` are separated by one NUL byte. No field can contain NUL
//! (Slurm's `job_desc` strings are C strings; every other field is ASCII from
//! a fixed alphabet) and the field count is fixed, so the encoding is
//! unambiguous. `uid` and `expiry` are unsigned decimal without leading
//! zeros, `account` is lower-cased (Slurm lower-cases account names) and is
//! `job_desc.account`, else the user's default account, else empty;
//! `sha256hex(script)` is lower-case hex of `job_desc.script` (the exact
//! bytes `sbatch` sent: the script file, or the `--wrap` script).
//!
//! # `canonical(resources)`
//!
//! Exactly the resource fields `job_submit.lua` can read from `job_desc`
//! (never `pn_min_memory`, which Lua reads as a lossy float), one
//! `name=value` per line in this order, joined by `\n` without a trailing
//! newline; `-` stands for "not set":
//!
//! | line | `job_desc` field | unset when | from `sbatch` |
//! |---|---|---|---|
//! | `mem_per_node` | `min_mem_per_node` (MiB) | nil | `--mem` (MiB, rounded up; `--mem=0` is `0`) |
//! | `mem_per_cpu` | `min_mem_per_cpu` (MiB) | nil | `--mem-per-cpu` |
//! | `cpus_per_task` | `cpus_per_task` | nil, 0 or `NO_VAL16` (65534) | `-c/--cpus-per-task` |
//! | `tres_per_node` | `tres_per_node` | nil or `""` | `--gres` (each entry prefixed `gres/`), `--gpus-per-node` (`gres/gpu[:type]:n`) |
//! | `tres_per_job` | `tres_per_job` | nil or `""` | `--gpus` (`gres/gpu[:type]:n`) |
//! | `tres_per_task` | `tres_per_task` | nil or `""` | `--gpus-per-task` (`gres/gpu[:type]=n`), `--tres-per-task`; `-c n` adds `cpu=n` |
//! | `tres_per_socket` | `tres_per_socket` | nil or `""` | `--gpus-per-socket` |
//! | `mem_per_tres` | `mem_per_tres` | nil or `""` | `--mem-per-gpu` (`gres/gpu:<MiB>`) |
//! | `tmp_disk` | `pn_min_tmp_disk` (MiB) | nil or `NO_VAL` (4294967294) | `--tmp` |
//!
//! Integers are formatted as unsigned decimal (Lua sees every number as a
//! float: `100.0` is written `100`). TRES strings are normalized by
//! [`normalize_tres`] so that Slurm's spellings (`gres/gpu:a100:2`,
//! `gres:gpu:2`, `gres/gpu=1`) compare equal: entries are lower-cased,
//! rewritten to `kind/name[:type]=count` (`name=count` for plain TRES), `cpu`
//! entries are dropped (they duplicate `cpus_per_task`), and the entries are
//! sorted. The mapping was observed on Slurm 26.05.5 (`job_submit.lua` dump,
//! docs/IMPLEMENTATION-NOTES.md "rightsizer-slurm").
//!
//! # Key file
//!
//! One key per line, `#` comments allowed:
//!
//! ```text
//! rsz-key-v1 <kid> <active|verify> <created-unix> <base64url-nopad 32-byte secret>
//! ```
//!
//! Exactly one key is `active` (the daemon signs with it); at most two keys
//! are listed; both verify. Rotation ([`KeyRing::rotate`]) is three steps so
//! that `job_submit.lua` always knows a key before the daemon signs with it:
//! `stage` (add a new `verify` key; install the file on the controller and
//! the login nodes), `promote` (the new key signs, the old one still
//! verifies tokens already issued), `retire` (drop the old key once every
//! token it signed has expired).

use std::fmt;
use std::io::Read;
use std::path::Path;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use hmac::{Hmac, KeyInit, Mac};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use ulid::Ulid;

use rightsizer_core::units::parse_slurm_mem;

/// Environment variable carrying the token (`RIGHTSIZER_TOKEN_<offset>` for
/// heterogeneous job components is checked first).
pub const TOKEN_ENV: &str = "RIGHTSIZER_TOKEN";
/// Domain-separation tag, the first field of the MAC message.
pub const MAC_DOMAIN: &str = "rightsizer-token-v1";
/// Token format version byte.
pub const TOKEN_VERSION: u8 = 1;
/// Key file line tag.
pub const KEY_LINE_TAG: &str = "rsz-key-v1";
/// Default token lifetime, seconds.
pub const DEFAULT_TTL_S: u64 = 600;
/// Default clock skew tolerated by verification, seconds.
pub const DEFAULT_SKEW_S: u64 = 120;

type HmacSha256 = Hmac<Sha256>;

/// Token errors.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum TokenError {
    /// Malformed token.
    #[error("malformed token: {0}")]
    Malformed(&'static str),
    /// Malformed key file.
    #[error("key file: {0}")]
    KeyFile(String),
    /// No usable key.
    #[error("no key: {0}")]
    NoKey(String),
    /// I/O.
    #[error("io: {0}")]
    Io(String),
}

/// The resource fields of `job_desc` that a token covers (see the module
/// docs for units and the `sbatch` mapping).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobDescResources {
    /// `min_mem_per_node`, MiB.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_mem_per_node: Option<u64>,
    /// `min_mem_per_cpu`, MiB.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_mem_per_cpu: Option<u64>,
    /// `cpus_per_task`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpus_per_task: Option<u32>,
    /// `tres_per_node`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tres_per_node: Option<String>,
    /// `tres_per_job`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tres_per_job: Option<String>,
    /// `tres_per_task`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tres_per_task: Option<String>,
    /// `tres_per_socket`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tres_per_socket: Option<String>,
    /// `mem_per_tres`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mem_per_tres: Option<String>,
    /// `pn_min_tmp_disk`, MiB.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pn_min_tmp_disk: Option<u64>,
}

fn opt_u64(v: Option<u64>) -> String {
    v.map_or_else(|| "-".to_string(), |n| n.to_string())
}

impl JobDescResources {
    /// `canonical(resources)` (module docs).
    pub fn canonical(&self) -> String {
        let cpt = match self.cpus_per_task {
            None | Some(0) | Some(65534) => "-".to_string(),
            Some(n) => n.to_string(),
        };
        let tmp = match self.pn_min_tmp_disk {
            None | Some(4_294_967_294) => "-".to_string(),
            Some(n) => n.to_string(),
        };
        [
            format!("mem_per_node={}", opt_u64(self.min_mem_per_node)),
            format!("mem_per_cpu={}", opt_u64(self.min_mem_per_cpu)),
            format!("cpus_per_task={cpt}"),
            format!("tres_per_node={}", normalize_tres(self.tres_per_node.as_deref())),
            format!("tres_per_job={}", normalize_tres(self.tres_per_job.as_deref())),
            format!("tres_per_task={}", normalize_tres(self.tres_per_task.as_deref())),
            format!("tres_per_socket={}", normalize_tres(self.tres_per_socket.as_deref())),
            format!("mem_per_tres={}", normalize_tres(self.mem_per_tres.as_deref())),
            format!("tmp_disk={tmp}"),
        ]
        .join("\n")
    }

    /// What Slurm puts into `job_desc` for the effective (merged) `sbatch`
    /// options of one job component, given as long option name → value
    /// (`"mem" → "64G"`, `"cpus-per-task" → "8"`, `"gres" → "gpu:a100:2"`).
    /// Unparseable values are kept out (the token then fails to verify,
    /// which is the safe direction: the job is attributed to the user).
    pub fn from_options<'a, F>(get: F) -> Self
    where
        F: Fn(&str) -> Option<&'a str>,
    {
        let mib = |s: &str| parse_slurm_mem(s).ok().map(|b| b.0.div_ceil(1 << 20));
        let mut r = JobDescResources::default();
        if let Some(v) = get("mem") {
            r.min_mem_per_node = mib(v);
        }
        if let Some(v) = get("mem-per-cpu") {
            r.min_mem_per_cpu = mib(v);
        }
        if let Some(v) = get("mem-per-gpu")
            && let Some(m) = mib(v)
        {
            r.mem_per_tres = Some(format!("gres/gpu:{m}"));
        }
        if let Some(v) = get("cpus-per-task")
            && let Ok(n) = v.trim().parse::<u32>()
            && n > 0
        {
            r.cpus_per_task = Some(n);
        }
        let mut per_node: Vec<String> = Vec::new();
        if let Some(v) = get("gres") {
            for e in v.split(',').map(str::trim).filter(|e| !e.is_empty() && !e.eq_ignore_ascii_case("none")) {
                per_node.push(if e.starts_with("gres/") || e.starts_with("gres:") { e.to_string() } else { format!("gres/{e}") });
            }
        }
        if let Some(v) = get("gpus-per-node") {
            per_node.push(format!("gres/gpu:{}", v.trim()));
        }
        if !per_node.is_empty() {
            r.tres_per_node = Some(per_node.join(","));
        }
        if let Some(v) = get("gpus") {
            r.tres_per_job = Some(format!("gres/gpu:{}", v.trim()));
        }
        let mut per_task: Vec<String> = Vec::new();
        if let Some(v) = get("tres-per-task") {
            per_task.push(v.trim().to_string());
        }
        if let Some(v) = get("gpus-per-task") {
            per_task.push(format!("gres/gpu:{}", v.trim()));
        }
        if !per_task.is_empty() {
            r.tres_per_task = Some(per_task.join(","));
        }
        if let Some(v) = get("gpus-per-socket") {
            r.tres_per_socket = Some(format!("gres/gpu:{}", v.trim()));
        }
        if let Some(v) = get("tmp") {
            r.pn_min_tmp_disk = mib(v);
        }
        r
    }
}

fn is_count(s: &str) -> bool {
    let b = s.as_bytes();
    let digits = b.iter().take_while(|c| c.is_ascii_digit()).count();
    digits > 0 && (digits == b.len() || (digits + 1 == b.len() && b"kmgtp".contains(&b[digits])))
}

/// Normalize a TRES list string (module docs): `None`/empty → `-`.
pub fn normalize_tres(s: Option<&str>) -> String {
    let Some(s) = s else { return "-".into() };
    let mut out: Vec<String> = Vec::new();
    for raw in s.split(',') {
        let e = raw.trim().to_ascii_lowercase();
        if e.is_empty() {
            continue;
        }
        // kind: "gres/x", "gres:x" (pre-21.08), "license/x", or any "k/x".
        let (kind, rest) = if let Some(r) = e.strip_prefix("gres/").or_else(|| e.strip_prefix("gres:")) {
            ("gres".to_string(), r.to_string())
        } else if let Some(r) = e.strip_prefix("license/").or_else(|| e.strip_prefix("license:")) {
            ("license".to_string(), r.to_string())
        } else if let Some((k, r)) = e.split_once('/') {
            (k.to_string(), r.to_string())
        } else {
            let name = e.split([':', '=']).next().unwrap_or("");
            if matches!(name, "cpu" | "mem" | "node" | "billing" | "energy") {
                (String::new(), e.clone())
            } else {
                ("gres".to_string(), e.clone())
            }
        };
        let (left, count) = if let Some((l, c)) = rest.rsplit_once('=') {
            (l.to_string(), c.to_string())
        } else if let Some((l, c)) = rest.rsplit_once(':')
            && is_count(c)
        {
            (l.to_string(), c.to_string())
        } else {
            (rest.clone(), "1".to_string())
        };
        let mut parts = left.splitn(2, ':');
        let name = parts.next().unwrap_or("").to_string();
        let ty = parts.next().map(str::to_string);
        if kind.is_empty() && name == "cpu" {
            continue;
        }
        let mut n = String::new();
        if !kind.is_empty() {
            n.push_str(&kind);
            n.push('/');
        }
        n.push_str(&name);
        if let Some(t) = ty
            && !t.is_empty()
        {
            n.push(':');
            n.push_str(&t);
        }
        n.push('=');
        n.push_str(&count);
        out.push(n);
    }
    if out.is_empty() {
        return "-".into();
    }
    out.sort();
    out.join(",")
}

/// Lower-case hex SHA-256 of a script.
pub fn script_sha256(script: &[u8]) -> String {
    hex::encode(Sha256::digest(script))
}

/// The script `sbatch --wrap CMD` sends (sbatch.c `_script_wrap`).
pub fn wrap_script(cmd: &str) -> String {
    format!("#!/bin/sh\n# This script was created by sbatch --wrap.\n\n{cmd}\n")
}

/// What a token attests, besides its key id.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenClaims {
    /// Submitting uid (`submit_uid`).
    pub uid: u32,
    /// Account (lower-cased when the MAC is computed).
    pub account: String,
    /// `sha256hex(job_desc.script)`.
    pub script_sha256: String,
    /// The resources, as `job_desc` will carry them.
    pub resources: JobDescResources,
    /// Decision id.
    pub decision_id: Ulid,
    /// Expiry, unix seconds.
    pub expiry: u64,
}

/// The MAC message (module docs).
pub fn mac_message(kid: &str, c: &TokenClaims) -> Vec<u8> {
    let fields = [
        MAC_DOMAIN.to_string(),
        kid.to_string(),
        c.uid.to_string(),
        c.account.to_ascii_lowercase(),
        c.script_sha256.to_ascii_lowercase(),
        c.resources.canonical(),
        c.decision_id.to_string(),
        c.expiry.to_string(),
    ];
    fields.join("\0").into_bytes()
}

/// A decoded (not yet verified) token.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParsedToken {
    /// Key id.
    pub kid: String,
    /// Decision id.
    pub decision_id: Ulid,
    /// Expiry, unix seconds.
    pub expiry: u64,
    /// MAC.
    pub mac: [u8; 32],
}

fn valid_kid(k: &str) -> bool {
    !k.is_empty() && k.len() <= 32 && k.bytes().all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
}

/// Decode a token.
pub fn parse_token(token: &str) -> Result<ParsedToken, TokenError> {
    let raw = URL_SAFE_NO_PAD.decode(token.trim()).map_err(|_| TokenError::Malformed("base64"))?;
    if raw.len() < 2 || raw[0] != TOKEN_VERSION {
        return Err(TokenError::Malformed("version"));
    }
    let k = raw[1] as usize;
    if raw.len() != 2 + k + 26 + 8 + 32 {
        return Err(TokenError::Malformed("length"));
    }
    let kid = std::str::from_utf8(&raw[2..2 + k]).map_err(|_| TokenError::Malformed("kid"))?.to_string();
    if !valid_kid(&kid) {
        return Err(TokenError::Malformed("kid"));
    }
    let id = std::str::from_utf8(&raw[2 + k..2 + k + 26]).map_err(|_| TokenError::Malformed("decision id"))?;
    let decision_id = Ulid::from_string(id).map_err(|_| TokenError::Malformed("decision id"))?;
    let mut e = [0u8; 8];
    e.copy_from_slice(&raw[2 + k + 26..2 + k + 34]);
    let mut mac = [0u8; 32];
    mac.copy_from_slice(&raw[2 + k + 34..]);
    Ok(ParsedToken { kid, decision_id, expiry: u64::from_be_bytes(e), mac })
}

/// A signing key.
#[derive(Clone, PartialEq, Eq)]
pub struct SigningKey {
    /// Key id (`[A-Za-z0-9._-]{1,32}`).
    pub kid: String,
    /// Whether it signs (`Active`) or only verifies.
    pub state: KeyState,
    /// Created, unix seconds.
    pub created: u64,
    /// The secret (32 bytes from `/dev/urandom` for generated keys).
    pub secret: Vec<u8>,
}

impl fmt::Debug for SigningKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SigningKey").field("kid", &self.kid).field("state", &self.state).field("created", &self.created).finish_non_exhaustive()
    }
}

/// Key state.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KeyState {
    /// Signs and verifies.
    Active,
    /// Verifies only (staged for promotion, or retiring).
    Verify,
}

impl SigningKey {
    /// Sign claims into a token.
    pub fn sign(&self, c: &TokenClaims) -> String {
        let mut mac = HmacSha256::new_from_slice(&self.secret).expect("hmac accepts any key length");
        mac.update(&mac_message(&self.kid, c));
        let tag = mac.finalize().into_bytes();
        let mut raw = Vec::with_capacity(2 + self.kid.len() + 26 + 8 + 32);
        raw.push(TOKEN_VERSION);
        raw.push(self.kid.len() as u8);
        raw.extend_from_slice(self.kid.as_bytes());
        raw.extend_from_slice(c.decision_id.to_string().as_bytes());
        raw.extend_from_slice(&c.expiry.to_be_bytes());
        raw.extend_from_slice(&tag);
        URL_SAFE_NO_PAD.encode(raw)
    }

    fn verify_mac(&self, msg: &[u8], tag: &[u8]) -> bool {
        let mut mac = HmacSha256::new_from_slice(&self.secret).expect("hmac accepts any key length");
        mac.update(msg);
        mac.verify_slice(tag).is_ok() // constant time
    }
}

/// What the verifier knows about the job (job_submit's view).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerifyInput<'a> {
    /// `submit_uid`.
    pub uid: u32,
    /// Account (`job_desc.account`, else the default account, else empty).
    pub account: &'a str,
    /// `sha256hex(job_desc.script)`.
    pub script_sha256: &'a str,
    /// The job's actual resource fields.
    pub resources: &'a JobDescResources,
    /// Now, unix seconds.
    pub now: u64,
    /// Tolerated clock skew, seconds.
    pub skew_s: u64,
}

/// Verification outcome; maps to `rightsizer:<id>:verified` / `:user`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Verification {
    /// The resources are rightsizer's decision.
    Verified {
        /// Decision id.
        decision_id: Ulid,
    },
    /// Absent, malformed, expired, unknown key or wrong MAC.
    User {
        /// The decision id, when the token could be decoded.
        decision_id: Option<Ulid>,
        /// Why.
        reason: &'static str,
    },
}

impl Verification {
    /// The `admin_comment` value job_submit.lua writes.
    pub fn admin_comment(&self) -> String {
        match self {
            Verification::Verified { decision_id } => format!("rightsizer:{decision_id}:verified"),
            Verification::User { decision_id: Some(id), .. } => format!("rightsizer:{id}:user"),
            Verification::User { decision_id: None, .. } => "rightsizer::user".to_string(),
        }
    }
}

/// The key ring: one active key, at most one more verify-only key.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct KeyRing {
    /// Keys, active first.
    pub keys: Vec<SigningKey>,
}

/// One rotation step (`rsz slurm rotate-key --stage|--promote|--retire`).
#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RotateStep {
    /// Add a new verify-only key.
    Stage,
    /// The staged key signs; the old active key verifies only.
    Promote,
    /// Drop verify-only keys.
    Retire,
}

fn random_bytes(n: usize) -> Result<Vec<u8>, TokenError> {
    let mut f = std::fs::File::open("/dev/urandom").map_err(|e| TokenError::Io(e.to_string()))?;
    let mut b = vec![0u8; n];
    f.read_exact(&mut b).map_err(|e| TokenError::Io(e.to_string()))?;
    Ok(b)
}

fn now_s() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

impl KeyRing {
    /// Generate a key: `k<created>-<4 hex>`, 32 random bytes.
    pub fn generate_key(state: KeyState) -> Result<SigningKey, TokenError> {
        let created = now_s();
        let tag = random_bytes(2)?;
        Ok(SigningKey { kid: format!("k{created}-{}", hex::encode(tag)), state, created, secret: random_bytes(32)? })
    }

    /// `rsz slurm keygen`: a ring with one fresh active key.
    pub fn generate() -> Result<Self, TokenError> {
        Ok(KeyRing { keys: vec![Self::generate_key(KeyState::Active)?] })
    }

    /// Parse a key file.
    pub fn parse(text: &str) -> Result<Self, TokenError> {
        let mut keys = Vec::new();
        for (i, line) in text.lines().enumerate() {
            let l = line.trim();
            if l.is_empty() || l.starts_with('#') {
                continue;
            }
            let f: Vec<&str> = l.split_whitespace().collect();
            let err = |m: &str| TokenError::KeyFile(format!("line {}: {m}", i + 1));
            if f.len() != 5 || f[0] != KEY_LINE_TAG {
                return Err(err("expected `rsz-key-v1 <kid> <active|verify> <created> <secret>`"));
            }
            if !valid_kid(f[1]) {
                return Err(err("invalid key id"));
            }
            let state = match f[2] {
                "active" => KeyState::Active,
                "verify" => KeyState::Verify,
                _ => return Err(err("state must be active or verify")),
            };
            let created = f[3].parse::<u64>().map_err(|_| err("created must be unix seconds"))?;
            let secret = URL_SAFE_NO_PAD.decode(f[4]).map_err(|_| err("secret must be base64url without padding"))?;
            if secret.len() < 16 {
                return Err(err("secret shorter than 16 bytes"));
            }
            keys.push(SigningKey { kid: f[1].to_string(), state, created, secret });
        }
        let ring = KeyRing { keys };
        ring.check()?;
        Ok(ring)
    }

    fn check(&self) -> Result<(), TokenError> {
        let active = self.keys.iter().filter(|k| k.state == KeyState::Active).count();
        if active != 1 {
            return Err(TokenError::KeyFile(format!("exactly one active key required, found {active}")));
        }
        if self.keys.len() > 2 {
            return Err(TokenError::KeyFile(format!("at most two keys, found {}", self.keys.len())));
        }
        if self.keys.len() == 2 && self.keys[0].kid == self.keys[1].kid {
            return Err(TokenError::KeyFile("duplicate key id".into()));
        }
        Ok(())
    }

    /// The key file text (active key first).
    pub fn to_text(&self) -> String {
        let mut keys: Vec<&SigningKey> = self.keys.iter().collect();
        keys.sort_by_key(|k| k.state != KeyState::Active);
        let mut s = String::from("# rightsizer Slurm token keys (rsz slurm keygen / rotate-key). Mode 0600.\n");
        for k in keys {
            let st = if k.state == KeyState::Active { "active" } else { "verify" };
            s.push_str(&format!("{KEY_LINE_TAG} {} {st} {} {}\n", k.kid, k.created, URL_SAFE_NO_PAD.encode(&k.secret)));
        }
        s
    }

    /// Load from a file.
    pub fn load(path: &Path) -> Result<Self, TokenError> {
        let t = std::fs::read_to_string(path).map_err(|e| TokenError::Io(format!("{}: {e}", path.display())))?;
        Self::parse(&t)
    }

    /// Write atomically (temp file in the same directory, mode 0600, rename).
    pub fn save(&self, path: &Path) -> Result<(), TokenError> {
        use std::io::Write;
        use std::os::unix::fs::OpenOptionsExt;
        self.check()?;
        let dir = path.parent().filter(|p| !p.as_os_str().is_empty()).unwrap_or(Path::new("."));
        let tmp = dir.join(format!(".{}.tmp-{}", path.file_name().and_then(|n| n.to_str()).unwrap_or("key"), std::process::id()));
        let io = |e: std::io::Error| TokenError::Io(e.to_string());
        {
            let mut f = std::fs::OpenOptions::new().write(true).create_new(true).mode(0o600).open(&tmp).map_err(io)?;
            f.write_all(self.to_text().as_bytes()).map_err(io)?;
            f.sync_all().map_err(io)?;
        }
        std::fs::rename(&tmp, path).map_err(io)
    }

    /// The active key.
    pub fn active(&self) -> Result<&SigningKey, TokenError> {
        self.keys.iter().find(|k| k.state == KeyState::Active).ok_or_else(|| TokenError::NoKey("no active key".into()))
    }

    /// Sign with the active key.
    pub fn sign(&self, c: &TokenClaims) -> Result<String, TokenError> {
        Ok(self.active()?.sign(c))
    }

    /// Apply one rotation step.
    pub fn rotate(&mut self, step: RotateStep) -> Result<(), TokenError> {
        match step {
            RotateStep::Stage => {
                if self.keys.len() >= 2 {
                    return Err(TokenError::KeyFile("a second key is already present: promote or retire it first".into()));
                }
                let mut k = Self::generate_key(KeyState::Verify)?;
                while self.keys.iter().any(|o| o.kid == k.kid) {
                    k = Self::generate_key(KeyState::Verify)?;
                }
                self.keys.push(k);
            }
            RotateStep::Promote => {
                let Some(i) = self.keys.iter().position(|k| k.state == KeyState::Verify) else {
                    return Err(TokenError::KeyFile("no staged key to promote: run --stage first".into()));
                };
                for k in &mut self.keys {
                    k.state = KeyState::Verify;
                }
                self.keys[i].state = KeyState::Active;
                self.keys.sort_by_key(|k| k.state != KeyState::Active);
            }
            RotateStep::Retire => self.keys.retain(|k| k.state == KeyState::Active),
        }
        self.check()
    }

    /// Verify a token against what the job actually carries. Every listed
    /// key verifies (two during rotation); the MAC comparison is constant
    /// time. Never errors: anything wrong is `User`.
    pub fn verify(&self, token: Option<&str>, input: &VerifyInput<'_>) -> Verification {
        let Some(t) = token.filter(|t| !t.trim().is_empty()) else {
            return Verification::User { decision_id: None, reason: "absent" };
        };
        let p = match parse_token(t) {
            Ok(p) => p,
            Err(_) => return Verification::User { decision_id: None, reason: "malformed" },
        };
        let id = Some(p.decision_id);
        if input.now > p.expiry.saturating_add(input.skew_s) {
            return Verification::User { decision_id: id, reason: "expired" };
        }
        let Some(key) = self.keys.iter().find(|k| k.kid == p.kid) else {
            return Verification::User { decision_id: id, reason: "unknown_key" };
        };
        let claims = TokenClaims {
            uid: input.uid,
            account: input.account.to_string(),
            script_sha256: input.script_sha256.to_string(),
            resources: input.resources.clone(),
            decision_id: p.decision_id,
            expiry: p.expiry,
        };
        if key.verify_mac(&mac_message(&p.kid, &claims), &p.mac) {
            Verification::Verified { decision_id: p.decision_id }
        } else {
            Verification::User { decision_id: id, reason: "mac" }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ring() -> KeyRing {
        KeyRing {
            keys: vec![SigningKey { kid: "k1".into(), state: KeyState::Active, created: 1, secret: b"0123456789abcdef0123456789abcdef".to_vec() }],
        }
    }

    fn claims() -> TokenClaims {
        TokenClaims {
            uid: 1000,
            account: "MLteam".into(),
            script_sha256: script_sha256(b"#!/bin/bash\necho hi\n"),
            resources: JobDescResources { min_mem_per_node: Some(24576), cpus_per_task: Some(4), ..Default::default() },
            decision_id: Ulid::from_string("01JDZ7Q4W8M7C9B5V3N2X1Y0ZA").unwrap(),
            expiry: 1_800_000_000,
        }
    }

    fn input(c: &TokenClaims) -> VerifyInput<'_> {
        VerifyInput { uid: c.uid, account: &c.account, script_sha256: &c.script_sha256, resources: &c.resources, now: c.expiry - 10, skew_s: 0 }
    }

    #[test]
    fn tres_normalization() {
        assert_eq!(normalize_tres(Some("gres/gpu:a100:2")), "gres/gpu:a100=2");
        assert_eq!(normalize_tres(Some("gres:gpu:2")), "gres/gpu=2");
        assert_eq!(normalize_tres(Some("gres/gpu=1")), "gres/gpu=1");
        assert_eq!(normalize_tres(Some("gpu:2")), "gres/gpu=2");
        assert_eq!(normalize_tres(Some("cpu=3,gres/gpu=1")), "gres/gpu=1");
        assert_eq!(normalize_tres(Some("cpu=3")), "-");
        assert_eq!(normalize_tres(Some("gres/gpu:10240")), "gres/gpu=10240");
        assert_eq!(normalize_tres(Some("gres/gpu:A100")), "gres/gpu:a100=1");
        assert_eq!(normalize_tres(Some("license/matlab=2,gres/gpu:2")), "gres/gpu=2,license/matlab=2");
        assert_eq!(normalize_tres(Some("")), "-");
        assert_eq!(normalize_tres(None), "-");
    }

    #[test]
    fn options_map_to_job_desc_as_observed_on_26_05() {
        let m = |pairs: &[(&'static str, &'static str)]| {
            let pairs = pairs.to_vec();
            JobDescResources::from_options(move |k| pairs.iter().find(|p| p.0 == k).map(|p| p.1))
        };
        // Observed job_desc dumps (IMPLEMENTATION-NOTES "rightsizer-slurm").
        let r = m(&[("mem", "1G"), ("cpus-per-task", "2")]);
        assert_eq!(r.min_mem_per_node, Some(1024));
        assert_eq!(r.cpus_per_task, Some(2));
        assert_eq!(m(&[("mem", "1500K")]).min_mem_per_node, Some(2));
        assert_eq!(m(&[("mem", "0")]).min_mem_per_node, Some(0));
        assert_eq!(m(&[("mem-per-cpu", "300")]).min_mem_per_cpu, Some(300));
        assert_eq!(m(&[("tmp", "2G")]).pn_min_tmp_disk, Some(2048));
        let g = m(&[("gpus", "2"), ("mem-per-gpu", "10G")]);
        assert_eq!(normalize_tres(g.tres_per_job.as_deref()), normalize_tres(Some("gres/gpu:2")));
        assert_eq!(normalize_tres(g.mem_per_tres.as_deref()), normalize_tres(Some("gres/gpu:10240")));
        assert_eq!(
            normalize_tres(m(&[("gres", "gpu:a100:2")]).tres_per_node.as_deref()),
            normalize_tres(Some("gres/gpu:a100:2"))
        );
        assert_eq!(normalize_tres(m(&[("gpus-per-node", "a100:2")]).tres_per_node.as_deref()), "gres/gpu:a100=2");
        assert_eq!(normalize_tres(m(&[("gpus-per-task", "1")]).tres_per_task.as_deref()), normalize_tres(Some("gres/gpu=1")));
        // cli_filter reports --gres already prefixed.
        assert_eq!(m(&[("gres", "gres/gpu:a100:1")]).tres_per_node.as_deref(), Some("gres/gpu:a100:1"));
    }

    #[test]
    fn canonical_form() {
        let r = JobDescResources { min_mem_per_node: Some(100), cpus_per_task: Some(65534), pn_min_tmp_disk: Some(4_294_967_294), ..Default::default() };
        assert_eq!(
            r.canonical(),
            "mem_per_node=100\nmem_per_cpu=-\ncpus_per_task=-\ntres_per_node=-\ntres_per_job=-\ntres_per_task=-\ntres_per_socket=-\nmem_per_tres=-\ntmp_disk=-"
        );
    }

    #[test]
    fn round_trip() {
        let r = ring();
        let c = claims();
        let t = r.sign(&c).unwrap();
        let p = parse_token(&t).unwrap();
        assert_eq!(p.kid, "k1");
        assert_eq!(p.decision_id, c.decision_id);
        assert_eq!(p.expiry, c.expiry);
        assert_eq!(r.verify(Some(&t), &input(&c)), Verification::Verified { decision_id: c.decision_id });
        assert!(!t.contains(['=', ',', '+', '/']), "safe inside --export lists");
        // Account case does not matter (Slurm lower-cases accounts).
        let mut lc = c.clone();
        lc.account = "mlteam".into();
        assert!(matches!(r.verify(Some(&t), &input(&lc)), Verification::Verified { .. }));
    }

    #[test]
    fn tamper_each_field() {
        let r = ring();
        let c = claims();
        let t = r.sign(&c).unwrap();
        let mut variants: Vec<TokenClaims> = Vec::new();
        let mut x = c.clone();
        x.uid = 1001;
        variants.push(x);
        let mut x = c.clone();
        x.account = "vision".into();
        variants.push(x);
        let mut x = c.clone();
        x.script_sha256 = script_sha256(b"#!/bin/bash\necho bye\n");
        variants.push(x);
        let mut x = c.clone();
        x.resources.min_mem_per_node = Some(24577);
        variants.push(x);
        let mut x = c.clone();
        x.resources.min_mem_per_node = None;
        x.resources.min_mem_per_cpu = Some(24576);
        variants.push(x);
        let mut x = c.clone();
        x.resources.cpus_per_task = Some(8);
        variants.push(x);
        let mut x = c.clone();
        x.resources.tres_per_node = Some("gres/gpu:1".into());
        variants.push(x);
        let mut x = c.clone();
        x.resources.tres_per_job = Some("gres/gpu:1".into());
        variants.push(x);
        let mut x = c.clone();
        x.resources.tres_per_task = Some("gres/gpu=1".into());
        variants.push(x);
        let mut x = c.clone();
        x.resources.tres_per_socket = Some("gres/gpu:1".into());
        variants.push(x);
        let mut x = c.clone();
        x.resources.mem_per_tres = Some("gres/gpu:1024".into());
        variants.push(x);
        let mut x = c.clone();
        x.resources.pn_min_tmp_disk = Some(10);
        variants.push(x);
        for v in &variants {
            assert!(matches!(r.verify(Some(&t), &input(v)), Verification::User { reason: "mac", .. }), "{v:?}");
        }
        // Tampering with the decision id or the expiry inside the token.
        let mut raw = URL_SAFE_NO_PAD.decode(&t).unwrap();
        let k = raw[1] as usize;
        let mut id = raw.clone();
        id[2 + k] = if id[2 + k] == b'0' { b'1' } else { b'0' };
        assert!(matches!(r.verify(Some(&URL_SAFE_NO_PAD.encode(&id)), &input(&c)), Verification::User { reason: "mac", .. }));
        raw[2 + k + 26 + 7] ^= 1;
        assert!(matches!(r.verify(Some(&URL_SAFE_NO_PAD.encode(&raw)), &input(&c)), Verification::User { reason: "mac", .. }));
        let mut m = URL_SAFE_NO_PAD.decode(&t).unwrap();
        let last = m.len() - 1;
        m[last] ^= 0x80;
        assert!(matches!(r.verify(Some(&URL_SAFE_NO_PAD.encode(&m)), &input(&c)), Verification::User { reason: "mac", .. }));
        assert!(matches!(r.verify(Some("garbage!"), &input(&c)), Verification::User { decision_id: None, reason: "malformed" }));
        assert!(matches!(r.verify(None, &input(&c)), Verification::User { decision_id: None, reason: "absent" }));
    }

    #[test]
    fn expiry_and_skew() {
        let r = ring();
        let c = claims();
        let t = r.sign(&c).unwrap();
        let mut i = input(&c);
        i.now = c.expiry + 1;
        assert!(matches!(r.verify(Some(&t), &i), Verification::User { reason: "expired", decision_id: Some(_) }));
        i.skew_s = 5;
        assert!(matches!(r.verify(Some(&t), &i), Verification::Verified { .. }));
        assert_eq!(
            Verification::User { decision_id: Some(c.decision_id), reason: "expired" }.admin_comment(),
            "rightsizer:01JDZ7Q4W8M7C9B5V3N2X1Y0ZA:user"
        );
    }

    #[test]
    fn rotation_with_two_keys() {
        let mut r = KeyRing::generate().unwrap();
        let c = claims();
        let old = r.sign(&c).unwrap();
        r.rotate(RotateStep::Stage).unwrap();
        assert_eq!(r.keys.len(), 2);
        // Still signing with the old key; both verify.
        assert_eq!(parse_token(&r.sign(&c).unwrap()).unwrap().kid, parse_token(&old).unwrap().kid);
        assert!(r.rotate(RotateStep::Stage).is_err());
        r.rotate(RotateStep::Promote).unwrap();
        let new = r.sign(&c).unwrap();
        assert_ne!(parse_token(&new).unwrap().kid, parse_token(&old).unwrap().kid);
        for t in [&old, &new] {
            assert!(matches!(r.verify(Some(t), &input(&c)), Verification::Verified { .. }));
        }
        // A controller that only has the staged file still verifies both.
        let text = r.to_text();
        let reread = KeyRing::parse(&text).unwrap();
        assert_eq!(reread, r);
        r.rotate(RotateStep::Retire).unwrap();
        assert_eq!(r.keys.len(), 1);
        assert!(matches!(r.verify(Some(&old), &input(&c)), Verification::User { reason: "unknown_key", .. }));
        assert!(matches!(r.verify(Some(&new), &input(&c)), Verification::Verified { .. }));
        assert!(r.clone().rotate(RotateStep::Promote).is_err());
    }

    #[test]
    fn key_file_errors_and_save() {
        assert!(KeyRing::parse("").is_err());
        assert!(KeyRing::parse("rsz-key-v1 k1 verify 1 MDEyMzQ1Njc4OWFiY2RlZg").is_err());
        assert!(KeyRing::parse("rsz-key-v1 k1 active 1 c2hvcnQ").is_err());
        assert!(KeyRing::parse("rsz-key-v1 k/1 active 1 MDEyMzQ1Njc4OWFiY2RlZg").is_err());
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("rightsizer.key");
        let r = KeyRing::generate().unwrap();
        r.save(&p).unwrap();
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(std::fs::metadata(&p).unwrap().permissions().mode() & 0o777, 0o600);
        assert_eq!(KeyRing::load(&p).unwrap(), r);
        assert!(!format!("{:?}", r.keys[0]).contains("secret"));
    }
}
