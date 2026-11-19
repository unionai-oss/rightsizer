//! The signed `rightsizer.io/prior` annotation (kubernetes.md §3.2, SPEC §2.3
//! S5): the origin resources of every sized unit, HMAC-SHA256-signed with a
//! key id, so attribution, ladders and `rsz k8s restore` use what the author
//! wrote. An unsigned or invalid annotation is ignored and overwritten.
//!
//! Format: `base64url(JSON {v, kid, prior: {unit → ResourceSpec},
//! defaulted?: {unit → [axis]}, ray?: {unit → {param → value|null}}, sig})`,
//! where `sig = base64url(HMAC-SHA256(key[kid], canonical JSON of the payload
//! without sig))`.

use std::collections::BTreeMap;

use base64::Engine as _;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use hmac::{Hmac, KeyInit, Mac};
use rightsizer_core::{AxisSet, ResourceSpec};
use serde::{Deserialize, Serialize};
use sha2::Sha256;

/// Annotation name.
pub const PRIOR_ANNOTATION: &str = "rightsizer.io/prior";

/// HMAC keys by key id; `active` signs, all verify (rotation).
#[derive(Clone, PartialEq, Eq)]
pub struct SigningKeys {
    /// Key id used for signing.
    pub active: String,
    /// Keys.
    pub keys: BTreeMap<String, Vec<u8>>,
}

impl std::fmt::Debug for SigningKeys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SigningKeys").field("active", &self.active).field("kids", &self.keys.keys().collect::<Vec<_>>()).finish()
    }
}

/// Errors loading keys.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum KeyError {
    /// No key.
    #[error("no signing key")]
    Empty,
    /// A key shorter than 16 bytes.
    #[error("signing key {0:?} is shorter than 16 bytes")]
    Short(String),
    /// Bad base64.
    #[error("signing key {0:?} is not base64")]
    Encoding(String),
}

impl SigningKeys {
    /// One key.
    pub fn single(kid: impl Into<String>, key: impl Into<Vec<u8>>) -> Self {
        let kid = kid.into();
        let mut keys = BTreeMap::new();
        keys.insert(kid.clone(), key.into());
        SigningKeys { active: kid, keys }
    }

    /// Parse a key file: lines `kid:base64-key`; the first line is active.
    /// A file with a single line without `:` is one key with kid `k0`.
    /// Empty lines and `#` comments are skipped.
    pub fn parse(text: &str) -> Result<Self, KeyError> {
        let mut keys = BTreeMap::new();
        let mut active = None;
        for line in text.lines().map(str::trim).filter(|l| !l.is_empty() && !l.starts_with('#')) {
            let (kid, b64) = line.split_once(':').unwrap_or(("k0", line));
            let key = STANDARD.decode(b64.trim()).map_err(|_| KeyError::Encoding(kid.into()))?;
            if key.len() < 16 {
                return Err(KeyError::Short(kid.into()));
            }
            active.get_or_insert_with(|| kid.trim().to_string());
            keys.insert(kid.trim().to_string(), key);
        }
        Ok(SigningKeys { active: active.ok_or(KeyError::Empty)?, keys })
    }

    fn mac(&self, kid: &str) -> Option<Hmac<Sha256>> {
        self.keys.get(kid).and_then(|k| <Hmac<Sha256> as KeyInit>::new_from_slice(k).ok())
    }
}

/// The signed content.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct SignedPrior {
    /// Origin specs by unit id (within the object).
    pub prior: BTreeMap<String, ResourceSpec>,
    /// Axes filled by platform defaults, by unit id.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub defaulted: BTreeMap<String, AxisSet>,
    /// Original KubeRay `rayStartParams` values rightsizer may change, by unit id.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub ray: BTreeMap<String, BTreeMap<String, Option<String>>>,
    /// The object this prior belongs to: `{namespace}/{kind}/{base name}`
    /// ([`crate::extract::prior_identity`]). A genuine prior copied onto
    /// another object (another namespace or workload) does not verify there,
    /// so it can neither make that object look sized nor supply a forged
    /// origin for outcome attribution (SPEC §2.3 S5, §8.2).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bind: Option<String>,
}

#[derive(Serialize)]
struct Unsigned<'a> {
    v: u16,
    kid: &'a str,
    #[serde(flatten)]
    body: &'a SignedPrior,
}

#[derive(Serialize, Deserialize)]
struct Envelope {
    v: u16,
    kid: String,
    #[serde(flatten)]
    body: SignedPrior,
    sig: String,
}

/// Why an annotation did not verify.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum VerifyError {
    /// Not base64url / JSON / the expected shape.
    #[error("malformed prior annotation")]
    Malformed,
    /// Unknown key id.
    #[error("unknown key id {0:?}")]
    UnknownKey(String),
    /// Bad signature.
    #[error("bad signature")]
    BadSignature,
    /// Unsupported version.
    #[error("unsupported version {0}")]
    Version(u16),
}

fn signing_bytes(kid: &str, body: &SignedPrior) -> Vec<u8> {
    rightsizer_core::digest::canonical_json(&Unsigned { v: 1, kid, body })
}

/// Sign `body` with the active key; returns the annotation value.
pub fn sign(body: &SignedPrior, keys: &SigningKeys) -> String {
    let mut mac = keys.mac(&keys.active).expect("active key present");
    mac.update(&signing_bytes(&keys.active, body));
    let sig = URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
    let env = Envelope { v: 1, kid: keys.active.clone(), body: body.clone(), sig };
    URL_SAFE_NO_PAD.encode(serde_json::to_vec(&env).expect("serialize"))
}

/// Verify an annotation value.
pub fn verify(value: &str, keys: &SigningKeys) -> Result<SignedPrior, VerifyError> {
    let raw = URL_SAFE_NO_PAD.decode(value.trim().trim_end_matches('=')).map_err(|_| VerifyError::Malformed)?;
    let env: Envelope = serde_json::from_slice(&raw).map_err(|_| VerifyError::Malformed)?;
    if env.v != 1 {
        return Err(VerifyError::Version(env.v));
    }
    let mut mac = keys.mac(&env.kid).ok_or_else(|| VerifyError::UnknownKey(env.kid.clone()))?;
    mac.update(&signing_bytes(&env.kid, &env.body));
    let sig = URL_SAFE_NO_PAD.decode(&env.sig).map_err(|_| VerifyError::Malformed)?;
    mac.verify_slice(&sig).map_err(|_| VerifyError::BadSignature)?;
    Ok(env.body)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rightsizer_core::{Axis, Bytes, MilliCpu};

    fn body() -> SignedPrior {
        let mut b = SignedPrior::default();
        let mut s = ResourceSpec::default();
        s.requests.memory = Some(Bytes::gib(8));
        s.requests.cpu = Some(MilliCpu(2000));
        s.limits.memory = Some(Bytes::gib(16));
        b.prior.insert("spec.containers[main]".into(), s);
        b.defaulted.insert("spec.containers[main]".into(), AxisSet::from(Axis::Cpu));
        b
    }

    #[test]
    fn roundtrip_and_rotation() {
        let k1 = SigningKeys::single("k1", b"0123456789abcdef0123".to_vec());
        let a = sign(&body(), &k1);
        assert_eq!(verify(&a, &k1).unwrap(), body());
        let mut both = SigningKeys::single("k2", b"fedcba9876543210fedc".to_vec());
        both.keys.insert("k1".into(), b"0123456789abcdef0123".to_vec());
        assert_eq!(verify(&a, &both).unwrap(), body());
        let other = SigningKeys::single("k1", b"another-key-entirely".to_vec());
        assert_eq!(verify(&a, &other), Err(VerifyError::BadSignature));
        assert!(matches!(verify(&a, &SigningKeys::single("zz", b"0123456789abcdef0123".to_vec())), Err(VerifyError::UnknownKey(_))));
    }

    #[test]
    fn tampering_is_detected() {
        let k = SigningKeys::single("k1", b"0123456789abcdef0123".to_vec());
        let a = sign(&body(), &k);
        let raw = URL_SAFE_NO_PAD.decode(&a).unwrap();
        let mut j: serde_json::Value = serde_json::from_slice(&raw).unwrap();
        j["prior"]["spec.containers[main]"]["requests"]["memory"] = serde_json::json!(1u64 << 40);
        let forged = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&j).unwrap());
        assert_eq!(verify(&forged, &k), Err(VerifyError::BadSignature));
        assert_eq!(verify("not base64 !!", &k), Err(VerifyError::Malformed));
        let unsigned = URL_SAFE_NO_PAD.encode(br#"{"v":1,"kid":"k1","prior":{}}"#);
        assert_eq!(verify(&unsigned, &k), Err(VerifyError::Malformed));
    }

    #[test]
    fn key_file() {
        let t = format!("# keys\nk2:{}\nk1:{}\n", STANDARD.encode(b"0123456789abcdef0123"), STANDARD.encode(b"fedcba9876543210fedc"));
        let k = SigningKeys::parse(&t).unwrap();
        assert_eq!(k.active, "k2");
        assert_eq!(k.keys.len(), 2);
        assert_eq!(SigningKeys::parse("k:c2hvcnQ="), Err(KeyError::Short("k".into())));
        assert_eq!(SigningKeys::parse(""), Err(KeyError::Empty));
    }
}
