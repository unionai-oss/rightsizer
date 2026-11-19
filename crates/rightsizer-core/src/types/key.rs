//! Workload identity: tenants, keys, unit and group ids (SPEC §4, §8.5).

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::TypeError;

/// The security principal that owns workloads, derived by the integration
/// from authenticated data (SPEC §4): a `/`-separated path of segments, e.g.
/// `"prod-us/ml-team"` (K8s `{cluster}/{namespace}`).
///
/// Segments are non-empty, use `[A-Za-z0-9._@+=,-]`, and are never `.` or
/// `..` (tenants become store path segments, SPEC §8.1: a dot segment
/// would let one tenant's paths alias another's or escape its prefix).
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(transparent)]
pub struct Tenant(String);

impl Tenant {
    /// Validate and wrap.
    pub fn new(s: impl Into<String>) -> Result<Self, TypeError> {
        let s = s.into();
        if s.is_empty() || s.len() > 256 || !s.split('/').all(valid_segment) {
            return Err(TypeError::InvalidTenant(s));
        }
        Ok(Tenant(s))
    }
    /// The tenant string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
    /// Number of `/`-separated segments.
    pub fn segments(&self) -> usize {
        self.0.split('/').count()
    }
}

impl<'de> Deserialize<'de> for Tenant {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        Tenant::new(String::deserialize(d)?).map_err(serde::de::Error::custom)
    }
}

impl fmt::Display for Tenant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

fn valid_segment(seg: &str) -> bool {
    !seg.is_empty()
        && seg != "."
        && seg != ".."
        && seg.len() <= 128
        && seg.bytes().all(|b| b.is_ascii_alphanumeric() || b"._@+=,-".contains(&b))
}

fn valid_platform(p: &str) -> bool {
    let b = p.as_bytes();
    !b.is_empty()
        && b.len() <= 32
        && b[0].is_ascii_lowercase()
        && b.iter().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == b'-')
}

fn valid_suffix(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 512
        && !s.starts_with('/')
        && !s.ends_with('/')
        && !s.contains("//")
        && s.split('/').all(|seg| !seg.is_empty() && seg != "." && seg != "..")
        && s.chars().all(|c| !c.is_control() && !c.is_whitespace())
}

/// Number of tenant segments for the built-in platforms (SPEC §4 and the
/// platform documents). Keys whose tenant has a different number of segments
/// (or whose platform is not listed) are written with an explicit `//`
/// separator: `{platform}://{tenant}//{suffix}`.
pub fn tenant_arity(platform: &str) -> Option<usize> {
    Some(match platform {
        "k8s" | "slurm" | "ray" | "sky" | "dstack" | "aws" | "gcp" | "modal" | "databricks" => 2,
        "flyte" | "azure" => 3,
        "airflow" | "metaflow" => 1,
        _ => return None,
    })
}

/// A workload key `{platform}://{tenant}/{suffix}` (SPEC §4, §8.5). Users
/// choose only the suffix; the tenant comes from authenticated data.
///
/// Because tenants and suffixes may both contain `/` (K8s suffixes look like
/// `pod/{name}/{container}`), the string form splits the tenant by the
/// platform's [`tenant_arity`]. When the arity is unknown or does not match,
/// the canonical form uses `//` between tenant and suffix; parsing always
/// accepts `//`.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WorkloadKey {
    /// Platform kind, e.g. `"k8s"`.
    pub platform: String,
    /// Authenticated tenant.
    pub tenant: Tenant,
    /// User-chosen suffix.
    pub suffix: String,
}

impl WorkloadKey {
    /// Build and validate.
    pub fn new(platform: impl Into<String>, tenant: Tenant, suffix: impl Into<String>) -> Result<Self, TypeError> {
        let platform = platform.into();
        let suffix = suffix.into();
        if !valid_platform(&platform) {
            return Err(TypeError::InvalidKey { key: platform, reason: "platform must match [a-z][a-z0-9-]{0,31}" });
        }
        if !valid_suffix(&suffix) {
            return Err(TypeError::InvalidKey {
                key: suffix,
                reason: "suffix must be non-empty, without whitespace, '//', '.' or '..' segments, or leading/trailing '/'",
            });
        }
        Ok(WorkloadKey { platform, tenant, suffix })
    }

    /// Parse the string form.
    pub fn parse(s: &str) -> Result<Self, TypeError> {
        let err = |reason| TypeError::InvalidKey { key: s.to_string(), reason };
        let (platform, rest) = s.split_once("://").ok_or_else(|| err("missing '://'"))?;
        let (tenant, suffix) = if let Some((t, x)) = rest.split_once("//") {
            (t.to_string(), x.to_string())
        } else {
            let n = tenant_arity(platform).unwrap_or(1);
            let parts: Vec<&str> = rest.splitn(n + 1, '/').collect();
            if parts.len() != n + 1 {
                return Err(err("too few segments for tenant and suffix"));
            }
            (parts[..n].join("/"), parts[n].to_string())
        };
        let tenant = Tenant::new(tenant).map_err(|_| err("invalid tenant"))?;
        WorkloadKey::new(platform, tenant, suffix).map_err(|e| match e {
            TypeError::InvalidKey { reason, .. } => err(reason),
            other => other,
        })
    }

    /// A stable short hash of the key (hex, 32 chars), used in store paths
    /// (`{key-hash}`, SPEC §8.1).
    pub fn hash(&self) -> String {
        crate::digest::sha256_hex(self.to_string().as_bytes())[..32].to_string()
    }
}

impl fmt::Display for WorkloadKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let sep = match tenant_arity(&self.platform) {
            Some(n) if n == self.tenant.segments() => "/",
            None if self.tenant.segments() == 1 => "/",
            _ => "//",
        };
        write!(f, "{}://{}{}{}", self.platform, self.tenant, sep, self.suffix)
    }
}

impl FromStr for WorkloadKey {
    type Err = TypeError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        WorkloadKey::parse(s)
    }
}

impl Serialize for WorkloadKey {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for WorkloadKey {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        WorkloadKey::parse(&String::deserialize(d)?).map_err(serde::de::Error::custom)
    }
}

#[cfg(feature = "schema")]
impl schemars::JsonSchema for WorkloadKey {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "WorkloadKey".into()
    }
    fn json_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({
            "description": "Workload key `{platform}://{tenant}/{suffix}` (or `{platform}://{tenant}//{suffix}`).",
            "type": "string",
            "pattern": "^[a-z][a-z0-9-]{0,31}://.+/.+$"
        })
    }
}

/// A unit's id, stable within its submission (plugins.md §2).
#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(transparent)]
pub struct UnitId(pub String);

/// A group id: units sized identically (SPEC §4).
#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(transparent)]
pub struct GroupId(pub String);

impl fmt::Display for UnitId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl fmt::Display for GroupId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn k8s_key_with_slashy_suffix() {
        let k = WorkloadKey::parse("k8s://prod/ml/pod/trainer/main").unwrap();
        assert_eq!(k.tenant.as_str(), "prod/ml");
        assert_eq!(k.suffix, "pod/trainer/main");
        assert_eq!(k.to_string(), "k8s://prod/ml/pod/trainer/main");
    }

    #[test]
    fn flyte_key() {
        let k: WorkloadKey = "flyte://acme/ml/development/env.train".parse().unwrap();
        assert_eq!(k.tenant.as_str(), "acme/ml/development");
        assert_eq!(k.suffix, "env.train");
    }

    #[test]
    fn explicit_separator_for_unusual_tenants() {
        let k = WorkloadKey::new("nomad", Tenant::new("dc1/ns").unwrap(), "job/web").unwrap();
        let s = k.to_string();
        assert_eq!(s, "nomad://dc1/ns//job/web");
        assert_eq!(WorkloadKey::parse(&s).unwrap(), k);
        let r = WorkloadKey::new("ray", Tenant::new("team-a").unwrap(), "f").unwrap();
        assert_eq!(r.to_string(), "ray://team-a//f");
        assert_eq!(WorkloadKey::parse("ray://team-a//f").unwrap(), r);
    }

    #[test]
    fn rejects_bad_keys() {
        for bad in [
            "k8s:/a/b/c",
            "K8S://a/b/c",
            "k8s://a/b",
            "k8s://a/b/",
            "k8s://a b/c/d",
            "k8s://a/b/c d",
            "k8s:///b/c",
            "slurm://c/acct//",
        ] {
            assert!(WorkloadKey::parse(bad).is_err(), "{bad}");
        }
    }

    /// Refinement round (item 4): `.` / `..` / empty segments never reach a
    /// store path, in tenants or suffixes; dots inside a segment are fine.
    #[test]
    fn rejects_dot_and_empty_segments() {
        for t in [".", "..", "a/.", "a/..", "../a", "a//b", "a/", "/a"] {
            assert!(Tenant::new(t).is_err(), "tenant {t:?}");
        }
        for k in [
            "k8s://../ns/job/x",
            "k8s://c/../job/x",
            "k8s://c/./job/x",
            "k8s://c/ns/job/..",
            "k8s://c/ns/../x",
            "k8s://c/ns/./x",
            "flyte://a/b/c/..",
            "nomad://dc1/..//job",
            "nomad://dc1/ns//../job",
            "nomad://dc1/ns//job/.",
        ] {
            assert!(WorkloadKey::parse(k).is_err(), "key {k:?}");
        }
        let t = Tenant::new("a.b/..c/d..").unwrap();
        assert_eq!(t.segments(), 3);
        assert!(WorkloadKey::new("k8s", Tenant::new("c/ns").unwrap(), "pod/x..y/.hidden").is_ok());
        assert!(WorkloadKey::new("k8s", Tenant::new("c/ns").unwrap(), "pod/../x").is_err());
        assert!(serde_json::from_str::<Tenant>("\"a/..\"").is_err());
    }

    #[test]
    fn serde_is_a_string() {
        let k = WorkloadKey::parse("slurm://gpu-a/mlteam/train.sh").unwrap();
        let j = serde_json::to_string(&k).unwrap();
        assert_eq!(j, "\"slurm://gpu-a/mlteam/train.sh\"");
        assert_eq!(serde_json::from_str::<WorkloadKey>(&j).unwrap(), k);
        assert_eq!(k.hash().len(), 32);
        // Cross-language vector shared with python/rightsizer-runtime.
        assert_eq!(WorkloadKey::parse("flyte://acme/proj/dev/env.train").unwrap().hash(), "eef92d58436c80afb2670dc464a4b4f2");
        assert_ne!(k.hash(), WorkloadKey::parse("slurm://gpu-a/mlteam/other.sh").unwrap().hash());
    }
}
