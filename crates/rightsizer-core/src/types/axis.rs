//! Axes and axis sets (SPEC §5.1).

use std::fmt;
use std::str::FromStr;

use enumflags2::{BitFlags, bitflags};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::TypeError;

/// A resource axis (SPEC §5.1).
#[bitflags]
#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum Axis {
    /// Host memory (incompressible).
    Memory,
    /// Host CPU (compressible).
    Cpu,
    /// Number of accelerators.
    GpuCount,
    /// Accelerator type.
    GpuType,
    /// Ephemeral storage (no grid in v1: always held).
    EphemeralStorage,
    /// Shared memory (`/dev/shm`; no grid in v1: always held).
    Shm,
}

impl Axis {
    /// Every axis, in canonical order.
    pub const ALL: [Axis; 6] =
        [Axis::Memory, Axis::Cpu, Axis::GpuCount, Axis::GpuType, Axis::EphemeralStorage, Axis::Shm];

    /// Canonical `snake_case` name.
    pub fn as_str(self) -> &'static str {
        match self {
            Axis::Memory => "memory",
            Axis::Cpu => "cpu",
            Axis::GpuCount => "gpu_count",
            Axis::GpuType => "gpu_type",
            Axis::EphemeralStorage => "ephemeral_storage",
            Axis::Shm => "shm",
        }
    }

    /// Whether an under-ask fails the unit rather than slowing it (SPEC §2.3 S3).
    /// Memory is incompressible; GPU memory is too but is not an [`Axis`]
    /// request in v1 (count and type are held, SPEC §5.4).
    pub fn is_incompressible(self) -> bool {
        matches!(self, Axis::Memory | Axis::EphemeralStorage | Axis::Shm)
    }
}

impl fmt::Display for Axis {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Parse one axis name or alias into the set it denotes. Aliases accepted at
/// every API surface: `"gpu"` = {GpuCount, GpuType}; `"disk"` =
/// EphemeralStorage (SPEC §5.1). Also accepts `-` for `_` and `mem`.
pub fn parse_axis_name(s: &str) -> Result<AxisSet, TypeError> {
    let n = s.trim().to_ascii_lowercase().replace('-', "_");
    let flags: BitFlags<Axis> = match n.as_str() {
        "memory" | "mem" => Axis::Memory.into(),
        "cpu" => Axis::Cpu.into(),
        "gpu_count" => Axis::GpuCount.into(),
        "gpu_type" => Axis::GpuType.into(),
        "gpu" => Axis::GpuCount | Axis::GpuType,
        "ephemeral_storage" | "disk" => Axis::EphemeralStorage.into(),
        "shm" => Axis::Shm.into(),
        _ => return Err(TypeError::UnknownAxis(s.to_string())),
    };
    Ok(AxisSet(flags))
}

impl FromStr for Axis {
    type Err = TypeError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let set = parse_axis_name(s)?;
        let mut it = set.iter();
        match (it.next(), it.next()) {
            (Some(a), None) => Ok(a),
            _ => Err(TypeError::UnknownAxis(s.to_string())),
        }
    }
}

/// A set of axes. Wraps `BitFlags<Axis>` (the spec's `type AxisSet`) so that it
/// serializes as a JSON array of canonical names and accepts aliases.
///
/// JSON: `["memory", "cpu"]`; deserialization also accepts a comma-separated
/// string (`"memory,gpu"`) and the aliases `gpu` and `disk`.
#[derive(Copy, Clone, Default, PartialEq, Eq, Hash)]
pub struct AxisSet(pub BitFlags<Axis>);

impl AxisSet {
    /// The empty set.
    pub fn empty() -> Self {
        Self(BitFlags::empty())
    }
    /// Every axis.
    pub fn all() -> Self {
        Self(BitFlags::all())
    }
    /// Whether `axis` is in the set.
    pub fn contains(&self, axis: Axis) -> bool {
        self.0.contains(axis)
    }
    /// Add `axis`.
    pub fn insert(&mut self, axis: Axis) {
        self.0.insert(axis);
    }
    /// Remove `axis`.
    pub fn remove(&mut self, axis: Axis) {
        self.0.remove(axis);
    }
    /// Whether the set is empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
    /// Axes in canonical order.
    pub fn iter(&self) -> impl Iterator<Item = Axis> + '_ {
        Axis::ALL.into_iter().filter(|a| self.0.contains(*a))
    }
    /// Union.
    pub fn union(self, other: AxisSet) -> AxisSet {
        AxisSet(self.0 | other.0)
    }
    /// Parse a comma-separated list of names and aliases.
    pub fn parse_list(s: &str) -> Result<Self, TypeError> {
        let mut out = AxisSet::empty();
        for part in s.split(',').map(str::trim).filter(|p| !p.is_empty()) {
            out = out.union(parse_axis_name(part)?);
        }
        Ok(out)
    }
}

impl From<Axis> for AxisSet {
    fn from(a: Axis) -> Self {
        AxisSet(a.into())
    }
}

impl From<BitFlags<Axis>> for AxisSet {
    fn from(b: BitFlags<Axis>) -> Self {
        AxisSet(b)
    }
}

impl FromIterator<Axis> for AxisSet {
    fn from_iter<T: IntoIterator<Item = Axis>>(iter: T) -> Self {
        let mut s = AxisSet::empty();
        for a in iter {
            s.insert(a);
        }
        s
    }
}

impl fmt::Debug for AxisSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_set().entries(self.iter()).finish()
    }
}

impl Serialize for AxisSet {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_seq(self.iter().map(Axis::as_str))
    }
}

impl<'de> Deserialize<'de> for AxisSet {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Repr {
            List(Vec<String>),
            Csv(String),
        }
        let parts = match Repr::deserialize(d)? {
            Repr::List(v) => v,
            Repr::Csv(s) => s.split(',').map(|p| p.trim().to_string()).filter(|p| !p.is_empty()).collect(),
        };
        let mut out = AxisSet::empty();
        for p in parts {
            out = out.union(parse_axis_name(&p).map_err(serde::de::Error::custom)?);
        }
        Ok(out)
    }
}

#[cfg(feature = "schema")]
impl schemars::JsonSchema for AxisSet {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "AxisSet".into()
    }
    fn json_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({
            "description": "A set of axes; aliases `gpu` = {gpu_count, gpu_type}, `disk` = ephemeral_storage.",
            "type": "array",
            "uniqueItems": true,
            "items": {
                "type": "string",
                "enum": ["memory", "cpu", "gpu_count", "gpu_type", "ephemeral_storage", "shm", "gpu", "disk", "mem"]
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aliases_expand() {
        let s = AxisSet::parse_list("gpu, disk").unwrap();
        assert!(s.contains(Axis::GpuCount) && s.contains(Axis::GpuType) && s.contains(Axis::EphemeralStorage));
        assert!(!s.contains(Axis::Memory));
        assert!("gpu".parse::<Axis>().is_err(), "gpu is a set, not one axis");
        assert_eq!("ephemeral-storage".parse::<Axis>().unwrap(), Axis::EphemeralStorage);
    }

    #[test]
    fn serde_round_trip() {
        let s: AxisSet = [Axis::Cpu, Axis::Memory].into_iter().collect();
        let j = serde_json::to_string(&s).unwrap();
        assert_eq!(j, r#"["memory","cpu"]"#);
        let back: AxisSet = serde_json::from_str(&j).unwrap();
        assert_eq!(back, s);
        let alias: AxisSet = serde_json::from_str(r#"["gpu"]"#).unwrap();
        assert_eq!(alias.iter().count(), 2);
        let csv: AxisSet = serde_json::from_str(r#""memory,disk""#).unwrap();
        assert!(csv.contains(Axis::EphemeralStorage));
        assert!(serde_json::from_str::<AxisSet>(r#"["nope"]"#).is_err());
    }
}
