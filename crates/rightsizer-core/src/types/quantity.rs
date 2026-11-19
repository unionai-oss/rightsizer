//! Integer quantities on the wire (SPEC §5.1, ARCHITECTURE.md §4).

use std::fmt;
use std::ops::{Add, Sub};

use serde::{Deserialize, Serialize};

/// A byte count. JSON: an unsigned integer.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(transparent)]
pub struct Bytes(pub u64);

impl Bytes {
    /// 1 KiB.
    pub const KIB: u64 = 1 << 10;
    /// 1 MiB.
    pub const MIB: u64 = 1 << 20;
    /// 1 GiB.
    pub const GIB: u64 = 1 << 30;
    /// 1 TiB.
    pub const TIB: u64 = 1 << 40;

    /// `n` MiB.
    pub const fn mib(n: u64) -> Self {
        Bytes(n * Self::MIB)
    }
    /// `n` GiB.
    pub const fn gib(n: u64) -> Self {
        Bytes(n * Self::GIB)
    }
    /// Fractional GiB, rounded up to a byte.
    pub fn from_gib_f64(g: f64) -> Self {
        Bytes::from_f64(g * Self::GIB as f64)
    }
    /// Fractional MiB, rounded up to a byte.
    pub fn from_mib_f64(m: f64) -> Self {
        Bytes::from_f64(m * Self::MIB as f64)
    }
    /// A float byte count, rounded up and saturated to `[0, u64::MAX]`.
    pub fn from_f64(b: f64) -> Self {
        if !b.is_finite() || b <= 0.0 {
            return Bytes(if b == f64::INFINITY { u64::MAX } else { 0 });
        }
        let c = b.ceil();
        Bytes(if c >= u64::MAX as f64 { u64::MAX } else { c as u64 })
    }
    /// As a float.
    pub fn as_f64(self) -> f64 {
        self.0 as f64
    }
    /// In GiB.
    pub fn as_gib(self) -> f64 {
        self.0 as f64 / Self::GIB as f64
    }
    /// In MiB.
    pub fn as_mib(self) -> f64 {
        self.0 as f64 / Self::MIB as f64
    }
    /// Multiply and round up.
    pub fn mul_f64(self, k: f64) -> Self {
        Bytes::from_f64(self.0 as f64 * k)
    }
}

impl Add for Bytes {
    type Output = Bytes;
    fn add(self, o: Bytes) -> Bytes {
        Bytes(self.0.saturating_add(o.0))
    }
}

impl Sub for Bytes {
    type Output = Bytes;
    fn sub(self, o: Bytes) -> Bytes {
        Bytes(self.0.saturating_sub(o.0))
    }
}

impl fmt::Display for Bytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&crate::units::format_k8s_bytes(self.0))
    }
}

/// CPU in millicores (1000 = one core). JSON: an unsigned integer.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(transparent)]
pub struct MilliCpu(pub u32);

impl MilliCpu {
    /// Whole cores.
    pub const fn cores(n: u32) -> Self {
        MilliCpu(n * 1000)
    }
    /// Fractional cores, rounded up to a millicore and saturated.
    pub fn from_cores_f64(c: f64) -> Self {
        if !c.is_finite() || c <= 0.0 {
            return MilliCpu(if c == f64::INFINITY { u32::MAX } else { 0 });
        }
        let m = (c * 1000.0 - 1e-9).ceil();
        MilliCpu(if m >= u32::MAX as f64 { u32::MAX } else { m as u32 })
    }
    /// As fractional cores.
    pub fn as_cores(self) -> f64 {
        self.0 as f64 / 1000.0
    }
    /// Whether this is a whole number of cores (static CPU manager, SPEC §7.4).
    pub fn is_whole(self) -> bool {
        self.0.is_multiple_of(1000)
    }
    /// Rounded up to whole cores.
    pub fn ceil_whole(self) -> Self {
        MilliCpu(self.0.div_ceil(1000).saturating_mul(1000))
    }
}

impl fmt::Display for MilliCpu {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&crate::units::format_k8s_cpu(*self))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conversions() {
        assert_eq!(Bytes::from_gib_f64(1.5), Bytes(1_610_612_736));
        assert_eq!(Bytes::from_f64(-3.0), Bytes(0));
        assert_eq!(MilliCpu::from_cores_f64(0.25), MilliCpu(250));
        assert_eq!(MilliCpu::from_cores_f64(1.0001), MilliCpu(1001));
        assert_eq!(MilliCpu(1500).ceil_whole(), MilliCpu(2000));
        assert!(MilliCpu(3000).is_whole());
        assert_eq!(serde_json::to_string(&Bytes::mib(1)).unwrap(), "1048576");
    }
}
