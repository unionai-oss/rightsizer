//! The accelerator catalog (SPEC §5.1.1).

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use super::{Bytes, TypeError};

/// A canonical accelerator id with per-device memory and family (SPEC §5.1.1).
/// JSON: the canonical string, e.g. `"A100-80G"`; parsing is case-insensitive.
#[allow(non_camel_case_types)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub enum AcceleratorId {
    /// NVIDIA T4, 16 GB.
    #[serde(rename = "T4-16G")]
    T4_16G,
    /// NVIDIA L4, 24 GB.
    #[serde(rename = "L4-24G")]
    L4_24G,
    /// NVIDIA A10G (AWS), 24 GB.
    #[serde(rename = "A10G-24G")]
    A10G_24G,
    /// NVIDIA A10, 24 GB.
    #[serde(rename = "A10-24G")]
    A10_24G,
    /// NVIDIA L40S, 48 GB.
    #[serde(rename = "L40S-48G")]
    L40S_48G,
    /// NVIDIA A100, 40 GB.
    #[serde(rename = "A100-40G")]
    A100_40G,
    /// NVIDIA A100, 80 GB.
    #[serde(rename = "A100-80G")]
    A100_80G,
    /// NVIDIA H100, 80 GB.
    #[serde(rename = "H100-80G")]
    H100_80G,
    /// NVIDIA H200, 141 GB.
    #[serde(rename = "H200-141G")]
    H200_141G,
    /// NVIDIA B200, 180 GB.
    #[serde(rename = "B200-180G")]
    B200_180G,
    /// AMD MI300X, 192 GB.
    #[serde(rename = "MI300X-192G")]
    MI300X_192G,
    /// Google TPU v5e (per chip).
    #[serde(rename = "TPU-V5E")]
    TPU_V5E,
    /// Google TPU v5p (per chip).
    #[serde(rename = "TPU-V5P")]
    TPU_V5P,
    /// Google TPU v6e / Trillium (per chip).
    #[serde(rename = "TPU-V6E")]
    TPU_V6E,
    /// AWS Trainium 1 (per chip).
    #[serde(rename = "TRN1")]
    TRN1,
    /// AWS Trainium 2 (per chip).
    #[serde(rename = "TRN2")]
    TRN2,
    /// AWS Inferentia 2 (per chip).
    #[serde(rename = "INF2")]
    INF2,
    /// A GPU of unknown type. Its type is never changed (SPEC §5.1.1).
    #[serde(rename = "unknown-gpu")]
    UnknownGpu,
}

/// Static facts about one catalog entry.
#[derive(Copy, Clone, Debug, PartialEq)]
pub struct AcceleratorInfo {
    /// Canonical id string.
    pub id: &'static str,
    /// Nominal device memory, GiB (the number in the id).
    pub nominal_gib: f64,
    /// Usable device memory, GiB: what NVML (or the vendor runtime) reports
    /// as total, rounded down. A shape fits when the busiest device's need is
    /// at most this (SPEC §5.4; `tuner:bench_gpu/grid.py`).
    pub usable_gib: f64,
    /// Architecture family, e.g. `"ampere"`.
    pub family: &'static str,
    /// Vendor, e.g. `"nvidia"`.
    pub vendor: &'static str,
}

impl AcceleratorId {
    /// The whole catalog, in a stable order.
    pub const ALL: [AcceleratorId; 18] = [
        AcceleratorId::T4_16G,
        AcceleratorId::L4_24G,
        AcceleratorId::A10G_24G,
        AcceleratorId::A10_24G,
        AcceleratorId::L40S_48G,
        AcceleratorId::A100_40G,
        AcceleratorId::A100_80G,
        AcceleratorId::H100_80G,
        AcceleratorId::H200_141G,
        AcceleratorId::B200_180G,
        AcceleratorId::MI300X_192G,
        AcceleratorId::TPU_V5E,
        AcceleratorId::TPU_V5P,
        AcceleratorId::TPU_V6E,
        AcceleratorId::TRN1,
        AcceleratorId::TRN2,
        AcceleratorId::INF2,
        AcceleratorId::UnknownGpu,
    ];

    /// Catalog facts. `UnknownGpu` has zero memory (unknown).
    pub fn info(self) -> AcceleratorInfo {
        use AcceleratorId::*;
        let (id, nominal_gib, usable_gib, family, vendor) = match self {
            T4_16G => ("T4-16G", 16.0, 15.0, "turing", "nvidia"),
            L4_24G => ("L4-24G", 24.0, 22.0, "ada-lovelace", "nvidia"),
            A10G_24G => ("A10G-24G", 24.0, 22.0, "ampere", "nvidia"),
            A10_24G => ("A10-24G", 24.0, 22.0, "ampere", "nvidia"),
            L40S_48G => ("L40S-48G", 48.0, 44.0, "ada-lovelace", "nvidia"),
            A100_40G => ("A100-40G", 40.0, 39.0, "ampere", "nvidia"),
            A100_80G => ("A100-80G", 80.0, 79.0, "ampere", "nvidia"),
            H100_80G => ("H100-80G", 80.0, 79.0, "hopper", "nvidia"),
            H200_141G => ("H200-141G", 141.0, 139.0, "hopper", "nvidia"),
            B200_180G => ("B200-180G", 180.0, 178.0, "blackwell", "nvidia"),
            MI300X_192G => ("MI300X-192G", 192.0, 191.0, "cdna3", "amd"),
            TPU_V5E => ("TPU-V5E", 16.0, 15.5, "tpu-v5", "google"),
            TPU_V5P => ("TPU-V5P", 95.0, 94.0, "tpu-v5", "google"),
            TPU_V6E => ("TPU-V6E", 32.0, 31.0, "tpu-v6", "google"),
            TRN1 => ("TRN1", 32.0, 31.0, "trainium", "aws"),
            TRN2 => ("TRN2", 96.0, 95.0, "trainium", "aws"),
            INF2 => ("INF2", 32.0, 31.0, "inferentia", "aws"),
            UnknownGpu => ("unknown-gpu", 0.0, 0.0, "unknown", "unknown"),
        };
        AcceleratorInfo { id, nominal_gib, usable_gib, family, vendor }
    }

    /// Canonical id string.
    pub fn as_str(self) -> &'static str {
        self.info().id
    }

    /// Usable device memory, or `None` for `unknown-gpu`.
    pub fn usable_memory(self) -> Option<Bytes> {
        let i = self.info();
        (i.usable_gib > 0.0).then(|| Bytes::from_gib_f64(i.usable_gib))
    }

    /// Nominal device memory, or `None` for `unknown-gpu`.
    pub fn nominal_memory(self) -> Option<Bytes> {
        let i = self.info();
        (i.nominal_gib > 0.0).then(|| Bytes::from_gib_f64(i.nominal_gib))
    }

    /// Architecture family.
    pub fn family(self) -> &'static str {
        self.info().family
    }

    /// Whether this is `unknown-gpu`.
    pub fn is_unknown(self) -> bool {
        self == AcceleratorId::UnknownGpu
    }
}

impl fmt::Display for AcceleratorId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for AcceleratorId {
    type Err = TypeError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let t = s.trim();
        AcceleratorId::ALL
            .into_iter()
            .find(|a| a.as_str().eq_ignore_ascii_case(t))
            .ok_or_else(|| TypeError::UnknownAccelerator(s.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_round_trips() {
        for a in AcceleratorId::ALL {
            let j = serde_json::to_string(&a).unwrap();
            assert_eq!(j, format!("\"{}\"", a.as_str()));
            assert_eq!(serde_json::from_str::<AcceleratorId>(&j).unwrap(), a);
            assert_eq!(a.as_str().to_lowercase().parse::<AcceleratorId>().unwrap(), a);
        }
        assert!("A100".parse::<AcceleratorId>().is_err());
        assert_eq!(AcceleratorId::UnknownGpu.usable_memory(), None);
        assert_eq!(AcceleratorId::T4_16G.usable_memory(), Some(Bytes::gib(15)));
        assert_eq!(AcceleratorId::H100_80G.family(), "hopper");
    }
}
