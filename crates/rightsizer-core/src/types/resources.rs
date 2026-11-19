//! Requests, limits, specs and priors (SPEC §5.1).

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use super::{AcceleratorId, Axis, AxisSet, Bytes, MilliCpu, TypeError};

/// An accelerator type × count (SPEC §5.1, §5.4).
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct GpuShape {
    /// Canonical accelerator id.
    pub accel: AcceleratorId,
    /// Number of devices.
    pub count: u32,
}

impl GpuShape {
    /// Total usable device memory (count × per-device usable), `None` for
    /// `unknown-gpu`.
    pub fn total_memory(&self) -> Option<Bytes> {
        self.accel.usable_memory().map(|b| Bytes(b.0.saturating_mul(self.count as u64)))
    }
}

impl fmt::Display for GpuShape {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.accel, self.count)
    }
}

/// Parses `"{accel}:{count}"`, e.g. `"A100-80G:1"`; a bare id means count 1.
impl FromStr for GpuShape {
    type Err = TypeError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (a, n) = match s.rsplit_once(':') {
            Some((a, n)) => (a, n.trim().parse::<u32>().map_err(|_| TypeError::InvalidGpuShape(s.into()))?),
            None => (s, 1),
        };
        if n == 0 {
            return Err(TypeError::InvalidGpuShape(s.into()));
        }
        Ok(GpuShape { accel: a.parse()?, count: n })
    }
}

/// Resource requests of one unit (SPEC §5.1). `None` = not set.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Requests {
    /// CPU.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu: Option<MilliCpu>,
    /// Memory.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory: Option<Bytes>,
    /// Accelerators.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gpu: Option<GpuShape>,
    /// Ephemeral storage.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ephemeral_storage: Option<Bytes>,
    /// Shared memory.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shm: Option<Bytes>,
}

impl Requests {
    /// Whether nothing is set.
    pub fn is_empty(&self) -> bool {
        self.cpu.is_none()
            && self.memory.is_none()
            && self.gpu.is_none()
            && self.ephemeral_storage.is_none()
            && self.shm.is_none()
    }

    /// Per-axis maximum (`None` on one side takes the other).
    pub fn max(&self, o: &Requests) -> Requests {
        fn mx<T: Ord + Copy>(a: Option<T>, b: Option<T>) -> Option<T> {
            match (a, b) {
                (Some(a), Some(b)) => Some(a.max(b)),
                (a, None) => a,
                (None, b) => b,
            }
        }
        Requests {
            cpu: mx(self.cpu, o.cpu),
            memory: mx(self.memory, o.memory),
            gpu: self.gpu.or(o.gpu),
            ephemeral_storage: mx(self.ephemeral_storage, o.ephemeral_storage),
            shm: mx(self.shm, o.shm),
        }
    }

    /// Per-axis minimum where both are set (`None` = unbounded on that side).
    pub fn min_bound(&self, o: &Requests) -> Requests {
        fn mn<T: Ord + Copy>(a: Option<T>, b: Option<T>) -> Option<T> {
            match (a, b) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (a, None) => a,
                (None, b) => b,
            }
        }
        Requests {
            cpu: mn(self.cpu, o.cpu),
            memory: mn(self.memory, o.memory),
            gpu: self.gpu.or(o.gpu),
            ephemeral_storage: mn(self.ephemeral_storage, o.ephemeral_storage),
            shm: mn(self.shm, o.shm),
        }
    }

    /// Whether `axis` has a value.
    pub fn has(&self, axis: Axis) -> bool {
        match axis {
            Axis::Memory => self.memory.is_some(),
            Axis::Cpu => self.cpu.is_some(),
            Axis::GpuCount | Axis::GpuType => self.gpu.is_some(),
            Axis::EphemeralStorage => self.ephemeral_storage.is_some(),
            Axis::Shm => self.shm.is_some(),
        }
    }
}

/// Resource limits (SPEC §5.1). GPU and shm have no separate limit.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Limits {
    /// CPU limit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu: Option<MilliCpu>,
    /// Memory limit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory: Option<Bytes>,
    /// Ephemeral-storage limit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ephemeral_storage: Option<Bytes>,
}

impl Limits {
    /// Whether no limit is set.
    pub fn is_empty(&self) -> bool {
        self.cpu.is_none() && self.memory.is_none() && self.ephemeral_storage.is_none()
    }
}

/// Requests and limits of one unit.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ResourceSpec {
    /// Requests.
    #[serde(default)]
    pub requests: Requests,
    /// Limits.
    #[serde(default)]
    pub limits: Limits,
}

/// Kubernetes-style QoS class (SPEC §5.1, §7.4).
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum QosClass {
    /// Limits = requests on every axis.
    Guaranteed,
    /// Some request or limit set.
    Burstable,
    /// Nothing set.
    BestEffort,
}

/// What the author declared, plus what platform defaults filled (SPEC §5.1).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Prior {
    /// The declared (origin) spec.
    #[serde(default)]
    pub spec: ResourceSpec,
    /// Axes filled by platform defaults, not the author: not pins (SPEC §7.4).
    #[serde(default)]
    pub defaulted: AxisSet,
    /// QoS class, where the platform has one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub qos: Option<QosClass>,
}

impl Prior {
    /// A prior with only requests.
    pub fn from_requests(requests: Requests) -> Self {
        Prior { spec: ResourceSpec { requests, limits: Limits::default() }, defaulted: AxisSet::empty(), qos: None }
    }
    /// Whether the prior is Guaranteed (explicitly, or limits = requests on
    /// CPU and memory with both set).
    pub fn is_guaranteed(&self) -> bool {
        match self.qos {
            Some(q) => q == QosClass::Guaranteed,
            None => {
                let (r, l) = (&self.spec.requests, &self.spec.limits);
                r.cpu.is_some() && r.memory.is_some() && l.cpu == r.cpu && l.memory == r.memory
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gpu_shape_parse() {
        let g: GpuShape = "A100-80G:2".parse().unwrap();
        assert_eq!(g, GpuShape { accel: AcceleratorId::A100_80G, count: 2 });
        assert_eq!(g.to_string(), "A100-80G:2");
        assert_eq!("l4-24g".parse::<GpuShape>().unwrap().count, 1);
        assert!("A100-80G:0".parse::<GpuShape>().is_err());
        assert_eq!(g.total_memory(), Some(Bytes::gib(158)));
    }

    #[test]
    fn requests_json_omits_unset() {
        let r = Requests { memory: Some(Bytes::gib(1)), ..Default::default() };
        assert_eq!(serde_json::to_string(&r).unwrap(), r#"{"memory":1073741824}"#);
        let p = Prior::from_requests(r);
        let j = serde_json::to_value(&p).unwrap();
        assert_eq!(j["defaulted"], serde_json::json!([]));
    }
}
