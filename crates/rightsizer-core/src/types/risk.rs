//! Modes (SPEC §4) and risk profiles (SPEC §7.3).

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use super::{PlatformRetry, TypeError};

/// Mode, ordered `off < observe < recommend < enforce` (SPEC §4).
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    /// Do nothing.
    Off,
    /// Record outcomes only.
    Observe,
    /// Decide and record, change nothing.
    #[default]
    Recommend,
    /// Apply under the safety contract.
    Enforce,
}

impl Mode {
    /// Canonical name.
    pub fn as_str(self) -> &'static str {
        match self {
            Mode::Off => "off",
            Mode::Observe => "observe",
            Mode::Recommend => "recommend",
            Mode::Enforce => "enforce",
        }
    }
}

impl fmt::Display for Mode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Mode {
    type Err = TypeError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "off" => Ok(Mode::Off),
            "observe" => Ok(Mode::Observe),
            "recommend" => Ok(Mode::Recommend),
            "enforce" => Ok(Mode::Enforce),
            _ => Err(TypeError::UnknownMode(s.into())),
        }
    }
}

/// The three named risk profiles (SPEC §7.3).
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum RiskName {
    /// oom 1, delay 1, neighbour 1.
    Lean,
    /// oom 4, delay 1, neighbour 2 (default).
    #[default]
    Balanced,
    /// oom 16, delay 1, neighbour 4.
    Averse,
}

/// Decode-time penalties (SPEC §7.2, §7.3). `oom_penalty` is in multiples of
/// `oracle_cost`; `delay_penalty` in multiples of the oracle's hourly price per
/// hour late; `neighbour_penalty` in multiples of `oracle_cost` × P(under-ask).
#[derive(Copy, Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Penalties {
    /// Per failed attempt.
    pub oom_penalty: f64,
    /// Per hour late.
    #[serde(default = "one")]
    pub delay_penalty: f64,
    /// Neighbour-risk premium when memory is not enforced.
    #[serde(default = "two")]
    pub neighbour_penalty: f64,
}

fn one() -> f64 {
    1.0
}
fn two() -> f64 {
    2.0
}

/// A risk profile: a named one, or custom penalties (SPEC §7.3). JSON:
/// `"balanced"` or `{"oom_penalty": 8, "delay_penalty": 1, "neighbour_penalty": 2}`.
#[derive(Copy, Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(untagged)]
pub enum RiskProfile {
    /// A named profile.
    Named(RiskName),
    /// Custom penalties.
    Custom(Penalties),
}

impl Default for RiskProfile {
    fn default() -> Self {
        RiskProfile::Named(RiskName::Balanced)
    }
}

impl RiskProfile {
    /// `lean`.
    pub const LEAN: RiskProfile = RiskProfile::Named(RiskName::Lean);
    /// `balanced`.
    pub const BALANCED: RiskProfile = RiskProfile::Named(RiskName::Balanced);
    /// `averse`.
    pub const AVERSE: RiskProfile = RiskProfile::Named(RiskName::Averse);

    /// The penalties this profile stands for.
    pub fn penalties(&self) -> Penalties {
        match self {
            RiskProfile::Named(RiskName::Lean) => {
                Penalties { oom_penalty: 1.0, delay_penalty: 1.0, neighbour_penalty: 1.0 }
            }
            RiskProfile::Named(RiskName::Balanced) => {
                Penalties { oom_penalty: 4.0, delay_penalty: 1.0, neighbour_penalty: 2.0 }
            }
            RiskProfile::Named(RiskName::Averse) => {
                Penalties { oom_penalty: 16.0, delay_penalty: 1.0, neighbour_penalty: 4.0 }
            }
            RiskProfile::Custom(p) => *p,
        }
    }

    /// The default profile for a platform: integrations with no ladder and
    /// `same_size = 0` default to `averse`, others to `balanced` (SPEC §7.3).
    pub fn default_for(retry: &PlatformRetry) -> RiskProfile {
        if retry.ladder.is_none() && retry.same_size == 0 { Self::AVERSE } else { Self::BALANCED }
    }
}

impl FromStr for RiskProfile {
    type Err = TypeError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "lean" => Ok(Self::LEAN),
            "balanced" => Ok(Self::BALANCED),
            "averse" => Ok(Self::AVERSE),
            _ => Err(TypeError::UnknownRisk(s.into())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn modes_are_ordered() {
        assert!(Mode::Off < Mode::Observe && Mode::Observe < Mode::Recommend && Mode::Recommend < Mode::Enforce);
        assert_eq!("ENFORCE".parse::<Mode>().unwrap(), Mode::Enforce);
        assert_eq!(serde_json::to_string(&Mode::Observe).unwrap(), "\"observe\"");
    }

    #[test]
    fn risk_json_forms() {
        let r: RiskProfile = serde_json::from_str("\"averse\"").unwrap();
        assert_eq!(r.penalties().oom_penalty, 16.0);
        let c: RiskProfile = serde_json::from_str(r#"{"oom_penalty": 8}"#).unwrap();
        assert_eq!(c.penalties(), Penalties { oom_penalty: 8.0, delay_penalty: 1.0, neighbour_penalty: 2.0 });
        assert_eq!(serde_json::to_string(&RiskProfile::BALANCED).unwrap(), "\"balanced\"");
        assert_eq!(RiskProfile::default_for(&PlatformRetry::default()), RiskProfile::AVERSE);
    }
}
