//! The data model of SPEC §5. Every type is a `serde` struct with stable
//! `snake_case` JSON; with the `schema` feature each also derives
//! `schemars::JsonSchema` (`cargo xtask schemas`).

mod accel;
mod axis;
mod context;
mod decision;
mod estimate;
mod key;
mod outcome;
mod platform;
mod quantity;
mod resources;
mod risk;

pub use accel::*;
pub use axis::*;
pub use context::*;
pub use decision::*;
pub use estimate::*;
pub use key::*;
pub use outcome::*;
pub use platform::*;
pub use quantity::*;
pub use resources::*;
pub use risk::*;

/// Errors from parsing or validating core types.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TypeError {
    /// An axis name that is neither canonical nor an alias.
    #[error("unknown axis {0:?}")]
    UnknownAxis(String),
    /// An accelerator id outside the catalog (SPEC §5.1.1).
    #[error("unknown accelerator id {0:?}")]
    UnknownAccelerator(String),
    /// A malformed `{accel}:{count}` GPU shape.
    #[error("invalid GPU shape {0:?}")]
    InvalidGpuShape(String),
    /// A malformed workload key.
    #[error("invalid workload key {key:?}: {reason}")]
    InvalidKey {
        /// The offending input.
        key: String,
        /// Why it was rejected.
        reason: &'static str,
    },
    /// A malformed tenant.
    #[error("invalid tenant {0:?}")]
    InvalidTenant(String),
    /// An unknown mode name.
    #[error("unknown mode {0:?}")]
    UnknownMode(String),
    /// An unknown risk profile name.
    #[error("unknown risk profile {0:?}")]
    UnknownRisk(String),
}
