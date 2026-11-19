//! `rightsizer-core`: the pure, synchronous heart of rightsizer.
//!
//! Everything here is deterministic and free of I/O (ARCHITECTURE.md §3): the
//! data model (SPEC §5), platform quantity parsing ([`units`]), grids
//! ([`grid`], SPEC §5.3), estimate normalization ([`normalize`], SPEC §5.5),
//! the cost model ([`cost`], SPEC §7.2), constraints ([`constraints`], SPEC
//! §7.4), the safety checks ([`safety`], SPEC §2.3 S3/S5), breaker logic
//! ([`breaker`], SPEC §2.3 S5), the decision rule ([`decide`], SPEC §7) and
//! mode resolution ([`policy`]).
//!
//! Wire conventions (ARCHITECTURE.md §4): quantities are integers (bytes,
//! millicores), enums are `snake_case` strings, tagged enums use
//! `{"kind": ...}`, timestamps are RFC 3339 UTC, ids are ULIDs and workload
//! keys are strings `{platform}://{tenant}/{suffix}`.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod breaker;
pub mod clock;
pub mod constraints;
pub mod cost;
pub mod decide;
pub mod digest;
pub mod grid;
pub mod normalize;
pub mod policy;
pub mod safety;
pub mod stats;
pub mod types;
pub mod units;

pub use decide::{DecisionInput, DecisionParams, decide};
pub use grid::Grid;
pub use types::*;

/// Current wire version written into every `"v"` field (SPEC §5, §13.2).
pub const WIRE_VERSION: u16 = 1;

pub(crate) fn wire_v() -> u16 {
    WIRE_VERSION
}
