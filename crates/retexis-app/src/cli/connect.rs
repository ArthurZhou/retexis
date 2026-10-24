//! Re-export of the shared dial path.
//!
//! `Connector` lives in `retexis-routing` because both applications need it:
//! one dial path, one set of guarantees about scheme resolution and duplicate
//! dials.

pub use retexis_routing::Connector;
