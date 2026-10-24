//! Re-export of the shared discovery scan.
//!
//! The merging logic lives in `retexis-discovery` because the GUI needs it
//! too; this module exists so `crate::cli::scan::...` keeps reading well at the
//! call sites.

pub use retexis_discovery::scan::{describe, endpoints, without_self, Scanner};
