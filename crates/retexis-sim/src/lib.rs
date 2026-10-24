//! retexis-sim — an interactive simulator built on the real protocol.
//!
//! The important claim of this crate: **it does not re-implement the protocol**.
//! Each simulated node is a real [`retexis_routing::Router`] on its own thread,
//! reached through the ordinary [`retexis_core::Link`] trait. The simulator
//! supplies a different *medium* (a virtual switch, see [`hub`]) and some
//! presentation state (positions, inboxes, an event log).
//!
//! ```text
//!   browser  ──HTTP/JSON──▶  http.rs  ──▶  sim.rs  ──▶  hub.rs
//!                                              │            │
//!                                              └──▶ real Routers (one thread each)
//! ```
//!
//! What is real here: handshake, distance-vector convergence, flooding and
//! dedup, TTL, ack + retransmission, flood limiting, HMAC authentication, and
//! the exact wire format. What is simulated: the wire itself.

#![forbid(unsafe_code)]

pub mod http;
pub mod hub;
pub mod sim;

/// Re-exported so `retexis_sim::json::Json` still names the type used by the
/// API; the codec itself lives in `retexis-http`.
pub use retexis_http::json;

pub use hub::{Hub, HubCounters, HubLink, LinkKey, LinkParams, LinkView};
pub use sim::{layout, Sim};
