//! retexis-core — the shared vocabulary of the stack.
//!
//! Three layers, three concerns:
//!
//! ```text
//!   retexis-app      transport  "what does the bytes mean"  (chat, later: file, stream)
//!   retexis-routing  routing    "where do the bytes go"     (ids, routes, flooding)
//!   retexis-core     contract   "what is a byte on the wire" (envelope, Link trait)
//!   retexis-link-tcp medium     "how do the bytes move"     (tcp, later: http/ws/radio)
//! ```
//!
//! This crate is dependency-free on purpose: it is the layer every other
//! crate agrees on, and it must compile on the smallest target in the fleet.

#![forbid(unsafe_code)]

pub mod discovery;
pub mod envelope;
pub mod id;
pub mod link;

pub use discovery::{
    Announcement, Beacon, Endpoint, NoBroadcast, ProbeKind, DEFAULT_PORT, MAX_PROBE, PROBE_MAGIC,
};
pub use envelope::{
    AckBody, Body, CodecError, ContentType, DataBody, Destination, Envelope, HelloBody, RouteBody,
    FLAG_ACK_REQ, FLAG_AUTH, HEADER_LEN, MAGIC, MAX_NAME, MAX_PAYLOAD, VERSION,
};
pub use id::{MessageId, NodeId};
pub use link::{
    next_link_id, split_url, Link, LinkAcceptor, LinkError, LinkFactory, LinkId, TransportRegistry,
};

use std::time::{SystemTime, UNIX_EPOCH};

/// Unix time in milliseconds.
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Formats a byte count for logs.
pub fn human_bytes(n: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut v = n as f64;
    let mut i = 0;
    while v >= 1024.0 && i + 1 < UNITS.len() {
        v /= 1024.0;
        i += 1;
    }
    if i == 0 {
        format!("{n} B")
    } else {
        format!("{v:.1} {}", UNITS[i])
    }
}
