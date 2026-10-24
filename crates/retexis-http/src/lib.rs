//! A tiny HTTP/1.1 server plus a JSON codec, `std` only.
//!
//! Two applications in this workspace need to expose a local JSON API to a
//! browser: the simulator and the GUI. Both need the same twenty things —
//! request line parsing, `content-length`, percent decoding, a response
//! writer — and neither needs a framework.
//!
//! This crate is that shared twenty things, and nothing else: it has no idea
//! what a mesh or a chat is. Each application supplies a routing function.
//!
//! ```text
//!   browser ──▶ serve(handler) ──▶ your handler(parts, &Request) -> Response
//!                    │
//!                    └── one thread per connection, Connection: close
//! ```
//!
//! Why not `axum`/`hyper`? Same reason as the rest of the project: a
//! dependency-free build that works on any target, for an API that only ever
//! talks to `localhost` and a browser tab.

#![forbid(unsafe_code)]

pub mod json;
pub mod server;

pub use json::Json;
pub use server::{
    error, html, json_response, ok, percent_decode, serve, serve_listener, text, with_body,
    Request, Response, HTML, JSON, TEXT,
};
