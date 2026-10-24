//! retexis-gui — a chat window for a real mesh node.
//!
//! This is an **application**, not a protocol: every message it shows came out
//! of [`retexis_data`], and every byte it sends goes back through it. The node
//! underneath is an ordinary [`retexis_routing::Router`] with the ordinary TCP
//! and UDP media, so two GUI instances talk to each other exactly the way two
//! `retexis` daemons do.
//!
//! ```text
//!   native window (Slint)             this process
//!   ┌──────────────┐               ┌──────────────────────────────┐
//!   │ ui/chat.slint│ ─── rows ────▶│ bridge.rs (models, timer)    │
//!   │  timeline    │ ◀── callbacks─│            │                 │
//!   │  peers       │               │            ▼                 │
//!   └──────────────┘               │      ChatSink  ← DataHost    │
//!                                  │            ▲                 │
//!                                  │      Router (real)  ← media  │
//!                                  └──────────────────────────────┘
//! ```
//!
//! The window is [Slint](https://slint.dev) with its **software renderer**:
//! the toolkit computes the layout, lays out the text, and draws it into a
//! plain framebuffer, so there is no GPU and no OpenGL anywhere in this crate.
//! Widgets come from `std-widgets` (`LineEdit`, `ScrollView`, `Spinner`);
//! [`bridge`] is the only code that knows both sides.

// `deny`, not `forbid`: Slint's generated code builds static vtables with
// `unsafe`, and `forbid` cannot be relaxed even inside one module. Everything
// written by hand is still checked; the exception is exactly one module wide.
#![deny(unsafe_code)]

pub mod app;
pub mod boot;
pub mod bridge;
pub mod chat;
pub mod fonts;
pub mod log;
pub mod model;
pub mod shot;

pub use app::{App, Options};
pub use log::{Ack, Entry, Kind, Log, Message};

// The UI itself is a `.slint` file compiled by `build.rs`. Everything the
// generated code needs lives here so `bridge` and `main` can name the types.
#[allow(unsafe_code)]
mod generated {
    slint::include_modules!();
}

pub use generated::*;
