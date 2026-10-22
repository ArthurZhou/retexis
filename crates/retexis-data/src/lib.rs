//! retexis-data — the data layer.
//!
//! Routing moves opaque bytes between node ids. This crate decides what those
//! bytes *are*, and there are exactly two shapes:
//!
//! * a [`Frame`] — one self-contained message on a channel (`chat`, `metrics`,
//!   `pty`). Everything fits in one envelope, so it is one transmission.
//! * a [`Stream`](stream) — something too big or too slow for one message:
//!   opened, written in chunks, closed. A 4 MB file or a terminal session.
//!
//! Both ride the same wire. The envelope's `content_type` says which:
//! `StreamOpen` / `StreamChunk` / `StreamClose` mark stream traffic, everything
//! else is a frame. That means a stream can cross a relay that has no idea what
//! a stream is — the router only ever sees envelopes.
//!
//! # Where the boundary is
//!
//! ```text
//!   retexis-data    "a message / a stream, and who consumes it"   ← this crate
//!   retexis-routing "which node, how many hops, did it arrive"
//!   retexis-core    "what is a byte on the wire"
//! ```
//!
//! Nothing here prints, parses arguments, or knows what a terminal is. The
//! application layer (`retexis-app`) implements [`DataSink`] and does that.
//! That split is the point: adding a `pty` channel means writing a sink, not
//! touching the protocol.
//!
//! # Deliberate omissions
//!
//! * **No framing of its own beyond the stream header.** Frames are one
//!   envelope; the medium already framed it.
//! * **No retransmission.** Reliable delivery is the router's job
//!   (`send_reliable`), and a stream can opt in per chunk.
//! * **No flow control.** A fast sender can outrun a slow receiver; the
//!   receiver drops what it cannot buffer and says so with
//!   [`StreamEvent::Dropped`]. Real backpressure needs a credit window, which
//!   needs the router to expose queue depth — not there yet.

#![forbid(unsafe_code)]

pub mod frame;
pub mod sink;
pub mod stream;

pub use frame::{send, send_reliable, Frame, InboundFrame};
pub use sink::{DataHost, DataSink};
pub use stream::{StreamAssembler, StreamEvent, StreamId, StreamSender};
