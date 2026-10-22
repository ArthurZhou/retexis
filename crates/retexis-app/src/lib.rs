//! retexis-app — the application layer.
//!
//! Everything here is *application*, not protocol:
//!
//! * [`chat`] — one application on the mesh: text messages on the `chat`
//!   channel. It implements [`DataSink`](retexis_data::DataSink) and prints.
//! * [`cli`] — the `retexis` command: arguments, the REPL, the network view.
//! * [`term`] — line-oriented output with a lock, so a background delivery
//!   thread cannot interleave mid-word with the prompt.
//!
//! The split that matters:
//!
//! ```text
//!   retexis-data   what a message / a stream is, and who consumes it   (protocol)
//!   retexis-app    chat, terminal, REPL, tables                        (application)
//! ```
//!
//! Nothing in this crate defines a wire format, a channel number, or a retry
//! policy. Nothing in `retexis-data` prints a line or parses an argument.
//! Adding a `pty` application means writing a `DataSink` here; the data layer
//! does not change.

#![forbid(unsafe_code)]

pub mod chat;
pub mod cli;
pub mod term;

pub use chat::{send_reliable, send_text, ChatApp, CHAT_CHANNEL};
pub use term::{line, lines};
