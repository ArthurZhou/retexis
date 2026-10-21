//! Text chat: the first transport, and the template for the next ones.
//!
//! Wire shape: `channel = "chat"`, `content_type = Text`, payload = UTF-8.
//! That is deliberately the dumbest possible thing that is still useful. It
//! exercises broadcast, unicast, multi-hop routing and identity without
//! needing any application-level framing of its own.

use crate::term;
use crate::Transport;
use retexis_core::{ContentType, MessageId, NodeId};
use retexis_routing::{Delivery, MeshHandle, SendError};

pub const CHAT_CHANNEL: &str = "chat";

/// Sends one chat line. `target = None` broadcasts to the whole mesh.
pub fn send_text(
    handle: &MeshHandle,
    target: Option<NodeId>,
    text: &str,
) -> Result<MessageId, SendError> {
    let bytes = text.as_bytes().to_vec();
    match target {
        None => handle.broadcast(CHAT_CHANNEL, ContentType::Text, bytes),
        Some(id) => handle.send_to(id, CHAT_CHANNEL, ContentType::Text, bytes),
    }
}

pub struct ChatTransport {
    me: NodeId,
    show_hops: bool,
}

impl ChatTransport {
    pub fn new(handle: &MeshHandle) -> Self {
        ChatTransport {
            me: handle.id(),
            show_hops: true,
        }
    }

    pub fn with_hops(mut self, show: bool) -> Self {
        self.show_hops = show;
        self
    }

    fn label(&self, d: &Delivery) -> String {
        let is_me = d.from == self.me;
        let who = if is_me {
            "me".to_string()
        } else {
            d.from_name
                .clone()
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| d.from.short())
        };
        let dest = if d.broadcast {
            "*"
        } else if is_me {
            "?"
        } else {
            "me"
        };
        let hops = if self.show_hops && d.hops > 0 {
            format!(" ({}h)", d.hops)
        } else {
            String::new()
        };
        format!("{who} → {dest}{hops}")
    }
}

impl Transport for ChatTransport {
    fn name(&self) -> &'static str {
        "chat"
    }

    fn channels(&self) -> &'static [&'static str] {
        &[CHAT_CHANNEL]
    }

    fn on_delivery(&mut self, _handle: &MeshHandle, d: &Delivery) {
        let label = self.label(d);
        match d.content_type {
            ContentType::Text => {
                let body = String::from_utf8_lossy(&d.payload);
                term::line(format!("{label:<22} {body}"));
            }
            other => {
                term::line(format!(
                    "{label:<22} <{} payload, {} bytes>",
                    other.label(),
                    d.payload.len()
                ));
            }
        }
    }
}

/// Reliable unicast: acked by the destination, retransmitted by the router.
///
/// Chat DMs use this; broadcasts never do (acking a broadcast turns one packet
/// into N).
pub fn send_reliable(handle: &MeshHandle, dst: NodeId, text: &str) -> Result<MessageId, SendError> {
    handle.send_reliable(
        dst,
        CHAT_CHANNEL,
        ContentType::Text,
        text.as_bytes().to_vec(),
    )
}
