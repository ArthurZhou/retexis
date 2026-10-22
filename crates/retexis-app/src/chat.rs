//! Text chat: the first application on the mesh.
//!
//! Wire shape: `channel = "chat"`, `content_type = Text`, payload = UTF-8.
//! Deliberately the dumbest thing that is still useful: it exercises
//! broadcast, unicast, multi-hop routing and identity without any
//! application-level framing of its own.
//!
//! This is an *application*, not protocol. It implements
//! [`DataSink`](retexis_data::DataSink) — the data layer hands it whole frames
//! and it prints them. Nothing here knows about links, hops or media; the hop
//! count is just a number that arrived with the message.

use crate::term;
use retexis_core::{ContentType, MessageId, NodeId};
use retexis_data::{frame, DataSink, Frame, InboundFrame};
use retexis_routing::{MeshHandle, SendError};

pub const CHAT_CHANNEL: &str = "chat";

/// Sends one chat line. `target = None` broadcasts to the whole mesh.
pub fn send_text(
    handle: &MeshHandle,
    target: Option<NodeId>,
    text: &str,
) -> Result<MessageId, SendError> {
    frame::send(handle, target, Frame::text(CHAT_CHANNEL, text))
}

/// Reliable unicast: acked by the destination, retransmitted by the router.
///
/// Chat DMs use this; broadcasts never do (acking a broadcast turns one packet
/// into N).
pub fn send_reliable(handle: &MeshHandle, dst: NodeId, text: &str) -> Result<MessageId, SendError> {
    frame::send_reliable(handle, dst, Frame::text(CHAT_CHANNEL, text))
}

/// Prints chat frames as they arrive.
pub struct ChatApp {
    me: NodeId,
    show_hops: bool,
}

impl ChatApp {
    pub fn new(handle: &MeshHandle) -> Self {
        ChatApp {
            me: handle.id(),
            show_hops: true,
        }
    }

    pub fn with_hops(mut self, show: bool) -> Self {
        self.show_hops = show;
        self
    }

    fn label(&self, frame: &InboundFrame) -> String {
        let is_me = frame.from == self.me;
        let who = if is_me {
            "me".to_string()
        } else {
            frame.sender()
        };
        let dest = if frame.broadcast {
            "*"
        } else if is_me {
            "?"
        } else {
            "me"
        };
        let hops = if self.show_hops && frame.hops > 0 {
            format!(" ({}h)", frame.hops)
        } else {
            String::new()
        };
        format!("{who} → {dest}{hops}")
    }
}

impl DataSink for ChatApp {
    fn name(&self) -> &'static str {
        "chat"
    }

    fn channels(&self) -> &'static [&'static str] {
        &[CHAT_CHANNEL]
    }

    fn on_frame(&mut self, _handle: &MeshHandle, frame: &InboundFrame) {
        let label = self.label(frame);
        match frame.frame.content_type {
            ContentType::Text => {
                let body = String::from_utf8_lossy(&frame.frame.payload);
                term::line(format!("{label:<22} {body}"));
            }
            other => {
                term::line(format!(
                    "{label:<22} <{} payload, {} bytes>",
                    other.label(),
                    frame.frame.payload.len()
                ));
            }
        }
    }

    /// Chat does not stream, but a `pty` sink would print chunks here instead.
    fn on_stream(&mut self, _handle: &MeshHandle, event: &retexis_data::StreamEvent) {
        use retexis_data::StreamEvent::*;
        match event {
            Opened { id, name, .. } => term::line(format!("… stream {id} '{name}' opened")),
            Data { id, chunk, .. } => term::line(format!(
                "… stream {id} {} byte{}",
                chunk.len(),
                if chunk.len() == 1 { "" } else { "s" }
            )),
            Closed { id, bytes, .. } => term::line(format!("… stream {id} closed ({bytes} bytes)")),
            Dropped {
                id, name, reason, ..
            } => term::line(format!(
                "… stream {id}{} dropped: {reason}",
                if name.is_empty() {
                    String::new()
                } else {
                    format!(" '{name}'")
                }
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use retexis_core::MessageId;

    fn frame_from(from: NodeId, name: Option<&str>, broadcast: bool, hops: u8) -> InboundFrame {
        InboundFrame {
            msg_id: MessageId::random(),
            from,
            from_name: name.map(|s| s.to_string()),
            broadcast,
            hops,
            created_at_ms: 0,
            frame: Frame::text(CHAT_CHANNEL, "hi"),
        }
    }

    fn app(me: NodeId) -> ChatApp {
        ChatApp {
            me,
            show_hops: true,
        }
    }

    #[test]
    fn labels_read_like_a_chat_line() {
        let me = NodeId::random();
        let other = NodeId::random();
        let app = app(me);
        assert_eq!(
            app.label(&frame_from(other, Some("bob"), true, 2)),
            "bob → * (2h)"
        );
        assert_eq!(
            app.label(&frame_from(other, None, false, 1)),
            format!("{} → me (1h)", other.short())
        );
        assert_eq!(app.label(&frame_from(me, None, true, 0)), "me → *");
    }

    #[test]
    fn hops_can_be_hidden() {
        let me = NodeId::random();
        let other = NodeId::random();
        let app = app(me).with_hops(false);
        assert_eq!(
            app.label(&frame_from(other, Some("bob"), true, 3)),
            "bob → *"
        );
    }
}
