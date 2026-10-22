//! Frames: one message, one envelope.
//!
//! A frame is the smallest unit the data layer knows about. It carries a
//! channel name (so several protocols can share one mesh without negotiating),
//! a content type (so a receiver knows how to read the bytes without guessing),
//! and the payload.
//!
//! The channel is *not* an identifier of a conversation — it is a demux key.
//! `chat` and `pty` can be in flight at the same time and neither knows the
//! other exists.

use retexis_core::{ContentType, MessageId, NodeId};
use retexis_routing::{Delivery, MeshHandle, SendError};

/// Largest payload a single frame can carry. Matches the envelope cap; a
/// caller that needs more uses a stream instead of a bigger frame.
pub const MAX_FRAME_PAYLOAD: usize = retexis_core::MAX_PAYLOAD;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Frame {
    pub channel: String,
    pub content_type: ContentType,
    pub payload: Vec<u8>,
}

impl Frame {
    pub fn new(
        channel: impl Into<String>,
        content_type: ContentType,
        payload: impl Into<Vec<u8>>,
    ) -> Self {
        Frame {
            channel: channel.into(),
            content_type,
            payload: payload.into(),
        }
    }

    pub fn text(channel: impl Into<String>, text: &str) -> Self {
        Frame::new(channel, ContentType::Text, text.as_bytes().to_vec())
    }

    pub fn binary(channel: impl Into<String>, bytes: impl Into<Vec<u8>>) -> Self {
        Frame::new(channel, ContentType::Binary, bytes)
    }

    pub fn json(channel: impl Into<String>, json: &str) -> Self {
        Frame::new(channel, ContentType::Json, json.as_bytes().to_vec())
    }

    /// True when this frame belongs to a stream rather than standing alone.
    pub fn is_stream(&self) -> bool {
        self.content_type.is_stream()
    }

    /// Payload as UTF-8, if it is valid. Lossy conversion is the caller's
    /// choice — silently mangling binary data would hide bugs.
    pub fn as_text(&self) -> Option<&str> {
        std::str::from_utf8(&self.payload).ok()
    }

    pub fn len(&self) -> usize {
        self.payload.len()
    }

    pub fn is_empty(&self) -> bool {
        self.payload.is_empty()
    }
}

/// A frame that arrived at this node, with the routing metadata attached.
#[derive(Clone, Debug)]
pub struct InboundFrame {
    pub msg_id: MessageId,
    pub from: NodeId,
    pub from_name: Option<String>,
    pub broadcast: bool,
    /// Hops travelled. `0` means we sent it ourselves and it looped back.
    pub hops: u8,
    pub created_at_ms: u64,
    pub frame: Frame,
}

impl InboundFrame {
    pub fn from_delivery(delivery: &Delivery) -> Self {
        InboundFrame {
            msg_id: delivery.msg_id,
            from: delivery.from,
            from_name: delivery.from_name.clone().filter(|s| !s.is_empty()),
            broadcast: delivery.broadcast,
            hops: delivery.hops,
            created_at_ms: delivery.created_at_ms,
            frame: Frame {
                channel: delivery.channel.clone(),
                content_type: delivery.content_type,
                payload: delivery.payload.clone(),
            },
        }
    }

    /// Best available label for the sender: its name, else its short id.
    pub fn sender(&self) -> String {
        match &self.from_name {
            Some(name) => name.clone(),
            None => self.from.short(),
        }
    }
}

/// Sends one frame. `dst = None` floods it to the whole mesh.
pub fn send(
    handle: &MeshHandle,
    dst: Option<NodeId>,
    frame: Frame,
) -> Result<MessageId, SendError> {
    match dst {
        None => handle.broadcast(&frame.channel, frame.content_type, frame.payload),
        Some(id) => handle.send_to(id, &frame.channel, frame.content_type, frame.payload),
    }
}

/// Sends one frame and waits for the destination's ack.
///
/// Unicast only: acking a broadcast would turn one packet into N.
pub fn send_reliable(
    handle: &MeshHandle,
    dst: NodeId,
    frame: Frame,
) -> Result<MessageId, SendError> {
    handle.send_reliable(dst, &frame.channel, frame.content_type, frame.payload)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constructors_set_the_content_type() {
        assert_eq!(Frame::text("chat", "hi").content_type, ContentType::Text);
        assert_eq!(
            Frame::binary("file", vec![1u8, 2]).content_type,
            ContentType::Binary
        );
        assert_eq!(Frame::json("m", "{}").content_type, ContentType::Json);
        assert_eq!(Frame::text("chat", "hi").len(), 2);
        assert!(Frame::text("chat", "").is_empty());
    }

    #[test]
    fn stream_frames_are_marked() {
        assert!(Frame::new("file", ContentType::StreamChunk, vec![]).is_stream());
        assert!(!Frame::text("chat", "x").is_stream());
    }

    #[test]
    fn text_accessor_refuses_invalid_utf8() {
        assert_eq!(Frame::text("chat", "你好").as_text(), Some("你好"));
        assert_eq!(Frame::binary("bin", vec![0xff, 0xfe]).as_text(), None);
    }
}
