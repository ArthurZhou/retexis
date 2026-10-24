//! The chat application: a [`DataSink`] that turns traffic into timeline
//! entries.
//!
//! Everything the window shows comes through here — there is no other path
//! from the router to the UI. That is deliberate: if a message is visible, it
//! arrived as a `Delivery` and was decoded by the data layer, exactly like it
//! would be in the terminal client.
//!
//! Two shapes arrive, and they are handled differently:
//!
//! * a **frame** is one message → one entry;
//! * a **stream** is a message too big for one envelope → reassembled by
//!   [`retexis_data::StreamAssembler`] before it reaches us as `Data` events,
//!   so the window still shows one entry when it closes.
//!
//! The one thing worth pointing out: our **own** broadcasts come back from the
//! router (hops 0). They are dropped here, because the composer already added
//! the message when it sent it — otherwise every broadcast would appear twice.

use crate::log::{Ack, Log, Message};
use retexis_core::NodeId;
use retexis_data::stream::{StreamEvent, StreamId};
use retexis_data::{DataSink, InboundFrame};
use retexis_routing::MeshHandle;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

/// The channel both ends of a conversation agree on.
pub const CHAT_CHANNEL: &str = "chat";

/// Above this many bytes, a message is sent as a stream instead of a frame.
///
/// One envelope holds 60 KiB, so the threshold is not a hard limit — it is the
/// point where chunking is worth the extra envelopes. A stream also survives
/// a medium whose `max_frame` is smaller than the message.
pub const STREAM_THRESHOLD: usize = 1024;

/// Refuse to buffer more than this for a single inbound stream.
const MAX_STREAM_BYTES: usize = 4 * 1024 * 1024;

/// A stream being reassembled by us.
///
/// The stream's own name is not kept: by the time it closes the message is
/// just text, and the name was only ever useful in the "dropped" case, where
/// the event carries it.
struct Partial {
    from_name: String,
    buf: Vec<u8>,
}

pub struct ChatSink {
    log: Arc<Mutex<Log>>,
    started: Instant,
    me: NodeId,
    partial: HashMap<(NodeId, StreamId), Partial>,
}

impl ChatSink {
    pub fn new(log: Arc<Mutex<Log>>, started: Instant, me: NodeId) -> Self {
        ChatSink {
            log,
            started,
            me,
            partial: HashMap::new(),
        }
    }

    fn at(&self) -> u64 {
        self.started.elapsed().as_millis() as u64
    }

    /// Name for a sender we only know by id.
    ///
    /// `Delivery` carries the name for frames, but stream events do not — the
    /// assembler works on payloads, not on envelopes. So streams ask the
    /// router's name table once, when the stream opens.
    fn name_of(&self, handle: &MeshHandle, from: NodeId) -> String {
        handle
            .status()
            .names
            .get(&from)
            .filter(|n| !n.is_empty())
            .cloned()
            .unwrap_or_else(|| from.short())
    }

    fn push_inbound(&self, f: &InboundFrame, text: String, stream: bool) {
        let bytes = f.frame.len();
        if let Ok(mut log) = self.log.lock() {
            log.message(
                self.at(),
                Message {
                    id: f.msg_id.hex(),
                    from: f.from.hex(),
                    from_name: f.sender(),
                    to: if f.broadcast {
                        None
                    } else {
                        Some(self.me.hex())
                    },
                    text,
                    hops: f.hops,
                    mine: false,
                    ack: Ack::None,
                    stream,
                    bytes,
                },
            );
        }
    }

    fn note(&self, tag: &'static str, text: impl Into<String>) {
        if let Ok(mut log) = self.log.lock() {
            log.system(self.at(), tag, text);
        }
    }
}

impl DataSink for ChatSink {
    fn name(&self) -> &'static str {
        "chat"
    }

    fn channels(&self) -> &'static [&'static str] {
        &[CHAT_CHANNEL]
    }

    fn on_frame(&mut self, _handle: &MeshHandle, frame: &InboundFrame) {
        // Our own broadcast, looped back by the router. The composer already
        // recorded it; showing it again would double every message we send.
        if frame.hops == 0 && frame.from == self.me {
            return;
        }
        let text = match frame.frame.as_text() {
            Some(t) => t.to_string(),
            None => format!("<{} 字节二进制>", frame.frame.len()),
        };
        self.push_inbound(frame, text, false);
    }

    fn on_stream(&mut self, handle: &MeshHandle, event: &StreamEvent) {
        match event {
            StreamEvent::Opened { from, id, .. } => {
                // An open with no data yet: remember where it came from so the
                // final message can be attributed. How it was addressed is not
                // kept here — `Closed` carries that, and the assembler always
                // emits an `Opened` first, even for a stream it adopted after
                // losing the original one.
                self.partial.insert(
                    (*from, *id),
                    Partial {
                        from_name: self.name_of(handle, *from),
                        buf: Vec::new(),
                    },
                );
            }
            StreamEvent::Data {
                from, id, chunk, ..
            } => {
                // A lost `Open` still produces data (the assembler creates the
                // stream implicitly), so this must not assume an entry exists.
                // Resolve the name before touching the map: the lookup needs
                // `&self`, the insert needs `&mut self`.
                if !self.partial.contains_key(&(*from, *id)) {
                    // The assembler announces an adopted stream with `Opened`,
                    // so this is only a belt-and-braces path.
                    let from_name = self.name_of(handle, *from);
                    self.partial.insert(
                        (*from, *id),
                        Partial {
                            from_name,
                            buf: Vec::new(),
                        },
                    );
                }
                let entry = self.partial.get_mut(&(*from, *id)).expect("just inserted");
                if entry.buf.len() + chunk.len() > MAX_STREAM_BYTES {
                    entry.buf.clear();
                    self.partial.remove(&(*from, *id));
                    self.note("stream", "收到的流超过 4 MiB，已丢弃");
                    return;
                }
                entry.buf.extend_from_slice(chunk);
            }
            StreamEvent::Closed {
                from,
                id,
                bytes,
                broadcast,
                hops,
                ..
            } => {
                let Some(part) = self.partial.remove(&(*from, *id)) else {
                    self.note("stream", format!("流 {id} 结束，但没有收到内容"));
                    return;
                };
                let size = if part.buf.is_empty() {
                    *bytes
                } else {
                    part.buf.len()
                };
                let text = match String::from_utf8(part.buf) {
                    Ok(t) => t,
                    Err(_) => format!("<{size} 字节二进制>"),
                };
                if let Ok(mut log) = self.log.lock() {
                    log.message(
                        self.at(),
                        Message {
                            id: format!("stream-{}", id.short()),
                            from: from.hex(),
                            from_name: part.from_name,
                            to: if *broadcast {
                                None
                            } else {
                                Some(self.me.hex())
                            },
                            text,
                            hops: *hops,
                            mine: false,
                            ack: Ack::None,
                            stream: true,
                            bytes: size,
                        },
                    );
                }
            }
            StreamEvent::Dropped {
                from,
                id,
                name,
                reason,
                ..
            } => {
                self.partial.remove(&(*from, *id));
                self.note(
                    "stream",
                    format!(
                        "流 {id}{} 接收失败：{reason}",
                        if name.is_empty() {
                            String::new()
                        } else {
                            format!(" '{name}'")
                        }
                    ),
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log::Kind;
    use retexis_core::{ContentType, MessageId};
    use retexis_data::Frame;

    /// A live router, because `ChatSink` legitimately asks it for names.
    /// The receiver is kept alive by the caller; dropping it would make the
    /// router's `send` fail.
    fn sink() -> (
        ChatSink,
        Arc<Mutex<Log>>,
        NodeId,
        MeshHandle,
        std::sync::mpsc::Receiver<retexis_routing::Delivery>,
    ) {
        let log = Arc::new(Mutex::new(Log::new()));
        let me = NodeId::random();
        let (tx, rx) = std::sync::mpsc::channel();
        let (handle, _thread) = retexis_routing::start(
            retexis_routing::RouterConfig {
                node_id: Some(me),
                node_name: "me".into(),
                ..Default::default()
            },
            tx,
            retexis_routing::silent(),
        );
        (
            ChatSink::new(Arc::clone(&log), Instant::now(), me),
            log,
            me,
            handle,
            rx,
        )
    }

    fn frame(from: NodeId, hops: u8, broadcast: bool, text: &str) -> InboundFrame {
        InboundFrame {
            msg_id: MessageId::random(),
            from,
            from_name: Some("alice".into()),
            broadcast,
            hops,
            created_at_ms: 0,
            frame: Frame::new(CHAT_CHANNEL, ContentType::Text, text.as_bytes().to_vec()),
        }
    }

    fn messages(log: &Arc<Mutex<Log>>) -> Vec<Message> {
        log.lock()
            .unwrap()
            .since(0)
            .filter_map(|e| match &e.kind {
                Kind::Message(m) => Some(m.clone()),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn an_inbound_frame_becomes_one_message() {
        let (mut sink, log, me, handle, _rx) = sink();
        let peer = NodeId::random();
        sink.on_frame(&handle, &frame(peer, 2, false, "hello"));
        let msgs = messages(&log);
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].text, "hello");
        assert_eq!(msgs[0].from_name, "alice");
        assert_eq!(msgs[0].hops, 2);
        assert!(!msgs[0].mine);
        assert_eq!(msgs[0].to, Some(me.hex()));
    }

    #[test]
    fn our_own_loopback_is_not_shown_twice() {
        let (mut sink, log, me, handle, _rx) = sink();
        // hops 0 from ourselves: this is the router echoing our broadcast.
        sink.on_frame(&handle, &frame(me, 0, true, "mine"));
        assert!(messages(&log).is_empty(), "must not duplicate the send");
    }

    #[test]
    fn a_broadcast_stream_is_not_a_direct_message() {
        let (mut sink, log, me, handle, _rx) = sink();
        let peer = NodeId::random();
        let id = StreamId(5);

        sink.on_stream(
            &handle,
            &StreamEvent::Opened {
                from: peer,
                channel: CHAT_CHANNEL.into(),
                id,
                name: "message".into(),
                broadcast: true,
                hops: 1,
            },
        );
        sink.on_stream(
            &handle,
            &StreamEvent::Data {
                from: peer,
                channel: CHAT_CHANNEL.into(),
                id,
                chunk: b"flooded".to_vec(),
            },
        );
        sink.on_stream(
            &handle,
            &StreamEvent::Closed {
                from: peer,
                channel: CHAT_CHANNEL.into(),
                id,
                bytes: 7,
                broadcast: true,
                hops: 1,
            },
        );

        let msgs = messages(&log);
        assert_eq!(msgs.len(), 1, "one stream is one message");
        assert_eq!(msgs[0].text, "flooded");
        assert!(msgs[0].stream);
        assert_eq!(msgs[0].to, None, "a broadcast must not look like a DM");
        assert_eq!(msgs[0].hops, 1);
        let _ = me;
    }

    #[test]
    fn a_broadcast_has_no_recipient() {
        let (mut sink, log, _me, handle, _rx) = sink();
        sink.on_frame(&handle, &frame(NodeId::random(), 1, true, "to all"));
        assert_eq!(messages(&log)[0].to, None);
    }
}
