//! Delivery: which sink gets which channel.
//!
//! The data layer's last job is demultiplexing. One mesh carries several
//! protocols at once — chat, a file transfer, a terminal session — and each
//! declares the channel names it claims. Everything else is somebody else's
//! problem.
//!
//! ```text
//!   Delivery (from routing)
//!        │
//!        ├── content_type is a stream?  → StreamAssembler → on_stream
//!        └── otherwise                  → on_frame
//! ```
//!
//! A sink that only cares about whole messages never sees a stream, and vice
//! versa. Neither knows the other exists.

use crate::frame::InboundFrame;
use crate::stream::{StreamAssembler, StreamEvent};
use retexis_routing::{Delivery, MeshHandle};

/// One protocol riding on the mesh.
///
/// `on_frame` is required because every sink handles at least whole messages.
/// `on_stream` is optional: most sinks will never see one.
pub trait DataSink: Send {
    fn name(&self) -> &'static str;

    /// Channel names this sink claims. A delivery on any other channel is not
    /// passed to it.
    fn channels(&self) -> &'static [&'static str];

    fn on_frame(&mut self, handle: &MeshHandle, frame: &InboundFrame);

    /// Called for reassembled stream traffic on a claimed channel.
    fn on_stream(&mut self, _handle: &MeshHandle, _event: &StreamEvent) {}
}

/// Routes deliveries to the sinks that claim their channel.
#[derive(Default)]
pub struct DataHost {
    sinks: Vec<Box<dyn DataSink>>,
    assembler: StreamAssembler,
}

impl DataHost {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, sink: Box<dyn DataSink>) {
        self.sinks.push(sink);
    }

    pub fn names(&self) -> Vec<&'static str> {
        self.sinks.iter().map(|s| s.name()).collect()
    }

    pub fn channels(&self) -> Vec<&'static str> {
        self.sinks
            .iter()
            .flat_map(|s| s.channels().iter().copied())
            .collect()
    }

    /// How many streams are half-received right now.
    pub fn open_streams(&self) -> usize {
        self.assembler.open_streams()
    }

    /// Returns `false` when nobody claimed the channel, which the caller
    /// reports — a misconfigured channel should not fail silently.
    pub fn dispatch(&mut self, handle: &MeshHandle, delivery: &Delivery) -> bool {
        let inbound = InboundFrame::from_delivery(delivery);
        if inbound.frame.is_stream() {
            return self.dispatch_stream(handle, &inbound);
        }
        let mut claimed = false;
        for sink in &mut self.sinks {
            if sink.channels().iter().any(|c| *c == inbound.frame.channel) {
                sink.on_frame(handle, &inbound);
                claimed = true;
            }
        }
        claimed
    }

    fn dispatch_stream(&mut self, handle: &MeshHandle, inbound: &InboundFrame) -> bool {
        let channel = inbound.frame.channel.clone();
        let events = self.assembler.accept(inbound);
        let mut claimed = !events.is_empty();
        for event in &events {
            for sink in &mut self.sinks {
                if sink.channels().iter().any(|c| *c == channel) {
                    sink.on_stream(handle, event);
                    claimed = true;
                }
            }
        }
        // A stream chunk that produced no event (a duplicate, say) is still
        // claimed: the channel is ours, there is just nothing new to report.
        claimed || self.channels().iter().any(|c| *c == channel)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use retexis_core::{ContentType, MessageId, NodeId};
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Default)]
    struct Recorder {
        frames: Arc<Mutex<Vec<String>>>,
        streams: Arc<Mutex<Vec<String>>>,
    }

    impl DataSink for Recorder {
        fn name(&self) -> &'static str {
            "recorder"
        }
        fn channels(&self) -> &'static [&'static str] {
            &["chat"]
        }
        fn on_frame(&mut self, _handle: &MeshHandle, frame: &InboundFrame) {
            self.frames
                .lock()
                .unwrap()
                .push(frame.frame.as_text().unwrap_or("<binary>").to_string());
        }
        fn on_stream(&mut self, _handle: &MeshHandle, event: &StreamEvent) {
            self.streams.lock().unwrap().push(format!("{event:?}"));
        }
    }

    fn delivery_from(
        from: NodeId,
        channel: &str,
        content_type: ContentType,
        payload: Vec<u8>,
    ) -> Delivery {
        Delivery {
            msg_id: MessageId::random(),
            from,
            from_name: Some("peer".into()),
            broadcast: false,
            channel: channel.to_string(),
            content_type,
            payload,
            hops: 1,
            created_at_ms: 0,
        }
    }

    fn delivery(channel: &str, content_type: ContentType, payload: Vec<u8>) -> Delivery {
        delivery_from(NodeId::random(), channel, content_type, payload)
    }

    #[test]
    fn frames_reach_the_sink_that_claims_the_channel() {
        let rec = Recorder::default();
        let frames = Arc::clone(&rec.frames);
        let mut host = DataHost::new();
        host.register(Box::new(rec));
        // No router is needed to route a frame to a sink; the handle is only
        // there for replies.
        let (tx, _rx) = std::sync::mpsc::channel();
        let (handle, _t) = retexis_routing::start(
            retexis_routing::RouterConfig::default(),
            tx,
            retexis_routing::silent(),
        );

        assert!(host.dispatch(
            &handle,
            &delivery("chat", ContentType::Text, b"hi".to_vec())
        ));
        assert_eq!(*frames.lock().unwrap(), vec!["hi".to_string()]);

        assert!(
            !host.dispatch(
                &handle,
                &delivery("metrics", ContentType::Text, b"x".to_vec())
            ),
            "an unclaimed channel must be reported, not swallowed"
        );
        assert_eq!(frames.lock().unwrap().len(), 1);
    }

    #[test]
    fn stream_chunks_go_to_on_stream_not_on_frame() {
        let rec = Recorder::default();
        let frames = Arc::clone(&rec.frames);
        let streams = Arc::clone(&rec.streams);
        let mut host = DataHost::new();
        host.register(Box::new(rec));
        let (tx, _rx) = std::sync::mpsc::channel();
        let (handle, _t) = retexis_routing::start(
            retexis_routing::RouterConfig::default(),
            tx,
            retexis_routing::silent(),
        );

        let peer = NodeId::random();
        let id = 42u64;
        let mut open = Vec::new();
        open.extend_from_slice(&id.to_be_bytes());
        open.push(4);
        open.extend_from_slice(b"note");
        assert!(host.dispatch(
            &handle,
            &delivery_from(peer, "chat", ContentType::StreamOpen, open)
        ));

        let mut chunk = Vec::new();
        chunk.extend_from_slice(&id.to_be_bytes());
        chunk.extend_from_slice(&0u32.to_be_bytes());
        chunk.extend_from_slice(b"payload");
        assert!(host.dispatch(
            &handle,
            &delivery_from(peer, "chat", ContentType::StreamChunk, chunk)
        ));

        assert!(
            frames.lock().unwrap().is_empty(),
            "stream traffic must not hit on_frame"
        );
        let seen = streams.lock().unwrap();
        assert_eq!(seen.len(), 2, "opened + one chunk: {seen:?}");
        assert!(seen[0].contains("Opened"), "{seen:?}");
        // `Debug` on a Vec<u8> shows bytes, not text — check the variant and
        // the length instead of the content.
        assert!(seen[1].contains("Data"), "{seen:?}");
        assert!(
            seen[1].contains("112, 97, 121"),
            "the chunk bytes: {seen:?}"
        );
        assert_eq!(host.open_streams(), 1);
    }

    #[test]
    fn the_host_reports_what_it_can_serve() {
        let mut host = DataHost::new();
        host.register(Box::new(Recorder::default()));
        assert_eq!(host.names(), vec!["recorder"]);
        assert_eq!(host.channels(), vec!["chat"]);
        assert_eq!(host.open_streams(), 0);
    }
}
