//! Streams: something too big for one envelope.
//!
//! A frame is atomic. A stream is not: it is opened, written in chunks, and
//! closed, and any of those may be lost on the way. The wire format is the one
//! the envelope already reserved:
//!
//! ```text
//!   StreamOpen   payload = u64 stream id | u8 name len | name
//!   StreamChunk  payload = u64 stream id | u32 seq      | data
//!   StreamClose  payload = u64 stream id | u32 chunks
//! ```
//!
//! Everything is big-endian, like the rest of the wire format.
//!
//! # Why the id lives in the payload, not in the envelope
//!
//! Because a relay must not need to know. The envelope header has no room for
//! "which stream", and adding one would mean every forwarder had to understand
//! streams. Keeping it in the payload means a 4 MB file crosses three relays
//! that only ever see `Body::Data`.
//!
//! # What this layer does and does not promise
//!
//! * **In order delivery to the sink.** Chunks may arrive out of order (UDP);
//!   the assembler buffers and releases them in sequence.
//! * **Survives a lost `Open`.** A chunk for an unknown stream implicitly opens
//!   it with an empty name — otherwise one lost packet would make the whole
//!   transfer undeliverable.
//! * **No retransmission.** `StreamSender::reliable` makes every chunk an
//!   acked unicast, which the router retries; without it a stream is as
//!   reliable as the medium.
//! * **No flow control.** A receiver that cannot buffer drops the stream and
//!   says [`StreamEvent::Dropped`]. Real backpressure needs a credit window.

use crate::frame::{Frame, InboundFrame, MAX_FRAME_PAYLOAD};
use retexis_core::{ContentType, MessageId, NodeId};
use retexis_routing::{MeshHandle, SendError};
use std::collections::{BTreeMap, HashMap};
use std::fmt;

/// Envelope overhead inside a stream payload: `u64 id` + `u32 seq`.
const CHUNK_HEADER: usize = 12;
/// `u64 id` + `u8 name_len`.
const OPEN_HEADER: usize = 9;
/// `u64 id` + `u32 chunks`.
const CLOSE_HEADER: usize = 12;
const MAX_NAME: usize = 64;

/// Identifies one stream between two nodes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct StreamId(pub u64);

impl StreamId {
    /// Random, because two senders must not collide on the same receiver.
    pub fn random() -> Self {
        let bytes = MessageId::random();
        let b = bytes.as_bytes();
        let mut raw = [0u8; 8];
        raw.copy_from_slice(&b[..8]);
        StreamId(u64::from_be_bytes(raw))
    }

    pub fn short(self) -> String {
        format!("{:08x}", (self.0 & 0xffff_ffff) as u32)
    }
}

impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.short())
    }
}

#[derive(Debug)]
pub enum StreamError {
    Send(SendError),
    /// The stream was closed (or dropped) and cannot be written to any more.
    Closed,
}

impl fmt::Display for StreamError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StreamError::Send(e) => write!(f, "{e}"),
            StreamError::Closed => f.write_str("stream already closed"),
        }
    }
}

impl std::error::Error for StreamError {}

impl From<SendError> for StreamError {
    fn from(e: SendError) -> Self {
        StreamError::Send(e)
    }
}

// ------------------------------------------------------------------- sender

/// The write half of a stream.
///
/// Dropping it without [`close`](StreamSender::close) leaves the receiver with
/// an open stream that will eventually be evicted — close it.
pub struct StreamSender<'a> {
    handle: &'a MeshHandle,
    dst: Option<NodeId>,
    channel: String,
    id: StreamId,
    seq: u32,
    chunks: u32,
    bytes: usize,
    closed: bool,
    reliable: bool,
    chunk_size: usize,
}

impl<'a> StreamSender<'a> {
    /// Opens a best-effort stream. `dst = None` floods every chunk.
    pub fn open(
        handle: &'a MeshHandle,
        dst: Option<NodeId>,
        channel: impl Into<String>,
        name: &str,
    ) -> Result<Self, StreamError> {
        Self::with_options(handle, dst, channel, name, false)
    }

    /// Opens a stream whose chunks are acked unicasts.
    ///
    /// Costs one ack per chunk, so use it for files over a lossy medium, not
    /// for a terminal session.
    pub fn open_reliable(
        handle: &'a MeshHandle,
        dst: NodeId,
        channel: impl Into<String>,
        name: &str,
    ) -> Result<Self, StreamError> {
        Self::with_options(handle, Some(dst), channel, name, true)
    }

    fn with_options(
        handle: &'a MeshHandle,
        dst: Option<NodeId>,
        channel: impl Into<String>,
        name: &str,
        reliable: bool,
    ) -> Result<Self, StreamError> {
        let name = truncate(name, MAX_NAME);
        let id = StreamId::random();
        let channel = channel.into();
        let mut payload = Vec::with_capacity(OPEN_HEADER + name.len());
        payload.extend_from_slice(&id.0.to_be_bytes());
        payload.push(name.len() as u8);
        payload.extend_from_slice(name.as_bytes());

        let frame = Frame::new(channel.clone(), ContentType::StreamOpen, payload);
        send_one(handle, dst, frame, reliable)?;

        Ok(StreamSender {
            handle,
            dst,
            channel,
            id,
            seq: 0,
            chunks: 0,
            bytes: 0,
            closed: false,
            reliable,
            chunk_size: MAX_FRAME_PAYLOAD - CHUNK_HEADER,
        })
    }

    /// Overrides the chunk size. Smaller chunks interleave better with other
    /// traffic; larger ones waste less on headers.
    ///
    /// The floor is one byte, not a "sensible" 256: a caller that wants tiny
    /// chunks (a test, a demo, a chatty terminal) should get them.
    pub fn with_chunk_size(mut self, bytes: usize) -> Self {
        self.chunk_size = bytes.clamp(1, MAX_FRAME_PAYLOAD - CHUNK_HEADER);
        self
    }

    pub fn id(&self) -> StreamId {
        self.id
    }

    pub fn bytes_written(&self) -> usize {
        self.bytes
    }

    pub fn chunks_written(&self) -> u32 {
        self.chunks
    }

    /// Writes `data`, splitting it across as many chunks as needed.
    /// Returns how many bytes were accepted.
    pub fn write(&mut self, data: &[u8]) -> Result<usize, StreamError> {
        if self.closed {
            return Err(StreamError::Closed);
        }
        let mut written = 0;
        for piece in data.chunks(self.chunk_size) {
            let mut payload = Vec::with_capacity(CHUNK_HEADER + piece.len());
            payload.extend_from_slice(&self.id.0.to_be_bytes());
            payload.extend_from_slice(&self.seq.to_be_bytes());
            payload.extend_from_slice(piece);

            let frame = Frame::new(self.channel.clone(), ContentType::StreamChunk, payload);
            send_one(self.handle, self.dst, frame, self.reliable)?;

            self.seq = self.seq.saturating_add(1);
            self.chunks = self.chunks.saturating_add(1);
            self.bytes += piece.len();
            written += piece.len();
        }
        Ok(written)
    }

    /// Ends the stream, telling the receiver how many chunks to expect.
    pub fn close(&mut self) -> Result<(), StreamError> {
        if self.closed {
            return Err(StreamError::Closed);
        }
        self.closed = true;
        let mut payload = Vec::with_capacity(CLOSE_HEADER);
        payload.extend_from_slice(&self.id.0.to_be_bytes());
        payload.extend_from_slice(&self.chunks.to_be_bytes());
        let frame = Frame::new(self.channel.clone(), ContentType::StreamClose, payload);
        send_one(self.handle, self.dst, frame, self.reliable).map(|_| ())
    }
}

fn send_one(
    handle: &MeshHandle,
    dst: Option<NodeId>,
    frame: Frame,
    reliable: bool,
) -> Result<MessageId, StreamError> {
    match (dst, reliable) {
        (Some(id), true) => Ok(retexis_data_send_reliable(handle, id, frame)?),
        _ => Ok(crate::frame::send(handle, dst, frame)?),
    }
}

fn retexis_data_send_reliable(
    handle: &MeshHandle,
    dst: NodeId,
    frame: Frame,
) -> Result<MessageId, SendError> {
    crate::frame::send_reliable(handle, dst, frame)
}

// ---------------------------------------------------------------- receiver

/// What the assembler produces. A sink gets these instead of raw chunks.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StreamEvent {
    Opened {
        from: NodeId,
        channel: String,
        id: StreamId,
        name: String,
    },
    Data {
        from: NodeId,
        channel: String,
        id: StreamId,
        chunk: Vec<u8>,
    },
    Closed {
        from: NodeId,
        channel: String,
        id: StreamId,
        bytes: usize,
    },
    Dropped {
        from: NodeId,
        channel: String,
        id: StreamId,
        /// The name from the `Open`, when we saw one. Empty otherwise.
        name: String,
        reason: StreamDropReason,
    },
}

impl StreamEvent {
    pub fn channel(&self) -> &str {
        match self {
            StreamEvent::Opened { channel, .. }
            | StreamEvent::Data { channel, .. }
            | StreamEvent::Closed { channel, .. }
            | StreamEvent::Dropped { channel, .. } => channel,
        }
    }

    pub fn from(&self) -> NodeId {
        match self {
            StreamEvent::Opened { from, .. }
            | StreamEvent::Data { from, .. }
            | StreamEvent::Closed { from, .. }
            | StreamEvent::Dropped { from, .. } => *from,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamDropReason {
    /// More concurrent streams than the assembler will track.
    TooManyStreams,
    /// A chunk arrived too far ahead of the expected sequence number.
    OutOfWindow,
    /// `Close` arrived but chunks were still missing.
    MissingChunks,
}

impl fmt::Display for StreamDropReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StreamDropReason::TooManyStreams => f.write_str("too many concurrent streams"),
            StreamDropReason::OutOfWindow => f.write_str("chunk outside the reorder window"),
            StreamDropReason::MissingChunks => f.write_str("closed with chunks missing"),
        }
    }
}

struct Pending {
    name: String,
    /// Next sequence number we still need.
    next: u32,
    /// Chunks that arrived early, waiting for the gap to fill.
    buffered: BTreeMap<u32, Vec<u8>>,
    bytes: usize,
}

/// Reassembles streams from inbound frames.
///
/// One per node: streams from different peers are independent, and the key is
/// `(sender, stream id)`.
pub struct StreamAssembler {
    open: HashMap<(NodeId, u64), Pending>,
    max_open: usize,
    window: u32,
}

impl Default for StreamAssembler {
    fn default() -> Self {
        Self::new()
    }
}

impl StreamAssembler {
    pub fn new() -> Self {
        StreamAssembler {
            open: HashMap::new(),
            max_open: 64,
            window: 256,
        }
    }

    pub fn open_streams(&self) -> usize {
        self.open.len()
    }

    /// Feeds one inbound frame. Returns the events it produced: none for a
    /// duplicate, one for a normal chunk, several when a gap finally fills.
    pub fn accept(&mut self, frame: &InboundFrame) -> Vec<StreamEvent> {
        let channel = frame.frame.channel.clone();
        let from = frame.from;
        let payload = &frame.frame.payload;

        match frame.frame.content_type {
            ContentType::StreamOpen => {
                let Some((id, name)) = parse_open(payload) else {
                    return Vec::new();
                };
                if self.open.contains_key(&(from, id.0)) {
                    return Vec::new(); // duplicate open; the stream is running
                }
                if self.open.len() >= self.max_open {
                    return vec![StreamEvent::Dropped {
                        from,
                        channel,
                        id,
                        name,
                        reason: StreamDropReason::TooManyStreams,
                    }];
                }
                self.open.insert(
                    (from, id.0),
                    Pending {
                        name: name.clone(),
                        next: 0,
                        buffered: BTreeMap::new(),
                        bytes: 0,
                    },
                );
                vec![StreamEvent::Opened {
                    from,
                    channel,
                    id,
                    name,
                }]
            }
            ContentType::StreamChunk => {
                let Some((id, seq, data)) = parse_chunk(payload) else {
                    return Vec::new();
                };
                // A lost Open must not kill the transfer: adopt the chunk.
                if !self.open.contains_key(&(from, id.0)) {
                    if self.open.len() >= self.max_open {
                        return vec![StreamEvent::Dropped {
                            from,
                            channel,
                            id,
                            name: String::new(),
                            reason: StreamDropReason::TooManyStreams,
                        }];
                    }
                    self.open.insert(
                        (from, id.0),
                        Pending {
                            name: String::new(),
                            next: 0,
                            buffered: BTreeMap::new(),
                            bytes: 0,
                        },
                    );
                }

                let mut out = Vec::new();
                let pending = self.open.get_mut(&(from, id.0)).expect("just inserted");
                if seq < pending.next {
                    return Vec::new(); // already delivered
                }
                if seq > pending.next + self.window {
                    return vec![StreamEvent::Dropped {
                        from,
                        channel,
                        id,
                        name: pending.name.clone(),
                        reason: StreamDropReason::OutOfWindow,
                    }];
                }
                pending.buffered.insert(seq, data.to_vec());
                // Release everything contiguous, in order.
                while let Some(chunk) = pending.buffered.remove(&pending.next) {
                    pending.bytes += chunk.len();
                    pending.next += 1;
                    out.push(StreamEvent::Data {
                        from,
                        channel: channel.clone(),
                        id,
                        chunk,
                    });
                }
                out
            }
            ContentType::StreamClose => {
                let Some((id, chunks)) = parse_close(payload) else {
                    return Vec::new();
                };
                let Some(pending) = self.open.remove(&(from, id.0)) else {
                    return Vec::new();
                };
                if !pending.buffered.is_empty() || pending.next < chunks {
                    return vec![StreamEvent::Dropped {
                        from,
                        channel,
                        id,
                        name: pending.name,
                        reason: StreamDropReason::MissingChunks,
                    }];
                }
                vec![StreamEvent::Closed {
                    from,
                    channel,
                    id,
                    bytes: pending.bytes,
                }]
            }
            _ => Vec::new(),
        }
    }

    /// Forgets a stream (peer vanished, or the sink gave up).
    pub fn forget(&mut self, from: NodeId, id: StreamId) {
        self.open.remove(&(from, id.0));
    }
}

fn parse_open(payload: &[u8]) -> Option<(StreamId, String)> {
    if payload.len() < OPEN_HEADER {
        return None;
    }
    let id = StreamId(u64::from_be_bytes(payload[..8].try_into().ok()?));
    let name_len = payload[8] as usize;
    let name = payload.get(9..9 + name_len)?;
    Some((id, String::from_utf8_lossy(name).to_string()))
}

fn parse_chunk(payload: &[u8]) -> Option<(StreamId, u32, &[u8])> {
    if payload.len() < CHUNK_HEADER {
        return None;
    }
    let id = StreamId(u64::from_be_bytes(payload[..8].try_into().ok()?));
    let seq = u32::from_be_bytes(payload[8..12].try_into().ok()?);
    Some((id, seq, &payload[CHUNK_HEADER..]))
}

fn parse_close(payload: &[u8]) -> Option<(StreamId, u32)> {
    if payload.len() < CLOSE_HEADER {
        return None;
    }
    let id = StreamId(u64::from_be_bytes(payload[..8].try_into().ok()?));
    let chunks = u32::from_be_bytes(payload[8..12].try_into().ok()?);
    Some((id, chunks))
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    let mut end = max;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    s[..end].to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use retexis_core::MessageId;

    /// One sender for the whole test. A fresh `NodeId` per frame would make
    /// every frame look like a different stream — which is exactly the bug the
    /// first version of these tests had.
    fn peer() -> NodeId {
        NodeId::random()
    }

    fn inbound(from: NodeId, content_type: ContentType, payload: Vec<u8>) -> InboundFrame {
        InboundFrame {
            msg_id: MessageId::random(),
            from,
            from_name: Some("peer".into()),
            broadcast: false,
            hops: 1,
            created_at_ms: 0,
            frame: Frame::new("file", content_type, payload),
        }
    }

    fn open_frame(from: NodeId, id: StreamId, name: &str) -> InboundFrame {
        let mut p = Vec::new();
        p.extend_from_slice(&id.0.to_be_bytes());
        p.push(name.len() as u8);
        p.extend_from_slice(name.as_bytes());
        inbound(from, ContentType::StreamOpen, p)
    }

    fn chunk_frame(from: NodeId, id: StreamId, seq: u32, data: &[u8]) -> InboundFrame {
        let mut p = Vec::new();
        p.extend_from_slice(&id.0.to_be_bytes());
        p.extend_from_slice(&seq.to_be_bytes());
        p.extend_from_slice(data);
        inbound(from, ContentType::StreamChunk, p)
    }

    fn close_frame(from: NodeId, id: StreamId, chunks: u32) -> InboundFrame {
        let mut p = Vec::new();
        p.extend_from_slice(&id.0.to_be_bytes());
        p.extend_from_slice(&chunks.to_be_bytes());
        inbound(from, ContentType::StreamClose, p)
    }

    #[test]
    fn a_stream_reassembles_in_order() {
        let mut asm = StreamAssembler::new();
        let from = peer();
        let id = StreamId(7);
        let events = asm.accept(&open_frame(from, id, "notes.txt"));
        assert_eq!(events.len(), 1);
        assert!(matches!(&events[0], StreamEvent::Opened { name, .. } if name == "notes.txt"));

        for (seq, text) in [(0u32, "hello "), (1, "world")] {
            let out = asm.accept(&chunk_frame(from, id, seq, text.as_bytes()));
            assert_eq!(out.len(), 1, "each in-order chunk is released at once");
            assert!(matches!(&out[0], StreamEvent::Data { chunk, .. } if chunk == text.as_bytes()));
        }

        let out = asm.accept(&close_frame(from, id, 2));
        assert!(matches!(&out[0], StreamEvent::Closed { bytes: 11, .. }));
        assert_eq!(asm.open_streams(), 0);
    }

    #[test]
    fn out_of_order_chunks_are_buffered_then_released() {
        let mut asm = StreamAssembler::new();
        let from = peer();
        let id = StreamId(9);
        asm.accept(&open_frame(from, id, "x"));

        // seq 1 arrives first: nothing may be delivered yet.
        assert!(asm.accept(&chunk_frame(from, id, 1, b"B")).is_empty());
        // seq 2 also early.
        assert!(asm.accept(&chunk_frame(from, id, 2, b"C")).is_empty());

        // seq 0 fills the gap and releases all three, in order.
        let out = asm.accept(&chunk_frame(from, id, 0, b"A"));
        let data: Vec<Vec<u8>> = out
            .iter()
            .filter_map(|e| match e {
                StreamEvent::Data { chunk, .. } => Some(chunk.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(data, vec![b"A".to_vec(), b"B".to_vec(), b"C".to_vec()]);
    }

    #[test]
    fn a_duplicate_chunk_is_ignored() {
        let mut asm = StreamAssembler::new();
        let from = peer();
        let id = StreamId(11);
        asm.accept(&open_frame(from, id, ""));
        assert_eq!(asm.accept(&chunk_frame(from, id, 0, b"A")).len(), 1);
        assert!(
            asm.accept(&chunk_frame(from, id, 0, b"A")).is_empty(),
            "retransmitted chunk must not be delivered twice"
        );
    }

    #[test]
    fn a_lost_open_does_not_kill_the_stream() {
        let mut asm = StreamAssembler::new();
        let from = peer();
        let id = StreamId(13);
        // No open frame at all.
        let out = asm.accept(&chunk_frame(from, id, 0, b"data"));
        assert!(matches!(&out[0], StreamEvent::Data { .. }));
        let out = asm.accept(&close_frame(from, id, 1));
        assert!(matches!(&out[0], StreamEvent::Closed { bytes: 4, .. }));
    }

    #[test]
    fn a_close_with_a_gap_is_reported_not_hidden() {
        let mut asm = StreamAssembler::new();
        let from = peer();
        let id = StreamId(15);
        asm.accept(&open_frame(from, id, ""));
        asm.accept(&chunk_frame(from, id, 0, b"A"));
        asm.accept(&chunk_frame(from, id, 2, b"C")); // seq 1 is missing
        let out = asm.accept(&close_frame(from, id, 3));
        assert!(matches!(
            &out[0],
            StreamEvent::Dropped {
                reason: StreamDropReason::MissingChunks,
                ..
            }
        ));
        assert_eq!(asm.open_streams(), 0, "a dropped stream is forgotten");
    }

    #[test]
    fn a_chunk_far_ahead_is_rejected() {
        let mut asm = StreamAssembler::new();
        let from = peer();
        let id = StreamId(17);
        asm.accept(&open_frame(from, id, ""));
        let out = asm.accept(&chunk_frame(from, id, 10_000, b"far"));
        assert!(matches!(
            &out[0],
            StreamEvent::Dropped {
                reason: StreamDropReason::OutOfWindow,
                ..
            }
        ));
    }

    #[test]
    fn two_senders_can_use_the_same_stream_id() {
        let mut asm = StreamAssembler::new();
        let (a, b) = (peer(), peer());
        let id = StreamId(21);
        asm.accept(&open_frame(a, id, "from-a"));
        asm.accept(&open_frame(b, id, "from-b"));
        assert_eq!(asm.open_streams(), 2, "the key is (sender, id)");

        let out = asm.accept(&chunk_frame(a, id, 0, b"A"));
        assert_eq!(out.len(), 1);
        // b's stream is untouched.
        let out = asm.accept(&chunk_frame(b, id, 0, b"B"));
        assert!(matches!(&out[0], StreamEvent::Data { chunk, .. } if chunk == b"B"));
    }

    #[test]
    fn stream_ids_are_not_all_zero() {
        let a = StreamId::random();
        let b = StreamId::random();
        assert_ne!(a, b);
        assert_eq!(a.short().len(), 8);
    }
}
