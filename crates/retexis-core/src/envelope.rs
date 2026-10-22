//! The retexis wire format.
//!
//! Every packet on the wire is one [`Envelope`]. The encoding is hand rolled
//! (no serde) for three reasons:
//!
//! 1. It is stable and self describing, so a packet captured today can be
//!    decoded by a tool written in another language tomorrow.
//! 2. It is allocation-light and predictable, which matters on constrained
//!    links (radio, embedded).
//! 3. The header is a *fixed* 64 bytes, so a link can decide whether a packet
//!    fits its MTU without understanding the body.
//!
//! ```text
//! offset  size  field
//!      0     2  magic  "RX" (0x52 0x58)
//!      2     1  version  (2)
//!      3     1  flags        0x01 = authenticated (tag appended)
//!                            0x02 = sender wants an ack
//!      4     1  ttl          (remaining hops)
//!      5     1  hops         (hops already travelled)
//!      6     1  dst_kind     0 = broadcast, 1 = unicast
//!      7     1  body_kind    0 data, 1 hello, 2 route, 3 bye, 4 ack
//!      8    16  message id
//!     24    16  source node id
//!     40    16  destination node id (all zero when broadcast)
//!     56     8  created_at, unix millis, big endian
//!     64     -  body
//!      -    16  auth tag, only when flags & 0x01  (truncated HMAC-SHA256
//!               over every byte before it, i.e. header + body)
//!
//! body(data)  : u8 channel_len | channel bytes | u8 content_type | u32 payload_len | payload
//! body(hello) : u8 name_len    | name bytes | u64 sender's id for this link
//! body(route) : u16 count      | count * (16 bytes node id | u8 metric | u8 name_len | name)
//! body(ack)   : 16 bytes acked message id
//! body(bye)   : <empty>
//! ```

use crate::id::{MessageId, NodeId};
use crate::now_ms;
use std::fmt;

pub const MAGIC: [u8; 2] = *b"RX";
pub const VERSION: u8 = 2;
pub const HEADER_LEN: usize = 64;
/// Hard cap on the application payload of a single envelope.
pub const MAX_PAYLOAD: usize = 60 * 1024;
/// Node names are truncated to this many bytes on the wire. They are a
/// convenience label, not an identity.
pub const MAX_NAME: usize = 48;

/// This envelope carries an authentication tag as its last 16 bytes.
pub const FLAG_AUTH: u8 = 0x01;
/// The sender wants the destination to acknowledge receipt (unicast only).
pub const FLAG_ACK_REQ: u8 = 0x02;

/// Truncates on a UTF-8 boundary so encoding never panics on a long name.
pub fn truncate_utf8(s: &str, max: usize) -> &str {
    if s.len() <= max {
        return s;
    }
    let mut end = max;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodecError {
    TooShort(usize),
    BadMagic([u8; 2]),
    BadVersion(u8),
    BadKind(u8),
    UnknownContentType(u8),
    TooLarge(usize),
    Truncated,
    InvalidUtf8,
    FieldTooLong(&'static str),
}

impl fmt::Display for CodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CodecError::TooShort(n) => write!(f, "packet too short: {n} bytes"),
            CodecError::BadMagic(m) => write!(f, "bad magic {m:02x?}"),
            CodecError::BadVersion(v) => write!(f, "unsupported wire version {v}"),
            CodecError::BadKind(k) => write!(f, "unknown body kind {k}"),
            CodecError::UnknownContentType(c) => write!(f, "unknown content type {c}"),
            CodecError::TooLarge(n) => write!(f, "payload too large: {n} bytes"),
            CodecError::Truncated => write!(f, "truncated packet"),
            CodecError::InvalidUtf8 => write!(f, "invalid utf-8 in a string field"),
            CodecError::FieldTooLong(f2) => write!(f, "field too long: {f2}"),
        }
    }
}

impl std::error::Error for CodecError {}

/// What the packet is addressed to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Destination {
    Broadcast,
    Node(NodeId),
}

impl Destination {
    pub fn is_broadcast(&self) -> bool {
        matches!(self, Destination::Broadcast)
    }

    pub fn target(&self) -> Option<NodeId> {
        match self {
            Destination::Broadcast => None,
            Destination::Node(n) => Some(*n),
        }
    }

    pub fn as_u8(&self) -> u8 {
        match self {
            Destination::Broadcast => 0,
            Destination::Node(_) => 1,
        }
    }
}

/// Payload semantics. `Text` and `Binary` are in use today; the stream
/// variants exist so a future streaming transport can reuse the same envelope
/// without a wire-format bump.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ContentType {
    Text = 0,
    Binary = 1,
    Json = 2,
    StreamOpen = 3,
    StreamChunk = 4,
    StreamClose = 5,
}

impl ContentType {
    pub fn as_u8(self) -> u8 {
        self as u8
    }

    pub fn from_u8(v: u8) -> Option<Self> {
        Some(match v {
            0 => ContentType::Text,
            1 => ContentType::Binary,
            2 => ContentType::Json,
            3 => ContentType::StreamOpen,
            4 => ContentType::StreamChunk,
            5 => ContentType::StreamClose,
            _ => return None,
        })
    }

    /// True for the three types that carry stream traffic.
    ///
    /// A relay never asks: it only forwards envelopes. This exists for the
    /// data layer, which has to tell "one message" from "part of a longer one"
    /// without keeping per-stream state in the router.
    pub fn is_stream(self) -> bool {
        matches!(
            self,
            ContentType::StreamOpen | ContentType::StreamChunk | ContentType::StreamClose
        )
    }

    pub fn label(self) -> &'static str {
        match self {
            ContentType::Text => "text",
            ContentType::Binary => "binary",
            ContentType::Json => "json",
            ContentType::StreamOpen => "stream-open",
            ContentType::StreamChunk => "stream-chunk",
            ContentType::StreamClose => "stream-close",
        }
    }
}

/// Application-level message: a named channel plus opaque bytes.
///
/// The routing layer never looks inside `payload`; the transport layer never
/// looks at the routing header. That is the whole point of the split.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataBody {
    pub channel: String,
    pub content_type: ContentType,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HelloBody {
    pub name: String,
    /// The sender's own link id for this connection.
    ///
    /// Both ends see the same physical link under two different local ids.
    /// Exchanging them lets both ends compute an identical sort key for the
    /// link, which is what makes duplicate-link resolution symmetric.
    pub link_id: u64,
}

/// Distance-vector update, only ever sent one hop.
///
/// Each entry carries the node's name as well as its distance, so a name
/// propagates across the mesh with the route that reaches it. Without that a
/// two-hop peer would only ever be a hex string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteBody {
    pub entries: Vec<(NodeId, u8, String)>,
}

/// End-to-end receipt. Routed like data, but consumed by the router: it is
/// never handed to an application transport.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AckBody {
    /// Id of the envelope being acknowledged.
    pub acked: MessageId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Body {
    Data(DataBody),
    Hello(HelloBody),
    Route(RouteBody),
    Ack(AckBody),
    Bye,
}

impl Body {
    pub fn kind(&self) -> u8 {
        match self {
            Body::Data(_) => 0,
            Body::Hello(_) => 1,
            Body::Route(_) => 2,
            Body::Bye => 3,
            Body::Ack(_) => 4,
        }
    }

    /// Control messages are consumed by the router. Only `Hello` and `Route`
    /// are strictly link-local; `Ack` is routed end to end.
    pub fn is_control(&self) -> bool {
        !matches!(self, Body::Data(_))
    }

    /// Envelopes that get forwarded when they are not addressed to us.
    pub fn is_routable(&self) -> bool {
        matches!(self, Body::Data(_) | Body::Ack(_))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Envelope {
    pub id: MessageId,
    pub src: NodeId,
    pub dst: Destination,
    pub flags: u8,
    pub ttl: u8,
    pub hops: u8,
    pub created_at_ms: u64,
    pub body: Body,
}

impl Envelope {
    pub fn new(src: NodeId, dst: Destination, ttl: u8, body: Body) -> Self {
        Envelope {
            id: MessageId::random(),
            src,
            dst,
            flags: 0,
            ttl,
            hops: 0,
            created_at_ms: now_ms(),
            body,
        }
    }

    /// Marks this envelope as carrying an auth tag. The routing layer sets
    /// this when a key is configured; the sealer then appends the tag.
    pub fn authenticated(mut self) -> Self {
        self.flags |= FLAG_AUTH;
        self
    }

    /// Asks the destination to acknowledge receipt. Unicast only: acking a
    /// broadcast would turn one packet into N.
    pub fn request_ack(mut self) -> Self {
        self.flags |= FLAG_ACK_REQ;
        self
    }

    pub fn needs_auth(&self) -> bool {
        self.flags & FLAG_AUTH != 0
    }

    pub fn wants_ack(&self) -> bool {
        self.flags & FLAG_ACK_REQ != 0
    }

    pub fn data(
        src: NodeId,
        dst: Destination,
        ttl: u8,
        channel: impl Into<String>,
        content_type: ContentType,
        payload: impl Into<Vec<u8>>,
    ) -> Self {
        Envelope::new(
            src,
            dst,
            ttl,
            Body::Data(DataBody {
                channel: channel.into(),
                content_type,
                payload: payload.into(),
            }),
        )
    }

    /// Link-local handshake: "this is who I am, on this link". Never
    /// forwarded (ttl 1).
    pub fn hello(src: NodeId, name: impl Into<String>, link_id: u64) -> Self {
        Envelope::new(
            src,
            Destination::Broadcast,
            1,
            Body::Hello(HelloBody {
                name: name.into(),
                link_id,
            }),
        )
    }

    /// Link-local distance-vector advertisement. Never forwarded (ttl 1).
    pub fn route_update(src: NodeId, entries: Vec<(NodeId, u8, String)>) -> Self {
        Envelope::new(
            src,
            Destination::Broadcast,
            1,
            Body::Route(RouteBody { entries }),
        )
    }

    pub fn bye(src: NodeId) -> Self {
        Envelope::new(src, Destination::Broadcast, 1, Body::Bye)
    }

    /// Receipt for `acked`, addressed back to the original sender.
    pub fn ack(src: NodeId, dst: NodeId, acked: MessageId, ttl: u8) -> Self {
        Envelope::new(
            src,
            Destination::Node(dst),
            ttl,
            Body::Ack(AckBody { acked }),
        )
    }

    pub fn is_control(&self) -> bool {
        self.body.is_control()
    }

    pub fn channel(&self) -> Option<&str> {
        match &self.body {
            Body::Data(d) => Some(&d.channel),
            _ => None,
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>, CodecError> {
        let mut buf = Vec::with_capacity(HEADER_LEN + 64);
        buf.extend_from_slice(&MAGIC);
        buf.push(VERSION);
        buf.push(self.flags);
        buf.push(self.ttl);
        buf.push(self.hops);
        buf.push(self.dst.as_u8());
        buf.push(self.body.kind());
        buf.extend_from_slice(self.id.as_bytes());
        buf.extend_from_slice(self.src.as_bytes());
        match &self.dst {
            Destination::Broadcast => buf.extend_from_slice(&[0u8; 16]),
            Destination::Node(n) => buf.extend_from_slice(n.as_bytes()),
        }
        buf.extend_from_slice(&self.created_at_ms.to_be_bytes());
        debug_assert_eq!(buf.len(), HEADER_LEN);

        match &self.body {
            Body::Data(d) => {
                if d.channel.len() > u8::MAX as usize {
                    return Err(CodecError::FieldTooLong("channel"));
                }
                if d.payload.len() > MAX_PAYLOAD {
                    return Err(CodecError::TooLarge(d.payload.len()));
                }
                buf.push(d.channel.len() as u8);
                buf.extend_from_slice(d.channel.as_bytes());
                buf.push(d.content_type.as_u8());
                buf.extend_from_slice(&(d.payload.len() as u32).to_be_bytes());
                buf.extend_from_slice(&d.payload);
            }
            Body::Hello(h) => {
                let name = truncate_utf8(&h.name, MAX_NAME);
                buf.push(name.len() as u8);
                buf.extend_from_slice(name.as_bytes());
                buf.extend_from_slice(&h.link_id.to_be_bytes());
            }
            Body::Route(r) => {
                if r.entries.len() > u16::MAX as usize {
                    return Err(CodecError::FieldTooLong("route entries"));
                }
                buf.extend_from_slice(&(r.entries.len() as u16).to_be_bytes());
                for (node, metric, name) in &r.entries {
                    buf.extend_from_slice(node.as_bytes());
                    buf.push(*metric);
                    let name = truncate_utf8(name, MAX_NAME);
                    buf.push(name.len() as u8);
                    buf.extend_from_slice(name.as_bytes());
                }
            }
            Body::Bye => {}
            Body::Ack(a) => buf.extend_from_slice(a.acked.as_bytes()),
        }
        Ok(buf)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        if bytes.len() < HEADER_LEN {
            return Err(CodecError::TooShort(bytes.len()));
        }
        if bytes[0..2] != MAGIC {
            return Err(CodecError::BadMagic([bytes[0], bytes[1]]));
        }
        if bytes[2] != VERSION {
            return Err(CodecError::BadVersion(bytes[2]));
        }
        let flags = bytes[3];
        let ttl = bytes[4];
        let hops = bytes[5];
        let dst_kind = bytes[6];
        let body_kind = bytes[7];

        let id = MessageId::from_slice(&bytes[8..24]).expect("slice is 16 bytes");
        let src = NodeId::from_slice(&bytes[24..40]).expect("slice is 16 bytes");
        let dst = match dst_kind {
            0 => Destination::Broadcast,
            1 => Destination::Node(NodeId::from_slice(&bytes[40..56]).expect("slice is 16 bytes")),
            k => return Err(CodecError::BadKind(k)),
        };
        let mut created = [0u8; 8];
        created.copy_from_slice(&bytes[56..64]);
        let created_at_ms = u64::from_be_bytes(created);

        let mut cur = Cursor {
            buf: bytes,
            pos: HEADER_LEN,
        };

        let body = match body_kind {
            0 => {
                let clen = cur.u8()? as usize;
                let channel = cur.str(clen)?;
                let ct = cur.u8()?;
                let content_type =
                    ContentType::from_u8(ct).ok_or(CodecError::UnknownContentType(ct))?;
                let plen = cur.u32()? as usize;
                if plen > MAX_PAYLOAD {
                    return Err(CodecError::TooLarge(plen));
                }
                let payload = cur.bytes(plen)?.to_vec();
                Body::Data(DataBody {
                    channel,
                    content_type,
                    payload,
                })
            }
            1 => {
                let nlen = cur.u8()? as usize;
                let name = cur.str(nlen)?;
                Body::Hello(HelloBody {
                    name,
                    link_id: cur.u64()?,
                })
            }
            2 => {
                let count = cur.u16()? as usize;
                let mut entries = Vec::with_capacity(count.min(1024));
                for _ in 0..count {
                    let node = NodeId::from_slice(cur.bytes(16)?).expect("slice is 16 bytes");
                    let metric = cur.u8()?;
                    let nlen = cur.u8()? as usize;
                    let name = cur.str(nlen)?;
                    entries.push((node, metric, name));
                }
                Body::Route(RouteBody { entries })
            }
            3 => Body::Bye,
            4 => {
                let acked = MessageId::from_slice(cur.bytes(16)?).expect("slice is 16 bytes");
                Body::Ack(AckBody { acked })
            }
            k => return Err(CodecError::BadKind(k)),
        };

        Ok(Envelope {
            id,
            src,
            dst,
            flags,
            ttl,
            hops,
            created_at_ms,
            body,
        })
    }
}

struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn bytes(&mut self, n: usize) -> Result<&'a [u8], CodecError> {
        if self.pos + n > self.buf.len() {
            return Err(CodecError::Truncated);
        }
        let out = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(out)
    }

    fn u8(&mut self) -> Result<u8, CodecError> {
        Ok(self.bytes(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, CodecError> {
        let b = self.bytes(2)?;
        Ok(u16::from_be_bytes([b[0], b[1]]))
    }

    fn u64(&mut self) -> Result<u64, CodecError> {
        let b = self.bytes(8)?;
        let mut out = [0u8; 8];
        out.copy_from_slice(b);
        Ok(u64::from_be_bytes(out))
    }

    fn u32(&mut self) -> Result<u32, CodecError> {
        let b = self.bytes(4)?;
        Ok(u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn str(&mut self, n: usize) -> Result<String, CodecError> {
        let b = self.bytes(n)?;
        String::from_utf8(b.to_vec()).map_err(|_| CodecError::InvalidUtf8)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_data() -> Envelope {
        Envelope::data(
            NodeId::random(),
            Destination::Node(NodeId::random()),
            7,
            "chat",
            ContentType::Text,
            b"hello mesh".to_vec(),
        )
    }

    #[test]
    fn data_roundtrip() {
        let env = sample_data();
        let bytes = env.encode().unwrap();
        assert_eq!(bytes.len(), HEADER_LEN + 1 + 4 + 1 + 4 + 10);
        let back = Envelope::decode(&bytes).unwrap();
        assert_eq!(back, env);
    }

    #[test]
    fn broadcast_roundtrip() {
        let env = Envelope::data(
            NodeId::random(),
            Destination::Broadcast,
            4,
            "chat",
            ContentType::Text,
            b"hi".to_vec(),
        );
        assert_eq!(Envelope::decode(&env.encode().unwrap()).unwrap(), env);
    }

    #[test]
    fn route_roundtrip() {
        let entries = vec![
            (NodeId::random(), 0u8, "alice".to_string()),
            (NodeId::random(), 3u8, String::new()),
        ];
        let env = Envelope::route_update(NodeId::random(), entries.clone());
        let back = Envelope::decode(&env.encode().unwrap()).unwrap();
        assert_eq!(back, env);
        if let Body::Route(r) = back.body {
            assert_eq!(r.entries, entries);
        } else {
            panic!("wrong body kind");
        }
    }

    #[test]
    fn long_names_are_truncated_on_a_char_boundary() {
        let long = "节点".repeat(40); // 240 bytes of multi-byte text
        let env = Envelope::route_update(
            NodeId::random(),
            vec![(NodeId::random(), 1u8, long.clone())],
        );
        let back = Envelope::decode(&env.encode().unwrap()).unwrap();
        match back.body {
            Body::Route(r) => {
                let name = &r.entries[0].2;
                assert!(name.len() <= MAX_NAME);
                assert!(name.is_char_boundary(name.len()));
                assert!(long.starts_with(name.as_str()));
            }
            _ => panic!("wrong body kind"),
        }
    }

    #[test]
    fn binary_payload_roundtrip() {
        let payload: Vec<u8> = (0..=255u8).collect();
        let env = Envelope::data(
            NodeId::random(),
            Destination::Broadcast,
            2,
            "blob",
            ContentType::Binary,
            payload.clone(),
        );
        let back = Envelope::decode(&env.encode().unwrap()).unwrap();
        match back.body {
            Body::Data(d) => {
                assert_eq!(d.payload, payload);
                assert_eq!(d.content_type, ContentType::Binary);
            }
            _ => panic!("wrong body kind"),
        }
    }

    #[test]
    fn rejects_garbage() {
        assert!(Envelope::decode(b"nope").is_err());
        let mut bytes = sample_data().encode().unwrap();
        bytes[0] = b'X';
        assert_eq!(Envelope::decode(&bytes), Err(CodecError::BadMagic(*b"XX")));
    }

    #[test]
    fn rejects_truncated() {
        let bytes = sample_data().encode().unwrap();
        assert!(Envelope::decode(&bytes[..bytes.len() - 2]).is_err());
    }

    #[test]
    fn control_messages_are_marked() {
        assert!(Envelope::hello(NodeId::random(), "x", 7).is_control());
        assert!(Envelope::bye(NodeId::random()).is_control());
        assert!(!sample_data().is_control());
    }

    #[test]
    fn ack_roundtrip() {
        let acked = MessageId::random();
        let env = Envelope::ack(NodeId::random(), NodeId::random(), acked, 8);
        let back = Envelope::decode(&env.encode().unwrap()).unwrap();
        assert_eq!(back, env);
        match &back.body {
            Body::Ack(a) => assert_eq!(a.acked, acked),
            _ => panic!("wrong body kind"),
        }
        assert!(back.body.is_control());
        assert!(back.body.is_routable(), "acks are routed end to end");
    }

    #[test]
    fn flags_survive_encoding() {
        let env = Envelope::data(
            NodeId::random(),
            Destination::Node(NodeId::random()),
            5,
            "chat",
            ContentType::Text,
            b"x".to_vec(),
        )
        .authenticated()
        .request_ack();
        assert!(env.needs_auth() && env.wants_ack());
        let back = Envelope::decode(&env.encode().unwrap()).unwrap();
        assert!(back.needs_auth());
        assert!(back.wants_ack());
        assert_eq!(back.flags, FLAG_AUTH | FLAG_ACK_REQ);
    }

    #[test]
    fn hello_and_route_are_link_local() {
        assert!(!Envelope::hello(NodeId::random(), "x", 7).body.is_routable());
        assert!(!Envelope::route_update(NodeId::random(), vec![])
            .body
            .is_routable());
    }
}
