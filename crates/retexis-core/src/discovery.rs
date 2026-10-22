//! Peer discovery — "who else can I reach on this medium?"
//!
//! A mesh is useless if you have to type an address. Every medium gets a
//! chance to answer one question: *can you reach several peers at once, and
//! if so, who is out there?* That is a property of the medium, not of routing,
//! so it belongs next to [`Link`](crate::Link), not inside the router.
//!
//! The contract is deliberately tiny:
//!
//! * [`Beacon::broadcast`] — fire one announcement at everyone reachable at
//!   once. Returns how many destinations were written.
//! * [`Beacon::discover`] — ask "who is out there?" and collect replies.
//!
//! A medium that physically cannot do this (a point-to-point pipe, a radio
//! with no shared channel, a simulated switch) returns
//! [`NoBroadcast`], whose two methods return `0` and an empty list. That is a
//! normal answer, not a failure: the caller can ask every medium the same way
//! and only act on the ones that found something.
//!
//! `retexis-discovery` implements the IP version (UDP broadcast); `retexis-link-tcp`
//! and `retexis-link-udp` hand it out, because a TCP endpoint can still be
//! *advertised* over a UDP broadcast — the announcement says which scheme and
//! address to connect to.

use crate::envelope::CodecError;
use crate::id::NodeId;
use crate::link::LinkError;
use std::time::Duration;

/// Magic prefix of a discovery probe. Versioned so a future format can be
/// distinguished on the wire instead of guessed at.
pub const PROBE_MAGIC: [u8; 4] = *b"RXD1";

/// Default UDP port for discovery. Overridable per node so two nodes on one
/// host can be tested without fighting over the port.
pub const DEFAULT_PORT: u16 = 47700;

/// Hard cap on a probe. Discovery traffic must never be a way to waste memory.
pub const MAX_PROBE: usize = 1024;

const MAX_NAME: usize = 48;
const MAX_SCHEME: usize = 12;
const MAX_ADDR: usize = 96;
const MAX_ENDPOINTS: usize = 8;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProbeKind {
    /// "Who is out there?" — no endpoints, the sender wants replies.
    Query,
    /// "Here is who I am" — endpoints to connect to.
    Reply,
}

impl ProbeKind {
    fn as_byte(self) -> u8 {
        match self {
            ProbeKind::Query => 0,
            ProbeKind::Reply => 1,
        }
    }

    fn from_byte(b: u8) -> Result<Self, CodecError> {
        match b {
            0 => Ok(ProbeKind::Query),
            1 => Ok(ProbeKind::Reply),
            k => Err(CodecError::BadKind(k)),
        }
    }
}

/// One way to reach a node: `udp://10.0.0.7:7101`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Endpoint {
    pub scheme: String,
    pub addr: String,
}

impl Endpoint {
    pub fn new(scheme: impl Into<String>, addr: impl Into<String>) -> Self {
        Endpoint {
            scheme: scheme.into(),
            addr: addr.into(),
        }
    }

    pub fn url(&self) -> String {
        format!("{}://{}", self.scheme, self.addr)
    }

    /// Splits `tcp://1.2.3.4:7100` into an endpoint.
    pub fn parse_url(url: &str) -> Option<Endpoint> {
        let (scheme, addr) = url.split_once("://")?;
        if scheme.is_empty() || addr.is_empty() {
            return None;
        }
        Some(Endpoint::new(scheme, addr))
    }
}

/// What goes on the wire during discovery.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Announcement {
    pub kind: ProbeKind,
    /// Identity, when the sender has one to give. `None` in a bare query.
    pub node: Option<NodeId>,
    pub name: String,
    pub endpoints: Vec<Endpoint>,
}

impl Announcement {
    /// A bare "who is out there?".
    pub fn query() -> Self {
        Announcement {
            kind: ProbeKind::Query,
            node: None,
            name: String::new(),
            endpoints: Vec::new(),
        }
    }

    /// "This is me, here is how to reach me."
    pub fn reply(node: NodeId, name: impl Into<String>, endpoints: Vec<Endpoint>) -> Self {
        Announcement {
            kind: ProbeKind::Reply,
            node: Some(node),
            name: name.into(),
            endpoints,
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>, CodecError> {
        let name = truncate(&self.name, MAX_NAME);
        if self.endpoints.len() > MAX_ENDPOINTS {
            return Err(CodecError::FieldTooLong("endpoints"));
        }
        let mut buf = Vec::with_capacity(64);
        buf.extend_from_slice(&PROBE_MAGIC);
        buf.push(self.kind.as_byte());
        buf.extend_from_slice(self.node.unwrap_or_default().as_bytes());
        buf.push(name.len() as u8);
        buf.extend_from_slice(name.as_bytes());
        buf.push(self.endpoints.len() as u8);
        for ep in &self.endpoints {
            if ep.scheme.len() > MAX_SCHEME || ep.addr.len() > MAX_ADDR {
                return Err(CodecError::FieldTooLong("endpoint"));
            }
            buf.push(ep.scheme.len() as u8);
            buf.extend_from_slice(ep.scheme.as_bytes());
            buf.push(ep.addr.len() as u8);
            buf.extend_from_slice(ep.addr.as_bytes());
        }
        if buf.len() > MAX_PROBE {
            return Err(CodecError::TooLarge(buf.len()));
        }
        Ok(buf)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        if bytes.len() > MAX_PROBE {
            return Err(CodecError::TooLarge(bytes.len()));
        }
        if bytes.len() < PROBE_MAGIC.len() + 1 {
            return Err(CodecError::TooShort(bytes.len()));
        }
        if bytes[..4] != PROBE_MAGIC {
            return Err(CodecError::BadMagic([bytes[0], bytes[1]]));
        }
        let mut cur = Cursor {
            bytes,
            at: PROBE_MAGIC.len(),
        };
        let kind = ProbeKind::from_byte(cur.u8()?)?;
        let node = NodeId::from_slice(cur.take(16)?).expect("16 bytes");
        let name_len = cur.u8()? as usize;
        let name = cur.text(name_len)?.to_string();
        let count = cur.u8()? as usize;
        if count > MAX_ENDPOINTS {
            return Err(CodecError::FieldTooLong("endpoints"));
        }
        let mut endpoints = Vec::with_capacity(count);
        for _ in 0..count {
            let scheme_len = cur.u8()? as usize;
            let scheme = cur.text(scheme_len)?.to_string();
            let addr_len = cur.u8()? as usize;
            let addr = cur.text(addr_len)?.to_string();
            endpoints.push(Endpoint { scheme, addr });
        }
        if cur.at != bytes.len() {
            return Err(CodecError::Truncated);
        }
        Ok(Announcement {
            kind,
            node: if node.is_zero() { None } else { Some(node) },
            name,
            endpoints,
        })
    }
}

/// What a medium can do about finding peers.
pub trait Beacon: Send + Sync {
    /// Scheme this beacon advertises, e.g. `"udp"`.
    fn scheme(&self) -> &'static str;

    /// Whether this medium can reach more than one peer at a time.
    fn can_broadcast(&self) -> bool {
        true
    }

    /// Send `announcement` to everyone reachable at once.
    ///
    /// Returns the number of destinations written. `0` means "reached nobody",
    /// which for a point-to-point medium is the only possible answer and is
    /// not an error.
    fn broadcast(&self, announcement: &Announcement) -> Result<usize, LinkError>;

    /// Ask "who is out there?" and wait up to `timeout` for replies.
    ///
    /// Returns an empty list when the medium cannot broadcast, or when nobody
    /// answered. Callers cannot tell those apart, and should not need to.
    fn discover(&self, timeout: Duration) -> Result<Vec<Announcement>, LinkError>;
}

/// A medium that cannot broadcast: point-to-point pipes, simulated switches,
/// anything where "everyone at once" has no meaning.
///
/// It exists so every transport can be asked the same question without the
/// caller special-casing which ones support discovery.
pub struct NoBroadcast {
    scheme: &'static str,
}

impl NoBroadcast {
    pub fn new(scheme: &'static str) -> Self {
        NoBroadcast { scheme }
    }
}

impl Beacon for NoBroadcast {
    fn scheme(&self) -> &'static str {
        self.scheme
    }

    fn can_broadcast(&self) -> bool {
        false
    }

    fn broadcast(&self, _announcement: &Announcement) -> Result<usize, LinkError> {
        Ok(0)
    }

    fn discover(&self, _timeout: Duration) -> Result<Vec<Announcement>, LinkError> {
        Ok(Vec::new())
    }
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

struct Cursor<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], CodecError> {
        let end = self.at.checked_add(n).ok_or(CodecError::Truncated)?;
        if end > self.bytes.len() {
            return Err(CodecError::Truncated);
        }
        let out = &self.bytes[self.at..end];
        self.at = end;
        Ok(out)
    }

    fn u8(&mut self) -> Result<u8, CodecError> {
        Ok(self.take(1)?[0])
    }

    fn text(&mut self, n: usize) -> Result<&'a str, CodecError> {
        let bytes = self.take(n)?;
        std::str::from_utf8(bytes).map_err(|_| CodecError::InvalidUtf8)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reply_roundtrips() {
        let node = NodeId::random();
        let a = Announcement::reply(
            node,
            "alice",
            vec![
                Endpoint::new("tcp", "10.0.0.7:7100"),
                Endpoint::new("udp", "10.0.0.7:7101"),
            ],
        );
        let back = Announcement::decode(&a.encode().unwrap()).unwrap();
        assert_eq!(back, a);
        assert_eq!(back.node, Some(node));
        assert_eq!(back.endpoints[0].url(), "tcp://10.0.0.7:7100");
    }

    #[test]
    fn query_has_no_identity() {
        let q = Announcement::query();
        let back = Announcement::decode(&q.encode().unwrap()).unwrap();
        assert_eq!(back.kind, ProbeKind::Query);
        assert_eq!(back.node, None);
        assert!(back.endpoints.is_empty());
    }

    #[test]
    fn a_long_name_is_truncated_on_a_char_boundary() {
        let long = "节点".repeat(40);
        let a = Announcement::reply(NodeId::random(), long, vec![]);
        let back = Announcement::decode(&a.encode().unwrap()).unwrap();
        assert!(back.name.len() <= MAX_NAME);
        // Still valid UTF-8, which is the point of truncating on a boundary.
        assert!(!back.name.is_empty());
    }

    #[test]
    fn garbage_is_rejected() {
        assert!(Announcement::decode(b"nope").is_err());
        assert!(
            Announcement::decode(b"RXD1").is_err(),
            "magic alone is too short"
        );
        let mut bytes = Announcement::query().encode().unwrap();
        bytes.push(0);
        assert!(Announcement::decode(&bytes).is_err(), "trailing bytes");
        let mut bytes = Announcement::query().encode().unwrap();
        bytes[4] = 9;
        assert!(Announcement::decode(&bytes).is_err(), "bad kind");
    }

    #[test]
    fn no_broadcast_answers_empty() {
        let b = NoBroadcast::new("hub");
        assert!(!b.can_broadcast());
        assert_eq!(b.broadcast(&Announcement::query()).unwrap(), 0);
        assert!(b.discover(Duration::from_millis(1)).unwrap().is_empty());
        assert_eq!(b.scheme(), "hub");
    }

    #[test]
    fn urls_split_into_endpoints() {
        assert_eq!(
            Endpoint::parse_url("udp://1.2.3.4:9"),
            Some(Endpoint::new("udp", "1.2.3.4:9"))
        );
        assert_eq!(Endpoint::parse_url("nonsense"), None);
    }
}
