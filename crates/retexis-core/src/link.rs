//! The hardware abstraction boundary.
//!
//! Everything above this module speaks [`Envelope`](crate::Envelope) bytes.
//! Everything below it speaks whatever the medium speaks. A [`Link`] is one
//! *point-to-point, byte-framed, ordered-ish* pipe: a TCP connection, a
//! WebSocket, an HTTP long-poll session, a LoRa channel between two radios.
//!
//! A `Link` is responsible for **framing**: the routing layer hands it a
//! bare encoded envelope and the link decides how to put it on the medium
//! (length prefix for streams, packet-per-frame for datagrams, chunking for
//! a 200-byte radio MTU). It is also responsible for de-framing on `recv`.
//!
//! Adding a medium therefore never touches the router: implement [`Link`] +
//! [`LinkAcceptor`] + [`LinkFactory`] and register the scheme.
//!
//! A medium may also be able to find peers by itself; that is the optional
//! [`LinkFactory::beacon`] capability. It defaults to
//! [`NoBroadcast`](crate::NoBroadcast), so a point-to-point medium compiles
//! and answers "nobody found" instead of forcing every implementation to care.

use crate::discovery::{Beacon, NoBroadcast};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

pub type LinkId = u64;

static NEXT_LINK_ID: AtomicU64 = AtomicU64::new(1);

pub fn next_link_id() -> LinkId {
    NEXT_LINK_ID.fetch_add(1, Ordering::Relaxed)
}

#[derive(Debug)]
pub enum LinkError {
    /// The underlying medium failed (reset, timeout, radio out of range...).
    Io(String),
    /// The peer or we closed the link.
    Closed,
    /// The encoded envelope does not fit this medium's frame size.
    FrameTooLarge { len: usize, max: usize },
    /// The bytes on the medium were not a valid frame.
    Framing(String),
}

impl LinkError {
    pub fn io(e: impl std::fmt::Display) -> Self {
        LinkError::Io(e.to_string())
    }

    pub fn is_closed(&self) -> bool {
        matches!(self, LinkError::Closed)
    }
}

impl fmt::Display for LinkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LinkError::Io(m) => write!(f, "io error: {m}"),
            LinkError::Closed => f.write_str("link closed"),
            LinkError::FrameTooLarge { len, max } => {
                write!(f, "frame of {len} bytes exceeds medium limit of {max}")
            }
            LinkError::Framing(m) => write!(f, "framing error: {m}"),
        }
    }
}

impl std::error::Error for LinkError {}

impl From<std::io::Error> for LinkError {
    fn from(e: std::io::Error) -> Self {
        match e.kind() {
            std::io::ErrorKind::UnexpectedEof
            | std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::ConnectionAborted
            | std::io::ErrorKind::BrokenPipe => LinkError::Closed,
            _ => LinkError::Io(e.to_string()),
        }
    }
}

/// One established point-to-point connection over some medium.
///
/// Implementations must be usable from several threads: `recv` is called from
/// a dedicated reader thread while `send` may be called concurrently from the
/// router thread.
pub trait Link: Send + Sync {
    fn id(&self) -> LinkId;

    /// Short name of the medium, e.g. `"tcp"`. Used in logs and in `/peers`.
    fn transport(&self) -> &'static str;

    /// Human readable peer address, e.g. `"127.0.0.1:7100"`.
    fn peer(&self) -> String;

    /// Did this node open the link (outbound dial) rather than accept it?
    ///
    /// Both ends must answer this consistently for the same physical link:
    /// the duplicate-link tie-break compares it with the node ids, so that two
    /// nodes that dialled each other at the same time agree on which of the
    /// two links to keep.
    fn initiator(&self) -> bool;

    /// Largest envelope (in bytes) this medium can carry in one frame.
    fn max_frame(&self) -> usize;

    /// Hand one already-encoded envelope to the medium. Implementations apply
    /// their own framing. Must be safe to call from multiple threads.
    fn send(&self, envelope: &[u8]) -> Result<(), LinkError>;

    /// Block until one complete envelope arrives, then return its raw bytes.
    /// Called from exactly one reader thread.
    fn recv(&self) -> Result<Vec<u8>, LinkError>;

    /// Best-effort teardown; must unblock a pending `recv`.
    fn shutdown(&self);
}

/// A listening endpoint that yields inbound [`Link`]s.
pub trait LinkAcceptor: Send + Sync {
    fn transport(&self) -> &'static str;

    /// Address actually bound, which is useful when the caller asked for
    /// port 0.
    fn local_addr(&self) -> String;

    /// Block until a peer connects.
    fn accept(&self) -> Result<Box<dyn Link>, LinkError>;

    fn shutdown(&self);
}

/// A medium. `listen` and `connect` are the only two ways a link is born.
pub trait LinkFactory: Send + Sync {
    /// URL scheme this factory handles, e.g. `"tcp"`.
    fn scheme(&self) -> &'static str;

    fn listen(&self, addr: &str) -> Result<Box<dyn LinkAcceptor>, LinkError>;

    fn connect(&self, addr: &str) -> Result<Box<dyn Link>, LinkError>;

    /// How peers on this medium can be found without being told an address.
    ///
    /// The default says "not possible": a point-to-point medium cannot ask
    /// "who is out there?". Media that can — IP broadcast, a radio channel
    /// with a shared frequency — override this and return a real
    /// [`Beacon`].
    fn beacon(&self) -> Box<dyn Beacon> {
        Box::new(NoBroadcast::new(self.scheme()))
    }
}

/// Scheme -> factory map, so `--listen tcp://0.0.0.0:7100` is resolved at
/// runtime. Adding `udp://` later is one `register` call at startup.
///
/// Factories are shared (`Arc`) rather than owned because more than one caller
/// needs one at a time: the listener path, and every supervised dialer, which
/// lives on its own thread and must own what it uses.
#[derive(Default)]
pub struct TransportRegistry {
    factories: Vec<Arc<dyn LinkFactory>>,
}

impl TransportRegistry {
    pub fn new() -> Self {
        Self {
            factories: Vec::new(),
        }
    }

    pub fn register(&mut self, factory: Arc<dyn LinkFactory>) {
        self.factories.push(factory);
    }

    pub fn schemes(&self) -> Vec<&'static str> {
        self.factories.iter().map(|f| f.scheme()).collect()
    }

    /// The factory for a URL's scheme.
    ///
    /// This is what keeps the dial path honest: `--connect udp://...` must
    /// resolve to the UDP factory, not to whichever medium happened to be
    /// hardcoded at the call site.
    pub fn factory_for(&self, url: &str) -> Option<Arc<dyn LinkFactory>> {
        let (scheme, _) = split_url(url).ok()?;
        self.factories
            .iter()
            .find(|f| f.scheme() == scheme)
            .map(Arc::clone)
    }

    fn factory(&self, scheme: &str) -> Result<Arc<dyn LinkFactory>, LinkError> {
        self.factories
            .iter()
            .find(|f| f.scheme() == scheme)
            .map(Arc::clone)
            .ok_or_else(|| {
                LinkError::Framing(format!(
                    "no transport registered for scheme '{scheme}' (have: {})",
                    self.schemes().join(", ")
                ))
            })
    }

    pub fn listen(&self, url: &str) -> Result<Box<dyn LinkAcceptor>, LinkError> {
        let (scheme, addr) = split_url(url)?;
        self.factory(scheme)?.listen(addr)
    }

    pub fn connect(&self, url: &str) -> Result<Box<dyn Link>, LinkError> {
        let (scheme, addr) = split_url(url)?;
        self.factory(scheme)?.connect(addr)
    }

    /// The discovery beacon of one scheme, or `None` when it is not registered.
    pub fn beacon_for(&self, scheme: &str) -> Option<Box<dyn Beacon>> {
        self.factories
            .iter()
            .find(|f| f.scheme() == scheme)
            .map(|f| f.beacon())
    }

    /// Every registered medium's beacon, in registration order.
    ///
    /// Media that cannot broadcast are included: their beacons return `0` and
    /// an empty list, so a caller can iterate without filtering.
    pub fn beacons(&self) -> Vec<Box<dyn Beacon>> {
        self.factories.iter().map(|f| f.beacon()).collect()
    }
}

/// Splits `tcp://host:port` into `("tcp", "host:port")`.
pub fn split_url(url: &str) -> Result<(&str, &str), LinkError> {
    match url.split_once("://") {
        Some((scheme, rest)) if !scheme.is_empty() && !rest.is_empty() => Ok((scheme, rest)),
        _ => Err(LinkError::Framing(format!(
            "expected a URL like tcp://host:port, got '{url}'"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_url_works() {
        assert_eq!(split_url("tcp://1.2.3.4:9").unwrap(), ("tcp", "1.2.3.4:9"));
        assert!(split_url("1.2.3.4:9").is_err());
        assert!(split_url("tcp://").is_err());
    }

    #[test]
    fn unknown_scheme_is_reported() {
        let reg = TransportRegistry::new();
        let err = match reg.listen("ws://x") {
            Ok(_) => panic!("an unregistered scheme must not resolve"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("no transport registered"));
        assert!(reg.factory_for("ws://x").is_none());
        assert!(reg.beacon_for("ws").is_none());
    }

    /// A stand-in medium: enough to prove the registry resolves by scheme and
    /// that the default beacon answers "cannot broadcast".
    struct Dummy(&'static str);

    impl LinkFactory for Dummy {
        fn scheme(&self) -> &'static str {
            self.0
        }
        fn listen(&self, _addr: &str) -> Result<Box<dyn LinkAcceptor>, LinkError> {
            Err(LinkError::Framing("dummy".into()))
        }
        fn connect(&self, _addr: &str) -> Result<Box<dyn Link>, LinkError> {
            Err(LinkError::Framing("dummy".into()))
        }
    }

    #[test]
    fn the_default_beacon_cannot_broadcast() {
        let mut reg = TransportRegistry::new();
        reg.register(Arc::new(Dummy("p2p")));
        let beacon = reg.beacon_for("p2p").expect("registered");
        assert_eq!(beacon.scheme(), "p2p");
        assert!(!beacon.can_broadcast());
        assert_eq!(beacon.broadcast(&crate::Announcement::query()).unwrap(), 0);
        assert!(beacon
            .discover(std::time::Duration::from_millis(1))
            .unwrap()
            .is_empty());
        assert_eq!(reg.beacons().len(), 1);
    }

    #[test]
    fn factory_for_resolves_by_scheme() {
        let mut reg = TransportRegistry::new();
        reg.register(Arc::new(Dummy("a")));
        reg.register(Arc::new(Dummy("b")));
        assert_eq!(reg.factory_for("a://x:1").unwrap().scheme(), "a");
        assert_eq!(reg.factory_for("b://x:1").unwrap().scheme(), "b");
        assert!(reg.factory_for("a//x").is_none(), "not a url");
        assert_eq!(reg.schemes(), vec!["a", "b"]);
    }
}
