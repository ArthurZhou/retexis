//! IP broadcast discovery.
//!
//! One implementation, used by every transport that has an IP address to
//! advertise. The discovery *channel* is always UDP broadcast; what differs is
//! the payload — a `tcp://` endpoint or a `udp://` endpoint. That is why TCP
//! can offer discovery at all: TCP cannot broadcast, but nothing stops a node
//! from shouting "my TCP port is 7100" over a UDP datagram.
//!
//! ```text
//!   node A                                          node B (--announce)
//!     │  UDP 255.255.255.255:47700  "who's out there?"  │
//!     ├───────────────────────────────────────────────▶│  responder
//!     │◀───────────────────────────────────────────────┤  "tcp://10.0.0.7:7100"
//!     │  unicast reply to A's ephemeral port            │
//! ```
//!
//! Design notes that matter:
//!
//! * The **querier binds an ephemeral port** and the responder replies to the
//!   source address. Only the responder needs the well-known port, so any
//!   number of nodes can scan from one host.
//! * A query is also sent to `127.0.0.1` on the same port. Real broadcast
//!   packets are not reliably looped back on every platform, and a
//!   single-machine demo that silently finds nothing is a bad first
//!   impression.
//! * The reply is the same size as the query, so this cannot be used as an
//!   amplifier.

use retexis_core::{Announcement, Beacon, LinkError, ProbeKind, DEFAULT_PORT, MAX_PROBE};
use std::io;
use std::net::{SocketAddr, UdpSocket};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

/// How long a single `recv_from` may block before the loop re-checks its
/// deadline. Small enough to keep scans snappy, large enough not to spin.
const SLICE: Duration = Duration::from_millis(50);

/// A beacon that speaks IP broadcast.
///
/// It holds no socket: every `broadcast`/`discover` call uses a fresh
/// ephemeral socket. That is what lets any number of these exist in one
/// process (one per medium) without fighting over a port.
pub struct BroadcastBeacon {
    scheme: &'static str,
    port: u16,
}

impl BroadcastBeacon {
    pub fn new(scheme: &'static str, port: u16) -> Self {
        BroadcastBeacon { scheme, port }
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    /// Where a probe goes: the limited broadcast address, plus loopback so a
    /// single host can discover itself.
    fn destinations(&self) -> Vec<SocketAddr> {
        let mut out = Vec::with_capacity(2);
        if let Ok(a) = format!("255.255.255.255:{}", self.port).parse() {
            out.push(a);
        }
        if let Ok(a) = format!("127.0.0.1:{}", self.port).parse() {
            out.push(a);
        }
        out
    }

    fn socket(&self) -> Result<UdpSocket, LinkError> {
        let sock = UdpSocket::bind("0.0.0.0:0").map_err(LinkError::io)?;
        sock.set_broadcast(true).map_err(LinkError::io)?;
        Ok(sock)
    }
}

impl Beacon for BroadcastBeacon {
    fn scheme(&self) -> &'static str {
        self.scheme
    }

    fn broadcast(&self, announcement: &Announcement) -> Result<usize, LinkError> {
        let bytes = announcement
            .encode()
            .map_err(|e| LinkError::Framing(e.to_string()))?;
        let sock = self.socket()?;
        let mut written = 0;
        for dest in self.destinations() {
            // A destination we cannot reach is not an error: it just means
            // nobody answered from that direction.
            if sock.send_to(&bytes, dest).is_ok() {
                written += 1;
            }
        }
        Ok(written)
    }

    fn discover(&self, timeout: Duration) -> Result<Vec<Announcement>, LinkError> {
        let query = Announcement::query()
            .encode()
            .map_err(|e| LinkError::Framing(e.to_string()))?;
        let sock = self.socket()?;
        for dest in self.destinations() {
            let _ = sock.send_to(&query, dest);
        }

        let deadline = Instant::now() + timeout;
        let mut buf = [0u8; MAX_PROBE];
        let mut found: Vec<Announcement> = Vec::new();
        while Instant::now() < deadline {
            let left = deadline.saturating_duration_since(Instant::now());
            sock.set_read_timeout(Some(left.min(SLICE)))
                .map_err(LinkError::io)?;
            match sock.recv_from(&mut buf) {
                Ok((n, _from)) => {
                    if let Ok(a) = Announcement::decode(&buf[..n]) {
                        if a.kind == ProbeKind::Reply && !found.iter().any(|seen| same(seen, &a)) {
                            found.push(a);
                        }
                    }
                    // Malformed or irrelevant datagrams are ignored: the port
                    // is on the open internet, anyone can send anything.
                }
                Err(e) if transient(&e) => {}
                Err(e) => return Err(LinkError::io(e)),
            }
        }
        Ok(found)
    }
}

/// Errors that mean "nothing to read yet", not "the socket is broken".
///
/// Windows is the reason this function exists: a UDP socket that sent to a
/// port nobody listens on gets an ICMP port-unreachable queued, and the *next*
/// `recv_from` fails with `WSAECONNRESET` (10054). Treating that as fatal
/// would make discovery fail exactly when it found nothing — the common case.
fn transient(e: &io::Error) -> bool {
    matches!(
        e.kind(),
        io::ErrorKind::WouldBlock
            | io::ErrorKind::TimedOut
            | io::ErrorKind::Interrupted
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionRefused
    )
}

fn same(a: &Announcement, b: &Announcement) -> bool {
    a.node == b.node && a.endpoints == b.endpoints
}

/// A running "answer discovery queries" loop.
pub struct Responder {
    stop: Arc<AtomicBool>,
    addr: SocketAddr,
    join: Option<JoinHandle<()>>,
}

impl Responder {
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }
}

impl Drop for Responder {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

/// Binds `port` on every interface and answers queries with `announcement`.
///
/// Pass port 0 to let the OS choose (the bound port comes back from
/// [`Responder::addr`]), which is how tests avoid fighting over 47700.
pub fn respond(announcement: Announcement, port: u16) -> io::Result<Responder> {
    let socket = UdpSocket::bind(format!("0.0.0.0:{port}"))?;
    socket.set_read_timeout(Some(SLICE))?;
    let addr = socket.local_addr()?;
    let reply = Arc::new(
        Announcement {
            kind: ProbeKind::Reply,
            ..announcement
        }
        .encode()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?,
    );

    let stop = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&stop);
    let join = std::thread::Builder::new()
        .name("retexis-discovery".to_string())
        .spawn(move || {
            let mut buf = [0u8; MAX_PROBE];
            while !flag.load(Ordering::Acquire) {
                match socket.recv_from(&mut buf) {
                    Ok((n, from)) => {
                        // Only queries deserve an answer. Replying to replies
                        // would let two nodes ping-pong forever.
                        if let Ok(probe) = Announcement::decode(&buf[..n]) {
                            if probe.kind == ProbeKind::Query {
                                let _ = socket.send_to(&reply, from);
                            }
                        }
                    }
                    Err(e) if transient(&e) => continue,
                    Err(_) => continue, // socket gone; the loop exits on stop
                }
            }
        })?;

    Ok(Responder {
        stop,
        addr,
        join: Some(join),
    })
}

/// Convenience: build the reply payload from a node's listen endpoints.
pub fn announcement_for(
    node: retexis_core::NodeId,
    name: &str,
    endpoints: Vec<retexis_core::Endpoint>,
) -> Announcement {
    Announcement::reply(node, name, endpoints)
}

/// Default discovery port, re-exported so callers do not need `core` too.
pub const PORT: u16 = DEFAULT_PORT;

#[cfg(test)]
mod tests {
    use super::*;
    use retexis_core::{Endpoint, NodeId};

    fn beacon(port: u16) -> BroadcastBeacon {
        BroadcastBeacon::new("udp", port)
    }

    #[test]
    fn a_responder_answers_a_query_on_loopback() {
        let node = NodeId::random();
        let reply = announcement_for(node, "alice", vec![Endpoint::new("udp", "10.0.0.7:7101")]);
        let responder = respond(reply.clone(), 0).expect("bind");
        let port = responder.addr().port();

        let found = beacon(port)
            .discover(Duration::from_millis(300))
            .expect("discover");
        assert_eq!(found.len(), 1, "one node should answer: {found:?}");
        assert_eq!(found[0].node, Some(node));
        assert_eq!(found[0].name, "alice");
        assert_eq!(found[0].endpoints, reply.endpoints);
    }

    #[test]
    fn discovering_nobody_is_an_empty_list_not_an_error() {
        // Bind then drop, so the port is (almost certainly) free and silent.
        let port = {
            let r = respond(Announcement::query(), 0).expect("bind");
            r.addr().port()
        };
        let found = beacon(port).discover(Duration::from_millis(120)).unwrap();
        assert!(found.is_empty());
    }

    #[test]
    fn a_query_alone_is_not_an_answer() {
        // A responder that only sees queries must not answer itself into a loop.
        let node = NodeId::random();
        let responder = respond(
            announcement_for(node, "quiet", vec![Endpoint::new("udp", "127.0.0.1:1")]),
            0,
        )
        .expect("bind");
        let port = responder.addr().port();
        // A raw query datagram, sent by hand.
        let sock = UdpSocket::bind("127.0.0.1:0").unwrap();
        sock.set_read_timeout(Some(Duration::from_millis(200)))
            .unwrap();
        sock.send_to(
            &Announcement::query().encode().unwrap(),
            format!("127.0.0.1:{port}"),
        )
        .unwrap();
        let mut buf = [0u8; MAX_PROBE];
        let (n, _) = sock.recv_from(&mut buf).expect("a reply should come back");
        let reply = Announcement::decode(&buf[..n]).unwrap();
        assert_eq!(reply.kind, ProbeKind::Reply);
        assert_eq!(reply.node, Some(node));
    }

    #[test]
    fn broadcast_reports_where_it_wrote() {
        let b = beacon(47700);
        // Loopback almost always works; the limited broadcast may not be
        // routable in a sandbox, so assert only the lower bound.
        let n = b.broadcast(&Announcement::query()).unwrap();
        assert!(n >= 1, "at least the loopback destination");
        assert_eq!(b.scheme(), "udp");
        assert!(b.can_broadcast());
    }
}
