//! UDP medium.
//!
//! Datagrams are already framed, so this link does **no framing at all**: one
//! envelope goes in one datagram, and one datagram comes back out as one
//! envelope. No length prefix, no reassembly, no half-packet state. That is
//! the whole reason the [`Link`] trait owns framing instead of the router.
//!
//! What UDP costs, and what the layers above have to absorb:
//!
//! * **loss** — a datagram can vanish. The retransmit layer already handles
//!   this; that is what it is for.
//! * **no connection** — "connected" here means a pseudo-connection: a peer
//!   address plus a demultiplexer. There is no handshake, so a dead peer looks
//!   exactly like a quiet one until the routing layer notices.
//! * **no order** — envelopes may arrive out of order. The router's dedup and
//!   the app layer's semantics do not assume order.
//!
//! Inbound: one bound socket, many peers. A demux thread reads datagrams and
//! routes each to the queue of the peer it came from; `accept` hands out one
//! [`UdpLink`] per peer. This is why the same socket can serve several
//! neighbours at once.

#![forbid(unsafe_code)]

use retexis_core::{
    next_link_id, Beacon, Link, LinkAcceptor, LinkError, LinkFactory, LinkId, DEFAULT_PORT,
};
use std::collections::HashMap;
use std::net::{SocketAddr, ToSocketAddrs, UdpSocket};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Largest UDP payload over IPv4. The envelope cap (60 KiB + header + tag)
/// fits comfortably, so a full-size envelope is one datagram.
pub const MAX_DATAGRAM: usize = 65_507;

/// Receive buffer. One byte larger than the largest legal payload, so a
/// datagram can never be silently truncated.
const READ_BUF: usize = 65_535;

/// How often a blocked read re-checks the shutdown flag. `UdpSocket` has no
/// `shutdown()`, so this is the only way to unblock a reader.
const POLL: Duration = Duration::from_millis(150);

/// Peers a listener will buffer before it starts dropping. A port scan must
/// not be able to make a node allocate without bound.
const MAX_PENDING: usize = 128;

pub struct UdpFactory {
    /// UDP port used for IP broadcast discovery.
    discovery_port: u16,
}

impl UdpFactory {
    pub fn new() -> Self {
        UdpFactory {
            discovery_port: DEFAULT_PORT,
        }
    }

    /// Overrides the discovery port, so two nodes on one host can be tested
    /// without fighting over 47700.
    pub fn with_discovery_port(mut self, port: u16) -> Self {
        self.discovery_port = port;
        self
    }
}

impl Default for UdpFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl LinkFactory for UdpFactory {
    fn scheme(&self) -> &'static str {
        "udp"
    }

    fn listen(&self, addr: &str) -> Result<Box<dyn LinkAcceptor>, LinkError> {
        Ok(Box::new(UdpAcceptor::bind(addr)?))
    }

    fn connect(&self, addr: &str) -> Result<Box<dyn Link>, LinkError> {
        Ok(Box::new(UdpLink::connect(addr)?))
    }

    /// UDP can broadcast, so discovery is real: a query goes out to the
    /// limited broadcast address and peers answer with their endpoints.
    fn beacon(&self) -> Box<dyn Beacon> {
        Box::new(retexis_discovery::BroadcastBeacon::new(
            "udp",
            self.discovery_port,
        ))
    }
}

// ------------------------------------------------------------------- inbound

struct Peer {
    tx: Sender<Vec<u8>>,
}

type Peers = Arc<Mutex<HashMap<SocketAddr, Peer>>>;

pub struct UdpAcceptor {
    socket: Arc<UdpSocket>,
    local: SocketAddr,
    peers: Peers,
    pending: Arc<Mutex<HashMap<SocketAddr, Receiver<Vec<u8>>>>>,
    /// Not `Sync` on its own, and `LinkAcceptor` requires `Sync`, so the
    /// receiver sits behind a mutex. `accept` is called from one thread.
    arrivals: Mutex<Receiver<SocketAddr>>,
    closed: Arc<AtomicBool>,
    dropped: Arc<AtomicU64>,
}

impl UdpAcceptor {
    fn bind(addr: &str) -> Result<Self, LinkError> {
        let socket = UdpSocket::bind(addr).map_err(LinkError::io)?;
        socket.set_read_timeout(Some(POLL)).map_err(LinkError::io)?;
        let local = socket.local_addr().map_err(LinkError::io)?;
        let socket = Arc::new(socket);
        let peers: Peers = Arc::new(Mutex::new(HashMap::new()));
        let pending: Arc<Mutex<HashMap<SocketAddr, Receiver<Vec<u8>>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let closed = Arc::new(AtomicBool::new(false));
        let dropped = Arc::new(AtomicU64::new(0));
        let (arrival_tx, arrivals) = channel();

        let demux_socket = Arc::clone(&socket);
        let demux_peers = Arc::clone(&peers);
        let demux_pending = Arc::clone(&pending);
        let demux_closed = Arc::clone(&closed);
        let demux_dropped = Arc::clone(&dropped);
        std::thread::Builder::new()
            .name(format!("udp-demux-{local}"))
            .spawn(move || {
                let mut buf = vec![0u8; READ_BUF];
                while !demux_closed.load(Ordering::Acquire) {
                    let (n, from) = match demux_socket.recv_from(&mut buf) {
                        Ok(v) => v,
                        Err(_) => continue, // timeout, or a transient error
                    };
                    let mut peers = demux_peers.lock().unwrap_or_else(|e| e.into_inner());
                    if let Some(peer) = peers.get(&from) {
                        // A closed link has been removed from the map, so a
                        // send error here just means "nobody is listening".
                        let _ = peer.tx.send(buf[..n].to_vec());
                        continue;
                    }
                    // First datagram from a new peer. Buffer it: it is very
                    // likely the `Hello`, and dropping it would cost a whole
                    // retry cycle.
                    let mut pending = demux_pending.lock().unwrap_or_else(|e| e.into_inner());
                    if pending.len() >= MAX_PENDING {
                        demux_dropped.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                    let (tx, rx) = channel();
                    let _ = tx.send(buf[..n].to_vec());
                    peers.insert(from, Peer { tx });
                    pending.insert(from, rx);
                    drop(pending);
                    drop(peers);
                    if arrival_tx.send(from).is_err() {
                        return; // the acceptor is gone
                    }
                }
            })
            .map_err(LinkError::io)?;

        Ok(UdpAcceptor {
            socket,
            local,
            peers,
            pending,
            arrivals: Mutex::new(arrivals),
            closed,
            dropped,
        })
    }

    /// Peers dropped because too many strangers showed up at once.
    pub fn dropped_strangers(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

impl LinkAcceptor for UdpAcceptor {
    fn transport(&self) -> &'static str {
        "udp"
    }

    fn local_addr(&self) -> String {
        self.local.to_string()
    }

    fn accept(&self) -> Result<Box<dyn Link>, LinkError> {
        let from = self
            .arrivals
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .recv()
            .map_err(|_| LinkError::Closed)?; // demux died
        let rx = {
            let mut pending = self.pending.lock().unwrap_or_else(|e| e.into_inner());
            pending.remove(&from).ok_or(LinkError::Closed)?
        };
        Ok(Box::new(UdpLink::accepted(
            Arc::clone(&self.socket),
            from,
            rx,
            Arc::clone(&self.peers),
        )))
    }

    fn shutdown(&self) {
        self.closed.store(true, Ordering::Release);
        // Dropping every sender unblocks the readers of every link at once.
        self.peers.lock().unwrap_or_else(|e| e.into_inner()).clear();
        self.pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
    }
}

// ------------------------------------------------------------------ outbound

enum Detach {
    /// Remove our queue from the acceptor's map; that closes the channel.
    Inbound(Peers),
    /// Flip the flag; the reader thread exits and drops its sender.
    Outbound(Arc<AtomicBool>),
}

pub struct UdpLink {
    id: LinkId,
    peer: SocketAddr,
    socket: Arc<UdpSocket>,
    connected: bool,
    rx: Mutex<Option<Receiver<Vec<u8>>>>,
    closed: AtomicBool,
    detach: Detach,
}

impl UdpLink {
    fn connect(addr: &str) -> Result<Self, LinkError> {
        let peer = resolve(addr)?;
        let socket = UdpSocket::bind("0.0.0.0:0").map_err(LinkError::io)?;
        socket.set_read_timeout(Some(POLL)).map_err(LinkError::io)?;
        // `connect` makes the kernel drop datagrams from anyone else, which
        // is the cheapest possible peer filter.
        socket.connect(peer).map_err(LinkError::io)?;
        let socket = Arc::new(socket);
        let (tx, rx) = channel();
        let closed = Arc::new(AtomicBool::new(false));

        let reader_socket = Arc::clone(&socket);
        let reader_closed = Arc::clone(&closed);
        std::thread::Builder::new()
            .name(format!("udp-read-{peer}"))
            .spawn(move || {
                let mut buf = vec![0u8; READ_BUF];
                while !reader_closed.load(Ordering::Acquire) {
                    match reader_socket.recv(&mut buf) {
                        Ok(n) => {
                            if tx.send(buf[..n].to_vec()).is_err() {
                                return; // link dropped
                            }
                        }
                        Err(_) => continue, // timeout
                    }
                }
            })
            .map_err(LinkError::io)?;

        Ok(UdpLink {
            id: next_link_id(),
            peer,
            socket,
            connected: true,
            rx: Mutex::new(Some(rx)),
            closed: AtomicBool::new(false),
            detach: Detach::Outbound(closed),
        })
    }

    fn accepted(
        socket: Arc<UdpSocket>,
        peer: SocketAddr,
        rx: Receiver<Vec<u8>>,
        peers: Peers,
    ) -> Self {
        UdpLink {
            id: next_link_id(),
            peer,
            socket,
            connected: false,
            rx: Mutex::new(Some(rx)),
            closed: AtomicBool::new(false),
            detach: Detach::Inbound(peers),
        }
    }
}

impl Link for UdpLink {
    fn id(&self) -> LinkId {
        self.id
    }

    fn transport(&self) -> &'static str {
        "udp"
    }

    fn peer(&self) -> String {
        self.peer.to_string()
    }

    fn initiator(&self) -> bool {
        self.connected
    }

    fn max_frame(&self) -> usize {
        MAX_DATAGRAM
    }

    fn send(&self, envelope: &[u8]) -> Result<(), LinkError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(LinkError::Closed);
        }
        if envelope.len() > MAX_DATAGRAM {
            return Err(LinkError::FrameTooLarge {
                len: envelope.len(),
                max: MAX_DATAGRAM,
            });
        }
        let written = if self.connected {
            self.socket.send(envelope)
        } else {
            self.socket.send_to(envelope, self.peer)
        };
        written.map(|_| ()).map_err(LinkError::from)
    }

    fn recv(&self) -> Result<Vec<u8>, LinkError> {
        let guard = self.rx.lock().unwrap_or_else(|e| e.into_inner());
        match guard.as_ref() {
            Some(rx) => rx.recv().map_err(|_| LinkError::Closed),
            None => Err(LinkError::Closed),
        }
    }

    fn shutdown(&self) {
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        match &self.detach {
            Detach::Inbound(peers) => {
                peers
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .remove(&self.peer);
            }
            Detach::Outbound(closed) => closed.store(true, Ordering::Release),
        }
    }
}

fn resolve(addr: &str) -> Result<SocketAddr, LinkError> {
    addr.to_socket_addrs()
        .map_err(LinkError::io)?
        .next()
        .ok_or_else(|| LinkError::Framing(format!("cannot resolve '{addr}'")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    fn wait_for<F: FnMut() -> bool>(mut f: F, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            if f() {
                return true;
            }
            if Instant::now() >= deadline {
                return false;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    fn a_datagram_arrives_whole() {
        let acc = UdpAcceptor::bind("127.0.0.1:0").unwrap();
        let addr = acc.local_addr();
        let server = std::thread::spawn(move || {
            let link = acc.accept().expect("accept");
            let first = link.recv().expect("first");
            let second = link.recv().expect("second");
            (first, second)
        });

        let client = UdpLink::connect(&addr).unwrap();
        client.send(b"hello").unwrap();
        client.send(&vec![7u8; 1024]).unwrap();

        let (first, second) = server.join().unwrap();
        assert_eq!(first, b"hello");
        assert_eq!(second.len(), 1024, "no framing means no truncation");
        assert!(second.iter().all(|b| *b == 7));
    }

    #[test]
    fn one_socket_serves_several_peers() {
        let acc = UdpAcceptor::bind("127.0.0.1:0").unwrap();
        let addr = acc.local_addr();
        let acceptor = Arc::new(acc);

        let a = UdpLink::connect(&addr).unwrap();
        a.send(b"from a").unwrap();

        let a_link = acceptor.accept().expect("accept a");
        let b = UdpLink::connect(&addr).unwrap();
        b.send(b"from b").unwrap();
        let b_link = acceptor.accept().expect("accept b");

        assert_eq!(a_link.recv().unwrap(), b"from a");
        assert_eq!(b_link.recv().unwrap(), b"from b");

        // Replies go back to the right peer.
        a_link.send(b"to a").unwrap();
        b_link.send(b"to b").unwrap();
        assert_eq!(a.recv().unwrap(), b"to a");
        assert_eq!(b.recv().unwrap(), b"to b");
        // Two views of one cable: the listener sees each client under its
        // ephemeral address, the client sees the address it dialled.
        assert_ne!(a_link.peer(), b_link.peer(), "two distinct peers");
        assert_eq!(a.peer(), addr, "the client dialled the listener");
        assert_ne!(
            a_link.peer(),
            addr,
            "the listener sees the client, not itself"
        );
    }

    #[test]
    fn shutdown_unblocks_recv_and_send_fails() {
        let acc = UdpAcceptor::bind("127.0.0.1:0").unwrap();
        let addr = acc.local_addr();
        let acceptor = Arc::new(acc);
        let client = UdpLink::connect(&addr).unwrap();
        client.send(b"hi").unwrap();
        let link = acceptor.accept().expect("accept");
        assert_eq!(link.recv().unwrap(), b"hi");

        let reader = {
            let link = Arc::new(link);
            let probe = Arc::clone(&link);
            let t = std::thread::spawn(move || probe.recv().is_err());
            std::thread::sleep(Duration::from_millis(50));
            link.shutdown();
            t
        };
        assert!(reader.join().unwrap(), "recv must unblock on shutdown");
    }

    #[test]
    fn an_oversized_datagram_is_rejected_before_it_is_sent() {
        let link = UdpLink::connect("127.0.0.1:9").unwrap();
        let err = link.send(&vec![0u8; MAX_DATAGRAM + 1]).unwrap_err();
        assert!(matches!(err, LinkError::FrameTooLarge { .. }));
        assert_eq!(link.max_frame(), MAX_DATAGRAM);
        assert!(link.initiator(), "we dialled");
    }

    #[test]
    fn a_dead_peer_is_not_an_error_until_it_is() {
        // Nothing is listening on this port; UDP send still succeeds. That is
        // the medium's honest behaviour, and why the layer above retransmits.
        let sock = UdpSocket::bind("127.0.0.1:0").unwrap();
        let port = sock.local_addr().unwrap().port();
        drop(sock);
        let link = UdpLink::connect(&format!("127.0.0.1:{port}")).unwrap();
        assert!(link.send(b"into the void").is_ok());
    }

    #[test]
    fn the_factory_advertises_a_real_beacon() {
        let f = UdpFactory::new();
        assert_eq!(f.scheme(), "udp");
        let beacon = f.beacon();
        assert!(beacon.can_broadcast());
        assert_eq!(beacon.scheme(), "udp");
    }

    #[test]
    fn the_acceptor_reports_its_bound_port() {
        let acc = UdpAcceptor::bind("127.0.0.1:0").unwrap();
        assert!(acc.local_addr().starts_with("127.0.0.1:"));
        assert_eq!(acc.transport(), "udp");
        assert!(wait_for(|| true, Duration::from_millis(1)));
    }
}
