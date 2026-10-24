//! retexis-routing — identity, routing, forwarding, reliability.
//!
//! Responsibilities:
//!
//! * own the node's identity ([`NodeId`]) and the link-local handshake
//!   (`Hello`) that binds a [`Link`] to a peer identity;
//! * maintain a distance-vector routing table over all known nodes;
//! * forward data envelopes: deliver locally, forward unicast along the
//!   table, flood broadcasts, suppress duplicates, decrement TTL;
//! * authenticate every packet with a pre-shared key, when one is configured;
//! * make unicast reliable with end-to-end acks and retransmission;
//! * keep outbound links alive ([`dial::Dialer`]);
//! * keep flooding from melting the mesh ([`limiter`]);
//! * expose a cheap, cloneable [`MeshHandle`] so application code can send
//!   and inspect the mesh without ever touching a socket.
//!
//! The router is a single-threaded event loop. Everything that touches
//! routing state happens on one thread, which removes a whole class of
//! locking bugs: there is exactly one writer, and readers get an immutable
//! snapshot through [`MeshHandle::status`].

#![forbid(unsafe_code)]

pub mod connector;
pub mod dedup;
pub mod dial;
pub mod limiter;
pub mod router;
pub mod table;

pub use connector::Connector;
pub use dial::{Dialer, ReconnectConfig};
pub use limiter::FloodConfig;
pub use router::start;

use crate::router::Event;
use retexis_core::{
    CodecError, ContentType, Destination, Envelope, Link, LinkAcceptor, LinkError, LinkId,
    MessageId, NodeId,
};
use retexis_crypto::Sealer;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::Sender;
use std::sync::{Arc, RwLock};
use std::thread::JoinHandle;
use std::time::Duration;

/// A notification sink for anything operationally interesting. The CLI prints
/// them; a daemon would ship them to its log or metrics pipeline.
pub type NoticeSink = Arc<dyn Fn(RouterNotice) + Send + Sync>;

/// Discards all notices. Handy in tests.
pub fn silent() -> NoticeSink {
    Arc::new(|_| {})
}

/// Collects notices into a channel, so a test can assert on them.
pub fn channel_sink() -> (NoticeSink, std::sync::mpsc::Receiver<RouterNotice>) {
    let (tx, rx) = std::sync::mpsc::channel();
    let sink: NoticeSink = Arc::new(move |n| {
        let _ = tx.send(n);
    });
    (sink, rx)
}

/// How hard to try before declaring a reliable unicast dead.
#[derive(Clone, Copy, Debug)]
pub struct RetryPolicy {
    /// Total transmissions, including the first one.
    pub max_attempts: u8,
    /// Delay before the second attempt; doubles from there.
    pub base_interval: Duration,
    pub max_interval: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        RetryPolicy {
            max_attempts: 4,
            base_interval: Duration::from_millis(300),
            max_interval: Duration::from_secs(5),
        }
    }
}

#[derive(Clone, Debug)]
pub struct RouterConfig {
    /// Fixed identity, or `None` to mint a random one.
    pub node_id: Option<NodeId>,
    pub node_name: String,
    /// Hop budget stamped on outgoing data packets. `8` means the packet may
    /// cross at most eight links before a node refuses to forward it.
    pub default_ttl: u8,
    /// How often to advertise the routing table to neighbours.
    pub announce_interval: Duration,
    /// A route that has not been refreshed for this long is dropped.
    pub route_timeout: Duration,
    /// How long to remember message ids for duplicate suppression.
    pub dedup_ttl: Duration,
    /// Shared network key. `None` runs in plaintext mode (dev only).
    pub sealer: Option<Sealer>,
    pub retry: RetryPolicy,
    pub flood: FloodConfig,
}

impl Default for RouterConfig {
    fn default() -> Self {
        RouterConfig {
            node_id: None,
            node_name: "node".to_string(),
            default_ttl: 8,
            announce_interval: Duration::from_millis(1000),
            route_timeout: Duration::from_millis(6000),
            dedup_ttl: Duration::from_secs(120),
            sealer: None,
            retry: RetryPolicy::default(),
            flood: FloodConfig::default(),
        }
    }
}

/// Why an envelope was not forwarded.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DropReason {
    Malformed(String),
    TtlExhausted,
    NoRoute,
    WouldLoop,
    TooLarge { len: usize, max: usize },
    EncodeFailed(String),
}

impl fmt::Display for DropReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DropReason::Malformed(m) => write!(f, "malformed packet ({m})"),
            DropReason::TtlExhausted => f.write_str("ttl exhausted"),
            DropReason::NoRoute => f.write_str("no route"),
            DropReason::WouldLoop => f.write_str("would loop back"),
            DropReason::TooLarge { len, max } => {
                write!(f, "envelope {len}B exceeds medium limit {max}B")
            }
            DropReason::EncodeFailed(m) => write!(f, "encode failed ({m})"),
        }
    }
}

/// Everything the routing layer wants to tell a human.
#[derive(Clone, Debug)]
pub enum RouterNotice {
    LinkUp {
        transport: &'static str,
        peer: String,
    },
    LinkDown {
        transport: &'static str,
        peer: String,
        reason: String,
    },
    RouteAdded {
        dst: NodeId,
        name: String,
        metric: u8,
        via: String,
    },
    RouteImproved {
        dst: NodeId,
        old: u8,
        new: u8,
        via: String,
    },
    RouteRemoved {
        dst: NodeId,
        name: String,
        reason: &'static str,
    },
    Dropped {
        msg: MessageId,
        reason: DropReason,
    },
    Flooded {
        msg: MessageId,
        links: usize,
    },
    /// A reliable unicast got its ack.
    Delivered {
        msg: MessageId,
        to: NodeId,
        attempts: u8,
        rtt_ms: u64,
    },
    /// A reliable unicast exhausted its retries.
    DeliveryFailed {
        msg: MessageId,
        dst: NodeId,
        attempts: u8,
    },
    /// A packet failed authentication and was discarded before parsing.
    AuthRejected {
        peer: String,
        reason: &'static str,
    },
    /// A second link to an already connected node was closed.
    DuplicateLink {
        node: NodeId,
        peer: String,
    },
    Reconnecting {
        url: String,
        attempt: u32,
        delay_ms: u64,
    },
    Reconnected {
        url: String,
    },
    DialFailed {
        url: String,
        attempt: u32,
        error: String,
    },
}

fn who(name: &str, id: NodeId) -> String {
    if name.is_empty() {
        id.short()
    } else {
        format!("{name}/{}", id.short())
    }
}

impl fmt::Display for RouterNotice {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RouterNotice::LinkUp { transport, peer } => {
                write!(f, "+ link up    {transport} {peer}")
            }
            RouterNotice::LinkDown {
                transport,
                peer,
                reason,
            } => write!(f, "- link down  {transport} {peer} ({reason})"),
            RouterNotice::RouteAdded {
                dst,
                name,
                metric,
                via,
            } => write!(
                f,
                "> route +    {} via {via} ({} hop)",
                who(name, *dst),
                metric
            ),
            RouterNotice::RouteImproved { dst, old, new, via } => write!(
                f,
                "> route ~    {} via {via} ({old} -> {new} hops)",
                dst.short()
            ),
            RouterNotice::RouteRemoved { dst, name, reason } => {
                write!(f, "> route -    {} ({reason})", who(name, *dst))
            }
            RouterNotice::Dropped { msg, reason } => {
                write!(f, "x dropped    {} ({reason})", msg.short())
            }
            RouterNotice::Flooded { msg, links } => {
                write!(f, "> flood      {} to {links} link(s)", msg.short())
            }
            RouterNotice::Delivered {
                msg,
                to,
                attempts,
                rtt_ms,
            } => write!(
                f,
                "v delivered  {} in {rtt_ms}ms to {} ({attempts} attempt{})",
                msg.short(),
                to.short(),
                if *attempts == 1 { "" } else { "s" }
            ),
            RouterNotice::DeliveryFailed { msg, dst, attempts } => write!(
                f,
                "x failed     {} to {} after {attempts} attempts (no ack)",
                msg.short(),
                dst.short()
            ),
            RouterNotice::AuthRejected { peer, reason } => {
                write!(f, "! auth       rejected {peer} ({reason})")
            }
            RouterNotice::DuplicateLink { node, peer } => {
                write!(f, "- dup link   to {} closed {peer}", node.short())
            }
            RouterNotice::Reconnecting {
                url,
                attempt,
                delay_ms,
            } => write!(f, "~ retry      {url} in {delay_ms}ms (attempt {attempt})"),
            RouterNotice::Reconnected { url } => write!(f, "~ connected  {url}"),
            RouterNotice::DialFailed {
                url,
                attempt,
                error,
            } => write!(f, "! dial       {url} (attempt {attempt}): {error}"),
        }
    }
}

/// A data message that arrived for this node (or for everyone).
#[derive(Clone, Debug)]
pub struct Delivery {
    pub msg_id: MessageId,
    pub from: NodeId,
    pub from_name: Option<String>,
    pub broadcast: bool,
    pub channel: String,
    pub content_type: ContentType,
    pub payload: Vec<u8>,
    /// Hops travelled before reaching us. 0 means we sent it ourselves.
    pub hops: u8,
    pub created_at_ms: u64,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct Stats {
    pub sent: u64,
    pub delivered: u64,
    pub forwarded: u64,
    pub flooded: u64,
    pub dropped_duplicate: u64,
    pub dropped_ttl: u64,
    pub dropped_no_route: u64,
    pub dropped_malformed: u64,
    pub frames_in: u64,
    pub frames_out: u64,
    pub bytes_in: u64,
    pub bytes_out: u64,
    /// Times we re-sent an unacknowledged reliable unicast.
    pub retransmits: u64,
    /// Reliable unicasts that got an ack.
    pub acked: u64,
    /// Reliable unicasts that gave up.
    pub delivery_failed: u64,
    pub acks_sent: u64,
    /// Duplicate packets that were re-acked (our ack had been lost).
    pub duplicate_acks: u64,
    /// Packets discarded by the authentication filter.
    pub auth_rejected: u64,
    /// Relayed broadcasts dropped by the flood limiter.
    pub flood_suppressed: u64,
    /// Redundant links closed by the duplicate-link rule.
    pub duplicate_links: u64,
}

#[derive(Clone, Debug)]
pub struct LinkInfo {
    pub link_id: LinkId,
    pub transport: &'static str,
    pub peer: String,
    pub node: Option<NodeId>,
    pub node_name: String,
    pub max_frame: usize,
    pub initiator: bool,
}

#[derive(Clone, Debug)]
pub struct RouteInfo {
    pub dst: NodeId,
    pub name: String,
    pub metric: u8,
    pub next_hop: LinkId,
    pub next_hop_peer: String,
    pub age_ms: u64,
}

/// Immutable snapshot of the node, published by the router after each event.
#[derive(Clone, Debug, Default)]
pub struct MeshStatus {
    pub self_id: NodeId,
    pub name: String,
    pub links: Vec<LinkInfo>,
    pub routes: Vec<RouteInfo>,
    pub names: std::collections::HashMap<NodeId, String>,
    pub stats: Stats,
    /// Reliable unicasts still waiting for an ack.
    pub pending: usize,
    /// Whether a pre-shared key is configured.
    pub auth: bool,
}

#[derive(Debug)]
pub enum SendError {
    /// The router thread is gone (shutdown, or it panicked).
    RouterGone,
    /// The payload cannot be represented in the wire format.
    Encode(CodecError),
}

impl fmt::Display for SendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SendError::RouterGone => f.write_str("mesh router is not running"),
            SendError::Encode(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for SendError {}

/// The application-facing handle to the mesh.
///
/// Cheap to clone, safe to share across threads: it only owns a channel
/// sender and a read handle on the status snapshot.
#[derive(Clone)]
pub struct MeshHandle {
    me: NodeId,
    name: String,
    tx: Sender<Event>,
    status: Arc<RwLock<MeshStatus>>,
    alive: Arc<AtomicBool>,
    default_ttl: u8,
}

impl MeshHandle {
    pub fn id(&self) -> NodeId {
        self.me
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// False once the router thread has stopped.
    pub fn is_running(&self) -> bool {
        self.alive.load(Ordering::Acquire)
    }

    /// Attaches an already established link to the mesh.
    pub fn add_link(&self, link: Box<dyn Link>) -> Result<LinkId, SendError> {
        let id = link.id();
        self.tx
            .send(Event::LinkUp(Arc::from(link)))
            .map_err(|_| SendError::RouterGone)?;
        Ok(id)
    }

    pub fn add_link_arc(&self, link: Arc<dyn Link>) -> Result<LinkId, SendError> {
        let id = link.id();
        self.tx
            .send(Event::LinkUp(link))
            .map_err(|_| SendError::RouterGone)?;
        Ok(id)
    }

    /// Is this link still attached and identified?
    pub fn is_link_up(&self, link_id: LinkId) -> bool {
        match self.status.read() {
            Ok(s) => s.links.iter().any(|l| l.link_id == link_id),
            Err(_) => false,
        }
    }

    /// Tears a link down as if the medium had failed. The dialer will notice
    /// and reconnect; an operator command or a test can force the situation.
    pub fn drop_link(&self, link_id: LinkId) -> Result<(), SendError> {
        self.tx
            .send(Event::DropLink(link_id))
            .map_err(|_| SendError::RouterGone)
    }

    /// Spawns an accept loop for a listener. Every inbound connection becomes
    /// a new mesh link. Returns the loop's thread handle.
    pub fn accept_from(
        &self,
        acceptor: Box<dyn LinkAcceptor>,
    ) -> Result<JoinHandle<()>, SendError> {
        let me = self.clone();
        let transport = acceptor.transport();
        let jh = std::thread::Builder::new()
            .name(format!("mesh-accept-{transport}"))
            .spawn(move || loop {
                match acceptor.accept() {
                    Ok(link) => {
                        if me.add_link(link).is_err() {
                            break; // router is gone
                        }
                    }
                    Err(LinkError::Closed) => break,
                    Err(_) => {
                        // A single failed handshake (port scan, half-open
                        // socket) must not kill the listener.
                        std::thread::sleep(Duration::from_millis(50));
                    }
                }
            })
            .map_err(|_| SendError::RouterGone)?;
        Ok(jh)
    }

    /// Best-effort broadcast: no acks, flooded, may be suppressed under load.
    pub fn broadcast(
        &self,
        channel: &str,
        content_type: ContentType,
        payload: impl Into<Vec<u8>>,
    ) -> Result<MessageId, SendError> {
        let env = Envelope::data(
            self.me,
            Destination::Broadcast,
            self.default_ttl,
            channel,
            content_type,
            payload.into(),
        );
        self.queue(env, false)
    }

    /// Best-effort unicast: routed, no acks.
    pub fn send_to(
        &self,
        dst: NodeId,
        channel: &str,
        content_type: ContentType,
        payload: impl Into<Vec<u8>>,
    ) -> Result<MessageId, SendError> {
        let env = Envelope::data(
            self.me,
            Destination::Node(dst),
            self.default_ttl,
            channel,
            content_type,
            payload.into(),
        );
        self.queue(env, false)
    }

    /// Reliable unicast: the destination acks, the router retransmits until it
    /// does, and gives up after the configured attempts. Progress and failure
    /// arrive as [`RouterNotice`]s.
    pub fn send_reliable(
        &self,
        dst: NodeId,
        channel: &str,
        content_type: ContentType,
        payload: impl Into<Vec<u8>>,
    ) -> Result<MessageId, SendError> {
        let env = Envelope::data(
            self.me,
            Destination::Node(dst),
            self.default_ttl,
            channel,
            content_type,
            payload.into(),
        )
        .request_ack();
        self.queue(env, true)
    }

    fn queue(&self, env: Envelope, reliable: bool) -> Result<MessageId, SendError> {
        // Validate before queueing so the caller learns about an oversized
        // payload immediately instead of via a log line later.
        env.encode().map_err(SendError::Encode)?;
        let id = env.id;
        let event = if reliable {
            Event::SendReliable(env)
        } else {
            Event::Send(env)
        };
        self.tx.send(event).map_err(|_| SendError::RouterGone)?;
        Ok(id)
    }

    pub fn status(&self) -> MeshStatus {
        self.status.read().map(|s| s.clone()).unwrap_or_default()
    }

    pub fn links(&self) -> Vec<LinkInfo> {
        self.status().links
    }

    pub fn routes(&self) -> Vec<RouteInfo> {
        self.status().routes
    }

    /// Resolves a user typed token to a node id. Accepts a full hex id, a
    /// short hex id, or a peer name (case insensitive, then substring).
    pub fn resolve(&self, token: &str) -> Option<NodeId> {
        let t = token.trim();
        if t.is_empty() {
            return None;
        }
        if let Some(id) = NodeId::parse_loose(t) {
            if id == self.me {
                return Some(id);
            }
            let st = self.status();
            if st.names.contains_key(&id) || st.routes.iter().any(|r| r.dst == id) {
                return Some(id);
            }
            // Fall through: the id may be a short form of a known peer.
        }
        let st = self.status();
        let lower = t.to_ascii_lowercase();
        if let Some((id, _)) = st.names.iter().find(|(_, n)| n.eq_ignore_ascii_case(t)) {
            return Some(*id);
        }
        if let Some((id, _)) = st
            .names
            .iter()
            .find(|(_, n)| n.to_ascii_lowercase().contains(&lower))
        {
            return Some(*id);
        }
        st.names
            .iter()
            .find(|(id, _)| id.short() == lower)
            .map(|(id, _)| *id)
    }

    /// Asks the router to stop. The router sends `Bye` to every neighbour and
    /// closes all links on its way out.
    pub fn shutdown(&self) {
        let _ = self.tx.send(Event::Shutdown);
    }
}
