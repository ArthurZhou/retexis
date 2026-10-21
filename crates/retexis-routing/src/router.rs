//! The single-threaded routing event loop.
//!
//! Everything mutable lives in [`Router`], which is owned by exactly one
//! thread. Links, accept loops, dialers and the application talk to it
//! exclusively through [`Event`] messages, so there is no lock around routing
//! state.

use crate::dedup::DedupOutcome;
use crate::limiter::FloodLimiter;
use crate::table::{LearnOutcome, RoutingTable};
use crate::{
    Delivery, DropReason, LinkInfo, MeshHandle, MeshStatus, NoticeSink, RetryPolicy, RouteInfo,
    RouterConfig, RouterNotice, Stats,
};
use retexis_core::{Body, Destination, Envelope, Link, LinkId, MessageId, NodeId, FLAG_AUTH};
use retexis_crypto::{Sealer, TAG_LEN};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{channel, Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, RwLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

/// Everything that can wake the router up.
pub(crate) enum Event {
    /// A new link has been established (inbound accept or outbound dial).
    LinkUp(Arc<dyn Link>),
    /// Raw bytes arrived on a link.
    Inbound { link_id: LinkId, bytes: Vec<u8> },
    /// A link died or was closed.
    LinkLost { link_id: LinkId, reason: String },
    /// An operator (or a test) tore a link down on purpose.
    DropLink(LinkId),
    /// The local application wants to send something, best effort.
    Send(Envelope),
    /// The local application wants an acked, retransmitted unicast.
    SendReliable(Envelope),
    /// Graceful stop.
    Shutdown,
}

struct Neighbor {
    link: Arc<dyn Link>,
    node: Option<NodeId>,
    node_name: String,
    /// The peer's own id for this link, learned from its `Hello`.
    peer_link_id: Option<u64>,
    /// When we last answered a `Hello` on this link. Rate-limits replies so a
    /// hello flood cannot turn us into an amplifier.
    last_hello_reply: Option<Instant>,
}

/// Minimum gap between `Hello` replies on one link.
const HELLO_REPLY_MIN: Duration = Duration::from_millis(200);

/// Order-independent key for one physical link, from its two local ids.
fn link_key(a: u64, b: u64) -> (u64, u64) {
    if a <= b {
        (a, b)
    } else {
        (b, a)
    }
}

/// One reliable unicast waiting for its ack.
struct Pending {
    env: Envelope,
    /// Transmissions so far, counting the first one.
    attempts: u8,
    next_at: Instant,
    started: Instant,
}

/// Boots a router on its own thread and returns the application handle.
pub fn start(
    cfg: RouterConfig,
    deliveries: Sender<Delivery>,
    notices: NoticeSink,
) -> (MeshHandle, JoinHandle<()>) {
    let me = cfg.node_id.unwrap_or_else(NodeId::random);
    let name = cfg.node_name.clone();
    let (tx, rx) = channel();
    let status = Arc::new(RwLock::new(MeshStatus {
        self_id: me,
        name: name.clone(),
        auth: cfg.sealer.is_some(),
        ..Default::default()
    }));
    let alive = Arc::new(AtomicBool::new(true));

    let handle = MeshHandle {
        me,
        name: name.clone(),
        tx: tx.clone(),
        status: Arc::clone(&status),
        alive: Arc::clone(&alive),
        default_ttl: cfg.default_ttl,
    };

    let router = Router {
        me,
        name,
        default_ttl: cfg.default_ttl,
        announce_interval: cfg.announce_interval,
        route_timeout: cfg.route_timeout,
        retry: cfg.retry,
        sealer: cfg.sealer,
        neighbors: HashMap::new(),
        table: RoutingTable::new(),
        names: HashMap::new(),
        dedup: crate::dedup::DedupCache::new(cfg.dedup_ttl),
        limiter: FloodLimiter::new(cfg.flood),
        pending: HashMap::new(),
        stats: Stats::default(),
        deliveries,
        notices,
        status,
        alive,
        tx,
    };

    let jh = thread::Builder::new()
        .name(format!("mesh-router-{}", me.short()))
        .spawn(move || router.run(rx))
        .expect("failed to spawn the router thread");
    (handle, jh)
}

struct Router {
    me: NodeId,
    name: String,
    default_ttl: u8,
    announce_interval: Duration,
    route_timeout: Duration,
    retry: RetryPolicy,
    sealer: Option<Sealer>,
    neighbors: HashMap<LinkId, Neighbor>,
    table: RoutingTable,
    names: HashMap<NodeId, String>,
    dedup: crate::dedup::DedupCache,
    limiter: FloodLimiter,
    pending: HashMap<MessageId, Pending>,
    stats: Stats,
    deliveries: Sender<Delivery>,
    notices: NoticeSink,
    status: Arc<RwLock<MeshStatus>>,
    alive: Arc<AtomicBool>,
    tx: Sender<Event>,
}

impl Router {
    fn run(mut self, rx: Receiver<Event>) {
        let mut next_announce = Instant::now() + self.announce_interval;
        loop {
            let now = Instant::now();
            let wait = next_announce
                .saturating_duration_since(now)
                .min(self.next_retry_in(now))
                .max(Duration::from_millis(1));
            match rx.recv_timeout(wait) {
                Ok(Event::Shutdown) => break,
                Ok(ev) => self.handle(ev),
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => break,
            }
            let now = Instant::now();
            self.retransmit_due(now);
            if now >= next_announce {
                self.announce_all();
                self.sweep(now);
                next_announce = now + self.announce_interval;
            }
            self.publish();
        }
        self.goodbye();
        self.alive.store(false, Ordering::Release);
        self.publish();
    }

    fn handle(&mut self, ev: Event) {
        match ev {
            Event::LinkUp(link) => self.on_link_up(link),
            Event::Inbound { link_id, bytes } => self.on_inbound(link_id, bytes),
            Event::LinkLost { link_id, reason } => self.on_link_lost(link_id, &reason),
            Event::DropLink(link_id) => self.on_link_lost(link_id, "dropped"),
            Event::Send(env) => self.on_local_send(env),
            Event::SendReliable(env) => self.on_local_send_reliable(env),
            Event::Shutdown => {}
        }
    }

    // ---------------------------------------------------------------- links

    fn on_link_up(&mut self, link: Arc<dyn Link>) {
        let link_id = link.id();
        if self.neighbors.contains_key(&link_id) {
            return;
        }
        let transport = link.transport();
        let peer = link.peer();
        self.neighbors.insert(
            link_id,
            Neighbor {
                link: Arc::clone(&link),
                node: None,
                node_name: String::new(),
                peer_link_id: None,
                last_hello_reply: None,
            },
        );
        self.spawn_reader(link_id, link);
        // Announce ourselves immediately; the peer does the same on its side.
        self.send_hello(link_id);
        self.notice(RouterNotice::LinkUp { transport, peer });
    }

    fn spawn_reader(&self, link_id: LinkId, link: Arc<dyn Link>) {
        let tx = self.tx.clone();
        let spawned = thread::Builder::new()
            .name(format!("mesh-link-{link_id}"))
            .spawn(move || loop {
                match link.recv() {
                    Ok(bytes) => {
                        if tx.send(Event::Inbound { link_id, bytes }).is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        let _ = tx.send(Event::LinkLost {
                            link_id,
                            reason: e.to_string(),
                        });
                        break;
                    }
                }
            });
        if spawned.is_err() {
            self.notice(RouterNotice::LinkDown {
                transport: "?",
                peer: link_id.to_string(),
                reason: "could not spawn reader thread".into(),
            });
        }
    }

    fn on_link_lost(&mut self, link_id: LinkId, reason: &str) {
        let n = match self.neighbors.remove(&link_id) {
            Some(n) => n,
            None => return,
        };
        let transport = n.link.transport();
        let peer = n.link.peer();
        n.link.shutdown();

        for dst in self.table.drop_via(link_id) {
            self.notice(RouterNotice::RouteRemoved {
                dst,
                name: self.name_of(dst),
                reason: "link down",
            });
        }
        self.notice(RouterNotice::LinkDown {
            transport,
            peer,
            reason: reason.to_string(),
        });
    }

    fn goodbye(&mut self) {
        let bye = Envelope::bye(self.me);
        for n in self.neighbors.values() {
            if n.node.is_some() {
                let mut env = bye.clone();
                if self.sealer.is_some() {
                    env.flags |= FLAG_AUTH;
                }
                if let Ok(bytes) = env.encode() {
                    let framed = match &self.sealer {
                        Some(s) => s.seal(&bytes),
                        None => bytes,
                    };
                    let _ = n.link.send(&framed);
                }
            }
            n.link.shutdown();
        }
        self.neighbors.clear();
        self.pending.clear();
    }

    // -------------------------------------------------------------- inbound

    fn on_inbound(&mut self, link_id: LinkId, bytes: Vec<u8>) {
        if !self.neighbors.contains_key(&link_id) {
            return;
        }
        self.stats.frames_in += 1;
        self.stats.bytes_in += bytes.len() as u64;

        // Authentication is a filter on the byte stream: it runs before any
        // parsing, so a hostile packet never reaches the codec.
        let claims_auth = bytes.len() > 4 && (bytes[3] & FLAG_AUTH) != 0;
        let have_key = self.sealer.is_some();
        let accepted = match (have_key, claims_auth) {
            (true, true) => self
                .sealer
                .as_ref()
                .map(|s| s.verify(&bytes))
                .unwrap_or(false),
            (false, false) => true,
            _ => false,
        };
        if !accepted {
            self.stats.auth_rejected += 1;
            let reason = match (have_key, claims_auth) {
                (true, true) => "bad tag",
                (true, false) => "unsigned packet on an authenticated mesh",
                _ => "signed packet but no key configured",
            };
            let peer = self.peer_of(link_id);
            self.notice(RouterNotice::AuthRejected { peer, reason });
            return;
        }
        let body = if claims_auth {
            &bytes[..bytes.len() - TAG_LEN]
        } else {
            &bytes[..]
        };

        let env = match Envelope::decode(body) {
            Ok(e) => e,
            Err(e) => {
                self.stats.dropped_malformed += 1;
                self.notice(RouterNotice::Dropped {
                    msg: MessageId::ZERO,
                    reason: DropReason::Malformed(e.to_string()),
                });
                return;
            }
        };

        match env.body {
            Body::Hello(_) => self.on_hello(link_id, env),
            Body::Route(_) => self.on_route_update(link_id, env),
            Body::Bye => {
                self.on_link_lost(link_id, "peer said bye");
            }
            Body::Data(_) | Body::Ack(_) => self.on_routable(link_id, env),
        }
    }

    /// Handshake: bind a link to a peer identity, then immediately hand over
    /// our whole routing table so a fresh link converges in one round.
    fn on_hello(&mut self, link_id: LinkId, env: Envelope) {
        let peer = env.src;
        let (peer_name, peer_link_id) = match env.body {
            Body::Hello(h) => (h.name, h.link_id),
            _ => (String::new(), 0),
        };

        if peer == self.me {
            self.notice(RouterNotice::LinkDown {
                transport: self.transport_of(link_id),
                peer: self.peer_of(link_id),
                reason: "connected to ourselves".into(),
            });
            if let Some(n) = self.neighbors.remove(&link_id) {
                n.link.shutdown();
            }
            return;
        }

        self.names.insert(peer, peer_name.clone());

        let is_new_identity = match self.neighbors.get(&link_id) {
            Some(n) => n.node != Some(peer),
            None => return,
        };

        // Two nodes can end up with several links between them: both dialled
        // at once, or a reconnect raced a dying socket. Keep exactly one.
        //
        // The choice must be identical on both ends or they will close each
        // other's survivor forever. Each end knows both ids of a link (its own
        // and the peer's, from the Hello), so `(min, max)` is a key both ends
        // compute the same way. Keep the smallest key.
        if is_new_identity {
            let new_key = link_key(link_id, peer_link_id);
            let existing_best = self
                .neighbors
                .iter()
                .filter(|(id, n)| **id != link_id && n.node == Some(peer))
                .filter_map(|(id, n)| n.peer_link_id.map(|pl| link_key(*id, pl)))
                .min();
            if let Some(best) = existing_best {
                if best <= new_key {
                    self.stats.duplicate_links += 1;
                    let peer_addr = self.peer_of(link_id);
                    self.notice(RouterNotice::DuplicateLink {
                        node: peer,
                        peer: peer_addr,
                    });
                    if let Some(n) = self.neighbors.remove(&link_id) {
                        n.link.shutdown();
                    }
                    return;
                }
            }
            let losers: Vec<LinkId> = self
                .neighbors
                .iter()
                .filter(|(id, n)| **id != link_id && n.node == Some(peer))
                .map(|(id, _)| *id)
                .collect();
            for id in losers {
                self.stats.duplicate_links += 1;
                let addr = self.peer_of(id);
                self.notice(RouterNotice::DuplicateLink {
                    node: peer,
                    peer: addr,
                });
                if let Some(n) = self.neighbors.remove(&id) {
                    n.link.shutdown();
                }
            }
        }

        if is_new_identity {
            if let Some(n) = self.neighbors.get_mut(&link_id) {
                n.node = Some(peer);
                n.node_name = peer_name.clone();
                n.peer_link_id = Some(peer_link_id);
            }
            if self.table.set_adjacent(peer, link_id, Instant::now()) {
                self.notice(RouterNotice::RouteAdded {
                    dst: peer,
                    name: peer_name,
                    metric: 1,
                    via: self.peer_of(link_id),
                });
            }
        }

        // Always answer a Hello with our own, even when we already know the
        // peer: it may have lost ours and have no way to tell us, and a
        // half-identified link silently drops everything. One exchange
        // identifies both ends so this terminates; the 200ms window stops a
        // hello flood from turning us into an amplifier.
        let now = Instant::now();
        let need_reply = self
            .neighbors
            .get(&link_id)
            .map(|n| {
                n.last_hello_reply
                    .map(|t| now.saturating_duration_since(t) >= HELLO_REPLY_MIN)
                    .unwrap_or(true)
            })
            .unwrap_or(false);
        if need_reply {
            if let Some(n) = self.neighbors.get_mut(&link_id) {
                n.last_hello_reply = Some(now);
            }
            self.send_hello(link_id);
        }
        if is_new_identity {
            self.send_update_to(link_id);
        }
    }

    fn on_route_update(&mut self, link_id: LinkId, env: Envelope) {
        let from = match self.neighbors.get(&link_id).and_then(|n| n.node) {
            Some(n) => n,
            None => return, // gossip before the handshake is not trusted
        };
        let entries = match env.body {
            Body::Route(r) => r.entries,
            _ => return,
        };
        let now = Instant::now();

        if self.table.set_adjacent(from, link_id, now) {
            self.notice(RouterNotice::RouteAdded {
                dst: from,
                name: self.name_of(from),
                metric: 1,
                via: self.peer_of(link_id),
            });
        }

        for (dst, metric, name) in entries {
            if dst == self.me {
                continue;
            }
            if !name.is_empty() {
                // Names ride along with routes: whoever can reach a node
                // knows what it is called, so the label survives multi-hop.
                // The advertisement is authoritative — a peer may have been
                // renamed since the handshake.
                self.names.insert(dst, name);
            }
            if dst == from {
                continue;
            }
            let candidate = metric.saturating_add(1);
            match self.table.learn(dst, candidate, link_id, now) {
                LearnOutcome::New => {
                    self.notice(RouterNotice::RouteAdded {
                        dst,
                        name: self.name_of(dst),
                        metric: candidate,
                        via: self.peer_of(link_id),
                    });
                }
                LearnOutcome::Improved { old } => {
                    self.notice(RouterNotice::RouteImproved {
                        dst,
                        old,
                        new: candidate,
                        via: self.peer_of(link_id),
                    });
                }
                LearnOutcome::Refreshed | LearnOutcome::Ignored => {}
            }
        }
    }

    // ----------------------------------------------------------- forwarding

    /// Data and acks: both are routed, both are deduplicated, both are
    /// forwarded when they are not ours.
    fn on_routable(&mut self, link_id: LinkId, env: Envelope) {
        let now = Instant::now();
        match self.dedup.mark(env.id, now) {
            DedupOutcome::Duplicate => {
                self.stats.dropped_duplicate += 1;

                if env.dst.target() == Some(self.me) {
                    // A retransmission means our ack was lost. Re-ack
                    // (idempotent) but never hand the payload to the
                    // application twice.
                    if env.wants_ack() {
                        self.stats.duplicate_acks += 1;
                        self.send_ack(&env);
                    }
                    return;
                }

                // We are only a relay here. A duplicate unicast means the
                // sender never got an ack, and our earlier forward may have
                // gone nowhere (the next hop was down). Swallowing it would
                // make the retry useless, so forward it again; TTL bounds the
                // work. Broadcasts are never retried, so they stay suppressed
                // and the storm protection is untouched.
                if env.dst.is_broadcast() {
                    return;
                }
                self.forward_out(&env, Some(link_id));
                return;
            }
            DedupOutcome::New => {}
        }

        let for_me = match env.dst {
            Destination::Broadcast => true,
            Destination::Node(n) => n == self.me,
        };
        if for_me {
            match &env.body {
                Body::Ack(a) => {
                    let acked = a.acked;
                    self.on_ack(env.src, acked);
                }
                _ => {
                    self.deliver(&env);
                    if env.wants_ack() && !env.dst.is_broadcast() {
                        self.send_ack(&env);
                    }
                }
            }
        }
        if !env.dst.is_broadcast() && for_me {
            return; // unicast for us: done
        }
        self.forward_out(&env, Some(link_id));
    }

    fn on_local_send(&mut self, env: Envelope) {
        self.stats.sent += 1;
        self.dedup.is_new(env.id, Instant::now());

        let for_me = match env.dst {
            Destination::Broadcast => true,
            Destination::Node(n) => n == self.me,
        };
        if for_me {
            // Local echo with hops = 0: this is our own message.
            self.deliver(&env);
        }
        if !env.dst.is_broadcast() && for_me {
            return;
        }
        self.forward_out(&env, None);
    }

    fn on_local_send_reliable(&mut self, env: Envelope) {
        match env.dst.target() {
            Some(d) if d != self.me => {}
            _ => {
                // Broadcast or self-addressed: nothing to acknowledge.
                self.on_local_send(env);
                return;
            }
        }
        let now = Instant::now();
        self.pending.insert(
            env.id,
            Pending {
                env: env.clone(),
                attempts: 1,
                next_at: now + self.retry.base_interval,
                started: now,
            },
        );
        self.on_local_send(env);
    }

    /// The single place where an envelope leaves this node towards the mesh.
    /// The first hop costs a hop too, exactly like a forwarded one.
    fn forward_out(&mut self, env: &Envelope, from: Option<LinkId>) {
        if env.ttl == 0 {
            self.stats.dropped_ttl += 1;
            self.notice(RouterNotice::Dropped {
                msg: env.id,
                reason: DropReason::TtlExhausted,
            });
            return;
        }

        let mut wire = env.clone();
        wire.ttl -= 1;
        wire.hops = wire.hops.saturating_add(1);

        match wire.dst {
            Destination::Broadcast => self.flood(&wire, from, from.is_some()),
            Destination::Node(dst) => self.forward_unicast(&wire, dst, from),
        }
    }

    fn forward_unicast(&mut self, env: &Envelope, dst: NodeId, from: Option<LinkId>) {
        match self.table.get(&dst).map(|e| e.next_hop) {
            Some(next) if Some(next) != from => {
                if self.send_via(next, env) {
                    self.stats.forwarded += 1;
                }
            }
            Some(_) => {
                // The only route we have points back where the packet came
                // from: forwarding it would create a two-node loop.
                self.stats.dropped_no_route += 1;
                self.notice(RouterNotice::Dropped {
                    msg: env.id,
                    reason: DropReason::WouldLoop,
                });
            }
            None => {
                // No route yet. Flooding is the route request: whoever owns
                // the destination will answer implicitly by advertising
                // itself, and the reply travels back the same way.
                self.flood(env, from, from.is_some());
            }
        }
    }

    fn flood(&mut self, env: &Envelope, except: Option<LinkId>, relayed: bool) {
        // Relayed broadcasts are the ones that can melt a dense mesh. Our own
        // broadcasts are never throttled here: a user typing is not a storm.
        if relayed && !self.limiter.allow(env.src, Instant::now()) {
            self.stats.flood_suppressed += 1;
            return;
        }

        let targets: Vec<LinkId> = self
            .neighbors
            .iter()
            .filter(|(id, n)| Some(**id) != except && n.node.is_some())
            .map(|(id, _)| *id)
            .collect();
        if targets.is_empty() {
            return;
        }
        let mut sent = 0usize;
        for id in targets {
            if self.send_via(id, env) {
                sent += 1;
            }
        }
        if sent > 0 {
            self.stats.flooded += 1;
            if !env.dst.is_broadcast() {
                self.notice(RouterNotice::Flooded {
                    msg: env.id,
                    links: sent,
                });
            }
        }
    }

    fn send_via(&mut self, link_id: LinkId, env: &Envelope) -> bool {
        let link = match self.neighbors.get(&link_id) {
            Some(n) => Arc::clone(&n.link),
            None => return false,
        };

        let mut stamped = env.clone();
        if self.sealer.is_some() {
            stamped.flags |= FLAG_AUTH;
        }
        let encoded = match stamped.encode() {
            Ok(b) => b,
            Err(e) => {
                self.notice(RouterNotice::Dropped {
                    msg: env.id,
                    reason: DropReason::EncodeFailed(e.to_string()),
                });
                return false;
            }
        };
        // Seal after encoding, so the tag covers every header field including
        // the source id: a packet cannot be re-addressed in flight.
        let bytes = match &self.sealer {
            Some(s) => s.seal(&encoded),
            None => encoded,
        };

        if bytes.len() > link.max_frame() {
            self.notice(RouterNotice::Dropped {
                msg: env.id,
                reason: DropReason::TooLarge {
                    len: bytes.len(),
                    max: link.max_frame(),
                },
            });
            return false;
        }
        match link.send(&bytes) {
            Ok(()) => {
                self.stats.frames_out += 1;
                self.stats.bytes_out += bytes.len() as u64;
                true
            }
            Err(e) => {
                // Report the failure through the channel rather than mutating
                // the neighbour map from inside a send loop.
                let _ = self.tx.send(Event::LinkLost {
                    link_id,
                    reason: e.to_string(),
                });
                false
            }
        }
    }

    fn deliver(&mut self, env: &Envelope) {
        let (channel, content_type, payload) = match &env.body {
            Body::Data(d) => (d.channel.clone(), d.content_type, d.payload.clone()),
            _ => return,
        };
        let from_name = if env.src == self.me {
            Some(self.name.clone())
        } else {
            self.name_of_opt(env.src).or_else(|| {
                self.neighbors
                    .values()
                    .find(|n| n.node == Some(env.src))
                    .map(|n| n.node_name.clone())
            })
        }
        .filter(|s| !s.is_empty());

        let d = Delivery {
            msg_id: env.id,
            from: env.src,
            from_name,
            broadcast: env.dst.is_broadcast(),
            channel,
            content_type,
            payload,
            hops: env.hops,
            created_at_ms: env.created_at_ms,
        };
        self.stats.delivered += 1;
        let _ = self.deliveries.send(d);
    }

    // ------------------------------------------------------------ reliability

    fn send_ack(&mut self, env: &Envelope) {
        let ack = Envelope::ack(self.me, env.src, env.id, self.default_ttl);
        self.stats.acks_sent += 1;
        self.forward_out(&ack, None);
    }

    fn on_ack(&mut self, from: NodeId, acked: MessageId) {
        if let Some(p) = self.pending.remove(&acked) {
            let rtt_ms = p.started.elapsed().as_millis() as u64;
            self.stats.acked += 1;
            self.notice(RouterNotice::Delivered {
                msg: acked,
                to: from,
                attempts: p.attempts,
                rtt_ms,
            });
        }
    }

    /// How long until the next retransmission is due.
    fn next_retry_in(&self, now: Instant) -> Duration {
        self.pending
            .values()
            .map(|p| p.next_at.saturating_duration_since(now))
            .min()
            .unwrap_or(Duration::from_secs(3600))
    }

    fn retransmit_due(&mut self, now: Instant) {
        if self.pending.is_empty() {
            return;
        }
        let due: Vec<MessageId> = self
            .pending
            .iter()
            .filter(|(_, p)| p.next_at <= now)
            .map(|(id, _)| *id)
            .collect();

        for id in due {
            let (env, attempts) = match self.pending.get(&id) {
                Some(p) => (p.env.clone(), p.attempts),
                None => continue,
            };
            if attempts >= self.retry.max_attempts {
                self.pending.remove(&id);
                self.stats.delivery_failed += 1;
                let dst = env.dst.target().unwrap_or_default();
                self.notice(RouterNotice::DeliveryFailed {
                    msg: id,
                    dst,
                    attempts,
                });
                continue;
            }
            let delay = self.backoff(attempts);
            if let Some(p) = self.pending.get_mut(&id) {
                p.attempts += 1;
                p.next_at = now + delay;
            }
            self.stats.retransmits += 1;
            // Same message id, so the receiver's dedup cache recognises it as
            // a retry and only re-acks.
            self.forward_out(&env, None);
        }
    }

    fn backoff(&self, attempts: u8) -> Duration {
        let shift = attempts.saturating_sub(1).min(6);
        let scaled = self.retry.base_interval.saturating_mul(1u32 << shift);
        scaled.min(self.retry.max_interval)
    }

    // ----------------------------------------------------------- control plane

    fn send_hello(&mut self, link_id: LinkId) {
        let env = Envelope::hello(self.me, self.name.clone(), link_id);
        self.send_via(link_id, &env);
    }

    fn send_update_to(&mut self, link_id: LinkId) {
        if self.neighbors.get(&link_id).and_then(|n| n.node).is_none() {
            return; // do not gossip to an unidentified peer
        }
        let entries: Vec<(NodeId, u8, String)> = self
            .table
            .update_for(self.me, Some(link_id))
            .into_iter()
            .map(|(node, metric)| {
                let name = if node == self.me {
                    self.name.clone()
                } else {
                    self.name_of(node)
                };
                (node, metric, name)
            })
            .collect();
        let env = Envelope::route_update(self.me, entries);
        self.send_via(link_id, &env);
    }

    fn announce_all(&mut self) {
        let targets: Vec<LinkId> = self.neighbors.keys().copied().collect();
        for id in targets {
            // A link whose peer never identified itself is useless: we cannot
            // advertise to it and we will not flood to it. `Hello` is
            // idempotent, so keep offering it until the handshake completes.
            // Without this, one lost hello leaves the link half-dead forever
            // (invisible on TCP, fatal on a lossy medium).
            let unidentified = self
                .neighbors
                .get(&id)
                .map(|n| n.node.is_none())
                .unwrap_or(false);
            if unidentified {
                self.send_hello(id);
            } else {
                self.send_update_to(id);
            }
        }
    }

    fn sweep(&mut self, now: Instant) {
        for dst in self.table.expire(now, self.route_timeout) {
            self.notice(RouterNotice::RouteRemoved {
                dst,
                name: self.name_of(dst),
                reason: "timeout",
            });
        }
        self.dedup.sweep(now);
    }

    // -------------------------------------------------------------- helpers

    fn name_of(&self, node: NodeId) -> String {
        self.name_of_opt(node).unwrap_or_default()
    }

    fn name_of_opt(&self, node: NodeId) -> Option<String> {
        self.names.get(&node).cloned()
    }

    fn peer_of(&self, link_id: LinkId) -> String {
        self.neighbors
            .get(&link_id)
            .map(|n| n.link.peer())
            .unwrap_or_default()
    }

    fn transport_of(&self, link_id: LinkId) -> &'static str {
        self.neighbors
            .get(&link_id)
            .map(|n| n.link.transport())
            .unwrap_or("?")
    }

    fn notice(&self, n: RouterNotice) {
        (self.notices)(n);
    }

    /// Publishes an immutable snapshot for `/peers`, `/routes` and metrics.
    fn publish(&self) {
        let now = Instant::now();
        let mut links: Vec<LinkInfo> = self
            .neighbors
            .iter()
            .map(|(id, n)| LinkInfo {
                link_id: *id,
                transport: n.link.transport(),
                peer: n.link.peer(),
                node: n.node,
                node_name: n.node_name.clone(),
                max_frame: n.link.max_frame(),
                initiator: n.link.initiator(),
            })
            .collect();
        links.sort_by(|a, b| a.peer.cmp(&b.peer));

        let mut routes: Vec<RouteInfo> = self
            .table
            .iter()
            .map(|(dst, e)| RouteInfo {
                dst: *dst,
                name: self.name_of(*dst),
                metric: e.metric,
                next_hop: e.next_hop,
                next_hop_peer: self.peer_of(e.next_hop),
                age_ms: now.saturating_duration_since(e.last_seen).as_millis() as u64,
            })
            .collect();
        routes.sort_by(|a, b| a.metric.cmp(&b.metric).then(a.dst.cmp(&b.dst)));

        let snapshot = MeshStatus {
            self_id: self.me,
            name: self.name.clone(),
            links,
            routes,
            names: self.names.clone(),
            stats: self.stats,
            pending: self.pending.len(),
            auth: self.sealer.is_some(),
        };
        if let Ok(mut w) = self.status.write() {
            *w = snapshot;
        }
    }
}
