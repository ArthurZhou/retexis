//! The virtual switch.
//!
//! A `Hub` is a layer-2 switch for simulated nodes: it owns every "cable"
//! between them, and it is the only thing that can create or destroy one.
//! Nodes never dial each other — they are handed a [`Link`] and told it exists,
//! exactly like a process handed a file descriptor.
//!
//! ```text
//!   node A (real Router thread)          node C (real Router thread)
//!        │  HubLink                            │  HubLink
//!        ▼                                     ▼
//!   ┌──────────────────── Hub ────────────────────┐
//!   │  port ── cable {up, latency, loss} ── port   │
//!   │            │                                │
//!   │            └── scheduler thread: due times   │
//!   └─────────────────────────────────────────────┘
//! ```
//!
//! Everything here is about *moving bytes*. It has no idea what an envelope
//! is, which is the same boundary the real TCP link keeps: framing in, framing
//! out, nothing else. That is why the simulator can reuse the production
//! router unchanged — it is just another medium.
//!
//! Three knobs, all live:
//!
//! * **latency** — each frame is scheduled `latency` into the future. Delivered
//!   by a dedicated thread with a due-time heap, not by a polling loop, so a
//!   5 ms link really is ~5 ms.
//! * **loss** — a per-frame probability, dropped silently (that is what a radio
//!   does; the protocol has to cope).
//! * **up/down** — cutting a cable drops the senders, which makes the blocked
//!   `recv` return `Closed` on both ends. The router then does what it would do
//!   on a real reset: `LinkDown`, route removal, retry of pending unicasts.

use retexis_core::{next_link_id, Link, LinkError, LinkId, NodeId};
use retexis_routing::MeshHandle;
use std::cmp::Ordering as CmpOrdering;
use std::collections::{BinaryHeap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};

/// Largest envelope a hub link will carry. Same order as the TCP link, so a
/// payload that works over the hub works over the wire.
pub const MAX_FRAME: usize = 64 * 1024;

/// How long the scheduler sleeps when nothing is due. Bounds how late a
/// supervision retry or a step deadline can be.
const IDLE_TICK: Duration = Duration::from_millis(25);

/// First supervised reconnect delay; doubles per consecutive failure.
const BACKOFF_BASE: Duration = Duration::from_millis(200);
const BACKOFF_MAX: Duration = Duration::from_secs(3);

// ------------------------------------------------------------------- cable

/// The two endpoints of a cable, in a canonical order.
///
/// Ordering by `NodeId` means both ends compute the same key, so the hub can
/// treat a cable as one object rather than two half-links that might disagree.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, PartialOrd, Ord)]
pub struct LinkKey {
    pub lo: NodeId,
    pub hi: NodeId,
}

impl LinkKey {
    pub fn new(a: NodeId, b: NodeId) -> Self {
        if a <= b {
            LinkKey { lo: a, hi: b }
        } else {
            LinkKey { lo: b, hi: a }
        }
    }

    pub fn contains(&self, node: NodeId) -> bool {
        self.lo == node || self.hi == node
    }

    pub fn other(&self, node: NodeId) -> NodeId {
        if self.lo == node {
            self.hi
        } else {
            self.lo
        }
    }

    /// Stable identifier used by the web UI. Sorted by hex text so it matches
    /// what the browser computes for the same pair.
    pub fn key_string(&self) -> String {
        let (a, b) = (self.lo.hex(), self.hi.hex());
        if a <= b {
            format!("{a}|{b}")
        } else {
            format!("{b}|{a}")
        }
    }

    pub fn parse(key: &str) -> Option<LinkKey> {
        let (a, b) = key.split_once('|')?;
        Some(LinkKey::new(
            NodeId::from_hex(a.trim())?,
            NodeId::from_hex(b.trim())?,
        ))
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct LinkParams {
    /// One-way delay. Applied per frame, both directions.
    pub latency: Duration,
    /// Probability in `0.0..=1.0` that a frame is silently dropped.
    pub loss: f64,
}

impl Default for LinkParams {
    fn default() -> Self {
        LinkParams {
            latency: Duration::from_millis(20),
            loss: 0.0,
        }
    }
}

/// What the UI needs to draw one cable.
#[derive(Clone, Copy, Debug)]
pub struct LinkView {
    pub up: bool,
    pub admin_up: bool,
    pub supervise: bool,
    pub params: LinkParams,
    /// Consecutive failed reconnect attempts, for the event log.
    pub failures: u32,
}

// -------------------------------------------------------------------- state

struct Port {
    key: LinkKey,
    tx: Sender<Vec<u8>>,
}

struct LinkState {
    /// Operator intent: is this cable supposed to be plugged in?
    admin_up: bool,
    /// Reality: are both ends currently attached?
    up: bool,
    params: LinkParams,
    /// Does a dialer keep retrying this cable after a failure?
    supervise: bool,
    failures: u32,
    next_try: Option<Instant>,
    /// Port ids for `(lo, hi)`; `[0, 0]` while down.
    ports: [u64; 2],
}

struct NodeInfo {
    name: String,
    handle: MeshHandle,
}

struct Scheduled {
    due: Instant,
    seq: u64,
    port: u64,
    bytes: Vec<u8>,
}

impl PartialEq for Scheduled {
    fn eq(&self, other: &Self) -> bool {
        self.due == other.due && self.seq == other.seq
    }
}
impl Eq for Scheduled {}
impl PartialOrd for Scheduled {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}
impl Ord for Scheduled {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        (self.due, self.seq).cmp(&(other.due, other.seq))
    }
}

struct Inner {
    nodes: HashMap<NodeId, NodeInfo>,
    links: HashMap<LinkKey, LinkState>,
    ports: HashMap<u64, Port>,
    /// Frames in flight, ordered by delivery time.
    pending: BinaryHeap<std::cmp::Reverse<Scheduled>>,
    paused: bool,
    paused_at: Option<Instant>,
    step_until: Option<Instant>,
    /// Time dilation: `latency / rate`. 1.0 is real time.
    rate: f64,
    seq: u64,
    rng: u64,
    // Counters, mostly so the UI can show that the hub is doing something.
    frames: u64,
    dropped: u64,
    delivered: u64,
    reconnects: u64,
}

impl Inner {
    fn new(seed: u64) -> Self {
        Inner {
            nodes: HashMap::new(),
            links: HashMap::new(),
            ports: HashMap::new(),
            pending: BinaryHeap::new(),
            paused: false,
            paused_at: None,
            step_until: None,
            rate: 1.0,
            seq: 0,
            rng: seed | 1,
            frames: 0,
            dropped: 0,
            delivered: 0,
            reconnects: 0,
        }
    }

    /// xorshift64*: deterministic for a given seed, good enough for loss.
    fn rand_unit(&mut self) -> f64 {
        let mut x = self.rng;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.rng = x;
        let bits = x.wrapping_mul(0x2545_F491_4F6C_DD1D) >> 11;
        bits as f64 / (1u64 << 53) as f64
    }

    fn pause(&mut self, now: Instant) {
        if !self.paused {
            self.paused = true;
            self.paused_at = Some(now);
        }
    }

    fn resume(&mut self, now: Instant) {
        if let Some(at) = self.paused_at.take() {
            // Frames that were in flight when we paused must not arrive in a
            // burst on resume: shift every due time by the pause duration.
            let frozen = now.saturating_duration_since(at);
            if !frozen.is_zero() {
                let items: Vec<Scheduled> = self.pending.drain().map(|r| r.0).collect();
                for mut s in items {
                    s.due += frozen;
                    self.pending.push(std::cmp::Reverse(s));
                }
            }
        }
        self.paused = false;
    }
}

/// Aggregate counters, useful in tests and for a "hub" line in the UI.
#[derive(Clone, Copy, Debug, Default)]
pub struct HubCounters {
    pub frames: u64,
    pub dropped: u64,
    pub delivered: u64,
    pub reconnects: u64,
}

// ---------------------------------------------------------------------- hub

pub struct Hub {
    inner: Mutex<Inner>,
    cv: Condvar,
    stop: AtomicBool,
    next_port: AtomicU64,
}

impl Hub {
    pub fn new(seed: u64) -> Arc<Hub> {
        let hub = Arc::new(Hub {
            inner: Mutex::new(Inner::new(seed)),
            cv: Condvar::new(),
            stop: AtomicBool::new(false),
            next_port: AtomicU64::new(1),
        });
        let runner = Arc::clone(&hub);
        std::thread::Builder::new()
            .name("retexis-hub".to_string())
            .spawn(move || runner.run())
            .expect("spawn hub thread");
        hub
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    // ------------------------------------------------------------- registry

    pub fn register(&self, id: NodeId, name: impl Into<String>, handle: MeshHandle) {
        self.lock().nodes.insert(
            id,
            NodeInfo {
                name: name.into(),
                handle,
            },
        );
        self.cv.notify_all();
    }

    /// Forgets a node and unplugs every cable that touched it. Called when a
    /// simulated process dies: it cannot hold links any more.
    pub fn unregister(&self, id: NodeId) {
        let mut inner = self.lock();
        inner.nodes.remove(&id);
        let keys: Vec<LinkKey> = inner
            .links
            .iter()
            .filter(|(k, _)| k.contains(id))
            .map(|(k, _)| *k)
            .collect();
        for key in keys {
            self.disconnect_locked(&mut inner, key);
        }
        drop(inner);
        self.cv.notify_all();
    }

    pub fn has_node(&self, id: NodeId) -> bool {
        self.lock().nodes.contains_key(&id)
    }

    pub fn node_name(&self, id: NodeId) -> Option<String> {
        self.lock().nodes.get(&id).map(|n| n.name.clone())
    }

    // --------------------------------------------------------------- cables

    /// Creates the cable if needed, then plugs it in.
    ///
    /// Returns `false` when an endpoint is missing, which is the honest answer
    /// for "connect a node that is down": the cable exists in the config, but
    /// there is nothing on the other end to talk to.
    pub fn connect(
        self: &Arc<Self>,
        a: NodeId,
        b: NodeId,
        params: LinkParams,
        supervise: bool,
    ) -> bool {
        if a == b {
            return false;
        }
        let key = LinkKey::new(a, b);
        let mut inner = self.lock();
        let entry = inner.links.entry(key).or_insert(LinkState {
            admin_up: true,
            up: false,
            params,
            supervise,
            failures: 0,
            next_try: None,
            ports: [0, 0],
        });
        entry.admin_up = true;
        entry.params = params;
        entry.supervise = supervise;
        let ok = self.connect_locked(&mut inner, key);
        drop(inner);
        self.cv.notify_all();
        ok
    }

    /// Plugs in every cable that is supposed to be up and touches `node`.
    ///
    /// `only_supervised` is the difference between "the operator restarted
    /// this node" (replug everything) and "the process crashed and came back"
    /// (only cables with a dialer come back on their own).
    pub fn replug(self: &Arc<Self>, node: NodeId, only_supervised: bool) {
        let mut inner = self.lock();
        let keys: Vec<LinkKey> = inner
            .links
            .iter()
            .filter(|(k, st)| k.contains(node) && st.admin_up && !st.up)
            .filter(|(_, st)| !only_supervised || st.supervise)
            .map(|(k, _)| *k)
            .collect();
        for key in keys {
            self.connect_locked(&mut inner, key);
        }
        drop(inner);
        self.cv.notify_all();
    }

    pub fn set_params(&self, key: LinkKey, params: LinkParams) {
        let mut inner = self.lock();
        if let Some(st) = inner.links.get_mut(&key) {
            st.params = params;
        }
    }

    pub fn set_supervise(&self, key: LinkKey, supervise: bool) {
        let mut inner = self.lock();
        if let Some(st) = inner.links.get_mut(&key) {
            st.supervise = supervise;
            if !supervise {
                st.next_try = None;
            }
        }
        drop(inner);
        self.cv.notify_all();
    }

    /// Operator intent for one cable. Cutting is `false`: it drops both ends
    /// immediately and stops any reconnect attempt, so a partition stays a
    /// partition until someone heals it.
    pub fn set_up(self: &Arc<Self>, key: LinkKey, up: bool) {
        let mut inner = self.lock();
        let Some(st) = inner.links.get_mut(&key) else {
            return;
        };
        st.admin_up = up;
        if up {
            st.failures = 0;
            st.next_try = None;
            self.connect_locked(&mut inner, key);
        } else {
            self.disconnect_locked(&mut inner, key);
        }
        drop(inner);
        self.cv.notify_all();
    }

    pub fn remove_link(&self, key: LinkKey) {
        let mut inner = self.lock();
        self.disconnect_locked(&mut inner, key);
        inner.links.remove(&key);
        drop(inner);
        self.cv.notify_all();
    }

    pub fn clear_links(&self) {
        let mut inner = self.lock();
        let keys: Vec<LinkKey> = inner.links.keys().copied().collect();
        for key in keys {
            self.disconnect_locked(&mut inner, key);
        }
        inner.links.clear();
        inner.pending.clear();
        drop(inner);
        self.cv.notify_all();
    }

    pub fn link_view(&self, key: LinkKey) -> Option<LinkView> {
        self.lock().links.get(&key).map(|st| LinkView {
            up: st.up,
            admin_up: st.admin_up,
            supervise: st.supervise,
            params: st.params,
            failures: st.failures,
        })
    }

    pub fn links(&self) -> Vec<(LinkKey, LinkView)> {
        let inner = self.lock();
        let mut out: Vec<(LinkKey, LinkView)> = inner
            .links
            .iter()
            .map(|(k, st)| {
                (
                    *k,
                    LinkView {
                        up: st.up,
                        admin_up: st.admin_up,
                        supervise: st.supervise,
                        params: st.params,
                        failures: st.failures,
                    },
                )
            })
            .collect();
        out.sort_by_key(|(k, _)| k.key_string());
        out
    }

    // -------------------------------------------------------------- runtime

    pub fn running(&self) -> bool {
        !self.lock().paused
    }

    pub fn set_running(&self, running: bool) {
        let mut inner = self.lock();
        let now = Instant::now();
        inner.step_until = None;
        if running {
            inner.resume(now);
        } else {
            inner.pause(now);
        }
        drop(inner);
        self.cv.notify_all();
    }

    /// Runs for `secs` of wall clock, then pauses again. Stepping is just a
    /// scheduled pause, which is why it composes with everything else.
    pub fn step(&self, secs: f64) {
        let mut inner = self.lock();
        let now = Instant::now();
        inner.resume(now);
        inner.step_until = Some(now + Duration::from_secs_f64(secs.max(0.01)));
        drop(inner);
        self.cv.notify_all();
    }

    pub fn rate(&self) -> f64 {
        self.lock().rate
    }

    /// Time dilation: `latency / rate`. Only the medium is scaled — announce
    /// timers live in the routers and stay real.
    pub fn set_rate(&self, rate: f64) {
        let mut inner = self.lock();
        inner.rate = rate.clamp(0.1, 20.0);
        drop(inner);
        self.cv.notify_all();
    }

    pub fn counters(&self) -> HubCounters {
        let inner = self.lock();
        HubCounters {
            frames: inner.frames,
            dropped: inner.dropped,
            delivered: inner.delivered,
            reconnects: inner.reconnects,
        }
    }

    pub fn pending_frames(&self) -> usize {
        self.lock().pending.len()
    }

    pub fn shutdown(&self) {
        self.stop.store(true, Ordering::Release);
        self.cv.notify_all();
    }

    // ------------------------------------------------------------ internals

    fn connect_locked(self: &Arc<Self>, inner: &mut Inner, key: LinkKey) -> bool {
        let Some(st) = inner.links.get(&key) else {
            return false;
        };
        if st.up {
            return true;
        }
        if !st.admin_up {
            return false;
        }
        if !inner.nodes.contains_key(&key.lo) || !inner.nodes.contains_key(&key.hi) {
            return false;
        }
        let params = st.params;

        // Both ports exist before either end is attached: the first `Hello`
        // must find a destination, otherwise it is dropped and the handshake
        // has to wait for a retry.
        let (tx_lo, rx_lo) = channel();
        let (tx_hi, rx_hi) = channel();
        let p_lo = self.next_port.fetch_add(1, Ordering::Relaxed);
        let p_hi = self.next_port.fetch_add(1, Ordering::Relaxed);
        inner.ports.insert(p_lo, Port { key, tx: tx_lo });
        inner.ports.insert(p_hi, Port { key, tx: tx_hi });

        let lo_link = Arc::new(HubLink::new(Arc::clone(self), key, key.lo, p_lo, rx_lo));
        let hi_link = Arc::new(HubLink::new(Arc::clone(self), key, key.hi, p_hi, rx_hi));

        if let Some(st) = inner.links.get_mut(&key) {
            st.ports = [p_lo, p_hi];
            st.up = true;
            st.failures = 0;
            st.next_try = None;
            st.params = params;
        }

        let (lo_handle, hi_handle) = match (inner.nodes.get(&key.lo), inner.nodes.get(&key.hi)) {
            (Some(a), Some(b)) => (a.handle.clone(), b.handle.clone()),
            _ => {
                inner.ports.remove(&p_lo);
                inner.ports.remove(&p_hi);
                if let Some(st) = inner.links.get_mut(&key) {
                    st.ports = [0, 0];
                    st.up = false;
                }
                return false;
            }
        };

        let lo_ok = lo_handle.add_link_arc(lo_link).is_ok();
        let hi_ok = hi_handle.add_link_arc(hi_link).is_ok();
        if !lo_ok || !hi_ok {
            inner.ports.remove(&p_lo);
            inner.ports.remove(&p_hi);
            if let Some(st) = inner.links.get_mut(&key) {
                st.ports = [0, 0];
                st.up = false;
            }
            return false;
        }
        true
    }

    fn disconnect_locked(&self, inner: &mut Inner, key: LinkKey) {
        let Some(st) = inner.links.get(&key) else {
            return;
        };
        let ports = st.ports;
        // Dropping the senders is what unblocks the readers on both ends, so
        // each router sees a clean `LinkError::Closed` and reports `LinkDown`.
        inner.ports.remove(&ports[0]);
        inner.ports.remove(&ports[1]);
        if let Some(st) = inner.links.get_mut(&key) {
            st.ports = [0, 0];
            st.up = false;
            st.next_try = None;
        }
    }

    /// Called by a [`HubLink`] when it is torn down (peer died, router
    /// shutdown, protocol closed it). Tears down the other end too: a cable
    /// with one end attached is not a cable.
    fn close_port(&self, port: u64) {
        let mut inner = self.lock();
        let Some(entry) = inner.ports.remove(&port) else {
            return;
        };
        let key = entry.key;
        let (other, was_up) = match inner.links.get(&key) {
            Some(st) if st.up => (
                if st.ports[0] == port {
                    st.ports[1]
                } else {
                    st.ports[0]
                },
                true,
            ),
            _ => (0, false),
        };
        if other != 0 {
            inner.ports.remove(&other);
        }
        if was_up {
            if let Some(st) = inner.links.get_mut(&key) {
                st.ports = [0, 0];
                st.up = false;
                st.failures = st.failures.saturating_add(1);
                st.next_try = if st.supervise && st.admin_up {
                    Some(Instant::now() + backoff(st.failures))
                } else {
                    None
                };
            }
        }
        drop(inner);
        self.cv.notify_all();
    }

    fn send_from(&self, port: u64, bytes: &[u8]) -> Result<(), LinkError> {
        if bytes.len() > MAX_FRAME {
            return Err(LinkError::FrameTooLarge {
                len: bytes.len(),
                max: MAX_FRAME,
            });
        }
        let mut inner = self.lock();
        let Some(entry) = inner.ports.get(&port) else {
            return Err(LinkError::Closed);
        };
        let key = entry.key;
        let Some(st) = inner.links.get(&key) else {
            return Err(LinkError::Closed);
        };
        if !st.up {
            return Err(LinkError::Closed);
        }
        let params = st.params;
        let target = if st.ports[0] == port {
            st.ports[1]
        } else {
            st.ports[0]
        };

        inner.frames += 1;
        if params.loss > 0.0 && inner.rand_unit() < params.loss {
            inner.dropped += 1;
            return Ok(()); // silence, not an error: a lost frame is not a fault
        }

        let rate = inner.rate.max(0.1);
        let delay = params.latency.mul_f64(1.0 / rate);
        let seq = inner.seq;
        inner.seq += 1;
        inner.pending.push(std::cmp::Reverse(Scheduled {
            due: Instant::now() + delay,
            seq,
            port: target,
            bytes: bytes.to_vec(),
        }));
        drop(inner);
        self.cv.notify_all();
        Ok(())
    }

    // -------------------------------------------------------------- threads

    fn run(self: Arc<Self>) {
        let mut inner = self.lock();
        while !self.stop.load(Ordering::Acquire) {
            let now = Instant::now();
            self.tick(&mut inner, now);
            let wait = wait_time(&inner, now);
            let (guard, _) = self
                .cv
                .wait_timeout(inner, wait)
                .unwrap_or_else(|e| e.into_inner());
            inner = guard;
        }
    }

    fn tick(self: &Arc<Self>, inner: &mut Inner, now: Instant) {
        if let Some(deadline) = inner.step_until {
            if now >= deadline {
                inner.step_until = None;
                inner.pause(now);
            }
        }
        self.supervise_locked(inner, now);
        if inner.paused {
            return;
        }
        loop {
            let due = match inner.pending.peek() {
                Some(top) if top.0.due <= now => top.0.due,
                _ => break,
            };
            let _ = due;
            let Some(item) = inner.pending.pop() else {
                break;
            };
            let s = item.0;
            let sent = match inner.ports.get(&s.port) {
                Some(port) => port.tx.send(s.bytes).is_ok(),
                None => false,
            };
            if sent {
                inner.delivered += 1;
            }
        }
    }

    /// Keeps supervised cables plugged in, with exponential backoff.
    fn supervise_locked(self: &Arc<Self>, inner: &mut Inner, now: Instant) {
        let due: Vec<LinkKey> = inner
            .links
            .iter()
            .filter(|(k, st)| {
                st.admin_up
                    && st.supervise
                    && !st.up
                    && st.next_try.map(|t| now >= t).unwrap_or(true)
                    && inner.nodes.contains_key(&k.lo)
                    && inner.nodes.contains_key(&k.hi)
            })
            .map(|(k, _)| *k)
            .collect();
        for key in due {
            if let Some(st) = inner.links.get_mut(&key) {
                st.next_try = Some(now + backoff(st.failures));
            }
            if self.connect_locked(inner, key) {
                inner.reconnects += 1;
            }
        }
    }
}

fn backoff(failures: u32) -> Duration {
    let shift = failures.min(4);
    (BACKOFF_BASE * (1 << shift)).min(BACKOFF_MAX)
}

fn wait_time(inner: &Inner, now: Instant) -> Duration {
    let horizon = now + IDLE_TICK;
    if inner.paused {
        return IDLE_TICK;
    }
    match inner.pending.peek() {
        Some(top) if top.0.due < horizon => top.0.due.saturating_duration_since(now),
        _ => IDLE_TICK,
    }
}

// ----------------------------------------------------------------- hub link

/// One end of a cable, as the router sees it.
///
/// This is the whole point of the `Link` trait: the router cannot tell this
/// apart from a TCP socket. `recv` blocks on a channel instead of a socket,
/// `send` hands bytes to the switch instead of the kernel.
pub struct HubLink {
    id: LinkId,
    port: u64,
    key: LinkKey,
    me: NodeId,
    hub: Arc<Hub>,
    rx: Mutex<Option<Receiver<Vec<u8>>>>,
    closed: AtomicBool,
}

impl HubLink {
    fn new(hub: Arc<Hub>, key: LinkKey, me: NodeId, port: u64, rx: Receiver<Vec<u8>>) -> Self {
        HubLink {
            id: next_link_id(),
            port,
            key,
            me,
            hub,
            rx: Mutex::new(Some(rx)),
            closed: AtomicBool::new(false),
        }
    }
}

impl Link for HubLink {
    fn id(&self) -> LinkId {
        self.id
    }

    fn transport(&self) -> &'static str {
        "hub"
    }

    fn peer(&self) -> String {
        format!("hub:{}", self.key.other(self.me).short())
    }

    fn initiator(&self) -> bool {
        // Neither end dialled: the switch plugged the cable in.
        false
    }

    fn max_frame(&self) -> usize {
        MAX_FRAME
    }

    fn send(&self, envelope: &[u8]) -> Result<(), LinkError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(LinkError::Closed);
        }
        self.hub.send_from(self.port, envelope)
    }

    fn recv(&self) -> Result<Vec<u8>, LinkError> {
        let guard = self.rx.lock().unwrap_or_else(|e| e.into_inner());
        match guard.as_ref() {
            Some(rx) => rx.recv().map_err(|_| LinkError::Closed),
            None => Err(LinkError::Closed),
        }
    }

    fn shutdown(&self) {
        self.closed.store(true, Ordering::Release);
        self.hub.close_port(self.port);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn link_keys_are_order_independent() {
        let a = NodeId::random();
        let b = NodeId::random();
        assert_eq!(LinkKey::new(a, b), LinkKey::new(b, a));
        assert_eq!(LinkKey::new(a, b).other(a), b);
        assert_eq!(LinkKey::new(a, b).other(b), a);
    }

    #[test]
    fn key_string_roundtrips() {
        let a = NodeId::random();
        let b = NodeId::random();
        let key = LinkKey::new(a, b);
        let text = key.key_string();
        assert_eq!(LinkKey::parse(&text), Some(key));
        assert_eq!(LinkKey::parse("nope"), None);
    }

    #[test]
    fn backoff_doubles_and_caps() {
        assert_eq!(backoff(0), BACKOFF_BASE);
        assert_eq!(backoff(1), BACKOFF_BASE * 2);
        assert_eq!(backoff(9), BACKOFF_MAX);
    }

    #[test]
    fn loss_is_reproducible_for_a_seed() {
        let mut a = Inner::new(7);
        let mut b = Inner::new(7);
        let xs: Vec<u64> = (0..8).map(|_| (a.rand_unit() * 1e6) as u64).collect();
        let ys: Vec<u64> = (0..8).map(|_| (b.rand_unit() * 1e6) as u64).collect();
        assert_eq!(xs, ys);
        // ...and actually spread over the unit interval.
        assert!(a.rand_unit() >= 0.0 && a.rand_unit() <= 1.0);
    }
}
