//! The simulator: real routers, a virtual switch, presentation state.
//!
//! Every node here is a genuine [`retexis_routing::Router`] running on its own
//! thread — the same code the `retexis` binary runs, including the handshake,
//! distance-vector table, dedup cache, flood limiter, ack/retransmit logic and
//! authentication. The simulator adds two things and nothing else:
//!
//! 1. a [`Hub`] that acts as the medium (see `hub.rs`), so the topology can be
//!    edited at runtime;
//! 2. presentation state — node positions, an inbox per node, an event log.
//!
//! That split is deliberate: positions are not protocol, so they never leak
//! into a router — and the router never learns it is being simulated. The
//! payoff is that `retexis-routing` needed **zero** changes to become
//! interactive: the whole medium contract is the `Link` trait.

use crate::hub::{Hub, LinkKey, LinkParams, LinkView};
use retexis_core::{ContentType, NodeId};
use retexis_http::Json;
use retexis_routing::{Delivery, MeshHandle, MeshStatus, RouterConfig, RouterNotice};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::channel;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

pub const CANVAS_W: f64 = 1000.0;
pub const CANVAS_H: f64 = 620.0;

const NAMES: [&str; 16] = [
    "alice", "bob", "carol", "dave", "erin", "frank", "grace", "heidi", "ivan", "judy", "kenji",
    "lena", "oscar", "peggy", "quinn", "rita",
];

/// How long a node keeps a delivered message for the UI.
const INBOX_CAP: usize = 200;
/// How many log lines the UI can page through.
const LOG_CAP: usize = 600;

/// The chat channel name. The simulator speaks the same channel as the CLI,
/// but it deliberately does **not** depend on `retexis-app`: a simulated
/// medium must not drag in real media or a terminal.
const CHAT_CHANNEL: &str = "chat";

/// Counters the inspector shows, in display order.
const STAT_KEYS: [&str; 10] = [
    "sent",
    "delivered",
    "forwarded",
    "retransmits",
    "acked",
    "delivery_failed",
    "dropped_duplicate",
    "flood_suppressed",
    "auth_rejected",
    "duplicate_links",
];

#[derive(Clone, Debug)]
pub struct InboxEntry {
    pub t: f64,
    pub from: String,
    pub broadcast: bool,
    pub hops: u8,
    pub text: String,
}

type Inbox = Arc<Mutex<VecDeque<InboxEntry>>>;

struct Node {
    id: NodeId,
    name: String,
    ttl: u8,
    x: f64,
    y: f64,
    alive: bool,
    handle: Option<MeshHandle>,
    inbox: Inbox,
}

#[derive(Clone, Debug)]
struct LogEntry {
    seq: u64,
    t: f64,
    who: String,
    tag: String,
    detail: String,
}

#[derive(Default)]
struct Log {
    entries: VecDeque<LogEntry>,
    seq: u64,
}

pub struct Sim {
    hub: Arc<Hub>,
    clock: Instant,
    nodes: Mutex<Vec<Node>>,
    log: Arc<Mutex<Log>>,
    name_cursor: AtomicUsize,
}

impl Sim {
    pub fn new(seed: u64) -> Arc<Sim> {
        let sim = Arc::new(Sim {
            hub: Hub::new(seed),
            clock: Instant::now(),
            nodes: Mutex::new(Vec::new()),
            log: Arc::new(Mutex::new(Log::default())),
            name_cursor: AtomicUsize::new(0),
        });
        sim.reset("line", 4);
        sim
    }

    pub fn hub(&self) -> &Arc<Hub> {
        &self.hub
    }

    pub fn clock(&self) -> f64 {
        self.clock.elapsed().as_secs_f64()
    }

    fn nodes(&self) -> MutexGuard<'_, Vec<Node>> {
        self.nodes.lock().unwrap_or_else(|e| e.into_inner())
    }

    // ------------------------------------------------------------------ log

    fn log(&self, who: &str, tag: &str, detail: impl Into<String>) {
        let t = self.clock();
        let mut log = self.log.lock().unwrap_or_else(|e| e.into_inner());
        log.seq += 1;
        let seq = log.seq;
        log.entries.push_back(LogEntry {
            seq,
            t,
            who: who.to_string(),
            tag: tag.to_string(),
            detail: detail.into(),
        });
        while log.entries.len() > LOG_CAP {
            log.entries.pop_front();
        }
    }

    fn notice_sink(&self, who: String) -> retexis_routing::NoticeSink {
        let sim_log = Arc::clone(&self.log);
        let clock = self.clock;
        Arc::new(move |notice: RouterNotice| {
            let (tag, detail) = notice_parts(&notice);
            let t = clock.elapsed().as_secs_f64();
            let mut log = sim_log.lock().unwrap_or_else(|e| e.into_inner());
            log.seq += 1;
            let seq = log.seq;
            log.entries.push_back(LogEntry {
                seq,
                t,
                who: who.clone(),
                tag: tag.to_string(),
                detail,
            });
            while log.entries.len() > LOG_CAP {
                log.entries.pop_front();
            }
        })
    }

    // ---------------------------------------------------------------- nodes

    /// Starts a real router on its own thread and pumps its deliveries into an
    /// inbox. Returns the handle; the router thread is detached and stops on
    /// `shutdown()`.
    fn start_router(&self, id: NodeId, name: &str, ttl: u8, inbox: Inbox) -> MeshHandle {
        let (tx, rx) = channel::<Delivery>();
        let cfg = RouterConfig {
            node_id: Some(id),
            node_name: name.to_string(),
            default_ttl: ttl,
            announce_interval: Duration::from_millis(400),
            route_timeout: Duration::from_millis(2500),
            ..Default::default()
        };
        let (handle, _join) = retexis_routing::start(cfg, tx, self.notice_sink(name.to_string()));

        let clock = self.clock;
        let _ = std::thread::Builder::new()
            .name(format!("sim-inbox-{}", id.short()))
            .spawn(move || {
                while let Ok(d) = rx.recv() {
                    let entry = InboxEntry {
                        t: clock.elapsed().as_secs_f64(),
                        from: d
                            .from_name
                            .clone()
                            .filter(|s| !s.is_empty())
                            .unwrap_or_else(|| d.from.short()),
                        broadcast: d.broadcast,
                        hops: d.hops,
                        text: String::from_utf8_lossy(&d.payload).to_string(),
                    };
                    let mut q = inbox.lock().unwrap_or_else(|e| e.into_inner());
                    q.push_back(entry);
                    while q.len() > INBOX_CAP {
                        q.pop_front();
                    }
                }
            });
        handle
    }

    fn spawn_node(&self, name: &str, ttl: u8, x: f64, y: f64) -> Node {
        let id = NodeId::random();
        let inbox: Inbox = Arc::new(Mutex::new(VecDeque::new()));
        let handle = self.start_router(id, name, ttl, Arc::clone(&inbox));
        self.hub.register(id, name.to_string(), handle.clone());
        Node {
            id,
            name: name.to_string(),
            ttl,
            x,
            y,
            alive: true,
            handle: Some(handle),
            inbox,
        }
    }

    // --------------------------------------------------------------- resets

    pub fn reset(&self, kind: &str, count: usize) {
        let old: Vec<Node> = {
            let mut guard = self.nodes();
            std::mem::take(&mut *guard)
        };
        for node in &old {
            if let Some(h) = &node.handle {
                h.shutdown();
            }
            self.hub.unregister(node.id);
        }
        self.hub.clear_links();
        {
            let mut log = self.log.lock().unwrap_or_else(|e| e.into_inner());
            log.entries.clear();
            log.seq = 0;
        }
        self.name_cursor.store(0, Ordering::Relaxed);

        let count = count.clamp(2, NAMES.len());
        let coords = layout(kind, count);
        let mut fresh = Vec::with_capacity(count);
        for (i, (x, y)) in coords.iter().enumerate() {
            fresh.push(self.spawn_node(NAMES[i], 8, *x, *y));
        }
        let ids: Vec<NodeId> = fresh.iter().map(|n| n.id).collect();
        {
            let mut guard = self.nodes();
            *guard = fresh;
        }
        for (a, b) in edges(kind, count) {
            self.hub
                .connect(ids[a], ids[b], LinkParams::default(), false);
        }
        self.log("hub", "reset", format!("{kind} x{count}"));
    }

    pub fn add_node(
        &self,
        name: Option<String>,
        ttl: u8,
        x: Option<f64>,
        y: Option<f64>,
    ) -> NodeId {
        let idx = self.name_cursor.fetch_add(1, Ordering::Relaxed);
        let base = match name.map(|n| n.trim().to_string()).filter(|n| !n.is_empty()) {
            Some(n) => n,
            None => NAMES[idx % NAMES.len()].to_string(),
        };
        let mut guard = self.nodes();
        let unique = unique_name(&guard, &base);
        let (x, y) = match (x, y) {
            (Some(x), Some(y)) => (x, y),
            _ => {
                // Golden-angle spiral: nodes never stack on top of each other.
                let angle = guard.len() as f64 * 2.399;
                (
                    CANVAS_W / 2.0 + 190.0 * angle.cos(),
                    CANVAS_H / 2.0 + 150.0 * angle.sin(),
                )
            }
        };
        let node = self.spawn_node(&unique, ttl, x, y);
        let id = node.id;
        guard.push(node);
        drop(guard);
        self.log("hub", "node +", unique);
        id
    }

    pub fn update_node(&self, id: NodeId, patch: &Json) -> Result<(), String> {
        let (idx, name, ttl) = {
            let guard = self.nodes();
            let idx = find(&guard, id).ok_or("unknown node")?;
            (idx, guard[idx].name.clone(), guard[idx].ttl)
        };

        let mut restart = false;
        let mut new_name = name.clone();
        let mut new_ttl = ttl;

        if let Some(n) = patch.str_of("name").map(|s| s.trim().to_string()) {
            if !n.is_empty() && n != name {
                new_name = n.chars().take(24).collect();
                restart = true;
            }
        }
        if let Some(t) = patch.f64_of("ttl") {
            let t = t.clamp(1.0, 255.0) as u8;
            if t != ttl {
                new_ttl = t;
                restart = true;
            }
        }
        if let (Some(x), Some(y)) = (patch.f64_of("x"), patch.f64_of("y")) {
            let mut guard = self.nodes();
            guard[idx].x = x;
            guard[idx].y = y;
        }

        if let Some(alive) = patch.bool_of("alive") {
            if alive {
                self.revive(idx)?;
            } else {
                self.kill(idx)?;
            }
        }
        if restart {
            self.restart(idx, &new_name, new_ttl);
        }
        Ok(())
    }

    /// Stops the process. Its router shuts down (sending `Bye`), its links are
    /// unplugged, and it keeps nothing but its inbox.
    fn kill(&self, idx: usize) -> Result<(), String> {
        let (id, name, handle) = {
            let mut guard = self.nodes();
            let node = guard.get_mut(idx).ok_or("unknown node")?;
            if !node.alive {
                return Ok(());
            }
            node.alive = false;
            (node.id, node.name.clone(), node.handle.take())
        };
        if let Some(h) = handle {
            h.shutdown();
        }
        self.hub.unregister(id);
        self.log(&name, "- node", "stopped");
        Ok(())
    }

    fn revive(&self, idx: usize) -> Result<(), String> {
        let (id, name, ttl, alive, inbox) = {
            let guard = self.nodes();
            let node = guard.get(idx).ok_or("unknown node")?;
            (
                node.id,
                node.name.clone(),
                node.ttl,
                node.alive,
                Arc::clone(&node.inbox),
            )
        };
        if alive {
            return Ok(());
        }
        let handle = self.start_router(id, &name, ttl, inbox);
        {
            let mut guard = self.nodes();
            let node = guard.get_mut(idx).ok_or("unknown node")?;
            node.alive = true;
            node.handle = Some(handle.clone());
        }
        self.hub.register(id, name.clone(), handle);
        // A restarted process has no connections. Only cables with a dialer
        // come back by themselves; the rest need an operator (the UI's heal).
        self.hub.replug(id, true);
        self.log(&name, "+ node", "started");
        Ok(())
    }

    /// Operator-initiated restart (rename, TTL change). The identity survives,
    /// the connections do not — but because a human asked, every cable that is
    /// supposed to be up is re-plugged, not just the supervised ones.
    fn restart(&self, idx: usize, name: &str, ttl: u8) {
        let (id, old_handle, inbox) = {
            let mut guard = self.nodes();
            let node = &mut guard[idx];
            node.name = name.to_string();
            node.ttl = ttl;
            (node.id, node.handle.take(), Arc::clone(&node.inbox))
        };
        if let Some(h) = old_handle {
            h.shutdown();
        }
        self.hub.unregister(id);
        if !self.nodes()[idx].alive {
            return;
        }
        let handle = self.start_router(id, name, ttl, inbox);
        {
            let mut guard = self.nodes();
            guard[idx].handle = Some(handle.clone());
        }
        self.hub.register(id, name.to_string(), handle);
        self.hub.replug(id, false);
        self.log(name, "~ node", "restarted (config changed)");
    }

    pub fn delete_node(&self, id: NodeId) -> Result<(), String> {
        let node = {
            let mut guard = self.nodes();
            let idx = find(&guard, id).ok_or("unknown node")?;
            guard.remove(idx)
        };
        if let Some(h) = &node.handle {
            h.shutdown();
        }
        for (key, _) in self.hub.links() {
            if key.contains(id) {
                self.hub.remove_link(key);
            }
        }
        self.hub.unregister(id);
        self.log(&node.name, "- node", "removed");
        Ok(())
    }

    // --------------------------------------------------------------- cables

    pub fn add_link(
        &self,
        a: NodeId,
        b: NodeId,
        params: LinkParams,
        supervise: bool,
    ) -> Result<LinkKey, String> {
        if a == b {
            return Err("cannot link a node to itself".into());
        }
        {
            let guard = self.nodes();
            find(&guard, a).ok_or("unknown node")?;
            find(&guard, b).ok_or("unknown node")?;
        }
        let key = LinkKey::new(a, b);
        self.hub.connect(a, b, params, supervise);
        self.log(
            "hub",
            "+ cable",
            format!(
                "{} ↔ {}",
                self.hub.node_name(a).unwrap_or_default(),
                self.hub.node_name(b).unwrap_or_default()
            ),
        );
        Ok(key)
    }

    pub fn update_link(&self, key: LinkKey, patch: &Json) -> Result<(), String> {
        let view = self.hub.link_view(key).ok_or("unknown link")?;
        let mut params = view.params;
        if let Some(ms) = patch.f64_of("latency") {
            params.latency = Duration::from_secs_f64(ms.clamp(0.0, 5.0));
        }
        if let Some(loss) = patch.f64_of("loss") {
            params.loss = loss.clamp(0.0, 0.95);
        }
        if params != view.params {
            self.hub.set_params(key, params);
        }
        if let Some(up) = patch.bool_of("up") {
            // Always act, never compare: a cable can be `admin_up` yet
            // disconnected (the peer restarted), and "heal" has to re-plug it.
            // `set_up` is idempotent when the cable is already up.
            self.hub.set_up(key, up);
            if up != view.admin_up {
                self.log("hub", if up { "+ heal" } else { "- cut" }, key.key_string());
            }
        }
        if let Some(sup) = patch.bool_of("supervise") {
            self.hub.set_supervise(key, sup);
        }
        Ok(())
    }

    pub fn delete_link(&self, key: LinkKey) -> Result<(), String> {
        if self.hub.link_view(key).is_none() {
            return Err("unknown link".into());
        }
        self.hub.remove_link(key);
        self.log("hub", "- cable", key.key_string());
        Ok(())
    }

    // ----------------------------------------------------------------- send

    pub fn send(
        &self,
        from: NodeId,
        to: Option<NodeId>,
        text: &str,
        reliable: bool,
    ) -> Result<(), String> {
        if text.is_empty() {
            return Err("empty message".into());
        }
        let handle = {
            let guard = self.nodes();
            let idx = find(&guard, from).ok_or("unknown sender")?;
            if !guard[idx].alive {
                return Err("that node is down".into());
            }
            guard[idx].handle.clone().ok_or("that node is down")?
        };
        let sent = match to {
            Some(dst) => {
                if reliable {
                    handle.send_reliable(dst, CHAT_CHANNEL, ContentType::Text, text)
                } else {
                    handle.send_to(dst, CHAT_CHANNEL, ContentType::Text, text)
                }
            }
            None => handle.broadcast(CHAT_CHANNEL, ContentType::Text, text),
        };
        sent.map_err(|e| e.to_string())?;
        Ok(())
    }

    // --------------------------------------------------------------- output

    pub fn snapshot(&self, since_log: u64) -> Json {
        let clock = self.clock();
        let guard = self.nodes();
        let mut nodes: Vec<&Node> = guard.iter().collect();
        nodes.sort_by(|a, b| a.name.cmp(&b.name));

        let mut node_json = Vec::with_capacity(nodes.len());
        for node in &nodes {
            node_json.push(self.node_view(node));
        }
        drop(guard);

        let mut links = Vec::new();
        for (key, view) in self.hub.links() {
            links.push(link_view_json(&key, &view));
        }

        let log = {
            let log = self.log.lock().unwrap_or_else(|e| e.into_inner());
            let entries: Vec<Json> = log
                .entries
                .iter()
                .filter(|e| e.seq > since_log)
                .map(|e| {
                    Json::obj([
                        ("seq", Json::Num(e.seq as f64)),
                        ("t", Json::Num(round3(e.t))),
                        ("who", Json::Str(e.who.clone())),
                        ("tag", Json::Str(e.tag.clone())),
                        ("detail", Json::Str(e.detail.clone())),
                    ])
                })
                .collect();
            (entries, log.seq)
        };

        Json::obj([
            ("clock", Json::Num(round3(clock))),
            ("running", Json::Bool(self.hub.running())),
            ("rate", Json::Num(self.hub.rate())),
            ("nodes", Json::Arr(node_json)),
            ("links", Json::Arr(links)),
            ("log", Json::Arr(log.0)),
            ("log_seq", Json::Num(log.1 as f64)),
        ])
    }

    fn node_view(&self, node: &Node) -> Json {
        // A stopped process has no routing table and no counters. Showing the
        // last snapshot it published would be a lie: the UI would draw routes
        // on a node that cannot use them.
        let status = match (&node.handle, node.alive) {
            (Some(h), true) => h.status(),
            _ => MeshStatus {
                self_id: node.id,
                name: node.name.clone(),
                ..Default::default()
            },
        };

        let mut routes: Vec<Json> = status
            .routes
            .iter()
            .map(|r| {
                Json::obj([
                    ("dst", Json::Str(r.dst.short())),
                    ("name", Json::Str(r.name.clone())),
                    ("hops", Json::Num(r.metric as f64)),
                    ("age", Json::Num(round3(r.age_ms as f64 / 1000.0))),
                ])
            })
            .collect();
        routes.sort_by(|a, b| {
            let ha = a.f64_of("hops").unwrap_or(0.0);
            let hb = b.f64_of("hops").unwrap_or(0.0);
            ha.partial_cmp(&hb).unwrap_or(std::cmp::Ordering::Equal)
        });

        let inbox = {
            let q = node.inbox.lock().unwrap_or_else(|e| e.into_inner());
            let start = q.len().saturating_sub(24);
            q.iter()
                .skip(start)
                .map(|m| {
                    Json::obj([
                        ("t", Json::Num(round3(m.t))),
                        ("from", Json::Str(m.from.clone())),
                        ("broadcast", Json::Bool(m.broadcast)),
                        ("hops", Json::Num(m.hops as f64)),
                        (
                            "text",
                            Json::Str(m.text.chars().take(200).collect::<String>()),
                        ),
                    ])
                })
                .collect()
        };

        Json::obj([
            ("id", Json::Str(node.id.hex())),
            ("short", Json::Str(node.id.short())),
            ("name", Json::Str(node.name.clone())),
            ("alive", Json::Bool(node.alive)),
            ("ttl", Json::Num(node.ttl as f64)),
            ("x", Json::Num(node.x.round())),
            ("y", Json::Num(node.y.round())),
            ("pending", Json::Num(status.pending as f64)),
            ("links", Json::Num(status.links.len() as f64)),
            ("routes", Json::Arr(routes)),
            ("stats", stats_json(&status.stats)),
            ("inbox", Json::Arr(inbox)),
        ])
    }

    pub fn node_ids(&self) -> Vec<NodeId> {
        self.nodes().iter().map(|n| n.id).collect()
    }

    /// Routing snapshot of one node, or `None` when it is down.
    ///
    /// Exposed for tests and for a future headless mode: it is the same data
    /// the UI gets, minus the JSON.
    pub fn status(&self, id: NodeId) -> Option<MeshStatus> {
        let guard = self.nodes();
        let idx = find(&guard, id)?;
        if !guard[idx].alive {
            return None;
        }
        guard[idx].handle.as_ref().map(|h| h.status())
    }

    pub fn inbox(&self, id: NodeId) -> Vec<InboxEntry> {
        let guard = self.nodes();
        match find(&guard, id) {
            Some(idx) => guard[idx]
                .inbox
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .iter()
                .cloned()
                .collect(),
            None => Vec::new(),
        }
    }

    pub fn is_alive(&self, id: NodeId) -> bool {
        self.nodes().iter().any(|n| n.id == id && n.alive)
    }

    pub fn name_of(&self, id: NodeId) -> Option<String> {
        self.nodes()
            .iter()
            .find(|n| n.id == id)
            .map(|n| n.name.clone())
    }

    pub fn id_of(&self, name: &str) -> Option<NodeId> {
        self.nodes().iter().find(|n| n.name == name).map(|n| n.id)
    }
}

fn find(nodes: &[Node], id: NodeId) -> Option<usize> {
    nodes.iter().position(|n| n.id == id)
}

fn unique_name(nodes: &[Node], base: &str) -> String {
    if !nodes.iter().any(|n| n.name == base) {
        return base.to_string();
    }
    let mut i = 2;
    while nodes.iter().any(|n| n.name == format!("{base}{i}")) {
        i += 1;
    }
    format!("{base}{i}")
}

fn round3(v: f64) -> f64 {
    (v * 1000.0).round() / 1000.0
}

fn stats_json(s: &retexis_routing::Stats) -> Json {
    let values = [
        s.sent,
        s.delivered,
        s.forwarded,
        s.retransmits,
        s.acked,
        s.delivery_failed,
        s.dropped_duplicate,
        s.flood_suppressed,
        s.auth_rejected,
        s.duplicate_links,
    ];
    Json::obj(
        STAT_KEYS
            .iter()
            .zip(values.iter())
            .map(|(k, v)| (*k, Json::Num(*v as f64))),
    )
}

fn link_view_json(key: &LinkKey, view: &LinkView) -> Json {
    Json::obj([
        ("key", Json::Str(key.key_string())),
        ("a", Json::Str(key.lo.hex())),
        ("b", Json::Str(key.hi.hex())),
        ("latency", Json::Num(view.params.latency.as_secs_f64())),
        ("loss", Json::Num(view.params.loss)),
        ("up", Json::Bool(view.up)),
        ("supervise", Json::Bool(view.supervise)),
    ])
}

fn label(name: &str, id: NodeId) -> String {
    if name.is_empty() {
        id.short()
    } else {
        format!("{name}/{}", id.short())
    }
}

/// Splits a router notice into the two columns the UI log shows.
fn notice_parts(n: &RouterNotice) -> (&'static str, String) {
    match n {
        RouterNotice::LinkUp { transport, peer } => ("+ link", format!("{transport} {peer}")),
        RouterNotice::LinkDown {
            transport,
            peer,
            reason,
        } => ("- link", format!("{transport} {peer} · {reason}")),
        RouterNotice::RouteAdded {
            dst,
            name,
            metric,
            via,
        } => (
            "route +",
            format!("{} via {via} ({metric}h)", label(name, *dst)),
        ),
        RouterNotice::RouteImproved { dst, old, new, via } => (
            "route ~",
            format!("{} via {via} ({old} -> {new} hops)", dst.short()),
        ),
        RouterNotice::RouteRemoved { dst, name, reason } => {
            ("route -", format!("{} ({reason})", label(name, *dst)))
        }
        RouterNotice::Dropped { msg, reason } => ("dropped", format!("{} ({reason})", msg.short())),
        RouterNotice::Flooded { msg, links } => {
            ("flood", format!("{} to {links} link(s)", msg.short()))
        }
        RouterNotice::Delivered {
            msg,
            to,
            attempts,
            rtt_ms,
        } => (
            "delivered",
            format!(
                "{} in {rtt_ms}ms to {} ({attempts} attempt{})",
                msg.short(),
                to.short(),
                if *attempts == 1 { "" } else { "s" }
            ),
        ),
        RouterNotice::DeliveryFailed { msg, dst, attempts } => (
            "failed",
            format!(
                "{} to {} after {attempts} attempts",
                msg.short(),
                dst.short()
            ),
        ),
        RouterNotice::AuthRejected { peer, reason } => {
            ("auth", format!("rejected {peer} ({reason})"))
        }
        RouterNotice::DuplicateLink { node, peer } => {
            ("dup link", format!("to {} closed {peer}", node.short()))
        }
        RouterNotice::Reconnecting {
            url,
            attempt,
            delay_ms,
        } => (
            "reconnect",
            format!("{url} in {delay_ms}ms (attempt {attempt})"),
        ),
        RouterNotice::Reconnected { url } => ("reconnected", url.clone()),
        RouterNotice::DialFailed {
            url,
            attempt,
            error,
        } => ("dial", format!("{url} (attempt {attempt}): {error}")),
    }
}

// ------------------------------------------------------------------ layouts

pub fn layout(kind: &str, n: usize) -> Vec<(f64, f64)> {
    let n = n.max(2);
    match kind {
        "ring" => {
            let (cx, cy, r) = (CANVAS_W / 2.0, CANVAS_H / 2.0, 210.0);
            (0..n)
                .map(|i| {
                    let a = 2.0 * std::f64::consts::PI * i as f64 / n as f64
                        - std::f64::consts::FRAC_PI_2;
                    (cx + r * a.cos(), cy + r * a.sin())
                })
                .collect()
        }
        "star" => {
            let (cx, cy, r) = (CANVAS_W / 2.0, CANVAS_H / 2.0, 220.0);
            let mut pts = vec![(cx, cy)];
            for i in 0..n.saturating_sub(1) {
                let a = 2.0 * std::f64::consts::PI * i as f64 / (n as f64 - 1.0)
                    - std::f64::consts::FRAC_PI_2;
                pts.push((cx + r * a.cos(), cy + r * a.sin()));
            }
            pts
        }
        "grid" => {
            let cols = (n as f64).sqrt().ceil() as usize;
            let rows = n.div_ceil(cols);
            let dx = if cols > 1 {
                (CANVAS_W - 240.0) / (cols as f64 - 1.0)
            } else {
                0.0
            };
            let dy = if rows > 1 {
                (CANVAS_H - 220.0) / (rows as f64 - 1.0)
            } else {
                0.0
            };
            (0..n)
                .map(|i| {
                    let (r, c) = (i / cols, i % cols);
                    (120.0 + c as f64 * dx, 130.0 + r as f64 * dy)
                })
                .collect()
        }
        _ => {
            let (left, right) = (130.0, CANVAS_W - 130.0);
            let step = (right - left) / (n as f64 - 1.0);
            (0..n)
                .map(|i| (left + i as f64 * step, CANVAS_H / 2.0))
                .collect()
        }
    }
}

pub fn edges(kind: &str, n: usize) -> Vec<(usize, usize)> {
    match kind {
        "ring" => (0..n).map(|i| (i, (i + 1) % n)).collect(),
        "star" => (1..n).map(|i| (0, i)).collect(),
        "grid" => {
            let cols = (n as f64).sqrt().ceil() as usize;
            let mut out = Vec::new();
            for i in 0..n {
                if i % cols != cols - 1 && i + 1 < n {
                    out.push((i, i + 1));
                }
                if i + cols < n {
                    out.push((i, i + cols));
                }
            }
            out
        }
        _ => (0..n.saturating_sub(1)).map(|i| (i, i + 1)).collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layouts_have_the_right_size() {
        for kind in ["line", "ring", "grid", "star"] {
            assert_eq!(layout(kind, 6).len(), 6, "{kind}");
        }
        assert_eq!(layout("star", 5).len(), 5);
        assert_eq!(layout("grid", 9).len(), 9);
    }

    #[test]
    fn line_edges_are_a_chain() {
        assert_eq!(edges("line", 4), vec![(0, 1), (1, 2), (2, 3)]);
        assert_eq!(edges("ring", 4), vec![(0, 1), (1, 2), (2, 3), (3, 0)]);
        assert_eq!(edges("star", 3), vec![(0, 1), (0, 2)]);
    }

    #[test]
    fn notice_tags_stay_short() {
        let (tag, detail) = notice_parts(&RouterNotice::LinkUp {
            transport: "hub",
            peer: "hub:abc".into(),
        });
        assert_eq!(tag, "+ link");
        assert!(detail.contains("hub"));
    }
}
