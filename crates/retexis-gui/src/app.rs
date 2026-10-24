//! Application state: the node, the log, and the operations the window can
//! trigger.
//!
//! Everything here is called from the UI thread, so the only shared mutable
//! state is the [`Log`] behind a mutex. The router is reached through its
//! handle, which is already thread-safe and does not block: `send` hands the
//! envelope to the router thread and returns.
//!
//! There is no JSON here and no `http` dependency: the window reads typed
//! values ([`View`], [`Entry`]). When the UI was a web page this module had a
//! `snapshot()` that serialised everything; that layer is gone with the page,
//! and what is left is the shape a widget actually wants.

use crate::chat::{CHAT_CHANNEL, STREAM_THRESHOLD};
use crate::log::{Ack, Entry, Log, Message};
use retexis_core::{Announcement, NodeId, TransportRegistry};
use retexis_data::stream::StreamSender;
use retexis_data::{send, send_reliable, Frame};
use retexis_discovery::scan::{without_self, Scanner};
use retexis_routing::{Connector, MeshHandle};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Everything the command line can set.
///
/// Plain data, parsed in `main.rs`: the state object should not know about
/// argv, which makes it constructible from a test.
#[derive(Clone, Debug)]
pub struct Options {
    pub name: String,
    pub id: Option<NodeId>,
    pub psk: Option<String>,
    pub listen: Vec<String>,
    pub connect: Vec<String>,
    pub ttl: u8,
    pub announce: bool,
    pub discover: bool,
    pub discovery_port: u16,
    /// Text to broadcast once at startup. Scripting and demos; the window has
    /// a composer for everything else.
    pub say: Option<String>,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            name: String::new(),
            id: None,
            psk: None,
            listen: Vec::new(),
            connect: Vec::new(),
            ttl: 8,
            announce: true,
            discover: false,
            discovery_port: retexis_core::DEFAULT_PORT,
            say: None,
        }
    }
}

/// One reachable node, as the sidebar shows it.
#[derive(Clone, Debug)]
pub struct PeerView {
    pub id: NodeId,
    pub name: String,
    pub hops: u8,
    /// The neighbour we would go through.
    pub via: String,
}

/// One attached link.
#[derive(Clone, Debug)]
pub struct LinkView {
    pub transport: &'static str,
    pub peer: String,
    pub name: String,
}

/// What the window needs to draw the header and the sidebar.
///
/// Rebuilt on every refresh; cheap because a mesh has tens of peers, not
/// thousands.
#[derive(Clone, Debug, Default)]
pub struct View {
    pub peers: Vec<PeerView>,
    pub links: Vec<LinkView>,
    /// Reliable unicasts still waiting for an ack.
    pub pending: usize,
    pub sent: u64,
    pub forwarded: u64,
}

pub struct App {
    me: NodeId,
    name: String,
    handle: MeshHandle,
    log: Arc<Mutex<Log>>,
    started: Instant,
    registry: Arc<TransportRegistry>,
    connector: Connector,
    /// URLs we are listening on, for the header line.
    listen: Vec<String>,
    auth: bool,
}

impl App {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        me: NodeId,
        name: String,
        handle: MeshHandle,
        log: Arc<Mutex<Log>>,
        started: Instant,
        registry: Arc<TransportRegistry>,
        connector: Connector,
        listen: Vec<String>,
        auth: bool,
    ) -> Self {
        App {
            me,
            name,
            handle,
            log,
            started,
            registry,
            connector,
            listen,
            auth,
        }
    }

    pub fn id(&self) -> NodeId {
        self.me
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn is_authed(&self) -> bool {
        self.auth
    }

    pub fn handle(&self) -> &MeshHandle {
        &self.handle
    }

    pub fn listen(&self) -> &[String] {
        &self.listen
    }

    /// A copy of the timeline. Used by tests, and by anything that wants the
    /// conversation without going through the UI.
    pub fn entries(&self) -> Vec<Entry> {
        self.entries_since(0)
    }

    /// Entries newer than `seq`.
    pub fn entries_since(&self, seq: u64) -> Vec<Entry> {
        match self.log.lock() {
            Ok(log) => log.since(seq).cloned().collect(),
            Err(_) => Vec::new(),
        }
    }

    pub fn last_seq(&self) -> u64 {
        match self.log.lock() {
            Ok(log) => log.last_seq(),
            Err(_) => 0,
        }
    }

    pub fn log(&self) -> Arc<Mutex<Log>> {
        Arc::clone(&self.log)
    }

    pub fn started(&self) -> Instant {
        self.started
    }

    pub fn at_ms(&self) -> u64 {
        self.started.elapsed().as_millis() as u64
    }

    /// Adds a line to the timeline. Used for things the router does not report
    /// (a failed bind, a discovery scan) as well as for notices.
    pub fn note(&self, tag: &'static str, text: impl Into<String>) {
        if let Ok(mut log) = self.log.lock() {
            log.system(self.at_ms(), tag, text);
        }
    }

    /// Sends one message. `to = None` broadcasts.
    ///
    /// Long messages take the stream path: one envelope holds 60 KiB, but
    /// chunking above 1 KiB keeps a big paste from blocking the mesh for a
    /// whole envelope's worth of airtime, and it works on a medium whose
    /// `max_frame` is smaller than the text.
    pub fn send(&self, to: Option<NodeId>, text: &str) -> Result<(), String> {
        let text = text.trim_end();
        if text.trim().is_empty() {
            return Err("empty message".into());
        }
        if text.len() > STREAM_THRESHOLD {
            return self.send_stream(to, text);
        }
        let frame = Frame::text(CHAT_CHANNEL, text);
        match to {
            None => {
                let id = send(&self.handle, None, frame).map_err(|e| e.to_string())?;
                self.record(id.hex(), None, text, Ack::None, false);
            }
            Some(dst) => {
                let id = send_reliable(&self.handle, dst, frame).map_err(|e| e.to_string())?;
                self.record(id.hex(), Some(dst.hex()), text, Ack::Pending, false);
            }
        }
        Ok(())
    }

    fn send_stream(&self, to: Option<NodeId>, text: &str) -> Result<(), String> {
        let mut sender = match to {
            Some(dst) => StreamSender::open_reliable(&self.handle, dst, CHAT_CHANNEL, "message"),
            None => StreamSender::open(&self.handle, None, CHAT_CHANNEL, "message"),
        }
        .map_err(|e| e.to_string())?;
        let id = format!("stream-{}", sender.id().short());
        sender.write(text.as_bytes()).map_err(|e| e.to_string())?;
        sender.close().map_err(|e| e.to_string())?;
        // A stream is acked chunk by chunk, so there is no single message id
        // to wait on: the bubble says "sent", not "delivered".
        self.record(id, to.map(|d| d.hex()), text, Ack::None, true);
        Ok(())
    }

    fn record(&self, id: String, to: Option<String>, text: &str, ack: Ack, stream: bool) {
        if let Ok(mut log) = self.log.lock() {
            log.message(
                self.at_ms(),
                Message {
                    id,
                    from: self.me.hex(),
                    from_name: self.name.clone(),
                    to,
                    text: text.to_string(),
                    hops: 0,
                    mine: true,
                    ack,
                    stream,
                    bytes: text.len(),
                },
            );
        }
    }

    /// Marks a message we sent as delivered or failed. Called from the notice
    /// sink, which runs on the router thread.
    pub fn set_ack(&self, id: &str, ack: Ack) {
        if let Ok(mut log) = self.log.lock() {
            log.set_ack(id, ack);
        }
    }

    /// Broadcasts a discovery query and returns who answered, minus ourselves.
    pub fn discover(&self, timeout: Duration) -> Vec<Announcement> {
        let scanner = Scanner::new(self.registry.beacons(), timeout);
        without_self(scanner.scan(), Some(self.me))
    }

    /// Dials a URL, resolving the medium by scheme.
    pub fn connect(&self, url: &str) -> Result<bool, String> {
        if self.is_self(url) {
            return Err(format!("{url} 是本机自己的监听地址，连自己没有意义"));
        }
        self.connector.dial(&self.handle, url)
    }

    /// True when `url` points at one of our own listeners.
    ///
    /// Easy mistake with two windows on one host: the second one types the
    /// first one's port, or picks the same default. The router rejects the
    /// self-link at the handshake, but by then a dialer exists and is
    /// retrying — better to refuse it up front with a sentence a person can
    /// act on.
    fn is_self(&self, url: &str) -> bool {
        let Some((_, rest)) = url.split_once("://") else {
            return false;
        };
        let Some((host, port)) = rest.rsplit_once(':') else {
            return false;
        };
        let same_port = self
            .listen
            .iter()
            .any(|l| l.rsplit_once(':').map(|(_, p)| p) == Some(port));
        if !same_port {
            return false;
        }
        let wildcard_or_loopback = matches!(
            host,
            "127.0.0.1" | "localhost" | "0.0.0.0" | "[::1]" | "[::]" | "::1" | "::"
        );
        wildcard_or_loopback
            || retexis_discovery::local_ip()
                .map(|ip| ip.to_string())
                .as_deref()
                == Some(host)
    }

    /// The header and sidebar, rebuilt from the router's published snapshot.
    pub fn view(&self) -> View {
        let st = self.handle.status();
        let mut routes = st.routes.clone();
        routes.sort_by(|a, b| a.metric.cmp(&b.metric).then_with(|| a.name.cmp(&b.name)));

        View {
            peers: routes
                .iter()
                .map(|r| PeerView {
                    id: r.dst,
                    name: if r.name.is_empty() {
                        r.dst.short()
                    } else {
                        r.name.clone()
                    },
                    hops: r.metric,
                    via: r.next_hop_peer.clone(),
                })
                .collect(),
            links: st
                .links
                .iter()
                .map(|l| LinkView {
                    transport: l.transport,
                    peer: l.peer.clone(),
                    name: l.node_name.clone(),
                })
                .collect(),
            pending: st.pending,
            sent: st.stats.sent,
            forwarded: st.stats.forwarded,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log::Kind;

    fn app() -> (Arc<App>, Arc<Mutex<Log>>) {
        let log = Arc::new(Mutex::new(Log::new()));
        let me = NodeId::random();
        let (tx, _rx) = std::sync::mpsc::channel();
        let (handle, _thread) = retexis_routing::start(
            retexis_routing::RouterConfig {
                node_id: Some(me),
                node_name: "tester".into(),
                ..Default::default()
            },
            tx,
            retexis_routing::silent(),
        );
        // The receiver must outlive the router; leaking it keeps this helper
        // honest without threading a lifetime through every test.
        std::mem::forget(_rx);
        let registry = Arc::new(TransportRegistry::new());
        let connector = Connector::new(
            Arc::clone(&registry),
            retexis_routing::silent(),
            retexis_routing::ReconnectConfig::default(),
        );
        let app = App::new(
            me,
            "tester".into(),
            handle,
            Arc::clone(&log),
            Instant::now(),
            registry,
            connector,
            vec!["tcp://127.0.0.1:1".into()],
            false,
        );
        (Arc::new(app), log)
    }

    fn kinds(log: &Arc<Mutex<Log>>) -> Vec<Kind> {
        log.lock()
            .unwrap()
            .since(0)
            .map(|e| e.kind.clone())
            .collect()
    }

    #[test]
    fn a_broadcast_is_recorded_as_sent_by_me() {
        let (app, log) = app();
        app.send(None, "hello all").unwrap();
        let items = kinds(&log);
        assert_eq!(items.len(), 1);
        match &items[0] {
            Kind::Message(m) => {
                assert!(m.mine);
                assert_eq!(m.to, None);
                assert_eq!(m.ack, Ack::None, "broadcasts are never acked");
                assert_eq!(m.text, "hello all");
            }
            _ => panic!("expected a message"),
        }
    }

    #[test]
    fn a_direct_message_waits_for_its_ack() {
        let (app, log) = app();
        let peer = NodeId::random();
        app.send(Some(peer), "hi bob").unwrap();
        match &kinds(&log)[0] {
            Kind::Message(m) => {
                assert_eq!(m.to, Some(peer.hex()));
                assert_eq!(m.ack, Ack::Pending);
            }
            _ => panic!("expected a message"),
        }
        // The notice sink calls this when the ack arrives.
        let id = match &kinds(&log)[0] {
            Kind::Message(m) => m.id.clone(),
            _ => unreachable!(),
        };
        app.set_ack(&id, Ack::Delivered);
        match &kinds(&log)[0] {
            Kind::Message(m) => assert_eq!(m.ack, Ack::Delivered),
            _ => unreachable!(),
        }
    }

    #[test]
    fn an_empty_message_is_rejected() {
        let (app, log) = app();
        assert!(app.send(None, "   \n").is_err());
        assert!(kinds(&log).is_empty());
    }

    #[test]
    fn a_long_message_takes_the_stream_path() {
        let (app, log) = app();
        let long = "x".repeat(STREAM_THRESHOLD + 1);
        app.send(None, &long).unwrap();
        match &kinds(&log)[0] {
            Kind::Message(m) => {
                assert!(m.stream, "over the threshold it must be a stream");
                assert_eq!(m.bytes, long.len());
            }
            _ => panic!("expected a message"),
        }
    }

    #[test]
    fn dialling_our_own_listener_is_refused() {
        // `app()` listens on tcp://127.0.0.1:1.
        let (app, _log) = app();
        assert!(app.is_self("tcp://127.0.0.1:1"), "loopback, same port");
        assert!(app.is_self("tcp://localhost:1"), "localhost, same port");
        assert!(app.is_self("tcp://0.0.0.0:1"), "wildcard, same port");
        assert!(!app.is_self("tcp://127.0.0.1:2"), "different port");
        assert!(!app.is_self("tcp://10.9.9.9:1"), "another host");
        assert!(!app.is_self("127.0.0.1:1"), "not a url");
        assert!(app.connect("tcp://127.0.0.1:1").is_err());
    }

    #[test]
    fn the_view_is_empty_until_the_mesh_learns_anything() {
        let (app, _log) = app();
        let view = app.view();
        assert!(view.peers.is_empty(), "no peers without links");
        assert!(view.links.is_empty());
        assert_eq!(view.pending, 0);
    }

    #[test]
    fn the_cursor_only_returns_new_entries() {
        let (app, _log) = app();
        app.note("app", "first");
        app.note("app", "second");
        assert_eq!(app.entries_since(0).len(), 2);
        let seq = app.last_seq();
        assert_eq!(seq, 2);
        assert!(app.entries_since(seq).is_empty());
        assert_eq!(app.entries_since(seq - 1).len(), 1);
    }
}
