//! Booting a node: registry → identity → router → pump → listeners →
//! discovery responder → dialers.
//!
//! Split out of `main.rs` so a test can boot a node without a browser, a
//! terminal, or an HTTP server — the two-node test needs exactly this and
//! nothing else. `main` keeps the parts that are actually about being a
//! desktop application: the banner, the browser, the local web server.

use crate::app::{App, Options};
use crate::chat::ChatSink;
use crate::log::{Ack, Log};
use retexis_core::{Announcement, Endpoint, NodeId, TransportRegistry};
use retexis_crypto::Sealer;
use retexis_data::DataHost;
use retexis_discovery as discovery;
use retexis_link_tcp::TcpFactory;
use retexis_link_udp::UdpFactory;
use retexis_routing::{
    Connector, MeshHandle, NoticeSink, ReconnectConfig, RouterConfig, RouterNotice,
};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

/// A running node. Drop it (or the process) and the router stops.
pub struct Boot {
    pub app: Arc<App>,
    pub router: JoinHandle<()>,
    /// What we advertise: one per listener, wildcard already replaced.
    pub endpoints: Vec<Endpoint>,
    /// What we actually bound, for the banner.
    pub listening: Vec<String>,
    /// Kept alive for the whole run: dropping it stops answering queries.
    pub responder: Option<discovery::Responder>,
    /// Non-fatal problems, in the order they happened, for `main` to print.
    pub warnings: Vec<String>,
}

impl Boot {
    /// First endpoint that a peer could dial, as a URL.
    pub fn dial_url(&self) -> Option<String> {
        self.endpoints.first().map(|e| e.url())
    }
}

pub fn boot(opts: &Options) -> Result<Boot, String> {
    let mut warnings = Vec::new();

    // --- media ---------------------------------------------------------------
    let mut registry = TransportRegistry::new();
    registry.register(Arc::new(
        TcpFactory::new().with_discovery_port(opts.discovery_port),
    ));
    registry.register(Arc::new(
        UdpFactory::new().with_discovery_port(opts.discovery_port),
    ));
    let registry = Arc::new(registry);

    // --- identity and router -------------------------------------------------
    let node_id = opts.id.unwrap_or_else(NodeId::random);
    let name = if opts.name.is_empty() {
        format!("node-{}", node_id.short())
    } else {
        opts.name.clone()
    };
    let sealer = opts.psk.as_ref().map(|p| Sealer::from_passphrase(p));
    let auth = sealer.is_some();

    let started = Instant::now();
    let log = Arc::new(Mutex::new(Log::new()));
    let notices: NoticeSink = {
        let log = Arc::clone(&log);
        Arc::new(move |n: RouterNotice| notice(&log, started, n))
    };

    let cfg = RouterConfig {
        node_id: Some(node_id),
        node_name: name.clone(),
        default_ttl: opts.ttl,
        sealer,
        ..Default::default()
    };
    let (tx, rx) = std::sync::mpsc::channel();
    let (handle, router) = retexis_routing::start(cfg, tx, Arc::clone(&notices));

    // --- deliveries → chat log ----------------------------------------------
    {
        let handle = handle.clone();
        let sink = ChatSink::new(Arc::clone(&log), started, node_id);
        std::thread::Builder::new()
            .name("gui-chat".into())
            .spawn(move || {
                let mut host = DataHost::new();
                host.register(Box::new(sink));
                for delivery in rx {
                    host.dispatch(&handle, &delivery);
                }
            })
            .map_err(|e| format!("cannot start the delivery thread: {e}"))?;
    }

    // --- listeners -----------------------------------------------------------
    let implicit = opts.listen.is_empty();
    let urls: Vec<String> = if implicit {
        // Default port with a small fallback range, so a second window on the
        // same machine just works instead of dying on "address in use".
        (7100..7110u16)
            .map(|p| format!("tcp://0.0.0.0:{p}"))
            .collect()
    } else {
        opts.listen.clone()
    };
    let mut endpoints: Vec<Endpoint> = Vec::new();
    let mut listening: Vec<String> = Vec::new();
    let mut last_error = String::new();
    for url in &urls {
        match attach(&registry, &handle, url, &mut endpoints, &mut listening) {
            Ok(()) => {
                if implicit {
                    break; // the first free port is the one we want
                }
            }
            Err(e) => {
                last_error = e;
                if !implicit {
                    warnings.push(format!("cannot listen on {url}: {last_error}"));
                }
            }
        }
    }
    if endpoints.is_empty() {
        warnings.push(format!(
            "no listener ({last_error}) — this node can only dial out"
        ));
    }

    // --- discovery: answer queries ------------------------------------------
    let announcement = Announcement::reply(node_id, name.clone(), endpoints.clone());
    let responder = if opts.announce && !endpoints.is_empty() {
        match discovery::respond(announcement, opts.discovery_port) {
            Ok(r) => Some(r),
            Err(e) => {
                // Another instance on this host usually holds the port. Not
                // fatal: we can still dial, we just cannot be found.
                warnings.push(format!(
                    "cannot answer discovery on {}: {e}",
                    opts.discovery_port
                ));
                None
            }
        }
    } else {
        None
    };

    // --- dialing -------------------------------------------------------------
    let connector = Connector::new(
        Arc::clone(&registry),
        Arc::clone(&notices),
        ReconnectConfig::default(),
    );

    let app = Arc::new(App::new(
        node_id,
        name,
        handle,
        Arc::clone(&log),
        started,
        Arc::clone(&registry),
        connector,
        listening.clone(),
        auth,
    ));

    // Through `App::connect`, not the connector: that is the path that refuses
    // to dial one of our own listeners with a sentence a person can act on, and
    // a `--connect` that points back at us is exactly the mistake the default
    // port range makes easy (two windows on one host).
    for url in &opts.connect {
        match app.connect(url) {
            Ok(true) => app.note("app", format!("正在连接 {url}")),
            Ok(false) => {}
            Err(e) => {
                app.note("app", e.clone());
                warnings.push(e);
            }
        }
    }

    if let Some(text) = &opts.say {
        if let Err(e) = app.send(None, text) {
            app.note("app", format!("startup message not sent: {e}"));
        }
    }

    if opts.discover {
        let found = app.discover(Duration::from_millis(800));
        if found.is_empty() {
            app.note("app", "启动时扫描了一次，附近没有人应答");
        }
        for a in found {
            for ep in &a.endpoints {
                match app.connect(&ep.url()) {
                    Ok(true) => app.note("app", format!("发现 {} · 连接 {}", label(&a), ep.url())),
                    Ok(false) => {}
                    Err(e) => app.note("app", format!("发现 {} 但连接失败：{e}", label(&a))),
                }
            }
        }
    }

    Ok(Boot {
        app,
        router,
        endpoints,
        listening,
        responder,
        warnings,
    })
}

/// Opens one listener and records its endpoint.
fn attach(
    registry: &TransportRegistry,
    handle: &MeshHandle,
    url: &str,
    endpoints: &mut Vec<Endpoint>,
    listening: &mut Vec<String>,
) -> Result<(), String> {
    let acceptor = registry.listen(url).map_err(|e| e.to_string())?;
    let bound = acceptor.local_addr();
    let transport = acceptor.transport();
    // Advertise a dialable address, not the wildcard we bound to.
    let endpoint = Endpoint::new(transport, discovery::advertised_addr(&bound));
    handle
        .accept_from(acceptor)
        .map_err(|e| format!("accept loop failed: {e}"))?;
    listening.push(format!("{transport}://{bound}"));
    endpoints.push(endpoint);
    Ok(())
}

/// What the router did, in the words a person reading a chat window wants.
fn notice(log: &Arc<Mutex<Log>>, started: Instant, n: RouterNotice) {
    let at = started.elapsed().as_millis() as u64;
    let Ok(mut log) = log.lock() else {
        return;
    };
    match &n {
        // The bubble shows the state; a line per message would be noise.
        RouterNotice::Delivered {
            msg,
            attempts,
            rtt_ms,
            ..
        } => {
            log.set_ack(&msg.hex(), Ack::Delivered);
            if *attempts > 1 {
                log.system(
                    at,
                    "deliver",
                    format!("重传 {} 次后送达（{rtt_ms}ms）", attempts - 1),
                );
            }
        }
        RouterNotice::DeliveryFailed { msg, attempts, .. } => {
            log.set_ack(&msg.hex(), Ack::Failed);
            log.system(
                at,
                "deliver",
                format!("发送失败：重试 {attempts} 次仍没有应答"),
            );
        }
        // Deliberately silent: topology churn is in the sidebar already.
        RouterNotice::RouteImproved { .. }
        | RouterNotice::Flooded { .. }
        | RouterNotice::Dropped { .. }
        | RouterNotice::DuplicateLink { .. } => {}
        RouterNotice::LinkUp { transport, peer } => {
            log.system(at, "link", format!("{transport} 链路已连接 · {peer}"));
        }
        RouterNotice::LinkDown {
            transport,
            peer,
            reason,
        } => {
            log.system(
                at,
                "link",
                format!("{transport} 链路断开 · {peer}（{reason}）"),
            );
        }
        RouterNotice::RouteAdded {
            dst,
            name,
            metric,
            via,
        } => {
            log.system(
                at,
                "route",
                format!("可到达 {} · {metric} 跳 · 经 {via}", who(name, *dst)),
            );
        }
        RouterNotice::RouteRemoved { dst, name, reason } => {
            log.system(
                at,
                "route",
                format!("失去路由 {}（{reason}）", who(name, *dst)),
            );
        }
        RouterNotice::AuthRejected { peer, reason } => {
            log.system(
                at,
                "auth",
                format!("来自 {peer} 的包未通过认证（{reason}）"),
            );
        }
        RouterNotice::Reconnecting {
            url,
            attempt,
            delay_ms,
        } => {
            log.system(
                at,
                "link",
                format!("{url} 断开，{delay_ms}ms 后第 {attempt} 次重连"),
            );
        }
        RouterNotice::Reconnected { url } => {
            log.system(at, "link", format!("已重新连接 {url}"));
        }
        RouterNotice::DialFailed {
            url,
            attempt,
            error,
        } => {
            log.system(
                at,
                "link",
                format!("连接 {url} 失败（第 {attempt} 次）：{error}"),
            );
        }
    }
}

fn who(name: &str, id: NodeId) -> String {
    if name.is_empty() {
        id.short()
    } else {
        format!("{name}/{}", id.short())
    }
}

fn label(a: &Announcement) -> String {
    match a.node {
        Some(id) => who(&a.name, id),
        None => a.name.clone(),
    }
}
