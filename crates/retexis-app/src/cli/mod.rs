//! The command-line application.
//!
//! This is the **application** layer: it parses arguments, owns the terminal,
//! renders tables, and wires the media together. It contains no protocol: every
//! frame it prints came from [`retexis_data`], and every byte it sends goes
//! through [`retexis_data`] too.
//!
//! Wiring is deliberately boring: build a registry of media, boot a router,
//! attach listeners, supervise dialers, pump deliveries into the data host.
//!
//! The registry is the only place that knows which media exist. Both the
//! listener path and the dialer path resolve a URL through it, so adding
//! `udp://` (or a future `ws://`) is a `register` call and nothing else.

pub mod args;
pub mod connect;
pub mod repl;
pub mod scan;
pub mod view;

use crate::chat::ChatApp;
use crate::term;
use args::Options;
use connect::Connector;
use retexis_core::{Announcement, Endpoint, NodeId, TransportRegistry};
use retexis_data::DataHost;
use retexis_discovery as discovery;
use retexis_link_tcp::TcpFactory;
use retexis_link_udp::UdpFactory;
use retexis_routing::dial::ReconnectConfig;
use retexis_routing::start;
use retexis_routing::{FloodConfig, NoticeSink, RetryPolicy, RouterConfig, RouterNotice};
use std::io::IsTerminal;
use std::sync::mpsc::channel;
use std::sync::Arc;
use std::time::Duration;

/// Runs the CLI. The binary crate is a one-line wrapper around this.
pub fn run() {
    let opts = Options::parse(std::env::args().skip(1));

    // Colour only when a human is looking: piped output and the demo scripts
    // must stay plain text.
    view::set_color(std::io::stdout().is_terminal());

    // --- media registry -----------------------------------------------------
    // One line per medium. Adding WebSocket/HTTP/radio support later is one
    // `register` call plus the crate that implements it.
    let mut registry = TransportRegistry::new();
    registry.register(Arc::new(
        TcpFactory::new().with_discovery_port(opts.discovery_port),
    ));
    registry.register(Arc::new(
        UdpFactory::new().with_discovery_port(opts.discovery_port),
    ));
    // Shared from here on: the dial path lives on its own threads, and the GUI
    // reaches the same registry through the same type.
    let registry = Arc::new(registry);

    // --- identity -----------------------------------------------------------
    let node_id = opts.id.unwrap_or_else(NodeId::random);
    let name = if opts.name.is_empty() {
        format!("node-{}", node_id.short())
    } else {
        opts.name.clone()
    };

    let sealer = opts.sealer();
    let auth_on = sealer.is_some();
    let flood = if opts.flood_limit {
        FloodConfig {
            rate_per_sec: opts.flood_rate,
            burst: opts.flood_burst,
            ..FloodConfig::default()
        }
    } else {
        FloodConfig::unlimited()
    };

    let cfg = RouterConfig {
        node_id: Some(node_id),
        node_name: name.clone(),
        default_ttl: opts.ttl,
        announce_interval: Duration::from_millis(opts.announce_ms),
        route_timeout: Duration::from_millis(opts.route_timeout_ms),
        sealer,
        retry: RetryPolicy {
            max_attempts: opts.retry_max,
            base_interval: Duration::from_millis(opts.retry_ms),
            max_interval: Duration::from_millis(opts.retry_max_ms),
        },
        flood,
        ..Default::default()
    };

    // --- routing ------------------------------------------------------------
    let (delivery_tx, delivery_rx) = channel();
    let notices: NoticeSink = Arc::new(|n: RouterNotice| term::line(format!("  · {n}")));
    let (handle, router_thread) = start(cfg, delivery_tx, Arc::clone(&notices));

    // --- transport ----------------------------------------------------------
    let delivery_handle = handle.clone();
    std::thread::Builder::new()
        .name("mesh-deliver".into())
        .spawn(move || {
            let mut host = DataHost::new();
            host.register(Box::new(ChatApp::new(&delivery_handle)));
            for delivery in delivery_rx {
                if !host.dispatch(&delivery_handle, &delivery) {
                    term::line(format!(
                        "  ? no transport claims channel '{}' ({} bytes)",
                        delivery.channel,
                        delivery.payload.len()
                    ));
                }
            }
        })
        .expect("failed to spawn the delivery thread");

    // --- media: listen ------------------------------------------------------
    // `endpoints` is what we advertise: a listener bound to 0.0.0.0 is useless
    // to a peer, so the wildcard is replaced with the local address. Without
    // that, discovery would hand out an address nobody can dial.
    let mut endpoints: Vec<Endpoint> = Vec::new();
    for url in &opts.listen {
        match registry.listen(url) {
            Ok(acceptor) => {
                let bound = acceptor.local_addr();
                let transport = acceptor.transport();
                let endpoint = Endpoint::new(transport, retexis_discovery::advertised_addr(&bound));
                term::line(format!(
                    "  · listening {transport}://{bound}  (advertising {})",
                    endpoint.url()
                ));
                endpoints.push(endpoint);
                if let Err(e) = handle.accept_from(acceptor) {
                    eprintln!("! accept loop failed: {e}");
                }
            }
            Err(e) => {
                eprintln!("! cannot listen on {url}: {e}");
                std::process::exit(1);
            }
        }
    }

    // --- discovery: answer queries ------------------------------------------
    // Kept alive for the whole run; dropping it stops the responder thread.
    let announcement = Announcement::reply(node_id, name.clone(), endpoints.clone());
    let _responder = if opts.announce {
        if endpoints.is_empty() {
            term::line("  ! --announce needs at least one --listen, ignoring it");
            None
        } else {
            match discovery::respond(announcement.clone(), opts.discovery_port) {
                Ok(r) => {
                    term::line(format!(
                        "  · answering discovery on udp://{} as {}",
                        r.addr(),
                        endpoints
                            .iter()
                            .map(|e| e.url())
                            .collect::<Vec<_>>()
                            .join(" ")
                    ));
                    Some(r)
                }
                Err(e) => {
                    term::line(format!(
                        "  ! cannot answer discovery on port {}: {e}",
                        opts.discovery_port
                    ));
                    None
                }
            }
        }
    } else {
        None
    };

    let scanner = scan::Scanner::new(registry.beacons(), Duration::from_millis(opts.discover_ms));

    // --- media: dial --------------------------------------------------------
    let connector = Connector::new(
        Arc::clone(&registry),
        Arc::clone(&notices),
        ReconnectConfig {
            enabled: opts.reconnect,
            ..ReconnectConfig::default()
        },
    );
    for url in &opts.connect {
        if let Err(e) = connector.dial(&handle, url) {
            eprintln!("! {e}");
        }
    }

    // --- discovery: find access points --------------------------------------
    if opts.discover {
        let asked = scanner.broadcasters();
        if asked.is_empty() {
            term::line("  · discovery: no medium here can broadcast");
        } else {
            term::line(format!(
                "  · discovery: asking {} on port {} ({} ms)",
                asked.join(", "),
                opts.discovery_port,
                opts.discover_ms
            ));
            let found = scanner.scan();
            print_found(&found);
            if opts.discover_connect {
                for endpoint in scan::endpoints(&found) {
                    if let Err(e) = connector.dial(&handle, &endpoint.url()) {
                        eprintln!("! {e}");
                    }
                }
            }
        }
    }

    // Discovery counts as connecting: a node that found its peers is not an
    // island, even though it was never given an address.
    if opts.listen.is_empty() && connector.attempted() == 0 {
        eprintln!("! warning: no --listen and no reachable peer, this node is an island");
    }

    term::line(format!(
        "retexis {} — {name}/{}",
        env!("CARGO_PKG_VERSION"),
        node_id.short()
    ));
    term::line(format!(
        "  auth {}   flood limit {}",
        if auth_on {
            "on (pre-shared key)"
        } else {
            "OFF (plaintext, dev only)"
        },
        if opts.flood_limit {
            format!("{:.0}/s burst {:.0}", opts.flood_rate, opts.flood_burst)
        } else {
            "off".to_string()
        }
    ));
    term::line(format!(
        "  media {}   discovery {}",
        registry.schemes().join(", "),
        if scanner.broadcasters().is_empty() {
            "unavailable".to_string()
        } else {
            format!("udp broadcast :{}", opts.discovery_port)
        }
    ));
    term::line("  /help for commands, plain text broadcasts to the mesh");

    repl::run(&handle, &name, Some(&scanner), Some(&connector));

    handle.shutdown();
    let _ = router_thread.join_timeout(Duration::from_millis(300));
    std::process::exit(0);
}

fn print_found(found: &[Announcement]) {
    if found.is_empty() {
        term::line("  · discovery: nobody answered");
        return;
    }
    for a in found {
        term::line(format!("  · found {}", scan::describe(a)));
    }
}

/// Replaces a wildcard bind address with the address a peer could actually
/// reach. `0.0.0.0:7100` means "listen everywhere", not "connect here".
/// `JoinHandle::join` has no timeout; a stuck router must not hang exit.
trait JoinTimeout {
    fn join_timeout(self, d: Duration) -> std::io::Result<()>;
}

impl JoinTimeout for std::thread::JoinHandle<()> {
    fn join_timeout(self, d: Duration) -> std::io::Result<()> {
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let _ = self.join();
            let _ = tx.send(());
        });
        rx.recv_timeout(d)
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "join timed out"))
    }
}
