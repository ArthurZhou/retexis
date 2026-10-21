//! `retexis` — a node plus a chat REPL.
//!
//! Wiring is deliberately boring: build a registry of media, boot a router,
//! attach listeners, supervise dialers, pump deliveries into the transport
//! host. Everything interesting happens in the crates below.

mod cli;
mod repl;

use cli::Options;
use retexis_app::{term, ChatTransport, TransportHost};
use retexis_core::{NodeId, TransportRegistry};
use retexis_link_tcp::TcpFactory;
use retexis_routing::dial::ReconnectConfig;
use retexis_routing::start;
use retexis_routing::{Dialer, FloodConfig, NoticeSink, RetryPolicy, RouterConfig, RouterNotice};
use std::sync::mpsc::channel;
use std::sync::Arc;
use std::time::Duration;

fn main() {
    let opts = Options::parse(std::env::args().skip(1));

    // --- media registry -----------------------------------------------------
    // One line per medium. Adding WebSocket/HTTP/radio support later is one
    // `register` call plus the crate that implements it.
    let mut registry = TransportRegistry::new();
    registry.register(Box::new(TcpFactory::new()));

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
            let mut host = TransportHost::new();
            host.register(Box::new(ChatTransport::new(&delivery_handle)));
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

    // --- media --------------------------------------------------------------
    for url in &opts.listen {
        match registry.listen(url) {
            Ok(acceptor) => {
                let bound = acceptor.local_addr();
                let transport = acceptor.transport();
                term::line(format!("  · listening {transport}://{bound}"));
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

    let reconnect = ReconnectConfig {
        enabled: opts.reconnect,
        ..ReconnectConfig::default()
    };
    for url in &opts.connect {
        match Dialer::new(Box::new(TcpFactory::new()), url) {
            Ok(dialer) => {
                term::line(format!(
                    "  · dialling {url}{}",
                    if opts.reconnect { " (supervised)" } else { "" }
                ));
                dialer
                    .with_config(reconnect)
                    .spawn(handle.clone(), Arc::clone(&notices));
            }
            Err(e) => eprintln!("! bad --connect url {url}: {e}"),
        }
    }

    if opts.listen.is_empty() && opts.connect.is_empty() {
        eprintln!("! warning: no --listen and no --connect, this node is an island");
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
    term::line("  /help for commands, plain text broadcasts to the mesh");

    repl::run(&handle, &name);

    handle.shutdown();
    let _ = router_thread.join_timeout(Duration::from_millis(300));
    std::process::exit(0);
}

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
