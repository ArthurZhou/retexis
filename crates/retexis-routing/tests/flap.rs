//! A link that dies immediately must not be retried every 300 ms forever.
//!
//! This is the failure mode the GUI hits first: two windows on one host, the
//! second one dials the first one's port — which happens to be its own — so
//! the router rejects the self-link at the handshake and the dialer sees
//! "connected, then closed" within a few milliseconds.
//!
//! Resetting the backoff on every successful `connect()` turns that into a
//! permanent 300 ms reconnect storm: hundreds of log lines, hundreds of
//! handshakes, no progress. The fix is to only treat a connection as a success
//! if it *stayed* up.

use retexis_core::{LinkAcceptor, LinkFactory};
use retexis_link_tcp::{TcpAcceptor, TcpConfig, TcpFactory};
use retexis_routing::{Dialer, NoticeSink, ReconnectConfig, RouterConfig, RouterNotice};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[test]
fn a_flapping_link_backs_off_instead_of_hammering() {
    // A listener that accepts and immediately hangs up: every dial succeeds
    // and every link dies at once.
    let acceptor = TcpAcceptor::bind("127.0.0.1:0", TcpConfig::default()).expect("bind");
    let addr = acceptor.local_addr();
    std::thread::spawn(move || {
        while let Ok(link) = acceptor.accept() {
            link.shutdown();
        }
    });

    let attempts = Arc::new(Mutex::new(Vec::new()));
    let sink: NoticeSink = {
        let attempts = Arc::clone(&attempts);
        Arc::new(move |n: RouterNotice| {
            if let RouterNotice::Reconnecting { attempt, .. } = n {
                attempts.lock().unwrap().push(attempt);
            }
        })
    };

    let (tx, _deliveries) = std::sync::mpsc::channel();
    let (handle, _router) = retexis_routing::start(
        RouterConfig {
            node_name: "flap".into(),
            ..Default::default()
        },
        tx,
        Arc::clone(&sink),
    );

    let factory: Arc<dyn LinkFactory> = Arc::new(TcpFactory::new());
    Dialer::new(factory, &format!("tcp://{addr}"))
        .expect("dialer")
        .with_config(ReconnectConfig {
            enabled: true,
            base: Duration::from_millis(40),
            max: Duration::from_millis(400),
            probe: Duration::from_millis(10),
        })
        .spawn(handle.clone(), sink);

    std::thread::sleep(Duration::from_millis(1500));
    handle.shutdown();

    let seen = attempts.lock().unwrap().clone();
    assert!(seen.len() >= 3, "expected a few retries, saw {seen:?}");
    assert!(
        *seen.iter().max().expect("at least one retry") >= 3,
        "the delay must escalate when the link never stays up, saw {seen:?}"
    );
}
