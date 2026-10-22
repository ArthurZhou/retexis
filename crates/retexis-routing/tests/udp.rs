//! The routing layer over UDP.
//!
//! These tests exist to check a claim, not a feature: **nothing in
//! `retexis-routing` changed to support UDP.** The same `start()`, the same
//! distance vector, the same flood/dedup/ack code runs over a datagram medium.
//! If that were not true, this file would need special cases — it has none.
//!
//! The interesting difference from TCP is the demux: one bound socket serves
//! several neighbours, so the middle node here is a real test of it.

use retexis_core::{ContentType, TransportRegistry};
use retexis_link_udp::UdpFactory;
use retexis_routing::{start, Delivery, MeshHandle, RouterConfig};
use std::sync::mpsc::{channel, Receiver};
use std::sync::Arc;
use std::time::{Duration, Instant};

const CHAT: &str = "chat";

fn node(name: &str) -> (MeshHandle, Receiver<Delivery>) {
    let (tx, rx) = channel();
    let cfg = RouterConfig {
        node_name: name.to_string(),
        announce_interval: Duration::from_millis(150),
        route_timeout: Duration::from_millis(1500),
        ..Default::default()
    };
    let (handle, _thread) = start(cfg, tx, retexis_routing::silent());
    (handle, rx)
}

fn registry() -> TransportRegistry {
    let mut reg = TransportRegistry::new();
    reg.register(Arc::new(UdpFactory::new()));
    reg
}

fn wait_until(mut pred: impl FnMut() -> bool, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if pred() {
            return true;
        }
        if Instant::now() >= deadline {
            return pred();
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn metric(handle: &MeshHandle, dst: retexis_core::NodeId) -> Option<u8> {
    handle
        .routes()
        .iter()
        .find(|r| r.dst == dst)
        .map(|r| r.metric)
}

#[test]
fn two_nodes_converge_over_a_datagram_medium() {
    let reg = registry();
    let (a, _a_rx) = node("a");
    let (b, _b_rx) = node("b");

    let acceptor = reg.listen("udp://127.0.0.1:0").expect("listen");
    let addr = acceptor.local_addr();
    a.accept_from(acceptor).expect("accept loop");
    b.add_link(reg.connect(&format!("udp://{addr}")).expect("connect"))
        .expect("add link");

    assert!(
        wait_until(
            || metric(&a, b.id()) == Some(1) && metric(&b, a.id()) == Some(1),
            Duration::from_secs(5)
        ),
        "a and b should see each other: a={:?} b={:?}",
        a.routes().len(),
        b.routes().len()
    );
    assert_eq!(a.links()[0].transport, "udp");
    assert_eq!(b.links()[0].transport, "udp");
}

#[test]
fn one_bound_socket_serves_two_neighbours() {
    // a — b — c, with b listening on a single UDP port. Both a and c are peers
    // of b, which only works if the demux keeps their queues apart.
    let reg = registry();
    let (a, _a_rx) = node("a");
    let (b, _b_rx) = node("b");
    let (c, _c_rx) = node("c");

    let acceptor = reg.listen("udp://127.0.0.1:0").expect("listen");
    let b_addr = acceptor.local_addr();
    b.accept_from(acceptor).expect("accept loop");

    a.add_link(reg.connect(&format!("udp://{b_addr}")).expect("a->b"))
        .expect("add a");
    c.add_link(reg.connect(&format!("udp://{b_addr}")).expect("c->b"))
        .expect("add c");

    assert!(
        wait_until(
            || metric(&c, a.id()) == Some(2) && metric(&a, c.id()) == Some(2),
            Duration::from_secs(6)
        ),
        "a and c should be two hops apart through b: a={:?} c={:?}",
        a.routes().iter().map(|r| r.metric).collect::<Vec<_>>(),
        c.routes().iter().map(|r| r.metric).collect::<Vec<_>>()
    );
    assert_eq!(b.links().len(), 2, "b keeps one link per neighbour");
}

#[test]
fn a_broadcast_crosses_two_datagram_hops() {
    let reg = registry();
    let (a, a_rx) = node("a");
    let (b, _b_rx) = node("b");
    let (c, c_rx) = node("c");

    let acceptor = reg.listen("udp://127.0.0.1:0").expect("listen");
    let b_addr = acceptor.local_addr();
    b.accept_from(acceptor).expect("accept loop");
    a.add_link(reg.connect(&format!("udp://{b_addr}")).expect("a->b"))
        .expect("add a");
    c.add_link(reg.connect(&format!("udp://{b_addr}")).expect("c->b"))
        .expect("add c");
    assert!(wait_until(
        || metric(&c, a.id()) == Some(2),
        Duration::from_secs(6)
    ));

    a.broadcast(CHAT, ContentType::Text, "hello over udp")
        .unwrap();
    let got = c_rx.recv_timeout(Duration::from_secs(4));
    let delivery = got.expect("c should receive the broadcast");
    assert_eq!(delivery.payload, b"hello over udp");
    assert!(delivery.broadcast);
    assert_eq!(delivery.hops, 2);
    // a sees its own broadcast with zero hops.
    let own = a_rx.recv_timeout(Duration::from_secs(2)).expect("own copy");
    assert_eq!(own.hops, 0);
}

#[test]
fn a_reliable_unicast_is_acked_over_udp() {
    let reg = registry();
    let (a, _a_rx) = node("a");
    let (b, _b_rx) = node("b");
    let (c, c_rx) = node("c");

    let acceptor = reg.listen("udp://127.0.0.1:0").expect("listen");
    let b_addr = acceptor.local_addr();
    b.accept_from(acceptor).expect("accept loop");
    a.add_link(reg.connect(&format!("udp://{b_addr}")).expect("a->b"))
        .expect("add a");
    c.add_link(reg.connect(&format!("udp://{b_addr}")).expect("c->b"))
        .expect("add c");
    assert!(wait_until(
        || metric(&a, c.id()) == Some(2),
        Duration::from_secs(6)
    ));

    a.send_reliable(c.id(), CHAT, ContentType::Text, "dm over udp")
        .unwrap();
    let delivery = c_rx
        .recv_timeout(Duration::from_secs(4))
        .expect("the dm should arrive");
    assert_eq!(delivery.payload, b"dm over udp");
    assert!(
        wait_until(|| a.status().stats.acked == 1, Duration::from_secs(4)),
        "the ack should come back over the same datagram path"
    );
}

#[test]
fn udp_links_report_the_medium_by_name() {
    let reg = registry();
    let (a, _a_rx) = node("a");
    let (b, _b_rx) = node("b");
    let acceptor = reg.listen("udp://127.0.0.1:0").expect("listen");
    let addr = acceptor.local_addr();
    a.accept_from(acceptor).expect("accept loop");
    b.add_link(reg.connect(&format!("udp://{addr}")).expect("connect"))
        .expect("add link");

    assert!(wait_until(
        || !a.links().is_empty() && !b.links().is_empty(),
        Duration::from_secs(3)
    ));
    let inbound = &a.links()[0];
    let outbound = &b.links()[0];
    assert_eq!(inbound.transport, "udp");
    assert!(!inbound.initiator, "a accepted");
    assert!(outbound.initiator, "b dialled");
    assert!(
        inbound.max_frame >= 60 * 1024,
        "a full envelope fits one datagram"
    );
}
