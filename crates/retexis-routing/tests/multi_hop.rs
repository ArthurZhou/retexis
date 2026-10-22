//! End-to-end mesh tests over real TCP sockets.
//!
//! These are the tests that actually prove the design: several routers, real
//! links, multi-hop forwarding, flooding with duplicate suppression.

use retexis_core::{ContentType, TransportRegistry};
use retexis_link_tcp::TcpFactory;
use retexis_routing::{start, Delivery, MeshHandle, RouterConfig};
use std::sync::mpsc::{channel, Receiver};
use std::time::{Duration, Instant};

const CHAT: &str = "chat";

fn registry() -> TransportRegistry {
    let mut reg = TransportRegistry::new();
    reg.register(std::sync::Arc::new(TcpFactory::new()));
    reg
}

fn node_with(name: &str, ttl: u8, announce_ms: u64) -> (MeshHandle, Receiver<Delivery>) {
    let (tx, rx) = channel();
    let cfg = RouterConfig {
        node_name: name.to_string(),
        default_ttl: ttl,
        announce_interval: Duration::from_millis(announce_ms),
        route_timeout: Duration::from_millis(announce_ms * 10 + 500),
        ..Default::default()
    };
    let (handle, _router_thread) = start(cfg, tx, retexis_routing::silent());
    (handle, rx)
}

fn node(name: &str) -> (MeshHandle, Receiver<Delivery>) {
    node_with(name, 8, 100)
}

/// Binds an ephemeral port and starts the accept loop; returns the address.
fn listen(handle: &MeshHandle) -> String {
    let acceptor = registry().listen("tcp://127.0.0.1:0").expect("bind");
    let addr = acceptor.local_addr();
    handle.accept_from(acceptor).expect("accept loop");
    addr
}

fn dial(handle: &MeshHandle, addr: &str) {
    let link = registry()
        .connect(&format!("tcp://{addr}"))
        .expect("connect");
    handle.add_link(link).expect("attach");
}

fn wait_until(mut cond: impl FnMut() -> bool, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if cond() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    cond()
}

/// Waits for the first delivery matching `pred`, skipping anything else.
fn expect(
    rx: &Receiver<Delivery>,
    pred: impl Fn(&Delivery) -> bool,
    timeout: Duration,
) -> Option<Delivery> {
    let deadline = Instant::now() + timeout;
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return None;
        }
        match rx.recv_timeout(left) {
            Ok(d) if pred(&d) => return Some(d),
            Ok(_) => continue,
            Err(_) => return None,
        }
    }
}

/// Counts deliveries matching `pred` until `window` elapses.
fn count(rx: &Receiver<Delivery>, pred: impl Fn(&Delivery) -> bool, window: Duration) -> usize {
    let deadline = Instant::now() + window;
    let mut n = 0;
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return n;
        }
        match rx.recv_timeout(left) {
            Ok(d) => {
                if pred(&d) {
                    n += 1
                }
            }
            Err(_) => return n,
        }
    }
}

/// alice <- bob <- carol: carol must reach alice through bob.
#[test]
fn line_topology_routes_two_hops() {
    let (alice, alice_rx) = node("alice");
    let (bob, _bob_rx) = node("bob");
    let (carol, carol_rx) = node("carol");

    let alice_addr = listen(&alice);
    let bob_addr = listen(&bob);
    dial(&bob, &alice_addr);
    dial(&carol, &bob_addr);

    assert!(
        wait_until(
            || carol.routes().iter().any(|r| r.dst == alice.id()),
            Duration::from_secs(5)
        ),
        "carol never learned a route to alice"
    );
    let route = carol
        .routes()
        .into_iter()
        .find(|r| r.dst == alice.id())
        .unwrap();
    assert_eq!(route.metric, 2, "alice is two hops from carol");
    assert!(
        carol.routes().iter().all(|r| r.dst != carol.id()),
        "a node must not route to itself"
    );
    // Names ride along with route advertisements, so they converge too.
    assert!(
        wait_until(
            || carol
                .status()
                .names
                .get(&alice.id())
                .map(|n| n == "alice")
                .unwrap_or(false),
            Duration::from_secs(5)
        ),
        "carol never learned alice's name: {:?}",
        carol.status().names
    );

    // --- broadcast travels the whole line --------------------------------
    let id = alice
        .broadcast(CHAT, ContentType::Text, "hello mesh")
        .unwrap();
    let at_carol = expect(&carol_rx, |d| d.msg_id == id, Duration::from_secs(3))
        .expect("carol never received the broadcast");
    assert_eq!(at_carol.from, alice.id());
    assert_eq!(at_carol.from_name.as_deref(), Some("alice"));
    assert_eq!(at_carol.hops, 2, "broadcast crossed two hops");
    assert_eq!(at_carol.payload, b"hello mesh".to_vec());

    // --- unicast the other way -------------------------------------------
    let id = carol
        .send_to(alice.id(), CHAT, ContentType::Text, "hi alice")
        .unwrap();
    let at_alice = expect(&alice_rx, |d| d.msg_id == id, Duration::from_secs(3))
        .expect("alice never received the unicast");
    assert_eq!(at_alice.from, carol.id());
    assert_eq!(at_alice.hops, 2);
    assert!(!at_alice.broadcast);
}

/// A triangle must deliver a broadcast exactly once per node: that is what
/// the duplicate cache buys us.
#[test]
fn triangle_delivers_each_broadcast_once() {
    let (a, _a_rx) = node("a");
    let (b, b_rx) = node("b");
    let (c, c_rx) = node("c");

    let a_addr = listen(&a);
    let b_addr = listen(&b);
    let c_addr = listen(&c);
    dial(&b, &a_addr);
    dial(&c, &b_addr);
    dial(&a, &c_addr); // closes the loop

    assert!(
        wait_until(
            || { a.routes().len() == 2 && b.routes().len() == 2 && c.routes().len() == 2 },
            Duration::from_secs(5)
        ),
        "the triangle did not converge: a={:?} b={:?} c={:?}",
        a.routes().len(),
        b.routes().len(),
        c.routes().len()
    );

    let id = a.broadcast(CHAT, ContentType::Text, "loop test").unwrap();
    let hits_b = count(&b_rx, |d| d.msg_id == id, Duration::from_millis(800));
    let hits_c = count(&c_rx, |d| d.msg_id == id, Duration::from_millis(800));
    assert_eq!(hits_b, 1, "b should see the flood exactly once");
    assert_eq!(hits_c, 1, "c should see the flood exactly once");
}

/// TTL 1 means "neighbours only": a two-hop node must never see it.
#[test]
fn ttl_limits_the_blast_radius() {
    let (alice, _alice_rx) = node_with("alice", 1, 100);
    let (bob, bob_rx) = node("bob");
    let (carol, carol_rx) = node("carol");

    let alice_addr = listen(&alice);
    let bob_addr = listen(&bob);
    dial(&bob, &alice_addr);
    dial(&carol, &bob_addr);

    assert!(
        wait_until(
            || carol.routes().iter().any(|r| r.dst == alice.id()),
            Duration::from_secs(5)
        ),
        "topology did not converge"
    );

    let id = alice
        .broadcast(CHAT, ContentType::Text, "one hop only")
        .unwrap();
    let at_bob = expect(&bob_rx, |d| d.msg_id == id, Duration::from_secs(2));
    assert!(at_bob.is_some(), "bob is a neighbour and must receive it");
    assert_eq!(at_bob.unwrap().hops, 1);

    let at_carol = expect(&carol_rx, |d| d.msg_id == id, Duration::from_millis(600));
    assert!(at_carol.is_none(), "ttl 1 must not cross bob");
}

/// Losing a link must invalidate the routes that used it, and a fresh
/// advertisement must repair the mesh through the other side.
#[test]
fn link_loss_invalidates_routes() {
    let (a, _a_rx) = node("a");
    let (b, _b_rx) = node("b");
    let (c, _c_rx) = node("c");

    let a_addr = listen(&a);
    let b_addr = listen(&b);
    dial(&b, &a_addr);
    dial(&c, &b_addr);

    assert!(
        wait_until(
            || c.routes().iter().any(|r| r.dst == a.id()),
            Duration::from_secs(5)
        ),
        "c never learned a route to a"
    );

    // Shut b down: both sides must forget what they learned through it.
    b.shutdown();

    assert!(
        wait_until(
            || c.routes().iter().all(|r| r.dst != a.id()),
            Duration::from_secs(5)
        ),
        "c kept a stale route to a after b left: {:?}",
        c.routes().iter().map(|r| r.dst.short()).collect::<Vec<_>>()
    );
}
