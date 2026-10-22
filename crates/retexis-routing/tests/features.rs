//! Integration tests for authentication, reliability, reconnection and flood
//! control.
//!
//! Most of these use `PipeLink`: an in-memory medium that drops every Nth
//! frame. That is the whole point of the `Link` abstraction — a deterministic,
//! instant, lossy network in 60 lines, no sockets, no sleeps, no flakiness.

use retexis_core::{
    next_link_id, ContentType, Link, LinkError, LinkId, MessageId, TransportRegistry,
};
use retexis_crypto::Sealer;
use retexis_link_tcp::TcpFactory;
use retexis_routing::{
    start, Delivery, Dialer, FloodConfig, MeshHandle, ReconnectConfig, RetryPolicy, RouterConfig,
};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{channel, Receiver, RecvTimeoutError, Sender};
use std::sync::Mutex;
use std::time::{Duration, Instant};

const CHAT: &str = "chat";

// ------------------------------------------------------------------ test medium

/// In-memory link. `drop_every = 3` drops the 1st, 4th, 7th... frame it sends.
struct PipeLink {
    id: LinkId,
    peer: String,
    out: Mutex<Option<Sender<Vec<u8>>>>,
    inp: Mutex<Receiver<Vec<u8>>>,
    sent: AtomicUsize,
    drop_every: usize,
    closed: AtomicBool,
}

impl Link for PipeLink {
    fn id(&self) -> LinkId {
        self.id
    }

    fn transport(&self) -> &'static str {
        "pipe"
    }

    fn peer(&self) -> String {
        self.peer.clone()
    }

    fn initiator(&self) -> bool {
        false
    }

    fn max_frame(&self) -> usize {
        1 << 20
    }

    fn send(&self, envelope: &[u8]) -> Result<(), LinkError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(LinkError::Closed);
        }
        let n = self.sent.fetch_add(1, Ordering::Relaxed);
        if self.drop_every > 0 && n % self.drop_every == 0 {
            return Ok(()); // swallowed by the medium
        }
        if let Some(tx) = self.out.lock().unwrap().as_ref() {
            tx.send(envelope.to_vec()).map_err(|_| LinkError::Closed)?;
        }
        Ok(())
    }

    fn recv(&self) -> Result<Vec<u8>, LinkError> {
        loop {
            if self.closed.load(Ordering::Acquire) {
                return Err(LinkError::Closed);
            }
            let rx = self.inp.lock().unwrap_or_else(|e| e.into_inner());
            match rx.recv_timeout(Duration::from_millis(20)) {
                Ok(bytes) => return Ok(bytes),
                Err(RecvTimeoutError::Timeout) => continue,
                Err(RecvTimeoutError::Disconnected) => return Err(LinkError::Closed),
            }
        }
    }

    fn shutdown(&self) {
        self.closed.store(true, Ordering::Release);
    }
}

fn pipe_pair(a_name: &str, b_name: &str, drop_every: usize) -> (Box<dyn Link>, Box<dyn Link>) {
    let (tx_a, rx_a) = channel::<Vec<u8>>();
    let (tx_b, rx_b) = channel::<Vec<u8>>();
    let a = PipeLink {
        id: next_link_id(),
        peer: b_name.to_string(),
        out: Mutex::new(Some(tx_a)),
        inp: Mutex::new(rx_b),
        sent: AtomicUsize::new(0),
        drop_every,
        closed: AtomicBool::new(false),
    };
    let b = PipeLink {
        id: next_link_id(),
        peer: a_name.to_string(),
        out: Mutex::new(Some(tx_b)),
        inp: Mutex::new(rx_a),
        sent: AtomicUsize::new(0),
        drop_every,
        closed: AtomicBool::new(false),
    };
    (Box::new(a), Box::new(b))
}

// ------------------------------------------------------------------ harness

fn node(name: &str, tweak: impl FnOnce(&mut RouterConfig)) -> (MeshHandle, Receiver<Delivery>) {
    let (tx, rx) = channel();
    let mut cfg = RouterConfig {
        node_name: name.to_string(),
        announce_interval: Duration::from_millis(100),
        route_timeout: Duration::from_millis(1500),
        ..Default::default()
    };
    tweak(&mut cfg);
    let (handle, _thread) = start(cfg, tx, retexis_routing::silent());
    (handle, rx)
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

fn key(pass: &str) -> Option<Sealer> {
    Some(Sealer::from_passphrase(pass))
}

// ------------------------------------------------------------------ auth

#[test]
fn auth_accepts_the_right_key_and_rejects_the_wrong_one() {
    let (alice, alice_rx) = node("alice", |c| c.sealer = key("mesh secret"));
    let (bob, _bob_rx) = node("bob", |c| c.sealer = key("mesh secret"));
    let (mallory, mallory_rx) = node("mallory", |c| c.sealer = key("guessed it"));

    let (la, lb) = pipe_pair("alice", "bob", 0);
    let (lb2, lm) = pipe_pair("bob", "mallory", 0);
    alice.add_link(la).unwrap();
    bob.add_link(lb).unwrap();
    bob.add_link(lb2).unwrap();
    mallory.add_link(lm).unwrap();

    assert!(
        wait_until(
            || alice.routes().iter().any(|r| r.dst == bob.id()),
            Duration::from_secs(5)
        ),
        "the two nodes sharing a key must converge"
    );

    // Give the intruder plenty of time to try.
    std::thread::sleep(Duration::from_millis(600));
    assert!(
        mallory.routes().is_empty(),
        "wrong key must not produce a route: {:?}",
        mallory
            .routes()
            .iter()
            .map(|r| r.dst.short())
            .collect::<Vec<_>>()
    );
    assert_eq!(
        mallory_rx.try_iter().count(),
        0,
        "wrong key must not receive traffic"
    );
    assert!(
        bob.status().stats.auth_rejected > 0,
        "bob should have counted the rejections"
    );

    let id = alice
        .send_reliable(bob.id(), CHAT, ContentType::Text, "secret")
        .unwrap();
    assert!(
        expect(&alice_rx, |d| d.msg_id == id, Duration::from_secs(1)).is_none(),
        "alice's own unicast is not echoed back to her"
    );
}

#[test]
fn a_plaintext_peer_is_refused_by_an_authenticated_mesh() {
    let (alice, _a_rx) = node("alice", |c| c.sealer = key("k"));
    let (bob, _b_rx) = node("bob", |c| c.sealer = key("k"));
    let (plain, _p_rx) = node("plain", |_| {});

    let (la, lb) = pipe_pair("alice", "bob", 0);
    let (lb2, lp) = pipe_pair("bob", "plain", 0);
    alice.add_link(la).unwrap();
    bob.add_link(lb).unwrap();
    bob.add_link(lb2).unwrap();
    plain.add_link(lp).unwrap();

    assert!(wait_until(
        || alice.routes().iter().any(|r| r.dst == bob.id()),
        Duration::from_secs(5)
    ));
    std::thread::sleep(Duration::from_millis(500));

    assert!(
        plain.routes().is_empty(),
        "an unsigned peer must not be admitted"
    );
    assert!(
        plain.status().stats.auth_rejected > 0,
        "the plaintext node cannot verify signed packets either"
    );
    assert!(bob.status().stats.auth_rejected > 0);
}

// ------------------------------------------------------------------ reliability

#[test]
fn reliable_unicast_survives_a_lossy_link() {
    // Every third frame on each direction is dropped, so a message plus its ack
    // cannot both get through on the first try.
    let (alice, alice_rx) = node("alice", |c| {
        c.retry = RetryPolicy {
            max_attempts: 8,
            base_interval: Duration::from_millis(60),
            max_interval: Duration::from_millis(300),
        };
    });
    let (bob, bob_rx) = node("bob", |_| {});

    let (la, lb) = pipe_pair("alice", "bob", 3);
    alice.add_link(la).unwrap();
    bob.add_link(lb).unwrap();

    assert!(
        wait_until(
            || alice.routes().iter().any(|r| r.dst == bob.id()),
            Duration::from_secs(5)
        ),
        "hello retries must complete the handshake over a lossy link"
    );

    let ids: Vec<_> = (0..5)
        .map(|i| {
            alice
                .send_reliable(bob.id(), CHAT, ContentType::Text, format!("msg {i}"))
                .unwrap()
        })
        .collect();

    // Drain everything first, then assert: deliveries can arrive out of order,
    // and a filtering `expect` would silently eat the ones we are not looking
    // at yet.
    let mut seen: std::collections::HashMap<MessageId, usize> = std::collections::HashMap::new();
    let deadline = Instant::now() + Duration::from_secs(6);
    while Instant::now() < deadline {
        match bob_rx.recv_timeout(Duration::from_millis(100)) {
            Ok(d) => {
                *seen.entry(d.msg_id).or_insert(0) += 1;
            }
            Err(RecvTimeoutError::Timeout) => {
                if seen.len() >= ids.len() {
                    break;
                }
            }
            Err(_) => break,
        }
    }
    for id in &ids {
        match seen.get(id) {
            Some(1) => {}
            Some(n) => panic!("message {} was delivered {n} times", id.short()),
            None => panic!("message {} never arrived", id.short()),
        }
    }

    assert!(
        wait_until(
            || alice.status().stats.acked >= ids.len() as u64,
            Duration::from_secs(3)
        ),
        "all acks should come back: {} of {}",
        alice.status().stats.acked,
        ids.len()
    );
    let stats = alice.status().stats;
    assert!(
        stats.retransmits > 0,
        "the lossy medium must have forced retransmissions"
    );
    assert_eq!(stats.delivery_failed, 0, "nothing should give up");
    assert!(
        bob.status().stats.duplicate_acks > 0,
        "lost acks should be recovered by re-acking the retry"
    );

    let _ = alice_rx;
}

#[test]
fn a_reliable_unicast_to_a_missing_node_fails_loudly() {
    let (alice, _rx) = node("alice", |c| {
        c.retry = RetryPolicy {
            max_attempts: 2,
            base_interval: Duration::from_millis(50),
            max_interval: Duration::from_millis(100),
        };
    });
    // Nobody is on the other end of this address.
    let ghost = retexis_core::NodeId::random();
    alice
        .send_reliable(ghost, CHAT, ContentType::Text, "into the void")
        .unwrap();

    assert!(
        wait_until(
            || alice.status().stats.delivery_failed == 1,
            Duration::from_secs(3)
        ),
        "an unacknowledged message must be reported as failed, not silently dropped"
    );
    assert_eq!(
        alice.status().pending,
        0,
        "the pending queue must be cleaned up"
    );
}

/// A relay must re-forward a retried unicast instead of eating it as a
/// duplicate. Without this, a DM that crosses a node whose next hop was briefly
/// down can never be delivered, no matter how many times it is retried.
#[test]
fn a_relay_reforwards_a_retried_unicast() {
    let (alice, alice_rx) = node("alice", |_| {});
    let (bob, _bob_rx) = node("bob", |_| {});
    let (carol, _carol_rx) = node("carol", |c| {
        c.retry = RetryPolicy {
            max_attempts: 12,
            base_interval: Duration::from_millis(60),
            max_interval: Duration::from_millis(400),
        };
    });

    let (la, lb) = pipe_pair("alice", "bob", 0);
    let (lb2, lc) = pipe_pair("bob", "carol", 0);
    alice.add_link(la).unwrap();
    bob.add_link(lb).unwrap();
    bob.add_link(lb2).unwrap();
    carol.add_link(lc).unwrap();

    assert!(
        wait_until(
            || carol.routes().iter().any(|r| r.dst == alice.id()),
            Duration::from_secs(5)
        ),
        "the line must converge first"
    );

    // Break bob<->alice. Carol can still reach bob, but bob can route nowhere.
    let on_bob = bob
        .links()
        .into_iter()
        .find(|l| l.node == Some(alice.id()))
        .expect("bob's link to alice")
        .link_id;
    let on_alice = alice
        .links()
        .into_iter()
        .find(|l| l.node == Some(bob.id()))
        .expect("alice's link to bob")
        .link_id;
    bob.drop_link(on_bob).unwrap();
    alice.drop_link(on_alice).unwrap();

    // Carol sends while the last hop is down: bob receives it, forwards it
    // nowhere, and remembers the id.
    let id = carol
        .send_reliable(
            alice.id(),
            CHAT,
            ContentType::Text,
            "through a broken relay",
        )
        .unwrap();
    std::thread::sleep(Duration::from_millis(250));

    // Heal. Carol's retries now hit bob's dedup cache.
    let (lb3, la2) = pipe_pair("bob", "alice", 0);
    bob.add_link(lb3).unwrap();
    alice.add_link(la2).unwrap();

    assert!(
        expect(&alice_rx, |d| d.msg_id == id, Duration::from_secs(6)).is_some(),
        "the retry was swallowed by the relay's dedup cache"
    );
    assert_eq!(
        count(&alice_rx, |d| d.msg_id == id, Duration::from_millis(150)),
        0,
        "and it must still be delivered only once"
    );
    assert!(
        bob.status().stats.dropped_duplicate > 0,
        "bob should have seen the retry as a duplicate and forwarded it anyway"
    );
}

// ------------------------------------------------------------------ reconnect

#[test]
fn the_dialer_reconnects_after_the_link_is_dropped() {
    let mut registry = TransportRegistry::new();
    registry.register(std::sync::Arc::new(TcpFactory::new()));

    let (listener, _l_rx) = node("listener", |_| {});
    let acceptor = registry.listen("tcp://127.0.0.1:0").expect("bind");
    let addr = acceptor.local_addr();
    listener.accept_from(acceptor).expect("accept loop");

    let (dialer_node, _d_rx) = node("dialer", |_| {});
    let dialer = Dialer::new(
        std::sync::Arc::new(TcpFactory::new()),
        &format!("tcp://{addr}"),
    )
    .expect("dialer")
    .with_config(ReconnectConfig {
        enabled: true,
        base: Duration::from_millis(50),
        max: Duration::from_millis(200),
        probe: Duration::from_millis(50),
    });
    dialer.spawn(dialer_node.clone(), retexis_routing::silent());

    assert!(
        wait_until(|| !listener.links().is_empty(), Duration::from_secs(5)),
        "the dialer never connected"
    );
    let first_link = listener.links()[0].link_id;

    // Sever it from the listening side; the dialer must notice and redial.
    listener.drop_link(first_link).unwrap();

    assert!(
        wait_until(
            || {
                let links = listener.links();
                links.len() == 1 && links[0].link_id != first_link
            },
            Duration::from_secs(5)
        ),
        "the dialer did not re-establish the link"
    );

    // And the mesh works again.
    let id = dialer_node
        .send_reliable(listener.id(), CHAT, ContentType::Text, "back online")
        .unwrap();
    assert!(
        wait_until(
            || dialer_node.status().stats.acked > 0,
            Duration::from_secs(3)
        ),
        "traffic must flow over the new link ({})",
        id.short()
    );
}

#[test]
fn two_nodes_dialling_each_other_end_up_with_one_link() {
    let mut registry = TransportRegistry::new();
    registry.register(std::sync::Arc::new(TcpFactory::new()));

    let (a, _a_rx) = node("a", |_| {});
    let (b, _b_rx) = node("b", |_| {});
    let acc_a = registry.listen("tcp://127.0.0.1:0").expect("bind a");
    let acc_b = registry.listen("tcp://127.0.0.1:0").expect("bind b");
    let addr_a = acc_a.local_addr();
    let addr_b = acc_b.local_addr();
    a.accept_from(acc_a).unwrap();
    b.accept_from(acc_b).unwrap();

    // Both sides dial each other: four links get established, two must die.
    for _ in 0..2 {
        a.add_link(registry.connect(&format!("tcp://{addr_b}")).unwrap())
            .unwrap();
        b.add_link(registry.connect(&format!("tcp://{addr_a}")).unwrap())
            .unwrap();
    }

    assert!(
        wait_until(
            || a.links().len() == 1 && b.links().len() == 1,
            Duration::from_secs(5)
        ),
        "duplicate links must collapse to one per side: a={} b={}",
        a.links().len(),
        b.links().len()
    );
    assert!(
        a.status().stats.duplicate_links > 0,
        "the collapse should be counted"
    );

    // Both ends agree on the survivor: it must stay stable, not flap.
    let survivor = a.links()[0].link_id;
    std::thread::sleep(Duration::from_millis(600));
    assert_eq!(
        a.links()[0].link_id,
        survivor,
        "the surviving link must not oscillate"
    );
    assert_eq!(a.links().len(), 1);
}

// ------------------------------------------------------------------ flood control

#[test]
fn the_flood_limiter_drops_relays_but_not_our_own_messages() {
    // bob is a relay with an almost non-existent flood budget.
    let (alice, alice_rx) = node("alice", |_| {});
    let (bob, _bob_rx) = node("bob", |c| {
        c.flood = FloodConfig {
            rate_per_sec: 1.0,
            burst: 1.0,
            per_source_rate: 1.0,
            per_source_burst: 1.0,
            ..FloodConfig::default()
        }
    });
    let (carol, carol_rx) = node("carol", |_| {});

    let (la, lb) = pipe_pair("alice", "bob", 0);
    let (lb2, lc) = pipe_pair("bob", "carol", 0);
    alice.add_link(la).unwrap();
    bob.add_link(lb).unwrap();
    bob.add_link(lb2).unwrap();
    carol.add_link(lc).unwrap();

    assert!(
        wait_until(
            || carol.routes().iter().any(|r| r.dst == alice.id()),
            Duration::from_secs(5)
        ),
        "the three-node line must converge before the storm"
    );

    let ids: Vec<_> = (0..10)
        .map(|i| {
            alice
                .broadcast(CHAT, ContentType::Text, format!("storm {i}"))
                .unwrap()
        })
        .collect();

    // Alice always sees her own broadcasts: local echo is never throttled.
    let own = count(
        &alice_rx,
        |d| ids.contains(&d.msg_id),
        Duration::from_millis(800),
    );
    assert_eq!(own, 10, "our own broadcasts must all be delivered locally");

    // Carol is behind the throttled relay.
    let through = count(
        &carol_rx,
        |d| ids.contains(&d.msg_id),
        Duration::from_millis(400),
    );
    assert!(
        through < 10,
        "the limiter should have suppressed some relays, carol saw {through}/10"
    );
    assert!(
        through > 0,
        "the limiter must not be a total blackout, carol saw {through}/10"
    );

    let suppressed = bob.status().stats.flood_suppressed;
    assert!(
        suppressed >= 5,
        "suppression must be counted, got {suppressed}"
    );
}
