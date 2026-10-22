//! Integration tests: real routers, connected by the virtual switch.
//!
//! Nothing here is mocked at the protocol level. Each node is a
//! `retexis_routing::Router` with its own thread, and these tests drive it
//! through the same `Hub` the web UI drives — so a green run means the
//! simulator is exercising the production code path, not a stand-in.

use retexis_core::NodeId;
use retexis_sim::json::Json;
use retexis_sim::{LinkKey, LinkParams, Sim};
use std::sync::Arc;
use std::thread::sleep;
use std::time::{Duration, Instant};

fn sim_with(preset: &str, n: usize) -> Arc<Sim> {
    let sim = Sim::new(1234);
    sim.reset(preset, n);
    sim.hub().set_running(true);
    sim
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
        sleep(Duration::from_millis(20));
    }
}

fn metric(sim: &Sim, from: NodeId, to: NodeId) -> Option<u8> {
    sim.status(from)?
        .routes
        .iter()
        .find(|r| r.dst == to)
        .map(|r| r.metric)
}

fn converged(sim: &Sim, ids: &[NodeId]) -> bool {
    ids.iter().all(|a| {
        ids.iter()
            .filter(|b| *b != a)
            .all(|b| metric(sim, *a, *b).is_some())
    })
}

// --------------------------------------------------------------- convergence

#[test]
fn a_line_converges_and_knows_the_distance() {
    let sim = sim_with("line", 3);
    let ids = sim.node_ids();
    assert!(
        wait_until(
            || metric(&sim, ids[2], ids[0]) == Some(2) && metric(&sim, ids[0], ids[2]) == Some(2),
            Duration::from_secs(8)
        ),
        "both ends should see 2 hops, saw {:?} / {:?}",
        metric(&sim, ids[2], ids[0]),
        metric(&sim, ids[0], ids[2])
    );
    assert_eq!(metric(&sim, ids[2], ids[1]), Some(1), "bob is adjacent");
}

#[test]
fn a_ring_converges_every_node_to_every_other() {
    let sim = sim_with("ring", 5);
    let ids = sim.node_ids();
    assert!(
        wait_until(|| converged(&sim, &ids), Duration::from_secs(10)),
        "ring of 5 did not converge: {:?}",
        ids.iter()
            .map(|id| sim.status(*id).map(|s| s.routes.len()).unwrap_or(0))
            .collect::<Vec<_>>()
    );
    // In a ring of 5 the worst case is 2 hops (the long way round is 3).
    for a in &ids {
        for b in &ids {
            if a != b {
                let m = metric(&sim, *a, *b).expect("route");
                assert!(m <= 2, "{m} hops in a 5-ring is wrong");
            }
        }
    }
}

// -------------------------------------------------------------------- data

#[test]
fn a_broadcast_reaches_every_node_exactly_once() {
    let sim = sim_with("ring", 4);
    let ids = sim.node_ids();
    assert!(wait_until(
        || converged(&sim, &ids),
        Duration::from_secs(10)
    ));

    sim.send(ids[0], None, "hello mesh", false).unwrap();
    assert!(
        wait_until(
            || ids.iter().all(|id| !sim.inbox(*id).is_empty()),
            Duration::from_secs(5)
        ),
        "the broadcast did not reach everyone"
    );

    // Give the duplicate paths in the ring a chance to arrive twice.
    sleep(Duration::from_millis(400));
    for id in &ids {
        let inbox = sim.inbox(*id);
        assert_eq!(
            inbox.len(),
            1,
            "{} received {} copies: {:?}",
            sim.name_of(*id).unwrap(),
            inbox.len(),
            inbox.iter().map(|e| e.text.clone()).collect::<Vec<_>>()
        );
        assert_eq!(inbox[0].text, "hello mesh");
    }
    // The sender sees its own broadcast with zero hops.
    let own = &sim.inbox(ids[0])[0];
    assert!(own.broadcast);
    assert_eq!(own.hops, 0);
}

#[test]
fn a_reliable_unicast_crosses_two_hops_and_is_acked() {
    let sim = sim_with("line", 3);
    let ids = sim.node_ids();
    assert!(wait_until(
        || metric(&sim, ids[2], ids[0]) == Some(2),
        Duration::from_secs(8)
    ));

    sim.send(ids[0], Some(ids[2]), "dm over two hops", true)
        .unwrap();
    assert!(
        wait_until(
            || !sim.inbox(ids[2]).is_empty() && sim.status(ids[0]).unwrap().stats.acked == 1,
            Duration::from_secs(5)
        ),
        "the ack never came back"
    );
    let inbox = sim.inbox(ids[2]);
    assert_eq!(inbox.len(), 1);
    assert_eq!(inbox[0].text, "dm over two hops");
    assert_eq!(inbox[0].from, "alice");
    assert_eq!(inbox[0].hops, 2);
    assert!(!inbox[0].broadcast);
}

// ------------------------------------------------------------------ cables

#[test]
fn cutting_a_cable_removes_the_routes_that_used_it() {
    let sim = sim_with("line", 4);
    let ids = sim.node_ids();
    assert!(wait_until(
        || metric(&sim, ids[0], ids[3]) == Some(3),
        Duration::from_secs(10)
    ));

    let key = LinkKey::new(ids[1], ids[2]);
    sim.update_link(key, &Json::obj([("up", Json::Bool(false))]))
        .unwrap();
    assert!(
        wait_until(
            || metric(&sim, ids[0], ids[3]).is_none() && metric(&sim, ids[3], ids[0]).is_none(),
            Duration::from_secs(5)
        ),
        "a partition must remove the routes across it"
    );
    // The halves still work.
    assert_eq!(metric(&sim, ids[0], ids[1]), Some(1));
    assert_eq!(metric(&sim, ids[3], ids[2]), Some(1));

    sim.update_link(key, &Json::obj([("up", Json::Bool(true))]))
        .unwrap();
    assert!(
        wait_until(
            || metric(&sim, ids[0], ids[3]) == Some(3),
            Duration::from_secs(8)
        ),
        "healing a cable must restore the route"
    );
}

#[test]
fn latency_is_applied_per_frame() {
    let sim = sim_with("line", 2);
    let ids = sim.node_ids();
    assert!(wait_until(
        || metric(&sim, ids[0], ids[1]).is_some(),
        Duration::from_secs(8)
    ));

    let key = LinkKey::new(ids[0], ids[1]);
    sim.update_link(
        key,
        &Json::obj([("latency", Json::Num(0.15)), ("loss", Json::Num(0.0))]),
    )
    .unwrap();

    let start = Instant::now();
    sim.send(ids[0], Some(ids[1]), "slow hello", true).unwrap();
    assert!(wait_until(
        || !sim.inbox(ids[1]).is_empty(),
        Duration::from_secs(3)
    ));
    let elapsed = start.elapsed();
    assert!(
        elapsed >= Duration::from_millis(120),
        "150ms of latency arrived in {elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_secs(2),
        "took far too long: {elapsed:?}"
    );
}

#[test]
fn a_lossy_cable_retransmits_without_double_delivery() {
    let sim = sim_with("line", 2);
    let ids = sim.node_ids();
    assert!(wait_until(
        || metric(&sim, ids[0], ids[1]).is_some(),
        Duration::from_secs(8)
    ));

    let key = LinkKey::new(ids[0], ids[1]);
    sim.update_link(
        key,
        &Json::obj([("loss", Json::Num(0.5)), ("latency", Json::Num(0.01))]),
    )
    .unwrap();

    const SENT: usize = 6;
    for i in 0..SENT {
        sim.send(ids[0], Some(ids[1]), &format!("msg {i}"), true)
            .unwrap();
    }
    // Default policy: 4 attempts, 300ms base, doubling.
    assert!(
        wait_until(
            || {
                let s = sim.status(ids[0]).unwrap().stats;
                s.acked + s.delivery_failed == SENT as u64
            },
            Duration::from_secs(20)
        ),
        "every unicast must resolve to acked or failed"
    );

    let stats = sim.status(ids[0]).unwrap().stats;
    assert!(stats.retransmits > 0, "50% loss should force a retransmit");
    assert!(
        stats.acked >= 3,
        "only {} of {SENT} got through",
        stats.acked
    );

    // Whatever did arrive, arrived once: dedup works even when retries race.
    //
    // `inbox.len() == acked` would be wrong: the last ack of a message can be
    // lost too, in which case the sender gives up on a message the receiver
    // did get. At-least-once delivery plus receiver-side dedup cannot promise
    // more than that, so the assertion is "no duplicates" and "everything the
    // sender believes was delivered really was".
    sleep(Duration::from_millis(300));
    let inbox = sim.inbox(ids[1]);
    assert!(
        inbox.len() >= stats.acked as usize,
        "sender acked {} but only {} arrived",
        stats.acked,
        inbox.len()
    );
    assert!(inbox.len() <= SENT, "more messages than were sent");
    let mut texts: Vec<String> = inbox.iter().map(|e| e.text.clone()).collect();
    texts.sort();
    texts.dedup();
    assert_eq!(texts.len(), inbox.len(), "a message was delivered twice");
}

// --------------------------------------------------------------- processes

#[test]
fn a_killed_node_forgets_its_table_and_a_supervised_cable_replugs_it() {
    let sim = sim_with("line", 3);
    let ids = sim.node_ids();
    assert!(wait_until(
        || metric(&sim, ids[0], ids[2]) == Some(2),
        Duration::from_secs(10)
    ));

    // Both cables get a dialer, which is what the UI's checkbox turns on.
    for key in [LinkKey::new(ids[0], ids[1]), LinkKey::new(ids[1], ids[2])] {
        sim.update_link(key, &Json::obj([("supervise", Json::Bool(true))]))
            .unwrap();
    }

    sim.update_node(ids[1], &Json::obj([("alive", Json::Bool(false))]))
        .unwrap();

    // A stopped process has no state: no status, and the peers drop it.
    assert!(
        sim.status(ids[1]).is_none(),
        "a dead node must not report routes"
    );
    assert!(
        wait_until(
            || metric(&sim, ids[0], ids[2]).is_none(),
            Duration::from_secs(6)
        ),
        "alice must stop routing through a dead bob"
    );

    sim.update_node(ids[1], &Json::obj([("alive", Json::Bool(true))]))
        .unwrap();
    assert!(
        wait_until(
            || metric(&sim, ids[0], ids[2]) == Some(2),
            Duration::from_secs(10)
        ),
        "a supervised cable must come back on its own after the restart"
    );
    assert!(
        sim.status(ids[1]).unwrap().stats.acked == 0,
        "counters reset"
    );
}

#[test]
fn an_unsupervised_cable_stays_down_after_a_restart() {
    let sim = sim_with("line", 3);
    let ids = sim.node_ids();
    assert!(wait_until(
        || metric(&sim, ids[0], ids[2]) == Some(2),
        Duration::from_secs(10)
    ));

    sim.update_node(ids[1], &Json::obj([("alive", Json::Bool(false))]))
        .unwrap();
    sim.update_node(ids[1], &Json::obj([("alive", Json::Bool(true))]))
        .unwrap();
    sleep(Duration::from_millis(600));
    assert!(
        sim.status(ids[1]).unwrap().links.is_empty(),
        "without a dialer the restarted node has no connections"
    );

    // An operator can still plug them back in.
    for key in [LinkKey::new(ids[0], ids[1]), LinkKey::new(ids[1], ids[2])] {
        sim.update_link(key, &Json::obj([("up", Json::Bool(true))]))
            .unwrap();
    }
    assert!(
        wait_until(
            || metric(&sim, ids[0], ids[2]) == Some(2),
            Duration::from_secs(10)
        ),
        "healing both cables must restore the line"
    );
}

#[test]
fn renaming_a_node_keeps_its_identity_and_its_cables() {
    let sim = sim_with("line", 2);
    let ids = sim.node_ids();
    assert!(wait_until(
        || metric(&sim, ids[0], ids[1]).is_some(),
        Duration::from_secs(8)
    ));

    sim.update_node(ids[0], &Json::obj([("name", Json::Str("robert".into()))]))
        .unwrap();
    assert_eq!(sim.name_of(ids[0]).as_deref(), Some("robert"));
    assert_eq!(sim.node_ids()[0], ids[0], "the id must survive a rename");
    assert!(
        wait_until(
            || metric(&sim, ids[0], ids[1]).is_some(),
            Duration::from_secs(8)
        ),
        "the cable must be re-plugged after the restart"
    );
}

#[test]
fn adding_a_node_grows_the_mesh() {
    let sim = sim_with("line", 3);
    let ids = sim.node_ids();
    assert!(wait_until(
        || converged(&sim, &ids),
        Duration::from_secs(10)
    ));

    let zoe = sim.add_node(Some("zoe".into()), 8, Some(700.0), Some(300.0));
    sim.add_link(ids[2], zoe, LinkParams::default(), false)
        .unwrap();
    assert!(
        wait_until(
            || metric(&sim, zoe, ids[0]) == Some(3),
            Duration::from_secs(10)
        ),
        "the new node should learn the whole line"
    );
    assert_eq!(sim.name_of(zoe).as_deref(), Some("zoe"));
}

#[test]
fn deleting_a_node_takes_its_cables_with_it() {
    let sim = sim_with("line", 3);
    let ids = sim.node_ids();
    assert!(wait_until(
        || converged(&sim, &ids),
        Duration::from_secs(10)
    ));

    sim.delete_node(ids[1]).unwrap();
    assert!(sim.status(ids[1]).is_none());
    assert_eq!(sim.node_ids().len(), 2);
    assert!(
        sim.hub().links().is_empty(),
        "deleting the middle node must remove both cables"
    );
}

#[test]
fn a_paused_hub_holds_frames_until_resumed() {
    let sim = sim_with("line", 2);
    let ids = sim.node_ids();
    assert!(wait_until(
        || metric(&sim, ids[0], ids[1]).is_some(),
        Duration::from_secs(8)
    ));

    let before = sim.inbox(ids[1]).len();
    sim.hub().set_running(false);
    sim.send(ids[0], Some(ids[1]), "held", true).unwrap();

    sleep(Duration::from_millis(400));
    assert_eq!(
        sim.inbox(ids[1]).len(),
        before,
        "nothing may be delivered while paused"
    );
    assert!(sim.hub().pending_frames() > 0, "the frame should be queued");

    sim.hub().set_running(true);
    assert!(
        wait_until(|| sim.inbox(ids[1]).len() > before, Duration::from_secs(3)),
        "resuming must release the queued frame"
    );
}

#[test]
fn the_snapshot_is_shaped_the_way_the_ui_expects() {
    let sim = sim_with("ring", 3);
    let ids = sim.node_ids();
    assert!(wait_until(
        || converged(&sim, &ids),
        Duration::from_secs(10)
    ));

    let state = sim.snapshot(0);
    assert!(state.get("nodes").is_some());
    assert!(state.f64_of("clock").is_some());
    assert_eq!(state.bool_of("running"), Some(true));

    let Json::Arr(nodes) = state.get("nodes").unwrap() else {
        panic!("nodes must be an array")
    };
    assert_eq!(nodes.len(), 3);
    let node = &nodes[0];
    for key in [
        "id", "short", "name", "alive", "ttl", "x", "y", "pending", "links",
    ] {
        assert!(node.get(key).is_some(), "node is missing '{key}'");
    }
    let Json::Arr(routes) = node.get("routes").unwrap() else {
        panic!("routes must be an array")
    };
    assert_eq!(routes.len(), 2);
    for key in ["dst", "name", "hops", "age"] {
        assert!(routes[0].get(key).is_some(), "route is missing '{key}'");
    }
    let stats = node.get("stats").unwrap();
    for key in ["sent", "delivered", "forwarded", "retransmits", "acked"] {
        assert!(stats.get(key).is_some(), "stats is missing '{key}'");
    }

    let Json::Arr(links) = state.get("links").unwrap() else {
        panic!("links must be an array")
    };
    assert_eq!(links.len(), 3);
    let link = &links[0];
    for key in ["key", "a", "b", "latency", "loss", "up", "supervise"] {
        assert!(link.get(key).is_some(), "link is missing '{key}'");
    }
    // The key must parse back into the same pair.
    let key = LinkKey::parse(link.str_of("key").unwrap()).expect("key parses");
    assert!(key.contains(ids[0]) || key.contains(ids[1]) || key.contains(ids[2]));

    let Json::Arr(log) = state.get("log").unwrap() else {
        panic!("log must be an array")
    };
    assert!(!log.is_empty(), "the event log should have entries");
    for key in ["seq", "t", "who", "tag", "detail"] {
        assert!(log[0].get(key).is_some(), "log entry is missing '{key}'");
    }
}

fn log_of(snapshot: &Json) -> &[Json] {
    match snapshot.get("log") {
        Some(Json::Arr(items)) => items,
        _ => panic!("log must be an array"),
    }
}

#[test]
fn the_log_is_incremental() {
    let sim = sim_with("line", 2);
    let ids = sim.node_ids();
    assert!(wait_until(
        || metric(&sim, ids[0], ids[1]).is_some(),
        Duration::from_secs(8)
    ));

    let full = sim.snapshot(0);
    let total = log_of(&full).len();
    assert!(total > 0);
    let seq = full.f64_of("log_seq").unwrap() as u64;
    assert_eq!(seq as usize, total, "seq must count log entries");

    // Asking for everything after the newest entry must return nothing...
    let caught_up = sim.snapshot(seq);
    assert!(log_of(&caught_up).is_empty(), "no new entries expected");

    // ...and after an old one, only the tail.
    let cut = seq.saturating_sub(2);
    let tail_snapshot = sim.snapshot(cut);
    let tail = log_of(&tail_snapshot);
    assert!(tail.len() >= 2, "the last two entries should come back");
    for entry in tail {
        assert!(
            entry.f64_of("seq").unwrap() as u64 > cut,
            "an entry older than the cursor leaked through"
        );
    }
}
