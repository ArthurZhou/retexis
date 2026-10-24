//! Two GUI nodes, real TCP, no browser.
//!
//! This is the test that matters for the GUI: it boots the same code path the
//! binary does ([`retexis_gui::boot`]) and asserts that a message typed in one
//! window shows up in the other. No simulated medium, no mocks — two routers,
//! one TCP connection, the production data layer in between.

use retexis_gui::app::Options;
use retexis_gui::boot::{self, Boot};
use retexis_gui::log::{Ack, Entry, Kind};
use std::time::{Duration, Instant};

fn options(name: &str) -> Options {
    Options {
        name: name.to_string(),
        // Port 0: the OS picks, so two tests can run in parallel.
        listen: vec!["tcp://127.0.0.1:0".to_string()],
        // No responder: discovery is not what this test is about, and two
        // responders would fight over the port.
        announce: false,
        ..Default::default()
    }
}

fn wait_until(mut pred: impl FnMut() -> bool, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if pred() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    pred()
}

fn messages(entries: &[Entry]) -> Vec<&retexis_gui::log::Message> {
    entries
        .iter()
        .filter_map(|e| match &e.kind {
            Kind::Message(m) => Some(m),
            _ => None,
        })
        .collect()
}

fn peer_seen(b: &Boot, name: &str) -> bool {
    b.app.view().peers.iter().any(|p| p.name == name)
}

/// Both nodes, already connected and converged.
fn pair() -> (Boot, Boot) {
    let alice = boot::boot(&options("alice")).expect("boot alice");
    let bob = boot::boot(&options("bob")).expect("boot bob");

    let url = alice.dial_url().expect("alice listens");
    assert!(bob.app.connect(&url).expect("dial alice"));

    assert!(
        wait_until(
            || peer_seen(&alice, "bob") && peer_seen(&bob, "alice"),
            Duration::from_secs(10)
        ),
        "the two nodes never learned about each other"
    );
    (alice, bob)
}

#[test]
fn a_message_typed_in_one_window_arrives_in_the_other() {
    let (alice, bob) = pair();

    bob.app
        .send(Some(alice.app.id()), "你好 alice，这条是私聊")
        .expect("send");

    assert!(
        wait_until(
            || messages(&alice.app.entries())
                .iter()
                .any(|m| m.text == "你好 alice，这条是私聊"),
            Duration::from_secs(6)
        ),
        "the message never arrived"
    );

    let arrived = messages(&alice.app.entries())
        .into_iter()
        .find(|m| m.text == "你好 alice，这条是私聊")
        .expect("just found")
        .clone();
    assert!(!arrived.mine);
    assert_eq!(arrived.from_name, "bob");
    assert_eq!(arrived.to, Some(alice.app.id().hex()), "addressed to alice");
    assert!(!arrived.stream, "a short message is one frame");

    // ...and the sender's bubble turns green.
    assert!(
        wait_until(
            || messages(&bob.app.entries())
                .iter()
                .any(|m| m.ack == Ack::Delivered),
            Duration::from_secs(6)
        ),
        "the sender never got its ack"
    );
}

#[test]
fn a_broadcast_reaches_the_other_side_without_an_ack() {
    let (alice, bob) = pair();

    alice.app.send(None, "大家好").expect("send");

    assert!(
        wait_until(
            || messages(&alice.app.entries())
                .iter()
                .any(|m| m.text == "大家好" && m.mine),
            Duration::from_secs(2)
        ),
        "our own message must be in our own timeline immediately"
    );

    // Exactly once: the router's loopback of our own broadcast must not add a
    // second bubble.
    assert!(
        wait_until(
            || messages(&bob.app.entries())
                .iter()
                .any(|m| m.text == "大家好"),
            Duration::from_secs(6)
        ),
        "the broadcast never arrived"
    );
    let mine = messages(&alice.app.entries())
        .iter()
        .filter(|m| m.text == "大家好")
        .count();
    assert_eq!(mine, 1, "our own broadcast appears exactly once");
}

#[test]
fn a_long_message_arrives_as_one_reassembled_bubble() {
    let (alice, bob) = pair();

    // Comfortably over the 1 KiB stream threshold.
    let text = "长消息。".repeat(400);
    assert!(text.len() > 1024);
    bob.app.send(Some(alice.app.id()), &text).expect("send");

    assert!(
        wait_until(
            || messages(&alice.app.entries())
                .iter()
                .any(|m| m.text == text),
            Duration::from_secs(10)
        ),
        "the stream never reassembled"
    );

    let arrived = messages(&alice.app.entries())
        .into_iter()
        .find(|m| m.text == text)
        .expect("just found")
        .clone();
    assert!(arrived.stream, "over the threshold it must be a stream");
    assert_eq!(arrived.bytes, text.len());
    assert_eq!(
        arrived.to,
        Some(alice.app.id().hex()),
        "a direct stream must not look like a broadcast"
    );

    // One bubble, not one per chunk.
    let count = messages(&alice.app.entries())
        .iter()
        .filter(|m| m.bytes == text.len())
        .count();
    assert_eq!(count, 1, "the stream must be shown once");
}

#[test]
fn dialling_ourselves_is_refused_with_a_reason() {
    let alice = boot::boot(&options("alice")).expect("boot");
    let url = alice.dial_url().expect("listens");
    let err = alice.app.connect(&url).expect_err("must refuse");
    assert!(
        err.contains("自己的监听地址"),
        "the message must say what went wrong: {err}"
    );
}

#[test]
fn a_message_sent_before_the_handshake_finishes_does_not_wedge_the_node() {
    let alice = boot::boot(&options("alice")).expect("boot alice");
    let bob = boot::boot(&options("bob")).expect("boot bob");
    let url = alice.dial_url().expect("alice listens");
    bob.app.connect(&url).expect("dial");

    // No waiting: at this instant the route table is empty and the handshake
    // has barely started. Typing into the window right after dialling is the
    // normal thing to do, so it must not panic, and it must not swallow the
    // message silently either: it is either refused with a sentence or taken.
    match bob.app.send(Some(alice.app.id()), "too early?") {
        Ok(()) => {}
        Err(e) => assert!(!e.is_empty(), "a refusal has to say something: {e:?}"),
    }

    // Whatever the answer was, the node is still a node: once the link is up,
    // the next message goes through.
    assert!(
        wait_until(|| peer_seen(&alice, "bob"), Duration::from_secs(10)),
        "the link never converged"
    );
    bob.app.send(Some(alice.app.id()), "after").expect("send");
    assert!(
        wait_until(
            || messages(&alice.app.entries())
                .iter()
                .any(|m| m.text == "after"),
            Duration::from_secs(6)
        ),
        "the node stopped working after the early send"
    );
}

#[test]
fn startup_say_and_connect_still_work() {
    let alice = boot::boot(&options("alice")).expect("boot alice");
    let url = alice.dial_url().expect("alice listens");

    let mut opts = options("bob");
    opts.connect = vec![url];
    opts.say = Some("从命令行启动".to_string());
    let bob = boot::boot(&opts).expect("boot bob");

    // `--say` puts the line in our own timeline: the window shows what it
    // said, whether or not anyone was there to hear it yet.
    assert!(
        messages(&bob.app.entries())
            .iter()
            .any(|m| m.text == "从命令行启动" && m.mine),
        "the startup line must be in the local log"
    );

    // `--connect` is a real dial: the peer shows up on both sides.
    assert!(
        wait_until(
            || peer_seen(&alice, "bob") && peer_seen(&bob, "alice"),
            Duration::from_secs(10)
        ),
        "--connect did not bring the link up"
    );

    // And the node is usable afterwards.
    bob.app.send(Some(alice.app.id()), "later").expect("send");
    assert!(
        wait_until(
            || messages(&alice.app.entries())
                .iter()
                .any(|m| m.text == "later"),
            Duration::from_secs(6)
        ),
        "the node booted with --say/--connect is not sending"
    );
}
