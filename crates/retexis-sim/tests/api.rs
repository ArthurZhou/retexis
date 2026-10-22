//! End-to-end tests of the control API over real HTTP.
//!
//! The browser is only a view, so the contract worth testing is the API: these
//! tests open sockets, send the same requests `index.html` sends, and check the
//! JSON comes back in the shape the UI reads. No browser needed.

use retexis_sim::http;
use retexis_sim::json::{self, Json};
use retexis_sim::Sim;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::Arc;
use std::thread::sleep;
use std::time::{Duration, Instant};

struct Server {
    addr: String,
}

impl Server {
    fn start(preset: &str, count: usize) -> Server {
        let sim = Sim::new(7);
        sim.reset(preset, count);
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().unwrap().to_string();
        let web = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("web");
        let served = Arc::clone(&sim);
        std::thread::spawn(move || {
            let _ = http::serve_listener(listener, served, web);
        });
        Server { addr }
    }

    fn call(&self, method: &str, path: &str, body: Option<&str>) -> (u16, String) {
        let mut stream = TcpStream::connect(&self.addr).expect("connect");
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let body = body.unwrap_or("");
        let request = format!(
            "{method} {path} HTTP/1.1\r\nhost: 127.0.0.1\r\ncontent-type: application/json\r\n\
             content-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(request.as_bytes()).expect("write");
        let mut out = Vec::new();
        stream.read_to_end(&mut out).expect("read");
        let text = String::from_utf8_lossy(&out).to_string();
        let status: u16 = text
            .split_whitespace()
            .nth(1)
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let body = text
            .split_once("\r\n\r\n")
            .map(|(_, b)| b.to_string())
            .unwrap_or_default();
        (status, body)
    }

    fn json(&self, method: &str, path: &str, body: Option<&str>) -> Json {
        let (status, text) = self.call(method, path, body);
        assert_eq!(status, 200, "{method} {path} -> {status}: {text}");
        json::parse(&text).unwrap_or_else(|e| panic!("bad json from {path}: {e}\n{text}"))
    }

    fn state(&self) -> Json {
        self.json("GET", "/api/state", None)
    }

    fn wait_for(&self, mut pred: impl FnMut(&Json) -> bool, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            if pred(&self.state()) {
                return true;
            }
            if Instant::now() >= deadline {
                return false;
            }
            sleep(Duration::from_millis(50));
        }
    }
}

fn nodes_of(state: &Json) -> &[Json] {
    match state.get("nodes") {
        Some(Json::Arr(items)) => items,
        _ => panic!("nodes must be an array"),
    }
}

fn links_of(state: &Json) -> &[Json] {
    match state.get("links") {
        Some(Json::Arr(items)) => items,
        _ => panic!("links must be an array"),
    }
}

fn node_named<'a>(state: &'a Json, name: &str) -> &'a Json {
    nodes_of(state)
        .iter()
        .find(|n| n.str_of("name") == Some(name))
        .unwrap_or_else(|| panic!("no node named {name}"))
}

fn id_of(state: &Json, name: &str) -> String {
    node_named(state, name).str_of("id").unwrap().to_string()
}

#[test]
fn serves_the_shared_ui() {
    let s = Server::start("line", 3);
    let (status, body) = s.call("GET", "/", None);
    assert_eq!(status, 200);
    assert!(body.contains("retexis"), "the UI should identify itself");
    assert!(body.contains("api/state"), "the UI should talk to the API");
}

#[test]
fn state_has_the_shape_the_ui_reads() {
    let s = Server::start("line", 4);
    let state = s.state();
    assert_eq!(nodes_of(&state).len(), 4);
    assert_eq!(links_of(&state).len(), 3);
    assert_eq!(state.bool_of("running"), Some(true));
    assert!(state.f64_of("clock").is_some());
    assert!(state.f64_of("rate").is_some());
    assert!(state.f64_of("log_seq").is_some());
    assert!(state.get("log").is_some());

    let alice = node_named(&state, "alice");
    assert_eq!(alice.bool_of("alive"), Some(true));
    assert!(alice.f64_of("x").is_some());
    assert!(alice.f64_of("y").is_some());
    assert!(alice.get("inbox").is_some());
    assert!(alice.get("stats").is_some());
    assert!(alice.get("routes").is_some());
}

#[test]
fn the_line_converges_without_any_help() {
    let s = Server::start("line", 4);
    // 3 hops needs a few announce rounds at 400 ms.
    assert!(
        s.wait_for(
            |state| nodes_of(state)
                .iter()
                .all(|n| n.get("routes").map(|_| true).unwrap_or(false)
                    && match n.get("routes") {
                        Some(Json::Arr(r)) => r.len() == 3,
                        _ => false,
                    }),
            Duration::from_secs(15)
        ),
        "every node should learn the other three: {:?}",
        nodes_of(&s.state())
            .iter()
            .map(|n| (
                n.str_of("name").unwrap_or("?").to_string(),
                n.get("routes").cloned()
            ))
            .collect::<Vec<_>>()
    );
}

#[test]
fn presets_rebuild_the_topology() {
    let s = Server::start("line", 3);
    s.json("POST", "/api/preset", Some(r#"{"kind":"ring","count":5}"#));
    let state = s.state();
    assert_eq!(nodes_of(&state).len(), 5);
    assert_eq!(
        links_of(&state).len(),
        5,
        "a ring has as many cables as nodes"
    );
    for link in links_of(&state) {
        assert!(link.bool_of("up").unwrap_or(false));
    }

    s.json("POST", "/api/preset", Some(r#"{"kind":"star","count":4}"#));
    let state = s.state();
    assert_eq!(nodes_of(&state).len(), 4);
    assert_eq!(links_of(&state).len(), 3, "a star of 4 has 3 spokes");
}

#[test]
fn a_node_can_be_added_renamed_and_removed() {
    let s = Server::start("line", 2);
    let created = s.json("POST", "/api/node", Some(r#"{"name":"zoe"}"#));
    let zoe = created.str_of("id").expect("id").to_string();
    assert_eq!(nodes_of(&s.state()).len(), 3);

    s.json(
        "PATCH",
        &format!("/api/node/{zoe}"),
        Some(r#"{"name":"zoe2","ttl":5}"#),
    );
    let state = s.state();
    let node = node_named(&state, "zoe2");
    assert_eq!(node.str_of("id"), Some(zoe.as_str()), "rename keeps the id");
    assert_eq!(node.f64_of("ttl"), Some(5.0));

    // Move it: presentation state, stored by the sim, never by a router.
    s.json(
        "PATCH",
        &format!("/api/node/{zoe}"),
        Some(r#"{"x":123,"y":456}"#),
    );
    let state = s.state();
    assert_eq!(node_named(&state, "zoe2").f64_of("x"), Some(123.0));

    s.json("DELETE", &format!("/api/node/{zoe}"), None);
    assert_eq!(nodes_of(&s.state()).len(), 2);
}

#[test]
fn a_cable_can_be_cut_healed_and_tuned() {
    let s = Server::start("line", 3);
    assert!(s.wait_for(
        |state| nodes_of(state).iter().all(|n| match n.get("routes") {
            Some(Json::Arr(r)) => r.len() == 2,
            _ => false,
        }),
        Duration::from_secs(15)
    ));

    // `links` is sorted by key, which says nothing about which cable is which.
    let state = s.state();
    let (bob, carol) = (id_of(&state, "bob"), id_of(&state, "carol"));
    let key = links_of(&state)
        .iter()
        .find(|l| {
            let ends = [l.str_of("a"), l.str_of("b")];
            ends.contains(&Some(bob.as_str())) && ends.contains(&Some(carol.as_str()))
        })
        .expect("the bob-carol cable")
        .str_of("key")
        .expect("key")
        .to_string();

    s.json(
        "PATCH",
        &format!("/api/link/{}", key.replace('|', "%7C")),
        Some(r#"{"up":false}"#),
    );
    let state = s.state();
    let cut = links_of(&state)
        .iter()
        .find(|l| l.str_of("key") == Some(key.as_str()))
        .expect("the cable is still listed");
    assert_eq!(cut.bool_of("up"), Some(false));
    assert!(s.wait_for(
        |state| node_named(state, "alice")
            .get("routes")
            .and_then(|r| match r {
                Json::Arr(a) => Some(a.len()),
                _ => None,
            })
            == Some(1),
        Duration::from_secs(6)
    ));

    s.json(
        "PATCH",
        &format!("/api/link/{}", key.replace('|', "%7C")),
        Some(r#"{"up":true,"latency":0.05,"loss":0.1,"supervise":true}"#),
    );
    let state = s.state();
    let healed = links_of(&state)
        .iter()
        .find(|l| l.str_of("key") == Some(key.as_str()))
        .expect("cable");
    assert_eq!(healed.bool_of("up"), Some(true));
    assert_eq!(healed.bool_of("supervise"), Some(true));
    assert!((healed.f64_of("latency").unwrap() - 0.05).abs() < 1e-6);
    assert!((healed.f64_of("loss").unwrap() - 0.1).abs() < 1e-6);

    // And a new cable between two nodes that had none.
    let a = id_of(&state, "alice");
    let c = id_of(&state, "carol");
    let made = s.json(
        "POST",
        "/api/link",
        Some(&format!(r#"{{"a":"{a}","b":"{c}","latency":0.01}}"#)),
    );
    assert!(made.str_of("key").is_some());
    assert_eq!(links_of(&s.state()).len(), 3);
}

#[test]
fn sending_over_http_reaches_the_inbox() {
    let s = Server::start("line", 3);
    assert!(s.wait_for(
        |state| nodes_of(state).iter().all(|n| match n.get("routes") {
            Some(Json::Arr(r)) => r.len() == 2,
            _ => false,
        }),
        Duration::from_secs(15)
    ));

    let state = s.state();
    let alice = id_of(&state, "alice");
    let carol = id_of(&state, "carol");

    s.json(
        "POST",
        "/api/send",
        Some(&format!(
            r#"{{"from":"{alice}","to":null,"text":"broadcast from the browser","reliable":false}}"#
        )),
    );
    assert!(
        s.wait_for(
            |state| !match node_named(state, "carol").get("inbox") {
                Some(Json::Arr(i)) => i.is_empty(),
                _ => true,
            },
            Duration::from_secs(6)
        ),
        "carol never saw the broadcast"
    );

    s.json(
        "POST",
        "/api/send",
        Some(&format!(
            r#"{{"from":"{carol}","to":"{alice}","text":"dm","reliable":true}}"#
        )),
    );
    assert!(
        s.wait_for(
            |state| node_named(state, "alice")
                .get("inbox")
                .and_then(|i| match i {
                    Json::Arr(a) => Some(a.iter().any(|m| m.str_of("text") == Some("dm"))),
                    _ => None,
                })
                .unwrap_or(false),
            Duration::from_secs(6)
        ),
        "the reliable unicast never arrived"
    );
    assert!(
        s.wait_for(
            |state| node_named(state, "carol")
                .get("stats")
                .and_then(|s| s.f64_of("acked"))
                .unwrap_or(0.0)
                == 1.0,
            Duration::from_secs(4)
        ),
        "the sender should have seen its ack"
    );
}

#[test]
fn pausing_and_stepping_are_visible_in_the_state() {
    let s = Server::start("line", 2);
    s.json("POST", "/api/control", Some(r#"{"running":false}"#));
    assert_eq!(s.state().bool_of("running"), Some(false));

    s.json("POST", "/api/control", Some(r#"{"rate":2.5}"#));
    assert_eq!(s.state().f64_of("rate"), Some(2.5));

    // A step resumes for a moment, then pauses again by itself.
    s.json("POST", "/api/control", Some(r#"{"step":0.1}"#));
    sleep(Duration::from_millis(400));
    assert_eq!(
        s.state().bool_of("running"),
        Some(false),
        "a step must pause again when it is done"
    );
}

#[test]
fn bad_requests_are_rejected_without_killing_the_server() {
    let s = Server::start("line", 2);

    let (status, _) = s.call("GET", "/api/nope", None);
    assert_eq!(status, 404);

    let (status, _) = s.call("POST", "/api/send", Some("{not json"));
    assert_eq!(status, 400);

    let (status, body) = s.call("POST", "/api/send", Some(r#"{"from":"zz","text":"x"}"#));
    assert_eq!(status, 400);
    assert!(body.contains("error"));

    let (status, _) = s.call("PATCH", "/api/node/not-a-node", Some(r#"{"ttl":3}"#));
    assert_eq!(status, 400);

    let (status, _) = s.call("PATCH", "/api/link/not-a-key", Some(r#"{"up":false}"#));
    assert_eq!(status, 400);

    // Still serving.
    assert!(s.state().f64_of("clock").is_some());
}

#[test]
fn the_log_cursor_only_returns_new_entries() {
    let s = Server::start("line", 3);
    sleep(Duration::from_millis(500));
    let state = s.state();
    let seq = state.f64_of("log_seq").unwrap() as u64;
    assert!(seq > 0);

    let (status, body) = s.call("GET", &format!("/api/state?log_since={seq}"), None);
    assert_eq!(status, 200);
    let json = json::parse(&body).unwrap();
    match json.get("log") {
        Some(Json::Arr(items)) => {
            for entry in items {
                assert!(entry.f64_of("seq").unwrap() as u64 > seq);
            }
        }
        _ => panic!("log must be an array"),
    }
}
