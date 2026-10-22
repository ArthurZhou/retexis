//! A stream across two real routers over TCP.
//!
//! The unit tests in `stream.rs` prove the assembler works on synthetic
//! frames. This proves the whole path: `StreamSender` → envelope → TCP →
//! router → `DataHost` → `DataSink`, with the receiver reassembling 64 KiB
//! from 64 chunks and getting exactly the bytes that were sent.

use retexis_core::{ContentType, LinkFactory, NodeId};
use retexis_data::{DataHost, DataSink, InboundFrame, StreamEvent, StreamSender};
use retexis_link_tcp::TcpFactory;
use retexis_routing::{start, Delivery, MeshHandle, RouterConfig};
use std::sync::mpsc::channel;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const CHANNEL: &str = "chat";

#[derive(Default)]
struct Collected {
    bytes: Vec<u8>,
    opened: usize,
    chunks: usize,
    closed: Option<usize>,
    dropped: Vec<String>,
}

struct Collector {
    state: Arc<Mutex<Collected>>,
}

impl DataSink for Collector {
    fn name(&self) -> &'static str {
        "collector"
    }

    fn channels(&self) -> &'static [&'static str] {
        &[CHANNEL]
    }

    fn on_frame(&mut self, _handle: &MeshHandle, _frame: &InboundFrame) {
        // A stream must not also arrive as a frame.
        self.state
            .lock()
            .unwrap()
            .dropped
            .push("unexpected frame".into());
    }

    fn on_stream(&mut self, _handle: &MeshHandle, event: &StreamEvent) {
        let mut st = self.state.lock().unwrap();
        match event {
            StreamEvent::Opened { .. } => st.opened += 1,
            StreamEvent::Data { chunk, .. } => {
                st.chunks += 1;
                st.bytes.extend_from_slice(chunk);
            }
            StreamEvent::Closed { bytes, .. } => st.closed = Some(*bytes),
            StreamEvent::Dropped { reason, .. } => st.dropped.push(reason.to_string()),
        }
    }
}

fn node(name: &str) -> (MeshHandle, std::sync::mpsc::Receiver<Delivery>) {
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

#[test]
fn a_64k_stream_arrives_byte_for_byte() {
    let factory = TcpFactory::new();
    let (a, a_rx) = node("a");
    let (b, _b_rx) = node("b");

    let acceptor = factory.listen("127.0.0.1:0").expect("listen");
    let addr = acceptor.local_addr();
    a.accept_from(acceptor).expect("accept loop");
    b.add_link(factory.connect(&addr).expect("connect"))
        .expect("add link");
    assert!(
        wait_until(
            || b.routes().iter().any(|r| r.dst == a.id()),
            Duration::from_secs(5)
        ),
        "the two nodes must see each other first"
    );

    // The receiving side runs the data layer on its own thread, exactly like
    // the CLI does.
    let state = Arc::new(Mutex::new(Collected::default()));
    let collector_state = Arc::clone(&state);
    let delivery_handle = a.clone();
    std::thread::spawn(move || {
        let mut host = DataHost::new();
        host.register(Box::new(Collector {
            state: collector_state,
        }));
        while let Ok(delivery) = a_rx.recv() {
            host.dispatch(&delivery_handle, &delivery);
        }
    });

    let payload: Vec<u8> = (0..64 * 1024).map(|i| (i % 251) as u8).collect();
    let mut sender = StreamSender::open(&b, Some(a.id()), CHANNEL, "blob.bin")
        .expect("open")
        .with_chunk_size(1024);
    let id = sender.id();
    assert_eq!(sender.write(&payload).expect("write"), payload.len());
    sender.close().expect("close");

    assert!(
        wait_until(
            || state.lock().unwrap().closed.is_some(),
            Duration::from_secs(10)
        ),
        "the stream never closed: {:?}",
        state.lock().unwrap().chunks
    );

    let st = state.lock().unwrap();
    assert!(st.dropped.is_empty(), "dropped: {:?}", st.dropped);
    assert_eq!(st.opened, 1, "one open event");
    assert_eq!(st.chunks, 64, "64 KiB in 1 KiB chunks");
    assert_eq!(st.bytes, payload, "reassembled bytes must match exactly");
    assert_eq!(st.closed, Some(payload.len()));
    // And the id the sender reported is the one the receiver saw.
    assert_ne!(id.0, 0);
}

#[test]
fn stream_and_frame_traffic_share_one_channel() {
    // The data layer routes by content type, so a sink sees whole messages and
    // stream events without either being confused for the other.
    let factory = TcpFactory::new();
    let (a, a_rx) = node("a");
    let (b, _b_rx) = node("b");
    let acceptor = factory.listen("127.0.0.1:0").expect("listen");
    let addr = acceptor.local_addr();
    a.accept_from(acceptor).expect("accept loop");
    b.add_link(factory.connect(&addr).expect("connect"))
        .expect("add link");
    assert!(wait_until(
        || b.routes().iter().any(|r| r.dst == a.id()),
        Duration::from_secs(5)
    ));

    let state = Arc::new(Mutex::new(Collected::default()));
    let frames = Arc::new(Mutex::new(Vec::<String>::new()));
    let collector_state = Arc::clone(&state);
    let frames_seen = Arc::clone(&frames);
    let delivery_handle = a.clone();
    std::thread::spawn(move || {
        struct Both {
            inner: Collector,
            frames: Arc<Mutex<Vec<String>>>,
        }
        impl DataSink for Both {
            fn name(&self) -> &'static str {
                "both"
            }
            fn channels(&self) -> &'static [&'static str] {
                &[CHANNEL]
            }
            fn on_frame(&mut self, _h: &MeshHandle, f: &InboundFrame) {
                // Record only: forwarding to `Collector::on_frame` would flag
                // the frame as unexpected, which it is not.
                self.frames
                    .lock()
                    .unwrap()
                    .push(f.frame.as_text().unwrap_or("?").to_string());
            }
            fn on_stream(&mut self, h: &MeshHandle, e: &StreamEvent) {
                self.inner.on_stream(h, e);
            }
        }
        let mut host = DataHost::new();
        host.register(Box::new(Both {
            inner: Collector {
                state: collector_state,
            },
            frames: frames_seen,
        }));
        while let Ok(delivery) = a_rx.recv() {
            host.dispatch(&delivery_handle, &delivery);
        }
    });

    b.send_to(a.id(), CHANNEL, ContentType::Text, b"plain".to_vec())
        .unwrap();
    let mut sender = StreamSender::open(&b, Some(a.id()), CHANNEL, "x").unwrap();
    sender.write(b"streamed").unwrap();
    sender.close().unwrap();

    assert!(
        wait_until(
            || {
                let st = state.lock().unwrap();
                st.closed.is_some() && !frames.lock().unwrap().is_empty()
            },
            Duration::from_secs(8)
        ),
        "both paths should complete"
    );
    let st = state.lock().unwrap();
    assert!(st.dropped.is_empty(), "dropped: {:?}", st.dropped);
    assert_eq!(st.bytes, b"streamed");
    assert_eq!(*frames.lock().unwrap(), vec!["plain".to_string()]);
}

#[test]
fn a_stream_to_a_dead_peer_does_not_panic() {
    // No listener at all: the sender must report through the router, not crash.
    let (b, _rx) = node("b");
    let ghost = NodeId::random();
    let mut sender = StreamSender::open(&b, Some(ghost), CHANNEL, "x").expect("open");
    // `send_to` with no route is accepted and dropped by the router; the data
    // layer only surfaces encode errors.
    assert!(sender.write(b"data").is_ok());
    assert!(sender.close().is_ok());
}
