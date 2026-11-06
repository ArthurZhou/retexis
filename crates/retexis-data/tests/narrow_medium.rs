//! A medium this stack has never heard of, carrying a stream.
//!
//! The point of this file is *what it does not do*: it never touches
//! `retexis-routing` or `retexis-core`, and it still gets routing, framing,
//! acknowledgement and reassembly. Adding a medium is supposed to be
//! `Link` + `LinkAcceptor` + `LinkFactory` and a `register` call; if that ever
//! stops being true, this test is where it shows.
//!
//! It is also the narrow case on purpose. `max_frame` here is 512 bytes, the
//! sort of limit a radio or a BLE characteristic has, and it is the reason
//! `StreamSender` sizes its chunks from the links instead of from a constant:
//! an envelope the router cannot fit is dropped, so a 61 KiB chunk on a
//! 512-byte medium is not a slow stream, it is an empty one.

use retexis_core::{next_link_id, Body, ContentType, Envelope, Link, LinkError, LinkId};
use retexis_data::StreamSender;
use retexis_routing::{start, MeshHandle, RouterConfig, RouterNotice};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Bytes a stream chunk spends on its own header (`u64 id` + `u32 seq`) inside
/// the frame payload. Repeated here on purpose: the test counts application
/// bytes, not bytes the protocol added.
const CHUNK_HEADER: usize = 12;

/// One end of an in-memory pipe. Everything sent lands in the other end's
/// queue, and in `wire` — the list of envelopes this end actually put out.
///
/// A real medium would put the bytes on a socket; the router cannot tell the
/// difference, which is the point.
struct Port {
    id: LinkId,
    max_frame: usize,
    out: Mutex<Sender<Vec<u8>>>,
    inbound: Mutex<Receiver<Vec<u8>>>,
    wire: Arc<Mutex<Vec<Vec<u8>>>>,
    closed: Arc<AtomicBool>,
}

impl Link for Port {
    fn id(&self) -> LinkId {
        self.id
    }

    fn transport(&self) -> &'static str {
        "pipe"
    }

    fn peer(&self) -> String {
        "pipe:peer".to_string()
    }

    fn initiator(&self) -> bool {
        true
    }

    fn max_frame(&self) -> usize {
        self.max_frame
    }

    fn send(&self, envelope: &[u8]) -> Result<(), LinkError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(LinkError::Closed);
        }
        // The router already refuses to hand us an oversized envelope. Refusing
        // again is what a real medium does, and it is what makes a wrong chunk
        // size impossible to wave through.
        if envelope.len() > self.max_frame {
            return Err(LinkError::FrameTooLarge {
                len: envelope.len(),
                max: self.max_frame,
            });
        }
        self.wire.lock().expect("wire").push(envelope.to_vec());
        let out = self.out.lock().expect("out");
        out.send(envelope.to_vec()).map_err(|_| LinkError::Closed)
    }

    fn recv(&self) -> Result<Vec<u8>, LinkError> {
        let rx = self.inbound.lock().expect("inbound");
        loop {
            if self.closed.load(Ordering::Acquire) {
                return Err(LinkError::Closed);
            }
            match rx.recv_timeout(Duration::from_millis(20)) {
                Ok(bytes) => return Ok(bytes),
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => continue,
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                    return Err(LinkError::Closed)
                }
            }
        }
    }

    fn shutdown(&self) {
        self.closed.store(true, Ordering::Release);
    }
}

/// Two ports wired back to back. Neither side dialled; the cable just exists.
fn duplex(max_frame: usize) -> (Port, Port, Arc<Mutex<Vec<Vec<u8>>>>) {
    let (a_to_b, b_in) = channel();
    let (b_to_a, a_in) = channel();
    let wire = Arc::new(Mutex::new(Vec::new()));
    let end = |id, out, inbound, wire: &Arc<Mutex<Vec<Vec<u8>>>>| Port {
        id,
        max_frame,
        out: Mutex::new(out),
        inbound: Mutex::new(inbound),
        wire: Arc::clone(wire),
        closed: Arc::new(AtomicBool::new(false)),
    };
    (
        end(next_link_id(), a_to_b, a_in, &wire),
        end(next_link_id(), b_to_a, b_in, &wire),
        wire,
    )
}

struct Node {
    handle: MeshHandle,
    /// Everything this node refused to send, for the failure message.
    drops: Arc<Mutex<Vec<String>>>,
    _thread: std::thread::JoinHandle<()>,
}

impl Drop for Node {
    fn drop(&mut self) {
        self.handle.shutdown();
    }
}

fn node(name: &str) -> Node {
    node_with(name, None)
}

fn node_with(name: &str, sealer: Option<retexis_crypto::Sealer>) -> Node {
    let (tx, rx) = channel();
    let drops = Arc::new(Mutex::new(Vec::new()));
    let sink = {
        let drops = Arc::clone(&drops);
        Arc::new(move |n: RouterNotice| {
            if let RouterNotice::Dropped { reason, .. } = n {
                drops.lock().expect("drops").push(format!("{reason:?}"));
            }
        })
    };
    let (handle, thread) = start(
        RouterConfig {
            node_name: name.into(),
            sealer,
            ..Default::default()
        },
        tx,
        sink,
    );
    // Something has to drain deliveries; dropping the receiver would make the
    // router's sends fail instead of being ignored.
    std::thread::spawn(move || while rx.recv().is_ok() {});
    Node {
        handle,
        drops,
        _thread: thread,
    }
}

fn wait_until(mut pred: impl FnMut() -> bool, what: &str) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if pred() {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("timed out waiting for {what}");
}

/// Stream chunks on the wire, as (count, application bytes).
fn chunks(wire: &Arc<Mutex<Vec<Vec<u8>>>>) -> (usize, usize) {
    let wire = wire.lock().expect("wire");
    let mut count = 0;
    let mut bytes = 0;
    for raw in wire.iter() {
        let Ok(envelope) = Envelope::decode(raw) else {
            continue;
        };
        if let Body::Data(data) = &envelope.body {
            if data.content_type == ContentType::StreamChunk {
                count += 1;
                // The chunk's own id/seq header is protocol, not payload.
                bytes += data.payload.len().saturating_sub(CHUNK_HEADER);
            }
        }
    }
    (count, bytes)
}

/// A pair of nodes that have already found each other over the pipe.
fn pair(max_frame: usize) -> (Node, Node, Arc<Mutex<Vec<Vec<u8>>>>) {
    pair_with(max_frame, None)
}

fn pair_with(
    max_frame: usize,
    sealer: Option<retexis_crypto::Sealer>,
) -> (Node, Node, Arc<Mutex<Vec<Vec<u8>>>>) {
    let (a, b, wire) = duplex(max_frame);
    let alice = node_with("alice", sealer.clone());
    let bob = node_with("bob", sealer);
    alice.handle.add_link(Box::new(a)).expect("alice plugs in");
    bob.handle.add_link(Box::new(b)).expect("bob plugs in");
    // Hello and the route advert have to cross before a flood has a target.
    wait_until(
        || {
            alice
                .handle
                .status()
                .links
                .iter()
                .any(|l| l.node.is_some())
        },
        "the two nodes to learn about each other",
    );
    (alice, bob, wire)
}

#[test]
fn a_512_byte_medium_carries_a_stream_without_a_hole() {
    const MAX_FRAME: usize = 512;
    let (alice, _bob, wire) = pair(MAX_FRAME);

    // 4 KiB over a medium whose whole envelope budget is half a kilobyte.
    let payload: Vec<u8> = (0..4096).map(|i| (i % 251) as u8).collect();
    let mut sender = StreamSender::open(&alice.handle, None, "chat", "narrow").expect("open");
    sender.write(&payload).expect("write");
    sender.close().expect("close");

    // `write` hands the chunks to the router queue and the router thread fills
    // the medium, so this waits for the last of it instead of assuming it is
    // instantaneous.
    wait_until(
        || chunks(&wire).1 == payload.len(),
        "the whole stream to reach the medium",
    );

    // Every chunk has to be one the medium can hold, and all of them together
    // have to be all of it: a chunk the router cannot fit is dropped, which
    // leaves bytes missing with no error anywhere.
    let (count, bytes) = chunks(&wire);
    assert_eq!(bytes, payload.len(), "the stream lost payload on the way out");
    assert!(
        count >= 4096 / MAX_FRAME,
        "{count} chunks is too few for 4096 bytes on a {MAX_FRAME}-byte medium"
    );
    assert_eq!(
        sender.chunks_written() as usize, count,
        "the sender's count must match what reached the medium"
    );
    assert!(
        alice.drops.lock().expect("drops").is_empty(),
        "the router refused to send: {:?}",
        alice.drops.lock().expect("drops")
    );

    let wire = wire.lock().expect("wire");
    for raw in wire.iter() {
        assert!(
            raw.len() <= MAX_FRAME,
            "an envelope of {} bytes exceeds the {MAX_FRAME}-byte medium",
            raw.len()
        );
    }
}

#[test]
fn the_chunk_size_follows_the_narrowest_link() {
    // The same stream over a 512-byte medium and over no medium at all: if the
    // chunk size were a constant, both would report the same number of chunks
    // and the narrow one would be empty.
    let (narrow_node, _bob, _wire) = pair(512);
    let mut narrow =
        StreamSender::open(&narrow_node.handle, None, "chat", "narrow").expect("open");
    narrow.write(&[0u8; 4096]).expect("write");
    narrow.close().expect("close");

    let bare = node("bare");
    let mut wide = StreamSender::open(&bare.handle, None, "chat", "bare").expect("open");
    wide.write(&[0u8; 4096]).expect("write");
    wide.close().expect("close");

    assert!(
        narrow.chunks_written() > wide.chunks_written(),
        "512 bytes per frame must need more chunks than 60 KiB per frame ({} vs {})",
        narrow.chunks_written(),
        wide.chunks_written()
    );
}

/// A sealed envelope is 16 bytes longer than a plaintext one. If the chunk
/// budget forgot that, every chunk on an authenticated narrow link would be
/// one tag too big — refused by the router, with no bytes delivered and no
/// obvious reason.
#[test]
fn an_authenticated_narrow_link_still_fits_its_frames() {
    const MAX_FRAME: usize = 512;
    let sealer = retexis_crypto::Sealer::from_passphrase("narrow mesh");
    let (alice, _bob, wire) = pair_with(MAX_FRAME, Some(sealer));

    let payload: Vec<u8> = (0..4096).map(|i| (i % 251) as u8).collect();
    let mut sender = StreamSender::open(&alice.handle, None, "chat", "sealed").expect("open");
    sender.write(&payload).expect("write");
    sender.close().expect("close");

    wait_until(
        || chunks(&wire).1 == payload.len(),
        "the sealed stream to reach the medium",
    );
    assert!(
        alice.drops.lock().expect("drops").is_empty(),
        "the router refused to send over a sealed link: {:?}",
        alice.drops.lock().expect("drops")
    );
    let wire = wire.lock().expect("wire");
    for raw in wire.iter() {
        assert!(
            raw.len() <= MAX_FRAME,
            "a sealed envelope of {} bytes exceeds the {MAX_FRAME}-byte medium",
            raw.len()
        );
    }
}
