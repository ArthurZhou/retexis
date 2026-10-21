//! Plain TCP as a retexis medium.
//!
//! TCP is a byte stream, not a message stream, so this link owns the framing:
//!
//! ```text
//!   +--------+----------------------------+
//!   | u32 BE |  encoded envelope (N bytes) |
//!   +--------+----------------------------+
//!     length N
//! ```
//!
//! One writer mutex per link keeps frames from interleaving; one reader thread
//! per link drains `recv`. `TCP_NODELAY` is on because mesh chat is latency
//! sensitive and our frames are already batched by the application.
//!
//! Swap this crate out for `retexis-link-ws` / `-http` / `-radio` and the
//! router never notices.

#![forbid(unsafe_code)]

use retexis_core::{next_link_id, Link, LinkAcceptor, LinkError, LinkFactory, LinkId};
use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use std::time::Duration;

/// Upper bound for one frame on this medium. Kept just above the envelope
/// maximum so a legal envelope always fits, while a corrupt length prefix
/// cannot make us allocate gigabytes.
pub const MAX_FRAME: usize = 1 << 20;

/// Length prefix size in bytes.
const PREFIX: usize = 4;

#[derive(Clone, Copy, Debug)]
pub struct TcpConfig {
    /// Socket read timeout. The reader thread wakes up this often to observe
    /// the `closed` flag, which is the only portable way to interrupt a
    /// blocking `recv` on Windows (`shutdown` does not reliably wake it).
    /// A few hundred milliseconds of idle wakeups is free compared to the
    /// cost of a hung thread.
    pub read_timeout: Option<Duration>,
    pub nodelay: bool,
}

impl Default for TcpConfig {
    fn default() -> Self {
        TcpConfig {
            read_timeout: Some(Duration::from_millis(250)),
            nodelay: true,
        }
    }
}

pub struct TcpLink {
    id: LinkId,
    peer: String,
    initiator: bool,
    writer: Mutex<TcpStream>,
    reader: TcpStream,
    /// Bytes already pulled off the socket but not yet handed to `recv`.
    /// Only one thread ever calls `recv`, but the buffer lives behind a mutex
    /// because `recv` takes `&self`.
    rbuf: Mutex<Vec<u8>>,
    closed: AtomicBool,
}

impl TcpLink {
    pub fn new(
        stream: TcpStream,
        peer: impl Into<String>,
        cfg: TcpConfig,
        initiator: bool,
    ) -> std::io::Result<Self> {
        stream.set_nodelay(cfg.nodelay)?;
        if let Some(t) = cfg.read_timeout {
            stream.set_read_timeout(Some(t))?;
        }
        let reader = stream.try_clone()?;
        Ok(TcpLink {
            id: next_link_id(),
            peer: peer.into(),
            initiator,
            writer: Mutex::new(stream),
            reader,
            rbuf: Mutex::new(Vec::new()),
            closed: AtomicBool::new(false),
        })
    }

    fn ensure_open(&self) -> Result<(), LinkError> {
        if self.closed.load(Ordering::Acquire) {
            Err(LinkError::Closed)
        } else {
            Ok(())
        }
    }
}

impl Link for TcpLink {
    fn id(&self) -> LinkId {
        self.id
    }

    fn transport(&self) -> &'static str {
        "tcp"
    }

    fn peer(&self) -> String {
        self.peer.clone()
    }

    fn initiator(&self) -> bool {
        self.initiator
    }

    fn max_frame(&self) -> usize {
        MAX_FRAME
    }

    fn send(&self, envelope: &[u8]) -> Result<(), LinkError> {
        self.ensure_open()?;
        if envelope.len() > MAX_FRAME {
            return Err(LinkError::FrameTooLarge {
                len: envelope.len(),
                max: MAX_FRAME,
            });
        }
        let mut frame = Vec::with_capacity(PREFIX + envelope.len());
        frame.extend_from_slice(&(envelope.len() as u32).to_be_bytes());
        frame.extend_from_slice(envelope);

        let mut w = self.writer.lock().unwrap_or_else(|e| e.into_inner());
        // One write_all for the whole frame: fewer syscalls and no window
        // where the prefix is visible without its body.
        w.write_all(&frame)?;
        w.flush()?;
        Ok(())
    }

    fn recv(&self) -> Result<Vec<u8>, LinkError> {
        loop {
            self.ensure_open()?;

            // 1. Can we satisfy the request from what we already buffered?
            {
                let mut buf = self.rbuf.lock().unwrap_or_else(|e| e.into_inner());
                if buf.len() >= PREFIX {
                    let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
                    if len == 0 {
                        return Err(LinkError::Framing("zero-length frame".into()));
                    }
                    if len > MAX_FRAME {
                        return Err(LinkError::FrameTooLarge {
                            len,
                            max: MAX_FRAME,
                        });
                    }
                    if buf.len() >= PREFIX + len {
                        let frame = buf[PREFIX..PREFIX + len].to_vec();
                        buf.drain(..PREFIX + len);
                        return Ok(frame);
                    }
                }
            }

            // 2. Pull more bytes. TCP gives us a stream, not messages, so a
            //    single read can return half a frame or several frames; the
            //    buffer above handles both.
            let mut chunk = [0u8; 16 * 1024];
            match (&self.reader).read(&mut chunk) {
                Ok(0) => return Err(LinkError::Closed),
                Ok(n) => self
                    .rbuf
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .extend_from_slice(&chunk[..n]),
                Err(e)
                    if matches!(
                        e.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                    ) =>
                {
                    continue; // idle: loop, re-check `closed`, try again
                }
                Err(e) => return Err(LinkError::from(e)),
            }
        }
    }

    fn shutdown(&self) {
        if !self.closed.swap(true, Ordering::AcqRel) {
            let _ = self.reader.shutdown(Shutdown::Both);
            if let Ok(w) = self.writer.lock() {
                let _ = w.shutdown(Shutdown::Both);
            }
        }
    }
}

impl Drop for TcpLink {
    fn drop(&mut self) {
        self.shutdown();
    }
}

pub struct TcpAcceptor {
    listener: TcpListener,
    local_addr: String,
    cfg: TcpConfig,
    closed: AtomicBool,
}

impl TcpAcceptor {
    pub fn bind(addr: &str, cfg: TcpConfig) -> Result<Self, LinkError> {
        let listener = TcpListener::bind(addr)?;
        let local_addr = listener
            .local_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|_| addr.to_string());
        Ok(TcpAcceptor {
            listener,
            local_addr,
            cfg,
            closed: AtomicBool::new(false),
        })
    }
}

impl LinkAcceptor for TcpAcceptor {
    fn transport(&self) -> &'static str {
        "tcp"
    }

    fn local_addr(&self) -> String {
        self.local_addr.clone()
    }

    fn accept(&self) -> Result<Box<dyn Link>, LinkError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(LinkError::Closed);
        }
        let (stream, addr) = self.listener.accept()?;
        let link = TcpLink::new(stream, addr.to_string(), self.cfg, false)?;
        Ok(Box::new(link))
    }

    fn shutdown(&self) {
        self.closed.store(true, Ordering::Release);
        // Closing a TcpListener from another thread is not portable, so we
        // just mark it closed; the accept loop exits on its own error path.
    }
}

/// Factory entry point for the transport registry.
#[derive(Clone, Copy, Debug, Default)]
pub struct TcpFactory {
    pub cfg: TcpConfig,
}

impl TcpFactory {
    pub fn new() -> Self {
        Self::default()
    }
}

impl LinkFactory for TcpFactory {
    fn scheme(&self) -> &'static str {
        "tcp"
    }

    fn listen(&self, addr: &str) -> Result<Box<dyn LinkAcceptor>, LinkError> {
        Ok(Box::new(TcpAcceptor::bind(addr, self.cfg)?))
    }

    fn connect(&self, addr: &str) -> Result<Box<dyn Link>, LinkError> {
        let stream = TcpStream::connect(addr)?;
        let peer = stream
            .peer_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|_| addr.to_string());
        Ok(Box::new(TcpLink::new(stream, peer, self.cfg, true)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn frames_survive_a_roundtrip() {
        let acc = TcpAcceptor::bind("127.0.0.1:0", TcpConfig::default()).unwrap();
        let addr = acc.local_addr();
        let acc = Arc::new(acc);

        let server = {
            let acc = Arc::clone(&acc);
            thread::spawn(move || {
                let link = acc.accept().unwrap();
                let got = link.recv().unwrap();
                link.send(b"pong").unwrap();
                got
            })
        };

        let client = TcpFactory::new().connect(&addr).unwrap();
        client.send(b"ping").unwrap();
        assert_eq!(client.recv().unwrap(), b"pong");
        assert_eq!(server.join().unwrap(), b"ping");
    }

    #[test]
    fn oversized_frame_is_refused_locally() {
        let acc = TcpAcceptor::bind("127.0.0.1:0", TcpConfig::default()).unwrap();
        let addr = acc.local_addr();
        let client = TcpFactory::new().connect(&addr).unwrap();
        let err = client.send(&vec![0u8; MAX_FRAME + 1]).unwrap_err();
        assert!(matches!(err, LinkError::FrameTooLarge { .. }));
    }

    #[test]
    fn shutdown_unblocks_recv() {
        let acc = TcpAcceptor::bind("127.0.0.1:0", TcpConfig::default()).unwrap();
        let addr = acc.local_addr();
        let client = TcpFactory::new().connect(&addr).unwrap();
        let link = Arc::new(client);
        let l2 = Arc::clone(&link);
        let reader = thread::spawn(move || l2.recv().is_err());
        thread::sleep(Duration::from_millis(50));
        link.shutdown();
        assert!(reader.join().unwrap());
    }

    /// TCP is a byte stream: two frames may arrive in one read, and one frame
    /// may arrive in two reads. Both must be reconstructed correctly.
    #[test]
    fn handles_pipelined_and_partial_frames() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let link = TcpFactory::new().connect(&addr).unwrap();
        let (mut sock, _) = listener.accept().unwrap();

        let mut wire = Vec::new();
        for body in [b"one".as_slice(), b"two".as_slice()] {
            wire.extend_from_slice(&(body.len() as u32).to_be_bytes());
            wire.extend_from_slice(body);
        }
        sock.write_all(&wire).unwrap();
        sock.flush().unwrap();

        assert_eq!(link.recv().unwrap(), b"one");
        assert_eq!(link.recv().unwrap(), b"two");

        let body = b"three";
        let mut frame = Vec::new();
        frame.extend_from_slice(&(body.len() as u32).to_be_bytes());
        frame.extend_from_slice(body);
        sock.write_all(&frame[..3]).unwrap();
        sock.flush().unwrap();
        thread::sleep(Duration::from_millis(30));
        sock.write_all(&frame[3..]).unwrap();
        sock.flush().unwrap();

        assert_eq!(link.recv().unwrap(), b"three");
    }
}
