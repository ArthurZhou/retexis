//! retexis-app — the transport layer.
//!
//! The routing layer moves opaque bytes between node ids. This layer decides
//! what those bytes *mean*, and it does so per **channel**. A channel is a
//! short name carried in every envelope (`"chat"`, `"file"`, `"pty"`), so
//! several transports can share one mesh without negotiating anything.
//!
//! Adding binary or streaming later means adding a [`Transport`] impl and a
//! channel name — the envelope already has `Binary` / `StreamChunk` content
//! types and the router never inspects payloads.

#![forbid(unsafe_code)]

pub mod chat;
pub mod term;

pub use chat::{send_reliable, send_text, ChatTransport, CHAT_CHANNEL};
pub use term::{line, lines};

use retexis_routing::{Delivery, MeshHandle};

/// One application protocol riding on the mesh.
pub trait Transport: Send {
    fn name(&self) -> &'static str;

    /// Channels this transport claims. Deliveries on other channels are not
    /// passed to it.
    fn channels(&self) -> &'static [&'static str];

    fn on_delivery(&mut self, handle: &MeshHandle, delivery: &Delivery);

    /// Optional periodic hook (progress bars, keepalives, stream timeouts).
    fn on_tick(&mut self, _handle: &MeshHandle) {}
}

/// Routes deliveries to whichever transport claims the channel.
#[derive(Default)]
pub struct TransportHost {
    transports: Vec<Box<dyn Transport>>,
}

impl TransportHost {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, t: Box<dyn Transport>) {
        self.transports.push(t);
    }

    pub fn names(&self) -> Vec<&'static str> {
        self.transports.iter().map(|t| t.name()).collect()
    }

    pub fn channels(&self) -> Vec<&'static str> {
        self.transports
            .iter()
            .flat_map(|t| t.channels().iter().copied())
            .collect()
    }

    /// Returns `false` when nobody wanted the message, which the CLI reports
    /// so a misconfigured channel does not fail silently.
    pub fn dispatch(&mut self, handle: &MeshHandle, delivery: &Delivery) -> bool {
        let mut claimed = false;
        for t in &mut self.transports {
            if t.channels().iter().any(|c| *c == delivery.channel) {
                t.on_delivery(handle, delivery);
                claimed = true;
            }
        }
        claimed
    }

    pub fn tick(&mut self, handle: &MeshHandle) {
        for t in &mut self.transports {
            t.on_tick(handle);
        }
    }
}
