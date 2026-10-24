//! One dial path, shared by every application.
//!
//! `--connect`, discovery results and the GUI's "connect" button all need the
//! same two things, and getting either wrong is a bug that only shows up on a
//! live mesh:
//!
//! * **which medium serves a URL** — resolved from the registry, so
//!   `udp://...` never silently dials over TCP. That bug existed once: the
//!   dial path used to construct a `TcpFactory` directly, which was invisible
//!   while TCP was the only medium.
//! * **which URLs are already dialling** — discovery can find a peer that was
//!   also given explicitly, and two dialers to the same address would fight
//!   over the duplicate-link rule.
//!
//! It lives here rather than in an application because the CLI and the GUI are
//! both applications, and a second copy of this logic would be a second copy
//! of the bug.
//!
//! Nothing here is protocol logic: it looks up a factory, builds a [`Dialer`],
//! and hands the result to the router.

use crate::dial::{Dialer, ReconnectConfig};
use crate::{MeshHandle, NoticeSink};
use retexis_core::TransportRegistry;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

pub struct Connector {
    registry: Arc<TransportRegistry>,
    notices: NoticeSink,
    cfg: ReconnectConfig,
    seen: Mutex<HashSet<String>>,
}

impl Connector {
    pub fn new(
        registry: Arc<TransportRegistry>,
        notices: NoticeSink,
        cfg: ReconnectConfig,
    ) -> Self {
        Connector {
            registry,
            notices,
            cfg,
            seen: Mutex::new(HashSet::new()),
        }
    }

    /// How many URLs this connector has dialled (successfully or not).
    pub fn attempted(&self) -> usize {
        self.seen.lock().unwrap_or_else(|e| e.into_inner()).len()
    }

    pub fn supervised(&self) -> bool {
        self.cfg.enabled
    }

    /// URLs this connector has been asked to dial, in no particular order.
    pub fn urls(&self) -> Vec<String> {
        let mut out: Vec<String> = self
            .seen
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .cloned()
            .collect();
        out.sort();
        out
    }

    /// Starts one dialer. Returns `Err` only for a bad URL or an unknown
    /// scheme; a peer that is merely unreachable is a runtime concern, and the
    /// dialer reports it through notices.
    pub fn dial(&self, handle: &MeshHandle, url: &str) -> Result<bool, String> {
        // Resolve first: a URL with no medium must not be remembered, or a
        // typo would poison that address until the process restarts.
        let factory = self.registry.factory_for(url).ok_or_else(|| {
            format!(
                "no medium for '{url}' (have: {})",
                self.registry.schemes().join(", ")
            )
        })?;
        {
            let mut seen = self.seen.lock().unwrap_or_else(|e| e.into_inner());
            if !seen.insert(url.to_string()) {
                return Ok(false); // already dialling it
            }
        }
        let dialer = match Dialer::new(factory, url) {
            Ok(d) => d,
            Err(e) => {
                self.seen
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .remove(url);
                return Err(e);
            }
        };
        dialer
            .with_config(self.cfg)
            .spawn(handle.clone(), Arc::clone(&self.notices));
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::start;
    use crate::Delivery;
    use std::sync::mpsc::Receiver;
    use std::time::Duration;

    /// The receiver is returned so it stays alive: dropping it would make the
    /// router's `send` fail (harmlessly, but noisily in a debugger).
    fn mesh() -> (MeshHandle, Receiver<Delivery>) {
        let (tx, rx) = std::sync::mpsc::channel();
        let cfg = crate::RouterConfig {
            node_name: "test".into(),
            ..Default::default()
        };
        let (handle, _thread) = start(cfg, tx, crate::silent());
        (handle, rx)
    }

    fn connector(registry: TransportRegistry) -> Connector {
        Connector::new(
            Arc::new(registry),
            crate::silent(),
            ReconnectConfig {
                enabled: false,
                ..Default::default()
            },
        )
    }

    #[test]
    fn an_unknown_scheme_names_the_ones_that_exist() {
        let connector = connector(TransportRegistry::new());
        let (handle, _rx) = mesh();
        let err = connector.dial(&handle, "carrier-pigeon://x:1").unwrap_err();
        assert!(err.contains("no medium"), "{err}");
    }

    #[test]
    fn a_bare_address_is_not_a_url() {
        let connector = connector(TransportRegistry::new());
        let (handle, _rx) = mesh();
        let err = connector.dial(&handle, "127.0.0.1:7100").unwrap_err();
        assert!(
            err.contains("no medium"),
            "bare host:port has no scheme: {err}"
        );
    }

    #[test]
    fn dialling_the_same_url_twice_is_a_no_op() {
        let mut registry = TransportRegistry::new();
        registry.register(Arc::new(retexis_link_tcp::TcpFactory::new()));
        let connector = connector(registry);
        let (handle, _rx) = mesh();
        // Nothing is listening; the point is that the second call is skipped.
        assert!(connector.dial(&handle, "tcp://127.0.0.1:1").unwrap());
        assert!(!connector.dial(&handle, "tcp://127.0.0.1:1").unwrap());
        assert_eq!(connector.attempted(), 1);
        assert_eq!(connector.urls(), vec!["tcp://127.0.0.1:1".to_string()]);
        std::thread::sleep(Duration::from_millis(50));
    }

    #[test]
    fn a_rejected_url_can_be_retried() {
        // The first attempt fails before a dialer exists, so the URL must not
        // be remembered: otherwise a typo would poison the address forever.
        let connector = connector(TransportRegistry::new());
        let (handle, _rx) = mesh();
        assert!(connector.dial(&handle, "nope://x:1").is_err());
        assert!(connector.dial(&handle, "nope://x:1").is_err());
        assert_eq!(connector.attempted(), 0, "a rejected URL is not 'seen'");
    }
}
