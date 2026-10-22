//! One dial path, used by `--connect`, by discovery results, and by the REPL.
//!
//! It is worth being a type rather than a function because of the two things
//! it remembers:
//!
//! * **which URLs are already dialling** — discovery can find a peer that was
//!   also given explicitly, and two dialers to the same address would fight
//!   over the duplicate-link rule;
//! * **which medium serves a URL** — resolved from the registry, so
//!   `udp://...` never silently dials over TCP. That bug existed once.
//!
//! Nothing here is protocol logic: it looks up a factory, builds a [`Dialer`],
//! and hands the result to the router.

use retexis_core::TransportRegistry;
use retexis_routing::dial::ReconnectConfig;
use retexis_routing::{Dialer, MeshHandle, NoticeSink};
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

pub struct Connector<'a> {
    registry: &'a TransportRegistry,
    notices: NoticeSink,
    cfg: ReconnectConfig,
    seen: Mutex<HashSet<String>>,
}

impl<'a> Connector<'a> {
    pub fn new(registry: &'a TransportRegistry, notices: NoticeSink, cfg: ReconnectConfig) -> Self {
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

    /// Starts one dialer. Returns `Err` only for a bad URL or an unknown
    /// scheme; a peer that is merely unreachable is a runtime concern, and the
    /// dialer reports it through notices.
    pub fn dial(&self, handle: &MeshHandle, url: &str) -> Result<bool, String> {
        {
            let mut seen = self.seen.lock().unwrap_or_else(|e| e.into_inner());
            if !seen.insert(url.to_string()) {
                return Ok(false); // already dialling it
            }
        }
        let factory = self.registry.factory_for(url).ok_or_else(|| {
            format!(
                "no medium for '{url}' (have: {})",
                self.registry.schemes().join(", ")
            )
        })?;
        let dialer = Dialer::new(factory, url)?;
        dialer
            .with_config(self.cfg)
            .spawn(handle.clone(), Arc::clone(&self.notices));
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use retexis_routing::start;
    use retexis_routing::Delivery;
    use std::sync::mpsc::Receiver;
    use std::time::Duration;

    /// The receiver is returned so it stays alive: dropping it would make the
    /// router's `send` fail (harmlessly, but noisily in a debugger).
    fn mesh() -> (MeshHandle, Receiver<Delivery>) {
        let (tx, rx) = std::sync::mpsc::channel();
        let cfg = retexis_routing::RouterConfig {
            node_name: "test".into(),
            ..Default::default()
        };
        let (handle, _thread) = start(cfg, tx, retexis_routing::silent());
        (handle, rx)
    }

    #[test]
    fn an_unknown_scheme_names_the_ones_that_exist() {
        let registry = TransportRegistry::new();
        let connector = Connector::new(
            &registry,
            retexis_routing::silent(),
            ReconnectConfig {
                enabled: false,
                ..Default::default()
            },
        );
        let (handle, _rx) = mesh();
        let err = connector.dial(&handle, "carrier-pigeon://x:1").unwrap_err();
        assert!(err.contains("no medium"), "{err}");
    }

    #[test]
    fn a_bare_address_is_not_a_url() {
        let registry = TransportRegistry::new();
        let connector = Connector::new(
            &registry,
            retexis_routing::silent(),
            ReconnectConfig::default(),
        );
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
        let connector = Connector::new(
            &registry,
            retexis_routing::silent(),
            ReconnectConfig {
                enabled: false,
                ..Default::default()
            },
        );
        let (handle, _rx) = mesh();
        // Nothing is listening; the point is that the second call is skipped.
        assert_eq!(connector.dial(&handle, "tcp://127.0.0.1:1").unwrap(), true);
        assert_eq!(connector.dial(&handle, "tcp://127.0.0.1:1").unwrap(), false);
        assert_eq!(connector.attempted(), 1);
        std::thread::sleep(Duration::from_millis(50));
    }
}
