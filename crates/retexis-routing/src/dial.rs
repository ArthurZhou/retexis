//! Keeping an outbound link alive.
//!
//! A mesh is only as good as its willingness to reconnect. TCP gives us no
//! notification that the *other end* died until we try to write, and a link
//! that silently went away leaves a hole in the routing table. So every
//! `--connect` target gets a supervisor thread that:
//!
//! 1. dials,
//! 2. watches the link through [`MeshHandle::is_link_up`],
//! 3. on loss, waits an exponentially increasing (jittered) delay and dials
//!    again, forever.
//!
//! The backoff exists so that a mesh of 200 nodes pointed at one dead relay
//! does not turn into a SYN flood. The jitter exists so they do not all retry
//! in lockstep when it comes back.

use crate::{MeshHandle, NoticeSink, RouterNotice};
use retexis_core::{split_url, LinkFactory};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug)]
pub struct ReconnectConfig {
    pub enabled: bool,
    /// First retry delay; doubles each failure.
    pub base: Duration,
    /// Ceiling for the delay.
    pub max: Duration,
    /// How often to check whether the link is still there.
    pub probe: Duration,
}

impl Default for ReconnectConfig {
    fn default() -> Self {
        ReconnectConfig {
            enabled: true,
            base: Duration::from_millis(250),
            max: Duration::from_secs(15),
            probe: Duration::from_millis(200),
        }
    }
}

/// A supervised outbound connection.
pub struct Dialer {
    factory: Box<dyn LinkFactory>,
    url: String,
    addr: String,
    cfg: ReconnectConfig,
}

impl Dialer {
    pub fn new(factory: Box<dyn LinkFactory>, url: &str) -> Result<Self, String> {
        let (_, addr) = split_url(url).map_err(|e| e.to_string())?;
        Ok(Dialer {
            factory,
            url: url.to_string(),
            addr: addr.to_string(),
            cfg: ReconnectConfig::default(),
        })
    }

    pub fn with_config(mut self, cfg: ReconnectConfig) -> Self {
        self.cfg = cfg;
        self
    }

    /// Runs the supervisor. The returned thread lives as long as the mesh.
    pub fn spawn(self, handle: MeshHandle, notices: NoticeSink) -> JoinHandle<()> {
        thread::Builder::new()
            .name(format!("mesh-dial-{}", self.addr))
            .spawn(move || self.run(handle, notices))
            .expect("failed to spawn the dialer thread")
    }

    fn run(self, handle: MeshHandle, notices: NoticeSink) {
        let mut failures: u32 = 0;
        loop {
            if !handle.is_running() {
                return;
            }

            let connected = match self.factory.connect(&self.addr) {
                Ok(link) => match handle.add_link(link) {
                    Ok(link_id) => {
                        failures = 0;
                        notices(RouterNotice::Reconnected {
                            url: self.url.clone(),
                        });
                        // Watch it until it dies or the mesh stops.
                        loop {
                            thread::sleep(self.cfg.probe);
                            if !handle.is_running() {
                                return;
                            }
                            if !handle.is_link_up(link_id) {
                                break;
                            }
                        }
                        false
                    }
                    Err(_) => return, // router is gone
                },
                Err(e) => {
                    notices(RouterNotice::DialFailed {
                        url: self.url.clone(),
                        attempt: failures + 1,
                        error: e.to_string(),
                    });
                    false
                }
            };

            if !self.cfg.enabled {
                return;
            }
            if connected {
                continue;
            }

            failures = failures.saturating_add(1);
            let delay = self.delay(failures);
            notices(RouterNotice::Reconnecting {
                url: self.url.clone(),
                attempt: failures,
                delay_ms: delay.as_millis() as u64,
            });
            thread::sleep(delay);
        }
    }

    /// Exponential backoff with +/-25% jitter.
    fn delay(&self, failures: u32) -> Duration {
        let shift = failures.saturating_sub(1).min(10);
        let base = self.cfg.base.saturating_mul(1u32 << shift);
        let capped = base.min(self.cfg.max);
        let jitter = jitter_permille(self.addr.len() as u64 * 31 + failures as u64);
        capped.mul_f64(jitter)
    }
}

/// Deterministic-ish jitter in 0.75..=1.25 without pulling in an RNG crate.
fn jitter_permille(seed: u64) -> f64 {
    let mut x = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    0.75 + (x % 501) as f64 / 1000.0
}

/// Convenience: connect once, no supervision.
pub fn dial_once(factory: &dyn LinkFactory, url: &str, handle: &MeshHandle) -> Result<(), String> {
    let (_, addr) = split_url(url).map_err(|e| e.to_string())?;
    let link = factory.connect(addr).map_err(|e| e.to_string())?;
    handle.add_link(link).map(|_| ()).map_err(|e| e.to_string())
}

/// Tracks how long a supervisor has been trying, for tests and logs.
#[derive(Debug)]
pub struct AttemptLog {
    pub started: Instant,
    pub attempts: u32,
}

impl AttemptLog {
    pub fn new() -> Self {
        AttemptLog {
            started: Instant::now(),
            attempts: 0,
        }
    }
}

impl Default for AttemptLog {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_grows_and_is_capped() {
        let d = Dialer {
            factory: Box::new(retexis_link_tcp::TcpFactory::new()),
            url: "tcp://x:1".into(),
            addr: "x:1".into(),
            cfg: ReconnectConfig {
                base: Duration::from_millis(100),
                max: Duration::from_millis(800),
                ..Default::default()
            },
        };
        let first = d.delay(1);
        let third = d.delay(3);
        let many = d.delay(30);
        assert!(first.as_millis() >= 75 && first.as_millis() <= 125);
        assert!(third > first);
        assert!(
            many <= Duration::from_millis(1000),
            "capped at max (plus jitter), got {many:?}"
        );
    }

    #[test]
    fn jitter_stays_in_range() {
        for seed in 0..1000 {
            let j = jitter_permille(seed);
            assert!((0.75..=1.25).contains(&j), "jitter {j} out of range");
        }
    }
}
