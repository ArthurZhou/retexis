//! Flood storm suppression.
//!
//! Flooding is what makes a mesh work without a topology database, and it is
//! also what melts a dense mesh: every broadcast is forwarded by every node to
//! every link, so one message costs O(E) transmissions and the total work
//! grows with the *product* of message rate and edge count.
//!
//! Duplicate suppression (see [`crate::dedup`]) already caps a single message
//! at one processing per node. What it does not cap is *how many distinct
//! messages a node relays per second*. That is what this module does: a token
//! bucket for relayed broadcasts, plus a per-source bucket so one noisy node
//! cannot spend everyone else's budget.
//!
//! Deliberately simple, and deliberately local: no coordination, no consensus,
//! no per-neighbour feedback. Each node independently decides how much relay
//! work it will do. Broadcast is best effort by definition, so dropping excess
//! relay work degrades gracefully instead of failing.

use retexis_core::NodeId;
use std::collections::HashMap;
use std::time::Instant;

#[derive(Clone, Copy, Debug)]
pub struct FloodConfig {
    /// Relayed broadcasts per second this node is willing to forward.
    pub rate_per_sec: f64,
    /// How many can burst at once (smooths the start of a storm).
    pub burst: f64,
    /// Relayed broadcasts per second accepted from any single source.
    pub per_source_rate: f64,
    pub per_source_burst: f64,
    /// Upper bound on tracked sources, so a spoofing attacker cannot grow the
    /// map without limit.
    pub max_sources: usize,
}

impl Default for FloodConfig {
    fn default() -> Self {
        FloodConfig {
            rate_per_sec: 200.0,
            burst: 100.0,
            per_source_rate: 40.0,
            per_source_burst: 30.0,
            max_sources: 512,
        }
    }
}

impl FloodConfig {
    /// Effectively disables suppression (for tests and for tiny meshes where
    /// you want to measure the storm).
    pub fn unlimited() -> Self {
        FloodConfig {
            rate_per_sec: f64::INFINITY,
            burst: f64::INFINITY,
            per_source_rate: f64::INFINITY,
            per_source_burst: f64::INFINITY,
            max_sources: 0,
        }
    }

    pub fn enabled(&self) -> bool {
        self.rate_per_sec.is_finite()
    }
}

#[derive(Debug)]
struct Bucket {
    tokens: f64,
    last: Instant,
}

impl Bucket {
    fn full(burst: f64, now: Instant) -> Self {
        Bucket {
            tokens: burst,
            last: now,
        }
    }

    fn take(&mut self, rate: f64, burst: f64, now: Instant) -> bool {
        if !rate.is_finite() {
            return true;
        }
        let elapsed = now.saturating_duration_since(self.last).as_secs_f64();
        self.tokens = (self.tokens + elapsed * rate).min(burst);
        self.last = now;
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

#[derive(Debug)]
pub struct FloodLimiter {
    cfg: FloodConfig,
    global: Bucket,
    per_source: HashMap<NodeId, Bucket>,
    suppressed: u64,
}

impl FloodLimiter {
    pub fn new(cfg: FloodConfig) -> Self {
        FloodLimiter {
            cfg,
            global: Bucket::full(cfg.burst, Instant::now()),
            per_source: HashMap::new(),
            suppressed: 0,
        }
    }

    /// May this node relay one more broadcast from `src` right now?
    pub fn allow(&mut self, src: NodeId, now: Instant) -> bool {
        if !self.cfg.enabled() {
            return true;
        }
        if !self.global.take(self.cfg.rate_per_sec, self.cfg.burst, now) {
            self.suppressed += 1;
            return false;
        }

        if self.per_source.len() >= self.cfg.max_sources && !self.per_source.contains_key(&src) {
            // Evict sources that have gone quiet; if they all are active, drop
            // the whole map. Losing accounting is better than unbounded growth.
            let stale: Vec<NodeId> = self
                .per_source
                .iter()
                .filter(|(_, b)| now.saturating_duration_since(b.last).as_secs_f64() > 5.0)
                .map(|(k, _)| *k)
                .collect();
            if stale.is_empty() {
                self.per_source.clear();
            } else {
                for k in stale {
                    self.per_source.remove(&k);
                }
            }
        }

        let bucket = self
            .per_source
            .entry(src)
            .or_insert_with(|| Bucket::full(self.cfg.per_source_burst, now));
        let ok = bucket.take(self.cfg.per_source_rate, self.cfg.per_source_burst, now);
        if !ok {
            self.suppressed += 1;
        }
        ok
    }

    pub fn suppressed(&self) -> u64 {
        self.suppressed
    }

    pub fn tracked_sources(&self) -> usize {
        self.per_source.len()
    }

    pub fn config(&self) -> FloodConfig {
        self.cfg
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn bucket_allows_a_burst_then_throttles() {
        let mut b = Bucket::full(3.0, Instant::now());
        let t = Instant::now();
        assert!(b.take(10.0, 3.0, t));
        assert!(b.take(10.0, 3.0, t));
        assert!(b.take(10.0, 3.0, t));
        assert!(!b.take(10.0, 3.0, t), "burst is exhausted");
        assert!(
            b.take(10.0, 3.0, t + Duration::from_millis(200)),
            "refills at the configured rate"
        );
    }

    #[test]
    fn unlimited_config_never_blocks() {
        let mut l = FloodLimiter::new(FloodConfig::unlimited());
        let src = NodeId::random();
        for _ in 0..10_000 {
            assert!(l.allow(src, Instant::now()));
        }
        assert_eq!(l.suppressed(), 0);
    }

    #[test]
    fn per_source_budget_isolates_a_noisy_node() {
        let cfg = FloodConfig {
            rate_per_sec: 10_000.0,
            burst: 10_000.0,
            per_source_rate: 5.0,
            per_source_burst: 5.0,
            max_sources: 16,
        };
        let mut l = FloodLimiter::new(cfg);
        let noisy = NodeId::random();
        let quiet = NodeId::random();
        let t = Instant::now();

        for _ in 0..5 {
            assert!(l.allow(noisy, t));
        }
        assert!(!l.allow(noisy, t), "noisy node exhausted its own budget");
        assert!(
            l.allow(quiet, t),
            "a quiet node is unaffected by a noisy one"
        );
    }

    #[test]
    fn global_budget_caps_total_work() {
        let cfg = FloodConfig {
            rate_per_sec: 100.0,
            burst: 10.0,
            per_source_rate: 1000.0,
            per_source_burst: 1000.0,
            max_sources: 64,
        };
        let mut l = FloodLimiter::new(cfg);
        let t = Instant::now();
        let mut allowed = 0;
        for i in 0..1000 {
            let src = NodeId::random();
            if l.allow(src, t) {
                allowed += 1;
            }
            let _ = i;
        }
        assert_eq!(allowed, 10, "burst is the hard cap at t=0");
        assert_eq!(l.suppressed(), 990);
    }

    #[test]
    fn source_table_is_bounded() {
        let cfg = FloodConfig {
            per_source_rate: 1e9,
            per_source_burst: 1e9,
            max_sources: 8,
            ..FloodConfig::default()
        };
        let mut l = FloodLimiter::new(cfg);
        let t = Instant::now();
        for _ in 0..1000 {
            l.allow(NodeId::random(), t);
        }
        assert!(l.tracked_sources() <= 8, "map stayed bounded");
    }
}
