//! Duplicate suppression.
//!
//! A mesh has cycles. Flooding without dedup would multiply packets by the
//! number of paths, so every node remembers the message ids it has already
//! handled for a while and drops repeats.
//!
//! This is what makes broadcast safe and what makes unicast forwarding
//! loop-free even when the routing table is stale.

use retexis_core::MessageId;
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Upper bound on tracked ids; beyond this we sweep eagerly.
const SOFT_CAP: usize = 16_384;

/// Result of offering a message id to the cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DedupOutcome {
    /// First time we see it: process normally.
    New,
    /// Already handled. For a retransmission this means "re-ack, do not
    /// re-deliver" — see the router.
    Duplicate,
}

#[derive(Debug)]
pub struct DedupCache {
    ttl: Duration,
    seen: HashMap<MessageId, Instant>,
}

impl DedupCache {
    pub fn new(ttl: Duration) -> Self {
        DedupCache {
            ttl,
            seen: HashMap::new(),
        }
    }

    /// Records `id` and reports whether it was already known.
    pub fn mark(&mut self, id: MessageId, now: Instant) -> DedupOutcome {
        if self.seen.len() >= SOFT_CAP {
            self.sweep(now);
            if self.seen.len() >= SOFT_CAP {
                // Still full of live entries: forget the oldest half rather
                // than growing without bound. Worst case a very old duplicate
                // gets re-processed, which the TTL still bounds.
                let mut ages: Vec<(MessageId, Instant)> =
                    self.seen.iter().map(|(k, v)| (*k, *v)).collect();
                ages.sort_by_key(|(_, t)| *t);
                for (k, _) in ages.into_iter().take(SOFT_CAP / 2) {
                    self.seen.remove(&k);
                }
            }
        }
        match self.seen.insert(id, now) {
            None => DedupOutcome::New,
            Some(_) => DedupOutcome::Duplicate,
        }
    }

    /// Convenience wrapper for callers that only care about "is it new".
    pub fn is_new(&mut self, id: MessageId, now: Instant) -> bool {
        self.mark(id, now) == DedupOutcome::New
    }

    /// Forgets ids older than the TTL. Returns how many were dropped.
    pub fn sweep(&mut self, now: Instant) -> usize {
        let ttl = self.ttl;
        let before = self.seen.len();
        self.seen
            .retain(|_, t| now.saturating_duration_since(*t) <= ttl);
        before - self.seen.len()
    }

    pub fn len(&self) -> usize {
        self.seen.len()
    }

    pub fn is_empty(&self) -> bool {
        self.seen.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_sight_is_new() {
        let mut c = DedupCache::new(Duration::from_secs(10));
        let id = MessageId::random();
        let t = Instant::now();
        assert_eq!(c.mark(id, t), DedupOutcome::New);
        assert_eq!(c.mark(id, t), DedupOutcome::Duplicate);
        assert!(!c.is_new(id, t));
    }

    #[test]
    fn expired_ids_are_forgotten() {
        let mut c = DedupCache::new(Duration::from_secs(1));
        let id = MessageId::random();
        let t = Instant::now();
        assert!(c.is_new(id, t));
        c.sweep(t + Duration::from_secs(5));
        assert!(c.is_empty());
    }
}
