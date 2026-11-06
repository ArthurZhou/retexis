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
    ///
    /// A duplicate does **not** refresh the timestamp. The window is "how long
    /// after first sight a repeat is still a repeat", and extending it on every
    /// repeat would let a replayed id live forever — which is exactly what an
    /// attacker holding one captured packet would do to keep a slot, and what
    /// a stale duplicate needs in order to slip through after the real window
    /// has closed.
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
        match self.seen.entry(id) {
            std::collections::hash_map::Entry::Occupied(_) => DedupOutcome::Duplicate,
            std::collections::hash_map::Entry::Vacant(slot) => {
                slot.insert(now);
                DedupOutcome::New
            }
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

    #[test]
    fn replaying_a_packet_does_not_extend_its_window() {
        // One captured packet, replayed forever: the entry must still expire
        // on schedule from the *first* sight, or the replay pins the cache.
        let mut c = DedupCache::new(Duration::from_secs(1));
        let id = MessageId::random();
        let t = Instant::now();
        assert!(c.is_new(id, t));
        assert_eq!(c.mark(id, t + Duration::from_millis(900)), DedupOutcome::Duplicate);
        c.sweep(t + Duration::from_millis(1100));
        assert!(
            c.is_empty(),
            "a duplicate refreshed the timestamp and kept the id alive"
        );
    }
}
