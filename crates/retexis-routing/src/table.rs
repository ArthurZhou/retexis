//! Distance-vector routing table.
//!
//! Each node stores, per destination, the neighbour link to hand the packet
//! to and the number of hops it believes that costs. That is a classic
//! distance vector (Bellman-Ford), which is the right shape for a mesh:
//! no central authority, no global topology, converges by gossip.
//!
//! Loop control:
//! * **Split horizon** — never advertise a route back out of the link we
//!   learned it from.
//! * **TTL** — every data packet carries a hop budget, so a stale route can
//!   only ever waste a bounded number of hops.
//! * **Dedup** — see [`crate::dedup`]; a flooded packet is processed once per
//!   node no matter how many paths reach it.

use retexis_core::{LinkId, NodeId};
use std::collections::HashMap;
use std::time::{Duration, Instant};

pub const INFINITY: u8 = u8::MAX;

/// Upper bound on remembered destinations.
///
/// Every entry in this table was supplied by a peer: a `route` advertisement
/// is gossip, and whoever sends it decides which node ids it names. Without a
/// ceiling, one peer that mints fresh ids per packet grows the table — and
/// every advertisement we then send — until the process dies. 2048 is far more
/// than a distance-vector mesh of this size converges to, and the cap turns a
/// memory exhaustion into "we ignore destinations we cannot hear about".
pub const MAX_ROUTES: usize = 2048;

#[derive(Debug, Clone, Copy)]
pub struct RouteEntry {
    pub next_hop: LinkId,
    pub metric: u8,
    pub last_seen: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LearnOutcome {
    /// Worse than or equal to what we already had; nothing changed.
    Ignored,
    /// Same next hop, same metric — just a keepalive refresh.
    Refreshed,
    /// First time we hear about this destination.
    New,
    /// A strictly shorter path was found.
    Improved { old: u8 },
    /// The table is at [`MAX_ROUTES`]; a *new* destination was dropped.
    Full,
}

#[derive(Debug, Default)]
pub struct RoutingTable {
    routes: HashMap<NodeId, RouteEntry>,
}

impl RoutingTable {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.routes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.routes.is_empty()
    }

    pub fn get(&self, dst: &NodeId) -> Option<&RouteEntry> {
        self.routes.get(dst)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&NodeId, &RouteEntry)> {
        self.routes.iter()
    }

    /// A directly connected peer is always one hop away; this wins over
    /// anything learned by gossip.
    ///
    /// Adjacency bypasses [`MAX_ROUTES`] on purpose: the node on the other end
    /// of a link is not a claim, it is a peer we are exchanging Hello with, and
    /// the number of them is bounded by the number of links we can open.
    pub fn set_adjacent(&mut self, node: NodeId, link: LinkId, now: Instant) -> bool {
        match self.routes.get_mut(&node) {
            Some(e) => {
                let changed = e.next_hop != link || e.metric != 1;
                e.next_hop = link;
                e.metric = 1;
                e.last_seen = now;
                changed
            }
            None => {
                self.routes.insert(
                    node,
                    RouteEntry {
                        next_hop: link,
                        metric: 1,
                        last_seen: now,
                    },
                );
                true
            }
        }
    }

    /// Applies one advertised `(destination, metric)` pair that arrived over
    /// `via`, where `metric` already includes the hop to the advertiser.
    pub fn learn(&mut self, dst: NodeId, metric: u8, via: LinkId, now: Instant) -> LearnOutcome {
        if metric == INFINITY {
            return LearnOutcome::Ignored;
        }
        match self.routes.get_mut(&dst) {
            None => {
                if self.routes.len() >= MAX_ROUTES {
                    return LearnOutcome::Full;
                }
                self.routes.insert(
                    dst,
                    RouteEntry {
                        next_hop: via,
                        metric,
                        last_seen: now,
                    },
                );
                LearnOutcome::New
            }
            Some(e) => {
                if e.next_hop == via {
                    let improved = metric < e.metric;
                    let old = e.metric;
                    e.metric = metric;
                    e.last_seen = now;
                    if improved {
                        LearnOutcome::Improved { old }
                    } else {
                        LearnOutcome::Refreshed
                    }
                } else if metric < e.metric {
                    let old = e.metric;
                    e.next_hop = via;
                    e.metric = metric;
                    e.last_seen = now;
                    LearnOutcome::Improved { old }
                } else {
                    LearnOutcome::Ignored
                }
            }
        }
    }

    pub fn remove(&mut self, dst: &NodeId) -> Option<RouteEntry> {
        self.routes.remove(dst)
    }

    /// Drops routes that have not been refreshed within `ttl`.
    pub fn expire(&mut self, now: Instant, ttl: Duration) -> Vec<NodeId> {
        let mut dead = Vec::new();
        self.routes.retain(|dst, e| {
            if now.saturating_duration_since(e.last_seen) > ttl {
                dead.push(*dst);
                false
            } else {
                true
            }
        });
        dead
    }

    /// Drops every route that pointed at a link that just went down.
    pub fn drop_via(&mut self, link: LinkId) -> Vec<NodeId> {
        let mut dead = Vec::new();
        self.routes.retain(|dst, e| {
            if e.next_hop == link {
                dead.push(*dst);
                false
            } else {
                true
            }
        });
        dead
    }

    /// Builds the advertisement we send out of `for_link`: ourselves at
    /// metric 0, plus everything except the routes that point back out of
    /// that same link (split horizon).
    pub fn update_for(&self, me: NodeId, for_link: Option<LinkId>) -> Vec<(NodeId, u8)> {
        let mut out = Vec::with_capacity(self.routes.len() + 1);
        out.push((me, 0));
        for (dst, e) in &self.routes {
            if *dst == me {
                continue;
            }
            if Some(e.next_hop) == for_link {
                continue;
            }
            if e.metric == INFINITY {
                continue;
            }
            out.push((*dst, e.metric));
        }
        out.sort_by_key(|(_, m)| *m);
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn now() -> Instant {
        Instant::now()
    }

    #[test]
    fn adjacent_beats_learned() {
        let mut t = RoutingTable::new();
        let a = NodeId::random();
        assert!(t.set_adjacent(a, 1, now()));
        assert_eq!(t.get(&a).unwrap().metric, 1);
        t.learn(a, 5, 2, now());
        assert_eq!(t.get(&a).unwrap().next_hop, 1, "direct link must win");
    }

    #[test]
    fn shorter_path_replaces_longer() {
        let mut t = RoutingTable::new();
        let dst = NodeId::random();
        assert_eq!(t.learn(dst, 4, 10, now()), LearnOutcome::New);
        assert!(matches!(
            t.learn(dst, 2, 11, now()),
            LearnOutcome::Improved { old: 4 }
        ));
        assert_eq!(t.get(&dst).unwrap().next_hop, 11);
    }

    #[test]
    fn longer_path_does_not_replace_shorter() {
        let mut t = RoutingTable::new();
        let dst = NodeId::random();
        t.learn(dst, 2, 10, now());
        assert_eq!(t.learn(dst, 3, 11, now()), LearnOutcome::Ignored);
        assert_eq!(t.get(&dst).unwrap().next_hop, 10);
    }

    #[test]
    fn split_horizon_hides_the_route_we_came_from() {
        let mut t = RoutingTable::new();
        let me = NodeId::random();
        let far = NodeId::random();
        let near = NodeId::random();
        t.learn(far, 2, 7, now());
        t.set_adjacent(near, 8, now());
        let for_link_7 = t.update_for(me, Some(7));
        assert!(!for_link_7.iter().any(|(d, _)| *d == far));
        assert!(for_link_7.iter().any(|(d, _)| *d == near));
        assert!(for_link_7.iter().any(|(d, m)| *d == me && *m == 0));
    }

    #[test]
    fn a_worse_metric_from_the_same_hop_still_wins() {
        // The neighbour's own path may have got longer; refusing that would
        // keep a distance that no longer exists.
        let mut t = RoutingTable::new();
        let dst = NodeId::random();
        t.learn(dst, 2, 10, now());
        t.learn(dst, 5, 10, now());
        assert_eq!(t.get(&dst).unwrap().metric, 5);
    }

    #[test]
    fn the_table_has_a_ceiling() {
        let mut t = RoutingTable::new();
        for _ in 0..MAX_ROUTES {
            assert_eq!(t.learn(NodeId::random(), 2, 1, now()), LearnOutcome::New);
        }
        let extra = NodeId::random();
        assert_eq!(t.learn(extra, 1, 1, now()), LearnOutcome::Full);
        assert_eq!(t.len(), MAX_ROUTES, "the table did not grow");
        assert!(t.get(&extra).is_none());

        // A destination already in the table can still be updated: the cap
        // bounds growth, it does not freeze the table.
        let known = *t.iter().next().map(|(d, _)| d).unwrap();
        assert!(matches!(
            t.learn(known, 1, 9, now()),
            LearnOutcome::Improved { .. }
        ));
    }

    #[test]
    fn adjacency_is_not_blocked_by_the_ceiling() {
        // A peer we are actually talking to is not a gossip claim.
        let mut t = RoutingTable::new();
        for _ in 0..MAX_ROUTES {
            t.learn(NodeId::random(), 3, 1, now());
        }
        let neighbour = NodeId::random();
        assert!(t.set_adjacent(neighbour, 77, now()));
        assert_eq!(t.get(&neighbour).unwrap().metric, 1);
    }

    #[test]
    fn expire_and_drop_via() {
        let mut t = RoutingTable::new();
        let a = NodeId::random();
        let b = NodeId::random();
        t.learn(a, 2, 1, now());
        t.learn(b, 2, 2, now());
        assert_eq!(t.drop_via(1), vec![a]);
        assert_eq!(t.len(), 1);
        let dead = t.expire(
            Instant::now() + Duration::from_secs(60),
            Duration::from_secs(5),
        );
        assert_eq!(dead, vec![b]);
        assert!(t.is_empty());
    }
}
