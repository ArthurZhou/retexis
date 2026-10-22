//! Asking every medium "who is out there?" and merging the answers.
//!
//! Each medium answers in its own way — or not at all. A medium that cannot
//! broadcast returns an empty list, so this module never needs to know which
//! is which: it asks all of them, in parallel, and deduplicates.
//!
//! Asking in parallel matters: two media with an 800 ms window would otherwise
//! take 1.6 s, and discovery is something a human waits for.

use retexis_core::{Announcement, Beacon, Endpoint};
use std::time::Duration;

/// A set of beacons plus the window to listen on each.
pub struct Scanner {
    beacons: Vec<Box<dyn Beacon>>,
    timeout: Duration,
}

impl Scanner {
    pub fn new(beacons: Vec<Box<dyn Beacon>>, timeout: Duration) -> Self {
        Scanner { beacons, timeout }
    }

    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    /// Media that claim they can broadcast, for the "asked N media" line.
    pub fn broadcasters(&self) -> Vec<&'static str> {
        self.beacons
            .iter()
            .filter(|b| b.can_broadcast())
            .map(|b| b.scheme())
            .collect()
    }

    pub fn scan(&self) -> Vec<Announcement> {
        self.scan_for(self.timeout)
    }

    /// Every medium is asked at once; the slowest one sets the wall clock.
    pub fn scan_for(&self, timeout: Duration) -> Vec<Announcement> {
        let mut found = Vec::new();
        std::thread::scope(|scope| {
            let handles: Vec<_> = self
                .beacons
                .iter()
                .filter(|b| b.can_broadcast())
                .map(|b| scope.spawn(move || b.discover(timeout).unwrap_or_default()))
                .collect();
            for handle in handles {
                if let Ok(list) = handle.join() {
                    found.extend(list);
                }
            }
        });
        found.sort_by(|a, b| a.name.cmp(&b.name));
        dedup(found)
    }
}

/// Two nodes on one host can answer twice (once per interface); keep one.
fn dedup(found: Vec<Announcement>) -> Vec<Announcement> {
    let mut out: Vec<Announcement> = Vec::with_capacity(found.len());
    for a in found {
        match out
            .iter_mut()
            .find(|seen| seen.node == a.node && seen.name == a.name)
        {
            Some(seen) => {
                for ep in &a.endpoints {
                    if !seen.endpoints.contains(ep) {
                        seen.endpoints.push(ep.clone());
                    }
                }
            }
            None => out.push(a),
        }
    }
    out
}

/// Flattened endpoints, in the order the announcements came back.
pub fn endpoints(found: &[Announcement]) -> Vec<Endpoint> {
    let mut out: Vec<Endpoint> = Vec::new();
    for a in found {
        for ep in &a.endpoints {
            if !out.contains(ep) {
                out.push(ep.clone());
            }
        }
    }
    out
}

/// One line per node, for the terminal.
pub fn describe(a: &Announcement) -> String {
    let who = match (a.name.is_empty(), a.node) {
        (false, Some(id)) => format!("{}/{}", a.name, id.short()),
        (false, None) => a.name.clone(),
        (true, Some(id)) => id.short(),
        (true, None) => "(anonymous)".to_string(),
    };
    let eps: Vec<String> = a.endpoints.iter().map(|e| e.url()).collect();
    if eps.is_empty() {
        format!("{who}  (no endpoints)")
    } else {
        format!("{who}  {}", eps.join("  "))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use retexis_core::{NoBroadcast, NodeId};

    fn ann(name: &str, node: NodeId, urls: &[&str]) -> Announcement {
        Announcement::reply(
            node,
            name,
            urls.iter().filter_map(|u| Endpoint::parse_url(u)).collect(),
        )
    }

    #[test]
    fn a_scanner_with_no_broadcaster_finds_nothing() {
        let scanner = Scanner::new(
            vec![Box::new(NoBroadcast::new("hub"))],
            Duration::from_millis(5),
        );
        assert!(scanner.broadcasters().is_empty());
        assert!(scanner.scan().is_empty());
    }

    #[test]
    fn endpoints_are_flattened_and_deduplicated() {
        let a = ann(
            "alice",
            NodeId::random(),
            &["tcp://1.2.3.4:1", "udp://1.2.3.4:2"],
        );
        let b = ann("bob", NodeId::random(), &["tcp://5.6.7.8:3"]);
        let eps = endpoints(&[a, b]);
        assert_eq!(eps.len(), 3);
        assert_eq!(eps[0].url(), "tcp://1.2.3.4:1");
    }

    #[test]
    fn describe_reads_like_a_line() {
        let a = ann("alice", NodeId::random(), &["tcp://1.2.3.4:1"]);
        let text = describe(&a);
        assert!(text.starts_with("alice/"));
        assert!(text.contains("tcp://1.2.3.4:1"));
    }

    #[test]
    fn the_same_node_answering_twice_is_merged() {
        let node = NodeId::random();
        let a = ann("alice", node, &["tcp://1.2.3.4:1"]);
        let b = ann("alice", node, &["udp://1.2.3.4:2"]);
        let merged = dedup(vec![a, b]);
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].endpoints.len(), 2);
    }
}
