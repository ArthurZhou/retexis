//! The conversation: messages and system events, in one ordered list.
//!
//! A chat window has to show two kinds of thing and they are not the same:
//!
//! * **messages** — what a human typed, on either end;
//! * **system events** — a link came up, a route was learned, a packet failed
//!   authentication.
//!
//! They share one timeline because that is how a mesh feels when you use it:
//! "why didn't my message arrive" is answered by the link-down line three
//! seconds earlier. But they are different variants, so the UI can style them
//! differently and the tests can assert on one without matching the other.
//!
//! The log is bounded twice — by entry count and by total text — because a
//! GUI that runs for a week must not grow without bound.

use std::collections::VecDeque;

/// How many entries to keep. Older ones fall off the front.
const MAX_ENTRIES: usize = 4_000;
/// Roughly how much message text to keep, in bytes.
const MAX_BYTES: usize = 8 * 1024 * 1024;

/// Delivery state of a message *we* sent.
///
/// Broadcasts are best-effort by construction, so they stay `None` forever:
/// acking a broadcast would turn one packet into N.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Ack {
    None,
    Pending,
    Delivered,
    Failed,
}

impl Ack {
    pub fn as_str(self) -> &'static str {
        match self {
            Ack::None => "none",
            Ack::Pending => "pending",
            Ack::Delivered => "delivered",
            Ack::Failed => "failed",
        }
    }
}

#[derive(Clone, Debug)]
pub struct Message {
    /// Wire message id, hex. The key for ack updates.
    pub id: String,
    pub from: String,
    pub from_name: String,
    /// `None` means broadcast.
    pub to: Option<String>,
    pub text: String,
    pub hops: u8,
    /// Sent from this node (shown on the right, no sender name).
    pub mine: bool,
    pub ack: Ack,
    /// Arrived as a stream, i.e. reassembled from chunks.
    pub stream: bool,
    pub bytes: usize,
}

#[derive(Clone, Debug)]
pub enum Kind {
    Message(Message),
    /// `tag` is a short machine-readable category (`link`, `route`, `auth`,
    /// `app`); `text` is what a human reads.
    System {
        tag: &'static str,
        text: String,
    },
}

#[derive(Clone, Debug)]
pub struct Entry {
    pub seq: u64,
    /// Milliseconds since the application started.
    ///
    /// Relative on purpose: the browser turns this into a wall clock, so a
    /// peer with a wrong clock cannot make its messages appear from 1970.
    pub at_ms: u64,
    pub kind: Kind,
}

impl Entry {
    fn weight(&self) -> usize {
        match &self.kind {
            Kind::Message(m) => m.text.len() + 64,
            Kind::System { text, .. } => text.len() + 32,
        }
    }
}

#[derive(Default)]
pub struct Log {
    seq: u64,
    entries: VecDeque<Entry>,
    bytes: usize,
}

impl Log {
    pub fn new() -> Self {
        Self::default()
    }

    /// Appends an entry and returns its sequence number.
    pub fn push(&mut self, at_ms: u64, kind: Kind) -> u64 {
        self.seq += 1;
        let entry = Entry {
            seq: self.seq,
            at_ms,
            kind,
        };
        self.bytes += entry.weight();
        self.entries.push_back(entry);
        while self.entries.len() > MAX_ENTRIES || self.bytes > MAX_BYTES {
            match self.entries.pop_front() {
                Some(old) => self.bytes = self.bytes.saturating_sub(old.weight()),
                None => break,
            }
        }
        self.seq
    }

    pub fn message(&mut self, at_ms: u64, msg: Message) -> u64 {
        self.push(at_ms, Kind::Message(msg))
    }

    pub fn system(&mut self, at_ms: u64, tag: &'static str, text: impl Into<String>) -> u64 {
        self.push(
            at_ms,
            Kind::System {
                tag,
                text: text.into(),
            },
        )
    }

    /// Records the delivery state of a message we sent.
    ///
    /// Searched from the back: the ack for a message almost always arrives
    /// while its entry is still near the end, and the alternative is an index
    /// that has to be invalidated when entries are evicted.
    pub fn set_ack(&mut self, id: &str, ack: Ack) -> bool {
        for entry in self.entries.iter_mut().rev() {
            if let Kind::Message(m) = &mut entry.kind {
                if m.id == id {
                    m.ack = ack;
                    return true;
                }
            }
        }
        false
    }

    /// Entries newer than `seq`. The caller passes back the highest seq it has
    /// already rendered.
    pub fn since(&self, seq: u64) -> impl Iterator<Item = &Entry> {
        self.entries.iter().filter(move |e| e.seq > seq)
    }

    pub fn last_seq(&self) -> u64 {
        self.seq
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn msg(id: &str, text: &str) -> Message {
        Message {
            id: id.to_string(),
            from: "aa".into(),
            from_name: "alice".into(),
            to: None,
            text: text.into(),
            hops: 1,
            mine: false,
            ack: Ack::None,
            stream: false,
            bytes: text.len(),
        }
    }

    #[test]
    fn sequence_numbers_are_monotonic_and_gap_free() {
        let mut log = Log::new();
        assert_eq!(
            log.push(
                0,
                Kind::System {
                    tag: "app",
                    text: "x".into()
                }
            ),
            1
        );
        assert_eq!(log.message(0, msg("1", "hi")), 2);
        assert_eq!(log.last_seq(), 2);
        let seqs: Vec<u64> = log.since(0).map(|e| e.seq).collect();
        assert_eq!(seqs, vec![1, 2]);
    }

    #[test]
    fn since_returns_only_new_entries() {
        let mut log = Log::new();
        log.message(0, msg("1", "one"));
        log.message(0, msg("2", "two"));
        let tail: Vec<u64> = log.since(1).map(|e| e.seq).collect();
        assert_eq!(tail, vec![2]);
        assert_eq!(log.since(2).count(), 0);
    }

    #[test]
    fn an_ack_finds_its_message() {
        let mut log = Log::new();
        log.message(0, msg("aa", "one"));
        log.message(0, msg("bb", "two"));
        assert!(log.set_ack("aa", Ack::Delivered));
        let acks: Vec<Ack> = log
            .since(0)
            .filter_map(|e| match &e.kind {
                Kind::Message(m) => Some(m.ack),
                _ => None,
            })
            .collect();
        assert_eq!(acks, vec![Ack::Delivered, Ack::None]);
        assert!(!log.set_ack("zz", Ack::Failed), "unknown id");
    }

    #[test]
    fn old_entries_are_evicted() {
        let mut log = Log::new();
        for i in 0..(MAX_ENTRIES + 10) {
            log.message(0, msg(&i.to_string(), "x"));
        }
        assert_eq!(log.len(), MAX_ENTRIES);
        // The oldest are gone, the newest are not, and seq keeps counting.
        assert_eq!(log.since(0).next().unwrap().seq, 11);
        assert_eq!(log.last_seq() as usize, MAX_ENTRIES + 10);
    }

    #[test]
    fn a_flood_of_large_messages_is_bounded_by_bytes() {
        let mut log = Log::new();
        let big = "x".repeat(64 * 1024);
        for i in 0..400 {
            log.message(0, msg(&i.to_string(), &big));
        }
        let kept: usize = log.since(0).map(|e| e.weight()).sum();
        assert!(
            kept <= MAX_BYTES + 64 * 1024,
            "log grew to {kept} bytes, cap is {MAX_BYTES}"
        );
        assert!(log.len() < 400, "some entries must have been evicted");
    }
}
