//! Rows for the window.
//!
//! The `.slint` file knows nothing about the protocol: it draws strings. This
//! module is where an [`Entry`] becomes a row — which is also the only place
//! that decides what "12s" or "DELIVERED · 1075 B" reads like.

use crate::log::{Ack, Entry, Kind, Message};
use crate::{EntryRow, PeerRow};

/// An entry as the window wants it.
pub fn row(entry: &Entry, now_ms: u64) -> EntryRow {
    match &entry.kind {
        Kind::System { tag, text } => EntryRow {
            kind: 1,
            tag: tag.to_uppercase().into(),
            sender: "".into(),
            text: text.as_str().into(),
            meta: "".into(),
            mine: false,
            footer: "".into(),
        },
        Kind::Message(m) => EntryRow {
            kind: 0,
            tag: "".into(),
            sender: sender(m).into(),
            text: m.text.as_str().into(),
            meta: meta(m, entry.at_ms, now_ms).into(),
            mine: m.mine,
            footer: footer(m).into(),
        },
    }
}

fn sender(m: &Message) -> String {
    if m.mine {
        return "ME".to_string();
    }
    if !m.from_name.is_empty() {
        return m.from_name.clone();
    }
    m.from.chars().take(8).collect()
}

/// The line next to the name: age, then the facts that change how it reads.
fn meta(m: &Message, at_ms: u64, now_ms: u64) -> String {
    let mut parts = vec![age(at_ms, now_ms)];
    if m.stream {
        parts.push("STREAM".to_string());
    }
    if m.to.is_some() {
        parts.push("DM".to_string());
    }
    if !m.mine && m.hops > 1 {
        parts.push(format!("{}H", m.hops));
    }
    parts.join(" · ")
}

/// The line under the text: delivery state and size, or nothing.
fn footer(m: &Message) -> String {
    let mut parts = Vec::new();
    let ack = ack_label(m);
    if !ack.is_empty() {
        parts.push(ack.to_string());
    }
    if m.stream {
        parts.push(format!("{} B", m.bytes));
    }
    parts.join(" · ")
}

pub fn peer(peer: &crate::app::PeerView, target: Option<retexis_core::NodeId>) -> PeerRow {
    PeerRow {
        name: peer.name.as_str().into(),
        meta: if peer.hops <= 1 {
            "direct".to_string()
        } else {
            format!("{} hops · {}", peer.hops, peer.via)
        }
        .into(),
        hops: peer.hops.to_string().into(),
        selected: target == Some(peer.id),
    }
}

fn ack_label(m: &Message) -> &'static str {
    if !m.mine {
        return "";
    }
    match m.ack {
        Ack::Pending => "SENDING",
        Ack::Delivered => "DELIVERED",
        Ack::Failed => "FAILED",
        Ack::None => {
            if m.to.is_some() {
                "SENT"
            } else {
                ""
            }
        }
    }
}

/// How long ago, in the smallest unit that is still informative.
///
/// Deliberately relative: the timeline is in-memory for one session, so an
/// absolute clock would need a timezone library for a label nobody reads twice.
pub fn age(at_ms: u64, now_ms: u64) -> String {
    let secs = now_ms.saturating_sub(at_ms) / 1000;
    match secs {
        0..=4 => "now".to_string(),
        5..=59 => format!("{secs}s"),
        60..=3599 => format!("{}m", secs / 60),
        _ => format!("{}h", secs / 3600),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn msg(mine: bool, to: Option<&str>, ack: Ack, stream: bool) -> Message {
        Message {
            id: "x".into(),
            from: "aa".into(),
            from_name: "bob".into(),
            to: to.map(|s| s.to_string()),
            text: "hi".into(),
            hops: 1,
            mine,
            ack,
            stream,
            bytes: 2,
        }
    }

    #[test]
    fn ages_are_compact() {
        assert_eq!(age(1_000, 1_000), "now");
        assert_eq!(age(1_000, 1_000 + 3_000), "now");
        assert_eq!(age(1_000, 1_000 + 12_000), "12s");
        assert_eq!(age(0, 5 * 60_000), "5m");
        assert_eq!(age(0, 3_600_000 + 120_000), "1h");
    }

    #[test]
    fn an_unacked_broadcast_says_nothing() {
        assert_eq!(ack_label(&msg(true, None, Ack::None, false)), "");
        assert_eq!(footer(&msg(true, None, Ack::None, false)), "");
    }

    #[test]
    fn a_delivered_direct_message_says_so() {
        assert_eq!(
            footer(&msg(true, Some("bb"), Ack::Delivered, false)),
            "DELIVERED"
        );
    }

    #[test]
    fn a_stream_reports_its_size() {
        assert_eq!(
            footer(&msg(true, Some("bb"), Ack::Delivered, true)),
            "DELIVERED · 2 B"
        );
    }

    #[test]
    fn meta_names_the_facts_that_matter() {
        let m = msg(false, Some("bb"), Ack::None, true);
        let row = row(
            &Entry {
                seq: 1,
                at_ms: 0,
                kind: Kind::Message(m),
            },
            5_000,
        );
        assert_eq!(row.sender, "bob");
        assert_eq!(row.meta, "5s · STREAM · DM");
        assert!(!row.mine);
    }

    #[test]
    fn our_own_messages_are_signed_me() {
        let row = row(
            &Entry {
                seq: 1,
                at_ms: 0,
                kind: Kind::Message(msg(true, None, Ack::None, false)),
            },
            0,
        );
        assert_eq!(row.sender, "ME");
        assert!(row.mine);
    }

    #[test]
    fn a_system_entry_is_a_tagged_line() {
        let row = row(
            &Entry {
                seq: 1,
                at_ms: 0,
                kind: Kind::System {
                    tag: "link",
                    text: "tcp up".into(),
                },
            },
            0,
        );
        assert_eq!(row.kind, 1);
        assert_eq!(row.tag, "LINK");
        assert_eq!(row.text, "tcp up");
    }
}
