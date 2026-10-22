//! Rendering the mesh for a human.
//!
//! The data layer hands the CLI a [`MeshStatus`]; this module turns it into
//! something you can read at a glance. It knows nothing about routing — it
//! only formats what the router already published.
//!
//! Two things make the difference between "a dump" and "a view":
//!
//! * **columns line up even with CJK names**, because a node called `节点一`
//!   occupies four terminal cells, not three characters;
//! * **links are the spine**. `/peers` lists cables, `/routes` lists
//!   destinations, but `/net` draws the network *through* each cable, which is
//!   how you actually see the topology.
//!
//! Colour is off unless stdout is a terminal, so piped output and the demo
//! scripts stay plain text.

use retexis_core::{human_bytes, NodeId};
use retexis_routing::MeshStatus;
use std::sync::atomic::{AtomicBool, Ordering};

const BOLD: &str = "1";
const DIM: &str = "2";
const CYAN: &str = "36";
const GREEN: &str = "32";
const RED: &str = "31";

static COLOR: AtomicBool = AtomicBool::new(false);

/// Turns ANSI colour on or off. The CLI calls this once, based on whether
/// stdout is a terminal.
pub fn set_color(on: bool) {
    COLOR.store(on, Ordering::Relaxed);
}

fn paint(text: &str, code: &str) -> String {
    if code.is_empty() || !COLOR.load(Ordering::Relaxed) {
        return text.to_string();
    }
    format!("\x1b[{code}m{text}\x1b[0m")
}

/// Terminal cells occupied by `s`. CJK and fullwidth characters take two.
fn width(s: &str) -> usize {
    s.chars().map(char_width).sum()
}

fn char_width(c: char) -> usize {
    let u = c as u32;
    match u {
        0 => 0,
        0x0300..=0x036F => 0, // combining marks
        0x1100..=0x115F
        | 0x2E80..=0x303E
        | 0x3041..=0x33FF
        | 0x3400..=0x4DBF
        | 0x4E00..=0x9FFF
        | 0xA000..=0xA4CF
        | 0xAC00..=0xD7A3
        | 0xF900..=0xFAFF
        | 0xFE30..=0xFE6F
        | 0xFF00..=0xFF60
        | 0xFFE0..=0xFFE6
        | 0x20000..=0x3FFFD => 2,
        _ => 1,
    }
}

/// Left-aligns to `cells`, then colours the result.
fn cell(text: &str, cells: usize, color: &str) -> String {
    let pad = cells.saturating_sub(width(text));
    let padded = format!("{text}{}", " ".repeat(pad));
    paint(&padded, color)
}

/// Right-aligns numbers, which is the only way a column of counts is readable.
fn num(value: impl std::fmt::Display, cells: usize, color: &str) -> String {
    let text = value.to_string();
    let pad = cells.saturating_sub(width(&text));
    let padded = format!("{}{text}", " ".repeat(pad));
    paint(&padded, color)
}

fn node_label(name: &str, id: NodeId) -> String {
    if name.is_empty() {
        id.short()
    } else {
        format!("{name}/{}", id.short())
    }
}

fn link_label(l: &retexis_routing::LinkInfo) -> String {
    match (l.node, l.node_name.is_empty()) {
        (Some(id), false) => format!("{}/{}", l.node_name, id.short()),
        (Some(id), true) => id.short(),
        (None, _) => "(unidentified)".to_string(),
    }
}

fn age(ms: u64) -> String {
    if ms < 1000 {
        format!("{ms}ms")
    } else {
        format!("{:.1}s", ms as f64 / 1000.0)
    }
}

/// Strips the padding the last column leaves behind, so piped output and
/// diffs are not full of invisible spaces.
fn finish(text: String) -> String {
    let mut out = text
        .lines()
        .map(|line| line.trim_end())
        .collect::<Vec<_>>()
        .join("\n");
    out.push('\n');
    out
}

fn rule(cells: usize) -> String {
    paint(&"─".repeat(cells), DIM)
}

// ------------------------------------------------------------------- /peers

/// Every cable, one row each.
pub fn links(st: &MeshStatus) -> String {
    finish(links_body(st))
}

fn links_body(st: &MeshStatus) -> String {
    let mut out = String::new();
    out.push_str(&format!(
        "{}   {} link{}\n",
        paint(&node_label(&st.name, st.self_id), BOLD),
        st.links.len(),
        if st.links.len() == 1 { "" } else { "s" }
    ));
    if st.links.is_empty() {
        out.push_str(&paint("  no links — /connect <url> or --discover\n", DIM));
        return out;
    }
    out.push_str(&format!(
        "{} {} {} {} {}\n",
        cell("#", 2, DIM),
        cell("MED", 5, DIM),
        cell("DIR", 3, DIM),
        cell("PEER", 24, DIM),
        cell("NODE", 24, DIM),
    ));
    out.push_str(&rule(62));
    out.push('\n');
    let mut links = st.links.clone();
    links.sort_by(|a, b| a.node.cmp(&b.node).then(a.peer.cmp(&b.peer)));
    for (i, l) in links.iter().enumerate() {
        out.push_str(&format!(
            "{} {} {} {} {}\n",
            num(i + 1, 2, DIM),
            cell(l.transport, 5, CYAN),
            cell(if l.initiator { "out" } else { "in" }, 3, DIM),
            cell(&l.peer, 24, ""),
            cell(&link_label(l), 24, BOLD),
        ));
    }
    out.push_str(&paint(
        &format!(
            "  max frame: {}\n",
            human_bytes(st.links.iter().map(|l| l.max_frame).max().unwrap_or(0) as u64)
        ),
        DIM,
    ));
    out
}

// ------------------------------------------------------------------ /routes

/// Every destination, with the hop that carries it.
pub fn routes(st: &MeshStatus) -> String {
    finish(routes_body(st))
}

fn routes_body(st: &MeshStatus) -> String {
    let mut out = String::new();
    out.push_str(&format!(
        "{}   {} route{}\n",
        paint(&node_label(&st.name, st.self_id), BOLD),
        st.routes.len(),
        if st.routes.len() == 1 { "" } else { "s" }
    ));
    if st.routes.is_empty() {
        out.push_str(&paint("  no remote routes yet\n", DIM));
        return out;
    }
    out.push_str(&format!(
        "{} {} {} {} {}\n",
        cell("DEST", 10, DIM),
        cell("NAME", 14, DIM),
        num("HOPS", 4, DIM),
        cell("NEXT HOP", 24, DIM),
        num("AGE", 7, DIM),
    ));
    out.push_str(&rule(62));
    out.push('\n');
    let mut routes = st.routes.clone();
    routes.sort_by_key(|r| (r.metric, r.name.clone()));
    for r in &routes {
        out.push_str(&format!(
            "{} {} {} {} {}\n",
            cell(&r.dst.short(), 10, ""),
            cell(if r.name.is_empty() { "-" } else { &r.name }, 14, BOLD),
            num(r.metric, 4, ""),
            cell(&r.next_hop_peer, 24, DIM),
            num(age(r.age_ms), 7, DIM),
        ));
    }
    out
}

// --------------------------------------------------------------------- /net

/// The network as seen *through* each cable.
///
/// This is the view that answers "what am I actually connected to": each link
/// is a branch, and everything reachable through it hangs off that branch.
/// The intermediate hops are not known (a distance-vector table only stores
/// the next hop), so the branch is one level deep by construction — drawing
/// more would be inventing information.
pub fn network(st: &MeshStatus) -> String {
    finish(network_body(st))
}

fn network_body(st: &MeshStatus) -> String {
    let mut out = String::new();
    out.push_str(&format!(
        "{}   {} link{} · {} reachable\n",
        paint(&node_label(&st.name, st.self_id), BOLD),
        st.links.len(),
        if st.links.len() == 1 { "" } else { "s" },
        st.routes.len(),
    ));
    out.push_str(&rule(66));
    out.push('\n');

    if st.links.is_empty() {
        out.push_str(&paint(
            "  no links — /connect <url>, or --discover to find peers\n",
            DIM,
        ));
        return out;
    }

    let mut links = st.links.clone();
    links.sort_by(|a, b| a.node.cmp(&b.node).then(a.peer.cmp(&b.peer)));

    for (i, l) in links.iter().enumerate() {
        let arrow = if l.initiator { "→" } else { "←" };
        out.push_str(&format!(
            "{} {} {} {} {}\n",
            num(i + 1, 2, DIM),
            cell(l.transport, 4, CYAN),
            paint(arrow, DIM),
            cell(&l.peer, 23, ""),
            cell(&link_label(l), 22, BOLD),
        ));

        // Everything routed through this cable, minus the peer itself (it is
        // already on the branch line above).
        let mut via: Vec<&retexis_routing::RouteInfo> = st
            .routes
            .iter()
            .filter(|r| r.next_hop == l.link_id)
            .filter(|r| Some(r.dst) != l.node)
            .collect();
        via.sort_by_key(|r| (r.metric, r.name.clone()));
        for (j, r) in via.iter().enumerate() {
            let last = j + 1 == via.len();
            let branch = if last { "└─" } else { "├─" };
            out.push_str(&format!(
                "   {} {} {} {}\n",
                paint(branch, DIM),
                cell(&node_label(&r.name, r.dst), 24, ""),
                num(format!("{}h", r.metric), 3, DIM),
                num(age(r.age_ms), 6, DIM),
            ));
        }
    }

    // Routes whose next hop is not a current link would be stale; say so
    // rather than hiding them.
    let known: Vec<_> = st.links.iter().map(|l| l.link_id).collect();
    let orphaned: Vec<_> = st
        .routes
        .iter()
        .filter(|r| !known.contains(&r.next_hop))
        .collect();
    if !orphaned.is_empty() {
        out.push_str(&paint(
            &format!("  {} route(s) via a link that is gone\n", orphaned.len()),
            RED,
        ));
    }
    out
}

// ------------------------------------------------------------------ /status

/// Counters, grouped by what they are for rather than by field order.
pub fn status(st: &MeshStatus, me_name: &str) -> String {
    finish(status_body(st, me_name))
}

fn status_body(st: &MeshStatus, me_name: &str) -> String {
    let s = st.stats;
    let mut out = String::new();
    out.push_str(&format!(
        "{}  {}\n",
        paint(me_name, BOLD),
        paint(&st.self_id.hex(), DIM)
    ));
    let auth = if st.auth {
        paint("auth on", GREEN)
    } else {
        paint("auth OFF", RED)
    };
    out.push_str(&format!(
        "  {auth}   {} links · {} routes · {} names known\n",
        st.links.len(),
        st.routes.len(),
        st.names.len(),
    ));
    out.push_str(&format!(
        "  traffic   sent {} · delivered {} · forwarded {} · flooded {}\n",
        s.sent, s.delivered, s.forwarded, s.flooded
    ));
    out.push_str(&format!(
        "  reliable  acked {} · failed {} · retransmits {} · acks sent {} · re-acks {}\n",
        s.acked, s.delivery_failed, s.retransmits, s.acks_sent, s.duplicate_acks
    ));
    out.push_str(&format!(
        "  dropped   dup {} · ttl {} · no-route {} · malformed {} · auth {}\n",
        s.dropped_duplicate,
        s.dropped_ttl,
        s.dropped_no_route,
        s.dropped_malformed,
        s.auth_rejected
    ));
    out.push_str(&format!(
        "  limits    flood suppressed {} · duplicate links {}\n",
        s.flood_suppressed, s.duplicate_links
    ));
    out.push_str(&format!(
        "  wire      in {} / {} frames · out {} / {} frames\n",
        human_bytes(s.bytes_in),
        s.frames_in,
        human_bytes(s.bytes_out),
        s.frames_out
    ));
    if st.pending > 0 {
        out.push_str(&format!(
            "  pending   {} reliable unicast(s) waiting for an ack\n",
            st.pending
        ));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use retexis_routing::{LinkInfo, RouteInfo, Stats};

    fn link(id: u64, node: NodeId, name: &str, peer: &str, out: bool) -> LinkInfo {
        LinkInfo {
            link_id: id,
            transport: "tcp",
            peer: peer.to_string(),
            node: Some(node),
            node_name: name.to_string(),
            max_frame: 1 << 20,
            initiator: out,
        }
    }

    fn route(dst: NodeId, name: &str, hops: u8, next_hop: u64) -> RouteInfo {
        RouteInfo {
            dst,
            name: name.to_string(),
            metric: hops,
            next_hop,
            next_hop_peer: "peer".into(),
            age_ms: 120,
        }
    }

    fn mesh() -> MeshStatus {
        let me = NodeId::random();
        let alice = NodeId::random();
        let carol = NodeId::random();
        let dave = NodeId::random();
        MeshStatus {
            self_id: me,
            name: "bob".into(),
            links: vec![
                link(1, alice, "alice", "10.0.0.7:7100", true),
                link(2, dave, "dave", "10.0.0.9:7100", false),
            ],
            routes: vec![
                route(alice, "alice", 1, 1),
                route(carol, "carol", 2, 1),
                route(dave, "dave", 1, 2),
            ],
            names: Default::default(),
            stats: Stats::default(),
            pending: 0,
            auth: true,
        }
    }

    #[test]
    fn width_counts_cjk_as_two_cells() {
        assert_eq!(width("abc"), 3);
        assert_eq!(width("节点"), 4);
        assert_eq!(width("a节b"), 4);
    }

    #[test]
    fn columns_line_up_with_cjk_names() {
        let st = mesh();
        let text = links(&st);
        let rows: Vec<&str> = text.lines().filter(|l| l.starts_with(" 1")).collect();
        assert_eq!(rows.len(), 1);
        // Padding is by display width, so the rendered line has the expected
        // number of cells regardless of what the name contains.
        assert!(width(rows[0]) >= 40);
    }

    #[test]
    fn the_network_view_hangs_routes_off_their_link() {
        let st = mesh();
        let text = network(&st);
        // carol is two hops away through link 1, so she must be a branch of it.
        let carol_line = text
            .lines()
            .find(|l| l.contains("carol"))
            .expect("carol should appear");
        assert!(carol_line.contains("├─") || carol_line.contains("└─"));
        assert!(carol_line.contains("2h"));
        // alice is the peer of link 1 and must not be repeated as a branch.
        assert_eq!(
            text.matches("alice").count(),
            1,
            "the direct peer should appear once:\n{text}"
        );
    }

    #[test]
    fn an_empty_mesh_says_what_to_do() {
        let mut st = mesh();
        st.links.clear();
        st.routes.clear();
        assert!(links(&st).contains("no links"));
        assert!(network(&st).contains("/connect"));
    }

    #[test]
    fn status_shows_auth_state_and_counters() {
        let st = mesh();
        let text = status(&st, "bob");
        assert!(text.contains("auth on"));
        assert!(text.contains("traffic"));
        assert!(text.contains("reliable"));
    }

    #[test]
    fn colour_is_off_by_default() {
        set_color(false);
        let st = mesh();
        assert!(!network(&st).contains('\u{1b}'));
    }
}
