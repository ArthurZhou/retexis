//! The interactive shell.
//!
//! Deliberately thin: it turns keystrokes into `MeshHandle` calls and
//! `MeshStatus` snapshots into tables. No protocol logic lives here.

use retexis_app::{chat, term};
use retexis_core::{human_bytes, NodeId};
use retexis_routing::MeshHandle;
use std::io::{self, BufRead, IsTerminal, Write};

pub fn run(handle: &MeshHandle, me_name: &str) {
    let interactive = io::stdin().is_terminal();
    let stdin = io::stdin();
    let mut lines = stdin.lock().lines();

    loop {
        if interactive {
            print!("› ");
            let _ = io::stdout().flush();
        }
        let line = match lines.next() {
            Some(Ok(l)) => l,
            Some(Err(e)) => {
                term::line(format!("! stdin error: {e}"));
                break;
            }
            None => break, // EOF: piped input finished
        };
        let input = line.trim();
        if input.is_empty() {
            continue;
        }
        if !dispatch(handle, me_name, input) {
            break;
        }
    }
}

/// Returns `false` when the REPL should stop.
fn dispatch(handle: &MeshHandle, me_name: &str, input: &str) -> bool {
    if !input.starts_with('/') {
        return broadcast(handle, input);
    }
    let (cmd, rest) = match input.split_once(char::is_whitespace) {
        Some((c, r)) => (c, r.trim()),
        None => (input, ""),
    };
    match cmd {
        "/help" | "/?" => {
            term::lines(HELP);
            true
        }
        "/id" => {
            term::line(format!(
                "{me_name}  {}  (short {})",
                handle.id().hex(),
                handle.id().short()
            ));
            true
        }
        "/peers" => {
            print_peers(handle);
            true
        }
        "/routes" => {
            print_routes(handle);
            true
        }
        "/status" => {
            print_status(handle, me_name);
            true
        }
        "/msg" | "/m" => {
            unicast(handle, rest);
            true
        }
        "/kick" => {
            kick(handle, rest);
            true
        }
        "/bc" => broadcast(handle, rest),
        "/quit" | "/exit" | "/q" => {
            term::line("bye");
            false
        }
        other => {
            term::line(format!("! unknown command {other} (try /help)"));
            true
        }
    }
}

fn broadcast(handle: &MeshHandle, text: &str) -> bool {
    match chat::send_text(handle, None, text) {
        Ok(_) => true,
        Err(e) => {
            term::line(format!("! send failed: {e}"));
            true
        }
    }
}

fn unicast(handle: &MeshHandle, rest: &str) {
    let mut parts = rest.splitn(2, char::is_whitespace);
    let token = parts.next().unwrap_or("").trim();
    let text = parts.next().unwrap_or("").trim();
    if token.is_empty() || text.is_empty() {
        term::line("usage: /msg <peer|short-id> <text>");
        return;
    }
    let dst = match handle.resolve(token) {
        Some(id) => id,
        None => {
            term::line(format!("! unknown peer '{token}' — try /peers"));
            return;
        }
    };
    // DMs are reliable: the destination acks and the router retransmits, so
    // delivery (or failure) shows up as a notice a moment later.
    match chat::send_reliable(handle, dst, text) {
        Ok(_) => term::line(format!(
            "{:<22} {text}",
            format!("me → {}", name_of(handle, dst))
        )),
        Err(e) => term::line(format!("! send failed: {e}")),
    }
}

fn kick(handle: &MeshHandle, token: &str) {
    if token.is_empty() {
        term::line("usage: /kick <peer|short-id>");
        return;
    }
    let dst = match handle.resolve(token) {
        Some(id) => id,
        None => {
            term::line(format!("! unknown peer '{token}' — try /peers"));
            return;
        }
    };
    let targets: Vec<_> = handle
        .status()
        .links
        .into_iter()
        .filter(|l| l.node == Some(dst))
        .collect();
    if targets.is_empty() {
        term::line(format!("! no link to {}", name_of(handle, dst)));
        return;
    }
    let count = targets.len();
    for link in targets {
        let _ = handle.drop_link(link.link_id);
    }
    term::line(format!(
        "- dropped {count} link(s) to {}; a supervised dialer will reconnect",
        name_of(handle, dst)
    ));
}

fn name_of(handle: &MeshHandle, id: NodeId) -> String {
    if id == handle.id() {
        return handle.name().to_string();
    }
    let st = handle.status();
    match st.names.get(&id) {
        Some(n) if !n.is_empty() => n.clone(),
        _ => id.short(),
    }
}

fn print_peers(handle: &MeshHandle) {
    let st = handle.status();
    if st.links.is_empty() {
        term::line("(no links)");
        return;
    }
    term::line(format!(
        "{:<6} {:<22} {:<4} {:<10} {:<12} {:>9}",
        "MEDIUM", "PEER", "DIR", "NODE", "NAME", "MAX FRAME"
    ));
    term::line("-".repeat(68));
    for l in &st.links {
        term::line(format!(
            "{:<6} {:<22} {:<4} {:<10} {:<12} {:>9}",
            l.transport,
            l.peer,
            if l.initiator { "out" } else { "in" },
            l.node.map(|n| n.short()).unwrap_or_else(|| "-".into()),
            if l.node_name.is_empty() {
                "-".to_string()
            } else {
                l.node_name.clone()
            },
            human_bytes(l.max_frame as u64),
        ));
    }
}

fn print_routes(handle: &MeshHandle) {
    let st = handle.status();
    term::line(format!(
        "{:<10} {:<12} {:>4}  {:<22} {:>7}",
        "DEST", "NAME", "HOPS", "VIA", "AGE"
    ));
    term::line("-".repeat(60));
    term::line(format!(
        "{:<10} {:<12} {:>4}  {:<22} {:>7}",
        "self", st.name, 0, "-", "-"
    ));
    for r in &st.routes {
        term::line(format!(
            "{:<10} {:<12} {:>4}  {:<22} {:>7}",
            r.dst.short(),
            if r.name.is_empty() { "-" } else { &r.name },
            r.metric,
            r.next_hop_peer,
            format!("{}ms", r.age_ms),
        ));
    }
    if st.routes.is_empty() {
        term::line("(no remote routes yet)");
    }
}

fn print_status(handle: &MeshHandle, me_name: &str) {
    let st = handle.status();
    let s = st.stats;
    term::line(format!("node      {me_name} / {}", handle.id().short()));
    term::line(format!(
        "links     {}   routes {}   known names {}",
        st.links.len(),
        st.routes.len(),
        st.names.len()
    ));
    term::line(format!(
        "auth      {}   pending {} reliable",
        if st.auth { "on (psk)" } else { "OFF" },
        st.pending
    ));
    term::line(format!(
        "sent      {}   forwarded {}   flooded {}   delivered {}",
        s.sent, s.forwarded, s.flooded, s.delivered
    ));
    term::line(format!(
        "reliable  acked {}   failed {}   retransmits {}   acks sent {}   re-acks {}",
        s.acked, s.delivery_failed, s.retransmits, s.acks_sent, s.duplicate_acks
    ));
    term::line(format!(
        "dropped   dup {}   ttl {}   no-route {}   malformed {}   auth {}",
        s.dropped_duplicate,
        s.dropped_ttl,
        s.dropped_no_route,
        s.dropped_malformed,
        s.auth_rejected
    ));
    term::line(format!(
        "flood     suppressed {}   duplicate links {}",
        s.flood_suppressed, s.duplicate_links
    ));
    term::line(format!(
        "traffic   in {} / {} frames, out {} / {} frames",
        human_bytes(s.bytes_in),
        s.frames_in,
        human_bytes(s.bytes_out),
        s.frames_out
    ));
}

const HELP: &str = "\
commands
  <text>                 broadcast to the whole mesh (best effort, flooded)
  /msg <peer|id> <text>  reliable unicast: acked and retransmitted
  /peers                 attached links: medium, direction, peer identity
  /routes                routing table (hops, next hop, age)
  /status                counters, including auth and flood suppression
  /kick <peer|id>        drop the links to a peer (watch it reconnect)
  /id                    my full node id
  /bc <text>             explicit broadcast (same as plain text)
  /quit                  leave the mesh
";
