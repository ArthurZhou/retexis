//! The interactive shell.
//!
//! Deliberately thin: it turns keystrokes into `MeshHandle` calls and
//! `MeshStatus` snapshots into tables. No protocol logic lives here.

use super::connect::Connector;
use super::scan::{self, Scanner};
use super::view;
use crate::chat::{self, CHAT_CHANNEL};
use crate::term;
use retexis_core::NodeId;
use retexis_data::StreamSender;
use retexis_routing::MeshHandle;
use std::io::{self, BufRead, IsTerminal, Write};
use std::time::Duration;

pub fn run(
    handle: &MeshHandle,
    me_name: &str,
    scanner: Option<&Scanner>,
    connector: Option<&Connector>,
) {
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
        if !dispatch(handle, me_name, scanner, connector, input) {
            break;
        }
    }
}

/// Returns `false` when the REPL should stop.
fn dispatch(
    handle: &MeshHandle,
    me_name: &str,
    scanner: Option<&Scanner>,
    connector: Option<&Connector>,
    input: &str,
) -> bool {
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
            term::line(view::links(&handle.status()));
            true
        }
        "/net" | "/map" => {
            term::line(view::network(&handle.status()));
            true
        }
        "/routes" => {
            term::line(view::routes(&handle.status()));
            true
        }
        "/status" => {
            term::line(view::status(&handle.status(), me_name));
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
        "/discover" | "/find" => {
            discover(scanner, rest);
            true
        }
        "/stream" => {
            send_stream(handle, rest);
            true
        }
        "/connect" | "/c" => {
            connect(connector, handle, rest);
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

/// `/stream <peer|id> <text>` — send text as a stream instead of one frame.
///
/// Same channel, different content type: the receiver's sink sees
/// `StreamOpen` / `StreamChunk` / `StreamClose` instead of one `Text` frame.
/// This exists to exercise the data layer from the CLI; a file transfer would
/// use the same `StreamSender` with a bigger payload.
fn send_stream(handle: &MeshHandle, rest: &str) {
    let Some((who, text)) = rest.split_once(char::is_whitespace) else {
        term::line("usage: /stream <peer|id> <text>");
        return;
    };
    let Some(dst) = handle.resolve(who) else {
        term::line(format!("! unknown peer '{who}' (try /peers or /routes)"));
        return;
    };
    // Small chunks on purpose: a 40-byte message should visibly become several
    // chunks rather than one, otherwise the stream path is untested in practice.
    match StreamSender::open_reliable(handle, dst, CHAT_CHANNEL, "text") {
        Ok(sender) => {
            let id = sender.id();
            let mut sender = sender.with_chunk_size(24);
            let result = sender.write(text.as_bytes()).and_then(|_| sender.close());
            match result {
                Ok(()) => term::line(format!(
                    "· stream {id} → {who}: {} bytes in {} chunk{}",
                    sender.bytes_written(),
                    sender.chunks_written(),
                    if sender.chunks_written() == 1 {
                        ""
                    } else {
                        "s"
                    }
                )),
                Err(e) => term::line(format!("! stream {id} failed: {e}")),
            }
        }
        Err(e) => term::line(format!("! cannot open a stream to {who}: {e}")),
    }
}

/// `/connect <url>` — dial an address you already know.
///
/// The same path `--connect` uses, so the medium is resolved by scheme: a
/// `udp://` URL cannot end up on a TCP socket.
fn connect(connector: Option<&Connector>, handle: &MeshHandle, rest: &str) {
    let Some(connector) = connector else {
        term::line("! this node was built without a connector");
        return;
    };
    let url = rest.trim();
    if url.is_empty() {
        term::line("usage: /connect <scheme>://<host>:<port>   e.g. /connect tcp://10.0.0.7:7100");
        return;
    }
    match connector.dial(handle, url) {
        Ok(true) => term::line(format!(
            "· dialling {url}{}",
            if connector.supervised() {
                " (supervised: retries with backoff)"
            } else {
                " (one attempt: --no-reconnect)"
            }
        )),
        Ok(false) => term::line(format!("· already dialling {url}")),
        Err(e) => term::line(format!("! {e}")),
    }
}

/// `/discover [ms]` — ask every medium who is out there.
///
/// It only reports. Connecting is an operator decision, so this never dials on
/// its own; `--discover-connect` is the opt-in for startup.
fn discover(scanner: Option<&Scanner>, rest: &str) {
    let Some(scanner) = scanner else {
        term::line("! this node was built without a scanner");
        return;
    };
    if scanner.broadcasters().is_empty() {
        term::line("! no medium here can broadcast — nothing to ask");
        return;
    }
    let timeout = match rest.trim() {
        "" => scanner.timeout(),
        raw => match raw.parse::<u64>() {
            Ok(ms) if (20..=60_000).contains(&ms) => Duration::from_millis(ms),
            _ => {
                term::line(format!("! '{raw}' is not a timeout in ms (20..60000)"));
                return;
            }
        },
    };
    term::line(format!(
        "· asking {} for {} ms",
        scanner.broadcasters().join(", "),
        timeout.as_millis()
    ));
    let found = scanner.scan_for(timeout);
    if found.is_empty() {
        term::line("· nobody answered");
        return;
    }
    for a in &found {
        term::line(format!("· {}", scan::describe(a)));
    }
    let urls: Vec<String> = scan::endpoints(&found).iter().map(|e| e.url()).collect();
    term::line(format!(
        "· {} endpoint(s); dial with --connect {}",
        urls.len(),
        urls.first().cloned().unwrap_or_default()
    ));
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

const HELP: &str = "\
commands
  <text>                 broadcast to the whole mesh (best effort, flooded)
  /msg <peer|id> <text>  reliable unicast: acked and retransmitted
  /net                   the network through each link (start here)
  /peers                 attached links: medium, direction, peer identity
  /routes                routing table (hops, next hop, age)
  /status                counters, including auth and flood suppression
  /stream <peer> <text>  send text as a stream (open/chunk/close)
  /connect <url>         dial an address you know (tcp://host:port)
  /discover [ms]         broadcast a query and list who answers
  /kick <peer|id>        drop the links to a peer (watch it reconnect)
  /id                    my full node id
  /bc <text>             explicit broadcast (same as plain text)
  /quit                  leave the mesh
";
