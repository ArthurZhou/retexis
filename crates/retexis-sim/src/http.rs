//! A small HTTP/1.1 server for the control API and the single-page UI.
//!
//! `std` only, one thread per connection, `Connection: close`. The UI polls
//! every 120 ms with a handful of parallel requests, which this handles
//! comfortably, and it keeps the whole crate dependency-free.
//!
//! The JSON shape here is the only contract the UI has: `web/index.html`
//! renders whatever `GET /api/state` returns and turns clicks into the requests
//! below. Nothing about the protocol is decided in this file, which is why the
//! UI can be tested through the API alone (`tests/api.rs`).

use crate::hub::{LinkKey, LinkParams};
use crate::json::{self, Json};
use crate::sim::Sim;
use retexis_core::NodeId;
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

struct Request {
    method: String,
    path: String,
    query: String,
    body: Vec<u8>,
}

pub fn serve(addr: &str, sim: Arc<Sim>, web: PathBuf) -> io::Result<()> {
    serve_listener(TcpListener::bind(addr)?, sim, web)
}

/// Same as [`serve`], on an already bound listener.
///
/// Tests bind port 0 and ask the OS which port they got, which is the only
/// reliable way to run several servers side by side.
pub fn serve_listener(listener: TcpListener, sim: Arc<Sim>, web: PathBuf) -> io::Result<()> {
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let sim = Arc::clone(&sim);
                let web = web.clone();
                // A browser opening six sockets at once must not serialise
                // behind one slow request, so each connection gets a thread.
                let _ = std::thread::Builder::new()
                    .name("retexis-http".to_string())
                    .spawn(move || {
                        let _ = handle(stream, &sim, &web);
                    });
            }
            Err(_) => continue,
        }
    }
    Ok(())
}

fn handle(mut stream: TcpStream, sim: &Arc<Sim>, web: &Path) -> io::Result<()> {
    let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
    let req = match read_request(&mut stream) {
        Ok(Some(r)) => r,
        Ok(None) => return Ok(()),
        Err(_) => return Ok(()),
    };

    let parts: Vec<String> = req
        .path
        .split('/')
        .filter(|p| !p.is_empty())
        .map(percent_decode)
        .collect();

    let (status, ctype, body) = route(&parts, &req, sim, web);
    write_response(&mut stream, status, ctype, &body)
}

type Response = (u16, &'static str, Vec<u8>);

fn route(parts: &[String], req: &Request, sim: &Arc<Sim>, web: &Path) -> Response {
    let method = req.method.as_str();

    if method == "GET" && (parts.is_empty() || parts[0] == "index.html" || parts[0] == "web") {
        return match std::fs::read(web.join("index.html")) {
            Ok(bytes) => (200, "text/html; charset=utf-8", bytes),
            Err(e) => (
                500,
                "text/plain; charset=utf-8",
                format!(
                    "cannot read {}: {e}\n\nPass --web <dir> or run from the repository root.",
                    web.join("index.html").display()
                )
                .into_bytes(),
            ),
        };
    }

    let api = parts.first().map(|s| s.as_str()) == Some("api");
    if !api {
        return error(404, "not found");
    }

    let rest = &parts[1..];
    match (method, rest) {
        ("GET", [what]) if what == "state" => {
            let since = query_value(&req.query, "log_since")
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(0);
            (200, JSON, sim.snapshot(since).encode().into_bytes())
        }
        ("POST", [what]) if what == "control" => with_body(req, |body| {
            if let Some(running) = body.bool_of("running") {
                sim.hub().set_running(running);
            }
            if let Some(rate) = body.f64_of("rate") {
                sim.hub().set_rate(rate);
            }
            if let Some(step) = body.f64_of("step") {
                sim.hub().step(step);
            }
            Ok(ok())
        }),
        ("POST", [what]) if what == "preset" => with_body(req, |body| {
            let kind = body.str_of("kind").unwrap_or("line").to_string();
            let count = body.f64_of("count").unwrap_or(4.0) as usize;
            sim.reset(&kind, count);
            Ok(ok())
        }),
        ("POST", [what]) if what == "node" => with_body(req, |body| {
            let ttl = body.f64_of("ttl").unwrap_or(8.0).clamp(1.0, 255.0) as u8;
            let id = sim.add_node(
                body.opt_str("name"),
                ttl,
                body.f64_of("x"),
                body.f64_of("y"),
            );
            Ok(json_response(Json::obj([
                ("ok", Json::Bool(true)),
                ("id", Json::Str(id.hex())),
            ])))
        }),
        ("POST", [what]) if what == "link" => with_body(req, |body| {
            let a = parse_id(body.str_of("a")).ok_or("bad node id")?;
            let b = parse_id(body.str_of("b")).ok_or("bad node id")?;
            let params = LinkParams {
                latency: Duration::from_secs_f64(
                    body.f64_of("latency").unwrap_or(0.02).clamp(0.0, 5.0),
                ),
                loss: body.f64_of("loss").unwrap_or(0.0).clamp(0.0, 0.95),
            };
            let supervise = body.bool_of("supervise").unwrap_or(false);
            let key = sim.add_link(a, b, params, supervise)?;
            Ok(json_response(Json::obj([
                ("ok", Json::Bool(true)),
                ("key", Json::Str(key.key_string())),
            ])))
        }),
        ("POST", [what]) if what == "send" => with_body(req, |body| {
            let from = parse_id(body.str_of("from")).ok_or("bad sender id")?;
            let to = match body.get("to") {
                None | Some(Json::Null) => None,
                Some(_) => Some(parse_id(body.str_of("to")).ok_or("bad recipient id")?),
            };
            let text = body.str_of("text").unwrap_or("").to_string();
            sim.send(from, to, &text, body.bool_of("reliable").unwrap_or(false))?;
            Ok(ok())
        }),
        ("PATCH", [what, id]) if what == "node" => with_body(req, |body| {
            let id = parse_id(Some(id.as_str())).ok_or("bad node id")?;
            sim.update_node(id, body)?;
            Ok(ok())
        }),
        ("PATCH", [what, key]) if what == "link" => with_body(req, |body| {
            let key = LinkKey::parse(key).ok_or("bad link key")?;
            sim.update_link(key, body)?;
            Ok(ok())
        }),
        ("DELETE", [what, id]) if what == "node" => match parse_id(Some(id.as_str())) {
            Some(id) => match sim.delete_node(id) {
                Ok(()) => ok(),
                Err(e) => error(400, &e),
            },
            None => error(400, "bad node id"),
        },
        ("DELETE", [what, key]) if what == "link" => match LinkKey::parse(key) {
            Some(key) => match sim.delete_link(key) {
                Ok(()) => ok(),
                Err(e) => error(400, &e),
            },
            None => error(400, "bad link key"),
        },
        _ => error(404, "not found"),
    }
}

const JSON: &str = "application/json; charset=utf-8";

fn ok() -> Response {
    json_response(Json::obj([("ok", Json::Bool(true))]))
}

fn json_response(v: Json) -> Response {
    (200, JSON, v.encode().into_bytes())
}

fn error(status: u16, message: &str) -> Response {
    (
        status,
        JSON,
        Json::obj([("error", Json::Str(message.to_string()))])
            .encode()
            .into_bytes(),
    )
}

fn with_body(req: &Request, f: impl FnOnce(&Json) -> Result<Response, String>) -> Response {
    let text = String::from_utf8_lossy(&req.body);
    let body = if text.trim().is_empty() {
        Json::Obj(Vec::new())
    } else {
        match json::parse(&text) {
            Ok(v) => v,
            Err(e) => return error(400, &format!("bad json: {e}")),
        }
    };
    match f(&body) {
        Ok(r) => r,
        Err(e) => error(400, &e),
    }
}

fn parse_id(s: Option<&str>) -> Option<NodeId> {
    NodeId::from_hex(s?.trim())
}

fn query_value(query: &str, key: &str) -> Option<String> {
    query.split('&').find_map(|pair| {
        let (k, v) = pair.split_once('=')?;
        if percent_decode(k) == key {
            Some(percent_decode(v))
        } else {
            None
        }
    })
}

fn percent_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%'
            && i + 2 < b.len()
            && b[i + 1].is_ascii_hexdigit()
            && b[i + 2].is_ascii_hexdigit()
        {
            if let Ok(v) = u8::from_str_radix(&s[i + 1..i + 3], 16) {
                out.push(v);
                i += 3;
                continue;
            }
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).to_string()
}

// -------------------------------------------------------------- plumbing

fn read_request(stream: &mut TcpStream) -> io::Result<Option<Request>> {
    let mut buf = Vec::with_capacity(2048);
    let mut chunk = [0u8; 4096];
    let head_end = loop {
        let n = stream.read(&mut chunk)?;
        if n == 0 {
            return Ok(None);
        }
        buf.extend_from_slice(&chunk[..n]);
        if let Some(pos) = find(&buf, b"\r\n\r\n") {
            break pos;
        }
        if buf.len() > 64 * 1024 {
            return Ok(None);
        }
    };

    let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
    let mut lines = head.split("\r\n");
    let request_line = lines.next().unwrap_or_default();
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_string();
    let target = parts.next().unwrap_or_default().to_string();
    if method.is_empty() || target.is_empty() {
        return Ok(None);
    }
    let (path, query) = match target.split_once('?') {
        Some((p, q)) => (p.to_string(), q.to_string()),
        None => (target, String::new()),
    };

    let mut content_length = 0usize;
    for line in lines {
        if let Some((name, value)) = line.split_once(':') {
            if name.eq_ignore_ascii_case("content-length") {
                content_length = value.trim().parse().unwrap_or(0);
            }
        }
    }
    if content_length > 1024 * 1024 {
        return Ok(None);
    }

    let body_start = head_end + 4;
    while buf.len() < body_start + content_length {
        let n = stream.read(&mut chunk)?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..n]);
    }
    let body = buf[body_start..(body_start + content_length).min(buf.len())].to_vec();

    Ok(Some(Request {
        method,
        path,
        query,
        body,
    }))
}

fn write_response(stream: &mut TcpStream, status: u16, ctype: &str, body: &[u8]) -> io::Result<()> {
    let reason = match status {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        500 => "Internal Server Error",
        _ => "OK",
    };
    let head = format!(
        "HTTP/1.1 {status} {reason}\r\n\
         content-type: {ctype}\r\n\
         content-length: {}\r\n\
         cache-control: no-store\r\n\
         connection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes())?;
    stream.write_all(body)?;
    stream.flush()
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || haystack.len() < needle.len() {
        return None;
    }
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percent_decoding_handles_pipe_and_utf8() {
        assert_eq!(percent_decode("a%7Cb"), "a|b");
        assert_eq!(percent_decode("%E4%BD%A0"), "你");
        assert_eq!(percent_decode("plain"), "plain");
        // A stray percent must not panic or eat the rest.
        assert_eq!(percent_decode("100%"), "100%");
    }

    #[test]
    fn query_values_are_extracted() {
        assert_eq!(
            query_value("log_since=12", "log_since").as_deref(),
            Some("12")
        );
        assert_eq!(
            query_value("a=1&log_since=3", "log_since").as_deref(),
            Some("3")
        );
        assert_eq!(query_value("a=1", "log_since"), None);
    }

    #[test]
    fn header_terminator_is_found() {
        assert_eq!(find(b"GET / HTTP/1.1\r\n\r\nbody", b"\r\n\r\n"), Some(14));
        assert_eq!(find(b"no terminator", b"\r\n\r\n"), None);
    }
}
