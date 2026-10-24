//! The simulator's control API, on top of the shared HTTP server.
//!
//! The plumbing (request parsing, percent decoding, response writing) lives in
//! `retexis-http`, because the GUI needs exactly the same thing. What is here
//! is only the routing table of the simulator's own endpoints.
//!
//! The JSON shape is the only contract `web/index.html` has: it renders
//! whatever `GET /api/state` returns and turns clicks into the requests below.
//! Nothing about the protocol is decided in this file, which is why the UI can
//! be tested through the API alone (`tests/api.rs`).

use crate::hub::{LinkKey, LinkParams};
use crate::sim::Sim;
use retexis_core::NodeId;
use retexis_http::{error, json_response, ok, with_body, Json, Request, Response};
use std::io;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

pub fn serve(addr: &str, sim: Arc<Sim>, web: PathBuf) -> io::Result<()> {
    serve_listener(TcpListener::bind(addr)?, sim, web)
}

/// Same as [`serve`], on an already bound listener.
///
/// Tests bind port 0 and ask the OS which port they got, which is the only
/// reliable way to run several servers side by side.
pub fn serve_listener(listener: TcpListener, sim: Arc<Sim>, web: PathBuf) -> io::Result<()> {
    retexis_http::serve_listener(listener, move |parts, req| route(parts, req, &sim, &web))
}

fn route(parts: &[String], req: &Request, sim: &Arc<Sim>, web: &Path) -> Response {
    if req.is("GET") && (parts.is_empty() || parts[0] == "index.html" || parts[0] == "web") {
        return match std::fs::read(web.join("index.html")) {
            Ok(bytes) => retexis_http::html(bytes),
            Err(e) => (
                500,
                retexis_http::TEXT,
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
    match (req.method.as_str(), rest) {
        ("GET", [what]) if what == "state" => {
            let since = req
                .query("log_since")
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(0);
            (
                200,
                retexis_http::JSON,
                sim.snapshot(since).encode().into_bytes(),
            )
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

fn parse_id(s: Option<&str>) -> Option<NodeId> {
    NodeId::from_hex(s?.trim())
}
