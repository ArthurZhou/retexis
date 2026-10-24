//! One thread per connection, `Connection: close`, no keep-alive.
//!
//! A browser tab polling every 400 ms opens a handful of sockets at a time and
//! closes them immediately. That is a workload a thread-per-connection server
//! handles comfortably, and it avoids both an event loop and a runtime.
//!
//! The handler is shared across connection threads, so it must be `Sync`; each
//! application closes over its own `Arc<State>`.

use crate::json::{self, Json};
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::time::Duration;

pub const JSON: &str = "application/json; charset=utf-8";
pub const HTML: &str = "text/html; charset=utf-8";
pub const TEXT: &str = "text/plain; charset=utf-8";

/// A parsed request. The body is raw bytes: JSON decoding is opt-in.
pub struct Request {
    pub method: String,
    pub path: String,
    pub query: String,
    pub body: Vec<u8>,
}

impl Request {
    /// One query parameter, percent-decoded.
    pub fn query(&self, key: &str) -> Option<String> {
        self.query.split('&').find_map(|pair| {
            let (k, v) = pair.split_once('=')?;
            if percent_decode(k) == key {
                Some(percent_decode(v))
            } else {
                None
            }
        })
    }

    /// The body as JSON. An empty body is an empty object, so `POST` handlers
    /// do not have to special-case "no arguments".
    pub fn json_body(&self) -> Result<Json, String> {
        let text = String::from_utf8_lossy(&self.body);
        if text.trim().is_empty() {
            return Ok(Json::Obj(Vec::new()));
        }
        json::parse(&text).map_err(|e| format!("bad json: {e}"))
    }

    pub fn is(&self, method: &str) -> bool {
        self.method.eq_ignore_ascii_case(method)
    }
}

/// Status, content type, body.
pub type Response = (u16, &'static str, Vec<u8>);

pub fn serve<H>(addr: &str, handler: H) -> io::Result<()>
where
    H: Fn(&[String], &Request) -> Response + Send + Sync + 'static,
{
    serve_listener(TcpListener::bind(addr)?, handler)
}

/// Same as [`serve`], on an already bound listener.
///
/// Tests bind port 0 and ask the OS which port they got, which is the only
/// reliable way to run several servers side by side.
pub fn serve_listener<H>(listener: TcpListener, handler: H) -> io::Result<()>
where
    H: Fn(&[String], &Request) -> Response + Send + Sync + 'static,
{
    let handler = Arc::new(handler);
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let handler = Arc::clone(&handler);
                // A browser opening six sockets at once must not serialise
                // behind one slow request, so each connection gets a thread.
                let _ = std::thread::Builder::new()
                    .name("retexis-http".to_string())
                    .spawn(move || {
                        let _ = handle(stream, handler.as_ref());
                    });
            }
            // A failed accept (EMFILE, a reset between accept and read) must
            // not kill the server: the next connection may be fine.
            Err(_) => continue,
        }
    }
    Ok(())
}

fn handle<H>(mut stream: TcpStream, handler: &H) -> io::Result<()>
where
    H: Fn(&[String], &Request) -> Response,
{
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

    let (status, ctype, body) = handler(&parts, &req);
    write_response(&mut stream, status, ctype, &body)
}

pub fn ok() -> Response {
    json_response(Json::obj([("ok", Json::Bool(true))]))
}

pub fn json_response(v: Json) -> Response {
    (200, JSON, v.encode().into_bytes())
}

pub fn html(bytes: impl Into<Vec<u8>>) -> Response {
    (200, HTML, bytes.into())
}

pub fn text(body: impl Into<String>) -> Response {
    (200, TEXT, body.into().into_bytes())
}

pub fn error(status: u16, message: &str) -> Response {
    (
        status,
        JSON,
        Json::obj([("error", Json::Str(message.to_string()))])
            .encode()
            .into_bytes(),
    )
}

/// Parses the body as JSON and maps both a bad body and a handler rejection to
/// a 400 with the message in it.
pub fn with_body(req: &Request, f: impl FnOnce(&Json) -> Result<Response, String>) -> Response {
    let body = match req.json_body() {
        Ok(v) => v,
        Err(e) => return error(400, &e),
    };
    match f(&body) {
        Ok(r) => r,
        Err(e) => error(400, &e),
    }
}

pub fn percent_decode(s: &str) -> String {
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
        // A header block this large is not a request we serve.
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
    // A megabyte is far more than any of these APIs needs, and refusing it
    // here means a hostile page cannot make us allocate without bound.
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
        let req = |q: &str| Request {
            method: "GET".into(),
            path: "/".into(),
            query: q.to_string(),
            body: Vec::new(),
        };
        assert_eq!(
            req("log_since=12").query("log_since").as_deref(),
            Some("12")
        );
        assert_eq!(
            req("a=1&log_since=3").query("log_since").as_deref(),
            Some("3")
        );
        assert_eq!(req("a=1").query("log_since"), None);
        assert_eq!(req("q=%E4%BD%A0").query("q").as_deref(), Some("你"));
    }

    #[test]
    fn an_empty_body_is_an_empty_object() {
        let req = Request {
            method: "POST".into(),
            path: "/".into(),
            query: String::new(),
            body: Vec::new(),
        };
        assert_eq!(req.json_body().unwrap(), Json::Obj(Vec::new()));
    }

    #[test]
    fn a_bad_body_is_an_error_not_a_panic() {
        let req = Request {
            method: "POST".into(),
            path: "/".into(),
            query: String::new(),
            body: b"{not json".to_vec(),
        };
        assert!(req.json_body().is_err());
    }

    #[test]
    fn header_terminator_is_found() {
        assert_eq!(find(b"GET / HTTP/1.1\r\n\r\nbody", b"\r\n\r\n"), Some(14));
        assert_eq!(find(b"no terminator", b"\r\n\r\n"), None);
    }
}
