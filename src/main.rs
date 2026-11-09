//! Disposable FunctionRef target for Phase 18E acceptance.
//! It uses the invocation token against FIBROUS_SERVICES_URL. It has no service secret.
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::time::Duration;

fn main() {
    let port: u16 = std::env::var("PORT").ok().and_then(|s| s.parse().ok()).unwrap_or(8080);
    let listener = TcpListener::bind(("0.0.0.0", port)).expect("bind");
    for mut stream in listener.incoming().flatten() {
        let _ = handle(&mut stream);
    }
}

fn handle(stream: &mut TcpStream) {
    let _ = stream.set_read_timeout(Some(Duration::from_secs(8)));
    let buf = read_http(stream);
    if buf.is_empty() {
        return;
    }
    let req = String::from_utf8_lossy(&buf);
    let mut lines = req.lines();
    let request = lines.next().unwrap_or("");
    let mut parts = request.split_whitespace();
    let method = parts.next().unwrap_or("GET");
    let path = parts.next().unwrap_or("/").split('?').next().unwrap_or("/");
    if method == "GET" && (path == "/health" || path == "/healthz") {
        let _ = write!(stream, "HTTP/1.1 200 OK\r\ncontent-type: text/plain\r\ncontent-length: 2\r\nconnection: close\r\n\r\nok");
        return;
    }
    if method == "POST" && path == "/invoke" {
        let body = req.split("\r\n\r\n").nth(1).unwrap_or("");
        let ctx = json_object(body, "context");
        let token = json_string(body, "token");
        let req_body = json_object(body, "body");
        let op = json_string(&req_body, "op");
        let op_result = match op.as_str() {
            "integration" => call_integration(&token, &req_body),
            "data_get" => call_data("GET", &token, &req_body),
            "data_put" => call_data("POST", &token, &req_body),
            _ => r#"{"status":0,"body":""}"#.to_string(),
        };
        let out_body = format!(
            "{{\"ok\":true,\"app_user_id\":{},\"email_verified\":{},\"tenant_id\":{},\"authority_scope\":{},\"principal_kind\":{},\"op\":{},\"result\":{}}}",
            json_raw(&ctx, "app_user_id"),
            json_bool(&ctx, "email_verified"),
            json_raw(&ctx, "tenant_id"),
            json_raw(&ctx, "authority_scope"),
            json_raw(&ctx, "principal_kind"),
            json_quote(&op),
            op_result,
        );
        let envelope = format!(
            "{{\"status\":200,\"content_type\":\"application/json\",\"headers\":{{}},\"body\":{out_body}}}"
        );
        let _ = write!(
            stream,
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
            envelope.len(),
            envelope
        );
        return;
    }
    let _ = write!(stream, "HTTP/1.1 404 Not Found\r\ncontent-length: 0\r\nconnection: close\r\n\r\n");
}

fn call_integration(token: &str, req_body: &str) -> String {
    let id = json_string(req_body, "integration_id");
    let path = json_string(req_body, "path");
    let method = {
        let m = json_string(req_body, "method");
        if m.is_empty() { "POST".into() } else { m }
    };
    let payload = format!(
        "{{\"method\":{},\"path\":{},\"body\":{{\"marker\":\"18e\"}}}}",
        json_quote(&method),
        json_quote(&path),
    );
    let (status, text) = gateway("POST", &format!("/v1/integrations/{id}/invoke"), token, &payload);
    format!("{{\"status\":{status},\"body\":{}}}", json_quote(&truncate(&text)))
}

fn call_data(method: &str, token: &str, req_body: &str) -> String {
    let collection = json_string(req_body, "collection");
    let id = json_string(req_body, "id");
    let path = if method == "GET" {
        format!("/v1/data/{collection}/{id}")
    } else {
        format!("/v1/data/{collection}")
    };
    let payload = if method == "GET" {
        String::new()
    } else {
        let doc = json_object(req_body, "record");
        if doc.is_empty() { r#"{"marker":"tenant-a"}"#.into() } else { doc }
    };
    let (status, text) = gateway(method, &path, token, &payload);
    format!("{{\"status\":{status},\"body\":{}}}", json_quote(&truncate(&text)))
}

fn gateway(method: &str, path: &str, token: &str, payload: &str) -> (u16, String) {
    let base = std::env::var("FIBROUS_SERVICES_URL").unwrap_or_else(|_| "http://127.0.0.1:8090".into());
    let host_port = base.trim_start_matches("http://").trim_end_matches('/');
    let mut stream = match TcpStream::connect(host_port) {
        Ok(s) => s,
        Err(_) => return (0, "connect failed".into()),
    };
    let _ = stream.set_read_timeout(Some(Duration::from_secs(8)));
    let req = format!(
        "{method} {path} HTTP/1.1\r\nhost: {host_port}\r\nauthorization: Bearer {token}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{payload}",
        payload.len()
    );
    if stream.write_all(req.as_bytes()).is_err() {
        return (0, "write failed".into());
    }
    let buf = read_http(&mut stream);
    let text = String::from_utf8_lossy(&buf);
    let status = text
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let body = text.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
    (status, body)
}

fn read_http(stream: &mut TcpStream) -> Vec<u8> {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 8192];
    loop {
        match stream.read(&mut tmp) {
            Ok(0) => break,
            Ok(n) => {
                buf.extend_from_slice(&tmp[..n]);
                if let Some(header_end) = find_header_end(&buf) {
                    let need = content_length(&buf[..header_end]).unwrap_or(0);
                    if buf.len() >= header_end + 4 + need || buf.windows(5).any(|w| w == b"close") && need == 0 && buf.len() > header_end + 4 {
                        if need == 0 || buf.len() >= header_end + 4 + need {
                            break;
                        }
                    }
                }
                if buf.len() > 256 * 1024 {
                    break;
                }
            }
            Err(_) => break,
        }
    }
    buf
}

fn find_header_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}

fn content_length(headers: &[u8]) -> Option<usize> {
    let text = String::from_utf8_lossy(headers);
    for line in text.lines() {
        let mut parts = line.splitn(2, ':');
        if parts.next().unwrap_or("").eq_ignore_ascii_case("content-length") {
            return parts.next().unwrap_or("").trim().parse().ok();
        }
    }
    None
}

fn truncate(s: &str) -> String {
    let t = s.replace('\n', " ").replace('\r', "");
    if t.len() > 1500 { t[..1500].to_string() } else { t }
}

fn json_quote(s: &str) -> String {
    let mut out = String::from("\"");
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            c if c.is_control() => out.push(' '),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

fn json_object(body: &str, key: &str) -> String {
    let needle = format!("\"{key}\"");
    let Some(i) = body.find(&needle) else { return String::new(); };
    let rest = &body[i + needle.len()..];
    let Some(rel) = rest.find('{') else {
        return String::new();
    };
    let start = i + needle.len() + rel;
    let mut depth = 0;
    for (n, c) in body[start..].char_indices() {
        match c {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    return body[start..start + n + 1].to_string();
                }
            }
            _ => {}
        }
    }
    String::new()
}

fn json_string(obj: &str, key: &str) -> String {
    let needle = format!("\"{key}\"");
    let Some(i) = obj.find(&needle) else { return String::new(); };
    let rest = obj[i + needle.len()..].trim_start().trim_start_matches(':').trim_start();
    let Some(stripped) = rest.strip_prefix('"') else { return String::new(); };
    let mut out = String::new();
    let mut chars = stripped.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            if let Some(n) = chars.next() { out.push(n); }
            continue;
        }
        if c == '"' { break; }
        out.push(c);
    }
    out
}

fn json_raw(obj: &str, key: &str) -> String {
    let needle = format!("\"{key}\"");
    let Some(i) = obj.find(&needle) else { return "null".into(); };
    let rest = obj[i + needle.len()..].trim_start().trim_start_matches(':').trim_start();
    if rest.starts_with("null") { return "null".into(); }
    if rest.starts_with('"') { return json_quote(&json_string(obj, key)); }
    "null".into()
}

fn json_bool(obj: &str, key: &str) -> &'static str {
    let needle = format!("\"{key}\"");
    match obj.find(&needle) {
        Some(i) => {
            let rest = obj[i + needle.len()..].trim_start().trim_start_matches(':').trim_start();
            if rest.starts_with("true") { "true" } else { "false" }
        }
        None => "false",
    }
}
