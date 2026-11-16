use super::*;
use std::{cell::Cell, time::Duration};
use futures_util::StreamExt;
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::{TcpListener, TcpStream}, sync::oneshot};

fn request_for(address: std::net::SocketAddr) -> (Request<Full<Bytes>>, String) {
    let mut headers = HeaderMap::new();
    headers.insert(header::AUTHORIZATION, HeaderValue::from_static("Bearer synthetic-owner"));
    headers.insert("x-codex-server-id", HeaderValue::from_static("synthetic-server"));
    headers.insert("sec-websocket-key", HeaderValue::from_static("must-be-regenerated"));
    headers.insert("sec-websocket-protocol", HeaderValue::from_static("must-not-be-offered"));
    headers.insert("sec-websocket-extensions", HeaderValue::from_static("must-not-be-offered"));
    upgrade_request(format!("http://{address}/remote/control?cursor=one").parse().unwrap(),
        Method::GET, headers, Bytes::new()).unwrap()
}

async fn read_request(socket: &mut TcpStream) -> String {
    let mut bytes = Vec::new();
    while !bytes.ends_with(b"\r\n\r\n") {
        assert!(bytes.len() < 8192);
        bytes.push(socket.read_u8().await.unwrap());
    }
    let request = String::from_utf8(bytes).unwrap();
    let lower = request.to_ascii_lowercase();
    assert!(request.starts_with("GET /remote/control?cursor=one HTTP/1.1\r\n"));
    assert!(lower.contains("authorization: bearer synthetic-owner\r\n"));
    assert!(lower.contains("x-codex-server-id: synthetic-server\r\n"));
    assert!(lower.contains("sec-websocket-version: 13\r\n"));
    assert!(lower.contains("connection: upgrade\r\n"));
    assert!(lower.contains("upgrade: websocket\r\n"));
    assert!(!lower.contains("sec-websocket-protocol:"));
    assert!(!lower.contains("sec-websocket-extensions:"));
    assert!(!lower.contains("must-be-regenerated"));
    request
}

async fn no_second_connection(listener: &TcpListener) {
    assert!(tokio::time::timeout(Duration::from_millis(30), listener.accept()).await.is_err(),
        "handshake must not reconnect or retry");
}

// The provider waits for the client's header callback before sending later
// chunks, ensuring that a rejection is not accidentally tested only as bytes
// overread alongside the HTTP headers.
async fn rejection(chunks: Vec<Vec<u8>>, cap: usize) -> (Result<Outcome, HandshakeError>, usize) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (request, accept) = request_for(listener.local_addr().unwrap());
    let client = Client::new();
    let calls = Cell::new(0);
    let (started_tx, started_rx) = oneshot::channel();
    let connect = client.exchange(request, accept, cap, Instant::now() + Duration::from_secs(2), |_, _| {
        calls.set(calls.get() + 1);
        let _ = started_tx.send(());
    });
    let serve = async {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_request(&mut socket).await;
        let mut chunks = chunks.into_iter();
        socket.write_all(&chunks.next().unwrap()).await.unwrap();
        started_rx.await.unwrap();
        for chunk in chunks {
            socket.write_all(&chunk).await.unwrap();
        }
        socket.shutdown().await.unwrap();
    };
    let (result, ()) = tokio::join!(connect, serve);
    no_second_connection(&listener).await;
    (result, calls.get())
}

async fn rejected_body(result: Result<Outcome, HandshakeError>) -> (StatusCode, HeaderMap, Bytes) {
    let response = match result {
        Ok(Outcome::Rejected(response)) => response,
        Ok(Outcome::Accepted(..)) => panic!("expected original HTTP rejection"),
        Err(error) => panic!("unexpected constant handshake error: {error}"),
    };
    let (parts, body) = response.into_parts();
    let body = body.collect().await.unwrap().to_bytes();
    (parts.status, parts.headers, body)
}

#[tokio::test]
async fn split_large_content_length_rejection_keeps_full_entity_and_sanitizes_framing() {
    let body = vec![b'x'; 64 * 1024];
    let headers = format!("HTTP/1.1 403 Forbidden\r\nContent-Length: {}\r\nContent-Type: application/json\r\nConnection: close, x-hop-one\r\nConnection: x-hop-two\r\nX-Hop-One: discard\r\nX-Hop-Two: discard\r\nX-Request-Id: original\r\n\r\n", body.len());
    let mut chunks = vec![headers.into_bytes()];
    chunks.extend(body.chunks(3001).map(<[u8]>::to_vec));
    let (result, calls) = rejection(chunks, 128 * 1024).await;
    assert_eq!(calls, 1);
    let (status, headers, actual) = rejected_body(result).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(actual.as_ref(), body);
    assert_eq!(headers[header::CONTENT_LENGTH], body.len().to_string());
    assert_eq!(headers[header::CONTENT_TYPE], "application/json");
    assert_eq!(headers["x-request-id"], "original");
    for name in ["connection", "x-hop-one", "x-hop-two", "transfer-encoding"] {
        assert!(!headers.contains_key(name));
    }
}

#[tokio::test]
async fn chunked_rejection_decodes_chunks_and_removes_trailer_framing() {
    let (result, calls) = rejection(vec![
        b"HTTP/1.1 429 Too Many Requests\r\nTransfer-Encoding: chunked\r\nTrailer: X-Trailer\r\nRetry-After: 7\r\n\r\n".to_vec(),
        b"4\r\nfull\r\n".to_vec(),
        b"5;ignored=yes\r\n body\r\n0\r\nX-Trailer: ignored\r\n\r\n".to_vec(),
    ], 1024).await;
    assert_eq!(calls, 1);
    let (status, headers, body) = rejected_body(result).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(body, "full body");
    assert_eq!(headers[header::CONTENT_LENGTH], "9");
    assert_eq!(headers[header::RETRY_AFTER], "7");
    assert!(!headers.contains_key(header::TRANSFER_ENCODING));
    assert!(!headers.contains_key(header::TRAILER));
    assert!(!headers.contains_key("x-trailer"));
}

#[tokio::test]
async fn eof_delimited_rejection_reads_until_original_connection_closes() {
    let (result, calls) = rejection(vec![
        b"HTTP/1.1 401 Unauthorized\r\nConnection: close\r\n\r\n".to_vec(),
        b"first ".to_vec(), b"second".to_vec(),
    ], 1024).await;
    assert_eq!(calls, 1);
    let (status, headers, body) = rejected_body(result).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(body, "first second");
    assert_eq!(headers[header::CONTENT_LENGTH], "12");
}

#[tokio::test]
async fn successful_upgrade_retains_coalesced_first_frame_on_same_connection() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (request, accept) = request_for(listener.local_addr().unwrap());
    let client = Client::new();
    let calls = Cell::new(0);
    let connect = async {
        let result = client.exchange(request, accept, 1024, Instant::now() + Duration::from_secs(2), |status, _| {
            assert_eq!(status, StatusCode::SWITCHING_PROTOCOLS);
            calls.set(calls.get() + 1);
        }).await;
        let (mut socket, headers) = match result {
            Ok(Outcome::Accepted(socket, headers)) => (socket, headers),
            _ => panic!("expected accepted upgrade"),
        };
        assert_eq!(headers["x-request-id"], "upgrade-original");
        assert_eq!(socket.get_config().max_frame_size, Some(1024));
        assert_eq!(socket.get_config().max_message_size, Some(1024));
        let message = tokio::time::timeout(Duration::from_secs(1), socket.next()).await.unwrap().unwrap().unwrap();
        assert_eq!(message.into_text().unwrap(), "hello");
    };
    let serve = async {
        let (mut socket, _) = listener.accept().await.unwrap();
        let request = read_request(&mut socket).await;
        let key = request.lines().find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("sec-websocket-key").then_some(value.trim())
        }).unwrap();
        let accept = derive_accept_key(key.as_bytes());
        let mut bytes = format!("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: keep-alive, Upgrade\r\nSec-WebSocket-Accept: {accept}\r\nX-Request-Id: upgrade-original\r\n\r\n").into_bytes();
        bytes.extend_from_slice(b"\x81\x05hello");
        socket.write_all(&bytes).await.unwrap();
    };
    tokio::join!(connect, serve);
    assert_eq!(calls.get(), 1);
    no_second_connection(&listener).await;
}

#[tokio::test]
async fn invalid_accept_and_unsolicited_negotiation_fail_closed_after_header_observation() {
    for extra in ["Sec-WebSocket-Protocol: surprise\r\n", "Sec-WebSocket-Extensions: permessage-deflate\r\n", "Sec-WebSocket-Accept: duplicate\r\n"] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (request, accept) = request_for(listener.local_addr().unwrap());
        let response = format!("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n{extra}\r\n");
        let client = Client::new();
        let calls = Cell::new(0);
        let connect = client.exchange(request, accept, 1024, Instant::now() + Duration::from_secs(2), |_, _| calls.set(calls.get() + 1));
        let serve = async {
            let (mut socket, _) = listener.accept().await.unwrap();
            read_request(&mut socket).await;
            socket.write_all(response.as_bytes()).await.unwrap();
        };
        let (result, ()) = tokio::join!(connect, serve);
        assert!(matches!(result, Err(HandshakeError::InvalidResponse)));
        assert_eq!(calls.get(), 1);
        no_second_connection(&listener).await;
    }
    let (result, calls) = rejection(vec![b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: wrong\r\n\r\n".to_vec()], 1024).await;
    assert!(matches!(result, Err(HandshakeError::InvalidResponse)));
    assert_eq!(calls, 1);
}

#[test]
fn upgrade_requires_http11_upgrade_and_connection_headers() {
    let mut headers = HeaderMap::new();
    headers.insert(header::UPGRADE, HeaderValue::from_static("websocket"));
    headers.insert(header::CONNECTION, HeaderValue::from_static("upgrade"));
    headers.insert("sec-websocket-accept", HeaderValue::from_static("expected"));
    assert!(validate_upgrade(Version::HTTP_11, &headers, "expected").is_ok());
    assert_eq!(validate_upgrade(Version::HTTP_10, &headers, "expected"), Err(HandshakeError::InvalidResponse));
    for name in [header::UPGRADE, header::CONNECTION] {
        let mut invalid = headers.clone();
        invalid.remove(name);
        assert_eq!(validate_upgrade(Version::HTTP_11, &invalid, "expected"), Err(HandshakeError::InvalidResponse));
    }
}

#[tokio::test]
async fn redirect_is_an_original_rejection_not_a_second_request() {
    let (result, calls) = rejection(vec![b"HTTP/1.1 307 Temporary Redirect\r\nLocation: http://127.0.0.1:1/do-not-connect\r\nContent-Length: 8\r\n\r\nredirect".to_vec()], 1024).await;
    assert_eq!(calls, 1);
    let (status, headers, body) = rejected_body(result).await;
    assert_eq!(status, StatusCode::TEMPORARY_REDIRECT);
    assert_eq!(headers[header::LOCATION], "http://127.0.0.1:1/do-not-connect");
    assert_eq!(body, "redirect");
}

#[tokio::test]
async fn closed_connection_without_response_is_not_retried() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (request, accept) = request_for(listener.local_addr().unwrap());
    let client = Client::new();
    let calls = Cell::new(0);
    let connect = client.exchange(request, accept, 1024, Instant::now() + Duration::from_secs(2), |_, _| calls.set(calls.get() + 1));
    let serve = async {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_request(&mut socket).await;
        drop(socket);
    };
    let (result, ()) = tokio::join!(connect, serve);
    assert!(matches!(result, Err(HandshakeError::Transport)));
    assert_eq!(calls.get(), 0);
    no_second_connection(&listener).await;
}

#[tokio::test]
async fn rejection_entity_and_header_limits_are_enforced() {
    for response in [
        format!("HTTP/1.1 403 Forbidden\r\nContent-Length: 1025\r\n\r\n{}", "x".repeat(1025)),
        format!("HTTP/1.1 403 Forbidden\r\nX-Large: {}\r\nContent-Length: 0\r\n\r\n", "x".repeat(1025)),
        format!("HTTP/1.1 403 Forbidden\r\nTransfer-Encoding: chunked\r\n\r\n401\r\n{}\r\n0\r\n\r\n", "x".repeat(1025)),
        format!("HTTP/1.1 403 Forbidden\r\nConnection: close\r\n\r\n{}", "x".repeat(1025)),
    ] {
        let (result, calls) = rejection(vec![response.into_bytes()], 1024).await;
        assert!(matches!(result, Err(HandshakeError::Limit)));
        assert_eq!(calls, 1);
    }
}

#[tokio::test]
async fn one_total_deadline_bounds_both_waiting_for_headers_and_entity_completion() {
    for send_headers in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (request, accept) = request_for(listener.local_addr().unwrap());
        let client = Client::new();
        let calls = Cell::new(0);
        let deadline = Instant::now() + Duration::from_millis(100);
        let connect = client.exchange(request, accept, 1024, deadline, |_, _| calls.set(calls.get() + 1));
        let serve = async {
            let (mut socket, _) = listener.accept().await.unwrap();
            read_request(&mut socket).await;
            if send_headers {
                socket.write_all(b"HTTP/1.1 403 Forbidden\r\nContent-Length: 10\r\n\r\npart").await.unwrap();
            }
            tokio::time::sleep_until(deadline + Duration::from_millis(10)).await;
        };
        let (result, ()) = tokio::join!(connect, serve);
        assert!(matches!(result, Err(HandshakeError::Deadline)));
        assert_eq!(calls.get(), usize::from(send_headers));
        no_second_connection(&listener).await;
    }
}
