use axum::body::Body;
use bytes::{Bytes, BytesMut};
use cc_lb_upstream::SignedRequest;
use http::{HeaderMap, HeaderValue, Method, Request, Response, StatusCode, Version, header};
use http_body_util::{BodyExt, Full};
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use hyper_util::{client::legacy::{Client as HttpClient, connect::HttpConnector}, rt::{TokioExecutor, TokioIo}};
use tokio::time::Instant;
use tokio_tungstenite::{WebSocketStream, tungstenite::{handshake::{client::generate_key, derive_accept_key}, protocol::{Role, WebSocketConfig}}};

pub(super) type ProviderSocket = WebSocketStream<TokioIo<hyper::upgrade::Upgraded>>;

#[derive(Clone)]
pub(crate) struct Client {
    connector: HttpsConnector<HttpConnector>,
}

pub(super) enum Outcome {
    Accepted(ProviderSocket, HeaderMap),
    Rejected(Response<Body>),
}

// Never retain an underlying error: transport errors can include signed URLs
// or provider-controlled content, neither of which belongs in diagnostics.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub(super) enum HandshakeError {
    #[error("remote control handshake request invalid")]
    InvalidRequest,
    #[error("remote control handshake transport failed")]
    Transport,
    #[error("remote control handshake response invalid")]
    InvalidResponse,
    #[error("remote control handshake limit exceeded")]
    Limit,
    #[error("remote control handshake deadline exceeded")]
    Deadline,
}

impl Client {
    pub(crate) fn new() -> Self {
        Self {
            connector: HttpsConnectorBuilder::new()
                .with_webpki_roots()
                .https_or_http()
                .enable_http1()
                .build(),
        }
    }

    pub(super) async fn connect(
        &self,
        request: SignedRequest,
        cap: usize,
        deadline: Instant,
        response_started: impl FnOnce(StatusCode, &HeaderMap),
    ) -> Result<Outcome, HandshakeError> {
        let (url, method, headers, body) = request.into_parts();
        let (request, accept) = upgrade_request(url, method, headers, body)?;
        self.exchange(request, accept, cap, deadline, response_started).await
    }

    async fn exchange(
        &self,
        request: Request<Full<Bytes>>,
        accept: String,
        cap: usize,
        deadline: Instant,
        response_started: impl FnOnce(StatusCode, &HeaderMap),
    ) -> Result<Outcome, HandshakeError> {
        if Instant::now() >= deadline {
            return Err(HandshakeError::Deadline);
        }
        if cap == 0 {
            return Err(HandshakeError::Limit);
        }
        // No pooled socket can trigger a reconnect, and even Hyper's normally
        // enabled retry of a cancelled, unwritten request is explicitly off.
        // Hyper requires an 8 KiB minimum parser buffer; the decoded header
        // budget below also enforces smaller caller caps.
        let client: HttpClient<_, Full<Bytes>> = HttpClient::builder(TokioExecutor::new())
            .retry_canceled_requests(false)
            .pool_max_idle_per_host(0)
            .http1_max_buf_size(cap.max(8192))
            .build(self.connector.clone());
        tokio::time::timeout_at(deadline, async move {
            let mut response = client.request(request).await.map_err(|_| HandshakeError::Transport)?;
            response_started(response.status(), response.headers());
            let header_bytes = response.headers().iter().try_fold(0usize, |size, (name, value)| {
                size.checked_add(name.as_str().len())?.checked_add(value.len())?.checked_add(4)
            }).ok_or(HandshakeError::Limit)?;
            if header_bytes > cap {
                return Err(HandshakeError::Limit);
            }
            if response.status() == StatusCode::SWITCHING_PROTOCOLS {
                validate_upgrade(response.version(), response.headers(), &accept)?;
                let headers = response.headers().clone();
                // Upgraded owns Hyper's unread buffer as well as the original
                // connection, so a first frame coalesced with 101 is retained.
                let upgraded = hyper::upgrade::on(&mut response).await.map_err(|_| HandshakeError::Transport)?;
                let config = WebSocketConfig::default()
                    .read_buffer_size(cap.min(8192))
                    .write_buffer_size(0)
                    .max_write_buffer_size(cap.saturating_add(14))
                    .max_message_size(Some(cap))
                    .max_frame_size(Some(cap));
                let socket = WebSocketStream::from_raw_socket(TokioIo::new(upgraded), Role::Client, Some(config)).await;
                return Ok(Outcome::Accepted(socket, headers));
            }
            let (mut parts, mut body) = response.into_parts();
            let mut bytes = BytesMut::new();
            // Hyper, not a second request or a handwritten parser, decodes
            // Content-Length, chunked framing, and close-delimited entities.
            while let Some(frame) = body.frame().await {
                let frame = frame.map_err(|_| HandshakeError::Transport)?;
                if let Ok(data) = frame.into_data() {
                    if data.len() > cap.saturating_sub(bytes.len()) {
                        return Err(HandshakeError::Limit);
                    }
                    bytes.extend_from_slice(&data);
                }
            }
            sanitize_rejection_headers(&mut parts.headers);
            parts.headers.insert(header::CONTENT_LENGTH,
                HeaderValue::from_str(&bytes.len().to_string()).map_err(|_| HandshakeError::InvalidResponse)?);
            Ok(Outcome::Rejected(Response::from_parts(parts, Body::from(bytes.freeze()))))
        }).await.map_err(|_| HandshakeError::Deadline)?
    }
}

fn upgrade_request(
    mut url: url::Url,
    method: Method,
    mut headers: HeaderMap,
    body: Bytes,
) -> Result<(Request<Full<Bytes>>, String), HandshakeError> {
    if method != Method::GET || !body.is_empty() || !url.username().is_empty()
        || url.password().is_some() || url.fragment().is_some() {
        return Err(HandshakeError::InvalidRequest);
    }
    match url.scheme() {
        "ws" => url.set_scheme("http").map_err(|_| HandshakeError::InvalidRequest)?,
        "wss" => url.set_scheme("https").map_err(|_| HandshakeError::InvalidRequest)?,
        "http" | "https" => {},
        _ => return Err(HandshakeError::InvalidRequest),
    }
    // Credentials are already owner-signed. Only transport/negotiation fields
    // are regenerated; no downstream credential is consulted or substituted.
    for field in ["host", "content-length", "transfer-encoding", "trailer", "te",
        "sec-websocket-accept", "sec-websocket-protocol", "sec-websocket-extensions"] {
        headers.remove(field);
    }
    let key = generate_key();
    let accept = derive_accept_key(key.as_bytes());
    headers.insert(header::CONNECTION, HeaderValue::from_static("Upgrade"));
    headers.insert(header::UPGRADE, HeaderValue::from_static("websocket"));
    headers.insert("sec-websocket-version", HeaderValue::from_static("13"));
    headers.insert("sec-websocket-key", HeaderValue::from_str(&key).map_err(|_| HandshakeError::InvalidRequest)?);
    let mut request = Request::builder().method(method).uri(url.as_str()).version(Version::HTTP_11)
        .body(Full::new(body)).map_err(|_| HandshakeError::InvalidRequest)?;
    *request.headers_mut() = headers;
    Ok((request, accept))
}

fn validate_upgrade(version: Version, headers: &HeaderMap, accept: &str) -> Result<(), HandshakeError> {
    let mut upgrades = headers.get_all(header::UPGRADE).iter();
    let mut accepts = headers.get_all("sec-websocket-accept").iter();
    if version != Version::HTTP_11
        || !upgrades.next().is_some_and(|value| value.as_bytes().eq_ignore_ascii_case(b"websocket"))
        || upgrades.next().is_some()
        || !accepts.next().is_some_and(|value| value.as_bytes() == accept.as_bytes())
        || accepts.next().is_some()
        || !headers.get_all(header::CONNECTION).iter().any(|value| {
            value.as_bytes().split(|byte| *byte == b',').any(|token| token.trim_ascii().eq_ignore_ascii_case(b"upgrade"))
        })
        || headers.contains_key("sec-websocket-protocol")
        || headers.contains_key("sec-websocket-extensions") {
        return Err(HandshakeError::InvalidResponse);
    }
    Ok(())
}

fn sanitize_rejection_headers(headers: &mut HeaderMap) {
    // Connection-nominated fields are hop-local too, including arbitrary names.
    let nominated: Vec<_> = headers.get_all(header::CONNECTION).iter()
        .flat_map(|value| value.as_bytes().split(|byte| *byte == b','))
        .filter_map(|token| header::HeaderName::from_bytes(token.trim_ascii()).ok())
        .collect();
    for name in nominated {
        headers.remove(name);
    }
    headers.remove(header::CONNECTION);
    for field in ["transfer-encoding", "content-length", "trailer", "te", "upgrade",
        "keep-alive", "proxy-authenticate", "proxy-authorization", "proxy-connection"] {
        headers.remove(field);
    }
}

#[cfg(test)]
#[path = "codex_remote_control_handshake_tests.rs"]
mod tests;
