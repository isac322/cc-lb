use super::*;

const INPUT_TOKENS_MARKER: &str = "x-cc-lb-emulate";
const INPUT_TOKENS_MARKER_VALUE: &str = "input-tokens-ws";

pub(crate) struct InputTokensWebsocketDispatch {
    inner: Arc<dyn UpstreamDispatch>,
    dns_resolver: cc_lb_engine::MetricDnsResolver,
    connect_timeout: Duration,
}

impl InputTokensWebsocketDispatch {
    pub(crate) fn new(
        inner: Arc<dyn UpstreamDispatch>,
        dns_resolver: cc_lb_engine::MetricDnsResolver,
        connect_timeout: Duration,
    ) -> Self {
        Self {
            inner,
            dns_resolver,
            connect_timeout,
        }
    }
}

fn dispatch_error(message: impl Into<String>) -> cc_lb_engine::DispatchError {
    cc_lb_engine::DispatchError::Transport {
        source: Box::new(io::Error::other(message.into())),
    }
}

fn json_response(status: StatusCode, value: serde_json::Value) -> Response<Body> {
    let body = serde_json::to_vec(&value)
        .unwrap_or_else(|_| b"{\"error\":{\"type\":\"server_error\"}}".to_vec());
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(Body::from(body))
        .expect("valid JSON response")
}

fn provider_error_response(status: StatusCode, value: serde_json::Value) -> Response<Body> {
    let value = if value.get("error").is_some() {
        value
    } else {
        serde_json::json!({
            "error": {
                "type": "server_error",
                "message": value.to_string(),
            }
        })
    };
    json_response(status, value)
}

fn response_failed_public(value: &serde_json::Value) -> (StatusCode, serde_json::Value) {
    let error = value.pointer("/response/error").cloned().unwrap_or_else(
        || serde_json::json!({"type":"server_error","message":"provider response failed"}),
    );
    let status = error
        .get("status")
        .and_then(serde_json::Value::as_u64)
        .and_then(|status| StatusCode::from_u16(status as u16).ok())
        .unwrap_or(StatusCode::BAD_GATEWAY);
    let public = serde_json::json!({"error": {
        "type": error.get("type").and_then(serde_json::Value::as_str).unwrap_or("server_error"),
        "code": error.get("code"),
        "message": error.get("message").and_then(serde_json::Value::as_str).unwrap_or("provider response failed")
    }});
    (status, public)
}

fn top_level_error_public(value: &serde_json::Value) -> (StatusCode, serde_json::Value) {
    let status = value
        .get("status")
        .and_then(serde_json::Value::as_u64)
        .and_then(|status| StatusCode::from_u16(status as u16).ok())
        .unwrap_or(StatusCode::BAD_GATEWAY);
    let public = value.get("error").cloned().unwrap_or_else(
        || serde_json::json!({"type":"server_error","message":"provider WebSocket error"}),
    );
    (status, serde_json::json!({"error": public}))
}

async fn emulate_input_tokens(
    request: SignedRequest,
    dns_resolver: &cc_lb_engine::MetricDnsResolver,
    connect_timeout: Duration,
) -> Result<Response<Body>, cc_lb_engine::DispatchError> {
    let mut url = request.url().clone();
    let path = url
        .path()
        .strip_suffix("/input_tokens")
        .ok_or_else(|| dispatch_error("input_tokens emulation marker used on an unexpected URL"))?
        .to_owned();
    if path.is_empty() {
        return Err(dispatch_error(
            "input_tokens emulation URL has no responses path",
        ));
    }
    url.set_path(&path);
    let scheme = match url.scheme() {
        "https" | "wss" => "wss",
        "http" | "ws" => "ws",
        _ => {
            return Err(dispatch_error(
                "unsupported input_tokens WebSocket URL scheme",
            ));
        }
    };
    url.set_scheme(scheme)
        .map_err(|_| dispatch_error("invalid input_tokens WebSocket URL"))?;

    let mut body: serde_json::Value = sonic_rs::from_slice(request.body())
        .map_err(|_| dispatch_error("input_tokens request body is not valid JSON"))?;
    let object = body
        .as_object_mut()
        .ok_or_else(|| dispatch_error("input_tokens request body must be an object"))?;
    object.remove("stream");
    object.insert("generate".to_owned(), serde_json::Value::Bool(false));
    object.insert("store".to_owned(), serde_json::Value::Bool(false));
    let mut frame = serde_json::Map::new();
    frame.insert(
        "type".to_owned(),
        serde_json::Value::String("response.create".to_owned()),
    );
    for (key, value) in object.iter() {
        frame.insert(key.clone(), value.clone());
    }
    let frame = serde_json::to_string(&serde_json::Value::Object(frame))
        .map_err(|_| dispatch_error("could not encode input_tokens WebSocket frame"))?;

    let mut ws_request = url
        .as_str()
        .into_client_request()
        .map_err(|_| dispatch_error("could not build input_tokens WebSocket request"))?;
    for name in [
        http::header::AUTHORIZATION.as_str(),
        "chatgpt-account-id",
        "originator",
        "version",
        "user-agent",
        "session_id",
    ] {
        if let Some(value) = request.headers().get(name) {
            ws_request.headers_mut().insert(
                HeaderName::from_bytes(name.as_bytes()).expect("static header name"),
                value.clone(),
            );
        }
    }
    ws_request.headers_mut().insert(
        HeaderName::from_static("openai-beta"),
        HeaderValue::from_static("responses_websockets=2026-02-06"),
    );
    let host = ws_request
        .uri()
        .host()
        .ok_or_else(|| dispatch_error("input_tokens WebSocket URL missing host"))?
        .to_owned();
    let port = ws_request.uri().port_u16().unwrap_or_else(|| {
        if ws_request.uri().scheme_str() == Some("wss") {
            443
        } else {
            80
        }
    });
    let connection = tokio::time::timeout(connect_timeout, async move {
        let dial_deadline = tokio::time::Instant::now() + connect_timeout;
        let addrs = dns_resolver
            .resolve_host_progressive(&host, port)
            .await
            .map_err(|err| {
                tokio_tungstenite::tungstenite::Error::Io(io::Error::other(err.to_string()))
            })?;
        let tcp = cc_lb_engine::dns_cache::connect_tcp_with_progressive_fallback(
            addrs,
            Some(dial_deadline),
        )
        .await
        .map_err(tokio_tungstenite::tungstenite::Error::Io)?;
        client_async_tls_with_config(ws_request, tcp, None, None).await
    })
    .await
    .map_err(|_| dispatch_error("input_tokens WebSocket handshake timed out"))?;
    let (mut ws, _) = match connection {
        Ok(connection) => connection,
        Err(tokio_tungstenite::tungstenite::Error::Http(response)) => {
            let status = response.status();
            let body = response.body().as_ref().map_or_else(
                || serde_json::json!({"error":{"type":"upstream_unavailable","message":"provider WebSocket handshake rejected"}}),
                |bytes| sonic_rs::from_slice(bytes).unwrap_or_else(|_| serde_json::json!({"error":{"type":"upstream_unavailable","message":"provider WebSocket handshake rejected"}})),
            );
            return Ok(provider_error_response(status, body));
        }
        Err(error) => {
            return Err(dispatch_error(format!(
                "input_tokens WebSocket connection failed: {error}"
            )));
        }
    };
    ws.send(UpstreamMessage::Text(frame.into()))
        .await
        .map_err(|error| dispatch_error(format!("input_tokens WebSocket send failed: {error}")))?;

    while let Some(message) = tokio::time::timeout(Duration::from_secs(30), ws.next())
        .await
        .map_err(|_| dispatch_error("input_tokens WebSocket read timed out"))?
    {
        let message = message.map_err(|error| {
            dispatch_error(format!("input_tokens WebSocket receive failed: {error}"))
        })?;
        let payload = match message {
            UpstreamMessage::Text(value) => Bytes::from(value.as_bytes().to_vec()),
            UpstreamMessage::Binary(value) => value,
            UpstreamMessage::Ping(value) => {
                ws.send(UpstreamMessage::Pong(value))
                    .await
                    .map_err(|error| {
                        dispatch_error(format!("input_tokens WebSocket pong failed: {error}"))
                    })?;
                continue;
            }
            UpstreamMessage::Pong(_) => continue,
            UpstreamMessage::Close(_) => break,
            _ => continue,
        };
        let value: serde_json::Value = match sonic_rs::from_slice(&payload) {
            Ok(value) => value,
            Err(_) => continue,
        };
        match value.get("type").and_then(serde_json::Value::as_str) {
            Some("response.completed") => {
                let input_tokens = value
                    .pointer("/response/usage/input_tokens")
                    .and_then(serde_json::Value::as_u64)
                    .ok_or_else(|| dispatch_error("input_tokens provider response had no usage"))?;
                let response = json_response(
                    StatusCode::OK,
                    serde_json::json!({"object":"response.input_tokens","input_tokens":input_tokens}),
                );
                let _ = ws.close(None).await;
                return Ok(response);
            }
            Some("response.failed") => {
                let (status, public) = response_failed_public(&value);
                let _ = ws.close(None).await;
                return Ok(json_response(status, public));
            }
            Some("error") => {
                let (status, public) = top_level_error_public(&value);
                let _ = ws.close(None).await;
                return Ok(json_response(status, public));
            }
            _ => {}
        }
    }
    Err(dispatch_error(
        "input_tokens WebSocket ended before response.completed",
    ))
}

#[async_trait]
impl UpstreamDispatch for InputTokensWebsocketDispatch {
    async fn dispatch(
        &self,
        request: SignedRequest,
    ) -> Result<Response<Body>, cc_lb_engine::DispatchError> {
        let emulate = request
            .headers()
            .get(INPUT_TOKENS_MARKER)
            .and_then(|value| value.to_str().ok())
            == Some(INPUT_TOKENS_MARKER_VALUE);
        if emulate {
            emulate_input_tokens(request, &self.dns_resolver, self.connect_timeout).await
        } else {
            self.inner.dispatch(request).await
        }
    }

    async fn dispatch_before(
        &self,
        request: SignedRequest,
        deadline: &cc_lb_engine::attempt_rail::DispatchDeadline,
    ) -> Result<Response<Body>, cc_lb_engine::attempt_rail::DispatchBeforeError> {
        deadline.check()?;
        self.dispatch(request).await.map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn response_failed_maps_only_public_failure_fields() {
        let (status, body) = response_failed_public(&serde_json::json!({
            "type": "response.failed",
            "response": {
                "error": {
                    "status": 422,
                    "type": "invalid_request_error",
                    "code": "bad_input",
                    "message": "invalid input",
                    "secret": "must not leak"
                }
            }
        }));
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(body["error"]["code"], "bad_input");
        assert_eq!(body["error"]["message"], "invalid input");
        assert!(body["error"].get("secret").is_none());
        assert!(body.get("response").is_none());
    }

    #[test]
    fn top_level_error_uses_status_and_public_error_object() {
        let (status, body) = top_level_error_public(&serde_json::json!({
            "type": "error",
            "status": 429,
            "error": {"type": "rate_limit_error", "message": "slow down"}
        }));
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(body["error"]["type"], "rate_limit_error");
        assert_eq!(body["error"]["message"], "slow down");
    }
}
