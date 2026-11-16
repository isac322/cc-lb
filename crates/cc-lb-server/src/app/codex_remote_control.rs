//! Remote-control admission, separate from pooled model dispatch.
//!
//! Account requests use a live owner fence and stored enrollment scope. Relay
//! requests additionally require a stored target proof and the atomic relay
//! dispatch fence before provider ciphertext can be decrypted. Requests never
//! enter the generic model pool or fail over to a different owner.

use axum::{Router, body::Body};
use http::{HeaderMap, HeaderValue, Method, Response, StatusCode};
use serde_json::Value;

use super::{ProxyState, lifecycle_handler};
use cc_lb_engine::{Authenticated, LifecycleContext, TerminalClassification};

#[path = "codex_remote_control_transport.rs"]
mod transport;
#[path = "codex_remote_control_socket.rs"]
mod socket;
#[path = "codex_remote_control_handshake.rs"]
mod handshake;
pub(super) use handshake::Client as WebSocketClient;

pub(super) const ENROLL: &str = "/backend-api/wham/remote/control/server/enroll";
pub(super) const REFRESH: &str = "/backend-api/wham/remote/control/server/refresh";
pub(super) const CLIENTS: &str = "/backend-api/wham/remote/control/environments/{environment_id}/clients";
pub(super) const CLIENT: &str = "/backend-api/wham/remote/control/environments/{environment_id}/clients/{client_id}";
pub(super) const PAIR: &str = "/backend-api/wham/remote/control/server/pair";
pub(super) const PAIR_STATUS: &str = "/backend-api/wham/remote/control/server/pair/status";
pub(super) const SERVER: &str = "/backend-api/wham/remote/control/server";

pub(super) fn route_template(path: &str) -> Option<&'static str> {
    match path {
        ENROLL => return Some(ENROLL),
        REFRESH => return Some(REFRESH),
        PAIR => return Some(PAIR),
        PAIR_STATUS => return Some(PAIR_STATUS),
        SERVER => return Some(SERVER),
        _ => {}
    }
    let mut segments = path.strip_prefix("/backend-api/wham/remote/control/environments/")?.split('/');
    if segments.next()?.is_empty() || segments.next()? != "clients" { return None; }
    match (segments.next(), segments.next()) {
        (None, None) => Some(CLIENTS),
        (Some(client), None) if !client.is_empty() => Some(CLIENT),
        _ => None,
    }
}

pub(super) fn register(routes: Router<ProxyState>) -> Router<ProxyState> {
    routes
        .route(ENROLL, axum::routing::post(lifecycle_handler))
        .route(REFRESH, axum::routing::post(lifecycle_handler))
        .route(CLIENTS, axum::routing::get(lifecycle_handler))
        .route(CLIENT, axum::routing::delete(lifecycle_handler))
        .route(PAIR, axum::routing::post(lifecycle_handler))
        .route(PAIR_STATUS, axum::routing::post(lifecycle_handler))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Operation {
    Enroll,
    Refresh,
    Clients,
    RevokeClient,
    Pair,
    PairStatus,
    Server,
}

impl Operation {
    pub(super) fn from_registered_path(path: &str) -> Option<Self> {
        match path {
            ENROLL => Some(Self::Enroll),
            REFRESH => Some(Self::Refresh),
            CLIENTS => Some(Self::Clients),
            CLIENT => Some(Self::RevokeClient),
            PAIR => Some(Self::Pair),
            PAIR_STATUS => Some(Self::PairStatus),
            SERVER => Some(Self::Server),
            _ => None,
        }
    }

    fn method(self) -> Method {
        match self {
            Self::Enroll | Self::Refresh | Self::Pair | Self::PairStatus => Method::POST,
            Self::Clients | Self::Server => Method::GET,
            Self::RevokeClient => Method::DELETE,
        }
    }
}

/// The capped request body is parsed once, only after authentication.
pub(super) struct ValidatedBody {
    bytes: bytes::Bytes,
    value: Option<Value>,
}

/// Never forward these paths to Lifecycle::handle: it is a model-pool
/// dispatcher, not an enrollment-scoped relay executor.
pub(super) async fn handle(
    state: &ProxyState,
    auth: &Authenticated,
    operation: Operation,
    parts: &mut http::request::Parts,
    body: bytes::Bytes,
    observer: Option<&LifecycleContext>,
) -> Response<Body> {
    // Axum GET routes also match HEAD. HEAD must not inherit RC GET authority
    // or reach the generic lifecycle even if account purpose admission allows it.
    if parts.method != operation.method() {
        let mut response = local_error(StatusCode::METHOD_NOT_ALLOWED,
            "method_not_allowed", "method is not registered for remote control", observer);
        response.headers_mut().insert(http::header::ALLOW,
            HeaderValue::from_static(match operation.method() {
                Method::GET => "GET",
                Method::DELETE => "DELETE",
                _ => "POST",
            }));
        return response;
    }
    let value = match validate_request(operation, &parts.headers, &body) {
        Ok(value) => value,
        Err(()) => return local_error(StatusCode::BAD_REQUEST, "invalid_remote_control_request",
            "invalid remote control payload or identity", observer),
    };
    transport::dispatch(state, auth, operation, parts, ValidatedBody { bytes: body, value }, observer).await
}

fn local_error(
    status: StatusCode,
    code: &'static str,
    reason: &'static str,
    observer: Option<&LifecycleContext>,
) -> Response<Body> {
    let mut response = Response::new(Body::from(
        serde_json::json!({"error": code, "reason": reason}).to_string(),
    ));
    *response.status_mut() = status;
    response.headers_mut().insert(http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"));
    response.headers_mut().insert(http::header::CACHE_CONTROL,
        HeaderValue::from_static("no-store"));
    response.headers_mut().insert("cc-lb-error-origin", HeaderValue::from_static("proxy"));
    let classification = TerminalClassification::local(status);
    response.extensions_mut().insert(classification);
    if let Some(observer) = observer {
        observer.terminate_local(classification);
    }
    response
}

fn nonempty_string<'a>(body: &'a Value, field: &str) -> Result<&'a str, ()> {
    body.get(field).and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty()).ok_or(())
}

fn single_header<'a>(headers: &'a HeaderMap, field: &'static str) -> Result<&'a str, ()> {
    let mut values = headers.get_all(field).iter();
    let value = values.next().ok_or(())?.to_str().map_err(|_| ())?;
    if values.next().is_some() || value.trim().is_empty() {
        return Err(());
    }
    Ok(value)
}

fn validate_request(operation: Operation, headers: &HeaderMap, bytes: &[u8]) -> Result<Option<Value>, ()> {
    if matches!(operation, Operation::Clients | Operation::RevokeClient) {
        return if bytes.is_empty() { Ok(None) } else { Err(()) };
    }
    if operation == Operation::Server {
        if !bytes.is_empty() { return Err(()); }
        for field in ["x-codex-server-id", "x-codex-installation-id", "x-codex-name", "x-codex-protocol-version"] {
            single_header(headers, field)?;
        }
        return Ok(None);
    }
    let body: Value = sonic_rs::from_slice(bytes).map_err(|_| ())?;
    if !body.is_object() { return Err(()); }
    match operation {
        Operation::Enroll | Operation::Refresh => {
            let installation = nonempty_string(&body, "installation_id")?;
            if installation != single_header(headers, "x-codex-installation-id")? {
                return Err(());
            }
            if operation == Operation::Refresh {
                nonempty_string(&body, "server_id")?;
            } else {
                for field in ["name", "os", "arch", "app_server_version"] {
                    nonempty_string(&body, field)?;
                }
            }
        }
        Operation::Pair => {
            body.get("manual_code").and_then(Value::as_bool).ok_or(())?;
        }
        Operation::PairStatus => {
            // Exactly one nonempty code, not an empty/null alternative or two
            // competing selectors. Pairing material is never echoed or logged.
            match (body.get("pairing_code"), body.get("manual_pairing_code")) {
                (Some(_), None) => { nonempty_string(&body, "pairing_code")?; }
                (None, Some(_)) => { nonempty_string(&body, "manual_pairing_code")?; }
                _ => return Err(()),
            }
        }
        Operation::Clients | Operation::RevokeClient | Operation::Server => unreachable!("handled above"),
    }
    Ok(Some(body))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn enrollment() -> Value {
        json!({"name":"machine", "os":"linux", "arch":"aarch64",
            "app_server_version":"0.169.0", "installation_id":"installation"})
    }

    fn installation_headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("x-codex-installation-id", HeaderValue::from_static("installation"));
        headers
    }

    #[test]
    fn enrollment_and_refresh_reject_malformed_or_conflicting_identity() {
        let headers = installation_headers();
        assert!(validate_request(Operation::Enroll, &headers, &serde_json::to_vec(&enrollment()).unwrap()).is_ok());
        for body in [json!(null), json!([]), json!({}), json!({"installation_id":false}),
            json!({"installation_id":"other"}), json!({"installation_id":"installation", "server_id":""})] {
            assert!(validate_request(Operation::Refresh, &headers, &serde_json::to_vec(&body).unwrap()).is_err());
        }
        for field in ["name", "os", "arch", "app_server_version", "installation_id"] {
            let mut body = enrollment();
            body[field] = json!(null);
            assert!(validate_request(Operation::Enroll, &headers, &serde_json::to_vec(&body).unwrap()).is_err());
        }
        assert!(validate_request(Operation::Enroll, &headers, b"{malformed").is_err());
        let refresh = br#"{"server_id":"server", "installation_id":"installation"}"#;
        assert!(validate_request(Operation::Refresh, &headers, refresh).is_ok());
        assert!(validate_request(Operation::Refresh, &HeaderMap::new(), refresh).is_err());
        let mut duplicate = headers;
        duplicate.append("x-codex-installation-id", HeaderValue::from_static("installation"));
        assert!(validate_request(Operation::Refresh, &duplicate, refresh).is_err());
    }

    #[test]
    fn pair_and_status_validate_only_the_captured_wire_contract() {
        let headers = HeaderMap::new();
        for value in [false, true] {
            assert!(validate_request(Operation::Pair, &headers, &serde_json::to_vec(&json!({"manual_code":value})).unwrap()).is_ok());
        }
        for body in [json!({}), json!({"manual_code":"true"}), json!([])] {
            assert!(validate_request(Operation::Pair, &headers, &serde_json::to_vec(&body).unwrap()).is_err());
        }
        for field in ["pairing_code", "manual_pairing_code"] {
            assert!(validate_request(Operation::PairStatus, &headers, &serde_json::to_vec(&json!({field:"synthetic-code"})).unwrap()).is_ok());
        }
        for body in [json!({}), json!({"pairing_code":""}), json!({"manual_pairing_code":null}),
            json!({"pairing_code":"one", "manual_pairing_code":"two"})] {
            assert!(validate_request(Operation::PairStatus, &headers, &serde_json::to_vec(&body).unwrap()).is_err());
        }
    }

    #[test]
    fn server_requires_unambiguous_identity_without_upgrading() {
        let mut headers = HeaderMap::new();
        for field in ["x-codex-server-id", "x-codex-installation-id", "x-codex-name", "x-codex-protocol-version"] {
            assert!(validate_request(Operation::Server, &headers, b"").is_err());
            headers.insert(field, HeaderValue::from_static("synthetic"));
        }
        assert!(validate_request(Operation::Server, &headers, b"").is_ok());
        assert!(validate_request(Operation::Server, &headers, b"data").is_err());
        headers.append("x-codex-server-id", HeaderValue::from_static("other"));
        assert!(validate_request(Operation::Server, &headers, b"").is_err());
    }

    #[tokio::test]
    async fn unavailable_is_explicit_local_and_contains_no_credentials() {
        use http_body_util::BodyExt as _;
        let response = local_error(StatusCode::SERVICE_UNAVAILABLE, "upstream_affinity_unavailable",
            "creating remote control owner is unavailable", None);
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers()["cc-lb-error-origin"], "proxy");
        assert_eq!(response.headers()[http::header::CACHE_CONTROL], "no-store");
        assert!(!response.headers().contains_key("upgrade"));
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["error"], "upstream_affinity_unavailable");
        assert!(body.get("remote_control_token").is_none());
    }
}
