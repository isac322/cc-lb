//! Type-enforced project rule: **authentication happens before any request work.**
//!
//! # The rule
//!
//! A request that has not been authenticated gets nothing. Not a buffered
//! body, not a JSON parse, not a routing decision, not a quota reservation,
//! not a durable row. An unauthenticated caller must be able to cost this
//! process nothing beyond reading its headers and writing back a rejection.
//!
//! # Why this is a type and not a comment
//!
//! Ordering rules written only in prose regress silently: someone adds a
//! "cheap" body peek above the authentication call, every test still passes,
//! and the process will happily buffer 32 MiB for an anonymous caller. So the
//! rule is expressed as a value.
//!
//! [`Authenticated`] is proof that authentication already succeeded. It has no
//! public constructor — [`authenticate_first`] is the only way to obtain one,
//! and it authenticates to hand one back. Every API that does work on behalf
//! of a request takes `&Authenticated`, so calling it without having
//! authenticated is not a bug to be caught in review: it does not compile.
//!
//! The compile-fail proofs live in
//! `crates/cc-lb-engine/tests/trybuild/authn_rail/`.

use axum::body::Body;
use axum::response::Response;
use cc_lb_control::api_keys::builtin_authn::{AuthnSuccess, BuiltinAuthError, BuiltinAuthn};
use cc_lb_control::api_keys::principal_view::PrincipalView;
use cc_lb_domain::Provider;
use http::header::{CONTENT_TYPE, RETRY_AFTER};
use http::{HeaderValue, Method, StatusCode};

use crate::error_format::{provider_error_response, provider_error_response_with_retry_after};
use crate::provider_capability::{ProviderCapability, classify};
use crate::terminal_observer::{LifecycleContext, error_codes};

fn auth_capability(provider: Option<Provider>) -> ProviderCapability {
    match provider {
        Some(Provider::Openai) => ProviderCapability::OpenaiResponses,
        Some(Provider::Gemini) => ProviderCapability::GeminiGenerateContent,
        Some(Provider::Anthropic) | None => ProviderCapability::AnthropicMessages,
    }
}

fn auth_capability_for_route(
    method: &Method,
    path: &str,
    provider: Option<Provider>,
) -> Option<ProviderCapability> {
    provider
        .map(|provider| auth_capability(Some(provider)))
        .or_else(|| {
            // The shared Responses WebSocket route is GET-only and therefore
            // does not appear in the POST capability classifier.
            (method == Method::GET && path == "/v1/responses")
                .then_some(ProviderCapability::OpenaiResponses)
                .or_else(|| classify(method, path, None))
        })
}

fn generic_error_response(
    status: StatusCode,
    error_type: &str,
    message: &str,
    retry_after_secs: Option<u64>,
) -> Response<Body> {
    let mut response = Response::new(Body::from(
        serde_json::json!({
            "error": error_type,
            "message": message,
        })
        .to_string(),
    ));
    *response.status_mut() = status;
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("application/json; charset=utf-8"),
    );
    if let Some(retry_after_secs) = retry_after_secs
        && let Ok(value) = HeaderValue::from_str(&retry_after_secs.to_string())
    {
        response.headers_mut().insert(RETRY_AFTER, value);
    }
    response
}

fn reject_unauthenticated_with_capability(
    error: &BuiltinAuthError,
    capability: Option<ProviderCapability>,
    observer: Option<&LifecycleContext>,
) -> Response<Body> {
    let status = StatusCode::from_u16(error.http_status()).unwrap_or(StatusCode::UNAUTHORIZED);
    let retry_after_secs = matches!(error, BuiltinAuthError::Unavailable).then_some(1);
    let response = match (capability, retry_after_secs) {
        (Some(capability), Some(retry_after_secs)) => provider_error_response_with_retry_after(
            capability,
            StatusCode::SERVICE_UNAVAILABLE,
            "authentication_error",
            &error.to_string(),
            None,
            retry_after_secs,
        ),
        (Some(capability), None) => provider_error_response(
            capability,
            status,
            "authentication_error",
            &error.to_string(),
            None,
        ),
        (None, retry_after_secs) => generic_error_response(
            if retry_after_secs.is_some() {
                StatusCode::SERVICE_UNAVAILABLE
            } else {
                status
            },
            "authentication_error",
            &error.to_string(),
            retry_after_secs,
        ),
    };
    if let Some(o) = observer {
        o.emit_request_started(false);
        o.emit_lifecycle(cc_lb_lifecycle::LifecycleEvent::AuthCompleted {
            event_id: o.event_id().to_owned(),
            result: Err(cc_lb_lifecycle::AuthFailure::AuthenticationFailed {
                http_status: status.as_u16(),
                reason: Some(key_auth_failure_reason(error).to_owned()),
            }),
        });
        o.terminate_failure(crate::terminal_observer::InternalFailure {
            status,
            error_code: error_codes::AUTHENTICATION_FAILED,
            error: cc_lb_domain::InternalError {
                stage: cc_lb_domain::InternalErrorStage::Authn,
                kind: if matches!(error, BuiltinAuthError::Unavailable) {
                    cc_lb_domain::InternalErrorKind::Unavailable
                } else {
                    cc_lb_domain::InternalErrorKind::InvalidInput
                },
                message: Some(error.to_string()),
            },
        });
    }
    response
}

pub fn reject_unauthenticated_for_provider(
    error: &BuiltinAuthError,
    provider: Option<Provider>,
    observer: Option<&LifecycleContext>,
) -> Response<Body> {
    reject_unauthenticated_with_capability(error, Some(auth_capability(provider)), observer)
}

/// Build a provider-native rejection from route metadata available before the
/// request body is read. Ambiguous paths use a provider-neutral JSON fallback.
pub fn reject_unauthenticated_for_route(
    error: &BuiltinAuthError,
    method: &Method,
    path: &str,
    provider: Option<Provider>,
    observer: Option<&LifecycleContext>,
) -> Response<Body> {
    reject_unauthenticated_with_capability(
        error,
        auth_capability_for_route(method, path, provider),
        observer,
    )
}

/// Build an Anthropic-shaped rejection for unscoped legacy callers.
pub fn reject_unauthenticated(
    error: &BuiltinAuthError,
    observer: Option<&LifecycleContext>,
) -> Response<Body> {
    reject_unauthenticated_for_provider(error, None, observer)
}

/// Proof that the request carrying it has been authenticated.
///
/// Obtainable only from [`authenticate_first`]. Hold one to be allowed to do
/// work on a request's behalf; see the module documentation for the rule this
/// encodes.
#[derive(Clone)]
pub struct Authenticated {
    success: AuthnSuccess,
    auth_ms: u64,
}

impl Authenticated {
    /// The authenticated principal.
    pub fn principal_id(&self) -> &str {
        &self.success.principal_id
    }

    /// The API key that authenticated the request.
    pub fn key_id(&self) -> &str {
        &self.success.key_id
    }

    /// How long authentication took, in milliseconds.
    pub fn auth_ms(&self) -> u64 {
        self.auth_ms
    }

    /// The full authentication result.
    pub fn success(&self) -> &AuthnSuccess {
        &self.success
    }
}

/// Authenticate a request from its headers, before anything else happens to it.
///
/// This is the only constructor of [`Authenticated`]. It deliberately takes
/// **only** headers: there is no parameter through which a caller could pass a
/// body, because reading the body is work and work comes after this returns.
pub async fn authenticate_first(
    authn: &BuiltinAuthn,
    headers: &http::HeaderMap,
    view: &PrincipalView,
) -> Result<Authenticated, BuiltinAuthError> {
    let started = std::time::Instant::now();
    let success = authn.authenticate(headers, view).await?;
    let auth_ms = started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
    Ok(Authenticated { success, auth_ms })
}

/// Coarse reason label consumed by the api-key metrics subscriber to tag
/// `cclb_key_auth_failures_total`. The buckets are deliberately coarse: a
/// per-variant label would let an attacker probe which part of a credential
/// was wrong by watching the metrics endpoint.
fn key_auth_failure_reason(source: &BuiltinAuthError) -> &'static str {
    match source {
        BuiltinAuthError::Expired => "Expired",
        BuiltinAuthError::KeyRevoked => "Revoked",
        BuiltinAuthError::PrincipalDisabled => "PrincipalDisabled",
        BuiltinAuthError::Unavailable => "Unavailable",
        BuiltinAuthError::MissingHeader
        | BuiltinAuthError::InvalidFormat
        | BuiltinAuthError::NotFound
        | BuiltinAuthError::SignatureMismatch
        | BuiltinAuthError::PrincipalMissing => "InvalidKey",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cc_lb_control::RequestEventBus;
    use http_body_util::BodyExt;
    use serde_json::Value;

    /// A disabled principal is a 403, and `Unavailable` degrades to 503 with a
    /// Retry-After rather than looking like a bad credential.
    #[test]
    fn rejection_maps_status_per_error_kind() {
        assert_eq!(
            reject_unauthenticated(&BuiltinAuthError::PrincipalDisabled, None).status(),
            StatusCode::FORBIDDEN
        );
        let unavailable = reject_unauthenticated(&BuiltinAuthError::Unavailable, None);
        assert_eq!(unavailable.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(
            unavailable
                .headers()
                .contains_key(http::header::RETRY_AFTER)
        );
    }

    /// The metric label must stay coarse: a per-variant reason would let an
    /// attacker probe which part of a credential was wrong via the metrics
    /// endpoint.
    #[test]
    fn credential_shape_failures_share_one_coarse_reason() {
        for error in [
            BuiltinAuthError::MissingHeader,
            BuiltinAuthError::InvalidFormat,
            BuiltinAuthError::NotFound,
            BuiltinAuthError::SignatureMismatch,
            BuiltinAuthError::PrincipalMissing,
        ] {
            assert_eq!(key_auth_failure_reason(&error), "InvalidKey");
        }
        assert_eq!(
            key_auth_failure_reason(&BuiltinAuthError::Expired),
            "Expired"
        );
    }

    #[tokio::test]
    async fn provider_rejections_use_native_envelopes_for_all_auth_statuses() {
        for provider in [
            cc_lb_domain::Provider::Anthropic,
            cc_lb_domain::Provider::Openai,
            cc_lb_domain::Provider::Gemini,
        ] {
            for error in [
                BuiltinAuthError::MissingHeader,
                BuiltinAuthError::PrincipalDisabled,
                BuiltinAuthError::Unavailable,
            ] {
                let expected_status = match &error {
                    BuiltinAuthError::MissingHeader => StatusCode::UNAUTHORIZED,
                    BuiltinAuthError::PrincipalDisabled => StatusCode::FORBIDDEN,
                    BuiltinAuthError::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
                    _ => unreachable!("fixture covers only status mapping"),
                };
                let response = reject_unauthenticated_for_provider(&error, Some(provider), None);
                assert_eq!(response.status(), expected_status);
                assert_eq!(
                    response.headers().get(http::header::CONTENT_TYPE),
                    Some(&http::HeaderValue::from_static(
                        "application/json; charset=utf-8"
                    ))
                );
                assert_eq!(
                    response
                        .headers()
                        .get(http::header::RETRY_AFTER)
                        .and_then(|value| value.to_str().ok()),
                    (error == BuiltinAuthError::Unavailable).then_some("1")
                );
                let body = response
                    .into_body()
                    .collect()
                    .await
                    .expect("auth response body collects")
                    .to_bytes();
                let value: Value = serde_json::from_slice(&body).expect("auth response is JSON");
                match provider {
                    cc_lb_domain::Provider::Anthropic => {
                        assert!(value.get("error").and_then(Value::as_object).is_some());
                        assert_eq!(value.get("type").and_then(Value::as_str), Some("error"));
                    }
                    cc_lb_domain::Provider::Openai => {
                        assert!(value.get("error").and_then(Value::as_object).is_some());
                        assert!(value.get("type").is_none());
                    }
                    cc_lb_domain::Provider::Gemini => {
                        let error = value
                            .get("error")
                            .and_then(Value::as_object)
                            .expect("Gemini error object");
                        assert!(error.get("code").and_then(Value::as_u64).is_some());
                        assert!(error.get("status").and_then(Value::as_str).is_some());
                    }
                }
            }
        }
    }

    #[tokio::test]
    async fn provider_rejections_emit_one_auth_failure_and_terminal_event() {
        for provider in [
            cc_lb_domain::Provider::Anthropic,
            cc_lb_domain::Provider::Openai,
            cc_lb_domain::Provider::Gemini,
        ] {
            let bus = std::sync::Arc::new(cc_lb_control::InMemoryBus::new());
            let mut events = bus.subscribe_lifecycle();
            let clock: cc_lb_clock::ClockHandle = std::sync::Arc::new(cc_lb_clock::SystemClock);
            let observer = LifecycleContext::new(
                format!("auth-rejection-{provider:?}"),
                bus.clone() as std::sync::Arc<dyn cc_lb_control::RequestEventBus>,
                &clock,
            );
            observer.mark_authn_reached();
            let response = reject_unauthenticated_for_provider(
                &BuiltinAuthError::MissingHeader,
                Some(provider),
                Some(&observer),
            );
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
            drop(observer);

            let events = std::iter::from_fn(|| events.try_recv().ok()).collect::<Vec<_>>();
            assert_eq!(
                events
                    .iter()
                    .filter(|event| matches!(
                        event,
                        cc_lb_lifecycle::LifecycleEvent::AuthCompleted {
                            result: Err(cc_lb_lifecycle::AuthFailure::AuthenticationFailed { .. }),
                            ..
                        }
                    ))
                    .count(),
                1,
                "{provider:?} must emit exactly one auth failure event",
            );
            assert_eq!(
                events
                    .iter()
                    .filter(|event| matches!(
                        event,
                        cc_lb_lifecycle::LifecycleEvent::RequestTerminated { .. }
                    ))
                    .count(),
                1,
                "{provider:?} must emit exactly one terminal event",
            );
            assert!(
                !events.iter().any(|event| matches!(
                    event,
                    cc_lb_lifecycle::LifecycleEvent::UpstreamAttempt { .. }
                        | cc_lb_lifecycle::LifecycleEvent::RouteCompleted { .. }
                )),
                "{provider:?} rejection must not dispatch or route: {events:?}",
            );
        }
    }
    type RouteAuthShapeCase = (Method, &'static str, Option<Provider>, fn(&Value) -> bool);
    #[tokio::test]
    async fn route_auth_rejections_classify_generation_paths_before_body_reads() {
        let cases: [RouteAuthShapeCase; 3] = [
            (Method::POST, "/v1/responses", None, |value: &Value| {
                value.get("error").and_then(Value::as_object).is_some()
                    && value.get("type").is_none()
            }),
            (
                Method::POST,
                "/v1beta/models/gemini-2.5-pro:generateContent",
                None,
                |value: &Value| {
                    value
                        .get("error")
                        .and_then(Value::as_object)
                        .is_some_and(|error| {
                            error.get("code").and_then(Value::as_u64).is_some()
                                && error.get("status").and_then(Value::as_str).is_some()
                        })
                },
            ),
            (Method::POST, "/v1/unknown", None, |value: &Value| {
                value.get("error").and_then(Value::as_str) == Some("authentication_error")
                    && value.get("type").is_none()
            }),
        ];

        for (method, path, provider, assert_shape) in cases {
            let response = reject_unauthenticated_for_route(
                &BuiltinAuthError::MissingHeader,
                &method,
                path,
                provider,
                None,
            );
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
            let body = response
                .into_body()
                .collect()
                .await
                .expect("auth response body collects")
                .to_bytes();
            let value: Value = serde_json::from_slice(&body).expect("auth response is JSON");
            assert!(assert_shape(&value), "unexpected auth envelope: {value}");
        }

        let response = reject_unauthenticated_for_route(
            &BuiltinAuthError::MissingHeader,
            &Method::POST,
            "/v1/messages",
            Some(Provider::Openai),
            None,
        );
        let body = response
            .into_body()
            .collect()
            .await
            .expect("scoped auth response body collects")
            .to_bytes();
        let value: Value = serde_json::from_slice(&body).expect("scoped auth response is JSON");
        assert!(value.get("error").and_then(Value::as_object).is_some());
        assert!(value.get("type").is_none());
    }
}
