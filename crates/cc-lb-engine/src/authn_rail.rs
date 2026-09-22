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
use http::StatusCode;

use crate::error_format::{anthropic_error_response, anthropic_error_response_with_retry_after};
use crate::terminal_observer::{LifecycleContext, error_codes};

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

/// Build the client response for a failed authentication, and record the
/// terminal outcome on the lifecycle context.
///
/// Authentication *failure* is still an authenticated-request attempt: it
/// reaches the credential check and is rejected, so it produces exactly one
/// request-log row. Only requests that never reach the check are silent.
pub fn reject_unauthenticated(
    error: &BuiltinAuthError,
    observer: Option<&LifecycleContext>,
) -> Response<Body> {
    if let Some(o) = observer {
        o.emit_provider_error("authentication_error", &error.to_string(), "authn");
    }
    let status = StatusCode::from_u16(error.http_status()).unwrap_or(StatusCode::UNAUTHORIZED);
    let response = match error {
        BuiltinAuthError::Unavailable => anthropic_error_response_with_retry_after(
            StatusCode::SERVICE_UNAVAILABLE,
            "authentication_error",
            &error.to_string(),
            1,
        ),
        _ => anthropic_error_response(status, "authentication_error", &error.to_string()),
    };
    if let Some(o) = observer {
        o.emit_lifecycle(cc_lb_lifecycle::LifecycleEvent::AuthCompleted {
            event_id: o.event_id().to_owned(),
            result: Err(cc_lb_lifecycle::AuthFailure::AuthenticationFailed {
                http_status: status.as_u16(),
                reason: Some(key_auth_failure_reason(error).to_owned()),
            }),
        });
        o.set_terminal(status, error_codes::AUTHENTICATION_FAILED);
        o.finish();
    }
    response
}

/// Coarse reason label consumed by the api-key metrics subscriber to tag
/// `cclb_key_auth_failures_total`. The buckets are deliberately coarse: a
/// per-variant label would let an attacker probe which part of a credential
/// was wrong by watching the metrics endpoint.
fn key_auth_failure_reason(source: &BuiltinAuthError) -> &'static str {
    match source {
        BuiltinAuthError::Expired => "Expired",
        BuiltinAuthError::KeyDisabled => "Disabled",
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
