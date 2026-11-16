//! Exact native Codex auxiliary routes; authentication and owner fencing stay in Lifecycle.
use axum::{Extension, Router, routing::{get, post}};
use cc_lb_engine::Authenticated;
use cc_lb_storage_api::codex_sessions::CredentialPurpose;
use http::Uri;

use super::{ProviderScope, ProxyState, lifecycle_handler};

const ROUTES: &[(&str, &str)] = &[
    ("/backend-api/codex/models", "/v1/models"),
    ("/backend-api/codex/alpha/search", "/v1/alpha/search"),
    ("/backend-api/codex/images/generations", "/v1/images/generations"),
    ("/backend-api/codex/images/edits", "/v1/images/edits"),
    ("/backend-api/codex/memories/trace_summarize", "/v1/memories/trace_summarize"),
    ("/backend-api/codex/alpha/notes/v2/list_files_by_prefix", "/v1/alpha/notes/v2/list_files_by_prefix"),
    ("/backend-api/codex/alpha/notes/v2/read_file", "/v1/alpha/notes/v2/read_file"),
    ("/backend-api/codex/alpha/notes/v2/search_contents", "/v1/alpha/notes/v2/search_contents"),
    ("/backend-api/codex/alpha/notes/v2/append_to_file", "/v1/alpha/notes/v2/append_to_file"),
    ("/backend-api/codex/alpha/notes/v2/write_file", "/v1/alpha/notes/v2/write_file"),
    ("/backend-api/codex/alpha/notes/v2/thread_hint", "/v1/alpha/notes/v2/thread_hint"),
];

pub(super) fn routes() -> Router<ProxyState> {
    let mut router = Router::new();
    for &(native, canonical) in ROUTES {
        let handler = if canonical == "/v1/models" {
            get(lifecycle_handler)
        } else {
            post(lifecycle_handler)
        };
        router = router.route(native, handler);
    }
    router.layer(Extension(ProviderScope(cc_lb_domain::Provider::Openai)))
}

fn canonical_path(path: &str) -> Option<&'static str> {
    ROUTES.iter().find_map(|&(native, canonical)| (path == native).then_some(canonical))
}

pub(super) fn authorize(
    auth: &Authenticated,
    path: &str,
) -> Result<(), cc_lb_control::api_keys::builtin_authn::BuiltinAuthError> {
    let session_only = canonical_path(path).is_some()
        || (path.starts_with("/v1/alpha/notes/v2/")
            && ROUTES.iter().any(|&(_, canonical)| path == canonical));
    if session_only && auth.purpose() != CredentialPurpose::SessionAccess {
        return Err(cc_lb_control::api_keys::builtin_authn::BuiltinAuthError::InvalidFormat);
    }
    Ok(())
}

pub(super) fn normalize_uri(uri: &mut Uri, _auth: &Authenticated) {
    let Some(canonical) = canonical_path(uri.path()) else { return; };
    let path = uri.query().map_or_else(
        || http::uri::PathAndQuery::from_static(canonical),
        |query| format!("{canonical}?{query}").parse().expect("existing URI query is valid"),
    );
    let mut parts = std::mem::take(uri).into_parts();
    parts.path_and_query = Some(path);
    *uri = Uri::from_parts(parts).expect("canonical auxiliary URI is valid");
}
