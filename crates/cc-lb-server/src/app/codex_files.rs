use axum::body::Body;
use axum::extract::{FromRequestParts, MatchedPath, OriginalUri, Path, State};
use cc_lb_clock::unix_secs;
use cc_lb_domain::{InternalError, InternalErrorKind, InternalErrorStage, Provider};
use cc_lb_engine::codex_files::{self, CodexFiles, FilesDispatchError};
use cc_lb_engine::codex_sessions::{self, SessionError};
use cc_lb_engine::{InternalFailure, LifecycleContext};
use cc_lb_storage_api::account_owner::OwnerBindingExpectation;
use cc_lb_storage_api::codex_sessions::CredentialPurpose;
use http::header::{CACHE_CONTROL, CONTENT_TYPE};
use http::{HeaderValue, Request, Response, StatusCode};
use http_body_util::{BodyExt, Limited};
use serde::Deserialize;

use super::{ProxyState, RequestBodyReadError, body_too_large_response, read_request_body};

const CREATE_PATH: &str = "/backend-api/files";
const FILE_KIND: &str = "chatgpt_file";

/// Only lifecycle metadata is decoded. Signed URLs, binary headers, opaque
/// fields, and PDF/retry metadata remain in the original response bytes.
#[derive(Deserialize)]
struct FileMetadata {
    file_id: Option<String>,
    status: Option<String>,
    expires_at: Option<u64>,
}

pub(super) async fn handler(
    State(state): State<ProxyState>,
    OriginalUri(uri): OriginalUri,
    request: Request<Body>,
) -> Response<Body> {
    let (mut parts, body) = request.into_parts();
    let observer = parts.extensions.get::<LifecycleContext>().cloned();
    if let Some(observer) = observer.as_ref() {
        observer.mark_authn_reached();
    }
    let auth = match state.lifecycle.authenticate(&parts.headers).await {
        Ok(auth) => auth,
        Err(error) => {
            return no_store(cc_lb_engine::reject_unauthenticated_for_route(
                &error, &parts.method, uri.path(), Some(Provider::Openai), observer.as_ref(),
            ));
        }
    };
    let matched_path = parts.extensions.get::<MatchedPath>()
        .map_or(uri.path(), MatchedPath::as_str);
    if let Err(error) = codex_sessions::authorize_registered_purpose(&auth, &parts.method, matched_path) {
        return no_store(cc_lb_engine::reject_unauthenticated_for_route(
            &error, &parts.method, uri.path(), Some(Provider::Openai), observer.as_ref(),
        ));
    }
    // The foundation's default purpose policy also accepts principal keys.
    // Files requires a selected session owner, so reject other purposes before
    // reading a body or consulting an owner binding.
    if auth.purpose() != CredentialPurpose::SessionAccess {
        return no_store(cc_lb_engine::reject_unauthenticated_for_route(
            &cc_lb_control::api_keys::builtin_authn::BuiltinAuthError::InvalidFormat,
            &parts.method, uri.path(), Some(Provider::Openai), observer.as_ref(),
        ));
    }
    if let Some(observer) = observer.as_ref() {
        state.lifecycle.record_local_authenticated_request(&auth, observer);
    }
    let owner = match state.codex_sessions.session_owner_constraint(&auth).await {
        Ok(owner) => owner,
        Err(error) => return session_error(error, observer.as_ref()),
    };
    let Some(snapshot) = auth.session() else {
        return session_error(SessionError::InvalidRequest, observer.as_ref());
    };
    let expected = OwnerBindingExpectation {
        binding_id: snapshot.owner_binding_id.clone(),
        principal_id: auth.principal_id().to_owned(),
        provider: snapshot.provider.clone(),
        owner_generation: snapshot.owner_generation,
        binding_revision: snapshot.binding_revision,
        workspace_id: snapshot.workspace_id.clone(),
    };
    let cap = state.body_caps.files;
    let body = match read_request_body(&parts.headers, body, cap, observer.clone(), &auth).await {
        Ok(body) => body,
        Err(RequestBodyReadError::TooLarge) => {
            if let Some(observer) = observer.as_ref() {
                observer.terminate_body_too_large(cap as u64);
            }
            let mut response = body_too_large_response();
            response.headers_mut().insert("cc-lb-error-origin", HeaderValue::from_static("proxy"));
            return no_store(response);
        }
        Err(RequestBodyReadError::Read(_)) => {
            return failure(StatusCode::BAD_REQUEST, "body_read_failed", InternalErrorStage::Ingress,
                InternalErrorKind::InvalidInput, observer.as_ref());
        }
    };
    let file_id = if uri.path() == CREATE_PATH {
        None
    } else {
        match Path::<String>::from_request_parts(&mut parts, &state).await {
            Ok(Path(file_id)) if file_id.is_empty() => {
                return failure(StatusCode::NOT_FOUND, "invalid_file_id", InternalErrorStage::Ingress,
                    InternalErrorKind::InvalidInput, observer.as_ref());
            }
            Ok(Path(file_id)) => Some(file_id),
            Err(_) => {
                return failure(StatusCode::BAD_REQUEST, "invalid_file_id", InternalErrorStage::Ingress,
                    InternalErrorKind::InvalidInput, observer.as_ref());
            }
        }
    };
    // Dispatch retains the original encoded path/query. Only the ledger key is
    // decoded; arbitrary ID text elsewhere is never inspected as ownership.
    parts.uri = uri;
    let response = match codex_files::dispatch(
        &state.lifecycle, &auth, &owner, Request::from_parts(parts, body), observer.as_ref(),
    ).await {
        Ok(response) => response,
        Err(error) => return dispatch_error(error, observer.as_ref()),
    };
    let (mut response_parts, response_body) = response.into_parts();
    let response_body = match Limited::new(response_body, cap).collect().await {
        Ok(body) => body.to_bytes(),
        Err(_) => {
            return failure(StatusCode::BAD_GATEWAY, "upstream_dispatch_failed", InternalErrorStage::Relay,
                InternalErrorKind::Unavailable, observer.as_ref());
        }
    };
    cc_lb_engine::strip_hop_by_hop(&mut response_parts.headers);
    codex_sessions::mark_upstream_response(&mut response_parts.headers);
    if response_parts.status.is_success() {
        // Parse at most once, and never serialize the provider body again.
        let metadata = sonic_rs::from_slice::<FileMetadata>(&response_body);
        let ledger = CodexFiles::new(state.storage.as_ref());
        match file_id {
            None => {
                let Ok(FileMetadata { file_id: Some(file_id), expires_at, .. }) = metadata else {
                    return failure(StatusCode::BAD_GATEWAY, "upstream_dispatch_failed", InternalErrorStage::Relay,
                        InternalErrorKind::InvalidOutput, observer.as_ref());
                };
                if file_id.is_empty() {
                    return failure(StatusCode::BAD_GATEWAY, "upstream_dispatch_failed", InternalErrorStage::Relay,
                        InternalErrorKind::InvalidOutput, observer.as_ref());
                }
                // Durable bind must commit before a successful create is exposed.
                if let Err(error) = ledger.create_success(
                    &auth, &expected, file_id, FILE_KIND.to_owned(),
                    unix_secs(state.clock.now()), expires_at,
                ).await {
                    return storage_error(error, observer.as_ref());
                }
            }
            Some(file_id) => {
                if let Ok(FileMetadata { status: Some(status), expires_at, .. }) = metadata
                    && status == "success"
                {
                    // An unobserved native ID is forwarded and a namespace miss
                    // remains a miss; it must not turn a provider success into404.
                    if let Err(error) = ledger.finalize(
                        &auth, &expected, FILE_KIND, &file_id, file_id.clone(), expires_at,
                    ).await {
                        return storage_error(error, observer.as_ref());
                    }
                }
            }
        }
    }
    codex_files::complete_response(&auth, response_parts.status, response_body.len(), observer.as_ref());
    Response::from_parts(response_parts, Body::from(response_body))
}

fn no_store(mut response: Response<Body>) -> Response<Body> {
    response.headers_mut().insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

fn failure(
    status: StatusCode,
    code: &'static str,
    stage: InternalErrorStage,
    kind: InternalErrorKind,
    observer: Option<&LifecycleContext>,
) -> Response<Body> {
    let mut response = Response::new(Body::from(format!(r#"{{"error":"{code}"}}"#)));
    *response.status_mut() = status;
    response.headers_mut().insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    response.headers_mut().insert("cc-lb-error-origin", HeaderValue::from_static("proxy"));
    let failure = InternalFailure {
        status, error_code: code,
        error: InternalError { stage, kind, message: Some(code.to_owned()) },
    };
    if let Some(observer) = observer { observer.terminate_failure(failure); }
    else { response.extensions_mut().insert(failure); }
    no_store(response)
}

fn session_error(error: SessionError, observer: Option<&LifecycleContext>) -> Response<Body> {
    if matches!(error, SessionError::Unavailable) {
        failure(StatusCode::SERVICE_UNAVAILABLE, "upstream_affinity_unavailable",
            InternalErrorStage::Storage, InternalErrorKind::Unavailable, observer)
    } else {
        no_store(codex_sessions::session_error_response(error, observer))
    }
}

fn dispatch_error(error: FilesDispatchError, observer: Option<&LifecycleContext>) -> Response<Body> {
    match error {
        FilesDispatchError::Session(error) => session_error(error, observer),
        FilesDispatchError::OwnerUnavailable => failure(StatusCode::SERVICE_UNAVAILABLE,
            "upstream_affinity_unavailable", InternalErrorStage::Router, InternalErrorKind::Unavailable, observer),
        FilesDispatchError::SigningFailed => failure(StatusCode::BAD_GATEWAY,
            "signer_failed", InternalErrorStage::Signer, InternalErrorKind::Unavailable, observer),
        FilesDispatchError::DispatchFailed => failure(StatusCode::BAD_GATEWAY,
            "upstream_dispatch_failed", InternalErrorStage::Relay, InternalErrorKind::Unavailable, observer),
    }
}

fn storage_error(_error: cc_lb_storage_api::StorageError, observer: Option<&LifecycleContext>) -> Response<Body> {
    failure(StatusCode::SERVICE_UNAVAILABLE, "upstream_affinity_unavailable",
        InternalErrorStage::Storage, InternalErrorKind::Unavailable, observer)
}
