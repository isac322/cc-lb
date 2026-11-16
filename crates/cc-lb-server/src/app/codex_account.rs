use super::ProxyState;
use axum::Extension;
use axum::body::Body;
use axum::extract::{OriginalUri, State};
use axum::http::{HeaderMap, HeaderValue, Response, header::CACHE_CONTROL};
use cc_lb_engine::LifecycleContext;
use cc_lb_engine::codex_sessions::{self, SessionError};

fn no_store(mut response: Response<Body>) -> Response<Body> {
    response
        .headers_mut()
        .insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

pub(super) const WORKSPACE_DISCOVERY_PATH: &str = "/backend-api/cc-lb/workspaces";

pub(super) async fn workspace_discovery_handler(
    State(state): State<ProxyState>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    observer: Option<Extension<LifecycleContext>>,
) -> Response<Body> {
    let observer = observer.map(|Extension(value)| value);
    if let Some(observer) = observer.as_ref() {
        observer.mark_authn_reached();
    }
    let auth = match state.lifecycle.authenticate(&headers).await {
        Ok(auth) => auth,
        Err(error) => {
            return no_store(cc_lb_engine::reject_unauthenticated_for_route(
                &error,
                &http::Method::GET,
                WORKSPACE_DISCOVERY_PATH,
                None,
                observer.as_ref(),
            ));
        }
    };
    if let Err(error) = codex_sessions::authorize_registered_purpose(
        &auth,
        &http::Method::GET,
        WORKSPACE_DISCOVERY_PATH,
    ) {
        return no_store(cc_lb_engine::reject_unauthenticated_for_route(
            &error,
            &http::Method::GET,
            WORKSPACE_DISCOVERY_PATH,
            None,
            observer.as_ref(),
        ));
    }
    if let Some(observer) = observer.as_ref() {
        state
            .lifecycle
            .record_local_authenticated_request(&auth, observer);
    }
    if uri.query().is_some() {
        return no_store(codex_sessions::session_error_response(
            SessionError::InvalidRequest,
            observer.as_ref(),
        ));
    }
    let discovery = match state.codex_sessions.list_session_workspaces(&auth).await {
        Ok(discovery) => discovery,
        Err(error) => {
            return no_store(codex_sessions::session_error_response(
                error,
                observer.as_ref(),
            ));
        }
    };

    let mut response = cc_lb_engine::codex_account::workspace_discovery_response(
        &auth,
        &discovery.session_id,
        discovery.workspace_generation,
        &discovery.selected_workspace_id,
        &discovery.workspaces,
    );
    let classification = cc_lb_engine::TerminalClassification::local(response.status());
    response.extensions_mut().insert(classification);
    if let Some(observer) = observer {
        observer.terminate_local(classification);
    }
    response
}
