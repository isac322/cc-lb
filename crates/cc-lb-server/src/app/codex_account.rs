use super::{ProxyState, RequestBodyReadError, read_request_body};
use axum::Extension;
use axum::body::Body;
use axum::extract::{OriginalUri, State};
use axum::http::{
    HeaderMap, HeaderValue, Request, Response, StatusCode, Uri, header::CACHE_CONTROL,
};
use cc_lb_engine::LifecycleContext;
use cc_lb_engine::codex_sessions::{self, SessionError};

fn canonical_account_path(path: &str) -> Option<&'static str> {
    Some(match path {
        "/api/codex/accounts/check" => "/backend-api/wham/accounts/check",
        "/api/codex/usage" => "/backend-api/wham/usage",
        "/api/codex/rate-limit-reset-credits" => "/backend-api/wham/rate-limit-reset-credits",
        "/api/codex/rate-limit-reset-credits/consume" => {
            "/backend-api/wham/rate-limit-reset-credits/consume"
        }
        "/api/codex/profiles/me" => "/backend-api/wham/profiles/me",
        "/api/codex/accounts/send_add_credits_nudge_email" => {
            "/backend-api/wham/accounts/send_add_credits_nudge_email"
        }
        "/api/codex/config/bundle" => "/backend-api/wham/config/bundle",
        "/api/codex/settings/user" => "/backend-api/wham/settings/user",
        "/api/codex/workspace-messages" => "/backend-api/wham/workspace-messages",
        "/api/codex/security-setup" => "/backend-api/wham/security-setup",
        "/api/codex/usage/thread-estimates/query" => {
            "/backend-api/wham/usage/thread-estimates/query"
        }
        "/api/codex/usage/thread_usage/query" => "/backend-api/wham/usage/thread_usage/query",
        "/api/codex/usage/thread_usage/query_v2" => "/backend-api/wham/usage/thread_usage/query_v2",
        "/api/codex/usage/plan_limit_history" => "/backend-api/wham/usage/plan_limit_history",
        "/api/codex/usage/daily-token-usage-breakdown" => {
            "/backend-api/wham/usage/daily-token-usage-breakdown"
        }
        "/api/codex/usage/credit-usage-events" => "/backend-api/wham/usage/credit-usage-events",
        "/api/codex/usage/daily-workspace-user-token-usage-breakdown" => {
            "/backend-api/wham/usage/daily-workspace-user-token-usage-breakdown"
        }
        "/api/codex/usage/daily-workspace-user-credit-usage" => {
            "/backend-api/wham/usage/daily-workspace-user-credit-usage"
        }
        "/api/codex/analytics/daily-workspace-usage-counts" => {
            "/backend-api/wham/analytics/daily-workspace-usage-counts"
        }
        "/api/codex/analytics/daily-plugin-usage-metrics" => {
            "/backend-api/wham/analytics/daily-plugin-usage-metrics"
        }
        "/api/codex/analytics/daily-skill-usage-metrics" => {
            "/backend-api/wham/analytics/daily-skill-usage-metrics"
        }
        "/api/codex/accounts/verified_access" => "/backend-api/accounts/verified_access",
        "/api/codex/accounts/switch-workspace-token" => {
            "/backend-api/accounts/switch-workspace-token"
        }
        _ => return None,
    })
}

fn canonicalize_account_uri(uri: &mut Uri) {
    let Some(path) = canonical_account_path(uri.path()) else {
        return;
    };
    let path_and_query = match uri.query() {
        Some(query) => format!("{path}?{query}"),
        None => path.to_owned(),
    };
    let mut parts = uri.clone().into_parts();
    parts.path_and_query = Some(
        path_and_query
            .parse()
            .expect("canonical account route is a valid path"),
    );
    *uri = Uri::from_parts(parts).expect("canonical account URI is valid");
}

fn no_store(mut response: Response<Body>) -> Response<Body> {
    response
        .headers_mut()
        .insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

/// A separate control transport: one send, bounded for the entire response,
/// and no redirect, automatic decompression, or transport-level retry.
pub(super) struct AccountDispatcher(reqwest::Client);

impl AccountDispatcher {
    pub(super) fn new(timeout: std::time::Duration) -> Self {
        Self(
            reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .no_gzip()
                .no_brotli()
                .no_deflate()
                .no_zstd()
                .retry(reqwest::retry::never())
                .timeout(timeout)
                .build()
                .expect("account control HTTP client configuration is valid"),
        )
    }
}

#[async_trait::async_trait]
impl cc_lb_engine::UpstreamDispatch for AccountDispatcher {
    async fn dispatch(
        &self,
        request: cc_lb_upstream::SignedRequest,
    ) -> Result<Response<Body>, cc_lb_engine::DispatchError> {
        let (url, method, headers, body) = request.into_parts();
        let mut upstream = self
            .0
            .request(method, url)
            .headers(headers)
            .body(body)
            .send()
            .await
            .map_err(|source| cc_lb_engine::DispatchError::Transport {
                source: Box::new(source),
            })?;
        let mut response = Response::new(Body::empty());
        *response.status_mut() = upstream.status();
        *response.headers_mut() = upstream.headers().clone();
        *response.version_mut() = upstream.version();
        *response.body_mut() = Body::from_stream(async_stream::stream! {
            loop {
                match upstream.chunk().await {
                    Ok(Some(chunk)) => yield Ok::<_, reqwest::Error>(chunk),
                    Ok(None) => break,
                    Err(error) => {
                        yield Err(error);
                        break;
                    }
                }
            }
        });
        Ok(response)
    }
}

pub(super) async fn account_handler(
    State(state): State<ProxyState>,
    request: Request<Body>,
) -> Response<Body> {
    no_store(account_response(&state, request).await)
}

async fn account_response(state: &ProxyState, request: Request<Body>) -> Response<Body> {
    let (mut parts, body) = request.into_parts();
    let observer = parts.extensions.get::<LifecycleContext>().cloned();
    if let Some(observer) = observer.as_ref() {
        observer.mark_authn_reached();
    }
    let auth = match state.lifecycle.authenticate(&parts.headers).await {
        Ok(auth) => auth,
        Err(error) => {
            return cc_lb_engine::reject_unauthenticated_for_route(
                &error,
                &parts.method,
                parts.uri.path(),
                None,
                observer.as_ref(),
            );
        }
    };
    // GET registration also admits HEAD. Never let the generic purpose fallback
    // make a principal key eligible for this owner-control path.
    if auth.purpose() != cc_lb_storage_api::codex_sessions::CredentialPurpose::SessionAccess {
        return cc_lb_engine::reject_unauthenticated_for_route(
            &cc_lb_control::api_keys::builtin_authn::BuiltinAuthError::InvalidFormat,
            &parts.method,
            parts.uri.path(),
            None,
            observer.as_ref(),
        );
    }
    canonicalize_account_uri(&mut parts.uri);
    if let Some(observer) = observer.as_ref() {
        state
            .lifecycle
            .record_local_authenticated_request(&auth, observer);
    }
    let owner = match state.codex_sessions.session_owner_constraint(&auth).await {
        Ok(owner) => owner,
        Err(error) => return codex_sessions::session_error_response(error, observer.as_ref()),
    };
    let cap = state.body_caps.for_path(parts.uri.path());
    let body_read_started = std::time::Instant::now();
    let body_result = read_request_body(&parts.headers, body, cap, observer.clone(), &auth).await;
    if let Some(observer) = observer.as_ref() {
        let body_read_ms = body_read_started
            .elapsed()
            .as_millis()
            .min(u128::from(u64::MAX)) as u64;
        let body_bytes = body_result.as_ref().ok().map(|body| body.len() as u64);
        observer.set_request_body_timing(body_read_ms, body_bytes);
    }
    let body = match body_result {
        Ok(body) => body,
        Err(RequestBodyReadError::TooLarge) => {
            if let Some(observer) = observer.as_ref() {
                observer.terminate_body_too_large(cap as u64);
            }
            return super::body_too_large_response();
        }
        Err(RequestBodyReadError::Read(source)) => {
            let mut response = Response::new(Body::from(format!("body read failed: {source}")));
            *response.status_mut() = StatusCode::BAD_REQUEST;
            response
                .extensions_mut()
                .insert(cc_lb_engine::InternalFailure::body_read_failed(
                    cc_lb_domain::InternalError {
                        stage: cc_lb_domain::InternalErrorStage::Ingress,
                        kind: cc_lb_domain::InternalErrorKind::InvalidInput,
                        message: Some(source.to_string()),
                    },
                ));
            return response;
        }
    };
    if parts.method == http::Method::POST
        && parts.uri.path() == "/backend-api/accounts/switch-workspace-token"
    {
        return workspace_switch_response(&state.codex_sessions, &auth, &body, observer.as_ref())
            .await;
    }
    let gate = state.websocket_bulkheads.bulkhead_with_dispatcher(
        format!("account-control:{}", owner.upstream_id()),
        state.websocket_bulkhead_config,
        state.account_dispatcher.clone(),
    );
    let permit = match gate.acquire_owned().await {
        Ok(permit) => permit,
        Err(_) => {
            return codex_sessions::session_error_response(
                SessionError::Unavailable,
                observer.as_ref(),
            );
        }
    };
    match state
        .lifecycle
        .dispatch_codex_account(
            &auth,
            &owner,
            Request::from_parts(parts, body),
            state.account_dispatcher.as_ref(),
        )
        .await
    {
        Ok(response) => {
            let (parts, body) = response.into_parts();
            let permit = if http_body::Body::is_end_stream(&body) {
                drop(permit);
                None
            } else {
                Some(permit)
            };
            Response::from_parts(parts, Body::new(AdmittedAccountBody { body, permit }))
        }
        Err(error) => codex_sessions::session_error_response(error, observer.as_ref()),
    }
}

struct AdmittedAccountBody {
    body: Body,
    permit: Option<cc_lb_engine::OwnedBulkheadGuard>,
}

impl http_body::Body for AdmittedAccountBody {
    type Data = bytes::Bytes;
    type Error = axum::Error;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        let result = std::pin::Pin::new(&mut self.body).poll_frame(cx);
        if matches!(result, std::task::Poll::Ready(None | Some(Err(_))))
            || self.body.is_end_stream()
        {
            self.permit.take();
        }
        result
    }

    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.body.size_hint()
    }
}

#[cfg(test)]
mod admission_body_tests {
    use super::*;
    use http_body::Body as _;
    use http_body_util::BodyExt;

    #[tokio::test]
    async fn permit_releases_on_eof_error_and_cancel() {
        let gate = cc_lb_engine::Bulkhead::new(
            "account-test",
            cc_lb_engine::BulkheadRuntimeConfig {
                max_conns_per_upstream: 1,
                semaphore_permits: 1,
                acquire_timeout: std::time::Duration::from_secs(1),
            },
            cc_lb_engine::make_default_dispatcher(1),
        );
        let body = Body::from("response");
        let mut wrapped = AdmittedAccountBody {
            body,
            permit: Some(gate.acquire_owned().await.unwrap()),
        };
        assert_eq!(gate.active(), 1);
        assert_eq!(
            wrapped.frame().await.unwrap().unwrap().into_data().unwrap(),
            "response"
        );
        assert!(wrapped.is_end_stream());
        assert_eq!(gate.active(), 0);
        let mut wrapped = AdmittedAccountBody {
            body: Body::from_stream(futures_util::stream::once(async {
                Err::<bytes::Bytes, _>(std::io::Error::other("body failed"))
            })),
            permit: Some(gate.acquire_owned().await.unwrap()),
        };
        assert!(wrapped.frame().await.unwrap().is_err());
        assert_eq!(gate.active(), 0);
        let mut wrapped = AdmittedAccountBody {
            body: Body::from_stream(futures_util::stream::pending::<
                Result<bytes::Bytes, std::io::Error>,
            >()),
            permit: Some(gate.acquire_owned().await.unwrap()),
        };
        {
            let frame = wrapped.frame();
            tokio::pin!(frame);
            assert!(futures_util::poll!(frame.as_mut()).is_pending());
        }
        assert_eq!(gate.active(), 1);
        drop(wrapped);
        assert_eq!(gate.active(), 0);
    }
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
pub(super) async fn workspace_switch_response(
    sessions: &cc_lb_engine::codex_sessions::CodexSessionService,
    auth: &cc_lb_engine::authn_rail::Authenticated,
    body: &[u8],
    observer: Option<&LifecycleContext>,
) -> Response<Body> {
    let result = match cc_lb_engine::codex_account::workspace_switch_target(auth, body) {
        Ok(target) => sessions
            .switch_session_workspace(auth, &target)
            .await
            .map(|grant| {
                let payload = serde_json::json!({
                    "session_id": grant.session_id,
                    "access_token": grant.access_token.expose(),
                    "token_type": "Bearer",
                    "expires_at": grant.expires_at,
                    "session_expires_at": grant.session_expires_at,
                    "origin": grant.origin,
                    "account_id": grant.account_id,
                    "user_id": grant.user_id,
                    "workspace_generation": grant.workspace_generation,
                });
                let mut response = Response::new(Body::from(
                    serde_json::to_vec(&payload).expect("session response serializes"),
                ));
                *response.status_mut() = http::StatusCode::OK;
                response.headers_mut().insert(
                    http::header::CONTENT_TYPE,
                    HeaderValue::from_static("application/json"),
                );
                response
            }),
        Err(_) => Err(SessionError::InvalidRequest),
    };
    let mut response = match result {
        Ok(response) => response,
        Err(error) => codex_sessions::session_error_response(error, observer),
    };
    response
        .headers_mut()
        .insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    let classification = cc_lb_engine::TerminalClassification::local(response.status());
    response.extensions_mut().insert(classification);
    if let Some(observer) = observer {
        observer.terminate_local(classification);
    }
    response
}
