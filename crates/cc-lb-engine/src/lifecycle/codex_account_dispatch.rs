use super::*;
use crate::codex_account::{AccountProjectionMetadata, project_codex_accounts};
use crate::codex_sessions::{SessionError, SessionOwnerConstraint};
use cc_lb_lifecycle::{LifecycleEvent, RouteInfo};
use std::pin::Pin;
use std::task::{Context, Poll};

const ACCOUNTS_CHECK: &str = "/backend-api/wham/accounts/check";
static ACCOUNT_BASE: std::sync::LazyLock<Url> = std::sync::LazyLock::new(|| {
    Url::parse("https://chatgpt.com/backend-api").expect("fixed account origin parses")
});

impl Lifecycle {
    /// Relays admitted account controls once to the captured session owner, without
    /// model routing, a generation reservation, response usage parsing, or failover.
    /// The dispatcher must not redirect, decompress, or retry and must bound HTTP time.
    pub async fn dispatch_codex_account(
        &self,
        auth: &crate::Authenticated,
        owner: &SessionOwnerConstraint,
        request: Request<Bytes>,
        dispatcher: &dyn UpstreamDispatch,
    ) -> Result<Response<Body>, SessionError> {
        if auth.purpose() != cc_lb_storage_api::codex_sessions::CredentialPurpose::SessionAccess
            || !is_account_relay_route(request.method(), request.uri().path())
        {
            return Err(SessionError::InvalidRequest);
        }
        owner.validate(auth).await?;
        // No approved residency resolver exists for this control origin. Never
        // silently send a region-bound owner through the global ChatGPT origin.
        if owner.outbound_region().is_some() {
            return Err(SessionError::Unavailable);
        }
        let session = auth.session().ok_or(SessionError::InvalidRequest)?;
        let view = self.dynamic_view.load();
        let record = view
            .upstreams_snapshot()
            .iter()
            .find(|record| {
                record.id == owner.upstream_id()
                    && record.enabled
                    && record.deleted_at_unix_secs.is_none()
                    && record.kind == StorageUpstreamKind::OpenaiOauth
            })
            .ok_or(SessionError::Unavailable)?;
        let upstream = upstream_for_record(record).map_err(|_| SessionError::Unavailable)?;
        let (parts, body) = request.into_parts();
        let observer = parts.extensions.get::<LifecycleContext>().cloned();
        let project_accounts = parts.method == Method::GET && parts.uri.path() == ACCOUNTS_CHECK;
        let mut headers = parts.headers;
        strip_hop_by_hop(&mut headers);
        crate::codex_sessions::strip_untrusted_provenance(&mut headers);
        for name in [
            "authorization",
            "proxy-authorization",
            "x-api-key",
            "x-goog-api-key",
            "api-key",
            "x-admin-key",
            "cookie",
            "chatgpt-account-id",
            "host",
            "content-length",
        ] {
            headers.remove(name);
        }
        if project_accounts {
            // This one response is JSON-projected, rather than byte-passthrough.
            headers.insert(
                http::header::ACCEPT_ENCODING,
                HeaderValue::from_static("identity"),
            );
        }
        let context = DialectShapeContext {
            request_id: next_request_id(),
            downstream_headers: headers,
            method: parts.method,
            path: parts.uri.path().to_owned(),
            query: parts.uri.query().map(str::to_owned),
            body_bytes: body,
        };
        let principal = Principal {
            id: auth.principal_id().to_owned(),
            kind: PrincipalKind::InternalKey,
        };
        let shaped = shape_request(&AccountDialect, &context, &upstream, &principal)
            .map_err(|_| SessionError::InvalidRequest)?;
        let expected_url = shaped.url().clone();
        let signer = view
            .signer_factory
            .with_router_choice(record.name.clone())
            .build(&upstream)
            .await
            .map_err(|_| SessionError::Unavailable)?;
        let workspace = HeaderValue::from_str(owner.provider_workspace_id())
            .map_err(|_| SessionError::Unavailable)?;
        let signed = Scoped::unreserved_for_lifecycle()
            .sign_for_workspace(signer.as_ref(), shaped, workspace)
            .await
            .map_err(|_| SessionError::Unavailable)?;
        if signed.url() != &expected_url || signed.method() != &context.method {
            return Err(SessionError::Unavailable);
        }
        // Signing may await credential refresh. A switch/revoke during that wait
        // invalidates this exact owner; never resolve a replacement or retry.
        owner.validate(auth).await?;
        if let Some(observer) = observer.as_ref() {
            record_dispatch(observer, record);
        }
        let mut response = match signed.dispatch(dispatcher).await {
            Ok(response) => response,
            Err(_) => {
                if let Some(observer) = observer.as_ref() {
                    observer.terminate_failure(InternalFailure {
                        status: StatusCode::SERVICE_UNAVAILABLE,
                        error_code: error_codes::UPSTREAM_DISPATCH_FAILED,
                        error: InternalError {
                            stage: InternalErrorStage::Relay,
                            kind: InternalErrorKind::Unavailable,
                            message: Some("account owner HTTP dispatch failed".to_owned()),
                        },
                    });
                }
                return Err(SessionError::Unavailable);
            }
        };
        if let Some(observer) = observer.as_ref() {
            observer.emit_lifecycle(LifecycleEvent::UpstreamResponseStarted {
                event_id: observer.event_id().to_owned(),
                status: response.status().as_u16(),
                headers: header_snapshot_from(response.headers(), record.kind),
                bulkhead_wait_ms: None,
                dns_ms: None,
                connect_ms: None,
                connection_reused: None,
                shape_ms: None,
                sign_ms: None,
                upstream_ttfb_ms: None,
                observed_at_unix_millis: None,
            });
        }
        strip_hop_by_hop(response.headers_mut());
        crate::codex_sessions::mark_upstream_response(response.headers_mut());
        for name in [
            "set-cookie",
            "set-cookie2",
            "cookie",
            "authorization",
            "proxy-authorization",
            "x-api-key",
            "api-key",
            "x-goog-api-key",
        ] {
            response.headers_mut().remove(name);
        }
        // Provider errors (including 401/403/429) never enter the local account
        // schema or local-token recovery path: preserve their status and bytes.
        if project_accounts && response.status().is_success() {
            let (mut parts, body) = response.into_parts();
            let bytes = axum::body::to_bytes(body, self.config.messages_body_cap_bytes)
                .await
                .map_err(|_| SessionError::Unavailable)?;
            let upstream = sonic_rs::from_slice(&bytes).map_err(|_| SessionError::Unavailable)?;
            let projected = project_codex_accounts(
                auth,
                upstream,
                &AccountProjectionMetadata {
                    provider_workspace_id: owner.provider_workspace_id(),
                    local_workspace_alias: &session.workspace_id,
                    proxy_origin: &session.origin,
                },
            )
            .map_err(|_| SessionError::Unavailable)?;
            let bytes = serde_json::to_vec(&projected).map_err(|_| SessionError::Unavailable)?;
            for name in [
                "content-length",
                "content-encoding",
                "etag",
                "last-modified",
                "content-md5",
                "digest",
                "content-digest",
                "repr-digest",
            ] {
                parts.headers.remove(name);
            }
            parts
                .headers
                .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
            response = Response::from_parts(parts, Body::from(bytes));
        }
        Ok(match observer {
            Some(observer) => {
                let (parts, body) = response.into_parts();
                let mut body = AccountResponseBody {
                    inner: body,
                    observer: Some(observer),
                    status: parts.status,
                };
                if body.inner.is_end_stream() {
                    body.finish();
                }
                Response::from_parts(parts, Body::new(body))
            }
            None => response,
        })
    }
}

fn is_account_relay_route(method: &Method, path: &str) -> bool {
    match method.as_str() {
        "GET" | "HEAD" => matches!(
            path,
            "/backend-api/wham/accounts/check"
                | "/backend-api/wham/usage"
                | "/backend-api/wham/rate-limit-reset-credits"
                | "/backend-api/wham/profiles/me"
                | "/backend-api/wham/config/bundle"
                | "/backend-api/wham/settings/user"
                | "/backend-api/wham/workspace-messages"
                | "/backend-api/wham/security-setup"
                | "/backend-api/wham/usage/plan_limit_history"
                | "/backend-api/wham/usage/daily-token-usage-breakdown"
                | "/backend-api/wham/usage/credit-usage-events"
                | "/backend-api/wham/usage/daily-workspace-user-token-usage-breakdown"
                | "/backend-api/wham/usage/daily-workspace-user-credit-usage"
                | "/backend-api/wham/analytics/daily-workspace-usage-counts"
                | "/backend-api/wham/analytics/daily-plugin-usage-metrics"
                | "/backend-api/wham/analytics/daily-skill-usage-metrics"
                | "/backend-api/accounts/verified_access"
        ),
        "POST" => matches!(
            path,
            "/backend-api/wham/rate-limit-reset-credits/consume"
                | "/backend-api/wham/accounts/send_add_credits_nudge_email"
                | "/backend-api/wham/usage/thread-estimates/query"
                | "/backend-api/wham/usage/thread_usage/query"
                | "/backend-api/wham/usage/thread_usage/query_v2"
        ),
        _ => false,
    }
}

struct AccountDialect;

impl UpstreamDialect for AccountDialect {
    fn shape(
        &self,
        context: &DialectShapeContext,
        _upstream: &Upstream,
        _principal: &Principal,
        builder: &mut ShapedRequestBuilder,
    ) -> Result<ShapedRequest, DialectError> {
        let base = &*ACCOUNT_BASE;
        let mut url = base
            .join(&context.path)
            .map_err(|_| DialectError::UnsupportedRequest {
                reason: "invalid account path".to_owned(),
            })?;
        url.set_query(context.query.as_deref());
        if !is_account_relay_route(&context.method, &context.path)
            || url.path() != context.path
            || url.query() != context.query.as_deref()
            || url.origin() != base.origin()
        {
            return Err(DialectError::UnsupportedRequest {
                reason: "account target changed during URL encoding".to_owned(),
            });
        }
        Ok(builder.shaped_request(
            url,
            context.method.clone(),
            context.downstream_headers.clone(),
            context.body_bytes.clone(),
        ))
    }
}

fn record_dispatch(observer: &LifecycleContext, record: &UpstreamRecord) {
    observer.emit_lifecycle(LifecycleEvent::RouteCompleted {
        event_id: observer.event_id().to_owned(),
        result: Ok(RouteInfo {
            upstream_id: record.id,
            upstream_name: record.name.clone(),
            model: None,
            upstream_kind: Some(storage_upstream_kind_label(record.kind).to_owned()),
            route_ms: None,
            routing_trace: None,
            predicted_cache_read_tokens: None,
            matched_v3_cache_key: None,
            breakpoint_content_block_index: None,
            matched_content_block_index: None,
            lookback_distance: None,
            predicted_cache_creation_tokens_5m: None,
            predicted_cache_creation_tokens_1h: None,
            token_estimate_source: None,
            cache_breakpoints: None,
            cache_prefix_hash: None,
            cache_value_micros: None,
            formula_winner_upstream_id: None,
            kept_upstream_id: None,
            quota_urgency_5h: None,
            quota_urgency_7d: None,
            quota_urgency_combined: None,
            quota_warning_multiplier: None,
        }),
        routing_trace: None,
    });
    observer.emit_lifecycle(LifecycleEvent::UpstreamAttempt {
        event_id: observer.event_id().to_owned(),
        attempt_num: 1,
        upstream_id: record.id,
    });
}

// Account controls can contain usage-shaped JSON, but never generation usage.
// Observe transport completion while leaving opaque control bytes untouched.
struct AccountResponseBody {
    inner: Body,
    observer: Option<LifecycleContext>,
    status: StatusCode,
}

impl AccountResponseBody {
    fn finish(&mut self) {
        if let Some(observer) = self.observer.take() {
            if self.status.is_client_error() {
                observer.set_upstream_error(self.status, UpstreamErrorCode::Upstream4xx);
            } else if self.status.is_server_error() {
                observer.set_upstream_error(self.status, UpstreamErrorCode::Upstream5xx);
            } else {
                observer.set_success_status(self.status);
            }
            observer.finish();
        }
    }
}

impl http_body::Body for AccountResponseBody {
    type Data = Bytes;
    type Error = axum::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, Self::Error>>> {
        let result = Pin::new(&mut self.inner).poll_frame(cx);
        if matches!(result, Poll::Ready(Some(Err(_)))) {
            if let Some(observer) = self.observer.take() {
                observer.set_upstream_error(self.status, UpstreamErrorCode::StreamError);
                observer.finish();
            }
        } else if matches!(result, Poll::Ready(None)) || self.inner.is_end_stream() {
            self.finish();
        }
        result
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }
    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
    }
}

impl Drop for AccountResponseBody {
    fn drop(&mut self) {
        if let Some(observer) = self.observer.take() {
            observer
                .set_client_closed(StatusCode::from_u16(499).expect("fixed client closed status"));
            observer.finish();
        }
    }
}

#[cfg(test)]
#[path = "codex_account_dispatch_tests.rs"]
mod tests;
