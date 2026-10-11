//! Principal prompt-cache policy through the real proxy path.
//!
//! The policy is only honest when the analyzed request, the routing preview,
//! the cost estimate, and the bytes that reach the upstream all describe the
//! same document. These tests drive the public `Lifecycle` entry points with a
//! recording dispatch, so they assert on the exact bytes a signer would send.

use crate::common;

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::body::Body;
use bytes::Bytes;
use cc_lb_control::api_keys::principal_view::{
    DialectCache, PrincipalRoutingArtifacts, PrincipalView, RouterPipelineCache, ShapePluginCache,
};
use cc_lb_control::{DynamicViewBuilder, DynamicViewHolder};
use cc_lb_domain::{Principal, Upstream};
use cc_lb_engine::cache_keepalive::{
    CacheKeepaliveCancelRequest, CacheKeepaliveEnqueueError, CacheKeepaliveEnqueueRequest,
    CacheKeepaliveEnqueuer, CacheKeepaliveNotTrackedRequest,
};
use cc_lb_engine::lifecycle::PreviewRouteInput;
use cc_lb_engine::{
    ApiKeyAwareSignerFactory, DispatchError, Lifecycle, LifecycleConfig, UpstreamDispatch,
};
use cc_lb_routing::{FilterError, FilterOutput, FilterPlugin, RoutingContext};
use cc_lb_storage_api::principal::{Limit, PrincipalKind, PrincipalRecord};
use cc_lb_storage_api::upstream::{UpstreamKind, UpstreamRecord};
use cc_lb_storage_api::{CacheKeepaliveConfig, ClassifierConfig, PromptCacheMode};
use cc_lb_upstream::{
    DialectError, DialectShapeContext, RetryDecision, ShapedRequest, ShapedRequestBuilder,
    SignedRequest, Signer, SignerError, SignerFactory, SigningCapability, UpstreamDialect,
};
use http::{HeaderMap, HeaderValue, Response, StatusCode};
use serde_json::{Value, json};
use url::Url;
use uuid::Uuid;

const UPSTREAM_ID: &str = "00000000-0000-0000-0000-0000000000a1";
const PRINCIPAL_NAME: &str = "principal-test";

/// Agent-in-turn response: the classifier would schedule a renewal for it.
fn agent_turn_response() -> Value {
    json!({
        "type": "message",
        "id": "msg_agent_turn",
        "content": [{"type": "tool_use", "id": "toolu_1", "name": "Bash", "input": {}}],
        "stop_reason": "tool_use",
        "usage": {"input_tokens": 10, "output_tokens": 2}
    })
}

/// Records the exact signed body of every attempt for assertions. An optional
/// status queue drives the retry paths (401 refresh, 5xx failover).
struct RecordingDispatch {
    bodies: Arc<Mutex<Vec<Bytes>>>,
    response: Value,
    statuses: Mutex<VecDeque<StatusCode>>,
}

impl RecordingDispatch {
    fn next_status(&self) -> StatusCode {
        let mut statuses = self.statuses.lock().expect("dispatch statuses lock");
        let next = statuses.pop_front().unwrap_or(StatusCode::OK);
        // The last queued status repeats, so a retry loop cannot exhaust it.
        if statuses.is_empty() {
            statuses.push_back(next);
        }
        next
    }
}

#[async_trait]
impl UpstreamDispatch for RecordingDispatch {
    async fn dispatch(&self, request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        self.bodies
            .lock()
            .expect("dispatch bodies lock")
            .push(request.body().clone());
        let status = self.next_status();
        let body = if status == StatusCode::OK {
            Bytes::from(self.response.to_string())
        } else if status == StatusCode::UNAUTHORIZED {
            Bytes::from_static(br#"{"type":"error","error":{"type":"authentication_error"}}"#)
        } else {
            Bytes::from_static(br#"{"type":"error","error":{"type":"overloaded_error"}}"#)
        };
        let mut response = Response::new(Body::from(body));
        *response.status_mut() = status;
        response.headers_mut().insert(
            http::header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        Ok(response)
    }
}

/// Records every keep-alive enqueue the lifecycle attempts.
#[derive(Default)]
struct RecordingEnqueuer {
    enqueued: Mutex<Vec<CacheKeepaliveEnqueueRequest>>,
}

impl RecordingEnqueuer {
    fn enqueued(&self) -> Vec<CacheKeepaliveEnqueueRequest> {
        self.enqueued.lock().expect("enqueued lock").clone()
    }
}

#[async_trait]
impl CacheKeepaliveEnqueuer for RecordingEnqueuer {
    async fn enqueue_cache_keepalive(
        &self,
        request: CacheKeepaliveEnqueueRequest,
    ) -> Result<(), CacheKeepaliveEnqueueError> {
        self.enqueued.lock().expect("enqueued lock").push(request);
        Ok(())
    }

    async fn cancel_cache_keepalive(
        &self,
        _request: CacheKeepaliveCancelRequest,
    ) -> Result<(), CacheKeepaliveEnqueueError> {
        Ok(())
    }

    async fn record_cache_keepalive_not_tracked(
        &self,
        _request: CacheKeepaliveNotTrackedRequest,
    ) -> Result<(), CacheKeepaliveEnqueueError> {
        Ok(())
    }
}

/// Records the `RoutingContext` a filter plugin observes, for both the real
/// request path and the admin route preview.
#[derive(Default)]
struct RecordingFilter {
    bodies: Mutex<Vec<Bytes>>,
}

impl RecordingFilter {
    fn bodies(&self) -> Vec<Bytes> {
        self.bodies.lock().expect("filter bodies lock").clone()
    }
}

impl FilterPlugin for RecordingFilter {
    fn filter(
        &self,
        ctx: &RoutingContext,
        _principal: &Principal,
        candidates: &[cc_lb_domain::UpstreamCandidate],
    ) -> Result<FilterOutput, FilterError> {
        self.bodies
            .lock()
            .expect("filter bodies lock")
            .push(ctx.body_bytes.clone());
        Ok(FilterOutput {
            kept_upstream_ids: candidates
                .iter()
                .map(|candidate| candidate.upstream_id)
                .collect(),
            reason: "records the effective body".to_owned(),
            per_candidate_reasons: Vec::new(),
            subscription_preference: None,
        })
    }

    fn plugin_id(&self) -> Uuid {
        Uuid::from_u128(0x5eed)
    }

    fn plugin_name(&self) -> &str {
        "recording-filter"
    }
}

/// A dialect that rebuilds the body with the client's original five-minute
/// control, the way a plugin that ignores the principal policy would.
struct FiveMinuteDialect {
    base_url: Url,
}

impl UpstreamDialect for FiveMinuteDialect {
    fn shape(
        &self,
        context: &DialectShapeContext,
        _upstream: &Upstream,
        _principal: &Principal,
        builder: &mut ShapedRequestBuilder,
    ) -> Result<ShapedRequest, DialectError> {
        let mut url = self.base_url.clone();
        url.set_path(context.path.trim_start_matches('/'));
        let body = serde_json::to_vec(&json!({
            "model": "claude-test",
            "max_tokens": 32,
            "system": [{
                "type": "text",
                "text": "cached",
                "cache_control": {"type": "ephemeral", "ttl": "5m"}
            }],
            "messages": [{"role": "user", "content": "hello"}]
        }))
        .expect("dialect fixture body serializes");
        Ok(builder.shaped_request(
            url,
            context.method.clone(),
            context.downstream_headers.clone(),
            Bytes::from(body),
        ))
    }
}

/// A dialect whose shaped body is not JSON at all.
struct NonJsonDialect;

impl UpstreamDialect for NonJsonDialect {
    fn shape(
        &self,
        context: &DialectShapeContext,
        _upstream: &Upstream,
        _principal: &Principal,
        builder: &mut ShapedRequestBuilder,
    ) -> Result<ShapedRequest, DialectError> {
        let mut url = Url::parse("http://principal-cache.local/").expect("url parses");
        url.set_path(context.path.trim_start_matches('/'));
        Ok(builder.shaped_request(
            url,
            context.method.clone(),
            context.downstream_headers.clone(),
            Bytes::from_static(b"not-json"),
        ))
    }
}

/// A dialect whose shaping always fails, forcing the raw-passthrough fallback.
struct FailingShapeDialect;

impl UpstreamDialect for FailingShapeDialect {
    fn shape(
        &self,
        _context: &DialectShapeContext,
        _upstream: &Upstream,
        _principal: &Principal,
        _builder: &mut ShapedRequestBuilder,
    ) -> Result<ShapedRequest, DialectError> {
        Err(DialectError::UnsupportedRequest {
            reason: "test dialect always fails".to_owned(),
        })
    }
}

/// A signer that answers the first 401 with a refreshed credential, the way
/// the Anthropic OAuth signer does.
struct RefreshingSignerFactory {
    refreshes: Arc<AtomicUsize>,
}

#[async_trait]
impl ApiKeyAwareSignerFactory for RefreshingSignerFactory {
    fn with_router_choice(&self, _upstream_name: String) -> Arc<dyn SignerFactory> {
        Arc::new(RefreshingSignerFactory {
            refreshes: Arc::clone(&self.refreshes),
        })
    }
}

#[async_trait]
impl SignerFactory for RefreshingSignerFactory {
    async fn build(&self, _upstream: &Upstream) -> Result<Arc<dyn Signer>, SignerError> {
        Ok(Arc::new(RefreshingSigner {
            refreshes: Arc::clone(&self.refreshes),
        }))
    }
}

struct RefreshingSigner {
    refreshes: Arc<AtomicUsize>,
}

#[async_trait]
impl Signer for RefreshingSigner {
    async fn sign(
        &self,
        mut shaped: ShapedRequest,
        capability: &mut SigningCapability,
    ) -> Result<SignedRequest, SignerError> {
        shaped
            .headers_mut()
            .insert("x-api-key", HeaderValue::from_static("sk-ant-test"));
        Ok(SignedRequest::from_shaped(shaped, capability))
    }

    async fn on_unauthorized(&self, _err: &cc_lb_upstream::UpstreamError) -> RetryDecision {
        if self.refreshes.fetch_add(1, Ordering::Relaxed) == 0 {
            RetryDecision::Refresh {
                new_signer: Arc::new(RefreshingSigner {
                    refreshes: Arc::clone(&self.refreshes),
                }),
            }
        } else {
            RetryDecision::Fail
        }
    }
}

struct PassthroughSignerFactory;

#[async_trait]
impl ApiKeyAwareSignerFactory for PassthroughSignerFactory {
    fn with_router_choice(&self, _upstream_name: String) -> Arc<dyn SignerFactory> {
        Arc::new(PassthroughSignerFactory)
    }
}

#[async_trait]
impl SignerFactory for PassthroughSignerFactory {
    async fn build(&self, _upstream: &Upstream) -> Result<Arc<dyn Signer>, SignerError> {
        Ok(Arc::new(PassthroughSigner))
    }
}

struct PassthroughSigner;

#[async_trait]
impl Signer for PassthroughSigner {
    async fn sign(
        &self,
        mut shaped: ShapedRequest,
        capability: &mut SigningCapability,
    ) -> Result<SignedRequest, SignerError> {
        shaped
            .headers_mut()
            .insert("x-api-key", HeaderValue::from_static("sk-ant-test"));
        Ok(SignedRequest::from_shaped(shaped, capability))
    }

    async fn on_unauthorized(&self, _err: &cc_lb_upstream::UpstreamError) -> RetryDecision {
        RetryDecision::Fail
    }
}

struct Harness {
    lifecycle: Lifecycle,
    bodies: Arc<Mutex<Vec<Bytes>>>,
    enqueuer: Arc<RecordingEnqueuer>,
    filter: Arc<RecordingFilter>,
    principal_name: &'static str,
}

struct HarnessOptions {
    mode: PromptCacheMode,
    principal_name: &'static str,
    dialect: Option<DialectCache>,
    with_recording_filter: bool,
    refreshing_signer: bool,
    statuses: Vec<StatusCode>,
}

impl Default for HarnessOptions {
    fn default() -> Self {
        Self {
            mode: PromptCacheMode::PreserveClient,
            principal_name: PRINCIPAL_NAME,
            dialect: None,
            with_recording_filter: false,
            refreshing_signer: false,
            statuses: Vec::new(),
        }
    }
}

fn principal_record(name: &str, mode: PromptCacheMode) -> PrincipalRecord {
    PrincipalRecord {
        id: Uuid::new_v4(),
        name: name.to_owned(),
        kind: PrincipalKind::Machine,
        allowed_models: vec!["*".to_owned()],
        allowed_upstreams: Vec::new(),
        default_limits: Vec::<Limit>::new(),
        enabled: true,
        deleted_at_unix_secs: None,
        revision: 1,
        created_at_unix_secs: 0,
        updated_at_unix_secs: 0,
        router_terminal_strategy: Default::default(),
        cache_keepalive: Some(CacheKeepaliveConfig {
            enabled: true,
            refresh_lead_time_5m_secs: 30,
            refresh_lead_time_1h_secs: 300,
            max_refreshes_per_session: 12,
            max_total_duration_secs: 14_400,
            snapshot_max_bytes: 524_288,
            classifier: ClassifierConfig::default(),
        }),
        prompt_cache_mode: mode,
    }
}

fn upstream_record() -> UpstreamRecord {
    UpstreamRecord {
        id: Uuid::parse_str(UPSTREAM_ID).expect("upstream id parses"),
        name: "primary".to_owned(),
        kind: UpstreamKind::AnthropicOauth,
        base_url: Some(Url::parse("http://principal-cache.local/").expect("base url parses")),
        enabled: true,
        oauth_credentials: None,
        oauth_never_refresh: false,
        api_key_ciphertext: None,
        last_apply_error: None,
        last_apply_at_unix_secs: None,
        deleted_at_unix_secs: None,
        revision: 1,
        oauth_token_generation: 0,
        created_at_unix_secs: 0,
        updated_at_unix_secs: 0,
        warmup_enabled: false,
        warmup_dialect_plugin: None,
        last_warmup_at_unix_secs: None,
    }
}

fn harness(options: HarnessOptions) -> Harness {
    let filter = Arc::new(RecordingFilter::default());
    let user_filters: Vec<Arc<dyn FilterPlugin>> = if options.with_recording_filter {
        vec![Arc::clone(&filter) as Arc<dyn FilterPlugin>]
    } else {
        Vec::new()
    };
    let principal = principal_record(options.principal_name, options.mode);
    let mut chains: HashMap<String, PrincipalRoutingArtifacts> = HashMap::new();
    chains.insert(
        options.principal_name.to_owned(),
        (
            Some(Arc::new(RouterPipelineCache {
                user_filters,
                terminal: cc_lb_domain::TerminalStrategy::FirstPick,
                instantiation_error: None,
            })),
            options.dialect.unwrap_or(DialectCache::Inherit),
        ),
    );
    let principal_view = Arc::new(PrincipalView::from_db(&[principal], chains));
    let bodies = Arc::new(Mutex::new(Vec::new()));
    let signer_factory: Arc<dyn ApiKeyAwareSignerFactory> = if options.refreshing_signer {
        Arc::new(RefreshingSignerFactory {
            refreshes: Arc::new(AtomicUsize::new(0)),
        })
    } else {
        Arc::new(PassthroughSignerFactory)
    };
    let view = DynamicViewBuilder::new(0)
        .signer_factory(signer_factory)
        .principal_view(Arc::clone(&principal_view))
        .upstream_records(vec![upstream_record()])
        .build();
    let enqueuer = Arc::new(RecordingEnqueuer::default());
    let lifecycle = Lifecycle::new_with_dynamic_view(
        common::managed_authn(options.principal_name),
        Arc::new(DynamicViewHolder::new(view)),
        Arc::new(RecordingDispatch {
            bodies: Arc::clone(&bodies),
            response: agent_turn_response(),
            statuses: Mutex::new(VecDeque::from(options.statuses.clone())),
        }),
        LifecycleConfig::default(),
        Arc::new(cc_lb_clock::SystemClock),
    )
    .with_cache_keepalive_enqueuer(Arc::clone(&enqueuer) as Arc<dyn CacheKeepaliveEnqueuer>);
    Harness {
        lifecycle,
        bodies,
        enqueuer,
        filter,
        principal_name: options.principal_name,
    }
}

fn messages_body_with_controls() -> Value {
    json!({
        "model": "claude-test",
        "max_tokens": 32,
        "tool_choice": {"type": "any"},
        "cache_control": {"type": "ephemeral"},
        "tools": [{
            "name": "lookup",
            "description": "lookup",
            "cache_control": {"type": "ephemeral", "ttl": "5m"},
            "input_schema": {
                "type": "object",
                "properties": {"note": {"cache_control": {"type": "ephemeral"}}}
            }
        }],
        "system": [{"type": "text", "text": "cached", "cache_control": {"type": "ephemeral"}}],
        "messages": [{"role": "user", "content": [
            {"type": "text", "text": "hello", "cache_control": {"type": "ephemeral", "ttl": "5m"}}
        ]}]
    })
}

async fn send(harness: &Harness, body: &Value) -> StatusCode {
    let request = common::messages_request(Bytes::from(body.to_string()));
    let auth = harness
        .lifecycle
        .authenticate(request.headers())
        .await
        .expect("request authenticates");
    assert_eq!(auth.success().principal_id, harness.principal_name);
    let response = harness
        .lifecycle
        .handle(request, &auth)
        .await
        .expect("request completes");
    let (status, _headers, _body) = common::collect_body(response).await;
    status
}

async fn settle_keepalive() {
    // The keep-alive classification path is detached; the recording enqueuer
    // observes it after the response completes.
    common::settle().await;
}

fn signed_body(harness: &Harness) -> Value {
    let bodies = harness.bodies.lock().expect("dispatch bodies lock");
    let body = bodies.last().expect("a request reached the upstream");
    serde_json::from_slice(body).expect("signed body is JSON")
}

#[tokio::test]
async fn one_hour_mode_promotes_every_outbound_control_and_skips_keepalives() {
    // Given: a principal whose mode asks for one-hour lifetimes.
    let harness = harness(HarnessOptions {
        mode: PromptCacheMode::OneHourNoKeepalive,
        ..HarnessOptions::default()
    });

    // When: a request carrying controls at every supported location is proxied.
    let status = send(&harness, &messages_body_with_controls()).await;
    assert_eq!(status, StatusCode::OK);
    settle_keepalive().await;

    // Then: the signed body carries one-hour controls everywhere the contract
    // reads them, nested tool data is untouched, and no renewal was scheduled.
    let body = signed_body(&harness);
    assert_eq!(body["cache_control"]["ttl"], json!("1h"));
    assert_eq!(body["tools"][0]["cache_control"]["ttl"], json!("1h"));
    assert_eq!(body["system"][0]["cache_control"]["ttl"], json!("1h"));
    assert_eq!(
        body["messages"][0]["content"][0]["cache_control"]["ttl"],
        json!("1h")
    );
    assert_eq!(
        body["tools"][0]["input_schema"]["properties"]["note"]["cache_control"],
        json!({"type": "ephemeral"}),
        "nested tool schema data stays as the client sent it"
    );
    assert!(
        harness.enqueuer.enqueued().is_empty(),
        "one-hour mode must not schedule a synthetic renewal"
    );
}

#[tokio::test]
async fn one_hour_mode_does_not_invent_controls() {
    let harness = harness(HarnessOptions {
        mode: PromptCacheMode::OneHourNoKeepalive,
        ..HarnessOptions::default()
    });
    let mut body = messages_body_with_controls();
    let object = body.as_object_mut().expect("body object");
    object.remove("cache_control");
    object.remove("tools");
    object
        .get_mut("system")
        .expect("system array")
        .as_array_mut()
        .expect("system is an array")
        .iter_mut()
        .for_each(|block| {
            block
                .as_object_mut()
                .expect("system block")
                .remove("cache_control");
        });
    body["messages"][0]["content"][0]
        .as_object_mut()
        .expect("content block")
        .remove("cache_control");

    let status = send(&harness, &body).await;
    assert_eq!(status, StatusCode::OK);

    let signed = signed_body(&harness);
    assert_eq!(signed, body, "a request without controls gains none");
}

#[tokio::test]
async fn preserve_client_mode_leaves_controls_and_keepalives_alone() {
    let harness = harness(HarnessOptions::default());
    let body = messages_body_with_controls();

    let status = send(&harness, &body).await;
    assert_eq!(status, StatusCode::OK);
    settle_keepalive().await;

    let signed = signed_body(&harness);
    assert_eq!(signed, body, "client policy is the request as sent");
    assert_eq!(
        harness.enqueuer.enqueued().len(),
        1,
        "an agent-in-turn response still schedules a renewal under client policy"
    );
}

#[tokio::test]
async fn custom_dialect_cannot_reintroduce_a_five_minute_control() {
    // Given: a principal in one-hour mode whose dialect rewrites the body with
    // the client's original five-minute control.
    let harness = harness(HarnessOptions {
        mode: PromptCacheMode::OneHourNoKeepalive,
        dialect: Some(DialectCache::Explicit(ShapePluginCache {
            dialect: Arc::new(FiveMinuteDialect {
                base_url: Url::parse("http://principal-cache.local/").expect("url parses"),
            }),
        })),
        ..HarnessOptions::default()
    });

    let status = send(&harness, &messages_body_with_controls()).await;
    assert_eq!(status, StatusCode::OK);

    let body = signed_body(&harness);
    assert_eq!(
        body["system"][0]["cache_control"]["ttl"],
        json!("1h"),
        "the pre-sign guard re-asserts the principal policy"
    );
}

/// Sends one POST on an arbitrary proxy route and returns the response status.
/// The attempt's signed body is read from the same recording dispatch the
/// `/v1/messages` helpers use.
async fn send_on_path(harness: &Harness, path: &str, body: &Value) -> StatusCode {
    let request = http::Request::builder()
        .method(http::Method::POST)
        .uri(path)
        .header("x-api-key", common::managed_api_key())
        .header("anthropic-version", "2023-06-01")
        .body(Bytes::from(body.to_string()))
        .expect("test request builds");
    let auth = harness
        .lifecycle
        .authenticate(request.headers())
        .await
        .expect("request authenticates");
    let response = harness
        .lifecycle
        .handle(request, &auth)
        .await
        .expect("request completes");
    let (status, _headers, _body) = common::collect_body(response).await;
    status
}

#[tokio::test]
async fn one_hour_mode_leaves_a_non_messages_route_body_to_the_dialect() {
    // Given: a principal in one-hour mode whose dialect rewrites whatever body
    // the route receives with a five-minute control.
    let harness = harness(HarnessOptions {
        mode: PromptCacheMode::OneHourNoKeepalive,
        dialect: Some(DialectCache::Explicit(ShapePluginCache {
            dialect: Arc::new(FiveMinuteDialect {
                base_url: Url::parse("http://principal-cache.local/").expect("url parses"),
            }),
        })),
        ..HarnessOptions::default()
    });

    // When: the same principal calls the count-token route.
    let status = send_on_path(
        &harness,
        "/v1/messages/count_tokens",
        &messages_body_with_controls(),
    )
    .await;

    // Then: the request succeeds and the shaped body keeps the dialect's own
    // five-minute control. Only `/v1/messages` carries the messages cache
    // contract, so a one-hour principal must not promote controls on a route
    // whose body prompt-cache analysis never interpreted.
    assert_eq!(status, StatusCode::OK);
    let body = signed_body(&harness);
    assert_eq!(
        body["system"][0]["cache_control"]["ttl"],
        json!("5m"),
        "a non-messages route body is not normalized"
    );
}

#[tokio::test]
async fn one_hour_mode_does_not_reject_a_non_json_body_on_an_unrelated_route() {
    // Given: a principal in one-hour mode whose dialect produces a body the
    // messages cache policy cannot address.
    let harness = harness(HarnessOptions {
        mode: PromptCacheMode::OneHourNoKeepalive,
        dialect: Some(DialectCache::Explicit(ShapePluginCache {
            dialect: Arc::new(NonJsonDialect),
        })),
        ..HarnessOptions::default()
    });

    // When: the principal calls the count-token route.
    let status = send_on_path(
        &harness,
        "/v1/messages/count_tokens",
        &messages_body_with_controls(),
    )
    .await;

    // Then: the attempt is dispatched rather than failed with a cache-policy
    // error the messages contract does not apply to this route.
    assert_eq!(status, StatusCode::OK);
    let bodies = harness.bodies.lock().expect("dispatch bodies lock");
    assert_eq!(
        bodies
            .last()
            .expect("request reached the upstream")
            .as_ref(),
        b"not-json"
    );
}

#[tokio::test]
async fn raw_passthrough_fallback_sends_the_effective_body() {
    // Given: a principal in one-hour mode whose dialect fails to shape.
    let harness = harness(HarnessOptions {
        mode: PromptCacheMode::OneHourNoKeepalive,
        dialect: Some(DialectCache::Explicit(ShapePluginCache {
            dialect: Arc::new(FailingShapeDialect),
        })),
        ..HarnessOptions::default()
    });

    let status = send(&harness, &messages_body_with_controls()).await;
    assert_eq!(status, StatusCode::OK);

    let body = signed_body(&harness);
    assert_eq!(
        body["system"][0]["cache_control"]["ttl"],
        json!("1h"),
        "the raw fallback carries the effective body, not the client bytes"
    );
}

#[tokio::test]
async fn principals_sharing_one_upstream_keep_independent_cache_policies() {
    // Given: two principals on the same upstream login, one in one-hour mode.
    let one_hour = harness(HarnessOptions {
        mode: PromptCacheMode::OneHourNoKeepalive,
        principal_name: "principal-one-hour",
        ..HarnessOptions::default()
    });
    let client = harness(HarnessOptions {
        principal_name: "principal-client",
        ..HarnessOptions::default()
    });
    let body = messages_body_with_controls();

    assert_eq!(send(&one_hour, &body).await, StatusCode::OK);
    assert_eq!(send(&client, &body).await, StatusCode::OK);
    settle_keepalive().await;

    assert_eq!(
        signed_body(&one_hour)["system"][0]["cache_control"]["ttl"],
        json!("1h")
    );
    assert_eq!(
        signed_body(&client),
        body,
        "the client-policy principal's controls are sent exactly as received"
    );
    assert!(one_hour.enqueuer.enqueued().is_empty());
    assert_eq!(
        client.enqueuer.enqueued().len(),
        1,
        "the client-policy principal keeps its renewal"
    );
}

#[tokio::test]
async fn same_account_refresh_retries_with_the_effective_body() {
    // Given: an one-hour principal whose first attempt is rejected with 401.
    let harness = harness(HarnessOptions {
        mode: PromptCacheMode::OneHourNoKeepalive,
        refreshing_signer: true,
        statuses: vec![StatusCode::UNAUTHORIZED, StatusCode::OK],
        ..HarnessOptions::default()
    });

    // When: the signer refreshes and the request is retried.
    let status = send(&harness, &messages_body_with_controls()).await;

    // Then: every attempt carried the effective body, not the client's TTLs.
    assert_eq!(status, StatusCode::OK);
    let bodies = harness.bodies.lock().expect("dispatch bodies lock");
    assert_eq!(bodies.len(), 2, "the refresh retried the request once");
    for body in bodies.iter() {
        let value: Value = serde_json::from_slice(body).expect("attempt body is JSON");
        assert_eq!(value["system"][0]["cache_control"]["ttl"], json!("1h"));
    }
}

#[tokio::test]
async fn non_json_shaped_body_fails_instead_of_sending_client_bytes() {
    // Given: one-hour mode and a dialect that produces a body the policy can
    // no longer address.
    let harness = harness(HarnessOptions {
        mode: PromptCacheMode::OneHourNoKeepalive,
        dialect: Some(DialectCache::Explicit(ShapePluginCache {
            dialect: Arc::new(NonJsonDialect),
        })),
        ..HarnessOptions::default()
    });

    let status = send(&harness, &messages_body_with_controls()).await;

    assert_eq!(
        status,
        StatusCode::BAD_GATEWAY,
        "the attempt fails instead of falling back to unnormalized bytes"
    );
    assert!(
        harness
            .bodies
            .lock()
            .expect("dispatch bodies lock")
            .is_empty(),
        "nothing reached the upstream"
    );
}

#[tokio::test]
async fn preview_and_real_request_share_one_effective_body() {
    // Given: a principal in one-hour mode with a recording route filter.
    let harness = harness(HarnessOptions {
        mode: PromptCacheMode::OneHourNoKeepalive,
        with_recording_filter: true,
        ..HarnessOptions::default()
    });
    let request_body = messages_body_with_controls();

    // When: the admin preview and a real request carry the same client body.
    harness
        .lifecycle
        .preview_route(PreviewRouteInput {
            principal_id: PRINCIPAL_NAME.to_owned(),
            request_id: None,
            headers: HeaderMap::new(),
            body_bytes: Bytes::from(request_body.to_string()),
        })
        .await
        .expect("preview succeeds");
    assert_eq!(send(&harness, &request_body).await, StatusCode::OK);

    // Then: the filter observed identical effective bytes on both paths, that
    // body carries the one-hour controls, and the upstream received it.
    let observed = harness.filter.bodies();
    assert_eq!(observed.len(), 2, "preview and request both ran the filter");
    assert_eq!(
        observed[0].as_ref(),
        observed[1].as_ref(),
        "preview and real request agree on the effective body"
    );
    let effective: Value = serde_json::from_slice(&observed[0]).expect("effective body is JSON");
    assert_eq!(effective["system"][0]["cache_control"]["ttl"], json!("1h"));
    let signed = harness.bodies.lock().expect("dispatch bodies lock");
    assert_eq!(
        signed.last().expect("request reached upstream").as_ref(),
        observed[0].as_ref()
    );
}

#[tokio::test]
async fn preserve_client_preview_matches_the_client_body() {
    // Control: with client policy the preview must not rewrite anything.
    let harness = harness(HarnessOptions {
        with_recording_filter: true,
        ..HarnessOptions::default()
    });
    let request_body = messages_body_with_controls();

    harness
        .lifecycle
        .preview_route(PreviewRouteInput {
            principal_id: PRINCIPAL_NAME.to_owned(),
            request_id: None,
            headers: HeaderMap::new(),
            body_bytes: Bytes::from(request_body.to_string()),
        })
        .await
        .expect("preview succeeds");

    let observed = harness.filter.bodies();
    let preview_body: Value = serde_json::from_slice(&observed[0]).expect("preview body is JSON");
    assert_eq!(preview_body, request_body);
}
