use std::collections::{BTreeMap, VecDeque};
use std::fmt::Debug;
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::Router;
use axum::body::Body;
use axum::http::{Method, Request, Response, StatusCode};
use bytes::Bytes;
use cc_lb_config::Config;
use cc_lb_engine::{DispatchError, UpstreamDispatch};
use cc_lb_server::app::build_app_for_testing_with_dispatch;
use cc_lb_upstream::SignedRequest;
use http_body_util::BodyExt;
use serde_json::Value;
use tower::ServiceExt;

const ADMIN_TOKEN: &str = "rfc-0002-t2-admin";
const HAPPY_MODEL: &str = "claude-sonnet-4-5-20250929";
const HAPPY_BODY: &str = r#"{"model":"claude-sonnet-4-5-20250929","messages":[{"role":"user","content":"hi"}],"max_tokens":16}"#;
const STREAM_BODY: &str = r#"{"model":"claude-sonnet-4-5-20250929","messages":[{"role":"user","content":"hi"}],"max_tokens":16,"stream":true}"#;
const HAPPY_RESPONSE: &str = r#"{"id":"msg_rfc_0002","type":"message","role":"assistant","model":"claude-sonnet-4-5-20250929","content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn","stop_sequence":null,"usage":{"input_tokens":100,"output_tokens":50}}"#;
const RATE_LIMIT_RESPONSE: &str =
    r#"{"type":"error","error":{"type":"rate_limit_error","message":"Test rate-limited"}}"#;
const STREAM_RESPONSE: &str = concat!(
    "event: message_start\n",
    "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_rfc_0002_stream\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-sonnet-4-5-20250929\",\"content\":[],\"stop_reason\":null,\"stop_sequence\":null,\"usage\":{\"input_tokens\":100,\"output_tokens\":0}}}\n\n",
    "event: content_block_start\n",
    "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
    "event: content_block_delta\n",
    "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"ok\"}}\n\n",
    "event: content_block_stop\n",
    "data: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
    "event: message_delta\n",
    "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\",\"stop_sequence\":null},\"usage\":{\"input_tokens\":100,\"output_tokens\":50}}\n\n",
    "event: message_stop\n",
    "data: {\"type\":\"message_stop\"}\n\n",
);

#[tokio::test(flavor = "current_thread")]
async fn t2__rfc_0002_terminal_mapping_table() {
    let dispatch = Arc::new(ScriptedDispatch::new([
        json_response(StatusCode::OK, HAPPY_RESPONSE),
        json_response(StatusCode::OK, HAPPY_RESPONSE),
        json_response(StatusCode::OK, HAPPY_RESPONSE),
        stream_response(),
        json_response(StatusCode::TOO_MANY_REQUESTS, RATE_LIMIT_RESPONSE),
    ]));
    let mut config = Config::default();
    config.admin.token = Some(ADMIN_TOKEN.to_owned());
    config.lifecycle_hook_adapter.enabled = false;
    config.lifecycle_pricing_subscriber.enabled = true;
    config.lifecycle_cache_observation_subscriber.enabled = true;
    let app = build_app_for_testing_with_dispatch(
        config,
        cc_lb_testkit::fixed_clock(1_700_000_000),
        Some(dispatch.clone()),
    )
    .await
    .expect("build RFC-0002 T2 app");
    let router = app.router.clone();
    let admin_router = app.admin_router.clone();
    let mut final_events = FinalEventStream::open(&admin_router).await;

    let oauth_before = request_event_count(&admin_router).await;
    let (oauth_status, _) =
        proxy_request(&router, Method::GET, "/api/oauth/usage", Body::empty(), &[]).await;
    run_barrier(&router, &mut final_events).await;
    let oauth_after = request_event_count(&admin_router).await;
    let oauth_terminal_dropped = terminal_dropped_count(&admin_router).await;

    let unknown_before = request_event_count(&admin_router).await;
    let (not_found_status, _) = proxy_request(
        &router,
        Method::GET,
        "/definitely/not-a-route",
        Body::empty(),
        &[],
    )
    .await;
    run_barrier(&router, &mut final_events).await;
    let unknown_after = request_event_count(&admin_router).await;
    let unknown_terminal_dropped = terminal_dropped_count(&admin_router).await;

    let non_stream_before = request_event_count(&admin_router).await;
    let ((non_stream_status, non_stream_event), happy_metrics_before, happy_metrics_after) =
        with_fresh_counter_snapshots(async {
            let (status, _) = proxy_request(
                &router,
                Method::POST,
                "/v1/messages",
                Body::from(Bytes::from_static(HAPPY_BODY.as_bytes())),
                &[
                    ("content-type", "application/json"),
                    ("anthropic-version", "2023-06-01"),
                ],
            )
            .await;
            let event = final_events.next().await;
            (status, event)
        })
        .await;
    let non_stream_after = request_event_count(&admin_router).await;

    let stream_before = non_stream_after;
    let (stream_status, _) = proxy_request(
        &router,
        Method::POST,
        "/v1/messages",
        Body::from(Bytes::from_static(STREAM_BODY.as_bytes())),
        &[
            ("content-type", "application/json"),
            ("anthropic-version", "2023-06-01"),
            ("accept", "text/event-stream"),
        ],
    )
    .await;
    let stream_event = final_events.next().await;
    let stream_after = request_event_count(&admin_router).await;

    let rate_limited_before = stream_after;
    let ((rate_limited_status, rate_limited_event), provider_error_before, provider_error_after) =
        with_fresh_counter_snapshots(async {
            let (status, _) = proxy_request(
                &router,
                Method::POST,
                "/v1/messages",
                Body::from(Bytes::from_static(HAPPY_BODY.as_bytes())),
                &[
                    ("content-type", "application/json"),
                    ("anthropic-version", "2023-06-01"),
                ],
            )
            .await;
            let event = final_events.next().await;
            (status, event)
        })
        .await;
    let rate_limited_after = request_event_count(&admin_router).await;

    let invalid_json_before = rate_limited_after;
    let dispatches_before_invalid = dispatch.call_count();
    let ((invalid_json_status, invalid_json_event), invalid_metrics_before, invalid_metrics_after) =
        with_fresh_counter_snapshots(async {
            let (status, _) = proxy_request(
                &router,
                Method::POST,
                "/v1/messages",
                Body::from(Bytes::from_static(b"not json at all")),
                &[
                    ("content-type", "application/json"),
                    ("anthropic-version", "2023-06-01"),
                ],
            )
            .await;
            let event = final_events.next().await;
            (status, event)
        })
        .await;
    let invalid_json_after = request_event_count(&admin_router).await;

    let lifecycle_kinds = [
        "request_started",
        "parse_completed",
        "auth_completed",
        "authentication_completed",
        "route_completed",
        "limit_decision",
        "upstream_attempt",
        "upstream_response_started",
        "usage_observed",
        "request_terminated",
        "priced",
        "cache_observed",
    ];
    let lifecycle_deltas = lifecycle_kinds
        .into_iter()
        .map(|kind| {
            (
                kind,
                counter_delta(
                    &happy_metrics_before,
                    &happy_metrics_after,
                    "cc_lb_lifecycle_events_total",
                    &[("kind", kind)],
                ),
            )
        })
        .collect::<BTreeMap<_, _>>();

    let observed = Observed {
        oauth_status,
        oauth_before,
        oauth_after,
        oauth_terminal_dropped,
        not_found_status,
        unknown_before,
        unknown_after,
        unknown_terminal_dropped,
        non_stream_status,
        non_stream_before,
        non_stream_after,
        non_stream_event,
        stream_status,
        stream_before,
        stream_after,
        stream_event,
        lifecycle_deltas,
        happy_provider_error_delta: counter_delta(
            &happy_metrics_before,
            &happy_metrics_after,
            "cc_lb_lifecycle_events_total",
            &[("kind", "provider_error_observed")],
        ),
        rate_limited_status,
        rate_limited_before,
        rate_limited_after,
        rate_limited_event,
        provider_error_delta: counter_delta(
            &provider_error_before,
            &provider_error_after,
            "cc_lb_lifecycle_events_total",
            &[("kind", "provider_error_observed")],
        ),
        invalid_json_status,
        invalid_json_before,
        invalid_json_after,
        invalid_json_event,
        invalid_parse_delta: counter_delta(
            &invalid_metrics_before,
            &invalid_metrics_after,
            "cc_lb_lifecycle_events_total",
            &[("kind", "parse_completed")],
        ),
        invalid_terminated_delta: counter_delta(
            &invalid_metrics_before,
            &invalid_metrics_after,
            "cc_lb_lifecycle_events_total",
            &[("kind", "request_terminated")],
        ),
        invalid_route_delta: counter_delta(
            &invalid_metrics_before,
            &invalid_metrics_after,
            "cc_lb_lifecycle_events_total",
            &[("kind", "route_completed")],
        ),
        invalid_upstream_delta: counter_delta(
            &invalid_metrics_before,
            &invalid_metrics_after,
            "cc_lb_lifecycle_events_total",
            &[("kind", "upstream_attempt")],
        ),
        invalid_dispatch_delta: dispatch.call_count() - dispatches_before_invalid,
    };

    type Assertion = fn(&Observed);
    let cases: [(&str, Assertion); 8] = [
        (
            "live_qa_2_oauth_usage_does_not_produce_request_event_row",
            assert_live_qa_2,
        ),
        (
            "live_qa_3_unknown_route_and_method_not_allowed_do_not_write_row",
            assert_live_qa_3,
        ),
        (
            "live_qa_6b_assembler_populates_cost_cache_usage_fields",
            assert_live_qa_6b,
        ),
        (
            "live_qa_6c_assembler_populates_stream_fields",
            assert_live_qa_6c,
        ),
        (
            "lqa_2b_lifecycle_metrics_increment_for_happy_non_stream",
            assert_lqa_2b,
        ),
        (
            "lqa_6b_non_stream_and_stream_rows_have_cache_and_cost_fields",
            assert_lqa_6b,
        ),
        (
            "lqa_6c_upstream_rate_limit_error_records_provider_error_metric",
            assert_lqa_6c,
        ),
        (
            "lqa_6f_invalid_json_records_400_and_stops_before_routing",
            assert_lqa_6f,
        ),
    ];
    for (_original, assert_case) in cases {
        assert_case(&observed);
    }

    assert_eq!(dispatch.pending_responses(), 0);
}

type CounterSnapshot = Vec<(String, Vec<(String, String)>, u64)>;

async fn with_fresh_counter_snapshots<T>(
    operation: impl Future<Output = T>,
) -> (T, CounterSnapshot, CounterSnapshot) {
    let (recorder, snapshotter) = cc_lb_testkit::local_recorder();
    let _recorder_guard = cc_lb_testkit::install_local_recorder(&recorder);
    let capture = || {
        snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .filter_map(|(key, _, _, value)| {
                let count = debug_counter_value(&value)?;
                let labels = key
                    .key()
                    .labels()
                    .map(|label| (label.key().to_owned(), label.value().to_owned()))
                    .collect::<Vec<_>>();
                Some((key.key().name().to_owned(), labels, count))
            })
            .collect::<CounterSnapshot>()
    };

    let before = capture();
    let output = operation.await;
    let after = capture();
    (output, before, after)
}

struct Observed {
    oauth_status: StatusCode,
    oauth_before: u64,
    oauth_after: u64,
    oauth_terminal_dropped: usize,
    not_found_status: StatusCode,
    unknown_before: u64,
    unknown_after: u64,
    unknown_terminal_dropped: usize,
    non_stream_status: StatusCode,
    non_stream_before: u64,
    non_stream_after: u64,
    non_stream_event: Value,
    stream_status: StatusCode,
    stream_before: u64,
    stream_after: u64,
    stream_event: Value,
    lifecycle_deltas: BTreeMap<&'static str, u64>,
    happy_provider_error_delta: u64,
    rate_limited_status: StatusCode,
    rate_limited_before: u64,
    rate_limited_after: u64,
    rate_limited_event: Value,
    provider_error_delta: u64,
    invalid_json_status: StatusCode,
    invalid_json_before: u64,
    invalid_json_after: u64,
    invalid_json_event: Value,
    invalid_parse_delta: u64,
    invalid_terminated_delta: u64,
    invalid_route_delta: u64,
    invalid_upstream_delta: u64,
    invalid_dispatch_delta: usize,
}

fn assert_live_qa_2(observed: &Observed) {
    let status = observed.oauth_status.as_u16();
    assert!(
        status < 500 || status == 401 || status == 404,
        "oauth usage returned unexpected server error: {status}"
    );
    assert_eq!(
        observed.oauth_after,
        observed.oauth_before + 1,
        "GET /api/oauth/usage must not produce a request_events row; the one-row delta is the deterministic barrier request"
    );
    assert_eq!(
        observed.oauth_terminal_dropped, 0,
        "terminal_dropped rows must be zero for non-lifecycle routes"
    );
}

fn assert_live_qa_3(observed: &Observed) {
    assert_eq!(observed.not_found_status, StatusCode::NOT_FOUND);
    assert_eq!(
        observed.unknown_after,
        observed.unknown_before + 1,
        "404 fallback must not produce a request_events row; the one-row delta is the deterministic barrier request"
    );
    assert_eq!(
        observed.unknown_terminal_dropped, 0,
        "terminal_dropped rows must be zero"
    );
}

fn assert_live_qa_6b(observed: &Observed) {
    assert_eq!(observed.non_stream_status, StatusCode::OK);
    assert_eq!(observed.non_stream_after, observed.non_stream_before + 1);
    let event = &observed.non_stream_event;
    assert_eq!(json_i64(event, "status"), 200);
    assert_eq!(json_str(event, "principal_id"), "test-principal");
    assert_eq!(json_str(event, "key_id"), "none-mode");
    assert_eq!(json_str(event, "principal_kind"), "machine");
    assert_eq!(json_str(event, "upstream_name"), "test-upstream");
    assert!(
        json_str(event, "upstream_id").parse::<uuid::Uuid>().is_ok(),
        "upstream_id must be a UUID: {}",
        event["upstream_id"]
    );
    assert_eq!(json_str(event, "model"), HAPPY_MODEL);
    assert_eq!(json_str(event, "upstream"), "anthropic_direct");
    assert_eq!(json_i64(event, "input_tokens"), 100);
    assert_eq!(json_i64(event, "output_tokens"), 50);
    assert!(event.get("error_code").is_some_and(Value::is_null));
    let ctx = "non-stream happy path";
    for field in [
        "cost_usd_micros",
        "cost_input_micros",
        "cost_output_micros",
        "cache_state",
        "auth_ms",
        "route_ms",
        "shape_ms",
        "sign_ms",
        "upstream_ttfb_ms",
        "upstream_body_ms",
        "body_bytes",
        "body_chunk_count",
    ] {
        assert_field_populated(event, field, ctx);
    }
    for field in ["cost_usd_micros", "cost_input_micros", "cost_output_micros"] {
        assert!(json_i64(event, field) > 0, "{field} must be positive");
    }
}

fn assert_live_qa_6c(observed: &Observed) {
    assert_eq!(observed.stream_status, StatusCode::OK);
    assert_eq!(observed.stream_after, observed.stream_before + 1);
    let ctx = "streaming happy path";
    for field in [
        "body_bytes",
        "stream_message_start_ms",
        "stream_total_ms",
        "sse_event_count",
        "cost_usd_micros",
        "input_tokens",
        "output_tokens",
    ] {
        assert_field_populated(&observed.stream_event, field, ctx);
    }
}

fn assert_lqa_2b(observed: &Observed) {
    assert_eq!(observed.non_stream_status, StatusCode::OK);
    for kind in [
        "request_started",
        "parse_completed",
        "auth_completed",
        "authentication_completed",
        "route_completed",
        "limit_decision",
        "upstream_attempt",
        "upstream_response_started",
        "usage_observed",
        "request_terminated",
        "priced",
        "cache_observed",
    ] {
        assert_eq!(observed.lifecycle_deltas[kind], 1, "metric {kind}");
    }
    assert_eq!(observed.happy_provider_error_delta, 0);
}

fn assert_lqa_6b(observed: &Observed) {
    assert_eq!(observed.non_stream_status, StatusCode::OK);
    assert_eq!(observed.stream_status, StatusCode::OK);
    assert_eq!(observed.stream_after, observed.non_stream_before + 2);
    for event in [&observed.non_stream_event, &observed.stream_event] {
        for field in [
            "cache_state",
            "cost_usd_micros",
            "cost_input_micros",
            "cost_output_micros",
        ] {
            assert_field_populated(event, field, "non-stream/stream cache and cost");
        }
    }
}

fn assert_lqa_6c(observed: &Observed) {
    assert_eq!(observed.rate_limited_status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(
        observed.rate_limited_after,
        observed.rate_limited_before + 1
    );
    assert_eq!(json_i64(&observed.rate_limited_event, "status"), 429);
    assert_eq!(
        json_str(&observed.rate_limited_event, "error_code"),
        "upstream_4xx"
    );
    assert_eq!(observed.provider_error_delta, 1);
}

fn assert_lqa_6f(observed: &Observed) {
    assert_eq!(observed.invalid_json_status, StatusCode::BAD_REQUEST);
    assert_eq!(
        observed.invalid_json_after,
        observed.invalid_json_before + 1
    );
    assert_eq!(
        json_str(&observed.invalid_json_event, "error_code"),
        "invalid_json"
    );
    assert_eq!(observed.invalid_parse_delta, 1);
    assert_eq!(observed.invalid_terminated_delta, 1);
    assert_eq!(observed.invalid_route_delta, 0);
    assert_eq!(observed.invalid_upstream_delta, 0);
    assert_eq!(observed.invalid_dispatch_delta, 0);
}

fn assert_field_populated(event: &Value, field: &str, ctx: &str) {
    let value = event.get(field);
    assert!(
        value.is_some_and(|value| !value.is_null()),
        "[{ctx}] assembler row missing {field}. value={value:?}\nfull payload:\n{}",
        serde_json::to_string_pretty(event).unwrap_or_default()
    );
}

fn json_i64(json: &Value, field: &str) -> i64 {
    json.get(field)
        .and_then(Value::as_i64)
        .unwrap_or_else(|| panic!("missing integer field {field}: {json}"))
}

fn json_str<'a>(json: &'a Value, field: &str) -> &'a str {
    json.get(field)
        .and_then(Value::as_str)
        .unwrap_or_else(|| panic!("missing string field {field}: {json}"))
}

fn debug_counter_value(value: &impl Debug) -> Option<u64> {
    let rendered = format!("{value:?}");
    rendered
        .strip_prefix("Counter(")?
        .strip_suffix(')')?
        .parse()
        .ok()
}

fn counter_delta(
    before: &CounterSnapshot,
    after: &CounterSnapshot,
    name: &str,
    labels: &[(&str, &str)],
) -> u64 {
    counter_value(after, name, labels)
        .checked_sub(counter_value(before, name, labels))
        .unwrap_or_else(|| panic!("counter {name} with labels {labels:?} decreased"))
}

fn counter_value(snapshot: &CounterSnapshot, name: &str, labels: &[(&str, &str)]) -> u64 {
    snapshot
        .iter()
        .find(|(sample_name, sample_labels, _)| {
            sample_name == name
                && sample_labels.len() == labels.len()
                && labels.iter().all(|(key, value)| {
                    sample_labels.iter().any(|(sample_key, sample_value)| {
                        sample_key == key && sample_value == value
                    })
                })
        })
        .map_or(0, |(_, _, value)| *value)
}

async fn run_barrier(router: &Router, final_events: &mut FinalEventStream) {
    let (status, _) = proxy_request(
        router,
        Method::POST,
        "/v1/messages",
        Body::from(Bytes::from_static(HAPPY_BODY.as_bytes())),
        &[
            ("content-type", "application/json"),
            ("anthropic-version", "2023-06-01"),
        ],
    )
    .await;
    assert_eq!(status, StatusCode::OK, "barrier request");
    let event = final_events.next().await;
    assert_eq!(event["model"], HAPPY_MODEL, "barrier final event");
}

async fn proxy_request(
    router: &Router,
    method: Method,
    uri: &str,
    body: Body,
    headers: &[(&str, &str)],
) -> (StatusCode, Bytes) {
    let mut builder = Request::builder().method(method).uri(uri);
    for (name, value) in headers {
        builder = builder.header(*name, *value);
    }
    let response = router
        .clone()
        .oneshot(builder.body(body).expect("proxy request builds"))
        .await
        .expect("proxy response");
    let status = response.status();
    let body = response
        .into_body()
        .collect()
        .await
        .expect("proxy body collects")
        .to_bytes();
    (status, body)
}

async fn request_event_count(admin_router: &Router) -> u64 {
    recent_events(admin_router).await["count"]
        .as_u64()
        .expect("recent event count")
}

async fn terminal_dropped_count(admin_router: &Router) -> usize {
    recent_events(admin_router).await["events"]
        .as_array()
        .expect("recent events array")
        .iter()
        .filter(|event| event["error_code"] == "terminal_dropped")
        .count()
}

async fn recent_events(admin_router: &Router) -> Value {
    let response = admin_router
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/admin/events/recent?limit=500")
                .header("authorization", format!("Bearer {ADMIN_TOKEN}"))
                .body(Body::empty())
                .expect("recent events request builds"),
        )
        .await
        .expect("recent events response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = response
        .into_body()
        .collect()
        .await
        .expect("recent events body collects")
        .to_bytes();
    serde_json::from_slice(&body).expect("recent events JSON")
}

struct FinalEventStream {
    body: Body,
    pending: String,
}

impl FinalEventStream {
    async fn open(admin_router: &Router) -> Self {
        let response = admin_router
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/admin/events/stream")
                    .header("authorization", format!("Bearer {ADMIN_TOKEN}"))
                    .body(Body::empty())
                    .expect("event stream request builds"),
            )
            .await
            .expect("event stream response");
        assert_eq!(response.status(), StatusCode::OK);
        Self {
            body: response.into_body(),
            pending: String::new(),
        }
    }

    async fn next(&mut self) -> Value {
        loop {
            while let Some(end) = self.pending.find("\n\n") {
                let frame = self.pending[..end].to_owned();
                self.pending.drain(..end + 2);
                let Some(data) = frame.lines().find_map(|line| line.strip_prefix("data: ")) else {
                    continue;
                };
                let update: Value = serde_json::from_str(data).expect("SSE data JSON");
                if update["phase"] == "final" {
                    return update["payload"]["event"].clone();
                }
            }

            let frame = self
                .body
                .frame()
                .await
                .expect("event stream remains open")
                .expect("event stream frame");
            if let Ok(data) = frame.into_data() {
                self.pending
                    .push_str(std::str::from_utf8(&data).expect("event stream UTF-8"));
            }
        }
    }
}

struct ScriptedDispatch {
    responses: Mutex<VecDeque<Response<Body>>>,
    calls: AtomicUsize,
}

impl ScriptedDispatch {
    fn new(responses: impl IntoIterator<Item = Response<Body>>) -> Self {
        Self {
            responses: Mutex::new(responses.into_iter().collect()),
            calls: AtomicUsize::new(0),
        }
    }

    fn call_count(&self) -> usize {
        self.calls.load(Ordering::Relaxed)
    }

    fn pending_responses(&self) -> usize {
        self.responses
            .lock()
            .expect("scripted responses lock")
            .len()
    }
}

#[async_trait]
impl UpstreamDispatch for ScriptedDispatch {
    async fn dispatch(&self, _request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        Ok(self
            .responses
            .lock()
            .expect("scripted responses lock")
            .pop_front()
            .expect("scripted response queue exhausted"))
    }
}

fn json_response(status: StatusCode, body: &'static str) -> Response<Body> {
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .header("request-id", "req_rfc_0002")
        .header("anthropic-organization-id", "org_test")
        .header("anthropic-ratelimit-requests-remaining", "999")
        .header("anthropic-ratelimit-tokens-remaining", "999000")
        .body(Body::from(Bytes::from_static(body.as_bytes())))
        .expect("JSON response builds")
}

fn stream_response() -> Response<Body> {
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "text/event-stream")
        .header("request-id", "req_rfc_0002_stream")
        .header("anthropic-organization-id", "org_test")
        .header("anthropic-ratelimit-requests-remaining", "999")
        .header("anthropic-ratelimit-tokens-remaining", "999000")
        .body(Body::from(Bytes::from_static(STREAM_RESPONSE.as_bytes())))
        .expect("stream response builds")
}
