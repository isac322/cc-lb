use super::*;
use cc_lb_control::dynamic_view::DynamicViewBuilder;
use cc_lb_request_log::RequestCacheEvidence;

// Captured Codex wire from the #1298 provider comparison, 2026-11-14.
// Response/message IDs, cache keys, and safety identifiers are fixed sentinels.
// The ten-event framing, nested usage, unknown fields, and tier changes are retained.
const REQUEST: &[u8] = include_bytes!("fixtures/codex-request.json");
const RENEWAL: &[u8] = include_bytes!("fixtures/codex-renewal.json");
const SSE: &[u8] = include_bytes!("fixtures/codex-renewal.sse");
const JSON_HIT: &[u8] = br#"{"usage":{"input_tokens":2048,"output_tokens":5,"input_tokens_details":{"cached_tokens":1024}}}"#;

struct WireDispatch {
    requests: Mutex<Vec<cc_lb_upstream::SignedRequest>>,
    status: StatusCode,
    body: Bytes,
}

#[async_trait::async_trait]
impl UpstreamDispatch for WireDispatch {
    async fn dispatch(
        &self,
        request: cc_lb_upstream::SignedRequest,
    ) -> Result<Response<crate::lifecycle::Body>, crate::lifecycle::DispatchError> {
        self.requests.lock().unwrap().push(request);
        let mut response = Response::new(crate::lifecycle::Body::from(self.body.clone()));
        *response.status_mut() = self.status;
        // Deliberately mismatched for SSE: the upstream kind owns the mode.
        response.headers_mut().insert(
            http::header::CONTENT_TYPE,
            http::HeaderValue::from_static("application/json"),
        );
        Ok(response)
    }
}

async fn dispatch_wire(
    kind: UpstreamKind,
    request: &[u8],
    status: StatusCode,
    response: Bytes,
    hydrate: bool,
) -> (DispatchOutcome, Vec<cc_lb_upstream::SignedRequest>) {
    let upstream = UpstreamRecord {
        id: Uuid::from_u128(1298),
        name: "codex-renewal".to_owned(),
        kind,
        enabled: true,
        ..UpstreamRecord::default()
    };
    let observations = Arc::new(RecordingKeepaliveObservationSink::default());
    let view = DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(TestSignerFactory))
        .principal_view(Arc::new(
            cc_lb_control::api_keys::principal_view::PrincipalView::for_tests(
                "test",
                true,
                vec!["*".to_owned()],
                Vec::new(),
                HashMap::new(),
            ),
        ))
        .upstream_records(vec![upstream.clone()])
        .prompt_cache_observation_sink(observations.clone())
        .build();
    let http = Arc::new(WireDispatch {
        requests: Mutex::new(Vec::new()),
        status,
        body: response,
    });
    let dispatcher = AnthropicKeepaliveDispatcher::new(
        Arc::new(DynamicViewHolder::new(view)),
        Arc::new(TestUpstreamStore::new(upstream.clone())),
        http.clone(),
        Arc::new(cc_lb_clock::SystemClock),
    );
    let mut snapshot = RequestSnapshot::capture_openai(
        "https://example.com/v1/responses".parse().unwrap(),
        Method::POST,
        HeaderMap::new(),
        Bytes::copy_from_slice(request),
        upstream.id,
        cc_lb_storage_api::CacheTtl::Ttl30m,
        usize::MAX,
    )
    .unwrap()
    .with_prefix_evidence(
        Some("fingerprint".to_owned()),
        Some("hash".to_owned()),
        Some("policy".to_owned()),
        Some("schema".to_owned()),
    );
    if hydrate {
        let persisted = snapshot.to_persisted();
        assert_eq!(persisted.body, request);
        snapshot = RequestSnapshot::from_persisted(persisted.clone()).unwrap();
        assert_eq!(snapshot.to_persisted(), persisted);
    }
    let outcome = dispatcher
        .dispatch(
            &snapshot,
            KeepaliveDispatchContext::new(None, "codex-regression".to_owned(), u64::MAX),
        )
        .await;
    assert!(
        observations.records.lock().is_empty(),
        "Responses must not publish Anthropic prefix observations"
    );
    let requests = http.requests.lock().unwrap().clone();
    (outcome, requests)
}

#[tokio::test]
async fn codex_captured_request_shape() {
    let (_, requests) = dispatch_wire(
        UpstreamKind::OpenaiOauth,
        REQUEST,
        StatusCode::OK,
        Bytes::from_static(SSE),
        false,
    )
    .await;
    let [request] = requests.as_slice() else {
        panic!("renewal request missing")
    };
    assert_eq!(request.body().as_ref(), RENEWAL);
}

#[tokio::test]
async fn codex_captured_response_usage() {
    let (outcome, _) = dispatch_wire(
        UpstreamKind::OpenaiOauth,
        REQUEST,
        StatusCode::OK,
        Bytes::from_static(SSE),
        false,
    )
    .await;
    let DispatchOutcome::CacheMiss { finalization } = outcome else {
        panic!("captured zero-cache SSE must finalize a measured miss");
    };
    let usage = finalization.usage;
    assert_eq!(
        (
            usage.input_tokens,
            usage.output_tokens,
            usage.cache_read_input_tokens
        ),
        (16, 5, 0)
    );
    assert_eq!(usage.cache_read_evidence, RequestCacheEvidence::Zero);
    assert_eq!(usage.cache_write_evidence, RequestCacheEvidence::Zero);
    assert_eq!(
        (
            usage.cache_write_input_tokens,
            usage.cache_write_input_tokens_30m,
            usage.cache_write_input_tokens_12h
        ),
        (Some(0), Some(0), Some(0))
    );
    assert_eq!(usage.service_tier.as_deref(), Some("default"));
    assert_eq!(usage.thinking_tokens, 0);
    assert!(usage.usage_present && usage.is_openai_responses && !usage.provider_cost_unknown);
    assert_eq!(finalization.status, 200);
}

// Synthetic counters in the captured terminal envelope, not a live cache hit.
fn terminal_wire(event: &str, usage: Value) -> Bytes {
    let capture = std::str::from_utf8(SSE).unwrap();
    let terminal = capture
        .split("\n\n")
        .find(|frame| frame.starts_with("event: response.completed\n"))
        .unwrap();
    let mut value: Value = serde_json::from_str(terminal.split_once("data: ").unwrap().1).unwrap();
    value["type"] = json!(event);
    value["response"]["usage"] = usage;
    value["response"]["status"] = json!(if event == "response.incomplete" {
        "incomplete"
    } else {
        "completed"
    });
    if event == "response.incomplete" {
        value["response"]["incomplete_details"] = json!({"reason":"max_output_tokens"});
    }
    // A pre-terminal usage value must not contaminate the terminal snapshot.
    Bytes::from(format!(
        "event: response.in_progress\ndata: {{\"type\":\"response.in_progress\",\"response\":{{\"usage\":{{\"input_tokens\":9999,\"input_tokens_details\":{{\"cached_tokens\":9999}}}}}}}}\n\nevent: {event}\ndata: {value}\n\n"
    ))
}

#[tokio::test]
async fn codex_terminal_usage() {
    for event in ["response.completed", "response.incomplete"] {
        let body = terminal_wire(
            event,
            json!({
                "input_tokens":2048,"output_tokens":5,
                "input_tokens_details":{"cached_tokens":1024,"cache_write_tokens":100,
                    "cache_write_tokens_details":{"ttl_30m_tokens":60,"ttl_12h_tokens":40}},
                "output_tokens_details":{"reasoning_tokens":2}
            }),
        );
        let (outcome, _) = dispatch_wire(
            UpstreamKind::OpenaiOauth,
            REQUEST,
            StatusCode::OK,
            body,
            false,
        )
        .await;
        let DispatchOutcome::CacheHit { finalization, .. } = outcome else {
            panic!("{event} must retain paid cache-hit usage")
        };
        let usage = finalization.usage;
        assert_eq!(
            (
                usage.input_tokens,
                usage.output_tokens,
                usage.cache_read_input_tokens
            ),
            (924, 5, 1024)
        );
        assert_eq!(usage.cache_write_input_tokens, Some(100));
        assert_eq!(
            (
                usage.cache_write_input_tokens_30m,
                usage.cache_write_input_tokens_12h
            ),
            (Some(60), Some(40))
        );
        assert_eq!(usage.cache_write_evidence, RequestCacheEvidence::Positive);
        assert_eq!(usage.service_tier.as_deref(), Some("default"));
        assert_eq!(usage.thinking_tokens, 2);
    }
}

#[tokio::test]
async fn codex_unknown_usage() {
    for event in ["response.completed", "response.incomplete"] {
        for usage in [Value::Null, json!(true), json!("invalid")] {
            let (outcome, _) = dispatch_wire(
                UpstreamKind::OpenaiOauth,
                REQUEST,
                StatusCode::OK,
                terminal_wire(event, usage),
                false,
            )
            .await;
            let DispatchOutcome::CacheMiss { finalization } = outcome else {
                panic!("terminal without usage must remain unknown")
            };
            assert!(!finalization.usage.usage_present);
            assert!(finalization.usage.provider_cost_unknown);
            assert_eq!(finalization.usage.cache_write_input_tokens, None);
        }
    }
}

#[tokio::test]
async fn codex_invalid_streams() {
    for (status, body) in [
        (StatusCode::BAD_REQUEST, br#"{"detail":"Stream must be set to true"}"#.as_slice()),
        (StatusCode::OK, b"event: response.created\ndata: {\"type\":\"response.created\",\"response\":{}}\n\n".as_slice()),
        (StatusCode::OK, b"event: response.completed\ndata: {".as_slice()),
        (StatusCode::OK, b"event: response.completed\ndata: {\n\n".as_slice()),
        (StatusCode::OK, b"event: response.failed\ndata: {\"type\":\"response.failed\",\"response\":{\"status\":\"failed\",\"usage\":{\"input_tokens\":2048,\"input_tokens_details\":{\"cached_tokens\":1024}}}}\n\n".as_slice()),
        (StatusCode::OK, b"event: error\ndata: {\"error\":{\"message\":\"provider failed\"}}\n\n".as_slice()),
        (StatusCode::OK, JSON_HIT),
    ] {
        let (outcome, _) = dispatch_wire(UpstreamKind::OpenaiOauth, REQUEST, status, Bytes::copy_from_slice(body), false).await;
        assert!(matches!(outcome, DispatchOutcome::Error(_)), "failed or unfinished renewal must not finalize");
    }
}

#[tokio::test]
async fn responses_hydrated_modes() {
    for kind in [UpstreamKind::OpenaiOauth, UpstreamKind::OpenaiApiKey] {
        let oauth = kind == UpstreamKind::OpenaiOauth;
        let response = if oauth { SSE } else { JSON_HIT };
        let (outcome, requests) = dispatch_wire(
            kind,
            REQUEST,
            StatusCode::OK,
            Bytes::from_static(response),
            true,
        )
        .await;
        assert!(matches!(
            outcome,
            DispatchOutcome::CacheHit { .. } | DispatchOutcome::CacheMiss { .. }
        ));
        let body: Value = serde_json::from_slice(requests[0].body()).unwrap();
        let mut expected: Value = serde_json::from_slice(RENEWAL).unwrap();
        expected["stream"] = json!(oauth);
        assert_eq!(body, expected);
    }
    let (outcome, _) = dispatch_wire(
        UpstreamKind::OpenaiApiKey,
        REQUEST,
        StatusCode::OK,
        Bytes::from_static(SSE),
        true,
    )
    .await;
    assert!(matches!(outcome, DispatchOutcome::Error(_)));
}

#[tokio::test]
async fn codex_prefix_fidelity() {
    let request = br#"{"model":"gpt-6.1-sol","instructions":"keep","input":[{"z":1.00,"a":"opaque"}],"tools":[{"z":2,"a":1}],"previous_response_id":"resp_owner","prompt_cache_options":{"ttl":"30m","mode":"implicit"},"store":false,"stream":false,"max_output_tokens":128}"#;
    let expected = br#"{"model":"gpt-6.1-sol","instructions":"keep","input":[{"z":1.00,"a":"opaque"}],"tools":[{"z":2,"a":1}],"previous_response_id":"resp_owner","prompt_cache_options":{"ttl":"30m","mode":"implicit"},"store":false,"stream":true,"max_output_tokens":16}"#;
    let (_, requests) = dispatch_wire(
        UpstreamKind::OpenaiOauth,
        request,
        StatusCode::OK,
        Bytes::from_static(SSE),
        true,
    )
    .await;
    assert_eq!(requests[0].body().as_ref(), expected);
}
