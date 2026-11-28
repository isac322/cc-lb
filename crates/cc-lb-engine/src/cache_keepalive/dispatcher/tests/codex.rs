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

#[tokio::test]
async fn openai_oauth_resolve_keepalive_url_uses_codex_base_and_preserves_path_prefix() {
    let legacy_snapshot = RequestSnapshot::capture_openai(
        "https://api.openai.com/v1/responses".parse().unwrap(),
        Method::POST,
        HeaderMap::new(),
        Bytes::copy_from_slice(REQUEST),
        Uuid::from_u128(1),
        cc_lb_storage_api::CacheTtl::Ttl30m,
        usize::MAX,
    )
    .unwrap();

    // Default OpenAI OAuth base_url (None) resolves to the Codex backend URL, never /v1/responses.
    let mut oauth_default = UpstreamRecord {
        id: Uuid::from_u128(1),
        kind: UpstreamKind::OpenaiOauth,
        base_url: None,
        enabled: true,
        ..UpstreamRecord::default()
    };
    assert_eq!(
        resolve_keepalive_url(&legacy_snapshot, &oauth_default)
            .unwrap()
            .as_str(),
        "https://chatgpt.com/backend-api/codex/responses"
    );

    // Custom base URL with /backend-api/codex prefix preserves prefix and strips /v1/.
    oauth_default.base_url = Some("http://127.0.0.1:4100/backend-api/codex".parse().unwrap());
    assert_eq!(
        resolve_keepalive_url(&legacy_snapshot, &oauth_default)
            .unwrap()
            .as_str(),
        "http://127.0.0.1:4100/backend-api/codex/responses"
    );

    // Mock base URL rooted at "/" resolves to "/responses".
    oauth_default.base_url = Some("http://127.0.0.1:4100/".parse().unwrap());
    assert_eq!(
        resolve_keepalive_url(&legacy_snapshot, &oauth_default)
            .unwrap()
            .as_str(),
        "http://127.0.0.1:4100/responses"
    );

    // API key upstream retains /v1/responses.
    let api_key_upstream = UpstreamRecord {
        id: Uuid::from_u128(1),
        kind: UpstreamKind::OpenaiApiKey,
        base_url: Some("http://127.0.0.1:4100/".parse().unwrap()),
        enabled: true,
        ..UpstreamRecord::default()
    };
    assert_eq!(
        resolve_keepalive_url(&legacy_snapshot, &api_key_upstream)
            .unwrap()
            .as_str(),
        "http://127.0.0.1:4100/v1/responses"
    );
}

#[tokio::test]
async fn codex_compressed_responses_decode_before_classification_and_errors() {
    use std::io::Write as _;

    struct EncodedDispatch {
        status: StatusCode,
        encoding: &'static str,
        body: Bytes,
    }

    #[async_trait::async_trait]
    impl UpstreamDispatch for EncodedDispatch {
        async fn dispatch(
            &self,
            _request: cc_lb_upstream::SignedRequest,
        ) -> Result<Response<crate::lifecycle::Body>, crate::lifecycle::DispatchError> {
            let mut response = Response::new(crate::lifecycle::Body::from(self.body.clone()));
            *response.status_mut() = self.status;
            response.headers_mut().insert(
                http::header::CONTENT_ENCODING,
                http::HeaderValue::from_static(self.encoding),
            );
            Ok(response)
        }
    }

    let sse_hit = terminal_wire(
        "response.completed",
        json!({
            "input_tokens": 2048,
            "output_tokens": 5,
            "input_tokens_details": {"cached_tokens": 1024}
        }),
    );

    let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    gz.write_all(&sse_hit).unwrap();
    let gzip_body = Bytes::from(gz.finish().unwrap());

    let mut br_body = Vec::new();
    {
        let mut writer = brotli::CompressorWriter::new(&mut br_body, 4096, 4, 22);
        writer.write_all(&sse_hit).unwrap();
    }

    let zstd_body = Bytes::from(zstd::stream::encode_all(sse_hit.as_ref(), 0).unwrap());

    let upstream = UpstreamRecord {
        id: Uuid::from_u128(1299),
        name: "codex-compressed".to_owned(),
        kind: UpstreamKind::OpenaiOauth,
        enabled: true,
        ..UpstreamRecord::default()
    };
    let snapshot = RequestSnapshot::capture_openai(
        "https://chatgpt.com/backend-api/codex/responses"
            .parse()
            .unwrap(),
        Method::POST,
        HeaderMap::new(),
        Bytes::copy_from_slice(REQUEST),
        upstream.id,
        cc_lb_storage_api::CacheTtl::Ttl30m,
        usize::MAX,
    )
    .unwrap();

    let build_dispatcher = |status, encoding, body| {
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
            .build();
        AnthropicKeepaliveDispatcher::new(
            Arc::new(DynamicViewHolder::new(view)),
            Arc::new(TestUpstreamStore::new(upstream.clone())),
            Arc::new(EncodedDispatch {
                status,
                encoding,
                body,
            }),
            Arc::new(cc_lb_clock::SystemClock),
        )
    };

    for (encoding, encoded) in [
        ("gzip", gzip_body.clone()),
        ("br", Bytes::from(br_body)),
        ("zstd", zstd_body),
    ] {
        let dispatcher = build_dispatcher(StatusCode::OK, encoding, encoded);
        let outcome = dispatcher
            .dispatch(
                &snapshot,
                KeepaliveDispatchContext::new(None, format!("enc-{encoding}"), u64::MAX),
            )
            .await;
        let DispatchOutcome::CacheHit { finalization, .. } = outcome else {
            panic!("compressed 200 ({encoding}) must decompress and classify as CacheHit");
        };
        assert_eq!(finalization.usage.cache_read_input_tokens, 1024);
    }

    // Corrupt compressed 200 must surface a clear decompression error rather than JSON parse garbage.
    let corrupt_dispatcher =
        build_dispatcher(StatusCode::OK, "gzip", Bytes::from_static(b"not-gzip"));
    let corrupt_outcome = corrupt_dispatcher
        .dispatch(
            &snapshot,
            KeepaliveDispatchContext::new(None, "corrupt-gzip".to_owned(), u64::MAX),
        )
        .await;
    assert!(
        matches!(corrupt_outcome, DispatchOutcome::Error(ref msg) if msg.contains("decompression failed")),
        "expected decompression error"
    );

    // Compressed non-200 error body keeps allowlisted codes and strips message/body text.
    let mut err_gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    err_gz
        .write_all(
            br#"{"error":{"type":"server_error","code":"bad_gateway","message":"rejected input: SYNTHETIC_PROMPT_SECRET_42"}}"#,
        )
        .unwrap();
    let err_dispatcher = build_dispatcher(
        StatusCode::BAD_GATEWAY,
        "gzip",
        Bytes::from(err_gz.finish().unwrap()),
    );
    let err_outcome = err_dispatcher
        .dispatch(
            &snapshot,
            KeepaliveDispatchContext::new(None, "err-gzip".to_owned(), u64::MAX),
        )
        .await;
    let DispatchOutcome::Error(msg) = err_outcome else {
        panic!("expected non-200 error outcome");
    };
    assert!(
        msg.contains("status=502")
            && msg.contains("type=server_error")
            && msg.contains("code=bad_gateway")
            && !msg.contains("SYNTHETIC_PROMPT_SECRET_42")
            && !msg.contains("rejected input"),
        "expected status and allowlisted error codes without body text: {msg}"
    );
}

#[tokio::test]
async fn codex_oauth_renewal_preserves_sent_priority_tier_when_response_reports_default() {
    let mut priority_req: Value = serde_json::from_slice(REQUEST).unwrap();
    priority_req["service_tier"] = json!("priority");
    let priority_bytes = serde_json::to_vec(&priority_req).unwrap();
    let (outcome, requests) = dispatch_wire(
        UpstreamKind::OpenaiOauth,
        &priority_bytes,
        StatusCode::OK,
        Bytes::from_static(SSE),
        false,
    )
    .await;
    let [sent_req] = requests.as_slice() else {
        panic!("renewal request missing")
    };
    let sent_json: Value = serde_json::from_slice(sent_req.body().as_ref()).unwrap();
    assert_eq!(sent_json["service_tier"], "priority");
    let DispatchOutcome::CacheMiss { finalization } = outcome else {
        panic!("captured zero-cache SSE must finalize a measured miss");
    };
    assert_eq!(finalization.usage.service_tier.as_deref(), Some("priority"));
}
