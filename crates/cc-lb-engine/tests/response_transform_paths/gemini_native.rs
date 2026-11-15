use super::*;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

// The URL requests the client's transport; the existing engine routes by upstream content type.
const REQUEST_BODY: &[u8] = br#"{"contents":[{"role":"user","parts":[{"text":"hello"}]}]}"#;
const JSON_ARRAY: &[u8] = br#"[{"candidates":[{"content":{"parts":[{"text":"hello"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":3,"candidatesTokenCount":2}}]"#;
const JSON_OBJECT: &[u8] = br#"{"candidates":[{"content":{"parts":[{"text":"hello"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":3,"candidatesTokenCount":2}}"#;
const OPAQUE_SSE: &[u8] = concat!(
    ": native keepalive\r\n\r\n",
    "data: {\"candidates\":[{\"content\":{\"role\":\"model\",\"parts\":[{\"thought\":true,\"text\":\"reasoning\"},{\"text\":\"\",\"thoughtSignature\":\"empty-part-signature+/==\"},{\"functionCall\":{\"id\":\"call-2\",\"name\":\"SecondTool\",\"args\":{\"n\":2}},\"thoughtSignature\":\"opaque+/not-an-owner-token==\"},{\"functionCall\":{\"id\":\"call-1\",\"name\":\"FirstTool\",\"args\":{\"n\":1}}},{\"inlineData\":{\"mimeType\":\"image/png\",\"data\":\"AAEC\"}},{\"fileData\":{\"mimeType\":\"application/pdf\",\"fileUri\":\"gs://fixture/document.pdf\"}},{\"executableCode\":{\"language\":\"PYTHON\",\"code\":\"print(1)\"}},{\"codeExecutionResult\":{\"outcome\":\"OUTCOME_OK\",\"output\":\"1\\n\"}}]},\"groundingMetadata\":{\"groundingChunks\":[{\"web\":{\"uri\":\"https://example.com/source\",\"title\":\"Source\"}}],\"groundingSupports\":[{\"segment\":{\"startIndex\":0,\"endIndex\":1},\"groundingChunkIndices\":[0]}]}}]}\r\n\r\n",
    "data: {\"candidates\":[{\"index\":0,\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":3,\"candidatesTokenCount\":2,\"thoughtsTokenCount\":4,\"cachedContentTokenCount\":1,\"totalTokenCount\":9}}\n\n",
).as_bytes();

struct Observation {
    status: StatusCode,
    headers: HeaderMap,
    body: Bytes,
    row: cc_lb_storage_api::RequestEvent,
    events: Vec<LifecycleEvent>,
}

impl Observation {
    fn assert_terminal(&self, error_code: Option<&str>) {
        let terminal = self
            .events
            .iter()
            .filter_map(|event| match event {
                LifecycleEvent::RequestTerminated { reason, .. } => Some(reason),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(terminal.len(), 1, "exactly one terminal event");
        let expected = error_code.map_or(TerminationReason::Success, |code| {
            TerminationReason::ErrorCode(code.to_owned())
        });
        assert_eq!(terminal[0], &expected);
        assert_eq!(self.row.error_code.as_deref(), error_code);
        assert_eq!(self.row.status, self.status.as_u16());
    }

    fn assert_framing_error(&self) {
        self.assert_terminal(Some("upstream_stream_error"));
        assert_eq!(
            self.row.upstream_error_type.as_deref(),
            Some("upstream_response_framing_error"),
        );
        assert_eq!(
            self.status,
            StatusCode::OK,
            "stream failure is carried in SSE"
        );
        assert_eq!(
            self.headers
                .get(CONTENT_TYPE)
                .unwrap()
                .to_str()
                .unwrap()
                .split(';')
                .next(),
            Some("text/event-stream"),
        );
        let text = std::str::from_utf8(&self.body).expect("framing error body is UTF-8");
        let events = parsed_sse_events(&self.body);
        let errors = events.iter().filter(|event| event.event == "error").map(|event| {
            serde_json::from_slice::<serde_json::Value>(&event.data).unwrap_or_else(|error| {
                panic!(
                    "error event must contain one standalone JSON payload, not a partial upstream data event joined to the generated error: {error}; data={:?}",
                    std::str::from_utf8(&event.data),
                )
            })
        }).collect::<Vec<_>>();
        assert_eq!(
            errors.len(),
            1,
            "exactly one dispatched Gemini error event: {text}"
        );
        assert_eq!(errors[0]["error"]["code"], 502);
        assert_eq!(errors[0]["error"]["status"], "UNAVAILABLE");
        assert_eq!(errors[0]["error"]["type"], "api_error");
        assert!(
            !errors[0]["error"]["message"]
                .as_str()
                .expect("error message")
                .is_empty()
        );
        assert!(
            errors[0].get("type").is_none(),
            "not an Anthropic error envelope"
        );
    }
}

fn native_request(query: &str) -> http::Request<Bytes> {
    let mut request = http::Request::builder()
        .method("POST")
        .uri(format!("/v1beta/models/test:streamGenerateContent{query}"))
        .header("x-api-key", common::managed_api_key())
        .header(CONTENT_TYPE, "application/json")
        .body(Bytes::from_static(REQUEST_BODY))
        .expect("native Gemini request builds");
    request
        .extensions_mut()
        .insert(cc_lb_engine::ProviderScope(cc_lb_domain::Provider::Gemini));
    request
}

async fn observe(
    request: http::Request<Bytes>,
    headers: HeaderMap,
    body: Bytes,
    with_hook: bool,
) -> TestResult<Observation> {
    observe_upstream(
        request,
        headers,
        body,
        with_hook,
        StatusCode::OK,
        StorageUpstreamKind::GeminiApiKey,
        "https://generativelanguage.googleapis.com/",
    )
    .await
}

async fn observe_upstream(
    request: http::Request<Bytes>,
    headers: HeaderMap,
    body: Bytes,
    with_hook: bool,
    status: StatusCode,
    upstream_kind: StorageUpstreamKind,
    base_url: &str,
) -> TestResult<Observation> {
    let dir = tempfile::tempdir()?;
    let storage = Arc::new(sqlite_storage(&dir, "gemini-native.sqlite").await?);
    let test_bus =
        TestLifecycleBus::new().with_assembler(Arc::clone(&storage) as Arc<dyn StorageTrait>);
    let mut receiver = test_bus.bus.subscribe_lifecycle();
    let hook = with_hook
        .then(|| Arc::new(UnchangedSseTransform::default()) as Arc<dyn SseEventTransformHook>);
    let lifecycle = with_test_limit_subject(lifecycle_with_record_and_config(
        None,
        hook,
        Arc::new(FixedDispatch {
            status,
            headers,
            body,
        }),
        LifecycleConfig::default(),
        upstream_record(upstream_kind, base_url),
    ))
    .with_event_bus(test_bus.bus_arc());
    let response = handle_authenticated(&lifecycle, request).await?;
    let (status, headers, body) = collect_body(response).await;
    let events = tokio::time::timeout(Duration::from_secs(3), async {
        let mut events = Vec::new();
        loop {
            let event = receiver.recv().await.expect("lifecycle event delivered");
            let terminated = matches!(event, LifecycleEvent::RequestTerminated { .. });
            events.push(event);
            if terminated {
                break events;
            }
        }
    })
    .await
    .expect("native Gemini request terminates");
    let mut rows = wait_for_events(storage.as_ref(), 1).await?;
    assert_eq!(rows.len(), 1);
    Ok(Observation {
        status,
        headers,
        body,
        row: rows.remove(0),
        events,
    })
}

fn parsed_sse_events(body: &[u8]) -> Vec<SseEvent> {
    // SSE fields are order independent. Blank lines dispatch events, `event:`
    // selects their type, and all `data:` fields join with LF. In particular,
    // `data: STOP\nevent: error\ndata: ERROR\n\n` is ONE error event whose
    // payload is `STOP\nERROR`, not a standalone generated JSON error.
    let text = std::str::from_utf8(body).expect("SSE body is UTF-8");
    let normalized = text.replace("\r\n", "\n").replace('\r', "\n");
    let mut events = Vec::new();
    let mut event = String::new();
    let mut data = String::new();
    for line in normalized.split_terminator('\n') {
        if line.is_empty() {
            if !data.is_empty() {
                data.pop(); // SSE removes the final data-field LF at dispatch.
                events.push(SseEvent {
                    event: std::mem::take(&mut event),
                    data: Bytes::from(std::mem::take(&mut data)),
                });
            }
            event.clear();
        } else {
            let (field, value) = line.split_once(':').unwrap_or((line, ""));
            let value = value.strip_prefix(' ').unwrap_or(value);
            match field {
                "event" => event = value.to_owned(),
                "data" => {
                    data.push_str(value);
                    data.push('\n');
                }
                _ => {}
            }
        }
    }
    // EOF does not dispatch an event without a terminating blank line.
    events
}

#[test]
fn sse_error_payload_semantics_reject_synthetic_joined_partial_event() {
    let stop = r#"{"candidates":[{"finishReason":"STOP"}]}"#;
    let error = r#"{"error":{"code":502,"status":"UNAVAILABLE","type":"api_error","message":"synthetic framing failure"}}"#;
    let joined = format!("data: {stop}\nevent: error\ndata: {error}\n\n");
    let events = parsed_sse_events(joined.as_bytes());
    assert_eq!(events.len(), 1, "event field need not be the first line");
    assert_eq!(events[0].event, "error");
    assert_eq!(
        events[0].data.as_ref(),
        format!("{stop}\n{error}").as_bytes()
    );
    assert!(
        serde_json::from_slice::<serde_json::Value>(&events[0].data).is_err(),
        "synthetic negative control dispatches one error event with two joined JSON documents",
    );

    let standalone = format!("data: {stop}\r\n\r\ndata: {error}\r\nevent: error\r\n\r\n");
    let events = parsed_sse_events(standalone.as_bytes());
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].event, "");
    assert_eq!(events[1].event, "error");
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&events[1].data)
            .expect("standalone JSON error"),
        serde_json::from_str::<serde_json::Value>(error).expect("synthetic error JSON"),
    );
    let extra_blank_line = format!("data: {stop}\n\n\nevent: error\ndata: {error}\n\n");
    let events = parsed_sse_events(extra_blank_line.as_bytes());
    assert_eq!(
        events.len(),
        2,
        "extra blank lines do not merge adjacent events"
    );
    assert_eq!(events[0].event, "");
    assert_eq!(events[0].data.as_ref(), stop.as_bytes());
    assert_eq!(events[1].event, "error");
    assert_eq!(events[1].data.as_ref(), error.as_bytes());
    assert!(
        parsed_sse_events(format!("event: error\ndata: {error}\n").as_bytes()).is_empty(),
        "EOF without a blank line must not dispatch a client-visible error",
    );
}

#[tokio::test]
async fn native_alt_sse_json_success_is_framing_error_without_stream_field() -> TestResult {
    for with_hook in [false, true] {
        for fixture in [JSON_OBJECT, JSON_ARRAY] {
            let body = Bytes::from_static(fixture);
            let observation = observe(
                native_request("?alt=sse"),
                json_headers(body.len(), false),
                body.clone(),
                with_hook,
            )
            .await?;
            observation.assert_framing_error();
            assert_ne!(
                observation.body, body,
                "JSON success cannot masquerade as SSE"
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn native_json_array_alt_json_and_absent_alt_remain_success() -> TestResult {
    for query in ["?alt=json", ""] {
        for with_hook in [false, true] {
            let body = Bytes::from_static(JSON_ARRAY);
            let observation = observe(
                native_request(query),
                json_headers(body.len(), false),
                body.clone(),
                with_hook,
            )
            .await?;
            assert_eq!(observation.status, StatusCode::OK);
            assert_eq!(observation.body, body);
            assert_eq!(
                observation.headers.get(CONTENT_TYPE),
                Some(&HeaderValue::from_static("application/json"))
            );
            observation.assert_terminal(None);
        }
    }
    Ok(())
}

#[tokio::test]
async fn native_alt_sse_non_success_json_remains_upstream_error() -> TestResult {
    for (status, fixture) in [
        (StatusCode::BAD_REQUEST, br#"{"error":{"code":400,"status":"INVALID_ARGUMENT","message":"invalid input"}}"#.as_slice()),
        (StatusCode::TOO_MANY_REQUESTS, br#"{"error":{"code":429,"status":"RESOURCE_EXHAUSTED","message":"synthetic quota exhausted"}}"#.as_slice()),
    ] {
        for with_hook in [false, true] {
            for sse_content_type in [false, true] {
                let body = Bytes::from_static(fixture);
                let headers = if sse_content_type { sse_headers() } else { json_headers(body.len(), false) };
                let observation = observe_upstream(
                    native_request("?alt=sse"),
                    headers,
                    body.clone(),
                    with_hook,
                    status,
                    StorageUpstreamKind::GeminiApiKey,
                    "https://generativelanguage.googleapis.com/",
                ).await?;
                assert_eq!(observation.status, status);
                assert_eq!(observation.body, body, "non-success JSON is not a success-framing defect");
                assert_eq!(
                    observation.headers.get(CONTENT_TYPE),
                    Some(&HeaderValue::from_static(if sse_content_type { "text/event-stream" } else { "application/json" })),
                );
                observation.assert_terminal(Some("upstream_4xx"));
                assert_ne!(observation.row.upstream_error_type.as_deref(), Some("upstream_response_framing_error"));
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn oauth_alt_sse_json_success_still_uses_buffered_envelope_unwrap() -> TestResult {
    for with_hook in [false, true] {
        let body = Bytes::from_static(br#"{"response":{"candidates":[{"content":{"parts":[{"text":"hello"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":3,"candidatesTokenCount":2}},"traceId":"synthetic-trace"}"#);
        let observation = observe_upstream(
            native_request("?alt=sse"),
            json_headers(body.len(), false),
            body,
            with_hook,
            StatusCode::OK,
            StorageUpstreamKind::GeminiOauth,
            "https://cloudcode-pa.googleapis.com/v1internal",
        )
        .await?;
        assert_eq!(observation.status, StatusCode::OK);
        assert_eq!(
            observation.headers.get(CONTENT_TYPE),
            Some(&HeaderValue::from_static("application/json"))
        );
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&observation.body)?,
            serde_json::from_slice::<serde_json::Value>(JSON_OBJECT)?,
            "OAuth JSON success is unwrapped, not rejected as native framing",
        );
        observation.assert_terminal(None);
    }
    Ok(())
}

#[tokio::test]
async fn native_sse_preserves_opaque_parts_grounding_and_usage_bytes() -> TestResult {
    for with_hook in [false, true] {
        let body = Bytes::from_static(OPAQUE_SSE);
        let observation = observe(
            native_request("?alt=sse"),
            sse_headers(),
            body.clone(),
            with_hook,
        )
        .await?;
        assert_eq!(observation.status, StatusCode::OK);
        assert_eq!(
            observation.body, body,
            "native parts and SSE framing are opaque"
        );
        observation.assert_terminal(None);
        // Gemini prompt includes cached input; candidates exclude thought output.
        assert_eq!(observation.row.input_tokens, Some(2));
        assert_eq!(observation.row.cache_read_input_tokens, Some(1));
        assert_eq!(observation.row.output_tokens, Some(6));
        assert_eq!(observation.row.thinking_tokens, Some(4));
    }
    Ok(())
}

#[tokio::test]
async fn native_truncated_sse_emits_exactly_one_error_and_non_success_terminal() -> TestResult {
    for with_hook in [false, true] {
        let body = Bytes::from_static(b"data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"hello\"}]}}]}\n\ndata: {\"candidates\":[{\"finishReason\":\"STOP\"}]}\n");
        let observation =
            observe(native_request("?alt=sse"), sse_headers(), body, with_hook).await?;
        observation.assert_framing_error();
        let completions = observation
            .events
            .iter()
            .filter_map(|event| match event {
                LifecycleEvent::StreamCompleted { result, .. } => Some(result),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(completions.len(), 1, "one stream completion");
        assert_eq!(
            completions[0]
                .as_ref()
                .expect_err("truncated SSE fails")
                .error_type,
            "upstream_response_framing_error"
        );
    }
    Ok(())
}

#[tokio::test]
async fn native_api_key_does_not_unwrap_oauth_response_envelope() -> TestResult {
    for with_hook in [false, true] {
        for is_sse in [false, true] {
            let body = if is_sse {
                Bytes::from_static(b"data: {\"response\":{\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"hello\"}]},\"finishReason\":\"STOP\"}]},\"traceId\":\"native-envelope\"}\n\n")
            } else {
                Bytes::from_static(br#"{"response":{"candidates":[{"content":{"parts":[{"text":"hello"}]},"finishReason":"STOP"}]},"traceId":"native-envelope"}"#)
            };
            let headers = if is_sse {
                sse_headers()
            } else {
                json_headers(body.len(), false)
            };
            let observation = observe(
                native_request(if is_sse { "?alt=sse" } else { "?alt=json" }),
                headers,
                body.clone(),
                with_hook,
            )
            .await?;
            assert_eq!(observation.status, StatusCode::OK);
            assert_eq!(
                observation.body, body,
                "API-key traffic must not use OAuth envelope rewriting"
            );
            observation.assert_terminal(None);
        }
    }
    Ok(())
}

struct CapturingNativeDispatch {
    inner: FixedDispatch,
    signed_bodies: Mutex<Vec<Bytes>>,
}

#[async_trait]
impl UpstreamDispatch for CapturingNativeDispatch {
    async fn dispatch(&self, request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        self.signed_bodies
            .lock()
            .expect("signed body capture")
            .push(request.body().clone());
        self.inner.dispatch(request).await
    }
}

#[tokio::test]
async fn native_tool_continuation_preserves_signed_requests_and_opaque_signatures() -> TestResult {
    for with_hook in [false, true] {
        let dispatcher = Arc::new(CapturingNativeDispatch {
            inner: FixedDispatch {
                status: StatusCode::OK,
                headers: sse_headers(),
                body: Bytes::from_static(OPAQUE_SSE),
            },
            signed_bodies: Mutex::new(Vec::new()),
        });
        let hook = with_hook
            .then(|| Arc::new(UnchangedSseTransform::default()) as Arc<dyn SseEventTransformHook>);
        let lifecycle = with_test_limit_subject(lifecycle_with_record_and_config(
            None,
            hook,
            dispatcher.clone(),
            LifecycleConfig::default(),
            upstream_record(
                StorageUpstreamKind::GeminiApiKey,
                "https://generativelanguage.googleapis.com/",
            ),
        ));
        let response = handle_authenticated(&lifecycle, native_request("?alt=sse")).await?;
        let (status, _, received) = collect_body(response).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(received.as_ref(), OPAQUE_SSE);

        // Reconstruct a client continuation from the actual relayed model turn.
        // Tool results belong to a subsequent user turn, not the model response.
        let model_event = std::str::from_utf8(&received)?
            .lines()
            .find_map(|line| line.strip_prefix("data: "))
            .expect("native model event");
        let model_event: serde_json::Value = serde_json::from_str(model_event)?;
        let model_content = model_event["candidates"][0]["content"].clone();
        let parts = model_content["parts"].as_array().expect("model parts");
        assert_eq!(parts[1]["text"], "");
        assert_eq!(parts[1]["thoughtSignature"], "empty-part-signature+/==");
        assert_eq!(parts[2]["functionCall"]["id"], "call-2");
        assert_eq!(parts[2]["functionCall"]["name"], "SecondTool");
        assert_eq!(parts[2]["thoughtSignature"], "opaque+/not-an-owner-token==");
        assert_eq!(parts[3]["functionCall"]["id"], "call-1");
        assert_eq!(parts[3]["functionCall"]["name"], "FirstTool");
        assert!(
            parts
                .iter()
                .all(|part| part.get("functionResponse").is_none())
        );
        let continuation = serde_json::json!({
            "contents": [
                {"role": "user", "parts": [{"text": "hello"}]},
                model_content,
                {"role": "user", "parts": [
                    {"functionResponse": {
                        "id": "call-2", "name": "SecondTool", "response": {"ok": true}
                    }},
                    {"functionResponse": {
                        "id": "call-1", "name": "FirstTool", "response": {"ok": false}
                    }}
                ]}
            ]
        });
        let continuation_body = Bytes::from(serde_json::to_vec(&continuation)?);
        let mut request = native_request("?alt=sse");
        *request.body_mut() = continuation_body.clone();
        let response = handle_authenticated(&lifecycle, request).await?;
        let (status, _, received) = collect_body(response).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(received.as_ref(), OPAQUE_SSE);
        assert_eq!(
            *dispatcher
                .signed_bodies
                .lock()
                .expect("signed body capture"),
            vec![Bytes::from_static(REQUEST_BODY), continuation_body],
            "both requests retain exact client bytes through shaping and signing",
        );
    }
    Ok(())
}
