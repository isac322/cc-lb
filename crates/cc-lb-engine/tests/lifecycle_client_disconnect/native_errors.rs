use super::*;
use client_disconnect_support::{
    assert_responses_failed, assert_stream_failure_once, sse_json_events,
};
use serde_json::{Value, json};
use std::io::Write as _;

const OUTPUT: &[u8] = b"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":7,\"delta\":\"partial\"}\n\n";
const CREATED: &[u8] = b"event: response.created\ndata: {\"type\":\"response.created\",\"sequence_number\":0,\"response\":{\"id\":\"resp_proxy_test\",\"object\":\"response\",\"status\":\"in_progress\"}}\n\n";

fn responses_request() -> http::Request<Bytes> {
    native_request(cc_lb_domain::Provider::Openai, true)
}

fn provider_case(provider: cc_lb_domain::Provider) -> (StorageUpstreamKind, &'static str, Value) {
    match provider {
        cc_lb_domain::Provider::Anthropic => (
            StorageUpstreamKind::AnthropicApiKey,
            "/v1/messages",
            json!({"model":"claude-test","messages":[]}),
        ),
        cc_lb_domain::Provider::Openai => (
            StorageUpstreamKind::OpenaiApiKey,
            "/v1/responses",
            json!({"model":"gpt-test","input":[]}),
        ),
        cc_lb_domain::Provider::Gemini => (
            StorageUpstreamKind::GeminiApiKey,
            "/v1beta/models/gemini-test:generateContent",
            json!({"contents":[]}),
        ),
    }
}

fn native_request(provider: cc_lb_domain::Provider, stream: bool) -> http::Request<Bytes> {
    let (_, path, mut value) = provider_case(provider);
    if stream {
        value["stream"] = json!(true);
    }
    let mut request = http::Request::builder()
        .method("POST")
        .uri(path)
        .header("x-api-key", common::managed_api_key())
        .body(Bytes::from(value.to_string()))
        .expect("native request builds");
    request.extensions_mut().insert(ProviderScope(provider));
    request
}

fn assert_http_error(
    provider: cc_lb_domain::Provider,
    status: StatusCode,
    body: &[u8],
    message: &str,
) {
    let value: Value = serde_json::from_slice(body).expect("local HTTP error is JSON");
    assert_eq!(value["error"]["message"], message);
    match provider {
        cc_lb_domain::Provider::Anthropic => {
            assert_eq!(value["type"], "error");
            assert_eq!(value["error"]["type"], "api_error");
        }
        cc_lb_domain::Provider::Openai => {
            assert!(
                value.get("type").is_none(),
                "no Anthropic outer wrapper: {value}"
            );
            assert_eq!(value["error"]["type"], "api_error");
            assert!(
                value["error"].get("param").is_some(),
                "OpenAI param key: {value}"
            );
            assert!(
                value["error"].get("code").is_some(),
                "OpenAI code key: {value}"
            );
        }
        cc_lb_domain::Provider::Gemini => {
            assert_eq!(value["error"]["code"], status.as_u16());
            assert_eq!(
                value["error"]["status"],
                if status == StatusCode::INTERNAL_SERVER_ERROR {
                    "INTERNAL"
                } else {
                    "UNAVAILABLE"
                }
            );
        }
    }
}

#[tokio::test]
async fn responses_proxy_failures_preserve_native_terminal_schema_and_accounting()
-> Result<(), Box<dyn std::error::Error>> {
    for fault in [
        "disconnect",
        "absent_id",
        "framing",
        "inspection",
        "decode",
        "transform",
    ] {
        let dir = tempfile::tempdir()?;
        let storage = Arc::new(sqlite_storage(&dir).await?);
        let bus = TestLifecycleBus::new().with_assembler(storage.clone() as Arc<dyn StorageTrait>);
        let mut events = lifecycle_receiver(&bus);
        let mut updates = bus.bus.subscribe();
        let known_id = !matches!(fault, "absent_id" | "transform");
        let mut prefix = Vec::new();
        if known_id {
            prefix.extend_from_slice(CREATED);
        }
        prefix.extend_from_slice(OUTPUT);
        let expected_type;
        let dispatch = match fault {
            "disconnect" | "absent_id" => {
                expected_type = "upstream_response_body_error";
                sse_dispatch(
                    StatusCode::OK,
                    upstream_frame_error_after(Bytes::from(prefix.clone())),
                )
            }
            "framing" => {
                expected_type = "upstream_response_framing_error";
                let mut body = prefix.clone();
                body.extend_from_slice(b"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\"");
                sse_dispatch(StatusCode::OK, Body::from(body))
            }
            "inspection" => {
                expected_type = "upstream_response_framing_error";
                let mut body = prefix.clone();
                body.extend_from_slice(
                    b"event: response.output_text.delta\ndata: {invalid-json}\n\n",
                );
                sse_dispatch(StatusCode::OK, Body::from(body))
            }
            "decode" => {
                expected_type = "upstream_response_decode_error";
                let mut encoder =
                    flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
                encoder.write_all(&prefix)?;
                let compressed = encoder.finish()?;
                encoded_sse_dispatch(
                    StatusCode::OK,
                    Body::from(compressed[..compressed.len() - 8].to_vec()),
                    "gzip",
                )
            }
            "transform" => {
                expected_type = "response_transform_error";
                let mut body = prefix.clone();
                body.extend_from_slice(b"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":8,\"delta\":\"not delivered\"}\n\n");
                sse_dispatch(StatusCode::OK, Body::from(body))
            }
            _ => unreachable!(),
        };
        let lifecycle = if fault == "transform" {
            transform_lifecycle_with_kind(dispatch, &bus, StorageUpstreamKind::OpenaiApiKey)
        } else {
            lifecycle_with_upstream_kind(dispatch, &bus, StorageUpstreamKind::OpenaiApiKey)
        };
        // Real response.created ids must bind before relay. SQLite here only
        // persists request events; its upstream FK fixture is not populated.
        let lifecycle = if known_id {
            lifecycle.with_upstream_affinity_store(Arc::new(
                crate::upstream_affinity::MemoryAffinityStore::default(),
            ))
        } else {
            lifecycle
        };
        let request = responses_request();
        let auth = lifecycle.authenticate(request.headers()).await?;
        let response = lifecycle.handle(request, &auth).await?;
        assert_eq!(response.status(), StatusCode::OK, "{fault}");
        let body = response.into_body().collect().await?.to_bytes();
        assert!(
            body.starts_with(&prefix),
            "normal output precedes {fault}: {body:?}"
        );
        let failure = assert_responses_failed(
            &body,
            known_id.then_some("resp_proxy_test"),
            if fault == "transform" { 8 } else { 7 },
        );
        let message = failure["response"]["error"]["message"]
            .as_str()
            .expect("native message");
        assert!(!message.contains("not delivered"));
        assert_stream_failure_once(&mut events, expected_type, 200, "upstream_stream_error").await;
        assert_one_final(&mut updates).await;
        let rows = common::stored_request_events(storage.as_ref()).await?;
        assert_eq!(rows.len(), 1, "one persisted row for {fault}");
        assert_eq!(rows[0].status, 200);
        assert_eq!(rows[0].error_code.as_deref(), Some("upstream_stream_error"));
        assert_eq!(rows[0].upstream_error_type.as_deref(), Some(expected_type));
    }
    Ok(())
}

#[tokio::test]
async fn responses_provider_failed_and_error_events_pass_through_byte_identically()
-> Result<(), Box<dyn std::error::Error>> {
    for terminal in [
        b"event: response.failed\ndata: {\"type\":\"response.failed\",\"sequence_number\":8,\"response\":{\"id\":\"resp_native\",\"object\":\"response\",\"status\":\"failed\",\"error\":{\"code\":\"server_error\",\"message\":\"native provider failure\"}}}\n\n".as_slice(),
        b"event: error\ndata: {\"type\":\"error\",\"sequence_number\":8,\"code\":\"server_error\",\"message\":\"native provider failure\"}\n\n".as_slice(),
    ] {
        let bus = TestLifecycleBus::new();
        let mut events = lifecycle_receiver(&bus);
        let mut raw = OUTPUT.to_vec();
        raw.extend_from_slice(terminal);
        // response.failed may still contain provider-owned response state.
        // Permit that legitimate bind instead of testing an unrelated FK error.
        let lifecycle = lifecycle_with_upstream_kind(sse_dispatch(StatusCode::OK, Body::from(raw.clone())), &bus, StorageUpstreamKind::OpenaiApiKey)
            .with_upstream_affinity_store(Arc::new(
                crate::upstream_affinity::MemoryAffinityStore::default(),
            ));
        let request = responses_request();
        let auth = lifecycle.authenticate(request.headers()).await.expect("request authenticates");
        let response = lifecycle.handle(request, &auth).await.expect("native failure relays");
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.expect("body collects").to_bytes();
        assert_eq!(body.as_ref(), raw.as_slice());
        assert_eq!(sse_json_events(&body).len(), 2);
        assert_error_terminal(&mut events, 200, "upstream_stream_error").await;
    }
    Ok(())
}

#[tokio::test]
async fn chat_completions_disconnect_keeps_generic_error_contract() {
    let first = b"data: {\"id\":\"chatcmpl-test\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"delta\":{\"content\":\"partial\"},\"finish_reason\":null}]}\n\n";
    let bus = TestLifecycleBus::new();
    let mut events = lifecycle_receiver(&bus);
    let lifecycle = lifecycle_with_upstream_kind(
        sse_dispatch(
            StatusCode::OK,
            upstream_frame_error_after(Bytes::from_static(first)),
        ),
        &bus,
        StorageUpstreamKind::OpenaiApiKey,
    );
    let mut request = http::Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header("x-api-key", common::managed_api_key())
        .body(Bytes::from_static(
            br#"{"model":"gpt-test","messages":[],"stream":true}"#,
        ))
        .expect("chat request");
    request
        .extensions_mut()
        .insert(ProviderScope(cc_lb_domain::Provider::Openai));
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("request authenticates");
    let response = lifecycle
        .handle(request, &auth)
        .await
        .expect("chat stream relays");
    assert_eq!(response.status(), StatusCode::OK);
    let body = response
        .into_body()
        .collect()
        .await
        .expect("body collects")
        .to_bytes();
    assert!(body.starts_with(first));
    let parsed = sse_json_events(&body);
    assert_eq!(parsed.len(), 2);
    assert_eq!(parsed[1].0, "error");
    assert_eq!(parsed[1].1["error"]["type"], "api_error");
    assert!(parsed.iter().all(|(name, _)| name != "response.failed"));
    assert_stream_failure_once(
        &mut events,
        "upstream_response_body_error",
        200,
        "upstream_stream_error",
    )
    .await;
}

#[tokio::test]
async fn principal_missing_after_authentication_uses_native_http_errors()
-> Result<(), Box<dyn std::error::Error>> {
    for provider in [
        cc_lb_domain::Provider::Anthropic,
        cc_lb_domain::Provider::Openai,
        cc_lb_domain::Provider::Gemini,
    ] {
        let dir = tempfile::tempdir()?;
        let storage = Arc::new(sqlite_storage(&dir).await?);
        let bus = TestLifecycleBus::new().with_assembler(storage.clone() as Arc<dyn StorageTrait>);
        let mut updates = bus.bus.subscribe();
        let mut events = lifecycle_receiver(&bus);
        let (kind, _, _) = provider_case(provider);
        let lifecycle = lifecycle_with_upstream_kind(
            json_dispatch(StatusCode::OK, Bytes::from_static(b"{}")),
            &bus,
            kind,
        );
        let request = native_request(provider, false);
        let auth = lifecycle
            .authenticate(request.headers())
            .await
            .expect("authenticate before principal removal");
        let authn = TestAuthn::new(TestState::default());
        lifecycle.dynamic_view().store(
            DynamicViewBuilder::new(1)
                .signer_factory(Arc::new(authn))
                .principal_view(Arc::new(PrincipalView::from_db(&[], HashMap::new())))
                .build(),
        );
        let response = lifecycle
            .handle(request, &auth)
            .await
            .expect("principal missing handled");
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body collects")
            .to_bytes();
        assert_http_error(
            provider,
            StatusCode::INTERNAL_SERVER_ERROR,
            &body,
            "authenticated principal is unavailable",
        );
        assert_error_terminal(&mut events, 500, "principal_missing").await;
        assert_one_final(&mut updates).await;
        let rows = common::stored_request_events(storage.as_ref()).await?;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].status, 500);
        assert_eq!(rows[0].error_code.as_deref(), Some("principal_missing"));
    }
    Ok(())
}

#[tokio::test]
async fn buffered_truncated_body_and_collected_unreadable_401_use_native_502()
-> Result<(), Box<dyn std::error::Error>> {
    for provider in [
        cc_lb_domain::Provider::Anthropic,
        cc_lb_domain::Provider::Openai,
        cc_lb_domain::Provider::Gemini,
    ] {
        for status in [StatusCode::OK, StatusCode::UNAUTHORIZED] {
            let dir = tempfile::tempdir()?;
            let storage = Arc::new(sqlite_storage(&dir).await?);
            let bus =
                TestLifecycleBus::new().with_assembler(storage.clone() as Arc<dyn StorageTrait>);
            let mut updates = bus.bus.subscribe();
            let mut events = lifecycle_receiver(&bus);
            let (kind, _, _) = provider_case(provider);
            let partial = Bytes::from_static(b"{\"secret_partial_upstream_body\":");
            let dispatch = client_disconnect_support::json_body_dispatch(
                status,
                upstream_frame_error_after(partial),
            );
            let lifecycle = client_disconnect_support::lifecycle_with_upstream_kind_and_refresh(
                dispatch, &bus, kind, false,
            );
            let request = native_request(provider, false);
            let auth = lifecycle
                .authenticate(request.headers())
                .await
                .expect("request authenticates");
            let response = lifecycle
                .handle(request, &auth)
                .await
                .expect("unreadable upstream body handled");
            assert_eq!(
                response.status(),
                StatusCode::BAD_GATEWAY,
                "{provider:?} {status}"
            );
            let body = response
                .into_body()
                .collect()
                .await
                .expect("body collects")
                .to_bytes();
            assert_http_error(
                provider,
                StatusCode::BAD_GATEWAY,
                &body,
                "upstream response body could not be read",
            );
            assert!(!String::from_utf8_lossy(&body).contains("secret_partial_upstream_body"));
            if status == StatusCode::OK {
                assert_stream_failure_once(
                    &mut events,
                    "upstream_response_body_error",
                    502,
                    "upstream_stream_error",
                )
                .await;
            } else {
                assert_error_terminal(&mut events, 502, "upstream_5xx").await;
            }
            assert_one_final(&mut updates).await;
            let rows = common::stored_request_events(storage.as_ref()).await?;
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].status, 502);
            assert_eq!(
                rows[0].error_code.as_deref(),
                Some(if status == StatusCode::OK {
                    "upstream_stream_error"
                } else {
                    "upstream_5xx"
                }),
            );
        }
    }
    Ok(())
}
