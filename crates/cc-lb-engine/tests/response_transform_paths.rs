use crate::common;

use async_trait::async_trait;
use axum::body::Body;
use bytes::Bytes;
use cc_lb_control::RequestEventBus;
use cc_lb_control::api_keys::concurrent_guard::KeyConcurrencyManager;
use cc_lb_control::api_keys::limit_engine::LimitEngine;
use cc_lb_control::api_keys::principal_view::{
    DialectCache, PrincipalRoutingArtifacts, PrincipalView, RouterPipelineCache, ShapePluginCache,
};
use cc_lb_control::{DynamicViewBuilder, DynamicViewHolder};
use cc_lb_domain::{Principal, PrincipalKind, TerminalStrategy, Upstream};
use cc_lb_engine::cache_keepalive::{
    CacheKeepaliveCancelRequest, CacheKeepaliveEnqueueError, CacheKeepaliveEnqueueRequest,
    CacheKeepaliveEnqueuer, CacheKeepaliveNotTrackedRequest, CancelReason,
};
use cc_lb_engine::{DispatchError, Lifecycle, LifecycleConfig, ProxyError, UpstreamDispatch};
use cc_lb_lifecycle::{LifecycleEvent, TerminationReason};
use cc_lb_storage_api::principal::PrincipalRecord;
use cc_lb_storage_api::upstream::{UpstreamKind as StorageUpstreamKind, UpstreamRecord};
use cc_lb_storage_api::{CacheKeepaliveConfig, ClassifierConfig};
use cc_lb_storage_api::{KeyStatus, StoredApiKeyRecord};
use cc_lb_storage_api::{MetaStore, RequestEventStore, Storage as StorageTrait};
use cc_lb_storage_sqlite::SqliteStorage;
use cc_lb_upstream::{
    DialectError, DialectShapeContext, ResponseTransformError, ResponseTransformHook,
    ShapedRequest, ShapedRequestBuilder, SignedRequest, SseEvent, SseEventTransformHook,
    TransformResponseRequest, TransformResponseResult, TransformSseEventRequest,
    TransformSseEventResult, UpstreamDialect,
};
use http::header::{CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_TYPE};
use http::{HeaderMap, HeaderValue, Response, StatusCode};
use http_body_util::BodyExt as _;
use std::collections::HashMap;
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use url::Url;
use uuid::Uuid;

use common::{TestAuthn, TestLifecycleBus, TestState, collect_body, messages_request};

#[path = "response_transform_paths/semantic_sse_eof.rs"]
mod semantic_sse_eof;

async fn buffered_transform_rewrites_tool_name_and_sanitizes_headers() {
    let transform = Arc::new(BufferedToolNameTransform::default());
    let lifecycle = lifecycle_with_transforms(Some(transform.clone()), None, buffered_dispatch());

    let request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[],"stream":false}"#,
    ));
    let response = handle_authenticated(&lifecycle, request)
        .await
        .expect("lifecycle handles buffered response");
    let (_status, headers, body) = collect_body(response).await;

    let body_text = std::str::from_utf8(&body).expect("body is utf8");
    assert!(body_text.contains(r#""name":"bash""#));
    assert!(!body_text.contains(r#""name":"Bash""#));
    assert_eq!(
        headers
            .get(CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .map(ToOwned::to_owned),
        Some(body.len().to_string())
    );
    assert!(headers.get(CONTENT_ENCODING).is_none());
    assert!(headers.get("x-cc-lb-spoof").is_none());
    assert!(headers.get("x-cc-lb-secret").is_none());
    assert!(headers.get("x-ratelimit-limit").is_none());
    assert!(headers.get("authorization").is_none());
    assert!(headers.get("x-api-key").is_none());
    assert!(headers.get("anthropic-ratelimit-requests-limit").is_none());
    assert_eq!(
        headers.get(CONTENT_TYPE),
        Some(&HeaderValue::from_static("application/json"))
    );
    assert_eq!(transform.seen_bodies(), vec![buffered_upstream_body()]);
}
async fn buffered_transform_preserves_only_original_upstream_unified_headers() {
    for (upstream_has_unified, upstream_status) in [
        (true, StatusCode::OK),
        (false, StatusCode::OK),
        (true, StatusCode::TOO_MANY_REQUESTS),
    ] {
        for replacement in [
            None,
            Some(HeaderMap::new()),
            Some(spoofed_plugin_headers(1)),
        ] {
            let upstream_body = buffered_upstream_body();
            let mut headers = json_headers(upstream_body.len(), false);
            add_rate_limit_header_fixture(&mut headers, upstream_has_unified);
            let mut replacement = replacement;
            if let Some(headers) = replacement.as_mut() {
                headers.insert("x-plugin-header", HeaderValue::from_static("kept"));
            }
            let replaces_headers = replacement.is_some();
            let transform = Arc::new(BufferedHeaderPolicyTransform {
                headers: replacement,
                seen_headers: Mutex::new(Vec::new()),
            });
            let lifecycle = lifecycle_with_transforms(
                Some(transform.clone()),
                None,
                Arc::new(FixedDispatch {
                    status: upstream_status,
                    headers,
                    body: upstream_body.clone(),
                }),
            );
            let response = handle_authenticated(
                &lifecycle,
                messages_request(Bytes::from_static(
                    br#"{"model":"claude-test","messages":[],"stream":false}"#,
                )),
            )
            .await
            .expect("lifecycle handles buffered replacement");
            let (status, headers, body) = collect_body(response).await;

            assert_eq!(status, upstream_status);
            assert_eq!(
                body,
                replace_bytes(&upstream_body, br#""name":"Bash""#, br#""name":"bash""#)
            );
            assert_eq!(
                headers.get(CONTENT_LENGTH).expect("replacement length"),
                &HeaderValue::from_str(&body.len().to_string()).expect("body length header")
            );
            assert!(headers.get(CONTENT_ENCODING).is_none());
            assert_client_rate_limit_headers(&headers, upstream_has_unified);
            if replaces_headers {
                assert_eq!(
                    headers.get("x-plugin-header"),
                    Some(&HeaderValue::from_static("kept"))
                );
                assert!(headers.get("x-safe-header").is_none());
            } else {
                assert_eq!(
                    headers.get("x-safe-header"),
                    Some(&HeaderValue::from_static("keep-me"))
                );
            }
            let seen = transform.seen_headers.lock().expect("seen headers lock");
            assert_eq!(seen.len(), 1);
            assert_plugin_headers_redacted(&seen[0]);
        }
    }
}

async fn compressed_sse_affinity_preserves_original_upstream_unified_headers() {
    let plaintext = complete_message_sse_body();
    for (encoding, body) in [
        ("gzip", gzip_bytes(&plaintext)),
        ("br", brotli_bytes(&plaintext)),
    ] {
        let mut headers = sse_headers();
        headers.insert(CONTENT_ENCODING, HeaderValue::from_static(encoding));
        headers.insert(
            CONTENT_LENGTH,
            HeaderValue::from_str(&body.len().to_string()).expect("content length"),
        );
        add_rate_limit_header_fixture(&mut headers, true);
        let lifecycle = lifecycle_with_transforms(
            None,
            None,
            Arc::new(FixedDispatch {
                status: StatusCode::OK,
                headers,
                body,
            }),
        );
        let request = messages_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[],"stream":true,"tools":[{"type":"web_search_20250305","name":"web_search"}]}"#,
        ));
        let response = handle_authenticated(&lifecycle, request)
            .await
            .expect("lifecycle handles compressed SSE affinity");
        let (status, headers, body) = collect_body(response).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, plaintext);
        assert!(headers.get(CONTENT_ENCODING).is_none());
        assert!(headers.get(CONTENT_LENGTH).is_none());
        assert_client_rate_limit_headers(&headers, true);
    }
}

async fn sse_transform_preserves_upstream_unified_headers_without_exposing_them_to_plugin() {
    for compressed in [false, true] {
        for upstream_status in [StatusCode::OK, StatusCode::TOO_MANY_REQUESTS] {
            for upstream_has_unified in [true, false] {
                let plaintext = sse_upstream_body(false);
                let body = if compressed {
                    gzip_bytes(&plaintext)
                } else {
                    plaintext.clone()
                };
                let mut headers = sse_headers();
                headers.insert(
                    CONTENT_LENGTH,
                    HeaderValue::from_str(&body.len().to_string()).expect("content length"),
                );
                if compressed {
                    headers.insert(CONTENT_ENCODING, HeaderValue::from_static("gzip"));
                }
                add_rate_limit_header_fixture(&mut headers, upstream_has_unified);
                let transform = Arc::new(HeaderCapturingSseTransform::default());
                let lifecycle = lifecycle_with_transforms(
                    None,
                    Some(transform.clone()),
                    Arc::new(FixedDispatch {
                        status: upstream_status,
                        headers,
                        body,
                    }),
                );
                let response = handle_authenticated(
                    &lifecycle,
                    messages_request(Bytes::from_static(
                        br#"{"model":"claude-test","messages":[],"stream":true}"#,
                    )),
                )
                .await
                .expect("lifecycle handles transformed SSE");
                let (status, headers, body) = collect_body(response).await;

                assert_eq!(status, upstream_status);
                assert_eq!(
                    body,
                    replace_bytes(&plaintext, br#""name":"Bash""#, br#""name":"bash""#)
                );
                assert!(headers.get(CONTENT_ENCODING).is_none());
                assert!(headers.get(CONTENT_LENGTH).is_none());
                assert_client_rate_limit_headers(&headers, upstream_has_unified);
                let seen = transform.seen_headers();
                assert_eq!(seen.len(), 2);
                for headers in seen {
                    assert_plugin_headers_redacted(&headers);
                }
            }
        }
    }
}

async fn buffered_gemini_oauth_code_assist_response_is_unwrapped() {
    let wrapped = Bytes::from_static(
        br#"{"response":{"candidates":[{"content":{"parts":[{"text":"hello"}]}}],"usageMetadata":{"promptTokenCount":120,"candidatesTokenCount":48}},"traceId":"trace"}"#,
    );
    let expected: serde_json::Value = serde_json::from_slice(
        br#"{"candidates":[{"content":{"parts":[{"text":"hello"}]}}],"usageMetadata":{"promptTokenCount":120,"candidatesTokenCount":48}}"#,
    )
    .expect("expected response JSON");
    let lifecycle = lifecycle_with_record(
        None,
        Arc::new(FixedDispatch {
            status: StatusCode::OK,
            headers: json_headers(wrapped.len(), false),
            body: wrapped,
        }),
        upstream_record(
            StorageUpstreamKind::GeminiOauth,
            "https://cloudcode-pa.googleapis.com/v1internal",
        ),
    );

    let request = gemini_generate_request();
    let response = handle_authenticated(&lifecycle, request)
        .await
        .expect("lifecycle handles Gemini Code Assist response");
    let (_status, _headers, body) = collect_body(response).await;

    let received: serde_json::Value =
        serde_json::from_slice(&body).expect("received response JSON");
    assert_eq!(received, expected);
}

async fn gemini_oauth_sse_unwraps_before_custom_transform() {
    let transform = Arc::new(GeminiSseMarkerTransform::default());
    let body = Bytes::from_static(
        b"event: response\ndata: {\"response\":{\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"hello\"}]}}]}}\n\n",
    );
    let lifecycle = lifecycle_with_record_and_config(
        None,
        Some(transform.clone()),
        Arc::new(FixedDispatch {
            status: StatusCode::OK,
            headers: sse_headers(),
            body,
        }),
        LifecycleConfig::default(),
        upstream_record(
            StorageUpstreamKind::GeminiOauth,
            "https://cloudcode-pa.googleapis.com/v1internal",
        ),
    );
    let response = handle_authenticated(&lifecycle, gemini_generate_request())
        .await
        .expect("lifecycle handles Gemini OAuth SSE response");
    let (_status, _headers, received) = collect_body(response).await;
    let text = std::str::from_utf8(&received).expect("SSE body is utf8");
    assert!(text.contains(r#""candidates":[{"content":{"parts":[{"text":"HELLO"}]}}]"#));
    assert!(!text.contains(r#""response":{"candidates""#));
    assert_eq!(transform.seen_events(), vec!["response".to_owned()]);
}

async fn buffered_native_gemini_response_bytes_remain_unchanged() {
    let body = Bytes::from_static(
        br#"{"candidates":[{"content":{"parts":[{"text":"hello"}]}}],"usageMetadata":{"promptTokenCount":120,"candidatesTokenCount":48}}"#,
    );
    let lifecycle = lifecycle_with_record(
        None,
        Arc::new(FixedDispatch {
            status: StatusCode::OK,
            headers: json_headers(body.len(), false),
            body: body.clone(),
        }),
        upstream_record(
            StorageUpstreamKind::GeminiApiKey,
            "https://generativelanguage.googleapis.com/",
        ),
    );

    let request = gemini_generate_request();
    let response = handle_authenticated(&lifecycle, request)
        .await
        .expect("lifecycle handles native Gemini response");
    let (_status, _headers, received) = collect_body(response).await;

    assert_eq!(received, body);
}

async fn buffered_anthropic_response_bytes_remain_unchanged() {
    let body = buffered_upstream_body();
    let lifecycle = lifecycle_with_record(
        None,
        Arc::new(FixedDispatch {
            status: StatusCode::OK,
            headers: json_headers(body.len(), false),
            body: body.clone(),
        }),
        default_upstream_record(),
    );

    let request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[],"stream":false}"#,
    ));
    let response = handle_authenticated(&lifecycle, request)
        .await
        .expect("lifecycle handles Anthropic response");
    let (_status, _headers, received) = collect_body(response).await;

    assert_eq!(received, body);
}

async fn buffered_transform_rewrites_upstream_error_response() {
    let transform = Arc::new(BufferedToolNameTransform::default());
    let lifecycle = lifecycle_with_transforms(
        Some(transform.clone()),
        None,
        Arc::new(FixedDispatch {
            status: StatusCode::BAD_REQUEST,
            headers: json_headers(buffered_upstream_body().len(), false),
            body: buffered_upstream_body(),
        }),
    );

    let request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[],"stream":false}"#,
    ));
    let response = handle_authenticated(&lifecycle, request)
        .await
        .expect("lifecycle handles buffered error response");
    let (status, _headers, body) = collect_body(response).await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    let body_text = std::str::from_utf8(&body).expect("body is utf8");
    assert!(body_text.contains(r#""name":"bash""#));
    assert_eq!(transform.seen_bodies(), vec![buffered_upstream_body()]);
}

async fn terminal_unauthorized_without_refresh_uses_buffered_transform() {
    for stream in [false, true] {
        let transform = Arc::new(BufferedToolNameTransform::default());
        let plaintext = Bytes::from_static(
            br#"{"error":{"type":"authentication_error","message":"failed","name":"Bash"}}"#,
        );
        let lifecycle = lifecycle_with_options(
            Some(transform.clone()),
            None,
            Arc::new(FixedDispatch {
                status: StatusCode::UNAUTHORIZED,
                headers: json_headers(plaintext.len(), false),
                body: plaintext.clone(),
            }),
            LifecycleConfig::default(),
            default_upstream_record(),
            false,
            Vec::new(),
        );
        let response = handle_authenticated(
            &lifecycle,
            messages_request(Bytes::from(format!(
                r#"{{"model":"claude-test","messages":[],"stream":{stream}}}"#,
            ))),
        )
        .await
        .expect("terminal upstream 401 is handled");
        let (status, _headers, body) = collect_body(response).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(
            body,
            replace_bytes(&plaintext, br#""name":"Bash""#, br#""name":"bash""#)
        );
        assert_eq!(transform.seen_bodies(), vec![plaintext]);
    }
}

async fn terminal_unauthorized_generated_body_skips_transform_when_refresh_declined() {
    let transform = Arc::new(UnchangedBufferedTransform::default());
    let body = Bytes::from(vec![b'x'; 16 * 1024]);
    let lifecycle = lifecycle_with_options(
        Some(transform.clone()),
        None,
        Arc::new(FixedDispatch {
            status: StatusCode::UNAUTHORIZED,
            headers: json_headers(body.len(), false),
            body,
        }),
        LifecycleConfig::default(),
        default_upstream_record(),
        false,
        Vec::new(),
    );
    let response = handle_authenticated(
        &lifecycle,
        messages_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[],"stream":true}"#,
        )),
    )
    .await
    .expect("generated 401 collection error is handled");
    let (status, _headers, body) = collect_body(response).await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert!(transform.seen_bodies().is_empty());
    assert!(String::from_utf8_lossy(&body).contains("upstream response body could not be read"));
}
async fn stream_request_json_errors_use_buffered_transform() {
    for limited in [false, true] {
        for compressed in [false, true] {
            for status in [
                StatusCode::BAD_REQUEST,
                StatusCode::UNAUTHORIZED,
                StatusCode::TOO_MANY_REQUESTS,
                StatusCode::INTERNAL_SERVER_ERROR,
            ] {
                let transform = Arc::new(BufferedToolNameTransform::default());
                let sse_transform = Arc::new(UnchangedSseTransform::default());
                let plaintext = Bytes::from_static(
                    br#"{"error":{"type":"api_error","message":"failed","name":"Bash"}}"#,
                );
                let upstream_body = if compressed {
                    gzip_bytes(&plaintext)
                } else {
                    plaintext.clone()
                };
                let lifecycle = lifecycle_with_transforms(
                    Some(transform.clone()),
                    Some(sse_transform.clone()),
                    Arc::new(ChunkedDispatch {
                        status,
                        headers: json_headers(upstream_body.len(), compressed),
                        chunks: vec![upstream_body.slice(..7), upstream_body.slice(7..)],
                    }),
                );
                let lifecycle = if limited {
                    with_test_limit_subject(lifecycle)
                } else {
                    lifecycle
                };
                let response = handle_authenticated(
                    &lifecycle,
                    messages_request(Bytes::from_static(
                        br#"{"model":"claude-test","messages":[],"stream":true}"#,
                    )),
                )
                .await
                .expect("lifecycle handles JSON error for streaming request");
                let (received_status, headers, body) = collect_body(response).await;

                assert_eq!(received_status, status);
                assert_eq!(
                    body,
                    replace_bytes(&plaintext, br#""name":"Bash""#, br#""name":"bash""#),
                );
                assert_eq!(transform.seen_bodies(), vec![plaintext]);
                assert!(sse_transform.seen_events().is_empty());
                assert!(headers.get(CONTENT_ENCODING).is_none());
                assert_eq!(
                    headers.get(CONTENT_LENGTH),
                    Some(&HeaderValue::from_str(&body.len().to_string()).expect("body length")),
                );
            }
        }
    }
}

async fn oversized_unauthorized_stream_body_skips_transform_of_generated_error() {
    let transform = Arc::new(UnchangedBufferedTransform::default());
    let body = Bytes::from(vec![b'x'; 16 * 1024]);
    let lifecycle = with_test_limit_subject(lifecycle_with_transforms(
        Some(transform.clone()),
        None,
        Arc::new(FixedDispatch {
            status: StatusCode::UNAUTHORIZED,
            headers: json_headers(body.len(), false),
            body,
        }),
    ));
    let response = handle_authenticated(
        &lifecycle,
        messages_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[],"stream":true}"#,
        )),
    )
    .await
    .expect("lifecycle handles oversized unauthorized body");
    let (status, _headers, body) = collect_body(response).await;

    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert!(transform.seen_bodies().is_empty());
    assert!(String::from_utf8_lossy(&body).contains("upstream response body could not be read"));
}

async fn non_sse_stream_transform_never_receives_a_capped_prefix() {
    for (body_len, limited) in [(128, false), (129, false), (128, true), (129, true)] {
        let transform = Arc::new(UnchangedBufferedTransform::default());
        let mut data = vec![b'x'; body_len];
        data[0] = b'"';
        data[body_len - 1] = b'"';
        let body = Bytes::from(data);
        let lifecycle = lifecycle_with_transforms_and_config(
            Some(transform.clone()),
            None,
            Arc::new(ChunkedDispatch {
                status: StatusCode::OK,
                headers: json_headers(body.len(), false),
                chunks: vec![body.slice(..64), body.slice(64..)],
            }),
            LifecycleConfig {
                messages_body_cap_bytes: 128,
                ..LifecycleConfig::default()
            },
        );
        let lifecycle = if limited {
            with_test_limit_subject(lifecycle)
        } else {
            lifecycle
        };
        let response = handle_authenticated(
            &lifecycle,
            messages_request(Bytes::from_static(
                br#"{"model":"claude-test","messages":[],"stream":true}"#,
            )),
        )
        .await
        .expect("lifecycle handles bounded non-SSE stream");
        let (status, _headers, received) = collect_body(response).await;

        if body_len == 128 {
            assert_eq!(status, StatusCode::OK);
            assert_eq!(received, body);
            assert_eq!(transform.seen_bodies(), vec![body]);
        } else {
            assert_eq!(status, StatusCode::BAD_GATEWAY);
            assert!(transform.seen_bodies().is_empty());
            assert_ne!(received, body);
        }
    }
}

async fn gemini_json_array_stream_is_transformed_once_as_a_whole() {
    for stream_in_body in [false, true] {
        let transform = Arc::new(BufferedToolNameTransform::default());
        let sse_transform = Arc::new(UnchangedSseTransform::default());
        let body = Bytes::from_static(
            br#"[{"candidates":[{"content":{"parts":[{"functionCall":{"name":"Bash","args":{}}}]}}]},{"candidates":[{"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":3,"candidatesTokenCount":2}}]"#,
        );
        let lifecycle = with_test_limit_subject(lifecycle_with_record_and_config(
            Some(transform.clone()),
            Some(sse_transform.clone()),
            Arc::new(ChunkedDispatch {
                status: StatusCode::OK,
                headers: json_headers(body.len(), false),
                chunks: vec![body.slice(..13), body.slice(13..37), body.slice(37..)],
            }),
            LifecycleConfig::default(),
            upstream_record(
                StorageUpstreamKind::GeminiApiKey,
                "https://generativelanguage.googleapis.com/",
            ),
        ));
        let request_body = if stream_in_body {
            Bytes::from_static(br#"{"contents":[],"stream":true}"#)
        } else {
            Bytes::from_static(br#"{"contents":[]}"#)
        };
        let request = http::Request::builder()
            .method("POST")
            .uri("/v1beta/models/gemini-test:streamGenerateContent")
            .header("x-api-key", common::managed_api_key())
            .body(request_body)
            .expect("Gemini streaming request builds");
        let response = handle_authenticated(&lifecycle, request)
            .await
            .expect("lifecycle handles Gemini JSON array stream");
        let (status, _headers, received) = collect_body(response).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            received,
            replace_bytes(&body, br#""name":"Bash""#, br#""name":"bash""#),
        );
        assert_eq!(transform.seen_bodies(), vec![body]);
        assert!(sse_transform.seen_events().is_empty());
    }
}

async fn gemini_oauth_non_sse_stream_unwraps_before_buffered_transform() {
    let transform = Arc::new(BufferedToolNameTransform::default());
    let wrapped = Bytes::from_static(
        br#"{"response":{"candidates":[{"content":{"parts":[{"functionCall":{"name":"Bash","args":{}}}]}}]},"traceId":"trace"}"#,
    );
    let lifecycle = with_test_limit_subject(lifecycle_with_record(
        Some(transform.clone()),
        Arc::new(FixedDispatch {
            status: StatusCode::OK,
            headers: json_headers(wrapped.len(), false),
            body: wrapped.clone(),
        }),
        upstream_record(
            StorageUpstreamKind::GeminiOauth,
            "https://cloudcode-pa.googleapis.com/v1internal",
        ),
    ));
    let request = http::Request::builder()
        .method("POST")
        .uri("/v1beta/models/gemini-test:streamGenerateContent")
        .header("x-api-key", common::managed_api_key())
        .body(Bytes::from_static(br#"{"contents":[],"stream":true}"#))
        .expect("Gemini OAuth streaming request builds");
    let response = handle_authenticated(&lifecycle, request)
        .await
        .expect("lifecycle handles wrapped Gemini stream response");
    let (status, _headers, received) = collect_body(response).await;
    let wrapped_json: serde_json::Value = serde_json::from_slice(&wrapped).expect("wrapped JSON");
    let expected = Bytes::from(
        serde_json::to_vec(&wrapped_json["response"]).expect("unwrapped response JSON"),
    );

    assert_eq!(status, StatusCode::OK);
    assert_eq!(transform.seen_bodies(), vec![expected.clone()]);
    assert_eq!(
        received,
        replace_bytes(&expected, br#""name":"Bash""#, br#""name":"bash""#),
    );
}

async fn gemini_oauth_stream_error(chunks: &[&'static [u8]]) -> (StatusCode, Bytes) {
    let lifecycle = with_test_limit_subject(lifecycle_with_record(
        None,
        Arc::new(ChunkedDispatch {
            status: StatusCode::TOO_MANY_REQUESTS,
            headers: sse_headers(),
            chunks: chunks.iter().copied().map(Bytes::from_static).collect(),
        }),
        upstream_record(
            StorageUpstreamKind::GeminiOauth,
            "https://cloudcode-pa.googleapis.com/v1internal",
        ),
    ));
    let request = http::Request::builder()
        .method("POST")
        .uri("/v1beta/models/gemini-test:streamGenerateContent?alt=sse")
        .header("x-api-key", common::managed_api_key())
        .body(Bytes::from_static(br#"{"contents":[]}"#))
        .expect("Gemini OAuth streaming request builds");
    let response = handle_authenticated(&lifecycle, request)
        .await
        .expect("lifecycle handles Gemini OAuth stream error");
    let (status, _headers, received) = collect_body(response).await;
    (status, received)
}

/// Cloud Code Assist answers a rejected `streamGenerateContent` with a plain
/// JSON error under `text/event-stream`. The client must receive Google's
/// error, not a local SSE framing error that replaces 429 with 502, including
/// when the JSON carries blank lines that SSE splitting would consume.
async fn gemini_oauth_stream_json_error_reaches_client_unchanged() {
    let compact: &[u8] = br#"{"error":{"code":429,"message":"Resource has been exhausted (e.g. check quota).","status":"RESOURCE_EXHAUSTED"}}"#;
    let cases: [&[&'static [u8]]; 5] = [
        &[compact],
        &[b"\n\n", compact],
        &[
            b"{\n  \"error\": {\n\n    \"code\": 429,\n",
            b"    \"status\": \"RESOURCE_EXHAUSTED\"\n  }\n}\n",
        ],
        // A chunk boundary inside a number still reads as an unfinished document.
        &[
            b"{\"error\":{\"code\":4",
            b"29,\"ratio\":-",
            b"2.5e",
            b"3}}",
        ],
        &[b"[{\"error\":{\"code\":429}}]\n\n"],
    ];
    for chunks in cases {
        let (status, received) = gemini_oauth_stream_error(chunks).await;
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(received, Bytes::from(chunks.concat()));
    }
}

/// SSE whose first field happens to read like JSON keeps SSE semantics: an
/// unterminated event is a framing error, and complete events are relayed
/// byte-for-byte, as on a body that never looked like JSON.
async fn gemini_oauth_stream_sse_error_starting_with_brace_stays_sse() {
    const EVENT: &[u8] =
        b"event: error\ndata: {\"error\":{\"code\":429,\"status\":\"RESOURCE_EXHAUSTED\"}}\n";
    for prefix in [
        &b"{extension: ignored\n"[..],
        b"[trace: ignored\n",
        b"{\"extension:\n",
    ] {
        let unterminated = [prefix, EVENT].concat().leak() as &'static [u8];
        let (status, received) = gemini_oauth_stream_error(&[unterminated]).await;
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
        let text = std::str::from_utf8(&received).expect("error frame is utf8");
        assert!(text.contains("incomplete event before transform output"));

        let complete = [prefix, EVENT, b"\n"].concat().leak() as &'static [u8];
        // The byte that breaks JSON syntax may arrive in a later chunk.
        let (head, tail) = complete.split_at(2);
        for chunks in [&[complete][..], &[head, tail]] {
            let (status, received) = gemini_oauth_stream_error(chunks).await;
            assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
            assert_eq!(received, Bytes::from_static(complete));
        }
    }

    // A body that broke JSON earlier is SSE even when its last byte could
    // continue a number: completed events are relayed before the framing error.
    for (body, relayed) in [
        (
            &b"{\n\nevent: update\n\nmessage"[..],
            &b"{\n\nevent: update\n\n"[..],
        ),
        (
            b"{\ndata: {\"code\":429}\n\n2",
            b"{\ndata: {\"code\":429}\n\n",
        ),
        // A number cannot stand where an object key is required.
        (b"{\n\n2", b"{\n\n"),
        (b"{\n\n+2", b"{\n\n"),
        // Where a value fits, the trailing bytes must still be a legal number.
        (b"{\"a\":\n\n01", b"{\"a\":\n\n"),
        (b"{\"a\":\n\n+2", b"{\"a\":\n\n"),
        (b"{\"a\":\n\ne", b"{\"a\":\n\n"),
        (b"{\"a\":\n\n0.e", b"{\"a\":\n\n"),
    ] {
        let (status, received) = gemini_oauth_stream_error(&[body]).await;
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
        assert!(received.starts_with(relayed));
        let text = std::str::from_utf8(&received).expect("relayed events are utf8");
        assert!(text.contains(
            "upstream SSE response ended with an incomplete event after transform output"
        ));
    }

    // A `\u` escape whose following bytes cannot be hex is not JSON, so its
    // completed event is relayed like any SSE event.
    for body in [&b"{\"a\":\"\\u\n\n"[..], b"[\"\\u0\r\r"] {
        let (status, received) = gemini_oauth_stream_error(&[body]).await;
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(received, Bytes::from_static(body));
    }

    // Whitespace-only error bodies: completed blank events reach the client
    // as sent, a lone line break is dropped, and other whitespace is an
    // unterminated event.
    for (body, expected) in [
        (&b"\n\n"[..], &b"\n\n"[..]),
        (b"\r\n\r\n", b"\r\n\r\n"),
        (b"\n", b""),
        (b"\r\n", b""),
    ] {
        let (status, received) = gemini_oauth_stream_error(&[body]).await;
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(received, Bytes::from_static(expected));
    }
    for body in [&b" "[..], b"\t"] {
        let (status, received) = gemini_oauth_stream_error(&[body]).await;
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
        let text = std::str::from_utf8(&received).expect("error frame is utf8");
        assert!(text.contains("incomplete event before transform output"));
    }
    // A completed blank event goes through the transform like any event,
    // so trailing whitespace is reported after transform output.
    for body in [&b"\n\n "[..], b"\r\n\r\n\t"] {
        let (status, received) = gemini_oauth_stream_error(&[body]).await;
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
        let blank = &body[..body.len() - 1];
        assert!(received.starts_with(blank));
        let text = std::str::from_utf8(&received).expect("error frame is utf8");
        assert!(text.contains("incomplete event after transform output"));
    }
}

async fn ingress_cap_does_not_limit_existing_response_paths() {
    for (stream, transform_response) in [(false, false), (false, true), (true, false)] {
        let transform = Arc::new(UnchangedBufferedTransform::default());
        let body = Bytes::from(format!(r#"{{"output":"{}"}}"#, "x".repeat(256)));
        let lifecycle = lifecycle_with_transforms_and_config(
            transform_response.then(|| transform.clone() as Arc<dyn ResponseTransformHook>),
            None,
            Arc::new(FixedDispatch {
                status: StatusCode::OK,
                headers: json_headers(body.len(), false),
                body: body.clone(),
            }),
            LifecycleConfig {
                messages_body_cap_bytes: 128,
                ..LifecycleConfig::default()
            },
        );
        let response = handle_authenticated(
            &lifecycle,
            messages_request(Bytes::from(format!(
                r#"{{"model":"claude-test","messages":[],"stream":{stream}}}"#,
            ))),
        )
        .await
        .expect("lifecycle preserves the existing response path");
        let (status, _headers, received) = collect_body(response).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(received, body);
        assert_eq!(
            transform.seen_bodies(),
            if transform_response {
                vec![body]
            } else {
                vec![]
            },
        );
    }
}

async fn audio_json_response_preserves_the_ordinary_buffered_path() {
    for content_type in [
        "application/json",
        "application/problem+json; charset=utf-8",
    ] {
        let transform = Arc::new(UnchangedBufferedTransform::default());
        let body = Bytes::from(format!(r#"{{"output":"{}"}}"#, "x".repeat(256)));
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static(content_type));
        let lifecycle = lifecycle_with_record_and_config(
            Some(transform.clone()),
            None,
            Arc::new(FixedDispatch {
                status: StatusCode::OK,
                headers,
                body: body.clone(),
            }),
            LifecycleConfig {
                messages_body_cap_bytes: 128,
                ..LifecycleConfig::default()
            },
            upstream_record(StorageUpstreamKind::OpenaiApiKey, "https://api.openai.com/"),
        );
        let response = handle_authenticated(&lifecycle, openai_audio_request("/v1/audio/speech"))
            .await
            .expect("audio JSON response uses the ordinary buffered path");
        let (status, _headers, received) = collect_body(response).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(received, body);
        assert_eq!(transform.seen_bodies(), vec![body]);
    }
}

async fn non_json_response_relays_first_chunk_before_upstream_eof() {
    for content_type in [
        Some("audio/mpeg"),
        Some("application/octet-stream"),
        Some("image/png"),
        Some("text/plain; charset=utf-8"),
        None,
    ] {
        for path in ["/v1/audio/speech", "/v1/audio/transcriptions"] {
            let release = Arc::new(tokio::sync::Notify::new());
            let lifecycle = lifecycle_with_record(
                None,
                Arc::new(BinaryFirstChunkThenGatedDispatch {
                    content_type,
                    release: release.clone(),
                }),
                upstream_record(StorageUpstreamKind::OpenaiApiKey, "https://api.openai.com/"),
            );
            let response = tokio::time::timeout(
                Duration::from_secs(1),
                handle_authenticated(&lifecycle, openai_audio_request(path)),
            )
            .await
            .expect("binary response headers do not await upstream EOF")
            .expect("lifecycle handles binary response");
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(
                response.headers().get(CONTENT_TYPE),
                content_type.map(HeaderValue::from_static).as_ref(),
            );
            let mut body = response.into_body();
            let first = tokio::time::timeout(Duration::from_secs(1), body.frame())
                .await
                .expect("first binary chunk does not await upstream EOF")
                .expect("first binary frame exists")
                .expect("first binary frame succeeds")
                .into_data()
                .expect("first binary frame is data");
            assert_eq!(first, Bytes::from_static(b"\xfffirst-chunk"));

            release.notify_one();
            let rest = body
                .collect()
                .await
                .expect("remaining binary body collects");
            assert_eq!(rest.to_bytes(), Bytes::from_static(b"\x00last-chunk"));
        }
    }
}

async fn non_json_buffered_transform_is_bounded_without_truncating_body() {
    for content_type in ["audio/mpeg", "application/octet-stream", "image/png"] {
        for (path, cap) in [("/v1/audio/speech", 128), ("/v1/audio/transcriptions", 192)] {
            for body_len in [cap, cap + 1] {
                let transform = Arc::new(UnchangedBufferedTransform::default());
                let body = Bytes::from(vec![0xff; body_len]);
                let mut headers = HeaderMap::new();
                headers.insert(CONTENT_TYPE, HeaderValue::from_static(content_type));
                let lifecycle = lifecycle_with_record_and_config(
                    Some(transform.clone()),
                    None,
                    Arc::new(ChunkedDispatch {
                        status: StatusCode::OK,
                        headers,
                        chunks: vec![body.slice(..64), body.slice(64..)],
                    }),
                    LifecycleConfig {
                        messages_body_cap_bytes: 128,
                        files_body_cap_bytes: 192,
                        ..LifecycleConfig::default()
                    },
                    upstream_record(StorageUpstreamKind::OpenaiApiKey, "https://api.openai.com/"),
                );
                let response = handle_authenticated(&lifecycle, openai_audio_request(path))
                    .await
                    .expect("lifecycle handles bounded binary transform");
                let (status, headers, received) = collect_body(response).await;

                if body_len == cap {
                    assert_eq!(status, StatusCode::OK);
                    assert_eq!(received, body);
                    assert_eq!(transform.seen_bodies(), vec![body]);
                } else {
                    assert_eq!(status, StatusCode::BAD_GATEWAY);
                    assert!(transform.seen_bodies().is_empty());
                    assert_eq!(
                        headers.get(CONTENT_TYPE),
                        Some(&HeaderValue::from_static("application/json; charset=utf-8")),
                    );
                    assert!(
                        String::from_utf8_lossy(&received)
                            .contains("upstream response body could not be read"),
                    );
                }
            }
        }
    }
}

async fn genuine_sse_uses_event_hook_without_waiting_for_body_eof() {
    let buffered = Arc::new(UnchangedBufferedTransform::default());
    let sse = Arc::new(UnchangedSseTransform::default());
    let lifecycle = with_test_limit_subject(lifecycle_with_transforms(
        Some(buffered.clone()),
        Some(sse.clone()),
        Arc::new(SseFirstEventThenPendingDispatch),
    ));
    let response = tokio::time::timeout(
        Duration::from_secs(1),
        handle_authenticated(
            &lifecycle,
            messages_request(Bytes::from_static(
                br#"{"model":"claude-test","messages":[],"stream":true}"#,
            )),
        ),
    )
    .await
    .expect("SSE response headers do not await body EOF")
    .expect("lifecycle handles SSE stream");
    let mut body = response.into_body();
    let first = tokio::time::timeout(Duration::from_secs(1), body.frame())
        .await
        .expect("first SSE event does not await body EOF")
        .expect("first SSE frame exists")
        .expect("first SSE frame succeeds")
        .into_data()
        .expect("first SSE frame is data");

    assert_eq!(
        first,
        Bytes::from_static(b"event: ping\ndata: {\"type\":\"ping\"}\n\n"),
    );
    assert_eq!(sse.seen_events(), vec!["ping".to_owned()]);
    assert!(buffered.seen_bodies().is_empty());
}

async fn buffered_unchanged_preserves_compressed_upstream_bytes() {
    let compressed = gzip_bytes(&buffered_upstream_body());
    let transform = Arc::new(UnchangedBufferedTransform::default());
    let lifecycle = lifecycle_with_transforms(
        Some(transform.clone()),
        None,
        Arc::new(FixedDispatch {
            status: StatusCode::OK,
            headers: with_upstream_unified_headers(json_headers(compressed.len(), true)),
            body: compressed.clone(),
        }),
    );

    let request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[],"stream":false}"#,
    ));
    let response = handle_authenticated(&lifecycle, request)
        .await
        .expect("lifecycle handles compressed buffered response");
    let (_status, headers, body) = collect_body(response).await;

    assert_eq!(body, compressed);
    assert_client_rate_limit_headers(&headers, true);
    assert_eq!(
        headers.get(CONTENT_ENCODING),
        Some(&HeaderValue::from_static("gzip"))
    );
    assert_eq!(
        headers
            .get(CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok()),
        Some(body.len().to_string().as_str())
    );
    assert_eq!(transform.seen_bodies(), vec![buffered_upstream_body()]);
}

async fn buffered_header_only_transform_preserves_compressed_upstream_bytes() {
    let compressed = gzip_bytes(&buffered_upstream_body());
    let transform = Arc::new(HeaderOnlyBufferedTransform::default());
    let lifecycle = lifecycle_with_transforms(
        Some(transform.clone()),
        None,
        Arc::new(FixedDispatch {
            status: StatusCode::OK,
            headers: with_upstream_unified_headers(json_headers(compressed.len(), true)),
            body: compressed.clone(),
        }),
    );

    let request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[],"stream":false}"#,
    ));
    let response = handle_authenticated(&lifecycle, request)
        .await
        .expect("lifecycle handles compressed header-only transform");
    let (_status, headers, body) = collect_body(response).await;

    assert_eq!(body, compressed);
    assert_client_rate_limit_headers(&headers, true);
    assert_eq!(
        headers.get(CONTENT_ENCODING),
        Some(&HeaderValue::from_static("gzip"))
    );
    assert_eq!(
        headers
            .get(CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok()),
        Some(body.len().to_string().as_str())
    );
    assert_eq!(
        headers.get("x-plugin-header"),
        Some(&HeaderValue::from_static("kept"))
    );
    assert!(headers.get("x-api-key").is_none());
    assert_eq!(transform.seen_bodies(), vec![buffered_upstream_body()]);
}

async fn buffered_unsupported_encoding_skips_transform() {
    let transform = Arc::new(BufferedToolNameTransform::default());
    let mut headers = json_headers(buffered_upstream_body().len(), false);
    headers.insert(CONTENT_ENCODING, HeaderValue::from_static("snappy"));
    let lifecycle = lifecycle_with_transforms(
        Some(transform.clone()),
        None,
        Arc::new(FixedDispatch {
            status: StatusCode::OK,
            headers,
            body: buffered_upstream_body(),
        }),
    );

    let request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[],"stream":false}"#,
    ));
    let response = handle_authenticated(&lifecycle, request)
        .await
        .expect("lifecycle handles unsupported encoded response");
    let (_status, headers, body) = collect_body(response).await;

    assert_eq!(body, buffered_upstream_body());
    assert_eq!(
        headers.get(CONTENT_ENCODING),
        Some(&HeaderValue::from_static("snappy"))
    );
    assert!(transform.seen_bodies().is_empty());
}

async fn buffered_transform_failure_fails_open_to_original_response() {
    let transform = Arc::new(FailingBufferedTransform);
    let lifecycle = lifecycle_with_transforms(Some(transform), None, buffered_dispatch());

    let request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[],"stream":false}"#,
    ));
    let response = handle_authenticated(&lifecycle, request)
        .await
        .expect("lifecycle handles buffered response");
    let (_status, headers, body) = collect_body(response).await;

    assert_eq!(body, buffered_upstream_body());
    assert_client_rate_limit_headers(&headers, true);
    assert_eq!(
        headers
            .get(CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .map(ToOwned::to_owned),
        Some(body.len().to_string())
    );
    assert!(headers.get(CONTENT_ENCODING).is_none());
}

async fn buffered_transform_failure_is_observed_as_error() -> Result<(), Box<dyn std::error::Error>>
{
    let dir = tempfile::tempdir()?;
    let storage = Arc::new(sqlite_storage(&dir, "response-transform-observation.sqlite").await?);
    let test_bus =
        TestLifecycleBus::new().with_assembler(Arc::clone(&storage) as Arc<dyn StorageTrait>);
    let transform = Arc::new(FailingBufferedTransform);
    let lifecycle = lifecycle_with_transforms(Some(transform), None, buffered_dispatch())
        .with_event_bus(test_bus.bus_arc());

    let request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[],"stream":false}"#,
    ));
    let response = handle_authenticated(&lifecycle, request)
        .await
        .expect("lifecycle handles buffered response");
    let (status, _headers, body) = collect_body(response).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, buffered_upstream_body());

    let events = wait_for_events(storage.as_ref(), 1).await?;
    assert_eq!(events.len(), 1);
    let event = &events[0];
    assert_eq!(event.status, StatusCode::OK.as_u16());
    assert_eq!(event.error_code.as_deref(), Some("upstream_stream_error"));
    Ok(())
}

async fn buffered_http_error_preserves_canonical_error_over_transform_failure()
-> Result<(), Box<dyn std::error::Error>> {
    let body = Bytes::from_static(
        br#"{"type":"error","error":{"type":"rate_limit_error","message":"forced fake rate limit response"}}"#,
    );
    let dir = tempfile::tempdir()?;
    let storage = Arc::new(sqlite_storage(&dir, "buffered-error-precedence.sqlite").await?);
    let test_bus =
        TestLifecycleBus::new().with_assembler(Arc::clone(&storage) as Arc<dyn StorageTrait>);
    let lifecycle = lifecycle_with_transforms(
        Some(Arc::new(FailingBufferedTransform)),
        None,
        Arc::new(FixedDispatch {
            status: StatusCode::TOO_MANY_REQUESTS,
            headers: json_headers(body.len(), false),
            body: body.clone(),
        }),
    )
    .with_event_bus(test_bus.bus_arc());

    let request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[],"stream":false}"#,
    ));
    let response = handle_authenticated(&lifecycle, request)
        .await
        .expect("lifecycle handles buffered upstream error");
    let (status, _headers, output) = collect_body(response).await;

    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(output, body);
    let events = wait_for_events(storage.as_ref(), 1).await?;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].error_code.as_deref(), Some("upstream_4xx"));
    assert_eq!(
        events[0].upstream_error_type.as_deref(),
        Some("rate_limit_error")
    );
    assert_eq!(
        events[0].upstream_error_message.as_deref(),
        Some("forced fake rate limit response")
    );
    Ok(())
}

async fn sqlite_storage(
    dir: &tempfile::TempDir,
    file_name: &str,
) -> Result<SqliteStorage, Box<dyn std::error::Error>> {
    let database_url = format!("sqlite://{}", dir.path().join(file_name).display());
    let storage =
        cc_lb_storage_sqlite::open_sqlite(&database_url, Arc::new(cc_lb_clock::SystemClock))
            .await?;
    storage.initialize().await?;
    Ok(storage)
}

async fn wait_for_events(
    storage: &dyn RequestEventStore,
    expected: usize,
) -> Result<Vec<cc_lb_storage_api::RequestEvent>, Box<dyn std::error::Error>> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
    loop {
        let events = common::stored_request_events(storage).await?;
        if events.len() >= expected {
            return Ok(events);
        }
        if std::time::Instant::now() >= deadline {
            panic!("expected {expected} request event(s), got {}", events.len());
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
}

async fn buffered_accounting_uses_pre_transform_usage() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let storage = Arc::new(sqlite_storage(&dir, "buffered-accounting.sqlite").await?);
    let test_bus =
        TestLifecycleBus::new().with_assembler(Arc::clone(&storage) as Arc<dyn StorageTrait>);
    let transform = Arc::new(BufferedUsageMutatingTransform);
    let lifecycle = lifecycle_with_transforms(Some(transform), None, buffered_dispatch())
        .with_event_bus(test_bus.bus_arc());

    let request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[],"stream":false}"#,
    ));
    let response = handle_authenticated(&lifecycle, request)
        .await
        .expect("lifecycle handles buffered response");
    let (_status, _headers, body) = collect_body(response).await;

    let body_text = std::str::from_utf8(&body).expect("body is utf8");
    assert!(body_text.contains(r#""input_tokens":999"#));
    let events = wait_for_events(storage.as_ref(), 1).await?;
    assert_eq!(events.len(), 1);
    assert_eq!(
        (events[0].input_tokens, events[0].output_tokens),
        (Some(3), Some(5))
    );
    Ok(())
}

async fn sse_transform_rewrites_content_block_start_event() {
    let transform = Arc::new(SseToolNameTransform::default());
    let lifecycle = lifecycle_with_transforms(None, Some(transform.clone()), sse_dispatch(false));

    let request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[],"stream":false}"#,
    ));
    let response = handle_authenticated(&lifecycle, request)
        .await
        .expect("lifecycle handles SSE response by upstream content-type");
    let (_status, _headers, body) = collect_body(response).await;

    let text = std::str::from_utf8(&body).expect("sse body is utf8");
    assert!(text.contains(r#"event: content_block_start"#));
    assert!(text.contains(r#""name":"bash""#));
    assert!(!text.contains(r#""name":"Bash""#));
    assert_eq!(
        transform.seen_events(),
        vec!["content_block_start".to_owned(), "message_stop".to_owned()]
    );
}

async fn sse_transform_receives_sanitized_response_headers() {
    let transform = Arc::new(HeaderCapturingSseTransform::default());
    let mut headers = sse_headers();
    headers.insert(CONTENT_LENGTH, HeaderValue::from_static("123"));
    headers.insert("x-cc-lb-secret", HeaderValue::from_static("remove-me"));
    headers.insert("x-ratelimit-limit", HeaderValue::from_static("remove-me"));
    headers.insert("authorization", HeaderValue::from_static("Bearer secret"));
    headers.insert("x-safe-header", HeaderValue::from_static("keep-me"));
    let lifecycle = lifecycle_with_transforms(
        None,
        Some(transform.clone()),
        Arc::new(FixedDispatch {
            status: StatusCode::OK,
            headers,
            body: sse_upstream_body(false),
        }),
    );

    let request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[],"stream":true}"#,
    ));
    let response = handle_authenticated(&lifecycle, request)
        .await
        .expect("lifecycle handles SSE response");
    let (_status, _headers, _body) = collect_body(response).await;
    let seen = transform.seen_headers();
    assert!(!seen.is_empty());
    let first = &seen[0];
    assert!(first.get(CONTENT_LENGTH).is_none());
    assert!(first.get("x-cc-lb-secret").is_none());
    assert!(first.get("x-ratelimit-limit").is_none());
    assert!(first.get("authorization").is_none());
    assert_eq!(
        first.get("x-safe-header"),
        Some(&HeaderValue::from_static("keep-me"))
    );
}

async fn sse_unchanged_transform_preserves_raw_event_bytes() {
    let transform = Arc::new(UnchangedSseTransform::default());
    let body =
        Bytes::from_static(b": keep-this-comment\nevent: ping\ndata: {\"type\":\"ping\"}\n\n");
    let lifecycle = lifecycle_with_transforms(
        None,
        Some(transform.clone()),
        Arc::new(FixedDispatch {
            status: StatusCode::OK,
            headers: sse_headers(),
            body: body.clone(),
        }),
    );

    let request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[],"stream":true}"#,
    ));
    let response = handle_authenticated(&lifecycle, request)
        .await
        .expect("lifecycle handles unchanged SSE response");
    let (_status, _headers, output) = collect_body(response).await;

    assert_eq!(output, body);
    assert_eq!(transform.seen_events(), vec!["ping".to_owned()]);
}

async fn sse_transform_decodes_gzip_and_emits_identity_sse() {
    let transform = Arc::new(SseToolNameTransform::default());
    let body = gzip_bytes(&sse_upstream_body(false));
    let mut headers = sse_headers();
    headers.insert(CONTENT_ENCODING, HeaderValue::from_static("gzip"));
    headers.insert(
        CONTENT_LENGTH,
        HeaderValue::from_str(&body.len().to_string()).expect("content length header"),
    );
    let lifecycle = lifecycle_with_transforms(
        None,
        Some(transform.clone()),
        Arc::new(FixedDispatch {
            status: StatusCode::OK,
            headers,
            body,
        }),
    );

    let request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[],"stream":true}"#,
    ));
    let response = handle_authenticated(&lifecycle, request)
        .await
        .expect("lifecycle handles gzip SSE response");
    let (_status, headers, body) = collect_body(response).await;

    assert!(headers.get(CONTENT_ENCODING).is_none());
    assert!(headers.get(CONTENT_LENGTH).is_none());
    let text = std::str::from_utf8(&body).expect("sse body is utf8");
    assert!(text.contains(r#""name":"bash""#));
    assert_eq!(
        transform.seen_events(),
        vec!["content_block_start".to_owned(), "message_stop".to_owned()]
    );
}

async fn gzip_content_length_passthrough_preserves_upstream_bytes_and_headers() {
    let plaintext = sse_upstream_body(false);
    let compressed = gzip_bytes(&plaintext);
    let split_points = [
        1,
        7,
        compressed.len() - 8,
        compressed.len() - 4,
        compressed.len(),
    ];
    let mut start = 0;
    let chunks = split_points
        .into_iter()
        .map(|end| {
            let chunk = Bytes::copy_from_slice(&compressed[start..end]);
            start = end;
            chunk
        })
        .collect::<Vec<_>>();
    let mut headers = sse_headers();
    headers.insert(CONTENT_ENCODING, HeaderValue::from_static("gzip"));
    headers.insert(
        CONTENT_LENGTH,
        HeaderValue::from_str(&compressed.len().to_string()).expect("content length header"),
    );
    let headers = with_upstream_unified_headers(headers);
    let lifecycle = lifecycle_with_transforms(
        None,
        None,
        Arc::new(ChunkedDispatch {
            status: StatusCode::OK,
            headers,
            chunks,
        }),
    );

    let request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[],"stream":true}"#,
    ));
    let response = handle_authenticated(&lifecycle, request)
        .await
        .expect("lifecycle handles chunked gzip passthrough");
    let (_status, headers, output) = collect_body(response).await;

    assert_eq!(
        headers.get(CONTENT_ENCODING),
        Some(&HeaderValue::from_static("gzip"))
    );
    assert!(
        headers.get(CONTENT_LENGTH).is_none(),
        "SSE passthrough normalizes downstream framing while preserving upstream length internally",
    );
    assert_eq!(output, compressed);
    assert_client_rate_limit_headers(&headers, true);
}
async fn content_length_client_stops_after_declared_gzip_bytes_without_terminal_drop() {
    let plaintext = sse_upstream_body(false);
    let compressed = gzip_bytes(&plaintext);
    let mut headers = sse_headers();
    headers.insert(CONTENT_ENCODING, HeaderValue::from_static("gzip"));
    headers.insert(
        CONTENT_LENGTH,
        HeaderValue::from_str(&compressed.len().to_string()).expect("content length header"),
    );
    let test_bus = TestLifecycleBus::new();
    let mut lifecycle_rx = test_bus.bus.subscribe_lifecycle();
    let lifecycle = lifecycle_with_transforms(
        None,
        None,
        Arc::new(FixedDispatch {
            status: StatusCode::OK,
            headers,
            body: compressed.clone(),
        }),
    )
    .with_event_bus(test_bus.bus_arc());

    let request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[],"stream":true}"#,
    ));
    let response = handle_authenticated(&lifecycle, request)
        .await
        .expect("lifecycle handles content-length gzip passthrough");
    assert!(
        response.headers().get(CONTENT_LENGTH).is_none(),
        "SSE passthrough normalizes downstream framing while preserving upstream length internally",
    );
    let mut body = response.into_body();
    let frame = body
        .frame()
        .await
        .expect("declared gzip bytes frame")
        .expect("gzip frame succeeds")
        .into_data()
        .expect("gzip data frame");
    assert_eq!(frame, compressed);
    drop(body);

    loop {
        let event = lifecycle_rx
            .recv()
            .await
            .expect("lifecycle event delivered");
        if let LifecycleEvent::RequestTerminated {
            reason,
            client_status,
            ..
        } = event
        {
            assert_eq!(reason, TerminationReason::Success);
            assert_eq!(client_status, StatusCode::OK.as_u16());
            break;
        }
    }
}

/// Regression: an identity SSE body with Content-Length relayed through the
/// request-hinted affinity gate must finalize before its final bytes reach the
/// client. A client that stops polling once the declared length is satisfied
/// must not turn the completed response into a 499 client cancellation.
async fn content_length_affinity_gated_sse_finalizes_success_before_final_bytes() {
    let body = complete_message_sse_body();
    let mut headers = sse_headers();
    headers.insert(
        CONTENT_LENGTH,
        HeaderValue::from_str(&body.len().to_string()).expect("content length header"),
    );
    let test_bus = TestLifecycleBus::new();
    let mut lifecycle_rx = test_bus.bus.subscribe_lifecycle();
    let lifecycle = lifecycle_with_transforms(
        None,
        None,
        Arc::new(FixedDispatch {
            status: StatusCode::OK,
            headers,
            body: body.clone(),
        }),
    )
    .with_event_bus(test_bus.bus_arc());

    let request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[],"stream":true,"tools":[{"type":"web_search_20250305","name":"web_search"}]}"#,
    ));
    let response = handle_authenticated(&lifecycle, request)
        .await
        .expect("lifecycle handles content-length affinity-gated SSE");
    assert!(
        response.headers().get(CONTENT_LENGTH).is_none(),
        "SSE affinity-gated framing is normalized while the upstream length remains internal",
    );
    let mut downstream = response.into_body();
    let mut received = Vec::with_capacity(body.len());
    while received.len() < body.len() {
        let frame = downstream
            .frame()
            .await
            .expect("declared SSE bytes frame")
            .expect("declared SSE frame succeeds")
            .into_data()
            .expect("declared SSE data frame");
        received.extend_from_slice(&frame);
    }
    assert_eq!(received, body);
    // A Content-Length client stops polling once the declared bytes arrived.
    drop(downstream);

    let mut stream_success_seen = false;
    loop {
        match lifecycle_rx
            .recv()
            .await
            .expect("lifecycle event delivered")
        {
            LifecycleEvent::StreamCompleted { result, .. } => {
                let success = result.expect("stream completes successfully");
                assert_eq!(success.usage.input_tokens, 3);
                assert_eq!(success.usage.output_tokens, 11);
                assert_eq!(success.body_bytes, Some(body.len() as u64));
                stream_success_seen = true;
            }
            LifecycleEvent::RequestTerminated {
                reason,
                client_status,
                ..
            } => {
                assert_eq!(reason, TerminationReason::Success);
                assert_eq!(client_status, StatusCode::OK.as_u16());
                break;
            }
            _ => {}
        }
    }
    assert!(stream_success_seen, "StreamCompleted precedes termination");
}

/// Control: dropping the downstream body before the upstream body ends is a
/// real client cancellation on the same affinity-gated SSE path.
async fn affinity_gated_sse_downstream_drop_before_end_records_client_closed() {
    let body = complete_message_sse_body();
    let split = body
        .windows(2)
        .position(|window| window == b"\n\n")
        .expect("first SSE event boundary")
        + 2;
    let first = body.slice(..split);
    let mut headers = sse_headers();
    headers.insert(
        CONTENT_LENGTH,
        HeaderValue::from_str(&body.len().to_string()).expect("content length header"),
    );
    let test_bus = TestLifecycleBus::new();
    let mut lifecycle_rx = test_bus.bus.subscribe_lifecycle();
    let lifecycle = lifecycle_with_transforms(
        None,
        None,
        Arc::new(ChunkedDispatch {
            status: StatusCode::OK,
            headers,
            chunks: vec![first.clone(), body.slice(split..)],
        }),
    )
    .with_event_bus(test_bus.bus_arc());

    let request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[],"stream":true,"tools":[{"type":"web_search_20250305","name":"web_search"}]}"#,
    ));
    let response = handle_authenticated(&lifecycle, request)
        .await
        .expect("lifecycle handles affinity-gated SSE");
    let mut downstream = response.into_body();
    let frame = downstream
        .frame()
        .await
        .expect("first SSE event frame")
        .expect("first SSE frame succeeds")
        .into_data()
        .expect("first SSE data frame");
    assert_eq!(frame, first);
    drop(downstream);

    loop {
        match lifecycle_rx
            .recv()
            .await
            .expect("lifecycle event delivered")
        {
            LifecycleEvent::StreamCompleted { .. } => {
                panic!("a cancelled stream must not report completion")
            }
            LifecycleEvent::RequestTerminated {
                reason,
                client_status,
                ..
            } => {
                assert_eq!(
                    reason,
                    TerminationReason::ErrorCode("client_closed_request".to_owned())
                );
                assert_eq!(client_status, 499);
                break;
            }
            _ => {}
        }
    }
}

async fn content_length_malformed_sse_delivers_raw_bytes_before_error_telemetry() {
    let malformed =
        Bytes::from_static(b"event: content_block_delta\ndata: {\"type\":\"content_block_delta\"");
    let mut headers = sse_headers();
    headers.insert(
        CONTENT_LENGTH,
        HeaderValue::from_str(&malformed.len().to_string()).expect("content length header"),
    );
    let test_bus = TestLifecycleBus::new();
    let mut lifecycle_rx = test_bus.bus.subscribe_lifecycle();
    let lifecycle = lifecycle_with_transforms(
        None,
        None,
        Arc::new(FixedDispatch {
            status: StatusCode::OK,
            headers,
            body: malformed.clone(),
        }),
    )
    .with_event_bus(test_bus.bus_arc());

    let request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[],"stream":true}"#,
    ));
    let response = handle_authenticated(&lifecycle, request)
        .await
        .expect("lifecycle handles malformed content-length SSE");
    let mut body = response.into_body();
    let first = body
        .frame()
        .await
        .expect("declared SSE bytes frame")
        .expect("declared SSE frame succeeds")
        .into_data()
        .expect("declared SSE data frame");
    assert_eq!(first, malformed);
    drop(body);

    let mut stream_error_seen = false;
    let mut terminal_seen = false;
    while !stream_error_seen || !terminal_seen {
        match lifecycle_rx
            .recv()
            .await
            .expect("lifecycle event delivered")
        {
            LifecycleEvent::StreamCompleted {
                result: Err(error), ..
            } => {
                assert_eq!(error.error_type, "upstream_response_framing_error");
                stream_error_seen = true;
            }
            LifecycleEvent::RequestTerminated {
                reason,
                client_status,
                ..
            } => {
                assert_eq!(
                    reason,
                    TerminationReason::ErrorCode("upstream_stream_error".to_owned())
                );
                assert_eq!(client_status, StatusCode::OK.as_u16());
                terminal_seen = true;
            }
            _ => {}
        }
    }
}

async fn truncated_gzip_passthrough_is_recorded_as_upstream_decode_error()
-> Result<(), Box<dyn std::error::Error>> {
    let plaintext = Bytes::from_static(b"event: ping\ndata: {\"type\":\"ping\"}\n\n");
    let body = incomplete_gzip_prefix(&plaintext);
    let mut headers = sse_headers();
    headers.insert(CONTENT_ENCODING, HeaderValue::from_static("gzip"));
    let dir = tempfile::tempdir()?;
    let storage = Arc::new(sqlite_storage(&dir, "truncated-gzip-passthrough.sqlite").await?);
    let test_bus =
        TestLifecycleBus::new().with_assembler(Arc::clone(&storage) as Arc<dyn StorageTrait>);
    let lifecycle = lifecycle_with_transforms(
        None,
        None,
        Arc::new(FixedDispatch {
            status: StatusCode::OK,
            headers,
            body: body.clone(),
        }),
    )
    .with_event_bus(test_bus.bus_arc());

    let request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[],"stream":true}"#,
    ));
    let response = handle_authenticated(&lifecycle, request)
        .await
        .expect("lifecycle handles truncated gzip passthrough");
    let (_status, headers, output) = collect_body(response).await;

    assert_eq!(
        headers.get(CONTENT_ENCODING),
        Some(&HeaderValue::from_static("gzip"))
    );
    assert_eq!(output, body);
    let events = wait_for_events(storage.as_ref(), 1).await?;
    assert_eq!(events.len(), 1);
    assert_eq!(
        events[0].error_code.as_deref(),
        Some("upstream_stream_error")
    );
    assert_eq!(
        events[0].upstream_error_type.as_deref(),
        Some("upstream_response_decode_error")
    );
    assert!(
        events[0]
            .upstream_error_message
            .as_deref()
            .is_some_and(|message| message.contains("matching checksum"))
    );
    Ok(())
}

async fn sse_transform_decodes_brotli_and_emits_identity_sse() {
    let transform = Arc::new(SseToolNameTransform::default());
    let body = brotli_bytes(&sse_upstream_body(false));
    let mut headers = sse_headers();
    headers.insert(CONTENT_ENCODING, HeaderValue::from_static("br"));
    headers.insert(
        CONTENT_LENGTH,
        HeaderValue::from_str(&body.len().to_string()).expect("content length header"),
    );
    let lifecycle = lifecycle_with_transforms(
        None,
        Some(transform.clone()),
        Arc::new(FixedDispatch {
            status: StatusCode::OK,
            headers,
            body,
        }),
    );

    let request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[],"stream":true}"#,
    ));
    let response = handle_authenticated(&lifecycle, request)
        .await
        .expect("lifecycle handles brotli SSE response");
    let (_status, headers, body) = collect_body(response).await;

    assert!(headers.get(CONTENT_ENCODING).is_none());
    assert!(headers.get(CONTENT_LENGTH).is_none());
    let text = std::str::from_utf8(&body).expect("sse body is utf8");
    assert!(text.contains(r#""name":"bash""#));
    assert_eq!(
        transform.seen_events(),
        vec!["content_block_start".to_owned(), "message_stop".to_owned()]
    );
}

async fn truncated_gzip_after_transformed_output_is_upstream_decode_error()
-> Result<(), Box<dyn std::error::Error>> {
    let transform = Arc::new(UnchangedSseTransform::default());
    let plaintext = Bytes::from_static(b"event: ping\ndata: {\"type\":\"ping\"}\n\n");
    let body = incomplete_gzip_prefix(&plaintext);
    let compressed_len = body.len() as u64;
    let mut headers = sse_headers();
    headers.insert(CONTENT_ENCODING, HeaderValue::from_static("gzip"));
    let dir = tempfile::tempdir()?;
    let storage = Arc::new(sqlite_storage(&dir, "truncated-gzip.sqlite").await?);
    let test_bus =
        TestLifecycleBus::new().with_assembler(Arc::clone(&storage) as Arc<dyn StorageTrait>);
    let mut lifecycle_rx = test_bus.bus.subscribe_lifecycle();
    let lifecycle = lifecycle_with_transforms(
        None,
        Some(transform.clone()),
        Arc::new(FixedDispatch {
            status: StatusCode::OK,
            headers,
            body,
        }),
    )
    .with_event_bus(test_bus.bus_arc());

    let request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[],"stream":true}"#,
    ));
    let response = handle_authenticated(&lifecycle, request)
        .await
        .expect("lifecycle handles truncated gzip SSE response");
    let (_status, headers, body) = collect_body(response).await;

    assert!(headers.get(CONTENT_ENCODING).is_none());
    let text = std::str::from_utf8(&body).expect("SSE body is utf8");
    assert!(text.contains("event: ping\n"));
    assert!(text.contains("event: error\n"));
    assert!(text.contains("\"api_error\""));
    assert!(!text.contains("response_transform_error"));
    assert_eq!(transform.seen_events(), vec!["ping".to_owned()]);

    let error = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if let LifecycleEvent::StreamCompleted {
                result: Err(error), ..
            } = lifecycle_rx
                .recv()
                .await
                .expect("lifecycle event delivered")
            {
                break error;
            }
        }
    })
    .await
    .expect("stream error observed");
    assert_eq!(error.error_type, "upstream_response_decode_error");
    assert!(error.error_message.contains("matching checksum"));

    let events = wait_for_events(storage.as_ref(), 1).await?;
    assert_eq!(events.len(), 1);
    let event = &events[0];
    assert_eq!(event.error_code.as_deref(), Some("upstream_stream_error"));
    assert_eq!(
        event.upstream_error_type.as_deref(),
        Some("upstream_response_decode_error")
    );
    assert!(
        event
            .upstream_error_message
            .as_deref()
            .is_some_and(|message| message.contains("matching checksum"))
    );
    // #663 Q02: truncation before message_stop stays fatal, records no warning,
    // and persists body-end attribution.
    assert_eq!(event.upstream_stream_warning_type, None);
    assert_eq!(event.upstream_stream_warning_message, None);
    assert_eq!(
        event.upstream_body_end.as_deref(),
        Some("decode_error_after_clean_end")
    );
    assert_eq!(event.upstream_body_bytes, Some(compressed_len));
    assert_eq!(event.upstream_content_encoding.as_deref(), Some("gzip"));
    assert_eq!(event.upstream_http_version.as_deref(), Some("HTTP/1.1"));
    assert_eq!(
        event.upstream_body_error_cause.as_deref(),
        Some("decode_error")
    );
    Ok(())
}

// #663 accept-with-warning policy: an upstream SSE body that fails after cc-lb
// parsed `message_stop` completes as success with a persisted warning when
// cc-lb is the SSE emitter of record; every other failure keeps today's error.

async fn sse_transform_gzip_trailer_missing_after_message_stop_is_accepted_with_warning()
-> Result<(), Box<dyn std::error::Error>> {
    let plaintext = complete_message_sse_body();
    let compressed = gzip_without_trailer(&plaintext);
    let transform = Arc::new(SseToolNameTransform::default());
    let relayed = relay_and_record(
        "q01-gzip-trailer-missing.sqlite",
        Some(transform.clone()),
        fixed_gzip_sse_dispatch(StatusCode::OK, compressed.clone()),
    )
    .await?;

    assert_eq!(relayed.status, StatusCode::OK);
    assert!(relayed.headers.get(CONTENT_ENCODING).is_none());
    assert_eq!(
        relayed.body,
        replace_bytes(&plaintext, br#""name":"Bash""#, br#""name":"bash""#)
    );
    assert!(!String::from_utf8_lossy(&relayed.body).contains("event: error"));
    assert_eq!(
        transform.seen_events(),
        vec![
            "message_start".to_owned(),
            "content_block_start".to_owned(),
            "message_delta".to_owned(),
            "message_stop".to_owned(),
        ]
    );
    let row = &relayed.row;
    assert_accepted_with_warning(row, "upstream_response_decode_error");
    assert!(
        row.upstream_stream_warning_message
            .as_deref()
            .is_some_and(|message| message.contains("matching checksum"))
    );
    assert_eq!(row.input_tokens, Some(3));
    assert_eq!(row.output_tokens, Some(11));
    assert_eq!(row.upstream_content_encoding.as_deref(), Some("gzip"));
    assert_eq!(row.upstream_http_version.as_deref(), Some("HTTP/1.1"));
    assert_eq!(row.upstream_body_bytes, Some(compressed.len() as u64));
    assert_eq!(
        row.upstream_body_end.as_deref(),
        Some("decode_error_after_clean_end")
    );
    assert_eq!(
        row.upstream_body_error_cause.as_deref(),
        Some("decode_error")
    );
    Ok(())
}

async fn sse_transform_gzip_crc_mismatch_after_message_stop_is_indistinguishable_from_missing_trailer()
-> Result<(), Box<dyn std::error::Error>> {
    let plaintext = complete_message_sse_body();
    let compressed = gzip_with_corrupt_crc(&plaintext);
    let relayed = relay_and_record(
        "q03-gzip-crc-mismatch.sqlite",
        Some(Arc::new(UnchangedSseTransform::default())),
        fixed_gzip_sse_dispatch(StatusCode::OK, compressed.clone()),
    )
    .await?;

    assert_eq!(relayed.body, plaintext);
    let row = &relayed.row;
    assert_accepted_with_warning(row, "upstream_response_decode_error");
    assert!(
        row.upstream_stream_warning_message
            .as_deref()
            .is_some_and(|message| message.contains("matching checksum"))
    );
    assert_eq!(row.upstream_body_bytes, Some(compressed.len() as u64));
    assert_eq!(
        row.upstream_body_end.as_deref(),
        Some("decode_error_after_clean_end")
    );
    Ok(())
}

async fn incomplete_trailing_event_after_message_stop_with_gzip_failure_stays_fatal()
-> Result<(), Box<dyn std::error::Error>> {
    let complete = complete_message_sse_body();
    let mut plaintext = complete.to_vec();
    plaintext.extend_from_slice(b"event: ping\ndata: {");
    let compressed = gzip_without_trailer(&Bytes::from(plaintext));
    let relayed = relay_and_record(
        "q04-partial-event-after-stop.sqlite",
        Some(Arc::new(UnchangedSseTransform::default())),
        fixed_gzip_sse_dispatch(StatusCode::OK, compressed.clone()),
    )
    .await?;

    assert!(relayed.body.starts_with(&complete));
    let text = std::str::from_utf8(&relayed.body).expect("SSE body is utf8");
    assert_eq!(text.matches("event: error\n").count(), 1);
    assert!(text.contains("matching checksum"));
    let row = &relayed.row;
    assert_eq!(row.error_code.as_deref(), Some("upstream_stream_error"));
    assert_eq!(
        row.upstream_error_type.as_deref(),
        Some("upstream_response_decode_error")
    );
    assert_eq!(row.upstream_stream_warning_type, None);
    assert_eq!(row.upstream_stream_warning_message, None);
    assert_eq!(
        row.upstream_body_end.as_deref(),
        Some("decode_error_after_clean_end")
    );
    assert_eq!(row.upstream_body_bytes, Some(compressed.len() as u64));
    Ok(())
}

async fn gzip_trailer_failure_after_message_stop_with_trailing_line_terminators_is_accepted()
-> Result<(), Box<dyn std::error::Error>> {
    let complete = complete_message_sse_body();
    let mut plaintext = complete.to_vec();
    plaintext.extend_from_slice(b"\r\n");
    let compressed = gzip_without_trailer(&Bytes::from(plaintext));
    let relayed = relay_and_record(
        "q05-trailing-terminators.sqlite",
        Some(Arc::new(UnchangedSseTransform::default())),
        fixed_gzip_sse_dispatch(StatusCode::OK, compressed),
    )
    .await?;

    assert!(relayed.body.starts_with(&complete));
    assert!(!String::from_utf8_lossy(&relayed.body).contains("event: error"));
    assert_accepted_with_warning(&relayed.row, "upstream_response_decode_error");
    assert_eq!(
        relayed.row.upstream_body_end.as_deref(),
        Some("decode_error_after_clean_end")
    );
    Ok(())
}

async fn gzip_body_error_after_message_stop_then_finish_failure_is_single_warning()
-> Result<(), Box<dyn std::error::Error>> {
    let plaintext = complete_message_sse_body();
    // Sync-flushed: every event decodes, but no final block or trailer follows,
    // so the decoder finish also fails after the transport error.
    let chunk = incomplete_gzip_prefix(&plaintext);
    let relayed = relay_and_record(
        "q06-body-error-after-stop.sqlite",
        Some(Arc::new(UnchangedSseTransform::default())),
        Arc::new(ScriptedDispatch::gzip_sse(
            vec![chunk.clone()],
            Some(std::io::ErrorKind::ConnectionReset),
        )),
    )
    .await?;

    assert_eq!(relayed.body, plaintext);
    assert!(!String::from_utf8_lossy(&relayed.body).contains("event: error"));
    let row = &relayed.row;
    assert_accepted_with_warning(row, "upstream_response_body_error");
    assert!(row.upstream_stream_warning_message.is_some());
    assert!(
        !row.upstream_stream_warning_message
            .as_deref()
            .is_some_and(|message| message.contains("matching checksum")),
        "finish failure must not replace the first warning"
    );
    assert_eq!(row.upstream_body_end.as_deref(), Some("transport_error"));
    assert_eq!(row.upstream_body_error_cause.as_deref(), Some("io_reset"));
    assert_eq!(
        row.upstream_body_error_io_kind.as_deref(),
        Some("ConnectionReset")
    );
    assert_eq!(row.upstream_body_bytes, Some(chunk.len() as u64));
    Ok(())
}

async fn gzip_body_error_before_message_stop_stays_fatal_with_attribution()
-> Result<(), Box<dyn std::error::Error>> {
    let plaintext = Bytes::from_static(
        b"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
    );
    let chunk = incomplete_gzip_prefix(&plaintext);
    let relayed = relay_and_record(
        "q07-body-error-before-stop.sqlite",
        Some(Arc::new(UnchangedSseTransform::default())),
        Arc::new(ScriptedDispatch::gzip_sse(
            vec![chunk.clone()],
            Some(std::io::ErrorKind::ConnectionReset),
        )),
    )
    .await?;

    assert!(relayed.body.starts_with(&plaintext));
    let text = std::str::from_utf8(&relayed.body).expect("SSE body is utf8");
    assert_eq!(text.matches("event: error\n").count(), 1);
    let row = &relayed.row;
    assert_eq!(row.error_code.as_deref(), Some("upstream_stream_error"));
    assert_eq!(
        row.upstream_error_type.as_deref(),
        Some("upstream_response_body_error")
    );
    assert_eq!(row.upstream_stream_warning_type, None);
    assert_eq!(row.upstream_body_end.as_deref(), Some("transport_error"));
    assert_eq!(row.upstream_body_error_cause.as_deref(), Some("io_reset"));
    assert_eq!(
        row.upstream_body_error_io_kind.as_deref(),
        Some("ConnectionReset")
    );
    assert_eq!(row.upstream_body_bytes, Some(chunk.len() as u64));
    assert_eq!(row.stream_message_stop_ms, None);
    Ok(())
}

async fn uncompressed_sse_body_error_after_message_stop_is_accepted_with_warning()
-> Result<(), Box<dyn std::error::Error>> {
    let plaintext = complete_message_sse_body();
    let relayed = relay_and_record(
        "q08-identity-body-error-after-stop.sqlite",
        None,
        Arc::new(ScriptedDispatch::identity_sse(
            vec![plaintext.clone()],
            Some(std::io::ErrorKind::ConnectionReset),
        )),
    )
    .await?;

    assert_eq!(relayed.body, plaintext);
    let row = &relayed.row;
    assert_accepted_with_warning(row, "upstream_response_body_error");
    assert_eq!(row.upstream_body_end.as_deref(), Some("transport_error"));
    assert_eq!(row.upstream_content_encoding, None);
    assert_eq!(row.upstream_body_bytes, Some(plaintext.len() as u64));
    Ok(())
}

async fn uncompressed_sse_body_error_after_message_stop_with_partial_event_stays_fatal()
-> Result<(), Box<dyn std::error::Error>> {
    let complete = complete_message_sse_body();
    let mut chunk = complete.to_vec();
    chunk.extend_from_slice(b"event: ping\ndata: {");
    let chunk = Bytes::from(chunk);
    let relayed = relay_and_record(
        "q09-identity-partial-event.sqlite",
        None,
        Arc::new(ScriptedDispatch::identity_sse(
            vec![chunk.clone()],
            Some(std::io::ErrorKind::ConnectionReset),
        )),
    )
    .await?;

    assert!(relayed.body.starts_with(&chunk));
    let text = std::str::from_utf8(&relayed.body).expect("SSE body is utf8");
    assert_eq!(text.matches("event: error\n").count(), 1);
    let row = &relayed.row;
    assert_eq!(row.error_code.as_deref(), Some("upstream_stream_error"));
    assert_eq!(
        row.upstream_error_type.as_deref(),
        Some("upstream_response_body_error")
    );
    assert_eq!(row.upstream_stream_warning_type, None);
    assert_eq!(row.upstream_body_end.as_deref(), Some("transport_error"));
    Ok(())
}

async fn compressed_passthrough_trailer_failure_after_message_stop_keeps_error()
-> Result<(), Box<dyn std::error::Error>> {
    let plaintext = complete_message_sse_body();
    let compressed = gzip_without_trailer(&plaintext);
    let relayed = relay_and_record(
        "q10-compressed-passthrough.sqlite",
        None,
        fixed_gzip_sse_dispatch(StatusCode::OK, compressed.clone()),
    )
    .await?;

    assert_eq!(relayed.body, compressed);
    assert_eq!(
        relayed.headers.get(CONTENT_ENCODING),
        Some(&HeaderValue::from_static("gzip"))
    );
    let row = &relayed.row;
    assert_eq!(row.error_code.as_deref(), Some("upstream_stream_error"));
    assert_eq!(
        row.upstream_error_type.as_deref(),
        Some("upstream_response_decode_error")
    );
    assert_eq!(row.upstream_stream_warning_type, None);
    assert_eq!(row.upstream_stream_warning_message, None);
    assert_eq!(
        row.upstream_body_end.as_deref(),
        Some("decode_error_after_clean_end")
    );
    assert_eq!(row.upstream_body_bytes, Some(compressed.len() as u64));
    assert_eq!(row.upstream_content_encoding.as_deref(), Some("gzip"));
    assert!(row.stream_message_stop_ms.is_some());
    Ok(())
}

async fn http_error_sse_with_gzip_trailer_failure_keeps_http_classification()
-> Result<(), Box<dyn std::error::Error>> {
    let compressed = gzip_without_trailer(&complete_message_sse_body());
    let relayed = relay_and_record(
        "q14-http-error-gzip.sqlite",
        Some(Arc::new(UnchangedSseTransform::default())),
        fixed_gzip_sse_dispatch(StatusCode::TOO_MANY_REQUESTS, compressed),
    )
    .await?;

    assert_eq!(relayed.status, StatusCode::TOO_MANY_REQUESTS);
    let text = std::str::from_utf8(&relayed.body).expect("SSE body is utf8");
    assert!(text.contains("matching checksum"));
    let row = &relayed.row;
    assert_eq!(row.status, StatusCode::TOO_MANY_REQUESTS.as_u16());
    assert_eq!(row.error_code.as_deref(), Some("upstream_4xx"));
    assert_eq!(row.upstream_stream_warning_type, None);
    assert_eq!(row.upstream_stream_warning_message, None);
    Ok(())
}

async fn provider_error_then_gzip_trailer_failure_is_not_accepted()
-> Result<(), Box<dyn std::error::Error>> {
    let plaintext = Bytes::from_static(
        b"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\nevent: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"Overloaded\"}}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
    );
    let relayed = relay_and_record(
        "q15-provider-error.sqlite",
        Some(Arc::new(UnchangedSseTransform::default())),
        fixed_gzip_sse_dispatch(StatusCode::OK, gzip_without_trailer(&plaintext)),
    )
    .await?;

    assert_eq!(relayed.body, plaintext);
    let text = std::str::from_utf8(&relayed.body).expect("SSE body is utf8");
    assert_eq!(text.matches("event: error\n").count(), 1);
    assert!(!text.contains("matching checksum"));
    let row = &relayed.row;
    assert_eq!(row.upstream_error_type.as_deref(), Some("overloaded_error"));
    assert_eq!(row.upstream_stream_warning_type, None);
    assert_eq!(row.upstream_stream_warning_message, None);
    Ok(())
}

// The SSE `event:` name alone is not the semantic stop: only the usage parser's
// `usage_update.message_stop` (JSON `type` match) enables accept-with-warning.

async fn event_named_message_stop_with_delta_payload_and_gzip_failure_stays_fatal()
-> Result<(), Box<dyn std::error::Error>> {
    let plaintext = Bytes::from_static(
        b"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\nevent: message_stop\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":11}}\n\n",
    );
    let relayed = relay_and_record(
        "name-only-message-stop-delta.sqlite",
        Some(Arc::new(UnchangedSseTransform::default())),
        fixed_gzip_sse_dispatch(StatusCode::OK, gzip_without_trailer(&plaintext)),
    )
    .await?;

    assert!(relayed.body.starts_with(&plaintext));
    let text = std::str::from_utf8(&relayed.body).expect("SSE body is utf8");
    assert_eq!(text.matches("event: error\n").count(), 1);
    assert!(text.contains("matching checksum"));
    let row = &relayed.row;
    assert_eq!(row.error_code.as_deref(), Some("upstream_stream_error"));
    assert_eq!(
        row.upstream_error_type.as_deref(),
        Some("upstream_response_decode_error")
    );
    assert_eq!(row.upstream_stream_warning_type, None);
    assert_eq!(row.upstream_stream_warning_message, None);
    Ok(())
}

async fn event_named_message_stop_with_malformed_payload_and_gzip_failure_stays_fatal()
-> Result<(), Box<dyn std::error::Error>> {
    let plaintext = Bytes::from_static(
        b"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\nevent: message_stop\ndata: {not-json}\n\n",
    );
    let relayed = relay_and_record(
        "name-only-message-stop-malformed.sqlite",
        Some(Arc::new(UnchangedSseTransform::default())),
        fixed_gzip_sse_dispatch(StatusCode::OK, gzip_without_trailer(&plaintext)),
    )
    .await?;

    assert!(relayed.body.starts_with(&plaintext));
    let text = std::str::from_utf8(&relayed.body).expect("SSE body is utf8");
    assert_eq!(text.matches("event: error\n").count(), 1);
    assert!(text.contains("matching checksum"));
    let row = &relayed.row;
    assert_eq!(row.error_code.as_deref(), Some("upstream_stream_error"));
    assert_eq!(
        row.upstream_error_type.as_deref(),
        Some("upstream_response_decode_error")
    );
    assert_eq!(row.upstream_stream_warning_type, None);
    assert_eq!(row.upstream_stream_warning_message, None);
    Ok(())
}

async fn name_only_message_stop_without_json_type_and_gzip_failure_stays_fatal()
-> Result<(), Box<dyn std::error::Error>> {
    for data in [b"data: {}".as_slice(), b"data: {\"type\":null}".as_slice()] {
        let mut plaintext = Vec::from(
            &b"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\nevent: message_stop\n"[..],
        );
        plaintext.extend_from_slice(data);
        plaintext.extend_from_slice(b"\n\n");
        let plaintext = Bytes::from(plaintext);
        let relayed = relay_and_record(
            "name-only-message-stop-json-type-missing.sqlite",
            Some(Arc::new(UnchangedSseTransform::default())),
            fixed_gzip_sse_dispatch(StatusCode::OK, gzip_without_trailer(&plaintext)),
        )
        .await?;

        assert!(relayed.body.starts_with(&plaintext));
        let text = std::str::from_utf8(&relayed.body).expect("SSE body is utf8");
        assert_eq!(text.matches("event: error\n").count(), 1);
        assert!(text.contains("matching checksum"));
        let row = &relayed.row;
        assert_eq!(row.error_code.as_deref(), Some("upstream_stream_error"));
        assert_eq!(
            row.upstream_error_type.as_deref(),
            Some("upstream_response_decode_error")
        );
        assert_eq!(row.upstream_stream_warning_type, None);
        assert_eq!(row.upstream_stream_warning_message, None);
    }
    Ok(())
}

async fn upstream_diagnostics_record_http2_version_request_id_and_content_length()
-> Result<(), Box<dyn std::error::Error>> {
    let compressed = gzip_without_trailer(&complete_message_sse_body());
    let mut dispatch = ScriptedDispatch::gzip_sse(vec![compressed.clone()], None);
    dispatch.version = http::Version::HTTP_2;
    dispatch.headers.insert(
        "request-id",
        HeaderValue::from_static("req_011CUpstream663"),
    );
    dispatch.headers.insert(
        CONTENT_LENGTH,
        HeaderValue::from_str(&compressed.len().to_string()).expect("content length header"),
    );
    let relayed = relay_and_record(
        "q16-http2-diagnostics.sqlite",
        Some(Arc::new(UnchangedSseTransform::default())),
        Arc::new(dispatch),
    )
    .await?;

    let row = &relayed.row;
    assert_accepted_with_warning(row, "upstream_response_decode_error");
    assert_eq!(row.upstream_http_version.as_deref(), Some("HTTP/2.0"));
    assert_eq!(
        row.upstream_request_id.as_deref(),
        Some("req_011CUpstream663")
    );
    assert_eq!(row.upstream_content_length, Some(compressed.len() as u64));
    assert_eq!(row.upstream_body_bytes, Some(compressed.len() as u64));
    Ok(())
}

async fn clean_gzip_sse_stream_records_no_upstream_diagnostics()
-> Result<(), Box<dyn std::error::Error>> {
    let plaintext = complete_message_sse_body();
    let relayed = relay_and_record(
        "q17-clean-stream.sqlite",
        Some(Arc::new(UnchangedSseTransform::default())),
        fixed_gzip_sse_dispatch(StatusCode::OK, gzip_bytes(&plaintext)),
    )
    .await?;

    assert_eq!(relayed.body, plaintext);
    let row = &relayed.row;
    assert_eq!(row.error_code, None);
    assert_eq!(row.upstream_error_type, None);
    assert_eq!(row.upstream_http_version, None);
    assert_eq!(row.upstream_request_id, None);
    assert_eq!(row.upstream_content_encoding, None);
    assert_eq!(row.upstream_content_length, None);
    assert_eq!(row.upstream_body_bytes, None);
    assert_eq!(row.upstream_body_end, None);
    assert_eq!(row.upstream_body_error_cause, None);
    assert_eq!(row.upstream_body_error_io_kind, None);
    assert_eq!(row.upstream_body_error_h2_reason, None);
    assert_eq!(row.upstream_stream_warning_type, None);
    assert_eq!(row.upstream_stream_warning_message, None);
    Ok(())
}

struct RelayedRow {
    status: StatusCode,
    headers: HeaderMap,
    body: Bytes,
    row: cc_lb_storage_api::RequestEvent,
}

async fn relay_and_record(
    db_name: &str,
    sse_transform: Option<Arc<dyn SseEventTransformHook>>,
    dispatcher: Arc<dyn UpstreamDispatch>,
) -> Result<RelayedRow, Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let storage = Arc::new(sqlite_storage(&dir, db_name).await?);
    let test_bus =
        TestLifecycleBus::new().with_assembler(Arc::clone(&storage) as Arc<dyn StorageTrait>);
    let lifecycle = lifecycle_with_transforms(None, sse_transform, dispatcher)
        .with_event_bus(test_bus.bus_arc());

    let request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[],"stream":true}"#,
    ));
    let response = handle_authenticated(&lifecycle, request)
        .await
        .expect("lifecycle handles upstream SSE response");
    let (status, headers, body) = collect_body(response).await;
    let mut events = wait_for_events(storage.as_ref(), 1).await?;
    assert_eq!(events.len(), 1);
    let row = events.remove(0);
    Ok(RelayedRow {
        status,
        headers,
        body,
        row,
    })
}

fn assert_accepted_with_warning(row: &cc_lb_storage_api::RequestEvent, warning_type: &str) {
    assert_eq!(row.status, StatusCode::OK.as_u16());
    assert_eq!(row.error_code, None);
    assert_eq!(row.upstream_error_type, None);
    assert_eq!(row.upstream_error_message, None);
    assert_eq!(
        row.upstream_stream_warning_type.as_deref(),
        Some(warning_type)
    );
    assert!(row.stream_message_stop_ms.is_some());
}

async fn truncated_gzip_before_transformed_output_emits_decode_error()
-> Result<(), Box<dyn std::error::Error>> {
    let transform = Arc::new(UnchangedSseTransform::default());
    let plaintext = Bytes::from_static(b"event: ping\ndata: {\"type\":\"ping\"}");
    let body = incomplete_gzip_prefix(&plaintext);
    let mut headers = sse_headers();
    headers.insert(CONTENT_ENCODING, HeaderValue::from_static("gzip"));
    let dir = tempfile::tempdir()?;
    let storage = Arc::new(sqlite_storage(&dir, "truncated-gzip-before-output.sqlite").await?);
    let test_bus =
        TestLifecycleBus::new().with_assembler(Arc::clone(&storage) as Arc<dyn StorageTrait>);
    let lifecycle = lifecycle_with_transforms(
        None,
        Some(transform.clone()),
        Arc::new(FixedDispatch {
            status: StatusCode::OK,
            headers,
            body,
        }),
    )
    .with_event_bus(test_bus.bus_arc());

    let request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[],"stream":true}"#,
    ));
    let response = handle_authenticated(&lifecycle, request)
        .await
        .expect("lifecycle handles truncated gzip before transform output");
    let (_status, headers, output) = collect_body(response).await;

    assert!(headers.get(CONTENT_ENCODING).is_none());
    let text = std::str::from_utf8(&output).expect("error frame is utf8");
    assert!(text.contains("event: error\n"));
    assert!(text.contains("\"api_error\""));
    assert!(text.contains("matching checksum"));
    assert!(transform.seen_events().is_empty());
    let events = wait_for_events(storage.as_ref(), 1).await?;
    assert_eq!(events.len(), 1);
    assert_eq!(
        events[0].upstream_error_type.as_deref(),
        Some("upstream_response_decode_error")
    );
    Ok(())
}

async fn gzip_transform_failure_preserves_transform_error() -> Result<(), Box<dyn std::error::Error>>
{
    let transform = Arc::new(FailAfterFirstSseTransform::default());
    let chunks = gzip_event_chunks(
        b"event: ping\ndata: {\"type\":\"ping\"}\n\n",
        b"event: ping\ndata: {\"type\":\"ping\"}\n\n",
    );
    let mut headers = sse_headers();
    headers.insert(CONTENT_ENCODING, HeaderValue::from_static("gzip"));
    let dir = tempfile::tempdir()?;
    let storage = Arc::new(sqlite_storage(&dir, "gzip-transform-failure.sqlite").await?);
    let test_bus =
        TestLifecycleBus::new().with_assembler(Arc::clone(&storage) as Arc<dyn StorageTrait>);
    let lifecycle = lifecycle_with_transforms(
        None,
        Some(transform),
        Arc::new(ChunkedDispatch {
            status: StatusCode::OK,
            headers,
            chunks,
        }),
    )
    .with_event_bus(test_bus.bus_arc());

    let request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[],"stream":true}"#,
    ));
    let response = handle_authenticated(&lifecycle, request)
        .await
        .expect("lifecycle handles gzip transform failure");
    let (_status, _headers, body) = collect_body(response).await;

    let text = std::str::from_utf8(&body).expect("SSE body is utf8");
    assert_eq!(text.matches("event: error\n").count(), 1);
    assert!(text.contains("response_transform_error"));
    assert!(!text.contains("\"api_error\""));

    let events = wait_for_events(storage.as_ref(), 1).await?;
    assert_eq!(events.len(), 1);
    assert_eq!(
        events[0].upstream_error_type.as_deref(),
        Some("response_transform_error")
    );
    // #663: cc-lb aborted the upstream body locally (third chunk never read),
    // so the decoder finish failure must not be recorded as upstream evidence.
    assert_eq!(events[0].upstream_http_version, None);
    assert_eq!(events[0].upstream_content_encoding, None);
    assert_eq!(events[0].upstream_body_bytes, None);
    assert_eq!(events[0].upstream_body_end, None);
    assert_eq!(events[0].upstream_body_error_cause, None);
    assert_eq!(events[0].upstream_body_error_io_kind, None);
    assert_eq!(events[0].upstream_body_error_h2_reason, None);
    assert_eq!(events[0].upstream_stream_warning_type, None);
    assert_eq!(events[0].upstream_stream_warning_message, None);
    Ok(())
}

async fn raw_deflate_passthrough_decode_failure_remains_success()
-> Result<(), Box<dyn std::error::Error>> {
    let plaintext = Bytes::from_static(b"event: ping\ndata: {\"type\":\"ping\"}\n\n");
    let body = raw_deflate_bytes(&plaintext);
    let mut headers = sse_headers();
    headers.insert(CONTENT_ENCODING, HeaderValue::from_static("deflate"));
    let dir = tempfile::tempdir()?;
    let storage = Arc::new(sqlite_storage(&dir, "raw-deflate-passthrough.sqlite").await?);
    let test_bus =
        TestLifecycleBus::new().with_assembler(Arc::clone(&storage) as Arc<dyn StorageTrait>);
    let lifecycle = lifecycle_with_transforms(
        None,
        None,
        Arc::new(FixedDispatch {
            status: StatusCode::OK,
            headers,
            body: body.clone(),
        }),
    )
    .with_event_bus(test_bus.bus_arc());

    let request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[],"stream":true}"#,
    ));
    let response = handle_authenticated(&lifecycle, request)
        .await
        .expect("lifecycle handles raw deflate passthrough");
    let (_status, headers, output) = collect_body(response).await;

    assert_eq!(output, body);
    assert_eq!(
        headers.get(CONTENT_ENCODING),
        Some(&HeaderValue::from_static("deflate"))
    );
    let events = wait_for_events(storage.as_ref(), 1).await?;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].error_code, None);
    assert_eq!(events[0].upstream_error_type, None);
    Ok(())
}

async fn non_sse_gzip_decode_failure_skips_sse_transform() -> Result<(), Box<dyn std::error::Error>>
{
    let transform = Arc::new(UnchangedSseTransform::default());
    let body = incomplete_gzip_prefix(&Bytes::from_static(br#"{"partial":true}"#));
    let headers = json_headers(body.len(), true);
    let dir = tempfile::tempdir()?;
    let storage = Arc::new(sqlite_storage(&dir, "non-sse-gzip.sqlite").await?);
    let test_bus =
        TestLifecycleBus::new().with_assembler(Arc::clone(&storage) as Arc<dyn StorageTrait>);
    let lifecycle = lifecycle_with_transforms(
        None,
        Some(transform.clone()),
        Arc::new(FixedDispatch {
            status: StatusCode::OK,
            headers,
            body: body.clone(),
        }),
    )
    .with_static_limit_subject(
        LimitEngine::new(
            Arc::new(KeyConcurrencyManager::new()),
            Arc::new(cc_lb_clock::SystemClock),
        ),
        "principal-test".to_owned(),
        "key-test".to_owned(),
        StoredApiKeyRecord {
            key_hash_b64: "key-test".to_owned(),
            status: KeyStatus::Active,
            ..StoredApiKeyRecord::default()
        },
    )
    .with_event_bus(test_bus.bus_arc());

    let request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[],"stream":true}"#,
    ));
    let response = handle_authenticated(&lifecycle, request)
        .await
        .expect("lifecycle handles non-SSE gzip response");
    let (_status, headers, output) = collect_body(response).await;

    assert_eq!(output, body);
    assert_eq!(
        headers.get(CONTENT_ENCODING),
        Some(&HeaderValue::from_static("gzip"))
    );
    assert!(transform.seen_events().is_empty());
    let events = wait_for_events(storage.as_ref(), 1).await?;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].error_code, None);
    assert_eq!(events[0].upstream_error_type, None);
    Ok(())
}

async fn sse_unsupported_encoding_skips_transform_and_raw_passes_through() {
    let transform = Arc::new(SseToolNameTransform::default());
    let body = sse_upstream_body(false);
    let mut headers = sse_headers();
    headers.insert(CONTENT_ENCODING, HeaderValue::from_static("snappy"));
    let lifecycle = lifecycle_with_transforms(
        None,
        Some(transform.clone()),
        Arc::new(FixedDispatch {
            status: StatusCode::OK,
            headers,
            body: body.clone(),
        }),
    );

    let request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[],"stream":true}"#,
    ));
    let response = handle_authenticated(&lifecycle, request)
        .await
        .expect("lifecycle handles unsupported encoded SSE response");
    let (_status, headers, output) = collect_body(response).await;

    assert_eq!(output, body);
    assert_eq!(
        headers.get(CONTENT_ENCODING),
        Some(&HeaderValue::from_static("snappy"))
    );
    assert!(transform.seen_events().is_empty());
}

async fn sse_transform_failure_before_output_fails_open_raw() {
    let transform = Arc::new(AlwaysFailingSseTransform);
    let lifecycle = lifecycle_with_transforms(None, Some(transform), sse_dispatch(false));

    let request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[],"stream":false}"#,
    ));
    let response = handle_authenticated(&lifecycle, request)
        .await
        .expect("lifecycle handles SSE response");
    let (_status, _headers, body) = collect_body(response).await;

    assert_eq!(body, sse_upstream_body(false));
}

async fn sse_transform_fail_open_preserves_keepalive_observation() {
    let body = Bytes::from_static(
        b"event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_failopen_keepalive\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[],\"usage\":{\"input_tokens\":3,\"output_tokens\":5,\"cache_read_input_tokens\":12000}}}\n\n\
          event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"tool_use\",\"id\":\"toolu_failopen\",\"name\":\"Bash\",\"input\":{}}}\n\n\
          event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"}}\n\n\
          event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
    );
    let enqueuer = Arc::new(TransformedQuestionEnqueuer::default());
    let lifecycle = lifecycle_with_record_config_and_keepalive(
        None,
        Some(Arc::new(AlwaysFailingSseTransform)),
        Arc::new(FixedDispatch {
            status: StatusCode::OK,
            headers: sse_headers(),
            body: body.clone(),
        }),
        LifecycleConfig::default(),
        default_upstream_record(),
        Some(CacheKeepaliveConfig {
            enabled: true,
            refresh_lead_time_5m_secs: 30,
            refresh_lead_time_1h_secs: 300,
            max_refreshes_per_session: 12,
            max_total_duration_secs: 14_400,
            snapshot_max_bytes: 524_288,
            min_cache_tokens: 10_000,
            classifier: ClassifierConfig::default(),
        }),
    )
    .with_cache_keepalive_enqueuer(Arc::clone(&enqueuer) as Arc<dyn CacheKeepaliveEnqueuer>);
    let mut request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","max_tokens":32,"system":[{"type":"text","text":"cached","cache_control":{"type":"ephemeral","ttl":"5m"}}],"messages":[{"role":"user","content":"hello"}],"stream":true}"#,
    ));
    request.headers_mut().insert(
        "x-claude-code-session-id",
        HeaderValue::from_static("fail-open-keepalive"),
    );
    let response = handle_authenticated(&lifecycle, request)
        .await
        .expect("fail-open response");
    let (_status, _headers, downstream) = collect_body(response).await;
    assert_eq!(downstream, body);

    tokio::time::timeout(Duration::from_secs(5), enqueuer.completed.notified())
        .await
        .expect("keepalive classification completes");
    assert_eq!(
        enqueuer.enqueued.lock().expect("enqueue lock").len(),
        1,
        "raw SSE fail-open must still produce a keep-alive snapshot"
    );
}

async fn sse_unterminated_final_event_is_upstream_framing_error() {
    let transform = Arc::new(UnchangedSseTransform::default());
    let body = Bytes::from_static(b"event: message_stop\ndata: {}\n");
    let lifecycle = lifecycle_with_transforms(
        None,
        Some(transform.clone()),
        Arc::new(FixedDispatch {
            status: StatusCode::OK,
            headers: sse_headers(),
            body: body.clone(),
        }),
    );

    let request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[],"stream":true}"#,
    ));
    let response = handle_authenticated(&lifecycle, request)
        .await
        .expect("lifecycle handles unterminated identity SSE event");
    let (_status, _headers, output) = collect_body(response).await;

    assert_ne!(output, body);
    let text = std::str::from_utf8(&output).expect("error frame is utf8");
    assert!(text.contains("event: error\n"));
    assert!(text.contains(r#""type":"api_error""#));
    assert!(text.contains("incomplete event before transform output"));
    assert!(transform.seen_events().is_empty());
}

async fn sse_unterminated_passthrough_is_upstream_framing_error()
-> Result<(), Box<dyn std::error::Error>> {
    let body = Bytes::from_static(b"event: message_stop\ndata: {}\n");
    let dir = tempfile::tempdir()?;
    let storage = Arc::new(sqlite_storage(&dir, "unterminated-passthrough.sqlite").await?);
    let test_bus =
        TestLifecycleBus::new().with_assembler(Arc::clone(&storage) as Arc<dyn StorageTrait>);
    let lifecycle = lifecycle_with_transforms(
        None,
        None,
        Arc::new(FixedDispatch {
            status: StatusCode::OK,
            headers: sse_headers(),
            body: body.clone(),
        }),
    )
    .with_event_bus(test_bus.bus_arc());

    let request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[],"stream":true}"#,
    ));
    let response = handle_authenticated(&lifecycle, request)
        .await
        .expect("lifecycle handles unterminated passthrough SSE event");
    let (_status, _headers, output) = collect_body(response).await;

    let text = std::str::from_utf8(&output).expect("SSE output is utf8");
    let error_offset = text.find("event: error\n").expect("error frame emitted");
    assert!(
        text[..error_offset].ends_with("\n\n"),
        "error frame must start after an SSE event boundary: {text:?}",
    );
    let events = wait_for_events(storage.as_ref(), 1).await?;
    assert_eq!(events.len(), 1);
    assert_eq!(
        events[0].error_code.as_deref(),
        Some("upstream_stream_error")
    );
    assert_eq!(
        events[0].upstream_error_type.as_deref(),
        Some("upstream_response_framing_error")
    );
    Ok(())
}

async fn sse_trailing_line_terminators_remain_success() -> Result<(), Box<dyn std::error::Error>> {
    let body =
        Bytes::from_static(b"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n\r\n");
    let dir = tempfile::tempdir()?;
    let storage = Arc::new(sqlite_storage(&dir, "trailing-terminators.sqlite").await?);
    let test_bus =
        TestLifecycleBus::new().with_assembler(Arc::clone(&storage) as Arc<dyn StorageTrait>);
    let lifecycle = lifecycle_with_transforms(
        None,
        None,
        Arc::new(FixedDispatch {
            status: StatusCode::OK,
            headers: sse_headers(),
            body: body.clone(),
        }),
    )
    .with_event_bus(test_bus.bus_arc());

    let request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[],"stream":true}"#,
    ));
    let response = handle_authenticated(&lifecycle, request)
        .await
        .expect("lifecycle handles trailing SSE line terminators");
    let (_status, _headers, output) = collect_body(response).await;

    assert_eq!(output, body);
    let events = wait_for_events(storage.as_ref(), 1).await?;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].error_code, None);
    assert_eq!(events[0].upstream_error_type, None);
    Ok(())
}

async fn sse_body_failure_after_parser_budget_starts_separate_error_frame() {
    let mut body = Vec::from(&b"data: "[..]);
    body.extend(std::iter::repeat_n(b'x', 256));
    let config = LifecycleConfig {
        messages_body_cap_bytes: 128,
        ..LifecycleConfig::default()
    };
    let lifecycle = lifecycle_with_transforms_and_config(
        None,
        None,
        Arc::new(BodyThenFrameErrorDispatch {
            status: StatusCode::OK,
            headers: sse_headers(),
            body: Bytes::from(body),
        }),
        config,
    );

    let request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[],"stream":true}"#,
    ));
    let response = handle_authenticated(&lifecycle, request)
        .await
        .expect("lifecycle handles body failure after parser budget");
    let (_status, _headers, output) = collect_body(response).await;

    let text = std::str::from_utf8(&output).expect("SSE output is utf8");
    let error_offset = text.find("event: error\n").expect("error frame emitted");
    assert!(
        text[..error_offset].ends_with("\n\n"),
        "error frame must start after an SSE event boundary: {text:?}",
    );
    assert_eq!(text.matches("event: error\n").count(), 1);
}

async fn sse_gzip_unterminated_final_event_emits_upstream_framing_error() {
    let transform = Arc::new(UnchangedSseTransform::default());
    let compressed = gzip_bytes(&Bytes::from_static(b"event: message_stop\ndata: {}\n"));
    let mut headers = sse_headers();
    headers.insert(CONTENT_ENCODING, HeaderValue::from_static("gzip"));
    let lifecycle = lifecycle_with_transforms(
        None,
        Some(transform.clone()),
        Arc::new(FixedDispatch {
            status: StatusCode::OK,
            headers,
            body: compressed.clone(),
        }),
    );

    let request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[],"stream":true}"#,
    ));
    let response = handle_authenticated(&lifecycle, request)
        .await
        .expect("lifecycle handles unterminated compressed SSE event");
    let (_status, headers, output) = collect_body(response).await;

    assert!(headers.get(CONTENT_ENCODING).is_none());
    assert_ne!(output, compressed);
    let text = std::str::from_utf8(&output).expect("error frame is utf8");
    assert!(text.contains("event: error\n"));
    assert!(text.contains(r#""type":"api_error""#));
    assert!(text.contains("incomplete event before transform output"));
    assert!(transform.seen_events().is_empty());
}

async fn sse_unterminated_http_error_preserves_upstream_status()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let storage = Arc::new(sqlite_storage(&dir, "unterminated-http-error.sqlite").await?);
    let test_bus =
        TestLifecycleBus::new().with_assembler(Arc::clone(&storage) as Arc<dyn StorageTrait>);
    let transform = Arc::new(UnchangedSseTransform::default());
    let body = Bytes::from_static(
        b"event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"rate_limit_error\",\"message\":\"forced fake rate limit response\"}}\n",
    );
    let lifecycle = lifecycle_with_transforms(
        None,
        Some(transform),
        Arc::new(FixedDispatch {
            status: StatusCode::TOO_MANY_REQUESTS,
            headers: sse_headers(),
            body,
        }),
    )
    .with_event_bus(test_bus.bus_arc());

    let request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[],"stream":true}"#,
    ));
    let response = handle_authenticated(&lifecycle, request)
        .await
        .expect("lifecycle handles unterminated HTTP error SSE event");
    let (status, _headers, output) = collect_body(response).await;

    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    let text = std::str::from_utf8(&output).expect("error frame is utf8");
    assert!(text.contains(r#""type":"api_error""#));
    let events = wait_for_events(storage.as_ref(), 1).await?;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].status, StatusCode::TOO_MANY_REQUESTS.as_u16());
    assert_eq!(events[0].error_code.as_deref(), Some("upstream_4xx"));
    assert_eq!(
        events[0].upstream_error_type.as_deref(),
        Some("upstream_response_framing_error")
    );
    Ok(())
}

async fn sse_unterminated_http_error_passthrough_preserves_upstream_body()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let storage = Arc::new(sqlite_storage(&dir, "unterminated-http-passthrough.sqlite").await?);
    let test_bus =
        TestLifecycleBus::new().with_assembler(Arc::clone(&storage) as Arc<dyn StorageTrait>);
    let body = Bytes::from_static(
        b"event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"rate_limit_error\",\"message\":\"forced fake rate limit response\"}}\n",
    );
    let lifecycle = lifecycle_with_transforms(
        None,
        None,
        Arc::new(FixedDispatch {
            status: StatusCode::TOO_MANY_REQUESTS,
            headers: sse_headers(),
            body: body.clone(),
        }),
    )
    .with_event_bus(test_bus.bus_arc());

    let request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[],"stream":true}"#,
    ));
    let response = handle_authenticated(&lifecycle, request)
        .await
        .expect("lifecycle handles unterminated HTTP error passthrough");
    let (status, _headers, output) = collect_body(response).await;

    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(output, body);
    let events = wait_for_events(storage.as_ref(), 1).await?;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].error_code.as_deref(), Some("upstream_4xx"));
    assert_ne!(
        events[0].upstream_error_type.as_deref(),
        Some("upstream_response_framing_error"),
    );
    assert!(
        events[0]
            .upstream_error_message
            .as_deref()
            .is_some_and(|message| message.contains("forced fake rate limit response")),
    );
    Ok(())
}

async fn sse_gzip_transform_failure_before_output_emits_error_frame() {
    assert_gzip_transform_failure_before_output(Arc::new(AlwaysFailingSseTransform)).await;
}

async fn sse_gzip_transform_failure_preserves_http_error_status()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let storage = Arc::new(sqlite_storage(&dir, "transform-http-error.sqlite").await?);
    let test_bus =
        TestLifecycleBus::new().with_assembler(Arc::clone(&storage) as Arc<dyn StorageTrait>);
    let compressed = gzip_bytes(&sse_upstream_body(false));
    let mut headers = sse_headers();
    headers.insert(CONTENT_ENCODING, HeaderValue::from_static("gzip"));
    let lifecycle = lifecycle_with_transforms(
        None,
        Some(Arc::new(AlwaysFailingSseTransform)),
        Arc::new(FixedDispatch {
            status: StatusCode::TOO_MANY_REQUESTS,
            headers,
            body: compressed,
        }),
    )
    .with_event_bus(test_bus.bus_arc());

    let request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[],"stream":true}"#,
    ));
    let response = handle_authenticated(&lifecycle, request)
        .await
        .expect("lifecycle handles transform failure on HTTP error response");
    let (status, _headers, output) = collect_body(response).await;

    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    let text = std::str::from_utf8(&output).expect("error frame is utf8");
    assert!(text.contains("response_transform_error"));
    let events = wait_for_events(storage.as_ref(), 1).await?;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].status, StatusCode::TOO_MANY_REQUESTS.as_u16());
    assert_eq!(events[0].error_code.as_deref(), Some("upstream_4xx"));
    assert_eq!(
        events[0].upstream_error_type.as_deref(),
        Some("response_transform_error")
    );
    Ok(())
}

async fn sse_gzip_invalid_event_name_before_output_emits_error_frame() {
    assert_gzip_transform_failure_before_output(Arc::new(InvalidEventNameSseTransform)).await;
}

async fn assert_gzip_transform_failure_before_output(transform: Arc<dyn SseEventTransformHook>) {
    let compressed = gzip_bytes(&sse_upstream_body(false));
    let mut headers = sse_headers();
    headers.insert(CONTENT_ENCODING, HeaderValue::from_static("gzip"));
    let lifecycle = lifecycle_with_transforms(
        None,
        Some(transform),
        Arc::new(FixedDispatch {
            status: StatusCode::OK,
            headers,
            body: compressed.clone(),
        }),
    );

    let request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[],"stream":true}"#,
    ));
    let response = handle_authenticated(&lifecycle, request)
        .await
        .expect("lifecycle handles compressed SSE transform failure");
    let (_status, headers, output) = collect_body(response).await;

    assert!(headers.get(CONTENT_ENCODING).is_none());
    assert_ne!(output, compressed);
    let text = std::str::from_utf8(&output).expect("error frame is utf8");
    assert!(text.contains("event: error\n"));
    assert!(text.contains("response_transform_error"));
    assert!(
        !text.contains("upstream SSE response could not be safely processed"),
        "non-gated transform failures must preserve the detailed transform error frame: {text:?}",
    );
}

async fn sse_transform_invalid_event_name_fails_open_without_injection() {
    let transform = Arc::new(InvalidEventNameSseTransform);
    let lifecycle = lifecycle_with_transforms(None, Some(transform), sse_dispatch(false));

    let request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[],"stream":false}"#,
    ));
    let response = handle_authenticated(&lifecycle, request)
        .await
        .expect("lifecycle handles SSE response");
    let (_status, _headers, body) = collect_body(response).await;

    assert_eq!(body, sse_upstream_body(false));
    let text = std::str::from_utf8(&body).expect("sse body is utf8");
    assert!(!text.contains("event: injected"));
}

async fn sse_transform_failure_after_transformed_output_terminates_classified() {
    let transform = Arc::new(FailAfterFirstSseTransform::default());
    let lifecycle = lifecycle_with_transforms(None, Some(transform), sse_dispatch(false));

    let request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[],"stream":false}"#,
    ));
    let response = handle_authenticated(&lifecycle, request)
        .await
        .expect("lifecycle handles SSE response");
    let (_status, _headers, body) = collect_body(response).await;

    let text = std::str::from_utf8(&body).expect("sse body is utf8");
    assert!(text.contains(r#""name":"bash""#));
    assert!(text.contains("event: error\n"));
    assert!(text.contains("response_transform_error"));
    assert!(!text.contains("message_stop"));
}

async fn sse_decompression_budget_before_output_emits_transform_error() {
    let transform = Arc::new(UnchangedSseTransform::default());
    let plaintext = vec![b'x'; 1024];
    let compressed = gzip_bytes(&Bytes::from(plaintext));
    let mut headers = sse_headers();
    headers.insert(CONTENT_ENCODING, HeaderValue::from_static("gzip"));
    let config = LifecycleConfig {
        messages_body_cap_bytes: 128,
        ..LifecycleConfig::default()
    };
    let lifecycle = lifecycle_with_transforms_and_config(
        None,
        Some(transform),
        Arc::new(FixedDispatch {
            status: StatusCode::OK,
            headers,
            body: compressed,
        }),
        config,
    );

    let request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[],"stream":true}"#,
    ));
    let response = handle_authenticated(&lifecycle, request)
        .await
        .expect("lifecycle handles over-budget gzip SSE response");
    let (_status, headers, output) = collect_body(response).await;

    assert!(headers.get(CONTENT_ENCODING).is_none());
    let text = std::str::from_utf8(&output).expect("error frame is utf8");
    assert!(text.contains("event: error\n"));
    assert!(text.contains("response_transform_error"));
    assert!(text.contains("before transform output"));
}

async fn sse_gzip_incomplete_event_budget_before_output_emits_transform_error() {
    let transform = Arc::new(UnchangedSseTransform::default());
    let mut plaintext = Vec::from(&b"data: "[..]);
    plaintext.extend(std::iter::repeat_n(b'x', 256));
    let compressed = gzip_bytes(&Bytes::from(plaintext));
    let mut headers = sse_headers();
    headers.insert(CONTENT_ENCODING, HeaderValue::from_static("gzip"));
    let config = LifecycleConfig {
        messages_body_cap_bytes: 128,
        ..LifecycleConfig::default()
    };
    let lifecycle = lifecycle_with_transforms_and_config(
        None,
        Some(transform),
        Arc::new(FixedDispatch {
            status: StatusCode::OK,
            headers,
            body: compressed,
        }),
        config,
    );

    let request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[],"stream":true}"#,
    ));
    let response = handle_authenticated(&lifecycle, request)
        .await
        .expect("lifecycle handles compressed incomplete SSE event");
    let (_status, headers, output) = collect_body(response).await;

    assert!(headers.get(CONTENT_ENCODING).is_none());
    let text = std::str::from_utf8(&output).expect("error frame is utf8");
    assert!(text.contains("event: error\n"));
    assert!(text.contains("response_transform_error"));
    assert!(text.contains("incomplete SSE event"));
    assert!(text.contains("before transform output"));
}

async fn sse_brotli_incomplete_event_budget_never_replays_compressed_bytes() {
    let transform = Arc::new(UnchangedSseTransform::default());
    let mut plaintext = Vec::from(&b"data: "[..]);
    plaintext.extend(std::iter::repeat_n(b'x', 256));
    let compressed = brotli_bytes(&Bytes::from(plaintext));
    let mut headers = sse_headers();
    headers.insert(CONTENT_ENCODING, HeaderValue::from_static("br"));
    let config = LifecycleConfig {
        messages_body_cap_bytes: 128,
        ..LifecycleConfig::default()
    };
    let lifecycle = lifecycle_with_transforms_and_config(
        None,
        Some(transform),
        Arc::new(FixedDispatch {
            status: StatusCode::OK,
            headers,
            body: compressed.clone(),
        }),
        config,
    );

    let request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[],"stream":true}"#,
    ));
    let response = handle_authenticated(&lifecycle, request)
        .await
        .expect("lifecycle handles brotli incomplete SSE event");
    let (_status, headers, output) = collect_body(response).await;

    assert!(headers.get(CONTENT_ENCODING).is_none());
    assert_ne!(output, compressed);
    let text = std::str::from_utf8(&output).expect("error frame is utf8");
    assert!(text.contains("event: error\n"));
    assert!(text.contains("response_transform_error"));
    assert!(text.contains("incomplete SSE event"));
}

async fn sse_incomplete_event_budget_after_output_uses_existing_error_frame() {
    let transform = Arc::new(UnchangedSseTransform::default());
    let mut body = Vec::from(&b"event: ping\ndata: {\"type\":\"ping\"}\n\n"[..]);
    body.extend_from_slice(b"data: ");
    body.extend(std::iter::repeat_n(b'x', 256));
    let config = LifecycleConfig {
        messages_body_cap_bytes: 128,
        ..LifecycleConfig::default()
    };
    let lifecycle = lifecycle_with_transforms_and_config(
        None,
        Some(transform),
        Arc::new(FixedDispatch {
            status: StatusCode::OK,
            headers: sse_headers(),
            body: Bytes::from(body),
        }),
        config,
    );

    let request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[],"stream":true}"#,
    ));
    let response = handle_authenticated(&lifecycle, request)
        .await
        .expect("lifecycle handles over-budget incomplete SSE event");
    let (_status, _headers, output) = collect_body(response).await;
    let text = std::str::from_utf8(&output).expect("output is SSE text");

    assert!(text.starts_with("event: ping\n"));
    assert!(text.contains("event: error\n"));
    assert!(text.contains("response_transform_error"));
    assert!(!text.contains(&"x".repeat(256)));
}

async fn sse_accounting_uses_pre_transform_usage() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let storage = Arc::new(sqlite_storage(&dir, "sse-accounting.sqlite").await?);
    let test_bus =
        TestLifecycleBus::new().with_assembler(Arc::clone(&storage) as Arc<dyn StorageTrait>);
    let transform = Arc::new(SseUsageMutatingTransform);
    let lifecycle = lifecycle_with_transforms(None, Some(transform), sse_dispatch(true))
        .with_event_bus(test_bus.bus_arc());

    let request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[],"stream":false}"#,
    ));
    let response = handle_authenticated(&lifecycle, request)
        .await
        .expect("lifecycle handles SSE response");
    let (_status, _headers, body) = collect_body(response).await;

    let text = std::str::from_utf8(&body).expect("sse body is utf8");
    assert!(text.contains(r#""input_tokens":999"#));
    let events = wait_for_events(storage.as_ref(), 1).await?;
    assert_eq!(events.len(), 1);
    assert_eq!(
        (events[0].input_tokens, events[0].output_tokens),
        (Some(7), Some(11))
    );
    Ok(())
}

async fn buffered_transformed_question_is_user_turn_without_keepalive_enqueue() {
    transformed_question_classification(false, QuestionTransformMode::Rename).await;
}

async fn sse_transformed_question_is_user_turn_without_keepalive_enqueue() {
    transformed_question_classification(true, QuestionTransformMode::Rename).await;
}

async fn sse_dropped_question_is_not_renewed() {
    transformed_question_classification(true, QuestionTransformMode::Drop).await;
}

async fn sse_explicitly_dropped_question_is_not_renewed() {
    transformed_question_classification(true, QuestionTransformMode::DropVariant).await;
}

async fn sse_multi_event_question_is_user_turn_without_keepalive_enqueue() {
    transformed_question_classification(true, QuestionTransformMode::Multiple).await;
}

async fn failed_open_question_transform_preserves_original_body_and_usage() {
    for streaming in [false, true] {
        let original = if streaming {
            Bytes::from_static(
                b"event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_failopen\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[],\"usage\":{\"input_tokens\":3,\"output_tokens\":5,\"cache_read_input_tokens\":12000}}}\n\nevent: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"tool_use\",\"id\":\"tool_question\",\"name\":\"Question\",\"input\":{}}}\n\nevent: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"}}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
            )
        } else {
            Bytes::from_static(
                br#"{"type":"message","content":[{"type":"tool_use","id":"tool_question","name":"Question","input":{}}],"stop_reason":"tool_use","usage":{"input_tokens":3,"output_tokens":5,"cache_read_input_tokens":12000}}"#,
            )
        };
        let bus = TestLifecycleBus::new();
        let mut events = bus.bus.subscribe_lifecycle();
        let lifecycle = lifecycle_with_transforms(
            if streaming {
                None
            } else {
                Some(Arc::new(FailingBufferedTransform))
            },
            if streaming {
                Some(Arc::new(AlwaysFailingSseTransform))
            } else {
                None
            },
            Arc::new(FixedDispatch {
                status: StatusCode::OK,
                headers: if streaming {
                    sse_headers()
                } else {
                    json_headers(original.len(), false)
                },
                body: original.clone(),
            }),
        )
        .with_event_bus(bus.bus_arc());
        let response = handle_authenticated(
            &lifecycle,
            messages_request(Bytes::from_static(
                br#"{"model":"claude-test","messages":[]}"#,
            )),
        )
        .await
        .expect("fail-open response");
        let (status, _, downstream) = collect_body(response).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            downstream, original,
            "fail-open must preserve every raw upstream byte"
        );
        let mut observed_usage = None;
        let mut completed = false;
        while let Ok(event) = events.try_recv() {
            match event {
                LifecycleEvent::UsageObserved { usage, .. } => observed_usage = Some(usage),
                LifecycleEvent::StreamCompleted { result, .. } => {
                    completed = true;
                    if streaming {
                        let success = result.expect("identity SSE fail-open remains successful");
                        assert_eq!(success.body_bytes, Some(original.len() as u64));
                        observed_usage = Some(success.usage);
                    } else {
                        assert_eq!(
                            result
                                .expect_err("buffered failure remains observable")
                                .error_type,
                            "response_transform_error"
                        );
                    }
                }
                _ => {}
            }
        }
        assert!(
            completed,
            "real lifecycle must complete its accounting observation"
        );
        let usage = observed_usage.expect("original upstream usage must remain observable");
        assert_eq!(
            (
                usage.input_tokens,
                usage.output_tokens,
                usage.cache_read_input_tokens
            ),
            (3, 5, 12_000),
            "the provider snapshot before fail-open must not be replaced or discarded"
        );
    }
}

#[derive(Clone, Copy)]
enum QuestionTransformMode {
    Rename,
    Drop,
    DropVariant,
    Multiple,
}

async fn transformed_question_classification(streaming: bool, mode: QuestionTransformMode) {
    let message = Bytes::from_static(
        br#"{"id":"msg_question","type":"message","role":"assistant","content":[{"type":"tool_use","id":"tool_question","name":"Question","input":{}}],"stop_reason":"tool_use","usage":{"input_tokens":3,"output_tokens":5,"cache_read_input_tokens":12000}}"#,
    );
    let upstream_body = if streaming {
        Bytes::from_static(
            b"event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_question\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[],\"usage\":{\"input_tokens\":3,\"output_tokens\":0,\"cache_read_input_tokens\":12000}}}\n\nevent: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"tool_use\",\"id\":\"tool_question\",\"name\":\"Question\",\"input\":{}}}\n\nevent: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"},\"usage\":{\"output_tokens\":5}}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
        )
    } else {
        message
    };
    let dispatch = Arc::new(FixedDispatch {
        status: StatusCode::OK,
        headers: if streaming {
            sse_headers()
        } else {
            json_headers(upstream_body.len(), false)
        },
        body: upstream_body.clone(),
    });
    let hook = Arc::new(QuestionToLowercaseTransform(mode));
    let enqueuer = Arc::new(TransformedQuestionEnqueuer::default());
    let lifecycle = lifecycle_with_record_config_and_keepalive(
        if streaming {
            None
        } else {
            Some(hook.clone() as Arc<dyn ResponseTransformHook>)
        },
        if streaming {
            Some(hook as Arc<dyn SseEventTransformHook>)
        } else {
            None
        },
        dispatch,
        LifecycleConfig::default(),
        default_upstream_record(),
        Some(CacheKeepaliveConfig {
            enabled: true,
            refresh_lead_time_5m_secs: 30,
            refresh_lead_time_1h_secs: 300,
            max_refreshes_per_session: 12,
            max_total_duration_secs: 14_400,
            snapshot_max_bytes: 524_288,
            min_cache_tokens: 10_000,
            classifier: ClassifierConfig::default(),
        }),
    )
    .with_cache_keepalive_enqueuer(Arc::clone(&enqueuer) as Arc<dyn CacheKeepaliveEnqueuer>);
    let mut request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","max_tokens":32,"system":[{"type":"text","text":"cached","cache_control":{"type":"ephemeral","ttl":"5m"}}],"messages":[{"role":"user","content":"hello"}]}"#,
    ));
    request.headers_mut().insert(
        "x-claude-code-session-id",
        HeaderValue::from_static("transformed-question"),
    );
    let response = handle_authenticated(&lifecycle, request)
        .await
        .expect("question response");
    let (status, _, downstream) = collect_body(response).await;
    assert_eq!(status, StatusCode::OK);
    match mode {
        QuestionTransformMode::Rename => assert_eq!(
            downstream,
            replace_bytes(
                &upstream_body,
                br#""name":"Question""#,
                br#""name":"question""#
            ),
            "public hook must emit the client-facing wait-for-user tool"
        ),
        QuestionTransformMode::Drop | QuestionTransformMode::DropVariant => {
            let text = std::str::from_utf8(&downstream).expect("dropped SSE utf8");
            assert!(!text.contains("content_block_start"));
            assert!(text.contains("message_stop"));
        }
        QuestionTransformMode::Multiple => {
            let text = std::str::from_utf8(&downstream).expect("multiple SSE utf8");
            assert_eq!(text.matches("event: content_block_start").count(), 2);
            assert!(text.contains(r#""name":"question""#));
            assert!(text.contains(r#""name":"ask""#));
            assert!(!text.contains(r#""name":"Question""#));
        }
    }
    tokio::time::timeout(Duration::from_secs(5), enqueuer.completed.notified())
        .await
        .expect("keepalive classification completes");
    assert!(
        enqueuer.enqueued.lock().expect("enqueue lock").is_empty(),
        "a client waiting for question input must not schedule a renewal"
    );
    assert_eq!(
        enqueuer.not_tracked.lock().expect("not tracked lock").len(),
        1
    );
    assert!(
        enqueuer
            .cancelled
            .lock()
            .expect("cancel lock")
            .iter()
            .any(|request| request.reason == CancelReason::UserTurnDetected),
        "the lifecycle must persist its non-agent cancellation"
    );
}

struct QuestionToLowercaseTransform(QuestionTransformMode);

impl ResponseTransformHook for QuestionToLowercaseTransform {
    fn transform_response(
        &self,
        request: TransformResponseRequest,
    ) -> Result<TransformResponseResult, ResponseTransformError> {
        Ok(TransformResponseResult::Replace {
            status: None,
            headers: None,
            body: Some(replace_bytes(
                &request.body,
                br#""name":"Question""#,
                br#""name":"question""#,
            )),
        })
    }
}

impl SseEventTransformHook for QuestionToLowercaseTransform {
    fn transform_sse_event(
        &self,
        request: TransformSseEventRequest,
    ) -> Result<TransformSseEventResult, ResponseTransformError> {
        if request.event.event != "content_block_start" {
            return Ok(TransformSseEventResult::Unchanged);
        }
        if matches!(self.0, QuestionTransformMode::DropVariant) {
            return Ok(TransformSseEventResult::Drop);
        }
        if matches!(self.0, QuestionTransformMode::Drop) {
            return Ok(TransformSseEventResult::Replace { events: vec![] });
        }
        let mut events = vec![SseEvent {
            event: request.event.event.clone(),
            data: replace_bytes(
                &request.event.data,
                br#""name":"Question""#,
                br#""name":"question""#,
            ),
        }];
        if matches!(self.0, QuestionTransformMode::Multiple) {
            events.push(SseEvent {
                event: request.event.event,
                data: Bytes::from_static(
                    br#"{"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"tool_ask","name":"ask","input":{}}}"#,
                ),
            });
        }
        Ok(TransformSseEventResult::Replace { events })
    }
}

#[derive(Default)]
struct TransformedQuestionEnqueuer {
    enqueued: Mutex<Vec<CacheKeepaliveEnqueueRequest>>,
    cancelled: Mutex<Vec<CacheKeepaliveCancelRequest>>,
    not_tracked: Mutex<Vec<CacheKeepaliveNotTrackedRequest>>,
    completed: tokio::sync::Notify,
}

#[async_trait]
impl CacheKeepaliveEnqueuer for TransformedQuestionEnqueuer {
    async fn enqueue_cache_keepalive(
        &self,
        request: CacheKeepaliveEnqueueRequest,
    ) -> Result<(), CacheKeepaliveEnqueueError> {
        self.enqueued.lock().expect("enqueue lock").push(request);
        self.completed.notify_one();
        Ok(())
    }

    async fn cancel_cache_keepalive(
        &self,
        request: CacheKeepaliveCancelRequest,
    ) -> Result<(), CacheKeepaliveEnqueueError> {
        self.cancelled.lock().expect("cancel lock").push(request);
        if !self
            .not_tracked
            .lock()
            .expect("not tracked lock")
            .is_empty()
        {
            self.completed.notify_one();
        }
        Ok(())
    }

    async fn record_cache_keepalive_not_tracked(
        &self,
        request: CacheKeepaliveNotTrackedRequest,
    ) -> Result<(), CacheKeepaliveEnqueueError> {
        self.not_tracked
            .lock()
            .expect("not tracked lock")
            .push(request);
        Ok(())
    }
}

async fn handle_authenticated(
    lifecycle: &Lifecycle,
    request: http::Request<Bytes>,
) -> Result<Response<Body>, ProxyError> {
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("test request authenticates");
    lifecycle.handle(request, &auth).await
}

fn openai_audio_request(path: &str) -> http::Request<Bytes> {
    let (content_type, body) = if path == "/v1/audio/transcriptions" {
        (
            "multipart/form-data; boundary=audio",
            Bytes::from_static(
                b"--audio\r\nContent-Disposition: form-data; name=\"model\"\r\n\r\nwhisper-1\r\n--audio--\r\n",
            ),
        )
    } else {
        (
            "application/json",
            Bytes::from_static(br#"{"model":"tts-1","input":"hello","voice":"alloy"}"#),
        )
    };
    let mut request = http::Request::builder()
        .method("POST")
        .uri(path)
        .header("x-api-key", common::managed_api_key())
        .header(CONTENT_TYPE, content_type)
        .body(body)
        .expect("OpenAI audio request builds");
    request
        .extensions_mut()
        .insert(cc_lb_engine::ProviderScope(cc_lb_domain::Provider::Openai));
    request
}

fn lifecycle_with_transforms(
    response_transform: Option<Arc<dyn ResponseTransformHook>>,
    sse_transform: Option<Arc<dyn SseEventTransformHook>>,
    dispatcher: Arc<dyn UpstreamDispatch>,
) -> Lifecycle {
    lifecycle_with_transforms_and_config(
        response_transform,
        sse_transform,
        dispatcher,
        LifecycleConfig::default(),
    )
}

fn lifecycle_with_transforms_and_config(
    response_transform: Option<Arc<dyn ResponseTransformHook>>,
    sse_transform: Option<Arc<dyn SseEventTransformHook>>,
    dispatcher: Arc<dyn UpstreamDispatch>,
    config: LifecycleConfig,
) -> Lifecycle {
    lifecycle_with_record_and_config(
        response_transform,
        sse_transform,
        dispatcher,
        config,
        default_upstream_record(),
    )
}

fn lifecycle_with_record(
    response_transform: Option<Arc<dyn ResponseTransformHook>>,
    dispatcher: Arc<dyn UpstreamDispatch>,
    record: UpstreamRecord,
) -> Lifecycle {
    lifecycle_with_record_and_config(
        response_transform,
        None,
        dispatcher,
        LifecycleConfig::default(),
        record,
    )
}

fn lifecycle_with_record_and_config(
    response_transform: Option<Arc<dyn ResponseTransformHook>>,
    sse_transform: Option<Arc<dyn SseEventTransformHook>>,
    dispatcher: Arc<dyn UpstreamDispatch>,
    config: LifecycleConfig,
    record: UpstreamRecord,
) -> Lifecycle {
    lifecycle_with_options(
        response_transform,
        sse_transform,
        dispatcher,
        config,
        record,
        true,
        Vec::new(),
    )
}

fn lifecycle_with_record_config_and_keepalive(
    response_transform: Option<Arc<dyn ResponseTransformHook>>,
    sse_transform: Option<Arc<dyn SseEventTransformHook>>,
    dispatcher: Arc<dyn UpstreamDispatch>,
    config: LifecycleConfig,
    record: UpstreamRecord,
    cache_keepalive: Option<CacheKeepaliveConfig>,
) -> Lifecycle {
    lifecycle_with_options_and_keepalive(
        response_transform,
        sse_transform,
        dispatcher,
        config,
        record,
        true,
        Vec::new(),
        cache_keepalive,
    )
}

fn lifecycle_with_options(
    response_transform: Option<Arc<dyn ResponseTransformHook>>,
    sse_transform: Option<Arc<dyn SseEventTransformHook>>,
    dispatcher: Arc<dyn UpstreamDispatch>,
    config: LifecycleConfig,
    record: UpstreamRecord,
    refresh_allowed: bool,
    limits: Vec<cc_lb_storage_api::principal::Limit>,
) -> Lifecycle {
    lifecycle_with_options_and_keepalive(
        response_transform,
        sse_transform,
        dispatcher,
        config,
        record,
        refresh_allowed,
        limits,
        None,
    )
}

#[allow(clippy::too_many_arguments)]
fn lifecycle_with_options_and_keepalive(
    response_transform: Option<Arc<dyn ResponseTransformHook>>,
    sse_transform: Option<Arc<dyn SseEventTransformHook>>,
    dispatcher: Arc<dyn UpstreamDispatch>,
    config: LifecycleConfig,
    record: UpstreamRecord,
    refresh_allowed: bool,
    limits: Vec<cc_lb_storage_api::principal::Limit>,
    cache_keepalive: Option<CacheKeepaliveConfig>,
) -> Lifecycle {
    let dialect = Arc::new(ShapeTransformDialect::new(
        response_transform,
        sse_transform,
    ));
    let state = TestState::default();
    let mut chains: HashMap<String, PrincipalRoutingArtifacts> = HashMap::new();
    chains.insert(
        "principal-test".to_owned(),
        (
            Some(Arc::new(RouterPipelineCache::empty(
                TerminalStrategy::FirstPick,
            ))),
            DialectCache::Explicit(ShapePluginCache { dialect }),
        ),
    );
    let principal = PrincipalRecord {
        id: Uuid::new_v4(),
        name: "principal-test".to_owned(),
        kind: cc_lb_storage_api::PrincipalKind::Machine,
        allowed_models: vec!["*".to_owned()],
        allowed_upstreams: vec![record.id],
        default_limits: limits,
        enabled: true,
        deleted_at_unix_secs: None,
        revision: 1,
        created_at_unix_secs: 0,
        updated_at_unix_secs: 0,
        router_terminal_strategy: Default::default(),
        cache_keepalive,
    };
    let mut authn = TestAuthn::with_principal_view(
        state,
        Arc::new(PrincipalView::from_db(&[principal], chains)),
    );
    authn.refresh_allowed = refresh_allowed;
    let view = DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(authn.clone()))
        .principal_view(authn.principal_view.clone())
        .upstream_records(vec![record])
        .build();
    Lifecycle::new_with_dynamic_view(
        authn.authn,
        Arc::new(DynamicViewHolder::new(view)),
        dispatcher,
        config,
        Arc::new(cc_lb_clock::SystemClock),
    )
}

fn with_test_limit_subject(lifecycle: Lifecycle) -> Lifecycle {
    lifecycle.with_static_limit_subject(
        LimitEngine::new(
            Arc::new(KeyConcurrencyManager::new()),
            Arc::new(cc_lb_clock::SystemClock),
        ),
        "principal-test".to_owned(),
        "key-test".to_owned(),
        StoredApiKeyRecord {
            key_hash_b64: "key-test".to_owned(),
            status: KeyStatus::Active,
            ..StoredApiKeyRecord::default()
        },
    )
}

struct ShapeTransformDialect {
    response_transform: Option<Arc<dyn ResponseTransformHook>>,
    sse_transform: Option<Arc<dyn SseEventTransformHook>>,
}

impl ShapeTransformDialect {
    fn new(
        response_transform: Option<Arc<dyn ResponseTransformHook>>,
        sse_transform: Option<Arc<dyn SseEventTransformHook>>,
    ) -> Self {
        Self {
            response_transform,
            sse_transform,
        }
    }
}

impl UpstreamDialect for ShapeTransformDialect {
    fn shape(
        &self,
        context: &DialectShapeContext,
        _upstream: &Upstream,
        _principal: &Principal,
        builder: &mut ShapedRequestBuilder,
    ) -> Result<ShapedRequest, DialectError> {
        let mut url = Url::parse("http://upstream.local/").expect("test URL parses");
        url.set_path(context.path.trim_start_matches('/'));
        url.set_query(context.query.as_deref());
        Ok(builder.shaped_request(
            url,
            context.method.clone(),
            context.downstream_headers.clone(),
            context.body_bytes.clone(),
        ))
    }

    fn response_transform_hook(&self) -> Option<&dyn ResponseTransformHook> {
        self.response_transform.as_deref()
    }

    fn sse_event_transform_hook(&self) -> Option<&dyn SseEventTransformHook> {
        self.sse_transform.as_deref()
    }
}

fn buffered_dispatch() -> Arc<dyn UpstreamDispatch> {
    Arc::new(FixedDispatch {
        status: StatusCode::OK,
        headers: with_upstream_unified_headers(json_headers(buffered_upstream_body().len(), false)),
        body: buffered_upstream_body(),
    })
}

fn sse_dispatch(with_usage_mutation_case: bool) -> Arc<dyn UpstreamDispatch> {
    Arc::new(FixedDispatch {
        status: StatusCode::OK,
        headers: sse_headers(),
        body: sse_upstream_body(with_usage_mutation_case),
    })
}

struct FixedDispatch {
    status: StatusCode,
    headers: HeaderMap,
    body: Bytes,
}

#[async_trait]
impl UpstreamDispatch for FixedDispatch {
    async fn dispatch(&self, _request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        let mut response = Response::new(Body::from(self.body.clone()));
        *response.status_mut() = self.status;
        *response.headers_mut() = self.headers.clone();
        Ok(response)
    }
}

struct SseFirstEventThenPendingDispatch;

#[async_trait]
impl UpstreamDispatch for SseFirstEventThenPendingDispatch {
    async fn dispatch(&self, _request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        let stream = async_stream::stream! {
            yield Ok::<Bytes, std::convert::Infallible>(Bytes::from_static(
                b"event: ping\ndata: {\"type\":\"ping\"}\n\n",
            ));
            std::future::pending::<()>().await;
        };
        let mut response = Response::new(Body::from_stream(stream));
        *response.headers_mut() = sse_headers();
        Ok(response)
    }
}

struct BinaryFirstChunkThenGatedDispatch {
    content_type: Option<&'static str>,
    release: Arc<tokio::sync::Notify>,
}

#[async_trait]
impl UpstreamDispatch for BinaryFirstChunkThenGatedDispatch {
    async fn dispatch(&self, _request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        let release = self.release.clone();
        let stream = async_stream::stream! {
            yield Ok::<Bytes, std::convert::Infallible>(Bytes::from_static(b"\xfffirst-chunk"));
            release.notified().await;
            yield Ok::<Bytes, std::convert::Infallible>(Bytes::from_static(b"\x00last-chunk"));
        };
        let mut response = Response::new(Body::from_stream(stream));
        if let Some(content_type) = self.content_type {
            response
                .headers_mut()
                .insert(CONTENT_TYPE, HeaderValue::from_static(content_type));
        }
        Ok(response)
    }
}

struct ChunkedDispatch {
    status: StatusCode,
    headers: HeaderMap,
    chunks: Vec<Bytes>,
}

#[async_trait]
impl UpstreamDispatch for ChunkedDispatch {
    async fn dispatch(&self, _request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        let chunks = self.chunks.clone();
        let stream = async_stream::stream! {
            for chunk in chunks {
                yield Ok::<Bytes, std::convert::Infallible>(chunk);
            }
        };
        let mut response = Response::new(Body::from_stream(stream));
        *response.status_mut() = self.status;
        *response.headers_mut() = self.headers.clone();
        Ok(response)
    }
}

struct BodyThenFrameErrorDispatch {
    status: StatusCode,
    headers: HeaderMap,
    body: Bytes,
}

#[async_trait]
impl UpstreamDispatch for BodyThenFrameErrorDispatch {
    async fn dispatch(&self, _request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        let body = self.body.clone();
        let stream = async_stream::stream! {
            yield Ok::<Bytes, std::io::Error>(body);
            yield Err(std::io::Error::other("forced upstream frame error"));
        };
        let mut response = Response::new(Body::from_stream(stream));
        *response.status_mut() = self.status;
        *response.headers_mut() = self.headers.clone();
        Ok(response)
    }
}

#[derive(Default)]
struct BufferedToolNameTransform {
    seen: Mutex<Vec<Bytes>>,
}

impl BufferedToolNameTransform {
    fn seen_bodies(&self) -> Vec<Bytes> {
        self.seen.lock().expect("seen lock").clone()
    }
}

impl ResponseTransformHook for BufferedToolNameTransform {
    fn transform_response(
        &self,
        request: TransformResponseRequest,
    ) -> Result<TransformResponseResult, ResponseTransformError> {
        self.seen
            .lock()
            .expect("seen lock")
            .push(request.body.clone());
        let body = replace_bytes(&request.body, br#""name":"Bash""#, br#""name":"bash""#);
        Ok(TransformResponseResult::Replace {
            status: None,
            headers: Some(spoofed_plugin_headers(body.len())),
            body: Some(body),
        })
    }
}

struct BufferedHeaderPolicyTransform {
    headers: Option<HeaderMap>,
    seen_headers: Mutex<Vec<HeaderMap>>,
}

impl ResponseTransformHook for BufferedHeaderPolicyTransform {
    fn transform_response(
        &self,
        request: TransformResponseRequest,
    ) -> Result<TransformResponseResult, ResponseTransformError> {
        self.seen_headers
            .lock()
            .expect("seen headers lock")
            .push(request.response_headers);
        Ok(TransformResponseResult::Replace {
            status: None,
            headers: self.headers.clone(),
            body: Some(replace_bytes(
                &request.body,
                br#""name":"Bash""#,
                br#""name":"bash""#,
            )),
        })
    }
}

struct FailingBufferedTransform;

impl ResponseTransformHook for FailingBufferedTransform {
    fn transform_response(
        &self,
        _request: TransformResponseRequest,
    ) -> Result<TransformResponseResult, ResponseTransformError> {
        Err(ResponseTransformError::Trap {
            reason: "forced buffered failure".to_owned(),
        })
    }
}

struct BufferedUsageMutatingTransform;

impl ResponseTransformHook for BufferedUsageMutatingTransform {
    fn transform_response(
        &self,
        _request: TransformResponseRequest,
    ) -> Result<TransformResponseResult, ResponseTransformError> {
        Ok(TransformResponseResult::Replace {
            status: None,
            headers: None,
            body: Some(Bytes::from_static(
                br#"{"type":"message","usage":{"input_tokens":999,"output_tokens":999},"content":[]}"#,
            )),
        })
    }
}

#[derive(Default)]
struct UnchangedBufferedTransform {
    seen: Mutex<Vec<Bytes>>,
}

impl UnchangedBufferedTransform {
    fn seen_bodies(&self) -> Vec<Bytes> {
        self.seen.lock().expect("seen lock").clone()
    }
}

impl ResponseTransformHook for UnchangedBufferedTransform {
    fn transform_response(
        &self,
        request: TransformResponseRequest,
    ) -> Result<TransformResponseResult, ResponseTransformError> {
        self.seen
            .lock()
            .expect("seen lock")
            .push(request.body.clone());
        Ok(TransformResponseResult::Unchanged)
    }
}

#[derive(Default)]
struct HeaderOnlyBufferedTransform {
    seen: Mutex<Vec<Bytes>>,
}

impl HeaderOnlyBufferedTransform {
    fn seen_bodies(&self) -> Vec<Bytes> {
        self.seen.lock().expect("seen lock").clone()
    }
}

impl ResponseTransformHook for HeaderOnlyBufferedTransform {
    fn transform_response(
        &self,
        request: TransformResponseRequest,
    ) -> Result<TransformResponseResult, ResponseTransformError> {
        self.seen
            .lock()
            .expect("seen lock")
            .push(request.body.clone());
        let mut headers = HeaderMap::new();
        headers.insert("x-plugin-header", HeaderValue::from_static("kept"));
        headers.insert("x-api-key", HeaderValue::from_static("secret"));
        Ok(TransformResponseResult::Replace {
            status: None,
            headers: Some(headers),
            body: None,
        })
    }
}

#[derive(Default)]
struct SseToolNameTransform {
    seen: Mutex<Vec<String>>,
}

impl SseToolNameTransform {
    fn seen_events(&self) -> Vec<String> {
        self.seen.lock().expect("seen lock").clone()
    }
}

impl SseEventTransformHook for SseToolNameTransform {
    fn transform_sse_event(
        &self,
        request: TransformSseEventRequest,
    ) -> Result<TransformSseEventResult, ResponseTransformError> {
        self.seen
            .lock()
            .expect("seen lock")
            .push(request.event.event.clone());
        if request.event.event == "content_block_start" {
            return Ok(TransformSseEventResult::Replace {
                events: vec![SseEvent {
                    event: request.event.event,
                    data: replace_bytes(
                        &request.event.data,
                        br#""name":"Bash""#,
                        br#""name":"bash""#,
                    ),
                }],
            });
        }
        Ok(TransformSseEventResult::Unchanged)
    }
}

#[derive(Default)]
struct GeminiSseMarkerTransform {
    seen: Mutex<Vec<String>>,
}

impl GeminiSseMarkerTransform {
    fn seen_events(&self) -> Vec<String> {
        self.seen.lock().expect("seen lock").clone()
    }
}

impl SseEventTransformHook for GeminiSseMarkerTransform {
    fn transform_sse_event(
        &self,
        request: TransformSseEventRequest,
    ) -> Result<TransformSseEventResult, ResponseTransformError> {
        self.seen
            .lock()
            .expect("seen lock")
            .push(request.event.event.clone());
        Ok(TransformSseEventResult::Replace {
            events: vec![SseEvent {
                event: request.event.event,
                data: replace_bytes(&request.event.data, b"hello", b"HELLO"),
            }],
        })
    }
}

#[derive(Default)]
struct UnchangedSseTransform {
    seen: Mutex<Vec<String>>,
}

impl UnchangedSseTransform {
    fn seen_events(&self) -> Vec<String> {
        self.seen.lock().expect("seen lock").clone()
    }
}

impl SseEventTransformHook for UnchangedSseTransform {
    fn transform_sse_event(
        &self,
        request: TransformSseEventRequest,
    ) -> Result<TransformSseEventResult, ResponseTransformError> {
        self.seen
            .lock()
            .expect("seen lock")
            .push(request.event.event);
        Ok(TransformSseEventResult::Unchanged)
    }
}

#[derive(Default)]
struct HeaderCapturingSseTransform {
    seen_headers: Mutex<Vec<HeaderMap>>,
}

impl HeaderCapturingSseTransform {
    fn seen_headers(&self) -> Vec<HeaderMap> {
        self.seen_headers.lock().expect("seen headers lock").clone()
    }
}

impl SseEventTransformHook for HeaderCapturingSseTransform {
    fn transform_sse_event(
        &self,
        request: TransformSseEventRequest,
    ) -> Result<TransformSseEventResult, ResponseTransformError> {
        self.seen_headers
            .lock()
            .expect("seen headers lock")
            .push(request.response_headers);
        if request.event.event == "content_block_start" {
            return Ok(TransformSseEventResult::Replace {
                events: vec![SseEvent {
                    event: request.event.event,
                    data: replace_bytes(
                        &request.event.data,
                        br#""name":"Bash""#,
                        br#""name":"bash""#,
                    ),
                }],
            });
        }
        Ok(TransformSseEventResult::Unchanged)
    }
}

struct AlwaysFailingSseTransform;

impl SseEventTransformHook for AlwaysFailingSseTransform {
    fn transform_sse_event(
        &self,
        _request: TransformSseEventRequest,
    ) -> Result<TransformSseEventResult, ResponseTransformError> {
        Err(ResponseTransformError::Runtime {
            reason: "forced sse failure".to_owned(),
        })
    }
}

#[derive(Default)]
struct FailAfterFirstSseTransform {
    calls: Mutex<u64>,
}

impl SseEventTransformHook for FailAfterFirstSseTransform {
    fn transform_sse_event(
        &self,
        request: TransformSseEventRequest,
    ) -> Result<TransformSseEventResult, ResponseTransformError> {
        let mut calls = self.calls.lock().expect("calls lock");
        *calls = calls.saturating_add(1);
        if *calls > 1 {
            return Err(ResponseTransformError::Runtime {
                reason: "forced post-output failure".to_owned(),
            });
        }
        Ok(TransformSseEventResult::Replace {
            events: vec![SseEvent {
                event: request.event.event,
                data: replace_bytes(
                    &request.event.data,
                    br#""name":"Bash""#,
                    br#""name":"bash""#,
                ),
            }],
        })
    }
}

struct SseUsageMutatingTransform;

impl SseEventTransformHook for SseUsageMutatingTransform {
    fn transform_sse_event(
        &self,
        request: TransformSseEventRequest,
    ) -> Result<TransformSseEventResult, ResponseTransformError> {
        if request.event.event == "message_stop" {
            return Ok(TransformSseEventResult::Replace {
                events: vec![SseEvent {
                    event: request.event.event,
                    data: Bytes::from_static(
                        br#"{"type":"message_stop","usage":{"input_tokens":999,"output_tokens":999}}"#,
                    ),
                }],
            });
        }
        Ok(TransformSseEventResult::Unchanged)
    }
}

struct InvalidEventNameSseTransform;

impl SseEventTransformHook for InvalidEventNameSseTransform {
    fn transform_sse_event(
        &self,
        request: TransformSseEventRequest,
    ) -> Result<TransformSseEventResult, ResponseTransformError> {
        Ok(TransformSseEventResult::Replace {
            events: vec![SseEvent {
                event: format!("{}\nevent: injected", request.event.event),
                data: request.event.data,
            }],
        })
    }
}

fn buffered_upstream_body() -> Bytes {
    Bytes::from_static(
        br#"{"type":"message","usage":{"input_tokens":3,"output_tokens":5},"content":[{"type":"tool_use","id":"toolu_1","name":"Bash","input":{}}]}"#,
    )
}

fn sse_upstream_body(with_usage_mutation_case: bool) -> Bytes {
    let usage = if with_usage_mutation_case {
        r#"{"type":"message_stop","usage":{"input_tokens":7,"output_tokens":11}}"#
    } else {
        r#"{"type":"message_stop"}"#
    };
    Bytes::from(format!(
        "event: content_block_start\ndata: {{\"type\":\"content_block_start\",\"index\":0,\"content_block\":{{\"type\":\"tool_use\",\"id\":\"toolu_1\",\"name\":\"Bash\",\"input\":{{}}}}}}\n\nevent: message_stop\ndata: {usage}\n\n"
    ))
}

fn replace_bytes(input: &Bytes, needle: &[u8], replacement: &[u8]) -> Bytes {
    let mut output = Vec::with_capacity(input.len());
    let mut cursor = input.as_ref();
    while let Some(index) = cursor
        .windows(needle.len())
        .position(|window| window == needle)
    {
        output.extend_from_slice(&cursor[..index]);
        output.extend_from_slice(replacement);
        cursor = &cursor[index + needle.len()..];
    }
    output.extend_from_slice(cursor);
    Bytes::from(output)
}

fn json_headers(content_len: usize, compressed: bool) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    headers.insert(
        CONTENT_LENGTH,
        HeaderValue::from_str(&content_len.to_string()).expect("content length header"),
    );
    if compressed {
        headers.insert(CONTENT_ENCODING, HeaderValue::from_static("gzip"));
    }
    headers
}

fn spoofed_plugin_headers(content_len: usize) -> HeaderMap {
    let mut headers = json_headers(content_len, true);
    headers.insert("connection", HeaderValue::from_static("x-sneaky"));
    headers.insert("x-sneaky", HeaderValue::from_static("remove-me"));
    headers.insert("x-cc-lb-spoof", HeaderValue::from_static("remove-me"));
    headers.insert("x-cc-lb-secret", HeaderValue::from_static("remove-me"));
    headers.insert("x-ratelimit-limit", HeaderValue::from_static("remove-me"));
    headers.insert("authorization", HeaderValue::from_static("Bearer secret"));
    headers.insert("x-api-key", HeaderValue::from_static("secret"));
    headers.insert(
        "anthropic-ratelimit-requests-limit",
        HeaderValue::from_static("1"),
    );
    headers.insert("x-plugin-header", HeaderValue::from_static("kept"));
    headers.insert(
        "anthropic-ratelimit-unified-status",
        HeaderValue::from_static("plugin-override"),
    );
    headers.insert(
        "anthropic-ratelimit-unified-plugin-injected",
        HeaderValue::from_static("plugin-injection"),
    );
    headers.insert(
        "anthropic-ratelimit-unified-hop",
        HeaderValue::from_static("plugin-hop-injection"),
    );
    headers
}

fn with_upstream_unified_headers(mut headers: HeaderMap) -> HeaderMap {
    for (name, value) in &upstream_unified_header_fixture() {
        headers.append(name.clone(), value.clone());
    }
    headers
}

fn upstream_unified_header_fixture() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        "anthropic-ratelimit-unified-status",
        HeaderValue::from_static("allowed"),
    );
    headers.append(
        "anthropic-ratelimit-unified-future-bucket-remaining",
        HeaderValue::from_bytes(b"first\t\x80opaque").expect("legal opaque header bytes"),
    );
    headers.append(
        "anthropic-ratelimit-unified-future-bucket-remaining",
        HeaderValue::from_static(" second value "),
    );
    headers.append(
        "anthropic-ratelimit-unified-future-bucket-remaining",
        HeaderValue::from_bytes(b"first\t\x80opaque").expect("legal duplicate opaque value"),
    );
    headers
}

fn add_rate_limit_header_fixture(headers: &mut HeaderMap, include_unified: bool) {
    if include_unified {
        for (name, value) in &upstream_unified_header_fixture() {
            headers.append(name.clone(), value.clone());
        }
    }
    for name in [
        "anthropic-ratelimit-requests-limit",
        "anthropic-ratelimit-unified",
        "anthropic-ratelimit-unifiedish-status",
        "x-ratelimit-limit",
        "x-cc-lb-secret",
        "authorization",
        "proxy-authorization",
        "x-api-key",
    ] {
        headers.insert(name, HeaderValue::from_static("remove-me"));
    }
    headers.insert(
        "connection",
        HeaderValue::from_static("AnThRoPiC-RaTeLiMiT-UnIfIeD-HoP, x-hop"),
    );
    headers.insert(
        "anthropic-ratelimit-unified-hop",
        HeaderValue::from_static("do-not-restore"),
    );
    headers.insert("x-hop", HeaderValue::from_static("remove-me"));
    headers.insert("x-safe-header", HeaderValue::from_static("keep-me"));
}

fn assert_client_rate_limit_headers(headers: &HeaderMap, include_unified: bool) {
    let expected = if include_unified {
        upstream_unified_header_fixture()
    } else {
        HeaderMap::new()
    };
    for name in expected.keys() {
        assert_eq!(
            headers
                .get_all(name)
                .iter()
                .map(HeaderValue::as_bytes)
                .collect::<Vec<_>>(),
            expected
                .get_all(name)
                .iter()
                .map(HeaderValue::as_bytes)
                .collect::<Vec<_>>(),
            "original raw values and per-name order for {name}"
        );
    }
    assert_eq!(
        headers
            .keys()
            .filter(|name| name.as_str().starts_with("anthropic-ratelimit-unified-"))
            .count(),
        expected.keys().count(),
        "no missing or synthesized unified header names"
    );
    for name in [
        "anthropic-ratelimit-requests-limit",
        "anthropic-ratelimit-unified",
        "anthropic-ratelimit-unifiedish-status",
        "anthropic-ratelimit-unified-hop",
        "x-ratelimit-limit",
        "x-cc-lb-secret",
        "x-cc-lb-spoof",
        "authorization",
        "proxy-authorization",
        "x-api-key",
        "connection",
        "x-hop",
        "x-sneaky",
    ] {
        assert!(headers.get(name).is_none(), "{name} remains removed");
    }
}

fn assert_plugin_headers_redacted(headers: &HeaderMap) {
    assert!(
        headers
            .keys()
            .all(|name| !name.as_str().starts_with("anthropic-ratelimit-"))
    );
    for name in [
        CONTENT_ENCODING.as_str(),
        CONTENT_LENGTH.as_str(),
        "x-ratelimit-limit",
        "x-cc-lb-secret",
        "authorization",
        "proxy-authorization",
        "x-api-key",
        "connection",
        "x-hop",
    ] {
        assert!(headers.get(name).is_none(), "plugin must not see {name}");
    }
    assert_eq!(
        headers.get("x-safe-header"),
        Some(&HeaderValue::from_static("keep-me"))
    );
}

fn gzip_bytes(body: &Bytes) -> Bytes {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(body).expect("gzip write succeeds");
    Bytes::from(encoder.finish().expect("gzip finish succeeds"))
}

fn brotli_bytes(body: &Bytes) -> Bytes {
    let mut compressed = Vec::new();
    {
        let mut encoder = brotli::CompressorWriter::new(&mut compressed, 4096, 5, 22);
        encoder.write_all(body).expect("brotli write succeeds");
    }
    Bytes::from(compressed)
}

fn incomplete_gzip_prefix(body: &Bytes) -> Bytes {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(body).expect("gzip write succeeds");
    encoder.flush().expect("gzip flush succeeds");
    Bytes::copy_from_slice(encoder.get_ref())
}

fn raw_deflate_bytes(body: &Bytes) -> Bytes {
    let mut encoder =
        flate2::write::DeflateEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(body).expect("deflate write succeeds");
    Bytes::from(encoder.finish().expect("deflate finish succeeds"))
}

fn gzip_event_chunks(first: &[u8], second: &[u8]) -> Vec<Bytes> {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(first).expect("first gzip event writes");
    encoder.flush().expect("first gzip event flushes");
    let first_end = encoder.get_ref().len();
    encoder.write_all(second).expect("second gzip event writes");
    encoder.flush().expect("second gzip event flushes");
    let second_end = encoder.get_ref().len();
    let compressed = encoder.finish().expect("gzip finish succeeds");
    vec![
        Bytes::copy_from_slice(&compressed[..first_end]),
        Bytes::copy_from_slice(&compressed[first_end..second_end]),
        Bytes::copy_from_slice(&compressed[second_end..]),
    ]
}

/// A complete Anthropic message stream: usage in `message_start` and
/// `message_delta`, one tool_use block, and `message_stop`.
fn complete_message_sse_body() -> Bytes {
    Bytes::from_static(
        b"event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_663\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-test\",\"content\":[],\"usage\":{\"input_tokens\":3,\"output_tokens\":1}}}\n\nevent: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"tool_use\",\"id\":\"toolu_1\",\"name\":\"Bash\",\"input\":{}}}\n\nevent: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"},\"usage\":{\"output_tokens\":11}}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
    )
}

/// A complete gzip member with its 8-byte CRC32/ISIZE trailer removed.
fn gzip_without_trailer(body: &Bytes) -> Bytes {
    let compressed = gzip_bytes(body);
    compressed.slice(..compressed.len() - 8)
}

/// A complete gzip member whose CRC32 trailer no longer matches the payload.
fn gzip_with_corrupt_crc(body: &Bytes) -> Bytes {
    let mut compressed = gzip_bytes(body).to_vec();
    let crc_offset = compressed.len() - 8;
    compressed[crc_offset] ^= 0xff;
    Bytes::from(compressed)
}

fn gzip_sse_headers() -> HeaderMap {
    let mut headers = sse_headers();
    headers.insert(CONTENT_ENCODING, HeaderValue::from_static("gzip"));
    headers
}

fn fixed_gzip_sse_dispatch(status: StatusCode, body: Bytes) -> Arc<dyn UpstreamDispatch> {
    Arc::new(FixedDispatch {
        status,
        headers: gzip_sse_headers(),
        body,
    })
}

/// Yields `chunks`, then optionally fails the body with `trailing_error`.
struct ScriptedDispatch {
    version: http::Version,
    headers: HeaderMap,
    chunks: Vec<Bytes>,
    trailing_error: Option<std::io::ErrorKind>,
}

impl ScriptedDispatch {
    fn gzip_sse(chunks: Vec<Bytes>, trailing_error: Option<std::io::ErrorKind>) -> Self {
        Self {
            version: http::Version::HTTP_11,
            headers: gzip_sse_headers(),
            chunks,
            trailing_error,
        }
    }

    fn identity_sse(chunks: Vec<Bytes>, trailing_error: Option<std::io::ErrorKind>) -> Self {
        Self {
            version: http::Version::HTTP_11,
            headers: sse_headers(),
            chunks,
            trailing_error,
        }
    }
}

#[async_trait]
impl UpstreamDispatch for ScriptedDispatch {
    async fn dispatch(&self, _request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        let chunks = self.chunks.clone();
        let trailing_error = self.trailing_error;
        let stream = async_stream::stream! {
            for chunk in chunks {
                yield Ok::<Bytes, std::io::Error>(chunk);
            }
            if let Some(kind) = trailing_error {
                yield Err(std::io::Error::from(kind));
            }
        };
        let mut response = Response::new(Body::from_stream(stream));
        *response.status_mut() = StatusCode::OK;
        *response.version_mut() = self.version;
        *response.headers_mut() = self.headers.clone();
        Ok(response)
    }
}

fn sse_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("text/event-stream"));
    headers
}

fn default_upstream_id() -> Uuid {
    Uuid::parse_str("00000000-0000-0000-0000-000000000001").expect("default upstream id parses")
}

fn default_upstream_record() -> UpstreamRecord {
    UpstreamRecord {
        id: default_upstream_id(),
        name: "test-upstream".to_owned(),
        kind: StorageUpstreamKind::AnthropicApiKey,
        base_url: Some(Url::parse("http://upstream.local/").expect("test URL parses")),
        enabled: true,
        oauth_credentials: None,
        oauth_never_refresh: false,
        api_key_ciphertext: Some(Vec::new()),
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
        workspace_id: None,
    }
}

fn upstream_record(kind: StorageUpstreamKind, base_url: &str) -> UpstreamRecord {
    let mut record = default_upstream_record();
    record.kind = kind;
    record.base_url = Some(Url::parse(base_url).expect("provider base URL parses"));
    record
}

fn gemini_generate_request() -> http::Request<Bytes> {
    http::Request::builder()
        .method("POST")
        .uri("/v1beta/models/gemini-test:generateContent")
        .header("x-api-key", common::managed_api_key())
        .body(Bytes::from_static(br#"{"contents":[]}"#))
        .expect("Gemini request builds")
}

fn _principal_for_doc() -> Principal {
    Principal {
        id: "principal-test".to_owned(),
        kind: PrincipalKind::ApiKey,
    }
}

#[tokio::test]
async fn response_transform_paths_suite_part_1() {
    buffered_transform_rewrites_tool_name_and_sanitizes_headers().await;
    buffered_transform_preserves_only_original_upstream_unified_headers().await;
    compressed_sse_affinity_preserves_original_upstream_unified_headers().await;
    sse_transform_preserves_upstream_unified_headers_without_exposing_them_to_plugin().await;
    buffered_gemini_oauth_code_assist_response_is_unwrapped().await;
    gemini_oauth_sse_unwraps_before_custom_transform().await;
    buffered_native_gemini_response_bytes_remain_unchanged().await;
    buffered_anthropic_response_bytes_remain_unchanged().await;
    buffered_transform_rewrites_upstream_error_response().await;
    terminal_unauthorized_without_refresh_uses_buffered_transform().await;
    terminal_unauthorized_generated_body_skips_transform_when_refresh_declined().await;
    stream_request_json_errors_use_buffered_transform().await;
    oversized_unauthorized_stream_body_skips_transform_of_generated_error().await;
    non_sse_stream_transform_never_receives_a_capped_prefix().await;
    gemini_json_array_stream_is_transformed_once_as_a_whole().await;
    gemini_oauth_non_sse_stream_unwraps_before_buffered_transform().await;
    gemini_oauth_stream_sse_error_starting_with_brace_stays_sse().await;
    gemini_oauth_stream_json_error_reaches_client_unchanged().await;
    ingress_cap_does_not_limit_existing_response_paths().await;
    audio_json_response_preserves_the_ordinary_buffered_path().await;
    non_json_response_relays_first_chunk_before_upstream_eof().await;
    non_json_buffered_transform_is_bounded_without_truncating_body().await;
    genuine_sse_uses_event_hook_without_waiting_for_body_eof().await;
    buffered_unchanged_preserves_compressed_upstream_bytes().await;
}

#[tokio::test]
async fn response_transform_paths_suite_part_2() {
    buffered_header_only_transform_preserves_compressed_upstream_bytes().await;
    buffered_unsupported_encoding_skips_transform().await;
    buffered_transform_failure_fails_open_to_original_response().await;
    buffered_transform_failure_is_observed_as_error()
        .await
        .unwrap();
    buffered_http_error_preserves_canonical_error_over_transform_failure()
        .await
        .unwrap();
    buffered_accounting_uses_pre_transform_usage()
        .await
        .unwrap();
    sse_transform_rewrites_content_block_start_event().await;
    sse_transform_receives_sanitized_response_headers().await;
    sse_unchanged_transform_preserves_raw_event_bytes().await;
    sse_transform_decodes_gzip_and_emits_identity_sse().await;
    gzip_content_length_passthrough_preserves_upstream_bytes_and_headers().await;
    content_length_client_stops_after_declared_gzip_bytes_without_terminal_drop().await;
    content_length_affinity_gated_sse_finalizes_success_before_final_bytes().await;
    affinity_gated_sse_downstream_drop_before_end_records_client_closed().await;
    content_length_malformed_sse_delivers_raw_bytes_before_error_telemetry().await;
    truncated_gzip_passthrough_is_recorded_as_upstream_decode_error()
        .await
        .unwrap();
    sse_transform_decodes_brotli_and_emits_identity_sse().await;
    truncated_gzip_after_transformed_output_is_upstream_decode_error()
        .await
        .unwrap();
    sse_transform_gzip_trailer_missing_after_message_stop_is_accepted_with_warning()
        .await
        .unwrap();
    sse_transform_gzip_crc_mismatch_after_message_stop_is_indistinguishable_from_missing_trailer()
        .await
        .unwrap();
    incomplete_trailing_event_after_message_stop_with_gzip_failure_stays_fatal()
        .await
        .unwrap();
    gzip_trailer_failure_after_message_stop_with_trailing_line_terminators_is_accepted()
        .await
        .unwrap();
}

#[tokio::test]
async fn response_transform_paths_suite_part_3() {
    gzip_body_error_after_message_stop_then_finish_failure_is_single_warning()
        .await
        .unwrap();
    gzip_body_error_before_message_stop_stays_fatal_with_attribution()
        .await
        .unwrap();
    uncompressed_sse_body_error_after_message_stop_is_accepted_with_warning()
        .await
        .unwrap();
    uncompressed_sse_body_error_after_message_stop_with_partial_event_stays_fatal()
        .await
        .unwrap();
    compressed_passthrough_trailer_failure_after_message_stop_keeps_error()
        .await
        .unwrap();
    http_error_sse_with_gzip_trailer_failure_keeps_http_classification()
        .await
        .unwrap();
    provider_error_then_gzip_trailer_failure_is_not_accepted()
        .await
        .unwrap();
    event_named_message_stop_with_delta_payload_and_gzip_failure_stays_fatal()
        .await
        .unwrap();
    event_named_message_stop_with_malformed_payload_and_gzip_failure_stays_fatal()
        .await
        .unwrap();
    name_only_message_stop_without_json_type_and_gzip_failure_stays_fatal()
        .await
        .unwrap();
    upstream_diagnostics_record_http2_version_request_id_and_content_length()
        .await
        .unwrap();
    clean_gzip_sse_stream_records_no_upstream_diagnostics()
        .await
        .unwrap();
    truncated_gzip_before_transformed_output_emits_decode_error()
        .await
        .unwrap();
    gzip_transform_failure_preserves_transform_error()
        .await
        .unwrap();
    raw_deflate_passthrough_decode_failure_remains_success()
        .await
        .unwrap();
    non_sse_gzip_decode_failure_skips_sse_transform()
        .await
        .unwrap();
    sse_unsupported_encoding_skips_transform_and_raw_passes_through().await;
    sse_transform_failure_before_output_fails_open_raw().await;
    sse_transform_fail_open_preserves_keepalive_observation().await;
    sse_unterminated_final_event_is_upstream_framing_error().await;
    sse_unterminated_passthrough_is_upstream_framing_error()
        .await
        .unwrap();
}

#[tokio::test]
async fn response_transform_paths_suite_part_4() {
    sse_trailing_line_terminators_remain_success()
        .await
        .unwrap();
    sse_body_failure_after_parser_budget_starts_separate_error_frame().await;
    sse_gzip_unterminated_final_event_emits_upstream_framing_error().await;
    sse_unterminated_http_error_preserves_upstream_status()
        .await
        .unwrap();
    sse_unterminated_http_error_passthrough_preserves_upstream_body()
        .await
        .unwrap();
    sse_gzip_transform_failure_before_output_emits_error_frame().await;
    sse_gzip_transform_failure_preserves_http_error_status()
        .await
        .unwrap();
    sse_gzip_invalid_event_name_before_output_emits_error_frame().await;
    sse_transform_invalid_event_name_fails_open_without_injection().await;
    sse_transform_failure_after_transformed_output_terminates_classified().await;
    sse_decompression_budget_before_output_emits_transform_error().await;
    sse_gzip_incomplete_event_budget_before_output_emits_transform_error().await;
    sse_brotli_incomplete_event_budget_never_replays_compressed_bytes().await;
    sse_incomplete_event_budget_after_output_uses_existing_error_frame().await;
    sse_accounting_uses_pre_transform_usage().await.unwrap();
    buffered_transformed_question_is_user_turn_without_keepalive_enqueue().await;
    sse_transformed_question_is_user_turn_without_keepalive_enqueue().await;
    sse_dropped_question_is_not_renewed().await;
    sse_explicitly_dropped_question_is_not_renewed().await;
    sse_multi_event_question_is_user_turn_without_keepalive_enqueue().await;
    failed_open_question_transform_preserves_original_body_and_usage().await;
}
