#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use aws_eventstream_codec::{decode_message, decode_messages, encode_message};
use axum::body::Body;
use bytes::Bytes;
use cc_lb_core::{strip_hop_by_hop, SseBatchConfig, SseRelay};
use cc_lb_plugin_api::{
    DialectError, ObservabilityError, ObservabilityHook, ObserveEvent, Principal, RequestContext,
    ShapedRequest, ShapedRequestBuilder, Upstream, UpstreamDialect,
};
use http::header::CONNECTION;
use http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use http_body_util::BodyExt;
use proptest::prelude::*;
use proptest::string::string_regex;
use proptest::test_runner::{Config as ProptestConfig, TestCaseError, TestCaseResult, TestRunner};
use tokio::runtime::{Builder, Runtime};

const MIN_PROPTEST_CASES: u32 = 1024;
const PRESERVED_HEADERS: [&str; 6] = [
    "anthropic-version",
    "anthropic-beta",
    "anthropic-property-test",
    "x-api-key",
    "authorization",
    "user-agent",
];
const HOP_BY_HOP_HEADERS: [&str; 9] = [
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "proxy-connection",
];

#[test]
fn sse_valid_event_sequences_preserve_raw_bytes() -> Result<(), String> {
    let runtime = runtime()?;
    run_property(
        "sse_valid_event_sequences_preserve_raw_bytes",
        valid_sse_stream_strategy(),
        |(events, split_seed)| {
            let input = render_sse_events(&events);
            let output = runtime
                .block_on(relay_bytes(input.clone(), split_seed))
                .map_err(TestCaseError::fail)?;
            prop_assert_eq!(output.as_ref(), input.as_slice());
            Ok(())
        },
    )
}

#[test]
fn sse_random_byte_sequences_do_not_panic() -> Result<(), String> {
    let runtime = runtime()?;
    run_property(
        "sse_random_byte_sequences_do_not_panic",
        (
            prop::collection::vec(any::<u8>(), 0..=256),
            prop::collection::vec(0usize..=64, 0..=16),
        ),
        |(input, split_seed)| {
            let _output = runtime
                .block_on(relay_bytes(input, split_seed))
                .map_err(TestCaseError::fail)?;
            Ok(())
        },
    )
}

#[test]
fn aws_eventstream_roundtrip_preserves_payload_and_event_type() -> Result<(), String> {
    run_property(
        "aws_eventstream_roundtrip_preserves_payload_and_event_type",
        (
            event_type_strategy(),
            prop::collection::vec(any::<u8>(), 0..=512),
        ),
        |(event_type, payload)| {
            let frame = encode_message(&event_type, &payload);
            let (decoded, consumed) = decode_message(&frame).map_err(|source| {
                TestCaseError::fail(format!("event-stream decode failed: {source}"))
            })?;
            prop_assert_eq!(consumed, frame.len());
            prop_assert_eq!(decoded.header_str(":event-type"), Some(event_type.as_str()));
            prop_assert_eq!(decoded.header_str(":message-type"), Some("event"));
            prop_assert_eq!(decoded.payload.as_slice(), payload.as_slice());

            let decoded_messages = decode_messages(&frame).map_err(|source| {
                TestCaseError::fail(format!("event-stream batch decode failed: {source}"))
            })?;
            prop_assert_eq!(decoded_messages.len(), 1);
            prop_assert_eq!(
                decoded_messages[0].header_str(":event-type"),
                Some(event_type.as_str())
            );
            prop_assert_eq!(decoded_messages[0].payload.as_slice(), payload.as_slice());
            Ok(())
        },
    )
}

#[test]
fn header_allowlist_preserves_non_hop_headers_unless_connection_listed() -> Result<(), String> {
    run_property(
        "header_allowlist_preserves_non_hop_headers_unless_connection_listed",
        header_preservation_case_strategy(),
        |case| {
            let mut headers = HeaderMap::new();
            let mut expected = BTreeMap::new();

            for (name, value) in PRESERVED_HEADERS.iter().zip(case.required_values.iter()) {
                insert_header(&mut headers, name, value);
                expected.insert((*name).to_owned(), value.clone());
            }
            for (name, value) in &case.extra_headers {
                insert_header(&mut headers, name, value);
                expected.insert(name.clone(), value.clone());
            }
            if !case.connection_tokens.is_empty() {
                insert_header(
                    &mut headers,
                    CONNECTION.as_str(),
                    &connection_value(&case.connection_tokens),
                );
            }

            let listed = listed_tokens(&case.connection_tokens);
            strip_hop_by_hop(&mut headers);

            for (name, value) in expected {
                if listed.contains(&name) {
                    prop_assert!(!headers.contains_key(name.as_str()));
                } else {
                    let actual = headers.get(name.as_str()).ok_or_else(|| {
                        TestCaseError::fail(format!("missing preserved header {name}"))
                    })?;
                    prop_assert_eq!(actual.as_bytes(), value.as_bytes());
                }
            }
            Ok(())
        },
    )
}

#[test]
fn hop_by_hop_headers_and_connection_extras_are_removed() -> Result<(), String> {
    run_property(
        "hop_by_hop_headers_and_connection_extras_are_removed",
        prop::collection::vec(non_hop_header_name_strategy("x-hop-extra"), 0..=12),
        |extra_names| {
            let mut headers = HeaderMap::new();
            for name in HOP_BY_HOP_HEADERS {
                insert_header(&mut headers, name, "hop");
            }
            for name in &extra_names {
                insert_header(&mut headers, name, "connection-listed");
            }

            let connection_tokens = if extra_names.is_empty() {
                vec!["x-unused-hop-extra".to_owned()]
            } else {
                extra_names.clone()
            };
            insert_header(
                &mut headers,
                CONNECTION.as_str(),
                &connection_value(&connection_tokens),
            );

            strip_hop_by_hop(&mut headers);

            for name in HOP_BY_HOP_HEADERS {
                prop_assert!(!headers.contains_key(name));
            }
            for name in extra_names {
                prop_assert!(!headers.contains_key(name.as_str()));
            }
            Ok(())
        },
    )
}

fn run_property<S, F>(name: &str, strategy: S, property: F) -> Result<(), String>
where
    S: Strategy,
    F: Fn(S::Value) -> TestCaseResult,
{
    let cases = proptest_case_count();
    println!("{name}: proptest_cases={cases}");
    let mut runner = TestRunner::new(ProptestConfig {
        cases,
        failure_persistence: None,
        ..ProptestConfig::default()
    });
    runner
        .run(&strategy, property)
        .map_err(|source| source.to_string())
}

fn proptest_case_count() -> u32 {
    std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(MIN_PROPTEST_CASES)
        .max(MIN_PROPTEST_CASES)
}

fn runtime() -> Result<Runtime, String> {
    Builder::new_multi_thread()
        .enable_time()
        .build()
        .map_err(|source| source.to_string())
}

#[derive(Clone, Debug)]
struct SseEventCase {
    comment: Option<String>,
    id: Option<String>,
    event_name: String,
    data_lines: Vec<String>,
}

fn valid_sse_stream_strategy() -> impl Strategy<Value = (Vec<SseEventCase>, Vec<usize>)> {
    (
        prop::collection::vec(sse_event_strategy(), 1..=12),
        prop::collection::vec(0usize..=64, 0..=24),
    )
}

fn sse_event_strategy() -> impl Strategy<Value = SseEventCase> {
    (
        prop::option::of(ascii_line_strategy(24)),
        prop::option::of(ascii_line_strategy(16)),
        event_type_strategy(),
        prop::collection::vec(ascii_line_strategy(48), 1..=4),
    )
        .prop_map(|(comment, id, event_name, data_lines)| SseEventCase {
            comment,
            id,
            event_name,
            data_lines,
        })
}

fn render_sse_events(events: &[SseEventCase]) -> Vec<u8> {
    let mut output = Vec::new();
    for event in events {
        if let Some(comment) = &event.comment {
            output.extend_from_slice(b": ");
            output.extend_from_slice(comment.as_bytes());
            output.push(b'\n');
        }
        if let Some(id) = &event.id {
            output.extend_from_slice(b"id: ");
            output.extend_from_slice(id.as_bytes());
            output.push(b'\n');
        }
        output.extend_from_slice(b"event: ");
        output.extend_from_slice(event.event_name.as_bytes());
        output.push(b'\n');
        for data in &event.data_lines {
            output.extend_from_slice(b"data: ");
            output.extend_from_slice(data.as_bytes());
            output.push(b'\n');
        }
        output.push(b'\n');
    }
    output
}

async fn relay_bytes(input: Vec<u8>, split_seed: Vec<usize>) -> Result<Bytes, String> {
    let bytes = Bytes::from(input);
    let chunks = split_bytes(bytes, split_seed);
    let response = relay().into_response_from_body(body_from_chunks(chunks));
    response
        .into_body()
        .collect()
        .await
        .map(|collected| collected.to_bytes())
        .map_err(|source| source.to_string())
}

fn split_bytes(input: Bytes, split_seed: Vec<usize>) -> Vec<Bytes> {
    if input.is_empty() {
        return vec![input];
    }

    let mut chunks = Vec::new();
    let mut offset = 0;
    for seed in split_seed {
        if offset >= input.len() {
            break;
        }
        let remaining = input.len() - offset;
        let max_take = remaining.min(32);
        let take = 1 + (seed % max_take);
        let end = offset + take;
        chunks.push(input.slice(offset..end));
        offset = end;
    }
    if offset < input.len() {
        chunks.push(input.slice(offset..input.len()));
    }
    chunks
}

fn body_from_chunks(chunks: Vec<Bytes>) -> Body {
    let stream = async_stream::stream! {
        for chunk in chunks {
            yield Ok::<Bytes, Infallible>(chunk);
        }
    };
    Body::from_stream(stream)
}

fn relay() -> SseRelay {
    SseRelay {
        obs: Arc::new(NoopHook),
        dialect: Arc::new(NoopDialect),
        batch: SseBatchConfig {
            max_events: 8,
            max_age: Duration::from_secs(60),
        },
        quota: None,
        principal_id: "property-principal".to_owned(),
        reservation: None,
        error_normalizer: None,
        upstream_kind: None,
    }
}

struct NoopHook;

impl ObservabilityHook for NoopHook {
    fn observe(&self, _event: ObserveEvent) -> Result<(), ObservabilityError> {
        Ok(())
    }
}

struct NoopDialect;

impl UpstreamDialect for NoopDialect {
    fn shape(
        &self,
        _ctx: &RequestContext,
        _upstream: &Upstream,
        _principal: &Principal,
        _builder: &mut ShapedRequestBuilder,
    ) -> Result<ShapedRequest, DialectError> {
        Err(DialectError::UnsupportedRequest {
            reason: "property tests use relay behavior only".to_owned(),
        })
    }

    fn normalize_error(&self, _status: StatusCode, _body: &Bytes) -> Option<Bytes> {
        None
    }
}

#[derive(Clone, Debug)]
struct HeaderPreservationCase {
    required_values: Vec<String>,
    extra_headers: Vec<(String, String)>,
    connection_tokens: Vec<String>,
}

fn header_preservation_case_strategy() -> impl Strategy<Value = HeaderPreservationCase> {
    (
        prop::collection::vec(
            header_value_strategy(48),
            PRESERVED_HEADERS.len()..=PRESERVED_HEADERS.len(),
        ),
        prop::collection::vec(
            (
                non_hop_header_name_strategy("x-prop"),
                header_value_strategy(48),
            ),
            0..=12,
        ),
        prop::collection::vec(connection_token_strategy(), 0..=8),
    )
        .prop_map(|(required_values, extra_headers, connection_tokens)| {
            HeaderPreservationCase {
                required_values,
                extra_headers,
                connection_tokens,
            }
        })
}

fn insert_header(headers: &mut HeaderMap, name: &str, value: &str) {
    let header_name = HeaderName::from_bytes(name.as_bytes())
        .expect("generated header name must be valid HTTP header name");
    let header_value = HeaderValue::from_str(value)
        .expect("generated header value must be valid HTTP header value");
    headers.insert(header_name, header_value);
}

fn connection_value(tokens: &[String]) -> String {
    let mut value = String::new();
    for (index, token) in tokens.iter().enumerate() {
        if index > 0 {
            value.push(',');
        }
        if index % 2 == 0 {
            value.push(' ');
            value.push_str(&token.to_ascii_uppercase());
            value.push(' ');
        } else {
            value.push_str(token);
        }
    }
    value
}

fn listed_tokens(tokens: &[String]) -> BTreeSet<String> {
    tokens
        .iter()
        .map(|token| token.to_ascii_lowercase())
        .collect()
}

fn event_type_strategy() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("message_start".to_owned()),
        Just("content_block_delta".to_owned()),
        Just("message_delta".to_owned()),
        Just("message_stop".to_owned()),
        Just("chunk".to_owned()),
        string_regex("[a-z][a-z0-9_-]{0,31}").expect("event type regex compiles"),
    ]
}

fn connection_token_strategy() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("anthropic-version".to_owned()),
        Just("anthropic-beta".to_owned()),
        Just("anthropic-property-test".to_owned()),
        Just("x-api-key".to_owned()),
        Just("authorization".to_owned()),
        Just("user-agent".to_owned()),
        non_hop_header_name_strategy("x-prop"),
    ]
}

fn non_hop_header_name_strategy(prefix: &'static str) -> impl Strategy<Value = String> {
    string_regex("[a-z0-9]{1,12}")
        .expect("header suffix regex compiles")
        .prop_map(move |suffix| format!("{prefix}-{suffix}"))
}

fn header_value_strategy(max_len: usize) -> impl Strategy<Value = String> {
    prop::collection::vec(33u8..=126, 0..=max_len)
        .prop_map(|bytes| String::from_utf8(bytes).expect("generated ASCII is valid UTF-8"))
}

fn ascii_line_strategy(max_len: usize) -> impl Strategy<Value = String> {
    prop::collection::vec(32u8..=126, 0..=max_len)
        .prop_map(|bytes| String::from_utf8(bytes).expect("generated ASCII is valid UTF-8"))
}
