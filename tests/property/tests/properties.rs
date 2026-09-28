#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet};

use cc_lb_engine::strip_hop_by_hop;
use http::header::CONNECTION;
use http::{HeaderMap, HeaderName, HeaderValue};
use proptest::prelude::*;
use proptest::string::string_regex;
use proptest::test_runner::{Config as ProptestConfig, TestCaseError, TestCaseResult, TestRunner};

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
        if index.is_multiple_of(2) {
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
