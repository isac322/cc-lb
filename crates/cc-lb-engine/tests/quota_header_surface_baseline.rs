use std::collections::BTreeMap;

use crate::common::{
    DispatchMode, MockDispatch, TestAuthn, TestState, collect_body, lifecycle_with,
    messages_request,
};
use bytes::Bytes;
use cc_lb_quota::build_subscription_quota_samples;
use cc_lb_quota::rate_limit_headers::parse_anthropic_unified_headers;
use cc_lb_storage_api::{SubscriptionQuotaStatus, SubscriptionQuotaWindow};
use http::{HeaderMap, HeaderValue, StatusCode};
use uuid::Uuid;

#[tokio::test]
async fn quota_header_surface_and_sample_remain_byte_stable() {
    let headers = quota_headers();
    let observations = parse_anthropic_unified_headers(&headers);
    let samples = build_subscription_quota_samples(&headers, Uuid::nil(), 1_700_000_000_000);

    assert_eq!(observations.len(), 1);
    assert_eq!(observations[0].window, SubscriptionQuotaWindow::FiveHour);
    assert_eq!(observations[0].utilization, Some(0.42));
    assert_eq!(
        observations[0].status,
        Some(SubscriptionQuotaStatus::AllowedWarning)
    );
    assert_eq!(samples.len(), 1);
    assert_eq!(samples[0].window, SubscriptionQuotaWindow::FiveHour);
    assert_eq!(samples[0].utilization, Some(0.42));
    assert_eq!(
        samples[0].status,
        Some(SubscriptionQuotaStatus::AllowedWarning)
    );

    let state = TestState::default();
    let lifecycle = lifecycle_with(
        TestAuthn::new(state.clone()),
        MockDispatch {
            state,
            mode: DispatchMode::HeadersOk(headers),
        },
    );
    let request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[]}"#,
    ));
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("test request authenticates");
    let response = lifecycle
        .handle(request, &auth)
        .await
        .expect("lifecycle handles request");
    let (status, emitted_headers, _body) = collect_body(response).await;

    assert_eq!(status, StatusCode::OK);
    let emitted = emitted_rate_limit_headers(&emitted_headers);
    let rendered = emitted
        .iter()
        .map(|(name, value)| format!("{name}:{value}"))
        .collect::<Vec<_>>()
        .join("\n");
    assert_eq!(rendered, expected_surface());
    println!("quota_header_surface=\n{rendered}");
}

fn quota_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    for (name, value) in [
        ("anthropic-ratelimit-requests-limit", "1000"),
        ("anthropic-ratelimit-requests-remaining", "997"),
        ("anthropic-ratelimit-requests-reset", "2026-05-20T00:00:01Z"),
        ("anthropic-ratelimit-tokens-limit", "100000"),
        ("anthropic-ratelimit-tokens-remaining", "99990"),
        ("anthropic-ratelimit-tokens-reset", "2026-05-20T00:00:02Z"),
        ("anthropic-ratelimit-unified-5h-utilization", "0.42"),
        ("anthropic-ratelimit-unified-5h-status", "allowed_warning"),
        ("anthropic-ratelimit-unified-5h-reset", "1800000000"),
    ] {
        headers.insert(name, HeaderValue::from_static(value));
    }
    headers
}

fn emitted_rate_limit_headers(headers: &HeaderMap) -> BTreeMap<String, String> {
    headers
        .iter()
        .filter_map(|(name, value)| {
            name.as_str()
                .starts_with("anthropic-ratelimit-")
                .then(|| {
                    value
                        .to_str()
                        .ok()
                        .map(|value| (name.to_string(), value.to_owned()))
                })
                .flatten()
        })
        .collect()
}

fn expected_surface() -> &'static str {
    "anthropic-ratelimit-requests-limit:1000\n\
anthropic-ratelimit-requests-remaining:997\n\
anthropic-ratelimit-requests-reset:2026-05-20T00:00:01Z\n\
anthropic-ratelimit-tokens-limit:100000\n\
anthropic-ratelimit-tokens-remaining:99990\n\
anthropic-ratelimit-tokens-reset:2026-05-20T00:00:02Z\n\
anthropic-ratelimit-unified-5h-reset:1800000000\n\
anthropic-ratelimit-unified-5h-status:allowed_warning\n\
anthropic-ratelimit-unified-5h-utilization:0.42"
}
