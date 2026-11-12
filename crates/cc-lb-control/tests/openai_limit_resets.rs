//! QA: account headers/no Reserve opt-in; explicit credit selection/no retry;
//! capped inventory and unknown metadata; malformed inventory/consume uncertainty;
//! all provider outcomes; configured URL styles; snapshot invalidation fences.
//!
//! Contract and fixture evidence (provider-owned tests, not invented API fields):
//! https://github.com/openai/codex/blob/main/codex-rs/backend-client/src/client/rate_limit_resets_tests.rs
//! https://github.com/openai/codex/blob/main/codex-rs/backend-client/src/types.rs

use std::sync::{Arc, Mutex};

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::{HeaderMap, Request, StatusCode};
use axum::routing::any;
use cc_lb_control::anthropic_metadata::make_metadata_http_client;
use cc_lb_control::openai_limit_resets::{
    OpenAiLimitResetAuth, OpenAiLimitResetConsumeOutcome, OpenAiLimitResetConsumeRequest,
    OpenAiLimitResetError, OpenAiLimitResetInventory, OpenAiLimitResetPollRecord,
    consume_openai_limit_reset_credit, list_openai_limit_reset_credits,
    openai_limit_reset_credits_url, openai_limit_reset_epoch_fence,
    openai_limit_reset_epoch_meta_key, openai_limit_reset_fence_started_at_millis,
    openai_limit_reset_meta_key, settled_openai_limit_reset_epoch,
};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

struct Recorded {
    method: String,
    path: String,
    headers: HeaderMap,
    body: Vec<u8>,
}

struct Fixture {
    base: Url,
    requests: Arc<Mutex<Vec<Recorded>>>,
    server: JoinHandle<()>,
}

impl Fixture {
    async fn new(status: StatusCode, response: Value) -> Self {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&requests);
        let response = response.to_string();
        let app = Router::new().fallback(any(move |request: Request<Body>| {
            let recorded = Arc::clone(&recorded);
            let response = response.clone();
            async move {
                let (parts, body) = request.into_parts();
                let body = to_bytes(body, 1024 * 1024).await.expect("bounded body");
                recorded.lock().expect("request lock").push(Recorded {
                    method: parts.method.to_string(),
                    path: parts.uri.path().to_owned(),
                    headers: parts.headers,
                    body: body.to_vec(),
                });
                (status, response)
            }
        }));
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("loopback fixture");
        let base = Url::parse(&format!(
            "http://{}/",
            listener.local_addr().expect("address")
        ))
        .expect("loopback URL");
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("fixture server");
        });
        Self {
            base,
            requests,
            server,
        }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}

fn auth() -> OpenAiLimitResetAuth<'static> {
    OpenAiLimitResetAuth {
        access_token: "fixture-token",
        account_id: "selected-account",
        user_agent: "codex-cli/test",
    }
}

fn consume_request() -> OpenAiLimitResetConsumeRequest<'static> {
    OpenAiLimitResetConsumeRequest {
        redeem_request_id: "redeem-456",
        credit_id: "credit-selected-not-first",
    }
}

#[tokio::test]
async fn inventory_preserves_provider_metadata_and_total_instead_of_list_length() {
    // Shape and strings match the primary Codex fixture. The reported total is
    // deliberately larger than this capped list; unknown status/type are retained.
    let fixture = Fixture::new(
        StatusCode::OK,
        json!({
            "credits": [{
                "id": "credit-1", "reset_type": "future_reset_type", "status": "future_status",
                "granted_at": "2026-06-17T00:00:00Z", "expires_at": "2026-07-17T00:00:00Z",
                "is_supported_by_plan": true,
                "redeem_started_at": null, "redeemed_at": null,
                "title": "Full reset (Weekly + 5 hr)", "description": "Ready to redeem"
            }],
            "available_count": 7, "total_earned_count": 9
        }),
    )
    .await;
    let base = fixture
        .base
        .join("backend-api/codex/?ignored=1#ignored")
        .unwrap();
    let inventory = list_openai_limit_reset_credits(
        &make_metadata_http_client(),
        &base,
        auth(),
        &CancellationToken::new(),
    )
    .await
    .expect("inventory");
    assert_eq!(inventory.available_count, 7);
    assert_eq!(inventory.credits.len(), 1);
    assert_eq!(inventory.applicable_available_count, None);
    let credit = &inventory.credits[0];
    assert_eq!(credit.reset_type, "future_reset_type");
    assert_eq!(credit.status, "future_status");
    assert_eq!(credit.is_supported_by_plan, Some(true));
    assert_eq!(credit.title.as_deref(), Some("Full reset (Weekly + 5 hr)"));
    assert_eq!(credit.description.as_deref(), Some("Ready to redeem"));
    assert_eq!(credit.expires_at.as_deref(), Some("2026-07-17T00:00:00Z"));
    let requests = fixture.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].method, "GET");
    assert_eq!(
        requests[0].path,
        "/backend-api/wham/rate-limit-reset-credits"
    );
    assert_eq!(requests[0].headers["authorization"], "Bearer fixture-token");
    assert_eq!(requests[0].headers["originator"], "codex_cli_rs");
    assert_eq!(requests[0].headers["user-agent"], "codex-cli/test");
    assert_eq!(
        requests[0].headers["chatgpt-account-id"],
        "selected-account"
    );
    assert!(
        !requests[0]
            .headers
            .contains_key("x-openai-codex-luna-reserve")
    );
    assert!(!requests[0].headers.contains_key("anthropic-beta"));
}

#[tokio::test]
async fn consume_forwards_exact_credit_and_redeem_request_id_once() {
    // Primary fixture: code=reset and numeric windows_reset, with an extra
    // credit object deliberately ignored by the official client.
    let fixture = Fixture::new(
        StatusCode::OK,
        json!({
            "code": "reset", "windows_reset": 2, "credit": {"id": "ignored-by-cli"}
        }),
    )
    .await;
    let result = consume_openai_limit_reset_credit(
        &make_metadata_http_client(),
        &fixture.base,
        auth(),
        consume_request(),
        &CancellationToken::new(),
    )
    .await
    .expect("consume result");
    assert_eq!(result.code, OpenAiLimitResetConsumeOutcome::Reset);
    assert_eq!(result.windows_reset, Some(2));
    let requests = fixture.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].method, "POST");
    assert_eq!(
        requests[0].path,
        "/api/codex/rate-limit-reset-credits/consume"
    );
    assert_eq!(
        requests[0].headers["chatgpt-account-id"],
        "selected-account"
    );
    assert_eq!(requests[0].headers["content-type"], "application/json");
    assert!(
        !requests[0]
            .headers
            .contains_key("x-openai-codex-luna-reserve")
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&requests[0].body).unwrap(),
        json!({
            "redeem_request_id": "redeem-456", "credit_id": "credit-selected-not-first"
        })
    );
}

#[tokio::test]
async fn malformed_inventory_is_not_an_empty_inventory() {
    for response in [
        json!({"available_count": 3}),
        json!({"credits": null, "available_count": 3}),
        json!({"credits": [], "available_count": "3"}),
        json!({"credits": [{}], "available_count": 3}),
        json!({"credits": []}),
    ] {
        let fixture = Fixture::new(StatusCode::OK, response).await;
        let result = list_openai_limit_reset_credits(
            &make_metadata_http_client(),
            &fixture.base,
            auth(),
            &CancellationToken::new(),
        )
        .await;
        assert!(matches!(result, Err(OpenAiLimitResetError::Parse(_))));
    }
    let fixture = Fixture::new(StatusCode::OK, json!({"credits": [], "available_count": 0})).await;
    assert!(
        list_openai_limit_reset_credits(
            &make_metadata_http_client(),
            &fixture.base,
            auth(),
            &CancellationToken::new(),
        )
        .await
        .unwrap()
        .credits
        .is_empty()
    );
}

#[tokio::test]
async fn unknown_or_malformed_consume_result_is_uncertain_and_never_retried() {
    for response in [
        json!({"code": "future_result", "windows_reset": 2}),
        json!({"code": "reset", "windows_reset": ["five_hour"]}),
        json!({"windows_reset": 2}),
    ] {
        let fixture = Fixture::new(StatusCode::OK, response).await;
        let error = consume_openai_limit_reset_credit(
            &make_metadata_http_client(),
            &fixture.base,
            auth(),
            consume_request(),
            &CancellationToken::new(),
        )
        .await
        .unwrap_err();
        assert!(matches!(error, OpenAiLimitResetError::Parse(_)));
        assert!(error.consume_outcome_is_uncertain());
        assert_eq!(fixture.requests.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn all_raw_consume_outcomes_remain_distinct() {
    for (code, expected) in [
        ("reset", OpenAiLimitResetConsumeOutcome::Reset),
        (
            "nothing_to_reset",
            OpenAiLimitResetConsumeOutcome::NothingToReset,
        ),
        ("no_credit", OpenAiLimitResetConsumeOutcome::NoCredit),
        (
            "already_redeemed",
            OpenAiLimitResetConsumeOutcome::AlreadyRedeemed,
        ),
    ] {
        let fixture = Fixture::new(StatusCode::OK, json!({"code": code})).await;
        let result = consume_openai_limit_reset_credit(
            &make_metadata_http_client(),
            &fixture.base,
            auth(),
            consume_request(),
            &CancellationToken::new(),
        )
        .await
        .unwrap();
        assert_eq!(result.code, expected);
        assert_eq!(result.windows_reset, None);
        assert_eq!(serde_json::to_value(&result).unwrap()["code"], code);
    }
}

#[tokio::test]
async fn consume_reset_count_preserves_null_and_explicit_zero() {
    for (raw_count, expected) in [
        (json!(null), None),
        (json!(0), Some(0)),
        (json!(2), Some(2)),
    ] {
        let fixture = Fixture::new(
            StatusCode::OK,
            json!({"code": "reset", "windows_reset": raw_count}),
        )
        .await;
        let result = consume_openai_limit_reset_credit(
            &make_metadata_http_client(),
            &fixture.base,
            auth(),
            consume_request(),
            &CancellationToken::new(),
        )
        .await
        .expect("nullable numeric reset count");
        assert_eq!(result.windows_reset, expected);
        assert_eq!(
            serde_json::to_value(&result).unwrap()["windows_reset"],
            raw_count
        );
    }
}

#[tokio::test]
async fn provider_plan_support_and_applicable_count_preserve_false_zero_and_unknown() {
    // Live 2026-11-12 inventory adds is_supported_by_plan=true to the primary
    // Codex fixture shape. False must survive equally: banked != plan-supported.
    for (support, applicable, expected_support, expected_count) in [
        (json!(true), json!(2), Some(true), Some(2)),
        (json!(false), json!(0), Some(false), Some(0)),
        (json!(null), json!(null), None, None),
    ] {
        let fixture = Fixture::new(
            StatusCode::OK,
            json!({
                "credits": [{
                    "id": "credit-live-shape", "reset_type": "codex_rate_limits",
                    "is_supported_by_plan": support, "status": "available",
                    "granted_at": "2026-10-31T20:20:26.649932Z",
                    "expires_at": "2026-11-30T20:20:26.649932Z",
                    "title": "Full reset", "description": "Ready to redeem"
                }],
                "available_count": 2,
                "applicable_available_count": applicable
            }),
        )
        .await;
        let inventory = list_openai_limit_reset_credits(
            &make_metadata_http_client(),
            &fixture.base,
            auth(),
            &CancellationToken::new(),
        )
        .await
        .expect("provider-owned applicability");
        assert_eq!(inventory.available_count, 2);
        assert_eq!(inventory.applicable_available_count, expected_count);
        assert_eq!(inventory.credits[0].is_supported_by_plan, expected_support);
        let serialized = serde_json::to_value(&inventory).unwrap();
        assert_eq!(serialized["credits"][0]["is_supported_by_plan"], support);
        assert_eq!(serialized["applicable_available_count"], applicable);
    }
    let missing: OpenAiLimitResetInventory = serde_json::from_value(json!({
        "credits": [{
            "id": "older-credit", "reset_type": "codex_rate_limits", "status": "available",
            "granted_at": "2026-10-31T20:20:26.649932Z"
        }],
        "available_count": 2
    }))
    .unwrap();
    assert_eq!(missing.applicable_available_count, None);
    assert_eq!(missing.credits[0].is_supported_by_plan, None);
}

#[tokio::test]
async fn rejection_and_uncertain_server_error_are_distinct_without_retry() {
    for (status, uncertain) in [
        (StatusCode::FORBIDDEN, false),
        (StatusCode::INTERNAL_SERVER_ERROR, true),
    ] {
        let fixture = Fixture::new(status, json!({"error": "fixture"})).await;
        let error = consume_openai_limit_reset_credit(
            &make_metadata_http_client(),
            &fixture.base,
            auth(),
            consume_request(),
            &CancellationToken::new(),
        )
        .await
        .unwrap_err();
        assert!(
            matches!(error, OpenAiLimitResetError::ProviderStatus(value) if value == status.as_u16())
        );
        assert_eq!(error.consume_outcome_is_uncertain(), uncertain);
        assert_eq!(fixture.requests.lock().unwrap().len(), 1);
    }
    for error in [
        OpenAiLimitResetError::Timeout,
        OpenAiLimitResetError::Cancelled,
        OpenAiLimitResetError::Transport("lost response".into()),
    ] {
        assert!(error.consume_outcome_is_uncertain());
    }
    assert!(!OpenAiLimitResetError::Connect("not delivered".into()).consume_outcome_is_uncertain());
    assert!(
        !OpenAiLimitResetError::Request("invalid header".into()).consume_outcome_is_uncertain()
    );
}

#[tokio::test]
async fn missing_selection_or_bound_account_never_dispatches() {
    let fixture = Fixture::new(StatusCode::OK, json!({"code": "reset"})).await;
    for (auth, request) in [
        (
            auth(),
            OpenAiLimitResetConsumeRequest {
                credit_id: "",
                ..consume_request()
            },
        ),
        (
            auth(),
            OpenAiLimitResetConsumeRequest {
                redeem_request_id: "",
                ..consume_request()
            },
        ),
        (
            OpenAiLimitResetAuth {
                account_id: "",
                ..auth()
            },
            consume_request(),
        ),
        (
            OpenAiLimitResetAuth {
                account_id: "invalid\nheader",
                ..auth()
            },
            consume_request(),
        ),
    ] {
        let error = consume_openai_limit_reset_credit(
            &make_metadata_http_client(),
            &fixture.base,
            auth,
            request,
            &CancellationToken::new(),
        )
        .await
        .unwrap_err();
        assert!(matches!(error, OpenAiLimitResetError::Request(_)));
        assert!(!error.consume_outcome_is_uncertain());
    }
    assert!(fixture.requests.lock().unwrap().is_empty());
}

#[test]
fn configured_backend_and_codex_api_urls_match_usage_semantics() {
    for base in [
        "https://chatgpt.com",
        "https://chatgpt.com/codex/",
        "https://chatgpt.com/backend-api",
        "https://chatgpt.com/backend-api/",
        "https://chatgpt.com/backend-api/codex/?x=1#fragment",
        "https://chat.openai.com/backend-api/codex",
    ] {
        let url = openai_limit_reset_credits_url(&Url::parse(base).unwrap());
        assert_eq!(url.path(), "/backend-api/wham/rate-limit-reset-credits");
        assert!(url.query().is_none());
        assert!(url.fragment().is_none());
    }
    for (base, path) in [
        (
            "https://codex.example/custom/",
            "/custom/api/codex/rate-limit-reset-credits",
        ),
        (
            "https://gateway.example/prefix/backend-api/codex/",
            "/prefix/backend-api/wham/rate-limit-reset-credits",
        ),
    ] {
        assert_eq!(
            openai_limit_reset_credits_url(&Url::parse(base).unwrap()).path(),
            path
        );
    }
}

#[test]
fn snapshot_keys_and_epoch_fences_are_provider_scoped_and_fail_closed() {
    let upstream = Uuid::new_v4();
    let attempt = Uuid::new_v4();
    assert_eq!(
        openai_limit_reset_meta_key(upstream),
        format!("openai_limit_reset:{upstream}")
    );
    assert_eq!(
        openai_limit_reset_epoch_meta_key(upstream),
        format!("openai_limit_reset_epoch:{upstream}")
    );
    let fence = openai_limit_reset_epoch_fence(attempt, 1234);
    assert_eq!(
        openai_limit_reset_fence_started_at_millis(&fence),
        Some(1234)
    );
    assert_eq!(settled_openai_limit_reset_epoch(Some(&fence)), None);
    assert_eq!(settled_openai_limit_reset_epoch(None), Some(None));
    assert_eq!(
        settled_openai_limit_reset_epoch(Some(&attempt.to_string())),
        Some(Some(attempt))
    );
    for invalid in ["pending:invalid:1234", "pending:invalid", "corrupt"] {
        assert_eq!(settled_openai_limit_reset_epoch(Some(invalid)), None);
        assert_eq!(openai_limit_reset_fence_started_at_millis(invalid), None);
    }
    let record = OpenAiLimitResetPollRecord {
        credential_fingerprint: 42,
        oauth_token_generation: 7,
        account_id: "bound-account".into(),
        epoch: Some(attempt),
        observed_at_unix_millis: 1234,
        inventory: None,
    };
    let restored: OpenAiLimitResetPollRecord =
        serde_json::from_value(serde_json::to_value(record).unwrap()).unwrap();
    assert_eq!(restored.credential_fingerprint, 42);
    assert_eq!(restored.oauth_token_generation, 7);
    assert_eq!(restored.account_id, "bound-account");
    assert_eq!(restored.epoch, Some(attempt));
    assert!(restored.inventory.is_none());
}

fn manual_usage(plan: &str, windows: &[(u64, Value)]) -> Value {
    let mut usage = json!({"plan_type":plan, "rate_limit":{}});
    for (index, (duration, used)) in windows.iter().enumerate() {
        usage["rate_limit"][if index == 0 {
            "primary_window"
        } else {
            "secondary_window"
        }] = json!({
            "limit_window_seconds": duration, "used_percent": used, "reset_at": 4_000_000_000u64
        });
    }
    usage
}

#[test]
fn manual_reset_requires_strictly_more_than_twenty_percent_of_a_covered_window() {
    use cc_lb_control::openai_limit_resets::classify_openai_manual_redemption;
    for (five, weekly, eligible) in [
        (0.0, 0.0, false),
        (20.0, 20.0, false),
        (20.1, 0.0, true),
        (0.0, 20.1, true),
        (100.0, 100.0, true),
    ] {
        let mut usage = manual_usage("plus", &[(18000, json!(five)), (604800, json!(weekly))]);
        usage["ordinary_usage_allowed"] = json!(false);
        usage["additional_rate_limits"] = json!([{"rate_limit":{"primary_window":{
            "limit_window_seconds":604800,"used_percent":100,"reset_at":4_000_000_000u64
        }}}]);
        let classified = classify_openai_manual_redemption(&usage, 1_800_000_000);
        assert_eq!(classified.eligible, eligible, "{usage}");
        assert_eq!(
            classified.reason.as_deref(),
            (!eligible).then_some("below_redemption_threshold")
        );
        assert_eq!(classified.windows.len(), 2);
        assert_eq!(classified.windows[0].window, "5h");
        assert_eq!(classified.windows[1].window, "7d");
    }
}

#[test]
fn manual_reset_coverage_comes_from_actual_main_windows_not_plan_labels() {
    use cc_lb_control::openai_limit_resets::classify_openai_manual_redemption;
    for plan in [
        "pro",
        "prolite",
        "promax",
        "plus",
        "business",
        "team",
        "self_serve_business_prolite",
        "self_serve_business_usage_based",
        "future_plan",
    ] {
        let pair = classify_openai_manual_redemption(
            &manual_usage(plan, &[(18000, json!(21)), (604800, json!(20))]),
            1_800_000_000,
        );
        assert!(
            pair.eligible,
            "reported main windows qualify independently of {plan}"
        );
        assert!(pair.reason.is_none());
        assert_eq!(pair.windows.len(), 2);
        assert_eq!(pair.windows[0].window, "5h");
        assert_eq!(pair.windows[1].window, "7d");
        let weekly_only = classify_openai_manual_redemption(
            &manual_usage(plan, &[(604800, json!(20.1))]),
            1_800_000_000,
        );
        assert!(weekly_only.eligible);
        assert_eq!(
            weekly_only.windows.len(),
            1,
            "no phantom five-hour allowance"
        );
        assert_eq!(weekly_only.windows[0].window, "7d");
    }
    for duration in [2_419_200, 2_505_600, 2_592_000, 2_678_400] {
        let classified = classify_openai_manual_redemption(
            &manual_usage("go", &[(duration, json!(21))]),
            1_800_000_000,
        );
        assert!(classified.eligible);
        assert_eq!(classified.windows.len(), 1);
        assert_eq!(classified.windows[0].window, "monthly");
        assert_eq!(classified.windows[0].duration_seconds, Some(duration));
        assert_eq!(
            classified.windows[0].resets_at_unix_secs,
            Some(4_000_000_000)
        );
    }
    let mut no_plan = manual_usage("ignored", &[(604800, json!(21))]);
    no_plan.as_object_mut().unwrap().remove("plan_type");
    no_plan["title"] = json!("Go monthly full reset");
    let classified = classify_openai_manual_redemption(&no_plan, 1_800_000_000);
    assert!(classified.eligible);
    assert_eq!(
        classified.windows[0].window, "7d",
        "labels cannot relabel native scope"
    );
    for usage in [
        manual_usage("business", &[]),
        manual_usage("plus", &[(12345, json!(100))]),
        json!({"plan_type":"go","rate_limit":{},"additional_rate_limits":[{
            "rate_limit":{"primary_window":{"limit_window_seconds":2592000,"used_percent":100,"reset_at":4_000_000_000u64}}
        }]}),
    ] {
        let classified = classify_openai_manual_redemption(&usage, 1_800_000_000);
        assert!(!classified.eligible);
        assert_eq!(classified.reason.as_deref(), Some("coverage_unreported"));
        assert!(classified.windows.is_empty());
    }
    for usage in [json!({}), json!({"rate_limit":null})] {
        let classified = classify_openai_manual_redemption(&usage, 1_800_000_000);
        assert!(!classified.eligible);
        assert_eq!(classified.reason.as_deref(), Some("usage_unavailable"));
    }
}

#[test]
fn manual_reset_unknown_malformed_duplicate_and_stale_windows_fail_closed() {
    use cc_lb_control::openai_limit_resets::classify_openai_manual_redemption;
    for invalid in [Value::Null, json!("100"), json!(-1), json!(100.1)] {
        let classified = classify_openai_manual_redemption(
            &manual_usage("plus", &[(18000, json!(20)), (604800, invalid)]),
            1_800_000_000,
        );
        assert!(!classified.eligible);
        assert_eq!(classified.reason.as_deref(), Some("incomplete_usage"));
        assert!(classified.windows[1].used_percent.is_none());
    }
    let mut stale = manual_usage("pro", &[(604800, json!(100))]);
    stale["rate_limit"]["primary_window"]["reset_at"] = json!(1_800_000_000);
    let duplicate = manual_usage("pro", &[(604800, json!(100)), (604800, json!(0))]);
    for usage in [stale, duplicate] {
        let classified = classify_openai_manual_redemption(&usage, 1_800_000_000);
        assert!(!classified.eligible);
        assert_eq!(classified.reason.as_deref(), Some("incomplete_usage"));
    }
    let missing_duration =
        json!({"plan_type":"pro","rate_limit":{"primary_window":{"used_percent":100}}});
    assert_eq!(
        classify_openai_manual_redemption(&missing_duration, 1_800_000_000)
            .reason
            .as_deref(),
        Some("coverage_unreported")
    );
    let mut unknown_reset = manual_usage("pro", &[(604800, json!(100))]);
    unknown_reset["rate_limit"]["primary_window"]
        .as_object_mut()
        .unwrap()
        .remove("reset_at");
    let classified = classify_openai_manual_redemption(&unknown_reset, 1_800_000_000);
    assert!(!classified.eligible);
    assert!(classified.windows[0].resets_at_unix_secs.is_none());
    let classified = classify_openai_manual_redemption(&Value::Null, 1_800_000_000);
    assert_eq!(classified.reason.as_deref(), Some("usage_unavailable"));
    let historical: OpenAiLimitResetInventory = serde_json::from_value(json!({
        "available_count":1,"credits":[]
    }))
    .unwrap();
    assert!(
        historical.manual_redemption.is_none(),
        "historical inventory is unknown, not eligible"
    );
}

#[test]
fn manual_reset_any_fresh_covered_allowance_can_qualify_with_unknown_siblings() {
    use cc_lb_control::openai_limit_resets::classify_openai_manual_redemption;
    for usage in [
        manual_usage("plus", &[(18000, json!(21))]),
        manual_usage("plus", &[(604800, json!(21))]),
        manual_usage("plus", &[(18000, json!(21)), (604800, Value::Null)]),
    ] {
        let classified = classify_openai_manual_redemption(&usage, 1_800_000_000);
        assert!(classified.eligible);
        assert!(classified.reason.is_none());
        assert_eq!(
            classified.windows.len(),
            usage["rate_limit"].as_object().unwrap().len(),
            "missing slots do not create phantom allowances"
        );
    }
    for reset in [Value::Null, json!(1_800_000_000), json!(1_700_000_000)] {
        let mut usage = manual_usage("plus", &[(18000, json!(21)), (604800, json!(100))]);
        usage["rate_limit"]["secondary_window"]["reset_at"] = reset;
        let classified = classify_openai_manual_redemption(&usage, 1_800_000_000);
        assert!(classified.eligible);
        assert!(classified.reason.is_none());
        assert!(
            classified.windows[1].used_percent.is_none(),
            "stale usage is not current UI data"
        );
    }
    let usage = manual_usage("plus", &[(18000, json!(20))]);
    let classified = classify_openai_manual_redemption(&usage, 1_800_000_000);
    assert!(!classified.eligible);
    assert_eq!(
        classified.reason.as_deref(),
        Some("below_redemption_threshold")
    );
}

#[tokio::test]
async fn manual_reset_usage_fetch_uses_native_account_headers_without_reserve_opt_in() {
    use cc_lb_control::openai_limit_resets::fetch_openai_reset_usage;
    let fixture = Fixture::new(
        StatusCode::OK,
        manual_usage("pro", &[(604800, json!(20.1))]),
    )
    .await;
    let usage = fetch_openai_reset_usage(
        &make_metadata_http_client(),
        &fixture.base,
        auth(),
        &CancellationToken::new(),
    )
    .await
    .unwrap();
    assert_eq!(usage["plan_type"], "pro");
    let requests = fixture.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].method, "GET");
    assert_eq!(requests[0].path, "/api/codex/usage");
    assert_eq!(
        requests[0].headers["chatgpt-account-id"],
        "selected-account"
    );
    assert_eq!(requests[0].headers["authorization"], "Bearer fixture-token");
    assert_eq!(requests[0].headers["originator"], "codex_cli_rs");
    assert!(
        !requests[0]
            .headers
            .contains_key("x-openai-codex-luna-reserve")
    );
}
