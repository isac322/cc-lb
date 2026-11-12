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
