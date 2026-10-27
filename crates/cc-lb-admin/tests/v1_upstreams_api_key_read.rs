//! API-key upstream read paths and Re-check (plan S3b): `POST
//! /upstreams/{id}/preflight`, `GET /upstreams/{id}/workspaces`, `GET
//! /upstreams/{id}/usage`, and `api_key_access` on list/detail. Test names
//! carry their QA ids (`qa-list.md`).

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use axum::body::Body;
use axum::extract::State;
use axum::http::{HeaderMap, Request, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use cc_lb_admin::{DynamicViewRebinder, router};
use cc_lb_control::DynamicView;
use cc_lb_storage_api::{
    ApiKeyAccessWrite, ApiKeyCapabilities, ApiKeyClass, ApiKeyInference, ApiKeyOrgCapability,
    ApiKeyResolution, ApiKeyType, ApiKeyUsagePollOutcome, ApiKeyWorkspace, ApiKeyWorkspaceMode,
    ApiUsageAttribution, RemediationCode, RequestEvent, RequestEventStore, UpstreamApiKeyStore,
    UpstreamApiUsageBucketWrite, UpstreamCreate, UpstreamKind, UpstreamStore, UsageRollupStore,
};
use fake_anthropic::{FakeApiKey, FakeWorkspace, IDENTITY_LINKED_MESSAGE, KeyPolicy, Script};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::Notify;
use tower::ServiceExt;
use url::Url;
use uuid::Uuid;

use crate::api_key_support::{
    ADMIN_TOKEN, ApiKeyHarness, HARNESS_NOW_UNIX_SECS as NOW, HarnessResponse, UPSTREAMS,
    spawn_api_key_harness,
};

const BND: &str = "sk-ant-api03-BOUNDKEYbound0000000000000000BBBB";
const BND_HINT: &str = "sk-ant-api03-...BBBB";
const ORG: &str = "sk-ant-api03-ORGSCOPEDorg00000000000000000OOOO";
const ORG_HINT: &str = "sk-ant-api03-...OOOO";
const ORG_2: &str = "sk-ant-api03-SECONDorgkey00000000000000000TTTT";
const BAD_KEY: &str = "sk-ant-api03-REVOKEDkey000000000000000000DDDD";
const ORG_2_HINT: &str = "sk-ant-api03-...TTTT";
const MODELS: &str = "/v1/models";
const COUNT_TOKENS: &str = "/v1/messages/count_tokens";
const ORG_PREFIX: &str = "/v1/organizations";
const WORKSPACES: &str = "/v1/organizations/workspaces";

const HOUR: u64 = 3_600;
const DAY: u64 = 86_400;
/// 2026-10-01T00:00:00Z / 2026-11-01T00:00:00Z.
const OCT_1: u64 = 1_790_812_800;
const NOV_1: u64 = 1_793_491_200;
const DEC_1: u64 = 1_796_083_200;
const JAN_1_2027: u64 = 1_798_761_600;
const FEB_1_2027: u64 = 1_801_440_000;

// ---------------------------------------------------------------------------
// Fixtures

fn anthropic_hint(key: &str) -> String {
    format!(
        "{}...{}",
        key.get(..16).unwrap_or_default(),
        key.get(key.len() - 4..).unwrap_or_default()
    )
}

fn bound_policy() -> KeyPolicy {
    KeyPolicy {
        bound_workspace: Some("wrkspc_B".to_owned()),
        ..KeyPolicy::default()
    }
}

fn org_policy(key: &str) -> KeyPolicy {
    KeyPolicy {
        requires_workspace: true,
        workspaces: vec![
            FakeWorkspace::new("wrkspc_P", "Production"),
            FakeWorkspace::new("wrkspc_S", "Staging"),
        ],
        api_key_items: vec![FakeApiKey {
            id: "apikey_01".to_owned(),
            partial_key_hint: anthropic_hint(key),
            principal: Some(json!({"type": "service_account_actor"})),
            scope: Some(json!({"type": "organization"})),
        }],
        ..KeyPolicy::default()
    }
}

fn org_access(
    api_key_id: Option<&str>,
    usage_report: ApiKeyOrgCapability,
    checked_at: u64,
) -> ApiKeyAccessWrite {
    ApiKeyAccessWrite {
        verified: true,
        key_class: ApiKeyClass::OrgScoped,
        key_hint: ORG_HINT.to_owned(),
        api_key_id: api_key_id.map(str::to_owned),
        needs_workspace: true,
        capabilities: ApiKeyCapabilities {
            inference: ApiKeyInference::Ok,
            workspace: ApiKeyWorkspace {
                mode: ApiKeyWorkspaceMode::Selected,
                id: Some("wrkspc_P".to_owned()),
                name: Some("Production".to_owned()),
            },
            workspace_list: ApiKeyOrgCapability::Ok,
            api_key_resolution: if api_key_id.is_some() {
                ApiKeyResolution::Resolved
            } else {
                ApiKeyResolution::Unresolved
            },
            key_type: Some(ApiKeyType::ServiceAccount),
        },
        remediation: if api_key_id.is_some() {
            Vec::new()
        } else {
            vec![RemediationCode::KeyIdUnresolved]
        },
        usage_report_state: Some(usage_report),
        last_error: None,
        checked_at_unix_secs: Some(checked_at),
        last_attempt_at_unix_secs: checked_at,
    }
}

fn bound_access() -> ApiKeyAccessWrite {
    ApiKeyAccessWrite {
        verified: true,
        key_class: ApiKeyClass::WorkspaceBound,
        key_hint: BND_HINT.to_owned(),
        api_key_id: None,
        needs_workspace: false,
        capabilities: ApiKeyCapabilities {
            inference: ApiKeyInference::Ok,
            workspace: ApiKeyWorkspace {
                mode: ApiKeyWorkspaceMode::Bound,
                id: Some("wrkspc_B".to_owned()),
                name: None,
            },
            workspace_list: ApiKeyOrgCapability::NotApplicable,
            api_key_resolution: ApiKeyResolution::NotApplicable,
            key_type: None,
        },
        remediation: vec![RemediationCode::KeyWorkspaceScopedUsageUnavailable],
        usage_report_state: Some(ApiKeyOrgCapability::NotApplicable),
        last_error: None,
        checked_at_unix_secs: Some(NOW - 60),
        last_attempt_at_unix_secs: NOW - 60,
    }
}

fn bucket(
    attribution: ApiUsageAttribution,
    subject: &str,
    width: u32,
    start: u64,
    model: &str,
    uncached: u64,
    list_cost_micros: u64,
) -> UpstreamApiUsageBucketWrite {
    UpstreamApiUsageBucketWrite {
        attribution,
        subject_id: subject.to_owned(),
        bucket_width_secs: width,
        bucket_start_unix_secs: start,
        model: model.to_owned(),
        uncached_input_tokens: uncached,
        cache_creation_5m_input_tokens: 0,
        cache_creation_1h_input_tokens: 0,
        cache_read_input_tokens: 0,
        output_tokens: 0,
        list_cost_micros,
    }
}

fn key_bucket(
    width: u32,
    start: u64,
    model: &str,
    tokens: u64,
    cost: u64,
) -> UpstreamApiUsageBucketWrite {
    bucket(
        ApiUsageAttribution::ApiKey,
        "apikey_01",
        width,
        start,
        model,
        tokens,
        cost,
    )
}

fn poll_success(buckets: Vec<UpstreamApiUsageBucketWrite>) -> ApiKeyUsagePollOutcome {
    ApiKeyUsagePollOutcome {
        attempted_at_unix_secs: NOW,
        usage_report_state: None,
        access_revision_seen: 0,
        consecutive_failures: 0,
        next_poll_after_unix_secs: None,
        last_success_at_unix_secs: Some(NOW),
        legacy_key_hint: None,
        claim_token: None,
        buckets,
    }
}

fn poll_failure(
    access_revision_seen: u64,
    usage_report_state: Option<ApiKeyOrgCapability>,
    consecutive_failures: u32,
) -> ApiKeyUsagePollOutcome {
    ApiKeyUsagePollOutcome {
        attempted_at_unix_secs: NOW,
        usage_report_state,
        access_revision_seen,
        consecutive_failures,
        next_poll_after_unix_secs: None,
        last_success_at_unix_secs: None,
        legacy_key_hint: None,
        claim_token: None,
        buckets: Vec::new(),
    }
}

/// Legacy API-key upstream at the fake plus an org-scoped snapshot.
async fn seeded_org(
    h: &ApiKeyHarness,
    name: &str,
    api_key_id: Option<&str>,
    usage_report: ApiKeyOrgCapability,
) -> Uuid {
    let record = h.seed_legacy_api_key_upstream(name, ORG).await;
    h.seed_details(record.id, org_access(api_key_id, usage_report, NOW - 60))
        .await;
    record.id
}

/// Org-scoped upstream whose poller has reported `buckets`.
async fn reported_org(
    h: &ApiKeyHarness,
    name: &str,
    api_key_id: Option<&str>,
    buckets: Vec<UpstreamApiUsageBucketWrite>,
) -> Uuid {
    let id = seeded_org(h, name, api_key_id, ApiKeyOrgCapability::Ok).await;
    h.seed_usage_buckets(id, poll_success(buckets)).await;
    id
}

async fn create_oauth_upstream(h: &ApiKeyHarness, name: &str) -> Uuid {
    UpstreamStore::create(
        h.storage.as_ref(),
        UpstreamCreate {
            id: Uuid::new_v4(),
            name: name.to_owned(),
            kind: UpstreamKind::AnthropicOauth,
            base_url: None,
            api_key_ciphertext: None,
            oauth_tokens: None,
            warmup_enabled: false,
            warmup_dialect_plugin: None,
            workspace_id: None,
            api_key_access: None,
        },
    )
    .await
    .expect("seed oauth upstream")
    .id
}

async fn recheck(h: &ApiKeyHarness, id: impl std::fmt::Display) -> HarnessResponse {
    h.request(
        "POST",
        &format!("{UPSTREAMS}/{id}/preflight"),
        Some(json!({})),
        None,
    )
    .await
}

async fn workspaces(h: &ApiKeyHarness, id: Uuid) -> HarnessResponse {
    h.request("GET", &format!("{UPSTREAMS}/{id}/workspaces"), None, None)
        .await
}

async fn usage(h: &ApiKeyHarness, id: Uuid, query: &str) -> HarnessResponse {
    let response = h
        .request(
            "GET",
            &format!("{UPSTREAMS}/{id}/usage?{query}"),
            None,
            None,
        )
        .await;
    assert_eq!(response.status, StatusCode::OK, "{}", response.body);
    response
}

fn series_keys(body: &Value) -> Vec<String> {
    body["series"]
        .as_array()
        .expect("series array")
        .iter()
        .map(|series| series["key"].as_str().expect("series key").to_owned())
        .collect()
}

fn series_input_tokens(body: &Value) -> u64 {
    body["series"]
        .as_array()
        .expect("series array")
        .iter()
        .flat_map(|series| series["buckets"].as_array().expect("buckets").iter())
        .map(|bucket| bucket["input_tokens"].as_u64().expect("input tokens"))
        .sum()
}

fn assert_nested(response: &HarnessResponse, status: StatusCode, code: &str) {
    assert_eq!(response.status, status, "{}", response.body);
    assert_eq!(response.body["error"]["code"], code, "{}", response.body);
    let message = response.body["error"]["message"]
        .as_str()
        .expect("operator message");
    assert!(!message.is_empty());
}

/// No 12-byte window of the key's secret body appears in `text` (SEC-01
/// style scan). The public `sk-ant-api03-` family prefix is part of the
/// allowed `key_hint`, so only what follows it is secret.
fn assert_no_key_material(text: &str, key: &str) {
    let secret = key.strip_prefix("sk-ant-api03-").unwrap_or(key);
    for start in 0..=secret.len() - 12 {
        let window = &secret[start..start + 12];
        assert!(!text.contains(window), "{text:?} leaks {window:?}");
    }
}

fn usage_event(upstream_id: Uuid, ts: u64, request_id: &str, input_tokens: u64) -> RequestEvent {
    RequestEvent {
        ts,
        ts_ms: Some(ts * 1_000),
        event_id: Some(format!("event-{request_id}")),
        request_id: request_id.to_owned(),
        principal_id: Some(Uuid::from_u128(0xA1).to_string()),
        key_id: Some("test-key".to_owned()),
        upstream_id: Some(upstream_id),
        upstream_name: Some("rollup".to_owned()),
        model: Some("claude-sonnet-4-5".to_owned()),
        status: 200,
        input_tokens: Some(input_tokens),
        output_tokens: Some(0),
        cache_creation_input_tokens: Some(0),
        cache_read_input_tokens: Some(0),
        cost_usd_micros: Some(0),
        duration_ms: 10,
        ..RequestEvent::default()
    }
}

fn outcome_event(
    upstream_id: Uuid,
    ts: u64,
    request_id: &str,
    status: u16,
    error_type: Option<&str>,
    message: Option<&str>,
) -> RequestEvent {
    RequestEvent {
        ts,
        ts_ms: Some(ts * 1_000),
        event_id: Some(format!("event-{request_id}")),
        request_id: request_id.to_owned(),
        upstream_id: Some(upstream_id),
        upstream_name: Some("billing".to_owned()),
        model: Some("claude-sonnet-4-5".to_owned()),
        status,
        error_code: (status >= 400).then(|| "upstream_4xx".to_owned()),
        upstream_error_type: error_type.map(str::to_owned),
        upstream_error_message: message.map(str::to_owned),
        duration_ms: 10,
        ..RequestEvent::default()
    }
}

async fn append(h: &ApiKeyHarness, events: &[RequestEvent]) {
    for event in events {
        h.storage
            .append_request_event(event)
            .await
            .expect("append request event");
    }
}

async fn append_and_roll_up(h: &ApiKeyHarness, events: &[RequestEvent]) {
    append(h, events).await;
    h.storage.rollup_usage_once().await.expect("rollup");
}

// ---------------------------------------------------------------------------
// RCK: Re-check

#[tokio::test]
async fn rck_01_recheck_probes_the_stored_key() {
    let h = spawn_api_key_harness().await;
    h.set_policy(BND, bound_policy());
    let record = h.seed_legacy_api_key_upstream("bound", BND).await;

    let response = recheck(&h, record.id).await;

    assert_eq!(response.status, StatusCode::OK, "{}", response.body);
    assert_eq!(response.header("cache-control"), Some("no-store"));
    assert_eq!(response.body["verdict"], "ok");
    assert_eq!(response.body["key_hint"], BND_HINT);
    assert_eq!(
        response.body["classification"]["key_class"],
        "workspace_bound"
    );
    let counted = h.requests(COUNT_TOKENS);
    assert!(!counted.is_empty());
    assert!(
        counted
            .iter()
            .all(|request| request.header("x-api-key") == Some(BND))
    );
    assert_eq!(h.requests(ORG_PREFIX).len(), 0);
}

#[tokio::test]
async fn rck_02_recheck_persists_the_snapshot_for_a_legacy_row() {
    let h = spawn_api_key_harness().await;
    h.set_policy(BND, bound_policy());
    let record = h.seed_legacy_api_key_upstream("legacy", BND).await;
    let before = h.get_upstream(record.id).await;
    assert!(before.body["api_key_access"].is_null(), "{}", before.body);

    assert_eq!(recheck(&h, record.id).await.status, StatusCode::OK);

    let after = h.get_upstream(record.id).await;
    let access = &after.body["api_key_access"];
    assert_eq!(access["verified"], true, "{}", after.body);
    assert_eq!(access["key_class"], "workspace_bound");
    assert_eq!(access["inference"], "ok");
    assert_eq!(access["workspace"]["mode"], "bound");
    assert_eq!(access["workspace"]["id"], "wrkspc_B");
    assert_eq!(access["checked_at_unix_secs"], NOW);
    let details = h.details(record.id).await.expect("details row");
    assert_eq!(details.key_hint, BND_HINT);
    assert_eq!(details.checked_at_unix_secs, Some(NOW));
}

struct CountingRebinder(AtomicUsize);

#[async_trait]
impl DynamicViewRebinder for CountingRebinder {
    async fn rebuild_dynamic_view(
        &self,
        _current_generation: u64,
    ) -> anyhow::Result<Arc<DynamicView>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        anyhow::bail!("rebuild counted by the test")
    }
}

async fn send(
    app: &axum::Router,
    method: &str,
    uri: &str,
    if_match: Option<&str>,
) -> (StatusCode, Value) {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header(header::AUTHORIZATION, format!("Bearer {ADMIN_TOKEN}"));
    let body = if method == "POST" {
        builder = builder.header(header::CONTENT_TYPE, "application/json");
        Body::from("{}")
    } else {
        Body::empty()
    };
    if let Some(if_match) = if_match {
        builder = builder.header(header::IF_MATCH, if_match);
    }
    let response = app
        .clone()
        .oneshot(builder.body(body).expect("request builds"))
        .await
        .expect("router answers");
    let status = response.status();
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("body collects")
        .to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

// RCK-03 + ACC-05: Re-check writes the details table only.
#[tokio::test]
async fn rck_03_acc_05_recheck_never_bumps_the_spec_or_rebuilds_the_view() {
    let h = spawn_api_key_harness().await;
    h.set_policy(BND, bound_policy());
    h.set_policy(
        BAD_KEY,
        KeyPolicy {
            models: Script::error(401, "authentication_error", "invalid x-api-key"),
            count_tokens: Script::error(401, "authentication_error", "invalid x-api-key"),
            ..KeyPolicy::default()
        },
    );
    let rebinder = Arc::new(CountingRebinder(AtomicUsize::new(0)));
    let mut state = h.state.clone();
    state.dynamic_view_rebinder = Some(rebinder.clone());
    let app = router(state);
    let good = h.seed_legacy_api_key_upstream("good", BND).await;
    let bad = h.seed_legacy_api_key_upstream("revoked", BAD_KEY).await;
    let good_before = h.record(good.id).await;
    let bad_before = h.record(bad.id).await;

    for _ in 0..5 {
        for id in [good.id, bad.id] {
            let (status, body) =
                send(&app, "POST", &format!("{UPSTREAMS}/{id}/preflight"), None).await;
            assert_eq!(status, StatusCode::OK, "{body}");
        }
    }

    assert_eq!(h.record(good.id).await, good_before);
    assert_eq!(h.record(bad.id).await, bad_before);
    assert_eq!(rebinder.0.load(Ordering::SeqCst), 0);
    assert_eq!(
        h.details(good.id).await.expect("details").key_class,
        ApiKeyClass::WorkspaceBound
    );

    // The counter does observe spec mutations.
    let (status, body) = send(
        &app,
        "DELETE",
        &format!("{UPSTREAMS}/{}", bad.id),
        Some(&format!("W/\"{}\"", bad_before.revision)),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT, "{body}");
    assert!(rebinder.0.load(Ordering::SeqCst) > 0);
}

#[tokio::test]
async fn rck_04_revoked_key_reads_denied_without_key_material() {
    let h = spawn_api_key_harness().await;
    h.set_policy(BAD_KEY, bound_policy());
    let record = h
        .seed_legacy_api_key_upstream("soon-revoked", BAD_KEY)
        .await;
    assert_eq!(recheck(&h, record.id).await.body["verdict"], "ok");

    h.set_policy(
        BAD_KEY,
        KeyPolicy {
            models: Script::error(401, "authentication_error", "invalid x-api-key"),
            count_tokens: Script::error(401, "authentication_error", "invalid x-api-key"),
            ..KeyPolicy::default()
        },
    );
    let response = recheck(&h, record.id).await;

    assert_eq!(response.status, StatusCode::OK);
    assert_eq!(response.body["verdict"], "rejected");
    assert_eq!(response.body["capabilities"]["inference"], "denied");
    assert_eq!(response.body["remediation"][0]["code"], "key_invalid");
    let detail = h.get_upstream(record.id).await;
    let access = &detail.body["api_key_access"];
    assert_eq!(access["verified"], false, "{}", detail.body);
    assert_eq!(access["inference"], "denied");
    let last_error = access["last_error"].as_str().expect("operator last_error");
    // The harness's org origin is the fake, so the key was checked "at
    // Anthropic" and the reason says so (D8).
    assert_eq!(
        last_error,
        "Anthropic rejected this key. Check for a typo, or the key is expired or deleted."
    );
    // A rejected key has no workspace fact: `required`, no id (D9).
    assert_eq!(
        response.body["classification"]["workspace"],
        json!({"mode": "required"})
    );
    assert_eq!(access["workspace"], json!({"mode": "required"}));
    assert_no_key_material(last_error, BAD_KEY);
    assert_no_key_material(&response.body.to_string(), BAD_KEY);
    assert_no_key_material(&detail.body.to_string(), BAD_KEY);
}

#[tokio::test]
async fn rck_05_recheck_sends_the_stored_workspace_and_persists_not_found() {
    let h = spawn_api_key_harness().await;
    h.set_policy(ORG, org_policy(ORG));
    let created = h
        .create_api_key_upstream("org", ORG, json!({"workspace_id": "wrkspc_P"}))
        .await;
    assert_eq!(created.status, StatusCode::CREATED, "{}", created.body);
    let id = created.id();
    h.recorder.clear();

    let ok = recheck(&h, id).await;
    assert_eq!(ok.body["verdict"], "ok", "{}", ok.body);
    let counted = h.requests(COUNT_TOKENS);
    assert!(!counted.is_empty());
    assert!(
        counted
            .iter()
            .all(|request| request.header("anthropic-workspace-id") == Some("wrkspc_P"))
    );

    // The workspace is deleted upstream: the stored header now gets 404.
    h.set_policy(
        ORG,
        KeyPolicy {
            workspaces: vec![FakeWorkspace::new("wrkspc_S", "Staging")],
            ..org_policy(ORG)
        },
    );
    let missing = recheck(&h, id).await;

    assert_eq!(missing.status, StatusCode::OK);
    assert_eq!(
        missing.body["verdict"], "needs_workspace",
        "{}",
        missing.body
    );
    assert_eq!(
        missing.body["remediation"][0]["code"],
        "workspace_not_found"
    );
    let details = h.details(id).await.expect("details");
    assert!(
        details
            .remediation
            .contains(&RemediationCode::WorkspaceNotFound)
    );
}

#[tokio::test]
async fn rck_07_recheck_restores_usage_reports_after_demotion() {
    let h = spawn_api_key_harness().await;
    h.set_policy(ORG, org_policy(ORG));
    let created = h
        .create_api_key_upstream("org", ORG, json!({"workspace_id": "wrkspc_P"}))
        .await;
    assert_eq!(created.status, StatusCode::CREATED, "{}", created.body);
    let id = created.id();
    let access_revision = h
        .details(id)
        .await
        .expect("details at create")
        .access_revision;
    // The poller demotes after a 403.
    h.seed_usage_buckets(
        id,
        poll_failure(access_revision, Some(ApiKeyOrgCapability::Denied), 1),
    )
    .await;
    assert_eq!(
        h.get_upstream(id).await.body["api_key_access"]["usage_report"],
        "denied"
    );
    h.clock.advance_secs(60);

    let response = recheck(&h, id).await;

    assert_eq!(
        response.body["capabilities"]["usage_report"], "ok",
        "{}",
        response.body
    );
    assert_eq!(
        h.get_upstream(id).await.body["api_key_access"]["usage_report"],
        "ok"
    );
    let candidates = h
        .storage
        .list_api_key_usage_poll_candidates(NOW + 60, Some(id), 10)
        .await
        .expect("candidates");
    assert_eq!(candidates.len(), 1, "the poller picks it up again");
}

#[tokio::test]
async fn rck_08_wrong_kind_missing_and_deleted_upstreams_make_no_calls() {
    let h = spawn_api_key_harness().await;
    let oauth = create_oauth_upstream(&h, "oauth").await;
    let deleted = h.seed_legacy_api_key_upstream("deleted", BND).await;
    UpstreamStore::soft_delete(h.storage.as_ref(), deleted.id, deleted.revision)
        .await
        .expect("soft delete");

    let not_api_key = recheck(&h, oauth).await;
    assert_eq!(not_api_key.status, StatusCode::BAD_REQUEST);
    assert_eq!(not_api_key.body["error"], "not_api_key_upstream");

    assert_eq!(
        recheck(&h, Uuid::new_v4()).await.status,
        StatusCode::NOT_FOUND
    );
    assert_eq!(recheck(&h, deleted.id).await.status, StatusCode::NOT_FOUND);
    let bad_id = recheck(&h, "not-a-uuid").await;
    assert_eq!(bad_id.status, StatusCode::BAD_REQUEST);
    assert_eq!(bad_id.body["error"], "invalid_upstream_id");

    // The same rails guard the other stored-key reads.
    let workspaces = h
        .request(
            "GET",
            &format!("{UPSTREAMS}/{oauth}/workspaces"),
            None,
            None,
        )
        .await;
    assert_eq!(workspaces.body["error"], "not_api_key_upstream");
    let usage = h
        .request(
            "GET",
            &format!("{UPSTREAMS}/{oauth}/usage?range=24h"),
            None,
            None,
        )
        .await;
    assert_eq!(usage.body["error"], "not_api_key_upstream");
    assert_eq!(h.outbound_count(), 0);
}

#[tokio::test]
async fn rck_10_bound_key_stored_with_a_foreign_workspace_is_caught() {
    let h = spawn_api_key_harness().await;
    h.set_policy(BND, bound_policy());
    let created = h
        .create_api_key_upstream(
            "mismatched",
            BND,
            json!({"skip_preflight": true, "workspace_id": "wrkspc_P"}),
        )
        .await;
    assert_eq!(created.status, StatusCode::CREATED, "{}", created.body);
    let id = created.id();

    let response = recheck(&h, id).await;

    assert_eq!(response.status, StatusCode::OK);
    assert_ne!(response.body["verdict"], "ok", "{}", response.body);
    assert_eq!(
        response.body["remediation"][0]["code"],
        "workspace_not_found"
    );
    let details = h.details(id).await.expect("details");
    assert!(
        details
            .remediation
            .contains(&RemediationCode::WorkspaceNotFound)
    );
    assert!(!details.verified);
    let last_error = details.last_error.expect("last_error names the workspace");
    assert!(last_error.contains("wrkspc_B"), "{last_error}");
    assert_eq!(
        h.get_upstream(id).await.body["api_key_access"]["verified"],
        false
    );
}

// ---------------------------------------------------------------------------
// WSL: workspace list

#[tokio::test]
async fn wsl_01_lists_workspaces_with_the_stored_key() {
    let h = spawn_api_key_harness().await;
    h.set_policy(ORG, org_policy(ORG));
    let id = seeded_org(&h, "org", Some("apikey_01"), ApiKeyOrgCapability::Ok).await;

    let response = workspaces(&h, id).await;

    assert_eq!(response.status, StatusCode::OK, "{}", response.body);
    assert_eq!(
        response.body,
        json!({"workspaces": [
            {"id": "wrkspc_P", "name": "Production"},
            {"id": "wrkspc_S", "name": "Staging"},
        ]})
    );
    let listed = h.requests(WORKSPACES);
    assert!(!listed.is_empty() && listed.len() <= 3);
    assert!(
        listed
            .iter()
            .all(|request| request.header("x-api-key") == Some(ORG))
    );
    assert_no_key_material(&response.body.to_string(), ORG);
}

// WSL-02 + WSL-06: refusal is the nested body with an operator message.
#[tokio::test]
async fn wsl_02_06_denied_list_is_nested_workspace_list_denied() {
    let h = spawn_api_key_harness().await;
    h.set_policy(
        ORG,
        KeyPolicy {
            workspaces_list: Script::error(403, "permission_error", "not allowed"),
            ..org_policy(ORG)
        },
    );
    let id = seeded_org(&h, "org", Some("apikey_01"), ApiKeyOrgCapability::Ok).await;

    let response = workspaces(&h, id).await;

    assert_nested(&response, StatusCode::FORBIDDEN, "workspace_list_denied");
    assert_no_key_material(&response.body.to_string(), ORG);
}

#[tokio::test]
async fn wsl_03_custom_host_is_refused_with_zero_outbound() {
    let h = spawn_api_key_harness().await;
    h.set_policy(ORG, org_policy(ORG));
    let created = h
        .create_api_key_upstream(
            "custom",
            ORG,
            json!({"base_url": "http://127.0.0.1:9/", "skip_preflight": true}),
        )
        .await;
    assert_eq!(created.status, StatusCode::CREATED, "{}", created.body);
    h.recorder.clear();

    let response = workspaces(&h, created.id()).await;

    assert_nested(&response, StatusCode::FORBIDDEN, "non_anthropic_base_url");
    assert_eq!(h.outbound_count(), 0);
}

// WSL-05 + WSL-06: provider failures are nested `probe_unreachable`.
#[tokio::test]
async fn wsl_05_06_upstream_error_is_nested_probe_unreachable() {
    let h = spawn_api_key_harness().await;
    h.set_policy(
        ORG,
        KeyPolicy {
            workspaces_list: Script::error(500, "api_error", "internal"),
            ..org_policy(ORG)
        },
    );
    let id = seeded_org(&h, "org", Some("apikey_01"), ApiKeyOrgCapability::Ok).await;

    let response = workspaces(&h, id).await;

    assert_nested(&response, StatusCode::BAD_GATEWAY, "probe_unreachable");
    assert!(response.body["error"]["message"].is_string());
}

// ---------------------------------------------------------------------------
// ACC: `api_key_access` on list and detail

#[tokio::test]
async fn acc_01_access_is_present_only_on_api_key_upstreams() {
    let h = spawn_api_key_harness().await;
    let api_key = seeded_org(&h, "org", Some("apikey_01"), ApiKeyOrgCapability::Ok).await;
    let oauth = create_oauth_upstream(&h, "oauth").await;

    let list = h.request("GET", UPSTREAMS, None, None).await;
    assert_eq!(list.status, StatusCode::OK);
    let rows = list.body["upstreams"].as_array().expect("upstream list");
    let row = |id: Uuid| {
        rows.iter()
            .find(|row| row["id"] == id.to_string())
            .expect("listed")
            .clone()
    };
    for body in [row(api_key), h.get_upstream(api_key).await.body] {
        let access = &body["api_key_access"];
        assert_eq!(access["verified"], true, "{body}");
        assert_eq!(access["key_class"], "org_scoped");
        assert_eq!(access["workspace"]["id"], "wrkspc_P");
        assert_eq!(access["usage_report"], "ok");
        assert_eq!(access["key_type"], "service_account");
    }
    for body in [row(oauth), h.get_upstream(oauth).await.body] {
        assert!(body["api_key_access"].is_null(), "{body}");
    }
}

#[tokio::test]
async fn acc_02_legacy_row_has_no_access() {
    let h = spawn_api_key_harness().await;
    let legacy = h.seed_legacy_api_key_upstream("legacy", ORG).await;

    let detail = h.get_upstream(legacy.id).await;
    assert_eq!(detail.status, StatusCode::OK);
    assert!(detail.body["api_key_access"].is_null(), "{}", detail.body);
    let list = h.request("GET", UPSTREAMS, None, None).await;
    assert!(list.body["upstreams"][0]["api_key_access"].is_null());
    assert_eq!(h.outbound_count(), 0);
}

#[tokio::test]
async fn acc_04_legacy_usage_reads_never_checked() {
    let h = spawn_api_key_harness().await;
    let legacy = h.seed_legacy_api_key_upstream("legacy", ORG).await;

    let body = usage(&h, legacy.id, "range=24h&step=hour").await.body;

    assert_eq!(body["source"], "estimated");
    assert_eq!(body["attribution"], "api_key");
    assert_eq!(body["stale"], false);
    assert_eq!(body["fix"], json!({"code": "usage_never_checked"}));
    assert_eq!(body["group_by"], "model");
}

#[tokio::test]
async fn usg_provider_api_keys_use_local_estimated_request_events() {
    let h = spawn_api_key_harness().await;
    for (index, kind) in [UpstreamKind::OpenaiApiKey, UpstreamKind::GeminiApiKey]
        .into_iter()
        .enumerate()
    {
        let created = UpstreamStore::create(
            h.storage.as_ref(),
            UpstreamCreate {
                id: Uuid::new_v4(),
                name: format!("provider-{index}"),
                kind,
                base_url: None,
                api_key_ciphertext: None,
                oauth_tokens: None,
                warmup_enabled: false,
                warmup_dialect_plugin: None,
                workspace_id: None,
                api_key_access: None,
            },
        )
        .await
        .expect("seed provider API-key upstream");
        let ciphertext = h
            .state
            .aead
            .encrypt(b"provider-key", created.id.as_bytes())
            .expect("seal provider API key");
        h.storage
            .update_api_key_secret(created.id, Some(ciphertext))
            .await
            .expect("store provider API key");
        append_and_roll_up(
            &h,
            &[usage_event(
                created.id,
                NOW - 600,
                &format!("provider-{index}"),
                73,
            )],
        )
        .await;

        let body = usage(&h, created.id, "range=24h&step=hour").await.body;
        assert_eq!(body["source"], "estimated", "{kind:?}");
        assert_eq!(body["attribution"], "api_key", "{kind:?}");
        assert_eq!(series_input_tokens(&body), 73, "{kind:?}");
        assert!(body["attribution_workspace_id"].is_null(), "{kind:?}");
    }
}

#[tokio::test]
async fn usg_all_oauth_kinds_remain_not_api_key_upstream() {
    let h = spawn_api_key_harness().await;
    for (index, kind) in [
        UpstreamKind::AnthropicOauth,
        UpstreamKind::OpenaiOauth,
        UpstreamKind::GeminiOauth,
    ]
    .into_iter()
    .enumerate()
    {
        let id = UpstreamStore::create(
            h.storage.as_ref(),
            UpstreamCreate {
                id: Uuid::new_v4(),
                name: format!("oauth-{index}"),
                kind,
                base_url: None,
                api_key_ciphertext: None,
                oauth_tokens: None,
                warmup_enabled: false,
                warmup_dialect_plugin: None,
                workspace_id: None,
                api_key_access: None,
            },
        )
        .await
        .expect("seed OAuth upstream")
        .id;
        let response = h
            .request(
                "GET",
                &format!("{UPSTREAMS}/{id}/usage?range=24h"),
                None,
                None,
            )
            .await;
        assert_eq!(response.status, StatusCode::BAD_REQUEST, "{kind:?}");
        assert_eq!(response.body["error"], "not_api_key_upstream", "{kind:?}");
    }
}

// ---------------------------------------------------------------------------
// USG: `GET /usage`

#[tokio::test]
async fn usg_01_reported_usage_for_the_resolved_key() {
    let h = spawn_api_key_harness().await;
    let id = reported_org(
        &h,
        "org",
        Some("apikey_01"),
        vec![
            key_bucket(3_600, NOW - 2 * HOUR, "claude-sonnet-4-5", 100, 1_000),
            key_bucket(3_600, NOW - HOUR, "", 5, 7),
            // Outside the 24h window.
            key_bucket(3_600, NOW - 2 * DAY, "claude-sonnet-4-5", 999, 999),
        ],
    )
    .await;

    let body = usage(&h, id, "range=24h&step=hour").await.body;

    assert_eq!(body["source"], "real");
    assert_eq!(body["attribution"], "api_key");
    assert_eq!(body["stale"], false);
    assert!(body["fix"].is_null());
    assert_eq!(body["step"], "hour");
    assert_eq!(body["group_by"], "model");
    assert_eq!(
        body["totals"],
        json!({"cost_usd_micros": 1_007, "cost_is_estimate": true})
    );
    let mut keys = series_keys(&body);
    keys.sort();
    assert_eq!(keys, vec!["Other", "claude-sonnet-4-5"]);
    assert_eq!(series_input_tokens(&body), 105);
}

#[tokio::test]
async fn usg_01_minute_step_answers_hour_buckets() {
    let h = spawn_api_key_harness().await;
    let id = reported_org(
        &h,
        "org",
        Some("apikey_01"),
        vec![key_bucket(3_600, NOW - HOUR, "claude-sonnet-4-5", 10, 1)],
    )
    .await;

    let body = usage(&h, id, "range=1h&step=minute").await.body;

    assert_eq!(body["source"], "real");
    assert_eq!(body["step"], "hour");
    assert_eq!(series_input_tokens(&body), 10);
}

#[tokio::test]
async fn usg_02_unresolved_key_reads_workspace_usage() {
    let h = spawn_api_key_harness().await;
    let id = reported_org(
        &h,
        "org",
        None,
        vec![
            bucket(
                ApiUsageAttribution::Workspace,
                "wrkspc_P",
                3_600,
                NOW - HOUR,
                "claude-sonnet-4-5",
                40,
                4,
            ),
            bucket(
                ApiUsageAttribution::Workspace,
                "wrkspc_S",
                3_600,
                NOW - HOUR,
                "claude-sonnet-4-5",
                500,
                50,
            ),
        ],
    )
    .await;

    let body = usage(&h, id, "range=24h&step=hour").await.body;

    assert_eq!(body["source"], "real");
    assert_eq!(body["attribution"], "workspace");
    // The figures name their own subject, not the upstream's current metadata.
    assert_eq!(body["attribution_workspace_id"], "wrkspc_P");
    assert!(body["fix"].is_null());
    assert_eq!(series_input_tokens(&body), 40);
}

// USG-17: the last report time is the stored poll success, whatever the
// range, never a bucket edge or the window start.
#[tokio::test]
async fn usg_17_last_reported_is_the_stored_poll_success_for_every_range() {
    let h = spawn_api_key_harness().await;
    // The only bucket is three days old: outside the 1h and 24h windows.
    let id = reported_org(
        &h,
        "org",
        Some("apikey_01"),
        vec![key_bucket(3_600, NOW - 3 * DAY, "claude-sonnet-4-5", 9, 1)],
    )
    .await;
    // A later failed poll keeps the success time it does not replace.
    h.seed_usage_buckets(id, poll_failure(0, None, 2)).await;

    for query in [
        "range=1h&step=minute",
        "range=24h&step=hour",
        "range=7d&step=hour",
    ] {
        let body = usage(&h, id, query).await.body;
        assert_eq!(body["source"], "real", "{query}");
        assert_eq!(body["last_reported_unix_secs"], NOW, "{query}");
        assert!(body["attribution_workspace_id"].is_null(), "{query}");
    }

    let never = seeded_org(&h, "never", Some("apikey_02"), ApiKeyOrgCapability::Ok).await;
    let body = usage(&h, never, "range=24h&step=hour").await.body;
    assert_eq!(body["source"], "estimated");
    assert!(body["last_reported_unix_secs"].is_null());
}

#[tokio::test]
async fn usg_03_bound_key_is_estimated_from_its_own_rollups() {
    let h = spawn_api_key_harness().await;
    let record = h.seed_legacy_api_key_upstream("bound", BND).await;
    h.seed_details(record.id, bound_access()).await;
    append_and_roll_up(&h, &[usage_event(record.id, NOW - 600, "bound-1", 70)]).await;

    let body = usage(&h, record.id, "range=24h&step=hour").await.body;

    assert_eq!(body["source"], "estimated");
    assert_eq!(body["attribution"], "api_key");
    assert_eq!(
        body["fix"],
        json!({"code": "key_workspace_scoped_usage_unavailable"})
    );
    assert_eq!(series_input_tokens(&body), 70);
    assert_eq!(body["month"]["tokens"], 70);
}

#[tokio::test]
async fn usg_04_denied_before_any_report_needs_a_role() {
    let h = spawn_api_key_harness().await;
    let id = seeded_org(&h, "org", Some("apikey_01"), ApiKeyOrgCapability::Denied).await;

    let body = usage(&h, id, "range=24h&step=hour").await.body;

    assert_eq!(body["source"], "estimated");
    assert_eq!(body["stale"], false);
    assert_eq!(body["fix"], json!({"code": "role_insufficient"}));
}

#[tokio::test]
async fn usg_06_custom_host_is_estimated_even_when_ok() {
    let h = spawn_api_key_harness().await;
    let created = h
        .create_api_key_upstream(
            "custom",
            ORG,
            json!({"base_url": "http://127.0.0.1:9/", "skip_preflight": true}),
        )
        .await;
    assert_eq!(created.status, StatusCode::CREATED, "{}", created.body);
    let id = created.id();
    h.seed_details(
        id,
        org_access(Some("apikey_01"), ApiKeyOrgCapability::Ok, NOW - 60),
    )
    .await;
    h.seed_usage_buckets(
        id,
        poll_success(vec![key_bucket(3_600, NOW - HOUR, "m", 5, 5)]),
    )
    .await;

    let body = usage(&h, id, "range=24h&step=hour").await.body;

    assert_eq!(body["source"], "estimated");
    assert_eq!(body["fix"], json!({"code": "non_anthropic_base_url"}));
}

#[tokio::test]
async fn usg_07_demoted_after_reports_serves_the_stale_snapshot() {
    let h = spawn_api_key_harness().await;
    let id = reported_org(
        &h,
        "org",
        Some("apikey_01"),
        vec![
            key_bucket(3_600, NOW - 3 * HOUR, "claude-sonnet-4-5", 30, 3),
            key_bucket(86_400, OCT_1 + DAY, "claude-sonnet-4-5", 30, 3),
        ],
    )
    .await;
    let access_revision = h.details(id).await.expect("details").access_revision;
    h.seed_usage_buckets(
        id,
        poll_failure(access_revision, Some(ApiKeyOrgCapability::Denied), 1),
    )
    .await;

    let body = usage(&h, id, "range=24h&step=hour").await.body;

    assert_eq!(body["source"], "real");
    assert_eq!(body["stale"], true);
    assert_eq!(body["fix"], json!({"code": "usage_poll_failed"}));
    // The last snapshot stays readable: the card dates it from the buckets.
    assert_eq!(series_input_tokens(&body), 30);
    assert_eq!(body["month"]["tokens"], 30);
}

// R2-2: a transient outage is stale without a fix code.
#[tokio::test]
async fn usg_07_transient_failures_are_stale_without_a_fix() {
    let h = spawn_api_key_harness().await;
    let id = reported_org(
        &h,
        "org",
        Some("apikey_01"),
        vec![key_bucket(3_600, NOW - HOUR, "claude-sonnet-4-5", 1, 1)],
    )
    .await;

    h.seed_usage_buckets(id, poll_failure(0, None, 1)).await;
    let one = usage(&h, id, "range=24h&step=hour").await.body;
    assert_eq!(
        (one["source"].clone(), one["stale"].clone()),
        (json!("real"), json!(false))
    );

    h.seed_usage_buckets(id, poll_failure(0, None, 2)).await;
    let two = usage(&h, id, "range=24h&step=hour").await.body;
    assert_eq!(two["source"], "real");
    assert_eq!(two["stale"], true);
    assert!(two["fix"].is_null());
}

#[tokio::test]
async fn usg_08_estimated_series_excludes_other_upstreams() {
    let h = spawn_api_key_harness().await;
    let target = h.seed_legacy_api_key_upstream("target", ORG).await;
    let other = h.seed_legacy_api_key_upstream("other", ORG_2).await;
    let oauth = create_oauth_upstream(&h, "oauth").await;
    append_and_roll_up(
        &h,
        &[
            usage_event(target.id, NOW - 600, "target-1", 11),
            usage_event(other.id, NOW - 600, "other-1", 222),
            usage_event(oauth, NOW - 600, "oauth-1", 3_333),
        ],
    )
    .await;

    let body = usage(&h, target.id, "range=24h&step=hour").await.body;

    assert_eq!(body["source"], "estimated");
    assert_eq!(series_input_tokens(&body), 11);
    assert_eq!(body["month"]["tokens"], 11);
}

#[tokio::test]
async fn usg_09_month_block_is_the_calendar_month_for_any_range() {
    let h = spawn_api_key_harness().await;
    let legacy = h.seed_legacy_api_key_upstream("legacy", ORG).await;

    for query in [
        "range=1h&step=minute",
        "range=24h&step=hour",
        "range=7d&step=hour",
    ] {
        let body = usage(&h, legacy.id, query).await.body;
        assert_eq!(body["month"]["start_unix_secs"], OCT_1, "{query}");
        assert_eq!(body["month"]["end_unix_secs"], NOV_1, "{query}");
        assert_eq!(body["month"]["tokens"], 0);
        assert_eq!(body["month"]["cost_usd_micros"], 0);
        assert!(body["month"]["billing_stop"].is_null());
    }
}

#[tokio::test]
async fn usg_10_month_boundaries_follow_utc() {
    let h = spawn_api_key_harness().await;
    let id = reported_org(
        &h,
        "org",
        Some("apikey_01"),
        vec![
            key_bucket(86_400, JAN_1_2027 - DAY, "claude-sonnet-4-5", 31, 1),
            key_bucket(86_400, JAN_1_2027, "claude-sonnet-4-5", 1, 1),
        ],
    )
    .await;

    h.clock.set_unix_secs(JAN_1_2027 - 1);
    let december = usage(&h, id, "range=24h&step=hour").await.body;
    assert_eq!(december["month"]["start_unix_secs"], DEC_1);
    assert_eq!(december["month"]["end_unix_secs"], JAN_1_2027);
    assert_eq!(december["month"]["tokens"], 31);

    h.clock.set_unix_secs(JAN_1_2027);
    let january = usage(&h, id, "range=24h&step=hour").await.body;
    assert_eq!(january["month"]["start_unix_secs"], JAN_1_2027);
    assert_eq!(january["month"]["end_unix_secs"], FEB_1_2027);
    assert_eq!(january["month"]["tokens"], 1);
}

#[tokio::test]
async fn usg_11_month_tokens_sum_the_current_key_only() {
    let h = spawn_api_key_harness().await;
    let id = reported_org(
        &h,
        "org",
        Some("apikey_01"),
        vec![
            key_bucket(86_400, OCT_1, "claude-sonnet-4-5", 100, 10),
            key_bucket(86_400, OCT_1 + 5 * DAY, "", 20, 2),
            // Last month.
            key_bucket(86_400, OCT_1 - DAY, "claude-sonnet-4-5", 7_000, 700),
            // The old key's rows are kept but never read (R2-1).
            bucket(
                ApiUsageAttribution::ApiKey,
                "apikey_old",
                86_400,
                OCT_1,
                "m",
                9_000,
                900,
            ),
            // Workspace rows of the same org.
            bucket(
                ApiUsageAttribution::Workspace,
                "wrkspc_P",
                86_400,
                OCT_1,
                "m",
                50,
                5,
            ),
        ],
    )
    .await;

    let month = usage(&h, id, "range=24h&step=hour").await.body["month"].clone();
    assert_eq!(month["start_unix_secs"], OCT_1);
    assert_eq!(month["tokens"], 120);
    assert_eq!(month["cost_usd_micros"], 12);

    // Unresolved key: the workspace's month.
    h.seed_details(id, org_access(None, ApiKeyOrgCapability::Ok, NOW - 30))
        .await;
    let workspace = usage(&h, id, "range=24h&step=hour").await.body;
    assert_eq!(workspace["attribution"], "workspace");
    assert_eq!(workspace["month"]["tokens"], 50);
}

#[tokio::test]
async fn usg_12_estimated_month_starts_at_the_retention_cut() {
    let h = spawn_api_key_harness().await;
    let legacy = h.seed_legacy_api_key_upstream("legacy", ORG).await;
    let day_20 = OCT_1 + 19 * DAY + 12 * HOUR;
    append_and_roll_up(
        &h,
        &[
            usage_event(legacy.id, OCT_1 + 9 * DAY, "old", 1_000),
            usage_event(legacy.id, OCT_1 + 14 * DAY + 600, "recent", 5),
        ],
    )
    .await;
    h.clock.set_unix_secs(day_20);
    let mut state = h.state.clone();
    let mut config = (*state.config).clone();
    config.request_event_retention_days = 7;
    state.config = Arc::new(config);
    let app = router(state);

    let (status, body) = send(
        &app,
        "GET",
        &format!("{UPSTREAMS}/{}/usage?range=24h&step=hour", legacy.id),
        None,
    )
    .await;

    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["source"], "estimated");
    assert_eq!(body["month"]["start_unix_secs"], day_20 - 7 * DAY);
    assert_eq!(body["month"]["end_unix_secs"], NOV_1);
    assert_eq!(body["month"]["tokens"], 5);
}

#[tokio::test]
async fn usg_14_invalid_parameters_are_flat_400s() {
    let h = spawn_api_key_harness().await;
    let legacy = h.seed_legacy_api_key_upstream("legacy", ORG).await;
    let path = format!("{UPSTREAMS}/{}/usage", legacy.id);

    for (query, code) in [
        ("", "invalid_range"),
        ("?range=30d", "invalid_range"),
        ("?range=-1", "invalid_range"),
        ("?range=24h&step=second", "invalid_step"),
        ("?range=7d&step=minute", "step_too_fine_for_range"),
    ] {
        let response = h
            .request("GET", &format!("{path}{query}"), None, None)
            .await;
        assert_eq!(response.status, StatusCode::BAD_REQUEST, "{query}");
        assert_eq!(response.body["error"], code, "{query}");
    }
    assert_eq!(h.outbound_count(), 0);
}

#[tokio::test]
async fn usg_16_usage_is_storage_only() {
    let h = spawn_api_key_harness().await;
    h.set_policy(ORG, org_policy(ORG));
    let reported = reported_org(
        &h,
        "reported",
        Some("apikey_01"),
        vec![key_bucket(3_600, NOW - HOUR, "m", 1, 1)],
    )
    .await;
    let legacy = h.seed_legacy_api_key_upstream("legacy", ORG_2).await;
    let denied = seeded_org(&h, "denied", None, ApiKeyOrgCapability::Denied).await;

    for id in [reported, legacy.id, denied] {
        for query in ["range=24h&step=hour", "range=15m&step=minute", "range=7d"] {
            usage(&h, id, query).await;
        }
        h.get_upstream(id).await;
    }
    h.request("GET", UPSTREAMS, None, None).await;

    assert_eq!(h.outbound_count(), 0);
}

// ---------------------------------------------------------------------------
// BIL: `month.billing_stop`

async fn billing_stop_after(events: &[(u64, u16, Option<&str>, Option<&str>)]) -> Value {
    let h = spawn_api_key_harness().await;
    let legacy = h.seed_legacy_api_key_upstream("billing", ORG).await;
    let events = events
        .iter()
        .enumerate()
        .map(|(index, (ts, status, error_type, message))| {
            outcome_event(
                legacy.id,
                *ts,
                &format!("req-{index}"),
                *status,
                *error_type,
                *message,
            )
        })
        .collect::<Vec<_>>();
    append(&h, &events).await;
    usage(&h, legacy.id, "range=24h&step=hour").await.body["month"]["billing_stop"].clone()
}

const CREDIT_BALANCE: &str = "Your credit balance is too low to access the Anthropic API. Please go to Plans & Billing to upgrade or purchase credits.";
const TIER_CAP: &str =
    "You have reached your API usage limits. You will regain access on 2026-11-01 at 00:00 UTC.";

#[tokio::test]
async fn bil_01_credit_balance_400_is_credits_exhausted() {
    let stop = billing_stop_after(&[(
        NOW - 120,
        400,
        Some("invalid_request_error"),
        Some(CREDIT_BALANCE),
    )])
    .await;
    assert_eq!(
        stop,
        json!({"kind": "credits_exhausted", "observed_at_unix_secs": NOW - 120})
    );
}

#[tokio::test]
async fn bil_02_tier_cap_429_is_tier_spend_cap() {
    let stop =
        billing_stop_after(&[(NOW - 60, 429, Some("rate_limit_error"), Some(TIER_CAP))]).await;
    assert_eq!(
        stop,
        json!({"kind": "tier_spend_cap", "observed_at_unix_secs": NOW - 60})
    );
}

#[tokio::test]
async fn bil_03_self_set_limits_are_spend_limit() {
    for message in [
        "You have reached your specified API usage limits. You will regain access on 2026-11-01 at 00:00 UTC.",
        "You have reached your specified workspace API usage limits. You will regain access on 2026-11-01 at 00:00 UTC.",
    ] {
        let stop =
            billing_stop_after(&[(NOW - 60, 400, Some("invalid_request_error"), Some(message))])
                .await;
        assert_eq!(stop["kind"], "spend_limit", "{message}");
    }
}

#[tokio::test]
async fn bil_04_402_is_billing_error() {
    let stop = billing_stop_after(&[(
        NOW - 60,
        402,
        Some("billing_error"),
        Some("Payment required"),
    )])
    .await;
    assert_eq!(stop["kind"], "billing_error");
}

#[tokio::test]
async fn bil_05_ordinary_failures_are_not_billing_stops() {
    for (status, error_type, message) in [
        (400, "invalid_request_error", "max_tokens: field required"),
        (
            429,
            "rate_limit_error",
            "This request would exceed the rate limit for your organization of 50 requests per minute.",
        ),
        (500, "api_error", "Internal server error"),
        (529, "overloaded_error", "Overloaded"),
        (401, "authentication_error", "invalid x-api-key"),
    ] {
        let stop = billing_stop_after(&[(NOW - 60, status, Some(error_type), Some(message))]).await;
        assert!(stop.is_null(), "{status} {message}");
    }
}

#[tokio::test]
async fn bil_06_a_later_success_clears_the_stop() {
    let stop = billing_stop_after(&[
        (
            NOW - 120,
            400,
            Some("invalid_request_error"),
            Some(CREDIT_BALANCE),
        ),
        (NOW - 60, 200, None, None),
    ])
    .await;
    assert!(stop.is_null(), "{stop}");
}

#[tokio::test]
async fn bil_07_last_months_stop_is_out_of_scope() {
    let stop = billing_stop_after(&[(
        OCT_1 - HOUR,
        400,
        Some("invalid_request_error"),
        Some(CREDIT_BALANCE),
    )])
    .await;
    assert!(stop.is_null(), "{stop}");
}

#[tokio::test]
async fn bil_08_stops_are_per_upstream() {
    let h = spawn_api_key_harness().await;
    let a = h.seed_legacy_api_key_upstream("a", ORG).await;
    let b = h.seed_legacy_api_key_upstream("b", ORG_2).await;
    append(
        &h,
        &[
            outcome_event(
                a.id,
                NOW - 60,
                "a-stop",
                429,
                Some("rate_limit_error"),
                Some(TIER_CAP),
            ),
            outcome_event(b.id, NOW - 120, "b-ok", 200, None, None),
        ],
    )
    .await;

    let on_a = usage(&h, a.id, "range=24h").await.body;
    let on_b = usage(&h, b.id, "range=24h").await.body;

    assert_eq!(on_a["month"]["billing_stop"]["kind"], "tier_spend_cap");
    assert!(on_b["month"]["billing_stop"].is_null());
}

#[tokio::test]
async fn bil_10_relayed_tier_cap_with_a_bounded_message_is_observable() {
    // The request log keeps a bounded copy of the provider message.
    let bounded = format!("{TIER_CAP} {}", "x".repeat(400));
    let bounded = &bounded[..480];
    let stop =
        billing_stop_after(&[(NOW - 30, 429, Some("rate_limit_error"), Some(bounded))]).await;
    assert_eq!(stop["kind"], "tier_spend_cap");
}

// ---------------------------------------------------------------------------
// CONC: Re-check racing a PUT

/// A custom endpoint whose first `count_tokens` call parks until released,
/// so a PUT can land while a Re-check probe is in flight. Requests without
/// `anthropic-workspace-id` get the identity-linked 400 (org-scoped key).
struct GatedEndpoint {
    arrived: Notify,
    release: Notify,
    count_tokens_calls: AtomicUsize,
}

fn identity_linked_400() -> Response {
    (
        StatusCode::BAD_REQUEST,
        axum::Json(json!({
            "type": "error",
            "error": {"type": "invalid_request_error", "message": IDENTITY_LINKED_MESSAGE},
        })),
    )
        .into_response()
}

async fn gated_models(headers: HeaderMap) -> Response {
    if !headers.contains_key("anthropic-workspace-id") {
        return identity_linked_400();
    }
    axum::Json(json!({
        "data": [{"type": "model", "id": "claude-gated-live", "display_name": "Gated", "created_at": "2025-01-01T00:00:00Z"}],
        "has_more": false,
        "first_id": "claude-gated-live",
        "last_id": "claude-gated-live",
    }))
    .into_response()
}

async fn gated_count_tokens(
    State(gate): State<Arc<GatedEndpoint>>,
    headers: HeaderMap,
) -> Response {
    if !headers.contains_key("anthropic-workspace-id") {
        return identity_linked_400();
    }
    if gate.count_tokens_calls.fetch_add(1, Ordering::SeqCst) == 0 {
        gate.arrived.notify_one();
        gate.release.notified().await;
    }
    axum::Json(json!({"input_tokens": 1})).into_response()
}

async fn spawn_gated_endpoint() -> (Arc<GatedEndpoint>, Url) {
    let gate = Arc::new(GatedEndpoint {
        arrived: Notify::new(),
        release: Notify::new(),
        count_tokens_calls: AtomicUsize::new(0),
    });
    let app = axum::Router::new()
        .route(MODELS, get(gated_models))
        .route(COUNT_TOKENS, post(gated_count_tokens))
        .with_state(gate.clone());
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind gated endpoint");
    let addr = listener.local_addr().expect("gated endpoint addr");
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    (
        gate,
        Url::parse(&format!("http://{addr}/")).expect("url parses"),
    )
}

/// Runs a Re-check and, once its probe is parked, `mutation`; then releases
/// the probe. Bounded so a regression fails instead of hanging.
async fn race_recheck<F>(
    h: &ApiKeyHarness,
    gate: &GatedEndpoint,
    id: Uuid,
    mutation: F,
) -> (HarnessResponse, HarnessResponse)
where
    F: Future<Output = HarnessResponse>,
{
    let raced = async {
        gate.arrived.notified().await;
        let response = mutation.await;
        gate.release.notify_one();
        response
    };
    tokio::time::timeout(Duration::from_secs(30), async {
        tokio::join!(recheck(h, id), raced)
    })
    .await
    .expect("re-check race finishes")
}

#[tokio::test]
async fn conc_02_recheck_racing_a_key_replace_is_discarded() {
    let h = spawn_api_key_harness().await;
    let (gate, endpoint) = spawn_gated_endpoint().await;
    let created = h
        .create_api_key_upstream(
            "replace-race",
            ORG,
            json!({"base_url": endpoint.as_str(), "skip_preflight": true, "workspace_id": "wrkspc_A"}),
        )
        .await;
    assert_eq!(created.status, StatusCode::CREATED, "{}", created.body);
    let id = created.id();
    let etag = h.get_upstream(id).await.etag();
    let before = h.record(id).await.revision;

    let (rechecked, replaced) = race_recheck(
        &h,
        &gate,
        id,
        h.put(
            id,
            &etag,
            json!({"api_key_value": ORG_2, "skip_preflight": true}),
        ),
    )
    .await;

    assert_eq!(replaced.status, StatusCode::OK, "{}", replaced.body);
    assert_eq!(rechecked.status, StatusCode::OK, "{}", rechecked.body);
    assert_eq!(rechecked.body["key_hint"], ORG_HINT);
    assert!(h.record(id).await.revision > before);
    // K1's result never lands over K2's state.
    if let Some(details) = h.details(id).await {
        assert_eq!(details.key_hint, ORG_2_HINT);
        assert!(!details.verified);
    }
    let access = &h.get_upstream(id).await.body["api_key_access"];
    assert_ne!(access["verified"], true, "{access}");
}

#[tokio::test]
async fn conc_03_recheck_racing_a_workspace_change_is_discarded() {
    let h = spawn_api_key_harness().await;
    let (gate, endpoint) = spawn_gated_endpoint().await;
    let created = h
        .create_api_key_upstream(
            "workspace-race",
            ORG,
            json!({"base_url": endpoint.as_str(), "skip_preflight": true, "workspace_id": "wrkspc_A"}),
        )
        .await;
    assert_eq!(created.status, StatusCode::CREATED, "{}", created.body);
    let id = created.id();
    let etag = h.get_upstream(id).await.etag();

    let (rechecked, moved) = race_recheck(
        &h,
        &gate,
        id,
        h.put(id, &etag, json!({"workspace_id": "wrkspc_B"})),
    )
    .await;

    assert_eq!(moved.status, StatusCode::OK, "{}", moved.body);
    assert_eq!(rechecked.status, StatusCode::OK, "{}", rechecked.body);
    assert_eq!(
        rechecked.body["classification"]["workspace"]["id"], "wrkspc_A",
        "the stale probe ran for the old workspace"
    );
    assert_eq!(h.record(id).await.workspace_id.as_deref(), Some("wrkspc_B"));
    let details = h.details(id).await.expect("details written by the PUT");
    assert_eq!(
        details.capabilities.workspace.id.as_deref(),
        Some("wrkspc_B")
    );
    let access = &h.get_upstream(id).await.body["api_key_access"];
    assert_eq!(access["workspace"]["id"], "wrkspc_B", "{access}");
    assert_eq!(access["workspace_list"], "not_applicable");
    assert_eq!(access["usage_report"], "not_applicable");
}
