//! Preflight probe runner against fake-anthropic (QA PRB, WS, ORG).
//!
//! Every test points both the upstream `base_url` and the injected org API
//! origin at loopback fixtures; nothing dials a non-loopback host.

use std::net::SocketAddr;
use std::time::Duration;

use axum::Router;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use cc_lb_control::anthropic_api_key::{
    AnthropicOrgApi, PreflightInput, PreflightMode, PreflightOutcome, PreflightVerdict,
    WARMUP_MODEL, WireInference, normalize_api_key, run_preflight,
};
use cc_lb_storage_api::{
    ApiKeyClass, ApiKeyInference, ApiKeyOrgCapability, ApiKeyResolution, ApiKeyType,
    ApiKeyWorkspaceMode, RemediationCode,
};
use fake_anthropic::{
    AppConfig, FakeApiKey, FakeWorkspace, IDENTITY_LINKED_MESSAGE, KeyPolicies, KeyPolicy,
    RecordedRequest, RequestRecorder, Script, app,
};
use serde_json::json;
use tokio::net::TcpListener;
use tokio::time::Instant;
use url::Url;

const NOW: u64 = 1_760_000_000;
const BND: &str = "sk-ant-api03-BOUNDKEYbound0000000000000000BBBB";
const ORG: &str = "sk-ant-api03-ORGSCOPEDorg00000000000000000OOOO";
const BAD: &str = "sk-ant-api03-REVOKEDkey000000000000000000DDDD";
const FRB: &str = "sk-ant-api03-FORBIDDENkey0000000000000000FFFF";
const LIM: &str = "sk-ant-api03-LIMITEDkey000000000000000000LLLL";
const PLAIN: &str = "sk-ant-api03-PLAINkey00000000000000000000PPPP";
const MODELS: &str = "/v1/models";
const COUNT_TOKENS: &str = "/v1/messages/count_tokens";
const ORG_PREFIX: &str = "/v1/organizations";
const WORKSPACES: &str = "/v1/organizations/workspaces";
const USAGE: &str = "/v1/organizations/usage_report/messages";
const API_KEYS: &str = "/v1/organizations/api_keys";

struct Fake {
    url: Url,
    policies: KeyPolicies,
    recorder: RequestRecorder,
}

impl Fake {
    async fn spawn() -> Self {
        let config = AppConfig::default();
        let policies = config.key_policies.clone();
        let recorder = config.recorder.clone();
        let url = serve(app(config)).await;
        Self {
            url,
            policies,
            recorder,
        }
    }

    /// Org API client whose origin is this fixture.
    fn org_api(&self) -> AnthropicOrgApi {
        AnthropicOrgApi::with_origin(self.url.clone())
    }

    fn requests(&self, path: &str) -> Vec<RecordedRequest> {
        self.recorder
            .snapshot()
            .into_iter()
            .filter(|request| request.path == path)
            .collect()
    }
}

async fn serve(router: Router) -> Url {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback fixture");
    let addr = listener.local_addr().expect("fixture addr");
    tokio::spawn(async move { axum::serve(listener, router).await });
    loopback_url(addr)
}

fn loopback_url(addr: SocketAddr) -> Url {
    Url::parse(&format!("http://{addr}/")).expect("loopback url parses")
}

async fn preflight(
    api: &AnthropicOrgApi,
    key: &str,
    base_url: &Url,
    workspace_id: Option<&str>,
) -> PreflightOutcome {
    let key = normalize_api_key(key).expect("test key normalizes");
    run_preflight(
        api,
        PreflightInput {
            key: &key,
            base_url,
            workspace_id,
            mode: PreflightMode::Candidate,
            now_unix_secs: NOW,
        },
    )
    .await
}

/// Anthropic's hint shape: 16 leading characters and the last four.
fn anthropic_hint(key: &str) -> String {
    format!(
        "{}...{}",
        key.get(..16).unwrap_or_default(),
        key.get(key.len() - 4..).unwrap_or_default()
    )
}

fn service_account(id: &str, key: &str) -> FakeApiKey {
    FakeApiKey {
        id: id.to_owned(),
        partial_key_hint: anthropic_hint(key),
        principal: Some(json!({"type": "service_account_actor", "service_account_id": "svac_01"})),
        scope: Some(json!({"type": "organization"})),
    }
}

/// An org-scoped identity key with workspaces P and S, full org access, and
/// one matching `api_keys` item.
fn org_policy() -> KeyPolicy {
    KeyPolicy {
        requires_workspace: true,
        workspaces: vec![
            FakeWorkspace::new("wrkspc_P", "Production"),
            FakeWorkspace::new("wrkspc_S", "Staging"),
        ],
        api_key_items: vec![service_account("apikey_01", ORG)],
        ..KeyPolicy::default()
    }
}

fn codes(outcome: &PreflightOutcome) -> Vec<RemediationCode> {
    outcome.wire.remediation.iter().map(|r| r.code).collect()
}

fn error(status: u16, kind: &str, message: &str) -> Script {
    Script::error(status, kind, message)
}

fn model_of(request: &RecordedRequest) -> Option<String> {
    request.body_json().and_then(|body| {
        body.get("model")
            .and_then(|model| model.as_str().map(ToOwned::to_owned))
    })
}

/// A result without a working check reports `required` with no id, both on
/// the wire and in the stored snapshot.
fn assert_unchecked_workspace(outcome: &PreflightOutcome) {
    for workspace in [
        &outcome.wire.classification.workspace,
        &outcome.access.capabilities.workspace,
    ] {
        assert_eq!(workspace.mode, ApiKeyWorkspaceMode::Required);
        assert_eq!(workspace.id, None);
    }
}

/// Plan §1.7 default path: a 2xx with no `anthropic-workspace-id` response
/// header and no identity-linked 400 is `unknown`/`ok`.
#[tokio::test]
async fn prb_no_workspace_header_classifies_unknown_ok() {
    let fake = Fake::spawn().await;
    let outcome = preflight(&fake.org_api(), PLAIN, &fake.url, None).await;

    assert_eq!(outcome.wire.verdict, PreflightVerdict::Ok);
    assert_eq!(outcome.wire.classification.key_class, ApiKeyClass::Unknown);
    assert!(!outcome.wire.classification.needs_workspace);
    assert_eq!(
        outcome.wire.classification.workspace.mode,
        ApiKeyWorkspaceMode::Bound
    );
    assert_eq!(outcome.wire.classification.workspace.id, None);
    assert_eq!(outcome.wire.capabilities.inference, WireInference::Ok);
    assert_eq!(
        outcome.wire.capabilities.workspace_list,
        ApiKeyOrgCapability::NotApplicable
    );
    assert_eq!(
        outcome.wire.capabilities.usage_report,
        ApiKeyOrgCapability::NotApplicable
    );
    assert_eq!(
        outcome.wire.capabilities.api_key_resolution,
        ApiKeyResolution::NotApplicable
    );
    assert_eq!(
        outcome.access.usage_report_state,
        Some(ApiKeyOrgCapability::NotApplicable)
    );
    assert!(codes(&outcome).is_empty());
    assert_eq!(fake.recorder.count(ORG_PREFIX), 0);

    // The same key behind a custom host only adds non_anthropic_base_url.
    let org_origin = Fake::spawn().await;
    let custom = preflight(&org_origin.org_api(), PLAIN, &fake.url, None).await;
    assert_eq!(custom.wire.verdict, PreflightVerdict::Ok);
    assert_eq!(codes(&custom), vec![RemediationCode::NonAnthropicBaseUrl]);
}

/// PRB-01: a bound key is ok, workspace-bound, and never reaches the org API.
#[tokio::test]
async fn prb_01_bound_key_ok() {
    let fake = Fake::spawn().await;
    fake.policies.set(
        BND,
        KeyPolicy {
            bound_workspace: Some("wrkspc_B".to_owned()),
            ..KeyPolicy::default()
        },
    );
    let outcome = preflight(&fake.org_api(), BND, &fake.url, None).await;

    assert_eq!(outcome.wire.verdict, PreflightVerdict::Ok);
    assert_eq!(
        outcome.wire.classification.key_class,
        ApiKeyClass::WorkspaceBound
    );
    assert_eq!(
        outcome.wire.classification.workspace.mode,
        ApiKeyWorkspaceMode::Bound
    );
    assert_eq!(
        outcome.wire.classification.workspace.id.as_deref(),
        Some("wrkspc_B")
    );
    assert_eq!(outcome.wire.capabilities.inference, WireInference::Ok);
    assert_eq!(
        outcome.wire.workspaces.state,
        ApiKeyOrgCapability::NotApplicable
    );
    assert_eq!(
        outcome.wire.capabilities.usage_report,
        ApiKeyOrgCapability::NotApplicable
    );
    assert_eq!(
        codes(&outcome),
        vec![RemediationCode::KeyWorkspaceScopedUsageUnavailable]
    );
    assert!(outcome.access.verified);
    assert_eq!(fake.recorder.count(ORG_PREFIX), 0);
}

/// PRB-02: count_tokens carries the live model id from P1a.
#[tokio::test]
async fn prb_02_live_model_used() {
    let fake = Fake::spawn().await;
    let outcome = preflight(&fake.org_api(), PLAIN, &fake.url, None).await;
    assert_eq!(outcome.wire.verdict, PreflightVerdict::Ok);

    let counted = fake.requests(COUNT_TOKENS);
    assert_eq!(counted.len(), 1);
    assert_eq!(model_of(&counted[0]).as_deref(), Some("claude-fable-5"));
    assert_ne!(model_of(&counted[0]).as_deref(), Some(WARMUP_MODEL));
}

/// PRB-03: a failing or non-list `/v1/models` falls back to WARMUP_MODEL.
#[tokio::test]
async fn prb_03_model_fallback() {
    for models in [
        error(500, "api_error", "internal"),
        Script::status(200, json!({"unexpected": true})),
    ] {
        let fake = Fake::spawn().await;
        fake.policies.set(
            PLAIN,
            KeyPolicy {
                models,
                ..KeyPolicy::default()
            },
        );
        let outcome = preflight(&fake.org_api(), PLAIN, &fake.url, None).await;

        assert_eq!(outcome.wire.verdict, PreflightVerdict::Ok);
        let counted = fake.requests(COUNT_TOKENS);
        assert_eq!(counted.len(), 1);
        assert_eq!(model_of(&counted[0]).as_deref(), Some(WARMUP_MODEL));
    }
}

/// PRB-04: 401 is rejected/key_invalid with inference denied and no org probe.
#[tokio::test]
async fn prb_04_unauthorized_rejected() {
    let fake = Fake::spawn().await;
    let unauthorized = error(401, "authentication_error", "invalid x-api-key");
    fake.policies.set(
        BAD,
        KeyPolicy {
            models: unauthorized.clone(),
            count_tokens: unauthorized,
            ..KeyPolicy::default()
        },
    );
    let outcome = preflight(&fake.org_api(), BAD, &fake.url, None).await;

    assert_eq!(outcome.wire.verdict, PreflightVerdict::Rejected);
    assert_eq!(codes(&outcome), vec![RemediationCode::KeyInvalid]);
    assert_eq!(outcome.wire.capabilities.inference, WireInference::Denied);
    assert_eq!(
        outcome.access.capabilities.inference,
        ApiKeyInference::Denied
    );
    assert!(!outcome.access.verified);
    assert_eq!(fake.recorder.count(ORG_PREFIX), 0);
    assert_unchecked_workspace(&outcome);
}

/// PRB-05: count_tokens 403 is rejected/key_forbidden with inference denied.
#[tokio::test]
async fn prb_05_forbidden_rejected() {
    let fake = Fake::spawn().await;
    fake.policies.set(
        FRB,
        KeyPolicy {
            count_tokens: error(403, "permission_error", "not allowed"),
            ..KeyPolicy::default()
        },
    );
    let outcome = preflight(&fake.org_api(), FRB, &fake.url, None).await;

    assert_eq!(outcome.wire.verdict, PreflightVerdict::Rejected);
    assert_eq!(outcome.wire.capabilities.inference, WireInference::Denied);
    assert_eq!(codes(&outcome), vec![RemediationCode::KeyForbidden]);
    assert!(outcome.transport.is_none());
    assert_unchecked_workspace(&outcome);
}

/// PRB-06: count_tokens 429 is ok with `ok_limited` on the wire.
#[tokio::test]
async fn prb_06_rate_limited_is_ok_limited() {
    let fake = Fake::spawn().await;
    fake.policies.set(
        LIM,
        KeyPolicy {
            count_tokens: error(429, "rate_limit_error", "slow down"),
            ..KeyPolicy::default()
        },
    );
    let outcome = preflight(&fake.org_api(), LIM, &fake.url, None).await;

    assert_eq!(outcome.wire.verdict, PreflightVerdict::Ok);
    assert_eq!(
        outcome.wire.capabilities.inference,
        WireInference::OkLimited
    );
    assert_eq!(outcome.access.capabilities.inference, ApiKeyInference::Ok);
    let wire = serde_json::to_value(&outcome.wire).expect("wire serializes");
    assert_eq!(wire["capabilities"]["inference"], "ok_limited");
}

/// PRB-07: 500 and 529 from count_tokens are unreachable, not timeouts.
#[tokio::test]
async fn prb_07_server_errors_unreachable() {
    for status in [500, 529] {
        let fake = Fake::spawn().await;
        fake.policies.set(
            PLAIN,
            KeyPolicy {
                count_tokens: error(status, "api_error", "upstream exploded"),
                ..KeyPolicy::default()
            },
        );
        let outcome = preflight(&fake.org_api(), PLAIN, &fake.url, None).await;

        assert_eq!(
            outcome.wire.verdict,
            PreflightVerdict::Unreachable,
            "{status}"
        );
        assert_eq!(codes(&outcome), vec![RemediationCode::ProbeUnreachable]);
        assert_eq!(
            outcome.transport.map(|transport| transport.timed_out),
            Some(false)
        );
        assert_eq!(outcome.access.checked_at_unix_secs, None);
        assert_eq!(outcome.access.usage_report_state, None);
        assert_unchecked_workspace(&outcome);
        let last_error = outcome.access.last_error.unwrap_or_default();
        assert!(
            last_error.contains(&format!("HTTP {status}")),
            "{last_error}"
        );
    }
}

/// PRB-08: a count_tokens stalled past the per-probe budget is unreachable
/// after about 10 s on the paused clock, never hanging.
#[tokio::test(start_paused = true)]
async fn prb_08_timeout_unreachable() {
    let fake = Fake::spawn().await;
    fake.policies.set(
        PLAIN,
        KeyPolicy {
            count_tokens: Script::delay(60, Script::Default),
            ..KeyPolicy::default()
        },
    );
    let started = Instant::now();
    let outcome = preflight(&fake.org_api(), PLAIN, &fake.url, None).await;
    let elapsed = started.elapsed();

    assert_eq!(outcome.wire.verdict, PreflightVerdict::Unreachable);
    assert_eq!(codes(&outcome), vec![RemediationCode::ProbeUnreachable]);
    assert_eq!(
        outcome.transport.map(|transport| transport.timed_out),
        Some(true)
    );
    // count_tokens alone takes the 10 s budget; P1a may take its own.
    assert!(elapsed >= Duration::from_secs(10), "{elapsed:?}");
    assert!(elapsed <= Duration::from_secs(20), "{elapsed:?}");
}

/// PRB-09: a closed port is unreachable; the detail never contains the key
/// and the copy names the endpoint, not Anthropic.
#[tokio::test]
async fn prb_09_connect_failure_unreachable() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let closed = loopback_url(listener.local_addr().expect("addr"));
    drop(listener);
    let org_origin = Fake::spawn().await;

    let outcome = preflight(&org_origin.org_api(), PLAIN, &closed, None).await;

    assert_eq!(outcome.wire.verdict, PreflightVerdict::Unreachable);
    assert_eq!(codes(&outcome), vec![RemediationCode::ProbeUnreachable]);
    let last_error = outcome.access.last_error.clone().unwrap_or_default();
    assert!(
        last_error.contains("Couldn't reach the endpoint"),
        "{last_error}"
    );
    assert!(!last_error.contains("PLAINkey"), "{last_error}");
    let wire = serde_json::to_string(&outcome.wire).expect("wire serializes");
    assert!(!wire.contains("PLAINkey"), "{wire}");
    assert_eq!(org_origin.recorder.count("/"), 0);
}

/// PRB-11: a non-identity 400 gets exactly one retry with WARMUP_MODEL, then
/// unreachable with a scrubbed, bounded detail.
#[tokio::test]
async fn prb_11_non_identity_400_retries_once() {
    let fake = Fake::spawn().await;
    let message = format!("model: not found (key {PLAIN}) {}", "x".repeat(400));
    fake.policies.set(
        PLAIN,
        KeyPolicy {
            count_tokens: error(400, "invalid_request_error", &message),
            ..KeyPolicy::default()
        },
    );
    let outcome = preflight(&fake.org_api(), PLAIN, &fake.url, None).await;

    let counted = fake.requests(COUNT_TOKENS);
    assert_eq!(counted.len(), 2);
    assert_eq!(model_of(&counted[0]).as_deref(), Some("claude-fable-5"));
    assert_eq!(model_of(&counted[1]).as_deref(), Some(WARMUP_MODEL));
    assert_eq!(outcome.wire.verdict, PreflightVerdict::Unreachable);
    assert_eq!(
        outcome.wire.capabilities.inference,
        WireInference::Unverified
    );
    assert_eq!(codes(&outcome), vec![RemediationCode::ProbeUnreachable]);
    let last_error = outcome.access.last_error.unwrap_or_default();
    assert!(last_error.chars().count() <= 200, "{last_error}");
    assert!(!last_error.contains("PLAINkey"), "{last_error}");
    assert!(last_error.contains("model: not found"), "{last_error}");
}

/// WS-01: identity-linked 400 without a workspace is needs_workspace.
#[tokio::test]
async fn ws_01_detects_org_scoped_key() {
    let fake = Fake::spawn().await;
    fake.policies.set(ORG, org_policy());
    let outcome = preflight(&fake.org_api(), ORG, &fake.url, None).await;

    assert_eq!(outcome.wire.verdict, PreflightVerdict::NeedsWorkspace);
    assert_eq!(
        outcome.wire.classification.key_class,
        ApiKeyClass::OrgScoped
    );
    assert!(outcome.wire.classification.needs_workspace);
    assert_eq!(
        outcome.wire.classification.workspace.mode,
        ApiKeyWorkspaceMode::Required
    );
    assert_eq!(codes(&outcome), vec![RemediationCode::WorkspaceRequired]);
    assert!(!codes(&outcome).contains(&RemediationCode::KeyInvalid));
    // The workspace list is offered for the selection step.
    assert_eq!(outcome.wire.workspaces.state, ApiKeyOrgCapability::Ok);
    assert_eq!(outcome.wire.workspaces.items.len(), 2);
}

/// WS-02: models 200 without the header but count_tokens identity-linked
/// is still needs_workspace.
#[tokio::test]
async fn ws_02_detects_on_count_tokens_only() {
    let fake = Fake::spawn().await;
    fake.policies.set(
        ORG,
        KeyPolicy {
            count_tokens: error(400, "invalid_request_error", IDENTITY_LINKED_MESSAGE),
            ..KeyPolicy::default()
        },
    );
    let outcome = preflight(&fake.org_api(), ORG, &fake.url, None).await;

    assert_eq!(outcome.wire.verdict, PreflightVerdict::NeedsWorkspace);
    assert_eq!(
        outcome.wire.classification.key_class,
        ApiKeyClass::OrgScoped
    );
    assert!(codes(&outcome).contains(&RemediationCode::WorkspaceRequired));
}

/// WS-04: a supplied workspace gets exactly one header retry and a selected,
/// named workspace.
#[tokio::test]
async fn ws_04_selected_workspace_ok() {
    let fake = Fake::spawn().await;
    fake.policies.set(ORG, org_policy());
    let outcome = preflight(&fake.org_api(), ORG, &fake.url, Some("wrkspc_P")).await;

    assert_eq!(outcome.wire.verdict, PreflightVerdict::Ok);
    let workspace = &outcome.wire.classification.workspace;
    assert_eq!(workspace.mode, ApiKeyWorkspaceMode::Selected);
    assert_eq!(workspace.id.as_deref(), Some("wrkspc_P"));
    assert_eq!(workspace.name.as_deref(), Some("Production"));
    assert!(codes(&outcome).is_empty(), "{:?}", codes(&outcome));

    let models = fake.requests(MODELS);
    assert_eq!(models.len(), 2);
    assert_eq!(models[0].header("anthropic-workspace-id"), None);
    assert_eq!(models[1].header("anthropic-workspace-id"), Some("wrkspc_P"));
    let counted = fake.requests(COUNT_TOKENS);
    assert_eq!(counted.len(), 1);
    assert_eq!(
        counted[0].header("anthropic-workspace-id"),
        Some("wrkspc_P")
    );
}

/// WS-05: an unknown workspace (404) is needs_workspace/workspace_not_found.
#[tokio::test]
async fn ws_05_unknown_workspace_not_found() {
    let fake = Fake::spawn().await;
    fake.policies.set(ORG, org_policy());
    let outcome = preflight(&fake.org_api(), ORG, &fake.url, Some("wrkspc_unknown")).await;

    assert_eq!(outcome.wire.verdict, PreflightVerdict::NeedsWorkspace);
    assert_eq!(
        codes(&outcome).first(),
        Some(&RemediationCode::WorkspaceNotFound)
    );
    assert!(!codes(&outcome).contains(&RemediationCode::WorkspaceRequired));
}

/// WS-09: model discovery under a workspace: the live model comes from the
/// header retry of `/v1/models` and count_tokens sends header and model.
#[tokio::test]
async fn ws_09_model_discovery_under_workspace() {
    let fake = Fake::spawn().await;
    fake.policies.set(
        ORG,
        KeyPolicy {
            models: Script::status(
                200,
                json!({"data": [{"id": "claude-workspace-live"}], "has_more": false}),
            ),
            ..org_policy()
        },
    );
    let outcome = preflight(&fake.org_api(), ORG, &fake.url, Some("wrkspc_S")).await;

    assert_eq!(outcome.wire.verdict, PreflightVerdict::Ok);
    let models = fake.requests(MODELS);
    assert_eq!(models.len(), 2);
    let counted = fake.requests(COUNT_TOKENS);
    assert_eq!(counted.len(), 1);
    assert_eq!(
        counted[0].header("anthropic-workspace-id"),
        Some("wrkspc_S")
    );
    assert_eq!(
        model_of(&counted[0]).as_deref(),
        Some("claude-workspace-live")
    );
}

/// ORG-01: P2 ok lists `{id, name}` items.
#[tokio::test]
async fn org_01_workspaces_ok() {
    let fake = Fake::spawn().await;
    fake.policies.set(ORG, org_policy());
    let outcome = preflight(&fake.org_api(), ORG, &fake.url, None).await;

    assert_eq!(outcome.wire.workspaces.state, ApiKeyOrgCapability::Ok);
    let items: Vec<(&str, &str)> = outcome
        .wire
        .workspaces
        .items
        .iter()
        .map(|item| (item.id.as_str(), item.name.as_str()))
        .collect();
    assert_eq!(
        items,
        vec![("wrkspc_P", "Production"), ("wrkspc_S", "Staging")]
    );
    assert_eq!(fake.requests(WORKSPACES).len(), 1);
}

/// ORG-02: at most three workspace pages are fetched.
#[tokio::test]
async fn org_02_workspace_page_cap() {
    let fake = Fake::spawn().await;
    let page = |n: u32| {
        let id = format!("wrkspc_page{n}");
        Script::status(
            200,
            json!({"data": [{"id": id, "name": format!("Page {n}")}], "has_more": true, "last_id": id}),
        )
    };
    fake.policies.set(
        ORG,
        KeyPolicy {
            workspaces_list: Script::sequence((1..=5).map(page)),
            ..org_policy()
        },
    );
    let outcome = preflight(&fake.org_api(), ORG, &fake.url, None).await;

    let pages = fake.requests(WORKSPACES);
    assert_eq!(pages.len(), 3);
    assert_eq!(pages[0].query.as_deref(), Some("limit=100"));
    assert_eq!(
        pages[1].query.as_deref(),
        Some("limit=100&after_id=wrkspc_page1")
    );
    assert_eq!(outcome.wire.workspaces.state, ApiKeyOrgCapability::Ok);
    let ids: Vec<&str> = outcome
        .wire
        .workspaces
        .items
        .iter()
        .map(|item| item.id.as_str())
        .collect();
    assert_eq!(ids, vec!["wrkspc_page1", "wrkspc_page2", "wrkspc_page3"]);
}

/// ORG-03: P2 403 is denied/workspace_list_denied; the verdict stays
/// needs_workspace (manual id path).
#[tokio::test]
async fn org_03_workspaces_denied() {
    let fake = Fake::spawn().await;
    fake.policies.set(
        ORG,
        KeyPolicy {
            workspaces_list: error(403, "permission_error", "role"),
            ..org_policy()
        },
    );
    let outcome = preflight(&fake.org_api(), ORG, &fake.url, None).await;

    assert_eq!(outcome.wire.verdict, PreflightVerdict::NeedsWorkspace);
    assert_eq!(outcome.wire.workspaces.state, ApiKeyOrgCapability::Denied);
    assert!(outcome.wire.workspaces.items.is_empty());
    assert!(codes(&outcome).contains(&RemediationCode::WorkspaceListDenied));
}

/// ORG-04: P2 500 is error, not denied.
#[tokio::test]
async fn org_04_workspaces_error() {
    let fake = Fake::spawn().await;
    fake.policies.set(
        ORG,
        KeyPolicy {
            workspaces_list: error(500, "api_error", "boom"),
            ..org_policy()
        },
    );
    let outcome = preflight(&fake.org_api(), ORG, &fake.url, None).await;

    assert_eq!(outcome.wire.workspaces.state, ApiKeyOrgCapability::Error);
    assert_eq!(
        outcome.wire.capabilities.workspace_list,
        ApiKeyOrgCapability::Error
    );
    assert!(!codes(&outcome).contains(&RemediationCode::WorkspaceListDenied));
}

/// ORG-05: P3 states: 200 ok, 401/403 denied (role_insufficient), 500 error.
#[tokio::test]
async fn org_05_usage_report_states() {
    let cases = [
        (Script::Default, ApiKeyOrgCapability::Ok, false),
        (
            error(401, "authentication_error", "no"),
            ApiKeyOrgCapability::Denied,
            true,
        ),
        (
            error(403, "permission_error", "role"),
            ApiKeyOrgCapability::Denied,
            true,
        ),
        (
            error(500, "api_error", "boom"),
            ApiKeyOrgCapability::Error,
            false,
        ),
    ];
    for (usage_report, expected, role_fix) in cases {
        let fake = Fake::spawn().await;
        fake.policies.set(
            ORG,
            KeyPolicy {
                usage_report,
                ..org_policy()
            },
        );
        let outcome = preflight(&fake.org_api(), ORG, &fake.url, Some("wrkspc_P")).await;

        assert_eq!(outcome.wire.capabilities.usage_report, expected);
        assert_eq!(outcome.access.usage_report_state, Some(expected));
        assert_eq!(
            codes(&outcome).contains(&RemediationCode::RoleInsufficient),
            role_fix,
            "{expected:?}"
        );
        let usage = fake.requests(USAGE);
        assert_eq!(usage.len(), 1);
        let query = usage[0].query.clone().unwrap_or_default();
        assert!(query.contains("limit=1"), "{query}");
        assert!(query.contains("bucket_width=1h"), "{query}");
    }
}

/// ORG-06: exactly one `partial_key_hint` match resolves the key id and type.
#[tokio::test]
async fn org_06_api_key_resolved() {
    let fake = Fake::spawn().await;
    let mut policy = org_policy();
    policy.api_key_items.push(FakeApiKey {
        id: "apikey_other".to_owned(),
        partial_key_hint: "sk-ant-api03-ZZZ...ZZZZ".to_owned(),
        principal: Some(json!({"type": "user_actor"})),
        scope: Some(json!({"type": "organization"})),
    });
    fake.policies.set(ORG, policy);
    let outcome = preflight(&fake.org_api(), ORG, &fake.url, Some("wrkspc_P")).await;

    assert_eq!(
        outcome.wire.capabilities.api_key_resolution,
        ApiKeyResolution::Resolved
    );
    assert_eq!(outcome.access.api_key_id.as_deref(), Some("apikey_01"));
    assert_eq!(
        outcome.wire.classification.key_type,
        Some(ApiKeyType::ServiceAccount)
    );
    assert_eq!(
        outcome.access.capabilities.key_type,
        Some(ApiKeyType::ServiceAccount)
    );
    assert!(!codes(&outcome).contains(&RemediationCode::KeyIdUnresolved));
}

/// ORG-07: two matches, zero matches, or a denied list never resolve and
/// never pick one.
#[tokio::test]
async fn org_07_api_key_ambiguous_or_missing() {
    let two = vec![
        service_account("apikey_a", ORG),
        service_account("apikey_b", ORG),
    ];
    let none = vec![service_account("apikey_x", PLAIN)];
    let cases = [
        (two, Script::Default),
        (none, Script::Default),
        (Vec::new(), error(403, "permission_error", "role")),
    ];
    for (api_key_items, api_keys) in cases {
        let fake = Fake::spawn().await;
        fake.policies.set(
            ORG,
            KeyPolicy {
                api_key_items,
                api_keys,
                ..org_policy()
            },
        );
        let outcome = preflight(&fake.org_api(), ORG, &fake.url, Some("wrkspc_P")).await;

        assert_eq!(
            outcome.wire.capabilities.api_key_resolution,
            ApiKeyResolution::Unresolved
        );
        assert_eq!(outcome.access.api_key_id, None);
        assert_eq!(outcome.wire.classification.key_type, None);
        assert!(codes(&outcome).contains(&RemediationCode::KeyIdUnresolved));
        assert_eq!(fake.requests(API_KEYS).len(), 1);
    }
}

/// One scripted `api_keys` page holding `items`.
fn api_key_page(items: serde_json::Value, has_more: bool, last_id: Option<&str>) -> Script {
    let mut body = json!({"data": items, "has_more": has_more});
    if let Some(last_id) = last_id {
        body["last_id"] = json!(last_id);
    }
    Script::status(200, body)
}

/// An `api_keys` item whose hint is `key`'s Anthropic hint.
fn api_key_item(id: &str, key: &str) -> serde_json::Value {
    json!({
        "id": id,
        "partial_key_hint": anthropic_hint(key),
        "principal": {"type": "service_account_actor", "service_account_id": "svac_01"},
    })
}

/// ORG-07b: a listing that stops before the provider's last page never
/// resolves, even when it holds exactly one apparent match: the page cap
/// reached with `has_more`, or `has_more` without a cursor. The request
/// bound holds.
#[tokio::test]
async fn org_07b_incomplete_api_key_listing_stays_unresolved() {
    let other = |n: u32| api_key_item(&format!("apikey_other{n}"), PLAIN);
    let capped = Script::sequence([
        api_key_page(
            json!([api_key_item("apikey_01", ORG)]),
            true,
            Some("apikey_01"),
        ),
        api_key_page(json!([other(2)]), true, Some("apikey_other2")),
        api_key_page(json!([other(3)]), true, Some("apikey_other3")),
        api_key_page(json!([api_key_item("apikey_real", ORG)]), false, None),
    ]);
    let cursorless = api_key_page(json!([api_key_item("apikey_01", ORG)]), true, None);
    for (api_keys, expected_requests) in [(capped, 3), (cursorless, 1)] {
        let fake = Fake::spawn().await;
        fake.policies.set(
            ORG,
            KeyPolicy {
                api_keys,
                ..org_policy()
            },
        );
        let outcome = preflight(&fake.org_api(), ORG, &fake.url, Some("wrkspc_P")).await;

        assert_eq!(
            outcome.wire.capabilities.api_key_resolution,
            ApiKeyResolution::Unresolved
        );
        assert_eq!(outcome.access.api_key_id, None);
        assert_eq!(outcome.wire.classification.key_type, None);
        assert!(codes(&outcome).contains(&RemediationCode::KeyIdUnresolved));
        assert_eq!(fake.requests(API_KEYS).len(), expected_requests);
    }
}

/// ORG-07c: a multi-page listing that reaches `has_more: false` within the
/// cap is complete, so its sole match resolves.
#[tokio::test]
async fn org_07c_complete_multi_page_api_key_listing_resolves() {
    let fake = Fake::spawn().await;
    fake.policies.set(
        ORG,
        KeyPolicy {
            api_keys: Script::sequence([
                api_key_page(
                    json!([api_key_item("apikey_other", PLAIN)]),
                    true,
                    Some("apikey_other"),
                ),
                api_key_page(json!([api_key_item("apikey_01", ORG)]), false, None),
            ]),
            ..org_policy()
        },
    );
    let outcome = preflight(&fake.org_api(), ORG, &fake.url, Some("wrkspc_P")).await;

    assert_eq!(
        outcome.wire.capabilities.api_key_resolution,
        ApiKeyResolution::Resolved
    );
    assert_eq!(outcome.access.api_key_id.as_deref(), Some("apikey_01"));
    let pages = fake.requests(API_KEYS);
    assert_eq!(pages.len(), 2);
    assert_eq!(
        pages[1].query.as_deref(),
        Some("limit=100&after_id=apikey_other")
    );
}

/// ORG-09: a bound key on the Anthropic host never calls the org API.
#[tokio::test]
async fn org_09_bound_key_never_calls_org() {
    let fake = Fake::spawn().await;
    fake.policies.set(
        BND,
        KeyPolicy {
            bound_workspace: Some("wrkspc_B".to_owned()),
            ..org_policy()
        },
    );
    let outcome = preflight(&fake.org_api(), BND, &fake.url, None).await;

    assert_eq!(outcome.wire.verdict, PreflightVerdict::Ok);
    assert_eq!(fake.recorder.count(ORG_PREFIX), 0);
    let capabilities = &outcome.wire.capabilities;
    assert_eq!(
        capabilities.workspace_list,
        ApiKeyOrgCapability::NotApplicable
    );
    assert_eq!(
        capabilities.usage_report,
        ApiKeyOrgCapability::NotApplicable
    );
    assert_eq!(
        capabilities.api_key_resolution,
        ApiKeyResolution::NotApplicable
    );
}

/// ORG-10: a custom host gets inference probes with `x-api-key` and zero
/// org probes anywhere.
#[tokio::test]
async fn org_10_custom_host_never_gets_org_probes() {
    let custom = Fake::spawn().await;
    let org_origin = Fake::spawn().await;
    custom.policies.set(ORG, org_policy());
    org_origin.policies.set(ORG, org_policy());
    let outcome = preflight(&org_origin.org_api(), ORG, &custom.url, None).await;

    assert!(
        custom
            .requests(MODELS)
            .iter()
            .all(|r| r.header("x-api-key") == Some(ORG))
    );
    assert!(!custom.requests(COUNT_TOKENS).is_empty());
    assert_eq!(custom.recorder.count(ORG_PREFIX), 0);
    assert_eq!(org_origin.recorder.count("/"), 0);
    let capabilities = &outcome.wire.capabilities;
    assert_eq!(
        outcome.wire.workspaces.state,
        ApiKeyOrgCapability::NotApplicable
    );
    assert_eq!(
        capabilities.workspace_list,
        ApiKeyOrgCapability::NotApplicable
    );
    assert_eq!(
        capabilities.usage_report,
        ApiKeyOrgCapability::NotApplicable
    );
    assert_eq!(
        capabilities.api_key_resolution,
        ApiKeyResolution::NotApplicable
    );
    assert!(codes(&outcome).contains(&RemediationCode::NonAnthropicBaseUrl));
    assert!(!codes(&outcome).contains(&RemediationCode::WorkspaceListDenied));
}

/// ORG-13: redirects are never followed, on inference or org probes.
#[tokio::test]
async fn org_13_no_redirects() {
    let other = Fake::spawn().await;
    let location = other.url.join("v1/models").expect("join").to_string();
    let redirect = Script::Redirect {
        status: 302,
        location: location.clone(),
    };

    let first = Fake::spawn().await;
    first.policies.set(
        PLAIN,
        KeyPolicy {
            models: redirect.clone(),
            count_tokens: Script::Redirect {
                status: 307,
                location,
            },
            ..KeyPolicy::default()
        },
    );
    let outcome = preflight(&first.org_api(), PLAIN, &first.url, None).await;
    assert_eq!(outcome.wire.verdict, PreflightVerdict::Unreachable);
    assert_eq!(codes(&outcome), vec![RemediationCode::ProbeUnreachable]);
    assert_eq!(first.requests(MODELS).len(), 1);
    assert_eq!(first.requests(COUNT_TOKENS).len(), 1);

    let org = Fake::spawn().await;
    org.policies.set(
        ORG,
        KeyPolicy {
            workspaces_list: redirect,
            ..org_policy()
        },
    );
    let outcome = preflight(&org.org_api(), ORG, &org.url, None).await;
    assert_eq!(outcome.wire.workspaces.state, ApiKeyOrgCapability::Error);
    assert_eq!(org.requests(WORKSPACES).len(), 1);

    assert_eq!(other.recorder.count("/"), 0);
}

/// ORG-14: internal targets get only inference probes; the outcome carries
/// the verdict, a code, and a scrubbed short message, never the raw body or
/// headers.
#[tokio::test]
async fn org_14_internal_targets() {
    const MARKER: &str = "ADMIN-INTERNAL-BODY-MARKER";
    let admin_like = serve(Router::new().fallback(|| async {
        (
            StatusCode::UNAUTHORIZED,
            [("x-internal", MARKER)],
            format!("{{\"error\":\"unauthorized\",\"detail\":\"{MARKER}\"}}"),
        )
            .into_response()
    }))
    .await;
    let failing = serve(Router::new().fallback(|| async {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            [("x-internal", MARKER)],
            format!("<html>{MARKER}</html>"),
        )
            .into_response()
    }))
    .await;
    let org_origin = Fake::spawn().await;
    let api = org_origin.org_api();

    let rejected = preflight(&api, PLAIN, &admin_like, None).await;
    assert_eq!(rejected.wire.verdict, PreflightVerdict::Rejected);
    assert_eq!(codes(&rejected), vec![RemediationCode::KeyInvalid]);

    let with_path = admin_like.join("admin/").expect("join");
    let rejected_path = preflight(&api, PLAIN, &with_path, None).await;
    assert_eq!(rejected_path.wire.verdict, PreflightVerdict::Rejected);

    let unreachable = preflight(&api, PLAIN, &failing, None).await;
    assert_eq!(unreachable.wire.verdict, PreflightVerdict::Unreachable);
    let last_error = unreachable.access.last_error.clone().unwrap_or_default();
    assert!(last_error.contains("HTTP 500"), "{last_error}");
    assert!(last_error.chars().count() <= 200);

    for outcome in [&rejected, &rejected_path, &unreachable] {
        let rendered = format!(
            "{}{:?}",
            serde_json::to_string(&outcome.wire).expect("wire serializes"),
            outcome.access
        );
        assert!(!rendered.contains(MARKER), "{rendered}");
        assert!(!rendered.contains("PLAINkey"), "{rendered}");
    }
    assert_eq!(org_origin.recorder.count("/"), 0);
}

/// ORG-16: slow, huge, and stalling hosts: every probe aborts within 10 s,
/// each preflight within 30 s, five run concurrently, and the body read is
/// capped.
#[tokio::test(start_paused = true)]
async fn org_16_slow_or_stalling_host_is_bounded() {
    let fake = Fake::spawn().await;
    fake.policies.set(
        PLAIN,
        KeyPolicy {
            models: Script::delay(60, Script::Default),
            count_tokens: Script::Stall,
            ..KeyPolicy::default()
        },
    );
    let api = fake.org_api();
    let base_url = fake.url.clone();
    let mut runs = tokio::task::JoinSet::new();
    for _ in 0..5 {
        let api = api.clone();
        let base_url = base_url.clone();
        runs.spawn(async move {
            let started = Instant::now();
            let outcome = preflight(&api, PLAIN, &base_url, None).await;
            (outcome, started.elapsed())
        });
    }
    let mut finished = 0;
    while let Some(joined) = runs.join_next().await {
        let (outcome, elapsed) = joined.expect("preflight task completes");
        finished += 1;
        assert_eq!(outcome.wire.verdict, PreflightVerdict::Unreachable);
        assert_eq!(
            outcome.transport.map(|transport| transport.timed_out),
            Some(true)
        );
        // Two probes, each cut at the 10 s per-probe budget.
        assert_eq!(elapsed, Duration::from_secs(20), "{elapsed:?}");
    }
    assert_eq!(finished, 5);
    assert_eq!(api.limits().max_body_bytes, 1024 * 1024);
}

/// ORG-16 (huge): a 50 MB body is cut at the 1 MiB cap without waiting for
/// the rest, and is unreachable rather than a timeout.
#[tokio::test]
async fn org_16_huge_body_is_capped() {
    let fake = Fake::spawn().await;
    fake.policies.set(
        PLAIN,
        KeyPolicy {
            count_tokens: Script::Huge {
                bytes: 50 * 1024 * 1024,
            },
            ..KeyPolicy::default()
        },
    );
    let outcome = preflight(&fake.org_api(), PLAIN, &fake.url, None).await;

    assert_eq!(outcome.wire.verdict, PreflightVerdict::Unreachable);
    assert_eq!(
        outcome.transport.map(|transport| transport.timed_out),
        Some(false)
    );
    let last_error = outcome.access.last_error.unwrap_or_default();
    assert!(last_error.contains("too large"), "{last_error}");
}

/// The wire response and access snapshot of `outcome`, rendered for leak
/// checks.
fn rendered(outcome: &PreflightOutcome) -> String {
    let wire = serde_json::to_string(&outcome.wire).expect("wire serializes");
    format!("{wire}\n{:?}", outcome.access)
}

/// SEC-PROVIDER-METADATA-02: a resolved-workspace header that echoes the key,
/// whole or as a fragment inside a valid-looking id, is treated as no header.
#[tokio::test]
async fn sec_workspace_header_echoing_key_is_dropped() {
    let fragment_id = "wrkspc_BOUNDKEYbound0000";
    for echoed in [BND.to_owned(), fragment_id.to_owned()] {
        let fake = Fake::spawn().await;
        fake.policies.set(
            BND,
            KeyPolicy {
                bound_workspace: Some(echoed.clone()),
                ..KeyPolicy::default()
            },
        );
        let outcome = preflight(&fake.org_api(), BND, &fake.url, None).await;

        assert!(
            fake.requests(MODELS)
                .iter()
                .any(|r| r.header("anthropic-workspace-id").is_none()),
            "{echoed}"
        );
        assert_eq!(outcome.wire.verdict, PreflightVerdict::Ok, "{echoed}");
        assert_eq!(
            outcome.wire.classification.key_class,
            ApiKeyClass::Unknown,
            "{echoed}"
        );
        assert_eq!(outcome.wire.classification.workspace.id, None, "{echoed}");
        let rendered = rendered(&outcome);
        assert!(!rendered.contains(BND), "{rendered}");
        assert!(!rendered.contains("BOUNDKEYbound"), "{rendered}");
    }
}

/// SEC-PROVIDER-METADATA-02: a resolved-workspace header that is not a valid
/// workspace id is treated as no header.
#[tokio::test]
async fn sec_invalid_workspace_header_is_dropped() {
    let fake = Fake::spawn().await;
    fake.policies.set(
        BND,
        KeyPolicy {
            bound_workspace: Some("not a workspace".to_owned()),
            ..KeyPolicy::default()
        },
    );
    let outcome = preflight(&fake.org_api(), BND, &fake.url, None).await;

    assert_eq!(outcome.wire.verdict, PreflightVerdict::Ok);
    assert_eq!(outcome.wire.classification.workspace.id, None);
    assert!(!rendered(&outcome).contains("not a workspace"));
}

/// SEC-PROVIDER-METADATA-02: listed workspaces whose id echoes the key are
/// dropped, and key fragments in names are masked.
#[tokio::test]
async fn sec_listed_workspaces_never_echo_key() {
    let fake = Fake::spawn().await;
    fake.policies.set(
        ORG,
        KeyPolicy {
            workspaces: vec![
                FakeWorkspace::new("wrkspc_P", format!("Prod {ORG}")),
                FakeWorkspace::new("wrkspc_ORGSCOPEDorg0000", "Echo"),
                FakeWorkspace::new("wrkspc_S", "Staging ORGSCOPEDorg000"),
            ],
            ..org_policy()
        },
    );
    let outcome = preflight(&fake.org_api(), ORG, &fake.url, None).await;

    assert_eq!(outcome.wire.verdict, PreflightVerdict::NeedsWorkspace);
    assert_eq!(outcome.wire.workspaces.state, ApiKeyOrgCapability::Ok);
    let ids: Vec<&str> = outcome
        .wire
        .workspaces
        .items
        .iter()
        .map(|item| item.id.as_str())
        .collect();
    assert_eq!(ids, vec!["wrkspc_P", "wrkspc_S"]);
    let rendered = rendered(&outcome);
    assert!(!rendered.contains(ORG), "{rendered}");
    assert!(!rendered.contains("ORGSCOPEDorg"), "{rendered}");
    assert!(rendered.contains("Prod "), "{rendered}");
}

/// SEC-PROVIDER-METADATA-02: a `api_keys` item whose id is not a valid
/// provider API-key id, or echoes the key, is dropped before resolution, so
/// nothing derived from it is persisted as `api_key_id`.
#[tokio::test]
async fn sec_listed_api_key_ids_never_echo_key() {
    for id in [
        "apikey_ORGSCOPEDorg0000",
        "not a key id",
        "sk-ant-api03-ORGSCOPED",
        &format!("apikey_{}", "a".repeat(121)),
    ] {
        let fake = Fake::spawn().await;
        let mut policy = org_policy();
        policy.api_key_items = vec![service_account(id, ORG)];
        fake.policies.set(ORG, policy);
        let outcome = preflight(&fake.org_api(), ORG, &fake.url, Some("wrkspc_P")).await;

        assert_eq!(
            outcome.wire.capabilities.api_key_resolution,
            ApiKeyResolution::Unresolved,
            "{id}"
        );
        assert_eq!(outcome.access.api_key_id, None, "{id}");
        let rendered = rendered(&outcome);
        assert!(!rendered.contains(ORG), "{rendered}");
        assert!(!rendered.contains("ORGSCOPEDorg"), "{rendered}");
        assert!(!rendered.contains(id), "{rendered}");
    }
}
