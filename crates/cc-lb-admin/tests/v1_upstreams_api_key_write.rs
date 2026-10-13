//! API-key upstream write paths: candidate preflight, create, PUT (workspace,
//! host, key replace), audit, and the remediation envelope.
//!
//! The org API origin and every upstream `base_url` point at a loopback
//! fake-anthropic; "custom host" cases use `localhost` on the same port, which
//! fails the org-origin rule while staying on loopback.

use axum::http::StatusCode;
use cc_lb_storage_api::{
    ApiKeyClass, ApiKeyInference, ApiKeyOrgCapability, ApiKeyWorkspaceMode, RemediationCode,
    UpstreamCreate, UpstreamKind, UpstreamStore,
};
use fake_anthropic::{FakeApiKey, FakeWorkspace, KeyPolicy, Script};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::api_key_support::{
    ApiKeyHarness, HARNESS_NOW_UNIX_SECS, HarnessResponse, PREFLIGHT, UPSTREAMS,
    spawn_api_key_harness,
};

const ADM: &str = "sk-ant-admin01-ADMINKEYadmin00000000000000AAAA";
const ENT: &str = "sk-ant-api01-ENTERPRISEkey000000000000000EEEE";
const OAT: &str = "sk-ant-oat01-OAUTHTOKENoauth00000000000000TTTT";
const BND: &str = "sk-ant-api03-BOUNDKEYbound0000000000000000BBBB";
const ORG: &str = "sk-ant-api03-ORGSCOPEDorg00000000000000000OOOO";
const BAD: &str = "sk-ant-api03-REVOKEDkey000000000000000000DDDD";
const FRB: &str = "sk-ant-api03-FORBIDDENkey0000000000000000FFFF";
const DWN: &str = "sk-ant-api03-DOWNSTREAMerr0000000000000000WWWW";
const PLAIN: &str = "sk-ant-api03-PLAINkey00000000000000000000PPPP";
const CANARY: &str = "sk-ant-api03-CANARYc4n4ryQ7xZp2Lm9Vw3Rt8Ks5Hj1Nb6AAAA";
/// A second canary that Anthropic rejects with 401.
const CANARY_401: &str = "sk-ant-api03-CANARYr3v0kdG4hT8yU2iO6pE1aS9dF5gBBBB";
const CANARY_HINT: &str = "sk-ant-api03-...AAAA";

const COUNT_TOKENS: &str = "/v1/messages/count_tokens";
const ORG_PREFIX: &str = "/v1/organizations";
const WORKSPACE_HEADER: &str = "anthropic-workspace-id";

/// Anthropic's `partial_key_hint`: 16 leading characters and the last four.
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

/// Org-scoped identity key: workspaces P, S, T, full org access, and one
/// matching `api_keys` item.
fn org_policy(key: &str) -> KeyPolicy {
    KeyPolicy {
        requires_workspace: true,
        workspaces: vec![
            FakeWorkspace::new("wrkspc_P", "Production"),
            FakeWorkspace::new("wrkspc_S", "Staging"),
            FakeWorkspace::new("wrkspc_T", "Testing"),
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

fn status_policy(status: u16, error_type: &str, message: &str) -> KeyPolicy {
    KeyPolicy {
        models: Script::error(status, error_type, message),
        count_tokens: Script::error(status, error_type, message),
        ..KeyPolicy::default()
    }
}

async fn harness() -> ApiKeyHarness {
    let harness = spawn_api_key_harness().await;
    harness.set_policy(BND, bound_policy());
    harness.set_policy(ORG, org_policy(ORG));
    harness.set_policy(CANARY, org_policy(CANARY));
    harness.set_policy(
        BAD,
        status_policy(401, "authentication_error", "invalid x-api-key"),
    );
    harness.set_policy(
        FRB,
        KeyPolicy {
            count_tokens: Script::error(403, "permission_error", "not allowed"),
            ..KeyPolicy::default()
        },
    );
    harness.set_policy(DWN, status_policy(500, "api_error", "overloaded"));
    harness
}

/// `http://localhost:<fake port>/`: same fixture, but not the org origin.
fn custom_host(harness: &ApiKeyHarness) -> String {
    let port = harness.fake_url.port().unwrap_or_default();
    format!("http://localhost:{port}/")
}

/// A loopback port nobody listens on.
async fn closed_port_url() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind probe port");
    let addr = listener.local_addr().expect("probe addr");
    drop(listener);
    format!("http://{addr}/")
}

#[track_caller]
fn assert_remediation(response: &HarnessResponse, status: StatusCode, code: &str) {
    assert_eq!(response.status, status, "{}", response.body);
    assert_eq!(response.body["error"]["code"], code, "{}", response.body);
    assert!(
        response.body["error"]["message"]
            .as_str()
            .is_some_and(|message| !message.is_empty()),
        "{}",
        response.body
    );
}

#[track_caller]
fn assert_flat(response: &HarnessResponse, status: StatusCode, error: &str) {
    assert_eq!(response.status, status, "{}", response.body);
    assert_eq!(response.body["error"], error, "{}", response.body);
}

fn codes(response: &HarnessResponse) -> Vec<String> {
    response.body["remediation"]
        .as_array()
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item["code"].as_str().map(ToOwned::to_owned))
                .collect()
        })
        .unwrap_or_default()
}

/// Neither the full key nor any 12-byte window of its secret body (after
/// the public `sk-ant-<family>-` prefix) may appear in `text`.
fn assert_no_key_material(text: &str, key: &str) {
    assert!(!text.contains(key), "key leaked into {text}");
    let body = key
        .strip_prefix("sk-ant-")
        .and_then(|rest| rest.split_once('-'))
        .map_or(key, |(_, body)| body);
    for start in 0..=body.len().saturating_sub(12) {
        let window = &body[start..start + 12];
        assert!(
            !text.contains(window),
            "key substring {window:?} leaked into {text}"
        );
    }
}

async fn create_ok(harness: &ApiKeyHarness, name: &str, key: &str, extra: Value) -> Uuid {
    let created = harness.create_api_key_upstream(name, key, extra).await;
    assert_eq!(created.status, StatusCode::CREATED, "{}", created.body);
    created.id()
}

async fn etag(harness: &ApiKeyHarness, id: Uuid) -> String {
    harness.get_upstream(id).await.etag()
}

// ---------------------------------------------------------------- GATE

async fn assert_gated_preflight(key: &str, key_class: &str, code: &str) {
    let harness = harness().await;
    let response = harness.preflight(key, json!({})).await;
    assert_eq!(response.status, StatusCode::OK);
    assert_eq!(response.body["verdict"], "rejected");
    assert_eq!(response.body["classification"]["key_class"], key_class);
    assert_eq!(response.body["capabilities"]["inference"], "unverified");
    assert_eq!(codes(&response), vec![code.to_owned()]);
    assert_eq!(harness.outbound_count(), 0);
}

#[tokio::test]
async fn gate_01_admin_key_rejected_without_outbound() {
    assert_gated_preflight(ADM, "admin", "key_admin").await;
}

#[tokio::test]
async fn gate_02_enterprise_key_rejected_without_outbound() {
    assert_gated_preflight(ENT, "enterprise", "key_enterprise_scoped").await;
}

#[tokio::test]
async fn gate_03_oauth_token_rejected_without_outbound() {
    assert_gated_preflight(OAT, "oauth_token", "key_oat").await;
}

#[tokio::test]
async fn gate_05_skip_cannot_bypass_gate() {
    let harness = harness().await;
    for (key, code) in [
        (ADM, "key_admin"),
        (ENT, "key_enterprise_scoped"),
        (OAT, "key_oat"),
    ] {
        let response = harness
            .create_api_key_upstream("gated", key, json!({"skip_preflight": true}))
            .await;
        assert_remediation(&response, StatusCode::BAD_REQUEST, code);
    }
    assert_eq!(harness.upstream_count().await, 0);
    assert_eq!(harness.outbound_count(), 0);
}

#[tokio::test]
async fn gate_06_gate_on_replace() {
    let harness = harness().await;
    let id = create_ok(&harness, "replace-gate", PLAIN, json!({})).await;
    let before = harness.record(id).await;
    harness.recorder.clear();

    let response = harness
        .put(
            id,
            &etag(&harness, id).await,
            json!({"api_key_value": ADM, "skip_preflight": true}),
        )
        .await;

    assert_remediation(&response, StatusCode::BAD_REQUEST, "key_admin");
    let after = harness.record(id).await;
    assert_eq!(after.api_key_ciphertext, before.api_key_ciphertext);
    assert_eq!(after.revision, before.revision);
    assert_eq!(harness.outbound_count(), 0);
}

#[tokio::test]
async fn gate_07_gate_on_custom_host() {
    let harness = harness().await;
    for (key, code) in [(ADM, "key_admin"), (OAT, "key_oat")] {
        let preflight = harness
            .preflight(key, json!({"base_url": "https://proxy.example.com/"}))
            .await;
        assert_eq!(preflight.status, StatusCode::OK);
        assert_eq!(preflight.body["verdict"], "rejected");
        assert_eq!(codes(&preflight), vec![code.to_owned()]);

        let created = harness
            .create_api_key_upstream(
                "custom-gate",
                key,
                json!({"base_url": "https://proxy.example.com/"}),
            )
            .await;
        assert_remediation(&created, StatusCode::BAD_REQUEST, code);
    }
    assert_eq!(harness.upstream_count().await, 0);
    assert_eq!(harness.outbound_count(), 0);
}

#[tokio::test]
async fn gate_08_whitespace_and_control_characters() {
    let harness = harness().await;
    let padded = format!("  {PLAIN}\n");
    let id = create_ok(&harness, "trimmed", &padded, json!({})).await;
    let record = harness.record(id).await;
    let stored = harness
        .state
        .aead
        .decrypt(
            record.api_key_ciphertext.as_deref().unwrap_or_default(),
            id.as_bytes(),
        )
        .expect("stored key decrypts");
    assert_eq!(stored, PLAIN.as_bytes());

    let outbound = harness.outbound_count();
    for raw in [
        "sk-ant-api03-inner space0000000000000000XXXX".to_owned(),
        "sk-ant-api03-crlf\r\n0000000000000000000XXXX".to_owned(),
        "sk-ant-api03-nul\u{0}000000000000000000000XXXX".to_owned(),
        "sk-ant-api03-nonascii\u{e4}0000000000000000XXXX".to_owned(),
        "sk-ant-api03-zerowidth\u{200b}000000000000000XXXX".to_owned(),
        "Bearer sk-ant-oat01-x".to_owned(),
    ] {
        for response in [
            harness.preflight(&raw, json!({})).await,
            harness
                .create_api_key_upstream("bad-chars", &raw, json!({}))
                .await,
        ] {
            assert_flat(&response, StatusCode::BAD_REQUEST, "invalid_input");
            assert_eq!(response.body["field"], "api_key_value");
        }
    }
    assert_eq!(harness.upstream_count().await, 1);
    assert_eq!(harness.outbound_count(), outbound);
}

#[tokio::test]
async fn gate_09_key_size() {
    let harness = harness().await;
    let empty = harness.preflight("", json!({})).await;
    assert_flat(&empty, StatusCode::BAD_REQUEST, "missing_api_key");

    let one_byte = harness.preflight("x", json!({})).await;
    assert_eq!(one_byte.status, StatusCode::OK);

    for len in [513, 4 * 1024] {
        let response = harness.preflight(&"x".repeat(len), json!({})).await;
        assert_flat(&response, StatusCode::BAD_REQUEST, "invalid_input");
        assert_eq!(response.body["field"], "api_key_value");
    }
    harness.recorder.clear();
    let huge = json!({"api_key_value": "x".repeat(3 * 1024 * 1024)}).to_string();
    let response = harness
        .raw_request("POST", PREFLIGHT, Some(huge), None, Some("test-token"))
        .await;
    assert_eq!(response.status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(harness.outbound_count(), 0);
}

// ---------------------------------------------------------------- WS / ORG-15

#[tokio::test]
async fn ws_06_malformed_workspace_id_is_never_sent() {
    let harness = harness().await;
    let long = "a".repeat(10 * 1024);
    let malformed = [
        "abc",
        "ws_1",
        " wrkspc_a\r\nX-Evil: 1",
        "wrkspc_\u{e4}",
        long.as_str(),
        "../",
        "",
    ];
    for workspace in malformed.iter().filter(|id| !id.is_empty()) {
        let preflight = harness
            .preflight(ORG, json!({"workspace_id": workspace}))
            .await;
        assert_eq!(preflight.status, StatusCode::OK);
        assert_eq!(preflight.body["verdict"], "needs_workspace");
        assert!(codes(&preflight).contains(&"workspace_id_malformed".to_owned()));

        let created = harness
            .create_api_key_upstream("malformed", ORG, json!({"workspace_id": workspace}))
            .await;
        assert_remediation(&created, StatusCode::BAD_REQUEST, "workspace_id_malformed");
    }
    let id = create_ok(&harness, "ws06", ORG, json!({"workspace_id": "wrkspc_P"})).await;
    let outbound = harness.outbound_count();
    let response = harness
        .put(
            id,
            &etag(&harness, id).await,
            json!({"workspace_id": "abc"}),
        )
        .await;
    assert_remediation(&response, StatusCode::BAD_REQUEST, "workspace_id_malformed");
    assert_eq!(harness.outbound_count(), outbound);
    assert_eq!(harness.upstream_count().await, 1);

    let sent: Vec<String> = harness
        .recorder
        .snapshot()
        .iter()
        .filter_map(|request| request.header(WORKSPACE_HEADER).map(ToOwned::to_owned))
        .collect();
    assert!(
        sent.iter().all(|id| id == "wrkspc_P"),
        "only the valid workspace is ever sent: {sent:?}"
    );
}

#[tokio::test]
async fn ws_08_bound_key_with_supplied_workspace() {
    let harness = harness().await;
    let preflight = harness
        .preflight(BND, json!({"workspace_id": "wrkspc_P"}))
        .await;
    assert_eq!(preflight.body["verdict"], "ok");
    assert_eq!(
        preflight.body["classification"]["workspace"],
        json!({"mode": "bound", "id": "wrkspc_B"})
    );

    let created = harness
        .create_api_key_upstream("bound-ws", BND, json!({"workspace_id": "wrkspc_P"}))
        .await;
    assert_flat(&created, StatusCode::BAD_REQUEST, "invalid_input");
    assert_eq!(created.body["field"], "workspace_id");
    assert_eq!(harness.upstream_count().await, 0);
}

#[tokio::test]
async fn org_15_base_url_scheme_validation() {
    let harness = harness().await;
    let oauth = harness
        .request(
            "POST",
            UPSTREAMS,
            Some(json!({"name": "oauth-ok", "kind": "anthropic_oauth"})),
            None,
        )
        .await;
    assert_eq!(oauth.status, StatusCode::CREATED);
    let oauth_id = oauth.id();
    let api_key_id = create_ok(&harness, "api-ok", PLAIN, json!({})).await;
    let outbound = harness.outbound_count();

    for bad in [
        "file:///etc/passwd",
        "unix:/var/run/x.sock",
        "ftp://h/",
        "data:,x",
        "javascript:alert(1)",
        "https://u:p@h/",
    ] {
        let api_key = harness
            .create_api_key_upstream("bad-url", PLAIN, json!({"base_url": bad}))
            .await;
        let oauth_create = harness
            .request(
                "POST",
                UPSTREAMS,
                Some(json!({"name": "bad-url-oauth", "kind": "anthropic_oauth", "base_url": bad})),
                None,
            )
            .await;
        let api_key_put = harness
            .put(
                api_key_id,
                &etag(&harness, api_key_id).await,
                json!({"base_url": bad}),
            )
            .await;
        let oauth_put = harness
            .put(
                oauth_id,
                &etag(&harness, oauth_id).await,
                json!({"base_url": bad}),
            )
            .await;
        for response in [api_key, oauth_create, api_key_put, oauth_put] {
            assert_flat(&response, StatusCode::BAD_REQUEST, "invalid_input");
            assert_eq!(response.body["field"], "base_url", "{bad}");
        }
    }
    assert_eq!(harness.upstream_count().await, 2);
    assert_eq!(harness.outbound_count(), outbound);
}

// ---------------------------------------------------------------- PF

#[tokio::test]
async fn pf_02_key_handling_responses_are_never_cached() {
    let harness = harness().await;
    for response in [
        harness.preflight(PLAIN, json!({})).await,
        harness.preflight(BAD, json!({})).await,
        harness.preflight("", json!({})).await,
        harness
            .raw_request(
                "POST",
                PREFLIGHT,
                Some("not json".to_owned()),
                None,
                Some("test-token"),
            )
            .await,
    ] {
        assert_eq!(
            response.header("cache-control"),
            Some("no-store"),
            "{}",
            response.status
        );
    }

    // Create and update answers for an API-key upstream follow a key check
    // and are never cached either; OAuth upstream writes are unchanged.
    let created = harness
        .create_api_key_upstream("never-cached", PLAIN, json!({}))
        .await;
    assert_eq!(created.status, StatusCode::CREATED, "{}", created.body);
    let id = created.id();
    let put = harness
        .put(
            id,
            &etag(&harness, id).await,
            json!({"api_key_value": PLAIN}),
        )
        .await;
    assert_eq!(put.status, StatusCode::OK, "{}", put.body);
    let patch = harness
        .request(
            "PATCH",
            &format!("{UPSTREAMS}/{id}"),
            Some(json!({"enabled": true})),
            Some(&etag(&harness, id).await),
        )
        .await;
    assert_eq!(patch.status, StatusCode::OK, "{}", patch.body);
    for response in [&created, &put, &patch] {
        assert_eq!(response.header("cache-control"), Some("no-store"));
    }
    let oauth = harness
        .request(
            "POST",
            UPSTREAMS,
            Some(json!({"name": "oauth-cached", "kind": "anthropic_oauth"})),
            None,
        )
        .await;
    assert_eq!(oauth.status, StatusCode::CREATED);
    assert_eq!(oauth.header("cache-control"), None);
}

#[tokio::test]
async fn pf_04_preflight_persists_nothing_but_audit() {
    let harness = harness().await;
    for key in [PLAIN, ORG, BND, BAD] {
        let response = harness.preflight(key, json!({})).await;
        assert_eq!(response.status, StatusCode::OK);
    }
    assert_eq!(harness.upstream_count().await, 0);
    let rows = harness.audit_rows().await;
    assert_eq!(rows.len(), 4);
    assert!(rows.iter().all(|row| {
        row.admin_action
            .as_deref()
            .is_some_and(|action| action.starts_with("upstream_preflight("))
    }));
}

#[tokio::test]
async fn pf_05_env_source_is_ignored() {
    let harness = harness().await;
    let id = create_ok(&harness, "env", PLAIN, json!({})).await;
    let outbound = harness.outbound_count();
    let env_only = json!({"api_key_env": "ANTHROPIC_API_KEY"});

    let preflight = harness
        .request("POST", PREFLIGHT, Some(env_only.clone()), None)
        .await;
    assert_flat(&preflight, StatusCode::BAD_REQUEST, "missing_api_key");
    let created = harness
        .request(
            "POST",
            UPSTREAMS,
            Some(json!({"name": "env-only", "kind": "anthropic_api_key", "api_key_env": "ANTHROPIC_API_KEY"})),
            None,
        )
        .await;
    assert_flat(&created, StatusCode::BAD_REQUEST, "missing_api_key");
    let before = harness.record(id).await;
    let put = harness.put(id, &etag(&harness, id).await, env_only).await;
    assert_eq!(put.status, StatusCode::OK);
    assert_eq!(
        harness.record(id).await.api_key_ciphertext,
        before.api_key_ciphertext
    );
    assert_eq!(harness.outbound_count(), outbound);
}

// ---------------------------------------------------------------- CRT

#[tokio::test]
async fn crt_01_server_reprobes_and_rejects() {
    let harness = harness().await;
    let response = harness.create_api_key_upstream("bad", BAD, json!({})).await;
    assert_remediation(&response, StatusCode::BAD_REQUEST, "key_invalid");
    assert_eq!(harness.upstream_count().await, 0);
    assert!(harness.outbound_count() > 0, "create probed the key itself");
}

#[tokio::test]
async fn crt_02_bound_key_happy_path() {
    let harness = harness().await;
    let created = harness
        .create_api_key_upstream("bound", BND, json!({}))
        .await;
    assert_eq!(created.status, StatusCode::CREATED);
    let access = &created.body["api_key_access"];
    assert_eq!(access["verified"], true);
    assert_eq!(access["key_class"], "workspace_bound");
    assert_eq!(access["inference"], "ok");
    assert_eq!(
        access["workspace"],
        json!({"mode": "bound", "id": "wrkspc_B"})
    );
    assert_eq!(access["workspace_list"], "not_applicable");
    assert_eq!(access["usage_report"], "not_applicable");
    assert_eq!(access["checked_at_unix_secs"], HARNESS_NOW_UNIX_SECS);
    assert_eq!(harness.record(created.id()).await.workspace_id, None);
}

#[tokio::test]
async fn crt_03_org_key_with_workspace() {
    let harness = harness().await;
    let created = harness
        .create_api_key_upstream("org", ORG, json!({"workspace_id": "wrkspc_P"}))
        .await;
    assert_eq!(created.status, StatusCode::CREATED, "{}", created.body);
    let access = &created.body["api_key_access"];
    assert_eq!(access["key_class"], "org_scoped");
    assert_eq!(
        access["workspace"],
        json!({"mode": "selected", "id": "wrkspc_P", "name": "Production"})
    );
    assert_eq!(access["usage_report"], "ok");
    let id = created.id();
    assert_eq!(
        harness.record(id).await.workspace_id.as_deref(),
        Some("wrkspc_P")
    );
    let details = harness.details(id).await.expect("details row");
    assert_eq!(details.api_key_id.as_deref(), Some("apikey_01"));
    assert_eq!(details.usage_report_state, Some(ApiKeyOrgCapability::Ok));
}

#[tokio::test]
async fn crt_04_org_key_without_workspace() {
    let harness = harness().await;
    let response = harness.create_api_key_upstream("org", ORG, json!({})).await;
    assert_remediation(&response, StatusCode::BAD_REQUEST, "workspace_required");
    assert_eq!(harness.upstream_count().await, 0);
}

#[tokio::test]
async fn crt_05_workspace_not_found_or_malformed() {
    let harness = harness().await;
    let unknown = harness
        .create_api_key_upstream("org", ORG, json!({"workspace_id": "wrkspc_unknown"}))
        .await;
    assert_remediation(&unknown, StatusCode::BAD_REQUEST, "workspace_not_found");
    let malformed = harness
        .create_api_key_upstream("org", ORG, json!({"workspace_id": "abc"}))
        .await;
    assert_remediation(
        &malformed,
        StatusCode::BAD_REQUEST,
        "workspace_id_malformed",
    );
    assert_eq!(harness.upstream_count().await, 0);
}

#[tokio::test]
async fn crt_06_forbidden_key_save_anyway() {
    let harness = harness().await;
    let first = harness.create_api_key_upstream("frb", FRB, json!({})).await;
    assert_remediation(&first, StatusCode::BAD_REQUEST, "key_forbidden");
    assert_eq!(harness.upstream_count().await, 0);

    let second = harness
        .create_api_key_upstream("frb", FRB, json!({"skip_preflight": true}))
        .await;
    assert_eq!(second.status, StatusCode::CREATED);
    assert_eq!(second.body["api_key_access"]["verified"], false);
    assert_eq!(second.body["api_key_access"]["inference"], "unverified");
}

#[tokio::test]
async fn crt_07_unreachable_then_save_without_checking() {
    let harness = harness().await;
    let down = closed_port_url().await;
    let refused = harness
        .create_api_key_upstream("down", PLAIN, json!({"base_url": down}))
        .await;
    assert_remediation(&refused, StatusCode::BAD_GATEWAY, "probe_unreachable");
    let server_error = harness
        .create_api_key_upstream("down", DWN, json!({}))
        .await;
    assert_remediation(&server_error, StatusCode::BAD_GATEWAY, "probe_unreachable");
    assert_eq!(harness.upstream_count().await, 0);

    let saved = harness
        .create_api_key_upstream(
            "down",
            PLAIN,
            json!({"base_url": down, "skip_preflight": true, "workspace_id": "wrkspc_P"}),
        )
        .await;
    assert_eq!(saved.status, StatusCode::CREATED);
    assert_eq!(saved.body["api_key_access"]["verified"], false);
    assert_eq!(
        harness.record(saved.id()).await.workspace_id.as_deref(),
        Some("wrkspc_P")
    );
}

#[tokio::test]
async fn crt_08_skip_preflight_semantics() {
    let harness = harness().await;
    for (name, extra) in [
        (
            "skip-ws",
            json!({"skip_preflight": true, "workspace_id": "wrkspc_P"}),
        ),
        ("skip-no-ws", json!({"skip_preflight": true})),
    ] {
        let created = harness.create_api_key_upstream(name, ORG, extra).await;
        assert_eq!(created.status, StatusCode::CREATED);
        let details = harness.details(created.id()).await.expect("details row");
        assert!(!details.verified);
        assert_eq!(details.key_class, ApiKeyClass::Unknown);
        assert_eq!(details.capabilities.inference, ApiKeyInference::Unverified);
        assert_eq!(details.checked_at_unix_secs, None);
        assert_eq!(
            details.remediation,
            vec![RemediationCode::UsageNeverChecked]
        );
    }
    assert_eq!(harness.outbound_count(), 0);
}

/// SEC-KEY-HINT-01: an unknown, caller-defined family is never shown as a
/// public prefix; response, stored details, and audit carry only the opaque
/// hint.
#[tokio::test]
async fn sec_skip_preflight_unknown_family_hint_is_opaque() {
    let harness = harness().await;
    let long = "sk-ant-TOKENSECRETxyzTOKENSECRETxyz-tail";
    let short = "sk-ant-TOKEN1234-";
    for (name, key, hint) in [("long", long, "sk-ant-...tail"), ("short", short, "...")] {
        let created = harness
            .create_api_key_upstream(name, key, json!({"skip_preflight": true}))
            .await;
        assert_eq!(created.status, StatusCode::CREATED, "{}", created.body);
        assert_eq!(created.body["key_hint"], hint, "{}", created.body);
        let details = harness.details(created.id()).await.expect("details row");
        assert_eq!(details.key_hint, hint);
    }
    let rows = harness.audit_rows().await;
    assert_eq!(rows.len(), 2);
    for row in rows {
        let action = row.admin_action.clone().unwrap_or_default();
        let payload = row.payload.clone().unwrap_or_default().to_string();
        assert!(!action.contains("TOKEN"), "{action}");
        assert!(!payload.contains("TOKEN"), "{payload}");
        assert!(
            action.contains("key_hint=sk-ant-...tail,") || action.contains("key_hint=...,"),
            "{action}"
        );
    }
    assert_eq!(harness.outbound_count(), 0);
}

#[tokio::test]
async fn crt_09_ciphertext_is_bound_to_upstream_id() {
    let harness = harness().await;
    let a = create_ok(&harness, "a", PLAIN, json!({})).await;
    let b = create_ok(&harness, "b", BND, json!({})).await;
    let a_record = harness.record(a).await;
    let b_record = harness.record(b).await;
    let a_ciphertext = a_record.api_key_ciphertext.clone().unwrap_or_default();
    let b_ciphertext = b_record.api_key_ciphertext.clone().unwrap_or_default();
    assert!(
        !a_ciphertext
            .windows(12)
            .any(|window| PLAIN.as_bytes().windows(12).any(|key| key == window))
    );
    assert_eq!(
        harness.state.aead.decrypt(&a_ciphertext, a.as_bytes()).ok(),
        Some(PLAIN.as_bytes().to_vec())
    );

    harness
        .storage
        .update_api_key_secret(a, Some(b_ciphertext))
        .await
        .expect("swap a");
    harness
        .storage
        .update_api_key_secret(b, Some(a_ciphertext))
        .await
        .expect("swap b");
    for id in [a, b] {
        let response = harness
            .put(
                id,
                &etag(&harness, id).await,
                json!({"base_url": custom_host(&harness)}),
            )
            .await;
        assert_flat(
            &response,
            StatusCode::INTERNAL_SERVER_ERROR,
            "api_key_credential_decrypt_failed",
        );
        assert_no_key_material(&response.body.to_string(), PLAIN);
        assert_no_key_material(&response.body.to_string(), BND);
    }
}

#[tokio::test]
async fn crt_10_custom_base_url_skips_org_probes() {
    let harness = harness().await;
    let created = harness
        .create_api_key_upstream(
            "proxy",
            ORG,
            json!({"base_url": custom_host(&harness), "workspace_id": "wrkspc_P"}),
        )
        .await;
    assert_eq!(created.status, StatusCode::CREATED, "{}", created.body);
    assert_eq!(
        created.body["api_key_access"]["usage_report"],
        "not_applicable"
    );
    assert_eq!(
        created.body["api_key_access"]["workspace_list"],
        "not_applicable"
    );
    assert!(harness.requests(ORG_PREFIX).is_empty());
    let details = harness.details(created.id()).await.expect("details row");
    assert!(
        details
            .remediation
            .contains(&RemediationCode::NonAnthropicBaseUrl)
    );
}

/// A key that cannot be classified (custom host, no workspace header) keeps
/// the operator's workspace on create and PUT instead of silently dropping it.
#[tokio::test]
async fn unclassified_key_on_custom_host_keeps_workspace() {
    let harness = harness().await;
    let created = harness
        .create_api_key_upstream(
            "custom-ws",
            PLAIN,
            json!({"base_url": custom_host(&harness), "workspace_id": "wrkspc_P"}),
        )
        .await;
    assert_eq!(created.status, StatusCode::CREATED, "{}", created.body);
    assert_eq!(created.body["api_key_access"]["key_class"], "unknown");
    assert_eq!(
        created.body["api_key_access"]["workspace"],
        json!({"mode": "selected", "id": "wrkspc_P"})
    );
    let id = created.id();
    assert_eq!(
        harness.record(id).await.workspace_id.as_deref(),
        Some("wrkspc_P")
    );

    let updated = harness
        .put(
            id,
            &etag(&harness, id).await,
            json!({"workspace_id": "wrkspc_S"}),
        )
        .await;
    assert_eq!(updated.status, StatusCode::OK, "{}", updated.body);
    assert_eq!(
        updated.body["api_key_access"]["workspace"],
        json!({"mode": "selected", "id": "wrkspc_S"})
    );
    assert_eq!(
        harness.record(id).await.workspace_id.as_deref(),
        Some("wrkspc_S")
    );

    let rehosted = harness
        .put(
            id,
            &etag(&harness, id).await,
            json!({"base_url": closed_port_url().await}),
        )
        .await;
    assert_eq!(rehosted.status, StatusCode::OK, "{}", rehosted.body);
    assert_eq!(
        harness.record(id).await.workspace_id.as_deref(),
        Some("wrkspc_S")
    );
}

#[tokio::test]
async fn crt_11_response_hygiene() {
    let harness = harness().await;
    let created = harness
        .create_api_key_upstream("hygiene", CANARY, json!({"workspace_id": "wrkspc_P"}))
        .await;
    assert_eq!(created.status, StatusCode::CREATED);
    let text = created.body.to_string();
    assert!(created.body.get("api_key_value").is_none());
    assert!(created.body.get("api_key_ciphertext").is_none());
    assert_no_key_material(&text, CANARY);
    assert!(created.body["api_key_access"].is_object());
    assert_eq!(created.body["key_hint"], CANARY_HINT);
}

// ---------------------------------------------------------------- UPD

#[tokio::test]
async fn upd_01_if_match_is_checked_before_any_probe() {
    let harness = harness().await;
    let id = create_ok(&harness, "org", ORG, json!({"workspace_id": "wrkspc_P"})).await;
    let current = etag(&harness, id).await;
    harness.recorder.clear();
    let body = json!({"workspace_id": "wrkspc_S"});

    let missing = harness
        .request(
            "PUT",
            &format!("{UPSTREAMS}/{id}"),
            Some(body.clone()),
            None,
        )
        .await;
    assert_flat(
        &missing,
        StatusCode::PRECONDITION_REQUIRED,
        "if_match_required",
    );
    let garbage = harness.put(id, "abc", body.clone()).await;
    assert_flat(&garbage, StatusCode::BAD_REQUEST, "invalid_if_match");
    let stale = harness.put(id, "W/\"999\"", body).await;
    assert_flat(&stale, StatusCode::CONFLICT, "stale_revision");
    assert_eq!(etag(&harness, id).await, current);
    assert_eq!(harness.outbound_count(), 0);
}

#[tokio::test]
async fn upd_02_workspace_change_reprobes_with_new_header() {
    let harness = harness().await;
    let id = create_ok(&harness, "org", ORG, json!({"workspace_id": "wrkspc_P"})).await;
    let before = harness.record(id).await;
    harness.recorder.clear();

    let response = harness
        .put(
            id,
            &etag(&harness, id).await,
            json!({"workspace_id": "wrkspc_S"}),
        )
        .await;

    assert_eq!(response.status, StatusCode::OK, "{}", response.body);
    assert_eq!(response.body["spec_revision"], before.revision + 1);
    assert_eq!(response.etag(), format!("W/\"{}\"", before.revision + 1));
    assert_eq!(
        response.body["api_key_access"]["workspace"]["id"],
        "wrkspc_S"
    );
    let after = harness.record(id).await;
    assert_eq!(after.workspace_id.as_deref(), Some("wrkspc_S"));
    let details = harness.details(id).await.expect("details row");
    assert_eq!(
        details.capabilities.workspace.mode,
        ApiKeyWorkspaceMode::Selected
    );
    let probes = harness.requests(COUNT_TOKENS);
    assert!(
        probes
            .iter()
            .any(|request| request.header(WORKSPACE_HEADER) == Some("wrkspc_S")),
        "the re-probe carries the new workspace"
    );
}

#[tokio::test]
async fn upd_04_unknown_workspace_leaves_spec_unchanged() {
    let harness = harness().await;
    let id = create_ok(&harness, "org", ORG, json!({"workspace_id": "wrkspc_P"})).await;
    let before = harness.record(id).await;
    let response = harness
        .put(
            id,
            &etag(&harness, id).await,
            json!({"workspace_id": "wrkspc_unknown"}),
        )
        .await;
    assert_remediation(&response, StatusCode::BAD_REQUEST, "workspace_not_found");
    let after = harness.record(id).await;
    assert_eq!(after.revision, before.revision);
    assert_eq!(after.workspace_id.as_deref(), Some("wrkspc_P"));
}

#[tokio::test]
async fn upd_05_clear_workspace() {
    let harness = harness().await;
    let org = create_ok(&harness, "org", ORG, json!({"workspace_id": "wrkspc_P"})).await;
    let org_before = harness.record(org).await;
    let response = harness
        .put(
            org,
            &etag(&harness, org).await,
            json!({"clear_workspace_id": true}),
        )
        .await;
    assert_remediation(&response, StatusCode::BAD_REQUEST, "workspace_required");
    assert_eq!(harness.record(org).await, org_before);

    let bound = create_ok(&harness, "bound", BND, json!({})).await;
    let bound_before = harness.record(bound).await;
    harness.recorder.clear();
    let response = harness
        .put(
            bound,
            &etag(&harness, bound).await,
            json!({"clear_workspace_id": true}),
        )
        .await;
    assert_eq!(response.status, StatusCode::OK);
    let bound_after = harness.record(bound).await;
    assert_eq!(bound_after.workspace_id, None);
    assert_eq!(bound_after.revision, bound_before.revision);
    assert_eq!(harness.outbound_count(), 0);
}

/// D-FXGAP1: "back on Anthropic" is the injected origin, so the return trip
/// sets `base_url` to the fixture instead of clearing it (clearing would dial
/// the production host).
#[tokio::test]
async fn upd_07_base_url_set_and_return_reprobes() {
    let harness = harness().await;
    let id = create_ok(&harness, "org", ORG, json!({"workspace_id": "wrkspc_P"})).await;
    let before = harness.record(id).await;
    harness.recorder.clear();

    let away = harness
        .put(
            id,
            &etag(&harness, id).await,
            json!({"base_url": custom_host(&harness)}),
        )
        .await;
    assert_eq!(away.status, StatusCode::OK, "{}", away.body);
    assert_eq!(away.body["spec_revision"], before.revision + 1);
    assert_eq!(
        away.body["api_key_access"]["usage_report"],
        "not_applicable"
    );
    assert!(harness.requests(ORG_PREFIX).is_empty());
    let details = harness.details(id).await.expect("details row");
    assert!(
        details
            .remediation
            .contains(&RemediationCode::NonAnthropicBaseUrl)
    );
    assert_eq!(
        details.usage_report_state,
        Some(ApiKeyOrgCapability::NotApplicable)
    );
    assert_eq!(
        harness.record(id).await.workspace_id.as_deref(),
        Some("wrkspc_P")
    );
    assert!(
        harness
            .requests(COUNT_TOKENS)
            .iter()
            .all(|request| request.header(WORKSPACE_HEADER) == Some("wrkspc_P")),
        "the stored workspace is sent at the new host"
    );

    let back = harness
        .put(
            id,
            &etag(&harness, id).await,
            json!({"base_url": harness.fake_url.as_str()}),
        )
        .await;
    assert_eq!(back.status, StatusCode::OK, "{}", back.body);
    assert_eq!(back.body["api_key_access"]["usage_report"], "ok");
    assert!(!harness.requests(ORG_PREFIX).is_empty());
    let details = harness.details(id).await.expect("details row");
    assert_eq!(details.api_key_id.as_deref(), Some("apikey_01"));
    assert_eq!(details.usage_report_state, Some(ApiKeyOrgCapability::Ok));

    // A rejecting probe never fails a host-only PUT (D-Z8).
    let down = closed_port_url().await;
    let unreachable = harness
        .put(id, &etag(&harness, id).await, json!({"base_url": down}))
        .await;
    assert_eq!(unreachable.status, StatusCode::OK, "{}", unreachable.body);
    assert_eq!(unreachable.body["api_key_access"]["verified"], false);
}

// ---------------------------------------------------------------- RPL

#[tokio::test]
async fn rpl_01_full_repreflight_on_replace() {
    let harness = harness().await;
    let id = create_ok(&harness, "replace", BND, json!({})).await;
    let before = harness.record(id).await;
    let response = harness
        .put(
            id,
            &etag(&harness, id).await,
            json!({"api_key_value": ORG, "workspace_id": "wrkspc_P"}),
        )
        .await;
    assert_eq!(response.status, StatusCode::OK, "{}", response.body);
    assert!(
        response.body["spec_revision"].as_u64() > Some(before.revision),
        "a key replace bumps the spec revision"
    );
    assert_ne!(response.etag(), format!("W/\"{}\"", before.revision));
    assert_eq!(response.body["api_key_access"]["key_class"], "org_scoped");
    assert_eq!(
        response.body["api_key_access"]["workspace"]["mode"],
        "selected"
    );
    let record = harness.record(id).await;
    assert_eq!(record.workspace_id.as_deref(), Some("wrkspc_P"));
    assert_eq!(
        harness
            .state
            .aead
            .decrypt(
                record.api_key_ciphertext.as_deref().unwrap_or_default(),
                id.as_bytes()
            )
            .ok(),
        Some(ORG.as_bytes().to_vec())
    );
    assert!(!harness.requests(ORG_PREFIX).is_empty(), "P2-P4 ran");
}

#[tokio::test]
async fn rpl_02_org_to_bound_drops_workspace() {
    let harness = harness().await;
    let id = create_ok(
        &harness,
        "replace",
        ORG,
        json!({"workspace_id": "wrkspc_P"}),
    )
    .await;
    let response = harness
        .put(
            id,
            &etag(&harness, id).await,
            json!({"api_key_value": BND, "clear_workspace_id": true}),
        )
        .await;
    assert_eq!(response.status, StatusCode::OK, "{}", response.body);
    assert_eq!(harness.record(id).await.workspace_id, None);
    assert_eq!(
        response.body["api_key_access"]["key_class"],
        "workspace_bound"
    );
}

#[tokio::test]
async fn rpl_03_rejected_replace_keeps_old_key() {
    let harness = harness().await;
    let id = create_ok(&harness, "replace", BND, json!({})).await;
    let before = harness.record(id).await;
    let details_before = harness.details(id).await;
    let response = harness
        .put(id, &etag(&harness, id).await, json!({"api_key_value": BAD}))
        .await;
    assert_remediation(&response, StatusCode::BAD_REQUEST, "key_invalid");
    assert_eq!(harness.record(id).await, before);
    assert_eq!(harness.details(id).await, details_before);
}

#[tokio::test]
async fn rpl_04_replace_with_skip() {
    let harness = harness().await;
    let id = create_ok(&harness, "replace", BND, json!({})).await;
    let before = harness.record(id).await;
    harness.recorder.clear();
    let response = harness
        .put(
            id,
            &etag(&harness, id).await,
            json!({"api_key_value": FRB, "skip_preflight": true}),
        )
        .await;
    assert_eq!(response.status, StatusCode::OK, "{}", response.body);
    assert!(
        response.body["spec_revision"].as_u64() > Some(before.revision),
        "a key replace bumps the spec revision"
    );
    assert_eq!(response.body["api_key_access"]["verified"], false);
    assert_eq!(harness.outbound_count(), 0);
}

// ---------------------------------------------------------------- SEC

#[tokio::test]
async fn sec_01_admin_responses_never_echo_the_key() {
    let harness = harness().await;
    let mut bodies = Vec::new();
    bodies.push(
        harness
            .preflight(CANARY, json!({"workspace_id": "wrkspc_P"}))
            .await,
    );
    let created = harness
        .create_api_key_upstream("canary", CANARY, json!({"workspace_id": "wrkspc_P"}))
        .await;
    let id = created.id();
    bodies.push(created);
    bodies.push(harness.request("GET", UPSTREAMS, None, None).await);
    bodies.push(harness.get_upstream(id).await);
    bodies.push(
        harness
            .put(
                id,
                &etag(&harness, id).await,
                json!({"api_key_value": CANARY, "workspace_id": "wrkspc_S"}),
            )
            .await,
    );
    for response in &bodies {
        assert!(response.status.is_success(), "{}", response.body);
        let rendered = format!("{:?}{}", response.headers, response.body);
        assert_no_key_material(&rendered, CANARY);
    }
    assert_eq!(bodies[0].body["key_hint"], CANARY_HINT);
    assert_eq!(bodies[1].body["key_hint"], CANARY_HINT);
}

#[tokio::test]
async fn sec_03_audit_rows_carry_only_the_hint() {
    let harness = harness().await;
    harness.set_policy(
        CANARY_401,
        status_policy(401, "authentication_error", "invalid x-api-key"),
    );
    harness
        .preflight(CANARY, json!({"workspace_id": "wrkspc_P"}))
        .await;
    harness.preflight(CANARY_401, json!({})).await;
    let id = create_ok(
        &harness,
        "audit",
        CANARY,
        json!({"workspace_id": "wrkspc_P"}),
    )
    .await;
    let replaced = harness
        .put(
            id,
            &etag(&harness, id).await,
            json!({"api_key_value": CANARY, "workspace_id": "wrkspc_S"}),
        )
        .await;
    assert_eq!(replaced.status, StatusCode::OK);
    let rows = harness.audit_rows().await;
    assert_eq!(rows.len(), 4);
    for row in &rows {
        let rendered = format!("{row:?}");
        assert_no_key_material(&rendered, CANARY);
        assert_no_key_material(&rendered, CANARY_401);
        assert!(rendered.contains("sk-ant-api03-..."), "{rendered}");
    }
}

#[tokio::test]
async fn sec_04_provider_echo_is_scrubbed_and_capped() {
    let harness = harness().await;
    let echo = format!("invalid key {CANARY} {}", "x".repeat(5 * 1024));
    let id = create_ok(&harness, "echo", CANARY, json!({"skip_preflight": true})).await;
    harness.set_policy(CANARY, status_policy(500, "api_error", &echo));

    let preflight = harness.preflight(CANARY, json!({})).await;
    assert_no_key_material(&preflight.body.to_string(), CANARY);
    let put = harness
        .put(
            id,
            &etag(&harness, id).await,
            json!({"base_url": custom_host(&harness)}),
        )
        .await;
    assert_eq!(put.status, StatusCode::OK);
    let last_error = put.body["api_key_access"]["last_error"]
        .as_str()
        .expect("last_error is set")
        .to_owned();
    assert!(last_error.chars().count() <= 200, "{last_error}");
    assert_no_key_material(&last_error, CANARY);
    assert_no_key_material(&put.body.to_string(), CANARY);
}

#[tokio::test]
async fn sec_06_no_key_at_rest_outside_the_ciphertext() {
    let harness = harness().await;
    let id = create_ok(
        &harness,
        "rest",
        CANARY,
        json!({"workspace_id": "wrkspc_P"}),
    )
    .await;
    harness.preflight(CANARY, json!({})).await;
    let replaced = harness
        .put(
            id,
            &etag(&harness, id).await,
            json!({"api_key_value": CANARY, "workspace_id": "wrkspc_S"}),
        )
        .await;
    assert_eq!(replaced.status, StatusCode::OK);
    // Every byte the store wrote (database, WAL, shared memory). The key is
    // only ever sealed, so no 12-byte window of it may appear in plain.
    let mut dump = Vec::new();
    for entry in std::fs::read_dir(harness.storage_dir()).expect("storage dir lists") {
        let path = entry.expect("dir entry").path();
        dump.extend(std::fs::read(&path).expect("storage file reads"));
    }
    let dump = String::from_utf8_lossy(&dump);
    assert!(dump.contains(CANARY_HINT), "the hint is stored");
    assert_no_key_material(&dump, CANARY);
}

// ---------------------------------------------------------------- AUTH

#[tokio::test]
async fn auth_01_unauthenticated_requests_cost_nothing() {
    let harness = harness().await;
    let id = create_ok(&harness, "auth", PLAIN, json!({})).await;
    let rows_before = harness.audit_rows().await.len();
    let outbound = harness.outbound_count();
    let valid = json!({"name": "x", "kind": "anthropic_api_key", "api_key_value": PLAIN, "workspace_id": "wrkspc_P"})
        .to_string();
    let bodies = [
        Some(valid),
        Some("{not json".to_owned()),
        Some("x".repeat(10 * 1024 * 1024)),
    ];
    let routes = [
        ("POST", PREFLIGHT.to_owned()),
        ("POST", UPSTREAMS.to_owned()),
        ("PUT", format!("{UPSTREAMS}/{id}")),
        ("POST", format!("{UPSTREAMS}/{id}/preflight")),
        ("GET", format!("{UPSTREAMS}/{id}/workspaces")),
        ("GET", format!("{UPSTREAMS}/{id}/usage")),
    ];
    for (method, uri) in &routes {
        for token in [None, Some("wrong-token"), Some("")] {
            for body in &bodies {
                let response = harness
                    .raw_request(method, uri, body.clone(), Some("W/\"1\""), token)
                    .await;
                assert_eq!(response.status, StatusCode::UNAUTHORIZED, "{method} {uri}");
            }
        }
    }
    assert_eq!(harness.outbound_count(), outbound);
    assert_eq!(harness.upstream_count().await, 1);
    assert!(harness.audit_rows().await.len() >= rows_before);
    assert_eq!(
        harness
            .audit_rows()
            .await
            .iter()
            .filter(|row| row
                .admin_action
                .as_deref()
                .is_some_and(|action| action.starts_with("upstream_")))
            .count(),
        1
    );
}

#[tokio::test]
async fn auth_02_every_new_route_requires_admin_auth() {
    let harness = harness().await;
    let id = Uuid::new_v4();
    for (method, uri) in [
        ("POST", PREFLIGHT.to_owned()),
        ("POST", format!("{UPSTREAMS}/{id}/preflight")),
        ("GET", format!("{UPSTREAMS}/{id}/workspaces")),
        ("GET", format!("{UPSTREAMS}/{id}/usage")),
        ("POST", UPSTREAMS.to_owned()),
        ("PUT", format!("{UPSTREAMS}/{id}")),
        ("PATCH", format!("{UPSTREAMS}/{id}")),
    ] {
        let response = harness
            .request_with_token(method, &uri, Some(json!({})), None, None)
            .await;
        assert_eq!(response.status, StatusCode::UNAUTHORIZED, "{method} {uri}");
    }
    let get_preflight = harness.request("GET", PREFLIGHT, None, None).await;
    assert_eq!(get_preflight.status, StatusCode::METHOD_NOT_ALLOWED);
}

#[tokio::test]
async fn auth_05_cross_kind_and_unknown_ids_on_put() {
    let harness = harness().await;
    let oauth = harness
        .request(
            "POST",
            UPSTREAMS,
            Some(json!({"name": "oauth", "kind": "anthropic_oauth"})),
            None,
        )
        .await;
    let oauth_id = oauth.id();
    let response = harness
        .put(
            oauth_id,
            &etag(&harness, oauth_id).await,
            json!({"workspace_id": "wrkspc_P"}),
        )
        .await;
    assert_flat(&response, StatusCode::BAD_REQUEST, "not_api_key_upstream");

    let deleted = UpstreamStore::create(
        harness.storage.as_ref(),
        UpstreamCreate {
            id: Uuid::new_v4(),
            name: "deleted".to_owned(),
            kind: UpstreamKind::AnthropicApiKey,
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
    .expect("seed");
    UpstreamStore::soft_delete(harness.storage.as_ref(), deleted.id, deleted.revision)
        .await
        .expect("soft delete");
    for id in [deleted.id, Uuid::new_v4()] {
        let response = harness
            .put(id, "W/\"1\"", json!({"workspace_id": "wrkspc_P"}))
            .await;
        assert_flat(&response, StatusCode::NOT_FOUND, "upstream_not_found");
    }
    assert_eq!(harness.outbound_count(), 0);
}

#[tokio::test]
async fn auth_07_audit_rows_name_identity_action_and_facts() {
    let harness = harness().await;
    harness
        .preflight(ORG, json!({"workspace_id": "wrkspc_P"}))
        .await;
    let id = create_ok(
        &harness,
        "audited",
        ORG,
        json!({"workspace_id": "wrkspc_P"}),
    )
    .await;
    let host = custom_host(&harness);
    let updated = harness
        .put(
            id,
            &etag(&harness, id).await,
            json!({"workspace_id": "wrkspc_S", "base_url": host}),
        )
        .await;
    assert_eq!(updated.status, StatusCode::OK, "{}", updated.body);

    let rows = harness.audit_rows().await;
    assert_eq!(rows.len(), 3);
    let hint = "sk-ant-api03-...OOOO";
    let preflight = rows.iter().find(|row| row.route == PREFLIGHT);
    let create = rows.iter().find(|row| row.route == UPSTREAMS);
    let update_route = format!("{UPSTREAMS}/{id}");
    let update = rows.iter().find(|row| row.route == update_route);
    for (row, verdict) in [(preflight, "ok"), (create, "ok"), (update, "ok")] {
        let row = row.expect("audit row exists");
        assert!(row.actor.is_some());
        let action = row.admin_action.clone().unwrap_or_default();
        assert!(action.contains(hint), "{action}");
        assert!(action.contains(&format!("verdict={verdict}")), "{action}");
        let payload = row.payload.clone().unwrap_or_default().to_string();
        assert!(payload.contains(hint), "{payload}");
    }
    let preflight_action = preflight
        .and_then(|row| row.admin_action.clone())
        .unwrap_or_default();
    assert!(preflight_action.starts_with("upstream_preflight("));
    assert!(preflight_action.contains("remediation="));
    let update_action = update
        .and_then(|row| row.admin_action.clone())
        .unwrap_or_default();
    assert!(
        update_action.contains(&format!("id={id}")),
        "{update_action}"
    );
    assert!(update_action.contains("base_url"), "{update_action}");
    assert!(update_action.contains("workspace_id"), "{update_action}");
    let port = harness.fake_url.port().unwrap_or_default();
    assert!(
        update_action.contains(&format!("old_host=http://127.0.0.1:{port}")),
        "{update_action}"
    );
    assert!(
        update_action.contains(&format!("new_host=http://localhost:{port}")),
        "{update_action}"
    );
}

// ---------------------------------------------------------------- CONC / CTR

#[tokio::test]
async fn conc_01_concurrent_puts_have_one_winner() {
    let harness = harness().await;
    let id = create_ok(&harness, "race", ORG, json!({"workspace_id": "wrkspc_P"})).await;
    let tag = etag(&harness, id).await;
    let (first, second) = tokio::join!(
        harness.put(id, &tag, json!({"workspace_id": "wrkspc_S"})),
        harness.put(id, &tag, json!({"workspace_id": "wrkspc_T"})),
    );
    let mut statuses = [first.status, second.status];
    statuses.sort();
    assert_eq!(statuses, [StatusCode::OK, StatusCode::CONFLICT]);
    let winner = if first.status == StatusCode::OK {
        "wrkspc_S"
    } else {
        "wrkspc_T"
    };
    let record = harness.record(id).await;
    assert_eq!(record.workspace_id.as_deref(), Some(winner));
    let details = harness.details(id).await.expect("details row");
    assert_eq!(details.capabilities.workspace.id.as_deref(), Some(winner));
}

#[tokio::test]
async fn ctr_02_error_envelopes() {
    let harness = harness().await;
    let id = create_ok(&harness, "envelope", PLAIN, json!({})).await;
    let tag = etag(&harness, id).await;

    // Remediation codes: nested body.
    for (response, status, code) in [
        (
            harness.create_api_key_upstream("e1", ADM, json!({})).await,
            StatusCode::BAD_REQUEST,
            "key_admin",
        ),
        (
            harness.create_api_key_upstream("e2", BAD, json!({})).await,
            StatusCode::BAD_REQUEST,
            "key_invalid",
        ),
        (
            harness.create_api_key_upstream("e3", ORG, json!({})).await,
            StatusCode::BAD_REQUEST,
            "workspace_required",
        ),
        (
            harness.put(id, &tag, json!({"workspace_id": "nope"})).await,
            StatusCode::BAD_REQUEST,
            "workspace_id_malformed",
        ),
        (
            harness.create_api_key_upstream("e4", DWN, json!({})).await,
            StatusCode::BAD_GATEWAY,
            "probe_unreachable",
        ),
    ] {
        assert_remediation(&response, status, code);
        assert!(response.body["error"].is_object());
    }

    // Generic failures stay flat.
    for (response, status, error) in [
        (
            harness.create_api_key_upstream("e5", "", json!({})).await,
            StatusCode::BAD_REQUEST,
            "missing_api_key",
        ),
        (
            harness.put(id, "W/\"999\"", json!({})).await,
            StatusCode::CONFLICT,
            "stale_revision",
        ),
        (
            harness
                .create_api_key_upstream("envelope", PLAIN, json!({}))
                .await,
            StatusCode::CONFLICT,
            "upstream_name_conflict",
        ),
    ] {
        assert_flat(&response, status, error);
    }
}
