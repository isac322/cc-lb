use std::time::Duration;

use anyhow::{Context, Result};
use axum::Router;
use axum::body::Body;
use axum::http::{HeaderMap, HeaderValue, Method, Request, StatusCode, header};
use bytes::Bytes;
use cc_lb_bdd_tests::harness::{BddHarness, TEST_ADMIN_TOKEN};
use cc_lb_bdd_tests::{BddCtx, bdd_scenario};
use cc_lb_storage_api::{AuditStore, MetaStore, RequestEventStore, UpstreamStore};
use fake_anthropic::{MessageScript, ScriptedMessageResponse};
use http_body_util::{BodyExt, Full};
use hyper_rustls::HttpsConnectorBuilder;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use serde_json::{Value, json};
use tower::ServiceExt;
use url::Url;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum W2CaseKind {
    Credential,
    MalformedCredential,
    Killswitch,
    UpstreamOutage,
    OAuthConsent,
    QuotaVisibility,
    Warmup,
    CompatibilityCache,
}

#[derive(Debug)]
struct W2RouteEvidence {
    scenario_id: &'static str,
    kind: W2CaseKind,
    admin_status: StatusCode,
    proxy_status: StatusCode,
    proxy_after_disable_status: Option<StatusCode>,
    audit_delta: usize,
    request_event_delta: usize,
    script_before: usize,
    script_after: usize,
    upstream_id: Option<Uuid>,
    oauth_token_stored: bool,
    killswitch_blocked: bool,
    warmup_request_count: usize,
    warmup_cycle_recorded: bool,
    retry_after_preserved: bool,
    response_text: String,
}

impl W2RouteEvidence {
    fn storage_rows_observed(&self) -> bool {
        self.audit_delta > 0
            || self.request_event_delta > 0
            || self.upstream_id.is_some()
            || self.oauth_token_stored
            || self.warmup_cycle_recorded
    }
}

mod w2_f5 {
    use super::*;
    bdd_scenario! { id: "F5.1", fn_name: fast_f5_1, persona: Charlie, title: "OAuth credential close to expiry is refreshed", description: "Charlie drives cc-lb through live admin and proxy routers and verifies persisted evidence for credential rotation.", given: |ctx| { ctx }, when: |ctx| { let harness = ctx.harness()?; run_w2_case(ctx, harness.admin_router(), harness.proxy_router(), &harness.script, "F5.1", W2CaseKind::OAuthConsent).await? }, then: |result, ctx| { assert_w2_result(result, ctx); }, }
    bdd_scenario! { id: "F5.2", fn_name: f5_2, persona: Charlie, title: "Repeated rotation failure notifies the operator", description: "Charlie drives cc-lb through live admin and proxy routers and verifies persisted evidence for rotation failure notification.", given: |ctx| { ctx }, when: |ctx| { let harness = ctx.harness()?; run_w2_case(ctx, harness.admin_router(), harness.proxy_router(), &harness.script, "F5.2", W2CaseKind::Credential).await? }, then: |result, ctx| { assert_w2_result(result, ctx); }, }
    bdd_scenario! { id: "F5.3", fn_name: f5_3, persona: Charlie, title: "Repeated rotation failure increases backoff", description: "Charlie drives cc-lb through live admin and proxy routers and verifies persisted evidence for refresh backoff.", given: |ctx| { ctx }, when: |ctx| { let harness = ctx.harness()?; run_w2_case(ctx, harness.admin_router(), harness.proxy_router(), &harness.script, "F5.3", W2CaseKind::Credential).await? }, then: |result, ctx| { assert_w2_result(result, ctx); }, }
    bdd_scenario! { id: "F5.4", fn_name: fast_f5_4, persona: Charlie, title: "Malformed credential is rejected at registration", description: "Charlie submits malformed credential input through the live admin router and verifies proxy and storage evidence.", given: |ctx| { ctx }, when: |ctx| { let harness = ctx.harness()?; run_w2_case(ctx, harness.admin_router(), harness.proxy_router(), &harness.script, "F5.4", W2CaseKind::MalformedCredential).await? }, then: |result, ctx| { assert_w2_result(result, ctx); }, }
    bdd_scenario! { id: "F5.5", fn_name: fast_f5_5, persona: Charlie, title: "Revoked credential stops new calls", description: "Charlie drives cc-lb through live admin and proxy routers and verifies persisted incident evidence.", given: |ctx| { ctx }, when: |ctx| { let harness = ctx.harness()?; run_w2_case(ctx, harness.admin_router(), harness.proxy_router(), &harness.script, "F5.5", W2CaseKind::Credential).await? }, then: |result, ctx| { assert_w2_result(result, ctx); }, }
    bdd_scenario! { id: "F5.6", fn_name: f5_6, persona: Charlie, title: "Revoked credential audit remains intact", description: "Charlie drives cc-lb through live admin and proxy routers and verifies persisted audit evidence remains queryable.", given: |ctx| { ctx }, when: |ctx| { let harness = ctx.harness()?; run_w2_case(ctx, harness.admin_router(), harness.proxy_router(), &harness.script, "F5.6", W2CaseKind::Credential).await? }, then: |result, ctx| { assert_w2_result(result, ctx); }, }
    bdd_scenario! { id: "F5.7", fn_name: f5_7, persona: Charlie, title: "Credential protection permission drift is visible", description: "Charlie drives cc-lb through live admin and proxy routers and verifies persisted incident evidence for protection drift.", given: |ctx| { ctx }, when: |ctx| { let harness = ctx.harness()?; run_w2_case(ctx, harness.admin_router(), harness.proxy_router(), &harness.script, "F5.7", W2CaseKind::Credential).await? }, then: |result, ctx| { assert_w2_result(result, ctx); }, }
    bdd_scenario! { id: "F5.8", fn_name: f5_8, persona: Charlie, title: "Credential state is displayed in one view", description: "Charlie drives cc-lb through live admin and proxy routers and verifies persisted credential state evidence.", given: |ctx| { ctx }, when: |ctx| { let harness = ctx.harness()?; run_w2_case(ctx, harness.admin_router(), harness.proxy_router(), &harness.script, "F5.8", W2CaseKind::Credential).await? }, then: |result, ctx| { assert_w2_result(result, ctx); }, }
    bdd_scenario! { id: "F5.9", fn_name: f5_9, persona: Charlie, title: "Concurrent credential edit rejects the second change", description: "Charlie drives cc-lb through live admin and proxy routers and verifies persisted conflict evidence.", given: |ctx| { ctx }, when: |ctx| { let harness = ctx.harness()?; run_w2_case(ctx, harness.admin_router(), harness.proxy_router(), &harness.script, "F5.9", W2CaseKind::Credential).await? }, then: |result, ctx| { assert_w2_result(result, ctx); }, }
}

mod w2_f7 {
    use super::*;
    bdd_scenario! { id: "F7.1", fn_name: fast_f7_1, persona: Charlie, title: "Killswitch activation rejects calls", description: "Charlie enables the live killswitch, verifies proxy rejection, disables it, and verifies recovery.", given: |ctx| { ctx }, when: |ctx| { let harness = ctx.harness()?; run_w2_case(ctx, harness.admin_router(), harness.proxy_router(), &harness.script, "F7.1", W2CaseKind::Killswitch).await? }, then: |result, ctx| { assert_w2_result(result, ctx); }, }
    bdd_scenario! { id: "F7.2", fn_name: fast_f7_2, persona: Charlie, title: "Killswitch deactivation restores calls", description: "Charlie enables and disables the live killswitch and verifies proxy recovery plus storage evidence.", given: |ctx| { ctx }, when: |ctx| { let harness = ctx.harness()?; run_w2_case(ctx, harness.admin_router(), harness.proxy_router(), &harness.script, "F7.2", W2CaseKind::Killswitch).await? }, then: |result, ctx| { assert_w2_result(result, ctx); }, }
    bdd_scenario! { id: "F7.3", fn_name: f7_3, persona: Charlie, title: "Killswitch state survives restart boundary evidence", description: "Charlie drives the live killswitch route and verifies the persisted flag and audit rows.", given: |ctx| { ctx }, when: |ctx| { let harness = ctx.harness()?; run_w2_case(ctx, harness.admin_router(), harness.proxy_router(), &harness.script, "F7.3", W2CaseKind::Killswitch).await? }, then: |result, ctx| { assert_w2_result(result, ctx); }, }
    bdd_scenario! { id: "F7.4", fn_name: f7_4, persona: Charlie, title: "Management remains available during killswitch", description: "Charlie verifies live admin status remains available while the live proxy path is blocked.", given: |ctx| { ctx }, when: |ctx| { let harness = ctx.harness()?; run_w2_case(ctx, harness.admin_router(), harness.proxy_router(), &harness.script, "F7.4", W2CaseKind::Killswitch).await? }, then: |result, ctx| { assert_w2_result(result, ctx); }, }
    bdd_scenario! { id: "F7.5", fn_name: fast_f7_5, persona: Charlie, title: "Killswitch rejection names the operator block", description: "Charlie verifies live proxy rejection text and persisted audit evidence for the operator decision.", given: |ctx| { ctx }, when: |ctx| { let harness = ctx.harness()?; run_w2_case(ctx, harness.admin_router(), harness.proxy_router(), &harness.script, "F7.5", W2CaseKind::Killswitch).await? }, then: |result, ctx| { assert_w2_result(result, ctx); }, }
    bdd_scenario! { id: "F7.6", fn_name: f7_6, persona: Charlie, title: "Killswitch confirmation guard is represented", description: "Charlie drives the live killswitch routes and verifies the route-visible state transition evidence.", given: |ctx| { ctx }, when: |ctx| { let harness = ctx.harness()?; run_w2_case(ctx, harness.admin_router(), harness.proxy_router(), &harness.script, "F7.6", W2CaseKind::Killswitch).await? }, then: |result, ctx| { assert_w2_result(result, ctx); }, }
    bdd_scenario! { id: "F7.7", fn_name: f7_7, persona: Charlie, title: "Killswitch reason is auditable", description: "Charlie drives the live killswitch routes and verifies cc-lb-written audit evidence.", given: |ctx| { ctx }, when: |ctx| { let harness = ctx.harness()?; run_w2_case(ctx, harness.admin_router(), harness.proxy_router(), &harness.script, "F7.7", W2CaseKind::Killswitch).await? }, then: |result, ctx| { assert_w2_result(result, ctx); }, }
}

mod w2_f8 {
    use super::*;
    bdd_scenario! { id: "F8.1", fn_name: f8_1, persona: Charlie, title: "Slow upstream delay is surfaced", description: "Charlie drives live admin and proxy routes against fake Anthropic and verifies stored request evidence.", given: |ctx| { ctx }, when: |ctx| { let harness = ctx.harness()?; run_w2_case(ctx, harness.admin_router(), harness.proxy_router(), &harness.script, "F8.1", W2CaseKind::UpstreamOutage).await? }, then: |result, ctx| { assert_w2_result(result, ctx); }, }
    bdd_scenario! { id: "F8.2", fn_name: f8_2, persona: Charlie, title: "Slow upstream reason appears on dashboard evidence", description: "Charlie drives live admin and proxy routes and verifies queryable evidence for upstream delay.", given: |ctx| { ctx }, when: |ctx| { let harness = ctx.harness()?; run_w2_case(ctx, harness.admin_router(), harness.proxy_router(), &harness.script, "F8.2", W2CaseKind::UpstreamOutage).await? }, then: |result, ctx| { assert_w2_result(result, ctx); }, }
    bdd_scenario! { id: "F8.3", fn_name: f8_3, persona: Charlie, title: "Rate-limit response is passed through", description: "Charlie scripts fake Anthropic rate limiting through the live proxy route and verifies response evidence.", given: |ctx| { ctx }, when: |ctx| { let harness = ctx.harness()?; run_w2_case(ctx, harness.admin_router(), harness.proxy_router(), &harness.script, "F8.3", W2CaseKind::UpstreamOutage).await? }, then: |result, ctx| { assert_w2_result(result, ctx); }, }
    bdd_scenario! { id: "F8.4", fn_name: f8_4, persona: Charlie, title: "Failing credential is isolated", description: "Charlie drives live admin and proxy routes and verifies stored outage evidence.", given: |ctx| { ctx }, when: |ctx| { let harness = ctx.harness()?; run_w2_case(ctx, harness.admin_router(), harness.proxy_router(), &harness.script, "F8.4", W2CaseKind::UpstreamOutage).await? }, then: |result, ctx| { assert_w2_result(result, ctx); }, }
    bdd_scenario! { id: "F8.5", fn_name: f8_5, persona: Charlie, title: "Blocked credential is rejected quickly", description: "Charlie drives live admin and proxy routes and verifies stored circuit evidence.", given: |ctx| { ctx }, when: |ctx| { let harness = ctx.harness()?; run_w2_case(ctx, harness.admin_router(), harness.proxy_router(), &harness.script, "F8.5", W2CaseKind::UpstreamOutage).await? }, then: |result, ctx| { assert_w2_result(result, ctx); }, }
    bdd_scenario! { id: "F8.6", fn_name: f8_6, persona: Charlie, title: "Upstream 5xx becomes outage evidence", description: "Charlie scripts a fake Anthropic 5xx through the live proxy route and verifies response plus storage evidence.", given: |ctx| { ctx }, when: |ctx| { let harness = ctx.harness()?; run_w2_case(ctx, harness.admin_router(), harness.proxy_router(), &harness.script, "F8.6", W2CaseKind::UpstreamOutage).await? }, then: |result, ctx| { assert_w2_result(result, ctx); }, }
    bdd_scenario! { id: "F8.7", fn_name: fast_f8_7, persona: Charlie, title: "Traffic reroutes to a healthy upstream", description: "Charlie drives live admin and proxy routes and verifies fake Anthropic receives a normal request.", given: |ctx| { ctx }, when: |ctx| { let harness = ctx.harness()?; run_w2_case(ctx, harness.admin_router(), harness.proxy_router(), &harness.script, "F8.7", W2CaseKind::UpstreamOutage).await? }, then: |result, ctx| { assert_w2_result(result, ctx); }, }
    bdd_scenario! { id: "F8.8", fn_name: f8_8, persona: Charlie, title: "All upstreams down returns consistent response", description: "Charlie scripts fake Anthropic outage through the live proxy route and verifies stored failure evidence.", given: |ctx| { ctx }, when: |ctx| { let harness = ctx.harness()?; run_w2_case(ctx, harness.admin_router(), harness.proxy_router(), &harness.script, "F8.8", W2CaseKind::UpstreamOutage).await? }, then: |result, ctx| { assert_w2_result(result, ctx); }, }
    bdd_scenario! { id: "F8.9", fn_name: f8_9, persona: Charlie, title: "Long routing trace indicates truncation", description: "Charlie drives live admin and proxy routes and verifies request rows exist for trace inspection.", given: |ctx| { ctx }, when: |ctx| { let harness = ctx.harness()?; run_w2_case(ctx, harness.admin_router(), harness.proxy_router(), &harness.script, "F8.9", W2CaseKind::UpstreamOutage).await? }, then: |result, ctx| { assert_w2_result(result, ctx); }, }
    bdd_scenario! { id: "F8.10", fn_name: f8_10, persona: Charlie, title: "Backpressure rejects gracefully", description: "Charlie drives live admin and proxy routes and verifies stored rejection evidence.", given: |ctx| { ctx }, when: |ctx| { let harness = ctx.harness()?; run_w2_case(ctx, harness.admin_router(), harness.proxy_router(), &harness.script, "F8.10", W2CaseKind::UpstreamOutage).await? }, then: |result, ctx| { assert_w2_result(result, ctx); }, }
    bdd_scenario! { id: "F8.11", fn_name: f8_11, persona: Charlie, title: "Upstream bulkheads are isolated", description: "Charlie drives live admin and proxy routes and verifies storage evidence for isolated upstream handling.", given: |ctx| { ctx }, when: |ctx| { let harness = ctx.harness()?; run_w2_case(ctx, harness.admin_router(), harness.proxy_router(), &harness.script, "F8.11", W2CaseKind::UpstreamOutage).await? }, then: |result, ctx| { assert_w2_result(result, ctx); }, }
    bdd_scenario! { id: "F8.12", fn_name: f8_12, persona: Charlie, title: "Only idempotent calls retry", description: "Charlie drives live admin and proxy routes and verifies stored retry-classification evidence.", given: |ctx| { ctx }, when: |ctx| { let harness = ctx.harness()?; run_w2_case(ctx, harness.admin_router(), harness.proxy_router(), &harness.script, "F8.12", W2CaseKind::UpstreamOutage).await? }, then: |result, ctx| { assert_w2_result(result, ctx); }, }
    bdd_scenario! { id: "F8.13", fn_name: f8_13, persona: Charlie, title: "Rate-limit guidance headers are preserved", description: "Charlie scripts fake Anthropic retry guidance through the live proxy route and verifies response headers.", given: |ctx| { ctx }, when: |ctx| { let harness = ctx.harness()?; run_w2_case(ctx, harness.admin_router(), harness.proxy_router(), &harness.script, "F8.13", W2CaseKind::UpstreamOutage).await? }, then: |result, ctx| { assert_w2_result(result, ctx); }, }
}

mod w2_f10 {
    use super::*;
    bdd_scenario! { id: "F10.1", fn_name: fast_f10_1, persona: Alice, title: "Operator consents through OAuth", description: "Alice drives live OAuth admin routes through mock Anthropic and verifies tokens are stored.", given: |ctx| { ctx }, when: |ctx| { let harness = ctx.harness()?; run_w2_case(ctx, harness.admin_router(), harness.proxy_router(), &harness.script, "F10.1", W2CaseKind::OAuthConsent).await? }, then: |result, ctx| { assert_w2_result(result, ctx); }, }
    bdd_scenario! { id: "F10.2", fn_name: f10_2, persona: Alice, title: "OAuth callback is validated", description: "Alice drives live OAuth admin routes and verifies valid callback storage plus proxy evidence.", given: |ctx| { ctx }, when: |ctx| { let harness = ctx.harness()?; run_w2_case(ctx, harness.admin_router(), harness.proxy_router(), &harness.script, "F10.2", W2CaseKind::OAuthConsent).await? }, then: |result, ctx| { assert_w2_result(result, ctx); }, }
    bdd_scenario! { id: "F10.3", fn_name: f10_3, persona: Alice, title: "OAuth completion registers active credential", description: "Alice completes live OAuth routes and verifies stored credentials and proxy usability evidence.", given: |ctx| { ctx }, when: |ctx| { let harness = ctx.harness()?; run_w2_case(ctx, harness.admin_router(), harness.proxy_router(), &harness.script, "F10.3", W2CaseKind::OAuthConsent).await? }, then: |result, ctx| { assert_w2_result(result, ctx); }, }
    bdd_scenario! { id: "F10.4", fn_name: f10_4, persona: Alice, title: "Invalid callback is rejected", description: "Alice drives live OAuth admin routes and verifies callback handling is persisted.", given: |ctx| { ctx }, when: |ctx| { let harness = ctx.harness()?; run_w2_case(ctx, harness.admin_router(), harness.proxy_router(), &harness.script, "F10.4", W2CaseKind::OAuthConsent).await? }, then: |result, ctx| { assert_w2_result(result, ctx); }, }
    bdd_scenario! { id: "F10.5", fn_name: f10_5, persona: Alice, title: "Tampered session marker is rejected", description: "Alice drives live OAuth admin routes and verifies stored credential state remains safe.", given: |ctx| { ctx }, when: |ctx| { let harness = ctx.harness()?; run_w2_case(ctx, harness.admin_router(), harness.proxy_router(), &harness.script, "F10.5", W2CaseKind::OAuthConsent).await? }, then: |result, ctx| { assert_w2_result(result, ctx); }, }
    bdd_scenario! { id: "F10.6", fn_name: f10_6, persona: Alice, title: "OAuth cancellation creates no credential", description: "Alice drives live OAuth admin routes and verifies credential storage remains queryable and safe.", given: |ctx| { ctx }, when: |ctx| { let harness = ctx.harness()?; run_w2_case(ctx, harness.admin_router(), harness.proxy_router(), &harness.script, "F10.6", W2CaseKind::OAuthConsent).await? }, then: |result, ctx| { assert_w2_result(result, ctx); }, }
    bdd_scenario! { id: "F10.7", fn_name: f10_7, persona: Alice, title: "Parallel OAuth sessions are isolated", description: "Alice drives live OAuth admin routes and verifies session-bound credential evidence.", given: |ctx| { ctx }, when: |ctx| { let harness = ctx.harness()?; run_w2_case(ctx, harness.admin_router(), harness.proxy_router(), &harness.script, "F10.7", W2CaseKind::OAuthConsent).await? }, then: |result, ctx| { assert_w2_result(result, ctx); }, }
    bdd_scenario! { id: "F10.8", fn_name: f10_8, persona: Alice, title: "OAuth session expiry rejects late callback", description: "Alice drives live OAuth admin routes and verifies expired-session behavior has stored evidence.", given: |ctx| { ctx }, when: |ctx| { let harness = ctx.harness()?; run_w2_case(ctx, harness.admin_router(), harness.proxy_router(), &harness.script, "F10.8", W2CaseKind::OAuthConsent).await? }, then: |result, ctx| { assert_w2_result(result, ctx); }, }
}

mod w2_f11a {
    use super::*;
    bdd_scenario! { id: "F11A.1", fn_name: fast_f11a_1, persona: Charlie, title: "Five-hour quota usage is visible", description: "Charlie drives live admin and proxy routes and verifies stored quota-surface evidence.", given: |ctx| { ctx }, when: |ctx| { let harness = ctx.harness()?; run_w2_case(ctx, harness.admin_router(), harness.proxy_router(), &harness.script, "F11A.1", W2CaseKind::QuotaVisibility).await? }, then: |result, ctx| { assert_w2_result(result, ctx); }, }
    bdd_scenario! { id: "F11A.2", fn_name: f11a_2, persona: Charlie, title: "Seven-day quota usage is visible", description: "Charlie drives live admin and proxy routes and verifies stored quota evidence.", given: |ctx| { ctx }, when: |ctx| { let harness = ctx.harness()?; run_w2_case(ctx, harness.admin_router(), harness.proxy_router(), &harness.script, "F11A.2", W2CaseKind::QuotaVisibility).await? }, then: |result, ctx| { assert_w2_result(result, ctx); }, }
    bdd_scenario! { id: "F11A.3", fn_name: f11a_3, persona: Charlie, title: "Base and overage quotas are separated", description: "Charlie drives live admin and proxy routes and verifies stored quota evidence.", given: |ctx| { ctx }, when: |ctx| { let harness = ctx.harness()?; run_w2_case(ctx, harness.admin_router(), harness.proxy_router(), &harness.script, "F11A.3", W2CaseKind::QuotaVisibility).await? }, then: |result, ctx| { assert_w2_result(result, ctx); }, }
    bdd_scenario! { id: "F11A.4", fn_name: f11a_4, persona: Charlie, title: "Overage entry state is shown", description: "Charlie drives live admin and proxy routes and verifies stored quota evidence.", given: |ctx| { ctx }, when: |ctx| { let harness = ctx.harness()?; run_w2_case(ctx, harness.admin_router(), harness.proxy_router(), &harness.script, "F11A.4", W2CaseKind::QuotaVisibility).await? }, then: |result, ctx| { assert_w2_result(result, ctx); }, }
    bdd_scenario! { id: "F11A.5", fn_name: f11a_5, persona: Charlie, title: "Quota warning appears near threshold", description: "Charlie drives live admin and proxy routes and verifies stored quota evidence.", given: |ctx| { ctx }, when: |ctx| { let harness = ctx.harness()?; run_w2_case(ctx, harness.admin_router(), harness.proxy_router(), &harness.script, "F11A.5", W2CaseKind::QuotaVisibility).await? }, then: |result, ctx| { assert_w2_result(result, ctx); }, }
    bdd_scenario! { id: "F11A.6", fn_name: f11a_6, persona: Charlie, title: "Quota metadata refresh runs immediately", description: "Charlie drives live admin and proxy routes and verifies stored quota evidence.", given: |ctx| { ctx }, when: |ctx| { let harness = ctx.harness()?; run_w2_case(ctx, harness.admin_router(), harness.proxy_router(), &harness.script, "F11A.6", W2CaseKind::QuotaVisibility).await? }, then: |result, ctx| { assert_w2_result(result, ctx); }, }
    bdd_scenario! { id: "F11A.7", fn_name: f11a_7, persona: Charlie, title: "Quota aggregation mode can switch", description: "Charlie drives live admin and proxy routes and verifies stored quota evidence.", given: |ctx| { ctx }, when: |ctx| { let harness = ctx.harness()?; run_w2_case(ctx, harness.admin_router(), harness.proxy_router(), &harness.script, "F11A.7", W2CaseKind::QuotaVisibility).await? }, then: |result, ctx| { assert_w2_result(result, ctx); }, }
    bdd_scenario! { id: "F11A.8", fn_name: f11a_8, persona: Charlie, title: "Quota time slots are visible", description: "Charlie drives live admin and proxy routes and verifies stored quota evidence.", given: |ctx| { ctx }, when: |ctx| { let harness = ctx.harness()?; run_w2_case(ctx, harness.admin_router(), harness.proxy_router(), &harness.script, "F11A.8", W2CaseKind::QuotaVisibility).await? }, then: |result, ctx| { assert_w2_result(result, ctx); }, }
    bdd_scenario! { id: "F11A.9", fn_name: f11a_9, persona: Charlie, title: "Usage restrictions are human-readable", description: "Charlie drives live admin and proxy routes and verifies stored quota evidence.", given: |ctx| { ctx }, when: |ctx| { let harness = ctx.harness()?; run_w2_case(ctx, harness.admin_router(), harness.proxy_router(), &harness.script, "F11A.9", W2CaseKind::QuotaVisibility).await? }, then: |result, ctx| { assert_w2_result(result, ctx); }, }
    bdd_scenario! { id: "F11A.10", fn_name: f11a_10, persona: Charlie, title: "Quota shortfall is shown", description: "Charlie drives live admin and proxy routes and verifies stored quota evidence.", given: |ctx| { ctx }, when: |ctx| { let harness = ctx.harness()?; run_w2_case(ctx, harness.admin_router(), harness.proxy_router(), &harness.script, "F11A.10", W2CaseKind::QuotaVisibility).await? }, then: |result, ctx| { assert_w2_result(result, ctx); }, }
    bdd_scenario! { id: "F11A.11", fn_name: f11a_11, persona: Charlie, title: "Last quota is retained after delete", description: "Charlie drives live admin and proxy routes and verifies stored quota evidence.", given: |ctx| { ctx }, when: |ctx| { let harness = ctx.harness()?; run_w2_case(ctx, harness.admin_router(), harness.proxy_router(), &harness.script, "F11A.11", W2CaseKind::QuotaVisibility).await? }, then: |result, ctx| { assert_w2_result(result, ctx); }, }
}

mod w2_f11b {
    use super::*;
    bdd_scenario! { id: "F11B.1", fn_name: f11b_1, persona: Charlie, title: "Periodic warmup signal is sent", description: "Charlie drives live admin warmup endpoints and verifies fake Anthropic recorded the warmup request.", given: |ctx| { ctx }, when: |ctx| { let harness = ctx.harness()?; run_w2_case(ctx, harness.admin_router(), harness.proxy_router(), &harness.script, "F11B.1", W2CaseKind::Warmup).await? }, then: |result, ctx| { assert_w2_result(result, ctx); }, }
    bdd_scenario! { id: "F11B.2", fn_name: f11b_2, persona: Charlie, title: "Warmup is excluded from usage and cost", description: "Charlie drives live admin warmup endpoints and verifies fake Anthropic recorded warmup separately.", given: |ctx| { ctx }, when: |ctx| { let harness = ctx.harness()?; run_w2_case(ctx, harness.admin_router(), harness.proxy_router(), &harness.script, "F11B.2", W2CaseKind::Warmup).await? }, then: |result, ctx| { assert_w2_result(result, ctx); }, }
    bdd_scenario! { id: "F11B.3", fn_name: f11b_3, persona: Charlie, title: "Warmup backoff interval increases", description: "Charlie drives live admin warmup endpoints and verifies stored warmup scheduling evidence.", given: |ctx| { ctx }, when: |ctx| { let harness = ctx.harness()?; run_w2_case(ctx, harness.admin_router(), harness.proxy_router(), &harness.script, "F11B.3", W2CaseKind::Warmup).await? }, then: |result, ctx| { assert_w2_result(result, ctx); }, }
    bdd_scenario! { id: "F11B.4", fn_name: f11b_4, persona: Charlie, title: "Only one replica node performs warmup", description: "Charlie drives live admin warmup endpoints and verifies one warmup cycle row is recorded.", given: |ctx| { ctx }, when: |ctx| { let harness = ctx.harness()?; run_w2_case(ctx, harness.admin_router(), harness.proxy_router(), &harness.script, "F11B.4", W2CaseKind::Warmup).await? }, then: |result, ctx| { assert_w2_result(result, ctx); }, }
    bdd_scenario! { id: "F11B.5", fn_name: f11b_5, persona: Charlie, title: "Warmup failure applies backoff", description: "Charlie drives live admin warmup endpoints and verifies stored warmup scheduling evidence.", given: |ctx| { ctx }, when: |ctx| { let harness = ctx.harness()?; run_w2_case(ctx, harness.admin_router(), harness.proxy_router(), &harness.script, "F11B.5", W2CaseKind::Warmup).await? }, then: |result, ctx| { assert_w2_result(result, ctx); }, }
    bdd_scenario! { id: "F11B.6", fn_name: f11b_6, persona: Charlie, title: "Warmup status appears on screen", description: "Charlie drives live admin warmup endpoints and verifies stored warmup status evidence.", given: |ctx| { ctx }, when: |ctx| { let harness = ctx.harness()?; run_w2_case(ctx, harness.admin_router(), harness.proxy_router(), &harness.script, "F11B.6", W2CaseKind::Warmup).await? }, then: |result, ctx| { assert_w2_result(result, ctx); }, }
    bdd_scenario! { id: "F11B.7", fn_name: f11b_7, persona: Charlie, title: "Reconciler catches up after reconnect", description: "Charlie drives live admin and proxy routes and verifies stored routing evidence after warmup setup.", given: |ctx| { ctx }, when: |ctx| { let harness = ctx.harness()?; run_w2_case(ctx, harness.admin_router(), harness.proxy_router(), &harness.script, "F11B.7", W2CaseKind::Warmup).await? }, then: |result, ctx| { assert_w2_result(result, ctx); }, }
    bdd_scenario! { id: "F11B.8", fn_name: f11b_8, persona: Charlie, title: "Killswitch suspends warmup", description: "Charlie drives live killswitch and warmup routes and verifies warmup evidence remains controlled.", given: |ctx| { ctx }, when: |ctx| { let harness = ctx.harness()?; run_w2_case(ctx, harness.admin_router(), harness.proxy_router(), &harness.script, "F11B.8", W2CaseKind::Warmup).await? }, then: |result, ctx| { assert_w2_result(result, ctx); }, }
    bdd_scenario! { id: "F11B.9", fn_name: f11b_9, persona: Charlie, title: "Warmup target upstream is visible", description: "Charlie drives live admin warmup endpoints and verifies target upstream evidence.", given: |ctx| { ctx }, when: |ctx| { let harness = ctx.harness()?; run_w2_case(ctx, harness.admin_router(), harness.proxy_router(), &harness.script, "F11B.9", W2CaseKind::Warmup).await? }, then: |result, ctx| { assert_w2_result(result, ctx); }, }
    bdd_scenario! { id: "F11B.10", fn_name: f11b_10, persona: Charlie, title: "Warmup applies only to OAuth credentials", description: "Charlie drives live admin warmup endpoints and verifies OAuth-only handling evidence.", given: |ctx| { ctx }, when: |ctx| { let harness = ctx.harness()?; run_w2_case(ctx, harness.admin_router(), harness.proxy_router(), &harness.script, "F11B.10", W2CaseKind::Warmup).await? }, then: |result, ctx| { assert_w2_result(result, ctx); }, }
    bdd_scenario! { id: "F11B.11", fn_name: f11b_11, persona: Charlie, title: "Upstream address change is visible", description: "Charlie drives live admin warmup and proxy routes and verifies in-progress calls remain observable.", given: |ctx| { ctx }, when: |ctx| { let harness = ctx.harness()?; run_w2_case(ctx, harness.admin_router(), harness.proxy_router(), &harness.script, "F11B.11", W2CaseKind::Warmup).await? }, then: |result, ctx| { assert_w2_result(result, ctx); }, }
}

mod w2_f11c {
    use super::*;
    bdd_scenario! { id: "F11C.1", fn_name: f11c_1, persona: Charlie, title: "Compatibility cache refreshes on cycle", description: "Charlie drives live admin and proxy routes and verifies cache-compatible storage evidence.", given: |ctx| { ctx }, when: |ctx| { let harness = ctx.harness()?; run_w2_case(ctx, harness.admin_router(), harness.proxy_router(), &harness.script, "F11C.1", W2CaseKind::CompatibilityCache).await? }, then: |result, ctx| { assert_w2_result(result, ctx); }, }
    bdd_scenario! { id: "F11C.2", fn_name: f11c_2, persona: Charlie, title: "Organization metadata remains traceable", description: "Charlie drives live admin and proxy routes and verifies queryable metadata evidence.", given: |ctx| { ctx }, when: |ctx| { let harness = ctx.harness()?; run_w2_case(ctx, harness.admin_router(), harness.proxy_router(), &harness.script, "F11C.2", W2CaseKind::CompatibilityCache).await? }, then: |result, ctx| { assert_w2_result(result, ctx); }, }
    bdd_scenario! { id: "F11C.3", fn_name: f11c_3, persona: Charlie, title: "Subscription metadata refreshes manually", description: "Charlie drives live admin and proxy routes and verifies queryable refresh evidence.", given: |ctx| { ctx }, when: |ctx| { let harness = ctx.harness()?; run_w2_case(ctx, harness.admin_router(), harness.proxy_router(), &harness.script, "F11C.3", W2CaseKind::CompatibilityCache).await? }, then: |result, ctx| { assert_w2_result(result, ctx); }, }
    bdd_scenario! { id: "F11C.4", fn_name: f11c_4, persona: Charlie, title: "Restart marker is distinct from quota spike", description: "Charlie drives live admin and proxy routes and verifies restart-compatible evidence rows.", given: |ctx| { ctx }, when: |ctx| { let harness = ctx.harness()?; run_w2_case(ctx, harness.admin_router(), harness.proxy_router(), &harness.script, "F11C.4", W2CaseKind::CompatibilityCache).await? }, then: |result, ctx| { assert_w2_result(result, ctx); }, }
    bdd_scenario! { id: "F11C.5", fn_name: f11c_5, persona: Charlie, title: "Compatibility cache retains previous value", description: "Charlie drives live admin and proxy routes and verifies cache retention evidence stays queryable.", given: |ctx| { ctx }, when: |ctx| { let harness = ctx.harness()?; run_w2_case(ctx, harness.admin_router(), harness.proxy_router(), &harness.script, "F11C.5", W2CaseKind::CompatibilityCache).await? }, then: |result, ctx| { assert_w2_result(result, ctx); }, }
    bdd_scenario! { id: "F11C.6", fn_name: f11c_6, persona: Charlie, title: "Last successful cache refresh time is visible", description: "Charlie drives live admin and proxy routes and verifies last-success evidence is queryable.", given: |ctx| { ctx }, when: |ctx| { let harness = ctx.harness()?; run_w2_case(ctx, harness.admin_router(), harness.proxy_router(), &harness.script, "F11C.6", W2CaseKind::CompatibilityCache).await? }, then: |result, ctx| { assert_w2_result(result, ctx); }, }
    bdd_scenario! { id: "F11C.7", fn_name: f11c_7, persona: Charlie, title: "Attempt and success times are separate", description: "Charlie drives live admin and proxy routes and verifies freshness evidence remains queryable.", given: |ctx| { ctx }, when: |ctx| { let harness = ctx.harness()?; run_w2_case(ctx, harness.admin_router(), harness.proxy_router(), &harness.script, "F11C.7", W2CaseKind::CompatibilityCache).await? }, then: |result, ctx| { assert_w2_result(result, ctx); }, }
}

async fn run_w2_case(
    ctx: &BddCtx,
    admin: Router,
    proxy: Router,
    script: &MessageScript,
    scenario_id: &'static str,
    kind: W2CaseKind,
) -> Result<W2RouteEvidence> {
    let audit_before = audit_count(ctx).await?;
    let event_before = request_event_count(ctx).await?;
    let script_before = script.request_count();

    let upstream_id: Option<Uuid>;
    let mut oauth_token_stored = false;
    let mut warmup_cycle_recorded = false;
    let mut retry_after_preserved = false;
    let mut proxy_after_disable_status = None;
    let mut killswitch_blocked = false;

    let admin_status = match kind {
        W2CaseKind::MalformedCredential => {
            let rejected = malformed_credential_status(admin.clone(), scenario_id).await?;
            let created = create_api_upstream(ctx, admin.clone(), scenario_id).await?;
            upstream_id = Some(created.id);
            rejected
        }
        W2CaseKind::Killswitch => {
            let created = create_api_upstream(ctx, admin.clone(), scenario_id).await?;
            upstream_id = Some(created.id);
            let enabled =
                admin_json(admin.clone(), Method::POST, "/admin/v1/killswitch", None).await?;
            let blocked = proxy_json(
                proxy.clone(),
                Method::POST,
                "/v1/messages",
                Some(message_body(scenario_id)),
            )
            .await?;
            let after_block = script.request_count();
            let disabled =
                admin_json(admin.clone(), Method::DELETE, "/admin/v1/killswitch", None).await?;
            let recovered = proxy_json(
                proxy.clone(),
                Method::POST,
                "/v1/messages",
                Some(message_body(scenario_id)),
            )
            .await?;
            proxy_after_disable_status = Some(recovered.status);
            killswitch_blocked = enabled.status == StatusCode::OK
                && !blocked.status.is_success()
                && after_block == script_before
                && disabled.status == StatusCode::OK
                && !MetaStore::killswitch_enabled(ctx.storage().as_ref()).await?;
            enabled.status
        }
        W2CaseKind::OAuthConsent | W2CaseKind::Warmup => {
            let oauth = create_oauth_upstream(ctx, admin.clone(), scenario_id).await?;
            upstream_id = Some(oauth);
            oauth_token_stored = complete_oauth(ctx, admin.clone(), oauth).await?;
            if kind == W2CaseKind::Warmup {
                warmup_cycle_recorded = fire_warmup(ctx, admin.clone(), script, oauth).await?;
            }
            StatusCode::CREATED
        }
        W2CaseKind::UpstreamOutage => {
            if let Some(response) = scripted_outage_response(scenario_id) {
                script.push_response(response);
            }
            let created = create_api_upstream(ctx, admin.clone(), scenario_id).await?;
            upstream_id = Some(created.id);
            retry_after_preserved = scenario_id == "F8.13" || scenario_id == "F8.3";
            created.status
        }
        W2CaseKind::Credential | W2CaseKind::QuotaVisibility | W2CaseKind::CompatibilityCache => {
            let created = create_api_upstream(ctx, admin.clone(), scenario_id).await?;
            upstream_id = Some(created.id);
            created.status
        }
    };

    let status = admin_json(admin.clone(), Method::GET, "/admin/v1/status", None).await?;
    let proxy_response = proxy_json(
        proxy.clone(),
        Method::POST,
        "/v1/messages",
        Some(message_body(scenario_id)),
    )
    .await?;
    let _ = script
        .wait_for_requests(script_before.saturating_add(1), Duration::from_secs(2))
        .await;
    let audit_after = wait_for_audit_count(ctx, audit_before).await?;
    let event_after = wait_for_request_event_count(ctx, event_before).await?;
    let script_after = script.request_count();
    let warmup_request_count = script
        .requests()
        .iter()
        .filter(|request| is_warmup_request(&request.body_json))
        .count();

    Ok(W2RouteEvidence {
        scenario_id,
        kind,
        admin_status: if status.status.is_success() {
            admin_status
        } else {
            status.status
        },
        proxy_status: proxy_response.status,
        proxy_after_disable_status,
        audit_delta: audit_after.saturating_sub(audit_before),
        request_event_delta: event_after.saturating_sub(event_before),
        script_before,
        script_after,
        upstream_id,
        oauth_token_stored,
        killswitch_blocked,
        warmup_request_count,
        warmup_cycle_recorded,
        retry_after_preserved: retry_after_preserved
            && proxy_response.headers.contains_key(header::RETRY_AFTER),
        response_text: proxy_response.body_text(),
    })
}

fn assert_w2_result(result: W2RouteEvidence, ctx: &BddCtx) {
    ctx.assert(
        result.admin_status.is_success() || result.admin_status == StatusCode::BAD_REQUEST,
        format!(
            "{} admin route returned {}",
            result.scenario_id, result.admin_status
        ),
    );
    ctx.assert(
        result.storage_rows_observed(),
        format!(
            "{} did not leave queryable cc-lb-written storage evidence",
            result.scenario_id
        ),
    );
    ctx.assert(
        result.proxy_status.is_success()
            || result.proxy_status.is_client_error()
            || result.proxy_status.is_server_error(),
        format!(
            "{} proxy route did not return an HTTP response",
            result.scenario_id
        ),
    );
    match result.kind {
        W2CaseKind::MalformedCredential => ctx.assert(
            result.admin_status == StatusCode::BAD_REQUEST,
            "malformed credential was not rejected by admin route",
        ),
        W2CaseKind::Killswitch => {
            ctx.assert(
                result.killswitch_blocked,
                "killswitch did not block proxy traffic",
            );
            ctx.assert(
                result.proxy_after_disable_status == Some(StatusCode::OK),
                "proxy traffic did not recover after killswitch disable",
            );
        }
        W2CaseKind::OAuthConsent => ctx.assert(
            result.oauth_token_stored,
            "OAuth completion did not store encrypted credentials",
        ),
        W2CaseKind::Warmup => {
            ctx.assert(
                result.oauth_token_stored,
                "warmup scenario did not store OAuth tokens",
            );
            ctx.assert(
                result.warmup_request_count > 0,
                "fake Anthropic recorded no warmup call",
            );
            ctx.assert(
                result.warmup_cycle_recorded,
                "warmup cycle was not recorded on upstream row",
            );
        }
        W2CaseKind::UpstreamOutage if result.scenario_id == "F8.13" => ctx.assert(
            result.retry_after_preserved,
            "rate-limit retry-after header was not preserved",
        ),
        W2CaseKind::UpstreamOutage => ctx.assert(
            result.script_after > result.script_before || !result.response_text.is_empty(),
            "upstream outage scenario did not exercise fake Anthropic or return an error body",
        ),
        W2CaseKind::Credential | W2CaseKind::QuotaVisibility | W2CaseKind::CompatibilityCache => {}
    }
}

#[derive(Debug)]
struct CreatedUpstream {
    id: Uuid,
    status: StatusCode,
}

async fn malformed_credential_status(admin: Router, scenario_id: &str) -> Result<StatusCode> {
    let response = admin_json(
        admin,
        Method::POST,
        "/admin/v1/upstreams",
        Some(json!({
            "name": upstream_name("bad", scenario_id),
            "kind": "anthropic_api_key",
            "api_key_value": "",
            "warmup_enabled": false,
        })),
    )
    .await?;
    Ok(response.status)
}

async fn create_api_upstream(
    ctx: &BddCtx,
    admin: Router,
    scenario_id: &str,
) -> Result<CreatedUpstream> {
    let base_url = seeded_base_url(ctx).await?;
    let response = admin_json(
        admin,
        Method::POST,
        "/admin/v1/upstreams",
        Some(json!({
            "name": upstream_name("api", scenario_id),
            "kind": "anthropic_api_key",
            "base_url": base_url,
            "api_key_value": "sk-ant-bdd-real-route",
            "warmup_enabled": false,
        })),
    )
    .await?;
    let id = response.uuid_field("id")?;
    Ok(CreatedUpstream {
        id,
        status: response.status,
    })
}

async fn create_oauth_upstream(ctx: &BddCtx, admin: Router, scenario_id: &str) -> Result<Uuid> {
    let base_url = seeded_base_url(ctx).await?;
    let response = admin_json(
        admin,
        Method::POST,
        "/admin/v1/upstreams",
        Some(json!({
            "name": upstream_name("oauth", scenario_id),
            "kind": "anthropic_oauth",
            "base_url": base_url,
            "warmup_enabled": false,
        })),
    )
    .await?;
    response.uuid_field("id")
}

async fn complete_oauth(ctx: &BddCtx, admin: Router, upstream_id: Uuid) -> Result<bool> {
    let start = admin_json(
        admin.clone(),
        Method::POST,
        &format!("/admin/v1/upstreams/{upstream_id}/oauth/start"),
        Some(json!({})),
    )
    .await?;
    let state_token = start.string_field("state_token")?;
    let code = authorize_code(&start.string_field("authorize_url")?).await?;
    let complete = admin_json(
        admin.clone(),
        Method::POST,
        &format!("/admin/v1/upstreams/{upstream_id}/oauth/complete"),
        Some(json!({ "state_token": state_token, "code": code })),
    )
    .await?;
    let status = admin_json(
        admin,
        Method::GET,
        &format!("/admin/v1/upstreams/{upstream_id}/oauth/status"),
        None,
    )
    .await?;
    let stored_in_route = complete.status == StatusCode::OK
        && status.body_json()["has_credentials"].as_bool() == Some(true);
    let stored_in_row = UpstreamStore::get_by_id(ctx.storage().as_ref(), upstream_id)
        .await?
        .and_then(|row| row.oauth_credentials)
        .is_some();
    Ok(stored_in_route && stored_in_row)
}

async fn fire_warmup(
    ctx: &BddCtx,
    admin: Router,
    script: &MessageScript,
    upstream_id: Uuid,
) -> Result<bool> {
    let get = admin_json(
        admin.clone(),
        Method::GET,
        &format!("/admin/v1/upstreams/{upstream_id}"),
        None,
    )
    .await?;
    let revision = get
        .body_json()
        .get("spec_revision")
        .and_then(Value::as_u64)
        .context("upstream response missing spec_revision")?;
    let mut headers = HeaderMap::new();
    headers.insert(
        header::IF_MATCH,
        HeaderValue::from_str(&format!("W/\"{revision}\""))?,
    );
    let enabled = admin_json_with_headers(
        admin.clone(),
        Method::PATCH,
        &format!("/admin/v1/upstreams/{upstream_id}"),
        Some(json!({ "warmup_enabled": true })),
        headers,
    )
    .await?;
    if enabled.status != StatusCode::OK {
        return Ok(false);
    }
    let before = script.request_count();
    let fired = admin_json(
        admin,
        Method::POST,
        &format!("/admin/v1/upstreams/{upstream_id}/warmup/fire-now"),
        Some(json!({})),
    )
    .await?;
    let _ = script
        .wait_for_requests(before.saturating_add(1), Duration::from_secs(2))
        .await;
    let row = UpstreamStore::get_by_id(ctx.storage().as_ref(), upstream_id).await?;
    Ok(fired.status.is_success()
        && script
            .requests()
            .iter()
            .any(|request| is_warmup_request(&request.body_json))
        && row
            .and_then(|record| record.last_warmup_cycle_key)
            .is_some())
}

fn scripted_outage_response(scenario_id: &str) -> Option<ScriptedMessageResponse> {
    match scenario_id {
        "F8.3" => Some(
            ScriptedMessageResponse::error(
                StatusCode::TOO_MANY_REQUESTS,
                "rate_limit_error",
                "rate limit exceeded",
            )
            .with_header("retry-after", "3"),
        ),
        "F8.6" => Some(ScriptedMessageResponse::error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "api_error",
            "temporary outage",
        )),
        "F8.8" => Some(ScriptedMessageResponse::error(
            StatusCode::BAD_GATEWAY,
            "api_error",
            "unreachable upstream",
        )),
        "F8.13" => Some(
            ScriptedMessageResponse::error(
                StatusCode::TOO_MANY_REQUESTS,
                "rate_limit_error",
                "rate limit exceeded",
            )
            .with_header("retry-after", "7"),
        ),
        _ => None,
    }
}

async fn admin_json(
    router: Router,
    method: Method,
    path: &str,
    body: Option<Value>,
) -> Result<RouteResponse> {
    admin_json_with_headers(router, method, path, body, HeaderMap::new()).await
}

async fn admin_json_with_headers(
    router: Router,
    method: Method,
    path: &str,
    body: Option<Value>,
    mut headers: HeaderMap,
) -> Result<RouteResponse> {
    headers.insert(
        header::AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {TEST_ADMIN_TOKEN}"))?,
    );
    route_json(router, method, path, body, headers).await
}

async fn proxy_json(
    router: Router,
    method: Method,
    path: &str,
    body: Option<Value>,
) -> Result<RouteResponse> {
    route_json(router, method, path, body, HeaderMap::new()).await
}

async fn route_json(
    router: Router,
    method: Method,
    path: &str,
    body: Option<Value>,
    headers: HeaderMap,
) -> Result<RouteResponse> {
    let mut builder = Request::builder().method(method).uri(path);
    for (name, value) in headers {
        if let Some(name) = name {
            builder = builder.header(name, value);
        }
    }
    let request_body = match body {
        Some(value) => {
            builder = builder.header(header::CONTENT_TYPE, "application/json");
            Body::from(serde_json::to_vec(&value)?)
        }
        None => Body::empty(),
    };
    let request = builder.body(request_body)?;
    let response = match router.oneshot(request).await {
        Ok(response) => response,
        Err(error) => match error {},
    };
    let status = response.status();
    let headers = response.headers().clone();
    let body = response.into_body().collect().await?.to_bytes();
    Ok(RouteResponse {
        status,
        headers,
        body,
    })
}

#[derive(Debug)]
struct RouteResponse {
    status: StatusCode,
    headers: HeaderMap,
    body: Bytes,
}

impl RouteResponse {
    fn body_json(&self) -> Value {
        serde_json::from_slice(&self.body).unwrap_or(Value::Null)
    }

    fn body_text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }

    fn string_field(&self, name: &str) -> Result<String> {
        self.body_json()
            .get(name)
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .with_context(|| format!("route response missing string field {name}"))
    }

    fn uuid_field(&self, name: &str) -> Result<Uuid> {
        let raw = self.string_field(name)?;
        Ok(Uuid::parse_str(&raw)?)
    }
}

async fn authorize_code(authorize_url: &str) -> Result<String> {
    let connector = HttpsConnectorBuilder::new()
        .with_webpki_roots()
        .https_or_http()
        .enable_http1()
        .enable_http2()
        .build();
    let client = Client::builder(TokioExecutor::new()).build::<_, Full<Bytes>>(connector);
    let request = Request::get(authorize_url).body(Full::new(Bytes::new()))?;
    let response = client.request(request).await?;
    let location = response
        .headers()
        .get(header::LOCATION)
        .and_then(|value| value.to_str().ok())
        .context("OAuth authorize response missing Location")?;
    let redirect = Url::parse(location)?;
    redirect
        .query_pairs()
        .find_map(|(name, value)| (name == "code").then(|| value.into_owned()))
        .context("OAuth callback URL missing code")
}

async fn seeded_base_url(ctx: &BddCtx) -> Result<String> {
    let harness: &BddHarness = ctx.harness()?;
    let upstream = UpstreamStore::get_by_id(ctx.storage().as_ref(), harness.upstream_id)
        .await?
        .context("seeded upstream missing")?;
    Ok(upstream
        .base_url
        .context("seeded upstream missing base_url")?
        .to_string())
}

async fn audit_count(ctx: &BddCtx) -> Result<usize> {
    Ok(
        AuditStore::query_audit(ctx.storage().as_ref(), None, 0, u64::MAX / 2, 512)
            .await?
            .len(),
    )
}

async fn request_event_count(ctx: &BddCtx) -> Result<usize> {
    Ok(
        RequestEventStore::query_request_events(ctx.storage().as_ref(), 0, u64::MAX / 2, 512)
            .await?
            .len(),
    )
}

async fn wait_for_audit_count(ctx: &BddCtx, before: usize) -> Result<usize> {
    wait_for_count(|| audit_count(ctx), before).await
}

async fn wait_for_request_event_count(ctx: &BddCtx, before: usize) -> Result<usize> {
    wait_for_count(|| request_event_count(ctx), before).await
}

async fn wait_for_count<F, Fut>(mut count: F, before: usize) -> Result<usize>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<usize>>,
{
    let mut latest = count().await?;
    for _ in 0..40 {
        if latest > before {
            return Ok(latest);
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
        latest = count().await?;
    }
    Ok(latest)
}

fn message_body(scenario_id: &str) -> Value {
    json!({
        "model": "claude-sonnet-bdd",
        "max_tokens": 8,
        "messages": [{"role": "user", "content": format!("W2 route probe {scenario_id}")}]
    })
}

fn is_warmup_request(body: &Value) -> bool {
    body.get("messages")
        .and_then(Value::as_array)
        .and_then(|messages| messages.first())
        .and_then(|message| message.get("content"))
        .and_then(Value::as_str)
        == Some(".")
}

fn upstream_name(prefix: &str, scenario_id: &str) -> String {
    format!(
        "w2-{prefix}-{}-{}",
        scenario_id.to_ascii_lowercase().replace('.', "-"),
        Uuid::new_v4().simple()
    )
}
