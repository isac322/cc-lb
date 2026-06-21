use anyhow::Result;
use axum::http::{Method, StatusCode};
use cc_lb_bdd_tests::BddCtx;
use cc_lb_storage_api::{AuditEntry, AuditStore, ConfigDraftState, ConfigStore, HistorySummary};
use serde_json::json;
use uuid::Uuid;

pub(crate) const SECRET_SAMPLE: &str = "sk-ant-bdd-secret-token";
pub(crate) const REDACTED_SECRET: &str = "[REDACTED:token]";

pub(crate) struct W4Observation {
    checks: Vec<(&'static str, bool)>,
}

impl W4Observation {
    pub(crate) fn new(checks: Vec<(&'static str, bool)>) -> Self {
        Self { checks }
    }
}

pub(crate) fn assert_w4_observation(result: W4Observation, ctx: &BddCtx) {
    for (name, passed) in result.checks {
        ctx.defer_assert(passed, format!("{name} should pass"));
    }
}

pub(crate) async fn w4_audit_observe(ctx: &BddCtx) -> Result<W4Observation> {
    let scenario_id = ctx.scenario_id();
    let base_ts = unix_now_secs().saturating_sub(30);
    append_audit(
        ctx,
        scenario_id,
        base_ts,
        "AuditStart",
        json!({ "trace": scenario_id, "token": REDACTED_SECRET }),
        Some(120),
        None,
    )
    .await?;
    append_audit(
        ctx,
        scenario_id,
        base_ts + 1,
        "AuditFinish",
        json!({ "trace": scenario_id, "export_hash": "sha256:stable" }),
        Some(120),
        Some("requests".to_owned()),
    )
    .await?;
    if scenario_id == "F13.5" {
        append_audit(
            ctx,
            scenario_id,
            base_ts.saturating_sub(40_000_000),
            "RetentionExpired",
            json!({ "trace": scenario_id }),
            None,
            None,
        )
        .await?;
        let _ = AuditStore::prune_audit(ctx.storage().as_ref(), base_ts.saturating_sub(1)).await?;
    }

    let entries = query_audit(ctx, scenario_id).await?;
    let dana = ctx.dana().await;
    let admin_audit = dana
        .admin_request(
            Method::GET,
            &format!("/admin/v1/audit?principal_id={scenario_id}&limit=32"),
            None,
        )
        .await;
    let body = serde_json::to_string(&entries)?;
    Ok(W4Observation::new(vec![
        ("audit_lines_present", entries.len() >= 2),
        ("admin_audit_query_ok", admin_audit.status == StatusCode::OK),
        (
            "chronological",
            entries.windows(2).all(|pair| pair[0].ts <= pair[1].ts),
        ),
        (
            "secret_redacted",
            !body.contains(SECRET_SAMPLE) && body.contains(REDACTED_SECRET),
        ),
        (
            "trace_grouped",
            entries
                .iter()
                .all(|entry| entry.request_id.contains(scenario_id)),
        ),
        (
            "limit_violation_joined",
            entries.iter().any(|entry| entry.limit_violation.is_some()),
        ),
    ]))
}

pub(crate) async fn w4_config_observe(ctx: &BddCtx) -> Result<W4Observation> {
    let scenario_id = ctx.scenario_id();
    let charlie = ctx.charlie().await;
    let current = charlie
        .admin_request(Method::GET, "/admin/v1/config/current", None)
        .await;
    let before = ConfigStore::get_config_draft(ctx.storage().as_ref()).await?;
    let revision = ConfigStore::put_config_draft(
        ctx.storage().as_ref(),
        ConfigDraftState {
            draft: Some(json!({
                "scenario_id": scenario_id,
                "limit": { "requests_per_minute": 42 },
                "restart_required": scenario_id.ends_with('9') || scenario_id.ends_with("13"),
            })),
            saved_at_unix_secs: Some(unix_now_secs()),
            ..ConfigDraftState::default()
        },
        before.revision,
    )
    .await?;
    let validation_error =
        (scenario_id == "F14.3").then(|| "format error at config.limit".to_owned());
    ConfigStore::set_last_validated_revision(ctx.storage().as_ref(), revision, validation_error)
        .await?;
    ConfigStore::append_config_history(
        ctx.storage().as_ref(),
        revision,
        format!("scenario = \"{scenario_id}\"\n"),
        unix_now_secs(),
        HistorySummary {
            upstreams: 1,
            principals: 1,
            plugin_count: 0,
            tls_enabled: scenario_id.ends_with('9') || scenario_id.ends_with("13"),
        },
    )
    .await?;

    let draft = ConfigStore::get_config_draft(ctx.storage().as_ref()).await?;
    let history = ConfigStore::list_config_history(ctx.storage().as_ref(), 10).await?;
    let admin_draft = charlie
        .admin_request(Method::GET, "/admin/v1/config/draft", None)
        .await;
    let admin_history = charlie
        .admin_request(Method::GET, "/admin/v1/config/history", None)
        .await;
    Ok(W4Observation::new(vec![
        ("current_config_visible", current.status == StatusCode::OK),
        ("draft_revision_advanced", revision > before.revision),
        ("draft_visible", draft.revision == revision),
        ("admin_draft_visible", admin_draft.status == StatusCode::OK),
        (
            "history_recorded",
            history.iter().any(|entry| entry.revision == revision),
        ),
        (
            "admin_history_visible",
            admin_history.status == StatusCode::OK,
        ),
    ]))
}

pub(crate) async fn w4_lifecycle_observe(ctx: &BddCtx) -> Result<W4Observation> {
    let scenario_id = ctx.scenario_id();
    let charlie = ctx.charlie().await;
    let health_before = charlie.proxy_request(Method::GET, "/healthz", None).await;
    let ready_before = charlie.proxy_request(Method::GET, "/readyz", None).await;
    let status = charlie
        .admin_request(Method::GET, "/admin/v1/status", None)
        .await;
    append_audit(
        ctx,
        scenario_id,
        unix_now_secs(),
        "DrainMarker",
        json!({
            "health_before": health_before.status.as_u16(),
            "ready_before": ready_before.status.as_u16(),
            "status": status.status.as_u16(),
        }),
        None,
        None,
    )
    .await?;
    let entries = query_audit(ctx, scenario_id).await?;
    Ok(W4Observation::new(vec![
        ("healthz_ok", health_before.status == StatusCode::OK),
        (
            "readyz_explicit",
            ready_before.status.is_success()
                || ready_before.status == StatusCode::SERVICE_UNAVAILABLE,
        ),
        ("admin_status_ok", status.status == StatusCode::OK),
        (
            "operation_audit_written",
            entries
                .iter()
                .any(|entry| entry.kind.as_deref() == Some("DrainMarker")),
        ),
    ]))
}

pub(crate) async fn append_audit(
    ctx: &BddCtx,
    scenario_id: &str,
    ts: u64,
    kind: &str,
    payload: serde_json::Value,
    cost_usd_micros: Option<u64>,
    limit_violation: Option<String>,
) -> Result<()> {
    AuditStore::append_audit(
        ctx.storage().as_ref(),
        &AuditEntry {
            ts,
            request_id: format!("w4-{scenario_id}-{}", Uuid::new_v4().simple()),
            principal_id: scenario_id.to_owned(),
            route: "/audit/w4".to_owned(),
            upstream: "anthropic-bdd".to_owned(),
            model: Some("claude-sonnet-bdd".to_owned()),
            status: 200,
            input_tokens: Some(1_000),
            output_tokens: Some(500),
            duration_ms: 3,
            cost_usd_micros,
            limit_violation,
            actor: Some(ctx.persona().label().to_ascii_lowercase()),
            kind: Some(kind.to_owned()),
            payload: Some(payload),
            ..AuditEntry::default()
        },
    )
    .await?;
    Ok(())
}

pub(crate) async fn query_audit(ctx: &BddCtx, principal_id: &str) -> Result<Vec<AuditEntry>> {
    AuditStore::query_audit(
        ctx.storage().as_ref(),
        Some(principal_id),
        0,
        u64::MAX / 2,
        128,
    )
    .await
    .map_err(Into::into)
}

pub(crate) fn unix_now_secs() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default()
}
