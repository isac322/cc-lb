use anyhow::Result;
use axum::http::{Method, StatusCode};
use cc_lb_bdd_tests::{Alice, Charlie, HttpResponse};

use super::w1_admin_key::w1_admin_observe;

pub(crate) struct W1DashboardObservation {
    summary: HttpResponse,
    usage: HttpResponse,
}

pub(crate) struct W1HealthObservation {
    liveness: HttpResponse,
    readiness: HttpResponse,
}

pub(crate) async fn w1_dashboard_observe(
    alice: &Alice<'_>,
    marker: &str,
) -> Result<W1DashboardObservation> {
    let _ = w1_admin_observe(alice, marker).await?;
    let summary = alice
        .admin_request(Method::GET, "/admin/v1/dashboard/summary?range=1h", None)
        .await;
    let usage = alice
        .admin_request(
            Method::GET,
            "/admin/v1/dashboard/usage?range=1h&group_by=model",
            None,
        )
        .await;
    Ok(W1DashboardObservation { summary, usage })
}

pub(crate) async fn w1_health_observe(charlie: &Charlie<'_>) -> Result<W1HealthObservation> {
    let liveness = charlie.proxy_request(Method::GET, "/healthz", None).await;
    let readiness = charlie.proxy_request(Method::GET, "/readyz", None).await;
    Ok(W1HealthObservation {
        liveness,
        readiness,
    })
}

pub(crate) fn assert_dashboard_result(
    result: W1DashboardObservation,
    ctx: &cc_lb_bdd_tests::BddCtx,
) {
    ctx.assert(
        result.summary.status == StatusCode::OK,
        format!(
            "expected dashboard summary to return 200, got {}",
            result.summary.status
        ),
    );
    ctx.assert(
        result.usage.status == StatusCode::OK,
        format!(
            "expected dashboard usage to return 200, got {}",
            result.usage.status
        ),
    );
    ctx.assert(
        result.summary.body_json().is_object(),
        "dashboard summary should return an object",
    );
    ctx.assert(
        result.usage.body_json().is_object(),
        "dashboard usage should return an object",
    );
}

pub(crate) fn assert_health_result(result: W1HealthObservation, ctx: &cc_lb_bdd_tests::BddCtx) {
    ctx.assert(
        result.liveness.status == StatusCode::OK,
        format!(
            "expected liveness to return 200, got {}",
            result.liveness.status
        ),
    );
    ctx.assert(
        result.readiness.status == StatusCode::OK
            || result.readiness.status == StatusCode::SERVICE_UNAVAILABLE,
        format!(
            "expected readiness to be explicit, got {}",
            result.readiness.status
        ),
    );
    ctx.assert(
        result.liveness.body_json()["status"].as_str().is_some(),
        "liveness body should include status",
    );
    ctx.assert(
        result.readiness.body_json()["ready"].as_bool().is_some(),
        "readiness body should include readiness",
    );
}
