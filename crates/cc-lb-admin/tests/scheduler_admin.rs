#![allow(clippy::manual_async_fn)]

mod admin_test_common;
#[path = "scheduler_admin/support.rs"]
mod support;

use axum::http::StatusCode;

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn scheduler_admin_sqlite_status_and_failures() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = support::sqlite_fixture().await?;
    support::seed_sqlite_usage_rollup(&fixture.pool).await?;
    support::seed_sqlite_failed_warmup(&fixture.pool).await?;
    let app = support::app_with_scheduler(fixture.handle.clone());

    let (status, status_body) =
        support::authed_json(app.clone(), "GET", "/admin/scheduler/status", &[]).await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(status_body["leader_status"], "single");
    assert_eq!(
        status_body["schedule_version"].as_str().unwrap_or(""),
        "unknown"
    );
    let recurring = status_body["recurring_jobs"]
        .as_array()
        .expect("recurring array");
    let names = recurring
        .iter()
        .filter_map(|job| job["name"].as_str())
        .collect::<Vec<_>>();
    assert!(names.contains(&"usage_rollup"));
    let rollup = recurring
        .iter()
        .find(|job| job["name"] == "usage_rollup")
        .expect("usage_rollup status");
    assert_eq!(rollup["last_run_status"], "Pending");
    assert!(rollup["next_run_at"].is_u64());

    let (failures_status, failures_body) = support::authed_json(
        app.clone(),
        "GET",
        "/admin/scheduler/failures?job_type=entity:warmup",
        &[],
    )
    .await?;
    assert_eq!(failures_status, StatusCode::OK);
    assert_eq!(failures_body["failures"][0]["job_type"], "entity:warmup");
    assert_eq!(
        failures_body["failures"][0]["payload_summary"],
        "entity:warmup:test"
    );

    let response = support::request(app, "POST", "/admin/scheduler/reconcile", &[]).await?;
    assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
    Ok(())
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn scheduler_admin_postgres_status_and_failures() -> Result<(), Box<dyn std::error::Error>> {
    let Some((admin, db_name, fixture)) = support::postgres_fixture().await? else {
        return Ok(());
    };
    let result = async {
        support::seed_postgres_failed_warmup(&fixture.pool).await?;
        let app = support::app_with_scheduler(fixture.handle.clone());
        let (status, body) =
            support::authed_json(app.clone(), "GET", "/admin/scheduler/status", &[]).await?;
        assert_eq!(status, StatusCode::OK);
        assert!(matches!(
            body["leader_status"].as_str(),
            Some("single" | "follower" | "leader")
        ));
        let (failures_status, failures_body) = support::authed_json(
            app.clone(),
            "GET",
            "/admin/scheduler/failures?job_type=entity:warmup",
            &[],
        )
        .await?;
        assert_eq!(failures_status, StatusCode::OK);
        assert_eq!(failures_body["failures"][0]["job_type"], "entity:warmup");
        let response = support::request(app, "POST", "/admin/scheduler/reconcile", &[]).await?;
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;
    fixture.pool.close().await;
    scheduler_sqlx::query(&format!(r#"DROP DATABASE IF EXISTS "{db_name}""#))
        .execute(&admin)
        .await?;
    admin.close().await;
    result
}
