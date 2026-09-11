#![allow(clippy::manual_async_fn)]

#[path = "scheduler_admin/support.rs"]
mod support;

use axum::http::StatusCode;

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn t3__scheduler_admin_sqlite_status_and_failures() -> Result<(), Box<dyn std::error::Error>>
{
    let fixture = support::sqlite_fixture().await?;
    support::seed_sqlite_usage_rollup(&fixture.pool).await?;
    support::seed_sqlite_failed_warmup(&fixture.pool).await?;
    let app = support::app_with_scheduler(fixture.handle.clone());

    let (status, status_body) =
        support::authed_json(app.clone(), "GET", "/admin/scheduler/status", &[]).await?;
    assert_eq!(status, StatusCode::OK);
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
        "/admin/scheduler/failures?job_type=adaptive",
        &[],
    )
    .await?;
    assert_eq!(failures_status, StatusCode::OK);
    assert_eq!(failures_body["failures"][0]["job_type"], "adaptive");
    assert_eq!(
        failures_body["failures"][0]["payload_summary"],
        "adaptive:warmup:test"
    );

    let response = support::request(app, "POST", "/admin/scheduler/reconcile", &[]).await?;
    assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
    Ok(())
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn t3_postgres__scheduler_admin_status_and_failures() -> Result<(), Box<dyn std::error::Error>>
{
    let fixture = support::postgres_fixture().await?;
    let result: Result<(), Box<dyn std::error::Error>> = async {
        support::seed_postgres_failed_warmup(&fixture.pool).await?;
        let app = support::app_with_scheduler(fixture.handle.clone());
        let (status, _body) =
            support::authed_json(app.clone(), "GET", "/admin/scheduler/status", &[]).await?;
        if status != StatusCode::OK {
            return Err(format!("scheduler status endpoint returned {status}").into());
        }
        let (failures_status, failures_body) = support::authed_json(
            app.clone(),
            "GET",
            "/admin/scheduler/failures?job_type=adaptive",
            &[],
        )
        .await?;
        if failures_status != StatusCode::OK {
            return Err(format!("scheduler failures endpoint returned {failures_status}").into());
        }
        if failures_body["failures"][0]["job_type"] != "adaptive" {
            return Err(format!(
                "expected adaptive failure, got {}",
                failures_body["failures"][0]["job_type"]
            )
            .into());
        }
        let response = support::request(app, "POST", "/admin/scheduler/reconcile", &[]).await?;
        if response.status() != StatusCode::METHOD_NOT_ALLOWED {
            return Err(format!(
                "scheduler reconcile endpoint returned {}",
                response.status()
            )
            .into());
        }
        Ok(())
    }
    .await;
    let teardown = fixture.teardown().await;
    result?;
    teardown?;
    Ok(())
}
