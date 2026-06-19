mod admin_test_common;
#[path = "scheduler_admin/support.rs"]
mod support;

use axum::http::StatusCode;
#[cfg(feature = "sqlite")]
use cc_lb_scheduler::jobs::reconcile::ReconcileUpstreams;
use cc_lb_scheduler::jobs::reconcile::SchedulerReconcileJob;
#[cfg(feature = "sqlite")]
use std::future::Future;

const TRACEPARENT: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn scheduler_admin_sqlite_status_failures_and_reconcile()
-> Result<(), Box<dyn std::error::Error>> {
    use cc_lb_scheduler::idempotency::SchedulerFailuresStore;

    let fixture = support::sqlite_fixture().await?;
    support::seed_sqlite_usage_rollup(&fixture.pool).await?;

    SchedulerFailuresStore::new(fixture.pool.clone())
        .record(
            "upstream_warmup",
            "entity:warmup:test",
            "boom",
            5,
            support::NOW_SECS,
        )
        .await?;
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
        "/admin/scheduler/failures?job_type=upstream_warmup",
        &[],
    )
    .await?;
    assert_eq!(failures_status, StatusCode::OK);
    assert_eq!(failures_body["failures"][0]["job_type"], "upstream_warmup");
    assert_eq!(
        failures_body["failures"][0]["payload_summary"],
        "entity:warmup:test"
    );

    let (reconcile_status, reconcile_body) = support::authed_json(
        app,
        "POST",
        "/admin/scheduler/reconcile",
        &[("traceparent", TRACEPARENT)],
    )
    .await?;
    assert_eq!(reconcile_status, StatusCode::ACCEPTED);
    let job_id = reconcile_body["job_id"].as_str().expect("job id");
    let payload: Vec<u8> = scheduler_sqlx::query_scalar("SELECT job FROM Jobs WHERE id = ?1")
        .bind(job_id)
        .fetch_one(&fixture.pool)
        .await?;
    let job: SchedulerReconcileJob = serde_json::from_slice(&payload)?;
    assert_eq!(job.traceparent.as_deref(), Some(TRACEPARENT));
    Ok(())
}

#[cfg(feature = "sqlite")]
#[test]
fn scheduler_admin_sqlite_dlq_feedback_surfaces_warmup_ticket_metric_and_admin_row()
-> Result<(), Box<dyn std::error::Error>> {
    use cc_lb_scheduler::jobs::reconcile::SchedulerReconcileJobHandler;
    use metrics_exporter_prometheus::PrometheusBuilder;

    let recorder = PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    metrics::with_local_recorder(&recorder, || {
        runtime.block_on(async {
            let fixture = support::sqlite_fixture().await?;
            support::seed_sqlite_failed_warmup(&fixture.pool).await?;
            let handler = SchedulerReconcileJobHandler::new(fixture.pool.clone(), EmptyUpstreams);
            let before = support::counter_value(&handle.render(), "upstream_warmup");

            let _result = handler
                .handle(SchedulerReconcileJob::default(), support::NOW_SECS)
                .await;

            let rendered = handle.render();
            assert_eq!(
                support::counter_value(&rendered, "upstream_warmup") - before,
                1.0
            );
            let count: i64 = scheduler_sqlx::query_scalar(
                "SELECT COUNT(*) FROM scheduler_failures WHERE job_type = 'upstream_warmup'",
            )
            .fetch_one(&fixture.pool)
            .await?;
            assert!(count >= 1);
            let (status, body) = support::authed_json(
                support::app_with_scheduler(fixture.handle),
                "GET",
                "/admin/scheduler/failures?job_type=upstream_warmup",
                &[],
            )
            .await?;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(body["failures"][0]["job_type"], "upstream_warmup");
            Ok::<(), Box<dyn std::error::Error>>(())
        })
    })?;
    Ok(())
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn scheduler_admin_postgres_status_failures_and_reconcile()
-> Result<(), Box<dyn std::error::Error>> {
    let Some((admin, db_name, fixture)) = support::postgres_fixture().await? else {
        return Ok(());
    };
    let result = async {
        cc_lb_scheduler::idempotency::SchedulerFailuresStore::new(fixture.pool.clone())
            .record(
                "upstream_warmup",
                "entity:warmup:pg",
                "boom",
                5,
                support::NOW_SECS,
            )
            .await?;
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
            "/admin/scheduler/failures?job_type=upstream_warmup",
            &[],
        )
        .await?;
        assert_eq!(failures_status, StatusCode::OK);
        assert_eq!(failures_body["failures"][0]["job_type"], "upstream_warmup");
        let (reconcile_status, reconcile_body) = support::authed_json(
            app,
            "POST",
            "/admin/scheduler/reconcile",
            &[("traceparent", TRACEPARENT)],
        )
        .await?;
        assert_eq!(reconcile_status, StatusCode::ACCEPTED);
        let job_id = reconcile_body["job_id"].as_str().expect("job id");
        let payload: Vec<u8> =
            scheduler_sqlx::query_scalar("SELECT job FROM apalis.jobs WHERE id = $1")
                .bind(job_id)
                .fetch_one(&fixture.pool)
                .await?;
        let job: SchedulerReconcileJob = serde_json::from_slice(&payload)?;
        assert_eq!(job.traceparent.as_deref(), Some(TRACEPARENT));
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

#[derive(Clone, Copy)]
#[cfg(feature = "sqlite")]
struct EmptyUpstreams;

#[cfg(feature = "sqlite")]
impl ReconcileUpstreams for EmptyUpstreams {
    fn list(
        &self,
        _after: Option<uuid::Uuid>,
        _limit: usize,
    ) -> impl Future<Output = cc_lb_scheduler::error::Result<Vec<cc_lb_storage_api::UpstreamRecord>>>
    + Send
    + '_ {
        async { Ok(Vec::new()) }
    }
}
