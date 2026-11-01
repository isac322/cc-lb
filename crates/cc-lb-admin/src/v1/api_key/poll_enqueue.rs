//! Immediate first usage poll for an API-key upstream (plan §1.9, R2-4).
//!
//! Create, key replace, and a Re-check that turns usage reporting `ok` each
//! push one `api_key_usage_poll` cron task for the upstream, so reported
//! figures do not wait for the next regular tick. Without a scheduler the
//! regular tick picks the upstream up.

use cc_lb_scheduler::error::SchedulerError;
use cc_lb_scheduler::jobs::api_key_usage_poll::{
    ApiKeyUsagePollCronJob, immediate_poll_idempotency_key,
};
use cc_lb_scheduler::worker::{CronJob, SchedulerPushTask};
use uuid::Uuid;

use crate::AdminState;

/// Enqueues one immediate usage poll. Best effort: the admin write already
/// succeeded, a duplicate key is a no-op, and any other failure is logged
/// with the upstream id only.
pub(crate) async fn enqueue_usage_poll(
    state: &AdminState,
    upstream_id: Uuid,
    spec_revision: u64,
    checked_at_unix_secs: u64,
) {
    let Some(scheduler) = state.scheduler.as_ref() else {
        return;
    };
    let tick_unix_secs = cc_lb_clock::unix_secs(state.clock.now());
    let task = SchedulerPushTask {
        args: CronJob::ApiKeyUsagePoll(ApiKeyUsagePollCronJob::for_upstream(
            tick_unix_secs,
            upstream_id,
        )),
        idempotency_key: Some(immediate_poll_idempotency_key(
            upstream_id,
            spec_revision,
            checked_at_unix_secs,
        )),
        run_at_unix_secs: None,
        max_attempts: Some(1),
    };
    match scheduler.push_cron_task(task).await {
        Ok(()) | Err(SchedulerError::Conflict(_)) => {}
        Err(error) => {
            tracing::warn!(%upstream_id, %error, "api-key usage poll enqueue failed");
        }
    }
}

#[cfg(all(test, feature = "sqlite"))]
mod tests {
    use std::collections::HashMap;
    use std::str::FromStr as _;
    use std::sync::Arc;

    use cc_lb_aead::AeadService;
    use cc_lb_clock::{ClockHandle, TestClock};
    use cc_lb_config::Config;
    use cc_lb_control::DynamicViewHolder;
    use cc_lb_control::api_keys::concurrent_guard::KeyConcurrencyManager;
    use cc_lb_control::api_keys::limit_engine::LimitEngine;
    use cc_lb_control::api_keys::principal_view::PrincipalView;
    use cc_lb_control::{DynamicViewBuilder, anthropic_api_key::AnthropicOrgApi};
    use cc_lb_scheduler::admin::SchedulerAdminHandle;
    use cc_lb_scheduler::worker::{CRON_QUEUE, SchedulerBackend, SqliteSchedulerBackend};
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use tempfile::TempDir;

    use super::*;

    const NOW: u64 = 1_800_000_000;

    struct NoopSignerFactory;

    impl cc_lb_upstream::ApiKeyAwareSignerFactory for NoopSignerFactory {
        fn with_router_choice(
            &self,
            _router_chosen_upstream_name: String,
        ) -> Arc<dyn cc_lb_upstream::SignerFactory> {
            Arc::new(NoopSignerFactory)
        }
    }

    #[async_trait::async_trait]
    impl cc_lb_upstream::SignerFactory for NoopSignerFactory {
        async fn build(
            &self,
            _upstream: &cc_lb_domain::Upstream,
        ) -> Result<Arc<dyn cc_lb_upstream::Signer>, cc_lb_upstream::SignerError> {
            Err(cc_lb_upstream::SignerError::MissingCredentials {
                reason: "noop test signer factory".to_owned(),
            })
        }
    }

    struct Harness {
        _dir: TempDir,
        pool: sqlx::SqlitePool,
        state: AdminState,
    }

    async fn harness(with_scheduler: bool) -> Harness {
        let dir = tempfile::tempdir().expect("tempdir");
        let url = format!("sqlite://{}", dir.path().join("scheduler.sqlite").display());
        let options = SqliteConnectOptions::from_str(&url)
            .expect("sqlite url parses")
            .create_if_missing(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .expect("sqlite opens");
        apalis_sqlite::SqliteStorage::setup(&pool)
            .await
            .expect("apalis setup");
        cc_lb_scheduler::migrations::apply_post_setup_migrations(&pool)
            .await
            .expect("scheduler migrations");
        let clock: ClockHandle = Arc::new(TestClock::new_at_secs(NOW));
        let backend =
            SchedulerBackend::Sqlite(SqliteSchedulerBackend::new(pool.clone(), clock.clone()));
        let view = DynamicViewBuilder::new(0)
            .signer_factory(Arc::new(NoopSignerFactory))
            .principal_view(Arc::new(PrincipalView::from_db(&[], HashMap::new())))
            .build();
        let state = AdminState {
            config_path: None,
            startup_config_overrides: Default::default(),
            storage: None,
            key_store: None,
            aead: Arc::new(AeadService::from_master_key([0; 32])),
            limit_engine: LimitEngine::new(Arc::new(KeyConcurrencyManager::new()), clock.clone()),
            lifecycle: None,
            lazy_refresher: None,
            runtime: None,
            data_dir: None,
            warmup_dialect_dispatcher: None,
            dynamic_view: Arc::new(DynamicViewHolder::new(view)),
            config: Arc::new(Config::default()),
            dynamic_view_rebinder: None,
            scheduler: with_scheduler.then(|| SchedulerAdminHandle::new(backend)),
            admin_auth: Arc::new(crate::auth::AdminAuthenticator::new(Vec::new())),
            start_time: std::time::Instant::now(),
            event_bus: None,
            storage_tail: crate::events::storage_tail_channel(),
            clock,
            anthropic_org_api: AnthropicOrgApi::production()
                .expect("production Anthropic origin parses"),
        };
        Harness {
            _dir: dir,
            pool,
            state,
        }
    }

    async fn poll_jobs(pool: &sqlx::SqlitePool) -> Vec<(String, Vec<u8>)> {
        sqlx::query_as(
            "SELECT idempotency_key, job FROM Jobs WHERE job_type = ?1 ORDER BY idempotency_key",
        )
        .bind(CRON_QUEUE)
        .fetch_all(pool)
        .await
        .expect("list cron jobs")
    }

    /// POL-20: one immediate job per (upstream, revision, checked_at); a
    /// Re-check at the same revision with a new `checked_at` is not dropped.
    #[tokio::test]
    async fn pol20_enqueues_one_immediate_poll_per_check() {
        let harness = harness(true).await;
        let upstream_id = Uuid::from_u128(42);

        enqueue_usage_poll(&harness.state, upstream_id, 3, NOW - 10).await;
        enqueue_usage_poll(&harness.state, upstream_id, 3, NOW - 10).await;
        enqueue_usage_poll(&harness.state, upstream_id, 3, NOW).await;

        let jobs = poll_jobs(&harness.pool).await;
        let keys: Vec<&str> = jobs.iter().map(|(key, _)| key.as_str()).collect();
        assert_eq!(
            keys,
            vec![
                format!("api_key_usage_poll:{upstream_id}:3:{}", NOW - 10).as_str(),
                format!("api_key_usage_poll:{upstream_id}:3:{NOW}").as_str(),
            ]
        );
        let payload: serde_json::Value =
            serde_json::from_slice(&jobs[0].1).expect("job payload is json");
        assert_eq!(payload["type"], "api_key_usage_poll");
        assert_eq!(payload["payload"]["upstream_id"], upstream_id.to_string());
        assert_eq!(payload["payload"]["tick_unix_secs"], NOW);
    }

    /// POL-20: without a scheduler nothing is enqueued and nothing fails;
    /// the regular tick picks the upstream up.
    #[tokio::test]
    async fn pol20_without_scheduler_is_a_no_op() {
        let harness = harness(false).await;

        enqueue_usage_poll(&harness.state, Uuid::from_u128(7), 1, NOW).await;

        assert!(poll_jobs(&harness.pool).await.is_empty());
    }
}
