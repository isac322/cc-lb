use super::*;

#[path = "scheduler_dispatch_tests/support.rs"]
mod support;

use ::http::StatusCode;
use async_trait::async_trait;
use cc_lb_clock::SystemClock;
use cc_lb_control::RequestEventBus;
use cc_lb_control::api_keys::concurrent_guard::KeyConcurrencyManager;
use cc_lb_control::api_keys::key_store::{CreateParams, KeyStore};
use cc_lb_control::api_keys::limit_engine::LimitEngine;
use cc_lb_engine::attempt_rail::AttemptIntent;
use cc_lb_engine::cache_keepalive::{
    CacheKeepaliveEnqueuer, DispatchOutcome, KeepaliveDispatchContext, KeepaliveDispatcher,
    RenewalFinalization, RenewalUsage, RequestSnapshot,
};
use cc_lb_lifecycle::LifecycleEvent;
use cc_lb_pricing::{CatalogSnapshot, CatalogStatus, Pricing, UsdPerMillion, global_catalog};
use cc_lb_scheduler::jobs::upstream_affinity_purge::UpstreamAffinityPurgeJob;
use cc_lb_scheduler::retry::JobOutcome;
use cc_lb_scheduler::worker::CronJob;
use cc_lb_storage_api::upstream::UpstreamKind;
use cc_lb_storage_api::{
    CacheKeepaliveEnqueueState, CacheKeepaliveSessionStatus, CacheKeepaliveSessionStore,
    CacheKeepaliveTerminalReason, Limit, LimitKind, RequestEventStore,
};
use serde_json::Value;
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::layer::{Context as LayerContext, Layer, SubscriberExt as _};

use crate::cache_keepalive_enqueuer::CacheKeepaliveTaskPusher;

use self::support::{FailingPusher, Fixture};

#[derive(Clone, Default)]
struct CacheKeepaliveWarnCapture {
    events: Arc<Mutex<Vec<String>>>,
}

impl<S> Layer<S> for CacheKeepaliveWarnCapture
where
    S: Subscriber,
{
    fn on_event(&self, event: &Event<'_>, _ctx: LayerContext<'_, S>) {
        if event.metadata().level() != &Level::WARN
            || event.metadata().target() != "cache_keepalive"
        {
            return;
        }
        let mut visitor = CacheKeepaliveEventVisitor::default();
        event.record(&mut visitor);
        let fields = visitor
            .fields
            .into_iter()
            .map(|(name, value)| format!("{name}={value}"))
            .collect::<Vec<_>>()
            .join(" ");
        self.events
            .lock()
            .expect("warning capture lock")
            .push(fields);
    }
}

#[derive(Default)]
struct CacheKeepaliveEventVisitor {
    fields: Vec<(String, String)>,
}

impl Visit for CacheKeepaliveEventVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.fields
            .push((field.name().to_owned(), format!("{value:?}")));
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.fields
            .push((field.name().to_owned(), value.to_owned()));
    }
}

struct StaticRenewalDispatcher {
    finalization: Mutex<Option<RenewalFinalization>>,
}

#[async_trait]
impl KeepaliveDispatcher for StaticRenewalDispatcher {
    async fn dispatch(
        &self,
        _snapshot: &RequestSnapshot,
        _context: KeepaliveDispatchContext,
    ) -> DispatchOutcome {
        DispatchOutcome::CacheHit {
            cache_anchor_age: Duration::ZERO,
            finalization: self
                .finalization
                .lock()
                .expect("finalization lock")
                .take()
                .expect("one renewal finalization"),
        }
    }
}

#[tokio::test]
async fn apalis_housekeeping_maintains_separate_sqlite_application_and_scheduler_files() {
    use cc_lb_clock::TestClock;
    use cc_lb_scheduler::jobs::apalis_housekeeping::ApalisHousekeepingJob;
    use cc_lb_scheduler::worker::SchedulerBackend;
    use cc_lb_storage_api::{CacheKeepaliveReplaceRequest, CacheTtl};

    const NOW: u64 = 1_800_000_000;
    const RUNNING_CUTOFF: u64 = NOW - 120;
    const PENDING_CUTOFF: u64 = NOW - 3_600;

    let fixture = Fixture::new().await;
    let scheduler_pool = match &fixture.backend.backend {
        SchedulerBackend::Sqlite(backend) => backend.pool(),
        #[cfg(feature = "postgres")]
        SchedulerBackend::Postgres(_) => panic!("fixture must open SQLite scheduler storage"),
    };
    let application_file: String =
        sqlx::query_scalar("SELECT file FROM pragma_database_list WHERE name = 'main'")
            .fetch_one(fixture.storage.pool())
            .await
            .expect("read application database file");
    let scheduler_file: String =
        sqlx::query_scalar("SELECT file FROM pragma_database_list WHERE name = 'main'")
            .fetch_one(scheduler_pool)
            .await
            .expect("read scheduler database file");
    assert_ne!(application_file, scheduler_file);
    assert!(application_file.ends_with("cc-lb.sqlite"));
    assert!(scheduler_file.ends_with("cc-lb.scheduler.sqlite"));

    let mut sessions_before = Vec::new();
    for (key, state, running_since, updated_at, expires_at) in [
        (
            "stale-running",
            "running",
            Some(RUNNING_CUTOFF - 1),
            NOW,
            NOW + 3_600,
        ),
        ("expired", "enqueued", None, NOW, NOW - 1),
        (
            "stale-pending",
            "pending",
            None,
            PENDING_CUTOFF - 1,
            NOW + 3_600,
        ),
        (
            "fresh-running",
            "running",
            Some(RUNNING_CUTOFF + 1),
            PENDING_CUTOFF - 1,
            NOW + 3_600,
        ),
        (
            "cutoff-running",
            "running",
            Some(RUNNING_CUTOFF),
            PENDING_CUTOFF - 1,
            NOW + 3_600,
        ),
        ("healthy-pending", "pending", None, NOW, NOW + 3_600),
        (
            "cutoff-pending",
            "pending",
            None,
            PENDING_CUTOFF,
            NOW + 3_600,
        ),
        (
            "healthy-enqueued",
            "enqueued",
            None,
            PENDING_CUTOFF - 1,
            NOW + 3_600,
        ),
    ] {
        fixture
            .storage
            .replace_from_real_request(&CacheKeepaliveReplaceRequest {
                session_key_hash: key.to_owned(),
                principal_id: "principal".to_owned(),
                accounting_key_id: None,
                upstream_id: fixture.upstream_id,
                cycle_id: uuid::Uuid::nil(),
                cache_anchor_at_unix_secs: NOW - 60,
                ttl: CacheTtl::Ttl5m,
                run_at_unix_secs: NOW + 30,
                expires_at_unix_secs: NOW + 3_600,
                encrypted_payload: vec![1, 2, 3, 4],
                display_reason: "housekeeping regression".to_owned(),
                config_snapshot: fixture.enqueue_request().config_snapshot,
                now_unix_secs: NOW - 7_200,
            })
            .await
            .expect("create application keepalive session through storage");
        sqlx::query(
            "UPDATE cache_keepalive_sessions
             SET enqueue_state = ?2, running_since_unix_secs = ?3, updated_at = ?4,
                 expires_at = ?5, error = 'before-housekeeping'
             WHERE session_key_hash = ?1",
        )
        .bind(key)
        .bind(state)
        .bind(running_since.map(|value| i64::try_from(value).expect("lease fits i64")))
        .bind(i64::try_from(updated_at).expect("updated timestamp fits i64"))
        .bind(i64::try_from(expires_at).expect("expiry fits i64"))
        .execute(fixture.storage.pool())
        .await
        .expect("seed persisted application session lifecycle");
        sessions_before.push(
            fixture
                .storage
                .get_cache_keepalive_session(key)
                .await
                .expect("read seeded session")
                .expect("seeded session exists"),
        );
    }

    sqlx::query(
        "INSERT INTO Workers (id, worker_type, storage_name, layers, last_seen, started_at)
         VALUES ('housekeeping-live-worker', 'cron', 'default', '', ?1, ?1)",
    )
    .bind(i64::try_from(NOW).expect("now fits i64"))
    .execute(scheduler_pool)
    .await
    .expect("seed live scheduler worker");
    let job_seeds = [
        (
            "stale-keepalive",
            "adaptive",
            Some("cache_keepalive:stale-running:1"),
            RUNNING_CUTOFF - 1,
        ),
        (
            "cutoff-keepalive",
            "cache_keepalive",
            Some("cache_keepalive:cutoff-running:1"),
            RUNNING_CUTOFF,
        ),
        (
            "fresh-keepalive",
            "cache_keepalive",
            Some("cache_keepalive:fresh-running:1"),
            RUNNING_CUTOFF + 1,
        ),
        ("stale-generic", "cron", None, RUNNING_CUTOFF - 1),
        ("cutoff-generic", "cron", None, RUNNING_CUTOFF),
        ("fresh-generic", "cron", None, RUNNING_CUTOFF + 1),
    ];
    for (id, job_type, idempotency_key, lock_at) in job_seeds {
        sqlx::query(
            "INSERT INTO Jobs
             (job, id, job_type, status, run_at, lock_by, lock_at, attempts,
              last_result, idempotency_key)
             VALUES (?1, ?2, ?3, 'Running', ?4, 'housekeeping-live-worker', ?5, 2,
                     ?6, ?7)",
        )
        .bind(vec![0_u8])
        .bind(id)
        .bind(job_type)
        .bind(i64::try_from(NOW).expect("run timestamp fits i64"))
        .bind(i64::try_from(lock_at).expect("lock timestamp fits i64"))
        .bind(r#"{"Ok":"untouched"}"#)
        .bind(idempotency_key)
        .execute(scheduler_pool)
        .await
        .expect("seed scheduler job");
    }

    let mut dispatch = fixture.dispatch(Arc::new(FailingPusher));
    dispatch.clock = Arc::new(TestClock::new_at_secs(NOW));
    let outcome = dispatch
        .dispatch_singleton(CronJob::ApalisHousekeeping(ApalisHousekeepingJob::default()))
        .await
        .expect("dispatch real housekeeping singleton");
    assert_eq!(outcome, JobOutcome::Done);

    let application_tables: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sqlite_master
         WHERE type = 'table' AND name = 'cache_keepalive_sessions'",
    )
    .fetch_one(fixture.storage.pool())
    .await
    .expect("inspect application session table");
    let scheduler_tables: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sqlite_master
         WHERE type = 'table' AND name = 'cache_keepalive_sessions'",
    )
    .fetch_one(scheduler_pool)
    .await
    .expect("inspect scheduler table ownership");
    assert_eq!(application_tables, 1);
    assert_eq!(
        scheduler_tables, 0,
        "application tables must not be duplicated"
    );

    for mut expected in sessions_before {
        let persisted = fixture
            .storage
            .get_cache_keepalive_session(&expected.session_key_hash)
            .await
            .expect("read persisted session after housekeeping");
        match expected.session_key_hash.as_str() {
            "expired" | "stale-pending" => {
                assert!(
                    persisted.is_none(),
                    "{} must be purged",
                    expected.session_key_hash
                );
            }
            "stale-running" => {
                expected.status = CacheKeepaliveSessionStatus::Terminal;
                expected.terminal_reason = Some(CacheKeepaliveTerminalReason::Stale);
                expected.enqueue_state = CacheKeepaliveEnqueueState::Pending;
                expected.running_since_unix_secs = None;
                expected.encrypted_payload.clear();
                expected.error = Some("keepalive renewal timed out while running".to_owned());
                expected.updated_at_unix_secs = NOW;
                assert_eq!(
                    persisted,
                    Some(expected),
                    "stale recovery must scrub payload/lease and preserve session identity",
                );
            }
            _ => assert_eq!(
                persisted,
                Some(expected),
                "healthy session must be unchanged"
            ),
        }
    }
    let session_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM cache_keepalive_sessions")
        .fetch_one(fixture.storage.pool())
        .await
        .expect("count surviving application sessions");
    assert_eq!(session_count, 6);

    for (id, job_type, idempotency_key, lock_at) in job_seeds {
        type PersistedJobRow = (
            String,
            Option<i64>,
            Option<String>,
            Option<i64>,
            i64,
            Option<String>,
            String,
            Option<String>,
        );
        let (
            status,
            done_at,
            lock_by,
            persisted_lock_at,
            attempts,
            last_result,
            persisted_type,
            persisted_key,
        ): PersistedJobRow = sqlx::query_as(
            "SELECT status, done_at, lock_by, lock_at, attempts, last_result,
                    job_type, idempotency_key FROM Jobs WHERE id = ?1",
        )
        .bind(id)
        .fetch_one(scheduler_pool)
        .await
        .expect("read persisted scheduler job");
        assert_eq!(persisted_type, job_type);
        assert_eq!(persisted_key.as_deref(), idempotency_key);
        if id == "stale-keepalive" || id == "stale-generic" {
            assert_eq!(
                status,
                if id == "stale-keepalive" {
                    "Killed"
                } else {
                    "Pending"
                }
            );
            assert_eq!(
                done_at,
                (id == "stale-keepalive").then_some(i64::try_from(NOW).expect("now fits i64")),
            );
            assert!(lock_by.is_none(), "{id} lock owner must be cleared");
            assert!(persisted_lock_at.is_none(), "{id} lease must be cleared");
            assert_eq!(attempts, if id == "stale-keepalive" { 2 } else { 3 });
            let result: Value =
                serde_json::from_str(&last_result.expect("stale job has housekeeping reason"))
                    .expect("housekeeping result is JSON");
            let error = result["Err"].as_str().expect("housekeeping error reason");
            assert!(error.contains("cc-lb housekeeping") && error.contains("stale"));
        } else {
            assert_eq!(
                status, "Running",
                "{id} must not be reaped at or after cutoff"
            );
            assert!(done_at.is_none());
            assert_eq!(lock_by.as_deref(), Some("housekeeping-live-worker"));
            assert_eq!(
                persisted_lock_at,
                Some(i64::try_from(lock_at).expect("lock timestamp fits i64")),
            );
            assert_eq!(attempts, 2);
            assert_eq!(last_result.as_deref(), Some(r#"{"Ok":"untouched"}"#));
        }
    }
    let job_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM Jobs")
        .fetch_one(scheduler_pool)
        .await
        .expect("count retained scheduler jobs");
    assert_eq!(
        job_count, 6,
        "stale keepalive must be killed, not deleted or requeued"
    );
}

#[tokio::test]
async fn apalis_housekeeping_missing_application_table_retries_and_recovers_qa10() {
    use cc_lb_scheduler::jobs::apalis_housekeeping::ApalisHousekeepingJob;

    let fixture = Fixture::new().await;
    sqlx::query(
        "ALTER TABLE cache_keepalive_sessions RENAME TO cache_keepalive_sessions_unavailable",
    )
    .execute(fixture.storage.pool())
    .await
    .expect("hide application session table");

    let outcome = fixture
        .dispatch(Arc::new(FailingPusher))
        .dispatch_singleton(CronJob::ApalisHousekeeping(ApalisHousekeepingJob::default()))
        .await
        .expect("dispatch housekeeping with missing application table");
    assert_eq!(
        outcome,
        JobOutcome::Retry {
            delay: Duration::from_secs(60)
        },
        "QA-10: missing application table must retry, not complete"
    );

    sqlx::query(
        "ALTER TABLE cache_keepalive_sessions_unavailable RENAME TO cache_keepalive_sessions",
    )
    .execute(fixture.storage.pool())
    .await
    .expect("restore application session table");
    let recovered = fixture
        .dispatch(Arc::new(FailingPusher))
        .dispatch_singleton(CronJob::ApalisHousekeeping(ApalisHousekeepingJob::default()))
        .await
        .expect("redispatch housekeeping after restoring application table");
    assert_eq!(recovered, JobOutcome::Done);
}

#[tokio::test]
async fn apalis_housekeeping_malformed_application_table_retries_and_recovers_qa11() {
    use cc_lb_scheduler::jobs::apalis_housekeeping::ApalisHousekeepingJob;
    use cc_lb_storage_api::MetaStore;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};

    let fixture = Fixture::new().await;
    let malformed_dir = tempfile::tempdir().expect("qa11 tempdir");
    let malformed_path = malformed_dir.path().join("qa11-malformed.sqlite");
    let malformed_url = format!("sqlite://{}", malformed_path.display());
    let malformed_options = malformed_url
        .parse::<SqliteConnectOptions>()
        .expect("parse isolated malformed application database URL")
        .create_if_missing(true);
    let malformed_pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(malformed_options)
        .await
        .expect("open isolated malformed application database");
    let malformed_storage = Arc::new(cc_lb_storage_sqlite::SqliteStorage::new(
        malformed_pool,
        Arc::new(SystemClock),
    ));
    malformed_storage
        .initialize()
        .await
        .expect("initialize isolated malformed application database");
    sqlx::query(
        "ALTER TABLE cache_keepalive_sessions RENAME COLUMN updated_at TO updated_at_malformed",
    )
    .execute(malformed_storage.pool())
    .await
    .expect("rename required application column away");

    let outcome = fixture
        .dispatch_with_application_storage(Arc::clone(&malformed_storage), Arc::new(FailingPusher))
        .dispatch_singleton(CronJob::ApalisHousekeeping(ApalisHousekeepingJob::default()))
        .await
        .expect("dispatch housekeeping with malformed application table");
    assert_eq!(
        outcome,
        JobOutcome::Retry {
            delay: Duration::from_secs(60)
        },
        "QA-11: malformed application table must retry, not complete"
    );

    sqlx::query(
        "ALTER TABLE cache_keepalive_sessions RENAME COLUMN updated_at_malformed TO updated_at",
    )
    .execute(malformed_storage.pool())
    .await
    .expect("restore required application column");
    let recovered = fixture
        .dispatch_with_application_storage(malformed_storage, Arc::new(FailingPusher))
        .dispatch_singleton(CronJob::ApalisHousekeeping(ApalisHousekeepingJob::default()))
        .await
        .expect("redispatch housekeeping after restoring application column");
    assert_eq!(recovered, JobOutcome::Done);
}

#[tokio::test]
async fn apalis_housekeeping_reopened_scheduler_factory_cleans_existing_files_qa18() {
    use cc_lb_clock::TestClock;
    use cc_lb_config::{SchedulerConfig, StorageConfig};
    use cc_lb_scheduler::jobs::apalis_housekeeping::ApalisHousekeepingJob;
    use cc_lb_scheduler::worker::SchedulerBackend;
    use cc_lb_storage_api::{CacheKeepaliveReplaceRequest, CacheTtl};

    const NOW: u64 = 1_800_000_000;
    let mut fixture = Fixture::new().await;
    fixture
        .storage
        .replace_from_real_request(&CacheKeepaliveReplaceRequest {
            session_key_hash: "qa18-stale".to_owned(),
            principal_id: "principal".to_owned(),
            accounting_key_id: None,
            upstream_id: fixture.upstream_id,
            cycle_id: uuid::Uuid::nil(),
            cache_anchor_at_unix_secs: NOW - 60,
            ttl: CacheTtl::Ttl5m,
            run_at_unix_secs: NOW,
            expires_at_unix_secs: NOW + 3_600,
            encrypted_payload: vec![1, 2, 3],
            display_reason: "QA-18".to_owned(),
            config_snapshot: fixture.enqueue_request().config_snapshot,
            now_unix_secs: NOW - 7_200,
        })
        .await
        .expect("seed existing application file");
    sqlx::query(
        "INSERT INTO Workers
         (id, worker_type, storage_name, layers, last_seen, started_at)
         VALUES ('qa18-worker', 'cron', 'default', '', ?1, ?1)",
    )
    .bind(i64::try_from(NOW).expect("timestamp fits i64"))
    .execute(match &fixture.backend.backend {
        SchedulerBackend::Sqlite(backend) => backend.pool(),
        #[cfg(feature = "postgres")]
        SchedulerBackend::Postgres(_) => panic!("QA-18 requires SQLite scheduler storage"),
    })
    .await
    .expect("seed scheduler worker");
    sqlx::query(
        "INSERT INTO Jobs
         (job, id, job_type, status, run_at, lock_by, lock_at, attempts,
          last_result, idempotency_key)
         VALUES (?1, ?2, 'cache_keepalive', 'Running', ?3, 'qa18-worker', ?4, 0,
                 ?5, ?6)",
    )
    .bind(vec![0_u8])
    .bind("qa18-scheduler-row")
    .bind(i64::try_from(NOW).expect("timestamp fits i64"))
    .bind(i64::try_from(NOW - 121).expect("timestamp fits i64"))
    .bind(r#"{"Ok":"untouched"}"#)
    .bind("cache_keepalive:qa18-stale:1")
    .execute(match &fixture.backend.backend {
        SchedulerBackend::Sqlite(backend) => backend.pool(),
        #[cfg(feature = "postgres")]
        SchedulerBackend::Postgres(_) => panic!("QA-18 requires SQLite scheduler storage"),
    })
    .await
    .expect("seed existing scheduler file");
    sqlx::query(
        "UPDATE cache_keepalive_sessions
         SET enqueue_state = 'running',
             running_since_unix_secs = ?1, updated_at = ?2",
    )
    .bind(i64::try_from(NOW - 121).expect("timestamp fits i64"))
    .bind(i64::try_from(NOW).expect("timestamp fits i64"))
    .execute(fixture.storage.pool())
    .await
    .expect("seed stale application session");

    let application_file: String =
        sqlx::query_scalar("SELECT file FROM pragma_database_list WHERE name = 'main'")
            .fetch_one(fixture.storage.pool())
            .await
            .expect("read existing application file");
    let reopened_storage = crate::storage_factory::open_storage(
        &StorageConfig::Sqlite {
            path: std::path::PathBuf::from(&application_file),
        },
        Arc::new(SystemClock),
    )
    .await
    .expect("reopen application through production factory");
    fixture.backend = crate::scheduler_factory::open_scheduler_storage(
        &StorageConfig::Sqlite {
            path: std::path::PathBuf::from(application_file),
        },
        &SchedulerConfig::default(),
        Arc::new(SystemClock),
    )
    .await
    .expect("reopen scheduler through production factory");

    let scheduler_pool = match &fixture.backend.backend {
        SchedulerBackend::Sqlite(backend) => backend.pool(),
        #[cfg(feature = "postgres")]
        SchedulerBackend::Postgres(_) => panic!("QA-18 requires SQLite scheduler storage"),
    };
    let scheduler_application_tables: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sqlite_master
         WHERE type = 'table' AND name = 'cache_keepalive_sessions'",
    )
    .fetch_one(scheduler_pool)
    .await
    .expect("inspect reopened scheduler tables");
    assert_eq!(
        scheduler_application_tables, 0,
        "reopening scheduler storage must not add application tables"
    );

    let mut dispatch = fixture.dispatch(Arc::new(FailingPusher));
    dispatch.storage = reopened_storage.storage;
    dispatch.clock = Arc::new(TestClock::new_at_secs(NOW));
    let outcome = dispatch
        .dispatch_singleton(CronJob::ApalisHousekeeping(ApalisHousekeepingJob::default()))
        .await
        .expect("dispatch housekeeping after reopening existing files");
    assert_eq!(outcome, JobOutcome::Done);
    let scheduler_status: String =
        sqlx::query_scalar("SELECT status FROM Jobs WHERE id = 'qa18-scheduler-row'")
            .fetch_one(scheduler_pool)
            .await
            .expect("read cleaned scheduler row");
    assert_eq!(scheduler_status, "Killed");
    let session = fixture
        .storage
        .get_cache_keepalive_session("qa18-stale")
        .await
        .expect("read cleaned application session")
        .expect("seeded application session remains as terminal record");
    assert_eq!(session.status, CacheKeepaliveSessionStatus::Terminal);
    assert_eq!(
        session.terminal_reason,
        Some(CacheKeepaliveTerminalReason::Stale)
    );
}

#[tokio::test]
async fn apalis_housekeeping_repeat_dispatch_converges_and_preserves_healthy_rows_qa12() {
    use cc_lb_clock::TestClock;
    use cc_lb_scheduler::jobs::apalis_housekeeping::ApalisHousekeepingJob;
    use cc_lb_storage_api::{CacheKeepaliveReplaceRequest, CacheTtl};

    const NOW: u64 = 1_800_000_000;
    let fixture = Fixture::new().await;
    for (key, running_since) in [("qa12-stale", Some(NOW - 121)), ("qa12-healthy", None)] {
        fixture
            .storage
            .replace_from_real_request(&CacheKeepaliveReplaceRequest {
                session_key_hash: key.to_owned(),
                principal_id: "principal".to_owned(),
                accounting_key_id: None,
                upstream_id: fixture.upstream_id,
                cycle_id: uuid::Uuid::nil(),
                cache_anchor_at_unix_secs: NOW - 60,
                ttl: CacheTtl::Ttl5m,
                run_at_unix_secs: NOW,
                expires_at_unix_secs: NOW + 3_600,
                encrypted_payload: vec![4, 5, 6],
                display_reason: "QA-12".to_owned(),
                config_snapshot: fixture.enqueue_request().config_snapshot,
                now_unix_secs: NOW - 7_200,
            })
            .await
            .expect("seed application session");
        if let Some(running_since) = running_since {
            sqlx::query(
                "UPDATE cache_keepalive_sessions
                 SET enqueue_state = 'running', status = 'active',
                     running_since_unix_secs = ?1, updated_at = ?2
                 WHERE session_key_hash = ?3",
            )
            .bind(i64::try_from(running_since).expect("timestamp fits i64"))
            .bind(i64::try_from(NOW).expect("timestamp fits i64"))
            .bind(key)
            .execute(fixture.storage.pool())
            .await
            .expect("seed stale running session");
        } else {
            sqlx::query(
                "UPDATE cache_keepalive_sessions
                 SET updated_at = ?1
                 WHERE session_key_hash = ?2",
            )
            .bind(i64::try_from(NOW).expect("timestamp fits i64"))
            .bind(key)
            .execute(fixture.storage.pool())
            .await
            .expect("seed healthy session timestamp");
        }
    }
    let mut dispatch = fixture.dispatch(Arc::new(FailingPusher));
    dispatch.clock = Arc::new(TestClock::new_at_secs(NOW));
    let first = dispatch
        .dispatch_singleton(CronJob::ApalisHousekeeping(ApalisHousekeepingJob::default()))
        .await
        .expect("first housekeeping dispatch");
    assert_eq!(first, JobOutcome::Done);
    let healthy_after_first = fixture
        .storage
        .get_cache_keepalive_session("qa12-healthy")
        .await
        .expect("read healthy row after first dispatch")
        .expect("healthy row survives first dispatch");
    let stale_after_first = fixture
        .storage
        .get_cache_keepalive_session("qa12-stale")
        .await
        .expect("read stale row after first dispatch")
        .expect("stale row is terminalized, not purged");

    let second = dispatch
        .dispatch_singleton(CronJob::ApalisHousekeeping(ApalisHousekeepingJob::default()))
        .await
        .expect("second housekeeping dispatch");
    assert_eq!(second, JobOutcome::Done);
    assert_eq!(
        fixture
            .storage
            .get_cache_keepalive_session("qa12-healthy")
            .await
            .expect("read healthy row after second dispatch"),
        Some(healthy_after_first),
        "QA-12: repeated housekeeping preserves healthy rows"
    );
    assert_eq!(
        fixture
            .storage
            .get_cache_keepalive_session("qa12-stale")
            .await
            .expect("read stale row after second dispatch"),
        Some(stale_after_first),
        "QA-12: repeated housekeeping converges without rewriting terminal rows"
    );
}

#[tokio::test]
async fn default_upstream_affinity_purge_physically_removes_stale_null_expiry_rows() {
    let fixture = Fixture::new().await;
    sqlx::query(
        "INSERT INTO upstream_affinity_v1
         (principal_id, provider, kind, value_sha256, upstream_id, observed_at_unix_secs, expires_at_unix_secs)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL)",
    )
    .bind("principal")
    .bind("anthropic")
    .bind("anthropic_web_search_encrypted_content")
    .bind(vec![7_u8; 32])
    .bind(fixture.upstream_id.to_string())
    .bind(0_i64)
    .execute(fixture.storage.pool())
    .await
    .expect("seed stale upstream affinity");

    let outcome = fixture
        .dispatch(Arc::new(FailingPusher))
        .dispatch_singleton(CronJob::UpstreamAffinityPurge(
            UpstreamAffinityPurgeJob::default(),
        ))
        .await
        .expect("dispatch upstream affinity purge");

    assert_eq!(outcome, JobOutcome::Done);
    let remaining: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM upstream_affinity_v1")
        .fetch_one(fixture.storage.pool())
        .await
        .expect("count upstream affinity rows");
    assert_eq!(remaining, 0);
}

#[tokio::test]
async fn upstream_affinity_purge_uses_the_captured_configured_retention() {
    let fixture = Fixture::new().await;
    let now_unix_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time after unix epoch")
        .as_secs();
    for (value_sha256, observed_at_unix_secs) in [
        (vec![8_u8; 32], now_unix_secs - 2 * 86_400),
        (vec![9_u8; 32], now_unix_secs - 12 * 3_600),
    ] {
        sqlx::query(
            "INSERT INTO upstream_affinity_v1
             (principal_id, provider, kind, value_sha256, upstream_id, observed_at_unix_secs, expires_at_unix_secs)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL)",
        )
        .bind("principal")
        .bind("anthropic")
        .bind("anthropic_web_search_encrypted_content")
        .bind(value_sha256)
        .bind(fixture.upstream_id.to_string())
        .bind(i64::try_from(observed_at_unix_secs).expect("test timestamp fits i64"))
        .execute(fixture.storage.pool())
        .await
        .expect("seed upstream affinity");
    }
    let mut config = Config::default();
    config.upstream_affinity.ttl_days = 1;

    let outcome = fixture
        .dispatch_with_config(Arc::new(FailingPusher), config)
        .dispatch_singleton(CronJob::UpstreamAffinityPurge(
            UpstreamAffinityPurgeJob::default(),
        ))
        .await
        .expect("dispatch upstream affinity purge");

    assert_eq!(outcome, JobOutcome::Done);
    let remaining: Vec<Vec<u8>> =
        sqlx::query_scalar("SELECT value_sha256 FROM upstream_affinity_v1")
            .fetch_all(fixture.storage.pool())
            .await
            .expect("read remaining upstream affinity rows");
    assert_eq!(remaining, vec![vec![9_u8; 32]]);
}

#[tokio::test]
async fn renewal_reconciles_real_reservation_once_before_forget() {
    // Given
    let fixture = Fixture::new().await;
    fixture.http.return_cache_hit_usage(100, 0, 10);
    let key_id = create_accounting_key(&fixture, LimitKind::InputTokens, 4_000).await;
    let pusher: Arc<dyn CacheKeepaliveTaskPusher> = Arc::new(fixture.backend.backend.clone());
    let enqueuer = fixture.enqueuer(Arc::clone(&pusher));
    let mut request = fixture.enqueue_request();
    request.accounting_key_id = Some(key_id.clone());
    enqueuer
        .enqueue_cache_keepalive(request)
        .await
        .expect("enqueue durable keepalive");
    let job = fixture.pending_keepalive_job(1).await;
    let limit_engine = LimitEngine::new(
        Arc::new(KeyConcurrencyManager::new()),
        Arc::new(SystemClock),
    );
    let dispatch = fixture.dispatch_with_limit_engine(Arc::clone(&pusher), limit_engine.clone());

    // When
    let outcome = dispatch
        .dispatch_cache_keepalive(job.clone())
        .await
        .expect("dispatch first keepalive delivery");
    let redelivery = dispatch
        .dispatch_cache_keepalive(job)
        .await
        .expect("dispatch duplicate keepalive delivery");

    // Then
    assert!(matches!(outcome, JobOutcome::Done));
    assert!(matches!(redelivery, JobOutcome::Noop));
    assert_eq!(request_event_count(&fixture).await, 1);
    let key_store = KeyStore::new(fixture.storage.clone());
    let key = key_store
        .get("principal", &key_id)
        .await
        .expect("load key")
        .expect("key exists");
    let view = fixture.dynamic_view().load();
    let verifier = limit_engine
        .reserve(
            view.principal_view.as_ref(),
            &key,
            "principal",
            "claude-test",
            Some(0),
            3_900,
            None,
        )
        .expect("one reconcile leaves 3900 input tokens available");
    assert!(
        limit_engine
            .reserve(
                view.principal_view.as_ref(),
                &key,
                "principal",
                "claude-test",
                Some(0),
                1,
                None,
            )
            .is_err(),
        "a duplicate reconcile or full RAII refund would leave more than 3900 tokens"
    );
    drop(verifier);
}

#[tokio::test]
async fn renewal_reconcile_false_errors_before_forget_and_lifecycle_publish() {
    // Given
    let fixture = Fixture::new().await;
    let bus = fixture.event_bus();
    let mut lifecycle_rx = bus.subscribe_lifecycle();
    let key_id = create_accounting_key(&fixture, LimitKind::InputTokens, 4_000).await;
    let key_store = KeyStore::new(fixture.storage.clone());
    let key = key_store
        .get("principal", &key_id)
        .await
        .expect("load key")
        .expect("key exists");
    let limit_engine = LimitEngine::new(
        Arc::new(KeyConcurrencyManager::new()),
        Arc::new(SystemClock),
    );
    let view = fixture.dynamic_view().load();
    let reservation = limit_engine
        .reserve(
            view.principal_view.as_ref(),
            &key,
            "principal",
            "claude-test",
            Some(0),
            100,
            None,
        )
        .expect("create reservation to invalidate before finalization");
    let reservation_id = reservation.id().to_owned();
    assert!(limit_engine.refund_by_id(&reservation_id));
    let accounting_guard = AttemptIntent::from_reservation(reservation)
        .into_reserved()
        .into_response_accounting_guard();
    let keepalive_dispatcher = Arc::new(StaticRenewalDispatcher {
        finalization: Mutex::new(Some(RenewalFinalization {
            usage: RenewalUsage {
                input_tokens: 100,
                ..RenewalUsage::default()
            },
            status: 200,
            duration: Duration::from_millis(7),
            request_start_ms: 0,
            accounting_guard,
        })),
    });
    let pusher: Arc<dyn CacheKeepaliveTaskPusher> = Arc::new(fixture.backend.backend.clone());
    let enqueuer = fixture.enqueuer(Arc::clone(&pusher));
    enqueuer
        .enqueue_cache_keepalive(fixture.enqueue_request())
        .await
        .expect("enqueue keepalive with invalidated reservation finalization");
    let job = fixture.pending_keepalive_job(1).await;
    let dispatch = fixture.dispatch_with_limit_engine_and_keepalive_dispatcher(
        pusher,
        limit_engine.clone(),
        keepalive_dispatcher,
    );

    // When
    let error = dispatch
        .dispatch_cache_keepalive(job)
        .await
        .expect_err("false reconcile must abort finalization");

    // Then
    assert!(
        error
            .to_string()
            .contains("cache keepalive renewal reconcile failed"),
        "unexpected error: {error}"
    );
    assert_eq!(request_event_count(&fixture).await, 1);
    assert!(
        lifecycle_rx.try_recv().is_err(),
        "false reconcile must not publish synthetic success lifecycle events"
    );
    let verifier = limit_engine
        .reserve(
            view.principal_view.as_ref(),
            &key,
            "principal",
            "claude-test",
            Some(0),
            4_000,
            None,
        )
        .expect("invalidated reservation remains refunded after guard drop");
    assert!(
        limit_engine
            .reserve(
                view.principal_view.as_ref(),
                &key,
                "principal",
                "claude-test",
                Some(0),
                1,
                None,
            )
            .is_err(),
        "extra capacity would indicate an unintended second refund"
    );
    drop(verifier);
}

#[tokio::test]
async fn observe_only_renewal_writes_priced_durable_row_and_projection_without_limit_decision() {
    // Given
    install_test_pricing();
    let fixture = Fixture::new().await;
    fixture.http.return_cache_hit_usage(10, 40, 10);
    let bus = fixture.event_bus();
    let mut lifecycle_rx = bus.subscribe_lifecycle();
    let pusher: Arc<dyn CacheKeepaliveTaskPusher> = Arc::new(fixture.backend.backend.clone());
    let enqueuer = fixture.enqueuer(Arc::clone(&pusher));
    enqueuer
        .enqueue_cache_keepalive(fixture.enqueue_request())
        .await
        .expect("enqueue observe-only keepalive");
    let job = fixture.pending_keepalive_job(1).await;
    let dispatch = fixture.dispatch(pusher);

    // When
    let outcome = dispatch
        .dispatch_cache_keepalive(job)
        .await
        .expect("dispatch observe-only keepalive");

    // Then
    assert!(matches!(outcome, JobOutcome::Done));
    assert_eq!(request_event_count(&fixture).await, 1);
    let events = stored_request_events(&fixture).await;
    assert_eq!(events[0].key_id, None);
    assert_eq!(events[0].cost_usd_micros, Some(140));
    let projection_key_id: Option<String> = sqlx::query_scalar(
        "SELECT accounting_key_id FROM cache_keepalive_turns WHERE source_ref_id = ?",
    )
    .bind("session-hash:1")
    .fetch_one(fixture.storage.pool())
    .await
    .expect("load observe-only turn projection key");
    let projection_cost_micros: i64 =
        sqlx::query_scalar("SELECT cost_micros FROM cache_keepalive_turns WHERE source_ref_id = ?")
            .bind("session-hash:1")
            .fetch_one(fixture.storage.pool())
            .await
            .expect("load observe-only turn projection cost");
    assert_eq!(projection_key_id, None);
    assert_eq!(projection_cost_micros, 140);
    let record = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load rescheduled observe-only session")
        .expect("observe-only session exists");
    assert_eq!(record.accounting_key_id, None);
    let mut saw_limit_decision = false;
    while let Ok(event) = lifecycle_rx.try_recv() {
        saw_limit_decision |= matches!(event, LifecycleEvent::LimitDecision { .. });
    }
    assert!(!saw_limit_decision);
}

#[tokio::test]
async fn repeated_null_key_renewals_remain_observe_only() {
    // Given
    install_test_pricing();
    let fixture = Fixture::new().await;
    fixture.http.return_cache_hit_usage(10, 40, 10);
    let bus = fixture.event_bus();
    let mut lifecycle_rx = bus.subscribe_lifecycle();
    let pusher: Arc<dyn CacheKeepaliveTaskPusher> = Arc::new(fixture.backend.backend.clone());
    let enqueuer = fixture.enqueuer(Arc::clone(&pusher));
    enqueuer
        .enqueue_cache_keepalive(fixture.enqueue_request())
        .await
        .expect("enqueue initial observe-only keepalive");
    let dispatch = fixture.dispatch(Arc::clone(&pusher));

    // When
    let first_outcome = dispatch
        .dispatch_cache_keepalive(fixture.pending_keepalive_job(1).await)
        .await
        .expect("dispatch first observe-only renewal");
    let second_outcome = dispatch
        .dispatch_cache_keepalive(fixture.pending_keepalive_job(2).await)
        .await
        .expect("dispatch repeated observe-only renewal");

    // Then
    assert!(matches!(first_outcome, JobOutcome::Done));
    assert!(matches!(second_outcome, JobOutcome::Done));
    let record = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load twice-rescheduled session")
        .expect("observe-only session exists");
    assert_eq!(record.generation, 3);
    assert_eq!(record.accounting_key_id, None);
    let projections: Vec<(Option<String>, i64)> = sqlx::query_as(
        "SELECT accounting_key_id, cost_micros FROM cache_keepalive_turns ORDER BY source_ref_id",
    )
    .fetch_all(fixture.storage.pool())
    .await
    .expect("load observe-only renewal projections");
    assert_eq!(projections, vec![(None, 140), (None, 140)]);
    let events = stored_request_events(&fixture).await;
    assert_eq!(events.len(), 2);
    assert!(events.iter().all(|event| event.key_id.is_none()));
    assert!(
        events
            .iter()
            .all(|event| event.cost_usd_micros == Some(140))
    );
    let mut saw_limit_decision = false;
    while let Ok(event) = lifecycle_rx.try_recv() {
        saw_limit_decision |= matches!(event, LifecycleEvent::LimitDecision { .. });
    }
    assert!(!saw_limit_decision);
}

#[tokio::test]
async fn real_request_reseeds_observe_only_session_without_double_reserve() {
    // Given
    let fixture = Fixture::new().await;
    fixture.http.return_cache_hit_usage(100, 0, 10);
    let key_id = create_accounting_key(&fixture, LimitKind::InputTokens, 4_000).await;
    let pusher: Arc<dyn CacheKeepaliveTaskPusher> = Arc::new(fixture.backend.backend.clone());
    let enqueuer = fixture.enqueuer(Arc::clone(&pusher));
    enqueuer
        .enqueue_cache_keepalive(fixture.enqueue_request())
        .await
        .expect("enqueue initial observe-only keepalive");
    let limit_engine = LimitEngine::new(
        Arc::new(KeyConcurrencyManager::new()),
        Arc::new(SystemClock),
    );
    let dispatch = fixture.dispatch_with_limit_engine(Arc::clone(&pusher), limit_engine.clone());
    dispatch
        .dispatch_cache_keepalive(fixture.pending_keepalive_job(1).await)
        .await
        .expect("dispatch initial observe-only renewal");
    let stale_observe_only_job = fixture.pending_keepalive_job(2).await;
    let mut real_request = fixture.enqueue_request();
    real_request.accounting_key_id = Some(key_id.clone());

    // When
    enqueuer
        .enqueue_cache_keepalive(real_request)
        .await
        .expect("reseed keepalive from real request");
    let stale_outcome = dispatch
        .dispatch_cache_keepalive(stale_observe_only_job)
        .await
        .expect("dispatch stale observe-only renewal");
    let reseeded_outcome = dispatch
        .dispatch_cache_keepalive(fixture.pending_keepalive_job(3).await)
        .await
        .expect("dispatch reseeded renewal");

    // Then
    assert!(matches!(stale_outcome, JobOutcome::Noop));
    assert!(matches!(reseeded_outcome, JobOutcome::Done));
    let record = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load reseeded session")
        .expect("reseeded session exists");
    assert_eq!(record.accounting_key_id.as_deref(), Some(key_id.as_str()));
    assert_eq!(request_event_count(&fixture).await, 2);
    let key_store = KeyStore::new(fixture.storage.clone());
    let key = key_store
        .get("principal", &key_id)
        .await
        .expect("load reseeded accounting key")
        .expect("reseeded accounting key exists");
    let view = fixture.dynamic_view().load();
    let verifier = limit_engine
        .reserve(
            view.principal_view.as_ref(),
            &key,
            "principal",
            "claude-test",
            Some(0),
            3_900,
            None,
        )
        .expect("one real renewal reconcile leaves 3900 input tokens available");
    assert!(
        limit_engine
            .reserve(
                view.principal_view.as_ref(),
                &key,
                "principal",
                "claude-test",
                Some(0),
                1,
                None,
            )
            .is_err(),
        "a stale observe-only turn must not reserve and a reseeded turn must reconcile exactly once"
    );
    drop(verifier);
}

#[tokio::test]
async fn renewal_inline_price_matches_pricing_subscriber_priced_event() {
    // Given
    install_test_pricing();
    let fixture = Fixture::new().await;
    fixture.http.return_cache_hit_usage(10, 40, 0);
    let bus = fixture.event_bus();
    let pricing_rx = bus.attach_lifecycle_pricing(32);
    let mut lifecycle_rx = bus.subscribe_lifecycle();
    let pricing_handle = cc_lb_pricing::spawn_lifecycle_pricing_subscriber(
        pricing_rx,
        bus.clone() as Arc<dyn RequestEventBus>,
    );
    let pusher: Arc<dyn CacheKeepaliveTaskPusher> = Arc::new(fixture.backend.backend.clone());
    let enqueuer = fixture.enqueuer(Arc::clone(&pusher));
    enqueuer
        .enqueue_cache_keepalive(fixture.enqueue_request())
        .await
        .expect("enqueue priced keepalive");
    let job = fixture.pending_keepalive_job(1).await;
    let dispatch = fixture.dispatch(pusher);

    // When
    let outcome = dispatch
        .dispatch_cache_keepalive(job)
        .await
        .expect("dispatch priced keepalive");
    pricing_handle.shutdown().await;

    // Then
    assert!(matches!(outcome, JobOutcome::Done));
    let events = stored_request_events(&fixture).await;
    let inline_cost = events[0].cost_usd_micros;
    let mut subscriber_cost = None;
    while let Ok(event) = lifecycle_rx.try_recv() {
        if let LifecycleEvent::Priced { cost, .. } = event {
            subscriber_cost = cost.total_micros;
        }
    }
    assert_eq!(inline_cost, subscriber_cost);
    assert_eq!(inline_cost, Some(140));
}

#[tokio::test]
async fn saturated_lifecycle_bus_still_persists_and_reconciles_once() {
    // Given
    let fixture = Fixture::new().await;
    fixture.http.return_cache_hit_usage(100, 0, 10);
    let key_id = create_accounting_key(&fixture, LimitKind::InputTokens, 4_000).await;
    let _full_receivers = saturate_lifecycle_bus(&fixture);
    let pusher: Arc<dyn CacheKeepaliveTaskPusher> = Arc::new(fixture.backend.backend.clone());
    let enqueuer = fixture.enqueuer(Arc::clone(&pusher));
    let mut request = fixture.enqueue_request();
    request.accounting_key_id = Some(key_id.clone());
    enqueuer
        .enqueue_cache_keepalive(request)
        .await
        .expect("enqueue saturated keepalive");
    let job = fixture.pending_keepalive_job(1).await;
    let limit_engine = LimitEngine::new(
        Arc::new(KeyConcurrencyManager::new()),
        Arc::new(SystemClock),
    );
    let dispatch = fixture.dispatch_with_limit_engine(pusher, limit_engine.clone());

    // When
    let outcome = dispatch
        .dispatch_cache_keepalive(job)
        .await
        .expect("dispatch saturated keepalive");

    // Then
    assert!(matches!(outcome, JobOutcome::Done));
    assert_eq!(request_event_count(&fixture).await, 1);
    let turn_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM cache_keepalive_turns")
        .fetch_one(fixture.storage.pool())
        .await
        .expect("count turn projections");
    assert_eq!(turn_count, 1);
    let key_store = KeyStore::new(fixture.storage.clone());
    let key = key_store
        .get("principal", &key_id)
        .await
        .expect("load key")
        .expect("key exists");
    let view = fixture.dynamic_view().load();
    let verifier = limit_engine
        .reserve(
            view.principal_view.as_ref(),
            &key,
            "principal",
            "claude-test",
            Some(0),
            3_900,
            None,
        )
        .expect("direct reconcile must happen despite full bus channels");
    drop(verifier);
}

#[tokio::test]
async fn renewal_response_persists_one_attributed_event_and_projection_set() {
    // Given
    let fixture = Fixture::new().await;
    let key_store = KeyStore::new(fixture.storage.clone());
    let (_key, secret) = key_store
        .create(
            "principal",
            CreateParams {
                label: "renewal accounting".to_owned(),
                description: None,
                expires_at_unix_secs: None,
                limit_overrides: vec![Limit {
                    kind: LimitKind::Requests,
                    window_secs: 60,
                    cap_micros: 10,
                }],
            },
        )
        .await
        .expect("create renewal accounting key");
    let pusher: Arc<dyn CacheKeepaliveTaskPusher> = Arc::new(fixture.backend.backend.clone());
    let enqueuer = fixture.enqueuer(Arc::clone(&pusher));
    let mut request = fixture.enqueue_request();
    let key_id = api_key_id(secret.expose());
    request.accounting_key_id = Some(key_id.clone());
    enqueuer
        .enqueue_cache_keepalive(request)
        .await
        .expect("enqueue durable keepalive");
    let job = fixture.pending_keepalive_job(1).await;
    let dispatch = fixture.dispatch(pusher);

    // When
    let outcome = dispatch
        .dispatch_cache_keepalive(job)
        .await
        .expect("dispatch cache keepalive job");

    // Then
    assert!(matches!(outcome, JobOutcome::Done));
    let row_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM request_events_v1")
        .fetch_one(fixture.storage.pool())
        .await
        .expect("count renewal event rows");
    assert_eq!(row_count, 1);
    let events = stored_request_events(&fixture).await;
    assert_eq!(events.len(), 1);
    let event = &events[0];
    assert_eq!(event.event_id.as_deref(), Some("renewal:session-hash:1"));
    assert_eq!(event.source_kind.as_deref(), Some("renewal"));
    assert_eq!(event.source_ref_id.as_deref(), Some("session-hash:1"));
    assert_eq!(event.principal_id.as_deref(), Some("principal"));
    assert_eq!(event.key_id.as_deref(), Some(key_id.as_str()));
    assert_eq!(event.upstream_id, Some(fixture.upstream_id));
    assert_eq!(event.model.as_deref(), Some("claude-test"));
    assert!(event.cost_usd_micros.is_some());
    let turn_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM cache_keepalive_turns")
        .fetch_one(fixture.storage.pool())
        .await
        .expect("count renewal turn projections");
    let decision_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM cache_keepalive_decisions")
        .fetch_one(fixture.storage.pool())
        .await
        .expect("count renewal decision projections");
    assert_eq!(turn_count, 1);
    assert_eq!(decision_count, 1);
}

#[derive(Clone, Copy, Debug)]
enum MidCycleResult {
    Hit,
    Miss,
    Error,
    SyntheticUnsupported,
}

struct UnsupportedAfterDispatch {
    inner: Arc<dyn KeepaliveDispatcher>,
}

#[async_trait]
impl KeepaliveDispatcher for UnsupportedAfterDispatch {
    async fn dispatch(
        &self,
        snapshot: &RequestSnapshot,
        context: KeepaliveDispatchContext,
    ) -> DispatchOutcome {
        // Only the outcome is synthetic: the real dispatcher owns and drops the reservation.
        drop(self.inner.dispatch(snapshot, context).await);
        DispatchOutcome::UnsupportedProvider("synthetic unsupported outcome".to_owned())
    }
}

async fn assert_input_headroom(
    fixture: &Fixture,
    engine: &Arc<LimitEngine>,
    key_id: &str,
    amount: i64,
) {
    let key = KeyStore::new(fixture.storage.clone())
        .get("principal", key_id)
        .await
        .expect("load accounting key")
        .expect("accounting key exists");
    let view = fixture.dynamic_view().load();
    let verifier = engine
        .reserve(
            view.principal_view.as_ref(),
            &key,
            "principal",
            "claude-test",
            Some(0),
            amount,
            None,
        )
        .expect("expected quota headroom remains");
    assert!(
        engine
            .reserve(
                view.principal_view.as_ref(),
                &key,
                "principal",
                "claude-test",
                Some(0),
                1,
                None
            )
            .is_err(),
        "headroom must equal the expected amount exactly"
    );
    drop(verifier);
}

async fn assert_mid_cycle_accounting(clear: bool, result: MidCycleResult) {
    install_test_pricing();
    let fixture = Fixture::new().await;
    match result {
        MidCycleResult::Hit | MidCycleResult::SyntheticUnsupported => {
            fixture.http.return_cache_hit_usage(100, 0, 10)
        }
        MidCycleResult::Miss => fixture.http.return_cache_hit_usage(100, 0, 0),
        MidCycleResult::Error => fixture.http.return_status(
            StatusCode::INTERNAL_SERVER_ERROR,
            serde_json::json!({"error": "boom"}),
        ),
    }
    let key_id = create_accounting_key(&fixture, LimitKind::InputTokens, 4_000).await;
    let pusher: Arc<dyn CacheKeepaliveTaskPusher> = Arc::new(fixture.backend.backend.clone());
    let mut request = fixture.enqueue_request();
    request.accounting_key_id = Some(key_id.clone());
    fixture
        .enqueuer(Arc::clone(&pusher))
        .enqueue_cache_keepalive(request)
        .await
        .expect("enqueue");
    let job = fixture.pending_keepalive_job(1).await;
    let limit_engine = LimitEngine::new(
        Arc::new(KeyConcurrencyManager::new()),
        Arc::new(SystemClock),
    );
    if clear {
        let mut principal =
            cc_lb_storage_api::PrincipalStore::get_by_name(fixture.storage.as_ref(), "principal")
                .await
                .expect("load principal")
                .expect("principal exists");
        principal.cache_keepalive = None;
        let dynamic_view = fixture.dynamic_view();
        fixture.http.run_on_next_dispatch(Arc::new(move || {
            let current = dynamic_view.load();
            let view = cc_lb_control::DynamicViewBuilder::from_view(&current)
                .principal_view(Arc::new(
                    cc_lb_control::api_keys::principal_view::PrincipalView::from_db(
                        std::slice::from_ref(&principal),
                        std::collections::HashMap::new(),
                    ),
                ))
                .build();
            dynamic_view.store(view);
        }));
    } else {
        fixture.disable_cache_keepalive_on_next_http_dispatch();
    }
    let dispatch = if matches!(result, MidCycleResult::SyntheticUnsupported) {
        let inner = Arc::new(
            cc_lb_engine::cache_keepalive::AnthropicKeepaliveDispatcher::new(
                fixture.dynamic_view(),
                fixture.storage.clone(),
                fixture.http.clone(),
                Arc::new(SystemClock),
            ),
        );
        fixture.dispatch_with_limit_engine_and_keepalive_dispatcher(
            pusher,
            limit_engine.clone(),
            Arc::new(UnsupportedAfterDispatch { inner }),
        )
    } else {
        fixture.dispatch_with_limit_engine(pusher, limit_engine.clone())
    };
    assert!(matches!(
        dispatch
            .dispatch_cache_keepalive(job.clone())
            .await
            .expect("dispatch"),
        JobOutcome::Done
    ));
    assert!(matches!(
        dispatch
            .dispatch_cache_keepalive(job)
            .await
            .expect("redeliver"),
        JobOutcome::Noop
    ));
    assert_eq!(
        fixture.http.requests.lock().expect("requests lock").len(),
        1
    );
    let completed = matches!(result, MidCycleResult::Hit | MidCycleResult::Miss);
    assert_eq!(
        request_event_count(&fixture).await,
        i64::from(completed),
        "clear={clear}, {result:?}"
    );
    let turns: Vec<(String, i64)> =
        sqlx::query_as("SELECT hit_miss, cost_micros FROM cache_keepalive_turns")
            .fetch_all(fixture.storage.pool())
            .await
            .expect("read turns");
    let decisions: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM cache_keepalive_decisions")
        .fetch_one(fixture.storage.pool())
        .await
        .expect("count decisions");
    assert_eq!(decisions, i64::from(completed));
    if completed {
        let events = stored_request_events(&fixture).await;
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0].event_id.as_deref(),
            Some("renewal:session-hash:1")
        );
        assert_eq!(events[0].input_tokens, Some(100));
        let (hit_miss, cache_read, cost) = if matches!(result, MidCycleResult::Hit) {
            ("hit", 10, 200)
        } else {
            ("miss", 0, 200)
        };
        assert_eq!(events[0].cache_read_input_tokens, Some(cache_read));
        // The fixture prices input at $2/M and has no cache-read price.
        // Thus 100 input tokens cost 200 micros; the 10 cached tokens add 0.
        assert_eq!(events[0].cost_input_micros, Some(200));
        assert_eq!(events[0].cost_cache_read_micros, Some(0));
        assert_eq!(events[0].cost_usd_micros, Some(cost));
        assert_eq!(turns, vec![(hit_miss.to_owned(), cost)]);
    } else {
        assert!(turns.is_empty());
    }
    let session = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load session")
        .expect("session exists");
    assert_eq!(session.status, CacheKeepaliveSessionStatus::Terminal);
    assert_eq!(
        session.terminal_reason,
        Some(CacheKeepaliveTerminalReason::Cancelled)
    );
    assert_eq!(session.generation, 1);
    let jobs = fixture
        .backend
        .backend
        .list_keepalive_tasks(&cc_lb_scheduler::worker::Filter {
            status: Some(cc_lb_scheduler::worker::TaskStatus::Pending),
            page: 1,
            page_size: Some(10),
        })
        .await
        .expect("list jobs");
    assert_eq!(jobs.len(), 1, "cancellation must not enqueue another job");
    assert_input_headroom(
        &fixture,
        &limit_engine,
        &key_id,
        if completed { 3_900 } else { 4_000 },
    )
    .await;
}

#[tokio::test]
async fn cache_keepalive_disable_or_clear_mid_cycle_accounts_completed_hit_exactly_once() {
    for clear in [false, true] {
        assert_mid_cycle_accounting(clear, MidCycleResult::Hit).await;
    }
}

#[tokio::test]
async fn cache_keepalive_disable_or_clear_mid_cycle_accounts_completed_miss_exactly_once() {
    for clear in [false, true] {
        assert_mid_cycle_accounting(clear, MidCycleResult::Miss).await;
    }
}

#[tokio::test]
async fn cache_keepalive_disable_or_clear_mid_cycle_error_refunds_without_finalizing() {
    for clear in [false, true] {
        assert_mid_cycle_accounting(clear, MidCycleResult::Error).await;
    }
}

#[tokio::test]
async fn cache_keepalive_disable_or_clear_mid_cycle_synthetic_unsupported_refunds_without_finalizing()
 {
    for clear in [false, true] {
        assert_mid_cycle_accounting(clear, MidCycleResult::SyntheticUnsupported).await;
    }
}

fn api_key_id(secret: &str) -> String {
    secret
        .strip_prefix("sk-cclb-")
        .and_then(|value| value.split_once('_'))
        .map(|(key_id, _)| key_id.to_owned())
        .expect("test key includes key id")
}

async fn create_accounting_key(fixture: &Fixture, kind: LimitKind, cap_micros: i64) -> String {
    let key_store = KeyStore::new(fixture.storage.clone());
    let (_, secret) = key_store
        .create(
            "principal",
            CreateParams {
                label: "renewal accounting".to_owned(),
                description: None,
                expires_at_unix_secs: None,
                limit_overrides: vec![Limit {
                    kind,
                    window_secs: 60,
                    cap_micros,
                }],
            },
        )
        .await
        .expect("create renewal accounting key");
    api_key_id(secret.expose())
}

async fn request_event_count(fixture: &Fixture) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM request_events_v1")
        .fetch_one(fixture.storage.pool())
        .await
        .expect("count request events")
}

/// Reads every persisted request event in append order through the cursor API.
async fn stored_request_events(fixture: &Fixture) -> Vec<cc_lb_storage_api::RequestEvent> {
    let cursor = fixture
        .storage
        .current_request_event_cursor()
        .await
        .expect("current request event cursor");
    fixture
        .storage
        .query_request_events_between_cursors(
            0,
            cursor,
            500,
            &cc_lb_storage_api::RequestEventStreamFilters::default(),
        )
        .await
        .expect("query request events")
        .into_iter()
        .map(|(_, event)| event)
        .collect()
}

fn install_test_pricing() {
    let mut models = std::collections::HashMap::new();
    models.insert(
        "claude-test".to_owned(),
        Pricing {
            model: "claude-test".to_owned(),
            input_per_million_usd: UsdPerMillion::from_whole_usd(2),
            output_per_million_usd: UsdPerMillion::from_whole_usd(3),
            input_image_per_million_usd: None,
            output_image_per_million_usd: None,
            by_tier: std::collections::BTreeMap::new(),
        },
    );
    global_catalog().install_snapshot(CatalogSnapshot {
        payload_hash: String::new(),
        fetched_at_ms: 0,
        models,
        raw_json: Vec::new(),
        cache_creation_per_million_usd: std::collections::HashMap::new(),
        cache_read_per_million_usd: std::collections::HashMap::new(),
        cache_creation_per_million_usd_by_tier: std::collections::HashMap::new(),
        cache_read_per_million_usd_by_tier: std::collections::HashMap::new(),
        above_tokens_rates: std::collections::HashMap::new(),
        status: CatalogStatus::Ok,
    });
}

fn saturate_lifecycle_bus(fixture: &Fixture) -> Vec<tokio::sync::mpsc::Receiver<LifecycleEvent>> {
    let bus = fixture.event_bus();
    let receivers = vec![
        bus.attach_lifecycle_writer(1),
        bus.attach_lifecycle_assembler(1),
        bus.attach_lifecycle_pricing(1),
        bus.attach_lifecycle_limit_reconcile(1),
    ];
    bus.publish_lifecycle(LifecycleEvent::RequestStarted {
        event_id: "saturate".to_owned(),
        request_id: "saturate".to_owned(),
        ts_ms: 0,
        stream: false,
        source_kind: None,
        source_ref_id: None,
        event_kind: None,
    });
    receivers
}

struct RenewalAnchorAge {
    inner: Arc<dyn KeepaliveDispatcher>,
    age: Duration,
}

#[async_trait]
impl KeepaliveDispatcher for RenewalAnchorAge {
    async fn dispatch(
        &self,
        snapshot: &RequestSnapshot,
        context: KeepaliveDispatchContext,
    ) -> DispatchOutcome {
        match self.inner.dispatch(snapshot, context).await {
            DispatchOutcome::CacheHit { finalization, .. } => DispatchOutcome::CacheHit {
                cache_anchor_age: self.age,
                finalization,
            },
            other => other,
        }
    }
}

#[tokio::test]
async fn cache_keepalive_hit_reschedule_uses_only_current_response_age_and_clamps_due_time() {
    for (response_age, expected_remaining) in [(17, 253), (400, 1)] {
        let fixture = Fixture::new().await;
        let pusher: Arc<dyn CacheKeepaliveTaskPusher> = Arc::new(fixture.backend.backend.clone());
        let mut request = fixture.enqueue_request();
        request.cache_anchor_age = Duration::from_secs(120);
        request.params.delay = Duration::from_secs(150);
        fixture
            .enqueuer(Arc::clone(&pusher))
            .enqueue_cache_keepalive(request)
            .await
            .expect("enqueue");
        let inner = Arc::new(
            cc_lb_engine::cache_keepalive::AnthropicKeepaliveDispatcher::new(
                fixture.dynamic_view(),
                fixture.storage.clone(),
                fixture.http.clone(),
                Arc::new(SystemClock),
            ),
        );
        let dispatch = fixture.dispatch_with_limit_engine_and_keepalive_dispatcher(
            pusher,
            LimitEngine::new(
                Arc::new(KeyConcurrencyManager::new()),
                Arc::new(SystemClock),
            ),
            Arc::new(RenewalAnchorAge {
                inner,
                age: Duration::from_secs(response_age),
            }),
        );
        let job = fixture.pending_keepalive_job(1).await;
        assert!(matches!(
            dispatch
                .dispatch_cache_keepalive(job)
                .await
                .expect("dispatch"),
            JobOutcome::Done
        ));
        let next = fixture.pending_keepalive_job(2).await;
        assert_eq!(next.refresh_delay_secs, 270);
        let record = fixture
            .storage
            .get_cache_keepalive_session("session-hash")
            .await
            .expect("load rescheduled")
            .expect("rescheduled exists");
        assert_eq!(
            record.updated_at_unix_secs - record.cache_anchor_at_unix_secs,
            response_age
        );
        assert_eq!(
            record.run_at_unix_secs - record.updated_at_unix_secs,
            expected_remaining
        );
    }
}

async fn assert_renewal_cadence(
    ttl: cc_lb_storage_api::CacheTtl,
    age_secs: u64,
    lead_secs: u32,
    initial_delay: u64,
    full_delay: u64,
    legacy: bool,
) {
    let fixture = Fixture::new().await;
    let pusher: Arc<dyn CacheKeepaliveTaskPusher> = Arc::new(fixture.backend.backend.clone());
    let mut request = fixture.enqueue_request();
    request.snapshot.ttl = ttl;
    request.cache_anchor_age = Duration::from_secs(age_secs);
    request.params.delay = Duration::from_secs(initial_delay);
    match ttl {
        cc_lb_storage_api::CacheTtl::Ttl5m => {
            request.config_snapshot.refresh_lead_time_5m_secs = lead_secs
        }
        cc_lb_storage_api::CacheTtl::Ttl1h | cc_lb_storage_api::CacheTtl::Ttl12h => {
            request.config_snapshot.refresh_lead_time_1h_secs = lead_secs
        }
        cc_lb_storage_api::CacheTtl::Ttl30m => {
            request.config_snapshot.refresh_lead_time_5m_secs = lead_secs
        }
    }
    fixture
        .enqueuer(Arc::clone(&pusher))
        .enqueue_cache_keepalive(request)
        .await
        .expect("enqueue");
    if legacy {
        sqlx::query(
            "UPDATE cache_keepalive_sessions SET config_snapshot = NULL WHERE session_key_hash = ?",
        )
        .bind("session-hash")
        .execute(fixture.storage.pool())
        .await
        .expect("create legacy row");
    }
    let first_job = fixture.pending_keepalive_job(1).await;
    assert_eq!(
        first_job.refresh_delay_secs, initial_delay,
        "initial anchor age remains applied"
    );
    let initial = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load initial")
        .expect("initial exists");
    assert_eq!(
        initial.run_at_unix_secs - initial.cache_anchor_at_unix_secs,
        age_secs + initial_delay
    );
    assert!(matches!(
        fixture
            .dispatch(pusher)
            .dispatch_cache_keepalive(first_job)
            .await
            .expect("dispatch"),
        JobOutcome::Done
    ));
    let next_job = fixture.pending_keepalive_job(2).await;
    assert_eq!(
        next_job.refresh_delay_secs, full_delay,
        "{ttl:?}, legacy={legacy}"
    );
    let record = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load rescheduled")
        .expect("rescheduled exists");
    assert_eq!(
        record.run_at_unix_secs - record.cache_anchor_at_unix_secs,
        full_delay
    );
    assert_eq!(
        record.expires_at_unix_secs - record.cache_anchor_at_unix_secs,
        ttl.as_secs()
    );
}

#[tokio::test]
async fn cache_keepalive_hit_reschedule_restores_full_five_minute_cadence() {
    assert_renewal_cadence(cc_lb_storage_api::CacheTtl::Ttl5m, 120, 30, 150, 270, false).await;
}

#[tokio::test]
async fn cache_keepalive_hit_reschedule_restores_full_one_hour_cadence() {
    assert_renewal_cadence(
        cc_lb_storage_api::CacheTtl::Ttl1h,
        120,
        300,
        3_180,
        3_300,
        false,
    )
    .await;
}

#[tokio::test]
async fn cache_keepalive_hit_reschedule_recovers_from_saturated_initial_delay() {
    assert_renewal_cadence(cc_lb_storage_api::CacheTtl::Ttl5m, 280, 30, 1, 270, false).await;
}

#[tokio::test]
async fn cache_keepalive_hit_reschedule_saturates_configured_lead_time() {
    for ttl in [
        cc_lb_storage_api::CacheTtl::Ttl5m,
        cc_lb_storage_api::CacheTtl::Ttl1h,
    ] {
        assert_renewal_cadence(ttl, 0, 7_200, 1, 1, false).await;
    }
}

#[tokio::test]
async fn cache_keepalive_hit_reschedule_legacy_config_uses_default_lead_times() {
    for (ttl, initial, full) in [
        (cc_lb_storage_api::CacheTtl::Ttl5m, 150, 270),
        (cc_lb_storage_api::CacheTtl::Ttl1h, 3_180, 3_300),
    ] {
        assert_renewal_cadence(ttl, 120, 17, initial, full, true).await;
    }
}

#[tokio::test]
async fn cache_keepalive_renewal_event_preserves_thread_in_storage_queries_and_lifecycle() {
    use cc_lb_engine::cache_keepalive::SessionKey;
    let principal = Uuid::from_u128(1);
    let cases = [
        (
            SessionKey::from_thread_id(principal, "thread-1"),
            Some("thread-1"),
        ),
        (
            SessionKey::from_thread_and_agent(principal, "thread-1", "agent-a"),
            Some("thread-1"),
        ),
        (
            SessionKey::from_thread_id(principal, "a/agent:b"),
            Some("a/agent:b"),
        ),
        (
            SessionKey::from_thread_and_agent(principal, "a%2Fagent:b", "agent/y"),
            Some("a%2Fagent:b"),
        ),
        (
            SessionKey::from_thread_and_agent(principal, "100%/x%25", "agent%/y"),
            Some("100%/x%25"),
        ),
        (
            SessionKey::from_thread_id(principal, "スレッド-é"),
            Some("スレッド-é"),
        ),
        (
            SessionKey::from_cache_prefix_hash(principal, "prefix/with%escapes"),
            None,
        ),
    ];
    for (key, expected_thread) in cases {
        let fixture = Fixture::new().await;
        let bus = fixture.event_bus();
        let mut lifecycle_rx = bus.subscribe_lifecycle();
        let pusher: Arc<dyn CacheKeepaliveTaskPusher> = Arc::new(fixture.backend.backend.clone());
        let mut request = fixture.enqueue_request();
        request.session_key_hash = key.to_string();
        fixture
            .enqueuer(Arc::clone(&pusher))
            .enqueue_cache_keepalive(request)
            .await
            .expect("enqueue");
        let job = fixture.pending_keepalive_job(1).await;
        assert!(matches!(
            fixture
                .dispatch(pusher)
                .dispatch_cache_keepalive(job)
                .await
                .expect("dispatch"),
            JobOutcome::Done
        ));
        let events = stored_request_events(&fixture).await;
        assert_eq!(events.len(), 1);
        let cursor = fixture
            .storage
            .current_request_event_cursor()
            .await
            .expect("cursor");
        let filtered = fixture
            .storage
            .query_request_events_between_cursors(
                0,
                cursor,
                10,
                &cc_lb_storage_api::RequestEventStreamFilters {
                    thread_id: Some(expected_thread.unwrap_or("unrelated-thread").to_owned()),
                    ..Default::default()
                },
            )
            .await
            .expect("thread filtered event query");
        assert_eq!(
            filtered.len(),
            usize::from(expected_thread.is_some()),
            "{key}"
        );
        assert_eq!(events[0].thread_id.as_deref(), expected_thread, "{key}");
        if let Some((_, filtered_event)) = filtered.first() {
            assert_eq!(filtered_event.event_id, events[0].event_id);
        }
        let mut parse_threads = Vec::new();
        while let Ok(event) = lifecycle_rx.try_recv() {
            if let LifecycleEvent::ParseCompleted {
                event_id,
                result: Ok(info),
            } = event
            {
                assert_eq!(Some(event_id.as_str()), events[0].event_id.as_deref());
                parse_threads.push(info.thread_id);
            }
        }
        assert_eq!(
            parse_threads,
            vec![expected_thread.map(str::to_owned)],
            "{key}"
        );
    }
}

#[tokio::test]
async fn durable_cache_keepalive_job_reschedules_without_rewriting_payload() {
    let fixture = Fixture::new().await;
    let pusher: Arc<dyn CacheKeepaliveTaskPusher> = Arc::new(fixture.backend.backend.clone());
    let enqueuer = fixture.enqueuer(Arc::clone(&pusher));
    enqueuer
        .enqueue_cache_keepalive(fixture.enqueue_request())
        .await
        .expect("enqueue durable keepalive");
    let initial_payload = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load initial session")
        .expect("initial session exists")
        .encrypted_payload;
    let first_job = fixture.pending_keepalive_job(1).await;
    let dispatch = fixture.dispatch(Arc::clone(&pusher));

    let first_outcome = dispatch
        .dispatch_cache_keepalive(first_job)
        .await
        .expect("dispatch first cache keepalive job");

    assert!(matches!(first_outcome, JobOutcome::Done));
    let first_record = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load first rescheduled session")
        .expect("first rescheduled session exists");
    assert_eq!(first_record.generation, 2);
    assert_eq!(first_record.status, CacheKeepaliveSessionStatus::Active);
    assert_eq!(first_record.encrypted_payload, initial_payload);

    let second_job = fixture.pending_keepalive_job(2).await;
    let second_outcome = dispatch
        .dispatch_cache_keepalive(second_job)
        .await
        .expect("dispatch second cache keepalive job");

    assert!(matches!(second_outcome, JobOutcome::Done));
    let second_record = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load second rescheduled session")
        .expect("second rescheduled session exists");
    assert_eq!(second_record.generation, 3);
    assert_eq!(second_record.status, CacheKeepaliveSessionStatus::Active);
    assert_eq!(second_record.encrypted_payload, initial_payload);
    fixture.pending_keepalive_job(3).await;

    let requests = fixture.http.requests.lock().expect("requests lock").clone();
    assert_eq!(requests.len(), 2);
    assert_eq!(
        requests[0].url,
        "http://fake-upstream.local/v1/messages?beta=true"
    );
    assert_eq!(requests[0].headers["x-api-key"], "sk-ant-rotated");
    assert_eq!(requests[0].headers["anthropic-version"], "2023-06-01");
    let body: Value = serde_json::from_slice(&requests[0].body).expect("body json");
    assert_eq!(body["max_tokens"], 0);
    assert!(body.get("stream").is_none());
    let signer_calls = fixture.signer_calls.lock().expect("signer calls lock");
    assert_eq!(signer_calls.as_slice(), &["fake-upstream", "fake-upstream"]);
}

#[tokio::test]
async fn legacy_cache_keepalive_payload_migrates_on_first_cache_hit() {
    let fixture = Fixture::new().await;
    let pusher: Arc<dyn CacheKeepaliveTaskPusher> = Arc::new(fixture.backend.backend.clone());
    let enqueuer = fixture.enqueuer(Arc::clone(&pusher));
    let request = fixture.enqueue_request();
    let legacy_plaintext =
        serde_json::to_vec(&request.snapshot.to_persisted()).expect("serialize legacy payload");
    enqueuer
        .enqueue_cache_keepalive(request)
        .await
        .expect("enqueue durable keepalive");
    let legacy_payload = fixture.encrypt_generation_payload(1, &legacy_plaintext);
    fixture
        .replace_encrypted_payload(legacy_payload.clone())
        .await;
    let dispatch = fixture.dispatch(Arc::clone(&pusher));

    let first_outcome = dispatch
        .dispatch_cache_keepalive(fixture.pending_keepalive_job(1).await)
        .await
        .expect("dispatch legacy cache keepalive job");

    assert!(matches!(first_outcome, JobOutcome::Done));
    let migrated = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load migrated session")
        .expect("migrated session exists");
    assert_eq!(migrated.generation, 2);
    assert_ne!(migrated.encrypted_payload, legacy_payload);

    let second_outcome = dispatch
        .dispatch_cache_keepalive(fixture.pending_keepalive_job(2).await)
        .await
        .expect("dispatch migrated cache keepalive job");

    assert!(matches!(second_outcome, JobOutcome::Done));
    let rescheduled = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load rescheduled migrated session")
        .expect("rescheduled migrated session exists");
    assert_eq!(rescheduled.generation, 3);
    assert_eq!(rescheduled.encrypted_payload, migrated.encrypted_payload);
}

#[tokio::test]
async fn raw_v2_json_cache_keepalive_payload_migrates_once_after_refresh()
-> Result<(), Box<dyn std::error::Error>> {
    assert_v2_json_cache_keepalive_payload_migrates_once_after_refresh(0).await
}

#[tokio::test]
async fn zstd_v2_json_cache_keepalive_payload_migrates_once_after_refresh()
-> Result<(), Box<dyn std::error::Error>> {
    assert_v2_json_cache_keepalive_payload_migrates_once_after_refresh(1).await
}

async fn assert_v2_json_cache_keepalive_payload_migrates_once_after_refresh(
    codec: u8,
) -> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new().await;
    let pusher: Arc<dyn CacheKeepaliveTaskPusher> = Arc::new(fixture.backend.backend.clone());
    let enqueuer = fixture.enqueuer(Arc::clone(&pusher));
    let mut request = fixture.enqueue_request();
    request.params.max_refreshes = 4;
    request.config_snapshot.max_refreshes_per_session = 4;
    let expected_snapshot = request.snapshot.to_persisted();
    // The cached system replaces the uncached message with "." before persistence.
    // The legacy JSON envelope retains those trimmed body bytes and their key order.
    assert_eq!(
        expected_snapshot.body.as_slice(),
        br#"{"model":"claude-test","max_tokens":32,"stream":true,"system":[{"type":"text","text":"cached prompt","cache_control":{"type":"ephemeral"}}],"messages":[{"role":"user","content":"."}]}"#,
    );
    let expected_wire_body = br#"{"model":"claude-test","max_tokens":0,"system":[{"type":"text","text":"cached prompt","cache_control":{"type":"ephemeral"}}],"messages":[{"role":"user","content":"."}]}"#;
    enqueuer.enqueue_cache_keepalive(request).await?;
    let dispatch = fixture.dispatch(Arc::clone(&pusher));

    assert!(matches!(
        dispatch
            .dispatch_cache_keepalive(fixture.pending_keepalive_job(1).await)
            .await?,
        JobOutcome::Done
    ));
    let before_migration = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await?
        .ok_or("session is missing before migration")?;
    assert_eq!(before_migration.generation, 2);
    assert_eq!(before_migration.refresh_count, 1);
    let payload_generation = before_migration
        .generation
        .checked_sub(u64::from(before_migration.refresh_count))
        .ok_or("fixture payload generation underflow")?;
    assert_eq!(payload_generation, 1);

    let encoded = serde_json::to_vec(&expected_snapshot)?;
    let encoded_len = u64::try_from(encoded.len())?;
    let plaintext = match codec {
        0 => encoded,
        1 => zstd::bulk::compress(&encoded, 1)?,
        _ => return Err("fixture requires a pre-fix v2 JSON codec".into()),
    };
    // Fixture's AEAD field is private to support; reproduce its fixed test key
    // locally instead of adding a production or fixture visibility seam.
    let aead = cc_lb_aead::AeadService::from_master_key([9; 32]);
    let aad = format!(
        "cache_keepalive_snapshot:v2:principal:session-hash:{}:{payload_generation}:{codec}:{encoded_len}",
        fixture.upstream_id
    );
    let ciphertext = aead.encrypt(&plaintext, aad.as_bytes())?;
    let mut old_payload = b"CCLBCKA2".to_vec();
    old_payload.push(codec);
    old_payload.extend_from_slice(&payload_generation.to_be_bytes());
    old_payload.extend_from_slice(&encoded_len.to_be_bytes());
    old_payload.extend_from_slice(&ciphertext);
    fixture.replace_encrypted_payload(old_payload.clone()).await;

    assert!(matches!(
        dispatch
            .dispatch_cache_keepalive(
                fixture
                    .pending_keepalive_job(before_migration.generation)
                    .await,
            )
            .await?,
        JobOutcome::Done
    ));
    let migrated = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await?
        .ok_or("migrated session is missing")?;
    assert_eq!(migrated.generation, 3);
    assert_eq!(migrated.refresh_count, 2);
    assert_eq!(migrated.status, CacheKeepaliveSessionStatus::Active);
    assert_ne!(migrated.encrypted_payload, old_payload);
    assert_eq!(
        migrated.encrypted_payload.get(..8),
        Some(b"CCLBCKA2".as_slice())
    );
    assert!(matches!(
        migrated.encrypted_payload.get(8),
        Some(&2) | Some(&3)
    ));
    let decoded = crate::cache_keepalive_payload::decrypt_cache_keepalive_payload(
        &aead,
        "principal",
        "session-hash",
        fixture.upstream_id,
        migrated.generation,
        migrated.refresh_count,
        &migrated.encrypted_payload,
    )?;
    assert_eq!(decoded.snapshot, expected_snapshot);
    assert!(!decoded.needs_rewrite);

    assert!(matches!(
        dispatch
            .dispatch_cache_keepalive(fixture.pending_keepalive_job(migrated.generation).await)
            .await?,
        JobOutcome::Done
    ));
    let rescheduled = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await?
        .ok_or("rescheduled migrated session is missing")?;
    assert_eq!(rescheduled.generation, 4);
    assert_eq!(rescheduled.refresh_count, 3);
    assert_eq!(rescheduled.status, CacheKeepaliveSessionStatus::Active);
    assert_eq!(rescheduled.encrypted_payload, migrated.encrypted_payload);
    fixture.pending_keepalive_job(rescheduled.generation).await;

    let requests = fixture
        .http
        .requests
        .lock()
        .map_err(|_| "requests lock is poisoned")?
        .clone();
    assert_eq!(requests.len(), 3);
    for request in &requests {
        assert_eq!(
            request.url,
            "http://fake-upstream.local/v1/messages?beta=true"
        );
        assert_eq!(
            request.headers.get("anthropic-version"),
            Some(&::http::HeaderValue::from_static("2023-06-01"))
        );
        assert_eq!(
            request.headers.get("x-api-key"),
            Some(&::http::HeaderValue::from_static("sk-ant-rotated"))
        );
        assert_eq!(request.body.as_ref(), expected_wire_body.as_slice());
    }
    let first = requests.first().ok_or("initial renewal was not captured")?;
    for request in requests.iter().skip(1) {
        assert_eq!(request.headers, first.headers);
    }
    Ok(())
}

#[tokio::test]
async fn cache_keepalive_missing_accounting_key_terminalizes_without_dispatch() {
    let fixture = Fixture::new().await;
    let pusher: Arc<dyn CacheKeepaliveTaskPusher> = Arc::new(fixture.backend.backend.clone());
    let enqueuer = fixture.enqueuer(Arc::clone(&pusher));
    enqueuer
        .enqueue_cache_keepalive(fixture.enqueue_request())
        .await
        .expect("enqueue durable keepalive");
    let job = fixture.pending_keepalive_job(1).await;
    sqlx::query(
        "UPDATE cache_keepalive_sessions SET accounting_key_id = ? WHERE session_key_hash = ?",
    )
    .bind("missing-accounting-key")
    .bind("session-hash")
    .execute(fixture.storage.pool())
    .await
    .expect("set missing accounting key");
    let dispatch = fixture.dispatch(pusher);

    let outcome = dispatch
        .dispatch_cache_keepalive(job)
        .await
        .expect("dispatch cache keepalive job");

    assert!(matches!(outcome, JobOutcome::Done));
    let record = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load session")
        .expect("session exists");
    assert_eq!(record.status, CacheKeepaliveSessionStatus::Terminal);
    assert_eq!(record.enqueue_state, CacheKeepaliveEnqueueState::Pending);
    assert!(record.running_since_unix_secs.is_none());
    assert!(
        record
            .error
            .as_deref()
            .is_some_and(|error| error.contains("accounting key is unavailable"))
    );
    assert_eq!(
        record.terminal_reason,
        Some(CacheKeepaliveTerminalReason::DispatchError)
    );
    assert!(
        fixture
            .http
            .requests
            .lock()
            .expect("requests lock")
            .is_empty()
    );
    assert!(
        fixture
            .signer_calls
            .lock()
            .expect("signer calls lock")
            .is_empty()
    );
}

#[tokio::test]
async fn cache_keepalive_accounting_key_uses_real_reserve_before_dispatch() {
    let fixture = Fixture::new().await;
    let key_id = create_accounting_key(&fixture, LimitKind::Requests, 0).await;
    let pusher: Arc<dyn CacheKeepaliveTaskPusher> = Arc::new(fixture.backend.backend.clone());
    let enqueuer = fixture.enqueuer(Arc::clone(&pusher));
    let mut request = fixture.enqueue_request();
    request.accounting_key_id = Some(key_id);
    enqueuer
        .enqueue_cache_keepalive(request)
        .await
        .expect("enqueue durable keepalive");
    let job = fixture.pending_keepalive_job(1).await;
    let dispatch = fixture.dispatch(pusher);

    let outcome = dispatch
        .dispatch_cache_keepalive(job)
        .await
        .expect("dispatch cache keepalive job");

    assert!(matches!(outcome, JobOutcome::Done));
    let record = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load session")
        .expect("session exists");
    assert_eq!(record.status, CacheKeepaliveSessionStatus::Terminal);
    assert_eq!(record.enqueue_state, CacheKeepaliveEnqueueState::Pending);
    assert!(record.running_since_unix_secs.is_none());
    assert!(
        record
            .error
            .as_deref()
            .is_some_and(|error| error.contains("Requests")),
        "unexpected reserve error: {:?}",
        record.error
    );
    assert_eq!(
        record.terminal_reason,
        Some(CacheKeepaliveTerminalReason::DispatchError)
    );
    assert!(
        fixture
            .http
            .requests
            .lock()
            .expect("requests lock")
            .is_empty()
    );
    assert!(
        fixture
            .signer_calls
            .lock()
            .expect("signer calls lock")
            .is_empty()
    );
}

#[tokio::test]
async fn cache_keepalive_redelivered_job_dispatches_once() {
    let fixture = Fixture::new().await;
    let pusher: Arc<dyn CacheKeepaliveTaskPusher> = Arc::new(fixture.backend.backend.clone());
    let enqueuer = fixture.enqueuer(Arc::clone(&pusher));
    enqueuer
        .enqueue_cache_keepalive(fixture.enqueue_request())
        .await
        .expect("enqueue durable keepalive");
    let job = fixture.pending_keepalive_job(1).await;
    let dispatch = fixture.dispatch(pusher);

    let first = dispatch
        .dispatch_cache_keepalive(job.clone())
        .await
        .expect("dispatch first delivery");
    let redelivery = dispatch
        .dispatch_cache_keepalive(job)
        .await
        .expect("dispatch redelivery");

    assert!(matches!(first, JobOutcome::Done));
    assert!(matches!(redelivery, JobOutcome::Noop));
    assert_eq!(
        fixture.http.requests.lock().expect("requests lock").len(),
        1
    );
    assert_eq!(
        fixture
            .signer_calls
            .lock()
            .expect("signer calls lock")
            .len(),
        1
    );
}

#[tokio::test]
async fn cache_keepalive_running_turn_does_not_redispatch() {
    let fixture = Fixture::new().await;
    let pusher: Arc<dyn CacheKeepaliveTaskPusher> = Arc::new(fixture.backend.backend.clone());
    let enqueuer = fixture.enqueuer(Arc::clone(&pusher));
    enqueuer
        .enqueue_cache_keepalive(fixture.enqueue_request())
        .await
        .expect("enqueue durable keepalive");
    let job = fixture.pending_keepalive_job(1).await;
    sqlx::query(
        "UPDATE cache_keepalive_sessions SET enqueue_state = 'running', running_since_unix_secs = 1 WHERE session_key_hash = ?",
    )
    .bind("session-hash")
    .execute(fixture.storage.pool())
    .await
    .expect("mark turn running");
    let dispatch = fixture.dispatch(pusher);

    let outcome = dispatch
        .dispatch_cache_keepalive(job)
        .await
        .expect("dispatch running turn");

    assert!(matches!(outcome, JobOutcome::Noop));
    assert!(
        fixture
            .http
            .requests
            .lock()
            .expect("requests lock")
            .is_empty()
    );
    assert!(
        fixture
            .signer_calls
            .lock()
            .expect("signer calls lock")
            .is_empty()
    );
}

#[tokio::test]
async fn hit_reschedule_enqueue_failure_terminalizes_new_generation() {
    let fixture = Fixture::new().await;
    let enqueuer = fixture.enqueuer(Arc::new(fixture.backend.backend.clone()));
    enqueuer
        .enqueue_cache_keepalive(fixture.enqueue_request())
        .await
        .expect("enqueue durable keepalive");
    let first_job = fixture.pending_keepalive_job(1).await;
    let dispatch = fixture.dispatch(Arc::new(FailingPusher));

    let result = dispatch.dispatch_cache_keepalive(first_job).await;

    assert!(result.is_err());
    let record = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load session")
        .expect("session exists");
    assert_eq!(record.generation, 2);
    assert_eq!(record.status, CacheKeepaliveSessionStatus::Terminal);
    assert_eq!(record.enqueue_state, CacheKeepaliveEnqueueState::Pending);
    assert!(record.running_since_unix_secs.is_none());
    assert!(
        record
            .error
            .as_deref()
            .is_some_and(|error| error.contains("forced reschedule enqueue failure")),
        "unexpected reschedule error: {:?}",
        record.error
    );
    assert_eq!(
        record.terminal_reason,
        Some(CacheKeepaliveTerminalReason::DispatchError)
    );
}

#[tokio::test]
async fn cache_keepalive_job_terminalizes_on_decrypt_failure() {
    let fixture = Fixture::new().await;
    let pusher: Arc<dyn CacheKeepaliveTaskPusher> = Arc::new(fixture.backend.backend.clone());
    let enqueuer = fixture.enqueuer(Arc::clone(&pusher));
    enqueuer
        .enqueue_cache_keepalive(fixture.enqueue_request())
        .await
        .expect("enqueue durable keepalive");
    let job = fixture.pending_keepalive_job(1).await;
    fixture.replace_encrypted_payload(vec![0; 32]).await;
    let dispatch = fixture.dispatch(pusher);

    let outcome = dispatch
        .dispatch_cache_keepalive(job)
        .await
        .expect("dispatch cache keepalive job");

    assert!(matches!(outcome, JobOutcome::Done));
    let record = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load session")
        .expect("session exists");
    assert_eq!(record.status, CacheKeepaliveSessionStatus::Terminal);
    assert_eq!(record.enqueue_state, CacheKeepaliveEnqueueState::Pending);
    assert!(record.running_since_unix_secs.is_none());
    assert!(
        record
            .error
            .as_deref()
            .is_some_and(|error| error.contains("payload"))
    );
    assert_eq!(
        record.terminal_reason,
        Some(CacheKeepaliveTerminalReason::DecryptFailed)
    );
    let requests = fixture.http.requests.lock().expect("requests lock").clone();
    assert!(requests.is_empty());
}

#[tokio::test]
async fn cache_keepalive_job_terminalizes_on_deserialization_failure() {
    let fixture = Fixture::new().await;
    let pusher: Arc<dyn CacheKeepaliveTaskPusher> = Arc::new(fixture.backend.backend.clone());
    let enqueuer = fixture.enqueuer(Arc::clone(&pusher));
    enqueuer
        .enqueue_cache_keepalive(fixture.enqueue_request())
        .await
        .expect("enqueue durable keepalive");
    let job = fixture.pending_keepalive_job(1).await;
    let encrypted = fixture.encrypt_generation_payload(1, br#"{}"#);
    fixture.replace_encrypted_payload(encrypted).await;
    let dispatch = fixture.dispatch(pusher);

    let outcome = dispatch
        .dispatch_cache_keepalive(job)
        .await
        .expect("dispatch cache keepalive job");

    assert!(matches!(outcome, JobOutcome::Done));
    let record = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load session")
        .expect("session exists");
    assert_eq!(record.status, CacheKeepaliveSessionStatus::Terminal);
    assert_eq!(record.enqueue_state, CacheKeepaliveEnqueueState::Pending);
    assert!(record.running_since_unix_secs.is_none());
    assert!(
        record
            .error
            .as_deref()
            .is_some_and(|error| !error.is_empty()),
        "unexpected deserialization error: {:?}",
        record.error
    );
    assert_eq!(
        record.terminal_reason,
        Some(CacheKeepaliveTerminalReason::DecryptFailed)
    );
    let requests = fixture.http.requests.lock().expect("requests lock").clone();
    assert!(requests.is_empty());
}

#[tokio::test]
async fn cache_keepalive_miss_terminalizes_session() {
    let fixture = Fixture::new().await;
    fixture.http.return_cache_miss();
    let pusher: Arc<dyn CacheKeepaliveTaskPusher> = Arc::new(fixture.backend.backend.clone());
    let enqueuer = fixture.enqueuer(Arc::clone(&pusher));
    enqueuer
        .enqueue_cache_keepalive(fixture.enqueue_request())
        .await
        .expect("enqueue durable keepalive");
    let job = fixture.pending_keepalive_job(1).await;
    let dispatch = fixture.dispatch(pusher);

    let outcome = dispatch
        .dispatch_cache_keepalive(job)
        .await
        .expect("dispatch cache keepalive job");

    assert!(matches!(outcome, JobOutcome::Done));
    let record = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load session")
        .expect("session exists");
    assert_eq!(record.status, CacheKeepaliveSessionStatus::Terminal);
    assert_eq!(record.enqueue_state, CacheKeepaliveEnqueueState::Pending);
    assert!(record.running_since_unix_secs.is_none());
    assert!(record.error.is_none());
    assert_eq!(
        record.terminal_reason,
        Some(CacheKeepaliveTerminalReason::CacheMiss)
    );
}

#[tokio::test]
async fn cache_keepalive_unsupported_provider_terminalizes_session() {
    let fixture = Fixture::new_with_upstream_kind(UpstreamKind::AnthropicApiKey).await;
    let pusher: Arc<dyn CacheKeepaliveTaskPusher> = Arc::new(fixture.backend.backend.clone());
    let enqueuer = fixture.enqueuer(Arc::clone(&pusher));
    enqueuer
        .enqueue_cache_keepalive(fixture.enqueue_request())
        .await
        .expect("enqueue durable keepalive");
    let job = fixture.pending_keepalive_job(1).await;
    let dispatch = fixture.dispatch(pusher);

    let outcome = dispatch
        .dispatch_cache_keepalive(job)
        .await
        .expect("dispatch cache keepalive job");

    assert!(matches!(outcome, JobOutcome::Done));
    let record = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load session")
        .expect("session exists");
    assert_eq!(record.status, CacheKeepaliveSessionStatus::Terminal);
    assert_eq!(record.enqueue_state, CacheKeepaliveEnqueueState::Pending);
    assert!(record.running_since_unix_secs.is_none());
    assert!(
        record
            .error
            .as_deref()
            .is_some_and(|error| error.contains("AnthropicApiKey"))
    );
    assert_eq!(
        record.terminal_reason,
        Some(CacheKeepaliveTerminalReason::UnsupportedProvider)
    );
    let requests = fixture.http.requests.lock().expect("requests lock").clone();
    assert!(requests.is_empty());
}

#[tokio::test]
async fn cache_keepalive_dispatch_error_terminalizes_session() {
    let fixture = Fixture::new().await;
    fixture.http.return_status(
        StatusCode::INTERNAL_SERVER_ERROR,
        serde_json::json!({"error":"boom"}),
    );
    let pusher: Arc<dyn CacheKeepaliveTaskPusher> = Arc::new(fixture.backend.backend.clone());
    let enqueuer = fixture.enqueuer(Arc::clone(&pusher));
    enqueuer
        .enqueue_cache_keepalive(fixture.enqueue_request())
        .await
        .expect("enqueue durable keepalive");
    let job = fixture.pending_keepalive_job(1).await;
    let dispatch = fixture.dispatch(pusher);

    let outcome = dispatch
        .dispatch_cache_keepalive(job)
        .await
        .expect("dispatch cache keepalive job");

    assert!(matches!(outcome, JobOutcome::Done));
    let record = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load session")
        .expect("session exists");
    assert_eq!(record.status, CacheKeepaliveSessionStatus::Terminal);
    assert_eq!(record.enqueue_state, CacheKeepaliveEnqueueState::Pending);
    assert!(record.running_since_unix_secs.is_none());
    assert!(
        record
            .error
            .as_deref()
            .is_some_and(|error| error.contains("500"))
    );
    assert_eq!(
        record.terminal_reason,
        Some(CacheKeepaliveTerminalReason::DispatchError)
    );
}
#[tokio::test]
async fn cache_keepalive_dispatch_error_persists_message_and_logs_warning() {
    let fixture = Fixture::new().await;
    fixture.http.return_status(
        StatusCode::BAD_REQUEST,
        serde_json::json!({"error":"thinking disabled does not accept display"}),
    );
    let pusher: Arc<dyn CacheKeepaliveTaskPusher> = Arc::new(fixture.backend.backend.clone());
    let enqueuer = fixture.enqueuer(Arc::clone(&pusher));
    enqueuer
        .enqueue_cache_keepalive(fixture.enqueue_request())
        .await
        .expect("enqueue durable keepalive");
    let job = fixture.pending_keepalive_job(1).await;
    let capture = CacheKeepaliveWarnCapture::default();
    let subscriber = tracing_subscriber::registry().with(capture.clone());
    let _guard = tracing::subscriber::set_default(subscriber);

    let outcome = fixture
        .dispatch(pusher)
        .dispatch_cache_keepalive(job)
        .await
        .expect("dispatch cache keepalive job");

    assert!(matches!(outcome, JobOutcome::Done));
    let record = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load session")
        .expect("session exists");
    assert_eq!(record.status, CacheKeepaliveSessionStatus::Terminal);
    assert_eq!(record.enqueue_state, CacheKeepaliveEnqueueState::Pending);
    assert!(record.running_since_unix_secs.is_none());
    let expected_error =
        r#"status=400 Bad Request body={"error":"thinking disabled does not accept display"}"#;
    assert!(
        record
            .error
            .as_deref()
            .is_some_and(|error| error.contains(expected_error))
    );
    assert_eq!(
        record.terminal_reason,
        Some(CacheKeepaliveTerminalReason::DispatchError)
    );
    let events = capture.events.lock().expect("warning capture lock").clone();
    assert!(events.iter().any(|event| {
        event.contains("session_key_hash=session-hash")
            && event.contains("generation=1")
            && event.contains("terminal_reason=dispatch_error")
            && event.contains("thinking disabled does not accept display")
    }));
}

#[tokio::test]
async fn cache_keepalive_hit_terminalizes_when_max_refreshes_reached() {
    let fixture = Fixture::new().await;
    let pusher: Arc<dyn CacheKeepaliveTaskPusher> = Arc::new(fixture.backend.backend.clone());
    let enqueuer = fixture.enqueuer(Arc::clone(&pusher));
    enqueuer
        .enqueue_cache_keepalive(fixture.enqueue_request())
        .await
        .expect("enqueue durable keepalive");
    let mut job = fixture.pending_keepalive_job(1).await;
    job.max_refreshes = 1;
    let dispatch = fixture.dispatch(pusher);

    let outcome = dispatch
        .dispatch_cache_keepalive(job)
        .await
        .expect("dispatch cache keepalive job");

    assert!(matches!(outcome, JobOutcome::Done));
    let record = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load session")
        .expect("session exists");
    assert_eq!(record.generation, 1);
    assert_eq!(record.status, CacheKeepaliveSessionStatus::Terminal);
    assert_eq!(record.enqueue_state, CacheKeepaliveEnqueueState::Pending);
    assert!(record.running_since_unix_secs.is_none());
    assert!(record.error.is_none());
    assert_eq!(
        record.terminal_reason,
        Some(CacheKeepaliveTerminalReason::MaxRefreshes)
    );
    let requests = fixture.http.requests.lock().expect("requests lock").clone();
    assert_eq!(requests.len(), 1);
}

#[tokio::test]
async fn cache_keepalive_hit_terminalizes_when_max_duration_reached() {
    let fixture = Fixture::new().await;
    let pusher: Arc<dyn CacheKeepaliveTaskPusher> = Arc::new(fixture.backend.backend.clone());
    let enqueuer = fixture.enqueuer(Arc::clone(&pusher));
    enqueuer
        .enqueue_cache_keepalive(fixture.enqueue_request())
        .await
        .expect("enqueue durable keepalive");
    let mut job = fixture.pending_keepalive_job(1).await;
    job.max_total_duration_secs = 0;
    let dispatch = fixture.dispatch(pusher);

    let outcome = dispatch
        .dispatch_cache_keepalive(job)
        .await
        .expect("dispatch cache keepalive job");

    assert!(matches!(outcome, JobOutcome::Done));
    let record = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load session")
        .expect("session exists");
    assert_eq!(record.generation, 1);
    assert_eq!(record.status, CacheKeepaliveSessionStatus::Terminal);
    assert_eq!(record.enqueue_state, CacheKeepaliveEnqueueState::Pending);
    assert!(record.running_since_unix_secs.is_none());
    assert!(record.error.is_none());
    assert_eq!(
        record.terminal_reason,
        Some(CacheKeepaliveTerminalReason::MaxDuration)
    );
    let requests = fixture.http.requests.lock().expect("requests lock").clone();
    assert_eq!(requests.len(), 1);
}

#[tokio::test]
async fn cache_keepalive_durable_cancel_makes_queued_old_generation_noop() {
    let fixture = Fixture::new().await;
    let pusher: Arc<dyn CacheKeepaliveTaskPusher> = Arc::new(fixture.backend.backend.clone());
    let enqueuer = fixture.enqueuer(Arc::clone(&pusher));
    enqueuer
        .enqueue_cache_keepalive(fixture.enqueue_request())
        .await
        .expect("enqueue durable keepalive");
    let queued_job = fixture.pending_keepalive_job(1).await;
    enqueuer
        .cancel_cache_keepalive(cc_lb_engine::cache_keepalive::CacheKeepaliveCancelRequest {
            session_key_hash: "session-hash".to_owned(),
            reason: cc_lb_engine::cache_keepalive::CancelReason::UserTurnDetected,
            expected_generation: None,
            cache_anchor_age: Duration::ZERO,
        })
        .await
        .expect("cancel durable keepalive");
    let dispatch = fixture.dispatch(pusher);

    let outcome = dispatch
        .dispatch_cache_keepalive(queued_job)
        .await
        .expect("dispatch terminalized keepalive job");

    assert!(matches!(outcome, JobOutcome::Noop));
    let requests = fixture.http.requests.lock().expect("requests lock").clone();
    assert!(requests.is_empty());
}

#[tokio::test]
async fn cache_keepalive_new_real_request_makes_queued_old_generation_noop() {
    let fixture = Fixture::new().await;
    let pusher: Arc<dyn CacheKeepaliveTaskPusher> = Arc::new(fixture.backend.backend.clone());
    let enqueuer = fixture.enqueuer(Arc::clone(&pusher));
    enqueuer
        .enqueue_cache_keepalive(fixture.enqueue_request())
        .await
        .expect("enqueue initial durable keepalive");
    let queued_old_job = fixture.pending_keepalive_job(1).await;
    enqueuer
        .enqueue_cache_keepalive(fixture.enqueue_request())
        .await
        .expect("replace durable keepalive from new real request");
    fixture.pending_keepalive_job(2).await;
    let dispatch = fixture.dispatch(pusher);

    let outcome = dispatch
        .dispatch_cache_keepalive(queued_old_job)
        .await
        .expect("dispatch stale keepalive job");

    assert!(matches!(outcome, JobOutcome::Noop));
    let record = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load session")
        .expect("session exists");
    assert_eq!(record.generation, 2);
    assert_eq!(record.status, CacheKeepaliveSessionStatus::Active);
    assert_eq!(record.refresh_count, 0);
    let requests = fixture.http.requests.lock().expect("requests lock").clone();
    assert!(requests.is_empty());
}

#[tokio::test]
async fn cache_keepalive_expired_session_terminalizes_before_dispatch() {
    let fixture = Fixture::new().await;
    let pusher: Arc<dyn CacheKeepaliveTaskPusher> = Arc::new(fixture.backend.backend.clone());
    let enqueuer = fixture.enqueuer(Arc::clone(&pusher));
    enqueuer
        .enqueue_cache_keepalive(fixture.enqueue_request())
        .await
        .expect("enqueue durable keepalive");
    let job = fixture.pending_keepalive_job(1).await;
    fixture.expire_session().await;
    let dispatch = fixture.dispatch(pusher);

    let outcome = dispatch
        .dispatch_cache_keepalive(job)
        .await
        .expect("dispatch cache keepalive job");

    assert!(matches!(outcome, JobOutcome::Done));
    let record = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load session")
        .expect("session exists");
    assert_eq!(record.status, CacheKeepaliveSessionStatus::Terminal);
    assert_eq!(record.enqueue_state, CacheKeepaliveEnqueueState::Pending);
    assert!(record.running_since_unix_secs.is_none());
    assert!(record.error.is_none());
    assert_eq!(
        record.terminal_reason,
        Some(CacheKeepaliveTerminalReason::Expired)
    );
    let requests = fixture.http.requests.lock().expect("requests lock").clone();
    assert!(requests.is_empty());
}

#[tokio::test]
async fn cache_keepalive_revoked_principal_terminalizes_before_dispatch() {
    let fixture = Fixture::new().await;
    let pusher: Arc<dyn CacheKeepaliveTaskPusher> = Arc::new(fixture.backend.backend.clone());
    let enqueuer = fixture.enqueuer(Arc::clone(&pusher));
    enqueuer
        .enqueue_cache_keepalive(fixture.enqueue_request())
        .await
        .expect("enqueue durable keepalive");
    let job = fixture.pending_keepalive_job(1).await;
    fixture.revoke_principal();
    let dispatch = fixture.dispatch(pusher);

    let outcome = dispatch
        .dispatch_cache_keepalive(job)
        .await
        .expect("dispatch cache keepalive job");

    assert!(matches!(outcome, JobOutcome::Done));
    let record = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load session")
        .expect("session exists");
    assert_eq!(record.status, CacheKeepaliveSessionStatus::Terminal);
    assert_eq!(record.enqueue_state, CacheKeepaliveEnqueueState::Pending);
    assert!(record.running_since_unix_secs.is_none());
    assert!(record.error.is_none());
    assert_eq!(
        record.terminal_reason,
        Some(CacheKeepaliveTerminalReason::Cancelled)
    );
    let requests = fixture.http.requests.lock().expect("requests lock").clone();
    assert!(requests.is_empty());
}

#[tokio::test]
async fn worker_dispatches_published_job_before_producer_marks_enqueued() {
    use std::panic::AssertUnwindSafe;

    use cc_lb_config::SchedulerConfig;
    use cc_lb_scheduler::worker::{
        AdaptiveDispatchFn, AdaptiveJob, Filter, SchedulerCtx, SchedulerPushTask, TaskStatus,
    };
    use futures_util::FutureExt as _;
    use tokio::sync::{Notify, mpsc};

    struct HoldAfterRealPush {
        backend: crate::scheduler_factory::SchedulerBackend,
        published: Arc<Notify>,
        resume: Arc<Notify>,
    }

    #[async_trait]
    impl CacheKeepaliveTaskPusher for HoldAfterRealPush {
        async fn push_cache_keepalive_task(
            &self,
            task: SchedulerPushTask<AdaptiveJob>,
        ) -> SchedulerResult<()> {
            self.backend.push_keepalive_task(task).await?;
            self.published.notify_one();
            self.resume.notified().await;
            Ok(())
        }
    }

    let fixture = Fixture::new().await;
    fixture.http.return_cache_hit_usage(0, 0, 10);
    let published = Arc::new(Notify::new());
    let resume = Arc::new(Notify::new());
    let marked = Arc::new(Notify::new());
    let pusher: Arc<dyn CacheKeepaliveTaskPusher> = Arc::new(HoldAfterRealPush {
        backend: fixture.backend.backend.clone(),
        published: Arc::clone(&published),
        resume: Arc::clone(&resume),
    });
    let enqueuer = fixture.enqueuer(pusher);
    let dispatch = fixture.dispatch(Arc::new(fixture.backend.backend.clone()));
    let mut request = fixture.enqueue_request();
    request.params.delay = Duration::from_secs(1);
    request.params.max_refreshes = 1;
    let cancel = CancellationToken::new();
    let mut handles = Vec::new();

    let result = AssertUnwindSafe(async {
        tokio::time::timeout(Duration::from_secs(10), async {
            let producer = async {
                let result = enqueuer.enqueue_cache_keepalive(request).await;
                marked.notify_one();
                result
            };
            let observer = async {
                published.notified().await;
                let before = fixture
                    .storage
                    .get_cache_keepalive_session("session-hash")
                    .await
                    .expect("read committed pending session")
                    .expect("published session exists");
                assert_eq!(before.status, CacheKeepaliveSessionStatus::Active);
                assert_eq!(before.enqueue_state, CacheKeepaliveEnqueueState::Pending);
                assert!(!before.encrypted_payload.is_empty());
                let tasks = fixture
                    .backend
                    .backend
                    .list_keepalive_tasks(&Filter {
                        status: Some(TaskStatus::Pending),
                        page: 1,
                        page_size: Some(10),
                    })
                    .await
                    .expect("read real queue publication");
                assert_eq!(tasks.len(), 1);
                assert_eq!(
                    tasks[0].idempotency_key.as_deref(),
                    Some(before.current_job_key.as_str())
                );
                assert_eq!(tasks[0].run_at_unix_secs, before.run_at_unix_secs);
                let expected_job = fixture.pending_keepalive_job(before.generation).await;
                assert_eq!(expected_job.session_key_hash, before.session_key_hash);
                assert_eq!(expected_job.generation, before.generation);
                assert_eq!(expected_job.refresh_delay_secs, 1);
                assert_eq!(expected_job.max_refreshes, 1);

                let (outcome_tx, mut outcome_rx) = mpsc::unbounded_channel();
                let cron_dispatch = dispatch.clone().cron_dispatch();
                let adaptive_dispatch: AdaptiveDispatchFn = Arc::new(move |job| {
                    let dispatch = dispatch.clone();
                    let outcome_tx = outcome_tx.clone();
                    Box::pin(async move {
                        let observed_job = match &job {
                            AdaptiveJob::CacheKeepalive(job) => Some(job.clone()),
                            _ => None,
                        };
                        let dispatched_at = cc_lb_clock::unix_secs(SystemTime::now());
                        let result = dispatch.dispatch_entity(job).await;
                        if let (Some(job), Ok(outcome)) = (observed_job, &result) {
                            let _ = outcome_tx.send((job, outcome.clone(), dispatched_at));
                        }
                        result
                    })
                });
                handles = fixture
                    .backend
                    .backend
                    .spawn(
                        SchedulerCtx::new(
                            SchedulerConfig::default(),
                            adaptive_dispatch,
                            cron_dispatch,
                            Arc::new(SystemClock),
                        ),
                        cancel.clone(),
                    )
                    .await
                    .expect("spawn actual SQLite scheduler consumers");
                let (job, outcome, dispatched_at) = outcome_rx
                    .recv()
                    .await
                    .expect("actual worker reports its dispatcher result");
                assert_eq!(job, expected_job);
                assert!(
                    before.run_at_unix_secs <= dispatched_at,
                    "worker waited until due"
                );
                assert!(
                    dispatched_at < before.expires_at_unix_secs,
                    "session was live"
                );
                assert_eq!(outcome, JobOutcome::Done);
                assert_eq!(
                    fixture.http.requests.lock().expect("requests lock").len(),
                    1
                );

                let done = loop {
                    let tasks = fixture
                        .backend
                        .backend
                        .list_keepalive_tasks(&Filter {
                            status: Some(TaskStatus::Done),
                            page: 1,
                            page_size: Some(10),
                        })
                        .await
                        .expect("observe actual queue acknowledgment");
                    if let Some(task) = tasks.into_iter().find(|task| {
                        task.idempotency_key.as_deref() == Some(before.current_job_key.as_str())
                    }) {
                        break task;
                    }
                    tokio::task::yield_now().await;
                };
                let actual_job = match done.args {
                    AdaptiveJob::CacheKeepalive(actual) => actual,
                    other => panic!("unexpected acknowledged job: {other:?}"),
                };
                assert_eq!(actual_job, expected_job);
                let terminal = fixture
                    .storage
                    .get_cache_keepalive_session("session-hash")
                    .await
                    .expect("read terminal session before producer resumes")
                    .expect("terminal session exists");
                assert_eq!(terminal.status, CacheKeepaliveSessionStatus::Terminal);
                assert_eq!(
                    terminal.terminal_reason,
                    Some(CacheKeepaliveTerminalReason::MaxRefreshes)
                );
                assert_eq!(terminal.enqueue_state, CacheKeepaliveEnqueueState::Pending);

                resume.notify_one();
                marked.notified().await;
                let after = fixture
                    .storage
                    .get_cache_keepalive_session("session-hash")
                    .await
                    .expect("read session after producer resumes")
                    .expect("session remains");
                assert_eq!(after.status, CacheKeepaliveSessionStatus::Terminal);
                assert_eq!(after.enqueue_state, CacheKeepaliveEnqueueState::Pending);
                assert_eq!(
                    after.terminal_reason,
                    Some(CacheKeepaliveTerminalReason::MaxRefreshes)
                );
                assert_eq!(after.generation, before.generation);
                assert_eq!(
                    fixture.http.requests.lock().expect("requests lock").len(),
                    1
                );
                let pending = fixture
                    .backend
                    .backend
                    .list_keepalive_tasks(&Filter {
                        status: Some(TaskStatus::Pending),
                        page: 1,
                        page_size: Some(10),
                    })
                    .await
                    .expect("read pending jobs after actual ACK");
                assert!(pending.is_empty(), "no queued retry remains after ACK");
            };
            let (enqueue_result, ()) = tokio::join!(producer, observer);
            enqueue_result.expect("producer completes after worker terminalization");
        })
        .await
        .expect("real worker completes the controlled schedule within ten seconds");
    })
    .catch_unwind()
    .await;

    cancel.cancel();
    let mut shutdown_failed = false;
    for mut handle in handles {
        match tokio::time::timeout(Duration::from_secs(5), &mut handle).await {
            Ok(Ok(())) => {}
            Ok(Err(_)) => shutdown_failed = true,
            Err(_) => {
                handle.abort();
                let _ = handle.await;
                shutdown_failed = true;
            }
        }
    }
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
    assert!(
        !shutdown_failed,
        "all owned scheduler workers must join cleanly"
    );
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_idle_keepalive_worker_renews_session_before_expiry() {
    use std::panic::AssertUnwindSafe;

    use cc_lb_config::SchedulerConfig;
    use cc_lb_engine::cache_keepalive::ScheduleParams;
    use cc_lb_scheduler::worker::{AdaptiveDispatchFn, CronDispatchFn, PostgresSchedulerBackend};
    use futures_util::FutureExt as _;
    use sqlx::postgres::{PgConnectOptions, PgPoolOptions};

    let Ok(database_url) = std::env::var("DATABASE_URL") else {
        eprintln!("SKIP: DATABASE_URL not set; skipping postgres keepalive QA-09");
        return;
    };
    let options: PgConnectOptions = database_url.parse().expect("parse postgres test URL");
    let admin_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(options.clone())
        .await
        .expect("connect postgres admin");
    let database_name = format!("cclb_keepalive_qa09_{}", Uuid::new_v4().simple());
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE DATABASE \"{database_name}\""
    )))
    .execute(&admin_pool)
    .await
    .expect("create isolated keepalive database");
    let pg_pool = PgPoolOptions::new()
        .max_connections(16)
        .acquire_timeout(Duration::from_secs(5))
        .connect_lazy_with(options.clone().database(&database_name));
    let cancel = CancellationToken::new();
    let mut handles = Vec::new();

    // Catch assertions as well as timeouts so the isolated database is dropped on failure.
    let result = AssertUnwindSafe(async {
        apalis_postgres::PostgresStorage::setup(&pg_pool)
            .await
            .expect("setup postgres scheduler");
        cc_lb_scheduler::migrations::apply_post_setup_migrations(&pg_pool)
            .await
            .expect("apply postgres scheduler migrations");
        let worker_pool = PgPoolOptions::new()
            .min_connections(6)
            .max_connections(6)
            .acquire_timeout(Duration::from_millis(250))
            .connect_lazy_with(options.database(&database_name));
        tokio::time::timeout(Duration::from_secs(10), async {
            let mut warm = Vec::with_capacity(6);
            while warm.len() < 6 {
                if let Ok(conn) = worker_pool.acquire().await {
                    warm.push(conn);
                }
            }
            drop(warm);
        })
        .await
        .expect("pre-warm worker pool connections");
        let pg_backend =
            SchedulerBackend::Postgres(PostgresSchedulerBackend::new(worker_pool.clone()));
        let push_backend =
            SchedulerBackend::Postgres(PostgresSchedulerBackend::new(pg_pool.clone()));
        let mut fixture = Fixture::new().await;
        fixture.backend.backend = push_backend.clone();
        fixture.http.return_cache_hit_usage(100, 0, 10);
        let pusher: Arc<dyn CacheKeepaliveTaskPusher> = Arc::new(push_backend);
        let enqueuer = fixture.enqueuer(Arc::clone(&pusher));
        let dispatch = fixture.dispatch(pusher);
        let adaptive_dispatch: AdaptiveDispatchFn = Arc::new(move |job| {
            let dispatch = dispatch.clone();
            Box::pin(async move {
                match job {
                    AdaptiveJob::CacheKeepalive(job) => {
                        dispatch.dispatch_cache_keepalive(job).await
                    }
                    _ => Ok(JobOutcome::Noop),
                }
            })
        });
        let cron_dispatch: CronDispatchFn = Arc::new(|_| Box::pin(async { Ok(JobOutcome::Noop) }));
        handles = pg_backend
            .spawn(
                SchedulerCtx::new(
                    SchedulerConfig::default(),
                    adaptive_dispatch,
                    cron_dispatch,
                    Arc::new(SystemClock),
                ),
                cancel.clone(),
            )
            .await
            .expect("spawn postgres scheduler");

        let listener_pids = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let worker_types: i64 =
                    sqlx::query_scalar("SELECT COUNT(DISTINCT worker_type) FROM apalis.workers")
                        .fetch_one(&pg_pool)
                        .await
                        .expect("check worker registration");
                let listener_pids: Vec<i32> = sqlx::query_scalar(
                    "SELECT pid FROM pg_stat_activity
                     WHERE datname = current_database()
                       AND pid <> pg_backend_pid()
                       AND state = 'idle'
                       AND query ILIKE 'LISTEN %'",
                )
                .fetch_all(&pg_pool)
                .await
                .expect("find postgres scheduler listeners");
                // All three consumers listen on the same channel. Drop every listener
                // in this isolated database so the keepalive listener is included.
                if worker_types >= 3 && listener_pids.len() >= 3 {
                    break listener_pids;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("keepalive worker and postgres listeners register");
        let (pre_held_tx, pre_held_rx) = tokio::sync::oneshot::channel();
        let grab_pool = worker_pool.clone();
        let grabber = tokio::spawn(async move {
            let mut pre_held_tx = Some(pre_held_tx);
            let mut held = Vec::with_capacity(6);
            while held.len() < 6 {
                if let Ok(conn) = grab_pool.acquire().await {
                    held.push(conn);
                    if held.len() >= 3
                        && let Some(tx) = pre_held_tx.take()
                    {
                        let _ = tx.send(());
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(350)).await;
            drop(held);
        });
        tokio::time::timeout(Duration::from_secs(10), pre_held_rx)
            .await
            .expect("timed out acquiring non-LISTEN worker_pool slots")
            .expect("pre_held channel closed");
        let _ = listener_pids;
        let terminated: Vec<bool> = sqlx::query_scalar(
            "SELECT pg_terminate_backend(pid)
             FROM pg_stat_activity
             WHERE datname = current_database()
               AND pid <> pg_backend_pid()
               AND query ILIKE 'LISTEN %'",
        )
        .fetch_all(&pg_pool)
        .await
        .expect("terminate postgres scheduler listeners");
        assert!(
            terminated.len() >= 3 && terminated.iter().all(|t| *t),
            "all three postgres scheduler listeners must terminate"
        );
        tokio::time::timeout(Duration::from_secs(10), grabber)
            .await
            .expect("timed out waiting for worker_pool grabber")
            .expect("join pool saturation task");
        tokio::time::sleep(Duration::from_millis(300)).await;

        let mut request = fixture.enqueue_request();
        request.params = ScheduleParams {
            delay: Duration::from_secs(2),
            max_refreshes: 3,
            max_total_duration_secs: Duration::from_secs(600).as_secs(),
        };
        enqueuer
            .enqueue_cache_keepalive(request)
            .await
            .expect("enqueue postgres keepalive");
        let record = fixture
            .storage
            .get_cache_keepalive_session("session-hash")
            .await
            .expect("load scheduled keepalive")
            .expect("scheduled keepalive exists");
        let expires_at_unix_secs = record.run_at_unix_secs + 4;
        let updated = sqlx::query(
            "UPDATE cache_keepalive_sessions SET expires_at = ?1 WHERE session_key_hash = ?2",
        )
        .bind(i64::try_from(expires_at_unix_secs).expect("test expiry fits i64"))
        .bind("session-hash")
        .execute(fixture.storage.pool())
        .await
        .expect("shorten keepalive session expiry");
        assert_eq!(updated.rows_affected(), 1);
        let updated = sqlx::query(
            "UPDATE apalis.jobs
             SET job = convert_to(
                 jsonb_set(
                     convert_from(job, 'UTF8')::jsonb,
                     '{payload,expires_at_unix_secs}',
                     to_jsonb($1::bigint)
                 )::text, 'UTF8'
             )
             WHERE job_type = 'cache_keepalive' AND idempotency_key = $2
               AND status = 'Pending'",
        )
        .bind(i64::try_from(expires_at_unix_secs).expect("test expiry fits i64"))
        .bind(format!(
            "cache_keepalive:session-hash:{}",
            record.generation
        ))
        .execute(&pg_pool)
        .await
        .expect("shorten queued keepalive payload expiry");
        assert_eq!(updated.rows_affected(), 1);

        // QA-09: an idle worker must renew before the four-second expiry margin,
        // even after losing LISTEN notifications.
        let record = tokio::time::timeout(Duration::from_secs(7), async {
            loop {
                let record = fixture
                    .storage
                    .get_cache_keepalive_session("session-hash")
                    .await
                    .expect("load keepalive renewal")
                    .expect("keepalive session exists");
                if record.refresh_count == 1 {
                    break record;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("postgres idle keepalive renews within seven seconds");
        (record, Arc::clone(&fixture.http))
    })
    .catch_unwind()
    .await;

    cancel.cancel();
    for mut handle in handles {
        if tokio::time::timeout(Duration::from_secs(5), &mut handle)
            .await
            .is_err()
        {
            handle.abort();
            let _ = handle.await;
        }
    }
    pg_pool.close().await;
    let dropped = sqlx::query(sqlx::AssertSqlSafe(format!(
        "DROP DATABASE \"{database_name}\" WITH (FORCE)"
    )))
    .execute(&admin_pool)
    .await;
    admin_pool.close().await;
    dropped.expect("drop isolated keepalive database");
    let (record, http) = match result {
        Ok(result) => result,
        Err(panic) => std::panic::resume_unwind(panic),
    };
    assert_eq!(record.status, CacheKeepaliveSessionStatus::Active);
    assert_eq!(record.terminal_reason, None);
    assert_ne!(
        record.terminal_reason,
        Some(CacheKeepaliveTerminalReason::Expired)
    );
    assert_eq!(record.refresh_count, 1);
    assert_eq!(http.requests.lock().expect("requests lock").len(), 1);
}
