#![allow(non_snake_case)]

#[cfg(feature = "postgres")]
async fn postgres_fixture() -> anyhow::Result<cc_lb_storage_conformance::PostgresFixture> {
    cc_lb_storage_conformance::postgres_fixture().await
}

#[cfg(feature = "postgres")]
async fn scheduler_postgres_pool(
    fixture: &cc_lb_storage_conformance::PostgresFixture,
) -> anyhow::Result<sqlx::PgPool> {
    use std::str::FromStr as _;

    use sqlx::postgres::{PgConnectOptions, PgPoolOptions};

    let search_path = format!("{},public", fixture.schema_name());
    let options = PgConnectOptions::from_str(fixture.database_url())?
        .options([("search_path", search_path.as_str())]);
    Ok(PgPoolOptions::new()
        .max_connections(8)
        .connect_with(options)
        .await?)
}

#[path = "admin_next_run.rs"]
mod admin_next_run;
#[path = "cron.rs"]
mod cron;
#[path = "cron_sqlite_partial_index.rs"]
mod cron_sqlite_partial_index;
#[path = "migrations.rs"]
mod migrations;
#[path = "oauth_refresh.rs"]
mod oauth_refresh;
#[path = "oauth_usage_poll.rs"]
mod oauth_usage_poll;
#[path = "oauth_usage_poll_postgres.rs"]
mod oauth_usage_poll_postgres;
#[path = "price_catalog.rs"]
mod price_catalog;
#[path = "scheduler_lazy_vs_proactive.rs"]
mod scheduler_lazy_vs_proactive;
#[path = "scheduler_metrics.rs"]
mod scheduler_metrics;
#[path = "shared_modules_retry.rs"]
mod shared_modules_retry;
#[path = "spike_uniqueness_postgres.rs"]
mod spike_uniqueness_postgres;
#[path = "spike_uniqueness_sqlite.rs"]
mod spike_uniqueness_sqlite;
#[path = "usage_rollup.rs"]
mod usage_rollup;
#[path = "watchdog_tests.rs"]
mod watchdog_tests;
#[path = "watchdog_warmup_cycle_keys.rs"]
mod watchdog_warmup_cycle_keys;
#[path = "worker.rs"]
mod worker;
#[path = "worker_restart_lifetime.rs"]
mod worker_restart_lifetime;
#[path = "worker_restart_lifetime_postgres.rs"]
mod worker_restart_lifetime_postgres;
