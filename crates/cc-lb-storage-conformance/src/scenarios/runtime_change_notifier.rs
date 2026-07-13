use std::{
    sync::Arc,
    time::{Duration, UNIX_EPOCH},
};

use anyhow::{Context, Result, ensure};
use cc_lb_storage_api::{
    BackendKind, ChangeChannel, ChangeEvent, MAX_CHANGE_PAYLOAD_LEN, MetaStore,
    RuntimeChangeNotifier, normalize_payload,
    upstream::{UpstreamCreate, UpstreamKind, UpstreamStore},
};
use tokio::{task::JoinHandle, time};
use tokio_util::sync::CancellationToken;

use crate::harness::{ConformanceBackend, with_conformance_fixture};

#[cfg(all(test, feature = "postgres"))]
const RECEIVE_TIMEOUT: Duration = Duration::from_secs(5);
const SUBSCRIBE_TIMEOUT: Duration = Duration::from_millis(10);
const POSTGRES_LATENCY_BUDGET: Duration = Duration::from_millis(5_000);
const SQLITE_LATENCY_BUDGET: Duration = Duration::from_millis(1_000);
const POSTGRES_LISTEN_STARTUP_DELAY: Duration = Duration::from_millis(100);

pub async fn subscribe_returns_without_blocking<N>(notifier: &N) -> Result<()>
where
    N: RuntimeChangeNotifier + ?Sized,
{
    time::timeout(SUBSCRIBE_TIMEOUT, notifier.subscribe())
        .await
        .context("subscribe should not block longer than 10ms")??;
    Ok(())
}

pub async fn cancel_during_run_is_graceful<N>(notifier: Arc<N>) -> Result<()>
where
    N: RuntimeChangeNotifier + 'static,
{
    let cancel = CancellationToken::new();
    let handle = spawn_run(Arc::clone(&notifier), cancel.clone());
    time::sleep(Duration::from_millis(20)).await;
    cancel.cancel();
    join_run(handle).await
}

pub async fn subscriber_receives_within_latency_budget<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
{
    with_conformance_fixture(backend, |storage| async move {
        let backend_kind = MetaStore::backend_kind(storage.as_ref()).await?;
        let latency_budget = latency_budget_for_backend(backend_kind)?;
        let mut receiver = RuntimeChangeNotifier::subscribe(storage.as_ref()).await?;
        let cancel = CancellationToken::new();
        let handle = spawn_run(Arc::clone(&storage), cancel.clone());
        startup_delay_for_backend(backend_kind).await;

        let mut subscriber = tokio::spawn(async move {
            loop {
                let event = receiver.recv().await?;
                if event.channel == ChangeChannel::Upstream {
                    return Ok::<_, anyhow::Error>(event);
                }
            }
        });

        let scenario_result = async {
            let record = UpstreamStore::create(storage.as_ref(), latency_upstream()).await?;
            let event = time::timeout(latency_budget, &mut subscriber)
                .await
                .with_context(|| {
                    format!(
                        "subscriber should receive upstream change within {}ms for {backend_kind:?}",
                        latency_budget.as_millis()
                    )
                })?
                .context("subscriber task should complete")??;

            let expected_payload = record.id.to_string();
            ensure!(
                event.payload == expected_payload,
                "notifier payload should identify mutated upstream; expected {expected_payload}, got {}",
                event.payload
            );

            Ok(())
        }
        .await;

        if !subscriber.is_finished() {
            subscriber.abort();
            let _ = subscriber.await;
        }
        cancel.cancel();
        let run_result = join_run(handle).await;

        scenario_result?;
        run_result
    })
    .await
}

pub fn payload_is_truncated_to_identifier_limit() -> Result<()> {
    let payload = "x".repeat(MAX_CHANGE_PAYLOAD_LEN + 50);
    let event = ChangeEvent::new(ChangeChannel::Upstream, &payload, UNIX_EPOCH);
    ensure!(
        event.payload.len() == MAX_CHANGE_PAYLOAD_LEN,
        "payload should be truncated to {MAX_CHANGE_PAYLOAD_LEN} chars"
    );
    ensure!(
        event.payload == normalize_payload(&payload),
        "ChangeEvent and helper should normalize payload identically"
    );
    Ok(())
}

fn latency_budget_for_backend(kind: BackendKind) -> Result<Duration> {
    match kind {
        BackendKind::Postgres => Ok(POSTGRES_LATENCY_BUDGET),
        BackendKind::Sqlite => Ok(SQLITE_LATENCY_BUDGET),
    }
}

async fn startup_delay_for_backend(kind: BackendKind) {
    if kind == BackendKind::Postgres {
        time::sleep(POSTGRES_LISTEN_STARTUP_DELAY).await;
    }
}

fn latency_upstream() -> UpstreamCreate {
    UpstreamCreate {
        name: "notifier-latency-budget".to_owned(),
        kind: UpstreamKind::AnthropicOauth,
        base_url: None,
        api_key_ciphertext: None,
        oauth_token_generation: None,
        warmup_enabled: false,
        warmup_dialect_plugin: None,
    }
}

#[cfg(all(test, feature = "postgres"))]
async fn recv_matching(
    receiver: &mut tokio::sync::broadcast::Receiver<ChangeEvent>,
    channel: ChangeChannel,
    payload: &str,
) -> Result<ChangeEvent> {
    let deadline = time::Instant::now() + RECEIVE_TIMEOUT;
    loop {
        let remaining = deadline.saturating_duration_since(time::Instant::now());
        ensure!(!remaining.is_zero(), "timed out waiting for {channel:?}");
        let event = time::timeout(remaining, receiver.recv()).await??;
        if event.channel == channel && event.payload == payload {
            return Ok(event);
        }
    }
}

fn spawn_run<N>(
    notifier: Arc<N>,
    cancel: CancellationToken,
) -> JoinHandle<cc_lb_storage_api::StorageResult<()>>
where
    N: RuntimeChangeNotifier + 'static,
{
    tokio::spawn(async move { notifier.run(cancel).await })
}

async fn join_run(handle: JoinHandle<cc_lb_storage_api::StorageResult<()>>) -> Result<()> {
    handle.await??;
    Ok(())
}

#[cfg(all(test, feature = "postgres"))]
mod tests {
    use std::{str::FromStr, sync::Arc};

    use cc_lb_storage_api::RuntimeChangeNotifier;
    use sqlx::{
        AssertSqlSafe,
        postgres::{PgConnectOptions, PgPoolOptions},
    };
    use uuid::Uuid;

    use super::*;

    /// Emit `payload` on `channel` repeatedly until a subscriber observes it,
    /// bounded by `RECEIVE_TIMEOUT`. Postgres LISTEN/NOTIFY is best-effort: a
    /// NOTIFY published while the listener is momentarily reconnecting — or is
    /// starved under CI coverage-instrumented load — is dropped for good (this
    /// is what flaked CI), so a single emit plus a fixed wait is racy. Re-
    /// emitting until the event round-trips is a positive-confirmation
    /// invariant, not a blind retry: a genuinely broken notifier never delivers
    /// and still fails at the deadline. It also subsumes listen-readiness, as
    /// emits published before the LISTEN is issued simply never arrive.
    async fn emit_until_received(
        pool: &sqlx::PgPool,
        receiver: &mut tokio::sync::broadcast::Receiver<ChangeEvent>,
        channel: ChangeChannel,
        payload: &str,
    ) -> Result<ChangeEvent> {
        let deadline = time::Instant::now() + RECEIVE_TIMEOUT;
        loop {
            emit(pool, channel, payload).await?;
            let remaining = deadline.saturating_duration_since(time::Instant::now());
            ensure!(!remaining.is_zero(), "did not receive {channel:?} in time");
            let poll_budget = remaining.min(Duration::from_millis(100));
            match time::timeout(poll_budget, receiver.recv()).await {
                Ok(Ok(event)) if event.channel == channel && event.payload == payload => {
                    return Ok(event);
                }
                Ok(Ok(_other)) => continue,
                Ok(Err(err)) => anyhow::bail!("notifier receiver closed: {err}"),
                Err(_) => continue,
            }
        }
    }

    async fn postgres_receive_from_other_conn(
        notifier: Arc<cc_lb_storage_postgres::PostgresStorage>,
        pool: &sqlx::PgPool,
    ) -> Result<()> {
        let cancel = CancellationToken::new();
        let handle = spawn_run(Arc::clone(&notifier), cancel.clone());
        let mut receiver = notifier.subscribe().await?;
        emit_until_received(pool, &mut receiver, ChangeChannel::Upstream, "upstream-a").await?;
        cancel.cancel();
        join_run(handle).await
    }

    async fn postgres_broadcast_fanout(
        notifier: Arc<cc_lb_storage_postgres::PostgresStorage>,
        pool: &sqlx::PgPool,
    ) -> Result<()> {
        let cancel = CancellationToken::new();
        let handle = spawn_run(Arc::clone(&notifier), cancel.clone());
        let mut a = notifier.subscribe().await?;
        let mut b = notifier.subscribe().await?;
        let mut c = notifier.subscribe().await?;
        emit_until_received(pool, &mut a, ChangeChannel::Principal, "principal-a").await?;
        recv_matching(&mut b, ChangeChannel::Principal, "principal-a").await?;
        recv_matching(&mut c, ChangeChannel::Principal, "principal-a").await?;
        cancel.cancel();
        join_run(handle).await
    }

    async fn postgres_all_four_channels_deliverable(
        notifier: Arc<cc_lb_storage_postgres::PostgresStorage>,
        pool: &sqlx::PgPool,
    ) -> Result<()> {
        let cancel = CancellationToken::new();
        let handle = spawn_run(Arc::clone(&notifier), cancel.clone());
        let mut receiver = notifier.subscribe().await?;
        for channel in ChangeChannel::ALL {
            emit_until_received(pool, &mut receiver, channel, channel.postgres_channel()).await?;
        }
        cancel.cancel();
        join_run(handle).await
    }

    async fn postgres_reconnects_after_terminate(
        notifier: Arc<cc_lb_storage_postgres::PostgresStorage>,
        pool: &sqlx::PgPool,
    ) -> Result<()> {
        let cancel = CancellationToken::new();
        let handle = spawn_run(Arc::clone(&notifier), cancel.clone());
        let mut receiver = notifier.subscribe().await?;
        emit_until_received(
            pool,
            &mut receiver,
            ChangeChannel::PluginRegistry,
            "before-terminate",
        )
        .await?;
        let terminated_pid = terminate_listener_backend(pool).await?;
        wait_for_reconnected_listener(pool, terminated_pid).await?;
        emit_until_received(
            pool,
            &mut receiver,
            ChangeChannel::PluginChain,
            "after-terminate",
        )
        .await?;
        cancel.cancel();
        join_run(handle).await
    }

    async fn emit(pool: &sqlx::PgPool, channel: ChangeChannel, payload: &str) -> Result<()> {
        sqlx::query("SELECT pg_notify($1, $2)")
            .bind(channel.postgres_channel())
            .bind(payload)
            .execute(pool)
            .await?;
        Ok(())
    }

    async fn terminate_listener_backend(pool: &sqlx::PgPool) -> Result<i32> {
        for _ in 0..100 {
            let pid = sqlx::query_scalar::<_, Option<i32>>("SELECT pid FROM pg_stat_activity WHERE query LIKE 'LISTEN %cclb_%_changed%' AND state = 'idle' ORDER BY backend_start DESC LIMIT 1")
                .fetch_one(pool)
                .await?;
            if let Some(pid) = pid {
                let terminated = sqlx::query_scalar::<_, bool>("SELECT pg_terminate_backend($1)")
                    .bind(pid)
                    .fetch_one(pool)
                    .await?;
                ensure!(terminated, "listener backend was not terminated");
                return Ok(pid);
            }
            time::sleep(Duration::from_millis(20)).await;
        }
        anyhow::bail!("timed out waiting for postgres listener backend")
    }

    async fn wait_for_reconnected_listener(pool: &sqlx::PgPool, terminated_pid: i32) -> Result<()> {
        for _ in 0..500 {
            let pid = sqlx::query_scalar::<_, i32>("SELECT pid FROM pg_stat_activity WHERE query LIKE 'LISTEN %cclb_%_changed%' AND state = 'idle' AND pid <> $1 ORDER BY backend_start DESC LIMIT 1")
                .bind(terminated_pid)
                .fetch_optional(pool)
                .await?;
            if pid.is_some() {
                return Ok(());
            }
            time::sleep(Duration::from_millis(20)).await;
        }
        anyhow::bail!("timed out waiting for reconnected postgres listener backend")
    }

    #[test]
    fn runtime_change_notifier_payload_is_truncated_to_identifier_limit() -> Result<()> {
        payload_is_truncated_to_identifier_limit()
    }

    #[tokio::test]
    async fn runtime_change_notifier_postgres_receive_from_other_conn() -> Result<()> {
        let Some(fixture) = PostgresFixture::create().await? else {
            return Ok(());
        };
        let result =
            postgres_receive_from_other_conn(Arc::new(fixture.storage.clone()), &fixture.pool)
                .await;
        fixture.teardown().await?;
        result
    }

    #[tokio::test]
    async fn runtime_change_notifier_postgres_broadcast_fanout() -> Result<()> {
        let Some(fixture) = PostgresFixture::create().await? else {
            return Ok(());
        };
        let result =
            postgres_broadcast_fanout(Arc::new(fixture.storage.clone()), &fixture.pool).await;
        fixture.teardown().await?;
        result
    }

    #[tokio::test]
    async fn runtime_change_notifier_postgres_all_four_channels_deliverable() -> Result<()> {
        let Some(fixture) = PostgresFixture::create().await? else {
            return Ok(());
        };
        let result = postgres_all_four_channels_deliverable(
            Arc::new(fixture.storage.clone()),
            &fixture.pool,
        )
        .await;
        fixture.teardown().await?;
        result
    }

    #[tokio::test]
    async fn runtime_change_notifier_reconnects_after_terminate() -> Result<()> {
        let Some(fixture) = PostgresFixture::create().await? else {
            return Ok(());
        };
        let result =
            postgres_reconnects_after_terminate(Arc::new(fixture.storage.clone()), &fixture.pool)
                .await;
        fixture.teardown().await?;
        result
    }

    struct PostgresFixture {
        schema: String,
        admin_pool: sqlx::PgPool,
        pool: sqlx::PgPool,
        listener_pool: sqlx::PgPool,
        storage: cc_lb_storage_postgres::PostgresStorage,
    }

    impl PostgresFixture {
        async fn create() -> Result<Option<Self>> {
            let Some(url) = std::env::var("CI_POSTGRES_URL").ok() else {
                eprintln!("skip: CI_POSTGRES_URL not set");
                return Ok(None);
            };
            let schema = format!("runtime_change_notifier_{}", Uuid::new_v4().simple());
            let admin_pool = PgPoolOptions::new()
                .max_connections(2)
                .connect(&url)
                .await?;
            sqlx::query(AssertSqlSafe(format!(
                "CREATE SCHEMA {}",
                quote_ident(&schema)
            )))
            .execute(&admin_pool)
            .await?;
            let pool = schema_pool(&url, &schema, 4).await?;
            let listener_pool = schema_pool(&url, &schema, 1).await?;
            let storage = cc_lb_storage_postgres::PostgresStorage::new_with_listener_pool(
                pool.clone(),
                listener_pool.clone(),
                Arc::new(cc_lb_engine::SystemClock),
            );
            Ok(Some(Self {
                schema,
                admin_pool,
                pool,
                listener_pool,
                storage,
            }))
        }

        async fn teardown(self) -> Result<()> {
            self.pool.close().await;
            self.listener_pool.close().await;
            sqlx::query(AssertSqlSafe(format!(
                "DROP SCHEMA IF EXISTS {} CASCADE",
                quote_ident(&self.schema)
            )))
            .execute(&self.admin_pool)
            .await?;
            self.admin_pool.close().await;
            Ok(())
        }
    }

    async fn schema_pool(url: &str, schema: &str, max_connections: u32) -> Result<sqlx::PgPool> {
        let search_path = format!("{}, public", quote_ident(schema));
        let options =
            PgConnectOptions::from_str(url)?.options([("search_path", search_path.as_str())]);
        Ok(PgPoolOptions::new()
            .max_connections(max_connections)
            .connect_with(options)
            .await?)
    }

    fn quote_ident(identifier: &str) -> String {
        assert!(
            identifier
                .chars()
                .all(|character| character.is_ascii_lowercase()
                    || character.is_ascii_digit()
                    || character == '_'),
            "unsafe postgres identifier: {identifier}"
        );
        format!("\"{identifier}\"")
    }
}
