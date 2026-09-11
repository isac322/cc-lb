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

// CI-latency budget: the coverage-instrumented nextest-cov pass runs all tests
// under llvm-cov on a shared runner. This timeout is only a hang-prevention
// bound; PostgreSQL LISTEN readiness is observed explicitly before emitting.
const RECEIVE_TIMEOUT: Duration = Duration::from_secs(15);
const SUBSCRIBE_TIMEOUT: Duration = Duration::from_millis(10);
const POSTGRES_LATENCY_BUDGET: Duration = Duration::from_millis(5_000);
const SQLITE_LATENCY_BUDGET: Duration = Duration::from_millis(1_000);
const POSTGRES_LISTENER_ESTABLISHED_PAYLOAD: &str = "postgres-listener-established";

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
    cancel.cancel();
    join_run(handle).await
}

pub async fn subscriber_receives_change<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
{
    subscriber_receives_with_timeout(backend, |_| Ok(RECEIVE_TIMEOUT), "bounded timeout").await
}

pub async fn subscriber_receives_within_latency_budget<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
{
    subscriber_receives_with_timeout(backend, latency_budget_for_backend, "latency budget").await
}

async fn subscriber_receives_with_timeout<B>(
    backend: Arc<B>,
    timeout_for_backend: fn(BackendKind) -> Result<Duration>,
    timeout_label: &'static str,
) -> Result<()>
where
    B: ConformanceBackend,
{
    with_conformance_fixture(backend, |storage| async move {
        let backend_kind = MetaStore::backend_kind(storage.as_ref()).await?;
        let delivery_timeout = timeout_for_backend(backend_kind)?;
        let mut receiver = RuntimeChangeNotifier::subscribe(storage.as_ref()).await?;
        let cancel = CancellationToken::new();
        let handle = spawn_run(Arc::clone(&storage), cancel.clone());

        let scenario_result: Result<()> = async {
            if backend_kind == BackendKind::Postgres {
                recv_matching(
                    &mut receiver,
                    ChangeChannel::Principal,
                    POSTGRES_LISTENER_ESTABLISHED_PAYLOAD,
                    RECEIVE_TIMEOUT,
                )
                .await
                .context("postgres listener should report LISTEN establishment")?;
            }

            let record = UpstreamStore::create(storage.as_ref(), notifier_upstream()).await?;
            let expected_payload = record.id.to_string();
            recv_matching(
                &mut receiver,
                ChangeChannel::Upstream,
                &expected_payload,
                delivery_timeout,
            )
            .await
            .with_context(|| {
                format!(
                    "subscriber should receive upstream change within {timeout_label} of {}ms for {backend_kind:?}",
                    delivery_timeout.as_millis()
                )
            })?;
            Ok(())
        }
        .await;

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

fn notifier_upstream() -> UpstreamCreate {
    UpstreamCreate {
        name: "notifier-delivery".to_owned(),
        kind: UpstreamKind::AnthropicOauth,
        base_url: None,
        api_key_ciphertext: None,
        oauth_token_generation: None,
        warmup_enabled: false,
        warmup_dialect_plugin: None,
    }
}

async fn recv_matching(
    receiver: &mut tokio::sync::broadcast::Receiver<ChangeEvent>,
    channel: ChangeChannel,
    payload: &str,
    timeout: Duration,
) -> Result<ChangeEvent> {
    let deadline = time::Instant::now() + timeout;
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

#[allow(non_snake_case)]
#[cfg(all(test, feature = "postgres"))]
mod tests {
    use std::{str::FromStr, sync::Arc};

    use cc_lb_storage_api::RuntimeChangeNotifier;
    use sqlx::postgres::{PgConnectOptions, PgPoolOptions};

    use super::*;

    async fn postgres_receive_from_other_conn(
        notifier: Arc<cc_lb_storage_postgres::PostgresStorage>,
        pool: &sqlx::PgPool,
    ) -> Result<()> {
        let mut receiver = notifier.subscribe().await?;
        let cancel = CancellationToken::new();
        let handle = spawn_run(Arc::clone(&notifier), cancel.clone());
        let scenario_result: Result<()> = async {
            wait_for_postgres_listener(&mut receiver).await?;
            emit(pool, ChangeChannel::Upstream, "upstream-a").await?;
            recv_matching(
                &mut receiver,
                ChangeChannel::Upstream,
                "upstream-a",
                RECEIVE_TIMEOUT,
            )
            .await?;
            Ok(())
        }
        .await;
        cancel.cancel();
        let run_result = join_run(handle).await;
        scenario_result?;
        run_result
    }

    async fn postgres_broadcast_fanout(
        notifier: Arc<cc_lb_storage_postgres::PostgresStorage>,
        pool: &sqlx::PgPool,
    ) -> Result<()> {
        let mut a = notifier.subscribe().await?;
        let mut b = notifier.subscribe().await?;
        let mut c = notifier.subscribe().await?;
        let cancel = CancellationToken::new();
        let handle = spawn_run(Arc::clone(&notifier), cancel.clone());
        let scenario_result: Result<()> = async {
            wait_for_postgres_listener(&mut a).await?;
            emit(pool, ChangeChannel::Principal, "principal-a").await?;
            recv_matching(
                &mut a,
                ChangeChannel::Principal,
                "principal-a",
                RECEIVE_TIMEOUT,
            )
            .await?;
            recv_matching(
                &mut b,
                ChangeChannel::Principal,
                "principal-a",
                RECEIVE_TIMEOUT,
            )
            .await?;
            recv_matching(
                &mut c,
                ChangeChannel::Principal,
                "principal-a",
                RECEIVE_TIMEOUT,
            )
            .await?;
            Ok(())
        }
        .await;
        cancel.cancel();
        let run_result = join_run(handle).await;
        scenario_result?;
        run_result
    }

    async fn postgres_all_four_channels_deliverable(
        notifier: Arc<cc_lb_storage_postgres::PostgresStorage>,
        pool: &sqlx::PgPool,
    ) -> Result<()> {
        let mut receiver = notifier.subscribe().await?;
        let cancel = CancellationToken::new();
        let handle = spawn_run(Arc::clone(&notifier), cancel.clone());
        let scenario_result: Result<()> = async {
            wait_for_postgres_listener(&mut receiver).await?;
            for channel in ChangeChannel::ALL {
                let payload = channel.postgres_channel();
                emit(pool, channel, payload).await?;
                recv_matching(&mut receiver, channel, payload, RECEIVE_TIMEOUT).await?;
            }
            Ok(())
        }
        .await;
        cancel.cancel();
        let run_result = join_run(handle).await;
        scenario_result?;
        run_result
    }

    async fn postgres_reconnects_after_terminate(
        notifier: Arc<cc_lb_storage_postgres::PostgresStorage>,
        pool: &sqlx::PgPool,
        listener_application_name: &str,
    ) -> Result<()> {
        let mut receiver = notifier.subscribe().await?;
        let cancel = CancellationToken::new();
        let handle = spawn_run(Arc::clone(&notifier), cancel.clone());
        let scenario_result: Result<()> = async {
            wait_for_postgres_listener(&mut receiver).await?;
            emit(pool, ChangeChannel::PluginRegistry, "before-terminate").await?;
            recv_matching(
                &mut receiver,
                ChangeChannel::PluginRegistry,
                "before-terminate",
                RECEIVE_TIMEOUT,
            )
            .await?;
            terminate_listener_backend(pool, listener_application_name).await?;
            wait_for_postgres_listener(&mut receiver).await?;
            emit(pool, ChangeChannel::PluginChain, "after-terminate").await?;
            recv_matching(
                &mut receiver,
                ChangeChannel::PluginChain,
                "after-terminate",
                RECEIVE_TIMEOUT,
            )
            .await?;
            Ok(())
        }
        .await;
        cancel.cancel();
        let run_result = join_run(handle).await;
        scenario_result?;
        run_result
    }

    async fn wait_for_postgres_listener(
        receiver: &mut tokio::sync::broadcast::Receiver<ChangeEvent>,
    ) -> Result<()> {
        recv_matching(
            receiver,
            ChangeChannel::Principal,
            POSTGRES_LISTENER_ESTABLISHED_PAYLOAD,
            RECEIVE_TIMEOUT,
        )
        .await?;
        Ok(())
    }

    async fn emit(pool: &sqlx::PgPool, channel: ChangeChannel, payload: &str) -> Result<()> {
        sqlx::query("SELECT pg_notify($1, $2)")
            .bind(channel.postgres_channel())
            .bind(payload)
            .execute(pool)
            .await?;
        Ok(())
    }

    async fn terminate_listener_backend(
        pool: &sqlx::PgPool,
        listener_application_name: &str,
    ) -> Result<()> {
        let pid = sqlx::query_scalar::<_, i32>("SELECT pid FROM pg_stat_activity WHERE application_name = $1 AND query LIKE 'LISTEN %cclb_%_changed%' AND state = 'idle' ORDER BY backend_start DESC LIMIT 1")
            .bind(listener_application_name)
            .fetch_optional(pool)
            .await?
            .context("postgres listener backend should exist after readiness event")?;
        let terminated = sqlx::query_scalar::<_, bool>("SELECT pg_terminate_backend($1)")
            .bind(pid)
            .fetch_one(pool)
            .await?;
        ensure!(terminated, "listener backend was not terminated");
        Ok(())
    }

    #[test]
    fn runtime_change_notifier_payload_is_truncated_to_identifier_limit() -> Result<()> {
        payload_is_truncated_to_identifier_limit()
    }

    #[tokio::test]
    async fn t3_postgres__runtime_change_notifier_receive_from_other_conn() -> Result<()> {
        let fixture = NotifierFixture::create().await?;
        let result =
            postgres_receive_from_other_conn(Arc::new(fixture.storage.clone()), &fixture.pool)
                .await;
        fixture.teardown().await?;
        result
    }

    #[tokio::test]
    async fn t3_postgres__runtime_change_notifier_broadcast_fanout() -> Result<()> {
        let fixture = NotifierFixture::create().await?;
        let result =
            postgres_broadcast_fanout(Arc::new(fixture.storage.clone()), &fixture.pool).await;
        fixture.teardown().await?;
        result
    }

    #[tokio::test]
    async fn t3_postgres__runtime_change_notifier_all_four_channels_deliverable() -> Result<()> {
        let fixture = NotifierFixture::create().await?;
        let result = postgres_all_four_channels_deliverable(
            Arc::new(fixture.storage.clone()),
            &fixture.pool,
        )
        .await;
        fixture.teardown().await?;
        result
    }

    #[tokio::test]
    async fn t3_postgres__runtime_change_notifier_reconnects_after_terminate() -> Result<()> {
        let fixture = NotifierFixture::create().await?;
        let result = postgres_reconnects_after_terminate(
            Arc::new(fixture.storage.clone()),
            &fixture.pool,
            fixture.schema(),
        )
        .await;
        fixture.teardown().await?;
        result
    }

    struct NotifierFixture {
        base: crate::PostgresFixture,
        pool: sqlx::PgPool,
        listener_pool: sqlx::PgPool,
        storage: cc_lb_storage_postgres::PostgresStorage,
    }

    impl NotifierFixture {
        async fn create() -> Result<Self> {
            let base = crate::postgres_fixture().await?;
            let pool = base.pool().clone();
            let listener_pool = schema_pool(
                base.database_url(),
                base.schema_name(),
                1,
                Some(base.schema_name()),
            )
            .await?;
            let storage = cc_lb_storage_postgres::PostgresStorage::new_with_listener_pool(
                pool.clone(),
                listener_pool.clone(),
                cc_lb_testkit::fixed_clock(1_700_000_000),
            );
            Ok(Self {
                base,
                pool,
                listener_pool,
                storage,
            })
        }

        fn schema(&self) -> &str {
            self.base.schema_name()
        }

        async fn teardown(self) -> Result<()> {
            self.listener_pool.close().await;
            self.base.teardown().await
        }
    }

    async fn schema_pool(
        url: &str,
        schema: &str,
        max_connections: u32,
        application_name: Option<&str>,
    ) -> Result<sqlx::PgPool> {
        let search_path = format!("{}, public", quote_ident(schema));
        let mut options =
            PgConnectOptions::from_str(url)?.options([("search_path", search_path.as_str())]);
        if let Some(application_name) = application_name {
            options = options.application_name(application_name);
        }
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
