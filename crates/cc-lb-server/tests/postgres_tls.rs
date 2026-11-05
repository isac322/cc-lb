#![cfg(feature = "postgres")]

use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use cc_lb_config::{PostgresPoolConfig, SchedulerConfig, StorageConfig};
use cc_lb_server::scheduler_factory::open_scheduler_storage;
use cc_lb_server::storage_factory::{
    open_pg_fanout_pool, open_storage, open_storage_for_maintenance, open_wal_size_probe,
    probe_postgres_connection,
};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

const DEADLINE: Duration = Duration::from_secs(5);
const SYNTHETIC_USER: &str = "tls_boundary_user_1053";
const SYNTHETIC_PASSWORD: &str = "tls_boundary_password_1053";
const SSL_REQUEST_CODE: u32 = 80877103;
const PROTOCOL_VERSION: u32 = 196608;
const STRICT_MODES: [&str; 3] = ["require", "verify-ca", "verify-full"];

#[derive(Clone, Copy, Debug)]
enum Factory {
    Storage,
    Scheduler,
    Probe,
    Maintenance,
    Wal,
    Fanout,
}

#[derive(Debug, PartialEq, Eq)]
struct Observation {
    ssl_request: bool,
    plaintext_startup: bool,
}

// PostgreSQL 18 protocol flow, section 54.2.10:
// https://www.postgresql.org/docs/18/protocol-flow.html#PROTOCOL-FLOW-SSL
// Reply N to SSLRequest, then observe framed StartupMessage or clean EOF.
async fn observe_connection(mut stream: TcpStream) -> Result<Observation, String> {
    let first = read_packet(&mut stream)
        .await?
        .ok_or_else(|| "connection closed before its first packet".to_owned())?;
    let ssl_request = first.as_slice() == SSL_REQUEST_CODE.to_be_bytes();
    let startup = if ssl_request {
        stream
            .write_all(b"N")
            .await
            .map_err(|error| format!("send SSL refusal: {error}"))?;
        read_packet(&mut stream).await?
    } else {
        Some(first)
    };
    let Some(startup) = startup else {
        return Ok(Observation {
            ssl_request,
            plaintext_startup: false,
        });
    };
    if startup.len() < 5 || startup[..4] != PROTOCOL_VERSION.to_be_bytes() {
        return Err("expected PostgreSQL protocol-3 StartupMessage".to_owned());
    }
    if startup.last() != Some(&0)
        || !startup
            .windows(SYNTHETIC_USER.len())
            .any(|bytes| bytes == SYNTHETIC_USER.as_bytes())
    {
        return Err("StartupMessage lacks framed startup parameters".to_owned());
    }

    // Fail authentication deliberately, without reflecting credentials. This makes
    // real SQLx callers finish promptly instead of waiting for a full fake DB.
    let payload = b"SFATAL\0VFATAL\0C28000\0Mpacket-level TLS test refusal\0\0";
    let length = u32::try_from(payload.len() + 4).expect("ErrorResponse fits u32");
    stream
        .write_all(b"E")
        .await
        .map_err(|error| format!("send ErrorResponse tag: {error}"))?;
    stream
        .write_all(&length.to_be_bytes())
        .await
        .map_err(|error| format!("send ErrorResponse length: {error}"))?;
    stream
        .write_all(payload)
        .await
        .map_err(|error| format!("send ErrorResponse payload: {error}"))?;
    stream
        .shutdown()
        .await
        .map_err(|error| format!("close packet connection: {error}"))?;
    Ok(Observation {
        ssl_request,
        plaintext_startup: true,
    })
}

async fn read_packet(stream: &mut TcpStream) -> Result<Option<Vec<u8>>, String> {
    let mut length_bytes = [0_u8; 4];
    // Read one byte first so only a zero-byte read counts as clean EOF. A
    // truncated length/body must fail, not masquerade as TLS refusing plaintext.
    if stream
        .read(&mut length_bytes[..1])
        .await
        .map_err(|error| format!("read packet prefix: {error}"))?
        == 0
    {
        return Ok(None);
    }
    stream
        .read_exact(&mut length_bytes[1..])
        .await
        .map_err(|error| format!("read packet length: {error}"))?;
    let length = u32::from_be_bytes(length_bytes);
    if !(8..=65536).contains(&length) {
        return Err(format!("invalid startup packet length {length}"));
    }
    let mut body = vec![0_u8; usize::try_from(length - 4).expect("bounded packet length")];
    stream
        .read_exact(&mut body)
        .await
        .map_err(|error| format!("read packet body: {error}"))?;
    Ok(Some(body))
}

fn pool_config(mode: Option<&str>) -> Value {
    let mut pool = json!({
        "max_connections": 1,
        "min_connections": 0,
        "acquire_timeout_secs": 1,
    });
    if let Some(mode) = mode {
        pool["sslmode"] = json!(mode);
    }
    pool
}

fn storage_config(url: &str, mode: Option<&str>) -> StorageConfig {
    serde_json::from_value(json!({
        "kind": "postgres",
        "url": url,
        "pool": pool_config(mode),
    }))
    .expect("Postgres storage configuration deserializes")
}

fn scheduler_config(mode: Option<&str>) -> SchedulerConfig {
    serde_json::from_value(json!({"separate_pool": pool_config(mode)}))
        .expect("scheduler configuration deserializes")
}

async fn probe(url: &str, mode: Option<&str>) -> Result<(), String> {
    let pool: PostgresPoolConfig =
        serde_json::from_value(pool_config(mode)).expect("probe pool configuration deserializes");
    probe_postgres_connection(url, &pool)
        .await
        .map_err(|error| error.to_string())
}

async fn call_factory(
    factory: Factory,
    url: &str,
    storage_mode: Option<&str>,
    scheduler_mode: Option<&str>,
) -> Result<(), String> {
    let storage = storage_config(url, storage_mode);
    let clock = Arc::new(cc_lb_clock::SystemClock);
    match factory {
        Factory::Storage => open_storage(&storage, clock)
            .await
            .map(|_| ())
            .map_err(|error| error.to_string()),
        Factory::Scheduler => {
            open_scheduler_storage(&storage, &scheduler_config(scheduler_mode), clock)
                .await
                .map(|_| ())
                .map_err(|error| error.to_string())
        }
        Factory::Probe => probe(url, storage_mode).await,
        Factory::Maintenance => open_storage_for_maintenance(&storage, clock)
            .await
            .map(|_| ())
            .map_err(|error| error.to_string()),
        Factory::Wal => open_wal_size_probe(&storage)
            .await
            .map(|_| ())
            .map_err(|error| error.to_string()),
        Factory::Fanout => match open_pg_fanout_pool(&storage).await {
            Ok(Some(pool)) => {
                let result = sqlx::query("SELECT 1")
                    .execute(&pool)
                    .await
                    .map(|_| ())
                    .map_err(|error| error.to_string());
                pool.close().await;
                result
            }
            Ok(None) => Err("Postgres fanout unexpectedly returned no pool".to_owned()),
            Err(error) => Err(error.to_string()),
        },
    }
}

async fn exercise(
    factory: Factory,
    query: Option<&str>,
    storage_mode: Option<&str>,
    scheduler_mode: Option<&str>,
) -> Vec<Observation> {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("bind PostgreSQL packet listener");
    let address = listener.local_addr().expect("read packet listener address");
    let suffix = query.map(|query| format!("?{query}")).unwrap_or_default();
    let url =
        format!("postgres://{SYNTHETIC_USER}:{SYNTHETIC_PASSWORD}@{address}/tls_test{suffix}");
    let cancel = CancellationToken::new();
    let server_cancel = cancel.clone();
    let server = async move {
        let mut handlers = JoinSet::new();
        loop {
            tokio::select! {
                // Drain any queued accepts before honoring completion, so even
                // early-rejection cases cannot hide a connection in the backlog.
                biased;
                accepted = listener.accept() => {
                    let (stream, _) = accepted.expect("accept PostgreSQL connection");
                    handlers.spawn(async move {
                        timeout(DEADLINE, observe_connection(stream))
                            .await
                            .expect("packet observation deadline exceeded")
                    });
                }
                _ = server_cancel.cancelled() => break,
            }
        }
        let mut observations = Vec::new();
        while let Some(handler) = handlers.join_next().await {
            observations.push(
                handler
                    .expect("packet observer task joins")
                    .expect("packet observer completes without protocol errors"),
            );
        }
        observations
    };
    let client = async {
        let result = timeout(
            DEADLINE,
            call_factory(factory, &url, storage_mode, scheduler_mode),
        )
        .await;
        // Always complete/cancel the accept loop before examining assertions.
        cancel.cancel();
        result
    };
    let (result, observations) = tokio::join!(client, server);
    let result = result.expect("public factory exceeded its bounded connection timeout");
    let error = result.expect_err("packet server deliberately cannot authenticate");
    assert!(
        !error.contains(SYNTHETIC_USER) && !error.contains(SYNTHETIC_PASSWORD),
        "{factory:?} leaked synthetic credentials: {error}"
    );
    observations
}

fn assert_packets(observations: &[Observation], ssl_request: bool, plaintext_startup: bool) {
    assert!(
        !observations.is_empty(),
        "no PostgreSQL connection was observed"
    );
    let expected = Observation {
        ssl_request,
        plaintext_startup,
    };
    for observation in observations {
        assert_eq!(observation, &expected);
    }
}

#[tokio::test]
async fn storage_strict_url_without_pool_sslmode_refuses_plaintext() {
    for mode in STRICT_MODES {
        let query = format!("sslmode={mode}");
        let observations = exercise(Factory::Storage, Some(&query), None, None).await;
        assert_packets(&observations, true, false);
    }
}

#[tokio::test]
async fn scheduler_strict_url_without_pool_sslmode_refuses_plaintext() {
    for mode in STRICT_MODES {
        let query = format!("sslmode={mode}");
        let observations = exercise(Factory::Scheduler, Some(&query), None, None).await;
        assert_packets(&observations, true, false);
    }
}

#[tokio::test]
async fn probe_strict_url_refuses_plaintext() {
    for mode in STRICT_MODES {
        let query = format!("sslmode={mode}");
        let observations = exercise(Factory::Probe, Some(&query), None, None).await;
        assert_packets(&observations, true, false);
    }
}

#[tokio::test]
async fn explicit_prefer_overrides_strict_url_healthy_packet_control() {
    for factory in [Factory::Storage, Factory::Scheduler, Factory::Probe] {
        let observations = exercise(
            factory,
            Some("sslmode=require"),
            Some("prefer"),
            Some("prefer"),
        )
        .await;
        assert_packets(&observations, true, true);
    }
}

#[tokio::test]
async fn explicit_strict_pool_sslmode_overrides_permissive_url() {
    for factory in [Factory::Storage, Factory::Scheduler, Factory::Probe] {
        for query in [None, Some("sslmode=disable")] {
            for mode in ["require", "VERIFY_CA", "verify_full"] {
                let observations = exercise(factory, query, Some(mode), Some(mode)).await;
                assert_packets(&observations, true, false);
            }
        }
    }
}

#[tokio::test]
async fn url_disable_without_pool_sslmode_sends_startup_without_sslrequest() {
    for factory in [Factory::Storage, Factory::Scheduler, Factory::Probe] {
        let observations = exercise(factory, Some("sslmode=disable"), None, None).await;
        assert_packets(&observations, false, true);
    }
}

#[tokio::test]
async fn no_url_or_pool_sslmode_defaults_to_prefer() {
    for factory in [Factory::Storage, Factory::Scheduler, Factory::Probe] {
        let observations = exercise(factory, None, None, None).await;
        assert_packets(&observations, true, true);
    }
}

#[tokio::test]
async fn url_ssl_mode_aliases_preserve_strict_and_disabled_packets() {
    for factory in [Factory::Storage, Factory::Scheduler, Factory::Probe] {
        let observations = exercise(factory, Some("ssl-mode=require"), None, None).await;
        assert_packets(&observations, true, false);
        let observations = exercise(factory, Some("ssl-mode=disable"), None, None).await;
        assert_packets(&observations, false, true);
    }
}

#[tokio::test]
async fn duplicate_url_sslmode_keys_use_last_value_for_all_factories() {
    for factory in [Factory::Storage, Factory::Scheduler, Factory::Probe] {
        let observations =
            exercise(factory, Some("sslmode=prefer&sslmode=require"), None, None).await;
        assert_packets(&observations, true, false);

        let observations =
            exercise(factory, Some("sslmode=require&sslmode=prefer"), None, None).await;
        assert_packets(&observations, true, true);

        let observations =
            exercise(factory, Some("ssl-mode=prefer&sslmode=require"), None, None).await;
        assert_packets(&observations, true, false);
    }
}

#[tokio::test]
async fn pgsslmode_isolated_subprocess_does_not_override_url_resolution() {
    if std::env::var_os("CC_LB_PGSSLMODE_CHILD").is_some() {
        let prefer = exercise(Factory::Storage, None, None, None).await;
        assert_packets(&prefer, true, true);
        let strict = exercise(Factory::Storage, Some("sslmode=require"), None, None).await;
        assert_packets(&strict, true, false);
        return;
    }

    let status = Command::new(std::env::current_exe().expect("integration test executable"))
        .args([
            "--exact",
            "postgres_tls::pgsslmode_isolated_subprocess_does_not_override_url_resolution",
            "--nocapture",
        ])
        .env("CC_LB_PGSSLMODE_CHILD", "1")
        .env("PGSSLMODE", "disable")
        .status()
        .expect("spawn PGSSLMODE child test");
    assert!(status.success(), "PGSSLMODE child failed: {status}");
}

#[tokio::test]
async fn explicit_disable_overrides_strict_url() {
    for factory in [Factory::Storage, Factory::Scheduler, Factory::Probe] {
        let observations = exercise(
            factory,
            Some("sslmode=require"),
            Some("disable"),
            Some("disable"),
        )
        .await;
        assert_packets(&observations, false, true);
    }
}

#[tokio::test]
async fn invalid_explicit_sslmode_fails_without_connecting() {
    for factory in [
        Factory::Storage,
        Factory::Scheduler,
        Factory::Probe,
        Factory::Maintenance,
        Factory::Wal,
        Factory::Fanout,
    ] {
        let observations = exercise(
            factory,
            None,
            Some("not-a-real-sslmode"),
            Some("not-a-real-sslmode"),
        )
        .await;
        assert!(
            observations.is_empty(),
            "{factory:?} connected with invalid mode: {observations:?}"
        );
    }
}

#[tokio::test]
async fn invalid_url_sslmode_fails_without_connecting() {
    for factory in [
        Factory::Storage,
        Factory::Scheduler,
        Factory::Probe,
        Factory::Maintenance,
        Factory::Wal,
        Factory::Fanout,
    ] {
        let observations = exercise(factory, Some("sslmode=not-a-real-sslmode"), None, None).await;
        assert!(
            observations.is_empty(),
            "{factory:?} connected with invalid URL mode: {observations:?}"
        );
    }
}

#[tokio::test]
async fn maintenance_wal_and_fanout_preserve_strict_url_without_pool_sslmode() {
    for factory in [Factory::Maintenance, Factory::Wal, Factory::Fanout] {
        let observations = exercise(factory, Some("sslmode=require"), None, None).await;
        assert_packets(&observations, true, false);
    }
}

#[tokio::test]
async fn scheduler_sslmode_is_independent_of_storage_pool_sslmode() {
    let observations = exercise(
        Factory::Scheduler,
        Some("sslmode=require"),
        Some("prefer"),
        None,
    )
    .await;
    assert_packets(&observations, true, false);
    let observations = exercise(
        Factory::Scheduler,
        Some("sslmode=disable"),
        Some("require"),
        Some("prefer"),
    )
    .await;
    assert_packets(&observations, true, true);
}
