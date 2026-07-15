use std::{sync::Arc, time::Duration};

use tokio::sync::mpsc;

use super::*;
use crate::store::open_capture_store;

#[tokio::test(flavor = "current_thread")]
async fn ttl_sweep_evicts_partial_that_never_terminates() -> Result<(), Box<dyn std::error::Error>>
{
    // Given
    let directory = tempfile::tempdir()?;
    let store = open_capture_store(&directory.path().join("capture.sqlite")).await?;
    tokio::time::pause();
    let (_sender, receiver) = mpsc::channel(1);
    let stats = Arc::new(SharedStats::default());
    let config = SinkConfig {
        max_partials: 4,
        partial_ttl: Duration::from_secs(5),
        sweep_interval: Duration::from_secs(1),
        flush_interval: Duration::from_secs(1),
        batch_size: 2,
        retention_max_rows: 100,
    };
    let mut writer = CaptureWriter::new(store, receiver, Arc::clone(&stats), config);
    writer.handle_message(CaptureMessage::Seed {
        event_id: "event-stale".to_owned(),
        request_id: "request-stale".to_owned(),
        ts_unix_ms: 1,
    });

    // When
    tokio::time::advance(Duration::from_secs(6)).await;
    writer.sweep_partials();

    // Then
    assert_eq!(stats.partial_ttl_evicted.load(Ordering::Relaxed), 1);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn running_writer_periodically_prunes_rows_over_retention_cap()
-> Result<(), Box<dyn std::error::Error>> {
    // Given
    let directory = tempfile::tempdir()?;
    let store = open_capture_store(&directory.path().join("capture.sqlite")).await?;
    for index in 0_i64..3 {
        sqlx::query(
            "INSERT INTO capture_v1 (
                event_id, request_id, ts_unix_ms, disposition, schema_version, payload_json
            ) VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(format!("event-retention-{index}"))
        .bind(format!("request-retention-{index}"))
        .bind(index)
        .bind("routed_dispatched_success")
        .bind(1_i64)
        .bind("{}")
        .execute(store.pool())
        .await?;
    }
    let (_sender, receiver) = mpsc::channel(1);
    let stats = Arc::new(SharedStats::default());
    let config = SinkConfig {
        max_partials: 4,
        partial_ttl: Duration::from_secs(5),
        sweep_interval: Duration::from_millis(1),
        flush_interval: Duration::from_secs(1),
        batch_size: 2,
        retention_max_rows: 2,
    };
    let (sweep_tx, sweep_rx) = oneshot::channel();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let writer =
        CaptureWriter::new(store.clone(), receiver, stats, config).with_sweep_observer(sweep_tx);
    let join = tokio::spawn(writer.run(shutdown_rx));

    // When
    tokio::time::timeout(Duration::from_secs(1), sweep_rx).await??;
    let _ = shutdown_tx.send(());
    join.await?;

    // Then
    let remaining: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM capture_v1")
        .fetch_one(store.pool())
        .await?;
    let oldest_remaining: i64 = sqlx::query_scalar("SELECT MIN(ts_unix_ms) FROM capture_v1")
        .fetch_one(store.pool())
        .await?;
    assert_eq!(remaining, 2);
    assert_eq!(oldest_remaining, 1);
    Ok(())
}
