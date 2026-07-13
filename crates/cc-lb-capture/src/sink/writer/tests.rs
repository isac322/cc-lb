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
