use cc_lb_domain::{CachePricingSummary, RoutingTrace};

use super::*;
use crate::{
    schema::{CapturedRequestInput, CapturedResponse, Disposition},
    store::open_capture_store,
};

fn input(event_id: &str) -> CapturedRequestInput {
    CapturedRequestInput {
        event_id: event_id.to_owned(),
        request_id: format!("request-{event_id}"),
        thread_id: None,
        canonical_model_id: "claude-sonnet-4-5".to_owned(),
        cache_pricing: CachePricingSummary {
            status: "unavailable".to_owned(),
            input_micros_per_million: None,
            cache_creation_5m_micros_per_million: None,
            cache_creation_1h_micros_per_million: None,
            cache_read_micros_per_million: None,
        },
        breakpoints: Vec::new(),
        candidates: Vec::new(),
        subscription_preference_input_upstream_ids: Vec::new(),
        routing_trace: RoutingTrace {
            stages: Vec::new(),
            terminal_decision: None,
        },
        captured_at_unix_ms: 1_700_000_000_123,
        salt_version: "v11".to_owned(),
        cache_cost_basis_version: "v1".to_owned(),
        capture_schema_version: 1,
        build_version: "test".to_owned(),
    }
}

fn response() -> CapturedResponse {
    CapturedResponse {
        input_tokens: Some(2_000),
        output_tokens: Some(500),
        cache_read_input_tokens: Some(1_000),
        cache_creation_input_tokens_5m: Some(200),
        cache_creation_input_tokens_1h: Some(300),
        chosen_upstream_id: None,
        upstream_status: Some(200),
        client_status: Some(200),
        duration_ms: Some(345),
        attempt_num: Some(1),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn writes_one_complete_row_when_seed_input_and_terminal_response_merge()
-> Result<(), Box<dyn std::error::Error>> {
    // Given
    let directory = tempfile::tempdir()?;
    let store = open_capture_store(&directory.path().join("capture.sqlite")).await?;
    let (sink, writer) = CaptureSink::new(store.clone(), 16);

    // When
    sink.try_seed(
        "event-1".to_owned(),
        "request-event-1".to_owned(),
        1_700_000_000_000,
    )?;
    sink.try_input(input("event-1"))?;
    sink.try_response(
        "event-1".to_owned(),
        response(),
        Disposition::RoutedDispatchedSuccess,
        true,
    )?;
    writer.shutdown().await;

    // Then
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM capture_v1")
        .fetch_one(store.pool())
        .await?;
    assert_eq!(count, 1);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn drops_terminal_response_when_no_input_was_captured()
-> Result<(), Box<dyn std::error::Error>> {
    // Given
    let directory = tempfile::tempdir()?;
    let store = open_capture_store(&directory.path().join("capture.sqlite")).await?;
    let (sink, writer) = CaptureSink::new(store.clone(), 16);

    // When
    sink.try_seed(
        "event-2".to_owned(),
        "request-event-2".to_owned(),
        1_700_000_000_000,
    )?;
    sink.try_response(
        "event-2".to_owned(),
        response(),
        Disposition::RoutedPreDispatchError,
        true,
    )?;
    writer.shutdown().await;

    // Then
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM capture_v1")
        .fetch_one(store.pool())
        .await?;
    assert_eq!(count, 0);
    assert_eq!(sink.response_without_input_total(), 1);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn returns_immediately_and_counts_drop_when_channel_is_full()
-> Result<(), Box<dyn std::error::Error>> {
    // Given
    let directory = tempfile::tempdir()?;
    let store = open_capture_store(&directory.path().join("capture.sqlite")).await?;
    let (sink, writer) = CaptureSink::new(store, 1);

    // When
    let accepted = sink.try_seed("event-3".to_owned(), "request-3".to_owned(), 1);
    let overflow = sink.try_seed("event-4".to_owned(), "request-4".to_owned(), 2);

    // Then
    assert_eq!(accepted, Ok(()));
    assert_eq!(overflow, Err(CaptureEnqueueError::Full));
    assert_eq!(sink.dropped_total(), 1);
    writer.shutdown().await;
    Ok(())
}
