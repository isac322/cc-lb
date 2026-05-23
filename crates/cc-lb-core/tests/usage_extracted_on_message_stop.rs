mod sse_relay_support;

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use cc_lb_core::{BucketKind, MockClock, QuotaManager, QuotaPolicy, SseBatchConfig, SseRelay};
use cc_lb_storage_redb::Storage;
use sse_relay_support::{RecordingHook, TestDialect, body_from_chunks, collect_response_body};

#[tokio::test]
async fn usage_extracted_on_message_stop() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let storage = Arc::new(Storage::open(&dir.path().join("quota.redb"), [22; 32])?);
    let quota = Arc::new(QuotaManager::with_clock(
        Arc::clone(&storage),
        QuotaPolicy {
            window_secs: 60,
            capacity_requests: 1_000,
            capacity_input_tokens: 1_000,
            capacity_output_tokens: 1_000,
        },
        Arc::new(MockClock::new(240)),
    ));
    let reservation = quota.reserve_output("principal-sse", 100).await.unwrap();
    let input = Bytes::from_static(
        b"event: message_stop\ndata: {\"type\":\"message_stop\",\"usage\":{\"input_tokens\":7,\"output_tokens\":42}}\n\n",
    );
    let hook = Arc::new(RecordingHook::default());
    let relay = SseRelay {
        obs: hook,
        dialect: Arc::new(TestDialect),
        batch: SseBatchConfig::default(),
        quota: Some(Arc::clone(&quota)),
        principal_id: "principal-sse".to_owned(),
        reservation: Some(reservation.clone()),
        error_normalizer: None,
        upstream_kind: None,
    };

    let response =
        relay.into_response_from_body(body_from_chunks(vec![input], Duration::ZERO, None));
    let _output = collect_response_body(response).await;

    let actual_output = storage.get_quota(
        "principal-sse",
        reservation.window_start,
        BucketKind::OutputTokens,
    )?;
    let actual_input = storage.get_quota(
        "principal-sse",
        reservation.window_start,
        BucketKind::InputTokens,
    )?;
    println!("actual_output_tokens={actual_output}");
    assert_eq!(actual_output, 42);
    assert_eq!(actual_input, 7);
    Ok(())
}
