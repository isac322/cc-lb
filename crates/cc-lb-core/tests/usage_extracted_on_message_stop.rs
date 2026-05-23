mod sse_relay_support;
mod storage_support;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use cc_lb_core::{BucketKind, MockClock, QuotaManager, QuotaPolicy, SseBatchConfig, SseRelay};
use sse_relay_support::{RecordingHook, TestDialect, body_from_chunks, collect_response_body};
use storage_support::TestStorage;

#[tokio::test]
async fn usage_extracted_on_message_stop() -> Result<(), Box<dyn std::error::Error>> {
    let storage = TestStorage::new();
    let quota = Arc::new(QuotaManager::with_clock(
        storage.as_storage(),
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
        streaming_usage: Arc::new(Mutex::new(Default::default())),
    };

    let response =
        relay.into_response_from_body(body_from_chunks(vec![input], Duration::ZERO, None));
    let _output = collect_response_body(response).await;

    let actual_output = storage
        .get_quota(
            "principal-sse",
            reservation.window_start,
            BucketKind::OutputTokens,
        )
        .await?;
    let actual_input = storage
        .get_quota(
            "principal-sse",
            reservation.window_start,
            BucketKind::InputTokens,
        )
        .await?;
    println!("actual_output_tokens={actual_output}");
    assert_eq!(actual_output, 42);
    assert_eq!(actual_input, 7);
    Ok(())
}
