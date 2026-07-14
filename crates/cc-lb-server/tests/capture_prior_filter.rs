#![cfg(feature = "capture")]

use crate::{capture_matrix_support, common};

use capture_matrix_support::{capture_config, capture_record, open_capture_pool};
use fake_anthropic::AppConfig;

#[tokio::test]
async fn prior_filter_narrows_subscription_preference_capture_input() {
    // Given
    let directory = tempfile::tempdir().expect("create prior-filter capture directory");
    let capture_path = directory.path().join("capture.sqlite");
    let config = format!(
        "{}\n[prompt_cache_shadow]\nenabled = true\n",
        capture_config(&capture_path, 1_024)
    );
    let mut server =
        common::spawn_capture_test_server_with_prior_filter(&config, AppConfig::default()).await;

    // When
    let response = common::http_post(
        server.proxy_addr,
        "/v1/messages",
        common::PRIOR_FILTER_CAPTURE_BODY,
        &[("request-id", "capture-prior-filter")],
    )
    .await
    .expect("post prior-filter request");
    assert_eq!(response.status, 200);
    server.graceful_shutdown();

    // Then
    let pool = open_capture_pool(&capture_path).await;
    let record = capture_record(&pool, "capture-prior-filter").await;
    let input = record.input;
    assert_eq!(input.candidates.len(), 2);
    let warm_upstream_id = input
        .candidates
        .iter()
        .find(|candidate| {
            candidate
                .cache_score
                .as_ref()
                .is_some_and(|score| score.predicted_cache_read_tokens > 0)
        })
        .expect("one candidate has the seeded warm prefix")
        .upstream_id;
    assert_eq!(
        input.subscription_preference_input_upstream_ids,
        vec![warm_upstream_id]
    );
    assert_eq!(input.routing_trace.stages[0].stage_name, "cache-affinity");
    assert_eq!(
        input.routing_trace.stages[1].stage_name,
        "subscription-preference"
    );
}
