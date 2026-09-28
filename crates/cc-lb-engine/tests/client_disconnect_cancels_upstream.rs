use crate::sse_relay_support;

use std::time::Duration;

use http_body_util::BodyExt;
use sse_relay_support::{DropSignal, body_from_chunks, numbered_events, relay_for};

#[tokio::test]
async fn client_disconnect_cancels_upstream() {
    let drop_signal = DropSignal::new();
    let relay = relay_for();
    let response = relay.into_response_from_body(body_from_chunks(
        numbered_events(100),
        Duration::from_millis(10),
        Some(drop_signal.clone()),
    ));
    let mut body = response.into_body();

    for _ in 0..3 {
        let frame = body.frame().await.expect("frame exists").expect("frame ok");
        assert!(frame.into_data().is_ok());
    }
    drop(body);

    let elapsed = tokio::time::timeout(Duration::from_secs(1), drop_signal.wait_closed_ms())
        .await
        .expect("upstream closed within 1s");
    println!("upstream_closed_within_ms={elapsed}");
    assert!(elapsed < 1000);
}
