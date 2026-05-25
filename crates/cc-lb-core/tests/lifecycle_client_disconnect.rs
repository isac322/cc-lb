mod common;

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::BodyExt;

use common::{
    DispatchMode, MockDispatch, RecordingHook, TestAuthn, TestState, lifecycle_with,
    messages_request,
};

#[tokio::test]
async fn dropping_downstream_body_drops_upstream_stream() {
    let state = TestState::default();
    let hook = Arc::new(RecordingHook::default());
    let lifecycle = lifecycle_with(
        TestAuthn::new(state.clone()),
        MockDispatch {
            state: state.clone(),
            mode: DispatchMode::StreamingOk,
        },
        hook,
    );

    let response = lifecycle
        .handle(messages_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[],"stream":true}"#,
        )))
        .await
        .expect("lifecycle handles request");
    let mut body = response.into_body();
    for _ in 0..3 {
        let frame = body
            .frame()
            .await
            .expect("frame available")
            .expect("frame ok");
        assert!(frame.into_data().is_ok());
    }
    drop(body);

    if !state.stream_dropped.load(Ordering::Relaxed) {
        tokio::time::timeout(Duration::from_secs(1), state.stream_drop_notify.notified())
            .await
            .expect("upstream stream dropped within 1s");
    }
    println!(
        "upstream_stream_dropped={}",
        state.stream_dropped.load(Ordering::Relaxed)
    );
    assert!(state.stream_dropped.load(Ordering::Relaxed));
}
