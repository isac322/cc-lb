mod common;

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use cc_lb_core::{
    Body, Bulkhead, BulkheadConfig, DispatchError, ExecuteError, Lifecycle, LifecycleConfig,
    UpstreamDispatch,
};
use cc_lb_plugin_api::SignedRequest;
use http::header::RETRY_AFTER;
use http::{Response, StatusCode};
use tokio::sync::Notify;
use url::Url;

use common::{
    RecordingHook, TestAuthn, TestRouter, TestState, collect_body, messages_request, signed_request,
};

#[tokio::test]
async fn execute_returns_bulkhead_full_when_queue_times_out()
-> Result<(), Box<dyn std::error::Error>> {
    let hold = Arc::new(HoldDispatch::default());
    let bulkhead = Bulkhead::new(
        "anthropic-direct",
        BulkheadConfig {
            max_conns_per_upstream: 1,
            semaphore_permits: 1,
            acquire_timeout: Duration::from_millis(10),
        },
        hold.clone(),
    );

    let first = {
        let bulkhead = bulkhead.clone();
        tokio::spawn(async move {
            bulkhead
                .execute(signed_request("http://anthropic-direct.local/").await)
                .await
        })
    };
    tokio::time::timeout(Duration::from_secs(1), hold.started.notified()).await?;

    let second = bulkhead
        .execute(signed_request("http://anthropic-direct.local/").await)
        .await;
    assert!(matches!(
        second,
        Err(ExecuteError::BulkheadFull(retry_after)) if retry_after == Duration::from_millis(10)
    ));

    hold.release.notify_waiters();
    first.await??;
    Ok(())
}

#[tokio::test]
async fn lifecycle_maps_bulkhead_full_to_503_retry_after() -> Result<(), Box<dyn std::error::Error>>
{
    let hold = Arc::new(HoldDispatch::default());
    let bulkhead = Bulkhead::new(
        "anthropic-direct",
        BulkheadConfig {
            max_conns_per_upstream: 1,
            semaphore_permits: 1,
            acquire_timeout: Duration::from_millis(10),
        },
        hold.clone(),
    );
    let dispatcher: Arc<dyn UpstreamDispatch> = bulkhead;
    let lifecycle = Lifecycle::new(
        Arc::new(TestAuthn::new(TestState::default())),
        Arc::new(TestRouter {
            base_url: Url::parse("http://anthropic-direct.local/")?,
        }),
        dispatcher.clone(),
        vec![Arc::new(RecordingHook::default())],
        LifecycleConfig::default(),
    );

    let first = tokio::spawn({
        let lifecycle = lifecycle;
        async move {
            lifecycle
                .handle(messages_request(Bytes::from_static(
                    br#"{"model":"claude-test","messages":[]}"#,
                )))
                .await
        }
    });
    tokio::time::timeout(Duration::from_secs(1), hold.started.notified()).await?;

    let second_lifecycle = Lifecycle::new(
        Arc::new(TestAuthn::new(TestState::default())),
        Arc::new(TestRouter {
            base_url: Url::parse("http://anthropic-direct.local/")?,
        }),
        dispatcher,
        vec![Arc::new(RecordingHook::default())],
        LifecycleConfig::default(),
    );
    let response = second_lifecycle
        .handle(messages_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[]}"#,
        )))
        .await?;
    let (status, headers, _body) = collect_body(response).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        headers
            .get(RETRY_AFTER)
            .and_then(|value| value.to_str().ok()),
        Some("1")
    );

    hold.release.notify_waiters();
    let response = first.await??;
    let (status, _, _) = collect_body(response).await;
    assert_eq!(status, StatusCode::OK);
    Ok(())
}

#[derive(Default)]
struct HoldDispatch {
    started: Notify,
    release: Notify,
}

#[async_trait]
impl UpstreamDispatch for HoldDispatch {
    async fn dispatch(&self, _request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        self.started.notify_one();
        self.release.notified().await;
        let mut response = Response::new(Body::from(Bytes::from_static(br#"{}"#)));
        *response.status_mut() = StatusCode::OK;
        Ok(response)
    }
}
