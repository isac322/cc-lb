use crate::common;

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use cc_lb_engine::{
    Body, BulkheadRegistry, BulkheadRuntimeConfig, DispatchError, ExecuteError, UpstreamDispatch,
};
use cc_lb_plugin_api::SignedRequest;
use http::{Response, StatusCode};
use tokio::sync::Notify;

use common::signed_request;

#[tokio::test]
async fn saturated_upstream_does_not_block_other_upstreams()
-> Result<(), Box<dyn std::error::Error>> {
    let registry = BulkheadRegistry::new();
    let config = BulkheadRuntimeConfig {
        max_conns_per_upstream: 1,
        semaphore_permits: 1,
        acquire_timeout: Duration::from_millis(10),
    };
    let hold_a = Arc::new(HoldDispatch::default());
    let bulkhead_a = registry.bulkhead_with_dispatcher("a", config, hold_a.clone());
    let bulkhead_b = registry.bulkhead_with_dispatcher("b", config, Arc::new(OkDispatch));

    let first_a = {
        let bulkhead_a = bulkhead_a.clone();
        tokio::spawn(async move {
            bulkhead_a
                .execute(signed_request("http://a.local/").await)
                .await
        })
    };
    tokio::time::timeout(Duration::from_secs(1), hold_a.started.notified()).await?;

    let response_b = bulkhead_b
        .execute(signed_request("http://b.local/").await)
        .await?;
    assert_eq!(response_b.status(), StatusCode::OK);

    let second_a = bulkhead_a
        .execute(signed_request("http://a.local/").await)
        .await;
    assert!(matches!(second_a, Err(ExecuteError::BulkheadFull(_))));

    hold_a.release.notify_waiters();
    first_a.await??;
    assert_eq!(bulkhead_a.active(), 0);
    assert_eq!(bulkhead_b.active(), 0);
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
        Ok(ok_response())
    }
}

struct OkDispatch;

#[async_trait]
impl UpstreamDispatch for OkDispatch {
    async fn dispatch(&self, _request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        Ok(ok_response())
    }
}

fn ok_response() -> Response<Body> {
    let mut response = Response::new(Body::from(Bytes::from_static(br#"{}"#)));
    *response.status_mut() = StatusCode::OK;
    response
}
