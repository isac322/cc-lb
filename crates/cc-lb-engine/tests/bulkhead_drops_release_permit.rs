use crate::common;

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use async_trait::async_trait;
use cc_lb_engine::{Body, Bulkhead, BulkheadRuntimeConfig, DispatchError, UpstreamDispatch};
use cc_lb_plugin_api::SignedRequest;
use http::Response;
use tokio::sync::Notify;

use common::signed_request;

#[tokio::test]
async fn cancelling_execute_drops_guard_and_releases_permit()
-> Result<(), Box<dyn std::error::Error>> {
    let hold = Arc::new(HoldDispatch::default());
    let bulkhead = Bulkhead::new(
        "anthropic-direct",
        BulkheadRuntimeConfig {
            max_conns_per_upstream: 1,
            semaphore_permits: 1,
            acquire_timeout: Duration::from_secs(1),
        },
        hold.clone(),
    );

    let task = {
        let bulkhead = bulkhead.clone();
        tokio::spawn(async move {
            bulkhead
                .execute(signed_request("http://anthropic-direct.local/").await)
                .await
        })
    };
    tokio::time::timeout(Duration::from_secs(1), hold.started.notified()).await?;
    assert_eq!(bulkhead.active(), 1);
    assert_eq!(bulkhead.semaphore.available_permits(), 0);

    task.abort();
    let join = task.await;
    assert!(join.is_err());
    assert_eq!(bulkhead.in_flight.load(Ordering::SeqCst), 0);
    assert_eq!(bulkhead.semaphore.available_permits(), 1);
    Ok(())
}

#[derive(Default)]
struct HoldDispatch {
    started: Notify,
}

#[async_trait]
impl UpstreamDispatch for HoldDispatch {
    async fn dispatch(&self, _request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        self.started.notify_one();
        std::future::pending::<Result<Response<Body>, DispatchError>>().await
    }
}
