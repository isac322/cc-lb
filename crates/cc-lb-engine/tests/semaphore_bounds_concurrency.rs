use crate::common;

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use cc_lb_engine::{Body, Bulkhead, BulkheadRuntimeConfig, DispatchError, UpstreamDispatch};
use cc_lb_upstream::SignedRequest;
use http::{Response, StatusCode};

use common::signed_request;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn semaphore_caps_concurrency_at_ten() -> Result<(), Box<dyn std::error::Error>> {
    let max_in_flight = Arc::new(AtomicU32::new(0));
    let dispatcher = Arc::new(CountingDispatch {
        active: Arc::new(AtomicU32::new(0)),
        max_in_flight: max_in_flight.clone(),
    });
    let bulkhead = Bulkhead::new(
        "anthropic-direct",
        BulkheadRuntimeConfig {
            max_conns_per_upstream: 10,
            semaphore_permits: 10,
            acquire_timeout: Duration::from_secs(2),
        },
        dispatcher,
    );

    let mut tasks = Vec::new();
    for _ in 0..100 {
        let bulkhead = bulkhead.clone();
        tasks.push(tokio::spawn(async move {
            bulkhead
                .execute(signed_request("http://anthropic-direct.local/").await)
                .await
        }));
    }

    for task in tasks {
        task.await??;
    }

    let observed = max_in_flight.load(Ordering::SeqCst);
    println!("max_in_flight={observed}");
    assert_eq!(observed, 10);
    assert_eq!(bulkhead.active(), 0);
    Ok(())
}

struct CountingDispatch {
    active: Arc<AtomicU32>,
    max_in_flight: Arc<AtomicU32>,
}

#[async_trait]
impl UpstreamDispatch for CountingDispatch {
    async fn dispatch(&self, _request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_in_flight.fetch_max(active, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(50)).await;
        self.active.fetch_sub(1, Ordering::SeqCst);
        let mut response = Response::new(Body::from(Bytes::from_static(br#"{}"#)));
        *response.status_mut() = StatusCode::OK;
        Ok(response)
    }
}
