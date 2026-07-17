use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{Mutex, mpsc, oneshot};

use crate::executor::{
    ExecutorConfig, RequestObservation, RequestSender, execute_open_loop, planned_request,
};
use crate::executor_metrics::{ExecutionTerminal, classify_terminal};
use crate::request_classifier::Classification;
use crate::sse_timing::SseTiming;
use crate::traffic::ExpectedStatusClass;

type SendFuture = Pin<Box<dyn Future<Output = RequestObservation> + Send>>;

#[derive(Clone)]
struct StalledFirstSender {
    started: mpsc::Sender<String>,
    first_release: Arc<Mutex<Option<oneshot::Receiver<()>>>>,
}

impl RequestSender for StalledFirstSender {
    fn send(&self, request: crate::manifest::PlannedRequest) -> SendFuture {
        let started = self.started.clone();
        let first_release = Arc::clone(&self.first_release);
        Box::pin(async move {
            let request_id = request.request_id;
            let _ = started.send(request_id.clone()).await;
            if request_id == "first"
                && let Some(release) = first_release.lock().await.take()
            {
                let _ = release.await;
            }
            RequestObservation::completed(200)
        })
    }
}

#[tokio::test]
async fn executor_is_open_loop() {
    // Given: the first response is held while a second request is scheduled one millisecond later.
    let (started_tx, mut started_rx) = mpsc::channel(2);
    let (release_tx, release_rx) = oneshot::channel();
    let sender = Arc::new(StalledFirstSender {
        started: started_tx,
        first_release: Arc::new(Mutex::new(Some(release_rx))),
    });
    let requests = vec![
        planned_request("first", 0, false),
        planned_request("second", 1, false),
    ];

    // When: the open-loop scheduler sends the manifest.
    let run = tokio::spawn(execute_open_loop(
        requests,
        sender,
        ExecutorConfig::with_late_tolerance(Duration::from_millis(100)),
    ));
    assert_eq!(started_rx.recv().await.as_deref(), Some("first"));

    // Then: the second send occurs before the stalled first response is released.
    let second = tokio::time::timeout(Duration::from_millis(100), started_rx.recv()).await;
    assert_eq!(second.ok().flatten().as_deref(), Some("second"));
    let _ = release_tx.send(());
    let report = run.await.expect("scheduler task joins");
    assert_eq!(report.evidence.counters.sent, 2);
    assert_eq!(report.evidence.counters.completed, 2);
    assert!(report.evidence.samples.coordinated_omission_drift_ms.p95 <= Some(100));
}

#[test]
fn sse_timing_split_frames() {
    // Given: two content deltas split across arbitrary body chunks.
    let mut timing = SseTiming::new(1_024);

    // When: the chunks are parsed at deterministic elapsed times.
    timing.push(
        b"event: content_block_delta\ndata: {\"type\":\"content_block_",
        Duration::from_millis(10),
    );
    timing.push(
        b"delta\",\"delta\":{\"text\":\"one\"}}\n\nevent: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"delta\":{\"text\":\"two\"}}\n\n",
        Duration::from_millis(25),
    );
    timing.push(
        b"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
        Duration::from_millis(30),
    );
    let summary = timing.finish();

    // Then: TTFT and the inter-delta gap preserve the frame completion times.
    assert_eq!(summary.ttft_ms, Some(25));
    assert_eq!(summary.first_delta_ms, Some(25));
    assert_eq!(summary.inter_delta_gaps_ms, vec![0]);
    assert!(summary.final_response);
    assert!(!summary.unexpected_truncation);
}

#[test]
fn cancellation_classified_expected() {
    // Given: a manifest request that intentionally cancels a streaming response.
    let expected = ExpectedStatusClass::ClientCancelled;

    // When: the executor records the client cancellation terminal state.
    let classification = classify_terminal(expected, ExecutionTerminal::ClientCancelled);

    // Then: the intentional cancellation remains an expected result.
    assert_eq!(classification, Classification::Expected);
}
