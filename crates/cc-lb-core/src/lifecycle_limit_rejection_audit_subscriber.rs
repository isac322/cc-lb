//! Phase-5 limit-rejection audit subscriber.
//!
//! Consumes `LifecycleEvent::LimitDecision::Rejected` and enqueues an
//! `AuditEntry` into `AuditWriterSink`, reproducing the inline handler
//! path (`Lifecycle::enqueue_limit_audit`).
//!
//! Legacy inline path remains authoritative in Phase 5. This subscriber
//! is advisory in shadow mode; Phase 6c deletes the inline call and flips
//! the subscriber default.

use std::sync::Arc;
use std::time::SystemTime;

use cc_lb_lifecycle::{LifecycleEvent, LimitDecisionKind};
use http::StatusCode;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::audit_writer::{AuditEntry, AuditWriterSink};

pub struct LimitRejectionAuditSubscriberHandle {
    shutdown_tx: oneshot::Sender<()>,
    join: JoinHandle<()>,
}

impl LimitRejectionAuditSubscriberHandle {
    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(());
        if let Err(error) = self.join.await {
            tracing::warn!(%error, "lifecycle limit rejection audit subscriber task panicked");
        }
    }
}

pub fn spawn_lifecycle_limit_rejection_audit_subscriber(
    rx: mpsc::Receiver<LifecycleEvent>,
    audit_sink: Arc<AuditWriterSink>,
) -> LimitRejectionAuditSubscriberHandle {
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let join = tokio::spawn(subscriber_loop(rx, audit_sink, shutdown_rx));
    LimitRejectionAuditSubscriberHandle { shutdown_tx, join }
}

async fn subscriber_loop(
    mut rx: mpsc::Receiver<LifecycleEvent>,
    audit_sink: Arc<AuditWriterSink>,
    mut shutdown: oneshot::Receiver<()>,
) {
    loop {
        tokio::select! {
            biased;
            event = rx.recv() => {
                match event {
                    Some(event) => handle_event(&audit_sink, event),
                    None => break,
                }
            }
            _ = &mut shutdown => break,
        }
    }

    while let Ok(event) = rx.try_recv() {
        handle_event(&audit_sink, event);
    }
}

fn handle_event(audit_sink: &AuditWriterSink, event: LifecycleEvent) {
    let LifecycleEvent::LimitDecision {
        event_id,
        decision:
            LimitDecisionKind::Rejected {
                subject,
                request_summary,
                route_summary,
                limit_violation,
                ..
            },
    } = event
    else {
        return;
    };

    let Some(violation) = limit_violation else {
        metrics::counter!(
            "cc_lb_lifecycle_limit_rejection_audit_events_total",
            "outcome" => "skipped_no_violation"
        )
        .increment(1);
        return;
    };
    let Some(subject) = subject else {
        metrics::counter!(
            "cc_lb_lifecycle_limit_rejection_audit_events_total",
            "outcome" => "skipped_no_subject"
        )
        .increment(1);
        return;
    };
    let request_summary = request_summary.unwrap_or(cc_lb_lifecycle::LimitRequestSummary {
        model: String::new(),
        path: String::new(),
        method: String::new(),
    });
    let route_summary = route_summary.unwrap_or(cc_lb_lifecycle::RouteSummary {
        upstream_name: String::new(),
    });

    let entry = AuditEntry {
        ts: system_time_unix_secs(SystemTime::now()),
        request_id: event_id,
        principal_id: subject.principal_id,
        route: request_summary.path,
        upstream: route_summary.upstream_name,
        model: Some(request_summary.model),
        status: StatusCode::TOO_MANY_REQUESTS.as_u16(),
        input_tokens: None,
        output_tokens: None,
        duration_ms: 0,
        agent_label: None,
        api_key_id: Some(subject.key_id),
        cost_usd_micros: None,
        limit_violation: Some(violation),
        admin_action: None,
        actor: Some("system".to_owned()),
    };

    match audit_sink.try_enqueue(entry) {
        Ok(()) => {
            metrics::counter!(
                "cc_lb_lifecycle_limit_rejection_audit_events_total",
                "outcome" => "enqueued"
            )
            .increment(1);
        }
        Err(_) => {
            metrics::counter!(
                "cc_lb_lifecycle_limit_rejection_audit_events_total",
                "outcome" => "dropped"
            )
            .increment(1);
        }
    }
}

fn system_time_unix_secs(t: SystemTime) -> u64 {
    t.duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}
