//! Limit-rejection audit subscriber.
//!
//! Consumes `LifecycleEvent::LimitDecision::Rejected` and enqueues an
//! `AuditEntry` into `AuditWriterSink`. This is the sole owner of the
//! limit-rejection audit trail.

use std::sync::Arc;
use std::time::SystemTime;

use cc_lb_lifecycle::{LifecycleEvent, LimitDecisionKind};
use http::StatusCode;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use cc_lb_control::audit_writer::{AuditEntry, AuditWriterSink};

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
    metrics::counter!(
        "cclb_limit_hits_total",
        "kind" => violation.clone(),
        "key_id" => subject.key_id.clone()
    )
    .increment(1);
    if violation == "Concurrent" {
        metrics::counter!(
            "cclb_concurrent_rejects_total",
            "key_id" => subject.key_id.clone()
        )
        .increment(1);
    }

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
        actor_authority: None,
        actor_subject: None,
        actor_kind: None,
        actor_email: None,
        payload: None,
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

#[cfg(test)]
mod tests {
    use super::*;
    use cc_lb_control::audit_writer::spawn_audit_writer;
    use cc_lb_lifecycle::{LimitRequestSummary, LimitSubject, RouteSummary};
    use cc_lb_storage_api::{
        AuditEntry as StoredAuditEntry, AuditQueryScope, AuditStore, StorageError, StorageResult,
    };
    use std::sync::Mutex as StdMutex;

    #[derive(Default)]
    struct RecordingAuditStore {
        entries: StdMutex<Vec<StoredAuditEntry>>,
    }

    #[async_trait::async_trait]
    impl AuditStore for RecordingAuditStore {
        async fn append_audit(&self, entry: &StoredAuditEntry) -> StorageResult<()> {
            self.entries.lock().unwrap().push(entry.clone());
            Ok(())
        }

        async fn query_audit(
            &self,
            _principal_id: Option<&str>,
            _since: u64,
            _until: u64,
            _limit: usize,
        ) -> StorageResult<Vec<StoredAuditEntry>> {
            Err(StorageError::Fatal {
                message: "query_audit is not used by subscriber tests".to_owned(),
            })
        }

        async fn query_recent_audit(
            &self,
            _scope: AuditQueryScope<'_>,
            _since: u64,
            _until: u64,
            _limit: usize,
            _admin_only: bool,
        ) -> StorageResult<Vec<StoredAuditEntry>> {
            Err(StorageError::Fatal {
                message: "query_recent_audit is not used by subscriber tests".to_owned(),
            })
        }

        async fn query_audit_by_actor(
            &self,
            _authority: &str,
            _subject: &str,
            _since: u64,
            _until: u64,
            _limit: usize,
        ) -> StorageResult<Vec<StoredAuditEntry>> {
            Err(StorageError::Fatal {
                message: "query_audit_by_actor is not used by subscriber tests".to_owned(),
            })
        }

        async fn prune_audit_before(
            &self,
            _cutoff_ts_x_1m: u64,
            _batch_size: usize,
        ) -> StorageResult<u64> {
            Err(StorageError::Fatal {
                message: "prune_audit_before is not used by subscriber tests".to_owned(),
            })
        }
    }

    fn eid(s: &str) -> String {
        s.to_owned()
    }

    #[tokio::test(flavor = "current_thread")]
    async fn rejected_limit_decision_enqueues_audit_entry() {
        let (tx, rx) = mpsc::channel(16);
        let audit_store = Arc::new(RecordingAuditStore::default());
        let (audit_sink, audit_join) = spawn_audit_writer(audit_store.clone(), 16);
        let handle = spawn_lifecycle_limit_rejection_audit_subscriber(rx, Arc::new(audit_sink));

        tx.send(LifecycleEvent::LimitDecision {
            event_id: eid("audit-a"),
            decision: LimitDecisionKind::Rejected {
                reason: "quota_exceeded".into(),
                subject: LimitSubject {
                    principal_id: "principal-a".into(),
                    key_id: "key-a".into(),
                },
                request_summary: LimitRequestSummary {
                    model: "claude-sonnet-4".into(),
                    path: "/v1/messages".into(),
                    method: "POST".into(),
                },
                route_summary: RouteSummary {
                    upstream_name: "upstream-a".into(),
                },
                limit_violation: Some("monthly_tokens".into()),
            },
        })
        .await
        .unwrap();
        drop(tx);
        handle.shutdown().await;
        audit_join.await.unwrap();

        let entries = audit_store.entries.lock().unwrap();
        assert_eq!(entries.len(), 1);
        let entry = &entries[0];
        assert_eq!(entry.request_id, "audit-a");
        assert_eq!(entry.principal_id, "principal-a");
        assert_eq!(entry.api_key_id.as_deref(), Some("key-a"));
        assert_eq!(entry.route, "/v1/messages");
        assert_eq!(entry.upstream, "upstream-a");
        assert_eq!(entry.model.as_deref(), Some("claude-sonnet-4"));
        assert_eq!(entry.status, StatusCode::TOO_MANY_REQUESTS.as_u16());
        assert_eq!(entry.limit_violation.as_deref(), Some("monthly_tokens"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn rejected_limit_decision_without_violation_is_skipped() {
        let (tx, rx) = mpsc::channel(16);
        let audit_store = Arc::new(RecordingAuditStore::default());
        let (audit_sink, audit_join) = spawn_audit_writer(audit_store.clone(), 16);
        let handle = spawn_lifecycle_limit_rejection_audit_subscriber(rx, Arc::new(audit_sink));

        tx.send(LifecycleEvent::LimitDecision {
            event_id: eid("audit-b"),
            decision: LimitDecisionKind::Rejected {
                reason: "quota_exceeded".into(),
                subject: LimitSubject {
                    principal_id: "principal-b".into(),
                    key_id: "key-b".into(),
                },
                request_summary: LimitRequestSummary {
                    model: String::new(),
                    path: String::new(),
                    method: String::new(),
                },
                route_summary: RouteSummary {
                    upstream_name: String::new(),
                },
                limit_violation: None,
            },
        })
        .await
        .unwrap();
        drop(tx);
        handle.shutdown().await;
        audit_join.await.unwrap();

        assert!(audit_store.entries.lock().unwrap().is_empty());
    }
}
