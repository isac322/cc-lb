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

use crate::clock::ClockHandle;
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
    clock: ClockHandle,
) -> LimitRejectionAuditSubscriberHandle {
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let join = tokio::spawn(subscriber_loop(rx, audit_sink, clock, shutdown_rx));
    LimitRejectionAuditSubscriberHandle { shutdown_tx, join }
}

async fn subscriber_loop(
    mut rx: mpsc::Receiver<LifecycleEvent>,
    audit_sink: Arc<AuditWriterSink>,
    clock: ClockHandle,
    mut shutdown: oneshot::Receiver<()>,
) {
    loop {
        tokio::select! {
            biased;
            event = rx.recv() => {
                match event {
                    Some(event) => handle_event(&audit_sink, &clock, event),
                    None => break,
                }
            }
            _ = &mut shutdown => break,
        }
    }

    while let Ok(event) = rx.try_recv() {
        handle_event(&audit_sink, &clock, event);
    }
}

fn handle_event(audit_sink: &AuditWriterSink, clock: &ClockHandle, event: LifecycleEvent) {
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

    let request_summary = request_summary.unwrap_or(cc_lb_lifecycle::LimitRequestSummary {
        model: String::new(),
        path: String::new(),
        method: String::new(),
    });
    let route_summary = route_summary.unwrap_or(cc_lb_lifecycle::RouteSummary {
        upstream_name: String::new(),
    });

    let entry = AuditEntry {
        ts: system_time_unix_secs(clock.now()),
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
        kind: None,
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
#[allow(non_snake_case)]
mod tests {
    use super::*;
    use cc_lb_control::audit_writer::spawn_audit_writer;
    use cc_lb_lifecycle::{LimitRequestSummary, LimitSubject, RouteSummary};
    use cc_lb_storage_api::{
        AuditEntry as StoredAuditEntry, AuditStore, StorageError, StorageResult,
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

        async fn prune_audit(&self, _older_than: u64) -> StorageResult<u64> {
            Err(StorageError::Fatal {
                message: "prune_audit is not used by subscriber tests".to_owned(),
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
        let handle = spawn_lifecycle_limit_rejection_audit_subscriber(
            rx,
            Arc::new(audit_sink),
            Arc::new(crate::clock::TestClock::new_at_secs(1_700_000_000)),
        );

        tx.send(LifecycleEvent::LimitDecision {
            event_id: eid("audit-a"),
            decision: LimitDecisionKind::Rejected {
                reason: "quota_exceeded".into(),
                subject: Some(LimitSubject {
                    principal_id: "principal-a".into(),
                    key_id: "key-a".into(),
                }),
                request_summary: Some(LimitRequestSummary {
                    model: "claude-sonnet-4".into(),
                    path: "/v1/messages".into(),
                    method: "POST".into(),
                }),
                route_summary: Some(RouteSummary {
                    upstream_name: "upstream-a".into(),
                }),
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
    async fn t2__concurrent_rejection_records_exact_metrics_and_audit() {
        let (recorder, snapshotter) = cc_lb_testkit::local_recorder();
        let _recorder_guard = cc_lb_testkit::install_local_recorder(&recorder);
        let (tx, rx) = mpsc::channel(16);
        let audit_store = Arc::new(RecordingAuditStore::default());
        let (audit_sink, audit_join) = spawn_audit_writer(audit_store.clone(), 16);
        let handle = spawn_lifecycle_limit_rejection_audit_subscriber(
            rx,
            Arc::new(audit_sink),
            cc_lb_testkit::fixed_clock(1_700_000_000),
        );

        tx.send(LifecycleEvent::LimitDecision {
            event_id: eid("concurrent-rejection"),
            decision: LimitDecisionKind::Rejected {
                reason: "concurrent request cap exceeded".into(),
                subject: Some(LimitSubject {
                    principal_id: "principal-concurrent".into(),
                    key_id: "key-concurrent".into(),
                }),
                request_summary: Some(LimitRequestSummary {
                    model: "claude-sonnet-4-5-20250929".into(),
                    path: "/v1/messages".into(),
                    method: "POST".into(),
                }),
                route_summary: Some(RouteSummary {
                    upstream_name: "fake_anthropic".into(),
                }),
                limit_violation: Some("Concurrent".into()),
            },
        })
        .await
        .expect("send concurrent rejection");
        drop(tx);
        handle.shutdown().await;
        audit_join.await.expect("audit writer joins");

        let entries = audit_store.entries.lock().expect("audit entries lock");
        assert_eq!(entries.len(), 1);
        let entry = &entries[0];
        assert_eq!(entry.ts, 1_700_000_000);
        assert_eq!(entry.request_id, "concurrent-rejection");
        assert_eq!(entry.principal_id, "principal-concurrent");
        assert_eq!(entry.api_key_id.as_deref(), Some("key-concurrent"));
        assert_eq!(entry.route, "/v1/messages");
        assert_eq!(entry.upstream, "fake_anthropic");
        assert_eq!(entry.model.as_deref(), Some("claude-sonnet-4-5-20250929"));
        assert_eq!(entry.status, StatusCode::TOO_MANY_REQUESTS.as_u16());
        assert_eq!(entry.limit_violation.as_deref(), Some("Concurrent"));
        drop(entries);

        let samples = snapshotter.snapshot().into_vec();
        for (name, labels) in [
            (
                "cclb_limit_hits_total",
                vec![("kind", "Concurrent"), ("key_id", "key-concurrent")],
            ),
            (
                "cclb_concurrent_rejects_total",
                vec![("key_id", "key-concurrent")],
            ),
        ] {
            let matching = samples
                .iter()
                .filter(|(key, _, _, _)| {
                    key.key().name() == name
                        && labels.iter().all(|(expected_key, expected_value)| {
                            key.key().labels().any(|label| {
                                label.key() == *expected_key && label.value() == *expected_value
                            })
                        })
                })
                .collect::<Vec<_>>();
            assert_eq!(matching.len(), 1, "metric {name} with labels {labels:?}");
            assert_eq!(
                format!("{:?}", matching[0].3),
                "Counter(1)",
                "metric {name} with labels {labels:?}"
            );
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn rejected_limit_decision_without_violation_is_skipped() {
        let (tx, rx) = mpsc::channel(16);
        let audit_store = Arc::new(RecordingAuditStore::default());
        let (audit_sink, audit_join) = spawn_audit_writer(audit_store.clone(), 16);
        let handle = spawn_lifecycle_limit_rejection_audit_subscriber(
            rx,
            Arc::new(audit_sink),
            Arc::new(crate::clock::TestClock::new_at_secs(1_700_000_000)),
        );

        tx.send(LifecycleEvent::LimitDecision {
            event_id: eid("audit-b"),
            decision: LimitDecisionKind::Rejected {
                reason: "quota_exceeded".into(),
                subject: Some(LimitSubject {
                    principal_id: "principal-b".into(),
                    key_id: "key-b".into(),
                }),
                request_summary: None,
                route_summary: None,
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
