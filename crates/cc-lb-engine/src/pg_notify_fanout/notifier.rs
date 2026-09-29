use std::time::Duration;

use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

use super::cache::PartialRetentionCache;
use super::metrics::{record_notify_dropped, record_notify_sent};
use super::protocol::TruncatedPartialNotify;
use crate::metrics_labels::{NotifyDropReason, NotifySentOutcome};
use cc_lb_control::event_bus::RequestEventUpdate;

pub const DEFAULT_PG_NOTIFY_CHANNEL: &str = "cc_lb_events_partial";
pub const PARTIAL_NOTIFY_MPSC_CAPACITY: usize = 1024;
const NOTIFY_PAYLOAD_LIMIT_BYTES: usize = 7500;

pub struct PgNotifier {
    pub pg_pool: sqlx::PgPool,
    pub in_rx: mpsc::Receiver<RequestEventUpdate>,
    pub retention: PartialRetentionCache,
    pub cluster_instance_url: String,
    channel: String,
}

impl PgNotifier {
    pub fn spawn(
        pg_pool: sqlx::PgPool,
        in_rx: mpsc::Receiver<RequestEventUpdate>,
        retention: PartialRetentionCache,
        cluster_instance_url: String,
        shutdown_rx: watch::Receiver<bool>,
    ) -> JoinHandle<()> {
        Self::spawn_with_channel(
            pg_pool,
            in_rx,
            retention,
            cluster_instance_url,
            DEFAULT_PG_NOTIFY_CHANNEL.to_owned(),
            shutdown_rx,
        )
    }

    pub fn spawn_with_channel(
        pg_pool: sqlx::PgPool,
        in_rx: mpsc::Receiver<RequestEventUpdate>,
        retention: PartialRetentionCache,
        cluster_instance_url: String,
        channel: String,
        shutdown_rx: watch::Receiver<bool>,
    ) -> JoinHandle<()> {
        let notifier = Self {
            pg_pool,
            in_rx,
            retention,
            cluster_instance_url,
            channel,
        };
        tokio::spawn(notifier.run(shutdown_rx))
    }

    async fn run(mut self, mut shutdown_rx: watch::Receiver<bool>) {
        let mut sweep_interval = tokio::time::interval(Duration::from_secs(60));
        sweep_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        return;
                    }
                }
                _ = sweep_interval.tick() => self.retention.prune_expired(),
                update = self.in_rx.recv() => {
                    let Some(update) = update else { return; };
                    self.publish_update(update).await;
                }
            }
        }
    }

    async fn publish_update(&self, update: RequestEventUpdate) {
        let RequestEventUpdate::Partial(partial) = update else {
            return;
        };
        let full_update = RequestEventUpdate::Partial(partial.clone());
        let inline_payload = match serde_json::to_vec(&super::protocol::InlinePartialNotify {
            update: &full_update,
            producer_url: self.cluster_instance_url.as_str(),
        }) {
            Ok(payload) => payload,
            Err(error) => {
                record_notify_dropped(NotifyDropReason::SerializeError);
                tracing::warn!(%error, "partial notify serialization failed");
                return;
            }
        };

        let (notify_payload, outcome) = if notify_payload_fits_inline(inline_payload.len()) {
            (inline_payload, NotifySentOutcome::Sent)
        } else {
            let payload = match serde_json::to_vec(&full_update) {
                Ok(payload) => payload,
                Err(error) => {
                    record_notify_dropped(NotifyDropReason::SerializeError);
                    tracing::warn!(%error, "retained partial serialization failed");
                    return;
                }
            };
            self.retention.insert(partial.event_id.clone(), payload);
            let marker = match serde_json::to_vec(&TruncatedPartialNotify {
                event_id: partial.event_id,
                phase: "partial",
                producer_url: self.cluster_instance_url.as_str(),
                truncated: true,
            }) {
                Ok(marker) => marker,
                Err(error) => {
                    record_notify_dropped(NotifyDropReason::SerializeError);
                    tracing::warn!(%error, "partial notify truncation marker serialization failed");
                    return;
                }
            };
            (marker, NotifySentOutcome::TruncatedSent)
        };

        match sqlx::query("SELECT pg_notify($1, $2)")
            .bind(&self.channel)
            .bind(String::from_utf8_lossy(&notify_payload).as_ref())
            .execute(&self.pg_pool)
            .await
        {
            Ok(_) => record_notify_sent(outcome),
            Err(error) => {
                record_notify_dropped(NotifyDropReason::PgError);
                record_notify_sent(NotifySentOutcome::Failed);
                tracing::warn!(%error, "pg_notify partial publish failed");
            }
        }
    }
}

pub(crate) fn notify_payload_fits_inline(payload_len: usize) -> bool {
    payload_len < NOTIFY_PAYLOAD_LIMIT_BYTES
}
