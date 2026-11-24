use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tokio::task::JoinHandle;

use super::DEFAULT_PG_NOTIFY_CHANNEL;
use super::metrics::{record_pg_listener_reconnect, record_queue_usage};
use super::protocol::NotifyOrigin;
use crate::live_tail::metrics_labels::PgListenerReconnectReason;
use cc_lb_control::event_bus::{RequestEventBus, RequestEventUpdate};

const QUEUE_USAGE_POLL_INTERVAL: Duration = Duration::from_secs(10);

pub struct PgListener {
    pub pg_pool: sqlx::PgPool,
    pub bus: Arc<dyn RequestEventBus>,
    local_producer_id: String,
    channel: String,
}

impl PgListener {
    pub fn spawn(
        pg_pool: sqlx::PgPool,
        bus: Arc<dyn RequestEventBus>,
        local_producer_id: String,
        shutdown_rx: watch::Receiver<bool>,
    ) -> JoinHandle<()> {
        Self::spawn_with_channel(
            pg_pool,
            bus,
            DEFAULT_PG_NOTIFY_CHANNEL.to_owned(),
            local_producer_id,
            shutdown_rx,
        )
    }

    pub fn spawn_with_channel(
        pg_pool: sqlx::PgPool,
        bus: Arc<dyn RequestEventBus>,
        channel: String,
        local_producer_id: String,
        shutdown_rx: watch::Receiver<bool>,
    ) -> JoinHandle<()> {
        let listener = Self {
            pg_pool,
            bus,
            local_producer_id,
            channel,
        };
        tokio::spawn(listener.run(shutdown_rx))
    }

    async fn run(self, mut shutdown_rx: watch::Receiver<bool>) {
        let mut queue_usage_interval = tokio::time::interval(QUEUE_USAGE_POLL_INTERVAL);
        queue_usage_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            let mut listener = match sqlx::postgres::PgListener::connect_with(&self.pg_pool).await {
                Ok(listener) => listener,
                Err(error) => {
                    let reason = PgListenerReconnectReason::ConnectFailed;
                    record_pg_listener_reconnect(reason);
                    tracing::warn!(%error, reason = reason.as_str(), "pg notify listener connect failed");
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_secs(1)) => continue,
                        changed = shutdown_rx.changed() => {
                            if changed.is_err() || *shutdown_rx.borrow() { return; }
                        }
                    }
                    continue;
                }
            };
            if let Err(error) = listener.listen(&self.channel).await {
                let reason = PgListenerReconnectReason::SubscribeFailed;
                record_pg_listener_reconnect(reason);
                tracing::warn!(%error, reason = reason.as_str(), channel = %self.channel, "pg notify listen failed");
                continue;
            }

            loop {
                tokio::select! {
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            return;
                        }
                    }
                    _ = queue_usage_interval.tick() => record_queue_usage(&self.pg_pool).await,
                    notification = listener.try_recv() => {
                        match notification {
                            Ok(Some(notification)) => self.handle_payload(notification.payload()).await,
                            Ok(None) => {
                                let reason = PgListenerReconnectReason::RecvFailed;
                                record_pg_listener_reconnect(reason);
                                tracing::warn!(reason = reason.as_str(), "pg notify listener recv connection lost; reconnected");
                            }
                            Err(error) => {
                                let reason = PgListenerReconnectReason::RecvFailed;
                                record_pg_listener_reconnect(reason);
                                tracing::warn!(%error, reason = reason.as_str(), "pg notify listener recv failed; reconnecting");
                                break;
                            }
                        }
                    }
                }
            }
        }
    }

    async fn handle_payload(&self, payload: &str) {
        if is_local_notification(payload, &self.local_producer_id) {
            return;
        }

        match serde_json::from_str::<RequestEventUpdate>(payload) {
            Ok(update) => self.bus.publish(update),
            Err(error) => tracing::warn!(%error, "pg notify payload parse failed"),
        }
    }
}

fn is_local_notification(payload: &str, local_producer_id: &str) -> bool {
    serde_json::from_str::<NotifyOrigin>(payload)
        .is_ok_and(|origin| origin.producer_id == local_producer_id)
}

#[cfg(test)]
mod tests {
    use cc_lb_request_log::{RequestEventPartial, RequestEventUpdate};

    use super::is_local_notification;
    use crate::live_tail::pg_notify_fanout::protocol::InlinePartialNotify;

    #[test]
    fn local_inline_partial_is_identified_by_producer_id() {
        let update = RequestEventUpdate::Partial(RequestEventPartial {
            event_id: "event-local".to_owned(),
            ..RequestEventPartial::default()
        });
        let payload = serde_json::to_string(&InlinePartialNotify {
            update: &update,
            producer_id: "producer-a",
        })
        .expect("inline partial serializes");

        assert!(is_local_notification(&payload, "producer-a"));
        assert!(!is_local_notification(&payload, "producer-b"));
    }
}
