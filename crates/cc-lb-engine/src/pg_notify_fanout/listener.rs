use std::sync::Arc;
use std::time::Duration;

use secrecy::{ExposeSecret, SecretString};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use super::metrics::{record_http_fetch, record_pg_listener_reconnect, record_queue_usage};
use super::notifier::DEFAULT_PG_NOTIFY_CHANNEL;
use super::protocol::{NotifyOrigin, TruncatedPartialNotifyOwned};
use crate::event_bus::{RequestEventBus, RequestEventUpdate};
use crate::metrics_labels::{NotifyHttpOutcome, PgListenerReconnectReason};

const HTTP_FETCH_TIMEOUT: Duration = Duration::from_secs(3);
const QUEUE_USAGE_POLL_INTERVAL: Duration = Duration::from_secs(10);

pub struct PgListener {
    pub pg_pool: sqlx::PgPool,
    pub bus: Arc<dyn RequestEventBus>,
    pub http_client: reqwest::Client,
    pub cluster_token: SecretString,
    local_instance_url: String,
    channel: String,
}

impl PgListener {
    pub fn spawn(
        pg_pool: sqlx::PgPool,
        bus: Arc<dyn RequestEventBus>,
        http_client: reqwest::Client,
        cluster_token: SecretString,
        local_instance_url: String,
        shutdown_rx: watch::Receiver<bool>,
    ) -> JoinHandle<()> {
        Self::spawn_with_channel(
            pg_pool,
            bus,
            http_client,
            cluster_token,
            DEFAULT_PG_NOTIFY_CHANNEL.to_owned(),
            local_instance_url,
            shutdown_rx,
        )
    }

    pub fn spawn_with_channel(
        pg_pool: sqlx::PgPool,
        bus: Arc<dyn RequestEventBus>,
        http_client: reqwest::Client,
        cluster_token: SecretString,
        channel: String,
        local_instance_url: String,
        shutdown_rx: watch::Receiver<bool>,
    ) -> JoinHandle<()> {
        let listener = Self {
            pg_pool,
            bus,
            http_client,
            cluster_token,
            local_instance_url,
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
        if is_local_notification(payload, &self.local_instance_url) {
            return;
        }

        if let Ok(update) = serde_json::from_str::<RequestEventUpdate>(payload) {
            self.bus.publish(update);
            return;
        }
        let marker = match serde_json::from_str::<TruncatedPartialNotifyOwned>(payload) {
            Ok(marker) => marker,
            Err(error) => {
                tracing::warn!(%error, "pg notify payload parse failed");
                return;
            }
        };
        if marker.phase != "partial" {
            return;
        }
        self.fetch_truncated(marker).await;
    }
    async fn fetch_truncated(&self, marker: TruncatedPartialNotifyOwned) {
        let url = format!(
            "{}/internal/v1/partials/{}",
            marker.producer_url.trim_end_matches('/'),
            marker.event_id
        );
        let request = self
            .http_client
            .get(url)
            .header("X-Cluster-Token", self.cluster_token.expose_secret());
        let response = match tokio::time::timeout(HTTP_FETCH_TIMEOUT, request.send()).await {
            Ok(Ok(response)) => response,
            Ok(Err(error)) => {
                record_http_fetch(NotifyHttpOutcome::NetworkError);
                tracing::warn!(%error, "partial fetch request failed");
                return;
            }
            Err(_) => {
                record_http_fetch(NotifyHttpOutcome::Timeout);
                return;
            }
        };
        self.handle_fetch_response(response).await;
    }

    async fn handle_fetch_response(&self, response: reqwest::Response) {
        let status = response.status();
        if status == reqwest::StatusCode::NOT_FOUND {
            record_http_fetch(NotifyHttpOutcome::NotFound);
            return;
        }
        if status == reqwest::StatusCode::UNAUTHORIZED {
            record_http_fetch(NotifyHttpOutcome::Unauthorized);
            return;
        }
        if !status.is_success() {
            record_http_fetch(NotifyHttpOutcome::NetworkError);
            return;
        }
        let bytes = match response.bytes().await {
            Ok(bytes) => bytes,
            Err(error) => {
                record_http_fetch(NotifyHttpOutcome::NetworkError);
                tracing::warn!(%error, "partial fetch body read failed");
                return;
            }
        };
        match serde_json::from_slice::<RequestEventUpdate>(&bytes) {
            Ok(update) => {
                record_http_fetch(NotifyHttpOutcome::Success);
                self.bus.publish(update);
            }
            Err(error) => tracing::warn!(%error, "partial fetch body parse failed"),
        }
    }
}

fn is_local_notification(payload: &str, local_instance_url: &str) -> bool {
    serde_json::from_str::<NotifyOrigin>(payload)
        .ok()
        .and_then(|origin| origin.producer_url)
        .is_some_and(|producer_url| producer_url == local_instance_url)
}

#[cfg(test)]
mod tests {
    use cc_lb_request_log::{RequestEventPartial, RequestEventUpdate};

    use super::is_local_notification;
    use crate::pg_notify_fanout::protocol::InlinePartialNotify;

    #[test]
    fn local_inline_partial_is_identified_by_producer_url() {
        let update = RequestEventUpdate::Partial(RequestEventPartial {
            event_id: "event-local".to_owned(),
            ..RequestEventPartial::default()
        });
        let payload = serde_json::to_string(&InlinePartialNotify {
            update: &update,
            producer_url: "http://instance-a",
        })
        .expect("inline partial serializes");

        assert!(is_local_notification(&payload, "http://instance-a"));
        assert!(!is_local_notification(&payload, "http://instance-b"));
        assert!(!is_local_notification(
            &serde_json::to_string(&update).expect("legacy update serializes"),
            "http://instance-a"
        ));
    }
}
