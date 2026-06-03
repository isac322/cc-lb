use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use cc_lb_config::AnthropicCompatPollerConfig;
use cc_lb_core::anthropic_compat::{COMPATIBILITY_KEYS, CompatibilityKey, run_compat_fetcher};
use http_body_util::Empty;
use hyper_rustls::HttpsConnectorBuilder;
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::dynamic_view_builder::Stores;

type CompatPollerClient = Client<hyper_rustls::HttpsConnector<HttpConnector>, Empty<Bytes>>;

pub struct AnthropicCompatPoller {
    pub stores: Arc<Stores>,
    pub config: AnthropicCompatPollerConfig,
    pub client: CompatPollerClient,
}

impl AnthropicCompatPoller {
    pub fn new(stores: Arc<Stores>, config: AnthropicCompatPollerConfig) -> Self {
        let connector = HttpsConnectorBuilder::new()
            .with_webpki_roots()
            .https_or_http()
            .enable_http1()
            .enable_http2()
            .build();
        let client = Client::builder(TokioExecutor::new()).build(connector);
        Self {
            stores,
            config,
            client,
        }
    }

    pub async fn run(self: Arc<Self>, cancel: CancellationToken) {
        let tick_secs = self.config.tick_interval_secs.max(60);
        let mut interval = tokio::time::interval(Duration::from_secs(tick_secs));
        loop {
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = interval.tick() => {}
            }
            let jitter_ms = rand::random_range(0..=self.config.jitter_secs.saturating_mul(1_000));
            let jitter = tokio::time::sleep(Duration::from_millis(jitter_ms));
            tokio::pin!(jitter);
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = &mut jitter => {}
            }
            for key in COMPATIBILITY_KEYS {
                if cancel.is_cancelled() {
                    return;
                }
                self.refresh_key(key, &cancel).await;
            }
        }
    }

    async fn refresh_key(&self, key: &CompatibilityKey, cancel: &CancellationToken) {
        let now = now_unix_secs();
        let record = match self
            .stores
            .anthropic_compatibility_kv
            .get_compatibility_kv(key.name)
            .await
        {
            Ok(record) => record,
            Err(error) => {
                tracing::warn!(key = key.name, error = %error, "compatibility KV read failed");
                return;
            }
        };
        let due = record.as_ref().is_none_or(|record| {
            now >= record
                .last_updated_at_unix_secs
                .saturating_add(key.refresh_interval.as_secs())
        });
        if !due {
            return;
        }

        match run_compat_fetcher(key.fetcher, cancel).await {
            Ok(outcome) => {
                if let Err(error) = self
                    .stores
                    .anthropic_compatibility_kv
                    .put_compatibility_kv_value(
                        key.name,
                        &outcome.value,
                        now,
                        Some(outcome.source_url.as_str()),
                    )
                    .await
                {
                    tracing::warn!(key = key.name, error = %error, "compatibility KV write failed");
                }
            }
            Err(error) => {
                let error_string = error.to_string();
                if let Err(write_error) = self
                    .stores
                    .anthropic_compatibility_kv
                    .put_compatibility_kv_failure(key.name, now, &error_string)
                    .await
                {
                    tracing::warn!(key = key.name, error = %write_error, "compatibility KV failure write failed");
                }
                tracing::warn!(key = key.name, error = %error_string, "compatibility fetch failed");
            }
        }
    }
}

pub fn spawn_anthropic_compat_poller(
    poller: AnthropicCompatPoller,
    cancel: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(Arc::new(poller).run(cancel))
}

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
