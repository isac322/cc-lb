use std::collections::HashMap;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use reqwest::Client;
use serde_json::Value;
use tokio::task::JoinHandle;
use tokio::time::{self, Instant, MissedTickBehavior};
use tracing::{error, info, warn};

use crate::{CatalogSnapshot, CatalogStatus, PriceCatalog, Pricing, UsdPerMillion};

pub struct LiteLlmLoader {
    catalog: Arc<PriceCatalog>,
    storage: Arc<cc_lb_storage_redb::Storage>,
    url: String,
    refresh_interval: Duration,
    cache_path: PathBuf,
    http: Client,
    failure_count: Arc<AtomicU64>,
    last_failure_kind: Arc<Mutex<Option<String>>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PriceCatalogStatus {
    Ok {
        fetched_at_ms: u64,
    },
    Stale {
        fetched_at_ms: u64,
        last_failure_kind: String,
    },
    CostDisabled,
}

#[derive(thiserror::Error, Debug)]
pub enum LoaderError {
    #[error("http error: {0}")]
    Http(String),
    #[error("json error: {0}")]
    Json(String),
    #[error("validation error: {0}")]
    Validation(String),
    #[error("storage error: {0}")]
    Storage(String),
}

impl LiteLlmLoader {
    pub fn new(
        catalog: Arc<PriceCatalog>,
        storage: Arc<cc_lb_storage_redb::Storage>,
        url: String,
        refresh_interval: Duration,
        cache_path: PathBuf,
    ) -> Self {
        let http = Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .expect("reqwest client should build with rustls-tls");

        Self {
            catalog,
            storage,
            url,
            refresh_interval,
            cache_path,
            http,
            failure_count: Arc::new(AtomicU64::new(0)),
            last_failure_kind: Arc::new(Mutex::new(None)),
        }
    }

    pub fn start_daemon(self) -> JoinHandle<()> {
        tokio::spawn(async move {
            self.cold_start().await;

            let mut interval = time::interval(self.refresh_interval);
            interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
            interval.tick().await;

            loop {
                interval.tick().await;
                if let Err(error) = self.fetch_install_persist().await {
                    self.record_failure(&error);
                    let failures = self.failure_count.load(Ordering::Relaxed);
                    warn!(
                        error = %error,
                        failures,
                        "litellm price catalog refresh failed"
                    );
                }
            }
        })
    }

    pub async fn wait_for_first_snapshot(catalog: &Arc<PriceCatalog>, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;

        loop {
            if matches!(catalog.status(), CatalogStatus::Ok) {
                return true;
            }

            let now = Instant::now();
            if now >= deadline {
                return false;
            }

            let remaining = deadline.saturating_duration_since(now);
            time::sleep(remaining.min(Duration::from_millis(50))).await;
        }
    }

    pub fn current_status(&self) -> PriceCatalogStatus {
        let snapshot = self.catalog.current();
        match snapshot.status {
            CatalogStatus::Ok => {
                if let Some(last_failure_kind) = self.last_failure_kind() {
                    PriceCatalogStatus::Stale {
                        fetched_at_ms: snapshot.fetched_at_ms,
                        last_failure_kind,
                    }
                } else {
                    PriceCatalogStatus::Ok {
                        fetched_at_ms: snapshot.fetched_at_ms,
                    }
                }
            }
            CatalogStatus::Stale => PriceCatalogStatus::Stale {
                fetched_at_ms: snapshot.fetched_at_ms,
                last_failure_kind: self
                    .last_failure_kind()
                    .unwrap_or_else(|| "unknown".to_owned()),
            },
            CatalogStatus::CostDisabled => PriceCatalogStatus::CostDisabled,
        }
    }

    async fn cold_start(&self) {
        let mut last_error = None;
        for attempt in 0..3 {
            match self.fetch_install_persist().await {
                Ok(fetched_at_ms) => {
                    self.record_success();
                    info!(fetched_at_ms, "litellm price catalog cold start fetched");
                    return;
                }
                Err(error) => {
                    self.record_failure(&error);
                    warn!(
                        attempt = attempt + 1,
                        error = %error,
                        "litellm price catalog cold start fetch failed"
                    );
                    last_error = Some(error);
                    if attempt < 2 {
                        time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        }

        match self.install_from_cache().await {
            Ok(Some(fetched_at_ms)) => {
                warn!(
                    fetched_at_ms,
                    "using disk cache for litellm price catalog cold start"
                );
            }
            Ok(None) => {
                self.catalog
                    .install_snapshot(CatalogSnapshot::empty_cost_disabled());
                error!(
                    last_error = ?last_error,
                    "litellm price catalog unavailable; cost pricing disabled"
                );
            }
            Err(error) => {
                self.record_failure(&error);
                self.catalog
                    .install_snapshot(CatalogSnapshot::empty_cost_disabled());
                error!(
                    error = %error,
                    last_error = ?last_error,
                    "litellm price catalog cache fallback failed; cost pricing disabled"
                );
            }
        }
    }

    async fn fetch_install_persist(&self) -> Result<u64, LoaderError> {
        let bytes = self.fetch_bytes().await?;
        let mut snapshot = parse_litellm_json(&bytes)?;
        let fetched_at_ms = snapshot.fetched_at_ms;
        snapshot.raw_json = bytes.clone();
        self.catalog.install_snapshot(snapshot);
        put_storage_snapshot(self.storage.clone(), bytes.clone(), fetched_at_ms).await?;
        write_disk_cache(self.cache_path.clone(), &bytes).await?;
        self.record_success();
        Ok(fetched_at_ms)
    }

    async fn fetch_bytes(&self) -> Result<Vec<u8>, LoaderError> {
        let response = self
            .http
            .get(&self.url)
            .send()
            .await
            .map_err(|error| LoaderError::Http(error.to_string()))?;
        let status = response.status();
        if !status.is_success() {
            return Err(LoaderError::Http(format!("unexpected status {status}")));
        }

        let bytes = response
            .bytes()
            .await
            .map_err(|error| LoaderError::Http(error.to_string()))?;
        Ok(bytes.to_vec())
    }

    async fn install_from_cache(&self) -> Result<Option<u64>, LoaderError> {
        if let Some(bytes) = read_disk_cache(self.cache_path.clone()).await? {
            return install_cached_bytes(&self.catalog, bytes, None).map(Some);
        }

        let Some(snapshot) = get_storage_snapshot(self.storage.clone()).await? else {
            return Ok(None);
        };
        let fetched_at_ms = snapshot.fetched_at_ms;
        install_cached_bytes(&self.catalog, snapshot.json_bytes, Some(fetched_at_ms)).map(Some)
    }

    fn record_success(&self) {
        if let Ok(mut guard) = self.last_failure_kind.lock() {
            *guard = None;
        }
    }

    fn record_failure(&self, error: &LoaderError) {
        self.failure_count.fetch_add(1, Ordering::Relaxed);
        metrics::counter!("cclb_price_catalog_refresh_failures_total").increment(1);
        if matches!(error, LoaderError::Validation(_)) {
            metrics::counter!("cclb_price_catalog_validation_failures_total").increment(1);
        }
        if let Ok(mut guard) = self.last_failure_kind.lock() {
            *guard = Some(error.kind().to_owned());
        }
    }

    fn last_failure_kind(&self) -> Option<String> {
        self.last_failure_kind
            .lock()
            .ok()
            .and_then(|guard| guard.clone())
    }
}

impl LoaderError {
    fn kind(&self) -> &'static str {
        match self {
            Self::Http(_) => "http",
            Self::Json(_) => "json",
            Self::Validation(_) => "validation",
            Self::Storage(_) => "storage",
        }
    }
}

fn parse_litellm_json(bytes: &[u8]) -> Result<CatalogSnapshot, LoaderError> {
    let root: HashMap<String, Value> =
        serde_json::from_slice(bytes).map_err(|error| LoaderError::Json(error.to_string()))?;
    let mut models = HashMap::new();
    let mut cache_creation_per_million_usd = HashMap::new();
    let mut cache_read_per_million_usd = HashMap::new();

    for (model, value) in root {
        if model == "sample_spec" {
            continue;
        }

        let Some(object) = value.as_object() else {
            continue;
        };
        let Some(input_cost) = object.get("input_cost_per_token").and_then(Value::as_f64) else {
            record_missing_catalog_field(&model, "input_cost_per_token");
            continue;
        };
        let Some(output_cost) = object.get("output_cost_per_token").and_then(Value::as_f64) else {
            record_missing_catalog_field(&model, "output_cost_per_token");
            continue;
        };

        let input_per_million_usd =
            usd_per_token_to_per_million(&model, "input_cost_per_token", input_cost)?;
        let output_per_million_usd =
            usd_per_token_to_per_million(&model, "output_cost_per_token", output_cost)?;

        if let Some(cache_creation_cost) = object
            .get("cache_creation_input_token_cost")
            .and_then(Value::as_f64)
        {
            cache_creation_per_million_usd.insert(
                model.clone(),
                usd_per_token_to_per_million(
                    &model,
                    "cache_creation_input_token_cost",
                    cache_creation_cost,
                )?,
            );
        }

        if let Some(cache_read_cost) = object
            .get("cache_read_input_token_cost")
            .and_then(Value::as_f64)
        {
            cache_read_per_million_usd.insert(
                model.clone(),
                usd_per_token_to_per_million(
                    &model,
                    "cache_read_input_token_cost",
                    cache_read_cost,
                )?,
            );
        }

        models.insert(
            model.clone(),
            Pricing {
                model,
                input_per_million_usd,
                output_per_million_usd,
            },
        );
    }

    if models.is_empty() {
        return Err(LoaderError::Validation(
            "litellm catalog contains no models with input_cost_per_token and output_cost_per_token"
                .to_owned(),
        ));
    }

    Ok(CatalogSnapshot {
        fetched_at_ms: now_ms(),
        models,
        raw_json: bytes.to_vec(),
        cache_creation_per_million_usd,
        cache_read_per_million_usd,
        status: CatalogStatus::Ok,
    })
}

fn usd_per_token_to_per_million(
    model: &str,
    field: &str,
    cost_per_token: f64,
) -> Result<UsdPerMillion, LoaderError> {
    if !cost_per_token.is_finite() || cost_per_token < 0.0 {
        return Err(LoaderError::Validation(format!(
            "invalid {field} for model {model}"
        )));
    }

    let micros_per_million = cost_per_token * 1_000_000.0 * 1_000_000.0;
    if micros_per_million > u64::MAX as f64 {
        return Err(LoaderError::Validation(format!(
            "{field} for model {model} exceeds u64 micros"
        )));
    }

    Ok(UsdPerMillion(micros_per_million.round() as u64))
}

fn record_missing_catalog_field(model: &str, field: &'static str) {
    metrics::counter!(
        "cclb_price_catalog_missing_field_total",
        "model" => model.to_owned(),
        "field" => field
    )
    .increment(1);
}

async fn put_storage_snapshot(
    storage: Arc<cc_lb_storage_redb::Storage>,
    bytes: Vec<u8>,
    fetched_at_ms: u64,
) -> Result<(), LoaderError> {
    tokio::task::spawn_blocking(move || storage.put_price_snapshot(&bytes, fetched_at_ms))
        .await
        .map_err(|error| LoaderError::Storage(error.to_string()))?
        .map_err(|error| LoaderError::Storage(error.to_string()))
}

async fn get_storage_snapshot(
    storage: Arc<cc_lb_storage_redb::Storage>,
) -> Result<Option<cc_lb_storage_redb::PriceSnapshot>, LoaderError> {
    tokio::task::spawn_blocking(move || storage.get_price_snapshot())
        .await
        .map_err(|error| LoaderError::Storage(error.to_string()))?
        .map_err(|error| LoaderError::Storage(error.to_string()))
}

async fn read_disk_cache(cache_path: PathBuf) -> Result<Option<Vec<u8>>, LoaderError> {
    match tokio::fs::read(&cache_path).await {
        Ok(bytes) => Ok(Some(bytes)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(LoaderError::Storage(format!(
            "read {}: {error}",
            cache_path.display()
        ))),
    }
}

async fn write_disk_cache(cache_path: PathBuf, bytes: &[u8]) -> Result<(), LoaderError> {
    if let Some(parent) = cache_path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|error| LoaderError::Storage(error.to_string()))?;
    }

    let tmp_path = tmp_cache_path(&cache_path);
    tokio::fs::write(&tmp_path, bytes)
        .await
        .map_err(|error| LoaderError::Storage(error.to_string()))?;
    tokio::fs::rename(&tmp_path, &cache_path)
        .await
        .map_err(|error| LoaderError::Storage(error.to_string()))
}

fn install_cached_bytes(
    catalog: &Arc<PriceCatalog>,
    bytes: Vec<u8>,
    fetched_at_ms: Option<u64>,
) -> Result<u64, LoaderError> {
    let mut snapshot = parse_litellm_json(&bytes)?;
    if let Some(fetched_at_ms) = fetched_at_ms {
        snapshot.fetched_at_ms = fetched_at_ms;
    }
    let fetched_at_ms = snapshot.fetched_at_ms;
    catalog.install_snapshot(snapshot);
    Ok(fetched_at_ms)
}

fn tmp_cache_path(cache_path: &Path) -> PathBuf {
    let mut tmp = OsString::from(cache_path.as_os_str());
    tmp.push(".tmp");
    PathBuf::from(tmp)
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().try_into().unwrap_or(u64::MAX))
        .unwrap_or(0)
}
