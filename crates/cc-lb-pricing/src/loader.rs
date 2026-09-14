use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use cc_lb_clock::{Clock, ClockHandle};

use bytes::Bytes;
use http::Request;
use http_body_util::{BodyExt, Empty};
use hyper_rustls::HttpsConnectorBuilder;
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use tokio::time::{self, Instant};

use cc_lb_storage_api::{PriceCatalogCache, PriceCatalogSnapshotFetch};

use crate::catalog_parser::parse_litellm_json;
use crate::loader_cache::{put_storage_snapshot, read_disk_cache, write_disk_cache};
use crate::{CatalogStatus, PriceCatalog};

type HttpClient = Client<hyper_rustls::HttpsConnector<HttpConnector>, Empty<Bytes>>;

pub struct LiteLlmLoader {
    catalog: Arc<PriceCatalog>,
    storage: Arc<dyn PriceCatalogCache>,
    url: String,
    cache_path: PathBuf,
    http: HttpClient,
    last_failure_kind: Arc<Mutex<Option<String>>>,
    clock: ClockHandle,
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FetchedCatalog {
    pub fetched_at_ms: u64,
    pub fingerprint: String,
    pub bytes: Vec<u8>,
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

fn build_http_client() -> HttpClient {
    let connector = HttpsConnectorBuilder::new()
        .with_webpki_roots()
        .https_or_http()
        .enable_http1()
        .enable_http2()
        .build();
    Client::builder(TokioExecutor::new()).build(connector)
}

impl LiteLlmLoader {
    pub fn new(
        catalog: Arc<PriceCatalog>,
        storage: Arc<dyn PriceCatalogCache>,
        url: String,
        cache_path: PathBuf,
        clock: ClockHandle,
    ) -> Self {
        let http = build_http_client();

        Self {
            catalog,
            storage,
            url,
            cache_path,
            http,
            last_failure_kind: Arc::new(Mutex::new(None)),
            clock,
        }
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

    pub async fn refresh_once(&self) -> Result<u64, LoaderError> {
        let fetched = self.fetch_and_fingerprint().await?;
        let fetched_at_ms = fetched.fetched_at_ms;
        self.persist_snapshot(&fetched).await?;
        let _installed = install_cached_bytes(
            &self.catalog,
            fetched.bytes,
            Some(fetched_at_ms),
            &*self.clock,
        )?;
        self.record_success();
        Ok(fetched_at_ms)
    }

    pub async fn fetch_and_fingerprint(&self) -> Result<FetchedCatalog, LoaderError> {
        let bytes = self.fetch_bytes().await?;
        let snapshot = parse_litellm_json(&bytes, &*self.clock)?;
        Ok(FetchedCatalog {
            fetched_at_ms: snapshot.fetched_at_ms,
            fingerprint: snapshot.payload_hash,
            bytes,
        })
    }

    pub async fn persist_snapshot(&self, fetched: &FetchedCatalog) -> Result<(), LoaderError> {
        put_storage_snapshot(
            self.storage.clone(),
            fetched.bytes.clone(),
            fetched.fetched_at_ms,
        )
        .await?;
        write_disk_cache(self.cache_path.clone(), &fetched.bytes).await
    }

    async fn fetch_bytes(&self) -> Result<Vec<u8>, LoaderError> {
        let request = Request::get(self.url.as_str())
            .body(Empty::<Bytes>::new())
            .map_err(|error| LoaderError::Http(error.to_string()))?;
        let response = tokio::time::timeout(Duration::from_secs(10), self.http.request(request))
            .await
            .map_err(|_| LoaderError::Http("request timed out".to_owned()))?
            .map_err(|error| LoaderError::Http(error.to_string()))?;
        let status = response.status();
        if !status.is_success() {
            return Err(LoaderError::Http(format!("unexpected status {status}")));
        }

        let bytes = tokio::time::timeout(Duration::from_secs(10), response.into_body().collect())
            .await
            .map_err(|_| LoaderError::Http("response body timed out".to_owned()))?
            .map_err(|error| LoaderError::Http(error.to_string()))?
            .to_bytes();
        Ok(bytes.to_vec())
    }

    pub async fn install_latest_local(&self) -> Result<bool, LoaderError> {
        let current = self.catalog.current();
        let fetched = self
            .storage
            .get_price_snapshot_if_changed(&current.payload_hash)
            .await
            .map_err(|error| LoaderError::Storage(error.to_string()))?;

        match fetched {
            PriceCatalogSnapshotFetch::Changed(snapshot) => {
                return install_cached_bytes(
                    &self.catalog,
                    snapshot.json_bytes,
                    Some(snapshot.fetched_at_ms),
                    &*self.clock,
                );
            }
            PriceCatalogSnapshotFetch::Unchanged(_) => return Ok(false),
            PriceCatalogSnapshotFetch::Missing => {}
        }

        if let Some(bytes) = read_disk_cache(self.cache_path.clone()).await? {
            return install_cached_bytes(&self.catalog, bytes, None, &*self.clock);
        }

        Ok(false)
    }

    fn record_success(&self) {
        if let Ok(mut guard) = self.last_failure_kind.lock() {
            *guard = None;
        }
    }

    fn last_failure_kind(&self) -> Option<String> {
        self.last_failure_kind
            .lock()
            .ok()
            .and_then(|guard| guard.clone())
    }
}

fn install_cached_bytes(
    catalog: &Arc<PriceCatalog>,
    bytes: Vec<u8>,
    fetched_at_ms: Option<u64>,
    clock: &dyn Clock,
) -> Result<bool, LoaderError> {
    let current = catalog.current();
    if matches!(current.status, CatalogStatus::Ok) && current.raw_json == bytes {
        return Ok(false);
    }
    let mut snapshot = parse_litellm_json(&bytes, clock)?;
    if let Some(fetched_at_ms) = fetched_at_ms {
        snapshot.fetched_at_ms = fetched_at_ms;
    }
    catalog.install_snapshot(snapshot);
    Ok(true)
}
