use std::future::Future;
use std::time::Duration;

use cc_lb_pricing::{FetchedCatalog, LiteLlmLoader, LoaderError};
use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::idempotency::{PriceCatalogVersion, PriceCatalogVersionsStore};
use crate::middleware::TraceparentCarrier;

pub const DEFAULT_PRICE_CATALOG_SOURCE: &str = "litellm";
pub const PRICE_CATALOG_REFRESH_RETRY_DELAY: Duration = Duration::from_secs(30);

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PriceCatalogRefreshJob {
    #[serde(default = "default_price_catalog_source")]
    pub source: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub traceparent: Option<String>,
}

impl Default for PriceCatalogRefreshJob {
    fn default() -> Self {
        Self {
            source: default_price_catalog_source(),
            traceparent: None,
        }
    }
}

impl TraceparentCarrier for PriceCatalogRefreshJob {
    fn traceparent(&self) -> Option<&str> {
        self.traceparent.as_deref()
    }

    fn set_traceparent(&mut self, traceparent: Option<String>) {
        self.traceparent = traceparent;
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PriceCatalogRefreshStatus {
    Applied,
    Noop,
}

impl PriceCatalogRefreshStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Applied => "applied",
            Self::Noop => "noop",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PriceCatalogRefreshJobResult {
    Done {
        status: PriceCatalogRefreshStatus,
        fingerprint: String,
    },
    Retry {
        delay: Duration,
        error: String,
    },
}

pub trait PriceCatalogRefreshLoader {
    fn fetch_and_fingerprint(
        &self,
    ) -> impl Future<Output = std::result::Result<FetchedCatalog, LoaderError>> + Send + '_;

    fn persist_snapshot<'a>(
        &'a self,
        fetched: &'a FetchedCatalog,
    ) -> impl Future<Output = std::result::Result<(), LoaderError>> + Send + 'a;
}

impl PriceCatalogRefreshLoader for LiteLlmLoader {
    async fn fetch_and_fingerprint(&self) -> std::result::Result<FetchedCatalog, LoaderError> {
        LiteLlmLoader::fetch_and_fingerprint(self).await
    }

    async fn persist_snapshot(
        &self,
        fetched: &FetchedCatalog,
    ) -> std::result::Result<(), LoaderError> {
        LiteLlmLoader::persist_snapshot(self, fetched).await
    }
}

pub trait PriceCatalogVersionRepository: Clone + Send + Sync {
    fn read_price_catalog_version<'a>(
        &'a self,
        source: &'a str,
    ) -> impl Future<Output = Result<Option<PriceCatalogVersion>>> + Send + 'a;

    fn upsert_price_catalog_fingerprint<'a>(
        &'a self,
        source: &'a str,
        fingerprint: &'a str,
        fetched_at_unix_secs: u64,
    ) -> impl Future<Output = Result<()>> + Send + 'a;
}

#[cfg(feature = "sqlite")]
impl PriceCatalogVersionRepository for PriceCatalogVersionsStore<sqlx::Sqlite> {
    async fn read_price_catalog_version(
        &self,
        source: &str,
    ) -> Result<Option<PriceCatalogVersion>> {
        PriceCatalogVersionsStore::<sqlx::Sqlite>::read(self, source).await
    }

    async fn upsert_price_catalog_fingerprint(
        &self,
        source: &str,
        fingerprint: &str,
        fetched_at_unix_secs: u64,
    ) -> Result<()> {
        PriceCatalogVersionsStore::<sqlx::Sqlite>::upsert_fingerprint(
            self,
            source,
            fingerprint,
            fetched_at_unix_secs,
        )
        .await
    }
}

#[cfg(feature = "postgres")]
impl PriceCatalogVersionRepository for PriceCatalogVersionsStore<sqlx::Postgres> {
    async fn read_price_catalog_version(
        &self,
        source: &str,
    ) -> Result<Option<PriceCatalogVersion>> {
        PriceCatalogVersionsStore::<sqlx::Postgres>::read(self, source).await
    }

    async fn upsert_price_catalog_fingerprint(
        &self,
        source: &str,
        fingerprint: &str,
        fetched_at_unix_secs: u64,
    ) -> Result<()> {
        PriceCatalogVersionsStore::<sqlx::Postgres>::upsert_fingerprint(
            self,
            source,
            fingerprint,
            fetched_at_unix_secs,
        )
        .await
    }
}

#[derive(Clone, Debug)]
pub struct PriceCatalogRefreshJobHandler<Versions, Loader> {
    versions: Versions,
    loader: Loader,
}

impl<Versions, Loader> PriceCatalogRefreshJobHandler<Versions, Loader> {
    pub const fn new(versions: Versions, loader: Loader) -> Self {
        Self { versions, loader }
    }
}

impl<Versions, Loader> PriceCatalogRefreshJobHandler<Versions, Loader>
where
    Versions: PriceCatalogVersionRepository,
    Loader: PriceCatalogRefreshLoader + Send + Sync,
{
    pub async fn handle(
        &self,
        job: PriceCatalogRefreshJob,
        now_unix_secs: u64,
    ) -> PriceCatalogRefreshJobResult {
        if let Some(traceparent) = job.traceparent.as_deref() {
            tracing::debug!(traceparent, source = %job.source, "handling price catalog refresh job");
        }

        let prior = match self.versions.read_price_catalog_version(&job.source).await {
            Ok(prior) => prior,
            Err(error) => return retry(error),
        };
        let fetched = match self.loader.fetch_and_fingerprint().await {
            Ok(fetched) => fetched,
            Err(error) => return retry(error),
        };

        if prior
            .as_ref()
            .is_some_and(|prior| prior.fingerprint == fetched.fingerprint)
        {
            record_price_catalog_status(PriceCatalogRefreshStatus::Noop);
            return PriceCatalogRefreshJobResult::Done {
                status: PriceCatalogRefreshStatus::Noop,
                fingerprint: fetched.fingerprint,
            };
        }

        if let Err(error) = self.loader.persist_snapshot(&fetched).await {
            return retry(error);
        }
        if let Err(error) = self
            .versions
            .upsert_price_catalog_fingerprint(&job.source, &fetched.fingerprint, now_unix_secs)
            .await
        {
            return retry(error);
        }

        record_price_catalog_status(PriceCatalogRefreshStatus::Applied);
        tracing::info!(source = %job.source, fingerprint = %fetched.fingerprint, "price catalog refresh job applied");
        PriceCatalogRefreshJobResult::Done {
            status: PriceCatalogRefreshStatus::Applied,
            fingerprint: fetched.fingerprint,
        }
    }
}

fn retry(error: impl ToString) -> PriceCatalogRefreshJobResult {
    PriceCatalogRefreshJobResult::Retry {
        delay: PRICE_CATALOG_REFRESH_RETRY_DELAY,
        error: error.to_string(),
    }
}

fn record_price_catalog_status(status: PriceCatalogRefreshStatus) {
    metrics::counter!(
        "cclb_scheduler_price_catalog_status_total",
        "status" => status.as_str()
    )
    .increment(1);
}

fn default_price_catalog_source() -> String {
    DEFAULT_PRICE_CATALOG_SOURCE.to_owned()
}
