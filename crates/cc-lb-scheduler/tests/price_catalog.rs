#![allow(clippy::manual_async_fn, clippy::too_many_arguments)]

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use cc_lb_pricing::{FetchedCatalog, LoaderError};
use cc_lb_scheduler::error::Result;
use cc_lb_scheduler::jobs::price_catalog::{
    PriceCatalogRefreshJob, PriceCatalogRefreshJobHandler, PriceCatalogRefreshJobResult,
    PriceCatalogRefreshLoader, PriceCatalogRefreshStatus,
};
#[cfg(feature = "sqlite")]
use cc_lb_scheduler::state_stores::PriceCatalogVersionsStore as SqlitePriceCatalogVersionsStore;
#[cfg(feature = "postgres")]
use cc_lb_scheduler::state_stores::PriceCatalogVersionsStore;
type MaybePriceCatalogVersion = Option<cc_lb_scheduler::state_stores::PriceCatalogVersion>;

mod jobs {
    pub mod price_catalog {
        use super::super::*;

        #[tokio::test]
        async fn t2__noop_skips_snapshot_and_cache_persistence() -> Result<()> {
            let versions = RecordingPriceCatalogVersions::default();

            run_noop_case(versions.clone()).await?;

            assert_eq!(
                versions.read_sources(),
                vec!["litellm".to_owned(), "litellm".to_owned()]
            );
            assert_eq!(
                versions.upserts(),
                vec![PriceCatalogUpsert {
                    source: "litellm".to_owned(),
                    fingerprint: "same".to_owned(),
                    fetched_at_unix_secs: 100,
                }]
            );
            Ok(())
        }

        #[tokio::test]
        async fn t2__changed_catalog_persists_and_updates_fingerprint() -> Result<()> {
            let versions = RecordingPriceCatalogVersions::default();

            run_changed_case(versions.clone()).await?;

            assert_eq!(
                versions.read_sources(),
                vec!["litellm".to_owned(), "litellm".to_owned()]
            );
            assert_eq!(
                versions.upserts(),
                vec![
                    PriceCatalogUpsert {
                        source: "litellm".to_owned(),
                        fingerprint: "old".to_owned(),
                        fetched_at_unix_secs: 100,
                    },
                    PriceCatalogUpsert {
                        source: "litellm".to_owned(),
                        fingerprint: "new".to_owned(),
                        fetched_at_unix_secs: 300,
                    },
                ]
            );
            Ok(())
        }

        #[cfg(feature = "sqlite")]
        #[tokio::test]
        async fn t3__sqlite_noop_skips_snapshot_and_cache_persistence() -> Result<()> {
            run_noop_case(sqlite_price_catalog_versions().await?).await
        }

        #[cfg(feature = "sqlite")]
        #[tokio::test]
        async fn t3__sqlite_changed_catalog_persists_and_updates_fingerprint() -> Result<()> {
            run_changed_case(sqlite_price_catalog_versions().await?).await
        }

        #[cfg(feature = "postgres")]
        #[tokio::test]
        async fn t3_postgres__noop_skips_snapshot_and_cache_persistence() -> anyhow::Result<()> {
            let (fixture, pool) = postgres_fixture().await?;
            let result = run_noop_case(PriceCatalogVersionsStore::new(pool.clone())).await;
            pool.close().await;
            fixture.teardown().await?;
            result?;
            Ok(())
        }

        #[cfg(feature = "postgres")]
        #[tokio::test]
        async fn t3_postgres__changed_catalog_persists_and_updates_fingerprint()
        -> anyhow::Result<()> {
            let (fixture, pool) = postgres_fixture().await?;
            let result = run_changed_case(PriceCatalogVersionsStore::new(pool.clone())).await;
            pool.close().await;
            fixture.teardown().await?;
            result?;
            Ok(())
        }
    }
}

#[derive(Clone, Default)]
struct RecordingPriceCatalogVersions {
    state: Arc<Mutex<RecordingPriceCatalogVersionsState>>,
}

#[derive(Default)]
struct RecordingPriceCatalogVersionsState {
    versions: BTreeMap<String, cc_lb_scheduler::state_stores::PriceCatalogVersion>,
    read_sources: Vec<String>,
    upserts: Vec<PriceCatalogUpsert>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PriceCatalogUpsert {
    source: String,
    fingerprint: String,
    fetched_at_unix_secs: u64,
}

impl RecordingPriceCatalogVersions {
    fn read_sources(&self) -> Vec<String> {
        self.state
            .lock()
            .expect("price catalog versions lock")
            .read_sources
            .clone()
    }

    fn upserts(&self) -> Vec<PriceCatalogUpsert> {
        self.state
            .lock()
            .expect("price catalog versions lock")
            .upserts
            .clone()
    }
}

impl cc_lb_scheduler::jobs::price_catalog::PriceCatalogVersionRepository
    for RecordingPriceCatalogVersions
{
    async fn read_price_catalog_version(&self, source: &str) -> Result<MaybePriceCatalogVersion> {
        let mut state = self.state.lock().expect("price catalog versions lock");
        state.read_sources.push(source.to_owned());
        Ok(state.versions.get(source).cloned())
    }

    async fn upsert_price_catalog_fingerprint(
        &self,
        source: &str,
        fingerprint: &str,
        fetched_at_unix_secs: u64,
    ) -> Result<()> {
        let mut state = self.state.lock().expect("price catalog versions lock");
        state.upserts.push(PriceCatalogUpsert {
            source: source.to_owned(),
            fingerprint: fingerprint.to_owned(),
            fetched_at_unix_secs,
        });
        state.versions.insert(
            source.to_owned(),
            cc_lb_scheduler::state_stores::PriceCatalogVersion {
                source: source.to_owned(),
                fingerprint: fingerprint.to_owned(),
                fetched_at_unix_secs,
            },
        );
        Ok(())
    }
}

async fn run_noop_case<V>(versions: V) -> Result<()>
where
    V: cc_lb_scheduler::jobs::price_catalog::PriceCatalogVersionRepository,
{
    versions
        .upsert_price_catalog_fingerprint("litellm", "same", 100)
        .await?;
    let loader = RecordingLoader::new(FetchedCatalog {
        fetched_at_ms: 200_000,
        fingerprint: "same".to_owned(),
        bytes: SAMPLE_LITELLM_JSON.as_bytes().to_vec(),
    });
    let calls = loader.calls.clone();
    let handler = PriceCatalogRefreshJobHandler::new(versions.clone(), loader);

    let result = handler.handle(PriceCatalogRefreshJob::default(), 200).await;

    assert_eq!(
        result,
        PriceCatalogRefreshJobResult::Done {
            status: PriceCatalogRefreshStatus::Noop,
            fingerprint: "same".to_owned(),
        }
    );
    assert_eq!(calls.fetches.load(Ordering::SeqCst), 1);
    assert_eq!(calls.persists.load(Ordering::SeqCst), 0);
    assert_eq!(calls.persisted_fingerprints(), Vec::<String>::new());
    assert_eq!(
        versions.read_price_catalog_version("litellm").await?,
        Some(cc_lb_scheduler::state_stores::PriceCatalogVersion {
            source: "litellm".to_owned(),
            fingerprint: "same".to_owned(),
            fetched_at_unix_secs: 100,
        })
    );
    Ok(())
}

async fn run_changed_case<V>(versions: V) -> Result<()>
where
    V: cc_lb_scheduler::jobs::price_catalog::PriceCatalogVersionRepository,
{
    versions
        .upsert_price_catalog_fingerprint("litellm", "old", 100)
        .await?;
    let loader = RecordingLoader::new(FetchedCatalog {
        fetched_at_ms: 300_000,
        fingerprint: "new".to_owned(),
        bytes: SAMPLE_LITELLM_JSON.as_bytes().to_vec(),
    });
    let calls = loader.calls.clone();
    let handler = PriceCatalogRefreshJobHandler::new(versions.clone(), loader);

    let result = handler.handle(PriceCatalogRefreshJob::default(), 300).await;

    assert_eq!(
        result,
        PriceCatalogRefreshJobResult::Done {
            status: PriceCatalogRefreshStatus::Applied,
            fingerprint: "new".to_owned(),
        }
    );
    assert_eq!(calls.fetches.load(Ordering::SeqCst), 1);
    assert_eq!(calls.persists.load(Ordering::SeqCst), 1);
    assert_eq!(calls.persisted_fingerprints(), vec!["new".to_owned()]);
    assert_eq!(
        versions.read_price_catalog_version("litellm").await?,
        Some(cc_lb_scheduler::state_stores::PriceCatalogVersion {
            source: "litellm".to_owned(),
            fingerprint: "new".to_owned(),
            fetched_at_unix_secs: 300,
        })
    );
    Ok(())
}

#[derive(Clone)]
struct RecordingLoader {
    fetched: FetchedCatalog,
    calls: Arc<RecordingCalls>,
}

impl RecordingLoader {
    fn new(fetched: FetchedCatalog) -> Self {
        Self {
            fetched,
            calls: Arc::new(RecordingCalls::default()),
        }
    }
}

impl PriceCatalogRefreshLoader for RecordingLoader {
    fn fetch_and_fingerprint(
        &self,
    ) -> impl Future<Output = std::result::Result<FetchedCatalog, LoaderError>> + Send + '_ {
        async move {
            self.calls.fetches.fetch_add(1, Ordering::SeqCst);
            Ok(self.fetched.clone())
        }
    }

    fn persist_snapshot<'a>(
        &'a self,
        fetched: &'a FetchedCatalog,
    ) -> impl Future<Output = std::result::Result<(), LoaderError>> + Send + 'a {
        async move {
            self.calls.persists.fetch_add(1, Ordering::SeqCst);
            self.calls
                .fingerprints
                .lock()
                .expect("fingerprints lock")
                .push(fetched.fingerprint.clone());
            Ok(())
        }
    }
}

#[derive(Default)]
struct RecordingCalls {
    fetches: AtomicUsize,
    persists: AtomicUsize,
    fingerprints: Mutex<Vec<String>>,
}

impl RecordingCalls {
    fn persisted_fingerprints(&self) -> Vec<String> {
        self.fingerprints.lock().expect("fingerprints lock").clone()
    }
}

#[cfg(feature = "sqlite")]
async fn sqlite_price_catalog_versions() -> Result<SqlitePriceCatalogVersionsStore<sqlx::Sqlite>> {
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await?;
    sqlx::raw_sql(include_str!(
        "../migrations/sqlite/0002_idempotency_tables.sql"
    ))
    .execute(&pool)
    .await?;
    Ok(SqlitePriceCatalogVersionsStore::new(pool))
}

#[cfg(feature = "postgres")]
async fn postgres_fixture()
-> anyhow::Result<(cc_lb_storage_conformance::PostgresFixture, sqlx::PgPool)> {
    let fixture = crate::postgres_fixture().await?;
    let pool = crate::scheduler_postgres_pool(&fixture).await?;
    sqlx::raw_sql(include_str!(
        "../migrations/postgres/0002_idempotency_tables.sql"
    ))
    .execute(&pool)
    .await?;
    Ok((fixture, pool))
}

const SAMPLE_LITELLM_JSON: &str = r#"
{
  "claude-3-5-sonnet-20241022": {"input_cost_per_token": 0.000003, "output_cost_per_token": 0.000015}
}
"#;
