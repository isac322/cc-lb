#![allow(deprecated, clippy::manual_async_fn, clippy::too_many_arguments)]

use std::future::Future;
#[cfg(feature = "postgres")]
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use cc_lb_pricing::{FetchedCatalog, LoaderError};
use cc_lb_scheduler::error::Result;
use cc_lb_scheduler::jobs::price_catalog::{
    PriceCatalogRefreshJob, PriceCatalogRefreshJobHandler, PriceCatalogRefreshJobResult,
    PriceCatalogRefreshLoader, PriceCatalogRefreshStatus,
};
use cc_lb_scheduler::state_stores::PriceCatalogVersionsStore;

mod jobs {
    pub mod price_catalog {
        use super::super::*;

        #[cfg(feature = "sqlite")]
        #[tokio::test]
        async fn sqlite_noop_skips_snapshot_and_cache_persistence() -> Result<()> {
            let pool = sqlite_memory().await?;
            run_noop_case(PriceCatalogVersionsStore::new(pool)).await
        }

        #[cfg(feature = "sqlite")]
        #[tokio::test]
        async fn sqlite_changed_catalog_persists_and_updates_fingerprint() -> Result<()> {
            let pool = sqlite_memory().await?;
            run_changed_case(PriceCatalogVersionsStore::new(pool)).await
        }

        #[cfg(feature = "postgres")]
        #[tokio::test]
        async fn postgres_noop_skips_snapshot_and_cache_persistence() -> Result<()> {
            let Some(fixture) = PostgresFixture::create().await? else {
                eprintln!("SKIP: DATABASE_URL not set - skipping postgres price catalog test");
                return Ok(());
            };
            let result = run_noop_case(PriceCatalogVersionsStore::new(fixture.pool.clone())).await;
            fixture.drop_schema().await?;
            result
        }

        #[cfg(feature = "postgres")]
        #[tokio::test]
        async fn postgres_changed_catalog_persists_and_updates_fingerprint() -> Result<()> {
            let Some(fixture) = PostgresFixture::create().await? else {
                eprintln!("SKIP: DATABASE_URL not set - skipping postgres price catalog test");
                return Ok(());
            };
            let result =
                run_changed_case(PriceCatalogVersionsStore::new(fixture.pool.clone())).await;
            fixture.drop_schema().await?;
            result
        }
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
async fn sqlite_memory() -> Result<sqlx::SqlitePool> {
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await?;
    sqlx::raw_sql(include_str!(
        "../migrations/sqlite/0002_idempotency_tables.sql"
    ))
    .execute(&pool)
    .await?;
    Ok(pool)
}

#[cfg(feature = "postgres")]
struct PostgresFixture {
    schema: String,
    admin: sqlx::PgPool,
    pool: sqlx::PgPool,
}

#[cfg(feature = "postgres")]
impl PostgresFixture {
    async fn create() -> Result<Option<Self>> {
        let Ok(url) = std::env::var("DATABASE_URL") else {
            return Ok(None);
        };
        let admin = sqlx::PgPool::connect(&url).await?;
        let schema = format!("price_catalog_{}", uuid::Uuid::new_v4().simple());
        sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(&admin)
            .await?;
        let options = sqlx::postgres::PgConnectOptions::from_str(&url)?
            .options([("search_path", schema.as_str())]);
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(2)
            .connect_with(options)
            .await?;
        sqlx::raw_sql(include_str!(
            "../migrations/postgres/0002_idempotency_tables.sql"
        ))
        .execute(&pool)
        .await?;
        Ok(Some(Self {
            schema,
            admin,
            pool,
        }))
    }

    async fn drop_schema(self) -> Result<()> {
        self.pool.close().await;
        sqlx::query(&format!("DROP SCHEMA {} CASCADE", self.schema))
            .execute(&self.admin)
            .await?;
        self.admin.close().await;
        Ok(())
    }
}

const SAMPLE_LITELLM_JSON: &str = r#"
{
  "claude-3-5-sonnet-20241022": {"input_cost_per_token": 0.000003, "output_cost_per_token": 0.000015}
}
"#;
