use std::{future::Future, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;
use cc_lb_storage_api::{
    BackendKind, RequestEvent, RequestEventListQuery, RequestEventStore, Storage as StorageTrait,
};

#[async_trait]
pub trait ConformanceBackend: Send + Sync + 'static {
    type Storage: StorageTrait + Send + Sync + 'static;
    type Fixture: Send + Sync;

    async fn create_fixture(&self) -> Result<Self::Fixture>;
    async fn open(&self, fixture: &Self::Fixture) -> Result<Self::Storage>;
    async fn teardown(&self, fixture: Self::Fixture) -> Result<()>;
    fn kind(&self) -> BackendKind;

    /// Waits until every request event already durably appended through
    /// `storage` is visible to a subsequent `rollup_usage_once()` call.
    /// Backends whose rollup cursor has no cross-transaction visibility
    /// horizon can rely on the default no-op. Postgres overrides this:
    /// its rollup cursor only admits events whose commit has fallen below
    /// the current MVCC snapshot xmin horizon, and that horizon is shared
    /// by every schema in the same test database, so a concurrently open
    /// transaction in a sibling test can transiently hold it back. A
    /// scenario that appends events and immediately rolls them up MUST
    /// call this first to avoid a genuine (not sleep-masked) visibility
    /// race against sibling tests sharing the same database.
    async fn wait_for_events_visible_for_rollup(&self, _storage: &Self::Storage) -> Result<()> {
        Ok(())
    }
}

pub struct ConformanceFixture<B: ConformanceBackend> {
    backend: Arc<B>,
    fixture: Option<B::Fixture>,
    storage: Arc<B::Storage>,
}

impl<B: ConformanceBackend> ConformanceFixture<B> {
    pub async fn new(backend: Arc<B>) -> Result<Self> {
        let fixture = backend.create_fixture().await?;
        let storage = Arc::new(backend.open(&fixture).await?);

        Ok(Self {
            backend,
            fixture: Some(fixture),
            storage,
        })
    }

    pub fn storage(&self) -> Arc<B::Storage> {
        Arc::clone(&self.storage)
    }

    pub fn backend_kind(&self) -> BackendKind {
        self.backend.kind()
    }

    pub async fn teardown(&mut self) -> Result<()> {
        if let Some(fixture) = self.fixture.take() {
            self.backend.teardown(fixture).await?;
        }

        Ok(())
    }
}

pub async fn with_conformance_fixture<B, F, Fut>(backend: Arc<B>, run: F) -> Result<()>
where
    B: ConformanceBackend,
    F: FnOnce(Arc<B::Storage>) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let mut fixture = ConformanceFixture::new(backend).await?;
    let result = run(fixture.storage()).await;
    let teardown = fixture.teardown().await;
    result?;
    teardown
}

pub fn scenario_applies_to_backend(kind: BackendKind, allowed: &[BackendKind]) -> bool {
    allowed.contains(&kind)
}

/// Reads every stored request event (all source kinds) oldest first: the
/// list view enumerates the rows, and the detail API returns each full
/// payload byte-identically. Every row must carry an `event_id`.
pub async fn stored_request_events<S>(storage: &S) -> Result<Vec<RequestEvent>>
where
    S: RequestEventStore + ?Sized,
{
    let items = storage
        .list_request_events(&RequestEventListQuery {
            since_unix_secs: 0,
            until_unix_secs: u64::MAX,
            limit: 500,
            source_kind: Some("all".to_owned()),
            ..Default::default()
        })
        .await?;
    let mut events = Vec::with_capacity(items.len());
    for item in items.into_iter().rev() {
        let event_id = item.event_id.ok_or_else(|| {
            anyhow::anyhow!("listed request event {} has no event_id", item.request_id)
        })?;
        let event = storage
            .get_request_event(&event_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("listed request event {event_id} has no detail row"))?;
        events.push(event);
    }
    Ok(events)
}
