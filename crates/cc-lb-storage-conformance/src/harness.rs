use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use cc_lb_storage_api::BackendKind;

#[async_trait]
pub trait ConformanceBackend: Send + Sync + 'static {
    type Storage: Send + Sync + 'static;
    type Fixture: Send + Sync;

    async fn create_fixture(&self) -> Result<Self::Fixture>;
    async fn open(&self, fixture: &Self::Fixture) -> Result<Self::Storage>;
    async fn teardown(&self, fixture: Self::Fixture) -> Result<()>;
    fn kind(&self) -> BackendKind;
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

pub fn scenario_applies_to_backend(kind: BackendKind, allowed: &[BackendKind]) -> bool {
    allowed.contains(&kind)
}
