use async_trait::async_trait;
use cc_lb_storage_api::upstream::{
    UpstreamCreate, UpstreamRecord, UpstreamStatusUpdate, UpstreamUpdate,
};
use cc_lb_storage_api::{StorageResult, UpstreamStore};
use uuid::Uuid;

pub(crate) struct StaticUpstreamStore {
    pub(crate) upstream: UpstreamRecord,
}

#[async_trait]
impl UpstreamStore for StaticUpstreamStore {
    async fn get_by_id(&self, id: Uuid) -> StorageResult<Option<UpstreamRecord>> {
        Ok((id == self.upstream.id).then(|| self.upstream.clone()))
    }

    async fn create(&self, _create: UpstreamCreate) -> StorageResult<UpstreamRecord> {
        unreachable!("not used by keepalive dispatcher test")
    }

    async fn get_by_name(&self, _name: &str) -> StorageResult<Option<UpstreamRecord>> {
        unreachable!("not used by keepalive dispatcher test")
    }

    async fn list(
        &self,
        _after: Option<Uuid>,
        _limit: usize,
    ) -> StorageResult<Vec<UpstreamRecord>> {
        unreachable!("not used by keepalive dispatcher test")
    }

    async fn update(
        &self,
        _id: Uuid,
        _expected_revision: u64,
        _update: UpstreamUpdate,
    ) -> StorageResult<UpstreamRecord> {
        unreachable!("not used by keepalive dispatcher test")
    }

    async fn set_enabled(
        &self,
        _id: Uuid,
        _expected_revision: u64,
        _enabled: bool,
    ) -> StorageResult<UpstreamRecord> {
        unreachable!("not used by keepalive dispatcher test")
    }

    async fn update_spec(
        &self,
        _id: Uuid,
        _expected_revision: u64,
        _update: UpstreamUpdate,
    ) -> StorageResult<UpstreamRecord> {
        unreachable!("not used by keepalive dispatcher test")
    }

    async fn update_api_key_secret(
        &self,
        _id: Uuid,
        _api_key_ciphertext: Option<Vec<u8>>,
    ) -> StorageResult<UpstreamRecord> {
        unreachable!("not used by keepalive dispatcher test")
    }

    async fn update_oauth_token(
        &self,
        _id: Uuid,
        _tokens: cc_lb_aead::EncryptedOAuthTokens,
    ) -> StorageResult<UpstreamRecord> {
        unreachable!("not used by keepalive dispatcher test")
    }

    async fn set_status(&self, _id: Uuid, _status: UpstreamStatusUpdate) -> StorageResult<()> {
        unreachable!("not used by keepalive dispatcher test")
    }

    async fn store_oauth_tokens(
        &self,
        _id: Uuid,
        _expected_revision: u64,
        _tokens: cc_lb_aead::EncryptedOAuthTokens,
    ) -> StorageResult<UpstreamRecord> {
        unreachable!("not used by keepalive dispatcher test")
    }

    async fn complete_refresh(
        &self,
        _id: Uuid,
        _holder: Uuid,
        _tokens: cc_lb_aead::EncryptedOAuthTokens,
    ) -> StorageResult<UpstreamRecord> {
        unreachable!("not used by keepalive dispatcher test")
    }

    async fn set_last_apply_error(&self, _id: Uuid, _error: Option<String>) -> StorageResult<()> {
        unreachable!("not used by keepalive dispatcher test")
    }

    async fn soft_delete(&self, _id: Uuid, _expected_revision: u64) -> StorageResult<()> {
        unreachable!("not used by keepalive dispatcher test")
    }

    async fn hard_delete(&self, _id: Uuid) -> StorageResult<()> {
        unreachable!("not used by keepalive dispatcher test")
    }

    async fn clear_warmup_dialect_plugin(
        &self,
        _id: Uuid,
        _expected_revision: u64,
    ) -> StorageResult<Option<UpstreamRecord>> {
        unreachable!("not used by keepalive dispatcher test")
    }
}
