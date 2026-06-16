use async_trait::async_trait;
use cc_lb_aead::EncryptedOAuthTokens;
use cc_lb_storage_api::upstream::{UpstreamLeaseKind, UpstreamStatusUpdate};
use cc_lb_storage_api::{
    StorageResult, UpstreamCreate, UpstreamRecord, UpstreamStore, UpstreamUpdate,
};
use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::SqliteStorage;

#[async_trait]
impl UpstreamStore for SqliteStorage {
    async fn create(&self, _create: UpstreamCreate) -> StorageResult<UpstreamRecord> {
        unimplemented!()
    }

    async fn get_by_name(&self, _name: &str) -> StorageResult<Option<UpstreamRecord>> {
        unimplemented!()
    }

    async fn get_by_id(&self, _id: Uuid) -> StorageResult<Option<UpstreamRecord>> {
        unimplemented!()
    }

    async fn list(
        &self,
        _after: Option<Uuid>,
        _limit: usize,
    ) -> StorageResult<Vec<UpstreamRecord>> {
        unimplemented!()
    }

    async fn update(
        &self,
        _id: Uuid,
        _expected_revision: u64,
        _update: UpstreamUpdate,
    ) -> StorageResult<UpstreamRecord> {
        unimplemented!()
    }

    async fn set_enabled(
        &self,
        _id: Uuid,
        _expected_revision: u64,
        _enabled: bool,
    ) -> StorageResult<UpstreamRecord> {
        unimplemented!()
    }

    async fn update_spec(
        &self,
        _id: Uuid,
        _expected_revision: u64,
        _update: UpstreamUpdate,
    ) -> StorageResult<UpstreamRecord> {
        unimplemented!()
    }

    async fn update_api_key_secret(
        &self,
        _id: Uuid,
        _api_key_ciphertext: Option<Vec<u8>>,
    ) -> StorageResult<UpstreamRecord> {
        unimplemented!()
    }

    async fn update_oauth_token(
        &self,
        _id: Uuid,
        _tokens: EncryptedOAuthTokens,
    ) -> StorageResult<UpstreamRecord> {
        unimplemented!()
    }

    async fn set_status(&self, _id: Uuid, _status: UpstreamStatusUpdate) -> StorageResult<()> {
        unimplemented!()
    }

    async fn claim_lease(
        &self,
        _id: Uuid,
        _lease_kind: UpstreamLeaseKind,
        _holder: String,
        _ttl_secs: i64,
    ) -> StorageResult<bool> {
        unimplemented!()
    }

    async fn renew_lease(
        &self,
        _id: Uuid,
        _lease_kind: UpstreamLeaseKind,
        _holder: String,
        _ttl_secs: i64,
    ) -> StorageResult<bool> {
        unimplemented!()
    }

    async fn release_lease(
        &self,
        _id: Uuid,
        _lease_kind: UpstreamLeaseKind,
        _holder: String,
    ) -> StorageResult<bool> {
        unimplemented!()
    }

    async fn store_oauth_tokens(
        &self,
        _id: Uuid,
        _expected_revision: u64,
        _tokens: EncryptedOAuthTokens,
    ) -> StorageResult<UpstreamRecord> {
        unimplemented!()
    }

    async fn claim_refresh_lease(
        &self,
        _id: Uuid,
        _holder: Uuid,
        _ttl_secs: u64,
    ) -> StorageResult<bool> {
        unimplemented!()
    }

    async fn complete_refresh(
        &self,
        _id: Uuid,
        _holder: Uuid,
        _tokens: EncryptedOAuthTokens,
    ) -> StorageResult<UpstreamRecord> {
        unimplemented!()
    }

    async fn release_lease_on_failure(
        &self,
        _id: Uuid,
        _holder: Uuid,
        _reason: String,
    ) -> StorageResult<()> {
        unimplemented!()
    }

    async fn set_last_apply_error(&self, _id: Uuid, _error: Option<String>) -> StorageResult<()> {
        unimplemented!()
    }

    async fn soft_delete(&self, _id: Uuid, _expected_revision: u64) -> StorageResult<()> {
        unimplemented!()
    }

    async fn hard_delete(&self, _id: Uuid) -> StorageResult<()> {
        unimplemented!()
    }

    async fn claim_warmup_lease(
        &self,
        _upstream_id: Uuid,
        _holder: &str,
        _ttl_secs: i64,
    ) -> StorageResult<bool> {
        unimplemented!()
    }

    async fn write_warmup_cycle_key(
        &self,
        _upstream_id: Uuid,
        _holder: &str,
        _new_cycle_key: i64,
        _next_warmup_at: Option<DateTime<Utc>>,
    ) -> StorageResult<bool> {
        unimplemented!()
    }

    async fn release_warmup_lease(&self, _id: Uuid, _holder: &str) -> StorageResult<bool> {
        unimplemented!()
    }

    async fn clear_warmup_dialect_plugin(
        &self,
        _id: Uuid,
        _expected_revision: u64,
    ) -> StorageResult<Option<UpstreamRecord>> {
        unimplemented!()
    }

    async fn warmup_now_unix_secs(&self) -> StorageResult<i64> {
        unimplemented!()
    }

    async fn write_warmup_next_at(
        &self,
        _upstream_id: Uuid,
        _holder: &str,
        _next_warmup_at: DateTime<Utc>,
    ) -> StorageResult<bool> {
        unimplemented!()
    }
}
