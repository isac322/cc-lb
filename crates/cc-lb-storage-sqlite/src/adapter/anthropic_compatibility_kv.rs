use async_trait::async_trait;
use cc_lb_storage_api::{AnthropicCompatibilityKvStore, CompatibilityKvRecord, StorageResult};

use crate::SqliteStorage;

#[async_trait]
impl AnthropicCompatibilityKvStore for SqliteStorage {
    async fn put_compatibility_kv_value(
        &self,
        _key: &str,
        _value: &str,
        _observed_at_unix_secs: u64,
        _source_url: Option<&str>,
    ) -> StorageResult<()> {
        unimplemented!()
    }

    async fn put_compatibility_kv_failure(
        &self,
        _key: &str,
        _attempted_at_unix_secs: u64,
        _error: &str,
    ) -> StorageResult<()> {
        unimplemented!()
    }

    async fn get_compatibility_kv(
        &self,
        _key: &str,
    ) -> StorageResult<Option<CompatibilityKvRecord>> {
        unimplemented!()
    }

    async fn list_compatibility_kv(&self) -> StorageResult<Vec<CompatibilityKvRecord>> {
        unimplemented!()
    }
}
