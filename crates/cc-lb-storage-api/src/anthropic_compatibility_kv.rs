use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::StorageResult;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompatibilityKvRecord {
    pub key: String,
    pub value: String,
    pub last_updated_at_unix_secs: u64,
    pub last_attempt_at_unix_secs: u64,
    pub last_error: Option<String>,
    pub source_url: Option<String>,
}

#[async_trait]
pub trait AnthropicCompatibilityKvStore: Send + Sync {
    async fn put_compatibility_kv_value(
        &self,
        key: &str,
        value: &str,
        observed_at_unix_secs: u64,
        source_url: Option<&str>,
    ) -> StorageResult<()>;

    async fn put_compatibility_kv_failure(
        &self,
        key: &str,
        attempted_at_unix_secs: u64,
        error: &str,
    ) -> StorageResult<()>;

    async fn get_compatibility_kv(&self, key: &str)
    -> StorageResult<Option<CompatibilityKvRecord>>;

    async fn list_compatibility_kv(&self) -> StorageResult<Vec<CompatibilityKvRecord>>;
}
