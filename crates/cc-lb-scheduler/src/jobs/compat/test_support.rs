use std::collections::HashMap;
use std::sync::Mutex;

use cc_lb_storage_api::{AnthropicCompatibilityKvStore, CompatibilityKvRecord, StorageResult};

#[derive(Default)]
pub(super) struct RecordingCompatibilityKv {
    records: Mutex<HashMap<String, CompatibilityKvRecord>>,
    value_writes: Mutex<Vec<String>>,
    failure_writes: Mutex<Vec<String>>,
}

impl RecordingCompatibilityKv {
    pub(super) fn value_write_count(&self) -> usize {
        self.value_writes.lock().expect("value writes lock").len()
    }
}

#[async_trait::async_trait]
impl AnthropicCompatibilityKvStore for RecordingCompatibilityKv {
    async fn put_compatibility_kv_value(
        &self,
        key: &str,
        value: &str,
        observed_at_unix_secs: u64,
        source_url: Option<&str>,
    ) -> StorageResult<()> {
        self.value_writes
            .lock()
            .expect("value writes lock")
            .push(key.to_owned());
        self.records.lock().expect("records lock").insert(
            key.to_owned(),
            CompatibilityKvRecord {
                key: key.to_owned(),
                value: value.to_owned(),
                last_updated_at_unix_secs: observed_at_unix_secs,
                last_attempt_at_unix_secs: observed_at_unix_secs,
                last_error: None,
                source_url: source_url.map(str::to_owned),
            },
        );
        Ok(())
    }

    async fn put_compatibility_kv_failure(
        &self,
        key: &str,
        _attempted_at_unix_secs: u64,
        error: &str,
    ) -> StorageResult<()> {
        self.failure_writes
            .lock()
            .expect("failure writes lock")
            .push(format!("{key}:{error}"));
        Ok(())
    }

    async fn get_compatibility_kv(
        &self,
        key: &str,
    ) -> StorageResult<Option<CompatibilityKvRecord>> {
        Ok(self.records.lock().expect("records lock").get(key).cloned())
    }

    async fn list_compatibility_kv(&self) -> StorageResult<Vec<CompatibilityKvRecord>> {
        Ok(self
            .records
            .lock()
            .expect("records lock")
            .values()
            .cloned()
            .collect())
    }
}
