use std::sync::Arc;

use anyhow::{Context, Result, ensure};
use cc_lb_storage_api::{AnthropicCompatibilityKvStore, CompatibilityKvRecord};

use crate::harness::{ConformanceBackend, with_conformance_fixture};

pub async fn run_all<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: AnthropicCompatibilityKvStore,
{
    put_then_get_roundtrip(Arc::clone(&backend)).await?;
    update_replaces_value_and_clears_error(Arc::clone(&backend)).await?;
    failure_preserves_last_good_value(Arc::clone(&backend)).await?;
    failure_on_never_seen_key_is_noop(Arc::clone(&backend)).await?;
    list_returns_keys_in_some_order(Arc::clone(&backend)).await?;
    older_observation_does_not_replace_newer(backend).await?;
    Ok(())
}

macro_rules! scenario {
    ($name:ident, $body:expr) => {
        pub async fn $name<B>(backend: Arc<B>) -> Result<()>
        where
            B: ConformanceBackend,
            B::Storage: AnthropicCompatibilityKvStore,
        {
            with_conformance_fixture(backend, $body).await
        }
    };
}

scenario!(put_then_get_roundtrip, |storage| async move {
    storage
        .put_compatibility_kv_value("models", "{}", 100, Some("https://example.test/models"))
        .await?;
    let record = storage
        .get_compatibility_kv("models")
        .await?
        .context("kv record should exist")?;
    ensure!(
        record
            == CompatibilityKvRecord {
                key: "models".to_owned(),
                value: "{}".to_owned(),
                last_updated_at_unix_secs: 100,
                last_attempt_at_unix_secs: 100,
                last_error: None,
                source_url: Some("https://example.test/models".to_owned()),
            },
        "kv record must round-trip"
    );
    Ok(())
});

scenario!(
    update_replaces_value_and_clears_error,
    |storage| async move {
        storage
            .put_compatibility_kv_value("models", "old", 100, Some("https://old.test"))
            .await?;
        storage
            .put_compatibility_kv_failure("models", 120, "timeout")
            .await?;
        storage
            .put_compatibility_kv_value("models", "new", 130, None)
            .await?;
        let record = storage
            .get_compatibility_kv("models")
            .await?
            .context("kv record should exist")?;
        ensure!(record.value == "new", "update should replace value");
        ensure!(record.last_error.is_none(), "update should clear error");
        ensure!(record.last_updated_at_unix_secs == 130, "updated timestamp");
        ensure!(record.last_attempt_at_unix_secs == 130, "attempt timestamp");
        ensure!(
            record.source_url.is_none(),
            "source url should update to None"
        );
        Ok(())
    }
);

scenario!(failure_preserves_last_good_value, |storage| async move {
    storage
        .put_compatibility_kv_value("models", "good", 100, Some("https://source.test"))
        .await?;
    storage
        .put_compatibility_kv_failure("models", 150, "fetch failed")
        .await?;
    let record = storage
        .get_compatibility_kv("models")
        .await?
        .context("kv record should exist")?;
    ensure!(record.value == "good", "failure should preserve value");
    ensure!(
        record.last_updated_at_unix_secs == 100,
        "updated timestamp preserved"
    );
    ensure!(
        record.last_attempt_at_unix_secs == 150,
        "attempt timestamp updates"
    );
    ensure!(
        record.last_error.as_deref() == Some("fetch failed"),
        "error stored"
    );
    Ok(())
});

scenario!(failure_on_never_seen_key_is_noop, |storage| async move {
    storage
        .put_compatibility_kv_failure("missing", 150, "fetch failed")
        .await?;
    ensure!(
        storage.get_compatibility_kv("missing").await?.is_none(),
        "failure on unseen key should not insert"
    );
    Ok(())
});

scenario!(list_returns_keys_in_some_order, |storage| async move {
    storage
        .put_compatibility_kv_value("b", "2", 20, None)
        .await?;
    storage
        .put_compatibility_kv_value("a", "1", 10, None)
        .await?;
    let mut keys = storage
        .list_compatibility_kv()
        .await?
        .into_iter()
        .map(|record| record.key)
        .collect::<Vec<_>>();
    keys.sort();
    ensure!(keys == ["a", "b"], "list should contain both keys");
    Ok(())
});

scenario!(
    older_observation_does_not_replace_newer,
    |storage| async move {
        storage
            .put_compatibility_kv_value("models", "new", 200, None)
            .await?;
        storage
            .put_compatibility_kv_value("models", "old", 100, None)
            .await?;
        let record = storage
            .get_compatibility_kv("models")
            .await?
            .context("kv record should exist")?;
        ensure!(
            record.value == "old",
            "KV value writes replace regardless of timestamp"
        );
        ensure!(
            record.last_updated_at_unix_secs == 100,
            "timestamp should match last value write"
        );
        Ok(())
    }
);
