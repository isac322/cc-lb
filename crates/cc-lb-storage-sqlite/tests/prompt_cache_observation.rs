use std::sync::Arc;

use cc_lb_domain::TtlClass;
use cc_lb_storage_api::{
    BackendKind, MetaStore, PromptCacheObservationRecord, PromptCacheObservationStore,
};
use uuid::Uuid;

#[tokio::test]
async fn prompt_cache_v3_observation_roundtrips_and_filters_by_key() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let database_url = format!(
        "sqlite://{}",
        temp_dir
            .path()
            .join("prompt-cache-observation.sqlite")
            .display()
    );
    let storage =
        cc_lb_storage_sqlite::open_sqlite(&database_url, Arc::new(cc_lb_clock::SystemClock))
            .await
            .expect("open sqlite");
    // Opaque to the storage layer: the column has no default or constraint and this test only
    // round-trips it, so the value is arbitrary and deliberately not tied to the engine's
    // HASH_SCHEMA_VERSION (which would drag cc-lb-engine into this crate's dev graph).
    const FIXTURE_SCHEMA_VERSION: u8 = 5;
    storage
        .initialize(BackendKind::Sqlite)
        .await
        .expect("initialize sqlite");
    let upstream_id = Uuid::from_u128(0x1234_5678_90ab_cdef_1234_5678_90ab_cdef);
    let record = PromptCacheObservationRecord {
        upstream_id,
        canonical_model_id: "claude-sonnet-4-5-20250929".to_owned(),
        v3_prefix_key: "v3:prefix-a".to_owned(),
        ttl_class: TtlClass::Ephemeral1h,
        expires_at_unix_secs: 1_800,
        last_observed_at_unix_secs: 1_500,
        hash_schema_version: FIXTURE_SCHEMA_VERSION,
        prefix_content_block_index: 17,
        estimated_prefix_tokens: 12_345,
        token_estimate_source: "local_tiktoken_v1".to_owned(),
        last_provider_cache_read_tokens: Some(12_000),
        last_provider_cache_creation_tokens: Some(345),
    };

    storage
        .upsert_observation(&record)
        .await
        .expect("upsert v3 prompt cache observation");
    let active = storage
        .list_active_for_upstream_keys(
            upstream_id,
            1_700,
            &["v3:prefix-a".to_owned(), "v3:missing".to_owned()],
        )
        .await
        .expect("list active by v3 keys");
    assert_eq!(active, vec![record.clone()]);
    assert_eq!(storage.count().await.expect("count"), 1);

    let purged = storage
        .purge_expired_before(1_900)
        .await
        .expect("purge expired v3 observation");
    assert_eq!(purged, 1);
    assert_eq!(storage.count().await.expect("count after purge"), 0);
}
