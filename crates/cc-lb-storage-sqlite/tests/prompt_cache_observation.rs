use std::sync::Arc;

use cc_lb_domain::TtlClass;
use cc_lb_storage_api::{
    MetaStore, PromptCacheObservationRecord, PromptCacheObservationStore,
    ProviderPromptCacheObservationRecord,
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
    storage.initialize().await.expect("initialize sqlite");
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
    };

    storage
        .upsert_observation(&record)
        .await
        .expect("upsert v3 prompt cache observation");
    let active = storage
        .list_active_for_candidates(
            &[upstream_id],
            "claude-sonnet-4-5-20250929",
            &["v3:prefix-a".to_owned(), "v3:missing".to_owned()],
            1_700,
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

#[tokio::test]
async fn prompt_cache_v3_stale_upsert_does_not_regress_fresh_row() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let database_url = format!(
        "sqlite://{}",
        temp_dir
            .path()
            .join("prompt-cache-observation-stale.sqlite")
            .display()
    );
    let storage =
        cc_lb_storage_sqlite::open_sqlite(&database_url, Arc::new(cc_lb_clock::SystemClock))
            .await
            .expect("open sqlite");
    const FIXTURE_SCHEMA_VERSION: u8 = 5;
    storage.initialize().await.expect("initialize sqlite");
    let upstream_id = Uuid::from_u128(0x1234_5678_90ab_cdef_1234_5678_90ab_cdef);
    let fresh = PromptCacheObservationRecord {
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
    };
    let mut stale = fresh.clone();
    stale.expires_at_unix_secs = 1_600;
    stale.last_observed_at_unix_secs = 1_400;
    stale.estimated_prefix_tokens = 1;
    stale.token_estimate_source = "stale_source".to_owned();

    storage
        .upsert_observation(&fresh)
        .await
        .expect("upsert fresh observation");
    storage
        .upsert_observation(&stale)
        .await
        .expect("stale upsert is accepted but must not regress");

    let active = storage
        .list_active_for_candidates(
            &[upstream_id],
            "claude-sonnet-4-5-20250929",
            &["v3:prefix-a".to_owned()],
            1_700,
        )
        .await
        .expect("list after stale upsert");
    assert_eq!(active, vec![fresh]);
}

#[tokio::test]
async fn provider_cache_unknown_or_expired_rows_are_not_active_and_only_expired_are_purged() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let database_url = format!(
        "sqlite://{}",
        temp_dir
            .path()
            .join("provider-prompt-cache-observation.sqlite")
            .display()
    );
    let storage =
        cc_lb_storage_sqlite::open_sqlite(&database_url, Arc::new(cc_lb_clock::SystemClock))
            .await
            .expect("open sqlite");
    storage.initialize().await.expect("initialize sqlite");
    let upstream_id = Uuid::from_u128(1);
    let base = ProviderPromptCacheObservationRecord {
        upstream_id,
        provider: "gemini".to_owned(),
        canonical_model_id: "gemini-2.5-pro".to_owned(),
        mode: "explicit".to_owned(),
        isolation_scope: "scope".to_owned(),
        identity_hash: "sha256:identity".to_owned(),
        boundary: "resource".to_owned(),
        evidence_source: "provider_usage_metadata".to_owned(),
        observed_cached_tokens: Some(256),
        observed_write_tokens: None,
        retention_secs: None,
        expires_at_unix_secs: None,
        last_observed_at_unix_secs: 100,
    };
    let mut expired = base.clone();
    expired.boundary = "expired".to_owned();
    expired.expires_at_unix_secs = Some(100);
    let mut active = base.clone();
    active.boundary = "active".to_owned();
    active.expires_at_unix_secs = Some(300);
    for record in [&base, &expired, &active] {
        storage
            .upsert_provider_observation(record)
            .await
            .expect("upsert provider observation");
    }

    let before_purge = storage
        .list_active_provider_observations(
            upstream_id,
            "gemini",
            "gemini-2.5-pro",
            "explicit",
            "scope",
            "sha256:identity",
            200,
        )
        .await
        .expect("list active provider observations");
    assert_eq!(before_purge.len(), 1);
    assert_eq!(before_purge[0].boundary, "active");

    assert_eq!(
        storage
            .purge_expired_before(200)
            .await
            .expect("purge provider observations"),
        1
    );
    let after_purge = storage
        .list_active_provider_observations(
            upstream_id,
            "gemini",
            "gemini-2.5-pro",
            "explicit",
            "scope",
            "sha256:identity",
            200,
        )
        .await
        .expect("list active provider observations after purge");
    assert_eq!(after_purge, vec![active]);
}
