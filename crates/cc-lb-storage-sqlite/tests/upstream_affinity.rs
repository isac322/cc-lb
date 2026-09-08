use std::sync::Arc;

use cc_lb_storage_api::{
    BackendKind, MetaStore, StorageError, UpstreamAffinityBinding, UpstreamAffinityKey,
    UpstreamAffinityKind, UpstreamAffinityStore, UpstreamCreate, UpstreamStore,
    upstream::UpstreamKind,
};
use sha2::{Digest, Sha256};

#[tokio::test]
async fn upstream_affinity_bind_is_idempotent_conflict_atomic_and_expiry_aware() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let database_url = format!(
        "sqlite://{}",
        temp_dir.path().join("upstream-affinity.sqlite").display()
    );
    let storage =
        cc_lb_storage_sqlite::open_sqlite(&database_url, Arc::new(cc_lb_clock::SystemClock))
            .await
            .expect("open sqlite");
    storage
        .initialize(BackendKind::Sqlite)
        .await
        .expect("initialize sqlite");

    let first_upstream = create_upstream(&storage, "affinity-first").await;
    let second_upstream = create_upstream(&storage, "affinity-second").await;
    let raw_opaque_value = "opaque-encrypted-content-that-must-never-be-stored";
    let first_key = affinity_key("principal-a", digest(raw_opaque_value));
    let second_key = affinity_key("principal-a", [2; 32]);

    storage
        .bind_upstream_affinities(&[
            binding(first_key.clone(), first_upstream, 10, Some(500)),
            binding(second_key.clone(), first_upstream, 11, None),
        ])
        .await
        .expect("bind initial affinities");

    let updated = binding(first_key.clone(), first_upstream, 20, Some(600));
    storage
        .bind_upstream_affinities(std::slice::from_ref(&updated))
        .await
        .expect("idempotently refresh affinity");
    assert_eq!(
        storage
            .resolve_upstream_affinities(std::slice::from_ref(&first_key), 500)
            .await
            .expect("resolve refreshed affinity"),
        vec![updated.clone()]
    );
    storage
        .bind_upstream_affinities(&[binding(first_key.clone(), first_upstream, 15, Some(550))])
        .await
        .expect("ignore stale same-target refresh");
    assert_eq!(
        storage
            .resolve_upstream_affinities(std::slice::from_ref(&first_key), 500)
            .await
            .expect("resolve after stale refresh"),
        vec![updated.clone()]
    );

    let error = storage
        .bind_upstream_affinities(&[
            binding(first_key.clone(), first_upstream, 30, Some(700)),
            binding(second_key.clone(), second_upstream, 31, None),
        ])
        .await
        .expect_err("different upstream target must conflict");
    assert!(matches!(error, StorageError::Conflict { .. }));

    let after_conflict = storage
        .resolve_upstream_affinities(&[first_key.clone(), second_key.clone()], 0)
        .await
        .expect("resolve after rolled-back conflict");
    assert_eq!(after_conflict.len(), 2);
    assert!(after_conflict.contains(&updated));
    assert!(after_conflict.contains(&binding(second_key.clone(), first_upstream, 11, None,)));

    let expiring_key = affinity_key("principal-a", [3; 32]);
    let permanent_key = affinity_key("principal-a", [4; 32]);
    let permanent = binding(permanent_key.clone(), first_upstream, 40, None);
    storage
        .bind_upstream_affinities(&[
            binding(expiring_key.clone(), first_upstream, 40, Some(100)),
            permanent.clone(),
        ])
        .await
        .expect("bind expiring affinities");
    assert_eq!(
        storage
            .resolve_upstream_affinities(&[expiring_key, permanent_key], 100)
            .await
            .expect("filter expired affinity"),
        vec![permanent]
    );

    let (storage_type, stored_len, stored_digest): (String, i64, Vec<u8>) = sqlx::query_as(
        "SELECT typeof(value_sha256), length(value_sha256), value_sha256 \
         FROM upstream_affinity_v1 \
         WHERE principal_id = ? AND provider = ? AND kind = ? AND value_sha256 = ?",
    )
    .bind(&first_key.principal_id)
    .bind(&first_key.provider)
    .bind(first_key.kind.as_str())
    .bind(first_key.value_sha256.as_slice())
    .fetch_one(storage.pool())
    .await
    .expect("inspect stored digest");
    assert_eq!(storage_type, "blob");
    assert_eq!(stored_len, 32);
    assert_eq!(stored_digest.as_slice(), first_key.value_sha256.as_slice());
    let raw_match_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM upstream_affinity_v1 WHERE value_sha256 = ?")
            .bind(raw_opaque_value.as_bytes())
            .fetch_one(storage.pool())
            .await
            .expect("check raw opaque value absence");
    assert_eq!(raw_match_count, 0);
}

async fn create_upstream(storage: &cc_lb_storage_sqlite::SqliteStorage, name: &str) -> uuid::Uuid {
    UpstreamStore::create(
        storage,
        UpstreamCreate {
            name: name.to_owned(),
            kind: UpstreamKind::AnthropicOauth,
            ..UpstreamCreate::default()
        },
    )
    .await
    .expect("create upstream")
    .id
}

fn affinity_key(principal_id: &str, value_sha256: [u8; 32]) -> UpstreamAffinityKey {
    UpstreamAffinityKey {
        principal_id: principal_id.to_owned(),
        provider: "anthropic".to_owned(),
        kind: UpstreamAffinityKind::AnthropicWebSearchEncryptedContent,
        value_sha256,
    }
}

fn binding(
    key: UpstreamAffinityKey,
    upstream_id: uuid::Uuid,
    observed_at_unix_secs: u64,
    expires_at_unix_secs: Option<u64>,
) -> UpstreamAffinityBinding {
    UpstreamAffinityBinding {
        key,
        upstream_id,
        observed_at_unix_secs,
        expires_at_unix_secs,
    }
}

fn digest(value: &str) -> [u8; 32] {
    Sha256::digest(value.as_bytes()).into()
}
