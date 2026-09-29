use std::sync::Arc;

use cc_lb_storage_api::{
    MetaStore, StorageError, UpstreamAffinityBinding, UpstreamAffinityKey, UpstreamAffinityKind,
    UpstreamAffinityStore, UpstreamCreate, UpstreamStore, upstream::UpstreamKind,
};
use sha2::{Digest, Sha256};

const BIND_BATCH_SIZE: usize = 128;
const TTL_SECS: u64 = 7_776_000;

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
    storage.initialize().await.expect("initialize sqlite");

    let first_upstream = create_upstream(&storage, "affinity-first").await;
    let second_upstream = create_upstream(&storage, "affinity-second").await;
    let raw_opaque_value = "opaque-encrypted-content-that-must-never-be-stored";
    let first_key = affinity_key("principal-a", digest(raw_opaque_value));
    let second_key = affinity_key("principal-a", [2; 32]);

    storage
        .bind_upstream_affinities(
            &[
                binding(first_key.clone(), first_upstream, 10, Some(500)),
                binding(second_key.clone(), first_upstream, 11, None),
            ],
            0,
            TTL_SECS,
        )
        .await
        .expect("bind initial affinities");

    let updated = binding(first_key.clone(), first_upstream, 20, Some(600));
    storage
        .bind_upstream_affinities(std::slice::from_ref(&updated), 0, TTL_SECS)
        .await
        .expect("idempotently refresh affinity");
    assert_eq!(
        storage
            .resolve_upstream_affinities(std::slice::from_ref(&first_key), 500, TTL_SECS)
            .await
            .expect("resolve refreshed affinity"),
        vec![updated.clone()]
    );
    storage
        .bind_upstream_affinities(
            &[binding(first_key.clone(), first_upstream, 15, Some(550))],
            0,
            TTL_SECS,
        )
        .await
        .expect("ignore stale same-target refresh");
    assert_eq!(
        storage
            .resolve_upstream_affinities(std::slice::from_ref(&first_key), 500, TTL_SECS)
            .await
            .expect("resolve after stale refresh"),
        vec![updated.clone()]
    );

    let duplicate_key = affinity_key("principal-duplicate", [5; 32]);
    let merged_duplicate = binding(duplicate_key.clone(), first_upstream, 60, None);
    storage
        .bind_upstream_affinities(
            &[
                binding(duplicate_key.clone(), first_upstream, 50, Some(700)),
                binding(duplicate_key.clone(), first_upstream, 60, Some(650)),
                binding(duplicate_key.clone(), first_upstream, 55, None),
            ],
            0,
            TTL_SECS,
        )
        .await
        .expect("merge duplicate same-target affinities");
    assert_eq!(
        storage
            .resolve_upstream_affinities(std::slice::from_ref(&duplicate_key), 0, TTL_SECS)
            .await
            .expect("resolve merged duplicate affinity"),
        vec![merged_duplicate]
    );

    let staged_key = affinity_key("principal-staged", [6; 32]);
    let duplicate_conflict_key = affinity_key("principal-duplicate-conflict", [7; 32]);
    let error = storage
        .bind_upstream_affinities(
            &[
                binding(staged_key.clone(), first_upstream, 70, None),
                binding(duplicate_conflict_key.clone(), first_upstream, 70, None),
                binding(duplicate_conflict_key.clone(), second_upstream, 71, None),
            ],
            0,
            TTL_SECS,
        )
        .await
        .expect_err("duplicate key with different targets must conflict");
    assert!(matches!(error, StorageError::Conflict { .. }));
    assert!(
        storage
            .resolve_upstream_affinities(&[staged_key, duplicate_conflict_key], 0, TTL_SECS)
            .await
            .expect("resolve after duplicate input conflict")
            .is_empty()
    );

    let error = storage
        .bind_upstream_affinities(
            &[
                binding(first_key.clone(), first_upstream, 30, Some(700)),
                binding(second_key.clone(), second_upstream, 31, None),
            ],
            0,
            TTL_SECS,
        )
        .await
        .expect_err("different upstream target must conflict");
    assert!(matches!(error, StorageError::Conflict { .. }));

    let after_conflict = storage
        .resolve_upstream_affinities(&[first_key.clone(), second_key.clone()], 0, TTL_SECS)
        .await
        .expect("resolve after rolled-back conflict");
    assert_eq!(after_conflict.len(), 2);
    assert!(after_conflict.contains(&updated));
    assert!(after_conflict.contains(&binding(second_key.clone(), first_upstream, 11, None,)));

    let invalid_key = affinity_key("principal-invalid", [8; 32]);
    let error = storage
        .bind_upstream_affinities(
            &[
                binding(first_key.clone(), first_upstream, 999, None),
                binding(invalid_key.clone(), first_upstream, u64::MAX, None),
            ],
            0,
            TTL_SECS,
        )
        .await
        .expect_err("out-of-range numeric input must fail");
    assert!(matches!(error, StorageError::InvalidInput { .. }));
    assert_eq!(
        storage
            .resolve_upstream_affinities(&[first_key.clone(), invalid_key], 0, TTL_SECS)
            .await
            .expect("resolve after invalid numeric rollback"),
        vec![updated.clone()]
    );

    let chunk_keys = (0..=BIND_BATCH_SIZE)
        .map(|index| {
            affinity_key(
                &format!("principal-batch-{index}"),
                digest(&format!("batch-key-{index}")),
            )
        })
        .collect::<Vec<_>>();
    let initial_chunk_bindings = chunk_keys
        .iter()
        .cloned()
        .map(|key| binding(key, first_upstream, 80, Some(800)))
        .collect::<Vec<_>>();
    storage
        .bind_upstream_affinities(&initial_chunk_bindings, 0, TTL_SECS)
        .await
        .expect("bind affinities across multiple chunks");
    let boundary_keys = [
        chunk_keys.first().expect("first chunk key").clone(),
        chunk_keys.last().expect("last chunk key").clone(),
    ];
    let boundary_bindings = storage
        .resolve_upstream_affinities(&boundary_keys, 0, TTL_SECS)
        .await
        .expect("resolve chunk boundary affinities");
    assert_eq!(boundary_bindings.len(), 2);
    assert!(boundary_bindings.contains(&initial_chunk_bindings[0]));
    assert!(
        boundary_bindings.contains(
            initial_chunk_bindings
                .last()
                .expect("last initial chunk binding")
        )
    );

    let late_conflict_key = affinity_key("principal-late-conflict", [9; 32]);
    storage
        .bind_upstream_affinities(
            &[binding(
                late_conflict_key.clone(),
                first_upstream,
                80,
                Some(800),
            )],
            0,
            TTL_SECS,
        )
        .await
        .expect("seed late chunk conflict");
    let mut conflicting_chunks = chunk_keys
        .iter()
        .cloned()
        .map(|key| binding(key, first_upstream, 90, Some(900)))
        .collect::<Vec<_>>();
    conflicting_chunks.push(binding(late_conflict_key, second_upstream, 90, Some(900)));
    let error = storage
        .bind_upstream_affinities(&conflicting_chunks, 0, TTL_SECS)
        .await
        .expect_err("later chunk conflict must roll back earlier chunks");
    assert!(matches!(error, StorageError::Conflict { .. }));
    let after_chunk_conflict = storage
        .resolve_upstream_affinities(&boundary_keys, 0, TTL_SECS)
        .await
        .expect("resolve after multi-chunk rollback");
    assert_eq!(after_chunk_conflict.len(), 2);
    assert!(after_chunk_conflict.contains(&initial_chunk_bindings[0]));
    assert!(
        after_chunk_conflict.contains(
            initial_chunk_bindings
                .last()
                .expect("last initial chunk binding")
        )
    );

    let expiring_key = affinity_key("principal-a", [3; 32]);
    let permanent_key = affinity_key("principal-a", [4; 32]);
    let permanent = binding(permanent_key.clone(), first_upstream, 40, None);
    storage
        .bind_upstream_affinities(
            &[
                binding(expiring_key.clone(), first_upstream, 40, Some(100)),
                permanent.clone(),
            ],
            0,
            TTL_SECS,
        )
        .await
        .expect("bind expiring affinities");
    assert_eq!(
        storage
            .resolve_upstream_affinities(&[expiring_key, permanent_key], 100, TTL_SECS)
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

#[tokio::test]
async fn upstream_affinity_retention_boundaries_and_expired_rebind_follow_current_policy() {
    let (_temp_dir, storage) = open_storage("retention-boundaries").await;
    let first_upstream = create_upstream(&storage, "retention-first").await;
    let second_upstream = create_upstream(&storage, "retention-second").await;
    let legacy_key = affinity_key("principal-retention", [20; 32]);
    let legacy = binding(legacy_key.clone(), first_upstream, 100, None);

    storage
        .bind_upstream_affinities(std::slice::from_ref(&legacy), 50, 100)
        .await
        .expect("bind legacy affinity");
    assert_eq!(
        storage
            .resolve_upstream_affinities(std::slice::from_ref(&legacy_key), 50, 100)
            .await
            .expect("resolve when now is below ttl"),
        vec![legacy.clone()]
    );
    assert_eq!(
        storage
            .resolve_upstream_affinities(std::slice::from_ref(&legacy_key), 200, 101)
            .await
            .expect("resolve with longer ttl"),
        vec![legacy.clone()]
    );
    assert!(
        storage
            .resolve_upstream_affinities(std::slice::from_ref(&legacy_key), 200, 100)
            .await
            .expect("resolve at retention equality")
            .is_empty()
    );

    let error = storage
        .bind_upstream_affinities(
            &[binding(legacy_key.clone(), second_upstream, 199, None)],
            199,
            100,
        )
        .await
        .expect_err("active different-target affinity must conflict");
    assert!(matches!(error, StorageError::Conflict { .. }));

    let rebound = binding(legacy_key.clone(), second_upstream, 201, Some(400));
    storage
        .bind_upstream_affinities(std::slice::from_ref(&rebound), 200, 100)
        .await
        .expect("retention-expired affinity can be rebound");
    assert_eq!(
        storage
            .resolve_upstream_affinities(std::slice::from_ref(&legacy_key), 200, 100)
            .await
            .expect("resolve retention rebound"),
        vec![rebound]
    );

    let expiry_key = affinity_key("principal-expiry", [21; 32]);
    storage
        .bind_upstream_affinities(
            &[binding(expiry_key.clone(), first_upstream, 190, Some(200))],
            190,
            100,
        )
        .await
        .expect("bind explicitly expiring affinity");
    assert!(
        storage
            .resolve_upstream_affinities(std::slice::from_ref(&expiry_key), 200, 100)
            .await
            .expect("resolve at explicit expiry equality")
            .is_empty()
    );
    let expiry_rebound = binding(expiry_key.clone(), second_upstream, 201, None);
    storage
        .bind_upstream_affinities(std::slice::from_ref(&expiry_rebound), 200, 100)
        .await
        .expect("explicitly expired affinity can be rebound");
    assert_eq!(
        storage
            .resolve_upstream_affinities(std::slice::from_ref(&expiry_key), 200, 100)
            .await
            .expect("resolve explicit-expiry rebound"),
        vec![expiry_rebound]
    );
}

#[tokio::test]
async fn upstream_affinity_purge_is_bounded_and_preserves_concurrent_rebind() {
    let (_temp_dir, storage) = open_storage("bounded-purge").await;
    let first_upstream = create_upstream(&storage, "purge-first").await;
    let second_upstream = create_upstream(&storage, "purge-second").await;
    let explicit_keys = (0..3)
        .map(|index| affinity_key(&format!("principal-explicit-{index}"), [30 + index; 32]))
        .collect::<Vec<_>>();
    let retention_keys = (0..3)
        .map(|index| affinity_key(&format!("principal-retention-{index}"), [40 + index; 32]))
        .collect::<Vec<_>>();
    let active_key = affinity_key("principal-active", [50; 32]);
    let mut bindings = explicit_keys
        .iter()
        .cloned()
        .map(|key| binding(key, first_upstream, 950, Some(1_000)))
        .collect::<Vec<_>>();
    bindings.extend(
        retention_keys
            .iter()
            .cloned()
            .map(|key| binding(key, first_upstream, 900, None)),
    );
    let active = binding(active_key.clone(), first_upstream, 901, None);
    bindings.push(active.clone());
    storage
        .bind_upstream_affinities(&bindings, 900, 100)
        .await
        .expect("seed purge rows");

    assert_eq!(
        storage
            .purge_expired_upstream_affinities(1_000, 100, 4)
            .await
            .expect("purge first bounded batch"),
        4
    );
    assert_eq!(
        storage
            .purge_expired_upstream_affinities(1_000, 100, 4)
            .await
            .expect("purge remaining expired rows"),
        2
    );
    assert_eq!(
        storage
            .purge_expired_upstream_affinities(1_000, 100, 4)
            .await
            .expect("purge empty expired set"),
        0
    );
    assert_eq!(
        storage
            .resolve_upstream_affinities(std::slice::from_ref(&active_key), 1_000, 100)
            .await
            .expect("resolve active row after purge"),
        vec![active]
    );

    let race_key = affinity_key("principal-race", [60; 32]);
    storage
        .bind_upstream_affinities(
            &[binding(race_key.clone(), first_upstream, 100, None)],
            100,
            100,
        )
        .await
        .expect("seed race row");
    let rebound = binding(race_key.clone(), second_upstream, 1_001, None);
    let purge_storage = storage.clone();
    let bind_storage = storage.clone();
    let (purged, rebound_result) = tokio::join!(
        purge_storage.purge_expired_upstream_affinities(1_000, 100, 1),
        bind_storage.bind_upstream_affinities(std::slice::from_ref(&rebound), 1_000, 100),
    );
    assert!(purged.expect("race purge") <= 1);
    rebound_result.expect("race rebind");
    assert_eq!(
        storage
            .resolve_upstream_affinities(std::slice::from_ref(&race_key), 1_000, 100)
            .await
            .expect("resolve concurrent rebound"),
        vec![rebound]
    );
}

async fn open_storage(file_name: &str) -> (tempfile::TempDir, cc_lb_storage_sqlite::SqliteStorage) {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let database_url = format!(
        "sqlite://{}",
        temp_dir
            .path()
            .join(format!("{file_name}.sqlite"))
            .display()
    );
    let storage =
        cc_lb_storage_sqlite::open_sqlite(&database_url, Arc::new(cc_lb_clock::SystemClock))
            .await
            .expect("open sqlite");
    storage.initialize().await.expect("initialize sqlite");
    (temp_dir, storage)
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
