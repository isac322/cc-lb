use std::{error::Error, str::FromStr, sync::Arc};

use cc_lb_storage_api::{
    ApiKeyAccessWrite, ApiKeyCapabilities, ApiKeyClass, ApiKeyOrgCapability, MetaStore,
    StorageError, UpstreamAffinityBinding, UpstreamAffinityKey, UpstreamAffinityKind,
    UpstreamAffinityStore, UpstreamApiKeyStore, UpstreamCreate, UpstreamStore, UpstreamUpdate,
    upstream::UpstreamKind,
};
use cc_lb_storage_postgres::PostgresStorage;
use sha2::{Digest, Sha256};
use sqlx::{
    AssertSqlSafe, PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use url::Url;
use uuid::Uuid;

const BIND_BATCH_SIZE: usize = 128;
const TTL_SECS: u64 = 7_776_000;

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[tokio::test]
#[ignore = "requires CI_POSTGRES_URL; run with --ignored"]
async fn upstream_affinity_bind_is_idempotent_conflict_atomic_and_expiry_aware() -> TestResult<()> {
    let Some(fixture) = Fixture::create().await? else {
        return Ok(());
    };
    let storage = fixture.store();
    storage.initialize().await?;

    let first_upstream = create_upstream(&storage, "affinity-first").await?;
    let second_upstream = create_upstream(&storage, "affinity-second").await?;
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
        .await?;

    let updated = binding(first_key.clone(), first_upstream, 20, Some(600));
    storage
        .bind_upstream_affinities(std::slice::from_ref(&updated), 0, TTL_SECS)
        .await?;
    assert_eq!(
        storage
            .resolve_upstream_affinities(std::slice::from_ref(&first_key), 500, TTL_SECS)
            .await?,
        vec![updated.clone()]
    );
    storage
        .bind_upstream_affinities(
            &[binding(first_key.clone(), first_upstream, 15, Some(550))],
            0,
            TTL_SECS,
        )
        .await?;
    assert_eq!(
        storage
            .resolve_upstream_affinities(std::slice::from_ref(&first_key), 500, TTL_SECS)
            .await?,
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
        .await?;
    assert_eq!(
        storage
            .resolve_upstream_affinities(std::slice::from_ref(&duplicate_key), 0, TTL_SECS)
            .await?,
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
            .await?
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
        .await?;
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
    assert!(
        matches!(&error, StorageError::Fatal { .. }),
        "unexpected numeric conversion error: {error:?}"
    );
    assert_eq!(
        storage
            .resolve_upstream_affinities(&[first_key.clone(), invalid_key], 0, TTL_SECS)
            .await?,
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
        .await?;
    let boundary_keys = [
        chunk_keys.first().expect("first chunk key").clone(),
        chunk_keys.last().expect("last chunk key").clone(),
    ];
    let boundary_bindings = storage
        .resolve_upstream_affinities(&boundary_keys, 0, TTL_SECS)
        .await?;
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
        .await?;
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
        .await?;
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
        .await?;
    assert_eq!(
        storage
            .resolve_upstream_affinities(&[expiring_key, permanent_key], 100, TTL_SECS)
            .await?,
        vec![permanent]
    );

    let (stored_len, stored_digest): (i32, Vec<u8>) = sqlx::query_as(
        "SELECT octet_length(value_sha256), value_sha256 \
         FROM upstream_affinity_v1 \
         WHERE principal_id = $1 AND provider = $2 AND kind = $3 AND value_sha256 = $4",
    )
    .bind(&first_key.principal_id)
    .bind(&first_key.provider)
    .bind(first_key.kind.as_str())
    .bind(first_key.value_sha256.as_slice())
    .fetch_one(fixture.pool())
    .await?;
    assert_eq!(stored_len, 32);
    assert_eq!(stored_digest.as_slice(), first_key.value_sha256.as_slice());
    let raw_match_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM upstream_affinity_v1 WHERE value_sha256 = $1")
            .bind(raw_opaque_value.as_bytes())
            .fetch_one(fixture.pool())
            .await?;
    assert_eq!(raw_match_count, 0);

    fixture.drop_schema().await
}

#[tokio::test]
#[ignore = "requires CI_POSTGRES_URL; run with --ignored"]
async fn upstream_affinity_retention_boundaries_and_expired_rebind_follow_current_policy()
-> TestResult<()> {
    let Some(fixture) = Fixture::create().await? else {
        return Ok(());
    };
    let storage = fixture.store();
    storage.initialize().await?;
    let first_upstream = create_upstream(&storage, "retention-first").await?;
    let second_upstream = create_upstream(&storage, "retention-second").await?;
    let legacy_key = affinity_key("principal-retention", [20; 32]);
    let legacy = binding(legacy_key.clone(), first_upstream, 100, None);

    storage
        .bind_upstream_affinities(std::slice::from_ref(&legacy), 50, 100)
        .await?;
    assert_eq!(
        storage
            .resolve_upstream_affinities(std::slice::from_ref(&legacy_key), 50, 100)
            .await?,
        vec![legacy.clone()]
    );
    assert_eq!(
        storage
            .resolve_upstream_affinities(std::slice::from_ref(&legacy_key), 200, 101)
            .await?,
        vec![legacy.clone()]
    );
    assert!(
        storage
            .resolve_upstream_affinities(std::slice::from_ref(&legacy_key), 200, 100)
            .await?
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
        .await?;
    assert_eq!(
        storage
            .resolve_upstream_affinities(std::slice::from_ref(&legacy_key), 200, 100)
            .await?,
        vec![rebound]
    );

    let expiry_key = affinity_key("principal-expiry", [21; 32]);
    storage
        .bind_upstream_affinities(
            &[binding(expiry_key.clone(), first_upstream, 190, Some(200))],
            190,
            100,
        )
        .await?;
    assert!(
        storage
            .resolve_upstream_affinities(std::slice::from_ref(&expiry_key), 200, 100)
            .await?
            .is_empty()
    );
    let expiry_rebound = binding(expiry_key.clone(), second_upstream, 201, None);
    storage
        .bind_upstream_affinities(std::slice::from_ref(&expiry_rebound), 200, 100)
        .await?;
    assert_eq!(
        storage
            .resolve_upstream_affinities(std::slice::from_ref(&expiry_key), 200, 100)
            .await?,
        vec![expiry_rebound]
    );

    fixture.drop_schema().await
}

#[tokio::test]
#[ignore = "requires CI_POSTGRES_URL; run with --ignored"]
async fn upstream_affinity_purge_is_bounded_and_preserves_concurrent_rebind() -> TestResult<()> {
    let Some(fixture) = Fixture::create().await? else {
        return Ok(());
    };
    let storage = fixture.store();
    storage.initialize().await?;
    let first_upstream = create_upstream(&storage, "purge-first").await?;
    let second_upstream = create_upstream(&storage, "purge-second").await?;
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
        .await?;

    assert_eq!(
        storage
            .purge_expired_upstream_affinities(1_000, 100, 4)
            .await?,
        4
    );
    assert_eq!(
        storage
            .purge_expired_upstream_affinities(1_000, 100, 4)
            .await?,
        2
    );
    assert_eq!(
        storage
            .purge_expired_upstream_affinities(1_000, 100, 4)
            .await?,
        0
    );
    assert_eq!(
        storage
            .resolve_upstream_affinities(std::slice::from_ref(&active_key), 1_000, 100)
            .await?,
        vec![active]
    );

    let race_key = affinity_key("principal-race", [60; 32]);
    storage
        .bind_upstream_affinities(
            &[binding(race_key.clone(), first_upstream, 100, None)],
            100,
            100,
        )
        .await?;
    let rebound = binding(race_key.clone(), second_upstream, 1_001, None);
    let mut renewal_tx = fixture.pool().begin().await?;
    sqlx::query(
        "UPDATE upstream_affinity_v1 \
         SET upstream_id = $1, observed_at_unix_secs = $2, expires_at_unix_secs = NULL \
         WHERE principal_id = $3 AND provider = $4 AND kind = $5 AND value_sha256 = $6",
    )
    .bind(second_upstream)
    .bind(1_001_i64)
    .bind(&race_key.principal_id)
    .bind(&race_key.provider)
    .bind(race_key.kind.as_str())
    .bind(race_key.value_sha256.as_slice())
    .execute(&mut *renewal_tx)
    .await?;

    let purged = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        storage.purge_expired_upstream_affinities(1_000, 100, 1),
    )
    .await
    .expect("purge must skip the locked renewal instead of blocking")?;
    assert_eq!(purged, 0);

    renewal_tx.commit().await?;
    assert_eq!(
        storage
            .resolve_upstream_affinities(std::slice::from_ref(&race_key), 1_000, 100)
            .await?,
        vec![rebound]
    );

    fixture.drop_schema().await
}

#[tokio::test]
#[ignore = "requires CI_POSTGRES_URL; run with --ignored"]
async fn upstream_affinity_owner_epoch_fences_gemini_identity_only() -> TestResult<()> {
    let Some(fixture) = Fixture::create().await? else {
        return Ok(());
    };
    let storage = fixture.store();
    storage.initialize().await?;

    let gemini = create_upstream_kind(
        &storage,
        "epoch-gemini",
        UpstreamKind::GeminiApiKey,
        Some(b"gemini-old".to_vec()),
    )
    .await?;
    assert_eq!(
        storage.read_upstream_owner_epoch(gemini, 1).await?,
        Some(0),
        "new upstreams start at epoch zero"
    );
    let gemini_key = affinity_key_with_kind(
        "principal-gemini",
        [70; 32],
        "gemini",
        UpstreamAffinityKind::GeminiCachedContent,
    );
    let initial = binding(gemini_key.clone(), gemini, 10, None);
    storage
        .bind_upstream_affinities(std::slice::from_ref(&initial), 0, TTL_SECS)
        .await?;

    let replaced =
        UpstreamStore::update_api_key_secret(&storage, gemini, Some(b"gemini-new".to_vec()))
            .await?;
    assert_eq!(replaced.revision, 2);
    assert_eq!(
        storage.read_upstream_owner_epoch(gemini, 1).await?,
        None,
        "a stale spec revision cannot read the current owner epoch"
    );
    assert_eq!(
        storage
            .read_upstream_owner_epoch(gemini, replaced.revision)
            .await?,
        Some(1),
        "Gemini API-key identity changes advance the owner epoch"
    );
    assert!(
        storage
            .resolve_upstream_affinities(std::slice::from_ref(&gemini_key), 10, TTL_SECS)
            .await?
            .is_empty(),
        "bindings from the previous Gemini owner epoch are stale"
    );

    let stale_keys = [
        binding(
            affinity_key_with_kind(
                "principal-gemini-stale-a",
                [71; 32],
                "gemini",
                UpstreamAffinityKind::GeminiCachedContent,
            ),
            gemini,
            0,
            None,
        ),
        binding(
            affinity_key_with_kind(
                "principal-gemini-stale-b",
                [72; 32],
                "gemini",
                UpstreamAffinityKind::GeminiCachedContent,
            ),
            gemini,
            0,
            None,
        ),
    ];
    let error = storage
        .bind_upstream_affinities(&stale_keys, 10, TTL_SECS)
        .await
        .expect_err("stale owner epoch must reject a new binding");
    assert!(matches!(error, StorageError::Conflict { .. }));
    assert!(
        storage
            .resolve_upstream_affinities(
                &stale_keys
                    .iter()
                    .map(|binding| binding.key.clone())
                    .collect::<Vec<_>>(),
                10,
                TTL_SECS,
            )
            .await?
            .is_empty(),
        "a rejected stale batch must not leave partial rows"
    );

    let fresh_key = affinity_key_with_kind(
        "principal-gemini-fresh",
        [73; 32],
        "gemini",
        UpstreamAffinityKind::GeminiCachedContent,
    );
    let mut fresh = binding(fresh_key.clone(), gemini, 11, None);
    fresh.owner_epoch = 1;
    storage
        .bind_upstream_affinities(std::slice::from_ref(&fresh), 10, TTL_SECS)
        .await?;
    assert_eq!(
        storage
            .resolve_upstream_affinities(std::slice::from_ref(&fresh_key), 10, TTL_SECS)
            .await?,
        vec![fresh.clone()]
    );

    let renamed = UpstreamStore::update(
        &storage,
        gemini,
        replaced.revision,
        UpstreamUpdate {
            name: Some("epoch-gemini-renamed".to_owned()),
            ..UpstreamUpdate::default()
        },
    )
    .await?;
    assert_eq!(
        storage
            .read_upstream_owner_epoch(gemini, renamed.revision)
            .await?,
        Some(1),
        "renaming does not change owner identity"
    );
    let enabled = UpstreamStore::set_enabled(&storage, gemini, renamed.revision, false).await?;
    assert_eq!(enabled.revision, renamed.revision + 1);
    assert_eq!(
        storage
            .read_upstream_owner_epoch(gemini, enabled.revision)
            .await?,
        Some(1),
        "enabling does not change owner identity"
    );
    assert_eq!(
        storage
            .resolve_upstream_affinities(std::slice::from_ref(&fresh_key), 10, TTL_SECS)
            .await?,
        vec![fresh]
    );

    let project_a = access(Some("project-a"), 12);
    UpstreamApiKeyStore::write_api_key_access(&storage, gemini, enabled.revision, &project_a)
        .await?;
    assert_eq!(
        storage
            .read_upstream_owner_epoch(gemini, enabled.revision)
            .await?,
        Some(2),
        "a changed Gemini project identity advances the owner epoch"
    );
    assert!(
        storage
            .resolve_upstream_affinities(std::slice::from_ref(&fresh_key), 12, TTL_SECS)
            .await?
            .is_empty(),
        "changing the Gemini project invalidates the prior binding"
    );
    let project_key = affinity_key_with_kind(
        "principal-gemini-project",
        [75; 32],
        "gemini",
        UpstreamAffinityKind::GeminiCachedContent,
    );
    let mut project_binding = binding(project_key.clone(), gemini, 12, None);
    project_binding.owner_epoch = 2;
    storage
        .bind_upstream_affinities(std::slice::from_ref(&project_binding), 12, TTL_SECS)
        .await?;
    let same_project = access(Some("project-a"), 13);
    UpstreamApiKeyStore::write_api_key_access(&storage, gemini, enabled.revision, &same_project)
        .await?;
    assert_eq!(
        storage
            .read_upstream_owner_epoch(gemini, enabled.revision)
            .await?,
        Some(2),
        "refreshing unchanged project access preserves the owner epoch"
    );
    assert_eq!(
        storage
            .resolve_upstream_affinities(std::slice::from_ref(&project_key), 13, TTL_SECS)
            .await?,
        vec![project_binding]
    );

    let with_base_url = UpstreamStore::update_spec(
        &storage,
        gemini,
        enabled.revision,
        UpstreamUpdate {
            base_url: Some(Some(Url::parse("https://gemini.example.test")?)),
            ..UpstreamUpdate::default()
        },
    )
    .await?;
    assert_eq!(
        storage
            .read_upstream_owner_epoch(gemini, with_base_url.revision)
            .await?,
        Some(3),
        "changing Gemini base URL advances the owner epoch"
    );
    assert_eq!(
        storage
            .read_upstream_owner_epoch(gemini, enabled.revision)
            .await?,
        None,
        "the pre-base-URL spec revision is stale"
    );
    assert!(
        storage
            .resolve_upstream_affinities(std::slice::from_ref(&project_key), 13, TTL_SECS)
            .await?
            .is_empty()
    );
    let same_base_url = UpstreamStore::update_spec(
        &storage,
        gemini,
        with_base_url.revision,
        UpstreamUpdate {
            base_url: Some(Some(Url::parse("https://gemini.example.test")?)),
            ..UpstreamUpdate::default()
        },
    )
    .await?;
    assert_eq!(
        storage
            .read_upstream_owner_epoch(gemini, same_base_url.revision)
            .await?,
        Some(3),
        "re-saving the same Gemini base URL preserves the owner epoch"
    );
    let base_key = affinity_key_with_kind(
        "principal-gemini-base-url",
        [76; 32],
        "gemini",
        UpstreamAffinityKind::GeminiCachedContent,
    );
    let mut base_binding = binding(base_key.clone(), gemini, 14, None);
    base_binding.owner_epoch = 3;
    storage
        .bind_upstream_affinities(std::slice::from_ref(&base_binding), 14, TTL_SECS)
        .await?;
    assert_eq!(
        storage
            .resolve_upstream_affinities(std::slice::from_ref(&base_key), 14, TTL_SECS)
            .await?,
        vec![base_binding]
    );
    let project_b = access(Some("project-b"), 15);
    UpstreamApiKeyStore::write_api_key_access(&storage, gemini, same_base_url.revision, &project_b)
        .await?;
    assert_eq!(
        storage
            .read_upstream_owner_epoch(gemini, same_base_url.revision)
            .await?,
        Some(4),
        "changing Gemini project identity invalidates the current binding"
    );
    assert!(
        storage
            .resolve_upstream_affinities(std::slice::from_ref(&base_key), 15, TTL_SECS)
            .await?
            .is_empty()
    );

    let openai = create_upstream_kind(
        &storage,
        "epoch-openai",
        UpstreamKind::OpenaiApiKey,
        Some(b"openai-old".to_vec()),
    )
    .await?;
    let openai_key = affinity_key_with_kind(
        "principal-openai",
        [74; 32],
        "openai",
        UpstreamAffinityKind::OpenaiResponseId,
    );
    let openai_binding = binding(openai_key.clone(), openai, 20, None);
    storage
        .bind_upstream_affinities(std::slice::from_ref(&openai_binding), 20, TTL_SECS)
        .await?;
    let openai_updated =
        UpstreamStore::update_api_key_secret(&storage, openai, Some(b"openai-new".to_vec()))
            .await?;
    assert_eq!(
        storage
            .read_upstream_owner_epoch(openai, openai_updated.revision)
            .await?,
        Some(0),
        "non-Gemini API-key credential changes preserve epoch zero"
    );
    let access = ApiKeyAccessWrite {
        verified: true,
        key_class: ApiKeyClass::Unknown,
        key_hint: "openai-test".to_owned(),
        api_key_id: None,
        needs_workspace: false,
        capabilities: ApiKeyCapabilities::not_checked(),
        remediation: Vec::new(),
        usage_report_state: Some(ApiKeyOrgCapability::NotApplicable),
        provider: None,
        project_id: None,
        project_source: None,
        provider_report_state: None,
        provider_report_status_metadata: None,
        last_error: None,
        checked_at_unix_secs: Some(21),
        last_attempt_at_unix_secs: 21,
    };
    UpstreamApiKeyStore::write_api_key_access(&storage, openai, openai_updated.revision, &access)
        .await?;
    assert_eq!(
        storage
            .read_upstream_owner_epoch(openai, openai_updated.revision)
            .await?,
        Some(0),
        "non-Gemini access metadata changes preserve epoch zero"
    );
    assert_eq!(
        storage
            .resolve_upstream_affinities(std::slice::from_ref(&openai_key), 21, TTL_SECS)
            .await?,
        vec![openai_binding]
    );

    fixture.drop_schema().await
}

async fn create_upstream(storage: &PostgresStorage, name: &str) -> TestResult<Uuid> {
    create_upstream_kind(storage, name, UpstreamKind::AnthropicOauth, None).await
}

async fn create_upstream_kind(
    storage: &PostgresStorage,
    name: &str,
    kind: UpstreamKind,
    api_key_ciphertext: Option<Vec<u8>>,
) -> TestResult<Uuid> {
    Ok(UpstreamStore::create(
        storage,
        UpstreamCreate {
            id: Uuid::new_v4(),
            name: name.to_owned(),
            kind,
            base_url: None,
            api_key_ciphertext,
            oauth_tokens: None,
            warmup_enabled: false,
            warmup_dialect_plugin: None,
            workspace_id: None,
            api_key_access: None,
        },
    )
    .await?
    .id)
}

fn affinity_key(principal_id: &str, value_sha256: [u8; 32]) -> UpstreamAffinityKey {
    affinity_key_with_kind(
        principal_id,
        value_sha256,
        "anthropic",
        UpstreamAffinityKind::AnthropicWebSearchEncryptedContent,
    )
}

fn affinity_key_with_kind(
    principal_id: &str,
    value_sha256: [u8; 32],
    provider: &str,
    kind: UpstreamAffinityKind,
) -> UpstreamAffinityKey {
    UpstreamAffinityKey {
        principal_id: principal_id.to_owned(),
        provider: provider.to_owned(),
        kind,
        value_sha256,
    }
}

fn access(project_id: Option<&str>, checked_at_unix_secs: u64) -> ApiKeyAccessWrite {
    ApiKeyAccessWrite {
        verified: true,
        key_class: ApiKeyClass::Unknown,
        key_hint: "test-key".to_owned(),
        api_key_id: None,
        needs_workspace: false,
        capabilities: ApiKeyCapabilities::not_checked(),
        remediation: Vec::new(),
        usage_report_state: Some(ApiKeyOrgCapability::NotApplicable),
        provider: None,
        project_id: project_id.map(str::to_owned),
        project_source: None,
        provider_report_state: None,
        provider_report_status_metadata: None,
        last_error: None,
        checked_at_unix_secs: Some(checked_at_unix_secs),
        last_attempt_at_unix_secs: checked_at_unix_secs,
    }
}

fn binding(
    key: UpstreamAffinityKey,
    upstream_id: Uuid,
    observed_at_unix_secs: u64,
    expires_at_unix_secs: Option<u64>,
) -> UpstreamAffinityBinding {
    UpstreamAffinityBinding {
        key,
        upstream_id,
        owner_epoch: 0,
        observed_at_unix_secs,
        expires_at_unix_secs,
    }
}

fn digest(value: &str) -> [u8; 32] {
    Sha256::digest(value.as_bytes()).into()
}

struct Fixture {
    schema: String,
    admin_pool: PgPool,
    pool: PgPool,
}

impl Fixture {
    async fn create() -> TestResult<Option<Self>> {
        let Some(url) = std::env::var("CI_POSTGRES_URL").ok() else {
            eprintln!("skip: CI_POSTGRES_URL not set");
            return Ok(None);
        };
        let schema = format!("upstream_affinity_{}", Uuid::new_v4().simple());
        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await?;
        sqlx::query(AssertSqlSafe(format!(
            "CREATE SCHEMA {}",
            quote_ident(&schema)
        )))
        .execute(&admin_pool)
        .await?;
        let options = PgConnectOptions::from_str(&url)?.options([("search_path", schema.as_str())]);
        let pool = PgPoolOptions::new()
            .max_connections(4)
            .connect_with(options)
            .await?;
        Ok(Some(Self {
            schema,
            admin_pool,
            pool,
        }))
    }

    fn store(&self) -> PostgresStorage {
        PostgresStorage::new(self.pool.clone(), Arc::new(cc_lb_clock::SystemClock))
    }

    fn pool(&self) -> &PgPool {
        &self.pool
    }

    async fn drop_schema(self) -> TestResult<()> {
        self.pool.close().await;
        sqlx::query(AssertSqlSafe(format!(
            "DROP SCHEMA IF EXISTS {} CASCADE",
            quote_ident(&self.schema)
        )))
        .execute(&self.admin_pool)
        .await?;
        self.admin_pool.close().await;
        Ok(())
    }
}

fn quote_ident(identifier: &str) -> String {
    assert!(
        identifier
            .chars()
            .all(|character| character.is_ascii_lowercase()
                || character.is_ascii_digit()
                || character == '_'),
        "unsafe postgres identifier: {identifier}"
    );
    format!("\"{identifier}\"")
}
