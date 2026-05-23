use cc_lb_storage_api::{
    LimitStateStore, QuotaStore,
    types::{BucketKind, PrincipalLimitIdentityKind, PrincipalLimitKind, PrincipalLimitState},
};
use cc_lb_storage_redb::Storage;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quota_store_trait_path_concurrent_increments_are_atomic()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("adapter-quota.redb");
    let storage = Storage::open(&path, [61; 32])?;
    let mut join_set = tokio::task::JoinSet::new();

    for _ in 0..100 {
        let storage = storage.clone();
        join_set.spawn(async move {
            QuotaStore::incr_quota(
                &storage,
                "adapter-alice",
                1_765_000_000,
                BucketKind::Requests,
                1,
            )
            .await
        });
    }

    while let Some(result) = join_set.join_next().await {
        result??;
    }

    let final_count = QuotaStore::get_quota(
        &storage,
        "adapter-alice",
        1_765_000_000,
        BucketKind::Requests,
    )
    .await?;
    assert_eq!(final_count, 100);

    Ok(())
}

#[tokio::test]
async fn quota_store_trait_path_try_incr_respects_capacity_boundary()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("adapter-quota-capacity.redb");
    let storage = Storage::open(&path, [63; 32])?;

    let first = QuotaStore::try_incr_quota(
        &storage,
        "adapter-alice",
        1_765_000_100,
        BucketKind::InputTokens,
        4,
        10,
    )
    .await?;
    let at_capacity = QuotaStore::try_incr_quota(
        &storage,
        "adapter-alice",
        1_765_000_100,
        BucketKind::InputTokens,
        6,
        10,
    )
    .await?;
    let over_capacity = QuotaStore::try_incr_quota(
        &storage,
        "adapter-alice",
        1_765_000_100,
        BucketKind::InputTokens,
        1,
        10,
    )
    .await?;

    assert_eq!(first, Some(4));
    assert_eq!(at_capacity, Some(10));
    assert_eq!(over_capacity, None);
    assert_eq!(
        QuotaStore::get_quota(
            &storage,
            "adapter-alice",
            1_765_000_100,
            BucketKind::InputTokens,
        )
        .await?,
        10
    );
    assert_eq!(
        QuotaStore::adjust_quota(
            &storage,
            "adapter-alice",
            1_765_000_100,
            BucketKind::InputTokens,
            -3,
        )
        .await?,
        7
    );

    Ok(())
}

#[tokio::test]
async fn quota_store_trait_path_sweep_removes_only_older_windows()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("adapter-quota-sweep.redb");
    let storage = Storage::open(&path, [65; 32])?;

    QuotaStore::incr_quota(&storage, "adapter-alice", 10, BucketKind::Requests, 3).await?;
    QuotaStore::incr_quota(&storage, "adapter-alice", 20, BucketKind::InputTokens, 5).await?;
    QuotaStore::incr_quota(&storage, "adapter-alice", 30, BucketKind::OutputTokens, 7).await?;
    QuotaStore::incr_quota(&storage, "adapter-bob", 30, BucketKind::Requests, 11).await?;

    let deleted = QuotaStore::sweep_old_quotas(&storage, 30).await?;

    assert_eq!(deleted, 2);
    assert_eq!(
        QuotaStore::get_quota(&storage, "adapter-alice", 10, BucketKind::Requests).await?,
        0
    );
    assert_eq!(
        QuotaStore::get_quota(&storage, "adapter-alice", 20, BucketKind::InputTokens).await?,
        0
    );
    assert_eq!(
        QuotaStore::get_quota(&storage, "adapter-alice", 30, BucketKind::OutputTokens).await?,
        7
    );
    assert_eq!(
        QuotaStore::get_quota(&storage, "adapter-bob", 30, BucketKind::Requests).await?,
        11
    );

    Ok(())
}

#[tokio::test]
async fn limit_state_store_trait_path_upserts_and_lists_latest_snapshots()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("adapter-limit-state.redb");
    let storage = Storage::open(&path, [67; 32])?;

    LimitStateStore::put_principal_limit_state(
        &storage,
        &state(
            "adapter-principal-a",
            PrincipalLimitIdentityKind::Account,
            Some("org-a"),
            true,
            "hourly",
            PrincipalLimitKind::Requests,
            Some(100),
            Some(90),
            Some("2026-05-24T00:00:01Z"),
            1_764_000_001,
        ),
    )
    .await?;
    LimitStateStore::put_principal_limit_state(
        &storage,
        &state(
            "adapter-principal-a",
            PrincipalLimitIdentityKind::Account,
            Some("org-a"),
            true,
            "hourly",
            PrincipalLimitKind::Requests,
            Some(100),
            Some(80),
            Some("2026-05-24T00:00:02Z"),
            1_764_000_002,
        ),
    )
    .await?;
    LimitStateStore::put_principal_limit_state(
        &storage,
        &state(
            "adapter-principal-a",
            PrincipalLimitIdentityKind::Credential,
            Some("credential-a"),
            false,
            "daily",
            PrincipalLimitKind::Tokens,
            Some(1000),
            Some(700),
            None,
            1_764_000_003,
        ),
    )
    .await?;
    LimitStateStore::put_principal_limit_state(
        &storage,
        &state(
            "adapter-principal-b",
            PrincipalLimitIdentityKind::Account,
            Some("org-b"),
            true,
            "hourly",
            PrincipalLimitKind::Requests,
            Some(200),
            Some(190),
            None,
            1_764_000_004,
        ),
    )
    .await?;

    let latest = LimitStateStore::get_principal_limit_state(
        &storage,
        "adapter-principal-a",
        PrincipalLimitIdentityKind::Account,
        Some("org-a"),
        "hourly",
        PrincipalLimitKind::Requests,
    )
    .await?
    .expect("latest account snapshot exists");
    assert_eq!(latest.remaining, Some(80));
    assert_eq!(latest.reset.as_deref(), Some("2026-05-24T00:00:02Z"));
    assert_eq!(latest.stored_at_unix_secs, 1_764_000_002);

    let listed =
        LimitStateStore::list_principal_limit_states(&storage, "adapter-principal-a").await?;
    assert_eq!(listed.len(), 2);
    assert!(
        listed
            .iter()
            .all(|snapshot| snapshot.principal_id == "adapter-principal-a")
    );
    assert!(listed.iter().any(|snapshot| {
        snapshot.identity_kind == PrincipalLimitIdentityKind::Account
            && snapshot.remaining == Some(80)
    }));
    assert!(listed.iter().any(|snapshot| {
        snapshot.identity_kind == PrincipalLimitIdentityKind::Credential
            && snapshot.remaining == Some(700)
    }));

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn state(
    principal_id: &str,
    identity_kind: PrincipalLimitIdentityKind,
    identity_value: Option<&str>,
    account_observed: bool,
    window: &str,
    kind: PrincipalLimitKind,
    limit: Option<u64>,
    remaining: Option<u64>,
    reset: Option<&str>,
    stored_at_unix_secs: u64,
) -> PrincipalLimitState {
    PrincipalLimitState {
        principal_id: principal_id.to_owned(),
        identity_kind,
        identity_value: identity_value.map(ToOwned::to_owned),
        account_observed,
        window: window.to_owned(),
        kind,
        limit,
        remaining,
        reset: reset.map(ToOwned::to_owned),
        observed_at_unix_secs: 1_764_000_000,
        stored_at_unix_secs,
    }
}
