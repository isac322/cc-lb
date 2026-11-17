use std::sync::Arc;

use cc_lb_storage_api::{
    AccountOwnerBinding, AccountOwnerStore, AccountOwnerWorkspaceTarget, MetaStore,
    OwnerBindingExpectation, PrincipalCreate, PrincipalKind, PrincipalStore, StorageError,
    UpstreamCreate, UpstreamKind, UpstreamStatusUpdate, UpstreamStore,
};
use cc_lb_storage_sqlite::SqliteStorage;
use uuid::Uuid;

async fn open(name: &str) -> (tempfile::TempDir, SqliteStorage, String) {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let database_url = format!("sqlite://{}", temp_dir.path().join(name).display());
    let storage =
        cc_lb_storage_sqlite::open_sqlite(&database_url, Arc::new(cc_lb_clock::SystemClock))
            .await
            .expect("open sqlite");
    storage.initialize().await.expect("initialize sqlite");
    (temp_dir, storage, database_url)
}

async fn seed(storage: &SqliteStorage) -> (Uuid, Uuid) {
    let principal = PrincipalStore::create(
        storage,
        PrincipalCreate {
            name: "owner-principal".to_owned(),
            kind: PrincipalKind::Machine,
            allowed_models: Vec::new(),
            allowed_upstreams: Vec::new(),
            default_limits: Vec::new(),
            cache_keepalive: None,
        },
        1,
    )
    .await
    .expect("create principal");

    let upstream_id = Uuid::new_v4();
    UpstreamStore::create(
        storage,
        UpstreamCreate {
            id: upstream_id,
            name: "owner-upstream".to_owned(),
            kind: UpstreamKind::OpenaiApiKey,
            base_url: None,
            api_key_ciphertext: Some(vec![1, 2, 3]),
            oauth_tokens: None,
            warmup_enabled: false,
            warmup_dialect_plugin: None,
            workspace_id: None,
            api_key_access: None,
        },
    )
    .await
    .expect("create upstream");
    (principal.id, upstream_id)
}

fn binding(principal_id: Uuid, upstream_id: Uuid) -> AccountOwnerBinding {
    AccountOwnerBinding {
        binding_id: "binding-1".to_owned(),
        principal_id: principal_id.to_string(),
        provider: "openai".to_owned(),
        owner_id: "owner-1".to_owned(),
        upstream_id,
        owner_generation: 1,
        local_user_id: "local-user-1".to_owned(),
        allowed_workspaces: vec!["workspace-a".to_owned(), "workspace-b".to_owned()],
        provider_workspace_id: "vendor-workspace-1".to_owned(),
        outbound_region: Some("us-east-1".to_owned()),
        revision: 1,
        revoked_at_unix_secs: None,
    }
}

fn expectation(binding: &AccountOwnerBinding, workspace_id: &str) -> OwnerBindingExpectation {
    OwnerBindingExpectation {
        binding_id: binding.binding_id.clone(),
        principal_id: binding.principal_id.clone(),
        provider: binding.provider.clone(),
        owner_generation: binding.owner_generation,
        binding_revision: binding.revision,
        workspace_id: workspace_id.to_owned(),
    }
}

fn assert_conflict<T: std::fmt::Debug>(result: Result<T, StorageError>) {
    assert!(
        matches!(result, Err(StorageError::Conflict { .. })),
        "expected conflict, got {result:?}"
    );
}

fn assert_unavailable<T: std::fmt::Debug>(result: Result<T, StorageError>) {
    assert!(
        matches!(result, Err(StorageError::UpstreamUnavailable { .. })),
        "expected upstream unavailable, got {result:?}"
    );
}

#[tokio::test]
async fn create_get_and_restart_preserve_binding_without_secrets() {
    let (_temp_dir, storage, database_url) = open("account-owner-restart.sqlite").await;
    let (principal_id, upstream_id) = seed(&storage).await;
    let expected_binding = binding(principal_id, upstream_id);
    AccountOwnerStore::create_owner_binding(&storage, expected_binding.clone())
        .await
        .expect("create binding");

    let stored = AccountOwnerStore::get_owner_binding(&storage, &expected_binding.binding_id)
        .await
        .expect("get binding")
        .expect("binding exists");
    assert_eq!(stored, expected_binding);
    let columns: Vec<String> = sqlx::query_scalar(
        "SELECT name FROM pragma_table_info('account_owner_bindings_v1') ORDER BY cid",
    )
    .fetch_all(storage.pool())
    .await
    .expect("read binding columns");
    assert!(!columns.iter().any(|column| column.contains("secret")));
    assert_eq!(
        columns,
        [
            "binding_id",
            "principal_id",
            "provider",
            "owner_id",
            "upstream_id",
            "owner_generation",
            "local_user_id",
            "allowed_workspaces_json",
            "provider_workspace_id",
            "outbound_region",
            "revision",
            "revoked_at_unix_secs",
        ]
    );

    storage.pool().close().await;
    drop(storage);
    let reopened = cc_lb_storage_sqlite::open_sqlite_existing(
        &database_url,
        Arc::new(cc_lb_clock::SystemClock),
    )
    .await
    .expect("reopen sqlite");
    reopened.initialize().await.expect("reinitialize sqlite");
    assert_eq!(
        AccountOwnerStore::get_owner_binding(&reopened, &expected_binding.binding_id)
            .await
            .expect("get binding after restart"),
        Some(expected_binding)
    );
}

#[tokio::test]
async fn tuple_generation_revision_and_workspace_mismatches_are_conflicts() {
    let (_temp_dir, storage, _) = open("account-owner-mismatch.sqlite").await;
    let (principal_id, upstream_id) = seed(&storage).await;
    let stored = binding(principal_id, upstream_id);
    AccountOwnerStore::create_owner_binding(&storage, stored.clone())
        .await
        .expect("create binding");

    let mut wrong = expectation(&stored, "workspace-a");
    wrong.principal_id = "other-principal".to_owned();
    assert_conflict(AccountOwnerStore::resolve_owner_binding(&storage, &wrong).await);
    wrong = expectation(&stored, "workspace-a");
    wrong.provider = "anthropic".to_owned();
    assert_conflict(AccountOwnerStore::resolve_owner_binding(&storage, &wrong).await);
    wrong = expectation(&stored, "workspace-a");
    wrong.owner_generation = 2;
    assert_conflict(AccountOwnerStore::resolve_owner_binding(&storage, &wrong).await);
    wrong = expectation(&stored, "workspace-a");
    wrong.binding_revision = 2;
    assert_conflict(AccountOwnerStore::resolve_owner_binding(&storage, &wrong).await);
    wrong = expectation(&stored, "workspace-other");
    assert_conflict(AccountOwnerStore::resolve_owner_binding(&storage, &wrong).await);
}

#[tokio::test]
async fn disabled_deleted_and_unhealthy_upstreams_are_unavailable() {
    let (_temp_dir, storage, _) = open("account-owner-upstream-state.sqlite").await;
    let (principal_id, upstream_id) = seed(&storage).await;
    let stored = binding(principal_id, upstream_id);
    AccountOwnerStore::create_owner_binding(&storage, stored.clone())
        .await
        .expect("create binding");

    let upstream = UpstreamStore::get_by_id(&storage, upstream_id)
        .await
        .expect("get upstream")
        .expect("upstream exists");
    UpstreamStore::set_enabled(&storage, upstream_id, upstream.revision, false)
        .await
        .expect("disable upstream");
    assert_unavailable(
        AccountOwnerStore::resolve_owner_binding(&storage, &expectation(&stored, "workspace-a"))
            .await,
    );
    let mut foreign = expectation(&stored, "workspace-a");
    foreign.principal_id = Uuid::new_v4().to_string();
    assert_conflict(AccountOwnerStore::resolve_owner_binding(&storage, &foreign).await);
    foreign = expectation(&stored, "workspace-a");
    foreign.provider = "anthropic".to_owned();
    assert_conflict(AccountOwnerStore::resolve_owner_binding(&storage, &foreign).await);

    let upstream = UpstreamStore::get_by_id(&storage, upstream_id)
        .await
        .expect("get disabled upstream")
        .expect("disabled upstream exists");
    UpstreamStore::set_enabled(&storage, upstream_id, upstream.revision, true)
        .await
        .expect("enable upstream");
    UpstreamStore::set_status(
        &storage,
        upstream_id,
        UpstreamStatusUpdate {
            last_apply_error: Some(Some("apply failed".to_owned())),
            ..UpstreamStatusUpdate::default()
        },
    )
    .await
    .expect("set apply error");
    assert_unavailable(
        AccountOwnerStore::resolve_owner_binding(&storage, &expectation(&stored, "workspace-a"))
            .await,
    );

    UpstreamStore::set_status(
        &storage,
        upstream_id,
        UpstreamStatusUpdate {
            last_apply_error: Some(None),
            ..UpstreamStatusUpdate::default()
        },
    )
    .await
    .expect("clear apply error");
    let upstream = UpstreamStore::get_by_id(&storage, upstream_id)
        .await
        .expect("get healthy upstream")
        .expect("healthy upstream exists");
    UpstreamStore::soft_delete(&storage, upstream_id, upstream.revision)
        .await
        .expect("delete upstream");
    assert_unavailable(
        AccountOwnerStore::resolve_owner_binding(&storage, &expectation(&stored, "workspace-a"))
            .await,
    );
}

#[tokio::test]
async fn replacement_preserves_generation_then_advances_and_withdraws_permission() {
    let (_temp_dir, storage, _) = open("account-owner-replacement.sqlite").await;
    let (principal_id, upstream_id) = seed(&storage).await;
    let stored = binding(principal_id, upstream_id);
    AccountOwnerStore::create_owner_binding(&storage, stored.clone())
        .await
        .expect("create binding");

    let mut same_owner = stored.clone();
    same_owner.allowed_workspaces = vec!["workspace-b".to_owned()];
    same_owner.revision = 2;
    same_owner.outbound_region = Some("different-region".to_owned());
    AccountOwnerStore::compare_and_replace_owner_binding(&storage, same_owner.clone(), 1)
        .await
        .expect("same-owner replacement");
    assert_eq!(same_owner.owner_generation, 1);
    assert_conflict(
        AccountOwnerStore::resolve_owner_binding(
            &storage,
            &expectation(&same_owner, "workspace-a"),
        )
        .await,
    );
    assert_eq!(
        AccountOwnerStore::resolve_owner_binding(
            &storage,
            &expectation(&same_owner, "workspace-b")
        )
        .await
        .expect("resolve replacement"),
        same_owner,
    );

    let mut new_owner = same_owner.clone();
    new_owner.owner_id = "owner-2".to_owned();
    new_owner.revision = 3;
    new_owner.owner_generation = 2;
    AccountOwnerStore::compare_and_replace_owner_binding(&storage, new_owner.clone(), 2)
        .await
        .expect("identity replacement");
    assert_conflict(
        AccountOwnerStore::compare_and_replace_owner_binding(&storage, new_owner.clone(), 2).await,
    );

    let mut revoked = new_owner.clone();
    revoked.revision = 4;
    revoked.revoked_at_unix_secs = Some(99);
    AccountOwnerStore::compare_and_replace_owner_binding(&storage, revoked.clone(), 3)
        .await
        .expect("revoke binding");
    assert_conflict(
        AccountOwnerStore::resolve_owner_binding(&storage, &expectation(&revoked, "workspace-b"))
            .await,
    );
    let mut resurrected = revoked.clone();
    resurrected.revision = 5;
    resurrected.revoked_at_unix_secs = None;
    assert_conflict(
        AccountOwnerStore::compare_and_replace_owner_binding(&storage, resurrected, 4).await,
    );
    assert_conflict(AccountOwnerStore::create_owner_binding(&storage, stored).await);
}

#[tokio::test]
async fn duplicate_create_and_cas_races_commit_only_one_winner() {
    let (_temp_dir, storage, _) = open("account-owner-race.sqlite").await;
    let (principal_id, upstream_id) = seed(&storage).await;
    let first = binding(principal_id, upstream_id);
    let second_storage = storage.clone();
    let second = first.clone();
    let (left, right) = tokio::join!(
        AccountOwnerStore::create_owner_binding(&storage, first.clone()),
        AccountOwnerStore::create_owner_binding(&second_storage, second),
    );
    assert_eq!(usize::from(left.is_ok()) + usize::from(right.is_ok()), 1);
    assert!(
        matches!(left, Err(StorageError::Conflict { .. }))
            || matches!(right, Err(StorageError::Conflict { .. }))
    );
    let stored = AccountOwnerStore::get_owner_binding(&storage, &first.binding_id)
        .await
        .expect("get race binding")
        .expect("race binding exists");

    let mut left_replacement = stored.clone();
    left_replacement.allowed_workspaces = vec!["workspace-left".to_owned()];
    left_replacement.revision = 2;
    let mut right_replacement = stored.clone();
    right_replacement.allowed_workspaces = vec!["workspace-right".to_owned()];
    right_replacement.revision = 2;
    let second_storage = storage.clone();
    let (left, right) = tokio::join!(
        AccountOwnerStore::compare_and_replace_owner_binding(
            &storage,
            left_replacement,
            stored.revision
        ),
        AccountOwnerStore::compare_and_replace_owner_binding(
            &second_storage,
            right_replacement,
            stored.revision
        ),
    );
    assert_eq!(usize::from(left.is_ok()) + usize::from(right.is_ok()), 1);
    assert!(
        matches!(left, Err(StorageError::Conflict { .. }))
            || matches!(right, Err(StorageError::Conflict { .. }))
    );
    let after = AccountOwnerStore::get_owner_binding(&storage, &stored.binding_id)
        .await
        .expect("get post-race binding")
        .expect("post-race binding exists");
    assert_eq!(after.revision, 2);
    assert!(
        after.allowed_workspaces == ["workspace-left"]
            || after.allowed_workspaces == ["workspace-right"]
    );
    let mappings: Vec<String> = sqlx::query_scalar(
        "SELECT workspace_id FROM account_owner_workspaces_v1
         WHERE workspace_id IN ('workspace-left', 'workspace-right')",
    )
    .fetch_all(storage.pool())
    .await
    .expect("read race aliases");
    assert_eq!(mappings, after.allowed_workspaces);
}

#[tokio::test]
async fn workspace_locator_isolates_owners_and_alias_conflicts_roll_back() {
    let (_temp_dir, storage, _) = open("account-owner-aliases.sqlite").await;
    let (principal_id, upstream_id) = seed(&storage).await;
    let first = binding(principal_id, upstream_id);
    storage
        .create_owner_binding(first.clone())
        .await
        .expect("create first owner");
    let mut second = first.clone();
    second.binding_id = "binding-2".to_owned();
    second.owner_id = "owner-2".to_owned();
    second.provider_workspace_id = "vendor-workspace-2".to_owned();
    second.allowed_workspaces = vec!["workspace-c".to_owned()];
    storage
        .create_owner_binding(second.clone())
        .await
        .expect("create second owner");

    let target = |workspace: &str| AccountOwnerWorkspaceTarget {
        principal_id: first.principal_id.clone(),
        provider: first.provider.clone(),
        local_user_id: first.local_user_id.clone(),
        workspace_id: workspace.to_owned(),
    };
    assert_eq!(
        storage
            .find_owner_binding_for_workspace(&target("workspace-a"))
            .await
            .expect("locate first"),
        first,
    );
    assert_eq!(
        storage
            .find_owner_binding_for_workspace(&target("workspace-c"))
            .await
            .expect("locate second"),
        second,
    );
    let mut wrong_user = target("workspace-c");
    wrong_user.local_user_id = "foreign-user".to_owned();
    assert_conflict(storage.find_owner_binding_for_workspace(&wrong_user).await);
    let mut wrong_principal = target("workspace-c");
    wrong_principal.principal_id = Uuid::new_v4().to_string();
    assert_conflict(
        storage
            .find_owner_binding_for_workspace(&wrong_principal)
            .await,
    );

    let mut duplicate_alias = second.clone();
    duplicate_alias.binding_id = "binding-conflict".to_owned();
    duplicate_alias.allowed_workspaces = vec![
        "workspace-created-before-conflict".to_owned(),
        "workspace-a".to_owned(),
    ];
    assert_conflict(storage.create_owner_binding(duplicate_alias.clone()).await);
    assert_eq!(
        storage
            .get_owner_binding(&duplicate_alias.binding_id)
            .await
            .expect("read failed create"),
        None
    );
    assert_conflict(
        storage
            .find_owner_binding_for_workspace(&target("workspace-created-before-conflict"))
            .await,
    );

    let mut failed_replacement = second.clone();
    failed_replacement.revision = 2;
    failed_replacement.allowed_workspaces = vec![
        "workspace-created-before-cas-conflict".to_owned(),
        "workspace-a".to_owned(),
    ];
    assert_conflict(
        storage
            .compare_and_replace_owner_binding(failed_replacement, 1)
            .await,
    );
    assert_eq!(
        storage
            .get_owner_binding(&second.binding_id)
            .await
            .expect("read failed replacement"),
        Some(second),
    );
    assert_conflict(
        storage
            .find_owner_binding_for_workspace(&target("workspace-created-before-cas-conflict"))
            .await,
    );

    let mut withdrawn = first.clone();
    withdrawn.revision = 2;
    withdrawn.allowed_workspaces = vec!["workspace-b".to_owned()];
    storage
        .compare_and_replace_owner_binding(withdrawn, 1)
        .await
        .expect("withdraw alias");
    assert_conflict(
        storage
            .find_owner_binding_for_workspace(&target("workspace-a"))
            .await,
    );
    duplicate_alias.allowed_workspaces = vec!["workspace-a".to_owned()];
    assert_conflict(storage.create_owner_binding(duplicate_alias).await);
}

#[tokio::test]
async fn vendor_workspace_changes_require_a_new_owner_generation() {
    let (_temp_dir, storage, _) = open("account-owner-vendor-workspace.sqlite").await;
    let (principal_id, upstream_id) = seed(&storage).await;
    let first = binding(principal_id, upstream_id);
    storage
        .create_owner_binding(first.clone())
        .await
        .expect("create binding");
    let mut moved = first.clone();
    moved.provider_workspace_id = "vendor-workspace-2".to_owned();
    moved.revision = 2;
    assert_conflict(
        storage
            .compare_and_replace_owner_binding(moved.clone(), 1)
            .await,
    );
    moved.owner_generation = 2;
    storage
        .compare_and_replace_owner_binding(moved.clone(), 1)
        .await
        .expect("move vendor workspace");
    assert_conflict(
        storage
            .resolve_owner_binding(&expectation(&first, "workspace-a"))
            .await,
    );
    assert_eq!(
        storage
            .resolve_owner_binding(&expectation(&moved, "workspace-a"))
            .await
            .expect("resolve moved owner"),
        moved,
    );
}

#[tokio::test]
async fn dead_owners_allow_withdrawal_and_revocation_but_not_new_grants_or_moves() {
    for state in ["disabled", "deleted", "apply-error"] {
        let (_temp_dir, storage, _) = open(&format!("account-owner-{state}.sqlite")).await;
        let (principal_id, upstream_id) = seed(&storage).await;
        let first = binding(principal_id, upstream_id);
        storage
            .create_owner_binding(first.clone())
            .await
            .expect("create binding");
        match state {
            "disabled" => {
                UpstreamStore::set_enabled(&storage, upstream_id, 1, false)
                    .await
                    .expect("disable upstream");
            }
            "deleted" => {
                UpstreamStore::soft_delete(&storage, upstream_id, 1)
                    .await
                    .expect("soft-delete upstream");
            }
            _ => {
                UpstreamStore::set_status(
                    &storage,
                    upstream_id,
                    UpstreamStatusUpdate {
                        last_apply_error: Some(Some("apply failed".to_owned())),
                        ..UpstreamStatusUpdate::default()
                    },
                )
                .await
                .expect("set apply error");
            }
        }

        let mut grant = first.clone();
        grant.revision = 2;
        grant.allowed_workspaces.push("new-workspace".to_owned());
        assert_unavailable(storage.compare_and_replace_owner_binding(grant, 1).await);
        let mut moved = first.clone();
        moved.revision = 2;
        moved.owner_generation = 2;
        moved.provider_workspace_id = "vendor-workspace-2".to_owned();
        assert_unavailable(storage.compare_and_replace_owner_binding(moved, 1).await);
        let mut region_change = first.clone();
        region_change.revision = 2;
        region_change.outbound_region = Some("different-region".to_owned());
        assert_unavailable(
            storage
                .compare_and_replace_owner_binding(region_change, 1)
                .await,
        );

        let mut withdrawn = first.clone();
        withdrawn.revision = 2;
        withdrawn.allowed_workspaces = vec!["workspace-b".to_owned()];
        storage
            .compare_and_replace_owner_binding(withdrawn.clone(), 1)
            .await
            .expect("withdraw from dead owner");
        let mut revoked = withdrawn;
        revoked.revision = 3;
        revoked.revoked_at_unix_secs = Some(100);
        storage
            .compare_and_replace_owner_binding(revoked.clone(), 2)
            .await
            .expect("revoke dead owner");
        assert_conflict(
            storage
                .resolve_owner_binding(&expectation(&revoked, "workspace-b"))
                .await,
        );
        assert_conflict(storage.create_owner_binding(first).await);
    }
}

#[tokio::test]
async fn invalid_create_and_immutable_replacements_leave_no_changes() {
    let (_temp_dir, storage, _) = open("account-owner-invalid.sqlite").await;
    let (principal_id, upstream_id) = seed(&storage).await;
    let first = binding(principal_id, upstream_id);
    let mut invalid = first.clone();
    invalid.allowed_workspaces.push("workspace-a".to_owned());
    assert_conflict(storage.create_owner_binding(invalid).await);
    let mut invalid = first.clone();
    invalid.provider_workspace_id.clear();
    assert_conflict(storage.create_owner_binding(invalid).await);
    let mut invalid = first.clone();
    invalid.provider = "anthropic".to_owned();
    assert_conflict(storage.create_owner_binding(invalid).await);
    let mut invalid = first.clone();
    invalid.revision = 2;
    assert_conflict(storage.create_owner_binding(invalid).await);
    assert_eq!(
        storage
            .get_owner_binding(&first.binding_id)
            .await
            .expect("read invalid create"),
        None
    );

    storage
        .create_owner_binding(first.clone())
        .await
        .expect("create valid binding");
    for field in ["principal", "local-user", "generation"] {
        let mut invalid = first.clone();
        invalid.revision = 2;
        match field {
            "principal" => invalid.principal_id = Uuid::new_v4().to_string(),
            "local-user" => invalid.local_user_id = "another-local-user".to_owned(),
            _ => invalid.owner_generation = 2,
        }
        assert_conflict(storage.compare_and_replace_owner_binding(invalid, 1).await);
    }
    assert_eq!(
        storage
            .get_owner_binding(&first.binding_id)
            .await
            .expect("read unchanged binding"),
        Some(first)
    );
}

#[tokio::test]
async fn workspace_list_is_sorted_scoped_and_intersects_permanent_reservations() {
    let (_temp_dir, storage, _) = open("owner-workspace-list.sqlite").await;
    let (principal_id, upstream_id) = seed(&storage).await;
    let mut first = binding(principal_id, upstream_id);
    first.allowed_workspaces = vec!["z".into(), "A".into(), "a".into(), "withdrawn".into()];
    storage
        .create_owner_binding(first.clone())
        .await
        .expect("first");
    let mut second = first.clone();
    second.binding_id = "binding-2".into();
    second.owner_id = "owner-2".into();
    second.allowed_workspaces = vec!["b".into()];
    storage.create_owner_binding(second).await.expect("second");
    first.allowed_workspaces = vec!["z".into(), "A".into(), "a".into()];
    first.revision += 1;
    storage
        .compare_and_replace_owner_binding(first.clone(), 1)
        .await
        .expect("withdraw");
    assert_eq!(
        storage
            .list_owner_workspace_targets(&first.principal_id, "openai", &first.local_user_id)
            .await
            .expect("list"),
        ["A", "a", "b", "z"]
    );
    for (principal, provider, user) in [
        (
            Uuid::new_v4().to_string(),
            "openai",
            first.local_user_id.as_str(),
        ),
        (
            first.principal_id.clone(),
            "anthropic",
            first.local_user_id.as_str(),
        ),
        (first.principal_id.clone(), "openai", "other-user"),
    ] {
        assert!(
            storage
                .list_owner_workspace_targets(&principal, provider, user)
                .await
                .expect("isolated")
                .is_empty()
        );
    }
    for invalid in [
        "invalid".to_owned(),
        principal_id.simple().to_string(),
        "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA".to_owned(),
    ] {
        assert_conflict(
            storage
                .list_owner_workspace_targets(&invalid, "openai", &first.local_user_id)
                .await,
        );
    }
    let reserved: String = sqlx::query_scalar(
        "SELECT binding_id FROM account_owner_workspaces_v1 WHERE principal_id = ? AND provider = 'openai' AND workspace_id = 'withdrawn'",
    ).bind(&first.principal_id).fetch_one(storage.pool()).await.expect("reservation retained");
    assert_eq!(reserved, first.binding_id);
    // A current grant without its matching permanent reservation is not listed.
    sqlx::query("UPDATE account_owner_bindings_v1 SET allowed_workspaces_json = '[\"unreserved\"]' WHERE binding_id = ?")
        .bind(&first.binding_id).execute(storage.pool()).await.expect("simulate unreserved grant");
    assert_eq!(
        storage
            .list_owner_workspace_targets(&first.principal_id, "openai", &first.local_user_id)
            .await
            .expect("reserved only"),
        ["b"]
    );
}

#[tokio::test]
async fn workspace_list_filters_binding_principal_and_upstream_state() {
    let (_temp_dir, storage, _) = open("owner-workspace-list-state.sqlite").await;
    let (principal_id, upstream_id) = seed(&storage).await;
    let stored = binding(principal_id, upstream_id);
    storage
        .create_owner_binding(stored.clone())
        .await
        .expect("binding");
    let list = || {
        storage.list_owner_workspace_targets(&stored.principal_id, "openai", &stored.local_user_id)
    };
    assert_eq!(list().await.expect("unrestricted").len(), 2);
    for restriction in [
        vec![Uuid::new_v4().to_string()],
        vec![upstream_id.to_string()],
        vec![],
    ] {
        sqlx::query("UPDATE principals_v1 SET allowed_upstreams = ? WHERE id = ?")
            .bind(serde_json::to_string(&restriction).expect("json"))
            .bind(&stored.principal_id)
            .execute(storage.pool())
            .await
            .expect("restrict principal");
        assert_eq!(
            list().await.expect("restricted").len(),
            if restriction.is_empty() || restriction[0] == upstream_id.to_string() {
                2
            } else {
                0
            }
        );
    }
    for (hide, restore) in [
        (
            "UPDATE account_owner_bindings_v1 SET revoked_at_unix_secs = 1",
            "UPDATE account_owner_bindings_v1 SET revoked_at_unix_secs = NULL",
        ),
        (
            "UPDATE upstream_spec_v1 SET enabled = 0",
            "UPDATE upstream_spec_v1 SET enabled = 1",
        ),
        (
            "UPDATE upstream_spec_v1 SET deleted_at = 1",
            "UPDATE upstream_spec_v1 SET deleted_at = NULL",
        ),
        (
            "UPDATE upstream_spec_v1 SET kind = 'anthropic_api_key'",
            "UPDATE upstream_spec_v1 SET kind = 'openai_api_key'",
        ),
        (
            "UPDATE principals_v1 SET enabled = 0",
            "UPDATE principals_v1 SET enabled = 1",
        ),
        (
            "UPDATE principals_v1 SET deleted_at = 1",
            "UPDATE principals_v1 SET deleted_at = NULL",
        ),
    ] {
        sqlx::query(hide)
            .execute(storage.pool())
            .await
            .expect("hide candidate");
        assert!(list().await.expect("filtered").is_empty(), "{hide}");
        sqlx::query(restore)
            .execute(storage.pool())
            .await
            .expect("restore candidate");
        assert_eq!(list().await.expect("restored").len(), 2, "{restore}");
    }
    UpstreamStore::set_status(
        &storage,
        upstream_id,
        UpstreamStatusUpdate {
            last_apply_error: Some(Some("apply failed".into())),
            ..UpstreamStatusUpdate::default()
        },
    )
    .await
    .expect("apply error");
    assert!(list().await.expect("unhealthy").is_empty());
}
