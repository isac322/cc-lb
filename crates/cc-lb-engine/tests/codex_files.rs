use std::sync::Arc;

use cc_lb_clock::SystemClock;
use cc_lb_engine::{
    authn_rail::{Authenticated, authenticate_first},
    codex_files::{CodexFiles, SignedUrlMetadata},
};
use cc_lb_storage_api::{
    AccountOwnerBinding, AccountOwnerStore, CodexFileState, MetaStore, OwnerBindingExpectation,
    StorageError,
};
use cc_lb_storage_sqlite::{SqliteStorage, open_sqlite, open_sqlite_existing};
use http::{HeaderMap, HeaderValue};
use uuid::Uuid;

use crate::common::{TestAuthn, TestState, managed_api_key};

const CREATED_AT: u64 = 4_100_000_000;
const PRINCIPAL_ID: &str = "principal-test";

struct Fixture {
    temp_dir: tempfile::TempDir,
    database_url: String,
    storage: SqliteStorage,
    owner: AccountOwnerBinding,
    other_owner: AccountOwnerBinding,
    foreign_owner: AccountOwnerBinding,
}

impl Fixture {
    async fn new() -> Self {
        let temp_dir = tempfile::tempdir().expect("temp directory");
        let database_url = format!(
            "sqlite://{}",
            temp_dir.path().join("files.sqlite").display()
        );
        let storage = open_sqlite(&database_url, Arc::new(SystemClock))
            .await
            .expect("open sqlite");
        storage.initialize().await.expect("initialize sqlite");

        let foreign_principal_id = Uuid::new_v4().to_string();
        for id in [PRINCIPAL_ID, foreign_principal_id.as_str()] {
            sqlx::query(
                "INSERT INTO principals_v1 (id, name, created_at, updated_at) VALUES (?, ?, 1, 1)",
            )
            .bind(id)
            .bind(format!("files-principal-{id}"))
            .execute(storage.pool())
            .await
            .expect("seed principal");
        }

        let owner = seed_binding(&storage, PRINCIPAL_ID, "owner-a", "workspace-a").await;
        let other_owner = seed_binding(&storage, PRINCIPAL_ID, "owner-b", "workspace-b").await;
        let foreign_owner = seed_binding(
            &storage,
            &foreign_principal_id,
            "foreign-owner",
            "workspace-f",
        )
        .await;

        Self {
            temp_dir,
            database_url,
            storage,
            owner,
            other_owner,
            foreign_owner,
        }
    }

    fn expectation(binding: &AccountOwnerBinding) -> OwnerBindingExpectation {
        OwnerBindingExpectation {
            binding_id: binding.binding_id.clone(),
            principal_id: binding.principal_id.clone(),
            provider: binding.provider.clone(),
            owner_generation: binding.owner_generation,
            binding_revision: binding.revision,
            workspace_id: binding.allowed_workspaces[0].clone(),
        }
    }
}

async fn authenticated() -> Authenticated {
    let fixture = TestAuthn::new(TestState::default());
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-api-key",
        HeaderValue::from_str(managed_api_key()).expect("test key header"),
    );
    authenticate_first(&fixture.authn, &headers, &fixture.principal_view)
        .await
        .expect("authenticate test principal")
}

async fn seed_binding(
    storage: &SqliteStorage,
    principal_id: &str,
    owner_id: &str,
    workspace_id: &str,
) -> AccountOwnerBinding {
    let upstream_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO upstream_spec_v1 (id, name, kind, created_at, updated_at) VALUES (?, ?, 'openai_oauth', 1, 1)",
    )
    .bind(upstream_id.to_string())
    .bind(format!("files-upstream-{owner_id}"))
    .execute(storage.pool())
    .await
    .expect("seed upstream");

    let binding = AccountOwnerBinding {
        binding_id: format!("files-binding-{owner_id}"),
        principal_id: principal_id.to_owned(),
        provider: "openai".to_owned(),
        owner_id: owner_id.to_owned(),
        upstream_id,
        owner_generation: 1,
        local_user_id: format!("local-{owner_id}"),
        allowed_workspaces: vec![workspace_id.to_owned()],
        provider_workspace_id: format!("provider-{workspace_id}"),
        outbound_region: None,
        revision: 1,
        revoked_at_unix_secs: None,
    };
    storage
        .create_owner_binding(binding.clone())
        .await
        .expect("seed owner binding");
    binding
}

async fn usage_and_provider_row_counts(storage: &SqliteStorage) -> (i64, i64) {
    let usage: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM usage_rollups_v2")
        .fetch_one(storage.pool())
        .await
        .expect("count usage rows");
    let provider_usage: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM upstream_api_usage_v1")
        .fetch_one(storage.pool())
        .await
        .expect("count provider usage rows");
    (usage, provider_usage)
}

#[tokio::test]
async fn create_finalize_reference_persist_across_restart_without_usage_or_cost_rows() {
    let fixture = Fixture::new().await;
    let auth = authenticated().await;
    let expected = Fixture::expectation(&fixture.owner);
    let file_id = "file-durable-1".to_owned();
    let before_counts = usage_and_provider_row_counts(&fixture.storage).await;

    let created = CodexFiles::new(&fixture.storage)
        .create_success(
            &auth,
            &expected,
            file_id.clone(),
            "input_file".to_owned(),
            CREATED_AT,
            Some(CREATED_AT + 3600),
        )
        .await
        .expect("create succeeds");
    assert_eq!(created.state, CodexFileState::Created);
    assert_eq!(created.owner_id, fixture.owner.owner_id);

    let finalized = CodexFiles::new(&fixture.storage)
        .finalize(
            &auth,
            &expected,
            "input_file",
            &file_id,
            "https://files.example.test/reference/abc".to_owned(),
            Some(CREATED_AT + 7200),
        )
        .await
        .expect("finalize succeeds")
        .expect("created file is visible to selected owner");
    assert_eq!(finalized.state, CodexFileState::Finalized);
    assert_eq!(
        finalized.reference.as_deref(),
        Some("https://files.example.test/reference/abc")
    );
    assert_eq!(
        usage_and_provider_row_counts(&fixture.storage).await,
        before_counts
    );

    drop(fixture.storage);
    let restarted = open_sqlite_existing(&fixture.database_url, Arc::new(SystemClock))
        .await
        .expect("reopen sqlite database");
    let persisted = CodexFiles::new(&restarted)
        .reference(&auth, &expected, "input_file", &file_id)
        .await
        .expect("reference survives restart")
        .expect("persisted selected-owner resource");
    assert_eq!(persisted, finalized);
    drop(restarted);
    drop(fixture.temp_dir);
}

#[tokio::test]
async fn same_provider_kind_and_file_id_coexist_under_scoped_owners() {
    let fixture = Fixture::new().await;
    let auth = authenticated().await;
    let expected = Fixture::expectation(&fixture.owner);
    let other_expected = Fixture::expectation(&fixture.other_owner);
    let file_id = "same-provider-file";

    CodexFiles::new(&fixture.storage)
        .create_success(
            &auth,
            &expected,
            file_id.to_owned(),
            "input_file".to_owned(),
            CREATED_AT,
            None,
        )
        .await
        .expect("create owner A");
    CodexFiles::new(&fixture.storage)
        .create_success(
            &auth,
            &other_expected,
            file_id.to_owned(),
            "input_file".to_owned(),
            CREATED_AT,
            None,
        )
        .await
        .expect("create owner B");

    let owner_a = CodexFiles::new(&fixture.storage)
        .finalize(
            &auth,
            &expected,
            "input_file",
            file_id,
            "https://files.example.test/a".to_owned(),
            None,
        )
        .await
        .expect("finalize owner A")
        .expect("owner A resource");
    let owner_b = CodexFiles::new(&fixture.storage)
        .finalize(
            &auth,
            &other_expected,
            "input_file",
            file_id,
            "https://files.example.test/b".to_owned(),
            None,
        )
        .await
        .expect("finalize owner B")
        .expect("owner B resource");
    assert_eq!(owner_a.owner_id, fixture.owner.owner_id);
    assert_eq!(owner_b.owner_id, fixture.other_owner.owner_id);
    assert_eq!(
        owner_a.reference.as_deref(),
        Some("https://files.example.test/a")
    );
    assert_eq!(
        owner_b.reference.as_deref(),
        Some("https://files.example.test/b")
    );
}

#[tokio::test]
async fn selected_owner_namespace_miss_returns_none_without_other_owner_dispatch() {
    let fixture = Fixture::new().await;
    let auth = authenticated().await;
    let expected = Fixture::expectation(&fixture.owner);
    let other_expected = Fixture::expectation(&fixture.other_owner);
    let file_id = "owner-scoped-file";

    CodexFiles::new(&fixture.storage)
        .create_success(
            &auth,
            &expected,
            file_id.to_owned(),
            "input_file".to_owned(),
            CREATED_AT,
            None,
        )
        .await
        .expect("create owner A");
    let other_owner_observation = CodexFiles::new(&fixture.storage)
        .reference(&auth, &other_expected, "input_file", file_id)
        .await
        .expect("selected owner miss is not a conflict");
    assert!(other_owner_observation.is_none());
}

#[tokio::test]
async fn cross_principal_reference_is_rejected_before_resource_lookup() {
    let fixture = Fixture::new().await;
    let auth = authenticated().await;
    let expected = Fixture::expectation(&fixture.owner);
    let foreign_expected = Fixture::expectation(&fixture.foreign_owner);
    let file_id = "file-principal-fence";

    CodexFiles::new(&fixture.storage)
        .create_success(
            &auth,
            &expected,
            file_id.to_owned(),
            "input_file".to_owned(),
            CREATED_AT,
            None,
        )
        .await
        .expect("create succeeds");
    let cross_principal = CodexFiles::new(&fixture.storage)
        .reference(&auth, &foreign_expected, "input_file", file_id)
        .await;
    assert!(matches!(
        cross_principal,
        Err(StorageError::Conflict { .. })
    ));
}

#[tokio::test]
async fn owner_generation_replacement_and_revocation_reject_old_file_access() {
    let fixture = Fixture::new().await;
    let auth = authenticated().await;
    let expected = Fixture::expectation(&fixture.owner);
    let file_id = "file-generation-fence";
    CodexFiles::new(&fixture.storage)
        .create_success(
            &auth,
            &expected,
            file_id.to_owned(),
            "input_file".to_owned(),
            CREATED_AT,
            None,
        )
        .await
        .expect("create succeeds");

    let mut replacement = fixture.owner.clone();
    replacement.owner_id = "owner-replaced".to_owned();
    replacement.owner_generation = 2;
    replacement.revision = 2;
    fixture
        .storage
        .compare_and_replace_owner_binding(replacement, 1)
        .await
        .expect("replace owner generation");
    let stale = CodexFiles::new(&fixture.storage)
        .reference(&auth, &expected, "input_file", file_id)
        .await;
    assert!(matches!(stale, Err(StorageError::Conflict { .. })));

    let live = fixture
        .storage
        .get_owner_binding(&fixture.owner.binding_id)
        .await
        .expect("read replacement")
        .expect("replacement exists");
    let mut revoked = live.clone();
    revoked.revision = 3;
    revoked.revoked_at_unix_secs = Some(CREATED_AT);
    fixture
        .storage
        .compare_and_replace_owner_binding(revoked, 2)
        .await
        .expect("revoke owner binding");
    let revoked_access = CodexFiles::new(&fixture.storage)
        .reference(&auth, &expected, "input_file", file_id)
        .await;
    assert!(matches!(revoked_access, Err(StorageError::Conflict { .. })));
}

#[tokio::test]
async fn unchanged_identity_revision_refresh_does_not_orphan_resource() {
    let fixture = Fixture::new().await;
    let auth = authenticated().await;
    let expected = Fixture::expectation(&fixture.owner);
    let file_id = "file-revision-refresh";
    CodexFiles::new(&fixture.storage)
        .create_success(
            &auth,
            &expected,
            file_id.to_owned(),
            "input_file".to_owned(),
            CREATED_AT,
            None,
        )
        .await
        .expect("create succeeds");

    let mut refreshed = fixture.owner.clone();
    refreshed.revision = 2;
    fixture
        .storage
        .compare_and_replace_owner_binding(refreshed.clone(), 1)
        .await
        .expect("refresh binding revision");
    let refreshed_expected = Fixture::expectation(&refreshed);
    let found = CodexFiles::new(&fixture.storage)
        .reference(&auth, &refreshed_expected, "input_file", file_id)
        .await
        .expect("reference after revision refresh")
        .expect("resource remains in selected namespace");
    assert_eq!(found.binding_revision, 1);
}

#[tokio::test]
async fn unavailable_upstream_is_preserved_as_unavailable() {
    let fixture = Fixture::new().await;
    let auth = authenticated().await;
    let expected = Fixture::expectation(&fixture.owner);
    let file_id = "file-upstream-unavailable";
    CodexFiles::new(&fixture.storage)
        .create_success(
            &auth,
            &expected,
            file_id.to_owned(),
            "input_file".to_owned(),
            CREATED_AT,
            None,
        )
        .await
        .expect("create succeeds");
    sqlx::query("UPDATE upstream_spec_v1 SET enabled = 0 WHERE id = ?")
        .bind(fixture.owner.upstream_id.to_string())
        .execute(fixture.storage.pool())
        .await
        .expect("disable upstream");
    let unavailable = CodexFiles::new(&fixture.storage)
        .reference(&auth, &expected, "input_file", file_id)
        .await;
    assert!(matches!(unavailable, Err(StorageError::Unavailable { .. })));
}

#[test]
fn signed_url_metadata_rejects_credentials_and_redacts_debug() {
    let metadata = SignedUrlMetadata::parse(
        "https://files.example.test/upload/abc?X-Amz-Signature=secret-value",
    )
    .expect("valid signed URL");
    assert_eq!(metadata.origin(), "https://files.example.test");
    assert_eq!(
        metadata.url(),
        "https://files.example.test/upload/abc?X-Amz-Signature=secret-value"
    );
    let debug = format!("{metadata:?}");
    assert!(!debug.contains("secret-value"));
    for invalid in [
        "https://user:password@files.example.test/upload",
        "https://files.example.test/upload#fragment",
        "http://files.example.test/upload",
    ] {
        assert!(matches!(
            SignedUrlMetadata::parse(invalid),
            Err(StorageError::Conflict { .. })
        ));
    }
}
