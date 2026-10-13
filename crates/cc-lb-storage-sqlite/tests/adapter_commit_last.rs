//! Pins that SQLite upstream and plugin-registry mutations decode their
//! returned record inside the mutation transaction, so COMMIT is the last
//! await: a decode failure rolls the mutation back instead of leaving a
//! committed change behind an error, and no pool read follows COMMIT.

use std::collections::BTreeMap;
use std::sync::Arc;

use cc_lb_aead::EncryptedOAuthTokens;
use cc_lb_storage_api::{
    MetaStore, PluginRegistryStore, StorageError, UpstreamCreate, UpstreamStore, UpstreamUpdate,
    WasmBlob, WasmRegistryEntryInput, upstream::UpstreamKind,
};
use cc_lb_storage_sqlite::SqliteStorage;
use uuid::Uuid;

async fn open(name: &str) -> (tempfile::TempDir, SqliteStorage) {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let database_url = format!("sqlite://{}", temp_dir.path().join(name).display());
    let storage =
        cc_lb_storage_sqlite::open_sqlite(&database_url, Arc::new(cc_lb_clock::SystemClock))
            .await
            .expect("open sqlite");
    storage.initialize().await.expect("initialize sqlite");
    (temp_dir, storage)
}

fn upstream(name: &str, kind: UpstreamKind) -> UpstreamCreate {
    let oauth = kind == UpstreamKind::AnthropicOauth;
    UpstreamCreate {
        id: Uuid::new_v4(),
        name: name.to_owned(),
        kind,
        base_url: None,
        api_key_ciphertext: (!oauth).then(|| vec![1, 2, 3]),
        oauth_tokens: None,
        warmup_enabled: false,
        warmup_dialect_plugin: None,
        workspace_id: None,
        api_key_access: None,
    }
}

fn tokens(byte: u8) -> EncryptedOAuthTokens {
    EncryptedOAuthTokens::from_ciphertext(vec![byte; 8])
}

#[derive(Debug, PartialEq, Eq)]
struct RawUpstream {
    name: String,
    enabled: i64,
    spec_revision: i64,
    api_key_ciphertext: Option<Vec<u8>>,
    oauth_credentials: Option<Vec<u8>>,
    oauth_token_generation: i64,
}

/// Reads the persisted upstream without the record decoder so corrupted rows
/// stay observable.
async fn raw_upstream(storage: &SqliteStorage, id: Uuid) -> RawUpstream {
    let (
        name,
        enabled,
        spec_revision,
        api_key_ciphertext,
        oauth_credentials,
        oauth_token_generation,
    ) = sqlx::query_as::<_, (String, i64, i64, Option<Vec<u8>>, Option<Vec<u8>>, i64)>(
        "SELECT spec.name, spec.enabled, spec.spec_revision, secret.api_key_ciphertext,
                    token.oauth_credentials_ciphertext, COALESCE(token.oauth_token_generation, 0)
               FROM upstream_spec_v1 spec
               LEFT JOIN upstream_api_key_secret_v1 secret ON secret.upstream_id = spec.id
               LEFT JOIN upstream_oauth_token_v1 token ON token.upstream_id = spec.id
              WHERE spec.id = ?",
    )
    .bind(id.to_string())
    .fetch_one(storage.pool())
    .await
    .expect("read raw upstream");
    RawUpstream {
        name,
        enabled,
        spec_revision,
        api_key_ciphertext,
        oauth_credentials,
        oauth_token_generation,
    }
}

/// Stores a base_url the record decoder rejects. No mutation under test
/// rewrites base_url, so only the returned-record decode can observe it.
async fn corrupt_base_url(storage: &SqliteStorage, id: Uuid) {
    sqlx::query("UPDATE upstream_spec_v1 SET base_url = 'not a url' WHERE id = ?")
        .bind(id.to_string())
        .execute(storage.pool())
        .await
        .expect("corrupt base_url");
}

fn assert_corrupted<T: std::fmt::Debug>(result: Result<T, StorageError>, operation: &str) {
    match result {
        Err(StorageError::Corrupted { .. }) => {}
        other => panic!("{operation} must surface the decode failure, got {other:?}"),
    }
}

#[tokio::test]
async fn upstream_mutations_return_the_committed_row() {
    let (_dir, storage) = open("commit-last-return.sqlite").await;

    let api = storage
        .create(upstream("commit-last-api", UpstreamKind::AnthropicApiKey))
        .await
        .expect("create api-key upstream");
    assert_eq!(
        Some(&api),
        storage.get_by_id(api.id).await.expect("read").as_ref()
    );

    let renamed = storage
        .update_spec(
            api.id,
            api.revision,
            UpstreamUpdate {
                name: Some("commit-last-api-renamed".to_owned()),
                ..UpstreamUpdate::default()
            },
        )
        .await
        .expect("update_spec");
    assert_eq!(
        Some(&renamed),
        storage.get_by_id(api.id).await.expect("read").as_ref()
    );

    let updated = storage
        .update(
            api.id,
            renamed.revision,
            UpstreamUpdate {
                api_key_ciphertext: Some(vec![4, 5, 6]),
                ..UpstreamUpdate::default()
            },
        )
        .await
        .expect("update");
    assert_eq!(
        Some(&updated),
        storage.get_by_id(api.id).await.expect("read").as_ref()
    );

    let disabled = storage
        .set_enabled(api.id, updated.revision, false)
        .await
        .expect("set_enabled");
    assert_eq!(
        Some(&disabled),
        storage.get_by_id(api.id).await.expect("read").as_ref()
    );

    let rekeyed = storage
        .update_api_key_secret(api.id, Some(vec![7, 8, 9]))
        .await
        .expect("update_api_key_secret");
    assert_eq!(
        Some(&rekeyed),
        storage.get_by_id(api.id).await.expect("read").as_ref()
    );

    let cleared = storage
        .clear_warmup_dialect_plugin(api.id, rekeyed.revision)
        .await
        .expect("clear_warmup_dialect_plugin")
        .expect("matching revision clears");
    assert_eq!(
        Some(&cleared),
        storage.get_by_id(api.id).await.expect("read").as_ref()
    );
    assert_eq!(
        None,
        storage
            .clear_warmup_dialect_plugin(api.id, rekeyed.revision)
            .await
            .expect("stale clear keeps None semantics")
    );

    let oauth = storage
        .create(upstream("commit-last-oauth", UpstreamKind::AnthropicOauth))
        .await
        .expect("create oauth upstream");
    let stored = storage
        .store_oauth_tokens(oauth.id, oauth.revision, tokens(1), false)
        .await
        .expect("store_oauth_tokens");
    assert_eq!(
        Some(&stored),
        storage.get_by_id(oauth.id).await.expect("read").as_ref()
    );

    let rotated = storage
        .update_oauth_token(oauth.id, tokens(2))
        .await
        .expect("update_oauth_token");
    assert_eq!(
        Some(&rotated),
        storage.get_by_id(oauth.id).await.expect("read").as_ref()
    );

    let refreshed = storage
        .complete_refresh(oauth.id, Uuid::new_v4(), tokens(3))
        .await
        .expect("complete_refresh");
    assert_eq!(
        Some(&refreshed),
        storage.get_by_id(oauth.id).await.expect("read").as_ref()
    );
    assert_eq!(
        refreshed.oauth_token_generation,
        rotated.oauth_token_generation + 1
    );
}

#[tokio::test]
async fn upstream_create_rolls_back_when_the_returned_record_cannot_decode() {
    let (_dir, storage) = open("commit-last-create.sqlite").await;
    // The trigger runs inside the create transaction, so the in-transaction
    // reload decodes the corrupted row before COMMIT.
    sqlx::query(
        "CREATE TRIGGER corrupt_new_upstream AFTER INSERT ON upstream_spec_v1
         BEGIN UPDATE upstream_spec_v1 SET base_url = 'not a url' WHERE id = NEW.id; END",
    )
    .execute(storage.pool())
    .await
    .expect("install corrupting trigger");

    let create = upstream("commit-last-create", UpstreamKind::AnthropicApiKey);
    let id = create.id;
    assert_corrupted(storage.create(create).await, "create");

    let persisted: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM upstream_spec_v1 WHERE id = ?")
        .bind(id.to_string())
        .fetch_one(storage.pool())
        .await
        .expect("count upstream rows");
    assert_eq!(persisted, 0, "failed create must not commit the upstream");
    let secrets: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM upstream_api_key_secret_v1 WHERE upstream_id = ?")
            .bind(id.to_string())
            .fetch_one(storage.pool())
            .await
            .expect("count secret rows");
    assert_eq!(secrets, 0, "failed create must not commit the credential");
}

#[tokio::test]
async fn upstream_mutations_roll_back_when_the_returned_record_cannot_decode() {
    let (_dir, storage) = open("commit-last-rollback.sqlite").await;
    let api = storage
        .create(upstream("commit-last-api", UpstreamKind::AnthropicApiKey))
        .await
        .expect("create api-key upstream");
    let oauth = storage
        .create(upstream("commit-last-oauth", UpstreamKind::AnthropicOauth))
        .await
        .expect("create oauth upstream");
    corrupt_base_url(&storage, api.id).await;
    corrupt_base_url(&storage, oauth.id).await;
    let api_before = raw_upstream(&storage, api.id).await;
    let oauth_before = raw_upstream(&storage, oauth.id).await;

    assert_corrupted(
        storage
            .update_spec(
                api.id,
                api.revision,
                UpstreamUpdate {
                    name: Some("commit-last-renamed".to_owned()),
                    ..UpstreamUpdate::default()
                },
            )
            .await,
        "update_spec",
    );
    assert_eq!(
        raw_upstream(&storage, api.id).await,
        api_before,
        "update_spec"
    );

    assert_corrupted(
        storage
            .update(
                api.id,
                api.revision,
                UpstreamUpdate {
                    name: Some("commit-last-renamed".to_owned()),
                    api_key_ciphertext: Some(vec![9, 9, 9]),
                    ..UpstreamUpdate::default()
                },
            )
            .await,
        "update",
    );
    assert_eq!(raw_upstream(&storage, api.id).await, api_before, "update");

    assert_corrupted(
        storage.set_enabled(api.id, api.revision, false).await,
        "set_enabled",
    );
    assert_eq!(
        raw_upstream(&storage, api.id).await,
        api_before,
        "set_enabled"
    );

    assert_corrupted(
        storage
            .update_api_key_secret(api.id, Some(vec![8, 8, 8]))
            .await,
        "update_api_key_secret",
    );
    assert_eq!(
        raw_upstream(&storage, api.id).await,
        api_before,
        "update_api_key_secret"
    );

    assert_corrupted(
        storage
            .clear_warmup_dialect_plugin(api.id, api.revision)
            .await,
        "clear_warmup_dialect_plugin",
    );
    assert_eq!(
        raw_upstream(&storage, api.id).await,
        api_before,
        "clear_warmup_dialect_plugin"
    );

    assert_corrupted(
        storage
            .store_oauth_tokens(oauth.id, oauth.revision, tokens(1), false)
            .await,
        "store_oauth_tokens",
    );
    assert_eq!(
        raw_upstream(&storage, oauth.id).await,
        oauth_before,
        "store_oauth_tokens"
    );

    assert_corrupted(
        storage.update_oauth_token(oauth.id, tokens(2)).await,
        "update_oauth_token",
    );
    assert_eq!(
        raw_upstream(&storage, oauth.id).await,
        oauth_before,
        "update_oauth_token"
    );

    assert_corrupted(
        storage
            .complete_refresh(oauth.id, Uuid::new_v4(), tokens(3))
            .await,
        "complete_refresh",
    );
    assert_eq!(
        raw_upstream(&storage, oauth.id).await,
        oauth_before,
        "complete_refresh"
    );
}

async fn persist_registry_entry(storage: &SqliteStorage) -> cc_lb_storage_api::WasmRegistryEntry {
    let bytes = vec![0, 97, 115, 109];
    let (entry, _) = storage
        .persist_wasm_upload(
            WasmBlob {
                sha256: [42; 32],
                size_bytes: bytes.len() as u64,
                bytes,
            },
            WasmRegistryEntryInput {
                name: "commit-last-plugin".to_owned(),
                version: None,
                original_filename: "commit-last-plugin.wasm".to_owned(),
                label: Some("before".to_owned()),
                uploaded_at_unix_secs: 1_800_000_100,
                uploaded_by_admin_id: Uuid::new_v4(),
                description: "description".to_owned(),
                usage: "usage".to_owned(),
                hook_metadata: BTreeMap::new(),
                supported_slots: Vec::new(),
            },
        )
        .await
        .expect("persist wasm upload");
    entry
}

async fn raw_registry_label(storage: &SqliteStorage, id: Uuid) -> (Option<String>, i64) {
    sqlx::query_as("SELECT label, revision FROM wasm_registry_v2 WHERE id = ?")
        .bind(id.to_string())
        .fetch_one(storage.pool())
        .await
        .expect("read raw registry row")
}

#[tokio::test]
async fn registry_label_update_returns_the_committed_row() {
    let (_dir, storage) = open("commit-last-registry-return.sqlite").await;
    let entry = persist_registry_entry(&storage).await;

    let updated = storage
        .update_registry_label(entry.id, entry.revision, Some("after".to_owned()))
        .await
        .expect("update_registry_label");
    assert_eq!(updated.label.as_deref(), Some("after"));
    assert_eq!(updated.revision, entry.revision + 1);
    assert_eq!(
        Some(&updated),
        storage
            .get_registry_entry_by_id(entry.id)
            .await
            .expect("read registry entry")
            .as_ref()
    );
}

#[tokio::test]
async fn registry_label_update_rolls_back_when_the_returned_entry_cannot_decode() {
    let (_dir, storage) = open("commit-last-registry-rollback.sqlite").await;
    let entry = persist_registry_entry(&storage).await;
    sqlx::query("UPDATE wasm_registry_v2 SET uploaded_by_admin_id = 'not-a-uuid' WHERE id = ?")
        .bind(entry.id.to_string())
        .execute(storage.pool())
        .await
        .expect("corrupt uploaded_by_admin_id");
    let before = raw_registry_label(&storage, entry.id).await;

    assert_corrupted(
        storage
            .update_registry_label(entry.id, entry.revision, Some("after".to_owned()))
            .await,
        "update_registry_label",
    );
    assert_eq!(
        raw_registry_label(&storage, entry.id).await,
        before,
        "failed label update must not commit"
    );
}
