use std::sync::Arc;

use cc_lb_clock::{ClockHandle, SystemClock};
use cc_lb_storage_api::{
    BUILTIN_SUBSCRIPTION_PREFERENCE_ID, BackendKind, MetaStore, PluginRegistryStore,
    PrincipalCreate, PrincipalKind, PrincipalStore, WasmBlob, WasmRegistryEntryInput,
};
use cc_lb_storage_sqlite::{SqliteStorage, open_sqlite};
use sqlx::SqlitePool;
use uuid::Uuid;

const MIGRATION: &str =
    include_str!("../migrations/0073_materialize_default_subscription_preference.sql");

#[tokio::test]
async fn migration_materializes_default_on_real_schema() {
    let directory = tempfile::tempdir().expect("create temporary directory");
    let database_url = format!(
        "sqlite://{}",
        directory.path().join("storage.sqlite").display()
    );
    let clock: ClockHandle = Arc::new(SystemClock);
    let storage = open_sqlite(&database_url, clock)
        .await
        .expect("open sqlite storage");
    storage
        .initialize(BackendKind::Sqlite)
        .await
        .expect("migrate sqlite storage");

    let empty = create_principal(&storage, "empty").await;
    let existing_builtin = create_principal(&storage, "builtin").await;
    let custom = create_principal(&storage, "custom").await;
    let custom_registry_id = persist_custom_registry(&storage).await;
    replace_router_registry(storage.pool(), custom, &custom_registry_id).await;
    let deleted_empty = create_principal(&storage, "deleted").await;
    remove_router_chain(storage.pool(), empty).await;
    remove_router_chain(storage.pool(), deleted_empty).await;
    sqlx::query("UPDATE principals_v1 SET deleted_at = 1 WHERE id = ?")
        .bind(deleted_empty.to_string())
        .execute(storage.pool())
        .await
        .expect("soft delete principal");

    sqlx::raw_sql(MIGRATION)
        .execute(storage.pool())
        .await
        .expect("apply migration against real schema");

    assert_eq!(builtin_order(storage.pool(), empty).await, Some(0));
    assert_eq!(builtin_count(storage.pool(), empty).await, 1);
    assert_eq!(
        builtin_order(storage.pool(), existing_builtin).await,
        Some(0)
    );
    assert_eq!(builtin_count(storage.pool(), existing_builtin).await, 1);
    assert_eq!(builtin_count(storage.pool(), custom).await, 0);
    assert_eq!(router_entry_count(storage.pool(), custom).await, 1);
    assert_eq!(builtin_order(storage.pool(), deleted_empty).await, None);
    assert_eq!(builtin_count(storage.pool(), deleted_empty).await, 0);
}

async fn create_principal(storage: &SqliteStorage, name: &str) -> Uuid {
    PrincipalStore::create(
        storage,
        PrincipalCreate {
            name: name.to_owned(),
            kind: PrincipalKind::Machine,
            allowed_models: Vec::new(),
            allowed_upstreams: Vec::new(),
            default_limits: Vec::new(),
            cache_keepalive: None,
        },
        1,
    )
    .await
    .expect("create principal")
    .id
}

async fn remove_router_chain(pool: &SqlitePool, principal_id: Uuid) {
    sqlx::query("DELETE FROM plugin_chains_v2 WHERE principal_id = ? AND slot = 'router'")
        .bind(principal_id.to_string())
        .execute(pool)
        .await
        .expect("remove router chain");
}

async fn persist_custom_registry(storage: &SqliteStorage) -> String {
    let (entry, _) = PluginRegistryStore::persist_wasm_upload(
        storage,
        WasmBlob {
            sha256: [0xA1; 32],
            bytes: vec![0],
            size_bytes: 1,
            parse_validated_at_unix_secs: 1,
        },
        WasmRegistryEntryInput {
            name: "custom-router-filter".to_owned(),
            version: None,
            original_filename: "custom-router-filter.wasm".to_owned(),
            label: None,
            uploaded_at_unix_secs: 1,
            uploaded_by_admin_id: Uuid::nil(),
            description: String::new(),
            usage: String::new(),
            hook_metadata: Default::default(),
            supported_slots: Vec::new(),
            schema_hash: None,
        },
    )
    .await
    .expect("persist custom registry");
    entry.id.to_string()
}

async fn replace_router_registry(pool: &SqlitePool, principal_id: Uuid, wasm_registry_id: &str) {
    sqlx::query(
        "UPDATE plugin_chains_v2
            SET wasm_registry_id = ?
          WHERE principal_id = ?
            AND slot = 'router'",
    )
    .bind(wasm_registry_id)
    .bind(principal_id.to_string())
    .execute(pool)
    .await
    .expect("replace router registry");
}

async fn builtin_order(pool: &SqlitePool, principal_id: Uuid) -> Option<i64> {
    sqlx::query_scalar(
        "SELECT order_value
           FROM plugin_chains_v2
          WHERE principal_id = ?
            AND slot = 'router'
            AND wasm_registry_id = ?",
    )
    .bind(principal_id.to_string())
    .bind(BUILTIN_SUBSCRIPTION_PREFERENCE_ID.to_string())
    .fetch_optional(pool)
    .await
    .expect("read built-in entry")
}

async fn builtin_count(pool: &SqlitePool, principal_id: Uuid) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*)
           FROM plugin_chains_v2
          WHERE principal_id = ?
            AND slot = 'router'
            AND wasm_registry_id = ?",
    )
    .bind(principal_id.to_string())
    .bind(BUILTIN_SUBSCRIPTION_PREFERENCE_ID.to_string())
    .fetch_one(pool)
    .await
    .expect("count built-in entries")
}

async fn router_entry_count(pool: &SqlitePool, principal_id: Uuid) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*)
           FROM plugin_chains_v2
          WHERE principal_id = ?
            AND slot = 'router'",
    )
    .bind(principal_id.to_string())
    .fetch_one(pool)
    .await
    .expect("count router entries")
}
