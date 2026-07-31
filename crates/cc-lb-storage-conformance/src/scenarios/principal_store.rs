use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{Result, ensure};
use cc_lb_plugin_wire::metadata::HookMetadata;
use cc_lb_storage_api::{
    AuditStore, PluginChainEntryInput, PluginRegistryStore, PluginSlotKind, PrincipalStore,
    StorageError, WasmBlob, WasmRegistryEntryInput, default_wire_version,
    principal::{
        Limit, LimitKind, PrincipalCreate, PrincipalKind, PrincipalRecord, PrincipalUpdate,
    },
    types::AuditEntry,
    validate_identifier,
};
use serde_json::json;
use uuid::Uuid;

use crate::harness::{ConformanceBackend, ConformanceFixture};

const BASE_TS: u64 = 1_900_000_000;

pub async fn run_all<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: AuditStore + PrincipalStore + PluginRegistryStore,
{
    create_persists_defaults(Arc::clone(&backend)).await?;
    get_by_id_returns_created(Arc::clone(&backend)).await?;
    get_by_name_returns_created(Arc::clone(&backend)).await?;
    list_paginates_by_name(Arc::clone(&backend)).await?;
    update_with_current_revision_succeeds(Arc::clone(&backend)).await?;
    stale_revision_conflict(Arc::clone(&backend)).await?;
    set_enabled_toggles(Arc::clone(&backend)).await?;
    allowed_models_persist_raw(Arc::clone(&backend)).await?;
    principal_allowed_upstreams_roundtrip(Arc::clone(&backend)).await?;
    default_limits_roundtrip(Arc::clone(&backend)).await?;
    router_terminal_strategy_roundtrip(Arc::clone(&backend)).await?;
    subscription_preference_is_seeded_as_router_entry(Arc::clone(&backend)).await?;
    soft_delete_excludes_default_list(Arc::clone(&backend)).await?;
    soft_delete_cascades_plugin_chains(Arc::clone(&backend)).await?;
    hard_delete_removes_unreferenced(Arc::clone(&backend)).await?;
    hard_delete_cascades_plugin_chains(Arc::clone(&backend)).await?;
    hard_delete_referenced_by_audit_conflicts(Arc::clone(&backend)).await?;
    validate_identifier_rejects_invalid_name(Arc::clone(&backend)).await?;
    set_last_apply_error_roundtrip(backend).await?;
    Ok(())
}

async fn create_persists_defaults<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: AuditStore + PrincipalStore,
{
    with_fixture(backend, |storage| async move {
        let record = PrincipalStore::create(&*storage, principal_create(1), BASE_TS).await?;
        ensure!(record.name == "principal-0001");
        ensure!(record.kind == PrincipalKind::Machine);
        ensure!(record.enabled);
        ensure!(record.revision == 0);
        ensure!(record.created_at_unix_secs == BASE_TS);
        Ok(())
    })
    .await
}

async fn get_by_id_returns_created<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: AuditStore + PrincipalStore,
{
    with_fixture(backend, |storage| async move {
        let record = PrincipalStore::create(&*storage, principal_create(2), BASE_TS).await?;
        let fetched = PrincipalStore::get_by_id(&*storage, record.id).await?;
        ensure!(fetched == Some(record));
        Ok(())
    })
    .await
}

async fn get_by_name_returns_created<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: AuditStore + PrincipalStore,
{
    with_fixture(backend, |storage| async move {
        let record = PrincipalStore::create(&*storage, principal_create(3), BASE_TS).await?;
        let fetched = PrincipalStore::get_by_name(&*storage, &record.name).await?;
        ensure!(fetched == Some(record));
        Ok(())
    })
    .await
}

async fn list_paginates_by_name<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: AuditStore + PrincipalStore,
{
    with_fixture(backend, |storage| async move {
        for index in [3, 1, 2] {
            PrincipalStore::create(&*storage, principal_create(index), BASE_TS + index as u64)
                .await?;
        }
        let page = PrincipalStore::list(&*storage, 1, 1, false).await?;
        ensure!(names(&page) == ["principal-0002"]);
        Ok(())
    })
    .await
}

async fn update_with_current_revision_succeeds<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: AuditStore + PrincipalStore,
{
    with_fixture(backend, |storage| async move {
        let record = PrincipalStore::create(&*storage, principal_create(4), BASE_TS).await?;
        let updated = PrincipalStore::update(
            &*storage,
            record.id,
            record.revision,
            PrincipalUpdate {
                name: Some("renamed-principal".to_owned()),
                allowed_models: None,
                allowed_upstreams: None,
                default_limits: None,
                router_terminal_strategy: None,
                cache_keepalive: None,
            },
            BASE_TS + 1,
        )
        .await?
        .expect("record should exist");
        ensure!(updated.name == "renamed-principal");
        ensure!(updated.revision == 1);
        ensure!(updated.updated_at_unix_secs == BASE_TS + 1);
        ensure!(
            PrincipalStore::get_by_name(&*storage, "principal-0004")
                .await?
                .is_none()
        );
        ensure!(
            PrincipalStore::get_by_name(&*storage, "renamed-principal")
                .await?
                .is_some()
        );
        Ok(())
    })
    .await
}

pub async fn stale_revision_conflict<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: AuditStore + PrincipalStore,
{
    with_fixture(backend, |storage| async move {
        let record = PrincipalStore::create(&*storage, principal_create(5), BASE_TS).await?;
        let error = PrincipalStore::set_enabled(
            &*storage,
            record.id,
            record.revision + 1,
            false,
            BASE_TS + 1,
        )
        .await
        .expect_err("stale revision must conflict");
        ensure!(matches!(error, StorageError::Conflict { .. }));
        Ok(())
    })
    .await
}

async fn set_enabled_toggles<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: AuditStore + PrincipalStore,
{
    with_fixture(backend, |storage| async move {
        let record = PrincipalStore::create(&*storage, principal_create(6), BASE_TS).await?;
        let disabled = PrincipalStore::set_enabled(&*storage, record.id, 0, false, BASE_TS + 1)
            .await?
            .expect("record should exist");
        ensure!(!disabled.enabled);
        ensure!(disabled.revision == 1);
        Ok(())
    })
    .await
}

async fn allowed_models_persist_raw<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: AuditStore + PrincipalStore,
{
    with_fixture(backend, |storage| async move {
        let mut input = principal_create(7);
        input.allowed_models = vec!["claude-*".to_owned(), "custom/model".to_owned()];
        let record = PrincipalStore::create(&*storage, input, BASE_TS).await?;
        let fetched = PrincipalStore::get_by_id(&*storage, record.id)
            .await?
            .unwrap();
        ensure!(fetched.allowed_models == ["claude-*", "custom/model"]);
        Ok(())
    })
    .await
}

pub async fn principal_allowed_upstreams_roundtrip<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: AuditStore + PrincipalStore,
{
    with_fixture(backend, |storage| async move {
        let upstream_a = Uuid::from_u128(0x0000000000000000000000000000000a);
        let upstream_b = Uuid::from_u128(0x0000000000000000000000000000000b);
        let upstream_c = Uuid::from_u128(0x0000000000000000000000000000000c);

        let mut input = principal_create(14);
        input.allowed_upstreams = vec![upstream_a, upstream_b];
        let record = PrincipalStore::create(&*storage, input, BASE_TS).await?;
        ensure!(record.allowed_upstreams == [upstream_a, upstream_b]);

        let fetched = PrincipalStore::get_by_id(&*storage, record.id)
            .await?
            .expect("record should exist");
        ensure!(fetched.allowed_upstreams == [upstream_a, upstream_b]);

        let updated = PrincipalStore::update(
            &*storage,
            record.id,
            record.revision,
            PrincipalUpdate {
                allowed_upstreams: Some(vec![upstream_c]),
                ..PrincipalUpdate::default()
            },
            BASE_TS + 1,
        )
        .await?
        .expect("record should exist");
        ensure!(updated.allowed_upstreams == [upstream_c]);

        let fetched = PrincipalStore::get_by_id(&*storage, record.id)
            .await?
            .expect("record should exist");
        ensure!(fetched.allowed_upstreams == [upstream_c]);
        Ok(())
    })
    .await
}

async fn default_limits_roundtrip<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: AuditStore + PrincipalStore,
{
    with_fixture(backend, |storage| async move {
        let record = PrincipalStore::create(&*storage, principal_create(8), BASE_TS).await?;
        let fetched = PrincipalStore::get_by_id(&*storage, record.id)
            .await?
            .unwrap();
        ensure!(fetched.default_limits == limits());
        Ok(())
    })
    .await
}

pub async fn router_terminal_strategy_roundtrip<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: AuditStore + PrincipalStore,
{
    with_fixture(backend, |storage| async move {
        let record = PrincipalStore::create(&*storage, principal_create(15), BASE_TS).await?;
        ensure!(
            serde_json::to_value(&record.router_terminal_strategy)? == json!("first-pick"),
            "default terminal strategy is first-pick"
        );

        let random_strategy = serde_json::from_value(json!("random"))?;
        let updated = PrincipalStore::update(
            &*storage,
            record.id,
            record.revision,
            PrincipalUpdate {
                router_terminal_strategy: Some(random_strategy),
                ..PrincipalUpdate::default()
            },
            BASE_TS + 1,
        )
        .await?
        .expect("record should exist");
        ensure!(
            serde_json::to_value(&updated.router_terminal_strategy)? == json!("random"),
            "updated terminal strategy is random"
        );

        let fetched = PrincipalStore::get_by_id(&*storage, record.id)
            .await?
            .expect("record should exist");
        ensure!(
            serde_json::to_value(&fetched.router_terminal_strategy)? == json!("random"),
            "terminal strategy persists"
        );
        Ok(())
    })
    .await
}

pub async fn subscription_preference_is_seeded_as_router_entry<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: AuditStore + PrincipalStore + PluginRegistryStore,
{
    with_fixture(backend, |storage| async move {
        let record = PrincipalStore::create(&*storage, principal_create(16), BASE_TS).await?;
        let entries = PluginRegistryStore::list_chain_for_principal(
            &*storage,
            record.id,
            PluginSlotKind::Router,
        )
        .await?;
        ensure!(
            entries.len() == 1,
            "new principals must receive exactly one subscription-preference router entry"
        );
        ensure!(
            entries[0].wasm_registry_id == cc_lb_storage_api::BUILTIN_SUBSCRIPTION_PREFERENCE_ID,
            "the seeded router entry must be subscription-preference"
        );
        ensure!(
            entries[0].order == 0,
            "the seeded router entry uses the reserved zero order"
        );
        Ok(())
    })
    .await
}

async fn soft_delete_excludes_default_list<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: AuditStore + PrincipalStore,
{
    with_fixture(backend, |storage| async move {
        let record = PrincipalStore::create(&*storage, principal_create(9), BASE_TS).await?;
        let deleted = PrincipalStore::soft_delete(&*storage, record.id, 0, BASE_TS + 2)
            .await?
            .unwrap();
        ensure!(deleted.deleted_at_unix_secs == Some(BASE_TS + 2));
        ensure!(
            PrincipalStore::list(&*storage, 0, 10, false)
                .await?
                .is_empty()
        );
        ensure!(PrincipalStore::list(&*storage, 0, 10, true).await?.len() == 1);
        Ok(())
    })
    .await
}

async fn soft_delete_cascades_plugin_chains<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: AuditStore + PrincipalStore + PluginRegistryStore,
{
    with_fixture(backend, |storage| async move {
        let (principal, plugin_id) = principal_with_plugin(storage.as_ref(), 16).await?;
        insert_all_slots(storage.as_ref(), principal.id, plugin_id).await?;

        PrincipalStore::soft_delete(
            storage.as_ref(),
            principal.id,
            principal.revision,
            BASE_TS + 20,
        )
        .await?
        .expect("record should soft delete");

        ensure_all_slots_empty(storage.as_ref(), principal.id).await?;
        Ok(())
    })
    .await
}

async fn hard_delete_removes_unreferenced<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: AuditStore + PrincipalStore,
{
    with_fixture(backend, |storage| async move {
        let record = PrincipalStore::create(&*storage, principal_create(10), BASE_TS).await?;
        ensure!(PrincipalStore::hard_delete(&*storage, record.id).await?);
        ensure!(
            PrincipalStore::get_by_id(&*storage, record.id)
                .await?
                .is_none()
        );
        ensure!(!PrincipalStore::hard_delete(&*storage, record.id).await?);
        Ok(())
    })
    .await
}

async fn hard_delete_cascades_plugin_chains<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: AuditStore + PrincipalStore + PluginRegistryStore,
{
    with_fixture(backend, |storage| async move {
        let (principal, plugin_id) = principal_with_plugin(storage.as_ref(), 17).await?;
        insert_all_slots(storage.as_ref(), principal.id, plugin_id).await?;

        ensure!(PrincipalStore::hard_delete(storage.as_ref(), principal.id).await?);

        ensure_all_slots_empty(storage.as_ref(), principal.id).await?;
        Ok(())
    })
    .await
}

async fn hard_delete_referenced_by_audit_conflicts<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: AuditStore + PrincipalStore,
{
    with_fixture(backend, |storage| async move {
        let record = PrincipalStore::create(&*storage, principal_create(11), BASE_TS).await?;
        AuditStore::append_audit(&*storage, &audit_entry(&record)).await?;
        let error = PrincipalStore::hard_delete(&*storage, record.id)
            .await
            .expect_err("referenced principal must not be hard deleted");
        ensure!(matches!(error, StorageError::Conflict { .. }));
        ensure!(
            PrincipalStore::get_by_id(&*storage, record.id)
                .await?
                .is_some()
        );
        Ok(())
    })
    .await
}

async fn validate_identifier_rejects_invalid_name<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: AuditStore + PrincipalStore,
{
    with_fixture(backend, |storage| async move {
        let mut input = principal_create(12);
        input.name = "system.invalid".to_owned();
        let error = PrincipalStore::create(&*storage, input, BASE_TS)
            .await
            .expect_err("reserved principal name must be rejected");
        ensure!(matches!(error, StorageError::InvalidInput { .. }));
        ensure!(validate_identifier("principal.name", "valid-principal").is_ok());
        Ok(())
    })
    .await
}

async fn set_last_apply_error_roundtrip<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: AuditStore + PrincipalStore,
{
    with_fixture(backend, |storage| async move {
        let record = PrincipalStore::create(&*storage, principal_create(13), BASE_TS).await?;
        let errored = PrincipalStore::set_last_apply_error(
            &*storage,
            record.id,
            Some("router compile failed".to_owned()),
            BASE_TS + 3,
        )
        .await?
        .unwrap();
        ensure!(errored.last_apply_error == Some("router compile failed".to_owned()));
        ensure!(errored.last_apply_at_unix_secs == Some(BASE_TS + 3));
        ensure!(errored.revision == record.revision);
        let cleared = PrincipalStore::set_last_apply_error(&*storage, record.id, None, BASE_TS + 4)
            .await?
            .unwrap();
        ensure!(cleared.last_apply_error.is_none());
        ensure!(cleared.last_apply_at_unix_secs == Some(BASE_TS + 4));
        ensure!(cleared.revision == record.revision);
        Ok(())
    })
    .await
}

async fn with_fixture<B, F, Fut>(backend: Arc<B>, run: F) -> Result<()>
where
    B: ConformanceBackend,
    F: FnOnce(Arc<B::Storage>) -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    let mut fixture = ConformanceFixture::new(backend).await?;
    let result = run(fixture.storage()).await;
    let teardown = fixture.teardown().await;
    result?;
    teardown
}

fn principal_create(index: usize) -> PrincipalCreate {
    PrincipalCreate {
        name: format!("principal-{index:04}"),
        kind: PrincipalKind::Machine,
        allowed_models: vec!["claude-sonnet-*".to_owned()],
        allowed_upstreams: vec![],
        default_limits: limits(),
        cache_keepalive: None,
    }
}

fn limits() -> Vec<Limit> {
    vec![Limit {
        kind: LimitKind::Requests,
        window_secs: 60,
        cap_micros: 100,
    }]
}

fn names(records: &[PrincipalRecord]) -> Vec<&str> {
    records.iter().map(|record| record.name.as_str()).collect()
}

fn audit_entry(record: &PrincipalRecord) -> AuditEntry {
    AuditEntry {
        ts: BASE_TS + 10,
        request_id: format!("request-{}", record.id),
        principal_id: record.id.to_string(),
        route: "messages".to_owned(),
        upstream: "primary".to_owned(),
        model: Some("claude-sonnet-4-5".to_owned()),
        status: 200,
        input_tokens: Some(1),
        output_tokens: Some(2),
        duration_ms: 3,
        agent_label: None,
        kind: Some("principal_store".to_owned()),
        payload: None,
        ..Default::default()
    }
}

async fn principal_with_plugin<S>(storage: &S, seed: u8) -> Result<(PrincipalRecord, Uuid)>
where
    S: PrincipalStore + PluginRegistryStore,
{
    let principal =
        PrincipalStore::create(storage, principal_create(seed as usize), BASE_TS).await?;
    let (plugin, _) = storage
        .persist_wasm_upload(
            WasmBlob {
                sha256: [seed; 32],
                bytes: vec![seed],
                size_bytes: 1,
                parse_validated_at_unix_secs: BASE_TS,
            },
            WasmRegistryEntryInput {
                schema_hash: None,
                name: format!("principal-cascade-plugin-{seed}"),
                version: None,
                original_filename: format!("principal-cascade-plugin-{seed}.wasm"),
                label: None,
                uploaded_at_unix_secs: BASE_TS,
                uploaded_by_admin_id: principal.id,
                description: format!("principal cascade plugin {seed}"),
                usage: "test fixture".to_owned(),
                hook_metadata: filter_hook_metadata(),
                supported_slots: Vec::new(),
            },
        )
        .await?;
    Ok((principal, plugin.id))
}

async fn insert_all_slots<S>(storage: &S, principal_id: Uuid, plugin_id: Uuid) -> Result<()>
where
    S: PluginRegistryStore,
{
    for (slot, order) in [
        (PluginSlotKind::Router, 100),
        (PluginSlotKind::ObservabilityHook, 200),
        (PluginSlotKind::Shape, 300),
    ] {
        storage
            .insert_chain_entry(PluginChainEntryInput {
                principal_id,
                slot,
                order,
                wasm_registry_id: plugin_id,
                config: json!({}),
                sse_per_event: false,
                batched_events_per_flush: 1,
                batched_flush_ms: 100,
            })
            .await?;
    }
    Ok(())
}

fn filter_hook_metadata() -> BTreeMap<String, HookMetadata> {
    BTreeMap::from([(
        "filter".to_owned(),
        HookMetadata {
            wire_version: default_wire_version(),
            description: "filter hook".to_owned(),
            usage: "called by router".to_owned(),
            mode: Default::default(),
        },
    )])
}

async fn ensure_all_slots_empty<S>(storage: &S, principal_id: Uuid) -> Result<()>
where
    S: PluginRegistryStore,
{
    for slot in [
        PluginSlotKind::Router,
        PluginSlotKind::ObservabilityHook,
        PluginSlotKind::Shape,
    ] {
        ensure!(
            storage
                .list_chain_for_principal(principal_id, slot)
                .await?
                .is_empty(),
            "principal-owned {slot:?} chain entries cascade"
        );
    }
    Ok(())
}
