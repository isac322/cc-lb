use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{Result, ensure};
use cc_lb_plugin_wire::metadata::HookMetadata;
use cc_lb_storage_api::{
    AuditEntry, AuditStore, CacheKeepaliveConfigSnapshot, CacheKeepaliveReplaceRequest,
    CacheKeepaliveSessionStatus, CacheKeepaliveSessionStore, CacheKeepaliveTerminalReason,
    CacheTtl, PluginChainEntryInput, PluginRegistryStore, PluginSlotKind, PrincipalStore,
    PromptCacheMode, StorageError, WasmBlob, WasmRegistryEntryInput, default_wire_version,
    principal::{
        Limit, LimitKind, PrincipalCreate, PrincipalKind, PrincipalRecord, PrincipalUpdate,
    },
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
    recreate_same_name_after_soft_delete_creates_new_principal(Arc::clone(&backend)).await?;
    soft_delete_cascades_plugin_chains(Arc::clone(&backend)).await?;
    hard_delete_removes_unreferenced(Arc::clone(&backend)).await?;
    hard_delete_cascades_plugin_chains(Arc::clone(&backend)).await?;
    hard_delete_referenced_by_audit_conflicts(Arc::clone(&backend)).await?;
    validate_identifier_rejects_invalid_name(backend).await?;
    Ok(())
}

/// The stored mode is non-null with a `preserve_client` default, survives a
/// round trip through both adapters, and is only replaced when the update
/// carries it.
pub async fn prompt_cache_mode_roundtrip_and_omission<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: AuditStore + PrincipalStore,
{
    with_fixture(backend, |storage| async move {
        let record = PrincipalStore::create(&*storage, principal_create(21), BASE_TS).await?;
        ensure!(
            record.prompt_cache_mode == PromptCacheMode::PreserveClient,
            "an omitted mode defaults to preserve_client"
        );
        let fetched = PrincipalStore::get_by_id(&*storage, record.id)
            .await?
            .expect("record exists");
        ensure!(fetched.prompt_cache_mode == PromptCacheMode::PreserveClient);

        let enabled = PrincipalStore::update(
            &*storage,
            record.id,
            record.revision,
            PrincipalUpdate {
                prompt_cache_mode: Some(PromptCacheMode::OneHourNoKeepalive),
                ..PrincipalUpdate::default()
            },
            BASE_TS + 1,
        )
        .await?
        .expect("record should exist");
        ensure!(enabled.prompt_cache_mode == PromptCacheMode::OneHourNoKeepalive);
        ensure!(
            PrincipalStore::list(&*storage, 0, 10, false)
                .await?
                .iter()
                .find(|list_record| list_record.id == record.id)
                .is_some_and(|list_record| {
                    list_record.prompt_cache_mode == PromptCacheMode::OneHourNoKeepalive
                }),
            "list carries the stored mode"
        );

        // An unrelated update must not clear the mode back to the default.
        let unrelated = PrincipalStore::update(
            &*storage,
            record.id,
            enabled.revision,
            PrincipalUpdate {
                allowed_models: Some(vec!["claude-opus-*".to_owned()]),
                ..PrincipalUpdate::default()
            },
            BASE_TS + 2,
        )
        .await?
        .expect("record should exist");
        ensure!(
            unrelated.prompt_cache_mode == PromptCacheMode::OneHourNoKeepalive,
            "omitting the mode preserves it"
        );

        let cleared = PrincipalStore::update(
            &*storage,
            record.id,
            unrelated.revision,
            PrincipalUpdate {
                prompt_cache_mode: Some(PromptCacheMode::PreserveClient),
                ..PrincipalUpdate::default()
            },
            BASE_TS + 3,
        )
        .await?
        .expect("record should exist");
        ensure!(cleared.prompt_cache_mode == PromptCacheMode::PreserveClient);
        Ok(())
    })
    .await
}

/// A stale revision is rejected for a mode-only update exactly like any other
/// principal field.
pub async fn prompt_cache_mode_stale_revision_conflict<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: AuditStore + PrincipalStore,
{
    with_fixture(backend, |storage| async move {
        let record = PrincipalStore::create(&*storage, principal_create(22), BASE_TS).await?;
        let updated = PrincipalStore::update(
            &*storage,
            record.id,
            record.revision,
            PrincipalUpdate {
                prompt_cache_mode: Some(PromptCacheMode::OneHourNoKeepalive),
                ..PrincipalUpdate::default()
            },
            BASE_TS + 1,
        )
        .await?
        .expect("record should exist");

        let stale = PrincipalStore::update(
            &*storage,
            record.id,
            record.revision,
            PrincipalUpdate {
                prompt_cache_mode: Some(PromptCacheMode::PreserveClient),
                ..PrincipalUpdate::default()
            },
            BASE_TS + 2,
        )
        .await;
        ensure!(
            matches!(stale, Err(StorageError::Conflict { .. })),
            "a stale revision must conflict"
        );
        let fetched = PrincipalStore::get_by_id(&*storage, record.id)
            .await?
            .expect("record exists");
        ensure!(
            fetched.prompt_cache_mode == PromptCacheMode::OneHourNoKeepalive,
            "the rejected write changed nothing"
        );
        ensure!(fetched.revision == updated.revision);
        Ok(())
    })
    .await
}

/// Entering one-hour mode retires the principal's active keep-alive sessions in
/// the same transaction and fences the durable enqueue boundary, so a request
/// admitted under the previous mode cannot recreate a session afterwards.
pub async fn prompt_cache_mode_cancels_active_keepalive_sessions<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: AuditStore + PrincipalStore + CacheKeepaliveSessionStore,
{
    with_fixture(backend, |storage| async move {
        let record = PrincipalStore::create(&*storage, principal_create(23), BASE_TS).await?;
        // Sessions are keyed by the principal id the caller authenticated as,
        // which is either the UUID or the name. Cover both identification
        // forms so the fence cannot miss a session.
        let uuid_session_key = format!("session-{}-uuid", record.id);
        let name_session_key = format!("session-{}-name", record.id);
        storage
            .replace_from_real_request(&keepalive_replace_request(
                &record.id.to_string(),
                &uuid_session_key,
            ))
            .await?;
        storage
            .replace_from_real_request(&keepalive_replace_request(&record.name, &name_session_key))
            .await?;
        for session_key_hash in [&uuid_session_key, &name_session_key] {
            let active = storage
                .get_cache_keepalive_session(session_key_hash)
                .await?
                .expect("session exists");
            ensure!(active.status == CacheKeepaliveSessionStatus::Active);
        }

        let updated = PrincipalStore::update(
            &*storage,
            record.id,
            record.revision,
            PrincipalUpdate {
                prompt_cache_mode: Some(PromptCacheMode::OneHourNoKeepalive),
                ..PrincipalUpdate::default()
            },
            BASE_TS + 1,
        )
        .await?
        .expect("record should exist");
        ensure!(updated.prompt_cache_mode == PromptCacheMode::OneHourNoKeepalive);

        for session_key_hash in [&uuid_session_key, &name_session_key] {
            let fenced = storage
                .get_cache_keepalive_session(session_key_hash)
                .await?
                .expect("session row remains for history");
            ensure!(
                fenced.status == CacheKeepaliveSessionStatus::Terminal,
                "the mode write retires the active session"
            );
            ensure!(
                fenced.terminal_reason == Some(CacheKeepaliveTerminalReason::Cancelled),
                "the retired session records the cancellation reason"
            );
            ensure!(
                fenced.encrypted_payload.is_empty(),
                "the retired session drops its payload"
            );
        }
        let fenced = storage
            .get_cache_keepalive_session(&uuid_session_key)
            .await?
            .expect("session row remains for history");

        let refused = storage
            .replace_from_real_request(&keepalive_replace_request(
                &record.id.to_string(),
                &uuid_session_key,
            ))
            .await;
        ensure!(
            matches!(
                refused,
                Err(StorageError::InvalidInput { ref field, .. })
                    if field == "principal.prompt_cache_mode"
            ),
            "one-hour mode fences the durable enqueue boundary: {refused:?}"
        );
        let still_terminal = storage
            .get_cache_keepalive_session(&uuid_session_key)
            .await?
            .expect("session row remains");
        ensure!(
            still_terminal.status == CacheKeepaliveSessionStatus::Terminal,
            "the refused enqueue left the fenced session untouched"
        );

        // Restoring client policy reopens the boundary for new real requests.
        let restored = PrincipalStore::update(
            &*storage,
            record.id,
            updated.revision,
            PrincipalUpdate {
                prompt_cache_mode: Some(PromptCacheMode::PreserveClient),
                ..PrincipalUpdate::default()
            },
            BASE_TS + 2,
        )
        .await?
        .expect("record should exist");
        ensure!(restored.prompt_cache_mode == PromptCacheMode::PreserveClient);
        let reopened = storage
            .replace_from_real_request(&keepalive_replace_request(
                &record.id.to_string(),
                &uuid_session_key,
            ))
            .await?;
        ensure!(
            reopened.status == CacheKeepaliveSessionStatus::Active,
            "a new real request starts a new generation after the mode is restored"
        );
        ensure!(
            reopened.generation > fenced.generation,
            "the restored session advances the generation fence"
        );
        Ok(())
    })
    .await
}

/// A rename and a mode switch can arrive in one revisioned update. The
/// retirement must cover every identity an admitted request could have
/// recorded: the immutable UUID, the name in force before the update, and the
/// new name — a name this principal is only now taking over, which may still
/// carry an active session from the principal that used it before. A response
/// admitted under the old name must not be able to create a session afterwards.
pub async fn prompt_cache_mode_rename_and_switch_retires_every_identity<B>(
    backend: Arc<B>,
) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: AuditStore + PrincipalStore + CacheKeepaliveSessionStore,
{
    with_fixture(backend, |storage| async move {
        // The previous holder of the name this principal is about to take over.
        // Its session survives the rename (a rename alone does not retire
        // sessions), so the name still carries an active session.
        let predecessor = PrincipalStore::create(&*storage, principal_create(28), BASE_TS).await?;
        let reused_name = predecessor.name.clone();
        let predecessor_session_key = "predecessor-name-session";
        storage
            .replace_from_real_request(&keepalive_replace_request(
                &reused_name,
                predecessor_session_key,
            ))
            .await?;
        PrincipalStore::update(
            &*storage,
            predecessor.id,
            predecessor.revision,
            PrincipalUpdate {
                name: Some(format!("{reused_name}-retired")),
                ..PrincipalUpdate::default()
            },
            BASE_TS + 1,
        )
        .await?
        .expect("record should exist");
        let lingering = storage
            .get_cache_keepalive_session(predecessor_session_key)
            .await?
            .expect("session row remains");
        ensure!(
            lingering.status == CacheKeepaliveSessionStatus::Active,
            "a rename alone keeps the session active, so the old name can carry one"
        );

        // The successor holds that name next, with two sessions of its own: one
        // admitted by UUID, one admitted by its previous name.
        let target = PrincipalStore::create(&*storage, principal_create(29), BASE_TS).await?;
        let previous_name = target.name.clone();
        let uuid_session_key = "successor-uuid-session";
        let previous_name_session_key = "successor-previous-name-session";
        storage
            .replace_from_real_request(&keepalive_replace_request(
                &target.id.to_string(),
                uuid_session_key,
            ))
            .await?;
        storage
            .replace_from_real_request(&keepalive_replace_request(
                &previous_name,
                previous_name_session_key,
            ))
            .await?;

        let updated = PrincipalStore::update(
            &*storage,
            target.id,
            target.revision,
            PrincipalUpdate {
                name: Some(reused_name.clone()),
                prompt_cache_mode: Some(PromptCacheMode::OneHourNoKeepalive),
                ..PrincipalUpdate::default()
            },
            BASE_TS + 2,
        )
        .await?
        .expect("record should exist");
        ensure!(
            updated.name == reused_name
                && updated.prompt_cache_mode == PromptCacheMode::OneHourNoKeepalive,
            "the update applies the rename and the mode together"
        );

        for session_key_hash in [
            predecessor_session_key,
            uuid_session_key,
            previous_name_session_key,
        ] {
            let fenced = storage
                .get_cache_keepalive_session(session_key_hash)
                .await?
                .expect("session row remains for history");
            ensure!(
                fenced.status == CacheKeepaliveSessionStatus::Terminal
                    && fenced.terminal_reason == Some(CacheKeepaliveTerminalReason::Cancelled),
                "the rename-and-switch update retires session {session_key_hash}"
            );
            ensure!(
                fenced.encrypted_payload.is_empty(),
                "the retired session drops its payload"
            );
        }

        // A response admitted under the previous name — the exact alias this
        // storage boundary used to accept — is fenced off twice over: the name
        // no longer resolves to a live principal, and the mode prohibits
        // keep-alives.
        let stale_admission = storage
            .replace_from_real_request(&keepalive_replace_request(
                &previous_name,
                "rename-late-response",
            ))
            .await;
        ensure!(
            matches!(stale_admission, Err(StorageError::PrincipalNotFound { .. })),
            "a late response holding the previous name cannot create a session: {stale_admission:?}"
        );
        let mode_admission = storage
            .replace_from_real_request(&keepalive_replace_request(
                &reused_name,
                "rename-late-response",
            ))
            .await;
        ensure!(
            matches!(
                mode_admission,
                Err(StorageError::InvalidInput { ref field, .. })
                    if field == "principal.prompt_cache_mode"
            ),
            "the reused name is fenced by the one-hour mode: {mode_admission:?}"
        );
        ensure!(
            storage
                .get_cache_keepalive_session("rename-late-response")
                .await?
                .is_none(),
            "a refused admission writes no session row"
        );

        // Turning the mode back off must not revive anything: only a new real
        // request may start a generation, and it starts above the retired one.
        let restored = PrincipalStore::update(
            &*storage,
            target.id,
            updated.revision,
            PrincipalUpdate {
                prompt_cache_mode: Some(PromptCacheMode::PreserveClient),
                ..PrincipalUpdate::default()
            },
            BASE_TS + 3,
        )
        .await?
        .expect("record should exist");
        ensure!(restored.prompt_cache_mode == PromptCacheMode::PreserveClient);
        for session_key_hash in [predecessor_session_key, previous_name_session_key] {
            let revived = storage
                .get_cache_keepalive_session(session_key_hash)
                .await?
                .expect("retired session row remains");
            ensure!(
                revived.status == CacheKeepaliveSessionStatus::Terminal
                    && revived.terminal_reason == Some(CacheKeepaliveTerminalReason::Cancelled),
                "restoring client policy leaves session {session_key_hash} terminal"
            );
        }
        let fresh = storage
            .replace_from_real_request(&keepalive_replace_request(
                &reused_name,
                "rename-fresh-session",
            ))
            .await?;
        ensure!(
            fresh.status == CacheKeepaliveSessionStatus::Active && fresh.generation == 1,
            "a new real request under the new name starts a fresh generation"
        );
        Ok(())
    })
    .await
}

/// Session admission resolves the caller-visible principal id the way
/// `PrincipalView::get` does and fails closed when that resolves to nothing.
pub async fn keepalive_session_admission_matches_principal_view_resolution<B>(
    backend: Arc<B>,
) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: AuditStore + PrincipalStore + CacheKeepaliveSessionStore,
{
    with_fixture(backend, |storage| async move {
        // A UUID-shaped principal *name* is legal. `PrincipalView` keys its
        // specs by name and keeps UUIDs only as aliases, so an id that spells
        // one principal's name and another principal's UUID resolves as the
        // name. Session admission must agree, or one principal's mode would
        // decide the other principal's keep-alives.
        let aliased_uuid_owner =
            PrincipalStore::create(&*storage, principal_create(25), BASE_TS).await?;
        let shaped_name = aliased_uuid_owner.id.to_string();
        let named_after_uuid = PrincipalStore::create(
            &*storage,
            PrincipalCreate {
                name: shaped_name.clone(),
                ..principal_create(26)
            },
            BASE_TS,
        )
        .await?;
        let switched = PrincipalStore::update(
            &*storage,
            aliased_uuid_owner.id,
            aliased_uuid_owner.revision,
            PrincipalUpdate {
                prompt_cache_mode: Some(PromptCacheMode::OneHourNoKeepalive),
                ..PrincipalUpdate::default()
            },
            BASE_TS + 1,
        )
        .await?
        .expect("record should exist");
        ensure!(
            switched.prompt_cache_mode == PromptCacheMode::OneHourNoKeepalive,
            "the UUID owner enters one-hour mode"
        );

        let admission = storage
            .replace_from_real_request(&keepalive_replace_request(
                &shaped_name,
                "uuid-shaped-session",
            ))
            .await;
        ensure!(
            admission.is_ok(),
            "the id resolves to the principal *named* that way, which preserves client \
             policy: {admission:?}"
        );
        let session = storage
            .get_cache_keepalive_session("uuid-shaped-session")
            .await?
            .expect("session exists");
        ensure!(
            session.principal_id == shaped_name
                && session.principal_id != named_after_uuid.id.to_string(),
            "the session is keyed by the resolution the dynamic view would make"
        );

        // A name that belongs to no live principal fails closed instead of
        // creating a session the scheduler could never authorize.
        let unknown = storage
            .replace_from_real_request(&keepalive_replace_request("no-such-principal", "ghost"))
            .await;
        ensure!(
            matches!(unknown, Err(StorageError::PrincipalNotFound { .. })),
            "an unresolvable principal id fails closed: {unknown:?}"
        );

        // A soft-deleted principal is unresolvable through both of its
        // identities: `PrincipalView` drops deleted rows, so neither the name
        // nor the UUID alias resolves.
        let doomed = PrincipalStore::create(&*storage, principal_create(27), BASE_TS).await?;
        PrincipalStore::soft_delete(&*storage, doomed.id, doomed.revision, BASE_TS + 2).await?;
        for identity in [&doomed.name, &doomed.id.to_string()] {
            let deleted = storage
                .replace_from_real_request(&keepalive_replace_request(
                    identity,
                    "deleted-principal-session",
                ))
                .await;
            ensure!(
                matches!(deleted, Err(StorageError::PrincipalNotFound { .. })),
                "a deleted principal's identity {identity} fails closed: {deleted:?}"
            );
        }
        ensure!(
            storage
                .get_cache_keepalive_session("deleted-principal-session")
                .await?
                .is_none(),
            "a refused admission writes no session row"
        );
        Ok(())
    })
    .await
}

fn keepalive_replace_request(
    principal_id: &str,
    session_key_hash: &str,
) -> CacheKeepaliveReplaceRequest {
    CacheKeepaliveReplaceRequest {
        session_key_hash: session_key_hash.to_owned(),
        principal_id: principal_id.to_owned(),
        accounting_key_id: None,
        upstream_id: Uuid::from_u128(9),
        cache_anchor_at_unix_secs: BASE_TS,
        ttl: CacheTtl::Ttl5m,
        run_at_unix_secs: BASE_TS + 270,
        expires_at_unix_secs: BASE_TS + 300,
        encrypted_payload: Vec::new(),
        display_reason: "agent-in-turn".to_owned(),
        config_snapshot: CacheKeepaliveConfigSnapshot {
            refresh_lead_time_5m_secs: 30,
            refresh_lead_time_1h_secs: 300,
            max_refreshes_per_session: 12,
            max_total_duration_secs: 14_400,
            snapshot_max_bytes: 524_288,
        },
        now_unix_secs: BASE_TS,
    }
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
                prompt_cache_mode: None,
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

async fn recreate_same_name_after_soft_delete_creates_new_principal<B>(
    backend: Arc<B>,
) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: AuditStore + PrincipalStore,
{
    with_fixture(backend, |storage| async move {
        const NAME: &str = "principal-name-reuse";
        let input = || {
            let mut input = principal_create(18);
            input.name = NAME.to_owned();
            input
        };

        let first = PrincipalStore::create(&*storage, input(), BASE_TS).await?;
        PrincipalStore::soft_delete(&*storage, first.id, first.revision, BASE_TS + 1)
            .await?
            .expect("first principal should soft delete");

        ensure!(
            PrincipalStore::get_by_name(&*storage, NAME)
                .await?
                .is_none(),
            "soft-deleted principal must be hidden from get_by_name"
        );

        let second = PrincipalStore::create(&*storage, input(), BASE_TS + 2).await?;
        ensure!(
            second.id != first.id,
            "recreating a soft-deleted principal name must allocate a new id"
        );
        ensure!(
            second.deleted_at_unix_secs.is_none(),
            "recreated principal must be live"
        );
        ensure!(
            PrincipalStore::get_by_name(&*storage, NAME)
                .await?
                .is_some_and(|record| record.id == second.id),
            "get_by_name must return the recreated live principal"
        );
        ensure!(
            PrincipalStore::get_by_id(&*storage, first.id)
                .await?
                .is_some_and(|record| record.deleted_at_unix_secs.is_some()),
            "first soft-deleted principal must remain addressable by id"
        );

        let error = PrincipalStore::create(&*storage, input(), BASE_TS + 3)
            .await
            .expect_err("creating a duplicate of a live principal name must fail");
        ensure!(
            matches!(error, StorageError::Conflict { .. }),
            "expected live duplicate name conflict, got {error:?}"
        );

        PrincipalStore::soft_delete(&*storage, second.id, second.revision, BASE_TS + 4)
            .await?
            .expect("second principal should soft delete");
        let third = PrincipalStore::create(&*storage, input(), BASE_TS + 5).await?;
        ensure!(
            third.id != first.id && third.id != second.id,
            "each recreation must allocate a new principal id"
        );
        ensure!(
            third.deleted_at_unix_secs.is_none(),
            "third principal must be live"
        );
        ensure!(
            PrincipalStore::get_by_id(&*storage, second.id)
                .await?
                .is_some_and(|record| record.deleted_at_unix_secs.is_some()),
            "second soft-deleted principal must remain addressable by id"
        );
        ensure!(
            PrincipalStore::get_by_id(&*storage, first.id)
                .await?
                .is_some_and(|record| record.deleted_at_unix_secs.is_some()),
            "first tombstone must remain after a second recreation"
        );
        ensure!(
            PrincipalStore::get_by_name(&*storage, NAME)
                .await?
                .is_some_and(|record| record.id == third.id),
            "get_by_name must return the third live principal"
        );
        ensure!(
            PrincipalStore::list(&*storage, 0, 10, false)
                .await?
                .iter()
                .filter(|record| record.name == NAME)
                .count()
                == 1,
            "only the third principal may be live"
        );
        ensure!(
            PrincipalStore::list(&*storage, 0, 10, true)
                .await?
                .iter()
                .filter(|record| record.name == NAME)
                .count()
                == 3,
            "two tombstones and one live principal must coexist"
        );
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
        prompt_cache_mode: Default::default(),
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
        admin_action: Some("principal_store".to_owned()),
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
            },
            WasmRegistryEntryInput {
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
    for (slot, order) in [(PluginSlotKind::Router, 100), (PluginSlotKind::Shape, 300)] {
        storage
            .insert_chain_entry(PluginChainEntryInput {
                principal_id,
                slot,
                order,
                wasm_registry_id: plugin_id,
                config: json!({}),
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
            mode: cc_lb_plugin_wire::metadata::HookMode::Active,
        },
    )])
}

async fn ensure_all_slots_empty<S>(storage: &S, principal_id: Uuid) -> Result<()>
where
    S: PluginRegistryStore,
{
    for slot in [PluginSlotKind::Router, PluginSlotKind::Shape] {
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
