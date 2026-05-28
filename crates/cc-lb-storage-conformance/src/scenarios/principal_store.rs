use std::sync::Arc;

use anyhow::{Result, ensure};
use cc_lb_storage_api::{
    AuditEntry, AuditStore, Limit, LimitKind, PrincipalCreate, PrincipalKind, PrincipalRecord,
    PrincipalStore, PrincipalUpdate, StorageError, validate_identifier,
};

use crate::harness::{ConformanceBackend, ConformanceFixture};

const BASE_TS: u64 = 1_900_000_000;

pub async fn run_all<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: AuditStore + PrincipalStore,
{
    create_persists_defaults(Arc::clone(&backend)).await?;
    get_by_id_returns_created(Arc::clone(&backend)).await?;
    get_by_name_returns_created(Arc::clone(&backend)).await?;
    list_paginates_by_name(Arc::clone(&backend)).await?;
    update_with_current_revision_succeeds(Arc::clone(&backend)).await?;
    stale_revision_conflict(Arc::clone(&backend)).await?;
    set_enabled_toggles(Arc::clone(&backend)).await?;
    allowed_models_persist_raw(Arc::clone(&backend)).await?;
    default_limits_roundtrip(Arc::clone(&backend)).await?;
    soft_delete_excludes_default_list(Arc::clone(&backend)).await?;
    hard_delete_removes_unreferenced(Arc::clone(&backend)).await?;
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
                default_limits: None,
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
            0,
            Some("router compile failed".to_owned()),
            BASE_TS + 3,
        )
        .await?
        .unwrap();
        ensure!(errored.last_apply_error == Some("router compile failed".to_owned()));
        ensure!(errored.last_apply_at_unix_secs == Some(BASE_TS + 3));
        let cleared =
            PrincipalStore::set_last_apply_error(&*storage, record.id, 1, None, BASE_TS + 4)
                .await?
                .unwrap();
        ensure!(cleared.last_apply_error.is_none());
        ensure!(cleared.last_apply_at_unix_secs == Some(BASE_TS + 4));
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
        default_limits: limits(),
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
        input_tokens: 1,
        output_tokens: 2,
        duration_ms: 3,
        agent_label: None,
        kind: Some("principal_store".to_owned()),
        payload: None,
    }
}
