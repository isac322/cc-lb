use async_trait::async_trait;
use cc_lb_plugin_api::TerminalStrategy;
use cc_lb_storage_api::{
    PrincipalCreate, PrincipalRecord, PrincipalStore, PrincipalUpdate, StorageResult,
    validate_identifier,
};
use redb::{ReadableDatabase, ReadableTable};
use uuid::Uuid;

use crate::error_map::{map_join_err, map_redb_err};
use crate::{
    PRINCIPAL_ALLOWED_UPSTREAMS_V1, PRINCIPALS_V2, PRINCIPALS_V2_BY_NAME, RedbStorage, StorageError,
};

#[async_trait]
impl PrincipalStore for RedbStorage {
    async fn create(
        &self,
        input: PrincipalCreate,
        now_unix_secs: u64,
    ) -> StorageResult<PrincipalRecord> {
        validate_identifier("principal.name", &input.name)?;
        let storage = self.clone();
        tokio::task::spawn_blocking(move || storage.create_principal(input, now_unix_secs))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn get_by_id(&self, id: Uuid) -> StorageResult<Option<PrincipalRecord>> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || storage.get_principal_by_id(id))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn get_by_name(&self, name: &str) -> StorageResult<Option<PrincipalRecord>> {
        let storage = self.clone();
        let name = name.to_owned();
        tokio::task::spawn_blocking(move || storage.get_principal_by_name(&name))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn list(
        &self,
        offset: usize,
        limit: usize,
        include_deleted: bool,
    ) -> StorageResult<Vec<PrincipalRecord>> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || storage.list_principals(offset, limit, include_deleted))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn update(
        &self,
        id: Uuid,
        expected_revision: u64,
        update: PrincipalUpdate,
        now_unix_secs: u64,
    ) -> StorageResult<Option<PrincipalRecord>> {
        if let Some(name) = update.name.as_deref() {
            validate_identifier("principal.name", name)?;
        }
        let storage = self.clone();
        tokio::task::spawn_blocking(move || {
            storage.update_principal(id, expected_revision, update, now_unix_secs)
        })
        .await
        .map_err(map_join_err)?
        .map_err(map_redb_err)
    }

    async fn set_enabled(
        &self,
        id: Uuid,
        expected_revision: u64,
        enabled: bool,
        now_unix_secs: u64,
    ) -> StorageResult<Option<PrincipalRecord>> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || {
            storage.set_principal_enabled(id, expected_revision, enabled, now_unix_secs)
        })
        .await
        .map_err(map_join_err)?
        .map_err(map_redb_err)
    }

    async fn soft_delete(
        &self,
        id: Uuid,
        expected_revision: u64,
        now_unix_secs: u64,
    ) -> StorageResult<Option<PrincipalRecord>> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || {
            storage.soft_delete_principal(id, expected_revision, now_unix_secs)
        })
        .await
        .map_err(map_join_err)?
        .map_err(map_redb_err)
    }

    async fn hard_delete(&self, id: Uuid) -> StorageResult<bool> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || storage.hard_delete_principal(id))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn set_last_apply_error(
        &self,
        id: Uuid,
        expected_revision: u64,
        error: Option<String>,
        applied_at_unix_secs: u64,
    ) -> StorageResult<Option<PrincipalRecord>> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || {
            storage.set_principal_last_apply_error(
                id,
                expected_revision,
                error,
                applied_at_unix_secs,
            )
        })
        .await
        .map_err(map_join_err)?
        .map_err(map_redb_err)
    }
}

impl RedbStorage {
    fn create_principal(
        &self,
        input: PrincipalCreate,
        now_unix_secs: u64,
    ) -> Result<PrincipalRecord, StorageError> {
        let write_txn = self.db.begin_write()?;
        let record = PrincipalRecord {
            id: Uuid::new_v4(),
            name: input.name,
            kind: input.kind,
            allowed_models: input.allowed_models,
            allowed_upstreams: input.allowed_upstreams,
            default_limits: input.default_limits,
            enabled: true,
            last_apply_error: None,
            last_apply_at_unix_secs: None,
            deleted_at_unix_secs: None,
            revision: 0,
            created_at_unix_secs: now_unix_secs,
            updated_at_unix_secs: now_unix_secs,
            router_terminal_strategy: TerminalStrategy::FirstPick,
        };
        {
            let name_index = write_txn.open_table(PRINCIPALS_V2_BY_NAME)?;
            if name_index.get(record.name.as_str())?.is_some() {
                return Err(StorageError::PrincipalNameConflict { name: record.name });
            }
        }
        {
            let mut principals = write_txn.open_table(PRINCIPALS_V2)?;
            let payload = serde_json::to_vec(&record)?;
            principals.insert(record.id.as_bytes().as_slice(), payload.as_slice())?;
        }
        {
            let mut name_index = write_txn.open_table(PRINCIPALS_V2_BY_NAME)?;
            name_index.insert(record.name.as_str(), record.id.as_bytes().as_slice())?;
        }
        write_principal_allowed_upstreams(&write_txn, record.id, &record.allowed_upstreams)?;
        write_txn.commit()?;
        Ok(record)
    }

    fn get_principal_by_id(&self, id: Uuid) -> Result<Option<PrincipalRecord>, StorageError> {
        let read_txn = self.db.begin_read()?;
        let principals = read_txn.open_table(PRINCIPALS_V2)?;
        let allowed_upstreams = read_txn.open_table(PRINCIPAL_ALLOWED_UPSTREAMS_V1)?;
        principals
            .get(id.as_bytes().as_slice())?
            .map(|stored| {
                let mut record: PrincipalRecord = serde_json::from_slice(stored.value())?;
                populate_principal_allowed_upstreams(&allowed_upstreams, &mut record)?;
                Ok(record)
            })
            .transpose()
    }

    fn get_principal_by_name(&self, name: &str) -> Result<Option<PrincipalRecord>, StorageError> {
        let read_txn = self.db.begin_read()?;
        let name_index = read_txn.open_table(PRINCIPALS_V2_BY_NAME)?;
        let Some(id) = name_index.get(name)? else {
            return Ok(None);
        };
        let id = uuid_from_bytes(id.value())?;
        let principals = read_txn.open_table(PRINCIPALS_V2)?;
        let allowed_upstreams = read_txn.open_table(PRINCIPAL_ALLOWED_UPSTREAMS_V1)?;
        principals
            .get(id.as_bytes().as_slice())?
            .map(|stored| {
                let mut record: PrincipalRecord = serde_json::from_slice(stored.value())?;
                populate_principal_allowed_upstreams(&allowed_upstreams, &mut record)?;
                Ok(record)
            })
            .transpose()
    }

    fn list_principals(
        &self,
        offset: usize,
        limit: usize,
        include_deleted: bool,
    ) -> Result<Vec<PrincipalRecord>, StorageError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let read_txn = self.db.begin_read()?;
        let principals = read_txn.open_table(PRINCIPALS_V2)?;
        let allowed_upstreams = read_txn.open_table(PRINCIPAL_ALLOWED_UPSTREAMS_V1)?;
        let mut records = Vec::new();
        for row in principals.iter()? {
            let (_, value) = row?;
            let mut record: PrincipalRecord = serde_json::from_slice(value.value())?;
            populate_principal_allowed_upstreams(&allowed_upstreams, &mut record)?;
            if include_deleted || record.deleted_at_unix_secs.is_none() {
                records.push(record);
            }
        }
        records.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(records.into_iter().skip(offset).take(limit).collect())
    }

    fn update_principal(
        &self,
        id: Uuid,
        expected_revision: u64,
        update: PrincipalUpdate,
        now_unix_secs: u64,
    ) -> Result<Option<PrincipalRecord>, StorageError> {
        let PrincipalUpdate {
            name,
            allowed_models,
            allowed_upstreams,
            default_limits,
            router_terminal_strategy,
        } = update;
        let allowed_upstreams_update = allowed_upstreams.clone();
        self.mutate_principal(id, expected_revision, allowed_upstreams_update, |record| {
            if let Some(name) = name {
                record.name = name;
            }
            if let Some(allowed_models) = allowed_models {
                record.allowed_models = allowed_models;
            }
            if let Some(allowed_upstreams) = allowed_upstreams {
                record.allowed_upstreams = allowed_upstreams;
            }
            if let Some(default_limits) = default_limits {
                record.default_limits = default_limits;
            }
            if let Some(router_terminal_strategy) = router_terminal_strategy {
                record.router_terminal_strategy = router_terminal_strategy;
            }
            record.updated_at_unix_secs = now_unix_secs;
            Ok(())
        })
    }

    fn set_principal_enabled(
        &self,
        id: Uuid,
        expected_revision: u64,
        enabled: bool,
        now_unix_secs: u64,
    ) -> Result<Option<PrincipalRecord>, StorageError> {
        self.mutate_principal(id, expected_revision, None, |record| {
            record.enabled = enabled;
            record.updated_at_unix_secs = now_unix_secs;
            Ok(())
        })
    }

    fn soft_delete_principal(
        &self,
        id: Uuid,
        expected_revision: u64,
        now_unix_secs: u64,
    ) -> Result<Option<PrincipalRecord>, StorageError> {
        self.mutate_principal(id, expected_revision, None, |record| {
            record.deleted_at_unix_secs = Some(now_unix_secs);
            record.updated_at_unix_secs = now_unix_secs;
            Ok(())
        })
    }

    fn hard_delete_principal(&self, id: Uuid) -> Result<bool, StorageError> {
        if !self
            .query_audit(Some(&id.to_string()), 0, u64::MAX, 1)?
            .is_empty()
        {
            return Err(StorageError::PrincipalReferencedByAudit { id: id.to_string() });
        }
        let write_txn = self.db.begin_write()?;
        let record = {
            let principals = write_txn.open_table(PRINCIPALS_V2)?;
            let Some(stored) = principals.get(id.as_bytes().as_slice())? else {
                return Ok(false);
            };
            serde_json::from_slice::<PrincipalRecord>(stored.value())?
        };
        {
            let mut principals = write_txn.open_table(PRINCIPALS_V2)?;
            principals.remove(id.as_bytes().as_slice())?;
        }
        {
            let mut name_index = write_txn.open_table(PRINCIPALS_V2_BY_NAME)?;
            name_index.remove(record.name.as_str())?;
        }
        {
            let mut allowed_upstreams = write_txn.open_table(PRINCIPAL_ALLOWED_UPSTREAMS_V1)?;
            allowed_upstreams.remove(id.as_bytes().as_slice())?;
        }
        write_txn.commit()?;
        Ok(true)
    }

    fn set_principal_last_apply_error(
        &self,
        id: Uuid,
        expected_revision: u64,
        error: Option<String>,
        applied_at_unix_secs: u64,
    ) -> Result<Option<PrincipalRecord>, StorageError> {
        self.mutate_principal(id, expected_revision, None, |record| {
            record.last_apply_error = error;
            record.last_apply_at_unix_secs = Some(applied_at_unix_secs);
            record.updated_at_unix_secs = applied_at_unix_secs;
            Ok(())
        })
    }

    fn mutate_principal<F>(
        &self,
        id: Uuid,
        expected_revision: u64,
        allowed_upstreams_update: Option<Vec<Uuid>>,
        mutate: F,
    ) -> Result<Option<PrincipalRecord>, StorageError>
    where
        F: FnOnce(&mut PrincipalRecord) -> Result<(), StorageError>,
    {
        let write_txn = self.db.begin_write()?;
        let mut record = {
            let principals = write_txn.open_table(PRINCIPALS_V2)?;
            let Some(stored) = principals.get(id.as_bytes().as_slice())? else {
                return Ok(None);
            };
            serde_json::from_slice::<PrincipalRecord>(stored.value())?
        };
        {
            let allowed_upstreams = write_txn.open_table(PRINCIPAL_ALLOWED_UPSTREAMS_V1)?;
            populate_principal_allowed_upstreams(&allowed_upstreams, &mut record)?;
        }
        if record.revision != expected_revision {
            return Err(StorageError::StalePrincipalRevision {
                current: record.revision,
            });
        }
        let old_name = record.name.clone();
        mutate(&mut record)?;
        if record.name != old_name {
            let name_index = write_txn.open_table(PRINCIPALS_V2_BY_NAME)?;
            if name_index.get(record.name.as_str())?.is_some() {
                return Err(StorageError::PrincipalNameConflict { name: record.name });
            }
        }
        record.revision = record
            .revision
            .checked_add(1)
            .ok_or(StorageError::PrincipalRevisionOverflow)?;
        {
            let mut principals = write_txn.open_table(PRINCIPALS_V2)?;
            let payload = serde_json::to_vec(&record)?;
            principals.insert(id.as_bytes().as_slice(), payload.as_slice())?;
        }
        if record.name != old_name {
            let mut name_index = write_txn.open_table(PRINCIPALS_V2_BY_NAME)?;
            name_index.remove(old_name.as_str())?;
            name_index.insert(record.name.as_str(), id.as_bytes().as_slice())?;
        }
        if let Some(allowed_upstreams) = allowed_upstreams_update.as_ref() {
            write_principal_allowed_upstreams(&write_txn, id, allowed_upstreams)?;
        }
        write_txn.commit()?;
        Ok(Some(record))
    }
}

fn populate_principal_allowed_upstreams(
    table: &impl ReadableTable<&'static [u8], &'static [u8]>,
    record: &mut PrincipalRecord,
) -> Result<(), StorageError> {
    if let Some(stored) = table.get(record.id.as_bytes().as_slice())? {
        record.allowed_upstreams = serde_json::from_slice(stored.value())?;
    }
    Ok(())
}

fn write_principal_allowed_upstreams(
    write_txn: &redb::WriteTransaction,
    principal_id: Uuid,
    allowed_upstreams: &[Uuid],
) -> Result<(), StorageError> {
    let mut table = write_txn.open_table(PRINCIPAL_ALLOWED_UPSTREAMS_V1)?;
    let payload = serde_json::to_vec(allowed_upstreams)?;
    table.insert(principal_id.as_bytes().as_slice(), payload.as_slice())?;
    Ok(())
}

fn uuid_from_bytes(bytes: &[u8]) -> Result<Uuid, StorageError> {
    Uuid::from_slice(bytes).map_err(|error| StorageError::InvalidBackendKind(error.to_string()))
}
