use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use cc_lb_aead::{AeadService, EncryptedOAuthTokens, OAuthTokenBundle};
use cc_lb_storage_api::upstream::{UpstreamLeaseKind, UpstreamStatusUpdate};
use cc_lb_storage_api::{
    StorageError, StorageResult, UpstreamCreate, UpstreamKind, UpstreamRecord, UpstreamStore,
    UpstreamUpdate, validate_identifier,
};
use tokio::sync::Mutex;
use url::Url;
use uuid::Uuid;

#[derive(Default)]
struct MemoryUpstreamStore {
    records: Mutex<Vec<UpstreamRecord>>,
}

#[async_trait]
impl UpstreamStore for MemoryUpstreamStore {
    async fn create(&self, create: UpstreamCreate) -> StorageResult<UpstreamRecord> {
        validate_identifier("upstream.name", &create.name)?;
        let mut records = self.records.lock().await;
        if records.iter().any(|record| record.name == create.name) {
            return Err(conflict("upstream name already exists"));
        }
        let now = now_unix_secs();
        let record = UpstreamRecord {
            id: Uuid::new_v4(),
            name: create.name,
            kind: create.kind,
            base_url: create.base_url,
            enabled: true,
            oauth_credentials: None,
            api_key_ciphertext: create.api_key_ciphertext,
            refresh_lease_holder: None,
            refresh_lease_until_unix_secs: None,
            last_apply_error: None,
            last_apply_at_unix_secs: None,
            deleted_at_unix_secs: None,
            revision: 1,
            created_at_unix_secs: now,
            updated_at_unix_secs: now,
            warmup_enabled: create.warmup_enabled,
            next_warmup_at: create.next_warmup_at,
            last_warmup_cycle_key: create.last_warmup_cycle_key,
            warmup_lease_holder: create.warmup_lease_holder,
            warmup_lease_until_unix_secs: create.warmup_lease_until_unix_secs,
            warmup_dialect_plugin: create.warmup_dialect_plugin,
        };
        records.push(record.clone());
        Ok(record)
    }

    async fn get_by_name(&self, name: &str) -> StorageResult<Option<UpstreamRecord>> {
        Ok(self
            .records
            .lock()
            .await
            .iter()
            .find(|record| record.name == name)
            .cloned())
    }

    async fn get_by_id(&self, id: Uuid) -> StorageResult<Option<UpstreamRecord>> {
        Ok(self
            .records
            .lock()
            .await
            .iter()
            .find(|record| record.id == id)
            .cloned())
    }

    async fn list(&self, after: Option<Uuid>, limit: usize) -> StorageResult<Vec<UpstreamRecord>> {
        let mut records: Vec<_> = self
            .records
            .lock()
            .await
            .iter()
            .filter(|record| record.deleted_at_unix_secs.is_none())
            .cloned()
            .collect();
        records.sort_by_key(|record| record.id);
        let start = after
            .and_then(|id| {
                records
                    .iter()
                    .position(|record| record.id == id)
                    .map(|index| index + 1)
            })
            .unwrap_or(0);
        Ok(records.into_iter().skip(start).take(limit).collect())
    }

    async fn update(
        &self,
        id: Uuid,
        expected_revision: u64,
        update: UpstreamUpdate,
    ) -> StorageResult<UpstreamRecord> {
        if let Some(name) = update.name.as_deref() {
            validate_identifier("upstream.name", name)?;
        }
        self.mutate(id, Some(expected_revision), |record| {
            if let Some(name) = update.name {
                record.name = name;
            }
            if let Some(base_url) = update.base_url {
                record.base_url = Some(base_url);
            }
            if let Some(enabled) = update.enabled {
                record.enabled = enabled;
            }
            if let Some(api_key_ciphertext) = update.api_key_ciphertext {
                record.api_key_ciphertext = Some(api_key_ciphertext);
            }
            if let Some(warmup_enabled) = update.warmup_enabled {
                record.warmup_enabled = warmup_enabled;
            }
            if update.next_warmup_at.is_some() {
                record.next_warmup_at = update.next_warmup_at;
            }
            if let Some(last_warmup_cycle_key) = update.last_warmup_cycle_key {
                record.last_warmup_cycle_key = Some(last_warmup_cycle_key);
            }
            if let Some(warmup_lease_holder) = update.warmup_lease_holder {
                record.warmup_lease_holder = Some(warmup_lease_holder);
            }
            if let Some(warmup_lease_until_unix_secs) = update.warmup_lease_until_unix_secs {
                record.warmup_lease_until_unix_secs = Some(warmup_lease_until_unix_secs);
            }
            if let Some(warmup_dialect_plugin) = update.warmup_dialect_plugin {
                record.warmup_dialect_plugin = Some(warmup_dialect_plugin);
            }
            Ok(())
        })
        .await
    }

    async fn set_enabled(
        &self,
        id: Uuid,
        expected_revision: u64,
        enabled: bool,
    ) -> StorageResult<UpstreamRecord> {
        self.mutate(id, Some(expected_revision), |record| {
            record.enabled = enabled;
            Ok(())
        })
        .await
    }

    async fn update_spec(
        &self,
        id: Uuid,
        expected_revision: u64,
        update: UpstreamUpdate,
    ) -> StorageResult<UpstreamRecord> {
        if let Some(name) = update.name.as_deref() {
            validate_identifier("upstream.name", name)?;
        }
        self.mutate(id, Some(expected_revision), |record| {
            if let Some(name) = update.name {
                record.name = name;
            }
            if let Some(base_url) = update.base_url {
                record.base_url = Some(base_url);
            }
            if let Some(enabled) = update.enabled {
                record.enabled = enabled;
            }
            if let Some(warmup_enabled) = update.warmup_enabled {
                record.warmup_enabled = warmup_enabled;
            }
            if let Some(warmup_dialect_plugin) = update.warmup_dialect_plugin {
                record.warmup_dialect_plugin = Some(warmup_dialect_plugin);
            }
            Ok(())
        })
        .await
    }

    async fn update_api_key_secret(
        &self,
        id: Uuid,
        api_key_ciphertext: Option<Vec<u8>>,
    ) -> StorageResult<UpstreamRecord> {
        self.mutate_without_revision(id, |record| {
            record.api_key_ciphertext = api_key_ciphertext;
            Ok(())
        })
        .await
    }

    async fn update_oauth_token(
        &self,
        id: Uuid,
        tokens: EncryptedOAuthTokens,
    ) -> StorageResult<UpstreamRecord> {
        self.mutate_without_revision(id, |record| {
            record.oauth_credentials = Some(tokens);
            Ok(())
        })
        .await
    }

    async fn set_status(&self, id: Uuid, status: UpstreamStatusUpdate) -> StorageResult<()> {
        self.mutate_without_revision(id, |record| {
            if let Some(value) = status.last_apply_error {
                record.last_apply_error = value;
            }
            if let Some(value) = status.last_apply_at_unix_secs {
                record.last_apply_at_unix_secs = value;
            }
            if let Some(value) = status.next_warmup_at {
                record.next_warmup_at = value;
            }
            if let Some(value) = status.last_warmup_cycle_key {
                record.last_warmup_cycle_key = value;
            }
            Ok(())
        })
        .await?;
        Ok(())
    }

    async fn claim_lease(
        &self,
        id: Uuid,
        lease_kind: UpstreamLeaseKind,
        holder: String,
        ttl_secs: i64,
    ) -> StorageResult<bool> {
        let now_i64 = now_unix_secs_i64()?;
        let now_u64 =
            u64::try_from(now_i64).map_err(|_| fatal("current unix timestamp is negative"))?;
        let until_i64 = now_i64.saturating_add(ttl_secs);
        let until_u64 = u64::try_from(until_i64).map_err(|_| StorageError::InvalidInput {
            field: "ttl_secs".to_owned(),
            reason: "refresh lease expiry cannot be represented as u64".to_owned(),
        })?;
        let mut records = self.records.lock().await;
        let Some(record) = records.iter_mut().find(|record| record.id == id) else {
            return Ok(false);
        };
        if record.deleted_at_unix_secs.is_some() {
            return Ok(false);
        }
        match lease_kind {
            UpstreamLeaseKind::Refresh => {
                let holder_id =
                    Uuid::parse_str(&holder).map_err(|error| StorageError::InvalidInput {
                        field: "holder".to_owned(),
                        reason: error.to_string(),
                    })?;
                if record
                    .refresh_lease_until_unix_secs
                    .is_some_and(|until| until > now_u64)
                    && record.refresh_lease_holder != Some(holder_id)
                {
                    return Ok(false);
                }
                record.refresh_lease_holder = Some(holder_id);
                record.refresh_lease_until_unix_secs = Some(until_u64);
            }
            UpstreamLeaseKind::Warmup => {
                if record
                    .warmup_lease_until_unix_secs
                    .is_some_and(|until| until > now_i64)
                    && record.warmup_lease_holder.as_deref() != Some(holder.as_str())
                {
                    return Ok(false);
                }
                record.warmup_lease_holder = Some(holder);
                record.warmup_lease_until_unix_secs = Some(until_i64);
            }
        }
        Ok(true)
    }

    async fn renew_lease(
        &self,
        id: Uuid,
        lease_kind: UpstreamLeaseKind,
        holder: String,
        ttl_secs: i64,
    ) -> StorageResult<bool> {
        let now_i64 = now_unix_secs_i64()?;
        let now_u64 =
            u64::try_from(now_i64).map_err(|_| fatal("current unix timestamp is negative"))?;
        let until_i64 = now_i64.saturating_add(ttl_secs);
        let until_u64 = u64::try_from(until_i64).map_err(|_| StorageError::InvalidInput {
            field: "ttl_secs".to_owned(),
            reason: "refresh lease expiry cannot be represented as u64".to_owned(),
        })?;
        let mut records = self.records.lock().await;
        let Some(record) = records.iter_mut().find(|record| record.id == id) else {
            return Ok(false);
        };
        match lease_kind {
            UpstreamLeaseKind::Refresh => {
                let holder_id =
                    Uuid::parse_str(&holder).map_err(|error| StorageError::InvalidInput {
                        field: "holder".to_owned(),
                        reason: error.to_string(),
                    })?;
                if record.refresh_lease_holder != Some(holder_id)
                    || record
                        .refresh_lease_until_unix_secs
                        .is_none_or(|until| until <= now_u64)
                {
                    return Ok(false);
                }
                record.refresh_lease_until_unix_secs = Some(until_u64);
            }
            UpstreamLeaseKind::Warmup => {
                if record.warmup_lease_holder.as_deref() != Some(holder.as_str())
                    || record
                        .warmup_lease_until_unix_secs
                        .is_none_or(|until| until <= now_i64)
                {
                    return Ok(false);
                }
                record.warmup_lease_until_unix_secs = Some(until_i64);
            }
        }
        Ok(true)
    }

    async fn release_lease(
        &self,
        id: Uuid,
        lease_kind: UpstreamLeaseKind,
        holder: String,
    ) -> StorageResult<bool> {
        let mut records = self.records.lock().await;
        let Some(record) = records.iter_mut().find(|record| record.id == id) else {
            return Ok(false);
        };
        match lease_kind {
            UpstreamLeaseKind::Refresh => {
                let holder_id =
                    Uuid::parse_str(&holder).map_err(|error| StorageError::InvalidInput {
                        field: "holder".to_owned(),
                        reason: error.to_string(),
                    })?;
                if record.refresh_lease_holder != Some(holder_id) {
                    return Ok(false);
                }
                record.refresh_lease_holder = None;
                record.refresh_lease_until_unix_secs = None;
            }
            UpstreamLeaseKind::Warmup => {
                if record.warmup_lease_holder.as_deref() != Some(holder.as_str()) {
                    return Ok(false);
                }
                record.warmup_lease_holder = None;
                record.warmup_lease_until_unix_secs = None;
            }
        }
        Ok(true)
    }

    async fn store_oauth_tokens(
        &self,
        id: Uuid,
        expected_revision: u64,
        tokens: EncryptedOAuthTokens,
    ) -> StorageResult<UpstreamRecord> {
        self.ensure_revision(id, expected_revision).await?;
        self.update_oauth_token(id, tokens).await
    }

    async fn claim_refresh_lease(
        &self,
        id: Uuid,
        holder: Uuid,
        ttl_secs: u64,
    ) -> StorageResult<bool> {
        let ttl_secs = i64::try_from(ttl_secs).map_err(|_| StorageError::InvalidInput {
            field: "ttl_secs".to_owned(),
            reason: "refresh lease ttl cannot be represented as i64".to_owned(),
        })?;
        self.claim_lease(id, UpstreamLeaseKind::Refresh, holder.to_string(), ttl_secs)
            .await
    }

    async fn complete_refresh(
        &self,
        id: Uuid,
        holder: Uuid,
        tokens: EncryptedOAuthTokens,
    ) -> StorageResult<UpstreamRecord> {
        if !self
            .release_lease(id, UpstreamLeaseKind::Refresh, holder.to_string())
            .await?
        {
            return Err(conflict("holder mismatch"));
        }
        self.update_oauth_token(id, tokens).await?;
        self.set_status(
            id,
            UpstreamStatusUpdate {
                last_apply_error: Some(None),
                ..UpstreamStatusUpdate::default()
            },
        )
        .await?;
        self.get_by_id(id)
            .await?
            .ok_or_else(|| conflict("upstream not found"))
    }

    async fn release_lease_on_failure(
        &self,
        id: Uuid,
        holder: Uuid,
        reason: String,
    ) -> StorageResult<()> {
        if !self
            .release_lease(id, UpstreamLeaseKind::Refresh, holder.to_string())
            .await?
        {
            return Err(conflict("holder mismatch"));
        }
        self.set_status(
            id,
            UpstreamStatusUpdate {
                last_apply_error: Some(Some(reason)),
                last_apply_at_unix_secs: Some(Some(now_unix_secs())),
                ..UpstreamStatusUpdate::default()
            },
        )
        .await?;
        Ok(())
    }

    async fn set_last_apply_error(&self, id: Uuid, error: Option<String>) -> StorageResult<()> {
        self.set_status(
            id,
            UpstreamStatusUpdate {
                last_apply_error: Some(error),
                last_apply_at_unix_secs: Some(Some(now_unix_secs())),
                ..UpstreamStatusUpdate::default()
            },
        )
        .await
    }

    async fn soft_delete(&self, id: Uuid, expected_revision: u64) -> StorageResult<()> {
        self.mutate(id, Some(expected_revision), |record| {
            record.deleted_at_unix_secs = Some(now_unix_secs());
            Ok(())
        })
        .await?;
        Ok(())
    }

    async fn hard_delete(&self, id: Uuid) -> StorageResult<()> {
        self.records.lock().await.retain(|record| record.id != id);
        Ok(())
    }

    async fn claim_warmup_lease(
        &self,
        id: Uuid,
        holder: &str,
        ttl_secs: i64,
    ) -> StorageResult<bool> {
        self.claim_lease(id, UpstreamLeaseKind::Warmup, holder.to_owned(), ttl_secs)
            .await
    }

    async fn write_warmup_cycle_key(
        &self,
        id: Uuid,
        holder: &str,
        new_cycle_key: i64,
        next_warmup_at: Option<chrono::DateTime<chrono::Utc>>,
    ) -> StorageResult<bool> {
        let now = now_unix_secs_i64()?;
        let mut records = self.records.lock().await;
        let Some(record) = records.iter_mut().find(|record| record.id == id) else {
            return Ok(false);
        };
        if record.deleted_at_unix_secs.is_some()
            || record.warmup_lease_holder.as_deref() != Some(holder)
            || record
                .warmup_lease_until_unix_secs
                .is_none_or(|until| until <= now)
            || record.last_warmup_cycle_key == Some(new_cycle_key)
        {
            return Ok(false);
        }
        record.last_warmup_cycle_key = Some(new_cycle_key);
        record.next_warmup_at = next_warmup_at;
        record.warmup_lease_holder = None;
        record.warmup_lease_until_unix_secs = None;
        Ok(true)
    }

    async fn release_warmup_lease(&self, id: Uuid, holder: &str) -> StorageResult<bool> {
        self.release_lease(id, UpstreamLeaseKind::Warmup, holder.to_owned())
            .await
    }

    async fn clear_warmup_dialect_plugin(
        &self,
        id: Uuid,
        expected_revision: u64,
    ) -> StorageResult<Option<UpstreamRecord>> {
        let mut records = self.records.lock().await;
        let Some(record) = records.iter_mut().find(|record| record.id == id) else {
            return Ok(None);
        };
        if record.deleted_at_unix_secs.is_some() || record.revision != expected_revision {
            return Ok(None);
        }
        record.warmup_dialect_plugin = None;
        record.revision += 1;
        record.updated_at_unix_secs = now_unix_secs();
        Ok(Some(record.clone()))
    }

    async fn write_warmup_next_at(
        &self,
        id: Uuid,
        holder: &str,
        next_warmup_at: chrono::DateTime<chrono::Utc>,
    ) -> StorageResult<bool> {
        let now = now_unix_secs_i64()?;
        let mut records = self.records.lock().await;
        let Some(record) = records.iter_mut().find(|record| record.id == id) else {
            return Ok(false);
        };
        if record.deleted_at_unix_secs.is_some()
            || record.warmup_lease_holder.as_deref() != Some(holder)
            || record
                .warmup_lease_until_unix_secs
                .is_none_or(|until| until <= now)
        {
            return Ok(false);
        }
        record.next_warmup_at = Some(next_warmup_at);
        Ok(true)
    }
}

impl MemoryUpstreamStore {
    async fn mutate<F>(
        &self,
        id: Uuid,
        expected_revision: Option<u64>,
        mutate: F,
    ) -> StorageResult<UpstreamRecord>
    where
        F: FnOnce(&mut UpstreamRecord) -> StorageResult<()>,
    {
        let mut records = self.records.lock().await;
        let duplicate_names: Vec<String> = records
            .iter()
            .filter(|record| record.id != id)
            .map(|record| record.name.clone())
            .collect();
        let record = records
            .iter_mut()
            .find(|record| record.id == id)
            .ok_or_else(|| conflict("upstream not found"))?;
        if expected_revision.is_some_and(|expected| record.revision != expected) {
            return Err(conflict("stale upstream revision"));
        }
        mutate(record)?;
        if duplicate_names.iter().any(|name| name == &record.name) {
            return Err(conflict("upstream name already exists"));
        }
        record.revision += 1;
        record.updated_at_unix_secs = now_unix_secs();
        Ok(record.clone())
    }

    async fn mutate_without_revision<F>(&self, id: Uuid, mutate: F) -> StorageResult<UpstreamRecord>
    where
        F: FnOnce(&mut UpstreamRecord) -> StorageResult<()>,
    {
        let mut records = self.records.lock().await;
        let record = records
            .iter_mut()
            .find(|record| record.id == id)
            .ok_or_else(|| conflict("upstream not found"))?;
        mutate(record)?;
        Ok(record.clone())
    }

    async fn ensure_revision(&self, id: Uuid, expected_revision: u64) -> StorageResult<()> {
        let records = self.records.lock().await;
        let record = records
            .iter()
            .find(|record| record.id == id)
            .ok_or_else(|| conflict("upstream not found"))?;
        if record.revision != expected_revision {
            return Err(conflict("stale upstream revision"));
        }
        Ok(())
    }
}

fn store() -> Arc<MemoryUpstreamStore> {
    Arc::new(MemoryUpstreamStore::default())
}

async fn create_default(store: &MemoryUpstreamStore, name: &str) -> UpstreamRecord {
    store
        .create(UpstreamCreate {
            name: name.to_owned(),
            kind: UpstreamKind::AnthropicOauth,
            base_url: Some(Url::parse("https://api.anthropic.com").unwrap()),
            api_key_ciphertext: None,
            warmup_enabled: false,
            next_warmup_at: None,
            last_warmup_cycle_key: None,
            warmup_lease_holder: None,
            warmup_lease_until_unix_secs: None,
            warmup_dialect_plugin: None,
        })
        .await
        .unwrap()
}

fn tokens(label: &str) -> EncryptedOAuthTokens {
    let aead = AeadService::from_master_key([7; 32]);
    EncryptedOAuthTokens::encrypt(
        &aead,
        &OAuthTokenBundle {
            access_token: format!("access-{label}"),
            refresh_token: format!("refresh-{label}"),
            expires_at_unix_secs: 1234,
            scopes: vec!["org:profile".to_owned()],
        },
        b"upstream-id",
    )
    .unwrap()
}

macro_rules! scenario {
    ($name:ident, $body:expr) => {
        #[tokio::test]
        async fn $name() {
            $body.await;
        }
    };
}

scenario!(upstream_store_01_create_sets_defaults, async {
    let s = store();
    let r = create_default(&s, "primary").await;
    assert_eq!(r.name, "primary");
    assert!(r.enabled);
    assert_eq!(r.revision, 1);
});
scenario!(upstream_store_02_get_by_id_returns_record, async {
    let s = store();
    let r = create_default(&s, "primary").await;
    assert_eq!(s.get_by_id(r.id).await.unwrap().unwrap().name, "primary");
});
scenario!(upstream_store_03_get_by_name_returns_record, async {
    let s = store();
    create_default(&s, "primary").await;
    assert!(s.get_by_name("primary").await.unwrap().is_some());
});
scenario!(upstream_store_04_list_returns_active_records, async {
    let s = store();
    create_default(&s, "a").await;
    create_default(&s, "b").await;
    assert_eq!(s.list(None, 10).await.unwrap().len(), 2);
});
scenario!(upstream_store_05_list_paginates_after_id, async {
    let s = store();
    create_default(&s, "a").await;
    create_default(&s, "b").await;
    let first = s.list(None, 1).await.unwrap();
    let second = s.list(Some(first[0].id), 1).await.unwrap();
    assert_eq!(second.len(), 1);
});
scenario!(
    upstream_store_06_update_with_revision_changes_fields,
    async {
        let s = store();
        let r = create_default(&s, "primary").await;
        let u = s
            .update(
                r.id,
                r.revision,
                UpstreamUpdate {
                    name: Some("renamed".to_owned()),
                    api_key_ciphertext: Some(vec![1, 2, 3]),
                    ..UpstreamUpdate::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(u.name, "renamed");
        assert_eq!(u.api_key_ciphertext, Some(vec![1, 2, 3]));
    }
);
scenario!(upstream_store_07_update_stale_revision_conflicts, async {
    let s = store();
    let r = create_default(&s, "primary").await;
    let e = s
        .update(r.id, 0, UpstreamUpdate::default())
        .await
        .unwrap_err();
    assert!(matches!(e, StorageError::Conflict { .. }));
});
scenario!(upstream_store_08_update_duplicate_name_conflicts, async {
    let s = store();
    let left = create_default(&s, "left").await;
    create_default(&s, "right").await;
    let e = s
        .update(
            left.id,
            left.revision,
            UpstreamUpdate {
                name: Some("right".to_owned()),
                ..UpstreamUpdate::default()
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(e, StorageError::Conflict { .. }));
});
scenario!(upstream_store_09_set_enabled_toggles, async {
    let s = store();
    let r = create_default(&s, "primary").await;
    assert!(
        !s.set_enabled(r.id, r.revision, false)
            .await
            .unwrap()
            .enabled
    );
});
scenario!(
    upstream_store_10_store_oauth_tokens_persists_ciphertext,
    async {
        let s = store();
        let r = create_default(&s, "primary").await;
        assert!(
            s.store_oauth_tokens(r.id, r.revision, tokens("one"))
                .await
                .unwrap()
                .oauth_credentials
                .is_some()
        );
    }
);
scenario!(upstream_store_11_oauth_tokens_decrypt_roundtrip, async {
    let s = store();
    let r = create_default(&s, "primary").await;
    let u = s
        .store_oauth_tokens(r.id, r.revision, tokens("one"))
        .await
        .unwrap();
    let aead = AeadService::from_master_key([7; 32]);
    let bundle = u
        .oauth_credentials
        .unwrap()
        .decrypt(&aead, b"upstream-id")
        .unwrap();
    assert_eq!(bundle.refresh_token, "refresh-one");
});
scenario!(upstream_store_12_claim_refresh_lease_success, async {
    let s = store();
    let r = create_default(&s, "primary").await;
    assert!(
        s.claim_refresh_lease(r.id, Uuid::new_v4(), 60)
            .await
            .unwrap()
    );
});
scenario!(
    upstream_store_13_claim_refresh_lease_contention_returns_false,
    async {
        let s = store();
        let r = create_default(&s, "primary").await;
        assert!(
            s.claim_refresh_lease(r.id, Uuid::new_v4(), 60)
                .await
                .unwrap()
        );
        assert!(
            !s.claim_refresh_lease(r.id, Uuid::new_v4(), 60)
                .await
                .unwrap()
        );
    }
);
scenario!(
    upstream_store_14_claim_refresh_lease_after_expiry_succeeds,
    async {
        let s = store();
        let r = create_default(&s, "primary").await;
        assert!(
            s.claim_refresh_lease(r.id, Uuid::new_v4(), 0)
                .await
                .unwrap()
        );
        assert!(
            s.claim_refresh_lease(r.id, Uuid::new_v4(), 60)
                .await
                .unwrap()
        );
    }
);
scenario!(
    upstream_store_15_complete_refresh_requires_holder_match,
    async {
        let s = store();
        let r = create_default(&s, "primary").await;
        let h = Uuid::new_v4();
        s.claim_refresh_lease(r.id, h, 60).await.unwrap();
        let e = s
            .complete_refresh(r.id, Uuid::new_v4(), tokens("two"))
            .await
            .unwrap_err();
        assert!(matches!(e, StorageError::Conflict { .. }));
    }
);
scenario!(upstream_store_16_complete_refresh_clears_lease, async {
    let s = store();
    let r = create_default(&s, "primary").await;
    let h = Uuid::new_v4();
    s.claim_refresh_lease(r.id, h, 60).await.unwrap();
    assert!(
        s.complete_refresh(r.id, h, tokens("two"))
            .await
            .unwrap()
            .refresh_lease_holder
            .is_none()
    );
});
scenario!(
    upstream_store_17_release_lease_on_failure_clears_lease_and_sets_error,
    async {
        let s = store();
        let r = create_default(&s, "primary").await;
        let h = Uuid::new_v4();
        s.claim_refresh_lease(r.id, h, 60).await.unwrap();
        s.release_lease_on_failure(r.id, h, "network".to_owned())
            .await
            .unwrap();
        let failed = s.get_by_id(r.id).await.unwrap().unwrap();
        assert_eq!(failed.refresh_lease_holder, None);
        assert_eq!(failed.refresh_lease_until_unix_secs, None);
        assert_eq!(failed.last_apply_error.as_deref(), Some("network"));
    }
);
scenario!(
    upstream_store_18_set_last_apply_error_sets_and_clears,
    async {
        let s = store();
        let r = create_default(&s, "primary").await;
        s.set_last_apply_error(r.id, Some("bad".to_owned()))
            .await
            .unwrap();
        assert!(
            s.get_by_id(r.id)
                .await
                .unwrap()
                .unwrap()
                .last_apply_error
                .is_some()
        );
        s.set_last_apply_error(r.id, None).await.unwrap();
        assert!(
            s.get_by_id(r.id)
                .await
                .unwrap()
                .unwrap()
                .last_apply_error
                .is_none()
        );
    }
);
scenario!(
    upstream_store_19_soft_delete_sets_deleted_at_and_hides_from_list,
    async {
        let s = store();
        let r = create_default(&s, "primary").await;
        s.soft_delete(r.id, r.revision).await.unwrap();
        assert!(
            s.get_by_id(r.id)
                .await
                .unwrap()
                .unwrap()
                .deleted_at_unix_secs
                .is_some()
        );
        assert!(s.list(None, 10).await.unwrap().is_empty());
    }
);
scenario!(
    upstream_store_20_hard_delete_and_validate_identifier,
    async {
        let s = store();
        let r = create_default(&s, "primary").await;
        s.hard_delete(r.id).await.unwrap();
        assert!(s.get_by_id(r.id).await.unwrap().is_none());
        let e = s
            .create(UpstreamCreate {
                name: "system.bad".to_owned(),
                kind: UpstreamKind::AnthropicApiKey,
                base_url: None,
                api_key_ciphertext: None,
                warmup_enabled: false,
                next_warmup_at: None,
                last_warmup_cycle_key: None,
                warmup_lease_holder: None,
                warmup_lease_until_unix_secs: None,
                warmup_dialect_plugin: None,
            })
            .await
            .unwrap_err();
        assert!(matches!(e, StorageError::InvalidInput { .. }));
    }
);

fn conflict(message: impl Into<String>) -> StorageError {
    StorageError::Conflict {
        message: message.into(),
    }
}

fn fatal(message: impl Into<String>) -> StorageError {
    StorageError::Fatal {
        message: message.into(),
    }
}

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn now_unix_secs_i64() -> StorageResult<i64> {
    i64::try_from(now_unix_secs())
        .map_err(|_| fatal("current unix timestamp cannot be represented as i64"))
}
