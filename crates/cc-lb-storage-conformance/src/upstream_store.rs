use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use cc_lb_aead::{AeadService, EncryptedOAuthTokens, OAuthTokenBundle};
use cc_lb_engine::{Clock, TestClock, unix_secs};
use cc_lb_storage_api::upstream::{
    OAuthRefreshTerminalFailure, UpstreamCreate, UpstreamKind, UpstreamRecord,
    UpstreamStatusUpdate, UpstreamStore, UpstreamUpdate,
};
use cc_lb_storage_api::{StorageError, StorageResult, validate_identifier};
use tokio::sync::Mutex;
use url::Url;
use uuid::Uuid;

struct MemoryUpstreamStore {
    state: Mutex<MemoryUpstreamState>,
    clock: Arc<TestClock>,
}

#[derive(Default)]
struct MemoryUpstreamState {
    records: Vec<UpstreamRecord>,
    refresh_leases: HashMap<Uuid, MemoryRefreshLease>,
    terminal_refresh_failures: HashMap<Uuid, MemoryTerminalRefreshFailure>,
}

#[derive(Clone, Copy)]
struct MemoryRefreshLease {
    holder: Uuid,
    expected_generation: u64,
    lease_until_unix_secs: u64,
}

#[derive(Clone)]
struct MemoryTerminalRefreshFailure {
    expected_generation: u64,
    code: String,
}

#[async_trait]
impl UpstreamStore for MemoryUpstreamStore {
    async fn create(&self, create: UpstreamCreate) -> StorageResult<UpstreamRecord> {
        validate_identifier("upstream.name", &create.name)?;
        let mut state = self.state.lock().await;
        if state
            .records
            .iter()
            .any(|record| record.name == create.name)
        {
            return Err(conflict("upstream name already exists"));
        }
        let now = unix_secs(self.clock.now());
        let record = UpstreamRecord {
            id: Uuid::new_v4(),
            name: create.name,
            kind: create.kind,
            base_url: create.base_url,
            enabled: true,
            oauth_credentials: None,
            api_key_ciphertext: create.api_key_ciphertext,
            last_apply_error: None,
            last_apply_at_unix_secs: None,
            deleted_at_unix_secs: None,
            revision: 1,
            oauth_token_generation: create.oauth_token_generation.unwrap_or_default(),
            created_at_unix_secs: now,
            updated_at_unix_secs: now,
            warmup_enabled: create.warmup_enabled,
            warmup_dialect_plugin: create.warmup_dialect_plugin,
            last_warmup_at_unix_secs: None,
        };
        state.records.push(record.clone());
        Ok(record)
    }

    async fn get_by_name(&self, name: &str) -> StorageResult<Option<UpstreamRecord>> {
        Ok(self
            .state
            .lock()
            .await
            .records
            .iter()
            .find(|record| record.name == name)
            .cloned())
    }

    async fn get_by_id(&self, id: Uuid) -> StorageResult<Option<UpstreamRecord>> {
        Ok(self
            .state
            .lock()
            .await
            .records
            .iter()
            .find(|record| record.id == id)
            .cloned())
    }

    async fn list(&self, after: Option<Uuid>, limit: usize) -> StorageResult<Vec<UpstreamRecord>> {
        let mut records: Vec<_> = self
            .state
            .lock()
            .await
            .records
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
        let invalidates_refresh_lease = update.oauth_token_generation.is_some();
        self.mutate(
            id,
            Some(expected_revision),
            invalidates_refresh_lease,
            |record| {
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
                if let Some(oauth_token_generation) = update.oauth_token_generation {
                    record.oauth_token_generation = oauth_token_generation;
                }
                if let Some(warmup_enabled) = update.warmup_enabled {
                    record.warmup_enabled = warmup_enabled;
                }
                if let Some(warmup_dialect_plugin) = update.warmup_dialect_plugin {
                    record.warmup_dialect_plugin = Some(warmup_dialect_plugin);
                }
                Ok(())
            },
        )
        .await
    }

    async fn set_enabled(
        &self,
        id: Uuid,
        expected_revision: u64,
        enabled: bool,
    ) -> StorageResult<UpstreamRecord> {
        self.mutate(id, Some(expected_revision), false, |record| {
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
        self.mutate(id, Some(expected_revision), false, |record| {
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
        self.mutate_without_revision(id, false, |record| {
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
        self.mutate_without_revision(id, true, |record| {
            record.oauth_credentials = Some(tokens);
            record.oauth_token_generation = record.oauth_token_generation.saturating_add(1);
            record.last_apply_error = None;
            Ok(())
        })
        .await
    }

    async fn set_status(&self, id: Uuid, status: UpstreamStatusUpdate) -> StorageResult<()> {
        self.mutate_without_revision(id, false, |record| {
            if let Some(value) = status.last_apply_error {
                record.last_apply_error = value;
            }
            if let Some(value) = status.last_apply_at_unix_secs {
                record.last_apply_at_unix_secs = value;
            }
            if let Some(value) = status.last_warmup_at_unix_secs {
                record.last_warmup_at_unix_secs = value;
            }
            Ok(())
        })
        .await?;
        Ok(())
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
        expected_generation: u64,
        ttl_secs: u64,
    ) -> StorageResult<bool> {
        let now = unix_secs(self.clock.now());
        let mut state = self.state.lock().await;
        let Some(current_generation) = state
            .records
            .iter()
            .find(|record| record.id == id && record.deleted_at_unix_secs.is_none())
            .map(|record| record.oauth_token_generation)
        else {
            return Ok(false);
        };
        if current_generation != expected_generation
            || state
                .terminal_refresh_failures
                .get(&id)
                .is_some_and(|failure| failure.expected_generation == expected_generation)
        {
            return Ok(false);
        }
        if state
            .refresh_leases
            .get(&id)
            .is_some_and(|lease| lease.lease_until_unix_secs > now)
        {
            return Ok(false);
        }
        state.refresh_leases.insert(
            id,
            MemoryRefreshLease {
                holder,
                expected_generation,
                lease_until_unix_secs: now.saturating_add(ttl_secs),
            },
        );
        Ok(true)
    }

    async fn fail_refresh(
        &self,
        id: Uuid,
        holder: Uuid,
        terminal_error: Option<String>,
    ) -> StorageResult<bool> {
        let now = unix_secs(self.clock.now());
        let mut state = self.state.lock().await;
        if !state
            .records
            .iter()
            .any(|record| record.id == id && record.deleted_at_unix_secs.is_none())
        {
            return Ok(false);
        }
        let Some(lease) = state.refresh_leases.get(&id).copied() else {
            return Ok(false);
        };
        if lease.holder != holder || lease.lease_until_unix_secs <= now {
            return Ok(false);
        }
        state.refresh_leases.remove(&id);
        if let Some(code) = terminal_error {
            state.terminal_refresh_failures.insert(
                id,
                MemoryTerminalRefreshFailure {
                    expected_generation: lease.expected_generation,
                    code,
                },
            );
        }
        Ok(true)
    }
    async fn read_oauth_refresh_terminal_failure(
        &self,
        id: Uuid,
    ) -> StorageResult<Option<OAuthRefreshTerminalFailure>> {
        Ok(self
            .state
            .lock()
            .await
            .terminal_refresh_failures
            .get(&id)
            .map(|failure| OAuthRefreshTerminalFailure {
                upstream_id: id,
                expected_generation: failure.expected_generation,
                code: failure.code.clone(),
            }))
    }

    async fn list_oauth_refresh_terminal_failures(
        &self,
    ) -> StorageResult<Vec<OAuthRefreshTerminalFailure>> {
        Ok(self
            .state
            .lock()
            .await
            .terminal_refresh_failures
            .iter()
            .map(|(upstream_id, failure)| OAuthRefreshTerminalFailure {
                upstream_id: *upstream_id,
                expected_generation: failure.expected_generation,
                code: failure.code.clone(),
            })
            .collect())
    }

    async fn complete_refresh(
        &self,
        id: Uuid,
        holder: Uuid,
        tokens: EncryptedOAuthTokens,
    ) -> StorageResult<UpstreamRecord> {
        let now = unix_secs(self.clock.now());
        let mut state = self.state.lock().await;
        let lease = state
            .refresh_leases
            .get(&id)
            .copied()
            .ok_or_else(|| conflict("oauth refresh lease fence failed"))?;
        if lease.holder != holder || lease.lease_until_unix_secs <= now {
            return Err(conflict("oauth refresh lease fence failed"));
        }
        let record = state
            .records
            .iter_mut()
            .find(|record| record.id == id && record.deleted_at_unix_secs.is_none())
            .ok_or_else(|| conflict("oauth refresh lease fence failed"))?;
        if record.oauth_token_generation != lease.expected_generation {
            return Err(conflict("oauth refresh lease fence failed"));
        }
        record.oauth_credentials = Some(tokens);
        record.oauth_token_generation = lease
            .expected_generation
            .checked_add(1)
            .ok_or_else(|| conflict("oauth token generation overflow"))?;
        record.last_apply_error = None;
        let refreshed = record.clone();
        state.refresh_leases.remove(&id);
        state.terminal_refresh_failures.remove(&id);
        Ok(refreshed)
    }

    async fn set_last_apply_error(&self, id: Uuid, error: Option<String>) -> StorageResult<()> {
        self.set_status(
            id,
            UpstreamStatusUpdate {
                last_apply_error: Some(error),
                last_apply_at_unix_secs: Some(Some(unix_secs(self.clock.now()))),
                ..UpstreamStatusUpdate::default()
            },
        )
        .await
    }

    async fn soft_delete(&self, id: Uuid, expected_revision: u64) -> StorageResult<()> {
        self.mutate(id, Some(expected_revision), false, |record| {
            record.deleted_at_unix_secs = Some(unix_secs(self.clock.now()));
            Ok(())
        })
        .await?;
        Ok(())
    }

    async fn hard_delete(&self, id: Uuid) -> StorageResult<()> {
        let mut state = self.state.lock().await;
        state.records.retain(|record| record.id != id);
        state.refresh_leases.remove(&id);
        state.terminal_refresh_failures.remove(&id);
        Ok(())
    }

    async fn clear_warmup_dialect_plugin(
        &self,
        id: Uuid,
        expected_revision: u64,
    ) -> StorageResult<Option<UpstreamRecord>> {
        let mut state = self.state.lock().await;
        let Some(record) = state.records.iter_mut().find(|record| record.id == id) else {
            return Ok(None);
        };
        if record.deleted_at_unix_secs.is_some() || record.revision != expected_revision {
            return Ok(None);
        }
        record.warmup_dialect_plugin = None;
        record.revision += 1;
        record.updated_at_unix_secs = unix_secs(self.clock.now());
        Ok(Some(record.clone()))
    }
}

impl MemoryUpstreamStore {
    async fn mutate<F>(
        &self,
        id: Uuid,
        expected_revision: Option<u64>,
        invalidates_refresh_lease: bool,
        mutate: F,
    ) -> StorageResult<UpstreamRecord>
    where
        F: FnOnce(&mut UpstreamRecord) -> StorageResult<()>,
    {
        let mut state = self.state.lock().await;
        let duplicate_names: Vec<String> = state
            .records
            .iter()
            .filter(|record| record.id != id)
            .map(|record| record.name.clone())
            .collect();
        let updated = {
            let record = state
                .records
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
            record.updated_at_unix_secs = unix_secs(self.clock.now());
            record.clone()
        };
        if invalidates_refresh_lease {
            state.refresh_leases.remove(&id);
            state.terminal_refresh_failures.remove(&id);
        }
        Ok(updated)
    }
    async fn mutate_without_revision<F>(
        &self,
        id: Uuid,
        invalidates_refresh_lease: bool,
        mutate: F,
    ) -> StorageResult<UpstreamRecord>
    where
        F: FnOnce(&mut UpstreamRecord) -> StorageResult<()>,
    {
        let mut state = self.state.lock().await;
        let updated = {
            let record = state
                .records
                .iter_mut()
                .find(|record| record.id == id)
                .ok_or_else(|| conflict("upstream not found"))?;
            mutate(record)?;
            record.clone()
        };
        if invalidates_refresh_lease {
            state.refresh_leases.remove(&id);
            state.terminal_refresh_failures.remove(&id);
        }
        Ok(updated)
    }

    async fn ensure_revision(&self, id: Uuid, expected_revision: u64) -> StorageResult<()> {
        let state = self.state.lock().await;
        let record = state
            .records
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
    Arc::new(MemoryUpstreamStore {
        state: Mutex::new(MemoryUpstreamState::default()),
        clock: Arc::new(TestClock::new_at_secs(1_800_000_000)),
    })
}

async fn create_default(store: &MemoryUpstreamStore, name: &str) -> UpstreamRecord {
    store
        .create(UpstreamCreate {
            name: name.to_owned(),
            kind: UpstreamKind::AnthropicOauth,
            base_url: Some(Url::parse("https://api.anthropic.com").unwrap()),
            api_key_ciphertext: None,
            oauth_token_generation: None,
            warmup_enabled: false,
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
            refresh_token_expires_at_unix_secs: None,
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
scenario!(upstream_store_12_complete_refresh_stores_tokens, async {
    let s = store();
    let r = create_default(&s, "primary").await;
    let h = Uuid::new_v4();
    assert!(
        s.claim_refresh_lease(r.id, h, r.oauth_token_generation, 60)
            .await
            .unwrap()
    );
    let refreshed = s.complete_refresh(r.id, h, tokens("two")).await.unwrap();
    assert!(refreshed.oauth_credentials.is_some());
    assert_eq!(
        refreshed.oauth_token_generation,
        r.oauth_token_generation + 1
    );
});
scenario!(upstream_store_13_refresh_lease_has_one_live_holder, async {
    let s = store();
    let r = create_default(&s, "primary").await;
    let holder = Uuid::new_v4();
    let contender = Uuid::new_v4();
    assert!(
        s.claim_refresh_lease(r.id, holder, r.oauth_token_generation, 60)
            .await
            .unwrap()
    );
    assert!(
        !s.claim_refresh_lease(r.id, contender, r.oauth_token_generation, 60)
            .await
            .unwrap()
    );
});
scenario!(
    upstream_store_14_wrong_refresh_holder_cannot_complete_or_fail,
    async {
        let s = store();
        let r = create_default(&s, "primary").await;
        let holder = Uuid::new_v4();
        let wrong_holder = Uuid::new_v4();
        assert!(
            s.claim_refresh_lease(r.id, holder, r.oauth_token_generation, 60)
                .await
                .unwrap()
        );
        assert!(!s.fail_refresh(r.id, wrong_holder, None).await.unwrap());
        assert!(matches!(
            s.complete_refresh(r.id, wrong_holder, tokens("wrong"))
                .await
                .unwrap_err(),
            StorageError::Conflict { .. }
        ));
        let unchanged = s.get_by_id(r.id).await.unwrap().unwrap();
        assert_eq!(unchanged.oauth_token_generation, r.oauth_token_generation);
        assert!(unchanged.last_apply_error.is_none());
    }
);
scenario!(
    upstream_store_15_refresh_completion_is_fenced_exactly_once,
    async {
        let s = store();
        let r = create_default(&s, "primary").await;
        s.set_last_apply_error(r.id, Some("previous refresh error".to_owned()))
            .await
            .unwrap();
        let holder = Uuid::new_v4();
        assert!(
            s.claim_refresh_lease(r.id, holder, r.oauth_token_generation, 60)
                .await
                .unwrap()
        );
        let refreshed = s
            .complete_refresh(r.id, holder, tokens("fresh"))
            .await
            .unwrap();
        assert_eq!(
            refreshed.oauth_token_generation,
            r.oauth_token_generation + 1
        );
        assert!(refreshed.last_apply_error.is_none());
        assert!(matches!(
            s.complete_refresh(r.id, holder, tokens("stale"))
                .await
                .unwrap_err(),
            StorageError::Conflict { .. }
        ));
        assert_eq!(
            s.get_by_id(r.id)
                .await
                .unwrap()
                .unwrap()
                .oauth_token_generation,
            r.oauth_token_generation + 1
        );
    }
);
scenario!(
    upstream_store_16_expired_refresh_lease_can_be_reclaimed,
    async {
        let s = store();
        let r = create_default(&s, "primary").await;
        let first = Uuid::new_v4();
        let second = Uuid::new_v4();
        assert!(
            s.claim_refresh_lease(r.id, first, r.oauth_token_generation, 60)
                .await
                .unwrap()
        );
        s.clock.advance_secs(60);
        assert!(
            s.claim_refresh_lease(r.id, second, r.oauth_token_generation, 60)
                .await
                .unwrap()
        );
    }
);
scenario!(
    upstream_store_17_manual_token_replacement_invalidates_refresh_lease,
    async {
        let s = store();
        let r = create_default(&s, "primary").await;
        let holder = Uuid::new_v4();
        assert!(
            s.claim_refresh_lease(r.id, holder, r.oauth_token_generation, 60)
                .await
                .unwrap()
        );
        s.update_oauth_token(r.id, tokens("manual")).await.unwrap();
        assert!(matches!(
            s.complete_refresh(r.id, holder, tokens("stale"))
                .await
                .unwrap_err(),
            StorageError::Conflict { .. }
        ));
    }
);
scenario!(upstream_store_refresh_failure_releases_lease, async {
    let s = store();
    let r = create_default(&s, "primary").await;
    let holder = Uuid::new_v4();
    let next_holder = Uuid::new_v4();
    assert!(
        s.claim_refresh_lease(r.id, holder, r.oauth_token_generation, 60)
            .await
            .unwrap()
    );
    assert!(s.fail_refresh(r.id, holder, None).await.unwrap());
    assert!(
        s.claim_refresh_lease(r.id, next_holder, r.oauth_token_generation, 60,)
            .await
            .unwrap()
    );
});
scenario!(
    upstream_store_terminal_refresh_failure_blocks_same_generation_until_token_replacement,
    async {
        let s = store();
        let r = create_default(&s, "primary").await;
        let holder = Uuid::new_v4();
        assert!(
            s.claim_refresh_lease(r.id, holder, r.oauth_token_generation, 60)
                .await
                .unwrap()
        );
        assert!(
            s.fail_refresh(r.id, holder, Some("invalid_grant".to_owned()))
                .await
                .unwrap()
        );
        assert_eq!(
            s.list_oauth_refresh_terminal_failures().await.unwrap(),
            vec![OAuthRefreshTerminalFailure {
                upstream_id: r.id,
                expected_generation: r.oauth_token_generation,
                code: "invalid_grant".to_owned(),
            }]
        );
        assert!(
            !s.claim_refresh_lease(r.id, Uuid::new_v4(), r.oauth_token_generation, 60,)
                .await
                .unwrap()
        );
        let updated = s
            .update_oauth_token(r.id, tokens("replacement"))
            .await
            .unwrap();
        assert!(
            s.list_oauth_refresh_terminal_failures()
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            s.claim_refresh_lease(r.id, Uuid::new_v4(), updated.oauth_token_generation, 60,)
                .await
                .unwrap()
        );
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
                oauth_token_generation: None,
                warmup_enabled: false,
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
