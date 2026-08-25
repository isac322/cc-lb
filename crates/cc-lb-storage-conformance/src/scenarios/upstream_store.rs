use std::{future::Future, sync::Arc};

use anyhow::{Result, ensure};
use async_trait::async_trait;
use cc_lb_aead::{AeadService, EncryptedOAuthTokens, OAuthTokenBundle};
use cc_lb_storage_api::upstream::{
    UpstreamCreate, UpstreamKind, UpstreamRecord, UpstreamStatusUpdate, UpstreamUpdate,
};
use cc_lb_storage_api::{StorageError, UpstreamStore};
use url::Url;
use uuid::Uuid;

#[async_trait]
pub trait UpstreamStoreBackend: Send + Sync + 'static {
    type Store: UpstreamStore;
    type Fixture: Send + Sync;

    async fn create_fixture(&self) -> Result<Self::Fixture>;
    async fn open(&self, fixture: &Self::Fixture) -> Result<Self::Store>;
    async fn teardown(&self, fixture: Self::Fixture) -> Result<()>;
}

struct Fixture<B: UpstreamStoreBackend> {
    backend: Arc<B>,
    fixture: Option<B::Fixture>,
    store: Arc<B::Store>,
}

impl<B: UpstreamStoreBackend> Fixture<B> {
    async fn new(backend: Arc<B>) -> Result<Self> {
        let fixture = backend.create_fixture().await?;
        let store = Arc::new(backend.open(&fixture).await?);
        Ok(Self {
            backend,
            fixture: Some(fixture),
            store,
        })
    }

    async fn teardown(&mut self) -> Result<()> {
        if let Some(fixture) = self.fixture.take() {
            self.backend.teardown(fixture).await?;
        }
        Ok(())
    }
}

pub async fn run_all<B>(backend: Arc<B>) -> Result<()>
where
    B: UpstreamStoreBackend,
{
    create_upstream(Arc::clone(&backend)).await?;
    get_by_id(Arc::clone(&backend)).await?;
    get_by_name(Arc::clone(&backend)).await?;
    get_by_name_missing(Arc::clone(&backend)).await?;
    list_pagination(Arc::clone(&backend)).await?;
    update_revision_ok(Arc::clone(&backend)).await?;
    update_stale_revision_conflict(Arc::clone(&backend)).await?;
    update_renames_name_index(Arc::clone(&backend)).await?;
    set_enabled_toggle(Arc::clone(&backend)).await?;
    store_oauth_tokens_roundtrip(Arc::clone(&backend)).await?;
    complete_refresh_stores_tokens(Arc::clone(&backend)).await?;
    refresh_lease_fencing(Arc::clone(&backend)).await?;
    refresh_failure_releases_lease(Arc::clone(&backend)).await?;
    manual_token_replacement_invalidates_refresh_lease(Arc::clone(&backend)).await?;
    set_last_apply_error_roundtrip(Arc::clone(&backend)).await?;
    status_update_does_not_bump_spec_revision(Arc::clone(&backend)).await?;
    secret_and_token_updates_do_not_bump_spec_revision(Arc::clone(&backend)).await?;
    soft_delete_sets_deleted_at(Arc::clone(&backend)).await?;
    update_spec_on_soft_deleted_returns_not_found(Arc::clone(&backend)).await?;
    set_status_on_soft_deleted_returns_not_found(Arc::clone(&backend)).await?;
    secret_update_on_soft_deleted_returns_not_found(Arc::clone(&backend)).await?;
    double_soft_delete_returns_not_found(Arc::clone(&backend)).await?;
    recreate_same_name_after_soft_delete_fails(Arc::clone(&backend)).await?;
    hard_delete_removes_row(Arc::clone(&backend)).await?;
    validate_identifier_rejects_bad_name(backend).await
}

macro_rules! scenario {
    ($name:ident, $body:expr) => {
        pub async fn $name<B>(backend: Arc<B>) -> Result<()>
        where
            B: UpstreamStoreBackend,
        {
            with_fixture(backend, $body).await
        }
    };
}

scenario!(create_upstream, |store| async move {
    let record = create_named(store.as_ref(), "upstream-create").await?;
    ensure!(record.name == "upstream-create", "name mismatch");
    ensure!(record.kind == UpstreamKind::AnthropicOauth, "kind mismatch");
    ensure!(record.enabled, "new upstream should be enabled");
    ensure!(record.revision == 1, "new upstream revision should be 1");
    Ok(())
});

scenario!(get_by_id, |store| async move {
    let record = create_named(store.as_ref(), "upstream-get-id").await?;
    ensure!(
        store.get_by_id(record.id).await? == Some(record),
        "get_by_id mismatch"
    );
    Ok(())
});

scenario!(get_by_name, |store| async move {
    let record = create_named(store.as_ref(), "upstream-get-name").await?;
    ensure!(
        store.get_by_name("upstream-get-name").await? == Some(record),
        "get_by_name mismatch"
    );
    Ok(())
});

scenario!(get_by_name_missing, |store| async move {
    ensure!(
        store.get_by_name("missing-upstream").await?.is_none(),
        "missing name should be None"
    );
    Ok(())
});

scenario!(list_pagination, |store| async move {
    for name in ["upstream-list-a", "upstream-list-b", "upstream-list-c"] {
        create_named(store.as_ref(), name).await?;
    }
    let first = store.list(None, 2).await?;
    ensure!(first.len() == 2, "first page should contain two rows");
    let second = store.list(Some(first[1].id), 2).await?;
    ensure!(second.len() == 1, "second page should contain one row");
    ensure!(
        first[0].id < first[1].id && first[1].id < second[0].id,
        "pagination order mismatch"
    );
    Ok(())
});

scenario!(update_revision_ok, |store| async move {
    let record = create_named(store.as_ref(), "upstream-update-ok").await?;
    let updated = store
        .update(
            record.id,
            record.revision,
            UpstreamUpdate {
                base_url: Some(url("https://example.com/v1")?),
                api_key_ciphertext: Some(vec![1, 2, 3]),
                ..UpstreamUpdate::default()
            },
        )
        .await?;
    ensure!(updated.revision == 2, "update should increment revision");
    ensure!(
        updated.base_url == Some(url("https://example.com/v1")?),
        "base_url mismatch"
    );
    ensure!(
        updated.api_key_ciphertext == Some(vec![1, 2, 3]),
        "ciphertext mismatch"
    );
    Ok(())
});

scenario!(update_stale_revision_conflict, |store| async move {
    let record = create_named(store.as_ref(), "upstream-update-stale").await?;
    let error = store
        .update(record.id, record.revision + 10, UpstreamUpdate::default())
        .await
        .expect_err("stale update should fail");
    ensure!(
        matches!(error, StorageError::Conflict { .. }),
        "expected conflict"
    );
    Ok(())
});

scenario!(update_renames_name_index, |store| async move {
    let record = create_named(store.as_ref(), "upstream-old-name").await?;
    store
        .update(
            record.id,
            record.revision,
            UpstreamUpdate {
                name: Some("upstream-new-name".to_owned()),
                ..UpstreamUpdate::default()
            },
        )
        .await?;
    ensure!(
        store.get_by_name("upstream-old-name").await?.is_none(),
        "old name index remains"
    );
    ensure!(
        store.get_by_name("upstream-new-name").await?.is_some(),
        "new name index missing"
    );
    Ok(())
});

scenario!(set_enabled_toggle, |store| async move {
    let record = create_named(store.as_ref(), "upstream-enabled").await?;
    let disabled = store.set_enabled(record.id, record.revision, false).await?;
    ensure!(
        !disabled.enabled && disabled.revision == 2,
        "disable mismatch"
    );
    let enabled = store
        .set_enabled(record.id, disabled.revision, true)
        .await?;
    ensure!(enabled.enabled && enabled.revision == 3, "enable mismatch");
    Ok(())
});

scenario!(store_oauth_tokens_roundtrip, |store| async move {
    let record = create_named(store.as_ref(), "upstream-oauth-store").await?;
    let aead = AeadService::from_master_key([42; 32]);
    let bundle = token_bundle("access-a", "refresh-a");
    let encrypted = EncryptedOAuthTokens::encrypt(&aead, &bundle, record.id.as_bytes())?;
    let updated = store
        .store_oauth_tokens(record.id, record.revision, encrypted)
        .await?;
    ensure!(
        updated
            .oauth_credentials
            .expect("tokens")
            .decrypt(&aead, record.id.as_bytes())?
            == bundle,
        "stored tokens should decrypt"
    );
    Ok(())
});

scenario!(complete_refresh_stores_tokens, |store| async move {
    let record = create_named(store.as_ref(), "upstream-refresh-complete").await?;
    let holder = Uuid::new_v4();
    let aead = AeadService::from_master_key([44; 32]);
    let bundle = token_bundle("access-b", "refresh-b");
    let encrypted = EncryptedOAuthTokens::encrypt(&aead, &bundle, record.id.as_bytes())?;
    ensure!(
        store
            .claim_refresh_lease(record.id, holder, record.oauth_token_generation, 3_600)
            .await?,
        "refresh lease should be claimed"
    );
    let refreshed = store.complete_refresh(record.id, holder, encrypted).await?;
    ensure!(
        refreshed
            .oauth_credentials
            .expect("tokens")
            .decrypt(&aead, record.id.as_bytes())?
            == bundle,
        "tokens mismatch"
    );
    Ok(())
});

scenario!(refresh_lease_fencing, |store| async move {
    let record = create_named(store.as_ref(), "upstream-refresh-fencing").await?;
    store
        .set_last_apply_error(record.id, Some("previous refresh error".to_owned()))
        .await?;
    let holder = Uuid::new_v4();
    let contender = Uuid::new_v4();
    let wrong_holder = Uuid::new_v4();
    ensure!(
        store
            .claim_refresh_lease(record.id, holder, record.oauth_token_generation, 3_600)
            .await?,
        "first holder should claim refresh lease"
    );
    ensure!(
        !store
            .claim_refresh_lease(record.id, contender, record.oauth_token_generation, 3_600)
            .await?,
        "contender must not claim a live refresh lease"
    );
    ensure!(
        !store.fail_refresh(record.id, wrong_holder, None).await?,
        "wrong holder must not fail refresh"
    );

    let aead = AeadService::from_master_key([46; 32]);
    let wrong_tokens = EncryptedOAuthTokens::encrypt(
        &aead,
        &token_bundle("access-wrong", "refresh-wrong"),
        record.id.as_bytes(),
    )?;
    let wrong_error = store
        .complete_refresh(record.id, wrong_holder, wrong_tokens)
        .await
        .expect_err("wrong holder must not complete refresh");
    ensure!(
        matches!(wrong_error, StorageError::Conflict { .. }),
        "wrong holder completion should conflict"
    );

    let fresh_bundle = token_bundle("access-fresh", "refresh-fresh");
    let fresh_tokens = EncryptedOAuthTokens::encrypt(&aead, &fresh_bundle, record.id.as_bytes())?;
    let refreshed = store
        .complete_refresh(record.id, holder, fresh_tokens)
        .await?;
    ensure!(
        refreshed.oauth_token_generation == record.oauth_token_generation + 1,
        "refresh completion must advance token generation exactly once"
    );
    ensure!(
        refreshed.last_apply_error.is_none(),
        "successful refresh should clear last_apply_error"
    );
    ensure!(
        refreshed
            .oauth_credentials
            .expect("tokens")
            .decrypt(&aead, record.id.as_bytes())?
            == fresh_bundle,
        "refresh completion stored unexpected tokens"
    );

    let stale_tokens = EncryptedOAuthTokens::encrypt(
        &aead,
        &token_bundle("access-stale", "refresh-stale"),
        record.id.as_bytes(),
    )?;
    let stale_error = store
        .complete_refresh(record.id, holder, stale_tokens)
        .await
        .expect_err("second completion must be rejected");
    ensure!(
        matches!(stale_error, StorageError::Conflict { .. }),
        "stale completion should conflict"
    );
    ensure!(
        store
            .read_oauth_token_generation(record.id)
            .await?
            .expect("generation")
            == record.oauth_token_generation + 1,
        "stale completion must not advance token generation"
    );
    Ok(())
});

scenario!(refresh_failure_releases_lease, |store| async move {
    let record = create_named(store.as_ref(), "upstream-refresh-failure").await?;
    let holder = Uuid::new_v4();
    let next_holder = Uuid::new_v4();
    ensure!(
        store
            .claim_refresh_lease(record.id, holder, record.oauth_token_generation, 3_600)
            .await?,
        "refresh lease should be claimed"
    );
    ensure!(
        store.fail_refresh(record.id, holder, None).await?,
        "matching live holder should fail refresh"
    );
    ensure!(
        store
            .claim_refresh_lease(record.id, next_holder, record.oauth_token_generation, 3_600,)
            .await?,
        "failed refresh should release the lease"
    );
    Ok(())
});

scenario!(
    manual_token_replacement_invalidates_refresh_lease,
    |store| async move {
        let record = create_named(store.as_ref(), "upstream-refresh-manual").await?;
        let old_holder = Uuid::new_v4();
        let new_holder = Uuid::new_v4();
        ensure!(
            store
                .claim_refresh_lease(record.id, old_holder, record.oauth_token_generation, 3_600)
                .await?,
            "old holder should claim refresh lease"
        );
        ensure!(
            store
                .fail_refresh(record.id, old_holder, Some("invalid_grant".to_owned()),)
                .await?,
            "terminal refresh failure should release the old lease"
        );
        ensure!(
            store
                .list_oauth_refresh_terminal_failures()
                .await?
                .iter()
                .any(|failure| {
                    failure.upstream_id == record.id
                        && failure.expected_generation == record.oauth_token_generation
                        && failure.code == "invalid_grant"
                }),
            "terminal refresh failure should be persisted for the credential generation"
        );
        ensure!(
            !store
                .claim_refresh_lease(record.id, new_holder, record.oauth_token_generation, 3_600,)
                .await?,
            "terminal refresh failure should block the same credential generation"
        );

        let aead = AeadService::from_master_key([47; 32]);
        let manual_tokens = EncryptedOAuthTokens::encrypt(
            &aead,
            &token_bundle("access-manual", "refresh-manual"),
            record.id.as_bytes(),
        )?;
        let manually_updated = store.update_oauth_token(record.id, manual_tokens).await?;
        ensure!(
            manually_updated.oauth_token_generation == record.oauth_token_generation + 1,
            "manual token replacement should advance the credential generation"
        );
        ensure!(
            manually_updated.last_apply_error.is_none(),
            "manual token replacement should clear the prior refresh error"
        );
        ensure!(
            store
                .list_oauth_refresh_terminal_failures()
                .await?
                .iter()
                .all(|failure| failure.upstream_id != record.id),
            "manual token replacement should clear the terminal refresh failure"
        );
        ensure!(
            store
                .claim_refresh_lease(
                    record.id,
                    new_holder,
                    manually_updated.oauth_token_generation,
                    3_600,
                )
                .await?,
            "manual token replacement should invalidate the old lease"
        );

        let stale_tokens = EncryptedOAuthTokens::encrypt(
            &aead,
            &token_bundle("access-old", "refresh-old"),
            record.id.as_bytes(),
        )?;
        let stale_error = store
            .complete_refresh(record.id, old_holder, stale_tokens)
            .await
            .expect_err("invalidated old holder must not complete refresh");
        ensure!(
            matches!(stale_error, StorageError::Conflict { .. }),
            "invalidated old holder completion should conflict"
        );
        Ok(())
    }
);

scenario!(set_last_apply_error_roundtrip, |store| async move {
    let record = create_named(store.as_ref(), "upstream-apply-error").await?;
    store
        .set_last_apply_error(record.id, Some("bad config".to_owned()))
        .await?;
    let errored = store.get_by_id(record.id).await?.expect("record");
    ensure!(
        errored.last_apply_error == Some("bad config".to_owned()),
        "error mismatch"
    );
    ensure!(
        errored.last_apply_at_unix_secs.is_some(),
        "apply timestamp missing"
    );
    store.set_last_apply_error(record.id, None).await?;
    ensure!(
        store
            .get_by_id(record.id)
            .await?
            .expect("record")
            .last_apply_error
            .is_none(),
        "error should clear"
    );
    Ok(())
});

scenario!(
    status_update_does_not_bump_spec_revision,
    |store| async move {
        let record = create_named(store.as_ref(), "upstream-status-stable").await?;
        store
            .set_status(
                record.id,
                UpstreamStatusUpdate {
                    last_apply_error: Some(Some("background".to_owned())),
                    last_apply_at_unix_secs: Some(Some(1_800_000_000)),
                    ..UpstreamStatusUpdate::default()
                },
            )
            .await?;
        let status_updated = store.get_by_id(record.id).await?.expect("record");
        ensure!(
            status_updated.revision == record.revision,
            "status update should not bump spec revision"
        );
        let spec_updated = store
            .update_spec(
                record.id,
                record.revision,
                UpstreamUpdate {
                    base_url: Some(url("https://status-stable.example.com")?),
                    ..UpstreamUpdate::default()
                },
            )
            .await?;
        ensure!(
            spec_updated.revision == record.revision + 1,
            "spec update should still use original revision after status update"
        );
        Ok(())
    }
);

scenario!(
    secret_and_token_updates_do_not_bump_spec_revision,
    |store| async move {
        let record = create_named(store.as_ref(), "upstream-secret-token-stable").await?;
        let secret_updated = store
            .update_api_key_secret(record.id, Some(vec![9, 8, 7]))
            .await?;
        ensure!(
            secret_updated.revision == record.revision,
            "secret update should not bump spec revision"
        );
        let aead = AeadService::from_master_key([45; 32]);
        let encrypted = EncryptedOAuthTokens::encrypt(
            &aead,
            &token_bundle("access-stable", "refresh-stable"),
            record.id.as_bytes(),
        )?;
        let token_updated = store.update_oauth_token(record.id, encrypted).await?;
        ensure!(
            token_updated.revision == record.revision,
            "token update should not bump spec revision"
        );
        let spec_updated = store
            .update_spec(
                record.id,
                record.revision,
                UpstreamUpdate {
                    base_url: Some(url("https://secret-token-stable.example.com")?),
                    ..UpstreamUpdate::default()
                },
            )
            .await?;
        ensure!(
            spec_updated.revision == record.revision + 1,
            "spec update should still use original revision after secret/token updates"
        );
        Ok(())
    }
);

scenario!(soft_delete_sets_deleted_at, |store| async move {
    let record = create_named(store.as_ref(), "upstream-soft-delete").await?;
    store.soft_delete(record.id, record.revision).await?;
    let deleted = store.get_by_id(record.id).await?.expect("record remains");
    ensure!(
        deleted.deleted_at_unix_secs.is_some(),
        "soft delete timestamp missing"
    );
    ensure!(
        deleted.revision == record.revision + 1,
        "soft delete should increment revision"
    );
    Ok(())
});

scenario!(
    update_spec_on_soft_deleted_returns_not_found,
    |store| async move {
        let record = create_named(store.as_ref(), "upstream-spec-after-delete").await?;
        store.soft_delete(record.id, record.revision).await?;
        let deleted = store.get_by_id(record.id).await?.expect("record remains");
        let error = store
            .update_spec(
                record.id,
                deleted.revision,
                UpstreamUpdate {
                    base_url: Some(url("https://example.com/after-delete")?),
                    ..UpstreamUpdate::default()
                },
            )
            .await
            .expect_err("spec update on soft-deleted upstream should fail");
        ensure!(
            matches!(error, StorageError::Conflict { .. }),
            "expected conflict/not-found error, got {error:?}"
        );
        let still_deleted = store.get_by_id(record.id).await?.expect("record remains");
        ensure!(
            still_deleted.revision == deleted.revision,
            "spec_revision should not advance on soft-deleted upstream"
        );
        Ok(())
    }
);

scenario!(
    set_status_on_soft_deleted_returns_not_found,
    |store| async move {
        let record = create_named(store.as_ref(), "upstream-status-after-delete").await?;
        store.soft_delete(record.id, record.revision).await?;
        let error = store
            .set_status(
                record.id,
                UpstreamStatusUpdate {
                    last_apply_error: Some(Some("bg-after-delete".to_owned())),
                    ..UpstreamStatusUpdate::default()
                },
            )
            .await
            .expect_err("status update on soft-deleted upstream should fail");
        ensure!(
            matches!(error, StorageError::Conflict { .. }),
            "expected conflict/not-found error, got {error:?}"
        );
        let after = store.get_by_id(record.id).await?.expect("record remains");
        ensure!(
            after.last_apply_error.is_none(),
            "background status write must not land on soft-deleted upstream"
        );
        Ok(())
    }
);

scenario!(
    secret_update_on_soft_deleted_returns_not_found,
    |store| async move {
        let record = create_named(store.as_ref(), "upstream-secret-after-delete").await?;
        store.soft_delete(record.id, record.revision).await?;
        let error = store
            .update_api_key_secret(record.id, Some(vec![9, 9, 9]))
            .await
            .expect_err("secret update on soft-deleted upstream should fail");
        ensure!(
            matches!(error, StorageError::Conflict { .. }),
            "expected conflict/not-found error, got {error:?}"
        );
        Ok(())
    }
);

scenario!(double_soft_delete_returns_not_found, |store| async move {
    let record = create_named(store.as_ref(), "upstream-double-soft-delete").await?;
    store.soft_delete(record.id, record.revision).await?;
    let deleted = store.get_by_id(record.id).await?.expect("record remains");
    let error = store
        .soft_delete(record.id, deleted.revision)
        .await
        .expect_err("second soft delete on soft-deleted upstream should fail");
    ensure!(
        matches!(error, StorageError::Conflict { .. }),
        "expected conflict/not-found error, got {error:?}"
    );
    let still_deleted = store.get_by_id(record.id).await?.expect("record remains");
    ensure!(
        still_deleted.revision == deleted.revision,
        "spec_revision must not advance on second soft delete"
    );
    ensure!(
        still_deleted.deleted_at_unix_secs == deleted.deleted_at_unix_secs,
        "deleted_at must not be re-stamped on second soft delete"
    );
    Ok(())
});

scenario!(
    recreate_same_name_after_soft_delete_fails,
    |store| async move {
        let record = create_named(store.as_ref(), "upstream-name-reservation").await?;
        store.soft_delete(record.id, record.revision).await?;
        let error = store
            .create(UpstreamCreate {
                name: "upstream-name-reservation".to_owned(),
                kind: UpstreamKind::AnthropicOauth,
                base_url: None,
                api_key_ciphertext: None,
                oauth_token_generation: None,
                warmup_enabled: false,
                warmup_dialect_plugin: None,
            })
            .await
            .expect_err("creating upstream with reserved name should fail");
        ensure!(
            matches!(error, StorageError::Conflict { .. }),
            "expected conflict error, got {error:?}"
        );
        Ok(())
    }
);

scenario!(hard_delete_removes_row, |store| async move {
    let record = create_named(store.as_ref(), "upstream-hard-delete").await?;
    let holder = Uuid::new_v4();
    ensure!(
        store
            .claim_refresh_lease(record.id, holder, record.oauth_token_generation, 60,)
            .await?,
        "refresh lease should be claimed before hard delete"
    );
    ensure!(
        store
            .fail_refresh(record.id, holder, Some("invalid_grant".to_owned()),)
            .await?,
        "terminal refresh failure should be recorded before hard delete"
    );
    store.hard_delete(record.id).await?;
    ensure!(
        store.get_by_id(record.id).await?.is_none(),
        "id row should be removed"
    );
    ensure!(
        store.get_by_name("upstream-hard-delete").await?.is_none(),
        "name row should be removed"
    );
    ensure!(
        store
            .list_oauth_refresh_terminal_failures()
            .await?
            .into_iter()
            .all(|failure| failure.upstream_id != record.id),
        "terminal refresh failure should be removed"
    );
    Ok(())
});

scenario!(validate_identifier_rejects_bad_name, |store| async move {
    let error = store
        .create(UpstreamCreate {
            name: "system.bad".to_owned(),
            kind: UpstreamKind::AnthropicOauth,
            base_url: None,
            api_key_ciphertext: None,
            oauth_token_generation: None,
            warmup_enabled: false,
            warmup_dialect_plugin: None,
        })
        .await
        .expect_err("reserved prefix should fail");
    ensure!(
        matches!(error, StorageError::InvalidInput { .. }),
        "expected InvalidInput"
    );
    Ok(())
});

async fn with_fixture<B, F, Fut>(backend: Arc<B>, scenario: F) -> Result<()>
where
    B: UpstreamStoreBackend,
    F: FnOnce(Arc<B::Store>) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let mut fixture = Fixture::new(backend).await?;
    let result = scenario(Arc::clone(&fixture.store)).await;
    let teardown = fixture.teardown().await;
    result?;
    teardown
}

async fn create_named(store: &dyn UpstreamStore, name: &str) -> Result<UpstreamRecord> {
    Ok(store
        .create(UpstreamCreate {
            name: name.to_owned(),
            kind: UpstreamKind::AnthropicOauth,
            base_url: None,
            api_key_ciphertext: None,
            oauth_token_generation: None,
            warmup_enabled: false,
            warmup_dialect_plugin: None,
        })
        .await?)
}

fn token_bundle(access_token: &str, refresh_token: &str) -> OAuthTokenBundle {
    OAuthTokenBundle {
        access_token: access_token.to_owned(),
        refresh_token: refresh_token.to_owned(),
        expires_at_unix_secs: 1_900_000_000,
        refresh_token_expires_at_unix_secs: None,
        scopes: vec!["org:profile".to_owned()],
    }
}

fn url(value: &str) -> Result<Url> {
    Ok(value.parse()?)
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "postgres")]
    use super::*;

    #[cfg(feature = "postgres")]
    mod postgres_backend {
        use std::str::FromStr;

        use cc_lb_storage_api::{BackendKind, MetaStore};
        use cc_lb_storage_postgres::PostgresStorage;
        use sqlx::{
            AssertSqlSafe, PgPool,
            postgres::{PgConnectOptions, PgPoolOptions},
        };

        use super::*;

        struct PostgresBackend;
        struct PostgresFixture {
            url: String,
            schema: String,
            pool: PgPool,
        }

        #[async_trait]
        impl UpstreamStoreBackend for PostgresBackend {
            type Store = PostgresStorage;
            type Fixture = PostgresFixture;

            async fn create_fixture(&self) -> Result<Self::Fixture> {
                let Some(url) = std::env::var("CI_POSTGRES_URL")
                    .ok()
                    .or_else(|| std::env::var("DATABASE_URL").ok())
                else {
                    anyhow::bail!("skip: CI_POSTGRES_URL or DATABASE_URL not set");
                };
                let schema = format!("upstream_store_{}", Uuid::new_v4().simple());
                let admin_pool = PgPoolOptions::new()
                    .max_connections(1)
                    .connect(&url)
                    .await?;
                sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
                    .execute(&admin_pool)
                    .await?;
                admin_pool.close().await;
                let pool = PgPoolOptions::new()
                    .max_connections(4)
                    .connect_with(
                        PgConnectOptions::from_str(&url)?
                            .options([("search_path", schema.as_str())]),
                    )
                    .await?;
                let storage = PostgresStorage::new(
                    pool.clone(),
                    std::sync::Arc::new(cc_lb_engine::SystemClock),
                );
                MetaStore::initialize(&storage, BackendKind::Postgres).await?;
                Ok(PostgresFixture { url, schema, pool })
            }

            async fn open(&self, fixture: &Self::Fixture) -> Result<Self::Store> {
                Ok(PostgresStorage::new(
                    fixture.pool.clone(),
                    std::sync::Arc::new(cc_lb_engine::SystemClock),
                ))
            }

            async fn teardown(&self, fixture: Self::Fixture) -> Result<()> {
                fixture.pool.close().await;
                let admin_pool = PgPoolOptions::new()
                    .max_connections(1)
                    .connect(&fixture.url)
                    .await?;
                sqlx::query(AssertSqlSafe(format!(
                    "DROP SCHEMA IF EXISTS {} CASCADE",
                    fixture.schema
                )))
                .execute(&admin_pool)
                .await?;
                admin_pool.close().await;
                Ok(())
            }
        }

        #[tokio::test]
        async fn upstream_store_postgres() -> Result<()> {
            match run_all(Arc::new(PostgresBackend)).await {
                Err(error) if error.to_string().contains("skip:") => {
                    eprintln!("{error}");
                    Ok(())
                }
                result => result,
            }
        }
    }
}
