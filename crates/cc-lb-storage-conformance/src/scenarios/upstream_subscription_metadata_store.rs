use std::sync::Arc;

use anyhow::{Result, ensure};
use cc_lb_aead::{AeadService, EncryptedOAuthTokens, OAuthTokenBundle};
use cc_lb_storage_api::{
    OAuthTokensCreate, UpstreamCreate, UpstreamKind, UpstreamStore,
    UpstreamSubscriptionMetadataRecord, UpstreamSubscriptionMetadataStore,
};
use uuid::Uuid;

use crate::harness::{ConformanceBackend, with_conformance_fixture};

pub async fn run_all<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: UpstreamSubscriptionMetadataStore,
{
    put_then_get(Arc::clone(&backend)).await?;
    update_overwrites(Arc::clone(&backend)).await?;
    list_returns_all_sorted(Arc::clone(&backend)).await?;
    conditional_oauth_write_rejects_replaced_and_deleted_credentials(Arc::clone(&backend)).await?;
    conditional_oauth_write_rejects_same_generation_metadata_publication(Arc::clone(&backend))
        .await?;
    missing_returns_none(backend).await
}

macro_rules! scenario {
    ($name:ident, $body:expr) => {
        pub async fn $name<B>(backend: Arc<B>) -> Result<()>
        where
            B: ConformanceBackend,
            B::Storage: UpstreamSubscriptionMetadataStore,
        {
            with_conformance_fixture(backend, $body).await
        }
    };
}

scenario!(put_then_get, |storage| async move {
    let record = record(upstream_id(2), Some("org-a"));
    storage.put_upstream_subscription_metadata(&record).await?;
    let loaded = storage
        .get_upstream_subscription_metadata(record.upstream_id)
        .await?;
    ensure!(loaded == Some(record), "record should round-trip");
    Ok(())
});

scenario!(update_overwrites, |storage| async move {
    let mut record = record(upstream_id(3), Some("org-a"));
    storage.put_upstream_subscription_metadata(&record).await?;
    record.organization_role = Some("admin".to_owned());
    record.workspace_role = Some("developer".to_owned());
    record.observed_at_unix_millis = 200;
    record.raw_roles = Some(r#"{"updated":true}"#.to_owned());
    storage.put_upstream_subscription_metadata(&record).await?;
    let loaded = storage
        .get_upstream_subscription_metadata(record.upstream_id)
        .await?;
    ensure!(loaded == Some(record), "new observation overwrites row");
    Ok(())
});

scenario!(list_returns_all_sorted, |storage| async move {
    let first = record(upstream_id(1), Some("org-a"));
    let second = record(upstream_id(2), Some("org-b"));
    storage.put_upstream_subscription_metadata(&second).await?;
    storage.put_upstream_subscription_metadata(&first).await?;
    let listed = storage.list_upstream_subscription_metadata().await?;
    ensure!(listed == [first, second], "list should sort by upstream id");
    Ok(())
});

scenario!(missing_returns_none, |storage| async move {
    ensure!(
        storage
            .get_upstream_subscription_metadata(upstream_id(99))
            .await?
            .is_none(),
        "unknown upstream should return none"
    );
    Ok(())
});

scenario!(
    conditional_oauth_write_rejects_replaced_and_deleted_credentials,
    |storage| async move {
        let id = upstream_id(101);
        let aead = AeadService::from_master_key([7; 32]);
        let token_bundle = |access_token: &str| OAuthTokenBundle {
            access_token: access_token.to_owned(),
            refresh_token: "synthetic-refresh".to_owned(),
            expires_at_unix_secs: 1_900_000_000,
            refresh_token_expires_at_unix_secs: None,
            account_id: Some("synthetic-metadata-account".to_owned()),
            project_id: None,
            oauth_profile: Default::default(),
            scopes: Vec::new(),
            never_refresh: false,
        };
        let tokens =
            EncryptedOAuthTokens::encrypt(&aead, &token_bundle("initial-access"), id.as_bytes())?;
        let original = storage
            .create(UpstreamCreate {
                id,
                name: "conditional-metadata".to_owned(),
                kind: UpstreamKind::OpenaiOauth,
                base_url: None,
                api_key_ciphertext: None,
                oauth_tokens: Some(OAuthTokensCreate {
                    tokens,
                    never_refresh: false,
                }),
                warmup_enabled: false,
                warmup_dialect_plugin: None,
                workspace_id: None,
                api_key_access: None,
            })
            .await?;
        let mut old = record(id, None);
        old.raw_bootstrap =
            Some(r#"{"oauth_account":{"account_name":"Old Poll Name"}}"#.to_owned());
        ensure!(
            storage
                .compare_and_put_upstream_subscription_metadata(
                    &old,
                    original.oauth_token_generation,
                    None,
                )
                .await?,
            "the active generation must accept metadata"
        );
        let mut competing = old.clone();
        competing.raw_bootstrap =
            Some(r#"{"oauth_account":{"account_name":"Competing Poll Name"}}"#.to_owned());
        ensure!(
            !storage
                .compare_and_put_upstream_subscription_metadata(
                    &competing,
                    original.oauth_token_generation,
                    None,
                )
                .await?,
            "the second absent-snapshot writer must not replace the first"
        );
        ensure!(storage.get_upstream_subscription_metadata(id).await? == Some(old.clone()));
        let replacement = EncryptedOAuthTokens::encrypt(
            &aead,
            &token_bundle("same-account-reconnect-access"),
            id.as_bytes(),
        )?;
        let current = storage
            .store_oauth_tokens(id, original.revision, replacement, false)
            .await?;
        ensure!(
            current.oauth_token_generation > original.oauth_token_generation,
            "reconnect must rotate generation"
        );
        let mut fresh = record(id, None);
        fresh.raw_bootstrap =
            Some(r#"{"oauth_account":{"account_name":"Fresh Reconnect Name"}}"#.to_owned());
        fresh.observed_at_unix_millis = 200;
        storage.put_upstream_subscription_metadata(&fresh).await?;
        old.observed_at_unix_millis = 300;
        ensure!(
            !storage
                .compare_and_put_upstream_subscription_metadata(
                    &old,
                    original.oauth_token_generation,
                    Some(&fresh),
                )
                .await?,
            "old generation must decline even if its response arrives later"
        );
        ensure!(
            storage.get_upstream_subscription_metadata(id).await? == Some(fresh.clone()),
            "fresh reconnect metadata must survive the stale write"
        );
        let expected_fresh = fresh.clone();
        fresh.observed_at_unix_millis = 201;
        ensure!(
            storage
                .compare_and_put_upstream_subscription_metadata(
                    &fresh,
                    current.oauth_token_generation,
                    Some(&expected_fresh),
                )
                .await?,
            "current generation must still succeed"
        );
        ensure!(
            storage.get_upstream_subscription_metadata(id).await? == Some(fresh.clone()),
            "an unchanged snapshot must allow publishing an updated record"
        );
        storage.soft_delete(id, current.revision).await?;
        ensure!(
            !storage
                .compare_and_put_upstream_subscription_metadata(
                    &old,
                    current.oauth_token_generation,
                    Some(&fresh),
                )
                .await?,
            "a deleted upstream must decline even when its generation matches"
        );
        ensure!(
            storage.get_upstream_subscription_metadata(id).await? == Some(fresh),
            "declining a deleted upstream must not alter metadata"
        );
        let absent = record(upstream_id(102), None);
        ensure!(
            !storage
                .compare_and_put_upstream_subscription_metadata(&absent, 0, None)
                .await?,
            "a missing upstream must decline without inserting a row"
        );
        ensure!(
            storage
                .get_upstream_subscription_metadata(absent.upstream_id)
                .await?
                .is_none()
        );
        Ok(())
    }
);

scenario!(
    conditional_oauth_write_rejects_same_generation_metadata_publication,
    |storage| async move {
        for metadata_present in [false, true] {
            let id = upstream_id(if metadata_present { 202 } else { 201 });
            let aead = AeadService::from_master_key([7; 32]);
            let bundle = OAuthTokenBundle {
                access_token: "already-reconnected-synthetic-access".to_owned(),
                refresh_token: "synthetic-refresh".to_owned(),
                expires_at_unix_secs: 1_900_000_000,
                refresh_token_expires_at_unix_secs: None,
                account_id: Some("synthetic-metadata-account".to_owned()),
                project_id: None,
                oauth_profile: Default::default(),
                scopes: Vec::new(),
                never_refresh: false,
            };
            let tokens = EncryptedOAuthTokens::encrypt(&aead, &bundle, id.as_bytes())?;
            let current = storage
                .create(UpstreamCreate {
                    id,
                    name: format!("metadata-publication-{metadata_present}"),
                    kind: UpstreamKind::OpenaiOauth,
                    base_url: None,
                    api_key_ciphertext: None,
                    oauth_tokens: Some(OAuthTokensCreate {
                        tokens,
                        never_refresh: false,
                    }),
                    warmup_enabled: false,
                    warmup_dialect_plugin: None,
                    workspace_id: None,
                    api_key_access: None,
                })
                .await?;
            let mut old = record(id, None);
            old.raw_bootstrap =
                Some(r#"{"oauth_account":{"account_name":"Old Poll Name"}}"#.to_owned());
            if metadata_present {
                storage.put_upstream_subscription_metadata(&old).await?;
            }
            let snapshot = storage.get_upstream_subscription_metadata(id).await?;
            ensure!(
                snapshot.is_some() == metadata_present,
                "capture the metadata visible before enrichment"
            );
            let mut fresh = record(id, None);
            fresh.raw_bootstrap =
                Some(r#"{"oauth_account":{"account_name":"Fresh Reconnect Name"}}"#.to_owned());
            storage.put_upstream_subscription_metadata(&fresh).await?;
            ensure!(
                !storage
                    .compare_and_put_upstream_subscription_metadata(
                        &old,
                        current.oauth_token_generation,
                        snapshot.as_ref(),
                    )
                    .await?,
                "a same-generation reconnect metadata publication must invalidate the poll snapshot"
            );
            ensure!(
                storage.get_upstream_subscription_metadata(id).await? == Some(fresh.clone()),
                "fresh metadata must survive whether the original snapshot was absent or present"
            );
            let mut changed_roles = fresh.clone();
            changed_roles.organization_role = Some("new-role".to_owned());
            storage
                .put_upstream_subscription_metadata(&changed_roles)
                .await?;
            ensure!(
                !storage
                    .compare_and_put_upstream_subscription_metadata(
                        &old,
                        current.oauth_token_generation,
                        Some(&fresh),
                    )
                    .await?,
                "compare the full semantic record, not only the bootstrap"
            );
            ensure!(storage.get_upstream_subscription_metadata(id).await? == Some(changed_roles));
        }
        Ok(())
    }
);

fn record(
    upstream_id: Uuid,
    organization_uuid: Option<&str>,
) -> UpstreamSubscriptionMetadataRecord {
    UpstreamSubscriptionMetadataRecord {
        upstream_id,
        organization_uuid: organization_uuid.map(str::to_owned),
        organization_role: Some("member".to_owned()),
        workspace_role: Some("user".to_owned()),
        observed_at_unix_millis: 100,
        last_error: None,
        raw_roles: Some(r#"{"organization_role":"member"}"#.to_owned()),
        raw_bootstrap: Some(r#"{"workspace":"default"}"#.to_owned()),
    }
}

fn upstream_id(value: u128) -> Uuid {
    Uuid::from_u128(value)
}
