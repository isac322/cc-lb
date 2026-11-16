use std::{error::Error, str::FromStr, sync::Arc, time::Duration};

use cc_lb_storage_api::{
    AccountOwnerBinding, AccountOwnerStore, IssueParams, ManagedKeyStore, MetaStore, PrincipalCreate,
    PrincipalKind, PrincipalStore,
};
use sqlx::{AssertSqlSafe, PgPool, postgres::{PgConnectOptions, PgPoolOptions}};

use super::*;
use crate::adapter::{managed_keys::PostgresManagedKeyStore, retry};

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

#[tokio::test]
async fn renewal_preserves_access_until_expiry_switch_fences_old_workspace_and_revoke_is_terminal() -> TestResult {
    let fixture = Fixture::create().await?;
    let result: TestResult = async {
        let mut initial = fixture.family("family-a");
        initial.plan_type = "plus".into();
        fixture.storage.issue_session(issue(&initial, 1, 2)).await?;
        assert!(fixture.storage.lookup_session_credential(&[99; 32]).await?.is_none());
        let auth = fixture.storage.lookup_session_credential(&[1; 32]).await?.ok_or("missing access")?;
        assert_eq!(auth.family, initial);
        assert_eq!(auth.credential.purpose, CredentialPurpose::SessionAccess);
        assert_eq!(auth.parent_key.label, "parent-label");
        assert!(auth.principal_enabled);
        assert_eq!(fixture.storage.get_session_family("family-a").await?, Some(initial.clone()));
        assert!(fixture.storage.get_session_family("missing").await?.is_none());

        let renewed = fixture.storage.replace_session_access(replace(&initial, "credential-2", 3, 105, None), 105).await?;
        assert_eq!(renewed.access_generation, 2);
        assert_eq!(renewed.plan_type, "plus");
        fixture.storage.fence_session_dispatch(&fixture.principal_id, &snapshot(&initial, 1), 106).await?;
        assert!(matches!(fixture.storage.fence_session_dispatch(&fixture.principal_id, &snapshot(&initial, 1), 110).await,
            Err(SessionStoreError::CredentialRejected)));
        // A still-live renewal recovers after the initial access expired exactly at exp.
        let renewed_again = fixture.storage.replace_session_access(replace(&renewed, "credential-2", 4, 111, None), 111).await?;
        fixture.storage.fence_session_dispatch(&fixture.principal_id, &snapshot(&renewed, 3), 112).await?;

        let switched = fixture.storage.replace_session_access(
            replace(&renewed_again, "credential-4", 5, 112, Some("workspace-b")), 112,
        ).await?;
        assert_eq!((switched.workspace_generation, switched.access_generation), (2, 4));
        assert_eq!(switched.workspace_id, "workspace-b");
        assert_eq!(switched.plan_type, "unknown");
        for seed in [1, 3, 4] {
            let old = fixture.storage.lookup_session_credential(&[seed; 32]).await?.ok_or("missing old access")?;
            assert_eq!(old.credential.revoked_at, Some(112));
        }
        assert!(fixture.storage.fence_session_dispatch(&fixture.principal_id, &snapshot(&renewed_again, 4), 113).await.is_err());
        let renewal = fixture.storage.lookup_session_credential(&[2; 32]).await?.ok_or("missing original renewal")?;
        assert_eq!(renewal.credential.workspace_generation, 1);
        assert_eq!(renewal.credential.revoked_at, None);
        assert_eq!(renewal.family.workspace_id, "workspace-b");
        assert_eq!(renewal.family.plan_type, "unknown");
        let selected = fixture.storage.replace_session_access(replace(&switched, "credential-2", 6, 113, None), 113).await?;
        assert_eq!(selected.workspace_id, "workspace-b");
        assert_eq!(selected.plan_type, "unknown");
        assert_eq!(fixture.storage.get_session_family("family-a").await?.ok_or("missing selected family")?.plan_type, "unknown");
        assert!(matches!(fixture.storage.replace_session_access(replace(&renewed_again, "credential-2", 7, 113, None), 113).await,
            Err(SessionStoreError::OwnerConflict)));
        assert!(fixture.storage.lookup_session_credential(&[7; 32]).await?.is_none());

        let independent = fixture.family("family-b");
        fixture.storage.issue_session(issue(&independent, 10, 11)).await?;
        // Relay validation owns its credential row. The internal helper only fences
        // shared authority and does not require an access record for a relay ID.
        let mut relay = snapshot(&selected, 6);
        relay.credential_id = "separately-validated-relay".into();
        relay.purpose = CredentialPurpose::RemoteRelay;
        let mut tx = fixture.storage.pool.begin().await?;
        validate_session_family_in_tx(&mut tx, &fixture.principal_id, &relay, 114).await?;
        tx.rollback().await?;
        assert!(matches!(fixture.storage.fence_session_dispatch(&fixture.principal_id, &relay, 114).await,
            Err(SessionStoreError::CredentialRejected)));
        fixture.storage.revoke_session_family("family-a", "credential-2", 114).await?;
        assert!(matches!(fixture.storage.revoke_session_family("family-a", "credential-2", 115).await,
            Err(SessionStoreError::CredentialRejected)));
        assert!(matches!(fixture.storage.fence_session_dispatch(&fixture.principal_id, &snapshot(&selected, 6), 115).await,
            Err(SessionStoreError::CredentialRejected)));
        let mut tx = fixture.storage.pool.begin().await?;
        assert!(matches!(validate_session_family_in_tx(&mut tx, &fixture.principal_id, &relay, 115).await,
            Err(SessionStoreError::CredentialRejected)));
        tx.rollback().await?;
        fixture.storage.fence_session_dispatch(&fixture.principal_id, &snapshot(&independent, 10), 105).await?;
        Ok(())
    }.await;
    let teardown = fixture.drop_schema().await;
    result?;
    teardown
}

#[tokio::test]
async fn deleted_or_missing_principal_is_not_a_live_disabled_principal() -> TestResult {
    let fixture = Fixture::create().await?;
    let result: TestResult = async {
        let family = fixture.family("principal-lineage");
        fixture.storage.issue_session(issue(&family, 1, 2)).await?;
        sqlx::query("UPDATE principals_v1 SET enabled = FALSE WHERE id = $1")
            .bind(fixture.principal_uuid).execute(&fixture.storage.pool).await?;
        let disabled = fixture.storage.lookup_session_credential(&[1; 32]).await?.ok_or("missing live disabled principal")?;
        assert!(!disabled.principal_enabled);
        assert!(matches!(fixture.storage.fence_session_dispatch(&fixture.principal_id, &snapshot(&family, 1), 105).await,
            Err(SessionStoreError::PrincipalDisabled)));

        // Deletion wins even when the deleted row also has enabled = FALSE.
        sqlx::query("UPDATE principals_v1 SET deleted_at = NOW() WHERE id = $1")
            .bind(fixture.principal_uuid).execute(&fixture.storage.pool).await?;
        assert!(fixture.storage.lookup_session_credential(&[1; 32]).await?.is_none());
        assert!(fixture.storage.lookup_session_credential(&[2; 32]).await?.is_none());
        assert!(matches!(fixture.storage.fence_session_dispatch(&fixture.principal_id, &snapshot(&family, 1), 105).await,
            Err(SessionStoreError::CredentialRejected)));
        assert!(PrincipalStore::hard_delete(&fixture.storage, fixture.principal_uuid).await?);
        let remaining_parent: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM managed_api_keys_v1 WHERE principal_id = $1 AND key_id = 'parent'",
        ).bind(&fixture.principal_id).fetch_one(&fixture.storage.pool).await?;
        assert_eq!(remaining_parent, 1);
        assert_eq!(fixture.storage.get_session_family("principal-lineage").await?, Some(family.clone()));
        assert!(fixture.storage.lookup_session_credential(&[1; 32]).await?.is_none());
        assert!(fixture.storage.lookup_session_credential(&[2; 32]).await?.is_none());
        assert!(matches!(fixture.storage.fence_session_dispatch(&fixture.principal_id, &snapshot(&family, 1), 105).await,
            Err(SessionStoreError::CredentialRejected)));
        Ok(())
    }.await;
    let teardown = fixture.drop_schema().await;
    result?;
    teardown
}

#[tokio::test]
async fn point_auth_lookup_does_not_discover_owner_and_live_fences_preserve_typed_failures() -> TestResult {
    let fixture = Fixture::create().await?;
    let result: TestResult = async {
        let family = fixture.family("authority");
        fixture.storage.issue_session(issue(&family, 1, 2)).await?;
        let access = snapshot(&family, 1);
        sqlx::query("UPDATE upstream_spec_v1 SET enabled = FALSE WHERE id = $1")
            .bind(fixture.upstream_id).execute(&fixture.storage.pool).await?;
        assert!(fixture.storage.lookup_session_credential(&[1; 32]).await?.is_some());
        assert_eq!(fixture.storage.get_session_family("authority").await?, Some(family.clone()));
        assert!(matches!(fixture.storage.fence_session_dispatch(&fixture.principal_id, &access, 105).await,
            Err(SessionStoreError::Storage(StorageError::Unavailable { .. }))));
        sqlx::query("UPDATE upstream_spec_v1 SET enabled = TRUE WHERE id = $1")
            .bind(fixture.upstream_id).execute(&fixture.storage.pool).await?;
        sqlx::query("UPDATE account_owner_bindings_v1 SET revision = revision + 1 WHERE binding_id = 'binding'")
            .execute(&fixture.storage.pool).await?;
        assert!(matches!(fixture.storage.fence_session_dispatch(&fixture.principal_id, &access, 105).await,
            Err(SessionStoreError::OwnerConflict)));
        sqlx::query("UPDATE account_owner_bindings_v1 SET revision = 1 WHERE binding_id = 'binding'")
            .execute(&fixture.storage.pool).await?;
        sqlx::query("UPDATE principals_v1 SET enabled = FALSE WHERE id = $1")
            .bind(fixture.principal_uuid).execute(&fixture.storage.pool).await?;
        let auth = fixture.storage.lookup_session_credential(&[1; 32]).await?.ok_or("missing disabled principal lookup")?;
        assert!(!auth.principal_enabled);
        assert!(matches!(fixture.storage.fence_session_dispatch(&fixture.principal_id, &access, 105).await,
            Err(SessionStoreError::PrincipalDisabled)));
        sqlx::query("UPDATE principals_v1 SET enabled = TRUE WHERE id = $1")
            .bind(fixture.principal_uuid).execute(&fixture.storage.pool).await?;
        sqlx::query("UPDATE managed_api_keys_v1 SET expires_at_unix_secs = 105 WHERE principal_id = $1 AND key_id = 'parent'")
            .bind(&fixture.principal_id).execute(&fixture.storage.pool).await?;
        // Ordinary parent-key semantics remain now > exp; session exp remains now >= exp.
        fixture.storage.fence_session_dispatch(&fixture.principal_id, &access, 105).await?;
        assert!(matches!(fixture.storage.fence_session_dispatch(&fixture.principal_id, &access, 106).await,
            Err(SessionStoreError::CredentialRejected)));
        sqlx::query("DELETE FROM managed_api_key_index_v1 WHERE principal_id = $1")
            .bind(&fixture.principal_id).execute(&fixture.storage.pool).await?;
        assert!(fixture.storage.lookup_session_credential(&[1; 32]).await?.is_none());
        assert!(matches!(fixture.storage.fence_session_dispatch(&fixture.principal_id, &access, 105).await,
            Err(SessionStoreError::CredentialRejected)));
        Ok(())
    }.await;
    let teardown = fixture.drop_schema().await;
    result?;
    teardown
}

#[tokio::test]
async fn workspace_discovery_is_read_only_and_does_not_wait_on_writer_lock() -> TestResult {
    let fixture = Fixture::create().await?;
    let result: TestResult = async {
        let family = fixture.family("discovery-read-only");
        fixture.storage.issue_session(issue(&family, 1, 2)).await?;
        let access = snapshot(&family, 1);
        let before: (String, i64) = sqlx::query_as(
            "SELECT allowed_workspaces_json, revision
             FROM account_owner_bindings_v1
             WHERE binding_id = 'binding'",
        )
        .fetch_one(&fixture.storage.pool)
        .await?;
        sqlx::query(
            "CREATE FUNCTION reject_discovery_writes() RETURNS trigger
             LANGUAGE plpgsql AS $$
             BEGIN
                 RAISE EXCEPTION 'workspace discovery must not write';
             END;
             $$",
        )
        .execute(&fixture.storage.pool)
        .await?;
        for table in [
            "codex_session_families_v1",
            "codex_session_credentials_v1",
            "account_owner_bindings_v1",
            "account_owner_workspaces_v1",
        ] {
            sqlx::query(AssertSqlSafe(format!(
                "CREATE TRIGGER reject_discovery_writes BEFORE INSERT OR UPDATE OR DELETE ON {table} \
                 FOR EACH ROW EXECUTE FUNCTION reject_discovery_writes()"
            )))
            .execute(&fixture.storage.pool)
            .await?;
        }

        let mut writer = fixture.storage.pool.begin().await?;
        sqlx::query(
            "SELECT binding_id
             FROM account_owner_bindings_v1
             WHERE binding_id = 'binding'
             FOR UPDATE",
        )
        .fetch_one(&mut *writer)
        .await?;


        let aliases = tokio::time::timeout(
            Duration::from_secs(1),
            fixture.storage.list_session_workspace_aliases(&fixture.principal_id, &access, 105),
        )
        .await??;
        assert_eq!(aliases, ["workspace-a", "workspace-b"]);
        writer.rollback().await?;

        let after: (String, i64) = sqlx::query_as(
            "SELECT allowed_workspaces_json, revision
             FROM account_owner_bindings_v1
             WHERE binding_id = 'binding'",
        )
        .fetch_one(&fixture.storage.pool)
        .await?;
        assert_eq!(after, before);
        Ok(())
    }
    .await;
    let teardown = fixture.drop_schema().await;
    result?;
    teardown
}

#[tokio::test]
async fn workspace_discovery_rejects_invalid_credentials_and_classifies_source_failures() -> TestResult {
    let fixture = Fixture::create().await?;
    let result: TestResult = async {
        let family = fixture.family("discovery-errors");
        fixture.storage.issue_session(issue(&family, 1, 2)).await?;
        let access = snapshot(&family, 1);

        let mut wrong_purpose = access.clone();
        wrong_purpose.purpose = CredentialPurpose::SessionRenewal;
        assert!(matches!(
            fixture.storage.list_session_workspace_aliases(&fixture.principal_id, &wrong_purpose, 105).await,
            Err(SessionStoreError::CredentialRejected)
        ));

        let mut missing_credential = access.clone();
        missing_credential.credential_id = "missing-credential".into();
        assert!(matches!(
            fixture.storage.list_session_workspace_aliases(&fixture.principal_id, &missing_credential, 105).await,
            Err(SessionStoreError::CredentialRejected)
        ));
        sqlx::query("UPDATE principals_v1 SET enabled = FALSE WHERE id = $1")
            .bind(fixture.principal_uuid)
            .execute(&fixture.storage.pool)
            .await?;
        assert!(matches!(
            fixture.storage.list_session_workspace_aliases(&fixture.principal_id, &access, 105).await,
            Err(SessionStoreError::PrincipalDisabled)
        ));
        sqlx::query("UPDATE principals_v1 SET enabled = TRUE WHERE id = $1")
            .bind(fixture.principal_uuid)
            .execute(&fixture.storage.pool)
            .await?;
        sqlx::query("UPDATE managed_api_keys_v1 SET status = 'revoked' WHERE principal_id = $1 AND key_id = 'parent'")
            .bind(&fixture.principal_id)
            .execute(&fixture.storage.pool)
            .await?;
        assert!(matches!(
            fixture.storage.list_session_workspace_aliases(&fixture.principal_id, &access, 105).await,
            Err(SessionStoreError::CredentialRejected)
        ));
        sqlx::query("UPDATE managed_api_keys_v1 SET status = 'active' WHERE principal_id = $1 AND key_id = 'parent'")
            .bind(&fixture.principal_id)
            .execute(&fixture.storage.pool)
            .await?;


        sqlx::query("UPDATE account_owner_bindings_v1 SET revision = 2 WHERE binding_id = 'binding'")
            .execute(&fixture.storage.pool)
            .await?;
        assert!(matches!(
            fixture.storage.list_session_workspace_aliases(&fixture.principal_id, &access, 105).await,
            Err(SessionStoreError::OwnerConflict)
        ));
        sqlx::query("UPDATE account_owner_bindings_v1 SET revision = 1 WHERE binding_id = 'binding'")
            .execute(&fixture.storage.pool)
            .await?;
        sqlx::query("UPDATE account_owner_bindings_v1 SET allowed_workspaces_json = '[\"workspace-b\"]' WHERE binding_id = 'binding'")
            .execute(&fixture.storage.pool)
            .await?;
        assert!(matches!(
            fixture.storage.list_session_workspace_aliases(&fixture.principal_id, &access, 105).await,
            Err(SessionStoreError::OwnerConflict)
        ));
        sqlx::query("UPDATE account_owner_bindings_v1 SET allowed_workspaces_json = '[\"workspace-a\", \"workspace-b\"]' WHERE binding_id = 'binding'")
            .execute(&fixture.storage.pool)
            .await?;


        sqlx::query("UPDATE upstream_spec_v1 SET enabled = FALSE WHERE id = $1")
            .bind(fixture.upstream_id)
            .execute(&fixture.storage.pool)
            .await?;
        assert!(matches!(
            fixture.storage.list_session_workspace_aliases(&fixture.principal_id, &access, 105).await,
            Err(SessionStoreError::Storage(StorageError::Unavailable { .. }))
        ));
        Ok(())
    }
    .await;
    let teardown = fixture.drop_schema().await;
    result?;
    teardown
}

#[tokio::test]
async fn workspace_discovery_uses_one_snapshot_across_binding_races() -> TestResult {
    let fixture = Fixture::create().await?;
    let result: TestResult = async {
        let family = fixture.family("discovery-snapshot-race");
        fixture.storage.issue_session(issue(&family, 1, 2)).await?;
        let access = snapshot(&family, 1);

        let mut writer = fixture.storage.pool.begin().await?;
        sqlx::query(
            "UPDATE account_owner_bindings_v1
             SET allowed_workspaces_json = '[\"workspace-a\"]', revision = revision + 1
             WHERE binding_id = 'binding'",
        )
        .execute(&mut *writer)
        .await?;

        let before_commit = fixture.storage.list_session_workspace_aliases(
            &fixture.principal_id,
            &access,
            105,
        ).await?;
        assert_eq!(before_commit, ["workspace-a", "workspace-b"]);
        writer.commit().await?;

        assert!(matches!(
            fixture.storage.list_session_workspace_aliases(&fixture.principal_id, &access, 105).await,
            Err(SessionStoreError::OwnerConflict)
        ));
        Ok(())
    }
    .await;
    let teardown = fixture.drop_schema().await;
    result?;
    teardown
}

#[tokio::test]
async fn workspace_discovery_rejects_stale_access_generation() -> TestResult {
    let fixture = Fixture::create().await?;
    let result: TestResult = async {
        let family = fixture.family("discovery-generation");
        fixture.storage.issue_session(issue(&family, 1, 2)).await?;
        let access = snapshot(&family, 1);
        sqlx::query(
            "UPDATE codex_session_families_v1
             SET access_generation = 2
             WHERE family_id = $1",
        )
        .bind(&family.family_id)
        .execute(&fixture.storage.pool)
        .await?;
        assert!(matches!(
            fixture.storage.list_session_workspace_aliases(&fixture.principal_id, &access, 105).await,
            Err(SessionStoreError::CredentialRejected)
        ));
        Ok(())
    }
    .await;
    let teardown = fixture.drop_schema().await;
    result?;
    teardown
}

#[tokio::test]
async fn issue_and_replace_failures_roll_back_all_rows_and_generation_changes() -> TestResult {
    let fixture = Fixture::create().await?;
    let result: TestResult = async {
        let family = fixture.family("original");
        fixture.storage.issue_session(issue(&family, 1, 2)).await?;
        let duplicate = fixture.family("duplicate");
        assert!(matches!(fixture.storage.issue_session(issue(&duplicate, 3, 2)).await,
            Err(SessionStoreError::Storage(StorageError::Conflict { .. }))));
        assert!(fixture.storage.get_session_family("duplicate").await?.is_none());
        assert!(fixture.storage.lookup_session_credential(&[3; 32]).await?.is_none());
        // A duplicate new access fails after the family update and old-workspace
        // revocation SQL; the transaction must roll both changes back.
        assert!(fixture.storage.replace_session_access(replace(&family, "credential-1", 2, 105, Some("workspace-b")), 105).await.is_err());
        assert_eq!(fixture.storage.get_session_family("original").await?, Some(family.clone()));
        let old = fixture.storage.lookup_session_credential(&[1; 32]).await?.ok_or("missing old access")?;
        assert_eq!(old.credential.revoked_at, None);
        fixture.storage.fence_session_dispatch(&fixture.principal_id, &snapshot(&family, 1), 106).await?;
        let mut expected = family.clone();
        expected.origin = "https://other.example".into();
        assert!(matches!(fixture.storage.replace_session_access(replace(&expected, "credential-2", 4, 105, None), 105).await,
            Err(SessionStoreError::OwnerConflict)));
        // Correct family with wrong purpose or another family's credential is never authority.
        assert!(matches!(fixture.storage.replace_session_access(replace(&family, "credential-1", 4, 105, None), 105).await,
            Err(SessionStoreError::CredentialRejected)));
        let another = fixture.family("another");
        fixture.storage.issue_session(issue(&another, 10, 11)).await?;
        assert!(matches!(fixture.storage.replace_session_access(replace(&family, "credential-11", 4, 105, None), 105).await,
            Err(SessionStoreError::CredentialRejected)));
        assert!(matches!(fixture.storage.revoke_session_family("original", "credential-11", 105).await,
            Err(SessionStoreError::CredentialRejected)));
        assert!(matches!(fixture.storage.replace_session_access(replace(&family, "credential-1", 4, 105, Some("not-permitted")), 105).await,
            Err(SessionStoreError::OwnerConflict)));
        let mut wrong_claims = replace(&family, "credential-2", 4, 105, None);
        wrong_claims.access.expires_at += 1;
        assert!(matches!(fixture.storage.replace_session_access(wrong_claims, 105).await,
            Err(SessionStoreError::InvalidOperation)));
        assert!(fixture.storage.lookup_session_credential(&[4; 32]).await?.is_none());
        Ok(())
    }.await;
    let teardown = fixture.drop_schema().await;
    result?;
    teardown
}

#[tokio::test]
async fn expiry_blocks_new_authority_but_does_not_block_internal_revoke_compensation() -> TestResult {
    let fixture = Fixture::create().await?;
    let result: TestResult = async {
        let family = fixture.family("expiry");
        fixture.storage.issue_session(issue(&family, 1, 2)).await?;
        let last = fixture.storage.replace_session_access(replace(&family, "credential-2", 3, 199, None), 199).await?;
        let auth = fixture.storage.lookup_session_credential(&[3; 32]).await?.ok_or("missing last access")?;
        assert_eq!(auth.credential.expires_at, 200);
        assert!(matches!(fixture.storage.replace_session_access(replace(&last, "credential-2", 4, 200, None), 200).await,
            Err(SessionStoreError::CredentialRejected)));
        fixture.storage.revoke_session_family("expiry", "credential-2", 200).await?;
        assert!(matches!(fixture.storage.revoke_session_family("expiry", "credential-2", 201).await,
            Err(SessionStoreError::CredentialRejected)));
        assert!(matches!(fixture.storage.fence_session_dispatch(&fixture.principal_id, &snapshot(&last, 3), 200).await,
            Err(SessionStoreError::CredentialRejected)));
        // Credential expiry is independently authoritative even when the family lives.
        let second = fixture.family("credential-expiry");
        fixture.storage.issue_session(issue(&second, 10, 11)).await?;
        sqlx::query("UPDATE codex_session_credentials_v1 SET expires_at = 105 WHERE credential_id = 'credential-11'")
            .execute(&fixture.storage.pool).await?;
        assert!(matches!(fixture.storage.replace_session_access(replace(&second, "credential-11", 12, 105, None), 105).await,
            Err(SessionStoreError::CredentialRejected)));
        fixture.storage.revoke_session_family("credential-expiry", "credential-11", 105).await?;
        Ok(())
    }.await;
    let teardown = fixture.drop_schema().await;
    result?;
    teardown
}

#[tokio::test]
async fn internal_revoke_compensates_after_owner_parent_and_principal_authority_withdrawal() -> TestResult {
    let fixture = Fixture::create().await?;
    let result: TestResult = async {
        let family = fixture.family("compensation");
        fixture.storage.issue_session(issue(&family, 1, 2)).await?;
        sqlx::query("UPDATE account_owner_bindings_v1 SET revoked_at_unix_secs = 105, revision = 2 WHERE binding_id = 'binding'")
            .execute(&fixture.storage.pool).await?;
        sqlx::query("UPDATE managed_api_keys_v1 SET status = 'revoked', revoked_at_unix_secs = 105 WHERE principal_id = $1")
            .bind(&fixture.principal_id).execute(&fixture.storage.pool).await?;
        sqlx::query("DELETE FROM managed_api_key_index_v1 WHERE principal_id = $1")
            .bind(&fixture.principal_id).execute(&fixture.storage.pool).await?;
        sqlx::query("UPDATE principals_v1 SET enabled = FALSE WHERE id = $1")
            .bind(fixture.principal_uuid).execute(&fixture.storage.pool).await?;
        sqlx::query("UPDATE upstream_spec_v1 SET enabled = FALSE WHERE id = $1")
            .bind(fixture.upstream_id).execute(&fixture.storage.pool).await?;
        fixture.storage.revoke_session_family("compensation", "credential-2", 106).await?;
        let stored = fixture.storage.get_session_family("compensation").await?.ok_or("missing revoked family")?;
        assert_eq!(stored.revoked_at, Some(106));
        let live_descendants: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM codex_session_credentials_v1 WHERE family_id = 'compensation' AND revoked_at IS NULL",
        ).fetch_one(&fixture.storage.pool).await?;
        assert_eq!(live_descendants, 0);
        assert!(matches!(fixture.storage.revoke_session_family("compensation", "credential-2", 107).await,
            Err(SessionStoreError::CredentialRejected)));
        Ok(())
    }.await;
    let teardown = fixture.drop_schema().await;
    result?;
    teardown
}

#[tokio::test]
async fn compensation_does_not_wait_for_upstream_binding_principal_or_parent_locks() -> TestResult {
    let fixture = Fixture::create().await?;
    let result: TestResult = async {
        let family = fixture.family("contraction-locks");
        fixture.storage.issue_session(issue(&family, 1, 2)).await?;
        let mut authority = fixture.storage.pool.begin().await?;
        validate_owner(&mut authority, &family).await?;
        lock_principal_and_parent(&mut authority, &family, 105).await?;
        sqlx::query("UPDATE account_owner_bindings_v1 SET revoked_at_unix_secs = 105, revision = 2 WHERE binding_id = 'binding'")
            .execute(&mut *authority).await?;
        sqlx::query("UPDATE managed_api_keys_v1 SET status = 'revoked', revoked_at_unix_secs = 105 WHERE principal_id = $1")
            .bind(&fixture.principal_id).execute(&mut *authority).await?;
        tokio::time::timeout(Duration::from_secs(5),
            fixture.storage.revoke_session_family("contraction-locks", "credential-2", 105),
        ).await??;
        authority.commit().await?;
        assert_eq!(fixture.storage.get_session_family("contraction-locks").await?.ok_or("missing family")?.revoked_at, Some(105));
        Ok(())
    }.await;
    let teardown = fixture.drop_schema().await;
    result?;
    teardown
}

#[tokio::test]
async fn revoke_races_with_renewal_and_duplicate_revoke_without_publishing_live_descendants() -> TestResult {
    let fixture = Fixture::create().await?;
    let result: TestResult = async {
        let family = fixture.family("renew-revoke-race");
        fixture.storage.issue_session(issue(&family, 1, 2)).await?;
        let (renewal, revocation) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(
                fixture.storage.replace_session_access(replace(&family, "credential-2", 3, 105, None), 105),
                fixture.storage.revoke_session_family("renew-revoke-race", "credential-2", 105),
            )
        }).await?;
        revocation?;
        match renewal {
            Ok(renewed) => {
                assert!(matches!(fixture.storage.fence_session_dispatch(&fixture.principal_id, &snapshot(&renewed, 3), 106).await,
                    Err(SessionStoreError::CredentialRejected)));
            }
            Err(SessionStoreError::CredentialRejected) => {}
            Err(error) => return Err(error.into()),
        }
        let live_descendants: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM codex_session_credentials_v1 WHERE family_id = 'renew-revoke-race' AND revoked_at IS NULL",
        ).fetch_one(&fixture.storage.pool).await?;
        assert_eq!(live_descendants, 0);

        let duplicate = fixture.family("duplicate-revoke");
        fixture.storage.issue_session(issue(&duplicate, 10, 11)).await?;
        let (first, second) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(
                fixture.storage.revoke_session_family("duplicate-revoke", "credential-11", 105),
                fixture.storage.revoke_session_family("duplicate-revoke", "credential-11", 105),
            )
        }).await?;
        assert!(matches!((first, second),
            (Ok(()), Err(SessionStoreError::CredentialRejected))
            | (Err(SessionStoreError::CredentialRejected), Ok(()))));
        Ok(())
    }.await;
    let teardown = fixture.drop_schema().await;
    result?;
    teardown
}

#[tokio::test]
async fn wrong_family_revoke_never_locks_another_familys_credential() -> TestResult {
    let fixture = Fixture::create().await?;
    let result: TestResult = async {
        let family = fixture.family("credential-scope");
        let other = fixture.family("other-credential-scope");
        fixture.storage.issue_session(issue(&family, 1, 2)).await?;
        fixture.storage.issue_session(issue(&other, 10, 11)).await?;
        let mut blocker = fixture.storage.pool.begin().await?;
        sqlx::query("SELECT credential_id FROM codex_session_credentials_v1 WHERE credential_id = 'credential-11' FOR UPDATE")
            .fetch_one(&mut *blocker).await?;
        let rejected = tokio::time::timeout(Duration::from_secs(5),
            fixture.storage.revoke_session_family("credential-scope", "credential-11", 105),
        ).await?;
        assert!(matches!(rejected, Err(SessionStoreError::CredentialRejected)));
        blocker.rollback().await?;
        assert_eq!(fixture.storage.get_session_family("credential-scope").await?, Some(family));
        Ok(())
    }.await;
    let teardown = fixture.drop_schema().await;
    result?;
    teardown
}

#[tokio::test]
async fn owner_revocation_wins_a_blocked_issue_and_upstream_is_locked_before_principal() -> TestResult {
    let fixture = Fixture::create().await?;
    let result: TestResult = async {
        let family = fixture.family("blocked-issue");
        let mut blocker = fixture.storage.pool.begin().await?;
        sqlx::query("SELECT id FROM upstream_spec_v1 WHERE id = $1 FOR UPDATE")
            .bind(fixture.upstream_id).fetch_one(&mut *blocker).await?;
        let issuance = fixture.storage.issue_session(issue(&family, 1, 2));
        tokio::pin!(issuance);
        assert!(tokio::time::timeout(Duration::from_millis(50), &mut issuance).await.is_err());
        // If issuance locked the principal before the upstream this NOWAIT would fail.
        let mut probe = fixture.storage.pool.begin().await?;
        sqlx::query("SELECT id FROM principals_v1 WHERE id = $1 FOR UPDATE NOWAIT")
            .bind(fixture.principal_uuid).fetch_one(&mut *probe).await?;
        probe.rollback().await?;
        sqlx::query("UPDATE account_owner_bindings_v1 SET revoked_at_unix_secs = 105, revision = 2 WHERE binding_id = 'binding'")
            .execute(&mut *blocker).await?;
        blocker.commit().await?;
        assert!(matches!(tokio::time::timeout(Duration::from_secs(5), &mut issuance).await?,
            Err(SessionStoreError::OwnerConflict)));
        assert!(fixture.storage.get_session_family("blocked-issue").await?.is_none());
        Ok(())
    }.await;
    let teardown = fixture.drop_schema().await;
    result?;
    teardown
}

#[tokio::test]
async fn principal_disable_wins_a_blocked_renewal() -> TestResult {
    let fixture = Fixture::create().await?;
    let result: TestResult = async {
        let family = fixture.family("principal-race");
        fixture.storage.issue_session(issue(&family, 1, 2)).await?;
        let mut blocker = fixture.storage.pool.begin().await?;
        sqlx::query("UPDATE principals_v1 SET enabled = FALSE WHERE id = $1")
            .bind(fixture.principal_uuid).execute(&mut *blocker).await?;
        let renewal = fixture.storage.replace_session_access(replace(&family, "credential-2", 3, 105, None), 105);
        tokio::pin!(renewal);
        assert!(tokio::time::timeout(Duration::from_millis(50), &mut renewal).await.is_err());
        blocker.commit().await?;
        assert!(matches!(tokio::time::timeout(Duration::from_secs(5), &mut renewal).await?, Err(SessionStoreError::PrincipalDisabled)));
        assert!(fixture.storage.lookup_session_credential(&[3; 32]).await?.is_none());
        Ok(())
    }.await;
    let teardown = fixture.drop_schema().await;
    result?;
    teardown
}

#[tokio::test]
async fn parent_revocation_wins_a_blocked_dispatch_fence() -> TestResult {
    let fixture = Fixture::create().await?;
    let result: TestResult = async {
        let family = fixture.family("parent-race");
        fixture.storage.issue_session(issue(&family, 1, 2)).await?;
        let mut blocker = fixture.storage.pool.begin().await?;
        sqlx::query("UPDATE managed_api_keys_v1 SET status = 'revoked', revoked_at_unix_secs = 105 WHERE principal_id = $1 AND key_id = 'parent'")
            .bind(&fixture.principal_id).execute(&mut *blocker).await?;
        sqlx::query("DELETE FROM managed_api_key_index_v1 WHERE principal_id = $1")
            .bind(&fixture.principal_id).execute(&mut *blocker).await?;
        let access = snapshot(&family, 1);
        let fence = fixture.storage.fence_session_dispatch(&fixture.principal_id, &access, 105);
        tokio::pin!(fence);
        assert!(tokio::time::timeout(Duration::from_millis(50), &mut fence).await.is_err());
        blocker.commit().await?;
        assert!(matches!(tokio::time::timeout(Duration::from_secs(5), &mut fence).await?, Err(SessionStoreError::CredentialRejected)));
        Ok(())
    }.await;
    let teardown = fixture.drop_schema().await;
    result?;
    teardown
}

#[tokio::test]
async fn family_revocation_wins_a_blocked_workspace_switch_without_publishing_access() -> TestResult {
    let fixture = Fixture::create().await?;
    let result: TestResult = async {
        let family = fixture.family("family-race");
        fixture.storage.issue_session(issue(&family, 1, 2)).await?;
        let mut blocker = fixture.storage.pool.begin().await?;
        sqlx::query("UPDATE codex_session_families_v1 SET revoked_at = 105 WHERE family_id = 'family-race'")
            .execute(&mut *blocker).await?;
        let switch = fixture.storage.replace_session_access(replace(&family, "credential-1", 3, 105, Some("workspace-b")), 105);
        tokio::pin!(switch);
        assert!(tokio::time::timeout(Duration::from_millis(50), &mut switch).await.is_err());
        blocker.commit().await?;
        assert!(matches!(tokio::time::timeout(Duration::from_secs(5), &mut switch).await?, Err(SessionStoreError::CredentialRejected)));
        assert!(fixture.storage.lookup_session_credential(&[3; 32]).await?.is_none());
        Ok(())
    }.await;
    let teardown = fixture.drop_schema().await;
    result?;
    teardown
}

#[tokio::test]
async fn cross_binding_switches_use_ordered_locks_and_renewal_follows_the_new_owner() -> TestResult {
    let fixture = Fixture::create().await?;
    let result: TestResult = async {
        let mut target = fixture.add_binding("binding-c", "workspace-c", "local-user").await?;
        target.owner_id = "replacement-provider-owner".into();
        target.owner_generation = 2;
        target.revision = 2;
        fixture.storage.compare_and_replace_owner_binding(target.clone(), 1).await?;
        assert_eq!(target.provider_workspace_id, "provider-workspace-c");
        let source_family = fixture.family("cross-source");
        let mut target_family = fixture.family("cross-target");
        target_family.owner_binding_id.clone_from(&target.binding_id);
        target_family.workspace_id = "workspace-c".into();
        target_family.owner_generation = target.owner_generation;
        target_family.binding_revision = target.revision;
        fixture.storage.issue_session(issue(&source_family, 1, 2)).await?;
        fixture.storage.issue_session(issue(&target_family, 10, 11)).await?;
        let mut to_target = replace(&source_family, "credential-1", 3, 105, Some("workspace-c"));
        to_target.target_owner = Some(binding_expectation(&target, "workspace-c"));
        let mut to_source = replace(&target_family, "credential-10", 12, 105, Some("workspace-a"));
        to_source.target_owner = Some(owner_expectation(&source_family));
        let (to_target, to_source) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(
                fixture.storage.replace_session_access(to_target, 105),
                fixture.storage.replace_session_access(to_source, 105),
            )
        }).await?;
        let selected = to_target?;
        let reversed = to_source?;
        assert_eq!(selected.owner_binding_id, "binding-c");
        assert_eq!(selected.workspace_id, "workspace-c");
        assert_eq!((selected.owner_generation, selected.binding_revision), (2, 2));
        assert_eq!(reversed.owner_binding_id, "binding");
        assert_eq!(reversed.workspace_id, "workspace-a");
        assert_eq!((selected.workspace_generation, selected.access_generation), (2, 2));
        let renewal = fixture.storage.lookup_session_credential(&[2; 32]).await?.ok_or("missing renewal")?;
        assert_eq!(renewal.family.owner_binding_id, "binding-c");
        assert_eq!(renewal.credential.workspace_generation, 1);
        let refreshed = fixture.storage.replace_session_access(replace(&selected, "credential-2", 4, 106, None), 106).await?;
        assert_eq!(refreshed.owner_binding_id, "binding-c");
        assert_eq!(refreshed.workspace_id, "workspace-c");
        fixture.storage.fence_session_dispatch(&fixture.principal_id, &snapshot(&selected, 3), 107).await?;
        assert!(fixture.storage.fence_session_dispatch(&fixture.principal_id, &snapshot(&source_family, 1), 107).await.is_err());
        Ok(())
    }.await;
    let teardown = fixture.drop_schema().await;
    result?;
    teardown
}

#[tokio::test]
async fn cross_binding_switch_rejects_different_local_identity_and_stale_target_revision() -> TestResult {
    let fixture = Fixture::create().await?;
    let result: TestResult = async {
        let other_user = fixture.add_binding("other-user", "workspace-other", "different-local-user").await?;
        let target = fixture.add_binding("binding-c", "workspace-c", "local-user").await?;
        let family = fixture.family("identity");
        fixture.storage.issue_session(issue(&family, 1, 2)).await?;
        let mut command = replace(&family, "credential-1", 3, 105, Some("workspace-other"));
        command.target_owner = Some(binding_expectation(&other_user, "workspace-other"));
        assert!(matches!(fixture.storage.replace_session_access(command, 105).await, Err(SessionStoreError::OwnerConflict)));
        let mut command = replace(&family, "credential-1", 3, 105, Some("workspace-c"));
        command.target_owner = Some(binding_expectation(&target, "workspace-c"));
        sqlx::query("UPDATE account_owner_bindings_v1 SET revision = 2 WHERE binding_id = 'binding-c'")
            .execute(&fixture.storage.pool).await?;
        assert!(matches!(fixture.storage.replace_session_access(command, 105).await, Err(SessionStoreError::OwnerConflict)));
        assert_eq!(fixture.storage.get_session_family("identity").await?, Some(family));
        assert!(fixture.storage.lookup_session_credential(&[3; 32]).await?.is_none());
        Ok(())
    }.await;
    let teardown = fixture.drop_schema().await;
    result?;
    teardown
}

#[tokio::test]
async fn target_owner_revocation_wins_a_blocked_cross_binding_switch() -> TestResult {
    let fixture = Fixture::create().await?;
    let result: TestResult = async {
        let target = fixture.add_binding("binding-c", "workspace-c", "local-user").await?;
        let family = fixture.family("target-race");
        fixture.storage.issue_session(issue(&family, 1, 2)).await?;
        let mut blocker = fixture.storage.pool.begin().await?;
        sqlx::query("SELECT id FROM upstream_spec_v1 WHERE id = $1 FOR UPDATE")
            .bind(target.upstream_id).fetch_one(&mut *blocker).await?;
        let mut command = replace(&family, "credential-1", 3, 105, Some("workspace-c"));
        command.target_owner = Some(binding_expectation(&target, "workspace-c"));
        let switch = fixture.storage.replace_session_access(command, 105);
        tokio::pin!(switch);
        assert!(tokio::time::timeout(Duration::from_millis(50), &mut switch).await.is_err());
        sqlx::query("UPDATE account_owner_bindings_v1 SET revoked_at_unix_secs = 105, revision = 2 WHERE binding_id = 'binding-c'")
            .execute(&mut *blocker).await?;
        blocker.commit().await?;
        assert!(matches!(tokio::time::timeout(Duration::from_secs(5), &mut switch).await?, Err(SessionStoreError::OwnerConflict)));
        assert_eq!(fixture.storage.get_session_family("target-race").await?, Some(family));
        assert!(fixture.storage.lookup_session_credential(&[3; 32]).await?.is_none());
        Ok(())
    }.await;
    let teardown = fixture.drop_schema().await;
    result?;
    teardown
}

#[tokio::test]
async fn concurrent_renew_generation_does_not_invalidate_older_live_access_fence() -> TestResult {
    let fixture = Fixture::create().await?;
    let result: TestResult = async {
        let family = fixture.family("renew-fence");
        fixture.storage.issue_session(issue(&family, 1, 2)).await?;
        let mut blocker = fixture.storage.pool.begin().await?;
        validate_owner(&mut blocker, &family).await?;
        lock_principal_and_parent(&mut blocker, &family, 105).await?;
        read_family(&mut blocker, &family.family_id, true).await?;
        let access = snapshot(&family, 1);
        let fence = fixture.storage.fence_session_dispatch(&fixture.principal_id, &access, 106);
        tokio::pin!(fence);
        assert!(tokio::time::timeout(Duration::from_millis(50), &mut fence).await.is_err());
        let mut renewed = family.clone();
        renewed.access_generation = 2;
        insert_credential(&mut blocker, &credential(&renewed, 3, CredentialPurpose::SessionAccess, 105)).await?;
        sqlx::query("UPDATE codex_session_families_v1 SET access_generation = 2 WHERE family_id = 'renew-fence'")
            .execute(&mut *blocker).await?;
        blocker.commit().await?;
        tokio::time::timeout(Duration::from_secs(5), &mut fence).await??;
        Ok(())
    }.await;
    let teardown = fixture.drop_schema().await;
    result?;
    teardown
}

fn binding_expectation(binding: &AccountOwnerBinding, workspace: &str) -> OwnerBindingExpectation {
    OwnerBindingExpectation {
        binding_id: binding.binding_id.clone(), principal_id: binding.principal_id.clone(),
        provider: binding.provider.clone(), owner_generation: binding.owner_generation,
        binding_revision: binding.revision, workspace_id: workspace.into(),
    }
}

#[tokio::test]
async fn issue_accepts_the_next_clock_second_but_rejects_future_or_expired_candidates() -> TestResult {
    let fixture = Fixture::create().await?;
    let result: TestResult = async {
        let minted = fixture.family("clock-boundary");
        let command = issue(&minted, 1, 2);
        fixture.clock.advance_secs(1);
        fixture.storage.issue_session(command).await?;
        assert_eq!(fixture.storage.get_session_family("clock-boundary").await?, Some(minted));
        let access = fixture.storage.lookup_session_credential(&[1; 32]).await?.ok_or("missing boundary access")?;
        assert_eq!((access.credential.issued_at, access.credential.expires_at), (100, 110));

        let mut future = fixture.family("future-issuance");
        future.created_at = 102;
        future.expires_at = 202;
        assert!(matches!(fixture.storage.issue_session(issue(&future, 3, 4)).await,
            Err(SessionStoreError::InvalidOperation)));
        assert!(fixture.storage.get_session_family("future-issuance").await?.is_none());
        fixture.clock.set_unix_secs(110);
        let expired_access = fixture.family("expired-at-issuance");
        assert!(matches!(fixture.storage.issue_session(issue(&expired_access, 5, 6)).await,
            Err(SessionStoreError::InvalidOperation)));
        assert!(fixture.storage.get_session_family("expired-at-issuance").await?.is_none());
        fixture.clock.set_unix_secs(200);
        let expired_family = fixture.family("family-expired-at-issuance");
        assert!(matches!(fixture.storage.issue_session(issue(&expired_family, 7, 8)).await,
            Err(SessionStoreError::InvalidOperation)));
        assert!(fixture.storage.get_session_family("family-expired-at-issuance").await?.is_none());
        Ok(())
    }.await;
    let teardown = fixture.drop_schema().await;
    result?;
    teardown
}

#[test]
fn issuance_rejects_inconsistent_ttls_ids_generations_purposes_and_timestamps() {
    let family = family("principal", "family");
    assert!(validate_issue(&issue(&family, 1, 2), 100).is_ok());
    let mut cases = Vec::new();
    let mut command = issue(&family, 1, 2); command.family.access_ttl_secs = 0; cases.push(command);
    let mut command = issue(&family, 1, 2); command.family.access_ttl_secs = 101; cases.push(command);
    let mut command = issue(&family, 1, 2); command.family.workspace_generation = 2; cases.push(command);
    let mut command = issue(&family, 1, 2); command.access.family_id = "other".into(); cases.push(command);
    let mut command = issue(&family, 1, 2); command.access.purpose = CredentialPurpose::SessionRenewal; cases.push(command);
    let mut command = issue(&family, 1, 2); command.access.expires_at += 1; cases.push(command);
    let mut command = issue(&family, 1, 2); command.access.issued_at += 1; cases.push(command);
    let mut command = issue(&family, 1, 2); command.renewal.expires_at -= 1; cases.push(command);
    let mut command = issue(&family, 1, 2); command.renewal.workspace_generation = 2; cases.push(command);
    let mut command = issue(&family, 1, 2); command.renewal.index_hash = command.access.index_hash; cases.push(command);
    let mut command = issue(&family, 1, 2); command.renewal.credential_id.clone_from(&command.access.credential_id); cases.push(command);
    for command in cases {
        assert!(matches!(validate_issue(&command, 100), Err(SessionStoreError::InvalidOperation)));
    }
}

fn family(principal_id: &str, family_id: &str) -> SessionFamily {
    SessionFamily {
        family_id: family_id.into(), principal_id: principal_id.into(), parent_key_id: "parent".into(),
        owner_binding_id: "binding".into(), provider: "openai".into(), owner_generation: 1,
        binding_revision: 1, workspace_id: "workspace-a".into(), workspace_generation: 1,
        access_generation: 1, local_user_id: "local-user".into(), plan_type: "unknown".into(),
        origin: "https://proxy.example".into(), access_ttl_secs: 10, created_at: 100,
        expires_at: 200, revoked_at: None,
    }
}

fn credential(family: &SessionFamily, seed: u8, purpose: CredentialPurpose, now: u64) -> SessionCredential {
    SessionCredential {
        credential_id: format!("credential-{seed}"), family_id: family.family_id.clone(), purpose,
        index_hash: [seed; 32], verify_hash: [seed.wrapping_add(50); 32], salt: [seed; 16],
        workspace_generation: family.workspace_generation, access_generation: family.access_generation,
        issued_at: now, expires_at: if purpose == CredentialPurpose::SessionRenewal {
            family.expires_at
        } else { (now + family.access_ttl_secs).min(family.expires_at) }, revoked_at: None,
    }
}

fn issue(family: &SessionFamily, access_seed: u8, renewal_seed: u8) -> IssueSession {
    IssueSession {
        family: family.clone(),
        access: credential(family, access_seed, CredentialPurpose::SessionAccess, family.created_at),
        renewal: credential(family, renewal_seed, CredentialPurpose::SessionRenewal, family.created_at),
    }
}

fn replace(family: &SessionFamily, authorizing: &str, seed: u8, now: u64, workspace: Option<&str>) -> ReplaceSessionAccess {
    let mut next = family.clone();
    next.access_generation += 1;
    if let Some(workspace) = workspace {
        next.workspace_id = workspace.into();
        next.workspace_generation += 1;
    }
    ReplaceSessionAccess {
        expected: family.clone(), authorizing_credential_id: authorizing.into(),
        access: credential(&next, seed, CredentialPurpose::SessionAccess, now),
        target_owner: workspace.map(|workspace| {
            let mut target = owner_expectation(family);
            target.workspace_id = workspace.into();
            target
        }),
    }
}

fn snapshot(family: &SessionFamily, seed: u8) -> SessionSnapshot {
    family_snapshot(family, &format!("credential-{seed}"), CredentialPurpose::SessionAccess)
}

struct Fixture {
    url: String,
    schema: String,
    storage: PostgresStorage,
    clock: Arc<cc_lb_clock::TestClock>,
    principal_uuid: Uuid,
    principal_id: String,
    upstream_id: Uuid,
}

impl Fixture {
    async fn create() -> TestResult<Self> {
        let url = std::env::var("CI_POSTGRES_URL").map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::NotFound, "CI_POSTGRES_URL must be set for PostgreSQL session tests")
        })?;
        let schema = format!("test_codex_sessions_{}", Uuid::new_v4().simple());
        let admin = PgPoolOptions::new().max_connections(1).connect_with(PgConnectOptions::from_str(&url)?).await?;
        sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}"))).execute(&admin).await?;
        admin.close().await;
        let pool = PgPoolOptions::new().max_connections(4)
            .connect_with(PgConnectOptions::from_str(&url)?.options([("search_path", schema.as_str())])).await?;
        let clock = Arc::new(cc_lb_clock::TestClock::new_at_secs(100));
        let storage = PostgresStorage::new(pool, clock.clone());
        let mut fixture = Self { url, schema, storage, clock, principal_uuid: Uuid::nil(), principal_id: String::new(), upstream_id: Uuid::new_v4() };
        let setup: TestResult = async {
            fixture.storage.initialize().await?;
            fixture.storage.verify_schema_current().await?;
            let principal = PrincipalStore::create(&fixture.storage, PrincipalCreate {
                name: "session-principal".into(), kind: PrincipalKind::Human, allowed_models: vec![],
                allowed_upstreams: vec![], default_limits: vec![], cache_keepalive: None,
            }, 100).await?;
            fixture.principal_uuid = principal.id;
            fixture.principal_id = principal.id.to_string();
            sqlx::query("INSERT INTO upstream_spec_v1 (id, name, kind, enabled, warmup_enabled, spec_revision, created_at, updated_at) VALUES ($1, 'session-upstream', 'openai_oauth', TRUE, FALSE, 1, NOW(), NOW())")
                .bind(fixture.upstream_id).execute(&fixture.storage.pool).await?;
            fixture.storage.create_owner_binding(AccountOwnerBinding {
                binding_id: "binding".into(), principal_id: fixture.principal_id.clone(), provider: "openai".into(),
                owner_id: "provider-owner".into(), upstream_id: fixture.upstream_id, owner_generation: 1,
                local_user_id: "local-user".into(), allowed_workspaces: vec!["workspace-a".into(), "workspace-b".into()],
                provider_workspace_id: "provider-workspace-a".into(),
                outbound_region: None, revision: 1, revoked_at_unix_secs: None,
            }).await?;
            let keys = PostgresManagedKeyStore::new(fixture.storage.pool.clone(), Arc::new(retry::RetryPolicy::default()), fixture.clock.clone());
            keys.issue(&fixture.principal_id, "parent", IssueParams {
                label: "parent-label".into(), description: None, expires_at_unix_secs: None,
                limit_overrides: vec![], secret_salt: [200; 16], verify_hash: [200; 32],
                last_4: "1234".into(), index_hash: [201; 32],
            }).await?;
            Ok(())
        }.await;
        if let Err(error) = setup {
            fixture.drop_schema().await?;
            return Err(error);
        }
        Ok(fixture)
    }

    fn family(&self, id: &str) -> SessionFamily { family(&self.principal_id, id) }

    async fn add_binding(&self, id: &str, workspace: &str, local_user: &str) -> TestResult<AccountOwnerBinding> {
        let upstream_id = Uuid::new_v4();
        sqlx::query("INSERT INTO upstream_spec_v1 (id, name, kind, enabled, warmup_enabled, spec_revision, created_at, updated_at) VALUES ($1, $2, 'openai_oauth', TRUE, FALSE, 1, NOW(), NOW())")
            .bind(upstream_id).bind(id).execute(&self.storage.pool).await?;
        let binding = AccountOwnerBinding {
            binding_id: id.into(), principal_id: self.principal_id.clone(), provider: "openai".into(),
            owner_id: format!("provider-owner-{id}"), upstream_id, owner_generation: 1,
            local_user_id: local_user.into(), allowed_workspaces: vec![workspace.into()],
            provider_workspace_id: format!("provider-{workspace}"), outbound_region: None,
            revision: 1, revoked_at_unix_secs: None,
        };
        self.storage.create_owner_binding(binding.clone()).await?;
        Ok(binding)
    }

    async fn drop_schema(self) -> TestResult {
        self.storage.pool.close().await;
        let admin: PgPool = PgPoolOptions::new().max_connections(1).connect_with(PgConnectOptions::from_str(&self.url)?).await?;
        sqlx::query(AssertSqlSafe(format!("DROP SCHEMA {} CASCADE", self.schema))).execute(&admin).await?;
        admin.close().await;
        Ok(())
    }
}
