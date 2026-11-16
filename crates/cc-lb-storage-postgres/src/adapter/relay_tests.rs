use std::{str::FromStr, sync::Arc};

use crate::adapter::{managed_keys::PostgresManagedKeyStore, retry};
use cc_lb_storage_api::{
    AccountOwnerBinding, AccountOwnerStore, IssueParams, ManagedKeyStore, MetaStore,
};
use cc_lb_storage_api::{
    KeyStatus, OwnerBindingExpectation,
    codex_sessions::{
        CodexSessionStore, IssueSession, ReplaceSessionAccess, SessionCredential, SessionFamily,
    },
};
use sqlx::{
    AssertSqlSafe,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use uuid::Uuid;

use super::*;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn family() -> SessionFamily {
    SessionFamily {
        family_id: "family".into(),
        principal_id: "00000000-0000-0000-0000-000000000002".into(),
        parent_key_id: "parent".into(),
        owner_binding_id: "binding".into(),
        provider: "openai".into(),
        owner_generation: 1,
        binding_revision: 1,
        workspace_id: "workspace".into(),
        workspace_generation: 1,
        access_generation: 1,
        local_user_id: "user".into(),
        plan_type: "unknown".into(),
        origin: "https://proxy.example.com".into(),
        access_ttl_secs: 100,
        created_at: 100,
        expires_at: 200,
        revoked_at: None,
    }
}

fn session_credential(
    f: &SessionFamily,
    seed: u8,
    purpose: CredentialPurpose,
    now: u64,
) -> SessionCredential {
    SessionCredential {
        credential_id: format!("session-{seed}"),
        family_id: f.family_id.clone(),
        purpose,
        index_hash: [seed; 32],
        verify_hash: [seed.wrapping_add(40); 32],
        salt: [seed; 16],
        workspace_generation: f.workspace_generation,
        access_generation: f.access_generation,
        issued_at: now,
        expires_at: (now + f.access_ttl_secs).min(f.expires_at),
        revoked_at: None,
    }
}

fn access(f: &SessionFamily, seed: u8) -> SessionSnapshot {
    let c = RelayCredential {
        credential_id: format!("session-{seed}"),
        enrollment_id: "unused".into(),
        identity: identity(),
        enrollment_generation: 1,
        index_hash: [seed; 32],
        verify_hash: [seed; 32],
        salt: [seed; 16],
        issued_at_unix_secs: 100,
        expires_at_unix_secs: 200,
    };
    let mut snapshot = RelayLineage::from_family(f).snapshot(&c);
    snapshot.purpose = CredentialPurpose::SessionAccess;
    snapshot
}

fn identity() -> RelayEnrollmentIdentity {
    RelayEnrollmentIdentity {
        family_id: "family".into(),
        server_id: "server".into(),
        environment_id: "environment".into(),
        installation_id: "installation".into(),
    }
}

fn grant(seed: u8, generation: u64, now: u64) -> IssueRelayEnrollment {
    IssueRelayEnrollment {
        principal_id: "00000000-0000-0000-0000-000000000002".into(),
        access: access(&family(), 1),
        enrollment_id: "enrollment".into(),
        identity: identity(),
        vendor_ciphertext: vec![seed; 24],
        provider_expires_at_unix_secs: 180,
        credential: RelayCredential {
            credential_id: format!("relay-{seed}"),
            enrollment_id: "enrollment".into(),
            identity: identity(),
            enrollment_generation: generation,
            index_hash: [seed; 32],
            verify_hash: [seed.wrapping_add(40); 32],
            salt: [seed; 16],
            issued_at_unix_secs: now,
            expires_at_unix_secs: 170,
        },
    }
}

async fn dispatch(
    store: &PostgresStorage,
    snapshot: &SessionSnapshot,
    now: u64,
) -> SessionStoreResult<RelayDispatch> {
    store
        .fence_relay_dispatch(
            "00000000-0000-0000-0000-000000000002",
            snapshot,
            &identity(),
            now,
        )
        .await
}

#[tokio::test]
async fn refresh_is_atomic_cas_and_unexpired_locals_share_current_ciphertext() -> TestResult {
    let Some(fixture) = Fixture::create().await? else {
        return Ok(());
    };
    let s = &fixture.storage;
    let result: TestResult = async {
        let initial = s.issue_relay_enrollment(grant(10, 1, 101), 101).await?;
        let old = s.lookup_relay_credential(&[10; 32]).await?.unwrap();
        let mut duplicate = grant(11, 2, 102);
        duplicate.credential.index_hash = [10; 32];
        assert!(matches!(
            s.refresh_relay_enrollment(
                RefreshRelayEnrollment {
                    grant: duplicate,
                    expected_generation: 1,
                },
                102
            )
            .await,
            Err(SessionStoreError::Storage(StorageError::Conflict { .. }))
        ));
        assert_eq!(
            dispatch(&s, &old.snapshot, 102).await?,
            RelayDispatch::Authorized(initial.clone())
        );
        assert!(s.lookup_relay_credential(&[11; 32]).await?.is_none());

        let (left, right) = tokio::join!(
            s.refresh_relay_enrollment(
                RefreshRelayEnrollment {
                    grant: grant(12, 2, 103),
                    expected_generation: 1
                },
                103
            ),
            s.refresh_relay_enrollment(
                RefreshRelayEnrollment {
                    grant: grant(13, 2, 103),
                    expected_generation: 1
                },
                103
            ),
        );
        let (current, loser) = match (left, right) {
            (Ok(current), Err(SessionStoreError::OwnerConflict)) => (current, 13),
            (Err(SessionStoreError::OwnerConflict), Ok(current)) => (current, 12),
            other => panic!("expected one atomic CAS winner: {other:?}"),
        };
        assert_eq!(current.generation, 2);
        assert_eq!(current.lineage, initial.lineage);
        assert!(s.lookup_relay_credential(&[loser; 32]).await?.is_none());
        let winner = s
            .lookup_relay_credential(&[current.vendor_ciphertext[0]; 32])
            .await?
            .unwrap();
        let old_after = s.lookup_relay_credential(&[10; 32]).await?.unwrap();
        assert_eq!(old_after.credential, old.credential);
        assert_eq!(old_after.snapshot, old.snapshot);
        for snapshot in [&old.snapshot, &winner.snapshot] {
            assert_eq!(
                dispatch(&s, snapshot, 104).await?,
                RelayDispatch::Authorized(current.clone())
            );
        }
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM codex_relay_credentials_v1")
            .fetch_one(&s.pool)
            .await?;
        assert_eq!(count, 2);
        Ok(())
    }
    .await;
    let teardown = fixture.drop_schema().await;
    result?;
    teardown
}

#[tokio::test]
async fn invalid_access_or_new_credential_never_mutates_enrollment() -> TestResult {
    let Some(fixture) = Fixture::create().await? else {
        return Ok(());
    };
    let s = &fixture.storage;
    let result: TestResult = async {
    let mut invalid = grant(10, 1, 101);
    invalid.access.credential_id = "missing".into();
    assert!(matches!(s.issue_relay_enrollment(invalid, 101).await, Err(SessionStoreError::CredentialRejected)));
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM codex_relay_enrollments_v1").fetch_one(&s.pool).await?;
    assert_eq!(count, 0);
    let initial = s.issue_relay_enrollment(grant(10, 1, 101), 101).await?;
    let old = s.lookup_relay_credential(&[10; 32]).await?.unwrap();
    for authorizer in ["missing", "session-2"] {
        let mut invalid = grant(11, 2, 102);
        invalid.access.credential_id = authorizer.into();
        assert!(matches!(s.refresh_relay_enrollment(RefreshRelayEnrollment {
            grant: invalid, expected_generation: 1,
        }, 102).await, Err(SessionStoreError::CredentialRejected)));
    }
    for purpose in [CredentialPurpose::SessionRenewal, CredentialPurpose::RemoteRelay] {
        let mut invalid = grant(11, 2, 102);
        invalid.access.purpose = purpose;
        assert!(matches!(s.refresh_relay_enrollment(RefreshRelayEnrollment {
            grant: invalid, expected_generation: 1,
        }, 102).await, Err(SessionStoreError::CredentialRejected)));
    }
    let mut invalid = grant(11, 2, 102);
    invalid.credential.expires_at_unix_secs = 102;
    assert!(matches!(s.refresh_relay_enrollment(RefreshRelayEnrollment {
        grant: invalid, expected_generation: 1,
    }, 102).await, Err(SessionStoreError::InvalidOperation)));
    let mut invalid = grant(11, 2, 102);
    invalid.provider_expires_at_unix_secs = 160;
    assert!(matches!(s.refresh_relay_enrollment(RefreshRelayEnrollment {
        grant: invalid, expected_generation: 1,
    }, 102).await, Err(SessionStoreError::InvalidOperation)));
    let mut invalid = grant(11, 2, 102);
    invalid.access.access_generation += 1;
    assert!(matches!(s.refresh_relay_enrollment(RefreshRelayEnrollment {
        grant: invalid, expected_generation: 1,
    }, 102).await, Err(SessionStoreError::CredentialRejected)));
    for withdrawal in [
        "UPDATE codex_session_credentials_v1 SET expires_at=102 WHERE credential_id='session-1'",
        "UPDATE codex_session_credentials_v1 SET expires_at=200, revoked_at=102 WHERE credential_id='session-1'",
    ] {
        sqlx::query(withdrawal).execute(&s.pool).await?;
        assert!(matches!(s.refresh_relay_enrollment(RefreshRelayEnrollment {
            grant: grant(11, 2, 102), expected_generation: 1,
        }, 102).await, Err(SessionStoreError::CredentialRejected)));
    }
    sqlx::query("UPDATE codex_session_credentials_v1 SET revoked_at=NULL WHERE credential_id='session-1'")
        .execute(&s.pool).await?;
    let mut foreign = family(); foreign.family_id = "foreign-family".into();
    s.issue_session(IssueSession {
        family: foreign.clone(), access: session_credential(&foreign, 4, CredentialPurpose::SessionAccess, 100),
        renewal: session_credential(&foreign, 5, CredentialPurpose::SessionRenewal, 100),
    }).await?;
    let mut invalid = grant(11, 2, 102); invalid.access.credential_id = "session-4".into();
    assert!(matches!(s.refresh_relay_enrollment(RefreshRelayEnrollment {
        grant: invalid, expected_generation: 1,
    }, 102).await, Err(SessionStoreError::CredentialRejected)));
    assert_eq!(dispatch(&s, &old.snapshot, 103).await?, RelayDispatch::Authorized(initial));
    assert!(s.lookup_relay_credential(&[11; 32]).await?.is_none());
    for purpose in [CredentialPurpose::SessionAccess, CredentialPurpose::SessionRenewal] {
        let mut wrong = old.snapshot.clone(); wrong.purpose = purpose;
        assert!(matches!(dispatch(&s, &wrong, 103).await, Err(SessionStoreError::CredentialRejected)));
    }
    Ok(())
    }.await;
    let teardown = fixture.drop_schema().await;
    result?;
    teardown
}

#[tokio::test]
async fn workspace_switch_and_rollback_cannot_revive_enrollment_or_refresh_its_stamps() -> TestResult
{
    let Some(fixture) = Fixture::create().await? else {
        return Ok(());
    };
    let s = &fixture.storage;
    let result: TestResult = async {
        s.issue_relay_enrollment(grant(10, 1, 101), 101).await?;
        let old = s.lookup_relay_credential(&[10; 32]).await?.unwrap();
        let mut selected = family();
        for (seed, workspace, now) in [(3, "other", 102), (4, "workspace", 103)] {
            let mut next = selected.clone();
            next.workspace_id = workspace.into();
            next.workspace_generation += 1;
            next.access_generation += 1;
            selected = s
                .replace_session_access(
                    ReplaceSessionAccess {
                        expected: selected.clone(),
                        authorizing_credential_id: format!(
                            "session-{}",
                            if seed == 3 { 1 } else { 3 }
                        ),
                        access: session_credential(
                            &next,
                            seed,
                            CredentialPurpose::SessionAccess,
                            now,
                        ),
                        target_owner: Some(OwnerBindingExpectation {
                            binding_id: "binding".into(),
                            principal_id: "00000000-0000-0000-0000-000000000002".into(),
                            provider: "openai".into(),
                            owner_generation: 1,
                            binding_revision: 1,
                            workspace_id: workspace.into(),
                        }),
                    },
                    now,
                )
                .await?;
            assert!(s.lookup_relay_credential(&[10; 32]).await?.is_none());
            assert!(matches!(
                dispatch(&s, &old.snapshot, now).await,
                Err(SessionStoreError::OwnerConflict)
            ));
        }
        let mut refresh = grant(11, 2, 104);
        refresh.access = access(&selected, 4);
        assert!(matches!(
            s.refresh_relay_enrollment(
                RefreshRelayEnrollment {
                    grant: refresh,
                    expected_generation: 1,
                },
                104
            )
            .await,
            Err(SessionStoreError::OwnerConflict)
        ));
        let mut reissue = grant(12, 1, 104);
        reissue.access = access(&selected, 4);
        assert!(matches!(
            s.issue_relay_enrollment(reissue, 104).await,
            Err(SessionStoreError::Storage(StorageError::Conflict { .. }))
        ));
        Ok(())
    }
    .await;
    let teardown = fixture.drop_schema().await;
    result?;
    teardown
}

#[tokio::test]
async fn renewal_preserves_original_stamps_and_header_never_reads_provider_material() -> TestResult
{
    let Some(fixture) = Fixture::create().await? else {
        return Ok(());
    };
    let s = &fixture.storage;
    let result: TestResult = async {
        let initial = s.issue_relay_enrollment(grant(10, 1, 101), 101).await?;
        let mut renewed = family();
        renewed.access_generation = 2;
        let renewed = s
            .replace_session_access(
                ReplaceSessionAccess {
                    expected: family(),
                    authorizing_credential_id: "session-2".into(),
                    access: session_credential(&renewed, 3, CredentialPurpose::SessionAccess, 102),
                    target_owner: None,
                },
                102,
            )
            .await?;
        let mut refresh = grant(11, 2, 103);
        refresh.access = access(&renewed, 3);
        let current = s
            .refresh_relay_enrollment(
                RefreshRelayEnrollment {
                    grant: refresh,
                    expected_generation: 1,
                },
                103,
            )
            .await?;
        assert_eq!(current.lineage, initial.lineage);
        assert_eq!(current.lineage.access_generation, 1);
        let row = sqlx::query(HEADER_SQL)
            .bind([10_u8; 32].as_slice())
            .fetch_one(&s.pool)
            .await?;
        assert!(row.try_get::<Vec<u8>, _>("vendor_ciphertext").is_err());
        assert!(
            row.try_get::<i64, _>("provider_expires_at_unix_secs")
                .is_err()
        );
        assert!(!HEADER_SQL.contains("account_owner_bindings"));
        sqlx::query("UPDATE upstream_spec_v1 SET enabled=FALSE")
            .execute(&s.pool)
            .await?;
        assert!(s.lookup_relay_credential(&[10; 32]).await?.is_some());
        let old = s.lookup_relay_credential(&[10; 32]).await?.unwrap();
        assert!(matches!(
            dispatch(&s, &old.snapshot, 104).await,
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
async fn expiry_equality_and_parent_family_principal_authority_are_fenced() -> TestResult {
    let Some(fixture) = Fixture::create().await? else {
        return Ok(());
    };
    let s = &fixture.storage;
    let result: TestResult = async {
        s.issue_relay_enrollment(grant(10, 1, 101), 101).await?;
        let old = s.lookup_relay_credential(&[10; 32]).await?.unwrap();
        // A shorter provider lifetime cannot rewrite an already issued local token.
        let mut shorter = grant(11, 2, 102);
        shorter.provider_expires_at_unix_secs = 150;
        shorter.credential.expires_at_unix_secs = 140;
        s.refresh_relay_enrollment(
            RefreshRelayEnrollment {
                grant: shorter,
                expected_generation: 1,
            },
            102,
        )
        .await?;
        assert!(matches!(
            dispatch(&s, &old.snapshot, 149).await?,
            RelayDispatch::Authorized(_)
        ));
        assert_eq!(
            dispatch(&s, &old.snapshot, 150).await?,
            RelayDispatch::ProviderExpired
        );
        assert!(s.lookup_relay_credential(&[10; 32]).await?.is_some());
        assert!(matches!(
            dispatch(&s, &old.snapshot, 170).await,
            Err(SessionStoreError::CredentialRejected)
        ));
        assert!(matches!(
            dispatch(&s, &old.snapshot, 200).await,
            Err(SessionStoreError::CredentialRejected)
        ));
        assert!(matches!(
            dispatch(&s, &old.snapshot, 100).await,
            Err(SessionStoreError::CredentialRejected)
        ));
        sqlx::query("UPDATE principals_v1 SET enabled=FALSE")
            .execute(&s.pool)
            .await?;
        assert!(
            !s.lookup_relay_credential(&[10; 32])
                .await?
                .unwrap()
                .principal_enabled
        );
        assert!(matches!(
            dispatch(&s, &old.snapshot, 102).await,
            Err(SessionStoreError::PrincipalDisabled)
        ));
        assert!(matches!(
            s.refresh_relay_enrollment(
                RefreshRelayEnrollment {
                    grant: grant(11, 2, 102),
                    expected_generation: 1
                },
                102
            )
            .await,
            Err(SessionStoreError::PrincipalDisabled)
        ));
        sqlx::query("UPDATE principals_v1 SET enabled=TRUE")
            .execute(&s.pool)
            .await?;
        sqlx::query("UPDATE managed_api_keys_v1 SET status='revoked', revoked_at_unix_secs=102")
            .execute(&s.pool)
            .await?;
        assert_eq!(
            s.lookup_relay_credential(&[10; 32])
                .await?
                .unwrap()
                .parent_key
                .status,
            KeyStatus::Revoked
        );
        assert!(matches!(
            dispatch(&s, &old.snapshot, 102).await,
            Err(SessionStoreError::CredentialRejected)
        ));
        assert!(matches!(
            s.refresh_relay_enrollment(
                RefreshRelayEnrollment {
                    grant: grant(11, 2, 102),
                    expected_generation: 1
                },
                102
            )
            .await,
            Err(SessionStoreError::CredentialRejected)
        ));
        sqlx::query("UPDATE managed_api_keys_v1 SET status='active', revoked_at_unix_secs=NULL")
            .execute(&s.pool)
            .await?;
        s.revoke_session_family("family", "session-2", 103).await?;
        assert!(matches!(
            dispatch(&s, &old.snapshot, 104).await,
            Err(SessionStoreError::CredentialRejected)
        ));
        assert!(matches!(
            s.refresh_relay_enrollment(
                RefreshRelayEnrollment {
                    grant: grant(11, 2, 104),
                    expected_generation: 1
                },
                104
            )
            .await,
            Err(SessionStoreError::CredentialRejected)
        ));
        assert!(s.lookup_relay_credential(&[12; 32]).await?.is_none());
        Ok(())
    }
    .await;
    let teardown = fixture.drop_schema().await;
    result?;
    teardown
}

#[tokio::test]
async fn generation_overflow_and_credential_expiry_beyond_family_are_atomic_failures() -> TestResult
{
    let Some(fixture) = Fixture::create().await? else {
        return Ok(());
    };
    let s = &fixture.storage;
    let result: TestResult = async {
        s.issue_relay_enrollment(grant(10, 1, 101), 101).await?;
        let mut invalid = grant(11, 2, 102);
        invalid.credential.expires_at_unix_secs = 201;
        invalid.provider_expires_at_unix_secs = 250;
        assert!(matches!(
            s.refresh_relay_enrollment(
                RefreshRelayEnrollment {
                    grant: invalid,
                    expected_generation: 1,
                },
                102
            )
            .await,
            Err(SessionStoreError::InvalidOperation)
        ));
        sqlx::query("UPDATE codex_relay_enrollments_v1 SET generation=9223372036854775807")
            .execute(&s.pool)
            .await?;
        let oversized = 1_u64 << 63;
        assert!(matches!(
            s.refresh_relay_enrollment(
                RefreshRelayEnrollment {
                    grant: grant(11, oversized, 102),
                    expected_generation: oversized - 1,
                },
                102
            )
            .await,
            Err(SessionStoreError::InvalidOperation)
        ));
        let current: (i64, Vec<u8>) =
            sqlx::query_as("SELECT generation, vendor_ciphertext FROM codex_relay_enrollments_v1")
                .fetch_one(&s.pool)
                .await?;
        assert_eq!(current, (i64::MAX, vec![10; 24]));
        assert!(s.lookup_relay_credential(&[11; 32]).await?.is_none());
        Ok(())
    }
    .await;
    let teardown = fixture.drop_schema().await;
    result?;
    teardown
}

struct Fixture {
    url: String,
    schema: String,
    storage: PostgresStorage,
}

impl Fixture {
    async fn create() -> TestResult<Option<Self>> {
        let Ok(url) = std::env::var("CI_POSTGRES_URL") else {
            eprintln!("skipped: CI_POSTGRES_URL unset; PostgreSQL relay assertions did not run");
            return Ok(None);
        };
        let schema = format!("test_codex_relay_{}", Uuid::new_v4().simple());
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(PgConnectOptions::from_str(&url)?)
            .await?;
        sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
            .execute(&admin)
            .await?;
        admin.close().await;
        let pool = PgPoolOptions::new()
            .max_connections(4)
            .connect_with(
                PgConnectOptions::from_str(&url)?.options([("search_path", schema.as_str())]),
            )
            .await?;
        let clock = Arc::new(cc_lb_clock::TestClock::new_at_secs(100));
        let storage = PostgresStorage::new(pool, clock.clone());
        let fixture = Self {
            url,
            schema,
            storage,
        };
        let setup: TestResult = async {
            fixture.storage.initialize().await?;
            fixture.storage.verify_schema_current().await?;
            sqlx::query("INSERT INTO principals_v1 (id,name,kind,allowed_models,default_limits,enabled,revision,created_at,updated_at) VALUES ('00000000-0000-0000-0000-000000000002','relay-principal','human','[]','[]',TRUE,1,NOW(),NOW())")
                .execute(&fixture.storage.pool).await?;
            let upstream_id = Uuid::from_u128(1);
            sqlx::query("INSERT INTO upstream_spec_v1 (id,name,kind,enabled,warmup_enabled,spec_revision,created_at,updated_at) VALUES ($1,'relay-owner','openai_oauth',TRUE,FALSE,1,NOW(),NOW())")
                .bind(upstream_id).execute(&fixture.storage.pool).await?;
            fixture.storage.create_owner_binding(AccountOwnerBinding {
                binding_id: "binding".into(), principal_id: "00000000-0000-0000-0000-000000000002".into(),
                provider: "openai".into(), owner_id: "owner".into(), upstream_id, owner_generation: 1,
                local_user_id: "user".into(), allowed_workspaces: vec!["workspace".into(), "other".into()],
                provider_workspace_id: "provider-workspace".into(), outbound_region: None,
                revision: 1, revoked_at_unix_secs: None,
            }).await?;
            let keys = PostgresManagedKeyStore::new(fixture.storage.pool.clone(), Arc::new(retry::RetryPolicy::default()), clock);
            keys.issue("00000000-0000-0000-0000-000000000002", "parent", IssueParams {
                label: "parent-label".into(), description: None, expires_at_unix_secs: None,
                limit_overrides: vec![], secret_salt: [200;16], verify_hash: [200;32],
                last_4: "1234".into(), index_hash: [201;32],
            }).await?;
            fixture.storage.issue_session(IssueSession {
                family: family(), access: session_credential(&family(), 1, CredentialPurpose::SessionAccess, 100),
                renewal: session_credential(&family(), 2, CredentialPurpose::SessionRenewal, 100),
            }).await?;
            Ok(())
        }.await;
        if let Err(error) = setup {
            fixture.drop_schema().await?;
            return Err(error);
        }
        Ok(Some(fixture))
    }

    async fn drop_schema(self) -> TestResult {
        self.storage.pool.close().await;
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(PgConnectOptions::from_str(&self.url)?)
            .await?;
        sqlx::query(AssertSqlSafe(format!(
            "DROP SCHEMA {} CASCADE",
            self.schema
        )))
        .execute(&admin)
        .await?;
        admin.close().await;
        Ok(())
    }
}

#[tokio::test]
async fn owner_revision_and_generation_changes_fence_without_affecting_header_lookup() -> TestResult
{
    let Some(fixture) = Fixture::create().await? else {
        return Ok(());
    };
    let s = &fixture.storage;
    let result: TestResult = async {
    let initial = s.issue_relay_enrollment(grant(10, 1, 101), 101).await?;
    let old = s.lookup_relay_credential(&[10; 32]).await?.unwrap();
    for change in [
        "UPDATE account_owner_bindings_v1 SET revision=2",
        "UPDATE account_owner_bindings_v1 SET owner_generation=2",
        "UPDATE account_owner_bindings_v1 SET revoked_at_unix_secs=102",
    ] {
        sqlx::query("UPDATE account_owner_bindings_v1 SET revision=1, owner_generation=1, revoked_at_unix_secs=NULL")
            .execute(&s.pool).await?;
        sqlx::query(change).execute(&s.pool).await?;
        assert!(s.lookup_relay_credential(&[10; 32]).await?.is_some());
        assert!(matches!(dispatch(&s, &old.snapshot, 102).await, Err(SessionStoreError::OwnerConflict)));
        assert!(matches!(s.refresh_relay_enrollment(RefreshRelayEnrollment {
            grant: grant(11, 2, 102), expected_generation: 1,
        }, 102).await, Err(SessionStoreError::OwnerConflict)));
        let stored: (i64, Vec<u8>) = sqlx::query_as(
            "SELECT generation, vendor_ciphertext FROM codex_relay_enrollments_v1",
        ).fetch_one(&s.pool).await?;
        assert_eq!(stored, (1, initial.vendor_ciphertext.clone()));
        assert!(s.lookup_relay_credential(&[11; 32]).await?.is_none());
    }
    Ok(())
    }.await;
    let teardown = fixture.drop_schema().await;
    result?;
    teardown
}
