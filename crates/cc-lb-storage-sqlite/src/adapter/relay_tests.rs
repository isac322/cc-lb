use cc_lb_storage_api::{
    KeyStatus, OwnerBindingExpectation,
    codex_sessions::{
        CodexSessionStore, IssueSession, ReplaceSessionAccess, SessionCredential, SessionFamily,
    },
};

use super::*;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

async fn store() -> SqliteStorage {
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    sqlx::migrate!("./migrations").run(&pool).await.unwrap();
    let store = SqliteStorage::new(
        pool,
        std::sync::Arc::new(cc_lb_clock::TestClock::new_at_secs(100)),
    );
    for statement in [
        "INSERT INTO principals_v1 (id,name,created_at,updated_at) VALUES ('principal','relay-principal',100,100)",
        "INSERT INTO upstream_spec_v1 (id,name,kind,created_at,updated_at) VALUES ('00000000-0000-0000-0000-000000000001','relay-owner','openai_oauth',100,100)",
        "INSERT INTO account_owner_bindings_v1 (binding_id,principal_id,provider,owner_id,upstream_id,owner_generation,local_user_id,allowed_workspaces_json,provider_workspace_id,revision) VALUES ('binding','principal','openai','owner','00000000-0000-0000-0000-000000000001',1,'user','[\"workspace\",\"other\"]','provider-workspace',1)",
        "INSERT INTO managed_keys_v1 (secret_hash,created_at,status,principal_id,key_id,label,verify_hash,secret_salt,limit_overrides,last_4,index_hash,updated_at) VALUES ('parent-hash',100,'active','principal','parent','parent-label',zeroblob(32),zeroblob(16),'[]','1234',zeroblob(32),100)",
    ] {
        sqlx::query(statement).execute(store.pool()).await.unwrap();
    }
    store
        .issue_session(IssueSession {
            family: family(),
            access: session_credential(&family(), 1, CredentialPurpose::SessionAccess, 100),
            renewal: session_credential(&family(), 2, CredentialPurpose::SessionRenewal, 100),
        })
        .await
        .unwrap();
    store
}

fn family() -> SessionFamily {
    SessionFamily {
        family_id: "family".into(),
        principal_id: "principal".into(),
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
        principal_id: "principal".into(),
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
    store: &SqliteStorage,
    snapshot: &SessionSnapshot,
    now: u64,
) -> SessionStoreResult<RelayDispatch> {
    store
        .fence_relay_dispatch("principal", snapshot, now)
        .await
}

#[tokio::test]
async fn refresh_is_atomic_cas_and_unexpired_locals_share_current_ciphertext() -> TestResult {
    let s = store().await;
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
        RelayDispatch::Authorized(Box::new(initial.clone()))
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
            RelayDispatch::Authorized(Box::new(current.clone()))
        );
    }
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM codex_relay_credentials_v1")
        .fetch_one(s.pool())
        .await?;
    assert_eq!(count, 2);
    Ok(())
}

#[tokio::test]
async fn invalid_access_or_new_credential_never_mutates_enrollment() -> TestResult {
    let s = store().await;
    let mut invalid = grant(10, 1, 101);
    invalid.access.credential_id = "missing".into();
    assert!(matches!(
        s.issue_relay_enrollment(invalid, 101).await,
        Err(SessionStoreError::CredentialRejected)
    ));
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM codex_relay_enrollments_v1")
        .fetch_one(s.pool())
        .await?;
    assert_eq!(count, 0);
    let initial = s.issue_relay_enrollment(grant(10, 1, 101), 101).await?;
    let old = s.lookup_relay_credential(&[10; 32]).await?.unwrap();
    for authorizer in ["missing", "session-2"] {
        let mut invalid = grant(11, 2, 102);
        invalid.access.credential_id = authorizer.into();
        assert!(matches!(
            s.refresh_relay_enrollment(
                RefreshRelayEnrollment {
                    grant: invalid,
                    expected_generation: 1,
                },
                102
            )
            .await,
            Err(SessionStoreError::CredentialRejected)
        ));
    }
    for purpose in [
        CredentialPurpose::SessionRenewal,
        CredentialPurpose::RemoteRelay,
    ] {
        let mut invalid = grant(11, 2, 102);
        invalid.access.purpose = purpose;
        assert!(matches!(
            s.refresh_relay_enrollment(
                RefreshRelayEnrollment {
                    grant: invalid,
                    expected_generation: 1,
                },
                102
            )
            .await,
            Err(SessionStoreError::CredentialRejected)
        ));
    }
    let mut invalid = grant(11, 2, 102);
    invalid.credential.expires_at_unix_secs = 102;
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
    let mut invalid = grant(11, 2, 102);
    invalid.provider_expires_at_unix_secs = 160;
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
    let mut invalid = grant(11, 2, 102);
    invalid.access.access_generation += 1;
    assert!(matches!(
        s.refresh_relay_enrollment(
            RefreshRelayEnrollment {
                grant: invalid,
                expected_generation: 1,
            },
            102
        )
        .await,
        Err(SessionStoreError::CredentialRejected)
    ));
    for withdrawal in [
        "UPDATE codex_session_credentials_v1 SET expires_at=102 WHERE credential_id='session-1'",
        "UPDATE codex_session_credentials_v1 SET expires_at=200, revoked_at=102 WHERE credential_id='session-1'",
    ] {
        sqlx::query(withdrawal).execute(s.pool()).await?;
        assert!(matches!(
            s.refresh_relay_enrollment(
                RefreshRelayEnrollment {
                    grant: grant(11, 2, 102),
                    expected_generation: 1,
                },
                102
            )
            .await,
            Err(SessionStoreError::CredentialRejected)
        ));
    }
    sqlx::query(
        "UPDATE codex_session_credentials_v1 SET revoked_at=NULL WHERE credential_id='session-1'",
    )
    .execute(s.pool())
    .await?;
    let mut foreign = family();
    foreign.family_id = "foreign-family".into();
    s.issue_session(IssueSession {
        family: foreign.clone(),
        access: session_credential(&foreign, 4, CredentialPurpose::SessionAccess, 100),
        renewal: session_credential(&foreign, 5, CredentialPurpose::SessionRenewal, 100),
    })
    .await?;
    let mut invalid = grant(11, 2, 102);
    invalid.access.credential_id = "session-4".into();
    assert!(matches!(
        s.refresh_relay_enrollment(
            RefreshRelayEnrollment {
                grant: invalid,
                expected_generation: 1,
            },
            102
        )
        .await,
        Err(SessionStoreError::CredentialRejected)
    ));
    assert_eq!(
        dispatch(&s, &old.snapshot, 103).await?,
        RelayDispatch::Authorized(Box::new(initial))
    );
    assert!(s.lookup_relay_credential(&[11; 32]).await?.is_none());
    for purpose in [
        CredentialPurpose::SessionAccess,
        CredentialPurpose::SessionRenewal,
    ] {
        let mut wrong = old.snapshot.clone();
        wrong.purpose = purpose;
        assert!(matches!(
            dispatch(&s, &wrong, 103).await,
            Err(SessionStoreError::CredentialRejected)
        ));
    }
    Ok(())
}

#[tokio::test]
async fn workspace_switch_and_rollback_cannot_revive_enrollment_or_refresh_its_stamps() -> TestResult
{
    let s = store().await;
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
                    authorizing_credential_id: format!("session-{}", if seed == 3 { 1 } else { 3 }),
                    access: session_credential(&next, seed, CredentialPurpose::SessionAccess, now),
                    target_owner: Some(OwnerBindingExpectation {
                        binding_id: "binding".into(),
                        principal_id: "principal".into(),
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

#[tokio::test]
async fn renewal_preserves_original_stamps_and_header_never_reads_provider_material() -> TestResult
{
    let s = store().await;
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
        .fetch_one(s.pool())
        .await?;
    assert!(row.try_get::<Vec<u8>, _>("vendor_ciphertext").is_err());
    assert!(
        row.try_get::<i64, _>("provider_expires_at_unix_secs")
            .is_err()
    );
    assert!(!HEADER_SQL.contains("account_owner_bindings"));
    sqlx::query("UPDATE upstream_spec_v1 SET enabled=0")
        .execute(s.pool())
        .await?;
    assert!(s.lookup_relay_credential(&[10; 32]).await?.is_some());
    let old = s.lookup_relay_credential(&[10; 32]).await?.unwrap();
    assert!(matches!(
        dispatch(&s, &old.snapshot, 104).await,
        Err(SessionStoreError::Storage(StorageError::Unavailable { .. }))
    ));
    Ok(())
}

#[tokio::test]
async fn expiry_equality_and_parent_family_principal_authority_are_fenced() -> TestResult {
    let s = store().await;
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
    sqlx::query("UPDATE principals_v1 SET enabled=0")
        .execute(s.pool())
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
    sqlx::query("UPDATE principals_v1 SET enabled=1")
        .execute(s.pool())
        .await?;
    sqlx::query("UPDATE managed_keys_v1 SET status='revoked', revoked_at=102")
        .execute(s.pool())
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
    sqlx::query("UPDATE managed_keys_v1 SET status='active', revoked_at=NULL")
        .execute(s.pool())
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

#[tokio::test]
async fn generation_overflow_and_credential_expiry_beyond_family_are_atomic_failures() -> TestResult
{
    let s = store().await;
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
        .execute(s.pool())
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
            .fetch_one(s.pool())
            .await?;
    assert_eq!(current, (i64::MAX, vec![10; 24]));
    assert!(s.lookup_relay_credential(&[11; 32]).await?.is_none());
    Ok(())
}

#[tokio::test]
async fn owner_revision_and_generation_changes_fence_without_affecting_header_lookup() -> TestResult
{
    let s = store().await;
    let initial = s.issue_relay_enrollment(grant(10, 1, 101), 101).await?;
    let old = s.lookup_relay_credential(&[10; 32]).await?.unwrap();
    for change in [
        "UPDATE account_owner_bindings_v1 SET revision=2",
        "UPDATE account_owner_bindings_v1 SET owner_generation=2",
        "UPDATE account_owner_bindings_v1 SET revoked_at_unix_secs=102",
    ] {
        sqlx::query("UPDATE account_owner_bindings_v1 SET revision=1, owner_generation=1, revoked_at_unix_secs=NULL")
            .execute(s.pool()).await?;
        sqlx::query(change).execute(s.pool()).await?;
        assert!(s.lookup_relay_credential(&[10; 32]).await?.is_some());
        assert!(matches!(
            dispatch(&s, &old.snapshot, 102).await,
            Err(SessionStoreError::OwnerConflict)
        ));
        assert!(matches!(
            s.refresh_relay_enrollment(
                RefreshRelayEnrollment {
                    grant: grant(11, 2, 102),
                    expected_generation: 1,
                },
                102
            )
            .await,
            Err(SessionStoreError::OwnerConflict)
        ));
        let stored: (i64, Vec<u8>) =
            sqlx::query_as("SELECT generation, vendor_ciphertext FROM codex_relay_enrollments_v1")
                .fetch_one(s.pool())
                .await?;
        assert_eq!(stored, (1, initial.vendor_ciphertext.clone()));
        assert!(s.lookup_relay_credential(&[11; 32]).await?.is_none());
    }
    Ok(())
}

#[tokio::test]
async fn access_lookup_returns_metadata_and_expired_provider_can_be_refreshed() -> TestResult {
    let s = store().await;
    let initial = s.issue_relay_enrollment(grant(10, 1, 101), 101).await?;
    let access = access(&family(), 1);
    let expected = RelayEnrollmentMetadata {
        enrollment_id: initial.enrollment_id.clone(),
        identity: initial.identity.clone(),
        lineage: initial.lineage.clone(),
        provider_expires_at_unix_secs: 180,
        generation: 1,
        updated_at_unix_secs: 101,
    };
    // An unreadable ciphertext column proves account lookup does not decode it.
    sqlx::query("UPDATE codex_relay_enrollments_v1 SET vendor_ciphertext='not-a-blob'")
        .execute(s.pool())
        .await?;
    for selector in [
        AccessRelaySelector::Server {
            server_id: "server".into(),
            installation_id: "installation".into(),
        },
        AccessRelaySelector::Environment { environment_id: "environment".into() },
    ] {
        assert_eq!(
            s.lookup_access_relay_enrollment("principal", &access, &selector, 180).await?,
            expected
        );
    }
    // Restore ciphertext for the separate refresh operation, whose contract reads it.
    sqlx::query("UPDATE codex_relay_enrollments_v1 SET vendor_ciphertext=?")
        .bind(initial.vendor_ciphertext)
        .execute(s.pool())
        .await?;
    let mut refreshed = grant(11, 2, 181);
    refreshed.provider_expires_at_unix_secs = 195;
    refreshed.credential.expires_at_unix_secs = 190;
    let current = s.refresh_relay_enrollment(
        RefreshRelayEnrollment { grant: refreshed, expected_generation: 1 },
        181,
    ).await?;
    assert_eq!(current.generation, 2);
    assert_eq!(current.provider_expires_at_unix_secs, 195);
    Ok(())
}

#[tokio::test]
async fn access_lookup_rejects_wrong_authority_selectors_and_ambiguous_environment() -> TestResult {
    let s = store().await;
    s.issue_relay_enrollment(grant(10, 1, 101), 101).await?;
    let valid = access(&family(), 1);
    let selector = AccessRelaySelector::Environment { environment_id: "environment".into() };
    let mut renewal = valid.clone();
    renewal.purpose = CredentialPurpose::SessionRenewal;
    let mut relay = valid.clone();
    relay.purpose = CredentialPurpose::RemoteRelay;
    let mut wrong_family = valid.clone();
    wrong_family.family_id = "other-family".into();
    let mut missing_credential = valid.clone();
    missing_credential.credential_id = "missing".into();
    for snapshot in [&renewal, &relay, &wrong_family, &missing_credential] {
        assert!(s.lookup_access_relay_enrollment("principal", snapshot, &selector, 102).await.is_err());
    }
    assert!(s.lookup_access_relay_enrollment("other-principal", &valid, &selector, 102).await.is_err());
    for selector in [
        AccessRelaySelector::Server { server_id: "other".into(), installation_id: "installation".into() },
        AccessRelaySelector::Server { server_id: "server".into(), installation_id: "other".into() },
        AccessRelaySelector::Environment { environment_id: "other".into() },
    ] {
        assert!(matches!(
            s.lookup_access_relay_enrollment("principal", &valid, &selector, 102).await,
            Err(SessionStoreError::OwnerConflict)
        ));
    }
    for selector in [
        AccessRelaySelector::Server { server_id: String::new(), installation_id: "installation".into() },
        AccessRelaySelector::Server { server_id: "server".into(), installation_id: String::new() },
        AccessRelaySelector::Environment { environment_id: String::new() },
    ] {
        assert!(matches!(
            s.lookup_access_relay_enrollment("principal", &valid, &selector, 102).await,
            Err(SessionStoreError::InvalidOperation)
        ));
    }
    let mut second = grant(11, 1, 102);
    second.enrollment_id = "second-enrollment".into();
    second.identity.server_id = "second-server".into();
    second.credential.enrollment_id = second.enrollment_id.clone();
    second.credential.identity = second.identity.clone();
    s.issue_relay_enrollment(second, 102).await?;
    assert!(matches!(
        s.lookup_access_relay_enrollment("principal", &valid, &selector, 103).await,
        Err(SessionStoreError::OwnerConflict)
    ));
    sqlx::query("UPDATE codex_relay_enrollments_v1 SET workspace_generation=2")
        .execute(s.pool())
        .await?;
    let server = AccessRelaySelector::Server {
        server_id: "server".into(),
        installation_id: "installation".into(),
    };
    assert!(matches!(
        s.lookup_access_relay_enrollment("principal", &valid, &server, 103).await,
        Err(SessionStoreError::OwnerConflict)
    ));
    Ok(())
}

#[tokio::test]
async fn dispatch_derives_nondefault_identity_and_never_returns_expired_or_revoked_ciphertext() -> TestResult {
    let s = store().await;
    let mut command = grant(10, 1, 101);
    command.identity.server_id = "stored-server".into();
    command.identity.environment_id = "stored-environment".into();
    command.identity.installation_id = "stored-installation".into();
    command.credential.identity = command.identity.clone();
    let initial = s.issue_relay_enrollment(command, 101).await?;
    let record = s.lookup_relay_credential(&[10; 32]).await?.unwrap();
    assert_eq!(
        dispatch(&s, &record.snapshot, 102).await?,
        RelayDispatch::Authorized(Box::new(initial))
    );
    assert!(dispatch(&s, &access(&family(), 1), 102).await.is_err());
    assert!(s.fence_relay_dispatch("other-principal", &record.snapshot, 102).await.is_err());
    let mut wrong_family = record.snapshot.clone();
    wrong_family.family_id = "other-family".into();
    assert!(dispatch(&s, &wrong_family, 102).await.is_err());
    sqlx::query("UPDATE codex_relay_enrollments_v1 SET provider_expires_at_unix_secs=103")
        .execute(s.pool())
        .await?;
    assert_eq!(dispatch(&s, &record.snapshot, 103).await?, RelayDispatch::ProviderExpired);
    s.revoke_session_family("family", "session-2", 104).await?;
    assert!(matches!(
        dispatch(&s, &record.snapshot, 105).await,
        Err(SessionStoreError::CredentialRejected)
    ));
    Ok(())
}
