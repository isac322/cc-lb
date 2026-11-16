use super::*;
use cc_lb_storage_api::codex_sessions::{SessionFamily, SessionSnapshot, SessionStoreResult};
use cc_lb_storage_api::{
    IssueRelayEnrollment, RefreshRelayEnrollment, RelayAuthRecord, RelayCredential, RelayDispatch,
    RelayEnrollment, RelayEnrollmentIdentity, RelayLineage, StoredApiKeyRecord,
};
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};

struct Store {
    record: Mutex<RelayAuthRecord>,
    reads: AtomicUsize,
    lookup_hash: Mutex<Option<[u8; 32]>>,
}
#[async_trait::async_trait]
impl RelayEnrollmentStore for Store {
    async fn lookup_relay_credential(
        &self,
        index_hash: &[u8; 32],
    ) -> SessionStoreResult<Option<RelayAuthRecord>> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        *self.lookup_hash.lock().unwrap() = Some(*index_hash);
        let r = self.record.lock().unwrap();
        Ok(Some(RelayAuthRecord {
            credential: r.credential.clone(),
            snapshot: r.snapshot.clone(),
            family: r.family.clone(),
            parent_key: r.parent_key.clone(),
            principal_enabled: r.principal_enabled,
        }))
    }
    async fn issue_relay_enrollment(
        &self,
        _: IssueRelayEnrollment,
        _: u64,
    ) -> SessionStoreResult<RelayEnrollment> {
        panic!("header verifier must not issue")
    }
    async fn refresh_relay_enrollment(
        &self,
        _: RefreshRelayEnrollment,
        _: u64,
    ) -> SessionStoreResult<RelayEnrollment> {
        panic!("header verifier must not refresh")
    }
    async fn fence_relay_dispatch(
        &self,
        _: &str,
        _: &SessionSnapshot,
        _: &RelayEnrollmentIdentity,
        _: u64,
    ) -> SessionStoreResult<RelayDispatch> {
        panic!("header verifier must not resolve owner")
    }
}
fn fixture() -> (
    session_token::MintedToken,
    Arc<Store>,
    StoredRelayVerifier,
    PrincipalView,
) {
    let token = session_token::mint_relay();
    let family = SessionFamily {
        family_id: "family".into(),
        principal_id: "principal-1".into(),
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
        access_ttl_secs: 50,
        created_at: 100,
        expires_at: 300,
        revoked_at: None,
    };
    let credential = RelayCredential {
        credential_id: "relay".into(),
        enrollment_id: "enrollment".into(),
        identity: RelayEnrollmentIdentity {
            family_id: family.family_id.clone(),
            server_id: "server".into(),
            environment_id: "environment".into(),
            installation_id: "installation".into(),
        },
        enrollment_generation: 1,
        index_hash: token.index_hash,
        verify_hash: token.verify_hash,
        salt: token.secret_salt,
        issued_at_unix_secs: 100,
        expires_at_unix_secs: 200,
    };
    let snapshot = RelayLineage::from_family(&family).snapshot(&credential);
    let parent_key = StoredApiKeyRecord {
        status: KeyStatus::Active,
        expires_at_unix_secs: Some(150),
        last_4: "1234".into(),
        limit_overrides: vec![cc_lb_storage_api::Limit {
            kind: cc_lb_storage_api::LimitKind::Requests,
            window_secs: 60,
            cap_micros: 3_000_000,
        }],
        ..Default::default()
    };
    let store = Arc::new(Store {
        record: Mutex::new(RelayAuthRecord {
            credential,
            snapshot,
            family,
            parent_key,
            principal_enabled: true,
        }),
        reads: AtomicUsize::new(0),
        lookup_hash: Mutex::new(None),
    });
    let verifier = StoredRelayVerifier::new(
        store.clone(),
        Arc::new(cc_lb_clock::TestClock::new_at_secs(150)),
    );
    let view = PrincipalView::for_tests(
        "principal-1",
        true,
        Vec::new(),
        Vec::new(),
        Default::default(),
    );
    (token, store, verifier, view)
}

#[tokio::test]
async fn malformed_and_cross_purpose_tokens_do_not_read_storage() {
    let (_, store, verifier, view) = fixture();
    let renewal = session_token::mint_renewal();
    for input in ["", "cclb-relay-v1.invalid", renewal.plaintext.expose()] {
        assert_eq!(
            verifier.authenticate_relay(input, &view).await.unwrap_err(),
            BuiltinAuthError::InvalidFormat
        );
    }
    assert_eq!(store.reads.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn renewal_overlap_preserves_parent_attribution_without_bearer() {
    let (token, store, verifier, view) = fixture();
    store.record.lock().unwrap().family.access_generation = 2;
    let result = verifier
        .authenticate_relay(token.plaintext.expose(), &view)
        .await
        .unwrap();
    assert_eq!(*store.lookup_hash.lock().unwrap(), Some(token.index_hash));
    assert_eq!(
        result.record.limit_overrides,
        store.record.lock().unwrap().parent_key.limit_overrides
    );
    assert_eq!(result.principal_id, "principal-1");
    assert_eq!(result.key_id, "parent");
    assert_eq!(result.record.expires_at_unix_secs, Some(150));
    assert_eq!(result.last_4, "1234");
    assert!(result.api_key.is_none());
    assert_eq!(result.session.unwrap().access_generation, 1);
}

#[tokio::test]
async fn different_valid_bearer_fails_constant_time_verifier() {
    let (_, _, verifier, view) = fixture();
    let other = session_token::mint_relay();
    assert_eq!(
        verifier
            .authenticate_relay(other.plaintext.expose(), &view)
            .await
            .unwrap_err(),
        BuiltinAuthError::SignatureMismatch
    );
}

#[tokio::test]
async fn revoked_expired_future_and_stale_lineage_are_rejected() {
    for case in 0..10 {
        let (token, store, verifier, view) = fixture();
        {
            let mut r = store.record.lock().unwrap();
            match case {
                0 => r.parent_key.revoked_at_unix_secs = Some(140),
                1 => r.family.revoked_at = Some(140),
                2 => r.credential.expires_at_unix_secs = 150,
                3 => r.credential.issued_at_unix_secs = 151,
                4 => r.family.created_at = 151,
                5 => r.family.workspace_generation = 3,
                6 => r.family.owner_generation = 2,
                7 => r.parent_key.expires_at_unix_secs = Some(149),
                8 => r.snapshot.purpose = CredentialPurpose::SessionAccess,
                _ => r.credential.expires_at_unix_secs = 301,
            }
        }
        assert_eq!(
            verifier
                .authenticate_relay(token.plaintext.expose(), &view)
                .await
                .unwrap_err(),
            BuiltinAuthError::KeyRevoked,
            "case {case}"
        );
    }
}

#[tokio::test]
async fn disabled_principal_is_not_reclassified_as_expired() {
    let (token, store, verifier, view) = fixture();
    store.record.lock().unwrap().principal_enabled = false;
    assert_eq!(
        verifier
            .authenticate_relay(token.plaintext.expose(), &view)
            .await
            .unwrap_err(),
        BuiltinAuthError::PrincipalDisabled
    );
}

#[tokio::test]
async fn live_principal_view_rejects_disabled_and_missing_principals() {
    let (token, _, verifier, _) = fixture();
    for (id, enabled, expected) in [
        ("principal-1", false, BuiltinAuthError::PrincipalDisabled),
        (
            "different-principal",
            true,
            BuiltinAuthError::PrincipalMissing,
        ),
    ] {
        let view =
            PrincipalView::for_tests(id, enabled, Vec::new(), Vec::new(), Default::default());
        assert_eq!(
            verifier
                .authenticate_relay(token.plaintext.expose(), &view)
                .await
                .unwrap_err(),
            expected,
        );
    }
}
