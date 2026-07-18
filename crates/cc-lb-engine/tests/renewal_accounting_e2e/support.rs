mod claims;
mod finalizer;
#[path = "../renewal_characterization/support.rs"]
mod renewal_fixture;

use std::{collections::HashMap, sync::Arc};

use cc_lb_engine::{
    InMemoryBus,
    api_keys::{
        concurrent_guard::KeyConcurrencyManager,
        limit_engine::{LimitEngine, Reservation},
        principal_view::PrincipalView,
    },
    cache_keepalive::{
        DispatchOutcome, KeepaliveDispatchContext, KeepaliveDispatcher, RenewalFinalization,
    },
};
use cc_lb_storage_api::{
    CacheKeepaliveReplaceRequest, CacheKeepaliveSessionRecord, CacheKeepaliveSessionStore,
    CacheKeepaliveTerminalReason, CacheTtl,
    types::{KeyStatus, Limit as StoredLimit, LimitKind, StoredApiKeyRecord},
};

use self::finalizer::{Completion, FinalizeInput, install_test_pricing, persist_finalization};
use claims::concurrent_sqlite_claims;
use renewal_fixture::RenewalFixture;

const PRINCIPAL_ID: &str = "renewal-principal";
const KEY_ID: &str = "renewal-key";
const OUTPUT_CAP: i64 = 100;

pub(crate) struct RenewalAccountingScenario {
    fixture: RenewalFixture,
    limit_engine: Arc<LimitEngine>,
    principal_view: Arc<PrincipalView>,
    key_record: StoredApiKeyRecord,
}

impl RenewalAccountingScenario {
    pub(crate) async fn new() -> Self {
        install_test_pricing();
        let fixture = RenewalFixture::new(10).await;
        assert_eq!(fixture.request_event_count().await, 0);
        let _unused_seed = fixture.schedule_session().await;
        let principal_view = Arc::new(PrincipalView::for_tests(
            PRINCIPAL_ID,
            true,
            vec!["*".to_owned()],
            Vec::new(),
            HashMap::new(),
        ));
        let key_record = StoredApiKeyRecord {
            key_hash_b64: KEY_ID.to_owned(),
            status: KeyStatus::Active,
            limit_overrides: vec![StoredLimit {
                kind: LimitKind::OutputTokens,
                window_secs: 60,
                cap_micros: OUTPUT_CAP,
            }],
            ..StoredApiKeyRecord::default()
        };

        Self {
            fixture,
            limit_engine: LimitEngine::new(
                Arc::new(KeyConcurrencyManager::new()),
                Arc::new(cc_lb_engine::SystemClock),
            ),
            principal_view,
            key_record,
        }
    }

    pub(crate) async fn schedule(
        &self,
        accounting_key_id: Option<&str>,
    ) -> CacheKeepaliveSessionRecord {
        let now = 1_000;
        let session = self
            .fixture
            .storage
            .replace_from_real_request(&CacheKeepaliveReplaceRequest {
                session_key_hash: "renewal-session".to_owned(),
                principal_id: PRINCIPAL_ID.to_owned(),
                accounting_key_id: accounting_key_id.map(ToOwned::to_owned),
                upstream_id: self.fixture.upstream_id,
                cache_anchor_at_unix_secs: now,
                ttl: CacheTtl::Ttl5m,
                run_at_unix_secs: now + 10,
                expires_at_unix_secs: now + 300,
                encrypted_payload: vec![1],
                now_unix_secs: now,
            })
            .await
            .expect("schedule renewal session");
        assert!(
            self.fixture
                .storage
                .mark_cache_keepalive_enqueued(
                    &session.session_key_hash,
                    session.generation,
                    now + 1,
                )
                .await
                .expect("mark renewal session enqueued")
        );
        session
    }

    pub(crate) async fn claim(&self, session: &CacheKeepaliveSessionRecord) -> bool {
        self.fixture
            .storage
            .claim_cache_keepalive_turn(&session.session_key_hash, session.generation, 1_002)
            .await
            .expect("claim renewal session")
    }

    pub(crate) async fn concurrent_claims(&self, session: &CacheKeepaliveSessionRecord) -> u8 {
        concurrent_sqlite_claims(Arc::clone(&self.fixture.storage), session.generation).await
    }

    pub(crate) async fn dispatch(
        &self,
        session: &CacheKeepaliveSessionRecord,
        reserve: bool,
    ) -> RenewalFinalization {
        let reservation = reserve.then(|| self.reserve());
        let source_ref_id = format!("{}:{}", session.session_key_hash, session.generation);
        let outcome = self
            .fixture
            .dispatcher
            .dispatch(
                &self.fixture.snapshot,
                KeepaliveDispatchContext::new(reservation, source_ref_id),
            )
            .await;
        match outcome {
            DispatchOutcome::CacheHit { finalization, .. } => finalization,
            DispatchOutcome::CacheMiss { .. }
            | DispatchOutcome::UnsupportedProvider(_)
            | DispatchOutcome::Error(_) => panic!("renewal fixture must produce a cache hit"),
        }
    }

    pub(crate) async fn dispatch_and_finalize(
        &self,
        session: &CacheKeepaliveSessionRecord,
        reserve: bool,
        bus: Option<&InMemoryBus>,
    ) -> Completion {
        let finalization = self.dispatch(session, reserve).await;
        self.finalize(session, finalization, bus).await
    }

    pub(crate) async fn disable_after_dispatch(
        &self,
        session: &CacheKeepaliveSessionRecord,
        finalization: RenewalFinalization,
    ) {
        self.terminalize(session, CacheKeepaliveTerminalReason::Cancelled)
            .await;
        drop(finalization);
    }

    pub(crate) async fn terminalize(
        &self,
        session: &CacheKeepaliveSessionRecord,
        reason: CacheKeepaliveTerminalReason,
    ) {
        assert!(
            self.fixture
                .storage
                .mark_cache_keepalive_terminal(
                    &session.session_key_hash,
                    session.generation,
                    reason,
                    1_003,
                )
                .await
                .expect("terminalize renewal session")
        );
    }

    pub(crate) async fn row_counts(&self) -> (i64, i64, i64) {
        let events = sqlx::query_scalar("SELECT COUNT(*) FROM request_events_v1")
            .fetch_one(self.fixture.storage.pool())
            .await
            .expect("count request events");
        let turns = sqlx::query_scalar("SELECT COUNT(*) FROM cache_keepalive_turns")
            .fetch_one(self.fixture.storage.pool())
            .await
            .expect("count renewal turns");
        let decisions = sqlx::query_scalar("SELECT COUNT(*) FROM cache_keepalive_decisions")
            .fetch_one(self.fixture.storage.pool())
            .await
            .expect("count renewal decisions");
        (events, turns, decisions)
    }

    pub(crate) fn http_calls(&self) -> u64 {
        self.fixture.http.calls()
    }

    pub(crate) async fn terminal_reason(&self) -> Option<CacheKeepaliveTerminalReason> {
        self.fixture
            .storage
            .get_cache_keepalive_session("renewal-session")
            .await
            .expect("load renewal session")
            .expect("renewal session exists")
            .terminal_reason
    }

    pub(crate) async fn assert_full_capacity_available(&self) {
        let verifier = self.reserve();
        assert!(
            self.limit_engine
                .reserve(
                    &self.principal_view,
                    &self.key_record,
                    PRINCIPAL_ID,
                    "claude-test",
                    1,
                    0,
                    None,
                )
                .is_err(),
            "the renewal reservation must be reconciled or refunded exactly once"
        );
        drop(verifier);
    }

    async fn finalize(
        &self,
        session: &CacheKeepaliveSessionRecord,
        finalization: RenewalFinalization,
        bus: Option<&InMemoryBus>,
    ) -> Completion {
        persist_finalization(FinalizeInput {
            storage: self.fixture.storage.as_ref(),
            limit_engine: self.limit_engine.as_ref(),
            session,
            finalization,
            bus,
        })
        .await
    }

    fn reserve(&self) -> Reservation {
        self.limit_engine
            .reserve(
                &self.principal_view,
                &self.key_record,
                PRINCIPAL_ID,
                "claude-test",
                OUTPUT_CAP,
                0,
                None,
            )
            .expect("reserve renewal output capacity")
    }
}
