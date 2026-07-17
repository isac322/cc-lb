use std::time::Duration;

use cc_lb_engine::cache_keepalive::{
    DispatchOutcome, KeepaliveDispatchContext, KeepaliveDispatcher,
};
use cc_lb_engine::clock::{Clock, unix_secs};
use cc_lb_storage_api::{
    CacheKeepaliveEnqueueState, CacheKeepaliveHitRefreshRequest, CacheKeepaliveSessionStatus,
    CacheKeepaliveSessionStore, CacheKeepaliveTerminalReason,
};

use super::support::RenewalFixture;

#[tokio::test]
async fn cache_hit_reschedules_current_generation_without_accounting_event() {
    // Given
    let fixture = RenewalFixture::new(10).await;
    let session = fixture.schedule_session().await;

    // When
    let outcome = fixture
        .dispatcher
        .dispatch(
            &fixture.snapshot,
            KeepaliveDispatchContext::new(None, "renewal-session:1".to_owned()),
        )
        .await;
    let now = unix_secs(fixture.clock.now());
    let refreshed = fixture
        .storage
        .reschedule_after_cache_hit(&CacheKeepaliveHitRefreshRequest {
            session_key_hash: session.session_key_hash,
            generation: session.generation,
            cache_anchor_at_unix_secs: now,
            run_at_unix_secs: now + 15,
            expires_at_unix_secs: now + 300,
            encrypted_payload: vec![2],
            now_unix_secs: now,
        })
        .await
        .expect("reschedule cache hit")
        .expect("current generation reschedules");
    let marked_enqueued = fixture
        .storage
        .mark_cache_keepalive_enqueued("renewal-session", refreshed.generation, now)
        .await
        .expect("mark rescheduled generation enqueued");

    // Then
    assert!(matches!(
        outcome,
        DispatchOutcome::CacheHit {
            cache_anchor_age, ..
        } if cache_anchor_age < Duration::from_secs(1)
    ));
    assert_eq!(fixture.http.calls(), 1);
    assert_eq!(refreshed.generation, 2);
    assert_eq!(refreshed.refresh_count, 1);
    assert_eq!(refreshed.status, CacheKeepaliveSessionStatus::Active);
    assert_eq!(refreshed.enqueue_state, CacheKeepaliveEnqueueState::Pending);
    assert_eq!(refreshed.run_at_unix_secs, 1_015);
    assert!(marked_enqueued);
    assert_eq!(fixture.request_event_count().await, 0);
}

#[tokio::test]
async fn cache_miss_terminalizes_current_generation_without_accounting_event() {
    // Given
    let fixture = RenewalFixture::new(0).await;
    let session = fixture.schedule_session().await;

    // When
    let outcome = fixture
        .dispatcher
        .dispatch(
            &fixture.snapshot,
            KeepaliveDispatchContext::new(None, "renewal-session:1".to_owned()),
        )
        .await;
    let terminalized = fixture
        .storage
        .mark_cache_keepalive_terminal(
            &session.session_key_hash,
            session.generation,
            CacheKeepaliveTerminalReason::CacheMiss,
            unix_secs(fixture.clock.now()),
        )
        .await
        .expect("terminalize cache miss");
    let record = fixture
        .storage
        .get_cache_keepalive_session("renewal-session")
        .await
        .expect("load terminal renewal session")
        .expect("renewal session exists");

    // Then
    assert!(matches!(outcome, DispatchOutcome::CacheMiss { .. }));
    assert_eq!(fixture.http.calls(), 1);
    assert!(terminalized);
    assert_eq!(record.status, CacheKeepaliveSessionStatus::Terminal);
    assert_eq!(
        record.terminal_reason,
        Some(CacheKeepaliveTerminalReason::CacheMiss)
    );
    assert_eq!(fixture.request_event_count().await, 0);
}

#[tokio::test]
async fn stale_generation_cannot_reschedule_or_terminalize_and_never_fires() {
    // Given
    let fixture = RenewalFixture::new(10).await;
    let stale = fixture.schedule_session().await;
    let current = fixture.schedule_session().await;
    let now = unix_secs(fixture.clock.now());

    // When
    let rescheduled = fixture
        .storage
        .reschedule_after_cache_hit(&CacheKeepaliveHitRefreshRequest {
            session_key_hash: stale.session_key_hash.clone(),
            generation: stale.generation,
            cache_anchor_at_unix_secs: now,
            run_at_unix_secs: now + 15,
            expires_at_unix_secs: now + 300,
            encrypted_payload: vec![3],
            now_unix_secs: now,
        })
        .await
        .expect("attempt stale reschedule");
    let terminalized = fixture
        .storage
        .mark_cache_keepalive_terminal(
            &stale.session_key_hash,
            stale.generation,
            CacheKeepaliveTerminalReason::CacheMiss,
            now,
        )
        .await
        .expect("attempt stale terminalization");

    // Then
    assert!(rescheduled.is_none());
    assert!(!terminalized);
    assert_eq!(fixture.http.calls(), 0);
    assert_eq!(current.generation, 2);
    let record = fixture
        .storage
        .get_cache_keepalive_session("renewal-session")
        .await
        .expect("load current renewal session")
        .expect("current renewal session exists");
    assert_eq!(record.generation, current.generation);
    assert_eq!(record.status, CacheKeepaliveSessionStatus::Active);
    assert_eq!(record.refresh_count, 0);
    assert_eq!(fixture.request_event_count().await, 0);
}
