use std::str::FromStr;
use std::sync::Arc;

use anyhow::Result;
use cc_lb_clock::SystemClock;
use cc_lb_storage_api::{
    CacheKeepaliveEnqueueState, CacheKeepaliveEvidenceRow, CacheKeepaliveHitRefreshRequest,
    CacheKeepaliveReplaceRequest, CacheKeepaliveSessionStatus, CacheKeepaliveSessionStore,
    CacheKeepaliveTerminalReason, CacheTtl, MetaStore, RequestEvent, RequestEventStore,
    cache_keepalive_job_key,
};
use cc_lb_storage_postgres::PostgresStorage;
use sqlx::{AssertSqlSafe, PgPool, postgres::PgConnectOptions, postgres::PgPoolOptions};
use uuid::Uuid;

fn postgres_url() -> Option<String> {
    std::env::var("CI_POSTGRES_URL").ok()
}

#[test]
fn cache_keepalive_postgres_conformance() {
    let Some(url) = postgres_url() else {
        eprintln!("skip: CI_POSTGRES_URL not set; requires isolated local/test postgres DSN");
        return;
    };

    tokio::runtime::Runtime::new()
        .expect("tokio runtime")
        .block_on(async move { run_conformance(&url).await })
        .expect("cache keepalive postgres conformance");
}

async fn append_evidence_filters_follow_ups_and_deduplicates_overlapping_cycles(
    storage: &PostgresStorage,
) -> Result<()> {
    let upstream_id = Uuid::from_u128(7);
    let cycle_id = Uuid::from_u128(8);
    let overlapping_cycle_id = Uuid::from_u128(9);
    let evidence =
        |id: &str, kind: &str, start: u64, cycle_id: Option<Uuid>| CacheKeepaliveEvidenceRow {
            evidence_id: id.to_owned(),
            cycle_id,
            evidence_kind: kind.to_owned(),
            request_start_ms: start,
            prefix_fingerprint: Some("fingerprint".to_owned()),
            prefix_hash: Some("prefix".to_owned()),
            prefix_policy_version: Some("policy".to_owned()),
            prefix_schema_version: Some("schema".to_owned()),
            principal_id: "principal".to_owned(),
            upstream_id,
            model: "model".to_owned(),
            ttl: CacheTtl::Ttl5m,
            hit_miss: Some("hit".to_owned()),
            cache_read_input_tokens: Some(1),
            cost_micros: Some(1),
            request_ref: Some(id.to_owned()),
            event_ref: Some(id.to_owned()),
        };
    let mut invalid = evidence("invalid", "follow_up", 1_400, None);
    invalid.cache_read_input_tokens = Some(0);
    let mut unmatched = evidence("unmatched", "follow_up", 1_500, None);
    unmatched.prefix_hash = Some("other-prefix".to_owned());
    let cursor = storage
        .append_request_event_with_evidence(
            &RequestEvent {
                ts: 1,
                ts_ms: Some(1_000),
                event_id: Some("filtered-event".to_owned()),
                ..RequestEvent::default()
            },
            &[
                evidence("renewal", "renewal", 1_000, Some(cycle_id)),
                evidence(
                    "overlapping-renewal",
                    "renewal",
                    1_100,
                    Some(overlapping_cycle_id),
                ),
                evidence("first-follow-up", "follow_up", 1_200, None),
                evidence("second-follow-up", "follow_up", 1_300, None),
                invalid,
                unmatched,
            ],
        )
        .await?;
    assert!(cursor > 0);
    let ids: Vec<String> =
        sqlx::query_scalar("SELECT evidence_id FROM cache_keepalive_evidence ORDER BY evidence_id")
            .fetch_all(storage.pool())
            .await?;
    assert_eq!(
        ids,
        vec!["first-follow-up", "overlapping-renewal", "renewal",]
    );
    Ok(())
}
async fn append_evidence_deduplicates_follow_ups_across_sibling_renewals(
    storage: &PostgresStorage,
) -> Result<()> {
    let upstream_id = Uuid::from_u128(7);
    let cycle_id = Uuid::from_u128(80);
    let evidence = |id: &str, kind: &str, start: u64, prefix: &str| CacheKeepaliveEvidenceRow {
        evidence_id: id.to_owned(),
        cycle_id: (kind == "renewal").then_some(cycle_id),
        evidence_kind: kind.to_owned(),
        request_start_ms: start,
        prefix_fingerprint: Some("fingerprint".to_owned()),
        prefix_hash: Some(prefix.to_owned()),
        prefix_policy_version: Some("policy".to_owned()),
        prefix_schema_version: Some("schema".to_owned()),
        principal_id: "principal".to_owned(),
        upstream_id,
        model: "model".to_owned(),
        ttl: CacheTtl::Ttl5m,
        hit_miss: Some("hit".to_owned()),
        cache_read_input_tokens: Some(1),
        cost_micros: Some(1),
        request_ref: Some(id.to_owned()),
        event_ref: Some(id.to_owned()),
    };

    storage
        .append_request_event_with_evidence(
            &RequestEvent {
                ts: 1,
                ts_ms: Some(1_000),
                event_id: Some("sibling-append-first-event".to_owned()),
                ..RequestEvent::default()
            },
            &[
                evidence("sibling-renewal-a", "renewal", 1_000, "prefix-a"),
                evidence("sibling-follow-up-a", "follow_up", 1_100, "prefix-a"),
            ],
        )
        .await?;
    storage
        .append_request_event_with_evidence(
            &RequestEvent {
                ts: 2,
                ts_ms: Some(2_000),
                event_id: Some("sibling-append-second-event".to_owned()),
                ..RequestEvent::default()
            },
            &[
                evidence("sibling-renewal-b", "renewal", 1_050, "prefix-b"),
                evidence("sibling-follow-up-b", "follow_up", 1_200, "prefix-b"),
            ],
        )
        .await?;

    let ids: Vec<String> = sqlx::query_scalar(
        "SELECT evidence_id
         FROM cache_keepalive_evidence
         WHERE evidence_id LIKE 'sibling-%'
         ORDER BY request_start_ms, evidence_id",
    )
    .fetch_all(storage.pool())
    .await?;
    assert_eq!(
        ids,
        vec![
            "sibling-renewal-a",
            "sibling-renewal-b",
            "sibling-follow-up-a",
        ]
    );
    sqlx::query("DELETE FROM cache_keepalive_evidence WHERE evidence_id LIKE 'sibling-%'")
        .execute(storage.pool())
        .await?;
    Ok(())
}

async fn append_evidence_rejects_duplicate_follow_up_with_long_lived_proof(
    storage: &PostgresStorage,
) -> Result<()> {
    let upstream_id = Uuid::from_u128(7);
    let cycle_id = Uuid::from_u128(82);
    let evidence = |id: &str, kind: &str, start: u64, prefix: &str| CacheKeepaliveEvidenceRow {
        evidence_id: id.to_owned(),
        cycle_id: (kind == "renewal").then_some(cycle_id),
        evidence_kind: kind.to_owned(),
        request_start_ms: start,
        prefix_fingerprint: Some("fingerprint".to_owned()),
        prefix_hash: Some(prefix.to_owned()),
        prefix_policy_version: Some("policy".to_owned()),
        prefix_schema_version: Some("schema".to_owned()),
        principal_id: "principal".to_owned(),
        upstream_id,
        model: "model".to_owned(),
        ttl: CacheTtl::Ttl1h,
        hit_miss: Some("hit".to_owned()),
        cache_read_input_tokens: Some(1),
        cost_micros: Some(1),
        request_ref: Some(id.to_owned()),
        event_ref: Some(id.to_owned()),
    };

    storage
        .append_request_event_with_evidence(
            &RequestEvent {
                ts: 1,
                ts_ms: Some(3_300_001),
                event_id: Some("long-lived-anchor-event".to_owned()),
                ..RequestEvent::default()
            },
            &[evidence(
                "long-lived-renewal-a",
                "renewal",
                3_300_001,
                "prefix-a",
            )],
        )
        .await?;
    storage
        .append_request_event_with_evidence(
            &RequestEvent {
                ts: 2,
                ts_ms: Some(3_400_000),
                event_id: Some("long-lived-proof-event".to_owned()),
                ..RequestEvent::default()
            },
            &[evidence(
                "long-lived-proof",
                "follow_up",
                3_400_000,
                "prefix-a",
            )],
        )
        .await?;
    storage
        .append_request_event_with_evidence(
            &RequestEvent {
                ts: 3,
                ts_ms: Some(8_000_000),
                event_id: Some("long-lived-sibling-event".to_owned()),
                ..RequestEvent::default()
            },
            &[evidence(
                "long-lived-renewal-b",
                "renewal",
                8_000_000,
                "prefix-b",
            )],
        )
        .await?;
    storage
        .append_request_event_with_evidence(
            &RequestEvent {
                ts: 4,
                ts_ms: Some(11_500_000),
                event_id: Some("long-lived-duplicate-event".to_owned()),
                ..RequestEvent::default()
            },
            &[evidence(
                "long-lived-duplicate",
                "follow_up",
                11_500_000,
                "prefix-b",
            )],
        )
        .await?;

    let ids: Vec<String> = sqlx::query_scalar(
        "SELECT evidence_id
         FROM cache_keepalive_evidence
         WHERE evidence_id LIKE 'long-lived-%'
         ORDER BY request_start_ms, evidence_id",
    )
    .fetch_all(storage.pool())
    .await?;
    assert_eq!(
        ids,
        vec![
            "long-lived-renewal-a",
            "long-lived-proof",
            "long-lived-renewal-b",
        ]
    );
    sqlx::query("DELETE FROM cache_keepalive_evidence WHERE evidence_id LIKE 'long-lived-%'")
        .execute(storage.pool())
        .await?;
    Ok(())
}

async fn purge_expired_prunes_unmatched_and_invalid_follow_ups_without_changing_count(
    storage: &PostgresStorage,
) -> Result<()> {
    let key = "expired-evidence-session";
    let mut request = replace_request(key, b"payload", 0);
    request.cycle_id = Uuid::from_u128(8);
    let session = storage.replace_from_real_request(&request).await?;
    storage
        .mark_cache_keepalive_enqueued(&session.session_key_hash, session.generation, 1)
        .await?;
    assert!(
        storage
            .claim_cache_keepalive_turn(&session.session_key_hash, session.generation, 2)
            .await?
    );
    let cycle_id = Uuid::from_u128(8);
    let zero_cycle_id = Uuid::from_u128(9);
    let null_cycle_id = Uuid::from_u128(10);
    for (id, cycle, kind, start, prefix_hash, read_tokens) in [
        (
            "renewal",
            Some(cycle_id),
            "renewal",
            1_000_i64,
            Some("prefix"),
            Some(1_i64),
        ),
        (
            "earliest",
            None,
            "follow_up",
            1_100,
            Some("prefix"),
            Some(1),
        ),
        ("later", None, "follow_up", 1_200, Some("prefix"), Some(1)),
        ("invalid", None, "follow_up", 1_300, Some("prefix"), Some(0)),
        (
            "zero-renewal",
            Some(zero_cycle_id),
            "renewal",
            2_000,
            Some("prefix"),
            Some(1),
        ),
        (
            "zero-only",
            None,
            "follow_up",
            2_100,
            Some("prefix"),
            Some(0),
        ),
        (
            "null-renewal",
            Some(null_cycle_id),
            "renewal",
            3_000,
            Some("prefix"),
            Some(1),
        ),
        ("null-only", None, "follow_up", 3_100, None, Some(1)),
    ] {
        sqlx::query(
            "INSERT INTO cache_keepalive_evidence
             (evidence_id, cycle_id, evidence_kind, request_start_ms, prefix_fingerprint,
              prefix_hash, prefix_policy_version, prefix_schema_version, principal_id,
              upstream_id, model, ttl, hit_miss, cache_read_input_tokens, cost_micros,
              request_ref, event_ref, created_at_ms)
             VALUES ($1, $2, $3, $4, 'fingerprint', $5, 'policy', 'schema',
                     'principal', $6, 'model', '5m', 'hit', $7, 1, $1, $1, $4)",
        )
        .bind(id)
        .bind(cycle)
        .bind(kind)
        .bind(start)
        .bind(prefix_hash)
        .bind(Uuid::from_u128(7))
        .bind(read_tokens)
        .execute(storage.pool())
        .await?;
    }
    assert_eq!(storage.purge_cache_keepalive_expired(400).await?, 1);
    let ids: Vec<String> =
        sqlx::query_scalar("SELECT evidence_id FROM cache_keepalive_evidence ORDER BY evidence_id")
            .fetch_all(storage.pool())
            .await?;
    assert_eq!(
        ids,
        vec!["earliest", "null-renewal", "renewal", "zero-renewal"]
    );
    Ok(())
}
async fn purge_expired_prunes_later_follow_up_across_sibling_renewals(
    storage: &PostgresStorage,
) -> Result<()> {
    let cycle_id = Uuid::from_u128(81);
    let mut request = replace_request("sibling-purge-session", b"payload", 0);
    request.cycle_id = cycle_id;
    let session = storage.replace_from_real_request(&request).await?;
    storage
        .mark_cache_keepalive_enqueued(&session.session_key_hash, session.generation, 1)
        .await?;
    assert!(
        storage
            .claim_cache_keepalive_turn(&session.session_key_hash, session.generation, 2)
            .await?
    );

    for (id, kind, start, prefix_hash) in [
        ("sibling-purge-renewal-a", "renewal", 1_000_i64, "prefix-a"),
        ("sibling-purge-renewal-b", "renewal", 1_050, "prefix-b"),
        ("sibling-purge-earliest", "follow_up", 1_100, "prefix-a"),
        ("sibling-purge-later", "follow_up", 1_200, "prefix-b"),
    ] {
        sqlx::query(
            "INSERT INTO cache_keepalive_evidence
             (evidence_id, cycle_id, evidence_kind, request_start_ms, prefix_fingerprint,
              prefix_hash, prefix_policy_version, prefix_schema_version, principal_id,
              upstream_id, model, ttl, hit_miss, cache_read_input_tokens, cost_micros,
              request_ref, event_ref, created_at_ms)
             VALUES ($1, $2, $3, $4, 'fingerprint', $5, 'policy', 'schema',
                     'principal', $6, 'model', '5m', 'hit', 1, 1, $1, $1, $4)",
        )
        .bind(id)
        .bind((kind == "renewal").then_some(cycle_id))
        .bind(kind)
        .bind(start)
        .bind(prefix_hash)
        .bind(Uuid::from_u128(7))
        .execute(storage.pool())
        .await?;
    }

    assert_eq!(storage.purge_cache_keepalive_expired(400).await?, 1);
    let ids: Vec<String> = sqlx::query_scalar(
        "SELECT evidence_id
         FROM cache_keepalive_evidence
         WHERE evidence_id LIKE 'sibling-purge-%'
         ORDER BY evidence_id",
    )
    .fetch_all(storage.pool())
    .await?;
    assert_eq!(
        ids,
        vec![
            "sibling-purge-earliest",
            "sibling-purge-renewal-a",
            "sibling-purge-renewal-b",
        ]
    );
    Ok(())
}

async fn purge_expired_preserves_follow_up_earliest_for_one_matching_cycle(
    storage: &PostgresStorage,
) -> Result<()> {
    let upstream_id = Uuid::from_u128(7);
    let cycle_one = Uuid::from_u128(83);
    let cycle_two = Uuid::from_u128(84);
    let mut request = replace_request("multi-cycle-purge-session", b"payload", 0);
    request.cycle_id = cycle_one;
    let session = storage.replace_from_real_request(&request).await?;
    storage
        .mark_cache_keepalive_enqueued(&session.session_key_hash, session.generation, 1)
        .await?;
    assert!(
        storage
            .claim_cache_keepalive_turn(&session.session_key_hash, session.generation, 2)
            .await?
    );

    for (id, cycle, kind, start) in [
        (
            "multi-cycle-renewal-earlier",
            Some(cycle_two),
            "renewal",
            1_000_i64,
        ),
        ("multi-cycle-earlier-proof", None, "follow_up", 1_500),
        (
            "multi-cycle-renewal-later",
            Some(cycle_one),
            "renewal",
            2_000,
        ),
        ("multi-cycle-later-proof", None, "follow_up", 2_500),
    ] {
        sqlx::query(
            "INSERT INTO cache_keepalive_evidence
             (evidence_id, cycle_id, evidence_kind, request_start_ms, prefix_fingerprint,
              prefix_hash, prefix_policy_version, prefix_schema_version, principal_id,
              upstream_id, model, ttl, hit_miss, cache_read_input_tokens, cost_micros,
              request_ref, event_ref, created_at_ms)
             VALUES ($1, $2, $3, $4, 'fingerprint', 'shared-prefix', 'policy', 'schema',
                     'principal', $5, 'model', '5m', 'hit', 1, 1, $1, $1, $4)",
        )
        .bind(id)
        .bind(cycle)
        .bind(kind)
        .bind(start)
        .bind(upstream_id)
        .execute(storage.pool())
        .await?;
    }

    assert_eq!(storage.purge_cache_keepalive_expired(100).await?, 1);
    let ids: Vec<String> = sqlx::query_scalar(
        "SELECT evidence_id
         FROM cache_keepalive_evidence
         WHERE evidence_id LIKE 'multi-cycle-%'
         ORDER BY request_start_ms, evidence_id",
    )
    .fetch_all(storage.pool())
    .await?;
    assert_eq!(
        ids,
        vec![
            "multi-cycle-renewal-earlier",
            "multi-cycle-earlier-proof",
            "multi-cycle-renewal-later",
            "multi-cycle-later-proof",
        ]
    );
    Ok(())
}

async fn run_conformance(url: &str) -> Result<()> {
    let fixture = Fixture::create(url).await?;
    let storage = PostgresStorage::new(fixture.pool.clone(), Arc::new(SystemClock));
    storage.initialize().await?;
    replace_from_real_request_fenced_persists_cycle_id_and_preserves_fences(&storage).await?;
    replace_from_real_request_bumps_generation_and_resets_counters(&storage).await?;
    replace_roundtrips_optional_accounting_key_id(&storage).await?;
    append_evidence_deduplicates_follow_ups_across_sibling_renewals(&storage).await?;
    append_evidence_rejects_duplicate_follow_up_with_long_lived_proof(&storage).await?;
    append_evidence_filters_follow_ups_and_deduplicates_overlapping_cycles(&storage).await?;
    purge_expired_prunes_unmatched_and_invalid_follow_ups_without_changing_count(&storage).await?;
    purge_expired_prunes_later_follow_up_across_sibling_renewals(&storage).await?;
    purge_expired_preserves_follow_up_earliest_for_one_matching_cycle(&storage).await?;
    update_payload_only_mutates_matching_pending_generation(&storage).await?;
    cache_hit_reschedule_preserves_duration_anchor_and_rejects_stale_generation(&storage).await?;
    conditional_enqueue_terminal_and_purge_are_generation_safe(&storage).await?;
    terminal_error_is_truncated_and_resets_running_enqueue_state(&storage).await?;
    mark_latest_terminal_only_mutates_active_latest_session(&storage).await?;
    recover_stale_running_requires_active_running_strictly_old_lease_and_scrubs_payload(&storage)
        .await?;
    purge_stale_pending_keeps_enqueued_work(&storage).await?;
    recover_stale_running_rejects_overflow_without_mutation(&storage).await?;
    purge_expired_uses_a_strict_cutoff_boundary(&storage).await?;
    purge_stale_pending_uses_strict_cutoff_and_state_predicates(&storage).await?;
    concurrent_recovery_counts_once_and_replacement_reschedule_preserve_latest_active_payload(
        &storage,
    )
    .await?;
    concurrent_replace_from_real_request_bumps_each_generation(&storage).await?;
    concurrent_hit_reschedule_allows_only_one_generation_cas(&storage).await?;
    concurrent_claim_allows_exactly_one_enqueued_generation_winner(&storage).await?;
    pending_claim_requires_payload_and_mark_cannot_overwrite_running(&storage).await?;
    ticketed_cancel_retires_generation_after_synthetic_refresh(&storage).await?;
    newer_ticket_replaces_terminal_generation_before_terminal_write_timestamp(&storage).await?;
    completion_tickets_preserve_active_anchor_and_legacy_timestamp_guards(&storage).await?;
    ticketed_completions_use_stable_logical_anchor_and_reject_slow_older_responses(&storage)
        .await?;

    fixture.drop_schema().await
}

async fn replace_from_real_request_fenced_persists_cycle_id_and_preserves_fences(
    storage: &PostgresStorage,
) -> Result<()> {
    let none_key = "fenced-cycle-id-none";
    let none_first_cycle_id = Uuid::from_u128(11);
    let none_second_cycle_id = Uuid::from_u128(12);
    let mut none_first_request = replace_request(none_key, b"fenced-none-first", 100);
    none_first_request.cycle_id = none_first_cycle_id;

    let none_first = storage
        .replace_from_real_request_fenced(&none_first_request, None)
        .await?
        .expect("fenced None fresh insert");
    assert_eq!(none_first.cycle_id, Some(none_first_cycle_id));
    assert_eq!(
        storage
            .get_cache_keepalive_session(none_key)
            .await?
            .expect("fenced None fresh session")
            .cycle_id,
        Some(none_first_cycle_id)
    );

    let mut none_stale_request = replace_request(none_key, b"fenced-none-stale", 99);
    none_stale_request.cycle_id = none_second_cycle_id;
    assert!(
        storage
            .replace_from_real_request_fenced(&none_stale_request, None)
            .await?
            .is_none()
    );
    assert_eq!(
        storage
            .get_cache_keepalive_session(none_key)
            .await?
            .expect("fenced None session after stale request"),
        none_first
    );

    let mut none_replacement_request = replace_request(none_key, b"fenced-none-replacement", 101);
    none_replacement_request.cycle_id = none_second_cycle_id;
    let none_replacement = storage
        .replace_from_real_request_fenced(&none_replacement_request, None)
        .await?
        .expect("fenced None newer replacement");
    assert_eq!(none_replacement.generation, none_first.generation + 1);
    assert_eq!(none_replacement.cycle_id, Some(none_second_cycle_id));
    assert_eq!(
        storage
            .get_cache_keepalive_session(none_key)
            .await?
            .expect("fenced None replacement session"),
        none_replacement
    );

    let ticketed_key = "fenced-cycle-id-ticketed";
    let ticketed_first_cycle_id = Uuid::from_u128(21);
    let ticketed_second_cycle_id = Uuid::from_u128(22);
    let ticketed_first_ticket = storage
        .allocate_cache_keepalive_completion_ticket(ticketed_key)
        .await?;
    let mut ticketed_first_request = replace_request(ticketed_key, b"fenced-ticketed-first", 100);
    ticketed_first_request.cycle_id = ticketed_first_cycle_id;
    let ticketed_first = storage
        .replace_from_real_request_fenced(&ticketed_first_request, Some(ticketed_first_ticket))
        .await?
        .expect("fenced ticketed fresh insert");
    assert_eq!(ticketed_first.cycle_id, Some(ticketed_first_cycle_id));
    assert_eq!(
        storage
            .get_cache_keepalive_session(ticketed_key)
            .await?
            .expect("fenced ticketed fresh session")
            .cycle_id,
        Some(ticketed_first_cycle_id)
    );

    let mut ticketed_replay_request = replace_request(ticketed_key, b"fenced-ticketed-replay", 101);
    ticketed_replay_request.cycle_id = ticketed_second_cycle_id;
    assert!(
        storage
            .replace_from_real_request_fenced(
                &ticketed_replay_request,
                Some(ticketed_first_ticket),
            )
            .await?
            .is_none()
    );
    assert_eq!(
        storage
            .get_cache_keepalive_session(ticketed_key)
            .await?
            .expect("fenced ticketed session after replay"),
        ticketed_first
    );
    assert_eq!(
        storage
            .get_meta_value(&cc_lb_storage_api::cache_keepalive_order_meta_key(
                ticketed_key
            ))
            .await?,
        Some(format!("{ticketed_first_ticket}:{ticketed_first_ticket}"))
    );

    let ticketed_replacement_ticket = storage
        .allocate_cache_keepalive_completion_ticket(ticketed_key)
        .await?;
    let mut ticketed_stale_request = replace_request(ticketed_key, b"fenced-ticketed-stale", 99);
    ticketed_stale_request.cycle_id = ticketed_second_cycle_id;
    assert!(
        storage
            .replace_from_real_request_fenced(
                &ticketed_stale_request,
                Some(ticketed_replacement_ticket),
            )
            .await?
            .is_none()
    );
    assert_eq!(
        storage
            .get_cache_keepalive_session(ticketed_key)
            .await?
            .expect("fenced ticketed session after stale request"),
        ticketed_first
    );
    assert_eq!(
        storage
            .get_meta_value(&cc_lb_storage_api::cache_keepalive_order_meta_key(
                ticketed_key
            ))
            .await?,
        Some(format!(
            "{ticketed_replacement_ticket}:{}",
            ticketed_first_ticket
        ))
    );

    let mut ticketed_replacement_request =
        replace_request(ticketed_key, b"fenced-ticketed-replacement", 101);
    ticketed_replacement_request.cycle_id = ticketed_second_cycle_id;
    let ticketed_replacement = storage
        .replace_from_real_request_fenced(
            &ticketed_replacement_request,
            Some(ticketed_replacement_ticket),
        )
        .await?
        .expect("fenced ticketed newer replacement");
    assert_eq!(
        ticketed_replacement.generation,
        ticketed_first.generation + 1
    );
    assert_eq!(
        ticketed_replacement.cycle_id,
        Some(ticketed_second_cycle_id)
    );
    assert_eq!(
        storage
            .get_cache_keepalive_session(ticketed_key)
            .await?
            .expect("fenced ticketed replacement session"),
        ticketed_replacement
    );
    assert_eq!(
        storage
            .get_meta_value(&cc_lb_storage_api::cache_keepalive_order_meta_key(
                ticketed_key
            ))
            .await?,
        Some(format!(
            "{ticketed_replacement_ticket}:{ticketed_replacement_ticket}"
        ))
    );

    let healthy = storage
        .replace_from_real_request(&replace_request("fenced-healthy", b"healthy", 200))
        .await?;
    assert_eq!(healthy.cycle_id, Some(Uuid::nil()));
    Ok(())
}

async fn replace_from_real_request_bumps_generation_and_resets_counters(
    storage: &PostgresStorage,
) -> Result<()> {
    let first = storage
        .replace_from_real_request(&replace_request("replace-session", b"ciphertext-one", 100))
        .await?;
    let second = storage
        .replace_from_real_request(&replace_request("replace-session", b"ciphertext-two", 200))
        .await?;

    assert_eq!(first.generation, 1);
    assert_eq!(second.generation, 2);
    assert_eq!(second.refresh_count, 0);
    assert_eq!(second.first_scheduled_at_unix_secs, 200);
    assert_eq!(second.cache_anchor_at_unix_secs, 200);
    assert_eq!(second.run_at_unix_secs, 470);
    assert_eq!(
        second.current_job_key,
        cache_keepalive_job_key("replace-session", 2)
    );
    assert_eq!(second.encrypted_payload, b"ciphertext-two");
    Ok(())
}

async fn replace_roundtrips_optional_accounting_key_id(storage: &PostgresStorage) -> Result<()> {
    let with_key = CacheKeepaliveReplaceRequest {
        accounting_key_id: Some("key-live-123".to_owned()),
        ..replace_request("keyed-session", b"ciphertext-with-key", 100)
    };
    let without_key = replace_request("unkeyed-session", b"ciphertext-without-key", 110);

    let with_key = storage.replace_from_real_request(&with_key).await?;
    let without_key = storage.replace_from_real_request(&without_key).await?;

    assert_eq!(with_key.accounting_key_id.as_deref(), Some("key-live-123"));
    assert_eq!(without_key.accounting_key_id, None);

    // This conformance suite shares one schema across steps. Terminalize these
    // sessions so they do not linger as active+pending rows and inflate the
    // global count asserted by purge_stale_pending_keeps_enqueued_work.
    for session_key_hash in ["keyed-session", "unkeyed-session"] {
        assert!(
            storage
                .mark_latest_cache_keepalive_terminal(
                    session_key_hash,
                    CacheKeepaliveTerminalReason::Cancelled,
                    120,
                )
                .await?
        );
    }
    Ok(())
}

async fn update_payload_only_mutates_matching_pending_generation(
    storage: &PostgresStorage,
) -> Result<()> {
    let record = storage
        .replace_from_real_request(&replace_request(
            "payload-session",
            b"pending-placeholder",
            100,
        ))
        .await?;

    assert!(
        !storage
            .update_cache_keepalive_payload("payload-session", record.generation + 1, b"stale", 101)
            .await?
    );
    assert!(
        storage
            .update_cache_keepalive_payload(
                "payload-session",
                record.generation,
                b"ciphertext",
                102
            )
            .await?
    );

    let updated = storage
        .get_cache_keepalive_session("payload-session")
        .await?
        .expect("row exists");
    assert_eq!(updated.encrypted_payload, b"ciphertext");
    assert_eq!(updated.enqueue_state, CacheKeepaliveEnqueueState::Pending);

    assert!(
        storage
            .mark_cache_keepalive_enqueued("payload-session", record.generation, 103)
            .await?
    );
    assert!(
        !storage
            .update_cache_keepalive_payload("payload-session", record.generation, b"late", 104)
            .await?
    );
    Ok(())
}

async fn cache_hit_reschedule_preserves_duration_anchor_and_rejects_stale_generation(
    storage: &PostgresStorage,
) -> Result<()> {
    let original = storage
        .replace_from_real_request(&replace_request("hit-session", b"ciphertext-one", 100))
        .await?;

    let stale = storage
        .reschedule_after_cache_hit(&CacheKeepaliveHitRefreshRequest {
            session_key_hash: "hit-session".to_owned(),
            generation: original.generation + 1,
            cache_anchor_at_unix_secs: 150,
            run_at_unix_secs: 420,
            expires_at_unix_secs: 450,
            encrypted_payload: None,
            now_unix_secs: 151,
        })
        .await?;
    assert!(stale.is_none());

    let updated = storage
        .reschedule_after_cache_hit(&CacheKeepaliveHitRefreshRequest {
            session_key_hash: "hit-session".to_owned(),
            generation: original.generation,
            cache_anchor_at_unix_secs: 150,
            run_at_unix_secs: 420,
            expires_at_unix_secs: 450,
            encrypted_payload: None,
            now_unix_secs: 151,
        })
        .await?
        .expect("row updated");

    assert_eq!(updated.generation, 2);
    assert_eq!(updated.refresh_count, 1);
    assert_eq!(updated.first_scheduled_at_unix_secs, 100);
    assert_eq!(updated.cache_anchor_at_unix_secs, 150);
    assert_eq!(updated.run_at_unix_secs, 420);
    assert_eq!(updated.encrypted_payload, b"ciphertext-one");
    let migrated = storage
        .reschedule_after_cache_hit(&CacheKeepaliveHitRefreshRequest {
            session_key_hash: "hit-session".to_owned(),
            generation: updated.generation,
            cache_anchor_at_unix_secs: 151,
            run_at_unix_secs: 421,
            expires_at_unix_secs: 451,
            encrypted_payload: Some(b"compressed-ciphertext".to_vec()),
            now_unix_secs: 152,
        })
        .await?
        .expect("row migrated");
    assert_eq!(migrated.generation, 3);
    assert_eq!(migrated.refresh_count, 2);
    assert_eq!(migrated.encrypted_payload, b"compressed-ciphertext");
    Ok(())
}

async fn conditional_enqueue_terminal_and_purge_are_generation_safe(
    storage: &PostgresStorage,
) -> Result<()> {
    let record = storage
        .replace_from_real_request(&replace_request("terminal-session", b"ciphertext", 50))
        .await?;

    assert!(
        storage
            .mark_cache_keepalive_enqueued("terminal-session", record.generation, 101)
            .await?
    );
    assert!(
        !storage
            .mark_cache_keepalive_terminal(
                "terminal-session",
                record.generation + 1,
                CacheKeepaliveTerminalReason::CacheMiss,
                None,
                102,
            )
            .await?
    );
    assert!(
        storage
            .mark_cache_keepalive_terminal(
                "terminal-session",
                record.generation,
                CacheKeepaliveTerminalReason::CacheMiss,
                None,
                102,
            )
            .await?
    );
    let terminal = storage
        .get_cache_keepalive_session("terminal-session")
        .await?
        .expect("terminal row exists");
    assert!(terminal.encrypted_payload.is_empty());
    let check = storage
        .check_cache_keepalive_generation("terminal-session")
        .await?
        .expect("row exists");
    assert_eq!(check.status, CacheKeepaliveSessionStatus::Terminal);
    assert_eq!(check.enqueue_state, CacheKeepaliveEnqueueState::Pending);

    let removed = storage.purge_cache_keepalive_expired(351).await?;
    assert_eq!(removed, 1);
    assert!(
        storage
            .get_cache_keepalive_session("terminal-session")
            .await?
            .is_none()
    );
    Ok(())
}

async fn terminal_error_is_truncated_and_resets_running_enqueue_state(
    storage: &PostgresStorage,
) -> Result<()> {
    let record = storage
        .replace_from_real_request(&replace_request(
            "terminal-error-session",
            b"ciphertext",
            400,
        ))
        .await?;
    storage
        .mark_cache_keepalive_enqueued(&record.session_key_hash, record.generation, 401)
        .await?;
    storage
        .claim_cache_keepalive_turn(&record.session_key_hash, record.generation, 402)
        .await?;
    let error = "é".repeat(300);
    storage
        .mark_cache_keepalive_terminal(
            &record.session_key_hash,
            record.generation,
            CacheKeepaliveTerminalReason::DispatchError,
            Some(&error),
            403,
        )
        .await?;
    let terminal = storage
        .get_cache_keepalive_session(&record.session_key_hash)
        .await?
        .expect("terminal session exists");
    let persisted_error = terminal.error.as_deref().expect("terminal error exists");
    assert_eq!(persisted_error.len(), 512);
    assert_eq!(persisted_error.chars().count(), 256);
    assert_eq!(terminal.enqueue_state, CacheKeepaliveEnqueueState::Pending);

    let cancelled = storage
        .replace_from_real_request(&replace_request("cancelled-session", b"ciphertext", 410))
        .await?;
    storage
        .mark_cache_keepalive_enqueued(&cancelled.session_key_hash, cancelled.generation, 411)
        .await?;
    storage
        .claim_cache_keepalive_turn(&cancelled.session_key_hash, cancelled.generation, 412)
        .await?;
    storage
        .cancel_cache_keepalive_fenced(
            &cancelled.session_key_hash,
            None,
            410,
            CacheKeepaliveTerminalReason::Cancelled,
            413,
        )
        .await?;
    let cancelled = storage
        .get_cache_keepalive_session(&cancelled.session_key_hash)
        .await?
        .expect("cancelled session exists");
    assert_eq!(cancelled.enqueue_state, CacheKeepaliveEnqueueState::Pending);
    assert_eq!(cancelled.error, None);

    let latest = storage
        .replace_from_real_request(&replace_request("latest-session", b"ciphertext", 420))
        .await?;
    storage
        .mark_cache_keepalive_enqueued(&latest.session_key_hash, latest.generation, 421)
        .await?;
    storage
        .claim_cache_keepalive_turn(&latest.session_key_hash, latest.generation, 422)
        .await?;
    storage
        .mark_latest_cache_keepalive_terminal(
            &latest.session_key_hash,
            CacheKeepaliveTerminalReason::Cancelled,
            423,
        )
        .await?;
    let latest = storage
        .get_cache_keepalive_session(&latest.session_key_hash)
        .await?
        .expect("latest session exists");
    assert_eq!(latest.enqueue_state, CacheKeepaliveEnqueueState::Pending);
    assert_eq!(latest.error, None);

    let active = storage
        .replace_from_real_request(&replace_request("reschedule-session", b"ciphertext", 430))
        .await?;
    sqlx::query(
        "UPDATE cache_keepalive_sessions SET error = $1 WHERE session_key_hash = $2 AND generation = $3",
    )
    .bind("stale error")
    .bind(&active.session_key_hash)
    .bind(i64::try_from(active.generation)?)
    .execute(storage.pool())
    .await?;
    let refreshed = storage
        .reschedule_after_cache_hit(&hit_request(&active.session_key_hash, active.generation))
        .await?
        .expect("rescheduled session exists");
    assert_eq!(refreshed.error, None);
    Ok(())
}

async fn mark_latest_terminal_only_mutates_active_latest_session(
    storage: &PostgresStorage,
) -> Result<()> {
    let session_key_hash = "latest-terminal";
    let first = storage
        .replace_from_real_request(&replace_request(session_key_hash, b"ciphertext-one", 100))
        .await?;
    let second = storage
        .replace_from_real_request(&replace_request(session_key_hash, b"ciphertext-two", 110))
        .await?;

    assert_eq!(first.generation, 1);
    assert_eq!(second.generation, 2);
    assert!(
        storage
            .mark_latest_cache_keepalive_terminal(
                session_key_hash,
                CacheKeepaliveTerminalReason::Cancelled,
                120,
            )
            .await?
    );
    assert!(
        !storage
            .mark_latest_cache_keepalive_terminal(
                session_key_hash,
                CacheKeepaliveTerminalReason::CacheMiss,
                121,
            )
            .await?
    );

    let record = storage
        .get_cache_keepalive_session(session_key_hash)
        .await?
        .expect("row exists");
    assert_eq!(record.generation, 2);
    assert_eq!(record.status, CacheKeepaliveSessionStatus::Terminal);
    assert_eq!(
        record.terminal_reason,
        Some(CacheKeepaliveTerminalReason::Cancelled)
    );
    assert!(record.encrypted_payload.is_empty());
    Ok(())
}

async fn recover_stale_running_requires_active_running_strictly_old_lease_and_scrubs_payload(
    storage: &PostgresStorage,
) -> Result<()> {
    let cutoff = 200;
    let now = 500;
    let mut before = Vec::new();

    for (key, status, enqueue_state, lease) in [
        ("stale-running", "active", "running", Some(199_i64)),
        ("exact-cutoff", "active", "running", Some(200)),
        ("fresh-running", "active", "running", Some(201)),
        ("null-lease", "active", "running", None),
        ("pending", "active", "pending", Some(199)),
        ("enqueued", "active", "enqueued", Some(199)),
        ("terminal", "terminal", "running", Some(199)),
    ] {
        let mut request = replace_request(key, b"ciphertext", 100);
        request.accounting_key_id = Some(format!("accounting-{key}"));
        storage.replace_from_real_request(&request).await?;
        request.now_unix_secs = 101;
        storage.replace_from_real_request(&request).await?;

        sqlx::query(
            "UPDATE cache_keepalive_sessions
             SET status = $1, enqueue_state = $2, running_since_unix_secs = $3, error = $4
             WHERE session_key_hash = $5",
        )
        .bind(status)
        .bind(enqueue_state)
        .bind(lease)
        .bind("previous error")
        .bind(key)
        .execute(storage.pool())
        .await?;
        before.push(
            storage
                .get_cache_keepalive_session(key)
                .await?
                .expect("seeded session exists"),
        );
    }

    let affected = storage
        .recover_cache_keepalive_stale_running(cutoff, now)
        .await?;
    assert!(
        affected >= 1,
        "the seeded stale running session must be recovered"
    );

    let mut expected = before.remove(0);
    let recovered = storage
        .get_cache_keepalive_session(&expected.session_key_hash)
        .await?
        .expect("recovered session remains persisted");
    assert_eq!(recovered.status, CacheKeepaliveSessionStatus::Terminal);
    assert_eq!(
        recovered.terminal_reason,
        Some(CacheKeepaliveTerminalReason::Stale)
    );
    assert_eq!(recovered.enqueue_state, CacheKeepaliveEnqueueState::Pending);
    assert_eq!(
        recovered.error.as_deref(),
        Some("keepalive renewal timed out while running")
    );
    assert_eq!(recovered.running_since_unix_secs, None);
    assert!(recovered.encrypted_payload.is_empty());
    assert_eq!(recovered.updated_at_unix_secs, now);
    assert_eq!(recovered.generation, expected.generation);
    assert_eq!(recovered.current_job_key, expected.current_job_key);

    expected.status = CacheKeepaliveSessionStatus::Terminal;
    expected.terminal_reason = Some(CacheKeepaliveTerminalReason::Stale);
    expected.enqueue_state = CacheKeepaliveEnqueueState::Pending;
    expected.error = Some("keepalive renewal timed out while running".to_owned());
    expected.running_since_unix_secs = None;
    expected.encrypted_payload.clear();
    expected.updated_at_unix_secs = now;
    assert_eq!(recovered, expected, "unrelated session fields must survive");

    for control in before {
        let persisted = storage
            .get_cache_keepalive_session(&control.session_key_hash)
            .await?
            .expect("noncandidate remains persisted");
        assert_eq!(
            persisted, control,
            "noncandidates must retain their payload, identity, and all other fields"
        );
    }
    storage
        .recover_cache_keepalive_stale_running(cutoff, now + 1)
        .await?;

    for key in [
        "stale-running",
        "exact-cutoff",
        "fresh-running",
        "null-lease",
        "pending",
        "enqueued",
        "terminal",
    ] {
        sqlx::query("DELETE FROM cache_keepalive_sessions WHERE session_key_hash = $1")
            .bind(key)
            .execute(storage.pool())
            .await?;
    }
    Ok(())
}

async fn purge_stale_pending_keeps_enqueued_work(storage: &PostgresStorage) -> Result<()> {
    delete_sessions(storage, &["stale-pending", "old-enqueued"]).await?;
    let pending = storage
        .replace_from_real_request(&replace_request("stale-pending", b"ciphertext", 100))
        .await?;
    let enqueued = storage
        .replace_from_real_request(&replace_request("old-enqueued", b"ciphertext", 110))
        .await?;
    assert!(
        storage
            .mark_cache_keepalive_enqueued("old-enqueued", enqueued.generation, 111)
            .await?
    );

    let removed = storage.purge_cache_keepalive_stale_pending(112).await?;

    assert_eq!(pending.enqueue_state, CacheKeepaliveEnqueueState::Pending);
    assert!(
        removed >= 1,
        "the seeded stale pending session must be purged"
    );
    assert!(
        storage
            .get_cache_keepalive_session("stale-pending")
            .await?
            .is_none()
    );
    assert!(
        storage
            .get_cache_keepalive_session("old-enqueued")
            .await?
            .is_some()
    );
    delete_sessions(storage, &["stale-pending", "old-enqueued"]).await
}

async fn delete_sessions(storage: &PostgresStorage, keys: &[&str]) -> Result<()> {
    for key in keys {
        sqlx::query("DELETE FROM cache_keepalive_sessions WHERE session_key_hash = $1")
            .bind(key)
            .execute(storage.pool())
            .await?;
    }
    Ok(())
}

async fn recover_stale_running_rejects_overflow_without_mutation(
    storage: &PostgresStorage,
) -> Result<()> {
    let key = "adapter-overflow";
    delete_sessions(storage, &[key]).await?;
    let record = storage
        .replace_from_real_request(&replace_request(key, b"overflow", 100))
        .await?;
    assert!(
        storage
            .mark_cache_keepalive_enqueued(key, record.generation, 110)
            .await?
    );
    assert!(
        storage
            .claim_cache_keepalive_turn(key, record.generation, 150)
            .await?
    );
    let before = storage
        .get_cache_keepalive_session(key)
        .await?
        .expect("running session exists");

    for (cutoff, now) in [(200, u64::MAX), (u64::MAX, 500)] {
        assert!(
            storage
                .recover_cache_keepalive_stale_running(cutoff, now)
                .await
                .is_err(),
            "out-of-range recovery timestamp must be rejected"
        );
        assert_eq!(
            storage
                .get_cache_keepalive_session(key)
                .await?
                .expect("session remains"),
            before,
            "overflow rejection must not mutate persisted state"
        );
    }
    delete_sessions(storage, &[key]).await
}

async fn purge_expired_uses_a_strict_cutoff_boundary(storage: &PostgresStorage) -> Result<()> {
    let keys = [
        "adapter-expired-before",
        "adapter-expired-exact",
        "adapter-expired-future",
    ];
    delete_sessions(storage, &keys).await?;
    for (key, now) in [
        ("adapter-expired-before", 99),
        ("adapter-expired-exact", 100),
        ("adapter-expired-future", 101),
    ] {
        storage
            .replace_from_real_request(&replace_request(key, b"expiry", now))
            .await?;
    }

    let removed = storage.purge_cache_keepalive_expired(400).await?;
    assert!(
        removed >= 1,
        "the seeded before-cutoff session must be purged"
    );
    assert!(
        storage
            .get_cache_keepalive_session(keys[0])
            .await?
            .is_none()
    );
    assert!(
        storage
            .get_cache_keepalive_session(keys[1])
            .await?
            .is_some(),
        "expiry equal to cutoff must not be purged"
    );
    assert!(
        storage
            .get_cache_keepalive_session(keys[2])
            .await?
            .is_some()
    );
    let removed = storage.purge_cache_keepalive_expired(401).await?;
    assert!(
        removed >= 1,
        "the seeded exact-cutoff session must be purged after advancing the cutoff"
    );
    assert!(
        storage
            .get_cache_keepalive_session(keys[1])
            .await?
            .is_none()
    );
    assert!(
        storage
            .get_cache_keepalive_session(keys[2])
            .await?
            .is_some()
    );
    delete_sessions(storage, &keys).await
}

async fn purge_stale_pending_uses_strict_cutoff_and_state_predicates(
    storage: &PostgresStorage,
) -> Result<()> {
    let keys = [
        "adapter-pending-stale",
        "adapter-pending-exact",
        "adapter-running-old",
        "adapter-terminal-old",
        "adapter-enqueued-old",
    ];
    delete_sessions(storage, &keys).await?;
    for key in keys {
        storage
            .replace_from_real_request(&replace_request(key, b"pending", 100))
            .await?;
    }
    for (key, status, enqueue_state, lease, updated_at) in [
        ("adapter-pending-stale", "active", "pending", None, 199_i64),
        ("adapter-pending-exact", "active", "pending", None, 200_i64),
        (
            "adapter-running-old",
            "active",
            "running",
            Some(199_i64),
            199_i64,
        ),
        ("adapter-terminal-old", "terminal", "pending", None, 199_i64),
        ("adapter-enqueued-old", "active", "enqueued", None, 199_i64),
    ] {
        sqlx::query(
            "UPDATE cache_keepalive_sessions
             SET status = $1, enqueue_state = $2, running_since_unix_secs = $3, updated_at = $4
             WHERE session_key_hash = $5",
        )
        .bind(status)
        .bind(enqueue_state)
        .bind(lease)
        .bind(updated_at)
        .bind(key)
        .execute(storage.pool())
        .await?;
    }

    let removed = storage.purge_cache_keepalive_stale_pending(200).await?;
    assert!(
        removed >= 1,
        "the seeded stale pending session must be purged"
    );
    assert!(
        storage
            .get_cache_keepalive_session(keys[0])
            .await?
            .is_none()
    );
    for (key, expected_status, expected_enqueue_state) in [
        (
            "adapter-pending-exact",
            CacheKeepaliveSessionStatus::Active,
            CacheKeepaliveEnqueueState::Pending,
        ),
        (
            "adapter-running-old",
            CacheKeepaliveSessionStatus::Active,
            CacheKeepaliveEnqueueState::Running,
        ),
        (
            "adapter-terminal-old",
            CacheKeepaliveSessionStatus::Terminal,
            CacheKeepaliveEnqueueState::Pending,
        ),
        (
            "adapter-enqueued-old",
            CacheKeepaliveSessionStatus::Active,
            CacheKeepaliveEnqueueState::Enqueued,
        ),
    ] {
        let persisted = storage
            .get_cache_keepalive_session(key)
            .await?
            .expect("excluded state remains persisted");
        assert_eq!(persisted.status, expected_status);
        assert_eq!(persisted.enqueue_state, expected_enqueue_state);
    }
    delete_sessions(storage, &keys).await
}

async fn concurrent_recovery_counts_once_and_replacement_reschedule_preserve_latest_active_payload(
    storage: &PostgresStorage,
) -> Result<()> {
    let key = "adapter-concurrent-recovery";
    let latest_key = "adapter-latest-before-recovery";
    delete_sessions(storage, &[key, latest_key]).await?;
    let storage = Arc::new(storage.clone());
    let running = storage
        .replace_from_real_request(&replace_request(key, b"concurrent-running", 100))
        .await?;
    assert!(
        storage
            .mark_cache_keepalive_enqueued(key, running.generation, 110)
            .await?
    );
    assert!(
        storage
            .claim_cache_keepalive_turn(key, running.generation, 150)
            .await?
    );
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let first = tokio::spawn({
        let storage = Arc::clone(&storage);
        let barrier = Arc::clone(&barrier);
        async move {
            barrier.wait().await;
            storage
                .recover_cache_keepalive_stale_running(200, 500)
                .await
        }
    });
    let second = tokio::spawn({
        let storage = Arc::clone(&storage);
        let barrier = Arc::clone(&barrier);
        async move {
            barrier.wait().await;
            storage
                .recover_cache_keepalive_stale_running(200, 500)
                .await
        }
    });
    let (first, second) = tokio::try_join!(first, second)?;
    let first = first?;
    let second = second?;
    assert!(
        first + second >= 1,
        "the seeded stale running session must be recovered"
    );
    let recovered = storage
        .get_cache_keepalive_session(key)
        .await?
        .expect("recovered concurrent session exists");
    assert_eq!(recovered.status, CacheKeepaliveSessionStatus::Terminal);
    assert!(recovered.encrypted_payload.is_empty());

    let previous = storage
        .replace_from_real_request(&replace_request(latest_key, b"previous-generation", 100))
        .await?;
    assert!(
        storage
            .mark_cache_keepalive_enqueued(latest_key, previous.generation, 110)
            .await?
    );
    assert!(
        storage
            .claim_cache_keepalive_turn(latest_key, previous.generation, 150)
            .await?
    );
    let latest = storage
        .replace_from_real_request(&replace_request(latest_key, b"latest-before-recovery", 600))
        .await?;
    let refreshed = storage
        .reschedule_after_cache_hit(&CacheKeepaliveHitRefreshRequest {
            session_key_hash: latest_key.to_owned(),
            generation: latest.generation,
            cache_anchor_at_unix_secs: 650,
            run_at_unix_secs: 920,
            expires_at_unix_secs: 950,
            encrypted_payload: Some(b"latest-rescheduled-payload".to_vec()),
            now_unix_secs: 650,
        })
        .await?
        .expect("latest active generation remains");
    assert_eq!(refreshed.generation, latest.generation + 1);
    assert_eq!(refreshed.encrypted_payload, b"latest-rescheduled-payload");
    assert_eq!(refreshed.status, CacheKeepaliveSessionStatus::Active);
    assert_eq!(refreshed.enqueue_state, CacheKeepaliveEnqueueState::Pending);
    storage
        .recover_cache_keepalive_stale_running(700, 800)
        .await?;
    assert_eq!(
        storage
            .get_cache_keepalive_session(latest_key)
            .await?
            .expect("latest exists"),
        refreshed
    );
    delete_sessions(&storage, &[key, latest_key]).await
}

async fn concurrent_replace_from_real_request_bumps_each_generation(
    storage: &PostgresStorage,
) -> Result<()> {
    let session_key_hash = "concurrent-replace";
    let original = storage
        .replace_from_real_request(&replace_request(session_key_hash, b"init", 100))
        .await?;
    assert_eq!(original.generation, 1);
    let barrier = Arc::new(tokio::sync::Barrier::new(2));

    let first = tokio::spawn({
        let storage = storage.clone();
        let barrier = Arc::clone(&barrier);
        async move {
            barrier.wait().await;
            storage
                .replace_from_real_request(&replace_request(session_key_hash, b"first", 110))
                .await
        }
    });
    let second = tokio::spawn({
        let storage = storage.clone();
        let barrier = Arc::clone(&barrier);
        async move {
            barrier.wait().await;
            storage
                .replace_from_real_request(&replace_request(session_key_hash, b"second", 120))
                .await
        }
    });

    let (first, second) = tokio::try_join!(first, second)?;
    let mut generations = vec![first?.generation, second?.generation];
    generations.sort_unstable();
    assert_eq!(generations, [2, 3]);
    let final_record = storage
        .get_cache_keepalive_session(session_key_hash)
        .await?
        .expect("row exists");
    assert_eq!(final_record.generation, 3);
    assert_eq!(
        final_record.current_job_key,
        cache_keepalive_job_key(session_key_hash, 3)
    );
    Ok(())
}

async fn concurrent_hit_reschedule_allows_only_one_generation_cas(
    storage: &PostgresStorage,
) -> Result<()> {
    let session_key_hash = "concurrent-hit-cas";
    let original = storage
        .replace_from_real_request(&replace_request(session_key_hash, b"init", 100))
        .await?;
    let barrier = Arc::new(tokio::sync::Barrier::new(2));

    let first = tokio::spawn({
        let storage = storage.clone();
        let barrier = Arc::clone(&barrier);
        async move {
            barrier.wait().await;
            storage
                .reschedule_after_cache_hit(&hit_request(session_key_hash, original.generation))
                .await
        }
    });
    let second = tokio::spawn({
        let storage = storage.clone();
        let barrier = Arc::clone(&barrier);
        async move {
            barrier.wait().await;
            storage
                .reschedule_after_cache_hit(&hit_request(session_key_hash, original.generation))
                .await
        }
    });

    let (first, second) = tokio::try_join!(first, second)?;
    let first = first?;
    let second = second?;
    assert_eq!(first.is_some() as u8 + second.is_some() as u8, 1);
    let winner = first.or(second).expect("one CAS succeeds");
    assert_eq!(winner.generation, 2);
    let final_record = storage
        .get_cache_keepalive_session(session_key_hash)
        .await?
        .expect("row exists");
    assert_eq!(final_record.generation, 2);
    assert_eq!(final_record.refresh_count, 1);
    Ok(())
}

async fn pending_claim_requires_payload_and_mark_cannot_overwrite_running(
    storage: &PostgresStorage,
) -> Result<()> {
    let session_key_hash = "pending-claim-payload";
    let record = storage
        .replace_from_real_request(&replace_request(session_key_hash, b"", 100))
        .await?;
    assert!(
        !storage
            .claim_cache_keepalive_turn(session_key_hash, record.generation, 101)
            .await?
    );
    assert!(
        storage
            .update_cache_keepalive_payload(
                session_key_hash,
                record.generation,
                b"ciphertext",
                102,
            )
            .await?
    );
    assert!(
        storage
            .claim_cache_keepalive_turn(session_key_hash, record.generation, 103)
            .await?
    );
    let claimed = storage
        .get_cache_keepalive_session(session_key_hash)
        .await?
        .expect("running session exists");
    assert_eq!(claimed.enqueue_state, CacheKeepaliveEnqueueState::Running);
    assert_eq!(claimed.running_since_unix_secs, Some(103));
    assert!(
        !storage
            .mark_cache_keepalive_enqueued(session_key_hash, record.generation, 104)
            .await?
    );
    let unchanged = storage
        .get_cache_keepalive_session(session_key_hash)
        .await?
        .expect("running session remains");
    assert_eq!(unchanged.enqueue_state, CacheKeepaliveEnqueueState::Running);
    assert_eq!(unchanged.running_since_unix_secs, Some(103));
    Ok(())
}

async fn concurrent_claim_allows_exactly_one_enqueued_generation_winner(
    storage: &PostgresStorage,
) -> Result<()> {
    let session_key_hash = "concurrent-claim-cas";
    let record = storage
        .replace_from_real_request(&replace_request(session_key_hash, b"init", 100))
        .await?;
    assert!(
        storage
            .mark_cache_keepalive_enqueued(session_key_hash, record.generation, 101)
            .await?
    );
    let barrier = Arc::new(tokio::sync::Barrier::new(2));

    let first = tokio::spawn({
        let storage = storage.clone();
        let barrier = Arc::clone(&barrier);
        async move {
            barrier.wait().await;
            storage
                .claim_cache_keepalive_turn(session_key_hash, record.generation, 200)
                .await
        }
    });
    let second = tokio::spawn({
        let storage = storage.clone();
        let barrier = Arc::clone(&barrier);
        async move {
            barrier.wait().await;
            storage
                .claim_cache_keepalive_turn(session_key_hash, record.generation, 200)
                .await
        }
    });

    let (first, second) = tokio::try_join!(first, second)?;
    let first = first?;
    let second = second?;
    assert_eq!(u8::from(first) + u8::from(second), 1);

    let claimed = storage
        .get_cache_keepalive_session(session_key_hash)
        .await?
        .expect("session exists");
    assert_eq!(claimed.enqueue_state, CacheKeepaliveEnqueueState::Running);
    assert_eq!(claimed.running_since_unix_secs, Some(200));
    Ok(())
}

async fn ticketed_completions_use_stable_logical_anchor_and_reject_slow_older_responses(
    storage: &PostgresStorage,
) -> Result<()> {
    let key = "ticketed-stable-logical-anchor";
    let original = storage
        .replace_from_real_request(&replace_request(key, b"original-prefix", 100))
        .await?;
    let replacement_ticket = storage
        .allocate_cache_keepalive_completion_ticket(key)
        .await?;
    let refreshed = storage
        .reschedule_after_cache_hit(&CacheKeepaliveHitRefreshRequest {
            session_key_hash: key.to_owned(),
            generation: original.generation,
            cache_anchor_at_unix_secs: 371,
            run_at_unix_secs: 641,
            expires_at_unix_secs: 671,
            encrypted_payload: None,
            now_unix_secs: 371,
        })
        .await?
        .expect("refreshed generation");
    assert_eq!(refreshed.first_scheduled_at_unix_secs, 100);
    let active = storage
        .replace_from_real_request_fenced(
            &replace_request(key, b"new-real-prefix", 370),
            Some(replacement_ticket),
        )
        .await?
        .expect("real logical anchor is newer despite older sliding anchor");
    assert_eq!(active.first_scheduled_at_unix_secs, 370);
    assert_eq!(active.cache_anchor_at_unix_secs, 370);
    assert_eq!(active.generation, refreshed.generation + 1);

    let cancel_ticket = storage
        .allocate_cache_keepalive_completion_ticket(key)
        .await?;
    assert!(
        !storage
            .cancel_cache_keepalive_fenced(
                key,
                Some(cancel_ticket),
                369,
                CacheKeepaliveTerminalReason::Cancelled,
                400,
            )
            .await?
    );
    assert_eq!(
        storage
            .get_meta_value(&cc_lb_storage_api::cache_keepalive_order_meta_key(key))
            .await?,
        Some("2:1".to_owned())
    );
    assert_eq!(
        storage
            .get_cache_keepalive_session(key)
            .await?
            .expect("protected real generation exists"),
        active
    );
    assert!(
        storage
            .cancel_cache_keepalive_fenced(
                key,
                Some(cancel_ticket),
                372,
                CacheKeepaliveTerminalReason::Cancelled,
                400,
            )
            .await?
    );
    let terminal = storage
        .get_cache_keepalive_session(key)
        .await?
        .expect("logical cancellation fence exists");
    assert_eq!(terminal.first_scheduled_at_unix_secs, 372);
    assert_eq!(terminal.cache_anchor_at_unix_secs, 370);

    let newer_ticket = storage
        .allocate_cache_keepalive_completion_ticket(key)
        .await?;
    assert!(
        storage
            .replace_from_real_request_fenced(
                &replace_request(key, b"slow-old-prefix", 371),
                Some(newer_ticket),
            )
            .await?
            .is_none()
    );
    assert_eq!(
        storage
            .get_meta_value(&cc_lb_storage_api::cache_keepalive_order_meta_key(key))
            .await?,
        Some("3:2".to_owned())
    );
    let replacement = storage
        .replace_from_real_request_fenced(
            &replace_request(key, b"new-post-compaction-prefix", 373),
            Some(newer_ticket),
        )
        .await?
        .expect("newer logical anchor reactivates terminal state");
    assert_eq!(replacement.status, CacheKeepaliveSessionStatus::Active);
    assert_eq!(replacement.first_scheduled_at_unix_secs, 373);
    assert_eq!(replacement.generation, terminal.generation + 1);
    assert_eq!(replacement.encrypted_payload, b"new-post-compaction-prefix");
    assert_eq!(
        storage
            .get_meta_value(&cc_lb_storage_api::cache_keepalive_order_meta_key(key))
            .await?,
        Some("3:3".to_owned())
    );
    Ok(())
}

async fn ticketed_cancel_retires_generation_after_synthetic_refresh(
    storage: &PostgresStorage,
) -> Result<()> {
    let key = "ticketed-compaction-after-hit";
    let original = storage
        .replace_from_real_request(&replace_request(key, b"old-prefix", 100))
        .await?;
    let ticket = storage
        .allocate_cache_keepalive_completion_ticket(key)
        .await?;
    assert_eq!(ticket, 1);
    let refreshed = storage
        .reschedule_after_cache_hit(&CacheKeepaliveHitRefreshRequest {
            session_key_hash: key.to_owned(),
            generation: original.generation,
            cache_anchor_at_unix_secs: 371,
            run_at_unix_secs: 641,
            expires_at_unix_secs: 671,
            encrypted_payload: None,
            now_unix_secs: 371,
        })
        .await?
        .expect("refreshed generation");

    assert!(
        storage
            .cancel_cache_keepalive_fenced(
                key,
                Some(ticket),
                370,
                CacheKeepaliveTerminalReason::Cancelled,
                400,
            )
            .await?
    );
    let terminal = storage
        .get_cache_keepalive_session(key)
        .await?
        .expect("terminal generation exists");
    assert_eq!(terminal.generation, refreshed.generation);
    assert_eq!(terminal.status, CacheKeepaliveSessionStatus::Terminal);
    assert_eq!(terminal.cache_anchor_at_unix_secs, 371);
    assert_eq!(terminal.first_scheduled_at_unix_secs, 370);
    assert_eq!(terminal.updated_at_unix_secs, 400);
    assert!(terminal.encrypted_payload.is_empty());
    assert_eq!(
        storage
            .get_meta_value(&cc_lb_storage_api::cache_keepalive_order_meta_key(key))
            .await?,
        Some("1:1".to_owned())
    );
    assert!(
        !storage
            .cancel_cache_keepalive_fenced(
                key,
                Some(ticket),
                401,
                CacheKeepaliveTerminalReason::Cancelled,
                401,
            )
            .await?
    );
    Ok(())
}

async fn newer_ticket_replaces_terminal_generation_before_terminal_write_timestamp(
    storage: &PostgresStorage,
) -> Result<()> {
    let key = "ticketed-post-compaction-completion";
    let original = storage
        .replace_from_real_request(&replace_request(key, b"old-prefix", 100))
        .await?;
    let cancelled_ticket = storage
        .allocate_cache_keepalive_completion_ticket(key)
        .await?;
    assert!(
        storage
            .cancel_cache_keepalive_fenced(
                key,
                Some(cancelled_ticket),
                100,
                CacheKeepaliveTerminalReason::Cancelled,
                103,
            )
            .await?
    );
    let newer_ticket = storage
        .allocate_cache_keepalive_completion_ticket(key)
        .await?;
    assert_eq!(newer_ticket, cancelled_ticket + 1);
    let active = storage
        .replace_from_real_request_fenced(
            &replace_request(key, b"post-compaction-prefix", 102),
            Some(newer_ticket),
        )
        .await?
        .expect("newer ticket supersedes terminal write timestamp");
    assert_eq!(active.generation, original.generation + 1);
    assert_eq!(active.status, CacheKeepaliveSessionStatus::Active);
    assert_eq!(active.cache_anchor_at_unix_secs, 102);
    assert_eq!(active.encrypted_payload, b"post-compaction-prefix");
    assert_eq!(
        storage
            .get_meta_value(&cc_lb_storage_api::cache_keepalive_order_meta_key(key))
            .await?,
        Some("2:2".to_owned())
    );
    assert!(
        storage
            .replace_from_real_request_fenced(
                &replace_request(key, b"obsolete-completion-prefix", 200),
                Some(cancelled_ticket),
            )
            .await?
            .is_none()
    );
    assert_eq!(
        storage
            .get_cache_keepalive_session(key)
            .await?
            .expect("post-compaction generation exists"),
        active
    );
    Ok(())
}

async fn completion_tickets_preserve_active_anchor_and_legacy_timestamp_guards(
    storage: &PostgresStorage,
) -> Result<()> {
    let key = "ticketed-active-and-legacy-guards";
    let original = storage
        .replace_from_real_request(&replace_request(key, b"newer-active-prefix", 371))
        .await?;
    let ticket = storage
        .allocate_cache_keepalive_completion_ticket(key)
        .await?;
    for expected_ticket in [None, Some(ticket)] {
        assert!(
            storage
                .replace_from_real_request_fenced(
                    &replace_request(key, b"older-prefix", 370),
                    expected_ticket,
                )
                .await?
                .is_none()
        );
    }
    assert!(
        !storage
            .cancel_cache_keepalive_fenced(
                key,
                None,
                370,
                CacheKeepaliveTerminalReason::Cancelled,
                400,
            )
            .await?
    );
    assert_eq!(
        storage
            .get_cache_keepalive_session(key)
            .await?
            .expect("active generation exists"),
        original
    );
    assert_eq!(
        storage
            .get_meta_value(&cc_lb_storage_api::cache_keepalive_order_meta_key(key))
            .await?,
        Some("1:0".to_owned())
    );
    assert!(
        storage
            .cancel_cache_keepalive_fenced(
                key,
                None,
                371,
                CacheKeepaliveTerminalReason::Cancelled,
                400,
            )
            .await?
    );
    assert!(
        storage
            .replace_from_real_request_fenced(
                &replace_request(key, b"stale-terminal-prefix", 399),
                None,
            )
            .await?
            .is_none()
    );
    assert!(
        storage
            .replace_from_real_request_fenced(
                &replace_request(key, b"older-logical-prefix", 370),
                Some(ticket),
            )
            .await?
            .is_none()
    );
    let replacement = storage
        .replace_from_real_request_fenced(
            &replace_request(key, b"logically-newer-prefix", 371),
            Some(ticket),
        )
        .await?
        .expect("same ticket can replace at the logical boundary");
    assert_eq!(replacement.generation, original.generation + 1);
    assert_eq!(replacement.status, CacheKeepaliveSessionStatus::Active);
    assert_eq!(replacement.encrypted_payload, b"logically-newer-prefix");
    Ok(())
}

fn replace_request(
    session_key_hash: &str,
    payload: &[u8],
    now: u64,
) -> CacheKeepaliveReplaceRequest {
    CacheKeepaliveReplaceRequest {
        session_key_hash: session_key_hash.to_owned(),
        principal_id: "principal".to_owned(),
        upstream_id: Uuid::from_u128(7),
        cycle_id: Uuid::nil(),
        ttl: CacheTtl::Ttl5m,
        cache_anchor_at_unix_secs: now,
        run_at_unix_secs: now + 270,
        expires_at_unix_secs: now + 300,
        encrypted_payload: payload.to_vec(),
        accounting_key_id: None,
        display_reason: "agent-in-turn".to_owned(),
        config_snapshot: cc_lb_storage_api::CacheKeepaliveConfigSnapshot {
            refresh_lead_time_5m_secs: 30,
            refresh_lead_time_1h_secs: 300,
            max_refreshes_per_session: 12,
            max_total_duration_secs: 14_400,
            snapshot_max_bytes: 524_288,
        },
        now_unix_secs: now,
    }
}

fn hit_request(session_key_hash: &str, generation: u64) -> CacheKeepaliveHitRefreshRequest {
    CacheKeepaliveHitRefreshRequest {
        session_key_hash: session_key_hash.to_owned(),
        generation,
        cache_anchor_at_unix_secs: 150,
        run_at_unix_secs: 420,
        expires_at_unix_secs: 450,
        encrypted_payload: None,
        now_unix_secs: 151,
    }
}

struct Fixture {
    schema: String,
    admin_pool: PgPool,
    pool: PgPool,
}

impl Fixture {
    async fn create(url: &str) -> Result<Self> {
        let schema = format!("cache_keepalive_{}", Uuid::new_v4().simple());
        let admin_pool = PgPoolOptions::new().max_connections(1).connect(url).await?;
        sqlx::query(AssertSqlSafe(format!(
            "CREATE SCHEMA {}",
            quote_ident(&schema)
        )))
        .execute(&admin_pool)
        .await?;
        let pool = schema_pool(url, &schema).await?;
        Ok(Self {
            schema,
            admin_pool,
            pool,
        })
    }

    async fn drop_schema(self) -> Result<()> {
        self.pool.close().await;
        sqlx::query(AssertSqlSafe(format!(
            "DROP SCHEMA IF EXISTS {} CASCADE",
            quote_ident(&self.schema)
        )))
        .execute(&self.admin_pool)
        .await?;
        self.admin_pool.close().await;
        Ok(())
    }
}

async fn schema_pool(url: &str, schema: &str) -> Result<PgPool> {
    let options = PgConnectOptions::from_str(url)?.options([("search_path", schema)]);
    Ok(PgPoolOptions::new()
        .max_connections(4)
        .connect_with(options)
        .await?)
}

fn quote_ident(identifier: &str) -> String {
    assert!(
        identifier.chars().all(|character| {
            character.is_ascii_lowercase() || character.is_ascii_digit() || character == '_'
        }),
        "unsafe postgres identifier: {identifier}"
    );
    format!("\"{identifier}\"")
}
