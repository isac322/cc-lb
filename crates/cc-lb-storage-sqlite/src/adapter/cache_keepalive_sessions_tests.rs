use std::sync::Arc;

use cc_lb_clock::SystemClock;
use cc_lb_storage_api::{
    CacheKeepaliveEnqueueState, CacheKeepaliveHitRefreshRequest, CacheKeepaliveReplaceRequest,
    CacheKeepaliveSessionStatus, CacheKeepaliveSessionStore, CacheKeepaliveTerminalReason,
    CacheTtl, MetaStore, cache_keepalive_job_key,
};
use tempfile::TempDir;
use uuid::Uuid;

use crate::{SqliteStorage, open_sqlite};

async fn storage() -> (TempDir, SqliteStorage) {
    let dir = tempfile::tempdir().expect("tempdir");
    let database_url = format!("sqlite://{}", dir.path().join("keepalive.sqlite").display());
    let storage = open_sqlite(&database_url, Arc::new(SystemClock))
        .await
        .expect("open sqlite");
    storage.initialize().await.expect("initialize sqlite");
    (dir, storage)
}

fn replace_request(payload: &[u8], now: u64) -> CacheKeepaliveReplaceRequest {
    CacheKeepaliveReplaceRequest {
        session_key_hash: "session-hash".to_owned(),
        principal_id: "principal".to_owned(),
        accounting_key_id: None,
        upstream_id: Uuid::from_u128(7),
        cycle_id: Uuid::nil(),
        cache_anchor_at_unix_secs: now,
        ttl: CacheTtl::Ttl5m,
        run_at_unix_secs: now + 270,
        expires_at_unix_secs: now + 300,
        encrypted_payload: payload.to_vec(),
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

fn order_counters(raw: String) -> (u64, u64) {
    let meta = cc_lb_storage_api::parse_keepalive_order_meta(Some(&raw)).unwrap();
    (meta.issued, meta.applied)
}

// Published master fdc0f9d parser/allocator: keep this independent of v2 helpers.
async fn allocate_deployed_legacy_ticket(storage: &SqliteStorage, key: &str) -> u64 {
    let key = format!("keepalive_order:{key}");
    let mut tx = storage.begin_immediate().await.unwrap();
    let existing = sqlx::query_scalar::<_, String>("SELECT value FROM meta_v1 WHERE key = ?")
        .bind(&key)
        .fetch_optional(&mut *tx)
        .await
        .unwrap();
    let (issued, applied) = deployed_legacy_counters(existing.as_deref());
    let next = issued.checked_add(1).unwrap();
    sqlx::query(
        "INSERT INTO meta_v1 (key, value) VALUES (?, ?)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
    )
    .bind(&key)
    .bind(format!("{next}:{applied}"))
    .execute(&mut *tx)
    .await
    .unwrap();
    tx.commit().await.unwrap();
    next
}

fn deployed_legacy_counters(value: Option<&str>) -> (u64, u64) {
    let Some(raw) = value else { return (0, 0) };
    let Some((issued_raw, applied_raw)) = raw.split_once(':') else {
        return (0, 0);
    };
    let issued = issued_raw.parse::<u64>().unwrap_or(0);
    let applied = applied_raw.parse::<u64>().unwrap_or(0);
    (issued, applied)
}

#[tokio::test]
async fn mixed_version_metadata_namespace_isolates_deployed_legacy_allocator_and_gc() {
    // QA: numeric floor, untouched legacy reader/writer, exact deployed orphan GC,
    // fresh ownership, begin/finalize, and existing v2 ignoring corrupt legacy.
    let (_directory, storage) = storage().await;
    let key = "mixed-version";
    let legacy_key = format!("keepalive_order:{key}");
    storage.put_meta_value(&legacy_key, "40:17").await.unwrap();
    assert_eq!(allocate_deployed_legacy_ticket(&storage, key).await, 41);
    let ticket = storage
        .allocate_cache_keepalive_completion_ticket(key, 100, 900)
        .await
        .unwrap();
    assert_eq!(ticket.sequence, 42);
    let legacy = storage.get_meta_value(&legacy_key).await.unwrap();
    assert_eq!(
        deployed_legacy_counters(legacy.as_deref()),
        (41, 17),
        "deployed legacy reader must still see its numeric pair"
    );
    let v2_key = cc_lb_storage_api::cache_keepalive_order_meta_key(key);
    let raw = storage.get_meta_value(&v2_key).await.unwrap().unwrap();
    let meta = cc_lb_storage_api::parse_keepalive_order_meta(Some(&raw)).unwrap();
    assert_eq!(meta.applied, 0, "legacy applied is not a v2 owner");
    assert_eq!(meta.lease.unwrap().epoch, ticket.epoch);
    assert_eq!(allocate_deployed_legacy_ticket(&storage, key).await, 42);
    assert_eq!(
        storage.get_meta_value(&v2_key).await.unwrap().as_deref(),
        Some(raw.as_str())
    );
    sqlx::query(
        "DELETE FROM meta_v1 WHERE key LIKE 'keepalive_order:%'
         AND substr(key, 17) NOT IN (SELECT session_key_hash FROM cache_keepalive_sessions)",
    )
    .execute(storage.pool())
    .await
    .unwrap();
    assert!(storage.get_meta_value(&legacy_key).await.unwrap().is_none());
    assert_eq!(
        storage.get_meta_value(&v2_key).await.unwrap().as_deref(),
        Some(raw.as_str())
    );
    let mut request = replace_request(b"modern", 101);
    request.session_key_hash = key.to_owned();
    assert!(
        storage
            .replace_from_real_request_fenced(&request, Some(ticket))
            .await
            .unwrap()
            .is_some()
    );
    let owner = storage
        .allocate_cache_keepalive_completion_ticket(key, 102, 1_000)
        .await
        .unwrap();
    assert!(
        storage
            .begin_cache_keepalive_request_fenced(key, Some(owner), 102, 950)
            .await
            .unwrap()
    );
    let owned_raw = storage.get_meta_value(&v2_key).await.unwrap();
    assert_eq!(allocate_deployed_legacy_ticket(&storage, key).await, 1);
    assert_eq!(storage.get_meta_value(&v2_key).await.unwrap(), owned_raw);
    storage
        .put_meta_value(&legacy_key, "corrupt")
        .await
        .unwrap();
    let later = storage
        .allocate_cache_keepalive_completion_ticket(key, 103, 200)
        .await
        .unwrap();
    assert_eq!(later.epoch, owner.epoch);
    assert_eq!(later.sequence, owner.sequence + 1);
    assert_eq!(later.valid_until_unix_secs, 200);
    assert_eq!(
        storage
            .get_meta_value(&legacy_key)
            .await
            .unwrap()
            .as_deref(),
        Some("corrupt")
    );
    let updated = storage.get_meta_value(&v2_key).await.unwrap().unwrap();
    assert_eq!(
        cc_lb_storage_api::parse_keepalive_order_meta(Some(&updated))
            .unwrap()
            .lease
            .unwrap()
            .protected_until_unix_secs,
        1_000
    );
    assert!(
        storage
            .cancel_cache_keepalive_fenced(
                key,
                Some(owner),
                102,
                CacheKeepaliveTerminalReason::Cancelled,
                Some("request_failed"),
                104
            )
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn housekeeping_expires_order_metadata_after_request_lifetime() {
    let (_directory, storage) = storage().await;
    let key = "bounded-order";
    storage
        .allocate_cache_keepalive_completion_ticket(key, 100, 200)
        .await
        .unwrap();
    let meta_key = cc_lb_storage_api::cache_keepalive_order_meta_key(key);
    for cutoff in [100, 200] {
        storage.purge_cache_keepalive_expired(cutoff).await.unwrap();
        assert!(storage.get_meta_value(&meta_key).await.unwrap().is_some());
    }
    storage.purge_cache_keepalive_expired(201).await.unwrap();
    assert!(
        storage.get_meta_value(&meta_key).await.unwrap().is_none(),
        "orphan order metadata must expire after its request protection window"
    );
}

#[tokio::test]
async fn housekeeping_retains_protected_order_counters_and_fails_closed_at_overflow() {
    let (_dir, storage) = storage().await;
    for stale_pending in [false, true] {
        let key = format!("retained-order-{stale_pending}");
        let mut request = replace_request(b"expiring-payload", 100);
        request.session_key_hash = key.clone();
        storage.replace_from_real_request(&request).await.unwrap();
        assert_eq!(
            storage
                .allocate_cache_keepalive_completion_ticket(&key, 100, 10_000_000)
                .await
                .unwrap()
                .sequence,
            1
        );
        if stale_pending {
            assert_eq!(
                storage
                    .purge_cache_keepalive_stale_pending(101)
                    .await
                    .unwrap(),
                1
            );
        } else {
            assert_eq!(storage.purge_cache_keepalive_expired(401).await.unwrap(), 1);
        }
        assert!(
            storage
                .get_cache_keepalive_session(&key)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            storage
                .purge_cache_keepalive_expired(1_000_000)
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            storage
                .get_meta_value(&cc_lb_storage_api::cache_keepalive_order_meta_key(&key))
                .await
                .unwrap()
                .map(order_counters),
            Some((1, 0))
        );
        assert_eq!(
            storage
                .allocate_cache_keepalive_completion_ticket(&key, 100, 10_000_000)
                .await
                .unwrap()
                .sequence,
            2
        );
    }
    let key = "retained-order-overflow";
    let order_key = cc_lb_storage_api::cache_keepalive_order_meta_key(key);
    let exhausted = format!("v2:10000000:100:{}:{}:7", Uuid::from_u128(7), u64::MAX);
    storage
        .put_meta_value(&order_key, &exhausted)
        .await
        .unwrap();
    assert_eq!(
        storage
            .purge_cache_keepalive_expired(1_000_000)
            .await
            .unwrap(),
        0
    );
    assert!(matches!(
        storage.allocate_cache_keepalive_completion_ticket(key, 100, 10_000_000).await,
        Err(cc_lb_storage_api::StorageError::Fatal { message })
            if message == "cache keepalive completion ticket overflow"
    ));
    assert_eq!(
        storage.get_meta_value(&order_key).await.unwrap(),
        Some(exhausted)
    );
}

#[tokio::test]
async fn housekeeping_never_recycles_in_flight_request_owners() {
    // QA: absent-row purge between processes, both completion kinds, an untouched
    // B-owner control, and unchanged marker/order metadata after a stale completion.
    let (dir, storage) = storage().await;
    let database_url = format!("sqlite://{}", dir.path().join("keepalive.sqlite").display());
    let other = open_sqlite(&database_url, Arc::new(SystemClock))
        .await
        .expect("open independent sqlite adapter");
    let mut failures = Vec::new();
    for replacement in [false, true] {
        for stale_attempt in [false, true] {
            let key = format!("purge-owner-{replacement}-{stale_attempt}");
            let failed_start = storage
                .allocate_cache_keepalive_completion_ticket(&key, 100, 10_000_000)
                .await
                .unwrap();
            assert_eq!(failed_start.sequence, 1);
            assert!(
                !storage
                    .begin_cache_keepalive_request_fenced(&key, Some(failed_start), 100, 900)
                    .await
                    .unwrap()
            );
            let ticket_a = storage
                .allocate_cache_keepalive_completion_ticket(&key, 100, 10_000_000)
                .await
                .unwrap();
            assert_eq!(ticket_a.sequence, 2);
            assert!(
                !storage
                    .begin_cache_keepalive_request_fenced(&key, Some(ticket_a), 101, 901)
                    .await
                    .unwrap()
            );
            assert_eq!(other.purge_cache_keepalive_expired(102).await.unwrap(), 0);
            let ticket_c = other
                .allocate_cache_keepalive_completion_ticket(&key, 100, 10_000_000)
                .await
                .unwrap();
            let mut completion = replace_request(b"c-prefix", 103);
            completion.session_key_hash = key.clone();
            let original = other
                .replace_from_real_request_fenced(&completion, Some(ticket_c))
                .await
                .unwrap()
                .unwrap();
            let ticket_b = other
                .allocate_cache_keepalive_completion_ticket(&key, 100, 10_000_000)
                .await
                .unwrap();
            assert!(
                other
                    .begin_cache_keepalive_request_fenced(&key, Some(ticket_b), 104, 904)
                    .await
                    .unwrap()
            );
            let marker = other
                .get_cache_keepalive_session(&key)
                .await
                .unwrap()
                .unwrap();
            let order_key = cc_lb_storage_api::cache_keepalive_order_meta_key(&key);
            let order_before = other.get_meta_value(&order_key).await.unwrap();
            completion.cache_anchor_at_unix_secs = 110;
            completion.now_unix_secs = 111;
            completion.encrypted_payload = b"late-a".to_vec();
            if stale_attempt {
                let accepted = if replacement {
                    storage
                        .replace_from_real_request_fenced(&completion, Some(ticket_a))
                        .await
                        .unwrap()
                        .is_some()
                } else {
                    storage
                        .cancel_cache_keepalive_fenced(
                            &key,
                            Some(ticket_a),
                            110,
                            CacheKeepaliveTerminalReason::Cancelled,
                            Some("request_failed"),
                            111,
                        )
                        .await
                        .unwrap()
                };
                if accepted
                    || storage
                        .get_cache_keepalive_session(&key)
                        .await
                        .unwrap()
                        .as_ref()
                        != Some(&marker)
                    || storage.get_meta_value(&order_key).await.unwrap() != order_before
                {
                    failures.push(format!(
                        "replacement={replacement}: late A2 resolved B's marker; C={ticket_c:?}, B={ticket_b:?}"
                    ));
                    continue;
                }
            }
            // Healthy B completes using its original start ticket, not a new one.
            completion.encrypted_payload = b"healthy-b".to_vec();
            let accepted = if replacement {
                other
                    .replace_from_real_request_fenced(&completion, Some(ticket_b))
                    .await
                    .unwrap()
                    .is_some()
            } else {
                other
                    .cancel_cache_keepalive_fenced(
                        &key,
                        Some(ticket_b),
                        110,
                        CacheKeepaliveTerminalReason::Cancelled,
                        Some("user_turn_detected"),
                        111,
                    )
                    .await
                    .unwrap()
            };
            assert!(
                accepted,
                "B's original start ticket must remain authoritative"
            );
            let resolved = other
                .get_cache_keepalive_session(&key)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(resolved.running_since_unix_secs, None);
            assert_eq!(
                resolved.generation,
                original.generation + u64::from(replacement)
            );
            assert_eq!(
                resolved.status,
                if replacement {
                    CacheKeepaliveSessionStatus::Active
                } else {
                    CacheKeepaliveSessionStatus::Terminal
                }
            );
            assert_eq!(
                resolved.encrypted_payload,
                if replacement {
                    b"healthy-b".to_vec()
                } else {
                    Vec::new()
                }
            );
            if ticket_c.sequence != 3 || ticket_b.sequence != 4 {
                failures.push(format!(
                    "replacement={replacement}, stale_attempt={stale_attempt}: ordering namespace recycled; C={ticket_c:?}, B={ticket_b:?}"
                ));
            }
        }
    }
    other.pool().close().await;
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[tokio::test]
async fn in_request_completion_requires_original_start_ticket() {
    let (_dir, storage) = storage().await;
    let mut failures = Vec::new();
    for start_b in [105, 100] {
        for replacement in [false, true] {
            for fresh_completion_ticket in [true, false] {
                let key = format!("owner-{start_b}-{replacement}-{fresh_completion_ticket}");
                let mut seed = replace_request(b"prefix", 90);
                seed.session_key_hash = key.clone();
                let original = storage.replace_from_real_request(&seed).await.unwrap();
                let ticket_a = storage
                    .allocate_cache_keepalive_completion_ticket(&key, 100, 10_000_000)
                    .await
                    .unwrap();
                assert!(
                    storage
                        .begin_cache_keepalive_request_fenced(&key, Some(ticket_a), 100, 900)
                        .await
                        .unwrap()
                );
                let ticket_b = storage
                    .allocate_cache_keepalive_completion_ticket(&key, 100, 10_000_000)
                    .await
                    .unwrap();
                assert!(
                    storage
                        .begin_cache_keepalive_request_fenced(&key, Some(ticket_b), start_b, 905)
                        .await
                        .unwrap()
                );
                let marker = storage
                    .get_cache_keepalive_session(&key)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(marker.generation, original.generation);
                assert_eq!(marker.running_since_unix_secs, Some(start_b));
                let completion_ticket = if fresh_completion_ticket {
                    storage
                        .allocate_cache_keepalive_completion_ticket(&key, 100, 10_000_000)
                        .await
                        .unwrap()
                } else {
                    ticket_a
                };
                // A first receives bytes at 110, after B starts; timestamps cannot identify B's owner.
                let mut completion = seed.clone();
                completion.cache_anchor_at_unix_secs = 110;
                completion.now_unix_secs = 111;
                completion.encrypted_payload = b"stale-a".to_vec();
                let order_key = cc_lb_storage_api::cache_keepalive_order_meta_key(&key);
                let order_before = storage.get_meta_value(&order_key).await.unwrap();
                let accepted = if replacement {
                    storage
                        .replace_from_real_request_fenced(&completion, Some(completion_ticket))
                        .await
                        .unwrap()
                        .is_some()
                } else {
                    storage
                        .cancel_cache_keepalive_fenced(
                            &key,
                            Some(completion_ticket),
                            110,
                            CacheKeepaliveTerminalReason::Cancelled,
                            Some("request_failed"),
                            111,
                        )
                        .await
                        .unwrap()
                };
                let after = storage
                    .get_cache_keepalive_session(&key)
                    .await
                    .unwrap()
                    .unwrap();
                if accepted || after != marker {
                    failures.push(format!("{key}: stale A resolved B's marker"));
                    continue;
                }
                assert_eq!(
                    storage.get_meta_value(&order_key).await.unwrap(),
                    order_before
                );
                // Even allocation after B's start must leave B's original ticket authoritative.
                let healthy = if replacement {
                    completion.encrypted_payload = b"healthy-b".to_vec();
                    storage
                        .replace_from_real_request_fenced(&completion, Some(ticket_b))
                        .await
                        .unwrap()
                        .is_some()
                } else {
                    storage
                        .cancel_cache_keepalive_fenced(
                            &key,
                            Some(ticket_b),
                            110,
                            CacheKeepaliveTerminalReason::Cancelled,
                            Some("user_turn_detected"),
                            112,
                        )
                        .await
                        .unwrap()
                };
                if !healthy {
                    failures.push(format!("{key}: B's original start ticket was rejected"));
                    continue;
                }
                let resolved = storage
                    .get_cache_keepalive_session(&key)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(resolved.running_since_unix_secs, None);
                assert_eq!(
                    resolved.generation,
                    original.generation + u64::from(replacement)
                );
                assert_eq!(
                    resolved.status,
                    if replacement {
                        CacheKeepaliveSessionStatus::Active
                    } else {
                        CacheKeepaliveSessionStatus::Terminal
                    }
                );
                assert!(
                    !storage
                        .begin_cache_keepalive_request_fenced(&key, Some(ticket_b), 113, 910)
                        .await
                        .unwrap()
                );
            }
        }
    }
    for absent in [false, true] {
        let key = format!("compatibility-{absent}");
        let mut request = replace_request(b"compatible", 100);
        request.session_key_hash = key.clone();
        if !absent {
            storage.replace_from_real_request(&request).await.unwrap();
        }
        let ticket = storage
            .allocate_cache_keepalive_completion_ticket(&key, 100, 10_000_000)
            .await
            .unwrap();
        if absent {
            assert!(
                !storage
                    .begin_cache_keepalive_request_fenced(&key, Some(ticket), 100, 900)
                    .await
                    .unwrap()
            );
        }
        // Without a marker, the original ticket can insert or replace using the ordinary fence.
        let completed = storage
            .replace_from_real_request_fenced(&request, Some(ticket))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(completed.generation, if absent { 1 } else { 2 });
        assert!(
            storage
                .replace_from_real_request_fenced(&request, Some(ticket))
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            !storage
                .cancel_cache_keepalive_fenced(
                    &key,
                    Some(ticket),
                    110,
                    CacheKeepaliveTerminalReason::Cancelled,
                    Some("request_failed"),
                    111
                )
                .await
                .unwrap()
        );
        assert_eq!(
            storage
                .get_cache_keepalive_session(&key)
                .await
                .unwrap()
                .unwrap(),
            completed
        );
        let cancel_ticket = storage
            .allocate_cache_keepalive_completion_ticket(&key, 100, 10_000_000)
            .await
            .unwrap();
        assert!(
            storage
                .cancel_cache_keepalive_fenced(
                    &key,
                    Some(cancel_ticket),
                    110,
                    CacheKeepaliveTerminalReason::Cancelled,
                    Some("request_failed"),
                    111
                )
                .await
                .unwrap()
        );
        assert!(
            !storage
                .cancel_cache_keepalive_fenced(
                    &key,
                    Some(cancel_ticket),
                    110,
                    CacheKeepaliveTerminalReason::Cancelled,
                    Some("request_failed"),
                    112
                )
                .await
                .unwrap()
        );
    }
    let mut first = replace_request(b"first", 110);
    first.session_key_hash = "competing-first-session".to_owned();
    let older = storage
        .allocate_cache_keepalive_completion_ticket(&first.session_key_hash, 100, 10_000_000)
        .await
        .unwrap();
    assert!(
        !storage
            .begin_cache_keepalive_request_fenced(&first.session_key_hash, Some(older), 100, 900)
            .await
            .unwrap()
    );
    let newer = storage
        .allocate_cache_keepalive_completion_ticket(&first.session_key_hash, 100, 10_000_000)
        .await
        .unwrap();
    assert!(
        !storage
            .begin_cache_keepalive_request_fenced(&first.session_key_hash, Some(newer), 105, 905)
            .await
            .unwrap()
    );
    assert!(
        storage
            .replace_from_real_request_fenced(&first, Some(older))
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        storage
            .replace_from_real_request_fenced(&first, Some(newer))
            .await
            .unwrap()
            .unwrap()
            .generation,
        1
    );
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[tokio::test]
async fn in_request_marker_lifecycle_preserves_ticket_and_logical_anchor_fences() {
    let (_dir, storage) = storage().await;
    let original = storage
        .replace_from_real_request(&replace_request(b"prefix", 100))
        .await
        .expect("seed");
    storage
        .reschedule_after_cache_hit(&CacheKeepaliveHitRefreshRequest {
            session_key_hash: "session-hash".to_owned(),
            generation: original.generation,
            cache_anchor_at_unix_secs: 371,
            run_at_unix_secs: 641,
            expires_at_unix_secs: 671,
            encrypted_payload: None,
            now_unix_secs: 371,
        })
        .await
        .expect("synthetic hit")
        .expect("synthetic hit accepted");
    let older_ticket = storage
        .allocate_cache_keepalive_completion_ticket("session-hash", 100, 10_000_000)
        .await
        .expect("older ticket");
    let begin_ticket = storage
        .allocate_cache_keepalive_completion_ticket("session-hash", 100, 10_000_000)
        .await
        .expect("begin ticket");
    assert!(
        storage
            .begin_cache_keepalive_request_fenced("session-hash", Some(begin_ticket), 370, 900)
            .await
            .expect("begin uses logical rather than synthetic anchor")
    );
    let marker = storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load marker")
        .expect("marker");
    assert_eq!(marker.status, CacheKeepaliveSessionStatus::Terminal);
    assert_eq!(
        marker.terminal_reason,
        Some(CacheKeepaliveTerminalReason::Cancelled)
    );
    assert_eq!(marker.cancel_reason.as_deref(), Some("new_request"));
    assert_eq!(marker.running_since_unix_secs, Some(370));
    assert_eq!(marker.run_at_unix_secs, 900);
    assert!(marker.expires_at_unix_secs >= 900);
    assert_eq!(marker.first_scheduled_at_unix_secs, 370);
    assert_eq!(marker.cache_anchor_at_unix_secs, 371);
    assert_eq!(marker.enqueue_state, CacheKeepaliveEnqueueState::Pending);
    assert!(marker.encrypted_payload.is_empty());
    assert!(
        !storage
            .authorize_cache_keepalive_dispatch("session-hash", marker.generation)
            .await
            .expect("request marker cannot dispatch")
    );
    assert!(
        !storage
            .begin_cache_keepalive_request_fenced("session-hash", Some(older_ticket), 380, 910)
            .await
            .expect("older ticket cannot replace marker")
    );
    assert!(
        !storage
            .cancel_cache_keepalive_fenced(
                "session-hash",
                Some(older_ticket),
                380,
                CacheKeepaliveTerminalReason::Cancelled,
                Some("request_failed"),
                381,
            )
            .await
            .expect("older ticket cannot finalize marker")
    );
    let next_ticket = storage
        .allocate_cache_keepalive_completion_ticket("session-hash", 100, 10_000_000)
        .await
        .expect("next ticket");
    assert!(
        !storage
            .begin_cache_keepalive_request_fenced("session-hash", Some(next_ticket), 369, 950)
            .await
            .expect("older logical start rejected")
    );
    assert_eq!(
        storage
            .get_meta_value(&cc_lb_storage_api::cache_keepalive_order_meta_key(
                "session-hash"
            ))
            .await
            .expect("rejected begin rolls back ticket")
            .map(order_counters),
        Some((next_ticket.sequence, begin_ticket.sequence))
    );
    assert!(
        storage
            .begin_cache_keepalive_request_fenced("session-hash", Some(next_ticket), 380, 950)
            .await
            .expect("newer request extends existing marker")
    );
    let extended = storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load extended marker")
        .expect("extended marker");
    assert_eq!(extended.running_since_unix_secs, Some(380));
    assert_eq!(extended.run_at_unix_secs, 950);
    assert_eq!(extended.generation, marker.generation);
    assert!(
        !storage
            .cancel_cache_keepalive_fenced(
                "session-hash",
                Some(next_ticket),
                379,
                CacheKeepaliveTerminalReason::Cancelled,
                Some("request_failed"),
                400,
            )
            .await
            .expect("older request cannot finalize newer marker")
    );
    assert!(
        storage
            .cancel_cache_keepalive_fenced(
                "session-hash",
                Some(next_ticket),
                380,
                CacheKeepaliveTerminalReason::Cancelled,
                Some("request_failed"),
                400,
            )
            .await
            .expect("final cancellation replaces provisional reason")
    );
    let finalized = storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load final cancellation")
        .expect("final cancellation");
    assert_eq!(finalized.running_since_unix_secs, None);
    assert_eq!(finalized.cancel_reason.as_deref(), Some("request_failed"));
    assert_eq!(finalized.status, CacheKeepaliveSessionStatus::Terminal);
    assert!(
        !storage
            .begin_cache_keepalive_request_fenced("session-hash", None, 401, 960)
            .await
            .expect("final cancellation cannot become provisional")
    );
}

#[tokio::test]
async fn in_request_marker_replacement_and_legacy_terminal_states() {
    let (_dir, storage) = storage().await;
    storage
        .replace_from_real_request(&replace_request(b"prefix", 100))
        .await
        .expect("seed");
    assert!(
        storage
            .begin_cache_keepalive_request_fenced("session-hash", None, 200, 900)
            .await
            .expect("unticketed begin")
    );
    let fresh_marker = storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load marker without keepalive turns")
        .expect("fresh marker");
    assert_eq!(fresh_marker.refresh_count, 0);
    assert!(fresh_marker.expires_at_unix_secs >= 900);
    assert!(
        !storage
            .begin_cache_keepalive_request_fenced("session-hash", None, 199, 950)
            .await
            .expect("unticketed older start cannot overwrite marker")
    );
    assert!(
        !storage
            .cancel_cache_keepalive_fenced(
                "session-hash",
                None,
                199,
                CacheKeepaliveTerminalReason::Cancelled,
                Some("request_failed"),
                201,
            )
            .await
            .expect("unticketed older cancellation cannot clear marker")
    );
    let active = storage
        .replace_from_real_request_fenced(&replace_request(b"new-prefix", 201), None)
        .await
        .expect("completion replaces marker")
        .expect("accepted completion");
    assert_eq!(active.status, CacheKeepaliveSessionStatus::Active);
    assert_eq!(active.running_since_unix_secs, None);
    assert_eq!(active.terminal_reason, None);
    assert_eq!(active.cancel_reason, None);
    assert_eq!(active.encrypted_payload, b"new-prefix");
    assert!(
        storage
            .cancel_cache_keepalive_fenced(
                "session-hash",
                None,
                202,
                CacheKeepaliveTerminalReason::Cancelled,
                Some("new_request"),
                202,
            )
            .await
            .expect("legacy terminal new_request without request start")
    );
    assert!(
        !storage
            .begin_cache_keepalive_request_fenced("session-hash", None, 203, 950)
            .await
            .expect("legacy terminal row cannot become an in-request marker")
    );
    assert!(
        !storage
            .cancel_cache_keepalive_fenced(
                "session-hash",
                None,
                203,
                CacheKeepaliveTerminalReason::Cancelled,
                Some("request_failed"),
                203,
            )
            .await
            .expect("legacy terminal row remains final")
    );
}

#[tokio::test]
async fn ticketed_completions_use_stable_logical_anchor_and_reject_slow_older_responses() {
    let (_dir, storage) = storage().await;
    let original = storage
        .replace_from_real_request(&replace_request(b"original-prefix", 100))
        .await
        .expect("initial real completion");
    let replacement_ticket = storage
        .allocate_cache_keepalive_completion_ticket("session-hash", 100, 10_000_000)
        .await
        .expect("replacement ticket");
    let refreshed = storage
        .reschedule_after_cache_hit(&CacheKeepaliveHitRefreshRequest {
            session_key_hash: "session-hash".to_owned(),
            generation: original.generation,
            cache_anchor_at_unix_secs: 371,
            run_at_unix_secs: 641,
            expires_at_unix_secs: 671,
            encrypted_payload: None,
            now_unix_secs: 371,
        })
        .await
        .expect("synthetic refresh")
        .expect("refreshed generation");
    assert_eq!(refreshed.first_scheduled_at_unix_secs, 100);
    let active = storage
        .replace_from_real_request_fenced(
            &replace_request(b"new-real-prefix", 370),
            Some(replacement_ticket),
        )
        .await
        .expect("new real completion after synthetic refresh")
        .expect("real logical anchor is newer despite older sliding anchor");
    assert_eq!(active.first_scheduled_at_unix_secs, 370);
    assert_eq!(active.cache_anchor_at_unix_secs, 370);
    assert_eq!(active.generation, refreshed.generation + 1);

    let cancel_ticket = storage
        .allocate_cache_keepalive_completion_ticket("session-hash", 100, 10_000_000)
        .await
        .expect("later-observed cancellation ticket");
    assert!(
        !storage
            .cancel_cache_keepalive_fenced(
                "session-hash",
                Some(cancel_ticket),
                369,
                CacheKeepaliveTerminalReason::Cancelled,
                None,
                400,
            )
            .await
            .expect("slow older cancellation")
    );
    assert_eq!(
        storage
            .get_meta_value(&cc_lb_storage_api::cache_keepalive_order_meta_key(
                "session-hash"
            ))
            .await
            .expect("rejected cancellation must roll back applied order")
            .map(order_counters),
        Some((2, 1))
    );
    assert_eq!(
        storage
            .get_cache_keepalive_session("session-hash")
            .await
            .expect("load protected real generation")
            .expect("protected real generation exists"),
        active
    );
    assert!(
        storage
            .cancel_cache_keepalive_fenced(
                "session-hash",
                Some(cancel_ticket),
                372,
                CacheKeepaliveTerminalReason::Cancelled,
                None,
                400,
            )
            .await
            .expect("retry same ticket at a valid logical boundary")
    );
    let terminal = storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load logical cancellation fence")
        .expect("logical cancellation fence exists");
    assert_eq!(terminal.first_scheduled_at_unix_secs, 372);
    assert_eq!(terminal.cache_anchor_at_unix_secs, 370);

    let newer_ticket = storage
        .allocate_cache_keepalive_completion_ticket("session-hash", 100, 10_000_000)
        .await
        .expect("later-observed replacement ticket");
    assert!(
        storage
            .replace_from_real_request_fenced(
                &replace_request(b"slow-old-prefix", 371),
                Some(newer_ticket),
            )
            .await
            .expect("slow older replacement")
            .is_none()
    );
    assert_eq!(
        storage
            .get_meta_value(&cc_lb_storage_api::cache_keepalive_order_meta_key(
                "session-hash"
            ))
            .await
            .expect("rejected replacement must roll back applied order")
            .map(order_counters),
        Some((3, 2))
    );
    let replacement = storage
        .replace_from_real_request_fenced(
            &replace_request(b"new-post-compaction-prefix", 373),
            Some(newer_ticket),
        )
        .await
        .expect("retry same replacement ticket")
        .expect("newer logical anchor reactivates terminal state");
    assert_eq!(replacement.status, CacheKeepaliveSessionStatus::Active);
    assert_eq!(replacement.first_scheduled_at_unix_secs, 373);
    assert_eq!(replacement.generation, terminal.generation + 1);
    assert_eq!(replacement.encrypted_payload, b"new-post-compaction-prefix");
    assert_eq!(
        storage
            .get_meta_value(&cc_lb_storage_api::cache_keepalive_order_meta_key(
                "session-hash"
            ))
            .await
            .expect("successful retry commits applied order")
            .map(order_counters),
        Some((3, 3))
    );
}

#[tokio::test]
async fn ticketed_cancel_retires_generation_after_synthetic_refresh() {
    let (_dir, storage) = storage().await;
    let original = storage
        .replace_from_real_request(&replace_request(b"old-prefix", 100))
        .await
        .expect("initial real generation");
    let ticket = storage
        .allocate_cache_keepalive_completion_ticket("session-hash", 100, 10_000_000)
        .await
        .expect("compaction completion ticket");
    assert_eq!(ticket.sequence, 1);
    let refreshed = storage
        .reschedule_after_cache_hit(&CacheKeepaliveHitRefreshRequest {
            session_key_hash: "session-hash".to_owned(),
            generation: original.generation,
            cache_anchor_at_unix_secs: 371,
            run_at_unix_secs: 641,
            expires_at_unix_secs: 671,
            encrypted_payload: None,
            now_unix_secs: 371,
        })
        .await
        .expect("synthetic refresh")
        .expect("refreshed generation");

    assert!(
        storage
            .cancel_cache_keepalive_fenced(
                "session-hash",
                Some(ticket),
                370,
                CacheKeepaliveTerminalReason::Cancelled,
                None,
                400,
            )
            .await
            .expect("ticketed compaction cancellation")
    );
    let terminal = storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load terminal generation")
        .expect("terminal generation exists");
    assert_eq!(terminal.generation, refreshed.generation);
    assert_eq!(terminal.status, CacheKeepaliveSessionStatus::Terminal);
    assert_eq!(terminal.cache_anchor_at_unix_secs, 371);
    assert_eq!(terminal.first_scheduled_at_unix_secs, 370);
    assert_eq!(terminal.updated_at_unix_secs, 400);
    assert!(terminal.encrypted_payload.is_empty());
    assert_eq!(
        storage
            .get_meta_value(&cc_lb_storage_api::cache_keepalive_order_meta_key(
                "session-hash"
            ))
            .await
            .expect("load completion order")
            .map(order_counters),
        Some((1, 1))
    );
    assert!(
        !storage
            .cancel_cache_keepalive_fenced(
                "session-hash",
                Some(ticket),
                401,
                CacheKeepaliveTerminalReason::Cancelled,
                None,
                401,
            )
            .await
            .expect("already applied ticket is stale")
    );
}

#[tokio::test]
async fn newer_ticket_replaces_terminal_generation_before_terminal_write_timestamp() {
    let (_dir, storage) = storage().await;
    let original = storage
        .replace_from_real_request(&replace_request(b"old-prefix", 100))
        .await
        .expect("initial real generation");
    let cancelled_ticket = storage
        .allocate_cache_keepalive_completion_ticket("session-hash", 100, 10_000_000)
        .await
        .expect("compaction completion ticket");
    assert!(
        storage
            .cancel_cache_keepalive_fenced(
                "session-hash",
                Some(cancelled_ticket),
                100,
                CacheKeepaliveTerminalReason::Cancelled,
                None,
                103,
            )
            .await
            .expect("compaction cancellation")
    );
    let newer_ticket = storage
        .allocate_cache_keepalive_completion_ticket("session-hash", 100, 10_000_000)
        .await
        .expect("post-compaction completion ticket");
    assert_eq!(newer_ticket.sequence, cancelled_ticket.sequence + 1);
    let active = storage
        .replace_from_real_request_fenced(
            &replace_request(b"post-compaction-prefix", 102),
            Some(newer_ticket),
        )
        .await
        .expect("newer real completion")
        .expect("newer ticket supersedes terminal write timestamp");
    assert_eq!(active.generation, original.generation + 1);
    assert_eq!(active.status, CacheKeepaliveSessionStatus::Active);
    assert_eq!(active.cache_anchor_at_unix_secs, 102);
    assert_eq!(active.encrypted_payload, b"post-compaction-prefix");
    assert_eq!(
        storage
            .get_meta_value(&cc_lb_storage_api::cache_keepalive_order_meta_key(
                "session-hash"
            ))
            .await
            .expect("load completion order")
            .map(order_counters),
        Some((2, 2))
    );
    assert!(
        storage
            .replace_from_real_request_fenced(
                &replace_request(b"obsolete-completion-prefix", 200),
                Some(cancelled_ticket),
            )
            .await
            .expect("older applied completion ticket")
            .is_none()
    );
    assert_eq!(
        storage
            .get_cache_keepalive_session("session-hash")
            .await
            .expect("load post-compaction generation")
            .expect("post-compaction generation exists"),
        active
    );
}

#[tokio::test]
async fn completion_tickets_preserve_active_anchor_and_legacy_timestamp_guards() {
    let (_dir, storage) = storage().await;
    let original = storage
        .replace_from_real_request(&replace_request(b"newer-active-prefix", 371))
        .await
        .expect("initial active generation");
    let ticket = storage
        .allocate_cache_keepalive_completion_ticket("session-hash", 100, 10_000_000)
        .await
        .expect("real completion ticket");
    for expected_ticket in [None, Some(ticket)] {
        assert!(
            storage
                .replace_from_real_request_fenced(
                    &replace_request(b"older-prefix", 370),
                    expected_ticket,
                )
                .await
                .expect("stale active anchor")
                .is_none()
        );
    }
    assert!(
        !storage
            .cancel_cache_keepalive_fenced(
                "session-hash",
                None,
                370,
                CacheKeepaliveTerminalReason::Cancelled,
                None,
                400,
            )
            .await
            .expect("legacy stale cancellation")
    );
    assert_eq!(
        storage
            .get_cache_keepalive_session("session-hash")
            .await
            .expect("load active generation")
            .expect("active generation exists"),
        original
    );
    assert_eq!(
        storage
            .get_meta_value(&cc_lb_storage_api::cache_keepalive_order_meta_key(
                "session-hash"
            ))
            .await
            .expect("rejected replacement preserves completion order")
            .map(order_counters),
        Some((1, 0))
    );
    assert!(
        storage
            .cancel_cache_keepalive_fenced(
                "session-hash",
                None,
                371,
                CacheKeepaliveTerminalReason::Cancelled,
                None,
                400,
            )
            .await
            .expect("legacy current-anchor cancellation")
    );
    assert!(
        storage
            .replace_from_real_request_fenced(&replace_request(b"stale-terminal-prefix", 399), None)
            .await
            .expect("legacy terminal timestamp guard")
            .is_none()
    );
    assert!(
        storage
            .replace_from_real_request_fenced(
                &replace_request(b"older-logical-prefix", 370),
                Some(ticket),
            )
            .await
            .expect("ticket cannot bypass logical terminal fence")
            .is_none()
    );
    let replacement = storage
        .replace_from_real_request_fenced(
            &replace_request(b"logically-newer-prefix", 371),
            Some(ticket),
        )
        .await
        .expect("ticketed terminal replacement")
        .expect("same ticket can replace at the logical boundary");
    assert_eq!(replacement.generation, original.generation + 1);
    assert_eq!(replacement.status, CacheKeepaliveSessionStatus::Active);
    assert_eq!(replacement.encrypted_payload, b"logically-newer-prefix");
}

#[tokio::test]
async fn replace_from_real_request_bumps_generation_and_resets_counters() {
    let (_dir, storage) = storage().await;

    let first = storage
        .replace_from_real_request(&replace_request(b"ciphertext-one", 100))
        .await
        .expect("insert first session");
    let second = storage
        .replace_from_real_request(&replace_request(b"ciphertext-two", 200))
        .await
        .expect("replace session");

    assert_eq!(first.generation, 1);
    assert_eq!(second.generation, 2);
    assert_eq!(second.refresh_count, 0);
    assert_eq!(second.first_scheduled_at_unix_secs, 200);
    assert_eq!(second.cache_anchor_at_unix_secs, 200);
    assert_eq!(second.run_at_unix_secs, 470);
    assert_eq!(
        second.current_job_key,
        cache_keepalive_job_key("session-hash", 2)
    );
    assert_eq!(second.encrypted_payload, b"ciphertext-two");
}

#[tokio::test]
async fn replace_roundtrips_optional_accounting_key_id() {
    let (_dir, storage) = storage().await;
    let with_key = CacheKeepaliveReplaceRequest {
        accounting_key_id: Some("key-live-123".to_owned()),
        ..replace_request(b"ciphertext-with-key", 100)
    };
    let mut without_key = replace_request(b"ciphertext-without-key", 110);
    without_key.session_key_hash = "session-without-key".to_owned();

    let with_key = storage
        .replace_from_real_request(&with_key)
        .await
        .expect("insert keyed session");
    let without_key = storage
        .replace_from_real_request(&without_key)
        .await
        .expect("insert unkeyed session");

    assert_eq!(with_key.accounting_key_id.as_deref(), Some("key-live-123"));
    assert_eq!(without_key.accounting_key_id, None);
}

#[tokio::test]
async fn concurrent_claim_allows_exactly_one_enqueued_generation_winner() {
    let (_dir, storage) = storage().await;
    let storage = Arc::new(storage);
    let record = storage
        .replace_from_real_request(&replace_request(b"ciphertext", 100))
        .await
        .expect("insert session");
    assert!(
        storage
            .mark_cache_keepalive_enqueued("session-hash", record.generation, 101)
            .await
            .expect("mark enqueued")
    );
    let barrier = Arc::new(tokio::sync::Barrier::new(2));

    let first = tokio::spawn({
        let storage = Arc::clone(&storage);
        let barrier = Arc::clone(&barrier);
        async move {
            barrier.wait().await;
            storage
                .claim_cache_keepalive_turn("session-hash", record.generation, 200)
                .await
        }
    });
    let second = tokio::spawn({
        let storage = Arc::clone(&storage);
        let barrier = Arc::clone(&barrier);
        async move {
            barrier.wait().await;
            storage
                .claim_cache_keepalive_turn("session-hash", record.generation, 200)
                .await
        }
    });

    let (first, second) = tokio::join!(first, second);
    let first = first
        .expect("first task joins")
        .expect("first claim succeeds");
    let second = second
        .expect("second task joins")
        .expect("second claim succeeds");

    assert_eq!(u8::from(first) + u8::from(second), 1);
    let claimed = storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load claimed session")
        .expect("session exists");
    assert_eq!(claimed.enqueue_state, CacheKeepaliveEnqueueState::Running);
    assert_eq!(claimed.running_since_unix_secs, Some(200));
}

#[tokio::test]
async fn pending_claim_requires_payload_and_mark_cannot_overwrite_running() {
    let (_dir, storage) = storage().await;
    let record = storage
        .replace_from_real_request(&replace_request(b"", 100))
        .await
        .expect("insert pending placeholder");

    assert!(
        !storage
            .claim_cache_keepalive_turn("session-hash", record.generation, 101)
            .await
            .expect("reject empty pending payload")
    );
    assert!(
        storage
            .update_cache_keepalive_payload("session-hash", record.generation, b"ciphertext", 102)
            .await
            .expect("fill pending payload")
    );
    assert!(
        storage
            .claim_cache_keepalive_turn("session-hash", record.generation, 103)
            .await
            .expect("claim published pending payload")
    );
    let claimed = storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load running session")
        .expect("running session exists");
    assert_eq!(claimed.enqueue_state, CacheKeepaliveEnqueueState::Running);
    assert_eq!(claimed.running_since_unix_secs, Some(103));
    assert!(
        !storage
            .mark_cache_keepalive_enqueued("session-hash", record.generation, 104)
            .await
            .expect("running mark is a no-op")
    );
    let unchanged = storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("reload running session")
        .expect("running session remains");
    assert_eq!(unchanged.enqueue_state, CacheKeepaliveEnqueueState::Running);
    assert_eq!(unchanged.running_since_unix_secs, Some(103));
}

#[tokio::test]
async fn update_payload_only_mutates_matching_pending_generation() {
    let (_dir, storage) = storage().await;
    let record = storage
        .replace_from_real_request(&replace_request(b"pending-placeholder", 100))
        .await
        .expect("insert session");

    assert!(
        !storage
            .update_cache_keepalive_payload("session-hash", record.generation + 1, b"stale", 101)
            .await
            .expect("stale payload no-op")
    );
    assert!(
        storage
            .update_cache_keepalive_payload("session-hash", record.generation, b"ciphertext", 102)
            .await
            .expect("fresh payload update")
    );

    let updated = storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load session")
        .expect("row exists");
    assert_eq!(updated.encrypted_payload, b"ciphertext");
    assert_eq!(updated.enqueue_state, CacheKeepaliveEnqueueState::Pending);

    assert!(
        storage
            .mark_cache_keepalive_enqueued("session-hash", record.generation, 103)
            .await
            .expect("mark enqueued")
    );
    assert!(
        !storage
            .update_cache_keepalive_payload("session-hash", record.generation, b"late", 104)
            .await
            .expect("enqueued payload no-op")
    );
}

#[tokio::test]
async fn cache_hit_reschedule_preserves_duration_anchor_and_rejects_stale_generation() {
    let (_dir, storage) = storage().await;
    let original = storage
        .replace_from_real_request(&replace_request(b"ciphertext-one", 100))
        .await
        .expect("insert first session");

    let stale = storage
        .reschedule_after_cache_hit(&CacheKeepaliveHitRefreshRequest {
            session_key_hash: "session-hash".to_owned(),
            generation: original.generation + 1,
            cache_anchor_at_unix_secs: 150,
            run_at_unix_secs: 420,
            expires_at_unix_secs: 450,
            encrypted_payload: None,
            now_unix_secs: 151,
        })
        .await
        .expect("stale update is a no-op");
    assert!(stale.is_none());

    let updated = storage
        .reschedule_after_cache_hit(&CacheKeepaliveHitRefreshRequest {
            session_key_hash: "session-hash".to_owned(),
            generation: original.generation,
            cache_anchor_at_unix_secs: 150,
            run_at_unix_secs: 420,
            expires_at_unix_secs: 450,
            encrypted_payload: None,
            now_unix_secs: 151,
        })
        .await
        .expect("fresh update")
        .expect("row updated");

    assert_eq!(updated.generation, 2);
    assert_eq!(updated.refresh_count, 1);
    assert_eq!(updated.first_scheduled_at_unix_secs, 100);
    assert_eq!(updated.cache_anchor_at_unix_secs, 150);
    assert_eq!(updated.run_at_unix_secs, 420);
    assert_eq!(updated.encrypted_payload, b"ciphertext-one");
    let migrated = storage
        .reschedule_after_cache_hit(&CacheKeepaliveHitRefreshRequest {
            session_key_hash: "session-hash".to_owned(),
            generation: updated.generation,
            cache_anchor_at_unix_secs: 151,
            run_at_unix_secs: 421,
            expires_at_unix_secs: 451,
            encrypted_payload: Some(b"compressed-ciphertext".to_vec()),
            now_unix_secs: 152,
        })
        .await
        .expect("fresh payload migration")
        .expect("row migrated");
    assert_eq!(migrated.generation, 3);
    assert_eq!(migrated.refresh_count, 2);
    assert_eq!(migrated.encrypted_payload, b"compressed-ciphertext");
}

#[tokio::test]
async fn conditional_enqueue_terminal_and_purge_are_generation_safe() {
    let (_dir, storage) = storage().await;
    let record = storage
        .replace_from_real_request(&replace_request(b"ciphertext", 100))
        .await
        .expect("insert session");

    assert!(
        storage
            .mark_cache_keepalive_enqueued("session-hash", record.generation, 101)
            .await
            .expect("mark enqueued")
    );
    assert!(
        !storage
            .mark_cache_keepalive_terminal(
                "session-hash",
                record.generation + 1,
                CacheKeepaliveTerminalReason::CacheMiss,
                None,
                102,
            )
            .await
            .expect("stale terminal no-op")
    );
    assert!(
        storage
            .mark_cache_keepalive_terminal(
                "session-hash",
                record.generation,
                CacheKeepaliveTerminalReason::CacheMiss,
                None,
                102,
            )
            .await
            .expect("fresh terminal")
    );
    let terminal = storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load terminal session")
        .expect("terminal session exists");
    assert!(terminal.encrypted_payload.is_empty());
    let check = storage
        .check_cache_keepalive_generation("session-hash")
        .await
        .expect("check")
        .expect("row exists");
    assert_eq!(check.status, CacheKeepaliveSessionStatus::Terminal);
    assert_eq!(check.enqueue_state, CacheKeepaliveEnqueueState::Pending);

    let removed = storage
        .purge_cache_keepalive_expired(401)
        .await
        .expect("purge expired");
    assert_eq!(removed, 0);
    assert!(
        storage
            .get_cache_keepalive_session("session-hash")
            .await
            .expect("get after retained purge")
            .is_some()
    );
    let removed = storage
        .purge_cache_keepalive_expired(102 + 7 * 24 * 60 * 60 + 1)
        .await
        .expect("purge retained session");
    assert_eq!(removed, 1);
    assert!(
        storage
            .get_cache_keepalive_session("session-hash")
            .await
            .expect("get after purge")
            .is_none()
    );
}

#[tokio::test]
async fn terminal_error_is_truncated_and_resets_running_enqueue_state() {
    let (_dir, storage) = storage().await;
    let record = storage
        .replace_from_real_request(&replace_request(b"ciphertext", 100))
        .await
        .expect("insert session");
    assert!(
        storage
            .mark_cache_keepalive_enqueued("session-hash", record.generation, 101)
            .await
            .expect("mark enqueued")
    );
    assert!(
        storage
            .claim_cache_keepalive_turn("session-hash", record.generation, 102)
            .await
            .expect("claim turn")
    );

    let error = "é".repeat(300);
    assert!(
        storage
            .mark_cache_keepalive_terminal(
                "session-hash",
                record.generation,
                CacheKeepaliveTerminalReason::DispatchError,
                Some(&error),
                103,
            )
            .await
            .expect("mark terminal")
    );

    let terminal = storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load terminal session")
        .expect("terminal session exists");
    let persisted_error = terminal.error.as_deref().expect("terminal error exists");
    assert_eq!(persisted_error.len(), 512);
    assert_eq!(persisted_error.chars().count(), 256);
    assert!(persisted_error.is_char_boundary(persisted_error.len()));
    assert_eq!(terminal.enqueue_state, CacheKeepaliveEnqueueState::Pending);
}

#[tokio::test]
async fn fenced_terminal_paths_reset_enqueue_state_and_reschedule_clears_error() {
    let (_dir, storage) = storage().await;
    let record = storage
        .replace_from_real_request(&replace_request(b"ciphertext", 100))
        .await
        .expect("insert session");
    assert!(
        storage
            .mark_cache_keepalive_enqueued("session-hash", record.generation, 101)
            .await
            .expect("mark enqueued")
    );
    assert!(
        storage
            .claim_cache_keepalive_turn("session-hash", record.generation, 102)
            .await
            .expect("claim turn")
    );
    assert!(
        storage
            .cancel_cache_keepalive_fenced(
                "session-hash",
                None,
                100,
                CacheKeepaliveTerminalReason::Cancelled,
                None,
                103,
            )
            .await
            .expect("cancel session")
    );
    let cancelled = storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load cancelled session")
        .expect("cancelled session exists");
    assert_eq!(cancelled.enqueue_state, CacheKeepaliveEnqueueState::Pending);
    assert_eq!(cancelled.error, None);

    let active = storage
        .replace_from_real_request(&replace_request(b"ciphertext-two", 110))
        .await
        .expect("replace session");
    assert!(
        storage
            .mark_cache_keepalive_enqueued("session-hash", active.generation, 111)
            .await
            .expect("mark replacement enqueued")
    );
    assert!(
        storage
            .claim_cache_keepalive_turn("session-hash", active.generation, 112)
            .await
            .expect("claim replacement turn")
    );
    assert!(
        storage
            .mark_latest_cache_keepalive_terminal(
                "session-hash",
                CacheKeepaliveTerminalReason::Cancelled,
                113,
            )
            .await
            .expect("mark latest terminal")
    );
    let latest = storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load latest session")
        .expect("latest session exists");
    assert_eq!(latest.enqueue_state, CacheKeepaliveEnqueueState::Pending);
    assert_eq!(latest.error, None);

    let active = storage
        .replace_from_real_request(&replace_request(b"ciphertext-three", 120))
        .await
        .expect("replace for reschedule");
    sqlx::query(
        "UPDATE cache_keepalive_sessions SET error = ? WHERE session_key_hash = ? AND generation = ?",
    )
    .bind("stale error")
    .bind(&active.session_key_hash)
    .bind(i64::try_from(active.generation).expect("generation fits i64"))
    .execute(storage.pool())
    .await
    .expect("seed active error");
    let refreshed = storage
        .reschedule_after_cache_hit(&CacheKeepaliveHitRefreshRequest {
            session_key_hash: active.session_key_hash,
            generation: active.generation,
            cache_anchor_at_unix_secs: 121,
            run_at_unix_secs: 391,
            expires_at_unix_secs: 421,
            encrypted_payload: None,
            now_unix_secs: 121,
        })
        .await
        .expect("reschedule after cache hit")
        .expect("rescheduled session exists");
    assert_eq!(refreshed.error, None);
}

#[tokio::test]
async fn mark_latest_terminal_only_mutates_active_latest_session() {
    let (_dir, storage) = storage().await;
    let first = storage
        .replace_from_real_request(&replace_request(b"ciphertext-one", 100))
        .await
        .expect("insert first session");
    let second = storage
        .replace_from_real_request(&replace_request(b"ciphertext-two", 110))
        .await
        .expect("replace session");

    assert_eq!(first.generation, 1);
    assert_eq!(second.generation, 2);
    assert!(
        storage
            .mark_latest_cache_keepalive_terminal(
                "session-hash",
                CacheKeepaliveTerminalReason::Cancelled,
                120,
            )
            .await
            .expect("mark latest terminal")
    );
    assert!(
        !storage
            .mark_latest_cache_keepalive_terminal(
                "session-hash",
                CacheKeepaliveTerminalReason::CacheMiss,
                121,
            )
            .await
            .expect("terminal row no-op")
    );

    let record = storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load session")
        .expect("row exists");
    assert_eq!(record.generation, 2);
    assert_eq!(record.status, CacheKeepaliveSessionStatus::Terminal);
    assert_eq!(
        record.terminal_reason,
        Some(CacheKeepaliveTerminalReason::Cancelled)
    );
    assert!(record.encrypted_payload.is_empty());
}

#[tokio::test]
async fn recover_stale_running_requires_active_running_strictly_old_lease_and_scrubs_payload() {
    let (_dir, storage) = storage().await;
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
        let mut request = replace_request(key.as_bytes(), 100);
        request.session_key_hash = key.to_owned();
        request.accounting_key_id = Some(format!("accounting-{key}"));
        storage
            .replace_from_real_request(&request)
            .await
            .expect("insert session");
        request.now_unix_secs = 101;
        storage
            .replace_from_real_request(&request)
            .await
            .expect("replace session to exercise a noninitial generation");

        // Legacy state combinations isolate each predicate, including a missing lease
        // and old leases on rows that are not active/running.
        sqlx::query(
            "UPDATE cache_keepalive_sessions
             SET status = ?, enqueue_state = ?, running_since_unix_secs = ?, error = ?
             WHERE session_key_hash = ?",
        )
        .bind(status)
        .bind(enqueue_state)
        .bind(lease)
        .bind("previous error")
        .bind(key)
        .execute(storage.pool())
        .await
        .expect("seed recovery state");
        before.push(
            storage
                .get_cache_keepalive_session(key)
                .await
                .expect("load seeded session")
                .expect("seeded session exists"),
        );
    }

    let affected = storage
        .recover_cache_keepalive_stale_running(cutoff, now)
        .await
        .expect("recover stale running sessions");
    assert_eq!(affected, 1);

    let mut expected = before.remove(0);
    let recovered = storage
        .get_cache_keepalive_session(&expected.session_key_hash)
        .await
        .expect("load recovered session")
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
            .await
            .expect("load noncandidate")
            .expect("noncandidate remains persisted");
        assert_eq!(
            persisted, control,
            "noncandidates must retain their payload, identity, and all other fields"
        );
    }
    assert_eq!(
        storage
            .recover_cache_keepalive_stale_running(cutoff, now + 1)
            .await
            .expect("recovery is idempotent"),
        0
    );
}

#[tokio::test]
async fn recover_stale_running_rejects_overflow_without_mutation() {
    let (_dir, storage) = storage().await;
    let record = storage
        .replace_from_real_request(&replace_request(b"overflow", 100))
        .await
        .expect("insert running session");
    assert!(
        storage
            .mark_cache_keepalive_enqueued("session-hash", record.generation, 110)
            .await
            .expect("mark enqueued")
    );
    assert!(
        storage
            .claim_cache_keepalive_turn("session-hash", record.generation, 150)
            .await
            .expect("claim running turn")
    );
    let before = storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load session before overflow checks")
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
                .get_cache_keepalive_session("session-hash")
                .await
                .expect("load session after overflow rejection")
                .expect("running session remains"),
            before,
            "overflow rejection must not mutate persisted state"
        );
    }
}

#[tokio::test]
async fn purge_expired_uses_a_strict_cutoff_boundary() {
    let (_dir, storage) = storage().await;
    for (key, now) in [
        ("expired-before-cutoff", 99),
        ("exact-cutoff-expiry", 100),
        ("future-expiry", 101),
    ] {
        let mut request = replace_request(key.as_bytes(), now);
        request.session_key_hash = key.to_owned();
        storage
            .replace_from_real_request(&request)
            .await
            .expect("insert expiry boundary session");
    }

    assert_eq!(
        storage
            .purge_cache_keepalive_expired(400)
            .await
            .expect("purge before cutoff"),
        1
    );
    assert!(
        storage
            .get_cache_keepalive_session("expired-before-cutoff")
            .await
            .expect("load expired row")
            .is_none()
    );
    assert!(
        storage
            .get_cache_keepalive_session("exact-cutoff-expiry")
            .await
            .expect("load exact boundary row")
            .is_some(),
        "expiry equal to cutoff must not be purged"
    );
    assert!(
        storage
            .get_cache_keepalive_session("future-expiry")
            .await
            .expect("load future row")
            .is_some()
    );
    assert_eq!(
        storage
            .purge_cache_keepalive_expired(401)
            .await
            .expect("purge exact boundary after advancing cutoff"),
        1
    );
    assert!(
        storage
            .get_cache_keepalive_session("exact-cutoff-expiry")
            .await
            .expect("load exact row after advancing cutoff")
            .is_none()
    );
    assert!(
        storage
            .get_cache_keepalive_session("future-expiry")
            .await
            .expect("load future row after purge")
            .is_some()
    );
}

#[tokio::test]
async fn purge_stale_pending_uses_strict_cutoff_and_state_predicates() {
    let (_dir, storage) = storage().await;
    for key in [
        "stale-pending-boundary",
        "exact-pending-boundary",
        "running-old",
        "terminal-old",
        "enqueued-old",
    ] {
        let mut request = replace_request(key.as_bytes(), 100);
        request.session_key_hash = key.to_owned();
        storage
            .replace_from_real_request(&request)
            .await
            .expect("insert pending predicate session");
    }
    for (key, status, enqueue_state, lease) in [
        ("stale-pending-boundary", "active", "pending", None),
        ("exact-pending-boundary", "active", "pending", None),
        ("running-old", "active", "running", Some(199_i64)),
        ("terminal-old", "terminal", "pending", None),
        ("enqueued-old", "active", "enqueued", None),
    ] {
        sqlx::query(
            "UPDATE cache_keepalive_sessions
             SET status = ?, enqueue_state = ?, running_since_unix_secs = ?, updated_at = ?
             WHERE session_key_hash = ?",
        )
        .bind(status)
        .bind(enqueue_state)
        .bind(lease)
        .bind(if key == "exact-pending-boundary" {
            200_i64
        } else {
            199_i64
        })
        .bind(key)
        .execute(storage.pool())
        .await
        .expect("seed pending predicate state");
    }

    assert_eq!(
        storage
            .purge_cache_keepalive_stale_pending(200)
            .await
            .expect("purge stale pending"),
        1
    );
    assert!(
        storage
            .get_cache_keepalive_session("stale-pending-boundary")
            .await
            .expect("load stale pending")
            .is_none()
    );
    for (key, expected_status, expected_enqueue_state) in [
        (
            "exact-pending-boundary",
            CacheKeepaliveSessionStatus::Active,
            CacheKeepaliveEnqueueState::Pending,
        ),
        (
            "running-old",
            CacheKeepaliveSessionStatus::Active,
            CacheKeepaliveEnqueueState::Running,
        ),
        (
            "terminal-old",
            CacheKeepaliveSessionStatus::Terminal,
            CacheKeepaliveEnqueueState::Pending,
        ),
        (
            "enqueued-old",
            CacheKeepaliveSessionStatus::Active,
            CacheKeepaliveEnqueueState::Enqueued,
        ),
    ] {
        let persisted = storage
            .get_cache_keepalive_session(key)
            .await
            .expect("load excluded pending predicate state")
            .expect("excluded state remains persisted");
        assert_eq!(persisted.status, expected_status);
        assert_eq!(persisted.enqueue_state, expected_enqueue_state);
    }
}

#[tokio::test]
async fn concurrent_recovery_counts_once_and_replacement_reschedule_preserve_latest_active_payload()
{
    let (_dir, storage) = storage().await;
    let storage = Arc::new(storage);
    let running = storage
        .replace_from_real_request(&replace_request(b"concurrent-running", 100))
        .await
        .expect("insert concurrent recovery session");
    assert!(
        storage
            .mark_cache_keepalive_enqueued("session-hash", running.generation, 110)
            .await
            .expect("mark concurrent session enqueued")
    );
    assert!(
        storage
            .claim_cache_keepalive_turn("session-hash", running.generation, 150)
            .await
            .expect("claim concurrent session")
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
    let (first, second) = tokio::try_join!(first, second).expect("concurrent recovery tasks");
    let first = first.expect("first recovery");
    let second = second.expect("second recovery");
    assert_eq!(first + second, 1);
    let recovered = storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load recovered concurrent session")
        .expect("recovered concurrent session exists");
    assert_eq!(recovered.status, CacheKeepaliveSessionStatus::Terminal);
    assert!(recovered.encrypted_payload.is_empty());

    let latest_key = "latest-before-recovery";
    let mut previous_request = replace_request(b"previous-generation", 100);
    previous_request.session_key_hash = latest_key.to_owned();
    let previous = storage
        .replace_from_real_request(&previous_request)
        .await
        .expect("insert previous active generation");
    assert!(
        storage
            .mark_cache_keepalive_enqueued(latest_key, previous.generation, 110)
            .await
            .expect("mark previous generation enqueued")
    );
    assert!(
        storage
            .claim_cache_keepalive_turn(latest_key, previous.generation, 150)
            .await
            .expect("claim previous generation")
    );
    let mut latest_request = replace_request(b"latest-before-recovery", 600);
    latest_request.session_key_hash = latest_key.to_owned();
    let latest = storage
        .replace_from_real_request(&latest_request)
        .await
        .expect("replace with latest active generation");
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
        .await
        .expect("reschedule latest active generation")
        .expect("latest active generation remains");
    assert_eq!(refreshed.generation, latest.generation + 1);
    assert_eq!(refreshed.encrypted_payload, b"latest-rescheduled-payload");
    assert_eq!(refreshed.status, CacheKeepaliveSessionStatus::Active);
    assert_eq!(refreshed.enqueue_state, CacheKeepaliveEnqueueState::Pending);
    assert_eq!(
        storage
            .recover_cache_keepalive_stale_running(700, 800)
            .await
            .expect("recover after latest replacement"),
        0
    );
    assert_eq!(
        storage
            .get_cache_keepalive_session(latest_key)
            .await
            .expect("load latest after recovery")
            .expect("latest session exists"),
        refreshed
    );
}

#[tokio::test]
async fn purge_stale_pending_keeps_enqueued_work() {
    let (_dir, storage) = storage().await;
    let pending = storage
        .replace_from_real_request(&replace_request(b"pending", 100))
        .await
        .expect("insert pending session");
    let mut enqueued_request = replace_request(b"enqueued", 110);
    enqueued_request.session_key_hash = "old-enqueued".to_owned();
    let enqueued = storage
        .replace_from_real_request(&enqueued_request)
        .await
        .expect("insert enqueued session");
    assert!(
        storage
            .mark_cache_keepalive_enqueued("old-enqueued", enqueued.generation, 111)
            .await
            .expect("mark enqueued")
    );

    let removed = storage
        .purge_cache_keepalive_stale_pending(112)
        .await
        .expect("purge stale pending");

    assert_eq!(pending.enqueue_state, CacheKeepaliveEnqueueState::Pending);
    assert_eq!(removed, 1);
    assert!(
        storage
            .get_cache_keepalive_session("session-hash")
            .await
            .expect("load pending after purge")
            .is_none()
    );
    assert!(
        storage
            .get_cache_keepalive_session("old-enqueued")
            .await
            .expect("load enqueued after purge")
            .is_some()
    );
}

#[tokio::test]
async fn cancellation_causes_roundtrip_and_replacement_clears_them() {
    let (_dir, storage) = storage().await;
    for (index, reason) in [
        None,
        Some("new_request"),
        Some("user_turn_detected"),
        Some("snapshot_too_large"),
        Some("cached_prefix_too_small"),
        Some("zero_token_renewal_unsupported"),
    ]
    .into_iter()
    .enumerate()
    {
        let now = 100 + index as u64 * 10;
        let active = storage
            .replace_from_real_request(&replace_request(b"prefix", now))
            .await
            .expect("replace");
        assert_eq!(active.cancel_reason, None);
        assert!(
            storage
                .cancel_cache_keepalive_fenced(
                    "session-hash",
                    None,
                    now,
                    CacheKeepaliveTerminalReason::Cancelled,
                    reason,
                    now + 1
                )
                .await
                .expect("cancel")
        );
        let terminal = storage
            .get_cache_keepalive_session("session-hash")
            .await
            .expect("get")
            .expect("session");
        assert_eq!(terminal.cancel_reason.as_deref(), reason);
        assert_eq!(
            terminal.terminal_reason,
            Some(CacheKeepaliveTerminalReason::Cancelled)
        );
        assert!(
            !storage
                .cancel_cache_keepalive_fenced(
                    "session-hash",
                    None,
                    now,
                    CacheKeepaliveTerminalReason::Cancelled,
                    Some("new_request"),
                    now + 2
                )
                .await
                .expect("duplicate cancellation")
        );
        assert_eq!(
            storage
                .get_cache_keepalive_session("session-hash")
                .await
                .expect("get")
                .expect("session")
                .cancel_reason
                .as_deref(),
            reason,
            "a stale cancellation cannot change the cause"
        );
    }
}

async fn replace_deployed_legacy_request(
    storage: &SqliteStorage,
    seed: &CacheKeepaliveReplaceRequest,
    anchor: i64,
    now: i64,
) -> Option<cc_lb_storage_api::CacheKeepaliveSessionRecord> {
    let key = seed.session_key_hash.as_str();
    let legacy_key = format!("keepalive_order:{key}");
    let old_ticket = allocate_deployed_legacy_ticket(storage, key).await;
    let mut tx = storage.begin_immediate().await.unwrap();
    // Exact fdc0f9d existing-row SELECT and ticketed logical-anchor fence.
    let existing = sqlx::query_as::<_, (i64, i64, String, i64, i64, i64)>(
        "SELECT generation, created_at, status, cache_anchor_at, updated_at, first_scheduled_at FROM cache_keepalive_sessions WHERE session_key_hash = ?",
    ).bind(key).fetch_one(&mut *tx).await.unwrap();
    if existing.5 > anchor {
        tx.rollback().await.unwrap();
        return None;
    }
    let raw = sqlx::query_scalar::<_, String>("SELECT value FROM meta_v1 WHERE key = ?")
        .bind(&legacy_key)
        .fetch_optional(&mut *tx)
        .await
        .unwrap();
    let (issued, applied) = deployed_legacy_counters(raw.as_deref());
    if old_ticket <= applied {
        tx.rollback().await.unwrap();
        return None;
    }
    sqlx::query(
        "INSERT INTO meta_v1 (key, value) VALUES (?, ?)
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value",
    )
    .bind(&legacy_key)
    .bind(format!("{}:{}", issued.max(old_ticket), old_ticket))
    .execute(&mut *tx)
    .await
    .unwrap();
    // Verbatim deployed master replacement SQL: no terminal-marker owner fence.
    sqlx::query("INSERT INTO cache_keepalive_sessions
             (session_key_hash, principal_id, accounting_key_id, upstream_id, cycle_id, generation, refresh_count,
              first_scheduled_at, cache_anchor_at, run_at, ttl, status, enqueue_state, running_since_unix_secs,
              current_job_key, encrypted_payload, display_reason, error, config_snapshot, last_message_at_ms, terminal_reason, expires_at, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, 'active', 'pending', NULL, ?, ?, ?, NULL, ?, ?, NULL, ?, ?, ?)
             ON CONFLICT(session_key_hash) DO UPDATE SET
              principal_id = excluded.principal_id,
              accounting_key_id = excluded.accounting_key_id,
              upstream_id = excluded.upstream_id,
              cycle_id = excluded.cycle_id,
              generation = excluded.generation,
              refresh_count = 0,
              first_scheduled_at = excluded.first_scheduled_at,
              cache_anchor_at = excluded.cache_anchor_at,
              run_at = excluded.run_at,
              ttl = excluded.ttl,
              status = 'active',
              enqueue_state = 'pending',
              running_since_unix_secs = NULL,
              current_job_key = excluded.current_job_key,
              encrypted_payload = excluded.encrypted_payload,
              display_reason = excluded.display_reason,
              error = NULL,
              config_snapshot = excluded.config_snapshot,
              last_message_at_ms = excluded.last_message_at_ms,
              terminal_reason = NULL,
              cancel_reason = NULL,
              expires_at = excluded.expires_at,
              updated_at = excluded.updated_at")
        .bind(key).bind(&seed.principal_id).bind(&seed.accounting_key_id)
        .bind(seed.upstream_id.to_string()).bind(seed.cycle_id.to_string())
        .bind(existing.0 + 1).bind(anchor).bind(anchor).bind(anchor + 270).bind("5m")
        .bind(cache_keepalive_job_key(key, (existing.0 + 1) as u64))
        .bind(b"old-completion".as_slice()).bind(&seed.display_reason)
        .bind(serde_json::to_string(&seed.config_snapshot).unwrap()).bind(anchor * 1_000)
        .bind(anchor + 300).bind(existing.1).bind(now)
        .execute(&mut *tx).await.unwrap();
    tx.commit().await.unwrap();
    storage.get_cache_keepalive_session(key).await.unwrap()
}

#[tokio::test]
async fn deployed_master_replacement_cannot_resolve_modern_request_marker() {
    // QA: raw-vs-normalized anchors, inclusive old-writer protection, modern
    // replacement/cancellation, expired owner rejection and later legacy resume.
    let (_directory, storage) = storage().await;
    for finish in ["replace", "cancel", "legacy-resume"] {
        let key = format!("mixed-version-shared-row-{finish}");
        storage
            .put_meta_value(&format!("keepalive_order:{key}"), "3:3")
            .await
            .unwrap();
        let mut seed = replace_request(b"seed", 100);
        seed.session_key_hash = key.clone();
        let active = storage.replace_from_real_request(&seed).await.unwrap();
        assert_eq!(active.first_scheduled_at_unix_secs, 100);
        let owner = storage
            .allocate_cache_keepalive_completion_ticket(&key, 105, 900)
            .await
            .unwrap();
        assert_eq!(owner.sequence, 4);
        assert!(
            storage
                .begin_cache_keepalive_request_fenced(&key, Some(owner), 105, 700)
                .await
                .unwrap()
        );
        let marker = storage
            .get_cache_keepalive_session(&key)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(marker.first_scheduled_at_unix_secs, 105);
        assert_eq!(marker.running_since_unix_secs, Some(105));
        assert_eq!(marker.run_at_unix_secs, 700);
        let raw_anchor = sqlx::query_scalar::<_, i64>(
            "SELECT first_scheduled_at FROM cache_keepalive_sessions WHERE session_key_hash = ?",
        )
        .bind(&key)
        .fetch_one(storage.pool())
        .await
        .unwrap();
        assert_eq!(raw_anchor, 901, "owner lease, not shorter marker deadline");
        let v2_key = cc_lb_storage_api::cache_keepalive_order_meta_key(&key);
        let owned_meta = storage.get_meta_value(&v2_key).await.unwrap();
        assert_deployed_active_only_mutations_reject_marker(&storage, &key, marker.generation)
            .await;
        assert_eq!(
            storage
                .get_cache_keepalive_session(&key)
                .await
                .unwrap()
                .as_ref(),
            Some(&marker)
        );
        assert_eq!(storage.get_meta_value(&v2_key).await.unwrap(), owned_meta);
        for anchor in [110, 900] {
            assert!(
                replace_deployed_legacy_request(&storage, &seed, anchor, anchor + 10)
                    .await
                    .is_none()
            );
            assert_eq!(
                storage
                    .get_cache_keepalive_session(&key)
                    .await
                    .unwrap()
                    .as_ref(),
                Some(&marker)
            );
            assert_eq!(storage.get_meta_value(&v2_key).await.unwrap(), owned_meta);
        }
        if finish == "replace" {
            let mut completion = seed.clone();
            completion.cache_anchor_at_unix_secs = 110;
            completion.now_unix_secs = 120;
            let row = storage
                .replace_from_real_request_fenced(&completion, Some(owner))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(row.status, CacheKeepaliveSessionStatus::Active);
            assert_eq!(row.first_scheduled_at_unix_secs, 110);
        } else if finish == "cancel" {
            assert!(
                storage
                    .cancel_cache_keepalive_fenced(
                        &key,
                        Some(owner),
                        110,
                        CacheKeepaliveTerminalReason::Cancelled,
                        Some("request_failed"),
                        120
                    )
                    .await
                    .unwrap()
            );
            let row = storage
                .get_cache_keepalive_session(&key)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(row.first_scheduled_at_unix_secs, 110);
            assert_eq!(row.running_since_unix_secs, None);
        } else {
            assert!(
                !storage
                    .cancel_cache_keepalive_fenced(
                        &key,
                        Some(owner),
                        901,
                        CacheKeepaliveTerminalReason::Cancelled,
                        Some("request_failed"),
                        901
                    )
                    .await
                    .unwrap()
            );
            assert_eq!(
                storage
                    .get_cache_keepalive_session(&key)
                    .await
                    .unwrap()
                    .as_ref(),
                Some(&marker)
            );
            let resumed = replace_deployed_legacy_request(&storage, &seed, 901, 910)
                .await
                .unwrap();
            assert_eq!(resumed.status, CacheKeepaliveSessionStatus::Active);
            assert_eq!(resumed.first_scheduled_at_unix_secs, 901);
        }
        let row = storage
            .get_cache_keepalive_session(&key)
            .await
            .unwrap()
            .unwrap();
        let raw = sqlx::query_scalar::<_, i64>(
            "SELECT first_scheduled_at FROM cache_keepalive_sessions WHERE session_key_hash = ?",
        )
        .bind(&key)
        .fetch_one(storage.pool())
        .await
        .unwrap();
        assert_eq!(
            raw as u64, row.first_scheduled_at_unix_secs,
            "no resolved row retains a synthetic fence"
        );
    }
}

async fn assert_deployed_active_only_mutations_reject_marker(
    storage: &SqliteStorage,
    key: &str,
    generation: u64,
) {
    // Verbatim master cancellation/reschedule statements: both require active.
    let cancelled = sqlx::query("UPDATE cache_keepalive_sessions SET status = 'terminal', enqueue_state = 'pending', terminal_reason = ?, cancel_reason = ?, error = NULL, running_since_unix_secs = NULL,
             encrypted_payload = X'',
             expires_at = CASE WHEN refresh_count > 0 OR EXISTS (SELECT 1 FROM cache_keepalive_turns turn_row WHERE turn_row.principal_id = cache_keepalive_sessions.principal_id AND turn_row.session_key_hash = cache_keepalive_sessions.session_key_hash) THEN MAX(expires_at, ?) ELSE expires_at END,
             updated_at = ?,
             first_scheduled_at = CASE WHEN ? THEN ? ELSE first_scheduled_at END
             WHERE session_key_hash = ? AND status = 'active'
               AND (CASE WHEN ? THEN first_scheduled_at ELSE cache_anchor_at END) <= ?")
        .bind("cancelled").bind("request_failed").bind(604_920_i64).bind(120_i64)
        .bind(true).bind(110_i64).bind(key).bind(true).bind(110_i64)
        .execute(storage.pool()).await.unwrap();
    assert_eq!(cancelled.rows_affected(), 0);
    let refreshed = sqlx::query("UPDATE cache_keepalive_sessions SET generation = ?, refresh_count = refresh_count + 1,
                 cache_anchor_at = ?, run_at = ?, ttl = ttl, status = 'active', enqueue_state = 'pending',
                 running_since_unix_secs = NULL, current_job_key = ?, encrypted_payload = ?, terminal_reason = NULL, error = NULL, expires_at = ?, updated_at = ?
                 WHERE session_key_hash = ? AND generation = ? AND status = 'active'
                 RETURNING *")
        .bind((generation + 1) as i64).bind(110_i64).bind(380_i64)
        .bind(cache_keepalive_job_key(key, generation + 1)).bind(b"old-hit".as_slice())
        .bind(410_i64).bind(120_i64).bind(key).bind(generation as i64)
        .fetch_optional(storage.pool()).await.unwrap();
    assert!(refreshed.is_none());
}

#[tokio::test]
async fn unticketed_marker_fence_restores_real_start_and_active_duration_anchor() {
    let (_directory, storage) = storage().await;
    let key = "unticketed-marker-fence";
    let mut seed = replace_request(b"seed", 100);
    seed.session_key_hash = key.to_owned();
    let active = storage.replace_from_real_request(&seed).await.unwrap();
    let refreshed = storage
        .reschedule_after_cache_hit(&CacheKeepaliveHitRefreshRequest {
            session_key_hash: key.to_owned(),
            generation: active.generation,
            cache_anchor_at_unix_secs: 150,
            run_at_unix_secs: 420,
            expires_at_unix_secs: 450,
            encrypted_payload: None,
            now_unix_secs: 150,
        })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        refreshed.first_scheduled_at_unix_secs, 100,
        "active duration anchor is not a protection fence"
    );
    assert!(
        storage
            .begin_cache_keepalive_request_fenced(key, None, 160, 700)
            .await
            .unwrap()
    );
    let marker = storage
        .get_cache_keepalive_session(key)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(marker.first_scheduled_at_unix_secs, 160);
    let raw = sqlx::query_scalar::<_, i64>(
        "SELECT first_scheduled_at FROM cache_keepalive_sessions WHERE session_key_hash = ?",
    )
    .bind(key)
    .fetch_one(storage.pool())
    .await
    .unwrap();
    assert_eq!(raw, 701, "None-owner direct API uses the marker deadline");
    assert!(
        storage
            .cancel_cache_keepalive_fenced(
                key,
                None,
                170,
                CacheKeepaliveTerminalReason::Cancelled,
                Some("request_failed"),
                180
            )
            .await
            .unwrap()
    );
    let row = storage
        .get_cache_keepalive_session(key)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        row.first_scheduled_at_unix_secs, 160,
        "unticketed finalization restores prior real start"
    );
    assert_eq!(row.running_since_unix_secs, None);
    let raw = sqlx::query_scalar::<_, i64>(
        "SELECT first_scheduled_at FROM cache_keepalive_sessions WHERE session_key_hash = ?",
    )
    .bind(key)
    .fetch_one(storage.pool())
    .await
    .unwrap();
    assert_eq!(raw, 160);
}
