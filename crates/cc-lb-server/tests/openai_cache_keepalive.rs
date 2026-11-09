//! Real listener -> OpenAI wire -> lifecycle -> SQLite -> admin/scheduler regression for #1162.

use std::time::Duration;

use reqwest::StatusCode;
use serde_json::{Value, json};
use sqlx::{Row, SqlitePool, sqlite::SqlitePoolOptions};

use crate::openai_cache_keepalive_support::{OpenAiFixture, large_input, request_body};

async fn pool(fixture: &OpenAiFixture) -> SqlitePool {
    SqlitePoolOptions::new()
        .max_connections(2)
        .connect(&format!(
            "sqlite://{}",
            fixture.server.sqlite_path.display()
        ))
        .await
        .expect("OpenAI keepalive SQLite")
}

async fn wait_for_session(pool: &SqlitePool, hash: &str) -> sqlx::sqlite::SqliteRow {
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if let Some(row) = sqlx::query(
                "SELECT * FROM cache_keepalive_sessions WHERE session_key_hash = ? \
                 AND status = 'active' AND enqueue_state = 'enqueued'",
            )
            .bind(hash)
            .fetch_optional(pool)
            .await
            .expect("read exact OpenAI keepalive session")
            {
                return row;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("documented OpenAI request must create an active enqueued session")
}

async fn wait_for_decision(pool: &SqlitePool, hash: &str) -> sqlx::sqlite::SqliteRow {
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if let Some(row) = sqlx::query(
                "SELECT * FROM cache_keepalive_decisions \
                 WHERE session_key_hash = ? AND decision = 'not_tracked' ORDER BY ts DESC LIMIT 1",
            )
            .bind(hash)
            .fetch_optional(pool)
            .await
            .expect("read exact OpenAI not-tracked decision")
            {
                return row;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("ineligible OpenAI cache lifetime/evidence must settle durably")
}

async fn assert_no_active_session(pool: &SqlitePool, hash: &str) {
    let active: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM cache_keepalive_sessions WHERE session_key_hash = ? AND status = 'active'",
    )
    .bind(hash)
    .fetch_one(pool)
    .await
    .expect("active session count");
    assert_eq!(
        active, 0,
        "ineligible request must not create active renewal state"
    );
}

fn assert_snapshot(row: &sqlx::sqlite::SqliteRow, expected_input: &Value) {
    assert_eq!(row.get::<String, _>("ttl"), "30m");
    assert_eq!(row.get::<i64, _>("generation"), 1);
    assert_eq!(row.get::<i64, _>("refresh_count"), 0);
    let anchor = row.get::<i64, _>("cache_anchor_at");
    let run_at = row.get::<i64, _>("run_at");
    assert_eq!(
        row.get::<i64, _>("expires_at") - anchor,
        1800,
        "30m snapshot expiry remains anchored at its native TTL"
    );
    assert!(
        (1769..=1770).contains(&(run_at - anchor)),
        "scheduler stores whole-second TTL-minus-lead boundary, delta={}",
        run_at - anchor
    );
    let encrypted: Vec<u8> = row.get("encrypted_payload");
    assert!(
        !encrypted.is_empty(),
        "renewal snapshot must be encrypted and durable"
    );
    let prefix = expected_input[0]["content"]
        .as_str()
        .unwrap_or("stable OpenAI cache prefix");
    assert!(
        !encrypted
            .windows(prefix.len().min(64))
            .any(|part| part == &prefix.as_bytes()[..prefix.len().min(64)])
    );
    let config: Value = serde_json::from_str(&row.get::<String, _>("config_snapshot"))
        .expect("snapshot config JSON");
    assert_eq!(config["max_refreshes_per_session"], 20);
    assert!(
        config["max_total_duration_secs"]
            .as_u64()
            .expect("duration")
            >= 1770
    );
}

#[tokio::test]
async fn documented_default_empty_and_30m_responses_schedule_encrypted_sessions() {
    let fixture = OpenAiFixture::start().await;
    let pool = pool(&fixture).await;
    for (index, options, stream) in [
        (0, None, false),
        (1, Some(json!({})), false),
        (2, Some(json!({"ttl":"30m"})), false),
        (3, Some(json!({"ttl":"30m"})), true),
    ] {
        let session = format!("00000000-0000-7000-8000-{index:012}");
        let hash = fixture.session_hash(&session);
        let input = large_input(None);
        let body = request_body(options.clone(), input.clone(), stream);
        let response = fixture.post(&session, body.clone()).await;
        assert_eq!(response.status(), StatusCode::OK);
        let wire = response.bytes().await.expect("complete proxied response");
        assert_eq!(
            wire.as_ref(),
            fixture.mock.response_wire(index).as_slice(),
            "ordinary response bytes must pass through unchanged"
        );
        if stream {
            assert!(
                wire.windows(b"response.completed".len())
                    .any(|part| part == b"response.completed")
            );
        } else {
            let value: Value = serde_json::from_slice(&wire).expect("buffered Responses JSON");
            assert_eq!(
                value["usage"]["input_tokens_details"]["cached_tokens"],
                2048
            );
        }
        let row = wait_for_session(&pool, &hash).await;
        assert_snapshot(&row, &input);
        let admin = fixture.list_keepalive().await;
        let admin_row = admin["rows"]
            .as_array()
            .expect("admin rows")
            .iter()
            .find(|row| row["session_key_hash"] == hash)
            .unwrap_or_else(|| panic!("scheduled OpenAI session missing from admin: {admin}"));
        assert_eq!(admin_row["ttl"], "30m");
        assert_eq!(admin_row["state"], "scheduled");
        let requests = fixture.mock.requests();
        let recorded = requests.last().expect("provider received request");
        assert_eq!(
            recorded.body, body,
            "ordinary provider wire must remain unchanged"
        );
        assert_eq!(
            serde_json::from_slice::<Value>(&recorded.raw_body).expect("provider JSON"),
            body
        );
        assert_eq!(recorded.stream, stream);
        assert_eq!(
            recorded.response_cache_options,
            options.unwrap_or(Value::Null)
        );
    }
}

#[tokio::test]
async fn selected_12h_is_not_tracked_and_reads_never_schedule_or_reanchor_it() {
    let fixture = OpenAiFixture::start().await;
    let pool = pool(&fixture).await;
    let session = "00000000-0000-7000-8000-000000000120";
    let hash = fixture.session_hash(session);
    let request = request_body(Some(json!({"ttl":"30m"})), large_input(Some("12h")), false);
    let first = fixture.post(session, request.clone()).await;
    assert_eq!(first.status(), StatusCode::OK);
    first.bytes().await.expect("first Responses body");
    let decision = wait_for_decision(&pool, &hash).await;
    let first_source_ref = decision.get::<String, _>("source_ref_id").to_owned();
    assert_eq!(decision.get::<String, _>("ttl"), "12h");
    assert_no_active_session(&pool, &hash).await;
    let second = fixture.post(session, request).await;
    assert_eq!(second.status(), StatusCode::OK);
    second.bytes().await.expect("second Responses body");
    fixture.mock.wait_for_requests(2).await;
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM cache_keepalive_decisions WHERE session_key_hash = ? AND decision = 'not_tracked'",
            )
            .bind(&hash)
            .fetch_one(&pool)
            .await
            .expect("12h decision count");
            if count >= 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("second 12h read must persist a fresh decision");
    let latest = sqlx::query(
        "SELECT * FROM cache_keepalive_decisions WHERE session_key_hash = ? AND source_ref_id != ? AND decision = 'not_tracked'",
    )
    .bind(&hash)
    .bind(&first_source_ref)
    .fetch_one(&pool)
    .await
    .expect("second 12h read decision");
    assert_eq!(latest.get::<String, _>("ttl"), "12h");
    assert_ne!(latest.get::<String, _>("source_ref_id"), first_source_ref);
    assert_eq!(
        latest.get::<String, _>("reason"),
        decision.get::<String, _>("reason")
    );
    assert_no_active_session(&pool, &hash).await;
    let admin = fixture.list_keepalive().await;
    let rows = admin["rows"].as_array().expect("admin rows");
    assert!(
        rows.iter()
            .any(|row| row["session_key_hash"] == hash && row["state"] == "not_tracked"),
        "12h must remain visible as existing not-tracked decision: {admin}"
    );
    let scheduler = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(&format!(
            "sqlite://{}",
            fixture
                .server
                .sqlite_path
                .with_extension("scheduler.sqlite")
                .display()
        ))
        .await
        .expect("scheduler SQLite");
    let jobs: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM Jobs WHERE job_type = 'cache_keepalive' AND idempotency_key LIKE ?",
    )
    .bind(format!("%{hash}%"))
    .fetch_one(&scheduler)
    .await
    .expect("12h job count");
    assert_eq!(jobs, 0, "12h reads must never produce renewal jobs");
}

#[tokio::test]
async fn native_evidence_and_both_opt_ins_remain_required() {
    let fixture = OpenAiFixture::start().await;
    let pool = pool(&fixture).await;
    for (index, tokens) in [(0, None), (1, Some(0)), (2, Some(1023))] {
        let session = format!("00000000-0000-7000-8000-{:012}", 200 + index);
        fixture.mock.reply_with_cached_tokens(tokens);
        let response = fixture
            .post(&session, request_body(None, large_input(None), false))
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        response.bytes().await.expect("native evidence response");
        let hash = fixture.session_hash(&session);
        wait_for_decision(&pool, &hash).await;
        assert_no_active_session(&pool, &hash).await;
    }
    for (index, enabled, openai_enabled, model) in [
        (3, false, true, "gpt-5.6"),
        (4, true, false, "gpt-5.6"),
        (5, true, true, "gpt-5.5"),
    ] {
        fixture
            .configure_keepalive(enabled, openai_enabled, 30)
            .await;
        let session = format!("00000000-0000-7000-8000-{:012}", 200 + index);
        let mut body = request_body(None, large_input(None), false);
        body["model"] = json!(model);
        let response = fixture.post(&session, body).await;
        assert_eq!(response.status(), StatusCode::OK);
        response.bytes().await.expect("opt-out response");
        fixture.mock.wait_for_requests(index + 1).await;
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                let count: i64 = sqlx::query_scalar(
                    "SELECT COUNT(*) FROM request_events_v1 WHERE observed_session_id = ? AND list_status = 200",
                )
                .bind(&session)
                .fetch_one(&pool)
                .await
                .expect("opt-out durable completion boundary");
                if count > 0 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("opt-out request must settle before active-state assertion");
        assert_no_active_session(&pool, &fixture.session_hash(&session)).await;
    }
}

#[tokio::test]
async fn scheduler_dispatches_openai_renewal_and_persists_native_hit_with_caps() {
    let mut tool_input = large_input(None);
    let items = tool_input.as_array_mut().expect("input array");
    items.insert(
        1,
        json!({
            "type": "function_call", "call_id": "call_renewal", "name": "lookup", "arguments": "{}"
        }),
    );
    items.insert(
        2,
        json!({
            "type": "function_call_output", "call_id": "call_renewal", "output": [{
                "type": "input_text", "text": "stable tool result",
                "prompt_cache_breakpoint": {"mode": "explicit", "ttl": "30m"}
            }]
        }),
    );
    for (input, marker_pointer) in [
        (
            large_input(Some("30m")),
            "/input/0/content/0/prompt_cache_breakpoint/ttl",
        ),
        (tool_input, "/input/2/output/0/prompt_cache_breakpoint/ttl"),
    ] {
        let fixture = OpenAiFixture::start().await;
        fixture
            .configure_keepalive_with_duration(true, true, 1799, 1)
            .await;
        let pool = pool(&fixture).await;
        let session = "00000000-0000-7000-8000-000000000300";
        let hash = fixture.session_hash(session);
        let response = fixture
            .post(
                session,
                request_body(Some(json!({"ttl":"12h"})), input.clone(), true),
            )
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        response.bytes().await.expect("stream completed");
        fixture.mock.wait_for_requests(2).await;
        tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let row = sqlx::query("SELECT hit_miss, cache_read_input_tokens FROM cache_keepalive_turns WHERE session_key_hash = ? LIMIT 1")
                .bind(&hash).fetch_optional(&pool).await.expect("durable native renewal");
            if let Some(row) = row {
                assert_eq!(row.get::<String, _>("hit_miss"), "hit");
                assert_eq!(row.get::<i64, _>("cache_read_input_tokens"), 2048);
                break;
            }
            tokio::task::yield_now().await;
        }
    }).await.expect("scheduler native outcome must be durable");
        let requests = fixture.mock.requests();
        let renewal = &requests[1].body;
        assert_eq!(renewal["input"], input, "renewal preserves selected prefix");
        assert_eq!(
            renewal["prompt_cache_options"]["ttl"], "12h",
            "full replay retains request TTL while selected breakpoint overrides it"
        );
        assert_eq!(
            renewal
                .pointer(marker_pointer)
                .expect("selected marker TTL"),
            "30m"
        );
        assert_eq!(renewal["max_output_tokens"], 16);
        assert_eq!(renewal["stream"], false);
        tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let row = sqlx::query(
                "SELECT status, terminal_reason, refresh_count FROM cache_keepalive_sessions WHERE session_key_hash = ?",
            )
            .bind(&hash)
            .fetch_one(&pool)
            .await
            .expect("post-renewal capped session");
            if row.get::<String, _>("status") == "terminal" {
                assert_eq!(row.get::<String, _>("terminal_reason"), "max_duration");
                assert_eq!(row.get::<i64, _>("refresh_count"), 0);
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("first renewal must terminalize at the one-second duration cap");
        assert_eq!(
            fixture.mock.requests().len(),
            2,
            "one ordinary request plus exactly one renewal"
        );
        let scheduler = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(&format!(
                "sqlite://{}",
                fixture
                    .server
                    .sqlite_path
                    .with_extension("scheduler.sqlite")
                    .display()
            ))
            .await
            .expect("duration-cap scheduler SQLite");
        let next_job = cc_lb_storage_api::cache_keepalive_job_key(&hash, 2);
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM Jobs WHERE idempotency_key = ?")
            .bind(next_job)
            .fetch_one(&scheduler)
            .await
            .expect("generation-two job count");
        assert_eq!(
            count, 0,
            "duration cap must prevent generation-two scheduling"
        );
    }
}
