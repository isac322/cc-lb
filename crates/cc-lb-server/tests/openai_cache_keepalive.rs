//! Real listener -> OpenAI wire -> lifecycle -> SQLite -> admin/scheduler regression for #1162.

use std::time::Duration;

use cc_lb_aead::AeadService;
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
        // The 30m renewal is due at 5s, inside the 8s cap; the next at 10s
        // is forbidden. A 1s job at a 1s cap must now be skipped before HTTP.
        fixture
            .configure_keepalive_with_duration(true, true, 1795, 8)
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
    .expect("first renewal must terminalize before another renewal would cross the duration cap");
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

#[tokio::test]
async fn foreground_and_renewal_roll_up_disjoint_net_input_and_native_write_presence() {
    for writes in [None, Some(0), Some(100)] {
        let fixture = OpenAiFixture::start().await;
        fixture
            .configure_keepalive_with_duration(true, true, 1795, 8)
            .await;
        let mut usage = json!({
            "input_tokens": 2048,
            "output_tokens": 16,
            "input_tokens_details": {"cached_tokens": 1024}
        });
        if let Some(writes) = writes {
            usage["input_tokens"] = json!(2048 + writes);
            usage["input_tokens_details"]["cache_write_tokens"] = json!(writes);
            usage["input_tokens_details"]["cache_write_tokens_details"] =
                json!({"ttl_30m_tokens": writes, "ttl_12h_tokens": 0});
        }
        fixture.mock.reply_with_usage(usage.clone());
        fixture.mock.reply_with_usage(usage);
        let session = "00000000-0000-7000-8000-000000001280";
        let response = fixture
            .post(
                session,
                request_body(Some(json!({"ttl": "30m"})), large_input(None), false),
            )
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        response
            .bytes()
            .await
            .expect("foreground response complete");
        fixture.mock.wait_for_requests(2).await;
        let pool = pool(&fixture).await;
        let rows = tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                let rows = sqlx::query(
                    "SELECT input_tokens, cache_read_input_tokens, cache_write_input_tokens, cache_write_input_tokens_30m, cache_write_input_tokens_12h, cache_creation_input_tokens, source_kind FROM request_events_v1 WHERE principal_id = ?",
                )
                .bind(&fixture.principal_id)
                .fetch_all(&pool)
                .await
                .expect("request usage rows");
                if rows.len() == 2 {
                    break rows;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("both foreground and renewal request rows must become durable");
        for row in &rows {
            assert_eq!(row.get::<Option<i64>, _>("input_tokens"), Some(1024));
            assert_eq!(
                row.get::<Option<i64>, _>("cache_read_input_tokens"),
                Some(1024)
            );
            assert_eq!(
                row.get::<Option<i64>, _>("cache_write_input_tokens"),
                writes
            );
            assert_eq!(
                row.get::<Option<i64>, _>("cache_write_input_tokens_30m"),
                writes
            );
            assert_eq!(
                row.get::<Option<i64>, _>("cache_write_input_tokens_12h"),
                writes.map(|_| 0)
            );
            if row.get::<Option<String>, _>("source_kind").as_deref() == Some("renewal") {
                assert_eq!(
                    row.get::<Option<i64>, _>("cache_creation_input_tokens"),
                    None
                );
            }
        }
        let rollup_storage = cc_lb_storage_sqlite::open_sqlite_existing(
            &format!("sqlite://{}", fixture.server.sqlite_path.display()),
            std::sync::Arc::new(cc_lb_clock::SystemClock),
        )
        .await
        .expect("open fixture sqlite for rollup");
        cc_lb_storage_api::UsageRollupStore::rollup_usage_once(&rollup_storage)
            .await
            .expect("roll up durable foreground and renewal events");
        let totals: (i64, i64) = sqlx::query_as(
            "SELECT COALESCE(SUM(request_count), 0), COALESCE(SUM(input_tokens), 0) FROM usage_rollups_v2 WHERE principal_id = ? AND resolution = 'minute'",
        )
        .bind(&fixture.principal_id)
        .fetch_one(&pool)
        .await
        .expect("rollup net input");
        assert_eq!(totals.0, 2);
        assert_eq!(
            totals.1, 2048,
            "foreground and renewal must both use net input"
        );
    }
}

fn decode_snapshot_headers(
    fixture: &OpenAiFixture,
    session_hash: &str,
    row: &sqlx::sqlite::SqliteRow,
) -> Value {
    let payload: Vec<u8> = row.get("encrypted_payload");
    assert!(!payload.is_empty(), "snapshot payload must be encrypted");
    assert_eq!(&payload[..8], b"CCLBCKA2", "snapshot payload framing");
    let codec = payload[8];
    let payload_generation = u64::try_from(row.get::<i64, _>("generation"))
        .expect("snapshot generation")
        .checked_sub(
            u64::try_from(row.get::<i64, _>("refresh_count")).expect("snapshot refresh count"),
        )
        .expect("base payload generation");
    let encoded_len = u64::from_be_bytes(payload[17..25].try_into().expect("encoded length"));
    let upstream_id: uuid::Uuid = fixture.upstream_id.parse().expect("upstream UUID");
    let aad = format!(
        "cache_keepalive_snapshot:v2:{}:{session_hash}:{upstream_id}:{payload_generation}:{codec}:{encoded_len}",
        fixture.principal_id
    );
    let plaintext = AeadService::from_master_key([0; 32])
        .decrypt(&payload[25..], aad.as_bytes())
        .expect("decrypt durable OpenAI snapshot");
    let encoded = match codec {
        0 | 2 => plaintext,
        1 | 3 => zstd::bulk::decompress(&plaintext, encoded_len as usize)
            .expect("decompress durable OpenAI snapshot"),
        other => panic!("unexpected snapshot codec {other}"),
    };
    if codec <= 1 {
        return serde_json::from_slice(&encoded).expect("decode durable OpenAI snapshot JSON");
    }
    assert_eq!(&encoded[..8], b"CCLBPS01", "snapshot frame");
    let metadata_len =
        u64::from_be_bytes(encoded[8..16].try_into().expect("metadata length")) as usize;
    let body_len = u64::from_be_bytes(encoded[16..24].try_into().expect("body length")) as usize;
    let metadata_end = 24 + metadata_len;
    assert_eq!(
        metadata_end + body_len,
        encoded.len(),
        "snapshot frame lengths"
    );
    serde_json::from_slice(&encoded[24..metadata_end]).expect("decode snapshot metadata")
}

fn persisted_header(headers: &Value, name: &str) -> Option<String> {
    headers["headers"]
        .as_array()?
        .iter()
        .find(|header| {
            header["name"]
                .as_str()
                .is_some_and(|value| value.eq_ignore_ascii_case(name))
        })
        .and_then(|header| header["value"].as_array())
        .map(|bytes| {
            bytes
                .iter()
                .map(|byte| byte.as_u64().expect("snapshot header byte") as u8)
                .collect::<Vec<_>>()
        })
        .map(|bytes| String::from_utf8(bytes).expect("snapshot header UTF-8"))
}

async fn run_openai_scope_case(
    index: usize,
    case: &str,
    organization: Option<&str>,
    project: Option<&str>,
    processing_region: Option<&str>,
    mixed_case: bool,
) {
    let fixture = OpenAiFixture::start().await;
    // Keep one paid native renewal inside the cap; the next cadence crosses it.
    fixture
        .configure_keepalive_with_duration(true, true, 1795, 8)
        .await;
    let pool = pool(&fixture).await;
    let session = format!("00000000-0000-7000-8000-{index:012}");
    let input = large_input(None);
    let mut body = request_body(Some(json!({"ttl": "30m"})), input.clone(), true);
    body["store"] = json!(false);
    let mut headers = vec![
        ("authorization", "Bearer client-secret"),
        ("x-goog-api-key", "client-google-secret"),
        ("api-key", "client-api-secret"),
        ("cookie", "client-cookie=secret"),
        ("ChatGPT-Account-ID", "client-account"),
    ];
    if let Some(organization) = organization {
        headers.push((
            if mixed_case {
                "OpenAI-Organization"
            } else {
                "openai-organization"
            },
            organization,
        ));
    }
    if let Some(project) = project {
        headers.push((
            if mixed_case {
                "oPeNaI-pRoJeCt"
            } else {
                "openai-project"
            },
            project,
        ));
    }
    if let Some(processing_region) = processing_region {
        headers.push((
            if mixed_case {
                "OpenAI-Processing-Region"
            } else {
                "openai-processing-region"
            },
            processing_region,
        ));
    }

    let response = fixture
        .post_with_headers(&session, body.clone(), &headers)
        .await;
    assert_eq!(response.status(), StatusCode::OK, "{case} initial response");
    response.bytes().await.expect("initial Responses body");
    let hash = fixture.session_hash(&session);
    let snapshot_row = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if let Some(row) = sqlx::query(
                "SELECT * FROM cache_keepalive_sessions \
                     WHERE session_key_hash = ? AND length(encrypted_payload) > 0",
            )
            .bind(&hash)
            .fetch_optional(&pool)
            .await
            .expect("read persisted OpenAI snapshot")
            {
                break row;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("OpenAI request must persist a renewal snapshot");
    let snapshot = decode_snapshot_headers(&fixture, &hash, &snapshot_row);
    assert_eq!(
        persisted_header(&snapshot, "openai-organization"),
        organization.map(str::to_owned),
        "{case} durable organization selector"
    );
    assert_eq!(
        persisted_header(&snapshot, "openai-project"),
        project.map(str::to_owned),
        "{case} durable project selector"
    );
    assert_eq!(
        persisted_header(&snapshot, "openai-processing-region"),
        processing_region.map(str::to_owned),
        "{case} durable processing-region selector"
    );
    for name in [
        "authorization",
        "x-api-key",
        "x-goog-api-key",
        "api-key",
        "cookie",
        "chatgpt-account-id",
    ] {
        assert!(
            persisted_header(&snapshot, name).is_none(),
            "{case} durable snapshot must exclude credential header {name}"
        );
    }

    fixture.mock.wait_for_requests(2).await;
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let row = sqlx::query(
                "SELECT status, terminal_reason FROM cache_keepalive_sessions \
                 WHERE session_key_hash = ?",
            )
            .bind(&hash)
            .fetch_one(&pool)
            .await
            .expect("post-renewal session state");
            if row.get::<String, _>("status") == "terminal" {
                assert_eq!(
                    row.get::<String, _>("terminal_reason"),
                    "max_duration",
                    "{case} renewal must honor duration cap"
                );
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("one OpenAI renewal must complete durably");

    let requests = fixture.mock.requests();
    assert_eq!(
        requests.len(),
        2,
        "{case} must have exactly one initial request and one renewal"
    );
    let initial = &requests[0];
    let renewal = &requests[1];
    for request in [initial, renewal] {
        assert_eq!(
            request
                .headers
                .get("authorization")
                .and_then(|v| v.to_str().ok()),
            Some("Bearer sk-openai-cache-test"),
            "{case} provider credential"
        );
        assert_eq!(
            request
                .headers
                .get("openai-organization")
                .and_then(|v| v.to_str().ok()),
            organization,
            "{case} organization selector"
        );
        assert_eq!(
            request
                .headers
                .get("openai-project")
                .and_then(|v| v.to_str().ok()),
            project,
            "{case} project selector"
        );
        assert_eq!(
            request
                .headers
                .get("openai-processing-region")
                .and_then(|v| v.to_str().ok()),
            processing_region,
            "{case} processing-region selector"
        );
    }
    for name in [
        "x-api-key",
        "x-goog-api-key",
        "api-key",
        "cookie",
        "chatgpt-account-id",
    ] {
        assert!(
            renewal.headers.get(name).is_none(),
            "{case} renewal must not replay credential header {name}"
        );
    }
    assert_eq!(initial.body["input"], body["input"], "{case} input");
    assert_eq!(
        initial.body["prompt_cache_options"], body["prompt_cache_options"],
        "{case} initial cache options"
    );
    assert_eq!(initial.body["store"], false, "{case} initial store");
    assert_eq!(renewal.body["input"], body["input"], "{case} renewal input");
    assert_eq!(
        renewal.body["prompt_cache_options"], body["prompt_cache_options"],
        "{case} renewal cache options"
    );
    assert_eq!(renewal.body["store"], false, "{case} renewal store");
    assert_eq!(renewal.body["max_output_tokens"], 16, "{case} renewal cap");
    assert_eq!(renewal.body["stream"], false, "{case} renewal buffering");
}

#[tokio::test]
async fn openai_scope_selectors_survive_one_scheduled_renewal_without_credentials() {
    for (index, (case, organization, project)) in [
        ("both", Some("org-client"), Some("proj-client")),
        ("organization-only", Some("org-client"), None),
        ("project-only", None, Some("proj-client")),
    ]
    .into_iter()
    .enumerate()
    {
        run_openai_scope_case(
            index,
            case,
            organization,
            project,
            (case == "both").then_some("us"),
            case == "both",
        )
        .await;
    }
}

#[tokio::test]
async fn openai_scope_selector_free_renewal_is_healthy() {
    run_openai_scope_case(900, "neither", None, None, None, false).await;
}
