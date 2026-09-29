use std::collections::HashSet;

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};

use super::fixtures::{principal_create_body_named, seed_cache_keepalive_decision};
use super::*;

const DAY_SECS: u64 = 24 * 60 * 60;

async fn create_cursor_test_principal(
    name: &str,
) -> (
    tempfile::TempDir,
    Arc<cc_lb_storage_sqlite::SqliteStorage>,
    cc_lb_admin::AdminState,
    Arc<TestClock>,
    String,
) {
    let clock = Arc::new(TestClock::new_at_secs(NOW_UNIX_SECS));
    let clock_handle: ClockHandle = clock.clone();
    let (directory, storage) =
        config_admin_common::temp_storage_with_clock(clock_handle.clone()).await;
    let state = config_admin_common::test_state_with_clock(
        Config::default(),
        Some(storage.clone()),
        clock_handle,
    );
    let (status, _headers, body, _raw) = authed_json(
        app(state.clone()),
        "POST",
        "/admin/v1/principals",
        Some(principal_create_body_named(name, true)),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "principal create: {body:?}");
    let principal_id = body["id"]
        .as_str()
        .expect("created principal id")
        .to_owned();
    (directory, storage, state, clock, principal_id)
}

async fn create_additional_principal(state: &cc_lb_admin::AdminState, name: &str) -> String {
    let (status, _headers, body, _raw) = authed_json(
        app(state.clone()),
        "POST",
        "/admin/v1/principals",
        Some(principal_create_body_named(name, true)),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "principal create: {body:?}");
    body["id"]
        .as_str()
        .expect("created principal id")
        .to_owned()
}

async fn seed_page_chain(
    storage: &cc_lb_storage_sqlite::SqliteStorage,
    principal_id: &str,
    prefix: &str,
    horizon_secs: u64,
    row_count: usize,
) {
    let horizon_start_secs = NOW_UNIX_SECS.saturating_sub(horizon_secs);
    for index in 0..row_count {
        let position = u64::try_from(index + 1).expect("fixture position fits u64");
        seed_cache_keepalive_decision(
            storage,
            principal_id,
            &format!("{prefix}-{position}"),
            horizon_start_secs
                .saturating_add(position.saturating_mul(60))
                .saturating_mul(1_000),
        )
        .await;
    }
}

async fn get_json(state: cc_lb_admin::AdminState, uri: &str) -> (StatusCode, Value) {
    let (status, _headers, raw) = authed_bytes(app(state), "GET", uri, None).await;
    let body = serde_json::from_slice(&raw).expect("response JSON");
    (status, body)
}

fn decode_cursor(cursor: &str) -> Value {
    let payload = URL_SAFE_NO_PAD.decode(cursor).expect("cursor base64url");
    serde_json::from_slice(&payload).expect("cursor JSON")
}

fn encode_cursor(cursor: &Value) -> String {
    URL_SAFE_NO_PAD.encode(serde_json::to_vec(cursor).expect("cursor JSON encode"))
}

fn assert_cursor_scope(cursor: &str, horizon: &str, horizon_start_ms: u64) {
    let decoded = decode_cursor(cursor);
    assert_eq!(decoded["horizon"], horizon);
    assert_eq!(decoded["horizon_start_ms"], horizon_start_ms);
}

fn insert_row_ids(body: &Value, seen: &mut HashSet<String>) {
    for row in body["rows"].as_array().expect("list rows") {
        let id = row["id"].as_str().expect("row id").to_owned();
        assert!(seen.insert(id.clone()), "duplicate paginated row {id}");
    }
}

#[tokio::test]
async fn cur_01_24h_and_7d_page_chains_survive_clock_advance_with_frozen_start() {
    for (horizon, horizon_secs) in [("24h", DAY_SECS), ("7d", DAY_SECS * 7)] {
        let (_directory, storage, state, clock, principal_id) =
            create_cursor_test_principal(&format!("cursor-cur-01-{horizon}")).await;
        seed_page_chain(&storage, &principal_id, horizon, horizon_secs, 6).await;
        let base_uri = format!(
            "/admin/v1/principals/{principal_id}/cache-keepalive?horizon={horizon}&limit=2"
        );
        let expected_horizon_start_ms = NOW_UNIX_SECS
            .saturating_sub(horizon_secs)
            .saturating_mul(1_000);

        let (status, first) = get_json(state.clone(), &base_uri).await;
        assert_eq!(status, StatusCode::OK, "{horizon} first page: {first:?}");
        assert_eq!(first["rows"].as_array().expect("first rows").len(), 2);
        let mut cursor = first["next_cursor"]
            .as_str()
            .expect("first next cursor")
            .to_owned();
        assert_cursor_scope(&cursor, horizon, expected_horizon_start_ms);
        let mut seen = HashSet::new();
        insert_row_ids(&first, &mut seen);

        for page_number in 2..=3 {
            clock.advance_secs(2 * 60 * 60);
            let page_uri = format!("{base_uri}&cursor={cursor}");
            let (status, page) = get_json(state.clone(), &page_uri).await;
            assert_eq!(
                status,
                StatusCode::OK,
                "{horizon} page {page_number} after time advance: {page:?}"
            );
            assert_eq!(page["rows"].as_array().expect("page rows").len(), 2);
            insert_row_ids(&page, &mut seen);
            if page_number < 3 {
                cursor = page["next_cursor"]
                    .as_str()
                    .expect("continuation cursor")
                    .to_owned();
                assert_cursor_scope(&cursor, horizon, expected_horizon_start_ms);
            } else {
                assert!(page["next_cursor"].is_null());
            }
        }
        assert_eq!(seen.len(), 6, "{horizon} page chain lost rows");
    }
}

#[tokio::test]
async fn cur_02_cursor_scope_principal_and_filter_mismatches_stay_bad_request() {
    let (_directory, storage, state, _clock, principal_id) =
        create_cursor_test_principal("cursor-cur-02-primary").await;
    seed_page_chain(&storage, &principal_id, "cur-02", DAY_SECS, 3).await;
    let other_principal_id = create_additional_principal(&state, "cursor-cur-02-other").await;
    let first_uri =
        format!("/admin/v1/principals/{principal_id}/cache-keepalive?horizon=24h&limit=1");
    let (status, first) = get_json(state.clone(), &first_uri).await;
    assert_eq!(status, StatusCode::OK, "first page: {first:?}");
    let cursor = first["next_cursor"].as_str().expect("next cursor");

    let mut untagged_cursor = decode_cursor(cursor);
    untagged_cursor
        .as_object_mut()
        .expect("cursor object")
        .remove("horizon");
    let untagged_cursor = encode_cursor(&untagged_cursor);
    let untagged_uri = format!("{first_uri}&cursor={untagged_cursor}");

    let wrong_requested_horizon = format!(
        "/admin/v1/principals/{principal_id}/cache-keepalive?horizon=7d&limit=1&cursor={cursor}"
    );
    let wrong_principal = format!(
        "/admin/v1/principals/{other_principal_id}/cache-keepalive?horizon=24h&limit=1&cursor={cursor}"
    );
    let wrong_filter = format!(
        "/admin/v1/principals/{principal_id}/cache-keepalive?horizon=24h&status=not_tracked&limit=1&cursor={cursor}"
    );

    let mut tampered_scope = decode_cursor(cursor);
    tampered_scope["horizon"] = json!("7d");
    let tampered_scope = encode_cursor(&tampered_scope);
    let tampered_scope_uri = format!(
        "/admin/v1/principals/{principal_id}/cache-keepalive?horizon=24h&limit=1&cursor={tampered_scope}"
    );

    let mut null_scope = decode_cursor(cursor);
    null_scope["horizon"] = Value::Null;
    let null_scope = encode_cursor(&null_scope);
    let null_scope_uri = format!(
        "/admin/v1/principals/{principal_id}/cache-keepalive?horizon=24h&limit=1&cursor={null_scope}"
    );

    let mut tampered_start = decode_cursor(cursor);
    tampered_start["horizon_start_ms"] = Value::Null;
    let tampered_start = encode_cursor(&tampered_start);
    let tampered_start_uri = format!(
        "/admin/v1/principals/{principal_id}/cache-keepalive?horizon=24h&limit=1&cursor={tampered_start}"
    );

    for (case, uri) in [
        ("requested horizon", wrong_requested_horizon),
        ("principal", wrong_principal),
        ("filter", wrong_filter),
        ("tampered horizon tag", tampered_scope_uri),
        ("tampered horizon start shape", tampered_start_uri),
        ("null horizon tag", null_scope_uri),
        ("missing horizon tag", untagged_uri),
    ] {
        let (status, body) = get_json(state.clone(), &uri).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{case}: {body:?}");
    }
}

#[tokio::test]
async fn cur_03_cursor_anchor_remains_frozen_across_repeated_clock_advances() {
    let (_directory, storage, state, clock, principal_id) =
        create_cursor_test_principal("cursor-cur-03").await;
    seed_page_chain(&storage, &principal_id, "cur-03", DAY_SECS, 8).await;
    let base_uri =
        format!("/admin/v1/principals/{principal_id}/cache-keepalive?horizon=24h&limit=1");
    let expected_horizon_start_ms = NOW_UNIX_SECS.saturating_sub(DAY_SECS).saturating_mul(1_000);
    let (status, first) = get_json(state.clone(), &base_uri).await;
    assert_eq!(status, StatusCode::OK, "first page: {first:?}");
    let mut seen = HashSet::new();
    insert_row_ids(&first, &mut seen);
    let mut cursor = first["next_cursor"]
        .as_str()
        .expect("first next cursor")
        .to_owned();
    assert_cursor_scope(&cursor, "24h", expected_horizon_start_ms);

    for advance_hours in [1, 3, 6, 12, 24, 48, 96] {
        clock.advance_secs(advance_hours * 60 * 60);
        let page_uri = format!("{base_uri}&cursor={cursor}");
        let (status, page) = get_json(state.clone(), &page_uri).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "page after advancing {advance_hours}h: {page:?}"
        );
        assert_eq!(page["rows"].as_array().expect("page rows").len(), 1);
        insert_row_ids(&page, &mut seen);
        match page["next_cursor"].as_str() {
            Some(next_cursor) => {
                assert_cursor_scope(next_cursor, "24h", expected_horizon_start_ms);
                cursor = next_cursor.to_owned();
            }
            None => assert_eq!(seen.len(), 8, "page chain ended early"),
        }
    }
    assert_eq!(seen.len(), 8, "repeated advances lost anchored rows");
}
