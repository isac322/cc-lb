mod config_admin_common;
#[path = "events_request_log_contracts/support.rs"]
mod events_request_log_contracts_support;

use std::sync::Arc;

use axum::http::StatusCode;
use cc_lb_config::Config;
use cc_lb_control::RequestEventBus;
use cc_lb_storage_api::RequestEventStore;
use config_admin_common::{app, authed_json, temp_storage, test_state};
use events_request_log_contracts_support::{
    BROAD_REQUEST_ID, CANCELED_REQUEST_ID, DROPPED_REQUEST_ID, EVENT_ID, MODEL,
    STRUCTURED_REQUEST_ID, THREAD_ID, UPSTREAM_ID, UPSTREAM_NAME, event_bus, publish_enriched,
    publish_recent_contracts, read_message_updates_through_final_window, stream_response,
    wait_for_initial_cursor,
};

#[tokio::test]
async fn events_stream_orders_parse_auth_route_enrichment_before_one_final() {
    let (_dir, storage) = temp_storage().await;
    let bus = event_bus();
    let mut state = test_state(Config::default(), Some(Arc::clone(&storage)));
    state.event_bus = Some(Arc::clone(&bus) as Arc<dyn RequestEventBus>);
    let response = stream_response(state).await;
    let mut body = response.into_body();
    wait_for_initial_cursor(&mut body).await;

    publish_enriched(&bus, storage.as_ref()).await;
    let updates = read_message_updates_through_final_window(&mut body).await;

    assert_eq!(
        updates
            .iter()
            .map(|update| update["phase"].as_str().expect("phase"))
            .collect::<Vec<_>>(),
        ["partial", "partial", "partial", "partial", "final"],
    );
    assert_eq!(
        updates
            .iter()
            .filter(|update| update["phase"] == "final")
            .count(),
        1,
    );

    let parse = &updates[0]["payload"];
    assert_eq!(parse["event_id"], EVENT_ID);
    assert_eq!(parse["thread_id"], THREAD_ID);
    assert_eq!(parse["model"], MODEL);
    assert!(parse["principal_id"].is_null());
    assert!(parse["upstream_id"].is_null());

    let auth = &updates[1]["payload"];
    assert_eq!(auth["event_id"], EVENT_ID);
    assert_eq!(auth["thread_id"], THREAD_ID);
    assert_eq!(auth["model"], MODEL);
    assert_eq!(auth["principal_id"], "principal-admin-contract");
    assert!(auth["upstream_id"].is_null());

    let route = &updates[2]["payload"];
    assert_eq!(route["event_id"], EVENT_ID);
    assert_eq!(route["thread_id"], THREAD_ID);
    assert_eq!(route["principal_id"], "principal-admin-contract");
    assert_eq!(route["upstream_id"], UPSTREAM_ID.to_string());
    assert_eq!(route["upstream_name"], UPSTREAM_NAME);

    let terminated = &updates[3]["payload"];
    assert_eq!(terminated["event_id"], EVENT_ID);

    let final_event = &updates[4]["payload"]["event"];
    assert_eq!(final_event["event_id"], EVENT_ID);
    assert_eq!(final_event["thread_id"], THREAD_ID);
    assert_eq!(final_event["status"], 200);
    let rows = storage
        .query_request_events(0, u64::MAX, 10)
        .await
        .expect("persisted stream contract row");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].event_id.as_deref(), Some(EVENT_ID));
}

#[tokio::test]
async fn events_recent_returns_structured_429_and_distinct_499_without_control_fabrication() {
    let (_dir, storage) = temp_storage().await;
    publish_recent_contracts(storage.as_ref()).await;
    let state = test_state(Config::default(), Some(Arc::clone(&storage)));

    let (status, _, four_xx, _) = authed_json(
        app(state.clone()),
        "GET",
        "/admin/events/recent?since_unix_secs=1700000000&until_unix_secs=1900000000&limit=3&status_class=4xx",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(four_xx["count"], 3);
    assert_eq!(four_xx["limit"], 3);
    let events = four_xx["events"].as_array().expect("4xx events");

    let structured = event_by_request_id(events, STRUCTURED_REQUEST_ID);
    assert_eq!(structured["status"], 429);
    assert_eq!(structured["error_code"], "upstream_4xx");
    assert_eq!(structured["upstream_error_type"], "rate_limit_error");
    assert_eq!(
        structured["upstream_error_message"],
        "bounded provider message"
    );

    let canceled = event_by_request_id(events, CANCELED_REQUEST_ID);
    assert_eq!(canceled["status"], 499);
    assert_eq!(canceled["error_code"], "client_closed_request");
    assert!(canceled["upstream_error_type"].is_null());
    assert!(canceled["upstream_error_message"].is_null());

    let broad = event_by_request_id(events, BROAD_REQUEST_ID);
    assert_eq!(broad["status"], 429);
    assert_eq!(broad["error_code"], "upstream_4xx");
    assert_no_fabricated_diagnostics(broad);

    let (status, _, five_xx, _) = authed_json(
        app(state),
        "GET",
        "/admin/events/recent?since_unix_secs=1700000000&until_unix_secs=1900000000&limit=1&status_class=5xx",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(five_xx["count"], 1);
    assert_eq!(five_xx["limit"], 1);
    let dropped = &five_xx["events"][0];
    assert_eq!(dropped["request_id"], DROPPED_REQUEST_ID);
    assert_eq!(dropped["status"], 504);
    assert_eq!(dropped["error_code"], "terminal_dropped");
    assert_no_fabricated_diagnostics(dropped);

    let rows = RequestEventStore::query_request_events(storage.as_ref(), 0, u64::MAX, 10)
        .await
        .expect("all request-log contract rows persist");
    assert_eq!(rows.len(), 4);
}

fn event_by_request_id<'a>(
    events: &'a [serde_json::Value],
    request_id: &str,
) -> &'a serde_json::Value {
    events
        .iter()
        .find(|event| event["request_id"] == request_id)
        .expect("request event exists")
}

fn assert_no_fabricated_diagnostics(event: &serde_json::Value) {
    assert!(event["upstream_error_type"].is_null());
    assert!(event["upstream_error_message"].is_null());
    assert!(event["principal_id"].is_null());
    assert!(event["model"].is_null());
    assert!(event["upstream_id"].is_null());
    assert!(event["upstream_name"].is_null());
}
