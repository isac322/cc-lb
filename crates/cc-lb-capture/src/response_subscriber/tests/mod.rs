mod fixtures;

use cc_lb_lifecycle::{LifecycleEvent, LimitDecisionKind, TerminationReason};
use uuid::Uuid;

use crate::schema::Disposition;

use fixtures::*;

#[tokio::test(flavor = "current_thread")]
async fn success_joins_route_usage_and_terminal_response() -> TestResult<()> {
    // Given
    let event_id = "event-success";
    let request_id = "request-success";
    let upstream_id = Uuid::from_u128(1);

    // When
    let rows = run(
        vec![
            started(event_id, request_id),
            route(event_id, upstream_id),
            attempt(event_id, 1, upstream_id),
            response_started(event_id, 200),
            usage_observed(event_id),
            terminated(event_id, TerminationReason::Success, 200),
        ],
        vec![input(event_id, request_id)],
    )
    .await?;

    // Then
    let [record] = rows.as_slice() else {
        panic!("expected one capture row, got {}", rows.len());
    };
    assert_eq!(record.disposition, Disposition::RoutedDispatchedSuccess);
    assert_eq!(record.response.chosen_upstream_id, Some(upstream_id));
    assert_eq!(record.response.upstream_status, Some(200));
    assert_eq!(record.response.client_status, Some(200));
    assert_eq!(record.response.duration_ms, Some(345));
    assert_eq!(record.response.attempt_num, Some(1));
    assert_eq!(record.response.input_tokens, Some(2_000));
    assert_eq!(record.response.cache_read_input_tokens, Some(1_000));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn denied_limit_maps_to_limit_rejected_without_usage() -> TestResult<()> {
    // Given
    let event_id = "event-limit";
    let request_id = "request-limit";
    let upstream_id = Uuid::from_u128(2);

    // When
    let rows = run(
        vec![
            started(event_id, request_id),
            route(event_id, upstream_id),
            LifecycleEvent::LimitDecision {
                event_id: event_id.to_owned(),
                decision: LimitDecisionKind::Rejected {
                    reason: "token_limit".to_owned(),
                    subject: None,
                    request_summary: None,
                    route_summary: None,
                    limit_violation: None,
                },
            },
            terminated(
                event_id,
                TerminationReason::ErrorCode("limit_rejected".to_owned()),
                429,
            ),
        ],
        vec![input(event_id, request_id)],
    )
    .await?;

    // Then
    let [record] = rows.as_slice() else {
        panic!("expected one capture row, got {}", rows.len());
    };
    assert_eq!(record.disposition, Disposition::RoutedLimitRejected);
    assert_eq!(record.response.attempt_num, Some(0));
    assert_eq!(record.response.input_tokens, None);
    assert_eq!(record.response.cache_read_input_tokens, None);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn status_499_maps_to_client_disconnected_after_dispatch() -> TestResult<()> {
    // Given
    let event_id = "event-disconnect";
    let request_id = "request-disconnect";
    let upstream_id = Uuid::from_u128(3);

    // When
    let rows = run(
        vec![
            started(event_id, request_id),
            route(event_id, upstream_id),
            attempt(event_id, 1, upstream_id),
            response_started(event_id, 200),
            terminated(
                event_id,
                TerminationReason::ErrorCode("client_closed_request".to_owned()),
                499,
            ),
        ],
        vec![input(event_id, request_id)],
    )
    .await?;

    // Then
    let [record] = rows.as_slice() else {
        panic!("expected one capture row, got {}", rows.len());
    };
    assert_eq!(record.disposition, Disposition::RoutedClientDisconnected);
    assert_eq!(record.response.client_status, Some(499));
    assert_eq!(record.response.attempt_num, Some(1));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn retry_uses_max_attempt_and_latest_upstream_status() -> TestResult<()> {
    // Given
    let event_id = "event-retry";
    let request_id = "request-retry";
    let upstream_id = Uuid::from_u128(4);

    // When
    let rows = run(
        vec![
            started(event_id, request_id),
            route(event_id, upstream_id),
            attempt(event_id, 1, upstream_id),
            response_started(event_id, 401),
            attempt(event_id, 2, upstream_id),
            response_started(event_id, 200),
            usage_observed(event_id),
            terminated(event_id, TerminationReason::Success, 200),
        ],
        vec![input(event_id, request_id)],
    )
    .await?;

    // Then
    let [record] = rows.as_slice() else {
        panic!("expected one capture row, got {}", rows.len());
    };
    assert_eq!(record.response.attempt_num, Some(2));
    assert_eq!(record.response.upstream_status, Some(200));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn identical_request_ids_remain_separate_by_event_id() -> TestResult<()> {
    // Given
    let request_id = "client-collision";
    let first_upstream = Uuid::from_u128(5);
    let second_upstream = Uuid::from_u128(6);

    // When
    let rows = run(
        vec![
            started("event-a", request_id),
            started("event-b", request_id),
            route("event-a", first_upstream),
            route("event-b", second_upstream),
            attempt("event-a", 1, first_upstream),
            attempt("event-b", 1, second_upstream),
            response_started("event-a", 200),
            response_started("event-b", 500),
            terminated("event-a", TerminationReason::Success, 200),
            terminated(
                "event-b",
                TerminationReason::ErrorCode("upstream_5xx".to_owned()),
                500,
            ),
        ],
        vec![input("event-a", request_id), input("event-b", request_id)],
    )
    .await?;

    // Then
    let [first, second] = rows.as_slice() else {
        panic!("expected two capture rows, got {}", rows.len());
    };
    assert_eq!(first.input.event_id, "event-a");
    assert_eq!(second.input.event_id, "event-b");
    assert_eq!(first.input.request_id, second.input.request_id);
    assert_eq!(first.response.chosen_upstream_id, Some(first_upstream));
    assert_eq!(second.response.chosen_upstream_id, Some(second_upstream));
    assert_eq!(first.disposition, Disposition::RoutedDispatchedSuccess);
    assert_eq!(second.disposition, Disposition::RoutedDispatchedError);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn termination_without_attempt_maps_to_pre_dispatch_error() -> TestResult<()> {
    // Given
    let event_id = "event-no-candidates";
    let request_id = "request-no-candidates";

    // When
    let rows = run(
        vec![
            started(event_id, request_id),
            no_route(event_id),
            terminated(
                event_id,
                TerminationReason::ErrorCode("route_no_upstream_after_filter".to_owned()),
                503,
            ),
        ],
        vec![input(event_id, request_id)],
    )
    .await?;

    // Then
    let [record] = rows.as_slice() else {
        panic!("expected one capture row, got {}", rows.len());
    };
    assert_eq!(record.disposition, Disposition::RoutedPreDispatchError);
    assert_eq!(record.response.attempt_num, Some(0));
    assert_eq!(record.response.input_tokens, None);
    assert_eq!(record.response.output_tokens, None);
    Ok(())
}
