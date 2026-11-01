//! #988 keepalive failure observability scenarios.

use std::time::Duration;

use axum::http::StatusCode;
use serde_json::Value;

use super::keepalive_support::harness::{
    Backend, ClockMode, DueJob, Harness, HarnessError, HarnessOptions, KeepaliveOptions,
    request_body,
};
use super::keepalive_support::mock_upstream::MockAnthropic;

const SESSION_ID: &str = "00000000-0000-7000-8000-000000000901";
const ERROR_BODY: &str = r#"{"type":"error","error":{"type":"invalid_request_error","message":"injected renewal failure"}}"#;
const ERROR_BODY_PREFIX: &str =
    r#"{"type":"error","error":{"type":"invalid_request_error","message":"injected"#;
#[derive(Clone, Copy, Debug)]
enum FailureCase {
    Http { status: StatusCode },
    Transport,
    Timeout,
    UpstreamGone,
}

impl FailureCase {
    fn expected_error(self) -> &'static str {
        match self {
            Self::Http { .. } => ERROR_BODY_PREFIX,
            Self::Transport => "upstream dispatch failed",
            Self::Timeout => "deadline has elapsed",
            Self::UpstreamGone => "upstream gone",
        }
    }

    fn expected_status(self) -> Option<u16> {
        match self {
            Self::Http { status } => Some(status.as_u16()),
            Self::Transport | Self::Timeout | Self::UpstreamGone => None,
        }
    }
}

fn assert_mock_attempt(harness: &Harness, case: FailureCase) {
    let attempts = harness
        .mock_events()
        .into_iter()
        .filter(|event| event.kind == "renewal")
        .collect::<Vec<_>>();
    match case {
        FailureCase::Http { status } => {
            let attempt = attempts
                .last()
                .unwrap_or_else(|| panic!("injected HTTP failure did not reach mock"));
            assert_eq!(attempt.status, status.as_u16());
            assert!(
                attempt
                    .rejection
                    .as_deref()
                    .is_none_or(|rejection| rejection.contains("injected")),
                "unexpected injected rejection: {:?}",
                attempt.rejection
            );
        }
        FailureCase::Timeout => {
            let attempt = attempts
                .last()
                .unwrap_or_else(|| panic!("timeout request did not reach mock"));
            assert_eq!(attempt.status, 0);
            assert_eq!(attempt.rejection.as_deref(), Some("hang"));
        }
        FailureCase::Transport | FailureCase::UpstreamGone => {
            assert!(
                attempts.is_empty(),
                "unexpected renewal attempts reached mock: {attempts:?}"
            );
        }
    }
}

async fn run_sqlite_failure_case(case: FailureCase) -> (Harness, String) {
    let harness = Harness::new(8_388_608).await;
    let session_key_hash = harness.session_key_hash(SESSION_ID, None);
    let live = harness
        .post(
            request_body(
                64_000,
                "failure-observability",
                100_000,
                "bash",
                "tool_use",
                false,
            ),
            SESSION_ID,
            None,
        )
        .await;
    assert_eq!(live["stop_reason"], "tool_use");
    harness
        .wait_for_enqueued_generation(&session_key_hash, 1)
        .await;

    match case {
        FailureCase::Http { status } => {
            harness
                .mock
                .fail_next_renewals(1, status, ERROR_BODY.to_owned());
        }
        FailureCase::Transport => {
            harness
                .set_upstream_base_url(MockAnthropic::closed_port_url())
                .await;
        }
        FailureCase::Timeout => harness.mock.hang_next_renewals(1),
        FailureCase::UpstreamGone => harness.delete_upstream().await,
    }

    if matches!(case, FailureCase::Timeout) {
        tokio::time::timeout(
            Duration::from_secs(20),
            harness.advance_and_dispatch_due(
                Duration::from_secs(270),
                &[DueJob {
                    session_key_hash: &session_key_hash,
                    generation: 1,
                    mock_prefix: None,
                }],
            ),
        )
        .await
        .unwrap_or_else(|_| panic!("timeout failure case exceeded bounded wait"));
    } else {
        harness
            .advance_and_dispatch_due(
                Duration::from_secs(270),
                &[DueJob {
                    session_key_hash: &session_key_hash,
                    generation: 1,
                    mock_prefix: None,
                }],
            )
            .await;
    }

    assert_failed_session(&harness, &session_key_hash, case).await;
    (harness, session_key_hash)
}

async fn assert_failed_session(harness: &Harness, session_key_hash: &str, case: FailureCase) {
    let row = harness
        .wait_for_session_terminal(session_key_hash, Duration::from_secs(20))
        .await
        .unwrap_or_else(|| panic!("failed session did not terminalize for {case:?}"));
    let error = row.error.as_deref().unwrap_or_default();
    assert!(!error.is_empty(), "failure error was not recorded: {row:?}");
    if let Some(status) = case.expected_status() {
        assert!(error.contains(&format!("status={status}")), "error={error}");
    }
    let expected = case.expected_error();
    if !expected.is_empty() {
        assert!(
            error
                .to_ascii_lowercase()
                .contains(&expected.to_ascii_lowercase()),
            "error={error}"
        );
    }
    assert_eq!(row.status, "terminal");
    assert_eq!(row.terminal_reason.as_deref(), Some("dispatch_error"));
    assert_ne!(row.enqueue_state, "running");
    assert_eq!(harness.count_renewal_rows().await, (0, 0));
    assert_mock_attempt(harness, case);
}

#[tokio::test]
async fn c1_http_400_invalid_request_terminalizes_without_renewal_rows() {
    let (_harness, _session_key_hash) = run_sqlite_failure_case(FailureCase::Http {
        status: StatusCode::BAD_REQUEST,
    })
    .await;
}

#[tokio::test]
async fn c2_http_500_terminalizes_without_renewal_rows() {
    let (_harness, _session_key_hash) = run_sqlite_failure_case(FailureCase::Http {
        status: StatusCode::INTERNAL_SERVER_ERROR,
    })
    .await;
}

#[tokio::test]
async fn c3_transport_error_terminalizes_without_renewal_rows() {
    let (_harness, _session_key_hash) = run_sqlite_failure_case(FailureCase::Transport).await;
}

#[tokio::test]
async fn c4_timeout_terminalizes_without_renewal_rows() {
    let (_harness, _session_key_hash) = run_sqlite_failure_case(FailureCase::Timeout).await;
}

#[tokio::test]
async fn c5_upstream_gone_terminalizes_without_renewal_rows() {
    let (_harness, _session_key_hash) = run_sqlite_failure_case(FailureCase::UpstreamGone).await;
}

#[tokio::test]
async fn c7_admin_list_and_detail_expose_c1_error() {
    let (harness, session_key_hash) = run_sqlite_failure_case(FailureCase::Http {
        status: StatusCode::BAD_REQUEST,
    })
    .await;

    let list_response = harness.admin_cache_keepalive().await;
    assert_eq!(list_response.status(), StatusCode::OK);
    let list: Value = list_response
        .json()
        .await
        .unwrap_or_else(|error| panic!("admin list JSON: {error}"));
    let rows = list["rows"]
        .as_array()
        .unwrap_or_else(|| panic!("admin list rows missing: {list}"));
    let row = rows
        .iter()
        .find(|row| row["session_key_hash"] == session_key_hash)
        .unwrap_or_else(|| panic!("failed session missing from admin list: {list}"));
    let list_error = row["error"].as_str().unwrap_or_default();
    assert!(list_error.contains("status=400"), "list row={row}");
    let detail_response = harness
        .admin_cache_keepalive_detail(&session_key_hash)
        .await;
    assert_eq!(detail_response.status(), StatusCode::OK);
    let detail: Value = detail_response
        .json()
        .await
        .unwrap_or_else(|error| panic!("admin detail JSON: {error}"));
    let detail_error = detail["error"].as_str().unwrap_or_default();
    assert!(detail_error.contains("status=400"), "detail={detail}");
}

#[tokio::test]
async fn c6_postgres_http_400_invalid_request_terminalizes_when_available() {
    let options = HarnessOptions {
        backend: Backend::Postgres,
        clock: ClockMode::System,
        keepalive: KeepaliveOptions {
            refresh_lead_time_5m_secs: 299,
            ..KeepaliveOptions::default()
        },
    };
    let Some(harness) = (match Harness::optional(options).await {
        Ok(harness) => harness,
        #[cfg(not(feature = "postgres"))]
        Err(HarnessError::PostgresFeatureDisabled) => return,
        #[cfg(feature = "postgres")]
        Err(HarnessError::PostgresUrlUnset) => return,
        Err(error) => panic!("PostgreSQL harness setup failed: {error}"),
    }) else {
        return;
    };

    let session_key_hash = harness.session_key_hash(SESSION_ID, None);
    harness
        .mock
        .fail_next_renewals(1, StatusCode::BAD_REQUEST, ERROR_BODY.to_owned());
    let live = harness
        .post(
            request_body(
                64_000,
                "postgres-failure-observability",
                100_000,
                "bash",
                "tool_use",
                false,
            ),
            SESSION_ID,
            None,
        )
        .await;
    assert_eq!(live["stop_reason"], "tool_use");

    let terminal = harness
        .wait_for_session_terminal(&session_key_hash, Duration::from_secs(20))
        .await
        .unwrap_or_else(|| panic!("PostgreSQL failure did not terminalize"));
    assert_eq!(terminal.terminal_reason.as_deref(), Some("dispatch_error"));
    assert_ne!(terminal.enqueue_state, "running");
    let error = terminal.error.as_deref().unwrap_or_default();
    assert!(error.contains("status=400"), "error={error}");
    assert!(error.contains(ERROR_BODY_PREFIX), "error={error}");
    assert_eq!(harness.count_renewal_rows().await, (0, 0));
    assert_mock_attempt(
        &harness,
        FailureCase::Http {
            status: StatusCode::BAD_REQUEST,
        },
    );
}
