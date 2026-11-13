use std::sync::Arc;

use anyhow::{Result, ensure};
use cc_lb_storage_api::{RequestEvent, RequestEventStore as _};

use crate::harness::{ConformanceBackend, stored_request_events, with_conformance_fixture};

/// Upstream stream diagnostics live only in the serde payload; every backend
/// must persist and read them back unchanged, and rows written without them
/// must read back with the fields absent.
pub async fn request_event_upstream_diagnostics_round_trip<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: cc_lb_storage_api::Storage,
{
    with_conformance_fixture(backend, |storage| async move {
        let with_diagnostics = RequestEvent {
            ts: 1_800_000_100,
            ts_ms: Some(1_800_000_100_000),
            request_id: "req_upstream_diagnostics".to_owned(),
            event_id: Some("req_upstream_diagnostics".to_owned()),
            principal_id: Some("p_diag".to_owned()),
            model: Some("claude-sonnet-4-5-20250929".to_owned()),
            status: 200,
            duration_ms: 900,
            error_code: Some("upstream_stream_error".to_owned()),
            upstream_error_type: Some("insufficient_quota".to_owned()),
            upstream_error_message: Some("quota exhausted".to_owned()),
            upstream_error_code: Some("credit_balance_exhausted".to_owned()),
            stream_message_stop_ms: Some(812),
            upstream_http_version: Some("HTTP/2.0".to_owned()),
            upstream_request_id: Some("req_011CUpstream".to_owned()),
            upstream_content_encoding: Some("gzip".to_owned()),
            upstream_content_length: Some(8_192),
            upstream_body_bytes: Some(7_777),
            upstream_body_end: Some("transport_error".to_owned()),
            upstream_body_error_cause: Some("upstream_body_error".to_owned()),
            upstream_body_error_io_kind: Some("connection_reset".to_owned()),
            upstream_body_error_h2_reason: Some("INTERNAL_ERROR".to_owned()),
            upstream_stream_warning_type: Some("upstream_response_body_error".to_owned()),
            upstream_stream_warning_message: Some(
                "upstream body ended after message_stop".to_owned(),
            ),
            ..Default::default()
        };
        let without_diagnostics = RequestEvent {
            ts: 1_800_000_101,
            ts_ms: Some(1_800_000_101_000),
            request_id: "req_upstream_diagnostics_absent".to_owned(),
            event_id: Some("req_upstream_diagnostics_absent".to_owned()),
            principal_id: Some("p_diag".to_owned()),
            status: 200,
            duration_ms: 10,
            ..Default::default()
        };
        let ws_incomplete_warning = RequestEvent {
            ts: 1_800_000_102,
            ts_ms: Some(1_800_000_102_000),
            request_id: "req_ws_incomplete_warning".to_owned(),
            event_id: Some("req_ws_incomplete_warning".to_owned()),
            principal_id: Some("p_diag".to_owned()),
            status: 101,
            error_code: None,
            upstream_error_type: None,
            upstream_error_message: None,
            upstream_error_code: None,
            upstream_body_bytes: Some(4_096),
            upstream_stream_warning_type: Some("response.incomplete".to_owned()),
            upstream_stream_warning_message: Some("max_output_tokens".to_owned()),
            input_tokens: Some(12),
            output_tokens: Some(16),
            ..Default::default()
        };

        storage.append_request_event(&with_diagnostics).await?;
        storage.append_request_event(&without_diagnostics).await?;
        storage.append_request_event(&ws_incomplete_warning).await?;

        let recent = stored_request_events(storage.as_ref()).await?;
        let got = recent
            .iter()
            .find(|e| e.request_id == "req_upstream_diagnostics")
            .ok_or_else(|| anyhow::anyhow!("diagnostics event must round-trip"))?;
        let expected = [
            (
                "upstream_http_version",
                &got.upstream_http_version,
                &with_diagnostics.upstream_http_version,
            ),
            (
                "upstream_request_id",
                &got.upstream_request_id,
                &with_diagnostics.upstream_request_id,
            ),
            (
                "upstream_content_encoding",
                &got.upstream_content_encoding,
                &with_diagnostics.upstream_content_encoding,
            ),
            (
                "upstream_body_end",
                &got.upstream_body_end,
                &with_diagnostics.upstream_body_end,
            ),
            (
                "upstream_body_error_cause",
                &got.upstream_body_error_cause,
                &with_diagnostics.upstream_body_error_cause,
            ),
            (
                "upstream_body_error_io_kind",
                &got.upstream_body_error_io_kind,
                &with_diagnostics.upstream_body_error_io_kind,
            ),
            (
                "upstream_body_error_h2_reason",
                &got.upstream_body_error_h2_reason,
                &with_diagnostics.upstream_body_error_h2_reason,
            ),
            (
                "upstream_stream_warning_type",
                &got.upstream_stream_warning_type,
                &with_diagnostics.upstream_stream_warning_type,
            ),
            (
                "upstream_stream_warning_message",
                &got.upstream_stream_warning_message,
                &with_diagnostics.upstream_stream_warning_message,
            ),
        ];
        for (name, actual, wanted) in expected {
            ensure!(
                actual == wanted,
                "{name} mismatch: {actual:?} != {wanted:?}"
            );
        }
        ensure!(
            got.upstream_content_length == Some(8_192),
            "upstream_content_length mismatch: {:?}",
            got.upstream_content_length
        );
        ensure!(
            got.upstream_body_bytes == Some(7_777),
            "upstream_body_bytes mismatch: {:?}",
            got.upstream_body_bytes
        );
        ensure!(
            got.stream_message_stop_ms == Some(812),
            "stream_message_stop_ms mismatch: {:?}",
            got.stream_message_stop_ms
        );
        ensure!(
            got.upstream_error_type.as_deref() == Some("insufficient_quota"),
            "upstream_error_type mismatch: {:?}",
            got.upstream_error_type
        );
        ensure!(
            got.upstream_error_code.as_deref() == Some("credit_balance_exhausted"),
            "upstream_error_code mismatch: {:?}",
            got.upstream_error_code
        );
        let absent = recent
            .iter()
            .find(|e| e.request_id == "req_upstream_diagnostics_absent")
            .ok_or_else(|| anyhow::anyhow!("plain event must round-trip"))?;
        ensure!(
            absent.upstream_http_version.is_none()
                && absent.upstream_request_id.is_none()
                && absent.upstream_content_encoding.is_none()
                && absent.upstream_content_length.is_none()
                && absent.upstream_body_bytes.is_none()
                && absent.upstream_body_end.is_none()
                && absent.upstream_body_error_cause.is_none()
                && absent.upstream_body_error_io_kind.is_none()
                && absent.upstream_body_error_h2_reason.is_none()
                && absent.upstream_stream_warning_type.is_none()
                && absent.upstream_stream_warning_message.is_none(),
            "event without diagnostics must read back with diagnostics absent: {absent:?}"
        );
        let warning = recent
            .iter()
            .find(|e| e.request_id == "req_ws_incomplete_warning")
            .ok_or_else(|| anyhow::anyhow!("completed WebSocket warning must round-trip"))?;
        ensure!(
            warning.status == 101
                && warning.error_code.is_none()
                && warning.upstream_error_type.is_none()
                && warning.upstream_error_message.is_none()
                && warning.upstream_error_code.is_none()
                && warning.upstream_body_bytes == Some(4_096)
                && warning.upstream_stream_warning_type.as_deref() == Some("response.incomplete")
                && warning.upstream_stream_warning_message.as_deref() == Some("max_output_tokens")
                && warning.input_tokens == Some(12)
                && warning.output_tokens == Some(16),
            "completed WebSocket warning must retain diagnostics without errors: {warning:?}"
        );

        Ok(())
    })
    .await
}
