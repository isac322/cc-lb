//! Per-request diagnostic fields shipped as structured stdout NDJSON.
//!
//! The five diagnostic fields of [`RequestEvent`] (`cache_breakpoints`,
//! `cache_control_message_indices`, `routing_trace`, `iterations`,
//! `internal_errors`) are emitted as one `cc_lb.request_diagnostics.v1` line
//! per persisted row. Live emission and the backfill CLI share
//! [`render_request_diagnostics_line`], so both produce byte-identical lines
//! for the same row except for the `source` value.

use chrono::{DateTime, SecondsFormat, Utc};
use serde::Serialize;
use serde_json::{Map, Value as JsonValue};
use uuid::Uuid;

use crate::{RequestCacheBreakpoint, RequestEvent, RequestEventKind};

/// Value of the leading `schema` key; log shippers route on the line prefix.
pub const REQUEST_DIAGNOSTICS_SCHEMA: &str = "cc_lb.request_diagnostics.v1";

/// Payload keys removed by strip-on-write and by the strip CLI.
pub const REQUEST_DIAGNOSTIC_PAYLOAD_KEYS: [&str; 5] = [
    "cache_breakpoints",
    "cache_control_message_indices",
    "routing_trace",
    "iterations",
    "internal_errors",
];

/// The five diagnostic fields of a [`RequestEvent`], owned.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RequestDiagnostics {
    pub cache_breakpoints: Vec<RequestCacheBreakpoint>,
    pub cache_control_message_indices: Vec<u64>,
    pub routing_trace: Option<cc_lb_domain::RoutingTrace>,
    pub iterations: Option<JsonValue>,
    pub internal_errors: Vec<cc_lb_domain::InternalError>,
}

impl RequestDiagnostics {
    /// Moves the five fields out of `event`, leaving their empty defaults so
    /// the row serializes without them.
    pub fn take_from(event: &mut RequestEvent) -> Self {
        Self {
            cache_breakpoints: std::mem::take(&mut event.cache_breakpoints),
            cache_control_message_indices: std::mem::take(&mut event.cache_control_message_indices),
            routing_trace: event.routing_trace.take(),
            iterations: event.iterations.take(),
            internal_errors: std::mem::take(&mut event.internal_errors),
        }
    }

    /// Copies the five fields out of `event`, leaving the row untouched.
    pub fn cloned_from(event: &RequestEvent) -> Self {
        Self {
            cache_breakpoints: event.cache_breakpoints.clone(),
            cache_control_message_indices: event.cache_control_message_indices.clone(),
            routing_trace: event.routing_trace.clone(),
            iterations: event.iterations.clone(),
            internal_errors: event.internal_errors.clone(),
        }
    }
}

/// Which emitter produced a diagnostics line.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestDiagnosticsSource {
    /// Written by the request-event assembler right after the row insert.
    Live,
    /// Re-exported from stored rows by the operator CLI.
    Backfill,
}

/// One `cc_lb.request_diagnostics.v1` line.
///
/// Field declaration order is the wire key order. Every key is always
/// serialized (no `skip_serializing_if`), so consumers can rely on the full
/// key set being present.
#[derive(Debug, Clone, Serialize)]
pub struct RequestDiagnosticsRecord {
    pub schema: &'static str,
    pub source: RequestDiagnosticsSource,
    pub event_id: String,
    pub seq: u64,
    pub ts: String,
    pub ts_ms: u64,
    pub request_id: String,
    pub principal_id: Option<String>,
    pub key_id: Option<String>,
    pub upstream_id: Option<Uuid>,
    pub upstream_name: Option<String>,
    pub model: Option<String>,
    pub status: u16,
    pub error_code: Option<String>,
    pub event_kind: Option<RequestEventKind>,
    pub source_kind: Option<String>,
    pub cache_breakpoints: Vec<RequestCacheBreakpoint>,
    pub cache_control_message_indices: Vec<u64>,
    pub routing_trace: Option<cc_lb_domain::RoutingTrace>,
    pub iterations: Option<JsonValue>,
    pub internal_errors: Vec<cc_lb_domain::InternalError>,
}

impl RequestDiagnosticsRecord {
    /// Builds the record for a persisted row.
    ///
    /// `seq` is the storage cursor returned by the insert (Postgres `seq`,
    /// SQLite `id`). The five diagnostic fields come from `diagnostics`, not
    /// from `event`, so callers that stripped the row before persisting still
    /// emit them. `ts_ms` falls back to `ts * 1000` for legacy rows without
    /// millisecond precision.
    pub fn new(
        source: RequestDiagnosticsSource,
        seq: u64,
        event_id: String,
        event: &RequestEvent,
        diagnostics: RequestDiagnostics,
    ) -> Self {
        let ts_ms = event.ts_ms.unwrap_or_else(|| event.ts.saturating_mul(1000));
        Self {
            schema: REQUEST_DIAGNOSTICS_SCHEMA,
            source,
            event_id,
            seq,
            ts: format_ts_ms(ts_ms),
            ts_ms,
            request_id: event.request_id.clone(),
            principal_id: event.principal_id.clone(),
            key_id: event.key_id.clone(),
            upstream_id: event.upstream_id,
            upstream_name: event.upstream_name.clone(),
            model: event.model.clone(),
            status: event.status,
            error_code: event.error_code.clone(),
            event_kind: event.event_kind,
            source_kind: event.source_kind.clone(),
            cache_breakpoints: diagnostics.cache_breakpoints,
            cache_control_message_indices: diagnostics.cache_control_message_indices,
            routing_trace: diagnostics.routing_trace,
            iterations: diagnostics.iterations,
            internal_errors: diagnostics.internal_errors,
        }
    }
}

/// RFC3339 UTC with millisecond precision (`2026-10-13T05:19:00.123Z`).
///
/// Millisecond values beyond chrono's representable range (far past year
/// 262143) cannot come from a real clock; they render as the Unix epoch
/// instead of panicking, while `ts_ms` keeps the raw value.
fn format_ts_ms(ts_ms: u64) -> String {
    i64::try_from(ts_ms)
        .ok()
        .and_then(DateTime::<Utc>::from_timestamp_millis)
        .unwrap_or_default()
        .to_rfc3339_opts(SecondsFormat::Millis, true)
}

/// The single renderer for live and backfill: `serde_json::to_vec(record)`
/// followed by `\n`. No redaction pass runs over the rendered line; producers
/// already redact the values, and line-level patterns would corrupt compact
/// JSON.
pub fn render_request_diagnostics_line(
    record: &RequestDiagnosticsRecord,
) -> Result<Vec<u8>, serde_json::Error> {
    let mut line = serde_json::to_vec(record)?;
    line.push(b'\n');
    Ok(line)
}

/// Strip-CLI payload transform.
///
/// Parses the raw stored payload as a JSON object (not [`RequestEvent`], so
/// unknown or legacy keys and float values are preserved). Returns `Ok(None)`
/// when none of [`REQUEST_DIAGNOSTIC_PAYLOAD_KEYS`] is present, meaning no
/// write is needed. Otherwise removes those keys and returns the re-encoded
/// object. Without serde_json's `preserve_order` the output keys are sorted,
/// which is acceptable for cold rows. A payload that is not a JSON object is
/// an error.
pub fn strip_payload_diagnostics(payload: &[u8]) -> Result<Option<Vec<u8>>, serde_json::Error> {
    let mut map: Map<String, JsonValue> = serde_json::from_slice(payload)?;
    let mut removed = false;
    for key in REQUEST_DIAGNOSTIC_PAYLOAD_KEYS {
        removed |= map.remove(key).is_some();
    }
    if !removed {
        return Ok(None);
    }
    serde_json::to_vec(&map).map(Some)
}

#[cfg(test)]
mod tests {
    use cc_lb_domain::{InternalError, InternalErrorKind, InternalErrorStage, RoutingTrace};
    use serde_json::json;

    use super::*;
    use crate::{RequestCacheBreakpointSource, RequestCacheLookbackPrefix};

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    const PREFIX: &str = "{\"schema\":\"cc_lb.request_diagnostics.v1\",";

    fn sample_breakpoint() -> RequestCacheBreakpoint {
        RequestCacheBreakpoint {
            block_index: 3,
            source: RequestCacheBreakpointSource::Message,
            path: "messages[1].content[0]".to_owned(),
            message_index: Some(1),
            ttl: Some("5m".to_owned()),
            prefix_hash: "abc".to_owned(),
            prefix_token_count: 120,
            lookback_prefixes: vec![RequestCacheLookbackPrefix {
                prefix_hash: "def".to_owned(),
                content_block_index: 2,
                lookback_distance: 1,
            }],
            token_estimate_source: Some("bytes".to_owned()),
        }
    }

    fn sample_diagnostics() -> RequestDiagnostics {
        RequestDiagnostics {
            cache_breakpoints: vec![sample_breakpoint()],
            cache_control_message_indices: vec![1, 4],
            routing_trace: None,
            iterations: Some(json!([{ "input_tokens": 5 }])),
            internal_errors: vec![InternalError {
                stage: InternalErrorStage::Router,
                kind: InternalErrorKind::Timeout,
                message: Some("slow".to_owned()),
            }],
        }
    }

    fn sample_event() -> RequestEvent {
        RequestEvent {
            ts: 1_791_868_740,
            ts_ms: Some(1_791_868_740_123),
            request_id: "req-1".to_owned(),
            principal_id: Some("principal-1".to_owned()),
            key_id: Some("key-1".to_owned()),
            upstream_id: Some(Uuid::from_u128(0x0192_0000_0000_7000_8000_0000_0000_0001)),
            upstream_name: Some("primary".to_owned()),
            model: Some("claude-sonnet-4-5".to_owned()),
            status: 200,
            error_code: Some("upstream_timeout".to_owned()),
            event_kind: Some(RequestEventKind::Messages),
            source_kind: Some("proxy".to_owned()),
            event_id: Some("evt-1".to_owned()),
            ..RequestEvent::default()
        }
    }

    fn render(record: &RequestDiagnosticsRecord) -> Result<String, Box<dyn std::error::Error>> {
        Ok(String::from_utf8(render_request_diagnostics_line(record)?)?)
    }

    #[test]
    fn populated_line_has_exact_key_order_and_values() -> TestResult {
        let record = RequestDiagnosticsRecord::new(
            RequestDiagnosticsSource::Live,
            42,
            "evt-1".to_owned(),
            &sample_event(),
            sample_diagnostics(),
        );
        let expected = concat!(
            "{\"schema\":\"cc_lb.request_diagnostics.v1\",\"source\":\"live\",",
            "\"event_id\":\"evt-1\",\"seq\":42,\"ts\":\"2026-10-13T05:19:00.123Z\",",
            "\"ts_ms\":1791868740123,\"request_id\":\"req-1\",",
            "\"principal_id\":\"principal-1\",\"key_id\":\"key-1\",",
            "\"upstream_id\":\"01920000-0000-7000-8000-000000000001\",",
            "\"upstream_name\":\"primary\",\"model\":\"claude-sonnet-4-5\",\"status\":200,",
            "\"error_code\":\"upstream_timeout\",\"event_kind\":\"messages\",",
            "\"source_kind\":\"proxy\",",
            "\"cache_breakpoints\":[{\"block_index\":3,\"source\":\"message\",",
            "\"path\":\"messages[1].content[0]\",\"message_index\":1,\"ttl\":\"5m\",",
            "\"prefix_hash\":\"abc\",\"prefix_token_count\":120,",
            "\"lookback_prefixes\":[{\"prefix_hash\":\"def\",\"content_block_index\":2,",
            "\"lookback_distance\":1}],\"token_estimate_source\":\"bytes\"}],",
            "\"cache_control_message_indices\":[1,4],\"routing_trace\":null,",
            "\"iterations\":[{\"input_tokens\":5}],",
            "\"internal_errors\":[{\"stage\":\"router\",\"kind\":\"timeout\",\"message\":\"slow\"}]}\n",
        );
        let line = render(&record)?;
        assert!(line.starts_with(PREFIX));
        assert_eq!(line, expected);
        Ok(())
    }

    #[test]
    fn empty_row_still_renders_every_key() -> TestResult {
        let record = RequestDiagnosticsRecord::new(
            RequestDiagnosticsSource::Backfill,
            7,
            "evt-0".to_owned(),
            &RequestEvent::default(),
            RequestDiagnostics::default(),
        );
        let expected = concat!(
            "{\"schema\":\"cc_lb.request_diagnostics.v1\",\"source\":\"backfill\",",
            "\"event_id\":\"evt-0\",\"seq\":7,\"ts\":\"1970-01-01T00:00:00.000Z\",\"ts_ms\":0,",
            "\"request_id\":\"\",\"principal_id\":null,\"key_id\":null,\"upstream_id\":null,",
            "\"upstream_name\":null,\"model\":null,\"status\":0,\"error_code\":null,",
            "\"event_kind\":null,\"source_kind\":null,\"cache_breakpoints\":[],",
            "\"cache_control_message_indices\":[],\"routing_trace\":null,",
            "\"iterations\":null,\"internal_errors\":[]}\n",
        );
        let line = render(&record)?;
        assert_eq!(line, expected);
        let parsed: Map<String, JsonValue> = serde_json::from_str(&line)?;
        assert_eq!(parsed.len(), 21);
        Ok(())
    }

    #[test]
    fn ts_falls_back_to_seconds_when_ts_ms_is_missing() -> TestResult {
        let event = RequestEvent {
            ts: 1_791_868_740,
            ts_ms: None,
            ..RequestEvent::default()
        };
        let record = RequestDiagnosticsRecord::new(
            RequestDiagnosticsSource::Backfill,
            1,
            "evt-legacy".to_owned(),
            &event,
            RequestDiagnostics::default(),
        );
        assert_eq!(record.ts_ms, 1_791_868_740_000);
        assert_eq!(record.ts, "2026-10-13T05:19:00.000Z");

        // An explicit zero (late-orphan rows) is kept as-is, not replaced by `ts`.
        let orphan = RequestEvent {
            ts: 1_791_868_740,
            ts_ms: Some(0),
            ..RequestEvent::default()
        };
        let record = RequestDiagnosticsRecord::new(
            RequestDiagnosticsSource::Live,
            2,
            "evt-orphan".to_owned(),
            &orphan,
            RequestDiagnostics::default(),
        );
        assert_eq!(record.ts_ms, 0);
        assert_eq!(record.ts, "1970-01-01T00:00:00.000Z");
        let line: JsonValue = serde_json::from_str(&render(&record)?)?;
        assert_eq!(line["ts_ms"], json!(0));
        Ok(())
    }

    #[test]
    fn ts_ms_beyond_chrono_range_does_not_panic() {
        assert_eq!(format_ts_ms(u64::MAX), "1970-01-01T00:00:00.000Z");
    }

    #[test]
    fn record_uses_supplied_diagnostics_not_the_row_fields() -> TestResult {
        let mut event = sample_event();
        let routing_trace = RoutingTrace::default();
        event.cache_breakpoints = vec![sample_breakpoint()];
        event.cache_control_message_indices = vec![9];
        event.routing_trace = Some(routing_trace.clone());
        event.iterations = Some(json!({ "n": 2 }));
        event.internal_errors = vec![InternalError::default()];

        let diagnostics = RequestDiagnostics::take_from(&mut event);
        let record = RequestDiagnosticsRecord::new(
            RequestDiagnosticsSource::Live,
            3,
            "evt-1".to_owned(),
            &event,
            diagnostics,
        );
        let line: JsonValue = serde_json::from_str(&render(&record)?)?;
        assert_eq!(line["cache_control_message_indices"], json!([9]));
        assert_eq!(line["routing_trace"], serde_json::to_value(&routing_trace)?);
        assert_eq!(line["iterations"], json!({ "n": 2 }));
        assert_eq!(
            line["cache_breakpoints"][0]["lookback_prefixes"][0]["prefix_hash"],
            json!("def")
        );
        assert_eq!(line["internal_errors"].as_array().map(Vec::len), Some(1));
        Ok(())
    }

    #[test]
    fn take_from_removes_the_five_keys_from_row_serialization() -> TestResult {
        let mut event = sample_event();
        let expected = sample_diagnostics();
        event.cache_breakpoints = expected.cache_breakpoints.clone();
        event.cache_control_message_indices = expected.cache_control_message_indices.clone();
        event.routing_trace = Some(RoutingTrace::default());
        event.iterations = expected.iterations.clone();
        event.internal_errors = expected.internal_errors.clone();

        let cloned = RequestDiagnostics::cloned_from(&event);
        let before: Map<String, JsonValue> = serde_json::from_value(serde_json::to_value(&event)?)?;
        for key in REQUEST_DIAGNOSTIC_PAYLOAD_KEYS {
            assert!(
                before.contains_key(key),
                "row should carry {key} before take"
            );
        }

        let taken = RequestDiagnostics::take_from(&mut event);
        assert_eq!(taken, cloned);
        assert_eq!(taken.cache_breakpoints, expected.cache_breakpoints);
        assert_eq!(taken.routing_trace, Some(RoutingTrace::default()));

        let after: Map<String, JsonValue> = serde_json::from_value(serde_json::to_value(&event)?)?;
        for key in REQUEST_DIAGNOSTIC_PAYLOAD_KEYS {
            assert!(!after.contains_key(key), "row still serializes {key}");
        }
        assert_eq!(after.get("request_id"), Some(&json!("req-1")));
        assert_eq!(after.get("event_id"), Some(&json!("evt-1")));
        Ok(())
    }

    #[test]
    fn producer_redacted_message_renders_as_valid_json() -> TestResult {
        let diagnostics = RequestDiagnostics {
            internal_errors: vec![InternalError {
                stage: InternalErrorStage::Relay,
                kind: InternalErrorKind::PluginError,
                message: Some("token=[REDACTED] tail".to_owned()),
            }],
            ..RequestDiagnostics::default()
        };
        let record = RequestDiagnosticsRecord::new(
            RequestDiagnosticsSource::Live,
            5,
            "evt-1".to_owned(),
            &sample_event(),
            diagnostics,
        );
        let bytes = render_request_diagnostics_line(&record)?;
        assert_eq!(bytes.last(), Some(&b'\n'));
        let parsed: Map<String, JsonValue> = serde_json::from_slice(&bytes)?;
        assert_eq!(parsed.len(), 21);
        assert_eq!(
            parsed
                .get("internal_errors")
                .and_then(|errors| errors.get(0))
                .and_then(|error| error.get("message")),
            Some(&json!("token=[REDACTED] tail"))
        );
        assert_eq!(parsed.get("event_id"), Some(&json!("evt-1")));
        Ok(())
    }

    #[test]
    fn strip_is_noop_without_diagnostic_keys() -> TestResult {
        let payload = br#"{"request_id":"req-1","status":200,"legacy_x":1}"#;
        assert_eq!(strip_payload_diagnostics(payload)?, None);
        Ok(())
    }

    #[test]
    fn strip_removes_keys_and_is_idempotent() -> TestResult {
        let mut event = sample_event();
        let diagnostics = sample_diagnostics();
        event.cache_breakpoints = diagnostics.cache_breakpoints;
        event.cache_control_message_indices = diagnostics.cache_control_message_indices;
        event.routing_trace = Some(RoutingTrace::default());
        event.iterations = diagnostics.iterations;
        event.internal_errors = diagnostics.internal_errors;
        let payload = serde_json::to_vec(&event)?;

        let stripped = strip_payload_diagnostics(&payload)?.ok_or("expected a rewrite")?;
        let map: Map<String, JsonValue> = serde_json::from_slice(&stripped)?;
        for key in REQUEST_DIAGNOSTIC_PAYLOAD_KEYS {
            assert!(!map.contains_key(key), "{key} survived strip");
        }
        // Nested lookback prefixes leave with their parent breakpoint list.
        assert!(!String::from_utf8(stripped.clone())?.contains("lookback_prefixes"));

        // The stripped payload still decodes as a row with the other fields intact.
        let decoded: RequestEvent = serde_json::from_slice(&stripped)?;
        assert_eq!(decoded.request_id, "req-1");
        assert_eq!(decoded.event_id.as_deref(), Some("evt-1"));
        assert!(decoded.cache_breakpoints.is_empty());

        assert_eq!(strip_payload_diagnostics(&stripped)?, None);
        Ok(())
    }

    #[test]
    fn strip_preserves_unknown_keys_and_float_bits() -> TestResult {
        let float = 0.1_f64 + 0.2_f64;
        let payload = serde_json::to_vec(&json!({
            "request_id": "req-1",
            "legacy_x": 1,
            "quota_urgency_5h": float,
            "routing_trace": { "stages": [] },
        }))?;
        let stripped = strip_payload_diagnostics(&payload)?.ok_or("expected a rewrite")?;
        let map: Map<String, JsonValue> = serde_json::from_slice(&stripped)?;
        assert_eq!(map.len(), 3);
        assert_eq!(map.get("legacy_x"), Some(&json!(1)));
        assert_eq!(map.get("request_id"), Some(&json!("req-1")));
        let restored = map
            .get("quota_urgency_5h")
            .and_then(JsonValue::as_f64)
            .ok_or("float missing")?;
        assert_eq!(restored.to_bits(), float.to_bits());
        Ok(())
    }

    #[test]
    fn strip_rejects_non_object_payloads() {
        assert!(strip_payload_diagnostics(b"[1,2,3]").is_err());
        assert!(strip_payload_diagnostics(b"\"cache_breakpoints\"").is_err());
        assert!(strip_payload_diagnostics(b"not json").is_err());
    }
}
