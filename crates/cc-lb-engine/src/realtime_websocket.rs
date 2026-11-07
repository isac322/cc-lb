//! Connection-pinned OpenAI Realtime and Gemini Live admission/accounting.
use bytes::Bytes;
use cc_lb_control::api_keys::limit_engine::{LimitEngine, Reservation};
use cc_lb_domain::Provider;
use cc_lb_lifecycle::{LifecycleEvent, UsageSnapshot, UsageSource};
use http::{HeaderMap, StatusCode};
use serde_json::Value;
use std::sync::Arc;
use url::Url;

use crate::responses_websocket::WebSocketCandidate;
use crate::terminal_observer::{LifecycleContext, UpstreamErrorCode};
use crate::usage_parser::{merge_usage_value, merge_usage_value_for_shape, UsageCounts};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RealtimeWebSocketKind {
    Openai,
    Gemini,
}

/// Provider endpoint details used when preparing a pinned WebSocket connection.
pub struct RealtimeWebSocketTarget<'a> {
    pub path: &'a str,
    pub query: Option<&'a str>,
    pub model: Option<&'a str>,
}

impl RealtimeWebSocketKind {
    pub const fn provider(self) -> Provider {
        match self {
            Self::Openai => Provider::Openai,
            Self::Gemini => Provider::Gemini,
        }
    }

    /// A cheap marker check avoids parsing audio and other ordinary relay frames.
    pub fn may_report_usage(self, frame: &[u8]) -> bool {
        let marker: &[u8] = match self {
            Self::Openai => b"response.done",
            Self::Gemini => b"usageMetadata",
        };
        frame.windows(marker.len()).any(|window| window == marker)
    }
}

pub struct RealtimeWebSocketObservation {
    pub(crate) observer: Option<LifecycleContext>,
    pub(crate) request_id: String,
}

impl RealtimeWebSocketObservation {
    pub fn finish(self, status: StatusCode) {
        if let Some(observer) = &self.observer {
            observer.set_success_status(status);
            observer.finish();
        }
    }
}

pub struct RealtimeWebSocketSession {
    pub candidate: WebSocketCandidate,
    pub url: Url,
    pub headers: HeaderMap,
    pub(crate) observer: Option<LifecycleContext>,
    pub(crate) reservation: Option<Reservation>,
    pub(crate) limit_engine: Option<Arc<LimitEngine>>,
    pub(crate) model: String,
}

impl RealtimeWebSocketSession {
    pub fn connected(&self) {
        if let Some(observer) = &self.observer {
            observer.emit_lifecycle(LifecycleEvent::UpstreamAttempt {
                event_id: observer.event_id().to_owned(),
                attempt_num: 1,
                upstream_id: self.candidate.upstream_id,
            });
            observer.set_success_status(StatusCode::SWITCHING_PROTOCOLS);
        }
    }

    /// Called after the usage worker drains; emits one terminal event per connection.
    pub fn finish(mut self, usage: RealtimeUsage, status: StatusCode) {
        if let Some(observer) = &self.observer {
            if let Some(counts) = usage.counts.as_ref() {
                observer.set_usage_counts(counts, status == StatusCode::SWITCHING_PROTOCOLS);
                observer.emit_lifecycle(LifecycleEvent::UsageObserved {
                    event_id: observer.event_id().to_owned(),
                    usage: UsageSnapshot {
                        input_tokens: counts.input_tokens,
                        output_tokens: counts.output_tokens,
                        input_image_tokens: counts.input_image_tokens,
                        output_image_tokens: counts.output_image_tokens,
                        cache_read_input_tokens: counts.cache_read_input_tokens,
                        ..Default::default()
                    },
                    source: UsageSource::NonStreamBody,
                });
            }
            if let Some(reservation) = self.reservation.take() {
                reservation.handoff_after_response();
            }
            if status.is_server_error() {
                observer.set_upstream_error(status, UpstreamErrorCode::StreamError);
            } else {
                observer.set_success_status(status);
            }
            observer.finish();
        } else if let (Some(engine), Some(reservation), Some(counts)) = (
            self.limit_engine.as_ref(),
            self.reservation.take(),
            usage.counts.as_ref(),
        ) {
            let cost = cc_lb_pricing::virtual_cost_micros_full_with_context_and_images(
                &self.model,
                counts.input_tokens,
                counts.output_tokens,
                counts.input_image_tokens.unwrap_or(0),
                counts.output_image_tokens.unwrap_or(0),
                0,
                0,
                0,
                counts.cache_read_input_tokens,
                None,
            )
            .unwrap_or_else(cc_lb_pricing::ComputedCostBreakdown::unknown);
            engine.reconcile(
                reservation,
                counts.input_tokens,
                counts.output_tokens,
                cost.total_micros,
            );
        }
    }

    /// Finish a failed provider handshake without treating HTTP errors as a
    /// successful relay response.
    pub fn finish_handshake(mut self, status: StatusCode) {
        if let Some(observer) = &self.observer {
            if let Some(reservation) = self.reservation.take() {
                reservation.handoff_after_response();
            }
            if status.is_client_error() {
                observer.set_upstream_error(status, UpstreamErrorCode::Upstream4xx);
            } else if status.is_server_error() {
                observer.set_upstream_error(status, UpstreamErrorCode::Upstream5xx);
            } else {
                observer.set_success_status(status);
            }
            observer.finish();
        }
    }
}

#[derive(Default)]
pub struct RealtimeUsage {
    counts: Option<UsageCounts>,
}

impl RealtimeUsage {
    /// Run on the bounded usage worker, never on the transport relay task.
    pub fn observe(&mut self, kind: RealtimeWebSocketKind, frame: &Bytes) {
        let Ok(value) = sonic_rs::from_slice::<Value>(frame) else {
            return;
        };
        let number =
            |value: &Value, name: &str| value.get(name).and_then(Value::as_u64).unwrap_or(0);
        match kind {
            RealtimeWebSocketKind::Openai => {
                if value.get("type").and_then(Value::as_str) != Some("response.done") {
                    return;
                }
                let Some(usage) = value
                    .pointer("/response/usage")
                    .filter(|value| value.is_object())
                else {
                    return;
                };
                let mut normalized = UsageCounts {
                    present: true,
                    billable_present: true,
                    ..Default::default()
                };
                merge_usage_value_for_shape(
                    &mut normalized,
                    usage,
                    Some(crate::provider_capability::UsageShape::OpenaiResponses),
                );
                let counts = self.counts.get_or_insert_with(UsageCounts::default);
                counts.present = true;
                counts.billable_present = true;
                counts.input_tokens = counts.input_tokens.saturating_add(normalized.input_tokens);
                counts.output_tokens = counts
                    .output_tokens
                    .saturating_add(normalized.output_tokens);
                counts.cache_read_input_tokens = counts
                    .cache_read_input_tokens
                    .saturating_add(normalized.cache_read_input_tokens);
                if let Some(image_tokens) = normalized.input_image_tokens {
                    counts.input_image_tokens = Some(
                        counts
                            .input_image_tokens
                            .unwrap_or_default()
                            .saturating_add(image_tokens),
                    );
                }
                if let Some(image_tokens) = normalized.output_image_tokens {
                    counts.output_image_tokens = Some(
                        counts
                            .output_image_tokens
                            .unwrap_or_default()
                            .saturating_add(image_tokens),
                    );
                }
            }
            RealtimeWebSocketKind::Gemini => {
                let Some(usage) = value.get("usageMetadata").filter(|value| value.is_object())
                else {
                    return;
                };
                let mut normalized = UsageCounts {
                    present: true,
                    billable_present: true,
                    ..Default::default()
                };
                merge_usage_value(&mut normalized, usage);
                // Live usageMetadata is cumulative for the session, unlike Realtime response.done.
                self.counts = Some(UsageCounts {
                    present: true,
                    billable_present: true,
                    input_tokens: normalized.input_tokens,
                    output_tokens: number(usage, "responseTokenCount")
                        .saturating_add(number(usage, "thoughtsTokenCount")),
                    cache_read_input_tokens: normalized.cache_read_input_tokens,
                    ..Default::default()
                });
            }
        }
    }
}

/// Extract routing metadata only from the first authenticated Gemini setup frame.
pub fn gemini_setup_model(_auth: &crate::Authenticated, frame: &[u8]) -> Option<String> {
    let value: Value = sonic_rs::from_slice(frame).ok()?;
    value
        .pointer("/setup/model")
        .and_then(Value::as_str)
        .map(|model| model.strip_prefix("models/").unwrap_or(model).to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn realtime_usage_sums_only_response_done() {
        let mut usage = RealtimeUsage::default();
        usage.observe(
            RealtimeWebSocketKind::Openai,
            &Bytes::from_static(
                br#"{"type":"response.created","response":{"usage":{"input_tokens":999}}}"#,
            ),
        );
        for _ in 0..2 {
            usage.observe(RealtimeWebSocketKind::Openai, &Bytes::from_static(br#"{"type":"response.done","response":{"usage":{"input_tokens":10,"output_tokens":3,"input_token_details":{"cached_tokens":4}}}}"#));
        }
        let counts = usage.counts.unwrap();
        assert!(counts.present);
        assert!(counts.billable_present);
        assert_eq!(
            (
                counts.input_tokens,
                counts.output_tokens,
                counts.cache_read_input_tokens
            ),
            (12, 6, 8)
        );
    }
    #[test]
    fn realtime_openai_usage_propagates_image_details_only_for_openai() {
        let mut openai = RealtimeUsage::default();
        openai.observe(
            RealtimeWebSocketKind::Openai,
            &Bytes::from_static(
                br#"{"type":"response.done","response":{"usage":{"input_tokens":100,"output_tokens":7,"input_token_details":{"image_tokens":12},"output_tokens_details":{"image_tokens":4}}}}"#,
            ),
        );
        let counts = openai.counts.unwrap();
        assert_eq!(counts.input_image_tokens, Some(12));
        assert_eq!(counts.output_image_tokens, Some(4));

        let mut gemini = RealtimeUsage::default();
        gemini.observe(
            RealtimeWebSocketKind::Gemini,
            &Bytes::from_static(
                br#"{"usageMetadata":{"promptTokenCount":100,"responseTokenCount":7,"modalityTokenCount":[{"modality":"IMAGE","tokenCount":12}]}}"#,
            ),
        );
        let counts = gemini.counts.unwrap();
        assert_eq!(counts.input_image_tokens, None);
        assert_eq!(counts.output_image_tokens, None);
    }


    #[test]
    fn realtime_usage_subtracts_cached_input_tokens() {
        let mut openai = RealtimeUsage::default();
        openai.observe(
            RealtimeWebSocketKind::Openai,
            &Bytes::from_static(
                br#"{"type":"response.done","response":{"usage":{"input_tokens":1000,"output_tokens":7,"input_token_details":{"cached_tokens":900}}}}"#,
            ),
        );
        let counts = openai.counts.unwrap();
        assert_eq!(
            (
                counts.input_tokens,
                counts.output_tokens,
                counts.cache_read_input_tokens
            ),
            (100, 7, 900)
        );

        let mut gemini = RealtimeUsage::default();
        gemini.observe(
            RealtimeWebSocketKind::Gemini,
            &Bytes::from_static(
                br#"{"usageMetadata":{"promptTokenCount":1000,"cachedContentTokenCount":900,"responseTokenCount":7}}"#,
            ),
        );
        let counts = gemini.counts.unwrap();
        assert_eq!(
            (
                counts.input_tokens,
                counts.output_tokens,
                counts.cache_read_input_tokens
            ),
            (100, 7, 900)
        );
    }

    #[test]
    fn live_usage_uses_latest_cumulative_snapshot() {
        let mut usage = RealtimeUsage::default();
        usage.observe(
            RealtimeWebSocketKind::Gemini,
            &Bytes::from_static(
                br#"{"usageMetadata":{"promptTokenCount":10,"responseTokenCount":3}}"#,
            ),
        );
        usage.observe(RealtimeWebSocketKind::Gemini, &Bytes::from_static(br#"{"usageMetadata":{"promptTokenCount":20,"responseTokenCount":7,"thoughtsTokenCount":2}}"#));
        let counts = usage.counts.unwrap();
        assert_eq!((counts.input_tokens, counts.output_tokens), (20, 9));
        assert!(
            !RealtimeWebSocketKind::Gemini
                .may_report_usage(br#"{"serverContent":{"audio":"abc"}}"#)
        );
    }

    #[test]
    fn realtime_usage_accounts_for_burst_beyond_relay_channel_capacity() {
        let mut usage = RealtimeUsage::default();
        let frame = Bytes::from_static(
            br#"{"type":"response.done","response":{"usage":{"input_tokens":1,"output_tokens":1}}}"#,
        );
        for _ in 0..64 {
            usage.observe(RealtimeWebSocketKind::Openai, &frame);
        }
        let counts = usage.counts.expect("usage should be present");
        assert_eq!((counts.input_tokens, counts.output_tokens), (64, 64));
    }
}
