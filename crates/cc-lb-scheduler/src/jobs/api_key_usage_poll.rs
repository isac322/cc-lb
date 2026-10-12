//! Cron payload for the Anthropic API-key usage poller.
//!
//! A regular tick has no `upstream_id` and polls every due candidate. An
//! immediate poll (after create, replace, or a Re-check that turns usage
//! reporting back on) names one upstream.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::middleware::TraceparentCarrier;

/// Recurring-job name in `scheduler.recurring_jobs` and the cron kind.
pub const API_KEY_USAGE_POLL_JOB: &str = "api_key_usage_poll";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ApiKeyUsagePollCronJob {
    pub tick_unix_secs: u64,
    /// `Some` restricts the tick to one upstream (immediate poll).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub traceparent: Option<String>,
}

impl ApiKeyUsagePollCronJob {
    /// A regular tick over every due candidate.
    pub const fn new(tick_unix_secs: u64) -> Self {
        Self {
            tick_unix_secs,
            upstream_id: None,
            traceparent: None,
        }
    }

    /// An immediate poll of one upstream.
    pub const fn for_upstream(tick_unix_secs: u64, upstream_id: Uuid) -> Self {
        Self {
            tick_unix_secs,
            upstream_id: Some(upstream_id),
            traceparent: None,
        }
    }
}

/// Idempotency key of an immediate poll. `checked_at_unix_secs` is part of
/// the key so a Re-check at an unchanged spec revision is not dropped as a
/// duplicate of an earlier enqueue.
pub fn immediate_poll_idempotency_key(
    upstream_id: Uuid,
    spec_revision: u64,
    checked_at_unix_secs: u64,
) -> String {
    format!("{API_KEY_USAGE_POLL_JOB}:{upstream_id}:{spec_revision}:{checked_at_unix_secs}")
}

impl TraceparentCarrier for ApiKeyUsagePollCronJob {
    fn traceparent(&self) -> Option<&str> {
        self.traceparent.as_deref()
    }

    fn set_traceparent(&mut self, traceparent: Option<String>) {
        self.traceparent = traceparent;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn regular_tick_payload_omits_upstream() {
        let value = serde_json::to_value(ApiKeyUsagePollCronJob::new(1_800_000_000))
            .expect("serialize regular tick");
        assert_eq!(
            value,
            serde_json::json!({"tick_unix_secs": 1_800_000_000_u64})
        );
    }

    #[test]
    fn immediate_poll_key_carries_revision_and_checked_at() {
        let id = Uuid::nil();
        assert_eq!(
            immediate_poll_idempotency_key(id, 7, 1_800_000_000),
            format!("api_key_usage_poll:{id}:7:1800000000")
        );
        assert_ne!(
            immediate_poll_idempotency_key(id, 7, 1_800_000_000),
            immediate_poll_idempotency_key(id, 7, 1_800_000_001)
        );
    }
}
