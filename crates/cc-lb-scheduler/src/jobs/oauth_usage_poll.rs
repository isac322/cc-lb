use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::middleware::TraceparentCarrier;

pub const NETWORK_FAILURE_STATUS: i32 = -1;

/// One-shot targeted OAuth usage refresh bridged from DB-only split `cc-lb-admin`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct OAuthUsageRefreshJob {
    pub upstream_id: Uuid,
    pub nonce: Uuid,
    #[serde(default)]
    pub traceparent: Option<String>,
}

impl OAuthUsageRefreshJob {
    pub const fn new(upstream_id: Uuid, nonce: Uuid) -> Self {
        Self {
            upstream_id,
            nonce,
            traceparent: None,
        }
    }

    pub fn idempotency_key(&self) -> String {
        format!("split_usage_refresh:{}:{}", self.upstream_id, self.nonce)
    }
}

impl TraceparentCarrier for OAuthUsageRefreshJob {
    fn traceparent(&self) -> Option<&str> {
        self.traceparent.as_deref()
    }

    fn set_traceparent(&mut self, traceparent: Option<String>) {
        self.traceparent = traceparent;
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct OAuthUsagePollCronJob {
    pub tick_unix_secs: u64,
    pub traceparent: Option<String>,
}

impl OAuthUsagePollCronJob {
    pub const fn new(tick_unix_secs: u64) -> Self {
        Self {
            tick_unix_secs,
            traceparent: None,
        }
    }
}

impl TraceparentCarrier for OAuthUsagePollCronJob {
    fn traceparent(&self) -> Option<&str> {
        self.traceparent.as_deref()
    }

    fn set_traceparent(&mut self, traceparent: Option<String>) {
        self.traceparent = traceparent;
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OAuthUsagePollObservation {
    Skip,
    Success {
        observed_at_unix_secs: u64,
        window_start_unix_millis: u64,
        window_end_unix_millis: u64,
        /// Provider usage was observed successfully, but only a supported main
        /// quota publication establishes post-reset routing freshness.
        affected_main_quota_published: bool,
    },
    Throttled {
        observed_at_unix_secs: u64,
    },
    StatusFailure {
        observed_at_unix_secs: u64,
        status: u16,
    },
    NetworkFailure {
        observed_at_unix_secs: u64,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oauth_usage_refresh_job_round_trips_and_formats_key() {
        let id = Uuid::from_u128(0x1111_2222_3333_4444_5555_6666_7777_8888);
        let nonce = Uuid::from_u128(0x42);
        let mut job = OAuthUsageRefreshJob::new(id, nonce);
        job.set_traceparent(Some("00-1234-5678-01".to_owned()));
        let value = serde_json::to_value(&job).expect("serialize");
        let decoded: OAuthUsageRefreshJob = serde_json::from_value(value).expect("deserialize");
        assert_eq!(decoded, job);
        assert_eq!(
            job.idempotency_key(),
            format!("split_usage_refresh:{id}:{nonce}")
        );
    }
}
