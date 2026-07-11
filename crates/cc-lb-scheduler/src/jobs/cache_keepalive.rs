use cc_lb_storage_api::{CacheTtl, cache_keepalive_job_key};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::middleware::TraceparentCarrier;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CacheKeepaliveJob {
    pub session_key_hash: String,
    pub generation: u64,
    pub principal_id: String,
    pub upstream_id: Uuid,
    pub ttl: CacheTtl,
    pub cache_anchor_at_unix_secs: u64,
    pub expires_at_unix_secs: u64,
    pub refresh_delay_secs: u64,
    pub max_refreshes: u32,
    pub max_total_duration_secs: u64,
    pub traceparent: Option<String>,
}

impl CacheKeepaliveJob {
    pub fn idempotency_key(&self) -> String {
        cache_keepalive_job_key(&self.session_key_hash, self.generation)
    }
}

impl TraceparentCarrier for CacheKeepaliveJob {
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

    fn job() -> CacheKeepaliveJob {
        CacheKeepaliveJob {
            session_key_hash: "session-hash".to_owned(),
            generation: 7,
            principal_id: "principal".to_owned(),
            upstream_id: Uuid::from_u128(42),
            ttl: CacheTtl::Ttl5m,
            cache_anchor_at_unix_secs: 1_800_000_000,
            expires_at_unix_secs: 1_800_000_300,
            refresh_delay_secs: 270,
            max_refreshes: 12,
            max_total_duration_secs: 14_400,
            traceparent: None,
        }
    }

    #[test]
    fn idempotency_key_is_generation_scoped() {
        assert_eq!(job().idempotency_key(), "cache_keepalive:session-hash:7");
    }

    #[test]
    fn serialized_payload_contains_only_lightweight_references() {
        let json = serde_json::to_string(&job()).expect("serialize cache keepalive job");

        assert!(json.contains("session-hash"));
        assert!(json.contains("principal"));
        assert!(!json.contains("prompt"));
        assert!(!json.contains("authorization"));
        assert!(!json.contains("x-api-key"));
    }
}
