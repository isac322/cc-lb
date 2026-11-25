use cc_lb_storage_api::WarmupLane;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// One adaptive warm-up task per (upstream, lane).
///
/// `lane` defaults to [`WarmupLane::Primary`] so payloads persisted before
/// lanes existed keep deserializing as primary-lane jobs.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct UpstreamWarmupJob {
    pub upstream_id: Uuid,
    pub cycle_key: u64,
    #[serde(default)]
    pub lane: WarmupLane,
}

impl UpstreamWarmupJob {
    pub const fn new(upstream_id: Uuid, cycle_key: u64) -> Self {
        Self::new_for_lane(upstream_id, cycle_key, WarmupLane::Primary)
    }

    pub const fn new_for_lane(upstream_id: Uuid, cycle_key: u64, lane: WarmupLane) -> Self {
        Self {
            upstream_id,
            cycle_key,
            lane,
        }
    }

    /// `adaptive:{lane job kind}:{upstream_id}:{cycle_secs}`.
    pub fn idempotency_key(&self, cycle_secs: u64) -> String {
        format!(
            "adaptive:{}:{}:{cycle_secs}",
            self.lane.job_kind(),
            self.upstream_id
        )
    }

    /// `adaptive:{lane job kind}:{upstream_id}:bootstrap:{seed_secs}`.
    pub fn bootstrap_idempotency_key(&self, seed_secs: u64) -> String {
        format!(
            "adaptive:{}:{}:bootstrap:{seed_secs}",
            self.lane.job_kind(),
            self.upstream_id
        )
    }
}

/// One-shot manual warmup execution bridged from DB-only split `cc-lb-admin`.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ManualWarmupJob {
    pub upstream_id: Uuid,
    #[serde(default)]
    pub lane: WarmupLane,
    pub holder: String,
    pub candidate_cycle_key: i64,
    pub nonce: Uuid,
    #[serde(default)]
    pub traceparent: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at_unix_secs: Option<u64>,
}

impl ManualWarmupJob {
    pub fn new(
        upstream_id: Uuid,
        lane: WarmupLane,
        holder: String,
        candidate_cycle_key: i64,
        nonce: Uuid,
    ) -> Self {
        Self {
            upstream_id,
            lane,
            holder,
            candidate_cycle_key,
            nonce,
            traceparent: None,
            expires_at_unix_secs: None,
        }
    }

    pub const fn with_expires_at(mut self, expires_at_unix_secs: u64) -> Self {
        self.expires_at_unix_secs = Some(expires_at_unix_secs);
        self
    }

    pub fn idempotency_key(&self) -> String {
        format!(
            "split_manual_warmup:{}:{}:{}",
            self.upstream_id,
            self.lane.as_str(),
            self.nonce
        )
    }
}

impl crate::middleware::TraceparentCarrier for ManualWarmupJob {
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

    const ID: &str = "11111111-2222-3333-4444-555555555555";

    fn id() -> Uuid {
        Uuid::parse_str(ID).expect("uuid")
    }

    #[test]
    fn legacy_payload_without_lane_deserializes_as_primary() {
        let job: UpstreamWarmupJob =
            serde_json::from_str(&format!(r#"{{"upstream_id":"{ID}","cycle_key":42}}"#))
                .expect("legacy payload");
        assert_eq!(job, UpstreamWarmupJob::new(id(), 42));
        assert_eq!(job.lane, WarmupLane::Primary);
    }

    #[test]
    fn lane_round_trips_through_json() {
        let job = UpstreamWarmupJob::new_for_lane(id(), 7, WarmupLane::ThirdParty);
        let value = serde_json::to_value(job).expect("serialize");
        assert_eq!(value["lane"], "third_party");
        let decoded: UpstreamWarmupJob = serde_json::from_value(value).expect("deserialize");
        assert_eq!(decoded, job);
    }

    #[test]
    fn primary_keys_are_unchanged() {
        let job = UpstreamWarmupJob::new(id(), 1);
        assert_eq!(
            job.idempotency_key(1_800),
            format!("adaptive:warmup:{ID}:1800")
        );
        assert_eq!(
            job.bootstrap_idempotency_key(9),
            format!("adaptive:warmup:{ID}:bootstrap:9")
        );
    }

    #[test]
    fn third_party_keys_use_third_party_kind_segment() {
        let job = UpstreamWarmupJob::new_for_lane(id(), 1, WarmupLane::ThirdParty);
        assert_eq!(
            job.idempotency_key(1_800),
            format!("adaptive:warmup_third_party:{ID}:1800")
        );
        assert_eq!(
            job.bootstrap_idempotency_key(9),
            format!("adaptive:warmup_third_party:{ID}:bootstrap:9")
        );
    }

    #[test]
    fn manual_warmup_job_round_trips_and_formats_key() {
        let nonce = Uuid::from_u128(0x99);
        let mut job = ManualWarmupJob::new(
            id(),
            WarmupLane::ThirdParty,
            "fire-now:test".to_owned(),
            1_700_000_000,
            nonce,
        );
        crate::middleware::TraceparentCarrier::set_traceparent(
            &mut job,
            Some("00-abc-def-01".to_owned()),
        );
        let encoded = serde_json::to_value(&job).expect("serialize");
        let decoded: ManualWarmupJob = serde_json::from_value(encoded).expect("deserialize");
        assert_eq!(decoded, job);
        assert_eq!(
            job.idempotency_key(),
            format!("split_manual_warmup:{ID}:third_party:{nonce}")
        );
    }

    #[test]
    fn manual_warmup_job_with_expires_at_round_trips() {
        let nonce = Uuid::from_u128(0x77);
        let job = ManualWarmupJob::new(
            id(),
            WarmupLane::Primary,
            "fire-now:expiry".to_owned(),
            1_700_000_000,
            nonce,
        )
        .with_expires_at(1_700_000_015);
        let encoded = serde_json::to_value(&job).expect("serialize");
        assert_eq!(encoded["expires_at_unix_secs"], 1_700_000_015);
        let decoded: ManualWarmupJob = serde_json::from_value(encoded).expect("deserialize");
        assert_eq!(decoded, job);
        assert_eq!(decoded.expires_at_unix_secs, Some(1_700_000_015));
    }
}
