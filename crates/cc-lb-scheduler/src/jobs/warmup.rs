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
}
