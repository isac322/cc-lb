use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct UpstreamWarmupJob {
    pub upstream_id: Uuid,
    pub cycle_key: u64,
}

impl UpstreamWarmupJob {
    pub const fn new(upstream_id: Uuid, cycle_key: u64) -> Self {
        Self {
            upstream_id,
            cycle_key,
        }
    }

    pub fn idempotency_key(&self, cycle_secs: u64) -> String {
        format!("adaptive:warmup:{}:{cycle_secs}", self.upstream_id)
    }
}
