use serde::{Deserialize, Serialize};

use crate::middleware::TraceparentCarrier;

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct PoolQuotaSnapshotCronJob {
    pub tick_unix_secs: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub traceparent: Option<String>,
}

impl PoolQuotaSnapshotCronJob {
    pub const fn new(tick_unix_secs: u64) -> Self {
        Self {
            tick_unix_secs,
            traceparent: None,
        }
    }
}

impl TraceparentCarrier for PoolQuotaSnapshotCronJob {
    fn traceparent(&self) -> Option<&str> {
        self.traceparent.as_deref()
    }

    fn set_traceparent(&mut self, traceparent: Option<String>) {
        self.traceparent = traceparent;
    }
}
