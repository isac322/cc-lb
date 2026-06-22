use serde::{Deserialize, Serialize};

use crate::middleware::TraceparentCarrier;

pub const NETWORK_FAILURE_STATUS: i32 = -1;

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
