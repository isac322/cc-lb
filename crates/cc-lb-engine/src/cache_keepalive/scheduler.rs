use std::time::Duration;

use async_trait::async_trait;

use cc_lb_storage_api::{CacheKeepaliveConfig, CacheTtl};

use crate::api_keys::limit_engine::Reservation;

use super::request_snapshot::RequestSnapshot;

#[derive(Clone, Debug)]
pub struct ScheduleParams {
    pub delay: Duration,
    pub max_refreshes: u32,
    pub max_total_duration_secs: u64,
}

impl ScheduleParams {
    pub fn from_cache_anchor_age(
        config: &CacheKeepaliveConfig,
        ttl: CacheTtl,
        cache_anchor_age: Duration,
    ) -> Self {
        let refresh_delay = Duration::from_secs(config.refresh_delay_secs(ttl));
        Self {
            delay: refresh_delay
                .checked_sub(cache_anchor_age)
                .unwrap_or(Duration::from_secs(1))
                .max(Duration::from_secs(1)),
            max_refreshes: config.max_refreshes_per_session,
            max_total_duration_secs: config.max_total_duration_secs,
        }
    }
}

#[async_trait]
pub trait KeepaliveDispatcher: Send + Sync + 'static {
    async fn dispatch(
        &self,
        snapshot: &RequestSnapshot,
        context: KeepaliveDispatchContext,
    ) -> DispatchOutcome;
}

pub struct KeepaliveDispatchContext {
    reservation: Option<Reservation>,
    source_ref_id: String,
}

impl KeepaliveDispatchContext {
    pub fn new(reservation: Option<Reservation>, source_ref_id: String) -> Self {
        Self {
            reservation,
            source_ref_id,
        }
    }

    pub fn source_ref_id(&self) -> &str {
        &self.source_ref_id
    }

    pub(crate) fn into_reservation(self) -> Option<Reservation> {
        self.reservation
    }
}

pub enum DispatchOutcome {
    CacheHit { cache_anchor_age: Duration },
    CacheMiss,
    UnsupportedProvider(String),
    Error(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schedule_params_subtract_cache_anchor_age_from_refresh_delay() {
        let config = CacheKeepaliveConfig {
            enabled: true,
            refresh_lead_time_5m_secs: 30,
            refresh_lead_time_1h_secs: 300,
            max_refreshes_per_session: 12,
            max_total_duration_secs: 14400,
            snapshot_max_bytes: 524_288,
            classifier: Default::default(),
        };

        let params = ScheduleParams::from_cache_anchor_age(
            &config,
            CacheTtl::Ttl5m,
            Duration::from_secs(120),
        );

        assert_eq!(params.delay, Duration::from_secs(150));
    }

    #[test]
    fn schedule_params_saturates_when_anchor_age_exceeds_refresh_delay() {
        let config = CacheKeepaliveConfig {
            enabled: true,
            refresh_lead_time_5m_secs: 30,
            refresh_lead_time_1h_secs: 300,
            max_refreshes_per_session: 12,
            max_total_duration_secs: 14400,
            snapshot_max_bytes: 524_288,
            classifier: Default::default(),
        };

        let params = ScheduleParams::from_cache_anchor_age(
            &config,
            CacheTtl::Ttl5m,
            Duration::from_secs(300),
        );

        assert_eq!(params.delay, Duration::from_secs(1));
    }
}
