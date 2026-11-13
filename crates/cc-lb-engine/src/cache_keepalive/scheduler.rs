use std::time::Duration;

use async_trait::async_trait;

use cc_lb_storage_api::{CacheKeepaliveConfig, CacheTtl};

use crate::attempt_rail::ResponseAccountingGuard;
use cc_lb_control::api_keys::limit_engine::Reservation;

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
    dispatch_deadline_unix_secs: u64,
}

impl KeepaliveDispatchContext {
    pub fn new(
        reservation: Option<Reservation>,
        source_ref_id: String,
        dispatch_deadline_unix_secs: u64,
    ) -> Self {
        Self {
            reservation,
            source_ref_id,
            dispatch_deadline_unix_secs,
        }
    }

    pub fn source_ref_id(&self) -> &str {
        &self.source_ref_id
    }

    pub(crate) fn dispatch_deadline_unix_secs(&self) -> u64 {
        self.dispatch_deadline_unix_secs
    }

    pub(crate) fn into_reservation(self) -> Option<Reservation> {
        self.reservation
    }
}

pub enum DispatchOutcome {
    CacheHit {
        cache_anchor_age: Duration,
        finalization: RenewalFinalization,
    },
    CacheMiss {
        finalization: RenewalFinalization,
    },
    /// The deadline elapsed before HTTP dispatch; no completed-request usage exists.
    MaxDuration,
    UnsupportedProvider(String),
    Error(String),
}

pub struct RenewalFinalization {
    pub usage: RenewalUsage,
    pub status: u16,
    pub duration: Duration,
    pub request_start_ms: u64,
    pub accounting_guard: ResponseAccountingGuard,
}

#[derive(Clone, Debug)]
pub struct RenewalUsage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_creation_input_tokens: u64,
    pub cache_creation_input_tokens_5m: u64,
    pub cache_creation_input_tokens_1h: u64,
    pub cache_read_input_tokens: u64,
    pub thinking_tokens: u64,
    pub input_image_tokens: Option<u64>,
    pub output_image_tokens: Option<u64>,
    pub cache_write_input_tokens: Option<u64>,
    pub cache_write_input_tokens_30m: Option<u64>,
    pub cache_write_input_tokens_12h: Option<u64>,
    pub cache_write_evidence: cc_lb_request_log::RequestCacheEvidence,
    pub cache_read_evidence: cc_lb_request_log::RequestCacheEvidence,
    pub service_tier: Option<String>,
    pub usage_present: bool,
    pub is_openai_responses: bool,
    pub provider_cost_unknown: bool,
}

impl Default for RenewalUsage {
    fn default() -> Self {
        Self {
            input_tokens: 0,
            output_tokens: 0,
            cache_creation_input_tokens: 0,
            cache_creation_input_tokens_5m: 0,
            cache_creation_input_tokens_1h: 0,
            cache_read_input_tokens: 0,
            thinking_tokens: 0,
            input_image_tokens: None,
            output_image_tokens: None,
            cache_write_input_tokens: None,
            cache_write_input_tokens_30m: None,
            cache_write_input_tokens_12h: None,
            cache_write_evidence: cc_lb_request_log::RequestCacheEvidence::Unreported,
            cache_read_evidence: cc_lb_request_log::RequestCacheEvidence::Unreported,
            service_tier: None,
            usage_present: true,
            is_openai_responses: false,
            provider_cost_unknown: false,
        }
    }
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
            min_cache_tokens: 10_000,
            snapshot_max_bytes: 8_388_608,
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
            snapshot_max_bytes: 8_388_608,
            min_cache_tokens: 10_000,
            classifier: Default::default(),
        };

        let params = ScheduleParams::from_cache_anchor_age(
            &config,
            CacheTtl::Ttl5m,
            Duration::from_secs(300),
        );

        assert_eq!(params.delay, Duration::from_secs(1));
    }

    #[test]
    fn schedule_params_preserve_configured_max_refreshes_across_ttls() {
        for max_refreshes_per_session in [4_u32, 12, 20] {
            let config = CacheKeepaliveConfig {
                enabled: true,
                refresh_lead_time_5m_secs: 30,
                refresh_lead_time_1h_secs: 300,
                max_refreshes_per_session,
                max_total_duration_secs: 14_400,
                snapshot_max_bytes: 8_388_608,
                min_cache_tokens: 10_000,
                classifier: Default::default(),
            };
            for ttl in [CacheTtl::Ttl5m, CacheTtl::Ttl1h, CacheTtl::Ttl30m] {
                let params =
                    ScheduleParams::from_cache_anchor_age(&config, ttl, Duration::from_secs(5));
                assert_eq!(
                    params.max_refreshes, max_refreshes_per_session,
                    "ttl={ttl:?}"
                );
            }
        }
    }
}
