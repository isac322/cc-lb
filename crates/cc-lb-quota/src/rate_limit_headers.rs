use cc_lb_storage_api::{SubscriptionQuotaStatus, SubscriptionQuotaWindow};

mod standard;
mod unified;

pub use standard::parse_anthropic_rate_limit_headers;
pub use unified::parse_anthropic_unified_headers;

#[derive(Clone, Debug, PartialEq)]
pub struct UnifiedQuotaObservation {
    pub window: SubscriptionQuotaWindow,
    pub utilization: Option<f64>,
    pub status: Option<SubscriptionQuotaStatus>,
    pub resets_at_unix_secs: Option<u64>,
    pub surpassed_threshold: Option<f64>,
    pub representative_claim: Option<String>,
    pub fallback_percentage: Option<f64>,
    pub fallback_available: Option<bool>,
    pub overage_in_use: Option<bool>,
    pub overage_period_monthly_utilization: Option<f64>,
    pub upgrade_paths: Option<Vec<String>>,
    pub disabled_reason: Option<String>,
}

impl Default for UnifiedQuotaObservation {
    fn default() -> Self {
        Self {
            window: SubscriptionQuotaWindow::FiveHour,
            utilization: None,
            status: None,
            resets_at_unix_secs: None,
            surpassed_threshold: None,
            representative_claim: None,
            fallback_percentage: None,
            fallback_available: None,
            overage_in_use: None,
            overage_period_monthly_utilization: None,
            upgrade_paths: None,
            disabled_reason: None,
        }
    }
}

pub fn clamp_utilization_fraction(value: f64) -> f64 {
    value.clamp(0.0, 1.0)
}

pub fn percent_to_utilization_fraction(value: f64) -> f64 {
    clamp_utilization_fraction(value / 100.0)
}
