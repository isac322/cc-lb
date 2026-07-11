#![forbid(unsafe_code)]

pub mod plan_capacity;
pub mod rate_limit_headers;

mod quota_samples;

pub use quota_samples::{build_subscription_quota_samples, unified_observation_to_sample};
pub use rate_limit_headers::{
    UnifiedQuotaObservation, clamp_utilization_fraction, parse_anthropic_rate_limit_headers,
    parse_anthropic_unified_headers, percent_to_utilization_fraction,
};
