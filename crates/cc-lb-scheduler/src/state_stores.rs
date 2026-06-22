#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "sqlite")]
mod sqlite;
mod types;

#[cfg(test)]
mod tests;

pub use types::{
    AnthropicCompatEtag, AnthropicCompatEtagsStore, OAuthUsagePollCursor,
    OAuthUsagePollCursorsStore, OAuthUsagePollScheduleConfig, PriceCatalogVersion,
    PriceCatalogVersionsStore,
};

use crate::error::{Result, SchedulerError};

pub(super) fn u64_to_i64(value: u64, field: &str) -> Result<i64> {
    i64::try_from(value).map_err(|_| SchedulerError::Job(format!("{field} exceeds i64::MAX")))
}

pub(super) fn option_u64_to_i64(value: Option<u64>, field: &str) -> Result<Option<i64>> {
    value.map(|inner| u64_to_i64(inner, field)).transpose()
}

pub(super) fn i64_to_u64(value: i64, field: &str) -> Result<u64> {
    u64::try_from(value).map_err(|_| SchedulerError::Job(format!("{field} is negative")))
}

pub(super) fn option_i64_to_u64(value: Option<i64>, field: &str) -> Result<Option<u64>> {
    value.map(|inner| i64_to_u64(inner, field)).transpose()
}

pub(super) fn u32_to_i32(value: u32, field: &str) -> Result<i32> {
    i32::try_from(value).map_err(|_| SchedulerError::Job(format!("{field} exceeds i32::MAX")))
}

#[cfg(feature = "postgres")]
pub(super) fn i32_to_u32(value: i32, field: &str) -> Result<u32> {
    u32::try_from(value).map_err(|_| SchedulerError::Job(format!("{field} is negative")))
}

#[cfg(feature = "sqlite")]
pub(super) fn i64_to_u32(value: i64, field: &str) -> Result<u32> {
    u32::try_from(value).map_err(|_| SchedulerError::Job(format!("{field} is outside u32")))
}

#[cfg(feature = "sqlite")]
pub(super) fn option_i64_to_i32(value: Option<i64>, field: &str) -> Result<Option<i32>> {
    value
        .map(|inner| {
            i32::try_from(inner).map_err(|_| SchedulerError::Job(format!("{field} is outside i32")))
        })
        .transpose()
}

pub(super) fn history_to_json(history: &[u64]) -> Result<String> {
    serde_json::to_string(history).map_err(|error| SchedulerError::Job(error.to_string()))
}

pub(super) fn parse_history(raw: &str, field: &str) -> Result<Vec<u64>> {
    serde_json::from_str(raw)
        .map_err(|error| SchedulerError::Job(format!("{field} has invalid JSON: {error}")))
}

pub(super) fn append_bounded(history: &[u64], observed_at_unix_secs: u64, cap: usize) -> Vec<u64> {
    if cap == 0 {
        return Vec::new();
    }
    let mut next = history.to_vec();
    next.push(observed_at_unix_secs);
    let overflow = next.len().saturating_sub(cap);
    if overflow > 0 {
        next.drain(0..overflow);
    }
    next
}
