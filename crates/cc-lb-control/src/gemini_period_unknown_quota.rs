//! Durable period-unknown Gemini meters, independent of canonical quota windows.

use cc_lb_storage_api::{MetaStore, StorageError, StorageResult};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const GEMINI_PERIOD_UNKNOWN_QUOTA_VERSION: u32 = 1;
pub const MAX_GEMINI_PERIOD_UNKNOWN_METERS_PER_GROUP: usize = 32;

pub fn gemini_period_unknown_quota_meta_key(upstream_id: Uuid) -> String {
    format!("gemini_period_unknown_quota:{upstream_id}")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GeminiQuotaGroupKind {
    Gemini,
    ThirdParty,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GeminiRawMeterOrigin {
    RetrieveUserQuota,
    RetrieveUserQuotaSummary,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GeminiPeriodUnknownMeter {
    pub group: GeminiQuotaGroupKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bucket_id: Option<String>,
    pub remaining_fraction: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reset_time_unix_secs: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remaining_amount: Option<String>,
    pub origin: GeminiRawMeterOrigin,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GeminiPeriodUnknownGroupRecord {
    pub observed_at_unix_millis: u64,
    pub meters: Vec<GeminiPeriodUnknownMeter>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GeminiPeriodUnknownQuotaRecord {
    pub version: u32,
    pub gemini: Option<GeminiPeriodUnknownGroupRecord>,
    pub third_party: Option<GeminiPeriodUnknownGroupRecord>,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub enum GeminiGroupPollUpdate {
    #[default]
    Unchanged,
    Replace(GeminiPeriodUnknownGroupRecord),
}

pub fn normalize_gemini_period_unknown_meters(
    group: GeminiQuotaGroupKind,
    meters: Vec<GeminiPeriodUnknownMeter>,
) -> Vec<GeminiPeriodUnknownMeter> {
    let mut normalized = std::collections::BTreeMap::new();
    for mut meter in meters {
        if meter.group != group || !meter.remaining_fraction.is_finite() {
            continue;
        }
        meter.remaining_fraction = meter.remaining_fraction.clamp(0.0, 1.0);
        let key = (meter.model_id.clone(), meter.bucket_id.clone());
        match normalized.entry(key) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(meter);
            }
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                let current: &GeminiPeriodUnknownMeter = entry.get();
                let earlier_reset = match (meter.reset_time_unix_secs, current.reset_time_unix_secs)
                {
                    (Some(incoming), Some(current)) => incoming < current,
                    (Some(_), None) => true,
                    _ => false,
                };
                if meter.remaining_fraction < current.remaining_fraction
                    || (meter.remaining_fraction == current.remaining_fraction && earlier_reset)
                {
                    entry.insert(meter);
                }
            }
        }
    }
    let mut meters: Vec<_> = normalized.into_values().collect();
    meters.sort_by(|a, b| {
        (
            a.model_id.as_deref().unwrap_or(""),
            a.bucket_id.as_deref().unwrap_or(""),
        )
            .cmp(&(
                b.model_id.as_deref().unwrap_or(""),
                b.bucket_id.as_deref().unwrap_or(""),
            ))
    });
    meters.truncate(MAX_GEMINI_PERIOD_UNKNOWN_METERS_PER_GROUP);
    meters
}

pub fn merge_gemini_period_unknown_quota(
    existing: Option<&GeminiPeriodUnknownQuotaRecord>,
    gemini: &GeminiGroupPollUpdate,
    third_party: &GeminiGroupPollUpdate,
) -> GeminiPeriodUnknownQuotaRecord {
    fn merge(
        existing: Option<&GeminiPeriodUnknownGroupRecord>,
        update: &GeminiGroupPollUpdate,
        group: GeminiQuotaGroupKind,
    ) -> Option<GeminiPeriodUnknownGroupRecord> {
        match update {
            GeminiGroupPollUpdate::Replace(incoming)
                if existing.is_none_or(|current| {
                    incoming.observed_at_unix_millis >= current.observed_at_unix_millis
                }) =>
            {
                Some(GeminiPeriodUnknownGroupRecord {
                    observed_at_unix_millis: incoming.observed_at_unix_millis,
                    meters: normalize_gemini_period_unknown_meters(group, incoming.meters.clone()),
                })
            }
            _ => existing.cloned(),
        }
    }
    GeminiPeriodUnknownQuotaRecord {
        version: GEMINI_PERIOD_UNKNOWN_QUOTA_VERSION,
        gemini: merge(
            existing.and_then(|record| record.gemini.as_ref()),
            gemini,
            GeminiQuotaGroupKind::Gemini,
        ),
        third_party: merge(
            existing.and_then(|record| record.third_party.as_ref()),
            third_party,
            GeminiQuotaGroupKind::ThirdParty,
        ),
    }
}

fn parse_record(raw: &str) -> Option<GeminiPeriodUnknownQuotaRecord> {
    let record: GeminiPeriodUnknownQuotaRecord = match serde_json::from_str(raw) {
        Ok(record) => record,
        Err(error) => {
            tracing::warn!(%error, "invalid Gemini period-unknown quota record");
            return None;
        }
    };
    if record.version != GEMINI_PERIOD_UNKNOWN_QUOTA_VERSION {
        tracing::warn!(
            version = record.version,
            "unknown Gemini period-unknown quota version"
        );
        return None;
    }
    Some(record)
}

pub async fn persist_gemini_period_unknown_quota(
    store: &dyn MetaStore,
    upstream_id: Uuid,
    gemini: GeminiGroupPollUpdate,
    third_party: GeminiGroupPollUpdate,
) -> StorageResult<()> {
    if matches!(
        (&gemini, &third_party),
        (
            GeminiGroupPollUpdate::Unchanged,
            GeminiGroupPollUpdate::Unchanged
        )
    ) {
        return Ok(());
    }
    let key = gemini_period_unknown_quota_meta_key(upstream_id);
    for _ in 0..8 {
        let raw = store.get_meta_value(&key).await?;
        let existing = raw.as_deref().and_then(parse_record);
        let merged = merge_gemini_period_unknown_quota(existing.as_ref(), &gemini, &third_party);
        if Some(&merged) == existing.as_ref() {
            return Ok(());
        }
        let serialized = serde_json::to_string(&merged).map_err(|error| StorageError::Fatal {
            message: format!("Gemini period-unknown quota encode failed: {error}"),
        })?;
        if store
            .compare_and_put_meta_value(&key, raw.as_deref(), &serialized)
            .await?
        {
            return Ok(());
        }
    }
    Ok(())
}

pub async fn load_gemini_period_unknown_quota(
    store: &dyn MetaStore,
    upstream_id: Uuid,
) -> StorageResult<Option<GeminiPeriodUnknownQuotaRecord>> {
    Ok(store
        .get_meta_value(&gemini_period_unknown_quota_meta_key(upstream_id))
        .await?
        .as_deref()
        .and_then(parse_record))
}
