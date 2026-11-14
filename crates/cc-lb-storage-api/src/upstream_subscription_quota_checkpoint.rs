use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    SubscriptionQuotaSample, SubscriptionQuotaSampleKind, SubscriptionQuotaSource,
    SubscriptionQuotaStatus, SubscriptionQuotaWindow,
};

const SEMANTIC_FINGERPRINT_VERSION: u8 = 2;
const DECIMAL_DIGITS: &[u8; 10] = b"0123456789";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SubscriptionQuotaSemanticFingerprint([u8; 32]);

macro_rules! fingerprint_fields {
    ($record:expr) => {{
        let record = $record;
        let mut bytes = Vec::with_capacity(256);
        bytes.push(SEMANTIC_FINGERPRINT_VERSION);
        write_optional_f64(&mut bytes, record.utilization);
        write_optional_status(&mut bytes, record.status);
        write_optional_u64(&mut bytes, record.resets_at_unix_secs);
        write_optional_f64(&mut bytes, record.surpassed_threshold);
        write_optional_f64(&mut bytes, record.fallback_percentage);
        write_optional_bool(&mut bytes, record.fallback_available);
        write_optional_bool(&mut bytes, record.overage_in_use);
        write_optional_f64(&mut bytes, record.overage_period_monthly_utilization);
        write_optional_strings(&mut bytes, record.upgrade_paths.as_deref());
        write_optional_string(&mut bytes, record.disabled_reason.as_deref());
        write_optional_bool(&mut bytes, record.extra_usage_enabled);
        write_optional_f64(&mut bytes, record.extra_usage_monthly_limit);
        write_optional_f64(&mut bytes, record.extra_usage_used_credits);
        write_optional_credits(&mut bytes, record.credits.as_ref());
        SubscriptionQuotaSemanticFingerprint(*blake3::hash(&bytes).as_bytes())
    }};
}

impl SubscriptionQuotaSemanticFingerprint {
    pub fn from_sample(record: &SubscriptionQuotaSample) -> Self {
        fingerprint_fields!(record)
    }

    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl SubscriptionQuotaSample {
    pub fn semantic_checkpoint_fingerprint(&self) -> SubscriptionQuotaSemanticFingerprint {
        SubscriptionQuotaSemanticFingerprint::from_sample(self)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SubscriptionQuotaCheckpointRecord {
    pub upstream_id: Uuid,
    pub window: SubscriptionQuotaWindow,
    pub source: SubscriptionQuotaSource,
    pub changed_at_unix_millis: u64,
    pub semantic_fingerprint: SubscriptionQuotaSemanticFingerprint,

    pub sample_kind: SubscriptionQuotaSampleKind,
    pub sample_id: Uuid,
    pub representative_claim: Option<String>,

    pub utilization: Option<f64>,
    pub status: Option<SubscriptionQuotaStatus>,
    pub resets_at_unix_secs: Option<u64>,
    pub surpassed_threshold: Option<f64>,
    pub fallback_percentage: Option<f64>,
    pub fallback_available: Option<bool>,
    pub overage_in_use: Option<bool>,
    pub overage_period_monthly_utilization: Option<f64>,
    pub upgrade_paths: Option<Vec<String>>,
    pub disabled_reason: Option<String>,

    pub extra_usage_enabled: Option<bool>,
    pub extra_usage_monthly_limit: Option<f64>,
    pub extra_usage_used_credits: Option<f64>,
    pub credits: Option<cc_lb_domain::SubscriptionQuotaCredits>,

    pub ingested_at_unix_millis: u64,
}

impl From<&SubscriptionQuotaSample> for SubscriptionQuotaCheckpointRecord {
    fn from(record: &SubscriptionQuotaSample) -> Self {
        Self {
            upstream_id: record.upstream_id,
            window: record.window,
            source: record.source,
            changed_at_unix_millis: record.observed_at_unix_millis,
            semantic_fingerprint: record.semantic_checkpoint_fingerprint(),
            sample_kind: record.sample_kind,
            sample_id: record.sample_id,
            representative_claim: record.representative_claim.clone(),
            utilization: record.utilization,
            status: record.status,
            resets_at_unix_secs: record.resets_at_unix_secs,
            surpassed_threshold: record.surpassed_threshold,
            fallback_percentage: record.fallback_percentage,
            fallback_available: record.fallback_available,
            overage_in_use: record.overage_in_use,
            overage_period_monthly_utilization: record.overage_period_monthly_utilization,
            upgrade_paths: record.upgrade_paths.clone(),
            disabled_reason: record.disabled_reason.clone(),
            extra_usage_enabled: record.extra_usage_enabled,
            extra_usage_monthly_limit: record.extra_usage_monthly_limit,
            credits: record.credits.clone(),
            extra_usage_used_credits: record.extra_usage_used_credits,
            ingested_at_unix_millis: record.ingested_at_unix_millis,
        }
    }
}
/// Result of comparing commit-known credit values across both physical streams.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CreditCheckpointChange {
    Older,
    Unchanged,
    Changed,
}

impl SubscriptionQuotaCheckpointRecord {
    /// Credit state is shared by Unified/Overage; quota state remains source-local.
    pub fn has_effective_credit_state(&self) -> bool {
        self.credits.is_some()
            && matches!(
                self.window,
                SubscriptionQuotaWindow::Unified | SubscriptionQuotaWindow::Overage
            )
    }

    /// Whether the record carries any non-credit quota state.
    pub fn has_quota_state(&self) -> bool {
        self.utilization.is_some()
            || self.status.is_some()
            || self.resets_at_unix_secs.is_some()
            || self.surpassed_threshold.is_some()
            || self.fallback_percentage.is_some()
            || self.fallback_available.is_some()
            || self.overage_in_use.is_some()
            || self.overage_period_monthly_utilization.is_some()
            || self.upgrade_paths.is_some()
            || self.disabled_reason.is_some()
            || self.extra_usage_enabled.is_some()
            || self.extra_usage_monthly_limit.is_some()
            || self.extra_usage_used_credits.is_some()
    }

    /// Normalize a physical checkpoint and compare its effective credit values.
    /// `rows` contains the recorded per-key anchors as of this observation.
    /// `latest` supplies commit-known freshness without rewriting historical anchors.
    pub fn prepare_credit_checkpoint(
        &mut self,
        mut rows: Vec<crate::SubscriptionQuotaSlimCheckpoint>,
        latest: Vec<crate::SubscriptionQuotaSlimCheckpoint>,
    ) -> CreditCheckpointChange {
        use cc_lb_domain::{CreditSource, SubscriptionQuotaCredits};
        use std::collections::BTreeMap;
        let newest = rows
            .iter()
            .chain(&latest)
            .map(|row| row.changed_at_unix_millis)
            .max();
        let older = newest.is_some_and(|at| self.changed_at_unix_millis < at);
        if !older {
            rows.extend(latest);
        }
        rows.sort_by_key(|row| {
            (
                row.window,
                row.source,
                row.changed_at_unix_millis,
                row.sample_id,
            )
        });
        let mut states = BTreeMap::<_, SubscriptionQuotaCredits>::new();
        for row in rows {
            let key = (row.window, row.source);
            if row.sample_kind == SubscriptionQuotaSampleKind::Absent {
                states.remove(&key);
            } else if let Some(mut credits) = row.credits {
                credits.stamp_observations(
                    row.changed_at_unix_millis,
                    if row.source == SubscriptionQuotaSource::Api {
                        CreditSource::Api
                    } else {
                        CreditSource::Header
                    },
                );
                states.entry(key).or_default().merge_observations(&credits);
            }
        }
        let merge = |states: &BTreeMap<_, SubscriptionQuotaCredits>| {
            let mut effective = SubscriptionQuotaCredits::default();
            for credits in states.values() {
                effective.merge_observations(credits);
            }
            effective
        };
        let before = merge(&states);
        if older {
            self.credits = Some(before);
            self.semantic_fingerprint = fingerprint_fields!(&*self);
            return CreditCheckpointChange::Older;
        }
        let key = (self.window, self.source);
        if let Some(mut incoming) = self.credits.take() {
            incoming.stamp_observations(
                self.changed_at_unix_millis,
                if self.source == SubscriptionQuotaSource::Api {
                    CreditSource::Api
                } else {
                    CreditSource::Header
                },
            );
            states.entry(key).or_default().merge_observations(&incoming);
        }
        let after = merge(&states);
        let changed = before.has_credits != after.has_credits
            || before.unlimited != after.unlimited
            || before.limit_reached != after.limit_reached
            || before.balance != after.balance
            || before.approx_local_messages != after.approx_local_messages
            || before.approx_cloud_messages != after.approx_cloud_messages;
        // Physical quota provenance stays on the row. Credit fields retain the
        // effective snapshot and their own original field provenance so a
        // subsequent quota-only change cannot resurrect ignored stale credits.
        self.credits = Some(after);
        self.semantic_fingerprint = fingerprint_fields!(&*self);
        if changed {
            CreditCheckpointChange::Changed
        } else {
            CreditCheckpointChange::Unchanged
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SubscriptionQuotaCheckpointRangeQuery {
    pub upstream_ids: Vec<Uuid>,
    pub windows: Vec<SubscriptionQuotaWindow>,
    pub sources: Vec<SubscriptionQuotaSource>,
    pub since_unix_millis: u64,
    pub until_unix_millis: u64,
}

fn write_optional_f64(bytes: &mut Vec<u8>, value: Option<f64>) {
    match value {
        Some(value) => {
            bytes.push(1);
            bytes.extend_from_slice(&value.to_bits().to_be_bytes());
        }
        None => bytes.push(0),
    }
}

fn write_optional_status(bytes: &mut Vec<u8>, value: Option<SubscriptionQuotaStatus>) {
    match value {
        Some(SubscriptionQuotaStatus::Allowed) => bytes.extend_from_slice(&[1, 1]),
        Some(SubscriptionQuotaStatus::AllowedWarning) => bytes.extend_from_slice(&[1, 2]),
        Some(SubscriptionQuotaStatus::Rejected) => bytes.extend_from_slice(&[1, 3]),
        None => bytes.push(0),
    }
}

fn write_optional_u64(bytes: &mut Vec<u8>, value: Option<u64>) {
    match value {
        Some(value) => {
            bytes.push(1);
            bytes.extend_from_slice(&value.to_be_bytes());
        }
        None => bytes.push(0),
    }
}

fn write_optional_bool(bytes: &mut Vec<u8>, value: Option<bool>) {
    match value {
        Some(false) => bytes.extend_from_slice(&[1, 0]),
        Some(true) => bytes.extend_from_slice(&[1, 1]),
        None => bytes.push(0),
    }
}

fn write_optional_strings(bytes: &mut Vec<u8>, value: Option<&[String]>) {
    match value {
        Some(values) => {
            bytes.push(1);
            write_len(bytes, values.len());
            for value in values {
                write_string(bytes, value);
            }
        }
        None => bytes.push(0),
    }
}

fn write_optional_string(bytes: &mut Vec<u8>, value: Option<&str>) {
    match value {
        Some(value) => {
            bytes.push(1);
            write_string(bytes, value);
        }
        None => bytes.push(0),
    }
}

fn write_optional_credits(
    bytes: &mut Vec<u8>,
    value: Option<&cc_lb_domain::SubscriptionQuotaCredits>,
) {
    match value {
        Some(value) => {
            bytes.push(1);
            write_optional_bool(bytes, value.has_credits);
            write_optional_bool(bytes, value.unlimited);
            write_optional_bool(bytes, value.limit_reached);
            write_optional_string(bytes, value.balance.as_deref());
            write_optional_u64s(
                bytes,
                value
                    .approx_local_messages
                    .as_ref()
                    .map(|pair| pair.as_slice()),
            );
            write_optional_u64s(
                bytes,
                value
                    .approx_cloud_messages
                    .as_ref()
                    .map(|pair| pair.as_slice()),
            );
        }
        None => bytes.push(0),
    }
}

fn write_optional_u64s(bytes: &mut Vec<u8>, value: Option<&[u64]>) {
    match value {
        Some(values) => {
            bytes.push(1);
            write_len(bytes, values.len());
            for value in values {
                write_optional_u64(bytes, Some(*value));
            }
        }
        None => bytes.push(0),
    }
}

fn write_string(bytes: &mut Vec<u8>, value: &str) {
    write_len(bytes, value.len());
    bytes.extend_from_slice(value.as_bytes());
}

fn write_len(bytes: &mut Vec<u8>, value: usize) {
    let mut divisor = 1;
    while value / divisor >= 10 {
        divisor *= 10;
    }

    let mut remaining = value;
    while divisor > 0 {
        let digit = remaining / divisor;
        bytes.push(DECIMAL_DIGITS[digit]);
        remaining %= divisor;
        divisor /= 10;
    }
    bytes.push(b':');
}
