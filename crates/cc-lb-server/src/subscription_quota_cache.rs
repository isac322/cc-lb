use std::collections::HashMap;
use std::sync::Arc;

use cc_lb_core::SubscriptionQuotaCacheLike;
use cc_lb_plugin_api::{SubscriptionQuotaCandidateSnapshot, SubscriptionQuotaDataState};
use cc_lb_storage_api::{
    StorageResult, SubscriptionQuotaObservationRecord, SubscriptionQuotaSource,
    SubscriptionQuotaStatus, SubscriptionQuotaWindow,
};
use parking_lot::RwLock;
use uuid::Uuid;

use crate::dynamic_view_builder::Stores;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MergedSource {
    Header,
    Api,
    Merged,
}

impl MergedSource {
    fn as_str(self) -> &'static str {
        match self {
            Self::Header => "header",
            Self::Api => "api",
            Self::Merged => "merged",
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct MergedQuotaSnapshot {
    pub source: MergedSource,
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
    pub extra_usage_enabled: Option<bool>,
    pub extra_usage_monthly_limit: Option<f64>,
    pub extra_usage_used_credits: Option<f64>,
    pub observed_at_unix_millis: u64,
}

#[derive(Clone, Debug, Default)]
struct SourceSnapshots {
    header: Option<MergedQuotaSnapshot>,
    api: Option<MergedQuotaSnapshot>,
}

#[derive(Debug, Default)]
pub struct SubscriptionQuotaCache {
    inner: Arc<RwLock<HashMap<Uuid, HashMap<SubscriptionQuotaWindow, SourceSnapshots>>>>,
}

impl SubscriptionQuotaCache {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn hydrate_from_store(
        &self,
        stores: &Stores,
        upstream_ids: &[Uuid],
    ) -> StorageResult<()> {
        let records = stores
            .upstream_subscription_quotas
            .list_latest_subscription_quota_for_upstreams(upstream_ids)
            .await?;
        for record in records {
            self.upsert_observation(record.upstream_id, &record);
        }
        Ok(())
    }

    pub fn upsert_observation(
        &self,
        upstream_id: Uuid,
        record: &SubscriptionQuotaObservationRecord,
    ) {
        let snapshot = MergedQuotaSnapshot::from_record(record);
        let mut guard = self.inner.write();
        let sources = guard
            .entry(upstream_id)
            .or_default()
            .entry(record.window)
            .or_default();
        let slot = match record.source {
            SubscriptionQuotaSource::Header => &mut sources.header,
            SubscriptionQuotaSource::Api => &mut sources.api,
        };
        if slot
            .as_ref()
            .is_none_or(|current| record.observed_at_unix_millis >= current.observed_at_unix_millis)
        {
            *slot = Some(snapshot);
        }
    }

    pub fn snapshot_for_upstream(
        &self,
        upstream_id: Uuid,
        now_unix_millis: u64,
        max_staleness_secs: u64,
    ) -> Vec<SubscriptionQuotaCandidateSnapshot> {
        let guard = self.inner.read();
        let windows = guard.get(&upstream_id);
        SubscriptionQuotaWindow::all()
            .iter()
            .copied()
            .map(
                |window| match windows.and_then(|by_window| by_window.get(&window)) {
                    Some(sources) => {
                        candidate_from_sources(window, sources, now_unix_millis, max_staleness_secs)
                    }
                    None => missing_candidate(window, max_staleness_secs),
                },
            )
            .collect()
    }
}

impl MergedQuotaSnapshot {
    fn from_record(record: &SubscriptionQuotaObservationRecord) -> Self {
        Self {
            source: match record.source {
                SubscriptionQuotaSource::Header => MergedSource::Header,
                SubscriptionQuotaSource::Api => MergedSource::Api,
            },
            utilization: record.utilization,
            status: record.status,
            resets_at_unix_secs: record.resets_at_unix_secs,
            surpassed_threshold: record.surpassed_threshold,
            representative_claim: record.representative_claim.clone(),
            fallback_percentage: record.fallback_percentage,
            fallback_available: record.fallback_available,
            overage_in_use: record.overage_in_use,
            overage_period_monthly_utilization: record.overage_period_monthly_utilization,
            upgrade_paths: record.upgrade_paths.clone(),
            disabled_reason: record.disabled_reason.clone(),
            extra_usage_enabled: record.extra_usage_enabled,
            extra_usage_monthly_limit: record.extra_usage_monthly_limit,
            extra_usage_used_credits: record.extra_usage_used_credits,
            observed_at_unix_millis: record.observed_at_unix_millis,
        }
    }
}

impl SubscriptionQuotaCacheLike for SubscriptionQuotaCache {
    fn upsert_observation(&self, record: &SubscriptionQuotaObservationRecord) {
        self.upsert_observation(record.upstream_id, record);
    }

    fn snapshot_for_upstream(
        &self,
        upstream_id: Uuid,
        now_unix_millis: u64,
        max_staleness_secs: u64,
    ) -> Vec<SubscriptionQuotaCandidateSnapshot> {
        SubscriptionQuotaCache::snapshot_for_upstream(
            self,
            upstream_id,
            now_unix_millis,
            max_staleness_secs,
        )
    }
}

fn candidate_from_sources(
    window: SubscriptionQuotaWindow,
    sources: &SourceSnapshots,
    now_unix_millis: u64,
    max_staleness_secs: u64,
) -> SubscriptionQuotaCandidateSnapshot {
    let header_fresh = sources
        .header
        .as_ref()
        .is_some_and(|snapshot| is_fresh(snapshot, now_unix_millis, max_staleness_secs));
    let api_fresh = sources
        .api
        .as_ref()
        .is_some_and(|snapshot| is_fresh(snapshot, now_unix_millis, max_staleness_secs));
    let state = if header_fresh || api_fresh {
        SubscriptionQuotaDataState::Fresh
    } else {
        SubscriptionQuotaDataState::Stale
    };
    let merged = merge_sources(sources, header_fresh, api_fresh);
    candidate_from_snapshot(window, state, merged, max_staleness_secs)
}

fn merge_sources(
    sources: &SourceSnapshots,
    header_fresh: bool,
    api_fresh: bool,
) -> MergedQuotaSnapshot {
    match (&sources.header, &sources.api, header_fresh, api_fresh) {
        (Some(header), Some(api), true, true) => merge_header_api(header, api),
        (Some(header), _, true, _) => header.clone(),
        (_, Some(api), _, true) => api.clone(),
        (Some(header), Some(api), _, _) => merge_header_api(header, api),
        (Some(header), None, _, _) => header.clone(),
        (None, Some(api), _, _) => api.clone(),
        (None, None, _, _) => MergedQuotaSnapshot {
            source: MergedSource::Merged,
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
            extra_usage_enabled: None,
            extra_usage_monthly_limit: None,
            extra_usage_used_credits: None,
            observed_at_unix_millis: 0,
        },
    }
}

fn merge_header_api(
    header: &MergedQuotaSnapshot,
    api: &MergedQuotaSnapshot,
) -> MergedQuotaSnapshot {
    MergedQuotaSnapshot {
        source: MergedSource::Merged,
        utilization: header.utilization.or(api.utilization),
        status: header.status.or(api.status),
        resets_at_unix_secs: header.resets_at_unix_secs.or(api.resets_at_unix_secs),
        surpassed_threshold: header.surpassed_threshold.or(api.surpassed_threshold),
        representative_claim: header
            .representative_claim
            .clone()
            .or_else(|| api.representative_claim.clone()),
        fallback_percentage: header.fallback_percentage.or(api.fallback_percentage),
        fallback_available: header.fallback_available.or(api.fallback_available),
        overage_in_use: header.overage_in_use.or(api.overage_in_use),
        overage_period_monthly_utilization: header
            .overage_period_monthly_utilization
            .or(api.overage_period_monthly_utilization),
        upgrade_paths: header
            .upgrade_paths
            .clone()
            .or_else(|| api.upgrade_paths.clone()),
        disabled_reason: header
            .disabled_reason
            .clone()
            .or_else(|| api.disabled_reason.clone()),
        extra_usage_enabled: api.extra_usage_enabled.or(header.extra_usage_enabled),
        extra_usage_monthly_limit: api
            .extra_usage_monthly_limit
            .or(header.extra_usage_monthly_limit),
        extra_usage_used_credits: api
            .extra_usage_used_credits
            .or(header.extra_usage_used_credits),
        observed_at_unix_millis: header
            .observed_at_unix_millis
            .max(api.observed_at_unix_millis),
    }
}

fn is_fresh(snapshot: &MergedQuotaSnapshot, now_unix_millis: u64, max_staleness_secs: u64) -> bool {
    now_unix_millis.saturating_sub(snapshot.observed_at_unix_millis)
        <= max_staleness_secs.saturating_mul(1_000)
}

fn candidate_from_snapshot(
    window: SubscriptionQuotaWindow,
    state: SubscriptionQuotaDataState,
    snapshot: MergedQuotaSnapshot,
    max_staleness_secs: u64,
) -> SubscriptionQuotaCandidateSnapshot {
    SubscriptionQuotaCandidateSnapshot {
        window: window.as_str().to_owned(),
        state,
        source: Some(snapshot.source.as_str().to_owned()),
        utilization: snapshot.utilization,
        status: snapshot.status.map(|status| status.as_str().to_owned()),
        resets_at_unix_secs: snapshot.resets_at_unix_secs,
        surpassed_threshold: snapshot.surpassed_threshold,
        representative_claim: snapshot.representative_claim,
        disabled_reason: snapshot.disabled_reason,
        extra_usage_enabled: snapshot.extra_usage_enabled,
        extra_usage_monthly_limit: snapshot.extra_usage_monthly_limit,
        extra_usage_used_credits: snapshot.extra_usage_used_credits,
        observed_at_unix_millis: Some(snapshot.observed_at_unix_millis),
        max_staleness_secs,
        fallback_available: snapshot.fallback_available,
        overage_in_use: snapshot.overage_in_use,
        overage_period_monthly_utilization: snapshot.overage_period_monthly_utilization,
        upgrade_paths: snapshot.upgrade_paths,
    }
}

fn missing_candidate(
    window: SubscriptionQuotaWindow,
    max_staleness_secs: u64,
) -> SubscriptionQuotaCandidateSnapshot {
    SubscriptionQuotaCandidateSnapshot {
        window: window.as_str().to_owned(),
        state: SubscriptionQuotaDataState::Missing,
        source: None,
        utilization: None,
        status: None,
        resets_at_unix_secs: None,
        surpassed_threshold: None,
        representative_claim: None,
        disabled_reason: None,
        extra_usage_enabled: None,
        extra_usage_monthly_limit: None,
        extra_usage_used_credits: None,
        observed_at_unix_millis: None,
        max_staleness_secs,
        fallback_available: None,
        overage_in_use: None,
        overage_period_monthly_utilization: None,
        upgrade_paths: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cc_lb_storage_api::SubscriptionQuotaSampleKind;

    const MAX_STALENESS_SECS: u64 = 1_800;

    fn snapshot_with(
        observed_at_unix_millis: u64,
        resets_at_unix_secs: Option<u64>,
    ) -> MergedQuotaSnapshot {
        MergedQuotaSnapshot {
            source: MergedSource::Header,
            utilization: Some(0.87),
            status: None,
            resets_at_unix_secs,
            surpassed_threshold: None,
            representative_claim: Some("seven_day".to_owned()),
            fallback_percentage: None,
            fallback_available: None,
            overage_in_use: None,
            overage_period_monthly_utilization: None,
            upgrade_paths: None,
            disabled_reason: None,
            extra_usage_enabled: None,
            extra_usage_monthly_limit: None,
            extra_usage_used_credits: None,
            observed_at_unix_millis,
        }
    }

    fn observation_record(
        upstream_id: Uuid,
        observed_at_unix_millis: u64,
        resets_at_unix_secs: Option<u64>,
    ) -> SubscriptionQuotaObservationRecord {
        SubscriptionQuotaObservationRecord {
            upstream_id,
            window: SubscriptionQuotaWindow::SevenDay,
            source: SubscriptionQuotaSource::Header,
            sample_kind: SubscriptionQuotaSampleKind::Sample,
            observed_at_unix_millis,
            sample_id: Uuid::nil(),
            utilization: Some(0.87),
            status: None,
            resets_at_unix_secs,
            surpassed_threshold: None,
            representative_claim: Some("seven_day".to_owned()),
            fallback_percentage: None,
            fallback_available: None,
            overage_in_use: None,
            overage_period_monthly_utilization: None,
            upgrade_paths: None,
            disabled_reason: None,
            extra_usage_enabled: None,
            extra_usage_monthly_limit: None,
            extra_usage_used_credits: None,
            ingested_at_unix_millis: observed_at_unix_millis,
        }
    }

    #[test]
    fn is_fresh_returns_false_when_resets_at_is_past() {
        let now_unix_millis: u64 = 1_750_000_000_000;
        let snapshot = snapshot_with(
            now_unix_millis - 60_000,
            Some(now_unix_millis / 1_000 - 1),
        );
        assert!(
            !is_fresh(&snapshot, now_unix_millis, MAX_STALENESS_SECS),
            "snapshot whose resets_at is in the past must be reported Stale",
        );
    }

    #[test]
    fn is_fresh_returns_true_when_resets_at_is_future_and_observation_recent() {
        let now_unix_millis: u64 = 1_750_000_000_000;
        let snapshot = snapshot_with(
            now_unix_millis - 10_000,
            Some(now_unix_millis / 1_000 + 60),
        );
        assert!(is_fresh(&snapshot, now_unix_millis, MAX_STALENESS_SECS));
    }

    #[test]
    fn is_fresh_returns_false_when_observation_too_old() {
        let now_unix_millis: u64 = 1_750_000_000_000;
        let snapshot = snapshot_with(
            now_unix_millis - (MAX_STALENESS_SECS * 1_000 + 1_000),
            Some(now_unix_millis / 1_000 + 3_600),
        );
        assert!(!is_fresh(&snapshot, now_unix_millis, MAX_STALENESS_SECS));
    }

    #[test]
    fn is_fresh_returns_true_when_no_resets_at() {
        let now_unix_millis: u64 = 1_750_000_000_000;
        let snapshot = snapshot_with(now_unix_millis - 5_000, None);
        assert!(is_fresh(&snapshot, now_unix_millis, MAX_STALENESS_SECS));
    }

    #[test]
    fn is_fresh_returns_false_exactly_at_resets_at() {
        let now_unix_millis: u64 = 1_750_000_000_000;
        let snapshot = snapshot_with(
            now_unix_millis - 1_000,
            Some(now_unix_millis / 1_000),
        );
        assert!(
            !is_fresh(&snapshot, now_unix_millis, MAX_STALENESS_SECS),
            "snapshot whose resets_at equals now must be reported Stale",
        );
    }

    #[test]
    fn snapshot_for_upstream_reports_stale_after_reset() {
        let cache = SubscriptionQuotaCache::new();
        let upstream_id = Uuid::new_v4();
        let now_unix_millis: u64 = 1_750_000_000_000;
        let record = observation_record(
            upstream_id,
            now_unix_millis - 30_000,
            Some(now_unix_millis / 1_000 - 10),
        );

        cache.upsert_observation(upstream_id, &record);
        let candidates = cache.snapshot_for_upstream(
            upstream_id,
            now_unix_millis,
            MAX_STALENESS_SECS,
        );

        let seven_day = candidates
            .iter()
            .find(|c| c.window == SubscriptionQuotaWindow::SevenDay.as_str())
            .expect("7d candidate present");
        assert_eq!(seven_day.state, SubscriptionQuotaDataState::Stale);
        assert_eq!(
            seven_day.utilization,
            Some(0.87),
            "utilization is still surfaced for diagnostics; downstream gates on state",
        );
    }

    #[test]
    fn snapshot_for_upstream_keeps_fresh_before_reset() {
        let cache = SubscriptionQuotaCache::new();
        let upstream_id = Uuid::new_v4();
        let now_unix_millis: u64 = 1_750_000_000_000;
        let record = observation_record(
            upstream_id,
            now_unix_millis - 30_000,
            Some(now_unix_millis / 1_000 + 10),
        );

        cache.upsert_observation(upstream_id, &record);
        let candidates = cache.snapshot_for_upstream(
            upstream_id,
            now_unix_millis,
            MAX_STALENESS_SECS,
        );

        let seven_day = candidates
            .iter()
            .find(|c| c.window == SubscriptionQuotaWindow::SevenDay.as_str())
            .expect("7d candidate present");
        assert_eq!(seven_day.state, SubscriptionQuotaDataState::Fresh);
    }
}
