use std::collections::HashMap;
use std::sync::Arc;

use cc_lb_engine::SubscriptionQuotaCacheLike;
use cc_lb_plugin_api::{SubscriptionQuotaCandidateSnapshot, SubscriptionQuotaDataState};
use cc_lb_storage_api::{
    StorageResult, SubscriptionQuotaSample, SubscriptionQuotaSource,
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
        record: &SubscriptionQuotaSample,
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
    fn from_record(record: &SubscriptionQuotaSample) -> Self {
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
    fn upsert_observation(&self, record: &SubscriptionQuotaSample) {
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
    // Pure source selection: return exactly one source snapshot wholesale.
    // Never blend fields between sources — the returned snapshot's `source`
    // label, `utilization`, `observed_at_unix_millis`, and every other field
    // must all come from the same physical observation.
    //
    // Selection rules:
    //   1. Only one source has data → return that one as-is.
    //   2. Both sources present, exactly one fresh → fresh source wins as-is.
    //   3. Both fresh or both stale → newer `observed_at_unix_millis` wins
    //      as-is (api wins on tie since it is the higher-fidelity feed).
    //   4. Neither source has data → empty placeholder.
    match (sources.header.as_ref(), sources.api.as_ref()) {
        (Some(header), Some(api)) => match (header_fresh, api_fresh) {
            (true, false) => header.clone(),
            (false, true) => api.clone(),
            _ => {
                if api.observed_at_unix_millis >= header.observed_at_unix_millis {
                    api.clone()
                } else {
                    header.clone()
                }
            }
        },
        (Some(header), None) => header.clone(),
        (None, Some(api)) => api.clone(),
        (None, None) => MergedQuotaSnapshot {
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

    fn snapshot(
        source: MergedSource,
        utilization: f64,
        observed_at_unix_millis: u64,
    ) -> MergedQuotaSnapshot {
        MergedQuotaSnapshot {
            source,
            utilization: Some(utilization),
            status: Some(SubscriptionQuotaStatus::Allowed),
            resets_at_unix_secs: Some(observed_at_unix_millis / 1_000 + 3_600),
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
            observed_at_unix_millis,
        }
    }

    #[test]
    fn both_fresh_picks_newer_api_wholesale() {
        let header = snapshot(MergedSource::Header, 0.01, 1_000);
        let api = snapshot(MergedSource::Api, 0.37, 5_000);
        let sources = SourceSnapshots {
            header: Some(header),
            api: Some(api.clone()),
        };
        let result = merge_sources(&sources, true, true);
        assert_eq!(result, api);
    }

    #[test]
    fn both_fresh_picks_newer_header_wholesale() {
        let header = snapshot(MergedSource::Header, 0.20, 9_000);
        let api = snapshot(MergedSource::Api, 0.37, 5_000);
        let sources = SourceSnapshots {
            header: Some(header.clone()),
            api: Some(api),
        };
        let result = merge_sources(&sources, true, true);
        assert_eq!(result, header);
    }

    #[test]
    fn fresh_wins_over_stale_even_when_stale_is_newer() {
        let header = snapshot(MergedSource::Header, 0.01, 10_000);
        let api = snapshot(MergedSource::Api, 0.37, 5_000);
        let sources = SourceSnapshots {
            header: Some(header),
            api: Some(api.clone()),
        };
        let result = merge_sources(&sources, false, true);
        assert_eq!(result, api);
    }

    #[test]
    fn both_stale_picks_newer_observed_at_wholesale() {
        let header = snapshot(MergedSource::Header, 0.01, 1_000);
        let api = snapshot(MergedSource::Api, 0.37, 5_000);
        let sources = SourceSnapshots {
            header: Some(header),
            api: Some(api.clone()),
        };
        let result = merge_sources(&sources, false, false);
        assert_eq!(result, api);
    }

    #[test]
    fn source_label_matches_utilization_origin() {
        let stale_header = snapshot(MergedSource::Header, 0.01, 1_000);
        let fresh_api = snapshot(MergedSource::Api, 0.37, 5_000);
        let sources = SourceSnapshots {
            header: Some(stale_header),
            api: Some(fresh_api),
        };
        let result = merge_sources(&sources, true, true);
        assert_eq!(result.source, MergedSource::Api);
        assert_eq!(result.utilization, Some(0.37));
        assert_eq!(result.observed_at_unix_millis, 5_000);
    }

    #[test]
    fn only_header_returns_header_wholesale() {
        let header = snapshot(MergedSource::Header, 0.10, 1_000);
        let sources = SourceSnapshots {
            header: Some(header.clone()),
            api: None,
        };
        let result = merge_sources(&sources, true, false);
        assert_eq!(result, header);
    }

    #[test]
    fn only_api_returns_api_wholesale() {
        let api = snapshot(MergedSource::Api, 0.42, 2_000);
        let sources = SourceSnapshots {
            header: None,
            api: Some(api.clone()),
        };
        let result = merge_sources(&sources, false, true);
        assert_eq!(result, api);
    }

    #[test]
    fn neither_returns_empty_placeholder() {
        let sources = SourceSnapshots::default();
        let result = merge_sources(&sources, false, false);
        assert_eq!(result.source, MergedSource::Merged);
        assert!(result.utilization.is_none());
        assert_eq!(result.observed_at_unix_millis, 0);
    }
}
