use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

use cc_lb_control::SubscriptionQuotaCacheLike;
use cc_lb_domain::{
    SubscriptionQuotaCandidateSnapshot, SubscriptionQuotaCredits, SubscriptionQuotaDataState,
};
use cc_lb_storage_api::{
    StorageResult, SubscriptionQuotaSample, SubscriptionQuotaSampleKind, SubscriptionQuotaSource,
    SubscriptionQuotaStatus, SubscriptionQuotaWindow,
};
use parking_lot::RwLock;
use uuid::Uuid;

use crate::dynamic_view_builder::Stores;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MergedSource {
    Header,
    Api,
}

impl MergedSource {
    fn as_str(self) -> &'static str {
        match self {
            Self::Header => "header",
            Self::Api => "api",
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct MergedQuotaSnapshot {
    pub source: MergedSource,
    pub sample_kind: SubscriptionQuotaSampleKind,
    pub utilization: Option<f64>,
    pub status: Option<SubscriptionQuotaStatus>,
    pub resets_at_unix_secs: Option<u64>,
    pub surpassed_threshold: Option<f64>,
    pub representative_claim: Option<String>,
    pub fallback_available: Option<bool>,
    pub overage_in_use: Option<bool>,
    pub overage_period_monthly_utilization: Option<f64>,
    pub upgrade_paths: Option<Vec<String>>,
    pub disabled_reason: Option<String>,
    pub extra_usage_enabled: Option<bool>,
    pub extra_usage_monthly_limit: Option<f64>,
    pub extra_usage_used_credits: Option<f64>,
    pub credits: Option<SubscriptionQuotaCredits>,
    pub observed_at_unix_millis: u64,
}

#[derive(Clone, Debug, Default)]
struct SourceSnapshots {
    header: Option<MergedQuotaSnapshot>,
    api: Option<MergedQuotaSnapshot>,
}

/// One `(upstream, window)` entry: the per-source observations plus a
/// per-slot version that every replacement of that slot bumps, whichever
/// path installed it.
#[derive(Debug, Default)]
struct WindowSlots {
    sources: SourceSnapshots,
    header_version: u64,
    api_version: u64,
}

impl WindowSlots {
    fn versions(&self) -> SourceVersions {
        SourceVersions {
            header: self.header_version,
            api: self.api_version,
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct SourceVersions {
    header: u64,
    api: u64,
}

impl SourceVersions {
    fn get(self, source: SubscriptionQuotaSource) -> u64 {
        match source {
            SubscriptionQuotaSource::Header => self.header,
            SubscriptionQuotaSource::Api => self.api,
        }
    }
}

/// Slot versions captured before a storage read. A slot absent here had
/// version 0 when captured, which is also the version of a new slot.
type SlotVersions = HashMap<(Uuid, SubscriptionQuotaWindow), SourceVersions>;

/// What an incoming observation does to a resident one with the same
/// `observed_at_unix_millis` in its `(upstream, window, source)` slot.
/// Strictly newer observations always replace, older ones never do, and an
/// empty slot always accepts.
enum EqualObservedAt<'a> {
    Replace,
    /// Keep the resident observation when a durable fallback has the same
    /// timestamp; strictly newer values still replace it.
    Keep,
    /// Replace only if the slot has not been replaced since these versions
    /// were captured, so a read that raced a concurrent install cannot
    /// restore older content.
    ReplaceIfUnchangedSince(&'a SlotVersions),
}

#[derive(Debug)]
pub struct SubscriptionQuotaCache {
    inner: Arc<RwLock<HashMap<Uuid, HashMap<SubscriptionQuotaWindow, WindowSlots>>>>,
    poll_sequence: AtomicU64,
    published_polls: parking_lot::Mutex<HashMap<Uuid, u64>>,
    poll_publisher: Uuid,
}

impl Default for SubscriptionQuotaCache {
    fn default() -> Self {
        Self {
            inner: Arc::default(),
            poll_sequence: AtomicU64::new(0),
            published_polls: parking_lot::Mutex::default(),
            poll_publisher: Uuid::new_v4(),
        }
    }
}

impl SubscriptionQuotaCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Request order is independent of observation timestamps. A delayed older
    /// request cannot publish after a newer poll has published its batch.
    pub(crate) fn begin_poll(&self) -> u64 {
        self.poll_sequence.fetch_add(1, AtomicOrdering::Relaxed) + 1
    }

    pub(crate) fn credit_request_order(&self, sequence: u64) -> cc_lb_domain::CreditRequestOrder {
        cc_lb_domain::CreditRequestOrder {
            publisher: self.poll_publisher,
            sequence,
        }
    }

    pub(crate) fn publish_poll(
        &self,
        upstream_id: Uuid,
        sequence: u64,
        records: &[SubscriptionQuotaSample],
    ) -> bool {
        let mut published = self.published_polls.lock();
        if published
            .get(&upstream_id)
            .is_some_and(|current| *current > sequence)
        {
            return false;
        }
        for record in records {
            self.upsert_observation(upstream_id, record);
        }
        published.insert(upstream_id, sequence);
        true
    }

    pub async fn hydrate_from_store(
        &self,
        stores: &Stores,
        upstream_ids: &[Uuid],
    ) -> StorageResult<()> {
        let records = self.read_latest(stores, upstream_ids).await?;
        for record in records {
            self.upsert_with_tie_policy(record.upstream_id, &record, EqualObservedAt::Replace);
        }
        Ok(())
    }

    /// Periodic-reconcile hydration. It captures the target slots' versions
    /// before the storage read; a stored observation then replaces the
    /// resident one when strictly newer, or when its timestamp is equal and
    /// no other path replaced the slot during the read. A same-timestamp
    /// observation persisted by another replica whose notification was lost
    /// is therefore recovered, while a read that raced a local or notified
    /// install cannot restore the older content. Empty slots always hydrate.
    pub(crate) async fn hydrate_from_store_unless_raced(
        &self,
        stores: &Stores,
        upstream_ids: &[Uuid],
    ) -> StorageResult<()> {
        let versions = self.slot_versions(upstream_ids);
        let records = self.read_latest(stores, upstream_ids).await?;
        for record in records {
            self.upsert_with_tie_policy(
                record.upstream_id,
                &record,
                EqualObservedAt::ReplaceIfUnchangedSince(&versions),
            );
        }
        Ok(())
    }

    async fn read_latest(
        &self,
        stores: &Stores,
        upstream_ids: &[Uuid],
    ) -> StorageResult<Vec<SubscriptionQuotaSample>> {
        stores
            .upstream_subscription_quotas
            .list_latest_subscription_quota_for_upstreams(upstream_ids)
            .await
    }

    fn slot_versions(&self, upstream_ids: &[Uuid]) -> SlotVersions {
        let guard = self.inner.read();
        let mut versions = SlotVersions::new();
        for upstream_id in upstream_ids {
            if let Some(windows) = guard.get(upstream_id) {
                for (window, slots) in windows {
                    versions.insert((*upstream_id, *window), slots.versions());
                }
            }
        }
        versions
    }

    pub fn upsert_observation(&self, upstream_id: Uuid, record: &SubscriptionQuotaSample) {
        self.upsert_with_tie_policy(upstream_id, record, EqualObservedAt::Replace);
    }
    /// Install a durable read-side fallback without overwriting an equal-time
    /// live observation that arrived while storage was being read.
    pub fn upsert_durable_if_newer(&self, upstream_id: Uuid, record: &SubscriptionQuotaSample) {
        self.upsert_with_tie_policy(upstream_id, record, EqualObservedAt::Keep);
    }

    fn upsert_with_tie_policy(
        &self,
        upstream_id: Uuid,
        record: &SubscriptionQuotaSample,
        tie: EqualObservedAt<'_>,
    ) {
        let mut snapshot = MergedQuotaSnapshot::from_record(record);
        let mut guard = self.inner.write();
        let slots = guard
            .entry(upstream_id)
            .or_default()
            .entry(record.window)
            .or_default();
        let (slot, version) = match record.source {
            SubscriptionQuotaSource::Header => {
                (&mut slots.sources.header, &mut slots.header_version)
            }
            SubscriptionQuotaSource::Api => (&mut slots.sources.api, &mut slots.api_version),
        };
        let accept = match slot.as_ref() {
            None => true,
            Some(current) => match record
                .observed_at_unix_millis
                .cmp(&current.observed_at_unix_millis)
            {
                Ordering::Greater => true,
                Ordering::Less => false,
                Ordering::Equal => match tie {
                    EqualObservedAt::Replace => true,
                    EqualObservedAt::Keep => false,
                    EqualObservedAt::ReplaceIfUnchangedSince(captured) => {
                        let captured = captured
                            .get(&(upstream_id, record.window))
                            .copied()
                            .unwrap_or_default()
                            .get(record.source);
                        captured == *version
                    }
                },
            },
        };
        if accept {
            if record.sample_kind == SubscriptionQuotaSampleKind::Sample {
                let source = match record.source {
                    SubscriptionQuotaSource::Header => cc_lb_domain::CreditSource::Header,
                    SubscriptionQuotaSource::Api => cc_lb_domain::CreditSource::Api,
                };
                if let Some(incoming) = snapshot.credits.as_mut() {
                    incoming.stamp_observations(record.observed_at_unix_millis, source);
                }
                if let Some(current) = slot.as_mut()
                    && let Some(mut credits) = current.credits.take()
                {
                    credits.stamp_observations(current.observed_at_unix_millis, source);
                    if let Some(incoming) = snapshot.credits.as_ref() {
                        credits.merge_observations(incoming);
                    }
                    snapshot.credits = Some(credits);
                }
            }
            *slot = Some(snapshot);
            *version = version.wrapping_add(1);
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
                    Some(slots) => candidate_from_sources(
                        window,
                        &slots.sources,
                        now_unix_millis,
                        max_staleness_secs,
                    ),
                    None => unobserved_candidate(window, max_staleness_secs),
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
            sample_kind: record.sample_kind,
            utilization: record.utilization,
            status: record.status,
            resets_at_unix_secs: record.resets_at_unix_secs,
            surpassed_threshold: record.surpassed_threshold,
            representative_claim: record.representative_claim.clone(),
            fallback_available: record.fallback_available,
            overage_in_use: record.overage_in_use,
            overage_period_monthly_utilization: record.overage_period_monthly_utilization,
            upgrade_paths: record.upgrade_paths.clone(),
            disabled_reason: record.disabled_reason.clone(),
            extra_usage_enabled: record.extra_usage_enabled,
            extra_usage_monthly_limit: record.extra_usage_monthly_limit,
            extra_usage_used_credits: record.extra_usage_used_credits,
            credits: record.credits.clone(),
            observed_at_unix_millis: record.observed_at_unix_millis,
        }
    }
}

impl SubscriptionQuotaCacheLike for SubscriptionQuotaCache {
    fn upsert_observation(&self, record: &SubscriptionQuotaSample) {
        self.upsert_observation(record.upstream_id, record);
    }
    fn upsert_durable_if_newer(&self, record: &SubscriptionQuotaSample) {
        self.upsert_durable_if_newer(record.upstream_id, record);
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
    let Some(mut merged) = merge_sources(sources, header_fresh, api_fresh) else {
        return unobserved_candidate(window, max_staleness_secs);
    };
    let mut credits: Option<SubscriptionQuotaCredits> = None;
    for snapshot in [sources.header.as_ref(), sources.api.as_ref()]
        .into_iter()
        .flatten()
    {
        if snapshot.sample_kind == SubscriptionQuotaSampleKind::Absent {
            continue;
        }
        if let Some(raw) = snapshot.credits.as_ref() {
            let mut incoming = raw.clone();
            incoming.stamp_observations(
                snapshot.observed_at_unix_millis,
                match snapshot.source {
                    MergedSource::Header => cc_lb_domain::CreditSource::Header,
                    MergedSource::Api => cc_lb_domain::CreditSource::Api,
                },
            );
            if let Some(current) = credits.as_mut() {
                current.merge_observations(&incoming);
            } else {
                credits = Some(incoming);
            }
        }
    }
    merged.credits = credits;
    let state = match merged.sample_kind {
        SubscriptionQuotaSampleKind::Absent => SubscriptionQuotaDataState::Absent,
        SubscriptionQuotaSampleKind::Sample => {
            if is_fresh(&merged, now_unix_millis, max_staleness_secs) {
                SubscriptionQuotaDataState::Fresh
            } else {
                SubscriptionQuotaDataState::Stale
            }
        }
    };
    candidate_from_snapshot(window, state, merged, max_staleness_secs)
}

fn merge_sources(
    sources: &SourceSnapshots,
    header_fresh: bool,
    api_fresh: bool,
) -> Option<MergedQuotaSnapshot> {
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
    //   4. Neither source has data → `None`.
    match (sources.header.as_ref(), sources.api.as_ref()) {
        (Some(header), Some(api)) => match (header_fresh, api_fresh) {
            (true, false) => Some(header.clone()),
            (false, true) => Some(api.clone()),
            _ => {
                if api.observed_at_unix_millis >= header.observed_at_unix_millis {
                    Some(api.clone())
                } else {
                    Some(header.clone())
                }
            }
        },
        (Some(header), None) => Some(header.clone()),
        (None, Some(api)) => Some(api.clone()),
        (None, None) => None,
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
        credits: snapshot.credits,
        observed_at_unix_millis: Some(snapshot.observed_at_unix_millis),
        max_staleness_secs,
        fallback_available: snapshot.fallback_available,
        overage_in_use: snapshot.overage_in_use,
        overage_period_monthly_utilization: snapshot.overage_period_monthly_utilization,
        upgrade_paths: snapshot.upgrade_paths,
    }
}

fn unobserved_candidate(
    window: SubscriptionQuotaWindow,
    max_staleness_secs: u64,
) -> SubscriptionQuotaCandidateSnapshot {
    SubscriptionQuotaCandidateSnapshot {
        window: window.as_str().to_owned(),
        state: SubscriptionQuotaDataState::Unobserved,
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
        credits: None,
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
            sample_kind: SubscriptionQuotaSampleKind::Sample,
            utilization: Some(utilization),
            status: Some(SubscriptionQuotaStatus::Allowed),
            resets_at_unix_secs: Some(observed_at_unix_millis / 1_000 + 3_600),
            surpassed_threshold: None,
            representative_claim: None,
            fallback_available: None,
            overage_in_use: None,
            overage_period_monthly_utilization: None,
            upgrade_paths: None,
            disabled_reason: None,
            extra_usage_enabled: None,
            extra_usage_monthly_limit: None,
            extra_usage_used_credits: None,
            credits: None,
            observed_at_unix_millis,
        }
    }

    fn record(
        sample_kind: SubscriptionQuotaSampleKind,
        observed_at_unix_millis: u64,
        utilization: Option<f64>,
    ) -> SubscriptionQuotaSample {
        SubscriptionQuotaSample {
            upstream_id: Uuid::nil(),
            window: SubscriptionQuotaWindow::SevenDay,
            source: SubscriptionQuotaSource::Api,
            sample_kind,
            observed_at_unix_millis,
            sample_id: Uuid::new_v4(),
            utilization,
            status: utilization.map(|_| SubscriptionQuotaStatus::Allowed),
            resets_at_unix_secs: utilization.map(|_| observed_at_unix_millis / 1_000 + 3_600),
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
            credits: None,
            extra_usage_used_credits: None,
            ingested_at_unix_millis: observed_at_unix_millis,
        }
    }

    #[test]
    fn poll_publication_rejects_older_request_even_with_later_observation_time() {
        let cache = SubscriptionQuotaCache::new();
        let old = cache.begin_poll();
        let fresh = cache.begin_poll();
        let refreshed = record(SubscriptionQuotaSampleKind::Sample, 1_000, Some(0.12));
        assert!(cache.publish_poll(Uuid::nil(), fresh, &[refreshed]));
        let delayed = record(SubscriptionQuotaSampleKind::Sample, 2_000, Some(1.0));
        assert!(!cache.publish_poll(Uuid::nil(), old, &[delayed]));
        assert_eq!(
            cache
                .snapshot_for_upstream(Uuid::nil(), 2_000, 60)
                .into_iter()
                .find(|snapshot| snapshot.window == "7d")
                .unwrap()
                .utilization,
            Some(0.12)
        );
    }

    #[test]
    fn poll_publication_accepts_later_request_when_older_request_publishes_first() {
        let cache = SubscriptionQuotaCache::new();
        let old = cache.begin_poll();
        let fresh = cache.begin_poll();
        let exhausted = record(SubscriptionQuotaSampleKind::Sample, 1_000, Some(1.0));
        assert!(cache.publish_poll(Uuid::nil(), old, &[exhausted]));
        let refreshed = record(SubscriptionQuotaSampleKind::Sample, 2_000, Some(0.12));
        assert!(cache.publish_poll(Uuid::nil(), fresh, &[refreshed]));
        assert_eq!(
            cache
                .snapshot_for_upstream(Uuid::nil(), 2_000, 60)
                .into_iter()
                .find(|snapshot| snapshot.window == "7d")
                .unwrap()
                .utilization,
            Some(0.12)
        );
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
        assert_eq!(result, Some(api));
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
        assert_eq!(result, Some(header));
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
        assert_eq!(result, Some(api));
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
        assert_eq!(result, Some(api));
    }

    #[test]
    fn source_label_matches_utilization_origin() {
        let stale_header = snapshot(MergedSource::Header, 0.01, 1_000);
        let fresh_api = snapshot(MergedSource::Api, 0.37, 5_000);
        let sources = SourceSnapshots {
            header: Some(stale_header),
            api: Some(fresh_api),
        };
        let result = merge_sources(&sources, true, true).expect("api snapshot selected");
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
        assert_eq!(result, Some(header));
    }

    #[test]
    fn only_api_returns_api_wholesale() {
        let api = snapshot(MergedSource::Api, 0.42, 2_000);
        let sources = SourceSnapshots {
            header: None,
            api: Some(api.clone()),
        };
        let result = merge_sources(&sources, false, true);
        assert_eq!(result, Some(api));
    }

    #[test]
    fn absent_remains_structurally_absent_after_freshness_window() {
        let mut absent = snapshot(MergedSource::Api, 0.0, 1_000);
        absent.sample_kind = SubscriptionQuotaSampleKind::Absent;
        absent.utilization = None;
        absent.status = None;
        absent.resets_at_unix_secs = None;
        let sources = SourceSnapshots {
            header: None,
            api: Some(absent),
        };

        let result =
            candidate_from_sources(SubscriptionQuotaWindow::SevenDay, &sources, 100_000, 1);

        assert_eq!(result.state, SubscriptionQuotaDataState::Absent);
        assert_eq!(result.observed_at_unix_millis, Some(1_000));
        assert!(result.utilization.is_none());
    }

    #[test]
    fn newer_sample_clears_prior_absence() {
        let cache = SubscriptionQuotaCache::new();
        cache.upsert_observation(
            Uuid::nil(),
            &record(SubscriptionQuotaSampleKind::Absent, 1_000, None),
        );
        cache.upsert_observation(
            Uuid::nil(),
            &record(SubscriptionQuotaSampleKind::Sample, 2_000, Some(0.2)),
        );

        let snapshots = cache.snapshot_for_upstream(Uuid::nil(), 2_000, 1);
        let seven_day = snapshots
            .iter()
            .find(|snapshot| snapshot.window == "7d")
            .expect("7d snapshot");
        assert_eq!(seven_day.state, SubscriptionQuotaDataState::Fresh);
        assert_eq!(seven_day.utilization, Some(0.2));
    }

    #[test]
    fn newer_stale_sample_wins_over_older_absence() {
        let header = snapshot(MergedSource::Header, 0.2, 2_000);
        let mut absent = snapshot(MergedSource::Api, 0.0, 1_000);
        absent.sample_kind = SubscriptionQuotaSampleKind::Absent;
        absent.utilization = None;
        absent.status = None;
        absent.resets_at_unix_secs = None;
        let sources = SourceSnapshots {
            header: Some(header),
            api: Some(absent),
        };

        let result =
            candidate_from_sources(SubscriptionQuotaWindow::SevenDay, &sources, 100_000, 1);

        assert_eq!(result.state, SubscriptionQuotaDataState::Stale);
        assert_eq!(result.source.as_deref(), Some("header"));
        assert_eq!(result.utilization, Some(0.2));
    }
}
