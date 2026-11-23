//! Pure candidate-snapshot merging and freshness classification over
//! durable or in-memory [`SubscriptionQuotaSample`] records.
//!
//! Shared between [`cc_lb_server`]'s in-memory `SubscriptionQuotaCache` and
//! [`cc_lb_admin`]/[`crate::subscription_quotas`] DB-only split reads so both
//! topologies compute identical quota snapshots and states.

use std::collections::HashMap;

use cc_lb_domain::{
    CreditSource, SubscriptionQuotaCandidateSnapshot, SubscriptionQuotaCredits,
    SubscriptionQuotaDataState,
};
use cc_lb_storage_api::{
    SubscriptionQuotaSample, SubscriptionQuotaSampleKind, SubscriptionQuotaSource,
    SubscriptionQuotaStatus, SubscriptionQuotaWindow,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MergedSource {
    Header,
    Api,
}

impl MergedSource {
    pub fn as_str(self) -> &'static str {
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

impl MergedQuotaSnapshot {
    pub fn from_record(record: &SubscriptionQuotaSample) -> Self {
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

#[derive(Clone, Debug, Default)]
pub struct SourceSnapshots {
    pub header: Option<MergedQuotaSnapshot>,
    pub api: Option<MergedQuotaSnapshot>,
}

impl SourceSnapshots {
    pub fn insert_if_newer(&mut self, record: &SubscriptionQuotaSample) {
        let mut incoming = MergedQuotaSnapshot::from_record(record);
        let slot = match record.source {
            SubscriptionQuotaSource::Header => &mut self.header,
            SubscriptionQuotaSource::Api => &mut self.api,
        };
        let accept = slot.as_ref().is_none_or(|current| {
            record.observed_at_unix_millis >= current.observed_at_unix_millis
        });
        if !accept {
            return;
        }
        if record.sample_kind == SubscriptionQuotaSampleKind::Sample {
            let source = match record.source {
                SubscriptionQuotaSource::Header => CreditSource::Header,
                SubscriptionQuotaSource::Api => CreditSource::Api,
            };
            if let Some(credits) = incoming.credits.as_mut() {
                credits.stamp_observations(record.observed_at_unix_millis, source);
            }
            if let Some(current) = slot.as_mut()
                && let Some(mut existing) = current.credits.take()
            {
                existing.stamp_observations(current.observed_at_unix_millis, source);
                if let Some(next) = incoming.credits.as_ref() {
                    existing.merge_observations(next);
                }
                incoming.credits = Some(existing);
            }
        }
        *slot = Some(incoming);
    }
}

/// Builds merged candidate snapshots across every canonical quota window from
/// durable [`SubscriptionQuotaSample`] records for one upstream.
pub fn candidate_snapshots_from_samples(
    samples: &[SubscriptionQuotaSample],
    now_unix_millis: u64,
    max_staleness_secs: u64,
) -> Vec<SubscriptionQuotaCandidateSnapshot> {
    let mut by_window = HashMap::<SubscriptionQuotaWindow, SourceSnapshots>::new();
    for sample in samples {
        by_window
            .entry(sample.window)
            .or_default()
            .insert_if_newer(sample);
    }
    SubscriptionQuotaWindow::all()
        .iter()
        .copied()
        .map(|window| match by_window.get(&window) {
            Some(sources) => {
                candidate_from_sources(window, sources, now_unix_millis, max_staleness_secs)
            }
            None => unobserved_candidate(window, max_staleness_secs),
        })
        .collect()
}

pub fn candidate_from_sources(
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
                    MergedSource::Header => CreditSource::Header,
                    MergedSource::Api => CreditSource::Api,
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

pub fn merge_sources(
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

pub fn candidate_from_snapshot(
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

pub fn unobserved_candidate(
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
    use uuid::Uuid;

    use super::*;

    fn sample_record(
        window: SubscriptionQuotaWindow,
        source: SubscriptionQuotaSource,
        sample_kind: SubscriptionQuotaSampleKind,
        observed_at_unix_millis: u64,
        utilization: Option<f64>,
    ) -> SubscriptionQuotaSample {
        SubscriptionQuotaSample {
            upstream_id: Uuid::nil(),
            window,
            source,
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
            extra_usage_used_credits: None,
            credits: None,
            ingested_at_unix_millis: observed_at_unix_millis,
        }
    }

    #[test]
    fn candidate_snapshots_from_samples_merges_sources_and_preserves_freshness() {
        let header = sample_record(
            SubscriptionQuotaWindow::FiveHour,
            SubscriptionQuotaSource::Header,
            SubscriptionQuotaSampleKind::Sample,
            99_000,
            Some(0.25),
        );
        let stale_api = sample_record(
            SubscriptionQuotaWindow::SevenDay,
            SubscriptionQuotaSource::Api,
            SubscriptionQuotaSampleKind::Sample,
            10_000,
            Some(0.80),
        );
        let absent = sample_record(
            SubscriptionQuotaWindow::SevenDayOpus,
            SubscriptionQuotaSource::Api,
            SubscriptionQuotaSampleKind::Absent,
            95_000,
            None,
        );

        let snapshots = candidate_snapshots_from_samples(&[header, stale_api, absent], 100_000, 10);
        let five_hour = snapshots.iter().find(|s| s.window == "5h").unwrap();
        assert_eq!(five_hour.state, SubscriptionQuotaDataState::Fresh);
        assert_eq!(five_hour.utilization, Some(0.25));
        assert_eq!(five_hour.source.as_deref(), Some("header"));

        let seven_day = snapshots.iter().find(|s| s.window == "7d").unwrap();
        assert_eq!(seven_day.state, SubscriptionQuotaDataState::Stale);
        assert_eq!(seven_day.utilization, Some(0.80));

        let opus = snapshots.iter().find(|s| s.window == "7d_opus").unwrap();
        assert_eq!(opus.state, SubscriptionQuotaDataState::Absent);

        let sonnet = snapshots.iter().find(|s| s.window == "7d_sonnet").unwrap();
        assert_eq!(sonnet.state, SubscriptionQuotaDataState::Unobserved);
    }
}
