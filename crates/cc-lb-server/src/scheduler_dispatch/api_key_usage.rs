//! `api_key_usage_poll`: reads Anthropic's org usage report for org-scoped
//! API-key upstreams and stores per-model buckets (plan §1.6, S4).
//!
//! One cycle per upstream sends at most two requests: a rolling 7-day `1h`
//! page and a month-to-date `1d` page, one page each. Legacy rows with no
//! usage state get one lazy usage-report probe instead. Before any outbound
//! call the cycle claims the upstream with a short storage lease, so a
//! regular tick and an overlapping immediate poll never both call the
//! provider; the outcome is a column-scoped write guarded by that claim and
//! by the spec revision read with the candidate, so a concurrent key replace
//! or Re-check wins.

use cc_lb_clock::unix_secs;
use cc_lb_control::anthropic_api_key::{
    OrgApiError, UsageBucketWidth, UsageFilter, UsageReportPage, UsageReportQuery, key_hint,
    resolve_api_key,
};
use cc_lb_pricing::{PriceCatalog, normalize_model_id};
use cc_lb_scheduler::error::Result as SchedulerResult;
use cc_lb_scheduler::jobs::api_key_usage_poll::{API_KEY_USAGE_POLL_JOB, ApiKeyUsagePollCronJob};
use cc_lb_scheduler::retry::JobOutcome;
use cc_lb_scheduler::scheduler_metrics::record_api_key_usage_poll_outcome;
use cc_lb_storage_api::usage_pruner::utc_month_start_unix_secs;
use cc_lb_storage_api::{
    ApiKeyOrgCapability, ApiKeyUsagePollCandidate, ApiKeyUsagePollOutcome, ApiUsageAttribution,
    StorageError, UpstreamApiKeyDetailsRecord, UpstreamApiUsageBucketWrite, UpstreamRecord,
    UpstreamStore,
};
use futures_util::StreamExt as _;
use std::time::Duration;
use tokio::time::Instant;
use uuid::Uuid;

use super::SchedulerDispatch;
use super::storage::storage_scheduler_error;

/// Interval used when the job is missing from `scheduler.recurring_jobs`.
const DEFAULT_INTERVAL_SECS: u64 = 300;
/// Candidates handled by one tick. Candidates come oldest poll attempt first,
/// so the overflow goes ahead of this tick's upstreams on a later tick.
const CANDIDATE_LIMIT: usize = 500;
/// Upstreams polled at once within a tick.
const POLL_CONCURRENCY: usize = 8;
/// Lease of one upstream's poll claim, counted from the clock read just
/// before the claim. It stays well below the poll interval, so a crashed
/// poller's upstream is reclaimed on a later tick instead of being polled
/// twice in one interval.
const CLAIM_LEASE_SECS: u64 = 60;
/// Monotonic budget for a claimed candidate's provider calls (two requests,
/// 10 s each with a bounded body read), counted from the claim's return.
const CLAIM_EXCHANGE_BUDGET_SECS: u64 = 30;
/// Lease time a provider call must leave unused when it ends: clock skew
/// between replicas (15 s) plus the second floored off a clock read.
const CLAIM_LEASE_MARGIN_SECS: u64 = 16;
// A claim that returns at once leaves room for the whole exchange budget.
const _: () = assert!(CLAIM_EXCHANGE_BUDGET_SECS + CLAIM_LEASE_MARGIN_SECS <= CLAIM_LEASE_SECS);
const HOUR_SECS: u64 = 3_600;
/// Rolling 7-day window of `1h` buckets, including the current hour.
const HOURLY_BUCKETS: u32 = 168;
/// Month-to-date `1d` buckets.
const DAILY_BUCKETS: u32 = 31;

/// Bounded per-upstream outcome; the label of `cclb_scheduler_api_key_usage_poll_total`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum PollLabel {
    Success,
    Probed,
    Denied,
    RateLimited,
    Failed,
    DecryptFailed,
    NotApplicable,
    NoSubject,
    Conflict,
    /// Another poller holds the upstream's claim; this cycle made no call.
    ClaimLost,
    /// The claim's lease had too little time left for a provider call, so
    /// the call was not started and the claim was released.
    LeaseSpent,
    StorageError,
}

impl PollLabel {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Probed => "probed",
            Self::Denied => "denied",
            Self::RateLimited => "rate_limited",
            Self::Failed => "failed",
            Self::DecryptFailed => "decrypt_failed",
            Self::NotApplicable => "not_applicable",
            Self::NoSubject => "no_subject",
            Self::Conflict => "conflict",
            Self::ClaimLost => "claim_lost",
            Self::LeaseSpent => "lease_spent",
            Self::StorageError => "storage_error",
        }
    }

    /// Whether the tick log counts this outcome as `not_ok`. Another poll
    /// owning the upstream (`claim_lost`) or a newer key, workspace, or host
    /// winning the race (`conflict`) is the design working, not a failure.
    const fn is_failure(self) -> bool {
        !matches!(
            self,
            Self::Success | Self::Probed | Self::NotApplicable | Self::ClaimLost | Self::Conflict
        )
    }
}

/// Times shared by every upstream in one tick.
#[derive(Clone, Copy, Debug)]
struct PollTimes {
    /// Wall clock when the tick started.
    now: u64,
    /// Scheduled tick time (never after `now`).
    tick: u64,
    interval: u64,
}

impl PollTimes {
    /// An on-time tick keeps the cadence: the next poll is one interval
    /// after the scheduled tick, so the following tick is due. A tick that
    /// runs half an interval or more late (a stalled producer or a queued
    /// backlog of elapsed ticks) counts from `now` instead, so the
    /// catch-up ticks behind it find the upstream not due and poll nothing.
    const fn next_regular_poll(self) -> u64 {
        self.next_regular_poll_from(self.now)
    }

    /// The regular deadline for a candidate whose poll ran at `now`, a
    /// fresh clock read at or after the tick-start `now`. A candidate that
    /// claims or finishes materially later than the tick start — queued
    /// behind `POLL_CONCURRENCY` slower candidates or held on a provider
    /// response — counts its next deadline from when it ran: a deadline
    /// computed from the tick start can already be expired by write time,
    /// which would make the next tick poll the same upstream again.
    const fn next_regular_poll_from(self, now: u64) -> u64 {
        let lateness = now.saturating_sub(self.tick);
        if lateness < self.interval / 2 {
            self.tick.saturating_add(self.interval)
        } else {
            now.saturating_add(self.interval)
        }
    }

    /// A 429 defers the next poll from `responded_at`, the clock read when
    /// the 429 arrived, not the tick start: a response that arrives late in
    /// the tick still waits the full `retry-after` (or one interval).
    fn after_rate_limit(self, responded_at: u64, retry_after_secs: Option<u64>) -> u64 {
        responded_at.saturating_add(retry_after_secs.unwrap_or(self.interval))
    }
}

/// When a claimed candidate may still start a provider call. Taken after
/// the claim returns, so time spent waiting on storage locks is charged to
/// the lease. A call starts only if it ends, at its own bound, both inside
/// the monotonic exchange budget and `CLAIM_LEASE_MARGIN_SECS` before the
/// lease (by a fresh wall-clock read) lets a peer reclaim the upstream.
/// Nothing relies on cancelling a request already sent.
#[derive(Clone, Copy, Debug)]
struct ClaimWindow {
    lease_until: u64,
    exchange_deadline: Instant,
}

impl ClaimWindow {
    fn after_claim(lease_until: u64, claimed_at: Instant, now_unix_secs: u64) -> Self {
        let usable = lease_until
            .saturating_sub(now_unix_secs)
            .saturating_sub(CLAIM_LEASE_MARGIN_SECS)
            .min(CLAIM_EXCHANGE_BUDGET_SECS);
        Self {
            lease_until,
            exchange_deadline: claimed_at
                .checked_add(Duration::from_secs(usable))
                .unwrap_or(claimed_at),
        }
    }

    /// Whether a call bounded by `call` may start at these clock readings.
    fn admits(self, call: Duration, now: Instant, now_unix_secs: u64) -> bool {
        let lease_left = Duration::from_secs(self.lease_until.saturating_sub(now_unix_secs));
        let needed = call.saturating_add(Duration::from_secs(CLAIM_LEASE_MARGIN_SECS));
        let fits_budget = self
            .exchange_deadline
            .checked_duration_since(now)
            .is_some_and(|left| left >= call);
        lease_left >= needed && fits_budget
    }
}

impl SchedulerDispatch {
    pub(super) async fn handle_api_key_usage_poll(
        &self,
        job: ApiKeyUsagePollCronJob,
    ) -> SchedulerResult<JobOutcome> {
        let now = unix_secs(self.clock.now());
        let times = PollTimes {
            now,
            tick: if job.tick_unix_secs == 0 {
                now
            } else {
                job.tick_unix_secs.min(now)
            },
            interval: self.api_key_usage_poll_interval_secs(),
        };
        let candidates = self
            .storage
            .list_api_key_usage_poll_candidates(now, job.upstream_id, CANDIDATE_LIMIT)
            .await
            .map_err(storage_scheduler_error)?;
        let polled = candidates.len();
        let labels: Vec<PollLabel> = futures_util::stream::iter(candidates)
            .map(|candidate| self.poll_api_key_candidate(candidate, times))
            .buffer_unordered(POLL_CONCURRENCY)
            .collect()
            .await;
        let mut failed = 0_usize;
        for label in labels {
            record_api_key_usage_poll_outcome(label.as_str(), 1);
            failed += usize::from(label.is_failure());
        }
        tracing::info!(
            tick_unix_secs = job.tick_unix_secs,
            only_upstream = ?job.upstream_id,
            polled,
            not_ok = failed,
            "api-key usage poll tick complete"
        );
        Ok(JobOutcome::Done)
    }

    fn api_key_usage_poll_interval_secs(&self) -> u64 {
        self.config
            .scheduler
            .recurring_jobs
            .get(API_KEY_USAGE_POLL_JOB)
            .map_or(DEFAULT_INTERVAL_SECS, |job| job.interval_secs)
    }

    async fn poll_api_key_candidate(
        &self,
        candidate: ApiKeyUsagePollCandidate,
        times: PollTimes,
    ) -> PollLabel {
        let ApiKeyUsagePollCandidate {
            mut upstream,
            details,
        } = candidate;
        let failures = details.as_ref().map_or(0, |row| row.usage_poll_failures);
        let key = resolve_api_key(&self.aead, &upstream);
        // Claim before any outbound call and before any outcome write: only
        // the claim owner polls this upstream and completes it.
        let legacy_key_hint = match (&details, &key) {
            (None, Ok(key)) => key_hint(key),
            _ => None,
        };
        let claim_token = Uuid::new_v4();
        // The lease starts at claim time, not tick start: a candidate queued
        // behind `POLL_CONCURRENCY` others claims late in the tick. Waiting
        // on storage locks still spends the lease, so the window for
        // provider calls is taken from fresh clock reads after the claim.
        let claimed_at = unix_secs(self.clock.now());
        let lease_until = claimed_at.saturating_add(CLAIM_LEASE_SECS);
        let hint = legacy_key_hint.as_deref().unwrap_or_default();
        let upstream_id = upstream.id;
        let claim_at_revision = |revision| {
            self.storage.claim_api_key_usage_poll(
                upstream_id,
                revision,
                claim_token,
                lease_until,
                hint,
                claimed_at,
            )
        };
        let mut claim = claim_at_revision(upstream.revision).await;
        if matches!(claim, Err(StorageError::Conflict { .. }))
            && let Some(current) = self.same_poll_inputs(&upstream).await
        {
            // A rename (or a disable and re-enable) since the candidate read
            // changes nothing this poll reads: claim against the current
            // revision. A disabled upstream is never re-claimed.
            claim = claim_at_revision(current.revision).await;
            upstream = current;
        }
        match claim {
            Ok(true) => {}
            Ok(false) => {
                tracing::debug!(
                    upstream_id = %upstream.id,
                    "api-key usage poll skipped: another poll holds this upstream"
                );
                return PollLabel::ClaimLost;
            }
            Err(StorageError::Conflict { .. }) => {
                tracing::debug!(
                    upstream_id = %upstream.id,
                    "api-key usage poll skipped: upstream changed before the claim"
                );
                return PollLabel::Conflict;
            }
            Err(error) => {
                tracing::warn!(
                    upstream_id = %upstream.id,
                    upstream_name = %upstream.name,
                    %error,
                    "api-key usage poll could not claim the upstream"
                );
                return PollLabel::StorageError;
            }
        }
        let window =
            ClaimWindow::after_claim(lease_until, Instant::now(), unix_secs(self.clock.now()));
        let mut outcome = ApiKeyUsagePollOutcome {
            attempted_at_unix_secs: times.now,
            usage_report_state: None,
            access_revision_seen: details.as_ref().map_or(0, |row| row.access_revision),
            consecutive_failures: failures,
            next_poll_after_unix_secs: Some(times.next_regular_poll()),
            last_success_at_unix_secs: None,
            legacy_key_hint,
            claim_token: Some(claim_token),
            buckets: Vec::new(),
        };
        let key = match key {
            Ok(key) => key,
            Err(reason) => {
                // Zero outbound calls; the log names the upstream only.
                tracing::warn!(
                    upstream_id = %upstream.id,
                    upstream_name = %upstream.name,
                    ?reason,
                    "api-key usage poll skipped: stored credential cannot be decrypted"
                );
                outcome.usage_report_state = Some(ApiKeyOrgCapability::Error);
                outcome.consecutive_failures = failures.saturating_add(1);
                return self
                    .write_api_key_poll_outcome(&upstream, outcome, times, PollLabel::DecryptFailed)
                    .await;
            }
        };
        let org_api_applies = upstream
            .base_url
            .as_ref()
            .is_none_or(|base_url| self.anthropic_org_api.applies_to(base_url));
        if !org_api_applies {
            outcome.usage_report_state = Some(ApiKeyOrgCapability::NotApplicable);
            outcome.consecutive_failures = 0;
            outcome.next_poll_after_unix_secs = None;
            return self
                .write_api_key_poll_outcome(&upstream, outcome, times, PollLabel::NotApplicable)
                .await;
        }
        let label = match details {
            Some(details) if details.usage_report_state.is_some() => {
                self.collect_api_key_usage(&upstream, &details, &key, times, window, &mut outcome)
                    .await
            }
            _ => {
                self.probe_api_key_usage_report(&upstream, &key, times, window, &mut outcome)
                    .await
            }
        };
        self.write_api_key_poll_outcome(&upstream, outcome, times, label)
            .await
    }

    /// Lazy P3 for a row whose usage state was never recorded: one
    /// usage-report probe decides `ok`, `denied`, or `error`.
    async fn probe_api_key_usage_report(
        &self,
        upstream: &UpstreamRecord,
        key: &str,
        times: PollTimes,
        window: ClaimWindow,
        outcome: &mut ApiKeyUsagePollOutcome,
    ) -> PollLabel {
        if !self.claim_admits_call(upstream, window) {
            return PollLabel::LeaseSpent;
        }
        let probe = self.anthropic_org_api.probe_usage_report(key, times.now);
        match tokio::time::timeout_at(window.exchange_deadline, probe)
            .await
            .unwrap_or(Err(OrgApiError::Timeout))
        {
            Ok(()) => {
                outcome.usage_report_state = Some(ApiKeyOrgCapability::Ok);
                outcome.consecutive_failures = 0;
                PollLabel::Probed
            }
            Err(error) => {
                let responded_at = unix_secs(self.clock.now());
                apply_org_error(upstream, error, times, responded_at, outcome);
                match error {
                    OrgApiError::Denied { .. } | OrgApiError::RateLimited { .. } => {}
                    _ => outcome.usage_report_state = Some(ApiKeyOrgCapability::Error),
                }
                org_error_label(error)
            }
        }
    }

    /// One regular cycle: the `1h` page, then the `1d` page.
    async fn collect_api_key_usage(
        &self,
        upstream: &UpstreamRecord,
        details: &UpstreamApiKeyDetailsRecord,
        key: &str,
        times: PollTimes,
        window: ClaimWindow,
        outcome: &mut ApiKeyUsagePollOutcome,
    ) -> PollLabel {
        let Some((attribution, subject_id, filter)) = usage_subject(upstream, details) else {
            tracing::info!(
                upstream_id = %upstream.id,
                upstream_name = %upstream.name,
                "api-key usage poll skipped: no resolved key id or workspace to report on"
            );
            return PollLabel::NoSubject;
        };
        let hour_start = times.now - times.now % HOUR_SECS;
        let windows = [
            (
                UsageBucketWidth::H1,
                hour_start.saturating_sub(u64::from(HOURLY_BUCKETS - 1) * HOUR_SECS),
                HOURLY_BUCKETS,
            ),
            (
                UsageBucketWidth::D1,
                utc_month_start_unix_secs(times.now),
                DAILY_BUCKETS,
            ),
        ];
        for (bucket_width, starting_at_unix_secs, limit) in windows {
            let query = UsageReportQuery {
                starting_at_unix_secs,
                ending_at_unix_secs: times.now.max(starting_at_unix_secs.saturating_add(1)),
                bucket_width,
                filter: filter.clone(),
                limit,
            };
            if !self.claim_admits_call(upstream, window) {
                return PollLabel::LeaseSpent;
            }
            let page = self.anthropic_org_api.usage_report(key, &query);
            match tokio::time::timeout_at(window.exchange_deadline, page)
                .await
                .unwrap_or(Err(OrgApiError::Timeout))
            {
                Ok(page) => {
                    if page.has_more {
                        tracing::debug!(
                            upstream_id = %upstream.id,
                            bucket_width = bucket_width.as_str(),
                            "api-key usage report has more pages; reading the first only"
                        );
                    }
                    append_buckets(
                        &self.price_catalog,
                        attribution,
                        &subject_id,
                        bucket_width,
                        page,
                        &mut outcome.buckets,
                    );
                }
                Err(error) => {
                    let responded_at = unix_secs(self.clock.now());
                    apply_org_error(upstream, error, times, responded_at, outcome);
                    return org_error_label(error);
                }
            }
        }
        outcome.consecutive_failures = 0;
        outcome.last_success_at_unix_secs = Some(times.now);
        PollLabel::Success
    }

    /// Whether the claim still covers one more provider call, by fresh reads
    /// of both clocks. A call that would not end, at its per-probe bound,
    /// inside the window is never started: an outer timeout cannot retract
    /// a request the HTTP client already sent. The caller's outcome write
    /// then releases the claim with no further call.
    fn claim_admits_call(&self, upstream: &UpstreamRecord, window: ClaimWindow) -> bool {
        let admitted = window.admits(
            self.anthropic_org_api.limits().per_probe,
            Instant::now(),
            unix_secs(self.clock.now()),
        );
        if !admitted {
            tracing::warn!(
                upstream_id = %upstream.id,
                upstream_name = %upstream.name,
                "api-key usage poll skipped a provider call: too little of the claim's lease is left"
            );
        }
        admitted
    }

    async fn write_api_key_poll_outcome(
        &self,
        upstream: &UpstreamRecord,
        mut outcome: ApiKeyUsagePollOutcome,
        times: PollTimes,
        label: PollLabel,
    ) -> PollLabel {
        // The outcome was built at the tick-start `now`; by the time a
        // candidate claimed behind `POLL_CONCURRENCY` slower polls and
        // finished, its tick-grid deadline may already be expired, which
        // would make the next tick poll the same upstream again. An outcome
        // still carrying the regular deadline is rebased on a fresh clock
        // read; a `429` deferral or a cleared schedule is left alone.
        let regular = times.next_regular_poll();
        if outcome.next_poll_after_unix_secs == Some(regular) {
            let ran_at = unix_secs(self.clock.now());
            if ran_at > times.now {
                outcome.next_poll_after_unix_secs = Some(times.next_regular_poll_from(ran_at));
            }
        }
        let mut written = self
            .storage
            .write_api_key_usage_poll_outcome(upstream.id, upstream.revision, &outcome)
            .await;
        if matches!(written, Err(StorageError::Conflict { .. }))
            && let Some(current) = self.same_poll_inputs(upstream).await
        {
            // A rename (or a disable and re-enable) mid-poll leaves the result
            // valid; the claim token still guards against a reclaim or access
            // write, and a disabled upstream drops the result.
            written = self
                .storage
                .write_api_key_usage_poll_outcome(upstream.id, current.revision, &outcome)
                .await;
        }
        match written {
            Ok(()) => label,
            Err(StorageError::Conflict { .. }) => {
                // The key, workspace, or host changed or the upstream was
                // deleted mid-tick, an access write released the claim, or
                // the lease expired and another poll reclaimed the upstream:
                // the newer state wins and this result is dropped.
                tracing::debug!(
                    upstream_id = %upstream.id,
                    "api-key usage poll result dropped: upstream changed during the poll"
                );
                PollLabel::Conflict
            }
            Err(error) => {
                tracing::warn!(
                    upstream_id = %upstream.id,
                    upstream_name = %upstream.name,
                    %error,
                    "api-key usage poll result could not be stored"
                );
                PollLabel::StorageError
            }
        }
    }

    /// The upstream as stored now, when its spec changed since `polled` but
    /// it is still enabled and nothing a poll reads changed (key, host,
    /// workspace, kind): a rename must not drop a valid result. `None` when
    /// the revision is the same (the conflict was the claim), the upstream is
    /// gone or disabled, or a polled input changed. Storage enforces the same
    /// `enabled` check atomically with the claim and the completion.
    async fn same_poll_inputs(&self, polled: &UpstreamRecord) -> Option<UpstreamRecord> {
        let current = UpstreamStore::get_by_id(self.storage.as_ref(), polled.id)
            .await
            .ok()
            .flatten()?;
        (current.revision != polled.revision
            && current.deleted_at_unix_secs.is_none()
            && current.enabled
            && current.kind == polled.kind
            && current.api_key_ciphertext == polled.api_key_ciphertext
            && current.base_url == polled.base_url
            && current.workspace_id == polled.workspace_id)
            .then_some(current)
    }
}

/// Usage-report subject: the resolved key id, else the selected workspace.
fn usage_subject(
    upstream: &UpstreamRecord,
    details: &UpstreamApiKeyDetailsRecord,
) -> Option<(ApiUsageAttribution, String, UsageFilter)> {
    if let Some(api_key_id) = details.api_key_id.as_ref() {
        return Some((
            ApiUsageAttribution::ApiKey,
            api_key_id.clone(),
            UsageFilter::ApiKey(api_key_id.clone()),
        ));
    }
    upstream.workspace_id.as_ref().map(|workspace_id| {
        (
            ApiUsageAttribution::Workspace,
            workspace_id.clone(),
            UsageFilter::Workspace(workspace_id.clone()),
        )
    })
}

/// Outcome columns for a failed org call. 401/403 demotes to `denied`
/// (guarded by `access_revision_seen`); 429 defers the next poll by
/// `retry-after` from `responded_at` without counting a failure; anything
/// else counts one consecutive failure.
fn apply_org_error(
    upstream: &UpstreamRecord,
    error: OrgApiError,
    times: PollTimes,
    responded_at: u64,
    outcome: &mut ApiKeyUsagePollOutcome,
) {
    match error {
        OrgApiError::Denied { .. } => {
            outcome.usage_report_state = Some(ApiKeyOrgCapability::Denied);
            outcome.next_poll_after_unix_secs = None;
        }
        OrgApiError::RateLimited { retry_after_secs } => {
            outcome.next_poll_after_unix_secs =
                Some(times.after_rate_limit(responded_at, retry_after_secs));
        }
        OrgApiError::Upstream { .. }
        | OrgApiError::Timeout
        | OrgApiError::Unreachable
        | OrgApiError::Malformed => {
            outcome.consecutive_failures = outcome.consecutive_failures.saturating_add(1);
        }
    }
    tracing::warn!(
        upstream_id = %upstream.id,
        upstream_name = %upstream.name,
        %error,
        "api-key usage poll failed"
    );
}

const fn org_error_label(error: OrgApiError) -> PollLabel {
    match error {
        OrgApiError::Denied { .. } => PollLabel::Denied,
        OrgApiError::RateLimited { .. } => PollLabel::RateLimited,
        OrgApiError::Upstream { .. }
        | OrgApiError::Timeout
        | OrgApiError::Unreachable
        | OrgApiError::Malformed => PollLabel::Failed,
    }
}

fn append_buckets(
    catalog: &PriceCatalog,
    attribution: ApiUsageAttribution,
    subject_id: &str,
    bucket_width: UsageBucketWidth,
    page: UsageReportPage,
    out: &mut Vec<UpstreamApiUsageBucketWrite>,
) {
    for bucket in page.buckets {
        for result in bucket.results {
            let model = result.model.unwrap_or_default();
            let list_cost_micros = list_cost_micros(
                catalog,
                &model,
                result.uncached_input_tokens,
                result.output_tokens,
                result.cache_creation_5m_input_tokens,
                result.cache_creation_1h_input_tokens,
                result.cache_read_input_tokens,
            );
            out.push(UpstreamApiUsageBucketWrite {
                attribution,
                subject_id: subject_id.to_owned(),
                bucket_width_secs: bucket_width.secs(),
                bucket_start_unix_secs: bucket.starting_at_unix_secs,
                model,
                uncached_input_tokens: result.uncached_input_tokens,
                cache_creation_5m_input_tokens: result.cache_creation_5m_input_tokens,
                cache_creation_1h_input_tokens: result.cache_creation_1h_input_tokens,
                cache_read_input_tokens: result.cache_read_input_tokens,
                output_tokens: result.output_tokens,
                list_cost_micros,
            });
        }
    }
}

/// List-price cost at standard tier from the installed catalog. An unknown
/// or missing model costs 0. Cache writes are priced only when the catalog
/// has a 5-minute cache-write price, matching the request-cost path; the
/// 1-hour write is twice the base input price.
fn list_cost_micros(
    catalog: &PriceCatalog,
    model: &str,
    uncached_input_tokens: u64,
    output_tokens: u64,
    cache_creation_5m_input_tokens: u64,
    cache_creation_1h_input_tokens: u64,
    cache_read_input_tokens: u64,
) -> u64 {
    if model.is_empty() {
        return 0;
    }
    let snapshot = catalog.current();
    let normalized = normalize_model_id(model);
    let Some(pricing) = snapshot.models.get(&normalized) else {
        return 0;
    };
    let input_price = pricing.input_per_million_usd.as_micros_usd();
    let component = |tokens: u64, price_micros_per_million: u64| {
        u128::from(tokens) * u128::from(price_micros_per_million) / 1_000_000
    };
    let mut total = component(uncached_input_tokens, input_price)
        + component(
            output_tokens,
            pricing.output_per_million_usd.as_micros_usd(),
        );
    if let Some(price_5m) = snapshot.cache_creation_per_million_usd.get(&normalized) {
        total += component(cache_creation_5m_input_tokens, price_5m.as_micros_usd())
            + component(
                cache_creation_1h_input_tokens,
                cc_lb_pricing::cache_creation_1h_micros_from_input(input_price),
            );
    }
    if let Some(price_read) = snapshot.cache_read_per_million_usd.get(&normalized) {
        total += component(cache_read_input_tokens, price_read.as_micros_usd());
    }
    u64::try_from(total).unwrap_or(u64::MAX)
}

#[cfg(all(test, feature = "sqlite"))]
#[path = "api_key_usage/tests.rs"]
mod tests;
