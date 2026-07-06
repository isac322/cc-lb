//! Subscription-preference router filter.
//!
//! Selects at most one OAuth upstream per request via:
//!
//! 1. A strict tier ordering (KnownBase > PartialBase > Overage > UnknownProbe)
//!    that keeps the Anthropic base plan strictly higher priority than the
//!    overage bucket.
//! 2. Within the winning tier, a Weighted Rendezvous Hash (WRH, Vilkonis) using
//!    per-candidate urgency as the weight. Urgency for base tiers is
//!    `capacity_multiplier * (1 - util)^2 / remaining_secs`, taken as the max
//!    over relevant base windows. Overage-tier urgency uses a fixed nominal
//!    30-day denominator and does not apply the capacity multiplier.
//! 3. A deterministic tiebreak if two candidates produce numerically identical
//!    WRH scores (rendezvous_hash DESC, upstream_id ASC).
//!
//! ## Design rationale
//!
//! The previous algorithm used a scalar `min_headroom + positive_ratio -
//! warning_penalty` score with a rendezvous hash as a secondary tiebreak. In
//! practice `f64::total_cmp` on the score never tied, so the rendezvous hash
//! never fired and traffic funnelled to whichever candidate had the highest
//! headroom. See the production trace at 2026-07-04 where 78% of cache-miss
//! traffic landed on `bear-max` even though four upstreams were healthy.
//!
//! WRH restores load spread. The urgency weight biases the distribution
//! towards candidates that will hit their reset first, and the capacity
//! multiplier lifts small-plan upstreams so their effective "burnable minutes
//! remaining" competes fairly with the large Max/Team plans they otherwise
//! lose to on raw headroom.
//!
//! ## Windows
//!
//! Base: `5h` and `7d`. Model-specific `7d_sonnet` and `7d_opus` labels are
//! deliberately ignored because they are not stable enough to drive routing.
//! `unified` is not itself an exhaustion window; its top-level flags
//! (`overage_in_use`, `fallback_available`, `extra_usage_*`) enrich the
//! overage assessment.
//!
//! ## Reset semantics
//!
//! - Fresh + `resets_at` in the future: window contributes to urgency.
//! - Fresh without `resets_at`: window excluded from urgency (Q4).
//! - Stale + `rejected` + future reset: hard negative (rejection still live).
//! - Stale + `rejected` + past reset: unknown (rejection expired).

use cc_lb_plugin_api::types::{CachePricingSummary, WrhKeySource};
use cc_lb_plugin_api::{
    BUILTIN_SUBSCRIPTION_PREFERENCE_ID, BUILTIN_SUBSCRIPTION_PREFERENCE_NAME, FilterError,
    FilterOutput, FilterPlugin, Principal, RequestContext, SubscriptionQuotaCandidateSnapshot,
    SubscriptionQuotaDataState, SubscriptionTier as PublicTier, UpstreamCandidate, UpstreamKind,
};
use parking_lot::Mutex;
use sha2::{Digest, Sha256};
use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use uuid::Uuid;

// -- Reason strings surfaced on FilterOutput.reason (log/audit surface). -----

pub(crate) const SUBSCRIPTION_ALIVE_REASON: &str = "keep:best_subscription_candidate";
pub(crate) const API_KEY_FALLBACK_REASON: &str = "keep:api_key_subscription_exhausted";
pub(crate) const NO_API_KEY_REASON: &str = "keep:subscription_exhausted_no_api_key";
pub(crate) const NO_SUBSCRIPTION_REASON: &str = "keep:no_subscription_candidates";

// -- Window labels. ----------------------------------------------------------

pub(crate) const WINDOW_FIVE_HOUR: &str = "5h";
pub(crate) const WINDOW_SEVEN_DAY: &str = "7d";
pub(crate) const WINDOW_OVERAGE: &str = "overage";
pub(crate) const WINDOW_UNIFIED: &str = "unified";

// -- Algorithm constants. ---------------------------------------------------

/// Exponent applied to per-window headroom `(1 - util)` before dividing by
/// remaining seconds. Q5 requires strong bias against near-full candidates.
pub(crate) const HEADROOM_EXPONENT: i32 = 2;

/// Upper bound on the plan capacity multiplier. Prevents 20x plans from
/// dominating pure headroom math; anything above cap saturates.
pub(crate) const CAPACITY_CAP: f64 = 2.0;

/// Fallback capacity ratio when the upstream has no plan metadata cached
/// (either the OAuth org poll has never completed or the plan is not in the
/// classification table).
pub(crate) const UNKNOWN_CAPACITY_RATIO: f64 = 1.0;

/// Floor on the remaining-seconds denominator, so a resets-at-in-3-seconds
/// candidate does not blow past finite arithmetic.
pub(crate) const MIN_REMAIN_SECS: u64 = 60;

/// Guard below which the aggregate WRH weight is treated as zero and the
/// filter falls back to a uniform distribution.
pub(crate) const EPSILON: f64 = 1e-12;

/// Nominal remaining-seconds denominator for overage-tier urgency. Anthropic
/// overage windows do not carry a reliable `resets_at`; billing rolls over on
/// the monthly boundary, so we use a fixed 30-day nominal.
pub(crate) const OVERAGE_REMAINING_NOMINAL_SECS: u64 = 30 * 86_400;

/// Baseline WRH weight for overage candidates whose utilization we cannot
/// read.
pub(crate) const OVERAGE_UNKNOWN_WEIGHT: f64 = 0.5;

/// Maximum raw `thread_id` bytes accepted as an in-memory routing key. Longer
/// caller-controlled values are hashed before WRH and TierMemory use them, so
/// entry count and key bytes are both bounded.
const MAX_THREAD_ROUTING_KEY_BYTES: usize = 256;

const HASHED_THREAD_ROUTING_KEY_PREFIX: &str = "sha256:";

/// A challenger must beat the current threaded owner by this score ratio for
/// consecutive observations before a non-forced handoff. Lower WRH scores win.
const SWITCH_SCORE_MARGIN: f64 = 1.25;

/// Number of consecutive margin wins required before switching a threaded
/// owner. This prevents one transient score crossing from seeding a new cache.
const SUCCESSOR_CONVERGENCE_OBSERVATIONS: u8 = 2;

/// Short version tag identifying the WRH salt/algorithm shape. Embedded in
/// [`RENDEZVOUS_SALT`] and surfaced on `SubscriptionPreferenceTrace.rendezvous_salt_version`
/// so trace queries can distinguish upstream-mix shifts caused by an
/// algorithm/salt change from those caused by upstream or quota state
/// changes. Bumped in lockstep with `RENDEZVOUS_SALT` — a `debug_assert`
/// in `tests::rendezvous_salt_embeds_version` guards the invariant.
pub(crate) const SALT_VERSION: &str = "v9";

/// Salt for the weighted-rendezvous hash. Bumped as a version stamp when the
/// selection algorithm changes shape; older salts must never be reused.
///
/// v9 (2026-07-06): warning-positive base windows remain KnownBase with a
/// same-tier multiplier, and threaded owner handoff is additionally gated by
/// estimated in-memory cache re-prime cost derived from plugin-visible pricing
/// and cache-score inputs.
///
/// v8 (2026-07-06): WRH is keyed on non-empty `thread_id`, falling back to
/// `request_id` only for stateless requests. Fresh base windows with
/// `allowed_warning`, or with `allowed` plus finite utilization at/above a
/// finite `surpassed_threshold`, remain selectable but demote the candidate
/// from KnownBase to PartialBase. Threaded requests keep a per-thread owner and
/// require a converged challenger before non-forced handoff. See
/// docs/adr/0005-thread-keyed-subscription-preference-with-warning-demotion.md.
///
/// v7 (2026-07-06): WRH is keyed on `request_id` and the per-candidate
/// weight is `quota_urgency * exp(CACHE_LOG_BOOST * cache_ratio)`, where
/// `cache_ratio` is `predicted_cache_read_tokens / max_in_tier_bucket`.
/// The exponential multiplier makes a deeply cached upstream keep winning
/// through ~95% utilisation and hand off probabilistically around 99%
/// (`CACHE_LOG_BOOST` is calibrated for a 99% crossover in the observed
/// bear-max/Runbear scenario). This replaces the v6 cache_affinity
/// max-ranker filter, which dropped every non-max cache candidate before
/// subscription-preference could see it — when the max-cache upstream
/// became tier-blocked (util ≥ 1.0), there was no fallback candidate left
/// to route to and the request hit 429. v7 keeps every candidate in the
/// bucket and lets tier assessment plus the cache-weighted WRH handle both
/// warm-session pinning and hard tier-eviction spill. See
/// docs/adr/0004-cache-weighted-subscription-preference.md and
/// docs/rfc/0003-cache-weighted-subscription-preference.md.
///
/// v6 (2026-07-06): WRH keyed on `request_id`, cache handling delegated to
/// a separate cache_affinity max-ranker filter. Rejected once the
/// tier-eviction spill bug was found: dropping non-max candidates before
/// subscription's tier assessment meant a saturated cache-holder had no
/// peer left to spill to.
///
/// v5 (rejected, never deployed): would have made subscription-preference
/// inspect candidate `cache_score` to switch between thread-id and
/// request-id keying. Rejected for violating filter separation.
///
/// v4 (2026-07-05): keyed WRH on `thread_id` so multi-turn sessions pinned
/// while the prompt cache stayed warm. Deployed by PR #322 after the
/// 2026-07-05 06:24 UTC scatter incident.
const RENDEZVOUS_SALT: &str =
    "cc-lb:subscription-preference:v9:dollar-cache-switch-gate:2026-07-06";

const CACHE_COST_BASIS_VERSION: &str = "v1";
const WARNING_MULTIPLIER: f64 = 0.20;
const NORMAL_SWITCH_CACHE_LOSS_BUDGET_MICROS: u64 = 50_000;

/// Exponent coefficient on the cache-weighted WRH multiplier.
/// `cache_weight_multiplier_i = exp(CACHE_LOG_BOOST * cache_ratio_i)` sits on
/// top of the quota urgency so a deeply cached upstream wins the intra-tier
/// draw through ~95% utilisation and hands off probabilistically around 99%.
///
/// Calibration: `ln(8100) / 0.94`. Chosen so that bear-max at util=0.99 with
/// cache_ratio=1.0 has the same `effective_weight` as Runbear at util=0.10
/// with cache_ratio=0.06 (i.e. only the shared BP0 hash), same
/// `remaining_secs` and `capacity_multiplier`. That is the crossover point
/// observed in the 2026-07-05 / 2026-07-06 ses_0d40 incidents — see
/// docs/adr/0004 for the numeric derivation. Changing this constant without
/// re-deriving that calibration will shift where the fresh-quota peer starts
/// winning; test `cache_boost_calibration_at_99_percent` guards the equality.
pub const CACHE_LOG_BOOST: f64 = 9.574_063_128_362_267;

// -- Tier memory (per-filter, per-DynamicView). -----------------------------

/// Retention window for a thread's last routing state.
/// Chosen to comfortably cover a typical multi-turn conversation without
/// pinning stale sessions in memory forever.
const TIER_MEMORY_TTL: Duration = Duration::from_secs(30 * 60);

/// Upper bound on the number of thread routing-state records the
/// per-filter memory keeps at once. When both this bound is hit AND all
/// existing records are fresh (no TTL evictions available), new insertions
/// are dropped rather than evicting a live record. This trades owner-memory
/// completeness for hard entry-count bounds: a dropped insertion falls back to
/// formula selection until another record can be accepted.
const TIER_MEMORY_CAP: usize = 10_000;

struct TierEntry {
    tier: PublicTier,
    owner_upstream_id: Option<Uuid>,
    pending_successor: Option<PendingSuccessor>,
    expires_at: Instant,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PendingSuccessor {
    upstream_id: Uuid,
    observations: u8,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct TierEntrySnapshot {
    tier: PublicTier,
    owner_upstream_id: Option<Uuid>,
}

/// Per-filter map from normalized thread routing key to previous tier, owner,
/// and pending successor. Shared by every `evaluate` call the same
/// `SubscriptionPreferenceFilter` instance handles; a `DynamicView` rebuild
/// replaces the filter and clears owner memory, after which routing falls back
/// to the cache-weighted WRH formula until the next owner is observed.
pub struct TierMemory {
    cap: usize,
    ttl: Duration,
    inner: Mutex<HashMap<String, TierEntry>>,
}

impl TierMemory {
    pub fn new() -> Self {
        Self::with_cap_and_ttl(TIER_MEMORY_CAP, TIER_MEMORY_TTL)
    }

    pub fn with_cap_and_ttl(cap: usize, ttl: Duration) -> Self {
        Self {
            cap,
            ttl,
            inner: Mutex::new(HashMap::new()),
        }
    }

    /// Return the last tier this memory saw for `thread_id`, provided the
    /// record has not aged past `ttl`. Returns `None` for absent, expired,
    /// or empty `thread_id`.
    pub fn get(&self, thread_id: &str, now: Instant) -> Option<PublicTier> {
        self.snapshot(thread_id, now).map(|entry| entry.tier)
    }

    /// Insert or refresh the tier for `thread_id`. Best-effort: when the
    /// map is at cap AND all existing entries are still fresh, the new
    /// key is dropped rather than evicting a live entry. Empty
    /// `thread_id` is a no-op.
    pub fn insert(&self, thread_id: &str, tier: PublicTier, now: Instant) {
        self.insert_owner(thread_id, tier, None, now);
    }

    fn snapshot(&self, thread_id: &str, now: Instant) -> Option<TierEntrySnapshot> {
        if thread_id.is_empty() {
            return None;
        }
        let thread_key = normalized_thread_routing_key(thread_id);
        let map = self.inner.lock();
        let entry = map.get(thread_key.as_ref())?;
        if entry.expires_at > now {
            Some(TierEntrySnapshot {
                tier: entry.tier,
                owner_upstream_id: entry.owner_upstream_id,
            })
        } else {
            None
        }
    }

    fn insert_owner(
        &self,
        thread_id: &str,
        tier: PublicTier,
        owner_upstream_id: Option<Uuid>,
        now: Instant,
    ) {
        if thread_id.is_empty() {
            return;
        }
        let thread_key = normalized_thread_routing_key(thread_id);
        let mut map = self.inner.lock();
        let expires_at = now + self.ttl;
        if let Some(entry) = map.get_mut(thread_key.as_ref()) {
            entry.tier = tier;
            entry.owner_upstream_id = owner_upstream_id;
            entry.pending_successor = None;
            entry.expires_at = expires_at;
            return;
        }
        if map.len() >= self.cap {
            map.retain(|_, e| e.expires_at > now);
        }
        if map.len() >= self.cap {
            return;
        }
        map.insert(
            thread_key.into_owned(),
            TierEntry {
                tier,
                owner_upstream_id,
                pending_successor: None,
                expires_at,
            },
        );
    }

    fn choose_thread_owner(
        &self,
        thread_id: &str,
        tier: PublicTier,
        formula_winner: Uuid,
        incumbent: Option<ThreadIncumbent>,
        now: Instant,
    ) -> ThreadOwnerDecision {
        if thread_id.is_empty() {
            return ThreadOwnerDecision {
                kept_upstream_id: formula_winner,
                switch_gate_reason: "stateless_formula_winner",
            };
        }
        let thread_key = normalized_thread_routing_key(thread_id);
        let mut map = self.inner.lock();
        let expires_at = now + self.ttl;
        let Some(entry) = map
            .get_mut(thread_key.as_ref())
            .filter(|entry| entry.expires_at > now)
        else {
            if map.len() >= self.cap {
                map.retain(|_, e| e.expires_at > now);
            }
            if map.len() < self.cap {
                map.insert(
                    thread_key.into_owned(),
                    TierEntry {
                        tier,
                        owner_upstream_id: Some(formula_winner),
                        pending_successor: None,
                        expires_at,
                    },
                );
            }
            return ThreadOwnerDecision {
                kept_upstream_id: formula_winner,
                switch_gate_reason: "no_previous_owner",
            };
        };

        if entry.tier != tier {
            entry.tier = tier;
            entry.owner_upstream_id = Some(formula_winner);
            entry.pending_successor = None;
            entry.expires_at = expires_at;
            return ThreadOwnerDecision {
                kept_upstream_id: formula_winner,
                switch_gate_reason: "tier_changed",
            };
        }

        let Some(incumbent) = incumbent else {
            entry.tier = tier;
            entry.owner_upstream_id = Some(formula_winner);
            entry.pending_successor = None;
            entry.expires_at = expires_at;
            return ThreadOwnerDecision {
                kept_upstream_id: formula_winner,
                switch_gate_reason: "no_incumbent_in_tier",
            };
        };

        if incumbent.upstream_id == formula_winner {
            entry.tier = tier;
            entry.owner_upstream_id = Some(incumbent.upstream_id);
            entry.pending_successor = None;
            entry.expires_at = expires_at;
            return ThreadOwnerDecision {
                kept_upstream_id: incumbent.upstream_id,
                switch_gate_reason: "formula_winner_is_incumbent",
            };
        }

        if !incumbent.challenger_beats_margin {
            entry.tier = tier;
            entry.owner_upstream_id = Some(incumbent.upstream_id);
            entry.pending_successor = None;
            entry.expires_at = expires_at;
            return ThreadOwnerDecision {
                kept_upstream_id: incumbent.upstream_id,
                switch_gate_reason: "margin_blocked",
            };
        }

        if !incumbent.cache_loss_allows_switch {
            entry.tier = tier;
            entry.owner_upstream_id = Some(incumbent.upstream_id);
            entry.pending_successor = None;
            entry.expires_at = expires_at;
            return ThreadOwnerDecision {
                kept_upstream_id: incumbent.upstream_id,
                switch_gate_reason: incumbent.switch_gate_reason,
            };
        }

        let observations = match entry.pending_successor {
            Some(pending) if pending.upstream_id == formula_winner => pending.observations + 1,
            Some(_) | None => 1,
        };
        if observations >= SUCCESSOR_CONVERGENCE_OBSERVATIONS {
            entry.tier = tier;
            entry.owner_upstream_id = Some(formula_winner);
            entry.pending_successor = None;
            entry.expires_at = expires_at;
            ThreadOwnerDecision {
                kept_upstream_id: formula_winner,
                switch_gate_reason: "successor_converged",
            }
        } else {
            entry.tier = tier;
            entry.owner_upstream_id = Some(incumbent.upstream_id);
            entry.pending_successor = Some(PendingSuccessor {
                upstream_id: formula_winner,
                observations,
            });
            entry.expires_at = expires_at;
            ThreadOwnerDecision {
                kept_upstream_id: incumbent.upstream_id,
                switch_gate_reason: "successor_pending",
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.inner.lock().len()
    }

    #[cfg(test)]
    pub(crate) fn max_key_len(&self) -> Option<usize> {
        self.inner.lock().keys().map(String::len).max()
    }
}

impl Default for TierMemory {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for TierMemory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TierMemory")
            .field("cap", &self.cap)
            .field("ttl", &self.ttl)
            .field("entries", &self.inner.lock().len())
            .finish()
    }
}

// -- Config knobs (compiled defaults today; expose per-principal later). ----

struct FilterConfig {
    unknown_probe_enabled: bool,
    stale_rejected_without_reset_blocks: bool,
    hard_overage_block_wins: bool,
    rendezvous_hash_salt: &'static str,
}

impl Default for FilterConfig {
    fn default() -> Self {
        Self {
            unknown_probe_enabled: true,
            stale_rejected_without_reset_blocks: true,
            hard_overage_block_wins: true,
            rendezvous_hash_salt: RENDEZVOUS_SALT,
        }
    }
}

// -- Public plugin type. ----------------------------------------------------

#[derive(Clone, Debug, Default)]
pub struct SubscriptionPreferenceFilter {
    tier_memory: Arc<TierMemory>,
}

impl SubscriptionPreferenceFilter {
    pub fn new() -> Self {
        Self::default()
    }

    #[cfg(test)]
    pub(crate) fn with_tier_memory(tier_memory: Arc<TierMemory>) -> Self {
        Self { tier_memory }
    }

    #[cfg(test)]
    pub(crate) fn tier_memory(&self) -> &Arc<TierMemory> {
        &self.tier_memory
    }
}

impl FilterPlugin for SubscriptionPreferenceFilter {
    fn filter(
        &self,
        ctx: &RequestContext,
        _principal: &Principal,
        candidates: &[UpstreamCandidate],
    ) -> Result<FilterOutput, FilterError> {
        let config = FilterConfig::default();
        Ok(evaluate(
            ctx,
            candidates,
            &config,
            &self.tier_memory,
            Instant::now(),
        ))
    }

    fn plugin_id(&self) -> Uuid {
        BUILTIN_SUBSCRIPTION_PREFERENCE_ID
    }

    fn plugin_name(&self) -> &str {
        BUILTIN_SUBSCRIPTION_PREFERENCE_NAME
    }
}

// -- Evaluation entry point. ------------------------------------------------

fn evaluate(
    ctx: &RequestContext,
    candidates: &[UpstreamCandidate],
    config: &FilterConfig,
    tier_memory: &TierMemory,
    now: Instant,
) -> FilterOutput {
    let canonical_model = ctx.canonical_model_id.as_str();
    let base_windows = relevant_base_windows(canonical_model);

    let has_subscription = candidates
        .iter()
        .any(|c| c.kind == UpstreamKind::AnthropicOauth);
    let has_api_key = candidates
        .iter()
        .any(|c| c.kind == UpstreamKind::AnthropicApiKey);

    if !has_subscription {
        return FilterOutput {
            kept_upstream_ids: collect_kind(candidates, UpstreamKind::AnthropicApiKey),
            reason: NO_SUBSCRIPTION_REASON.to_owned(),
            per_candidate_reasons: Vec::new(),
            subscription_preference: None,
            cache_affinity: None,
        };
    }

    let mut buckets: [Vec<Assessment<'_>>; 4] = [Vec::new(), Vec::new(), Vec::new(), Vec::new()];
    for (index, candidate) in candidates.iter().enumerate() {
        if candidate.kind != UpstreamKind::AnthropicOauth {
            continue;
        }
        if let Some(assessment) = assess_candidate(candidate, index, &base_windows, config) {
            buckets[assessment.tier as usize].push(assessment);
        }
    }

    let all_assessments: Vec<cc_lb_plugin_api::CandidateUrgency> = buckets
        .iter()
        .flat_map(|bucket| {
            let max_cache_tokens_in_bucket: u32 = bucket
                .iter()
                .map(candidate_cache_read_tokens)
                .max()
                .unwrap_or(0);
            let total_urgency_in_bucket: f64 = bucket.iter().map(|a| a.urgency).sum();
            let uniform_fallback = total_urgency_in_bucket < EPSILON;
            bucket.iter().map(move |a| {
                let cache_tokens = candidate_cache_read_tokens(a);
                let cache_ratio =
                    cache_ratio_within_bucket(cache_tokens, max_cache_tokens_in_bucket);
                let cache_weight_multiplier = (CACHE_LOG_BOOST * cache_ratio).exp();
                let quota_weight = if uniform_fallback { 1.0 } else { a.urgency };
                let effective_weight =
                    quota_weight * cache_weight_multiplier * a.warning_multiplier;
                let estimated_input_cost_micros =
                    estimate_candidate_input_cost_micros(a.candidate, &ctx.cache_pricing)
                        .unwrap_or(0);
                let cache_savings_ratio = cache_savings_ratio(a.candidate, &ctx.cache_pricing);
                cc_lb_plugin_api::CandidateUrgency {
                    upstream_id: a.candidate.upstream_id,
                    tier: tier_to_plugin_api(a.tier),
                    urgency: effective_weight,
                    quota_urgency: a.urgency,
                    predicted_cache_read_tokens: cache_tokens,
                    predicted_cache_creation_tokens_5m: candidate_cache_creation_tokens_5m(a),
                    predicted_cache_creation_tokens_1h: candidate_cache_creation_tokens_1h(a),
                    predicted_uncached_input_tokens: candidate_uncached_input_tokens(a),
                    cache_ratio,
                    cache_weight_multiplier,
                    warning_multiplier: a.warning_multiplier,
                    cache_savings_ratio,
                    estimated_input_cost_micros,
                    effective_weight,
                }
            })
        })
        .collect();

    let session_thread_key = ctx
        .thread_id
        .as_deref()
        .filter(|id| !id.is_empty())
        .map(normalized_thread_routing_key);
    let routing_key = session_thread_key
        .as_deref()
        .unwrap_or(ctx.request_id.as_str());
    let wrh_key_source = match session_thread_key.as_ref() {
        Some(_) => WrhKeySource::ThreadId,
        None => WrhKeySource::RequestId,
    };

    for bucket in buckets.iter() {
        if bucket.is_empty() {
            continue;
        }
        let selection = pick_within_tier(bucket, routing_key, config);
        let formula_winner = selection.winner;
        let chosen_tier = tier_to_plugin_api(formula_winner.tier);
        let previous_entry = session_thread_key
            .as_deref()
            .and_then(|thread_id| tier_memory.snapshot(thread_id, now));
        let previous_tier = previous_entry.map(|entry| entry.tier);
        let incumbent = previous_entry
            .and_then(|entry| entry.owner_upstream_id)
            .and_then(|owner_id| {
                bucket
                    .iter()
                    .find(|assessment| assessment.candidate.upstream_id == owner_id)
            })
            .map(|owner| {
                let challenger_key = wrh_key(
                    formula_winner,
                    routing_key,
                    config,
                    selection.uniform,
                    selection.max_cache_tokens,
                );
                let incumbent_key = wrh_key(
                    owner,
                    routing_key,
                    config,
                    selection.uniform,
                    selection.max_cache_tokens,
                );
                let cache_loss_gate = cache_loss_gate(owner, formula_winner, &ctx.cache_pricing);
                ThreadIncumbent {
                    upstream_id: owner.candidate.upstream_id,
                    challenger_beats_margin: formula_winner.candidate.upstream_id
                        != owner.candidate.upstream_id
                        && challenger_key.score * SWITCH_SCORE_MARGIN <= incumbent_key.score,
                    cache_loss_allows_switch: cache_loss_gate.allows_switch,
                    estimated_switch_cache_loss_micros: cache_loss_gate.estimated_micros,
                    cache_loss_status: cache_loss_gate.status,
                    switch_gate_reason: cache_loss_gate.reason,
                }
            });
        let incumbent_trace = incumbent;
        let owner_decision = match session_thread_key.as_deref() {
            Some(thread_id) => tier_memory.choose_thread_owner(
                thread_id,
                chosen_tier,
                formula_winner.candidate.upstream_id,
                incumbent,
                now,
            ),
            None => ThreadOwnerDecision {
                kept_upstream_id: formula_winner.candidate.upstream_id,
                switch_gate_reason: "stateless_formula_winner",
            },
        };
        let kept_upstream_id = owner_decision.kept_upstream_id;

        let trace = cc_lb_plugin_api::SubscriptionPreferenceTrace {
            chosen_tier,
            candidates: all_assessments,
            wrh_key_source,
            previous_tier,
            rendezvous_salt_version: Some(SALT_VERSION.to_owned()),
            cache_cost_basis_version: Some(CACHE_COST_BASIS_VERSION.to_owned()),
            formula_winner_upstream_id: Some(formula_winner.candidate.upstream_id),
            kept_upstream_id: Some(kept_upstream_id),
            incumbent_upstream_id: incumbent_trace.map(|incumbent| incumbent.upstream_id),
            estimated_switch_cache_loss_micros: incumbent_trace
                .and_then(|incumbent| incumbent.estimated_switch_cache_loss_micros),
            cache_loss_status: incumbent_trace
                .map(|incumbent| incumbent.cache_loss_status.to_owned()),
            switch_gate_reason: Some(owner_decision.switch_gate_reason.to_owned()),
        };
        return FilterOutput {
            kept_upstream_ids: vec![kept_upstream_id],
            reason: SUBSCRIPTION_ALIVE_REASON.to_owned(),
            per_candidate_reasons: Vec::new(),
            subscription_preference: Some(trace),
            cache_affinity: None,
        };
    }

    if has_api_key {
        FilterOutput {
            kept_upstream_ids: collect_kind(candidates, UpstreamKind::AnthropicApiKey),
            reason: API_KEY_FALLBACK_REASON.to_owned(),
            per_candidate_reasons: Vec::new(),
            subscription_preference: None,
            cache_affinity: None,
        }
    } else {
        FilterOutput {
            kept_upstream_ids: collect_kind(candidates, UpstreamKind::AnthropicOauth),
            reason: NO_API_KEY_REASON.to_owned(),
            per_candidate_reasons: Vec::new(),
            subscription_preference: None,
            cache_affinity: None,
        }
    }
}

fn relevant_base_windows(_canonical_model: &str) -> Vec<&'static str> {
    vec![WINDOW_FIVE_HOUR, WINDOW_SEVEN_DAY]
}

fn normalized_thread_routing_key(thread_id: &str) -> Cow<'_, str> {
    if thread_id.len() <= MAX_THREAD_ROUTING_KEY_BYTES {
        return Cow::Borrowed(thread_id);
    }

    let mut hasher = Sha256::new();
    hasher.update(RENDEZVOUS_SALT.as_bytes());
    hasher.update([0]);
    hasher.update(thread_id.as_bytes());
    let digest = hasher.finalize();
    let mut key = String::with_capacity(HASHED_THREAD_ROUTING_KEY_PREFIX.len() + 64);
    key.push_str(HASHED_THREAD_ROUTING_KEY_PREFIX);
    for byte in digest {
        let high = usize::from(byte >> 4);
        let low = usize::from(byte & 0x0f);
        key.push(char::from(b"0123456789abcdef"[high]));
        key.push(char::from(b"0123456789abcdef"[low]));
    }
    Cow::Owned(key)
}

fn collect_kind(candidates: &[UpstreamCandidate], kind: UpstreamKind) -> Vec<Uuid> {
    candidates
        .iter()
        .filter(|c| c.kind == kind)
        .map(|c| c.upstream_id)
        .collect()
}

// -- Tier & signal types. ---------------------------------------------------

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Tier {
    KnownBase = 0,
    PartialBase = 1,
    Overage = 2,
    UnknownProbe = 3,
}

fn tier_to_plugin_api(tier: Tier) -> cc_lb_plugin_api::SubscriptionTier {
    match tier {
        Tier::KnownBase => cc_lb_plugin_api::SubscriptionTier::KnownBase,
        Tier::PartialBase => cc_lb_plugin_api::SubscriptionTier::PartialBase,
        Tier::Overage => cc_lb_plugin_api::SubscriptionTier::Overage,
        Tier::UnknownProbe => cc_lb_plugin_api::SubscriptionTier::UnknownProbe,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BaseSignal {
    CurrentPositive,
    WarningPositive,
    HardNegative,
    Unknown,
}

struct OverageAssessment {
    ok: bool,
    base_exhausted_hint: bool,
    /// Fresh, finite utilization on the overage window (if present). Used
    /// for overage-tier urgency; `None` means "use OVERAGE_UNKNOWN_WEIGHT".
    fresh_overage_util: Option<f64>,
}

struct Assessment<'a> {
    candidate: &'a UpstreamCandidate,
    original_index: usize,
    tier: Tier,
    urgency: f64,
    warning_multiplier: f64,
}

#[derive(Clone, Copy, Debug)]
struct ThreadIncumbent {
    upstream_id: Uuid,
    challenger_beats_margin: bool,
    cache_loss_allows_switch: bool,
    estimated_switch_cache_loss_micros: Option<u64>,
    cache_loss_status: &'static str,
    switch_gate_reason: &'static str,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ThreadOwnerDecision {
    kept_upstream_id: Uuid,
    switch_gate_reason: &'static str,
}

#[derive(Clone, Copy, Debug)]
struct CacheLossGate {
    allows_switch: bool,
    estimated_micros: Option<u64>,
    status: &'static str,
    reason: &'static str,
}

// -- Candidate assessment. --------------------------------------------------

fn assess_candidate<'a>(
    candidate: &'a UpstreamCandidate,
    original_index: usize,
    base_windows: &[&'static str],
    config: &FilterConfig,
) -> Option<Assessment<'a>> {
    let now_secs = candidate_estimated_now(candidate);
    let multiplier = capacity_multiplier(candidate);

    let mut positive_count = 0u32;
    let mut warning_positive_count = 0u32;
    let mut hard_negative_count = 0u32;
    let mut base_urgency = 0.0f64;
    for &window in base_windows {
        let snapshot = find_snapshot(candidate, window);
        let signal = classify_base_snapshot(snapshot, now_secs, config);
        match signal {
            BaseSignal::CurrentPositive => {
                positive_count += 1;
                if let Some(contribution) =
                    window_urgency_contribution(snapshot, now_secs, multiplier)
                    && contribution > base_urgency
                {
                    base_urgency = contribution;
                }
            }
            BaseSignal::WarningPositive => {
                positive_count += 1;
                warning_positive_count += 1;
                if let Some(contribution) =
                    window_urgency_contribution(snapshot, now_secs, multiplier)
                    && contribution > base_urgency
                {
                    base_urgency = contribution;
                }
            }
            BaseSignal::HardNegative => hard_negative_count += 1,
            BaseSignal::Unknown => {}
        }
    }
    let total = base_windows.len() as u32;

    let overage = assess_overage(candidate, config);
    let base_proven_blocked = hard_negative_count > 0 || overage.base_exhausted_hint;

    let (tier, urgency) = if base_proven_blocked {
        if overage.ok {
            (Tier::Overage, overage_urgency(overage.fresh_overage_util))
        } else {
            return None;
        }
    } else if positive_count == total && total > 0 {
        (Tier::KnownBase, base_urgency)
    } else if positive_count > 0 {
        (Tier::PartialBase, base_urgency)
    } else if config.unknown_probe_enabled {
        (Tier::UnknownProbe, 0.0)
    } else {
        return None;
    };

    Some(Assessment {
        candidate,
        original_index,
        tier,
        urgency,
        warning_multiplier: if warning_positive_count > 0 {
            WARNING_MULTIPLIER
        } else {
            1.0
        },
    })
}

/// Compute the WRH urgency contribution of a single base window. Returns
/// `None` when the window should be excluded from urgency: non-fresh state,
/// missing / non-finite utilization, or missing / already-elapsed `resets_at`.
fn window_urgency_contribution(
    snapshot: Option<&SubscriptionQuotaCandidateSnapshot>,
    now_secs: u64,
    multiplier: f64,
) -> Option<f64> {
    let snap = snapshot?;
    if snap.state != SubscriptionQuotaDataState::Fresh {
        return None;
    }
    let util = snap.utilization?;
    if !util.is_finite() {
        return None;
    }
    let resets_at = snap.resets_at_unix_secs?;
    if resets_at <= now_secs {
        return None;
    }
    let remaining_secs = (resets_at - now_secs).max(MIN_REMAIN_SECS) as f64;
    let clamped_util = util.clamp(0.0, 1.0);
    let headroom = (1.0 - clamped_util).powi(HEADROOM_EXPONENT);
    Some(multiplier * headroom / remaining_secs)
}

fn overage_urgency(fresh_util: Option<f64>) -> f64 {
    match fresh_util {
        Some(u) if u.is_finite() => {
            let clamped = u.clamp(0.0, 1.0);
            let headroom = (1.0 - clamped).powi(HEADROOM_EXPONENT);
            headroom / (OVERAGE_REMAINING_NOMINAL_SECS as f64)
        }
        _ => OVERAGE_UNKNOWN_WEIGHT,
    }
}

/// Map the upstream's plan capacity ratio to a WRH multiplier in `[0, 2.0]`.
/// `sqrt` compresses the range so Pro (1.0), team_standard (1.25), and
/// large Max/Team plans (5x, 6.25x, 20x) all sit within one order of
/// magnitude, and the `CAPACITY_CAP` prevents 20x from dominating.
fn capacity_multiplier(candidate: &UpstreamCandidate) -> f64 {
    let ratio = candidate
        .plan_capacity_ratio
        .unwrap_or(UNKNOWN_CAPACITY_RATIO);
    if !ratio.is_finite() || ratio <= 0.0 {
        return UNKNOWN_CAPACITY_RATIO;
    }
    ratio.sqrt().min(CAPACITY_CAP)
}

fn classify_base_snapshot(
    snapshot: Option<&SubscriptionQuotaCandidateSnapshot>,
    now_secs: u64,
    config: &FilterConfig,
) -> BaseSignal {
    let Some(snap) = snapshot else {
        return BaseSignal::Unknown;
    };
    match snap.state {
        SubscriptionQuotaDataState::Missing => BaseSignal::Unknown,
        SubscriptionQuotaDataState::Fresh => {
            if snap.disabled_reason.is_some() {
                return BaseSignal::HardNegative;
            }
            if let Some(util) = snap.utilization
                && util.is_finite()
                && util >= 1.0
            {
                return BaseSignal::HardNegative;
            }
            let allowed_surpassed_threshold = match (snap.utilization, snap.surpassed_threshold) {
                (Some(util), Some(threshold)) => {
                    util.is_finite() && threshold.is_finite() && util >= threshold
                }
                (Some(_), None) | (None, Some(_)) | (None, None) => false,
            };
            match snap.status.as_deref() {
                Some("rejected") => BaseSignal::HardNegative,
                Some("allowed_warning") => BaseSignal::WarningPositive,
                Some("allowed") if allowed_surpassed_threshold => BaseSignal::WarningPositive,
                Some("allowed") => BaseSignal::CurrentPositive,
                Some(_) | None => utilization_signal(snap.utilization),
            }
        }
        SubscriptionQuotaDataState::Stale => {
            if snap.status.as_deref() == Some("rejected") {
                match snap.resets_at_unix_secs {
                    Some(resets_at) if resets_at > now_secs => BaseSignal::HardNegative,
                    Some(_) => BaseSignal::Unknown,
                    None => {
                        if config.stale_rejected_without_reset_blocks {
                            BaseSignal::HardNegative
                        } else {
                            BaseSignal::Unknown
                        }
                    }
                }
            } else {
                BaseSignal::Unknown
            }
        }
    }
}

fn utilization_signal(util: Option<f64>) -> BaseSignal {
    match util {
        Some(u) if !u.is_finite() => BaseSignal::Unknown,
        Some(u) if u >= 1.0 => BaseSignal::HardNegative,
        Some(u) if (0.0..1.0).contains(&u) => BaseSignal::CurrentPositive,
        _ => BaseSignal::Unknown,
    }
}

fn assess_overage(candidate: &UpstreamCandidate, config: &FilterConfig) -> OverageAssessment {
    let overage_snap = find_snapshot(candidate, WINDOW_OVERAGE);
    let unified_snap = find_snapshot(candidate, WINDOW_UNIFIED);

    let overage_in_use = unified_snap
        .and_then(|s| s.overage_in_use)
        .or_else(|| overage_snap.and_then(|s| s.overage_in_use))
        .unwrap_or(false);
    let fallback_available = unified_snap
        .and_then(|s| s.fallback_available)
        .or_else(|| overage_snap.and_then(|s| s.fallback_available));
    let extra_usage_enabled = overage_snap
        .and_then(|s| s.extra_usage_enabled)
        .or_else(|| unified_snap.and_then(|s| s.extra_usage_enabled));
    let extra_usage_remaining = overage_snap
        .and_then(snapshot_extra_usage_remaining)
        .or_else(|| unified_snap.and_then(snapshot_extra_usage_remaining));

    let is_overage_fresh = overage_snap
        .map(|s| s.state == SubscriptionQuotaDataState::Fresh)
        .unwrap_or(false);
    let overage_status = overage_snap.and_then(|s| s.status.as_deref());
    let overage_util = overage_snap
        .and_then(|s| s.utilization)
        .filter(|u| u.is_finite());

    let mut positive_count = 0u32;
    if matches!(overage_status, Some("allowed") | Some("allowed_warning")) {
        positive_count += 1;
    }
    if let Some(u) = overage_util
        && u < 1.0
    {
        positive_count += 1;
    }
    if fallback_available == Some(true) {
        positive_count += 1;
    }
    if overage_in_use {
        positive_count += 1;
    }
    if extra_usage_enabled == Some(true) && extra_usage_remaining.map(|r| r > 0.0).unwrap_or(false)
    {
        positive_count += 1;
    }
    let positive = positive_count > 0;

    let overage_status_blocked = is_overage_fresh && overage_status == Some("rejected");
    let overage_util_blocked = is_overage_fresh && overage_util.map(|u| u >= 1.0).unwrap_or(false);
    let extra_usage_disabled = extra_usage_enabled == Some(false);
    let extra_usage_exhausted = extra_usage_enabled == Some(true)
        && extra_usage_remaining.map(|r| r <= 0.0).unwrap_or(false);
    let blocked = fallback_available == Some(false)
        || overage_status_blocked
        || overage_util_blocked
        || extra_usage_disabled
        || extra_usage_exhausted;

    let ok = if config.hard_overage_block_wins {
        positive && !blocked
    } else {
        positive
    };

    let fresh_overage_util = if is_overage_fresh { overage_util } else { None };

    OverageAssessment {
        ok,
        base_exhausted_hint: overage_in_use,
        fresh_overage_util,
    }
}

fn snapshot_extra_usage_remaining(s: &SubscriptionQuotaCandidateSnapshot) -> Option<f64> {
    match (s.extra_usage_monthly_limit, s.extra_usage_used_credits) {
        (Some(limit), Some(used)) if limit.is_finite() && used.is_finite() => Some(limit - used),
        (Some(limit), None) if limit.is_finite() => Some(limit),
        _ => None,
    }
}

fn find_snapshot<'a>(
    candidate: &'a UpstreamCandidate,
    window: &str,
) -> Option<&'a SubscriptionQuotaCandidateSnapshot> {
    candidate
        .subscription_quotas
        .iter()
        .find(|s| s.window == window)
}

/// Estimate the wall clock for reset-comparison purposes. Both the candidate
/// observation and the freshest snapshot observation are lower bounds on the
/// real clock; taking the max minimises the window in which a long-since-
/// reset quota still appears rejected.
fn candidate_estimated_now(candidate: &UpstreamCandidate) -> u64 {
    let snap_max = candidate
        .subscription_quotas
        .iter()
        .filter_map(|s| s.observed_at_unix_millis.map(|m| m / 1_000))
        .max()
        .unwrap_or(0);
    candidate.observed_at_unix_secs.max(snap_max)
}

// -- Weighted-rendezvous selection. -----------------------------------------

struct TierSelection<'a, 'b> {
    winner: &'b Assessment<'a>,
    uniform: bool,
    max_cache_tokens: u32,
}

fn pick_within_tier<'a, 'b>(
    bucket: &'b [Assessment<'a>],
    routing_key: &str,
    config: &FilterConfig,
) -> TierSelection<'a, 'b> {
    debug_assert!(!bucket.is_empty());
    let total_urgency: f64 = bucket.iter().map(|a| a.urgency).sum();
    let uniform = total_urgency < EPSILON;
    let max_cache_tokens: u32 = bucket
        .iter()
        .map(candidate_cache_read_tokens)
        .max()
        .unwrap_or(0);

    if bucket.len() == 1 {
        return TierSelection {
            winner: &bucket[0],
            uniform,
            max_cache_tokens,
        };
    }

    let mut best_index = 0usize;
    let mut best_key = wrh_key(&bucket[0], routing_key, config, uniform, max_cache_tokens);
    for (i, assessment) in bucket.iter().enumerate().skip(1) {
        let key = wrh_key(assessment, routing_key, config, uniform, max_cache_tokens);
        if compare_wrh_key(&key, &best_key) == Ordering::Less {
            best_index = i;
            best_key = key;
        }
    }
    TierSelection {
        winner: &bucket[best_index],
        uniform,
        max_cache_tokens,
    }
}

/// Read-token count on a bucket assessment. Collapses `None` cache_score and
/// `Some(0)` into zero because both mean "no cache-read benefit for this
/// request"; they tie under the max-in-bucket comparison and both produce
/// `cache_ratio = 0` when they are the only signal.
fn candidate_cache_read_tokens(assessment: &Assessment<'_>) -> u32 {
    assessment
        .candidate
        .cache_score
        .as_ref()
        .map_or(0, |score| score.predicted_cache_read_tokens)
}

fn candidate_cache_creation_tokens_5m(assessment: &Assessment<'_>) -> u32 {
    assessment
        .candidate
        .cache_score
        .as_ref()
        .map_or(0, |score| score.predicted_cache_creation_tokens_5m)
}

fn candidate_cache_creation_tokens_1h(assessment: &Assessment<'_>) -> u32 {
    assessment
        .candidate
        .cache_score
        .as_ref()
        .map_or(0, |score| score.predicted_cache_creation_tokens_1h)
}

fn candidate_uncached_input_tokens(assessment: &Assessment<'_>) -> u32 {
    assessment
        .candidate
        .cache_score
        .as_ref()
        .map_or(0, |score| score.predicted_uncached_input_tokens)
}

/// Cache ratio inside one tier bucket. Returns `0.0` when the bucket has no
/// positive cache signal so the exponential multiplier collapses to `1.0`
/// (cold-pool fallback: WRH reduces to pure quota urgency, matching the
/// v6 all-cold behaviour tests already lock in).
fn cache_ratio_within_bucket(candidate_cache_tokens: u32, max_cache_tokens_in_bucket: u32) -> f64 {
    if max_cache_tokens_in_bucket == 0 {
        0.0
    } else {
        candidate_cache_tokens as f64 / max_cache_tokens_in_bucket as f64
    }
}

fn estimate_candidate_input_cost_micros(
    candidate: &UpstreamCandidate,
    pricing: &CachePricingSummary,
) -> Option<u64> {
    let score = candidate.cache_score.as_ref()?;
    let input_price = pricing.input_micros_per_million?;
    let cache_creation_5m_price = pricing.cache_creation_5m_micros_per_million?;
    let cache_creation_1h_price = pricing.cache_creation_1h_micros_per_million?;
    let cache_read_price = pricing.cache_read_micros_per_million?;
    Some(
        micros_for_tokens(score.predicted_uncached_input_tokens, input_price)
            .saturating_add(micros_for_tokens(
                score.predicted_cache_creation_tokens_5m,
                cache_creation_5m_price,
            ))
            .saturating_add(micros_for_tokens(
                score.predicted_cache_creation_tokens_1h,
                cache_creation_1h_price,
            ))
            .saturating_add(micros_for_tokens(
                score.predicted_cache_read_tokens,
                cache_read_price,
            )),
    )
}

fn cache_savings_ratio(candidate: &UpstreamCandidate, pricing: &CachePricingSummary) -> f64 {
    let Some(score) = candidate.cache_score.as_ref() else {
        return 0.0;
    };
    let Some(input_price) = pricing.input_micros_per_million else {
        return 0.0;
    };
    let Some(read_price) = pricing.cache_read_micros_per_million else {
        return 0.0;
    };
    let cold_cost = micros_for_tokens(score.predicted_cache_read_tokens, input_price);
    if cold_cost == 0 {
        return 0.0;
    }
    let read_cost = micros_for_tokens(score.predicted_cache_read_tokens, read_price);
    cold_cost.saturating_sub(read_cost) as f64 / cold_cost as f64
}

fn cache_loss_gate(
    incumbent: &Assessment<'_>,
    challenger: &Assessment<'_>,
    pricing: &CachePricingSummary,
) -> CacheLossGate {
    if incumbent.candidate.upstream_id == challenger.candidate.upstream_id {
        return CacheLossGate {
            allows_switch: true,
            estimated_micros: Some(0),
            status: "known",
            reason: "formula_winner_is_incumbent",
        };
    }

    let read_loss_tokens = candidate_cache_read_tokens(incumbent)
        .saturating_sub(candidate_cache_read_tokens(challenger));
    let creation_5m_tokens = candidate_cache_creation_tokens_5m(challenger).max(read_loss_tokens);
    let creation_1h_tokens = candidate_cache_creation_tokens_1h(challenger);
    if creation_5m_tokens == 0 && creation_1h_tokens == 0 {
        return CacheLossGate {
            allows_switch: true,
            estimated_micros: Some(0),
            status: "known",
            reason: "cache_loss_none",
        };
    }

    let Some(cache_creation_5m_price) = pricing.cache_creation_5m_micros_per_million else {
        return unknown_cache_loss_gate();
    };
    let Some(cache_creation_1h_price) = pricing.cache_creation_1h_micros_per_million else {
        return unknown_cache_loss_gate();
    };
    let Some(cache_read_price) = pricing.cache_read_micros_per_million else {
        return unknown_cache_loss_gate();
    };

    let delta_5m = cache_creation_5m_price.saturating_sub(cache_read_price);
    let delta_1h = cache_creation_1h_price.saturating_sub(cache_read_price);
    let estimated = micros_for_tokens(creation_5m_tokens, delta_5m)
        .saturating_add(micros_for_tokens(creation_1h_tokens, delta_1h));
    let allows_switch = estimated <= NORMAL_SWITCH_CACHE_LOSS_BUDGET_MICROS;
    CacheLossGate {
        allows_switch,
        estimated_micros: Some(estimated),
        status: "known",
        reason: if allows_switch {
            "cache_loss_allowed"
        } else {
            "cache_loss_blocked"
        },
    }
}

fn unknown_cache_loss_gate() -> CacheLossGate {
    CacheLossGate {
        allows_switch: false,
        estimated_micros: None,
        status: "unknown",
        reason: "cache_loss_unknown",
    }
}

fn micros_for_tokens(tokens: u32, micros_per_million: u64) -> u64 {
    (u128::from(tokens) * u128::from(micros_per_million) / 1_000_000)
        .try_into()
        .unwrap_or(u64::MAX)
}

struct WrhKey {
    /// `-ln(u) / weight`. Lower key wins.
    score: f64,
    /// Negated raw hash so that higher hash wins during score ties.
    rendezvous_neg: u64,
    upstream_id: Uuid,
    original_index: usize,
}

fn wrh_key(
    assessment: &Assessment<'_>,
    routing_key: &str,
    config: &FilterConfig,
    uniform: bool,
    max_cache_tokens: u32,
) -> WrhKey {
    let hash = rendezvous_hash(
        config.rendezvous_hash_salt,
        routing_key,
        assessment.candidate.upstream_id,
    );
    let quota_weight = if uniform { 1.0 } else { assessment.urgency };
    let cache_tokens = candidate_cache_read_tokens(assessment);
    let cache_ratio = cache_ratio_within_bucket(cache_tokens, max_cache_tokens);
    let effective_weight =
        quota_weight * (CACHE_LOG_BOOST * cache_ratio).exp() * assessment.warning_multiplier;
    let u = hash_to_open_unit(hash);
    let score = if effective_weight <= 0.0 {
        f64::INFINITY
    } else {
        -u.ln() / effective_weight
    };
    WrhKey {
        score,
        rendezvous_neg: u64::MAX - hash,
        upstream_id: assessment.candidate.upstream_id,
        original_index: assessment.original_index,
    }
}

fn compare_wrh_key(a: &WrhKey, b: &WrhKey) -> Ordering {
    a.score
        .total_cmp(&b.score)
        .then_with(|| a.rendezvous_neg.cmp(&b.rendezvous_neg))
        .then_with(|| a.upstream_id.cmp(&b.upstream_id))
        .then_with(|| a.original_index.cmp(&b.original_index))
}

/// Map a 64-bit hash to `u ∈ (0, 1)` using the top 53 bits so the division
/// is exact in f64. Guarantees `-ln(u)` is a finite positive number so WRH
/// scoring is numerically well-defined.
fn hash_to_open_unit(hash: u64) -> f64 {
    let top53 = hash >> 11;
    ((top53 as f64) + 0.5) / ((1u64 << 53) as f64)
}

// -- Rendezvous hash (FNV-1a 64 over salt || routing key || upstream_id,
// with a Murmur3 fmix64 avalanche finalizer). ------------------------------
//
// FNV-1a alone has weak avalanche: two inputs differing in one trailing byte
// produce hash values that differ by only a small fixed delta * fnv_prime.
// That is fatal for WRH — near-identical `u` values across candidates cause
// the highest-weight candidate to win every request. The Murmur3 fmix64
// step spreads any local input change across all 64 output bits, restoring
// the "independent uniforms per candidate" property WRH requires.

fn rendezvous_hash(salt: &str, routing_key: &str, upstream_id: Uuid) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    let fnv_prime: u64 = 0x100_0000_01b3;
    let mix = |h: &mut u64, byte: u8| {
        *h ^= byte as u64;
        *h = h.wrapping_mul(fnv_prime);
    };
    for &b in salt.as_bytes() {
        mix(&mut h, b);
    }
    mix(&mut h, 0);
    for &b in routing_key.as_bytes() {
        mix(&mut h, b);
    }
    mix(&mut h, 0);
    for &b in upstream_id.as_bytes() {
        mix(&mut h, b);
    }
    fmix64(h)
}

fn fmix64(mut h: u64) -> u64 {
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51_afd7_ed55_8ccd);
    h ^= h >> 33;
    h = h.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    h ^= h >> 33;
    h
}

#[cfg(test)]
mod tests;
