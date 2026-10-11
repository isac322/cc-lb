//! Reset-priority ranking: burn the subscription whose quota expires soonest.
//!
//! This is the guest-side, `no_std`, libm-free decision core. It is kept
//! separate from the wasm ABI adapter so it can be unit-tested on the host
//! target with plain Rust structs.
//!
//! ## What the rule is
//!
//! Prefer quota that expires soon while retaining useful cache affinity.
//! Exhausted or provider-rejected accounts are never selected.
//!
//! ## Priority order
//!
//! Block rejected or exhausted accounts first. Prefer usable weekly headroom,
//! then form a 10% band around the best shared/model weekly expiry urgency.
//! Within that band, the largest predicted cache hit wins even when its
//! five-hour window is nearly spent. Five-hour pressure never overrides weekly
//! urgency or a warm cache. At 100% usage or a provider rejection, the account
//! is blocked and the next usable account is selected.
//!
//! With no predicted cache hit, prefer five-hour headroom inside the weekly
//! band, then use a stable conversation hash. A new conversation therefore
//! starts on an account with room, while an existing warm conversation can
//! consume its owner's five-hour quota. Without a conversation id, use weekly
//! urgency and upstream id to break ties deterministically.
//!
//! Weekly expiry urgency uses the libm-free heuristic:
//!
//! ```text
//! remaining_fraction / clamp(time_to_reset_fraction, 0.01, 1.0) ^ 2
//! ```
//!
//! This favors imminent weekly resets. It differs from the host's burn-pace
//! pressure; urgency above 1 does not prove usage is behind linear pace.
//! Cache affinity is limited to accounts with comparable weekly urgency, so
//! quota that is substantially closer to expiry can still override a cache hit.
//!
//! ## Selection output
//!
//! Exactly one subscription candidate is kept whenever one is viable. Keeping
//! two would hand the choice to the terminal strategy (`first_pick` or
//! `random`), and `random` would silently undo the ranking. Non-subscription
//! candidates (API keys) are always passed through untouched so the built-in
//! `subscription-preference` filter downstream can still fall back to them.
//!
//! When all subscriptions are blocked, only API-key candidates survive.
//! Without a fallback, the host answers `route_no_upstream_after_filter`.

use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;

/// Upstream kind string the host uses for an OAuth subscription upstream.
pub const KIND_ANTHROPIC_OAUTH: &str = "anthropic_oauth";

pub const WINDOW_FIVE_HOUR: &str = "5h";
pub const WINDOW_SEVEN_DAY: &str = "7d";
pub const WINDOW_SEVEN_DAY_OPUS: &str = "7d_opus";
pub const WINDOW_SEVEN_DAY_SONNET: &str = "7d_sonnet";
pub const WINDOW_SEVEN_DAY_FABLE: &str = "7d_fable";

const FABLE_MODEL: &str = "claude-fable-5";
const SONNET_MODEL_PREFIX: &str = "claude-sonnet-";
const OPUS_MODEL_PREFIX: &str = "claude-opus-";

const SEVEN_DAY_WINDOW_SECS: f64 = 604_800.0;

/// Guard below which the time-to-reset denominator stops scaling, so a window
/// that resets in seconds cannot produce an unbounded urgency.
const TIME_FRACTION_FLOOR: f64 = 0.01;

/// Without a cache hit, prefer peers below this five-hour utilization.
/// This soft threshold never displaces a warm account within the weekly band.
pub const FIVE_HOUR_TIGHT_UTIL: f64 = 0.90;

/// Utilization at or above which the provider itself will reject the window.
const EXHAUSTED_UTIL: f64 = 1.0;

/// Heuristic weekly headroom floor, not an estimate of the next request size.
/// Below it, an account is demoted when a peer has more room.
const MIN_REMAINING_FRACTION: f64 = 0.005;

/// Near-tie band: candidates within this multiplicative distance of the best
/// urgency are treated as equivalent and settled by affinity instead.
const AFFINITY_BAND: f64 = 0.10;

/// Freshness of a quota window snapshot, mirroring
/// `cc_lb_domain::SubscriptionQuotaDataState`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DataState {
    Fresh,
    Stale,
    Absent,
    Unobserved,
}

impl DataState {
    #[must_use]
    pub fn parse(value: &str) -> Self {
        match value {
            "fresh" => Self::Fresh,
            "stale" => Self::Stale,
            "absent" => Self::Absent,
            _ => Self::Unobserved,
        }
    }
}

/// One provider quota window as seen by the plugin.
#[derive(Clone, Copy, Debug)]
pub struct QuotaWindow {
    pub state: DataState,
    pub utilization: Option<f64>,
    pub resets_at_unix_secs: Option<u64>,
    /// When the host observed this window, in Unix seconds (the wire carries
    /// Unix milliseconds).
    ///
    /// This is the reliable clock on the wire: the candidate-level stamp is the
    /// *rate-limit* cache time and is 0 unless a rate-limit snapshot exists,
    /// whereas any snapshot the host has already classified as fresh was
    /// necessarily observed close to its own now.
    pub observed_at_unix_secs: Option<u64>,
    /// `status == "rejected"` or a provider `disabled_reason` is set.
    pub rejected: bool,
}

impl QuotaWindow {
    /// Utilization clamped to `0.0..=1.0`, or `None` when unusable.
    fn utilization(&self) -> Option<f64> {
        self.utilization
            .filter(|value| value.is_finite())
            .map(|value| value.clamp(0.0, 1.0))
    }

    /// Fresh, finite utilization and a reset still ahead of `now`.
    fn remaining_and_secs(&self, now: u64) -> Option<(f64, u64)> {
        if self.state != DataState::Fresh {
            return None;
        }
        let utilization = self.utilization.filter(|value| value.is_finite())?;
        let resets_at = self.resets_at_unix_secs?;
        if resets_at <= now {
            return None;
        }
        Some((
            (1.0 - utilization).clamp(0.0, 1.0),
            resets_at.saturating_sub(now),
        ))
    }
}

/// The quota windows of one candidate that this plugin reasons about.
#[derive(Clone, Copy, Debug, Default)]
pub struct CandidateQuotas<'a> {
    pub five_hour: Option<&'a QuotaWindow>,
    pub shared_weekly: Option<&'a QuotaWindow>,
    pub model_weekly: Option<&'a QuotaWindow>,
}

/// One upstream candidate as seen by the algorithm.
#[derive(Clone, Copy, Debug)]
pub struct CandidateView<'a> {
    pub upstream_id: &'a str,
    pub kind: &'a str,
    pub observed_at_unix_secs: u64,
    pub predicted_cache_read_tokens: u32,
    pub quotas: CandidateQuotas<'a>,
}

/// One routing request.
#[derive(Clone, Copy, Debug)]
pub struct RequestView<'a> {
    /// Cache-affinity key. Claude Code sends a stable per-conversation value
    /// the host surfaces as `thread_id`.
    pub thread_id: Option<&'a str>,
    /// The host's routing clock for this pass, in Unix seconds (filter wire V2).
    ///
    /// This is the *current* time, not a measurement stamp. Quota observations
    /// can be minutes old, so a countdown built from them alone drifts — and on
    /// an idle account it reads as "nothing expiring" exactly when the reset is
    /// closest, which inverts the rule's deadline term.
    pub now_unix_secs: u64,
    pub candidates: &'a [CandidateView<'a>],
}

/// The plugin's decision for one request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Decision {
    /// Upstream ids to accept: pass-through (non-subscription) candidates in
    /// input order, then the chosen subscription candidate, if any.
    pub accepted: Vec<String>,
    /// `(upstream_id, decision, reason)` for every candidate: pass-through
    /// candidates first, then subscriptions. The host keys decisions by
    /// upstream id, so the grouping is for audit readability only.
    pub per_candidate: Vec<(String, &'static str, String)>,
}

const ACCEPT: &str = "accept";
const REJECT: &str = "reject";

/// Pick the account to burn. See the module docs for the rule.
#[must_use]
pub fn decide(request: &RequestView<'_>) -> Decision {
    // The host's routing clock is authoritative on wire V2. The observation
    // estimate stays as a floor so a synthetic request (or a host that reports
    // no clock) cannot make a fresh window look like it reset in the future.
    let now = request.now_unix_secs.max(estimated_now(request.candidates));

    let evaluated: Vec<Evaluated<'_>> = request
        .candidates
        .iter()
        .map(|candidate| evaluate(candidate, now))
        .collect();

    let mut accepted: Vec<String> = Vec::new();
    let mut decisions: Vec<(String, &'static str, String)> = Vec::new();

    // Non-subscription candidates always survive: the built-in filter that runs
    // after this one still owns the API-key fallback path.
    for candidate in &evaluated {
        if !candidate.is_subscription {
            accepted.push(String::from(candidate.upstream_id));
            decisions.push((
                String::from(candidate.upstream_id),
                ACCEPT,
                String::from("pass_through_non_subscription"),
            ));
        }
    }

    let subscriptions: Vec<&Evaluated<'_>> = evaluated
        .iter()
        .filter(|candidate| candidate.is_subscription)
        .collect();
    if subscriptions.is_empty() {
        return Decision {
            accepted,
            per_candidate: decisions,
        };
    }

    let viable: Vec<&Evaluated<'_>> = subscriptions
        .iter()
        .copied()
        .filter(|candidate| candidate.blocked.is_none())
        .collect();

    let winner = if viable.is_empty() {
        None
    } else {
        pick_winner(&viable, request.thread_id)
    };

    for candidate in &subscriptions {
        let (decision, reason) = match winner {
            Some(winner) if winner.upstream_id == candidate.upstream_id => (ACCEPT, {
                format!(
                    "accept:reset_priority priority=weekly 5h_reset_in={}s 7d_reset_in={}s weekly_urgency={:.2} cache_read_tokens={}",
                    candidate.five_hour_reset_secs,
                    candidate.weekly_reset_secs,
                    candidate.weekly_urgency,
                    candidate.predicted_cache_read_tokens
                )
            }),
            _ => (REJECT, candidate.reason()),
        };
        if decision == ACCEPT {
            accepted.push(String::from(candidate.upstream_id));
        }
        decisions.push((String::from(candidate.upstream_id), decision, reason));
    }

    Decision {
        accepted,
        per_candidate: decisions,
    }
}

/// Observation-derived clock floor, mirroring the built-in filter's
/// `candidate_estimated_now`.
///
/// That is: the candidate-level stamp when the host has one, otherwise the
/// freshest quota observation on the wire.
///
/// Wire V2 supplies the real routing clock, so this is only a floor. It still
/// earns its place: the host stamps `observed_at_unix_secs` from its
/// *rate-limit* cache and leaves it at 0 when no rate-limit snapshot exists,
/// which is the normal state of a healthy pool. Read alone, that field makes
/// every window look decades away, clamps every time fraction to 1.0, and
/// silently reduces the whole rule to "whoever has the most quota left wins" —
/// the deadline term disappears without any error. The floor keeps that guard
/// for synthetic requests and for a host that reports no routing clock.
fn estimated_now(candidates: &[CandidateView<'_>]) -> u64 {
    let mut now = 0;
    for candidate in candidates {
        now = now.max(candidate.observed_at_unix_secs);
        for window in [
            candidate.quotas.five_hour,
            candidate.quotas.shared_weekly,
            candidate.quotas.model_weekly,
        ]
        .into_iter()
        .flatten()
        {
            if let Some(observed_at) = window.observed_at_unix_secs {
                now = now.max(observed_at);
            }
        }
    }
    now
}

/// Which model-scoped weekly window applies to a request for this canonical
/// model id. The adapter uses it to fill [`CandidateQuotas::model_weekly`].
#[must_use]
pub fn model_weekly_window(canonical_model_id: &str) -> Option<&'static str> {
    if canonical_model_id == FABLE_MODEL {
        Some(WINDOW_SEVEN_DAY_FABLE)
    } else if canonical_model_id.starts_with(SONNET_MODEL_PREFIX) {
        Some(WINDOW_SEVEN_DAY_SONNET)
    } else if canonical_model_id.starts_with(OPUS_MODEL_PREFIX) {
        Some(WINDOW_SEVEN_DAY_OPUS)
    } else {
        None
    }
}

struct Evaluated<'a> {
    upstream_id: &'a str,
    is_subscription: bool,
    /// `Some(reason)` when the account is provably out of room.
    blocked: Option<&'static str>,
    /// Five-hour window has headroom or no usable positive snapshot.
    five_hour_has_headroom: bool,
    /// Weekly remaining fraction is above the heuristic headroom floor.
    has_weekly_room: bool,
    /// Weekly term (shared and model-scoped weekly windows combined).
    weekly_urgency: f64,
    /// Seconds until the five-hour reset, for the audit reason.
    five_hour_reset_secs: u64,
    /// Seconds until the soonest weekly reset, for the audit reason.
    weekly_reset_secs: u64,
    predicted_cache_read_tokens: u32,
}

impl Evaluated<'_> {
    fn reason(&self) -> String {
        self.blocked.map_or_else(
            || String::from("reject:lower_priority_or_affinity"),
            |reason| format!("reject:{reason}"),
        )
    }
}

fn evaluate<'a>(candidate: &CandidateView<'a>, now: u64) -> Evaluated<'a> {
    let quotas = candidate.quotas;
    let mut evaluated = Evaluated {
        upstream_id: candidate.upstream_id,
        is_subscription: candidate.kind == KIND_ANTHROPIC_OAUTH,
        blocked: None,
        five_hour_has_headroom: true,
        has_weekly_room: true,
        weekly_urgency: 0.0,
        five_hour_reset_secs: 0,
        weekly_reset_secs: 0,
        predicted_cache_read_tokens: candidate.predicted_cache_read_tokens,
    };
    if !evaluated.is_subscription {
        return evaluated;
    }

    let model_quotas = quotas.model_weekly;

    // Match the downstream base-plan admission rules before narrowing to one
    // account. Otherwise a rejected winner can hide a usable runner-up.
    evaluated.blocked = quota_block(quotas.five_hour, now, "five_hour_exhausted")
        .or_else(|| quota_block(quotas.shared_weekly, now, "weekly_exhausted"))
        .or_else(|| quota_block(model_quotas, now, "weekly_exhausted"));

    let five_hour = quotas
        .five_hour
        .and_then(|window| window.remaining_and_secs(now));
    evaluated.five_hour_has_headroom =
        five_hour.is_none_or(|(remaining, _)| remaining > 1.0 - FIVE_HOUR_TIGHT_UTIL);
    evaluated.five_hour_reset_secs = five_hour.map_or(0, |(_, secs)| secs);

    let shared = weekly_urgency(quotas.shared_weekly, now);
    let model = weekly_urgency(model_quotas, now);
    evaluated.weekly_reset_secs = soonest_known_reset(shared.secs, model.secs);
    evaluated.weekly_urgency = shared.urgency.max(model.urgency);

    // Heuristic headroom: both weekly constraints bind, so use the smaller
    // usable remaining fraction, not just the shared window.
    let weekly_remaining = [quotas.shared_weekly, model_quotas]
        .into_iter()
        .flatten()
        .filter_map(|window| window.remaining_and_secs(now))
        .map(|(remaining, _)| remaining)
        .reduce(f64::min);
    evaluated.has_weekly_room =
        weekly_remaining.is_none_or(|remaining| remaining >= MIN_REMAINING_FRACTION);

    evaluated
}

/// A base-plan window that proves the account is out of room.
///
/// Stale rejection without a reset remains blocked, matching the built-in
/// filter's default.
fn quota_block(
    window: Option<&QuotaWindow>,
    now: u64,
    reason: &'static str,
) -> Option<&'static str> {
    let window = window?;
    let exhausted = window.rejected
        || window
            .utilization()
            .is_some_and(|utilization| utilization >= EXHAUSTED_UTIL);
    if !exhausted {
        return None;
    }
    match window.state {
        DataState::Fresh => Some(reason),
        // A stale rejection still blocks while its reset is in the future, and
        // stops blocking once the window it described has rolled over.
        DataState::Stale => match window.resets_at_unix_secs {
            Some(resets_at) if resets_at > now => Some(reason),
            None => Some(reason),
            Some(_) => None,
        },
        DataState::Absent | DataState::Unobserved => None,
    }
}

/// Select by weekly urgency, cache warmth, then cold-session headroom.
fn pick_winner<'a, 'b>(
    viable: &[&'b Evaluated<'a>],
    thread_id: Option<&str>,
) -> Option<&'b Evaluated<'a>> {
    let has_weekly_room = viable.iter().any(|candidate| candidate.has_weekly_room);
    let weekly_pool = || {
        viable
            .iter()
            .copied()
            .filter(|candidate| !has_weekly_room || candidate.has_weekly_room)
    };
    let best_weekly = weekly_pool()
        .map(|candidate| candidate.weekly_urgency)
        .fold(0.0, f64::max);
    let threshold = best_weekly / (1.0 + AFFINITY_BAND);
    let weekly_band = || weekly_pool().filter(|candidate| candidate.weekly_urgency >= threshold);

    let warmest = weekly_band()
        .map(|candidate| candidate.predicted_cache_read_tokens)
        .max()
        .unwrap_or(0);
    if warmest > 0 {
        return break_tie(
            weekly_band().filter(|candidate| candidate.predicted_cache_read_tokens == warmest),
            thread_id,
        );
    }

    let has_five_hour_headroom = weekly_band().any(|candidate| candidate.five_hour_has_headroom);
    break_tie(
        weekly_band()
            .filter(|candidate| !has_five_hour_headroom || candidate.five_hour_has_headroom),
        thread_id,
    )
}

/// `secs` as a fraction of `window_secs`.
///
/// The `u64 as f64` conversion loses precision above 2^53 seconds (285 million
/// years); every value here is under a week and the result is only ever
/// compared against another ratio, so the loss cannot change a decision. This
/// is the crate's single cast site, kept in one place with its rationale.
#[allow(clippy::cast_precision_loss, clippy::as_conversions)]
fn fraction_of_window(secs: u64, window_secs: f64) -> f64 {
    secs as f64 / window_secs
}

struct WeeklyUrgency {
    urgency: f64,
    secs: u64,
}

/// `remaining / time_left ^ 2`, clamped so an imminent reset stays finite.
fn weekly_urgency(window: Option<&QuotaWindow>, now: u64) -> WeeklyUrgency {
    let Some(window) = window else {
        return WeeklyUrgency {
            urgency: 0.0,
            secs: 0,
        };
    };
    let Some((remaining, secs_to_reset)) = window.remaining_and_secs(now) else {
        return WeeklyUrgency {
            urgency: 0.0,
            secs: 0,
        };
    };
    let time_fraction =
        fraction_of_window(secs_to_reset, SEVEN_DAY_WINDOW_SECS).clamp(TIME_FRACTION_FLOOR, 1.0);
    WeeklyUrgency {
        urgency: remaining / (time_fraction * time_fraction),
        secs: secs_to_reset,
    }
}

/// The next reset that restores capacity, for the audit reason. Zero means
/// "no usable weekly snapshot".
fn soonest_known_reset(shared_secs: u64, model_secs: u64) -> u64 {
    match (shared_secs, model_secs) {
        (0, model_secs) => model_secs,
        (shared_secs, 0) => shared_secs,
        (shared_secs, model_secs) => shared_secs.min(model_secs),
    }
}

/// Stable tie-breaking over the eligible candidates without temporary vectors.
fn break_tie<'a, 'b>(
    group: impl Iterator<Item = &'b Evaluated<'a>>,
    thread_id: Option<&str>,
) -> Option<&'b Evaluated<'a>> {
    match thread_id.filter(|value| !value.is_empty()) {
        Some(key) => group.min_by_key(|candidate| {
            (
                affinity_hash(key, candidate.upstream_id),
                candidate.upstream_id,
            )
        }),
        None => group.max_by(|left, right| {
            left.weekly_urgency
                .total_cmp(&right.weekly_urgency)
                .then_with(|| right.upstream_id.cmp(left.upstream_id))
        }),
    }
}

/// FNV-1a over `thread_id || upstream_id`. Stable across processes and builds,
/// which is what makes a stateless filter able to pin a thread to an account.
fn affinity_hash(thread_id: &str, upstream_id: &str) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in thread_id.as_bytes().iter().chain(upstream_id.as_bytes()) {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

#[cfg(test)]
mod tests;
