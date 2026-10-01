use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BackendKind {
    Postgres,
    Sqlite,
}

impl BackendKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Postgres => "postgres",
            Self::Sqlite => "sqlite",
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RequestEventStreamFilters {
    pub principal_id: Option<String>,
    pub thread_id: Option<String>,
    pub model: Option<String>,
    pub upstream_id: Option<Uuid>,
    pub status_class: Option<StatusClass>,
    pub errors_only: bool,
    /// Endpoint classification filter. `Some(kind)` matches rows whose
    /// effective kind (renewal via `source_kind` precedence, else stored
    /// `event_kind`, else `unclassified` for historical NULLs) equals `kind`.
    /// When set, it bypasses the implicit `source_kind <> 'renewal'`
    /// exclusion on list/histogram queries unless `source_kind` is also
    /// explicitly supplied.
    pub event_kind: Option<cc_lb_request_log::RequestEventKind>,
}

/// Shared contract for the request-log `model` filter.
///
/// Historical SQL (list and histogram), the in-memory live tail, the SSE
/// fan-out and the admin UI all filter on `model` independently; they must
/// agree on the exact predicate or the same row shows up in one path and is
/// silently dropped in another.
///
/// Normalization: the needle is trimmed and matched case-insensitively
/// (ASCII). One leading `claude-` vendor prefix on the needle is ignored, so
/// `sonnet`, `3-5-sonnet` and `claude-3-5-sonnet` all compare on the same
/// core. An empty or whitespace-only needle — or one that reduces to nothing
/// after the prefix strip (`claude-`) — means "no model filter": it behaves
/// exactly like an absent filter and every row passes, mirroring the
/// `? IS NULL` optional-clause shape in SQL.
///
/// Match rule for a present filter with lowercased core `c` against a
/// lowercased model `m`: `m` starts with `c`, OR `m` starts with `claude-`
/// and the remainder contains `c`. The second arm is what makes the filter
/// vendor-prefix-agnostic: `sonnet` finds `claude-sonnet-4-5` and
/// `claude-3-5-sonnet-20241022`, while non-`claude-*` models still only match
/// on their own literal prefix. A row without a model never matches a present
/// filter, mirroring `lower(model) LIKE …` which is NULL (and therefore
/// false) for `model IS NULL`.
///
/// SQL form: bind both patterns from [`model_filter_like_patterns`] as a
/// disjunction — `lower(model) LIKE <p1> ESCAPE '\' OR lower(model) LIKE
/// <p2> ESCAPE '\'`. Every pattern is left-anchored on a literal prefix
/// (`c%` and `claude-%c%`), never a bare `%c%` contains, so the clause stays
/// index-safe: Postgres can range-seek `request_events_v1_lower_model_list_
/// order_idx` (`lower(model) text_pattern_ops`) on either arm, and SQLite's
/// equivalent predicate keeps the same shape. `claude-` is the only vendor
/// prefix modeled; stored model names in this system are `claude-*` or
/// literal non-vendor names.
pub fn model_filter_matches(needle: &str, model: Option<&str>) -> bool {
    let Some(core) = model_filter_core(needle) else {
        // Absent filter: no restriction — every row passes, including
        // model-less rows (the SQL `? IS NULL` arm).
        return true;
    };
    let Some(model) = model else {
        return false;
    };
    let model = model.as_bytes();
    let core = core.as_bytes();
    if model.len() >= core.len() && model[..core.len()].eq_ignore_ascii_case(core) {
        return true;
    }
    if model.len() > CLAUDE_PREFIX.len()
        && model[..CLAUDE_PREFIX.len()].eq_ignore_ascii_case(CLAUDE_PREFIX.as_bytes())
    {
        return model[CLAUDE_PREFIX.len()..]
            .windows(core.len())
            .any(|window| window.eq_ignore_ascii_case(core));
    }
    false
}

/// `LIKE` pattern pair equivalent to [`model_filter_matches`]:
/// `[c%, claude-%c%]` where `c` is the normalized core (see
/// [`model_filter_core`]). Bind both against `lower(model)` with
/// `ESCAPE '\'` as a disjunction. Wildcards in the needle are escaped so a
/// user-typed `%` or `_` matches literally.
///
/// Returns `None` when the needle normalizes to an absent filter; callers
/// then bind `NULL`/omit the predicate so the row passes unfiltered.
pub fn model_filter_like_patterns(needle: &str) -> Option<[String; 2]> {
    let core = model_filter_core(needle)?;
    let mut prefix = String::with_capacity(core.len() + 4);
    let mut contains = String::with_capacity(core.len() + 4 + CLAUDE_PREFIX.len());
    contains.push_str(CLAUDE_PREFIX);
    contains.push('%');
    for ch in core.chars() {
        if matches!(ch, '%' | '_' | '\\') {
            prefix.push('\\');
            contains.push('\\');
        }
        let ch = ch.to_ascii_lowercase();
        prefix.push(ch);
        contains.push(ch);
    }
    prefix.push('%');
    contains.push('%');
    Some([prefix, contains])
}

/// The one vendor prefix the model filter hides: model names stored without
/// it still match on their literal prefix, and names stored with it are
/// searched inside the remainder.
const CLAUDE_PREFIX: &str = "claude-";

/// Normalizes the model-filter needle to its match core.
///
/// Trims surrounding whitespace and removes one leading `claude-`
/// (case-insensitive). `None` means the filter is absent: empty/whitespace
/// input, or nothing left after the prefix strip.
fn model_filter_core(needle: &str) -> Option<&str> {
    let needle = needle.trim();
    let core = match needle.get(..CLAUDE_PREFIX.len()) {
        Some(prefix) if prefix.eq_ignore_ascii_case(CLAUDE_PREFIX) => {
            &needle[CLAUDE_PREFIX.len()..]
        }
        _ => needle,
    };
    if core.is_empty() { None } else { Some(core) }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatusClass {
    TwoXx,
    ThreeXx,
    FourXx,
    FiveXx,
}

impl StatusClass {
    pub fn matches(self, status: u16) -> bool {
        match self {
            Self::TwoXx => (200..=299).contains(&status),
            Self::ThreeXx => (300..=399).contains(&status),
            Self::FourXx => (400..=499).contains(&status),
            Self::FiveXx => (500..=599).contains(&status),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrincipalLimitKind {
    Requests,
    Tokens,
    InputTokens,
    OutputTokens,
}

impl PrincipalLimitKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Requests => "requests",
            Self::Tokens => "tokens",
            Self::InputTokens => "input_tokens",
            Self::OutputTokens => "output_tokens",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrincipalLimitIdentityKind {
    Account,
    Credential,
    Unobserved,
}

impl PrincipalLimitIdentityKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Account => "account",
            Self::Credential => "credential",
            Self::Unobserved => "unobserved",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrincipalLimitState {
    pub principal_id: String,
    pub identity_kind: PrincipalLimitIdentityKind,
    pub identity_value: Option<String>,
    pub account_observed: bool,
    pub window: String,
    pub kind: PrincipalLimitKind,
    pub limit: Option<u64>,
    pub remaining: Option<u64>,
    pub reset: Option<String>,
    pub observed_at_unix_secs: u64,
    pub stored_at_unix_secs: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ConfigDraftState {
    pub draft: Option<Value>,
    pub revision: u64,
    pub last_validated_revision: Option<u64>,
    pub last_validation: Option<Value>,
    pub saved_at_unix_secs: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryEntry {
    pub revision: u64,
    pub applied_at_unix_secs: u64,
}

const UNKNOWN_USAGE_DIMENSION: &str = "unknown";
const MAX_USAGE_DIMENSION_CHARS: usize = 64;

/// Canonicalizes dimensions before they become usage-rollup keys.
///
/// Request-event queries that join back to rollups must use this same function
/// so missing, truncated, or punctuation-bearing principal IDs cannot drift
/// into a different dashboard series.
pub fn normalize_usage_rollup_dimension(value: Option<&str>) -> String {
    let Some(value) = value else {
        return UNKNOWN_USAGE_DIMENSION.to_owned();
    };
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return UNKNOWN_USAGE_DIMENSION.to_owned();
    }

    let mut normalized = String::new();
    for ch in trimmed.chars().take(MAX_USAGE_DIMENSION_CHARS) {
        if ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.' | ':' | '@') {
            normalized.push(ch);
        } else {
            normalized.push('_');
        }
    }
    if normalized.is_empty() {
        UNKNOWN_USAGE_DIMENSION.to_owned()
    } else {
        normalized
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UsageRollupResolution {
    Minute,
    Hour,
}

impl UsageRollupResolution {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Minute => "minute",
            Self::Hour => "hour",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UsageRollup {
    pub resolution: UsageRollupResolution,
    pub bucket_start: u64,
    pub principal: String,
    pub upstream_id: Uuid,
    pub upstream_name: String,
    pub model: String,
    pub request_count: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    #[serde(default)]
    pub cache_creation_input_tokens: u64,
    #[serde(default)]
    pub cache_read_input_tokens: u64,
    pub error_count: u64,
    pub latency_count: u64,
    pub latency_ms_sum: u64,
    pub latency_ms_min: Option<u64>,
    pub latency_ms_max: Option<u64>,
    #[serde(default)]
    pub proxy_setup_ms_count: u64,
    #[serde(default)]
    pub proxy_setup_ms_sum: u64,
    #[serde(default)]
    pub shape_ms_count: u64,
    #[serde(default)]
    pub shape_ms_sum: u64,
    #[serde(default)]
    pub sign_ms_count: u64,
    #[serde(default)]
    pub sign_ms_sum: u64,
    #[serde(default)]
    pub upstream_ttfb_ms_count: u64,
    #[serde(default)]
    pub upstream_ttfb_ms_sum: u64,
    #[serde(default)]
    pub upstream_body_ms_count: u64,
    #[serde(default)]
    pub upstream_body_ms_sum: u64,
    pub virtual_cost_micros: u64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct OverviewExcludedErrorBucket {
    pub bucket_start: u64,
    pub error_count: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct UsageTokenInterval {
    pub interval_id: u64,
    pub upstream_id: Uuid,
    pub start_unix_secs: u64,
    pub end_unix_secs: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct UsageTokenIntervalSum {
    pub interval_id: u64,
    pub tokens: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UsageRollupRun {
    pub processed_events: u64,
    pub updated_rollups: u64,
    pub checkpoint: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::{model_filter_like_patterns, model_filter_matches};

    #[test]
    fn model_filter_matches_case_insensitive() {
        // Literal prefix on the stored name, case-insensitive.
        assert!(model_filter_matches(
            "claude-sonnet",
            Some("claude-sonnet-4-5-20250929")
        ));
        assert!(model_filter_matches(
            "Claude-Sonnet",
            Some("claude-sonnet-4-5")
        ));
        assert!(model_filter_matches(
            "claude-sonnet-4-5",
            Some("CLAUDE-SONNET-4-5")
        ));
        assert!(model_filter_matches("gpt", Some("gpt-4o")));
        assert!(!model_filter_matches("4o", Some("gpt-4o")));
        assert!(!model_filter_matches(
            "claude-sonnet-4-5-2025",
            Some("claude-sonnet-4-5")
        ));
    }

    #[test]
    fn model_filter_matches_without_the_claude_prefix() {
        // The vendor prefix is hidden both on the needle and on stored names:
        // the core must appear at the start of a non-claude name, or anywhere
        // inside the `claude-` remainder.
        assert!(model_filter_matches("sonnet", Some("claude-sonnet-4-5")));
        assert!(model_filter_matches(
            "sonnet",
            Some("claude-3-5-sonnet-20241022")
        ));
        assert!(model_filter_matches(
            "3-5-sonnet",
            Some("claude-3-5-sonnet-20241022")
        ));
        assert!(model_filter_matches(
            "claude-3-5-sonnet",
            Some("claude-3-5-sonnet-20241022")
        ));
        // Removing `claude-` makes the filter prefix-independent for every
        // stored model name; it does not require the stored name to carry the
        // vendor prefix.
        assert!(model_filter_matches("claude-opus", Some("opus-x")));
        assert!(!model_filter_matches("sonnet", Some("other-sonnet-1")));
    }

    #[test]
    fn model_filter_never_matches_a_model_less_row() {
        assert!(!model_filter_matches("claude", None));
        // …but an absent filter passes every row, like `? IS NULL` in SQL.
        assert!(model_filter_matches("", None));
        assert!(model_filter_matches("   ", Some("claude-opus-4-1")));
        assert!(model_filter_matches("claude-", None));
    }

    #[test]
    fn model_filter_like_patterns_normalize_and_escape() {
        assert_eq!(
            model_filter_like_patterns("Claude-Opus"),
            Some(["opus%".to_owned(), "claude-%opus%".to_owned()])
        );
        assert_eq!(
            model_filter_like_patterns("  claude  "),
            Some(["claude%".to_owned(), "claude-%claude%".to_owned()])
        );
        assert_eq!(
            model_filter_like_patterns("a%b_c\\d"),
            Some([
                "a\\%b\\_c\\\\d%".to_owned(),
                "claude-%a\\%b\\_c\\\\d%".to_owned(),
            ])
        );
        assert_eq!(model_filter_like_patterns(""), None);
        assert_eq!(model_filter_like_patterns("  "), None);
        assert_eq!(model_filter_like_patterns("claude-"), None);
    }
}
