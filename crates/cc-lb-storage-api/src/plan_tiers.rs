//! Storage contract for the DB-managed, historized plan-tier ratio catalog, the
//! exact-match metadata->tier override table, and per-upstream resolved-tier
//! history.
//!
//! See `docs/adr/0005-pool-quota-history-recompute-derivability.md`.
//!
//! `tier_key` is stored as TEXT (not a Rust enum) to keep this crate free of a
//! dependency on `cc-lb-engine`, which owns the `TierKey` type. The set of legal
//! values is enforced by a `CHECK (tier_key IN (...))` constraint in the
//! migrations and parsed back into `cc_lb_quota::plan_capacity::TierKey` by the
//! consumer (the dynamic-view builder).
//!
//! All three tables are SCD Type 2 (effective-dated). The `upsert_*` / `append_*`
//! methods perform an atomic close-open transition: they close the current open
//! row and insert a new open row only when the value actually changed, and are
//! idempotent no-ops when the open row already matches.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::StorageResult;

/// A row in the historized Pro-relative ratio catalog
/// (`plan_tier_ratio_history_v1`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlanTierRatioRecord {
    /// Canonical tier key (`cc_lb_quota::plan_capacity::TierKey::as_str`).
    pub tier_key: String,
    /// Pro-relative capacity multiplier. Must be finite and > 0.
    pub pro_relative_ratio: f64,
    pub effective_from_unix_millis: i64,
    /// `None` marks the currently-effective (open) row.
    pub effective_to_unix_millis: Option<i64>,
    pub provenance: String,
    pub created_at_unix_millis: i64,
}

/// A row in the exact-match metadata->tier override table
/// (`metadata_tier_mapping_override_v1`). A `None` field means "this metadata
/// field is absent"; matching is exact (no substring / priority / JSON).
///
/// The adapters store an absent (`None`) field as the empty string `''` in
/// NOT NULL columns, so the open-row uniqueness index is a plain composite over
/// `(organization_type, rate_limit_tier, seat_tier)` with no `NULL != NULL`
/// pitfall. Real Anthropic metadata values are never empty, so `''` is an
/// unambiguous "absent" marker and round-trips back to `None` on read.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MetadataTierMappingOverrideRecord {
    pub organization_type: Option<String>,
    pub rate_limit_tier: Option<String>,
    pub seat_tier: Option<String>,
    /// Canonical tier key this exact triple resolves to.
    pub tier_key: String,
    pub effective_from_unix_millis: i64,
    pub effective_to_unix_millis: Option<i64>,
    pub provenance: String,
    pub created_at_unix_millis: i64,
}

/// How an upstream's tier was resolved for a given history interval.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TierResolutionSource {
    /// Matched an exact-match override row.
    Override,
    /// Matched the built-in `classify_plan_tier` logic.
    Builtin,
    /// Not recognized by either; surfaced for human attention.
    Unknown,
}

impl TierResolutionSource {
    pub const fn as_str(self) -> &'static str {
        match self {
            TierResolutionSource::Override => "override",
            TierResolutionSource::Builtin => "builtin",
            TierResolutionSource::Unknown => "unknown",
        }
    }
}

/// A row in the per-upstream resolved-tier history
/// (`upstream_plan_tier_history_v1`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UpstreamPlanTierRecord {
    pub upstream_id: Uuid,
    pub organization_uuid: Option<String>,
    /// Raw metadata triple, retained so history can be re-classified later.
    pub organization_type: Option<String>,
    pub rate_limit_tier: Option<String>,
    pub seat_tier: Option<String>,
    /// Resolved tier key, or `None` when the metadata was not recognized
    /// (`resolution_source == Unknown`).
    pub tier_key: Option<String>,
    pub resolution_source: TierResolutionSource,
    /// The ratio in effect for the resolved tier at write time. AUDIT ONLY:
    /// faithful recompute must join `plan_tier_ratio_history_v1` as-of `T`, not
    /// read this snapshot.
    pub resolved_ratio_snapshot: Option<f64>,
    pub observed_at_unix_millis: i64,
    pub effective_from_unix_millis: i64,
    pub effective_to_unix_millis: Option<i64>,
    pub provenance: String,
    pub created_at_unix_millis: i64,
}

/// Store for the plan-tier ratio catalog, exact-match overrides, and per-upstream
/// resolved-tier history. All writers are SCD2 close-open transitions.
#[async_trait]
pub trait PlanTierStore: Send + Sync {
    // --- Ratio catalog (plan_tier_ratio_history_v1) ---

    /// SCD2 upsert of a tier's Pro-relative ratio. In one transaction: no-op if
    /// the open row already has this ratio; otherwise close the open row at
    /// `record.effective_from_unix_millis` and insert the new open row.
    /// Rejects non-finite / non-positive ratios.
    async fn upsert_plan_tier_ratio(&self, record: &PlanTierRatioRecord) -> StorageResult<()>;

    /// The currently-effective (open) ratio row per tier.
    async fn list_current_plan_tier_ratios(&self) -> StorageResult<Vec<PlanTierRatioRecord>>;

    /// The ratio rows effective at `as_of_unix_millis` (for faithful recompute).
    async fn list_plan_tier_ratios_as_of(
        &self,
        as_of_unix_millis: i64,
    ) -> StorageResult<Vec<PlanTierRatioRecord>>;

    // --- Exact-match overrides (metadata_tier_mapping_override_v1) ---

    /// SCD2 upsert of an exact `(organization_type, rate_limit_tier, seat_tier)`
    /// -> `tier_key` override. Idempotent no-op when the open row already maps to
    /// the same tier.
    async fn upsert_metadata_tier_override(
        &self,
        record: &MetadataTierMappingOverrideRecord,
    ) -> StorageResult<()>;

    async fn list_current_metadata_tier_overrides(
        &self,
    ) -> StorageResult<Vec<MetadataTierMappingOverrideRecord>>;

    async fn list_metadata_tier_overrides_as_of(
        &self,
        as_of_unix_millis: i64,
    ) -> StorageResult<Vec<MetadataTierMappingOverrideRecord>>;

    // --- Per-upstream resolved-tier history (upstream_plan_tier_history_v1) ---

    /// SCD2 append of an upstream's resolved tier. No-op when the open row
    /// already has the same tier, source, and raw triple; otherwise close the
    /// open row and insert the new one.
    async fn append_upstream_plan_tier(&self, record: &UpstreamPlanTierRecord)
    -> StorageResult<()>;

    async fn list_current_upstream_plan_tiers(&self) -> StorageResult<Vec<UpstreamPlanTierRecord>>;

    async fn list_upstream_plan_tiers_as_of(
        &self,
        as_of_unix_millis: i64,
    ) -> StorageResult<Vec<UpstreamPlanTierRecord>>;
}
