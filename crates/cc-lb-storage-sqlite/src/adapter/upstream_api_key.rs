use async_trait::async_trait;
use cc_lb_storage_api::{
    ApiKeyAccessWrite, ApiKeyCapabilities, ApiKeyClass, ApiKeyOrgCapability,
    ApiKeyUsagePollCandidate, ApiKeyUsagePollOutcome, ApiUsageAttribution, RemediationCode,
    StorageError, StorageResult, UpstreamApiKeyDetailsRecord, UpstreamApiKeyStore,
    UpstreamApiUsageBucketQuery, UpstreamApiUsageBucketRecord, UpstreamApiUsageBucketWrite,
    UpstreamRequestOutcome,
};
use sqlx::{Row, Sqlite, Transaction, sqlite::SqliteRow};
use uuid::Uuid;

use super::upstreams::{
    conflict, i64_to_u64, now_unix_secs_u64, optional_i64_to_u64, split_row_to_record,
    split_upstream_columns, split_upstream_joins, u64_to_i64, usize_to_i64,
};
use crate::{SqliteStorage, map_sqlx_error};

/// Details columns, aliased so they never collide with the spec columns of
/// the poll-candidate join. `details_upstream_id IS NULL` means no row.
macro_rules! details_columns {
    () => {
        "d.upstream_id AS details_upstream_id, d.verified AS details_verified, d.key_class AS details_key_class, d.key_hint AS details_key_hint, d.api_key_id AS details_api_key_id, d.needs_workspace AS details_needs_workspace, d.capabilities_json AS details_capabilities_json, d.remediation_json AS details_remediation_json, d.usage_report_state AS details_usage_report_state, d.last_error AS details_last_error, d.checked_at_unix_secs AS details_checked_at_unix_secs, d.last_attempt_at_unix_secs AS details_last_attempt_at_unix_secs, d.access_revision AS details_access_revision, d.usage_poll_failures AS details_usage_poll_failures, d.usage_next_poll_after_unix_secs AS details_usage_next_poll_after_unix_secs, d.usage_last_success_at_unix_secs AS details_usage_last_success_at_unix_secs, d.usage_last_attempt_at_unix_secs AS details_usage_last_attempt_at_unix_secs, d.usage_poll_claim_token AS details_usage_poll_claim_token, d.usage_poll_claim_lease_until_unix_secs AS details_usage_poll_claim_lease_until_unix_secs, d.created_at_unix_secs AS details_created_at_unix_secs, d.updated_at_unix_secs AS details_updated_at_unix_secs"
    };
}

macro_rules! bucket_columns {
    () => {
        "attribution, subject_id, bucket_width_secs, bucket_start_unix_secs, model, uncached_input_tokens, cache_creation_5m_input_tokens, cache_creation_1h_input_tokens, cache_read_input_tokens, output_tokens, list_cost_micros, observed_at_unix_secs"
    };
}

/// Poll-state columns of one completed poll; binds `?1`..`?5`, then the
/// caller's `WHERE` binds `?6` (upstream id) and, when claimed, `?7`. A
/// non-null `next_poll_after` never replaces a later stored deadline, so an
/// overlapping success cannot shorten a 429 `retry-after`; `NULL` clears it.
macro_rules! poll_state_update_prefix {
    () => {
        "UPDATE upstream_api_key_details_v1 \
            SET usage_poll_failures = ?1, \
                usage_next_poll_after_unix_secs = CASE \
                    WHEN ?2 IS NOT NULL AND usage_next_poll_after_unix_secs > ?2 \
                        THEN usage_next_poll_after_unix_secs \
                    ELSE ?2 END, \
                usage_last_success_at_unix_secs = COALESCE(?3, usage_last_success_at_unix_secs), \
                usage_last_attempt_at_unix_secs = ?4, \
                updated_at_unix_secs = ?5"
    };
}

#[async_trait]
impl UpstreamApiKeyStore for SqliteStorage {
    async fn get_api_key_details(
        &self,
        upstream_id: Uuid,
    ) -> StorageResult<Option<UpstreamApiKeyDetailsRecord>> {
        let row = sqlx::query(concat!(
            "SELECT ",
            details_columns!(),
            " FROM upstream_api_key_details_v1 d WHERE d.upstream_id = ?"
        ))
        .bind(upstream_id.to_string())
        .fetch_optional(self.pool())
        .await
        .map_err(map_sqlx_error)?;
        Ok(row.as_ref().map(details_from_row).transpose()?.flatten())
    }

    async fn list_api_key_details(
        &self,
        upstream_ids: &[Uuid],
    ) -> StorageResult<Vec<UpstreamApiKeyDetailsRecord>> {
        if upstream_ids.is_empty() {
            return Ok(Vec::new());
        }
        let ids =
            serde_json::to_string(&upstream_ids.iter().map(Uuid::to_string).collect::<Vec<_>>())?;
        let rows = sqlx::query(concat!(
            "SELECT ",
            details_columns!(),
            " FROM upstream_api_key_details_v1 d WHERE d.upstream_id IN (SELECT value FROM json_each(?)) ORDER BY d.upstream_id"
        ))
        .bind(ids)
        .fetch_all(self.pool())
        .await
        .map_err(map_sqlx_error)?;
        let mut records = Vec::with_capacity(rows.len());
        for row in &rows {
            if let Some(record) = details_from_row(row)? {
                records.push(record);
            }
        }
        Ok(records)
    }

    async fn write_api_key_access(
        &self,
        upstream_id: Uuid,
        expected_spec_revision: u64,
        access: &ApiKeyAccessWrite,
    ) -> StorageResult<()> {
        let now = now_unix_secs_u64(self.clock())?;
        let mut tx = self.begin_immediate().await?;
        ensure_live_spec_revision_in_tx(&mut tx, upstream_id, expected_spec_revision).await?;
        write_api_key_access_in_tx(&mut tx, upstream_id, access, now).await?;
        tx.commit().await.map_err(map_sqlx_error)
    }

    async fn record_api_key_probe_failure(
        &self,
        upstream_id: Uuid,
        expected_spec_revision: u64,
        last_error: &str,
        remediation: &[RemediationCode],
        attempted_at_unix_secs: u64,
    ) -> StorageResult<()> {
        let now = now_unix_secs_u64(self.clock())?;
        let remediation_json = serde_json::to_string(remediation)?;
        let mut tx = self.begin_immediate().await?;
        ensure_live_spec_revision_in_tx(&mut tx, upstream_id, expected_spec_revision).await?;
        insert_details_if_missing_in_tx(&mut tx, upstream_id, "", now).await?;
        sqlx::query(
            "UPDATE upstream_api_key_details_v1
                SET last_error = ?,
                    remediation_json = ?,
                    last_attempt_at_unix_secs = ?,
                    updated_at_unix_secs = ?
              WHERE upstream_id = ?",
        )
        .bind(last_error)
        .bind(remediation_json)
        .bind(u64_to_i64(
            attempted_at_unix_secs,
            "last_attempt_at_unix_secs",
        )?)
        .bind(u64_to_i64(now, "updated_at_unix_secs")?)
        .bind(upstream_id.to_string())
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;
        tx.commit().await.map_err(map_sqlx_error)
    }

    async fn list_api_key_usage_poll_candidates(
        &self,
        now_unix_secs: u64,
        only_upstream: Option<Uuid>,
        limit: usize,
    ) -> StorageResult<Vec<ApiKeyUsagePollCandidate>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let rows = sqlx::query(concat!(
            "SELECT ",
            split_upstream_columns!(),
            ", ",
            details_columns!(),
            split_upstream_joins!(),
            "LEFT JOIN upstream_api_key_details_v1 d ON d.upstream_id = spec.id \
             WHERE spec.deleted_at IS NULL \
               AND spec.enabled = 1 \
               AND spec.kind = 'anthropic_api_key' \
               AND (?1 IS NULL OR spec.id = ?1) \
               AND (d.upstream_id IS NULL \
                    OR d.usage_report_state IS NULL \
                    OR (d.usage_report_state = 'ok' AND d.key_class = 'org_scoped')) \
               AND (d.usage_next_poll_after_unix_secs IS NULL \
                    OR d.usage_next_poll_after_unix_secs <= ?2) \
               AND (d.usage_poll_claim_token IS NULL \
                    OR d.usage_poll_claim_lease_until_unix_secs <= ?2) \
             ORDER BY d.usage_last_attempt_at_unix_secs ASC NULLS FIRST, spec.id \
             LIMIT ?3"
        ))
        .bind(only_upstream.map(|id| id.to_string()))
        .bind(u64_to_i64(now_unix_secs, "usage poll now")?)
        .bind(usize_to_i64(limit, "usage poll candidate limit")?)
        .fetch_all(self.pool())
        .await
        .map_err(map_sqlx_error)?;
        let mut candidates = Vec::with_capacity(rows.len());
        for row in rows {
            let details = details_from_row(&row)?;
            let upstream = split_row_to_record(row)?;
            candidates.push(ApiKeyUsagePollCandidate { upstream, details });
        }
        Ok(candidates)
    }

    async fn claim_api_key_usage_poll(
        &self,
        upstream_id: Uuid,
        expected_spec_revision: u64,
        claim_token: Uuid,
        lease_until_unix_secs: u64,
        key_hint: &str,
        now_unix_secs: u64,
    ) -> StorageResult<bool> {
        let updated_at = now_unix_secs_u64(self.clock())?;
        let mut tx = self.begin_immediate().await?;
        ensure_pollable_spec_revision_in_tx(&mut tx, upstream_id, expected_spec_revision).await?;
        // One statement: a missing row is inserted already claimed; an
        // existing row is claimed only while it is still eligible, due, and
        // unclaimed (or its lease expired). `rows_affected` is 0 otherwise.
        let claimed = sqlx::query(
            "INSERT INTO upstream_api_key_details_v1 (
                upstream_id, verified, key_class, key_hint, needs_workspace, capabilities_json,
                usage_poll_claim_token, usage_poll_claim_lease_until_unix_secs,
                created_at_unix_secs, updated_at_unix_secs
             ) VALUES (?1, 0, 'unknown', ?2, 0, ?3, ?4, ?5, ?6, ?6)
             ON CONFLICT (upstream_id) DO UPDATE
                SET usage_poll_claim_token = excluded.usage_poll_claim_token,
                    usage_poll_claim_lease_until_unix_secs =
                        excluded.usage_poll_claim_lease_until_unix_secs,
                    updated_at_unix_secs = excluded.updated_at_unix_secs
              WHERE (upstream_api_key_details_v1.usage_poll_claim_token IS NULL
                     OR upstream_api_key_details_v1.usage_poll_claim_lease_until_unix_secs <= ?7)
                AND (upstream_api_key_details_v1.usage_report_state IS NULL
                     OR (upstream_api_key_details_v1.usage_report_state = 'ok'
                         AND upstream_api_key_details_v1.key_class = 'org_scoped'))
                AND (upstream_api_key_details_v1.usage_next_poll_after_unix_secs IS NULL
                     OR upstream_api_key_details_v1.usage_next_poll_after_unix_secs <= ?7)",
        )
        .bind(upstream_id.to_string())
        .bind(key_hint)
        .bind(serde_json::to_string(&ApiKeyCapabilities::not_checked())?)
        .bind(claim_token.to_string())
        .bind(u64_to_i64(
            lease_until_unix_secs,
            "usage_poll_claim_lease_until_unix_secs",
        )?)
        .bind(u64_to_i64(updated_at, "updated_at_unix_secs")?)
        .bind(u64_to_i64(now_unix_secs, "usage poll claim now")?)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_error)?
        .rows_affected()
            == 1;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(claimed)
    }

    async fn write_api_key_usage_poll_outcome(
        &self,
        upstream_id: Uuid,
        expected_spec_revision: u64,
        outcome: &ApiKeyUsagePollOutcome,
    ) -> StorageResult<()> {
        let now = now_unix_secs_u64(self.clock())?;
        let mut tx = self.begin_immediate().await?;
        if outcome.claim_token.is_some() {
            ensure_pollable_spec_revision_in_tx(&mut tx, upstream_id, expected_spec_revision)
                .await?;
        } else {
            ensure_live_spec_revision_in_tx(&mut tx, upstream_id, expected_spec_revision).await?;
        }
        insert_details_if_missing_in_tx(
            &mut tx,
            upstream_id,
            outcome.legacy_key_hint.as_deref().unwrap_or_default(),
            now,
        )
        .await?;
        // The poll-state write goes first: with a claim it is conditional on
        // still owning it, and a lost claim must not upsert any bucket.
        let query = match outcome.claim_token {
            Some(_) => concat!(
                poll_state_update_prefix!(),
                ", usage_poll_claim_token = NULL, usage_poll_claim_lease_until_unix_secs = NULL \
                 WHERE upstream_id = ?6 AND usage_poll_claim_token = ?7"
            ),
            None => concat!(poll_state_update_prefix!(), " WHERE upstream_id = ?6"),
        };
        let mut update = sqlx::query(query)
            .bind(i64::from(outcome.consecutive_failures))
            .bind(optional_u64_to_i64(
                outcome.next_poll_after_unix_secs,
                "usage_next_poll_after_unix_secs",
            )?)
            .bind(optional_u64_to_i64(
                outcome.last_success_at_unix_secs,
                "usage_last_success_at_unix_secs",
            )?)
            .bind(u64_to_i64(
                outcome.attempted_at_unix_secs,
                "usage_last_attempt_at_unix_secs",
            )?)
            .bind(u64_to_i64(now, "updated_at_unix_secs")?)
            .bind(upstream_id.to_string());
        if let Some(token) = outcome.claim_token {
            update = update.bind(token.to_string());
        }
        let written = update
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?
            .rows_affected();
        if written == 0 {
            // Only a claim-guarded write can miss: the row was reclaimed
            // after the lease expired or an access write released it.
            return Err(conflict("usage poll claim no longer owned"));
        }
        for bucket in &outcome.buckets {
            upsert_bucket_in_tx(&mut tx, upstream_id, bucket, outcome.attempted_at_unix_secs)
                .await?;
        }
        if let Some(state) = outcome.usage_report_state {
            // A Re-check that landed mid-tick bumped `access_revision`; its
            // state wins and this one column is skipped (CONC-07).
            sqlx::query(
                "UPDATE upstream_api_key_details_v1
                    SET usage_report_state = ?
                  WHERE upstream_id = ? AND access_revision = ?",
            )
            .bind(state.as_str())
            .bind(upstream_id.to_string())
            .bind(u64_to_i64(outcome.access_revision_seen, "access_revision")?)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
        }
        tx.commit().await.map_err(map_sqlx_error)
    }

    async fn query_upstream_api_usage_buckets(
        &self,
        query: &UpstreamApiUsageBucketQuery,
    ) -> StorageResult<Vec<UpstreamApiUsageBucketRecord>> {
        let rows = sqlx::query(concat!(
            "SELECT ",
            bucket_columns!(),
            " FROM upstream_api_usage_v1 \
             WHERE upstream_id = ? AND attribution = ? AND subject_id = ? \
               AND bucket_width_secs = ? \
               AND bucket_start_unix_secs >= ? AND bucket_start_unix_secs < ? \
             ORDER BY bucket_start_unix_secs, model"
        ))
        .bind(query.upstream_id.to_string())
        .bind(query.attribution.as_str())
        .bind(&query.subject_id)
        .bind(i64::from(query.bucket_width_secs))
        .bind(u64_to_i64(query.start_unix_secs, "usage query start")?)
        .bind(u64_to_i64(query.end_unix_secs, "usage query end")?)
        .fetch_all(self.pool())
        .await
        .map_err(map_sqlx_error)?;
        rows.iter().map(bucket_from_row).collect()
    }

    async fn prune_upstream_api_usage_buckets(
        &self,
        before_unix_secs: u64,
        batch_size: usize,
    ) -> StorageResult<u64> {
        if batch_size == 0 {
            return Ok(0);
        }
        let mut tx = self.begin_immediate().await?;
        let removed = sqlx::query(
            "DELETE FROM upstream_api_usage_v1 \
             WHERE rowid IN ( \
                 SELECT rowid FROM upstream_api_usage_v1 \
                 WHERE bucket_start_unix_secs < ? \
                 ORDER BY bucket_start_unix_secs \
                 LIMIT ? \
             )",
        )
        .bind(u64_to_i64(before_unix_secs, "usage prune cutoff")?)
        .bind(usize_to_i64(batch_size, "usage prune batch size")?)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_error)?
        .rows_affected();
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(removed)
    }

    async fn latest_upstream_request_outcome(
        &self,
        upstream_id: Uuid,
        since_unix_ms: u64,
    ) -> StorageResult<Option<UpstreamRequestOutcome>> {
        let row = sqlx::query(
            "SELECT list_status, upstream_error_type, upstream_error_message, list_ts_ms \
             FROM request_events_v1 \
             WHERE upstream_id = ? AND list_ts_ms >= ? AND list_status IS NOT NULL \
             ORDER BY list_ts_ms DESC, list_event_key DESC \
             LIMIT 1",
        )
        .bind(upstream_id.to_string())
        .bind(u64_to_i64(since_unix_ms, "request outcome since")?)
        .fetch_optional(self.pool())
        .await
        .map_err(map_sqlx_error)?;
        let Some(row) = row else {
            return Ok(None);
        };
        let status = row
            .try_get::<i64, _>("list_status")
            .map_err(map_sqlx_error)?;
        Ok(Some(UpstreamRequestOutcome {
            status: u16::try_from(status).map_err(|_| StorageError::Corrupted {
                message: format!("request event status {status} out of range"),
            })?,
            upstream_error_type: row.try_get("upstream_error_type").map_err(map_sqlx_error)?,
            upstream_error_message: row
                .try_get("upstream_error_message")
                .map_err(map_sqlx_error)?,
            observed_at_unix_ms: i64_to_u64(
                row.try_get("list_ts_ms").map_err(map_sqlx_error)?,
                "request event list_ts_ms",
            )?,
        }))
    }
}

/// Write guard: the spec is live and still at `expected_revision`. The
/// surrounding `BEGIN IMMEDIATE` transaction holds the write lock, so the
/// guard cannot be invalidated before commit.
async fn ensure_live_spec_revision_in_tx(
    tx: &mut Transaction<'static, Sqlite>,
    upstream_id: Uuid,
    expected_revision: u64,
) -> StorageResult<()> {
    let row: Option<i64> = sqlx::query_scalar(
        "SELECT 1 FROM upstream_spec_v1 WHERE id = ? AND spec_revision = ? AND deleted_at IS NULL",
    )
    .bind(upstream_id.to_string())
    .bind(u64_to_i64(expected_revision, "upstream spec revision")?)
    .fetch_optional(&mut **tx)
    .await
    .map_err(map_sqlx_error)?;
    if row.is_none() {
        return Err(conflict("stale or deleted upstream"));
    }
    Ok(())
}

/// Poll guard: [`ensure_live_spec_revision_in_tx`] plus `enabled`. Candidate
/// selection only lists enabled upstreams; a disable that lands after that
/// read bumps the revision, and this check keeps a claim or a claimed
/// completion from ever succeeding against a disabled spec.
async fn ensure_pollable_spec_revision_in_tx(
    tx: &mut Transaction<'static, Sqlite>,
    upstream_id: Uuid,
    expected_revision: u64,
) -> StorageResult<()> {
    let row: Option<i64> = sqlx::query_scalar(
        "SELECT 1 FROM upstream_spec_v1
          WHERE id = ? AND spec_revision = ? AND deleted_at IS NULL AND enabled = 1",
    )
    .bind(upstream_id.to_string())
    .bind(u64_to_i64(expected_revision, "upstream spec revision")?)
    .fetch_optional(&mut **tx)
    .await
    .map_err(map_sqlx_error)?;
    if row.is_none() {
        return Err(conflict("stale, deleted, or disabled upstream"));
    }
    Ok(())
}

/// Insert-if-missing with not-checked defaults; every writer follows it with
/// a column-scoped `UPDATE` of only its own columns.
async fn insert_details_if_missing_in_tx(
    tx: &mut Transaction<'static, Sqlite>,
    upstream_id: Uuid,
    key_hint: &str,
    now_unix_secs: u64,
) -> StorageResult<()> {
    let now = u64_to_i64(now_unix_secs, "details timestamp")?;
    sqlx::query(
        "INSERT INTO upstream_api_key_details_v1 (
            upstream_id, verified, key_class, key_hint, needs_workspace, capabilities_json,
            created_at_unix_secs, updated_at_unix_secs
         ) VALUES (?, 0, 'unknown', ?, 0, ?, ?, ?)
         ON CONFLICT (upstream_id) DO NOTHING",
    )
    .bind(upstream_id.to_string())
    .bind(key_hint)
    .bind(serde_json::to_string(&ApiKeyCapabilities::not_checked())?)
    .bind(now)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(map_sqlx_error)?;
    Ok(())
}

/// Capability/access column write inside a transaction that already
/// validated the spec revision. It also releases any usage-poll claim: a poll
/// in flight read the previous credential/workspace, so its completion is
/// rejected and the next poll starts from the new access state.
pub(crate) async fn write_api_key_access_in_tx(
    tx: &mut Transaction<'static, Sqlite>,
    upstream_id: Uuid,
    access: &ApiKeyAccessWrite,
    now_unix_secs: u64,
) -> StorageResult<()> {
    insert_details_if_missing_in_tx(tx, upstream_id, &access.key_hint, now_unix_secs).await?;
    sqlx::query(
        "UPDATE upstream_api_key_details_v1
            SET verified = ?,
                key_class = ?,
                key_hint = ?,
                api_key_id = ?,
                needs_workspace = ?,
                capabilities_json = ?,
                remediation_json = ?,
                usage_report_state = ?,
                last_error = ?,
                checked_at_unix_secs = ?,
                last_attempt_at_unix_secs = ?,
                access_revision = access_revision + 1,
                usage_poll_claim_token = NULL,
                usage_poll_claim_lease_until_unix_secs = NULL,
                updated_at_unix_secs = ?
          WHERE upstream_id = ?",
    )
    .bind(access.verified)
    .bind(access.key_class.as_str())
    .bind(&access.key_hint)
    .bind(access.api_key_id.as_deref())
    .bind(access.needs_workspace)
    .bind(serde_json::to_string(&access.capabilities)?)
    .bind(serde_json::to_string(&access.remediation)?)
    .bind(access.usage_report_state.map(ApiKeyOrgCapability::as_str))
    .bind(access.last_error.as_deref())
    .bind(optional_u64_to_i64(
        access.checked_at_unix_secs,
        "checked_at_unix_secs",
    )?)
    .bind(u64_to_i64(
        access.last_attempt_at_unix_secs,
        "last_attempt_at_unix_secs",
    )?)
    .bind(u64_to_i64(now_unix_secs, "updated_at_unix_secs")?)
    .bind(upstream_id.to_string())
    .execute(&mut **tx)
    .await
    .map_err(map_sqlx_error)?;
    Ok(())
}

/// Zeroes poll failures and clears the poll schedule and success/attempt
/// timestamps. Usage rows are kept; a missing details row stays missing.
pub(crate) async fn reset_api_key_poll_state_in_tx(
    tx: &mut Transaction<'static, Sqlite>,
    upstream_id: Uuid,
    now_unix_secs: u64,
) -> StorageResult<()> {
    sqlx::query(
        "UPDATE upstream_api_key_details_v1
            SET usage_poll_failures = 0,
                usage_next_poll_after_unix_secs = NULL,
                usage_last_success_at_unix_secs = NULL,
                usage_last_attempt_at_unix_secs = NULL,
                usage_poll_claim_token = NULL,
                usage_poll_claim_lease_until_unix_secs = NULL,
                updated_at_unix_secs = ?
          WHERE upstream_id = ?",
    )
    .bind(u64_to_i64(now_unix_secs, "updated_at_unix_secs")?)
    .bind(upstream_id.to_string())
    .execute(&mut **tx)
    .await
    .map_err(map_sqlx_error)?;
    Ok(())
}

async fn upsert_bucket_in_tx(
    tx: &mut Transaction<'static, Sqlite>,
    upstream_id: Uuid,
    bucket: &UpstreamApiUsageBucketWrite,
    observed_at_unix_secs: u64,
) -> StorageResult<()> {
    sqlx::query(concat!(
        "INSERT INTO upstream_api_usage_v1 (upstream_id, ",
        bucket_columns!(),
        ") \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT (upstream_id, attribution, subject_id, bucket_width_secs, bucket_start_unix_secs, model) \
         DO UPDATE SET uncached_input_tokens = excluded.uncached_input_tokens, \
                       cache_creation_5m_input_tokens = excluded.cache_creation_5m_input_tokens, \
                       cache_creation_1h_input_tokens = excluded.cache_creation_1h_input_tokens, \
                       cache_read_input_tokens = excluded.cache_read_input_tokens, \
                       output_tokens = excluded.output_tokens, \
                       list_cost_micros = excluded.list_cost_micros, \
                       observed_at_unix_secs = excluded.observed_at_unix_secs"
    ))
    .bind(upstream_id.to_string())
    .bind(bucket.attribution.as_str())
    .bind(&bucket.subject_id)
    .bind(i64::from(bucket.bucket_width_secs))
    .bind(u64_to_i64(bucket.bucket_start_unix_secs, "bucket_start_unix_secs")?)
    .bind(&bucket.model)
    .bind(u64_to_i64(bucket.uncached_input_tokens, "uncached_input_tokens")?)
    .bind(u64_to_i64(
        bucket.cache_creation_5m_input_tokens,
        "cache_creation_5m_input_tokens",
    )?)
    .bind(u64_to_i64(
        bucket.cache_creation_1h_input_tokens,
        "cache_creation_1h_input_tokens",
    )?)
    .bind(u64_to_i64(
        bucket.cache_read_input_tokens,
        "cache_read_input_tokens",
    )?)
    .bind(u64_to_i64(bucket.output_tokens, "output_tokens")?)
    .bind(u64_to_i64(bucket.list_cost_micros, "list_cost_micros")?)
    .bind(u64_to_i64(observed_at_unix_secs, "observed_at_unix_secs")?)
    .execute(&mut **tx)
    .await
    .map_err(map_sqlx_error)?;
    Ok(())
}

fn details_from_row(row: &SqliteRow) -> StorageResult<Option<UpstreamApiKeyDetailsRecord>> {
    let Some(upstream_id) = row
        .try_get::<Option<String>, _>("details_upstream_id")
        .map_err(map_sqlx_error)?
    else {
        return Ok(None);
    };
    let key_class = row
        .try_get::<String, _>("details_key_class")
        .map_err(map_sqlx_error)?;
    let capabilities = row
        .try_get::<String, _>("details_capabilities_json")
        .map_err(map_sqlx_error)?;
    let remediation = row
        .try_get::<String, _>("details_remediation_json")
        .map_err(map_sqlx_error)?;
    let usage_report_state = row
        .try_get::<Option<String>, _>("details_usage_report_state")
        .map_err(map_sqlx_error)?;
    Ok(Some(UpstreamApiKeyDetailsRecord {
        upstream_id: Uuid::parse_str(&upstream_id).map_err(|error| StorageError::Corrupted {
            message: format!("invalid api-key details upstream id {upstream_id}: {error}"),
        })?,
        verified: row
            .try_get::<i64, _>("details_verified")
            .map_err(map_sqlx_error)?
            != 0,
        key_class: ApiKeyClass::parse(&key_class).ok_or_else(|| StorageError::Corrupted {
            message: format!("invalid api-key class {key_class}"),
        })?,
        key_hint: row.try_get("details_key_hint").map_err(map_sqlx_error)?,
        api_key_id: row.try_get("details_api_key_id").map_err(map_sqlx_error)?,
        needs_workspace: row
            .try_get::<i64, _>("details_needs_workspace")
            .map_err(map_sqlx_error)?
            != 0,
        capabilities: serde_json::from_str(&capabilities)?,
        remediation: serde_json::from_str(&remediation)?,
        usage_report_state: usage_report_state
            .map(|state| {
                ApiKeyOrgCapability::parse(&state).ok_or_else(|| StorageError::Corrupted {
                    message: format!("invalid usage_report_state {state}"),
                })
            })
            .transpose()?,
        last_error: row.try_get("details_last_error").map_err(map_sqlx_error)?,
        checked_at_unix_secs: optional_i64_to_u64(
            row.try_get("details_checked_at_unix_secs")
                .map_err(map_sqlx_error)?,
            "checked_at_unix_secs",
        )?,
        last_attempt_at_unix_secs: optional_i64_to_u64(
            row.try_get("details_last_attempt_at_unix_secs")
                .map_err(map_sqlx_error)?,
            "last_attempt_at_unix_secs",
        )?,
        access_revision: i64_to_u64(
            row.try_get("details_access_revision")
                .map_err(map_sqlx_error)?,
            "access_revision",
        )?,
        usage_poll_failures: i64_to_u32(
            row.try_get("details_usage_poll_failures")
                .map_err(map_sqlx_error)?,
            "usage_poll_failures",
        )?,
        usage_next_poll_after_unix_secs: optional_i64_to_u64(
            row.try_get("details_usage_next_poll_after_unix_secs")
                .map_err(map_sqlx_error)?,
            "usage_next_poll_after_unix_secs",
        )?,
        usage_last_success_at_unix_secs: optional_i64_to_u64(
            row.try_get("details_usage_last_success_at_unix_secs")
                .map_err(map_sqlx_error)?,
            "usage_last_success_at_unix_secs",
        )?,
        usage_last_attempt_at_unix_secs: optional_i64_to_u64(
            row.try_get("details_usage_last_attempt_at_unix_secs")
                .map_err(map_sqlx_error)?,
            "usage_last_attempt_at_unix_secs",
        )?,
        usage_poll_claim_token: row
            .try_get::<Option<String>, _>("details_usage_poll_claim_token")
            .map_err(map_sqlx_error)?
            .map(|token| {
                Uuid::parse_str(&token).map_err(|error| StorageError::Corrupted {
                    message: format!("invalid usage poll claim token {token}: {error}"),
                })
            })
            .transpose()?,
        usage_poll_claim_lease_until_unix_secs: optional_i64_to_u64(
            row.try_get("details_usage_poll_claim_lease_until_unix_secs")
                .map_err(map_sqlx_error)?,
            "usage_poll_claim_lease_until_unix_secs",
        )?,
        created_at_unix_secs: i64_to_u64(
            row.try_get("details_created_at_unix_secs")
                .map_err(map_sqlx_error)?,
            "created_at_unix_secs",
        )?,
        updated_at_unix_secs: i64_to_u64(
            row.try_get("details_updated_at_unix_secs")
                .map_err(map_sqlx_error)?,
            "updated_at_unix_secs",
        )?,
    }))
}

fn bucket_from_row(row: &SqliteRow) -> StorageResult<UpstreamApiUsageBucketRecord> {
    let attribution = row
        .try_get::<String, _>("attribution")
        .map_err(map_sqlx_error)?;
    let token = |column: &str| -> StorageResult<u64> {
        i64_to_u64(row.try_get(column).map_err(map_sqlx_error)?, column)
    };
    Ok(UpstreamApiUsageBucketRecord {
        attribution: ApiUsageAttribution::parse(&attribution).ok_or_else(|| {
            StorageError::Corrupted {
                message: format!("invalid usage attribution {attribution}"),
            }
        })?,
        subject_id: row.try_get("subject_id").map_err(map_sqlx_error)?,
        bucket_width_secs: i64_to_u32(
            row.try_get("bucket_width_secs").map_err(map_sqlx_error)?,
            "bucket_width_secs",
        )?,
        bucket_start_unix_secs: token("bucket_start_unix_secs")?,
        model: row.try_get("model").map_err(map_sqlx_error)?,
        uncached_input_tokens: token("uncached_input_tokens")?,
        cache_creation_5m_input_tokens: token("cache_creation_5m_input_tokens")?,
        cache_creation_1h_input_tokens: token("cache_creation_1h_input_tokens")?,
        cache_read_input_tokens: token("cache_read_input_tokens")?,
        output_tokens: token("output_tokens")?,
        list_cost_micros: token("list_cost_micros")?,
        observed_at_unix_secs: token("observed_at_unix_secs")?,
    })
}

fn optional_u64_to_i64(value: Option<u64>, field: &str) -> StorageResult<Option<i64>> {
    value.map(|value| u64_to_i64(value, field)).transpose()
}

fn i64_to_u32(value: i64, field: &str) -> StorageResult<u32> {
    u32::try_from(value).map_err(|_| StorageError::Corrupted {
        message: format!("{field} out of range"),
    })
}
