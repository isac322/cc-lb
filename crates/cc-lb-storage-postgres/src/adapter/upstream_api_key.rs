use async_trait::async_trait;
use cc_lb_clock::unix_secs;
use cc_lb_storage_api::{
    ApiKeyAccessWrite, ApiKeyCapabilities, ApiKeyClass, ApiKeyOrgCapability,
    ApiKeyUsagePollCandidate, ApiKeyUsagePollOutcome, ApiUsageAttribution,
    OpenAiOrganizationReportingCredentialCiphertextRecord,
    OpenAiOrganizationReportingCredentialRecord, OpenAiOrganizationReportingCredentialStatus,
    OpenAiOrganizationReportingCredentialWrite, Provider, ProviderProjectSource,
    ProviderReportState, RemediationCode, StorageError, StorageResult, UpstreamApiKeyDetailsRecord,
    UpstreamApiKeyStore, UpstreamApiUsageBucketQuery, UpstreamApiUsageBucketRecord,
    UpstreamApiUsageBucketWrite, UpstreamRequestOutcome,
};
use sqlx::{Postgres, Row, Transaction, postgres::PgRow};
use uuid::Uuid;

use super::upstreams::{split_row_to_record, split_upstream_columns, split_upstream_joins};
use crate::{
    adapter::{PostgresStorage, conflict, i64_to_u64, u64_to_i64},
    error_map::map_sqlx_error,
};

/// Details columns, aliased so they never collide with the spec columns of
/// the poll-candidate join. `details_upstream_id IS NULL` means no row.
macro_rules! details_columns {
    () => {
        "d.upstream_id AS details_upstream_id, d.verified AS details_verified, d.key_class AS details_key_class, d.key_hint AS details_key_hint, d.api_key_id AS details_api_key_id, d.needs_workspace AS details_needs_workspace, d.capabilities_json AS details_capabilities_json, d.remediation_json AS details_remediation_json, d.usage_report_state AS details_usage_report_state, d.provider AS details_provider, d.project_id AS details_project_id, d.project_source AS details_project_source, d.provider_report_state AS details_provider_report_state, d.provider_report_status_metadata_json AS details_provider_report_status_metadata_json, d.last_error AS details_last_error, d.checked_at_unix_secs AS details_checked_at_unix_secs, d.last_attempt_at_unix_secs AS details_last_attempt_at_unix_secs, d.access_revision AS details_access_revision, d.usage_poll_failures AS details_usage_poll_failures, d.usage_next_poll_after_unix_secs AS details_usage_next_poll_after_unix_secs, d.usage_last_success_at_unix_secs AS details_usage_last_success_at_unix_secs, d.usage_last_attempt_at_unix_secs AS details_usage_last_attempt_at_unix_secs, d.usage_poll_claim_token AS details_usage_poll_claim_token, d.usage_poll_claim_lease_until_unix_secs AS details_usage_poll_claim_lease_until_unix_secs, d.created_at_unix_secs AS details_created_at_unix_secs, d.updated_at_unix_secs AS details_updated_at_unix_secs"
    };
}

macro_rules! bucket_columns {
    () => {
        "attribution, subject_id, bucket_width_secs, bucket_start_unix_secs, model, uncached_input_tokens, cache_creation_5m_input_tokens, cache_creation_1h_input_tokens, cache_read_input_tokens, output_tokens, list_cost_micros, observed_at_unix_secs"
    };
}

/// Poll-state columns of one completed poll; binds `$2`..`$6` after `$1`
/// (upstream id) and, when claimed, `$7` (claim token) in the caller's
/// `WHERE`. A non-null `next_poll_after` never replaces a later stored
/// deadline, so an overlapping success cannot shorten a 429 `retry-after`;
/// `NULL` clears it.
macro_rules! poll_state_update_prefix {
    () => {
        "UPDATE upstream_api_key_details_v1 \
            SET usage_poll_failures = $2, \
                usage_next_poll_after_unix_secs = CASE \
                    WHEN $3::BIGINT IS NOT NULL AND usage_next_poll_after_unix_secs > $3::BIGINT \
                        THEN usage_next_poll_after_unix_secs \
                    ELSE $3::BIGINT END, \
                usage_last_success_at_unix_secs = COALESCE($4, usage_last_success_at_unix_secs), \
                usage_last_attempt_at_unix_secs = $5, \
                updated_at_unix_secs = $6"
    };
}
/// Provider-neutral report columns of one completed poll. Each non-null
/// candidate value is revision-fenced independently of the poll-state write.
macro_rules! provider_report_update {
    () => {
        "UPDATE upstream_api_key_details_v1
            SET provider = COALESCE($2, provider),
                provider_report_state = COALESCE($3, provider_report_state),
                provider_report_status_metadata_json =
                    COALESCE($4, provider_report_status_metadata_json)
          WHERE upstream_id = $1 AND access_revision = $5"
    };
}

#[async_trait]
impl UpstreamApiKeyStore for PostgresStorage {
    async fn get_api_key_details(
        &self,
        upstream_id: Uuid,
    ) -> StorageResult<Option<UpstreamApiKeyDetailsRecord>> {
        let row = sqlx::query(concat!(
            "SELECT ",
            details_columns!(),
            " FROM upstream_api_key_details_v1 d WHERE d.upstream_id = $1"
        ))
        .bind(upstream_id)
        .fetch_optional(&self.pool)
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
        let rows = sqlx::query(concat!(
            "SELECT ",
            details_columns!(),
            " FROM upstream_api_key_details_v1 d WHERE d.upstream_id = ANY($1) ORDER BY d.upstream_id"
        ))
        .bind(upstream_ids)
        .fetch_all(&self.pool)
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
    async fn get_openai_org_reporting_credential(
        &self,
    ) -> StorageResult<Option<OpenAiOrganizationReportingCredentialRecord>> {
        let row = sqlx::query(
            "SELECT scope, redacted_hint, status, revision, updated_at_unix_secs
             FROM openai_org_reporting_credential_v1
             WHERE scope = 'deployment'",
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx_error)?;
        credential_record_from_postgres_row(row.as_ref())
    }

    async fn get_openai_org_reporting_credential_ciphertext(
        &self,
    ) -> StorageResult<Option<OpenAiOrganizationReportingCredentialCiphertextRecord>> {
        let row = sqlx::query(
            "SELECT encrypted_secret_ciphertext, status, revision
             FROM openai_org_reporting_credential_v1
             WHERE scope = 'deployment'",
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx_error)?;
        credential_ciphertext_record_from_postgres_row(row.as_ref())
    }
    async fn claim_openai_org_reporting_poll(
        &self,
        credential_revision: u64,
        now_unix_secs: u64,
        lease_until_unix_secs: u64,
    ) -> StorageResult<bool> {
        let changed = sqlx::query(
            "UPDATE openai_org_reporting_credential_v1
                SET poll_claim_lease_until_unix_secs = $1
              WHERE scope = 'deployment'
                AND status = 'configured'
                AND revision = $2
                AND (poll_claim_lease_until_unix_secs IS NULL
                     OR poll_claim_lease_until_unix_secs <= $3)",
        )
        .bind(u64_to_i64(lease_until_unix_secs, "credential lease until")?)
        .bind(u64_to_i64(credential_revision, "credential revision")?)
        .bind(u64_to_i64(now_unix_secs, "credential lease now")?)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx_error)?;
        Ok(changed.rows_affected() == 1)
    }

    async fn mark_openai_org_reporting_credential_denied(
        &self,
        expected_revision: u64,
        updated_at_unix_secs: u64,
    ) -> StorageResult<()> {
        let changed = sqlx::query(
            "UPDATE openai_org_reporting_credential_v1
                SET status = 'denied',
                    revision = revision + 1,
                    updated_at_unix_secs = $1,
                    poll_claim_lease_until_unix_secs = NULL
              WHERE scope = 'deployment' AND status = 'configured' AND revision = $2",
        )
        .bind(u64_to_i64(
            updated_at_unix_secs,
            "credential denied updated_at_unix_secs",
        )?)
        .bind(u64_to_i64(expected_revision, "credential revision")?)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx_error)?;
        if changed.rows_affected() == 1 {
            Ok(())
        } else {
            Err(conflict("OpenAI reporting credential revision changed"))
        }
    }

    async fn replace_openai_org_reporting_credential(
        &self,
        write: &OpenAiOrganizationReportingCredentialWrite,
    ) -> StorageResult<OpenAiOrganizationReportingCredentialRecord> {
        let mut tx = self.pool.begin().await.map_err(map_sqlx_error)?;
        let result = if let Some(expected_revision) = write.expected_revision {
            sqlx::query(
                "UPDATE openai_org_reporting_credential_v1
                    SET encrypted_secret_ciphertext = $1,
                        redacted_hint = $2,
                        status = $3,
                        revision = revision + 1,
                        updated_at_unix_secs = $4,
                        poll_claim_lease_until_unix_secs = NULL
                  WHERE scope = 'deployment' AND revision = $5",
            )
            .bind(&write.encrypted_secret_ciphertext)
            .bind(&write.redacted_hint)
            .bind(write.status.as_str())
            .bind(u64_to_i64(
                write.updated_at_unix_secs,
                "credential updated_at_unix_secs",
            )?)
            .bind(u64_to_i64(
                expected_revision,
                "credential expected revision",
            )?)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?
        } else {
            sqlx::query(
                "INSERT INTO openai_org_reporting_credential_v1 (
                    scope, encrypted_secret_ciphertext, redacted_hint, status,
                    revision, created_at_unix_secs, updated_at_unix_secs
                 ) VALUES ('deployment', $1, $2, $3, 1, $4, $4)
                 ON CONFLICT (scope) DO NOTHING",
            )
            .bind(&write.encrypted_secret_ciphertext)
            .bind(&write.redacted_hint)
            .bind(write.status.as_str())
            .bind(u64_to_i64(
                write.updated_at_unix_secs,
                "credential updated_at_unix_secs",
            )?)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?
        };
        if result.rows_affected() != 1 {
            return Err(conflict("stale or existing OpenAI reporting credential"));
        }
        if write.status == OpenAiOrganizationReportingCredentialStatus::Configured {
            sqlx::query(
                "UPDATE upstream_api_key_details_v1
                    SET provider_report_state = NULL,
                        provider_report_status_metadata_json = NULL,
                        usage_poll_failures = 0,
                        usage_next_poll_after_unix_secs = NULL,
                        usage_poll_claim_token = NULL,
                        usage_poll_claim_lease_until_unix_secs = NULL,
                        updated_at_unix_secs = CASE
                            WHEN updated_at_unix_secs >= $1 THEN updated_at_unix_secs
                            ELSE $1
                        END
                  WHERE provider_report_state IN ('not_configured', 'denied')
                    AND EXISTS (
                        SELECT 1
                          FROM upstream_spec_v1 spec
                         WHERE spec.id = upstream_api_key_details_v1.upstream_id
                           AND spec.kind = 'openai_api_key'
                    )",
            )
            .bind(u64_to_i64(
                write.updated_at_unix_secs,
                "details updated_at_unix_secs",
            )?)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
        }

        let row = sqlx::query(
            "SELECT scope, redacted_hint, status, revision, updated_at_unix_secs
             FROM openai_org_reporting_credential_v1
             WHERE scope = 'deployment'",
        )
        .fetch_one(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;
        let record = credential_record_from_postgres_row(Some(&row))?.ok_or_else(|| {
            StorageError::Corrupted {
                message: "OpenAI reporting credential row disappeared".to_owned(),
            }
        })?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(record)
    }

    async fn write_api_key_access(
        &self,
        upstream_id: Uuid,
        expected_spec_revision: u64,
        access: &ApiKeyAccessWrite,
    ) -> StorageResult<()> {
        let now = unix_secs(self.clock.now());
        let mut tx = self.pool.begin().await.map_err(map_sqlx_error)?;
        ensure_live_spec_revision_for_access_in_tx(&mut tx, upstream_id, expected_spec_revision)
            .await?;
        let project_changed =
            api_key_access_project_changed_in_tx(&mut tx, upstream_id, access).await?;
        write_api_key_access_in_tx(&mut tx, upstream_id, access, now).await?;
        if project_changed {
            sqlx::query(
                "UPDATE upstream_spec_v1
                    SET owner_epoch = owner_epoch + CASE WHEN kind = 'gemini_api_key' THEN 1 ELSE 0 END,
                        updated_at = NOW()
                  WHERE id = $1 AND deleted_at IS NULL",
            )
            .bind(upstream_id)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
        }
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
        let now = unix_secs(self.clock.now());
        let remediation_json = serde_json::to_string(remediation)?;
        let mut tx = self.pool.begin().await.map_err(map_sqlx_error)?;
        ensure_live_spec_revision_in_tx(&mut tx, upstream_id, expected_spec_revision).await?;
        insert_details_if_missing_in_tx(&mut tx, upstream_id, "", now).await?;
        sqlx::query(
            "UPDATE upstream_api_key_details_v1
                SET last_error = $2,
                    remediation_json = $3,
                    last_attempt_at_unix_secs = $4,
                    updated_at_unix_secs = $5
              WHERE upstream_id = $1",
        )
        .bind(upstream_id)
        .bind(last_error)
        .bind(remediation_json)
        .bind(u64_to_i64(
            attempted_at_unix_secs,
            "last_attempt_at_unix_secs",
        )?)
        .bind(u64_to_i64(now, "updated_at_unix_secs")?)
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
               AND spec.enabled \
               AND spec.kind IN ('anthropic_api_key', 'openai_api_key', 'gemini_api_key') \
               AND ($1::uuid IS NULL OR spec.id = $1) \
               AND ( \
                    (spec.kind = 'anthropic_api_key' \
                     AND (d.upstream_id IS NULL \
                          OR d.usage_report_state IS NULL \
                          OR (d.usage_report_state = 'ok' AND d.key_class = 'org_scoped'))) \
                    OR (spec.kind IN ('openai_api_key', 'gemini_api_key') \
                        AND (d.provider_report_state IS NULL \
                             OR d.provider_report_state IN ('unavailable', 'failed', 'stale'))) \
               ) \
               AND (d.usage_next_poll_after_unix_secs IS NULL \
                    OR d.usage_next_poll_after_unix_secs <= $2) \
               AND (d.usage_poll_claim_token IS NULL \
                    OR d.usage_poll_claim_lease_until_unix_secs <= $2) \
             ORDER BY d.usage_last_attempt_at_unix_secs ASC NULLS FIRST, spec.id \
             LIMIT $3"
        ))
        .bind(only_upstream)
        .bind(u64_to_i64(now_unix_secs, "usage poll now")?)
        .bind(usize_to_i64(limit, "usage poll candidate limit")?)
        .fetch_all(&self.pool)
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
        let updated_at = unix_secs(self.clock.now());
        let mut tx = self.pool.begin().await.map_err(map_sqlx_error)?;
        ensure_pollable_spec_revision_in_tx(&mut tx, upstream_id, expected_spec_revision).await?;
        // One statement: a missing row is inserted already claimed; an
        // existing row is claimed only while it is still eligible, due, and
        // unclaimed (or its lease expired). A concurrent claimer blocks on the
        // row lock and re-evaluates the WHERE against the winner's committed
        // row, so exactly one claim lands; `rows_affected` is 0 otherwise.
        let claimed = sqlx::query(
            "INSERT INTO upstream_api_key_details_v1 (
                upstream_id, verified, key_class, key_hint, needs_workspace, capabilities_json,
                usage_poll_claim_token, usage_poll_claim_lease_until_unix_secs,
                created_at_unix_secs, updated_at_unix_secs
             ) VALUES ($1, FALSE, 'unknown', $2, FALSE, $3, $4, $5, $6, $6)
             ON CONFLICT (upstream_id) DO UPDATE
                SET usage_poll_claim_token = EXCLUDED.usage_poll_claim_token,
                    usage_poll_claim_lease_until_unix_secs =
                        EXCLUDED.usage_poll_claim_lease_until_unix_secs,
                    updated_at_unix_secs = EXCLUDED.updated_at_unix_secs
              WHERE (upstream_api_key_details_v1.usage_poll_claim_token IS NULL
                     OR upstream_api_key_details_v1.usage_poll_claim_lease_until_unix_secs <= $7)
                AND ( \
                     (EXISTS (SELECT 1 FROM upstream_spec_v1 spec \
                              WHERE spec.id = upstream_api_key_details_v1.upstream_id \
                                AND spec.kind = 'anthropic_api_key') \
                      AND (upstream_api_key_details_v1.usage_report_state IS NULL \
                           OR (upstream_api_key_details_v1.usage_report_state = 'ok' \
                               AND upstream_api_key_details_v1.key_class = 'org_scoped'))) \
                     OR (EXISTS (SELECT 1 FROM upstream_spec_v1 spec \
                                 WHERE spec.id = upstream_api_key_details_v1.upstream_id \
                                   AND spec.kind IN ('openai_api_key', 'gemini_api_key')) \
                         AND (upstream_api_key_details_v1.provider_report_state IS NULL \
                              OR upstream_api_key_details_v1.provider_report_state IN ('unavailable', 'failed', 'stale'))) \
                ) \
                AND (upstream_api_key_details_v1.usage_next_poll_after_unix_secs IS NULL \
                     OR upstream_api_key_details_v1.usage_next_poll_after_unix_secs <= $7)",
        )
        .bind(upstream_id)
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
        let now = unix_secs(self.clock.now());
        let mut tx = self.pool.begin().await.map_err(map_sqlx_error)?;
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
                 WHERE upstream_id = $1 AND usage_poll_claim_token = $7"
            ),
            None => concat!(poll_state_update_prefix!(), " WHERE upstream_id = $1"),
        };
        let mut update = sqlx::query(query)
            .bind(upstream_id)
            .bind(u32_to_i32(
                outcome.consecutive_failures,
                "usage_poll_failures",
            )?)
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
            .bind(u64_to_i64(now, "updated_at_unix_secs")?);
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
                    SET usage_report_state = $2
                  WHERE upstream_id = $1 AND access_revision = $3",
            )
            .bind(upstream_id)
            .bind(state.as_str())
            .bind(u64_to_i64(outcome.access_revision_seen, "access_revision")?)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
        }
        if outcome.provider.is_some()
            || outcome.provider_report_state.is_some()
            || outcome.provider_report_status_metadata.is_some()
        {
            sqlx::query(provider_report_update!())
                .bind(upstream_id)
                .bind(outcome.provider.map(provider_as_str))
                .bind(
                    outcome
                        .provider_report_state
                        .map(ProviderReportState::as_str),
                )
                .bind(outcome.provider_report_status_metadata.as_deref())
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
             WHERE upstream_id = $1 AND attribution = $2 AND subject_id = $3 \
               AND bucket_width_secs = $4 \
               AND bucket_start_unix_secs >= $5 AND bucket_start_unix_secs < $6 \
             ORDER BY bucket_start_unix_secs, model"
        ))
        .bind(query.upstream_id)
        .bind(query.attribution.as_str())
        .bind(&query.subject_id)
        .bind(u32_to_i32(query.bucket_width_secs, "bucket_width_secs")?)
        .bind(u64_to_i64(query.start_unix_secs, "usage query start")?)
        .bind(u64_to_i64(query.end_unix_secs, "usage query end")?)
        .fetch_all(&self.pool)
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
        let removed = sqlx::query(
            "DELETE FROM upstream_api_usage_v1 \
             WHERE ctid IN ( \
                 SELECT ctid FROM upstream_api_usage_v1 \
                 WHERE bucket_start_unix_secs < $1 \
                 ORDER BY bucket_start_unix_secs \
                 LIMIT $2 \
                 FOR UPDATE SKIP LOCKED \
             )",
        )
        .bind(u64_to_i64(before_unix_secs, "usage prune cutoff")?)
        .bind(usize_to_i64(batch_size, "usage prune batch size")?)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx_error)?
        .rows_affected();
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
             WHERE upstream_id = $1 AND list_ts_ms >= $2 AND list_status IS NOT NULL \
             ORDER BY list_ts_ms DESC, list_event_key DESC \
             LIMIT 1",
        )
        .bind(upstream_id)
        .bind(u64_to_i64(since_unix_ms, "request outcome since")?)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx_error)?;
        let Some(row) = row else {
            return Ok(None);
        };
        let status = row
            .try_get::<i32, _>("list_status")
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

/// Write guard: the spec is live and still at `expected_revision`. `FOR SHARE`
/// keeps a concurrent spec update or soft delete from committing underneath
/// this transaction while letting other details writers proceed.
async fn ensure_live_spec_revision_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    upstream_id: Uuid,
    expected_revision: u64,
) -> StorageResult<()> {
    let row: Option<i32> = sqlx::query_scalar(
        "SELECT 1 FROM upstream_spec_v1 \
         WHERE id = $1 AND spec_revision = $2 AND deleted_at IS NULL \
         FOR SHARE",
    )
    .bind(upstream_id)
    .bind(u64_to_i64(expected_revision, "upstream spec revision")?)
    .fetch_optional(&mut **tx)
    .await
    .map_err(map_sqlx_error)?;
    if row.is_none() {
        return Err(conflict("stale or deleted upstream"));
    }
    Ok(())
}
/// Access writes take the spec lock exclusively before locking the details row,
/// avoiding a shared-to-exclusive upgrade deadlock with concurrent writers.
async fn ensure_live_spec_revision_for_access_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    upstream_id: Uuid,
    expected_revision: u64,
) -> StorageResult<()> {
    let row: Option<i32> = sqlx::query_scalar(
        "SELECT 1 FROM upstream_spec_v1 \
         WHERE id = $1 AND spec_revision = $2 AND deleted_at IS NULL \
         FOR UPDATE",
    )
    .bind(upstream_id)
    .bind(u64_to_i64(expected_revision, "upstream spec revision")?)
    .fetch_optional(&mut **tx)
    .await
    .map_err(map_sqlx_error)?;
    if row.is_none() {
        return Err(conflict("stale or deleted upstream"));
    }
    Ok(())
}

/// Poll guard: [`ensure_live_spec_revision_in_tx`] plus `enabled`, under the
/// same `FOR SHARE` lock. Candidate selection only lists enabled upstreams; a
/// disable that lands after that read bumps the revision, and this check
/// keeps a claim or a claimed completion from ever succeeding against a
/// disabled spec.
async fn ensure_pollable_spec_revision_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    upstream_id: Uuid,
    expected_revision: u64,
) -> StorageResult<()> {
    let row: Option<i32> = sqlx::query_scalar(
        "SELECT 1 FROM upstream_spec_v1 \
         WHERE id = $1 AND spec_revision = $2 AND deleted_at IS NULL AND enabled \
         FOR SHARE",
    )
    .bind(upstream_id)
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
    tx: &mut Transaction<'_, Postgres>,
    upstream_id: Uuid,
    key_hint: &str,
    now_unix_secs: u64,
) -> StorageResult<()> {
    sqlx::query(
        "INSERT INTO upstream_api_key_details_v1 (
            upstream_id, verified, key_class, key_hint, needs_workspace, capabilities_json,
            created_at_unix_secs, updated_at_unix_secs
         ) VALUES ($1, FALSE, 'unknown', $2, FALSE, $3, $4, $4)
         ON CONFLICT (upstream_id) DO NOTHING",
    )
    .bind(upstream_id)
    .bind(key_hint)
    .bind(serde_json::to_string(&ApiKeyCapabilities::not_checked())?)
    .bind(u64_to_i64(now_unix_secs, "details timestamp")?)
    .execute(&mut **tx)
    .await
    .map_err(map_sqlx_error)?;
    Ok(())
}

/// Returns whether the provider project identity changes. The details row is
/// locked so the comparison and subsequent write share one transaction.
pub(crate) async fn api_key_access_project_changed_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    upstream_id: Uuid,
    access: &ApiKeyAccessWrite,
) -> StorageResult<bool> {
    let previous: Option<Option<String>> = sqlx::query_scalar(
        "SELECT project_id
           FROM upstream_api_key_details_v1
          WHERE upstream_id = $1
          FOR UPDATE",
    )
    .bind(upstream_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(map_sqlx_error)?;
    Ok(previous.flatten().as_deref() != access.project_id.as_deref())
}

/// Capability/access column write inside a transaction that already
/// validated the spec revision. It also releases any usage-poll claim: a poll
/// in flight read the previous credential/workspace, so its completion is
/// rejected and the next poll starts from the new access state.
pub(crate) async fn write_api_key_access_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    upstream_id: Uuid,
    access: &ApiKeyAccessWrite,
    now_unix_secs: u64,
) -> StorageResult<()> {
    insert_details_if_missing_in_tx(tx, upstream_id, &access.key_hint, now_unix_secs).await?;
    sqlx::query(
        "UPDATE upstream_api_key_details_v1
            SET verified = $2,
                key_class = $3,
                key_hint = $4,
                api_key_id = $5,
                needs_workspace = $6,
                capabilities_json = $7,
                remediation_json = $8,
                usage_report_state = $9,
                provider = $10,
                project_id = $11,
                project_source = $12,
                provider_report_state = $13,
                provider_report_status_metadata_json = $14,
                last_error = $15,
                checked_at_unix_secs = $16,
                last_attempt_at_unix_secs = $17,
                access_revision = access_revision + 1,
                usage_poll_claim_token = NULL,
                usage_poll_claim_lease_until_unix_secs = NULL,
                updated_at_unix_secs = $18
          WHERE upstream_id = $1",
    )
    .bind(upstream_id)
    .bind(access.verified)
    .bind(access.key_class.as_str())
    .bind(&access.key_hint)
    .bind(access.api_key_id.as_deref())
    .bind(access.needs_workspace)
    .bind(serde_json::to_string(&access.capabilities)?)
    .bind(serde_json::to_string(&access.remediation)?)
    .bind(access.usage_report_state.map(ApiKeyOrgCapability::as_str))
    .bind(access.provider.map(provider_as_str))
    .bind(&access.project_id)
    .bind(access.project_source.map(ProviderProjectSource::as_str))
    .bind(
        access
            .provider_report_state
            .map(ProviderReportState::as_str),
    )
    .bind(access.provider_report_status_metadata.as_deref())
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
    .execute(&mut **tx)
    .await
    .map_err(map_sqlx_error)?;
    Ok(())
}

/// Zeroes poll failures and clears the poll schedule and success/attempt
/// timestamps. Usage rows are kept; a missing details row stays missing.
pub(crate) async fn reset_api_key_poll_state_in_tx(
    tx: &mut Transaction<'_, Postgres>,
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
                updated_at_unix_secs = $2
          WHERE upstream_id = $1",
    )
    .bind(upstream_id)
    .bind(u64_to_i64(now_unix_secs, "updated_at_unix_secs")?)
    .execute(&mut **tx)
    .await
    .map_err(map_sqlx_error)?;
    Ok(())
}

async fn upsert_bucket_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    upstream_id: Uuid,
    bucket: &UpstreamApiUsageBucketWrite,
    observed_at_unix_secs: u64,
) -> StorageResult<()> {
    sqlx::query(concat!(
        "INSERT INTO upstream_api_usage_v1 (upstream_id, ",
        bucket_columns!(),
        ") \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13) \
         ON CONFLICT (upstream_id, attribution, subject_id, bucket_width_secs, bucket_start_unix_secs, model) \
         DO UPDATE SET uncached_input_tokens = EXCLUDED.uncached_input_tokens, \
                       cache_creation_5m_input_tokens = EXCLUDED.cache_creation_5m_input_tokens, \
                       cache_creation_1h_input_tokens = EXCLUDED.cache_creation_1h_input_tokens, \
                       cache_read_input_tokens = EXCLUDED.cache_read_input_tokens, \
                       output_tokens = EXCLUDED.output_tokens, \
                       list_cost_micros = EXCLUDED.list_cost_micros, \
                       observed_at_unix_secs = EXCLUDED.observed_at_unix_secs"
    ))
    .bind(upstream_id)
    .bind(bucket.attribution.as_str())
    .bind(&bucket.subject_id)
    .bind(u32_to_i32(bucket.bucket_width_secs, "bucket_width_secs")?)
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

fn details_from_row(row: &PgRow) -> StorageResult<Option<UpstreamApiKeyDetailsRecord>> {
    let Some(upstream_id) = row
        .try_get::<Option<Uuid>, _>("details_upstream_id")
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
    let provider = row
        .try_get::<Option<String>, _>("details_provider")
        .map_err(map_sqlx_error)?;
    let project_source = row
        .try_get::<Option<String>, _>("details_project_source")
        .map_err(map_sqlx_error)?;
    let provider_report_state = row
        .try_get::<Option<String>, _>("details_provider_report_state")
        .map_err(map_sqlx_error)?;
    Ok(Some(UpstreamApiKeyDetailsRecord {
        upstream_id,
        verified: row.try_get("details_verified").map_err(map_sqlx_error)?,
        key_class: ApiKeyClass::parse(&key_class).ok_or_else(|| StorageError::Corrupted {
            message: format!("invalid api-key class {key_class}"),
        })?,
        key_hint: row.try_get("details_key_hint").map_err(map_sqlx_error)?,
        api_key_id: row.try_get("details_api_key_id").map_err(map_sqlx_error)?,
        needs_workspace: row
            .try_get("details_needs_workspace")
            .map_err(map_sqlx_error)?,
        capabilities: serde_json::from_str(&capabilities)?,
        remediation: serde_json::from_str(&remediation)?,
        usage_report_state: usage_report_state
            .map(|state| {
                ApiKeyOrgCapability::parse(&state).ok_or_else(|| StorageError::Corrupted {
                    message: format!("invalid usage_report_state {state}"),
                })
            })
            .transpose()?,
        provider: provider
            .as_deref()
            .map(|value| {
                parse_provider(value).ok_or_else(|| StorageError::Corrupted {
                    message: format!("invalid provider {value}"),
                })
            })
            .transpose()?,
        project_id: row.try_get("details_project_id").map_err(map_sqlx_error)?,
        project_source: project_source
            .as_deref()
            .map(|value| {
                ProviderProjectSource::parse(value).ok_or_else(|| StorageError::Corrupted {
                    message: format!("invalid project_source {value}"),
                })
            })
            .transpose()?,
        provider_report_state: provider_report_state
            .as_deref()
            .map(|value| {
                ProviderReportState::parse(value).ok_or_else(|| StorageError::Corrupted {
                    message: format!("invalid provider_report_state {value}"),
                })
            })
            .transpose()?,
        provider_report_status_metadata: row
            .try_get("details_provider_report_status_metadata_json")
            .map_err(map_sqlx_error)?,
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
        usage_poll_failures: i32_to_u32(
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

fn bucket_from_row(row: &PgRow) -> StorageResult<UpstreamApiUsageBucketRecord> {
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
        bucket_width_secs: i32_to_u32(
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

fn optional_i64_to_u64(value: Option<i64>, field: &str) -> StorageResult<Option<u64>> {
    value.map(|value| i64_to_u64(value, field)).transpose()
}

fn usize_to_i64(value: usize, field: &str) -> StorageResult<i64> {
    i64::try_from(value).map_err(|_| StorageError::Fatal {
        message: format!("{field} cannot be represented as postgres bigint"),
    })
}

fn u32_to_i32(value: u32, field: &str) -> StorageResult<i32> {
    i32::try_from(value).map_err(|_| StorageError::InvalidInput {
        field: field.to_owned(),
        reason: "exceeds postgres integer range".to_owned(),
    })
}

fn i32_to_u32(value: i32, field: &str) -> StorageResult<u32> {
    u32::try_from(value).map_err(|_| StorageError::Corrupted {
        message: format!("{field} is negative"),
    })
}
fn provider_as_str(provider: Provider) -> &'static str {
    match provider {
        Provider::Anthropic => "anthropic",
        Provider::Openai => "openai",
        Provider::Gemini => "gemini",
    }
}

fn parse_provider(value: &str) -> Option<Provider> {
    match value {
        "anthropic" => Some(Provider::Anthropic),
        "openai" => Some(Provider::Openai),
        "gemini" => Some(Provider::Gemini),
        _ => None,
    }
}

fn credential_ciphertext_record_from_postgres_row(
    row: Option<&PgRow>,
) -> StorageResult<Option<OpenAiOrganizationReportingCredentialCiphertextRecord>> {
    let Some(row) = row else {
        return Ok(None);
    };
    let status = row.try_get::<String, _>("status").map_err(map_sqlx_error)?;
    Ok(Some(
        OpenAiOrganizationReportingCredentialCiphertextRecord {
            encrypted_secret_ciphertext: row
                .try_get("encrypted_secret_ciphertext")
                .map_err(map_sqlx_error)?,
            status: OpenAiOrganizationReportingCredentialStatus::parse(&status).ok_or_else(
                || StorageError::Corrupted {
                    message: format!("invalid OpenAI reporting credential status {status}"),
                },
            )?,
            revision: i64_to_u64(
                row.try_get("revision").map_err(map_sqlx_error)?,
                "credential revision",
            )?,
        },
    ))
}

fn credential_record_from_postgres_row(
    row: Option<&PgRow>,
) -> StorageResult<Option<OpenAiOrganizationReportingCredentialRecord>> {
    let Some(row) = row else {
        return Ok(None);
    };
    let status = row.try_get::<String, _>("status").map_err(map_sqlx_error)?;
    Ok(Some(OpenAiOrganizationReportingCredentialRecord {
        scope: row.try_get("scope").map_err(map_sqlx_error)?,
        redacted_hint: row.try_get("redacted_hint").map_err(map_sqlx_error)?,
        status: OpenAiOrganizationReportingCredentialStatus::parse(&status).ok_or_else(|| {
            StorageError::Corrupted {
                message: format!("invalid OpenAI reporting credential status {status}"),
            }
        })?,
        revision: i64_to_u64(
            row.try_get("revision").map_err(map_sqlx_error)?,
            "credential revision",
        )?,
        updated_at_unix_secs: i64_to_u64(
            row.try_get("updated_at_unix_secs")
                .map_err(map_sqlx_error)?,
            "credential updated_at_unix_secs",
        )?,
    }))
}
