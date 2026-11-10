use async_trait::async_trait;
use cc_lb_storage_api::{
    CacheKeepaliveEnqueueState, CacheKeepaliveGenerationCheck, CacheKeepaliveHitRefreshRequest,
    CacheKeepaliveReplaceRequest, CacheKeepaliveSessionRecord, CacheKeepaliveSessionStatus,
    CacheKeepaliveSessionStore, CacheKeepaliveTerminalReason, CacheTtl, StorageError,
    StorageResult, cache_keepalive_job_key, cache_keepalive_order_meta_key,
    format_keepalive_order_meta, parse_keepalive_order_meta,
};
use sqlx::{Row, postgres::PgRow};

use crate::{
    adapter::{PostgresStorage, i64_to_u64, u64_to_i64},
    error_map::map_sqlx_error,
};
const TERMINAL_SESSION_RETENTION_SECS: u64 = 7 * 24 * 60 * 60;

#[async_trait]
impl CacheKeepaliveSessionStore for PostgresStorage {
    async fn prune_cache_keepalive_history_before(
        &self,
        cutoff_unix_secs: u64,
        batch_size: usize,
    ) -> StorageResult<u64> {
        if batch_size == 0 {
            return Ok(0);
        }
        let cutoff = u64_to_i64(cutoff_unix_secs, "keepalive history prune cutoff")?;
        let evidence_cutoff = u64_to_i64(
            cutoff_unix_secs
                .saturating_sub(60 * 60)
                .saturating_mul(1_000),
            "keepalive evidence prune cutoff",
        )?;
        let limit = u64_to_i64(batch_size as u64, "keepalive history prune batch size")?;
        let mut tx = self.pool.begin().await.map_err(map_sqlx_error)?;
        let mut removed = 0u64;
        for sql in [
            "DELETE FROM cache_keepalive_decisions WHERE ctid IN (SELECT ctid FROM cache_keepalive_decisions WHERE ts < $1 ORDER BY ts LIMIT $2)",
            "DELETE FROM cache_keepalive_turns WHERE ctid IN (SELECT ctid FROM cache_keepalive_turns WHERE ts < $1 ORDER BY ts LIMIT $2)",
        ] {
            if removed == limit as u64 {
                break;
            }
            removed += sqlx::query(sql)
                .bind(cutoff)
                .bind(limit - removed as i64)
                .execute(&mut *tx)
                .await
                .map_err(map_sqlx_error)?
                .rows_affected();
        }
        if removed < limit as u64 {
            removed += sqlx::query(
                "DELETE FROM cache_keepalive_evidence WHERE ctid IN (
                    SELECT ctid FROM cache_keepalive_evidence
                    WHERE request_start_ms < $1
                    ORDER BY request_start_ms, evidence_id
                    LIMIT $2
                )",
            )
            .bind(evidence_cutoff)
            .bind(limit - removed as i64)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?
            .rows_affected();
        }
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(removed)
    }

    async fn replace_from_real_request(
        &self,
        request: &CacheKeepaliveReplaceRequest,
    ) -> StorageResult<CacheKeepaliveSessionRecord> {
        let config_snapshot = serde_json::to_string(&request.config_snapshot)?;
        let last_message_at_ms = request
            .cache_anchor_at_unix_secs
            .checked_mul(1_000)
            .ok_or_else(|| StorageError::InvalidInput {
                field: "cache_keepalive_last_message_at_ms".to_owned(),
                reason: "value exceeds u64 milliseconds range".to_owned(),
            })?;
        let row = sqlx::query(
            "INSERT INTO cache_keepalive_sessions
             (session_key_hash, principal_id, accounting_key_id, upstream_id, cycle_id, generation, refresh_count,
              first_scheduled_at, cache_anchor_at, run_at, ttl, status, enqueue_state, running_since_unix_secs,
              current_job_key, encrypted_payload, display_reason, error, config_snapshot, last_message_at_ms, terminal_reason, expires_at, created_at, updated_at)
             VALUES ($1, $2, $3, $4, $5, 1, 0, $6, $7, $8, $9, 'active', 'pending', NULL, $10, $11, $12, NULL, $13, $14, NULL, $15, $16, $17)
             ON CONFLICT(session_key_hash) DO UPDATE SET
              principal_id = EXCLUDED.principal_id,
              accounting_key_id = EXCLUDED.accounting_key_id,
              upstream_id = EXCLUDED.upstream_id,
              cycle_id = EXCLUDED.cycle_id,
              generation = cache_keepalive_sessions.generation + 1,
              refresh_count = 0,
              first_scheduled_at = EXCLUDED.first_scheduled_at,
              cache_anchor_at = EXCLUDED.cache_anchor_at,
              run_at = EXCLUDED.run_at,
              ttl = EXCLUDED.ttl,
              status = 'active',
              enqueue_state = 'pending',
              running_since_unix_secs = NULL,
              current_job_key = 'cache_keepalive:' || EXCLUDED.session_key_hash || ':' || (cache_keepalive_sessions.generation + 1),
              encrypted_payload = EXCLUDED.encrypted_payload,
              display_reason = EXCLUDED.display_reason,
              error = NULL,
              config_snapshot = EXCLUDED.config_snapshot,
              last_message_at_ms = EXCLUDED.last_message_at_ms,
              terminal_reason = NULL,
              expires_at = EXCLUDED.expires_at,
              updated_at = EXCLUDED.updated_at
             RETURNING *",
        )
        .bind(&request.session_key_hash)
        .bind(&request.principal_id)
        .bind(&request.accounting_key_id)
        .bind(request.upstream_id)
        .bind(request.cycle_id)
        .bind(u64_to_i64(request.cache_anchor_at_unix_secs, "cache keepalive first_scheduled_at")?)
        .bind(u64_to_i64(request.cache_anchor_at_unix_secs, "cache keepalive cache_anchor_at")?)
        .bind(u64_to_i64(request.run_at_unix_secs, "cache keepalive run_at")?)
        .bind(ttl_to_db(request.ttl))
        .bind(cache_keepalive_job_key(&request.session_key_hash, 1))
        .bind(&request.encrypted_payload)
        .bind(&request.display_reason)
        .bind(&config_snapshot)
        .bind(u64_to_i64(
            last_message_at_ms,
            "cache keepalive last_message_at_ms",
        )?)
        .bind(u64_to_i64(request.expires_at_unix_secs, "cache keepalive expires_at")?)
        .bind(u64_to_i64(request.now_unix_secs, "cache keepalive created_at")?)
        .bind(u64_to_i64(request.now_unix_secs, "cache keepalive updated_at")?)
        .fetch_one(&self.pool)
        .await
        .map_err(map_sqlx_error)?;
        row_to_record(row)
    }

    async fn allocate_cache_keepalive_completion_ticket(
        &self,
        session_key_hash: &str,
    ) -> StorageResult<u64> {
        let key = cache_keepalive_order_meta_key(session_key_hash);
        let mut tx = self.pool.begin().await.map_err(map_sqlx_error)?;
        sqlx::query(
            "INSERT INTO meta (key, value) VALUES ($1, '0:0') ON CONFLICT (key) DO NOTHING",
        )
        .bind(&key)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;
        let existing =
            sqlx::query_scalar::<_, String>("SELECT value FROM meta WHERE key = $1 FOR UPDATE")
                .bind(&key)
                .fetch_optional(&mut *tx)
                .await
                .map_err(map_sqlx_error)?;
        let (issued, applied) = parse_keepalive_order_meta(existing.as_deref());
        let next_issued = issued.checked_add(1).ok_or_else(|| StorageError::Fatal {
            message: "cache keepalive completion ticket overflow".to_owned(),
        })?;
        let updated = format_keepalive_order_meta(next_issued, applied);
        sqlx::query("UPDATE meta SET value = $2 WHERE key = $1")
            .bind(&key)
            .bind(&updated)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(next_issued)
    }

    async fn replace_from_real_request_fenced(
        &self,
        request: &CacheKeepaliveReplaceRequest,
        expected_ticket: Option<u64>,
    ) -> StorageResult<Option<CacheKeepaliveSessionRecord>> {
        let config_snapshot = serde_json::to_string(&request.config_snapshot)?;
        let last_message_at_ms = request
            .cache_anchor_at_unix_secs
            .checked_mul(1_000)
            .ok_or_else(|| StorageError::InvalidInput {
                field: "cache_keepalive_last_message_at_ms".to_owned(),
                reason: "value exceeds u64 milliseconds range".to_owned(),
            })?;
        let mut tx = self.pool.begin().await.map_err(map_sqlx_error)?;
        if let Some(ticket) = expected_ticket {
            let order_key = cache_keepalive_order_meta_key(&request.session_key_hash);
            sqlx::query(
                "INSERT INTO meta (key, value) VALUES ($1, '0:0') ON CONFLICT (key) DO NOTHING",
            )
            .bind(&order_key)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
            let order_raw =
                sqlx::query_scalar::<_, String>("SELECT value FROM meta WHERE key = $1 FOR UPDATE")
                    .bind(&order_key)
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(map_sqlx_error)?;
            let (issued, applied) = parse_keepalive_order_meta(order_raw.as_deref());
            if ticket <= applied {
                return Ok(None);
            }
            let updated_order = format_keepalive_order_meta(issued.max(ticket), ticket);
            sqlx::query("UPDATE meta SET value = $2 WHERE key = $1")
                .bind(&order_key)
                .bind(&updated_order)
                .execute(&mut *tx)
                .await
                .map_err(map_sqlx_error)?;
        }
        // Real tickets compare the stable real-completion boundary; synthetic
        // hits advance cache_anchor_at but leave first_scheduled_at unchanged.
        let row = sqlx::query(
            "INSERT INTO cache_keepalive_sessions
             (session_key_hash, principal_id, accounting_key_id, upstream_id, cycle_id, generation, refresh_count,
              first_scheduled_at, cache_anchor_at, run_at, ttl, status, enqueue_state, running_since_unix_secs,
              current_job_key, encrypted_payload, display_reason, error, config_snapshot, last_message_at_ms, terminal_reason, expires_at, created_at, updated_at)
             VALUES ($1, $2, $3, $4, $5, 1, 0, $6, $7, $8, $9, 'active', 'pending', NULL, $10, $11, $12, NULL, $13, $14, NULL, $15, $16, $17)
             ON CONFLICT(session_key_hash) DO UPDATE SET
              principal_id = EXCLUDED.principal_id,
              accounting_key_id = EXCLUDED.accounting_key_id,
              upstream_id = EXCLUDED.upstream_id,
              cycle_id = EXCLUDED.cycle_id,
              generation = cache_keepalive_sessions.generation + 1,
              refresh_count = 0,
              first_scheduled_at = EXCLUDED.first_scheduled_at,
              cache_anchor_at = EXCLUDED.cache_anchor_at,
              run_at = EXCLUDED.run_at,
              ttl = EXCLUDED.ttl,
              status = 'active',
              enqueue_state = 'pending',
              running_since_unix_secs = NULL,
              current_job_key = 'cache_keepalive:' || EXCLUDED.session_key_hash || ':' || (cache_keepalive_sessions.generation + 1),
              encrypted_payload = EXCLUDED.encrypted_payload,
              display_reason = EXCLUDED.display_reason,
              error = NULL,
              config_snapshot = EXCLUDED.config_snapshot,
              last_message_at_ms = EXCLUDED.last_message_at_ms,
              terminal_reason = NULL,
              expires_at = EXCLUDED.expires_at,
              updated_at = EXCLUDED.updated_at
             WHERE ($18 AND cache_keepalive_sessions.first_scheduled_at <= EXCLUDED.cache_anchor_at)
                OR (NOT $18 AND cache_keepalive_sessions.cache_anchor_at <= EXCLUDED.cache_anchor_at
                    AND (cache_keepalive_sessions.status != 'terminal' OR cache_keepalive_sessions.updated_at <= EXCLUDED.cache_anchor_at))
             RETURNING *",
        )
        .bind(&request.session_key_hash)
        .bind(&request.principal_id)
        .bind(&request.accounting_key_id)
        .bind(request.upstream_id)
        .bind(request.cycle_id)
        .bind(u64_to_i64(
            request.cache_anchor_at_unix_secs,
            "cache keepalive first_scheduled_at",
        )?)
        .bind(u64_to_i64(
            request.cache_anchor_at_unix_secs,
            "cache keepalive cache_anchor_at",
        )?)
        .bind(u64_to_i64(
            request.run_at_unix_secs,
            "cache keepalive run_at",
        )?)
        .bind(ttl_to_db(request.ttl))
        .bind(cache_keepalive_job_key(&request.session_key_hash, 1))
        .bind(&request.encrypted_payload)
        .bind(&request.display_reason)
        .bind(&config_snapshot)
        .bind(u64_to_i64(
            last_message_at_ms,
            "cache keepalive last_message_at_ms",
        )?)
        .bind(u64_to_i64(
            request.expires_at_unix_secs,
            "cache keepalive expires_at",
        )?)
        .bind(u64_to_i64(
            request.now_unix_secs,
            "cache keepalive created_at",
        )?)
        .bind(u64_to_i64(
            request.now_unix_secs,
            "cache keepalive updated_at",
        )?)
        .bind(expected_ticket.is_some())
        .fetch_optional(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;
        let Some(row) = row else {
            return Ok(None);
        };
        let record = row_to_record(row)?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(Some(record))
    }

    async fn get_cache_keepalive_session(
        &self,
        session_key_hash: &str,
    ) -> StorageResult<Option<CacheKeepaliveSessionRecord>> {
        let row = sqlx::query("SELECT * FROM cache_keepalive_sessions WHERE session_key_hash = $1")
            .bind(session_key_hash)
            .fetch_optional(&self.pool)
            .await
            .map_err(map_sqlx_error)?;
        row.map(row_to_record).transpose()
    }

    async fn mark_cache_keepalive_enqueued(
        &self,
        session_key_hash: &str,
        generation: u64,
        now_unix_secs: u64,
    ) -> StorageResult<bool> {
        let result = sqlx::query(
            "UPDATE cache_keepalive_sessions SET enqueue_state = 'enqueued', running_since_unix_secs = NULL, updated_at = $1
             WHERE session_key_hash = $2 AND generation = $3 AND status = 'active' AND enqueue_state = 'pending'",
        )
        .bind(u64_to_i64(now_unix_secs, "cache keepalive updated_at")?)
        .bind(session_key_hash)
        .bind(u64_to_i64(generation, "cache keepalive generation")?)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx_error)?;
        Ok(result.rows_affected() == 1)
    }

    async fn claim_cache_keepalive_turn(
        &self,
        session_key_hash: &str,
        generation: u64,
        now_unix_secs: u64,
    ) -> StorageResult<bool> {
        let now_unix_secs = u64_to_i64(now_unix_secs, "cache keepalive running_since_unix_secs")?;
        let result = sqlx::query(
            "UPDATE cache_keepalive_sessions
             SET enqueue_state = 'running', running_since_unix_secs = $1, updated_at = $1
             WHERE session_key_hash = $2 AND generation = $3 AND status = 'active'
               AND (enqueue_state = 'enqueued' OR (enqueue_state = 'pending' AND octet_length(encrypted_payload) > 0))",
        )
        .bind(now_unix_secs)
        .bind(session_key_hash)
        .bind(u64_to_i64(generation, "cache keepalive generation")?)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx_error)?;
        Ok(result.rows_affected() == 1)
    }

    async fn authorize_cache_keepalive_dispatch(
        &self,
        session_key_hash: &str,
        generation: u64,
    ) -> StorageResult<bool> {
        sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (
                 SELECT 1
                 FROM cache_keepalive_sessions
                 WHERE session_key_hash = $1 AND generation = $2 AND status = 'active'
                   AND enqueue_state = 'running'
             )",
        )
        .bind(session_key_hash)
        .bind(u64_to_i64(generation, "cache keepalive generation")?)
        .fetch_one(&self.pool)
        .await
        .map_err(map_sqlx_error)
    }

    async fn update_cache_keepalive_payload(
        &self,
        session_key_hash: &str,
        generation: u64,
        encrypted_payload: &[u8],
        now_unix_secs: u64,
    ) -> StorageResult<bool> {
        let result = sqlx::query(
            "UPDATE cache_keepalive_sessions SET encrypted_payload = $1, updated_at = $2
             WHERE session_key_hash = $3 AND generation = $4 AND status = 'active' AND enqueue_state = 'pending'",
        )
        .bind(encrypted_payload)
        .bind(u64_to_i64(now_unix_secs, "cache keepalive updated_at")?)
        .bind(session_key_hash)
        .bind(u64_to_i64(generation, "cache keepalive generation")?)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx_error)?;
        Ok(result.rows_affected() == 1)
    }

    async fn check_cache_keepalive_generation(
        &self,
        session_key_hash: &str,
    ) -> StorageResult<Option<CacheKeepaliveGenerationCheck>> {
        let row = sqlx::query("SELECT generation, status, enqueue_state FROM cache_keepalive_sessions WHERE session_key_hash = $1")
            .bind(session_key_hash)
            .fetch_optional(&self.pool)
            .await
            .map_err(map_sqlx_error)?;
        row.map(row_to_check).transpose()
    }

    async fn reschedule_after_cache_hit(
        &self,
        request: &CacheKeepaliveHitRefreshRequest,
    ) -> StorageResult<Option<CacheKeepaliveSessionRecord>> {
        let next_generation =
            request
                .generation
                .checked_add(1)
                .ok_or_else(|| StorageError::Fatal {
                    message: "cache keepalive generation overflow".to_owned(),
                })?;
        let next_generation_db = u64_to_i64(next_generation, "cache keepalive generation")?;
        let cache_anchor_at = u64_to_i64(
            request.cache_anchor_at_unix_secs,
            "cache keepalive cache_anchor_at",
        )?;
        let run_at = u64_to_i64(request.run_at_unix_secs, "cache keepalive run_at")?;
        let expires_at = u64_to_i64(request.expires_at_unix_secs, "cache keepalive expires_at")?;
        let updated_at = u64_to_i64(request.now_unix_secs, "cache keepalive updated_at")?;
        let generation = u64_to_i64(request.generation, "cache keepalive generation")?;
        let current_job_key = cache_keepalive_job_key(&request.session_key_hash, next_generation);
        let row = if let Some(encrypted_payload) = request.encrypted_payload.as_deref() {
            sqlx::query(
                "UPDATE cache_keepalive_sessions SET generation = $1, refresh_count = refresh_count + 1,
                 cache_anchor_at = $2, run_at = $3, status = 'active', enqueue_state = 'pending', running_since_unix_secs = NULL,
                 current_job_key = $4, encrypted_payload = $5, terminal_reason = NULL, error = NULL, expires_at = $6, updated_at = $7
                 WHERE session_key_hash = $8 AND generation = $9 AND status = 'active'
                 RETURNING *",
            )
            .bind(next_generation_db)
            .bind(cache_anchor_at)
            .bind(run_at)
            .bind(&current_job_key)
            .bind(encrypted_payload)
            .bind(expires_at)
            .bind(updated_at)
            .bind(&request.session_key_hash)
            .bind(generation)
            .fetch_optional(&self.pool)
            .await
            .map_err(map_sqlx_error)?
        } else {
            sqlx::query(
                "UPDATE cache_keepalive_sessions SET generation = $1, refresh_count = refresh_count + 1,
                 cache_anchor_at = $2, run_at = $3, status = 'active', enqueue_state = 'pending', running_since_unix_secs = NULL,
                 current_job_key = $4, terminal_reason = NULL, error = NULL, expires_at = $5, updated_at = $6
                 WHERE session_key_hash = $7 AND generation = $8 AND status = 'active'
                 RETURNING *",
            )
            .bind(next_generation_db)
            .bind(cache_anchor_at)
            .bind(run_at)
            .bind(&current_job_key)
            .bind(expires_at)
            .bind(updated_at)
            .bind(&request.session_key_hash)
            .bind(generation)
            .fetch_optional(&self.pool)
            .await
            .map_err(map_sqlx_error)?
        };
        row.map(row_to_record).transpose()
    }

    async fn mark_cache_keepalive_terminal(
        &self,
        session_key_hash: &str,
        generation: u64,
        reason: CacheKeepaliveTerminalReason,
        error: Option<&str>,
        now_unix_secs: u64,
    ) -> StorageResult<bool> {
        let error = error.map(cc_lb_storage_api::truncate_cache_keepalive_error);
        let retained_expires_at = u64_to_i64(
            now_unix_secs.saturating_add(TERMINAL_SESSION_RETENTION_SECS),
            "cache keepalive terminal expires_at",
        )?;
        let result = sqlx::query(
            "UPDATE cache_keepalive_sessions SET status = 'terminal', enqueue_state = 'pending', terminal_reason = $1, error = $2, running_since_unix_secs = NULL,
             encrypted_payload = ''::bytea, expires_at = GREATEST(expires_at, $6), updated_at = $3
             WHERE session_key_hash = $4 AND generation = $5 AND status = 'active'",
        )
        .bind(reason.as_str())
        .bind(error)
        .bind(u64_to_i64(now_unix_secs, "cache keepalive updated_at")?)
        .bind(session_key_hash)
        .bind(u64_to_i64(generation, "cache keepalive generation")?)
        .bind(retained_expires_at)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx_error)?;
        Ok(result.rows_affected() == 1)
    }

    async fn cancel_cache_keepalive_fenced(
        &self,
        session_key_hash: &str,
        expected_ticket: Option<u64>,
        cancel_anchor_at_unix_secs: u64,
        reason: CacheKeepaliveTerminalReason,
        now_unix_secs: u64,
    ) -> StorageResult<bool> {
        let mut tx = self.pool.begin().await.map_err(map_sqlx_error)?;
        if let Some(ticket) = expected_ticket {
            let order_key = cache_keepalive_order_meta_key(session_key_hash);
            sqlx::query(
                "INSERT INTO meta (key, value) VALUES ($1, '0:0') ON CONFLICT (key) DO NOTHING",
            )
            .bind(&order_key)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
            let order_raw =
                sqlx::query_scalar::<_, String>("SELECT value FROM meta WHERE key = $1 FOR UPDATE")
                    .bind(&order_key)
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(map_sqlx_error)?;
            let (issued, applied) = parse_keepalive_order_meta(order_raw.as_deref());
            if ticket <= applied {
                return Ok(false);
            }
            let updated_order = format_keepalive_order_meta(issued.max(ticket), ticket);
            sqlx::query("UPDATE meta SET value = $2 WHERE key = $1")
                .bind(&order_key)
                .bind(&updated_order)
                .execute(&mut *tx)
                .await
                .map_err(map_sqlx_error)?;
        }
        // Ticketed cancellation advances the stable logical boundary while
        // leaving the synthetic scheduling/activity anchor unchanged.
        let retained_expires_at = u64_to_i64(
            now_unix_secs.saturating_add(TERMINAL_SESSION_RETENTION_SECS),
            "cache keepalive terminal expires_at",
        )?;
        let result = sqlx::query(
            "UPDATE cache_keepalive_sessions SET status = 'terminal', enqueue_state = 'pending', terminal_reason = $1, error = NULL, running_since_unix_secs = NULL,
             encrypted_payload = ''::bytea,
             expires_at = CASE WHEN refresh_count > 0 OR EXISTS (SELECT 1 FROM cache_keepalive_turns turn_row WHERE turn_row.principal_id = cache_keepalive_sessions.principal_id AND turn_row.session_key_hash = cache_keepalive_sessions.session_key_hash) THEN GREATEST(expires_at, $6) ELSE expires_at END,
             updated_at = $2,
             first_scheduled_at = CASE WHEN $4 THEN $5 ELSE first_scheduled_at END
             WHERE session_key_hash = $3 AND status = 'active'
               AND (CASE WHEN $4 THEN first_scheduled_at ELSE cache_anchor_at END) <= $5",
        )
        .bind(reason.as_str())
        .bind(u64_to_i64(now_unix_secs, "cache keepalive updated_at")?)
        .bind(session_key_hash)
        .bind(expected_ticket.is_some())
        .bind(u64_to_i64(
            cancel_anchor_at_unix_secs,
            "cache keepalive cancel_anchor_at",
        )?)
        .bind(retained_expires_at)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;
        if result.rows_affected() == 0 {
            // Rejecting a stale logical anchor must not consume its ticket.
            tx.rollback().await.map_err(map_sqlx_error)?;
            return Ok(false);
        }
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(true)
    }

    async fn mark_latest_cache_keepalive_terminal(
        &self,
        session_key_hash: &str,
        reason: CacheKeepaliveTerminalReason,
        now_unix_secs: u64,
    ) -> StorageResult<bool> {
        let retained_expires_at = u64_to_i64(
            now_unix_secs.saturating_add(TERMINAL_SESSION_RETENTION_SECS),
            "cache keepalive terminal expires_at",
        )?;
        let result = sqlx::query(
            "UPDATE cache_keepalive_sessions SET status = 'terminal', enqueue_state = 'pending', terminal_reason = $1, error = NULL, running_since_unix_secs = NULL,
             encrypted_payload = ''::bytea,
             expires_at = CASE WHEN refresh_count > 0 OR EXISTS (SELECT 1 FROM cache_keepalive_turns turn_row WHERE turn_row.principal_id = cache_keepalive_sessions.principal_id AND turn_row.session_key_hash = cache_keepalive_sessions.session_key_hash) THEN GREATEST(expires_at, $4) ELSE expires_at END,
             updated_at = $2
             WHERE session_key_hash = $3 AND status = 'active'",
        )
        .bind(reason.as_str())
        .bind(u64_to_i64(now_unix_secs, "cache keepalive updated_at")?)
        .bind(session_key_hash)
        .bind(retained_expires_at)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx_error)?;
        Ok(result.rows_affected() == 1)
    }
    async fn recover_cache_keepalive_stale_running(
        &self,
        stale_running_cutoff_unix_secs: u64,
        now_unix_secs: u64,
    ) -> StorageResult<u64> {
        let result = sqlx::query(
            "UPDATE cache_keepalive_sessions
             SET status = 'terminal', terminal_reason = 'stale', enqueue_state = 'pending',
                 error = 'keepalive renewal timed out while running',
                 running_since_unix_secs = NULL, encrypted_payload = ''::bytea, updated_at = $1
             WHERE status = 'active'
               AND enqueue_state = 'running'
               AND running_since_unix_secs IS NOT NULL
               AND running_since_unix_secs < $2",
        )
        .bind(u64_to_i64(now_unix_secs, "cache keepalive updated_at")?)
        .bind(u64_to_i64(
            stale_running_cutoff_unix_secs,
            "cache keepalive stale running cutoff",
        )?)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx_error)?;
        Ok(result.rows_affected())
    }

    async fn purge_cache_keepalive_expired(&self, cutoff_unix_secs: u64) -> StorageResult<u64> {
        let result = sqlx::query("DELETE FROM cache_keepalive_sessions WHERE expires_at < $1")
            .bind(u64_to_i64(
                cutoff_unix_secs,
                "cache keepalive purge cutoff",
            )?)
            .execute(&self.pool)
            .await
            .map_err(map_sqlx_error)?;
        sqlx::query(
            "DELETE FROM meta WHERE key LIKE 'keepalive_order:%'
             AND substr(key, 17) NOT IN (SELECT session_key_hash FROM cache_keepalive_sessions)",
        )
        .execute(&self.pool)
        .await
        .map_err(map_sqlx_error)?;
        sqlx::query(
            "DELETE FROM cache_keepalive_evidence AS f
             WHERE f.evidence_kind = 'follow_up'
               AND NOT EXISTS (
                   SELECT 1
                   FROM cache_keepalive_sessions AS s
                   WHERE s.principal_id = f.principal_id
                     AND s.upstream_id = f.upstream_id
                     AND s.status = 'active'
                     AND s.enqueue_state = 'running'
               )
               AND (
                   f.cache_read_input_tokens IS NULL
                   OR f.cache_read_input_tokens <= 0
                   OR f.prefix_hash IS NULL
                   OR NOT EXISTS (
                       SELECT 1
                       FROM cache_keepalive_evidence AS r
                       WHERE r.evidence_kind = 'renewal'
                         AND r.cycle_id IS NOT NULL
                         AND r.prefix_policy_version IS NOT NULL
                         AND r.prefix_schema_version IS NOT NULL
                         AND r.principal_id = f.principal_id
                         AND r.upstream_id = f.upstream_id
                         AND r.model = f.model
                         AND r.prefix_hash = f.prefix_hash
                         AND f.request_start_ms > r.request_start_ms
                         AND f.request_start_ms <= r.request_start_ms +
                             CASE r.ttl WHEN '1h' THEN 3600000 ELSE 300000 END
                         AND NOT EXISTS (
                             SELECT 1
                             FROM cache_keepalive_evidence AS e
                             WHERE e.evidence_kind = 'follow_up'
                               AND e.cache_read_input_tokens > 0
                               AND e.prefix_hash IS NOT NULL
                               AND e.principal_id = r.principal_id
                               AND e.upstream_id = r.upstream_id
                               AND e.model = r.model
                               AND (e.request_start_ms < f.request_start_ms OR
                                    (e.request_start_ms = f.request_start_ms AND e.evidence_id < f.evidence_id))
                               AND EXISTS (
                                   SELECT 1
                                   FROM cache_keepalive_evidence AS r2
                                   WHERE r2.evidence_kind = 'renewal'
                                     AND r2.cycle_id = r.cycle_id
                                     AND r2.cycle_id IS NOT NULL
                                     AND r2.prefix_policy_version IS NOT NULL
                                     AND r2.prefix_schema_version IS NOT NULL
                                     AND r2.principal_id = r.principal_id
                                     AND r2.upstream_id = r.upstream_id
                                     AND r2.model = r.model
                                     AND r2.prefix_hash = e.prefix_hash
                                     AND e.request_start_ms > r2.request_start_ms
                                     AND e.request_start_ms <= r2.request_start_ms +
                                         CASE r2.ttl WHEN '1h' THEN 3600000 ELSE 300000 END
                               )
                         )
                   )
             )",
        )
        .execute(&self.pool)
        .await
        .map_err(map_sqlx_error)?;

        Ok(result.rows_affected())
    }

    async fn purge_cache_keepalive_stale_pending(
        &self,
        cutoff_unix_secs: u64,
    ) -> StorageResult<u64> {
        let result = sqlx::query(
            "DELETE FROM cache_keepalive_sessions WHERE status = 'active' AND enqueue_state = 'pending' AND updated_at < $1",
        )
        .bind(u64_to_i64(cutoff_unix_secs, "cache keepalive purge cutoff")?)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx_error)?;
        Ok(result.rows_affected())
    }
}

fn row_to_check(row: PgRow) -> StorageResult<CacheKeepaliveGenerationCheck> {
    Ok(CacheKeepaliveGenerationCheck {
        generation: i64_to_u64(
            row.try_get("generation").map_err(map_sqlx_error)?,
            "cache keepalive generation",
        )?,
        status: status_from_db(
            row.try_get::<String, _>("status")
                .map_err(map_sqlx_error)?
                .as_str(),
        )?,
        enqueue_state: enqueue_state_from_db(
            row.try_get::<String, _>("enqueue_state")
                .map_err(map_sqlx_error)?
                .as_str(),
        )?,
    })
}

pub(crate) fn row_to_record(row: PgRow) -> StorageResult<CacheKeepaliveSessionRecord> {
    Ok(CacheKeepaliveSessionRecord {
        session_key_hash: row.try_get("session_key_hash").map_err(map_sqlx_error)?,
        principal_id: row.try_get("principal_id").map_err(map_sqlx_error)?,
        accounting_key_id: row.try_get("accounting_key_id").map_err(map_sqlx_error)?,
        upstream_id: row.try_get("upstream_id").map_err(map_sqlx_error)?,
        cycle_id: row.try_get("cycle_id").map_err(map_sqlx_error)?,
        generation: i64_to_u64(
            row.try_get("generation").map_err(map_sqlx_error)?,
            "cache keepalive generation",
        )?,
        refresh_count: i64_to_u32(
            row.try_get("refresh_count").map_err(map_sqlx_error)?,
            "cache keepalive refresh_count",
        )?,
        first_scheduled_at_unix_secs: i64_to_u64(
            row.try_get("first_scheduled_at").map_err(map_sqlx_error)?,
            "cache keepalive first_scheduled_at",
        )?,
        cache_anchor_at_unix_secs: i64_to_u64(
            row.try_get("cache_anchor_at").map_err(map_sqlx_error)?,
            "cache keepalive cache_anchor_at",
        )?,
        run_at_unix_secs: i64_to_u64(
            row.try_get("run_at").map_err(map_sqlx_error)?,
            "cache keepalive run_at",
        )?,
        ttl: ttl_from_db(&row.try_get::<String, _>("ttl").map_err(map_sqlx_error)?)?,
        status: status_from_db(&row.try_get::<String, _>("status").map_err(map_sqlx_error)?)?,
        enqueue_state: enqueue_state_from_db(
            &row.try_get::<String, _>("enqueue_state")
                .map_err(map_sqlx_error)?,
        )?,
        running_since_unix_secs: row
            .try_get::<Option<i64>, _>("running_since_unix_secs")
            .map_err(map_sqlx_error)?
            .map(|value| i64_to_u64(value, "cache keepalive running_since_unix_secs"))
            .transpose()?,
        current_job_key: row.try_get("current_job_key").map_err(map_sqlx_error)?,
        encrypted_payload: row.try_get("encrypted_payload").map_err(map_sqlx_error)?,
        display_reason: row.try_get("display_reason").map_err(map_sqlx_error)?,
        error: row.try_get("error").map_err(map_sqlx_error)?,
        config_snapshot: config_snapshot_from_db(
            row.try_get("config_snapshot").map_err(map_sqlx_error)?,
        )?,
        terminal_reason: row
            .try_get::<Option<String>, _>("terminal_reason")
            .map_err(map_sqlx_error)?
            .as_deref()
            .map(terminal_reason_from_db)
            .transpose()?,
        expires_at_unix_secs: i64_to_u64(
            row.try_get("expires_at").map_err(map_sqlx_error)?,
            "cache keepalive expires_at",
        )?,
        created_at_unix_secs: i64_to_u64(
            row.try_get("created_at").map_err(map_sqlx_error)?,
            "cache keepalive created_at",
        )?,
        updated_at_unix_secs: i64_to_u64(
            row.try_get("updated_at").map_err(map_sqlx_error)?,
            "cache keepalive updated_at",
        )?,
    })
}

fn ttl_to_db(ttl: CacheTtl) -> &'static str {
    match ttl {
        CacheTtl::Ttl5m => "5m",
        CacheTtl::Ttl1h => "1h",
        CacheTtl::Ttl30m => "30m",
        CacheTtl::Ttl12h => "12h",
    }
}

fn ttl_from_db(value: &str) -> StorageResult<CacheTtl> {
    match value {
        "5m" => Ok(CacheTtl::Ttl5m),
        "1h" => Ok(CacheTtl::Ttl1h),
        "30m" => Ok(CacheTtl::Ttl30m),
        "12h" => Ok(CacheTtl::Ttl12h),
        value => Err(StorageError::Corrupted {
            message: format!("invalid cache keepalive ttl {value}"),
        }),
    }
}

fn status_from_db(value: &str) -> StorageResult<CacheKeepaliveSessionStatus> {
    match value {
        "active" => Ok(CacheKeepaliveSessionStatus::Active),
        "terminal" => Ok(CacheKeepaliveSessionStatus::Terminal),
        value => Err(StorageError::Corrupted {
            message: format!("invalid cache keepalive status {value}"),
        }),
    }
}

fn enqueue_state_from_db(value: &str) -> StorageResult<CacheKeepaliveEnqueueState> {
    match value {
        "pending" => Ok(CacheKeepaliveEnqueueState::Pending),
        "enqueued" => Ok(CacheKeepaliveEnqueueState::Enqueued),
        "running" => Ok(CacheKeepaliveEnqueueState::Running),
        value => Err(StorageError::Corrupted {
            message: format!("invalid cache keepalive enqueue_state {value}"),
        }),
    }
}

fn terminal_reason_from_db(value: &str) -> StorageResult<CacheKeepaliveTerminalReason> {
    match value {
        "cancelled" => Ok(CacheKeepaliveTerminalReason::Cancelled),
        "expired" => Ok(CacheKeepaliveTerminalReason::Expired),
        "max_refreshes" => Ok(CacheKeepaliveTerminalReason::MaxRefreshes),
        "max_duration" => Ok(CacheKeepaliveTerminalReason::MaxDuration),
        "cache_miss" => Ok(CacheKeepaliveTerminalReason::CacheMiss),
        "dispatch_error" => Ok(CacheKeepaliveTerminalReason::DispatchError),
        "decrypt_failed" => Ok(CacheKeepaliveTerminalReason::DecryptFailed),
        "unsupported_provider" => Ok(CacheKeepaliveTerminalReason::UnsupportedProvider),
        "stale" => Ok(CacheKeepaliveTerminalReason::Stale),
        value => Err(StorageError::Corrupted {
            message: format!("invalid cache keepalive terminal_reason {value}"),
        }),
    }
}

fn i64_to_u32(value: i64, field: &str) -> StorageResult<u32> {
    u32::try_from(value).map_err(|_| StorageError::Corrupted {
        message: format!("invalid {field} value {value}"),
    })
}

fn config_snapshot_from_db(
    value: Option<String>,
) -> StorageResult<Option<cc_lb_storage_api::CacheKeepaliveConfigSnapshot>> {
    value
        .map(|json| {
            serde_json::from_str(&json).map_err(|error| StorageError::Corrupted {
                message: format!("invalid cache keepalive config snapshot: {error}"),
            })
        })
        .transpose()
}
