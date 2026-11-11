use async_trait::async_trait;
use cc_lb_storage_api::{
    CacheKeepaliveEnqueueState, CacheKeepaliveGenerationCheck, CacheKeepaliveHitRefreshRequest,
    CacheKeepaliveReplaceRequest, CacheKeepaliveSessionRecord, CacheKeepaliveSessionStatus,
    CacheKeepaliveSessionStore, CacheKeepaliveTerminalReason, CacheTtl, StorageError,
    StorageResult, cache_keepalive_job_key, cache_keepalive_order_meta_key,
    format_keepalive_order_meta, parse_keepalive_order_meta,
};
use sqlx::Row;
use uuid::Uuid;

use crate::{SqliteStorage, map_sqlx_error};

const TERMINAL_SESSION_RETENTION_SECS: u64 = 7 * 24 * 60 * 60;

#[async_trait]
impl CacheKeepaliveSessionStore for SqliteStorage {
    async fn prune_cache_keepalive_history_before(
        &self,
        cutoff_unix_secs: u64,
        batch_size: usize,
    ) -> StorageResult<u64> {
        if batch_size == 0 {
            return Ok(0);
        }
        let cutoff = u64_to_i64(cutoff_unix_secs, "keepalive history prune cutoff")?;
        let cutoff_ms = u64_to_i64(
            cutoff_unix_secs
                .saturating_sub(60 * 60)
                .saturating_mul(1_000),
            "keepalive evidence prune cutoff",
        )?;
        let limit =
            super::request_events::usize_to_i64(batch_size, "keepalive history prune batch size")?;
        let decisions_removed = sqlx::query(
            "DELETE FROM cache_keepalive_decisions WHERE rowid IN ( \
             SELECT rowid FROM cache_keepalive_decisions WHERE ts < ? \
             ORDER BY ts, source_ref_id LIMIT ?)",
        )
        .bind(cutoff)
        .bind(limit)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx_error)?
        .rows_affected();
        let remaining = limit - decisions_removed as i64;
        let turns_removed = if remaining > 0 {
            sqlx::query(
                "DELETE FROM cache_keepalive_turns WHERE rowid IN ( \
                 SELECT rowid FROM cache_keepalive_turns WHERE ts < ? \
                 ORDER BY ts, source_ref_id LIMIT ?)",
            )
            .bind(cutoff)
            .bind(remaining)
            .execute(&self.pool)
            .await
            .map_err(map_sqlx_error)?
            .rows_affected()
        } else {
            0
        };
        let remaining = remaining - turns_removed as i64;
        let evidence_removed = if remaining > 0 {
            sqlx::query(
                "DELETE FROM cache_keepalive_evidence WHERE rowid IN ( \
                 SELECT rowid FROM cache_keepalive_evidence \
                 WHERE request_start_ms < ? \
                 ORDER BY request_start_ms, evidence_id LIMIT ?)",
            )
            .bind(cutoff_ms)
            .bind(remaining)
            .execute(&self.pool)
            .await
            .map_err(map_sqlx_error)?
            .rows_affected()
        } else {
            0
        };
        Ok(decisions_removed + turns_removed + evidence_removed)
    }

    async fn replace_from_real_request(
        &self,
        request: &CacheKeepaliveReplaceRequest,
    ) -> StorageResult<CacheKeepaliveSessionRecord> {
        self.replace_from_real_request_fenced(request, None)
            .await?
            .ok_or_else(missing_after_write)
    }

    async fn allocate_cache_keepalive_completion_ticket(
        &self,
        session_key_hash: &str,
    ) -> StorageResult<u64> {
        let key = cache_keepalive_order_meta_key(session_key_hash);
        let mut tx = self.begin_immediate().await?;
        let existing = sqlx::query_scalar::<_, String>("SELECT value FROM meta_v1 WHERE key = ?")
            .bind(&key)
            .fetch_optional(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
        let (issued, applied) = parse_keepalive_order_meta(existing.as_deref());
        let next_issued = issued.checked_add(1).ok_or_else(|| StorageError::Fatal {
            message: "cache keepalive completion ticket overflow".to_owned(),
        })?;
        let updated = format_keepalive_order_meta(next_issued, applied);
        sqlx::query(
            "INSERT INTO meta_v1 (key, value) VALUES (?, ?)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        )
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
        let mut tx = self.begin_immediate().await?;
        let existing = sqlx::query_as::<_, (i64, i64, String, i64, i64, i64)>(
            "SELECT generation, created_at, status, cache_anchor_at, updated_at, first_scheduled_at FROM cache_keepalive_sessions WHERE session_key_hash = ?",
        )
        .bind(&request.session_key_hash)
        .fetch_optional(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;
        let parsed_existing = match existing {
            Some((
                generation,
                created_at,
                status,
                cache_anchor_at,
                updated_at,
                first_scheduled_at,
            )) => Some((
                i64_to_u64(generation, "cache keepalive generation")?,
                i64_to_u64(created_at, "cache keepalive created_at")?,
                status_from_db(&status)?,
                i64_to_u64(cache_anchor_at, "cache keepalive cache_anchor_at")?,
                i64_to_u64(updated_at, "cache keepalive updated_at")?,
                i64_to_u64(first_scheduled_at, "cache keepalive first_scheduled_at")?,
            )),
            None => None,
        };
        if let Some((_, _, status, existing_anchor, existing_updated, logical_anchor)) =
            parsed_existing
        {
            // Synthetic hits move cache_anchor_at, but not the real-completion
            // boundary in first_scheduled_at. Ticket order alone is insufficient
            // because a slower, older response can observe a higher ticket.
            if expected_ticket.is_some() {
                if logical_anchor > request.cache_anchor_at_unix_secs {
                    return Ok(None);
                }
            } else if existing_anchor > request.cache_anchor_at_unix_secs
                || status == CacheKeepaliveSessionStatus::Terminal
                    && existing_updated > request.cache_anchor_at_unix_secs
            {
                return Ok(None);
            }
        }
        if let Some(ticket) = expected_ticket {
            let order_key = cache_keepalive_order_meta_key(&request.session_key_hash);
            let order_raw =
                sqlx::query_scalar::<_, String>("SELECT value FROM meta_v1 WHERE key = ?")
                    .bind(&order_key)
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(map_sqlx_error)?;
            let (issued, applied) = parse_keepalive_order_meta(order_raw.as_deref());
            if ticket <= applied {
                return Ok(None);
            }
            let updated_order = format_keepalive_order_meta(issued.max(ticket), ticket);
            sqlx::query(
                "INSERT INTO meta_v1 (key, value) VALUES (?, ?)
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            )
            .bind(&order_key)
            .bind(&updated_order)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
        }
        let generation = parsed_existing
            .map(|(generation, _, _, _, _, _)| generation)
            .unwrap_or(0)
            .checked_add(1)
            .ok_or_else(|| StorageError::Fatal {
                message: "cache keepalive generation overflow".to_owned(),
            })?;
        let created_at = parsed_existing
            .map(|(_, created_at, _, _, _, _)| created_at)
            .unwrap_or(request.now_unix_secs);
        let job_key = cache_keepalive_job_key(&request.session_key_hash, generation);
        sqlx::query(
"INSERT INTO cache_keepalive_sessions
             (session_key_hash, principal_id, accounting_key_id, upstream_id, cycle_id, generation, refresh_count,
              first_scheduled_at, cache_anchor_at, run_at, ttl, status, enqueue_state, running_since_unix_secs,
              current_job_key, encrypted_payload, display_reason, error, config_snapshot, last_message_at_ms, terminal_reason, expires_at, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, 'active', 'pending', NULL, ?, ?, ?, NULL, ?, ?, NULL, ?, ?, ?)
             ON CONFLICT(session_key_hash) DO UPDATE SET
              principal_id = excluded.principal_id,
              accounting_key_id = excluded.accounting_key_id,
              upstream_id = excluded.upstream_id,
              cycle_id = excluded.cycle_id,
              generation = excluded.generation,
              refresh_count = 0,
              first_scheduled_at = excluded.first_scheduled_at,
              cache_anchor_at = excluded.cache_anchor_at,
              run_at = excluded.run_at,
              ttl = excluded.ttl,
              status = 'active',
              enqueue_state = 'pending',
              running_since_unix_secs = NULL,
              current_job_key = excluded.current_job_key,
              encrypted_payload = excluded.encrypted_payload,
              display_reason = excluded.display_reason,
              error = NULL,
              config_snapshot = excluded.config_snapshot,
              last_message_at_ms = excluded.last_message_at_ms,
              terminal_reason = NULL,
              cancel_reason = NULL,
              expires_at = excluded.expires_at,
              updated_at = excluded.updated_at",
        )
        .bind(&request.session_key_hash)
        .bind(&request.principal_id)
        .bind(&request.accounting_key_id)
        .bind(request.upstream_id.to_string())
        .bind(request.cycle_id.to_string())
        .bind(u64_to_i64(generation, "cache keepalive generation")?)
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
        .bind(&job_key)
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
        .bind(u64_to_i64(created_at, "cache keepalive created_at")?)
        .bind(u64_to_i64(
            request.now_unix_secs,
            "cache keepalive updated_at",
        )?)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;
        let row = sqlx::query("SELECT * FROM cache_keepalive_sessions WHERE session_key_hash = ?")
            .bind(&request.session_key_hash)
            .fetch_optional(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
        let record = row
            .map(row_to_record)
            .transpose()?
            .ok_or_else(missing_after_write)?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(Some(record))
    }

    async fn get_cache_keepalive_session(
        &self,
        session_key_hash: &str,
    ) -> StorageResult<Option<CacheKeepaliveSessionRecord>> {
        let row = sqlx::query("SELECT * FROM cache_keepalive_sessions WHERE session_key_hash = ?")
            .bind(session_key_hash)
            .fetch_optional(self.pool())
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
            "UPDATE cache_keepalive_sessions SET enqueue_state = 'enqueued', running_since_unix_secs = NULL, updated_at = ?
             WHERE session_key_hash = ? AND generation = ? AND status = 'active' AND enqueue_state = 'pending'",
        )
        .bind(u64_to_i64(now_unix_secs, "cache keepalive updated_at")?)
        .bind(session_key_hash)
        .bind(u64_to_i64(generation, "cache keepalive generation")?)
        .execute(self.pool())
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
             SET enqueue_state = 'running', running_since_unix_secs = ?, updated_at = ?
             WHERE session_key_hash = ? AND generation = ? AND status = 'active'
               AND (enqueue_state = 'enqueued' OR (enqueue_state = 'pending' AND length(encrypted_payload) > 0))",
        )
        .bind(now_unix_secs)
        .bind(now_unix_secs)
        .bind(session_key_hash)
        .bind(u64_to_i64(generation, "cache keepalive generation")?)
        .execute(self.pool())
        .await
        .map_err(map_sqlx_error)?;
        Ok(result.rows_affected() == 1)
    }

    async fn authorize_cache_keepalive_dispatch(
        &self,
        session_key_hash: &str,
        generation: u64,
    ) -> StorageResult<bool> {
        let authorized = sqlx::query_scalar::<_, i64>(
            "SELECT EXISTS (
                 SELECT 1
                 FROM cache_keepalive_sessions
                 WHERE session_key_hash = ? AND generation = ? AND status = 'active'
                   AND enqueue_state = 'running'
             )",
        )
        .bind(session_key_hash)
        .bind(u64_to_i64(generation, "cache keepalive generation")?)
        .fetch_one(self.pool())
        .await
        .map_err(map_sqlx_error)?;
        Ok(authorized == 1)
    }

    async fn update_cache_keepalive_payload(
        &self,
        session_key_hash: &str,
        generation: u64,
        encrypted_payload: &[u8],
        now_unix_secs: u64,
    ) -> StorageResult<bool> {
        let result = sqlx::query(
            "UPDATE cache_keepalive_sessions SET encrypted_payload = ?, updated_at = ?
             WHERE session_key_hash = ? AND generation = ? AND status = 'active' AND enqueue_state = 'pending'",
        )
        .bind(encrypted_payload)
        .bind(u64_to_i64(now_unix_secs, "cache keepalive updated_at")?)
        .bind(session_key_hash)
        .bind(u64_to_i64(generation, "cache keepalive generation")?)
        .execute(self.pool())
        .await
        .map_err(map_sqlx_error)?;
        Ok(result.rows_affected() == 1)
    }

    async fn check_cache_keepalive_generation(
        &self,
        session_key_hash: &str,
    ) -> StorageResult<Option<CacheKeepaliveGenerationCheck>> {
        let row = sqlx::query("SELECT generation, status, enqueue_state FROM cache_keepalive_sessions WHERE session_key_hash = ?")
            .bind(session_key_hash)
            .fetch_optional(self.pool())
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
                "UPDATE cache_keepalive_sessions SET generation = ?, refresh_count = refresh_count + 1,
                 cache_anchor_at = ?, run_at = ?, ttl = ttl, status = 'active', enqueue_state = 'pending',
                 running_since_unix_secs = NULL, current_job_key = ?, encrypted_payload = ?, terminal_reason = NULL, error = NULL, expires_at = ?, updated_at = ?
                 WHERE session_key_hash = ? AND generation = ? AND status = 'active'
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
            .fetch_optional(self.pool())
            .await
            .map_err(map_sqlx_error)?
        } else {
            sqlx::query(
                "UPDATE cache_keepalive_sessions SET generation = ?, refresh_count = refresh_count + 1,
                 cache_anchor_at = ?, run_at = ?, ttl = ttl, status = 'active', enqueue_state = 'pending',
                 running_since_unix_secs = NULL, current_job_key = ?, terminal_reason = NULL, error = NULL, expires_at = ?, updated_at = ?
                 WHERE session_key_hash = ? AND generation = ? AND status = 'active'
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
            .fetch_optional(self.pool())
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
            "UPDATE cache_keepalive_sessions SET status = 'terminal', enqueue_state = 'pending', terminal_reason = ?, error = ?,
             running_since_unix_secs = NULL, encrypted_payload = X'', expires_at = MAX(expires_at, ?), updated_at = ?
             WHERE session_key_hash = ? AND generation = ? AND status = 'active'",
        )
        .bind(reason.as_str())
        .bind(error)
        .bind(retained_expires_at)
        .bind(u64_to_i64(now_unix_secs, "cache keepalive updated_at")?)
        .bind(session_key_hash)
        .bind(u64_to_i64(generation, "cache keepalive generation")?)
        .execute(self.pool())
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
        cancel_reason: Option<&str>,
        now_unix_secs: u64,
    ) -> StorageResult<bool> {
        let mut tx = self.begin_immediate().await?;
        if let Some(ticket) = expected_ticket {
            let order_key = cache_keepalive_order_meta_key(session_key_hash);
            let order_raw =
                sqlx::query_scalar::<_, String>("SELECT value FROM meta_v1 WHERE key = ?")
                    .bind(&order_key)
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(map_sqlx_error)?;
            let (issued, applied) = parse_keepalive_order_meta(order_raw.as_deref());
            if ticket <= applied {
                return Ok(false);
            }
            let updated_order = format_keepalive_order_meta(issued.max(ticket), ticket);
            sqlx::query(
                "INSERT INTO meta_v1 (key, value) VALUES (?, ?)
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            )
            .bind(&order_key)
            .bind(&updated_order)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
        }
        // Ticketed real completions compare against the stable logical boundary,
        // not the sliding cache anchor maintained by synthetic refreshes.
        let cancel_anchor = u64_to_i64(
            cancel_anchor_at_unix_secs,
            "cache keepalive cancel_anchor_at",
        )?;
        let retained_expires_at = u64_to_i64(
            now_unix_secs.saturating_add(TERMINAL_SESSION_RETENTION_SECS),
            "cache keepalive terminal expires_at",
        )?;
        let result = sqlx::query(
            "UPDATE cache_keepalive_sessions SET status = 'terminal', enqueue_state = 'pending', terminal_reason = ?, cancel_reason = ?, error = NULL, running_since_unix_secs = NULL,
             encrypted_payload = X'',
             expires_at = CASE WHEN refresh_count > 0 OR EXISTS (SELECT 1 FROM cache_keepalive_turns turn_row WHERE turn_row.principal_id = cache_keepalive_sessions.principal_id AND turn_row.session_key_hash = cache_keepalive_sessions.session_key_hash) THEN MAX(expires_at, ?) ELSE expires_at END,
             updated_at = ?,
             first_scheduled_at = CASE WHEN ? THEN ? ELSE first_scheduled_at END
             WHERE session_key_hash = ? AND status = 'active'
               AND (CASE WHEN ? THEN first_scheduled_at ELSE cache_anchor_at END) <= ?",
        )
        .bind(reason.as_str())
        .bind(cancel_reason)
        .bind(retained_expires_at)
        .bind(u64_to_i64(now_unix_secs, "cache keepalive updated_at")?)
        .bind(expected_ticket.is_some())
        .bind(cancel_anchor)
        .bind(session_key_hash)
        .bind(expected_ticket.is_some())
        .bind(cancel_anchor)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;
        if result.rows_affected() == 0 {
            // Do not consume the ticket when its logical anchor was stale.
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
            "UPDATE cache_keepalive_sessions SET status = 'terminal', enqueue_state = 'pending', terminal_reason = ?, error = NULL, running_since_unix_secs = NULL,
             encrypted_payload = X'',
             expires_at = CASE WHEN refresh_count > 0 OR EXISTS (SELECT 1 FROM cache_keepalive_turns turn_row WHERE turn_row.principal_id = cache_keepalive_sessions.principal_id AND turn_row.session_key_hash = cache_keepalive_sessions.session_key_hash) THEN MAX(expires_at, ?) ELSE expires_at END,
             updated_at = ?
             WHERE session_key_hash = ? AND status = 'active'",
        )
        .bind(reason.as_str())
        .bind(retained_expires_at)
        .bind(u64_to_i64(now_unix_secs, "cache keepalive updated_at")?)
        .bind(session_key_hash)
        .execute(self.pool())
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
                 running_since_unix_secs = NULL, encrypted_payload = X'', updated_at = ?
             WHERE status = 'active'
               AND enqueue_state = 'running'
               AND running_since_unix_secs IS NOT NULL
               AND running_since_unix_secs < ?",
        )
        .bind(u64_to_i64(now_unix_secs, "cache keepalive updated_at")?)
        .bind(u64_to_i64(
            stale_running_cutoff_unix_secs,
            "cache keepalive stale running cutoff",
        )?)
        .execute(self.pool())
        .await
        .map_err(map_sqlx_error)?;
        Ok(result.rows_affected())
    }

    async fn purge_cache_keepalive_expired(&self, cutoff_unix_secs: u64) -> StorageResult<u64> {
        let result = sqlx::query("DELETE FROM cache_keepalive_sessions WHERE expires_at < ?")
            .bind(u64_to_i64(
                cutoff_unix_secs,
                "cache keepalive purge cutoff",
            )?)
            .execute(self.pool())
            .await
            .map_err(map_sqlx_error)?;
        sqlx::query(
            "DELETE FROM meta_v1 WHERE key LIKE 'keepalive_order:%'
             AND substr(key, 17) NOT IN (SELECT session_key_hash FROM cache_keepalive_sessions)",
        )
        .execute(self.pool())
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
        .execute(self.pool())
        .await
        .map_err(map_sqlx_error)?;
        Ok(result.rows_affected())
    }

    async fn purge_cache_keepalive_stale_pending(
        &self,
        cutoff_unix_secs: u64,
    ) -> StorageResult<u64> {
        let result = sqlx::query(
            "DELETE FROM cache_keepalive_sessions WHERE status = 'active' AND enqueue_state = 'pending' AND updated_at < ?",
        )
        .bind(u64_to_i64(cutoff_unix_secs, "cache keepalive purge cutoff")?)
        .execute(self.pool())
        .await
        .map_err(map_sqlx_error)?;
        Ok(result.rows_affected())
    }
}

fn row_to_check(row: sqlx::sqlite::SqliteRow) -> StorageResult<CacheKeepaliveGenerationCheck> {
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

pub(crate) fn row_to_record(
    row: sqlx::sqlite::SqliteRow,
) -> StorageResult<CacheKeepaliveSessionRecord> {
    Ok(CacheKeepaliveSessionRecord {
        session_key_hash: row.try_get("session_key_hash").map_err(map_sqlx_error)?,
        principal_id: row.try_get("principal_id").map_err(map_sqlx_error)?,
        accounting_key_id: row.try_get("accounting_key_id").map_err(map_sqlx_error)?,
        upstream_id: uuid_from_db(
            &row.try_get::<String, _>("upstream_id")
                .map_err(map_sqlx_error)?,
        )?,
        cycle_id: row
            .try_get::<Option<String>, _>("cycle_id")
            .map_err(map_sqlx_error)?
            .map(|value| uuid_from_db(&value))
            .transpose()?,
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
        cancel_reason: row.try_get("cancel_reason").map_err(map_sqlx_error)?,
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

fn uuid_from_db(value: &str) -> StorageResult<Uuid> {
    Uuid::parse_str(value).map_err(|error| StorageError::Corrupted {
        message: format!("invalid cache keepalive upstream_id: {error}"),
    })
}

fn u64_to_i64(value: u64, field: &str) -> StorageResult<i64> {
    i64::try_from(value).map_err(|_| StorageError::InvalidInput {
        field: field.to_owned(),
        reason: "value exceeds i64::MAX".to_owned(),
    })
}

fn i64_to_u64(value: i64, field: &str) -> StorageResult<u64> {
    u64::try_from(value).map_err(|_| StorageError::Corrupted {
        message: format!("negative {field} value {value}"),
    })
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

fn missing_after_write() -> StorageError {
    StorageError::Fatal {
        message: "cache keepalive session missing after write".to_owned(),
    }
}
