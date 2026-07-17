use async_trait::async_trait;
use cc_lb_storage_api::{
    CacheKeepaliveEnqueueState, CacheKeepaliveGenerationCheck, CacheKeepaliveHitRefreshRequest,
    CacheKeepaliveReplaceRequest, CacheKeepaliveSessionRecord, CacheKeepaliveSessionStatus,
    CacheKeepaliveSessionStore, CacheKeepaliveTerminalReason, CacheTtl, StorageError,
    StorageResult, cache_keepalive_job_key,
};
use sqlx::Row;
use uuid::Uuid;

use crate::{SqliteStorage, map_sqlx_error};

#[async_trait]
impl CacheKeepaliveSessionStore for SqliteStorage {
    async fn replace_from_real_request(
        &self,
        request: &CacheKeepaliveReplaceRequest,
    ) -> StorageResult<CacheKeepaliveSessionRecord> {
        let mut tx = self.begin_immediate().await?;
        let existing = sqlx::query_as::<_, (i64, i64)>(
            "SELECT generation, created_at FROM cache_keepalive_sessions WHERE session_key_hash = ?",
        )
        .bind(&request.session_key_hash)
        .fetch_optional(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;
        let generation = existing
            .map(|(generation, _)| i64_to_u64(generation, "cache keepalive generation"))
            .transpose()?
            .unwrap_or(0)
            .checked_add(1)
            .ok_or_else(|| StorageError::Fatal {
                message: "cache keepalive generation overflow".to_owned(),
            })?;
        let created_at = existing
            .map(|(_, created_at)| i64_to_u64(created_at, "cache keepalive created_at"))
            .transpose()?
            .unwrap_or(request.now_unix_secs);
        let job_key = cache_keepalive_job_key(&request.session_key_hash, generation);
        sqlx::query(
            "INSERT INTO cache_keepalive_sessions
             (session_key_hash, principal_id, accounting_key_id, upstream_id, generation, refresh_count,
              first_scheduled_at, cache_anchor_at, run_at, ttl, status, enqueue_state, running_since_unix_secs,
              current_job_key, encrypted_payload, terminal_reason, expires_at, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, 0, ?, ?, ?, ?, 'active', 'pending', NULL, ?, ?, NULL, ?, ?, ?)
             ON CONFLICT(session_key_hash) DO UPDATE SET
              principal_id = excluded.principal_id,
              accounting_key_id = excluded.accounting_key_id,
              upstream_id = excluded.upstream_id,
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
              terminal_reason = NULL,
              expires_at = excluded.expires_at,
              updated_at = excluded.updated_at",
        )
        .bind(&request.session_key_hash)
        .bind(&request.principal_id)
        .bind(&request.accounting_key_id)
        .bind(request.upstream_id.to_string())
	        .bind(u64_to_i64(generation, "cache keepalive generation")?)
	        .bind(u64_to_i64(request.cache_anchor_at_unix_secs, "cache keepalive first_scheduled_at")?)
	        .bind(u64_to_i64(request.cache_anchor_at_unix_secs, "cache keepalive cache_anchor_at")?)
	        .bind(u64_to_i64(request.run_at_unix_secs, "cache keepalive run_at")?)
	        .bind(ttl_to_db(request.ttl))
        .bind(&job_key)
        .bind(&request.encrypted_payload)
        .bind(u64_to_i64(request.expires_at_unix_secs, "cache keepalive expires_at")?)
        .bind(u64_to_i64(created_at, "cache keepalive created_at")?)
        .bind(u64_to_i64(request.now_unix_secs, "cache keepalive updated_at")?)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;
        tx.commit().await.map_err(map_sqlx_error)?;
        self.get_cache_keepalive_session(&request.session_key_hash)
            .await?
            .ok_or_else(missing_after_write)
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
             WHERE session_key_hash = ? AND generation = ? AND status = 'active' AND enqueue_state = 'enqueued'",
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
        let result = sqlx::query(
	            "UPDATE cache_keepalive_sessions SET generation = ?, refresh_count = refresh_count + 1,
              cache_anchor_at = ?, run_at = ?, ttl = ttl, status = 'active', enqueue_state = 'pending',
              running_since_unix_secs = NULL, current_job_key = ?, encrypted_payload = ?, terminal_reason = NULL, expires_at = ?, updated_at = ?
	             WHERE session_key_hash = ? AND generation = ? AND status = 'active'",
	        )
	        .bind(u64_to_i64(next_generation, "cache keepalive generation")?)
	        .bind(u64_to_i64(request.cache_anchor_at_unix_secs, "cache keepalive cache_anchor_at")?)
	        .bind(u64_to_i64(request.run_at_unix_secs, "cache keepalive run_at")?)
	        .bind(cache_keepalive_job_key(&request.session_key_hash, next_generation))
        .bind(&request.encrypted_payload)
        .bind(u64_to_i64(request.expires_at_unix_secs, "cache keepalive expires_at")?)
        .bind(u64_to_i64(request.now_unix_secs, "cache keepalive updated_at")?)
        .bind(&request.session_key_hash)
        .bind(u64_to_i64(request.generation, "cache keepalive generation")?)
        .execute(self.pool())
        .await
        .map_err(map_sqlx_error)?;
        if result.rows_affected() == 0 {
            return Ok(None);
        }
        self.get_cache_keepalive_session(&request.session_key_hash)
            .await
    }

    async fn mark_cache_keepalive_terminal(
        &self,
        session_key_hash: &str,
        generation: u64,
        reason: CacheKeepaliveTerminalReason,
        now_unix_secs: u64,
    ) -> StorageResult<bool> {
        let result = sqlx::query(
            "UPDATE cache_keepalive_sessions SET status = 'terminal', terminal_reason = ?, running_since_unix_secs = NULL, updated_at = ?
             WHERE session_key_hash = ? AND generation = ? AND status = 'active'",
        )
        .bind(reason.as_str())
        .bind(u64_to_i64(now_unix_secs, "cache keepalive updated_at")?)
        .bind(session_key_hash)
        .bind(u64_to_i64(generation, "cache keepalive generation")?)
        .execute(self.pool())
        .await
        .map_err(map_sqlx_error)?;
        Ok(result.rows_affected() == 1)
    }

    async fn mark_latest_cache_keepalive_terminal(
        &self,
        session_key_hash: &str,
        reason: CacheKeepaliveTerminalReason,
        now_unix_secs: u64,
    ) -> StorageResult<bool> {
        let result = sqlx::query(
            "UPDATE cache_keepalive_sessions SET status = 'terminal', terminal_reason = ?, running_since_unix_secs = NULL, updated_at = ?
             WHERE session_key_hash = ? AND status = 'active'",
        )
        .bind(reason.as_str())
        .bind(u64_to_i64(now_unix_secs, "cache keepalive updated_at")?)
        .bind(session_key_hash)
        .execute(self.pool())
        .await
        .map_err(map_sqlx_error)?;
        Ok(result.rows_affected() == 1)
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

fn row_to_record(row: sqlx::sqlite::SqliteRow) -> StorageResult<CacheKeepaliveSessionRecord> {
    Ok(CacheKeepaliveSessionRecord {
        session_key_hash: row.try_get("session_key_hash").map_err(map_sqlx_error)?,
        principal_id: row.try_get("principal_id").map_err(map_sqlx_error)?,
        accounting_key_id: row.try_get("accounting_key_id").map_err(map_sqlx_error)?,
        upstream_id: uuid_from_db(
            &row.try_get::<String, _>("upstream_id")
                .map_err(map_sqlx_error)?,
        )?,
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
    }
}

fn ttl_from_db(value: &str) -> StorageResult<CacheTtl> {
    match value {
        "5m" => Ok(CacheTtl::Ttl5m),
        "1h" => Ok(CacheTtl::Ttl1h),
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

fn missing_after_write() -> StorageError {
    StorageError::Fatal {
        message: "cache keepalive session missing after write".to_owned(),
    }
}
