use cc_lb_clock::unix_secs;
use cc_lb_control::api_keys::limit_engine::Reservation;
use cc_lb_control::api_keys::principal_view::PrincipalStatus;
use cc_lb_engine::cache_keepalive::{DispatchOutcome, KeepaliveDispatchContext, RequestSnapshot};
use cc_lb_scheduler::error::Result as SchedulerResult;
use cc_lb_scheduler::jobs::cache_keepalive::CacheKeepaliveJob;
use cc_lb_scheduler::retry::JobOutcome;
use cc_lb_scheduler::worker::AdaptiveJob;
use cc_lb_storage_api::{
    CacheKeepaliveHitRefreshRequest, CacheKeepaliveSessionRecord, CacheKeepaliveSessionStatus,
    CacheKeepaliveSessionStore, CacheKeepaliveTerminalReason,
};

use super::SchedulerDispatch;
use crate::cache_keepalive_payload::{
    base_payload_generation, decrypt_cache_keepalive_payload, encrypt_cache_keepalive_payload,
};

#[path = "cache_keepalive/finalizer.rs"]
mod finalizer;
#[path = "cache_keepalive/lifecycle.rs"]
mod lifecycle;

impl SchedulerDispatch {
    pub(super) async fn dispatch_cache_keepalive(
        &self,
        job: CacheKeepaliveJob,
    ) -> SchedulerResult<JobOutcome> {
        let Some(record) = CacheKeepaliveSessionStore::get_cache_keepalive_session(
            self.storage.as_ref(),
            &job.session_key_hash,
        )
        .await
        .map_err(|error| cc_lb_scheduler::error::SchedulerError::Job(error.to_string()))?
        else {
            return Ok(JobOutcome::Noop);
        };
        if record.generation != job.generation
            || record.status == CacheKeepaliveSessionStatus::Terminal
        {
            return Ok(JobOutcome::Noop);
        }

        let now = unix_secs(self.clock.now());
        if now >= record.expires_at_unix_secs {
            self.mark_cache_keepalive_terminal(&job, CacheKeepaliveTerminalReason::Expired)
                .await?;
            return Ok(JobOutcome::Done);
        }

        let claimed = CacheKeepaliveSessionStore::claim_cache_keepalive_turn(
            self.storage.as_ref(),
            &job.session_key_hash,
            job.generation,
            now,
        )
        .await
        .map_err(|error| cc_lb_scheduler::error::SchedulerError::Job(error.to_string()))?;
        if !claimed {
            return Ok(JobOutcome::Noop);
        }

        if !self.cache_keepalive_still_authorized(&record) {
            self.mark_cache_keepalive_terminal(&job, CacheKeepaliveTerminalReason::Cancelled)
                .await?;
            return Ok(JobOutcome::Done);
        }

        let decoded_payload = match decrypt_cache_keepalive_payload(
            self.aead.as_ref(),
            &record.principal_id,
            &record.session_key_hash,
            record.upstream_id,
            record.generation,
            record.refresh_count,
            &record.encrypted_payload,
        ) {
            Ok(payload) => payload,
            Err(_) => {
                self.mark_cache_keepalive_terminal(
                    &job,
                    CacheKeepaliveTerminalReason::DecryptFailed,
                )
                .await?;
                return Ok(JobOutcome::Done);
            }
        };
        let needs_payload_rewrite = decoded_payload.needs_rewrite;
        let snapshot = match RequestSnapshot::from_persisted(decoded_payload.snapshot) {
            Ok(snapshot) => snapshot,
            Err(_) => {
                self.mark_cache_keepalive_terminal(
                    &job,
                    CacheKeepaliveTerminalReason::DecryptFailed,
                )
                .await?;
                return Ok(JobOutcome::Done);
            }
        };

        let reservation = match self.reserve_cache_keepalive(&record, &snapshot).await {
            Ok(reservation) => reservation,
            Err(_) => {
                self.mark_cache_keepalive_terminal(
                    &job,
                    CacheKeepaliveTerminalReason::DispatchError,
                )
                .await?;
                return Ok(JobOutcome::Done);
            }
        };
        let source_ref_id = format!("{}:{}", job.session_key_hash, job.generation);

        let outcome = self
            .keepalive_dispatcher
            .dispatch(
                &snapshot,
                KeepaliveDispatchContext::new(reservation, source_ref_id),
            )
            .await;
        if !self.cache_keepalive_still_authorized(&record) {
            self.mark_cache_keepalive_terminal(&job, CacheKeepaliveTerminalReason::Cancelled)
                .await?;
            return Ok(JobOutcome::Done);
        }

        match outcome {
            DispatchOutcome::CacheHit {
                cache_anchor_age,
                finalization,
            } => {
                self.finalize_cache_keepalive_renewal(
                    &job,
                    &record,
                    &snapshot,
                    finalization,
                    "hit",
                )
                .await?;
                self.reschedule_cache_keepalive_hit(
                    job,
                    record,
                    snapshot,
                    cache_anchor_age,
                    needs_payload_rewrite,
                )
                .await
            }
            DispatchOutcome::CacheMiss { finalization } => {
                self.finalize_cache_keepalive_renewal(
                    &job,
                    &record,
                    &snapshot,
                    finalization,
                    "miss",
                )
                .await?;
                self.mark_cache_keepalive_terminal(&job, CacheKeepaliveTerminalReason::CacheMiss)
                    .await?;
                Ok(JobOutcome::Done)
            }
            DispatchOutcome::UnsupportedProvider(_) => {
                self.mark_cache_keepalive_terminal(
                    &job,
                    CacheKeepaliveTerminalReason::UnsupportedProvider,
                )
                .await?;
                Ok(JobOutcome::Done)
            }
            DispatchOutcome::Error(_) => {
                self.mark_cache_keepalive_terminal(
                    &job,
                    CacheKeepaliveTerminalReason::DispatchError,
                )
                .await?;
                Ok(JobOutcome::Done)
            }
        }
    }

    fn cache_keepalive_still_authorized(&self, record: &CacheKeepaliveSessionRecord) -> bool {
        let view = self.dynamic_view.load();
        if view.principal_view.principal_status(&record.principal_id) != PrincipalStatus::Active {
            return false;
        }
        let Some(principal) = view.principal_view.get(&record.principal_id) else {
            return false;
        };
        if principal.cache_keepalive().is_none() {
            return false;
        }
        let allowed = principal.allowed_upstreams();
        if allowed.is_empty() {
            true
        } else {
            allowed.contains(&record.upstream_id)
        }
    }

    async fn reserve_cache_keepalive(
        &self,
        record: &CacheKeepaliveSessionRecord,
        snapshot: &RequestSnapshot,
    ) -> SchedulerResult<Option<Reservation>> {
        let Some(accounting_key_id) = record.accounting_key_id.as_deref() else {
            return Ok(None);
        };
        let Some(key_record) = self
            .key_store
            .get(&record.principal_id, accounting_key_id)
            .await
            .map_err(|error| cc_lb_scheduler::error::SchedulerError::Job(error.to_string()))?
        else {
            return Err(cc_lb_scheduler::error::SchedulerError::Job(
                "cache keepalive accounting key is unavailable".to_owned(),
            ));
        };
        let keepalive_body = snapshot
            .build_keepalive_body()
            .map_err(|error| cc_lb_scheduler::error::SchedulerError::Job(error.to_string()))?;
        let value: serde_json::Value = sonic_rs::from_slice(&keepalive_body)
            .map_err(|error| cc_lb_scheduler::error::SchedulerError::Job(error.to_string()))?;
        let model = value
            .get("model")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown");
        let max_tokens = value
            .get("max_tokens")
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0);
        let cost_estimate_micros = self
            .price_catalog
            .estimate_max(
                model,
                4_000,
                u64::try_from(max_tokens.max(0)).unwrap_or(0),
                None,
            )
            .map(|cost| i64::try_from(cost).unwrap_or(i64::MAX));
        let view = self.dynamic_view.load();
        self.limit_engine
            .reserve(
                view.principal_view.as_ref(),
                &key_record,
                &record.principal_id,
                model,
                max_tokens,
                4_000,
                cost_estimate_micros,
            )
            .map(Some)
            .map_err(|error| cc_lb_scheduler::error::SchedulerError::Job(format!("{error:?}")))
    }

    async fn reschedule_cache_keepalive_hit(
        &self,
        job: CacheKeepaliveJob,
        record: cc_lb_storage_api::CacheKeepaliveSessionRecord,
        snapshot: RequestSnapshot,
        cache_anchor_age: std::time::Duration,
        needs_payload_rewrite: bool,
    ) -> SchedulerResult<JobOutcome> {
        let now = unix_secs(self.clock.now());
        if record.refresh_count.saturating_add(1) >= job.max_refreshes {
            self.mark_cache_keepalive_terminal(&job, CacheKeepaliveTerminalReason::MaxRefreshes)
                .await?;
            return Ok(JobOutcome::Done);
        }
        if now.saturating_sub(record.first_scheduled_at_unix_secs) >= job.max_total_duration_secs {
            self.mark_cache_keepalive_terminal(&job, CacheKeepaliveTerminalReason::MaxDuration)
                .await?;
            return Ok(JobOutcome::Done);
        }

        let cache_anchor_at_unix_secs = now.saturating_sub(cache_anchor_age.as_secs());
        let run_at_unix_secs = cache_anchor_at_unix_secs
            .saturating_add(job.refresh_delay_secs)
            .max(now.saturating_add(1));
        let encrypted_payload = if needs_payload_rewrite {
            let payload_generation =
                base_payload_generation(record.generation, record.refresh_count).map_err(
                    |error| cc_lb_scheduler::error::SchedulerError::Job(error.to_string()),
                )?;
            Some(
                encrypt_cache_keepalive_payload(
                    self.aead.as_ref(),
                    &record.principal_id,
                    &record.session_key_hash,
                    record.upstream_id,
                    payload_generation,
                    &snapshot.to_persisted(),
                )
                .map_err(|error| cc_lb_scheduler::error::SchedulerError::Job(error.to_string()))?,
            )
        } else {
            None
        };
        let updated = CacheKeepaliveSessionStore::reschedule_after_cache_hit(
            self.storage.as_ref(),
            &CacheKeepaliveHitRefreshRequest {
                session_key_hash: job.session_key_hash.clone(),
                generation: job.generation,
                cache_anchor_at_unix_secs,
                run_at_unix_secs,
                expires_at_unix_secs: cache_anchor_at_unix_secs.saturating_add(job.ttl.as_secs()),
                encrypted_payload,
                now_unix_secs: now,
            },
        )
        .await
        .map_err(|error| cc_lb_scheduler::error::SchedulerError::Job(error.to_string()))?;
        let Some(updated) = updated else {
            return Ok(JobOutcome::Noop);
        };

        let next_job = CacheKeepaliveJob {
            session_key_hash: updated.session_key_hash.clone(),
            generation: updated.generation,
            principal_id: updated.principal_id.clone(),
            upstream_id: updated.upstream_id,
            ttl: updated.ttl,
            cache_anchor_at_unix_secs: updated.cache_anchor_at_unix_secs,
            expires_at_unix_secs: updated.expires_at_unix_secs,
            refresh_delay_secs: job.refresh_delay_secs,
            max_refreshes: job.max_refreshes,
            max_total_duration_secs: job.max_total_duration_secs,
            traceparent: job.traceparent,
        };
        let idempotency_key = next_job.idempotency_key();
        let task = cc_lb_scheduler::worker::SchedulerPushTask {
            args: AdaptiveJob::CacheKeepalive(next_job),
            idempotency_key: Some(idempotency_key),
            run_at_unix_secs: Some(updated.run_at_unix_secs),
            max_attempts: Some(1),
        };
        match self
            .cache_keepalive_pusher
            .push_cache_keepalive_task(task)
            .await
        {
            Ok(()) | Err(cc_lb_scheduler::error::SchedulerError::Conflict(_)) => {
                CacheKeepaliveSessionStore::mark_cache_keepalive_enqueued(
                    self.storage.as_ref(),
                    &updated.session_key_hash,
                    updated.generation,
                    now,
                )
                .await
                .map_err(|error| cc_lb_scheduler::error::SchedulerError::Job(error.to_string()))?;
                Ok(JobOutcome::Done)
            }
            Err(error) => {
                CacheKeepaliveSessionStore::mark_cache_keepalive_terminal(
                    self.storage.as_ref(),
                    &updated.session_key_hash,
                    updated.generation,
                    CacheKeepaliveTerminalReason::DispatchError,
                    now,
                )
                .await
                .map_err(|error| cc_lb_scheduler::error::SchedulerError::Job(error.to_string()))?;
                Err(error)
            }
        }
    }

    async fn mark_cache_keepalive_terminal(
        &self,
        job: &CacheKeepaliveJob,
        reason: CacheKeepaliveTerminalReason,
    ) -> SchedulerResult<bool> {
        CacheKeepaliveSessionStore::mark_cache_keepalive_terminal(
            self.storage.as_ref(),
            &job.session_key_hash,
            job.generation,
            reason,
            unix_secs(self.clock.now()),
        )
        .await
        .map_err(|error| cc_lb_scheduler::error::SchedulerError::Job(error.to_string()))
    }
}
