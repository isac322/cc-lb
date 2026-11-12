use async_trait::async_trait;
use cc_lb_admin::{LimitResetUsageRefresh, LimitResetUsageRefreshPort};
use cc_lb_control::anthropic_metadata::{
    CEDAR_EMBER_FENCE_RECOVERY_AFTER_MILLIS, cedar_ember_epoch_meta_key,
    cedar_ember_fence_started_at_millis, make_metadata_http_client, settled_cedar_ember_epoch,
};
use cc_lb_control::openai_limit_resets::{
    OPENAI_LIMIT_RESET_FENCE_RECOVERY_AFTER_MILLIS, OpenAiLimitResetAuth,
    OpenAiLimitResetPollRecord, classify_openai_manual_redemption, list_openai_limit_reset_credits,
    openai_limit_reset_epoch_meta_key, openai_limit_reset_fence_started_at_millis,
    openai_limit_reset_meta_key, settled_openai_limit_reset_epoch,
};
use cc_lb_scheduler::error::{Result as SchedulerResult, SchedulerError};
use cc_lb_scheduler::jobs::oauth_usage_poll::OAuthUsagePollObservation;
use cc_lb_storage_api::{UpstreamRecord, UpstreamStore, upstream::UpstreamKind};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{
    SchedulerDispatch,
    http::{decrypt_bundle, upstream_base_url},
    time::now_unix_millis,
};

/// Captured before the provider GET, including before any 401 retry. A reset
/// or credential replacement invalidates the complete observation, not merely
/// its coupon metadata: otherwise a delayed pre-reset body can exhaust routing
/// again after the reset continuation published its new quota.
pub(super) struct UsagePollFence {
    epoch: Option<Uuid>,
    epoch_key: Option<String>,
}

impl SchedulerDispatch {
    pub(super) async fn capture_usage_poll_fence(
        &self,
        upstream: &UpstreamRecord,
    ) -> SchedulerResult<UsagePollFence> {
        let (key, recovery_after) = match upstream.kind {
            UpstreamKind::AnthropicOauth => (
                cedar_ember_epoch_meta_key(upstream.id),
                CEDAR_EMBER_FENCE_RECOVERY_AFTER_MILLIS,
            ),
            UpstreamKind::OpenaiOauth => (
                openai_limit_reset_epoch_meta_key(upstream.id),
                OPENAI_LIMIT_RESET_FENCE_RECOVERY_AFTER_MILLIS,
            ),
            _ => {
                return Ok(UsagePollFence {
                    epoch: None,
                    epoch_key: None,
                });
            }
        };
        let raw = self
            .storage
            .get_meta_value(&key)
            .await
            .map_err(super::storage::storage_scheduler_error)?;
        if let Some(epoch) = settled_epoch(upstream.kind, raw.as_deref()) {
            return Ok(UsagePollFence {
                epoch,
                epoch_key: Some(key),
            });
        }
        let started_at = raw.as_deref().and_then(|raw| match upstream.kind {
            UpstreamKind::AnthropicOauth => cedar_ember_fence_started_at_millis(raw),
            UpstreamKind::OpenaiOauth => openai_limit_reset_fence_started_at_millis(raw),
            _ => None,
        });
        if let Some(raw) = raw
            && started_at.is_some_and(|started| {
                now_unix_millis(&*self.clock).saturating_sub(started) >= recovery_after
            })
        {
            // Recover abandoned consumption fences, never replay consumption.
            let epoch = Uuid::new_v4();
            if self
                .storage
                .compare_and_put_meta_value(&key, Some(&raw), &epoch.to_string())
                .await
                .map_err(super::storage::storage_scheduler_error)?
            {
                return Ok(UsagePollFence {
                    epoch: Some(epoch),
                    epoch_key: Some(key),
                });
            }
        }
        Err(SchedulerError::Job(
            "limit reset epoch is not settled".to_owned(),
        ))
    }

    pub(super) async fn validate_usage_poll_fence(
        &self,
        upstream: &UpstreamRecord,
        fence: &UsagePollFence,
    ) -> SchedulerResult<()> {
        let current = UpstreamStore::get_by_id(self.storage.as_ref(), upstream.id)
            .await
            .map_err(super::storage::storage_scheduler_error)?;
        if current.as_ref().is_none_or(|current| {
            current.kind != upstream.kind
                || current.deleted_at_unix_secs.is_some()
                || current.oauth_token_generation != upstream.oauth_token_generation
                || current.oauth_credential_fingerprint() != upstream.oauth_credential_fingerprint()
        }) {
            return Err(SchedulerError::Job(
                "usage poll credential changed before publication".to_owned(),
            ));
        }
        if let Some(key) = &fence.epoch_key {
            let raw = self
                .storage
                .get_meta_value(key)
                .await
                .map_err(super::storage::storage_scheduler_error)?;
            if settled_epoch(upstream.kind, raw.as_deref()) != Some(fence.epoch) {
                return Err(SchedulerError::Job(
                    "usage poll reset epoch changed before publication".to_owned(),
                ));
            }
        }
        Ok(())
    }

    pub(super) async fn publish_usage_poll(
        &self,
        upstream: &UpstreamRecord,
        fence: &UsagePollFence,
        sequence: u64,
        records: &[cc_lb_storage_api::SubscriptionQuotaSample],
    ) -> SchedulerResult<()> {
        if let Some(epoch_key) = &fence.epoch_key {
            let publication = cc_lb_storage_api::SubscriptionQuotaPollFence {
                upstream_id: upstream.id,
                oauth_token_generation: upstream.oauth_token_generation,
                epoch_key: epoch_key.clone(),
                epoch_value: fence.epoch.map(|epoch| epoch.to_string()),
            };
            if !self
                .storage
                .record_subscription_quota_poll(records, &publication)
                .await
                .map_err(super::storage::storage_scheduler_error)?
            {
                return Err(SchedulerError::Job(
                    "usage poll identity or reset epoch changed before durable publication"
                        .to_owned(),
                ));
            }
            if !self
                .subscription_quota_cache
                .publish_poll(upstream.id, sequence, records)
            {
                return Err(SchedulerError::Job(
                    "a newer usage poll already published routing quota".to_owned(),
                ));
            }
        } else {
            for record in records {
                self.subscription_quota_cache
                    .upsert_observation(upstream.id, record);
                self.subscription_quota_sink
                    .enqueue(record.clone())
                    .map_err(|error| SchedulerError::Job(error.to_string()))?;
            }
        }
        Ok(())
    }

    pub(super) async fn fetch_openai_reset_inventory(
        &self,
        upstream: &UpstreamRecord,
        fence: &UsagePollFence,
        usage_body: &[u8],
    ) -> SchedulerResult<Option<OpenAiLimitResetPollRecord>> {
        if upstream.kind != UpstreamKind::OpenaiOauth {
            return Ok(None);
        }
        let bundle = decrypt_bundle(upstream, self.aead.as_ref())?;
        let Some(account_id) = bundle
            .account_id
            .filter(|id| !id.trim().is_empty())
            .or_else(|| cc_lb_oauth_protocol::account_id_from_access_token(&bundle.access_token))
        else {
            return Ok(None);
        };
        let summary = serde_json::from_slice::<serde_json::Value>(usage_body).ok();
        if summary
            .as_ref()
            .and_then(|summary| summary.get("account_id"))
            .is_some_and(|observed| observed.as_str() != Some(account_id.as_str()))
        {
            return Err(SchedulerError::Job(
                "usage poll account identity mismatch".to_owned(),
            ));
        }
        let base = upstream_base_url(upstream)?;
        let inventory = match list_openai_limit_reset_credits(
            &make_metadata_http_client(),
            &base,
            OpenAiLimitResetAuth {
                access_token: &bundle.access_token,
                account_id: &account_id,
                user_agent: "codex-cli",
            },
            &self.cancel,
        )
        .await
        {
            Ok(mut inventory) => {
                inventory.manual_redemption = Some(classify_openai_manual_redemption(
                    summary.as_ref().unwrap_or(&serde_json::Value::Null),
                    now_unix_millis(&*self.clock) / 1_000,
                ));
                // Applicability comes from this cycle's account-bound usage
                // response only when the inventory total agrees. A truncated
                // credit list is not the total and is never used to infer it.
                if let Some(summary) = summary
                    .as_ref()
                    .filter(|summary| {
                        summary
                            .get("account_id")
                            .and_then(serde_json::Value::as_str)
                            == Some(account_id.as_str())
                    })
                    .and_then(|summary| summary.get("rate_limit_reset_credits"))
                    && summary
                        .get("available_count")
                        .and_then(serde_json::Value::as_i64)
                        == Some(inventory.available_count)
                    && let Some(count) = summary
                        .get("applicable_available_count")
                        .and_then(serde_json::Value::as_i64)
                {
                    inventory.applicable_available_count = Some(count);
                }
                Some(inventory)
            }
            Err(error) => {
                tracing::warn!(upstream_id = %upstream.id, %error, "OpenAI reset inventory fetch failed");
                None
            }
        };
        Ok(Some(OpenAiLimitResetPollRecord {
            credential_fingerprint: upstream.oauth_credential_fingerprint().unwrap_or_default(),
            oauth_token_generation: upstream.oauth_token_generation,
            account_id,
            epoch: fence.epoch,
            observed_at_unix_millis: now_unix_millis(&*self.clock),
            inventory,
        }))
    }

    pub(super) async fn persist_openai_reset_inventory(
        &self,
        upstream: &UpstreamRecord,
        fence: &UsagePollFence,
        record: &OpenAiLimitResetPollRecord,
    ) {
        // Metadata enrichment and quota enqueue may have yielded since the
        // quota publication check. Never persist an obsolete coupon snapshot.
        if self
            .validate_usage_poll_fence(upstream, fence)
            .await
            .is_err()
        {
            return;
        }
        let Ok(value) = serde_json::to_string(record) else {
            return;
        };
        if let Err(error) = self
            .storage
            .put_meta_value(&openai_limit_reset_meta_key(upstream.id), &value)
            .await
        {
            tracing::warn!(upstream_id = %upstream.id, %error, "OpenAI reset inventory write failed");
        }
    }
}

impl UsagePollFence {
    pub(super) fn epoch(&self) -> Option<Uuid> {
        self.epoch
    }
}

fn settled_epoch(kind: UpstreamKind, raw: Option<&str>) -> Option<Option<Uuid>> {
    match kind {
        UpstreamKind::AnthropicOauth => settled_cedar_ember_epoch(raw),
        UpstreamKind::OpenaiOauth => settled_openai_limit_reset_epoch(raw),
        _ => Some(None),
    }
}

#[async_trait]
impl LimitResetUsageRefreshPort for SchedulerDispatch {
    async fn refresh_upstream_usage(
        &self,
        upstream_id: Uuid,
        cancel: &CancellationToken,
    ) -> LimitResetUsageRefresh {
        // The caller owns a continuation token independent of the request.
        // Cloning dispatch reuses the actual targeted poll and routing cache,
        // without enqueueing fleet cron, warmup, or a second consume request.
        let mut dispatch = self.clone();
        dispatch.cancel = cancel.clone();
        tokio::select! {
            _ = cancel.cancelled() => LimitResetUsageRefresh::Failed,
            result = tokio::time::timeout(std::time::Duration::from_secs(30), dispatch.poll_usage(upstream_id, None)) => {
                match result {
                    Ok(Ok(OAuthUsagePollObservation::Success { .. })) => LimitResetUsageRefresh::Refreshed,
                    Ok(Ok(OAuthUsagePollObservation::Skip)) => LimitResetUsageRefresh::NotApplicable,
                    _ => LimitResetUsageRefresh::Failed,
                }
            }
        }
    }
}
