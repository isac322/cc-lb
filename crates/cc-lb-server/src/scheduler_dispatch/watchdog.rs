use cc_lb_aead::AeadService;
use cc_lb_scheduler::error::Result as SchedulerResult;
use cc_lb_storage_api::upstream::UpstreamKind;
use cc_lb_storage_api::{UpstreamRecord, UpstreamStore};
use uuid::Uuid;

use crate::scheduler_dispatch::storage::storage_scheduler_error;

use super::SchedulerDispatch;

const UPSTREAM_PAGE_LIMIT: usize = 1_000;

impl SchedulerDispatch {
    pub(super) async fn list_warmup_watchdog_upstream_ids(&self) -> SchedulerResult<Vec<Uuid>> {
        self.list_watchdog_upstream_ids(WatchdogUpstreamFilter::Warmup)
            .await
    }

    pub(super) async fn list_oauth_watchdog_upstream_ids(&self) -> SchedulerResult<Vec<Uuid>> {
        self.list_watchdog_upstream_ids(WatchdogUpstreamFilter::OAuth)
            .await
    }

    async fn list_watchdog_upstream_ids(
        &self,
        filter: WatchdogUpstreamFilter,
    ) -> SchedulerResult<Vec<Uuid>> {
        let mut upstream_ids = Vec::new();
        let mut after = None;
        loop {
            let page = UpstreamStore::list(self.storage.as_ref(), after, UPSTREAM_PAGE_LIMIT)
                .await
                .map_err(storage_scheduler_error)?;
            if page.is_empty() {
                break;
            }
            upstream_ids.extend(
                page.iter()
                    .filter(|upstream| filter.matches(upstream, self.aead.as_ref()))
                    .map(|upstream| upstream.id),
            );
            after = page.last().map(|upstream| upstream.id);
            if page.len() < UPSTREAM_PAGE_LIMIT {
                break;
            }
        }
        Ok(upstream_ids)
    }
}

#[derive(Clone, Copy)]
enum WatchdogUpstreamFilter {
    Warmup,
    OAuth,
}

impl WatchdogUpstreamFilter {
    fn matches(self, upstream: &UpstreamRecord, aead: &AeadService) -> bool {
        if upstream.kind != UpstreamKind::AnthropicOauth
            || upstream.deleted_at_unix_secs.is_some()
            || upstream.oauth_credentials.is_none()
        {
            return false;
        }
        match self {
            Self::Warmup => upstream.warmup_enabled,
            Self::OAuth => !is_never_refresh_upstream(upstream, aead),
        }
    }
}

/// `true` when the upstream's decrypted credential is long-lived and must never
/// be refreshed. Undecryptable credentials keep being watched so a
/// broken-but-refreshing upstream still gets a refresh job.
fn is_never_refresh_upstream(upstream: &UpstreamRecord, aead: &AeadService) -> bool {
    upstream
        .oauth_credentials
        .as_ref()
        .and_then(|credentials| credentials.decrypt(aead, upstream.id.as_bytes()).ok())
        .is_some_and(|bundle| bundle.never_refresh)
}

#[cfg(test)]
mod tests {
    use cc_lb_aead::{AeadService, EncryptedOAuthTokens, OAuthTokenBundle};

    use super::*;

    #[test]
    fn oauth_filter_matches_disabled_registered_oauth_upstream() {
        let aead = AeadService::from_master_key([7; 32]);
        let mut upstream = registered_oauth_upstream();
        upstream.enabled = false;
        upstream.warmup_enabled = false;

        assert!(WatchdogUpstreamFilter::OAuth.matches(&upstream, &aead));
    }

    #[test]
    fn oauth_filter_rejects_deleted_or_missing_credentials() {
        let aead = AeadService::from_master_key([7; 32]);
        let mut deleted = registered_oauth_upstream();
        deleted.deleted_at_unix_secs = Some(1_800_000_000);
        let mut missing_credentials = registered_oauth_upstream();
        missing_credentials.oauth_credentials = None;

        assert!(!WatchdogUpstreamFilter::OAuth.matches(&deleted, &aead));
        assert!(!WatchdogUpstreamFilter::OAuth.matches(&missing_credentials, &aead));
    }

    #[test]
    fn oauth_filter_rejects_non_oauth_upstream() {
        let aead = AeadService::from_master_key([7; 32]);
        let mut upstream = registered_oauth_upstream();
        upstream.kind = UpstreamKind::AnthropicApiKey;

        assert!(!WatchdogUpstreamFilter::OAuth.matches(&upstream, &aead));
    }

    #[test]
    fn oauth_filter_rejects_never_refresh_credentials() {
        let aead = AeadService::from_master_key([7; 32]);
        let mut long_lived = registered_oauth_upstream();
        long_lived.oauth_credentials = Some(encrypted_bundle(&aead, long_lived.id, true));
        let mut refreshing = registered_oauth_upstream();
        refreshing.oauth_credentials = Some(encrypted_bundle(&aead, refreshing.id, false));

        assert!(!WatchdogUpstreamFilter::OAuth.matches(&long_lived, &aead));
        assert!(WatchdogUpstreamFilter::OAuth.matches(&refreshing, &aead));
    }

    #[test]
    fn warmup_filter_uses_warmup_enabled_only() {
        let aead = AeadService::from_master_key([7; 32]);
        let mut disabled_warmup = registered_oauth_upstream();
        disabled_warmup.enabled = true;
        disabled_warmup.warmup_enabled = false;
        let mut enabled_warmup = disabled_warmup.clone();
        enabled_warmup.enabled = false;
        enabled_warmup.warmup_enabled = true;

        assert!(!WatchdogUpstreamFilter::Warmup.matches(&disabled_warmup, &aead));
        assert!(WatchdogUpstreamFilter::Warmup.matches(&enabled_warmup, &aead));
    }

    fn encrypted_bundle(
        aead: &AeadService,
        upstream_id: Uuid,
        never_refresh: bool,
    ) -> EncryptedOAuthTokens {
        EncryptedOAuthTokens::encrypt(
            aead,
            &OAuthTokenBundle {
                access_token: "sk-ant-oat01-test".to_owned(),
                refresh_token: "sk-ant-ort01-test".to_owned(),
                expires_at_unix_secs: 9_999_999_999,
                refresh_token_expires_at_unix_secs: None,
                scopes: vec!["user:inference".to_owned()],
                never_refresh,
            },
            upstream_id.as_bytes(),
        )
        .expect("bundle encrypts")
    }

    fn registered_oauth_upstream() -> UpstreamRecord {
        UpstreamRecord {
            id: Uuid::new_v4(),
            name: "oauth-upstream".to_owned(),
            kind: UpstreamKind::AnthropicOauth,
            base_url: None,
            enabled: true,
            oauth_credentials: Some(EncryptedOAuthTokens::from_ciphertext(vec![1])),
            api_key_ciphertext: None,
            last_apply_error: None,
            last_apply_at_unix_secs: None,
            deleted_at_unix_secs: None,
            revision: 1,
            oauth_token_generation: 1,
            created_at_unix_secs: 1,
            updated_at_unix_secs: 1,
            warmup_enabled: false,
            warmup_dialect_plugin: None,
            last_warmup_at_unix_secs: None,
        }
    }
}
