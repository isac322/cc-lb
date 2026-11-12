use cc_lb_aead::AeadService;
use cc_lb_oauth_protocol::refresh_requires_reconnect;
use cc_lb_scheduler::error::Result as SchedulerResult;
use cc_lb_storage_api::upstream::UpstreamKind;
use cc_lb_storage_api::{UpstreamRecord, UpstreamStore, WarmupLane};
use uuid::Uuid;

use crate::scheduler_dispatch::http::decrypt_bundle;
use crate::scheduler_dispatch::storage::storage_scheduler_error;

use super::SchedulerDispatch;

const UPSTREAM_PAGE_LIMIT: usize = 1_000;

impl SchedulerDispatch {
    pub(super) async fn list_warmup_watchdog_upstream_ids(
        &self,
        lane: WarmupLane,
    ) -> SchedulerResult<Vec<Uuid>> {
        let filter = match lane {
            WarmupLane::Primary => WatchdogUpstreamFilter::Warmup,
            WarmupLane::ThirdParty => WatchdogUpstreamFilter::WarmupThirdParty,
        };
        self.list_watchdog_upstream_ids(filter).await
    }

    pub(super) async fn list_oauth_refresh_watchdog_upstream_ids(
        &self,
    ) -> SchedulerResult<Vec<Uuid>> {
        self.list_watchdog_upstream_ids(WatchdogUpstreamFilter::OAuth)
            .await
    }

    pub(super) async fn list_oauth_usage_poll_upstream_ids(&self) -> SchedulerResult<Vec<Uuid>> {
        self.list_watchdog_upstream_ids(WatchdogUpstreamFilter::OAuthUsagePoll)
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
    /// Claude/GPT-OSS warm-up lane: only Gemini OAuth credentials issued by the
    /// Antigravity client carry that quota group.
    WarmupThirdParty,
    OAuth,
    OAuthUsagePoll,
}

impl WatchdogUpstreamFilter {
    fn matches(self, upstream: &UpstreamRecord, aead: &AeadService) -> bool {
        if upstream.deleted_at_unix_secs.is_some() || upstream.oauth_credentials.is_none() {
            return false;
        }
        match self {
            Self::Warmup => {
                WarmupLane::lanes_for_kind(upstream.kind).contains(&WarmupLane::Primary)
                    && upstream.warmup_enabled
            }
            Self::WarmupThirdParty => {
                let lane = WarmupLane::ThirdParty;
                if !upstream.warmup_enabled
                    || !WarmupLane::lanes_for_kind(upstream.kind).contains(&lane)
                {
                    return false;
                }
                // A credential that cannot be decrypted only drops this
                // upstream from the third-party lane; the primary lane and its
                // dispatch report the decrypt failure on their own.
                match decrypt_bundle(upstream, aead) {
                    Ok(bundle) => lane.credential_supports(upstream.kind, &bundle),
                    Err(error) => {
                        tracing::warn!(
                            upstream_id = %upstream.id,
                            %error,
                            "third-party warmup watchdog skipped upstream: credential decrypt failed"
                        );
                        false
                    }
                }
            }
            Self::OAuth => {
                matches!(
                    upstream.kind,
                    UpstreamKind::AnthropicOauth
                        | UpstreamKind::OpenaiOauth
                        | UpstreamKind::GeminiOauth
                ) && !upstream.oauth_never_refresh
                    && !refresh_requires_reconnect(upstream.last_apply_error.as_deref())
            }
            Self::OAuthUsagePoll => {
                matches!(
                    upstream.kind,
                    UpstreamKind::AnthropicOauth
                        | UpstreamKind::OpenaiOauth
                        | UpstreamKind::GeminiOauth
                ) && !refresh_requires_reconnect(upstream.last_apply_error.as_deref())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use cc_lb_aead::{EncryptedOAuthTokens, OAuthTokenBundle};
    use cc_lb_oauth_protocol::GeminiOAuthProfile;

    use super::*;

    #[test]
    fn oauth_filter_matches_disabled_registered_oauth_upstream() {
        let mut upstream = registered_oauth_upstream();
        upstream.enabled = false;
        upstream.warmup_enabled = false;

        assert!(WatchdogUpstreamFilter::OAuth.matches(&upstream, &aead()));
    }

    #[test]
    fn oauth_filter_rejects_deleted_or_missing_credentials() {
        let mut deleted = registered_oauth_upstream();
        deleted.deleted_at_unix_secs = Some(1_800_000_000);
        let mut missing_credentials = registered_oauth_upstream();
        missing_credentials.oauth_credentials = None;

        assert!(!WatchdogUpstreamFilter::OAuth.matches(&deleted, &aead()));
        assert!(!WatchdogUpstreamFilter::OAuth.matches(&missing_credentials, &aead()));
    }

    #[test]
    fn oauth_filter_rejects_non_oauth_upstream() {
        let mut upstream = registered_oauth_upstream();
        upstream.kind = UpstreamKind::AnthropicApiKey;

        assert!(!WatchdogUpstreamFilter::OAuth.matches(&upstream, &aead()));
    }

    #[test]
    fn oauth_filter_includes_openai_and_gemini_oauth_upstreams() {
        for kind in [UpstreamKind::OpenaiOauth, UpstreamKind::GeminiOauth] {
            let upstream = oauth_upstream(kind);

            assert!(
                WatchdogUpstreamFilter::OAuth.matches(&upstream, &aead()),
                "{kind:?} OAuth upstream should be refresh-watchdog eligible"
            );
        }
    }

    #[test]
    fn oauth_filter_rejects_openai_and_gemini_never_refresh_upstreams() {
        for kind in [UpstreamKind::OpenaiOauth, UpstreamKind::GeminiOauth] {
            let mut upstream = oauth_upstream(kind);
            upstream.oauth_never_refresh = true;

            assert!(!WatchdogUpstreamFilter::OAuth.matches(&upstream, &aead()));
        }
    }

    #[test]
    fn oauth_filter_rejects_openai_and_gemini_deleted_or_missing_credentials() {
        for kind in [UpstreamKind::OpenaiOauth, UpstreamKind::GeminiOauth] {
            let mut deleted = oauth_upstream(kind);
            deleted.deleted_at_unix_secs = Some(1_800_000_000);
            let mut missing_credentials = oauth_upstream(kind);
            missing_credentials.oauth_credentials = None;

            assert!(!WatchdogUpstreamFilter::OAuth.matches(&deleted, &aead()));
            assert!(!WatchdogUpstreamFilter::OAuth.matches(&missing_credentials, &aead()));
        }
    }

    #[test]
    fn oauth_filter_rejects_openai_and_gemini_api_key_upstreams() {
        for kind in [UpstreamKind::OpenaiApiKey, UpstreamKind::GeminiApiKey] {
            let upstream = oauth_upstream(kind);

            assert!(!WatchdogUpstreamFilter::OAuth.matches(&upstream, &aead()));
        }
    }

    #[test]
    fn oauth_filter_rejects_openai_and_gemini_reconnect_required_upstreams() {
        for kind in [UpstreamKind::OpenaiOauth, UpstreamKind::GeminiOauth] {
            let mut upstream = oauth_upstream(kind);
            upstream.last_apply_error = Some("status_401".to_owned());

            assert!(!WatchdogUpstreamFilter::OAuth.matches(&upstream, &aead()));
        }
    }

    /// OAU-03: no OAuth refresh, warmup-watchdog, or OAuth usage job is ever
    /// listed for an API-key upstream, even one carrying OAuth-shaped fields.
    #[test]
    fn oau03_no_oauth_job_filter_matches_api_key_upstream() {
        let mut upstream = registered_oauth_upstream();
        upstream.kind = UpstreamKind::AnthropicApiKey;
        upstream.warmup_enabled = true;

        for filter in [
            WatchdogUpstreamFilter::Warmup,
            WatchdogUpstreamFilter::WarmupThirdParty,
            WatchdogUpstreamFilter::OAuth,
            WatchdogUpstreamFilter::OAuthUsagePoll,
        ] {
            assert!(!filter.matches(&upstream, &aead()));
        }
    }

    #[test]
    fn oauth_filter_rejects_never_refresh_credentials() {
        let mut long_lived = registered_oauth_upstream();
        long_lived.oauth_never_refresh = true;
        let refreshing = registered_oauth_upstream();

        assert!(!WatchdogUpstreamFilter::OAuth.matches(&long_lived, &aead()));
        assert!(WatchdogUpstreamFilter::OAuth.matches(&refreshing, &aead()));
    }

    #[test]
    fn usage_poll_filter_includes_never_refresh_credentials() {
        let mut long_lived = registered_oauth_upstream();
        long_lived.oauth_never_refresh = true;
        let refreshing = registered_oauth_upstream();

        assert!(WatchdogUpstreamFilter::OAuthUsagePoll.matches(&long_lived, &aead()));
        assert!(WatchdogUpstreamFilter::OAuthUsagePoll.matches(&refreshing, &aead()));
    }

    #[test]
    fn warmup_filter_uses_warmup_enabled_only() {
        let mut disabled_warmup = registered_oauth_upstream();
        disabled_warmup.enabled = true;
        disabled_warmup.warmup_enabled = false;
        let mut enabled_warmup = disabled_warmup.clone();
        enabled_warmup.enabled = false;
        enabled_warmup.warmup_enabled = true;

        assert!(!WatchdogUpstreamFilter::Warmup.matches(&disabled_warmup, &aead()));
        assert!(WatchdogUpstreamFilter::Warmup.matches(&enabled_warmup, &aead()));
    }

    #[test]
    fn third_party_warmup_filter_selects_antigravity_gemini_oauth() {
        let upstream = gemini_upstream_with_profile(GeminiOAuthProfile::Antigravity);

        assert!(WatchdogUpstreamFilter::WarmupThirdParty.matches(&upstream, &aead()));
        assert!(WatchdogUpstreamFilter::Warmup.matches(&upstream, &aead()));
    }

    #[test]
    fn third_party_warmup_filter_excludes_gemini_cli_profile() {
        let upstream = gemini_upstream_with_profile(GeminiOAuthProfile::GeminiCli);

        assert!(!WatchdogUpstreamFilter::WarmupThirdParty.matches(&upstream, &aead()));
        assert!(WatchdogUpstreamFilter::Warmup.matches(&upstream, &aead()));
    }

    #[test]
    fn third_party_warmup_filter_excludes_non_gemini_kinds() {
        for kind in [
            UpstreamKind::AnthropicOauth,
            UpstreamKind::OpenaiOauth,
            UpstreamKind::AnthropicApiKey,
            UpstreamKind::OpenaiApiKey,
            UpstreamKind::GeminiApiKey,
        ] {
            // Even an Antigravity-shaped credential never qualifies another kind.
            let mut upstream = gemini_upstream_with_profile(GeminiOAuthProfile::Antigravity);
            upstream.kind = kind;

            assert!(
                !WatchdogUpstreamFilter::WarmupThirdParty.matches(&upstream, &aead()),
                "{kind:?} must not be seeded for the third-party lane"
            );
        }
    }

    #[test]
    fn third_party_warmup_filter_requires_warmup_enabled_and_live_credentials() {
        let mut disabled = gemini_upstream_with_profile(GeminiOAuthProfile::Antigravity);
        disabled.warmup_enabled = false;
        let mut deleted = gemini_upstream_with_profile(GeminiOAuthProfile::Antigravity);
        deleted.deleted_at_unix_secs = Some(1_800_000_000);
        let mut missing_credentials = gemini_upstream_with_profile(GeminiOAuthProfile::Antigravity);
        missing_credentials.oauth_credentials = None;

        for upstream in [disabled, deleted, missing_credentials] {
            assert!(!WatchdogUpstreamFilter::WarmupThirdParty.matches(&upstream, &aead()));
        }
    }

    #[test]
    fn third_party_decrypt_failure_excludes_only_third_party_lane() {
        let mut upstream = oauth_upstream(UpstreamKind::GeminiOauth);
        upstream.warmup_enabled = true;
        // `oauth_upstream` carries an undecryptable placeholder ciphertext.

        assert!(!WatchdogUpstreamFilter::WarmupThirdParty.matches(&upstream, &aead()));
        assert!(WatchdogUpstreamFilter::Warmup.matches(&upstream, &aead()));
    }

    fn aead() -> AeadService {
        AeadService::from_master_key([42; 32])
    }

    fn gemini_upstream_with_profile(profile: GeminiOAuthProfile) -> UpstreamRecord {
        let mut upstream = oauth_upstream(UpstreamKind::GeminiOauth);
        upstream.warmup_enabled = true;
        let bundle = OAuthTokenBundle {
            access_token: "access".to_owned(),
            refresh_token: "refresh".to_owned(),
            expires_at_unix_secs: 1_900_000_000,
            refresh_token_expires_at_unix_secs: None,
            account_id: None,
            project_id: Some("project".to_owned()),
            oauth_profile: profile,
            scopes: Vec::new(),
            never_refresh: false,
        };
        upstream.oauth_credentials = Some(
            EncryptedOAuthTokens::encrypt(&aead(), &bundle, upstream.id.as_bytes())
                .expect("encrypt bundle"),
        );
        upstream
    }

    fn registered_oauth_upstream() -> UpstreamRecord {
        oauth_upstream(UpstreamKind::AnthropicOauth)
    }

    fn oauth_upstream(kind: UpstreamKind) -> UpstreamRecord {
        UpstreamRecord {
            id: Uuid::new_v4(),
            name: "oauth-upstream".to_owned(),
            kind,
            base_url: None,
            enabled: true,
            oauth_credentials: Some(EncryptedOAuthTokens::from_ciphertext(vec![1])),
            oauth_never_refresh: false,
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
            workspace_id: None,
        }
    }
}
