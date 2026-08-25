use std::future::Future;

use cc_lb_aead::EncryptedOAuthTokens;
use cc_lb_storage_api::{StorageError, UpstreamRecord, UpstreamStore};
use uuid::Uuid;

use crate::error::{Result, SchedulerError};

pub trait OAuthRefreshUpstreams {
    fn get_by_id(
        &self,
        id: Uuid,
    ) -> impl Future<Output = Result<Option<UpstreamRecord>>> + Send + '_;

    fn claim_refresh_lease(
        &self,
        id: Uuid,
        holder: Uuid,
        expected_generation: u64,
        ttl_secs: u64,
    ) -> impl Future<Output = Result<bool>> + Send + '_;

    fn read_oauth_refresh_terminal_failure(
        &self,
        id: Uuid,
    ) -> impl Future<Output = Result<Option<cc_lb_storage_api::OAuthRefreshTerminalFailure>>> + Send + '_;

    fn fail_refresh(
        &self,
        id: Uuid,
        holder: Uuid,
        terminal_error: Option<String>,
    ) -> impl Future<Output = Result<bool>> + Send + '_;

    fn complete_refresh(
        &self,
        id: Uuid,
        holder: Uuid,
        tokens: EncryptedOAuthTokens,
    ) -> impl Future<Output = Result<UpstreamRecord>> + Send + '_;
}

impl<T> OAuthRefreshUpstreams for T
where
    T: UpstreamStore + Sync,
{
    async fn get_by_id(&self, id: Uuid) -> Result<Option<UpstreamRecord>> {
        UpstreamStore::get_by_id(self, id)
            .await
            .map_err(storage_error)
    }

    async fn claim_refresh_lease(
        &self,
        id: Uuid,
        holder: Uuid,
        expected_generation: u64,
        ttl_secs: u64,
    ) -> Result<bool> {
        UpstreamStore::claim_refresh_lease(self, id, holder, expected_generation, ttl_secs)
            .await
            .map_err(storage_error)
    }

    async fn read_oauth_refresh_terminal_failure(
        &self,
        id: Uuid,
    ) -> Result<Option<cc_lb_storage_api::OAuthRefreshTerminalFailure>> {
        UpstreamStore::read_oauth_refresh_terminal_failure(self, id)
            .await
            .map_err(storage_error)
    }

    async fn fail_refresh(
        &self,
        id: Uuid,
        holder: Uuid,
        terminal_error: Option<String>,
    ) -> Result<bool> {
        UpstreamStore::fail_refresh(self, id, holder, terminal_error)
            .await
            .map_err(storage_error)
    }

    async fn complete_refresh(
        &self,
        id: Uuid,
        holder: Uuid,
        tokens: EncryptedOAuthTokens,
    ) -> Result<UpstreamRecord> {
        UpstreamStore::complete_refresh(self, id, holder, tokens)
            .await
            .map_err(storage_error)
    }
}

fn storage_error(error: StorageError) -> SchedulerError {
    SchedulerError::Job(error.to_string())
}
