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

    fn complete_refresh(
        &self,
        id: Uuid,
        holder: Uuid,
        tokens: EncryptedOAuthTokens,
    ) -> impl Future<Output = Result<UpstreamRecord>> + Send + '_;

    fn read_oauth_token_generation(
        &self,
        id: Uuid,
    ) -> impl Future<Output = Result<Option<u64>>> + Send + '_;
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

    async fn read_oauth_token_generation(&self, id: Uuid) -> Result<Option<u64>> {
        UpstreamStore::read_oauth_token_generation(self, id)
            .await
            .map_err(storage_error)
    }
}

fn storage_error(error: StorageError) -> SchedulerError {
    SchedulerError::Job(error.to_string())
}
