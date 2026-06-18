use std::future::Future;

use cc_lb_aead::EncryptedOAuthTokens;
use cc_lb_storage_api::{StorageError, UpstreamRecord, UpstreamStore};
use uuid::Uuid;

use crate::error::{Result, SchedulerError};
use crate::idempotency::OAuthRefreshClaimsStore;

pub trait OAuthRefreshClaims {
    fn try_acquire<'a>(
        &'a self,
        upstream_id: Uuid,
        holder: &'a str,
        ttl_secs: u64,
        now_unix_secs: u64,
    ) -> impl Future<Output = Result<bool>> + Send + 'a;

    fn complete_and_bump_generation<'a>(
        &'a self,
        upstream_id: Uuid,
        holder: &'a str,
        new_generation: u64,
    ) -> impl Future<Output = Result<bool>> + Send + 'a;

    fn release_if_holder<'a>(
        &'a self,
        upstream_id: Uuid,
        holder: &'a str,
    ) -> impl Future<Output = Result<bool>> + Send + 'a;
}

#[cfg(feature = "sqlite")]
impl OAuthRefreshClaims for OAuthRefreshClaimsStore<sqlx::Sqlite> {
    async fn try_acquire(
        &self,
        upstream_id: Uuid,
        holder: &str,
        ttl_secs: u64,
        now_unix_secs: u64,
    ) -> Result<bool> {
        OAuthRefreshClaimsStore::<sqlx::Sqlite>::try_acquire(
            self,
            upstream_id,
            holder,
            ttl_secs,
            now_unix_secs,
        )
        .await
    }

    async fn complete_and_bump_generation(
        &self,
        upstream_id: Uuid,
        holder: &str,
        new_generation: u64,
    ) -> Result<bool> {
        OAuthRefreshClaimsStore::<sqlx::Sqlite>::complete_and_bump_generation(
            self,
            upstream_id,
            holder,
            new_generation,
        )
        .await
    }

    async fn release_if_holder(&self, upstream_id: Uuid, holder: &str) -> Result<bool> {
        OAuthRefreshClaimsStore::<sqlx::Sqlite>::release_if_holder(self, upstream_id, holder).await
    }
}

#[cfg(feature = "postgres")]
impl OAuthRefreshClaims for OAuthRefreshClaimsStore<sqlx::Postgres> {
    async fn try_acquire(
        &self,
        upstream_id: Uuid,
        holder: &str,
        ttl_secs: u64,
        now_unix_secs: u64,
    ) -> Result<bool> {
        OAuthRefreshClaimsStore::<sqlx::Postgres>::try_acquire(
            self,
            upstream_id,
            holder,
            ttl_secs,
            now_unix_secs,
        )
        .await
    }

    async fn complete_and_bump_generation(
        &self,
        upstream_id: Uuid,
        holder: &str,
        new_generation: u64,
    ) -> Result<bool> {
        OAuthRefreshClaimsStore::<sqlx::Postgres>::complete_and_bump_generation(
            self,
            upstream_id,
            holder,
            new_generation,
        )
        .await
    }

    async fn release_if_holder(&self, upstream_id: Uuid, holder: &str) -> Result<bool> {
        OAuthRefreshClaimsStore::<sqlx::Postgres>::release_if_holder(self, upstream_id, holder)
            .await
    }
}

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
