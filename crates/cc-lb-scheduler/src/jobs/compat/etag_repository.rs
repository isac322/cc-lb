use std::future::Future;
use std::pin::Pin;

use crate::error::Result;
use crate::idempotency::{AnthropicCompatEtag, AnthropicCompatEtagsStore};

pub type CompatJobFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub trait CompatEtagRepository {
    fn read_compat_etag<'a>(
        &'a self,
        key: &'a str,
    ) -> CompatJobFuture<'a, Result<Option<AnthropicCompatEtag>>>;

    fn upsert_compat_value<'a>(
        &'a self,
        key: &'a str,
        etag: Option<&'a str>,
        hash: &'a str,
        now_unix_secs: u64,
    ) -> CompatJobFuture<'a, Result<()>>;
}

#[cfg(feature = "sqlite")]
impl CompatEtagRepository for AnthropicCompatEtagsStore<sqlx::Sqlite> {
    fn read_compat_etag<'a>(
        &'a self,
        key: &'a str,
    ) -> CompatJobFuture<'a, Result<Option<AnthropicCompatEtag>>> {
        Box::pin(self.read(key))
    }

    fn upsert_compat_value<'a>(
        &'a self,
        key: &'a str,
        etag: Option<&'a str>,
        hash: &'a str,
        now_unix_secs: u64,
    ) -> CompatJobFuture<'a, Result<()>> {
        Box::pin(self.upsert_value(key, etag, hash, now_unix_secs))
    }
}

#[cfg(feature = "postgres")]
impl CompatEtagRepository for AnthropicCompatEtagsStore<sqlx::Postgres> {
    fn read_compat_etag<'a>(
        &'a self,
        key: &'a str,
    ) -> CompatJobFuture<'a, Result<Option<AnthropicCompatEtag>>> {
        Box::pin(self.read(key))
    }

    fn upsert_compat_value<'a>(
        &'a self,
        key: &'a str,
        etag: Option<&'a str>,
        hash: &'a str,
        now_unix_secs: u64,
    ) -> CompatJobFuture<'a, Result<()>> {
        Box::pin(self.upsert_value(key, etag, hash, now_unix_secs))
    }
}
