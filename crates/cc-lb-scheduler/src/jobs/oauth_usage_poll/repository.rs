use std::future::Future;
use std::pin::Pin;

use uuid::Uuid;

use super::OAuthUsagePollHandler;
use crate::error::Result;
use crate::idempotency::OAuthUsagePollCursor;

pub type OAuthUsagePollFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub trait OAuthUsagePollCursorRepository {
    fn read_cursor(
        &self,
        upstream_id: Uuid,
    ) -> OAuthUsagePollFuture<'_, Result<Option<OAuthUsagePollCursor>>>;

    fn record_success_cursor(
        &self,
        upstream_id: Uuid,
        observed_at_unix_secs: u64,
        window_start_unix_millis: u64,
        window_end_unix_millis: u64,
        ring_cap: usize,
    ) -> OAuthUsagePollFuture<'_, Result<()>>;

    fn record_throttle_cursor(
        &self,
        upstream_id: Uuid,
        observed_at_unix_secs: u64,
        throttle_count: u32,
        ring_cap: usize,
    ) -> OAuthUsagePollFuture<'_, Result<()>>;

    fn upsert_cursor<'a>(
        &'a self,
        cursor: &'a OAuthUsagePollCursor,
    ) -> OAuthUsagePollFuture<'a, Result<()>>;
}

#[cfg(feature = "sqlite")]
impl OAuthUsagePollCursorRepository for OAuthUsagePollHandler<sqlx::Sqlite> {
    fn read_cursor(
        &self,
        upstream_id: Uuid,
    ) -> OAuthUsagePollFuture<'_, Result<Option<OAuthUsagePollCursor>>> {
        Box::pin(self.cursors.read(upstream_id))
    }

    fn record_success_cursor(
        &self,
        upstream_id: Uuid,
        observed_at_unix_secs: u64,
        window_start_unix_millis: u64,
        window_end_unix_millis: u64,
        ring_cap: usize,
    ) -> OAuthUsagePollFuture<'_, Result<()>> {
        Box::pin(self.cursors.record_success(
            upstream_id,
            observed_at_unix_secs,
            window_start_unix_millis,
            window_end_unix_millis,
            ring_cap,
        ))
    }

    fn record_throttle_cursor(
        &self,
        upstream_id: Uuid,
        observed_at_unix_secs: u64,
        throttle_count: u32,
        ring_cap: usize,
    ) -> OAuthUsagePollFuture<'_, Result<()>> {
        Box::pin(self.cursors.record_throttle(
            upstream_id,
            observed_at_unix_secs,
            throttle_count,
            ring_cap,
        ))
    }

    fn upsert_cursor<'a>(
        &'a self,
        cursor: &'a OAuthUsagePollCursor,
    ) -> OAuthUsagePollFuture<'a, Result<()>> {
        Box::pin(self.cursors.upsert(cursor))
    }
}

#[cfg(feature = "postgres")]
impl OAuthUsagePollCursorRepository for OAuthUsagePollHandler<sqlx::Postgres> {
    fn read_cursor(
        &self,
        upstream_id: Uuid,
    ) -> OAuthUsagePollFuture<'_, Result<Option<OAuthUsagePollCursor>>> {
        Box::pin(self.cursors.read(upstream_id))
    }

    fn record_success_cursor(
        &self,
        upstream_id: Uuid,
        observed_at_unix_secs: u64,
        window_start_unix_millis: u64,
        window_end_unix_millis: u64,
        ring_cap: usize,
    ) -> OAuthUsagePollFuture<'_, Result<()>> {
        Box::pin(self.cursors.record_success(
            upstream_id,
            observed_at_unix_secs,
            window_start_unix_millis,
            window_end_unix_millis,
            ring_cap,
        ))
    }

    fn record_throttle_cursor(
        &self,
        upstream_id: Uuid,
        observed_at_unix_secs: u64,
        throttle_count: u32,
        ring_cap: usize,
    ) -> OAuthUsagePollFuture<'_, Result<()>> {
        Box::pin(self.cursors.record_throttle(
            upstream_id,
            observed_at_unix_secs,
            throttle_count,
            ring_cap,
        ))
    }

    fn upsert_cursor<'a>(
        &'a self,
        cursor: &'a OAuthUsagePollCursor,
    ) -> OAuthUsagePollFuture<'a, Result<()>> {
        Box::pin(self.cursors.upsert(cursor))
    }
}
