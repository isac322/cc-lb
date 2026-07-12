use std::time::Duration;

use cc_lb_storage_api::StorageError;
use sqlx::Error as SqlxError;

pub struct RetryPolicy {
    attempts: Vec<Duration>,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            attempts: vec![
                Duration::from_millis(50),
                Duration::from_millis(200),
                Duration::from_millis(500),
            ],
        }
    }
}

pub async fn with_retry<T, F, Fut>(policy: &RetryPolicy, mut op: F) -> Result<T, SqlxError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, SqlxError>>,
{
    let mut attempt = 0usize;
    loop {
        match op().await {
            Ok(value) => return Ok(value),
            Err(err) if is_transient(&err) && attempt < policy.attempts.len() => {
                tokio::time::sleep(policy.attempts[attempt]).await;
                attempt += 1;
            }
            Err(err) => return Err(err),
        }
    }
}

pub async fn with_retry_storage<T, F, Fut>(
    policy: &RetryPolicy,
    mut op: F,
) -> Result<T, StorageError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, StorageError>>,
{
    let mut attempt = 0usize;
    loop {
        match op().await {
            Ok(value) => return Ok(value),
            Err(err) if err.is_retryable() && attempt < policy.attempts.len() => {
                tokio::time::sleep(policy.attempts[attempt]).await;
                attempt += 1;
            }
            Err(err) => return Err(err),
        }
    }
}

fn is_transient(err: &SqlxError) -> bool {
    if matches!(
        err,
        SqlxError::PoolTimedOut | SqlxError::PoolClosed | SqlxError::Io(_) | SqlxError::Tls(_)
    ) {
        return true;
    }
    // 40001 serialization_failure and 40P01 deadlock_detected abort the whole
    // transaction; a fresh attempt can commit, so treat them as retryable.
    err.as_database_error()
        .and_then(|db| db.code())
        .is_some_and(|code| matches!(code.as_ref(), "40001" | "40P01"))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    #[tokio::test]
    async fn transient_error_retries_then_succeeds() {
        let call_count = Arc::new(AtomicUsize::new(0));
        let policy = RetryPolicy::default();

        let result = with_retry(&policy, || {
            let count = Arc::clone(&call_count);
            async move {
                let n = count.fetch_add(1, Ordering::SeqCst);
                if n < 2 {
                    Err(SqlxError::PoolTimedOut)
                } else {
                    Ok(42u32)
                }
            }
        })
        .await;

        assert_eq!(result.unwrap(), 42u32);
        assert_eq!(call_count.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn logical_error_not_retried() {
        let call_count = Arc::new(AtomicUsize::new(0));
        let policy = RetryPolicy::default();

        let result: Result<u32, SqlxError> = with_retry(&policy, || {
            let count = Arc::clone(&call_count);
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                Err(SqlxError::RowNotFound)
            }
        })
        .await;

        assert!(matches!(result, Err(SqlxError::RowNotFound)));
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn exhausted_returns_error() {
        let call_count = Arc::new(AtomicUsize::new(0));
        let policy = RetryPolicy::default();

        let result: Result<u32, SqlxError> = with_retry(&policy, || {
            let count = Arc::clone(&call_count);
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                Err(SqlxError::PoolClosed)
            }
        })
        .await;

        assert!(matches!(result, Err(SqlxError::PoolClosed)));
        assert_eq!(call_count.load(Ordering::SeqCst), 4);
    }

    #[tokio::test]
    async fn retryable_storage_error_retries_then_succeeds() {
        let call_count = Arc::new(AtomicUsize::new(0));
        let policy = RetryPolicy::default();

        let result = with_retry_storage(&policy, || {
            let count = Arc::clone(&call_count);
            async move {
                let n = count.fetch_add(1, Ordering::SeqCst);
                if n < 2 {
                    Err(StorageError::Transient {
                        retryable: true,
                        source: Box::<dyn std::error::Error + Send + Sync>::from("serialize"),
                    })
                } else {
                    Ok(7u32)
                }
            }
        })
        .await;

        assert_eq!(result.unwrap(), 7u32);
        assert_eq!(call_count.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn non_retryable_storage_error_not_retried() {
        let call_count = Arc::new(AtomicUsize::new(0));
        let policy = RetryPolicy::default();

        let result: Result<u32, StorageError> = with_retry_storage(&policy, || {
            let count = Arc::clone(&call_count);
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                Err(StorageError::Conflict {
                    message: "dup".to_owned(),
                })
            }
        })
        .await;

        assert!(matches!(result, Err(StorageError::Conflict { .. })));
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
    }
}
