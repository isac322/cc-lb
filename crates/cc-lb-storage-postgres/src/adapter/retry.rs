use std::time::Duration;

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

fn is_transient(err: &SqlxError) -> bool {
    matches!(
        err,
        SqlxError::PoolTimedOut | SqlxError::PoolClosed | SqlxError::Io(_) | SqlxError::Tls(_)
    )
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
}
