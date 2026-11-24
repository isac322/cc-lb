//! Tracks detached post-commit tasks so shutdown can drain them before exit.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::Notify;
use tokio::task::JoinHandle;

#[derive(Clone, Default)]
pub struct PostCommitTracker {
    inner: Arc<PostCommitTrackerInner>,
}

#[derive(Default)]
struct PostCommitTrackerInner {
    in_flight: AtomicUsize,
    idle: Notify,
}

struct PostCommitGuard {
    inner: Arc<PostCommitTrackerInner>,
}

impl Drop for PostCommitGuard {
    fn drop(&mut self) {
        if self.inner.in_flight.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.inner.idle.notify_waiters();
        }
    }
}

impl PostCommitTracker {
    pub fn spawn<F, T>(&self, future: F) -> JoinHandle<T>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        // Increment before spawning so wait_idle never misses an unpolled task.
        self.inner.in_flight.fetch_add(1, Ordering::AcqRel);
        let guard = PostCommitGuard {
            inner: Arc::clone(&self.inner),
        };
        tokio::spawn(async move {
            let _guard = guard;
            future.await
        })
    }

    pub fn in_flight(&self) -> usize {
        self.inner.in_flight.load(Ordering::Acquire)
    }

    pub async fn wait_idle(&self) {
        loop {
            let notified = self.inner.idle.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.inner.in_flight.load(Ordering::Acquire) == 0 {
                return;
            }
            notified.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;

    #[tokio::test]
    async fn wait_idle_returns_immediately_when_empty() {
        let tracker = PostCommitTracker::default();
        assert_eq!(tracker.in_flight(), 0);
        tracker.wait_idle().await;
        assert_eq!(tracker.in_flight(), 0);
    }

    #[tokio::test]
    async fn wait_idle_blocks_until_detached_task_finishes() {
        let tracker = PostCommitTracker::default();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
        let completed = Arc::new(AtomicBool::new(false));
        let completed_task = Arc::clone(&completed);

        let handle = tracker.spawn(async move {
            let _ = release_rx.await;
            completed_task.store(true, Ordering::Release);
            42usize
        });
        assert_eq!(tracker.in_flight(), 1);

        let wait_tracker = tracker.clone();
        let waiter = tokio::spawn(async move {
            wait_tracker.wait_idle().await;
        });
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());

        let _ = release_tx.send(());
        waiter.await.expect("wait_idle finishes");
        assert!(completed.load(Ordering::Acquire));
        assert_eq!(tracker.in_flight(), 0);
        assert_eq!(handle.await.expect("task result"), 42);
    }

    #[tokio::test]
    async fn wait_idle_releases_when_task_panics() {
        let tracker = PostCommitTracker::default();
        let handle = tracker.spawn(async {
            panic!("intentional test panic");
        });
        tracker.wait_idle().await;
        assert_eq!(tracker.in_flight(), 0);
        assert!(handle.await.expect_err("panic propagates").is_panic());
    }
}
