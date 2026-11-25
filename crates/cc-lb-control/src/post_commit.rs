//! Tracks in-flight admin mutations and detached post-commit continuations so
//! shutdown closes admission and drains active work before exit.

use std::future::Future;
use std::sync::{Arc, Mutex, PoisonError};

use tokio::sync::Notify;
use tokio::task::JoinHandle;

#[derive(Default)]
struct PostCommitState {
    closed: bool,
    in_flight: usize,
}

#[derive(Default)]
struct PostCommitTrackerInner {
    state: Mutex<PostCommitState>,
    idle: Notify,
}

fn release_in_flight(inner: &PostCommitTrackerInner) {
    let became_idle = {
        let mut state = inner.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.in_flight = state.in_flight.saturating_sub(1);
        state.in_flight == 0
    };
    if became_idle {
        inner.idle.notify_waiters();
    }
}

/// RAII permit held across an admitted admin mutating request so shutdown
/// `wait_idle` cannot observe an idle tracker before its handler registers
/// post-commit work (including HTTP/2 requests spawned onto `TokioExecutor`).
pub struct PostCommitPermit {
    inner: Arc<PostCommitTrackerInner>,
}

impl Drop for PostCommitPermit {
    fn drop(&mut self) {
        release_in_flight(&self.inner);
    }
}

struct PostCommitGuard {
    inner: Arc<PostCommitTrackerInner>,
}

impl Drop for PostCommitGuard {
    fn drop(&mut self) {
        release_in_flight(&self.inner);
    }
}

#[derive(Clone, Default)]
pub struct PostCommitTracker {
    inner: Arc<PostCommitTrackerInner>,
}

impl PostCommitTracker {
    /// Closes request admission so subsequent `try_begin_request` calls reject
    /// new mutations while already-admitted requests may still spawn and finish
    /// their post-commit continuations.
    pub fn close(&self) {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        state.closed = true;
    }

    pub fn is_closed(&self) -> bool {
        self.inner
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .closed
    }

    /// Admits one mutating admin request unless shutdown has closed the gate.
    pub fn try_begin_request(&self) -> Option<PostCommitPermit> {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if state.closed {
            return None;
        }
        state.in_flight += 1;
        Some(PostCommitPermit {
            inner: Arc::clone(&self.inner),
        })
    }

    pub fn spawn<F, T>(&self, future: F) -> JoinHandle<T>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        // Increment under the mutex before spawning so wait_idle never misses an
        // unpolled continuation even if the enclosing HTTP request permit drops first.
        {
            let mut state = self
                .inner
                .state
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            state.in_flight += 1;
        }
        let guard = PostCommitGuard {
            inner: Arc::clone(&self.inner),
        };
        tokio::spawn(async move {
            let _guard = guard;
            future.await
        })
    }

    pub fn in_flight(&self) -> usize {
        self.inner
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .in_flight
    }

    pub async fn wait_idle(&self) {
        loop {
            let notified = self.inner.idle.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let state = self
                    .inner
                    .state
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner);
                if state.in_flight == 0 {
                    return;
                }
            }
            notified.await;
        }
    }

    pub async fn close_and_wait_idle(&self) {
        self.close();
        self.wait_idle().await;
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

    #[tokio::test]
    async fn close_rejects_new_requests_and_wait_idle_tracks_inflight_request_and_continuation() {
        let tracker = PostCommitTracker::default();
        let permit = tracker
            .try_begin_request()
            .expect("open tracker must admit request");
        assert_eq!(tracker.in_flight(), 1);

        tracker.close();
        assert!(tracker.is_closed());
        assert!(
            tracker.try_begin_request().is_none(),
            "closed tracker must reject new mutations"
        );

        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let wait_tracker = tracker.clone();
        let waiter = tokio::spawn(async move {
            wait_tracker.wait_idle().await;
        });
        tokio::task::yield_now().await;
        assert!(
            !waiter.is_finished(),
            "wait_idle must wait for request permit"
        );

        // Already-admitted request spawns a post-commit continuation before dropping permit.
        let continuation = tracker.spawn(async move {
            let _ = rx.await;
        });
        assert_eq!(tracker.in_flight(), 2);
        drop(permit);
        assert_eq!(tracker.in_flight(), 1);
        tokio::task::yield_now().await;
        assert!(
            !waiter.is_finished(),
            "wait_idle must remain blocked for spawned continuation"
        );

        let _ = tx.send(());
        waiter.await.expect("wait_idle finishes");
        continuation.await.expect("continuation joins");
        assert_eq!(tracker.in_flight(), 0);
    }
}
