use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::Notify;

#[derive(Clone)]
pub struct OAuthRefreshPause {
    inner: Arc<OAuthRefreshPauseInner>,
}

struct OAuthRefreshPauseInner {
    entered: AtomicBool,
    released: AtomicBool,
    entered_notify: Notify,
    released_notify: Notify,
}

impl fmt::Debug for OAuthRefreshPause {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OAuthRefreshPause")
            .field("entered", &self.inner.entered.load(Ordering::SeqCst))
            .field("released", &self.inner.released.load(Ordering::SeqCst))
            .finish()
    }
}

impl Default for OAuthRefreshPause {
    fn default() -> Self {
        Self::new()
    }
}

impl OAuthRefreshPause {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(OAuthRefreshPauseInner {
                entered: AtomicBool::new(false),
                released: AtomicBool::new(false),
                entered_notify: Notify::new(),
                released_notify: Notify::new(),
            }),
        }
    }

    pub async fn wait_until_entered(&self) {
        while !self.inner.entered.load(Ordering::SeqCst) {
            self.inner.entered_notify.notified().await;
        }
    }

    pub fn release(&self) {
        self.inner.released.store(true, Ordering::SeqCst);
        self.inner.released_notify.notify_waiters();
    }

    pub(crate) async fn pause_response(&self) {
        self.inner.entered.store(true, Ordering::SeqCst);
        self.inner.entered_notify.notify_waiters();
        while !self.inner.released.load(Ordering::SeqCst) {
            self.inner.released_notify.notified().await;
        }
    }
}
