use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use tokio::sync::Notify;

#[derive(Clone, Debug)]
pub struct ManualGate {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    is_open: AtomicBool,
    notify: Notify,
}

impl ManualGate {
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                is_open: AtomicBool::new(false),
                notify: Notify::new(),
            }),
        }
    }

    pub fn open(&self) {
        if !self.inner.is_open.swap(true, Ordering::AcqRel) {
            self.inner.notify.notify_waiters();
        }
    }

    pub async fn wait(&self) {
        loop {
            if self.inner.is_open.load(Ordering::Acquire) {
                return;
            }

            let notified = self.inner.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            if self.inner.is_open.load(Ordering::Acquire) {
                return;
            }

            notified.await;
        }
    }
}

impl Default for ManualGate {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::ManualGate;

    #[tokio::test]
    async fn open_before_wait_is_observed() {
        let gate = ManualGate::new();
        gate.open();
        gate.wait().await;
    }

    #[tokio::test]
    async fn open_releases_all_waiters() {
        let gate = ManualGate::new();
        let first = tokio::spawn({
            let gate = gate.clone();
            async move { gate.wait().await }
        });
        let second = tokio::spawn({
            let gate = gate.clone();
            async move { gate.wait().await }
        });

        tokio::task::yield_now().await;
        gate.open();

        first.await.expect("first waiter");
        second.await.expect("second waiter");
    }
}
