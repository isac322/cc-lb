use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub trait Clock: Send + Sync {
    fn now_unix_secs(&self) -> u64;
}

#[derive(Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_unix_secs(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_secs()
    }
}

#[derive(Debug)]
pub struct MockClock {
    now_unix_secs: AtomicU64,
}

impl MockClock {
    pub fn new(now_unix_secs: u64) -> Self {
        Self {
            now_unix_secs: AtomicU64::new(now_unix_secs),
        }
    }

    pub fn set(&self, now_unix_secs: u64) {
        self.now_unix_secs.store(now_unix_secs, Ordering::SeqCst);
    }

    pub fn advance(&self, delta: Duration) {
        self.now_unix_secs
            .fetch_add(delta.as_secs(), Ordering::SeqCst);
    }
}

impl Clock for MockClock {
    fn now_unix_secs(&self) -> u64 {
        self.now_unix_secs.load(Ordering::SeqCst)
    }
}
