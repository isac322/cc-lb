#![forbid(unsafe_code)]

#[cfg(not(loom))]
pub fn _ensure_compiles() {}

#[cfg(loom)]
pub mod quota {
    use loom::sync::atomic::{AtomicU64, Ordering};

    pub struct QuotaCounter {
        current_usage: AtomicU64,
    }

    impl QuotaCounter {
        pub fn new() -> Self {
            Self {
                current_usage: AtomicU64::new(0),
            }
        }

        pub fn try_consume(&self, amount: u64, limit: u64) -> ConsumeResult {
            let mut observed = self.current_usage.load(Ordering::SeqCst);
            loop {
                if observed.saturating_add(amount) > limit {
                    return ConsumeResult::Rejected {
                        observed,
                        amount,
                        limit,
                    };
                }

                let next = observed + amount;
                match self.current_usage.compare_exchange(
                    observed,
                    next,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    Ok(_) => return ConsumeResult::Allowed { amount },
                    Err(actual) => observed = actual,
                }
            }
        }

        pub fn usage(&self) -> u64 {
            self.current_usage.load(Ordering::SeqCst)
        }
    }

    impl Default for QuotaCounter {
        fn default() -> Self {
            Self::new()
        }
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub enum ConsumeResult {
        Allowed {
            amount: u64,
        },
        Rejected {
            observed: u64,
            amount: u64,
            limit: u64,
        },
    }
}

#[cfg(loom)]
pub mod arcswap_cert {
    use loom::sync::{Arc, Mutex};

    #[derive(Debug)]
    pub struct Cert {
        pub id: u64,
    }

    pub struct CertStore {
        current: Mutex<Arc<Cert>>,
    }

    impl CertStore {
        pub fn new(cert: Arc<Cert>) -> Self {
            Self {
                current: Mutex::new(cert),
            }
        }

        pub fn load(&self) -> Arc<Cert> {
            let current = match self.current.lock() {
                Ok(current) => current,
                Err(poisoned) => poisoned.into_inner(),
            };
            Arc::clone(&current)
        }

        pub fn store_next(&self) {
            let mut current = match self.current.lock() {
                Ok(current) => current,
                Err(poisoned) => poisoned.into_inner(),
            };
            let next_id = current.id + 1;
            *current = Arc::new(Cert { id: next_id });
        }
    }
}

#[cfg(loom)]
pub mod single_flight {
    use loom::sync::Mutex;
    use loom::sync::atomic::{AtomicU32, AtomicU64, Ordering};

    pub struct RefreshState {
        lock: Mutex<()>,
        last_refresh_at: AtomicU64,
        token_calls: AtomicU32,
    }

    impl RefreshState {
        pub fn new() -> Self {
            Self {
                lock: Mutex::new(()),
                last_refresh_at: AtomicU64::new(0),
                token_calls: AtomicU32::new(0),
            }
        }

        pub fn on_unauthorized(&self, observed_refresh_at: u64) {
            let _guard = match self.lock.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            let current_refresh_at = self.last_refresh_at.load(Ordering::SeqCst);
            if current_refresh_at > observed_refresh_at {
                return;
            }

            self.token_calls.fetch_add(1, Ordering::SeqCst);
            self.last_refresh_at
                .store(current_refresh_at + 1, Ordering::SeqCst);
        }

        pub fn token_calls(&self) -> u32 {
            self.token_calls.load(Ordering::SeqCst)
        }
    }

    impl Default for RefreshState {
        fn default() -> Self {
            Self::new()
        }
    }
}
