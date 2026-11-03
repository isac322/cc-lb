use std::env;
use std::ffi::OsString;
use std::sync::{Mutex, MutexGuard};

static TRYBUILD_ENVIRONMENT_LOCK: Mutex<()> = Mutex::new(());

const COMPILER_ENVIRONMENT_VARIABLES: [&str; 4] = [
    "RUSTFLAGS",
    "CARGO_ENCODED_RUSTFLAGS",
    "CARGO_BUILD_RUSTC_WRAPPER",
    "RUSTC_WRAPPER",
];

pub(crate) struct TrybuildEnvironmentGuard {
    previous: [(&'static str, Option<OsString>); 4],
    _lock: MutexGuard<'static, ()>,
}

impl TrybuildEnvironmentGuard {
    pub(crate) fn new() -> Self {
        let lock = TRYBUILD_ENVIRONMENT_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let guard = Self {
            previous: COMPILER_ENVIRONMENT_VARIABLES.map(|key| (key, env::var_os(key))),
            _lock: lock,
        };
        for (key, _) in &guard.previous {
            // SAFETY: test-only; both trybuild runners hold the shared environment lock.
            unsafe { env::remove_var(key) };
        }
        guard
    }
}

impl Drop for TrybuildEnvironmentGuard {
    fn drop(&mut self) {
        for (key, previous) in &self.previous {
            // SAFETY: test-only; the shared lock remains held until restoration completes.
            unsafe {
                match previous {
                    Some(value) => env::set_var(key, value),
                    None => env::remove_var(key),
                }
            }
        }
    }
}
