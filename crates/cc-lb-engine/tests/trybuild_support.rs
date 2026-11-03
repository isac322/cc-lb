use std::env;
use std::ffi::OsString;
use std::path::Path;
use std::process;
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
    previous_target_dir: Option<OsString>,
    _lock: MutexGuard<'static, ()>,
}

impl TrybuildEnvironmentGuard {
    pub(crate) fn new() -> Self {
        let lock = TRYBUILD_ENVIRONMENT_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let previous_target_dir = env::var_os("CARGO_TARGET_DIR");
        let target_dir = previous_target_dir
            .as_deref()
            .map(Path::new)
            .unwrap_or_else(|| Path::new("target"))
            .join(format!("trybuild-{}", process::id()));
        let guard = Self {
            previous: COMPILER_ENVIRONMENT_VARIABLES.map(|key| (key, env::var_os(key))),
            previous_target_dir,
            _lock: lock,
        };
        for (key, _) in &guard.previous {
            // SAFETY: test-only; both trybuild runners hold the shared environment lock.
            unsafe { env::remove_var(key) };
        }
        // SAFETY: test-only; both trybuild runners hold the shared environment lock.
        unsafe { env::set_var("CARGO_TARGET_DIR", target_dir) };
        guard
    }
}

impl Drop for TrybuildEnvironmentGuard {
    fn drop(&mut self) {
        // SAFETY: test-only; the shared lock remains held until restoration completes.
        unsafe {
            match &self.previous_target_dir {
                Some(value) => env::set_var("CARGO_TARGET_DIR", value),
                None => env::remove_var("CARGO_TARGET_DIR"),
            }
        }
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
