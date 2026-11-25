//! Process-wide Wasmtime epoch ticker for guest loop preemption.
//!
//! A single daemon thread advances epochs for every live hot engine every
//! [`EPOCH_TICK_INTERVAL_MS`] ms. Engines are held weakly so destroyed
//! runtimes stop ticking cleanly while engines kept alive solely by
//! `PROCESS_WIDE_COMPILED_MODULE_CACHE` continue to preempt infinite loops.

use std::sync::{LazyLock, Mutex};
use std::time::Duration;

use wasmtime::{Engine, EngineWeak};

pub(crate) const EPOCH_TICK_INTERVAL_MS: u64 = 10;
pub(crate) const DEFAULT_EXECUTION_TIMEOUT_MS: u64 = 1_000;
/// Includes `+ 1` partial leading tick because the daemon ticker runs asynchronously
/// relative to store creation, ensuring the configured budget never fires early.
pub(crate) const DEFAULT_EPOCH_DEADLINE_TICKS: u64 =
    (DEFAULT_EXECUTION_TIMEOUT_MS / EPOCH_TICK_INTERVAL_MS) + 1;

static EPOCH_REGISTRY: LazyLock<Mutex<Vec<EngineWeak>>> = LazyLock::new(|| {
    let registry = Mutex::new(Vec::<EngineWeak>::new());
    std::thread::Builder::new()
        .name("cclb-wasm-epoch".to_owned())
        .spawn(run_epoch_ticker)
        .expect("spawn wasmtime epoch ticker thread");
    registry
});

pub(crate) fn ticks_for_timeout(timeout: Duration) -> u64 {
    let millis = u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX);
    // Add one extra tick to account for the partial interval before the first ticker step.
    millis
        .div_ceil(EPOCH_TICK_INTERVAL_MS)
        .max(1)
        .saturating_add(1)
}

pub(crate) fn register_engine(engine: &Engine) {
    let weak = engine.weak();
    let mut engines = EPOCH_REGISTRY
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    engines.retain(|entry| entry.upgrade().is_some());
    engines.push(weak);
}

fn run_epoch_ticker() -> ! {
    let interval = Duration::from_millis(EPOCH_TICK_INTERVAL_MS);
    loop {
        std::thread::sleep(interval);
        let live: Vec<Engine> = {
            let mut engines = EPOCH_REGISTRY
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let mut upgraded = Vec::with_capacity(engines.len());
            engines.retain(|weak| match weak.upgrade() {
                Some(engine) => {
                    upgraded.push(engine);
                    true
                }
                None => false,
            });
            upgraded
        };
        for engine in live {
            engine.increment_epoch();
        }
    }
}
