//! Minimal observe plugin used by
//! `crates/cc-lb-runtime-wasmtime/tests/observe_drain.rs`.
//!
//! Accepts any [`ObserveEvent`] and returns. Verifies that:
//!
//! * The host alloc → call → free flow holds for the observe hook.
//! * Observe returns `(0, 0)` packed (the dispatch helper skips
//!   the output free path automatically).
#![cfg_attr(target_arch = "wasm32", no_std)]

extern crate alloc;

use cc_lb_pdk_wasmtime::types::ArchivedObserveEvent;

#[cc_lb_pdk_wasmtime::plugin(name = "wasmtime-observe-noop", version = "0.1.0")]
mod noop {
    use super::*;

    #[cc_lb_pdk_wasmtime::handler(name = "observe", view)]
    pub fn observe(_event: &ArchivedObserveEvent) {
        // Best-effort drain: drop everything. Real plugins would
        // buffer + drain in a host-side queue per RFC §observe.
    }
}

pub use noop::observe;
