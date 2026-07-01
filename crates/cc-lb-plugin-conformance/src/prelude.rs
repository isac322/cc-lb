//! One-line import of everything a typical conformance test needs.
//!
//! ```ignore
//! use cc_lb_plugin_conformance::prelude::*;
//!
//! #[test]
//! fn conformance() {
//!     ConformanceSuite::for_shape(&wasm()).with_plugin_name("x").run();
//! }
//! ```

pub use crate::{
    ConformanceSuite, PluginSession, conformance_engine_config,
    fixtures::{hdr, observe_event_samples, synth_principal},
};
pub use cc_lb_plugin_types::{
    FilterRequest, FilterResponse, Header, NormalizeErrorRequest, NormalizeErrorResponse,
    ObserveEvent, PerCandidateReason, Principal, ShapeRequest, ShapeResponse, Upstream,
    UpstreamCandidate,
};
pub use cc_lb_runtime_wasmtime::{HotEngineConfig, SlotKind};
