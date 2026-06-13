//! In-process conformance scaffolding for cc-lb plugin authors.
//!
//! This crate exposes protocol-layer wrappers, plugin-kind verifier entry points,
//! and test fixture helpers for compiled WebAssembly plugins targeting cc-lb.
//!
//! # Supported Plugin Kinds
//!
//! The conformance suite supports testing the following plugin types:
//!
//! * **Router Plugins**: Plugins that route requests to different upstreams.
//! * **Shape Plugins**: Plugins that transform request and response shapes.
//! * **Observability Plugins**: Plugins that monitor and log request metrics.
//!
//! Signer plugins are **not** supported in the 0.1.x release series.
//!
//! # Quick-Start Example
//!
//! Here is a complete example of how to write conformance tests for your plugin.
//! This example assumes you have compiled your plugin to a WebAssembly binary and
//! made it available to your tests.
//!
//! ```rust,ignore
//! use cc_lb_plugin_conformance::{handshake, self_check, dispatch::PluginSession, fixtures};
//!
//! const WASM: &[u8] = include_bytes!(env!("CARGO_CDYLIB_FILE_MY_CC_LB_PLUGIN"));
//!
//! #[test]
//! fn handshake_negotiates_shape_v1() {
//!     let report = handshake::run(WASM).unwrap();
//!     assert_eq!(report.chosen_versions["shape"], 1);
//!     assert_eq!(report.identity.name, "my-plugin");
//! }
//!
//! #[test]
//! fn self_check_clean() {
//!     let report = self_check::run(WASM).unwrap();
//!     assert!(matches!(report.status, self_check::SelfCheckStatus::Success));
//!     assert!(report.failures.is_empty());
//! }
//!
//! #[test]
//! fn shape_round_trip_basic() {
//!     use cc_lb_plugin_conformance::prelude::*;
//!     let mut session = PluginSession::new(WASM).unwrap();
//!     let req = fixtures::shape_request_builder()
//!         .method("POST")
//!         .path("/v1/messages")
//!         .body_base64("eyJ0ZXN0IjoxfQ==")
//!         .build();
//!     let outcome = session.dispatch::<cc_lb_plugin_wire::wire_function::ShapeFn>(req).unwrap();
//!     let cc_lb_plugin_conformance::dispatch::DispatchOutcome::Ok(resp) = outcome else {
//!         panic!("shape returned Fallback");
//!     };
//!     assert_eq!(resp.url, "https://api.anthropic.com/v1/messages");
//! }
//! ```
//!
//! # SemVer Policy
//!
//! This crate follows the same 0.1.x to 0.2.x cadence as the other plugin-author SDK crates.
//! All public structs and enums are marked `#[non_exhaustive]` to allow future additions.
//! Error `reason` string fields are diagnostics and are not part of the stable SemVer contract.
//! Callers should match on enum variants instead of asserting on exact error text.
//!
//! # Compile-Cost Note
//!
//! This crate depends on the Extism and Wasmtime runtimes.
//! Transitive compilation of these dependencies can take about 5 to 10 minutes for a cold build.
//!
//! # CI Smoke Note
//!
//! The CI smoke tests for this crate are Linux-only.

#![forbid(unsafe_code)]

#[cfg(feature = "dispatch")]
pub mod dispatch;
mod errors;
pub mod fixtures;
pub mod handshake;
pub mod identity;
pub mod prelude;
pub mod self_check;
#[cfg(feature = "dispatch")]
pub mod verify;

pub use errors::{ExtraInfo, LayerResult, VerifyError, VerifyReport};
#[cfg(feature = "dispatch")]
pub use verify::{
    verify_observability_plugin, verify_observability_plugin_with_caps, verify_router_plugin,
    verify_router_plugin_with_caps, verify_shape_plugin, verify_shape_plugin_with_caps,
};
