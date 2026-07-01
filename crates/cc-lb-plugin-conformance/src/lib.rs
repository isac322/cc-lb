//! In-process conformance harness for cc-lb wasmtime plugins.
//!
//! Plugin authors add this crate as a dev-dependency and get a
//! production-equivalent host loader + round-trip harness with no
//! per-plugin boilerplate. The harness owns the admission invariants,
//! so when cc-lb tightens an invariant every plugin re-runs the new
//! rule on rebuild.
//!
//! # Usage
//!
//! ```ignore
//! use cc_lb_plugin_conformance::ConformanceSuite;
//!
//! fn wasm() -> Vec<u8> {
//!     std::fs::read(
//!         std::env::var("CC_LB_PLUGIN_WASM").unwrap_or_else(|_| {
//!             concat!(env!("CARGO_MANIFEST_DIR"),
//!                 "/target/wasm32-unknown-unknown/release/my_plugin.wasm").into()
//!         }),
//!     ).expect("build plugin wasm first")
//! }
//!
//! #[test]
//! fn conformance() {
//!     ConformanceSuite::for_shape(&wasm())
//!         .with_plugin_name("my-plugin")
//!         .run();
//! }
//! ```
//!
//! For custom semantic tests, drop down to `call_shape` /
//! `call_normalize_error` / `call_filter` / `call_observe` directly
//! and assert on the decoded response.

#![deny(unsafe_code)]

use cc_lb_plugin_api::SlotKey;
use cc_lb_plugin_types::{
    ArchivedFilterResponse, ArchivedNormalizeErrorResponse, ArchivedShapeResponse, FilterRequest,
    FilterResponse, NormalizeErrorRequest, NormalizeErrorResponse, ObserveEvent, ShapeRequest,
    ShapeResponse,
};
use cc_lb_runtime_wasmtime::{HotEngineConfig, SlotKind, WasmtimeRuntime, inspect_wasm};
use rkyv::rancor::Error as RkyvError;
use rkyv::util::AlignedVec;

/// Fluent builder that owns the compiled wasm + the resource budget the
/// admission and round-trip helpers use.
///
/// [`Self::run`] fires the full built-in suite; individual `call_*` and
/// `assert_*` methods let plugin authors compose their own tests on top.
pub struct ConformanceSuite<'a> {
    wasm: &'a [u8],
    kind: SlotKind,
    plugin_name: String,
    engine_config: HotEngineConfig,
}

impl<'a> ConformanceSuite<'a> {
    /// Build a suite for a Filter-slot plugin.
    pub fn for_filter(wasm: &'a [u8]) -> Self {
        Self::with_kind(wasm, SlotKind::Filter)
    }

    /// Build a suite for a Shape-slot plugin.
    ///
    /// Shape plugins export both `cc_lb_shape` and `cc_lb_normalize_error`
    /// per RFC-0001; both are exercised by [`Self::run`].
    pub fn for_shape(wasm: &'a [u8]) -> Self {
        Self::with_kind(wasm, SlotKind::Shape)
    }

    /// Build a suite for an Observe-slot plugin.
    pub fn for_observe(wasm: &'a [u8]) -> Self {
        Self::with_kind(wasm, SlotKind::Observe)
    }

    fn with_kind(wasm: &'a [u8], kind: SlotKind) -> Self {
        let label = match kind {
            SlotKind::Filter => "filter",
            SlotKind::Shape => "shape",
            SlotKind::Observe => "observe",
        };
        Self {
            wasm,
            kind,
            plugin_name: format!("conformance-{label}"),
            engine_config: conformance_engine_config(),
        }
    }

    /// Override the plugin name the suite uses when registering the slot.
    /// Purely cosmetic — affects log lines only, not admission.
    pub fn with_plugin_name(mut self, name: impl Into<String>) -> Self {
        self.plugin_name = name.into();
        self
    }

    /// Override the [`HotEngineConfig`] used by the suite. Defaults to
    /// [`conformance_engine_config`] which mirrors production cc-lb
    /// defaults (1024 pages / 1B fuel / 1 MiB stack).
    pub fn with_engine_config(mut self, cfg: HotEngineConfig) -> Self {
        self.engine_config = cfg;
        self
    }

    /// Static admission: no host imports, required exports present,
    /// per-hook `cc_lb.schema.*.v1` custom section BLAKE3 matches the
    /// expected wire tag. Uses [`SlotKind::required_schemas`] so new
    /// admission invariants added to cc-lb propagate here on bump.
    pub fn assert_static_admission(&self) {
        let inspection = inspect_wasm(self.kind, self.wasm)
            .unwrap_or_else(|e| panic!("inspect_wasm rejected plugin: {e}"));

        for (section_name, wire_tag) in self.kind.required_schemas() {
            let hash = inspection
                .schema_hashes
                .iter()
                .find(|(name, _)| name == section_name)
                .map(|(_, hash)| hash)
                .unwrap_or_else(|| panic!("missing schema section {section_name}"));
            let want = blake3::hash(wire_tag);
            assert_eq!(
                hash,
                want.as_bytes(),
                "schema hash mismatch for {section_name}: guest emitted {} but host expects {}",
                hex(hash),
                hex(want.as_bytes())
            );
        }
    }

    /// Register the plugin in a fresh [`WasmtimeRuntime`] and return the
    /// runtime + slot key so callers can drive further `call_*` traffic.
    pub fn register(&self) -> (WasmtimeRuntime, SlotKey) {
        let runtime = WasmtimeRuntime::new(self.engine_config.clone())
            .expect("wasmtime engine build must succeed");
        let slot_key = SlotKey::global(self.plugin_name.clone());
        match self.kind {
            SlotKind::Filter => runtime
                .register_filter(slot_key.clone(), self.plugin_name.clone(), self.wasm)
                .map(|_| ())
                .expect("register_filter must accept a conforming plugin"),
            SlotKind::Shape => runtime
                .register_shape(slot_key.clone(), self.plugin_name.clone(), self.wasm)
                .map(|_| ())
                .expect("register_shape must accept a conforming plugin"),
            SlotKind::Observe => runtime
                .register_observe(slot_key.clone(), self.plugin_name.clone(), self.wasm)
                .map(|_| ())
                .expect("register_observe must accept a conforming plugin"),
        }
        (runtime, slot_key)
    }

    /// Round-trip a [`FilterRequest`] through the guest boundary.
    /// Panics if the slot kind is not Filter.
    pub fn call_filter(&self, request: FilterRequest) -> FilterResponse {
        assert!(
            matches!(self.kind, SlotKind::Filter),
            "call_filter requires SlotKind::Filter, got {:?}",
            self.kind
        );
        let (runtime, slot_key) = self.register();
        let in_bytes = rkyv::to_bytes::<RkyvError>(&request).expect("rkyv encode FilterRequest");
        let out_bytes = runtime
            .call_filter(&slot_key, in_bytes.as_slice())
            .expect("guest cc_lb_filter must complete without trap");
        let mut aligned = AlignedVec::<16>::with_capacity(out_bytes.len());
        aligned.extend_from_slice(&out_bytes);
        let archived = rkyv::access::<ArchivedFilterResponse, RkyvError>(&aligned)
            .expect("rkyv access FilterResponse");
        rkyv::deserialize::<FilterResponse, RkyvError>(archived)
            .expect("rkyv deserialize FilterResponse")
    }

    /// Round-trip a [`ShapeRequest`] through the guest boundary.
    /// Panics if the slot kind is not Shape.
    pub fn call_shape(&self, request: ShapeRequest) -> ShapeResponse {
        assert!(
            matches!(self.kind, SlotKind::Shape),
            "call_shape requires SlotKind::Shape, got {:?}",
            self.kind
        );
        let (runtime, slot_key) = self.register();
        let in_bytes = rkyv::to_bytes::<RkyvError>(&request).expect("rkyv encode ShapeRequest");
        let out_bytes = runtime
            .call_shape(&slot_key, in_bytes.as_slice())
            .expect("guest cc_lb_shape must complete without trap");
        let mut aligned = AlignedVec::<16>::with_capacity(out_bytes.len());
        aligned.extend_from_slice(&out_bytes);
        let archived = rkyv::access::<ArchivedShapeResponse, RkyvError>(&aligned)
            .expect("rkyv access ShapeResponse");
        rkyv::deserialize::<ShapeResponse, RkyvError>(archived)
            .expect("rkyv deserialize ShapeResponse")
    }

    /// Round-trip a [`NormalizeErrorRequest`] through the guest boundary.
    /// Only valid for Shape kind. Returns `None` when the plugin returns
    /// an empty normalized body (pass-through convention).
    pub fn call_normalize_error(
        &self,
        request: NormalizeErrorRequest,
    ) -> Option<NormalizeErrorResponse> {
        assert!(
            matches!(self.kind, SlotKind::Shape),
            "call_normalize_error requires SlotKind::Shape, got {:?}",
            self.kind
        );
        let (runtime, slot_key) = self.register();
        let in_bytes =
            rkyv::to_bytes::<RkyvError>(&request).expect("rkyv encode NormalizeErrorRequest");
        let out_bytes = runtime
            .call_normalize_error(&slot_key, in_bytes.as_slice())
            .expect("guest cc_lb_normalize_error must complete without trap");
        let mut aligned = AlignedVec::<16>::with_capacity(out_bytes.len());
        aligned.extend_from_slice(&out_bytes);
        let archived = rkyv::access::<ArchivedNormalizeErrorResponse, RkyvError>(&aligned)
            .expect("rkyv access NormalizeErrorResponse");
        let response = rkyv::deserialize::<NormalizeErrorResponse, RkyvError>(archived)
            .expect("rkyv deserialize NormalizeErrorResponse");
        if response.normalized.is_none() {
            None
        } else {
            Some(response)
        }
    }

    /// Send an [`ObserveEvent`] through the guest boundary. Observe
    /// hooks are side-effect-only; the return type is `()`. Panics if
    /// the slot kind is not Observe.
    pub fn call_observe(&self, event: ObserveEvent) {
        assert!(
            matches!(self.kind, SlotKind::Observe),
            "call_observe requires SlotKind::Observe, got {:?}",
            self.kind
        );
        let (runtime, slot_key) = self.register();
        let in_bytes = rkyv::to_bytes::<RkyvError>(&event).expect("rkyv encode ObserveEvent");
        runtime
            .call_observe(&slot_key, in_bytes.as_slice())
            .expect("guest cc_lb_observe must complete without trap");
    }

    /// Run the full built-in conformance suite. Currently:
    /// - [`Self::assert_static_admission`]
    /// - Load + register via a fresh [`WasmtimeRuntime`] so plugin
    ///   instantiation errors surface here rather than at production
    ///   admit time.
    pub fn run(&self) {
        self.assert_static_admission();
        let _ = self.register();
    }
}

/// Opinionated [`HotEngineConfig`] used by the conformance suite by
/// default. Values match cc-lb's production `HotEngineConfig::default()`
/// so tests exercise the same resource envelope real traffic sees, but
/// stay pinned here regardless of future host default changes.
pub fn conformance_engine_config() -> HotEngineConfig {
    HotEngineConfig {
        memory_max_pages: 1024,
        fuel_per_call: 1_000_000_000,
        max_wasm_stack: 1024 * 1024,
        pool_total_memories: 64,
        pool_total_core_instances: 64,
    }
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(out, "{b:02x}");
    }
    out
}
