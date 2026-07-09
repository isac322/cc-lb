//! In-process conformance harness for cc-lb wasmtime plugins.
//!
//! Plugin authors add this crate as a dev-dependency and get a
//! production-equivalent host loader + round-trip harness with no
//! per-plugin boilerplate.
//!
//! # Design
//!
//! Two layers.
//!
//! [`ConformanceSuite`] is an immutable builder that pins the wasm bytes,
//! slot kind, plugin name, and engine budget. Its [`ConformanceSuite::run`]
//! and [`ConformanceSuite::session`] are the entry points.
//!
//! [`PluginSession`] wraps a live [`WasmtimeRuntime`] with the plugin
//! already registered against a fresh slot. Every `call_*` on the session
//! reuses that registration — no per-call rebuild — so plugin authors can
//! write dozens of semantic tests without a linear runtime-init cost.
//!
//! The suite does not duplicate host admission checks: `register_*`
//! internally runs [`cc_lb_runtime_wasmtime::inspect_wasm`], so
//! successfully constructing a [`PluginSession`] IS the static admission
//! proof. Plugin-specific semantic assertions (scrub correctness, URL
//! policy, header filtering, billing header presence) stay in the
//! plugin's own test suite — the harness does not know what the plugin
//! is supposed to do, only how the boundary works.
//!
//! # Usage
//!
//! ```ignore
//! use cc_lb_plugin_conformance::prelude::*;
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
//! fn admits_and_registers() {
//!     ConformanceSuite::for_shape(&wasm())
//!         .with_plugin_name("my-plugin")
//!         .run();
//! }
//!
//! #[test]
//! fn shape_semantics() {
//!     let session = ConformanceSuite::for_shape(&wasm()).session();
//!     let response = session.call_shape(ShapeRequest {
//!         request_id: "t-1".into(),
//!         method: "POST".into(),
//!         path: "/v1/messages".into(),
//!         query: None,
//!         headers: vec![hdr("content-type", "application/json")],
//!         body: br#"{"model":"claude-3"}"#.to_vec(),
//!         principal: synth_principal(),
//!         upstream: Upstream::AnthropicDirect { base_url: None },
//!     });
//!     assert_eq!(response.method, "POST");
//! }
//! ```

#![deny(unsafe_code)]

pub mod fixtures;
pub mod prelude;

use cc_lb_plugin_api::SlotKey;
use cc_lb_plugin_wire::{
    ArchivedFilterResponse, ArchivedShapeResponse, FilterRequest, FilterResponse, ObserveEvent,
    ShapeRequest, ShapeResponse,
};
use cc_lb_runtime_wasmtime::{
    HotEngineConfig, ModuleInspection, SlotKind, WasmtimeRuntime, inspect_wasm,
};
use rkyv::rancor::Error as RkyvError;
use rkyv::util::AlignedVec;

use crate::fixtures::observe_event_samples;

/// Fluent builder pinning the wasm bytes + slot kind + engine budget.
///
/// [`Self::run`] fires the minimal registration smoke. For real work,
/// call [`Self::session`] and drive the returned [`PluginSession`].
pub struct ConformanceSuite<'a> {
    wasm: &'a [u8],
    kind: ConformanceKind,
    plugin_name: String,
    engine_config: HotEngineConfig,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ConformanceKind {
    Filter,
    Shape,
    Observe,
}

impl ConformanceKind {
    const ALL: [Self; 3] = [Self::Filter, Self::Shape, Self::Observe];

    fn slot_kind(self) -> SlotKind {
        match self {
            Self::Filter => SlotKind::Filter,
            Self::Shape => SlotKind::Shape,
            Self::Observe => SlotKind::Observe,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Filter => "filter",
            Self::Shape => "shape",
            Self::Observe => "observe",
        }
    }
}

impl<'a> ConformanceSuite<'a> {
    /// Build a suite for a Filter-slot plugin.
    pub fn for_filter(wasm: &'a [u8]) -> Self {
        Self::with_kind(wasm, ConformanceKind::Filter)
    }

    /// Build a suite for a Shape-slot plugin.
    pub fn for_shape(wasm: &'a [u8]) -> Self {
        Self::with_kind(wasm, ConformanceKind::Shape)
    }

    /// Build a suite for an Observe-slot plugin.
    pub fn for_observe(wasm: &'a [u8]) -> Self {
        Self::with_kind(wasm, ConformanceKind::Observe)
    }

    pub fn from_wasm(wasm: &'a [u8]) -> Self {
        let mut matches = ConformanceKind::ALL
            .into_iter()
            .filter(|kind| inspect_wasm(kind.slot_kind(), wasm).is_ok());
        let Some(kind) = matches.next() else {
            panic!("inspect_wasm did not recognise a supported plugin export")
        };
        assert!(
            matches.next().is_none(),
            "wasm exports multiple hooks; use the explicit for_* constructor"
        );
        Self::with_kind(wasm, kind)
    }

    fn with_kind(wasm: &'a [u8], kind: ConformanceKind) -> Self {
        let label = kind.label();
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
    /// defaults (1024 pages / 1 MiB stack).
    pub fn with_engine_config(mut self, cfg: HotEngineConfig) -> Self {
        self.engine_config = cfg;
        self
    }

    /// Optional standalone admission gate — parses the wasm through
    /// [`inspect_wasm`] and panics on rejection. NOT called by
    /// [`Self::run`] because [`Self::session`] already invokes
    /// `inspect_wasm` transitively via `register_*`. Kept public so
    /// authors can assert admission WITHOUT paying for full engine
    /// setup (fast pre-flight in a `build.rs`, etc.).
    pub fn assert_static_admission(&self) {
        inspect_wasm(self.kind.slot_kind(), self.wasm)
            .unwrap_or_else(|e| panic!("inspect_wasm rejected plugin: {e}"));
    }

    pub fn inspect(&self) -> ModuleInspection {
        inspect_wasm(self.kind.slot_kind(), self.wasm)
            .unwrap_or_else(|e| panic!("inspect_wasm rejected plugin: {e}"))
    }

    pub fn assert_recognisable_by_current_host(&self) {
        let runtime = WasmtimeRuntime::new(self.engine_config.clone())
            .expect("wasmtime engine build must succeed");
        runtime
            .admit_wasm(self.kind.slot_kind(), self.wasm)
            .unwrap_or_else(|e| panic!("admit_wasm rejected plugin: {e}"));
    }

    /// Build a fresh [`WasmtimeRuntime`], register the plugin under
    /// [`Self::plugin_name`], and return the live session.
    ///
    /// A successful `session()` return IS the static admission proof —
    /// `register_*` invokes `inspect_wasm` before instantiate.
    pub fn session(&self) -> PluginSession {
        let runtime = WasmtimeRuntime::new(self.engine_config.clone())
            .expect("wasmtime engine build must succeed");
        let slot_key = SlotKey::global(self.plugin_name.clone());
        match self.kind {
            ConformanceKind::Filter => runtime
                .register_filter(slot_key.clone(), self.plugin_name.clone(), self.wasm)
                .map(|_| ())
                .expect("register_filter must accept a conforming plugin"),
            ConformanceKind::Shape => runtime
                .register_shape(slot_key.clone(), self.plugin_name.clone(), self.wasm)
                .map(|_| ())
                .expect("register_shape must accept a conforming plugin"),
            ConformanceKind::Observe => runtime
                .register_observe(slot_key.clone(), self.plugin_name.clone(), self.wasm)
                .map(|_| ())
                .expect("register_observe must accept a conforming plugin"),
        }
        PluginSession {
            runtime,
            slot_key,
            kind: self.kind,
        }
    }

    /// Built-in conformance smoke: build a session (covers static
    /// admission via `inspect_wasm` + wasm compile + `InstancePre`),
    /// then push one minimal-valid payload through every hook the slot
    /// exports. A plugin whose `cc_lb_shape` traps or returns
    /// un-decodable bytes cannot pass this — registration alone would
    /// have missed it.
    ///
    /// Coverage:
    /// - `Filter` → `call_filter(sample_filter_request())`
    /// - `Shape` → `call_shape(sample_shape_request())`
    /// - `Observe` → [`PluginSession::exercise_observe_variants`]
    ///
    /// Assertions are boundary-only: hooks must not trap and responses
    /// (where hooks return one) must rkyv-decode as the expected type.
    /// Plugin-specific semantics (URL policy, header filtering, scrub
    /// correctness) are the plugin author's responsibility to test.
    pub fn run(&self) {
        let session = self.session();
        match self.kind {
            ConformanceKind::Filter => {
                let _ = session.call_filter(fixtures::sample_filter_request());
            }
            ConformanceKind::Shape => {
                let _ = session.call_shape(fixtures::sample_shape_request());
            }
            ConformanceKind::Observe => {
                session.exercise_observe_variants();
            }
        }
    }
}

/// A live plugin session — the runtime holds the compiled module and
/// the slot is registered. Every `call_*` reuses this registration, so
/// N-request tests pay one instantiate cost instead of N.
pub struct PluginSession {
    runtime: WasmtimeRuntime,
    slot_key: SlotKey,
    kind: ConformanceKind,
}

impl PluginSession {
    /// The slot kind this session was built for.
    pub fn kind(&self) -> SlotKind {
        self.kind.slot_kind()
    }

    /// The `SlotKey` the plugin is registered under.
    pub fn slot_key(&self) -> &SlotKey {
        &self.slot_key
    }

    /// Underlying [`WasmtimeRuntime`] — escape hatch for advanced
    /// tests that want to poke at hot-swap, pure vs stateful mode,
    /// version bumps, etc.
    pub fn runtime(&self) -> &WasmtimeRuntime {
        &self.runtime
    }

    /// Round-trip a [`FilterRequest`] through the guest boundary.
    /// Panics if this session is not Filter-kind.
    pub fn call_filter(&self, request: FilterRequest) -> FilterResponse {
        assert!(
            matches!(self.kind, ConformanceKind::Filter),
            "call_filter requires SlotKind::Filter, got {:?}",
            self.kind.slot_kind()
        );
        let in_bytes = rkyv::to_bytes::<RkyvError>(&request).expect("rkyv encode FilterRequest");
        let out_bytes = self
            .runtime
            .call_filter(&self.slot_key, in_bytes.as_slice())
            .expect("guest cc_lb_filter must complete without trap");
        let mut aligned = AlignedVec::<16>::with_capacity(out_bytes.len());
        aligned.extend_from_slice(&out_bytes);
        let archived = rkyv::access::<ArchivedFilterResponse, RkyvError>(&aligned)
            .expect("rkyv access FilterResponse");
        rkyv::deserialize::<FilterResponse, RkyvError>(archived)
            .expect("rkyv deserialize FilterResponse")
    }

    /// Round-trip a [`ShapeRequest`] through the guest boundary.
    /// Panics if this session is not Shape-kind.
    pub fn call_shape(&self, request: ShapeRequest) -> ShapeResponse {
        assert!(
            matches!(self.kind, ConformanceKind::Shape),
            "call_shape requires SlotKind::Shape, got {:?}",
            self.kind.slot_kind()
        );
        let in_bytes = rkyv::to_bytes::<RkyvError>(&request).expect("rkyv encode ShapeRequest");
        let out_bytes = self
            .runtime
            .call_shape(&self.slot_key, in_bytes.as_slice())
            .expect("guest cc_lb_shape must complete without trap");
        let mut aligned = AlignedVec::<16>::with_capacity(out_bytes.len());
        aligned.extend_from_slice(&out_bytes);
        let archived = rkyv::access::<ArchivedShapeResponse, RkyvError>(&aligned)
            .expect("rkyv access ShapeResponse");
        rkyv::deserialize::<ShapeResponse, RkyvError>(archived)
            .expect("rkyv deserialize ShapeResponse")
    }

    /// Send an [`ObserveEvent`] through the guest boundary. Observe
    /// hooks are side-effect-only; return type is `()`. Panics if this
    /// session is not Observe-kind.
    pub fn call_observe(&self, event: ObserveEvent) {
        assert!(
            matches!(self.kind, ConformanceKind::Observe),
            "call_observe requires SlotKind::Observe, got {:?}",
            self.kind.slot_kind()
        );
        let in_bytes = rkyv::to_bytes::<RkyvError>(&event).expect("rkyv encode ObserveEvent");
        self.runtime
            .call_observe(&self.slot_key, in_bytes.as_slice())
            .expect("guest cc_lb_observe must complete without trap");
    }

    /// Send one of every [`ObserveEvent`] variant, using
    /// [`observe_event_samples`], and assert no variant traps. Cheap
    /// coverage insurance for observe plugins because a missing match
    /// arm in the guest would only surface when production emits that
    /// specific event kind. Panics if this session is not Observe-kind.
    pub fn exercise_observe_variants(&self) {
        for event in observe_event_samples() {
            self.call_observe(event);
        }
    }
}

/// Opinionated [`HotEngineConfig`] used by the conformance suite by
/// default. Values match cc-lb's production `HotEngineConfig::default()`
/// so tests exercise the same resource envelope real traffic sees, but
/// stay pinned here regardless of future host default changes.
pub fn conformance_engine_config() -> HotEngineConfig {
    HotEngineConfig {
        memory_max_pages: 1024,
        max_wasm_stack: 1024 * 1024,
        pool_total_memories: 64,
        pool_total_core_instances: 64,
        ..HotEngineConfig::default()
    }
}
