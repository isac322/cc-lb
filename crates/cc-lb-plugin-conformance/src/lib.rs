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
#[cfg(test)]
mod tests;

use cc_lb_plugin_wire::{
    schema::{ProviderFamily, WireVersion},
    v1, v2,
};
use cc_lb_runtime_wasmtime::{
    HookKind, HotEngineConfig, ModuleInspection, RuntimeSlotKey, WasmPluginWireDispatch,
    WasmtimeRuntime, admit_wasm, inspect_wasm,
};
use rkyv::rancor::Error as RkyvError;
use rkyv::util::AlignedVec;

/// A request that can be encoded for a selected filter wire version.
pub trait FilterWireRequest {
    type Response;
    const VERSION: WireVersion;
    fn encode(&self) -> AlignedVec<16>;
    fn decode(bytes: &[u8]) -> Self::Response;
}

/// A request that can be encoded for a selected shape wire version.
pub trait ShapeWireRequest {
    type Response;
    const VERSION: WireVersion;
    fn encode(&self) -> AlignedVec<16>;
    fn decode(bytes: &[u8]) -> Self::Response;
}

/// A request that can be encoded for a selected buffered-response wire version.
pub trait TransformResponseWireRequest {
    type Response;
    const VERSION: WireVersion;
    fn encode(&self) -> AlignedVec<16>;
    fn decode(bytes: &[u8]) -> Self::Response;
}

/// A request that can be encoded for a selected SSE-event wire version.
pub trait TransformSseWireRequest {
    type Response;
    const VERSION: WireVersion;
    fn encode(&self) -> AlignedVec<16>;
    fn decode(bytes: &[u8]) -> Self::Response;
}

macro_rules! impl_wire_request {
    ($trait_name:ident, $request:path, $response:path, $archived:path, $version:expr) => {
        impl $trait_name for $request {
            type Response = $response;
            const VERSION: WireVersion = $version;

            fn encode(&self) -> AlignedVec<16> {
                rkyv::to_bytes::<RkyvError>(self).expect("rkyv encode conformance request")
            }

            fn decode(bytes: &[u8]) -> Self::Response {
                let mut aligned = AlignedVec::<16>::with_capacity(bytes.len());
                aligned.extend_from_slice(bytes);
                let archived = rkyv::access::<$archived, RkyvError>(&aligned)
                    .expect("rkyv access conformance response");
                rkyv::deserialize::<$response, RkyvError>(archived)
                    .expect("rkyv deserialize conformance response")
            }
        }
    };
}

impl_wire_request!(
    FilterWireRequest,
    v1::FilterRequest,
    v1::FilterResponse,
    v1::ArchivedFilterResponse,
    WireVersion::V1
);
impl_wire_request!(
    FilterWireRequest,
    v2::FilterRequest,
    v2::FilterResponse,
    v2::ArchivedFilterResponse,
    WireVersion::V2
);
impl_wire_request!(
    ShapeWireRequest,
    v1::ShapeRequest,
    v1::ShapeResponse,
    v1::ArchivedShapeResponse,
    WireVersion::V1
);
impl_wire_request!(
    ShapeWireRequest,
    v2::ShapeRequest,
    v2::ShapeResponse,
    v2::ArchivedShapeResponse,
    WireVersion::V2
);
impl_wire_request!(
    TransformResponseWireRequest,
    v1::TransformResponseRequest,
    v1::TransformResponseResult,
    v1::ArchivedTransformResponseResult,
    WireVersion::V1
);
impl_wire_request!(
    TransformResponseWireRequest,
    v2::TransformResponseRequest,
    v2::TransformResponseResult,
    v2::ArchivedTransformResponseResult,
    WireVersion::V2
);
impl_wire_request!(
    TransformSseWireRequest,
    v1::TransformSseEventRequest,
    v1::TransformSseEventResult,
    v1::ArchivedTransformSseEventResult,
    WireVersion::V1
);
impl_wire_request!(
    TransformSseWireRequest,
    v2::TransformSseEventRequest,
    v2::TransformSseEventResult,
    v2::ArchivedTransformSseEventResult,
    WireVersion::V2
);

/// Fluent builder pinning the wasm bytes + slot kind + engine budget.
///
/// [`Self::run`] fires the minimal registration smoke. For real work,
/// call [`Self::session`] and drive the returned [`PluginSession`].
pub struct ConformanceSuite<'a> {
    wasm: &'a [u8],
    kind: ConformanceKind,
    provider: ProviderFamily,
    plugin_name: String,
    engine_config: HotEngineConfig,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ConformanceKind {
    Filter,
    Shape,
}

impl ConformanceKind {
    const ALL: [Self; 2] = [Self::Filter, Self::Shape];

    fn slot_kind(self) -> HookKind {
        match self {
            Self::Filter => HookKind::Filter,
            Self::Shape => HookKind::Shape,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Filter => "filter",
            Self::Shape => "shape",
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

    /// Build a Filter-slot suite for a selected provider.
    pub fn for_filter_provider(wasm: &'a [u8], provider: ProviderFamily) -> Self {
        Self::for_filter(wasm).with_provider(provider)
    }

    /// Build a Shape-slot suite for a selected provider.
    pub fn for_shape_provider(wasm: &'a [u8], provider: ProviderFamily) -> Self {
        Self::for_shape(wasm).with_provider(provider)
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
            provider: ProviderFamily::Anthropic,
            plugin_name: format!("conformance-{label}"),
            engine_config: HotEngineConfig::default(),
        }
    }

    /// Select the provider-qualified exports and fixture family.
    pub fn with_provider(mut self, provider: ProviderFamily) -> Self {
        self.provider = provider;
        self
    }

    /// The provider selected by this suite.
    pub fn provider(&self) -> ProviderFamily {
        self.provider
    }

    /// Override the plugin name the suite uses when registering the slot.
    /// Purely cosmetic — affects log lines only, not admission.
    pub fn with_plugin_name(mut self, name: impl Into<String>) -> Self {
        self.plugin_name = name.into();
        self
    }

    /// Override the [`HotEngineConfig`] used by the suite. Defaults to
    /// [`HotEngineConfig::default()`], the production cc-lb envelope.
    pub fn with_engine_config(mut self, cfg: HotEngineConfig) -> Self {
        self.engine_config = cfg;
        self
    }

    pub fn inspect(&self) -> ModuleInspection {
        inspect_wasm(self.kind.slot_kind(), self.wasm)
            .unwrap_or_else(|e| panic!("inspect_wasm rejected plugin: {e}"))
    }

    pub fn assert_recognisable_by_current_host(&self) {
        let runtime = WasmtimeRuntime::new(self.engine_config.clone())
            .expect("wasmtime engine build must succeed");
        admit_wasm(
            runtime.engine(),
            runtime.linker(),
            self.kind.slot_kind(),
            self.wasm,
            runtime.config(),
        )
        .unwrap_or_else(|e| panic!("admit_wasm rejected plugin: {e}"));
    }

    /// Build a fresh [`WasmtimeRuntime`], register the plugin under the
    /// configured plugin name, and return the live session.
    ///
    /// A successful `session()` return IS the static admission proof —
    /// `register_*` invokes `inspect_wasm` before instantiate.
    pub fn session(&self) -> PluginSession {
        let runtime = WasmtimeRuntime::new(self.engine_config.clone())
            .expect("wasmtime engine build must succeed");
        let slot_key = RuntimeSlotKey::global(self.plugin_name.clone());
        let slot = match self.kind {
            ConformanceKind::Filter => runtime
                .register_filter(slot_key.clone(), self.plugin_name.clone(), self.wasm)
                .expect("register_filter must accept a conforming plugin"),
            ConformanceKind::Shape => runtime
                .register_shape(slot_key.clone(), self.plugin_name.clone(), self.wasm)
                .expect("register_shape must accept a conforming plugin"),
        };
        let dispatch = WasmPluginWireDispatch::from_slot(slot, runtime.config_arc())
            .for_provider(self.provider)
            .unwrap_or_else(|| {
                panic!(
                    "plugin does not export a usable {:?} hook for {:?}",
                    self.kind.slot_kind(),
                    self.provider
                )
            });
        PluginSession {
            runtime,
            slot_key,
            kind: self.kind,
            provider: self.provider,
            dispatch,
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
    /// - `Filter` → canonical V1 request
    /// - `Shape` → `call_shape(sample_shape_request())`
    ///
    /// Assertions are boundary-only: hooks must not trap and responses
    /// (where hooks return one) must rkyv-decode as the expected type.
    /// Plugin-specific semantics (URL policy, header filtering, scrub
    /// correctness) are the plugin author's responsibility to test.
    pub fn run(&self) {
        let session = self.session();
        match self.kind {
            ConformanceKind::Filter => match session.dispatch.filter_wire_version() {
                Some(WireVersion::V1) => {
                    let _ = session.call_filter(fixtures::sample_filter_request());
                }
                Some(WireVersion::V2) => {
                    let _ = session.call_filter(fixtures::sample_filter_request_v2(self.provider));
                }
                None => unreachable!(),
            },
            ConformanceKind::Shape => {
                if let Some(version) = session.dispatch.shape_wire_version() {
                    match version {
                        WireVersion::V1 => {
                            let _ = session
                                .call_shape(fixtures::sample_shape_request_for(self.provider));
                        }
                        WireVersion::V2 => {
                            let _ = session
                                .call_shape(fixtures::sample_shape_request_v2(self.provider));
                        }
                    }
                }
                if session.dispatch.has_response_transform() {
                    match session.dispatch.response_wire_version() {
                        Some(WireVersion::V1) => {
                            let _ = session.call_transform_response(
                                fixtures::sample_transform_response_request_for(self.provider),
                            );
                        }
                        Some(WireVersion::V2) => {
                            let _ = session.call_transform_response(
                                fixtures::sample_transform_response_request_v2(self.provider),
                            );
                        }
                        None => unreachable!(),
                    }
                }
                if session.dispatch.has_sse_event_transform() {
                    match session.dispatch.sse_wire_version() {
                        Some(WireVersion::V1) => {
                            let _ = session.call_transform_sse_event(
                                fixtures::sample_transform_sse_event_request_for(self.provider),
                            );
                        }
                        Some(WireVersion::V2) => {
                            let _ = session.call_transform_sse_event(
                                fixtures::sample_transform_sse_event_request_v2(self.provider),
                            );
                        }
                        None => unreachable!(),
                    }
                }
                if self.provider == ProviderFamily::Gemini
                    && session.dispatch.has_shape_envelope_gemini()
                {
                    let _ = session.call_shape_envelope_gemini(
                        fixtures::sample_gemini_shape_envelope_request(),
                    );
                }
            }
        }
    }
}

/// A live plugin session — the runtime holds the compiled module and
/// the slot is registered. Every `call_*` reuses this registration, so
/// N-request tests pay one instantiate cost instead of N.
pub struct PluginSession {
    runtime: WasmtimeRuntime,
    slot_key: RuntimeSlotKey,
    kind: ConformanceKind,
    provider: ProviderFamily,
    dispatch: WasmPluginWireDispatch,
}

impl PluginSession {
    /// The slot kind this session was built for.
    pub fn kind(&self) -> HookKind {
        self.kind.slot_kind()
    }

    /// The [`RuntimeSlotKey`] the plugin is registered under.
    pub fn slot_key(&self) -> &RuntimeSlotKey {
        &self.slot_key
    }

    /// Underlying [`WasmtimeRuntime`] — escape hatch for advanced
    /// tests that want to poke at hot-swap or slot eviction.
    pub fn runtime(&self) -> &WasmtimeRuntime {
        &self.runtime
    }

    /// Provider selected for this session.
    pub fn provider(&self) -> ProviderFamily {
        self.provider
    }

    /// Whether the optional Gemini envelope export is present and active.
    pub fn has_shape_envelope_gemini(&self) -> bool {
        self.dispatch.has_shape_envelope_gemini()
    }

    /// Whether a buffered response transform is present and active.
    pub fn has_response_transform(&self) -> bool {
        self.dispatch.has_response_transform()
    }

    /// Whether an SSE transform is present and active.
    pub fn has_sse_event_transform(&self) -> bool {
        self.dispatch.has_sse_event_transform()
    }

    /// Round-trip a filter request using the selected hook's declared wire version.
    pub fn call_filter<R>(&self, request: R) -> R::Response
    where
        R: FilterWireRequest,
    {
        assert!(
            matches!(self.kind, ConformanceKind::Filter),
            "call_filter requires HookKind::Filter, got {:?}",
            self.kind.slot_kind()
        );
        assert_eq!(
            self.dispatch.filter_wire_version(),
            Some(R::VERSION),
            "filter fixture wire version does not match the plugin declaration"
        );
        let in_bytes = request.encode();
        self.dispatch
            .call_filter_scoped(&in_bytes, R::decode)
            .expect("guest filter hook must complete without trap")
    }

    /// Round-trip a shape request using the selected hook's declared wire version.
    pub fn call_shape<R>(&self, request: R) -> R::Response
    where
        R: ShapeWireRequest,
    {
        assert!(
            matches!(self.kind, ConformanceKind::Shape),
            "call_shape requires HookKind::Shape, got {:?}",
            self.kind.slot_kind()
        );
        assert_eq!(
            self.dispatch.shape_wire_version(),
            Some(R::VERSION),
            "shape fixture wire version does not match the plugin declaration"
        );
        let in_bytes = request.encode();
        self.dispatch
            .call_shape_scoped(&in_bytes, R::decode)
            .expect("guest shape hook must complete without trap")
    }

    /// Round-trip a buffered response request using V1 or V2 as declared by the hook.
    pub fn call_transform_response<R>(&self, request: R) -> R::Response
    where
        R: TransformResponseWireRequest,
    {
        assert!(
            matches!(self.kind, ConformanceKind::Shape),
            "call_transform_response requires a Shape-kind session"
        );
        assert_eq!(
            self.dispatch.response_wire_version(),
            Some(R::VERSION),
            "response fixture wire version does not match the plugin declaration"
        );
        let in_bytes = request.encode();
        self.dispatch
            .call_transform_response_scoped(&in_bytes, R::decode)
            .expect("guest response hook must complete without trap")
    }

    /// Round-trip an SSE event request using V1 or V2 as declared by the hook.
    pub fn call_transform_sse_event<R>(&self, request: R) -> R::Response
    where
        R: TransformSseWireRequest,
    {
        assert!(
            matches!(self.kind, ConformanceKind::Shape),
            "call_transform_sse_event requires a Shape-kind session"
        );
        assert_eq!(
            self.dispatch.sse_wire_version(),
            Some(R::VERSION),
            "SSE fixture wire version does not match the plugin declaration"
        );
        let in_bytes = request.encode();
        self.dispatch
            .call_transform_sse_event_scoped(&in_bytes, R::decode)
            .expect("guest SSE hook must complete without trap")
    }

    /// Round-trip the optional Gemini envelope hook.
    pub fn call_shape_envelope_gemini(
        &self,
        request: v2::ShapeEnvelopeGeminiRequest,
    ) -> v2::ShapeEnvelopeGeminiResponse {
        assert_eq!(
            self.provider,
            ProviderFamily::Gemini,
            "Gemini envelope calls require a Gemini-selected session"
        );
        assert_eq!(
            self.dispatch.envelope_wire_version(),
            Some(WireVersion::V2),
            "Gemini envelope must declare wire V2"
        );
        let in_bytes =
            rkyv::to_bytes::<RkyvError>(&request).expect("rkyv encode ShapeEnvelopeGeminiRequest");
        let out_bytes = self
            .dispatch
            .call_shape_envelope_gemini_scoped(&in_bytes, |bytes| {
                let mut aligned = AlignedVec::<16>::with_capacity(bytes.len());
                aligned.extend_from_slice(bytes);
                let archived =
                    rkyv::access::<v2::ArchivedShapeEnvelopeGeminiResponse, RkyvError>(&aligned)
                        .expect("rkyv access ShapeEnvelopeGeminiResponse");
                rkyv::deserialize::<v2::ShapeEnvelopeGeminiResponse, RkyvError>(archived)
                    .expect("rkyv deserialize ShapeEnvelopeGeminiResponse")
            })
            .expect("guest Gemini envelope hook must complete without trap");
        out_bytes
    }
}
