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
    ConformanceSuite, FilterWireRequest, PluginSession, ShapeWireRequest,
    TransformResponseWireRequest, TransformSseWireRequest,
    fixtures::{
        hdr, sample_filter_request, sample_filter_request_v2,
        sample_gemini_generate_content_shape_request,
        sample_gemini_generate_content_transform_response_request,
        sample_gemini_generate_content_transform_sse_event_request,
        sample_gemini_shape_envelope_request, sample_gemini_stream_generate_content_shape_request,
        sample_gemini_stream_generate_content_transform_response_request,
        sample_gemini_stream_generate_content_transform_sse_event_request,
        sample_openai_chat_completions_shape_request,
        sample_openai_chat_completions_transform_response_request,
        sample_openai_chat_completions_transform_sse_event_request,
        sample_openai_responses_shape_request, sample_openai_responses_transform_response_request,
        sample_openai_responses_transform_sse_event_request, sample_shape_request,
        sample_shape_request_for, sample_shape_request_v2, sample_transform_response_request,
        sample_transform_response_request_for, sample_transform_response_request_v2,
        sample_transform_sse_event_request, sample_transform_sse_event_request_for,
        sample_transform_sse_event_request_v2, synth_principal,
    },
};
pub use cc_lb_plugin_wire::{
    FilterRequest, FilterResponse, Header, PerCandidateReason, Principal, ProviderFamily,
    ShapeRequest, ShapeResponse, SseEvent, TransformResponseRequest, TransformResponseResult,
    TransformSseEventRequest, TransformSseEventResult, Upstream, UpstreamCandidate, WireVersion,
    v2::{
        FilterRequest as FilterRequestV2, FilterResponse as FilterResponseV2, Header as HeaderV2,
        Principal as PrincipalV2, ShapeEnvelopeGeminiRequest, ShapeEnvelopeGeminiResponse,
        ShapeRequest as ShapeRequestV2, ShapeResponse as ShapeResponseV2, SseEvent as SseEventV2,
        TransformResponseRequest as TransformResponseRequestV2,
        TransformResponseResult as TransformResponseResultV2,
        TransformSseEventRequest as TransformSseEventRequestV2,
        TransformSseEventResult as TransformSseEventResultV2, Upstream as UpstreamV2,
        UpstreamCandidate as UpstreamCandidateV2, UpstreamIdentity,
    },
};
pub use cc_lb_runtime_wasmtime::{HookKind, HotEngineConfig};
