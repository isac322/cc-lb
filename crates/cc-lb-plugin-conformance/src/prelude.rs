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
    TransformRealtimeFrameWireRequest, TransformResponseWireRequest, TransformSseWireRequest,
    fixtures::{
        hdr, sample_filter_request, sample_filter_request_v3,
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
        sample_shape_request_for, sample_shape_request_v2,
        sample_transform_realtime_frame_request_v2, sample_transform_response_request,
        sample_transform_response_request_for, sample_transform_response_request_v2,
        sample_transform_sse_event_request, sample_transform_sse_event_request_for,
        sample_transform_sse_event_request_v2, synth_principal,
    },
};
pub use cc_lb_plugin_wire::{
    ProviderFamily, WireVersion,
    v2::{
        FilterResponse, FilterResponse as FilterResponseV2, Header, Header as HeaderV2,
        PerCandidateReason, Principal, Principal as PrincipalV2, ShapeEnvelopeGeminiRequest,
        ShapeEnvelopeGeminiResponse, ShapeRequest, ShapeRequest as ShapeRequestV2, ShapeResponse,
        ShapeResponse as ShapeResponseV2, SseEvent, SseEvent as SseEventV2,
        TransformRealtimeFrameRequest,
        TransformRealtimeFrameRequest as TransformRealtimeFrameRequestV2,
        TransformRealtimeFrameResult,
        TransformRealtimeFrameResult as TransformRealtimeFrameResultV2, TransformResponseRequest,
        TransformResponseRequest as TransformResponseRequestV2, TransformResponseResult,
        TransformResponseResult as TransformResponseResultV2, TransformSseEventRequest,
        TransformSseEventRequest as TransformSseEventRequestV2, TransformSseEventResult,
        TransformSseEventResult as TransformSseEventResultV2, Upstream, Upstream as UpstreamV2,
        UpstreamCandidate as UpstreamCandidateV2, UpstreamIdentity,
    },
    v3::{
        FilterRequest, FilterRequest as FilterRequestV3, NativeCachePricing,
        NativeCachePricing as NativeCachePricingV3, NativeCachePricingComponent,
        NativeCachePricingComponent as NativeCachePricingComponentV3, NativeCachePricingLifetime,
        NativeCachePricingLifetime as NativeCachePricingLifetimeV3, NativeCachePricingProvider,
        NativeCachePricingProvider as NativeCachePricingProviderV3, NativeCachePricingRate,
        NativeCachePricingRate as NativeCachePricingRateV3, NativeCachePricingStatus,
        NativeCachePricingStatus as NativeCachePricingStatusV3, UpstreamCandidate,
    },
};
pub use cc_lb_runtime_wasmtime::{HookKind, HotEngineConfig};
