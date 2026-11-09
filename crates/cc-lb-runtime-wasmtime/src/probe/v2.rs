use std::sync::Arc;

use cc_lb_plugin_wire::schema::{HookKind, ProviderFamily};
use cc_lb_plugin_wire::v2::*;
use rkyv::rancor::Error as RkyvError;

use crate::cache::call_hook_export_scoped;
use crate::cell::PluginCell;
use crate::error::WasmtimeRuntimeError;

use super::{copy_aligned, probe_failed, sample_body, sample_model, sample_path};

macro_rules! probe {
    ($name:ident, $sample:ident, $hook:ident, $response:ty) => {
        pub(super) fn $name(
            cell: &Arc<PluginCell>,
            provider: ProviderFamily,
            export: &str,
        ) -> Result<(), WasmtimeRuntimeError> {
            let input = rkyv::to_bytes::<RkyvError>(&$sample(provider)).map_err(|error| {
                probe_failed(HookKind::$hook, format!("encode V2 request: {error}"))
            })?;
            let output = call_hook_export_scoped(
                cell,
                input.as_slice(),
                HookKind::$hook,
                export,
                copy_aligned,
            )
            .map_err(|error| probe_failed(HookKind::$hook, error.to_string()))?;
            rkyv::access::<$response, RkyvError>(&output).map_err(|error| {
                probe_failed(HookKind::$hook, format!("decode V2 response: {error}"))
            })?;
            Ok(())
        }
    };
}

probe!(probe_shape, sample_shape, Shape, ArchivedShapeResponse);
probe!(
    probe_response,
    sample_response,
    TransformResponse,
    ArchivedTransformResponseResult
);
probe!(
    probe_sse,
    sample_sse,
    TransformSseEvent,
    ArchivedTransformSseEventResult
);
probe!(
    probe_frame,
    sample_frame,
    TransformRealtimeFrame,
    ArchivedTransformRealtimeFrameResult
);
probe!(
    probe_envelope,
    sample_envelope,
    ShapeEnvelopeGemini,
    ArchivedShapeEnvelopeGeminiResponse
);

fn principal() -> Principal {
    Principal {
        id: Box::from("probe-principal"),
        kind: Box::from("api_key"),
        claims: Box::new([]),
    }
}

fn header(content_type: &str) -> Header {
    Header {
        name: Box::from("content-type"),
        value: Box::from(content_type.as_bytes()),
    }
}

fn upstream(provider: ProviderFamily) -> Upstream {
    let base_url = Some(Box::from("https://example.test"));
    match provider {
        ProviderFamily::Anthropic => Upstream::AnthropicDirect { base_url },
        ProviderFamily::Openai => Upstream::OpenaiDirect { base_url },
        ProviderFamily::Gemini => Upstream::GeminiDirect { base_url },
    }
}

fn identity(provider: ProviderFamily) -> UpstreamIdentity {
    UpstreamIdentity {
        provider: Box::from(provider.as_str()),
        credential_kind: Box::from("api_key"),
        upstream_id: Box::from("probe-upstream"),
        name: Box::from("Probe upstream"),
        base_url: Some(Box::from("https://example.test")),
        gemini_oauth_profile: None,
        code_assist_project_id: None,
    }
}

fn sample_shape(provider: ProviderFamily) -> ShapeRequest {
    ShapeRequest {
        request_id: Box::from("probe-req-1"),
        method: Box::from("POST"),
        path: Box::from(sample_path(provider)),
        query: None,
        headers: Box::new([header("application/json")]),
        body: Box::from(sample_body(provider)),
        principal: principal(),
        upstream: upstream(provider),
        upstream_identity: identity(provider),
    }
}

fn sample_response(provider: ProviderFamily) -> TransformResponseRequest {
    TransformResponseRequest {
        request_id: Box::from("probe-req-1"),
        principal: principal(),
        upstream: upstream(provider),
        upstream_identity: identity(provider),
        request_method: Box::from("POST"),
        request_path: Box::from(sample_path(provider)),
        canonical_model_id: Box::from(sample_model(provider)),
        response_status: 200,
        response_headers: Box::new([header("application/json")]),
        body: Box::from(match provider {
            ProviderFamily::Anthropic => &br#"{"content":[]}"#[..],
            ProviderFamily::Openai => &br#"{"id":"resp_probe","output":[]}"#[..],
            ProviderFamily::Gemini => &br#"{"candidates":[]}"#[..],
        }),
    }
}

fn sample_sse(provider: ProviderFamily) -> TransformSseEventRequest {
    TransformSseEventRequest {
        request_id: Box::from("probe-req-1"),
        principal: principal(),
        upstream: upstream(provider),
        upstream_identity: identity(provider),
        request_method: Box::from("POST"),
        request_path: Box::from(sample_path(provider)),
        canonical_model_id: Box::from(sample_model(provider)),
        response_status: 200,
        response_headers: Box::new([header("text/event-stream")]),
        event: SseEvent {
            event: Box::from(match provider {
                ProviderFamily::Anthropic => "content_block_start",
                ProviderFamily::Openai => "response.created",
                ProviderFamily::Gemini => "",
            }),
            data: Box::from(match provider {
                ProviderFamily::Anthropic => &br#"{"type":"content_block_start"}"#[..],
                ProviderFamily::Openai => {
                    &br#"{"type":"response.created","response":{"id":"resp_probe","output":[]}}"#[..]
                }
                ProviderFamily::Gemini => &br#"{"candidates":[]}"#[..],
            }),
        },
    }
}
fn sample_frame(provider: ProviderFamily) -> TransformRealtimeFrameRequest {
    TransformRealtimeFrameRequest {
        request_id: Box::from("probe-req-1"),
        principal: principal(),
        upstream: upstream(provider),
        upstream_identity: identity(provider),
        request_method: Box::from("GET"),
        request_path: Box::from(match provider {
            ProviderFamily::Openai => "/v1/realtime",
            ProviderFamily::Gemini => "/v1beta.BidiGenerateContent",
            ProviderFamily::Anthropic => "/v1/realtime",
        }),
        canonical_model_id: Box::from(sample_model(provider)),
        direction: Box::from("provider_to_client"),
        kind: Box::from("json"),
        payload: Box::from(&br#"{"type":"probe"}"#[..]),
    }
}

fn sample_envelope(_provider: ProviderFamily) -> ShapeEnvelopeGeminiRequest {
    let mut upstream = identity(ProviderFamily::Gemini);
    upstream.credential_kind = Box::from("oauth");
    upstream.gemini_oauth_profile = Some(Box::from("gemini_cli"));
    upstream.code_assist_project_id = Some(Box::from("probe-project"));
    ShapeEnvelopeGeminiRequest {
        envelope_body: Box::from(&br#"{"model":"gemini-2.5-flash","project":"probe-project","request":{"contents":[{"role":"user","parts":[{"text":"probe"}]}]}}"#[..]),
        url: Box::from("https://cloudcode-pa.googleapis.com/v1internal:generateContent"),
        headers: Box::new([header("application/json")]),
        upstream,
    }
}
