use std::sync::Arc;

use cc_lb_plugin_api::{Principal, PrincipalKind, Upstream};
use cc_lb_plugin_wire::metadata::HookMode;
use cc_lb_plugin_wire::schema::{HookKind, WireVersion};
use cc_lb_plugin_wire::{
    ArchivedTransformResponseResult, ArchivedTransformSseEventResult, ClaimRef, HeaderRef,
    PrincipalRef, QueryRef, SseEvent, SseEventRef, TransformResponseRequestRef,
    TransformSseEventRequestRef, UpstreamRef,
};
use cc_lb_upstream::{
    ResponseTransformError, ResponseTransformHook, SseEventTransformHook, TransformResponseRequest,
    TransformResponseResult, TransformSseEventRequest, TransformSseEventResult,
};
use rkyv::rancor::Error as RkyvError;
use rkyv::util::AlignedVec;

use crate::cache::{call_transform_response_hook, call_transform_sse_event_hook};
use crate::cell::PluginCell;
use crate::error::WasmtimeRuntimeError;

pub struct WasmtimeResponseTransformHook {
    cell: Arc<PluginCell>,
    runtime_config: Arc<crate::HotEngineConfig>,
    wire_version: Option<WireVersion>,
    mode: HookMode,
}

impl WasmtimeResponseTransformHook {
    pub(crate) fn from_cell(
        cell: Arc<PluginCell>,
        runtime_config: Arc<crate::HotEngineConfig>,
    ) -> Self {
        let metadata = cell
            .metadata
            .hooks
            .get(HookKind::TransformResponse.as_str());
        let wire_version =
            metadata.and_then(|metadata| WireVersion::from_u8(metadata.wire_version));
        let mode = metadata.map_or(HookMode::Active, |metadata| metadata.mode);
        Self {
            cell,
            runtime_config,
            wire_version,
            mode,
        }
    }
}

impl ResponseTransformHook for WasmtimeResponseTransformHook {
    fn transform_response(
        &self,
        request: TransformResponseRequest,
    ) -> Result<TransformResponseResult, ResponseTransformError> {
        if self.mode.is_noop() {
            return Ok(TransformResponseResult::Unchanged);
        }
        let in_bytes = host_to_wire_transform_response(&request).map_err(|e| {
            response_runtime_error(format!("rkyv encode TransformResponseRequest: {e}"))
        })?;
        let out_bytes = match self.wire_version {
            Some(WireVersion::V1) => call_transform_response_hook(&self.cell, in_bytes.as_slice()),
            None => {
                return Err(response_runtime_error(
                    "shape plugin is missing transform_response metadata".to_owned(),
                ));
            }
        }
        .map_err(runtime_error_to_response_transform)?;
        reject_oversized_output(
            out_bytes.as_slice(),
            self.runtime_config.wire_bounds.output_body_bytes,
        )?;
        wire_to_host_transform_response(&out_bytes)
    }
}

pub struct WasmtimeSseEventTransformHook {
    cell: Arc<PluginCell>,
    runtime_config: Arc<crate::HotEngineConfig>,
    wire_version: Option<WireVersion>,
    mode: HookMode,
}

impl WasmtimeSseEventTransformHook {
    pub(crate) fn from_cell(
        cell: Arc<PluginCell>,
        runtime_config: Arc<crate::HotEngineConfig>,
    ) -> Self {
        let metadata = cell
            .metadata
            .hooks
            .get(HookKind::TransformSseEvent.as_str());
        let wire_version =
            metadata.and_then(|metadata| WireVersion::from_u8(metadata.wire_version));
        let mode = metadata.map_or(HookMode::Active, |metadata| metadata.mode);
        Self {
            cell,
            runtime_config,
            wire_version,
            mode,
        }
    }
}

impl SseEventTransformHook for WasmtimeSseEventTransformHook {
    fn transform_sse_event(
        &self,
        request: TransformSseEventRequest,
    ) -> Result<TransformSseEventResult, ResponseTransformError> {
        if self.mode.is_noop() {
            return Ok(TransformSseEventResult::Unchanged);
        }
        let in_bytes = host_to_wire_transform_sse_event(&request).map_err(|e| {
            response_runtime_error(format!("rkyv encode TransformSseEventRequest: {e}"))
        })?;
        let out_bytes = match self.wire_version {
            Some(WireVersion::V1) => call_transform_sse_event_hook(&self.cell, in_bytes.as_slice()),
            None => {
                return Err(response_runtime_error(
                    "shape plugin is missing transform_sse_event metadata".to_owned(),
                ));
            }
        }
        .map_err(runtime_error_to_response_transform)?;
        reject_oversized_output(
            out_bytes.as_slice(),
            self.runtime_config.wire_bounds.output_body_bytes,
        )?;
        wire_to_host_transform_sse_event(&out_bytes)
    }
}

fn host_to_wire_transform_response(
    request: &TransformResponseRequest,
) -> Result<AlignedVec<16>, RkyvError> {
    let principal_kind = principal_kind_to_wire(&request.principal);
    let claim_bufs = claim_buffers(&request.principal);
    let claim_refs = claim_refs(&claim_bufs);
    let header_refs = header_refs(&request.response_headers);
    let upstream_base_url = upstream_base_url(&request.upstream);
    let upstream = UpstreamRef::AnthropicDirect {
        base_url: upstream_base_url.as_deref().map(|value| QueryRef { value }),
    };
    let wire = TransformResponseRequestRef {
        request_id: request.request_id.as_str(),
        principal: PrincipalRef {
            id: request.principal.id.as_str(),
            kind: principal_kind,
            claims: &claim_refs,
        },
        upstream,
        request_method: request.request_method.as_str(),
        request_path: request.request_path.as_str(),
        canonical_model_id: request.canonical_model_id.as_str(),
        response_status: request.response_status.as_u16(),
        response_headers: &header_refs,
        body: request.body.as_ref(),
    };
    rkyv::to_bytes::<RkyvError>(&wire)
}

fn host_to_wire_transform_sse_event(
    request: &TransformSseEventRequest,
) -> Result<AlignedVec<16>, RkyvError> {
    let principal_kind = principal_kind_to_wire(&request.principal);
    let claim_bufs = claim_buffers(&request.principal);
    let claim_refs = claim_refs(&claim_bufs);
    let header_refs = header_refs(&request.response_headers);
    let upstream_base_url = upstream_base_url(&request.upstream);
    let upstream = UpstreamRef::AnthropicDirect {
        base_url: upstream_base_url.as_deref().map(|value| QueryRef { value }),
    };
    let event = SseEventRef {
        event: request.event.event.as_str(),
        data: request.event.data.as_ref(),
    };
    let wire = TransformSseEventRequestRef {
        request_id: request.request_id.as_str(),
        principal: PrincipalRef {
            id: request.principal.id.as_str(),
            kind: principal_kind,
            claims: &claim_refs,
        },
        upstream,
        request_method: request.request_method.as_str(),
        request_path: request.request_path.as_str(),
        canonical_model_id: request.canonical_model_id.as_str(),
        response_status: request.response_status.as_u16(),
        response_headers: &header_refs,
        event,
    };
    rkyv::to_bytes::<RkyvError>(&wire)
}

fn wire_to_host_transform_response(
    bytes: &AlignedVec<16>,
) -> Result<TransformResponseResult, ResponseTransformError> {
    let archived = rkyv::access::<ArchivedTransformResponseResult, RkyvError>(bytes)
        .map_err(|e| response_runtime_error(format!("rkyv access TransformResponseResult: {e}")))?;
    if let ArchivedTransformResponseResult::Unchanged = archived {
        return Ok(TransformResponseResult::Unchanged);
    }
    let result: cc_lb_plugin_wire::TransformResponseResult =
        rkyv::deserialize::<cc_lb_plugin_wire::TransformResponseResult, RkyvError>(archived)
            .map_err(|e| {
                response_runtime_error(format!("rkyv deserialize TransformResponseResult: {e}"))
            })?;
    match result {
        cc_lb_plugin_wire::TransformResponseResult::Unchanged => {
            Ok(TransformResponseResult::Unchanged)
        }
        cc_lb_plugin_wire::TransformResponseResult::Replace {
            status,
            headers,
            body,
        } => Ok(TransformResponseResult::Replace {
            status: status.map(status_code_from_u16).transpose()?,
            headers: headers.map(headers_from_wire).transpose()?,
            body: body.map(bytes::Bytes::from),
        }),
    }
}

fn wire_to_host_transform_sse_event(
    bytes: &AlignedVec<16>,
) -> Result<TransformSseEventResult, ResponseTransformError> {
    let archived = rkyv::access::<ArchivedTransformSseEventResult, RkyvError>(bytes)
        .map_err(|e| response_runtime_error(format!("rkyv access TransformSseEventResult: {e}")))?;
    match archived {
        ArchivedTransformSseEventResult::Unchanged => {
            return Ok(TransformSseEventResult::Unchanged);
        }
        ArchivedTransformSseEventResult::Drop => return Ok(TransformSseEventResult::Drop),
        ArchivedTransformSseEventResult::Replace { .. } => {}
    }
    let result: cc_lb_plugin_wire::TransformSseEventResult =
        rkyv::deserialize::<cc_lb_plugin_wire::TransformSseEventResult, RkyvError>(archived)
            .map_err(|e| {
                response_runtime_error(format!("rkyv deserialize TransformSseEventResult: {e}"))
            })?;
    match result {
        cc_lb_plugin_wire::TransformSseEventResult::Unchanged => {
            Ok(TransformSseEventResult::Unchanged)
        }
        cc_lb_plugin_wire::TransformSseEventResult::Replace { events } => {
            Ok(TransformSseEventResult::Replace {
                events: events
                    .into_vec()
                    .into_iter()
                    .map(sse_event_from_wire)
                    .collect(),
            })
        }
        cc_lb_plugin_wire::TransformSseEventResult::Drop => Ok(TransformSseEventResult::Drop),
    }
}

fn reject_oversized_output(bytes: &[u8], bound: u64) -> Result<(), ResponseTransformError> {
    if bytes.len() as u64 > bound {
        return Err(response_runtime_error(format!(
            "response transform output {} bytes exceeds wire_bounds.output_body_bytes ({bound})",
            bytes.len()
        )));
    }
    Ok(())
}

fn claim_buffers(principal: &Principal) -> Vec<(&str, Vec<u8>)> {
    principal
        .claims
        .iter()
        .filter_map(|(key, value)| {
            serde_json::to_vec(value)
                .ok()
                .map(|bytes| (key.as_str(), bytes))
        })
        .collect()
}

fn claim_refs<'a>(claim_bufs: &'a [(&'a str, Vec<u8>)]) -> Vec<ClaimRef<'a>> {
    claim_bufs
        .iter()
        .map(|(key, value)| ClaimRef {
            key,
            value: value.as_slice(),
        })
        .collect()
}

fn header_refs(headers: &http::HeaderMap) -> Vec<HeaderRef<'_>> {
    headers
        .iter()
        .map(|(name, value)| HeaderRef {
            name: name.as_str(),
            value: value.as_bytes(),
        })
        .collect()
}

fn headers_from_wire(
    headers: Box<[cc_lb_plugin_wire::Header]>,
) -> Result<http::HeaderMap, ResponseTransformError> {
    let mut out = http::HeaderMap::new();
    for header in headers {
        let name = http::HeaderName::from_bytes(header.name.as_bytes()).map_err(|e| {
            response_runtime_error(format!(
                "plugin returned invalid header name `{}`: {e}",
                header.name
            ))
        })?;
        let value = http::HeaderValue::from_bytes(&header.value).map_err(|e| {
            response_runtime_error(format!(
                "plugin returned invalid header value for `{}`: {e}",
                header.name
            ))
        })?;
        out.append(name, value);
    }
    Ok(out)
}

fn principal_kind_to_wire(principal: &Principal) -> &'static str {
    match principal.kind {
        PrincipalKind::ApiKey => "api_key",
        PrincipalKind::OAuthSubject => "oauth_subject",
        PrincipalKind::InternalKey => "internal_key",
        PrincipalKind::WorkloadIdentity => "workload_identity",
        PrincipalKind::SubscriptionBearer => "subscription_bearer",
    }
}

fn upstream_base_url(upstream: &Upstream) -> Option<String> {
    match upstream {
        Upstream::AnthropicDirect { base_url } => base_url.as_ref().map(ToString::to_string),
    }
}

fn status_code_from_u16(status: u16) -> Result<http::StatusCode, ResponseTransformError> {
    http::StatusCode::from_u16(status).map_err(|e| {
        response_runtime_error(format!("plugin returned invalid status `{status}`: {e}"))
    })
}

fn sse_event_from_wire(event: SseEvent) -> cc_lb_upstream::SseEvent {
    cc_lb_upstream::SseEvent {
        event: event.event.into_string(),
        data: bytes::Bytes::from(event.data.into_vec()),
    }
}

fn runtime_error_to_response_transform(err: WasmtimeRuntimeError) -> ResponseTransformError {
    match err {
        WasmtimeRuntimeError::GuestTrap { phase, source } => ResponseTransformError::Trap {
            reason: format!("{phase}: {source}"),
        },
        other => response_runtime_error(other.to_string()),
    }
}

fn response_runtime_error(reason: String) -> ResponseTransformError {
    ResponseTransformError::Runtime { reason }
}
