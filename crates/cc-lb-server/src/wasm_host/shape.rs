use std::sync::Arc;

use cc_lb_domain::{Principal, Upstream};
use cc_lb_plugin_wire::metadata::HookMode;
use cc_lb_plugin_wire::schema::WireVersion;
use cc_lb_plugin_wire::{
    ArchivedTransformResponseResult, ArchivedTransformSseEventResult, ClaimRef, HeaderRef,
    PrincipalRef, QueryRef, SseEvent, SseEventRef, TransformResponseRequestRef,
    TransformSseEventRequestRef, UpstreamRef,
};
use cc_lb_runtime_wasmtime::{WasmPluginWireDispatch, WasmtimeRuntimeError};
use cc_lb_upstream::{
    DialectError, DialectShapeContext, ResponseTransformError, ResponseTransformHook,
    ShapedRequest, ShapedRequestBuilder, SseEventTransformHook, TransformResponseRequest,
    TransformResponseResult, TransformSseEventRequest, TransformSseEventResult, UpstreamDialect,
};
use rkyv::rancor::Error as RkyvError;
use rkyv::util::AlignedVec;

use super::{
    access_archived_scoped_or_copy,
    filter_request::{is_stripped_downstream_header, principal_kind_to_wire},
    serialize_with_input_scratch,
};

pub struct WasmtimeUpstreamDialect {
    dispatch: Arc<WasmPluginWireDispatch>,
    response_transform: Option<WasmtimeResponseTransformHook>,
    sse_event_transform: Option<WasmtimeSseEventTransformHook>,
}

impl WasmtimeUpstreamDialect {
    pub fn new(dispatch: Arc<WasmPluginWireDispatch>) -> Self {
        let response_transform = if dispatch.has_response_transform() {
            Some(WasmtimeResponseTransformHook {
                dispatch: Arc::clone(&dispatch),
                wire_version: dispatch.shape_wire_version(),
                mode: HookMode::Active,
            })
        } else {
            None
        };
        let sse_event_transform = if dispatch.has_sse_event_transform() {
            Some(WasmtimeSseEventTransformHook {
                dispatch: Arc::clone(&dispatch),
                wire_version: dispatch.shape_wire_version(),
                mode: HookMode::Active,
            })
        } else {
            None
        };
        Self {
            dispatch,
            response_transform,
            sse_event_transform,
        }
    }
}

impl UpstreamDialect for WasmtimeUpstreamDialect {
    fn shape(
        &self,
        context: &DialectShapeContext,
        upstream: &Upstream,
        principal: &Principal,
        builder: &mut ShapedRequestBuilder,
    ) -> Result<ShapedRequest, DialectError> {
        let wire_version = self.dispatch.shape_wire_version();
        match wire_version {
            Some(WireVersion::V1) => {}
            Some(WireVersion::V2) => {
                return Err(DialectError::UnsupportedRequest {
                    reason: "shape hook does not support wire V2".to_owned(),
                });
            }
            None => {
                return Err(DialectError::UnsupportedRequest {
                    reason: "plugin metadata missing shape hook".to_owned(),
                });
            }
        }

        let guest_result = with_wire_shape_request(
            context,
            upstream,
            principal,
            self.dispatch.cookie_redaction(),
            |in_bytes| {
                self.dispatch.call_shape_scoped(in_bytes, |guest_bytes| {
                    let out_bound = self.dispatch.wire_bounds().output_body_bytes;
                    if guest_bytes.len() as u64 > out_bound {
                        return Err(DialectError::UnsupportedRequest {
                            reason: format!(
                                "shape output {} bytes exceeds wire_bounds.output_body_bytes ({out_bound})",
                                guest_bytes.len(),
                            ),
                        });
                    }
                    access_archived_scoped_or_copy::<
                        cc_lb_plugin_wire::ArchivedShapeResponse,
                        _,
                    >(guest_bytes, |archived| {
                        wire_to_host_shaped_request(
                            builder,
                            archived,
                            upstream,
                            self.dispatch.shape_origin_policy(),
                            self.dispatch.wire_bounds(),
                        )
                    })
                    .map_err(|e| DialectError::UnsupportedRequest {
                        reason: format!("rkyv access ShapeResponse: {e}"),
                    })
                    .and_then(std::convert::identity)
                })
            },
        )
        .map_err(|e| DialectError::UnsupportedRequest {
            reason: format!("rkyv encode ShapeRequest: {e}"),
        })?;
        guest_result.map_err(runtime_error_to_dialect)?
    }

    fn response_transform_hook(&self) -> Option<&dyn ResponseTransformHook> {
        self.response_transform
            .as_ref()
            .map(|h| h as &dyn ResponseTransformHook)
    }

    fn sse_event_transform_hook(&self) -> Option<&dyn SseEventTransformHook> {
        self.sse_event_transform
            .as_ref()
            .map(|h| h as &dyn SseEventTransformHook)
    }
}

struct WasmtimeResponseTransformHook {
    dispatch: Arc<WasmPluginWireDispatch>,
    wire_version: Option<WireVersion>,
    mode: HookMode,
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
            Some(WireVersion::V1) => self.dispatch.call_transform_response(in_bytes.as_slice()),
            Some(WireVersion::V2) => {
                return Err(response_runtime_error(
                    "transform_response hook does not support wire V2".to_owned(),
                ));
            }
            None => {
                return Err(response_runtime_error(
                    "shape plugin is missing transform_response metadata".to_owned(),
                ));
            }
        }
        .map_err(runtime_error_to_response_transform)?;
        reject_oversized_output(
            out_bytes.as_slice(),
            self.dispatch.wire_bounds().output_body_bytes,
        )?;
        wire_to_host_transform_response(&out_bytes)
    }
}

struct WasmtimeSseEventTransformHook {
    dispatch: Arc<WasmPluginWireDispatch>,
    wire_version: Option<WireVersion>,
    mode: HookMode,
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
            Some(WireVersion::V1) => self.dispatch.call_transform_sse_event(in_bytes.as_slice()),
            Some(WireVersion::V2) => {
                return Err(response_runtime_error(
                    "transform_sse_event hook does not support wire V2".to_owned(),
                ));
            }
            None => {
                return Err(response_runtime_error(
                    "shape plugin is missing transform_sse_event metadata".to_owned(),
                ));
            }
        }
        .map_err(runtime_error_to_response_transform)?;
        reject_oversized_output(
            out_bytes.as_slice(),
            self.dispatch.wire_bounds().output_body_bytes,
        )?;
        wire_to_host_transform_sse_event(&out_bytes)
    }
}

pub(super) fn host_upstream_to_wire(upstream: &Upstream) -> cc_lb_plugin_wire::Upstream {
    match upstream {
        Upstream::AnthropicDirect { base_url } => cc_lb_plugin_wire::Upstream::AnthropicDirect {
            base_url: base_url.as_ref().map(|u| u.to_string().into_boxed_str()),
        },
    }
}

fn with_wire_shape_request<R>(
    context: &DialectShapeContext,
    upstream: &Upstream,
    principal: &Principal,
    cookie_redaction: bool,
    with_bytes: impl for<'a> FnOnce(&'a [u8]) -> R,
) -> Result<R, RkyvError> {
    let principal_kind_str = principal_kind_to_wire(principal);
    let claim_bufs: Vec<(&str, Vec<u8>)> = principal
        .claims
        .iter()
        .filter_map(|(k, v)| serde_json::to_vec(v).ok().map(|bytes| (k.as_str(), bytes)))
        .collect();
    let claim_refs: Vec<ClaimRef<'_>> = claim_bufs
        .iter()
        .map(|(k, v)| ClaimRef {
            key: k,
            value: v.as_slice(),
        })
        .collect();
    let header_refs: Vec<HeaderRef<'_>> = context
        .downstream_headers
        .iter()
        .filter(|(name, _)| !is_stripped_downstream_header(name.as_str(), cookie_redaction))
        .map(|(name, value)| HeaderRef {
            name: name.as_str(),
            value: value.as_bytes(),
        })
        .collect();
    let base_url_str = match upstream {
        Upstream::AnthropicDirect { base_url } => base_url.as_ref().map(|u| u.to_string()),
    };
    let upstream_ref = UpstreamRef::AnthropicDirect {
        base_url: base_url_str.as_deref().map(|s| QueryRef { value: s }),
    };
    let query_ref = context.query.as_deref().map(|s| QueryRef { value: s });
    let request = cc_lb_plugin_wire::ShapeRequestRef {
        request_id: context.request_id.as_str(),
        method: context.method.as_str(),
        path: context.path.as_str(),
        query: query_ref,
        headers: &header_refs,
        body: context.body_bytes.as_ref(),
        principal: PrincipalRef {
            id: principal.id.as_str(),
            kind: principal_kind_str,
            claims: &claim_refs,
        },
        upstream: upstream_ref,
    };
    serialize_with_input_scratch(&request, with_bytes)
}

fn upstream_base_url(upstream: &Upstream) -> Option<url::Url> {
    match upstream {
        Upstream::AnthropicDirect { base_url } => base_url.clone(),
    }
}

fn is_stripped_shape_output_header(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    matches!(
        lower.as_str(),
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
            | "host"
            | "content-length"
            | "authorization"
            | "x-api-key"
    ) || lower.starts_with("x-anthropic-")
}

fn wire_to_host_shaped_request(
    builder: &mut ShapedRequestBuilder,
    archived: &cc_lb_plugin_wire::ArchivedShapeResponse,
    upstream: &Upstream,
    origin_policy: cc_lb_runtime_wasmtime::policy::ShapeOriginPolicy,
    wire_bounds: &cc_lb_runtime_wasmtime::policy::PluginWireBounds,
) -> Result<ShapedRequest, DialectError> {
    if archived.headers.len() as u32 > wire_bounds.max_headers {
        return Err(DialectError::UnsupportedRequest {
            reason: format!(
                "shape plugin returned {} headers, exceeds wire_bounds.max_headers ({})",
                archived.headers.len(),
                wire_bounds.max_headers,
            ),
        });
    }

    let url_str: &str = &archived.url;
    let url = url::Url::parse(url_str)?;

    if matches!(
        origin_policy,
        cc_lb_runtime_wasmtime::policy::ShapeOriginPolicy::SelectedUpstreamOrigin
    ) && let Some(expected) = upstream_base_url(upstream)
    {
        let expected_origin = expected.origin();
        let actual_origin = url.origin();
        if expected_origin != actual_origin {
            return Err(DialectError::UnsupportedRequest {
                reason: format!(
                    "shape plugin returned URL origin `{}` but selected upstream requires `{}`",
                    actual_origin.ascii_serialization(),
                    expected_origin.ascii_serialization(),
                ),
            });
        }
    }

    let method_str: &str = &archived.method;
    let method = http::Method::from_bytes(method_str.as_bytes()).map_err(|e| {
        DialectError::UnsupportedRequest {
            reason: format!("plugin returned invalid method `{method_str}`: {e}"),
        }
    })?;

    let mut headers = http::HeaderMap::new();
    for h in archived.headers.iter() {
        let h_name: &str = &h.name;
        let h_value: &[u8] = &h.value;
        if is_stripped_shape_output_header(h_name) {
            tracing::debug!(header = %h_name, "dropping shape-plugin output header per hop-by-hop/signer contract");
            continue;
        }
        if h_value.len() as u32 > wire_bounds.max_header_value_bytes {
            return Err(DialectError::UnsupportedRequest {
                reason: format!(
                    "shape plugin header `{h_name}` value {} bytes exceeds wire_bounds.max_header_value_bytes ({})",
                    h_value.len(),
                    wire_bounds.max_header_value_bytes,
                ),
            });
        }
        let name = http::HeaderName::from_bytes(h_name.as_bytes()).map_err(|e| {
            DialectError::UnsupportedRequest {
                reason: format!("plugin returned invalid header name `{h_name}`: {e}"),
            }
        })?;
        let value = http::HeaderValue::from_bytes(h_value).map_err(|e| {
            DialectError::UnsupportedRequest {
                reason: format!("plugin returned invalid header value for `{h_name}`: {e}"),
            }
        })?;
        headers.append(name, value);
    }

    let body: &[u8] = &archived.body;
    Ok(builder.shaped_request(url, method, headers, bytes::Bytes::copy_from_slice(body)))
}

fn runtime_error_to_dialect(err: WasmtimeRuntimeError) -> DialectError {
    match err {
        WasmtimeRuntimeError::GuestTrap { phase, source } => DialectError::UnsupportedRequest {
            reason: format!("{phase}: {source}"),
        },
        other => DialectError::UnsupportedRequest {
            reason: other.to_string(),
        },
    }
}

fn host_to_wire_transform_response(
    request: &TransformResponseRequest,
) -> Result<AlignedVec<16>, RkyvError> {
    let principal_kind = principal_kind_to_wire(&request.principal);
    let claim_bufs = claim_buffers(&request.principal);
    let claim_refs = claim_refs_from_bufs(&claim_bufs);
    let header_refs = header_refs_from_map(&request.response_headers);
    let upstream_base_url = upstream_base_url_str(&request.upstream);
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
    let claim_refs = claim_refs_from_bufs(&claim_bufs);
    let header_refs = header_refs_from_map(&request.response_headers);
    let upstream_base_url = upstream_base_url_str(&request.upstream);
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

fn claim_refs_from_bufs<'a>(claim_bufs: &'a [(&'a str, Vec<u8>)]) -> Vec<ClaimRef<'a>> {
    claim_bufs
        .iter()
        .map(|(key, value)| ClaimRef {
            key,
            value: value.as_slice(),
        })
        .collect()
}

fn header_refs_from_map(headers: &http::HeaderMap) -> Vec<HeaderRef<'_>> {
    headers
        .iter()
        .map(|(name, value)| HeaderRef {
            name: name.as_str(),
            value: value.as_bytes(),
        })
        .collect()
}

fn upstream_base_url_str(upstream: &Upstream) -> Option<String> {
    match upstream {
        Upstream::AnthropicDirect { base_url } => base_url.as_ref().map(ToString::to_string),
    }
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
