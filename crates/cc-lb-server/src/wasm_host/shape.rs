use std::sync::Arc;

use cc_lb_domain::{Principal, Upstream, UpstreamIdentity};
use cc_lb_plugin_wire::schema::WireVersion;
use cc_lb_plugin_wire::v2;
use cc_lb_plugin_wire::{
    ArchivedTransformResponseResult, ArchivedTransformSseEventResult, HeaderRef, PrincipalRef,
    QueryRef, SseEvent, SseEventRef, TransformResponseRequestRef, TransformSseEventRequestRef,
    UpstreamRef,
};
use cc_lb_runtime_wasmtime::{WasmPluginWireDispatch, WasmtimeRuntimeError};
use cc_lb_upstream::{
    DialectError, DialectShapeContext, GeminiEnvelopeHook, GeminiEnvelopeRequest,
    GeminiEnvelopeResult, RealtimeFrameDirection, RealtimeFrameKind, RealtimeFrameTransformHook,
    ResponseTransformError, ResponseTransformHook, ShapedRequest, ShapedRequestBuilder,
    SseEventTransformHook, TransformRealtimeFrameRequest, TransformRealtimeFrameResult,
    TransformResponseRequest, TransformResponseResult, TransformSseEventRequest,
    TransformSseEventResult, UpstreamDialect,
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
    realtime_frame_transform: Option<WasmtimeRealtimeFrameTransformHook>,
    native_request: Option<Arc<dyn UpstreamDialect>>,
    upstream_identity: Option<UpstreamIdentity>,
}

impl WasmtimeUpstreamDialect {
    pub fn new(dispatch: Arc<WasmPluginWireDispatch>) -> Self {
        let response_transform = if dispatch.has_response_transform() {
            Some(WasmtimeResponseTransformHook {
                dispatch: Arc::clone(&dispatch),
                wire_version: dispatch.response_wire_version(),
                identity: None,
            })
        } else {
            None
        };
        let sse_event_transform = if dispatch.has_sse_event_transform() {
            Some(WasmtimeSseEventTransformHook {
                dispatch: Arc::clone(&dispatch),
                wire_version: dispatch.sse_wire_version(),
                identity: None,
            })
        } else {
            None
        };
        let realtime_frame_transform = if dispatch.has_realtime_frame_transform() {
            Some(WasmtimeRealtimeFrameTransformHook {
                dispatch: Arc::clone(&dispatch),
                wire_version: dispatch.realtime_frame_wire_version(),
                identity: None,
            })
        } else {
            None
        };
        Self {
            dispatch,
            response_transform,
            sse_event_transform,
            realtime_frame_transform,
            native_request: None,
            upstream_identity: None,
        }
    }

    /// Compose a response-only plugin with the selected upstream's native request dialect.
    pub fn with_native_request(mut self, native: Arc<dyn UpstreamDialect>) -> Self {
        self.native_request = Some(native);
        self
    }
    pub fn with_upstream_identity(mut self, identity: UpstreamIdentity) -> Self {
        self.upstream_identity = Some(identity.clone());
        if let Some(hook) = self.response_transform.as_mut() {
            hook.identity = Some(identity.clone());
        }
        if let Some(hook) = self.sse_event_transform.as_mut() {
            hook.identity = Some(identity.clone());
        }
        if let Some(hook) = self.realtime_frame_transform.as_mut() {
            hook.identity = Some(identity);
        }
        self
    }
    fn call_shape_output(
        &self,
        in_bytes: &[u8],
        builder: &mut ShapedRequestBuilder,
        upstream: &Upstream,
    ) -> Result<ShapedRequest, DialectError> {
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
            access_archived_scoped_or_copy::<cc_lb_plugin_wire::ArchivedShapeResponse, _>(
                guest_bytes,
                |archived| {
                    let mut shaped = wire_to_host_shaped_request(
                        builder,
                        archived,
                        upstream,
                        self.dispatch.shape_origin_policy(),
                        self.dispatch.wire_bounds(),
                    )?;
                    if self.dispatch.has_shape_envelope_gemini()
                        && let Some(identity) = self.upstream_identity.clone()
                    {
                        shaped.set_gemini_envelope_hook(Arc::new(
                            WasmtimeGeminiEnvelopeHook {
                                dispatch: Arc::clone(&self.dispatch),
                                identity,
                            },
                        ));
                    }
                    Ok(shaped)
                },
            )
            .map_err(|e| DialectError::UnsupportedRequest {
                reason: format!("rkyv access ShapeResponse: {e}"),
            })
            .and_then(std::convert::identity)
        })
        .map_err(runtime_error_to_dialect)?
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
        let identity = self.upstream_identity.as_ref();
        match wire_version {
            Some(WireVersion::V1) => {}
            Some(WireVersion::V2) if identity.is_some() => {}
            Some(WireVersion::V2) => {
                return Err(DialectError::UnsupportedRequest {
                    reason: "provider-qualified shape hook is missing upstream identity".to_owned(),
                });
            }
            None => {
                if let Some(native) = &self.native_request {
                    return native.shape(context, upstream, principal, builder);
                }
                return Err(DialectError::UnsupportedRequest {
                    reason: "plugin metadata missing shape hook".to_owned(),
                });
            }
        }
        match wire_version.expect("shape wire version checked above") {
            WireVersion::V1 => with_wire_shape_request(
                context,
                upstream,
                principal,
                self.dispatch.cookie_redaction(),
                |in_bytes| self.call_shape_output(in_bytes, builder, upstream),
            ),
            WireVersion::V2 => with_wire_shape_request_v2(
                context,
                upstream,
                principal,
                identity.expect("V2 identity checked above"),
                self.dispatch.cookie_redaction(),
                |in_bytes| self.call_shape_output(in_bytes, builder, upstream),
            ),
        }
        .map_err(|e| DialectError::UnsupportedRequest {
            reason: format!("rkyv encode ShapeRequest: {e}"),
        })?
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
    fn realtime_frame_transform_hook(&self) -> Option<&dyn RealtimeFrameTransformHook> {
        self.realtime_frame_transform
            .as_ref()
            .map(|h| h as &dyn RealtimeFrameTransformHook)
    }
}

struct WasmtimeGeminiEnvelopeHook {
    dispatch: Arc<WasmPluginWireDispatch>,
    identity: UpstreamIdentity,
}

impl GeminiEnvelopeHook for WasmtimeGeminiEnvelopeHook {
    fn shape_envelope_gemini(
        &self,
        request: GeminiEnvelopeRequest<'_>,
    ) -> Result<GeminiEnvelopeResult, cc_lb_upstream::SignerError> {
        if self.identity.credential_kind == "gemini_oauth"
            && !request
                .url
                .host_str()
                .is_some_and(|host| host.ends_with(".googleapis.com"))
        {
            return Err(cc_lb_upstream::SignerError::SigningFailed {
                reason: "Gemini OAuth envelope hook requires a Code Assist Google host".to_owned(),
            });
        }
        let headers: Vec<v2::HeaderRef<'_>> = request
            .headers
            .iter()
            .map(|(name, value)| v2::HeaderRef {
                name: name.as_str(),
                value: value.as_bytes(),
            })
            .collect();
        let identity_ref = v2::UpstreamIdentityRef {
            provider: match self.identity.provider {
                cc_lb_domain::Provider::Anthropic => "anthropic",
                cc_lb_domain::Provider::Openai => "openai",
                cc_lb_domain::Provider::Gemini => "gemini",
            },
            credential_kind: self.identity.credential_kind.as_str(),
            upstream_id: self.identity.upstream_id.as_str(),
            name: self.identity.name.as_str(),
            base_url: self
                .identity
                .base_url
                .as_deref()
                .map(|value| v2::OptionalStrRef { value }),
            gemini_oauth_profile: self
                .identity
                .gemini_oauth_profile
                .as_deref()
                .map(|value| v2::OptionalStrRef { value }),
            code_assist_project_id: self
                .identity
                .code_assist_project_id
                .as_deref()
                .map(|value| v2::OptionalStrRef { value }),
        };
        let wire = v2::ShapeEnvelopeGeminiRequestRef {
            envelope_body: request.envelope_body.as_ref(),
            url: request.url.as_str(),
            headers: &headers,
            upstream: identity_ref,
        };
        let input = rkyv::to_bytes::<RkyvError>(&wire).map_err(|error| {
            cc_lb_upstream::SignerError::SigningFailed {
                reason: format!("rkyv encode Gemini envelope request: {error}"),
            }
        })?;
        let output = self
            .dispatch
            .call_shape_envelope_gemini_scoped(input.as_slice(), |bytes| bytes.to_vec())
            .map_err(|error| cc_lb_upstream::SignerError::SigningFailed {
                reason: format!("Gemini envelope hook failed: {error}"),
            })?;
        if output.len() as u64 > self.dispatch.wire_bounds().output_body_bytes {
            return Err(cc_lb_upstream::SignerError::SigningFailed {
                reason: "Gemini envelope hook output exceeds wire bounds".to_owned(),
            });
        }
        let result = access_archived_scoped_or_copy::<v2::ArchivedShapeEnvelopeGeminiResponse, _>(
            &output,
            rkyv::deserialize::<v2::ShapeEnvelopeGeminiResponse, RkyvError>,
        )
        .map_err(|error| cc_lb_upstream::SignerError::SigningFailed {
            reason: format!("rkyv decode Gemini envelope response: {error}"),
        })?
        .map_err(|error| cc_lb_upstream::SignerError::SigningFailed {
            reason: format!("rkyv deserialize Gemini envelope response: {error}"),
        })?;
        let headers = result
            .headers
            .map(|headers| {
                headers.into_vec().into_iter().try_fold(
                    http::HeaderMap::new(),
                    |mut map, header| {
                        let name = http::header::HeaderName::from_bytes(header.name.as_bytes())
                            .map_err(|_| cc_lb_upstream::SignerError::SigningFailed {
                                reason: "Gemini envelope hook returned invalid header name"
                                    .to_owned(),
                            })?;
                        let value = http::HeaderValue::from_bytes(&header.value).map_err(|_| {
                            cc_lb_upstream::SignerError::SigningFailed {
                                reason: "Gemini envelope hook returned invalid header value"
                                    .to_owned(),
                            }
                        })?;
                        map.append(name, value);
                        Ok(map)
                    },
                )
            })
            .transpose()?;
        Ok(GeminiEnvelopeResult {
            envelope_body: result.envelope_body.map(bytes::Bytes::from),
            headers,
        })
    }
}

struct WasmtimeResponseTransformHook {
    dispatch: Arc<WasmPluginWireDispatch>,
    wire_version: Option<WireVersion>,
    identity: Option<UpstreamIdentity>,
}
impl ResponseTransformHook for WasmtimeResponseTransformHook {
    fn transform_response(
        &self,
        request: TransformResponseRequest,
    ) -> Result<TransformResponseResult, ResponseTransformError> {
        let in_bytes = match self.wire_version {
            Some(WireVersion::V1) => host_to_wire_transform_response(&request),
            Some(WireVersion::V2) => {
                let Some(identity) = self.identity.as_ref() else {
                    return Err(response_runtime_error(
                        "V2 response transform missing upstream identity".to_owned(),
                    ));
                };
                host_to_wire_transform_response_v2(&request, identity)
            }
            None => {
                return Err(response_runtime_error(
                    "shape plugin is missing transform_response metadata".to_owned(),
                ));
            }
        }
        .map_err(|e| {
            response_runtime_error(format!("rkyv encode TransformResponseRequest: {e}"))
        })?;
        let out_bytes = self
            .dispatch
            .call_transform_response(in_bytes.as_slice())
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
    identity: Option<UpstreamIdentity>,
}

impl SseEventTransformHook for WasmtimeSseEventTransformHook {
    fn transform_sse_event(
        &self,
        request: TransformSseEventRequest,
    ) -> Result<TransformSseEventResult, ResponseTransformError> {
        let in_bytes = match self.wire_version {
            Some(WireVersion::V1) => host_to_wire_transform_sse_event(&request),
            Some(WireVersion::V2) => {
                let Some(identity) = self.identity.as_ref() else {
                    return Err(response_runtime_error(
                        "V2 SSE transform missing upstream identity".to_owned(),
                    ));
                };
                host_to_wire_transform_sse_event_v2(&request, identity)
            }
            None => {
                return Err(response_runtime_error(
                    "shape plugin is missing transform_sse_event metadata".to_owned(),
                ));
            }
        }
        .map_err(|e| {
            response_runtime_error(format!("rkyv encode TransformSseEventRequest: {e}"))
        })?;
        let out_bytes = self
            .dispatch
            .call_transform_sse_event(in_bytes.as_slice())
            .map_err(runtime_error_to_response_transform)?;
        reject_oversized_output(
            out_bytes.as_slice(),
            self.dispatch.wire_bounds().output_body_bytes,
        )?;
        wire_to_host_transform_sse_event(&out_bytes)
    }
}
struct WasmtimeRealtimeFrameTransformHook {
    dispatch: Arc<WasmPluginWireDispatch>,
    wire_version: Option<WireVersion>,
    identity: Option<UpstreamIdentity>,
}

impl RealtimeFrameTransformHook for WasmtimeRealtimeFrameTransformHook {
    fn transform_realtime_frame(
        &self,
        request: TransformRealtimeFrameRequest,
    ) -> Result<TransformRealtimeFrameResult, ResponseTransformError> {
        let identity = self.identity.as_ref().ok_or_else(|| {
            response_runtime_error(
                "V2 realtime frame transform missing upstream identity".to_owned(),
            )
        })?;
        if self.wire_version != Some(WireVersion::V2) {
            return Err(response_runtime_error(
                "realtime frame transform requires wire version 2".to_owned(),
            ));
        }
        let input = host_to_wire_transform_realtime_frame_v2(&request, identity)
            .map_err(|e| response_runtime_error(format!("rkyv encode realtime frame: {e}")))?;
        let output = self
            .dispatch
            .call_transform_realtime_frame(input.as_slice())
            .map_err(runtime_error_to_response_transform)?;
        reject_oversized_output(
            output.as_slice(),
            self.dispatch.wire_bounds().output_body_bytes,
        )?;
        wire_to_host_transform_realtime_frame(&output)
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
    let header_refs: Vec<HeaderRef<'_>> = context
        .downstream_headers
        .iter()
        .filter(|(name, _)| !is_stripped_downstream_header(name.as_str(), cookie_redaction))
        .map(|(name, value)| HeaderRef {
            name: name.as_str(),
            value: value.as_bytes(),
        })
        .collect();
    let base_url_str = upstream_base_url(upstream).map(|u| u.to_string());
    let upstream_ref = match upstream {
        Upstream::AnthropicDirect { .. } => UpstreamRef::AnthropicDirect {
            base_url: base_url_str.as_deref().map(|s| QueryRef { value: s }),
        },
        Upstream::OpenaiDirect { .. } => UpstreamRef::OpenaiDirect {
            base_url: base_url_str.as_deref().map(|s| QueryRef { value: s }),
        },
        Upstream::GeminiDirect { .. } => UpstreamRef::GeminiDirect {
            base_url: base_url_str.as_deref().map(|s| QueryRef { value: s }),
        },
    };
    let scrubbed_query = context
        .query
        .as_deref()
        .map(cc_lb_upstream::scrub_query_api_key);
    let query_ref = scrubbed_query.as_deref().map(|s| QueryRef { value: s });
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
            claims: &[],
        },
        upstream: upstream_ref,
    };
    serialize_with_input_scratch(&request, with_bytes)
}

fn with_wire_shape_request_v2<R>(
    context: &DialectShapeContext,
    upstream: &Upstream,
    principal: &Principal,
    identity: &UpstreamIdentity,
    cookie_redaction: bool,
    with_bytes: impl for<'a> FnOnce(&'a [u8]) -> R,
) -> Result<R, RkyvError> {
    let principal_kind_str = principal_kind_to_wire(principal);
    let header_refs: Vec<v2::HeaderRef<'_>> = context
        .downstream_headers
        .iter()
        .filter(|(name, _)| !is_stripped_downstream_header(name.as_str(), cookie_redaction))
        .map(|(name, value)| v2::HeaderRef {
            name: name.as_str(),
            value: value.as_bytes(),
        })
        .collect();
    let base_url_str = upstream_base_url(upstream).map(|u| u.to_string());
    let upstream_ref = match upstream {
        Upstream::AnthropicDirect { .. } => v2::UpstreamRef::AnthropicDirect {
            base_url: base_url_str
                .as_deref()
                .map(|value| v2::OptionalStrRef { value }),
        },
        Upstream::OpenaiDirect { .. } => v2::UpstreamRef::OpenaiDirect {
            base_url: base_url_str
                .as_deref()
                .map(|value| v2::OptionalStrRef { value }),
        },
        Upstream::GeminiDirect { .. } => v2::UpstreamRef::GeminiDirect {
            base_url: base_url_str
                .as_deref()
                .map(|value| v2::OptionalStrRef { value }),
        },
    };
    let provider = match identity.provider {
        cc_lb_domain::Provider::Anthropic => "anthropic",
        cc_lb_domain::Provider::Openai => "openai",
        cc_lb_domain::Provider::Gemini => "gemini",
    };
    let identity_ref = v2::UpstreamIdentityRef {
        provider,
        credential_kind: identity.credential_kind.as_str(),
        upstream_id: identity.upstream_id.as_str(),
        name: identity.name.as_str(),
        base_url: identity
            .base_url
            .as_deref()
            .map(|value| v2::OptionalStrRef { value }),
        gemini_oauth_profile: identity
            .gemini_oauth_profile
            .as_deref()
            .map(|value| v2::OptionalStrRef { value }),
        code_assist_project_id: identity
            .code_assist_project_id
            .as_deref()
            .map(|value| v2::OptionalStrRef { value }),
    };
    let scrubbed_query = context
        .query
        .as_deref()
        .map(cc_lb_upstream::scrub_query_api_key);
    let query_ref = scrubbed_query
        .as_deref()
        .map(|value| v2::OptionalStrRef { value });
    let request = v2::ShapeRequestRef {
        request_id: context.request_id.as_str(),
        method: context.method.as_str(),
        path: context.path.as_str(),
        query: query_ref,
        headers: &header_refs,
        body: context.body_bytes.as_ref(),
        principal: v2::PrincipalRef {
            id: principal.id.as_str(),
            kind: principal_kind_str,
            claims: &[],
        },
        upstream: upstream_ref,
        upstream_identity: identity_ref,
    };
    serialize_with_input_scratch(&request, with_bytes)
}
fn canonical_base_url(value: &str) -> url::Url {
    match url::Url::parse(value) {
        Ok(url) => url,
        Err(_) => unreachable!("canonical upstream base URL must parse"),
    }
}

fn upstream_base_url(upstream: &Upstream) -> Option<url::Url> {
    let default = match upstream {
        Upstream::AnthropicDirect { .. } => "https://api.anthropic.com",
        Upstream::OpenaiDirect { .. } => "https://api.openai.com",
        Upstream::GeminiDirect { .. } => "https://generativelanguage.googleapis.com",
    };
    match upstream {
        Upstream::AnthropicDirect { base_url }
        | Upstream::OpenaiDirect { base_url }
        | Upstream::GeminiDirect { base_url } => Some(
            base_url
                .clone()
                .unwrap_or_else(|| canonical_base_url(default)),
        ),
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
            | "x-goog-api-key"
            | "api-key"
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
    let mut parsed_url = url::Url::parse(url_str)?;

    if matches!(
        origin_policy,
        cc_lb_runtime_wasmtime::policy::ShapeOriginPolicy::SelectedUpstreamOrigin
    ) && let Some(expected) = upstream_base_url(upstream)
    {
        let expected_origin = expected.origin();
        let actual_origin = parsed_url.origin();
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
    cc_lb_upstream::scrub_url_query_api_key(&mut parsed_url);

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
    Ok(builder.shaped_request(
        parsed_url,
        method,
        headers,
        bytes::Bytes::copy_from_slice(body),
    ))
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
    let header_refs = header_refs_from_map(&request.response_headers);
    let upstream_base_url = upstream_base_url_str(&request.upstream);
    let upstream = match &request.upstream {
        Upstream::AnthropicDirect { .. } => UpstreamRef::AnthropicDirect {
            base_url: upstream_base_url.as_deref().map(|value| QueryRef { value }),
        },
        Upstream::OpenaiDirect { .. } => UpstreamRef::OpenaiDirect {
            base_url: upstream_base_url.as_deref().map(|value| QueryRef { value }),
        },
        Upstream::GeminiDirect { .. } => UpstreamRef::GeminiDirect {
            base_url: upstream_base_url.as_deref().map(|value| QueryRef { value }),
        },
    };
    let wire = TransformResponseRequestRef {
        request_id: request.request_id.as_str(),
        principal: PrincipalRef {
            id: request.principal.id.as_str(),
            kind: principal_kind,
            claims: &[],
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
    let header_refs = header_refs_from_map(&request.response_headers);
    let upstream_base_url = upstream_base_url_str(&request.upstream);
    let upstream = match &request.upstream {
        Upstream::AnthropicDirect { .. } => UpstreamRef::AnthropicDirect {
            base_url: upstream_base_url.as_deref().map(|value| QueryRef { value }),
        },
        Upstream::OpenaiDirect { .. } => UpstreamRef::OpenaiDirect {
            base_url: upstream_base_url.as_deref().map(|value| QueryRef { value }),
        },
        Upstream::GeminiDirect { .. } => UpstreamRef::GeminiDirect {
            base_url: upstream_base_url.as_deref().map(|value| QueryRef { value }),
        },
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
            claims: &[],
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

fn v2_identity_ref(identity: &UpstreamIdentity) -> v2::UpstreamIdentityRef<'_> {
    v2::UpstreamIdentityRef {
        provider: match identity.provider {
            cc_lb_domain::Provider::Anthropic => "anthropic",
            cc_lb_domain::Provider::Openai => "openai",
            cc_lb_domain::Provider::Gemini => "gemini",
        },
        credential_kind: identity.credential_kind.as_str(),
        upstream_id: identity.upstream_id.as_str(),
        name: identity.name.as_str(),
        base_url: identity
            .base_url
            .as_deref()
            .map(|value| v2::OptionalStrRef { value }),
        gemini_oauth_profile: identity
            .gemini_oauth_profile
            .as_deref()
            .map(|value| v2::OptionalStrRef { value }),
        code_assist_project_id: identity
            .code_assist_project_id
            .as_deref()
            .map(|value| v2::OptionalStrRef { value }),
    }
}

fn v2_upstream_ref(upstream: &Upstream) -> v2::UpstreamRef<'_> {
    match upstream {
        Upstream::AnthropicDirect { .. } => v2::UpstreamRef::AnthropicDirect { base_url: None },
        Upstream::OpenaiDirect { .. } => v2::UpstreamRef::OpenaiDirect { base_url: None },
        Upstream::GeminiDirect { .. } => v2::UpstreamRef::GeminiDirect { base_url: None },
    }
}

fn host_to_wire_transform_response_v2(
    request: &TransformResponseRequest,
    identity: &UpstreamIdentity,
) -> Result<AlignedVec<16>, RkyvError> {
    let headers = request
        .response_headers
        .iter()
        .map(|(name, value)| v2::HeaderRef {
            name: name.as_str(),
            value: value.as_bytes(),
        })
        .collect::<Vec<_>>();
    let wire = v2::TransformResponseRequestRef {
        request_id: request.request_id.as_str(),
        principal: v2::PrincipalRef {
            id: request.principal.id.as_str(),
            kind: principal_kind_to_wire(&request.principal),
            claims: &[],
        },
        upstream: v2_upstream_ref(&request.upstream),
        upstream_identity: v2_identity_ref(identity),
        request_method: request.request_method.as_str(),
        request_path: request.request_path.as_str(),
        canonical_model_id: request.canonical_model_id.as_str(),
        response_status: request.response_status.as_u16(),
        response_headers: &headers,
        body: request.body.as_ref(),
    };
    rkyv::to_bytes::<RkyvError>(&wire)
}

fn host_to_wire_transform_sse_event_v2(
    request: &TransformSseEventRequest,
    identity: &UpstreamIdentity,
) -> Result<AlignedVec<16>, RkyvError> {
    let headers = request
        .response_headers
        .iter()
        .map(|(name, value)| v2::HeaderRef {
            name: name.as_str(),
            value: value.as_bytes(),
        })
        .collect::<Vec<_>>();
    let wire = v2::TransformSseEventRequestRef {
        request_id: request.request_id.as_str(),
        principal: v2::PrincipalRef {
            id: request.principal.id.as_str(),
            kind: principal_kind_to_wire(&request.principal),
            claims: &[],
        },
        upstream: v2_upstream_ref(&request.upstream),
        upstream_identity: v2_identity_ref(identity),
        request_method: request.request_method.as_str(),
        request_path: request.request_path.as_str(),
        canonical_model_id: request.canonical_model_id.as_str(),
        response_status: request.response_status.as_u16(),
        response_headers: &headers,
        event: v2::SseEventRef {
            event: request.event.event.as_str(),
            data: request.event.data.as_ref(),
        },
    };
    rkyv::to_bytes::<RkyvError>(&wire)
}
fn host_to_wire_transform_realtime_frame_v2(
    request: &TransformRealtimeFrameRequest,
    identity: &UpstreamIdentity,
) -> Result<AlignedVec<16>, RkyvError> {
    let wire = v2::TransformRealtimeFrameRequestRef {
        request_id: request.request_id.as_str(),
        principal: v2::PrincipalRef {
            id: request.principal.id.as_str(),
            kind: principal_kind_to_wire(&request.principal),
            claims: &[],
        },
        upstream: v2_upstream_ref(&request.upstream),
        upstream_identity: v2_identity_ref(identity),
        request_method: request.request_method.as_str(),
        request_path: request.request_path.as_str(),
        canonical_model_id: request.canonical_model_id.as_str(),
        direction: match request.direction {
            RealtimeFrameDirection::Setup => "setup",
            RealtimeFrameDirection::ClientToProvider => "client_to_provider",
            RealtimeFrameDirection::ProviderToClient => "provider_to_client",
        },
        kind: match request.kind {
            RealtimeFrameKind::Json => "json",
            RealtimeFrameKind::Text => "text",
            RealtimeFrameKind::Binary => "binary",
        },
        payload: request.payload.as_ref(),
    };
    rkyv::to_bytes::<RkyvError>(&wire)
}

fn wire_to_host_transform_realtime_frame(
    bytes: &AlignedVec<16>,
) -> Result<TransformRealtimeFrameResult, ResponseTransformError> {
    let archived = rkyv::access::<v2::ArchivedTransformRealtimeFrameResult, RkyvError>(bytes)
        .map_err(|e| response_runtime_error(format!("rkyv access realtime frame result: {e}")))?;
    let result: v2::TransformRealtimeFrameResult =
        rkyv::deserialize::<v2::TransformRealtimeFrameResult, RkyvError>(archived).map_err(
            |e| response_runtime_error(format!("rkyv deserialize realtime frame result: {e}")),
        )?;
    match result {
        v2::TransformRealtimeFrameResult::Unchanged => Ok(TransformRealtimeFrameResult::Unchanged),
        v2::TransformRealtimeFrameResult::Drop => Ok(TransformRealtimeFrameResult::Drop),
        v2::TransformRealtimeFrameResult::Replace { kind, payload } => {
            let kind = match kind.as_ref() {
                "json" => RealtimeFrameKind::Json,
                "text" => RealtimeFrameKind::Text,
                "binary" => RealtimeFrameKind::Binary,
                other => {
                    return Err(response_runtime_error(format!(
                        "unknown realtime frame kind `{other}`"
                    )));
                }
            };
            Ok(TransformRealtimeFrameResult::Replace {
                kind,
                payload: bytes::Bytes::from(payload),
            })
        }
    }
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
    upstream_base_url(upstream).map(|url| url.to_string())
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

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use cc_lb_domain::{PrincipalKind, Provider, Upstream, UpstreamIdentity};
    use cc_lb_plugin_wire::{ArchivedShapeRequest, ArchivedShapeResponse, ShapeResponse};
    use cc_lb_upstream::{DialectShapeContext, UpstreamDialect, shape_request};
    use http::{HeaderMap, HeaderValue, Method};

    use super::*;

    fn principal() -> Principal {
        Principal {
            id: "principal-shape".to_owned(),
            kind: PrincipalKind::ApiKey,
        }
    }

    #[test]
    fn default_provider_base_urls_are_exposed_to_shape_and_transforms() {
        for (upstream, expected) in [
            (
                Upstream::OpenaiDirect { base_url: None },
                "https://api.openai.com/",
            ),
            (
                Upstream::GeminiDirect { base_url: None },
                "https://generativelanguage.googleapis.com/",
            ),
        ] {
            assert_eq!(
                upstream_base_url(&upstream).as_ref().map(url::Url::as_str),
                Some(expected)
            );
            assert_eq!(upstream_base_url_str(&upstream).as_deref(), Some(expected));
        }
    }

    #[test]
    fn shape_wire_scrubs_provider_credentials_and_query_keys() {
        let mut headers = HeaderMap::new();
        headers.insert("x-goog-api-key", HeaderValue::from_static("gemini-secret"));
        headers.insert("api-key", HeaderValue::from_static("provider-secret"));
        headers.insert("x-request-id", HeaderValue::from_static("request"));
        let context = DialectShapeContext {
            request_id: "req-shape".to_owned(),
            downstream_headers: headers,
            method: Method::POST,
            path: "/v1beta/models/gemini-2.5-pro:generateContent".to_owned(),
            query: Some("key=raw-secret&foo=bar&KEY=other".to_owned()),
            body_bytes: Bytes::from_static(b"{}"),
        };

        with_wire_shape_request(
            &context,
            &Upstream::GeminiDirect { base_url: None },
            &principal(),
            false,
            |bytes| {
                let archived =
                    rkyv::access::<ArchivedShapeRequest, RkyvError>(bytes).expect("shape request");
                let query: Option<&str> = archived.query.as_ref().map(|value| &**value);
                assert_eq!(query, Some("foo=bar"));
                let header_names: Vec<&str> = archived
                    .headers
                    .iter()
                    .map(|header| {
                        let name: &str = &header.name;
                        name
                    })
                    .collect();
                assert_eq!(header_names, vec!["x-request-id"]);
                match &archived.upstream {
                    cc_lb_plugin_wire::ArchivedUpstream::GeminiDirect { base_url } => {
                        let base_url = base_url.as_ref().expect("default base URL");
                        let base_url: &str = base_url;
                        assert_eq!(base_url, "https://generativelanguage.googleapis.com/");
                    }
                    other => panic!("unexpected upstream variant: {other:?}"),
                }
            },
        )
        .expect("encode shape request");
    }

    #[test]
    fn shape_output_scrubs_provider_credentials() {
        for name in ["Authorization", "x-api-key", "x-goog-api-key", "api-key"] {
            assert!(is_stripped_shape_output_header(name), "{name}");
        }
    }
    #[test]
    fn shape_output_scrubs_query_api_keys() {
        struct ScrubbingDialect;

        impl UpstreamDialect for ScrubbingDialect {
            fn shape(
                &self,
                _context: &DialectShapeContext,
                upstream: &Upstream,
                _principal: &Principal,
                builder: &mut ShapedRequestBuilder,
            ) -> Result<ShapedRequest, DialectError> {
                let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&ShapeResponse {
                    url: Box::from(
                        "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key=secret&keep=1",
                    ),
                    method: Box::from("POST"),
                    headers: Box::new([]),
                    body: Box::new([]),
                })
                .map_err(|error| DialectError::UnsupportedRequest {
                    reason: error.to_string(),
                })?;
                let archived =
                    rkyv::access::<ArchivedShapeResponse, RkyvError>(&bytes).map_err(|error| {
                        DialectError::UnsupportedRequest {
                            reason: error.to_string(),
                        }
                    })?;
                wire_to_host_shaped_request(
                    builder,
                    archived,
                    upstream,
                    cc_lb_runtime_wasmtime::policy::ShapeOriginPolicy::Unrestricted,
                    &cc_lb_runtime_wasmtime::policy::PluginWireBounds::default(),
                )
            }
        }

        let context = DialectShapeContext {
            request_id: "req-shape-url".to_owned(),
            downstream_headers: HeaderMap::new(),
            method: Method::POST,
            path: "/v1beta/models/gemini-2.5-flash:generateContent".to_owned(),
            query: None,
            body_bytes: Bytes::new(),
        };
        let shaped = shape_request(
            &ScrubbingDialect,
            &context,
            &Upstream::GeminiDirect { base_url: None },
            &principal(),
        )
        .expect("shape request");
        assert_eq!(shaped.url().query(), Some("keep=1"));
    }

    #[test]
    fn realtime_frame_wire_contract_preserves_host_owned_identity_and_transport_shape() {
        let identity = UpstreamIdentity {
            provider: Provider::Gemini,
            credential_kind: "gemini_api_key".to_owned(),
            upstream_id: "upstream-42".to_owned(),
            name: "live-primary".to_owned(),
            base_url: Some("https://provider.example/".to_owned()),
            gemini_oauth_profile: None,
            code_assist_project_id: None,
        };
        for (direction, expected_direction) in [
            (RealtimeFrameDirection::Setup, "setup"),
            (
                RealtimeFrameDirection::ClientToProvider,
                "client_to_provider",
            ),
            (
                RealtimeFrameDirection::ProviderToClient,
                "provider_to_client",
            ),
        ] {
            for (kind, expected_kind, payload) in [
                (
                    RealtimeFrameKind::Json,
                    "json",
                    Bytes::from_static(br#"{}"#),
                ),
                (
                    RealtimeFrameKind::Text,
                    "text",
                    Bytes::from_static(b"voice"),
                ),
                (
                    RealtimeFrameKind::Binary,
                    "binary",
                    Bytes::from_static(&[0, 1, 255]),
                ),
            ] {
                let bytes = host_to_wire_transform_realtime_frame_v2(
                    &TransformRealtimeFrameRequest {
                        request_id: "request-1".to_owned(),
                        principal: principal(),
                        upstream: Upstream::GeminiDirect { base_url: None },
                        request_method: Method::GET,
                        request_path: "/ws/live".to_owned(),
                        canonical_model_id: "gemini-live".to_owned(),
                        direction,
                        kind,
                        payload: payload.clone(),
                    },
                    &identity,
                )
                .expect("realtime frame request serializes");
                let archived =
                    rkyv::access::<v2::ArchivedTransformRealtimeFrameRequest, RkyvError>(&bytes)
                        .expect("realtime frame request archives");
                let method: &str = &archived.request_method;
                let path: &str = &archived.request_path;
                let direction: &str = &archived.direction;
                let kind: &str = &archived.kind;
                let upstream_id: &str = &archived.upstream_identity.upstream_id;
                assert_eq!(method, "GET");
                assert_eq!(path, "/ws/live");
                assert_eq!(direction, expected_direction);
                assert_eq!(kind, expected_kind);
                assert_eq!(upstream_id, "upstream-42");
                assert_eq!(archived.payload.as_ref(), payload.as_ref());
            }
        }
    }

    #[test]
    fn realtime_frame_wire_results_cover_unchanged_replace_and_drop() {
        let cases = [
            (
                v2::TransformRealtimeFrameResult::Unchanged,
                TransformRealtimeFrameResult::Unchanged,
            ),
            (
                v2::TransformRealtimeFrameResult::Replace {
                    kind: "binary".into(),
                    payload: vec![0, 1, 255].into_boxed_slice(),
                },
                TransformRealtimeFrameResult::Replace {
                    kind: RealtimeFrameKind::Binary,
                    payload: Bytes::from_static(&[0, 1, 255]),
                },
            ),
            (
                v2::TransformRealtimeFrameResult::Drop,
                TransformRealtimeFrameResult::Drop,
            ),
        ];
        for (wire, expected) in cases {
            let bytes = rkyv::to_bytes::<RkyvError>(&wire).expect("realtime result serializes");
            assert_eq!(
                wire_to_host_transform_realtime_frame(&bytes).expect("realtime result decodes"),
                expected
            );
        }
    }

    #[test]
    fn realtime_frame_plugin_output_is_rejected_at_the_wire_bound() {
        let error = reject_oversized_output(b"0123456789", 9).expect_err("bound is enforced");
        assert!(
            error
                .to_string()
                .contains("exceeds wire_bounds.output_body_bytes")
        );
    }
}
