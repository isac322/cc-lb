//! `FilterPlugin` adapter backed by the wasmtime runtime.
//!
//! Phase 1 W6 — bridges the host-side `cc_lb_plugin_api::FilterPlugin`
//! trait surface to the rkyv wire types in `cc_lb_plugin_types`. One
//! request flows through three transforms:
//!
//! 1. `(RequestContext, Principal, [UpstreamCandidate]) -> wire FilterRequest`
//! 2. `rkyv::to_bytes -> WasmtimeRuntime::call_filter -> Vec<u8>`
//! 3. `Vec<u8> -> AlignedVec -> rkyv::access -> deserialize -> FilterOutput`
//!
//! The intermediate `AlignedVec` copy on the response path is required
//! by rkyv 0.8: `rkyv::access` enforces destination alignment matching
//! `align_of::<Archived<T>>()` (16 for our root types) and the host
//! receives a plain `Vec<u8>` from `Memory::data()` whose allocator
//! provides no alignment guarantee.
//!
//! `slot_key()` lives on the [`FilterPlugin`] trait and returns the
//! canonical [`SlotKey`] from `cc-lb-plugin-api`.
//!
//! See `docs/rfc/0001-plugin-runtime-vnext.md` §FilterPlugin adapter.

use std::sync::Arc;

use cc_lb_plugin_api::{
    FilterError, FilterOutput, FilterPlugin, PerCandidateReason, Principal, RequestContext,
    SlotKey, UpstreamCandidate,
};
use cc_lb_plugin_types::{
    ArchivedFilterResponse, FilterRequest as WireFilterRequest, Header as WireHeader,
    Principal as WirePrincipal, UpstreamCandidate as WireUpstreamCandidate,
};
use rkyv::rancor::Error as RkyvError;
use rkyv::util::AlignedVec;
use uuid::Uuid;

use crate::cache::call_filter_hook;
use crate::cell::{PluginCell, PluginSlot};
use crate::error::WasmtimeRuntimeError;

/// `FilterPlugin` adapter backed by a wasmtime `PluginSlot`. The
/// adapter **snapshots** the slot's [`PluginCell`] at construction
/// time so re-registering the same slot during the next dynamic-view
/// rebuild cannot leak the new cell into the previous live view —
/// each `DynamicView` keeps the adapter cells it was built with until
/// it is itself replaced.
pub struct WasmtimeFilterPlugin {
    slot_key: SlotKey,
    cell: Arc<PluginCell>,
    plugin_id: Uuid,
    plugin_name: String,
    runtime_config: Arc<crate::HotEngineConfig>,
}

impl WasmtimeFilterPlugin {
    /// Snapshots `slot.current` for the lifetime of this adapter.
    /// Subsequent re-registrations on the same `SlotKey` will not
    /// disturb this adapter's view of the plugin.
    pub fn new(
        slot: Arc<PluginSlot>,
        slot_key: SlotKey,
        plugin_id: Uuid,
        plugin_name: impl Into<String>,
        runtime_config: Arc<crate::HotEngineConfig>,
    ) -> Self {
        let cell = slot.current.load_full();
        Self {
            slot_key,
            cell,
            plugin_id,
            plugin_name: plugin_name.into(),
            runtime_config,
        }
    }
}

impl FilterPlugin for WasmtimeFilterPlugin {
    fn filter(
        &self,
        ctx: &RequestContext,
        principal: &Principal,
        candidates: &[UpstreamCandidate],
    ) -> Result<FilterOutput, FilterError> {
        let request = host_to_wire_request(
            ctx,
            principal,
            candidates,
            self.runtime_config.cookie_redaction,
        );

        let in_bytes = rkyv::to_bytes::<RkyvError>(&request).map_err(|e| FilterError::Runtime {
            reason: format!("rkyv encode request: {e}"),
        })?;

        let out_bytes =
            call_filter_hook(&self.cell, in_bytes.as_slice()).map_err(runtime_error_to_filter)?;
        // Wire-bound cap on filter output; matches the DEFAULT_FILES_CAP_BYTES
        // request body cap by default so legitimate large-message flows are
        // unaffected. Tighter caps are opt-in via config.
        let bound = self.runtime_config.wire_bounds.output_body_bytes;
        if out_bytes.len() as u64 > bound {
            return Err(FilterError::Runtime {
                reason: format!(
                    "filter output {} bytes exceeds wire_bounds.output_body_bytes ({})",
                    out_bytes.len(),
                    bound
                ),
            });
        }

        // rkyv::access enforces 16-byte alignment on the bytes; Vec<u8>
        // from Memory::data() carries no such guarantee. Copy through
        // AlignedVec to satisfy the validator. The subsequent
        // wire_to_host_output walks the archived view directly (RFC-0001
        // gap-analysis #7): no rkyv::deserialize, no owned-String or
        // owned-Vec allocations per candidate.
        let mut aligned = AlignedVec::<16>::with_capacity(out_bytes.len());
        aligned.extend_from_slice(&out_bytes);

        let archived =
            rkyv::access::<ArchivedFilterResponse, RkyvError>(&aligned).map_err(|e| {
                FilterError::Runtime {
                    reason: format!("rkyv access response: {e}"),
                }
            })?;

        wire_to_host_output(
            archived,
            self.runtime_config.wire_bounds.reason_bytes as usize,
        )
    }

    fn plugin_id(&self) -> Uuid {
        self.plugin_id
    }

    fn plugin_name(&self) -> &str {
        &self.plugin_name
    }

    fn slot_key(&self) -> SlotKey {
        self.slot_key.clone()
    }
}

fn runtime_error_to_filter(err: WasmtimeRuntimeError) -> FilterError {
    match err {
        WasmtimeRuntimeError::GuestTrap { phase, source } => FilterError::Trap {
            reason: format!("{phase}: {source}"),
        },
        other => FilterError::Runtime {
            reason: other.to_string(),
        },
    }
}

fn host_to_wire_request(
    ctx: &RequestContext,
    principal: &Principal,
    candidates: &[UpstreamCandidate],
    cookie_redaction: bool,
) -> WireFilterRequest {
    WireFilterRequest {
        request_id: ctx.request_id.clone(),
        method: ctx.method.as_str().to_owned(),
        path: ctx.path.clone(),
        query: ctx.query.clone(),
        headers: ctx
            .downstream_headers
            .iter()
            .filter(|(name, _)| !is_stripped_downstream_header(name.as_str(), cookie_redaction))
            .map(|(name, value)| WireHeader {
                name: name.as_str().to_owned(),
                value: value.as_bytes().to_vec(),
            })
            .collect(),
        body: ctx.body_bytes.to_vec(),
        principal: WirePrincipal {
            id: principal.id.clone(),
            kind: principal_kind_to_wire(principal),
            claims: claims_to_wire(&principal.claims),
        },
        candidates: candidates
            .iter()
            .map(|c| WireUpstreamCandidate {
                upstream_id: c.upstream_id.to_string(),
                name: c.name.clone(),
                kind: c.kind.as_str().to_owned(),
                observed_at_unix_secs: c.observed_at_unix_secs,
                predicted_cache_read_tokens: c
                    .cache_score
                    .as_ref()
                    .map(|s| s.predicted_cache_read_tokens)
                    .unwrap_or(0),
            })
            .collect(),
    }
}

fn principal_kind_to_wire(principal: &Principal) -> String {
    serde_json::to_value(&principal.kind)
        .ok()
        .and_then(|value| value.as_str().map(ToOwned::to_owned))
        .unwrap_or_else(|| "unknown".to_owned())
}

fn claims_to_wire(claims: &serde_json::Map<String, serde_json::Value>) -> Vec<(String, Vec<u8>)> {
    claims
        .iter()
        .filter_map(|(k, v)| serde_json::to_vec(v).ok().map(|bytes| (k.clone(), bytes)))
        .collect()
}

/// Downstream-request headers that must NEVER cross the plugin
/// boundary. `authorization`/`x-api-key` are the primary key material
/// for the proxy, `host` is meaningless once the request is being
/// routed to an upstream, and `proxy-authorization` is a hop-by-hop
/// credential that the host already strips before dispatch
/// (`hop_by_hop.rs`) — allowing guest visibility of it would let a
/// buggy plugin log or exfiltrate a downstream proxy credential.
///
/// When `cookie_redaction` is `true`, also strip `cookie` — an
/// opt-in hardening for deployments that treat downstream session
/// cookies as sensitive. The default (`false`) preserves pre-Sprint-3
/// behaviour so filter plugins that route on `cookie` values keep
/// working without a config change.
fn is_stripped_downstream_header(name: &str, cookie_redaction: bool) -> bool {
    let lower = name.to_ascii_lowercase();
    if matches!(
        lower.as_str(),
        "authorization" | "x-api-key" | "host" | "proxy-authorization"
    ) {
        return true;
    }
    cookie_redaction && lower.as_str() == "cookie"
}

/// Shape-plugin output headers that must NEVER reach the dispatcher.
/// Hop-by-hop headers per RFC 7230 §6.1 are meaningless upstream (the
/// host manages its own connection). Signer/auth-owned headers
/// (`authorization`, `x-api-key`, `x-anthropic-*`) must come from the
/// authenticated proxy signing path — a shape plugin trying to inject
/// them is either buggy or hostile. `host` and `content-length` are
/// derived from the request URL and body respectively.
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

fn wire_to_host_output(
    archived: &ArchivedFilterResponse,
    reason_cap: usize,
) -> Result<FilterOutput, FilterError> {
    let mut kept_upstream_ids = Vec::new();
    let mut per_candidate_reasons = Vec::new();
    let mut reasons = Vec::new();

    for result in archived.results.iter() {
        let upstream_id_str: &str = &result.upstream_id;
        let decision_str: &str = &result.decision;
        let reason_str: &str = &result.reason;
        let upstream_id =
            Uuid::parse_str(upstream_id_str).map_err(|source| FilterError::Runtime {
                reason: format!(
                    "plugin returned invalid upstream_id `{upstream_id_str}`: {source}"
                ),
            })?;
        if decision_str == "accept" {
            kept_upstream_ids.push(upstream_id);
        } else {
            per_candidate_reasons.push(per_candidate_reason_from_label(decision_str, reason_str));
        }
        if !reason_str.is_empty() {
            let truncated = truncate_reason(reason_str, reason_cap);
            reasons.push(format!("{upstream_id_str}: {truncated}"));
        }
    }

    Ok(FilterOutput {
        kept_upstream_ids,
        reason: reasons.join("; "),
        per_candidate_reasons,
    })
}

/// Truncate `reason` to at most `cap` chars, respecting UTF-8
/// character boundaries. Returns the input slice unchanged when
/// under the cap so the common case allocates nothing.
fn truncate_reason(reason: &str, cap: usize) -> std::borrow::Cow<'_, str> {
    if reason.len() <= cap {
        return std::borrow::Cow::Borrowed(reason);
    }
    let mut end = cap;
    while end > 0 && !reason.is_char_boundary(end) {
        end -= 1;
    }
    std::borrow::Cow::Owned(reason[..end].to_owned())
}

fn per_candidate_reason_from_label(decision: &str, reason: &str) -> PerCandidateReason {
    let label = if decision == "accept" {
        reason
    } else {
        decision
    };
    let label = label.replace('-', "_").to_ascii_lowercase();
    if label.contains("rate_limit") {
        PerCandidateReason::RateLimited
    } else if label.contains("quota") {
        PerCandidateReason::InsufficientQuota
    } else if label.contains("unhealthy") {
        PerCandidateReason::Unhealthy
    } else {
        PerCandidateReason::RejectedByPlugin
    }
}

/// `UpstreamDialect` adapter that routes both `shape` and
/// `normalize_error` to a single wasmtime `SlotKind::Shape` slot.
/// Snapshots the cell at construction time — see
/// [`WasmtimeFilterPlugin`] for the atomic hot-swap rationale.
pub struct WasmtimeUpstreamDialect {
    cell: Arc<PluginCell>,
    runtime_config: Arc<crate::HotEngineConfig>,
}

impl WasmtimeUpstreamDialect {
    pub fn new(slot: Arc<PluginSlot>, runtime_config: Arc<crate::HotEngineConfig>) -> Self {
        let cell = slot.current.load_full();
        Self {
            cell,
            runtime_config,
        }
    }
}

impl cc_lb_plugin_api::UpstreamDialect for WasmtimeUpstreamDialect {
    fn shape(
        &self,
        ctx: &RequestContext,
        upstream: &cc_lb_plugin_api::Upstream,
        principal: &Principal,
        builder: &mut cc_lb_plugin_api::ShapedRequestBuilder,
    ) -> Result<cc_lb_plugin_api::ShapedRequest, cc_lb_plugin_api::DialectError> {
        let request = host_to_wire_shape_request(
            ctx,
            upstream,
            principal,
            self.runtime_config.cookie_redaction,
        );

        let in_bytes = rkyv::to_bytes::<RkyvError>(&request).map_err(|e| {
            cc_lb_plugin_api::DialectError::UnsupportedRequest {
                reason: format!("rkyv encode ShapeRequest: {e}"),
            }
        })?;

        let out_bytes = crate::cache::call_shape_hook(&self.cell, in_bytes.as_slice())
            .map_err(runtime_error_to_dialect)?;
        let out_bound = self.runtime_config.wire_bounds.output_body_bytes;
        if out_bytes.len() as u64 > out_bound {
            return Err(cc_lb_plugin_api::DialectError::UnsupportedRequest {
                reason: format!(
                    "shape output {} bytes exceeds wire_bounds.output_body_bytes ({})",
                    out_bytes.len(),
                    out_bound
                ),
            });
        }

        let mut aligned = AlignedVec::<16>::with_capacity(out_bytes.len());
        aligned.extend_from_slice(&out_bytes);

        let archived =
            rkyv::access::<cc_lb_plugin_types::ArchivedShapeResponse, RkyvError>(&aligned)
                .map_err(|e| cc_lb_plugin_api::DialectError::UnsupportedRequest {
                    reason: format!("rkyv access ShapeResponse: {e}"),
                })?;

        wire_to_host_shaped_request(
            builder,
            archived,
            upstream,
            self.runtime_config.shape_origin_policy,
            &self.runtime_config.wire_bounds,
        )
    }

    fn normalize_error(
        &self,
        status: http::StatusCode,
        body: &bytes::Bytes,
    ) -> Option<bytes::Bytes> {
        let request = cc_lb_plugin_types::NormalizeErrorRequest {
            status: status.as_u16(),
            body: body.to_vec(),
        };

        let in_bytes = rkyv::to_bytes::<RkyvError>(&request).ok()?;
        let out_bytes =
            crate::cache::call_normalize_error_hook(&self.cell, in_bytes.as_slice()).ok()?;

        let mut aligned = AlignedVec::<16>::with_capacity(out_bytes.len());
        aligned.extend_from_slice(&out_bytes);

        let archived =
            rkyv::access::<cc_lb_plugin_types::ArchivedNormalizeErrorResponse, RkyvError>(&aligned)
                .ok()?;

        let normalize_cap = self.runtime_config.wire_bounds.normalize_error_body_bytes;
        // Walk archived directly; ArchivedOption::as_ref → Option<&ArchivedVec<u8>>.
        // Only the surviving body needs an owned copy for Bytes.
        archived.normalized.as_ref().and_then(|archived_body| {
            let body_slice: &[u8] = archived_body.as_slice();
            if body_slice.len() as u64 > normalize_cap {
                tracing::warn!(
                    body_len = body_slice.len(),
                    cap = normalize_cap,
                    "normalize_error output body exceeds wire_bounds cap; dropping"
                );
                None
            } else {
                Some(bytes::Bytes::copy_from_slice(body_slice))
            }
        })
    }
}

fn runtime_error_to_dialect(err: WasmtimeRuntimeError) -> cc_lb_plugin_api::DialectError {
    match err {
        WasmtimeRuntimeError::GuestTrap { phase, source } => {
            cc_lb_plugin_api::DialectError::UnsupportedRequest {
                reason: format!("{phase}: {source}"),
            }
        }
        other => cc_lb_plugin_api::DialectError::UnsupportedRequest {
            reason: other.to_string(),
        },
    }
}

fn host_to_wire_shape_request(
    ctx: &RequestContext,
    upstream: &cc_lb_plugin_api::Upstream,
    principal: &Principal,
    cookie_redaction: bool,
) -> cc_lb_plugin_types::ShapeRequest {
    cc_lb_plugin_types::ShapeRequest {
        request_id: ctx.request_id.clone(),
        method: ctx.method.as_str().to_owned(),
        path: ctx.path.clone(),
        query: ctx.query.clone(),
        headers: ctx
            .downstream_headers
            .iter()
            .filter(|(name, _)| !is_stripped_downstream_header(name.as_str(), cookie_redaction))
            .map(|(name, value)| WireHeader {
                name: name.as_str().to_owned(),
                value: value.as_bytes().to_vec(),
            })
            .collect(),
        body: ctx.body_bytes.to_vec(),
        principal: WirePrincipal {
            id: principal.id.clone(),
            kind: principal_kind_to_wire(principal),
            claims: claims_to_wire(&principal.claims),
        },
        upstream: host_upstream_to_wire(upstream),
    }
}

fn host_upstream_to_wire(upstream: &cc_lb_plugin_api::Upstream) -> cc_lb_plugin_types::Upstream {
    match upstream {
        cc_lb_plugin_api::Upstream::AnthropicDirect { base_url } => {
            cc_lb_plugin_types::Upstream::AnthropicDirect {
                base_url: base_url.as_ref().map(|u| u.to_string()),
            }
        }
    }
}

/// Return the base URL the selected upstream expects the shaped
/// request to reach. Returns `None` when the upstream variant has no
/// pinned host (e.g. an operator-configured `None` override).
/// Callers that need origin equality treat `None` as "policy cannot
/// be enforced for this upstream" and skip the guard.
fn upstream_base_url(upstream: &cc_lb_plugin_api::Upstream) -> Option<url::Url> {
    match upstream {
        cc_lb_plugin_api::Upstream::AnthropicDirect { base_url } => base_url.clone(),
    }
}

fn wire_to_host_shaped_request(
    builder: &mut cc_lb_plugin_api::ShapedRequestBuilder,
    archived: &cc_lb_plugin_types::ArchivedShapeResponse,
    upstream: &cc_lb_plugin_api::Upstream,
    origin_policy: crate::policy::ShapeOriginPolicy,
    wire_bounds: &crate::policy::PluginWireBounds,
) -> Result<cc_lb_plugin_api::ShapedRequest, cc_lb_plugin_api::DialectError> {
    // Enforce max_headers before parsing: a plugin returning
    // 100k headers should not force the host to parse them all.
    if archived.headers.len() as u32 > wire_bounds.max_headers {
        return Err(cc_lb_plugin_api::DialectError::UnsupportedRequest {
            reason: format!(
                "shape plugin returned {} headers, exceeds wire_bounds.max_headers ({})",
                archived.headers.len(),
                wire_bounds.max_headers,
            ),
        });
    }

    let url_str: &str = &archived.url;
    let url = url::Url::parse(url_str)?;

    // Origin guard: only enforced when policy is `SelectedUpstreamOrigin`.
    // Default `Unrestricted` preserves pre-Sprint-3 behaviour so shape
    // plugins that legitimately route to an alternate host (gateway,
    // subdomain, test endpoint) keep working without a config change.
    if matches!(
        origin_policy,
        crate::policy::ShapeOriginPolicy::SelectedUpstreamOrigin
    ) && let Some(expected) = upstream_base_url(upstream)
    {
        let expected_origin = expected.origin();
        let actual_origin = url.origin();
        if expected_origin != actual_origin {
            return Err(cc_lb_plugin_api::DialectError::UnsupportedRequest {
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
        cc_lb_plugin_api::DialectError::UnsupportedRequest {
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
            return Err(cc_lb_plugin_api::DialectError::UnsupportedRequest {
                reason: format!(
                    "shape plugin header `{h_name}` value {} bytes exceeds wire_bounds.max_header_value_bytes ({})",
                    h_value.len(),
                    wire_bounds.max_header_value_bytes,
                ),
            });
        }
        let name = http::HeaderName::from_bytes(h_name.as_bytes()).map_err(|e| {
            cc_lb_plugin_api::DialectError::UnsupportedRequest {
                reason: format!("plugin returned invalid header name `{h_name}`: {e}"),
            }
        })?;
        let value = http::HeaderValue::from_bytes(h_value).map_err(|e| {
            cc_lb_plugin_api::DialectError::UnsupportedRequest {
                reason: format!("plugin returned invalid header value for `{h_name}`: {e}"),
            }
        })?;
        headers.append(name, value);
    }

    let body: &[u8] = &archived.body;
    Ok(builder.shaped_request(url, method, headers, bytes::Bytes::copy_from_slice(body)))
}

/// `ObservabilityHook` adapter for a wasmtime `SlotKind::Observe`
/// slot. Snapshots the cell at construction time — see
/// [`WasmtimeFilterPlugin`] for the atomic hot-swap rationale.
pub struct WasmtimeObservabilityHookPlugin {
    cell: Arc<PluginCell>,
}

impl WasmtimeObservabilityHookPlugin {
    pub fn new(slot: Arc<PluginSlot>, runtime_config: Arc<crate::HotEngineConfig>) -> Self {
        let _ = runtime_config;
        let cell = slot.current.load_full();
        Self { cell }
    }
}

impl cc_lb_plugin_api::ObservabilityHook for WasmtimeObservabilityHookPlugin {
    fn observe(
        &self,
        event: cc_lb_plugin_api::ObserveEvent,
    ) -> Result<(), cc_lb_plugin_api::ObservabilityError> {
        let wire = host_observe_event_to_wire(event);
        let in_bytes = rkyv::to_bytes::<RkyvError>(&wire).map_err(|e| {
            cc_lb_plugin_api::ObservabilityError::Dropped {
                reason: format!("rkyv encode ObserveEvent: {e}"),
            }
        })?;
        crate::cache::call_observe_hook(&self.cell, in_bytes.as_slice()).map_err(|e| {
            cc_lb_plugin_api::ObservabilityError::Dropped {
                reason: e.to_string(),
            }
        })?;
        Ok(())
    }
}

fn host_observe_event_to_wire(
    event: cc_lb_plugin_api::ObserveEvent,
) -> cc_lb_plugin_types::ObserveEvent {
    use cc_lb_plugin_api::ObserveEvent as Host;
    use cc_lb_plugin_types::ObserveEvent as Wire;
    match event {
        Host::RequestStarted {
            request_id,
            downstream_user_agent,
        } => Wire::RequestStarted {
            request_id,
            downstream_user_agent,
        },
        Host::AuthnComplete { principal_id, kind } => Wire::AuthnComplete {
            principal_id,
            principal_kind: serde_json::to_value(&kind)
                .ok()
                .and_then(|v| v.as_str().map(ToOwned::to_owned))
                .unwrap_or_else(|| "unknown".to_owned()),
        },
        Host::UpstreamChosen { upstream } => Wire::UpstreamChosen {
            upstream: host_upstream_to_wire(&upstream),
        },
        Host::Chunk {
            batch_index,
            event_count,
            total_bytes,
        } => Wire::Chunk {
            batch_index,
            event_count: event_count as u64,
            total_bytes: total_bytes as u64,
        },
        Host::RequestFinished {
            status,
            input_tokens,
            output_tokens,
            cache_creation_input_tokens,
            cache_read_input_tokens,
            duration_ms,
        } => Wire::RequestFinished {
            status: status.as_u16(),
            input_tokens,
            output_tokens,
            cache_creation_input_tokens,
            cache_read_input_tokens,
            duration_ms,
        },
        Host::Error {
            code,
            message,
            source,
        } => Wire::Error {
            code,
            message,
            source,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cc_lb_plugin_api::PrincipalKind;
    use cc_lb_plugin_types::FilterResponse as WireFilterResponse;
    use cc_lb_plugin_types::PerCandidateReason as WirePerCandidateReason;

    fn fixture_principal() -> Principal {
        let mut claims = serde_json::Map::new();
        claims.insert("scope".to_owned(), serde_json::Value::from("inference"));
        Principal {
            id: "tenant-a".to_owned(),
            kind: PrincipalKind::ApiKey,
            claims,
        }
    }

    fn fixture_request() -> RequestContext {
        let mut headers = http::HeaderMap::new();
        headers.insert(
            http::header::CONTENT_TYPE,
            http::HeaderValue::from_static("application/json"),
        );
        headers.insert(
            http::header::AUTHORIZATION,
            http::HeaderValue::from_static("Bearer secret"),
        );
        RequestContext {
            request_id: "req-123".to_owned(),
            downstream_headers: headers,
            method: http::Method::POST,
            path: "/v1/messages".to_owned(),
            query: None,
            body_bytes: bytes::Bytes::from_static(b"{\"msg\":\"hi\"}"),
            cache_breakpoints: Vec::new(),
            canonical_model_id: "claude-fixture".to_owned(),
        }
    }

    #[test]
    fn host_to_wire_strips_auth_headers() {
        let principal = fixture_principal();
        let ctx = fixture_request();
        let wire = host_to_wire_request(&ctx, &principal, &[], false);

        assert_eq!(wire.request_id, "req-123");
        assert_eq!(wire.method, "POST");
        assert_eq!(wire.path, "/v1/messages");
        assert_eq!(wire.headers.len(), 1, "authorization must be filtered out");
        assert_eq!(wire.headers[0].name, "content-type");
        assert_eq!(wire.principal.id, "tenant-a");
        assert_eq!(wire.principal.kind, "api_key");
        assert!(wire.principal.claims.iter().any(|(k, _)| k == "scope"));
    }

    #[test]
    fn wire_to_host_splits_kept_and_rejected() {
        let response = WireFilterResponse {
            results: vec![
                WirePerCandidateReason {
                    upstream_id: "11111111-1111-1111-1111-111111111111".to_owned(),
                    decision: "accept".to_owned(),
                    reason: "top-K".to_owned(),
                },
                WirePerCandidateReason {
                    upstream_id: "22222222-2222-2222-2222-222222222222".to_owned(),
                    decision: "rate-limit".to_owned(),
                    reason: "burst exceeded".to_owned(),
                },
            ],
        };
        let bytes = rkyv::to_bytes::<RkyvError>(&response).expect("encode");
        let mut aligned = AlignedVec::<16>::with_capacity(bytes.len());
        aligned.extend_from_slice(&bytes);
        let archived =
            rkyv::access::<ArchivedFilterResponse, RkyvError>(&aligned).expect("archived view");
        let out = wire_to_host_output(archived, 256).expect("conversion must succeed");
        assert_eq!(out.kept_upstream_ids.len(), 1);
        assert_eq!(
            out.kept_upstream_ids[0],
            Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap()
        );
        assert_eq!(out.per_candidate_reasons.len(), 1);
        assert_eq!(
            out.per_candidate_reasons[0],
            PerCandidateReason::RateLimited
        );
    }
}
