//! Candidate-key preflight (`POST /admin/v1/upstreams/preflight`) and the
//! key-check step shared by API-key create and PUT.
//!
//! Every input is validated locally before any outbound request: the key is
//! normalized and prefix-gated, `base_url` must be http(s) without userinfo,
//! and a workspace id must be well formed. Only then does the probe chain run.

use axum::{
    Extension, Json,
    extract::{State, rejection::JsonRejection},
    http::{HeaderValue, Method, StatusCode, header},
    response::{IntoResponse, Response},
};
use bytes::Bytes;
use cc_lb_control::AuditPayload;
use cc_lb_control::anthropic_api_key::{
    KeyInputError, NormalizedApiKey, PreflightCapabilities, PreflightClassification,
    PreflightInput, PreflightMode, PreflightOutcome, PreflightRequest, PreflightVerdict,
    PreflightWireResponse, PreflightWorkspaces, ProbeTransport, WireInference,
    endpoint_operator_message, http_status, key_hint, normalize_api_key, operator_message,
    prefix_gate, run_preflight, validate_base_url, validate_workspace_id,
};
use cc_lb_storage_api::{
    ApiKeyAccessWrite, ApiKeyCapabilities, ApiKeyClass, ApiKeyInference, ApiKeyOrgCapability,
    ApiKeyResolution, ApiKeyWorkspace, ApiKeyWorkspaceMode, RemediationCode, UpstreamKind,
    UpstreamRecord,
};
use http_body_util::{BodyExt, Full, Limited};
use serde_json::json;
use url::Url;

use super::recheck::stored_api_key;
use crate::AdminState;
use crate::auth::AdminIdentity;
use crate::v1::upstreams::{UpstreamError, audit_upstream_action, effective_base_url};

pub(crate) const PREFLIGHT_ROUTE: &str = "/admin/v1/upstreams/preflight";

/// `POST /admin/v1/upstreams/preflight`: probes a pasted key. The verdict is
/// data (always 200); only request-shape errors are 4xx. Nothing but the
/// audit row is persisted. Every answer carries `Cache-Control: no-store`.
pub(crate) async fn preflight_candidate(
    State(state): State<AdminState>,
    Extension(identity): Extension<AdminIdentity>,
    body: Result<Json<PreflightRequest>, JsonRejection>,
) -> Response {
    let response = match body {
        Ok(Json(body)) => match preflight(&state, &identity, body).await {
            Ok(response) => response,
            Err(error) => error.into_response(),
        },
        Err(rejection) => (
            rejection.status(),
            Json(json!({
                "error": "validation_failed",
                "field": null,
                "message": rejection.body_text(),
            })),
        )
            .into_response(),
    };
    no_store(response)
}

/// Marks a response as never cacheable.
pub(crate) fn no_store(mut response: Response) -> Response {
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

async fn preflight(
    state: &AdminState,
    identity: &AdminIdentity,
    body: PreflightRequest,
) -> Result<Response, UpstreamError> {
    let kind = body.kind.unwrap_or(UpstreamKind::AnthropicApiKey);
    if !matches!(
        kind,
        UpstreamKind::AnthropicApiKey | UpstreamKind::OpenaiApiKey | UpstreamKind::GeminiApiKey
    ) {
        return Err(UpstreamError::NotApiKeyUpstream);
    }
    let key = normalize_key_input(body.api_key_value.as_deref())?;
    let base_url = match body
        .base_url
        .as_deref()
        .map(str::trim)
        .filter(|raw| !raw.is_empty())
    {
        Some(raw) => {
            let url = Url::parse(raw).map_err(|_| UpstreamError::InvalidInput {
                field: "base_url".to_owned(),
                reason: "base_url must be an absolute http or https URL".to_owned(),
            })?;
            validate_base_url_input(&url)?;
            url
        }
        None => effective_base_url(None, kind)?,
    };
    let workspace_id = body.workspace_id.as_deref().filter(|id| !id.is_empty());
    let now_unix_secs = now_unix_secs(state);
    let outcome = if matches!(
        kind,
        UpstreamKind::OpenaiApiKey | UpstreamKind::GeminiApiKey
    ) {
        provider_native_preflight(state, &key, &base_url, kind, now_unix_secs).await
    } else {
        run_preflight(
            &state.anthropic_org_api,
            PreflightInput {
                key: &key,
                base_url: &base_url,
                workspace_id,
                mode: PreflightMode::Candidate,
                now_unix_secs,
            },
        )
        .await
    };
    let wire = outcome.wire;
    let payload = AuditPayload::UpstreamPreflight {
        key_hint: wire.key_hint.clone(),
        verdict: verdict_label(wire.verdict).to_owned(),
        remediation: wire
            .remediation
            .iter()
            .map(|remediation| remediation.code.as_str())
            .collect(),
    };
    audit_upstream_action(
        state,
        identity,
        &payload,
        PREFLIGHT_ROUTE,
        None,
        StatusCode::OK,
    )
    .await?;
    Ok(Json(wire).into_response())
}

pub(crate) async fn provider_native_preflight(
    state: &AdminState,
    key: &NormalizedApiKey,
    base_url: &Url,
    kind: UpstreamKind,
    now_unix_secs: u64,
) -> PreflightOutcome {
    let url = provider_probe_url(base_url, kind);
    let header_value = match kind {
        UpstreamKind::OpenaiApiKey => format!("Bearer {}", key.as_str()),
        UpstreamKind::GeminiApiKey => key.as_str().to_owned(),
        _ => {
            return provider_outcome(
                key,
                now_unix_secs,
                ProviderProbeResult::Rejected {
                    code: RemediationCode::KeyInvalid,
                },
            );
        }
    };
    let header_name = match kind {
        UpstreamKind::OpenaiApiKey => "authorization",
        UpstreamKind::GeminiApiKey => "x-goog-api-key",
        _ => {
            return provider_outcome(
                key,
                now_unix_secs,
                ProviderProbeResult::Rejected {
                    code: RemediationCode::KeyInvalid,
                },
            );
        }
    };
    let header_value = match HeaderValue::from_str(&header_value) {
        Ok(value) => value,
        Err(_) => {
            return provider_outcome(
                key,
                now_unix_secs,
                ProviderProbeResult::Unreachable { timed_out: false },
            );
        }
    };
    let request = match axum::http::Request::builder()
        .method(Method::GET)
        .uri(url.as_str())
        .header(header_name, header_value)
        .body(Full::new(Bytes::new()))
    {
        Ok(request) => request,
        Err(_) => {
            return provider_outcome(
                key,
                now_unix_secs,
                ProviderProbeResult::Unreachable { timed_out: false },
            );
        }
    };
    let exchange = async {
        let response = state
            .anthropic_org_api
            .http()
            .request(request)
            .await
            .map_err(|_| ())?;
        let status = response.status();
        Limited::new(
            response.into_body(),
            state.anthropic_org_api.limits().max_body_bytes,
        )
        .collect()
        .await
        .map_err(|_| ())?;
        Ok::<StatusCode, ()>(status)
    };
    let result =
        match tokio::time::timeout(state.anthropic_org_api.limits().per_probe, exchange).await {
            Ok(Ok(status)) if status == StatusCode::OK => ProviderProbeResult::Ok,
            Ok(Ok(status)) if status == StatusCode::UNAUTHORIZED => ProviderProbeResult::Rejected {
                code: RemediationCode::KeyInvalid,
            },
            Ok(Ok(status)) if status == StatusCode::FORBIDDEN => ProviderProbeResult::Rejected {
                code: RemediationCode::KeyForbidden,
            },
            Ok(Ok(status)) if status == StatusCode::TOO_MANY_REQUESTS => {
                ProviderProbeResult::RateLimited
            }
            Ok(Ok(status)) if status.is_server_error() => {
                ProviderProbeResult::Unreachable { timed_out: false }
            }
            Ok(Ok(_)) => ProviderProbeResult::Rejected {
                code: RemediationCode::KeyInvalid,
            },
            Ok(Err(())) => ProviderProbeResult::Unreachable { timed_out: false },
            Err(_) => ProviderProbeResult::Unreachable { timed_out: true },
        };
    provider_outcome(key, now_unix_secs, result)
}

#[derive(Debug, Clone, Copy)]
enum ProviderProbeResult {
    Ok,
    Rejected { code: RemediationCode },
    RateLimited,
    Unreachable { timed_out: bool },
}

fn provider_probe_url(base_url: &Url, kind: UpstreamKind) -> Url {
    let mut url = base_url.clone();
    url.set_query(None);
    url.set_fragment(None);
    let mut path = url.path().trim_end_matches('/').to_owned();
    match kind {
        UpstreamKind::OpenaiApiKey => {
            if path.ends_with("/v1") {
                path.push_str("/models");
            } else {
                path.push_str("/v1/models");
            }
        }
        UpstreamKind::GeminiApiKey => path.push_str("/v1beta/models"),
        _ => {}
    }
    if path.is_empty() {
        path.push('/');
    }
    url.set_path(&path);
    url
}

fn provider_outcome(
    key: &NormalizedApiKey,
    now_unix_secs: u64,
    result: ProviderProbeResult,
) -> PreflightOutcome {
    let hint = key_hint(key.as_str());
    let (verdict, inference, remediation, last_error, checked, transport) = match result {
        ProviderProbeResult::Ok => (
            PreflightVerdict::Ok,
            WireInference::Ok,
            Vec::new(),
            None,
            true,
            None,
        ),
        ProviderProbeResult::Rejected { code } => (
            PreflightVerdict::Rejected,
            WireInference::Denied,
            vec![code],
            Some(match code {
                RemediationCode::KeyForbidden => "provider denied this API key".to_owned(),
                _ => "provider rejected this API key".to_owned(),
            }),
            true,
            None,
        ),
        ProviderProbeResult::RateLimited => (
            PreflightVerdict::Ok,
            WireInference::OkLimited,
            Vec::new(),
            None,
            true,
            None,
        ),
        ProviderProbeResult::Unreachable { timed_out } => (
            PreflightVerdict::Unreachable,
            WireInference::Unverified,
            vec![RemediationCode::ProbeUnreachable],
            Some(if timed_out {
                "API-key probe timed out".to_owned()
            } else {
                "could not reach the provider API".to_owned()
            }),
            false,
            Some(ProbeTransport { timed_out }),
        ),
    };
    let workspace = ApiKeyWorkspace {
        mode: ApiKeyWorkspaceMode::Required,
        id: None,
        name: None,
    };
    let capabilities = ApiKeyCapabilities {
        inference: match inference {
            WireInference::Ok | WireInference::OkLimited => ApiKeyInference::Ok,
            WireInference::Denied => ApiKeyInference::Denied,
            WireInference::Unverified => ApiKeyInference::Unverified,
        },
        workspace: workspace.clone(),
        workspace_list: ApiKeyOrgCapability::NotApplicable,
        api_key_resolution: ApiKeyResolution::NotApplicable,
        key_type: None,
    };
    let wire = PreflightWireResponse {
        verdict,
        key_hint: hint.clone(),
        classification: PreflightClassification {
            key_class: ApiKeyClass::Unknown,
            needs_workspace: false,
            workspace,
            key_type: None,
        },
        workspaces: PreflightWorkspaces {
            state: ApiKeyOrgCapability::NotApplicable,
            items: Vec::new(),
        },
        capabilities: PreflightCapabilities {
            inference,
            workspace_list: ApiKeyOrgCapability::NotApplicable,
            usage_report: ApiKeyOrgCapability::NotApplicable,
            api_key_resolution: ApiKeyResolution::NotApplicable,
        },
        remediation: remediation
            .iter()
            .copied()
            .map(|code| cc_lb_control::anthropic_api_key::Remediation { code })
            .collect(),
        checked_at: now_unix_secs,
    };
    PreflightOutcome {
        wire,
        access: ApiKeyAccessWrite {
            verified: verdict == PreflightVerdict::Ok,
            key_class: ApiKeyClass::Unknown,
            key_hint: hint.unwrap_or_default(),
            api_key_id: None,
            needs_workspace: false,
            capabilities,
            remediation,
            usage_report_state: Some(ApiKeyOrgCapability::NotApplicable),
            last_error,
            checked_at_unix_secs: checked.then_some(now_unix_secs),
            last_attempt_at_unix_secs: now_unix_secs,
        },
        transport,
    }
}

/// Result of checking a key before it is stored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KeyCheck {
    /// Capability snapshot written to the details row.
    pub(crate) access: ApiKeyAccessWrite,
    /// Workspace the spec stores: the selected one, otherwise none.
    pub(crate) workspace_id: Option<String>,
    /// `None` when the probe was skipped.
    pub(crate) verdict: Option<PreflightVerdict>,
}

impl KeyCheck {
    /// `supplied` is the workspace the operator chose (or the stored one for a
    /// host-only re-probe). A key the probe could not classify (`unknown`,
    /// e.g. on a custom host that sends no workspace header) keeps it: only
    /// a positively workspace-bound key rejects an explicit workspace.
    fn from_outcome(mut outcome: PreflightOutcome, supplied: Option<&str>) -> Self {
        if outcome.access.key_class == ApiKeyClass::Unknown
            && let Some(id) = supplied
        {
            outcome.access.capabilities.workspace = ApiKeyWorkspace {
                mode: ApiKeyWorkspaceMode::Selected,
                id: Some(id.to_owned()),
                name: None,
            };
        }
        let workspace = &outcome.access.capabilities.workspace;
        let workspace_id = (workspace.mode == ApiKeyWorkspaceMode::Selected)
            .then(|| workspace.id.clone())
            .flatten();
        Self {
            access: outcome.access,
            workspace_id,
            verdict: Some(outcome.wire.verdict),
        }
    }

    /// Snapshot stored by `skip_preflight`: nothing is known about the key,
    /// so it reads as "not checked" until a Re-check.
    pub(crate) fn unverified(
        key: &NormalizedApiKey,
        workspace_id: Option<&str>,
        now_unix_secs: u64,
    ) -> Self {
        let mut capabilities = ApiKeyCapabilities::not_checked();
        if let Some(id) = workspace_id {
            capabilities.workspace = ApiKeyWorkspace {
                mode: ApiKeyWorkspaceMode::Selected,
                id: Some(id.to_owned()),
                name: None,
            };
        }
        Self {
            access: ApiKeyAccessWrite {
                verified: false,
                key_class: ApiKeyClass::Unknown,
                key_hint: key_hint(key.as_str()).unwrap_or_default(),
                api_key_id: None,
                needs_workspace: false,
                capabilities,
                remediation: vec![RemediationCode::UsageNeverChecked],
                usage_report_state: None,
                last_error: None,
                checked_at_unix_secs: None,
                last_attempt_at_unix_secs: now_unix_secs,
            },
            workspace_id: workspace_id.map(ToOwned::to_owned),
            verdict: None,
        }
    }

    pub(crate) fn key_hint(&self) -> Option<String> {
        Some(self.access.key_hint.clone()).filter(|hint| !hint.is_empty())
    }

    pub(crate) fn verdict_label(&self) -> Option<String> {
        self.verdict
            .map(|verdict| verdict_label(verdict).to_owned())
    }

    pub(crate) fn remediation_labels(&self) -> Vec<&'static str> {
        self.access
            .remediation
            .iter()
            .map(|code| code.as_str())
            .collect()
    }

    /// `checked_at` of a check that found usage reports readable; such a
    /// check enqueues an immediate usage poll.
    pub(crate) fn usage_reported_at(&self) -> Option<u64> {
        (self.access.usage_report_state == Some(ApiKeyOrgCapability::Ok))
            .then_some(self.access.checked_at_unix_secs)
            .flatten()
    }
}

/// A validated API-key create: the key and its check.
pub(crate) struct PreparedApiKeyCreate {
    pub(crate) key: NormalizedApiKey,
    pub(crate) check: KeyCheck,
}

/// Validates and checks the key of an API-key create. Failures persist
/// nothing; P0 applies even with `skip_preflight`.
pub(crate) async fn prepare_api_key_create(
    state: &AdminState,
    kind: UpstreamKind,
    api_key_value: Option<&str>,
    base_url: Option<&Url>,
    workspace_id: Option<&str>,
    skip_preflight: bool,
) -> Result<PreparedApiKeyCreate, UpstreamError> {
    let key = normalize_key_input(api_key_value)?;
    if let Some(id) = workspace_id {
        validate_workspace_input(id)?;
    }
    gate_key(&key)?;
    let now = now_unix_secs(state);
    let _ = effective_base_url(base_url, kind)?;
    let check = if skip_preflight || kind != UpstreamKind::AnthropicApiKey {
        KeyCheck::unverified(&key, workspace_id, now)
    } else {
        let base_url = effective_base_url(base_url, kind)?;
        probe_strict(
            state,
            &key,
            &base_url,
            workspace_id,
            workspace_id.is_some(),
            now,
        )
        .await?
    };
    Ok(PreparedApiKeyCreate { key, check })
}

/// The API-key part of a PUT.
#[derive(Default)]
pub(crate) struct PreparedApiKeyUpdate {
    /// The replacement key, when `api_key_value` was sent.
    pub(crate) new_key: Option<NormalizedApiKey>,
    /// `Some` when the spec's workspace changes.
    pub(crate) workspace_id: Option<Option<String>>,
    /// `Some` when the key was (re-)checked or stored unverified.
    pub(crate) check: Option<KeyCheck>,
    /// Key, workspace, or host changed: the usage-poll state restarts.
    pub(crate) reset_poll_state: bool,
}

/// What a PUT asks of an API-key upstream's key, workspace, and host.
pub(crate) struct ApiKeyUpdateRequest<'a> {
    pub(crate) api_key_value: Option<&'a str>,
    pub(crate) workspace_id: Option<&'a str>,
    pub(crate) clear_workspace_id: bool,
    /// The spec's `base_url` after the PUT.
    pub(crate) base_url: Option<&'a Url>,
    pub(crate) skip_preflight: bool,
}

/// Validates a PUT on an API-key upstream and re-probes when the key, the
/// workspace, or the host changes.
///
/// A new key or workspace must pass the check (failures persist nothing). A
/// `base_url`-only change re-probes the stored key at the new host and stores
/// whatever it finds; it never fails the PUT.
pub(crate) async fn prepare_api_key_update(
    state: &AdminState,
    current: &UpstreamRecord,
    request: ApiKeyUpdateRequest<'_>,
) -> Result<PreparedApiKeyUpdate, UpstreamError> {
    let new_key = request
        .api_key_value
        .map(|raw| normalize_key_input(Some(raw)))
        .transpose()?;
    if let Some(id) = request.workspace_id {
        validate_workspace_input(id)?;
    }
    if let Some(key) = new_key.as_ref() {
        gate_key(key)?;
    }
    let target_workspace = if request.clear_workspace_id {
        None
    } else {
        request
            .workspace_id
            .map(ToOwned::to_owned)
            .or_else(|| current.workspace_id.clone())
    };
    let workspace_changed = target_workspace != current.workspace_id;
    let host_changed = request.base_url != current.base_url.as_ref();
    if new_key.is_none() && !workspace_changed && !host_changed {
        return Ok(PreparedApiKeyUpdate::default());
    }

    let now = now_unix_secs(state);
    let stored_key;
    let key = match new_key.as_ref() {
        Some(key) => key,
        None => {
            stored_key = stored_api_key(state, current)?;
            &stored_key
        }
    };
    let base_url = effective_base_url(request.base_url, current.kind)?;
    let check = if request.skip_preflight || current.kind != UpstreamKind::AnthropicApiKey {
        KeyCheck::unverified(key, target_workspace.as_deref(), now)
    } else if new_key.is_some() || workspace_changed {
        probe_strict(
            state,
            key,
            &base_url,
            target_workspace.as_deref(),
            request.workspace_id.is_some(),
            now,
        )
        .await?
    } else {
        // Host-only change: probe the stored key exactly as the signer will
        // send it and keep the workspace (D-Z8).
        let outcome = run_preflight(
            &state.anthropic_org_api,
            PreflightInput {
                key,
                base_url: &base_url,
                workspace_id: None,
                mode: PreflightMode::StoredKey {
                    stored_workspace_id: current.workspace_id.as_deref(),
                },
                now_unix_secs: now,
            },
        )
        .await;
        let mut check = KeyCheck::from_outcome(outcome, current.workspace_id.as_deref());
        check.workspace_id = current.workspace_id.clone();
        check
    };
    let workspace_id =
        (check.workspace_id != current.workspace_id).then(|| check.workspace_id.clone());
    Ok(PreparedApiKeyUpdate {
        new_key,
        workspace_id,
        check: Some(check),
        reset_poll_state: true,
    })
}

/// Runs P0-P4 for a candidate and accepts only `ok`. `explicit_workspace`
/// marks a workspace the operator sent, which a bound key refuses (D-Z4).
async fn probe_strict(
    state: &AdminState,
    key: &NormalizedApiKey,
    base_url: &Url,
    workspace_id: Option<&str>,
    explicit_workspace: bool,
    now_unix_secs: u64,
) -> Result<KeyCheck, UpstreamError> {
    let outcome = run_preflight(
        &state.anthropic_org_api,
        PreflightInput {
            key,
            base_url,
            workspace_id,
            mode: PreflightMode::Candidate,
            now_unix_secs,
        },
    )
    .await;
    match outcome.wire.verdict {
        PreflightVerdict::Ok => {
            if explicit_workspace && outcome.access.key_class == ApiKeyClass::WorkspaceBound {
                return Err(UpstreamError::InvalidInput {
                    field: "workspace_id".to_owned(),
                    reason: "this key sets its own workspace; remove workspace_id".to_owned(),
                });
            }
            Ok(KeyCheck::from_outcome(outcome, workspace_id))
        }
        PreflightVerdict::NeedsWorkspace | PreflightVerdict::Rejected => {
            let code = outcome
                .wire
                .remediation
                .first()
                .map(|remediation| remediation.code)
                .unwrap_or(RemediationCode::KeyInvalid);
            let on_anthropic = state.anthropic_org_api.applies_to(base_url);
            Err(UpstreamError::Remediation {
                status: http_status(code).unwrap_or(StatusCode::BAD_REQUEST),
                code,
                message: endpoint_operator_message(code, on_anthropic).to_owned(),
            })
        }
        PreflightVerdict::Unreachable => Err(remediation_error(
            RemediationCode::ProbeUnreachable,
            outcome
                .transport
                .is_some_and(|transport| transport.timed_out),
        )),
    }
}

/// Nested remediation failure for `code`; an unreachable probe that timed out
/// answers 504, other unreachable probes 502.
pub(crate) fn remediation_error(code: RemediationCode, timed_out: bool) -> UpstreamError {
    let status = if code == RemediationCode::ProbeUnreachable && timed_out {
        StatusCode::GATEWAY_TIMEOUT
    } else {
        http_status(code).unwrap_or(StatusCode::BAD_REQUEST)
    };
    UpstreamError::Remediation {
        status,
        code,
        message: operator_message(code).to_owned(),
    }
}

/// D-INP01/02: trim, then refuse an empty, non-printable, or oversized key.
pub(crate) fn normalize_key_input(raw: Option<&str>) -> Result<NormalizedApiKey, UpstreamError> {
    normalize_api_key(raw.unwrap_or_default()).map_err(|error| match error {
        KeyInputError::Missing => UpstreamError::BadRequest {
            error: "missing_api_key",
            detail: "anthropic_api_key upstreams require api_key_value".to_owned(),
        },
        KeyInputError::InvalidCharacters => UpstreamError::InvalidInput {
            field: "api_key_value".to_owned(),
            reason: "API keys contain no spaces or line breaks".to_owned(),
        },
        KeyInputError::TooLong => UpstreamError::InvalidInput {
            field: "api_key_value".to_owned(),
            reason: "API keys are at most 512 bytes".to_owned(),
        },
    })
}

/// P0: admin, enterprise, and OAuth tokens are refused on every host.
pub(crate) fn gate_key(key: &NormalizedApiKey) -> Result<(), UpstreamError> {
    match prefix_gate(key) {
        Some((_, code)) => Err(remediation_error(code, false)),
        None => Ok(()),
    }
}

/// D-SSRF03: every written `base_url` is http(s) without userinfo.
pub(crate) fn validate_base_url_input(base_url: &Url) -> Result<(), UpstreamError> {
    validate_base_url(base_url).map_err(|reason| UpstreamError::InvalidInput {
        field: "base_url".to_owned(),
        reason: reason.to_owned(),
    })
}

/// A malformed workspace id is refused locally and never sent.
pub(crate) fn validate_workspace_input(workspace_id: &str) -> Result<(), UpstreamError> {
    if validate_workspace_id(workspace_id) {
        Ok(())
    } else {
        Err(remediation_error(
            RemediationCode::WorkspaceIdMalformed,
            false,
        ))
    }
}

fn now_unix_secs(state: &AdminState) -> u64 {
    cc_lb_clock::unix_secs(state.clock.now())
}

pub(crate) const fn verdict_label(verdict: PreflightVerdict) -> &'static str {
    match verdict {
        PreflightVerdict::Ok => "ok",
        PreflightVerdict::NeedsWorkspace => "needs_workspace",
        PreflightVerdict::Rejected => "rejected",
        PreflightVerdict::Unreachable => "unreachable",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input_error(raw: &str) -> Option<String> {
        normalize_key_input(Some(raw))
            .err()
            .map(|error| error.error_code().to_owned())
    }

    /// GATE-08 (T0): the outer trim is accepted; any other control or
    /// non-ASCII character is refused with `invalid_input`.
    #[test]
    fn gate_08_whitespace_and_control_characters() {
        assert_eq!(
            normalize_key_input(Some(" sk-ant-api03-abc\n"))
                .map(|key| key.as_str().to_owned())
                .ok(),
            Some("sk-ant-api03-abc".to_owned())
        );
        for raw in [
            "sk-ant-api03 abc",
            "sk-ant-api03-a\r\nb",
            "sk-ant-api03-a\0b",
            "sk-ant-api03-äbc",
            "sk-ant-api03-a\u{200b}b",
            "Bearer sk-ant-oat01-x",
        ] {
            assert_eq!(
                input_error(raw).as_deref(),
                Some("invalid_input"),
                "{raw:?}"
            );
        }
    }

    /// GATE-09 (T0): empty is `missing_api_key`, over 512 bytes is
    /// `invalid_input`.
    #[test]
    fn gate_09_key_size() {
        assert_eq!(input_error("").as_deref(), Some("missing_api_key"));
        assert_eq!(input_error("   ").as_deref(), Some("missing_api_key"));
        assert!(normalize_key_input(Some("x")).is_ok());
        assert!(normalize_key_input(Some(&"x".repeat(512))).is_ok());
        assert_eq!(
            input_error(&"x".repeat(513)).as_deref(),
            Some("invalid_input")
        );
        assert_eq!(
            normalize_key_input(None)
                .err()
                .map(|error| error.error_code()),
            Some("missing_api_key")
        );
    }

    #[test]
    fn unreachable_status_depends_on_timeout() {
        let status =
            |timed_out| remediation_error(RemediationCode::ProbeUnreachable, timed_out).status();
        assert_eq!(status(false), StatusCode::BAD_GATEWAY);
        assert_eq!(status(true), StatusCode::GATEWAY_TIMEOUT);
        assert_eq!(
            remediation_error(RemediationCode::WorkspaceRequired, false).status(),
            StatusCode::BAD_REQUEST
        );
    }

    #[test]
    fn unverified_check_reads_not_checked() {
        let key = normalize_api_key("sk-ant-api03-abcdefghijklmnopqrstuvwxyz");
        assert!(key.is_ok());
        let Ok(key) = key else { return };
        let check = KeyCheck::unverified(&key, Some("wrkspc_P"), 42);
        assert!(!check.access.verified);
        assert_eq!(check.access.checked_at_unix_secs, None);
        assert_eq!(check.access.key_hint, "sk-ant-api03-...wxyz");
        assert_eq!(check.workspace_id.as_deref(), Some("wrkspc_P"));
        assert_eq!(check.verdict, None);
        assert_eq!(check.usage_reported_at(), None);
        assert_eq!(check.remediation_labels(), vec!["usage_never_checked"]);
    }
}
