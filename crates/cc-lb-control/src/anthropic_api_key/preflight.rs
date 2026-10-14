//! Preflight / Re-check probe runner (plan §1.7).
//!
//! P0 is the local prefix gate. P1a lists models on the upstream's
//! `base_url` (live model, bound workspace, identity-linked 400). P1b counts
//! tokens with that model and decides inference. P2-P4 (workspaces, usage
//! report, API keys) run only for org-scoped keys whose `base_url` is the org
//! API origin, concurrently under a semaphore and within the total deadline.
//! Every outcome is data: the runner never fails, never logs the key, and
//! scrubs provider text before it is kept.

use bytes::Bytes;
use cc_lb_storage_api::{
    ApiKeyAccessWrite, ApiKeyCapabilities, ApiKeyClass, ApiKeyInference, ApiKeyOrgCapability,
    ApiKeyResolution, ApiKeyType, ApiKeyWorkspace, ApiKeyWorkspaceMode, RemediationCode,
};
use cc_lb_upstream::upstream_request_url;
use http::{Method, StatusCode};
use tokio::sync::Semaphore;
use tokio::time::Instant;
use url::Url;

use super::WARMUP_MODEL;
use super::client::AnthropicOrgApi;
use super::gate::prefix_gate;
use super::hint::key_hint;
use super::normalize::NormalizedApiKey;
use super::org_probe::{OrgApiError, hint_matches};
use super::probe::{
    ProbeFailure, ProbeRequest, ProbeResponse, first_claude_model, is_identity_linked,
    is_malformed_workspace, is_token_count, response_detail, scrubbed, send,
};
use super::wire::{
    AnthropicWorkspace, PreflightCapabilities, PreflightClassification, PreflightVerdict,
    PreflightWireResponse, PreflightWorkspaces, Remediation, WireInference,
};
use super::workspace_id::validate_workspace_id;

/// Which key is probed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PreflightMode<'a> {
    /// A pasted key (create, replace). A bound key ignores a supplied
    /// workspace id.
    Candidate,
    /// The stored key (Re-check). The stored workspace id is sent the way the
    /// signer sends it, so a bound key stored with another workspace is
    /// caught as `workspace_not_found`.
    StoredKey {
        stored_workspace_id: Option<&'a str>,
    },
}

pub struct PreflightInput<'a> {
    pub key: &'a NormalizedApiKey,
    pub base_url: &'a Url,
    /// Workspace the operator chose for a candidate; ignored in
    /// [`PreflightMode::StoredKey`]. A malformed id is never sent.
    pub workspace_id: Option<&'a str>,
    pub mode: PreflightMode<'a>,
    pub now_unix_secs: u64,
}

/// Set when the verdict is `unreachable`; `timed_out` lets callers answer
/// 504 instead of 502.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProbeTransport {
    pub timed_out: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreflightOutcome {
    pub wire: PreflightWireResponse,
    pub access: ApiKeyAccessWrite,
    pub transport: Option<ProbeTransport>,
}

/// Runs P0-P4 for one key. Never fails: every result is a verdict.
pub async fn run_preflight(api: &AnthropicOrgApi, input: PreflightInput<'_>) -> PreflightOutcome {
    let key = input.key.as_str();
    let hint = key_hint(key);
    if let Some((key_class, code)) = prefix_gate(input.key) {
        return gated(hint, key_class, code, input.now_unix_secs);
    }

    let deadline = Instant::now() + api.limits().total;
    let (supplied, stored_mode) = match input.mode {
        PreflightMode::Candidate => (input.workspace_id, false),
        PreflightMode::StoredKey {
            stored_workspace_id,
        } => (stored_workspace_id, true),
    };
    let malformed_supplied = supplied.is_some_and(|id| !validate_workspace_id(id));
    let supplied = supplied.filter(|id| validate_workspace_id(id));
    let on_org_origin = api.applies_to(input.base_url);

    let probe = InferenceProbe {
        api,
        deadline,
        key,
        base_url: input.base_url,
        on_org_origin,
    };
    let inference = probe.run(supplied, stored_mode).await;

    let key_class = if inference.identity {
        ApiKeyClass::OrgScoped
    } else if inference.bound.is_some() {
        ApiKeyClass::WorkspaceBound
    } else {
        ApiKeyClass::Unknown
    };
    let usable = matches!(
        inference.result,
        InferenceResult::Works { .. } | InferenceResult::NeedsWorkspace(_)
    );
    let org = if key_class == ApiKeyClass::OrgScoped && usable && on_org_origin {
        Some(probe_org(api, deadline, key, input.now_unix_secs).await)
    } else {
        None
    };

    assemble(Assembly {
        hint,
        key_class,
        inference,
        org,
        malformed_supplied,
        on_org_origin,
        now_unix_secs: input.now_unix_secs,
    })
}

fn gated(
    hint: Option<String>,
    key_class: ApiKeyClass,
    code: RemediationCode,
    now_unix_secs: u64,
) -> PreflightOutcome {
    let capabilities = ApiKeyCapabilities::not_checked();
    let wire = PreflightWireResponse {
        verdict: PreflightVerdict::Rejected,
        key_hint: hint.clone(),
        classification: PreflightClassification {
            key_class,
            needs_workspace: false,
            workspace: capabilities.workspace.clone(),
            key_type: None,
        },
        workspaces: PreflightWorkspaces {
            state: ApiKeyOrgCapability::NotApplicable,
            items: Vec::new(),
        },
        capabilities: PreflightCapabilities {
            inference: WireInference::Unverified,
            workspace_list: ApiKeyOrgCapability::NotApplicable,
            usage_report: ApiKeyOrgCapability::NotApplicable,
            api_key_resolution: ApiKeyResolution::NotApplicable,
        },
        remediation: vec![Remediation { code }],
        checked_at: now_unix_secs,
    };
    let access = ApiKeyAccessWrite {
        verified: false,
        key_class,
        key_hint: hint.unwrap_or_default(),
        api_key_id: None,
        needs_workspace: false,
        capabilities,
        remediation: vec![code],
        usage_report_state: Some(ApiKeyOrgCapability::NotApplicable),
        last_error: None,
        checked_at_unix_secs: Some(now_unix_secs),
        last_attempt_at_unix_secs: now_unix_secs,
    };
    PreflightOutcome {
        wire,
        access,
        transport: None,
    }
}

#[derive(Debug)]
enum InferenceResult {
    /// `count_tokens` answered 2xx (`limited: false`) or 429.
    Works {
        limited: bool,
    },
    /// The key authenticated but the workspace is missing or wrong.
    NeedsWorkspace(RemediationCode),
    /// 401 or 403.
    Rejected(RemediationCode),
    Unreachable {
        timed_out: bool,
    },
}

#[derive(Debug)]
struct InferenceFacts {
    result: InferenceResult,
    /// An identity-linked 400 was seen: the key is org-scoped.
    identity: bool,
    /// Workspace a header-less 2xx resolved to: the key is bound.
    bound: Option<String>,
    /// Workspace header the working `count_tokens` call carried.
    selected: Option<String>,
    last_error: Option<String>,
}

struct InferenceProbe<'a> {
    api: &'a AnthropicOrgApi,
    deadline: Instant,
    key: &'a str,
    base_url: &'a Url,
    on_org_origin: bool,
}

impl InferenceProbe<'_> {
    async fn run(&self, supplied: Option<&str>, stored_mode: bool) -> InferenceFacts {
        let mut facts = InferenceFacts {
            result: InferenceResult::Unreachable { timed_out: false },
            identity: false,
            bound: None,
            selected: None,
            last_error: None,
        };

        // P1a: live model, binding, identity.
        let mut live_model = None;
        match self.models(None).await {
            Ok(response) if response.status.is_success() => {
                facts.bound = response.resolved_workspace(self.key).map(ToOwned::to_owned);
                live_model = first_claude_model(&response.body);
            }
            Ok(response) if response.status == StatusCode::UNAUTHORIZED => {
                facts.result = InferenceResult::Rejected(RemediationCode::KeyInvalid);
                return facts;
            }
            Ok(response) if is_identity_linked(response.status, &response.body) => {
                facts.identity = true;
                if let Some(workspace_id) = supplied
                    && let Ok(retry) = self.models(Some(workspace_id)).await
                    && retry.status.is_success()
                {
                    live_model = first_claude_model(&retry.body);
                }
            }
            Ok(_) | Err(_) => {}
        }

        // P1b: count tokens.
        let mut model_retry_left = live_model.is_some();
        let mut model = live_model.unwrap_or_else(|| WARMUP_MODEL.to_owned());
        let mut header_retry_left = true;
        let mut header = if facts.identity || stored_mode {
            supplied
        } else {
            None
        };
        loop {
            let response = match self.count_tokens(&model, header).await {
                Ok(response) => response,
                Err(failure) => {
                    facts.result = InferenceResult::Unreachable {
                        timed_out: failure == ProbeFailure::Timeout,
                    };
                    facts.last_error = Some(self.unreachable_error(failure.describe()));
                    return facts;
                }
            };
            let status = response.status;
            if status.is_success() && is_token_count(&response.body) {
                if header.is_none() && facts.bound.is_none() {
                    facts.bound = response.resolved_workspace(self.key).map(ToOwned::to_owned);
                }
                facts.selected = header.map(ToOwned::to_owned);
                facts.result = InferenceResult::Works { limited: false };
                return facts;
            }
            if status == StatusCode::TOO_MANY_REQUESTS {
                facts.selected = header.map(ToOwned::to_owned);
                facts.result = InferenceResult::Works { limited: true };
                return facts;
            }
            if status == StatusCode::UNAUTHORIZED {
                facts.result = InferenceResult::Rejected(RemediationCode::KeyInvalid);
                return facts;
            }
            if status == StatusCode::FORBIDDEN {
                facts.result = InferenceResult::Rejected(RemediationCode::KeyForbidden);
                return facts;
            }
            if is_identity_linked(status, &response.body) {
                facts.identity = true;
                if header.is_none()
                    && header_retry_left
                    && let Some(workspace_id) = supplied
                {
                    header_retry_left = false;
                    header = Some(workspace_id);
                    continue;
                }
                facts.result = InferenceResult::NeedsWorkspace(RemediationCode::WorkspaceRequired);
                return facts;
            }
            if let Some(workspace_id) = header {
                // Only ids that passed the local format check are ever sent,
                // so Anthropic's "must be a valid workspace ID" 400 means the
                // org has no such workspace, the same as a 404.
                if is_malformed_workspace(status, &response.body) || status == StatusCode::NOT_FOUND
                {
                    facts.last_error = Some(match facts.bound.as_deref() {
                        Some(bound) => scrubbed(
                            self.key,
                            &format!("This key acts in workspace {bound}, not {workspace_id}."),
                        ),
                        None => response_detail(self.key, &response),
                    });
                    facts.result =
                        InferenceResult::NeedsWorkspace(RemediationCode::WorkspaceNotFound);
                    return facts;
                }
            }
            if model_retry_left && matches!(status, StatusCode::BAD_REQUEST | StatusCode::NOT_FOUND)
            {
                model_retry_left = false;
                model = WARMUP_MODEL.to_owned();
                continue;
            }
            facts.result = InferenceResult::Unreachable { timed_out: false };
            facts.last_error = Some(self.unreachable_error(&response_detail(self.key, &response)));
            return facts;
        }
    }

    fn unreachable_error(&self, detail: &str) -> String {
        let target = if self.on_org_origin {
            "Anthropic"
        } else {
            "the endpoint"
        };
        scrubbed(
            self.key,
            &format!("Couldn't reach {target}; the key wasn't checked ({detail})."),
        )
    }

    fn budget(&self) -> std::time::Duration {
        self.deadline
            .saturating_duration_since(Instant::now())
            .min(self.api.limits().per_probe)
    }

    async fn models(&self, workspace_id: Option<&str>) -> Result<ProbeResponse, ProbeFailure> {
        let url = upstream_request_url(self.base_url, "/v1/models", None);
        send(
            self.api,
            self.budget(),
            ProbeRequest {
                method: Method::GET,
                url,
                key: self.key,
                workspace_id,
                json_body: None,
            },
        )
        .await
    }

    async fn count_tokens(
        &self,
        model: &str,
        workspace_id: Option<&str>,
    ) -> Result<ProbeResponse, ProbeFailure> {
        let url = upstream_request_url(self.base_url, "/v1/messages/count_tokens", None);
        let body = serde_json::to_vec(&serde_json::json!({
            "model": model,
            "messages": [{"role": "user", "content": "ping"}]
        }))
        .map_err(|_| ProbeFailure::Unreachable)?;
        send(
            self.api,
            self.budget(),
            ProbeRequest {
                method: Method::POST,
                url,
                key: self.key,
                workspace_id,
                json_body: Some(Bytes::from(body)),
            },
        )
        .await
    }
}

#[derive(Debug)]
struct OrgFacts {
    workspace_list: ApiKeyOrgCapability,
    workspaces: Vec<AnthropicWorkspace>,
    usage_report: ApiKeyOrgCapability,
    resolution: ApiKeyResolution,
    api_key_id: Option<String>,
    key_type: Option<ApiKeyType>,
}

async fn bounded<T>(
    semaphore: &Semaphore,
    deadline: Instant,
    probe: impl Future<Output = Result<T, OrgApiError>>,
) -> Result<T, OrgApiError> {
    let Ok(_permit) = semaphore.acquire().await else {
        return Err(OrgApiError::Unreachable);
    };
    tokio::time::timeout_at(deadline, probe)
        .await
        .unwrap_or(Err(OrgApiError::Timeout))
}

/// P2-P4, concurrently under the probe semaphore and the total deadline.
async fn probe_org(
    api: &AnthropicOrgApi,
    deadline: Instant,
    key: &str,
    now_unix_secs: u64,
) -> OrgFacts {
    let semaphore = Semaphore::new(api.limits().max_concurrent_probes.max(1));
    let (workspaces, usage, api_keys) = tokio::join!(
        bounded(&semaphore, deadline, api.list_workspaces(key)),
        bounded(
            &semaphore,
            deadline,
            api.probe_usage_report(key, now_unix_secs)
        ),
        bounded(&semaphore, deadline, api.list_api_keys(key)),
    );
    let (workspace_list, workspaces) = match workspaces {
        Ok(items) => (ApiKeyOrgCapability::Ok, items),
        Err(error) => (error.capability(), Vec::new()),
    };
    let usage_report = match usage {
        Ok(()) => ApiKeyOrgCapability::Ok,
        Err(error) => error.capability(),
    };
    // A sole hint match only identifies the key when every key was listed:
    // an omitted page could hold the real key with the same masked hint.
    let matched = api_keys
        .ok()
        .filter(|listing| listing.complete)
        .and_then(|listing| {
            let mut matches = listing
                .keys
                .into_iter()
                .filter(|listed| hint_matches(&listed.partial_key_hint, key));
            match (matches.next(), matches.next()) {
                (Some(only), None) => Some(only),
                _ => None,
            }
        });
    match matched {
        Some(listed) => OrgFacts {
            workspace_list,
            workspaces,
            usage_report,
            resolution: ApiKeyResolution::Resolved,
            api_key_id: Some(listed.id),
            key_type: listed.key_type,
        },
        None => OrgFacts {
            workspace_list,
            workspaces,
            usage_report,
            resolution: ApiKeyResolution::Unresolved,
            api_key_id: None,
            key_type: None,
        },
    }
}

struct Assembly {
    hint: Option<String>,
    key_class: ApiKeyClass,
    inference: InferenceFacts,
    org: Option<OrgFacts>,
    malformed_supplied: bool,
    on_org_origin: bool,
    now_unix_secs: u64,
}

fn assemble(parts: Assembly) -> PreflightOutcome {
    let Assembly {
        hint,
        key_class,
        inference,
        org,
        malformed_supplied,
        on_org_origin,
        now_unix_secs,
    } = parts;
    let mut remediation = Vec::new();
    let (mut verdict, wire_inference, transport) = match inference.result {
        InferenceResult::Works { limited } => (
            PreflightVerdict::Ok,
            if limited {
                WireInference::OkLimited
            } else {
                WireInference::Ok
            },
            None,
        ),
        InferenceResult::NeedsWorkspace(code) => {
            remediation.push(code);
            (
                PreflightVerdict::NeedsWorkspace,
                WireInference::Unverified,
                None,
            )
        }
        InferenceResult::Rejected(code) => {
            remediation.push(code);
            (PreflightVerdict::Rejected, WireInference::Denied, None)
        }
        InferenceResult::Unreachable { timed_out } => {
            remediation.push(RemediationCode::ProbeUnreachable);
            (
                PreflightVerdict::Unreachable,
                WireInference::Unverified,
                Some(ProbeTransport { timed_out }),
            )
        }
    };

    let org_scoped = key_class == ApiKeyClass::OrgScoped;
    // An org-scoped key that worked without a workspace header still needs
    // one chosen; a malformed supplied id is reported instead of required.
    if org_scoped && verdict == PreflightVerdict::Ok && inference.selected.is_none() {
        verdict = PreflightVerdict::NeedsWorkspace;
        remediation.push(RemediationCode::WorkspaceRequired);
    }
    if org_scoped
        && malformed_supplied
        && let Some(code) = remediation
            .iter_mut()
            .find(|code| **code == RemediationCode::WorkspaceRequired)
    {
        *code = RemediationCode::WorkspaceIdMalformed;
    }

    let workspaces_items = org
        .as_ref()
        .map(|org| org.workspaces.clone())
        .unwrap_or_default();
    // `bound` and `selected` are facts a working check established; every
    // other result reports `required` with no id, the not-checked default.
    let workspace = match key_class {
        ApiKeyClass::WorkspaceBound => ApiKeyWorkspace {
            mode: ApiKeyWorkspaceMode::Bound,
            id: inference.bound.clone(),
            name: None,
        },
        ApiKeyClass::OrgScoped => match inference
            .selected
            .clone()
            .filter(|_| verdict == PreflightVerdict::Ok)
        {
            Some(id) => ApiKeyWorkspace {
                mode: ApiKeyWorkspaceMode::Selected,
                name: workspaces_items
                    .iter()
                    .find(|workspace| workspace.id == id)
                    .map(|workspace| workspace.name.clone()),
                id: Some(id),
            },
            None => ApiKeyWorkspace {
                mode: ApiKeyWorkspaceMode::Required,
                id: None,
                name: None,
            },
        },
        _ if verdict == PreflightVerdict::Ok => ApiKeyWorkspace {
            mode: ApiKeyWorkspaceMode::Bound,
            id: None,
            name: None,
        },
        _ => ApiKeyWorkspace {
            mode: ApiKeyWorkspaceMode::Required,
            id: None,
            name: None,
        },
    };

    let usable = matches!(
        verdict,
        PreflightVerdict::Ok | PreflightVerdict::NeedsWorkspace
    );
    let (workspace_list, usage_report, resolution, api_key_id, key_type) = match &org {
        Some(org) => {
            if org.workspace_list == ApiKeyOrgCapability::Denied {
                remediation.push(RemediationCode::WorkspaceListDenied);
            }
            if org.usage_report == ApiKeyOrgCapability::Denied {
                remediation.push(RemediationCode::RoleInsufficient);
            }
            if org.resolution == ApiKeyResolution::Unresolved {
                remediation.push(RemediationCode::KeyIdUnresolved);
            }
            (
                org.workspace_list,
                org.usage_report,
                org.resolution,
                org.api_key_id.clone(),
                org.key_type,
            )
        }
        None => {
            if usable && key_class == ApiKeyClass::WorkspaceBound {
                remediation.push(RemediationCode::KeyWorkspaceScopedUsageUnavailable);
            }
            (
                ApiKeyOrgCapability::NotApplicable,
                ApiKeyOrgCapability::NotApplicable,
                ApiKeyResolution::NotApplicable,
                None,
                None,
            )
        }
    };
    if usable && !on_org_origin {
        remediation.push(RemediationCode::NonAnthropicBaseUrl);
    }

    let usage_report_state = match verdict {
        PreflightVerdict::Unreachable => None,
        _ => Some(usage_report),
    };
    let checked_at_unix_secs = (verdict != PreflightVerdict::Unreachable).then_some(now_unix_secs);
    let needs_workspace = org_scoped;
    let stored_inference = match wire_inference {
        WireInference::Ok | WireInference::OkLimited => ApiKeyInference::Ok,
        WireInference::Denied => ApiKeyInference::Denied,
        WireInference::Unverified => ApiKeyInference::Unverified,
    };

    let wire = PreflightWireResponse {
        verdict,
        key_hint: hint.clone(),
        classification: PreflightClassification {
            key_class,
            needs_workspace,
            workspace: workspace.clone(),
            key_type,
        },
        workspaces: PreflightWorkspaces {
            state: workspace_list,
            items: if workspace_list == ApiKeyOrgCapability::Ok {
                workspaces_items
            } else {
                Vec::new()
            },
        },
        capabilities: PreflightCapabilities {
            inference: wire_inference,
            workspace_list,
            usage_report,
            api_key_resolution: resolution,
        },
        remediation: remediation
            .iter()
            .map(|code| Remediation { code: *code })
            .collect(),
        checked_at: now_unix_secs,
    };
    let access = ApiKeyAccessWrite {
        verified: stored_inference == ApiKeyInference::Ok,
        key_class,
        key_hint: hint.unwrap_or_default(),
        api_key_id,
        needs_workspace,
        capabilities: ApiKeyCapabilities {
            inference: stored_inference,
            workspace,
            workspace_list,
            api_key_resolution: resolution,
            key_type,
        },
        remediation,
        usage_report_state,
        last_error: inference.last_error,
        checked_at_unix_secs,
        last_attempt_at_unix_secs: now_unix_secs,
    };
    PreflightOutcome {
        wire,
        access,
        transport,
    }
}
