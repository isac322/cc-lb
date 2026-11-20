use bytes::Bytes;
use http::header::{ACCEPT, CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_TYPE, HeaderValue};
use serde_json::Value;

use crate::authn_rail::Authenticated;
use crate::openai_compat::{
    self, CompatReject,
    chat::ChatPlan,
    images::ImageStreamKind,
    request::{ApiKeyOnlyFeature, NormalizeInput, Normalized, Transport},
    transcription::TranscriptFormat,
};
use crate::provider_capability::ProviderCapability;
use crate::request_context::RequestContext;
use cc_lb_domain::OpenaiOauthCompatPolicy;
use cc_lb_storage_api::upstream::UpstreamKind as StorageUpstreamKind;

#[derive(Clone, Debug)]
pub(crate) struct CompatResponsePlan {
    pub kind: CompatResponseKind,
    pub truncation_emulate: bool,
}

#[derive(Clone, Debug)]
pub(crate) enum CompatResponseKind {
    ResponsesAggregate { echo_metadata: Option<Value> },
    ResponsesStream { echo_metadata: Option<Value> },
    Compact { original_request: Value },
    Chat { plan: ChatPlan },
    ModelsList,
    ModelsGet { id: String },
    ImagesStream { kind: ImageStreamKind },
    Transcription { format: TranscriptFormat },
    PassthroughNormalizeErrors,
}

#[derive(Clone, Debug)]
pub(crate) struct PreparedCompatRequest {
    pub path: String,
    pub query: Option<String>,
    pub body: Bytes,
    pub response_plan: Option<CompatResponsePlan>,
}

pub(crate) fn effective_policy(
    server: &OpenaiOauthCompatPolicy,
    override_policy: Option<&cc_lb_domain::OpenaiOauthCompatOverride>,
) -> OpenaiOauthCompatPolicy {
    override_policy.map_or(*server, |policy| policy.resolve(server))
}

pub(crate) fn api_key_only_features(
    body: Option<&Value>,
    capability: ProviderCapability,
) -> Vec<ApiKeyOnlyFeature> {
    if !matches!(
        capability,
        ProviderCapability::OpenaiResponses | ProviderCapability::OpenaiChatCompletions
    ) {
        return Vec::new();
    }
    body.map(|body| openai_compat::request::api_key_only_features(body, Transport::Http))
        .unwrap_or_default()
}

pub(crate) fn retain_api_key_candidates(
    candidates: &mut Vec<cc_lb_domain::UpstreamCandidate>,
    features: &[ApiKeyOnlyFeature],
) -> bool {
    if features.is_empty() {
        return false;
    }
    let has_api_key = candidates
        .iter()
        .any(|candidate| candidate.kind == cc_lb_domain::UpstreamKind::OpenaiApiKey);
    if has_api_key {
        candidates.retain(|candidate| candidate.kind == cc_lb_domain::UpstreamKind::OpenaiApiKey);
    }
    has_api_key
}

pub(crate) fn response_plan_for_request(
    ctx: &RequestContext,
    capability: ProviderCapability,
    policy: &OpenaiOauthCompatPolicy,
    body: Option<&Value>,
) -> Result<Option<CompatResponsePlan>, CompatReject> {
    let Some(body) = body else { return Ok(None) };
    let plan = match capability {
        ProviderCapability::OpenaiResponses => {
            let normalized = openai_compat::request::normalize_responses_request(NormalizeInput {
                body,
                policy,
                transport: Transport::Http,
            })?;
            Some(CompatResponsePlan {
                kind: if normalized.aggregate_to_json {
                    CompatResponseKind::ResponsesAggregate {
                        echo_metadata: normalized.echo_metadata,
                    }
                } else {
                    CompatResponseKind::ResponsesStream {
                        echo_metadata: normalized.echo_metadata,
                    }
                },
                truncation_emulate: normalized.truncation_emulate,
            })
        }
        ProviderCapability::OpenaiResponsesCompact => Some(CompatResponsePlan {
            kind: CompatResponseKind::Compact {
                original_request: body.clone(),
            },
            truncation_emulate: false,
        }),
        ProviderCapability::OpenaiChatCompletions => {
            let translated = openai_compat::chat::translate_chat_request(body, policy)?;
            Some(CompatResponsePlan {
                kind: CompatResponseKind::Chat {
                    plan: translated.plan,
                },
                truncation_emulate: false,
            })
        }
        ProviderCapability::OpenaiModels => {
            if ctx.path == "/v1/models" {
                Some(CompatResponsePlan {
                    kind: CompatResponseKind::ModelsList,
                    truncation_emulate: false,
                })
            } else {
                Some(CompatResponsePlan {
                    kind: CompatResponseKind::ModelsGet {
                        id: ctx.path.rsplit('/').next().unwrap_or_default().to_owned(),
                    },
                    truncation_emulate: false,
                })
            }
        }
        ProviderCapability::OpenaiImages if ctx.path == "/v1/images/generations" => {
            Some(CompatResponsePlan {
                kind: CompatResponseKind::ImagesStream {
                    kind: ImageStreamKind::Generation,
                },
                truncation_emulate: false,
            })
        }
        ProviderCapability::OpenaiImages if ctx.path == "/v1/images/edits" => {
            Some(CompatResponsePlan {
                kind: CompatResponseKind::ImagesStream {
                    kind: ImageStreamKind::Edit,
                },
                truncation_emulate: false,
            })
        }
        ProviderCapability::OpenaiAudio => Some(CompatResponsePlan {
            kind: CompatResponseKind::PassthroughNormalizeErrors,
            truncation_emulate: false,
        }),
        _ => None,
    };
    Ok(plan)
}

pub(crate) async fn prepare_request(
    _auth: &Authenticated,
    ctx: &RequestContext,
    capability: ProviderCapability,
    upstream_kind: StorageUpstreamKind,
    policy: &OpenaiOauthCompatPolicy,
    response_plan: Option<CompatResponsePlan>,
) -> Result<PreparedCompatRequest, CompatReject> {
    if upstream_kind != StorageUpstreamKind::OpenaiOauth {
        return Ok(PreparedCompatRequest {
            path: ctx.path.clone(),
            query: ctx.query.clone(),
            body: ctx.body_bytes.clone(),
            response_plan: None,
        });
    }
    let mut path = ctx.path.clone();
    let mut query = ctx.query.clone();
    let mut body = ctx.body_bytes.clone();
    let mut plan = response_plan;
    match capability {
        ProviderCapability::OpenaiResponses => {
            if let Some(value) = sonic_rs::from_slice::<Value>(&ctx.body_bytes).ok() {
                let features = api_key_only_features(Some(&value), capability);
                let value = if features.is_empty() {
                    value
                } else {
                    openai_compat::request::degrade_without_api_key(&value, &features, policy)?
                };
                let normalized =
                    openai_compat::request::normalize_responses_request(NormalizeInput {
                        body: &value,
                        policy,
                        transport: Transport::Http,
                    })?;
                if let Some(next) = normalized.body {
                    body = serde_json::to_vec(&next)
                        .map(Bytes::from)
                        .map_err(|_| CompatReject::unsupported_parameter("body"))?;
                }
                if let Some(p) = &mut plan {
                    p.truncation_emulate = normalized.truncation_emulate;
                }
            }
        }
        ProviderCapability::OpenaiResponsesCompact => {
            let value = sonic_rs::from_slice::<Value>(&ctx.body_bytes)
                .map_err(|_| CompatReject::unsupported_parameter("body"))?;
            body = serde_json::to_vec(&openai_compat::compact::compact_request_to_responses(
                &value,
            )?)
            .map(Bytes::from)
            .map_err(|_| CompatReject::unsupported_parameter("body"))?;
            path = "/v1/responses".to_owned();
        }
        ProviderCapability::OpenaiChatCompletions => {
            let value = sonic_rs::from_slice::<Value>(&ctx.body_bytes)
                .map_err(|_| CompatReject::unsupported_parameter("body"))?;
            let translated = openai_compat::chat::translate_chat_request(&value, policy)?;
            body = serde_json::to_vec(&translated.responses_body)
                .map(Bytes::from)
                .map_err(|_| CompatReject::unsupported_parameter("body"))?;
            path = "/v1/responses".to_owned();
            plan = Some(CompatResponsePlan {
                kind: CompatResponseKind::Chat {
                    plan: translated.plan,
                },
                truncation_emulate: false,
            });
        }
        ProviderCapability::OpenaiModels => {
            let public = !openai_compat::models::is_codex_catalog_query(ctx.query.as_deref());
            if public {
                query = Some(openai_compat::models::catalog_query_for_public(
                    ctx.query.as_deref(),
                ));
                if path != "/v1/models" {
                    path = "/v1/models".to_owned();
                }
            }
        }
        ProviderCapability::OpenaiAudioTranscriptions => {
            let content_type = ctx
                .downstream_headers
                .get(CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .unwrap_or("");
            let format = openai_compat::transcription::inspect_transcription_request(
                content_type,
                ctx.body_bytes.clone(),
            )
            .await?;
            plan = Some(CompatResponsePlan {
                kind: CompatResponseKind::Transcription { format },
                truncation_emulate: false,
            });
        }
        ProviderCapability::OpenaiImages if ctx.path == "/v1/images/edits" => {
            let content_type = ctx
                .downstream_headers
                .get(CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");
            let converted =
                openai_compat::images::multipart_edit_to_json(content_type, ctx.body_bytes.clone())
                    .await?;
            body = serde_json::to_vec(&converted.body)
                .map(Bytes::from)
                .map_err(|_| CompatReject::unsupported_parameter("body"))?;
            if let Some(p) = &mut plan {
                p.kind = CompatResponseKind::ImagesStream {
                    kind: ImageStreamKind::Edit,
                };
            }
        }
        ProviderCapability::OpenaiResponsesInputTokens => {
            let value = sonic_rs::from_slice::<Value>(&ctx.body_bytes)
                .map_err(|_| CompatReject::unsupported_parameter("body"))?;
            let normalized = openai_compat::request::normalize_responses_request(NormalizeInput {
                body: &value,
                policy,
                transport: Transport::WebSocket,
            })?;
            if let Some(next) = normalized.body {
                body = serde_json::to_vec(&next)
                    .map(Bytes::from)
                    .map_err(|_| CompatReject::unsupported_parameter("body"))?;
            }
        }
        _ => {}
    }
    Ok(PreparedCompatRequest {
        path,
        query,
        body,
        response_plan: plan,
    })
}

pub(crate) fn rewritten_headers(
    mut headers: http::HeaderMap,
    body_len: usize,
    json_body: bool,
) -> http::HeaderMap {
    headers.remove(CONTENT_LENGTH);
    headers.remove(CONTENT_ENCODING);
    headers.remove(ACCEPT);
    if json_body {
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    }
    if let Ok(value) = HeaderValue::from_str(&body_len.to_string()) {
        headers.insert(CONTENT_LENGTH, value);
    }
    headers
}
