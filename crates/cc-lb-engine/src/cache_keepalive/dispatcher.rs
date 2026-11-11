use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use cc_lb_clock::{ClockHandle, unix_millis, unix_secs};
use cc_lb_control::dynamic_view::DynamicViewHolder;
use cc_lb_domain::{
    CacheBreakpoint, CacheBreakpointSource, PrefixBytes, Principal, PrincipalKind, Provider,
    TtlClass, Upstream,
};
use cc_lb_storage_api::UpstreamStore;
use cc_lb_storage_api::upstream::{UpstreamKind, UpstreamRecord};
use cc_lb_upstream::{
    DialectError, DialectShapeContext, ShapedRequest, ShapedRequestBuilder, Signer,
    UpstreamDialect, shape_request,
};
use http::StatusCode;
use http_body_util::BodyExt;
use serde::Deserialize;
use serde_json::Value;
use url::Url;

use crate::attempt_rail::{
    AttemptIntent, DispatchBeforeError, DispatchDeadline, ResponseAccountingGuard, Signed,
};
use crate::lifecycle::{
    PromptCacheUsage, UpstreamDispatch, decode_prompt_cache_observations_pure,
    prompt_cache_observation_context, prompt_cache_policy_for_upstream,
    publish_prompt_cache_observations,
};
use crate::model_resolution::{cache_threshold_tokens, canonical_model_id};
use crate::prompt_cache_simulator::{
    V3PromptCacheBlockSource, analyze_v3_prompt_cache_with_policy,
    cacheable_breakpoint_prefix_keys_with_tokenize_duration,
};
use crate::usage_decoder::decode_full_body;
use crate::usage_parser::{cache_creation_split, observe_non_stream_json_value};

use super::request_snapshot::RequestSnapshot;
use super::scheduler::{
    DispatchOutcome, KeepaliveDispatchContext, KeepaliveDispatcher, RenewalFinalization,
    RenewalUsage,
};

const ERROR_BODY_PREFIX_BYTES: usize = 512;

pub struct AnthropicKeepaliveDispatcher {
    dynamic_view: Arc<DynamicViewHolder>,
    upstream_store: Arc<dyn UpstreamStore>,
    http_client: Arc<dyn UpstreamDispatch>,
    clock: ClockHandle,
    timeout: Duration,
}

impl AnthropicKeepaliveDispatcher {
    pub fn new(
        dynamic_view: Arc<DynamicViewHolder>,
        upstream_store: Arc<dyn UpstreamStore>,
        http_client: Arc<dyn UpstreamDispatch>,
        clock: ClockHandle,
    ) -> Self {
        Self {
            dynamic_view,
            upstream_store,
            http_client,
            clock,
            timeout: Duration::from_secs(30),
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    async fn keepalive_signing_input(
        &self,
        snapshot: &RequestSnapshot,
        upstream: &UpstreamRecord,
        source_ref_id: &str,
    ) -> Result<(Arc<dyn Signer>, ShapedRequest, Bytes), String> {
        let upstream_api = direct_upstream(snapshot, upstream)?;
        let signer_factory = self
            .dynamic_view
            .load()
            .signer_factory
            .with_router_choice(upstream.name.clone());
        let signer = signer_factory
            .build(&upstream_api)
            .await
            .map_err(|source| source.to_string())?;
        let keepalive_body = snapshot
            .build_keepalive_body()
            .map_err(|source| source.to_string())?;
        let target_url = resolve_keepalive_url(snapshot, upstream)?;
        let shaped = shaped_request_from_snapshot(
            snapshot,
            keepalive_body.clone(),
            &upstream_api,
            target_url,
            source_ref_id,
        )?;
        Ok((signer, shaped, keepalive_body))
    }

    async fn dispatch_signed(
        &self,
        signed: Signed<'_>,
        deadline: &DispatchDeadline,
    ) -> Result<(StatusCode, http::HeaderMap, Bytes, Duration), DispatchBeforeError> {
        let response = signed
            .dispatch_before(self.http_client.as_ref(), deadline)
            .await?;
        let cache_anchor_at = Instant::now();
        let status = response.status();
        let headers = response.headers().clone();
        let body = response
            .into_body()
            .collect()
            .await
            .map_err(|source| {
                DispatchBeforeError::Dispatch(crate::DispatchError::Transport {
                    source: Box::new(source),
                })
            })?
            .to_bytes();
        Ok((status, headers, body, cache_anchor_at.elapsed()))
    }
}

#[async_trait]
impl KeepaliveDispatcher for AnthropicKeepaliveDispatcher {
    async fn dispatch(
        &self,
        snapshot: &RequestSnapshot,
        context: KeepaliveDispatchContext,
    ) -> DispatchOutcome {
        let upstream = match self.upstream_store.get_by_id(snapshot.upstream_id).await {
            Ok(Some(upstream)) if upstream.deleted_at_unix_secs.is_none() && upstream.enabled => {
                upstream
            }
            Ok(Some(_)) | Ok(None) => return DispatchOutcome::Error("upstream gone".to_owned()),
            Err(source) => return DispatchOutcome::Error(source.to_string()),
        };
        if let Err(error) = keepalive_upstream_supported_for_snapshot(snapshot, &upstream) {
            return DispatchOutcome::UnsupportedProvider(error);
        }

        let (signer, shaped, keepalive_body) = match self
            .keepalive_signing_input(snapshot, &upstream, context.source_ref_id())
            .await
        {
            Ok(input) => input,
            Err(error) => return DispatchOutcome::Error(error),
        };
        let observation_plan = keepalive_observation_plan(&keepalive_body, &upstream);

        let dispatch_started = Instant::now();
        let deadline = context.dispatch_deadline_unix_secs();
        let intent = match context.into_reservation() {
            Some(reservation) => AttemptIntent::from_reservation(reservation),
            None => AttemptIntent::observe_only(),
        };
        let reserved = intent.into_reserved();
        let signed = match reserved.begin_attempt().sign(signer.as_ref(), shaped).await {
            Ok(signed) => signed,
            Err(error) => return DispatchOutcome::Error(error.to_string()),
        };
        // Upstream lookup, signer construction, and OAuth refresh may all await.
        // Enforce the send boundary after them, while RAII still owns the unsent
        // reservation and before any completed-request accounting is created.
        let dispatch_anchor_unix_secs = unix_secs(self.clock.now());
        if dispatch_anchor_unix_secs >= deadline {
            return DispatchOutcome::MaxDuration;
        }
        let dispatch_start_ms = unix_millis(self.clock.now()).try_into().unwrap_or(u64::MAX);
        let send_deadline = DispatchDeadline::new(self.clock.clone(), deadline);
        let dispatch_result =
            match tokio::time::timeout(self.timeout, self.dispatch_signed(signed, &send_deadline))
                .await
            {
                Ok(Err(DispatchBeforeError::DeadlineElapsed)) => {
                    return DispatchOutcome::MaxDuration;
                }
                result => result,
            };
        let accounting_guard = reserved.into_response_accounting_guard();

        match dispatch_result {
            Ok(Ok((StatusCode::OK, headers, body, cache_anchor_age))) => {
                let outcome = classify_success_body_with_surface(
                    &body,
                    cache_anchor_age,
                    dispatch_started.elapsed(),
                    dispatch_start_ms,
                    accounting_guard,
                    snapshot.surface.as_deref(),
                );
                if let DispatchOutcome::CacheHit { .. } = &outcome
                    && let Some(plan) = observation_plan.as_ref()
                {
                    publish_keepalive_observation(
                        &self.dynamic_view,
                        snapshot.upstream_id,
                        plan,
                        &headers,
                        &body,
                        dispatch_anchor_unix_secs,
                    );
                }
                outcome
            }
            Ok(Ok((status, _headers, body, _))) => {
                DispatchOutcome::Error(format!("status={} body={}", status, body_prefix(&body)))
            }
            Ok(Err(source)) => DispatchOutcome::Error(source.to_string()),
            Err(_elapsed) => DispatchOutcome::Error(format!(
                "renewal timed out after {}s",
                self.timeout.as_secs()
            )),
        }
    }
}

#[cfg(test)]
fn keepalive_upstream_supported(upstream: &UpstreamRecord) -> Result<(), String> {
    match upstream.kind {
        UpstreamKind::AnthropicApiKey => Err(
            "AnthropicApiKey keep-alive is disabled until storage-backed signing is available; downstream keys cannot be replayed to Anthropic".to_owned(),
        ),
        UpstreamKind::AnthropicOauth => Ok(()),
        _ => Err(
            "provider keep-alive is unsupported; provider-specific background replay is not available"
                .to_owned(),
        ),
    }
}

fn keepalive_upstream_supported_for_snapshot(
    snapshot: &RequestSnapshot,
    upstream: &UpstreamRecord,
) -> Result<(), String> {
    match (snapshot.surface.as_deref(), upstream.kind) {
        (Some("responses"), UpstreamKind::OpenaiApiKey | UpstreamKind::OpenaiOauth) => {
            if matches!(
                snapshot.ttl,
                cc_lb_storage_api::CacheTtl::Ttl30m | cc_lb_storage_api::CacheTtl::Ttl12h
            ) {
                Ok(())
            } else {
                Err("OpenAI Responses keep-alive requires a 30m or 12h retention".to_owned())
            }
        }
        (Some("responses"), _) => {
            Err("OpenAI Responses keep-alive requires an OpenAI upstream".to_owned())
        }
        (None, UpstreamKind::AnthropicOauth) => Ok(()),
        (None, UpstreamKind::AnthropicApiKey) => Err(
            "AnthropicApiKey keep-alive is disabled until storage-backed signing is available; downstream keys cannot be replayed to Anthropic".to_owned(),
        ),
        _ => Err(
            "provider keep-alive is unsupported; provider-specific background replay is not available"
                .to_owned(),
        ),
    }
}

fn direct_upstream(
    snapshot: &RequestSnapshot,
    upstream: &UpstreamRecord,
) -> Result<Upstream, String> {
    match (snapshot.surface.as_deref(), upstream.kind) {
        (Some("responses"), UpstreamKind::OpenaiApiKey | UpstreamKind::OpenaiOauth) => {
            Ok(Upstream::OpenaiDirect {
                base_url: upstream.base_url.clone(),
            })
        }
        (None, UpstreamKind::AnthropicApiKey | UpstreamKind::AnthropicOauth) => {
            Ok(Upstream::AnthropicDirect {
                base_url: upstream.base_url.clone(),
            })
        }
        _ => Err("provider keep-alive request surface does not match upstream".to_owned()),
    }
}

fn resolve_keepalive_url(
    snapshot: &RequestSnapshot,
    upstream: &UpstreamRecord,
) -> Result<Url, String> {
    let mut resolved = if let Some(base_url) = upstream.base_url.as_ref() {
        cc_lb_upstream::upstream_request_url(base_url, snapshot.url.path(), snapshot.url.query())
    } else {
        snapshot.url.clone()
    };
    if upstream.kind == UpstreamKind::AnthropicOauth
        && !resolved
            .query_pairs()
            .any(|(key, value)| key == "beta" && value == "true")
    {
        resolved.query_pairs_mut().append_pair("beta", "true");
    }
    Ok(resolved)
}

fn shaped_request_from_snapshot(
    snapshot: &RequestSnapshot,
    body: Bytes,
    upstream: &Upstream,
    target_url: Url,
    source_ref_id: &str,
) -> Result<ShapedRequest, String> {
    let dialect = SnapshotDialect {
        url: target_url.clone(),
    };
    let context = DialectShapeContext {
        request_id: source_ref_id.to_owned(),
        downstream_headers: snapshot.headers.clone(),
        method: snapshot.method.clone(),
        path: target_url.path().to_owned(),
        query: target_url.query().map(ToOwned::to_owned),
        body_bytes: body,
    };
    let principal = Principal {
        id: snapshot.upstream_id.to_string(),
        kind: PrincipalKind::ApiKey,
    };
    shape_request(&dialect, &context, upstream, &principal).map_err(|source| source.to_string())
}

struct SnapshotDialect {
    url: Url,
}

impl UpstreamDialect for SnapshotDialect {
    fn shape(
        &self,
        context: &DialectShapeContext,
        _upstream: &Upstream,
        _principal: &Principal,
        builder: &mut ShapedRequestBuilder,
    ) -> Result<ShapedRequest, DialectError> {
        Ok(builder.shaped_request(
            self.url.clone(),
            context.method.clone(),
            context.downstream_headers.clone(),
            context.body_bytes.clone(),
        ))
    }
}

struct KeepaliveObservationPlan {
    canonical_model_id: String,
    cache_breakpoints: Vec<CacheBreakpoint>,
    cacheable_breakpoint_prefix_keys: HashSet<String>,
}

fn keepalive_observation_plan(
    body: &[u8],
    upstream: &UpstreamRecord,
) -> Option<KeepaliveObservationPlan> {
    let value = sonic_rs::from_slice::<Value>(body).ok()?;
    let canonical_model = value
        .get("model")
        .and_then(Value::as_str)
        .map(|requested| canonical_model_id(Provider::Anthropic, requested))?
        .to_owned();
    let (analysis, _) = analyze_v3_prompt_cache_with_policy(
        &value,
        &canonical_model,
        prompt_cache_policy_for_upstream(upstream),
    );
    if analysis.breakpoints.is_empty() {
        return None;
    }
    let threshold = cache_threshold_tokens(&canonical_model);
    let (cacheable_keys, _) = cacheable_breakpoint_prefix_keys_with_tokenize_duration(
        &analysis,
        &canonical_model,
        threshold,
        true,
    );
    let cacheable_breakpoint_prefix_keys = cacheable_keys.into_iter().collect();
    let cache_breakpoints = analysis
        .breakpoints
        .into_iter()
        .filter_map(|breakpoint| {
            let block_index = u32::try_from(breakpoint.block_index).ok()?;
            let source = match breakpoint.source {
                V3PromptCacheBlockSource::Tools => CacheBreakpointSource::Tools,
                V3PromptCacheBlockSource::System => CacheBreakpointSource::System,
                V3PromptCacheBlockSource::Message => CacheBreakpointSource::Message,
            };
            let requested_ttl = match breakpoint.ttl.as_deref() {
                Some(ttl) if ttl.eq_ignore_ascii_case("1h") => TtlClass::Ephemeral1h,
                _ => TtlClass::Ephemeral5m,
            };
            let lookback_prefixes = breakpoint
                .lookback_prefixes
                .into_iter()
                .filter_map(|lookback| {
                    Some(cc_lb_domain::CacheLookbackPrefix {
                        prefix_hash: lookback.prefix_key,
                        content_block_index: u32::try_from(lookback.content_block_index).ok()?,
                        lookback_distance: u32::try_from(lookback.lookback_distance).ok()?,
                    })
                })
                .collect();
            Some(CacheBreakpoint {
                block_index,
                source,
                path: breakpoint.path,
                message_index: breakpoint
                    .message_index
                    .and_then(|index| u32::try_from(index).ok()),
                prefix_hash: breakpoint.prefix_key,
                prefix_bytes: breakpoint.prefix_bytes,
                requested_ttl,
                lookback_prefixes,
                token_estimate_source: Some(PrefixBytes::ESTIMATE_SOURCE.to_owned()),
            })
        })
        .collect::<Vec<_>>();
    (!cache_breakpoints.is_empty()).then_some(KeepaliveObservationPlan {
        canonical_model_id: canonical_model,
        cache_breakpoints,
        cacheable_breakpoint_prefix_keys,
    })
}

fn publish_keepalive_observation(
    dynamic_view: &DynamicViewHolder,
    upstream_id: uuid::Uuid,
    plan: &KeepaliveObservationPlan,
    response_headers: &http::HeaderMap,
    response_body: &[u8],
    dispatch_anchor_unix_secs: u64,
) {
    let output_budget = response_body.len().saturating_mul(4).max(1024 * 1024);
    let Ok(Some(decoded_body)) = decode_full_body(response_headers, response_body, output_budget)
    else {
        return;
    };
    let Ok(value) = sonic_rs::from_slice::<Value>(decoded_body.as_ref()) else {
        return;
    };
    let usage = observe_non_stream_json_value(&value).usage;
    if !usage.has_affirmative_read_only_cache_usage() {
        return;
    }
    let view = dynamic_view.load();
    let Some(context) = prompt_cache_observation_context(
        &view,
        upstream_id,
        &plan.canonical_model_id,
        &plan.cache_breakpoints,
        &plan.cacheable_breakpoint_prefix_keys,
        None,
        None,
    ) else {
        return;
    };
    let decoded = decode_prompt_cache_observations_pure(
        &context,
        PromptCacheUsage::from(&usage),
        dispatch_anchor_unix_secs,
    );
    publish_prompt_cache_observations(&context, &decoded, dispatch_anchor_unix_secs);
}

#[derive(Deserialize, Default)]
struct KeepaliveResponseUsage {
    #[serde(default)]
    input_tokens: u64,
    #[serde(default)]
    output_tokens: u64,
    #[serde(default)]
    cache_creation_input_tokens: u64,
    // Preserve validation of legacy flat TTL counters without using them as buckets.
    #[serde(default, rename = "cache_creation_input_tokens_5m")]
    _cache_creation_input_tokens_5m: u64,
    #[serde(default, rename = "cache_creation_input_tokens_1h")]
    _cache_creation_input_tokens_1h: u64,
    #[serde(default)]
    cache_creation: serde_json::Value,
    #[serde(default)]
    cache_read_input_tokens: u64,
}

#[derive(Deserialize)]
struct KeepaliveResponseBody {
    usage: Option<KeepaliveResponseUsage>,
}

fn classify_success_body_with_surface(
    body: &[u8],
    cache_anchor_age: Duration,
    duration: Duration,
    request_start_ms: u64,
    accounting_guard: ResponseAccountingGuard,
    surface: Option<&str>,
) -> DispatchOutcome {
    if surface == Some("responses") {
        let Ok(value) = sonic_rs::from_slice::<Value>(body) else {
            return DispatchOutcome::Error("invalid OpenAI Responses JSON".to_owned());
        };
        let usage = value.get("usage").and_then(Value::as_object);
        let input_tokens = usage
            .and_then(|usage| usage.get("input_tokens"))
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let output_tokens = usage
            .and_then(|usage| usage.get("output_tokens"))
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let reasoning_tokens = usage
            .and_then(|usage| usage.get("output_tokens_details"))
            .and_then(|details| details.get("reasoning_tokens"))
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let details = usage.and_then(|usage| usage.get("input_tokens_details"));
        let cache_read = details
            .and_then(|details| details.get("cached_tokens"))
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let _cache_write = details
            .and_then(|details| details.get("cache_write_tokens"))
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let finalization = RenewalFinalization {
            usage: RenewalUsage {
                input_tokens,
                output_tokens,
                // OpenAI lifetime-specific writes are not Anthropic cache
                // buckets. Keep the raw response out of those projections and
                // mark cost unknown until provider pricing is modeled.
                cache_creation_input_tokens: 0,
                cache_creation_input_tokens_5m: 0,
                cache_creation_input_tokens_1h: 0,
                cache_read_input_tokens: cache_read,
                reasoning_tokens,
                provider_cost_unknown: true,
            },
            status: StatusCode::OK.as_u16(),
            duration,
            request_start_ms,
            accounting_guard,
        };
        return if cache_read > 0 {
            DispatchOutcome::CacheHit {
                cache_anchor_age,
                finalization,
            }
        } else {
            DispatchOutcome::CacheMiss { finalization }
        };
    }
    match sonic_rs::from_slice::<KeepaliveResponseBody>(body) {
        Ok(parsed) => {
            let usage = parsed.usage.unwrap_or_default();
            let (cache_creation_total, cache_creation_5m, cache_creation_1h) = cache_creation_split(
                usage.cache_creation_input_tokens,
                Some(&usage.cache_creation),
            );
            let finalization = RenewalFinalization {
                usage: RenewalUsage {
                    input_tokens: usage.input_tokens,
                    output_tokens: usage.output_tokens,
                    cache_creation_input_tokens: cache_creation_total,
                    cache_creation_input_tokens_5m: cache_creation_5m,
                    cache_creation_input_tokens_1h: cache_creation_1h,
                    cache_read_input_tokens: usage.cache_read_input_tokens,
                    reasoning_tokens: 0,
                    provider_cost_unknown: false,
                },
                status: StatusCode::OK.as_u16(),
                duration,
                request_start_ms,
                accounting_guard,
            };
            // Renewal appends no cache-controlled content. Any cache creation
            // means a protected prefix was rebuilt rather than fully preserved.
            // Keep its paid usage, but never perpetuate a partial cache hit.
            if finalization.usage.cache_read_input_tokens > 0
                && finalization.usage.cache_creation_input_tokens == 0
            {
                DispatchOutcome::CacheHit {
                    cache_anchor_age,
                    finalization,
                }
            } else {
                DispatchOutcome::CacheMiss { finalization }
            }
        }
        Err(source) => DispatchOutcome::Error(source.to_string()),
    }
}

#[cfg(test)]
fn classify_success_body(
    body: &[u8],
    cache_anchor_age: Duration,
    duration: Duration,
    request_start_ms: u64,
    accounting_guard: ResponseAccountingGuard,
) -> DispatchOutcome {
    classify_success_body_with_surface(
        body,
        cache_anchor_age,
        duration,
        request_start_ms,
        accounting_guard,
        None,
    )
}

fn body_prefix(body: &[u8]) -> String {
    let prefix_len = body.len().min(ERROR_BODY_PREFIX_BYTES);
    String::from_utf8_lossy(&body[..prefix_len]).into_owned()
}

#[cfg(test)]
mod tests;
