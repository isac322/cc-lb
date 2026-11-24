use super::routing::upstream_kind_for_candidate;
use super::*;

#[derive(Clone, Debug)]
pub(crate) struct SelectedCacheMatch {
    pub(crate) prefix_hash: String,
    pub(crate) content_block_index: u32,
    pub(crate) ttl_class: TtlClass,
    pub(crate) estimated_prefix_bytes: PrefixBytes,
}

#[derive(Clone, Debug)]
pub(super) struct ResolvedWarmMatch {
    pub(super) entry: WarmCacheEntry,
    pub(super) prefix_hash: String,
    pub(super) matched_breakpoint_block_index: u32,
    pub(super) content_block_index: u32,
    pub(super) lookback_distance: u32,
    pub(super) token_estimate_source: Option<String>,
}

pub(super) struct StructuralCandidate<'a> {
    pub(super) prefix_hash: &'a str,
    pub(super) content_block_index: u32,
    pub(super) memberships: Vec<(&'a CacheBreakpoint, &'a CacheLookbackPrefix)>,
}

/// Fetch warm prompt-cache observations for this request's candidate
/// upstreams and prefix keys from the shared observation store.
///
/// This is the only warmth authority: a single bounded batch read scoped to
/// the request's candidate keys. A read failure is reported and degrades to
/// an empty (neutral) observation set — never to a request failure.
#[cfg(test)]
pub(super) async fn fetch_prompt_cache_warm_entries(
    view: &DynamicView,
    principal_id: &str,
    request_kind: RequestKind,
    canonical_model: &str,
    request_breakpoints: &[CacheBreakpoint],
    clock: &dyn Clock,
) -> HashMap<Uuid, Vec<WarmCacheEntry>> {
    fetch_prompt_cache_warm_entries_for_bundles(
        view,
        principal_id,
        request_kind,
        canonical_model,
        &PromptCacheBundles::single(request_breakpoints),
        clock,
    )
    .await
}

pub(super) async fn fetch_prompt_cache_warm_entries_for_bundles(
    view: &DynamicView,
    principal_id: &str,
    request_kind: RequestKind,
    canonical_model: &str,
    bundles: &PromptCacheBundles,
    clock: &dyn Clock,
) -> HashMap<Uuid, Vec<WarmCacheEntry>> {
    let mut warm_entries: HashMap<Uuid, Vec<WarmCacheEntry>> = HashMap::new();
    let (Some(store), Some(allowed_upstreams)) = (
        view.prompt_cache_observation_store_opt(),
        view.principal_view.allowed_upstreams(principal_id),
    ) else {
        return warm_entries;
    };
    if canonical_model.is_empty()
        || bundles.official.cache_breakpoints.is_empty()
            && bundles.literal.cache_breakpoints.is_empty()
    {
        return warm_entries;
    }
    let upstream_ids: Vec<Uuid> = view
        .upstreams_snapshot()
        .iter()
        .filter(|upstream| upstream.enabled)
        .filter(|upstream| upstream.deleted_at_unix_secs.is_none())
        .filter(|upstream| request_kind.eligible(upstream))
        .filter(|upstream| allowed_upstreams.is_empty() || allowed_upstreams.contains(&upstream.id))
        .map(|upstream| upstream.id)
        .collect();
    if upstream_ids.is_empty() {
        return warm_entries;
    }
    let mut prefix_keys: Vec<String> = Vec::new();
    let mut seen_prefix_keys: HashSet<&str> = HashSet::new();
    for bundle in [&bundles.official, &bundles.literal] {
        for breakpoint in &bundle.cache_breakpoints {
            for prefix in &breakpoint.lookback_prefixes {
                if seen_prefix_keys.insert(prefix.prefix_hash.as_str()) {
                    prefix_keys.push(prefix.prefix_hash.clone());
                }
            }
        }
    }
    if prefix_keys.is_empty() {
        return warm_entries;
    }
    let now_unix_secs = unix_secs(clock.now());
    let lookup_span = tracing::info_span!(
        "proxy.prompt_cache_observation_lookup",
        otel.status_code = tracing::field::Empty,
    );
    let records = store
        .list_active_for_candidates(&upstream_ids, canonical_model, &prefix_keys, now_unix_secs)
        .instrument(lookup_span.clone())
        .await;
    match records {
        Ok(records) => {
            // A successful empty lookup is known-cold; a failed lookup is
            // unknown. Preserve that distinction for routing cost estimates.
            for upstream_id in upstream_ids {
                warm_entries.insert(upstream_id, Vec::new());
            }
            for record in records {
                // The store bounds by candidate keys and expiry; the schema
                // version guard stays here so a backend cannot warm-route a
                // request on keys derived under a different hash schema.
                if record.hash_schema_version != HASH_SCHEMA_VERSION {
                    continue;
                }
                // A size measured by another estimator is in a different
                // unit; such a row is known-cold, not an error.
                let Some(estimated_prefix_bytes) = PrefixBytes::from_persisted(
                    record.estimated_prefix_tokens,
                    &record.token_estimate_source,
                ) else {
                    continue;
                };
                warm_entries
                    .entry(record.upstream_id)
                    .or_default()
                    .push(WarmCacheEntry {
                        prefix_hash: record.v3_prefix_key,
                        expires_at_unix_secs: record.expires_at_unix_secs,
                        ttl_class: record.ttl_class,
                        last_observed_at_unix_secs: record.last_observed_at_unix_secs,
                        content_block_index: record.prefix_content_block_index,
                        estimated_prefix_bytes,
                        hash_schema_version: record.hash_schema_version,
                    });
            }
        }
        Err(error) => {
            lookup_span.record("otel.status_code", "ERROR");
            cc_lb_observability::inc_cache_observation_read_failed();
            tracing::warn!(
                %error,
                "prompt-cache observation lookup failed; routing without cache affinity"
            );
        }
    }
    warm_entries
}
/// Fetch provider-native cache observations for a request's eligible upstreams.
///
/// The storage API is keyed by one upstream at a time, so this performs one
/// bounded lookup per eligible candidate, before `route_guard` is entered.
/// Invalid request metadata and lookup failures both produce a neutral result.
pub(super) async fn fetch_provider_native_cache_scores(
    view: &DynamicView,
    principal_id: &str,
    request_kind: RequestKind,
    canonical_model: &str,
    native_cache: Option<&cc_lb_request_log::RequestNativeCacheObservation>,
    clock: &dyn Clock,
) -> HashMap<Uuid, CacheScore> {
    let Some((provider, mode, scope, identity_hash, retention_secs)) =
        provider_native_lookup_params(native_cache, canonical_model, request_kind)
    else {
        return HashMap::new();
    };
    let Some(allowed_upstreams) = view.principal_view.allowed_upstreams(principal_id) else {
        return HashMap::new();
    };
    let Some(store) = view.prompt_cache_observation_store_opt() else {
        return HashMap::new();
    };
    let now_unix_secs = unix_secs(clock.now());
    let upstream_ids: Vec<Uuid> = view
        .upstreams_snapshot()
        .iter()
        .filter(|upstream| upstream.enabled)
        .filter(|upstream| upstream.deleted_at_unix_secs.is_none())
        .filter(|upstream| request_kind.eligible(upstream))
        .filter(|upstream| allowed_upstreams.is_empty() || allowed_upstreams.contains(&upstream.id))
        .filter(|upstream| upstream_credentials_routable(view, upstream))
        .map(|upstream| upstream.id)
        .collect();

    let lookup_span = tracing::info_span!(
        "proxy.provider_prompt_cache_observation_lookup",
        otel.status_code = tracing::field::Empty,
    );
    let records = store
        .list_active_provider_observations_for_candidates(
            &upstream_ids,
            provider,
            canonical_model,
            mode,
            scope,
            identity_hash,
            Some(retention_secs),
            now_unix_secs,
        )
        .instrument(lookup_span.clone())
        .await;
    let records = match records {
        Ok(records) => records,
        Err(error) => {
            lookup_span.record("otel.status_code", "ERROR");
            cc_lb_observability::inc_cache_observation_read_failed();
            tracing::warn!(
                %error,
                "provider prompt-cache observation lookup failed; routing without native cache preference"
            );
            return HashMap::new();
        }
    };
    let mut scores: HashMap<Uuid, CacheScore> = HashMap::new();
    for record in records {
        if !upstream_ids.contains(&record.upstream_id)
            || record.provider != provider
            || record.canonical_model_id != canonical_model
            || record.mode != mode
            || record.isolation_scope != scope
            || record.identity_hash != identity_hash
            || record.retention_secs != Some(retention_secs)
            || !record
                .expires_at_unix_secs
                .is_some_and(|expires| expires > now_unix_secs)
            || !record
                .observed_cached_tokens
                .is_some_and(|tokens| tokens > 0)
        {
            continue;
        }
        scores
            .entry(record.upstream_id)
            .and_modify(|score| {
                if record.observed_cached_tokens.unwrap_or_default()
                    > score.predicted_cache_read_tokens.into()
                {
                    *score = native_cache_score_from_record(&record);
                }
            })
            .or_insert_with(|| native_cache_score_from_record(&record));
    }
    scores
}

pub(super) fn native_cache_scope(scope: Option<&str>) -> Option<&str> {
    let scope = scope?;
    let (organization, region) = scope.strip_prefix("org=")?.split_once(";region=")?;
    (!organization.is_empty()
        && !organization.contains(';')
        && !region.is_empty()
        && !region.contains(';'))
    .then_some(scope)
}
pub(super) fn provider_native_lookup_params<'a>(
    native_cache: Option<&'a cc_lb_request_log::RequestNativeCacheObservation>,
    canonical_model: &str,
    request_kind: RequestKind,
) -> Option<(&'a str, &'a str, &'a str, &'a str, u64)> {
    let native = native_cache?;
    if native.provider == "openai" && provider_cache::supports_modern_openai_cache(canonical_model)
    {
        return None;
    }
    let scope = native_cache_scope(native.scope.as_deref())?;
    let identity_hash = native
        .identity_hash
        .as_deref()
        .filter(|identity| is_provider_native_identity(identity))?;
    let retention_secs = native.retention_secs?;
    (native.provider == "openai"
        && !native.mode.is_empty()
        && !canonical_model.is_empty()
        && (request_kind.matches_upstream(StorageUpstreamKind::OpenaiApiKey)
            || request_kind.matches_upstream(StorageUpstreamKind::OpenaiOauth)))
    .then_some((
        native.provider.as_str(),
        native.mode.as_str(),
        scope,
        identity_hash,
        retention_secs,
    ))
}

pub(super) fn is_provider_native_identity(identity_hash: &str) -> bool {
    let Some(digest) = identity_hash.strip_prefix("sha256:") else {
        return false;
    };
    digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
}

pub(super) fn native_cache_score_from_record(
    record: &ProviderPromptCacheObservationRecord,
) -> CacheScore {
    CacheScore {
        predicted_cache_read_tokens: record
            .observed_cached_tokens
            .unwrap_or_default()
            .min(u64::from(u32::MAX)) as u32,
        predicted_cache_creation_tokens_5m: 0,
        predicted_cache_creation_tokens_1h: 0,
        predicted_uncached_input_tokens: 0,
        predicted_expires_at_unix_secs: record.expires_at_unix_secs,
        matched_breakpoint_index: None,
        confidence: 1.0,
        ambiguity_reason: Some("provider_native_observation".to_owned()),
        matched_v3_cache_key: None,
        breakpoint_content_block_index: None,
        matched_content_block_index: None,
        lookback_distance: None,
        token_estimate_source: None,
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn build_candidates_with_native_cache_for_bundles(
    view: &DynamicView,
    principal_id: &str,
    request_kind: RequestKind,
    request_breakpoints: &[CacheBreakpoint],
    warm_entries: &HashMap<Uuid, Vec<WarmCacheEntry>>,
    bundles: Option<&PromptCacheBundles>,
    native_cache_scores: &HashMap<Uuid, CacheScore>,
    clock: &dyn Clock,
) -> (Vec<UpstreamCandidate>, HashMap<Uuid, SelectedCacheMatch>) {
    build_candidates_with_matches_for_bundles_and_native(
        view,
        principal_id,
        request_kind,
        request_breakpoints,
        warm_entries,
        bundles,
        Some(native_cache_scores),
        clock,
    )
}

pub fn build_candidates(
    view: &DynamicView,
    principal_id: &str,
    request_kind: RequestKind,
    request_breakpoints: &[CacheBreakpoint],
    warm_entries: &HashMap<Uuid, Vec<WarmCacheEntry>>,
    clock: &dyn Clock,
) -> Vec<UpstreamCandidate> {
    build_candidates_with_matches_for_bundles(
        view,
        principal_id,
        request_kind,
        request_breakpoints,
        warm_entries,
        None,
        clock,
    )
    .0
}

pub(crate) fn build_candidates_with_matches_for_bundles(
    view: &DynamicView,
    principal_id: &str,
    request_kind: RequestKind,
    request_breakpoints: &[CacheBreakpoint],
    warm_entries: &HashMap<Uuid, Vec<WarmCacheEntry>>,
    bundles: Option<&PromptCacheBundles>,
    clock: &dyn Clock,
) -> (Vec<UpstreamCandidate>, HashMap<Uuid, SelectedCacheMatch>) {
    build_candidates_with_matches_for_bundles_and_native(
        view,
        principal_id,
        request_kind,
        request_breakpoints,
        warm_entries,
        bundles,
        None,
        clock,
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) fn build_candidates_with_matches_for_bundles_and_native(
    view: &DynamicView,
    principal_id: &str,
    request_kind: RequestKind,
    request_breakpoints: &[CacheBreakpoint],
    warm_entries: &HashMap<Uuid, Vec<WarmCacheEntry>>,
    bundles: Option<&PromptCacheBundles>,
    native_cache_scores: Option<&HashMap<Uuid, CacheScore>>,
    clock: &dyn Clock,
) -> (Vec<UpstreamCandidate>, HashMap<Uuid, SelectedCacheMatch>) {
    let Some(allowed_upstreams) = view.principal_view.allowed_upstreams(principal_id) else {
        return (Vec::new(), HashMap::new());
    };

    let mut candidates: Vec<UpstreamCandidate> = Vec::new();
    let mut selected_matches: HashMap<Uuid, SelectedCacheMatch> = HashMap::new();
    {
        let rate_limit_cache = view.upstream_rate_limit_cache.read();
        let now_unix_millis = unix_now_ms(clock);
        let now_unix_secs = unix_secs(clock.now());
        for upstream in view
            .upstreams_snapshot()
            .iter()
            .filter(|upstream| upstream.enabled)
            .filter(|upstream| upstream.deleted_at_unix_secs.is_none())
            .filter(|upstream| request_kind.eligible(upstream))
            .filter(|upstream| {
                allowed_upstreams.is_empty() || allowed_upstreams.contains(&upstream.id)
            })
            .filter(|upstream| upstream_credentials_routable(view, upstream))
        {
            let has_rate_limit_snapshot = rate_limit_cache
                .snapshots
                .get(&upstream.id)
                .is_some_and(|snapshots| !snapshots.is_empty());
            let observed_at_unix_secs = if has_rate_limit_snapshot {
                rate_limit_cache.updated_at_unix_secs
            } else {
                0
            };
            let bundle_breakpoints = bundles
                .map(|bundles| bundles.for_upstream(upstream).cache_breakpoints.as_slice())
                .unwrap_or(request_breakpoints);
            let cache_score = native_cache_scores
                .and_then(|scores| scores.get(&upstream.id).cloned())
                .or_else(|| {
                    warm_entries.get(&upstream.id).and_then(|entries| {
                        let matched =
                            resolve_longest_warm_match(entries, bundle_breakpoints, now_unix_secs);
                        let score =
                            build_cache_score_from_match(bundle_breakpoints, matched.as_ref());
                        if score.is_some()
                            && let Some(matched) = matched.as_ref()
                        {
                            selected_matches.insert(
                                upstream.id,
                                SelectedCacheMatch {
                                    prefix_hash: matched.prefix_hash.clone(),
                                    content_block_index: matched.content_block_index,
                                    ttl_class: matched.entry.ttl_class,
                                    estimated_prefix_bytes: matched.entry.estimated_prefix_bytes,
                                },
                            );
                        }
                        score
                    })
                });

            let plan_info = view.plan_info_by_upstream.get(&upstream.id);
            candidates.push(UpstreamCandidate {
                upstream_id: upstream.id,
                name: upstream.name.clone(),
                kind: upstream_kind_for_candidate(upstream.kind),
                subscription_quotas: view.subscription_quota_cache.snapshot_for_upstream(
                    upstream.id,
                    now_unix_millis,
                    view.subscription_quota_routing_max_staleness_secs,
                ),
                observed_at_unix_secs,
                cache_score,
                base_url: upstream.base_url.as_ref().map(|url| url.to_string()),
                plan_capacity_ratio: plan_info.map(|p| p.capacity_ratio),
                organization_type: plan_info.and_then(|p| p.organization_type.clone()),
                rate_limit_tier: plan_info.and_then(|p| p.rate_limit_tier.clone()),
                seat_tier: plan_info.and_then(|p| p.seat_tier.clone()),
            });
        }
    }
    candidates.sort_unstable_by_key(|c| c.upstream_id);
    (candidates, selected_matches)
}

/// Excludes upstreams whose credentials are known dead: OAuth accounts that
/// need an operator reconnect and any upstream whose last apply failed.
pub(super) fn upstream_credentials_routable(view: &DynamicView, upstream: &UpstreamRecord) -> bool {
    if matches!(
        upstream.kind,
        StorageUpstreamKind::AnthropicOauth
            | StorageUpstreamKind::OpenaiOauth
            | StorageUpstreamKind::GeminiOauth
    ) && cc_lb_oauth_protocol::refresh_requires_reconnect(upstream.last_apply_error.as_deref())
    {
        return false;
    }
    !view
        .upstream_status_snapshot
        .entries
        .get(&upstream.name)
        .is_some_and(|entry| entry.status == cc_lb_control::ApplyStatus::Error)
}

#[cfg(test)]
pub(super) fn build_cache_score(
    request_breakpoints: &[CacheBreakpoint],
    warm_entries: &[WarmCacheEntry],
) -> Option<CacheScore> {
    let matched = resolve_warm_match(request_breakpoints, |prefix_hash, eligible_ttls| {
        warm_entries
            .iter()
            .filter(|entry| entry.prefix_hash == prefix_hash)
            .filter(|entry| eligible_ttls.contains(&entry.ttl_class))
            .max_by_key(|entry| entry.expires_at_unix_secs)
            .cloned()
    });
    build_cache_score_from_match(request_breakpoints, matched.as_ref())
}

pub(super) fn build_cache_score_from_match(
    request_breakpoints: &[CacheBreakpoint],
    matched: Option<&ResolvedWarmMatch>,
) -> Option<CacheScore> {
    if request_breakpoints.is_empty() {
        return None;
    }

    if !valid_ttl_order(request_breakpoints) {
        return None;
    }

    let matched_prefix_bytes = matched.map_or(PrefixBytes::default(), |matched| {
        matched.entry.estimated_prefix_bytes
    });
    let anchor = matched.map(|matched| (matched.content_block_index, matched.entry.ttl_class));

    let deepest_1h = deepest_breakpoint(request_breakpoints, TtlClass::Ephemeral1h, anchor);
    let deepest_5m = deepest_breakpoint(request_breakpoints, TtlClass::Ephemeral5m, anchor);

    let mut non_monotonic = false;
    let one_hour_baseline = matched
        .filter(|matched| matched.entry.ttl_class == TtlClass::Ephemeral1h)
        .map_or(PrefixBytes::default(), |matched| {
            matched.entry.estimated_prefix_bytes
        });
    let predicted_cache_creation_bytes_1h =
        deepest_1h.map_or(PrefixBytes::default(), |breakpoint| {
            non_monotonic |= breakpoint.prefix_bytes < one_hour_baseline;
            breakpoint.prefix_bytes.saturating_sub(one_hour_baseline)
        });
    let boundary = deepest_1h.map_or(matched_prefix_bytes, |breakpoint| breakpoint.prefix_bytes);
    let predicted_cache_creation_bytes_5m =
        deepest_5m.map_or(PrefixBytes::default(), |breakpoint| {
            non_monotonic |= breakpoint.prefix_bytes < boundary;
            breakpoint.prefix_bytes.saturating_sub(boundary)
        });
    let ambiguity_reason = non_monotonic.then(|| "non_monotonic_token_estimate".to_owned());

    Some(CacheScore {
        predicted_cache_read_tokens: token_estimate_u32(matched_prefix_bytes),
        predicted_cache_creation_tokens_5m: token_estimate_u32(predicted_cache_creation_bytes_5m),
        predicted_cache_creation_tokens_1h: token_estimate_u32(predicted_cache_creation_bytes_1h),
        predicted_uncached_input_tokens: 0,
        predicted_expires_at_unix_secs: matched.map(|matched| matched.entry.expires_at_unix_secs),
        matched_breakpoint_index: matched.map(|matched| matched.matched_breakpoint_block_index),
        confidence: if matched.is_some() { 1.0 } else { 0.0 },
        ambiguity_reason,
        matched_v3_cache_key: matched.map(|matched| matched.prefix_hash.clone()),
        breakpoint_content_block_index: matched
            .map(|matched| matched.matched_breakpoint_block_index),
        matched_content_block_index: matched.map(|matched| matched.content_block_index),
        lookback_distance: matched.map(|matched| matched.lookback_distance),
        token_estimate_source: matched.and_then(|matched| matched.token_estimate_source.clone()),
    })
}

pub(super) fn valid_ttl_order(breakpoints: &[CacheBreakpoint]) -> bool {
    let deepest_1h = breakpoints
        .iter()
        .filter(|breakpoint| breakpoint.requested_ttl == TtlClass::Ephemeral1h)
        .map(|breakpoint| breakpoint.block_index)
        .max();
    let shallowest_5m = breakpoints
        .iter()
        .filter(|breakpoint| breakpoint.requested_ttl == TtlClass::Ephemeral5m)
        .map(|breakpoint| breakpoint.block_index)
        .min();
    match (deepest_1h, shallowest_5m) {
        (Some(latest_1h), Some(earliest_5m)) => latest_1h < earliest_5m,
        _ => true,
    }
}

pub(super) fn deepest_breakpoint(
    breakpoints: &[CacheBreakpoint],
    ttl: TtlClass,
    anchor: Option<(u32, TtlClass)>,
) -> Option<&CacheBreakpoint> {
    breakpoints
        .iter()
        .filter(|breakpoint| breakpoint.requested_ttl == ttl)
        .filter(|breakpoint| breakpoint_needs_write(breakpoint, anchor))
        .max_by_key(|breakpoint| breakpoint.block_index)
}

pub(super) fn breakpoint_needs_write(
    breakpoint: &CacheBreakpoint,
    anchor: Option<(u32, TtlClass)>,
) -> bool {
    let Some((matched_index, matched_ttl)) = anchor else {
        return true;
    };
    match breakpoint.block_index.cmp(&matched_index) {
        std::cmp::Ordering::Less => false,
        std::cmp::Ordering::Greater => true,
        std::cmp::Ordering::Equal => {
            !eligible_ttls(breakpoint.requested_ttl).contains(&matched_ttl)
        }
    }
}

pub(super) fn eligible_ttls(requested: TtlClass) -> Vec<TtlClass> {
    match requested {
        TtlClass::Ephemeral5m => vec![TtlClass::Ephemeral5m, TtlClass::Ephemeral1h],
        TtlClass::Ephemeral1h => vec![TtlClass::Ephemeral1h],
    }
}

pub(super) fn resolve_longest_warm_match(
    warm_entries: &[WarmCacheEntry],
    request_breakpoints: &[CacheBreakpoint],
    now_unix_secs: u64,
) -> Option<ResolvedWarmMatch> {
    resolve_warm_match(request_breakpoints, |prefix_hash, eligible_ttls| {
        warm_entries
            .iter()
            .filter(|entry| entry.prefix_hash == prefix_hash)
            .filter(|entry| eligible_ttls.contains(&entry.ttl_class))
            // Selection-time expiry re-check: a record committed between the
            // batch read and this decision must not warm-route once expired.
            .filter(|entry| entry.expires_at_unix_secs > now_unix_secs)
            .max_by_key(|entry| entry.expires_at_unix_secs)
            .cloned()
    })
}

pub(super) fn resolve_warm_match(
    request_breakpoints: &[CacheBreakpoint],
    mut lookup: impl FnMut(&str, &[TtlClass]) -> Option<WarmCacheEntry>,
) -> Option<ResolvedWarmMatch> {
    let mut candidates: Vec<StructuralCandidate<'_>> = Vec::new();
    for breakpoint in request_breakpoints {
        for prefix in &breakpoint.lookback_prefixes {
            if let Some(candidate) = candidates.iter_mut().find(|candidate| {
                candidate.prefix_hash == prefix.prefix_hash
                    && candidate.content_block_index == prefix.content_block_index
            }) {
                candidate.memberships.push((breakpoint, prefix));
            } else {
                candidates.push(StructuralCandidate {
                    prefix_hash: &prefix.prefix_hash,
                    content_block_index: prefix.content_block_index,
                    memberships: vec![(breakpoint, prefix)],
                });
            }
        }
    }
    candidates.sort_by(|left, right| {
        right
            .content_block_index
            .cmp(&left.content_block_index)
            .then_with(|| left.prefix_hash.cmp(right.prefix_hash))
    });
    for candidate in candidates {
        let eligible_ttls = if candidate
            .memberships
            .iter()
            .any(|(breakpoint, _)| breakpoint.requested_ttl == TtlClass::Ephemeral5m)
        {
            vec![TtlClass::Ephemeral5m, TtlClass::Ephemeral1h]
        } else {
            vec![TtlClass::Ephemeral1h]
        };
        let Some(entry) = lookup(candidate.prefix_hash, &eligible_ttls) else {
            continue;
        };
        let (breakpoint, prefix) = candidate
            .memberships
            .into_iter()
            .filter(|(breakpoint, _)| {
                eligible_ttls_for_entry(breakpoint.requested_ttl, entry.ttl_class)
            })
            .min_by_key(|(breakpoint, prefix)| {
                (prefix.lookback_distance, breakpoint.block_index)
            })?;
        return Some(ResolvedWarmMatch {
            entry,
            prefix_hash: candidate.prefix_hash.to_owned(),
            matched_breakpoint_block_index: breakpoint.block_index,
            content_block_index: candidate.content_block_index,
            lookback_distance: prefix.lookback_distance,
            token_estimate_source: breakpoint.token_estimate_source.clone(),
        });
    }
    None
}

pub(super) fn eligible_ttls_for_entry(requested: TtlClass, entry: TtlClass) -> bool {
    eligible_ttls(requested).contains(&entry)
}

pub(super) fn cache_pricing_summary_for_model(
    catalog: &cc_lb_pricing::PriceCatalog,
    model: &str,
    service_tier: Option<&str>,
) -> CachePricingSummary {
    if model.is_empty() {
        return CachePricingSummary {
            native: None,
            status: "unknown".to_owned(),
            input_micros_per_million: None,
            cache_creation_5m_micros_per_million: None,
            cache_creation_1h_micros_per_million: None,
            cache_read_micros_per_million: None,
        };
    }

    let normalized = cc_lb_pricing::normalize_model_id(model);
    let native = catalog
        .native_pricing(model, service_tier)
        .map(native_cache_pricing_from_catalog);
    if matches!(
        native.as_ref().map(|pricing| pricing.provider),
        Some(NativeCachePricingProvider::OpenAi | NativeCachePricingProvider::Gemini)
    ) {
        return CachePricingSummary {
            native,
            ..CachePricingSummary::default()
        };
    }
    let pricing = catalog.routing_cache_pricing(model, service_tier);
    let input = pricing.map(|rates| rates.input_per_million_usd.as_micros_usd());
    let cache_creation_5m = pricing.and_then(|rates| {
        rates
            .cache_creation_5m_per_million_usd
            .map(|price| price.as_micros_usd())
    });
    let cache_creation_1h = pricing.and_then(|rates| {
        rates
            .cache_creation_1h_per_million_usd
            .map(|price| price.as_micros_usd())
    });
    let cache_read = pricing.and_then(|rates| {
        rates
            .cache_read_per_million_usd
            .map(|price| price.as_micros_usd())
    });
    let status = if input.is_some() && cache_creation_5m.is_some() && cache_read.is_some() {
        "known"
    } else {
        if let Some(mut fallback) = anthropic_family_cache_pricing_summary(&normalized) {
            fallback.native = native;
            return fallback;
        }
        "unknown"
    };

    CachePricingSummary {
        native,
        status: status.to_owned(),
        input_micros_per_million: input,
        cache_creation_5m_micros_per_million: cache_creation_5m,
        cache_creation_1h_micros_per_million: cache_creation_1h,
        cache_read_micros_per_million: cache_read,
    }
}

pub(super) fn native_cache_pricing_from_catalog(
    pricing: cc_lb_pricing::ProviderNativePricing,
) -> NativeCachePricing {
    let mut rates = Vec::with_capacity(5 + pricing.total_write.len());
    rates.push(native_cache_pricing_rate(
        NativeCachePricingComponent::OrdinaryInput,
        None,
        pricing.ordinary_input,
    ));
    rates.push(native_cache_pricing_rate(
        NativeCachePricingComponent::CachedInputRead,
        None,
        pricing.cached_input_read,
    ));
    for (lifetime, rate) in pricing.total_write {
        rates.push(native_cache_pricing_rate(
            NativeCachePricingComponent::TotalWrite,
            Some(native_cache_pricing_lifetime(lifetime)),
            rate,
        ));
    }
    rates.push(native_cache_pricing_rate(
        NativeCachePricingComponent::ExplicitResourceToken,
        None,
        pricing.explicit_resource.token,
    ));
    rates.push(native_cache_pricing_rate(
        NativeCachePricingComponent::ExplicitResourceStorage,
        Some(NativeCachePricingLifetime::ExplicitResourceTtl),
        pricing.explicit_resource.storage,
    ));
    rates.push(native_cache_pricing_rate(
        NativeCachePricingComponent::ExplicitResourceTtl,
        Some(NativeCachePricingLifetime::ExplicitResourceTtl),
        pricing.explicit_resource.ttl,
    ));
    NativeCachePricing {
        version: 1,
        provider: native_cache_pricing_provider(pricing.provider),
        rates,
    }
}

pub(super) fn native_cache_pricing_provider(
    provider: cc_lb_pricing::NativePricingProvider,
) -> NativeCachePricingProvider {
    match provider {
        cc_lb_pricing::NativePricingProvider::Anthropic => NativeCachePricingProvider::Anthropic,
        cc_lb_pricing::NativePricingProvider::OpenAi => NativeCachePricingProvider::OpenAi,
        cc_lb_pricing::NativePricingProvider::Gemini => NativeCachePricingProvider::Gemini,
        cc_lb_pricing::NativePricingProvider::Unknown => NativeCachePricingProvider::Unknown,
    }
}

pub(super) fn native_cache_pricing_lifetime(
    lifetime: cc_lb_pricing::NativePricingLifetime,
) -> NativeCachePricingLifetime {
    match lifetime {
        cc_lb_pricing::NativePricingLifetime::ModelRetention => {
            NativeCachePricingLifetime::ModelRetention
        }
        cc_lb_pricing::NativePricingLifetime::ThirtyMinutes => {
            NativeCachePricingLifetime::ThirtyMinutes
        }
        cc_lb_pricing::NativePricingLifetime::OneHour => NativeCachePricingLifetime::OneHour,
        cc_lb_pricing::NativePricingLifetime::TwelveHours => {
            NativeCachePricingLifetime::TwelveHours
        }
        cc_lb_pricing::NativePricingLifetime::ExplicitResourceTtl => {
            NativeCachePricingLifetime::ExplicitResourceTtl
        }
    }
}

pub(super) fn native_cache_pricing_rate(
    component: NativeCachePricingComponent,
    lifetime: Option<NativeCachePricingLifetime>,
    rate: cc_lb_pricing::NativePricingRate,
) -> NativeCachePricingRate {
    let status = match rate.status {
        cc_lb_pricing::NativePricingStatus::Known
            if rate
                .price_per_million_usd
                .map(|price| price.as_micros_usd())
                != Some(0) =>
        {
            NativeCachePricingStatus::Known
        }
        cc_lb_pricing::NativePricingStatus::NotApplicable => {
            NativeCachePricingStatus::NotApplicable
        }
        cc_lb_pricing::NativePricingStatus::Known | cc_lb_pricing::NativePricingStatus::Unknown => {
            NativeCachePricingStatus::Unknown
        }
    };
    let micros_per_million = if status == NativeCachePricingStatus::Known {
        rate.price_per_million_usd
            .map(|price| price.as_micros_usd())
    } else {
        None
    };
    NativeCachePricingRate {
        component,
        lifetime,
        status,
        micros_per_million,
    }
}

pub(super) fn anthropic_family_cache_pricing_summary(model: &str) -> Option<CachePricingSummary> {
    let input_micros_per_million = anthropic_family_input_micros_per_million(model)?;
    Some(CachePricingSummary {
        native: None,
        status: "known".to_owned(),
        input_micros_per_million: Some(input_micros_per_million),
        cache_creation_5m_micros_per_million: Some(cache_creation_5m_micros_from_input(
            input_micros_per_million,
        )),
        cache_creation_1h_micros_per_million: Some(
            cc_lb_pricing::cache_creation_1h_micros_from_input(input_micros_per_million),
        ),
        cache_read_micros_per_million: Some(cache_read_micros_from_input(input_micros_per_million)),
    })
}

/// Base input price per million tokens for an Anthropic model, used only when the live price
/// catalog has no entry for it.
///
/// Matching is version-aware, not family-wide. A blanket `claude-opus => $15` was correct only
/// for Opus 4 and 4.1: Opus 4.5 through Opus 5 are $5/MTok, so the family rule overpriced every
/// newer Opus by 3x on input and, through the derived multipliers, on both cache-write rates and
/// the cache-read rate. Prefixes are ordered most-specific first because `claude-opus-4` is a
/// prefix of `claude-opus-4-5`.
///
/// Source: <https://platform.claude.com/docs/en/build-with-claude/prompt-caching#pricing>
/// (retrieved 2026-07-27).
pub(super) fn anthropic_family_input_micros_per_million(model: &str) -> Option<u64> {
    const FABLE_AND_MYTHOS: u64 = 10_000_000;
    const OPUS_CURRENT: u64 = 5_000_000;
    const OPUS_LEGACY: u64 = 15_000_000;
    const SONNET_STANDARD: u64 = 3_000_000;
    const HAIKU_CURRENT: u64 = 1_000_000;
    const HAIKU_LEGACY: u64 = 800_000;

    // Most specific first.
    const PREFIXES: [(&str, u64); 14] = [
        ("claude-fable", FABLE_AND_MYTHOS),
        ("claude-mythos", FABLE_AND_MYTHOS),
        ("claude-opus-4-1", OPUS_LEGACY),
        ("claude-opus-4-5", OPUS_CURRENT),
        ("claude-opus-4-6", OPUS_CURRENT),
        ("claude-opus-4-7", OPUS_CURRENT),
        ("claude-opus-4-8", OPUS_CURRENT),
        ("claude-opus-5", OPUS_CURRENT),
        // Bare `claude-opus-4` (no minor) is the retired original at legacy pricing. It must come
        // after every `claude-opus-4-N` arm above.
        ("claude-opus-4", OPUS_LEGACY),
        // Catch-all for future generations. Without it a `claude-opus-6` would return None and
        // lose cache pricing entirely, disabling cache-value scoring for that model; inheriting
        // current pricing is the cheaper wrong answer.
        ("claude-opus", OPUS_CURRENT),
        ("claude-sonnet-5", SONNET_STANDARD),
        ("claude-sonnet", SONNET_STANDARD),
        // Haiku 3.5 is addressed as `claude-3-5-haiku-*`; `claude-haiku-3-5` is not an id
        // Anthropic emits.
        ("claude-3-5-haiku", HAIKU_LEGACY),
        ("claude-haiku", HAIKU_CURRENT),
    ];

    PREFIXES
        .iter()
        .find(|(prefix, _)| model.starts_with(prefix))
        .map(|(_, micros)| *micros)
}

pub(super) fn cache_creation_5m_micros_from_input(input_micros: u64) -> u64 {
    (u128::from(input_micros) * 5 / 4)
        .try_into()
        .unwrap_or(u64::MAX)
}

pub(super) fn cache_read_micros_from_input(input_micros: u64) -> u64 {
    (u128::from(input_micros) / 10)
        .try_into()
        .unwrap_or(u64::MAX)
}

pub(super) fn saturating_u64_to_u32(value: u64) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

/// Token estimate of a prefix size, saturated to the `CacheScore` width.
pub(super) fn token_estimate_u32(bytes: PrefixBytes) -> u32 {
    saturating_u64_to_u32(bytes.as_token_estimate())
}

#[derive(Clone)]
pub struct PromptCacheObservationContext {
    pub(crate) upstream_id: Uuid,
    pub(crate) canonical_model_id: String,
    pub(crate) cache_breakpoints: Vec<CacheBreakpoint>,
    pub(crate) cacheable_breakpoint_prefix_keys: HashSet<String>,
    pub(crate) selected_match: Option<SelectedCacheMatch>,
    pub(crate) native_cache: Option<cc_lb_request_log::RequestNativeCacheObservation>,
    /// Direct non-blocking publication port for decoded observations.
    /// Replaces the old lifecycle-bus hop: enqueue failures are reported
    /// and never fail the request.
    pub(crate) sink: Arc<dyn PromptCacheObservationSinkLike>,
    /// Margin already subtracted at observation creation; reads compare
    /// `expires_at > now` only.
    pub(crate) grace_margin_secs: u64,
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct PromptCacheUsage {
    pub(crate) cache_creation_input_tokens: u64,
    pub(crate) cache_read_input_tokens: u64,
    pub(crate) cache_read_evidence: cc_lb_request_log::RequestCacheEvidence,
    pub(crate) cache_write_input_tokens: Option<u64>,
    pub(crate) cache_write_evidence: cc_lb_request_log::RequestCacheEvidence,
    /// True only for present valid positive reads and complete numeric zero
    /// creation evidence, never from accounting counters' zero defaults.
    pub(crate) read_only_evidence: bool,
}

impl From<&UsageCounts> for PromptCacheUsage {
    fn from(usage: &UsageCounts) -> Self {
        Self {
            cache_creation_input_tokens: usage.cache_creation_input_tokens,
            cache_read_input_tokens: usage.cache_read_input_tokens,
            cache_read_evidence: request_cache_evidence(usage.cache_read_evidence),
            cache_write_input_tokens: usage.cache_write_input_tokens,
            cache_write_evidence: request_cache_evidence(usage.cache_write_evidence),
            read_only_evidence: usage.has_affirmative_read_only_cache_usage(),
        }
    }
}

pub(super) fn request_cache_evidence(
    evidence: usage_parser::CacheTokenEvidence,
) -> cc_lb_request_log::RequestCacheEvidence {
    match evidence {
        usage_parser::CacheTokenEvidence::Unreported => {
            cc_lb_request_log::RequestCacheEvidence::Unreported
        }
        usage_parser::CacheTokenEvidence::Zero => cc_lb_request_log::RequestCacheEvidence::Zero,
        usage_parser::CacheTokenEvidence::Positive => {
            cc_lb_request_log::RequestCacheEvidence::Positive
        }
        usage_parser::CacheTokenEvidence::Invalid => {
            cc_lb_request_log::RequestCacheEvidence::Invalid
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DecodedPromptCacheObservation {
    pub(crate) prefix_hash: String,
    pub(crate) ttl_class: TtlClass,
    pub(crate) expires_at_unix_secs: u64,
    pub(crate) prefix_content_block_index: u32,
    pub(crate) estimated_prefix_bytes: PrefixBytes,
    pub(super) kind: DecodedPromptCacheObservationKind,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct PromptCacheObservationDecodeResult {
    pub(crate) observations: Vec<DecodedPromptCacheObservation>,
    pub(crate) dropped_below_threshold: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DecodedPromptCacheObservationKind {
    Hit,
    Write,
}

pub(crate) fn prompt_cache_observation_context(
    view: &DynamicView,
    upstream_id: Uuid,
    canonical_model_id: &str,
    cache_breakpoints: &[CacheBreakpoint],
    cacheable_breakpoint_prefix_keys: &HashSet<String>,
    selected_match: Option<SelectedCacheMatch>,
    native_cache: Option<cc_lb_request_log::RequestNativeCacheObservation>,
) -> Option<PromptCacheObservationContext> {
    if canonical_model_id.is_empty() || (cache_breakpoints.is_empty() && native_cache.is_none()) {
        return None;
    }
    let sink = Arc::clone(&view.prompt_cache_observation_sink);
    Some(PromptCacheObservationContext {
        upstream_id,
        canonical_model_id: canonical_model_id.to_owned(),
        cache_breakpoints: cache_breakpoints.to_vec(),
        cacheable_breakpoint_prefix_keys: cacheable_breakpoint_prefix_keys.clone(),
        selected_match,
        native_cache,
        sink,
        grace_margin_secs: view.prompt_cache_grace_margin_secs,
    })
}

pub(crate) fn decode_prompt_cache_observations_pure(
    context: &PromptCacheObservationContext,
    usage: PromptCacheUsage,
    now_unix_secs: u64,
) -> PromptCacheObservationDecodeResult {
    if usage.cache_creation_input_tokens == 0 && usage.cache_read_input_tokens == 0 {
        return PromptCacheObservationDecodeResult::default();
    }

    let hit = (usage.cache_read_input_tokens > 0)
        .then_some(context.selected_match.as_ref())
        .flatten();
    let grace_secs = context.grace_margin_secs;
    let mut observations = Vec::new();
    let mut dropped_below_threshold = 0_u32;
    if let Some(matched) = hit {
        observations.push(DecodedPromptCacheObservation {
            prefix_hash: matched.prefix_hash.clone(),
            ttl_class: matched.ttl_class,
            expires_at_unix_secs: prompt_cache_observation_expires_at(
                now_unix_secs,
                matched.ttl_class,
                grace_secs,
            ),
            prefix_content_block_index: matched.content_block_index,
            estimated_prefix_bytes: matched.estimated_prefix_bytes,
            kind: DecodedPromptCacheObservationKind::Hit,
        });
    } else if context.selected_match.is_none()
        && usage.cache_creation_input_tokens == 0
        && usage.cache_read_input_tokens > 0
        && usage.read_only_evidence
        && let Some(breakpoint) = context
            .cache_breakpoints
            .iter()
            .filter(|breakpoint| {
                context
                    .cacheable_breakpoint_prefix_keys
                    .contains(&breakpoint.prefix_hash)
            })
            .max_by_key(|breakpoint| breakpoint.block_index)
    {
        // Explicit and automatic controls are already resolved to the same
        // breakpoint representation. Infer only its deepest eligible boundary,
        // never a lookback key or a replacement for a carried predicted match.
        observations.push(DecodedPromptCacheObservation {
            prefix_hash: breakpoint.prefix_hash.clone(),
            ttl_class: breakpoint.requested_ttl,
            expires_at_unix_secs: prompt_cache_observation_expires_at(
                now_unix_secs,
                breakpoint.requested_ttl,
                grace_secs,
            ),
            prefix_content_block_index: breakpoint.block_index,
            estimated_prefix_bytes: breakpoint.prefix_bytes,
            kind: DecodedPromptCacheObservationKind::Hit,
        });
    }

    if usage.cache_creation_input_tokens > 0 {
        let anchor = hit.map(|matched| (matched.content_block_index, matched.ttl_class));
        for breakpoint in &context.cache_breakpoints {
            if !breakpoint_needs_write(breakpoint, anchor) {
                continue;
            }
            if !context
                .cacheable_breakpoint_prefix_keys
                .contains(&breakpoint.prefix_hash)
            {
                dropped_below_threshold = dropped_below_threshold.saturating_add(1);
                continue;
            }
            observations.push(DecodedPromptCacheObservation {
                prefix_hash: breakpoint.prefix_hash.clone(),
                ttl_class: breakpoint.requested_ttl,
                expires_at_unix_secs: prompt_cache_observation_expires_at(
                    now_unix_secs,
                    breakpoint.requested_ttl,
                    grace_secs,
                ),
                prefix_content_block_index: breakpoint.block_index,
                estimated_prefix_bytes: breakpoint.prefix_bytes,
                kind: DecodedPromptCacheObservationKind::Write,
            });
        }
    }

    PromptCacheObservationDecodeResult {
        observations,
        dropped_below_threshold,
    }
}

/// Publish decoded observations directly to the bounded sink.
///
/// Runs at the first valid usage evidence (SSE `message_start`, compat SSE
/// same point, or buffered response completion). Enqueue is non-blocking;
/// failures are logged and counted by the sink — the request continues.
pub(crate) fn publish_prompt_cache_observations(
    context: &PromptCacheObservationContext,
    decode: &PromptCacheObservationDecodeResult,
    now_unix_secs: u64,
) {
    for _ in 0..decode.dropped_below_threshold {
        cc_lb_observability::inc_cache_observation_dropped(
            cc_lb_observability::cache_observation_dropped_reason::BELOW_THRESHOLD,
        );
    }
    let sink = &context.sink;
    for observation in &decode.observations {
        let record = PromptCacheObservationRecord {
            upstream_id: context.upstream_id,
            canonical_model_id: context.canonical_model_id.clone(),
            v3_prefix_key: observation.prefix_hash.clone(),
            ttl_class: observation.ttl_class,
            expires_at_unix_secs: observation.expires_at_unix_secs,
            last_observed_at_unix_secs: now_unix_secs,
            hash_schema_version: HASH_SCHEMA_VERSION,
            prefix_content_block_index: observation.prefix_content_block_index,
            estimated_prefix_tokens: observation.estimated_prefix_bytes.get(),
            token_estimate_source: PrefixBytes::ESTIMATE_SOURCE.to_owned(),
        };
        if let Err(error) = sink.enqueue(record) {
            match error {
                PromptCacheObservationEnqueueError::ChannelFull => {}
                PromptCacheObservationEnqueueError::ChannelClosed => {
                    tracing::warn!("prompt cache observation sink is closed");
                }
            }
        }
    }
}

pub(crate) fn publish_prompt_cache_observations_with_usage(
    context: &PromptCacheObservationContext,
    decode: &PromptCacheObservationDecodeResult,
    usage: PromptCacheUsage,
    now_unix_secs: u64,
) {
    publish_prompt_cache_observations(context, decode, now_unix_secs);
    publish_provider_cache_observation(context, usage, now_unix_secs);
}

pub(super) fn publish_provider_cache_observation(
    context: &PromptCacheObservationContext,
    usage: PromptCacheUsage,
    now_unix_secs: u64,
) {
    let Some(native) = context.native_cache.as_ref() else {
        return;
    };
    let (_, observed_cached_tokens, observed_write_tokens) = provider_cache::project_usage(
        native,
        usage.cache_read_evidence,
        usage.cache_read_input_tokens,
        usage.cache_write_evidence,
        usage.cache_write_input_tokens,
    );
    let record = ProviderPromptCacheObservationRecord {
        upstream_id: context.upstream_id,
        provider: native.provider.clone(),
        canonical_model_id: context.canonical_model_id.clone(),
        mode: native.mode.clone(),
        isolation_scope: native.scope.clone().unwrap_or_default(),
        identity_hash: native.identity_hash.clone().unwrap_or_default(),
        boundary: native.boundary.clone().unwrap_or_default(),
        evidence_source: native.evidence_source.clone().unwrap_or_default(),
        observed_cached_tokens,
        observed_write_tokens,
        retention_secs: native.retention_secs,
        expires_at_unix_secs: native.expires_at_unix_secs.or_else(|| {
            native
                .retention_secs
                .map(|ttl| now_unix_secs.saturating_add(ttl))
        }),
        last_observed_at_unix_secs: now_unix_secs,
    };
    if let Err(error) = context.sink.enqueue_provider(record) {
        match error {
            PromptCacheObservationEnqueueError::ChannelFull => {}
            PromptCacheObservationEnqueueError::ChannelClosed => {
                tracing::warn!("provider prompt cache observation sink is closed");
            }
        }
    }
}

pub(super) fn prompt_cache_observation_expires_at(
    now_unix_secs: u64,
    ttl_class: TtlClass,
    grace_secs: u64,
) -> u64 {
    now_unix_secs
        .saturating_add(prompt_cache_ttl_secs(ttl_class))
        .saturating_sub(grace_secs)
}

pub(super) fn prompt_cache_ttl_secs(ttl_class: TtlClass) -> u64 {
    match ttl_class {
        TtlClass::Ephemeral5m => 5 * 60,
        TtlClass::Ephemeral1h => 60 * 60,
    }
}

#[derive(Clone, Default)]
pub(crate) struct PromptCacheBundle {
    pub(crate) cache_breakpoints: Vec<CacheBreakpoint>,
    pub(crate) cacheable_breakpoint_prefix_keys: HashSet<String>,
    pub(crate) request_breakpoints: Vec<RequestCacheBreakpoint>,
    pub(crate) cache_prefix_hash: Option<String>,
}

#[derive(Clone, Default)]
pub(crate) struct PromptCacheBundles {
    pub(crate) official: PromptCacheBundle,
    pub(crate) literal: PromptCacheBundle,
}

impl PromptCacheBundles {
    #[cfg(test)]
    pub(super) fn single(cache_breakpoints: &[CacheBreakpoint]) -> Self {
        let request_breakpoints = cache_breakpoints
            .iter()
            .map(request_cache_breakpoint_from_plugin)
            .collect::<Vec<_>>();
        let cache_prefix_hash = cache_breakpoints
            .last()
            .map(|breakpoint| breakpoint.prefix_hash.clone());
        let cacheable_breakpoint_prefix_keys = cache_breakpoints
            .iter()
            .map(|breakpoint| breakpoint.prefix_hash.clone())
            .collect();
        let bundle = PromptCacheBundle {
            cache_breakpoints: cache_breakpoints.to_vec(),
            cacheable_breakpoint_prefix_keys,
            request_breakpoints,
            cache_prefix_hash,
        };
        Self {
            official: bundle.clone(),
            literal: bundle,
        }
    }

    pub(super) fn for_upstream(&self, upstream: &UpstreamRecord) -> &PromptCacheBundle {
        match prompt_cache_policy_for_upstream(upstream) {
            PromptCachePolicy::AnthropicOfficial => &self.official,
            PromptCachePolicy::Literal => &self.literal,
        }
    }
}

#[cfg(test)]
pub(super) fn request_cache_breakpoint_from_plugin(
    breakpoint: &CacheBreakpoint,
) -> RequestCacheBreakpoint {
    RequestCacheBreakpoint {
        block_index: u64::from(breakpoint.block_index),
        source: request_breakpoint_source_from_plugin(breakpoint.source),
        path: breakpoint.path.clone(),
        message_index: breakpoint.message_index.map(u64::from),
        ttl: Some(match breakpoint.requested_ttl {
            TtlClass::Ephemeral1h => "1h".to_owned(),
            TtlClass::Ephemeral5m => "5m".to_owned(),
        }),
        prefix_hash: breakpoint.prefix_hash.clone(),
        prefix_token_count: breakpoint.prefix_bytes.get(),
        lookback_prefixes: breakpoint
            .lookback_prefixes
            .iter()
            .map(|prefix| RequestCacheLookbackPrefix {
                prefix_hash: prefix.prefix_hash.clone(),
                content_block_index: u64::from(prefix.content_block_index),
                lookback_distance: u64::from(prefix.lookback_distance),
            })
            .collect(),
        token_estimate_source: Some(V3_TOKEN_ESTIMATE_SOURCE.to_owned()),
    }
}

#[cfg(test)]
pub(super) fn request_breakpoint_source_from_plugin(
    source: CacheBreakpointSource,
) -> RequestCacheBreakpointSource {
    match source {
        CacheBreakpointSource::Tools => RequestCacheBreakpointSource::Tools,
        CacheBreakpointSource::System => RequestCacheBreakpointSource::System,
        CacheBreakpointSource::Message => RequestCacheBreakpointSource::Message,
    }
}

pub(crate) fn prompt_cache_policy_for_upstream(upstream: &UpstreamRecord) -> PromptCachePolicy {
    let official_kind = matches!(
        upstream.kind,
        StorageUpstreamKind::AnthropicApiKey | StorageUpstreamKind::AnthropicOauth
    );
    let official_url = upstream
        .base_url
        .as_ref()
        .is_none_or(is_official_anthropic_base_url);
    if official_kind && official_url {
        PromptCachePolicy::AnthropicOfficial
    } else {
        PromptCachePolicy::Literal
    }
}

pub(super) fn is_official_anthropic_base_url(url: &Url) -> bool {
    url.scheme() == "https"
        && url.host_str() == Some("api.anthropic.com")
        && url.port().is_none_or(|port| port == 443)
        && url.username().is_empty()
        && url.password().is_none()
}

pub(super) fn prompt_cache_bundle_from_analysis(
    analysis: &V3PromptCacheAnalysis,
    cacheable_prefix_keys: HashSet<String>,
) -> PromptCacheBundle {
    prompt_cache_bundle_from_breakpoints(&analysis.breakpoints, cacheable_prefix_keys)
}

pub(super) fn prompt_cache_bundle_from_breakpoints(
    breakpoints: &[crate::prompt_cache_simulator::V3PromptCacheBreakpoint],
    cacheable_prefix_keys: HashSet<String>,
) -> PromptCacheBundle {
    let cache_breakpoints = breakpoints
        .iter()
        .map(plugin_cache_breakpoint_from_v3)
        .collect();
    PromptCacheBundle {
        request_breakpoints: breakpoints
            .iter()
            .map(request_cache_breakpoint_from_v3)
            .collect(),
        cache_prefix_hash: breakpoints
            .last()
            .map(|breakpoint| breakpoint.prefix_key.clone()),
        cacheable_breakpoint_prefix_keys: cacheable_prefix_keys,
        cache_breakpoints,
    }
}

pub(super) fn request_cache_breakpoint_from_v3(
    breakpoint: &crate::prompt_cache_simulator::V3PromptCacheBreakpoint,
) -> RequestCacheBreakpoint {
    RequestCacheBreakpoint {
        block_index: breakpoint.block_index,
        source: request_breakpoint_source_from_v3(breakpoint.source),
        path: breakpoint.path.clone(),
        message_index: breakpoint.message_index,
        ttl: breakpoint.ttl.clone(),
        prefix_hash: breakpoint.prefix_key.clone(),
        prefix_token_count: breakpoint.prefix_bytes.get(),
        lookback_prefixes: breakpoint
            .lookback_prefixes
            .iter()
            .map(|prefix| RequestCacheLookbackPrefix {
                prefix_hash: prefix.prefix_key.clone(),
                content_block_index: prefix.content_block_index,
                lookback_distance: prefix.lookback_distance,
            })
            .collect(),
        token_estimate_source: Some(V3_TOKEN_ESTIMATE_SOURCE.to_owned()),
    }
}

pub(super) fn plugin_cache_breakpoint_from_v3(
    breakpoint: &crate::prompt_cache_simulator::V3PromptCacheBreakpoint,
) -> CacheBreakpoint {
    CacheBreakpoint {
        block_index: saturating_u64_to_u32(breakpoint.block_index),
        source: plugin_cache_breakpoint_source(request_breakpoint_source_from_v3(
            breakpoint.source,
        )),
        path: breakpoint.path.clone(),
        message_index: breakpoint.message_index.map(saturating_u64_to_u32),
        prefix_hash: breakpoint.prefix_key.clone(),
        prefix_bytes: breakpoint.prefix_bytes,
        requested_ttl: plugin_ttl_class(breakpoint.ttl.as_deref()),
        lookback_prefixes: breakpoint
            .lookback_prefixes
            .iter()
            .map(|prefix| CacheLookbackPrefix {
                prefix_hash: prefix.prefix_key.clone(),
                content_block_index: saturating_u64_to_u32(prefix.content_block_index),
                lookback_distance: saturating_u64_to_u32(prefix.lookback_distance),
            })
            .collect(),
        token_estimate_source: Some(V3_TOKEN_ESTIMATE_SOURCE.to_owned()),
    }
}

#[derive(Clone, Default)]
pub(crate) struct RequestCacheMetadata {
    pub(crate) request_json: Option<Value>,
    pub(crate) thread_id: Option<String>,
    pub(crate) observed_session_id: Option<String>,
    pub(crate) request_kind: Option<String>,
    pub(crate) explicit_request_class: Option<String>,
    pub(crate) explicit_auxiliary: bool,
    pub(crate) claude_agent_id: Option<String>,
    pub(crate) claude_parent_agent_id: Option<String>,
    pub(crate) parent_session_id: Option<String>,
    pub(crate) client_app: Option<String>,
    pub(crate) session_id_source: Option<String>,
    pub(crate) cache_keepalive_surface: Option<String>,
    pub(crate) cache_keepalive_ttl: Option<cc_lb_storage_api::CacheTtl>,
    pub(crate) openai_selected_boundary: Option<String>,
    pub(crate) openai_replay_identity: Option<String>,
    pub(super) requested_service_tier: Option<String>,
    pub(super) message_id: Option<String>,
    pub(super) message_index: Option<u64>,
    pub(super) message_count: Option<u64>,
    pub(super) thinking_budget_tokens: Option<u64>,
    pub(super) reasoning_effort: Option<String>,
    pub(super) thinking_level: Option<String>,
    pub(super) cache_control_message_indices: Vec<u64>,
    pub(crate) cache_breakpoints: Vec<RequestCacheBreakpoint>,
    pub(crate) cache_prefix_hash: Option<String>,
    pub(crate) bundles: PromptCacheBundles,
    pub(crate) native_cache: Option<cc_lb_request_log::RequestNativeCacheObservation>,
    pub(super) canonical_model_id: String,
}

impl RequestCacheMetadata {
    pub(super) fn plugin_cache_breakpoints(&self) -> Vec<CacheBreakpoint> {
        self.cache_breakpoints
            .iter()
            .map(|breakpoint| CacheBreakpoint {
                block_index: saturating_u64_to_u32(breakpoint.block_index),
                source: plugin_cache_breakpoint_source(breakpoint.source),
                path: breakpoint.path.clone(),
                message_index: breakpoint.message_index.map(saturating_u64_to_u32),
                prefix_hash: breakpoint.prefix_hash.clone(),
                // Produced in-process by the current estimator (see
                // `request_cache_metadata_from_value_with_analysis_for_capability`).
                prefix_bytes: PrefixBytes::new(breakpoint.prefix_token_count),
                requested_ttl: plugin_ttl_class(breakpoint.ttl.as_deref()),
                lookback_prefixes: breakpoint
                    .lookback_prefixes
                    .iter()
                    .map(|prefix| CacheLookbackPrefix {
                        prefix_hash: prefix.prefix_hash.clone(),
                        content_block_index: saturating_u64_to_u32(prefix.content_block_index),
                        lookback_distance: saturating_u64_to_u32(prefix.lookback_distance),
                    })
                    .collect(),
                token_estimate_source: breakpoint.token_estimate_source.clone(),
            })
            .collect()
    }
}

pub(super) fn plugin_cache_breakpoint_source(
    source: RequestCacheBreakpointSource,
) -> CacheBreakpointSource {
    match source {
        RequestCacheBreakpointSource::Tools => CacheBreakpointSource::Tools,
        RequestCacheBreakpointSource::System => CacheBreakpointSource::System,
        RequestCacheBreakpointSource::Message => CacheBreakpointSource::Message,
    }
}

pub(super) fn plugin_ttl_class(ttl: Option<&str>) -> TtlClass {
    match ttl {
        Some(ttl) if ttl.eq_ignore_ascii_case("1h") => TtlClass::Ephemeral1h,
        _ => TtlClass::Ephemeral5m,
    }
}
pub(super) fn prompt_cache_token_scope(headers: &HeaderMap) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"cc-lb-token-cache-scope-v1\0");
    let credential = headers
        .get("x-api-key")
        .map(|value| ("x-api-key", value))
        .or_else(|| {
            headers
                .get("authorization")
                .map(|value| ("authorization", value))
        });
    if let Some((name, value)) = credential {
        hasher.update(&(name.len() as u64).to_le_bytes());
        hasher.update(name.as_bytes());
        hasher.update(&(value.as_bytes().len() as u64).to_le_bytes());
        hasher.update(value.as_bytes());
    } else {
        hasher.update(b"unauthenticated");
    }
    *hasher.finalize().as_bytes()
}

#[cfg(test)]
pub(super) fn request_cache_metadata(headers: &HeaderMap, body: &Bytes) -> RequestCacheMetadata {
    let value = sonic_rs::from_slice::<Value>(body).ok();
    request_cache_metadata_from_value(headers, value.as_ref())
}

#[cfg(test)]
pub(super) fn request_cache_metadata_from_value(
    headers: &HeaderMap,
    value: Option<&Value>,
) -> RequestCacheMetadata {
    let analysis = value.map(|value| {
        let canonical_model = value
            .get("model")
            .and_then(Value::as_str)
            .map(|model| canonical_model_id(Provider::Anthropic, model))
            .unwrap_or_default();
        PromptCacheAnalysisOutput {
            analysis: analyze_v3_prompt_cache(value, canonical_model),
            cacheable_breakpoint_prefix_keys: Vec::new(),
            literal_cacheable_breakpoint_prefix_keys: Vec::new(),
            timings: PromptCacheAnalysisTimings::default(),
        }
    });
    request_cache_metadata_from_value_with_analysis_for_capability(
        headers, value, analysis, None, None,
    )
}

pub(super) fn request_reasoning_metadata(
    value: &Value,
    capability: Option<ProviderCapability>,
) -> (Option<u64>, Option<String>) {
    let anthropic_thinking_budget = || {
        value
            .get("thinking")
            .filter(|thinking| thinking.get("type").and_then(Value::as_str) == Some("enabled"))
            .and_then(|thinking| thinking.get("budget_tokens"))
            .and_then(Value::as_u64)
            .filter(|budget| *budget <= 9_007_199_254_740_991)
    };
    let anthropic_reasoning_effort = || {
        value
            .get("output_config")
            .and_then(|config| config.get("effort"))
            .and_then(Value::as_str)
            .filter(|effort| !effort.is_empty() && effort.len() <= 64)
            .map(str::to_owned)
    };
    match capability {
        // Callers without a capability retain the historical Anthropic-shaped
        // extraction used by cache and preview metadata.
        None | Some(ProviderCapability::AnthropicMessages) => {
            (anthropic_thinking_budget(), anthropic_reasoning_effort())
        }
        Some(
            ProviderCapability::OpenaiChatCompletions
            | ProviderCapability::GeminiOpenaiChatCompletions,
        ) => (
            None,
            value
                .get("reasoning_effort")
                .and_then(Value::as_str)
                .filter(|effort| !effort.is_empty() && effort.len() <= 64)
                .map(str::to_owned),
        ),
        Some(ProviderCapability::OpenaiResponses) => (
            None,
            value
                .get("reasoning")
                .and_then(|reasoning| reasoning.get("effort"))
                .and_then(Value::as_str)
                .filter(|effort| !effort.is_empty() && effort.len() <= 64)
                .map(str::to_owned),
        ),
        Some(ProviderCapability::GeminiGenerateContent) => (
            value
                .get("generationConfig")
                .and_then(|config| config.get("thinkingConfig"))
                .and_then(|thinking| {
                    thinking
                        .get("thinkingBudget")
                        .or_else(|| thinking.get("thinking_budget"))
                })
                .and_then(Value::as_u64)
                .filter(|budget| *budget <= 9_007_199_254_740_991),
            None,
        ),
        _ => (None, None),
    }
}

/// Return the service tier sent to the selected upstream after compatibility
/// coercion, preserving raw values for non-OAuth traffic.
pub(super) fn compat_service_tier_for_upstream(
    capability: ProviderCapability,
    upstream_kind: StorageUpstreamKind,
    requested: Option<&str>,
    oauth_coerced: bool,
) -> Option<String> {
    let requested = requested?;
    if matches!(
        capability,
        ProviderCapability::OpenaiResponses | ProviderCapability::OpenaiChatCompletions
    ) && upstream_kind == StorageUpstreamKind::OpenaiOauth
        && oauth_coerced
        && matches!(requested, "auto" | "flex")
    {
        return Some("default".to_owned());
    }
    Some(requested.to_owned())
}

/// Normalize field names only. Keep the requested value, including unknown
/// casing, visible to routing and admission rather than treating it as absent.
pub(super) fn request_service_tier(
    value: &Value,
    capability: Option<ProviderCapability>,
) -> Option<&str> {
    let tier = value.get("service_tier").or_else(|| {
        (capability == Some(ProviderCapability::GeminiGenerateContent))
            .then(|| value.get("serviceTier"))
            .flatten()
    })?;
    let tier = tier.as_str()?;
    if capability == Some(ProviderCapability::GeminiGenerateContent) && tier.trim().is_empty() {
        return None;
    }
    Some(tier)
}

pub(super) fn request_thinking_level(
    value: &Value,
    capability: Option<ProviderCapability>,
) -> Option<String> {
    if capability != Some(ProviderCapability::GeminiGenerateContent) {
        return None;
    }
    value
        .get("generationConfig")
        .and_then(|config| config.get("thinkingConfig"))
        .and_then(|thinking| {
            thinking
                .get("thinkingLevel")
                .or_else(|| thinking.get("thinking_level"))
        })
        .and_then(Value::as_str)
        .filter(|level| !level.is_empty() && level.len() <= 64)
        .map(str::to_owned)
}

pub(super) fn bounded_openai_replay_component(raw: &str) -> String {
    let digest = Sha256::digest(raw.as_bytes());
    format!(
        "sha256:{}",
        digest
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    )
}
pub(super) fn openai_input_digest_and_visible_tokens(value: &Value) -> (String, u64) {
    let Some(input) = value.get("input") else {
        return (String::new(), 0);
    };
    let Ok(input_bytes) = sonic_rs::to_vec(input) else {
        return (String::new(), 0);
    };
    let visible_tokens = (input_bytes.len() as u64 / 4).min(10_000_000);
    let digest = Sha256::digest(&input_bytes);
    let digest = digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    (digest, visible_tokens)
}

#[cfg(test)]
pub(crate) fn openai_replay_identity(
    value: &Value,
    headers: &HeaderMap,
    model: &str,
    ttl: Option<cc_lb_storage_api::CacheTtl>,
    prefix_hash: Option<&str>,
) -> Option<String> {
    let (input_digest, _) = openai_input_digest_and_visible_tokens(value);
    openai_replay_identity_with_boundary(
        value,
        headers,
        model,
        ttl,
        prefix_hash,
        None,
        &input_digest,
    )
}

pub(super) fn openai_replay_identity_with_boundary(
    value: &Value,
    headers: &HeaderMap,
    model: &str,
    ttl: Option<cc_lb_storage_api::CacheTtl>,
    prefix_hash: Option<&str>,
    selected_boundary: Option<&str>,
    input_digest: &str,
) -> Option<String> {
    let prompt_cache_key = value
        .get("prompt_cache_key")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let conversation_identity = match value.get("conversation") {
        Some(Value::String(id)) => format!("string:{id}"),
        Some(Value::Object(object)) => object
            .get("id")
            .and_then(Value::as_str)
            .map(|id| format!("object:{id}"))
            .unwrap_or_default(),
        _ => String::new(),
    };
    let organization = value
        .get("organization")
        .or_else(|| value.get("organization_id"))
        .and_then(Value::as_str)
        .or_else(|| {
            headers
                .get("openai-organization")
                .and_then(|value| value.to_str().ok())
        })
        .unwrap_or_default();
    let processing_region = value
        .get("processing_region")
        .or_else(|| value.get("inference_geo"))
        .and_then(Value::as_str)
        .or_else(|| {
            headers
                .get("openai-processing-region")
                .and_then(|value| value.to_str().ok())
        })
        .unwrap_or_default();
    let lifetime = ttl.map(|ttl| match ttl {
        cc_lb_storage_api::CacheTtl::Ttl30m => "30m",
        cc_lb_storage_api::CacheTtl::Ttl12h => "12h",
        cc_lb_storage_api::CacheTtl::Ttl5m => "5m",
        cc_lb_storage_api::CacheTtl::Ttl1h => "1h",
    });
    let namespace = format!(
        "openai-replay-v2|model={}|lifetime={}|prefix={}|boundary={}|organization={}|region={}|prompt_cache_key={}|conversation={}|input={}",
        bounded_openai_replay_component(model),
        bounded_openai_replay_component(lifetime.unwrap_or("unknown")),
        bounded_openai_replay_component(prefix_hash.unwrap_or("")),
        bounded_openai_replay_component(selected_boundary.unwrap_or("unknown")),
        bounded_openai_replay_component(organization),
        bounded_openai_replay_component(processing_region),
        bounded_openai_replay_component(prompt_cache_key),
        bounded_openai_replay_component(&conversation_identity),
        bounded_openai_replay_component(input_digest),
    );
    let digest = Sha256::digest(namespace.as_bytes());
    Some(format!(
        "openai-replay-v2:{}",
        digest
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    ))
}

pub(super) fn request_cache_metadata_from_value_with_analysis_for_capability(
    headers: &HeaderMap,
    value: Option<&Value>,
    analysis: Option<PromptCacheAnalysisOutput>,
    capability: Option<ProviderCapability>,
    path_model: Option<&str>,
) -> RequestCacheMetadata {
    let identity = request_identity_metadata(headers, value, capability);
    let thread_id = identity.thread_id.clone();
    let observed_session_id = identity.observed_session_id.clone();
    let request_kind = identity.request_kind.clone();
    let explicit_request_class = identity.explicit_request_class.clone();
    let explicit_auxiliary = identity.explicit_auxiliary;
    let claude_agent_id = identity.claude_agent_id.clone();
    let claude_parent_agent_id = identity.claude_parent_agent_id.clone();
    let parent_session_id = identity.parent_session_id.clone();
    let client_app = identity.client_app.clone();
    let session_id_source = identity.session_id_source.clone();

    let Some(value) = value else {
        return RequestCacheMetadata {
            thread_id,
            observed_session_id,
            request_kind,
            explicit_request_class,
            explicit_auxiliary,
            claude_agent_id,
            claude_parent_agent_id,
            parent_session_id,
            client_app,
            session_id_source,
            canonical_model_id: path_model
                .map(|model| canonical_model_for_capability(capability, model).to_owned())
                .unwrap_or_default(),
            ..Default::default()
        };
    };
    let model = if capability == Some(ProviderCapability::GeminiGenerateContent) {
        path_model
    } else if capability
        .is_some_and(|capability| capability.provider() == cc_lb_domain::Provider::Gemini)
    {
        path_model.or_else(|| value.get("model").and_then(Value::as_str))
    } else {
        value.get("model").and_then(Value::as_str)
    };
    let canonical_model_id = model
        .map(|model| canonical_model_for_capability(capability, model))
        .unwrap_or_default()
        .to_owned();
    let modern_openai_interpretation = if capability == Some(ProviderCapability::OpenaiResponses)
        && crate::cache_keepalive::openai::is_gpt56_or_newer(&canonical_model_id)
    {
        Some(crate::cache_keepalive::openai::interpret_modern(value))
    } else {
        None
    };
    let native_cache = capability.and_then(|capability| {
        provider_cache::analyze_with_openai_interpretation(
            capability,
            value,
            &canonical_model_id,
            headers,
            modern_openai_interpretation.as_ref().map(Option::as_ref),
        )
    });
    let requested_service_tier = request_service_tier(value, capability).map(str::to_owned);

    let messages = value.get("messages").and_then(Value::as_array);
    let message_count = messages.map(|items| items.len() as u64);
    let message_index = message_count.and_then(|count| count.checked_sub(1));
    let message_id = messages
        .and_then(|items| items.last())
        .and_then(|message| message.get("id"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);

    let output = analysis.unwrap_or_else(|| PromptCacheAnalysisOutput {
        analysis: analyze_v3_prompt_cache(value, &canonical_model_id),
        cacheable_breakpoint_prefix_keys: Vec::new(),
        literal_cacheable_breakpoint_prefix_keys: Vec::new(),
        timings: PromptCacheAnalysisTimings::default(),
    });
    let literal_cacheable_prefix_keys = output
        .literal_cacheable_breakpoint_prefix_keys
        .into_iter()
        .collect::<HashSet<_>>();
    let official_cacheable_prefix_keys = output
        .cacheable_breakpoint_prefix_keys
        .iter()
        .cloned()
        .collect::<HashSet<_>>();
    let analysis = output.analysis;
    let official_bundle =
        prompt_cache_bundle_from_analysis(&analysis, official_cacheable_prefix_keys.clone());
    let has_billing_header = analysis.blocks.iter().any(|block| {
        block.source == V3PromptCacheBlockSource::System
            && block.path == "system[0]"
            && block.value.get("type").and_then(Value::as_str) == Some("text")
            && block
                .value
                .get("text")
                .and_then(Value::as_str)
                .is_some_and(|text| text.starts_with("x-anthropic-billing-header:"))
    });
    let literal_bundle = if has_billing_header {
        prompt_cache_bundle_from_breakpoints(
            &analysis.literal_breakpoints,
            literal_cacheable_prefix_keys,
        )
    } else {
        official_bundle.clone()
    };
    let cache_breakpoints = official_bundle.request_breakpoints.clone();
    let raw_cache_breakpoints = &literal_bundle.request_breakpoints;
    let mut cache_control_message_indices = raw_cache_breakpoints
        .iter()
        .filter_map(|breakpoint| breakpoint.message_index)
        .collect::<Vec<_>>();
    cache_control_message_indices.sort_unstable();
    cache_control_message_indices.dedup();
    let (thinking_budget_tokens, reasoning_effort) = request_reasoning_metadata(value, capability);
    let cache_prefix_hash = cache_breakpoints
        .last()
        .map(|breakpoint| breakpoint.prefix_hash.clone());
    let (openai_input_digest, openai_visible_prefix_tokens) =
        if capability == Some(ProviderCapability::OpenaiResponses) {
            openai_input_digest_and_visible_tokens(value)
        } else {
            (String::new(), 0)
        };
    let (cache_keepalive_surface, cache_keepalive_ttl, openai_selected_boundary) =
        if capability == Some(ProviderCapability::OpenaiResponses) {
            let interpretation = modern_openai_interpretation.flatten();
            let lifetime = interpretation.as_ref().and_then(|value| value.lifetime);
            let eligible = lifetime.is_some_and(|lifetime| {
                crate::cache_keepalive::openai::eligible_prefix(
                    &canonical_model_id,
                    openai_visible_prefix_tokens,
                    Some(lifetime),
                )
            });
            if eligible {
                (
                    Some("responses".to_owned()),
                    lifetime.map(|lifetime| match lifetime {
                        crate::cache_keepalive::openai::Lifetime::ThirtyMinutes => {
                            cc_lb_storage_api::CacheTtl::Ttl30m
                        }
                        crate::cache_keepalive::openai::Lifetime::TwelveHours => {
                            cc_lb_storage_api::CacheTtl::Ttl12h
                        }
                    }),
                    interpretation.and_then(|value| value.selected_boundary),
                )
            } else {
                (None, None, None)
            }
        } else {
            (None, None, None)
        };
    let replay_identity = (capability == Some(ProviderCapability::OpenaiResponses))
        .then(|| {
            openai_replay_identity_with_boundary(
                value,
                headers,
                &canonical_model_id,
                cache_keepalive_ttl,
                cache_prefix_hash.as_deref(),
                openai_selected_boundary.as_deref(),
                &openai_input_digest,
            )
        })
        .flatten();
    RequestCacheMetadata {
        request_json: None,
        thread_id,
        observed_session_id,
        request_kind,
        explicit_request_class,
        explicit_auxiliary,
        claude_agent_id,
        claude_parent_agent_id,
        parent_session_id,
        client_app,
        session_id_source,
        cache_keepalive_surface,
        cache_keepalive_ttl,
        openai_selected_boundary,
        openai_replay_identity: replay_identity,
        requested_service_tier,
        message_id,
        message_index,
        message_count,
        thinking_budget_tokens,
        reasoning_effort,
        thinking_level: request_thinking_level(value, capability),
        cache_control_message_indices,
        cache_breakpoints,
        cache_prefix_hash,
        bundles: PromptCacheBundles {
            official: official_bundle,
            literal: literal_bundle,
        },
        native_cache,
        canonical_model_id,
    }
}

pub(super) fn request_breakpoint_source_from_v3(
    source: V3PromptCacheBlockSource,
) -> RequestCacheBreakpointSource {
    match source {
        V3PromptCacheBlockSource::Tools => RequestCacheBreakpointSource::Tools,
        V3PromptCacheBlockSource::System => RequestCacheBreakpointSource::System,
        V3PromptCacheBlockSource::Message => RequestCacheBreakpointSource::Message,
    }
}

#[cfg(test)]
mod cache_score_tests {
    use std::collections::BTreeMap;

    use super::{
        HASH_SCHEMA_VERSION, StorageUpstreamKind, anthropic_family_cache_pricing_summary,
        anthropic_family_input_micros_per_million, build_cache_score,
        cache_pricing_summary_for_model, compat_service_tier_for_upstream,
    };
    use crate::provider_capability::ProviderCapability;
    use cc_lb_domain::{
        CacheBreakpoint, CacheBreakpointSource, CacheLookbackPrefix, PrefixBytes, TtlClass,
        WarmCacheEntry,
    };

    use cc_lb_pricing::{
        CatalogSnapshot, CatalogStatus, NativePricingLifetime, NativePricingProvider,
        NativePricingRate, PriceCatalog, Pricing, ProviderNativePricing, TierRate, UsdPerMillion,
    };
    use proptest::prelude::*;

    #[test]
    fn build_cache_score_counts_missing_breakpoints_as_incremental_segments_by_ttl() {
        let score = build_cache_score(
            &[
                breakpoint(0, 100_000, TtlClass::Ephemeral1h),
                breakpoint(1, 300_000, TtlClass::Ephemeral5m),
                breakpoint(2, 500_000, TtlClass::Ephemeral5m),
            ],
            &[],
        )
        .expect("cache score for cacheable request");

        assert_eq!(score.predicted_cache_read_tokens, 0);
        assert_eq!(score.predicted_cache_creation_tokens_1h, 100_000);
        assert_eq!(score.predicted_cache_creation_tokens_5m, 400_000);
    }

    #[test]
    fn build_cache_score_only_counts_segments_after_longest_live_breakpoint() {
        let score = build_cache_score(
            &[
                breakpoint(0, 100_000, TtlClass::Ephemeral1h),
                breakpoint(1, 300_000, TtlClass::Ephemeral5m),
                breakpoint(2, 500_000, TtlClass::Ephemeral5m),
            ],
            &[warm_entry("bp-1", TtlClass::Ephemeral5m, 300_000)],
        )
        .expect("cache score for cacheable request");

        assert_eq!(score.predicted_cache_read_tokens, 300_000);
        assert_eq!(score.predicted_cache_creation_tokens_1h, 0);
        assert_eq!(score.predicted_cache_creation_tokens_5m, 200_000);
        assert_eq!(score.matched_breakpoint_index, Some(1));
    }

    #[test]
    fn build_cache_score_prices_read_from_matched_prefix_not_owning_breakpoint() {
        let breakpoint = CacheBreakpoint {
            block_index: 2,
            source: CacheBreakpointSource::Message,
            path: "messages.2.content".to_owned(),
            message_index: Some(2),
            prefix_hash: "bp-2".to_owned(),
            prefix_bytes: PrefixBytes::new(300_000),
            requested_ttl: TtlClass::Ephemeral5m,
            lookback_prefixes: vec![
                cc_lb_domain::CacheLookbackPrefix {
                    prefix_hash: "bp-2".to_owned(),
                    content_block_index: 2,
                    lookback_distance: 0,
                },
                cc_lb_domain::CacheLookbackPrefix {
                    prefix_hash: "warm-0".to_owned(),
                    content_block_index: 0,
                    lookback_distance: 2,
                },
            ],
            token_estimate_source: Some("test".to_owned()),
        };
        let score = build_cache_score(
            std::slice::from_ref(&breakpoint),
            &[warm_entry("warm-0", TtlClass::Ephemeral5m, 100_000)],
        )
        .expect("cache score for cacheable request");

        assert_eq!(score.predicted_cache_read_tokens, 100_000);
        assert_eq!(score.predicted_cache_creation_tokens_5m, 200_000);
        assert_eq!(score.matched_content_block_index, Some(0));
    }

    #[test]
    fn anthropic_family_cache_pricing_fallback_covers_opus_4_8_alias() {
        let pricing = anthropic_family_cache_pricing_summary("claude-opus-4-8")
            .expect("opus family fallback exists");

        assert_eq!(pricing.status, "known");
        assert_eq!(pricing.input_micros_per_million, Some(5_000_000));
        assert_eq!(
            pricing.cache_creation_5m_micros_per_million,
            Some(6_250_000)
        );
        assert_eq!(
            pricing.cache_creation_1h_micros_per_million,
            Some(10_000_000)
        );
        assert_eq!(pricing.cache_read_micros_per_million, Some(500_000));
    }

    #[test]
    fn anthropic_family_input_pricing_splits_opus_generations() {
        // A blanket `claude-opus => $15` overpriced every Opus from 4.5 on by 3x. Bare
        // `claude-opus-4` must not be shadowed by the `claude-opus-4-N` arms that precede it.
        for current in [
            "claude-opus-4-5",
            "claude-opus-4-5-20251101",
            "claude-opus-4-6",
            "claude-opus-4-7",
            "claude-opus-4-8",
            "claude-opus-4-8-20250514",
            "claude-opus-5",
        ] {
            assert_eq!(
                anthropic_family_input_micros_per_million(current),
                Some(5_000_000),
                "{current} is current-generation Opus pricing"
            );
        }
        for legacy in [
            "claude-opus-4",
            "claude-opus-4-1",
            "claude-opus-4-1-20250805",
        ] {
            assert_eq!(
                anthropic_family_input_micros_per_million(legacy),
                Some(15_000_000),
                "{legacy} is legacy Opus pricing"
            );
        }
    }

    #[test]
    fn anthropic_family_input_pricing_covers_haiku_and_sonnet_generations() {
        assert_eq!(
            anthropic_family_input_micros_per_million("claude-3-5-haiku-20241022"),
            Some(800_000),
            "Haiku 3.5 uses the pre-4.0 id ordering and is cheaper than Haiku 4.5"
        );
        assert_eq!(
            anthropic_family_input_micros_per_million("claude-haiku-4-5-20251001"),
            Some(1_000_000)
        );
        assert_eq!(
            anthropic_family_input_micros_per_million("claude-sonnet-5"),
            Some(3_000_000),
            "Sonnet 5 uses standard Sonnet rates after 2026-08-31"
        );
        assert_eq!(
            anthropic_family_input_micros_per_million("claude-sonnet-4-6"),
            Some(3_000_000)
        );
    }

    #[test]
    fn anthropic_family_cache_pricing_fallback_covers_fable_5() {
        let pricing = anthropic_family_cache_pricing_summary("claude-fable-5")
            .expect("fable family fallback exists");

        assert_eq!(pricing.status, "known");
        assert_eq!(pricing.input_micros_per_million, Some(10_000_000));
        assert_eq!(
            pricing.cache_creation_5m_micros_per_million,
            Some(12_500_000)
        );
        assert_eq!(
            pricing.cache_creation_1h_micros_per_million,
            Some(20_000_000)
        );
        assert_eq!(pricing.cache_read_micros_per_million, Some(1_000_000));
    }

    #[test]
    fn anthropic_family_cache_pricing_fallback_covers_mythos_5() {
        let pricing = anthropic_family_cache_pricing_summary("claude-mythos-5")
            .expect("mythos family fallback exists");

        assert_eq!(pricing.status, "known");
        assert_eq!(pricing.input_micros_per_million, Some(10_000_000));
        assert_eq!(
            pricing.cache_creation_5m_micros_per_million,
            Some(12_500_000)
        );
        assert_eq!(
            pricing.cache_creation_1h_micros_per_million,
            Some(20_000_000)
        );
        assert_eq!(pricing.cache_read_micros_per_million, Some(1_000_000));
    }

    #[test]
    fn anthropic_family_cache_pricing_fallback_leaves_non_claude_unknown() {
        assert!(anthropic_family_cache_pricing_summary("gpt-5.5").is_none());
    }

    #[test]
    fn cache_pricing_summary_uses_requested_service_tier() {
        // Given a catalog whose priority tier differs from every base cache rate.
        let catalog = PriceCatalog::new_empty();
        let mut snapshot = CatalogSnapshot::empty_cost_disabled();
        snapshot.status = CatalogStatus::Ok;
        snapshot.models.insert(
            "tier-model".to_owned(),
            Pricing {
                native: None,
                model: "tier-model".to_owned(),
                input_per_million_usd: UsdPerMillion::from_whole_usd(2),
                output_per_million_usd: UsdPerMillion::from_whole_usd(8),
                by_tier: BTreeMap::from([(
                    "priority".to_owned(),
                    TierRate {
                        input_per_million_usd: UsdPerMillion::from_whole_usd(3),
                        output_per_million_usd: UsdPerMillion::from_whole_usd(12),
                    },
                )]),
                input_image_per_million_usd: None,
                output_image_per_million_usd: None,
            },
        );
        snapshot
            .cache_creation_per_million_usd
            .insert("tier-model".to_owned(), UsdPerMillion::from_whole_usd(4));
        snapshot.cache_creation_per_million_usd_by_tier.insert(
            "tier-model".to_owned(),
            BTreeMap::from([("priority".to_owned(), UsdPerMillion::from_whole_usd(6))]),
        );
        snapshot
            .cache_read_per_million_usd
            .insert("tier-model".to_owned(), UsdPerMillion::from_whole_usd(1));
        snapshot.cache_read_per_million_usd_by_tier.insert(
            "tier-model".to_owned(),
            BTreeMap::from([("priority".to_owned(), UsdPerMillion::from_whole_usd(2))]),
        );
        catalog.install_snapshot(snapshot);

        // When the routing summary is built with the requested priority tier.
        let pricing = cache_pricing_summary_for_model(&catalog, "tier-model", Some("priority"));

        // Then all cache estimate rates reflect that tier: the 1h write derives from the tier's
        // own base input price (2.0x), not from the tier 5m price.
        assert_eq!(pricing.input_micros_per_million, Some(3_000_000));
        assert_eq!(
            pricing.cache_creation_5m_micros_per_million,
            Some(6_000_000)
        );
        assert_eq!(
            pricing.cache_creation_1h_micros_per_million,
            Some(6_000_000)
        );
        assert_eq!(pricing.cache_read_micros_per_million, Some(2_000_000));
        for alias in ["service_tier", "serviceTier"] {
            let request = serde_json::json!({(alias): "priority"});
            let metadata = super::request_cache_metadata_from_value_with_analysis_for_capability(
                &http::HeaderMap::new(),
                Some(&request),
                None,
                Some(super::ProviderCapability::GeminiGenerateContent),
                None,
            );
            let gemini_pricing = cache_pricing_summary_for_model(
                &catalog,
                "tier-model",
                metadata.requested_service_tier.as_deref(),
            );
            assert_eq!(gemini_pricing.input_micros_per_million, Some(3_000_000));
            assert_eq!(
                gemini_pricing.cache_read_micros_per_million,
                Some(2_000_000)
            );
        }
    }
    #[test]
    fn oauth_compat_service_tier_tracks_sent_value() {
        assert_eq!(
            compat_service_tier_for_upstream(
                ProviderCapability::OpenaiResponses,
                StorageUpstreamKind::OpenaiOauth,
                Some("auto"),
                true,
            )
            .as_deref(),
            Some("default")
        );
        assert_eq!(
            compat_service_tier_for_upstream(
                ProviderCapability::OpenaiChatCompletions,
                StorageUpstreamKind::OpenaiApiKey,
                Some("auto"),
                true,
            )
            .as_deref(),
            Some("auto")
        );
    }

    #[test]
    fn native_cache_pricing_context_maps_rates_without_legacy_fallback() {
        let catalog = PriceCatalog::new_empty();
        let mut snapshot = CatalogSnapshot::empty_cost_disabled();
        snapshot.status = CatalogStatus::Ok;
        snapshot.models.insert(
            "gpt-native".to_owned(),
            Pricing {
                native: Some(ProviderNativePricing {
                    provider: NativePricingProvider::OpenAi,
                    ordinary_input: NativePricingRate::known(UsdPerMillion::from_whole_usd(2)),
                    cached_input_read: NativePricingRate::known(UsdPerMillion::from_micros_usd(
                        100_000,
                    )),
                    cached_input_read_by_tier: BTreeMap::new(),
                    cached_input_read_above_tokens: BTreeMap::new(),
                    total_write: BTreeMap::from([(
                        NativePricingLifetime::ThirtyMinutes,
                        NativePricingRate::known(UsdPerMillion::from_whole_usd(3)),
                    )]),
                    explicit_resource: Default::default(),
                }),
                model: "gpt-native".to_owned(),
                input_per_million_usd: UsdPerMillion::from_whole_usd(2),
                output_per_million_usd: UsdPerMillion::from_whole_usd(8),
                input_image_per_million_usd: None,
                output_image_per_million_usd: None,
                by_tier: BTreeMap::new(),
            },
        );
        snapshot
            .cache_creation_per_million_usd
            .insert("gpt-native".to_owned(), UsdPerMillion::from_whole_usd(3));
        snapshot
            .cache_read_per_million_usd
            .insert("gpt-native".to_owned(), UsdPerMillion::from_whole_usd(1));
        catalog.install_snapshot(snapshot);

        let pricing = cache_pricing_summary_for_model(&catalog, "gpt-native", None);

        assert_eq!(pricing.status, "unknown");
        assert_eq!(pricing.input_micros_per_million, None);
        assert_eq!(pricing.cache_read_micros_per_million, None);
        assert_eq!(pricing.cache_creation_5m_micros_per_million, None);
        assert_eq!(pricing.cache_creation_1h_micros_per_million, None);
        let native = pricing.native.expect("native pricing context");
        assert_eq!(native.version, 1);
        assert_eq!(
            native.provider,
            cc_lb_domain::NativeCachePricingProvider::OpenAi
        );
        let ordinary = native
            .rates
            .iter()
            .find(|rate| rate.component == cc_lb_domain::NativeCachePricingComponent::OrdinaryInput)
            .expect("ordinary input rate");
        assert_eq!(
            ordinary.status,
            cc_lb_domain::NativeCachePricingStatus::Known
        );
        assert_eq!(ordinary.micros_per_million, Some(2_000_000));
        let total_write = native
            .rates
            .iter()
            .find(|rate| {
                rate.component == cc_lb_domain::NativeCachePricingComponent::TotalWrite
                    && rate.lifetime
                        == Some(cc_lb_domain::NativeCachePricingLifetime::ThirtyMinutes)
            })
            .expect("total write rate");
        assert_eq!(
            total_write.status,
            cc_lb_domain::NativeCachePricingStatus::Known
        );
        assert_eq!(total_write.micros_per_million, Some(3_000_000));
    }

    #[test]
    fn native_zero_rate_is_not_emitted_as_known_zero() {
        let catalog = PriceCatalog::new_empty();
        let mut snapshot = CatalogSnapshot::empty_cost_disabled();
        snapshot.status = CatalogStatus::Ok;
        snapshot.models.insert(
            "gpt-zero-native".to_owned(),
            Pricing {
                native: Some(ProviderNativePricing {
                    provider: NativePricingProvider::OpenAi,
                    ordinary_input: NativePricingRate::known(UsdPerMillion::from_micros_usd(0)),
                    cached_input_read: NativePricingRate::unknown(),
                    cached_input_read_by_tier: BTreeMap::new(),
                    cached_input_read_above_tokens: BTreeMap::new(),
                    total_write: BTreeMap::new(),
                    explicit_resource: Default::default(),
                }),
                model: "gpt-zero-native".to_owned(),
                input_per_million_usd: UsdPerMillion::from_whole_usd(2),
                output_per_million_usd: UsdPerMillion::from_whole_usd(8),
                input_image_per_million_usd: None,
                output_image_per_million_usd: None,
                by_tier: BTreeMap::new(),
            },
        );
        catalog.install_snapshot(snapshot);

        let pricing = cache_pricing_summary_for_model(&catalog, "gpt-zero-native", None);
        let ordinary = pricing
            .native
            .expect("native pricing context")
            .rates
            .into_iter()
            .find(|rate| rate.component == cc_lb_domain::NativeCachePricingComponent::OrdinaryInput)
            .expect("ordinary input rate");
        assert_eq!(
            ordinary.status,
            cc_lb_domain::NativeCachePricingStatus::Unknown
        );
        assert_eq!(ordinary.micros_per_million, None);
    }

    #[test]
    fn fold_decreasing_tokens_picks_later_structural() {
        let score = build_cache_score(
            &[
                breakpoint(0, 100, TtlClass::Ephemeral5m),
                breakpoint(1, 90, TtlClass::Ephemeral5m),
            ],
            &[],
        )
        .expect("cache score for cacheable request");

        assert_eq!(score.predicted_cache_read_tokens, 0);
        assert_eq!(score.predicted_cache_creation_tokens_5m, 90);
        assert_eq!(score.predicted_cache_creation_tokens_1h, 0);
    }

    #[test]
    fn fold_all_5m() {
        let score = build_cache_score(
            &[
                breakpoint(0, 100_000, TtlClass::Ephemeral5m),
                breakpoint(1, 250_000, TtlClass::Ephemeral5m),
                breakpoint(2, 500_000, TtlClass::Ephemeral5m),
            ],
            &[],
        )
        .expect("cache score for cacheable request");

        assert_eq!(score.predicted_cache_creation_tokens_5m, 500_000);
        assert_eq!(score.predicted_cache_creation_tokens_1h, 0);
    }

    #[test]
    fn fold_all_1h() {
        let score = build_cache_score(
            &[
                breakpoint(0, 100_000, TtlClass::Ephemeral1h),
                breakpoint(1, 250_000, TtlClass::Ephemeral1h),
                breakpoint(2, 500_000, TtlClass::Ephemeral1h),
            ],
            &[],
        )
        .expect("cache score for cacheable request");

        assert_eq!(score.predicted_cache_creation_tokens_1h, 500_000);
        assert_eq!(score.predicted_cache_creation_tokens_5m, 0);
    }

    #[test]
    fn fold_1h_then_5m_index_split() {
        let score = build_cache_score(
            &[
                breakpoint(0, 100_000, TtlClass::Ephemeral1h),
                breakpoint(1, 300_000, TtlClass::Ephemeral5m),
            ],
            &[],
        )
        .expect("cache score for cacheable request");

        assert_eq!(score.predicted_cache_creation_tokens_1h, 100_000);
        assert_eq!(score.predicted_cache_creation_tokens_5m, 200_000);
        assert_eq!(score.ambiguity_reason, None);
    }

    #[test]
    fn fold_non_monotonic_1h_then_5m_saturates_and_flags() {
        let score = build_cache_score(
            &[
                breakpoint(0, 100, TtlClass::Ephemeral1h),
                breakpoint(1, 90, TtlClass::Ephemeral5m),
            ],
            &[],
        )
        .expect("cache score for cacheable request");

        assert_eq!(score.predicted_cache_creation_tokens_1h, 100);
        assert_eq!(score.predicted_cache_creation_tokens_5m, 0);
        assert!(score.ambiguity_reason.is_some());
    }

    #[test]
    fn fold_same_index_ttl_upgrade() {
        let score = build_cache_score(
            &[breakpoint(2, 200_000, TtlClass::Ephemeral1h)],
            &[warm_entry("bp-2", TtlClass::Ephemeral5m, 200_000)],
        )
        .expect("cache score for cacheable request");

        assert_eq!(score.predicted_cache_read_tokens, 0);
        assert_eq!(score.matched_breakpoint_index, None);
        assert_eq!(score.predicted_cache_creation_tokens_1h, 200_000);
        assert_eq!(score.predicted_cache_creation_tokens_5m, 0);
    }

    #[test]
    fn five_minute_request_matches_one_hour_entry_at_same_key() {
        let score = build_cache_score(
            &[breakpoint(2, 200_000, TtlClass::Ephemeral5m)],
            &[warm_entry("bp-2", TtlClass::Ephemeral1h, 200_000)],
        )
        .expect("cache score for cacheable request");

        assert_eq!(score.predicted_cache_read_tokens, 200_000);
        assert_eq!(score.matched_breakpoint_index, Some(2));
        assert_eq!(score.predicted_cache_creation_tokens_5m, 0);
        assert_eq!(score.predicted_cache_creation_tokens_1h, 0);
    }

    #[test]
    fn overlapping_memberships_select_greatest_eligible_expiry() {
        let breakpoints = overlapping_mixed_ttl_breakpoints();
        let mut one_hour = warm_entry("shared", TtlClass::Ephemeral1h, 90_000);
        one_hour.expires_at_unix_secs = 1_700_000_200;
        let mut five_minute = warm_entry("shared", TtlClass::Ephemeral5m, 100_000);
        five_minute.expires_at_unix_secs = 1_700_000_300;

        let score = build_cache_score(&breakpoints, &[one_hour, five_minute])
            .expect("cache score for overlapping memberships");

        assert_eq!(score.predicted_cache_read_tokens, 100_000);
        assert_eq!(score.predicted_expires_at_unix_secs, Some(1_700_000_300));
        assert_eq!(score.matched_breakpoint_index, Some(15));
    }

    #[test]
    fn overlapping_five_minute_hit_prices_same_index_one_hour_upgrade() {
        let breakpoints = overlapping_mixed_ttl_breakpoints();
        let score = build_cache_score(
            &breakpoints,
            &[warm_entry("shared", TtlClass::Ephemeral5m, 100_000)],
        )
        .expect("cache score for overlapping memberships");

        assert_eq!(score.predicted_cache_read_tokens, 100_000);
        assert_eq!(score.predicted_cache_creation_tokens_1h, 100_000);
        assert_eq!(score.predicted_cache_creation_tokens_5m, 50_000);
        assert_eq!(score.matched_breakpoint_index, Some(15));
        assert_eq!(score.matched_content_block_index, Some(10));
    }

    #[test]
    fn invalid_ttl_order_marks_ineligible() {
        let score = build_cache_score(
            &[
                breakpoint(0, 100_000, TtlClass::Ephemeral5m),
                breakpoint(1, 200_000, TtlClass::Ephemeral1h),
            ],
            &[],
        );

        assert!(score.is_none());
    }

    fn reference_cache_score(
        breakpoints: &[CacheBreakpoint],
        warm: &[WarmCacheEntry],
    ) -> Option<(u64, u64, u64, Option<u32>)> {
        if breakpoints.is_empty() {
            return None;
        }
        let latest_1h = breakpoints
            .iter()
            .filter(|bp| bp.requested_ttl == TtlClass::Ephemeral1h)
            .map(|bp| bp.block_index)
            .max();
        let earliest_5m = breakpoints
            .iter()
            .filter(|bp| bp.requested_ttl == TtlClass::Ephemeral5m)
            .map(|bp| bp.block_index)
            .min();
        if let (Some(last_1h), Some(first_5m)) = (latest_1h, earliest_5m)
            && last_1h >= first_5m
        {
            return None;
        }
        let mut best: Option<(u32, u64, TtlClass, u64)> = None;
        for breakpoint in breakpoints {
            for prefix in &breakpoint.lookback_prefixes {
                let accepts_five_minute = breakpoints.iter().any(|candidate| {
                    candidate.requested_ttl == TtlClass::Ephemeral5m
                        && candidate.lookback_prefixes.iter().any(|candidate_prefix| {
                            candidate_prefix.prefix_hash == prefix.prefix_hash
                                && candidate_prefix.content_block_index
                                    == prefix.content_block_index
                        })
                });
                let Some(entry) = warm
                    .iter()
                    .filter(|entry| entry.prefix_hash == prefix.prefix_hash)
                    .filter(|entry| {
                        entry.ttl_class == TtlClass::Ephemeral1h
                            || accepts_five_minute && entry.ttl_class == TtlClass::Ephemeral5m
                    })
                    .max_by_key(|entry| entry.expires_at_unix_secs)
                else {
                    continue;
                };
                if best.is_none_or(|(index, _, _, expiry)| {
                    prefix.content_block_index > index
                        || prefix.content_block_index == index
                            && entry.expires_at_unix_secs > expiry
                }) {
                    best = Some((
                        prefix.content_block_index,
                        entry.estimated_prefix_bytes.get(),
                        entry.ttl_class,
                        entry.expires_at_unix_secs,
                    ));
                }
            }
        }
        let matched_index = best.map(|(index, _, _, _)| index);
        let matched_tokens = best.map_or(0, |(_, tokens, _, _)| tokens);
        let matched_ttl = best.map(|(_, _, ttl, _)| ttl);
        let deepest = |ttl: TtlClass| -> Option<u64> {
            breakpoints
                .iter()
                .filter(|bp| bp.requested_ttl == ttl)
                .filter(|bp| match (matched_index, matched_ttl) {
                    (None, None) => true,
                    (Some(index), Some(_)) if bp.block_index < index => false,
                    (Some(index), _) if bp.block_index > index => true,
                    (Some(_), Some(entry_ttl)) => match bp.requested_ttl {
                        TtlClass::Ephemeral5m => false,
                        TtlClass::Ephemeral1h => entry_ttl != TtlClass::Ephemeral1h,
                    },
                    _ => true,
                })
                .max_by_key(|bp| bp.block_index)
                .map(|bp| bp.prefix_bytes.get())
        };
        let deepest_1h = deepest(TtlClass::Ephemeral1h);
        let deepest_5m = deepest(TtlClass::Ephemeral5m);
        let one_hour_baseline = if matched_ttl == Some(TtlClass::Ephemeral1h) {
            matched_tokens
        } else {
            0
        };
        let creation_1h = deepest_1h.map_or(0, |tokens| tokens.saturating_sub(one_hour_baseline));
        let boundary = deepest_1h.unwrap_or(matched_tokens);
        let creation_5m = deepest_5m.map_or(0, |tokens| tokens.saturating_sub(boundary));
        Some((matched_tokens, creation_5m, creation_1h, matched_index))
    }

    fn lookback_from(index: u32) -> Vec<CacheLookbackPrefix> {
        let start = index.saturating_sub(2);
        (start..=index)
            .rev()
            .map(|position| CacheLookbackPrefix {
                prefix_hash: format!("h{position}"),
                content_block_index: position,
                lookback_distance: index - position,
            })
            .collect()
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(512))]

        #[test]
        fn build_cache_score_matches_reference_oracle(
            tokens in proptest::collection::vec(0u64..=500_000, 1..=5),
            ttl_1h in proptest::collection::vec(any::<bool>(), 5),
            warm_flags in proptest::collection::vec(any::<bool>(), 5),
            warm_ttl_1h in proptest::collection::vec(any::<bool>(), 5),
        ) {
            let count = tokens.len();
            let breakpoints: Vec<CacheBreakpoint> = (0..count)
                .map(|position| {
                    let index = u32::try_from(position).expect("small index");
                    CacheBreakpoint {
                        block_index: index,
                        source: CacheBreakpointSource::Message,
                        path: format!("messages.{index}.content"),
                        message_index: Some(index),
                        prefix_hash: format!("h{index}"),
                        prefix_bytes: PrefixBytes::new(tokens[position]),
                        requested_ttl: if ttl_1h[position] {
                            TtlClass::Ephemeral1h
                        } else {
                            TtlClass::Ephemeral5m
                        },
                        lookback_prefixes: lookback_from(index),
                        token_estimate_source: Some("test".to_owned()),
                    }
                })
                .collect();
            let warm: Vec<WarmCacheEntry> = (0..count)
                .filter(|&position| warm_flags[position])
                .map(|position| {
                    let index = u32::try_from(position).expect("small index");
                    WarmCacheEntry {
                        prefix_hash: format!("h{index}"),
                        expires_at_unix_secs: 1_700_000_300 + u64::from(index),
                        ttl_class: if warm_ttl_1h[position] {
                            TtlClass::Ephemeral1h
                        } else {
                            TtlClass::Ephemeral5m
                        },
                        last_observed_at_unix_secs: 1_700_000_000,
                        content_block_index: index,
                        estimated_prefix_bytes: PrefixBytes::new(tokens[position]),
                        hash_schema_version: HASH_SCHEMA_VERSION,
                    }
                })
                .collect();

            let got = build_cache_score(&breakpoints, &warm);
            let want = reference_cache_score(&breakpoints, &warm);
            match (got, want) {
                (None, None) => {}
                (Some(score), Some((read, creation_5m, creation_1h, matched_index))) => {
                    prop_assert_eq!(u64::from(score.predicted_cache_read_tokens), read);
                    prop_assert_eq!(
                        u64::from(score.predicted_cache_creation_tokens_5m),
                        creation_5m
                    );
                    prop_assert_eq!(
                        u64::from(score.predicted_cache_creation_tokens_1h),
                        creation_1h
                    );
                    prop_assert_eq!(score.matched_content_block_index, matched_index);
                }
                (got, want) => prop_assert!(
                    false,
                    "eligibility mismatch: got_some={} want_some={}",
                    got.is_some(),
                    want.is_some()
                ),
            }
        }
    }

    fn breakpoint(block_index: u32, prefix_bytes: u64, requested_ttl: TtlClass) -> CacheBreakpoint {
        CacheBreakpoint {
            block_index,
            source: CacheBreakpointSource::Message,
            path: format!("messages.{block_index}.content"),
            message_index: Some(block_index),
            prefix_hash: format!("bp-{block_index}"),
            prefix_bytes: PrefixBytes::new(prefix_bytes),
            requested_ttl,
            lookback_prefixes: vec![cc_lb_domain::CacheLookbackPrefix {
                prefix_hash: format!("bp-{block_index}"),
                content_block_index: block_index,
                lookback_distance: 0,
            }],
            token_estimate_source: Some("test".to_owned()),
        }
    }

    fn overlapping_mixed_ttl_breakpoints() -> [CacheBreakpoint; 2] {
        let mut one_hour = breakpoint(10, 100_000, TtlClass::Ephemeral1h);
        one_hour.prefix_hash = "shared".to_owned();
        one_hour.lookback_prefixes = vec![CacheLookbackPrefix {
            prefix_hash: "shared".to_owned(),
            content_block_index: 10,
            lookback_distance: 0,
        }];
        let mut five_minute = breakpoint(15, 150_000, TtlClass::Ephemeral5m);
        five_minute.lookback_prefixes.push(CacheLookbackPrefix {
            prefix_hash: "shared".to_owned(),
            content_block_index: 10,
            lookback_distance: 5,
        });
        [one_hour, five_minute]
    }

    fn warm_entry(
        prefix_hash: &str,
        ttl_class: TtlClass,
        estimated_prefix_bytes: u64,
    ) -> WarmCacheEntry {
        WarmCacheEntry {
            prefix_hash: prefix_hash.to_owned(),
            expires_at_unix_secs: 1_700_000_300,
            ttl_class,
            last_observed_at_unix_secs: 1_700_000_000,
            content_block_index: 0,
            estimated_prefix_bytes: PrefixBytes::new(estimated_prefix_bytes),
            hash_schema_version: HASH_SCHEMA_VERSION,
        }
    }
}

#[cfg(test)]
mod provider_native_cache_tests {
    use super::{
        RequestKind, extract_request_affinity_keys, native_cache_score_from_record,
        provider_native_lookup_params,
    };
    use cc_lb_request_log::{RequestCacheEvidence, RequestNativeCacheObservation};
    use cc_lb_storage_api::ProviderPromptCacheObservationRecord;
    use serde_json::json;
    use uuid::Uuid;

    fn native(
        provider: &str,
        mode: &str,
        scope: Option<&str>,
        identity_hash: Option<&str>,
        retention_secs: Option<u64>,
    ) -> RequestNativeCacheObservation {
        RequestNativeCacheObservation {
            provider: provider.to_owned(),
            mode: mode.to_owned(),
            scope: scope.map(str::to_owned),
            identity_hash: identity_hash.map(str::to_owned),
            retention_secs,
            evidence: RequestCacheEvidence::Unreported,
            ..Default::default()
        }
    }

    #[test]
    fn native_lookup_requires_complete_scope_hashed_key_and_matching_request() {
        let valid = native(
            "openai",
            "implicit",
            Some("org=acme;region=us"),
            Some("sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            Some(86_400),
        );
        assert!(
            provider_native_lookup_params(Some(&valid), "gpt-5", RequestKind::OpenaiResponses,)
                .is_some()
        );

        for invalid in [
            native(
                "openai",
                "implicit",
                Some("org=acme;region="),
                Some("sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
                Some(86_400),
            ),
            native(
                "openai",
                "implicit",
                Some("org=acme"),
                Some("sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
                Some(86_400),
            ),
            native(
                "openai",
                "implicit",
                Some("org=acme;region=us"),
                None,
                Some(86_400),
            ),
            native(
                "openai",
                "implicit",
                Some("org=acme;region=us"),
                Some("v3:local-anthropic-prefix"),
                Some(86_400),
            ),
            native(
                "gemini",
                "implicit",
                Some("org=acme;region=us"),
                Some("sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
                Some(86_400),
            ),
        ] {
            assert!(
                provider_native_lookup_params(
                    Some(&invalid),
                    "gpt-5",
                    RequestKind::OpenaiResponses,
                )
                .is_none()
            );
        }
    }

    #[test]
    fn native_lookup_includes_api_key_only_openai_candidates() {
        let request = native(
            "openai",
            "implicit",
            Some("org=acme;region=us"),
            Some("sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            Some(86_400),
        );
        assert_eq!(
            provider_native_lookup_params(
                Some(&request),
                "gpt-5",
                RequestKind::OpenaiResponsesInputTokens,
            )
            .map(|(_, _, _, _, retention_secs)| retention_secs),
            Some(86_400)
        );
    }

    #[test]
    fn native_lookup_rejects_missing_retention() {
        let request = native(
            "openai",
            "implicit",
            Some("org=acme;region=us"),
            Some("sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            None,
        );
        assert!(
            provider_native_lookup_params(Some(&request), "gpt-4o", RequestKind::OpenaiResponses,)
                .is_none()
        );
    }

    #[test]
    fn native_lookup_preserves_zero_in_memory_retention() {
        let request = native(
            "openai",
            "implicit",
            Some("org=acme;region=us"),
            Some("sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            Some(0),
        );
        assert_eq!(
            provider_native_lookup_params(Some(&request), "gpt-4o", RequestKind::OpenaiResponses,)
                .map(|(_, _, _, _, retention_secs)| retention_secs),
            Some(0)
        );
    }

    #[test]
    fn missing_key_has_no_native_score_even_for_modern_openai() {
        let request = native(
            "openai",
            "implicit",
            Some("org=acme;region=us"),
            None,
            Some(1_800),
        );
        assert!(
            provider_native_lookup_params(Some(&request), "gpt-5.6", RequestKind::OpenaiResponses,)
                .is_none()
        );
    }

    #[test]
    fn present_key_has_no_native_score_for_modern_openai() {
        let request = native(
            "openai",
            "implicit",
            Some("org=acme;region=us"),
            Some("sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            Some(1_800),
        );
        assert!(
            provider_native_lookup_params(Some(&request), "gpt-5.6", RequestKind::OpenaiResponses,)
                .is_none()
        );
    }

    #[test]
    fn gemini_native_evidence_never_becomes_soft_score_but_cached_content_stays_affine() {
        for mode in ["implicit", "explicit"] {
            let request = native(
                "gemini",
                mode,
                Some("project"),
                Some("sha256:cached-content"),
                Some(1_800),
            );
            assert!(
                provider_native_lookup_params(
                    Some(&request),
                    "gemini-2.5-pro",
                    RequestKind::GeminiGenerateContent,
                )
                .is_none()
            );
        }

        let keys = extract_request_affinity_keys(
            RequestKind::GeminiGenerateContent.capability(),
            &json!({"cachedContent": "projects/acme/locations/us/cachedContents/1"}),
            "principal",
        )
        .expect("cachedContent affinity extraction");
        assert_eq!(keys.len(), 1);
    }

    #[test]
    fn native_score_exposes_observed_tokens_without_anthropic_cache_key() {
        let record = ProviderPromptCacheObservationRecord {
            upstream_id: Uuid::from_u128(1),
            provider: "openai".to_owned(),
            canonical_model_id: "gpt-5".to_owned(),
            mode: "implicit".to_owned(),
            isolation_scope: "org=acme;region=us".to_owned(),
            identity_hash: "sha256:key".to_owned(),
            boundary: "provider".to_owned(),
            evidence_source: "provider_usage_metadata".to_owned(),
            observed_cached_tokens: Some(u64::from(u32::MAX) + 1),
            observed_write_tokens: None,
            retention_secs: Some(86_400),
            expires_at_unix_secs: Some(1_800_000_000),
            last_observed_at_unix_secs: 1_799_999_000,
        };
        let score = native_cache_score_from_record(&record);
        assert_eq!(score.predicted_cache_read_tokens, u32::MAX);
        assert_eq!(score.predicted_expires_at_unix_secs, Some(1_800_000_000));
        assert!(score.matched_v3_cache_key.is_none());
    }
}

#[cfg(test)]
mod prompt_cache_policy_tests {
    use super::*;
    use crate::prompt_cache_simulator::{block_digest_call_count, reset_block_digest_call_count};
    use cc_lb_control::traits::NoopPromptCacheObservationSink;
    #[test]
    fn official_url_policy_accepts_only_default_anthropic_authority() {
        for (url, expected) in [
            ("https://api.anthropic.com", true),
            ("https://@api.anthropic.com", true),
            ("https://api.anthropic.com/v1/messages?x=1#fragment", true),
            ("https://api.anthropic.com:443/proxy", true),
            ("http://api.anthropic.com", false),
            ("https://api.anthropic.com:8443", false),
            ("https://user:pass@api.anthropic.com", false),
            ("https://proxy.example.com", false),
        ] {
            let url = url.parse().expect("test URL parses");
            assert_eq!(is_official_anthropic_base_url(&url), expected, "{url}");
        }
    }

    #[test]
    fn empty_userinfo_is_canonicalized_to_the_official_url() {
        let empty_userinfo: Url = "https://@api.anthropic.com"
            .parse()
            .expect("empty-userinfo URL parses");
        let official: Url = "https://api.anthropic.com"
            .parse()
            .expect("official URL parses");
        assert_eq!(empty_userinfo, official);
        assert_eq!(empty_userinfo.as_str(), "https://api.anthropic.com/");
        assert!(empty_userinfo.username().is_empty());
        assert!(empty_userinfo.password().is_none());
        assert!(is_official_anthropic_base_url(&empty_userinfo));
    }

    #[test]
    fn policy_bundles_digest_each_raw_block_once_per_request() {
        for include_billing_header in [false, true] {
            let mut system = Vec::new();
            if include_billing_header {
                system.push(json!({"type":"text","text":"x-anthropic-billing-header: cron-a"}));
            }
            system.push(json!({
                "type":"text",
                "text":"stable",
                "cache_control":{"type":"ephemeral"}
            }));
            let request = serde_json::to_vec(&json!({
                "model":"claude-sonnet-4-5",
                "system":system,
            }))
            .expect("request serializes");
            reset_block_digest_call_count();
            let _ = request_cache_metadata(&HeaderMap::new(), &Bytes::from(request));
            assert_eq!(
                block_digest_call_count(),
                if include_billing_header { 2 } else { 1 },
                "billing_header={include_billing_header}"
            );
        }
    }

    #[test]
    fn policy_bundles_tokenize_each_raw_prefix_boundary_once()
    -> Result<(), Box<dyn std::error::Error>> {
        for include_billing_header in [false, true] {
            let mut system = Vec::new();
            if include_billing_header {
                system.push(json!({"type":"text","text":"x-anthropic-billing-header: cron-a"}));
            }
            system.push(json!({
                "type": "text",
                "text": "a".repeat(2_000),
                "cache_control": {"type": "ephemeral"}
            }));
            let request = json!({"model":"claude-sonnet-4-5","system":system});
            let canonical_model = canonical_model_id(Provider::Anthropic, "claude-sonnet-4-5");
            cc_lb_routing_eval::tokenizer::reset_tokenizer_call_count();
            let analysis = analyze_v3_prompt_cache(&request, canonical_model);
            let eligible = cacheable_breakpoint_prefix_keys(
                &analysis,
                canonical_model,
                cache_threshold_tokens(canonical_model),
            );
            assert_eq!(cc_lb_routing_eval::tokenizer::tokenizer_call_count(), 1);
            let output = PromptCacheAnalysisOutput {
                analysis,
                cacheable_breakpoint_prefix_keys: eligible,
                literal_cacheable_breakpoint_prefix_keys: Vec::new(),
                timings: PromptCacheAnalysisTimings::default(),
            };
            let metadata = request_cache_metadata_from_value_with_analysis_for_capability(
                &HeaderMap::new(),
                Some(&request),
                Some(output),
                None,
                None,
            );
            assert_eq!(metadata.bundles.official.cache_breakpoints.len(), 1);
            assert_eq!(metadata.bundles.literal.cache_breakpoints.len(), 1);
            assert_eq!(
                cc_lb_routing_eval::tokenizer::tokenizer_call_count(),
                1,
                "shared raw boundary was tokenized again, billing_header={include_billing_header}"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn literal_only_billing_breakpoint_records_custom_cache_creation()
    -> Result<(), Box<dyn std::error::Error>> {
        let request = json!({
            "model": "claude-sonnet-4-5",
            "tools": [{
                "name": "large_tool",
                "description": "tool schema context ".repeat(2_000),
                "input_schema": {"type": "object"}
            }],
            "system": [{
                "type": "text",
                "text": "x-anthropic-billing-header: cron-attribution",
                "cache_control": {"type": "ephemeral"}
            }]
        });
        let canonical_model = canonical_model_id(Provider::Anthropic, "claude-sonnet-4-5");
        let output = PromptCacheAnalysisExecutor::default()
            .analyze(
                Arc::new(request.clone()),
                canonical_model.to_owned(),
                [0; 32],
                Some(cache_threshold_tokens(canonical_model)),
            )
            .await;
        assert!(output.analysis.breakpoints.is_empty());
        let metadata = request_cache_metadata_from_value_with_analysis_for_capability(
            &HeaderMap::new(),
            Some(&request),
            Some(output),
            None,
            None,
        );
        assert!(metadata.bundles.official.cache_breakpoints.is_empty());
        let literal = &metadata.bundles.literal;
        let breakpoint = literal
            .cache_breakpoints
            .first()
            .ok_or("literal breakpoint missing")?;
        assert!(
            literal
                .cacheable_breakpoint_prefix_keys
                .contains(&breakpoint.prefix_hash)
        );
        let context = PromptCacheObservationContext {
            upstream_id: Uuid::from_u128(1004),
            canonical_model_id: metadata.canonical_model_id.clone(),
            cache_breakpoints: literal.cache_breakpoints.clone(),
            cacheable_breakpoint_prefix_keys: literal.cacheable_breakpoint_prefix_keys.clone(),
            selected_match: None,
            native_cache: None,
            sink: Arc::new(NoopPromptCacheObservationSink),
            grace_margin_secs: 0,
        };
        let decoded = decode_prompt_cache_observations_pure(
            &context,
            PromptCacheUsage {
                cache_creation_input_tokens: 8_192,
                cache_read_input_tokens: 0,
                read_only_evidence: false,
                ..PromptCacheUsage::default()
            },
            1_800_000_000,
        );
        assert_eq!(decoded.observations.len(), 1);
        assert_eq!(
            decoded
                .observations
                .first()
                .map(|observation| observation.prefix_hash.as_str()),
            Some(breakpoint.prefix_hash.as_str())
        );
        assert_eq!(decoded.dropped_below_threshold, 0);
        Ok(())
    }
}

#[cfg(test)]
mod prompt_cache_token_scope_tests {
    use super::*;

    #[test]
    fn x_api_key_scope_ignores_lower_precedence_authorization() {
        let mut first = HeaderMap::new();
        first.insert("x-api-key", HeaderValue::from_static("stable-key"));
        first.insert("authorization", HeaderValue::from_static("Bearer first"));
        let mut second = HeaderMap::new();
        second.insert("x-api-key", HeaderValue::from_static("stable-key"));
        second.insert("authorization", HeaderValue::from_static("Bearer second"));
        assert_eq!(
            prompt_cache_token_scope(&first),
            prompt_cache_token_scope(&second)
        );
    }

    #[test]
    fn authorization_scope_changes_without_x_api_key() {
        let mut first = HeaderMap::new();
        first.insert("authorization", HeaderValue::from_static("Bearer first"));
        let mut second = HeaderMap::new();
        second.insert("authorization", HeaderValue::from_static("Bearer second"));
        assert_ne!(
            prompt_cache_token_scope(&first),
            prompt_cache_token_scope(&second)
        );
    }
}
