use cc_lb_plugin_api::types::{CachePricingSummary, CacheScore};

pub(crate) fn select_cache_score_by_value(
    exact_score: Option<CacheScore>,
    thread_score: Option<CacheScore>,
    pricing: &CachePricingSummary,
) -> Option<CacheScore> {
    let exact_value = exact_score
        .as_ref()
        .and_then(|score| cache_score_value_micros(score, pricing));
    let thread_value = thread_score
        .as_ref()
        .and_then(|score| cache_score_value_micros(score, pricing));

    match (exact_score, thread_score) {
        (Some(exact_score), Some(thread_score)) => {
            if thread_value_is_better(exact_value, thread_value) {
                Some(thread_score)
            } else {
                Some(exact_score)
            }
        }
        (Some(exact_score), None) => Some(exact_score),
        (None, Some(thread_score)) => Some(thread_score),
        (None, None) => None,
    }
}

fn thread_value_is_better(exact_value: Option<i64>, thread_value: Option<i64>) -> bool {
    match (exact_value, thread_value) {
        (_, Some(thread_value)) if thread_value <= 0 => false,
        (Some(exact_value), Some(thread_value)) => thread_value > exact_value,
        (None, Some(thread_value)) => thread_value > 0,
        (_, None) => false,
    }
}

fn cache_score_value_micros(score: &CacheScore, pricing: &CachePricingSummary) -> Option<i64> {
    let input_price = pricing.input_micros_per_million?;
    let cache_creation_5m_price = pricing.cache_creation_5m_micros_per_million?;
    let cache_creation_1h_price = pricing.cache_creation_1h_micros_per_million?;
    let cache_read_price = pricing.cache_read_micros_per_million?;
    let cold_read_cost = micros_for_cache_tokens(score.predicted_cache_read_tokens, input_price);
    let cached_read_cost =
        micros_for_cache_tokens(score.predicted_cache_read_tokens, cache_read_price);
    let read_savings = cold_read_cost.saturating_sub(cached_read_cost);
    let create_cost = micros_for_cache_tokens(
        score.predicted_cache_creation_tokens_5m,
        cache_creation_5m_price,
    )
    .saturating_add(micros_for_cache_tokens(
        score.predicted_cache_creation_tokens_1h,
        cache_creation_1h_price,
    ));
    Some(saturating_i128_to_i64(
        i128::from(read_savings) - i128::from(create_cost),
    ))
}

fn micros_for_cache_tokens(tokens: u32, micros_per_million: u64) -> u64 {
    (u128::from(tokens) * u128::from(micros_per_million) / 1_000_000)
        .try_into()
        .unwrap_or(u64::MAX)
}

fn saturating_i128_to_i64(value: i128) -> i64 {
    i64::try_from(value).unwrap_or(if value.is_negative() {
        i64::MIN
    } else {
        i64::MAX
    })
}

#[cfg(test)]
mod tests {
    use super::select_cache_score_by_value;
    use cc_lb_plugin_api::types::{CachePricingSummary, CacheScore};

    #[test]
    fn selects_positive_thread_value_when_exact_score_is_creation_only() {
        let exact_score = cache_score(0, 200_000, None, 0.0, None);
        let thread_score = cache_score(
            120_000,
            0,
            Some(1_700_000_300),
            0.5,
            Some("thread_usage_lineage"),
        );

        let selected = select_cache_score_by_value(
            Some(exact_score),
            Some(thread_score),
            &opus_cache_pricing(),
        )
        .expect("positive thread score selected");

        assert_eq!(selected.predicted_cache_read_tokens, 120_000);
        assert_eq!(selected.predicted_cache_creation_tokens_5m, 0);
        assert_eq!(
            selected.ambiguity_reason.as_deref(),
            Some("thread_usage_lineage")
        );
    }

    #[test]
    fn keeps_exact_score_when_values_tie() {
        let exact_score = cache_score(120_000, 0, Some(1_700_000_600), 1.0, None);
        let thread_score = cache_score(
            120_000,
            0,
            Some(1_700_000_300),
            0.5,
            Some("thread_usage_lineage"),
        );

        let selected = select_cache_score_by_value(
            Some(exact_score),
            Some(thread_score),
            &opus_cache_pricing(),
        )
        .expect("exact score selected on tie");

        assert_eq!(selected.confidence, 1.0);
        assert_eq!(selected.ambiguity_reason, None);
    }

    fn cache_score(
        read_tokens: u32,
        creation_tokens_5m: u32,
        expires_at: Option<u64>,
        confidence: f32,
        ambiguity_reason: Option<&str>,
    ) -> CacheScore {
        CacheScore {
            predicted_cache_read_tokens: read_tokens,
            predicted_cache_creation_tokens_5m: creation_tokens_5m,
            predicted_cache_creation_tokens_1h: 0,
            predicted_uncached_input_tokens: 0,
            predicted_expires_at_unix_secs: expires_at,
            matched_breakpoint_index: None,
            confidence,
            ambiguity_reason: ambiguity_reason.map(ToOwned::to_owned),
        }
    }

    fn opus_cache_pricing() -> CachePricingSummary {
        CachePricingSummary {
            status: "known".to_owned(),
            input_micros_per_million: Some(15_000_000),
            cache_creation_5m_micros_per_million: Some(18_750_000),
            cache_creation_1h_micros_per_million: Some(30_000_000),
            cache_read_micros_per_million: Some(1_500_000),
        }
    }
}
