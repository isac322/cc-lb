use std::sync::Arc;

use anyhow::{Result, ensure};
use cc_lb_storage_api::{
    RequestEvent, RequestEventPrincipalCostQuery, RequestEventStore,
    normalize_usage_rollup_dimension,
};
use uuid::Uuid;

use crate::harness::{ConformanceBackend, ConformanceFixture};

pub async fn principal_cost_components_aggregate_without_fabrication<B>(
    backend: Arc<B>,
) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: cc_lb_storage_api::Storage,
{
    let mut fixture = ConformanceFixture::new(backend).await?;
    let result: Result<()> = async {
        let storage = fixture.storage();
        let range_start = 1_900_500_000_u64;
        let upstream_id = Uuid::from_u128(0x51);
        let other_upstream_id = Uuid::from_u128(0x52);
        let long_principal = format!("{} ignored suffix", "x".repeat(64));

        storage
            .append_request_event(&cost_event(
                range_start + 5,
                "principal-cost-modern",
                Some("\u{00a0}\tteam/A\u{3000}"),
                upstream_id,
                Some(15),
                Some([1, 2, 3, 4, 5]),
                Some("renewal"),
            ))
            .await?;
        storage
            .append_request_event(&cost_event(
                range_start + 10,
                "principal-cost-legacy",
                Some("team A"),
                upstream_id,
                Some(10),
                None,
                None,
            ))
            .await?;
        storage
            .append_request_event(&cost_event(
                range_start + 15,
                "principal-cost-other-upstream",
                Some("team A"),
                other_upstream_id,
                Some(20),
                Some([2, 3, 4, 5, 6]),
                None,
            ))
            .await?;
        storage
            .append_request_event(&cost_event(
                range_start + 65,
                "principal-cost-zero",
                None,
                upstream_id,
                Some(0),
                Some([0, 0, 0, 0, 0]),
                None,
            ))
            .await?;
        storage
            .append_request_event(&cost_event(
                range_start + 70,
                "principal-cost-unrecorded",
                Some("legacy-only"),
                upstream_id,
                Some(7),
                None,
                None,
            ))
            .await?;
        storage
            .append_request_event(&cost_event(
                range_start + 75,
                "principal-cost-truncated",
                Some(&long_principal),
                upstream_id,
                Some(3),
                Some([1, 1, 1, 0, 0]),
                None,
            ))
            .await?;
        storage
            .append_request_event(&cost_event(
                range_start + 120,
                "principal-cost-exclusive-end",
                Some("team A"),
                upstream_id,
                Some(999),
                Some([999, 0, 0, 0, 0]),
                None,
            ))
            .await?;
        let normalized_principal = normalize_usage_rollup_dimension(Some("team A"));
        let normalized_long_principal =
            normalize_usage_rollup_dimension(Some(long_principal.as_str()));
        let selected_principals = vec![
            normalized_principal.clone(),
            normalized_long_principal.clone(),
            "unknown".to_owned(),
        ];

        let buckets = storage
            .request_event_principal_costs(&RequestEventPrincipalCostQuery {
                since_unix_secs: range_start,
                until_unix_secs: range_start + 120,
                bucket_width_secs: 60,
                upstream_id: None,
                principal_keys: selected_principals.clone(),
            })
            .await?;

        let mixed = buckets
            .iter()
            .find(|bucket| {
                bucket.principal == normalized_principal
                    && bucket.bucket_start_unix_secs == range_start
            })
            .expect("normalized mixed principal bucket");
        ensure!(
            mixed.total_cost_micros == 45,
            "mixed total must not double count"
        );
        ensure!(
            mixed.component_costs_recorded,
            "modern rows record components"
        );
        ensure!(mixed.cost_input_micros == 3, "input cost mismatch");
        ensure!(mixed.cost_output_micros == 5, "output cost mismatch");
        ensure!(
            mixed.cost_cache_creation_5m_micros == 7,
            "cache creation 5m cost mismatch"
        );
        ensure!(
            mixed.cost_cache_creation_1h_micros == 9,
            "cache creation 1h cost mismatch"
        );
        ensure!(
            mixed.cost_cache_read_micros == 11,
            "cache read cost mismatch"
        );

        let zero = buckets
            .iter()
            .find(|bucket| {
                bucket.principal == "unknown" && bucket.bucket_start_unix_secs == range_start + 60
            })
            .expect("zero-valued recorded component bucket");
        ensure!(zero.total_cost_micros == 0, "zero total mismatch");
        ensure!(
            zero.component_costs_recorded,
            "recorded zero components must retain coverage"
        );
        ensure!(
            zero.cost_input_micros == 0
                && zero.cost_output_micros == 0
                && zero.cost_cache_creation_5m_micros == 0
                && zero.cost_cache_creation_1h_micros == 0
                && zero.cost_cache_read_micros == 0,
            "zero component values must remain zero"
        );
        let truncated = buckets
            .iter()
            .find(|bucket| {
                bucket.principal == normalized_long_principal
                    && bucket.bucket_start_unix_secs == range_start + 60
            })
            .expect("truncated principal bucket");
        ensure!(
            truncated.principal.chars().count() == 64,
            "principal key must be truncated to 64 characters"
        );
        ensure!(truncated.total_cost_micros == 3, "truncated total mismatch");
        ensure!(
            truncated.cost_input_micros == 1
                && truncated.cost_output_micros == 1
                && truncated.cost_cache_creation_5m_micros == 1,
            "truncated principal components mismatch"
        );

        ensure!(
            buckets
                .iter()
                .all(|bucket| selected_principals.contains(&bucket.principal)),
            "unselected normalized principals must be excluded"
        );

        let filtered_buckets = storage
            .request_event_principal_costs(&RequestEventPrincipalCostQuery {
                since_unix_secs: range_start,
                until_unix_secs: range_start + 120,
                bucket_width_secs: 60,
                upstream_id: Some(upstream_id),
                principal_keys: vec![normalized_principal.clone()],
            })
            .await?;
        let filtered_mixed = filtered_buckets
            .iter()
            .find(|bucket| {
                bucket.principal == normalized_principal
                    && bucket.bucket_start_unix_secs == range_start
            })
            .expect("upstream-filtered normalized principal bucket");
        ensure!(
            filtered_mixed.total_cost_micros == 25,
            "upstream filter must exclude other-upstream costs"
        );
        ensure!(
            filtered_mixed.cost_input_micros == 1,
            "filtered input mismatch"
        );
        ensure!(
            filtered_mixed.cost_output_micros == 2,
            "filtered output mismatch"
        );
        ensure!(
            filtered_mixed.cost_cache_creation_5m_micros == 3,
            "filtered cache creation 5m mismatch"
        );
        ensure!(
            filtered_mixed.cost_cache_creation_1h_micros == 4,
            "filtered cache creation 1h mismatch"
        );
        ensure!(
            filtered_mixed.cost_cache_read_micros == 5,
            "filtered cache read mismatch"
        );
        ensure!(
            filtered_mixed.total_cost_micros < mixed.total_cost_micros,
            "filtered and unfiltered totals must differ"
        );

        let empty_selection = storage
            .request_event_principal_costs(&RequestEventPrincipalCostQuery {
                since_unix_secs: range_start,
                until_unix_secs: range_start + 120,
                bucket_width_secs: 60,
                upstream_id: None,
                principal_keys: Vec::new(),
            })
            .await?;
        ensure!(
            empty_selection.is_empty(),
            "empty principal selection must return no rows"
        );

        ensure!(
            buckets.iter().all(|bucket| bucket.total_cost_micros < 999),
            "[since, until) must exclude the end-boundary event"
        );
        Ok(())
    }
    .await;
    let teardown = fixture.teardown().await;
    result?;
    teardown
}

fn cost_event(
    ts: u64,
    request_id: &str,
    principal_id: Option<&str>,
    upstream_id: Uuid,
    total_cost_micros: Option<i64>,
    components: Option<[i64; 5]>,
    source_kind: Option<&str>,
) -> RequestEvent {
    let [
        input,
        output,
        cache_creation_5m,
        cache_creation_1h,
        cache_read,
    ] = components.unwrap_or([0; 5]);
    RequestEvent {
        ts,
        ts_ms: Some(ts * 1_000),
        request_id: request_id.to_owned(),
        event_id: Some(format!("{request_id}-event")),
        source_kind: source_kind.map(ToOwned::to_owned),
        principal_id: principal_id.map(ToOwned::to_owned),
        upstream_id: Some(upstream_id),
        status: 200,
        cost_usd_micros: total_cost_micros,
        cost_input_micros: components.map(|_| input),
        cost_output_micros: components.map(|_| output),
        cost_cache_creation_5m_micros: components.map(|_| cache_creation_5m),
        cost_cache_creation_1h_micros: components.map(|_| cache_creation_1h),
        cost_cache_read_micros: components.map(|_| cache_read),
        ..RequestEvent::default()
    }
}
