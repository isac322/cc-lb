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
        let team_principal = Uuid::from_u128(0x56).to_string();
        let uuid_principal = Uuid::from_u128(0x53).to_string();
        let unselected_principal = Uuid::from_u128(0x57).to_string();
        let partial_principal =
            normalize_usage_rollup_dimension(Some(Uuid::from_u128(0x58).to_string().as_str()));

        storage
            .append_request_event(&cost_event(
                range_start + 5,
                "principal-cost-modern",
                Some(&team_principal),
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
                Some(&team_principal),
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
                Some(&team_principal),
                other_upstream_id,
                Some(20),
                Some([2, 3, 4, 5, 6]),
                None,
            ))
            .await?;
        storage
            .append_request_event(&cost_event(
                range_start + 20,
                "principal-cost-uuid",
                Some(&uuid_principal),
                upstream_id,
                Some(13),
                Some([1, 2, 3, 3, 4]),
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
                Some(&unselected_principal),
                upstream_id,
                Some(7),
                None,
                None,
            ))
            .await?;
        let mut partial_event = cost_event(
            range_start + 80,
            "principal-cost-negative-partial",
            Some(&partial_principal),
            upstream_id,
            Some(-11),
            None,
            None,
        );
        partial_event.cost_input_micros = Some(-1);
        partial_event.cost_output_micros = None;
        partial_event.cost_cache_creation_5m_micros = Some(4);
        partial_event.cost_cache_creation_1h_micros = None;
        partial_event.cost_cache_read_micros = Some(0);
        storage.append_request_event(&partial_event).await?;
        storage
            .append_request_event(&cost_event(
                range_start + 120,
                "principal-cost-exclusive-end",
                Some(&team_principal),
                upstream_id,
                Some(999),
                Some([999, 0, 0, 0, 0]),
                None,
            ))
            .await?;
        let normalized_principal = normalize_usage_rollup_dimension(Some(&team_principal));
        ensure!(
            normalized_principal == team_principal,
            "canonical UUID principal keys must normalize to themselves"
        );
        let selected_principals = vec![
            normalized_principal.clone(),
            "unknown".to_owned(),
            partial_principal.clone(),
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
        let partial = buckets
            .iter()
            .find(|bucket| {
                bucket.principal == partial_principal
                    && bucket.bucket_start_unix_secs == range_start + 60
            })
            .expect("negative and partially recorded component bucket");
        ensure!(
            partial.total_cost_micros == 0,
            "negative total cost must clamp to zero"
        );
        ensure!(
            partial.component_costs_recorded,
            "partially recorded components must retain coverage"
        );
        ensure!(
            partial.cost_input_micros == 0
                && partial.cost_output_micros == 0
                && partial.cost_cache_creation_5m_micros == 4
                && partial.cost_cache_creation_1h_micros == 0
                && partial.cost_cache_read_micros == 0,
            "negative and NULL components must clamp or coalesce without fabrication"
        );

        ensure!(
            buckets
                .iter()
                .all(|bucket| selected_principals.contains(&bucket.principal)),
            "unselected normalized principals must be excluded"
        );

        let uuid_buckets = storage
            .request_event_principal_costs(&RequestEventPrincipalCostQuery {
                since_unix_secs: range_start,
                until_unix_secs: range_start + 120,
                bucket_width_secs: 60,
                upstream_id: Some(upstream_id),
                principal_keys: vec![uuid_principal.clone()],
            })
            .await?;
        ensure!(
            uuid_buckets.len() == 1,
            "UUID principal selection must return exactly one bucket"
        );
        let uuid_bucket = &uuid_buckets[0];
        ensure!(
            uuid_bucket.principal == uuid_principal,
            "UUID principal key must remain exact"
        );
        ensure!(
            uuid_bucket.total_cost_micros == 13
                && uuid_bucket.cost_input_micros == 1
                && uuid_bucket.cost_output_micros == 2
                && uuid_bucket.cost_cache_creation_5m_micros == 3
                && uuid_bucket.cost_cache_creation_1h_micros == 3
                && uuid_bucket.cost_cache_read_micros == 4,
            "UUID principal component costs mismatch"
        );
        let mixed_shape_buckets = storage
            .request_event_principal_costs(&RequestEventPrincipalCostQuery {
                since_unix_secs: range_start,
                until_unix_secs: range_start + 120,
                bucket_width_secs: 60,
                upstream_id: Some(upstream_id),
                principal_keys: vec![uuid_principal.clone(), "unknown".to_owned()],
            })
            .await?;
        ensure!(
            mixed_shape_buckets.len() == 2,
            "mixed UUID and NULL-principal selection must return both static-query branches"
        );
        ensure!(
            mixed_shape_buckets.iter().any(|bucket| {
                bucket.principal == uuid_principal && bucket.total_cost_micros == 13
            }),
            "mixed selection must retain UUID costs"
        );
        ensure!(
            mixed_shape_buckets.iter().any(|bucket| {
                bucket.principal == "unknown"
                    && bucket.bucket_start_unix_secs == range_start + 60
                    && bucket.total_cost_micros == 0
                    && bucket.component_costs_recorded
            }),
            "mixed selection must retain NULL-principal costs"
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

        storage
            .append_request_event(&cost_event(
                range_start + 40,
                "principal-cost-transition-same-upstream",
                Some(&team_principal),
                upstream_id,
                Some(29),
                Some([5, 6, 7, 5, 6]),
                Some("transition"),
            ))
            .await?;
        let refreshed_filtered_buckets = storage
            .request_event_principal_costs(&RequestEventPrincipalCostQuery {
                since_unix_secs: range_start,
                until_unix_secs: range_start + 120,
                bucket_width_secs: 60,
                upstream_id: Some(upstream_id),
                principal_keys: vec![normalized_principal.clone()],
            })
            .await?;
        let refreshed_filtered_mixed = refreshed_filtered_buckets
            .iter()
            .find(|bucket| {
                bucket.principal == normalized_principal
                    && bucket.bucket_start_unix_secs == range_start
            })
            .expect("refreshed upstream-filtered normalized principal bucket");
        ensure!(
            refreshed_filtered_mixed.total_cost_micros == filtered_mixed.total_cost_micros + 29,
            "same-upstream append must increase filtered total by the appended delta"
        );
        ensure!(
            refreshed_filtered_mixed.cost_input_micros == filtered_mixed.cost_input_micros + 5
                && refreshed_filtered_mixed.cost_output_micros
                    == filtered_mixed.cost_output_micros + 6
                && refreshed_filtered_mixed.cost_cache_creation_5m_micros
                    == filtered_mixed.cost_cache_creation_5m_micros + 7
                && refreshed_filtered_mixed.cost_cache_creation_1h_micros
                    == filtered_mixed.cost_cache_creation_1h_micros + 5
                && refreshed_filtered_mixed.cost_cache_read_micros
                    == filtered_mixed.cost_cache_read_micros + 6,
            "same-upstream append must increase filtered components by the appended deltas"
        );

        storage
            .append_request_event(&cost_event(
                range_start + 45,
                "principal-cost-transition-other-upstream",
                Some(&team_principal),
                other_upstream_id,
                Some(31),
                Some([6, 7, 8, 4, 6]),
                Some("transition"),
            ))
            .await?;
        let refreshed_unfiltered_buckets = storage
            .request_event_principal_costs(&RequestEventPrincipalCostQuery {
                since_unix_secs: range_start,
                until_unix_secs: range_start + 120,
                bucket_width_secs: 60,
                upstream_id: None,
                principal_keys: vec![normalized_principal.clone()],
            })
            .await?;
        let refreshed_unfiltered_mixed = refreshed_unfiltered_buckets
            .iter()
            .find(|bucket| {
                bucket.principal == normalized_principal
                    && bucket.bucket_start_unix_secs == range_start
            })
            .expect("refreshed unfiltered normalized principal bucket");
        ensure!(
            refreshed_unfiltered_mixed.total_cost_micros == mixed.total_cost_micros + 29 + 31,
            "unfiltered requery must observe both appended total deltas"
        );
        ensure!(
            refreshed_unfiltered_mixed.cost_input_micros == mixed.cost_input_micros + 5 + 6
                && refreshed_unfiltered_mixed.cost_output_micros
                    == mixed.cost_output_micros + 6 + 7
                && refreshed_unfiltered_mixed.cost_cache_creation_5m_micros
                    == mixed.cost_cache_creation_5m_micros + 7 + 8
                && refreshed_unfiltered_mixed.cost_cache_creation_1h_micros
                    == mixed.cost_cache_creation_1h_micros + 5 + 4
                && refreshed_unfiltered_mixed.cost_cache_read_micros
                    == mixed.cost_cache_read_micros + 6 + 6,
            "unfiltered requery must observe both appended component deltas"
        );

        let isolated_filtered_buckets = storage
            .request_event_principal_costs(&RequestEventPrincipalCostQuery {
                since_unix_secs: range_start,
                until_unix_secs: range_start + 120,
                bucket_width_secs: 60,
                upstream_id: Some(upstream_id),
                principal_keys: vec![normalized_principal.clone()],
            })
            .await?;
        let isolated_filtered_mixed = isolated_filtered_buckets
            .iter()
            .find(|bucket| {
                bucket.principal == normalized_principal
                    && bucket.bucket_start_unix_secs == range_start
            })
            .expect("isolated upstream-filtered normalized principal bucket");
        ensure!(
            isolated_filtered_mixed.total_cost_micros == refreshed_filtered_mixed.total_cost_micros
                && isolated_filtered_mixed.cost_input_micros
                    == refreshed_filtered_mixed.cost_input_micros
                && isolated_filtered_mixed.cost_output_micros
                    == refreshed_filtered_mixed.cost_output_micros
                && isolated_filtered_mixed.cost_cache_creation_5m_micros
                    == refreshed_filtered_mixed.cost_cache_creation_5m_micros
                && isolated_filtered_mixed.cost_cache_creation_1h_micros
                    == refreshed_filtered_mixed.cost_cache_creation_1h_micros
                && isolated_filtered_mixed.cost_cache_read_micros
                    == refreshed_filtered_mixed.cost_cache_read_micros,
            "other-upstream append must not leak into the upstream-filtered requery"
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
