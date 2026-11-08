use std::sync::Arc;

use anyhow::{Result, ensure};
use cc_lb_storage_api::{
    RequestEvent, RequestEventStore, UsageRollup, UsageRollupResolution, UsageRollupStore,
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
    let raw_backend = Arc::clone(&backend);
    let mut fixture = ConformanceFixture::new(backend).await?;
    let result: Result<()> = async {
        let storage = fixture.storage();
        let range_start = 1_900_500_000_u64;
        let upstream_id = Uuid::from_u128(0x51);
        let other_upstream_id = Uuid::from_u128(0x52);
        let team_principal = Uuid::from_u128(0x50).to_string();
        let uuid_principal = Uuid::from_u128(0x53).to_string();
        let unrecorded_principal = Uuid::from_u128(0x59).to_string();

        for event in [
            cost_event(
                range_start + 5,
                "principal-cost-modern",
                Some(&team_principal),
                upstream_id,
                Some(15),
                Some([1, 2, 3, 4, 5]),
            ),
            cost_event(
                range_start + 10,
                "principal-cost-unrecorded",
                Some(&team_principal),
                upstream_id,
                Some(10),
                None,
            ),
            cost_event(
                range_start + 12,
                "principal-cost-unrecorded-only",
                Some(&unrecorded_principal),
                upstream_id,
                Some(9),
                None,
            ),
            cost_event(
                range_start + 15,
                "principal-cost-other-upstream",
                Some(&team_principal),
                other_upstream_id,
                Some(20),
                Some([2, 3, 4, 5, 6]),
            ),
            cost_event(
                range_start + 20,
                "principal-cost-uuid",
                Some(&uuid_principal),
                upstream_id,
                Some(13),
                Some([1, 2, 3, 3, 4]),
            ),
            cost_event(
                range_start + 65,
                "principal-cost-zero",
                None,
                upstream_id,
                Some(0),
                Some([0, 0, 0, 0, 0]),
            ),
            cost_event(
                range_start + 70,
                "principal-cost-null-other-upstream",
                None,
                other_upstream_id,
                Some(17),
                Some([1, 2, 3, 4, 5]),
            ),
            cost_event(
                range_start + 26,
                "principal-cost-non-uuid",
                Some("legacy/principal"),
                upstream_id,
                Some(107),
                Some([107, -5, 0, 0, 0]),
            ),
            cost_event(
                range_start + 120,
                "principal-cost-exclusive-end",
                Some(&team_principal),
                upstream_id,
                Some(999),
                Some([999, 0, 0, 0, 0]),
            ),
        ] {
            storage.append_request_event(&event).await?;
        }

        raw_backend
            .wait_for_events_visible_for_rollup(storage.as_ref())
            .await?;
        storage.rollup_usage_once().await?;
        let rollups = storage
            .query_usage_rollups_in_range(
                UsageRollupResolution::Minute,
                range_start,
                range_start + 120,
            )
            .await?;

        let team = find_rollup(&rollups, &team_principal, upstream_id, range_start)?;
        ensure!(team.virtual_cost_micros == 25, "team total mismatch");
        ensure!(team.cost_components_event_count == 1, "team coverage mismatch");
        ensure!(
            [
                team.cost_input_micros,
                team.cost_output_micros,
                team.cost_cache_creation_5m_micros,
                team.cost_cache_creation_1h_micros,
                team.cost_cache_read_micros,
            ] == [1, 2, 3, 4, 5],
            "team components mismatch: {team:?}"
        );

        let other_team = find_rollup(&rollups, &team_principal, other_upstream_id, range_start)?;
        ensure!(other_team.virtual_cost_micros == 20, "other-upstream total mismatch");
        ensure!(other_team.cost_components_event_count == 1);
        ensure!(
            [
                other_team.cost_input_micros,
                other_team.cost_output_micros,
                other_team.cost_cache_creation_5m_micros,
                other_team.cost_cache_creation_1h_micros,
                other_team.cost_cache_read_micros,
            ] == [2, 3, 4, 5, 6],
            "other-upstream components mismatch: {other_team:?}"
        );

        let uuid = find_rollup(&rollups, &uuid_principal, upstream_id, range_start)?;
        ensure!(uuid.virtual_cost_micros == 13, "UUID total mismatch");
        ensure!(uuid.cost_components_event_count == 1);
        ensure!(
            [
                uuid.cost_input_micros,
                uuid.cost_output_micros,
                uuid.cost_cache_creation_5m_micros,
                uuid.cost_cache_creation_1h_micros,
                uuid.cost_cache_read_micros,
            ] == [1, 2, 3, 3, 4],
            "UUID components mismatch: {uuid:?}"
        );

        let unknown_zero = find_rollup(
            &rollups,
            "unknown",
            upstream_id,
            range_start + 60,
        )?;
        ensure!(unknown_zero.virtual_cost_micros == 0);
        ensure!(unknown_zero.cost_components_event_count > 0);
        ensure!(
            unknown_zero.cost_input_micros == 0
                && unknown_zero.cost_output_micros == 0
                && unknown_zero.cost_cache_creation_5m_micros == 0
                && unknown_zero.cost_cache_creation_1h_micros == 0
                && unknown_zero.cost_cache_read_micros == 0,
            "recorded zero components must remain zero"
        );
        let unknown_other = find_rollup(
            &rollups,
            "unknown",
            other_upstream_id,
            range_start + 60,
        )?;
        ensure!(
            unknown_other.virtual_cost_micros == 17
                && unknown_other.cost_components_event_count == 1
                && unknown_other.cost_input_micros == 1
                && unknown_other.cost_output_micros == 2
                && unknown_other.cost_cache_creation_5m_micros == 3
                && unknown_other.cost_cache_creation_1h_micros == 4
                && unknown_other.cost_cache_read_micros == 5,
            "unknown principal other-upstream components mismatch: {unknown_other:?}"
        );

        let normalized = normalize_usage_rollup_dimension(Some("legacy/principal"));
        ensure!(normalized == "legacy_principal");
        let non_uuid = find_rollup(&rollups, &normalized, upstream_id, range_start)?;
        ensure!(non_uuid.virtual_cost_micros == 107);
        let unrecorded_only =
            find_rollup(&rollups, &unrecorded_principal, upstream_id, range_start)?;
        ensure!(
            unrecorded_only.cost_components_event_count == 0
                && unrecorded_only.cost_input_micros == 0
                && unrecorded_only.cost_output_micros == 0
                && unrecorded_only.cost_cache_creation_5m_micros == 0
                && unrecorded_only.cost_cache_creation_1h_micros == 0
                && unrecorded_only.cost_cache_read_micros == 0,
            "unrecorded components must retain zero coverage"
        );
        ensure!(non_uuid.cost_components_event_count > 0);

        let baseline_rollups = rollups.clone();
        raw_backend
            .execute_raw_sql(
                storage.as_ref(),
                "UPDATE usage_rollups_v2 SET cost_input_micros = 0, cost_output_micros = 0, cost_cache_creation_5m_micros = 0, cost_cache_creation_1h_micros = 0, cost_cache_read_micros = 0, cost_components_event_count = 0",
            )
            .await?;
        raw_backend
            .execute_raw_sql(
                storage.as_ref(),
                "DELETE FROM usage_rollup_checkpoints_v1 WHERE id IN ('cost_components_backfill_upper', 'cost_components_backfill_cursor')",
            )
            .await?;
        // Append one new live event after the frozen high_water to prove live and backfill ranges are disjoint.
        storage
            .append_request_event(&cost_event(
                range_start + 30,
                "principal-cost-post-upgrade-live",
                Some(&uuid_principal),
                upstream_id,
                Some(10),
                Some([2, 2, 2, 2, 2]),
            ))
            .await?;
        raw_backend
            .wait_for_events_visible_for_rollup(storage.as_ref())
            .await?;
        storage.rollup_usage_once().await?;
        // Second run must be a no-op once backfill cursor reaches upper.
        storage.rollup_usage_once().await?;
        let restored = storage
            .query_usage_rollups_in_range(
                UsageRollupResolution::Minute,
                range_start,
                range_start + 120,
            )
            .await?;
        for base in &baseline_rollups {
            let actual =
                find_rollup(&restored, &base.principal, base.upstream_id, base.bucket_start)?;
            if base.principal == uuid_principal
                && base.upstream_id == upstream_id
                && base.bucket_start == range_start
            {
                ensure!(actual.request_count == base.request_count + 1);
                ensure!(actual.virtual_cost_micros == base.virtual_cost_micros + 10);
                ensure!(
                    actual.cost_components_event_count == base.cost_components_event_count + 1
                );
                ensure!(
                    [
                        actual.cost_input_micros,
                        actual.cost_output_micros,
                        actual.cost_cache_creation_5m_micros,
                        actual.cost_cache_creation_1h_micros,
                        actual.cost_cache_read_micros,
                    ] == [
                        base.cost_input_micros + 2,
                        base.cost_output_micros + 2,
                        base.cost_cache_creation_5m_micros + 2,
                        base.cost_cache_creation_1h_micros + 2,
                        base.cost_cache_read_micros + 2,
                    ],
                    "live + backfill combined rollup mismatch: {actual:?}"
                );
            } else {
                ensure!(
                    actual == base,
                    "backfilled rollup mismatch: actual={actual:?} expected={base:?}"
                );
            }
        }
        Ok(())
    }
    .await;
    let teardown = fixture.teardown().await;
    result?;
    teardown
}

fn find_rollup<'a>(
    rollups: &'a [UsageRollup],
    principal: &str,
    upstream_id: Uuid,
    bucket_start: u64,
) -> Result<&'a UsageRollup> {
    rollups
        .iter()
        .find(|rollup| {
            rollup.resolution == UsageRollupResolution::Minute
                && rollup.principal == principal
                && rollup.upstream_id == upstream_id
                && rollup.bucket_start == bucket_start
        })
        .ok_or_else(|| {
            anyhow::anyhow!(
                "missing rollup for {principal}/{upstream_id}/{bucket_start}: {rollups:?}"
            )
        })
}

fn cost_event(
    ts: u64,
    request_id: &str,
    principal_id: Option<&str>,
    upstream_id: Uuid,
    total_cost_micros: Option<i64>,
    components: Option<[i64; 5]>,
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
