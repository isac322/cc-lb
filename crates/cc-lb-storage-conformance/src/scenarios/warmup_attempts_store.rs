use std::sync::Arc;

use anyhow::{Result, ensure};
use cc_lb_storage_api::{
    UpstreamStore, UpstreamWarmupAttemptStore, WarmupAttemptListFilters, WarmupAttemptOutcome,
    WarmupAttemptStatus, WarmupDispatchKind, WarmupLane, WarmupSkipReason, WarmupSuccessReason,
    WarmupTransientFailureReason,
};

use crate::{
    harness::{ConformanceBackend, with_conformance_fixture},
    scenarios::warmup_attempts_fixture::{
        attempt, create_upstream, cursor, filters, ids, insert_all, lane_attempt, outcomes,
    },
};

pub async fn run_all<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: UpstreamStore + UpstreamWarmupAttemptStore,
{
    insert_and_read_back(Arc::clone(&backend)).await?;
    cursor_pagination(Arc::clone(&backend)).await?;
    outcome_filter(Arc::clone(&backend)).await?;
    fk_cascade_on_upstream_delete(Arc::clone(&backend)).await?;
    lane_round_trip_and_filter(Arc::clone(&backend)).await?;
    row_without_lane_reads_as_primary(backend).await?;
    Ok(())
}

macro_rules! scenario {
    ($name:ident, $body:expr) => {
        pub async fn $name<B>(backend: Arc<B>) -> Result<()>
        where
            B: ConformanceBackend,
            B::Storage: UpstreamStore + UpstreamWarmupAttemptStore,
        {
            with_conformance_fixture(backend, $body).await
        }
    };
}

scenario!(insert_and_read_back, |storage| async move {
    let upstream = create_upstream(storage.as_ref(), "warmup-insert-read").await?;
    let record = attempt(
        upstream.id,
        1,
        1_800_000_000,
        WarmupAttemptOutcome::Skipped(WarmupSkipReason::SevenDayQuotaExhausted),
    );
    storage.insert_warmup_attempt(&record).await?;

    ensure!(
        storage
            .list_warmup_attempts_for_upstream(upstream.id, WarmupAttemptListFilters::default())
            .await?
            == [record],
        "list should return inserted row"
    );
    Ok(())
});

scenario!(cursor_pagination, |storage| async move {
    let upstream = create_upstream(storage.as_ref(), "warmup-cursor-pagination").await?;
    let records = (0..10)
        .map(|idx| {
            attempt(
                upstream.id,
                idx + 1,
                1_800_000_000 + idx as i64,
                WarmupAttemptOutcome::Success(WarmupSuccessReason::CycleAdvanced),
            )
        })
        .collect::<Vec<_>>();
    insert_all(storage.as_ref(), &records).await?;

    let mut before = None;
    let mut pages = Vec::new();
    loop {
        let page = storage
            .list_warmup_attempts_for_upstream(upstream.id, filters(Some(3), before, None, None))
            .await?;
        if page.is_empty() {
            break;
        }
        before = page.last().map(cursor);
        pages.push(ids(&page));
    }

    ensure!(
        pages
            == [
                vec![records[9].id, records[8].id, records[7].id],
                vec![records[6].id, records[5].id, records[4].id],
                vec![records[3].id, records[2].id, records[1].id],
                vec![records[0].id],
            ],
        "cursor pages should follow descending order"
    );
    Ok(())
});

scenario!(outcome_filter, |storage| async move {
    let upstream = create_upstream(storage.as_ref(), "warmup-outcome-filter").await?;
    let records = outcomes(
        upstream.id,
        &[
            WarmupAttemptOutcome::Success(WarmupSuccessReason::CycleAdvanced),
            WarmupAttemptOutcome::TransientFailure(WarmupTransientFailureReason::NetworkError),
            WarmupAttemptOutcome::Success(WarmupSuccessReason::WindowAlreadyActive),
            WarmupAttemptOutcome::Skipped(WarmupSkipReason::UpstreamDisabled),
            WarmupAttemptOutcome::Success(WarmupSuccessReason::CycleAdvanced),
        ],
        1_800_000_010,
    );
    insert_all(storage.as_ref(), &records).await?;

    let listed = storage
        .list_warmup_attempts_for_upstream(
            upstream.id,
            filters(Some(10), None, Some(WarmupAttemptStatus::Success), None),
        )
        .await?;

    ensure!(
        listed
            .iter()
            .all(|record| record.outcome.status() == WarmupAttemptStatus::Success),
        "status filter should return only success rows"
    );
    ensure!(
        ids(&listed) == [records[4].id, records[2].id, records[0].id],
        "filtered rows should be ordered newest first"
    );
    Ok(())
});

scenario!(fk_cascade_on_upstream_delete, |storage| async move {
    let upstream = create_upstream(storage.as_ref(), "warmup-fk-cascade").await?;
    let record = attempt(
        upstream.id,
        1,
        1_800_000_000,
        WarmupAttemptOutcome::Success(WarmupSuccessReason::CycleAdvanced),
    );
    storage.insert_warmup_attempt(&record).await?;
    storage.hard_delete(upstream.id).await?;

    ensure!(
        storage
            .list_warmup_attempts_for_upstream(upstream.id, WarmupAttemptListFilters::default())
            .await?
            .is_empty(),
        "deleting upstream_spec_v1 parent should cascade warmup attempts"
    );
    Ok(())
});

scenario!(lane_round_trip_and_filter, |storage| async move {
    let upstream = create_upstream(storage.as_ref(), "warmup-lane-filter").await?;
    let records = vec![
        lane_attempt(
            upstream.id,
            1,
            1_800_000_000,
            WarmupLane::Primary,
            WarmupAttemptOutcome::Success(WarmupSuccessReason::CycleAdvanced),
        ),
        lane_attempt(
            upstream.id,
            2,
            1_800_000_010,
            WarmupLane::ThirdParty,
            WarmupAttemptOutcome::Skipped(WarmupSkipReason::QuotaGroupUnavailable),
        ),
        lane_attempt(
            upstream.id,
            3,
            1_800_000_020,
            WarmupLane::Primary,
            WarmupAttemptOutcome::Skipped(WarmupSkipReason::SevenDayQuotaExhausted),
        ),
        lane_attempt(
            upstream.id,
            4,
            1_800_000_030,
            WarmupLane::ThirdParty,
            WarmupAttemptOutcome::Success(WarmupSuccessReason::CycleAdvanced),
        ),
        lane_attempt(
            upstream.id,
            5,
            1_800_000_040,
            WarmupLane::ThirdParty,
            WarmupAttemptOutcome::Skipped(WarmupSkipReason::QuotaGroupUnavailable),
        ),
    ];
    insert_all(storage.as_ref(), &records).await?;

    let all = storage
        .list_warmup_attempts_for_upstream(upstream.id, WarmupAttemptListFilters::default())
        .await?;
    ensure!(
        all == records.iter().rev().cloned().collect::<Vec<_>>(),
        "lane and quota_group_unavailable should round-trip unchanged, newest first"
    );

    let third_party = storage
        .list_warmup_attempts_for_upstream(
            upstream.id,
            filters(Some(10), None, None, Some(WarmupLane::ThirdParty)),
        )
        .await?;
    ensure!(
        ids(&third_party) == [records[4].id, records[3].id, records[1].id],
        "third_party lane filter should return only third_party rows newest first"
    );

    let primary = storage
        .list_warmup_attempts_for_upstream(
            upstream.id,
            filters(Some(10), None, None, Some(WarmupLane::Primary)),
        )
        .await?;
    ensure!(
        ids(&primary) == [records[2].id, records[0].id],
        "primary lane filter should exclude third_party rows"
    );

    let third_party_skips = storage
        .list_warmup_attempts_for_upstream(
            upstream.id,
            filters(
                Some(10),
                None,
                Some(WarmupAttemptStatus::Skipped),
                Some(WarmupLane::ThirdParty),
            ),
        )
        .await?;
    ensure!(
        ids(&third_party_skips) == [records[4].id, records[1].id],
        "lane and status filters should combine"
    );

    let third_party_page = storage
        .list_warmup_attempts_for_upstream(
            upstream.id,
            filters(
                Some(10),
                Some(cursor(&records[4])),
                None,
                Some(WarmupLane::ThirdParty),
            ),
        )
        .await?;
    ensure!(
        ids(&third_party_page) == [records[3].id, records[1].id],
        "lane filter should combine with the cursor"
    );
    Ok(())
});

/// Rows written before the lane column existed carry no lane; the column
/// default must surface them as primary.
pub async fn row_without_lane_reads_as_primary<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: UpstreamStore + UpstreamWarmupAttemptStore,
{
    let raw_backend = Arc::clone(&backend);
    with_conformance_fixture(backend, |storage| async move {
        let upstream = create_upstream(storage.as_ref(), "warmup-lane-legacy-row").await?;
        let inserted = raw_backend
            .execute_raw_sql(
                &storage,
                "INSERT INTO warmup_attempts_v1 \
                 (id, upstream_id, attempted_at_unix_secs, completed_at_unix_secs, \
                  scheduled_for_unix_secs, trigger, outcome, reason, dispatch_kind, \
                  upstream_spec_revision) \
                 SELECT '00000000-0000-0000-0000-000000004101', id, 1800000000, 1800000005, \
                  1799999940, 'scheduled', 'success', 'cycle_advanced', 'not_dispatched', 1 \
                 FROM upstream_spec_v1 WHERE name = 'warmup-lane-legacy-row'",
            )
            .await?;
        ensure!(
            inserted == 1,
            "legacy warmup attempt row should be inserted"
        );

        let expected = attempt(
            upstream.id,
            0x101,
            1_800_000_000,
            WarmupAttemptOutcome::Success(WarmupSuccessReason::CycleAdvanced),
        );
        ensure!(
            expected.lane == WarmupLane::Primary
                && expected.dispatch_kind == Some(WarmupDispatchKind::NotDispatched),
            "fixture should describe the legacy row"
        );
        ensure!(
            storage
                .list_warmup_attempts_for_upstream(
                    upstream.id,
                    filters(Some(10), None, None, Some(WarmupLane::Primary)),
                )
                .await?
                == [expected],
            "a row inserted without a lane should read back as primary"
        );
        ensure!(
            storage
                .list_warmup_attempts_for_upstream(
                    upstream.id,
                    filters(Some(10), None, None, Some(WarmupLane::ThirdParty)),
                )
                .await?
                .is_empty(),
            "a row inserted without a lane should not appear in the third_party lane"
        );
        Ok(())
    })
    .await
}
