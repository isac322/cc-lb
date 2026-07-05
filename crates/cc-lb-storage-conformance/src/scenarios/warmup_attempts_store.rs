use std::sync::Arc;

use anyhow::{Result, ensure};
use cc_lb_engine::ClockHandle;
use cc_lb_storage_api::{
    UpstreamStore, UpstreamWarmupAttemptStore, WarmupAttemptListFilters, WarmupAttemptOutcome,
    WarmupAttemptStatus, WarmupAttemptSummary, WarmupPermanentFailureReason, WarmupSkipReason,
    WarmupSuccessReason, WarmupTransientFailureReason,
};

use crate::{
    harness::{ConformanceBackend, with_conformance_fixture},
    scenarios::warmup_attempts_fixture::{
        attempt, create_upstream, cursor, filters, ids, insert_all, outcomes,
    },
};

const SEVEN_DAYS_SECS: i64 = 7 * 86_400;

pub async fn run_all<B>(backend: Arc<B>, clock: ClockHandle) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: UpstreamStore + UpstreamWarmupAttemptStore,
{
    insert_and_read_back(Arc::clone(&backend)).await?;
    cursor_pagination(Arc::clone(&backend)).await?;
    outcome_filter(Arc::clone(&backend)).await?;
    summary_7d(Arc::clone(&backend), Arc::clone(&clock)).await?;
    fk_cascade_on_upstream_delete(Arc::clone(&backend)).await?;
    latest_for_upstream_when_empty(backend).await?;
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
            == [record.clone()],
        "list should return inserted row"
    );
    ensure!(
        storage
            .latest_warmup_attempt_for_upstream(upstream.id)
            .await?
            == Some(record),
        "latest should return inserted row"
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
            .list_warmup_attempts_for_upstream(upstream.id, filters(Some(3), before, None))
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
            filters(Some(10), None, Some(WarmupAttemptStatus::Success)),
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

pub async fn summary_7d<B>(backend: Arc<B>, clock: ClockHandle) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: UpstreamStore + UpstreamWarmupAttemptStore,
{
    with_conformance_fixture(backend, |storage| async move {
        let upstream = create_upstream(storage.as_ref(), "warmup-summary-7d").await?;
        let now = chrono::DateTime::<chrono::Utc>::from(clock.now()).timestamp();
        let mut records = outcomes(
            upstream.id,
            &[
                WarmupAttemptOutcome::Success(WarmupSuccessReason::CycleAdvanced),
                WarmupAttemptOutcome::Success(WarmupSuccessReason::CycleAdvanced),
                WarmupAttemptOutcome::Success(WarmupSuccessReason::WindowAlreadyActive),
                WarmupAttemptOutcome::TransientFailure(WarmupTransientFailureReason::NetworkError),
                WarmupAttemptOutcome::PermanentFailure(WarmupPermanentFailureReason::AuthFailed),
                WarmupAttemptOutcome::Skipped(WarmupSkipReason::UpstreamDisabled),
            ],
            now - 360,
        );
        records.extend([
            attempt(
                upstream.id,
                20,
                now - SEVEN_DAYS_SECS - 60,
                WarmupAttemptOutcome::Success(WarmupSuccessReason::CycleAdvanced),
            ),
            attempt(
                upstream.id,
                21,
                now - SEVEN_DAYS_SECS - 120,
                WarmupAttemptOutcome::Skipped(WarmupSkipReason::UpstreamDisabled),
            ),
        ]);
        insert_all(storage.as_ref(), &records).await?;

        let cutoff_unix_secs = now
            .checked_sub(SEVEN_DAYS_SECS)
            .expect("summary cutoff fits in i64");
        ensure!(
            storage
                .summarize_recent_warmup_attempts(upstream.id, cutoff_unix_secs)
                .await?
                == WarmupAttemptSummary {
                    success: 3,
                    skipped: 1,
                    transient_failure: 1,
                    permanent_failure: 1,
                },
            "summary should count only attempts inside the 7 day window"
        );
        Ok(())
    })
    .await
}

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

scenario!(latest_for_upstream_when_empty, |storage| async move {
    let upstream = create_upstream(storage.as_ref(), "warmup-empty-latest").await?;
    ensure!(
        storage
            .latest_warmup_attempt_for_upstream(upstream.id)
            .await?
            .is_none(),
        "latest should be None when no attempts exist"
    );
    Ok(())
});
