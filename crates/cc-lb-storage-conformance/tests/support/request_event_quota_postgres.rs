macro_rules! define_request_event_quota_postgres_tests {
    () => {
        use cc_lb_storage_api::RequestEventStore as _;

        type PostgresQuotaRow = (Option<f64>, Option<f64>, Option<f64>, Option<f64>);

        const POSTGRES_QUOTA_SELECT: &str = "SELECT quota_urgency_5h, quota_urgency_7d, \
            quota_urgency_combined, quota_warning_multiplier \
            FROM request_events_v1 WHERE event_id = $1";

        #[test]
        fn request_event_quota_columns_are_persisted_postgres() {
            run_postgres_scenario(
                "request_event_quota_columns_are_persisted",
                |backend| async move {
                    let fixture = backend.create_fixture().await?;
                    let storage = backend.open(&fixture).await?;
                    let event = request_event_quota_support::populated_event()?;

                    // Given a v11 trace whose selected and losing candidates differ.
                    request_event_quota_support::assert_populated_event(&event);

                    // When the selected winner values are persisted through RequestEventStore.
                    storage.append_request_event(&event).await?;

                    // Then payload reads and dedicated columns both retain the selected values.
                    let read_back = storage.query_request_events(0, u64::MAX, 10).await?;
                    assert_eq!(read_back.len(), 1);
                    request_event_quota_support::assert_populated_event(&read_back[0]);
                    let recent = storage.query_recent_request_events(0, u64::MAX, 10).await?;
                    assert_eq!(recent.len(), 1);
                    request_event_quota_support::assert_populated_event(&recent[0]);

                    let row = sqlx::query_as::<_, PostgresQuotaRow>(POSTGRES_QUOTA_SELECT)
                        .bind(request_event_quota_support::SELECTED_EVENT_ID)
                        .fetch_one(&fixture.pool)
                        .await?;
                    let quota = request_event_quota_support::SELECTED_QUOTA;
                    assert_eq!(
                        row,
                        (
                            Some(quota.urgency_5h),
                            Some(quota.urgency_7d),
                            Some(quota.urgency_combined),
                            Some(quota.warning_multiplier),
                        )
                    );

                    drop(storage);
                    backend.teardown(fixture).await
                },
            );
        }

        #[test]
        fn request_event_cursor_defers_committed_rows_behind_old_snapshot_postgres() {
            run_postgres_scenario(
                "request_event_cursor_defers_committed_rows_behind_old_snapshot",
                |backend| async move {
                    let fixture = backend.create_fixture().await?;
                    let storage = backend.open(&fixture).await?;
                    let event = request_event_quota_support::populated_event()?;

                    // Given an older repeatable-read transaction has established its snapshot.
                    let mut blocker = fixture.pool.begin().await?;
                    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                        .execute(&mut *blocker)
                        .await?;
                    sqlx::query("SELECT pg_current_xact_id()")
                        .execute(&mut *blocker)
                        .await?;

                    // When a newer request event is committed and its cursor is observed.
                    storage.append_request_event(&event).await?;
                    let cursor = storage.current_request_event_cursor().await?;
                    let cursor_rows = storage
                        .query_request_events_between_cursors(
                            0,
                            cursor,
                            10,
                            &cc_lb_storage_api::RequestEventStreamFilters::default(),
                        )
                        .await?;

                    // Then cursor reads defer the row while the older snapshot is active.
                    assert!(cursor_rows.is_empty());

                    blocker.commit().await?;
                    drop(storage);
                    backend.teardown(fixture).await
                },
            );
        }

        #[test]
        fn request_event_quota_columns_null_for_mismatch_and_historical_rows_postgres() {
            run_postgres_scenario(
                "request_event_quota_columns_null_for_mismatch_and_historical_rows",
                |backend| async move {
                    let fixture = backend.create_fixture().await?;
                    let storage = backend.open(&fixture).await?;
                    let mismatch = request_event_quota_support::mismatch_event()?;

                    // Given a trace whose terminal upstream is absent from its candidates.
                    request_event_quota_support::assert_mismatch_event(&mismatch);
                    storage.append_request_event(&mismatch).await?;
                    let historical = request_event_quota_support::historical_event();
                    let historical_payload = serde_json::to_vec(&historical)?;
                    sqlx::query(
                        "INSERT INTO request_events_v1 \
                         (ts, payload, event_id) VALUES ($1, $2, $3)",
                    )
                    .bind(chrono::DateTime::from_timestamp(
                        i64::try_from(historical.ts)?,
                        0,
                    ))
                    .bind(historical_payload)
                    .bind(request_event_quota_support::HISTORICAL_EVENT_ID)
                    .execute(&fixture.pool)
                    .await?;

                    // When both rows are read directly after the migration.
                    let mismatch_row = sqlx::query_as::<_, PostgresQuotaRow>(POSTGRES_QUOTA_SELECT)
                        .bind(request_event_quota_support::MISMATCH_EVENT_ID)
                        .fetch_one(&fixture.pool)
                        .await?;
                    let historical_row =
                        sqlx::query_as::<_, PostgresQuotaRow>(POSTGRES_QUOTA_SELECT)
                            .bind(request_event_quota_support::HISTORICAL_EVENT_ID)
                            .fetch_one(&fixture.pool)
                            .await?;

                    // Then all dedicated quota columns stay NULL.
                    let null_row: PostgresQuotaRow = (None, None, None, None);
                    assert_eq!(mismatch_row, null_row);
                    assert_eq!(historical_row, null_row);

                    drop(storage);
                    backend.teardown(fixture).await
                },
            );
        }
    };
}

pub(crate) use define_request_event_quota_postgres_tests;
