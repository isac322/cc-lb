use std::{future::Future, sync::Arc};

use anyhow::Result;
use cc_lb_storage_api::{
    RequestEvent, RequestEventHistogramQuery, RequestEventKind, RequestEventListItem,
    RequestEventListQuery, RequestEventStore, RequestEventStreamFilters, StatusClass,
};
use uuid::Uuid;

use crate::harness::{ConformanceBackend, ConformanceFixture};

pub async fn request_event_list_projects_rows_and_preserves_detail<B: ConformanceBackend>(
    backend: Arc<B>,
) -> Result<()> {
    with_fixture(Arc::clone(&backend), |storage| async move {
        let upstream_id = Uuid::from_u128(7);
        let other_upstream_id = Uuid::from_u128(8);

        let matching = request_event("event-b", "req-b", 2_000, 200, "principal-a", upstream_id);

        let legacy = RequestEvent {
            ts: 2,
            ts_ms: Some(2_000),
            request_id: "req-a".to_owned(),
            event_id: Some("event-a".to_owned()),
            principal_id: Some("principal-a".to_owned()),
            upstream_id: Some(upstream_id),
            upstream_name: Some("upstream-a".to_owned()),
            thread_id: Some("thread-a".to_owned()),
            model: Some("claude-sonnet-4-5".to_owned()),
            status: 204,
            ..Default::default()
        };

        let mut renewal = request_event("event-c", "req-c", 3_000, 202, "principal-a", upstream_id);
        renewal.source_kind = Some("renewal".to_owned());

        let mut wrong_principal = decoy(&matching, "decoy-principal", 9_600);
        wrong_principal.principal_id = Some("principal-b".to_owned());
        let mut wrong_model = decoy(&matching, "decoy-model", 9_500);
        wrong_model.model = Some("claude-opus-4-1".to_owned());
        let mut wrong_upstream_id = decoy(&matching, "decoy-upstream-id", 9_400);
        wrong_upstream_id.upstream_id = Some(other_upstream_id);
        let mut wrong_thread = decoy(&matching, "decoy-thread", 9_300);
        wrong_thread.thread_id = Some("thread-b".to_owned());
        let mut wrong_status = decoy(&matching, "decoy-status", 9_100);
        wrong_status.status = 500;

        for event in [
            &legacy,
            &matching,
            &renewal,
            &wrong_principal,
            &wrong_model,
            &wrong_upstream_id,
            &wrong_thread,
            &wrong_status,
        ] {
            storage.append_request_event(event).await?;
        }

        let filters = RequestEventStreamFilters {
            principal_id: Some("principal-a".to_owned()),
            thread_id: Some("thread-a".to_owned()),
            model: Some("claude-sonnet-4-5".to_owned()),
            upstream_id: Some(upstream_id),
            provider_upstream_ids: None,
            event_kinds: None,
            status_class: Some(StatusClass::TwoXx),
            errors_only: false,
            source_kind: None,
            exclude_renewal: false,
        };

        let default_page = storage
            .list_request_events(&list_query(filters.clone(), None, 10, None, None))
            .await?;
        assert_event_ids(&default_page, &["event-b", "event-a"]);
        assert_list_matches_details(storage.as_ref(), &default_page).await?;
        assert_current_io_timings(&default_page[0]);
        assert_legacy_io_timings(&default_page[1]);
        assert_eq!(default_page[0].thinking_level.as_deref(), Some("LOW"));
        assert_eq!(default_page[1].thinking_level, None);

        let first_page = storage
            .list_request_events(&list_query(filters.clone(), Some("all"), 2, None, None))
            .await?;
        assert_event_ids(&first_page, &["event-c", "event-b"]);
        assert_list_matches_details(storage.as_ref(), &first_page).await?;

        let cursor = first_page.last().expect("first page cursor row");
        let second_page = storage
            .list_request_events(&list_query(
                filters.clone(),
                Some("all"),
                2,
                Some(
                    cursor
                        .ts_ms
                        .unwrap_or_else(|| cursor.ts.saturating_mul(1_000)),
                ),
                Some(
                    cursor
                        .event_id
                        .clone()
                        .unwrap_or_else(|| cursor.request_id.clone()),
                ),
            ))
            .await?;
        assert_event_ids(&second_page, &["event-a"]);
        assert_list_matches_details(storage.as_ref(), &second_page).await?;

        let renewal_page = storage
            .list_request_events(&list_query(
                filters.clone(),
                Some("renewal"),
                10,
                None,
                None,
            ))
            .await?;
        assert_event_ids(&renewal_page, &["event-c"]);
        assert_list_matches_details(storage.as_ref(), &renewal_page).await?;

        let proxy_page = storage
            .list_request_events(&list_query(filters, Some("proxy"), 10, None, None))
            .await?;
        assert_event_ids(&proxy_page, &["event-b"]);
        assert_list_matches_details(storage.as_ref(), &proxy_page).await?;

        let detail = storage
            .get_request_event(matching.event_id.as_deref().unwrap_or_default())
            .await?
            .expect("stored request event detail");
        assert_eq!(serde_json::to_vec(&detail)?, serde_json::to_vec(&matching)?);
        let until_seq = storage
            .request_event_stable_seq_ceiling(std::time::Duration::from_secs(5))
            .await?;
        let exported = storage
            .export_request_events(&cc_lb_storage_api::RequestEventExportPage {
                after_seq: 0,
                until_seq,
                from_ts_ms: None,
                to_ts_ms: None,
                limit: 100,
            })
            .await?;
        let exported_matching = exported
            .iter()
            .find(|(_, event)| event.event_id == matching.event_id)
            .expect("matching request is exported");
        assert_eq!(exported_matching.1.thinking_level, matching.thinking_level);

        for index in 0..60 {
            let mut event = request_event(
                &format!("sparse-{index}"),
                &format!("req-sparse-{index}"),
                20_000 + index * 1_000,
                200,
                "principal-sparse",
                upstream_id,
            );
            event.error_code = None;
            if index == 0 {
                event.status = 429;
            } else if index == 1 {
                event.error_code = Some("stream_error".to_owned());
            } else if index == 2 {
                event.status = 500;
            }
            storage.append_request_event(&event).await?;
        }

        let errors = RequestEventStreamFilters {
            principal_id: Some("principal-sparse".to_owned()),
            errors_only: true,
            ..Default::default()
        };
        let first = storage
            .list_request_events(&list_query(errors.clone(), None, 2, None, None))
            .await?;
        assert_event_ids(&first, &["sparse-2", "sparse-1"]);
        let second = storage
            .list_request_events(&list_query(
                errors.clone(),
                None,
                2,
                first[1].ts_ms,
                first[1].event_id.clone(),
            ))
            .await?;
        assert_event_ids(&second, &["sparse-0"]);
        let third = storage
            .list_request_events(&list_query(
                errors,
                None,
                2,
                second[0].ts_ms,
                second[0].event_id.clone(),
            ))
            .await?;
        assert!(third.is_empty());

        assert_cursor_pages_filter_before_limit(backend.as_ref(), storage.as_ref()).await?;
        assert_cursor_model_filter_preserves_ascii_case(backend.as_ref(), storage.as_ref()).await?;

        Ok(())
    })
    .await
}

pub async fn request_event_list_model_filter_matches_case_insensitive_prefix<
    B: ConformanceBackend,
>(
    backend: Arc<B>,
) -> Result<()> {
    with_fixture(backend, |storage| async move {
        let upstream_id = Uuid::from_u128(7);
        let base = request_event(
            "event-base",
            "req-base",
            5_000,
            200,
            "principal-a",
            upstream_id,
        );

        let mut legacy = decoy(&base, "event-legacy", 4_500);
        legacy.model = Some("claude-3-5-sonnet-20241022".to_owned());
        let mut dated = decoy(&base, "event-dated", 4_000);
        dated.model = Some("claude-sonnet-4-5-20250929".to_owned());
        let mut upper = decoy(&base, "event-upper", 3_000);
        upper.model = Some("CLAUDE-SONNET-4-5-PREVIEW".to_owned());
        let mut other_family = decoy(&base, "event-opus", 2_000);
        other_family.model = Some("claude-opus-4-1".to_owned());
        let mut non_vendor = decoy(&base, "event-local", 1_500);
        non_vendor.model = Some("local-sonnet".to_owned());
        let mut wildcard = decoy(&base, "event-wildcard", 1_000);
        wildcard.model = Some("claude%sonnet".to_owned());
        let mut model_less = decoy(&base, "event-model-less", 500);
        model_less.model = None;

        for event in [
            &base,
            &legacy,
            &dated,
            &upper,
            &other_family,
            &non_vendor,
            &wildcard,
            &model_less,
        ] {
            storage.append_request_event(event).await?;
        }

        let by_model = |model: &str| RequestEventStreamFilters {
            model: Some(model.to_owned()),
            ..RequestEventStreamFilters::default()
        };
        let sonnet_family = ["event-base", "event-legacy", "event-dated", "event-upper"];

        // The `claude-` vendor prefix on the needle is optional: with or
        // without it, any case, the needle matches the start of the model or
        // anywhere after a stored `claude-` prefix. Other families,
        // non-`claude-*` models containing the needle mid-name, and
        // model-less rows stay out.
        for needle in ["claude-sonnet", "Claude-Sonnet", "sonnet", "  SONNET  "] {
            let page = storage
                .list_request_events(&list_query(by_model(needle), None, 10, None, None))
                .await?;
            assert_event_ids(&page, &sonnet_family);
        }

        // A version-qualified fragment matches mid-name inside `claude-*`.
        for needle in [
            "3-5-sonnet",
            "claude-3-5-sonnet",
            "claude-3-5-sonnet-20241022",
        ] {
            let page = storage
                .list_request_events(&list_query(by_model(needle), None, 10, None, None))
                .await?;
            assert_event_ids(&page, &["event-legacy"]);
        }

        let exact_page = storage
            .list_request_events(&list_query(
                by_model("claude-sonnet-4-5-20250929"),
                None,
                10,
                None,
                None,
            ))
            .await?;
        assert_event_ids(&exact_page, &["event-dated"]);

        // Non-vendor model names still match on their own literal prefix.
        let non_vendor_page = storage
            .list_request_events(&list_query(by_model("local"), None, 10, None, None))
            .await?;
        assert_event_ids(&non_vendor_page, &["event-local"]);

        // `%` is a literal in the needle, not a wildcard.
        let wildcard_page = storage
            .list_request_events(&list_query(by_model("claude%"), None, 10, None, None))
            .await?;
        assert_event_ids(&wildcard_page, &["event-wildcard"]);

        let miss_page = storage
            .list_request_events(&list_query(by_model("gpt"), None, 10, None, None))
            .await?;
        assert_event_ids(&miss_page, &[]);

        // Blank or bare-prefix needles are an absent filter: every row,
        // including model-less ones, passes.
        for needle in ["", "   ", "claude-", "CLAUDE-"] {
            let page = storage
                .list_request_events(&list_query(by_model(needle), None, 10, None, None))
                .await?;
            assert_event_ids(
                &page,
                &[
                    "event-base",
                    "event-legacy",
                    "event-dated",
                    "event-upper",
                    "event-opus",
                    "event-local",
                    "event-wildcard",
                    "event-model-less",
                ],
            );
        }

        Ok(())
    })
    .await
}

pub async fn request_event_kind_sets_filter_before_pagination<B: ConformanceBackend>(
    backend: Arc<B>,
) -> Result<()> {
    with_fixture(Arc::clone(&backend), |storage| async move {
        const PREFIX_LEN: usize = 501;
        const GENERATION_END: usize = 1_002;
        const ROW_COUNT: usize = 1_008;
        let generation_kinds = [
            RequestEventKind::Messages,
            RequestEventKind::OpenaiResponses,
            RequestEventKind::GeminiGenerateContent,
        ];
        let mut expected_rows = Vec::with_capacity(ROW_COUNT);
        for index in 0..ROW_COUNT {
            // Newer metadata rows precede older generation rows both in list
            // order and append order, exposing either raw-page filtering bug.
            let mut event = cursor_event(index);
            event.status = if index.is_multiple_of(2) { 503 } else { 200 };
            event.error_code = None;
            event.event_kind = Some(if index < PREFIX_LEN {
                RequestEventKind::Models
            } else {
                generation_kinds[(index - PREFIX_LEN) % generation_kinds.len()]
            });
            match index {
                1_002 => {
                    event.source_kind = Some("renewal".to_owned());
                    event.event_kind = Some(RequestEventKind::Messages);
                }
                1_003 => {
                    event.source_kind = Some("renewal".to_owned());
                    event.event_kind = None;
                }
                1_004 => event.event_kind = Some(RequestEventKind::Renewal),
                1_005 => {
                    event.source_kind = None;
                    event.event_kind = None;
                }
                1_006 => event.event_kind = Some(RequestEventKind::CountTokens),
                1_007 => event.event_kind = Some(RequestEventKind::Other),
                _ => {}
            }
            let cursor = storage.append_request_event(&event).await?;
            expected_rows.push((cursor, event.event_id.expect("kind-set fixture event ID")));
        }
        let hi = expected_rows.last().expect("kind-set high fence").0;
        backend
            .wait_for_events_visible_for_rollup(storage.as_ref())
            .await?;

        let cases = [
            (
                "generation kinds cross 501 newer metadata rows and a full matching page",
                Some(generation_kinds.to_vec()),
                None,
                (PREFIX_LEN..GENERATION_END).collect::<Vec<_>>(),
            ),
            (
                "duplicate kinds are set membership, not duplicate rows",
                Some(vec![
                    RequestEventKind::GeminiGenerateContent,
                    RequestEventKind::Messages,
                    RequestEventKind::Messages,
                ]),
                Some("proxy"),
                (PREFIX_LEN..GENERATION_END)
                    .filter(|index| (index - PREFIX_LEN) % 3 != 1)
                    .collect(),
            ),
            (
                "renewal source wins over recorded messages and NULL is unclassified",
                Some(vec![
                    RequestEventKind::Renewal,
                    RequestEventKind::Unclassified,
                ]),
                None,
                (1_002..1_006).collect(),
            ),
            (
                "all sources does not remove an explicit kind set",
                Some(generation_kinds.to_vec()),
                Some("all"),
                (PREFIX_LEN..GENERATION_END).collect(),
            ),
            (
                "source conjunction keeps only stored proxy renewal",
                Some(vec![
                    RequestEventKind::Renewal,
                    RequestEventKind::Unclassified,
                ]),
                Some("proxy"),
                vec![1_004],
            ),
            (
                "source conjunction keeps both authoritative renewal sources",
                Some(vec![RequestEventKind::Messages, RequestEventKind::Renewal]),
                Some("renewal"),
                vec![1_002, 1_003],
            ),
            (
                "recorded generation kind cannot override renewal source",
                Some(generation_kinds.to_vec()),
                Some("renewal"),
                Vec::new(),
            ),
            (
                "empty set stays empty even with all sources",
                Some(Vec::new()),
                Some("all"),
                Vec::new(),
            ),
            (
                "empty set stays empty without an explicit source",
                Some(Vec::new()),
                None,
                Vec::new(),
            ),
            (
                "absent kind set preserves default source renewal exclusion",
                None,
                None,
                (0..GENERATION_END).chain(1_004..ROW_COUNT).collect(),
            ),
            (
                "all sources without a kind set includes every row",
                None,
                Some("all"),
                (0..ROW_COUNT).collect(),
            ),
        ];
        for (name, event_kinds, source_kind, expected) in cases {
            let filters = RequestEventStreamFilters {
                event_kinds,
                ..Default::default()
            };
            let mut query = list_query(filters.clone(), source_kind, 500, None, None);
            for expected_page in expected.chunks(500) {
                let page = storage.list_request_events(&query).await?;
                let actual = page
                    .iter()
                    .map(|row| row.event_id.as_deref().expect("listed kind-set event ID"))
                    .collect::<Vec<_>>();
                let expected_ids = expected_page
                    .iter()
                    .map(|&index| expected_rows[index].1.as_str())
                    .collect::<Vec<_>>();
                assert_eq!(actual, expected_ids, "{name}: list page");
                let last = page.last().expect("nonempty kind-set list page");
                query.until_ts_ms = last.ts_ms;
                query.until_event_id = last.event_id.clone();
            }
            assert!(
                storage.list_request_events(&query).await?.is_empty(),
                "{name}: list exhausted"
            );

            let buckets = storage
                .request_event_histogram(&RequestEventHistogramQuery {
                    since_unix_secs: 1_998_000,
                    until_unix_secs: 2_000_000,
                    bucket_ms: 2_001_000,
                    bucket_count: 1,
                    filters: filters.clone(),
                    source_kind: source_kind.map(str::to_owned),
                })
                .await?;
            assert_eq!(buckets.len(), 1, "{name}: zero-filled histogram axis");
            assert_eq!(
                buckets[0].total_count,
                expected.len() as u64,
                "{name}: histogram counts all matches without the list cap"
            );
            assert_eq!(
                buckets[0].error_count,
                expected
                    .iter()
                    .filter(|index| index.is_multiple_of(2))
                    .count() as u64,
                "{name}: histogram error count"
            );

            // The raw cursor API has no implicit admin renewal exclusion;
            // translate that default explicitly, just as the admin caller does.
            let cursor_filters = RequestEventStreamFilters {
                source_kind: source_kind
                    .filter(|source| *source != "all")
                    .map(str::to_owned),
                exclude_renewal: source_kind.is_none() && filters.event_kinds.is_none(),
                ..filters
            };
            let mut after = 0;
            for expected_page in expected.chunks(500) {
                let rows = storage
                    .query_request_events_between_cursors(after, hi, 500, &cursor_filters)
                    .await?;
                assert_cursor_rows(&rows, &expected_rows, expected_page, name);
                after = rows.last().expect("nonempty kind-set cursor page").0;
            }
            let exhausted = storage
                .query_request_events_between_cursors(after, hi, 500, &cursor_filters)
                .await?;
            assert_cursor_rows(&exhausted, &expected_rows, &[], name);
        }
        Ok(())
    })
    .await
}

async fn assert_cursor_pages_filter_before_limit<B: ConformanceBackend>(
    backend: &B,
    storage: &B::Storage,
) -> Result<()> {
    const PREFIX_LEN: usize = 501;
    const MATCH_LEN: usize = 501;

    backend.wait_for_events_visible_for_rollup(storage).await?;
    let after = storage.current_request_event_cursor().await?;
    let mut expected_rows = Vec::with_capacity(PREFIX_LEN + MATCH_LEN + 1);
    for index in 0..(PREFIX_LEN + MATCH_LEN) {
        let mut event = cursor_event(index);
        if index < PREFIX_LEN {
            event.principal_id = Some("cursor-other-principal".to_owned());
            event.thread_id = Some("cursor-other-thread".to_owned());
            event.upstream_id = Some(Uuid::from_u128(99));
            event.model = Some("sonnetAA-local".to_owned());
            event.status = 200;
            event.event_kind = Some(RequestEventKind::Models);
        } else {
            let offset = index - PREFIX_LEN;
            if offset >= 499 {
                event.upstream_id = Some(Uuid::from_u128(99));
                event.status = 200;
            }
            if offset == 498 {
                event.source_kind = None;
                event.event_kind = None;
            } else if offset == 499 {
                event.error_code = Some(String::new());
                event.event_kind = Some(RequestEventKind::Renewal);
            } else if offset == 500 {
                event.thread_id = Some("cursor-other-thread".to_owned());
                event.model = Some("cursor-gap-last".to_owned());
                event.source_kind = Some("renewal".to_owned());
            }
        }
        let cursor = storage.append_request_event(&event).await?;
        expected_rows.push((cursor, event.event_id.expect("cursor fixture event ID")));
    }
    let hi = expected_rows.last().expect("cursor fixture high fence").0;
    let outside = cursor_event(PREFIX_LEN + MATCH_LEN);
    let outside_cursor = storage.append_request_event(&outside).await?;
    expected_rows.push((
        outside_cursor,
        outside.event_id.expect("outside-fence event ID"),
    ));
    backend.wait_for_events_visible_for_rollup(storage).await?;

    // Default is the raw tail-poller contract, not the admin list default:
    // both a stored renewal kind and an authoritative renewal source survive.
    let raw_filters = RequestEventStreamFilters::default();
    let mut raw_after = after;
    for (start, end) in [(0, 500), (500, 1_000), (1_000, 1_002)] {
        let rows = storage
            .query_request_events_between_cursors(raw_after, hi, 500, &raw_filters)
            .await?;
        assert_cursor_rows(
            &rows,
            &expected_rows,
            &(start..end).collect::<Vec<_>>(),
            "raw Default page includes renewals and respects fixed hi",
        );
        raw_after = rows.last().expect("nonempty raw page").0;
    }
    let later = storage
        .query_request_events_between_cursors(hi, outside_cursor, 500, &raw_filters)
        .await?;
    assert_cursor_rows(
        &later,
        &expected_rows,
        &[PREFIX_LEN + MATCH_LEN],
        "append above fixed hi remains available to the next range",
    );

    let model = |needle: &str| RequestEventStreamFilters {
        model: Some(needle.to_owned()),
        ..Default::default()
    };
    let cases = [
        (
            "zero filtered matches",
            RequestEventStreamFilters {
                principal_id: Some("cursor-absent".to_owned()),
                ..Default::default()
            },
            Vec::new(),
        ),
        (
            "principal: 501 filtered matches",
            RequestEventStreamFilters {
                principal_id: Some("cursor-principal".to_owned()),
                ..Default::default()
            },
            (501..1_002).collect(),
        ),
        (
            "thread: 500 filtered matches",
            RequestEventStreamFilters {
                thread_id: Some("cursor-thread".to_owned()),
                ..Default::default()
            },
            (501..1_001).collect(),
        ),
        (
            "upstream: 499 filtered matches",
            RequestEventStreamFilters {
                upstream_id: Some(Uuid::from_u128(7)),
                ..Default::default()
            },
            (501..1_000).collect(),
        ),
        (
            "model prefix and vendor contains are case-insensitive and literal",
            model("  SoNnEt%_  "),
            (502..1_001).collect(),
        ),
        (
            "model prefix arm",
            model("SONNET%_-LOCAL"),
            (503..1_000).step_by(2).collect(),
        ),
        (
            "model vendor contains arm",
            model("claude-3-5-sonnet%_"),
            (502..1_001).step_by(2).collect(),
        ),
        (
            "status class",
            RequestEventStreamFilters {
                status_class: Some(StatusClass::FiveXx),
                ..Default::default()
            },
            (501..1_000).collect(),
        ),
        (
            "errors include failing status or any non-NULL error code",
            RequestEventStreamFilters {
                errors_only: true,
                ..Default::default()
            },
            (501..1_001).collect(),
        ),
        (
            "effective messages exclude renewal source and historical NULL kind",
            RequestEventStreamFilters {
                event_kinds: Some(vec![RequestEventKind::Messages]),
                ..Default::default()
            },
            (501..999).collect(),
        ),
        (
            "effective renewal uses source precedence or stored kind",
            RequestEventStreamFilters {
                event_kinds: Some(vec![RequestEventKind::Renewal]),
                ..Default::default()
            },
            vec![1_000, 1_001],
        ),
        (
            "multiple effective kinds include messages and renewal but not unclassified",
            RequestEventStreamFilters {
                event_kinds: Some(vec![RequestEventKind::Messages, RequestEventKind::Renewal]),
                ..Default::default()
            },
            (501..999).chain(1_000..1_002).collect(),
        ),
        (
            "empty event kind set matches nothing",
            RequestEventStreamFilters {
                event_kinds: Some(Vec::new()),
                ..Default::default()
            },
            Vec::new(),
        ),
        (
            "historical NULL kind is unclassified",
            RequestEventStreamFilters {
                event_kinds: Some(vec![RequestEventKind::Unclassified]),
                ..Default::default()
            },
            vec![999],
        ),
        (
            "exact source_kind proxy",
            RequestEventStreamFilters {
                source_kind: Some("proxy".to_owned()),
                ..Default::default()
            },
            (0..999).chain([1_000]).collect(),
        ),
        (
            "exact source_kind renewal",
            RequestEventStreamFilters {
                source_kind: Some("renewal".to_owned()),
                ..Default::default()
            },
            vec![1_001],
        ),
        (
            "exclude source renewal preserves NULL and proxy-stored renewal",
            RequestEventStreamFilters {
                exclude_renewal: true,
                ..Default::default()
            },
            (0..1_001).collect(),
        ),
        (
            "event kind and source kind are conjunctive",
            RequestEventStreamFilters {
                event_kinds: Some(vec![RequestEventKind::Renewal]),
                source_kind: Some("proxy".to_owned()),
                ..Default::default()
            },
            vec![1_000],
        ),
        (
            "renewal source conflicts with exclude renewal",
            RequestEventStreamFilters {
                source_kind: Some("renewal".to_owned()),
                exclude_renewal: true,
                ..Default::default()
            },
            Vec::new(),
        ),
        (
            "raw Default preserves renewal rows",
            RequestEventStreamFilters::default(),
            (0..1_002).collect(),
        ),
        (
            "combined matcher fields are conjunctive",
            RequestEventStreamFilters {
                principal_id: Some("cursor-principal".to_owned()),
                thread_id: Some("cursor-thread".to_owned()),
                model: Some("  SoNnEt%_  ".to_owned()),
                upstream_id: Some(Uuid::from_u128(7)),
                provider_upstream_ids: None,
                status_class: Some(StatusClass::FiveXx),
                errors_only: true,
                event_kinds: Some(vec![RequestEventKind::Messages]),
                source_kind: None,
                exclude_renewal: false,
            },
            (502..999).collect(),
        ),
    ];
    for (name, filters, expected) in cases {
        let mut page_after = after;
        for expected_page in expected.chunks(500) {
            let rows = storage
                .query_request_events_between_cursors(page_after, hi, 500, &filters)
                .await?;
            assert_cursor_rows(&rows, &expected_rows, expected_page, name);
            page_after = rows.last().expect("nonempty filtered page").0;
        }
        let exhausted = storage
            .query_request_events_between_cursors(page_after, hi, 500, &filters)
            .await?;
        assert_cursor_rows(&exhausted, &expected_rows, &[], name);
    }

    // A successful first page does not justify stopping at the next raw gap.
    // limit=1 must cross another nonmatching window on continuation.
    let gap_filter = model("cursor-gap");
    let first = storage
        .query_request_events_between_cursors(after, hi, 1, &gap_filter)
        .await?;
    assert_cursor_rows(&first, &expected_rows, &[501], "limit=1 first match");
    let second = storage
        .query_request_events_between_cursors(first[0].0, hi, 1, &gap_filter)
        .await?;
    assert_cursor_rows(
        &second,
        &expected_rows,
        &[1_001],
        "limit=1 continuation crosses a sparse gap",
    );
    let exhausted = storage
        .query_request_events_between_cursors(second[0].0, hi, 1, &gap_filter)
        .await?;
    assert_cursor_rows(&exhausted, &expected_rows, &[], "limit=1 exhausted");

    Ok(())
}

async fn assert_cursor_model_filter_preserves_ascii_case<B: ConformanceBackend>(
    backend: &B,
    storage: &B::Storage,
) -> Result<()> {
    backend.wait_for_events_visible_for_rollup(storage).await?;
    let after = storage.current_request_event_cursor().await?;
    let mut expected_rows = Vec::new();
    for (index, model) in ["Éclair", "ÉCLAIR-preview", "éclair"]
        .into_iter()
        .enumerate()
    {
        let mut event = cursor_event(2_000 + index);
        event.model = Some(model.to_owned());
        let cursor = storage.append_request_event(&event).await?;
        expected_rows.push((cursor, event.event_id.expect("ASCII-case fixture event ID")));
    }
    let hi = expected_rows.last().expect("ASCII-case high fence").0;
    backend.wait_for_events_visible_for_rollup(storage).await?;
    for (needle, expected, name) in [
        (
            "Éclair",
            vec![0, 1],
            "exact non-ASCII identity preserves ASCII-insensitive prefix matching",
        ),
        (
            "ÉCLAIR",
            vec![0, 1],
            "ASCII case changes do not fold distinct non-ASCII characters",
        ),
        (
            "éclair",
            vec![2],
            "lowercase non-ASCII needle excludes uppercase non-ASCII models",
        ),
    ] {
        let filters = RequestEventStreamFilters {
            model: Some(needle.to_owned()),
            ..Default::default()
        };
        let rows = storage
            .query_request_events_between_cursors(after, hi, 500, &filters)
            .await?;
        assert_cursor_rows(&rows, &expected_rows, &expected, name);
    }

    const PREFIX_LEN: usize = 501;
    backend.wait_for_events_visible_for_rollup(storage).await?;
    let after = storage.current_request_event_cursor().await?;
    let mut expected_rows = Vec::with_capacity(PREFIX_LEN + 1);
    for index in 0..=PREFIX_LEN {
        let mut event = cursor_event(3_000 + index);
        event.model = Some(
            if index < PREFIX_LEN {
                "Éclair-nonmatch"
            } else {
                "éClAiR-match"
            }
            .to_owned(),
        );
        let cursor = storage.append_request_event(&event).await?;
        expected_rows.push((
            cursor,
            event.event_id.expect("non-ASCII gap fixture event ID"),
        ));
    }
    let hi = expected_rows.last().expect("non-ASCII gap high fence").0;
    backend.wait_for_events_visible_for_rollup(storage).await?;
    let filters = RequestEventStreamFilters {
        model: Some("éclair".to_owned()),
        ..Default::default()
    };
    let rows = storage
        .query_request_events_between_cursors(after, hi, 500, &filters)
        .await?;
    assert_cursor_rows(
        &rows,
        &expected_rows,
        &[PREFIX_LEN],
        "501 Unicode-fold false positives do not consume the matching cap",
    );
    let exhausted = storage
        .query_request_events_between_cursors(rows[0].0, hi, 500, &filters)
        .await?;
    assert_cursor_rows(
        &exhausted,
        &expected_rows,
        &[],
        "non-ASCII gap continuation is exhausted",
    );

    Ok(())
}

fn cursor_event(index: usize) -> RequestEvent {
    RequestEvent {
        // Deliberately descending timestamps: cursor pages order by append,
        // not the reverse-chronological list ordering.
        ts: 2_000_000 - index as u64,
        ts_ms: Some((2_000_000 - index as u64) * 1_000),
        event_id: Some(format!("cursor-event-{index:04}")),
        source_kind: Some("proxy".to_owned()),
        event_kind: Some(RequestEventKind::Messages),
        principal_id: Some("cursor-principal".to_owned()),
        thread_id: Some("cursor-thread".to_owned()),
        upstream_id: Some(Uuid::from_u128(7)),
        model: Some(
            if index == 501 {
                "cursor-gap-first"
            } else if index == 1001 {
                "cursor-gap-last"
            } else if index.is_multiple_of(2) {
                "CLAUDE-3-5-SONNET%_-20241022"
            } else {
                "SONNET%_-local"
            }
            .to_owned(),
        ),
        status: 503,
        ..Default::default()
    }
}

fn assert_cursor_rows(
    rows: &[(u64, RequestEvent)],
    expected_rows: &[(u64, String)],
    expected_indices: &[usize],
    name: &str,
) {
    let actual = rows
        .iter()
        .map(|(cursor, event)| (*cursor, event.event_id.as_deref().expect("stored event ID")))
        .collect::<Vec<_>>();
    let expected = expected_indices
        .iter()
        .map(|&index| (expected_rows[index].0, expected_rows[index].1.as_str()))
        .collect::<Vec<_>>();
    assert_eq!(actual, expected, "{name}");
}

async fn with_fixture<B, F, Fut>(backend: Arc<B>, scenario: F) -> Result<()>
where
    B: ConformanceBackend,
    F: FnOnce(Arc<B::Storage>) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let mut fixture = ConformanceFixture::new(backend).await?;
    let result = scenario(fixture.storage()).await;
    let teardown_result = fixture.teardown().await;

    result?;
    teardown_result
}

fn request_event(
    event_id: &str,
    request_id: &str,
    ts_ms: u64,
    status: u16,
    principal_id: &str,
    upstream_id: Uuid,
) -> RequestEvent {
    RequestEvent {
        ts: ts_ms / 1_000,
        ts_ms: Some(ts_ms),
        request_id: request_id.to_owned(),
        event_id: Some(event_id.to_owned()),
        source_kind: Some("proxy".to_owned()),
        principal_id: Some(principal_id.to_owned()),
        upstream_id: Some(upstream_id),
        upstream_name: Some("upstream-a".to_owned()),
        thread_id: Some("thread-a".to_owned()),
        model: Some("claude-sonnet-4-5".to_owned()),
        observed_session_id: Some("session-a".to_owned()),
        request_kind: Some("subagent".to_owned()),
        claude_agent_id: Some("agent-a".to_owned()),
        claude_parent_agent_id: Some("agent-parent".to_owned()),
        parent_session_id: Some("session-parent".to_owned()),
        client_app: Some("cli-bg".to_owned()),
        session_id_source: Some("x-claude-code-session-id".to_owned()),
        reasoning_effort: Some("high".to_owned()),
        thinking_level: Some("LOW".to_owned()),
        thinking_budget_tokens: Some(101),
        thinking_tokens: Some(102),
        service_tier: Some("standard".to_owned()),
        status,
        error_code: Some("provider_error".to_owned()),
        upstream_error_type: Some("rate_limit_error".to_owned()),
        upstream_error_message: Some("bounded provider message".to_owned()),
        duration_ms: 40,
        auth_ms: Some(5),
        route_ms: Some(6),
        limit_reserve_ms: Some(7),
        request_body_first_chunk_ms: Some(0.125),
        request_body_receive_ms: Some(1.375),
        request_body_wait_ms: Some(2.625),
        request_body_process_ms: Some(0.875),
        request_body_chunk_count: Some(3),
        response_body_wait_ms: Some(3.125),
        response_body_process_ms: Some(0.625),
        response_body_downstream_poll_gap_ms: Some(4.875),
        retry_overhead_ms: Some(5.5),
        json_parse_ms: Some(0.125),
        cache_structure_ms: Some(0.25),
        cache_token_key_ms: Some(0.375),
        cache_count_lookup_ms: Some(0.5),
        cache_tokenizer_queue_ms: Some(0.625),
        cache_serialize_ms: Some(0.75),
        cache_tokenize_ms: Some(0.0),
        prepare_signer_ms: Some(1.25),
        bulkhead_wait_ms: Some(8),
        dns_ms: Some(9),
        connect_ms: Some(10),
        connection_reused: Some(true),
        proxy_setup_ms: Some(13),
        shape_ms: Some(14),
        sign_ms: Some(15),
        upstream_ttfb_ms: Some(16),
        upstream_body_ms: Some(17),
        stream_first_content_delta_ms: Some(18),
        stream_last_content_delta_ms: Some(19),
        inter_token_avg_ms: Some(20),
        input_tokens: Some(21),
        output_tokens: Some(22),
        cache_creation_input_tokens: Some(23),
        cache_creation_input_tokens_5m: Some(24),
        cache_creation_input_tokens_1h: Some(25),
        cache_read_input_tokens: Some(26),
        cache_write_input_tokens: Some(33),
        cache_write_input_tokens_30m: Some(34),
        cache_write_input_tokens_12h: Some(35),
        cost_usd_micros: Some(27),
        cost_input_micros: Some(28),
        cost_output_micros: Some(29),
        cost_cache_creation_5m_micros: Some(30),
        cost_cache_creation_1h_micros: Some(31),
        cost_cache_read_micros: Some(32),
        cost_cache_write_micros: Some(36),
        cost_cache_write_30m_micros: Some(37),
        cost_cache_write_12h_micros: Some(38),
        ..Default::default()
    }
}

fn list_query(
    filters: RequestEventStreamFilters,
    source_kind: Option<&str>,
    limit: usize,
    until_ts_ms: Option<u64>,
    until_event_id: Option<String>,
) -> RequestEventListQuery {
    RequestEventListQuery {
        since_unix_secs: 0,
        until_unix_secs: u64::MAX,
        until_ts_ms,
        until_event_id,
        limit,
        filters,
        source_kind: source_kind.map(str::to_owned),
    }
}

fn assert_event_ids(items: &[RequestEventListItem], expected: &[&str]) {
    let actual = items
        .iter()
        .map(|item| item.event_id.as_deref().unwrap_or_default())
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
}

fn assert_current_io_timings(item: &RequestEventListItem) {
    assert_eq!(item.request_body_first_chunk_ms, Some(0.125));
    assert_eq!(item.request_body_receive_ms, Some(1.375));
    assert_eq!(item.request_body_wait_ms, Some(2.625));
    assert_eq!(item.request_body_process_ms, Some(0.875));
    assert_eq!(item.request_body_chunk_count, Some(3));
    assert_eq!(item.response_body_wait_ms, Some(3.125));
    assert_eq!(item.response_body_process_ms, Some(0.625));
    assert_eq!(item.response_body_downstream_poll_gap_ms, Some(4.875));
    assert_eq!(item.retry_overhead_ms, Some(5.5));
}

fn assert_legacy_io_timings(item: &RequestEventListItem) {
    assert_eq!(item.request_body_first_chunk_ms, None);
    assert_eq!(item.request_body_receive_ms, None);
    assert_eq!(item.request_body_wait_ms, None);
    assert_eq!(item.request_body_process_ms, None);
    assert_eq!(item.request_body_chunk_count, None);
    assert_eq!(item.response_body_wait_ms, None);
    assert_eq!(item.response_body_process_ms, None);
    assert_eq!(item.response_body_downstream_poll_gap_ms, None);
    assert_eq!(item.retry_overhead_ms, None);
}

async fn assert_list_matches_details<S>(storage: &S, items: &[RequestEventListItem]) -> Result<()>
where
    S: RequestEventStore + ?Sized,
{
    for item in items {
        let event_id = item.event_id.as_deref().expect("list event ID");
        let detail = storage
            .get_request_event(event_id)
            .await?
            .expect("stored request event detail");
        assert_eq!(item, &list_projection(&detail));
    }
    Ok(())
}

fn list_projection(event: &RequestEvent) -> RequestEventListItem {
    RequestEventListItem {
        ts: event.ts,
        ts_ms: Some(
            event
                .ts_ms
                .unwrap_or_else(|| event.ts.saturating_mul(1_000)),
        ),
        request_id: event.request_id.clone(),
        event_id: event.event_id.clone(),
        source_kind: event.source_kind.clone(),
        event_kind: event.event_kind,
        principal_id: event.principal_id.clone(),
        upstream_id: event.upstream_id,
        upstream_name: event.upstream_name.clone(),
        thread_id: event.thread_id.clone(),
        observed_session_id: event.observed_session_id.clone(),
        request_kind: event.request_kind.clone(),
        claude_agent_id: event.claude_agent_id.clone(),
        claude_parent_agent_id: event.claude_parent_agent_id.clone(),
        parent_session_id: event.parent_session_id.clone(),
        client_app: event.client_app.clone(),
        session_id_source: event.session_id_source.clone(),
        model: event.model.clone(),
        reasoning_effort: event.reasoning_effort.clone(),
        thinking_level: event.thinking_level.clone(),
        thinking_budget_tokens: event.thinking_budget_tokens,
        thinking_tokens: event.thinking_tokens,
        service_tier: event.service_tier.clone(),
        status: event.status,
        error_code: event.error_code.clone(),
        upstream_error_type: event.upstream_error_type.clone(),
        upstream_error_message: event.upstream_error_message.clone(),
        duration_ms: event.duration_ms,
        request_body_read_ms: event.request_body_read_ms,
        request_body_bytes: event.request_body_bytes,
        request_body_first_chunk_ms: event.request_body_first_chunk_ms,
        request_body_receive_ms: event.request_body_receive_ms,
        request_body_wait_ms: event.request_body_wait_ms,
        request_body_process_ms: event.request_body_process_ms,
        request_body_chunk_count: event.request_body_chunk_count,
        response_body_wait_ms: event.response_body_wait_ms,
        response_body_process_ms: event.response_body_process_ms,
        response_body_downstream_poll_gap_ms: event.response_body_downstream_poll_gap_ms,
        retry_overhead_ms: event.retry_overhead_ms,
        auth_ms: event.auth_ms,
        route_ms: event.route_ms,
        limit_reserve_ms: event.limit_reserve_ms,
        json_parse_ms: event.json_parse_ms,
        cache_structure_ms: event.cache_structure_ms,
        cache_token_key_ms: event.cache_token_key_ms,
        cache_count_lookup_ms: event.cache_count_lookup_ms,
        cache_tokenizer_queue_ms: event.cache_tokenizer_queue_ms,
        cache_serialize_ms: event.cache_serialize_ms,
        cache_tokenize_ms: event.cache_tokenize_ms,
        prepare_signer_ms: event.prepare_signer_ms,
        bulkhead_wait_ms: event.bulkhead_wait_ms,
        dns_ms: event.dns_ms,
        connect_ms: event.connect_ms,
        connection_reused: event.connection_reused,
        proxy_setup_ms: event.proxy_setup_ms,
        shape_ms: event.shape_ms,
        sign_ms: event.sign_ms,
        upstream_ttfb_ms: event.upstream_ttfb_ms,
        upstream_body_ms: event.upstream_body_ms,
        finalize_ms: event.finalize_ms,
        stream_first_content_delta_ms: event.stream_first_content_delta_ms,
        stream_last_content_delta_ms: event.stream_last_content_delta_ms,
        inter_token_avg_ms: event.inter_token_avg_ms,
        input_tokens: event.input_tokens,
        output_tokens: event.output_tokens,
        cache_creation_input_tokens: event.cache_creation_input_tokens,
        cache_creation_input_tokens_5m: event.cache_creation_input_tokens_5m,
        cache_creation_input_tokens_1h: event.cache_creation_input_tokens_1h,
        cache_read_input_tokens: event.cache_read_input_tokens,
        cache_write_input_tokens: event.cache_write_input_tokens,
        cache_write_input_tokens_30m: event.cache_write_input_tokens_30m,
        cache_write_input_tokens_12h: event.cache_write_input_tokens_12h,
        cost_usd_micros: event.cost_usd_micros,
        cost_input_micros: event.cost_input_micros,
        cost_output_micros: event.cost_output_micros,
        cost_cache_creation_5m_micros: event.cost_cache_creation_5m_micros,
        cost_cache_creation_1h_micros: event.cost_cache_creation_1h_micros,
        cost_cache_read_micros: event.cost_cache_read_micros,
        cost_cache_write_micros: event.cost_cache_write_micros,
        cost_cache_write_30m_micros: event.cost_cache_write_30m_micros,
        cost_cache_write_12h_micros: event.cost_cache_write_12h_micros,
    }
}

fn decoy(event: &RequestEvent, event_id: &str, ts_ms: u64) -> RequestEvent {
    let mut event = event.clone();
    event.ts = ts_ms / 1_000;
    event.ts_ms = Some(ts_ms);
    event.request_id = format!("{event_id}-request");
    event.event_id = Some(event_id.to_owned());
    event
}
