use cc_lb_storage_api::{
    RequestEvent, RequestEventStore, RequestEventUpstream, UsageRollup, UsageRollupResolution,
    UsageRollupRun, UsageRollupStore,
};
use cc_lb_storage_redb::RedbStorage;

#[tokio::test]
async fn rollup_usage_once_trait_path_checkpoint_is_idempotent()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("adapter-rollups.redb");
    let storage = RedbStorage::open(&path)?;

    for event in fixture_events() {
        RequestEventStore::append_request_event(&storage, &event).await?;
    }

    let first = UsageRollupStore::rollup_usage_once(&storage).await?;
    assert_eq!(first.processed_events, 4);
    assert_eq!(first.updated_rollups, 5);
    assert!(first.checkpoint.is_some());
    assert_eq!(
        UsageRollupStore::usage_rollup_checkpoint(&storage).await?,
        first.checkpoint
    );

    let rollups = UsageRollupStore::query_usage_rollups(&storage).await?;
    assert_eq!(rollups.len(), 5);
    assert_rollup(
        find_rollup(
            &rollups,
            UsageRollupResolution::Minute,
            1_800_000_000,
            "principal-a",
            "anthropic_direct",
            "claude-sonnet-4-5",
        ),
        ExpectedRollup {
            request_count: 2,
            input_tokens: 15,
            output_tokens: 20,
            error_count: 1,
            latency_count: 2,
            latency_ms_sum: 400,
            latency_ms_min: Some(100),
            latency_ms_max: Some(300),
            virtual_cost_micros: 345,
        },
    );
    assert_rollup(
        find_rollup(
            &rollups,
            UsageRollupResolution::Hour,
            1_800_000_000,
            "principal-a",
            "anthropic_direct",
            "claude-sonnet-4-5",
        ),
        ExpectedRollup {
            request_count: 3,
            input_tokens: 22,
            output_tokens: 28,
            error_count: 1,
            latency_count: 3,
            latency_ms_sum: 450,
            latency_ms_min: Some(50),
            latency_ms_max: Some(300),
            virtual_cost_micros: 486,
        },
    );

    let second = UsageRollupStore::rollup_usage_once(&storage).await?;
    assert_eq!(second.processed_events, 0);
    assert_eq!(second.updated_rollups, 0);
    assert_eq!(second.checkpoint, first.checkpoint);
    assert_eq!(
        UsageRollupStore::query_usage_rollups(&storage).await?,
        rollups
    );

    Ok(())
}

#[tokio::test]
async fn advance_checkpoint_and_persist_trait_path_keeps_range_results_idempotent()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("adapter-rollups-advance.redb");
    let storage = RedbStorage::open(&path)?;

    for event in fixture_events() {
        RequestEventStore::append_request_event(&storage, &event).await?;
    }

    let requested_run = UsageRollupRun {
        processed_events: 0,
        updated_rollups: 0,
        checkpoint: None,
    };

    UsageRollupStore::advance_rollup_checkpoint_and_persist(&storage, &requested_run).await?;
    let first_checkpoint = UsageRollupStore::usage_rollup_checkpoint(&storage).await?;
    assert!(first_checkpoint.is_some());

    let first_minute = UsageRollupStore::query_usage_rollups_in_range(
        &storage,
        UsageRollupResolution::Minute,
        1_800_000_000,
        1_800_000_060,
    )
    .await?;
    assert_eq!(first_minute.len(), 2);
    assert!(first_minute.iter().all(|rollup| {
        rollup.resolution == UsageRollupResolution::Minute && rollup.bucket_start == 1_800_000_000
    }));

    let all_rollups = UsageRollupStore::query_usage_rollups(&storage).await?;
    UsageRollupStore::advance_rollup_checkpoint_and_persist(&storage, &requested_run).await?;
    assert_eq!(
        UsageRollupStore::usage_rollup_checkpoint(&storage).await?,
        first_checkpoint
    );
    assert_eq!(
        UsageRollupStore::query_usage_rollups(&storage).await?,
        all_rollups
    );

    Ok(())
}

#[derive(Clone, Copy)]
struct ExpectedRollup {
    request_count: u64,
    input_tokens: u64,
    output_tokens: u64,
    error_count: u64,
    latency_count: u64,
    latency_ms_sum: u64,
    latency_ms_min: Option<u64>,
    latency_ms_max: Option<u64>,
    virtual_cost_micros: u64,
}

fn assert_rollup(rollup: &UsageRollup, expected: ExpectedRollup) {
    assert_eq!(rollup.request_count, expected.request_count);
    assert_eq!(rollup.input_tokens, expected.input_tokens);
    assert_eq!(rollup.output_tokens, expected.output_tokens);
    assert_eq!(rollup.error_count, expected.error_count);
    assert_eq!(rollup.latency_count, expected.latency_count);
    assert_eq!(rollup.latency_ms_sum, expected.latency_ms_sum);
    assert_eq!(rollup.latency_ms_min, expected.latency_ms_min);
    assert_eq!(rollup.latency_ms_max, expected.latency_ms_max);
    assert_eq!(rollup.virtual_cost_micros, expected.virtual_cost_micros);
}

fn find_rollup<'a>(
    rollups: &'a [UsageRollup],
    resolution: UsageRollupResolution,
    bucket_start: u64,
    principal: &str,
    upstream: &str,
    model: &str,
) -> &'a UsageRollup {
    rollups
        .iter()
        .find(|rollup| {
            rollup.resolution == resolution
                && rollup.bucket_start == bucket_start
                && rollup.principal == principal
                && rollup.upstream == upstream
                && rollup.model == model
        })
        .expect("expected rollup row")
}

fn fixture_events() -> Vec<RequestEvent> {
    vec![
        event(
            1_800_000_005,
            "req-a-1",
            "principal-a",
            RequestEventUpstream::AnthropicDirect,
            "claude-sonnet-4-5",
            200,
            Some(10),
            Some(20),
            100,
        ),
        event(
            1_800_000_030,
            "req-a-2",
            "principal-a",
            RequestEventUpstream::AnthropicDirect,
            "claude-sonnet-4-5",
            429,
            Some(5),
            None,
            300,
        ),
        event(
            1_800_000_065,
            "req-a-3",
            "principal-a",
            RequestEventUpstream::AnthropicDirect,
            "claude-sonnet-4-5",
            200,
            Some(7),
            Some(8),
            50,
        ),
        event(
            1_800_000_045,
            "req-b-1",
            "principal-b",
            RequestEventUpstream::Vertex,
            "claude-opus-4-1",
            500,
            None,
            Some(1),
            700,
        ),
    ]
}

#[allow(clippy::too_many_arguments)]
fn event(
    ts: u64,
    request_id: &str,
    principal: &str,
    upstream: RequestEventUpstream,
    model: &str,
    status: u16,
    input_tokens: Option<u64>,
    output_tokens: Option<u64>,
    duration_ms: u64,
) -> RequestEvent {
    RequestEvent {
        ts,
        request_id: request_id.to_owned(),
        principal_id: Some(principal.to_owned()),
        principal_kind: Some("api_key".to_owned()),
        upstream: Some(upstream),
        model: Some(model.to_owned()),
        status,
        input_tokens,
        output_tokens,
        duration_ms,
        error_code: None,
    }
}
