use anyhow::Result;
use cc_lb_storage_api::{
    UpstreamStore, UpstreamWarmupAttemptStore, WarmupAttemptCursor, WarmupAttemptListFilters,
    WarmupAttemptOutcome, WarmupAttemptRecord, WarmupAttemptStatus, WarmupAttemptTrigger,
    WarmupDispatchKind,
    upstream::{UpstreamCreate, UpstreamKind, UpstreamRecord},
};
use uuid::Uuid;

pub async fn create_upstream(store: &dyn UpstreamStore, name: &str) -> Result<UpstreamRecord> {
    Ok(store
        .create(UpstreamCreate {
            id: Uuid::new_v4(),
            name: name.to_owned(),
            kind: UpstreamKind::AnthropicOauth,
            base_url: None,
            api_key_ciphertext: None,
            oauth_tokens: None,
            warmup_enabled: true,
            warmup_dialect_plugin: None,
        })
        .await?)
}

pub async fn insert_all(
    store: &dyn UpstreamWarmupAttemptStore,
    records: &[WarmupAttemptRecord],
) -> Result<()> {
    for record in records {
        store.insert_warmup_attempt(record).await?;
    }
    Ok(())
}

pub fn attempt(
    upstream_id: Uuid,
    id_seed: u128,
    attempted_at_unix_secs: i64,
    outcome: WarmupAttemptOutcome,
) -> WarmupAttemptRecord {
    WarmupAttemptRecord {
        id: Uuid::from_u128(0x4000 + id_seed),
        upstream_id,
        attempted_at_unix_secs,
        completed_at_unix_secs: Some(attempted_at_unix_secs + 5),
        scheduled_for_unix_secs: attempted_at_unix_secs - 60,
        trigger: WarmupAttemptTrigger::Scheduled,
        outcome,
        dispatch_kind: Some(WarmupDispatchKind::NotDispatched),
        http_status: None,
        cycle_key: None,
        expected_cycle_key: None,
        idle_secs_since_prev_window: None,
        replica_id: None,
        lease_holder: None,
        upstream_spec_revision: 1,
        dialect_plugin_snapshot: None,
        error_detail: None,
    }
}

pub fn outcomes(
    upstream_id: Uuid,
    outcomes: &[WarmupAttemptOutcome],
    first_attempted_at: i64,
) -> Vec<WarmupAttemptRecord> {
    outcomes
        .iter()
        .enumerate()
        .map(|(idx, outcome)| {
            attempt(
                upstream_id,
                idx as u128 + 1,
                first_attempted_at + idx as i64 * 10,
                *outcome,
            )
        })
        .collect()
}

pub fn filters(
    limit: Option<u32>,
    before: Option<WarmupAttemptCursor>,
    status: Option<WarmupAttemptStatus>,
) -> WarmupAttemptListFilters {
    WarmupAttemptListFilters {
        limit,
        before,
        status,
    }
}

pub fn cursor(record: &WarmupAttemptRecord) -> WarmupAttemptCursor {
    WarmupAttemptCursor {
        attempted_at_unix_secs: record.attempted_at_unix_secs,
        id: record.id,
    }
}

pub fn ids(records: &[WarmupAttemptRecord]) -> Vec<Uuid> {
    records.iter().map(|record| record.id).collect()
}
