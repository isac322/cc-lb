//! Atomicity conformance scenarios.

use anyhow::{Result, ensure};
use cc_lb_storage_api::{
    RequestEvent, RequestEventStore, UpstreamCreate, UpstreamStore, UpstreamUpdate, UsageRollup,
    UsageRollupResolution, UsageRollupStore, upstream::UpstreamKind,
};
use uuid::Uuid;

use super::super::{ConformanceBackend, ConformanceFixture};

pub async fn usage_rollup_v2_preserves_upstream_id_across_renames<B>(
    fixture: &ConformanceFixture<B>,
) -> Result<()>
where
    B: ConformanceBackend,
{
    let storage = fixture.storage();
    let upstream = storage
        .create(UpstreamCreate {
            id: Uuid::new_v4(),
            name: "alpha".to_owned(),
            kind: UpstreamKind::AnthropicApiKey,
            base_url: None,
            api_key_ciphertext: None,
            oauth_tokens: None,
            warmup_enabled: false,
            warmup_dialect_plugin: None,
            workspace_id: None,
            vertex: None,
            vertex_credential_type: None,
            vertex_quota_project_id: None,
            api_key_access: None,
        })
        .await?;
    let upstream_id = upstream.id;

    storage
        .append_request_event(&request_event_with_upstream_identity(
            1_800_010_005,
            "usage-rename-alpha",
            "usage-rename-principal",
            upstream_id,
            "alpha",
        ))
        .await?;
    storage.rollup_usage_once().await?;

    let updated = storage
        .update(
            upstream_id,
            upstream.revision,
            UpstreamUpdate {
                name: Some("beta".to_owned()),
                ..UpstreamUpdate::default()
            },
        )
        .await?;
    ensure!(
        updated.id == upstream_id,
        "rename must keep upstream id stable"
    );

    storage
        .append_request_event(&request_event_with_upstream_identity(
            1_800_010_065,
            "usage-rename-beta",
            "usage-rename-principal",
            upstream_id,
            "beta",
        ))
        .await?;
    storage.rollup_usage_once().await?;

    let rollups = all_usage_rollups(storage.as_ref()).await?;
    let by_upstream_id = rollups
        .iter()
        .filter(|rollup| rollup.upstream_id == upstream_id)
        .collect::<Vec<_>>();
    let observed = by_upstream_id
        .iter()
        .map(|rollup| {
            (
                rollup.resolution,
                rollup.bucket_start,
                rollup.upstream_name.clone(),
            )
        })
        .collect::<Vec<_>>();
    ensure!(
        by_upstream_id.len() == 3,
        "expected two minute rows plus one merged hour row for renamed events, got {}",
        by_upstream_id.len()
    );
    ensure!(
        by_upstream_id
            .iter()
            .any(|rollup| rollup.bucket_start == 1_800_009_960 && rollup.upstream_name == "alpha"),
        "alpha rollup should keep stable id with alpha display name; observed {observed:?}"
    );
    ensure!(
        by_upstream_id
            .iter()
            .any(|rollup| rollup.bucket_start == 1_800_010_020 && rollup.upstream_name == "beta"),
        "beta rollup should keep stable id with beta display name"
    );

    Ok(())
}

async fn all_usage_rollups<S: UsageRollupStore + ?Sized>(storage: &S) -> Result<Vec<UsageRollup>> {
    let mut rollups = Vec::new();
    for resolution in [UsageRollupResolution::Minute, UsageRollupResolution::Hour] {
        rollups.extend(
            storage
                .query_usage_rollups_in_range(resolution, 0, i64::MAX as u64)
                .await?,
        );
    }
    Ok(rollups)
}

fn request_event_with_upstream_identity(
    ts: u64,
    request_id: &str,
    principal: &str,
    upstream_id: Uuid,
    upstream_name: &str,
) -> RequestEvent {
    RequestEvent {
        ts,
        ts_ms: Some(ts * 1_000),
        request_id: request_id.to_owned(),
        event_id: Some(request_id.to_owned()),
        principal_id: Some(principal.to_owned()),
        principal_kind: Some("api_key".to_owned()),
        upstream_id: Some(upstream_id),
        upstream_name: Some(upstream_name.to_owned()),
        model: Some("claude-sonnet-4-5".to_owned()),
        status: 200,
        input_tokens: Some(1),
        output_tokens: Some(2),
        duration_ms: 10,
        error_code: None,
        ..Default::default()
    }
}
