#![allow(non_snake_case)]

use std::sync::{Arc, Mutex};

use cc_lb_aead::EncryptedOAuthTokens;
use cc_lb_storage_api::upstream::{UpstreamKind, UpstreamRecord};
use uuid::Uuid;

use super::{
    MetadataRefreshJob, MetadataRefreshJobHandler, MetadataRefreshJobOutcome, MetadataRefreshRunner,
};
use crate::error::Result;
type MaybeUpstreamRecord = Option<UpstreamRecord>;

#[tokio::test]
async fn t2__skips_stale_follow_up_when_current_generation_is_newer() -> Result<()> {
    // Given: a job for generation 4 and storage already at generation 5.
    let upstream_id = Uuid::from_u128(1);
    let runner = FakeMetadataRefreshRunner::with_upstream(refreshable_record(upstream_id, 5));
    let handler = MetadataRefreshJobHandler::new(runner.clone());

    // When: the metadata refresh job is handled.
    let outcome = handler
        .handle(MetadataRefreshJob::new(upstream_id, 4))
        .await?;

    // Then: the stale follow-up is skipped and metadata fetch is not run.
    assert_eq!(outcome, MetadataRefreshJobOutcome::Stale);
    assert_eq!(runner.refresh_calls(), 0);
    Ok(())
}

#[tokio::test]
async fn t2__applies_metadata_refresh_when_generation_is_current() -> Result<()> {
    // Given: a job whose credential generation matches storage.
    let upstream_id = Uuid::from_u128(2);
    let runner = FakeMetadataRefreshRunner::with_upstream(refreshable_record(upstream_id, 5));
    let handler = MetadataRefreshJobHandler::new(runner.clone());

    // When: the metadata refresh job is handled.
    let outcome = handler
        .handle(MetadataRefreshJob::new(upstream_id, 5))
        .await?;

    // Then: the fresh generation is applied exactly once.
    assert_eq!(outcome, MetadataRefreshJobOutcome::Applied);
    assert_eq!(runner.refresh_calls(), 1);
    Ok(())
}

#[tokio::test]
async fn t2__skips_when_upstream_was_removed_mid_flight() -> Result<()> {
    // Given: the queued job references an upstream no longer present in storage.
    let upstream_id = Uuid::from_u128(3);
    let runner = FakeMetadataRefreshRunner::without_upstream();
    let handler = MetadataRefreshJobHandler::new(runner.clone());

    // When: the metadata refresh job is handled after removal.
    let outcome = handler
        .handle(MetadataRefreshJob::new(upstream_id, 5))
        .await?;

    // Then: no metadata refresh is attempted.
    assert_eq!(outcome, MetadataRefreshJobOutcome::UpstreamRemoved);
    assert_eq!(runner.refresh_calls(), 0);
    Ok(())
}

#[derive(Clone)]
struct FakeMetadataRefreshRunner {
    state: Arc<Mutex<FakeMetadataRefreshState>>,
}

struct FakeMetadataRefreshState {
    upstream: Option<UpstreamRecord>,
    refresh_calls: usize,
}

impl FakeMetadataRefreshRunner {
    fn with_upstream(upstream: UpstreamRecord) -> Self {
        Self {
            state: Arc::new(Mutex::new(FakeMetadataRefreshState {
                upstream: Some(upstream),
                refresh_calls: 0,
            })),
        }
    }

    fn without_upstream() -> Self {
        Self {
            state: Arc::new(Mutex::new(FakeMetadataRefreshState {
                upstream: None,
                refresh_calls: 0,
            })),
        }
    }

    fn refresh_calls(&self) -> usize {
        self.state
            .lock()
            .expect("metadata runner lock")
            .refresh_calls
    }
}

impl MetadataRefreshRunner for FakeMetadataRefreshRunner {
    async fn load_upstream(&self, _upstream_id: Uuid) -> Result<MaybeUpstreamRecord> {
        Ok(self
            .state
            .lock()
            .expect("metadata runner lock")
            .upstream
            .clone())
    }

    async fn run_metadata_refresh(&self, _upstream: &UpstreamRecord) -> Result<()> {
        self.state
            .lock()
            .expect("metadata runner lock")
            .refresh_calls += 1;
        Ok(())
    }
}

fn refreshable_record(upstream_id: Uuid, generation: u64) -> UpstreamRecord {
    UpstreamRecord {
        id: upstream_id,
        name: "oauth-upstream".to_owned(),
        kind: UpstreamKind::AnthropicOauth,
        enabled: true,
        oauth_credentials: Some(EncryptedOAuthTokens::from_ciphertext(vec![1])),
        revision: 1,
        oauth_token_generation: generation,
        created_at_unix_secs: 1,
        updated_at_unix_secs: 1,
        ..UpstreamRecord::default()
    }
}
