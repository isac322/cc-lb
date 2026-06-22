#![allow(deprecated, dead_code)]

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use cc_lb_aead::EncryptedOAuthTokens;
use cc_lb_scheduler::error::Result;
use cc_lb_scheduler::jobs::metadata_refresh::MetadataRefreshJob;
use cc_lb_scheduler::jobs::oauth_refresh::OAuthRefreshUpstreams;
use cc_lb_storage_api::upstream::{UpstreamKind, UpstreamRecord};
use uuid::Uuid;

#[derive(Clone)]
pub struct FakeUpstreams {
    state: Arc<Mutex<FakeUpstreamsState>>,
}

#[derive(Debug)]
struct FakeUpstreamsState {
    record: UpstreamRecord,
    completed_generation: u64,
    complete_calls: usize,
    read_generations: VecDeque<Option<u64>>,
}

impl FakeUpstreams {
    pub fn new(record: UpstreamRecord, completed_generation: u64) -> Self {
        Self {
            state: Arc::new(Mutex::new(FakeUpstreamsState {
                record,
                completed_generation,
                complete_calls: 0,
                read_generations: VecDeque::new(),
            })),
        }
    }

    pub fn with_read_generations<const N: usize>(self, generations: [Option<u64>; N]) -> Self {
        self.state.lock().expect("upstreams lock").read_generations = generations.into();
        self
    }

    pub fn complete_calls(&self) -> usize {
        self.state.lock().expect("upstreams lock").complete_calls
    }
}

impl OAuthRefreshUpstreams for FakeUpstreams {
    async fn get_by_id(&self, _id: Uuid) -> Result<Option<UpstreamRecord>> {
        Ok(Some(
            self.state.lock().expect("upstreams lock").record.clone(),
        ))
    }

    async fn complete_refresh(
        &self,
        _id: Uuid,
        _holder: Uuid,
        _tokens: EncryptedOAuthTokens,
    ) -> Result<UpstreamRecord> {
        let mut state = self.state.lock().expect("upstreams lock");
        state.complete_calls += 1;
        state.record.oauth_token_generation = state.completed_generation;
        Ok(state.record.clone())
    }

    async fn read_oauth_token_generation(&self, _id: Uuid) -> Result<Option<u64>> {
        let mut state = self.state.lock().expect("upstreams lock");
        Ok(state
            .read_generations
            .pop_front()
            .unwrap_or(Some(state.record.oauth_token_generation)))
    }
}

pub fn refreshable_record(upstream_id: Uuid, generation: u64) -> UpstreamRecord {
    UpstreamRecord {
        id: upstream_id,
        name: "oauth-upstream".to_owned(),
        kind: UpstreamKind::AnthropicOauth,
        base_url: None,
        enabled: true,
        oauth_credentials: Some(encrypted_tokens(1)),
        api_key_ciphertext: None,
        last_apply_error: None,
        last_apply_at_unix_secs: None,
        deleted_at_unix_secs: None,
        revision: 1,
        oauth_token_generation: generation,
        created_at_unix_secs: 1,
        updated_at_unix_secs: 1,
        warmup_enabled: false,
        warmup_dialect_plugin: None,
        last_warmup_at_unix_secs: None,
    }
}

pub fn encrypted_tokens(byte: u8) -> EncryptedOAuthTokens {
    EncryptedOAuthTokens::from_ciphertext(vec![byte])
}

pub fn capture_metadata(
    enqueued: Arc<Mutex<Option<MetadataRefreshJob>>>,
) -> impl FnOnce(MetadataRefreshJob) -> std::future::Ready<Result<()>> {
    move |job| {
        *enqueued.lock().expect("enqueued lock") = Some(job);
        std::future::ready(Ok(()))
    }
}

pub fn count_refresh_calls(
    calls: Arc<Mutex<usize>>,
) -> impl FnOnce(UpstreamRecord) -> std::future::Ready<Result<EncryptedOAuthTokens>> {
    move |_| {
        *calls.lock().expect("refresh lock") += 1;
        std::future::ready(Ok(encrypted_tokens(2)))
    }
}
