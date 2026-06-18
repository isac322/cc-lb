use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use cc_lb_aead::EncryptedOAuthTokens;
use cc_lb_scheduler::error::Result;
use cc_lb_scheduler::jobs::metadata_refresh::MetadataRefreshJob;
use cc_lb_scheduler::jobs::oauth_refresh::{OAuthRefreshClaims, OAuthRefreshUpstreams};
use cc_lb_storage_api::upstream::{UpstreamKind, UpstreamRecord};
use uuid::Uuid;

#[derive(Clone)]
pub struct FakeClaims {
    state: Arc<Mutex<FakeClaimsState>>,
}

#[derive(Debug)]
struct FakeClaimsState {
    acquire: bool,
    releases: usize,
    completed_generation: Option<u64>,
}

impl FakeClaims {
    pub fn new(acquire: bool) -> Self {
        Self {
            state: Arc::new(Mutex::new(FakeClaimsState {
                acquire,
                releases: 0,
                completed_generation: None,
            })),
        }
    }

    pub fn release_count(&self) -> usize {
        self.state.lock().expect("claims lock").releases
    }

    pub fn completed_generation(&self) -> Option<u64> {
        self.state.lock().expect("claims lock").completed_generation
    }
}

impl OAuthRefreshClaims for FakeClaims {
    async fn try_acquire(
        &self,
        _upstream_id: Uuid,
        _holder: &str,
        _ttl_secs: u64,
        _now_unix_secs: u64,
    ) -> Result<bool> {
        Ok(self.state.lock().expect("claims lock").acquire)
    }

    async fn complete_and_bump_generation(
        &self,
        _upstream_id: Uuid,
        _holder: &str,
        new_generation: u64,
    ) -> Result<bool> {
        self.state.lock().expect("claims lock").completed_generation = Some(new_generation);
        Ok(true)
    }

    async fn release_if_holder(&self, _upstream_id: Uuid, _holder: &str) -> Result<bool> {
        self.state.lock().expect("claims lock").releases += 1;
        Ok(true)
    }
}

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
        refresh_lease_holder: None,
        refresh_lease_until_unix_secs: None,
        last_apply_error: None,
        last_apply_at_unix_secs: None,
        deleted_at_unix_secs: None,
        revision: 1,
        oauth_token_generation: generation,
        created_at_unix_secs: 1,
        updated_at_unix_secs: 1,
        warmup_enabled: false,
        next_warmup_at: None,
        last_warmup_cycle_key: None,
        warmup_lease_holder: None,
        warmup_lease_until_unix_secs: None,
        warmup_dialect_plugin: None,
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
