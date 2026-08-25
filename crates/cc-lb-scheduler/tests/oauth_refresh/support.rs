#![allow(dead_code)]

use std::sync::{Arc, Mutex};

use cc_lb_aead::EncryptedOAuthTokens;
use cc_lb_scheduler::error::{Result, SchedulerError};
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
    lease_holder: Option<Uuid>,
    claim_calls: usize,
    complete_calls: usize,
    fail_calls: usize,
    failure_reason: Option<String>,
    failure_generation: Option<u64>,
    replacement_generation_on_claim: Option<u64>,
}

impl FakeUpstreams {
    pub fn new(record: UpstreamRecord, completed_generation: u64) -> Self {
        Self {
            state: Arc::new(Mutex::new(FakeUpstreamsState {
                record,
                completed_generation,
                lease_holder: None,
                claim_calls: 0,
                complete_calls: 0,
                fail_calls: 0,
                failure_reason: None,
                failure_generation: None,
                replacement_generation_on_claim: None,
            })),
        }
    }

    pub fn with_busy_lease(self) -> Self {
        self.state.lock().expect("upstreams lock").lease_holder = Some(Uuid::new_v4());
        self
    }

    pub fn with_replacement_after_claim(self, generation: u64) -> Self {
        self.state
            .lock()
            .expect("upstreams lock")
            .replacement_generation_on_claim = Some(generation);
        self
    }

    pub fn complete_calls(&self) -> usize {
        self.state.lock().expect("upstreams lock").complete_calls
    }

    pub fn claim_calls(&self) -> usize {
        self.state.lock().expect("upstreams lock").claim_calls
    }

    pub fn fail_calls(&self) -> usize {
        self.state.lock().expect("upstreams lock").fail_calls
    }

    pub fn failure_reason(&self) -> Option<String> {
        self.state
            .lock()
            .expect("upstreams lock")
            .failure_reason
            .clone()
    }

    pub fn replace_credentials(&self, generation: u64) {
        let mut state = self.state.lock().expect("upstreams lock");
        state.record.oauth_token_generation = generation;
        state.lease_holder = None;
        state.failure_generation = None;
        state.failure_reason = None;
    }
}

impl OAuthRefreshUpstreams for FakeUpstreams {
    async fn get_by_id(&self, _id: Uuid) -> Result<Option<UpstreamRecord>> {
        Ok(Some(
            self.state.lock().expect("upstreams lock").record.clone(),
        ))
    }

    async fn claim_refresh_lease(
        &self,
        _id: Uuid,
        holder: Uuid,
        expected_generation: u64,
        _ttl_secs: u64,
    ) -> Result<bool> {
        let mut state = self.state.lock().expect("upstreams lock");
        state.claim_calls += 1;
        if state.lease_holder.is_some()
            || state.record.oauth_token_generation != expected_generation
            || state.failure_generation == Some(expected_generation)
        {
            return Ok(false);
        }
        state.lease_holder = Some(holder);
        if let Some(generation) = state.replacement_generation_on_claim.take() {
            state.record.oauth_token_generation = generation;
            state.lease_holder = None;
        }
        Ok(true)
    }

    async fn read_oauth_refresh_terminal_failure(
        &self,
        _id: Uuid,
    ) -> Result<Option<cc_lb_storage_api::OAuthRefreshTerminalFailure>> {
        let state = self.state.lock().expect("upstreams lock");
        Ok(state
            .failure_generation
            .zip(state.failure_reason.clone())
            .map(
                |(expected_generation, code)| cc_lb_storage_api::OAuthRefreshTerminalFailure {
                    upstream_id: state.record.id,
                    expected_generation,
                    code,
                },
            ))
    }

    async fn fail_refresh(
        &self,
        _id: Uuid,
        holder: Uuid,
        terminal_error: Option<String>,
    ) -> Result<bool> {
        let mut state = self.state.lock().expect("upstreams lock");
        state.fail_calls += 1;
        if state.lease_holder != Some(holder) {
            return Ok(false);
        }
        state.lease_holder = None;
        state.failure_generation = terminal_error
            .as_ref()
            .map(|_| state.record.oauth_token_generation);
        state.failure_reason = terminal_error;
        Ok(true)
    }

    async fn complete_refresh(
        &self,
        _id: Uuid,
        holder: Uuid,
        _tokens: EncryptedOAuthTokens,
    ) -> Result<UpstreamRecord> {
        let mut state = self.state.lock().expect("upstreams lock");
        if state.lease_holder != Some(holder) {
            return Err(SchedulerError::Conflict(
                "oauth refresh lease fence failed".to_owned(),
            ));
        }
        state.lease_holder = None;
        state.complete_calls += 1;
        state.record.oauth_token_generation = state.completed_generation;
        state.failure_generation = None;
        state.failure_reason = None;
        Ok(state.record.clone())
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
