use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use cc_lb_aead::AeadService;
use cc_lb_plugin_api::{
    Principal, PrincipalKind, RequestContext, ShapedRequest, ShapedRequestBuilder, SignerFactory,
    Upstream, UpstreamDialect, shape_request, sign_request,
};
use cc_lb_signer_anthropic_key::AnthropicKeySignerFactory;
use cc_lb_storage_api::{
    AnthropicApiKeyCredential, ApiKeyStore, AuditEntry, AuditStore, BackendKind, ConfigDraftState,
    ConfigStore, HistoryEntry, HistorySummary, LimitStateStore, MetaStore, OAuthCredentialStore,
    PrincipalLimitIdentityKind, PrincipalLimitKind, PrincipalLimitState, QuotaStore, RequestEvent,
    RequestEventStore, StorageError, StorageResult, UsageRollup, UsageRollupResolution,
    UsageRollupRun, UsageRollupStore,
};
use http::header::{AUTHORIZATION, USER_AGENT};
use http::{HeaderMap, HeaderValue, Method};

#[derive(Default)]
struct MemoryStorage {
    oauth: Mutex<HashMap<(String, String), Vec<u8>>>,
    anthropic_api_keys: Mutex<HashMap<String, Vec<u8>>>,
}

struct DirectDialect;

impl UpstreamDialect for DirectDialect {
    fn shape(
        &self,
        _ctx: &RequestContext,
        _upstream: &Upstream,
        _principal: &Principal,
        builder: &mut ShapedRequestBuilder,
    ) -> Result<ShapedRequest, cc_lb_plugin_api::DialectError> {
        let mut headers = HeaderMap::new();
        headers.insert(USER_AGENT, HeaderValue::from_static("test-agent"));
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_static("Bearer ck-internal-alice-X"),
        );
        Ok(builder.shaped_request(
            "https://api.anthropic.com/v1/messages"
                .parse()
                .expect("url"),
            Method::POST,
            headers,
            Bytes::from_static(br#"{"model":"c"}"#),
        ))
    }

    fn normalize_error(&self, _status: http::StatusCode, _body: &Bytes) -> Option<Bytes> {
        None
    }
}

#[tokio::test]
async fn sign_loads_api_key_from_storage_key() {
    let storage = Arc::new(MemoryStorage::default());
    let signer_storage: Arc<dyn OAuthCredentialStore> = storage.clone();
    let aead = Arc::new(AeadService::from_master_key([0x24; 32]));
    let storage_key = "alice:real_anthropic_api_key";
    let credential = AnthropicApiKeyCredential {
        anthropic_api_key: "sk-ant-real-aaaa".to_owned(),
    };
    let plaintext = serde_json::to_vec(&credential).expect("serialize real key");
    let ciphertext = aead
        .encrypt(&plaintext, &anthropic_api_key_aad(storage_key))
        .expect("encrypt real key");
    storage
        .put_anthropic_api_key_ciphertext(storage_key, &ciphertext)
        .await
        .expect("seed real key");

    let ctx = RequestContext {
        request_id: "req-internal-key-storage-test".to_owned(),
        downstream_headers: HeaderMap::new(),
        method: Method::POST,
        path: "/v1/messages".to_owned(),
        query: None,
        body_bytes: Bytes::from_static(b"{}"),
    };
    let principal = Principal {
        id: "alice".to_owned(),
        kind: PrincipalKind::InternalKey,
        claims: serde_json::Map::new(),
    };
    let shaped = shape_request(&DirectDialect, &ctx, &Upstream::AnthropicDirect, &principal)
        .expect("shape request");
    let signer =
        AnthropicKeySignerFactory::from_storage(storage_key.to_owned(), signer_storage, aead)
            .build(&Upstream::AnthropicDirect)
            .await
            .expect("signer");
    let signed = sign_request(signer.as_ref(), shaped)
        .await
        .expect("signed request");

    assert_eq!(
        signed
            .headers()
            .get("x-api-key")
            .and_then(|value| value.to_str().ok()),
        Some("sk-ant-real-aaaa")
    );
    assert_eq!(
        signed
            .headers()
            .get(USER_AGENT)
            .and_then(|value| value.to_str().ok()),
        Some("test-agent")
    );
    assert!(signed.headers().get(AUTHORIZATION).is_none());
}

fn anthropic_api_key_aad(storage_key: &str) -> Vec<u8> {
    format!("anthropic-api-key:{storage_key}").into_bytes()
}

fn unsupported<T>() -> StorageResult<T> {
    Err(StorageError::Fatal {
        message: "unsupported test storage method".to_owned(),
    })
}

#[async_trait]
impl OAuthCredentialStore for MemoryStorage {
    async fn put_oauth_ciphertext(
        &self,
        principal_id: &str,
        provider: &str,
        ciphertext: &[u8],
    ) -> StorageResult<()> {
        self.oauth.lock().expect("oauth storage lock").insert(
            (principal_id.to_owned(), provider.to_owned()),
            ciphertext.to_vec(),
        );
        Ok(())
    }

    async fn get_oauth_ciphertext(
        &self,
        principal_id: &str,
        provider: &str,
    ) -> StorageResult<Option<Vec<u8>>> {
        Ok(self
            .oauth
            .lock()
            .expect("oauth storage lock")
            .get(&(principal_id.to_owned(), provider.to_owned()))
            .cloned())
    }

    async fn delete_oauth(&self, principal_id: &str, provider: &str) -> StorageResult<bool> {
        Ok(self
            .oauth
            .lock()
            .expect("oauth storage lock")
            .remove(&(principal_id.to_owned(), provider.to_owned()))
            .is_some())
    }

    async fn put_anthropic_api_key_ciphertext(
        &self,
        storage_key: &str,
        ciphertext: &[u8],
    ) -> StorageResult<()> {
        self.anthropic_api_keys
            .lock()
            .expect("anthropic api key storage lock")
            .insert(storage_key.to_owned(), ciphertext.to_vec());
        Ok(())
    }

    async fn get_anthropic_api_key_ciphertext(
        &self,
        storage_key: &str,
    ) -> StorageResult<Option<Vec<u8>>> {
        Ok(self
            .anthropic_api_keys
            .lock()
            .expect("anthropic api key storage lock")
            .get(storage_key)
            .cloned())
    }
}

#[async_trait]
impl AuditStore for MemoryStorage {
    async fn append_audit(&self, _entry: &AuditEntry) -> StorageResult<()> {
        unsupported()
    }

    async fn query_audit(
        &self,
        _principal_id: Option<&str>,
        _since: u64,
        _until: u64,
        _limit: usize,
    ) -> StorageResult<Vec<AuditEntry>> {
        unsupported()
    }

    async fn prune_audit(&self, _older_than: u64) -> StorageResult<u64> {
        unsupported()
    }
}

#[async_trait]
impl RequestEventStore for MemoryStorage {
    async fn append_request_event(&self, _event: &RequestEvent) -> StorageResult<()> {
        unsupported()
    }

    async fn query_request_events(
        &self,
        _since: u64,
        _until: u64,
        _limit: usize,
    ) -> StorageResult<Vec<RequestEvent>> {
        unsupported()
    }
}

#[async_trait]
impl QuotaStore for MemoryStorage {
    async fn incr_quota(
        &self,
        _principal_id: &str,
        _window_start: u64,
        _kind: cc_lb_storage_api::BucketKind,
        _amount: u64,
    ) -> StorageResult<u64> {
        unsupported()
    }

    async fn try_incr_quota(
        &self,
        _principal_id: &str,
        _window_start: u64,
        _kind: cc_lb_storage_api::BucketKind,
        _amount: u64,
        _capacity: u64,
    ) -> StorageResult<Option<u64>> {
        unsupported()
    }

    async fn get_quota(
        &self,
        _principal_id: &str,
        _window_start: u64,
        _kind: cc_lb_storage_api::BucketKind,
    ) -> StorageResult<u64> {
        unsupported()
    }

    async fn adjust_quota(
        &self,
        _principal_id: &str,
        _window_start: u64,
        _kind: cc_lb_storage_api::BucketKind,
        _delta: i64,
    ) -> StorageResult<u64> {
        unsupported()
    }

    async fn sweep_old_quotas(&self, _older_than_window_start: u64) -> StorageResult<u64> {
        unsupported()
    }
}

#[async_trait]
impl LimitStateStore for MemoryStorage {
    async fn put_principal_limit_state(&self, _state: &PrincipalLimitState) -> StorageResult<()> {
        unsupported()
    }

    async fn get_principal_limit_state(
        &self,
        _principal_id: &str,
        _identity_kind: PrincipalLimitIdentityKind,
        _identity_value: Option<&str>,
        _window: &str,
        _kind: PrincipalLimitKind,
    ) -> StorageResult<Option<PrincipalLimitState>> {
        unsupported()
    }

    async fn list_principal_limit_states(
        &self,
        _principal_id: &str,
    ) -> StorageResult<Vec<PrincipalLimitState>> {
        unsupported()
    }
}

#[async_trait]
impl UsageRollupStore for MemoryStorage {
    async fn rollup_usage_once(&self) -> StorageResult<UsageRollupRun> {
        unsupported()
    }

    async fn query_usage_rollups(&self) -> StorageResult<Vec<UsageRollup>> {
        unsupported()
    }

    async fn query_usage_rollups_in_range(
        &self,
        _resolution: UsageRollupResolution,
        _window_start_unix_secs: u64,
        _window_end_unix_secs: u64,
    ) -> StorageResult<Vec<UsageRollup>> {
        unsupported()
    }

    async fn usage_rollup_checkpoint(&self) -> StorageResult<Option<u64>> {
        unsupported()
    }

    async fn advance_rollup_checkpoint_and_persist(
        &self,
        _run: &UsageRollupRun,
    ) -> StorageResult<()> {
        unsupported()
    }
}

#[async_trait]
impl ApiKeyStore for MemoryStorage {
    async fn put_api_key_ciphertext(
        &self,
        _principal_id: &str,
        _key_id: &str,
        _ciphertext: &[u8],
    ) -> StorageResult<()> {
        unsupported()
    }

    async fn get_api_key_ciphertext(
        &self,
        _principal_id: &str,
        _key_id: &str,
    ) -> StorageResult<Option<Vec<u8>>> {
        unsupported()
    }

    async fn list_api_key_ciphertexts(
        &self,
        _principal_id: &str,
    ) -> StorageResult<Vec<(String, Vec<u8>)>> {
        unsupported()
    }

    async fn revoke_api_key(
        &self,
        _principal_id: &str,
        _key_id: &str,
        _revoked_ciphertext: &[u8],
    ) -> StorageResult<bool> {
        unsupported()
    }
}

#[async_trait]
impl ConfigStore for MemoryStorage {
    async fn get_config_draft(&self) -> StorageResult<ConfigDraftState> {
        unsupported()
    }

    async fn put_config_draft(
        &self,
        _new: ConfigDraftState,
        _expected_revision: u64,
    ) -> StorageResult<u64> {
        unsupported()
    }

    async fn set_last_validated_revision(
        &self,
        _revision: u64,
        _error: Option<String>,
    ) -> StorageResult<()> {
        unsupported()
    }

    async fn append_config_history(
        &self,
        _revision: u64,
        _config_toml: String,
        _applied_at_unix_secs: u64,
        _summary: HistorySummary,
    ) -> StorageResult<()> {
        unsupported()
    }

    async fn list_config_history(&self, _limit: usize) -> StorageResult<Vec<HistoryEntry>> {
        unsupported()
    }

    async fn get_config_history(&self, _revision: u64) -> StorageResult<Option<HistoryEntry>> {
        unsupported()
    }
}

#[async_trait]
impl MetaStore for MemoryStorage {
    async fn initialize(&self, _requested: BackendKind) -> StorageResult<()> {
        unsupported()
    }

    async fn contract_version(&self) -> StorageResult<u32> {
        unsupported()
    }

    async fn backend_kind(&self) -> StorageResult<BackendKind> {
        unsupported()
    }

    async fn killswitch_enabled(&self) -> StorageResult<bool> {
        unsupported()
    }

    async fn set_killswitch_enabled(&self, _enabled: bool) -> StorageResult<()> {
        unsupported()
    }
}
