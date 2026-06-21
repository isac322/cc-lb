# BDD Persona Helper API Specification

This document defines the concrete Rust API specification for the Alice, Bob, Charlie, and Dana persona clients used to execute the 321 BDD scenarios.

## §1 Overview

The BDD test suite uses four distinct personas to verify the behavior of the cc-lb reverse proxy. Each persona represents a specific role with clear boundaries and privileges.

1. **Alice (Operator)**: Manages the lifecycle of teams, client keys, and global configurations. She accesses the system using an admin token.
2. **Bob (Developer)**: Represents the client application or plugin author. He sends messages to the proxy, uploads plugins, and views his own usage metrics. Authentication uses a per-principal API key.
3. **Charlie (SRE)**: Handles system reliability, incident response, and multi-replica operations. He uses an admin token with SRE scope.
4. **Dana (Auditor)**: Verifies compliance and security. She has read-only access to audit logs and cryptographic signers. Access requires a read-only token.

The `BddCtx` struct serves as the bootstrap entry point. It instantiates and configures the appropriate client for each persona.

## §2 Role Boundaries Table

The following table outlines the roles, authentication mechanisms, capabilities, and constraints for each persona.

| Persona | Role | Auth | Can do | Cannot do | Used in scenarios |
|---|---|---|---|---|---|
| Alice | Operator | admin token | principal CRUD, key issue, view metrics, drain | secret regen requires Charlie privilege | 121 |
| Bob | Developer / plugin author | per-principal API key | call /v1/messages, install plugin, view own usage | cannot create principal, cannot view other principals | 52 |
| Charlie | SRE | admin token + SRE scope | incident response, drain, multi-replica ops, warmup leases, backend parity checks | no audit redaction bypass | 108 |
| Dana | Auditor | read-only token | audit log query, redaction verification, list signers | any mutation (write returns 403) | 40 |

## §3 Rust API

The following signatures define the persona clients and their bootstrap methods on `BddCtx`.

```rust
impl BddCtx {
    pub async fn alice(&self) -> OperatorClient;
    pub async fn bob(&self) -> DeveloperClient;
    pub async fn charlie(&self) -> SreClient;
    pub async fn dana(&self) -> AuditorClient;
}

pub struct OperatorClient { pub token: String, pub endpoint: String }
impl OperatorClient {
    pub async fn create_principal(&self, name: &str) -> Result<PrincipalCreateResult, BddClientError>; // F1.1a, F1.1b
    pub async fn list_principals(&self) -> Result<Vec<PrincipalSummary>, BddClientError>; // F2.5
    pub async fn issue_key(&self, principal: &str) -> Result<KeyIssueResult, BddClientError>; // F2.1
    pub async fn revoke_key(&self, key_id: uuid::Uuid) -> Result<RevokeResult, BddClientError>; // F2.3
    pub async fn drain_node(&self, node_id: &str) -> Result<DrainResult, BddClientError>; // F17.3
    pub async fn view_metrics(&self) -> Result<MetricsSnapshot, BddClientError>; // F4.1a, F4.1b
    pub async fn update_principal_quota(&self, name: &str, quota: QuotaConfig, version: u32) -> Result<PrincipalUpdateResult, BddClientError>; // F1.3
    pub async fn set_principal_active(&self, name: &str, active: bool) -> Result<PrincipalUpdateResult, BddClientError>; // F1.4
    pub async fn delete_principal(&self, name: &str) -> Result<DeleteResult, BddClientError>; // F1.7
    pub async fn rotate_key(&self, key_id: uuid::Uuid) -> Result<KeyRotateResult, BddClientError>; // F2.2
    pub async fn update_key_metadata(&self, key_id: uuid::Uuid, holder: &str, memo: &str) -> Result<KeyUpdateResult, BddClientError>; // F2.6
    pub async fn update_key_status(&self, key_id: uuid::Uuid, status: KeyStatus) -> Result<KeyUpdateResult, BddClientError>; // F2.13a
    pub async fn update_principal_limits(&self, name: &str, limits: QuotaConfig) -> Result<PrincipalUpdateResult, BddClientError>; // F6.5
    pub async fn set_global_killswitch(&self, active: bool) -> Result<KillswitchResult, BddClientError>; // F7.1
    pub async fn attach_policy(&self, principal: &str, policy: PolicyConfig) -> Result<PolicyAttachResult, BddClientError>; // F9.1
    pub async fn detach_policy(&self, principal: &str, policy_id: &str) -> Result<PolicyDetachResult, BddClientError>; // F9.2
    pub async fn save_draft_config(&self, config: DraftConfig) -> Result<DraftSaveResult, BddClientError>; // F14.1
    pub async fn validate_draft_config(&self) -> Result<DraftValidateResult, BddClientError>; // F14.2
    pub async fn apply_draft_config(&self) -> Result<ConfigApplyResult, BddClientError>; // F14.4
    pub async fn view_config_history(&self) -> Result<Vec<ConfigHistoryEntry>, BddClientError>; // F14.6
    pub async fn rollback_config(&self, version: u32) -> Result<ConfigApplyResult, BddClientError>; // F14.7
    pub async fn fetch_external_pricing(&self) -> Result<PricingFetchResult, BddClientError>; // F24.1
}

pub struct DeveloperClient { pub api_key: String, pub endpoint: String }
impl DeveloperClient {
    pub async fn send_message(&self, body: MessageRequest) -> Result<MessageResponse, BddClientError>; // F3.1
    pub async fn send_message_stream(&self, body: MessageRequest) -> Result<MessageStream, BddClientError>; // F3.2
    pub async fn install_plugin(&self, manifest: PluginManifest) -> Result<InstallResult, BddClientError>; // F12.1
    pub async fn view_own_usage(&self) -> Result<UsageSummary, BddClientError>; // F3.5c
    pub async fn upload_file(&self, name: &str, content: Vec<u8>) -> Result<FileUploadResult, BddClientError>; // F3.9
    pub async fn download_file(&self, file_id: &str) -> Result<Vec<u8>, BddClientError>; // F3.9
    pub async fn delete_file(&self, file_id: &str) -> Result<FileDeleteResult, BddClientError>; // F3.9
}

pub struct SreClient { pub token: String, pub endpoint: String }
impl SreClient {
    pub async fn warmup_lease(&self, principal: &str) -> Result<WarmupLease, BddClientError>; // F11A.9
    pub async fn replica_status(&self) -> Result<Vec<ReplicaState>, BddClientError>; // F17.1
    pub async fn parity_check(&self, key: &str) -> Result<ParityReport, BddClientError>; // F24.7
    pub async fn trigger_warmup(&self) -> Result<WarmupResult, BddClientError>; // F11B.4
    pub async fn view_warmup_metrics(&self) -> Result<WarmupMetrics, BddClientError>; // F11C.6
}

pub struct AuditorClient { pub token: String, pub endpoint: String }
impl AuditorClient {
    pub async fn query_audit(&self, filter: AuditFilter) -> Result<Vec<AuditEntry>, BddClientError>; // F13.1
    pub async fn assert_redacted(&self, entry: &AuditEntry, field: &str); // F20.1
    pub async fn list_signers(&self) -> Result<Vec<SignerInfo>, BddClientError>; // F24.7
}
```

## §4 Result Types

The following Rust structures define the data models returned by the persona clients.

```rust
pub enum BddClientError {
    HttpError { status: u16, message: String },
    NetworkError(String),
    SerializationError(String),
    AssertionFailed(String),
}

pub struct PrincipalCreateResult {
    pub principal_id: String,
    pub name: String,
    pub is_active: bool,
    pub first_key: Option<String>,
    pub created_at: u64,
}

pub struct PrincipalSummary {
    pub principal_id: String,
    pub name: String,
    pub is_active: bool,
    pub key_count: u32,
}

pub struct KeyIssueResult {
    pub key_id: uuid::Uuid,
    pub principal_id: String,
    pub secret_preview: String,
    pub secret_full: Option<String>,
    pub holder: String,
    pub is_machine: bool,
    pub expires_at: Option<u64>,
}

pub struct RevokeResult {
    pub key_id: uuid::Uuid,
    pub revoked_at: u64,
}

pub struct DrainResult {
    pub node_id: String,
    pub active_connections: u32,
    pub is_drained: bool,
}

pub struct MetricsSnapshot {
    pub total_calls: u64,
    pub successful_calls: u64,
    pub rejected_calls: u64,
    pub total_cost_usd: f64,
    pub cache_hit_rate: f64,
    pub active_connections: u32,
}

pub struct PrincipalUpdateResult {
    pub principal_id: String,
    pub version: u32,
    pub updated_at: u64,
}

pub struct DeleteResult {
    pub principal_id: String,
    pub deleted_at: u64,
}

pub struct KeyRotateResult {
    pub old_key_id: uuid::Uuid,
    pub new_key_id: uuid::Uuid,
    pub new_secret_full: String,
    pub overlap_ends_at: u64,
}

pub struct KeyUpdateResult {
    pub key_id: uuid::Uuid,
    pub updated_at: u64,
}

pub struct KillswitchResult {
    pub is_active: bool,
    pub updated_at: u64,
}

pub struct PolicyAttachResult {
    pub policy_id: String,
    pub principal_id: String,
    pub attached_at: u64,
}

pub struct PolicyDetachResult {
    pub policy_id: String,
    pub principal_id: String,
    pub detached_at: u64,
}

pub struct DraftSaveResult {
    pub draft_id: String,
    pub saved_at: u64,
}

pub struct DraftValidateResult {
    pub is_valid: bool,
    pub errors: Vec<String>,
}

pub struct ConfigApplyResult {
    pub version: u32,
    pub applied_at: u64,
}

pub struct ConfigHistoryEntry {
    pub version: u32,
    pub applied_at: u64,
    pub actor: String,
    pub description: String,
}

pub struct PricingFetchResult {
    pub source_url: String,
    pub fetched_at: u64,
    pub model_count: u32,
}

pub struct MessageRequest {
    pub model: String,
    pub messages: Vec<MessageItem>,
    pub stream: bool,
}

pub struct MessageItem {
    pub role: String,
    pub content: String,
}

pub struct MessageResponse {
    pub id: String,
    pub model: String,
    pub content: String,
    pub usage: UsageMetrics,
}

pub struct UsageMetrics {
    pub input_tokens: u32,
    pub output_tokens: u32,
}

pub struct MessageStream {
    pub stream_id: String,
}

pub struct PluginManifest {
    pub name: String,
    pub version: String,
    pub wasm_hash: String,
    pub allowed_hosts: Vec<String>,
}

pub struct InstallResult {
    pub plugin_id: String,
    pub installed_at: u64,
}

pub struct UsageSummary {
    pub principal_id: String,
    pub total_calls: u64,
    pub total_tokens: u64,
    pub total_cost_usd: f64,
}

pub struct FileUploadResult {
    pub file_id: String,
    pub uploaded_at: u64,
}

pub struct FileDeleteResult {
    pub file_id: String,
    pub deleted_at: u64,
}

pub struct WarmupLease {
    pub lease_id: String,
    pub principal_id: String,
    pub acquired_at: u64,
    pub expires_at: u64,
}

pub struct ReplicaState {
    pub node_id: String,
    pub status: String,
    pub is_leader: bool,
    pub last_heartbeat: u64,
}

pub struct ParityReport {
    pub is_consistent: bool,
    pub sqlite_hash: String,
    pub postgres_hash: String,
    pub checked_at: u64,
}

pub struct WarmupResult {
    pub success_count: u32,
    pub failure_count: u32,
    pub duration_ms: u64,
}

pub struct WarmupMetrics {
    pub total_warmups: u64,
    pub last_warmup_at: u64,
    pub average_latency_ms: u32,
}

pub struct AuditFilter {
    pub principal_id: Option<String>,
    pub actor: Option<String>,
    pub start_time: Option<u64>,
    pub end_time: Option<u64>,
    pub limit: u32,
}

pub struct AuditEntry {
    pub entry_id: uuid::Uuid,
    pub timestamp: u64,
    pub actor: String,
    pub action: String,
    pub principal_id: Option<String>,
    pub details: String,
}

pub struct SignerInfo {
    pub signer_id: String,
    pub public_key: String,
    pub algorithm: String,
}

pub struct QuotaConfig {
    pub max_calls_per_minute: u32,
    pub max_cost_per_month: f64,
}

pub struct PolicyConfig {
    pub policy_id: String,
    pub rules: Vec<String>,
}

pub struct DraftConfig {
    pub config_json: String,
}

pub enum KeyStatus {
    Active,
    Suspended,
    Revoked,
}
```

## §5 Bootstrap

The `BddCtx` struct manages the initialization and authentication of each client.

1. **Alice (Operator)**: The client reads the admin bootstrap token from the environment variable `CC_LB_BOOTSTRAP_ADMIN_TOKEN`. It configures the client with this token to access the admin endpoints.
2. **Bob (Developer)**: The context generates a unique API key for Bob. It registers this key in the database under a test principal. The client uses this key for message dispatch and plugin operations.
3. **Dana (Auditor)**: The context requests a read-only token by calling the admin endpoint `POST /admin/principals/{id}/audit-token` using Alice's admin token. This token is scoped to read-only audit access.
4. **Charlie (SRE)**: The client uses the admin token with SRE scope. This token is configured during the context setup.

## §6 Forbidden Behaviors

The following table defines the negative-path assertion contract for unauthorized actions.

| Persona | Attempted Action | Expected Status Code | Expected Error Class |
|---|---|---|---|
| Bob | GET /admin/principals | 403 | UnauthorizedAction |
| Bob | GET /admin/metrics | 403 | UnauthorizedAction |
| Dana | POST /admin/principals | 403 | ReadOnlyViolation |
| Dana | POST /v1/messages | 401 | InvalidApiKey |
| Alice | POST /v1/messages | 401 | InvalidApiKey |
| Bob | POST /admin/plugins (invalid signature) | 400 | InvalidSignature |
| Charlie | POST /admin/audit/bypass-redaction | 403 | RedactionBypassDenied |

## §7 Persona-to-Scenario Mapping Reference

The complete mapping of personas to individual scenarios is maintained in the [BDD Test Conversion Map](/home/bhyoo/cc-lb-bdd/bdd-test-conversion-map.md).
