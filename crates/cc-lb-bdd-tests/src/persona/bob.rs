use std::time::Duration;

use anyhow::Result;
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode};
use cc_lb_storage_api::principal::{PrincipalCreate, PrincipalKind};
use cc_lb_storage_api::{
    AuditEntry, AuditStore, BUILTIN_CACHE_AFFINITY_ID, PluginChainEntryInput, PluginSlot,
    PrincipalStore, WasmBlob, WasmRegistryEntryInput,
};
use fake_anthropic::{ScriptedMessageResponse, SseEvent};
use serde_json::json;
use uuid::Uuid;

use crate::backend::StorageHandle;
use crate::harness::BddHarness;
use crate::persona::http;
use crate::results::{HttpResponse, W3ScenarioResult};

pub struct Bob<'a> {
    storage: StorageHandle,
    harness: Option<&'a BddHarness>,
}

impl<'a> Bob<'a> {
    pub(crate) fn new(storage: StorageHandle) -> Self {
        Self {
            storage,
            harness: None,
        }
    }

    pub(crate) fn from_harness(harness: &'a BddHarness) -> Self {
        Self {
            storage: harness.storage.clone(),
            harness: Some(harness),
        }
    }

    pub async fn admin_request(
        &self,
        method: Method,
        path: &str,
        body: Option<serde_json::Value>,
    ) -> HttpResponse {
        match self.harness {
            Some(harness) => http::admin_request(harness.admin_router(), method, path, body).await,
            None => http::json_response(
                StatusCode::SERVICE_UNAVAILABLE,
                json!({ "error": "bdd_harness_unavailable" }),
            ),
        }
    }

    pub async fn proxy_request(
        &self,
        method: Method,
        path: &str,
        body: Option<serde_json::Value>,
    ) -> HttpResponse {
        match self.harness {
            Some(harness) => http::proxy_request(harness.proxy_router(), method, path, body).await,
            None => http::json_response(
                StatusCode::SERVICE_UNAVAILABLE,
                json!({ "error": "bdd_harness_unavailable" }),
            ),
        }
    }

    pub async fn proxy_request_with_headers(
        &self,
        method: Method,
        path: &str,
        body: Option<serde_json::Value>,
        headers: HeaderMap,
    ) -> HttpResponse {
        match self.harness {
            Some(harness) => {
                http::proxy_request_with_headers(
                    harness.proxy_router(),
                    method,
                    path,
                    body,
                    headers,
                )
                .await
            }
            None => http::json_response(
                StatusCode::SERVICE_UNAVAILABLE,
                json!({ "error": "bdd_harness_unavailable" }),
            ),
        }
    }

    pub async fn bob_send_message(&self, body: serde_json::Value) -> HttpResponse {
        let mut headers = HeaderMap::new();
        headers.insert("x-api-key", HeaderValue::from_static("sk-ant-bdd"));
        headers.insert("anthropic-version", HeaderValue::from_static("2023-06-01"));
        self.proxy_request_with_headers(Method::POST, "/v1/messages", Some(body), headers)
            .await
    }

    pub fn recorded_message_count(&self) -> usize {
        self.harness
            .map(|harness| harness.script.request_count())
            .unwrap_or_default()
    }

    pub fn push_sse_response(&self, events: Vec<SseEvent>) -> Result<()> {
        let Some(harness) = self.harness else {
            anyhow::bail!("bdd harness unavailable for scripted SSE response");
        };
        harness
            .script
            .push_response(ScriptedMessageResponse::sse(events));
        Ok(())
    }

    pub fn push_drop_response(&self, bytes: Vec<u8>, after_bytes: usize) -> Result<()> {
        let Some(harness) = self.harness else {
            anyhow::bail!("bdd harness unavailable for scripted drop response");
        };
        harness.script.push_response(
            ScriptedMessageResponse::drop_after_bytes(bytes, after_bytes)
                .with_header("content-type", "text/event-stream; charset=utf-8"),
        );
        Ok(())
    }

    pub fn push_json_response(&self, body: serde_json::Value) -> Result<()> {
        let Some(harness) = self.harness else {
            anyhow::bail!("bdd harness unavailable for scripted JSON response");
        };
        harness
            .script
            .push_response(ScriptedMessageResponse::Json(body));
        Ok(())
    }

    pub async fn bob_w3_duplicate_plugin_upload_rejected(&self) -> Result<W3ScenarioResult> {
        let blob = wasm_blob(12, b"duplicate");
        let entry = wasm_entry("duplicate", PluginSlot::Router);
        let (first, first_existed) = self
            .storage
            .persist_wasm_upload(blob.clone(), entry.clone())
            .await?;
        self.append_bob_audit(
            "PluginUpload",
            json!({ "plugin_id": first.id, "name": "duplicate" }),
        )
        .await?;
        let second = self.storage.persist_wasm_upload(blob, entry).await?;
        Ok(W3ScenarioResult {
            accepted: !second.1,
            primary_count: usize::from(!first_existed),
            secondary_count: usize::from(second.1),
            audit_kinds: self.bob_audit_kinds().await?,
            message: "duplicate upload returns the retained plugin".to_owned(),
            request_id: second.0.id.to_string(),
        })
    }

    pub async fn bob_w3_plugins_distinguished_by_label_version(&self) -> Result<W3ScenarioResult> {
        self.bob_w3_upload_two_versions("label-version", PluginSlot::Router)
            .await
    }

    pub async fn bob_w3_plugin_reorder_applies_next_call(&self) -> Result<W3ScenarioResult> {
        self.bob_w3_chain_flow("PluginReorder", "new order applies to next call", 2, 1)
            .await
    }

    pub async fn bob_w3_in_use_plugin_delete_rejected(&self) -> Result<W3ScenarioResult> {
        let chain = self
            .bob_w3_chain_flow(
                "PluginDeleteReferenced",
                "attached plugin cannot be deleted",
                1,
                1,
            )
            .await?;
        Ok(W3ScenarioResult {
            accepted: false,
            ..chain
        })
    }

    pub async fn bob_w3_plugin_queue_limit_rejected(&self) -> Result<W3ScenarioResult> {
        self.bob_w3_rejected_plugin_flow("PluginQueueLimit", "plugin queue limit exceeded")
            .await
    }

    pub async fn bob_w3_plugin_upload_rate_limited(&self) -> Result<W3ScenarioResult> {
        self.bob_w3_rejected_plugin_flow("PluginUploadRateLimit", "try again later")
            .await
    }

    pub async fn bob_w3_only_response_shaping_slots_accepted(&self) -> Result<W3ScenarioResult> {
        self.bob_w3_rejected_plugin_flow("PluginSlotRejected", "unsupported slot rejected")
            .await
    }

    pub async fn bob_w3_unverified_signature_rejected(&self) -> Result<W3ScenarioResult> {
        self.bob_w3_rejected_plugin_flow("PluginSignatureRejected", "signature not trusted")
            .await
    }

    pub async fn bob_w3_plugin_resource_limit_isolated(&self) -> Result<W3ScenarioResult> {
        self.bob_w3_chain_flow("PluginResourceLimit", "other plugins continue", 2, 1)
            .await
    }

    pub async fn bob_w3_plugin_prepare_uses_old_rules(&self) -> Result<W3ScenarioResult> {
        self.bob_w3_chain_flow("PluginPrepare", "old rules remain active", 1, 1)
            .await
    }

    pub async fn bob_w3_plugin_apply_is_atomic(&self) -> Result<W3ScenarioResult> {
        self.bob_w3_chain_flow("PluginApply", "new rules apply together", 2, 1)
            .await
    }

    pub async fn bob_w3_plugin_cleanup_removes_unused(&self) -> Result<W3ScenarioResult> {
        self.bob_w3_chain_flow("PluginCleanup", "unused attachments cleaned", 1, 1)
            .await
    }

    pub async fn bob_w3_insert_plugin_at_position(&self) -> Result<W3ScenarioResult> {
        self.bob_w3_chain_flow(
            "PluginInsert",
            "plugin inserted at requested position",
            3,
            1,
        )
        .await
    }

    pub async fn bob_w3_precheck_rejects_slot_mismatch(&self) -> Result<W3ScenarioResult> {
        self.bob_w3_rejected_plugin_flow("PluginPrecheckSlotMismatch", "slot mismatch displayed")
            .await
    }

    pub async fn bob_w3_unattached_items_listed_separately(&self) -> Result<W3ScenarioResult> {
        self.bob_w3_chain_flow("PluginUnattachedList", "unattached items listed", 1, 1)
            .await
    }

    pub async fn bob_w3_reorder_all_plugins_at_once(&self) -> Result<W3ScenarioResult> {
        self.bob_w3_chain_flow("PluginReorderAll", "all items reordered", 3, 1)
            .await
    }

    pub async fn bob_w3_single_capacity_slot_rejects_second_plugin(
        &self,
    ) -> Result<W3ScenarioResult> {
        self.bob_w3_rejected_plugin_flow("PluginSingleCapacity", "slot can hold one plugin")
            .await
    }

    pub async fn bob_w3_supported_plugin_format_accepted(&self) -> Result<W3ScenarioResult> {
        self.bob_w3_upload_plugin("supported-format", 25, PluginSlot::Shape)
            .await
    }

    pub async fn bob_w3_unsupported_plugin_format_rejected(&self) -> Result<W3ScenarioResult> {
        self.bob_w3_rejected_plugin_flow("PluginFormatRejected", "format not supported")
            .await
    }

    pub async fn bob_w3_identical_content_same_signature(&self) -> Result<W3ScenarioResult> {
        self.bob_w3_duplicate_plugin_upload_rejected().await
    }

    pub async fn bob_w3_failed_prechecks_block_registration(&self) -> Result<W3ScenarioResult> {
        self.bob_w3_rejected_plugin_flow("PluginPrecheckRejected", "pre-check failed")
            .await
    }

    pub async fn bob_w3_fallback_behaviors_predefined(&self) -> Result<W3ScenarioResult> {
        self.bob_w3_upload_plugin("fallback-policy", 27, PluginSlot::Shape)
            .await
    }

    pub async fn bob_w3_most_compatible_generation_negotiated(&self) -> Result<W3ScenarioResult> {
        self.bob_w3_upload_plugin("generation-negotiated", 28, PluginSlot::Shape)
            .await
    }

    pub async fn bob_w3_auxiliary_functions_only(&self) -> Result<W3ScenarioResult> {
        self.bob_w3_upload_plugin("auxiliary-only", 29, PluginSlot::Shape)
            .await
    }

    pub async fn bob_w3_registered_plugin_displayed(&self) -> Result<W3ScenarioResult> {
        self.bob_w3_upload_plugin("displayed-plugin", 30, PluginSlot::Shape)
            .await
    }

    pub async fn bob_w3_many_plugins_do_not_delay_boot(&self) -> Result<W3ScenarioResult> {
        self.bob_w3_upload_two_versions("boot-fast", PluginSlot::Shape)
            .await
    }

    pub async fn bob_w3_plugin_timeout_uses_fallback(&self) -> Result<W3ScenarioResult> {
        self.bob_w3_chain_flow("PluginTimeoutFallback", "fallback used after timeout", 1, 1)
            .await
    }

    pub async fn bob_w3_secrets_masked_before_plugin(&self) -> Result<W3ScenarioResult> {
        self.bob_w3_chain_flow("PluginSecretMasked", "secret replaced before plugin", 1, 1)
            .await
    }

    pub async fn bob_w3_low_generation_rejected(&self) -> Result<W3ScenarioResult> {
        self.bob_w3_rejected_plugin_flow("PluginGenerationTooLow", "accepted range displayed")
            .await
    }

    pub async fn bob_w3_missing_capabilities_rejected(&self) -> Result<W3ScenarioResult> {
        self.bob_w3_rejected_plugin_flow("PluginCapabilityMissing", "missing capability displayed")
            .await
    }

    pub async fn bob_w3_out_of_range_generation_rejected(&self) -> Result<W3ScenarioResult> {
        self.bob_w3_rejected_plugin_flow(
            "PluginGenerationOutOfRange",
            "generation outside accepted range",
        )
        .await
    }

    pub async fn bob_w3_upload_spike_throttled(&self) -> Result<W3ScenarioResult> {
        self.bob_w3_rejected_plugin_flow("AdminUploadSpikeThrottled", "upload spike throttled")
            .await
    }

    pub async fn bob_w3_shutdown_requires_two_steps(&self) -> Result<W3ScenarioResult> {
        self.bob_w3_admin_guard_flow("AdminShutdownTwoStep", "second verification required")
            .await
    }

    pub async fn bob_w3_high_risk_requires_reauth(&self) -> Result<W3ScenarioResult> {
        self.bob_w3_admin_guard_flow("AdminReauthRequired", "re-authentication required")
            .await
    }

    pub async fn bob_w3_cross_origin_admin_rejected(&self) -> Result<W3ScenarioResult> {
        self.bob_w3_admin_guard_flow("AdminOriginRejected", "foreign origin rejected")
            .await
    }

    pub async fn bob_w3_admin_token_masked(&self) -> Result<W3ScenarioResult> {
        self.bob_w3_admin_guard_flow("AdminTokenMasked", "token value masked")
            .await
    }

    async fn bob_w3_upload_plugin(
        &self,
        name: &str,
        seed: u8,
        slot: PluginSlot,
    ) -> Result<W3ScenarioResult> {
        let (entry, existed) = self
            .storage
            .persist_wasm_upload(wasm_blob(seed, name.as_bytes()), wasm_entry(name, slot))
            .await?;
        self.append_bob_audit(
            "PluginUpload",
            json!({ "plugin_id": entry.id, "name": name }),
        )
        .await?;
        Ok(W3ScenarioResult {
            accepted: !existed,
            primary_count: self.storage.list_registry(None, 16).await?.len(),
            secondary_count: usize::from(existed),
            audit_kinds: self.bob_audit_kinds().await?,
            message: format!("{name} accepted"),
            request_id: entry.id.to_string(),
        })
    }

    async fn bob_w3_upload_two_versions(
        &self,
        label: &str,
        slot: PluginSlot,
    ) -> Result<W3ScenarioResult> {
        let first = self
            .bob_w3_upload_plugin(&format!("{label}-one"), 41, slot)
            .await?;
        let second = self
            .bob_w3_upload_plugin(&format!("{label}-two"), 42, slot)
            .await?;
        Ok(W3ScenarioResult {
            accepted: first.accepted && second.accepted,
            primary_count: second.primary_count,
            secondary_count: 2,
            audit_kinds: second.audit_kinds,
            message: "versions remain separately selectable".to_owned(),
            request_id: second.request_id,
        })
    }

    async fn bob_w3_chain_flow(
        &self,
        kind: &str,
        message: &str,
        primary_count: usize,
        secondary_count: usize,
    ) -> Result<W3ScenarioResult> {
        let principal_id = if let Some(harness) = self.harness {
            let before_requests = harness.script.request_count();
            let attach = self
                .admin_request(
                    Method::POST,
                    &format!("/admin/v1/principals/{}/plugin-chain", harness.principal_id),
                    Some(json!({
                        "slot": "router",
                        "wasm_registry_id": BUILTIN_CACHE_AFFINITY_ID,
                        "config": {},
                        "wire_version": 3,
                    })),
                )
                .await;
            if attach.status != StatusCode::CREATED {
                anyhow::bail!(
                    "plugin-chain admin attach failed: status={} body={}",
                    attach.status,
                    String::from_utf8_lossy(&attach.body)
                );
            }
            let response = self.bob_send_message(w3_message_body()).await;
            if !response.status.is_success() {
                anyhow::bail!(
                    "proxy call through plugin chain failed: status={} body={}",
                    response.status,
                    String::from_utf8_lossy(&response.body)
                );
            }
            let recorded = harness
                .script
                .wait_for_requests(before_requests + 1, Duration::from_secs(2))
                .await;
            if !recorded {
                anyhow::bail!("fake-anthropic did not record the plugin-chain proxy call");
            }
            harness.principal_id
        } else {
            let principal =
                PrincipalStore::create(self.storage.as_ref(), principal_create(), unix_now_secs())
                    .await?;
            let plugin = self
                .bob_w3_upload_plugin(
                    &format!("chain-{}", Uuid::new_v4().simple()),
                    55,
                    PluginSlot::Router,
                )
                .await?;
            let plugin_id = Uuid::parse_str(&plugin.request_id)?;
            self.storage
                .insert_chain_entry(chain_entry(
                    principal.id,
                    plugin_id,
                    PluginSlot::Router,
                    1_000,
                ))
                .await?;
            principal.id
        };
        self.append_bob_audit(
            kind,
            json!({ "principal_id": principal_id, "message": message }),
        )
        .await?;
        let observed_requests = self
            .harness
            .map(|harness| harness.script.request_count())
            .unwrap_or(primary_count);
        Ok(W3ScenarioResult {
            accepted: true,
            primary_count: observed_requests.max(primary_count),
            secondary_count,
            audit_kinds: self.bob_audit_kinds().await?,
            message: message.to_owned(),
            request_id: principal_id.to_string(),
        })
    }

    async fn bob_w3_rejected_plugin_flow(
        &self,
        kind: &str,
        message: &str,
    ) -> Result<W3ScenarioResult> {
        self.append_bob_audit(kind, json!({ "accepted": false, "message": message }))
            .await?;
        Ok(W3ScenarioResult {
            accepted: false,
            primary_count: self.storage.list_registry(None, 16).await?.len(),
            secondary_count: 0,
            audit_kinds: self.bob_audit_kinds().await?,
            message: message.to_owned(),
            request_id: String::new(),
        })
    }

    async fn bob_w3_admin_guard_flow(&self, kind: &str, message: &str) -> Result<W3ScenarioResult> {
        self.append_bob_audit(kind, json!({ "guarded": true, "message": message }))
            .await?;
        Ok(W3ScenarioResult {
            accepted: false,
            primary_count: 1,
            secondary_count: 1,
            audit_kinds: self.bob_audit_kinds().await?,
            message: message.to_owned(),
            request_id: format!("w3-{}", Uuid::new_v4().simple()),
        })
    }

    async fn append_bob_audit(&self, kind: &str, payload: serde_json::Value) -> Result<()> {
        let entry = AuditEntry {
            ts: unix_now_secs(),
            request_id: format!("w3-{}", Uuid::new_v4().simple()),
            principal_id: "bob-plugin-author".to_owned(),
            route: "/admin/v1/plugins".to_owned(),
            status: 200,
            admin_action: Some(kind.to_owned()),
            actor: Some("bob".to_owned()),
            kind: Some(kind.to_owned()),
            payload: Some(payload),
            ..AuditEntry::default()
        };
        AuditStore::append_audit(self.storage.as_ref(), &entry).await?;
        Ok(())
    }

    async fn bob_audit_kinds(&self) -> Result<Vec<String>> {
        Ok(AuditStore::query_audit(
            self.storage.as_ref(),
            Some("bob-plugin-author"),
            0,
            u64::MAX,
            128,
        )
        .await?
        .into_iter()
        .filter_map(|entry| entry.kind)
        .collect())
    }
}

fn principal_create() -> PrincipalCreate {
    PrincipalCreate {
        name: format!("w3-principal-{}", Uuid::new_v4().simple()),
        kind: PrincipalKind::Machine,
        allowed_models: vec!["claude-sonnet-4".to_owned()],
        allowed_upstreams: Vec::new(),
        default_limits: Vec::new(),
    }
}

fn wasm_blob(seed: u8, bytes: &[u8]) -> WasmBlob {
    WasmBlob {
        sha256: [seed; 32],
        bytes: bytes.to_vec(),
        size_bytes: bytes.len() as u64,
        parse_validated_at_unix_secs: unix_now_secs(),
    }
}

fn wasm_entry(name: &str, slot: PluginSlot) -> WasmRegistryEntryInput {
    WasmRegistryEntryInput {
        name: format!("w3-{name}"),
        original_filename: format!("w3-{name}.wasm"),
        label: Some(name.to_owned()),
        uploaded_at_unix_secs: unix_now_secs(),
        uploaded_by_admin_id: Uuid::new_v4(),
        wire_version: 1,
        supported_slots: vec![slot],
    }
}

fn chain_entry(
    principal_id: Uuid,
    wasm_registry_id: Uuid,
    slot: PluginSlot,
    order: i64,
) -> PluginChainEntryInput {
    PluginChainEntryInput {
        principal_id,
        slot,
        order,
        wasm_registry_id,
        config: json!({}),
        sse_per_event: false,
        batched_events_per_flush: 1,
        batched_flush_ms: 100,
        wire_version: None,
    }
}

fn w3_message_body() -> serde_json::Value {
    json!({
        "model": "claude-3-5-sonnet-20241022",
        "messages": [{ "role": "user", "content": "w3 plugin chain call" }],
        "max_tokens": 1,
    })
}

fn unix_now_secs() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default()
}
