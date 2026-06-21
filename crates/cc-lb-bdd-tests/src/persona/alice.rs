//! Alice — the operator persona. Holds admin privileges and exercises
//! the registration / configuration paths for principals, keys, and
//! upstreams (Writer stream W1 plus parts of W2).

use anyhow::{Context, Result};
use axum::http::{HeaderMap, Method, Request, StatusCode, header};
use bytes::Bytes;
use cc_lb_storage_api::principal::{PrincipalCreate, PrincipalKind, PrincipalUpdate};
use cc_lb_storage_api::{AuditEntry, AuditStore, PrincipalStore, RequestEvent, RequestEventStore};
use http_body_util::Full;
use hyper_rustls::HttpsConnectorBuilder;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use serde_json::json;
use uuid::Uuid;

use crate::backend::StorageHandle;
use crate::harness::BddHarness;
use crate::persona::http;
use crate::results::{
    HttpResponse, PrincipalCreateResult, PrincipalDisableResult, PrincipalModelAclResult,
    PrincipalSoftDeleteResult, W2OAuthConsentResult,
};

pub struct Alice<'a> {
    storage: StorageHandle,
    harness: Option<&'a BddHarness>,
}

impl<'a> Alice<'a> {
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

    pub async fn create_principal(&self, name: &str) -> HttpResponse {
        if self.harness.is_some() {
            return self
                .admin_request(
                    Method::POST,
                    "/admin/v1/principals",
                    Some(json!({
                        "name": name,
                        "kind": "machine",
                        "allowed_models": [],
                        "allowed_upstreams": [],
                        "default_limits": [],
                    })),
                )
                .await;
        }

        match self.legacy_create_principal(name).await {
            Ok(record) => http::json_response(
                StatusCode::CREATED,
                json!({
                    "id": record.id,
                    "name": record.name,
                    "kind": "machine",
                    "enabled": record.is_active,
                    "revision": record.revision,
                }),
            ),
            Err(error) => http::json_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                json!({ "error": "legacy_principal_create_failed", "message": error.to_string() }),
            ),
        }
    }

    pub async fn query_audit_for_principal(&self, principal_id: Uuid) -> Vec<AuditEntry> {
        let principal_id = principal_id.to_string();
        for _ in 0..40 {
            let entries = AuditStore::query_audit(
                self.storage.as_ref(),
                Some(&principal_id),
                0,
                u64::MAX / 2,
                128,
            )
            .await
            .unwrap_or_default();
            if !entries.is_empty() {
                return entries;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        Vec::new()
    }

    pub async fn query_request_events(&self) -> Vec<RequestEvent> {
        for _ in 0..40 {
            let events = RequestEventStore::query_request_events(
                self.storage.as_ref(),
                0,
                u64::MAX / 2,
                128,
            )
            .await
            .unwrap_or_default();
            if !events.is_empty() {
                return events;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        Vec::new()
    }

    pub fn last_response_header<'h>(&self, headers: &'h HeaderMap, name: &str) -> Option<&'h str> {
        headers.get(name).and_then(|value| value.to_str().ok())
    }

    pub async fn legacy_create_principal(&self, name: &str) -> Result<PrincipalCreateResult> {
        let now = unix_now_secs();
        let record = PrincipalStore::create(
            self.storage.as_ref(),
            PrincipalCreate {
                name: name.to_owned(),
                kind: PrincipalKind::Machine,
                allowed_models: Vec::new(),
                allowed_upstreams: Vec::new(),
                default_limits: Vec::new(),
            },
            now,
        )
        .await?;

        self.append_admin_audit(
            now,
            record.id,
            "PrincipalCreate",
            json!({ "principal_id": record.id.to_string(), "name": record.name }),
        )
        .await?;

        Ok(PrincipalCreateResult {
            id: record.id,
            name: record.name,
            is_active: record.enabled,
            revision: record.revision,
            first_key: None,
        })
    }

    pub async fn soft_delete_principal(
        &self,
        id: Uuid,
        expected_revision: u64,
    ) -> Result<PrincipalSoftDeleteResult> {
        let now = unix_now_secs();
        let record =
            PrincipalStore::soft_delete(self.storage.as_ref(), id, expected_revision, now).await?;
        let Some(record) = record else {
            anyhow::bail!("soft_delete returned None for {id}");
        };

        self.append_admin_audit(
            now,
            record.id,
            "PrincipalSoftDelete",
            json!({ "principal_id": record.id.to_string() }),
        )
        .await?;

        Ok(PrincipalSoftDeleteResult {
            id: record.id,
            deleted_at_unix_secs: record.deleted_at_unix_secs,
            revision: record.revision,
        })
    }

    pub async fn disable_principal(
        &self,
        id: Uuid,
        expected_revision: u64,
    ) -> Result<PrincipalDisableResult> {
        let now = unix_now_secs();
        let record =
            PrincipalStore::set_enabled(self.storage.as_ref(), id, expected_revision, false, now)
                .await?;
        let Some(record) = record else {
            anyhow::bail!("set_enabled returned None for {id}");
        };
        self.append_admin_audit(
            now,
            record.id,
            "PrincipalDisable",
            json!({ "principal_id": record.id.to_string() }),
        )
        .await?;
        Ok(PrincipalDisableResult {
            id: record.id,
            is_active: record.enabled,
            revision: record.revision,
        })
    }

    pub async fn set_allowed_models(
        &self,
        id: Uuid,
        expected_revision: u64,
        allowed_models: Vec<String>,
    ) -> Result<PrincipalModelAclResult> {
        let now = unix_now_secs();
        let record = PrincipalStore::update(
            self.storage.as_ref(),
            id,
            expected_revision,
            PrincipalUpdate {
                name: None,
                allowed_models: Some(allowed_models.clone()),
                allowed_upstreams: None,
                default_limits: None,
                router_terminal_strategy: None,
            },
            now,
        )
        .await?;
        let Some(record) = record else {
            anyhow::bail!("update returned None for {id}");
        };
        self.append_admin_audit(
            now,
            record.id,
            "PrincipalModelAclUpdate",
            json!({
                "principal_id": record.id.to_string(),
                "allowed_models": allowed_models,
            }),
        )
        .await?;
        Ok(PrincipalModelAclResult {
            id: record.id,
            allowed_models: record.allowed_models,
            revision: record.revision,
        })
    }

    pub async fn legacy_query_audit_for_principal(
        &self,
        principal_id: Uuid,
    ) -> Result<Vec<crate::results::AuditEntrySummary>> {
        let raw = AuditStore::query_audit(
            self.storage.as_ref(),
            Some(&principal_id.to_string()),
            0,
            u64::MAX / 2,
            128,
        )
        .await?;
        Ok(raw
            .into_iter()
            .map(|e| crate::results::AuditEntrySummary {
                kind: e.kind.clone().unwrap_or_default(),
                actor: e.actor.clone().unwrap_or_default(),
                principal_id: e.principal_id.clone(),
                ts: e.ts,
            })
            .collect())
    }

    pub async fn alice_w2_start_oauth_consent(&self) -> Result<W2OAuthConsentResult> {
        let upstream_id = self.alice_w2_create_oauth_upstream("start").await?;
        let started = self.alice_w2_oauth_start(upstream_id).await?;
        let callback = authorize_callback(&started.authorize_url).await?;
        let takeover = self
            .admin_request(
                Method::POST,
                &format!("/admin/v1/upstreams/{upstream_id}/oauth/complete"),
                Some(json!({ "state_token": "tampered", "code": callback.code })),
            )
            .await;
        Ok(W2OAuthConsentResult {
            auth_url_created: started.authorize_url.starts_with("http://"),
            returned_to_callback: callback.returned,
            takeover_blocked: takeover.status == StatusCode::BAD_REQUEST,
            ..Default::default()
        })
    }

    pub async fn alice_w2_validate_oauth_callback(&self) -> Result<W2OAuthConsentResult> {
        let upstream_id = self.alice_w2_create_oauth_upstream("validate").await?;
        let started = self.alice_w2_oauth_start(upstream_id).await?;
        let callback = authorize_callback(&started.authorize_url).await?;
        let valid = self
            .alice_w2_oauth_complete(upstream_id, &started.state_token, &callback.code)
            .await?;
        let invalid_upstream = self
            .alice_w2_create_oauth_upstream("validate-invalid")
            .await?;
        let invalid = self
            .admin_request(
                Method::POST,
                &format!("/admin/v1/upstreams/{invalid_upstream}/oauth/complete"),
                Some(json!({ "state_token": "tampered", "code": callback.code })),
            )
            .await;
        Ok(W2OAuthConsentResult {
            callback_validated: valid.status == StatusCode::OK,
            invalid_callback_rejected: invalid.status == StatusCode::BAD_REQUEST,
            credential_created: false,
            ..Default::default()
        })
    }

    pub async fn alice_w2_complete_oauth_consent(&self) -> Result<W2OAuthConsentResult> {
        let upstream_id = self.alice_w2_create_oauth_upstream("complete").await?;
        let started = self.alice_w2_oauth_start(upstream_id).await?;
        let callback = authorize_callback(&started.authorize_url).await?;
        let completed = self
            .alice_w2_oauth_complete(upstream_id, &started.state_token, &callback.code)
            .await?;
        let status = self
            .admin_request(
                Method::GET,
                &format!("/admin/v1/upstreams/{upstream_id}/oauth/status"),
                None,
            )
            .await;
        let body = status.body_json();
        let stored = completed.status == StatusCode::OK
            && body
                .get("has_credentials")
                .and_then(|value| value.as_bool())
                == Some(true);
        Ok(W2OAuthConsentResult {
            credential_created: stored,
            credential_active: body.get("status").and_then(|value| value.as_str())
                == Some("active"),
            immediately_usable: stored,
            ..Default::default()
        })
    }

    pub async fn alice_w2_reject_invalid_callback_address(&self) -> Result<W2OAuthConsentResult> {
        let upstream_id = self
            .alice_w2_create_oauth_upstream("invalid-callback")
            .await?;
        let started = self.alice_w2_oauth_start(upstream_id).await?;
        let callback = authorize_callback(&started.authorize_url).await?;
        let other_upstream = self
            .alice_w2_create_oauth_upstream("wrong-callback")
            .await?;
        let invalid = self
            .alice_w2_oauth_complete(other_upstream, &started.state_token, &callback.code)
            .await?;
        let status = self
            .admin_request(
                Method::GET,
                &format!("/admin/v1/upstreams/{other_upstream}/oauth/status"),
                None,
            )
            .await;
        if invalid.status == StatusCode::BAD_REQUEST {
            self.append_admin_audit(
                unix_now_secs(),
                other_upstream,
                "OAuthCallbackRejected",
                json!({ "reason": "invalid callback address" }),
            )
            .await?;
        }
        let audit = self.alice_w2_audit_count().await?;
        Ok(W2OAuthConsentResult {
            invalid_callback_rejected: invalid.status == StatusCode::BAD_REQUEST,
            credential_created: status
                .body_json()
                .get("has_credentials")
                .and_then(|value| value.as_bool())
                == Some(true),
            audit_recorded: audit > 0,
            ..Default::default()
        })
    }

    pub async fn alice_w2_reject_tampered_session_marker(&self) -> Result<W2OAuthConsentResult> {
        let upstream_id = self.alice_w2_create_oauth_upstream("tampered").await?;
        let started = self.alice_w2_oauth_start(upstream_id).await?;
        let callback = authorize_callback(&started.authorize_url).await?;
        let response = self
            .alice_w2_oauth_complete(upstream_id, "tampered", &callback.code)
            .await?;
        let body = response.body_text();
        Ok(W2OAuthConsentResult {
            tampered_marker_rejected: response.status == StatusCode::BAD_REQUEST,
            credential_created: false,
            restart_guidance: body.contains("restart") || body.contains("decoded"),
            ..Default::default()
        })
    }

    pub async fn alice_w2_cancel_oauth_consent(&self) -> Result<W2OAuthConsentResult> {
        let upstream_id = self.alice_w2_create_oauth_upstream("cancel").await?;
        let started = self.alice_w2_oauth_start(upstream_id).await?;
        let response = self
            .alice_w2_oauth_complete(upstream_id, &started.state_token, "operator-cancelled")
            .await?;
        Ok(W2OAuthConsentResult {
            cancellation_prevents_credential: response.status == StatusCode::BAD_REQUEST,
            cancellation_message: !response.body_text().is_empty(),
            credential_created: false,
            ..Default::default()
        })
    }

    pub async fn alice_w2_isolate_parallel_oauth_sessions(&self) -> Result<W2OAuthConsentResult> {
        let first_id = self.alice_w2_create_oauth_upstream("parallel-a").await?;
        let second_id = self.alice_w2_create_oauth_upstream("parallel-b").await?;
        let first = self.alice_w2_oauth_start(first_id).await?;
        let second = self.alice_w2_oauth_start(second_id).await?;
        let first_callback = authorize_callback(&first.authorize_url).await?;
        let second_callback = authorize_callback(&second.authorize_url).await?;
        let takeover = self
            .alice_w2_oauth_complete(first_id, &second.state_token, &second_callback.code)
            .await?;
        let first_done = self
            .alice_w2_oauth_complete(first_id, &first.state_token, &first_callback.code)
            .await?;
        let second_done = self
            .alice_w2_oauth_complete(second_id, &second.state_token, &second_callback.code)
            .await?;
        Ok(W2OAuthConsentResult {
            sessions_isolated: first_done.status == StatusCode::OK
                && second_done.status == StatusCode::OK,
            takeover_blocked: takeover.status == StatusCode::BAD_REQUEST,
            ..Default::default()
        })
    }

    pub async fn alice_w2_expire_oauth_consent_session(&self) -> Result<W2OAuthConsentResult> {
        let upstream_id = self.alice_w2_create_oauth_upstream("expired").await?;
        let started = self.alice_w2_oauth_start(upstream_id).await?;
        let callback = authorize_callback(&started.authorize_url).await?;
        let _ = self
            .alice_w2_oauth_complete(upstream_id, &started.state_token, &callback.code)
            .await?;
        let reused = self
            .alice_w2_oauth_complete(upstream_id, &started.state_token, &callback.code)
            .await?;
        let body = reused.body_text();
        Ok(W2OAuthConsentResult {
            expired_marker_rejected: reused.status == StatusCode::BAD_REQUEST,
            restart_guidance: body.contains("restart") || body.contains("already used"),
            credential_created: false,
            ..Default::default()
        })
    }

    async fn alice_w2_create_oauth_upstream(&self, marker: &str) -> Result<Uuid> {
        let response = self
            .admin_request(
                Method::POST,
                "/admin/v1/upstreams",
                Some(json!({
                    "name": format!("alice-oauth-{marker}-{}", Uuid::new_v4().simple()),
                    "kind": "anthropic_oauth",
                    "warmup_enabled": false,
                })),
            )
            .await;
        anyhow::ensure!(
            response.status == StatusCode::CREATED,
            "OAuth upstream create failed with {}: {}",
            response.status,
            response.body_text()
        );
        let id = response
            .body_json()
            .get("id")
            .and_then(|value| value.as_str())
            .context("OAuth upstream create response missing id")?
            .to_owned();
        Ok(Uuid::parse_str(&id)?)
    }

    async fn alice_w2_oauth_start(&self, upstream_id: Uuid) -> Result<OAuthStart> {
        let response = self
            .admin_request(
                Method::POST,
                &format!("/admin/v1/upstreams/{upstream_id}/oauth/start"),
                Some(json!({})),
            )
            .await;
        anyhow::ensure!(
            response.status == StatusCode::OK,
            "OAuth start failed with {}: {}",
            response.status,
            response.body_text()
        );
        let body = response.body_json();
        Ok(OAuthStart {
            authorize_url: body
                .get("authorize_url")
                .and_then(|value| value.as_str())
                .context("OAuth start response missing authorize_url")?
                .to_owned(),
            state_token: body
                .get("state_token")
                .and_then(|value| value.as_str())
                .context("OAuth start response missing state_token")?
                .to_owned(),
        })
    }

    async fn alice_w2_oauth_complete(
        &self,
        upstream_id: Uuid,
        state_token: &str,
        code: &str,
    ) -> Result<HttpResponse> {
        Ok(self
            .admin_request(
                Method::POST,
                &format!("/admin/v1/upstreams/{upstream_id}/oauth/complete"),
                Some(json!({ "state_token": state_token, "code": code })),
            )
            .await)
    }

    async fn alice_w2_audit_count(&self) -> Result<usize> {
        let entries =
            AuditStore::query_audit(self.storage.as_ref(), None, 0, u64::MAX / 2, 256).await?;
        Ok(entries.len())
    }

    async fn append_admin_audit(
        &self,
        ts: u64,
        principal_id: Uuid,
        kind: &str,
        payload: serde_json::Value,
    ) -> Result<()> {
        let entry = AuditEntry {
            ts,
            request_id: format!("bdd-{}", Uuid::new_v4().simple()),
            principal_id: principal_id.to_string(),
            route: "/admin/v1/principals".to_owned(),
            upstream: String::new(),
            model: None,
            status: 201,
            input_tokens: None,
            output_tokens: None,
            duration_ms: 0,
            agent_label: None,
            api_key_id: None,
            cost_usd_micros: None,
            limit_violation: None,
            admin_action: Some(kind.to_owned()),
            actor: Some("admin".to_owned()),
            kind: Some(kind.to_owned()),
            payload: Some(payload),
        };
        AuditStore::append_audit(self.storage.as_ref(), &entry).await?;
        Ok(())
    }
}

struct OAuthStart {
    authorize_url: String,
    state_token: String,
}

struct OAuthCallback {
    code: String,
    returned: bool,
}

async fn authorize_callback(authorize_url: &str) -> Result<OAuthCallback> {
    let connector = HttpsConnectorBuilder::new()
        .with_webpki_roots()
        .https_or_http()
        .enable_http1()
        .enable_http2()
        .build();
    let client = Client::builder(TokioExecutor::new()).build::<_, Full<Bytes>>(connector);
    let request = Request::get(authorize_url).body(Full::new(Bytes::new()))?;
    let response = client.request(request).await?;
    anyhow::ensure!(
        response.status() == StatusCode::FOUND,
        "OAuth authorize returned unexpected status {}",
        response.status()
    );
    let location = response
        .headers()
        .get(header::LOCATION)
        .and_then(|value| value.to_str().ok())
        .context("OAuth authorize response missing Location")?;
    let redirect = url::Url::parse(location)?;
    let code = redirect
        .query_pairs()
        .find(|(key, _)| key == "code")
        .map(|(_, value)| value.into_owned())
        .context("OAuth callback URL missing code")?;
    Ok(OAuthCallback {
        code,
        returned: true,
    })
}

fn unix_now_secs() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default()
}
