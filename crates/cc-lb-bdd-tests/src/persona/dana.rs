//! Dana — auditor persona. Read-only token for audit log, redaction,
//! and observability scenarios (W4).

use axum::http::{Method, StatusCode};
use cc_lb_storage_api::{AuditEntry, AuditStore};
use serde_json::json;

use crate::backend::StorageHandle;
use crate::harness::BddHarness;
use crate::persona::http;
use crate::results::HttpResponse;

pub struct Dana<'a> {
    storage: StorageHandle,
    harness: Option<&'a BddHarness>,
}

impl<'a> Dana<'a> {
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

    pub async fn dana_query_audit(&self, principal_id: Option<&str>) -> Vec<AuditEntry> {
        AuditStore::query_audit(self.storage.as_ref(), principal_id, 0, u64::MAX / 2, 128)
            .await
            .unwrap_or_default()
    }
}
