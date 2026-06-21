//! Alice — the operator persona. Holds admin privileges and exercises
//! the registration / configuration paths for principals, keys, and
//! upstreams (Writer stream W1 plus parts of W2).

use anyhow::Result;
use cc_lb_storage_api::principal::{PrincipalCreate, PrincipalKind};
use cc_lb_storage_api::{AuditEntry, AuditStore, PrincipalStore};
use serde_json::json;
use uuid::Uuid;

use crate::backend::StorageHandle;
use crate::results::{AuditEntrySummary, PrincipalCreateResult, PrincipalSoftDeleteResult};

pub struct Alice {
    storage: StorageHandle,
}

impl Alice {
    pub(crate) fn new(storage: StorageHandle) -> Self {
        Self { storage }
    }

    pub async fn create_principal(&self, name: &str) -> Result<PrincipalCreateResult> {
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

    pub async fn query_audit_for_principal(
        &self,
        principal_id: Uuid,
    ) -> Result<Vec<AuditEntrySummary>> {
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
            .map(|e| AuditEntrySummary {
                kind: e.kind.clone().unwrap_or_default(),
                actor: e.actor.clone().unwrap_or_default(),
                principal_id: e.principal_id.clone(),
                ts: e.ts,
            })
            .collect())
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

fn unix_now_secs() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default()
}
