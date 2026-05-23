use async_trait::async_trait;
use cc_lb_storage_api::{types::AuditEntry as ApiAuditEntry, AuditStore, StorageResult};

use crate::{AuditEntry as RedbAuditEntry, RedbStorage};

use super::error_map::{map_join_err, map_redb_err};

#[async_trait]
impl AuditStore for RedbStorage {
    async fn append_audit(&self, entry: &ApiAuditEntry) -> StorageResult<()> {
        let storage = self.clone();
        let entry = to_redb_audit_entry(entry);

        tokio::task::spawn_blocking(move || RedbStorage::append_audit(&storage, &entry))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn query_audit(
        &self,
        principal_id: Option<&str>,
        since: u64,
        until: u64,
        limit: usize,
    ) -> StorageResult<Vec<ApiAuditEntry>> {
        let storage = self.clone();
        let principal_id = principal_id.map(str::to_owned);

        tokio::task::spawn_blocking(move || {
            RedbStorage::query_audit(&storage, principal_id.as_deref(), since, until, limit)
        })
        .await
        .map_err(map_join_err)?
        .map(|entries| entries.into_iter().map(to_api_audit_entry).collect())
        .map_err(map_redb_err)
    }

    async fn prune_audit(&self, older_than: u64) -> StorageResult<u64> {
        let storage = self.clone();

        tokio::task::spawn_blocking(move || RedbStorage::prune_audit(&storage, older_than))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }
}

fn to_redb_audit_entry(entry: &ApiAuditEntry) -> RedbAuditEntry {
    RedbAuditEntry {
        ts: entry.ts,
        request_id: entry.request_id.clone(),
        principal_id: entry.principal_id.clone(),
        route: entry.route.clone(),
        upstream: entry.upstream.clone(),
        model: entry.model.clone(),
        status: entry.status,
        input_tokens: entry.input_tokens,
        output_tokens: entry.output_tokens,
        duration_ms: entry.duration_ms,
        agent_label: entry.agent_label.clone(),
        kind: entry.kind.clone(),
        payload: entry.payload.clone(),
    }
}

fn to_api_audit_entry(entry: RedbAuditEntry) -> ApiAuditEntry {
    ApiAuditEntry {
        ts: entry.ts,
        request_id: entry.request_id,
        principal_id: entry.principal_id,
        route: entry.route,
        upstream: entry.upstream,
        model: entry.model,
        status: entry.status,
        input_tokens: entry.input_tokens,
        output_tokens: entry.output_tokens,
        duration_ms: entry.duration_ms,
        agent_label: entry.agent_label,
        kind: entry.kind,
        payload: entry.payload,
    }
}
