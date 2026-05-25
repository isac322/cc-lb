use async_trait::async_trait;
use cc_lb_storage_api::{
    types::{RequestEvent as ApiRequestEvent, RequestEventUpstream as ApiRequestEventUpstream},
    RequestEventStore, StorageResult,
};

use crate::{
    RedbStorage, RequestEvent as RedbRequestEvent, RequestEventUpstream as RedbRequestEventUpstream,
};

use super::error_map::{map_join_err, map_redb_err};

#[async_trait]
impl RequestEventStore for RedbStorage {
    async fn append_request_event(&self, event: &ApiRequestEvent) -> StorageResult<()> {
        let storage = self.clone();
        let event = to_redb_request_event(event);

        tokio::task::spawn_blocking(move || RedbStorage::append_request_event(&storage, &event))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn query_request_events(
        &self,
        since: u64,
        until: u64,
        limit: usize,
    ) -> StorageResult<Vec<ApiRequestEvent>> {
        let storage = self.clone();

        tokio::task::spawn_blocking(move || {
            RedbStorage::query_request_events(&storage, since, until, limit)
        })
        .await
        .map_err(map_join_err)?
        .map(|events| events.into_iter().map(to_api_request_event).collect())
        .map_err(map_redb_err)
    }
}

fn to_redb_request_event(event: &ApiRequestEvent) -> RedbRequestEvent {
    RedbRequestEvent {
        ts: event.ts,
        request_id: event.request_id.clone(),
        principal_id: event.principal_id.clone(),
        principal_kind: event.principal_kind.clone(),
        upstream: event.upstream.map(to_redb_request_event_upstream),
        model: event.model.clone(),
        status: event.status,
        input_tokens: event.input_tokens,
        output_tokens: event.output_tokens,
        duration_ms: event.duration_ms,
        error_code: event.error_code.clone(),
    }
}

fn to_api_request_event(event: RedbRequestEvent) -> ApiRequestEvent {
    ApiRequestEvent {
        ts: event.ts,
        request_id: event.request_id,
        principal_id: event.principal_id,
        principal_kind: event.principal_kind,
        upstream: event.upstream.map(to_api_request_event_upstream),
        model: event.model,
        status: event.status,
        input_tokens: event.input_tokens,
        output_tokens: event.output_tokens,
        duration_ms: event.duration_ms,
        error_code: event.error_code,
    }
}

fn to_redb_request_event_upstream(upstream: ApiRequestEventUpstream) -> RedbRequestEventUpstream {
    match upstream {
        ApiRequestEventUpstream::AnthropicDirect => RedbRequestEventUpstream::AnthropicDirect,
        ApiRequestEventUpstream::CustomAnthropicSpec => {
            RedbRequestEventUpstream::CustomAnthropicSpec
        }
    }
}

fn to_api_request_event_upstream(upstream: RedbRequestEventUpstream) -> ApiRequestEventUpstream {
    match upstream {
        RedbRequestEventUpstream::AnthropicDirect => ApiRequestEventUpstream::AnthropicDirect,
        RedbRequestEventUpstream::CustomAnthropicSpec => {
            ApiRequestEventUpstream::CustomAnthropicSpec
        }
    }
}
