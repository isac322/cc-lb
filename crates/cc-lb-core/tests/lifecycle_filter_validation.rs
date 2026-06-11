mod common;

#[path = "router_lifecycle_support/mod.rs"]
mod router_lifecycle_support;

use std::sync::Arc;

use bytes::Bytes;
use cc_lb_core::api_keys::concurrent_guard::KeyConcurrencyManager;
use cc_lb_core::api_keys::limit_engine::LimitEngine;
use cc_lb_plugin_api::{
    FilterError, FilterOutput, FilterPlugin, InternalErrorKind, InternalErrorStage,
    PerCandidateReason, Principal, RequestContext, UpstreamCandidate,
};
use cc_lb_storage_api::types::{KeyStatus, StoredApiKeyRecord};
use cc_lb_storage_api::{RequestEventStore, Storage as StorageTrait};
use cc_lb_storage_redb::Storage as RedbStorage;
use http::StatusCode;
use uuid::Uuid;

use common::{collect_body, messages_request};
use router_lifecycle_support::{
    InvalidOutputFilter, InvalidOutputKind, RouterLifecycleState, api_key_record,
    lifecycle_with_records,
};

#[tokio::test]
async fn invalid_kept_ids_pass_candidates_through_and_log_invalid_output()
-> Result<(), Box<dyn std::error::Error>> {
    for (seed, kind) in [
        (41_u8, InvalidOutputKind::Unknown),
        (42_u8, InvalidOutputKind::Duplicate),
        (43_u8, InvalidOutputKind::Superset),
    ] {
        let first = upstream_id(1);
        let second = upstream_id(2);
        let state = RouterLifecycleState::default();
        let storage = request_storage(seed)?;
        let lifecycle = lifecycle_with_records(
            vec![
                api_key_record(first, "first", "http://first.local/"),
                api_key_record(second, "second", "http://second.local/"),
            ],
            vec![Arc::new(InvalidOutputFilter { kind })],
            state.clone(),
        )
        .with_request_event_storage(Arc::clone(&storage) as Arc<dyn StorageTrait>)
        .with_static_limit_subject(
            LimitEngine::new(Arc::new(KeyConcurrencyManager::new())),
            "principal-test".to_owned(),
            "key-test".to_owned(),
            active_record(),
        );

        let response = lifecycle
            .handle(messages_request(Bytes::from_static(
                br#"{"model":"claude-test","messages":[],"max_tokens":16}"#,
            )))
            .await?;
        let (status, _headers, _body) = collect_body(response).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            state
                .router_candidates
                .lock()
                .expect("router candidates lock")
                .as_slice(),
            &[vec![first]],
            "{kind:?} invalid output passes through to terminal strategy"
        );
        assert_eq!(
            *state.dispatch_calls.lock().expect("dispatch calls lock"),
            1,
            "{kind:?} invalid output must not become a 503"
        );

        let event = single_request_event(storage.as_ref()).await?;
        assert!(event.internal_errors.iter().any(|error| {
            error.stage == InternalErrorStage::RouterFilter
                && error.kind == InternalErrorKind::InvalidOutput
                && error
                    .message
                    .as_deref()
                    .is_some_and(|message| message.contains("filter output validation failed"))
        }));
    }
    Ok(())
}

#[tokio::test]
async fn reason_count_mismatch_is_sanitized_without_rejecting_valid_kept_ids()
-> Result<(), Box<dyn std::error::Error>> {
    let first = upstream_id(11);
    let second = upstream_id(12);
    let state = RouterLifecycleState::default();
    let storage = request_storage(44)?;
    let lifecycle = lifecycle_with_records(
        vec![
            api_key_record(first, "first", "http://first.local/"),
            api_key_record(second, "second", "http://second.local/"),
        ],
        vec![Arc::new(MissingReasonFilter { kept: second })],
        state.clone(),
    )
    .with_request_event_storage(Arc::clone(&storage) as Arc<dyn StorageTrait>)
    .with_static_limit_subject(
        LimitEngine::new(Arc::new(KeyConcurrencyManager::new())),
        "principal-test".to_owned(),
        "key-test".to_owned(),
        active_record(),
    );

    let response = lifecycle
        .handle(messages_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[],"max_tokens":16}"#,
        )))
        .await?;
    let (status, _headers, _body) = collect_body(response).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        state
            .router_candidates
            .lock()
            .expect("router candidates lock")
            .as_slice(),
        &[vec![second]],
    );
    let event = single_request_event(storage.as_ref()).await?;
    assert!(event.internal_errors.iter().any(|error| {
        error.stage == InternalErrorStage::RouterFilter
            && error.kind == InternalErrorKind::InvalidOutput
            && error.message.as_deref() == Some("per_candidate_reasons sanitized")
    }));
    Ok(())
}

struct MissingReasonFilter {
    kept: Uuid,
}

impl FilterPlugin for MissingReasonFilter {
    fn filter(
        &self,
        _ctx: &RequestContext,
        _principal: &Principal,
        _candidates: &[UpstreamCandidate],
    ) -> Result<FilterOutput, FilterError> {
        Ok(FilterOutput {
            kept_upstream_ids: vec![self.kept],
            reason: "kept second with incomplete reasons".to_owned(),
            per_candidate_reasons: vec![PerCandidateReason {
                upstream_id: self.kept,
                kept: true,
                reason: "kept".to_owned(),
            }],
        })
    }

    fn plugin_id(&self) -> Uuid {
        Uuid::from_u128(0x0000_0000_0000_0000_0000_0000_0000_3001)
    }

    fn plugin_name(&self) -> &str {
        "missing-reason-filter"
    }
}

fn request_storage(seed: u8) -> Result<Arc<RedbStorage>, Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("lifecycle-filter-validation.redb");
    let storage = Arc::new(RedbStorage::open(&path, [seed; 32])?);
    Box::leak(Box::new(dir));
    Ok(storage)
}

fn active_record() -> StoredApiKeyRecord {
    StoredApiKeyRecord {
        key_hash_b64: "key-test".to_owned(),
        status: KeyStatus::Active,
        ..StoredApiKeyRecord::default()
    }
}

async fn single_request_event(
    storage: &RedbStorage,
) -> Result<cc_lb_storage_api::RequestEvent, Box<dyn std::error::Error>> {
    let events = RequestEventStore::query_request_events(storage, 0, u64::MAX, 10).await?;
    assert_eq!(events.len(), 1);
    Ok(events.into_iter().next().expect("one request event"))
}

fn upstream_id(index: u128) -> Uuid {
    Uuid::from_u128(index)
}
