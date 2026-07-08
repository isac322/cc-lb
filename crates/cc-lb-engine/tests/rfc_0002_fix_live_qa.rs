#![allow(deprecated)]

mod common;

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use cc_lb_engine::api_keys::principal_view::{
    DialectCache, ObservabilityHooksCache, PrincipalView, ResponseTransformCache,
    RouterPipelineCache, SseEventTransformCache,
};
use cc_lb_engine::{DynamicViewBuilder, DynamicViewHolder, Lifecycle, LifecycleConfig};
use cc_lb_plugin_api::{
    FilterError, FilterOutput, FilterPlugin, InternalErrorKind, InternalErrorStage, Principal,
    RequestContext, RouteDecision, RouteError, RouterPlugin, TerminalStrategy, Upstream,
    UpstreamCandidate,
};
use cc_lb_storage_api::upstream::{UpstreamKind as StorageUpstreamKind, UpstreamRecord};
use cc_lb_storage_api::{BackendKind, MetaStore, RequestEventStore, Storage as StorageTrait};
use cc_lb_storage_sqlite::SqliteStorage;
use http::StatusCode;
use serde_json::Value;
use url::Url;
use uuid::Uuid;

use common::{
    DispatchMode, MockDispatch, TestAuthn, TestLifecycleBus, TestState, collect_body,
    messages_request,
};

#[tokio::test]
async fn lqa_5a_drop_all_filter_returns_503_and_logs_routing_trace()
-> Result<(), Box<dyn std::error::Error>> {
    let upstream_id = Uuid::from_u128(1);
    let filter_calls = Arc::new(Mutex::new(Vec::new()));
    let router_calls = Arc::new(Mutex::new(Vec::new()));
    let state = TestState::default();
    let dir = tempfile::tempdir()?;
    let storage = Arc::new(sqlite_storage(&dir, "rfc-0002-live-qa.sqlite").await?);
    let test_bus = TestLifecycleBus::new().with_assembler(storage.clone() as Arc<dyn StorageTrait>);
    let lifecycle = lifecycle_with_pipeline(
        vec![Arc::new(KeepFilter {
            kept_upstream_ids: Vec::new(),
            calls: Arc::clone(&filter_calls),
        })],
        vec![upstream_record(upstream_id, "first")],
        Arc::new(RecordingRouter {
            calls: Arc::clone(&router_calls),
            selected_id: Some(upstream_id),
        }),
        state.clone(),
    )
    .with_event_bus(test_bus.bus_arc());

    let response = lifecycle
        .handle(messages_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[],"max_tokens":16}"#,
        )))
        .await?;
    let (status, _headers, body) = collect_body(response).await;

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    let body: Value = serde_json::from_slice(&body)?;
    assert_eq!(body["error"]["type"], "route_no_upstream_after_filter");
    assert_eq!(state.upstream_calls.load(Ordering::Relaxed), 0);
    assert_eq!(
        filter_calls.lock().expect("filter calls lock").as_slice(),
        &[vec![upstream_id]]
    );
    assert!(router_calls.lock().expect("router calls lock").is_empty());

    let events = wait_for_events(storage.as_ref(), 1).await?;
    let event = &events[0];
    assert_eq!(event.status, StatusCode::SERVICE_UNAVAILABLE.as_u16());
    assert_eq!(
        event.error_code.as_deref(),
        Some("route_no_upstream_after_filter")
    );
    let trace = event.routing_trace.as_ref().expect("routing trace logged");
    assert_eq!(trace.stages[0].stage_name, "drop-all");
    assert!(event.internal_errors.iter().any(|error| {
        error.stage == InternalErrorStage::RouterFilter
            && error.kind == InternalErrorKind::Unavailable
            && error.message.as_deref()
                == Some("no upstream candidates remain after routing filters")
    }));
    Ok(())
}

async fn wait_for_events(
    storage: &dyn RequestEventStore,
    expected: usize,
) -> Result<Vec<cc_lb_storage_api::types::RequestEvent>, Box<dyn std::error::Error>> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
    loop {
        let events = RequestEventStore::query_request_events(storage, 0, u64::MAX, 10).await?;
        if events.len() >= expected {
            return Ok(events);
        }
        if std::time::Instant::now() >= deadline {
            panic!("expected {expected} request event(s), got {}", events.len());
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
}

async fn sqlite_storage(
    dir: &tempfile::TempDir,
    file_name: &str,
) -> Result<SqliteStorage, Box<dyn std::error::Error>> {
    let database_url = format!("sqlite://{}", dir.path().join(file_name).display());
    let storage =
        cc_lb_storage_sqlite::open_sqlite(&database_url, Arc::new(cc_lb_engine::SystemClock))
            .await?;
    storage.initialize(BackendKind::Sqlite).await?;
    Ok(storage)
}

fn lifecycle_with_pipeline(
    filters: Vec<Arc<dyn FilterPlugin>>,
    records: Vec<UpstreamRecord>,
    router: Arc<dyn RouterPlugin>,
    state: TestState,
) -> Lifecycle {
    let authn = TestAuthn::with_principal_view(state.clone(), principal_view(filters));
    let dispatcher = Arc::new(MockDispatch {
        state,
        mode: DispatchMode::Statuses(Arc::new(Mutex::new(vec![StatusCode::OK].into()))),
    });
    let view = DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(authn.clone()))
        .global_router(router)
        .global_observability_hooks(Vec::new())
        .principal_view(authn.principal_view.clone())
        .upstream_records(records)
        .build();
    Lifecycle::new_with_dynamic_view(
        authn.authn.clone(),
        Arc::new(DynamicViewHolder::new(view)),
        dispatcher,
        LifecycleConfig::default(),
        Arc::new(cc_lb_engine::SystemClock),
    )
}

fn principal_view(filters: Vec<Arc<dyn FilterPlugin>>) -> Arc<PrincipalView> {
    let pipeline = Arc::new(RouterPipelineCache {
        user_filters: filters,
        terminal: TerminalStrategy::FirstPick,
        instantiation_error: None,
    });
    let mut chains = HashMap::new();
    chains.insert(
        "principal-test".to_owned(),
        (
            Some(pipeline),
            ObservabilityHooksCache::Inherit,
            DialectCache::Inherit,
            ResponseTransformCache::None,
            SseEventTransformCache::None,
        ),
    );
    Arc::new(PrincipalView::for_tests(
        "principal-test",
        true,
        vec!["*".to_owned()],
        Vec::new(),
        chains,
    ))
}

fn upstream_record(id: Uuid, name: &str) -> UpstreamRecord {
    UpstreamRecord {
        id,
        name: name.to_owned(),
        kind: StorageUpstreamKind::AnthropicApiKey,
        base_url: Some(Url::parse("http://upstream.local/").expect("test URL parses")),
        enabled: true,
        oauth_credentials: None,
        api_key_ciphertext: Some(Vec::new()),
        last_apply_error: None,
        last_apply_at_unix_secs: None,
        deleted_at_unix_secs: None,
        revision: 1,
        oauth_token_generation: 0,
        created_at_unix_secs: 0,
        updated_at_unix_secs: 0,
        warmup_enabled: false,
        warmup_dialect_plugin: None,
        last_warmup_at_unix_secs: None,
    }
}

struct RecordingRouter {
    calls: Arc<Mutex<Vec<Vec<Uuid>>>>,
    selected_id: Option<Uuid>,
}

impl RouterPlugin for RecordingRouter {
    fn route(
        &self,
        _ctx: &RequestContext,
        _principal: &Principal,
        candidates: &[UpstreamCandidate],
    ) -> Result<RouteDecision, RouteError> {
        self.calls.lock().expect("router calls lock").push(
            candidates
                .iter()
                .map(|candidate| candidate.upstream_id)
                .collect(),
        );
        Ok(RouteDecision {
            upstream_id: self.selected_id,
            upstream: Upstream::AnthropicDirect { base_url: None },
            dialect: Arc::new(common::PassthroughDialect {
                base_url: Url::parse("http://upstream.local/").expect("test URL parses"),
            }),
        })
    }
}

struct KeepFilter {
    kept_upstream_ids: Vec<Uuid>,
    calls: Arc<Mutex<Vec<Vec<Uuid>>>>,
}

impl FilterPlugin for KeepFilter {
    fn filter(
        &self,
        _ctx: &RequestContext,
        _principal: &Principal,
        candidates: &[UpstreamCandidate],
    ) -> Result<FilterOutput, FilterError> {
        self.calls.lock().expect("filter calls lock").push(
            candidates
                .iter()
                .map(|candidate| candidate.upstream_id)
                .collect(),
        );
        Ok(FilterOutput {
            kept_upstream_ids: self.kept_upstream_ids.clone(),
            reason: "drop-all removed every candidate".to_owned(),
            per_candidate_reasons: Vec::new(),
            subscription_preference: None,
            cache_affinity: None,
        })
    }

    fn plugin_id(&self) -> Uuid {
        Uuid::nil()
    }

    fn plugin_name(&self) -> &str {
        "drop-all"
    }
}
