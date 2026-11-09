use crate::common;

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use cc_lb_control::api_keys::principal_view::{
    DialectCache, PrincipalView, ProviderFilterChains, RouterPipelineCache,
};
use cc_lb_control::{DynamicViewBuilder, DynamicViewHolder, RequestEventBus};
use cc_lb_domain::{Principal, Provider, TerminalStrategy, UpstreamCandidate};
use cc_lb_engine::realtime_websocket::{RealtimeWebSocketKind, RealtimeWebSocketTarget};
use cc_lb_engine::{Lifecycle, LifecycleConfig};
use cc_lb_routing::{FilterError, FilterOutput, FilterPlugin};
use cc_lb_storage_api::upstream::{UpstreamKind as StorageUpstreamKind, UpstreamRecord};
use http::{HeaderValue, StatusCode};
use parking_lot::Mutex as ParkingMutex;
use url::Url;
use uuid::Uuid;

use common::{DispatchMode, MockDispatch, TestAuthn, TestState, collect_body, messages_request};

#[tokio::test]
async fn pipeline_filters_candidates_before_terminal_strategy()
-> Result<(), Box<dyn std::error::Error>> {
    let upstream_id = default_upstream_id();
    let filter_calls = Arc::new(Mutex::new(Vec::new()));
    let state = TestState::default();
    let filters: Vec<Arc<dyn FilterPlugin>> = vec![Arc::new(RecordingFilter {
        name: "keep-default",
        calls: filter_calls.clone(),
        kept_upstream_ids: vec![upstream_id],
    })];
    let lifecycle = lifecycle_with_pipeline(filters, state.clone());

    let request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[]}"#,
    ));
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("test request authenticates");
    let response = lifecycle.handle(request, &auth).await?;
    let (status, _headers, _body) = collect_body(response).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(state.upstream_calls.load(Ordering::Relaxed), 1);
    assert_eq!(
        filter_calls.lock().unwrap().as_slice(),
        &[vec![upstream_id]]
    );
    Ok(())
}

#[tokio::test]
async fn request_model_reaches_filters_without_prompt_cache_shadow()
-> Result<(), Box<dyn std::error::Error>> {
    let upstream_id = default_upstream_id();
    let canonical_models = Arc::new(ParkingMutex::new(Vec::new()));
    let state = TestState::default();
    let filters: Vec<Arc<dyn FilterPlugin>> = vec![Arc::new(RecordingModelFilter {
        canonical_models: canonical_models.clone(),
        kept_upstream_ids: vec![upstream_id],
    })];
    let lifecycle = lifecycle_with_pipeline(filters, state);

    let request = messages_request(Bytes::from_static(
        br#"{"model":"claude-fable-5","messages":[]}"#,
    ));
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("test request authenticates");
    let response = lifecycle.handle(request, &auth).await?;
    let (status, _headers, _body) = collect_body(response).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(canonical_models.lock().as_slice(), &["claude-fable-5"]);
    Ok(())
}

#[tokio::test]
async fn trap_and_runtime_errors_pass_candidates_through() -> Result<(), Box<dyn std::error::Error>>
{
    for error_kind in [FilterErrorKind::Trap, FilterErrorKind::Runtime] {
        let upstream_id = default_upstream_id();
        let error_filter_calls = Arc::new(Mutex::new(Vec::new()));
        let keep_filter_calls = Arc::new(Mutex::new(Vec::new()));
        let state = TestState::default();
        let filters: Vec<Arc<dyn FilterPlugin>> = vec![
            Arc::new(ErrorFilter {
                name: error_kind.stage_name(),
                calls: error_filter_calls.clone(),
                error_kind,
            }),
            Arc::new(RecordingFilter {
                name: "after-error",
                calls: keep_filter_calls.clone(),
                kept_upstream_ids: vec![upstream_id],
            }),
        ];
        let lifecycle = lifecycle_with_pipeline(filters, state.clone());

        let request = messages_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[]}"#,
        ));
        let auth = lifecycle
            .authenticate(request.headers())
            .await
            .expect("test request authenticates");
        let response = lifecycle.handle(request, &auth).await?;
        let (status, _headers, _body) = collect_body(response).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(state.upstream_calls.load(Ordering::Relaxed), 1);
        assert_eq!(
            error_filter_calls.lock().unwrap().as_slice(),
            &[vec![upstream_id]]
        );
        assert_eq!(
            keep_filter_calls.lock().unwrap().as_slice(),
            &[vec![upstream_id]]
        );
    }
    Ok(())
}

#[tokio::test]
async fn empty_stage_output_propagates_to_later_stages_and_terminal_strategy()
-> Result<(), Box<dyn std::error::Error>> {
    let upstream_id = default_upstream_id();
    let empty_filter_calls = Arc::new(Mutex::new(Vec::new()));
    let later_filter_calls = Arc::new(Mutex::new(Vec::new()));
    let state = TestState::default();
    let filters: Vec<Arc<dyn FilterPlugin>> = vec![
        Arc::new(RecordingFilter {
            name: "empty",
            calls: empty_filter_calls.clone(),
            kept_upstream_ids: Vec::new(),
        }),
        Arc::new(RecordingFilter {
            name: "later",
            calls: later_filter_calls.clone(),
            kept_upstream_ids: Vec::new(),
        }),
    ];
    let lifecycle = lifecycle_with_pipeline(filters, state.clone());

    let request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[]}"#,
    ));
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("test request authenticates");
    let response = lifecycle.handle(request, &auth).await?;
    let (status, _headers, _body) = collect_body(response).await;

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(state.upstream_calls.load(Ordering::Relaxed), 0);
    assert_eq!(
        empty_filter_calls.lock().unwrap().as_slice(),
        &[vec![upstream_id]]
    );
    assert_eq!(
        later_filter_calls.lock().unwrap().as_slice(),
        &[Vec::<Uuid>::new()]
    );
    Ok(())
}

#[tokio::test]
async fn realtime_provider_filters_see_bounded_precedence_identity_without_claude_ancestry()
-> Result<(), Box<dyn std::error::Error>> {
    let openai_id = Uuid::from_u128(2);
    let gemini_id = Uuid::from_u128(3);
    let openai_threads = Arc::new(Mutex::new(Vec::new()));
    let gemini_threads = Arc::new(Mutex::new(Vec::new()));
    let openai_filter = Arc::new(RecordingIdentityFilter {
        name: "openai-identity",
        threads: Arc::clone(&openai_threads),
        kept_upstream_ids: vec![openai_id],
    });
    let gemini_filter = Arc::new(RecordingIdentityFilter {
        name: "gemini-identity",
        threads: Arc::clone(&gemini_threads),
        kept_upstream_ids: vec![gemini_id],
    });
    let principal_view =
        principal_view_with_provider_filters(vec![openai_filter], vec![gemini_filter]);
    let authn = TestAuthn::with_principal_view(TestState::default(), principal_view.clone());
    let test_bus = common::TestLifecycleBus::new();
    let mut lifecycle_events = test_bus.bus.subscribe_lifecycle();
    let view = DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(authn.clone()))
        .principal_view(principal_view)
        .upstream_records(vec![
            provider_upstream_record(
                openai_id,
                "openai-realtime",
                StorageUpstreamKind::OpenaiApiKey,
            ),
            provider_upstream_record(gemini_id, "gemini-live", StorageUpstreamKind::GeminiApiKey),
        ])
        .build();
    let lifecycle = Lifecycle::new_with_dynamic_view(
        authn.authn.clone(),
        Arc::new(DynamicViewHolder::new(view)),
        Arc::new(MockDispatch {
            state: authn.state.clone(),
            mode: DispatchMode::Statuses(Arc::new(Mutex::new(vec![StatusCode::OK].into()))),
        }),
        LifecycleConfig::default(),
        Arc::new(cc_lb_clock::SystemClock),
    )
    .with_event_bus(test_bus.bus_arc());
    let request = messages_request(Bytes::new());
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("websocket setup authenticates");
    let oversized = "x".repeat(513);

    for (kind, path, model, expected_provider, expected_threads) in [
        (
            RealtimeWebSocketKind::Openai,
            "/v1/realtime",
            "gpt-realtime",
            Provider::Openai,
            vec!["openai-top".to_owned(), "openai-fallback".to_owned()],
        ),
        (
            RealtimeWebSocketKind::Gemini,
            "/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent",
            "gemini-live",
            Provider::Gemini,
            vec!["gemini-top".to_owned(), "gemini-fallback".to_owned()],
        ),
    ] {
        for (top, fallback, expected_thread) in [
            (
                expected_threads[0].clone(),
                "lower-priority-session".to_owned(),
                expected_threads[0].clone(),
            ),
            (
                oversized.clone(),
                expected_threads[1].clone(),
                expected_threads[1].clone(),
            ),
        ] {
            let mut headers = request.headers().clone();
            headers.insert(
                "x-claude-code-session-id",
                HeaderValue::from_str(&top).expect("identity header"),
            );
            headers.insert(
                "x-claude-session-id",
                HeaderValue::from_str(&fallback).expect("fallback identity header"),
            );
            headers.insert(
                "x-session-affinity",
                HeaderValue::from_static("lower-priority-affinity"),
            );
            headers.insert("x-session-id", HeaderValue::from_static("lowest-priority"));
            headers.insert(
                "x-claude-code-agent-id",
                HeaderValue::from_static("must-not-become-live-ancestry"),
            );
            headers.insert(
                "x-claude-code-parent-agent-id",
                HeaderValue::from_static("must-not-become-live-parent"),
            );

            let observation = lifecycle.begin_realtime_websocket(&auth, path);
            let result = lifecycle
                .prepare_realtime_websocket(
                    &auth,
                    observation,
                    kind,
                    &headers,
                    RealtimeWebSocketTarget {
                        path,
                        query: None,
                        model: Some(model),
                    },
                    Bytes::from_static(br#"{"setup":{"type":"realtime"}}"#),
                )
                .await;
            assert!(
                result.is_ok(),
                "{expected_provider:?} setup should route after filtering"
            );

            let parse = loop {
                let event = tokio::time::timeout(Duration::from_secs(1), lifecycle_events.recv())
                    .await
                    .expect("setup lifecycle event arrives")
                    .expect("lifecycle event stream remains open");
                if let cc_lb_lifecycle::LifecycleEvent::ParseCompleted {
                    result: Ok(info), ..
                } = event
                {
                    break info;
                }
            };
            assert_eq!(parse.thread_id.as_deref(), Some(expected_thread.as_str()));
            assert_eq!(parse.claude_agent_id, None);
            assert_eq!(parse.claude_parent_agent_id, None);
            assert_eq!(parse.parent_session_id, None);
        }
    }

    assert_eq!(
        openai_threads.lock().unwrap().as_slice(),
        ["openai-top", "openai-fallback"]
    );
    assert_eq!(
        gemini_threads.lock().unwrap().as_slice(),
        ["gemini-top", "gemini-fallback"]
    );
    Ok(())
}

fn lifecycle_with_pipeline(filters: Vec<Arc<dyn FilterPlugin>>, state: TestState) -> Lifecycle {
    let principal_view = principal_view(filters);
    let authn = TestAuthn::with_principal_view(state.clone(), principal_view.clone());
    let dispatcher = Arc::new(MockDispatch {
        state,
        mode: DispatchMode::Statuses(Arc::new(Mutex::new(vec![StatusCode::OK].into()))),
    });
    let view = DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(authn.clone()))
        .principal_view(principal_view)
        .upstream_records(vec![test_upstream_record()])
        .build();
    Lifecycle::new_with_dynamic_view(
        authn.authn.clone(),
        Arc::new(DynamicViewHolder::new(view)),
        dispatcher,
        LifecycleConfig::default(),
        Arc::new(cc_lb_clock::SystemClock),
    )
}

fn principal_view(filters: Vec<Arc<dyn FilterPlugin>>) -> Arc<PrincipalView> {
    let pipeline = Arc::new(RouterPipelineCache {
        user_filters: filters,
        terminal: TerminalStrategy::FirstPick,
        instantiation_error: None,
        provider_filters: None,
    });
    let mut chains = HashMap::new();
    chains.insert(
        "principal-test".to_owned(),
        (Some(pipeline), DialectCache::Inherit),
    );
    Arc::new(PrincipalView::for_tests(
        "principal-test",
        true,
        vec!["*".to_owned()],
        Vec::new(),
        chains,
    ))
}

fn principal_view_with_provider_filters(
    openai: Vec<Arc<dyn FilterPlugin>>,
    gemini: Vec<Arc<dyn FilterPlugin>>,
) -> Arc<PrincipalView> {
    let pipeline = Arc::new(RouterPipelineCache {
        user_filters: Vec::new(),
        terminal: TerminalStrategy::FirstPick,
        instantiation_error: None,
        provider_filters: Some(ProviderFilterChains {
            anthropic: Vec::new(),
            openai,
            gemini,
        }),
    });
    let mut chains = HashMap::new();
    chains.insert(
        "principal-test".to_owned(),
        (Some(pipeline), DialectCache::Inherit),
    );
    Arc::new(PrincipalView::for_tests(
        "principal-test",
        true,
        vec!["*".to_owned()],
        Vec::new(),
        chains,
    ))
}

fn provider_upstream_record(id: Uuid, name: &str, kind: StorageUpstreamKind) -> UpstreamRecord {
    UpstreamRecord {
        id,
        name: name.to_owned(),
        kind,
        base_url: Some(Url::parse("http://upstream.local/").expect("test URL parses")),
        enabled: true,
        api_key_ciphertext: Some(Vec::new()),
        revision: 1,
        ..UpstreamRecord::default()
    }
}

fn test_upstream_record() -> UpstreamRecord {
    UpstreamRecord {
        id: default_upstream_id(),
        name: "test-upstream".to_owned(),
        kind: StorageUpstreamKind::AnthropicApiKey,
        base_url: Some(Url::parse("http://upstream.local/").expect("test URL parses")),
        enabled: true,
        oauth_credentials: None,
        oauth_never_refresh: false,
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
        workspace_id: None,
    }
}

fn default_upstream_id() -> Uuid {
    Uuid::parse_str("00000000-0000-0000-0000-000000000001").expect("default upstream id parses")
}

struct RecordingIdentityFilter {
    name: &'static str,
    threads: Arc<Mutex<Vec<String>>>,
    kept_upstream_ids: Vec<Uuid>,
}

impl FilterPlugin for RecordingIdentityFilter {
    fn filter(
        &self,
        ctx: &cc_lb_routing::RoutingContext,
        _principal: &Principal,
        _candidates: &[UpstreamCandidate],
    ) -> Result<FilterOutput, FilterError> {
        self.threads.lock().unwrap().push(
            ctx.thread_id
                .clone()
                .expect("thread identity reaches filter"),
        );
        Ok(FilterOutput {
            kept_upstream_ids: self.kept_upstream_ids.clone(),
            reason: format!("{} kept identity", self.name),
            per_candidate_reasons: Vec::new(),
            subscription_preference: None,
        })
    }

    fn plugin_id(&self) -> Uuid {
        Uuid::nil()
    }

    fn plugin_name(&self) -> &str {
        self.name
    }
}

struct RecordingFilter {
    name: &'static str,
    calls: Arc<Mutex<Vec<Vec<Uuid>>>>,
    kept_upstream_ids: Vec<Uuid>,
}

impl FilterPlugin for RecordingFilter {
    fn filter(
        &self,
        _ctx: &cc_lb_routing::RoutingContext,
        _principal: &Principal,
        candidates: &[UpstreamCandidate],
    ) -> Result<FilterOutput, FilterError> {
        self.calls.lock().unwrap().push(
            candidates
                .iter()
                .map(|candidate| candidate.upstream_id)
                .collect(),
        );
        Ok(FilterOutput {
            kept_upstream_ids: self.kept_upstream_ids.clone(),
            reason: format!("{} kept {}", self.name, self.kept_upstream_ids.len()),
            per_candidate_reasons: Vec::new(),
            subscription_preference: None,
        })
    }

    fn plugin_id(&self) -> Uuid {
        Uuid::nil()
    }

    fn plugin_name(&self) -> &str {
        self.name
    }
}
struct RecordingModelFilter {
    canonical_models: Arc<ParkingMutex<Vec<String>>>,
    kept_upstream_ids: Vec<Uuid>,
}

impl FilterPlugin for RecordingModelFilter {
    fn filter(
        &self,
        ctx: &cc_lb_routing::RoutingContext,
        _principal: &Principal,
        _candidates: &[UpstreamCandidate],
    ) -> Result<FilterOutput, FilterError> {
        self.canonical_models
            .lock()
            .push(ctx.canonical_model_id.clone());
        Ok(FilterOutput {
            kept_upstream_ids: self.kept_upstream_ids.clone(),
            reason: "record:model".to_owned(),
            per_candidate_reasons: Vec::new(),
            subscription_preference: None,
        })
    }

    fn plugin_id(&self) -> Uuid {
        Uuid::nil()
    }

    fn plugin_name(&self) -> &str {
        "record-model"
    }
}

struct ErrorFilter {
    name: &'static str,
    calls: Arc<Mutex<Vec<Vec<Uuid>>>>,
    error_kind: FilterErrorKind,
}

impl FilterPlugin for ErrorFilter {
    fn filter(
        &self,
        _ctx: &cc_lb_routing::RoutingContext,
        _principal: &Principal,
        candidates: &[UpstreamCandidate],
    ) -> Result<FilterOutput, FilterError> {
        self.calls.lock().unwrap().push(
            candidates
                .iter()
                .map(|candidate| candidate.upstream_id)
                .collect(),
        );
        match self.error_kind {
            FilterErrorKind::Trap => Err(FilterError::Trap {
                reason: "trap fixture".to_owned(),
            }),
            FilterErrorKind::Runtime => Err(FilterError::Runtime {
                reason: "runtime fixture".to_owned(),
            }),
        }
    }

    fn plugin_id(&self) -> Uuid {
        Uuid::nil()
    }

    fn plugin_name(&self) -> &str {
        self.name
    }
}

#[derive(Clone, Copy)]
enum FilterErrorKind {
    Trap,
    Runtime,
}

impl FilterErrorKind {
    fn stage_name(self) -> &'static str {
        match self {
            Self::Trap => "trap-filter",
            Self::Runtime => "runtime-filter",
        }
    }
}
