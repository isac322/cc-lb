use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use axum::body::Body;
use bytes::Bytes;
use cc_lb_control::{DynamicViewBuilder, DynamicViewHolder, RequestEventBus};
use cc_lb_engine::{BreakerRegistry, DispatchError, Lifecycle, LifecycleConfig, UpstreamDispatch};
use cc_lb_storage_api::upstream::{UpstreamKind as StorageUpstreamKind, UpstreamRecord};
use cc_lb_storage_api::{MetaStore, Storage as StorageTrait};
use cc_lb_storage_sqlite::SqliteStorage;
use cc_lb_upstream::SignedRequest;
use http::{Response, StatusCode};
use http_body_util::BodyExt;

use crate::common::{
    TestAuthn, TestLifecycleBus, TestState, lifecycle_with_parts, managed_api_key, messages_request,
};

#[tokio::test]
async fn openai_image_and_transcription_usage_persist_in_request_events()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let storage = Arc::new(sqlite_storage(&dir).await?);
    let test_bus = TestLifecycleBus::new().with_assembler(storage.clone() as Arc<dyn StorageTrait>);
    let mut update_rx = test_bus.bus.subscribe();
    let state = TestState::default();
    let authn = TestAuthn::new(state);
    let upstream_id = uuid::Uuid::from_u128(1_093_001);
    let view = DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(authn.clone()))
        .principal_view(authn.principal_view.clone())
        .upstream_records(vec![UpstreamRecord {
            id: upstream_id,
            name: "openai-usage-events".to_owned(),
            kind: StorageUpstreamKind::OpenaiApiKey,
            base_url: Some("https://api.openai.com/".parse()?),
            enabled: true,
            api_key_ciphertext: Some(Vec::new()),
            ..UpstreamRecord::default()
        }])
        .build();
    let dispatcher = OpenAiUsageDispatch {
        responses: Arc::new(Mutex::new(VecDeque::from([
            Bytes::from_static(
                br#"{"created":1,"data":[],"usage":{"input_tokens":11,"output_tokens":3,"total_tokens":14}}"#,
            ),
            Bytes::from_static(
                br#"{"text":"transcribed","usage":{"type":"tokens","input_tokens":17,"output_tokens":5,"total_tokens":22}}"#,
            ),
        ]))),
    };
    let lifecycle = Lifecycle::new_with_dynamic_view(
        authn.authn,
        Arc::new(DynamicViewHolder::new(view)),
        Arc::new(dispatcher),
        LifecycleConfig::default(),
        Arc::new(cc_lb_clock::SystemClock),
    )
    .with_event_bus(test_bus.bus_arc());

    for (path, body) in [
        (
            "/v1/images/generations",
            br#"{"model":"gpt-image-1"}"#.as_slice(),
        ),
        (
            "/v1/audio/transcriptions",
            br#"{"model":"whisper-1"}"#.as_slice(),
        ),
    ] {
        let mut request = http::Request::builder()
            .method("POST")
            .uri(path)
            .header("x-api-key", managed_api_key())
            .header("content-type", "application/json")
            .body(Bytes::from_static(body))?;
        request
            .extensions_mut()
            .insert(cc_lb_engine::ProviderScope(cc_lb_domain::Provider::Openai));
        let auth = lifecycle
            .authenticate(request.headers())
            .await
            .expect("OpenAI request authenticates");
        let response = lifecycle.handle(request, &auth).await?;
        assert_eq!(response.status(), StatusCode::OK, "{path} response");
        response.into_body().collect().await?;

        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if matches!(
                    update_rx.recv().await.expect("request update delivered"),
                    cc_lb_request_log::RequestEventUpdate::Final(_)
                ) {
                    break;
                }
            }
        })
        .await
        .expect("OpenAI final request event arrives");
    }

    let events = crate::common::stored_request_events(storage.as_ref()).await?;
    assert_eq!(events.len(), 2);
    assert!(
        events
            .iter()
            .any(|event| event.input_tokens == Some(11) && event.output_tokens == Some(3)),
        "image usage persists in the request event"
    );
    assert!(
        events
            .iter()
            .any(|event| event.input_tokens == Some(17) && event.output_tokens == Some(5)),
        "transcription usage persists in the request event"
    );

    Ok(())
}

#[test]
fn lifecycle_request_events_current_registry_smoke() {
    let _registry = BreakerRegistry::new();
}

#[tokio::test]
async fn realtime_websocket_observers_classify_attempted_routes() {
    let test_bus = TestLifecycleBus::new();
    let mut lifecycle_events = test_bus.bus.subscribe_lifecycle();
    let lifecycle = lifecycle_with_parts(
        TestAuthn::new(TestState::default()),
        Arc::new(CapturingDispatch::default()),
        LifecycleConfig::default(),
    )
    .with_event_bus(test_bus.bus_arc());
    let auth = lifecycle
        .authenticate(messages_request(Bytes::new()).headers())
        .await
        .expect("test request authenticates");

    for (path, expected) in [
        (
            "/v1/realtime",
            cc_lb_request_log::RequestEventKind::OpenaiResponses,
        ),
        (
            "/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent",
            cc_lb_request_log::RequestEventKind::GeminiGenerateContent,
        ),
    ] {
        lifecycle
            .begin_realtime_websocket(&auth, path)
            .finish(StatusCode::SWITCHING_PROTOCOLS);
        let mut saw_started = false;
        let mut saw_terminated = false;
        while !saw_terminated {
            match tokio::time::timeout(Duration::from_secs(1), lifecycle_events.recv())
                .await
                .expect("lifecycle event arrives")
                .expect("lifecycle event stream remains open")
            {
                cc_lb_lifecycle::LifecycleEvent::RequestStarted { event_kind, .. } => {
                    assert_eq!(event_kind, Some(expected));
                    saw_started = true;
                }
                cc_lb_lifecycle::LifecycleEvent::RequestTerminated { .. } => {
                    saw_terminated = true;
                }
                _ => {}
            }
        }
        assert!(saw_started);
    }
}

#[tokio::test]
async fn hermes_root_identity_crosses_proxy_and_persists_without_body_rewrite()
-> Result<(), Box<dyn std::error::Error>> {
    let original_body = Bytes::from_static(
        br#"{"model":"claude-test","session_id":"hermes-root-session","tags":["product=hermes-agent","client=hermes-client-v0.19.0","conversation=hermes-root-session"],"system":[{"type":"text","text":"You are a focused subagent working on a specific delegated task.\n\nYOUR TASK:\nReview the auth change"}],"messages":[{"role":"user","content":"Review the auth change"}],"tools":[{"name":"terminal"}]}"#,
    );
    let dir = tempfile::tempdir()?;
    let storage = Arc::new(sqlite_storage(&dir).await?);
    let test_bus = TestLifecycleBus::new().with_assembler(storage.clone() as Arc<dyn StorageTrait>);
    let mut update_rx = test_bus.bus.subscribe();
    let dispatcher = CapturingDispatch::default();
    let lifecycle = lifecycle_with_parts(
        TestAuthn::new(TestState::default()),
        Arc::new(dispatcher.clone()),
        LifecycleConfig::default(),
    )
    .with_event_bus(test_bus.bus_arc());

    let request = messages_request(original_body.clone());
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("test request authenticates");
    let response = lifecycle.handle(request, &auth).await?;
    assert_eq!(response.status(), StatusCode::OK);
    response.into_body().collect().await?;

    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if matches!(
                update_rx.recv().await.expect("request update delivered"),
                cc_lb_request_log::RequestEventUpdate::Final(_)
            ) {
                break;
            }
        }
    })
    .await
    .expect("final request event arrives");

    assert_eq!(
        dispatcher
            .bodies
            .lock()
            .expect("captured bodies lock")
            .as_slice(),
        &[original_body]
    );
    let events = crate::common::stored_request_events(storage.as_ref()).await?;
    assert_eq!(events.len(), 1);
    let event = &events[0];
    assert_eq!(event.thread_id.as_deref(), Some("hermes-root-session"));
    assert_eq!(
        event.observed_session_id.as_deref(),
        Some("hermes-root-session")
    );
    assert_eq!(event.session_id_source.as_deref(), Some("session_id"));
    assert_eq!(event.request_kind.as_deref(), Some("subagent"));
    assert_eq!(event.parent_session_id, None);
    assert_eq!(event.client_app, None);

    Ok(())
}

#[tokio::test]
async fn aside_main_worker_and_title_identities_persist() -> Result<(), Box<dyn std::error::Error>>
{
    let dir = tempfile::tempdir()?;
    let storage = Arc::new(sqlite_storage(&dir).await?);
    let test_bus = TestLifecycleBus::new().with_assembler(storage.clone() as Arc<dyn StorageTrait>);
    let mut update_rx = test_bus.bus.subscribe();
    let lifecycle = lifecycle_with_parts(
        TestAuthn::new(TestState::default()),
        Arc::new(CapturingDispatch::default()),
        LifecycleConfig::default(),
    )
    .with_event_bus(test_bus.bus_arc());

    for (session_id, system, request_kind, parent_session_id) in [
        (
            "aside-main",
            "You are Aside, a helpful, proactive, and smart AI browser.",
            "main",
            None,
        ),
        (
            "aside:worker:aside-main:default",
            "You are Aside, a helpful, proactive, and smart AI browser.",
            "subagent",
            Some("aside-main"),
        ),
        (
            "aside-main",
            "Generate a brief title that would help the user find this conversation later.",
            "session_title",
            None,
        ),
    ] {
        let body = Bytes::from(serde_json::to_vec(&serde_json::json!({
            "model": "claude-test",
            "system": system,
            "messages": [{"role": "user", "content": "hello"}],
        }))?);
        let mut request = messages_request(body);
        request.headers_mut().insert(
            http::HeaderName::from_static("x-aside-provider-session-id"),
            http::HeaderValue::from_static(session_id),
        );
        let auth = lifecycle
            .authenticate(request.headers())
            .await
            .expect("Aside request authenticates");
        let response = lifecycle.handle(request, &auth).await?;
        assert_eq!(response.status(), StatusCode::OK);
        response.into_body().collect().await?;
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if matches!(
                    update_rx.recv().await.expect("request update delivered"),
                    cc_lb_request_log::RequestEventUpdate::Final(_)
                ) {
                    break;
                }
            }
        })
        .await
        .expect("Aside final request event arrives");

        let events = crate::common::stored_request_events(storage.as_ref()).await?;
        assert!(
            events.iter().any(|event| {
                event.observed_session_id.as_deref() == Some(session_id)
                    && event.thread_id.as_deref() == Some(session_id)
                    && event.parent_session_id.as_deref() == parent_session_id
                    && event.session_id_source.as_deref() == Some("x-aside-provider-session-id")
                    && event.request_kind.as_deref() == Some(request_kind)
            }),
            "{request_kind} identity persists"
        );
    }
    assert_eq!(
        crate::common::stored_request_events(storage.as_ref())
            .await?
            .len(),
        3
    );
    Ok(())
}

#[derive(Clone, Default)]
struct CapturingDispatch {
    bodies: Arc<Mutex<Vec<Bytes>>>,
}

#[async_trait]
impl UpstreamDispatch for CapturingDispatch {
    async fn dispatch(&self, request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        self.bodies
            .lock()
            .expect("captured bodies lock")
            .push(request.body().clone());

        let mut response = Response::new(Body::from(Bytes::from_static(
            br#"{"type":"message","usage":{"input_tokens":1,"output_tokens":1}}"#,
        )));
        *response.status_mut() = StatusCode::OK;
        Ok(response)
    }
}

#[derive(Clone)]
struct OpenAiUsageDispatch {
    responses: Arc<Mutex<VecDeque<Bytes>>>,
}

#[async_trait]
impl UpstreamDispatch for OpenAiUsageDispatch {
    async fn dispatch(&self, _request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        let body = self
            .responses
            .lock()
            .expect("OpenAI response queue lock")
            .pop_front()
            .expect("one mocked response per request");
        let mut response = Response::new(Body::from(body));
        *response.status_mut() = StatusCode::OK;
        response.headers_mut().insert(
            http::header::CONTENT_TYPE,
            http::HeaderValue::from_static("application/json"),
        );
        Ok(response)
    }
}

async fn sqlite_storage(
    dir: &tempfile::TempDir,
) -> Result<SqliteStorage, Box<dyn std::error::Error>> {
    let database_url = format!(
        "sqlite://{}",
        dir.path().join("hermes-request-events.sqlite").display()
    );
    let storage =
        cc_lb_storage_sqlite::open_sqlite(&database_url, Arc::new(cc_lb_clock::SystemClock))
            .await?;
    storage.initialize().await?;
    Ok(storage)
}
