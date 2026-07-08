mod common;
#[path = "cache_keepalive_proxy_path/fixtures.rs"]
mod fixtures;
#[path = "cache_keepalive_proxy_path/store.rs"]
mod store;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use cc_lb_engine::api_keys::principal_view::PrincipalView;
use cc_lb_engine::cache_keepalive::{AnthropicKeepaliveDispatcher, KeepaliveScheduler};
use cc_lb_engine::{DynamicViewBuilder, DynamicViewHolder, LifecycleConfig};
use http::{HeaderValue, StatusCode};
use serde_json::Value;
use uuid::Uuid;

use common::{RecordingHook, TestAuthn, TestState, collect_body, messages_request};
use fixtures::{
    AgentTurnDispatch, FirstRouter, RecordingKeepaliveDispatch, RecordingSignerFactory,
    principal_with_keepalive, settle, upstream_record,
};
use store::StaticUpstreamStore;

#[tokio::test(start_paused = true)]
async fn lifecycle_schedules_and_fires_keepalive_through_current_proxy_path() {
    let upstream_id =
        Uuid::parse_str("00000000-0000-0000-0000-0000000000aa").expect("upstream id parses");
    let upstream = upstream_record(upstream_id, "primary", "http://keepalive.local/");
    let principal_view = Arc::new(PrincipalView::from_db(
        &[principal_with_keepalive()],
        std::collections::HashMap::new(),
    ));
    let authn = TestAuthn::with_principal_view(TestState::default(), Arc::clone(&principal_view));
    let signer_log = Arc::new(Mutex::new(Vec::new()));
    let keepalive_dispatch = Arc::new(RecordingKeepaliveDispatch::default());
    let holder = Arc::new(DynamicViewHolder::new(
        DynamicViewBuilder::new(0)
            .signer_factory(Arc::new(RecordingSignerFactory {
                label: "initial",
                calls: Arc::clone(&signer_log),
            }))
            .global_router(Arc::new(FirstRouter))
            .global_observability_hooks(vec![Arc::new(RecordingHook::default())])
            .principal_view(Arc::clone(&principal_view))
            .upstream_records(vec![upstream.clone()])
            .build(),
    ));
    let keepalive_dispatcher = AnthropicKeepaliveDispatcher::new(
        Arc::clone(&holder),
        Arc::new(StaticUpstreamStore { upstream }),
        keepalive_dispatch.clone(),
    )
    .with_timeout(Duration::from_secs(1));
    let scheduler = KeepaliveScheduler::new(Arc::new(keepalive_dispatcher));
    let lifecycle = cc_lb_engine::Lifecycle::new_with_dynamic_view(
        authn.authn.clone(),
        Arc::clone(&holder),
        Arc::new(AgentTurnDispatch),
        LifecycleConfig::default(),
        Arc::new(cc_lb_engine::SystemClock),
    )
    .with_keepalive_scheduler(Arc::clone(&scheduler));

    let mut request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","max_tokens":32,"stream":true,"tool_choice":{"type":"any"},"system":[{"type":"text","text":"cached","cache_control":{"type":"ephemeral"}}],"messages":[{"role":"user","content":"hello"}]}"#,
    ));
    request
        .headers_mut()
        .insert("x-api-key", HeaderValue::from_static("sk-cclb-downstream"));
    request.headers_mut().insert(
        "anthropic-beta",
        HeaderValue::from_static("prompt-caching-2024-07-31"),
    );

    let response = lifecycle.handle(request).await.expect("request succeeds");
    let (status, _headers, _body) = collect_body(response).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(scheduler.active_session_count(), 1);
    assert!(keepalive_dispatch.requests().is_empty());

    holder.store(
        DynamicViewBuilder::from_view(&holder.load())
            .signer_factory(Arc::new(RecordingSignerFactory {
                label: "rotated",
                calls: Arc::clone(&signer_log),
            }))
            .build(),
    );

    settle().await;
    tokio::time::advance(Duration::from_secs(1)).await;
    settle().await;

    let requests = keepalive_dispatch.requests();
    assert_eq!(requests.len(), 1);
    let request = &requests[0];
    assert_eq!(request.url, "http://keepalive.local/v1/messages");
    assert_eq!(request.headers["x-api-key"], "sk-ant-rotated");
    assert_eq!(request.headers["anthropic-version"], "2023-06-01");
    assert_eq!(
        request.headers["anthropic-beta"],
        "prompt-caching-2024-07-31"
    );
    let body: Value = serde_json::from_slice(&request.body).expect("keepalive body is JSON");
    assert_eq!(body["max_tokens"], 0);
    assert!(body.get("stream").is_none());
    assert_eq!(body["tool_choice"]["type"], "auto");
    assert_eq!(body["system"][0]["cache_control"]["type"], "ephemeral");

    let calls = signer_log.lock().expect("signer log lock");
    assert!(calls.iter().any(|call| {
        call.label == "rotated"
            && call.downstream_api_key.is_empty()
            && call.upstream_name == "primary"
    }));
}
