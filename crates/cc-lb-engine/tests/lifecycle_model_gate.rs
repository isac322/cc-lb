use crate::common;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use cc_lb_engine::api_keys::concurrent_guard::KeyConcurrencyManager;
use cc_lb_engine::api_keys::limit_engine::LimitEngine;
use cc_lb_engine::api_keys::principal_view::PrincipalView;
use cc_lb_storage_api::types::{KeyStatus, StoredApiKeyRecord};
use http::StatusCode;

use common::{
    DispatchMode, MockDispatch, RecordingHook, TestAuthn, TestState, collect_body, lifecycle_with,
    messages_request,
};

fn engine_with_allowed_models(
    allowed_models: Vec<String>,
) -> (Arc<LimitEngine>, Arc<PrincipalView>) {
    let view = Arc::new(PrincipalView::for_tests(
        "principal-test",
        true,
        allowed_models,
        Vec::new(),
        HashMap::new(),
    ));
    (
        LimitEngine::new(
            Arc::new(KeyConcurrencyManager::new()),
            Arc::new(cc_lb_engine::SystemClock),
        ),
        view,
    )
}

fn active_record() -> StoredApiKeyRecord {
    StoredApiKeyRecord {
        key_hash_b64: "key-test".to_owned(),
        status: KeyStatus::Active,
        ..StoredApiKeyRecord::default()
    }
}

#[tokio::test]
async fn model_gate_rejects_disallowed_model_before_upstream() {
    let state = TestState::default();
    let hook = Arc::new(RecordingHook::default());
    let (limit_engine, view) = engine_with_allowed_models(vec!["allowed-model".to_owned()]);
    let lifecycle = lifecycle_with(
        TestAuthn::with_principal_view(state.clone(), view),
        MockDispatch {
            state: state.clone(),
            mode: DispatchMode::StreamingOk,
        },
        hook,
    )
    .with_static_limit_subject(
        limit_engine,
        "principal-test".to_owned(),
        "key-test".to_owned(),
        active_record(),
    );

    let request = messages_request(Bytes::from_static(
        br#"{"model":"forbidden-model","messages":[]}"#,
    ));
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("test request authenticates");
    let response = lifecycle
        .handle(request, &auth)
        .await
        .expect("lifecycle handles request");
    let (status, _headers, body) = collect_body(response).await;

    assert_eq!(status, StatusCode::FORBIDDEN);
    assert!(String::from_utf8_lossy(&body).contains("not allowed"));
    assert_eq!(state.upstream_calls.load(Ordering::Relaxed), 0);
}
