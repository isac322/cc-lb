mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use cc_lb_core::{
    Body, BreakerConfig, BreakerRegistry, BreakerState, CircuitBreakerDispatch, DispatchError,
    Lifecycle, LifecycleConfig, UpstreamDispatch,
};
use cc_lb_plugin_api::SignedRequest;
use http::{Response, StatusCode};
use url::Url;

use common::{RecordingHook, TestAuthn, TestRouter, TestState, collect_body, messages_request};

#[tokio::test]
async fn bedrock_failure_does_not_open_anthropic_direct_breaker()
-> Result<(), Box<dyn std::error::Error>> {
    let registry = Arc::new(BreakerRegistry::new());
    let inner = Arc::new(HostDispatch::default());
    let dispatch = Arc::new(CircuitBreakerDispatch::new(
        inner.clone(),
        registry.clone(),
        BreakerConfig {
            failures_to_open: 5,
            failure_window: Duration::from_secs(10),
            half_open_after: Duration::from_secs(30),
            half_open_max_in_flight: 1,
        },
        Arc::new(|request: &SignedRequest| {
            request.url().host_str().unwrap_or("unknown").to_owned()
        }),
    ));
    let anthropic = lifecycle_for("http://anthropic-direct.local/", dispatch.clone())?;
    let bedrock = lifecycle_for("http://bedrock.local/", dispatch)?;

    let (status, _, _) = collect_body(
        anthropic
            .handle(messages_request(Bytes::from_static(
                br#"{"model":"claude-test","messages":[]}"#,
            )))
            .await?,
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    for _ in 0..50 {
        let response = bedrock
            .handle(messages_request(Bytes::from_static(
                br#"{"model":"claude-test","messages":[]}"#,
            )))
            .await?;
        let (status, _, _) = collect_body(response).await;
        assert!(status == StatusCode::INTERNAL_SERVER_ERROR || status == StatusCode::BAD_GATEWAY);
    }

    let (status, _, _) = collect_body(
        anthropic
            .handle(messages_request(Bytes::from_static(
                br#"{"model":"claude-test","messages":[]}"#,
            )))
            .await?,
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let bedrock_breaker = registry
        .get("bedrock.local")
        .expect("bedrock breaker exists");
    let anthropic_breaker = registry
        .get("anthropic-direct.local")
        .expect("anthropic-direct breaker exists");
    println!(
        "gauge_transition upstream=bedrock.local state={:?} value={}",
        bedrock_breaker.current_state(),
        bedrock_breaker.current_state().gauge_value()
    );
    println!(
        "gauge_transition upstream=anthropic-direct.local state={:?} value={}",
        anthropic_breaker.current_state(),
        anthropic_breaker.current_state().gauge_value()
    );
    println!(
        "bedrock_inner_calls={} anthropic_direct_inner_calls={}",
        inner.bedrock_calls.load(Ordering::SeqCst),
        inner.anthropic_direct_calls.load(Ordering::SeqCst)
    );

    assert_eq!(bedrock_breaker.current_state(), BreakerState::Open);
    assert_eq!(anthropic_breaker.current_state(), BreakerState::Closed);
    assert_eq!(inner.bedrock_calls.load(Ordering::SeqCst), 5);
    assert_eq!(inner.anthropic_direct_calls.load(Ordering::SeqCst), 2);
    Ok(())
}

fn lifecycle_for(
    base_url: &str,
    dispatch: Arc<dyn UpstreamDispatch>,
) -> Result<Lifecycle, Box<dyn std::error::Error>> {
    Ok(Lifecycle::new(
        Arc::new(TestAuthn::new(TestState::default())),
        Arc::new(TestRouter {
            base_url: Url::parse(base_url)?,
        }),
        dispatch,
        vec![Arc::new(RecordingHook::default())],
        LifecycleConfig::default(),
    ))
}

#[derive(Default)]
struct HostDispatch {
    bedrock_calls: AtomicU32,
    anthropic_direct_calls: AtomicU32,
}

#[async_trait]
impl UpstreamDispatch for HostDispatch {
    async fn dispatch(&self, request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        match request.url().host_str().unwrap_or("unknown") {
            "bedrock.local" => {
                let call = self.bedrock_calls.fetch_add(1, Ordering::SeqCst) + 1;
                println!("bedrock_inner_call={call} status=500");
                Ok(response(StatusCode::INTERNAL_SERVER_ERROR))
            }
            "anthropic-direct.local" => {
                let call = self.anthropic_direct_calls.fetch_add(1, Ordering::SeqCst) + 1;
                println!("anthropic_direct_inner_call={call} status=200");
                Ok(response(StatusCode::OK))
            }
            host => Err(DispatchError::Transport {
                reason: format!("unexpected host {host}"),
            }),
        }
    }
}

fn response(status: StatusCode) -> Response<Body> {
    let body = if status.is_success() {
        Bytes::from_static(br#"{"type":"message","usage":{"input_tokens":1,"output_tokens":1}}"#)
    } else {
        Bytes::from_static(br#"{"type":"error","error":{"type":"api_error","message":"forced"}}"#)
    };
    let mut response = Response::new(Body::from(body));
    *response.status_mut() = status;
    response
}
