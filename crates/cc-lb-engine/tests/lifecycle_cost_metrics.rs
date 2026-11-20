//! #1299: replay provider usage shapes through the real lifecycle, pricing,
//! SQLite assembler, and metrics subscriber. Prices below are fixture rates.
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::body::Body;
use bytes::Bytes;
use cc_lb_control::{DynamicViewBuilder, DynamicViewHolder, RequestEventBus};
use cc_lb_domain::Provider;
use cc_lb_engine::{DispatchError, Lifecycle, LifecycleConfig, ProviderScope, UpstreamDispatch};
use cc_lb_lifecycle::LifecycleEvent;
use cc_lb_request_log::RequestEventUpdate;
use cc_lb_storage_api::upstream::{UpstreamCreate, UpstreamKind};
use cc_lb_storage_api::{MetaStore, PriceCatalogCache, Storage, UpstreamStore};
use cc_lb_upstream::SignedRequest;
use http::{Request, Response, StatusCode};
use metrics_util::debugging::{DebugValue, DebuggingRecorder};

use crate::common::{
    TestAuthn, TestLifecycleBus, TestState, collect_body, managed_api_key, stored_request_events,
};

// Reduced from the real Responses capture for #1299. Provider usage and cache
// detail fields are preserved; response IDs/text are synthetic and non-sensitive.
const OPENAI: &str = r#"{"id":"resp_cost_oracle","object":"response","status":"completed","model":"gpt-6.1-sol","output":[],"usage":{"input_tokens":309,"input_tokens_details":{"cached_tokens":0,"cache_write_tokens":0,"cache_write_tokens_details":{"ttl_12h_tokens":0,"ttl_30m_tokens":0}},"output_tokens":8,"output_tokens_details":{"reasoning_tokens":0},"total_tokens":317}}"#;
// Captured Gemini API generateContent fixture: prompt 13, output 7.
const GEMINI: &str = r#"{"candidates":[{"content":{"role":"model","parts":[{"text":"OK"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":13,"candidatesTokenCount":7,"totalTokenCount":20},"modelVersion":"gemini-3-flash"}"#;
// Existing Messages protocol fixture is the unaffected request/token control.
const ANTHROPIC: &str = r#"{"id":"msg_cost_oracle","type":"message","role":"assistant","model":"claude-test","content":[{"type":"text","text":"OK"}],"stop_reason":"end_turn","usage":{"input_tokens":13,"output_tokens":7,"cache_read_input_tokens":12000}}"#;
const PRICES: &[u8] = br#"{
  "gpt-6.1-sol":{"input_cost_per_token":0.000001,"output_cost_per_token":0.000002,"cache_read_input_token_cost":0.0000005,"litellm_provider":"openai","mode":"chat"},
  "gemini-3-flash":{"input_cost_per_token":0.000001,"output_cost_per_token":0.000002,"litellm_provider":"gemini","mode":"chat"},
  "claude-test":{"input_cost_per_token":0.000001,"output_cost_per_token":0.000002,"cache_read_input_token_cost":0.0000005,"litellm_provider":"anthropic","mode":"chat"}
}"#;

struct WireDispatch {
    body: Bytes,
    streaming: bool,
}
#[async_trait]
impl UpstreamDispatch for WireDispatch {
    async fn dispatch(&self, _request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        Ok(Response::builder()
            .status(200)
            .header(
                "content-type",
                if self.streaming {
                    "text/event-stream"
                } else {
                    "application/json"
                },
            )
            .body(Body::from(self.body.clone()))
            .unwrap())
    }
}

#[tokio::test(flavor = "current_thread")]
async fn captured_provider_costs_match_durable_rows() {
    const CHILD: &str = "CC_LB_COST_METRICS_ORACLE_CHILD";
    if std::env::var_os(CHILD).is_none() {
        // All engine integration modules share one binary. Isolate the global
        // catalog from tests that install unrelated snapshots concurrently.
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "lifecycle_cost_metrics::captured_provider_costs_match_durable_rows",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(CHILD, "1")
            .output()
            .expect("cost oracle child");
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }
    let clock = Arc::new(cc_lb_clock::SystemClock);
    let catalog_storage = Arc::new(
        cc_lb_storage_sqlite::open_sqlite("sqlite::memory:", clock.clone())
            .await
            .unwrap(),
    );
    catalog_storage.initialize().await.unwrap();
    catalog_storage.put_price_snapshot(PRICES, 1).await.unwrap();
    let loader = cc_lb_pricing::LiteLlmLoader::new(
        cc_lb_pricing::global_catalog().clone(),
        catalog_storage,
        "http://unused.invalid/".into(),
        "unused-cost-oracle-cache".into(),
        clock,
    );
    assert!(loader.install_latest_local().await.unwrap());
    let mut failures = Vec::new();
    for (provider, model, input, output, cached, expected_cost) in [
        (Provider::Openai, "gpt-6.1-sol", 309, 8, 0, 325),
        (Provider::Gemini, "gemini-3-flash", 13, 7, 0, 27),
        (Provider::Anthropic, "claude-test", 13, 7, 12000, 6027),
    ] {
        for streaming in [false, true] {
            for priced in [true, false] {
                let actual_model = if priced {
                    model.to_owned()
                } else {
                    format!("{model}-unpriced-oracle")
                };
                let raw = match provider {
                    Provider::Openai => OPENAI,
                    Provider::Gemini => GEMINI,
                    _ => ANTHROPIC,
                }
                .replace(model, &actual_model);
                let wire = if !streaming {
                    raw
                } else {
                    match provider {
                        Provider::Openai => format!(
                            "event: response.completed\ndata: {{\"type\":\"response.completed\",\"response\":{raw}}}\n\n"
                        ),
                        Provider::Gemini => format!("data: {raw}\n\n"),
                        _ => format!(
                            "event: message_start\ndata: {{\"type\":\"message_start\",\"message\":{raw}}}\n\nevent: message_delta\ndata: {{\"type\":\"message_delta\",\"delta\":{{\"stop_reason\":\"end_turn\"}},\"usage\":{{\"output_tokens\":7}}}}\n\nevent: message_stop\ndata: {{\"type\":\"message_stop\"}}\n\n"
                        ),
                    }
                };
                let (cost, virtual_cost) = run_case(
                    provider,
                    &actual_model,
                    streaming,
                    &wire,
                    false,
                    input,
                    output,
                    cached,
                )
                .await;
                let expected = priced.then_some(expected_cost);
                assert_eq!(
                    cost, expected,
                    "durable oracle {provider:?} streaming={streaming} priced={priced}"
                );
                if virtual_cost != expected.map(|n| n as u64) {
                    failures.push(format!("{provider:?} streaming={streaming} priced={priced}: durable={cost:?}, metrics={virtual_cost:?}"));
                }
            }
        }
    }
    let (cost, metric) = run_case(
        Provider::Gemini,
        "gemini-3-flash",
        false,
        r#"{"totalTokens":3}"#,
        true,
        0,
        0,
        0,
    )
    .await;
    assert_eq!((cost, metric), (None, None), "countTokens is unmetered");
    assert!(
        failures.is_empty(),
        "cost mismatches:\n{}",
        failures.join("\n")
    );
}

#[allow(clippy::too_many_arguments)]
async fn run_case(
    provider: Provider,
    model: &str,
    streaming: bool,
    wire: &str,
    count_tokens: bool,
    input: u64,
    output: u64,
    cached: u64,
) -> (Option<i64>, Option<u64>) {
    let recorder = DebuggingRecorder::new();
    let snapshot = recorder.snapshotter();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let storage = Arc::new(
        cc_lb_storage_sqlite::open_sqlite("sqlite::memory:", Arc::new(cc_lb_clock::SystemClock))
            .await
            .unwrap(),
    );
    storage.initialize().await.unwrap();
    let mut bus = TestLifecycleBus::new().with_assembler(storage.clone() as Arc<dyn Storage>);
    let mut updates = bus.bus.subscribe();
    let mut events = bus.bus.subscribe_lifecycle();
    let pricing = cc_lb_pricing::spawn_lifecycle_pricing_subscriber(
        bus.bus.attach_lifecycle_pricing(128),
        bus.bus_arc(),
    );
    let metrics = cc_lb_engine::spawn_lifecycle_api_key_metrics_subscriber(
        bus.bus.attach_lifecycle_api_key_metrics(128),
        bus.bus_arc(),
    );
    let authn = TestAuthn::new(TestState::default());
    let kind = match provider {
        Provider::Openai => UpstreamKind::OpenaiApiKey,
        Provider::Gemini => UpstreamKind::GeminiApiKey,
        _ => UpstreamKind::AnthropicApiKey,
    };
    let upstream = storage
        .create(UpstreamCreate {
            id: uuid::Uuid::from_u128(1299),
            name: "cost-oracle".into(),
            kind,
            base_url: Some("https://provider.example/".parse().unwrap()),
            api_key_ciphertext: Some(Vec::new()),
            oauth_tokens: None,
            warmup_enabled: false,
            warmup_dialect_plugin: None,
            workspace_id: None,
            api_key_access: None,
        })
        .await
        .unwrap();
    let view = DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(authn.clone()))
        .principal_view(authn.principal_view.clone())
        .upstream_records(vec![upstream])
        .build();
    let lifecycle = Lifecycle::new_with_dynamic_view(
        authn.authn,
        Arc::new(DynamicViewHolder::new(view)),
        Arc::new(WireDispatch {
            body: Bytes::copy_from_slice(wire.as_bytes()),
            streaming,
        }),
        LifecycleConfig::default(),
        Arc::new(cc_lb_clock::SystemClock),
    )
    .with_event_bus(bus.bus_arc())
    .with_upstream_affinity_store(storage.clone());
    let (path, body) = match provider {
        Provider::Openai => (
            "/v1/responses".to_owned(),
            serde_json::json!({"model":model,"input":"Reply OK","stream":streaming}),
        ),
        Provider::Gemini => (
            format!(
                "/v1beta/models/{model}:{}",
                if count_tokens {
                    "countTokens"
                } else if streaming {
                    "streamGenerateContent"
                } else {
                    "generateContent"
                }
            ),
            serde_json::json!({"contents":[{"role":"user","parts":[{"text":"Reply OK"}]}]}),
        ),
        _ => (
            "/v1/messages".to_owned(),
            serde_json::json!({"model":model,"messages":[{"role":"user","content":"Reply OK"}],"max_tokens":32,"stream":streaming}),
        ),
    };
    let mut request = Request::builder()
        .method("POST")
        .uri(path)
        .header("x-api-key", managed_api_key())
        .header("content-type", "application/json")
        .body(Bytes::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();
    request.extensions_mut().insert(ProviderScope(provider));
    let auth = lifecycle.authenticate(request.headers()).await.unwrap();
    let (status, _, returned) = collect_body(lifecycle.handle(request, &auth).await.unwrap()).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "{provider:?} {model}: {}",
        String::from_utf8_lossy(&returned)
    );
    assert_eq!(
        returned.as_ref(),
        wire.as_bytes(),
        "provider bytes preserved"
    );
    bus.shutdown().await;
    tokio::time::timeout(Duration::from_secs(5), async {
        while !matches!(updates.recv().await.unwrap(), RequestEventUpdate::Final(_)) {}
    })
    .await
    .expect("durable final row");
    pricing.shutdown().await;
    metrics.shutdown().await;
    let rows = stored_request_events(storage.as_ref()).await.unwrap();
    assert_eq!(rows.len(), 1);
    let samples = snapshot.snapshot().into_vec();
    let sum = |name: &str, label: Option<(&str, &str)>| -> Option<u64> {
        let values: Vec<_> = samples
            .iter()
            .filter_map(|(key, _, _, value)| {
                if key.key().name() != name
                    || label.is_some_and(|(name, value)| {
                        !key.key()
                            .labels()
                            .any(|l| l.key() == name && l.value() == value)
                    })
                {
                    return None;
                }
                match value {
                    DebugValue::Counter(n) => Some(*n),
                    _ => None,
                }
            })
            .collect();
        (!values.is_empty()).then(|| values.into_iter().sum())
    };
    assert_eq!(sum("cclb_api_key_requests_total", None), Some(1));
    if !count_tokens {
        assert_eq!(rows[0].input_tokens, Some(input));
        assert_eq!(rows[0].output_tokens, Some(output));
        assert_eq!(
            sum("cc_lb_tokens_total", Some(("direction", "input"))),
            Some(input)
        );
        assert_eq!(
            sum("cc_lb_tokens_total", Some(("direction", "output"))),
            Some(output)
        );
        assert_eq!(
            sum("cclb_api_key_tokens_total", Some(("kind", "cache_read"))),
            (cached > 0).then_some(cached)
        );
        let mut terminal_seen = false;
        let mut price_seen = false;
        while let Ok(event) = events.try_recv() {
            match event {
                LifecycleEvent::RequestTerminated { .. } => terminal_seen = true,
                LifecycleEvent::Priced { .. } => {
                    assert!(terminal_seen, "real derived price follows terminal");
                    price_seen = true;
                }
                _ => {}
            }
        }
        assert!(price_seen);
    } else {
        assert_eq!(sum("cc_lb_tokens_total", None), None);
    }
    if provider == Provider::Anthropic {
        println!("Anthropic request/token control passed, streaming={streaming}");
    }
    let virtual_cost = sum("cc_lb_virtual_cost_usd_total", None);
    assert_eq!(
        sum("cclb_api_key_cost_usd_micro_total", None),
        virtual_cost.filter(|n| *n > 0)
    );
    (rows[0].cost_usd_micros, virtual_cost)
}
