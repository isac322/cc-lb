use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::body::Body;
use bytes::Bytes;
use cc_lb_control::{DynamicViewBuilder, DynamicViewHolder};
use cc_lb_domain::{VertexTarget, VertexUpstreamConfig};
use cc_lb_engine::{
    ApiKeyAwareSignerFactory, DispatchError, Lifecycle, LifecycleConfig, UpstreamDispatch,
};
use cc_lb_storage_api::upstream::{UpstreamCreate, UpstreamKind, UpstreamRecord};
use cc_lb_storage_api::{MetaStore, UpstreamAffinityStore, UpstreamStore};
use cc_lb_storage_sqlite::open_sqlite;
use cc_lb_upstream::{
    RetryDecision, ShapedRequest, SignedRequest, Signer, SignerError, SignerFactory,
    SigningCapability, UpstreamError,
};
use http::header::{CONTENT_ENCODING, CONTENT_TYPE};
use http::{HeaderMap, HeaderValue, Method, Request, Response, StatusCode};
use serde_json::Value;
use url::Url;
use uuid::Uuid;

use crate::common::{TestAuthn, TestState, collect_body, managed_api_key};

#[derive(Clone)]
enum Reply {
    Json(StatusCode, &'static [u8]),
    Bytes(StatusCode, HeaderMap, Bytes),
}

#[derive(Default)]
struct Dispatch {
    replies: Mutex<VecDeque<Reply>>,
    requests: Mutex<Vec<SignedRequest>>,
}

#[async_trait]
impl UpstreamDispatch for Dispatch {
    async fn dispatch(&self, request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        self.requests.lock().expect("request lock").push(request);
        let reply = self
            .replies
            .lock()
            .expect("reply lock")
            .pop_front()
            .unwrap_or(Reply::Json(StatusCode::OK, br#"{}"#));
        let response = match reply {
            Reply::Json(status, body) => Response::builder()
                .status(status)
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(body))
                .expect("JSON response builds"),
            Reply::Bytes(status, headers, body) => {
                let mut response = Response::new(Body::from(body));
                *response.status_mut() = status;
                *response.headers_mut() = headers;
                response
            }
        };
        Ok(response)
    }
}

struct Factory {
    refresh: bool,
}

impl ApiKeyAwareSignerFactory for Factory {
    fn with_router_choice(&self, _router_chosen_upstream_name: String) -> Arc<dyn SignerFactory> {
        Arc::new(Self {
            refresh: self.refresh,
        })
    }
}

#[async_trait]
impl SignerFactory for Factory {
    async fn build(
        &self,
        _upstream: &cc_lb_domain::Upstream,
    ) -> Result<Arc<dyn Signer>, SignerError> {
        Ok(Arc::new(PassSigner {
            refresh: self.refresh,
        }))
    }
}

struct PassSigner {
    refresh: bool,
}

#[async_trait]
impl Signer for PassSigner {
    async fn sign(
        &self,
        shaped: ShapedRequest,
        capability: &mut SigningCapability,
    ) -> Result<SignedRequest, SignerError> {
        Ok(SignedRequest::from_shaped(shaped, capability))
    }

    async fn on_unauthorized(&self, _err: &UpstreamError) -> RetryDecision {
        if self.refresh {
            RetryDecision::Refresh {
                new_signer: Arc::new(Self { refresh: false }),
            }
        } else {
            RetryDecision::Fail
        }
    }
}

fn record(id: u128, kind: UpstreamKind, regional: bool) -> UpstreamRecord {
    UpstreamRecord {
        id: Uuid::from_u128(id),
        name: format!("upstream-{id}"),
        kind,
        base_url: Some(Url::parse("http://vertex.test/").expect("base URL")),
        enabled: true,
        api_key_ciphertext: Some(Vec::new()),
        vertex: Some(VertexUpstreamConfig {
            target: if regional {
                VertexTarget::Regional {
                    project_id: "project-123456".to_owned().into(),
                    location: "us-central1".to_owned().into(),
                }
            } else {
                VertexTarget::Express
            },
            api_version: Default::default(),
        }),
        ..UpstreamRecord::default()
    }
}

fn lifecycle(records: Vec<UpstreamRecord>, dispatch: Arc<Dispatch>, refresh: bool) -> Lifecycle {
    let authn = TestAuthn::new(TestState::default());
    let view = DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(Factory { refresh }))
        .principal_view(authn.principal_view.clone())
        .upstream_records(records)
        .build();
    Lifecycle::new_with_dynamic_view(
        authn.authn,
        Arc::new(DynamicViewHolder::new(view)),
        dispatch,
        LifecycleConfig::default(),
        Arc::new(cc_lb_clock::SystemClock),
    )
}

fn lifecycle_with_store(
    records: Vec<UpstreamRecord>,
    dispatch: Arc<Dispatch>,
    refresh: bool,
    store: Arc<dyn UpstreamAffinityStore>,
) -> Lifecycle {
    lifecycle(records, dispatch, refresh).with_upstream_affinity_store(store)
}

fn gemini_request(method: Method, path: &str, body: &[u8]) -> Request<Bytes> {
    let mut request = Request::builder()
        .method(method)
        .uri(path)
        .header("x-api-key", managed_api_key())
        .body(Bytes::copy_from_slice(body))
        .expect("Gemini request builds");
    request
        .extensions_mut()
        .insert(cc_lb_engine::ProviderScope(cc_lb_domain::Provider::Gemini));
    request
}

async fn send(lifecycle: &Lifecycle, request: Request<Bytes>) -> (StatusCode, HeaderMap, Bytes) {
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("request authenticates");
    collect_body(
        lifecycle
            .handle(request, &auth)
            .await
            .expect("lifecycle handles request"),
    )
    .await
}

#[tokio::test]
async fn qa_elig_2_vertex_only_pool_rejects_unsupported_routes_before_signing() {
    let dispatch = Arc::new(Dispatch::default());
    let lifecycle = lifecycle(
        vec![record(1, UpstreamKind::VertexApiKey, true)],
        dispatch.clone(),
        false,
    );
    let request = gemini_request(
        Method::POST,
        "/v1beta/models/gemini-2.5-flash:embedContent",
        br#"{"content":{"parts":[{"text":"hello"}]}}"#,
    );
    let (status, _headers, _body) = send(&lifecycle, request).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(dispatch.requests.lock().expect("request lock").is_empty());
}

#[tokio::test]
async fn qa_aff_1_and_2_vertex_cached_content_response_is_projected_and_reusable() {
    let temp_dir = tempfile::tempdir().expect("create sqlite tempdir");
    let database_url = format!(
        "sqlite://{}",
        temp_dir.path().join("vertex-affinity.sqlite").display()
    );
    let storage = Arc::new(
        open_sqlite(&database_url, Arc::new(cc_lb_clock::SystemClock))
            .await
            .expect("open sqlite affinity storage"),
    );
    storage
        .initialize()
        .await
        .expect("initialize affinity storage");
    let source = record(2, UpstreamKind::VertexCredential, true);
    let upstream = storage
        .create(UpstreamCreate {
            id: source.id,
            name: source.name,
            kind: source.kind,
            base_url: source.base_url,
            api_key_ciphertext: Some(vec![1]),
            oauth_tokens: None,
            warmup_enabled: false,
            warmup_dialect_plugin: None,
            workspace_id: None,
            vertex: source.vertex,
            vertex_credential_type: Some(cc_lb_domain::VertexCredentialType::ServiceAccount),
            vertex_quota_project_id: None,
            api_key_access: None,
        })
        .await
        .expect("create Vertex upstream");
    let dispatch = Arc::new(Dispatch {
        replies: Mutex::new(VecDeque::from([
            Reply::Json(
                StatusCode::OK,
                br#"{"name":"projects/project-123456/locations/us-central1/cachedContents/cache-1","model":"projects/project-123456/locations/us-central1/publishers/google/models/gemini-2.5-flash","expireTime":"2030-01-01T00:00:00Z"}"#,
            ),
            Reply::Json(StatusCode::OK, br#"{"candidates":[]}"#),
        ])),
        ..Dispatch::default()
    });
    let lifecycle = lifecycle_with_store(vec![upstream], dispatch.clone(), false, storage);
    let (status, _headers, body) = send(
        &lifecycle,
        gemini_request(
            Method::POST,
            "/v1beta/cachedContents",
            br#"{"model":"models/gemini-2.5-flash","contents":[]}"#,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let response: Value = serde_json::from_slice(&body).expect("projected cache JSON");
    assert_eq!(response["name"], "cachedContents/cache-1");
    assert_eq!(response["model"], "models/gemini-2.5-flash");
    let (status, _headers, _body) = send(
        &lifecycle,
        gemini_request(
            Method::POST,
            "/v1beta/models/gemini-2.5-flash:generateContent",
            br#"{"cachedContent":"cachedContents/cache-1","contents":[]}"#,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn qa_resp_1_and_4_vertex_success_and_invalid_cache_response_are_safe() {
    let dispatch = Arc::new(Dispatch {
        replies: Mutex::new(VecDeque::from([Reply::Json(
            StatusCode::OK,
            br#"{"name":"bad"}"#,
        )])),
        ..Dispatch::default()
    });
    let lifecycle = lifecycle(
        vec![record(3, UpstreamKind::VertexApiKey, true)],
        dispatch.clone(),
        false,
    );
    let (status, _headers, body) = send(
        &lifecycle,
        gemini_request(
            Method::POST,
            "/v1beta/cachedContents",
            br#"{"model":"models/gemini-2.5-flash","contents":[]}"#,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    let response: Value = serde_json::from_slice(&body).expect("generated affinity JSON");
    assert_eq!(response["error"]["type"], "route_no_upstream_after_filter");
}

#[tokio::test]
async fn qa_elig_3_vertex_api_key_cache_and_catalog_are_ineligible_for_all_targets() {
    for (id, regional) in [(30, false), (31, true)] {
        let dispatch = Arc::new(Dispatch::default());
        let lifecycle = lifecycle(
            vec![record(id, UpstreamKind::VertexApiKey, regional)],
            dispatch.clone(),
            false,
        );
        for (method, path, body) in [
            (
                Method::POST,
                "/v1beta/cachedContents",
                br#"{"model":"models/gemini-2.5-flash","contents":[]}"#.as_slice(),
            ),
            (Method::GET, "/v1beta/models", b"".as_slice()),
        ] {
            let (status, _headers, _body) =
                send(&lifecycle, gemini_request(method, path, body)).await;
            assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{path}");
        }
        assert!(dispatch.requests.lock().expect("request lock").is_empty());
    }
}

#[test]
fn qa_elig_4_vertex_live_requires_oauth_credentials() {
    assert!(!cc_lb_engine::lifecycle::live_eligible(&record(
        32,
        UpstreamKind::VertexApiKey,
        false,
    )));
    assert!(!cc_lb_engine::lifecycle::live_eligible(&record(
        33,
        UpstreamKind::VertexApiKey,
        true,
    )));
    assert!(cc_lb_engine::lifecycle::live_eligible(&record(
        34,
        UpstreamKind::VertexCredential,
        true,
    )));
}

#[tokio::test]
async fn qa_resp_3_non_vertex_gzip_response_is_byte_and_header_passthrough() {
    use std::io::Write;
    let plaintext = br#"{"candidates":[],"usageMetadata":{"promptTokenCount":2}}"#;
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(plaintext).expect("gzip write");
    let compressed = Bytes::from(encoder.finish().expect("gzip finish"));
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    headers.insert(CONTENT_ENCODING, HeaderValue::from_static("gzip"));
    let dispatch = Arc::new(Dispatch {
        replies: Mutex::new(VecDeque::from([Reply::Bytes(
            StatusCode::OK,
            headers,
            compressed.clone(),
        )])),
        ..Dispatch::default()
    });
    let lifecycle = lifecycle(
        vec![record(4, UpstreamKind::GeminiApiKey, true)],
        dispatch,
        false,
    );
    let (status, headers, body) = send(
        &lifecycle,
        gemini_request(
            Method::POST,
            "/v1beta/models/gemini-2.5-flash:generateContent",
            br#"{"contents":[]}"#,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        headers.get(CONTENT_ENCODING),
        Some(&HeaderValue::from_static("gzip"))
    );
    assert_eq!(body, compressed);
}

#[tokio::test]
async fn qa_err_1_and_3_vertex_400_403_do_not_fail_over_and_429_is_bounded() {
    for status in [StatusCode::BAD_REQUEST, StatusCode::FORBIDDEN] {
        let dispatch = Arc::new(Dispatch {
            replies: Mutex::new(VecDeque::from([
                Reply::Json(status, br#"{"error":{"status":"INVALID_ARGUMENT"}}"#),
                Reply::Json(StatusCode::OK, br#"{}"#),
            ])),
            ..Dispatch::default()
        });
        let lifecycle = lifecycle(
            vec![
                record(10, UpstreamKind::VertexApiKey, true),
                record(11, UpstreamKind::VertexApiKey, true),
            ],
            dispatch.clone(),
            false,
        );
        let (actual, _headers, _body) = send(
            &lifecycle,
            gemini_request(
                Method::POST,
                "/v1beta/models/gemini-2.5-flash:generateContent",
                br#"{"contents":[]}"#,
            ),
        )
        .await;
        assert_eq!(actual, status);
        assert_eq!(dispatch.requests.lock().expect("request lock").len(), 1);
    }
}

#[tokio::test]
async fn qa_err_2_credential_401_refreshes_once_then_retries() {
    let dispatch = Arc::new(Dispatch {
        replies: Mutex::new(VecDeque::from([
            Reply::Json(StatusCode::UNAUTHORIZED, br#"{"error":"expired"}"#),
            Reply::Json(StatusCode::OK, br#"{"candidates":[]}"#),
        ])),
        ..Dispatch::default()
    });
    let lifecycle = lifecycle(
        vec![record(12, UpstreamKind::VertexCredential, true)],
        dispatch.clone(),
        true,
    );
    let (status, _headers, _body) = send(
        &lifecycle,
        gemini_request(
            Method::POST,
            "/v1beta/models/gemini-2.5-flash:generateContent",
            br#"{"contents":[]}"#,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(dispatch.requests.lock().expect("request lock").len(), 2);
}

#[tokio::test]
async fn qa_model_1_and_2_projectless_catalog_is_projected_without_entitlement_claims() {
    let dispatch = Arc::new(Dispatch {
        replies: Mutex::new(VecDeque::from([
            Reply::Json(
                StatusCode::OK,
                br#"{"publisherModels":[{"name":"publishers/google/models/gemini-2.5-flash","versionId":"002"},{"name":"publishers/google/models/other"}],"nextPageToken":"next"}"#,
            ),
        ])),
        ..Dispatch::default()
    });
    let lifecycle = lifecycle(
        vec![record(13, UpstreamKind::VertexCredential, true)],
        dispatch,
        false,
    );
    let (status, _headers, body) = send(
        &lifecycle,
        gemini_request(Method::GET, "/v1beta/models", b""),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let body: Value = serde_json::from_slice(&body).expect("catalog JSON");
    assert_eq!(body["models"][0]["name"], "models/gemini-2.5-flash");
    assert_eq!(body["models"][0]["version"], "002");
    assert_eq!(body["nextPageToken"], "next");
    assert!(
        body["models"][0]
            .get("supportedGenerationMethods")
            .is_none()
    );
}

#[tokio::test]
async fn qa_kind_2_subscription_preference_keeps_vertex_for_final_metered_attempt() {
    let dispatch = Arc::new(Dispatch {
        replies: Mutex::new(VecDeque::from([
            Reply::Json(StatusCode::TOO_MANY_REQUESTS, br#"{"error":"quota"}"#),
            Reply::Json(StatusCode::OK, br#"{"candidates":[]}"#),
        ])),
        ..Dispatch::default()
    });
    let mut subscription = record(14, UpstreamKind::GeminiOauth, true);
    subscription.vertex = None;
    let lifecycle = lifecycle(
        vec![subscription, record(15, UpstreamKind::VertexApiKey, true)],
        dispatch.clone(),
        false,
    );
    let (status, _headers, _body) = send(
        &lifecycle,
        gemini_request(
            Method::POST,
            "/v1beta/models/gemini-2.5-flash:generateContent",
            br#"{"contents":[]}"#,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(dispatch.requests.lock().expect("request lock").len(), 2);
}

#[tokio::test]
async fn qa_aff_4_missing_owner_rejects_cached_content_without_failover() {
    let dispatch = Arc::new(Dispatch::default());
    let lifecycle = lifecycle(
        vec![record(16, UpstreamKind::VertexApiKey, true)],
        dispatch.clone(),
        false,
    );
    let (status, _headers, _body) = send(
        &lifecycle,
        gemini_request(
            Method::POST,
            "/v1beta/models/gemini-2.5-flash:generateContent",
            br#"{"cachedContent":"cachedContents/missing","contents":[]}"#,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(dispatch.requests.lock().expect("request lock").is_empty());
}

#[tokio::test]
async fn qa_err_3_hintless_vertex_429_excludes_only_its_regional_scope() {
    let dispatch = Arc::new(Dispatch {
        replies: Mutex::new(VecDeque::from([
            Reply::Json(StatusCode::TOO_MANY_REQUESTS, br#"{"error":"quota"}"#),
            Reply::Json(StatusCode::OK, br#"{"candidates":[]}"#),
        ])),
        ..Dispatch::default()
    });
    let mut alternate = record(18, UpstreamKind::VertexApiKey, true);
    alternate.vertex = Some(VertexUpstreamConfig {
        target: VertexTarget::Regional {
            project_id: "other-project-123".to_owned().into(),
            location: "us-central1".to_owned().into(),
        },
        api_version: Default::default(),
    });
    let lifecycle = lifecycle(
        vec![record(17, UpstreamKind::VertexApiKey, true), alternate],
        dispatch.clone(),
        false,
    );
    let (status, _headers, _body) = send(
        &lifecycle,
        gemini_request(
            Method::POST,
            "/v1beta/models/gemini-2.5-flash:generateContent",
            br#"{"contents":[]}"#,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(dispatch.requests.lock().expect("request lock").len(), 2);
}

#[tokio::test]
async fn qa_reg_1_existing_gemini_api_key_wire_shape_is_unchanged() {
    let dispatch = Arc::new(Dispatch {
        replies: Mutex::new(VecDeque::from([Reply::Json(
            StatusCode::OK,
            br#"{"candidates":[]}"#,
        )])),
        ..Dispatch::default()
    });
    let mut upstream = record(15, UpstreamKind::GeminiApiKey, true);
    upstream.vertex = None;
    let lifecycle = lifecycle(vec![upstream], dispatch.clone(), false);
    let request_body = br#"{"contents":[]}"#;
    let (status, _headers, _body) = send(
        &lifecycle,
        gemini_request(
            Method::POST,
            "/v1beta/models/gemini-2.5-flash:generateContent",
            request_body,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let requests = dispatch.requests.lock().expect("request lock");
    assert_eq!(
        requests[0].url().path(),
        "/v1beta/models/gemini-2.5-flash:generateContent"
    );
    assert_eq!(requests[0].body().as_ref(), request_body);
}

#[tokio::test]
async fn qa_reg_2_gemini_oauth_wire_shape_is_unchanged() {
    let dispatch = Arc::new(Dispatch {
        replies: Mutex::new(VecDeque::from([Reply::Json(
            StatusCode::OK,
            br#"{"candidates":[]}"#,
        )])),
        ..Dispatch::default()
    });
    let mut upstream = record(19, UpstreamKind::GeminiOauth, true);
    upstream.vertex = None;
    let lifecycle = lifecycle(vec![upstream], dispatch.clone(), false);
    let request_body = br#"{"contents":[]}"#;
    let (status, _headers, _body) = send(
        &lifecycle,
        gemini_request(
            Method::POST,
            "/v1beta/models/gemini-2.5-flash:generateContent",
            request_body,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let requests = dispatch.requests.lock().expect("request lock");
    assert_eq!(
        requests[0].url().path(),
        "/v1beta/models/gemini-2.5-flash:generateContent"
    );
    assert_eq!(requests[0].body().as_ref(), request_body);
    assert_eq!(
        requests[0].headers().get("x-api-key"),
        Some(&HeaderValue::from_str(managed_api_key()).expect("managed key header"))
    );
}
