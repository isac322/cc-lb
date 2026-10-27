use std::collections::HashMap;
use std::net::SocketAddr;

use bytes::Bytes;
use cc_lb_domain::{Principal, PrincipalKind, Upstream};
use cc_lb_engine::make_default_dispatcher;
use cc_lb_signer_anthropic_key::{ProviderKeyKind, ProviderKeySigner};
use cc_lb_upstream::{
    DialectError, DialectShapeContext, ShapedRequest, ShapedRequestBuilder, SignedRequest,
    UpstreamDialect, shape_request, sign_request,
};
use http::{HeaderMap, Method};
use http_body_util::BodyExt;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use url::Url;

const REQUEST_BODY: &[u8] = br#"{"model":"claude-test","messages":[]}"#;
const OPENAI_SSE: &[u8] = b"event: response.output_text.delta\ndata: {\"delta\":\"hello\"}\n\n";
const GEMINI_ERROR: &[u8] = br#"{"error":{"code":429,"status":"RESOURCE_EXHAUSTED"}}"#;

#[tokio::test]
async fn oa_http_01_posts_responses_with_bearer_and_preserves_native_sse() {
    let upstream = FakeUpstream::start(ResponseSpec {
        status: 200,
        content_type: "text/event-stream",
        body: OPENAI_SSE,
    })
    .await;
    let dispatcher = make_default_dispatcher(0);
    let request = provider_signed_request(
        &upstream.base_url(),
        Upstream::OpenaiDirect { base_url: None },
        "/v1/responses",
        None,
        ProviderKeyKind::Openai,
        "provider-native-test-token",
    )
    .await;

    let response = dispatcher
        .dispatch(request)
        .await
        .expect("loopback OpenAI dispatch succeeds");
    let status = response.status();
    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let body = response
        .into_body()
        .collect()
        .await
        .expect("collect native SSE body")
        .to_bytes();
    let captured = upstream.finish().await;

    assert_eq!(captured.method, "POST");
    assert_eq!(captured.body.as_ref(), REQUEST_BODY);
    assert_eq!(status, http::StatusCode::OK);
    assert_eq!(content_type.as_deref(), Some("text/event-stream"));
    assert_eq!(body.as_ref(), OPENAI_SSE);
    assert_eq!(captured.target, "/v1/responses");
    assert_eq!(
        captured.header("authorization"),
        Some("Bearer provider-native-test-token")
    );
}

#[tokio::test]
async fn gm_http_01_posts_generate_content_and_preserves_error_response() {
    let upstream = FakeUpstream::start(ResponseSpec {
        status: 429,
        content_type: "application/json",
        body: GEMINI_ERROR,
    })
    .await;
    let dispatcher = make_default_dispatcher(0);
    let request = provider_signed_request(
        &upstream.base_url(),
        Upstream::GeminiDirect { base_url: None },
        "/v1beta/models/gemini-test:generateContent",
        None,
        ProviderKeyKind::Gemini,
        "provider-native-test-key",
    )
    .await;

    let response = dispatcher
        .dispatch(request)
        .await
        .expect("loopback Gemini dispatch succeeds");
    let status = response.status();
    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let body = response
        .into_body()
        .collect()
        .await
        .expect("collect Gemini error body")
        .to_bytes();
    let captured = upstream.finish().await;

    assert_eq!(captured.method, "POST");
    assert_eq!(captured.body.as_ref(), REQUEST_BODY);
    assert_eq!(status, http::StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(content_type.as_deref(), Some("application/json"));
    assert_eq!(body.as_ref(), GEMINI_ERROR);
    assert_eq!(
        captured.target,
        "/v1beta/models/gemini-test:generateContent"
    );
    assert_eq!(
        captured.header("x-goog-api-key"),
        Some("provider-native-test-key")
    );
}

#[tokio::test]
async fn gm_http_03_posts_count_tokens_with_api_key() {
    let upstream = FakeUpstream::start(ResponseSpec {
        status: 200,
        content_type: "application/json",
        body: br#"{"totalTokens":42}"#,
    })
    .await;
    let dispatcher = make_default_dispatcher(0);
    let request = provider_signed_request(
        &upstream.base_url(),
        Upstream::GeminiDirect { base_url: None },
        "/v1beta/models/gemini-test:countTokens",
        None,
        ProviderKeyKind::Gemini,
        "provider-native-test-key",
    )
    .await;
    let response = dispatcher
        .dispatch(request)
        .await
        .expect("loopback Gemini countTokens dispatch succeeds");
    assert_eq!(response.status(), http::StatusCode::OK);
    let body = response
        .into_body()
        .collect()
        .await
        .expect("collect Gemini countTokens body")
        .to_bytes();
    let captured = upstream.finish().await;
    assert_eq!(body.as_ref(), br#"{"totalTokens":42}"#);
    assert_eq!(captured.target, "/v1beta/models/gemini-test:countTokens");
    assert_eq!(
        captured.header("x-goog-api-key"),
        Some("provider-native-test-key")
    );
}
#[tokio::test]
async fn gm_http_02_preserves_stream_generate_content_sse_query() {
    let upstream = FakeUpstream::start(ResponseSpec {
        status: 200,
        content_type: "text/event-stream",
        body: OPENAI_SSE,
    })
    .await;
    let dispatcher = make_default_dispatcher(0);
    let request = provider_signed_request(
        &upstream.base_url(),
        Upstream::GeminiDirect { base_url: None },
        "/v1beta/models/gemini-test:streamGenerateContent",
        Some("alt=sse"),
        ProviderKeyKind::Gemini,
        "provider-native-test-key",
    )
    .await;

    let response = dispatcher
        .dispatch(request)
        .await
        .expect("loopback Gemini streaming dispatch succeeds");
    let body = response
        .into_body()
        .collect()
        .await
        .expect("collect Gemini SSE body")
        .to_bytes();
    let captured = upstream.finish().await;

    assert_eq!(captured.method, "POST");
    assert_eq!(captured.body.as_ref(), REQUEST_BODY);
    assert_eq!(body.as_ref(), OPENAI_SSE);
    assert_eq!(
        captured.target,
        "/v1beta/models/gemini-test:streamGenerateContent?alt=sse"
    );
    assert_eq!(
        captured.header("x-goog-api-key"),
        Some("provider-native-test-key")
    );
}

async fn provider_signed_request(
    base_url: &str,
    upstream: Upstream,
    path: &str,
    query: Option<&str>,
    signer_kind: ProviderKeyKind,
    api_key: &str,
) -> SignedRequest {
    let context = DialectShapeContext {
        request_id: "provider-native-test".to_owned(),
        downstream_headers: HeaderMap::new(),
        method: Method::POST,
        path: path.to_owned(),
        query: query.map(str::to_owned),
        body_bytes: Bytes::from_static(REQUEST_BODY),
    };
    let principal = Principal {
        id: "provider-native-test".to_owned(),
        kind: PrincipalKind::ApiKey,
    };
    let shaped = shape_request(
        &ProviderDialect {
            base_url: Url::parse(base_url).expect("test URL parses"),
        },
        &context,
        &upstream,
        &principal,
    )
    .expect("provider-native request shapes");
    let signer = ProviderKeySigner::new(signer_kind, api_key).expect("provider signer builds");
    sign_request(&signer, shaped)
        .await
        .expect("provider-native request signs")
}

struct ProviderDialect {
    base_url: Url,
}

impl UpstreamDialect for ProviderDialect {
    fn shape(
        &self,
        context: &DialectShapeContext,
        upstream: &Upstream,
        _principal: &Principal,
        builder: &mut ShapedRequestBuilder,
    ) -> Result<ShapedRequest, DialectError> {
        match upstream {
            Upstream::OpenaiDirect { .. } | Upstream::GeminiDirect { .. } => {}
            Upstream::AnthropicDirect { .. } => {
                return Err(DialectError::UnsupportedRequest {
                    reason: "provider-native test requires an OpenAI or Gemini upstream".to_owned(),
                });
            }
        }
        let mut url = self.base_url.clone();
        url.set_path(context.path.trim_start_matches('/'));
        url.set_query(context.query.as_deref());
        Ok(builder.shaped_request(
            url,
            context.method.clone(),
            context.downstream_headers.clone(),
            context.body_bytes.clone(),
        ))
    }
}

struct ResponseSpec {
    status: u16,
    content_type: &'static str,
    body: &'static [u8],
}

struct FakeUpstream {
    addr: SocketAddr,
    task: JoinHandle<std::io::Result<CapturedRequest>>,
}

impl FakeUpstream {
    async fn start(response: ResponseSpec) -> Self {
        let listener = TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("bind loopback upstream");
        let addr = listener.local_addr().expect("read loopback address");
        let task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            let request = read_request(&mut stream).await?;
            let response_head = format!(
                "HTTP/1.1 {} OK\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                response.status,
                response.content_type,
                response.body.len(),
            );
            stream.write_all(response_head.as_bytes()).await?;
            stream.write_all(response.body).await?;
            Ok(request)
        });
        Self { addr, task }
    }

    fn base_url(&self) -> String {
        format!("http://{}", self.addr)
    }

    async fn finish(self) -> CapturedRequest {
        self.task
            .await
            .expect("loopback upstream task joins")
            .expect("loopback upstream serves request")
    }
}

struct CapturedRequest {
    method: String,
    target: String,
    headers: HashMap<String, String>,
    body: Bytes,
}

impl CapturedRequest {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .get(&name.to_ascii_lowercase())
            .map(String::as_str)
    }
}

async fn read_request(stream: &mut tokio::net::TcpStream) -> std::io::Result<CapturedRequest> {
    let mut buffer = Vec::new();
    let header_end = loop {
        let mut chunk = [0_u8; 1024];
        let read = stream.read(&mut chunk).await?;
        if read == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "upstream closed before request headers",
            ));
        }
        buffer.extend_from_slice(&chunk[..read]);
        if let Some(index) = buffer.windows(4).position(|window| window == b"\r\n\r\n") {
            break index + 4;
        }
    };

    let header_text = String::from_utf8_lossy(&buffer[..header_end]);
    let mut lines = header_text.split("\r\n");
    let request_line = lines.next().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, "missing request line")
    })?;
    let mut request_parts = request_line.split_ascii_whitespace();
    let method = request_parts
        .next()
        .ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "missing request method")
        })?
        .to_owned();
    let target = request_parts
        .next()
        .ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "missing request target")
        })?
        .to_owned();

    let mut headers = HashMap::new();
    let mut content_length = None;
    for line in lines.filter(|line| !line.is_empty()) {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        let name = name.to_ascii_lowercase();
        let value = value.trim().to_owned();
        if name == "content-length" {
            content_length =
                Some(value.parse::<usize>().map_err(|source| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, source)
                })?);
        }
        headers.insert(name, value);
    }
    let content_length = content_length.ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, "missing content length")
    })?;
    while buffer.len() < header_end + content_length {
        let mut chunk = [0_u8; 1024];
        let read = stream.read(&mut chunk).await?;
        if read == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "upstream closed before request body",
            ));
        }
        buffer.extend_from_slice(&chunk[..read]);
    }

    Ok(CapturedRequest {
        method,
        target,
        headers,
        body: Bytes::copy_from_slice(&buffer[header_end..header_end + content_length]),
    })
}
