#![forbid(unsafe_code)]

use async_trait::async_trait;
use bytes::Bytes;
use cc_lb_domain::{UpstreamKind, VertexTarget, VertexUpstreamConfig, vertex::origin};
use cc_lb_upstream::{
    RetryDecision, ShapedRequest, SignedRequest, Signer, SignerError, SigningCapability,
    UpstreamError, scrub_secret,
};
use cc_lb_vertex_auth::BearerSource;
use http::header::{AUTHORIZATION, HeaderValue};
use std::borrow::Cow;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use url::Url;

/// The configured credential used to authenticate a Vertex request.
#[derive(Clone)]
pub enum VertexCredentialSource {
    /// A configured Vertex API key, sent as `x-goog-api-key`.
    ApiKey(HeaderValue),
    /// A process-local token source for a credential JSON upstream.
    Bearer(Arc<dyn BearerSource>),
}

impl fmt::Debug for VertexCredentialSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("VertexCredentialSource([REDACTED])")
    }
}

/// Error constructing a Vertex route or rewriting a request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum VertexRouteError {
    /// The ingress route or model resource is not valid.
    InvalidRoute,
    /// The request body contains an ambiguous or invalid resource reference.
    InvalidBody,
}

impl fmt::Display for VertexRouteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidRoute => "invalid Vertex route",
            Self::InvalidBody => "invalid Vertex request body",
        })
    }
}

impl std::error::Error for VertexRouteError {}

pub struct VertexSigner {
    upstream_id: String,
    kind: UpstreamKind,
    cfg: VertexUpstreamConfig,
    base_url: Option<Url>,
    quota_project: Option<String>,
    source: VertexCredentialSource,
    scrubbed_secret: Option<String>,
    attempted_token_id: Arc<AtomicU64>,
}
impl Clone for VertexSigner {
    fn clone(&self) -> Self {
        Self {
            upstream_id: self.upstream_id.clone(),
            kind: self.kind,
            cfg: self.cfg.clone(),
            base_url: self.base_url.clone(),
            quota_project: self.quota_project.clone(),
            source: self.source.clone(),
            scrubbed_secret: self.scrubbed_secret.clone(),
            attempted_token_id: Arc::new(AtomicU64::new(
                self.attempted_token_id.load(Ordering::Relaxed),
            )),
        }
    }
}

impl fmt::Debug for VertexSigner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VertexSigner")
            .field("upstream_id", &self.upstream_id)
            .field("kind", &self.kind)
            .field("cfg", &self.cfg)
            .field("credential", &"[REDACTED]")
            .finish()
    }
}

impl VertexSigner {
    /// Constructs a signer from persisted Vertex configuration and credential source.
    pub fn new(
        upstream_id: &str,
        kind: UpstreamKind,
        cfg: VertexUpstreamConfig,
        base_url: Option<Url>,
        quota_project: Option<String>,
        source: VertexCredentialSource,
    ) -> Self {
        let scrubbed_secret = match &source {
            VertexCredentialSource::ApiKey(value) => value.to_str().ok().map(str::to_owned),
            VertexCredentialSource::Bearer(_) => None,
        };
        Self {
            upstream_id: upstream_id.to_owned(),
            kind,
            base_url,
            cfg,
            quota_project,
            source,
            scrubbed_secret,
            attempted_token_id: Arc::new(AtomicU64::new(0)),
        }
    }
}

fn signing_error(reason: &'static str) -> SignerError {
    SignerError::SigningFailed {
        reason: reason.to_owned(),
    }
}

fn strip_client_credentials(headers: &mut http::HeaderMap) {
    for name in [
        "authorization",
        "x-api-key",
        "x-goog-api-key",
        "api-key",
        "x-goog-user-project",
        "x-goog-iam-authorization-token",
        "x-goog-iam-authority-selector",
        "x-vertex-ai-llm-request-type",
        "x-vertex-ai-llm-shared-request-type",
    ] {
        headers.remove(name);
    }
}

fn route_query(url: &Url, route: Route) -> String {
    let mut pairs = Vec::new();
    for (key, value) in url.query_pairs() {
        let keep = match route {
            Route::Generate | Route::Count | Route::Stream => key == "alt",
            Route::Live | Route::CachedCreate => false,
            Route::Models | Route::ModelGet => key == "pageSize" || key == "pageToken",
        };
        if keep {
            pairs.push((key.into_owned(), value.into_owned()));
        }
    }
    if matches!(route, Route::Stream) && !pairs.iter().any(|(key, _)| key == "alt") {
        pairs.push(("alt".to_owned(), "sse".to_owned()));
    }
    let mut serializer = url::form_urlencoded::Serializer::new(String::new());
    for (key, value) in pairs {
        serializer.append_pair(&key, &value);
    }
    serializer.finish()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Route {
    Generate,
    Stream,
    Live,
    Count,
    CachedCreate,
    Models,
    ModelGet,
}

fn model_id(value: &str) -> Result<&str, VertexRouteError> {
    let value = value.strip_prefix("models/").unwrap_or(value);
    if value.is_empty()
        || value.len() > 256
        || value.contains('%')
        || !value
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphanumeric)
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
    {
        return Err(VertexRouteError::InvalidRoute);
    }
    Ok(value)
}

fn ingress_path(path: &str) -> Result<(Route, Option<&str>), VertexRouteError> {
    let path = path.strip_prefix("/gemini").unwrap_or(path);
    if path.starts_with("/ws/") {
        return Ok((Route::Live, None));
    }
    let (version, rest) = if let Some(rest) = path.strip_prefix("/v1beta/") {
        ("v1beta1", rest)
    } else if let Some(rest) = path.strip_prefix("/v1/") {
        ("v1", rest)
    } else {
        return Err(VertexRouteError::InvalidRoute);
    };
    let _ = version;
    if rest == "cachedContents" {
        return Ok((Route::CachedCreate, None));
    }
    if rest == "models" {
        return Ok((Route::Models, None));
    }
    let rest = rest
        .strip_prefix("models/")
        .ok_or(VertexRouteError::InvalidRoute)?;
    if let Some((model, operation)) = rest.rsplit_once(':') {
        let operation = match operation {
            "generateContent" => Route::Generate,
            "streamGenerateContent" => Route::Stream,
            "countTokens" => Route::Count,
            _ => return Err(VertexRouteError::InvalidRoute),
        };
        return Ok((operation, Some(model_id(model)?)));
    }
    Ok((Route::ModelGet, Some(model_id(rest)?)))
}

fn vertex_path(cfg: &VertexUpstreamConfig, route: Route, model: Option<&str>) -> String {
    let version = cfg.api_version.as_str();
    let prefix = match &cfg.target {
        VertexTarget::Express => format!("/{version}/publishers/google"),
        VertexTarget::Regional {
            project_id,
            location,
        } => {
            format!("/{version}/projects/{project_id}/locations/{location}/publishers/google")
        }
    };
    match route {
        Route::Generate => format!(
            "{prefix}/models/{}:generateContent",
            model.unwrap_or_default()
        ),
        Route::Stream => format!(
            "{prefix}/models/{}:streamGenerateContent",
            model.unwrap_or_default()
        ),
        Route::Count => format!("{prefix}/models/{}:countTokens", model.unwrap_or_default()),
        Route::CachedCreate => match &cfg.target {
            VertexTarget::Regional {
                project_id,
                location,
            } => {
                format!("/{version}/projects/{project_id}/locations/{location}/cachedContents")
            }
            VertexTarget::Express => format!("/{version}/cachedContents"),
        },
        Route::Models => format!("/{version}/publishers/google/models"),
        Route::ModelGet => format!(
            "/{version}/publishers/google/models/{}",
            model.unwrap_or_default()
        ),
        Route::Live => format!("{prefix}/models"),
    }
}

fn endpoint_url(
    cfg: &VertexUpstreamConfig,
    base_url: Option<&Url>,
) -> Result<Url, VertexRouteError> {
    base_url
        .cloned()
        .or_else(|| Url::parse(&origin(&cfg.target)).ok())
        .ok_or(VertexRouteError::InvalidRoute)
}

fn build_url(
    cfg: &VertexUpstreamConfig,
    base_url: Option<&Url>,
    route: Route,
    model: Option<&str>,
    query: &str,
) -> Result<Url, VertexRouteError> {
    let base = endpoint_url(cfg, base_url)?;
    Ok(cc_lb_upstream::upstream_request_url(
        &base,
        &vertex_path(cfg, route, model),
        (!query.is_empty()).then_some(query),
    ))
}

/// Builds the Vertex Live WebSocket endpoint for a persisted target.
pub fn vertex_live_target(
    cfg: &VertexUpstreamConfig,
    base_url: Option<&Url>,
) -> Result<Url, VertexRouteError> {
    let mut target = endpoint_url(cfg, base_url)?;
    target
        .set_scheme("wss")
        .map_err(|_| VertexRouteError::InvalidRoute)?;
    target.set_path(&format!(
        "/ws/google.cloud.aiplatform.{}.LlmBidiService/BidiGenerateContent",
        cfg.api_version.as_str()
    ));
    target.set_query(None);
    Ok(target)
}

/// Rewrites the model in a Gemini Live setup frame without changing other fields.
pub fn rewrite_live_setup_model(
    frame: &[u8],
    cfg: &VertexUpstreamConfig,
) -> Result<Option<Bytes>, VertexRouteError> {
    let scanned = scan_json(frame).map_err(|_| VertexRouteError::InvalidBody)?;
    validate_json_object(frame).map_err(|_| VertexRouteError::InvalidBody)?;
    let Some(setup) = scanned
        .object_field(JsonKey::Setup)
        .and_then(|field| field.node.object.as_ref())
    else {
        return Ok(None);
    };
    let Some(model_field) = setup.iter().find(|field| field.key == JsonKey::Model) else {
        return Ok(None);
    };
    let value = string_value(frame, &model_field.node).ok_or(VertexRouteError::InvalidBody)?;
    let model = model_id(&value)?;
    let replacement = serde_json::to_vec(&live_model_name(cfg, model))
        .map_err(|_| VertexRouteError::InvalidBody)?;
    let mut out = frame.to_vec();
    out.splice(model_field.node.start..model_field.node.end, replacement);
    Ok(Some(Bytes::from(out)))
}

fn live_model_name(cfg: &VertexUpstreamConfig, model: &str) -> String {
    match &cfg.target {
        VertexTarget::Express => format!("models/{model}"),
        VertexTarget::Regional {
            project_id,
            location,
        } => {
            format!("projects/{project_id}/locations/{location}/publishers/google/models/{model}")
        }
    }
}

#[async_trait]
impl Signer for VertexSigner {
    async fn sign(
        &self,
        mut shaped: ShapedRequest,
        capability: &mut SigningCapability,
    ) -> Result<SignedRequest, SignerError> {
        if !matches!(
            self.kind,
            UpstreamKind::VertexApiKey | UpstreamKind::VertexCredential
        ) {
            return Err(signing_error(
                "Vertex signer used for a non-Vertex upstream",
            ));
        }
        let (route, model) = ingress_path(shaped.url().path())
            .map_err(|_| signing_error("invalid Vertex ingress route"))?;
        if matches!(route, Route::CachedCreate) && matches!(self.cfg.target, VertexTarget::Express)
        {
            return Err(signing_error(
                "cached contents are unavailable on Vertex Express",
            ));
        }
        let query = route_query(shaped.url(), route);
        let url = build_url(&self.cfg, self.base_url.as_ref(), route, model, &query)
            .map_err(|_| signing_error("invalid Vertex route"))?;
        let rewrite = match route {
            Route::CachedCreate => Some(rewrite_cached_create(shaped.body(), &self.cfg)),
            Route::Generate | Route::Stream | Route::Count => {
                Some(rewrite_cached_reference(shaped.body(), &self.cfg, route))
            }
            Route::Models | Route::ModelGet | Route::Live => None,
        };
        if let Some(result) = rewrite {
            let body = result.map_err(|_| signing_error("invalid Vertex request body"))?;
            if body.as_ref() != shaped.body().as_ref() {
                shaped.set_body(body);
                shaped.headers_mut().remove("content-length");
            }
        }
        shaped.set_url(url);
        strip_client_credentials(shaped.headers_mut());
        match &self.source {
            VertexCredentialSource::ApiKey(value) => {
                shaped.headers_mut().insert("x-goog-api-key", value.clone());
            }
            VertexCredentialSource::Bearer(source) => {
                let token = source.bearer().await.map_err(|error| {
                    if let Some(reason) = error.terminal_reason() {
                        SignerError::MissingCredentials {
                            reason: reason.to_owned(),
                        }
                    } else {
                        signing_error("could not mint Vertex access token")
                    }
                })?;
                self.attempted_token_id.store(token.id, Ordering::Relaxed);
                shaped.headers_mut().insert(AUTHORIZATION, token.header);
                let quota = self
                    .quota_project
                    .as_deref()
                    .or_else(|| source.quota_project());
                if let Some(quota) = quota {
                    let value = HeaderValue::from_str(quota)
                        .map_err(|_| signing_error("invalid Vertex quota project"))?;
                    shaped.headers_mut().insert("x-goog-user-project", value);
                }
            }
        }
        Ok(SignedRequest::from_shaped(shaped, capability))
    }

    async fn on_unauthorized(&self, _err: &UpstreamError) -> RetryDecision {
        if let VertexCredentialSource::Bearer(source) = &self.source {
            let token_id = self.attempted_token_id.load(Ordering::Relaxed);
            if token_id != 0 && source.invalidate_if_current(token_id) {
                return RetryDecision::Refresh {
                    new_signer: Arc::new(self.clone()),
                };
            }
        }
        RetryDecision::Fail
    }

    fn scrub_secrets<'a>(&self, text: &'a str) -> Cow<'a, str> {
        self.scrubbed_secret
            .as_deref()
            .map(|secret| scrub_secret(secret, text))
            .unwrap_or(Cow::Borrowed(text))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum JsonKey {
    Model,
    CachedContent,
    GenerateContentRequest,
    Setup,
    SnakeCachedContent,
    SnakeGenerateContentRequest,
    Other,
}

#[derive(Clone, Debug)]
struct JsonNode {
    start: usize,
    end: usize,
    object: Option<Vec<JsonField>>,
}
#[derive(Clone, Debug)]
struct JsonField {
    key: JsonKey,
    node: JsonNode,
}

impl JsonNode {
    fn object_field(&self, key: JsonKey) -> Option<&JsonField> {
        self.object.as_ref()?.iter().find(|field| field.key == key)
    }
}

fn string_value(input: &[u8], node: &JsonNode) -> Option<String> {
    (node.start < node.end && input.get(node.start) == Some(&b'"'))
        .then(|| serde_json::from_slice(&input[node.start..node.end]).ok())
        .flatten()
}

fn scan_json(input: &[u8]) -> Result<JsonNode, ()> {
    let (node, pos) = scan_value(input, 0, 0, true)?;
    if skip_ws(input, pos) != input.len() {
        return Err(());
    }
    Ok(node)
}

fn skip_ws(input: &[u8], mut pos: usize) -> usize {
    while input
        .get(pos)
        .is_some_and(|b| matches!(b, b' ' | b'\n' | b'\r' | b'\t'))
    {
        pos += 1;
    }
    pos
}

fn scan_string(input: &[u8], start: usize) -> Result<usize, ()> {
    if input.get(start) != Some(&b'"') {
        return Err(());
    }
    let mut pos = start + 1;
    while let Some(byte) = input.get(pos).copied() {
        match byte {
            b'"' => return Ok(pos + 1),
            b'\\' => {
                pos += 2;
                if input.get(pos - 1).is_none() {
                    return Err(());
                }
            }
            0..=0x1f => return Err(()),
            _ => pos += 1,
        }
    }
    Err(())
}

fn key_kind(input: &[u8], start: usize, end: usize) -> Result<JsonKey, ()> {
    let raw = input
        .get(start + 1..end.checked_sub(1).ok_or(())?)
        .ok_or(())?;
    let matches_key = |key: &str| raw == key.as_bytes();
    let decoded = || serde_json::from_slice::<String>(&input[start..end]).map_err(|_| ());
    if matches_key("model") {
        return Ok(JsonKey::Model);
    }
    if matches_key("cachedContent") {
        return Ok(JsonKey::CachedContent);
    }
    if matches_key("generateContentRequest") {
        return Ok(JsonKey::GenerateContentRequest);
    }
    if matches_key("setup") {
        return Ok(JsonKey::Setup);
    }
    if matches_key("cached_content") {
        return Ok(JsonKey::SnakeCachedContent);
    }
    if matches_key("generate_content_request") {
        return Ok(JsonKey::SnakeGenerateContentRequest);
    }
    if raw.contains(&b'\\') {
        return Ok(match decoded()?.as_str() {
            "model" => JsonKey::Model,
            "cachedContent" => JsonKey::CachedContent,
            "generateContentRequest" => JsonKey::GenerateContentRequest,
            "setup" => JsonKey::Setup,
            "cached_content" => JsonKey::SnakeCachedContent,
            "generate_content_request" => JsonKey::SnakeGenerateContentRequest,
            _ => JsonKey::Other,
        });
    }
    Ok(JsonKey::Other)
}

fn key_bit(key: JsonKey) -> Option<u8> {
    match key {
        JsonKey::Model => Some(1),
        JsonKey::CachedContent => Some(2),
        JsonKey::GenerateContentRequest => Some(4),
        JsonKey::Setup => Some(8),
        JsonKey::SnakeCachedContent | JsonKey::SnakeGenerateContentRequest | JsonKey::Other => None,
    }
}

const MAX_JSON_DEPTH: usize = 64;

fn scan_value(
    input: &[u8],
    start: usize,
    depth: usize,
    reject_snake_aliases: bool,
) -> Result<(JsonNode, usize), ()> {
    let start = skip_ws(input, start);
    let byte = *input.get(start).ok_or(())?;
    if byte == b'"' {
        let end = scan_string(input, start)?;
        return Ok((
            JsonNode {
                start,
                end,
                object: None,
            },
            end,
        ));
    }
    if byte == b'{' {
        if depth >= MAX_JSON_DEPTH {
            return Err(());
        }
        let mut pos = skip_ws(input, start + 1);
        let mut fields = Vec::new();
        let mut seen = 0u8;
        if input.get(pos) == Some(&b'}') {
            return Ok((
                JsonNode {
                    start,
                    end: pos + 1,
                    object: Some(fields),
                },
                pos + 1,
            ));
        }
        loop {
            let key_end = scan_string(input, pos)?;
            let key = key_kind(input, pos, key_end)?;
            if reject_snake_aliases
                && matches!(
                    key,
                    JsonKey::SnakeCachedContent | JsonKey::SnakeGenerateContentRequest
                )
            {
                return Err(());
            }
            pos = skip_ws(input, key_end);
            if input.get(pos) != Some(&b':') {
                return Err(());
            }
            let child_reject_aliases =
                matches!(key, JsonKey::GenerateContentRequest | JsonKey::Setup);
            let (node, end) = scan_value(input, pos + 1, depth + 1, child_reject_aliases)?;
            if let Some(bit) = key_bit(key) {
                if seen & bit != 0 {
                    return Err(());
                }
                seen |= bit;
            }
            fields.push(JsonField { key, node });
            pos = skip_ws(input, end);
            match input.get(pos) {
                Some(b',') => {
                    pos = skip_ws(input, pos + 1);
                }
                Some(b'}') => {
                    return Ok((
                        JsonNode {
                            start,
                            end: pos + 1,
                            object: Some(fields),
                        },
                        pos + 1,
                    ));
                }
                _ => return Err(()),
            }
        }
    }
    if byte == b'[' {
        if depth >= MAX_JSON_DEPTH {
            return Err(());
        }
        let mut pos = skip_ws(input, start + 1);
        if input.get(pos) == Some(&b']') {
            return Ok((
                JsonNode {
                    start,
                    end: pos + 1,
                    object: None,
                },
                pos + 1,
            ));
        }
        loop {
            let (_, end) = scan_value(input, pos, depth + 1, false)?;
            pos = skip_ws(input, end);
            match input.get(pos) {
                Some(b',') => pos = skip_ws(input, pos + 1),
                Some(b']') => {
                    return Ok((
                        JsonNode {
                            start,
                            end: pos + 1,
                            object: None,
                        },
                        pos + 1,
                    ));
                }
                _ => return Err(()),
            }
        }
    }
    let mut end = start;
    while input
        .get(end)
        .is_some_and(|b| !matches!(b, b',' | b'}' | b']' | b' ' | b'\n' | b'\r' | b'\t'))
    {
        end += 1;
    }
    if end == start {
        return Err(());
    }
    Ok((
        JsonNode {
            start,
            end,
            object: None,
        },
        end,
    ))
}
fn replacement_model(cfg: &VertexUpstreamConfig, value: &str) -> Result<Vec<u8>, ()> {
    let model = model_id(value).map_err(|_| ())?;
    serde_json::to_vec(&live_model_name(cfg, model)).map_err(|_| ())
}

fn replace_spans(mut input: Vec<u8>, mut spans: Vec<(usize, usize, Vec<u8>)>) -> Vec<u8> {
    spans.sort_by_key(|(start, _, _)| std::cmp::Reverse(*start));
    for (start, end, replacement) in spans {
        input.splice(start..end, replacement);
    }
    input
}

fn validate_json_object(input: &[u8]) -> Result<(), ()> {
    let mut iter = sonic_rs::to_object_iter(input);
    for entry in &mut iter {
        entry.map_err(|_| ())?;
    }
    Ok(())
}

fn rewrite_cached_create(body: &Bytes, cfg: &VertexUpstreamConfig) -> Result<Bytes, ()> {
    let root = scan_json(body).map_err(|_| ())?;
    validate_json_object(body)?;
    let model = root.object_field(JsonKey::Model).ok_or(())?;
    let value = string_value(body, &model.node).ok_or(())?;
    if !value.starts_with("models/") {
        return Err(());
    }
    let replacement = replacement_model(cfg, &value)?;
    Ok(Bytes::from(replace_spans(
        body.to_vec(),
        vec![(model.node.start, model.node.end, replacement)],
    )))
}

fn rewrite_cached_reference(
    body: &Bytes,
    cfg: &VertexUpstreamConfig,
    route: Route,
) -> Result<Bytes, ()> {
    let raw = body.as_ref();
    let has_cache = raw
        .windows(b"cachedContent".len())
        .any(|w| w == b"cachedContent");
    let has_wrapper = raw
        .windows(b"generateContentRequest".len())
        .any(|w| w == b"generateContentRequest");
    let has_escaped_key = raw.windows(2).any(|w| w == b"\\u");
    let has_snake_alias = raw
        .windows(b"cached_content".len())
        .any(|w| w == b"cached_content")
        || raw
            .windows(b"generate_content_request".len())
            .any(|w| w == b"generate_content_request");
    let has_count_model =
        matches!(route, Route::Count) && raw.windows(b"\"model\"".len()).any(|w| w == b"\"model\"");
    if !has_cache && !has_wrapper && !has_escaped_key && !has_count_model && !has_snake_alias {
        return Ok(body.clone());
    }
    let root = scan_json(body).map_err(|_| ())?;
    validate_json_object(raw)?;
    let mut target = root.clone();
    let sole_wrapper = matches!(route, Route::Count)
        && root.object.as_ref().is_some_and(|fields| fields.len() == 1)
        && root.object_field(JsonKey::GenerateContentRequest).is_some();
    if sole_wrapper {
        target = root
            .object_field(JsonKey::GenerateContentRequest)
            .ok_or(())?
            .node
            .clone();
        if target.object.is_none() {
            return Err(());
        }
    }
    let mut spans = Vec::new();
    let mut rewrite_object = |object: &Vec<JsonField>| -> Result<(), ()> {
        if matches!(route, Route::Count)
            && let Some(model) = object.iter().find(|field| field.key == JsonKey::Model)
            && let Some(value) = string_value(raw, &model.node)
        {
            spans.push((
                model.node.start,
                model.node.end,
                replacement_model(cfg, &value)?,
            ));
        }
        if let Some(cache) = object
            .iter()
            .find(|field| field.key == JsonKey::CachedContent)
        {
            if let Some(value) = string_value(raw, &cache.node) {
                let id = value.strip_prefix("cachedContents/").ok_or(())?;
                if id.is_empty() || id.contains('/') || id.contains('%') {
                    return Err(());
                }
                let full = match &cfg.target {
                    VertexTarget::Regional {
                        project_id,
                        location,
                    } => format!("projects/{project_id}/locations/{location}/cachedContents/{id}"),
                    VertexTarget::Express => return Err(()),
                };
                spans.push((
                    cache.node.start,
                    cache.node.end,
                    serde_json::to_vec(&full).map_err(|_| ())?,
                ));
            } else if raw
                .get(cache.node.start..cache.node.end)
                .is_some_and(|v| v == b"null")
            {
                // Null cache references are forwarded unchanged.
            } else {
                return Err(());
            }
        }
        Ok(())
    };
    if let Some(object) = target.object.as_ref() {
        rewrite_object(object)?;
    }
    if let Some(nested) = target
        .object_field(JsonKey::GenerateContentRequest)
        .and_then(|field| field.node.object.as_ref())
    {
        rewrite_object(nested)?;
    }
    if spans.is_empty() && !sole_wrapper {
        return Ok(body.clone());
    }
    let out = replace_spans(raw.to_vec(), spans);
    if sole_wrapper {
        // Re-run against the extracted object so the wrapper is removed while preserving its bytes.
        let inner = target.start..target.end;
        let mut nested_spans = Vec::new();
        let nested = scan_json(&raw[inner.clone()]).map_err(|_| ())?;
        if let Some(object) = nested.object.as_ref() {
            if matches!(route, Route::Count)
                && let Some(model) = object.iter().find(|field| field.key == JsonKey::Model)
                && let Some(value) = string_value(&raw[inner.clone()], &model.node)
            {
                nested_spans.push((
                    model.node.start,
                    model.node.end,
                    replacement_model(cfg, &value)?,
                ));
            }
            if let Some(cache) = object
                .iter()
                .find(|field| field.key == JsonKey::CachedContent)
                && let Some(value) = string_value(&raw[inner.clone()], &cache.node)
            {
                let id = value.strip_prefix("cachedContents/").ok_or(())?;
                let full = match &cfg.target {
                    VertexTarget::Regional {
                        project_id,
                        location,
                    } => format!("projects/{project_id}/locations/{location}/cachedContents/{id}"),
                    VertexTarget::Express => return Err(()),
                };
                nested_spans.push((
                    cache.node.start,
                    cache.node.end,
                    serde_json::to_vec(&full).map_err(|_| ())?,
                ));
            }
        }
        return Ok(Bytes::from(replace_spans(
            raw[inner].to_vec(),
            nested_spans,
        )));
    }
    Ok(Bytes::from(out))
}

#[cfg(test)]
mod tests {
    use super::*;
    use cc_lb_domain::VertexApiVersion;
    fn regional() -> VertexUpstreamConfig {
        VertexUpstreamConfig {
            target: VertexTarget::Regional {
                project_id: "demo-project".into(),
                location: "us-central1".into(),
            },
            api_version: VertexApiVersion::V1Beta1,
        }
    }

    #[test]
    fn qa_url_1_regional_path_host_and_version() {
        let (route, model) =
            ingress_path("/v1beta/models/gemini-2.5-flash:generateContent").unwrap();
        let query = route_query(
            &Url::parse("https://example.test/?key=bad&alt=json").unwrap(),
            route,
        );
        let url = build_url(&regional(), None, route, model, &query).unwrap();
        assert_eq!(
            url.as_str(),
            "https://us-central1-aiplatform.googleapis.com/v1beta1/projects/demo-project/locations/us-central1/publishers/google/models/gemini-2.5-flash:generateContent?alt=json"
        );
    }

    #[test]
    fn qa_url_2_stream_adds_sse_and_retains_only_alt() {
        let (route, model) =
            ingress_path("/gemini/v1/models/gemini:streamGenerateContent").unwrap();
        let query = route_query(&Url::parse("https://example.test/?foo=x").unwrap(), route);
        assert_eq!(query, "alt=sse");
        let url = build_url(&regional(), None, route, model, &query).unwrap();
        assert_eq!(url.query(), Some("alt=sse"));
    }

    #[test]
    fn qa_url_4_hostile_model_fails_before_credential() {
        assert!(ingress_path("/v1/models/../models/tunedModels/x:generateContent").is_err());
        assert!(ingress_path("/v1/models/a%2Fb:generateContent").is_err());
        assert!(model_id(".").is_err());
        assert!(model_id("..").is_err());
        assert!(model_id("_x").is_err());
        assert!(
            rewrite_live_setup_model(
                br#"{"setup":{"model":"models/a"},"setup":{"model":"models/b"}}"#,
                &regional()
            )
            .is_err()
        );
        assert!(
            rewrite_cached_reference(
                &Bytes::from_static(br#"{"cached_content":"cachedContents/x"}"#),
                &regional(),
                Route::Generate
            )
            .is_err()
        );
    }

    #[test]
    fn qa_body_2_cached_content_rewrites_and_rejects_duplicates() {
        let cfg = regional();
        let body = Bytes::from_static(br#"{"cachedContent":"cachedContents/abc","contents":[]}"#);
        let rewritten = rewrite_cached_reference(&body, &cfg, Route::Generate).unwrap();
        assert_eq!(
            rewritten.as_ref(),
            br#"{"cachedContent":"projects/demo-project/locations/us-central1/cachedContents/abc","contents":[]}"#
        );
        let duplicate = Bytes::from_static(
            br#"{"cachedContent":"cachedContents/a","cachedContent":"cachedContents/b"}"#,
        );
        assert!(rewrite_cached_reference(&duplicate, &cfg, Route::Generate).is_err());
    }

    #[test]
    fn qa_body_3_count_wrapper_flattens_only_sole_wrapper() {
        let cfg = regional();
        let body = Bytes::from_static(
            br#"{"generateContentRequest":{"model":"models/gemini","cachedContent":"cachedContents/abc"}}"#,
        );
        let rewritten = rewrite_cached_reference(&body, &cfg, Route::Count).unwrap();
        assert_eq!(
            rewritten.as_ref(),
            br#"{"model":"projects/demo-project/locations/us-central1/publishers/google/models/gemini","cachedContent":"projects/demo-project/locations/us-central1/cachedContents/abc"}"#
        );
        let mixed = Bytes::from_static(
            br#"{"generateContentRequest":{"cachedContent":"cachedContents/abc"},"other":1}"#,
        );
        assert_eq!(
            rewrite_cached_reference(&mixed, &cfg, Route::Count)
                .unwrap()
                .as_ref(),
            br#"{"generateContentRequest":{"cachedContent":"projects/demo-project/locations/us-central1/cachedContents/abc"},"other":1}"#
        );
    }

    #[test]
    fn qa_live_2_rewrites_only_setup_model_for_text_and_binary_payloads() {
        let cfg = regional();
        let frame =
            br#"{"setup":{"model":"models/gemini","generationConfig":{"temperature":0.1}}}"#;
        let rewritten = rewrite_live_setup_model(frame, &cfg).unwrap().unwrap();
        assert_eq!(
            rewritten.as_ref(),
            br#"{"setup":{"model":"projects/demo-project/locations/us-central1/publishers/google/models/gemini","generationConfig":{"temperature":0.1}}}"#
        );
        assert!(
            rewrite_live_setup_model(br#"{"setup":{"generationConfig":{}}}"#, &cfg)
                .unwrap()
                .is_none()
        );
    }
    #[test]
    fn qa_url_5_base_url_override_is_origin_only() {
        let base = Url::parse("https://proxy.example.test/prefix?secret=1").unwrap();
        let (route, model) = ingress_path("/v1/models/gemini:generateContent").unwrap();
        let url = build_url(&regional(), Some(&base), route, model, "").unwrap();
        assert_eq!(
            url.as_str(),
            "https://proxy.example.test/v1beta1/projects/demo-project/locations/us-central1/publishers/google/models/gemini:generateContent"
        );
        let live = vertex_live_target(&regional(), Some(&base)).unwrap();
        assert_eq!(
            live.as_str(),
            "wss://proxy.example.test/ws/google.cloud.aiplatform.v1beta1.LlmBidiService/BidiGenerateContent"
        );
    }
    #[test]
    fn qa_body_2_cached_create_rejects_invalid_json_grammar() {
        assert!(
            rewrite_cached_create(
                &Bytes::from_static(br#"{"model":"models/gemini","#),
                &regional()
            )
            .is_err()
        );
    }

    #[test]
    fn qa_sec_1_live_target_uses_fixed_path_without_query() {
        let target = vertex_live_target(&regional(), None).unwrap();
        assert_eq!(
            target.as_str(),
            "wss://us-central1-aiplatform.googleapis.com/ws/google.cloud.aiplatform.v1beta1.LlmBidiService/BidiGenerateContent"
        );
    }

    #[test]
    fn qa_url_3_route_allowlists_drop_credentials_and_unrelated_queries() {
        let (route, _) = ingress_path("/v1/models").unwrap();
        let url = Url::parse(
            "https://example.test/?pageSize=10&pageToken=next&key=secret&access_token=secret&alt=sse",
        )
        .unwrap();
        assert_eq!(route_query(&url, route), "pageSize=10&pageToken=next");
    }

    #[test]
    fn qa_url_5_origin_is_rebuilt_from_validated_target() {
        let cfg = VertexUpstreamConfig {
            target: VertexTarget::Regional {
                project_id: "demo-project".into(),
                location: "eu".into(),
            },
            api_version: VertexApiVersion::V1,
        };
        let (route, model) = ingress_path("/v1/models/gemini:generateContent").unwrap();
        let url = build_url(&cfg, None, route, model, "").unwrap();
        assert_eq!(
            url.as_str(),
            "https://aiplatform.eu.rep.googleapis.com/v1/projects/demo-project/locations/eu/publishers/google/models/gemini:generateContent"
        );
    }

    #[test]
    fn qa_url_6_scoped_and_unscoped_ingress_have_same_vertex_route() {
        let cfg = regional();
        let (a, am) = ingress_path("/v1beta/models/gemini:generateContent").unwrap();
        let (b, bm) = ingress_path("/gemini/v1beta/models/gemini:generateContent").unwrap();
        assert_eq!(
            build_url(&cfg, None, a, am, "").unwrap(),
            build_url(&cfg, None, b, bm, "").unwrap()
        );
    }

    #[test]
    fn qa_body_1_ordinary_body_is_unchanged_without_copying() {
        let cfg = regional();
        let body = Bytes::from_static(br#"{"contents":[{"parts":[{"text":"hello"}]}]}"#);
        let rewritten = rewrite_cached_reference(&body, &cfg, Route::Generate).unwrap();
        assert_eq!(rewritten.as_ref(), body.as_ref());
        assert_eq!(rewritten.as_ptr(), body.as_ptr());
    }

    #[test]
    fn qa_body_4_cached_create_rewrites_only_model_bytes() {
        let cfg = regional();
        let body = Bytes::from_static(
            br#"{"model":"models/gemini","displayName":"cache","contents":[{"parts":[{"text":"x"}]}]}"#,
        );
        let rewritten = rewrite_cached_create(&body, &cfg).unwrap();
        assert_eq!(
            rewritten.as_ref(),
            br#"{"model":"projects/demo-project/locations/us-central1/publishers/google/models/gemini","displayName":"cache","contents":[{"parts":[{"text":"x"}]}]}"#
        );
    }

    #[test]
    fn qa_live_1_target_uses_versioned_vertex_live_endpoint() {
        let target = vertex_live_target(&regional(), None).unwrap();
        assert_eq!(target.scheme(), "wss");
        assert!(target.path().contains("aiplatform.v1beta1.LlmBidiService"));
    }

    #[test]
    fn qa_live_3_express_target_builds_a_supported_live_resource() {
        let cfg = VertexUpstreamConfig {
            target: VertexTarget::Express,
            api_version: VertexApiVersion::V1Beta1,
        };
        assert!(matches!(cfg.target, VertexTarget::Express));
        assert!(vertex_live_target(&cfg, None).is_ok());
    }

    #[test]
    fn qa_live_4_setup_rewrite_is_single_attempt_and_deterministic() {
        let cfg = regional();
        let frame = br#"{"setup":{"model":"models/gemini"}}"#;
        assert_eq!(
            rewrite_live_setup_model(frame, &cfg).unwrap(),
            rewrite_live_setup_model(frame, &cfg).unwrap()
        );
    }

    #[test]
    fn qa_live_5_setup_without_model_is_forwarded_unchanged() {
        let cfg = regional();
        assert_eq!(
            rewrite_live_setup_model(br#"{"setup":{"generationConfig":{}}}"#, &cfg).unwrap(),
            None
        );
    }

    #[test]
    fn qa_sec_2_forbidden_headers_are_removed_by_signer() {
        let mut headers = http::HeaderMap::new();
        for name in [
            "authorization",
            "x-api-key",
            "x-goog-api-key",
            "api-key",
            "x-goog-user-project",
            "x-goog-iam-authorization-token",
            "x-goog-iam-authority-selector",
            "x-vertex-ai-llm-request-type",
            "x-vertex-ai-llm-shared-request-type",
        ] {
            headers.insert(name, HeaderValue::from_static("caller-secret"));
        }
        strip_client_credentials(&mut headers);
        assert!(headers.is_empty());
    }

    #[test]
    fn qa_sec_6_resource_names_fail_closed_for_model_and_cache_smuggling() {
        assert!(model_id("tunedModels/gemini").is_err());
        assert!(model_id("models/gemini/../other").is_err());
        let body = Bytes::from_static(br#"{"cachedContent":{"name":"cachedContents/x"}}"#);
        assert!(rewrite_cached_reference(&body, &regional(), Route::Generate).is_err());
    }

    #[test]
    fn qa_sec_3_debug_output_redacts_api_key() {
        let signer = VertexSigner::new(
            "upstream",
            UpstreamKind::VertexApiKey,
            regional(),
            None,
            None,
            VertexCredentialSource::ApiKey(HeaderValue::from_static("secret-key")),
        );
        assert!(!format!("{signer:?}").contains("secret-key"));
    }

    #[test]
    fn qa_sec_4_invalid_resource_url_is_never_forwarded() {
        assert!(ingress_path("/v1/models/gemini%2f..%2fsecret:generateContent").is_err());
        assert!(ingress_path("/v1/models/gemini@evil:generateContent").is_err());
    }

    #[test]
    fn qa_sec_5_credential_material_is_not_in_debug_or_route() {
        let signer = VertexSigner::new(
            "upstream",
            UpstreamKind::VertexApiKey,
            regional(),
            None,
            None,
            VertexCredentialSource::ApiKey(HeaderValue::from_static("private-key-material")),
        );
        assert!(!format!("{signer:?}").contains("private-key-material"));
        assert!(!vertex_path(&regional(), Route::Generate, Some("gemini")).contains("private-key"));
    }
    #[test]
    fn qa_sec_7_deep_json_bodies_fail_cleanly() {
        fn deeply_nested(prefix: &[u8], suffix: &[u8]) -> Bytes {
            let mut body = Vec::with_capacity(prefix.len() + 200_000 + suffix.len());
            body.extend_from_slice(prefix);
            body.extend(std::iter::repeat_n(b'[', 100_000));
            body.extend(std::iter::repeat_n(b']', 100_000));
            body.extend_from_slice(suffix);
            Bytes::from(body)
        }

        let cfg = regional();
        let create = deeply_nested(br#"{"model":"models/gemini","contents":"#, br#"}"#);
        assert!(rewrite_cached_create(&create, &cfg).is_err());

        let reference = deeply_nested(
            br#"{"cachedContent":"cachedContents/cache","contents":"#,
            br#"}"#,
        );
        assert!(rewrite_cached_reference(&reference, &cfg, Route::Generate).is_err());

        let live = deeply_nested(
            br#"{"setup":{"model":"models/gemini","generationConfig":"#,
            br#"}}}"#,
        );
        assert!(rewrite_live_setup_model(&live, &cfg).is_err());
    }

    #[test]
    fn qa_body_7_nested_tool_args_allow_snake_case_aliases() {
        let body = Bytes::from_static(
            br#"{"contents":[{"parts":[{"functionCall":{"args":{"cached_content":"opaque"}}}]}]}"#,
        );
        assert_eq!(
            rewrite_cached_reference(&body, &regional(), Route::Generate)
                .unwrap()
                .as_ref(),
            body.as_ref()
        );
    }
}
