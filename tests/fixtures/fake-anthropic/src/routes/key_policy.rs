//! Per-key scripted answers and the request recorder.
//!
//! A key with no policy keeps the fixture's historical behavior: `/v1/models`
//! and `/v1/messages/count_tokens` answer 200 without an
//! `anthropic-workspace-id` response header. A policy scripts the inference
//! probes, the workspace rules, and the org Admin API routes for one exact
//! `x-api-key` value.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::convert::Infallible;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use axum::Json;
use axum::body::{Body, Bytes};
use axum::extract::{Request, State};
use axum::http::header::{CONTENT_LENGTH, CONTENT_TYPE, LOCATION};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use tokio::time::sleep;

use super::helpers::error_response;
use super::state::{AppState, with_fixture_headers};

/// Request and response header naming the workspace a request acts in.
pub(crate) const WORKSPACE_HEADER: &str = "anthropic-workspace-id";

/// Message of Anthropic's identity-linked 400, as returned by
/// api.anthropic.com (observed 2026-10-12).
pub const IDENTITY_LINKED_MESSAGE: &str = "This API key is not scoped to a workspace, so this request must include the anthropic-workspace-id header with the ID of the workspace to use. Add the header, or use an API key that is scoped to a workspace.";

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

/// Policies keyed by the exact `x-api-key` value. Clones share one map, so a
/// test keeps a handle and changes policies while the fixture runs.
#[derive(Clone, Default)]
pub struct KeyPolicies {
    inner: Arc<Mutex<HashMap<String, KeyPolicy>>>,
}

impl KeyPolicies {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set(&self, key: impl Into<String>, policy: KeyPolicy) {
        lock(&self.inner).insert(key.into(), policy);
    }

    pub fn remove(&self, key: &str) {
        lock(&self.inner).remove(key);
    }

    pub fn clear(&self) {
        lock(&self.inner).clear();
    }

    pub(crate) fn get(&self, key: &str) -> Option<KeyPolicy> {
        lock(&self.inner).get(key).cloned()
    }
}

impl fmt::Debug for KeyPolicies {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KeyPolicies")
            .field("len", &lock(&self.inner).len())
            .finish()
    }
}

/// Scripted behavior for one key. Every field defaults, so a JSON policy
/// names only what it changes.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct KeyPolicy {
    /// `GET /v1/models`.
    pub models: Script,
    /// `POST /v1/messages/count_tokens`.
    pub count_tokens: Script,
    /// The workspace a bound key acts in. 2xx inference answers carry it as
    /// the `anthropic-workspace-id` response header; a request naming another
    /// workspace gets 404.
    pub bound_workspace: Option<String>,
    /// Org-scoped identity key: inference without `anthropic-workspace-id`
    /// answers the identity-linked 400.
    pub requires_workspace: bool,
    /// Workspaces the key can act in and that the workspace list returns.
    /// When non-empty, a request naming a workspace outside it gets 404.
    pub workspaces: Vec<FakeWorkspace>,
    /// `GET /v1/organizations/workspaces`.
    pub workspaces_list: Script,
    /// `GET /v1/organizations/usage_report/messages`.
    pub usage_report: Script,
    /// `GET /v1/organizations/api_keys`.
    pub api_keys: Script,
    /// Raw usage-report pages served in order by the default usage script;
    /// the last page repeats.
    pub usage_pages: Vec<Value>,
    /// Items the default `api_keys` script returns.
    pub api_key_items: Vec<FakeApiKey>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FakeWorkspace {
    pub id: String,
    pub name: String,
}

impl FakeWorkspace {
    pub fn new(id: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            name: name.into(),
        }
    }
}

/// One `api_keys` item. `principal` and `scope` are echoed verbatim, e.g.
/// `{"type":"service_account_actor"}` and `{"type":"organization"}`;
/// a `null` principal is a legacy workspace key.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FakeApiKey {
    pub id: String,
    pub partial_key_hint: String,
    #[serde(default)]
    pub principal: Option<Value>,
    #[serde(default)]
    pub scope: Option<Value>,
}

/// How one endpoint answers.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Script {
    /// The endpoint's normal answer.
    #[default]
    Default,
    /// A fixed status, JSON body, and headers.
    Status {
        status: u16,
        #[serde(default)]
        body: Value,
        #[serde(default)]
        headers: BTreeMap<String, String>,
    },
    /// Each call takes the next step; once one step is left it repeats.
    Sequence { steps: ScriptSteps },
    /// Waits `secs` on the tokio clock, then answers with `then`.
    Delay {
        secs: u64,
        #[serde(default)]
        then: Box<Script>,
    },
    /// A redirect to `location`.
    Redirect { status: u16, location: String },
    /// 200 with a streamed body of `bytes` spaces.
    Huge { bytes: usize },
    /// 200 headers, then a body that never arrives.
    Stall,
}

impl Script {
    pub fn status(status: u16, body: Value) -> Self {
        Self::Status {
            status,
            body,
            headers: BTreeMap::new(),
        }
    }

    /// Anthropic's error envelope.
    pub fn error(status: u16, error_type: &str, message: &str) -> Self {
        Self::status(
            status,
            serde_json::json!({"type": "error", "error": {"type": error_type, "message": message}}),
        )
    }

    pub fn sequence(steps: impl IntoIterator<Item = Script>) -> Self {
        Self::Sequence {
            steps: ScriptSteps::new(steps),
        }
    }

    pub fn delay(secs: u64, then: Script) -> Self {
        Self::Delay {
            secs,
            then: Box::new(then),
        }
    }

    pub fn with_header(self, name: impl Into<String>, value: impl Into<String>) -> Self {
        match self {
            Self::Status {
                status,
                body,
                mut headers,
            } => {
                headers.insert(name.into(), value.into());
                Self::Status {
                    status,
                    body,
                    headers,
                }
            }
            other => other,
        }
    }

    /// Resolves sequences to the step this call uses.
    fn next_step(&self) -> Script {
        let mut current = self.clone();
        while let Self::Sequence { steps } = current {
            current = steps.take();
        }
        current
    }
}

/// Shared, consumable steps of a [`Script::Sequence`].
#[derive(Clone, Default)]
pub struct ScriptSteps {
    inner: Arc<Mutex<VecDeque<Script>>>,
}

impl ScriptSteps {
    fn new(steps: impl IntoIterator<Item = Script>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(steps.into_iter().collect())),
        }
    }

    fn take(&self) -> Script {
        let mut steps = lock(&self.inner);
        if steps.len() > 1 {
            steps.pop_front().unwrap_or_default()
        } else {
            steps.front().cloned().unwrap_or_default()
        }
    }
}

impl<'de> Deserialize<'de> for ScriptSteps {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Vec::<Script>::deserialize(deserializer).map(Self::new)
    }
}

impl fmt::Debug for ScriptSteps {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ScriptSteps")
            .field("remaining", &lock(&self.inner).len())
            .finish()
    }
}

/// Answers with `script`, using `default` for [`Script::Default`].
pub(crate) async fn serve(script: &Script, default: impl FnOnce() -> Response) -> Response {
    let mut current = script.next_step();
    loop {
        match current {
            Script::Default => return default(),
            Script::Status {
                status,
                body,
                headers,
            } => return status_response(status, body, &headers),
            Script::Sequence { .. } => current = current.next_step(),
            Script::Delay { secs, then } => {
                sleep(Duration::from_secs(secs)).await;
                current = then.next_step();
            }
            Script::Redirect { status, location } => {
                let mut response = StatusCode::from_u16(status)
                    .unwrap_or(StatusCode::FOUND)
                    .into_response();
                if let Ok(value) = HeaderValue::from_str(&location) {
                    response.headers_mut().insert(LOCATION, value);
                }
                return with_fixture_headers(response);
            }
            Script::Huge { bytes } => return huge_response(bytes),
            Script::Stall => return stalled_response(),
        }
    }
}

fn status_response(status: u16, body: Value, headers: &BTreeMap<String, String>) -> Response {
    let mut response = Json(body).into_response();
    *response.status_mut() =
        StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    for (name, value) in headers {
        if let (Ok(name), Ok(value)) = (
            HeaderName::from_bytes(name.as_bytes()),
            HeaderValue::from_str(value),
        ) {
            response.headers_mut().insert(name, value);
        }
    }
    with_fixture_headers(response)
}

fn huge_response(bytes: usize) -> Response {
    const CHUNK: usize = 64 * 1024;
    let chunk = Bytes::from(vec![b' '; CHUNK]);
    let stream = async_stream::stream! {
        let mut remaining = bytes;
        while remaining > 0 {
            let len = remaining.min(CHUNK);
            remaining -= len;
            yield Ok::<Bytes, Infallible>(chunk.slice(..len));
        }
    };
    json_stream_response(Body::from_stream(stream))
}

fn stalled_response() -> Response {
    let stream = async_stream::stream! {
        std::future::pending::<()>().await;
        yield Ok::<Bytes, Infallible>(Bytes::new());
    };
    json_stream_response(Body::from_stream(stream))
}

fn json_stream_response(body: Body) -> Response {
    let mut response = Response::new(body);
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    with_fixture_headers(response)
}

pub(crate) fn header_value<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|value| value.to_str().ok())
}

/// The policy for the request's `x-api-key`, if any.
pub(crate) fn policy_for(state: &AppState, headers: &HeaderMap) -> Option<KeyPolicy> {
    header_value(headers, "x-api-key").and_then(|key| state.config.key_policies.get(key))
}

/// Workspace rules shared by every inference endpoint. `Some` is the answer;
/// `None` lets the endpoint's script run.
pub(crate) fn workspace_gate(policy: &KeyPolicy, headers: &HeaderMap) -> Option<Response> {
    let requested = header_value(headers, WORKSPACE_HEADER);
    if let Some(bound) = policy.bound_workspace.as_deref() {
        return requested.filter(|id| *id != bound).map(|id| {
            error_response(
                StatusCode::NOT_FOUND,
                "not_found_error",
                &format!("workspace {id} was not found; this API key acts in workspace {bound}"),
            )
        });
    }
    if !policy.requires_workspace {
        return None;
    }
    let Some(requested) = requested else {
        return Some(error_response(
            StatusCode::BAD_REQUEST,
            "invalid_request_error",
            IDENTITY_LINKED_MESSAGE,
        ));
    };
    let well_formed = requested.strip_prefix("wrkspc_").is_some_and(|body| {
        !body.is_empty() && body.bytes().all(|byte| byte.is_ascii_alphanumeric())
    });
    if !well_formed {
        return Some(error_response(
            StatusCode::BAD_REQUEST,
            "invalid_request_error",
            "anthropic-workspace-id header must be a valid workspace ID.",
        ));
    }
    let known = policy.workspaces.is_empty()
        || policy
            .workspaces
            .iter()
            .any(|workspace| workspace.id == requested);
    // api.anthropic.com answers a well-formed id the org does not have with
    // the same 400 as a malformed one (observed 2026-10-12).
    (!known).then(|| {
        error_response(
            StatusCode::BAD_REQUEST,
            "invalid_request_error",
            "anthropic-workspace-id header must be a valid workspace ID.",
        )
    })
}

/// Adds the resolved workspace to a 2xx answer, as Anthropic does.
pub(crate) fn with_resolved_workspace(
    mut response: Response,
    policy: &KeyPolicy,
    headers: &HeaderMap,
) -> Response {
    if !response.status().is_success() {
        return response;
    }
    let resolved = policy
        .bound_workspace
        .as_deref()
        .or_else(|| header_value(headers, WORKSPACE_HEADER).filter(|_| policy.requires_workspace));
    if let Some(value) = resolved.and_then(|id| HeaderValue::from_str(id).ok()) {
        response.headers_mut().insert(WORKSPACE_HEADER, value);
    }
    response
}

/// One request as the fixture received it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct RecordedRequest {
    pub method: String,
    pub path: String,
    pub query: Option<String>,
    /// Lowercase header names; repeated headers are joined with `, `.
    pub headers: BTreeMap<String, String>,
    /// The body when `content-length` is at most 64 KiB.
    pub body: Option<String>,
}

impl RecordedRequest {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).map(String::as_str)
    }

    /// The body parsed as JSON, when it was recorded and parses.
    pub fn body_json(&self) -> Option<Value> {
        self.body
            .as_deref()
            .and_then(|body| serde_json::from_str(body).ok())
    }
}

/// Every request the fixture receives, except its own `/__*` debug routes.
/// Clones share one log.
#[derive(Clone, Default)]
pub struct RequestRecorder {
    inner: Arc<Mutex<Vec<RecordedRequest>>>,
}

impl RequestRecorder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn snapshot(&self) -> Vec<RecordedRequest> {
        lock(&self.inner).clone()
    }

    /// Requests whose path starts with `path_prefix`.
    pub fn count(&self, path_prefix: &str) -> usize {
        lock(&self.inner)
            .iter()
            .filter(|request| request.path.starts_with(path_prefix))
            .count()
    }

    pub fn clear(&self) {
        lock(&self.inner).clear();
    }

    fn record(&self, request: RecordedRequest) {
        lock(&self.inner).push(request);
    }
}

impl fmt::Debug for RequestRecorder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RequestRecorder")
            .field("len", &lock(&self.inner).len())
            .finish()
    }
}

/// Bodies up to this size (by `content-length`) are kept in the log.
const MAX_RECORDED_BODY_BYTES: usize = 64 * 1024;

pub(crate) async fn record_request(
    State(state): State<Arc<AppState>>,
    request: Request,
    next: Next,
) -> Response {
    if request.uri().path().starts_with("/__") {
        return next.run(request).await;
    }
    let (parts, body) = request.into_parts();
    let mut headers = BTreeMap::<String, String>::new();
    for (name, value) in &parts.headers {
        let value = String::from_utf8_lossy(value.as_bytes()).into_owned();
        headers
            .entry(name.as_str().to_owned())
            .and_modify(|joined| {
                joined.push_str(", ");
                joined.push_str(&value);
            })
            .or_insert(value);
    }
    let small = parts
        .headers
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<usize>().ok())
        .is_some_and(|len| len <= MAX_RECORDED_BODY_BYTES);
    let (body, recorded_body) = if small {
        match axum::body::to_bytes(body, MAX_RECORDED_BODY_BYTES).await {
            Ok(bytes) => {
                let text = String::from_utf8_lossy(&bytes).into_owned();
                (Body::from(bytes), Some(text))
            }
            Err(_) => (Body::empty(), None),
        }
    } else {
        (body, None)
    };
    state.config.recorder.record(RecordedRequest {
        method: parts.method.as_str().to_owned(),
        path: parts.uri.path().to_owned(),
        query: parts.uri.query().map(ToOwned::to_owned),
        headers,
        body: recorded_body,
    });
    next.run(Request::from_parts(parts, body)).await
}
