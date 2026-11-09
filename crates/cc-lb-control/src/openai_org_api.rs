//! Bounded OpenAI organization-admin API client.
//!
//! This client is intentionally separate from serving-provider clients.  Its
//! credential is an organization administrator credential and requests always
//! use the fixed OpenAI organization origin in production; an upstream serving
//! URL is never consulted.

use std::fmt;
use std::time::Duration;

use bytes::Bytes;
use http::header::{AUTHORIZATION, CONTENT_TYPE, RETRY_AFTER};
use http::{Method, Request, StatusCode};
use http_body_util::{BodyExt, Full, LengthLimitError, Limited};
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use serde::Deserialize;
use serde::de::DeserializeOwned;
use url::Url;

const PRODUCTION_ORIGIN: &str = "https://api.openai.com/";
const PROJECTS_PATH: &str = "v1/organization/projects";
const USAGE_PATH: &str = "v1/organization/usage/completions";
const COSTS_PATH: &str = "v1/organization/costs";
const DEFAULT_LIMIT: u32 = 100;

/// The HTTP client deliberately does not follow redirects.
type HttpClient = Client<HttpsConnector<HttpConnector>, Full<Bytes>>;

/// Bounds applied to every organization request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpenAiOrgLimits {
    /// Deadline for one request, including reading its bounded body.
    pub per_request: Duration,
    /// Maximum response body accepted from OpenAI.
    pub max_body_bytes: usize,
    /// Maximum project pages fetched by [`OpenAiOrgApi::list_projects`].
    pub max_project_pages: u32,
}

impl OpenAiOrgLimits {
    pub const PRODUCTION: Self = Self {
        per_request: Duration::from_secs(10),
        max_body_bytes: 1024 * 1024,
        max_project_pages: 3,
    };
}

/// Why an organization-admin API call did not produce usable data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum OpenAiOrgApiError {
    /// The credential is not allowed to use organization-admin APIs.
    #[error("OpenAI organization API denied the request (HTTP {status})")]
    Denied { status: u16 },
    /// The organization API asked the caller to retry later.
    #[error("OpenAI organization API rate limited the request")]
    RateLimited { retry_after_secs: Option<u64> },
    /// A non-success response other than denial or rate limiting.
    #[error("OpenAI organization API answered HTTP {status}")]
    Upstream { status: u16 },
    /// No complete response arrived before the request deadline.
    #[error("OpenAI organization API timed out")]
    Timeout,
    /// The connection failed or the server attempted a redirect.
    #[error("OpenAI organization API unreachable")]
    Unreachable,
    /// The provider response exceeded the configured body cap.
    #[error("OpenAI organization API response body too large")]
    BodyTooLarge,
    /// A successful response was not valid JSON or did not have the documented shape.
    #[error("OpenAI organization API answered malformed JSON")]
    MalformedJson,
}

/// OpenAI's organization project, retaining archive and provider metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenAiOrganizationProject {
    pub id: String,
    pub name: String,
    pub created_at: u64,
    pub archived_at: Option<u64>,
    pub status: Option<String>,
    pub external_key_id: Option<String>,
    pub residency: Option<String>,
}

/// Projects fetched before the bounded page cap was reached.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenAiProjectListing {
    pub projects: Vec<OpenAiOrganizationProject>,
    /// False when OpenAI reported another page that was not fetched.
    pub complete: bool,
}

/// A bounded query for text-generation usage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenAiUsageQuery {
    pub start_time: u64,
    pub end_time: Option<u64>,
    pub bucket_width: OpenAiBucketWidth,
    pub limit: u32,
    pub page: Option<String>,
}

impl Default for OpenAiUsageQuery {
    fn default() -> Self {
        Self {
            start_time: 0,
            end_time: None,
            bucket_width: OpenAiBucketWidth::Day,
            limit: DEFAULT_LIMIT,
            page: None,
        }
    }
}

/// Supported OpenAI organization bucket widths for this client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenAiBucketWidth {
    Hour,
    Day,
}

impl OpenAiBucketWidth {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Hour => "1h",
            Self::Day => "1d",
        }
    }
}

/// A text-generation usage page and its provider grouping fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenAiTextUsagePage {
    pub buckets: Vec<OpenAiTextUsageBucket>,
    pub next_page: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenAiTextUsageBucket {
    pub start_time: u64,
    pub end_time: u64,
    pub results: Vec<OpenAiTextUsageResult>,
}

/// One OpenAI text-generation usage grouping result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenAiTextUsageResult {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub input_cached_tokens: u64,
    pub num_model_requests: u64,
    pub project_id: Option<String>,
    pub user_id: Option<String>,
    pub api_key_id: Option<String>,
    pub model: Option<String>,
    pub batch: Option<bool>,
}

/// A bounded query for daily organization costs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenAiCostsQuery {
    pub start_time: u64,
    pub end_time: Option<u64>,
    pub bucket_width: OpenAiBucketWidth,
    pub limit: u32,
    pub page: Option<String>,
}

impl Default for OpenAiCostsQuery {
    fn default() -> Self {
        Self {
            start_time: 0,
            end_time: None,
            bucket_width: OpenAiBucketWidth::Day,
            limit: DEFAULT_LIMIT,
            page: None,
        }
    }
}

/// A daily cost page with currency and line-item fields preserved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenAiCostsPage {
    pub buckets: Vec<OpenAiCostsBucket>,
    pub next_page: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenAiCostsBucket {
    pub start_time: u64,
    pub end_time: u64,
    pub results: Vec<OpenAiCostResult>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenAiCostResult {
    pub amount: OpenAiCostAmount,
    pub line_item: Option<String>,
    pub project_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenAiCostAmount {
    pub value: String,
    pub currency: String,
}

/// Bounded client for OpenAI organization-admin endpoints.
#[derive(Clone)]
pub struct OpenAiOrgApi {
    origin: Url,
    http: HttpClient,
    limits: OpenAiOrgLimits,
}

impl OpenAiOrgApi {
    /// Construct a client pinned to `https://api.openai.com/`.
    pub fn production() -> Result<Self, url::ParseError> {
        Url::parse(PRODUCTION_ORIGIN).map(Self::with_origin)
    }

    /// Construct a client against a test origin. Production callers should use
    /// [`Self::production`]; this method exists only for contract-faithful fakes.
    pub fn with_origin(origin: Url) -> Self {
        Self {
            origin,
            http: http_client(),
            limits: OpenAiOrgLimits::PRODUCTION,
        }
    }

    /// Construct a client with explicit bounds, primarily for local fakes and
    /// deterministic timeout/body-limit tests.
    pub fn with_origin_and_limits(origin: Url, limits: OpenAiOrgLimits) -> Self {
        Self {
            origin,
            http: http_client(),
            limits,
        }
    }

    pub fn origin(&self) -> &Url {
        &self.origin
    }

    pub fn limits(&self) -> &OpenAiOrgLimits {
        &self.limits
    }

    /// Fetch active projects, following `after` cursors for at most the project page cap.
    ///
    /// OpenAI defaults to excluding archived projects; callers that need the
    /// full authorized project set can use [`Self::list_projects_with_archived`].
    pub async fn list_projects(
        &self,
        credential: &str,
    ) -> Result<OpenAiProjectListing, OpenAiOrgApiError> {
        self.list_projects_inner(credential, false).await
    }

    /// Fetch projects including archived projects, following `after` cursors
    /// for at most the project page cap.
    pub async fn list_projects_with_archived(
        &self,
        credential: &str,
    ) -> Result<OpenAiProjectListing, OpenAiOrgApiError> {
        self.list_projects_inner(credential, true).await
    }

    async fn list_projects_inner(
        &self,
        credential: &str,
        include_archived: bool,
    ) -> Result<OpenAiProjectListing, OpenAiOrgApiError> {
        let mut projects = Vec::new();
        let mut after: Option<String> = None;
        for _ in 0..self.limits.max_project_pages {
            let mut query = vec![
                ("limit".to_owned(), DEFAULT_LIMIT.to_string()),
                ("include_archived".to_owned(), include_archived.to_string()),
            ];
            if let Some(cursor) = after.as_deref() {
                query.push(("after".to_owned(), cursor.to_owned()));
            }
            let body = self.get(credential, PROJECTS_PATH, &query).await?;
            let page: RawProjectPage = parse_json(&body)?;
            projects.extend(page.data.into_iter().map(project));
            if !page.has_more {
                return Ok(OpenAiProjectListing {
                    projects,
                    complete: true,
                });
            }
            match page.last_id {
                Some(last_id) if !last_id.is_empty() => after = Some(last_id),
                _ => break,
            }
        }
        Ok(OpenAiProjectListing {
            projects,
            complete: false,
        })
    }

    /// Fetch one text-generation usage page, grouped by project, API key, and
    /// model. Rows without a model remain explicit aggregate rows.
    pub async fn text_usage(
        &self,
        credential: &str,
        query: &OpenAiUsageQuery,
    ) -> Result<OpenAiTextUsagePage, OpenAiOrgApiError> {
        let body = self
            .get(credential, USAGE_PATH, &usage_query(query))
            .await?;
        parse_usage(&body)
    }

    /// Fetch one daily (or explicitly requested bucket width) cost page.
    pub async fn costs(
        &self,
        credential: &str,
        query: &OpenAiCostsQuery,
    ) -> Result<OpenAiCostsPage, OpenAiOrgApiError> {
        let body = self.get(credential, COSTS_PATH, &cost_query(query)).await?;
        parse_costs(&body)
    }

    async fn get(
        &self,
        credential: &str,
        path: &str,
        query: &[(String, String)],
    ) -> Result<Bytes, OpenAiOrgApiError> {
        if self.limits.per_request.is_zero() {
            return Err(OpenAiOrgApiError::Timeout);
        }
        let mut url = self
            .origin
            .join(path)
            .map_err(|_| OpenAiOrgApiError::Unreachable)?;
        url.query_pairs_mut().extend_pairs(
            query
                .iter()
                .map(|(name, value)| (name.as_str(), value.as_str())),
        );
        let request = Request::builder()
            .method(Method::GET)
            .uri(url.as_str())
            .header(AUTHORIZATION, format!("Bearer {credential}"))
            .header(CONTENT_TYPE, "application/json")
            .body(Full::new(Bytes::new()))
            .map_err(|_| OpenAiOrgApiError::Unreachable)?;
        let exchange = async {
            let response = self
                .http
                .request(request)
                .await
                .map_err(|_| OpenAiOrgApiError::Unreachable)?;
            let (parts, body) = response.into_parts();
            let body = Limited::new(body, self.limits.max_body_bytes)
                .collect()
                .await
                .map_err(|error| {
                    if error.downcast_ref::<LengthLimitError>().is_some() {
                        OpenAiOrgApiError::BodyTooLarge
                    } else {
                        OpenAiOrgApiError::Unreachable
                    }
                })?
                .to_bytes();
            classify(parts.status, &parts.headers, body)
        };
        tokio::time::timeout(self.limits.per_request, exchange)
            .await
            .map_err(|_| OpenAiOrgApiError::Timeout)?
    }
}

impl fmt::Debug for OpenAiOrgApi {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpenAiOrgApi")
            .field("origin", &self.origin.as_str())
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

fn http_client() -> HttpClient {
    let connector = HttpsConnectorBuilder::new()
        .with_webpki_roots()
        .https_or_http()
        .enable_http1()
        .build();
    Client::builder(TokioExecutor::new()).build(connector)
}

fn classify(
    status: StatusCode,
    headers: &http::HeaderMap,
    body: Bytes,
) -> Result<Bytes, OpenAiOrgApiError> {
    if status.is_success() {
        return Ok(body);
    }
    if status.is_redirection() {
        return Err(OpenAiOrgApiError::Unreachable);
    }
    Err(match status {
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => OpenAiOrgApiError::Denied {
            status: status.as_u16(),
        },
        StatusCode::TOO_MANY_REQUESTS => OpenAiOrgApiError::RateLimited {
            retry_after_secs: headers
                .get(RETRY_AFTER)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.trim().parse().ok()),
        },
        other => OpenAiOrgApiError::Upstream {
            status: other.as_u16(),
        },
    })
}

fn parse_json<T: DeserializeOwned>(body: &[u8]) -> Result<T, OpenAiOrgApiError> {
    serde_json::from_slice(body).map_err(|_| OpenAiOrgApiError::MalformedJson)
}

#[derive(Deserialize)]
struct RawProjectPage {
    #[serde(default)]
    data: Vec<RawProject>,
    #[serde(default)]
    has_more: bool,
    #[serde(default)]
    last_id: Option<String>,
}

#[derive(Deserialize)]
struct RawProject {
    id: String,
    name: String,
    #[serde(default)]
    created_at: u64,
    #[serde(default)]
    archived_at: Option<u64>,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    external_key_id: Option<String>,
    #[serde(default)]
    residency: Option<String>,
}

fn project(raw: RawProject) -> OpenAiOrganizationProject {
    OpenAiOrganizationProject {
        id: raw.id,
        name: raw.name,
        created_at: raw.created_at,
        archived_at: raw.archived_at,
        status: raw.status,
        external_key_id: raw.external_key_id,
        residency: raw.residency,
    }
}

fn usage_query(query: &OpenAiUsageQuery) -> Vec<(String, String)> {
    let mut values = vec![
        ("start_time".into(), query.start_time.to_string()),
        ("bucket_width".into(), query.bucket_width.as_str().into()),
        ("limit".into(), query.limit.to_string()),
        ("group_by[]".into(), "project_id".into()),
        ("group_by[]".into(), "api_key_id".into()),
        ("group_by[]".into(), "model".into()),
    ];
    if let Some(end) = query.end_time {
        values.push(("end_time".into(), end.to_string()));
    }
    if let Some(page) = query.page.as_deref() {
        values.push(("page".into(), page.into()));
    }
    values
}

fn cost_query(query: &OpenAiCostsQuery) -> Vec<(String, String)> {
    let mut values = vec![
        ("start_time".into(), query.start_time.to_string()),
        ("bucket_width".into(), query.bucket_width.as_str().into()),
        ("limit".into(), query.limit.to_string()),
        ("group_by[]".into(), "project_id".into()),
    ];
    if let Some(end) = query.end_time {
        values.push(("end_time".into(), end.to_string()));
    }
    if let Some(page) = query.page.as_deref() {
        values.push(("page".into(), page.into()));
    }
    values
}

#[derive(Deserialize)]
struct RawUsagePage {
    #[serde(default)]
    data: Vec<RawUsageBucket>,
    #[serde(default)]
    next_page: Option<String>,
}

#[derive(Deserialize)]
struct RawUsageBucket {
    start_time: u64,
    end_time: u64,
    #[serde(default)]
    results: Vec<RawUsageResult>,
}

#[derive(Deserialize)]
struct RawUsageResult {
    #[serde(default)]
    input_tokens: u64,
    #[serde(default)]
    output_tokens: u64,
    #[serde(default)]
    input_cached_tokens: u64,
    #[serde(default)]
    num_model_requests: u64,
    #[serde(default)]
    project_id: Option<String>,
    #[serde(default)]
    user_id: Option<String>,
    #[serde(default)]
    api_key_id: Option<String>,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    batch: Option<bool>,
}

fn parse_usage(body: &[u8]) -> Result<OpenAiTextUsagePage, OpenAiOrgApiError> {
    let raw: RawUsagePage = parse_json(body)?;
    Ok(OpenAiTextUsagePage {
        buckets: raw
            .data
            .into_iter()
            .map(|bucket| OpenAiTextUsageBucket {
                start_time: bucket.start_time,
                end_time: bucket.end_time,
                results: bucket
                    .results
                    .into_iter()
                    .map(|result| OpenAiTextUsageResult {
                        input_tokens: result.input_tokens,
                        output_tokens: result.output_tokens,
                        input_cached_tokens: result.input_cached_tokens,
                        num_model_requests: result.num_model_requests,
                        project_id: result.project_id,
                        user_id: result.user_id,
                        api_key_id: result.api_key_id,
                        model: result.model,
                        batch: result.batch,
                    })
                    .collect(),
            })
            .collect(),
        next_page: raw.next_page,
    })
}

#[derive(Deserialize)]
struct RawCostsPage {
    #[serde(default)]
    data: Vec<RawCostsBucket>,
    #[serde(default)]
    next_page: Option<String>,
}

#[derive(Deserialize)]
struct RawCostsBucket {
    start_time: u64,
    end_time: u64,
    #[serde(default)]
    results: Vec<RawCostResult>,
}

#[derive(Deserialize)]
struct RawCostResult {
    amount: RawCostAmount,
    #[serde(default)]
    line_item: Option<String>,
    #[serde(default)]
    project_id: Option<String>,
}

#[derive(Deserialize)]
struct RawCostAmount {
    value: serde_json::Value,
    currency: String,
}

fn parse_costs(body: &[u8]) -> Result<OpenAiCostsPage, OpenAiOrgApiError> {
    let raw: RawCostsPage = parse_json(body)?;
    Ok(OpenAiCostsPage {
        buckets: raw
            .data
            .into_iter()
            .map(|bucket| OpenAiCostsBucket {
                start_time: bucket.start_time,
                end_time: bucket.end_time,
                results: bucket
                    .results
                    .into_iter()
                    .map(|result| OpenAiCostResult {
                        amount: OpenAiCostAmount {
                            value: match result.amount.value {
                                serde_json::Value::String(value) => value,
                                value => value.to_string(),
                            },
                            currency: result.amount.currency,
                        },
                        line_item: result.line_item,
                        project_id: result.project_id,
                    })
                    .collect(),
            })
            .collect(),
        next_page: raw.next_page,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn production_is_pinned_to_official_origin() {
        let api = OpenAiOrgApi::production().expect("official origin parses");
        assert_eq!(api.origin().as_str(), PRODUCTION_ORIGIN);
        assert!(!api.origin().as_str().contains("proxy"));
    }

    #[test]
    fn project_and_archive_fields_are_preserved() {
        let page = br#"{"data":[{"id":"proj_1","name":"billing","created_at":10,"archived_at":20,"status":"archived","external_key_id":"ext_1","residency":"us"}],"has_more":false}"#;
        let parsed: RawProjectPage = parse_json(page).expect("project page");
        let listing = OpenAiProjectListing {
            projects: parsed.data.into_iter().map(project).collect(),
            complete: !parsed.has_more,
        };
        assert_eq!(listing.projects[0].archived_at, Some(20));
        assert_eq!(listing.projects[0].status.as_deref(), Some("archived"));
        assert_eq!(
            listing.projects[0].external_key_id.as_deref(),
            Some("ext_1")
        );
        assert_eq!(listing.projects[0].residency.as_deref(), Some("us"));
    }

    #[test]
    fn usage_and_cost_provider_fields_are_preserved() {
        let usage = parse_usage(br#"{"data":[{"start_time":1,"end_time":2,"results":[{"input_tokens":3,"output_tokens":4,"input_cached_tokens":5,"num_model_requests":6,"project_id":"proj","api_key_id":"key","model":"gpt","batch":true}]}],"next_page":"p2"}"#).expect("usage");
        let result = &usage.buckets[0].results[0];
        assert_eq!(result.api_key_id.as_deref(), Some("key"));
        assert_eq!(result.model.as_deref(), Some("gpt"));
        assert_eq!(result.batch, Some(true));

        let costs = parse_costs(br#"{"data":[{"start_time":1,"end_time":2,"results":[{"amount":{"value":1.25,"currency":"usd"},"line_item":"text-input","project_id":"proj"}]}]}"#).expect("costs");
        assert_eq!(costs.buckets[0].results[0].amount.value, "1.25");
        assert_eq!(costs.buckets[0].results[0].amount.currency, "usd");
        assert_eq!(
            costs.buckets[0].results[0].line_item.as_deref(),
            Some("text-input")
        );
    }

    #[test]
    fn malformed_payload_is_distinct() {
        assert_eq!(
            parse_usage(b"not json"),
            Err(OpenAiOrgApiError::MalformedJson)
        );
    }

    #[test]
    fn status_classes_preserve_retry_after_without_body_details() {
        let headers =
            http::HeaderMap::from_iter([(RETRY_AFTER, http::HeaderValue::from_static("17"))]);
        assert_eq!(
            classify(StatusCode::TOO_MANY_REQUESTS, &headers, Bytes::new()),
            Err(OpenAiOrgApiError::RateLimited {
                retry_after_secs: Some(17)
            })
        );
        assert_eq!(
            classify(
                StatusCode::FORBIDDEN,
                &headers,
                Bytes::from_static(b"secret")
            ),
            Err(OpenAiOrgApiError::Denied { status: 403 })
        );
    }

    #[test]
    fn query_construction_is_bounded_and_grouped() {
        let query = OpenAiUsageQuery {
            start_time: 10,
            end_time: Some(20),
            bucket_width: OpenAiBucketWidth::Day,
            limit: 2,
            page: Some("cursor".into()),
        };
        let values = usage_query(&query);
        assert!(values.contains(&("group_by[]".into(), "project_id".into())));
        assert!(values.contains(&("group_by[]".into(), "api_key_id".into())));
        assert!(values.contains(&("group_by[]".into(), "model".into())));
        assert!(values.contains(&("page".into(), "cursor".into())));
        assert_eq!(values.iter().filter(|(name, _)| name == "limit").count(), 1);
    }
}
