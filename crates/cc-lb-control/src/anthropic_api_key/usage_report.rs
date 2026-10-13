//! `GET /v1/organizations/usage_report/messages` reader used by the usage
//! poller: one page per call, grouped by model, filtered to one API key or
//! one workspace.

use chrono::DateTime;
use serde::Deserialize;
use serde_json::Value;

use super::client::AnthropicOrgApi;
use super::org_probe::{OrgApiError, USAGE_REPORT_PATH, parse, rfc3339};

/// Bucket width of a usage report request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum UsageBucketWidth {
    H1,
    D1,
}

impl UsageBucketWidth {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::H1 => "1h",
            Self::D1 => "1d",
        }
    }

    pub const fn secs(self) -> u32 {
        match self {
            Self::H1 => 3_600,
            Self::D1 => 86_400,
        }
    }
}

/// Subject a usage report is filtered to.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum UsageFilter {
    /// An `apikey_...` id.
    ApiKey(String),
    /// A `wrkspc_...` id.
    Workspace(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UsageReportQuery {
    pub starting_at_unix_secs: u64,
    pub ending_at_unix_secs: u64,
    pub bucket_width: UsageBucketWidth,
    pub filter: UsageFilter,
    pub limit: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UsageReportPage {
    pub buckets: Vec<UsageReportBucket>,
    pub has_more: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UsageReportBucket {
    pub starting_at_unix_secs: u64,
    pub ending_at_unix_secs: u64,
    pub results: Vec<UsageReportResult>,
}

/// Tokens for one model in one bucket. `model` is `None` when the provider
/// reports no model.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UsageReportResult {
    pub model: Option<String>,
    pub uncached_input_tokens: u64,
    pub cache_creation_5m_input_tokens: u64,
    pub cache_creation_1h_input_tokens: u64,
    pub cache_read_input_tokens: u64,
    pub output_tokens: u64,
}

#[derive(Deserialize)]
struct RawPage {
    data: Vec<RawBucket>,
    #[serde(default)]
    has_more: bool,
}

#[derive(Deserialize)]
struct RawBucket {
    starting_at: String,
    ending_at: String,
    #[serde(default)]
    results: Vec<RawResult>,
}

#[derive(Deserialize)]
struct RawResult {
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    uncached_input_tokens: Value,
    #[serde(default)]
    cache_creation: Option<RawCacheCreation>,
    #[serde(default)]
    cache_read_input_tokens: Value,
    #[serde(default)]
    output_tokens: Value,
}

#[derive(Deserialize)]
struct RawCacheCreation {
    #[serde(default)]
    ephemeral_5m_input_tokens: Value,
    #[serde(default)]
    ephemeral_1h_input_tokens: Value,
}

impl AnthropicOrgApi {
    /// One page of the usage report for `query`, grouped by model.
    pub async fn usage_report(
        &self,
        key: &str,
        query: &UsageReportQuery,
    ) -> Result<UsageReportPage, OrgApiError> {
        let starting_at = rfc3339(query.starting_at_unix_secs)?;
        let ending_at = rfc3339(query.ending_at_unix_secs)?;
        let limit = query.limit.to_string();
        let (filter_name, filter_value) = match &query.filter {
            UsageFilter::ApiKey(id) => ("api_key_ids[]", id.as_str()),
            UsageFilter::Workspace(id) => ("workspace_ids[]", id.as_str()),
        };
        let body = self
            .org_get(
                key,
                USAGE_REPORT_PATH,
                &[
                    ("starting_at", starting_at.as_str()),
                    ("ending_at", ending_at.as_str()),
                    ("bucket_width", query.bucket_width.as_str()),
                    ("limit", limit.as_str()),
                    ("group_by[]", "model"),
                    (filter_name, filter_value),
                ],
            )
            .await?;
        parse_page(&body)
    }
}

/// Parses a usage report page. A bucket with unreadable times is
/// malformed; a result with a negative or non-numeric token count is
/// dropped, and fractional or huge counts saturate.
fn parse_page(body: &[u8]) -> Result<UsageReportPage, OrgApiError> {
    let raw = parse::<RawPage>(body)?;
    let buckets = raw
        .data
        .into_iter()
        .map(|bucket| {
            Ok(UsageReportBucket {
                starting_at_unix_secs: unix_secs(&bucket.starting_at)?,
                ending_at_unix_secs: unix_secs(&bucket.ending_at)?,
                results: bucket.results.into_iter().filter_map(result).collect(),
            })
        })
        .collect::<Result<Vec<_>, OrgApiError>>()?;
    Ok(UsageReportPage {
        buckets,
        has_more: raw.has_more,
    })
}

fn result(raw: RawResult) -> Option<UsageReportResult> {
    let (cache_5m, cache_1h) = match raw.cache_creation {
        Some(cache) => (
            tokens(&cache.ephemeral_5m_input_tokens)?,
            tokens(&cache.ephemeral_1h_input_tokens)?,
        ),
        None => (0, 0),
    };
    Some(UsageReportResult {
        model: raw.model.filter(|model| !model.is_empty()),
        uncached_input_tokens: tokens(&raw.uncached_input_tokens)?,
        cache_creation_5m_input_tokens: cache_5m,
        cache_creation_1h_input_tokens: cache_1h,
        cache_read_input_tokens: tokens(&raw.cache_read_input_tokens)?,
        output_tokens: tokens(&raw.output_tokens)?,
    })
}

/// A token count: missing or `null` is 0; negative or non-numeric is `None`.
fn tokens(value: &Value) -> Option<u64> {
    match value {
        Value::Null => Some(0),
        Value::Number(number) => number.as_u64().or_else(|| {
            number
                .as_f64()
                .filter(|float| float.is_finite() && *float >= 0.0)
                // `as` saturates at u64::MAX.
                .map(|float| float as u64)
        }),
        _ => None,
    }
}

fn unix_secs(text: &str) -> Result<u64, OrgApiError> {
    DateTime::parse_from_rfc3339(text)
        .ok()
        .and_then(|time| u64::try_from(time.timestamp()).ok())
        .ok_or(OrgApiError::Malformed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_documented_page() {
        let body = serde_json::json!({
            "data": [{
                "starting_at": "2026-10-12T00:00:00Z",
                "ending_at": "2026-10-12T01:00:00Z",
                "results": [{
                    "model": "claude-x",
                    "uncached_input_tokens": 10,
                    "cache_creation": {"ephemeral_5m_input_tokens": 2, "ephemeral_1h_input_tokens": 3},
                    "cache_read_input_tokens": 4,
                    "output_tokens": 5
                }, {
                    "model": null,
                    "uncached_input_tokens": 1e30,
                    "output_tokens": 1
                }, {
                    "model": "neg",
                    "uncached_input_tokens": -1
                }]
            }],
            "has_more": true,
            "next_page": "page_2"
        });
        let page = parse_page(body.to_string().as_bytes());
        let Ok(page) = page else {
            panic!("page parses: {page:?}");
        };
        assert!(page.has_more);
        assert_eq!(page.buckets.len(), 1);
        let bucket = &page.buckets[0];
        assert_eq!(
            bucket.ending_at_unix_secs - bucket.starting_at_unix_secs,
            3600
        );
        assert_eq!(bucket.results.len(), 2);
        assert_eq!(
            bucket.results[0],
            UsageReportResult {
                model: Some("claude-x".to_owned()),
                uncached_input_tokens: 10,
                cache_creation_5m_input_tokens: 2,
                cache_creation_1h_input_tokens: 3,
                cache_read_input_tokens: 4,
                output_tokens: 5,
            }
        );
        assert_eq!(bucket.results[1].model, None);
        assert_eq!(bucket.results[1].uncached_input_tokens, u64::MAX);
    }

    #[test]
    fn missing_data_is_malformed() {
        assert_eq!(
            parse_page(br#"{"has_more":true}"#),
            Err(OrgApiError::Malformed)
        );
        assert_eq!(parse_page(b"<html>"), Err(OrgApiError::Malformed));
    }
}
