//! Org Admin API probes P2-P4: workspaces, usage report access, and API-key
//! id resolution. They always go to [`AnthropicOrgApi::origin`], never to an
//! upstream's `base_url`; whether they run at all is the caller's host rule.

use cc_lb_storage_api::{ApiKeyOrgCapability, ApiKeyType};
use chrono::{DateTime, Utc};
use http::{Method, StatusCode};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use url::Url;

use super::api_key_id::validate_api_key_id;
use super::client::AnthropicOrgApi;
use super::probe::{ProbeFailure, ProbeRequest, ProbeResponse, echoes_key, send};
use super::wire::AnthropicWorkspace;
use super::workspace_id::validate_workspace_id;

const WORKSPACES_PATH: &str = "v1/organizations/workspaces";
const API_KEYS_PATH: &str = "v1/organizations/api_keys";
pub(crate) const USAGE_REPORT_PATH: &str = "v1/organizations/usage_report/messages";
const PAGE_LIMIT: &str = "100";

/// Why an org Admin API call produced no usable answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum OrgApiError {
    /// 401/403: the key's account lacks access.
    #[error("org API denied the request (HTTP {status})")]
    Denied { status: u16 },
    /// 429, with the `retry-after` seconds when given.
    #[error("org API rate limited the request")]
    RateLimited { retry_after_secs: Option<u64> },
    /// Any other non-2xx status.
    #[error("org API answered HTTP {status}")]
    Upstream { status: u16 },
    #[error("org API timed out")]
    Timeout,
    /// Connection failure or a redirect, which is never followed.
    #[error("org API unreachable")]
    Unreachable,
    /// A 2xx body that is not the documented shape, or too large.
    #[error("org API answered malformed data")]
    Malformed,
}

impl OrgApiError {
    /// Capability recorded for a probe that failed this way.
    pub fn capability(self) -> ApiKeyOrgCapability {
        match self {
            Self::Denied { .. } => ApiKeyOrgCapability::Denied,
            Self::RateLimited { .. }
            | Self::Upstream { .. }
            | Self::Timeout
            | Self::Unreachable
            | Self::Malformed => ApiKeyOrgCapability::Error,
        }
    }
}

/// One `GET /v1/organizations/api_keys` item, reduced to what resolution
/// needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ListedApiKey {
    pub id: String,
    pub partial_key_hint: String,
    pub key_type: Option<ApiKeyType>,
}

/// The `GET /v1/organizations/api_keys` items fetched within the page cap.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ApiKeyListing {
    pub keys: Vec<ListedApiKey>,
    /// False when the provider reported more keys than were fetched: the
    /// page cap was reached with `has_more`, or `has_more` lacked a cursor.
    pub complete: bool,
}

struct Pages<T> {
    items: Vec<T>,
    complete: bool,
}

#[derive(Deserialize)]
struct ListPage<T> {
    data: Vec<T>,
    #[serde(default)]
    has_more: bool,
    #[serde(default)]
    last_id: Option<String>,
}

#[derive(Deserialize)]
struct WorkspaceItem {
    id: String,
    #[serde(default)]
    name: String,
}

#[derive(Deserialize)]
struct ApiKeyItem {
    id: String,
    #[serde(default)]
    partial_key_hint: Option<String>,
    #[serde(default)]
    principal: Option<Principal>,
}

#[derive(Deserialize)]
struct Principal {
    #[serde(rename = "type")]
    kind: String,
}

#[derive(Deserialize)]
struct UsageProbePage {
    #[serde(rename = "data")]
    _data: Vec<serde_json::Value>,
}

impl AnthropicOrgApi {
    /// P2: workspaces the key's account can see, at most
    /// [`ProbeLimits::page_cap`](super::client::ProbeLimits) pages.
    ///
    /// Items whose id is not a valid workspace id or echoes `key` are
    /// dropped, and every fragment of `key` in a name is masked, so provider
    /// metadata never carries the credential into responses or storage.
    pub async fn list_workspaces(&self, key: &str) -> Result<Vec<AnthropicWorkspace>, OrgApiError> {
        let items = self
            .list_pages::<WorkspaceItem>(key, WORKSPACES_PATH)
            .await?
            .items;
        Ok(items
            .into_iter()
            .filter(|item| validate_workspace_id(&item.id) && !echoes_key(key, &item.id))
            .map(|item| AnthropicWorkspace {
                name: cc_lb_upstream::scrub_secret(key, &item.name).into_owned(),
                id: item.id,
            })
            .collect())
    }

    /// P3: whether the key may read usage reports. One `1h` bucket, one page.
    pub async fn probe_usage_report(
        &self,
        key: &str,
        now_unix_secs: u64,
    ) -> Result<(), OrgApiError> {
        let hour_start = now_unix_secs - now_unix_secs % 3600;
        let starting_at = rfc3339(hour_start.saturating_sub(3600))?;
        let body = self
            .org_get(
                key,
                USAGE_REPORT_PATH,
                &[
                    ("starting_at", starting_at.as_str()),
                    ("bucket_width", "1h"),
                    ("limit", "1"),
                ],
            )
            .await?;
        parse::<UsageProbePage>(&body).map(|_| ())
    }

    /// P4: the org's API keys, at most `page_cap` pages, and whether that
    /// covered every key.
    ///
    /// Items whose id is not a valid API-key id or echoes `key` are dropped:
    /// the id is persisted as `api_key_id` and later sent as the usage-report
    /// subject, so a malformed or echoing provider id would create a durable
    /// secret sink.
    pub(crate) async fn list_api_keys(&self, key: &str) -> Result<ApiKeyListing, OrgApiError> {
        let Pages { items, complete } = self.list_pages::<ApiKeyItem>(key, API_KEYS_PATH).await?;
        let keys = items
            .into_iter()
            .filter(|item| validate_api_key_id(&item.id) && !echoes_key(key, &item.id))
            .filter_map(|item| {
                let key_type = match item.principal.as_ref().map(|p| p.kind.as_str()) {
                    None => Some(ApiKeyType::Workspace),
                    Some("user_actor") => Some(ApiKeyType::Personal),
                    Some("service_account_actor") => Some(ApiKeyType::ServiceAccount),
                    Some(_) => None,
                };
                item.partial_key_hint.map(|partial_key_hint| ListedApiKey {
                    id: item.id,
                    partial_key_hint,
                    key_type,
                })
            })
            .collect();
        Ok(ApiKeyListing { keys, complete })
    }

    /// Follows `after_id` cursors for at most `page_cap` pages. The listing
    /// is complete only when a page reports `has_more: false`.
    async fn list_pages<T: DeserializeOwned>(
        &self,
        key: &str,
        path: &str,
    ) -> Result<Pages<T>, OrgApiError> {
        let mut items = Vec::new();
        let mut after_id: Option<String> = None;
        for _ in 0..self.limits().page_cap {
            let mut query = vec![("limit", PAGE_LIMIT)];
            if let Some(after_id) = after_id.as_deref() {
                query.push(("after_id", after_id));
            }
            let body = self.org_get(key, path, &query).await?;
            let page = parse::<ListPage<T>>(&body)?;
            items.extend(page.data);
            if !page.has_more {
                return Ok(Pages {
                    items,
                    complete: true,
                });
            }
            match page.last_id {
                Some(last_id) => after_id = Some(last_id),
                None => break,
            }
        }
        Ok(Pages {
            items,
            complete: false,
        })
    }

    /// One GET against the org origin; returns the 2xx body.
    pub(crate) async fn org_get(
        &self,
        key: &str,
        path: &str,
        query: &[(&str, &str)],
    ) -> Result<bytes::Bytes, OrgApiError> {
        let mut url: Url = self
            .origin()
            .join(path)
            .map_err(|_| OrgApiError::Unreachable)?;
        if !query.is_empty() {
            url.query_pairs_mut().extend_pairs(query);
        }
        let response = send(
            self,
            self.limits().per_probe,
            ProbeRequest {
                method: Method::GET,
                url,
                key,
                workspace_id: None,
                json_body: None,
            },
        )
        .await
        .map_err(|failure| match failure {
            ProbeFailure::Timeout => OrgApiError::Timeout,
            ProbeFailure::Unreachable => OrgApiError::Unreachable,
            ProbeFailure::TooLarge => OrgApiError::Malformed,
        })?;
        classify(response)
    }
}

fn classify(response: ProbeResponse) -> Result<bytes::Bytes, OrgApiError> {
    let status = response.status;
    if status.is_success() {
        return Ok(response.body);
    }
    if status.is_redirection() {
        return Err(OrgApiError::Unreachable);
    }
    Err(match status {
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => OrgApiError::Denied {
            status: status.as_u16(),
        },
        StatusCode::TOO_MANY_REQUESTS => OrgApiError::RateLimited {
            retry_after_secs: response
                .headers
                .get(http::header::RETRY_AFTER)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.trim().parse().ok()),
        },
        other => OrgApiError::Upstream {
            status: other.as_u16(),
        },
    })
}

pub(crate) fn parse<T: DeserializeOwned>(body: &[u8]) -> Result<T, OrgApiError> {
    serde_json::from_slice(body).map_err(|_| OrgApiError::Malformed)
}

pub(crate) fn rfc3339(unix_secs: u64) -> Result<String, OrgApiError> {
    i64::try_from(unix_secs)
        .ok()
        .and_then(|secs| DateTime::<Utc>::from_timestamp(secs, 0))
        .map(|time| time.format("%Y-%m-%dT%H:%M:%SZ").to_string())
        .ok_or(OrgApiError::Malformed)
}

/// True when Anthropic's `partial_key_hint` (`<head>...<tail>`) fits `key`.
pub(crate) fn hint_matches(partial_key_hint: &str, key: &str) -> bool {
    partial_key_hint
        .split_once("...")
        .is_some_and(|(head, tail)| {
            !head.is_empty()
                && !tail.is_empty()
                && key.len() >= head.len() + tail.len()
                && key.starts_with(head)
                && key.ends_with(tail)
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hint_matching_uses_head_and_tail() {
        let key = "sk-ant-api03-R2DxxxxxxxxxxxxxxxxigAA";
        assert!(hint_matches("sk-ant-api03-R2D...igAA", key));
        assert!(hint_matches("sk-ant-api03-...igAA", key));
        assert!(!hint_matches("sk-ant-api03-R2D...igAB", key));
        assert!(!hint_matches("sk-ant-api03-R2DigAA", key));
        assert!(!hint_matches("...igAA", key));
        assert!(!hint_matches(
            "sk-ant-api03-R2Dxxxxxxx...xxxxxxxxxigAA",
            "sk-ant-api03-R2DigAA"
        ));
    }

    #[test]
    fn rfc3339_formats_utc() {
        assert_eq!(
            rfc3339(1_760_000_000).as_deref(),
            Ok("2025-10-09T08:53:20Z")
        );
    }

    #[test]
    fn denied_maps_to_denied_capability() {
        assert_eq!(
            OrgApiError::Denied { status: 403 }.capability(),
            ApiKeyOrgCapability::Denied
        );
        assert_eq!(
            OrgApiError::Timeout.capability(),
            ApiKeyOrgCapability::Error
        );
    }
}
