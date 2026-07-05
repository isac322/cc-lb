use std::time::Duration;

use bytes::Bytes;
use http::header::{ACCEPT, AUTHORIZATION, USER_AGENT};
use http::{HeaderValue, Request};
use http_body_util::{BodyExt, Full};
use hyper_rustls::HttpsConnectorBuilder;
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use serde::Deserialize;
use serde_json::Value;
use thiserror::Error;
use tokio_util::sync::CancellationToken;

const PROFILE_URL: &str = "https://api.anthropic.com/api/oauth/profile";
const ROLES_URL: &str = "https://api.anthropic.com/api/oauth/claude_cli/roles";
const OVERAGE_URL_PREFIX: &str = "https://api.anthropic.com/api/oauth/organizations/";
const FETCH_TIMEOUT: Duration = Duration::from_secs(10);

pub type MetadataHttpClient = Client<hyper_rustls::HttpsConnector<HttpConnector>, Full<Bytes>>;

#[derive(Debug, Error, Eq, PartialEq)]
pub enum MetadataFetchError {
    #[error("metadata fetch HTTP error: {0}")]
    Http(String),
    #[error("metadata fetch parse error: {0}")]
    Parse(String),
    #[error("metadata fetch timed out")]
    Timeout,
    #[error("metadata fetch cancelled")]
    Cancelled,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ProfileResponse {
    #[serde(skip)]
    pub raw_body: Vec<u8>,
    #[serde(default)]
    pub account: Option<ProfileAccount>,
    #[serde(default)]
    pub organization: Option<ProfileOrganization>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ProfileAccount {
    #[serde(default)]
    pub uuid: Option<String>,
    #[serde(default)]
    pub email: Option<String>,
    #[serde(default)]
    pub display_name: Option<String>,
    #[serde(default)]
    pub created_at: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ProfileOrganization {
    #[serde(default)]
    pub uuid: Option<String>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub organization_name: Option<String>,
    #[serde(default)]
    pub organization_type: Option<String>,
    #[serde(default)]
    pub rate_limit_tier: Option<String>,
    #[serde(default)]
    pub seat_tier: Option<String>,
    #[serde(default)]
    pub has_extra_usage_enabled: Option<bool>,
    #[serde(default)]
    pub billing_type: Option<String>,
    #[serde(default)]
    pub subscription_created_at: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RolesResponse {
    #[serde(skip)]
    pub raw_body: Vec<u8>,
    #[serde(default)]
    pub organization_uuid: Option<String>,
    #[serde(default)]
    pub organization_role: Option<String>,
    #[serde(default)]
    pub workspace_role: Option<String>,
    #[serde(default)]
    pub raw_bootstrap: Option<Value>,
    #[serde(default)]
    pub bootstrap: Option<Value>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct OverageGrantResponse {
    #[serde(skip)]
    pub raw_body: Vec<u8>,
    #[serde(default)]
    pub amount_minor_units: Option<i64>,
    #[serde(default)]
    pub currency: Option<String>,
    #[serde(default)]
    pub granted: Option<bool>,
    #[serde(default)]
    pub eligible: Option<bool>,
    #[serde(default)]
    pub overage_credit: Option<OverageCredit>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct OverageCredit {
    #[serde(default)]
    pub amount_minor_units: Option<i64>,
    #[serde(default)]
    pub currency: Option<String>,
    #[serde(default)]
    pub granted: Option<bool>,
    #[serde(default)]
    pub eligible: Option<bool>,
}

pub fn make_metadata_http_client() -> MetadataHttpClient {
    let connector = HttpsConnectorBuilder::new()
        .with_webpki_roots()
        .https_or_http()
        .enable_http1()
        .enable_http2()
        .build();
    Client::builder(TokioExecutor::new()).build(connector)
}

pub async fn fetch_oauth_profile(
    client: &MetadataHttpClient,
    access_token: &str,
    user_agent: &str,
    cancel: &CancellationToken,
) -> Result<ProfileResponse, MetadataFetchError> {
    let body = fetch_json(client, PROFILE_URL, access_token, user_agent, cancel).await?;
    let mut parsed: ProfileResponse = serde_json::from_slice(&body)
        .map_err(|error| MetadataFetchError::Parse(error.to_string()))?;
    parsed.raw_body = body.to_vec();
    Ok(parsed)
}

pub async fn fetch_claude_cli_roles(
    client: &MetadataHttpClient,
    access_token: &str,
    user_agent: &str,
    cancel: &CancellationToken,
) -> Result<RolesResponse, MetadataFetchError> {
    let body = fetch_json(client, ROLES_URL, access_token, user_agent, cancel).await?;
    let mut parsed: RolesResponse = serde_json::from_slice(&body)
        .map_err(|error| MetadataFetchError::Parse(error.to_string()))?;
    parsed.raw_body = body.to_vec();
    Ok(parsed)
}

pub async fn fetch_overage_credit_grant(
    client: &MetadataHttpClient,
    access_token: &str,
    user_agent: &str,
    org_uuid: &str,
    cancel: &CancellationToken,
) -> Result<OverageGrantResponse, MetadataFetchError> {
    let url = format!("{OVERAGE_URL_PREFIX}{org_uuid}/overage_credit_grant");
    let body = fetch_json(client, &url, access_token, user_agent, cancel).await?;
    let mut parsed: OverageGrantResponse = serde_json::from_slice(&body)
        .map_err(|error| MetadataFetchError::Parse(error.to_string()))?;
    parsed.raw_body = body.to_vec();
    Ok(parsed)
}

async fn fetch_json(
    client: &MetadataHttpClient,
    url: &str,
    access_token: &str,
    user_agent: &str,
    cancel: &CancellationToken,
) -> Result<Bytes, MetadataFetchError> {
    let authorization = HeaderValue::from_str(&format!("Bearer {access_token}"))
        .map_err(|error| MetadataFetchError::Http(error.to_string()))?;
    let user_agent = HeaderValue::from_str(user_agent)
        .map_err(|error| MetadataFetchError::Http(error.to_string()))?;
    let request = Request::get(url)
        .header(AUTHORIZATION, authorization)
        .header(USER_AGENT, user_agent)
        .header(ACCEPT, "application/json")
        .body(Full::new(Bytes::new()))
        .map_err(|error| MetadataFetchError::Http(error.to_string()))?;

    let response = tokio::select! {
        _ = cancel.cancelled() => return Err(MetadataFetchError::Cancelled),
        response = tokio::time::timeout(FETCH_TIMEOUT, client.request(request)) => {
            response
                .map_err(|_| MetadataFetchError::Timeout)?
                .map_err(|error| MetadataFetchError::Http(error.to_string()))?
        }
    };
    let status = response.status();
    if !status.is_success() {
        return Err(MetadataFetchError::Http(format!("{url} returned {status}")));
    }
    let body = tokio::select! {
        _ = cancel.cancelled() => return Err(MetadataFetchError::Cancelled),
        body = tokio::time::timeout(FETCH_TIMEOUT, response.into_body().collect()) => {
            body
                .map_err(|_| MetadataFetchError::Timeout)?
                .map_err(|error| MetadataFetchError::Http(error.to_string()))?
                .to_bytes()
        }
    };
    Ok(body)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn anthropic_metadata_profile_parses_unknown_tolerant_shape() {
        let json = br#"{
          "account": {"uuid":"acct-1","email":"local@example.com","display_name":"Local","created_at":"2024-01-01T00:00:00Z"},
          "organization": {"uuid":"org-1","name":"Org","organization_type":"claude_max","rate_limit_tier":"tier-1","has_extra_usage_enabled":true,"billing_type":"subscription","subscription_created_at":"1700000000"},
          "future_field": {"ok": true}
        }"#;
        let parsed: ProfileResponse = serde_json::from_slice(json).expect("parse profile");
        assert_eq!(
            parsed.account.unwrap().email.as_deref(),
            Some("local@example.com")
        );
        let organization = parsed.organization.unwrap();
        assert_eq!(organization.uuid.as_deref(), Some("org-1"));
        assert_eq!(organization.has_extra_usage_enabled, Some(true));
    }

    #[test]
    fn anthropic_metadata_roles_parses_defaulted_fields() {
        let json = br#"{"organization_role":"admin","workspace_role":"developer","bootstrap":{"workspace":"default"}}"#;
        let parsed: RolesResponse = serde_json::from_slice(json).expect("parse roles");
        assert_eq!(parsed.organization_role.as_deref(), Some("admin"));
        assert_eq!(parsed.workspace_role.as_deref(), Some("developer"));
        assert!(parsed.bootstrap.is_some());
    }

    #[test]
    fn anthropic_metadata_overage_parses_flat_and_nested_shapes() {
        let flat: OverageGrantResponse = serde_json::from_slice(
            br#"{"amount_minor_units":1000,"currency":"USD","granted":false,"eligible":true}"#,
        )
        .expect("parse flat overage");
        assert_eq!(flat.amount_minor_units, Some(1000));
        let nested: OverageGrantResponse = serde_json::from_slice(
            br#"{"overage_credit":{"amount_minor_units":2000,"currency":"USD","granted":true,"eligible":true}}"#,
        )
        .expect("parse nested overage");
        assert_eq!(
            nested.overage_credit.unwrap().amount_minor_units,
            Some(2000)
        );
    }
}
