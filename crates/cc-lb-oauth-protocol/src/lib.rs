#![forbid(unsafe_code)]

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Public installed-app client used by the OpenAI Codex device and browser flows.
pub const OPENAI_CODEX_CLIENT_ID: &str = "app_EMoamEEZ73f0CkXaXp7hrann";
pub const OPENAI_OAUTH_ISSUER_URL: &str = "https://auth.openai.com";
pub const OPENAI_OAUTH_AUTHORIZE_URL: &str = "https://auth.openai.com/oauth/authorize";
pub const OPENAI_OAUTH_TOKEN_URL: &str = "https://auth.openai.com/oauth/token";
pub const OPENAI_OAUTH_BROWSER_REDIRECT_URI: &str = "http://localhost:1455/auth/callback";
pub const OPENAI_OAUTH_DEVICE_USERCODE_URL: &str =
    "https://auth.openai.com/api/accounts/deviceauth/usercode";
pub const OPENAI_OAUTH_DEVICE_TOKEN_URL: &str =
    "https://auth.openai.com/api/accounts/deviceauth/token";
pub const OPENAI_OAUTH_DEVICE_VERIFICATION_URL: &str = "https://auth.openai.com/codex/device";
pub const OPENAI_OAUTH_DEVICE_REDIRECT_URI: &str = "https://auth.openai.com/deviceauth/callback";

/// OAuth endpoints used by the Gemini CLI OAuth flow.
pub const GEMINI_OAUTH_AUTH_URL: &str = "https://accounts.google.com/o/oauth2/v2/auth";
pub const GEMINI_OAUTH_TOKEN_URL: &str = "https://oauth2.googleapis.com/token";
pub const GEMINI_OAUTH_REDIRECT_URI: &str = "https://codeassist.google.com/authcode";
pub const GEMINI_OAUTH_CODE_ASSIST_URL: &str = "https://cloudcode-pa.googleapis.com/v1internal";

/// OAuth endpoints and request metadata used by the Antigravity Gemini subscription flow.
pub const ANTIGRAVITY_OAUTH_AUTH_URL: &str = "https://accounts.google.com/o/oauth2/v2/auth";
pub const ANTIGRAVITY_OAUTH_TOKEN_URL: &str = "https://oauth2.googleapis.com/token";
pub const ANTIGRAVITY_OAUTH_REDIRECT_URI: &str = "http://localhost:51121/oauth-callback";
pub const ANTIGRAVITY_OAUTH_CODE_ASSIST_URL: &str = "https://daily-cloudcode-pa.googleapis.com";
pub const ANTIGRAVITY_OAUTH_USERINFO_URL: &str =
    "https://www.googleapis.com/oauth2/v2/userinfo?alt=json";
pub const ANTIGRAVITY_OAUTH_USER_AGENT: &str =
    "antigravity/hub/2.24.0 (aidev_client; os_type=darwin; arch=arm64; cl=1000335682)";

pub const ANTIGRAVITY_OAUTH_SCOPES: &[&str] = &[
    "https://www.googleapis.com/auth/cloud-platform",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
    "https://www.googleapis.com/auth/cclog",
    "https://www.googleapis.com/auth/experimentsandconfigs",
];

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GeminiOAuthProfile {
    #[default]
    GeminiCli,
    Antigravity,
}

#[derive(Debug, Error)]
pub enum TokenEndpointParseError {
    #[error("oauth token response json failed: {source}")]
    Json { source: serde_json::Error },
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct TokenEndpointResponse {
    pub access_token: String,
    pub refresh_token: Option<String>,
    pub expires_in: u64,
    pub refresh_token_expires_in: Option<u64>,
    pub scope: Option<String>,
    #[serde(default)]
    pub account_id: Option<String>,
    #[serde(default)]
    pub id_token: Option<String>,
}

/// Display-only identity claims obtained during a provider OAuth exchange.
/// Decoding a JWT here is not signature verification or an authentication decision.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
pub struct OAuthAccountIdentity {
    pub account_email: Option<String>,
    pub account_id: Option<String>,
    pub plan: Option<String>,
    pub project_id: Option<String>,
    #[serde(default)]
    pub account_name: Option<String>,
    #[serde(default)]
    pub plan_name: Option<String>,
}

pub fn openai_identity_from_token(token: &str) -> OAuthAccountIdentity {
    let Some(claims) = token
        .split('.')
        .nth(1)
        .and_then(|payload| URL_SAFE_NO_PAD.decode(payload).ok())
        .and_then(|decoded| serde_json::from_slice::<serde_json::Value>(&decoded).ok())
    else {
        return OAuthAccountIdentity::default();
    };
    let auth = claims.get("https://api.openai.com/auth");
    let claim = |value: Option<&serde_json::Value>| {
        value
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
    };
    OAuthAccountIdentity {
        account_email: claim(claims.get("email")).or_else(|| {
            claim(
                claims
                    .get("https://api.openai.com/profile")
                    .and_then(|value| value.get("email")),
            )
        }),
        account_id: claim(auth.and_then(|value| value.get("chatgpt_account_id"))),
        plan: claim(auth.and_then(|value| value.get("chatgpt_plan_type"))),
        project_id: None,
        account_name: claim(claims.get("name")),
        plan_name: None,
    }
}

/// Allowlisted display metadata from the existing Gemini setup endpoints.
/// The paid tier's identifier and name must always come from the same tier.
pub fn gemini_identity_from_responses(
    load: &serde_json::Value,
    userinfo: &serde_json::Value,
) -> OAuthAccountIdentity {
    let text = |value: Option<&serde_json::Value>| {
        value
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
    };
    let tier = ["paidTier", "currentTier"].into_iter().find_map(|key| {
        load.get(key).filter(|tier| {
            tier.get("id")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|id| !id.trim().is_empty())
        })
    });
    let project = load.get("cloudaicompanionProject");
    OAuthAccountIdentity {
        account_email: text(userinfo.get("email")),
        account_name: text(userinfo.get("name")),
        plan: tier.and_then(|tier| text(tier.get("id"))),
        plan_name: tier.and_then(|tier| text(tier.get("name"))),
        project_id: text(project).or_else(|| text(project.and_then(|project| project.get("id")))),
        ..OAuthAccountIdentity::default()
    }
}

pub fn account_id_from_id_token(id_token: &str) -> Option<String> {
    let payload = id_token.split('.').nth(1)?;
    let decoded = URL_SAFE_NO_PAD.decode(payload).ok()?;
    let claims = serde_json::from_slice::<serde_json::Value>(&decoded).ok()?;
    claims
        .get("https://api.openai.com/auth")
        .and_then(|auth| auth.get("chatgpt_account_id"))
        .and_then(serde_json::Value::as_str)
        .filter(|account_id| !account_id.is_empty())
        .map(str::to_owned)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExistingTokenParts {
    pub refresh_token: String,
    pub refresh_token_expires_at_unix_secs: Option<u64>,
    pub scopes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefreshedTokenParts {
    pub access_token: String,
    pub refresh_token: String,
    pub expires_at_unix_secs: u64,
    pub refresh_token_expires_at_unix_secs: Option<u64>,
    pub scopes: Vec<String>,
}

pub fn refresh_token_form_body(client_id: &str, refresh_token: &str) -> String {
    let mut serializer = url::form_urlencoded::Serializer::new(String::new());
    serializer.append_pair("grant_type", "refresh_token");
    serializer.append_pair("client_id", client_id);
    serializer.append_pair("refresh_token", refresh_token);
    serializer.finish()
}

pub fn gemini_refresh_token_form_body(
    client_id: &str,
    client_secret: &str,
    refresh_token: &str,
) -> String {
    let mut serializer = url::form_urlencoded::Serializer::new(String::new());
    serializer.append_pair("grant_type", "refresh_token");
    serializer.append_pair("client_id", client_id);
    serializer.append_pair("client_secret", client_secret);
    serializer.append_pair("refresh_token", refresh_token);
    serializer.finish()
}

pub fn refresh_token_json_body(client_id: &str, refresh_token: &str) -> String {
    serde_json::json!({
        "grant_type": "refresh_token",
        "client_id": client_id,
        "refresh_token": refresh_token,
    })
    .to_string()
}

pub fn parse_token_endpoint_response(
    body: &[u8],
) -> Result<TokenEndpointResponse, TokenEndpointParseError> {
    let value = serde_json::from_slice::<serde_json::Value>(body)
        .map_err(|source| TokenEndpointParseError::Json { source })?;
    let mut response = serde_json::from_value::<TokenEndpointResponse>(value.clone())
        .map_err(|source| TokenEndpointParseError::Json { source })?;
    if value.get("account_id").is_none() {
        response.account_id = account_id_from_access_token(&response.access_token).or_else(|| {
            response
                .id_token
                .as_deref()
                .and_then(account_id_from_id_token)
        });
    }
    Ok(response)
}

pub fn account_id_from_access_token(access_token: &str) -> Option<String> {
    let payload = access_token.split('.').nth(1)?;
    let decoded = URL_SAFE_NO_PAD.decode(payload).ok()?;
    let claims = serde_json::from_slice::<serde_json::Value>(&decoded).ok()?;
    claims
        .get("https://api.openai.com/auth")
        .and_then(|auth| auth.get("chatgpt_account_id"))
        .and_then(serde_json::Value::as_str)
        .filter(|account_id| !account_id.is_empty())
        .map(str::to_owned)
}

#[derive(Deserialize)]
struct RefreshErrorResponse<'a> {
    #[serde(borrow)]
    error: Option<&'a str>,
}

pub fn is_terminal_refresh_failure(status: u16, body: &[u8]) -> bool {
    match status {
        401 => true,
        400 => {
            serde_json::from_slice::<RefreshErrorResponse<'_>>(body)
                .ok()
                .and_then(|response| response.error)
                == Some("invalid_grant")
        }
        _ => false,
    }
}

pub fn refresh_requires_reconnect(reason: Option<&str>) -> bool {
    matches!(
        reason,
        Some("status_400" | "status_401" | "refresh_token_expired")
    )
}

pub fn refreshed_token_parts(
    existing: ExistingTokenParts,
    response: TokenEndpointResponse,
    now_epoch_secs: u64,
) -> RefreshedTokenParts {
    let scopes = response
        .scope
        .map(|scope| scope.split_whitespace().map(str::to_owned).collect())
        .unwrap_or(existing.scopes);
    let refresh_token_expires_at_unix_secs = response
        .refresh_token_expires_in
        .map(|expires_in| now_epoch_secs.saturating_add(expires_in))
        .or(existing.refresh_token_expires_at_unix_secs);

    RefreshedTokenParts {
        access_token: response.access_token,
        refresh_token: response.refresh_token.unwrap_or(existing.refresh_token),
        expires_at_unix_secs: now_epoch_secs.saturating_add(response.expires_in),
        refresh_token_expires_at_unix_secs,
        scopes,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn openai_identity_reads_email_plan_and_workspace_without_retaining_tokens()
    -> Result<(), Box<dyn std::error::Error>> {
        let token = jwt_access_token(serde_json::json!({
            "email": "alice@example.com",
            "https://api.openai.com/auth": {
                "chatgpt_account_id": "team-workspace",
                "chatgpt_plan_type": "team"
            }
        }));
        let identity = openai_identity_from_token(&token);
        assert_eq!(identity.account_email.as_deref(), Some("alice@example.com"));
        assert_eq!(identity.account_id.as_deref(), Some("team-workspace"));
        assert_eq!(identity.plan.as_deref(), Some("team"));
        assert_eq!(identity.project_id, None);
        assert!(!serde_json::to_string(&identity)?.contains(&token));
        assert_eq!(
            openai_identity_from_token("opaque"),
            OAuthAccountIdentity::default()
        );
        let profile = openai_identity_from_token(&jwt_access_token(serde_json::json!({
            "https://api.openai.com/profile": { "email": "alice@example.com" }
        })));
        assert_eq!(profile.account_email.as_deref(), Some("alice@example.com"));
        Ok(())
    }

    #[test]
    fn openai_identity_measured_claims_allowlist_name_without_unbound_organization()
    -> Result<(), Box<dyn std::error::Error>> {
        let token = jwt_access_token(serde_json::json!({
            "email": "member@example.test",
            "name": "Synthetic Member",
            "picture": "https://example.test/private-picture",
            "https://api.openai.com/auth": {
                "chatgpt_account_id": "synthetic-workspace",
                "chatgpt_plan_type": "prolite",
                "chatgpt_user_id": "private-user",
                "organizations": [
                    {"id": "unrelated-org", "is_default": true, "title": "Unbound Org", "role": "owner"}
                ]
            }
        }));
        let value = serde_json::to_value(openai_identity_from_token(&token))?;
        assert_eq!(value["account_name"], "Synthetic Member");
        assert_eq!(value["plan"], "prolite");
        assert!(value.get("organization_name").is_none());
        let raw = value.to_string();
        for private in [
            "private-picture",
            "private-user",
            "Unbound Org",
            token.as_str(),
        ] {
            assert!(!raw.contains(private));
        }
        let legacy: OAuthAccountIdentity = serde_json::from_value(serde_json::json!({
            "account_email": "member@example.test",
            "account_id": "synthetic-workspace", "plan": "team", "project_id": null
        }))?;
        assert_eq!(
            serde_json::to_value(legacy)?["account_name"],
            serde_json::Value::Null
        );
        Ok(())
    }

    #[test]
    fn refresh_form_body_encodes_refresh_token_grant() {
        // Given: an OAuth refresh token request with characters requiring encoding.
        let client_id = "client id";
        let refresh_token = "rt+/=";

        // When: the protocol helper builds a form-urlencoded request body.
        let body = refresh_token_form_body(client_id, refresh_token);

        // Then: the body carries the refresh grant contract exactly once.
        let parsed = url::form_urlencoded::parse(body.as_bytes()).collect::<Vec<_>>();
        assert_eq!(
            parsed,
            vec![
                ("grant_type".into(), "refresh_token".into()),
                ("client_id".into(), "client id".into()),
                ("refresh_token".into(), "rt+/=".into()),
            ]
        );
    }

    #[test]
    fn refresh_json_body_encodes_openai_refresh_grant() {
        let body = refresh_token_json_body("codex-client", "refresh-token");
        let parsed: serde_json::Value = serde_json::from_str(&body).expect("valid json");
        assert_eq!(
            parsed,
            serde_json::json!({
                "grant_type": "refresh_token",
                "client_id": "codex-client",
                "refresh_token": "refresh-token",
            })
        );
    }

    #[test]
    fn terminal_refresh_failure_classifies_oauth_error_responses() {
        assert!(is_terminal_refresh_failure(401, b""));
        assert!(is_terminal_refresh_failure(
            400,
            br#"{"error":"invalid_grant"}"#
        ));
        assert!(is_terminal_refresh_failure(
            400,
            br#"{"error":"invalid_grant","error_description":"expired"}"#
        ));

        assert!(!is_terminal_refresh_failure(
            400,
            br#"{"error":"invalid_client"}"#
        ));
        assert!(!is_terminal_refresh_failure(
            400,
            br#"{"error":"invalid_grant"#
        ));
        assert!(!is_terminal_refresh_failure(
            400,
            b"<html>bad request</html>"
        ));
        assert!(!is_terminal_refresh_failure(
            429,
            br#"{"error":"invalid_grant"}"#
        ));
        assert!(!is_terminal_refresh_failure(
            500,
            br#"{"error":"invalid_grant"}"#
        ));
    }

    #[test]
    fn refresh_reconnect_reasons_are_terminal_only() {
        for reason in ["status_400", "status_401", "refresh_token_expired"] {
            assert!(refresh_requires_reconnect(Some(reason)));
        }
        for reason in [None, Some("network"), Some("status_429"), Some("parse")] {
            assert!(!refresh_requires_reconnect(reason));
        }
    }

    #[test]
    fn refreshed_token_parts_preserve_missing_refresh_token_scope_and_expiry() {
        // Given: an endpoint response that omits optional refresh-token fields and scope.
        let response =
            parse_token_endpoint_response(br#"{"access_token":"new-access","expires_in":30}"#)
                .unwrap();
        let existing = ExistingTokenParts {
            refresh_token: "old-refresh".to_owned(),
            refresh_token_expires_at_unix_secs: Some(1_000),
            scopes: vec!["old:scope".to_owned()],
        };

        // When: the response is materialized into refreshed token parts.
        let refreshed = refreshed_token_parts(existing, response, 100);

        // Then: fallback fields remain unchanged and access expiry is relative to now.
        assert_eq!(refreshed.access_token, "new-access");
        assert_eq!(refreshed.refresh_token, "old-refresh");
        assert_eq!(refreshed.expires_at_unix_secs, 130);
        assert_eq!(refreshed.refresh_token_expires_at_unix_secs, Some(1_000));
        assert_eq!(refreshed.scopes, vec!["old:scope"]);
    }

    #[test]
    fn refreshed_token_parts_apply_returned_scope_and_expiries() {
        // Given: an endpoint response that rotates the token and both expiry clocks.
        let response = parse_token_endpoint_response(
            br#"{"access_token":"new-access","refresh_token":"new-refresh","expires_in":5,"refresh_token_expires_in":7,"scope":"a b c"}"#,
        )
        .unwrap();
        let existing = ExistingTokenParts {
            refresh_token: "old-refresh".to_owned(),
            refresh_token_expires_at_unix_secs: Some(500),
            scopes: vec!["old".to_owned()],
        };

        // When: the response is materialized into refreshed token parts.
        let refreshed = refreshed_token_parts(existing, response, u64::MAX - 1);

        // Then: returned token/scope win and both relative expiries saturate.
        assert_eq!(refreshed.refresh_token, "new-refresh");
        assert_eq!(refreshed.expires_at_unix_secs, u64::MAX);
        assert_eq!(refreshed.refresh_token_expires_at_unix_secs, Some(u64::MAX));
        assert_eq!(refreshed.scopes, vec!["a", "b", "c"]);
    }
    #[test]
    fn parse_account_id_from_jwt_access_token_with_top_level_precedence_and_opaque_fallback() {
        let jwt_access_token = "eyJhbGciOiJub25lIn0.eyJodHRwczovL2FwaS5vcGVuYWkuY29tL2F1dGgiOnsiY2hhdGdwdF9hY2NvdW50X2lkIjoiand0LWFjY291bnQifX0.signature";
        let jwt_response = parse_token_endpoint_response(
            format!(r#"{{"access_token":"{jwt_access_token}","expires_in":60}}"#).as_bytes(),
        )
        .expect("JWT token response parses");
        assert_eq!(jwt_response.account_id.as_deref(), Some("jwt-account"));

        let top_level_response = parse_token_endpoint_response(
            format!(
                r#"{{"access_token":"{jwt_access_token}","expires_in":60,"account_id":"response-account"}}"#
            )
            .as_bytes(),
        )
        .expect("top-level account id response parses");
        assert_eq!(
            top_level_response.account_id.as_deref(),
            Some("response-account")
        );

        let opaque_response =
            parse_token_endpoint_response(br#"{"access_token":"opaque-access","expires_in":60}"#)
                .expect("opaque token response parses");
        assert_eq!(opaque_response.account_id, None);
    }

    #[test]
    fn account_id_from_access_token_derives_reusable_openai_jwt_claim() {
        let access_token = jwt_access_token(serde_json::json!({
            "https://api.openai.com/auth": {
                "chatgpt_account_id": "jwt-account",
            },
        }));

        assert_eq!(
            account_id_from_access_token(&access_token).as_deref(),
            Some("jwt-account")
        );
    }

    #[test]
    fn account_id_from_access_token_rejects_opaque_malformed_and_empty_claims() {
        use base64::Engine as _;

        assert_eq!(account_id_from_access_token("opaque-access"), None);
        let malformed_claims = format!(
            "header.{}.signature",
            URL_SAFE_NO_PAD.encode(b"{malformed-json")
        );
        assert_eq!(account_id_from_access_token(&malformed_claims), None);
        assert_eq!(
            account_id_from_access_token("header.not-base64.signature"),
            None
        );
        assert_eq!(
            account_id_from_access_token(&jwt_access_token(serde_json::json!({}))),
            None
        );
        assert_eq!(
            account_id_from_access_token(&jwt_access_token(serde_json::json!({
                "https://api.openai.com/auth": {
                    "chatgpt_account_id": "",
                },
            }))),
            None
        );
    }

    #[test]
    fn parse_token_endpoint_response_preserves_explicit_top_level_account_id() {
        let access_token = jwt_access_token(serde_json::json!({
            "https://api.openai.com/auth": {
                "chatgpt_account_id": "jwt-account",
            },
        }));
        let response = parse_token_endpoint_response(
            serde_json::json!({
                "access_token": access_token,
                "expires_in": 60,
                "account_id": "response-account",
            })
            .to_string()
            .as_bytes(),
        )
        .expect("token response parses");

        assert_eq!(response.account_id.as_deref(), Some("response-account"));
    }

    fn jwt_access_token(claims: serde_json::Value) -> String {
        use base64::Engine as _;

        format!(
            "header.{}.signature",
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims).expect("claims serialize"))
        )
    }
}
