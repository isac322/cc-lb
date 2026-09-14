mod authorize;
pub(crate) mod cloudflare_access;
mod identity;
pub(crate) mod jwks;
pub(crate) mod static_token;

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::Json;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, Method, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use cc_lb_config::{AdminAuthConfig, AdminAuthProviderConfig};
use serde::Serialize;
use serde_json::json;

pub use authorize::{AdminAction, authorize};
pub use identity::{AdminActorKind, AdminIdentity};

use self::cloudflare_access::CloudflareAccessProvider;
use self::static_token::StaticTokenProvider;
use crate::{
    AdminState,
    audit::{AdminAuditEvent, record_admin_audit, record_auth_rejected},
};

pub enum ProviderOutcome {
    NotPresent,
    Verified(AdminIdentity),
    Rejected(&'static str),
}

#[async_trait]
pub trait AdminAuthProvider: Send + Sync {
    fn id(&self) -> &str;

    fn is_static_token(&self) -> bool {
        false
    }

    async fn authenticate(&self, headers: &HeaderMap) -> ProviderOutcome;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AdminAuthMode {
    StaticToken,
    External,
}

pub struct AdminAuthenticator {
    providers: Vec<Arc<dyn AdminAuthProvider>>,
}

impl AdminAuthenticator {
    pub fn new(providers: Vec<Arc<dyn AdminAuthProvider>>) -> Self {
        Self { providers }
    }

    pub fn mode(&self) -> AdminAuthMode {
        if !self.providers.is_empty()
            && self
                .providers
                .iter()
                .all(|provider| provider.is_static_token())
        {
            AdminAuthMode::StaticToken
        } else {
            AdminAuthMode::External
        }
    }

    pub async fn authenticate(&self, headers: &HeaderMap) -> Result<AdminIdentity, AdminAuthError> {
        if self.providers.is_empty() {
            return Err(AdminAuthError::NotConfigured);
        }

        let mut verified = None;
        let mut ambiguous = false;
        for provider in &self.providers {
            match provider.authenticate(headers).await {
                ProviderOutcome::NotPresent => {}
                ProviderOutcome::Rejected(reason) => {
                    return Err(AdminAuthError::Rejected(reason));
                }
                ProviderOutcome::Verified(identity) => {
                    if verified.is_some() {
                        ambiguous = true;
                    } else {
                        verified = Some(identity);
                    }
                }
            }
        }

        if ambiguous {
            return Err(AdminAuthError::Ambiguous);
        }
        verified.ok_or(AdminAuthError::Missing)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum AdminAuthError {
    #[error("admin credentials are missing")]
    Missing,
    #[error("admin credentials were rejected: {0}")]
    Rejected(&'static str),
    #[error("multiple admin identities were verified")]
    Ambiguous,
    #[error("admin authentication is not configured")]
    NotConfigured,
}

impl AdminAuthError {
    fn reason(self) -> &'static str {
        match self {
            Self::Missing => "missing_credentials",
            Self::Rejected(reason) => reason,
            Self::Ambiguous => "ambiguous_actor",
            Self::NotConfigured => "not_configured",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AdminAuthBuildError {
    #[error("admin authentication token environment variable is missing or empty: {0}")]
    MissingTokenEnv(String),
    #[error("admin authentication provider {provider_id} has an invalid header name: {header}")]
    InvalidHeader { provider_id: String, header: String },
}

pub fn build_providers(
    config: &AdminAuthConfig,
) -> Result<Vec<Arc<dyn AdminAuthProvider>>, AdminAuthBuildError> {
    config
        .providers
        .iter()
        .map(|provider| match provider {
            AdminAuthProviderConfig::StaticToken { id, token_env } => {
                let token = std::env::var(token_env)
                    .ok()
                    .filter(|token| !token.trim().is_empty())
                    .ok_or_else(|| AdminAuthBuildError::MissingTokenEnv(token_env.clone()))?;
                Ok(Arc::new(StaticTokenProvider::new(id.clone(), token))
                    as Arc<dyn AdminAuthProvider>)
            }
            AdminAuthProviderConfig::CloudflareAccess {
                id,
                team_domain,
                audiences,
                header,
            } => CloudflareAccessProvider::new(
                id.clone(),
                header.clone(),
                team_domain.clone(),
                audiences.clone(),
            )
            .map(|provider| Arc::new(provider) as Arc<dyn AdminAuthProvider>)
            .map_err(|()| AdminAuthBuildError::InvalidHeader {
                provider_id: id.clone(),
                header: header.clone(),
            }),
        })
        .collect()
}

pub async fn require_admin_auth(
    State(state): State<AdminState>,
    mut request: Request,
    next: Next,
) -> Response {
    let identity = match state.admin_auth.authenticate(request.headers()).await {
        Ok(identity) => identity,
        Err(error) => {
            if let Err(audit_error) = record_auth_rejected(
                &state,
                request.method(),
                request.uri().path(),
                error.reason(),
            )
            .await
            {
                tracing::warn!(
                    error = %audit_error,
                    reason = error.reason(),
                    "admin authentication rejection audit write failed"
                );
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
            return (
                StatusCode::UNAUTHORIZED,
                Json(json!({
                    "error": "unauthorized",
                    "auth_mode": state.admin_auth.mode(),
                })),
            )
                .into_response();
        }
    };

    let action = if request.method() == Method::GET || request.method() == Method::HEAD {
        AdminAction::Read
    } else {
        AdminAction::Write
    };
    if let Err(error) = authorize(&identity, action) {
        if let Err(audit_error) = record_admin_audit(
            &state,
            AdminAuditEvent {
                identity: Some(&identity),
                system_component: None,
                action: "authz_denied",
                route: request.uri().path(),
                target_principal_id: None,
                target_upstream: None,
                api_key_id: None,
                status: StatusCode::FORBIDDEN.as_u16(),
                payload: Some(json!({
                    "method": request.method().as_str(),
                    "reason": error.reason(),
                })),
            },
        )
        .await
        {
            tracing::warn!(
                error = %audit_error,
                reason = error.reason(),
                "admin authorization denial audit write failed"
            );
        }
        return (StatusCode::FORBIDDEN, Json(json!({ "error": "forbidden" }))).into_response();
    }

    request.extensions_mut().insert(identity);
    next.run(request).await
}
