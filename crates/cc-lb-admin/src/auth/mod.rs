mod authorize;
pub(crate) mod cloudflare_access;
mod identity;
pub(crate) mod jwks;
pub(crate) mod static_token;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use axum::Json;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, Method, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use cc_lb_config::{AdminAuthConfig, AdminAuthProviderConfig};
use serde::Serialize;
use serde_json::json;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, SemaphorePermit};

pub use authorize::{AdminAction, authorize};
pub use identity::{AdminActorKind, AdminIdentity};

use self::cloudflare_access::CloudflareAccessProvider;
use self::static_token::StaticTokenProvider;
use crate::{
    AdminState,
    audit::{AdminAuditEvent, record_admin_audit, record_auth_rejected_with_suppressed},
};

const MAX_CONCURRENT_REJECT_SLEEPS: usize = 64;
const REJECT_SLEEP_DURATION: Duration = Duration::from_millis(100);
const REJECT_AUDIT_BURST: u32 = 10;
const REJECT_AUDIT_REFILL_INTERVAL: Duration = Duration::from_millis(100);
const MAX_CONCURRENT_REJECT_AUDITS: usize = 4;
const REJECT_AUDIT_WRITE_TIMEOUT: Duration = Duration::from_secs(2);

struct RejectAuditBucket {
    tokens: u32,
    burst: u32,
    refill_interval: Duration,
    last_refill: Option<Instant>,
}

impl RejectAuditBucket {
    fn new(burst: u32, refill_interval: Duration) -> Self {
        Self {
            tokens: burst,
            burst,
            refill_interval,
            last_refill: None,
        }
    }

    fn try_consume(&mut self, now: Instant) -> bool {
        let last = *self.last_refill.get_or_insert(now);
        if self.refill_interval.is_zero() {
            return true;
        }
        let elapsed = now.saturating_duration_since(last).as_nanos();
        let interval = self.refill_interval.as_nanos();
        let refills = u32::try_from(elapsed / interval).unwrap_or(u32::MAX);
        if refills > 0 {
            self.tokens = self.tokens.saturating_add(refills).min(self.burst);
            let advance = u128::from(refills).saturating_mul(interval);
            self.last_refill =
                Some(last + Duration::from_nanos(u64::try_from(advance).unwrap_or(u64::MAX)));
        }
        if self.tokens == 0 {
            return false;
        }
        self.tokens -= 1;
        true
    }
}

struct AuthRejectionGuard {
    sleep_semaphore: Arc<Semaphore>,
    audit_semaphore: Semaphore,
    audit_bucket: StdMutex<RejectAuditBucket>,
    suppressed_audit_count: AtomicU64,
}

impl AuthRejectionGuard {
    fn new(sleep_limit: usize, audit_burst: u32, audit_refill_interval: Duration) -> Self {
        Self {
            sleep_semaphore: Arc::new(Semaphore::new(sleep_limit)),
            audit_semaphore: Semaphore::new(MAX_CONCURRENT_REJECT_AUDITS),
            audit_bucket: StdMutex::new(RejectAuditBucket::new(audit_burst, audit_refill_interval)),
            suppressed_audit_count: AtomicU64::new(0),
        }
    }

    fn default_guard() -> Self {
        Self::new(
            MAX_CONCURRENT_REJECT_SLEEPS,
            REJECT_AUDIT_BURST,
            REJECT_AUDIT_REFILL_INTERVAL,
        )
    }

    fn try_acquire_sleep(&self) -> Option<OwnedSemaphorePermit> {
        Arc::clone(&self.sleep_semaphore).try_acquire_owned().ok()
    }

    fn try_begin_audit(&self, now: Instant) -> Option<(SemaphorePermit<'_>, u64)> {
        let permit = self.audit_semaphore.try_acquire().ok()?;
        let allowed = self
            .audit_bucket
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .try_consume(now);
        if !allowed {
            return None;
        }
        let suppressed = self.suppressed_audit_count.swap(0, Ordering::AcqRel);
        Some((permit, suppressed))
    }

    fn record_suppressed(&self, count: u64) {
        if count > 0 {
            self.suppressed_audit_count
                .fetch_add(count, Ordering::Relaxed);
        }
    }
}

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
    rejection_guard: AuthRejectionGuard,
}

impl AdminAuthenticator {
    pub fn new(providers: Vec<Arc<dyn AdminAuthProvider>>) -> Self {
        Self {
            providers,
            rejection_guard: AuthRejectionGuard::default_guard(),
        }
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
            let guard = &state.admin_auth.rejection_guard;
            let Some(_sleep_permit) = guard.try_acquire_sleep() else {
                guard.record_suppressed(1);
                return (
                    StatusCode::TOO_MANY_REQUESTS,
                    [(header::RETRY_AFTER, "1")],
                    Json(json!({
                        "error": "too_many_requests",
                        "auth_mode": state.admin_auth.mode(),
                    })),
                )
                    .into_response();
            };

            if let Some((_audit_permit, suppressed)) = guard.try_begin_audit(Instant::now()) {
                match tokio::time::timeout(
                    REJECT_AUDIT_WRITE_TIMEOUT,
                    record_auth_rejected_with_suppressed(
                        &state,
                        request.method(),
                        request.uri().path(),
                        error.reason(),
                        suppressed,
                    ),
                )
                .await
                {
                    Ok(Ok(())) => {}
                    Ok(Err(audit_error)) => {
                        guard.record_suppressed(suppressed);
                        tracing::warn!(
                            error = %audit_error,
                            reason = error.reason(),
                            "admin authentication rejection audit write failed"
                        );
                    }
                    Err(_) => {
                        guard.record_suppressed(suppressed);
                        tracing::warn!(
                            reason = error.reason(),
                            timeout_secs = REJECT_AUDIT_WRITE_TIMEOUT.as_secs(),
                            "admin authentication rejection audit write timed out"
                        );
                    }
                }
            } else {
                guard.record_suppressed(1);
            }
            tokio::time::sleep(REJECT_SLEEP_DURATION).await;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auth_rejection_guard_rolls_up_suppressed_audit_count_after_burst() {
        let guard = AuthRejectionGuard::new(4, 2, Duration::from_secs(10));
        let t0 = Instant::now();

        let (p1, sup1) = guard.try_begin_audit(t0).expect("burst 1");
        assert_eq!(sup1, 0);
        drop(p1);

        let (p2, sup2) = guard.try_begin_audit(t0).expect("burst 2");
        assert_eq!(sup2, 0);
        drop(p2);

        // Next 3 calls at t0 exhaust burst and are rolled up into suppressed count.
        for _ in 0..3 {
            assert!(guard.try_begin_audit(t0).is_none());
            guard.record_suppressed(1);
        }

        // Next audit after refill carries the rolled-up suppressed count.
        let (p3, sup3) = guard
            .try_begin_audit(t0 + Duration::from_secs(10))
            .expect("refill after 10s");
        assert_eq!(sup3, 3);
        drop(p3);
    }
}
