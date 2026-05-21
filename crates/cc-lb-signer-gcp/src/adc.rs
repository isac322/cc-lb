use std::fmt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use dashmap::DashMap;

use crate::single_flight;
use crate::token::{GcpToken, GcpTokenError, GcpTokenProvider, REFRESH_BUFFER};

/// Application Default Credentials token provider backed by `gcp_auth`.
#[derive(Default)]
pub struct AdcTokenProvider {
    cache: DashMap<Vec<String>, GcpToken>,
}

impl AdcTokenProvider {
    /// Creates an ADC-backed provider with an empty token cache.
    pub fn new() -> Self {
        Self {
            cache: DashMap::new(),
        }
    }

    async fn load_fresh_token(&self, scopes: &[String]) -> Result<GcpToken, GcpTokenError> {
        let provider = gcp_auth::provider().await.map_err(adc_error)?;
        let scope_refs: Vec<&str> = scopes.iter().map(String::as_str).collect();
        let token = provider.token(&scope_refs).await.map_err(adc_error)?;
        let expires_at = system_time_from_unix(token.expires_at().timestamp());
        let gcp_token = GcpToken::new(token.as_str().to_owned(), expires_at, scopes.to_vec());
        self.cache
            .insert(single_flight::scope_key(scopes), gcp_token.clone());
        Ok(gcp_token)
    }
}

impl fmt::Debug for AdcTokenProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdcTokenProvider")
            .field("cache", &"[REDACTED]")
            .finish()
    }
}

#[async_trait]
impl GcpTokenProvider for AdcTokenProvider {
    async fn get_token(&self, scopes: &[String]) -> Result<GcpToken, GcpTokenError> {
        let key = single_flight::scope_key(scopes);
        if let Some(token) = self.cache.get(&key) {
            if !token.expires_within(REFRESH_BUFFER) {
                return Ok(token.clone());
            }
        }

        self.load_fresh_token(&key).await
    }

    async fn refresh_token(&self, scopes: &[String]) -> Result<GcpToken, GcpTokenError> {
        let key = single_flight::scope_key(scopes);
        self.cache.remove(&key);
        self.load_fresh_token(&key).await
    }
}

fn adc_error(source: gcp_auth::Error) -> GcpTokenError {
    GcpTokenError::Provider {
        reason: source.to_string(),
    }
}

fn system_time_from_unix(timestamp: i64) -> SystemTime {
    if timestamp >= 0 {
        UNIX_EPOCH + Duration::from_secs(timestamp.unsigned_abs())
    } else {
        UNIX_EPOCH - Duration::from_secs(timestamp.unsigned_abs())
    }
}
