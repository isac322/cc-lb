use std::collections::VecDeque;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use secrecy::SecretString;
use thiserror::Error;

/// Refresh buffer used by the signer and token providers.
pub const REFRESH_BUFFER: Duration = Duration::from_secs(60);

/// GCP OAuth bearer token material plus expiry metadata.
#[derive(Clone)]
pub struct GcpToken {
    /// Redacted bearer token value.
    pub value: SecretString,
    /// Absolute expiry time for the bearer token.
    pub expires_at: SystemTime,
    /// OAuth scopes attached to the token.
    pub scopes: Vec<String>,
}

impl GcpToken {
    /// Creates a token from plaintext bearer text.
    pub fn new(value: impl Into<String>, expires_at: SystemTime, scopes: Vec<String>) -> Self {
        Self {
            value: SecretString::new(value.into().into_boxed_str()),
            expires_at,
            scopes,
        }
    }

    /// Returns true when the token expires before the provided window elapses.
    pub fn expires_within(&self, window: Duration) -> bool {
        match self.expires_at.duration_since(SystemTime::now()) {
            Ok(remaining) => remaining < window,
            Err(_) => true,
        }
    }
}

impl fmt::Debug for GcpToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GcpToken")
            .field("value", &"[REDACTED]")
            .field("expires_at", &self.expires_at)
            .field("scopes", &self.scopes)
            .finish()
    }
}

/// Errors returned while loading or refreshing GCP OAuth tokens.
#[derive(Debug, Error)]
pub enum GcpTokenError {
    /// No ADC credential source was available.
    #[error("missing GCP credentials: {reason}")]
    MissingCredentials {
        /// Redacted failure reason.
        reason: String,
    },
    /// Credentials were present but invalid.
    #[error("invalid GCP credentials: {reason}")]
    InvalidCredentials {
        /// Redacted failure reason.
        reason: String,
    },
    /// Token provider execution failed.
    #[error("GCP token provider failed: {reason}")]
    Provider {
        /// Redacted failure reason.
        reason: String,
    },
}

/// Async token provider used by the GCP OAuth signer.
#[async_trait]
pub trait GcpTokenProvider: fmt::Debug + Send + Sync {
    /// Returns a token for the requested scopes.
    async fn get_token(&self, scopes: &[String]) -> Result<GcpToken, GcpTokenError>;

    /// Forces a refresh for the requested scopes.
    async fn refresh_token(&self, scopes: &[String]) -> Result<GcpToken, GcpTokenError> {
        self.get_token(scopes).await
    }
}

/// Injectable deterministic token provider for tests.
#[derive(Clone)]
pub struct StaticGcpTokenProvider {
    current: Arc<Mutex<GcpToken>>,
    refresh_tokens: Arc<Mutex<VecDeque<GcpToken>>>,
    refresh_error: Arc<Mutex<Option<String>>>,
    get_calls: Arc<AtomicU64>,
    refresh_calls: Arc<AtomicU64>,
}

impl StaticGcpTokenProvider {
    /// Creates a provider that always returns the supplied token until refreshed.
    pub fn new(token: GcpToken) -> Self {
        Self {
            current: Arc::new(Mutex::new(token)),
            refresh_tokens: Arc::new(Mutex::new(VecDeque::new())),
            refresh_error: Arc::new(Mutex::new(None)),
            get_calls: Arc::new(AtomicU64::new(0)),
            refresh_calls: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Adds tokens returned by subsequent forced refresh calls.
    pub fn with_refresh_tokens(self, tokens: Vec<GcpToken>) -> Self {
        if let Ok(mut refresh_tokens) = self.refresh_tokens.lock() {
            *refresh_tokens = tokens.into();
        }
        self
    }

    /// Makes refresh calls fail with the provided redacted reason.
    pub fn with_refresh_error(self, reason: impl Into<String>) -> Self {
        if let Ok(mut refresh_error) = self.refresh_error.lock() {
            *refresh_error = Some(reason.into());
        }
        self
    }

    /// Number of get_token calls observed.
    pub fn get_calls(&self) -> u64 {
        self.get_calls.load(Ordering::Relaxed)
    }

    /// Number of refresh_token calls observed.
    pub fn refresh_calls(&self) -> u64 {
        self.refresh_calls.load(Ordering::Relaxed)
    }
}

impl fmt::Debug for StaticGcpTokenProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StaticGcpTokenProvider")
            .field("current", &"[REDACTED]")
            .field("refresh_tokens", &"[REDACTED]")
            .field("refresh_error", &self.has_refresh_error())
            .field("get_calls", &self.get_calls())
            .field("refresh_calls", &self.refresh_calls())
            .finish()
    }
}

impl StaticGcpTokenProvider {
    fn has_refresh_error(&self) -> bool {
        self.refresh_error
            .lock()
            .map(|refresh_error| refresh_error.is_some())
            .unwrap_or(true)
    }

    fn lock_error() -> GcpTokenError {
        GcpTokenError::Provider {
            reason: "static GCP token provider lock poisoned".to_owned(),
        }
    }
}

#[async_trait]
impl GcpTokenProvider for StaticGcpTokenProvider {
    async fn get_token(&self, _scopes: &[String]) -> Result<GcpToken, GcpTokenError> {
        self.get_calls.fetch_add(1, Ordering::Relaxed);
        self.current
            .lock()
            .map(|token| token.clone())
            .map_err(|_| Self::lock_error())
    }

    async fn refresh_token(&self, _scopes: &[String]) -> Result<GcpToken, GcpTokenError> {
        self.refresh_calls.fetch_add(1, Ordering::Relaxed);

        if let Some(reason) = self
            .refresh_error
            .lock()
            .map_err(|_| Self::lock_error())?
            .clone()
        {
            return Err(GcpTokenError::Provider { reason });
        }

        let token = {
            let mut refresh_tokens = self.refresh_tokens.lock().map_err(|_| Self::lock_error())?;
            refresh_tokens.pop_front()
        };

        if let Some(token) = token {
            let mut current = self.current.lock().map_err(|_| Self::lock_error())?;
            *current = token.clone();
            Ok(token)
        } else {
            self.current
                .lock()
                .map(|token| token.clone())
                .map_err(|_| Self::lock_error())
        }
    }
}
