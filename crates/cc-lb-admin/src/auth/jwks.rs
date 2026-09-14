use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ring::signature::{RSA_PKCS1_2048_8192_SHA256, RsaPublicKeyComponents};
use serde::Deserialize;
use tokio::sync::Mutex;

const DEFAULT_TTL: Duration = Duration::from_secs(60 * 60);
const REFRESH_RETRY_INTERVAL: Duration = Duration::from_secs(5 * 60);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(3);

pub(crate) struct JwksCache {
    url: String,
    client: reqwest::Client,
    keys: ArcSwap<HashMap<String, Arc<RsaPublicKey>>>,
    fetched_at: StdMutex<Option<Instant>>,
    last_miss_refetch: StdMutex<Option<Instant>>,
    last_failed_refresh: StdMutex<Option<Instant>>,
    ttl: Duration,
    refresh_lock: Mutex<()>,
}

impl JwksCache {
    pub(crate) fn new(url: String) -> Self {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let client = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()
            .expect("valid JWKS HTTP client configuration");
        Self {
            url,
            client,
            keys: ArcSwap::from_pointee(HashMap::new()),
            fetched_at: StdMutex::new(None),
            last_miss_refetch: StdMutex::new(None),
            last_failed_refresh: StdMutex::new(None),
            ttl: DEFAULT_TTL,
            refresh_lock: Mutex::new(()),
        }
    }

    pub(crate) async fn get(&self, kid: &str) -> Result<Arc<RsaPublicKey>, JwksError> {
        if self.cache_is_fresh()
            && let Some(key) = self.cached_key(kid)
        {
            return Ok(key);
        }

        let _refresh_guard = self.refresh_lock.lock().await;

        if self.cache_is_fresh()
            && let Some(key) = self.cached_key(kid)
        {
            return Ok(key);
        }

        let now = Instant::now();
        let cached_key = self.cached_key(kid);
        if self.refresh_failure_is_recent(now)
            && let Some(key) = &cached_key
        {
            return Ok(Arc::clone(key));
        }

        if cached_key.is_none() && self.has_fetched() && !self.reserve_miss_refetch(now) {
            return Err(JwksError::UnknownKid);
        }
        if cached_key.is_none() && !self.has_fetched() && self.refresh_failure_is_recent(now) {
            return Err(JwksError::RefreshThrottled);
        }

        match self.refresh().await {
            Ok(()) => self.clear_refresh_failure(),
            Err(error) => {
                self.mark_refresh_failure(now);
                return cached_key.ok_or(error);
            }
        }

        if let Some(key) = self.cached_key(kid) {
            return Ok(key);
        }

        self.mark_miss_refetch(Instant::now());
        Err(JwksError::UnknownKid)
    }

    fn cached_key(&self, kid: &str) -> Option<Arc<RsaPublicKey>> {
        self.keys.load().get(kid).cloned()
    }

    fn has_fetched(&self) -> bool {
        self.fetched_at
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .is_some()
    }

    fn cache_is_fresh(&self) -> bool {
        self.fetched_at
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .is_some_and(|fetched_at| fetched_at.elapsed() < self.ttl)
    }

    fn reserve_miss_refetch(&self, now: Instant) -> bool {
        let mut last_refetch = self
            .last_miss_refetch
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if last_refetch
            .as_ref()
            .is_some_and(|last| now.saturating_duration_since(*last) < REFRESH_RETRY_INTERVAL)
        {
            return false;
        }
        *last_refetch = Some(now);
        true
    }

    fn mark_miss_refetch(&self, now: Instant) {
        *self
            .last_miss_refetch
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(now);
    }

    fn refresh_failure_is_recent(&self, now: Instant) -> bool {
        self.last_failed_refresh
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
            .is_some_and(|last| now.saturating_duration_since(*last) < REFRESH_RETRY_INTERVAL)
    }

    fn mark_refresh_failure(&self, now: Instant) {
        *self
            .last_failed_refresh
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(now);
    }

    fn clear_refresh_failure(&self) {
        *self
            .last_failed_refresh
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
    }

    async fn refresh(&self) -> Result<(), JwksError> {
        let response = self
            .client
            .get(&self.url)
            .send()
            .await?
            .error_for_status()?;
        let body = response.bytes().await?;
        let document: JwksDocument =
            sonic_rs::from_slice(&body).map_err(|_| JwksError::InvalidDocument)?;

        let mut keys = HashMap::with_capacity(document.keys.len());
        for jwk in document.keys {
            if jwk.kid.is_empty()
                || jwk.kty != "RSA"
                || jwk
                    .algorithm
                    .as_deref()
                    .is_some_and(|algorithm| algorithm != "RS256")
                || jwk
                    .key_use
                    .as_deref()
                    .is_some_and(|key_use| key_use != "sig")
            {
                continue;
            }
            let (Ok(modulus), Ok(exponent)) = (
                URL_SAFE_NO_PAD.decode(jwk.modulus),
                URL_SAFE_NO_PAD.decode(jwk.exponent),
            ) else {
                continue;
            };
            if modulus.is_empty() || exponent.is_empty() {
                continue;
            }
            keys.insert(jwk.kid, Arc::new(RsaPublicKey { modulus, exponent }));
        }

        if keys.is_empty() {
            return Err(JwksError::InvalidDocument);
        }

        self.keys.store(Arc::new(keys));
        *self
            .fetched_at
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Instant::now());
        Ok(())
    }
}

pub(crate) struct RsaPublicKey {
    modulus: Vec<u8>,
    exponent: Vec<u8>,
}

impl RsaPublicKey {
    pub(crate) fn verify_rs256(
        &self,
        message: &[u8],
        signature: &[u8],
    ) -> Result<(), ring::error::Unspecified> {
        RsaPublicKeyComponents {
            n: &self.modulus,
            e: &self.exponent,
        }
        .verify(&RSA_PKCS1_2048_8192_SHA256, message, signature)
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum JwksError {
    #[error("JWKS request failed: {0}")]
    Request(#[from] reqwest::Error),
    #[error("JWKS document has no usable RSA signing keys")]
    InvalidDocument,
    #[error("JWKS refresh retry was throttled")]
    RefreshThrottled,
    #[error("JWT key id was not found in JWKS")]
    UnknownKid,
}

#[derive(Deserialize)]
struct JwksDocument {
    keys: Vec<Jwk>,
}

#[derive(Deserialize)]
struct Jwk {
    kid: String,
    kty: String,
    #[serde(rename = "alg")]
    algorithm: Option<String>,
    #[serde(rename = "use")]
    key_use: Option<String>,
    #[serde(rename = "n")]
    modulus: String,
    #[serde(rename = "e")]
    exponent: String,
}
