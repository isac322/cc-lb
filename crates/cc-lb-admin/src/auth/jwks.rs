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
const UNKNOWN_KID_COOLDOWN: Duration = Duration::from_secs(5 * 60);
const MAX_TRACKED_UNKNOWN_KIDS: usize = 256;
const MISS_REFETCH_BURST: u32 = 3;
const MISS_REFETCH_REFILL_INTERVAL: Duration = Duration::from_secs(10);
const INITIAL_FAILURE_BACKOFF: Duration = Duration::from_secs(2);
const MAX_FAILURE_BACKOFF: Duration = Duration::from_secs(30);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(3);
const MAX_JWKS_BODY_BYTES: usize = 256 * 1024;

struct MissRefetchBucket {
    tokens: u32,
    burst: u32,
    refill_interval: Duration,
    last_refill: Option<Instant>,
}

impl MissRefetchBucket {
    fn new(burst: u32, refill_interval: Duration) -> Self {
        Self {
            tokens: burst,
            burst,
            refill_interval,
            last_refill: None,
        }
    }

    fn try_consume(&mut self, now: Instant) -> bool {
        let last_refill = *self.last_refill.get_or_insert(now);
        if self.refill_interval.is_zero() {
            return true;
        }
        let elapsed_nanos = now.saturating_duration_since(last_refill).as_nanos();
        let interval_nanos = self.refill_interval.as_nanos();
        let refills = u32::try_from(elapsed_nanos / interval_nanos).unwrap_or(u32::MAX);
        if refills > 0 {
            self.tokens = self.tokens.saturating_add(refills).min(self.burst);
            let advance_nanos = u128::from(refills).saturating_mul(interval_nanos);
            let advance = Duration::from_nanos(u64::try_from(advance_nanos).unwrap_or(u64::MAX));
            self.last_refill = Some(last_refill + advance);
        }
        if self.tokens == 0 {
            return false;
        }
        self.tokens -= 1;
        true
    }
}

struct FailureBackoffState {
    consecutive_failures: u32,
    next_retry_at: Option<Instant>,
    initial: Duration,
    max: Duration,
}

impl FailureBackoffState {
    fn new(initial: Duration, max: Duration) -> Self {
        Self {
            consecutive_failures: 0,
            next_retry_at: None,
            initial,
            max,
        }
    }

    fn is_active(&self, now: Instant) -> bool {
        self.next_retry_at.is_some_and(|retry_at| now < retry_at)
    }

    fn record_failure(&mut self, now: Instant) {
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        let shift = self.consecutive_failures.saturating_sub(1).min(16);
        let delay = self
            .initial
            .checked_mul(1_u32 << shift)
            .unwrap_or(self.max)
            .min(self.max);
        self.next_retry_at = Some(now + delay);
    }

    fn clear(&mut self) {
        self.consecutive_failures = 0;
        self.next_retry_at = None;
    }
}

pub(crate) struct JwksCache {
    url: String,
    client: reqwest::Client,
    keys: ArcSwap<HashMap<String, Arc<RsaPublicKey>>>,
    fetched_at: StdMutex<Option<Instant>>,
    unknown_kids: StdMutex<HashMap<String, Instant>>,
    unknown_kid_cooldown: Duration,
    miss_refetch_bucket: StdMutex<MissRefetchBucket>,
    failure_backoff: StdMutex<FailureBackoffState>,
    ttl: Duration,
    refresh_lock: Mutex<()>,
}

impl JwksCache {
    pub(crate) fn new(url: String) -> Self {
        Self::with_timings(
            url,
            DEFAULT_TTL,
            UNKNOWN_KID_COOLDOWN,
            MISS_REFETCH_BURST,
            MISS_REFETCH_REFILL_INTERVAL,
            INITIAL_FAILURE_BACKOFF,
            MAX_FAILURE_BACKOFF,
        )
    }

    pub(crate) fn with_timings(
        url: String,
        ttl: Duration,
        unknown_kid_cooldown: Duration,
        miss_refetch_burst: u32,
        miss_refetch_refill_interval: Duration,
        initial_failure_backoff: Duration,
        max_failure_backoff: Duration,
    ) -> Self {
        let _ = rustls::crypto::ring::default_provider().install_default();
        Self {
            url,
            client: reqwest::Client::builder()
                .timeout(REQUEST_TIMEOUT)
                .build()
                .expect("valid JWKS HTTP client configuration"),
            keys: ArcSwap::from_pointee(HashMap::new()),
            fetched_at: StdMutex::new(None),
            unknown_kids: StdMutex::new(HashMap::new()),
            unknown_kid_cooldown,
            miss_refetch_bucket: StdMutex::new(MissRefetchBucket::new(
                miss_refetch_burst,
                miss_refetch_refill_interval,
            )),
            failure_backoff: StdMutex::new(FailureBackoffState::new(
                initial_failure_backoff,
                max_failure_backoff,
            )),
            ttl,
            refresh_lock: Mutex::new(()),
        }
    }

    pub(crate) async fn get(&self, kid: &str) -> Result<Arc<RsaPublicKey>, JwksError> {
        self.get_with_clock(kid, Instant::now).await
    }

    #[cfg(test)]
    pub(crate) async fn get_at(
        &self,
        kid: &str,
        now: Instant,
    ) -> Result<Arc<RsaPublicKey>, JwksError> {
        self.get_with_clock(kid, || now).await
    }

    pub(crate) async fn get_with_clock(
        &self,
        kid: &str,
        mut now_fn: impl FnMut() -> Instant,
    ) -> Result<Arc<RsaPublicKey>, JwksError> {
        let check_now = now_fn();
        if self.cache_is_fresh(check_now)
            && let Some(key) = self.cached_key(kid)
        {
            return Ok(key);
        }
        if self.is_unknown_kid_cached(kid, check_now) {
            return Err(JwksError::UnknownKid);
        }

        let _refresh_guard = self.refresh_lock.lock().await;
        let locked_now = now_fn();

        if self.cache_is_fresh(locked_now)
            && let Some(key) = self.cached_key(kid)
        {
            return Ok(key);
        }
        if self.is_unknown_kid_cached(kid, locked_now) {
            return Err(JwksError::UnknownKid);
        }

        let cached_key = self.cached_key(kid);
        if self.refresh_failure_backoff_active(locked_now) {
            return cached_key.ok_or(JwksError::RefreshThrottled);
        }

        if cached_key.is_none()
            && self.has_fetched()
            && !self.consume_miss_refetch_token(locked_now)
        {
            return Err(JwksError::UnknownKid);
        }

        let refresh_result = self.refresh_with_clock(&mut now_fn).await;
        let completed_now = now_fn();
        match refresh_result {
            Ok(()) => self.clear_refresh_failure(),
            Err(error) => {
                self.mark_refresh_failure(completed_now);
                return cached_key.ok_or(error);
            }
        }

        if let Some(key) = self.cached_key(kid) {
            return Ok(key);
        }

        self.mark_unknown_kid(kid, completed_now);
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

    fn cache_is_fresh(&self, now: Instant) -> bool {
        self.fetched_at
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .is_some_and(|fetched_at| now.saturating_duration_since(fetched_at) < self.ttl)
    }

    fn is_unknown_kid_cached(&self, kid: &str, now: Instant) -> bool {
        if self.unknown_kid_cooldown.is_zero() {
            return false;
        }
        let mut unknown = self
            .unknown_kids
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(&seen_at) = unknown.get(kid) else {
            return false;
        };
        if now.saturating_duration_since(seen_at) < self.unknown_kid_cooldown {
            return true;
        }
        unknown.remove(kid);
        false
    }

    fn mark_unknown_kid(&self, kid: &str, now: Instant) {
        if self.unknown_kid_cooldown.is_zero() {
            return;
        }
        let mut unknown = self
            .unknown_kids
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let cooldown = self.unknown_kid_cooldown;
        unknown.retain(|_, seen_at| now.saturating_duration_since(*seen_at) < cooldown);
        if !unknown.contains_key(kid)
            && unknown.len() >= MAX_TRACKED_UNKNOWN_KIDS
            && let Some(oldest_kid) = unknown
                .iter()
                .min_by_key(|(_, seen_at)| **seen_at)
                .map(|(k, _)| k.clone())
        {
            unknown.remove(&oldest_kid);
        }
        unknown.insert(kid.to_owned(), now);
    }

    fn consume_miss_refetch_token(&self, now: Instant) -> bool {
        self.miss_refetch_bucket
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .try_consume(now)
    }

    fn refresh_failure_backoff_active(&self, now: Instant) -> bool {
        self.failure_backoff
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .is_active(now)
    }

    fn mark_refresh_failure(&self, now: Instant) {
        self.failure_backoff
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .record_failure(now);
    }

    fn clear_refresh_failure(&self) {
        self.failure_backoff
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clear();
    }

    async fn refresh_with_clock(
        &self,
        now_fn: &mut impl FnMut() -> Instant,
    ) -> Result<(), JwksError> {
        let mut response = self
            .client
            .get(&self.url)
            .send()
            .await?
            .error_for_status()?;
        if response
            .content_length()
            .is_some_and(|len| len > MAX_JWKS_BODY_BYTES as u64)
        {
            return Err(JwksError::InvalidDocument);
        }
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await? {
            if body.len().saturating_add(chunk.len()) > MAX_JWKS_BODY_BYTES {
                return Err(JwksError::InvalidDocument);
            }
            body.extend_from_slice(&chunk);
        }
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

        {
            let mut unknown = self
                .unknown_kids
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            unknown.retain(|kid, _| !keys.contains_key(kid));
        }
        self.keys.store(Arc::new(keys));
        *self
            .fetched_at
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(now_fn());
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    const TEST_MODULUS: &str = "w63MnzLXxLyjN_x5_44pkBeNkbY5kg88mwirAa7lzgrYjSRy36qTl8c7rXSMxhh9Qns9RDBNMQRRxcmAdv-zsBtSrwehh30yDBog01zPJ_hB3vKYDL8OW6t830A0FNzyEHWLxwNXKMpO5vaVvyWLWGZcIXLnnuY_0g9fAvvjk1LoAF8W15YuoT7LagXSxwUwBErlScqEjeHETG0cTc7r5764EiFDcVn7QpIU2OKN-B_ackbzoFSQzRJ4DgOaMJCRHoDrsq3F3q-LmaFE2oJ2NuLqwM-kYt7k-P2kXiD2J39fH6IHmnY1cRrHIQp6jtFaGImArK_mtGyAbPF2lCS9JQ";
    const TEST_EXPONENT: &str = "AQAB";

    fn valid_jwks_json(kids: &[&str]) -> String {
        let entries: Vec<_> = kids
            .iter()
            .map(|kid| {
                serde_json::json!({
                    "kid": kid,
                    "kty": "RSA",
                    "alg": "RS256",
                    "use": "sig",
                    "n": TEST_MODULUS,
                    "e": TEST_EXPONENT,
                })
            })
            .collect();
        serde_json::json!({ "keys": entries }).to_string()
    }

    async fn spawn_jwks_server<F>(handler: F) -> (String, Arc<AtomicUsize>)
    where
        F: Fn(usize) -> (u16, Vec<(String, String)>, String) + Send + Sync + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let hits = Arc::new(AtomicUsize::new(0));
        let hits_clone = Arc::clone(&hits);
        let handler = Arc::new(handler);

        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let hit = hits_clone.fetch_add(1, Ordering::SeqCst) + 1;
                let (status, extra_headers, body) = handler(hit);
                let mut buf = [0_u8; 2048];
                let _ = stream.read(&mut buf).await;
                let mut raw = format!(
                    "HTTP/1.1 {status} OK\r\nContent-Length: {}\r\nConnection: close\r\n",
                    body.len()
                );
                for (name, value) in extra_headers {
                    raw.push_str(&format!("{name}: {value}\r\n"));
                }
                raw.push_str("\r\n");
                let _ = stream.write_all(raw.as_bytes()).await;
                let _ = stream.write_all(body.as_bytes()).await;
                let _ = stream.shutdown().await;
            }
        });

        (format!("http://{addr}/certs"), hits)
    }

    #[tokio::test]
    async fn unknown_kid_uses_per_kid_negative_cache_and_global_miss_bucket() {
        let (url, hits) =
            spawn_jwks_server(|_| (200, Vec::new(), valid_jwks_json(&["good"]))).await;
        let cache = JwksCache::with_timings(
            url,
            Duration::from_secs(3600),
            Duration::from_secs(300),
            3,
            Duration::from_secs(10),
            Duration::from_secs(2),
            Duration::from_secs(30),
        );
        let t0 = Instant::now();

        assert!(cache.get_at("good", t0).await.is_ok());
        assert_eq!(hits.load(Ordering::SeqCst), 1);

        // Consumes token 1..3 for distinct unknown kids.
        assert!(matches!(
            cache.get_at("miss-1", t0).await,
            Err(JwksError::UnknownKid)
        ));
        assert!(matches!(
            cache.get_at("miss-2", t0).await,
            Err(JwksError::UnknownKid)
        ));
        assert!(matches!(
            cache.get_at("miss-3", t0).await,
            Err(JwksError::UnknownKid)
        ));
        assert_eq!(hits.load(Ordering::SeqCst), 4);

        // Repeating any tracked unknown kid stays cached negatively even after refill.
        let after_refill = t0 + Duration::from_secs(15);
        assert!(matches!(
            cache.get_at("miss-1", after_refill).await,
            Err(JwksError::UnknownKid)
        ));
        assert_eq!(hits.load(Ordering::SeqCst), 4);

        // Distinct unknown kids are gated by the global bucket (1 refill after 15s, then denied).
        assert!(matches!(
            cache.get_at("miss-4", after_refill).await,
            Err(JwksError::UnknownKid)
        ));
        assert!(matches!(
            cache.get_at("miss-5", after_refill).await,
            Err(JwksError::UnknownKid)
        ));
        assert_eq!(hits.load(Ordering::SeqCst), 5);
    }

    #[tokio::test]
    async fn refresh_failure_applies_exponential_backoff_and_preserves_stale_keys() {
        let (url, hits) = spawn_jwks_server(|hit| {
            if hit == 1 || hit >= 4 {
                (200, Vec::new(), valid_jwks_json(&["good"]))
            } else {
                (503, Vec::new(), "unavailable".to_owned())
            }
        })
        .await;
        let cache = JwksCache::with_timings(
            url,
            Duration::from_secs(60),
            Duration::from_secs(300),
            3,
            Duration::from_secs(10),
            Duration::from_secs(2),
            Duration::from_secs(30),
        );
        let t0 = Instant::now();

        assert!(cache.get_at("good", t0).await.is_ok());
        assert_eq!(hits.load(Ordering::SeqCst), 1);

        // At t0 + 61s cache is stale; hit 2 returns 503, stale key still served, 2s backoff armed.
        let t1 = t0 + Duration::from_secs(61);
        assert!(cache.get_at("good", t1).await.is_ok());
        assert_eq!(hits.load(Ordering::SeqCst), 2);

        // Within 2s backoff window: no upstream request, stale key still returned.
        assert!(
            cache
                .get_at("good", t1 + Duration::from_secs(1))
                .await
                .is_ok()
        );
        assert_eq!(hits.load(Ordering::SeqCst), 2);

        // At t1 + 2s: hit 3 returns 503 again, 4s backoff armed.
        let t2 = t1 + Duration::from_secs(2);
        assert!(cache.get_at("good", t2).await.is_ok());
        assert_eq!(hits.load(Ordering::SeqCst), 3);

        // At t2 + 3s (still within 4s exponential backoff): no upstream request.
        assert!(
            cache
                .get_at("good", t2 + Duration::from_secs(3))
                .await
                .is_ok()
        );
        assert_eq!(hits.load(Ordering::SeqCst), 3);

        // At t2 + 4s: backoff elapsed, hit 4 succeeds and resets backoff.
        assert!(
            cache
                .get_at("good", t2 + Duration::from_secs(4))
                .await
                .is_ok()
        );
        assert_eq!(hits.load(Ordering::SeqCst), 4);
    }

    #[test]
    fn unknown_kids_cache_remains_bounded_at_max_tracked() {
        let cache = JwksCache::new("http://127.0.0.1:0/certs".to_owned());
        let base = Instant::now();
        for idx in 0..(MAX_TRACKED_UNKNOWN_KIDS + 40) {
            cache.mark_unknown_kid(
                &format!("kid-{idx}"),
                base + Duration::from_millis(idx as u64),
            );
        }
        let guard = cache.unknown_kids.lock().expect("lock");
        assert_eq!(guard.len(), MAX_TRACKED_UNKNOWN_KIDS);
        assert!(!guard.contains_key("kid-0"));
        assert!(guard.contains_key(&format!("kid-{}", MAX_TRACKED_UNKNOWN_KIDS + 39)));
    }

    #[tokio::test]
    async fn jwks_response_exceeding_256_kib_is_rejected() {
        let oversized = "x".repeat(MAX_JWKS_BODY_BYTES + 128);
        let (url, _hits) = spawn_jwks_server(move |_| (200, Vec::new(), oversized.clone())).await;
        let cache = JwksCache::new(url);
        let result = cache.get_at("any", Instant::now()).await;
        assert!(matches!(result, Err(JwksError::InvalidDocument)));
    }

    #[tokio::test]
    async fn retry_backoff_starts_at_refresh_completion_not_entry() {
        let (url, hits) =
            spawn_jwks_server(|_| (503, Vec::new(), "upstream unavailable".to_owned())).await;
        let cache = JwksCache::with_timings(
            url,
            Duration::from_secs(60),
            Duration::from_secs(300),
            3,
            Duration::from_secs(10),
            Duration::from_secs(2),
            Duration::from_secs(30),
        );
        let t0 = Instant::now();
        // Simulates a 3s HTTP fetch failing after entry at t0: check_now=t0, locked_now=t0,
        // fetch completion=t0+3s. Backoff of 2s must expire at t0+5s, not t0+2s.
        let calls = AtomicUsize::new(0);
        let err = cache
            .get_with_clock("good", || {
                let n = calls.fetch_add(1, Ordering::SeqCst);
                if n < 2 {
                    t0
                } else {
                    t0 + Duration::from_secs(3)
                }
            })
            .await
            .err()
            .expect("failed initial fetch returns error");
        assert!(matches!(err, JwksError::Request(_)));
        assert_eq!(hits.load(Ordering::SeqCst), 1);

        // At t0 + 4s (only 1s after completion at t0 + 3s): still in 2s backoff -> throttled.
        let err_throttled = cache
            .get_at("good", t0 + Duration::from_secs(4))
            .await
            .err()
            .expect("backoff still active 1s after failure completion");
        assert!(matches!(err_throttled, JwksError::RefreshThrottled));
        assert_eq!(hits.load(Ordering::SeqCst), 1);

        // At t0 + 5s (2s after completion at t0 + 3s): backoff elapsed -> retries upstream.
        let err_retry = cache
            .get_at("good", t0 + Duration::from_secs(5))
            .await
            .err()
            .expect("retry fails with 503");
        assert!(matches!(err_retry, JwksError::Request(_)));
        assert_eq!(hits.load(Ordering::SeqCst), 2);
    }
}
