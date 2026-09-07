use std::{
    collections::HashMap,
    future::Future,
    hash::Hash,
    sync::{Arc, Mutex, MutexGuard, Weak},
    time::{Duration, Instant},
};

use axum::{
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use tokio::sync::watch;

// Principal totals include unrolled live-tail events, so keep this shorter than
// the dashboard's minute bucket width.
pub(crate) const PRINCIPAL_TOTALS_CACHE_TTL: Duration = Duration::from_secs(2);

pub(crate) type SharedBuildResult<V, E> = Result<Arc<V>, Arc<E>>;

#[derive(Clone, PartialEq, Eq, Hash)]
struct ScopedCacheKey<K> {
    owner_id: usize,
    key: K,
}

enum CacheEntry<V, E, O: ?Sized> {
    Ready {
        owner: Weak<O>,
        value: Arc<V>,
        expires_at: Instant,
    },
    InFlight {
        owner: Weak<O>,
        flight_id: u64,
        receiver: watch::Receiver<Option<SharedBuildResult<V, E>>>,
    },
}

struct CacheState<K, V, E, O: ?Sized> {
    next_flight_id: u64,
    entries: HashMap<ScopedCacheKey<K>, CacheEntry<V, E, O>>,
}

impl<K, V, E, O: ?Sized> Default for CacheState<K, V, E, O> {
    fn default() -> Self {
        Self {
            next_flight_id: 0,
            entries: HashMap::new(),
        }
    }
}

struct ShortTtlSingleFlightCacheInner<K, V, E, O: ?Sized> {
    ttl: Duration,
    state: Mutex<CacheState<K, V, E, O>>,
}

pub(crate) struct ShortTtlSingleFlightCache<K, V, E, O: ?Sized> {
    inner: Arc<ShortTtlSingleFlightCacheInner<K, V, E, O>>,
}

impl<K, V, E, O> ShortTtlSingleFlightCache<K, V, E, O>
where
    K: Clone + Eq + Hash + Send + 'static,
    V: Send + Sync + 'static,
    E: Send + Sync + 'static,
    O: ?Sized + Send + Sync + 'static,
{
    pub(crate) fn new(ttl: Duration) -> Self {
        Self {
            inner: Arc::new(ShortTtlSingleFlightCacheInner {
                ttl,
                state: Mutex::new(CacheState::default()),
            }),
        }
    }

    pub(crate) async fn get_or_build<F>(
        &self,
        owner: &Arc<O>,
        key: K,
        build: F,
    ) -> SharedBuildResult<V, E>
    where
        F: Future<Output = Result<V, E>> + Send + 'static,
    {
        let scoped_key = ScopedCacheKey {
            // The process-wide dashboard cache must never bridge distinct storage
            // instances, even when their normalized queries are identical.
            owner_id: Arc::as_ptr(owner) as *const () as usize,
            key,
        };
        let mut build = Some(build);

        loop {
            let (receiver, flight_id, leader) = {
                let now = Instant::now();
                let mut state = lock_cache_state(&self.inner.state);
                state.entries.retain(|_, entry| match entry {
                    CacheEntry::Ready {
                        owner, expires_at, ..
                    } => owner.strong_count() != 0 && *expires_at > now,
                    CacheEntry::InFlight {
                        owner, receiver, ..
                    } => owner.strong_count() != 0 && receiver.has_changed().is_ok(),
                });

                let existing_flight = match state.entries.get(&scoped_key) {
                    Some(CacheEntry::Ready {
                        owner: cached_owner,
                        value,
                        ..
                    }) if same_owner(cached_owner, owner) => return Ok(Arc::clone(value)),
                    Some(CacheEntry::InFlight {
                        owner: cached_owner,
                        flight_id,
                        receiver,
                    }) if same_owner(cached_owner, owner) && receiver.has_changed().is_ok() => {
                        Some((receiver.clone(), *flight_id))
                    }
                    _ => None,
                };
                if let Some((receiver, flight_id)) = existing_flight {
                    (receiver, flight_id, None)
                } else {
                    state.entries.remove(&scoped_key);
                    let (receiver, flight_id, sender, leader_build) =
                        start_flight(&mut state, owner, scoped_key.clone(), build.take());
                    (receiver, flight_id, Some((sender, leader_build)))
                }
            };

            if let Some((sender, leader_build)) = leader {
                let inner = Arc::clone(&self.inner);
                let owner = Arc::downgrade(owner);
                let leader_key = scoped_key.clone();
                // Detach the build from every request future. Cancelling a caller only
                // drops its receiver; the shared calculation still completes.
                tokio::spawn(async move {
                    let result = leader_build.await.map(Arc::new).map_err(Arc::new);
                    {
                        let mut state = lock_cache_state(&inner.state);
                        let flight_is_current = matches!(
                            state.entries.get(&leader_key),
                            Some(CacheEntry::InFlight {
                                flight_id: current_flight_id,
                                ..
                            }) if *current_flight_id == flight_id
                        );
                        // Successful values get the short TTL. Errors leave no entry,
                        // so the next request retries immediately.
                        if flight_is_current {
                            match &result {
                                Ok(value) => {
                                    state.entries.insert(
                                        leader_key,
                                        CacheEntry::Ready {
                                            owner,
                                            value: Arc::clone(value),
                                            expires_at: Instant::now() + inner.ttl,
                                        },
                                    );
                                }
                                Err(_) => {
                                    state.entries.remove(&leader_key);
                                }
                            }
                        }
                    }
                    let _ = sender.send(Some(result));
                });
            }

            match receive_flight(receiver).await {
                Ok(result) => return result,
                Err(()) => {
                    remove_flight_if_current(&self.inner, &scoped_key, flight_id);
                    if build.is_none() {
                        panic!("single-flight leader dropped without publishing a result");
                    }
                }
            }
        }
    }
}

type FlightStart<V, E, F> = (
    watch::Receiver<Option<SharedBuildResult<V, E>>>,
    u64,
    watch::Sender<Option<SharedBuildResult<V, E>>>,
    F,
);

fn start_flight<K, V, E, O: ?Sized, F>(
    state: &mut CacheState<K, V, E, O>,
    owner: &Arc<O>,
    scoped_key: ScopedCacheKey<K>,
    build: Option<F>,
) -> FlightStart<V, E, F>
where
    K: Eq + Hash,
{
    let flight_id = state.next_flight_id;
    state.next_flight_id = state.next_flight_id.wrapping_add(1);
    let (sender, receiver) = watch::channel(None);
    state.entries.insert(
        scoped_key,
        CacheEntry::InFlight {
            owner: Arc::downgrade(owner),
            flight_id,
            receiver: receiver.clone(),
        },
    );
    (
        receiver,
        flight_id,
        sender,
        build.expect("new single-flight entry has a builder"),
    )
}

async fn receive_flight<V, E>(
    mut receiver: watch::Receiver<Option<SharedBuildResult<V, E>>>,
) -> Result<SharedBuildResult<V, E>, ()> {
    loop {
        if let Some(result) = receiver.borrow().clone() {
            return Ok(result);
        }
        receiver.changed().await.map_err(|_| ())?;
    }
}

fn remove_flight_if_current<K, V, E, O: ?Sized>(
    inner: &ShortTtlSingleFlightCacheInner<K, V, E, O>,
    scoped_key: &ScopedCacheKey<K>,
    flight_id: u64,
) where
    K: Eq + Hash,
{
    let mut state = lock_cache_state(&inner.state);
    let flight_is_current = matches!(
        state.entries.get(scoped_key),
        Some(CacheEntry::InFlight {
            flight_id: current_flight_id,
            ..
        }) if *current_flight_id == flight_id
    );
    if flight_is_current {
        state.entries.remove(scoped_key);
    }
}

fn same_owner<O: ?Sized>(cached_owner: &Weak<O>, owner: &Arc<O>) -> bool {
    cached_owner
        .upgrade()
        .is_some_and(|cached_owner| Arc::ptr_eq(&cached_owner, owner))
}

fn lock_cache_state<T>(state: &Mutex<T>) -> MutexGuard<'_, T> {
    state.lock().unwrap_or_else(|error| error.into_inner())
}

pub(crate) fn matches_if_none_match(headers: &HeaderMap, expected: &str) -> bool {
    headers
        .get(header::IF_NONE_MATCH)
        .and_then(|value| value.to_str().ok())
        .map(|raw| raw.split(',').any(|tag| tag.trim() == expected))
        .unwrap_or(false)
}

pub(crate) fn not_modified_response(etag: &str) -> Response {
    let mut response = StatusCode::NOT_MODIFIED.into_response();
    apply_private_revalidation_headers(&mut response, Some(etag));
    response
}

pub(crate) fn apply_private_revalidation(mut response: Response, etag: Option<&str>) -> Response {
    apply_private_revalidation_headers(&mut response, etag);
    response
}

fn apply_private_revalidation_headers(response: &mut Response, etag: Option<&str>) {
    let headers = response.headers_mut();
    if let Some(etag) = etag
        && let Ok(value) = HeaderValue::from_str(etag)
    {
        headers.insert(header::ETAG, value);
    }
    headers.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("private, no-cache"),
    );
}
