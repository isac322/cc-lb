use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use dashmap::DashMap;
use tokio::sync::oneshot;
use tracing::warn;

use cc_lb_storage_api::{CacheKeepaliveConfig, CacheTtl};

use super::metrics::{self, CancelReason};
use super::request_snapshot::RequestSnapshot;
use super::session_key::SessionKey;

const MAX_ACTIVE_SESSIONS_HARD_CAP: usize = 10_000;

#[derive(Clone, Debug)]
pub struct ScheduleParams {
    pub delay: Duration,
    pub max_refreshes: u32,
    pub max_total_duration_secs: u64,
}

impl ScheduleParams {
    pub fn from(config: &CacheKeepaliveConfig, ttl: CacheTtl) -> Self {
        Self {
            delay: Duration::from_secs(config.refresh_delay_secs(ttl)),
            max_refreshes: config.max_refreshes_per_session,
            max_total_duration_secs: config.max_total_duration_secs,
        }
    }
}

#[async_trait]
pub trait KeepaliveDispatcher: Send + Sync + 'static {
    async fn dispatch(&self, snapshot: &RequestSnapshot) -> DispatchOutcome;
}

pub enum DispatchOutcome {
    CacheHit,
    CacheMiss,
    Error(String),
}

pub struct KeepaliveScheduler {
    sessions: Arc<DashMap<SessionKey, SessionEntry>>,
    dispatcher: Arc<dyn KeepaliveDispatcher>,
}

struct SessionEntry {
    generation: u64,
    refresh_count: u32,
    first_scheduled_at: Instant,
    snapshot: Arc<RequestSnapshot>,
    params: ScheduleParams,
    cancel_tx: Option<oneshot::Sender<()>>,
    principal_name: Arc<str>,
}

impl KeepaliveScheduler {
    pub fn new(dispatcher: Arc<dyn KeepaliveDispatcher>) -> Arc<Self> {
        Arc::new(Self {
            sessions: Arc::new(DashMap::new()),
            dispatcher,
        })
    }

    pub fn active_session_count(&self) -> usize {
        self.sessions.len()
    }

    pub fn schedule_or_replace(
        self: &Arc<Self>,
        key: SessionKey,
        snapshot: Arc<RequestSnapshot>,
        params: ScheduleParams,
        principal_name: Arc<str>,
    ) {
        if !self.sessions.contains_key(&key) && self.sessions.len() >= MAX_ACTIVE_SESSIONS_HARD_CAP
        {
            metrics::record_cancelled(&principal_name, CancelReason::ProcessCapExceeded);
            return;
        }
        self.install_entry(
            key,
            snapshot,
            params,
            principal_name,
            /* preserve_counters */ false,
        );
    }

    pub fn cancel(&self, key: &SessionKey, reason: CancelReason) -> bool {
        let Some((_, mut entry)) = self.sessions.remove(key) else {
            return false;
        };
        if let Some(tx) = entry.cancel_tx.take() {
            let _ = tx.send(());
        }
        metrics::record_cancelled(&entry.principal_name, reason);
        metrics::set_active_sessions(&entry.principal_name, self.sessions.len());
        true
    }

    pub async fn shutdown(&self) {
        let keys: Vec<SessionKey> = self
            .sessions
            .iter()
            .map(|entry| entry.key().clone())
            .collect();
        for key in keys {
            self.cancel(&key, CancelReason::Shutdown);
        }
    }

    fn install_entry(
        self: &Arc<Self>,
        key: SessionKey,
        snapshot: Arc<RequestSnapshot>,
        params: ScheduleParams,
        principal_name: Arc<str>,
        preserve_counters: bool,
    ) {
        let (cancel_tx, cancel_rx) = oneshot::channel();
        let (generation, refresh_count, first_scheduled_at, prev_cancel_tx, prev_principal_name) = {
            match self.sessions.get_mut(&key) {
                Some(mut prev) => {
                    let next_generation = prev.generation.wrapping_add(1);
                    let refresh_count = if preserve_counters {
                        prev.refresh_count
                    } else {
                        0
                    };
                    let first_scheduled_at = if preserve_counters {
                        prev.first_scheduled_at
                    } else {
                        Instant::now()
                    };
                    let prev_tx = prev.cancel_tx.take();
                    (
                        next_generation,
                        refresh_count,
                        first_scheduled_at,
                        prev_tx,
                        Some(Arc::clone(&prev.principal_name)),
                    )
                }
                None => (1, 0, Instant::now(), None, None),
            }
        };

        let ttl_label = ttl_label(snapshot.ttl);
        let entry = SessionEntry {
            generation,
            refresh_count,
            first_scheduled_at,
            snapshot: Arc::clone(&snapshot),
            params: params.clone(),
            cancel_tx: Some(cancel_tx),
            principal_name: Arc::clone(&principal_name),
        };
        self.sessions.insert(key.clone(), entry);

        if let Some(tx) = prev_cancel_tx {
            let _ = tx.send(());
            if !preserve_counters && let Some(prev_name) = prev_principal_name {
                metrics::record_cancelled(&prev_name, CancelReason::NewRequest);
            }
        }

        let this = Arc::clone(self);
        let key_clone = key.clone();
        let delay = params.delay;
        tokio::spawn(async move {
            tokio::select! {
                _ = tokio::time::sleep(delay) => {
                    this.fire(key_clone, generation).await;
                }
                _ = cancel_rx => {}
            }
        });

        metrics::record_scheduled(&principal_name, ttl_label);
        metrics::set_active_sessions(&principal_name, self.sessions.len());
    }

    async fn fire(self: Arc<Self>, key: SessionKey, expected_generation: u64) {
        let (snapshot, params, refresh_count, first_scheduled_at, principal_name) = {
            let Some(entry) = self.sessions.get(&key) else {
                return;
            };
            if entry.generation != expected_generation {
                return;
            }
            (
                Arc::clone(&entry.snapshot),
                entry.params.clone(),
                entry.refresh_count,
                entry.first_scheduled_at,
                Arc::clone(&entry.principal_name),
            )
        };

        if refresh_count >= params.max_refreshes {
            self.cancel(&key, CancelReason::MaxRefreshes);
            return;
        }
        if first_scheduled_at.elapsed().as_secs() >= params.max_total_duration_secs {
            self.cancel(&key, CancelReason::MaxDuration);
            return;
        }

        let outcome = self.dispatcher.dispatch(&snapshot).await;
        let ttl_label = ttl_label(snapshot.ttl);
        // Post-dispatch generation re-check. If a new real request replaced
        // this session's entry while dispatch was in flight, the new
        // generation owns the state; drop this stale fire's rescheduling
        // result silently.
        if !self.generation_still_current(&key, expected_generation) {
            return;
        }
        match outcome {
            DispatchOutcome::CacheHit => {
                metrics::record_fired(&principal_name, ttl_label, "hit");
                self.increment_refresh_count(&key, expected_generation);
                self.install_entry(
                    key,
                    snapshot,
                    params,
                    principal_name,
                    /* preserve_counters */ true,
                );
            }
            DispatchOutcome::CacheMiss => {
                metrics::record_fired(&principal_name, ttl_label, "miss");
                self.cancel(&key, CancelReason::UpstreamGone);
            }
            DispatchOutcome::Error(err) => {
                warn!(target: "cache_keepalive", session = %key, error = %err, "keep-alive dispatch failed");
                metrics::record_fired(&principal_name, ttl_label, "error");
                self.cancel(&key, CancelReason::UpstreamGone);
            }
        }
    }

    fn generation_still_current(&self, key: &SessionKey, expected_generation: u64) -> bool {
        self.sessions
            .get(key)
            .is_some_and(|entry| entry.generation == expected_generation)
    }

    fn increment_refresh_count(&self, key: &SessionKey, expected_generation: u64) {
        if let Some(mut entry) = self.sessions.get_mut(key)
            && entry.generation == expected_generation
        {
            entry.refresh_count = entry.refresh_count.saturating_add(1);
        }
    }
}

fn ttl_label(ttl: CacheTtl) -> &'static str {
    match ttl {
        CacheTtl::Ttl5m => "5m",
        CacheTtl::Ttl1h => "1h",
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use bytes::Bytes;
    use http::{HeaderMap, Method};
    use uuid::Uuid;

    use super::*;

    fn snapshot() -> Arc<RequestSnapshot> {
        Arc::new(RequestSnapshot {
            url: "https://api.anthropic.com/v1/messages".parse().unwrap(),
            method: Method::POST,
            headers: HeaderMap::new(),
            body: Bytes::from_static(b"{\"model\":\"m\",\"max_tokens\":1}"),
            upstream_id: Uuid::nil(),
            ttl: CacheTtl::Ttl5m,
        })
    }

    struct CountingDispatcher {
        fires: AtomicUsize,
        outcomes: StdMutex<Vec<DispatchOutcome>>,
    }

    impl CountingDispatcher {
        fn new() -> Self {
            Self {
                fires: AtomicUsize::new(0),
                outcomes: StdMutex::new(Vec::new()),
            }
        }

        fn with_outcomes(outcomes: Vec<DispatchOutcome>) -> Self {
            Self {
                fires: AtomicUsize::new(0),
                outcomes: StdMutex::new(outcomes),
            }
        }

        fn count(&self) -> usize {
            self.fires.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl KeepaliveDispatcher for CountingDispatcher {
        async fn dispatch(&self, _: &RequestSnapshot) -> DispatchOutcome {
            let outcome = {
                let mut guard = self.outcomes.lock().unwrap();
                if guard.is_empty() {
                    DispatchOutcome::CacheMiss
                } else {
                    guard.remove(0)
                }
            };
            self.fires.fetch_add(1, Ordering::SeqCst);
            outcome
        }
    }

    fn params(delay_secs: u64) -> ScheduleParams {
        ScheduleParams {
            delay: Duration::from_secs(delay_secs),
            max_refreshes: 12,
            max_total_duration_secs: 14400,
        }
    }

    fn key() -> SessionKey {
        SessionKey::from_thread_id(Uuid::nil(), "session-1")
    }

    async fn settle() {
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn scheduled_job_fires_after_delay() {
        let dispatcher = Arc::new(CountingDispatcher::new());
        let scheduler = KeepaliveScheduler::new(dispatcher.clone());
        scheduler.schedule_or_replace(key(), snapshot(), params(60), Arc::from("test-principal"));
        settle().await;
        assert_eq!(dispatcher.count(), 0);
        tokio::time::advance(Duration::from_secs(59)).await;
        settle().await;
        assert_eq!(dispatcher.count(), 0);
        tokio::time::advance(Duration::from_secs(2)).await;
        settle().await;
        assert_eq!(dispatcher.count(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn replaced_job_does_not_fire_original() {
        let dispatcher = Arc::new(CountingDispatcher::new());
        let scheduler = KeepaliveScheduler::new(dispatcher.clone());
        scheduler.schedule_or_replace(key(), snapshot(), params(60), Arc::from("test-principal"));
        settle().await;
        tokio::time::advance(Duration::from_secs(30)).await;
        settle().await;
        scheduler.schedule_or_replace(key(), snapshot(), params(120), Arc::from("test-principal"));
        settle().await;
        tokio::time::advance(Duration::from_secs(35)).await;
        settle().await;
        assert_eq!(dispatcher.count(), 0);
        tokio::time::advance(Duration::from_secs(90)).await;
        settle().await;
        assert_eq!(dispatcher.count(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn cache_hit_reschedules_up_to_max_refreshes() {
        let dispatcher = Arc::new(CountingDispatcher::with_outcomes(vec![
            DispatchOutcome::CacheHit,
            DispatchOutcome::CacheHit,
            DispatchOutcome::CacheHit,
        ]));
        let scheduler = KeepaliveScheduler::new(dispatcher.clone());
        let mut p = params(1);
        p.max_refreshes = 2;
        scheduler.schedule_or_replace(key(), snapshot(), p, Arc::from("principal"));

        for _ in 0..6 {
            tokio::time::advance(Duration::from_secs(1)).await;
            settle().await;
        }

        assert_eq!(dispatcher.count(), 2);
        assert_eq!(scheduler.active_session_count(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_cancels_all_pending() {
        let dispatcher = Arc::new(CountingDispatcher::new());
        let scheduler = KeepaliveScheduler::new(dispatcher.clone());
        scheduler.schedule_or_replace(key(), snapshot(), params(60), Arc::from("test-principal"));
        settle().await;
        assert_eq!(scheduler.active_session_count(), 1);
        scheduler.shutdown().await;
        assert_eq!(scheduler.active_session_count(), 0);
        tokio::time::advance(Duration::from_secs(120)).await;
        settle().await;
        assert_eq!(dispatcher.count(), 0);
    }
}
