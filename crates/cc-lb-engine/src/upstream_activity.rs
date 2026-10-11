//! Process-local admission evidence, separate from routing cache scores.
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use cc_lb_clock::ClockHandle;
use cc_lb_storage_api::PromptCacheObservationStore;
use uuid::Uuid;

type Key = (Uuid, String);
type Owner = (String, String);
const MAX_READINESS_KEYS: usize = 4096;
const READ_FAILURE_COOLDOWN_SECS: u64 = 60;

#[derive(Default)]
struct State {
    initialized: HashMap<Key, u64>,
    failed_initialization: HashMap<Key, u64>,
    historical: HashMap<(Uuid, Option<String>), u64>,
    attempts: HashMap<u64, Entry>,
    next_id: u64,
}

struct Entry {
    key: Key,
    owner: Option<Owner>,
    active: bool,
    exclusive: bool,
    dispatched: bool,
    observed: bool,
    deadline: u64,
    potential_ttl: u64,
}

/// One registry shared by proxy and keepalive for the lifetime of a server process.
pub struct UpstreamActivity {
    state: Mutex<State>,
    initialization: tokio::sync::Mutex<()>,
    clock: ClockHandle,
}

impl UpstreamActivity {
    pub fn new(clock: ClockHandle) -> Self {
        Self {
            state: Mutex::new(State::default()),
            initialization: tokio::sync::Mutex::new(()),
            clock,
        }
    }

    pub(crate) fn now(&self) -> u64 {
        cc_lb_clock::unix_secs(self.clock.now())
    }

    /// Initialize new account/model partitions once before tracking attempts.
    /// Failure leaves them unknown; it never authorizes optional failover.
    pub(crate) async fn initialize(
        &self,
        store: Option<&dyn PromptCacheObservationStore>,
        ids: &[Uuid],
        model: &str,
    ) {
        let _initialization = self.initialization.lock().await;
        let missing: Vec<_> = {
            let state = self.state.lock().unwrap();
            ids.iter()
                .copied()
                .filter(|id| {
                    !state.initialized.contains_key(&(*id, model.to_owned()))
                        && !state
                            .failed_initialization
                            .get(&(*id, model.to_owned()))
                            .is_some_and(|retry_after| *retry_after > self.now())
                })
                .collect()
        };
        if missing.is_empty() {
            return;
        }
        let Some(store) = store else {
            return;
        };
        let rows =
            match store
                .list_active_protection(&missing, model, self.now())
                .await
            {
                Ok(rows) => rows,
                Err(_) => {
                    self.state.lock().unwrap().failed_initialization.extend(
                        missing.into_iter().map(|id| {
                            (
                                (id, model.to_owned()),
                                self.now().saturating_add(READ_FAILURE_COOLDOWN_SECS),
                            )
                        }),
                    );
                    self.trim_readiness();
                    return;
                }
            };
        let mut state = self.state.lock().unwrap();
        for id in missing {
            state.failed_initialization.remove(&(id, model.to_owned()));
            state.initialized.insert((id, model.to_owned()), self.now());
        }
        for row in rows {
            let key = (row.upstream_id, row.canonical_model_id);
            state
                .historical
                .entry(key)
                .and_modify(|deadline| {
                    *deadline = (*deadline).max(row.protection_expires_at_unix_secs)
                })
                .or_insert(row.protection_expires_at_unix_secs);
        }
        drop(state);
        self.trim_readiness();
    }

    fn trim_readiness(&self) {
        let mut state = self.state.lock().unwrap();
        trim_readiness(&mut state, MAX_READINESS_KEYS);
    }

    fn permitted(
        state: &State,
        key: &Key,
        owner: &Option<Owner>,
        fallback: bool,
        now: u64,
    ) -> bool {
        if fallback
            && (!state.initialized.contains_key(key)
                || state
                    .historical
                    .get(&(key.0, Some(key.1.clone())))
                    .into_iter()
                    .chain(state.historical.get(&(key.0, None)))
                    .any(|deadline| *deadline > now))
        {
            return false;
        }
        state
            .attempts
            .values()
            .filter(|entry| &entry.key == key && (entry.active || entry.deadline > now))
            .all(|entry| {
                (!fallback && !entry.exclusive) || (owner.is_some() && entry.owner == *owner)
            })
    }

    pub(crate) fn eligible(
        &self,
        id: Uuid,
        model: &str,
        owner: &Option<Owner>,
        fallback: bool,
    ) -> bool {
        Self::permitted(
            &self.state.lock().unwrap(),
            &(id, model.to_owned()),
            owner,
            fallback,
            self.now(),
        )
    }

    pub(crate) fn claim(
        self: &Arc<Self>,
        id: Uuid,
        model: &str,
        owner: Option<Owner>,
        fallback: bool,
        potential_ttl: u64,
    ) -> Option<Arc<ActivityGuard>> {
        let now = self.now();
        let mut state = self.state.lock().unwrap();
        state
            .attempts
            .retain(|_, entry| entry.active || entry.deadline > now);
        state.historical.retain(|_, deadline| *deadline > now);
        let key = (id, model.to_owned());
        if !Self::permitted(&state, &key, &owner, fallback, now) {
            return None;
        }
        state.next_id += 1;
        let attempt_id = state.next_id;
        state.attempts.insert(
            attempt_id,
            Entry {
                key,
                owner,
                active: true,
                exclusive: fallback,
                dispatched: false,
                observed: false,
                deadline: 0,
                potential_ttl,
            },
        );
        Some(Arc::new(ActivityGuard {
            registry: Arc::clone(self),
            attempt_id,
        }))
    }
}

fn trim_readiness(state: &mut State, max_keys: usize) {
    while state.initialized.len() + state.failed_initialization.len() > max_keys {
        let oldest_ready = state
            .initialized
            .iter()
            .min_by_key(|(_, time)| *time)
            .map(|(key, time)| (key.clone(), *time));
        let oldest_failed = state
            .failed_initialization
            .iter()
            .min_by_key(|(_, time)| *time)
            .map(|(key, time)| (key.clone(), *time));
        match (oldest_ready, oldest_failed) {
            (Some((_key, ready)), Some((failed_key, failed))) if failed < ready => {
                state.failed_initialization.remove(&failed_key);
            }
            (Some((key, _)), _) => {
                state.initialized.remove(&key);
            }
            (None, Some((key, _))) => {
                state.failed_initialization.remove(&key);
            }
            (None, None) => break,
        }
    }
}

/// Account-specific ownership travels with the attempt, never its caller reservation.
pub(crate) struct ActivityGuard {
    registry: Arc<UpstreamActivity>,
    attempt_id: u64,
}

impl ActivityGuard {
    pub(crate) fn dispatched(&self) {
        if let Some(entry) = self
            .registry
            .state
            .lock()
            .unwrap()
            .attempts
            .get_mut(&self.attempt_id)
        {
            entry.dispatched = true;
            entry.deadline = entry
                .deadline
                .max(self.registry.now().saturating_add(entry.potential_ttl));
        }
    }

    pub(crate) fn prepare(&self, potential_ttl: u64) {
        if let Some(entry) = self
            .registry
            .state
            .lock()
            .unwrap()
            .attempts
            .get_mut(&self.attempt_id)
        {
            entry.potential_ttl = potential_ttl;
        }
    }

    pub(crate) fn observe(&self, deadline: u64) {
        if let Some(entry) = self
            .registry
            .state
            .lock()
            .unwrap()
            .attempts
            .get_mut(&self.attempt_id)
        {
            // Never shorten the actual-dispatch deadline with an earlier decode anchor.
            entry.deadline = entry.deadline.max(deadline);
            entry.observed = true;
        }
    }

    pub(crate) fn reject(&self) {
        self.registry
            .state
            .lock()
            .unwrap()
            .attempts
            .remove(&self.attempt_id);
    }
}

impl Drop for ActivityGuard {
    fn drop(&mut self) {
        let mut state = self.registry.state.lock().unwrap();
        if let Some(entry) = state.attempts.get_mut(&self.attempt_id) {
            if entry.dispatched {
                if !entry.observed {
                    entry.deadline = entry
                        .deadline
                        .max(self.registry.now().saturating_add(entry.potential_ttl));
                }
                entry.active = false;
            } else {
                state.attempts.remove(&self.attempt_id);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cc_lb_clock::TestClock;

    fn setup() -> (Arc<UpstreamActivity>, Uuid) {
        let activity = Arc::new(UpstreamActivity::new(Arc::new(TestClock::new_at_secs(
            1_000,
        ))));
        let id = Uuid::from_u128(1);
        activity
            .state
            .lock()
            .unwrap()
            .initialized
            .insert((id, "model".to_owned()), 1000);
        (activity, id)
    }
    fn owner(session: &str) -> Option<Owner> {
        Some(("principal".to_owned(), session.to_owned()))
    }

    #[test]
    fn fallback_claim_is_atomic_and_blocks_unrelated_primary() {
        let (registry, id) = setup();
        let guard = registry.claim(id, "model", owner("a"), true, 3600).unwrap();
        assert!(
            registry
                .claim(id, "model", owner("b"), true, 3600)
                .is_none()
        );
        assert!(
            registry
                .claim(id, "model", owner("b"), false, 3600)
                .is_none()
        );
        assert!(
            registry
                .claim(id, "model", owner("a"), false, 3600)
                .is_some()
        );
        drop(guard);
        assert!(
            registry
                .claim(id, "model", owner("b"), true, 3600)
                .is_some()
        );
    }

    #[test]
    fn primary_sharing_survives_but_vetoes_fallback_in_other_arrival_order() {
        let (registry, id) = setup();
        let _a = registry.claim(id, "model", owner("a"), false, 300).unwrap();
        let _b = registry.claim(id, "model", owner("b"), false, 300).unwrap();
        assert!(registry.claim(id, "model", owner("a"), true, 300).is_none());
        assert!(registry.claim(id, "model", None, true, 300).is_none());
    }

    #[test]
    fn cancellation_and_dropped_writes_retain_protection_while_rejection_is_attempt_scoped() {
        let (registry, id) = setup();
        let a = registry
            .claim(id, "model", owner("a"), false, 3600)
            .unwrap();
        a.dispatched();
        a.observe(4_600);
        drop(a);
        let b = registry.claim(id, "model", owner("b"), false, 300).unwrap();
        b.dispatched();
        b.reject();
        drop(b);
        assert!(registry.claim(id, "model", owner("b"), true, 300).is_none());
        assert!(registry.claim(id, "model", owner("a"), true, 300).is_some());
    }

    #[test]
    fn historical_unknown_is_separate_from_local_owner_and_missing_keys_fail_closed() {
        let (registry, id) = setup();
        registry
            .state
            .lock()
            .unwrap()
            .historical
            .insert((id, Some("model".to_owned())), 4_600);
        let local = registry.claim(id, "model", owner("a"), false, 300).unwrap();
        local.dispatched();
        local.observe(1_300);
        drop(local);
        assert!(registry.claim(id, "model", owner("a"), true, 300).is_none());
        assert!(
            registry
                .claim(Uuid::from_u128(2), "model", owner("a"), true, 300)
                .is_none()
        );
    }

    #[test]
    fn encrypted_model_history_is_one_account_wide_entry() {
        let (registry, id) = setup();
        {
            let mut state = registry.state.lock().unwrap();
            state.historical.insert((id, None), 4600);
            for model in ["model", "future-a", "future-b"] {
                state.initialized.insert((id, model.to_owned()), 1000);
            }
        }
        for model in ["model", "future-a", "future-b"] {
            assert!(!registry.eligible(id, model, &owner("a"), true));
            assert!(registry.eligible(id, model, &owner("a"), false));
        }
        assert_eq!(registry.state.lock().unwrap().historical.len(), 1);
    }

    #[test]
    fn delayed_signing_and_old_decode_anchor_never_shorten_one_hour_protection() {
        let clock = Arc::new(TestClock::new_at_secs(1000));
        let registry = Arc::new(UpstreamActivity::new(clock.clone()));
        let id = Uuid::from_u128(1);
        registry
            .state
            .lock()
            .unwrap()
            .initialized
            .insert((id, "model".to_owned()), 1000);
        let guard = registry
            .claim(id, "model", owner("a"), false, 3600)
            .unwrap();
        clock.advance_secs(100); // deterministic signing delay, no sleep.
        guard.dispatched();
        guard.observe(4600);
        drop(guard);
        clock.set_unix_secs(4650);
        assert!(
            registry
                .claim(id, "model", owner("b"), true, 3600)
                .is_none()
        );
        clock.set_unix_secs(4700);
        assert!(
            registry
                .claim(id, "model", owner("b"), true, 3600)
                .is_some()
        );
    }

    #[test]
    fn competing_unrelated_fallbacks_have_one_atomic_winner() {
        let (registry, id) = setup();
        let barrier = Arc::new(std::sync::Barrier::new(3));
        let handles: Vec<_> = ["a", "b"]
            .into_iter()
            .map(|session| {
                let registry = registry.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    let claim = registry.claim(id, "model", owner(session), true, 300);
                    barrier.wait(); // retain ownership until both admission decisions completed.
                    claim.is_some()
                })
            })
            .collect();
        barrier.wait();
        barrier.wait();
        assert_eq!(
            handles
                .into_iter()
                .map(|handle| usize::from(handle.join().unwrap()))
                .sum::<usize>(),
            1
        );
    }

    #[tokio::test]
    async fn empty_import_initializes_once_and_new_keys_and_failed_reads_remain_unknown() {
        use async_trait::async_trait;
        use cc_lb_storage_api::{PromptCacheProtection, StorageError, StorageResult};
        struct Store {
            calls: std::sync::atomic::AtomicUsize,
            unavailable: bool,
        }
        #[async_trait]
        impl PromptCacheObservationStore for Store {
            async fn list_active_protection(
                &self,
                _ids: &[Uuid],
                _model: &str,
                _now: u64,
            ) -> StorageResult<Vec<PromptCacheProtection>> {
                self.calls
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if self.unavailable {
                    Err(StorageError::Unavailable {
                        message: "unavailable".to_owned(),
                    })
                } else {
                    Ok(Vec::new())
                }
            }
        }
        let clock = Arc::new(TestClock::new_at_secs(1000));
        let registry = Arc::new(UpstreamActivity::new(clock.clone()));
        let id = Uuid::from_u128(1);
        let new = Uuid::from_u128(2);
        let store = Store {
            calls: std::sync::atomic::AtomicUsize::new(0),
            unavailable: false,
        };
        registry.initialize(Some(&store), &[id], "model").await;
        registry.initialize(Some(&store), &[id], "model").await;
        assert_eq!(store.calls.load(std::sync::atomic::Ordering::Relaxed), 1);
        assert!(registry.claim(id, "model", owner("a"), true, 300).is_some());
        assert!(
            registry
                .claim(new, "model", owner("a"), true, 300)
                .is_none()
        );
        let failed = Store {
            calls: std::sync::atomic::AtomicUsize::new(0),
            unavailable: true,
        };
        registry.initialize(Some(&failed), &[new], "model").await;
        registry.initialize(Some(&store), &[new], "model").await;
        assert!(
            registry
                .claim(new, "model", owner("a"), true, 300)
                .is_none()
        );
        assert!(
            registry
                .claim(new, "model", owner("a"), false, 300)
                .is_some()
        );
        clock.advance_secs(READ_FAILURE_COOLDOWN_SECS);
        registry.initialize(Some(&store), &[new], "model").await;
        assert!(
            registry
                .claim(new, "model", owner("a"), true, 300)
                .is_some()
        );
    }
    #[test]
    fn readiness_eviction_never_erases_live_unknown_or_exclusive_protection() {
        let (registry, id) = setup();
        let guard = registry.claim(id, "model", owner("a"), true, 3600).unwrap();
        {
            let mut state = registry.state.lock().unwrap();
            state
                .historical
                .insert((id, Some("model".to_owned())), 4600);
            state
                .initialized
                .insert((Uuid::from_u128(2), "future-model".to_owned()), 1001);
            trim_readiness(&mut state, 1);
            assert_eq!(
                state.initialized.len() + state.failed_initialization.len(),
                1
            );
            assert!(
                state
                    .historical
                    .contains_key(&(id, Some("model".to_owned())))
            );
        }
        assert!(!registry.eligible(id, "model", &owner("a"), true));
        assert!(!registry.eligible(id, "model", &owner("b"), false));
        drop(guard);
        assert!(!registry.eligible(id, "model", &owner("a"), true));
    }
}
