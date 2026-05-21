use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::Mutex;

pub type RefreshLocks = Arc<DashMap<String, Arc<Mutex<()>>>>;

pub fn new_refresh_locks() -> RefreshLocks {
    Arc::new(DashMap::new())
}

pub fn single_flight_key(principal_id: &str, provider: &str) -> String {
    format!("{principal_id}\u{1f}{provider}")
}

pub fn lock_for(locks: &RefreshLocks, principal_id: &str, provider: &str) -> Arc<Mutex<()>> {
    locks
        .entry(single_flight_key(principal_id, provider))
        .or_insert_with(|| Arc::new(Mutex::new(())))
        .clone()
}
