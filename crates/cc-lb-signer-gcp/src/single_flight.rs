use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::Mutex;

/// Shared per-scope-set refresh lock map.
pub type GcpSingleFlightLocks = Arc<DashMap<Vec<String>, Arc<Mutex<()>>>>;

/// Creates an empty GCP refresh single-flight map.
pub fn new_single_flight_locks() -> GcpSingleFlightLocks {
    Arc::new(DashMap::new())
}

/// Canonicalizes scopes for map keys and provider calls.
pub fn scope_key(scopes: &[String]) -> Vec<String> {
    let mut key = scopes.to_vec();
    key.sort();
    key.dedup();
    key
}

/// Returns the single-flight mutex for the requested scope set.
pub fn lock_for(locks: &GcpSingleFlightLocks, scopes: &[String]) -> Arc<Mutex<()>> {
    locks
        .entry(scope_key(scopes))
        .or_insert_with(|| Arc::new(Mutex::new(())))
        .clone()
}
