use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use cc_lb_storage_api::{
    ApiKeyMutation, IssueParams, KeyStatus, Limit, ManagedKeyStore, NotifierConnectionState,
    StorageError, StoredApiKeyRecord,
};
use dashmap::DashMap;
use thiserror::Error;

use super::secret::{self, NewKeyOutput, RedactedSecret};

pub type Result<T> = std::result::Result<T, KeyStoreError>;

const MAX_POSITIVE_CACHE_ENTRIES: usize = 4_096;
const DEFAULT_POSITIVE_CACHE_TTL: Duration = Duration::from_secs(30);

#[derive(Clone)]
pub struct KeyStore {
    storage: Arc<dyn ManagedKeyStore>,
    positive_cache: Option<Arc<PositiveKeyCache>>,
}

struct PositiveKeyCache {
    entries: DashMap<[u8; 32], CachedKeyEntry>,
    connection_state: Arc<NotifierConnectionState>,
    publish_lock: parking_lot::Mutex<()>,
    invalidation_epoch: AtomicU64,
    insert_seq: AtomicU64,
    ttl: Duration,
    max_entries: usize,
}

#[derive(Clone)]
struct CachedKeyEntry {
    principal_id: String,
    key_id: String,
    record: StoredApiKeyRecord,
    inserted_at: Instant,
    connection_epoch: u64,
    seq: u64,
}

impl PositiveKeyCache {
    fn new(
        connection_state: Arc<NotifierConnectionState>,
        ttl: Duration,
        max_entries: usize,
    ) -> Self {
        Self {
            entries: DashMap::new(),
            connection_state,
            publish_lock: parking_lot::Mutex::new(()),
            invalidation_epoch: AtomicU64::new(0),
            insert_seq: AtomicU64::new(0),
            ttl,
            max_entries,
        }
    }

    fn insert(
        &self,
        index_hash: [u8; 32],
        principal_id: String,
        key_id: String,
        record: StoredApiKeyRecord,
        connection_epoch: u64,
    ) {
        if self.max_entries == 0 {
            return;
        }
        if !self.entries.contains_key(&index_hash) && self.entries.len() >= self.max_entries {
            self.entries.retain(|_, entry| {
                entry.connection_epoch == connection_epoch && entry.inserted_at.elapsed() < self.ttl
            });
            while self.entries.len() >= self.max_entries {
                let oldest = self
                    .entries
                    .iter()
                    .min_by_key(|entry| entry.value().seq)
                    .map(|entry| *entry.key());
                if let Some(key) = oldest {
                    self.entries.remove(&key);
                } else {
                    break;
                }
            }
        }
        let seq = self.insert_seq.fetch_add(1, Ordering::Relaxed) + 1;
        self.entries.insert(
            index_hash,
            CachedKeyEntry {
                principal_id,
                key_id,
                record,
                inserted_at: Instant::now(),
                connection_epoch,
                seq,
            },
        );
    }

    fn invalidate_key(&self, principal_id: &str, key_id: &str) {
        let _guard = self.publish_lock.lock();
        self.invalidation_epoch.fetch_add(1, Ordering::AcqRel);
        self.entries
            .retain(|_, entry| entry.principal_id != principal_id || entry.key_id != key_id);
    }

    fn invalidate_principal(&self, principal_id: &str) {
        let _guard = self.publish_lock.lock();
        self.invalidation_epoch.fetch_add(1, Ordering::AcqRel);
        self.entries
            .retain(|_, entry| entry.principal_id != principal_id);
    }

    fn invalidate_all(&self) {
        let _guard = self.publish_lock.lock();
        self.invalidation_epoch.fetch_add(1, Ordering::AcqRel);
        self.entries.clear();
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateParams {
    pub label: String,
    pub description: Option<String>,
    pub expires_at_unix_secs: Option<u64>,
    pub limit_overrides: Vec<Limit>,
}

#[derive(Debug, Error)]
pub enum KeyStoreError {
    #[error(transparent)]
    Storage(#[from] StorageError),
}

impl KeyStore {
    pub fn new(storage: Arc<dyn ManagedKeyStore>) -> Self {
        Self {
            storage,
            positive_cache: None,
        }
    }

    #[must_use]
    pub fn with_positive_cache(mut self, connection_state: Arc<NotifierConnectionState>) -> Self {
        self.positive_cache = Some(Arc::new(PositiveKeyCache::new(
            connection_state,
            DEFAULT_POSITIVE_CACHE_TTL,
            MAX_POSITIVE_CACHE_ENTRIES,
        )));
        self
    }

    #[cfg(test)]
    #[must_use]
    pub fn with_positive_cache_config(
        mut self,
        connection_state: Arc<NotifierConnectionState>,
        ttl: Duration,
        max_entries: usize,
    ) -> Self {
        self.positive_cache = Some(Arc::new(PositiveKeyCache::new(
            connection_state,
            ttl,
            max_entries,
        )));
        self
    }

    pub fn invalidate_key(&self, principal_id: &str, key_id: &str) {
        if let Some(cache) = &self.positive_cache {
            cache.invalidate_key(principal_id, key_id);
        }
    }

    pub fn invalidate_principal(&self, principal_id: &str) {
        if let Some(cache) = &self.positive_cache {
            cache.invalidate_principal(principal_id);
        }
    }

    pub fn invalidate_all(&self) {
        if let Some(cache) = &self.positive_cache {
            cache.invalidate_all();
        }
    }

    pub async fn create(
        &self,
        principal_id: &str,
        params: CreateParams,
    ) -> Result<(StoredApiKeyRecord, RedactedSecret)> {
        let NewKeyOutput {
            plaintext,
            key_id,
            secret_salt,
            index_hash,
            verify_hash,
            last_4,
        } = secret::generate_new();

        let issue_params = IssueParams {
            label: params.label,
            description: params.description,
            expires_at_unix_secs: params.expires_at_unix_secs,
            limit_overrides: params.limit_overrides,
            secret_salt,
            verify_hash,
            last_4,
            index_hash,
        };

        let record = self
            .storage
            .issue(principal_id, &key_id, issue_params)
            .await?;

        Ok((record, plaintext))
    }

    pub async fn lookup_by_index_hash(
        &self,
        index_hash: &[u8; 32],
    ) -> Result<Option<(String, String, StoredApiKeyRecord)>> {
        let Some(cache) = &self.positive_cache else {
            return Ok(self.storage.lookup_by_index_hash(index_hash).await?);
        };

        let Some(conn_epoch) = cache.connection_state.current_epoch() else {
            if !cache.entries.is_empty() {
                cache.entries.clear();
            }
            return Ok(self.storage.lookup_by_index_hash(index_hash).await?);
        };

        if let Some(cached) = cache.entries.get(index_hash) {
            if cached.connection_epoch == conn_epoch && cached.inserted_at.elapsed() < cache.ttl {
                return Ok(Some((
                    cached.principal_id.clone(),
                    cached.key_id.clone(),
                    cached.record.clone(),
                )));
            }
            drop(cached);
            cache.entries.remove(index_hash);
        }

        let inv_before = cache.invalidation_epoch.load(Ordering::Acquire);
        let fetched = self.storage.lookup_by_index_hash(index_hash).await?;
        if let Some((ref principal_id, ref key_id, ref record)) = fetched
            && record.status == KeyStatus::Active
        {
            let _publish_guard = cache.publish_lock.lock();
            if cache.connection_state.current_epoch() == Some(conn_epoch)
                && cache.invalidation_epoch.load(Ordering::Acquire) == inv_before
            {
                cache.insert(
                    *index_hash,
                    principal_id.clone(),
                    key_id.clone(),
                    record.clone(),
                    conn_epoch,
                );
            }
        }
        Ok(fetched)
    }

    pub async fn get(
        &self,
        principal_id: &str,
        key_id: &str,
    ) -> Result<Option<StoredApiKeyRecord>> {
        Ok(self.storage.get(principal_id, key_id).await?)
    }

    pub async fn list_by_principal(&self, principal_id: &str) -> Result<Vec<StoredApiKeyRecord>> {
        Ok(self.storage.list_by_principal(principal_id).await?)
    }

    pub async fn list_all(&self) -> Result<Vec<(String, String, StoredApiKeyRecord)>> {
        Ok(self.storage.list_all().await?)
    }

    pub async fn revoke(&self, principal_id: &str, key_id: &str) -> Result<()> {
        self.storage
            .revoke_zero_secrets(principal_id, key_id)
            .await?;
        self.invalidate_key(principal_id, key_id);
        Ok(())
    }

    pub async fn patch(
        &self,
        principal_id: &str,
        key_id: &str,
        mutation: ApiKeyMutation,
    ) -> Result<()> {
        self.storage.update(principal_id, key_id, mutation).await?;
        self.invalidate_key(principal_id, key_id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;

    use super::*;

    struct CountingManagedKeyStore {
        rows: Mutex<HashMap<[u8; 32], (String, String, StoredApiKeyRecord)>>,
        lookups: AtomicUsize,
        lookup_gate: Mutex<
            Option<(
                tokio::sync::oneshot::Sender<()>,
                tokio::sync::oneshot::Receiver<()>,
            )>,
        >,
    }

    impl CountingManagedKeyStore {
        fn new() -> Self {
            Self {
                rows: Mutex::new(HashMap::new()),
                lookups: AtomicUsize::new(0),
                lookup_gate: Mutex::new(None),
            }
        }

        fn insert_active(&self, index_hash: [u8; 32], principal_id: &str, key_id: &str) {
            let record = StoredApiKeyRecord {
                label: key_id.to_owned(),
                issued_at_unix_secs: 1_700_000_000,
                revoked_at_unix_secs: None,
                key_hash_b64: "hash".to_owned(),
                verify_hash: [1; 32],
                secret_salt: [2; 16],
                limit_overrides: Vec::new(),
                status: KeyStatus::Active,
                expires_at_unix_secs: None,
                last_4: "1234".to_owned(),
                description: None,
                index_hash,
            };
            self.rows.lock().expect("rows lock").insert(
                index_hash,
                (principal_id.to_owned(), key_id.to_owned(), record),
            );
        }

        fn lookups(&self) -> usize {
            self.lookups.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl ManagedKeyStore for CountingManagedKeyStore {
        async fn issue(
            &self,
            _principal_id: &str,
            _key_id: &str,
            _params: IssueParams,
        ) -> cc_lb_storage_api::StorageResult<StoredApiKeyRecord> {
            unimplemented!()
        }

        async fn get(
            &self,
            _principal_id: &str,
            _key_id: &str,
        ) -> cc_lb_storage_api::StorageResult<Option<StoredApiKeyRecord>> {
            unimplemented!()
        }

        async fn lookup_by_index_hash(
            &self,
            index_hash: &[u8; 32],
        ) -> cc_lb_storage_api::StorageResult<Option<(String, String, StoredApiKeyRecord)>>
        {
            self.lookups.fetch_add(1, Ordering::SeqCst);
            let gate = self.lookup_gate.lock().expect("gate lock").take();
            if let Some((entered, release)) = gate {
                let _ = entered.send(());
                let _ = release.await;
            }
            Ok(self
                .rows
                .lock()
                .expect("rows lock")
                .get(index_hash)
                .cloned())
        }

        async fn list_by_principal(
            &self,
            _principal_id: &str,
        ) -> cc_lb_storage_api::StorageResult<Vec<StoredApiKeyRecord>> {
            unimplemented!()
        }

        async fn list_all(
            &self,
        ) -> cc_lb_storage_api::StorageResult<Vec<(String, String, StoredApiKeyRecord)>> {
            unimplemented!()
        }

        async fn update(
            &self,
            _principal_id: &str,
            _key_id: &str,
            _mutation: ApiKeyMutation,
        ) -> cc_lb_storage_api::StorageResult<()> {
            Ok(())
        }

        async fn revoke_zero_secrets(
            &self,
            principal_id: &str,
            key_id: &str,
        ) -> cc_lb_storage_api::StorageResult<()> {
            self.rows
                .lock()
                .expect("rows lock")
                .retain(|_, (pid, kid, _)| pid != principal_id || kid != key_id);
            Ok(())
        }
    }

    #[tokio::test]
    async fn positive_cache_hits_on_active_keys_and_never_caches_misses() {
        let backend = Arc::new(CountingManagedKeyStore::new());
        let hash = [7u8; 32];
        let missing = [9u8; 32];
        backend.insert_active(hash, "p1", "k1");

        let conn = Arc::new(NotifierConnectionState::new_always_connected());
        let store = KeyStore::new(backend.clone()).with_positive_cache(conn);

        assert!(store.lookup_by_index_hash(&hash).await.unwrap().is_some());
        assert!(store.lookup_by_index_hash(&hash).await.unwrap().is_some());
        assert_eq!(
            backend.lookups(),
            1,
            "second lookup should hit positive cache"
        );

        assert!(
            store
                .lookup_by_index_hash(&missing)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .lookup_by_index_hash(&missing)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            backend.lookups(),
            3,
            "negative lookups must never be cached"
        );
    }

    #[tokio::test]
    async fn disconnected_notifier_clears_and_bypasses_positive_cache() {
        let backend = Arc::new(CountingManagedKeyStore::new());
        let hash = [11u8; 32];
        backend.insert_active(hash, "p1", "k1");

        let conn = Arc::new(NotifierConnectionState::new_always_connected());
        let store = KeyStore::new(backend.clone()).with_positive_cache(Arc::clone(&conn));

        assert!(store.lookup_by_index_hash(&hash).await.unwrap().is_some());
        assert_eq!(backend.lookups(), 1);

        conn.mark_disconnected();
        assert!(store.lookup_by_index_hash(&hash).await.unwrap().is_some());
        assert_eq!(
            backend.lookups(),
            2,
            "disconnected listener must bypass cache"
        );

        conn.mark_connected();
        assert!(store.lookup_by_index_hash(&hash).await.unwrap().is_some());
        assert!(store.lookup_by_index_hash(&hash).await.unwrap().is_some());
        assert_eq!(
            backend.lookups(),
            3,
            "reconnected listener caches within new epoch"
        );
    }

    #[tokio::test]
    async fn concurrent_invalidation_prevents_resurrecting_revoked_key() {
        let backend = Arc::new(CountingManagedKeyStore::new());
        let hash = [21u8; 32];
        backend.insert_active(hash, "p1", "k1");
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        *backend.lookup_gate.lock().expect("gate lock") = Some((entered_tx, release_rx));

        let conn = Arc::new(NotifierConnectionState::new_always_connected());
        let store = Arc::new(KeyStore::new(backend.clone()).with_positive_cache(conn));

        let reader_store = Arc::clone(&store);
        let reader = tokio::spawn(async move { reader_store.lookup_by_index_hash(&hash).await });
        entered_rx.await.expect("lookup entered storage");

        store.revoke("p1", "k1").await.unwrap();
        release_tx.send(()).expect("release lookup");

        let _ = reader.await.unwrap().unwrap();
        assert!(
            store.lookup_by_index_hash(&hash).await.unwrap().is_none(),
            "revoked key must not be resurrected in positive cache"
        );
    }

    #[tokio::test]
    async fn positive_cache_evicts_oldest_when_at_capacity() {
        let backend = Arc::new(CountingManagedKeyStore::new());
        backend.insert_active([1u8; 32], "p1", "k1");
        backend.insert_active([2u8; 32], "p1", "k2");
        backend.insert_active([3u8; 32], "p1", "k3");

        let conn = Arc::new(NotifierConnectionState::new_always_connected());
        let store = KeyStore::new(backend.clone()).with_positive_cache_config(
            conn,
            Duration::from_secs(30),
            2,
        );

        assert!(
            store
                .lookup_by_index_hash(&[1u8; 32])
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            store
                .lookup_by_index_hash(&[2u8; 32])
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            store
                .lookup_by_index_hash(&[3u8; 32])
                .await
                .unwrap()
                .is_some()
        );
        assert_eq!(backend.lookups(), 3);

        // k2 and k3 remain cached; k1 was evicted.
        assert!(
            store
                .lookup_by_index_hash(&[2u8; 32])
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            store
                .lookup_by_index_hash(&[3u8; 32])
                .await
                .unwrap()
                .is_some()
        );
        assert_eq!(backend.lookups(), 3);
        assert!(
            store
                .lookup_by_index_hash(&[1u8; 32])
                .await
                .unwrap()
                .is_some()
        );
        assert_eq!(backend.lookups(), 4);
    }

    #[tokio::test]
    async fn positive_cache_does_not_publish_stale_record_across_invalidation() {
        let backend = Arc::new(CountingManagedKeyStore::new());
        let hash = [42u8; 32];
        backend.insert_active(hash, "p-race", "k-race");

        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        *backend.lookup_gate.lock().expect("gate lock") = Some((entered_tx, release_rx));

        let conn = Arc::new(NotifierConnectionState::new_always_connected());
        let store = Arc::new(KeyStore::new(backend.clone()).with_positive_cache(conn));

        let store_lookup = Arc::clone(&store);
        let lookup_task =
            tokio::spawn(async move { store_lookup.lookup_by_index_hash(&hash).await });
        entered_rx.await.expect("lookup reached storage await");

        // Revoke key in backing storage and invalidate local cache while in-flight lookup is suspended.
        backend
            .revoke_zero_secrets("p-race", "k-race")
            .await
            .unwrap();
        store.invalidate_key("p-race", "k-race");
        let _ = release_tx.send(());

        let _ = lookup_task.await.expect("join").expect("lookup ok");
        assert!(
            store
                .lookup_by_index_hash(&hash)
                .await
                .expect("post-invalidation lookup ok")
                .is_none(),
            "revoked key must not be served from a stale in-flight positive cache fill"
        );
        assert_eq!(backend.lookups(), 2);
    }
}
