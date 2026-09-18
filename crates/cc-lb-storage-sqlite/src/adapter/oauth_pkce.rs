use std::{collections::HashMap, sync::MutexGuard};

use async_trait::async_trait;
use cc_lb_storage_api::{OAuthPkceStore, StorageError, StorageResult, StoredOAuthPkceFlow};

use crate::SqliteStorage;

/// SQLite is a single-instance backend, so in-flight PKCE handshakes live in
/// process memory instead of a table. Clones of `SqliteStorage` share the same
/// map through the `Arc`.
#[async_trait]
impl OAuthPkceStore for SqliteStorage {
    async fn put_pkce_flow(&self, flow: &StoredOAuthPkceFlow) -> StorageResult<()> {
        let mut flows = lock_pkce_flows(self)?;
        // Treat the new flow's creation time as "now" and drop expired entries.
        flows.retain(|_, f| f.expires_at_unix_secs > flow.created_at_unix_secs);
        flows.insert(flow.state_token.clone(), flow.clone());
        Ok(())
    }

    async fn get_pkce_flow(
        &self,
        state_token: &str,
        now_unix_secs: u64,
    ) -> StorageResult<Option<StoredOAuthPkceFlow>> {
        let mut flows = lock_pkce_flows(self)?;
        match flows.get(state_token) {
            Some(flow) if flow.expires_at_unix_secs > now_unix_secs => Ok(Some(flow.clone())),
            Some(_) => {
                flows.remove(state_token);
                Ok(None)
            }
            None => Ok(None),
        }
    }

    async fn delete_pkce_flow(&self, state_token: &str) -> StorageResult<()> {
        lock_pkce_flows(self)?.remove(state_token);
        Ok(())
    }
}

fn lock_pkce_flows(
    storage: &SqliteStorage,
) -> StorageResult<MutexGuard<'_, HashMap<String, StoredOAuthPkceFlow>>> {
    storage
        .pkce_flows()
        .lock()
        .map_err(|_| StorageError::Unavailable {
            message: "oauth pkce flow map lock poisoned".to_owned(),
        })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use cc_lb_clock::SystemClock;

    use super::*;
    use crate::open_sqlite;

    async fn storage() -> SqliteStorage {
        open_sqlite("sqlite::memory:", Arc::new(SystemClock))
            .await
            .expect("open in-memory sqlite")
    }

    fn flow(state: &str, payload: &[u8], created: u64, expires: u64) -> StoredOAuthPkceFlow {
        StoredOAuthPkceFlow {
            state_token: state.to_owned(),
            encrypted_payload: payload.to_vec(),
            created_at_unix_secs: created,
            expires_at_unix_secs: expires,
        }
    }

    #[tokio::test]
    async fn put_then_get_returns_flow() {
        let storage = storage().await;
        let flow = flow("state-1", b"ciphertext", 100, 200);

        storage.put_pkce_flow(&flow).await.unwrap();

        assert_eq!(
            storage.get_pkce_flow("state-1", 150).await.unwrap(),
            Some(flow)
        );
    }

    #[tokio::test]
    async fn get_expired_returns_none_and_removes() {
        let storage = storage().await;
        storage
            .put_pkce_flow(&flow("state-1", b"ciphertext", 100, 200))
            .await
            .unwrap();

        assert_eq!(storage.get_pkce_flow("state-1", 200).await.unwrap(), None);
        // The expired entry was removed, not just hidden.
        assert!(lock_pkce_flows(&storage).unwrap().is_empty());
    }

    #[tokio::test]
    async fn delete_missing_is_ok() {
        let storage = storage().await;
        storage.delete_pkce_flow("missing").await.unwrap();
    }

    #[tokio::test]
    async fn put_replaces_existing_payload() {
        let storage = storage().await;
        storage
            .put_pkce_flow(&flow("state-1", b"old", 100, 200))
            .await
            .unwrap();
        storage
            .put_pkce_flow(&flow("state-1", b"new", 110, 300))
            .await
            .unwrap();

        assert_eq!(
            storage.get_pkce_flow("state-1", 150).await.unwrap(),
            Some(flow("state-1", b"new", 110, 300))
        );
    }

    #[tokio::test]
    async fn put_evicts_other_expired_flows() {
        let storage = storage().await;
        storage
            .put_pkce_flow(&flow("stale", b"old", 100, 150))
            .await
            .unwrap();
        storage
            .put_pkce_flow(&flow("fresh", b"new", 200, 300))
            .await
            .unwrap();

        assert!(lock_pkce_flows(&storage).unwrap().get("stale").is_none());
        assert_eq!(
            storage.get_pkce_flow("fresh", 250).await.unwrap(),
            Some(flow("fresh", b"new", 200, 300))
        );
    }
}
