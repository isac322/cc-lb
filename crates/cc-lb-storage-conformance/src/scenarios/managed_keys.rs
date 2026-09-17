//! Managed API key store conformance scenarios.

use std::{collections::HashSet, future::Future, sync::Arc};

use anyhow::{Result, ensure};
use async_trait::async_trait;
use cc_lb_storage_api::{
    ManagedKeyStore, StorageError,
    types::{ApiKeyMutation, IssueParams, KeyStatus, Limit, LimitKind},
};
#[cfg(feature = "postgres")]
use cc_lb_storage_postgres::PostgresManagedKeyStore;
#[cfg(feature = "sqlite")]
use cc_lb_storage_sqlite::SqliteStorage;
use futures::future::{join_all, try_join_all};

/// Test-only seeding for keys issued before the issuance pause.
///
/// Production `issue` intentionally returns [`StorageError::Unavailable`];
/// scenarios that exercise existing-key behavior seed rows directly instead.
#[async_trait]
pub trait ManagedKeySeed: ManagedKeyStore {
    async fn seed_existing(
        &self,
        principal_id: &str,
        key_id: &str,
        params: &IssueParams,
        issued_at_unix_secs: u64,
    ) -> Result<()>;
}

#[cfg(feature = "sqlite")]
#[async_trait]
impl ManagedKeySeed for SqliteStorage {
    async fn seed_existing(
        &self,
        principal_id: &str,
        key_id: &str,
        params: &IssueParams,
        issued_at_unix_secs: u64,
    ) -> Result<()> {
        super::managed_key_seed::seed_sqlite(
            self.pool(),
            principal_id,
            key_id,
            params,
            i64::try_from(issued_at_unix_secs)?,
        )
        .await?;
        Ok(())
    }
}

/// Postgres managed-key store plus the pool needed for test-only seeding.
///
/// `PostgresManagedKeyStore` keeps its pool private, so the conformance suite
/// wraps it; every `ManagedKeyStore` call delegates to the real store.
#[cfg(feature = "postgres")]
pub struct SeededPostgresStore {
    inner: PostgresManagedKeyStore,
    pool: sqlx::PgPool,
}

#[cfg(feature = "postgres")]
impl SeededPostgresStore {
    pub fn new(inner: PostgresManagedKeyStore, pool: sqlx::PgPool) -> Self {
        Self { inner, pool }
    }
}

#[cfg(feature = "postgres")]
#[async_trait]
impl ManagedKeyStore for SeededPostgresStore {
    async fn issue(
        &self,
        principal_id: &str,
        key_id: &str,
        params: IssueParams,
    ) -> cc_lb_storage_api::StorageResult<cc_lb_storage_api::StoredApiKeyRecord> {
        self.inner.issue(principal_id, key_id, params).await
    }

    async fn get(
        &self,
        principal_id: &str,
        key_id: &str,
    ) -> cc_lb_storage_api::StorageResult<Option<cc_lb_storage_api::StoredApiKeyRecord>> {
        self.inner.get(principal_id, key_id).await
    }

    async fn lookup_by_index_hash(
        &self,
        index_hash: &[u8; 32],
    ) -> cc_lb_storage_api::StorageResult<
        Option<(String, String, cc_lb_storage_api::StoredApiKeyRecord)>,
    > {
        self.inner.lookup_by_index_hash(index_hash).await
    }

    async fn list_by_principal(
        &self,
        principal_id: &str,
    ) -> cc_lb_storage_api::StorageResult<Vec<cc_lb_storage_api::StoredApiKeyRecord>> {
        self.inner.list_by_principal(principal_id).await
    }

    async fn list_all(
        &self,
    ) -> cc_lb_storage_api::StorageResult<
        Vec<(String, String, cc_lb_storage_api::StoredApiKeyRecord)>,
    > {
        self.inner.list_all().await
    }

    async fn update(
        &self,
        principal_id: &str,
        key_id: &str,
        mutation: ApiKeyMutation,
    ) -> cc_lb_storage_api::StorageResult<()> {
        self.inner.update(principal_id, key_id, mutation).await
    }

    async fn revoke_zero_secrets(
        &self,
        principal_id: &str,
        key_id: &str,
    ) -> cc_lb_storage_api::StorageResult<()> {
        self.inner.revoke_zero_secrets(principal_id, key_id).await
    }
}

#[cfg(feature = "postgres")]
#[async_trait]
impl ManagedKeySeed for SeededPostgresStore {
    async fn seed_existing(
        &self,
        principal_id: &str,
        key_id: &str,
        params: &IssueParams,
        issued_at_unix_secs: u64,
    ) -> Result<()> {
        let legacy: bool = sqlx::query_scalar(
            "SELECT COUNT(*) = 2 \
             FROM information_schema.columns \
             WHERE table_schema = current_schema() \
               AND table_name = 'managed_api_keys_v1' \
               AND column_name IN ('upstream_kind', 'principal_kind')",
        )
        .fetch_one(&self.pool)
        .await?;

        let mut tx = self.pool.begin().await?;
        let mut query = sqlx::QueryBuilder::<sqlx::Postgres>::new(
            "INSERT INTO managed_api_keys_v1 (principal_id, key_id, label, \
             issued_at_unix_secs, revoked_at_unix_secs, key_hash_b64, verify_hash, \
             secret_salt, limit_overrides, status, expires_at_unix_secs, last_4, \
             description, index_hash, created_at, updated_at",
        );
        if legacy {
            query.push(", upstream_kind, principal_kind");
        }
        query.push(") VALUES (");
        {
            let mut values = query.separated(",");
            values
                .push_bind(principal_id)
                .push_bind(key_id)
                .push_bind(&params.label)
                .push_bind(i64::try_from(issued_at_unix_secs)?)
                .push_bind(Option::<i64>::None)
                .push_bind(base64_url_no_pad(&params.verify_hash))
                .push_bind(params.verify_hash.as_slice())
                .push_bind(params.secret_salt.as_slice())
                .push_bind(serde_json::to_value(&params.limit_overrides)?)
                .push_bind("active")
                .push_bind(params.expires_at_unix_secs.map(i64::try_from).transpose()?)
                .push_bind(&params.last_4)
                .push_bind(&params.description)
                .push_bind(params.index_hash.as_slice())
                .push("NOW()")
                .push("NOW()");
            if legacy {
                values.push_bind("anthropic_key").push_bind("machine");
            }
        }
        query.push(")").build().execute(&mut *tx).await?;
        sqlx::query(
            "INSERT INTO managed_api_key_index_v1 (index_hash, principal_id, key_id) \
             VALUES ($1, $2, $3)",
        )
        .bind(params.index_hash.as_slice())
        .bind(principal_id)
        .bind(key_id)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }
}

#[async_trait]
pub trait ManagedKeyBackend: Send + Sync + 'static {
    type Store: ManagedKeySeed;
    type Fixture: Send + Sync;

    async fn create_fixture(&self) -> Result<Self::Fixture>;
    async fn open(&self, fixture: &Self::Fixture) -> Result<Self::Store>;
    async fn teardown(&self, fixture: Self::Fixture) -> Result<()>;
}

struct ManagedKeyFixture<B: ManagedKeyBackend> {
    backend: Arc<B>,
    fixture: Option<B::Fixture>,
    store: Arc<B::Store>,
}

impl<B: ManagedKeyBackend> ManagedKeyFixture<B> {
    async fn new(backend: Arc<B>) -> Result<Self> {
        let fixture = backend.create_fixture().await?;
        let store = Arc::new(backend.open(&fixture).await?);

        Ok(Self {
            backend,
            fixture: Some(fixture),
            store,
        })
    }

    fn store(&self) -> Arc<B::Store> {
        Arc::clone(&self.store)
    }

    async fn teardown(&mut self) -> Result<()> {
        if let Some(fixture) = self.fixture.take() {
            self.backend.teardown(fixture).await?;
        }

        Ok(())
    }
}

pub async fn run_all<B>(backend: Arc<B>) -> Result<()>
where
    B: ManagedKeyBackend,
{
    managed_keys_happy_path(Arc::clone(&backend)).await?;
    managed_keys_nul_byte_rejected(Arc::clone(&backend)).await?;
    managed_keys_concurrent_issue_no_index_collision(Arc::clone(&backend)).await?;
    managed_keys_equivalent_records(backend).await?;

    Ok(())
}

pub async fn managed_keys_happy_path<B>(backend: Arc<B>) -> Result<()>
where
    B: ManagedKeyBackend,
{
    with_fixture(backend, |store| async move {
        let params = issue_params(11);
        let index_hash = params.index_hash;
        store
            .seed_existing("principal-a", "key-a", &params, 1_700_000_000)
            .await?;
        let issued = store
            .get("principal-a", "key-a")
            .await?
            .ok_or_else(|| anyhow::anyhow!("seeded key should be readable"))?;
        let expected = expected_record(&params, issued.issued_at_unix_secs);
        assert_record_bytes_eq(&issued, &expected)?;

        let fetched = store
            .get("principal-a", "key-a")
            .await?
            .ok_or_else(|| anyhow::anyhow!("issued key should be readable"))?;
        assert_record_bytes_eq(&fetched, &issued)?;

        let lookup = store
            .lookup_by_index_hash(&index_hash)
            .await?
            .ok_or_else(|| anyhow::anyhow!("index lookup should find issued key"))?;
        ensure!(lookup.0 == "principal-a", "lookup principal mismatch");
        ensure!(lookup.1 == "key-a", "lookup key mismatch");
        assert_record_bytes_eq(&lookup.2, &issued)?;

        let principal_records = store.list_by_principal("principal-a").await?;
        ensure!(
            principal_records.len() == 1,
            "principal list should contain one key"
        );
        assert_record_bytes_eq(&principal_records[0], &issued)?;

        let all_records = store.list_all().await?;
        ensure!(all_records.len() == 1, "list_all should contain one key");
        ensure!(
            all_records[0].0 == "principal-a",
            "list_all principal mismatch"
        );
        ensure!(all_records[0].1 == "key-a", "list_all key mismatch");
        assert_record_bytes_eq(&all_records[0].2, &issued)?;

        store
            .update(
                "principal-a",
                "key-a",
                ApiKeyMutation {
                    label: Some("renamed-key".to_owned()),
                    description: Some(None),
                    expires_at_unix_secs: Some(None),
                    limit_overrides: Some(vec![Limit {
                        kind: LimitKind::OutputTokens,
                        window_secs: 300,
                        cap_micros: 99,
                    }]),
                    status: Some(KeyStatus::Disabled),
                },
            )
            .await?;

        let updated = store
            .lookup_by_index_hash(&index_hash)
            .await?
            .ok_or_else(|| anyhow::anyhow!("disabled key should stay indexed"))?
            .2;
        ensure!(updated.label == "renamed-key", "label mutation not stored");
        ensure!(
            updated.description.is_none(),
            "description mutation not stored"
        );
        ensure!(
            updated.expires_at_unix_secs.is_none(),
            "expiry mutation not stored"
        );
        ensure!(
            updated.status == KeyStatus::Disabled,
            "status mutation not stored"
        );
        ensure!(
            updated.limit_overrides
                == vec![Limit {
                    kind: LimitKind::OutputTokens,
                    window_secs: 300,
                    cap_micros: 99,
                }],
            "limit mutation not stored"
        );

        store.revoke_zero_secrets("principal-a", "key-a").await?;
        ensure!(
            store.lookup_by_index_hash(&index_hash).await?.is_none(),
            "revoked key should be removed from index"
        );
        let revoked = store
            .get("principal-a", "key-a")
            .await?
            .ok_or_else(|| anyhow::anyhow!("revoked key record should remain stored"))?;
        ensure!(
            revoked.status == KeyStatus::Revoked,
            "revoked status mismatch"
        );
        ensure!(
            revoked.revoked_at_unix_secs.is_some(),
            "revoked timestamp should be set"
        );
        ensure!(
            revoked.index_hash == [0; 32],
            "revoked index hash should be zeroed"
        );
        ensure!(
            revoked.verify_hash == [0; 32],
            "revoked verify hash should be zeroed"
        );
        ensure!(
            revoked.secret_salt == [0; 16],
            "revoked salt should be zeroed"
        );
        ensure!(
            revoked.last_4 == params.last_4,
            "revoked last_4 should be preserved"
        );

        Ok(())
    })
    .await
}

pub async fn managed_keys_nul_byte_rejected<B>(backend: Arc<B>) -> Result<()>
where
    B: ManagedKeyBackend,
{
    with_fixture(backend, |store| async move {
        let principal_error = store
            .update("bad\0principal", "key-a", ApiKeyMutation::default())
            .await
            .expect_err("NUL principal id should be rejected");
        assert_invalid_field(principal_error, "principal_id")?;

        let key_error = store
            .get("principal-a", "bad\0key")
            .await
            .expect_err("NUL key id should be rejected");
        assert_invalid_field(key_error, "key_id")?;

        let list_error = store
            .list_by_principal("bad\0principal")
            .await
            .expect_err("NUL principal list id should be rejected");
        assert_invalid_field(list_error, "principal_id")?;

        Ok(())
    })
    .await
}

pub async fn managed_keys_concurrent_issue_no_index_collision<B>(backend: Arc<B>) -> Result<()>
where
    B: ManagedKeyBackend,
{
    const KEY_COUNT: u8 = 100;

    with_fixture(backend, |store| async move {
        // Issuance is paused: every concurrent issue must report Unavailable.
        let issues = (0..KEY_COUNT).map(|seed| {
            let store = Arc::clone(&store);
            async move {
                let key_id = format!("key-{seed:02}");
                let params = issue_params(seed.wrapping_add(40));
                store.issue("principal-concurrent", &key_id, params).await
            }
        });
        let results = join_all(issues).await;
        ensure!(
            results.len() == usize::from(KEY_COUNT),
            "all concurrent issues should complete"
        );
        let mut unavailable = 0_usize;
        for result in &results {
            match result {
                Err(StorageError::Unavailable { .. }) => unavailable += 1,
                other => anyhow::bail!("paused issue should be Unavailable, got {other:?}"),
            }
        }
        ensure!(
            unavailable == usize::from(KEY_COUNT),
            "every concurrent issue should be Unavailable"
        );
        ensure!(
            store
                .list_by_principal("principal-concurrent")
                .await?
                .is_empty(),
            "paused issuance should persist no rows"
        );

        // Preseed existing keys, then exercise concurrent index lookups.
        let mut seeded = Vec::with_capacity(usize::from(KEY_COUNT));
        for seed in 0..KEY_COUNT {
            let key_id = format!("key-{seed:02}");
            let params = issue_params(seed.wrapping_add(40));
            let index_hash = params.index_hash;
            store
                .seed_existing("principal-concurrent", &key_id, &params, 1_700_000_000)
                .await?;
            let record = store
                .get("principal-concurrent", &key_id)
                .await?
                .ok_or_else(|| anyhow::anyhow!("seeded key {key_id} should be readable"))?;
            seeded.push((key_id, index_hash, record));
        }

        let lookups = seeded.iter().map(|(_, index_hash, _)| {
            let store = Arc::clone(&store);
            let index_hash = *index_hash;
            async move { store.lookup_by_index_hash(&index_hash).await }
        });
        let looked_up = try_join_all(lookups).await?;
        ensure!(
            looked_up.len() == usize::from(KEY_COUNT),
            "all concurrent lookups should complete"
        );
        let unique: HashSet<&str> = seeded
            .iter()
            .map(|(key_id, _, _)| key_id.as_str())
            .collect();
        assert_eq!(unique.len(), 100);

        let listed = store.list_by_principal("principal-concurrent").await?;
        ensure!(
            listed.len() == usize::from(KEY_COUNT),
            "principal list should contain every seeded key"
        );

        for ((key_id, index_hash, seeded_record), lookup) in seeded.iter().zip(looked_up) {
            let fetched = store
                .get("principal-concurrent", key_id)
                .await?
                .ok_or_else(|| anyhow::anyhow!("missing seeded key {key_id}"))?;
            assert_record_bytes_eq(&fetched, seeded_record)?;

            let lookup =
                lookup.ok_or_else(|| anyhow::anyhow!("missing index lookup for {key_id}"))?;
            ensure!(
                lookup.0 == "principal-concurrent",
                "concurrent lookup principal mismatch"
            );
            ensure!(&lookup.1 == key_id, "concurrent lookup key mismatch");
            ensure!(
                lookup.2.index_hash == *index_hash,
                "concurrent lookup index hash mismatch"
            );
            assert_record_bytes_eq(&lookup.2, seeded_record)?;
        }

        Ok(())
    })
    .await
}

pub async fn managed_keys_cross_backend_equivalence(
    sqlite: &dyn ManagedKeySeed,
    postgres: &dyn ManagedKeySeed,
) -> Result<()> {
    let params = issue_params(121);
    sqlite
        .seed_existing(
            "principal-cross-backend-equivalence",
            "key-cross-backend-equivalence",
            &params,
            1_700_000_000,
        )
        .await?;
    postgres
        .seed_existing(
            "principal-cross-backend-equivalence",
            "key-cross-backend-equivalence",
            &params,
            1_700_000_000,
        )
        .await?;

    let sqlite_record = sqlite
        .get(
            "principal-cross-backend-equivalence",
            "key-cross-backend-equivalence",
        )
        .await?
        .ok_or_else(|| anyhow::anyhow!("sqlite seeded key should be readable"))?;
    let postgres_record = postgres
        .get(
            "principal-cross-backend-equivalence",
            "key-cross-backend-equivalence",
        )
        .await?
        .ok_or_else(|| anyhow::anyhow!("postgres seeded key should be readable"))?;

    assert_record_bytes_eq(&sqlite_record, &postgres_record)
}

pub async fn managed_keys_equivalent_records<B>(backend: Arc<B>) -> Result<()>
where
    B: ManagedKeyBackend,
{
    with_fixture(backend, |store| async move {
        let first_params = issue_params(91);
        let first_index_hash = first_params.index_hash;
        store
            .seed_existing(
                "principal-equivalence",
                "key-a",
                &first_params,
                1_700_000_000,
            )
            .await?;
        let second_params = issue_params(92);
        store
            .seed_existing(
                "principal-equivalence",
                "key-b",
                &second_params,
                1_700_000_100,
            )
            .await?;
        let first = store
            .get("principal-equivalence", "key-a")
            .await?
            .ok_or_else(|| anyhow::anyhow!("key-a should be readable"))?;
        let second = store
            .get("principal-equivalence", "key-b")
            .await?
            .ok_or_else(|| anyhow::anyhow!("key-b should be readable"))?;

        let fetched = store
            .get("principal-equivalence", "key-a")
            .await?
            .ok_or_else(|| anyhow::anyhow!("key-a should be readable"))?;
        let lookup = store
            .lookup_by_index_hash(&first_index_hash)
            .await?
            .ok_or_else(|| anyhow::anyhow!("key-a index should be readable"))?
            .2;
        let principal_records = store.list_by_principal("principal-equivalence").await?;
        let listed_first = principal_records
            .iter()
            .find(|record| record.index_hash == first.index_hash)
            .ok_or_else(|| anyhow::anyhow!("principal list missing key-a"))?;
        let listed_second = principal_records
            .iter()
            .find(|record| record.index_hash == second.index_hash)
            .ok_or_else(|| anyhow::anyhow!("principal list missing key-b"))?;
        let all_records = store.list_all().await?;
        let all_first = all_records
            .iter()
            .find(|(principal_id, key_id, _)| {
                principal_id == "principal-equivalence" && key_id == "key-a"
            })
            .ok_or_else(|| anyhow::anyhow!("list_all missing key-a"))?;

        assert_record_bytes_eq(&fetched, &first)?;
        assert_record_bytes_eq(&lookup, &first)?;
        assert_record_bytes_eq(listed_first, &first)?;
        assert_record_bytes_eq(&all_first.2, &first)?;
        assert_record_bytes_eq(listed_second, &second)?;

        Ok(())
    })
    .await
}

async fn with_fixture<B, F, Fut>(backend: Arc<B>, scenario: F) -> Result<()>
where
    B: ManagedKeyBackend,
    F: FnOnce(Arc<B::Store>) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let mut fixture = ManagedKeyFixture::new(backend).await?;
    let result = scenario(fixture.store()).await;
    let teardown = fixture.teardown().await;
    result?;
    teardown
}

fn issue_params(seed: u8) -> IssueParams {
    IssueParams {
        label: format!("managed-key-{seed}"),
        description: Some(format!("description-{seed}")),
        expires_at_unix_secs: Some(1_900_000_000 + u64::from(seed)),
        limit_overrides: vec![
            Limit {
                kind: LimitKind::Requests,
                window_secs: 60,
                cap_micros: i64::from(seed) * 10,
            },
            Limit {
                kind: LimitKind::InputTokens,
                window_secs: 3_600,
                cap_micros: i64::from(seed) * 100,
            },
        ],
        secret_salt: [seed; 16],
        verify_hash: [seed.wrapping_add(1); 32],
        last_4: format!("{seed:04}"),
        index_hash: [seed.wrapping_add(2); 32],
    }
}

fn expected_record(
    params: &IssueParams,
    issued_at_unix_secs: u64,
) -> cc_lb_storage_api::StoredApiKeyRecord {
    cc_lb_storage_api::StoredApiKeyRecord {
        label: params.label.clone(),
        issued_at_unix_secs,
        revoked_at_unix_secs: None,
        key_hash_b64: base64_url_no_pad(&params.verify_hash),
        verify_hash: params.verify_hash,
        secret_salt: params.secret_salt,
        limit_overrides: params.limit_overrides.clone(),
        status: KeyStatus::Active,
        expires_at_unix_secs: params.expires_at_unix_secs,
        last_4: params.last_4.clone(),
        description: params.description.clone(),
        index_hash: params.index_hash,
    }
}

fn assert_record_bytes_eq(
    left: &cc_lb_storage_api::StoredApiKeyRecord,
    right: &cc_lb_storage_api::StoredApiKeyRecord,
) -> Result<()> {
    let left_bytes = normalized_record_bytes(left)?;
    let right_bytes = normalized_record_bytes(right)?;
    ensure!(left_bytes == right_bytes, "normalized record bytes differ");
    Ok(())
}

fn normalized_record_bytes(record: &cc_lb_storage_api::StoredApiKeyRecord) -> Result<Vec<u8>> {
    let mut normalized = record.clone();
    normalized.issued_at_unix_secs = 0;
    normalized.revoked_at_unix_secs = normalized.revoked_at_unix_secs.map(|_| 0);
    Ok(serde_json::to_vec(&normalized)?)
}

fn assert_invalid_field(error: StorageError, expected_field: &str) -> Result<()> {
    match error {
        StorageError::InvalidInput { field, reason } => {
            ensure!(field == expected_field, "invalid input field mismatch");
            ensure!(
                reason.contains("NUL"),
                "invalid input reason should mention NUL"
            );
            Ok(())
        }
        other => Err(anyhow::anyhow!(
            "expected InvalidInput for {expected_field}, got {other:?}"
        )),
    }
}

fn base64_url_no_pad(bytes: &[u8; 32]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::with_capacity((bytes.len() * 4).div_ceil(3));
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0];
        let b1 = chunk.get(1).copied().unwrap_or(0);
        let b2 = chunk.get(2).copied().unwrap_or(0);

        out.push(ALPHABET[(b0 >> 2) as usize] as char);
        out.push(ALPHABET[(((b0 & 0b0000_0011) << 4) | (b1 >> 4)) as usize] as char);
        if chunk.len() > 1 {
            out.push(ALPHABET[(((b1 & 0b0000_1111) << 2) | (b2 >> 6)) as usize] as char);
        }
        if chunk.len() > 2 {
            out.push(ALPHABET[(b2 & 0b0011_1111) as usize] as char);
        }
    }
    out
}
