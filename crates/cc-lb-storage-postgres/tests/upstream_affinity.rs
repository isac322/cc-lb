use std::{error::Error, str::FromStr, sync::Arc};

use cc_lb_storage_api::{
    BackendKind, MetaStore, StorageError, UpstreamAffinityBinding, UpstreamAffinityKey,
    UpstreamAffinityKind, UpstreamAffinityStore, UpstreamCreate, UpstreamStore,
    upstream::UpstreamKind,
};
use cc_lb_storage_postgres::PostgresStorage;
use sha2::{Digest, Sha256};
use sqlx::{
    AssertSqlSafe, PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use uuid::Uuid;

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[tokio::test]
#[ignore = "requires CI_POSTGRES_URL; run with --ignored"]
async fn upstream_affinity_bind_is_idempotent_conflict_atomic_and_expiry_aware() -> TestResult<()> {
    let Some(fixture) = Fixture::create().await? else {
        return Ok(());
    };
    let storage = fixture.store();
    storage.initialize(BackendKind::Postgres).await?;

    let first_upstream = create_upstream(&storage, "affinity-first").await?;
    let second_upstream = create_upstream(&storage, "affinity-second").await?;
    let raw_opaque_value = "opaque-encrypted-content-that-must-never-be-stored";
    let first_key = affinity_key("principal-a", digest(raw_opaque_value));
    let second_key = affinity_key("principal-a", [2; 32]);

    storage
        .bind_upstream_affinities(&[
            binding(first_key.clone(), first_upstream, 10, Some(500)),
            binding(second_key.clone(), first_upstream, 11, None),
        ])
        .await?;

    let updated = binding(first_key.clone(), first_upstream, 20, Some(600));
    storage
        .bind_upstream_affinities(std::slice::from_ref(&updated))
        .await?;
    assert_eq!(
        storage
            .resolve_upstream_affinities(std::slice::from_ref(&first_key), 500)
            .await?,
        vec![updated.clone()]
    );
    storage
        .bind_upstream_affinities(&[binding(first_key.clone(), first_upstream, 15, Some(550))])
        .await?;
    assert_eq!(
        storage
            .resolve_upstream_affinities(std::slice::from_ref(&first_key), 500)
            .await?,
        vec![updated.clone()]
    );

    let error = storage
        .bind_upstream_affinities(&[
            binding(first_key.clone(), first_upstream, 30, Some(700)),
            binding(second_key.clone(), second_upstream, 31, None),
        ])
        .await
        .expect_err("different upstream target must conflict");
    assert!(matches!(error, StorageError::Conflict { .. }));

    let after_conflict = storage
        .resolve_upstream_affinities(&[first_key.clone(), second_key.clone()], 0)
        .await?;
    assert_eq!(after_conflict.len(), 2);
    assert!(after_conflict.contains(&updated));
    assert!(after_conflict.contains(&binding(second_key.clone(), first_upstream, 11, None,)));

    let expiring_key = affinity_key("principal-a", [3; 32]);
    let permanent_key = affinity_key("principal-a", [4; 32]);
    let permanent = binding(permanent_key.clone(), first_upstream, 40, None);
    storage
        .bind_upstream_affinities(&[
            binding(expiring_key.clone(), first_upstream, 40, Some(100)),
            permanent.clone(),
        ])
        .await?;
    assert_eq!(
        storage
            .resolve_upstream_affinities(&[expiring_key, permanent_key], 100)
            .await?,
        vec![permanent]
    );

    let (stored_len, stored_digest): (i32, Vec<u8>) = sqlx::query_as(
        "SELECT octet_length(value_sha256), value_sha256 \
         FROM upstream_affinity_v1 \
         WHERE principal_id = $1 AND provider = $2 AND kind = $3 AND value_sha256 = $4",
    )
    .bind(&first_key.principal_id)
    .bind(&first_key.provider)
    .bind(first_key.kind.as_str())
    .bind(first_key.value_sha256.as_slice())
    .fetch_one(fixture.pool())
    .await?;
    assert_eq!(stored_len, 32);
    assert_eq!(stored_digest.as_slice(), first_key.value_sha256.as_slice());
    let raw_match_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM upstream_affinity_v1 WHERE value_sha256 = $1")
            .bind(raw_opaque_value.as_bytes())
            .fetch_one(fixture.pool())
            .await?;
    assert_eq!(raw_match_count, 0);

    fixture.drop_schema().await
}

async fn create_upstream(storage: &PostgresStorage, name: &str) -> TestResult<Uuid> {
    Ok(UpstreamStore::create(
        storage,
        UpstreamCreate {
            name: name.to_owned(),
            kind: UpstreamKind::AnthropicOauth,
            ..UpstreamCreate::default()
        },
    )
    .await?
    .id)
}

fn affinity_key(principal_id: &str, value_sha256: [u8; 32]) -> UpstreamAffinityKey {
    UpstreamAffinityKey {
        principal_id: principal_id.to_owned(),
        provider: "anthropic".to_owned(),
        kind: UpstreamAffinityKind::AnthropicWebSearchEncryptedContent,
        value_sha256,
    }
}

fn binding(
    key: UpstreamAffinityKey,
    upstream_id: Uuid,
    observed_at_unix_secs: u64,
    expires_at_unix_secs: Option<u64>,
) -> UpstreamAffinityBinding {
    UpstreamAffinityBinding {
        key,
        upstream_id,
        observed_at_unix_secs,
        expires_at_unix_secs,
    }
}

fn digest(value: &str) -> [u8; 32] {
    Sha256::digest(value.as_bytes()).into()
}

struct Fixture {
    schema: String,
    admin_pool: PgPool,
    pool: PgPool,
}

impl Fixture {
    async fn create() -> TestResult<Option<Self>> {
        let Some(url) = std::env::var("CI_POSTGRES_URL").ok() else {
            eprintln!("skip: CI_POSTGRES_URL not set");
            return Ok(None);
        };
        let schema = format!("upstream_affinity_{}", Uuid::new_v4().simple());
        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await?;
        sqlx::query(AssertSqlSafe(format!(
            "CREATE SCHEMA {}",
            quote_ident(&schema)
        )))
        .execute(&admin_pool)
        .await?;
        let options = PgConnectOptions::from_str(&url)?.options([("search_path", schema.as_str())]);
        let pool = PgPoolOptions::new()
            .max_connections(4)
            .connect_with(options)
            .await?;
        Ok(Some(Self {
            schema,
            admin_pool,
            pool,
        }))
    }

    fn store(&self) -> PostgresStorage {
        PostgresStorage::new(self.pool.clone(), Arc::new(cc_lb_clock::SystemClock))
    }

    fn pool(&self) -> &PgPool {
        &self.pool
    }

    async fn drop_schema(self) -> TestResult<()> {
        self.pool.close().await;
        sqlx::query(AssertSqlSafe(format!(
            "DROP SCHEMA IF EXISTS {} CASCADE",
            quote_ident(&self.schema)
        )))
        .execute(&self.admin_pool)
        .await?;
        self.admin_pool.close().await;
        Ok(())
    }
}

fn quote_ident(identifier: &str) -> String {
    assert!(
        identifier
            .chars()
            .all(|character| character.is_ascii_lowercase()
                || character.is_ascii_digit()
                || character == '_'),
        "unsafe postgres identifier: {identifier}"
    );
    format!("\"{identifier}\"")
}
