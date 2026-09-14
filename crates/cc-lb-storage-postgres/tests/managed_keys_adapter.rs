use std::{error::Error, str::FromStr, sync::Arc};

use cc_lb_storage_api::{
    ManagedKeyStore, StorageError,
    types::{
        ApiKeyMutation, IssueParams, KeyStatus, Limit, LimitKind, PrincipalKindLite, UpstreamKind,
    },
};
use cc_lb_storage_postgres::{PostgresManagedKeyStore, adapter::retry};
use sqlx::{
    AssertSqlSafe, PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

const MIGRATIONS: &[&str] = &[
    include_str!("../migrations/0013_managed_api_keys.sql"),
    include_str!("../migrations/0014_managed_api_key_index.sql"),
    include_str!("../migrations/0020_drop_per_key_pin.sql"),
];

#[tokio::test]
async fn t3_postgres__issue_get_lookup_update_list_and_revoke() -> TestResult {
    let url = crate::postgres_fixture::required_postgres_url();
    let fixture = Fixture::create(&url, "lifecycle").await?;
    let store = fixture.store();
    let params = issue_params(11);
    let index_hash = params.index_hash;

    let issued = store.issue("principal-a", "key-a", params.clone()).await?;
    assert_eq!(issued.label, "label-11");
    assert_eq!(issued.status, KeyStatus::Active);
    assert_eq!(issued.verify_hash, params.verify_hash);

    let fetched = store.get("principal-a", "key-a").await?;
    assert_eq!(fetched.as_ref(), Some(&issued));

    let lookup = store
        .lookup_by_index_hash(&index_hash)
        .await?
        .ok_or("lookup should find issued key")?;
    assert_eq!(lookup.0, "principal-a");
    assert_eq!(lookup.1, "key-a");
    assert_eq!(lookup.2, issued);

    let second = store
        .issue("principal-b", "key-b", issue_params(22))
        .await?;
    assert_eq!(store.list_by_principal("principal-a").await?.len(), 1);
    let all = store.list_all().await?;
    assert_eq!(all.len(), 2);
    assert!(all.iter().any(|(_, _, record)| record == &second));

    store
        .update(
            "principal-a",
            "key-a",
            ApiKeyMutation {
                label: Some("renamed".to_owned()),
                description: Some(None),
                expires_at_unix_secs: Some(None),
                limit_overrides: Some(vec![Limit {
                    kind: LimitKind::OutputTokens,
                    window_secs: 120,
                    cap_micros: 55,
                }]),
                status: Some(KeyStatus::Disabled),
            },
        )
        .await?;
    let updated = store
        .lookup_by_index_hash(&index_hash)
        .await?
        .ok_or("disabled key should remain indexed")?
        .2;
    assert_eq!(updated.label, "renamed");
    assert_eq!(updated.description, None);
    assert_eq!(updated.expires_at_unix_secs, None);
    assert_eq!(updated.status, KeyStatus::Disabled);
    assert_eq!(updated.limit_overrides[0].kind, LimitKind::OutputTokens);

    store.revoke_zero_secrets("principal-a", "key-a").await?;
    assert!(store.lookup_by_index_hash(&index_hash).await?.is_none());
    let revoked = store
        .get("principal-a", "key-a")
        .await?
        .ok_or("revoked record should remain stored")?;
    assert_eq!(revoked.status, KeyStatus::Revoked);
    assert!(revoked.revoked_at_unix_secs.is_some());
    assert_eq!(revoked.index_hash, [0; 32]);
    assert_eq!(revoked.verify_hash, [0; 32]);
    assert_eq!(revoked.secret_salt, [0; 16]);
    assert_eq!(revoked.last_4, params.last_4);

    fixture.drop_schema().await
}

#[tokio::test]
async fn t3_postgres__duplicate_index_hash_surfaces_conflict() -> TestResult {
    let url = crate::postgres_fixture::required_postgres_url();
    let fixture = Fixture::create(&url, "duplicate_index").await?;
    let store = fixture.store();
    let params = issue_params(33);
    store.issue("principal-a", "key-a", params.clone()).await?;
    let error = store
        .issue("principal-a", "key-b", params)
        .await
        .err()
        .ok_or("duplicate index hash should fail")?;
    assert!(matches!(error, StorageError::Conflict { .. }));

    fixture.drop_schema().await
}

#[tokio::test]
async fn t3_postgres__invalid_identifier_rejected_before_sql() -> TestResult {
    let url = crate::postgres_fixture::required_postgres_url();
    let fixture = Fixture::create(&url, "invalid_identifier").await?;
    let store = fixture.store();
    let error = store
        .get("", "key-a")
        .await
        .err()
        .ok_or("empty principal should fail")?;
    assert!(matches!(error, StorageError::InvalidInput { .. }));

    fixture.drop_schema().await
}

struct Fixture {
    url: String,
    schema: String,
    pool: PgPool,
}

impl Fixture {
    async fn create(url: &str, case: &str) -> TestResult<Self> {
        let schema = format!("test_managed_keys_{case}");
        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(PgConnectOptions::from_str(url)?)
            .await?;
        sqlx::query(AssertSqlSafe(format!(
            "DROP SCHEMA IF EXISTS {schema} CASCADE"
        )))
        .execute(&admin_pool)
        .await?;
        sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
            .execute(&admin_pool)
            .await?;
        admin_pool.close().await;

        let pool = PgPoolOptions::new()
            .max_connections(4)
            .connect_with(
                PgConnectOptions::from_str(url)?.options([("search_path", schema.as_str())]),
            )
            .await?;
        for migration in MIGRATIONS {
            sqlx::raw_sql(*migration).execute(&pool).await?;
        }

        Ok(Self {
            url: url.to_owned(),
            schema,
            pool,
        })
    }

    fn store(&self) -> PostgresManagedKeyStore {
        PostgresManagedKeyStore::new(
            self.pool.clone(),
            Arc::new(retry::RetryPolicy::default()),
            cc_lb_testkit::fixed_clock(1_700_000_000),
        )
    }

    async fn drop_schema(self) -> TestResult {
        self.pool.close().await;
        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(PgConnectOptions::from_str(&self.url)?)
            .await?;
        sqlx::query(AssertSqlSafe(format!(
            "DROP SCHEMA IF EXISTS {} CASCADE",
            self.schema
        )))
        .execute(&admin_pool)
        .await?;
        admin_pool.close().await;
        Ok(())
    }
}

fn issue_params(seed: u8) -> IssueParams {
    IssueParams {
        label: format!("label-{seed}"),
        description: Some(format!("description-{seed}")),
        upstream_kind: UpstreamKind::AnthropicKey,
        expires_at_unix_secs: Some(1_800_000_000 + u64::from(seed)),
        limit_overrides: vec![Limit {
            kind: LimitKind::Requests,
            window_secs: 60,
            cap_micros: i64::from(seed),
        }],
        secret_salt: [seed; 16],
        verify_hash: [seed; 32],
        last_4: format!("{seed:04}"),
        principal_kind: PrincipalKindLite::Machine,
        index_hash: [seed.wrapping_add(100); 32],
    }
}
