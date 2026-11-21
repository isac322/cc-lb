use anyhow::Result;
use cc_lb_domain::{CoerceOrReject, OpenaiOauthCompatOverride};
use cc_lb_storage_api::principal::{Limit, LimitKind};
use cc_lb_storage_api::{
    MetaStore, PrincipalCreate, PrincipalKind, PrincipalStore, PrincipalUpdate,
};
use cc_lb_storage_postgres::PostgresStorage;
use serde_json::json;
use sqlx::PgPool;

fn get_postgres_url() -> Option<String> {
    std::env::var("CI_POSTGRES_URL").ok()
}

#[test]
fn principal_terminal_strategy() {
    let url = match get_postgres_url() {
        Some(url) => url,
        None => {
            eprintln!("skip: CI_POSTGRES_URL not set");
            return;
        }
    };

    tokio::runtime::Runtime::new()
        .expect("tokio runtime")
        .block_on(async move { run_test(&url).await })
        .expect("principal terminal strategy test");
}

async fn run_test(url: &str) -> Result<()> {
    let fixture = Fixture::create(url).await?;
    let storage = PostgresStorage::new(
        fixture.pool.clone(),
        std::sync::Arc::new(cc_lb_clock::SystemClock),
    );
    storage.initialize().await?;

    default_strategy_is_first_pick(&storage).await?;
    update_strategy_to_random_roundtrips(&storage).await?;
    oauth_compat_roundtrips(&storage).await?;
    unsupported_strategy_update_is_rejected(&fixture.pool, &storage).await?;

    fixture.drop_schema().await
}

async fn default_strategy_is_first_pick(storage: &PostgresStorage) -> Result<()> {
    let record =
        PrincipalStore::create(storage, principal_create("default-strategy"), 1_900_000_000)
            .await?;
    assert_eq!(
        strategy_json(&record.router_terminal_strategy)?,
        json!("first-pick")
    );

    let fetched = PrincipalStore::get_by_id(storage, record.id)
        .await?
        .expect("record should exist");
    assert_eq!(
        strategy_json(&fetched.router_terminal_strategy)?,
        json!("first-pick")
    );
    Ok(())
}

async fn update_strategy_to_random_roundtrips(storage: &PostgresStorage) -> Result<()> {
    let record =
        PrincipalStore::create(storage, principal_create("random-strategy"), 1_900_000_010).await?;
    let random_strategy = serde_json::from_value(json!("random"))?;

    let updated = PrincipalStore::update(
        storage,
        record.id,
        record.revision,
        PrincipalUpdate {
            router_terminal_strategy: Some(random_strategy),
            ..PrincipalUpdate::default()
        },
        1_900_000_011,
    )
    .await?
    .expect("record should exist");
    assert_eq!(
        strategy_json(&updated.router_terminal_strategy)?,
        json!("random")
    );

    let fetched = PrincipalStore::get_by_name(storage, &record.name)
        .await?
        .expect("record should exist");
    assert_eq!(
        strategy_json(&fetched.router_terminal_strategy)?,
        json!("random")
    );
    Ok(())
}
async fn oauth_compat_roundtrips(storage: &PostgresStorage) -> Result<()> {
    let initial = OpenaiOauthCompatOverride {
        store_true: Some(CoerceOrReject::Reject),
        ..Default::default()
    };
    let mut create = principal_create("oauth-compat");
    create.openai_oauth_compat = Some(initial);
    let record = PrincipalStore::create(storage, create, 1_900_000_030).await?;
    assert_eq!(record.openai_oauth_compat, Some(initial));

    let fetched = PrincipalStore::get_by_id(storage, record.id)
        .await?
        .expect("record should exist");
    assert_eq!(fetched.openai_oauth_compat, Some(initial));

    let next = OpenaiOauthCompatOverride {
        store_true: Some(CoerceOrReject::Coerce),
        ..Default::default()
    };
    let updated = PrincipalStore::update(
        storage,
        record.id,
        fetched.revision,
        PrincipalUpdate {
            openai_oauth_compat: Some(Some(next)),
            ..PrincipalUpdate::default()
        },
        1_900_000_031,
    )
    .await?
    .expect("record should exist");
    assert_eq!(updated.openai_oauth_compat, Some(next));

    let cleared = PrincipalStore::update(
        storage,
        record.id,
        updated.revision,
        PrincipalUpdate {
            openai_oauth_compat: Some(None),
            ..PrincipalUpdate::default()
        },
        1_900_000_032,
    )
    .await?
    .expect("record should exist");
    assert_eq!(cleared.openai_oauth_compat, None);
    Ok(())
}

async fn unsupported_strategy_update_is_rejected(
    pool: &PgPool,
    storage: &PostgresStorage,
) -> Result<()> {
    let record = PrincipalStore::create(
        storage,
        principal_create("unsupported-strategy"),
        1_900_000_020,
    )
    .await?;

    let error = sqlx::query(
        "UPDATE principals_v1 SET router_terminal_strategy = 'round-robin' WHERE id = $1",
    )
    .bind(record.id)
    .execute(pool)
    .await
    .expect_err("unsupported terminal strategy must be rejected by postgres");
    let database_error = error
        .as_database_error()
        .expect("postgres check violation is a database error");
    assert_eq!(database_error.code().as_deref(), Some("23514"));

    let fetched = PrincipalStore::get_by_id(storage, record.id)
        .await?
        .expect("record should exist");
    assert_eq!(
        strategy_json(&fetched.router_terminal_strategy)?,
        json!("first-pick")
    );
    Ok(())
}

fn principal_create(name: &str) -> PrincipalCreate {
    PrincipalCreate {
        name: name.to_owned(),
        kind: PrincipalKind::Machine,
        allowed_models: vec!["claude-sonnet-*".to_owned()],
        allowed_upstreams: vec![],
        default_limits: vec![Limit {
            kind: LimitKind::Requests,
            window_secs: 60,
            cap_micros: 100,
        }],
        cache_keepalive: None,
        openai_oauth_compat: None,
    }
}

fn strategy_json(strategy: &impl serde::Serialize) -> Result<serde_json::Value> {
    Ok(serde_json::to_value(strategy)?)
}

struct Fixture {
    isolated: crate::pg_test_support::IsolatedPg,
    pool: PgPool,
}

impl Fixture {
    async fn create(url: &str) -> Result<Self> {
        let isolated = crate::pg_test_support::IsolatedPg::create_templated(url).await?;
        let pool = match isolated.pool(4).await {
            Ok(pool) => pool,
            Err(error) => {
                let _ = isolated.drop().await;
                return Err(error);
            }
        };

        Ok(Self { isolated, pool })
    }

    async fn drop_schema(self) -> Result<()> {
        self.pool.close().await;
        self.isolated.drop().await
    }
}
