//! Admin mutations make COMMIT their final await: the returned record is read
//! inside the mutation's transaction, and principal change notifications are
//! queued inside it too. A record decode or notification failure therefore
//! rolls the mutation back instead of leaving a committed change plus an error.

use std::str::FromStr;
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use cc_lb_aead::EncryptedOAuthTokens;
use cc_lb_storage_api::principal::{Limit, LimitKind};
use cc_lb_storage_api::{
    MetaStore, OAuthTokensCreate, PluginRegistryStore, PrincipalCreate, PrincipalKind,
    PrincipalStore, PrincipalUpdate, UpstreamCreate, UpstreamKind, UpstreamRecord, UpstreamStore,
    UpstreamUpdate, UpstreamWarmupDialectPlugin,
};
use cc_lb_storage_postgres::PostgresStorage;
use sqlx::postgres::{PgConnectOptions, PgListener, PgPoolOptions};
use sqlx::{AssertSqlSafe, PgPool};
use url::Url;
use uuid::Uuid;

const PRINCIPAL_CHANNEL: &str = "cclb_principal_changed";

fn get_postgres_url() -> Option<String> {
    std::env::var("CI_POSTGRES_URL").ok()
}

#[test]
fn admin_mutation_commit_boundary() {
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
        .expect("admin mutation commit boundary test");
}

async fn run_test(url: &str) -> Result<()> {
    let fixture = Fixture::create(url).await?;
    let storage = PostgresStorage::new(
        fixture.pool.clone(),
        std::sync::Arc::new(cc_lb_clock::SystemClock),
    );
    storage.initialize().await?;

    upstream_mutations_return_committed_row(&storage).await?;
    upstream_record_decode_failure_rolls_back(&fixture.pool, &storage).await?;
    registry_label_returns_committed_row(&storage, &fixture.pool).await?;
    registry_label_decode_failure_rolls_back(&fixture.pool, &storage).await?;
    principal_mutations_return_committed_row_with_notification(&fixture.pool, &storage).await?;
    principal_notification_failure_rolls_back(&fixture, &storage).await?;

    fixture.drop_schema().await
}

async fn upstream_mutations_return_committed_row(storage: &PostgresStorage) -> Result<()> {
    let api = UpstreamStore::create(storage, api_key_upstream("committed-api")).await?;
    assert_committed_upstream(storage, &api).await?;

    let updated = UpstreamStore::update(
        storage,
        api.id,
        api.revision,
        UpstreamUpdate {
            name: Some("committed-api-renamed".to_owned()),
            api_key_ciphertext: Some(b"rotated".to_vec()),
            ..UpstreamUpdate::default()
        },
    )
    .await?;
    ensure!(updated.revision == api.revision + 1);
    assert_committed_upstream(storage, &updated).await?;

    let disabled = UpstreamStore::set_enabled(storage, api.id, updated.revision, false).await?;
    ensure!(!disabled.enabled);
    assert_committed_upstream(storage, &disabled).await?;

    let secret =
        UpstreamStore::update_api_key_secret(storage, api.id, Some(b"third".to_vec())).await?;
    assert_committed_upstream(storage, &secret).await?;

    let warmup = UpstreamStore::create(storage, warmup_upstream("committed-warmup")).await?;
    let cleared = UpstreamStore::clear_warmup_dialect_plugin(storage, warmup.id, warmup.revision)
        .await?
        .context("clear should match the current revision")?;
    ensure!(cleared.warmup_dialect_plugin.is_none());
    assert_committed_upstream(storage, &cleared).await?;
    let stale =
        UpstreamStore::clear_warmup_dialect_plugin(storage, warmup.id, warmup.revision).await?;
    ensure!(stale.is_none(), "stale clear keeps its no-row result");

    let oauth = UpstreamStore::create(storage, oauth_upstream("committed-oauth")).await?;
    let stored = UpstreamStore::update_oauth_token(storage, oauth.id, tokens(b"second")).await?;
    ensure!(stored.oauth_token_generation == oauth.oauth_token_generation + 1);
    assert_committed_upstream(storage, &stored).await?;

    let refreshed =
        UpstreamStore::complete_refresh(storage, oauth.id, Uuid::new_v4(), tokens(b"third"))
            .await?;
    ensure!(refreshed.oauth_token_generation == stored.oauth_token_generation + 1);
    assert_committed_upstream(storage, &refreshed).await?;
    Ok(())
}

/// A trigger corrupts the stored base URL so the in-transaction record decode
/// fails. Before the commit-last change the mutation committed first and the
/// later pool reload failed, leaving the corrupt change durable.
async fn upstream_record_decode_failure_rolls_back(
    pool: &PgPool,
    storage: &PostgresStorage,
) -> Result<()> {
    let api = UpstreamStore::create(storage, api_key_upstream("rollback-api")).await?;
    let warmup = UpstreamStore::create(storage, warmup_upstream("rollback-warmup")).await?;
    let oauth = UpstreamStore::create(storage, oauth_upstream("rollback-oauth")).await?;
    install_upstream_corruption(pool).await?;

    let create = UpstreamStore::create(storage, api_key_upstream("rollback-create")).await;
    let update = UpstreamStore::update(
        storage,
        api.id,
        api.revision,
        UpstreamUpdate {
            enabled: Some(false),
            ..UpstreamUpdate::default()
        },
    )
    .await;
    let set_enabled = UpstreamStore::set_enabled(storage, api.id, api.revision, false).await;
    let secret =
        UpstreamStore::update_api_key_secret(storage, api.id, Some(b"lost".to_vec())).await;
    let clear =
        UpstreamStore::clear_warmup_dialect_plugin(storage, warmup.id, warmup.revision).await;
    let token = UpstreamStore::update_oauth_token(storage, oauth.id, tokens(b"lost")).await;
    let store = UpstreamStore::store_oauth_tokens(
        storage,
        oauth.id,
        oauth.revision,
        tokens(b"lost"),
        false,
    )
    .await;
    let refresh =
        UpstreamStore::complete_refresh(storage, oauth.id, Uuid::new_v4(), tokens(b"lost")).await;

    remove_upstream_corruption(pool).await?;
    ensure!(create.is_err(), "create must surface the decode failure");
    ensure!(update.is_err(), "update must surface the decode failure");
    ensure!(
        set_enabled.is_err(),
        "set_enabled must surface the decode failure"
    );
    ensure!(
        secret.is_err(),
        "api key update must surface the decode failure"
    );
    ensure!(
        clear.is_err(),
        "warmup clear must surface the decode failure"
    );
    ensure!(
        token.is_err(),
        "token update must surface the decode failure"
    );
    ensure!(
        store.is_err(),
        "token store must surface the decode failure"
    );
    ensure!(refresh.is_err(), "refresh must surface the decode failure");

    ensure!(
        UpstreamStore::get_by_name(storage, "rollback-create")
            .await?
            .is_none(),
        "failed create must not commit"
    );
    for before in [&api, &warmup, &oauth] {
        let after = UpstreamStore::get_by_id(storage, before.id).await?;
        ensure!(
            after.as_ref() == Some(before),
            "failed mutation must leave {} unchanged: {after:?}",
            before.name
        );
    }
    Ok(())
}

async fn registry_label_returns_committed_row(
    storage: &PostgresStorage,
    pool: &PgPool,
) -> Result<()> {
    let id = insert_registry_entry(pool, "committed-label", 1).await?;
    let before = PluginRegistryStore::get_registry_entry_by_id(storage, id)
        .await?
        .context("registry entry exists")?;
    let updated = PluginRegistryStore::update_registry_label(
        storage,
        id,
        before.revision,
        Some("fresh".to_owned()),
    )
    .await?;
    ensure!(updated.label.as_deref() == Some("fresh"));
    ensure!(updated.revision == before.revision + 1);
    let committed = PluginRegistryStore::get_registry_entry_by_id(storage, id).await?;
    ensure!(committed.as_ref() == Some(&updated));
    Ok(())
}

/// A trigger stores undecodable hook metadata. Before the commit-last change
/// the label update committed, then its pool reload failed.
async fn registry_label_decode_failure_rolls_back(
    pool: &PgPool,
    storage: &PostgresStorage,
) -> Result<()> {
    let id = insert_registry_entry(pool, "rollback-label", 2).await?;
    let before = PluginRegistryStore::get_registry_entry_by_id(storage, id)
        .await?
        .context("registry entry exists")?;
    execute(
        pool,
        "CREATE FUNCTION corrupt_hook_metadata() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN NEW.hook_metadata := 'not json'; RETURN NEW; END $$",
    )
    .await?;
    execute(
        pool,
        "CREATE TRIGGER corrupt_hook_metadata BEFORE UPDATE ON wasm_registry_v2
         FOR EACH ROW EXECUTE FUNCTION corrupt_hook_metadata()",
    )
    .await?;

    let result = PluginRegistryStore::update_registry_label(
        storage,
        id,
        before.revision,
        Some("lost".to_owned()),
    )
    .await;

    execute(
        pool,
        "DROP TRIGGER corrupt_hook_metadata ON wasm_registry_v2",
    )
    .await?;
    execute(pool, "DROP FUNCTION corrupt_hook_metadata()").await?;
    ensure!(
        result.is_err(),
        "label update must surface the decode failure"
    );
    let after = PluginRegistryStore::get_registry_entry_by_id(storage, id).await?;
    ensure!(
        after.as_ref() == Some(&before),
        "failed label update must not commit: {after:?}"
    );
    Ok(())
}

async fn principal_mutations_return_committed_row_with_notification(
    pool: &PgPool,
    storage: &PostgresStorage,
) -> Result<()> {
    let mut listener = PgListener::connect_with(pool).await?;
    listener.listen(PRINCIPAL_CHANNEL).await?;

    let created =
        PrincipalStore::create(storage, principal_create("notify-principal"), 1_900_000_000)
            .await?;
    expect_principal_notification(&mut listener, created.id).await?;
    ensure!(PrincipalStore::get_by_id(storage, created.id).await? == Some(created.clone()));

    let updated = PrincipalStore::update(
        storage,
        created.id,
        created.revision,
        PrincipalUpdate {
            allowed_models: Some(vec!["claude-opus-*".to_owned()]),
            ..PrincipalUpdate::default()
        },
        1_900_000_001,
    )
    .await?
    .context("principal exists")?;
    ensure!(updated.revision == created.revision + 1);
    expect_principal_notification(&mut listener, created.id).await?;
    ensure!(PrincipalStore::get_by_id(storage, created.id).await? == Some(updated.clone()));

    let disabled =
        PrincipalStore::set_enabled(storage, created.id, updated.revision, false, 1_900_000_002)
            .await?
            .context("principal exists")?;
    ensure!(!disabled.enabled);
    expect_principal_notification(&mut listener, created.id).await?;
    ensure!(PrincipalStore::get_by_id(storage, created.id).await? == Some(disabled));
    Ok(())
}

/// Shadows `pg_notify(text, text)` ahead of `pg_catalog` in this schema's
/// search path with a function that always raises. Before the commit-last
/// change the principal mutation committed and only the later pool NOTIFY
/// failed, leaving the change durable without its notification. The failing
/// storage uses a pool opened after the shadow exists, so no connection holds
/// a statement already resolved to `pg_catalog.pg_notify`.
async fn principal_notification_failure_rolls_back(
    fixture: &Fixture,
    storage: &PostgresStorage,
) -> Result<()> {
    let pool = &fixture.pool;
    let existing =
        PrincipalStore::create(storage, principal_create("notify-failure"), 1_900_000_100).await?;
    execute(
        pool,
        "CREATE FUNCTION pg_notify(text, text) RETURNS void LANGUAGE plpgsql AS $$
         BEGIN RAISE EXCEPTION 'notify unavailable'; END $$",
    )
    .await?;
    let failing_pool = fixture.fresh_pool().await?;
    let failing = PostgresStorage::new(
        failing_pool.clone(),
        std::sync::Arc::new(cc_lb_clock::SystemClock),
    );

    let create = PrincipalStore::create(
        &failing,
        principal_create("notify-failure-new"),
        1_900_000_101,
    )
    .await;
    let update = PrincipalStore::update(
        &failing,
        existing.id,
        existing.revision,
        PrincipalUpdate {
            allowed_models: Some(vec!["claude-opus-*".to_owned()]),
            ..PrincipalUpdate::default()
        },
        1_900_000_102,
    )
    .await;
    let set_enabled = PrincipalStore::set_enabled(
        &failing,
        existing.id,
        existing.revision,
        false,
        1_900_000_103,
    )
    .await;

    failing_pool.close().await;
    execute(pool, "DROP FUNCTION pg_notify(text, text)").await?;
    ensure!(
        create.is_err(),
        "create must surface the notification failure"
    );
    ensure!(
        update.is_err(),
        "update must surface the notification failure"
    );
    ensure!(
        set_enabled.is_err(),
        "set_enabled must surface the notification failure"
    );
    ensure!(
        PrincipalStore::get_by_name(storage, "notify-failure-new")
            .await?
            .is_none(),
        "failed create must not commit"
    );
    ensure!(
        PrincipalStore::get_by_id(storage, existing.id).await? == Some(existing),
        "failed principal updates must not commit"
    );
    Ok(())
}

async fn expect_principal_notification(listener: &mut PgListener, id: Uuid) -> Result<()> {
    let expected = id.to_string();
    loop {
        let notification = tokio::time::timeout(Duration::from_secs(10), listener.recv())
            .await
            .context("principal notification was not delivered")??;
        if notification.channel() == PRINCIPAL_CHANNEL && notification.payload() == expected {
            return Ok(());
        }
    }
}

async fn assert_committed_upstream(
    storage: &PostgresStorage,
    returned: &UpstreamRecord,
) -> Result<()> {
    let committed = UpstreamStore::get_by_id(storage, returned.id).await?;
    ensure!(
        committed.as_ref() == Some(returned),
        "returned record must equal the committed row: returned {returned:?}, committed {committed:?}"
    );
    Ok(())
}

async fn install_upstream_corruption(pool: &PgPool) -> Result<()> {
    execute(
        pool,
        "CREATE FUNCTION corrupt_upstream_base_url() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN NEW.base_url := 'not a url'; RETURN NEW; END $$",
    )
    .await?;
    execute(
        pool,
        "CREATE TRIGGER corrupt_upstream_base_url BEFORE INSERT OR UPDATE ON upstream_spec_v1
         FOR EACH ROW EXECUTE FUNCTION corrupt_upstream_base_url()",
    )
    .await?;
    execute(
        pool,
        "CREATE FUNCTION corrupt_parent_upstream() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
           UPDATE upstream_spec_v1 SET base_url = 'not a url' WHERE id = NEW.upstream_id;
           RETURN NULL;
         END $$",
    )
    .await?;
    for table in ["upstream_api_key_secret_v1", "upstream_oauth_token_v1"] {
        execute(
            pool,
            &format!(
                "CREATE TRIGGER corrupt_parent_upstream AFTER INSERT OR UPDATE ON {table}
                 FOR EACH ROW EXECUTE FUNCTION corrupt_parent_upstream()"
            ),
        )
        .await?;
    }
    Ok(())
}

async fn remove_upstream_corruption(pool: &PgPool) -> Result<()> {
    for table in ["upstream_api_key_secret_v1", "upstream_oauth_token_v1"] {
        execute(
            pool,
            &format!("DROP TRIGGER corrupt_parent_upstream ON {table}"),
        )
        .await?;
    }
    execute(pool, "DROP FUNCTION corrupt_parent_upstream()").await?;
    execute(
        pool,
        "DROP TRIGGER corrupt_upstream_base_url ON upstream_spec_v1",
    )
    .await?;
    execute(pool, "DROP FUNCTION corrupt_upstream_base_url()").await
}

async fn insert_registry_entry(pool: &PgPool, name: &str, seed: u8) -> Result<Uuid> {
    let id = Uuid::new_v4();
    let sha256 = vec![seed; 32];
    sqlx::query("INSERT INTO wasm_blobs_v2 (sha256, bytes, size_bytes) VALUES ($1, $2, 4)")
        .bind(&sha256)
        .bind(b"wasm".to_vec())
        .execute(pool)
        .await?;
    sqlx::query(
        "INSERT INTO wasm_registry_v2 (id, sha256, name, original_filename, label, uploaded_at, uploaded_by_admin_id)
         VALUES ($1, $2, $3, 'plugin.wasm', 'initial', NOW(), $4)",
    )
    .bind(id)
    .bind(&sha256)
    .bind(name)
    .bind(Uuid::new_v4())
    .execute(pool)
    .await?;
    Ok(id)
}

async fn execute(pool: &PgPool, sql: &str) -> Result<()> {
    sqlx::query(AssertSqlSafe(sql.to_owned()))
        .execute(pool)
        .await?;
    Ok(())
}

fn tokens(ciphertext: &[u8]) -> EncryptedOAuthTokens {
    EncryptedOAuthTokens::from_ciphertext(ciphertext.to_vec())
}

fn api_key_upstream(name: &str) -> UpstreamCreate {
    UpstreamCreate {
        id: Uuid::new_v4(),
        name: name.to_owned(),
        kind: UpstreamKind::AnthropicApiKey,
        base_url: Some(Url::parse("https://api.example.test/").expect("static url")),
        api_key_ciphertext: Some(b"initial".to_vec()),
        oauth_tokens: None,
        warmup_enabled: false,
        warmup_dialect_plugin: None,
        workspace_id: None,
        api_key_access: None,
    }
}

fn warmup_upstream(name: &str) -> UpstreamCreate {
    UpstreamCreate {
        warmup_enabled: true,
        warmup_dialect_plugin: Some(UpstreamWarmupDialectPlugin {
            wasm_registry_id: Uuid::new_v4(),
            config: serde_json::json!({ "dialect": "test" }),
            wire_version: None,
        }),
        ..api_key_upstream(name)
    }
}

fn oauth_upstream(name: &str) -> UpstreamCreate {
    UpstreamCreate {
        kind: UpstreamKind::AnthropicOauth,
        base_url: None,
        api_key_ciphertext: None,
        oauth_tokens: Some(OAuthTokensCreate {
            tokens: tokens(b"initial"),
            never_refresh: false,
        }),
        ..api_key_upstream(name)
    }
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
    }
}

struct Fixture {
    schema: String,
    admin_pool: PgPool,
    pool: PgPool,
    options: PgConnectOptions,
}

impl Fixture {
    async fn create(url: &str) -> Result<Self> {
        let schema = format!("admin_commit_boundary_{}", Uuid::new_v4().simple());
        let admin_pool = PgPoolOptions::new().max_connections(1).connect(url).await?;
        sqlx::query(AssertSqlSafe(format!(
            "CREATE SCHEMA {}",
            quote_ident(&schema)
        )))
        .execute(&admin_pool)
        .await?;
        // `pg_catalog` follows the test schema so a schema-local function can
        // deterministically shadow `pg_notify` for the rollback case.
        let options = PgConnectOptions::from_str(url)?
            .options([("search_path", format!("{schema},pg_catalog"))]);
        let pool = PgPoolOptions::new()
            .max_connections(4)
            .connect_with(options.clone())
            .await?;
        Ok(Self {
            schema,
            admin_pool,
            pool,
            options,
        })
    }

    /// Opens a pool with no previously prepared statements.
    async fn fresh_pool(&self) -> Result<PgPool> {
        Ok(PgPoolOptions::new()
            .max_connections(1)
            .connect_with(self.options.clone())
            .await?)
    }

    async fn drop_schema(self) -> Result<()> {
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
