use std::sync::Arc;

use cc_lb_domain::TerminalStrategy;
use cc_lb_storage_api::{MetaStore, PrincipalStore};
use uuid::Uuid;

#[tokio::test]
async fn migration_normalizes_legacy_removed_terminal_strategies_to_first_pick() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let database_url = format!(
        "sqlite://{}",
        temp_dir
            .path()
            .join("principal-terminal-strategy.sqlite")
            .display()
    );
    let storage =
        cc_lb_storage_sqlite::open_sqlite(&database_url, Arc::new(cc_lb_clock::SystemClock))
            .await
            .expect("open sqlite");
    let round_robin_id = Uuid::from_u128(1);
    let least_connections_id = Uuid::from_u128(2);

    sqlx::query(
        "CREATE TABLE principals_v1 (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            kind TEXT NOT NULL DEFAULT 'machine' CHECK (kind IN ('machine','human','admin')),
            enabled INTEGER NOT NULL DEFAULT 1,
            allowed_models TEXT NOT NULL DEFAULT '[]',
            allowed_upstreams TEXT NOT NULL DEFAULT '[]',
            default_limits TEXT NOT NULL DEFAULT '[]',
            router_terminal_strategy TEXT NOT NULL DEFAULT 'first-pick' CHECK (router_terminal_strategy IN ('first-pick','random','round-robin','least-connections')),
            revision INTEGER NOT NULL DEFAULT 0 CHECK (revision >= 0),
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            last_apply_error TEXT,
            last_apply_at INTEGER,
            deleted_at INTEGER
        )",
    )
    .execute(storage.pool())
    .await
    .expect("create legacy principals table");

    sqlx::query(
        "INSERT INTO principals_v1 (id, name, kind, router_terminal_strategy, created_at, updated_at)
         VALUES (?, 'legacy-round-robin', 'machine', 'round-robin', 1, 1),
                (?, 'legacy-least-connections', 'machine', 'least-connections', 1, 1)",
    )
    .bind(round_robin_id.to_string())
    .bind(least_connections_id.to_string())
    .execute(storage.pool())
    .await
    .expect("seed legacy principal rows");

    storage.initialize().await.expect("run sqlite migrations");

    let round_robin_db_value: String =
        sqlx::query_scalar("SELECT router_terminal_strategy FROM principals_v1 WHERE id = ?")
            .bind(round_robin_id.to_string())
            .fetch_one(storage.pool())
            .await
            .expect("read migrated round-robin row");
    assert_eq!(round_robin_db_value, "first-pick");

    let least_connections_db_value: String =
        sqlx::query_scalar("SELECT router_terminal_strategy FROM principals_v1 WHERE id = ?")
            .bind(least_connections_id.to_string())
            .fetch_one(storage.pool())
            .await
            .expect("read migrated least-connections row");
    assert_eq!(least_connections_db_value, "first-pick");

    let round_robin_record = PrincipalStore::get_by_id(&storage, round_robin_id)
        .await
        .expect("read round-robin record")
        .expect("round-robin record exists");
    assert_eq!(
        round_robin_record.router_terminal_strategy,
        TerminalStrategy::FirstPick
    );
}
