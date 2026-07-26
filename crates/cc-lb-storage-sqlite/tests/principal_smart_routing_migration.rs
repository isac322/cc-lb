//! Migration `0072_principals_smart_routing` has to preserve today's behaviour
//! exactly:
//!
//! * a principal with no built-in `subscription-preference` router entry was
//!   not running the filter, so it must end up opted **out**;
//! * a principal whose chain **leads** with the built-in entry is reproduced by
//!   the flag, so the now-redundant entry is retired;
//! * a mid-chain entry encodes a deliberate position among other router
//!   plugins, which the flag cannot express — deleting it would silently move
//!   the filter to the head of the chain, so it must survive.
//!
//! The shipped migration text runs verbatim against a minimal fixture schema,
//! so `ALTER`, `UPDATE`, and `DELETE` are all exercised in file order.

use sqlx::SqlitePool;
use uuid::Uuid;

const MIGRATION: &str = include_str!("../migrations/0072_principals_smart_routing.sql");
const BUILTIN_ID: &str = "00000000-0000-0000-0000-000000000002";
const CUSTOM_ID: &str = "11111111-1111-1111-1111-111111111111";

/// Only the columns the migration reads or writes. A wider fixture would drift
/// from the real schema without adding coverage.
const FIXTURE_SCHEMA: &str = "
CREATE TABLE principals_v1 (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE plugin_chains_v2 (
    id TEXT PRIMARY KEY,
    principal_id TEXT NOT NULL,
    slot TEXT NOT NULL,
    wasm_registry_id BLOB NOT NULL,
    order_value INTEGER NOT NULL
);
";

async fn seed_principal(pool: &SqlitePool, id: Uuid, name: &str) {
    sqlx::query("INSERT INTO principals_v1 (id, name) VALUES (?, ?)")
        .bind(id.to_string())
        .bind(name)
        .execute(pool)
        .await
        .expect("seed principal");
}

async fn seed_chain_entry(
    pool: &SqlitePool,
    principal: Uuid,
    registry_id: &str,
    order_value: i64,
) -> String {
    let id = Uuid::new_v4().to_string();
    sqlx::query(
        "INSERT INTO plugin_chains_v2 (id, principal_id, slot, wasm_registry_id, order_value)
         VALUES (?, ?, 'router', ?, ?)",
    )
    .bind(&id)
    .bind(principal.to_string())
    .bind(registry_id)
    .bind(order_value)
    .execute(pool)
    .await
    .expect("seed chain entry");
    id
}

async fn entry_exists(pool: &SqlitePool, id: &str) -> bool {
    sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM plugin_chains_v2 WHERE id = ?")
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("count chain entry")
        == 1
}

async fn smart_routing_enabled(pool: &SqlitePool, principal: Uuid) -> i64 {
    sqlx::query_scalar("SELECT smart_routing_enabled FROM principals_v1 WHERE id = ?")
        .bind(principal.to_string())
        .fetch_one(pool)
        .await
        .expect("read smart_routing_enabled")
}

#[tokio::test]
async fn migration_preserves_current_smart_routing_behaviour() {
    let pool = SqlitePool::connect("sqlite::memory:")
        .await
        .expect("open in-memory sqlite");
    sqlx::raw_sql(FIXTURE_SCHEMA)
        .execute(&pool)
        .await
        .expect("create fixture schema");

    let no_chain = Uuid::from_u128(0x1000);
    let builtin_only = Uuid::from_u128(0x1001);
    let builtin_leads = Uuid::from_u128(0x1002);
    let builtin_mid_chain = Uuid::from_u128(0x1003);
    let order_tie = Uuid::from_u128(0x1004);
    let custom_only = Uuid::from_u128(0x1005);

    seed_principal(&pool, no_chain, "no-chain").await;
    seed_principal(&pool, builtin_only, "builtin-only").await;
    seed_principal(&pool, builtin_leads, "builtin-leads").await;
    seed_principal(&pool, builtin_mid_chain, "builtin-mid-chain").await;
    seed_principal(&pool, order_tie, "order-tie").await;
    seed_principal(&pool, custom_only, "custom-only").await;

    let solo = seed_chain_entry(&pool, builtin_only, BUILTIN_ID, 100).await;

    let leading = seed_chain_entry(&pool, builtin_leads, BUILTIN_ID, 100).await;
    let trailing_custom = seed_chain_entry(&pool, builtin_leads, CUSTOM_ID, 200).await;

    let leading_custom = seed_chain_entry(&pool, builtin_mid_chain, CUSTOM_ID, 100).await;
    let mid_chain = seed_chain_entry(&pool, builtin_mid_chain, BUILTIN_ID, 200).await;

    let tied_custom = seed_chain_entry(&pool, order_tie, CUSTOM_ID, 100).await;
    let tied_builtin = seed_chain_entry(&pool, order_tie, BUILTIN_ID, 100).await;

    let unrelated_custom = seed_chain_entry(&pool, custom_only, CUSTOM_ID, 100).await;

    sqlx::raw_sql(MIGRATION)
        .execute(&pool)
        .await
        .expect("apply migration 0072");

    // Principals that were not running the filter must stay opted out. This is
    // the fleet-wide branch: nearly every principal has no built-in entry.
    assert_eq!(
        smart_routing_enabled(&pool, no_chain).await,
        0,
        "a principal with no router chain was not running the filter"
    );
    assert_eq!(
        smart_routing_enabled(&pool, custom_only).await,
        0,
        "a router chain without the built-in entry was not running the filter"
    );

    for principal in [builtin_only, builtin_leads, builtin_mid_chain, order_tie] {
        assert_eq!(
            smart_routing_enabled(&pool, principal).await,
            1,
            "a principal with the built-in entry keeps the filter"
        );
    }

    assert!(
        !entry_exists(&pool, &solo).await,
        "a builtin-only chain is fully reproduced by the flag"
    );
    assert!(
        !entry_exists(&pool, &leading).await,
        "a leading builtin entry is reproduced by the flag"
    );
    assert!(
        entry_exists(&pool, &mid_chain).await,
        "a mid-chain builtin entry pins an order the flag cannot express"
    );
    assert!(
        entry_exists(&pool, &tied_builtin).await,
        "an order tie is ambiguous, so the entry is left alone"
    );

    for custom in [
        &trailing_custom,
        &leading_custom,
        &tied_custom,
        &unrelated_custom,
    ] {
        assert!(
            entry_exists(&pool, custom).await,
            "user-uploaded router filters are never touched"
        );
    }
}
