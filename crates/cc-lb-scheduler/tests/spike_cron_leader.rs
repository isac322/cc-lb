use std::time::Duration;

use apalis_cron::CronStream;
use cron::Schedule;
use futures_util::StreamExt as _;
use sqlx::Connection as _;
use sqlx::PgPool;
use sqlx::postgres::PgConnection;
use std::str::FromStr;
use tokio::sync::oneshot;

const LOCK_KEY_B: i64 = 0x0000_CC1B_5CDE_0001_u64 as i64;
const LOCK_KEY_C: i64 = 0x0000_CC1B_5CDE_0002_u64 as i64;

struct SpikeTable {
    drop_schema: &'static str,
    create_schema: &'static str,
    create_table: &'static str,
    insert_tick: &'static str,
    count_ticks: &'static str,
    select_standby_replica: &'static str,
}

const TABLE_A: SpikeTable = SpikeTable {
    drop_schema: "DROP SCHEMA IF EXISTS apalis_spike_cron_a CASCADE",
    create_schema: "CREATE SCHEMA apalis_spike_cron_a",
    create_table: "CREATE TABLE apalis_spike_cron_a.ticks (id TEXT PRIMARY KEY, replica TEXT NOT NULL, ticked_at TIMESTAMPTZ NOT NULL DEFAULT now())",
    insert_tick: "INSERT INTO apalis_spike_cron_a.ticks (id, replica) VALUES ($1, $2)",
    count_ticks: "SELECT COUNT(*) FROM apalis_spike_cron_a.ticks",
    select_standby_replica: "SELECT replica FROM apalis_spike_cron_a.ticks WHERE id = 'tick-standby-leader'",
};

const TABLE_B: SpikeTable = SpikeTable {
    drop_schema: "DROP SCHEMA IF EXISTS apalis_spike_cron_b CASCADE",
    create_schema: "CREATE SCHEMA apalis_spike_cron_b",
    create_table: "CREATE TABLE apalis_spike_cron_b.ticks (id TEXT PRIMARY KEY, replica TEXT NOT NULL, ticked_at TIMESTAMPTZ NOT NULL DEFAULT now())",
    insert_tick: "INSERT INTO apalis_spike_cron_b.ticks (id, replica) VALUES ($1, $2)",
    count_ticks: "SELECT COUNT(*) FROM apalis_spike_cron_b.ticks",
    select_standby_replica: "SELECT replica FROM apalis_spike_cron_b.ticks WHERE id = 'tick-standby-leader'",
};

const TABLE_C: SpikeTable = SpikeTable {
    drop_schema: "DROP SCHEMA IF EXISTS apalis_spike_cron_c CASCADE",
    create_schema: "CREATE SCHEMA apalis_spike_cron_c",
    create_table: "CREATE TABLE apalis_spike_cron_c.ticks (id TEXT PRIMARY KEY, replica TEXT NOT NULL, ticked_at TIMESTAMPTZ NOT NULL DEFAULT now())",
    insert_tick: "INSERT INTO apalis_spike_cron_c.ticks (id, replica) VALUES ($1, $2)",
    count_ticks: "SELECT COUNT(*) FROM apalis_spike_cron_c.ticks",
    select_standby_replica: "SELECT replica FROM apalis_spike_cron_c.ticks WHERE id = 'tick-standby-leader'",
};

async fn connect_pool() -> Option<(PgPool, String)> {
    let url = std::env::var("DATABASE_URL").ok()?;
    let pool = PgPool::connect(&url).await.expect("PgPool::connect failed");
    Some((pool, url))
}

async fn connect_dedicated(url: &str) -> PgConnection {
    PgConnection::connect(url)
        .await
        .expect("PgConnection::connect failed")
}

async fn setup(pool: &PgPool, table: &SpikeTable) {
    sqlx::query(table.drop_schema).execute(pool).await.unwrap();
    sqlx::query(table.create_schema)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query(table.create_table).execute(pool).await.unwrap();
}

async fn teardown(pool: &PgPool, table: &SpikeTable) {
    sqlx::query(table.drop_schema).execute(pool).await.unwrap();
}

async fn insert_tick(pool: &PgPool, table: &SpikeTable, id: &str, replica: &str) {
    sqlx::query(table.insert_tick)
        .bind(id)
        .bind(replica)
        .execute(pool)
        .await
        .unwrap();
}

async fn count_ticks(pool: &PgPool, table: &SpikeTable) -> i64 {
    sqlx::query_scalar(table.count_ticks)
        .fetch_one(pool)
        .await
        .unwrap()
}

#[tokio::test]
async fn a_no_leader_election_produces_duplicate_ticks() {
    let Some((pool, _url)) = connect_pool().await else {
        eprintln!("SKIP: DATABASE_URL not set; skipping spike A");
        return;
    };
    setup(&pool, &TABLE_A).await;

    let schedule = Schedule::from_str("1/1 * * * * *").unwrap();
    let mut replica1_cron = CronStream::new(schedule.clone());
    let mut replica2_cron = CronStream::new(schedule);

    let replica1_tick = async {
        tokio::time::timeout(Duration::from_secs(2), replica1_cron.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
    };
    let replica2_tick = async {
        tokio::time::timeout(Duration::from_secs(2), replica2_cron.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
    };
    let (tick1, tick2) = tokio::join!(replica1_tick, replica2_tick);

    insert_tick(&pool, &TABLE_A, "tick-replica1-00000001", "replica-1").await;
    insert_tick(&pool, &TABLE_A, "tick-replica2-00000001", "replica-2").await;

    let count = count_ticks(&pool, &TABLE_A).await;
    assert_eq!(count, 2);
    eprintln!(
        "Scenario A: {count} ticks for apalis-cron slots {:?} and {:?}; duplicates confirmed",
        tick1.get_timestamp(),
        tick2.get_timestamp()
    );

    teardown(&pool, &TABLE_A).await;
}

#[tokio::test]
async fn b_advisory_lock_ensures_single_leader() {
    let Some((pool, url)) = connect_pool().await else {
        eprintln!("SKIP: DATABASE_URL not set; skipping spike B");
        return;
    };
    setup(&pool, &TABLE_B).await;

    let mut conn1 = connect_dedicated(&url).await;
    let mut conn2 = connect_dedicated(&url).await;

    let acquired1: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
        .bind(LOCK_KEY_B)
        .fetch_one(&mut conn1)
        .await
        .unwrap();
    let acquired2: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
        .bind(LOCK_KEY_B)
        .fetch_one(&mut conn2)
        .await
        .unwrap();

    assert!(acquired1);
    assert!(!acquired2);
    eprintln!("Scenario B: replica-1 acquired={acquired1}, replica-2 acquired={acquired2}");

    if acquired1 {
        insert_tick(&pool, &TABLE_B, "tick-leader-0001", "replica-1-leader").await;
    }
    if acquired2 {
        insert_tick(&pool, &TABLE_B, "tick-leader-0002", "replica-2-standby").await;
    }

    let count = count_ticks(&pool, &TABLE_B).await;
    assert_eq!(count, 1);
    eprintln!("Scenario B: {count} tick recorded; leader only");

    conn1.close().await.unwrap();
    conn2.close().await.unwrap();
    teardown(&pool, &TABLE_B).await;
}

#[tokio::test]
async fn c_leader_drop_releases_lock_standby_acquires() {
    let Some((pool, url)) = connect_pool().await else {
        eprintln!("SKIP: DATABASE_URL not set; skipping spike C");
        return;
    };
    setup(&pool, &TABLE_C).await;

    let mut leader_conn = connect_dedicated(&url).await;
    let leader_acquired: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
        .bind(LOCK_KEY_C)
        .fetch_one(&mut leader_conn)
        .await
        .unwrap();
    assert!(leader_acquired);
    eprintln!("Scenario C: leader acquired lock {LOCK_KEY_C:#018x}");

    let (ready_tx, ready_rx) = oneshot::channel::<()>();
    let url_clone = url.clone();
    let pool_clone = pool.clone();

    let standby = tokio::spawn(async move {
        let mut standby_conn = connect_dedicated(&url_clone).await;
        ready_tx.send(()).expect("ready channel send failed");

        sqlx::query("SELECT pg_advisory_lock($1)")
            .bind(LOCK_KEY_C)
            .execute(&mut standby_conn)
            .await
            .expect("standby pg_advisory_lock failed");

        insert_tick(
            &pool_clone,
            &TABLE_C,
            "tick-standby-leader",
            "replica-2-new-leader",
        )
        .await;

        standby_conn.close().await.unwrap();
    });

    ready_rx.await.expect("ready signal lost");
    tokio::time::sleep(Duration::from_millis(100)).await;
    leader_conn.close().await.unwrap();

    tokio::time::timeout(Duration::from_secs(5), standby)
        .await
        .expect("standby did not acquire lock within 5 s")
        .expect("standby task panicked");

    let count = count_ticks(&pool, &TABLE_C).await;
    assert_eq!(count, 1);

    let replica: String = sqlx::query_scalar(TABLE_C.select_standby_replica)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(replica, "replica-2-new-leader");
    eprintln!("Scenario C: standby became leader and recorded one tick");

    teardown(&pool, &TABLE_C).await;
}
