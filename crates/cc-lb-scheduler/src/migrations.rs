//! Database migrations for the scheduler tables.

use sqlx::{Database, Executor, Pool};

use crate::error::Result;

#[doc(hidden)]
pub trait ApalisPostSetupMigrationDatabase: Database {
    const POST_SETUP_MIGRATION_SQL: &'static [&'static str];
}

#[cfg(feature = "sqlite")]
impl ApalisPostSetupMigrationDatabase for sqlx::Sqlite {
    const POST_SETUP_MIGRATION_SQL: &'static [&'static str] = &[
        include_str!("../migrations/sqlite/0001_apalis_partial_unique.sql"),
        include_str!("../migrations/sqlite/0002_idempotency_tables.sql"),
    ];
}

#[cfg(feature = "postgres")]
impl ApalisPostSetupMigrationDatabase for sqlx::Postgres {
    const POST_SETUP_MIGRATION_SQL: &'static [&'static str] = &[
        include_str!("../migrations/postgres/0001_apalis_partial_unique.sql"),
        include_str!("../migrations/postgres/0002_idempotency_tables.sql"),
    ];
}

pub async fn apply_post_setup_migrations<Db>(pool: &Pool<Db>) -> Result<()>
where
    Db: ApalisPostSetupMigrationDatabase,
    for<'executor> &'executor Pool<Db>: Executor<'executor, Database = Db>,
{
    for sql in Db::POST_SETUP_MIGRATION_SQL {
        sqlx::raw_sql(sql).execute(pool).await?;
    }
    Ok(())
}
