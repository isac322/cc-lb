//! Database migrations for the scheduler tables.

use sqlx::{Database, Executor, Pool};

use crate::error::Result;

#[doc(hidden)]
pub trait ApalisPostSetupMigrationDatabase: Database {
    const POST_SETUP_MIGRATION_SQL: &'static str;
}

#[cfg(feature = "sqlite")]
impl ApalisPostSetupMigrationDatabase for sqlx::Sqlite {
    const POST_SETUP_MIGRATION_SQL: &'static str =
        include_str!("../migrations/sqlite/0001_apalis_partial_unique.sql");
}

#[cfg(feature = "postgres")]
impl ApalisPostSetupMigrationDatabase for sqlx::Postgres {
    const POST_SETUP_MIGRATION_SQL: &'static str =
        include_str!("../migrations/postgres/0001_apalis_partial_unique.sql");
}

pub async fn apply_post_setup_migrations<Db>(pool: &Pool<Db>) -> Result<()>
where
    Db: ApalisPostSetupMigrationDatabase,
    for<'executor> &'executor Pool<Db>: Executor<'executor, Database = Db>,
{
    sqlx::raw_sql(Db::POST_SETUP_MIGRATION_SQL)
        .execute(pool)
        .await?;
    Ok(())
}
