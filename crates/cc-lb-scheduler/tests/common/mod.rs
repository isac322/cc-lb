#![allow(dead_code)]

#[cfg(feature = "postgres")]
pub async fn create_postgres_database(
    admin_pool: &sqlx::PgPool,
    database: &str,
    template_env: &str,
) -> Result<(), sqlx::Error> {
    if let Ok(template_base) = std::env::var(template_env)
        && !template_base.trim().is_empty()
    {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash as _, Hasher as _};

        let template_base = template_base.trim();
        let pool_size = std::env::var(format!("{template_env}_POOL_SIZE"))
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(1)
            .max(1);
        let slot = {
            let mut hasher = DefaultHasher::new();
            database.hash(&mut hasher);
            (hasher.finish() as usize) % pool_size
        };
        let template_name = if pool_size > 1 {
            format!("{template_base}_{slot}")
        } else {
            template_base.to_owned()
        };
        let lock_base = if template_base.contains("sched") {
            0x4343_4C42_5343_0000_i64
        } else {
            0x4343_4C42_0000_0000_i64
        };
        let lock_key = lock_base + i64::try_from(slot).unwrap_or(0);
        let mut conn = admin_pool.acquire().await?;
        sqlx::query("SELECT pg_advisory_lock($1)")
            .bind(lock_key)
            .execute(&mut *conn)
            .await?;
        let wal_sql = format!(
            r#"CREATE DATABASE "{database}" TEMPLATE "{template_name}" STRATEGY = WAL_LOG"#
        );
        let res = match sqlx::query(sqlx::AssertSqlSafe(wal_sql))
            .execute(&mut *conn)
            .await
        {
            Ok(_) => Ok(()),
            Err(_) => {
                let fallback_sql =
                    format!(r#"CREATE DATABASE "{database}" TEMPLATE "{template_name}""#);
                sqlx::query(sqlx::AssertSqlSafe(fallback_sql))
                    .execute(&mut *conn)
                    .await
                    .map(|_| ())
            }
        };
        let _ = sqlx::query("SELECT pg_advisory_unlock($1)")
            .bind(lock_key)
            .execute(&mut *conn)
            .await;
        res?;
        return Ok(());
    }
    sqlx::query(sqlx::AssertSqlSafe(format!(
        r#"CREATE DATABASE "{database}""#
    )))
    .execute(admin_pool)
    .await?;
    Ok(())
}
