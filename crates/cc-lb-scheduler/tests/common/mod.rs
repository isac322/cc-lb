#![allow(dead_code)]

#[cfg(feature = "postgres")]
pub async fn create_postgres_database(
    admin_pool: &sqlx::PgPool,
    database: &str,
    template_env: &str,
) -> Result<(), sqlx::Error> {
    const TEMPLATE_CLONE_LOCK: i64 = 0x4343_4C42_5047_544D;
    if let Ok(template) = std::env::var(template_env)
        && !template.trim().is_empty()
    {
        let mut conn = admin_pool.acquire().await?;
        sqlx::query("SELECT pg_advisory_lock($1)")
            .bind(TEMPLATE_CLONE_LOCK)
            .execute(&mut *conn)
            .await?;
        let res = sqlx::query(sqlx::AssertSqlSafe(format!(
            r#"CREATE DATABASE "{database}" TEMPLATE "{template}" STRATEGY = WAL_LOG"#
        )))
        .execute(&mut *conn)
        .await;
        let _ = sqlx::query("SELECT pg_advisory_unlock($1)")
            .bind(TEMPLATE_CLONE_LOCK)
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
