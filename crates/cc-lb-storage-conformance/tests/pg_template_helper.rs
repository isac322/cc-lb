#![allow(dead_code)]

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::str::FromStr;

use anyhow::Result;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{AssertSqlSafe, PgPool};
use uuid::Uuid;

enum Backend {
    Database { db: String },
    Schema { schema: String, admin_pool: PgPool },
}

pub struct IsolatedPg {
    backend: Backend,
    admin_url: String,
    base_options: PgConnectOptions,
    schema_name: String,
}

impl IsolatedPg {
    pub async fn create_templated(url: &str) -> Result<Self> {
        let Some(template_base) = std::env::var("CC_LB_PG_APP_TEMPLATE")
            .ok()
            .map(|s| s.trim().to_owned())
            .filter(|s| !s.is_empty())
        else {
            return Self::create_schema(url).await;
        };

        let db = format!("cc_lb_c_{}", Uuid::new_v4().simple());
        let pool_size = std::env::var("CC_LB_PG_APP_TEMPLATE_POOL_SIZE")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(1)
            .max(1);
        let slot = slot_for(&db, pool_size);
        let template_name = if pool_size > 1 {
            format!("{template_base}_{slot}")
        } else {
            template_base
        };

        let base_url_opts = PgConnectOptions::from_str(url)?;
        let admin_opts = base_url_opts.clone().database("postgres");
        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(admin_opts)
            .await?;

        let lock_key = 0x4343_4c42_0000_0000_i64 + i64::try_from(slot).unwrap_or(0);
        let mut conn = admin_pool.acquire().await?;
        sqlx::query("SELECT pg_advisory_lock($1)")
            .bind(lock_key)
            .execute(&mut *conn)
            .await?;

        let wal_sql = format!(
            "CREATE DATABASE {} TEMPLATE {} STRATEGY = WAL_LOG",
            quote_ident(&db),
            quote_ident(&template_name)
        );
        let create_res = match sqlx::query(AssertSqlSafe(wal_sql))
            .execute(&mut *conn)
            .await
        {
            Ok(_) => Ok(()),
            Err(_) => {
                let fallback_sql = format!(
                    "CREATE DATABASE {} TEMPLATE {}",
                    quote_ident(&db),
                    quote_ident(&template_name)
                );
                sqlx::query(AssertSqlSafe(fallback_sql))
                    .execute(&mut *conn)
                    .await
                    .map(|_| ())
            }
        };

        let _ = sqlx::query("SELECT pg_advisory_unlock($1)")
            .bind(lock_key)
            .execute(&mut *conn)
            .await;
        drop(conn);
        admin_pool.close().await;
        create_res?;

        Ok(Self {
            backend: Backend::Database { db: db.clone() },
            admin_url: url.to_owned(),
            base_options: base_url_opts.database(&db),
            schema_name: "public".to_owned(),
        })
    }

    async fn create_schema(url: &str) -> Result<Self> {
        let schema = format!("cc_lb_cs_{}", Uuid::new_v4().simple());
        let base_url_opts = PgConnectOptions::from_str(url)?;
        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(base_url_opts.clone())
            .await?;
        sqlx::query(AssertSqlSafe(format!(
            "CREATE SCHEMA {}",
            quote_ident(&schema)
        )))
        .execute(&admin_pool)
        .await?;
        Ok(Self {
            backend: Backend::Schema {
                schema: schema.clone(),
                admin_pool,
            },
            admin_url: url.to_owned(),
            base_options: base_url_opts,
            schema_name: schema,
        })
    }

    pub fn connect_options(&self) -> PgConnectOptions {
        let mut opts = self.base_options.clone();
        if let Backend::Schema { .. } = &self.backend {
            let search_path = format!("{}, public", self.schema_name);
            opts = opts.options([("search_path", search_path)]);
        }
        opts
    }

    pub async fn pool(&self, max_connections: u32) -> Result<PgPool> {
        Ok(PgPoolOptions::new()
            .max_connections(max_connections)
            .connect_with(self.connect_options())
            .await?)
    }

    pub async fn drop(self) -> Result<()> {
        match self.backend {
            Backend::Database { db } => {
                let admin_opts = PgConnectOptions::from_str(&self.admin_url)?.database("postgres");
                let admin_pool = PgPoolOptions::new()
                    .max_connections(1)
                    .connect_with(admin_opts)
                    .await?;
                let force_sql =
                    format!("DROP DATABASE IF EXISTS {} WITH (FORCE)", quote_ident(&db));
                if sqlx::query(AssertSqlSafe(force_sql))
                    .execute(&admin_pool)
                    .await
                    .is_err()
                {
                    let _ = sqlx::query(
                        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()",
                    )
                    .bind(&db)
                    .execute(&admin_pool)
                    .await;
                    let drop_sql = format!("DROP DATABASE IF EXISTS {}", quote_ident(&db));
                    sqlx::query(AssertSqlSafe(drop_sql))
                        .execute(&admin_pool)
                        .await?;
                }
                admin_pool.close().await;
            }
            Backend::Schema { schema, admin_pool } => {
                sqlx::query(AssertSqlSafe(format!(
                    "DROP SCHEMA IF EXISTS {} CASCADE",
                    quote_ident(&schema)
                )))
                .execute(&admin_pool)
                .await?;
                admin_pool.close().await;
            }
        }
        Ok(())
    }
}

fn slot_for(db: &str, pool_size: usize) -> usize {
    let mut hasher = DefaultHasher::new();
    db.hash(&mut hasher);
    (hasher.finish() as usize) % pool_size.max(1)
}

pub fn quote_ident(identifier: &str) -> String {
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
