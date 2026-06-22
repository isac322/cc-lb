use sqlx::{Postgres, Row, postgres::PgRow};

use crate::{
    error::Result,
    state_stores::{
        AnthropicCompatEtag, AnthropicCompatEtagsStore, PriceCatalogVersion,
        PriceCatalogVersionsStore, i64_to_u64, u64_to_i64,
    },
};

impl AnthropicCompatEtagsStore<Postgres> {
    pub async fn read(&self, key: &str) -> Result<Option<AnthropicCompatEtag>> {
        let row = sqlx::query("SELECT * FROM anthropic_compat_etags WHERE key = $1")
            .bind(key)
            .fetch_optional(&self.pool)
            .await?;
        row.map(row_to_compat).transpose()
    }

    pub async fn upsert_value(
        &self,
        key: &str,
        etag: Option<&str>,
        hash: &str,
        now_unix_secs: u64,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO anthropic_compat_etags (key, etag, last_applied_at_unix_secs, last_value_hash) \
             VALUES ($1, $2, $3, $4) ON CONFLICT(key) DO UPDATE SET \
             etag = EXCLUDED.etag, last_applied_at_unix_secs = EXCLUDED.last_applied_at_unix_secs, \
             last_value_hash = EXCLUDED.last_value_hash",
        )
        .bind(key)
        .bind(etag)
        .bind(u64_to_i64(now_unix_secs, "last_applied_at_unix_secs")?)
        .bind(hash)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

impl PriceCatalogVersionsStore<Postgres> {
    pub async fn read(&self, source: &str) -> Result<Option<PriceCatalogVersion>> {
        let row = sqlx::query("SELECT * FROM price_catalog_versions WHERE source = $1")
            .bind(source)
            .fetch_optional(&self.pool)
            .await?;
        row.map(row_to_price).transpose()
    }

    pub async fn upsert_fingerprint(
        &self,
        source: &str,
        fingerprint: &str,
        fetched_at_unix_secs: u64,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO price_catalog_versions (source, fingerprint, fetched_at_unix_secs) \
             VALUES ($1, $2, $3) ON CONFLICT(source) DO UPDATE SET \
             fingerprint = EXCLUDED.fingerprint, fetched_at_unix_secs = EXCLUDED.fetched_at_unix_secs",
        )
        .bind(source)
        .bind(fingerprint)
        .bind(u64_to_i64(fetched_at_unix_secs, "fetched_at_unix_secs")?)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

fn row_to_compat(row: PgRow) -> Result<AnthropicCompatEtag> {
    Ok(AnthropicCompatEtag {
        key: row.try_get("key")?,
        etag: row.try_get("etag")?,
        last_applied_at_unix_secs: i64_to_u64(
            row.try_get("last_applied_at_unix_secs")?,
            "last_applied_at_unix_secs",
        )?,
        last_value_hash: row.try_get("last_value_hash")?,
    })
}

fn row_to_price(row: PgRow) -> Result<PriceCatalogVersion> {
    Ok(PriceCatalogVersion {
        source: row.try_get("source")?,
        fingerprint: row.try_get("fingerprint")?,
        fetched_at_unix_secs: i64_to_u64(
            row.try_get("fetched_at_unix_secs")?,
            "fetched_at_unix_secs",
        )?,
    })
}
