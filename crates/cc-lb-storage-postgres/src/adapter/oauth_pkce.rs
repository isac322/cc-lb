use async_trait::async_trait;
use cc_lb_clock::unix_secs;
use cc_lb_storage_api::{OAuthPkceStore, StorageResult, StoredOAuthPkceFlow};
use sqlx::{Row, postgres::PgRow};

use crate::{
    adapter::{PostgresStorage, i64_to_u64, u64_to_i64},
    error_map::map_sqlx_error,
};

#[async_trait]
impl OAuthPkceStore for PostgresStorage {
    async fn put_pkce_flow(&self, flow: &StoredOAuthPkceFlow) -> StorageResult<()> {
        let now = u64_to_i64(
            unix_secs(self.clock.now()),
            "oauth pkce flow current unix secs",
        )?;
        let mut tx = self.pool.begin().await.map_err(map_sqlx_error)?;
        sqlx::query("DELETE FROM oauth_pkce_flows_v1 WHERE expires_at_unix_secs <= $1")
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
        sqlx::query(
            "INSERT INTO oauth_pkce_flows_v1 \
             (state_token, encrypted_payload, created_at_unix_secs, expires_at_unix_secs) \
             VALUES ($1, $2, $3, $4) \
             ON CONFLICT (state_token) DO UPDATE SET \
             encrypted_payload = EXCLUDED.encrypted_payload, \
             created_at_unix_secs = EXCLUDED.created_at_unix_secs, \
             expires_at_unix_secs = EXCLUDED.expires_at_unix_secs",
        )
        .bind(&flow.state_token)
        .bind(flow.encrypted_payload.as_slice())
        .bind(u64_to_i64(
            flow.created_at_unix_secs,
            "oauth pkce flow created_at_unix_secs",
        )?)
        .bind(u64_to_i64(
            flow.expires_at_unix_secs,
            "oauth pkce flow expires_at_unix_secs",
        )?)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;
        tx.commit().await.map_err(map_sqlx_error)
    }

    async fn get_pkce_flow(
        &self,
        state_token: &str,
        now_unix_secs: u64,
    ) -> StorageResult<Option<StoredOAuthPkceFlow>> {
        let row = sqlx::query(
            "SELECT state_token, encrypted_payload, created_at_unix_secs, expires_at_unix_secs \
             FROM oauth_pkce_flows_v1 WHERE state_token = $1",
        )
        .bind(state_token)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx_error)?;
        let Some(row) = row else {
            return Ok(None);
        };
        let flow = row_to_flow(row)?;
        if flow.expires_at_unix_secs <= now_unix_secs {
            sqlx::query("DELETE FROM oauth_pkce_flows_v1 WHERE state_token = $1")
                .bind(state_token)
                .execute(&self.pool)
                .await
                .map_err(map_sqlx_error)?;
            return Ok(None);
        }
        Ok(Some(flow))
    }

    async fn delete_pkce_flow(&self, state_token: &str) -> StorageResult<()> {
        sqlx::query("DELETE FROM oauth_pkce_flows_v1 WHERE state_token = $1")
            .bind(state_token)
            .execute(&self.pool)
            .await
            .map_err(map_sqlx_error)?;
        Ok(())
    }
}

fn row_to_flow(row: PgRow) -> StorageResult<StoredOAuthPkceFlow> {
    Ok(StoredOAuthPkceFlow {
        state_token: row.try_get("state_token").map_err(map_sqlx_error)?,
        encrypted_payload: row.try_get("encrypted_payload").map_err(map_sqlx_error)?,
        created_at_unix_secs: i64_to_u64(
            row.try_get("created_at_unix_secs")
                .map_err(map_sqlx_error)?,
            "oauth pkce flow created_at_unix_secs",
        )?,
        expires_at_unix_secs: i64_to_u64(
            row.try_get("expires_at_unix_secs")
                .map_err(map_sqlx_error)?,
            "oauth pkce flow expires_at_unix_secs",
        )?,
    })
}
