use async_trait::async_trait;
use cc_lb_storage_api::{
    LimitStateStore, PrincipalLimitIdentityKind, PrincipalLimitKind, PrincipalLimitState,
    StorageResult,
};
use serde_json::Value;

use crate::{adapter::PostgresStorage, error_map::map_sqlx_error};

#[async_trait]
impl LimitStateStore for PostgresStorage {
    async fn put_principal_limit_state(&self, state: &PrincipalLimitState) -> StorageResult<()> {
        let snapshot = serde_json::to_value(state)?;
        sqlx::query(
            "INSERT INTO principal_limit_states_v1              (principal_id, identity_kind, identity_value, window, kind, snapshot, updated_at)              VALUES ($1,$2,$3,$4,$5,$6,NOW())              ON CONFLICT (principal_id, identity_kind, identity_value, window, kind)              DO UPDATE SET snapshot = EXCLUDED.snapshot, updated_at = NOW()",
        )
        .bind(&state.principal_id)
        .bind(state.identity_kind.as_str())
        .bind(identity_value_key(
            state.identity_kind,
            state.identity_value.as_deref(),
        ))
        .bind(&state.window)
        .bind(state.kind.as_str())
        .bind(snapshot)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx_error)?;

        Ok(())
    }

    async fn get_principal_limit_state(
        &self,
        principal_id: &str,
        identity_kind: PrincipalLimitIdentityKind,
        identity_value: Option<&str>,
        window: &str,
        kind: PrincipalLimitKind,
    ) -> StorageResult<Option<PrincipalLimitState>> {
        let snapshot = sqlx::query_scalar::<_, Value>(
            "SELECT snapshot FROM principal_limit_states_v1              WHERE principal_id = $1 AND identity_kind = $2 AND identity_value = $3              AND window = $4 AND kind = $5",
        )
        .bind(principal_id)
        .bind(identity_kind.as_str())
        .bind(identity_value_key(identity_kind, identity_value))
        .bind(window)
        .bind(kind.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx_error)?;

        snapshot
            .map(serde_json::from_value)
            .transpose()
            .map_err(Into::into)
    }

    async fn list_principal_limit_states(
        &self,
        principal_id: &str,
    ) -> StorageResult<Vec<PrincipalLimitState>> {
        let rows = sqlx::query_scalar::<_, Value>(
            "SELECT snapshot FROM principal_limit_states_v1              WHERE principal_id = $1 ORDER BY updated_at ASC",
        )
        .bind(principal_id)
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx_error)?;

        rows.into_iter()
            .map(|snapshot| serde_json::from_value(snapshot).map_err(Into::into))
            .collect()
    }
}

fn identity_value_key(kind: PrincipalLimitIdentityKind, value: Option<&str>) -> String {
    value
        .filter(|_| kind != PrincipalLimitIdentityKind::Unobserved)
        .unwrap_or("")
        .to_owned()
}
