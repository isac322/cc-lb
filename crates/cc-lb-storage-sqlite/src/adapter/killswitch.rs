use cc_lb_storage_api::StorageResult;

use crate::{SqliteStorage, map_sqlx_error};

pub(crate) async fn enabled(storage: &SqliteStorage) -> StorageResult<Option<bool>> {
    let value: Option<i64> = sqlx::query_scalar("SELECT enabled FROM killswitch_v1 WHERE id = 1")
        .fetch_optional(storage.pool())
        .await
        .map_err(map_sqlx_error)?;
    Ok(value.map(|value| value != 0))
}

pub(crate) async fn set_enabled(storage: &SqliteStorage, enabled: bool) -> StorageResult<()> {
    sqlx::query(
        "INSERT INTO killswitch_v1 (id, enabled, reason) VALUES (1, ?, NULL) ON CONFLICT(id) DO UPDATE SET enabled = excluded.enabled",
    )
    .bind(enabled)
    .execute(storage.pool())
    .await
    .map_err(map_sqlx_error)?;

    Ok(())
}
