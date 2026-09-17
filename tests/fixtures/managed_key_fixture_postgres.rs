//! Existing-key fixture for the bridge's migration-114 PostgreSQL tests.
use base64::Engine as _;
use sqlx::postgres::PgPoolOptions;

pub async fn seed_existing(
    database_url: &str,
    principal_id: &str,
    label: &str,
) -> Result<(String, String), Box<dyn std::error::Error + Send + Sync>> {
    let generated = cc_lb_control::api_keys::secret::generate_new();
    let hash = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(generated.verify_hash);
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(database_url)
        .await?;
    let mut tx = pool.begin().await?;
    sqlx::query(
        "INSERT INTO managed_api_keys_v1
         (principal_id,key_id,label,issued_at_unix_secs,revoked_at_unix_secs,key_hash_b64,
          verify_hash,secret_salt,upstream_kind,limit_overrides,status,expires_at_unix_secs,
          last_4,description,principal_kind,index_hash,created_at,updated_at)
         VALUES ($1,$2,$3,1700000000,NULL,$4,$5,$6,'anthropic_key','[]'::jsonb,'active',NULL,
                 $7,NULL,'machine',$8,NOW(),NOW())",
    )
    .bind(principal_id)
    .bind(&generated.key_id)
    .bind(label)
    .bind(hash)
    .bind(generated.verify_hash.as_slice())
    .bind(generated.secret_salt.as_slice())
    .bind(&generated.last_4)
    .bind(generated.index_hash.as_slice())
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "INSERT INTO managed_api_key_index_v1(index_hash,principal_id,key_id) VALUES ($1,$2,$3)",
    )
    .bind(generated.index_hash.as_slice())
    .bind(principal_id)
    .bind(&generated.key_id)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    pool.close().await;
    Ok((generated.key_id, generated.plaintext.expose().to_owned()))
}
