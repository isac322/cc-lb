//! Test-only setup for keys issued before the compatibility release.
//! Production issuance remains paused; existing-key behavior uses real storage.
#![allow(dead_code)]

use cc_lb_storage_api::types::IssueParams;
use sqlx::{QueryBuilder, Sqlite, SqlitePool};

pub async fn seed_sqlite(
    pool: &SqlitePool,
    principal_id: &str,
    key_id: &str,
    params: &IssueParams,
    issued_at: i64,
) -> Result<(), sqlx::Error> {
    let legacy: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM pragma_table_info('managed_keys_v1') WHERE name = 'upstream_kind')",
    )
    .fetch_one(pool)
    .await?;
    let id = format!("{principal_id}:{key_id}");
    let hash = hash_text(&params.verify_hash);
    let limits = serde_json::to_string(&params.limit_overrides)
        .map_err(|error| sqlx::Error::Encode(Box::new(error)))?;
    let expires = params
        .expires_at_unix_secs
        .map(i64::try_from)
        .transpose()
        .map_err(|error| sqlx::Error::Encode(Box::new(error)))?;
    let mut query = QueryBuilder::<Sqlite>::new(
        "INSERT INTO managed_keys_v1 (id,name,secret_hash,created_at,expires_at,status,principal_id,key_id,label,revoked_at,verify_hash,secret_salt,limit_overrides,last_4,description,index_hash,updated_at",
    );
    if legacy {
        query.push(",upstream_kind,principal_kind");
    }
    query.push(") VALUES (");
    {
        let mut values = query.separated(",");
        values
            .push_bind(&id)
            .push_bind(&id)
            .push_bind(&hash)
            .push_bind(issued_at)
            .push_bind(expires)
            .push_bind("active")
            .push_bind(principal_id)
            .push_bind(key_id)
            .push_bind(&params.label)
            .push_bind(Option::<i64>::None)
            .push_bind(params.verify_hash.as_slice())
            .push_bind(params.secret_salt.as_slice())
            .push_bind(limits)
            .push_bind(&params.last_4)
            .push_bind(&params.description)
            .push_bind(params.index_hash.as_slice())
            .push_bind(issued_at);
        if legacy {
            values.push_bind("anthropic_key").push_bind("machine");
        }
    }
    query.push(")").build().execute(pool).await?;
    Ok(())
}

pub fn hash_text(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut output = String::new();
    let mut accumulator = 0_u32;
    let mut bits = 0_u32;
    for byte in bytes {
        accumulator = (accumulator << 8) | u32::from(*byte);
        bits += 8;
        while bits >= 6 {
            bits -= 6;
            output.push(ALPHABET[((accumulator >> bits) & 63) as usize] as char);
        }
    }
    if bits != 0 {
        output.push(ALPHABET[((accumulator << (6 - bits)) & 63) as usize] as char);
    }
    output
}
