//! Private integration-test setup, never part of an application API.
#![allow(dead_code)]

#[path = "managed_key_seed.rs"]
mod seed;

use cc_lb_control::api_keys::{key_store::CreateParams, secret};
use cc_lb_storage_api::{IssueParams, ManagedKeyStore, StoredApiKeyRecord};

pub async fn create_existing(
    storage: &cc_lb_storage_sqlite::SqliteStorage,
    principal_id: &str,
    params: CreateParams,
) -> Result<(StoredApiKeyRecord, secret::RedactedSecret), Box<dyn std::error::Error + Send + Sync>>
{
    let generated = secret::generate_new();
    let issue = IssueParams {
        label: params.label,
        description: params.description,
        expires_at_unix_secs: params.expires_at_unix_secs,
        limit_overrides: params.limit_overrides,
        secret_salt: generated.secret_salt,
        verify_hash: generated.verify_hash,
        last_4: generated.last_4,
        index_hash: generated.index_hash,
    };
    seed::seed_sqlite(
        storage.pool(),
        principal_id,
        &generated.key_id,
        &issue,
        1_700_000_000,
    )
    .await?;
    let record = ManagedKeyStore::get(storage, principal_id, &generated.key_id)
        .await?
        .ok_or("seeded managed key is missing")?;
    Ok((record, generated.plaintext))
}
