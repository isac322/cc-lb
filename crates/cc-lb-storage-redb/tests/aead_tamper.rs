use cc_lb_storage_redb::{
    oauth_key, OAuthCredentials, Storage, StorageError, OAUTH_CREDENTIALS_V1,
};
use redb::ReadableTable;

#[test]
fn tampered_oauth_ciphertext_returns_aead_error() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("storage.redb");
    let creds = OAuthCredentials {
        access_token: "access-token".to_owned(),
        refresh_token: "refresh-token".to_owned(),
        expires_at: 1_765_000_000,
        scopes: vec!["messages".to_owned()],
    };

    let storage = Storage::open(&path, [9; 32])?;
    storage.put_oauth("alice", "anthropic_oauth", &creds)?;
    drop(storage);

    tamper_oauth_value(&path, "alice", "anthropic_oauth")?;

    let storage = Storage::open(&path, [9; 32])?;
    let err = storage
        .get_oauth("alice", "anthropic_oauth")
        .expect_err("tampered ciphertext must fail authentication");
    println!("aead_decrypt_failure: {err}");
    assert!(matches!(err, StorageError::AeadAuthenticationFailed));

    Ok(())
}

fn tamper_oauth_value(
    path: &std::path::Path,
    principal_id: &str,
    provider: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let db = redb::Database::create(path)?;
    let write_txn = db.begin_write()?;
    {
        let mut table = write_txn.open_table(OAUTH_CREDENTIALS_V1)?;
        let key = oauth_key(principal_id, provider);
        let mut raw = table
            .get(key.as_slice())?
            .expect("stored OAuth value exists")
            .value()
            .to_vec();
        let last = raw.last_mut().expect("nonce plus tag exists");
        *last ^= 0x01;
        table.insert(key.as_slice(), raw.as_slice())?;
    }
    write_txn.commit()?;
    Ok(())
}
