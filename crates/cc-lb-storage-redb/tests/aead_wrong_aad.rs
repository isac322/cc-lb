use cc_lb_storage_redb::{
    OAUTH_CREDENTIALS_V1, OAuthCredentials, Storage, StorageError, oauth_key,
};
use redb::ReadableTable;

#[test]
fn copied_ciphertext_under_another_principal_fails_aad() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("storage.redb");
    let creds = OAuthCredentials {
        access_token: "access-token".to_owned(),
        refresh_token: "refresh-token".to_owned(),
        expires_at: 1_765_000_000,
        scopes: vec!["messages".to_owned()],
    };

    let storage = Storage::open(&path, [11; 32])?;
    storage.put_oauth("alice", "anthropic_oauth", &creds)?;
    drop(storage);

    copy_ciphertext_to_principal(&path, "alice", "bob", "anthropic_oauth")?;

    let storage = Storage::open(&path, [11; 32])?;
    let err = storage
        .get_oauth("bob", "anthropic_oauth")
        .expect_err("wrong principal AAD must fail authentication");
    println!("aead_verify_failure_wrong_aad: {err}");
    assert!(matches!(err, StorageError::AeadAuthenticationFailed));
    assert_eq!(storage.get_oauth("alice", "anthropic_oauth")?, Some(creds));

    Ok(())
}

fn copy_ciphertext_to_principal(
    path: &std::path::Path,
    from_principal: &str,
    to_principal: &str,
    provider: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let db = redb::Database::create(path)?;
    let write_txn = db.begin_write()?;
    {
        let mut table = write_txn.open_table(OAUTH_CREDENTIALS_V1)?;
        let from_key = oauth_key(from_principal, provider);
        let to_key = oauth_key(to_principal, provider);
        let raw = table
            .get(from_key.as_slice())?
            .expect("stored OAuth value exists")
            .value()
            .to_vec();
        table.insert(to_key.as_slice(), raw.as_slice())?;
    }
    write_txn.commit()?;
    Ok(())
}
