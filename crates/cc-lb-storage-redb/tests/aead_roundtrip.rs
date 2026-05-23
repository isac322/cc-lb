use cc_lb_storage_redb::{OAUTH_CREDENTIALS_V1, OAuthCredentials, Storage, oauth_key};

#[test]
fn oauth_roundtrip_keeps_tokens_encrypted_at_rest() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("storage.redb");
    let creds = OAuthCredentials {
        access_token: "access-secret-token".to_owned(),
        refresh_token: "refresh-secret-token".to_owned(),
        expires_at: 1_765_000_000,
        scopes: vec!["messages".to_owned(), "files".to_owned()],
    };

    let storage = Storage::open(&path, [7; 32])?;
    storage.put_oauth("alice", "anthropic_oauth", &creds)?;

    assert_eq!(
        storage.get_oauth("alice", "anthropic_oauth")?,
        Some(creds.clone())
    );
    assert_eq!(storage.get_oauth("missing", "anthropic_oauth")?, None);
    drop(storage);

    let raw_value = {
        let db = redb::Database::create(&path)?;
        let read_txn = db.begin_read()?;
        let table = read_txn.open_table(OAUTH_CREDENTIALS_V1)?;
        table
            .get(oauth_key("alice", "anthropic_oauth").as_slice())?
            .expect("stored OAuth value exists")
            .value()
            .to_vec()
    };

    assert!(!contains_bytes(&raw_value, b"access-secret-token"));
    assert!(!contains_bytes(&raw_value, b"refresh-secret-token"));

    let storage = Storage::open(&path, [7; 32])?;
    storage.delete_oauth("alice", "anthropic_oauth")?;
    assert_eq!(storage.get_oauth("alice", "anthropic_oauth")?, None);

    Ok(())
}

fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}
