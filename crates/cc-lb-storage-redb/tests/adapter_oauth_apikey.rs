use cc_lb_storage_api::{ApiKeyStore, OAuthCredentialStore};
use cc_lb_storage_redb::RedbStorage;

#[tokio::test]
async fn oauth_ciphertext_trait_path_roundtrips_arbitrary_bytes_and_isolates_keys()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("adapter-oauth-apikey-oauth.redb");
    let storage = RedbStorage::open(&path)?;

    let alice_ciphertext = vec![0, 1, 2, 3, 0xfe, 0xff, b'{', b'}'];
    let bob_ciphertext = vec![9, 8, 7, 0, 6];

    OAuthCredentialStore::put_oauth_ciphertext(
        &storage,
        "principal-a",
        "anthropic_oauth",
        &alice_ciphertext,
    )
    .await?;
    OAuthCredentialStore::put_oauth_ciphertext(
        &storage,
        "principal-b",
        "anthropic_oauth",
        &bob_ciphertext,
    )
    .await?;

    assert_eq!(
        OAuthCredentialStore::get_oauth_ciphertext(&storage, "principal-a", "anthropic_oauth")
            .await?,
        Some(alice_ciphertext.clone())
    );
    assert_eq!(
        OAuthCredentialStore::get_oauth_ciphertext(&storage, "principal-a", "vertex_oauth").await?,
        None
    );
    assert_eq!(
        OAuthCredentialStore::get_oauth_ciphertext(&storage, "principal-b", "anthropic_oauth")
            .await?,
        Some(bob_ciphertext.clone())
    );

    assert!(OAuthCredentialStore::delete_oauth(&storage, "principal-a", "anthropic_oauth").await?);
    assert!(!OAuthCredentialStore::delete_oauth(&storage, "principal-a", "anthropic_oauth").await?);
    assert_eq!(
        OAuthCredentialStore::get_oauth_ciphertext(&storage, "principal-a", "anthropic_oauth")
            .await?,
        None
    );
    assert_eq!(
        OAuthCredentialStore::get_oauth_ciphertext(&storage, "principal-b", "anthropic_oauth")
            .await?,
        Some(bob_ciphertext)
    );

    Ok(())
}

#[tokio::test]
async fn anthropic_api_key_ciphertext_trait_path_roundtrips_by_storage_key()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("adapter-oauth-apikey-anthropic.redb");
    let storage = RedbStorage::open(&path)?;
    let ciphertext = vec![0xaa, 0, 0xbb, 4, 5, 6];

    OAuthCredentialStore::put_anthropic_api_key_ciphertext(
        &storage,
        "anthropic:principal-a:slot-1",
        &ciphertext,
    )
    .await?;

    assert_eq!(
        OAuthCredentialStore::get_anthropic_api_key_ciphertext(
            &storage,
            "anthropic:principal-a:slot-1"
        )
        .await?,
        Some(ciphertext)
    );
    assert_eq!(
        OAuthCredentialStore::get_anthropic_api_key_ciphertext(
            &storage,
            "anthropic:principal-a:slot-2"
        )
        .await?,
        None
    );

    Ok(())
}

#[tokio::test]
async fn api_key_store_trait_path_lists_ciphertexts_in_key_id_order()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("adapter-oauth-apikey-list.redb");
    let storage = RedbStorage::open(&path)?;

    ApiKeyStore::put_api_key_ciphertext(&storage, "principal-a", "key-c", b"cipher-c").await?;
    ApiKeyStore::put_api_key_ciphertext(&storage, "principal-a", "key-a", &[0, 1, 2]).await?;
    ApiKeyStore::put_api_key_ciphertext(&storage, "principal-b", "key-b", b"other").await?;
    ApiKeyStore::put_api_key_ciphertext(&storage, "principal-a", "key-b", b"cipher-b").await?;

    assert_eq!(
        ApiKeyStore::get_api_key_ciphertext(&storage, "principal-a", "key-a").await?,
        Some(vec![0, 1, 2])
    );
    assert_eq!(
        ApiKeyStore::list_api_key_ciphertexts(&storage, "principal-a").await?,
        vec![
            ("key-a".to_owned(), vec![0, 1, 2]),
            ("key-b".to_owned(), b"cipher-b".to_vec()),
            ("key-c".to_owned(), b"cipher-c".to_vec()),
        ]
    );
    assert_eq!(
        ApiKeyStore::list_api_key_ciphertexts(&storage, "principal-b").await?,
        vec![("key-b".to_owned(), b"other".to_vec())]
    );

    Ok(())
}

#[tokio::test]
async fn api_key_store_trait_path_revoke_replaces_only_existing_ciphertext()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("adapter-oauth-apikey-revoke.redb");
    let storage = RedbStorage::open(&path)?;

    assert!(!ApiKeyStore::revoke_api_key(&storage, "principal-a", "missing", b"revoked").await?);
    assert_eq!(
        ApiKeyStore::get_api_key_ciphertext(&storage, "principal-a", "missing").await?,
        None
    );

    ApiKeyStore::put_api_key_ciphertext(&storage, "principal-a", "key-1", b"active").await?;

    assert!(ApiKeyStore::revoke_api_key(&storage, "principal-a", "key-1", b"revoked").await?);
    assert_eq!(
        ApiKeyStore::get_api_key_ciphertext(&storage, "principal-a", "key-1").await?,
        Some(b"revoked".to_vec())
    );

    assert!(ApiKeyStore::revoke_api_key(&storage, "principal-a", "key-1", b"revoked-again").await?);
    assert_eq!(
        ApiKeyStore::get_api_key_ciphertext(&storage, "principal-a", "key-1").await?,
        Some(b"revoked-again".to_vec())
    );
    assert!(
        !ApiKeyStore::revoke_api_key(&storage, "principal-b", "key-1", b"wrong-principal").await?
    );
    assert_eq!(
        ApiKeyStore::list_api_key_ciphertexts(&storage, "principal-b").await?,
        Vec::<(String, Vec<u8>)>::new()
    );

    Ok(())
}
