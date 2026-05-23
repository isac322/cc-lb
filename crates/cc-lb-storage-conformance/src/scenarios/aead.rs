//! AEAD conformance scenarios for ciphertext-only credential stores.

use std::sync::Arc;

use anyhow::{Context, Result, ensure};
use cc_lb_aead::{AeadError, AeadService};
use cc_lb_storage_api::{ApiKeyStore, OAuthCredentialStore, Storage};

pub async fn run_all<Store>(storage: Arc<Store>, aead: &AeadService) -> Result<()>
where
    Store: Storage,
{
    let storage = storage.as_ref();

    aead_oauth_roundtrip(storage, aead).await?;
    aead_oauth_tamper(storage, aead).await?;
    aead_oauth_wrong_aad(storage, aead).await?;
    aead_apikey_roundtrip(storage, aead).await?;
    aead_apikey_list_and_revoke(storage, aead).await?;
    aead_anthropic_api_key_roundtrip(storage, aead).await?;
    ciphertext_principal_isolation(storage, aead).await?;

    Ok(())
}

pub async fn aead_oauth_roundtrip<Store>(storage: &Store, aead: &AeadService) -> Result<()>
where
    Store: OAuthCredentialStore + ?Sized,
{
    let principal_id = "t17-oauth-roundtrip-principal";
    let provider = "anthropic_oauth";
    let plaintext = br#"{"access_token":"oauth-access-token","refresh_token":"oauth-refresh-token","expires_at":4102444800,"scopes":["user:profile","claude"]}"#;
    let aad = oauth_aad(principal_id, provider);
    let ciphertext = aead.encrypt(plaintext, &aad)?;

    OAuthCredentialStore::put_oauth_ciphertext(storage, principal_id, provider, &ciphertext)
        .await?;

    let retrieved = OAuthCredentialStore::get_oauth_ciphertext(storage, principal_id, provider)
        .await?
        .context("stored OAuth ciphertext must be present")?;

    ensure!(
        retrieved == ciphertext,
        "OAuth store must preserve opaque bytes"
    );
    ensure!(
        !contains_bytes(&retrieved, plaintext),
        "OAuth store must receive ciphertext, not plaintext"
    );
    ensure!(
        aead.decrypt(&retrieved, &aad)? == plaintext,
        "OAuth ciphertext must decrypt to the original plaintext"
    );

    Ok(())
}

pub async fn aead_oauth_tamper<Store>(storage: &Store, aead: &AeadService) -> Result<()>
where
    Store: OAuthCredentialStore + ?Sized,
{
    let principal_id = "t17-oauth-tamper-principal";
    let provider = "anthropic_oauth";
    let plaintext = b"oauth-token-that-must-not-survive-tampering";
    let aad = oauth_aad(principal_id, provider);
    let ciphertext = aead.encrypt(plaintext, &aad)?;

    OAuthCredentialStore::put_oauth_ciphertext(storage, principal_id, provider, &ciphertext)
        .await?;

    let mut tampered = OAuthCredentialStore::get_oauth_ciphertext(storage, principal_id, provider)
        .await?
        .context("stored OAuth ciphertext must be present before tamper")?;
    let last = tampered
        .last_mut()
        .context("AEAD ciphertext must contain nonce and tag bytes")?;
    *last ^= 0x01;

    assert_decryption_failed(aead.decrypt(&tampered, &aad), "tampered OAuth ciphertext")
}

pub async fn aead_oauth_wrong_aad<Store>(storage: &Store, aead: &AeadService) -> Result<()>
where
    Store: OAuthCredentialStore + ?Sized,
{
    let principal_id = "t17-oauth-wrong-aad-principal";
    let provider = "anthropic_oauth";
    let plaintext = b"oauth-token-bound-to-principal-and-provider";
    let aad = oauth_aad(principal_id, provider);
    let ciphertext = aead.encrypt(plaintext, &aad)?;

    OAuthCredentialStore::put_oauth_ciphertext(storage, principal_id, provider, &ciphertext)
        .await?;

    let retrieved = OAuthCredentialStore::get_oauth_ciphertext(storage, principal_id, provider)
        .await?
        .context("stored OAuth ciphertext must be present before wrong-AAD check")?;
    let wrong_aad = oauth_aad(principal_id, "vertex_oauth");

    assert_decryption_failed(aead.decrypt(&retrieved, &wrong_aad), "OAuth wrong AAD")
}

pub async fn aead_apikey_roundtrip<Store>(storage: &Store, aead: &AeadService) -> Result<()>
where
    Store: ApiKeyStore + ?Sized,
{
    let principal_id = "t17-apikey-roundtrip-principal";
    let key_id = "key-roundtrip";
    let plaintext = br#"{"key_id":"key-roundtrip","label":"primary","issued_at_unix_secs":1700000000,"revoked_at_unix_secs":null}"#;
    let aad = api_key_aad(principal_id, key_id);
    let ciphertext = aead.encrypt(plaintext, &aad)?;

    ApiKeyStore::put_api_key_ciphertext(storage, principal_id, key_id, &ciphertext).await?;

    let retrieved = ApiKeyStore::get_api_key_ciphertext(storage, principal_id, key_id)
        .await?
        .context("stored API-key ciphertext must be present")?;

    ensure!(
        retrieved == ciphertext,
        "API-key store must preserve opaque bytes"
    );
    ensure!(
        !contains_bytes(&retrieved, plaintext),
        "API-key store must receive ciphertext, not plaintext"
    );
    ensure!(
        aead.decrypt(&retrieved, &aad)? == plaintext,
        "API-key ciphertext must decrypt to the original plaintext"
    );

    Ok(())
}

pub async fn aead_apikey_list_and_revoke<Store>(storage: &Store, aead: &AeadService) -> Result<()>
where
    Store: ApiKeyStore + ?Sized,
{
    let principal_id = "t17-apikey-list-principal";
    let active_records = [
        ("key-c", b"active-api-key-c".as_slice()),
        ("key-a", b"active-api-key-a".as_slice()),
        ("key-b", b"active-api-key-b".as_slice()),
    ];

    for (key_id, plaintext) in active_records {
        let aad = api_key_aad(principal_id, key_id);
        let ciphertext = aead.encrypt(plaintext, &aad)?;
        ApiKeyStore::put_api_key_ciphertext(storage, principal_id, key_id, &ciphertext).await?;
    }

    ApiKeyStore::put_api_key_ciphertext(
        storage,
        "t17-apikey-list-other-principal",
        "key-b",
        &aead.encrypt(
            b"other-principal-active-api-key-b",
            &api_key_aad("t17-apikey-list-other-principal", "key-b"),
        )?,
    )
    .await?;

    let listed = ApiKeyStore::list_api_key_ciphertexts(storage, principal_id).await?;
    let listed_key_ids = listed
        .iter()
        .map(|(key_id, _)| key_id.as_str())
        .collect::<Vec<_>>();
    ensure!(
        listed_key_ids == ["key-a", "key-b", "key-c"],
        "API-key ciphertext listing must be scoped and sorted by key_id"
    );

    for (key_id, ciphertext) in &listed {
        let aad = api_key_aad(principal_id, key_id);
        let plaintext = aead.decrypt(ciphertext, &aad)?;
        ensure!(
            plaintext
                == format!(
                    "active-api-key-{}",
                    key_id.strip_prefix("key-").unwrap_or(key_id)
                )
                .as_bytes(),
            "listed API-key ciphertext must decrypt under its key_id AAD"
        );
    }

    let revoked_plaintext = b"revoked-api-key-b";
    let revoked_ciphertext =
        aead.encrypt(revoked_plaintext, &api_key_aad(principal_id, "key-b"))?;
    ensure!(
        ApiKeyStore::revoke_api_key(storage, principal_id, "key-b", &revoked_ciphertext).await?,
        "revoking an existing API key must replace ciphertext"
    );
    ensure!(
        !ApiKeyStore::revoke_api_key(
            storage,
            principal_id,
            "missing-key",
            &aead.encrypt(b"missing", &api_key_aad(principal_id, "missing-key"))?,
        )
        .await?,
        "revoking a missing API key must report false"
    );

    let retrieved = ApiKeyStore::get_api_key_ciphertext(storage, principal_id, "key-b")
        .await?
        .context("revoked API-key ciphertext must remain present")?;
    ensure!(
        aead.decrypt(&retrieved, &api_key_aad(principal_id, "key-b"))? == revoked_plaintext,
        "revoked API-key ciphertext must decrypt to the revoked marker"
    );

    Ok(())
}

pub async fn aead_anthropic_api_key_roundtrip<Store>(
    storage: &Store,
    aead: &AeadService,
) -> Result<()>
where
    Store: OAuthCredentialStore + ?Sized,
{
    let storage_key = "anthropic:t17-anthropic-api-key-principal:default";
    let plaintext = br#"{"anthropic_api_key":"sk-ant-api03-test-secret"}"#;
    let aad = anthropic_api_key_aad(storage_key);
    let ciphertext = aead.encrypt(plaintext, &aad)?;

    OAuthCredentialStore::put_anthropic_api_key_ciphertext(storage, storage_key, &ciphertext)
        .await?;

    let retrieved = OAuthCredentialStore::get_anthropic_api_key_ciphertext(storage, storage_key)
        .await?
        .context("stored Anthropic API-key ciphertext must be present")?;

    ensure!(
        retrieved == ciphertext,
        "Anthropic API-key store must preserve opaque bytes"
    );
    ensure!(
        !contains_bytes(&retrieved, plaintext),
        "Anthropic API-key store must receive ciphertext, not plaintext"
    );
    ensure!(
        aead.decrypt(&retrieved, &aad)? == plaintext,
        "Anthropic API-key ciphertext must decrypt to the original plaintext"
    );

    Ok(())
}

pub async fn ciphertext_principal_isolation<Store>(
    storage: &Store,
    aead: &AeadService,
) -> Result<()>
where
    Store: OAuthCredentialStore + ApiKeyStore + ?Sized,
{
    let alice = "t17-isolation-alice";
    let bob = "t17-isolation-bob";
    let provider = "anthropic_oauth";
    let key_id = "shared-key-id";

    let oauth_ciphertext = aead.encrypt(b"alice-oauth-secret", &oauth_aad(alice, provider))?;
    OAuthCredentialStore::put_oauth_ciphertext(storage, alice, provider, &oauth_ciphertext).await?;
    ensure!(
        OAuthCredentialStore::get_oauth_ciphertext(storage, bob, provider)
            .await?
            .is_none(),
        "OAuth ciphertext lookup must be scoped by principal"
    );
    let alice_oauth = OAuthCredentialStore::get_oauth_ciphertext(storage, alice, provider)
        .await?
        .context("Alice OAuth ciphertext must be present")?;
    assert_decryption_failed(
        aead.decrypt(&alice_oauth, &oauth_aad(bob, provider)),
        "OAuth ciphertext under another principal AAD",
    )?;

    let api_key_ciphertext = aead.encrypt(b"alice-api-key-secret", &api_key_aad(alice, key_id))?;
    ApiKeyStore::put_api_key_ciphertext(storage, alice, key_id, &api_key_ciphertext).await?;
    ensure!(
        ApiKeyStore::get_api_key_ciphertext(storage, bob, key_id)
            .await?
            .is_none(),
        "API-key ciphertext lookup must be scoped by principal"
    );
    ensure!(
        ApiKeyStore::list_api_key_ciphertexts(storage, bob)
            .await?
            .is_empty(),
        "API-key ciphertext listing must be scoped by principal"
    );
    let alice_api_key = ApiKeyStore::get_api_key_ciphertext(storage, alice, key_id)
        .await?
        .context("Alice API-key ciphertext must be present")?;
    assert_decryption_failed(
        aead.decrypt(&alice_api_key, &api_key_aad(bob, key_id)),
        "API-key ciphertext under another principal AAD",
    )
}

/// Builds stable AAD for OAuth credentials from the storage lookup identity.
fn oauth_aad(principal_id: &str, provider: &str) -> Vec<u8> {
    [
        b"oauth".as_slice(),
        principal_id.as_bytes(),
        provider.as_bytes(),
    ]
    .join(&0)
}

/// Builds stable AAD for stored API-key records from the principal and key id.
fn api_key_aad(principal_id: &str, key_id: &str) -> Vec<u8> {
    [
        b"api-key".as_slice(),
        principal_id.as_bytes(),
        key_id.as_bytes(),
    ]
    .join(&0)
}

/// Builds stable AAD for the Anthropic direct-key slot from its storage key.
fn anthropic_api_key_aad(storage_key: &str) -> Vec<u8> {
    [b"anthropic-api-key".as_slice(), storage_key.as_bytes()].join(&0)
}

fn assert_decryption_failed(result: cc_lb_aead::AeadResult<Vec<u8>>, context: &str) -> Result<()> {
    match result {
        Err(AeadError::DecryptionFailed) => Ok(()),
        Err(error) => anyhow::bail!("{context} failed with unexpected AEAD error: {error}"),
        Ok(_) => anyhow::bail!("{context} decrypted successfully but should fail"),
    }
}

fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}
