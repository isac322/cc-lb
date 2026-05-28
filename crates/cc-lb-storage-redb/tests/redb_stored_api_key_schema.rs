use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use bincode::config::standard;
use bincode::serde::{decode_from_slice, encode_to_vec};
use cc_lb_storage_api::types::{
    ApiKeyMutation, IssueParams, KeyStatus, Limit, LimitKind, PrincipalKindLite,
    StoredApiKeyRecord, UpstreamKind,
};
use cc_lb_storage_redb::{API_KEYS_V1, Storage, api_key_storage_key};
use redb::ReadableDatabase;
use serde::{Deserialize, Serialize};

#[test]
fn redb_stored_api_key_schema_roundtrip_preserves_new_fields()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("api_keys.redb");
    let storage = Storage::open(&path, [11; 32])?;
    let params = IssueParams {
        label: "managed key".to_owned(),
        description: Some("support key".to_owned()),
        upstream_kind: UpstreamKind::AnthropicKey,
        upstream_credential_ref: "anthropic-prod".to_owned(),
        expires_at_unix_secs: Some(1_765_000_000),
        limit_overrides: vec![Limit {
            kind: LimitKind::Requests,
            window_secs: 3_600,
            cap_micros: 250,
        }],
        secret_salt: [7; 16],
        verify_hash: [9; 32],
        last_4: "abcd".to_owned(),
        principal_kind: PrincipalKindLite::Human,
        index_hash: [5; 32],
    };

    storage.issue_api_key_record("alice", "key-1", params.clone())?;

    let listed = storage.list_api_keys("alice")?;
    assert_eq!(listed.len(), 1);
    let record = listed.first().cloned().expect("one stored api key");
    assert_eq!(record.label, params.label);
    assert_eq!(record.description, params.description);
    assert_eq!(record.upstream_kind, params.upstream_kind);
    assert_eq!(
        record.upstream_credential_ref,
        params.upstream_credential_ref
    );
    assert_eq!(record.limit_overrides, params.limit_overrides);
    assert_eq!(record.status, KeyStatus::Active);
    assert_eq!(record.expires_at_unix_secs, params.expires_at_unix_secs);
    assert_eq!(record.last_4, params.last_4);
    assert_eq!(record.principal_kind, params.principal_kind);
    assert_eq!(record.index_hash, params.index_hash);
    assert_eq!(
        record.key_hash_b64,
        URL_SAFE_NO_PAD.encode(params.verify_hash)
    );

    let fetched = storage
        .get_api_key("alice", "key-1")?
        .expect("stored api key available");
    assert_eq!(fetched, record);
    assert!(fetched.issued_at_unix_secs > 0);

    let encoded = encode_to_vec(&record, standard().with_variable_int_encoding())?;
    let (decoded, consumed) = decode_from_slice::<StoredApiKeyRecord, _>(
        &encoded,
        standard().with_variable_int_encoding(),
    )?;
    assert_eq!(consumed, encoded.len());
    assert_eq!(decoded, record);

    let legacy = LegacyStoredApiKeyRecord {
        label: "legacy".to_owned(),
        issued_at_unix_secs: 12,
        revoked_at_unix_secs: Some(34),
        key_hash_b64: "legacy-hash".to_owned(),
    };
    let legacy_bytes = encode_to_vec(
        LegacyStoredApiKeyRecordWire::V0(legacy.clone()),
        standard().with_variable_int_encoding(),
    )?;
    let (legacy_decoded, consumed) = decode_from_slice::<StoredApiKeyRecord, _>(
        &legacy_bytes,
        standard().with_variable_int_encoding(),
    )?;
    assert_eq!(consumed, legacy_bytes.len());
    assert_eq!(legacy_decoded.label, legacy.label);
    assert_eq!(
        legacy_decoded.issued_at_unix_secs,
        legacy.issued_at_unix_secs
    );
    assert_eq!(
        legacy_decoded.revoked_at_unix_secs,
        legacy.revoked_at_unix_secs
    );
    assert_eq!(legacy_decoded.key_hash_b64, legacy.key_hash_b64);
    assert_eq!(legacy_decoded.status, KeyStatus::Active);
    assert_eq!(legacy_decoded.limit_overrides, Vec::<Limit>::new());
    assert_eq!(legacy_decoded.verify_hash, [0; 32]);

    drop(storage);

    let raw_value = {
        let db = redb::Database::create(&path)?;
        let read_txn = db.begin_read()?;
        let table = read_txn.open_table(API_KEYS_V1)?;
        table
            .get(api_key_storage_key("alice", "key-1").as_slice())?
            .expect("stored api key row")
            .value()
            .to_vec()
    };
    assert!(!contains_bytes(&raw_value, b"managed key"));
    assert!(!contains_bytes(&raw_value, b"anthropic-prod"));

    Ok(())
}

#[test]
fn redb_stored_api_key_update_transitions_status() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("api_keys_update.redb");
    let storage = Storage::open(&path, [13; 32])?;
    let params = IssueParams {
        label: "managed key".to_owned(),
        description: Some("original".to_owned()),
        upstream_kind: UpstreamKind::AnthropicOAuth,
        upstream_credential_ref: "oauth-cred".to_owned(),
        expires_at_unix_secs: Some(1_765_123_456),
        limit_overrides: vec![Limit {
            kind: LimitKind::OutputTokens,
            window_secs: 86_400,
            cap_micros: 900,
        }],
        secret_salt: [3; 16],
        verify_hash: [4; 32],
        last_4: "wxyz".to_owned(),
        principal_kind: PrincipalKindLite::Machine,
        index_hash: [8; 32],
    };

    storage.issue_api_key_record("alice", "key-2", params.clone())?;

    let updated = storage.update_api_key_record(
        "alice",
        "key-2",
        ApiKeyMutation {
            label: Some("updated key".to_owned()),
            description: Some(Some("updated description".to_owned())),
            expires_at_unix_secs: Some(Some(1_800_000_000)),
            limit_overrides: Some(vec![Limit {
                kind: LimitKind::Requests,
                window_secs: 60,
                cap_micros: 1_200,
            }]),
            status: Some(KeyStatus::Disabled),
        },
    )?;

    let updated = updated.expect("row updated");
    assert_eq!(updated.label, "updated key");
    assert_eq!(updated.description, Some("updated description".to_owned()));
    assert_eq!(updated.expires_at_unix_secs, Some(1_800_000_000));
    assert_eq!(updated.limit_overrides.len(), 1);
    assert_eq!(updated.status, KeyStatus::Disabled);

    let fetched = storage
        .get_api_key("alice", "key-2")?
        .expect("updated api key available");
    assert_eq!(fetched, updated);

    let revoked = storage
        .revoke_api_key_record("alice", "key-2")?
        .expect("row revoked");
    assert_eq!(revoked.status, KeyStatus::Revoked);
    assert!(revoked.revoked_at_unix_secs.is_some());

    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct LegacyStoredApiKeyRecord {
    label: String,
    issued_at_unix_secs: u64,
    revoked_at_unix_secs: Option<u64>,
    key_hash_b64: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum LegacyStoredApiKeyRecordWire {
    V0(LegacyStoredApiKeyRecord),
    V1(LegacyStoredApiKeyRecord),
}

fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}
