use cc_lb_storage_redb::{OAUTH_CREDENTIALS_V1, Storage, oauth_key};
use redb::ReadableDatabase;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() != 5 {
        return Err("usage: inspect_oauth <redb_path> <hex_key> <principal_id> <provider>".into());
    }
    let key = decode_hex_key(&args[2])?;
    let storage = Storage::open(std::path::Path::new(&args[1]), key)?;
    let creds = storage
        .get_oauth(&args[3], &args[4])?
        .ok_or("oauth row missing")?;
    drop(storage);

    let raw = raw_oauth_value(std::path::Path::new(&args[1]), &args[3], &args[4])?;
    let access_plaintext = contains_bytes(&raw, creds.access_token.as_bytes());
    let refresh_plaintext = contains_bytes(&raw, creds.refresh_token.as_bytes());
    println!(
        "oauth row exists principal={} provider={} access_prefix={} refresh_prefix={} payload_non_plaintext={}",
        args[3],
        args[4],
        creds.access_token.starts_with("sk-ant-oat01-"),
        creds.refresh_token.starts_with("sk-ant-ort01-"),
        !access_plaintext && !refresh_plaintext
    );
    if access_plaintext || refresh_plaintext {
        return Err("oauth row contains plaintext token bytes".into());
    }
    Ok(())
}

fn raw_oauth_value(
    path: &std::path::Path,
    principal_id: &str,
    provider: &str,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let db = redb::Database::create(path)?;
    let read_txn = db.begin_read()?;
    let table = read_txn.open_table(OAUTH_CREDENTIALS_V1)?;
    let key = oauth_key(principal_id, provider);
    let value = table
        .get(key.as_slice())?
        .ok_or("stored OAuth value missing")?;
    Ok(value.value().to_vec())
}

fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

fn decode_hex_key(value: &str) -> Result<[u8; 32], Box<dyn std::error::Error>> {
    if value.len() != 64 {
        return Err("storage key must be 64 hex chars".into());
    }
    let mut key = [0_u8; 32];
    for (index, chunk) in value.as_bytes().chunks(2).enumerate() {
        let high = hex_nibble(chunk[0])?;
        let low = hex_nibble(chunk[1])?;
        key[index] = (high << 4) | low;
    }
    Ok(key)
}

fn hex_nibble(byte: u8) -> Result<u8, Box<dyn std::error::Error>> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err("invalid hex digit".into()),
    }
}
