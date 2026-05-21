use cc_lb_storage_redb::Storage;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() != 5 {
        return Err(
            "usage: seed_real_anthropic_key <redb_path> <hex_key> <storage_key> <api_key>".into(),
        );
    }

    let key = decode_hex_key(&args[2])?;
    let storage = Storage::open(std::path::Path::new(&args[1]), key)?;
    storage.put_anthropic_api_key(&args[3], &args[4])?;
    println!(
        "seeded storage_key={} payload=anthropic_api_key encrypted=true",
        args[3]
    );
    Ok(())
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
