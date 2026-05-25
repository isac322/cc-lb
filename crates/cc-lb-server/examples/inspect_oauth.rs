use std::env;
use std::path::PathBuf;
use std::sync::Arc;

use cc_lb_aead::AeadService;
use cc_lb_config::{PostgresPoolConfig, StorageConfig};
use cc_lb_server::storage_factory;
use cc_lb_storage_api::OAuthCredentials;
use clap::{Parser, ValueEnum};

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum Backend {
    Redb,
    Postgres,
}

#[derive(Debug, Parser)]
#[command(name = "inspect_oauth", about = "Inspect OAuth credentials")]
pub struct Args {
    #[arg(long, value_enum)]
    pub backend: Backend,

    #[arg(long, value_name = "PATH")]
    pub redb_path: Option<PathBuf>,

    #[arg(long, value_name = "URL")]
    pub postgres_url: Option<String>,

    #[arg(long, value_name = "VAR")]
    pub aead_key_env: String,

    #[arg(long, value_name = "ID")]
    pub principal_id: Option<String>,

    #[arg(long, value_name = "PROVIDER")]
    pub provider: Option<String>,
}

fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn decode_hex_key(value: &str) -> Result<[u8; 32], String> {
    if value.len() != 64 {
        return Err(format!(
            "Key must be 64 hex characters (32 bytes); got length {}",
            value.len()
        ));
    }
    let mut key = [0_u8; 32];
    for (index, chunk) in value.as_bytes().chunks(2).enumerate() {
        let high =
            hex_nibble(chunk[0]).ok_or_else(|| "Invalid hex character in key".to_string())?;
        let low = hex_nibble(chunk[1]).ok_or_else(|| "Invalid hex character in key".to_string())?;
        key[index] = (high << 4) | low;
    }
    Ok(key)
}

fn mask_token(token: &str) -> String {
    if token.len() <= 8 {
        "********".to_string()
    } else {
        format!("{}...{}", &token[..4], &token[token.len() - 4..])
    }
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let config = match args.backend {
        Backend::Redb => {
            let path = match args.redb_path {
                Some(p) => p,
                None => {
                    eprintln!("Error: --redb-path is required when --backend is redb");
                    std::process::exit(1);
                }
            };
            StorageConfig::Redb { path }
        }
        Backend::Postgres => {
            let url = match args.postgres_url {
                Some(u) => u,
                None => {
                    eprintln!("Error: --postgres-url is required when --backend is postgres");
                    std::process::exit(1);
                }
            };
            StorageConfig::Postgres {
                url,
                pool: PostgresPoolConfig::default(),
            }
        }
    };

    let principal_id = match args.principal_id {
        Some(id) => id,
        None => {
            eprintln!("Error: --principal-id is required");
            std::process::exit(1);
        }
    };

    let provider = match args.provider {
        Some(p) => p,
        None => {
            eprintln!("Error: --provider is required");
            std::process::exit(1);
        }
    };

    let key_hex = match env::var(&args.aead_key_env) {
        Ok(val) => val,
        Err(_) => {
            eprintln!(
                "Error: Environment variable '{}' not found",
                args.aead_key_env
            );
            std::process::exit(1);
        }
    };

    let key_bytes = match decode_hex_key(&key_hex) {
        Ok(bytes) => bytes,
        Err(err) => {
            eprintln!("Error: Failed to decode key: {}", err);
            std::process::exit(1);
        }
    };

    let aead = Arc::new(AeadService::try_from_key(&key_bytes).expect("valid key"));

    let storage = match storage_factory::open_storage(&config, aead.clone(), key_bytes).await {
        Ok(s) => s,
        Err(err) => {
            eprintln!("Error: Failed to open storage: {}", err);
            std::process::exit(1);
        }
    };

    let ciphertext = match storage.get_oauth_ciphertext(&principal_id, &provider).await {
        Ok(Some(c)) => c,
        Ok(None) => {
            eprintln!(
                "Error: No OAuth credentials found for principal_id='{}', provider='{}'",
                principal_id, provider
            );
            std::process::exit(1);
        }
        Err(err) => {
            eprintln!("Error: Failed to get OAuth ciphertext: {}", err);
            std::process::exit(1);
        }
    };

    let aad = format!("oauth:{principal_id}:{provider}");
    let decrypted_bytes = match aead.decrypt(&ciphertext, aad.as_bytes()) {
        Ok(bytes) => bytes,
        Err(err) => {
            eprintln!("Error: Decryption failed: {}", err);
            std::process::exit(1);
        }
    };

    let creds: OAuthCredentials = match serde_json::from_slice(&decrypted_bytes) {
        Ok(c) => c,
        Err(err) => {
            eprintln!("Error: Failed to deserialize OAuth credentials: {}", err);
            std::process::exit(1);
        }
    };

    println!("Decrypted OAuth Credentials:");
    println!("  Access Token:  {}", mask_token(&creds.access_token));
    println!("  Refresh Token: {}", mask_token(&creds.refresh_token));
    println!("  Expires At:    {}", creds.expires_at);
    println!("  Scopes:        {:?}", creds.scopes);
}
