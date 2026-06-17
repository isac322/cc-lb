use std::env;
use std::path::PathBuf;
use std::sync::Arc;

use cc_lb_aead::AeadService;
use cc_lb_config::{PostgresPoolConfig, StorageConfig};
use cc_lb_server::storage_factory;
use cc_lb_storage_api::AnthropicApiKeyCredential;
use clap::{Parser, ValueEnum};

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum Backend {
    Sqlite,
    Postgres,
}

#[derive(Debug, Parser)]
#[command(
    name = "seed_real_anthropic_key",
    about = "Seed real Anthropic API key"
)]
pub struct Args {
    #[arg(long, value_enum)]
    pub backend: Backend,

    #[arg(long, value_name = "PATH")]
    pub sqlite_path: Option<PathBuf>,

    #[arg(long, value_name = "URL")]
    pub postgres_url: Option<String>,

    #[arg(long, value_name = "VAR")]
    pub aead_key_env: String,

    #[arg(long, value_name = "ID")]
    pub principal_id: String,

    #[arg(long, value_name = "KEY")]
    pub api_key: String,
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

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let config = match args.backend {
        Backend::Sqlite => {
            let path = match args.sqlite_path {
                Some(p) => p,
                None => {
                    eprintln!("Error: --sqlite-path is required when --backend is sqlite");
                    std::process::exit(1);
                }
            };
            StorageConfig::Sqlite { path }
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

    let storage_key = format!("{}:real_anthropic_api_key", args.principal_id);
    let credential = AnthropicApiKeyCredential {
        anthropic_api_key: args.api_key,
    };

    let plaintext = match serde_json::to_vec(&credential) {
        Ok(bytes) => bytes,
        Err(err) => {
            eprintln!("Error: Failed to serialize credential: {}", err);
            std::process::exit(1);
        }
    };

    let aad = format!("anthropic-api-key:{storage_key}");
    let ciphertext = match aead.encrypt(&plaintext, aad.as_bytes()) {
        Ok(c) => c,
        Err(err) => {
            eprintln!("Error: Encryption failed: {}", err);
            std::process::exit(1);
        }
    };

    if let Err(err) = storage
        .put_anthropic_api_key_ciphertext(&storage_key, &ciphertext)
        .await
    {
        eprintln!("Error: Failed to store API key ciphertext: {}", err);
        std::process::exit(1);
    }

    println!("seeded key for storage_key={}", storage_key);
}
