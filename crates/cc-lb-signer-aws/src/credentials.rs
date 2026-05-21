use std::collections::BTreeMap;
use std::env;
use std::fmt;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use secrecy::SecretString;
use thiserror::Error;

/// AWS credentials resolved for SigV4 signing.
#[derive(Clone)]
pub struct AwsCredentials {
    /// AWS access key ID.
    pub access_key_id: String,
    /// AWS secret access key.
    pub secret_access_key: SecretString,
    /// Optional AWS session token.
    pub session_token: Option<SecretString>,
    /// Optional credential expiration time.
    pub expires_at: Option<SystemTime>,
}

impl AwsCredentials {
    /// Creates AWS credentials from owned string values.
    pub fn new(
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
        session_token: Option<String>,
        expires_at: Option<SystemTime>,
    ) -> Self {
        Self {
            access_key_id: access_key_id.into(),
            secret_access_key: SecretString::new(secret_access_key.into().into_boxed_str()),
            session_token: session_token.map(|token| SecretString::new(token.into_boxed_str())),
            expires_at,
        }
    }
}

impl fmt::Debug for AwsCredentials {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AwsCredentials")
            .field("access_key_id", &self.access_key_id)
            .field("secret_access_key", &"[REDACTED]")
            .field(
                "session_token",
                &self.session_token.as_ref().map(|_| "[REDACTED]"),
            )
            .field("expires_at", &self.expires_at)
            .finish()
    }
}

/// Errors returned by AWS credential providers.
#[derive(Debug, Error)]
pub enum AwsCredentialsError {
    /// No credentials were found in this source.
    #[error("aws credentials not found: {location}")]
    NotFound {
        /// Credential location that did not contain usable credentials.
        location: String,
    },
    /// Credentials were present but malformed or incomplete.
    #[error("invalid aws credentials: {reason}")]
    Invalid {
        /// Redacted validation failure.
        reason: String,
    },
    /// A credentials file could not be read.
    #[error("failed to read aws credentials file {path}: {reason}")]
    Io {
        /// Credentials file path.
        path: PathBuf,
        /// Redacted I/O failure.
        reason: String,
    },
}

/// Async AWS credentials provider boundary used by the signer.
#[async_trait]
pub trait AwsCredentialsProvider: fmt::Debug + Send + Sync {
    /// Resolves credentials for signing.
    async fn resolve(&self) -> Result<AwsCredentials, AwsCredentialsError>;
}

/// Resolves credentials from AWS environment variables.
#[derive(Clone, Debug, Default)]
pub struct EnvCredentialsProvider;

impl EnvCredentialsProvider {
    /// Creates an environment credentials provider.
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl AwsCredentialsProvider for EnvCredentialsProvider {
    async fn resolve(&self) -> Result<AwsCredentials, AwsCredentialsError> {
        let access_key_id = non_empty_env("AWS_ACCESS_KEY_ID");
        let secret_access_key = non_empty_env("AWS_SECRET_ACCESS_KEY");
        let session_token = non_empty_env("AWS_SESSION_TOKEN");

        match (access_key_id, secret_access_key) {
            (Some(access_key_id), Some(secret_access_key)) => Ok(AwsCredentials::new(
                access_key_id,
                secret_access_key,
                session_token,
                None,
            )),
            (None, None) => Err(AwsCredentialsError::NotFound {
                location: "environment".to_owned(),
            }),
            _ => Err(AwsCredentialsError::Invalid {
                reason: "environment must include both AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY"
                    .to_owned(),
            }),
        }
    }
}

/// Resolves credentials supplied explicitly by configuration.
#[derive(Clone)]
pub struct StaticCredentialsProvider {
    credentials: AwsCredentials,
}

impl StaticCredentialsProvider {
    /// Creates a static credentials provider.
    pub fn new(
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
        session_token: Option<String>,
    ) -> Self {
        Self {
            credentials: AwsCredentials::new(access_key_id, secret_access_key, session_token, None),
        }
    }

    /// Creates a static provider from a prebuilt credentials value.
    pub fn from_credentials(credentials: AwsCredentials) -> Self {
        Self { credentials }
    }
}

impl fmt::Debug for StaticCredentialsProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StaticCredentialsProvider")
            .field("credentials", &self.credentials)
            .finish()
    }
}

#[async_trait]
impl AwsCredentialsProvider for StaticCredentialsProvider {
    async fn resolve(&self) -> Result<AwsCredentials, AwsCredentialsError> {
        Ok(self.credentials.clone())
    }
}

/// Resolves credentials from a minimal AWS shared credentials INI file.
#[derive(Clone, Debug)]
pub struct ProfileCredentialsProvider {
    /// AWS profile section name.
    pub profile: String,
    path: Option<PathBuf>,
}

impl ProfileCredentialsProvider {
    /// Creates a provider for a profile from the default credentials path.
    pub fn new(profile: impl Into<String>) -> Self {
        Self {
            profile: profile.into(),
            path: None,
        }
    }

    /// Creates a provider for a profile from an explicit credentials file path.
    pub fn with_path(profile: impl Into<String>, path: impl Into<PathBuf>) -> Self {
        Self {
            profile: profile.into(),
            path: Some(path.into()),
        }
    }

    fn credentials_path(&self) -> Result<PathBuf, AwsCredentialsError> {
        if let Some(path) = &self.path {
            return Ok(path.clone());
        }

        if let Some(path) = non_empty_env("AWS_SHARED_CREDENTIALS_FILE") {
            return Ok(PathBuf::from(path));
        }

        let Some(home) = non_empty_env("HOME") else {
            return Err(AwsCredentialsError::NotFound {
                location:
                    "AWS shared credentials file requires HOME or AWS_SHARED_CREDENTIALS_FILE"
                        .to_owned(),
            });
        };

        Ok(PathBuf::from(home).join(".aws").join("credentials"))
    }
}

#[async_trait]
impl AwsCredentialsProvider for ProfileCredentialsProvider {
    async fn resolve(&self) -> Result<AwsCredentials, AwsCredentialsError> {
        let path = self.credentials_path()?;
        let contents = match std::fs::read_to_string(&path) {
            Ok(contents) => contents,
            Err(source) if source.kind() == io::ErrorKind::NotFound => {
                return Err(AwsCredentialsError::NotFound {
                    location: format!("profile {} in {}", self.profile, path.display()),
                });
            }
            Err(source) => {
                return Err(AwsCredentialsError::Io {
                    path,
                    reason: source.to_string(),
                });
            }
        };

        let sections = parse_credentials_ini(&contents);
        let profile_section = sections
            .get(&self.profile)
            .or_else(|| sections.get(&format!("profile {}", self.profile)));
        let Some(section) = profile_section else {
            return Err(AwsCredentialsError::NotFound {
                location: format!("profile {}", self.profile),
            });
        };

        let access_key_id = section
            .get("aws_access_key_id")
            .filter(|value| !is_blank(value));
        let secret_access_key = section
            .get("aws_secret_access_key")
            .filter(|value| !is_blank(value));
        let session_token = section
            .get("aws_session_token")
            .or_else(|| section.get("aws_security_token"))
            .filter(|value| !is_blank(value))
            .cloned();

        match (access_key_id, secret_access_key) {
            (Some(access_key_id), Some(secret_access_key)) => Ok(AwsCredentials::new(
                access_key_id.clone(),
                secret_access_key.clone(),
                session_token,
                None,
            )),
            _ => Err(AwsCredentialsError::Invalid {
                reason: format!(
                    "profile {} must include aws_access_key_id and aws_secret_access_key",
                    self.profile
                ),
            }),
        }
    }
}

/// Resolves credentials from an ordered provider chain.
#[derive(Clone, Debug)]
pub struct ChainCredentialsProvider {
    providers: Vec<Arc<dyn AwsCredentialsProvider>>,
}

impl ChainCredentialsProvider {
    /// Creates an ordered credentials provider chain.
    pub fn new(providers: Vec<Arc<dyn AwsCredentialsProvider>>) -> Self {
        Self { providers }
    }

    /// Creates the standard cc-lb AWS chain: environment, explicit config, profile.
    pub fn standard(
        explicit: Option<AwsCredentials>,
        profile: impl Into<String>,
    ) -> Arc<dyn AwsCredentialsProvider> {
        let mut providers: Vec<Arc<dyn AwsCredentialsProvider>> = Vec::new();
        providers.push(Arc::new(EnvCredentialsProvider::new()));
        if let Some(credentials) = explicit {
            providers.push(Arc::new(StaticCredentialsProvider::from_credentials(
                credentials,
            )));
        }
        providers.push(Arc::new(ProfileCredentialsProvider::new(profile)));
        Arc::new(Self::new(providers))
    }
}

#[async_trait]
impl AwsCredentialsProvider for ChainCredentialsProvider {
    async fn resolve(&self) -> Result<AwsCredentials, AwsCredentialsError> {
        for provider in &self.providers {
            match provider.resolve().await {
                Ok(credentials) => return Ok(credentials),
                Err(AwsCredentialsError::NotFound { .. }) => {}
                Err(error) => return Err(error),
            }
        }

        Err(AwsCredentialsError::NotFound {
            location: "credential chain".to_owned(),
        })
    }
}

fn parse_credentials_ini(contents: &str) -> BTreeMap<String, BTreeMap<String, String>> {
    let mut sections = BTreeMap::new();
    let mut current_section: Option<String> = None;

    for raw_line in contents.lines() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') || line.starts_with(';') {
            continue;
        }

        if let Some(section) = line
            .strip_prefix('[')
            .and_then(|rest| rest.strip_suffix(']'))
        {
            let section = section.trim();
            if !section.is_empty() {
                current_section = Some(section.to_owned());
                sections
                    .entry(section.to_owned())
                    .or_insert_with(BTreeMap::new);
            }
            continue;
        }

        let Some(section) = &current_section else {
            continue;
        };
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        let key = key.trim();
        if key.is_empty() {
            continue;
        }
        sections
            .entry(section.clone())
            .or_insert_with(BTreeMap::new)
            .insert(key.to_owned(), value.trim().to_owned());
    }

    sections
}

fn non_empty_env(name: &str) -> Option<String> {
    env::var(name)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn is_blank(value: &str) -> bool {
    value.trim().is_empty()
}
