use serde::{Deserialize, Serialize};

/// Vertex AI target endpoint shape.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum VertexTarget {
    /// Express endpoint without project or location.
    Express,
    /// Regional Vertex endpoint.
    Regional {
        /// Google Cloud project identifier or number.
        project_id: Box<str>,
        /// Google Cloud location.
        location: Box<str>,
    },
}

/// Vertex API version used for the upstream request path.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum VertexApiVersion {
    /// Stable Vertex API.
    V1,
    /// Preview Vertex API (the default for compatibility).
    #[default]
    V1Beta1,
}

impl VertexApiVersion {
    /// Returns the API path segment for this version.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::V1 => "v1",
            Self::V1Beta1 => "v1beta1",
        }
    }
}

/// Credential format used by a Vertex upstream.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VertexCredentialType {
    /// A Google service-account credential.
    ServiceAccount,
    /// A Google authorized-user credential.
    AuthorizedUser,
    /// A service-account credential used through impersonation.
    ImpersonatedServiceAccount,
}

impl VertexCredentialType {
    /// Returns the serialized credential type name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ServiceAccount => "service_account",
            Self::AuthorizedUser => "authorized_user",
            Self::ImpersonatedServiceAccount => "impersonated_service_account",
        }
    }

    /// Parses a serialized credential type name.
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "service_account" => Some(Self::ServiceAccount),
            "authorized_user" => Some(Self::AuthorizedUser),
            "impersonated_service_account" => Some(Self::ImpersonatedServiceAccount),
            _ => None,
        }
    }
}

/// Persisted Vertex-specific upstream configuration.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct VertexUpstreamConfig {
    /// Vertex endpoint target.
    pub target: VertexTarget,
    /// API version used for requests.
    #[serde(default)]
    pub api_version: VertexApiVersion,
}

impl VertexUpstreamConfig {
    /// Validates this configuration against whether a credential is configured.
    pub fn validate(&self, credential: bool) -> Result<(), VertexValidationError> {
        match (&self.target, credential) {
            (VertexTarget::Express, true) => Err(VertexValidationError::CredentialRequiresRegional),
            (VertexTarget::Express, false) => Ok(()),
            (
                VertexTarget::Regional {
                    project_id,
                    location,
                },
                _,
            ) => {
                validate_project_id(project_id)?;
                validate_location(location)
            }
        }
    }
}

/// Validation errors for Vertex endpoint configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VertexValidationError {
    /// Project identifier is invalid.
    InvalidProjectId,
    /// Location is invalid.
    InvalidLocation,
    /// A credential requires a regional endpoint.
    CredentialRequiresRegional,
}
/// Validates a Google Cloud project identifier or project number.
pub fn validate_project_id(value: &str) -> Result<(), VertexValidationError> {
    let valid_id = (6..=30).contains(&value.len())
        && value.as_bytes().first().is_some_and(u8::is_ascii_lowercase)
        && value
            .as_bytes()
            .last()
            .is_some_and(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
        && value
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-');
    let valid_number = (6..=20).contains(&value.len()) && value.bytes().all(|b| b.is_ascii_digit());
    if valid_id || valid_number {
        Ok(())
    } else {
        Err(VertexValidationError::InvalidProjectId)
    }
}

/// Validates a Google Cloud region name.
pub fn validate_location(value: &str) -> Result<(), VertexValidationError> {
    if value.is_empty()
        || value.len() > 32
        || !value
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        || value
            .as_bytes()
            .first()
            .is_some_and(|b| !b.is_ascii_lowercase())
        || value
            .as_bytes()
            .last()
            .is_some_and(|b| !b.is_ascii_lowercase() && !b.is_ascii_digit())
    {
        return Err(VertexValidationError::InvalidLocation);
    }
    Ok(())
}

/// Returns the origin URL for a Vertex target.
pub fn origin(target: &VertexTarget) -> String {
    match target {
        VertexTarget::Express => "https://aiplatform.googleapis.com".to_owned(),
        VertexTarget::Regional { location, .. } => match location.as_ref() {
            "global" => "https://aiplatform.googleapis.com".to_owned(),
            "us" | "eu" => format!("https://aiplatform.{location}.rep.googleapis.com"),
            location => format!("https://{location}-aiplatform.googleapis.com"),
        },
    }
}

/// Error reason indicating that the credential grant is invalid.
pub const VERTEX_CREDENTIAL_INVALID_GRANT: &str = "vertex_credential_invalid_grant";
/// Error reason indicating that the credential client is invalid.
pub const VERTEX_CREDENTIAL_INVALID_CLIENT: &str = "vertex_credential_invalid_client";

/// Returns whether an authentication failure requires replacing the credential.
pub fn vertex_credential_requires_replacement(reason: Option<&str>) -> bool {
    matches!(
        reason,
        Some(VERTEX_CREDENTIAL_INVALID_GRANT | VERTEX_CREDENTIAL_INVALID_CLIENT)
    )
}

#[cfg(test)]
mod tests {
    use super::{
        VertexApiVersion, VertexCredentialType, VertexTarget, VertexUpstreamConfig,
        VertexValidationError, validate_location, validate_project_id,
    };

    #[test]
    fn qa_sto_3_vertex_validators_accept_positive_location_list_and_reject_hostile_values() {
        for location in ["global", "us", "eu", "us-central1", "europe-west4"] {
            assert_eq!(validate_location(location), Ok(()), "{location}");
        }
        for location in [
            "",
            "US-central1",
            "us.central1",
            "us/central1",
            "us:central1",
        ] {
            assert_eq!(
                validate_location(location),
                Err(VertexValidationError::InvalidLocation),
                "{location}"
            );
        }
        for project in ["my-project-123", "123456789012"] {
            assert_eq!(validate_project_id(project), Ok(()), "{project}");
        }
        for project in ["My-project-123", "my.project", "my/project", "short"] {
            assert_eq!(
                validate_project_id(project),
                Err(VertexValidationError::InvalidProjectId),
                "{project}"
            );
        }
        let express = VertexUpstreamConfig {
            target: VertexTarget::Express,
            api_version: VertexApiVersion::V1Beta1,
        };
        assert_eq!(
            express.validate(false),
            Ok(()),
            "API-key Express config is valid"
        );
        assert_eq!(
            express.validate(true),
            Err(VertexValidationError::CredentialRequiresRegional)
        );
        let regional = VertexUpstreamConfig {
            target: VertexTarget::Regional {
                project_id: "my-project-123".into(),
                location: "global".into(),
            },
            api_version: VertexApiVersion::V1,
        };
        assert_eq!(regional.validate(true), Ok(()));
        let _ = VertexCredentialType::ServiceAccount;
    }
}
