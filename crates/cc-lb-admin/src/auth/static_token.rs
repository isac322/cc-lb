use async_trait::async_trait;
use http::{HeaderMap, header};
use subtle::ConstantTimeEq;

use super::{AdminActorKind, AdminAuthProvider, AdminIdentity, ProviderOutcome};

pub(crate) struct StaticTokenProvider {
    id: String,
    token: String,
}

impl StaticTokenProvider {
    pub(crate) fn new(id: impl Into<String>, token: String) -> Self {
        Self {
            id: id.into(),
            token,
        }
    }
}

#[async_trait]
impl AdminAuthProvider for StaticTokenProvider {
    fn id(&self) -> &str {
        &self.id
    }

    fn is_static_token(&self) -> bool {
        true
    }

    async fn authenticate(&self, headers: &HeaderMap) -> ProviderOutcome {
        let Some(token) = headers
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
        else {
            return ProviderOutcome::NotPresent;
        };

        if !bool::from(token.as_bytes().ct_eq(self.token.as_bytes())) {
            return ProviderOutcome::Rejected("static_token_mismatch");
        }

        ProviderOutcome::Verified(AdminIdentity {
            authority: "static-token".to_owned(),
            subject: self.id.clone(),
            kind: AdminActorKind::BreakGlass,
            provider_id: self.id.clone(),
            email: None,
            display_name: Some("Shared admin token".to_owned()),
            groups: Vec::new(),
            expires_at_unix_secs: None,
        })
    }
}
