use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;

use cc_lb_domain::{Provider, UpstreamKind};
use thiserror::Error;

use crate::{AuthKindDescriptor, AuthKindId, ProviderAdapter, ProviderId};

/// Errors produced when registering a [`ProviderAdapter`] in [`ProviderRegistry`].
#[derive(Debug, Error)]
pub enum ProviderRegistryError {
    /// A provider with the same [`ProviderId`] has already been registered.
    #[error("duplicate provider registration for `{provider_id}`")]
    DuplicateProvider {
        /// Conflicting provider identifier.
        provider_id: ProviderId,
    },
    /// An [`AuthKindId`] conflicts with an existing descriptor in the same adapter or another adapter.
    #[error(
        "duplicate auth kind `{auth_kind_id}` (existing provider `{existing_provider_id}`, new provider `{new_provider_id}`)"
    )]
    DuplicateAuthKind {
        /// Conflicting auth kind identifier.
        auth_kind_id: AuthKindId,
        /// Provider that already owns the auth kind.
        existing_provider_id: ProviderId,
        /// Provider attempting to register the duplicate auth kind.
        new_provider_id: ProviderId,
    },
    /// One of an auth kind's configured default URLs failed to parse.
    #[error("invalid {kind} base URL `{url}` for auth kind `{auth_kind_id}`: {source}")]
    InvalidBaseUrl {
        /// Auth kind whose descriptor carried the invalid URL.
        auth_kind_id: AuthKindId,
        /// Which URL slot failed (`default`, `control`, or `preflight`).
        kind: &'static str,
        /// Raw invalid URL string.
        url: String,
        /// Underlying URL parse error.
        #[source]
        source: url::ParseError,
    },
}

struct RegisteredAuthKind {
    adapter_idx: usize,
    descriptor: AuthKindDescriptor,
}

/// In-memory registry mapping [`ProviderId`] and [`AuthKindId`] values to [`ProviderAdapter`] implementations.
#[derive(Default)]
pub struct ProviderRegistry {
    adapters: Vec<Arc<dyn ProviderAdapter>>,
    by_provider: HashMap<ProviderId, usize>,
    by_auth_kind: HashMap<AuthKindId, RegisteredAuthKind>,
}

impl fmt::Debug for ProviderRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let provider_ids: Vec<ProviderId> = self
            .adapters
            .iter()
            .map(|adapter| adapter.provider_id())
            .collect();
        f.debug_struct("ProviderRegistry")
            .field("providers", &provider_ids)
            .finish()
    }
}

impl ProviderRegistry {
    /// Creates an empty [`ProviderRegistry`].
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers an isolated [`ProviderAdapter`], validating provider ID, auth kinds, and default URLs.
    pub fn register(
        &mut self,
        adapter: Arc<dyn ProviderAdapter>,
    ) -> Result<&mut Self, ProviderRegistryError> {
        let provider_id = adapter.provider_id();
        if self.by_provider.contains_key(&provider_id) {
            return Err(ProviderRegistryError::DuplicateProvider { provider_id });
        }

        let auth_kinds = adapter.auth_kinds();
        let mut seen_in_adapter = HashSet::with_capacity(auth_kinds.len());
        for descriptor in auth_kinds {
            if !seen_in_adapter.insert(descriptor.id.clone()) {
                return Err(ProviderRegistryError::DuplicateAuthKind {
                    auth_kind_id: descriptor.id.clone(),
                    existing_provider_id: provider_id.clone(),
                    new_provider_id: provider_id,
                });
            }
            if let Some(existing) = self.by_auth_kind.get(&descriptor.id) {
                return Err(ProviderRegistryError::DuplicateAuthKind {
                    auth_kind_id: descriptor.id.clone(),
                    existing_provider_id: self.adapters[existing.adapter_idx].provider_id(),
                    new_provider_id: provider_id,
                });
            }
            if let Err(source) = descriptor.default_base_url_parsed() {
                return Err(ProviderRegistryError::InvalidBaseUrl {
                    auth_kind_id: descriptor.id.clone(),
                    kind: "default",
                    url: descriptor.default_base_url.to_string(),
                    source,
                });
            }
            if let Some(control) = descriptor.default_control_base_url.as_ref()
                && let Err(source) = url::Url::parse(control.as_ref())
            {
                return Err(ProviderRegistryError::InvalidBaseUrl {
                    auth_kind_id: descriptor.id.clone(),
                    kind: "control",
                    url: control.to_string(),
                    source,
                });
            }
            if let Some(preflight) = descriptor.default_preflight_base_url.as_ref()
                && let Err(source) = url::Url::parse(preflight.as_ref())
            {
                return Err(ProviderRegistryError::InvalidBaseUrl {
                    auth_kind_id: descriptor.id.clone(),
                    kind: "preflight",
                    url: preflight.to_string(),
                    source,
                });
            }
        }

        let adapter_idx = self.adapters.len();
        for descriptor in auth_kinds {
            self.by_auth_kind.insert(
                descriptor.id.clone(),
                RegisteredAuthKind {
                    adapter_idx,
                    descriptor: descriptor.clone(),
                },
            );
        }
        self.by_provider.insert(provider_id, adapter_idx);
        self.adapters.push(adapter);
        Ok(self)
    }

    /// Looks up a registered adapter by [`ProviderId`].
    pub fn get(&self, provider_id: &ProviderId) -> Option<&Arc<dyn ProviderAdapter>> {
        self.by_provider
            .get(provider_id)
            .map(|&idx| &self.adapters[idx])
    }

    /// Looks up the primary registered adapter for a wire [`Provider`] enum value.
    pub fn for_wire_provider(&self, provider: Provider) -> Option<&Arc<dyn ProviderAdapter>> {
        self.get(&ProviderId::from(provider)).or_else(|| {
            self.adapters
                .iter()
                .find(|adapter| adapter.wire_provider() == Some(provider))
        })
    }

    /// Looks up the owning [`ProviderAdapter`] for an [`AuthKindId`].
    pub fn adapter_for_auth_kind(
        &self,
        auth_kind_id: &AuthKindId,
    ) -> Option<&Arc<dyn ProviderAdapter>> {
        self.by_auth_kind
            .get(auth_kind_id)
            .map(|entry| &self.adapters[entry.adapter_idx])
    }

    /// Looks up an [`AuthKindDescriptor`] by [`AuthKindId`].
    pub fn auth_kind(&self, auth_kind_id: &AuthKindId) -> Option<&AuthKindDescriptor> {
        self.by_auth_kind
            .get(auth_kind_id)
            .map(|entry| &entry.descriptor)
    }

    /// Looks up the [`AuthKindDescriptor`] matching a wire [`UpstreamKind`].
    pub fn auth_kind_for_upstream_kind(&self, kind: UpstreamKind) -> Option<&AuthKindDescriptor> {
        self.auth_kind(&AuthKindId::from(kind))
    }

    /// Resolves a model identifier via the registered adapter for `provider_id`, returning
    /// `requested` unchanged if the provider is not registered.
    pub fn canonical_model_id<'a>(&self, provider_id: &ProviderId, requested: &'a str) -> &'a str {
        match self.get(provider_id) {
            Some(adapter) => adapter.canonical_model_id(requested),
            None => requested,
        }
    }

    /// Resolves the minimum cacheable prefix threshold in tokens across registered providers.
    pub fn cache_threshold_tokens(&self, canonical: &str) -> Option<usize> {
        self.adapters
            .iter()
            .find_map(|adapter| adapter.cache_threshold_tokens(canonical))
    }

    /// Iterates over all registered provider adapters in registration order.
    pub fn providers(&self) -> impl Iterator<Item = &Arc<dyn ProviderAdapter>> {
        self.adapters.iter()
    }

    /// Returns the number of registered provider adapters.
    pub fn len(&self) -> usize {
        self.adapters.len()
    }

    /// Returns `true` when no provider adapters are registered.
    pub fn is_empty(&self) -> bool {
        self.adapters.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::sync::Arc;

    use bytes::Bytes;
    use cc_lb_domain::{CredentialKind, Provider, UpstreamKind};

    use super::{ProviderRegistry, ProviderRegistryError};
    use crate::{
        AuthKindDescriptor, AuthKindId, AuthMode, ErrorFormatRequest, ProviderAdapter, ProviderId,
    };

    struct TestAdapter {
        id: ProviderId,
        kinds: Vec<AuthKindDescriptor>,
    }

    impl ProviderAdapter for TestAdapter {
        fn provider_id(&self) -> ProviderId {
            self.id.clone()
        }

        fn display_name(&self) -> &str {
            self.id.as_str()
        }

        fn wire_provider(&self) -> Option<Provider> {
            (self.id == ProviderId::ANTHROPIC).then_some(Provider::Anthropic)
        }

        fn auth_kinds(&self) -> &[AuthKindDescriptor] {
            &self.kinds
        }

        fn format_error_body(&self, _req: ErrorFormatRequest<'_>) -> Bytes {
            Bytes::from_static(b"{}")
        }
    }

    fn descriptor(id: AuthKindId, url: &'static str) -> AuthKindDescriptor {
        AuthKindDescriptor {
            id,
            display_name: Cow::Borrowed("Test"),
            auth_mode: AuthMode::ApiKey,
            metric_label: Cow::Borrowed("test_key"),
            default_base_url: Cow::Borrowed(url),
            default_control_base_url: None,
            default_preflight_base_url: None,
            requires_vertex_config: false,
        }
    }

    #[test]
    fn wire_conversions_preserve_canonical_ids() {
        assert_eq!(ProviderId::from(Provider::Anthropic), ProviderId::ANTHROPIC);
        assert_eq!(ProviderId::from(Provider::Openai), ProviderId::OPENAI);
        assert_eq!(ProviderId::from(Provider::Gemini), ProviderId::GEMINI);

        assert_eq!(
            AuthKindId::from(UpstreamKind::AnthropicApiKey),
            AuthKindId::ANTHROPIC_API_KEY
        );
        assert_eq!(
            AuthKindId::from(UpstreamKind::AnthropicOauth),
            AuthKindId::ANTHROPIC_OAUTH
        );
        assert_eq!(
            AuthKindId::from(UpstreamKind::OpenaiApiKey),
            AuthKindId::OPENAI_API_KEY
        );
        assert_eq!(
            AuthKindId::from(UpstreamKind::OpenaiOauth),
            AuthKindId::OPENAI_OAUTH
        );
        assert_eq!(
            AuthKindId::from(UpstreamKind::GeminiApiKey),
            AuthKindId::GEMINI_API_KEY
        );
        assert_eq!(
            AuthKindId::from(UpstreamKind::GeminiOauth),
            AuthKindId::GEMINI_OAUTH
        );
        assert_eq!(
            AuthKindId::from(UpstreamKind::VertexApiKey),
            AuthKindId::VERTEX_API_KEY
        );
        assert_eq!(
            AuthKindId::from(UpstreamKind::VertexCredential),
            AuthKindId::VERTEX_CREDENTIAL
        );
    }

    #[test]
    fn auth_mode_credential_kind_mapping() {
        assert_eq!(AuthMode::ApiKey.credential_kind(), CredentialKind::ApiKey);
        assert_eq!(
            AuthMode::ServiceCredential.credential_kind(),
            CredentialKind::ApiKey
        );
        assert_eq!(AuthMode::Oauth.credential_kind(), CredentialKind::Oauth);
    }

    #[test]
    fn registry_rejects_duplicate_provider_auth_kind_and_bad_url() {
        let mut registry = ProviderRegistry::new();
        registry
            .register(Arc::new(TestAdapter {
                id: ProviderId::ANTHROPIC,
                kinds: vec![descriptor(
                    AuthKindId::ANTHROPIC_API_KEY,
                    "https://api.anthropic.com/",
                )],
            }))
            .expect("first register succeeds");

        let dup_provider = registry
            .register(Arc::new(TestAdapter {
                id: ProviderId::ANTHROPIC,
                kinds: vec![descriptor(
                    AuthKindId::from_static("other"),
                    "https://example.com/",
                )],
            }))
            .unwrap_err();
        assert!(matches!(
            dup_provider,
            ProviderRegistryError::DuplicateProvider { .. }
        ));

        let dup_auth = registry
            .register(Arc::new(TestAdapter {
                id: ProviderId::OPENAI,
                kinds: vec![descriptor(
                    AuthKindId::ANTHROPIC_API_KEY,
                    "https://api.openai.com/",
                )],
            }))
            .unwrap_err();
        assert!(matches!(
            dup_auth,
            ProviderRegistryError::DuplicateAuthKind { .. }
        ));

        let bad_url = registry
            .register(Arc::new(TestAdapter {
                id: ProviderId::GEMINI,
                kinds: vec![descriptor(AuthKindId::GEMINI_API_KEY, "not a url")],
            }))
            .unwrap_err();
        assert!(matches!(
            bad_url,
            ProviderRegistryError::InvalidBaseUrl { .. }
        ));
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn registry_snapshots_validated_descriptors_against_dynamic_auth_kinds_slice() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct DynamicKindsAdapter {
            mode: Arc<AtomicUsize>,
            initial: [AuthKindDescriptor; 2],
            reordered: [AuthKindDescriptor; 2],
        }

        impl ProviderAdapter for DynamicKindsAdapter {
            fn provider_id(&self) -> ProviderId {
                ProviderId::from_static("dynamic")
            }

            fn display_name(&self) -> &str {
                "Dynamic"
            }

            fn wire_provider(&self) -> Option<Provider> {
                None
            }

            fn auth_kinds(&self) -> &[AuthKindDescriptor] {
                match self.mode.load(Ordering::SeqCst) {
                    0 => &self.initial,
                    1 => &self.reordered,
                    _ => &[],
                }
            }

            fn format_error_body(&self, _req: ErrorFormatRequest<'_>) -> Bytes {
                Bytes::from_static(b"{}")
            }
        }

        let kind_a = AuthKindId::from_static("dynamic_a");
        let kind_b = AuthKindId::from_static("dynamic_b");
        let mode = Arc::new(AtomicUsize::new(0));
        let mut desc_a = descriptor(kind_a.clone(), "https://a.example.com/");
        desc_a.metric_label = Cow::Borrowed("metric_a");
        let mut desc_b = descriptor(kind_b.clone(), "https://b.example.com/");
        desc_b.metric_label = Cow::Borrowed("metric_b");

        let adapter = DynamicKindsAdapter {
            mode: Arc::clone(&mode),
            initial: [desc_a.clone(), desc_b.clone()],
            reordered: [desc_b, desc_a],
        };

        let mut registry = ProviderRegistry::new();
        registry.register(Arc::new(adapter)).expect("register");

        // Switch adapter slice order to reversed, then empty: lookups must still
        // return the original validated descriptors without swapping or panicking.
        for next_mode in [1usize, 2usize] {
            mode.store(next_mode, Ordering::SeqCst);
            let got_a = registry.auth_kind(&kind_a).expect("kind_a present");
            assert_eq!(got_a.default_base_url, "https://a.example.com/");
            assert_eq!(got_a.metric_label, "metric_a");
            let got_b = registry.auth_kind(&kind_b).expect("kind_b present");
            assert_eq!(got_b.default_base_url, "https://b.example.com/");
            assert_eq!(got_b.metric_label, "metric_b");
        }
    }
}
