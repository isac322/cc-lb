//! Built-in composition of isolated provider adapter crates into [`ProviderRegistry`].

#![deny(unsafe_code)]
#![warn(missing_docs)]

use std::sync::{Arc, OnceLock};

use cc_lb_provider_anthropic::AnthropicProviderAdapter;
use cc_lb_provider_gemini::GeminiProviderAdapter;
use cc_lb_provider_openai::OpenaiProviderAdapter;
use cc_lb_provider_spi::ProviderRegistry;
use cc_lb_provider_vertex::VertexProviderAdapter;

static BUILTIN_REGISTRY: OnceLock<ProviderRegistry> = OnceLock::new();

/// Builds a fresh [`ProviderRegistry`] pre-populated with all built-in provider adapters.
pub fn build_builtin_provider_registry() -> ProviderRegistry {
    let mut registry = ProviderRegistry::new();
    registry
        .register(Arc::new(AnthropicProviderAdapter))
        .expect("anthropic adapter registers cleanly");
    registry
        .register(Arc::new(OpenaiProviderAdapter))
        .expect("openai adapter registers cleanly");
    registry
        .register(Arc::new(GeminiProviderAdapter))
        .expect("gemini adapter registers cleanly");
    registry
        .register(Arc::new(VertexProviderAdapter))
        .expect("vertex adapter registers cleanly");
    registry
}

/// Returns the shared process-wide built-in [`ProviderRegistry`].
pub fn builtin_provider_registry() -> &'static ProviderRegistry {
    BUILTIN_REGISTRY.get_or_init(build_builtin_provider_registry)
}
