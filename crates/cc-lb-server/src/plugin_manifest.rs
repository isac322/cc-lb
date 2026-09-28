/// Plugin manifest passed to runtime adapters when instantiating plugins.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct PluginManifest {
    /// Plugin name from configuration.
    pub(crate) name: String,
    /// Filesystem path or runtime-specific locator for the plugin artifact.
    pub(crate) artifact: String,
}
