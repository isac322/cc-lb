//! PostgreSQL storage backend crate for cc-lb.

pub mod adapter;
pub mod config;
pub mod error_map;
pub mod plugin_registry;

pub use adapter::PostgresStorage;
pub use adapter::managed_keys::PostgresManagedKeyStore;
pub use cc_lb_storage_api::RuntimeChangeNotifier;
pub use config::PostgresConfig;
pub use plugin_registry::{PostgresPluginBlobRepo, PostgresPluginRegistryRepo};
