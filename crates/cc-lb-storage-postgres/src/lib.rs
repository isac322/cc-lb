//! PostgreSQL storage backend crate for cc-lb.

pub mod adapter;
pub mod config;

pub use adapter::PostgresStorage;
pub use config::PostgresConfig;
