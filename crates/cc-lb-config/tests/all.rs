#![allow(non_snake_case)]

mod common;

#[path = "hot_reload.rs"]
mod hot_reload;
#[path = "load_minimal.rs"]
mod load_minimal;
#[path = "scheduler_config.rs"]
mod scheduler_config;
#[path = "schema_freshness.rs"]
mod schema_freshness;
#[path = "storage_postgres.rs"]
mod storage_postgres;
#[path = "string_parity.rs"]
mod string_parity;
#[path = "validation_failures.rs"]
mod validation_failures;
#[path = "wasmtime_config.rs"]
mod wasmtime_config;
