#![forbid(unsafe_code)]
//! Shared runtime plumbing across `cc-lb` (standalone), `cc-lb-proxy`, and `cc-lb-admin`:
//! AEAD key decoding, build metadata, replica identity, storage/scheduler opening,
//! shared admin HTTP layers, and Postgres live-tail fanout/polling.

pub mod admin_http;
pub mod aead_key;
pub mod build_meta;
pub mod live_tail;
pub mod replica;
pub mod runtime_utils;
pub mod storage_factory;
pub mod wasmtime_builder;

pub use runtime_utils::{
    await_or_abort, is_container_environment, resolve_data_dir, warn_if_container_loopback_bind,
};
