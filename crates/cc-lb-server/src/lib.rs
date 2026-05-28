#![forbid(unsafe_code)]

// ## Exit Code Reference (for runbook/operators)
//
// 1: generic fatal / unhandled error
// 2: storage backend requires a cargo feature that is not compiled in (FeatureDisabled)
// 3: backend kind mismatch — stored kind ≠ configured kind (BackendKindMismatch)
// 4: storage connection failed (ConnectionFailed)
// 5: storage initialization failed (InitFailed)

pub mod app;
pub mod bootstrap;
pub mod build_meta;
pub mod builtins;
pub mod chaos;
pub mod cli;
pub mod drain;
pub mod dynamic_view_builder;
pub mod notify_listener;
pub mod preflight;
pub mod reconcile;
pub mod refresh;
pub mod reload;
pub mod replica;
pub(crate) mod revision_hash;
pub mod signal;
pub mod storage_factory;
pub mod tls;
pub mod validate;
pub mod version;

pub use app::{App, BuildError, build_app, build_app_with_path, run_serve};
