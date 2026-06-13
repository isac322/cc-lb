#![forbid(unsafe_code)]

pub mod dispatch;
pub mod handshake;
pub mod host_functions;
pub mod identity;
pub mod self_check;

pub use handshake::{BuildPluginError, build_plugin};
