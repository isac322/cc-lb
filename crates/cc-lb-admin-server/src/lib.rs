#![forbid(unsafe_code)]

pub mod app;
pub mod cli;
pub mod split_ports;
pub mod version;

pub use app::{
    AdminApp, AdminBuildError, AdminServeError, build_admin_app_with_storage, run_admin_serve,
};
