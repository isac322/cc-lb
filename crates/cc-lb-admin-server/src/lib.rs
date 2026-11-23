#![forbid(unsafe_code)]

pub mod app;
pub mod cli;
pub mod version;

pub use app::{
    AdminApp, AdminBuildError, AdminServeError, build_admin_app, build_admin_app_with_path,
    build_admin_app_with_storage, run_admin_serve,
};
