#![forbid(unsafe_code)]

pub mod modes;
pub mod oauth;
pub mod routes;
pub mod sse;

pub use routes::{AppConfig, app};
