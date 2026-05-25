#![forbid(unsafe_code)]

//! AEAD service primitives for the multi-database backend.
//!
//! This crate provides the AEAD service boundary above storage so future
//! implementations can encrypt and decrypt opaque ciphertext without coupling
//! backends to crypto details.

pub mod error;
pub mod service;

pub use error::{AeadError, AeadResult};
pub use service::AeadService;
