//! Wire types + versioning contract between cc-lb host and wasm plugins.
//!
//! Public surface for plugin authors: hook wire types defined per version
//! submodule. `pub use v1::*` at crate root re-exports the current baseline
//! for ergonomic import. New wire versions add new submodules (`pub mod v2;`)
//! without breaking existing code.
//!
//! See `docs/plugin-author-guide.md` and `docs/rfc/0001-plugin-runtime-vnext.md`
//! for the versioning + admission-gate contract.

#![no_std]
#![forbid(unsafe_code)]

extern crate alloc;
extern crate self as cc_lb_plugin_wire;

#[cfg(feature = "std")]
extern crate std;

#[cfg(feature = "std")]
pub mod metadata;
pub mod schema;
pub mod v1;

#[cfg(feature = "std")]
pub use metadata::{HookMetadata, MetadataError, PluginMetadata};
pub use schema::*;
pub use v1::*;
