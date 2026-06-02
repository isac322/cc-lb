extern crate alloc;

use alloc::{string::String, vec::Vec};
use serde::{Deserialize, Serialize};

pub mod canonical;

pub use canonical::{CanonicalError, CanonicalOffer, canonicalize, host_offer_hash};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HandshakeOfferRaw {
    pub handshake_schema_version: u32,
    pub envelope_version: u32,
    pub function_versions: Vec<FunctionVersionOfferRaw>,
    pub host_capabilities: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FunctionVersionOfferRaw {
    pub function: String,
    pub versions: Vec<u32>,
}
