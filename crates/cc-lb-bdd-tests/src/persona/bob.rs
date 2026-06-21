//! Bob — developer / plugin author persona. Carries a per-principal
//! API key and exercises the developer-facing flows (W3 scenarios
//! plus the developer-side of W1 traffic scenarios). Bob's surface is
//! intentionally stubbed in M2; the full client lands as scenarios
//! that need it come online.

use crate::backend::StorageHandle;

pub struct Bob {
    #[allow(dead_code)]
    storage: StorageHandle,
}

impl Bob {
    pub(crate) fn new(storage: StorageHandle) -> Self {
        Self { storage }
    }
}
