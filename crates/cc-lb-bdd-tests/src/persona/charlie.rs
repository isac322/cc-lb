//! Charlie — SRE persona. Admin token plus operational tooling for
//! incident response, drain, multi-replica, backend parity, and
//! warmup-lease scenarios (W2 plus parts of W4). Stubbed in M2.

use crate::backend::StorageHandle;

pub struct Charlie {
    #[allow(dead_code)]
    storage: StorageHandle,
}

impl Charlie {
    pub(crate) fn new(storage: StorageHandle) -> Self {
        Self { storage }
    }
}
