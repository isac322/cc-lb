//! Dana — auditor persona. Read-only token for audit log, redaction,
//! and observability scenarios (W4). Stubbed in M2.

use crate::backend::StorageHandle;

pub struct Dana {
    #[allow(dead_code)]
    storage: StorageHandle,
}

impl Dana {
    pub(crate) fn new(storage: StorageHandle) -> Self {
        Self { storage }
    }
}
