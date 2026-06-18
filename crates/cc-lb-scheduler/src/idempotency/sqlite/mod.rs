mod claims;
mod cursors;
mod failures;
mod metadata;
mod warmup;

use uuid::Uuid;

pub(super) fn sqlite_uuid(upstream_id: Uuid) -> Vec<u8> {
    upstream_id.as_bytes().to_vec()
}
