mod compat_and_catalog;
mod cursors;

use uuid::Uuid;

pub(super) fn sqlite_uuid(upstream_id: Uuid) -> Vec<u8> {
    upstream_id.as_bytes().to_vec()
}
