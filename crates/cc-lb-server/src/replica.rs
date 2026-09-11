use std::fs;
use std::io;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use uuid::Uuid;

/// Load or create a stable replica identifier.
///
/// If `${data_dir}/replica_id` exists and is readable as a valid UUID, returns that UUID.
/// If the file is missing or corrupted, generates a new UUID, writes it atomically
/// (tmp file + fsync + rename), sets permissions to 0600, and returns the new UUID.
///
/// # Errors
/// Returns `io::Error` if file operations fail (e.g., permission denied, disk full).
pub fn load_or_create_replica_id(data_dir: &Path) -> io::Result<Uuid> {
    let replica_id_path = data_dir.join("replica_id");

    // Try to read existing replica_id
    if replica_id_path.exists() {
        match fs::read_to_string(&replica_id_path) {
            Ok(content) => {
                let trimmed = content.trim();
                if let Ok(uuid) = Uuid::parse_str(trimmed) {
                    return Ok(uuid);
                }
                // If parsing fails, log warning and treat as missing
                tracing::warn!(
                    path = %replica_id_path.display(),
                    content = %trimmed,
                    "replica_id file exists but contains invalid UUID; regenerating"
                );
            }
            Err(e) => {
                tracing::warn!(
                    path = %replica_id_path.display(),
                    error = %e,
                    "failed to read replica_id file; regenerating"
                );
            }
        }
    }

    // Generate new UUID and write atomically
    let new_uuid = Uuid::new_v4();
    let uuid_string = format!("{}\n", new_uuid);

    // Write to temporary file first
    let temp_path = data_dir.join("replica_id.tmp");
    fs::write(&temp_path, &uuid_string)?;

    // Fsync to ensure durability
    let temp_file = fs::OpenOptions::new().write(true).open(&temp_path)?;
    temp_file.sync_all()?;
    drop(temp_file);

    // Set permissions to 0600 before rename (security: no world-readable UUID)
    fs::set_permissions(&temp_path, fs::Permissions::from_mode(0o600))?;

    // Atomic rename
    fs::rename(&temp_path, &replica_id_path)?;

    Ok(new_uuid)
}

#[cfg(test)]
#[allow(non_snake_case)]
mod tests {
    use super::*;

    #[test]
    fn t3__creates_when_missing() {
        let dir = tempfile::TempDir::new().unwrap();
        let data_dir = dir.path();

        let uuid1 = load_or_create_replica_id(data_dir).unwrap();
        assert!(!uuid1.is_nil());

        let replica_id_path = data_dir.join("replica_id");
        assert!(replica_id_path.exists());
    }

    #[test]
    fn t3__reads_existing() {
        let dir = tempfile::TempDir::new().unwrap();
        let data_dir = dir.path();
        let replica_id_path = data_dir.join("replica_id");

        // Pre-create a replica_id file with a known UUID
        let known_uuid = Uuid::from_u128(1);
        fs::write(&replica_id_path, format!("{}\n", known_uuid)).unwrap();

        let loaded_uuid = load_or_create_replica_id(data_dir).unwrap();
        assert_eq!(loaded_uuid, known_uuid);
    }

    #[test]
    fn t3__corrupt_file_regenerates_and_logs() {
        let dir = tempfile::TempDir::new().unwrap();
        let data_dir = dir.path();
        let replica_id_path = data_dir.join("replica_id");

        // Create a corrupt replica_id file
        fs::write(&replica_id_path, "not-a-valid-uuid\n").unwrap();

        let uuid = load_or_create_replica_id(data_dir).unwrap();
        assert!(!uuid.is_nil());

        // File should now contain valid UUID
        let content = fs::read_to_string(&replica_id_path).unwrap();
        assert!(Uuid::parse_str(content.trim()).is_ok());
    }

    #[test]
    fn t3__permissions_0600() {
        let dir = tempfile::TempDir::new().unwrap();
        let data_dir = dir.path();

        load_or_create_replica_id(data_dir).unwrap();

        let replica_id_path = data_dir.join("replica_id");
        let metadata = fs::metadata(&replica_id_path).unwrap();
        let mode = metadata.permissions().mode();

        // Check that permissions are 0600 (rw-------)
        assert_eq!(mode & 0o777, 0o600);
    }

    #[test]
    fn t3__stable_across_restarts() {
        let dir = tempfile::TempDir::new().unwrap();
        let data_dir = dir.path();

        let uuid1 = load_or_create_replica_id(data_dir).unwrap();
        let uuid2 = load_or_create_replica_id(data_dir).unwrap();

        assert_eq!(uuid1, uuid2);
    }
}
