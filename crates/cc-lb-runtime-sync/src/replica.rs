use std::fs;
use std::io::{self, Write as _};
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use uuid::Uuid;

fn read_existing_replica_id(replica_id_path: &Path) -> (bool, Option<Uuid>) {
    match fs::read_to_string(replica_id_path) {
        Ok(content) => {
            let trimmed = content.trim();
            if let Ok(uuid) = Uuid::parse_str(trimmed) {
                return (true, Some(uuid));
            }
            tracing::warn!(
                path = %replica_id_path.display(),
                content = %trimmed,
                "replica_id file exists but contains invalid UUID; regenerating"
            );
            (true, None)
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => (false, None),
        Err(e) => {
            tracing::warn!(
                path = %replica_id_path.display(),
                error = %e,
                "failed to read replica_id file; regenerating"
            );
            (true, None)
        }
    }
}

fn sync_dir_best_effort(data_dir: &Path) {
    if let Ok(dir) = fs::File::open(data_dir) {
        let _ = dir.sync_all();
    }
}

/// Load or create a stable replica identifier.
///
/// If `${data_dir}/replica_id` exists and is readable as a valid UUID, returns that UUID.
/// If the file is missing or corrupted, generates a new UUID, writes it atomically via a unique
/// `NamedTempFile` (`0o600`, `sync_all`, non-clobbering persist when creating, atomic rename when
/// replacing corrupt contents), and returns the active UUID.
///
/// # Errors
/// Returns `io::Error` if file operations fail (e.g., permission denied, disk full).
pub fn load_or_create_replica_id(data_dir: &Path) -> io::Result<Uuid> {
    let replica_id_path = data_dir.join("replica_id");
    let (file_existed, existing_uuid) = read_existing_replica_id(&replica_id_path);
    if let Some(uuid) = existing_uuid {
        return Ok(uuid);
    }

    let new_uuid = Uuid::new_v4();
    let uuid_string = format!("{new_uuid}\n");

    let mut temp_file = tempfile::NamedTempFile::new_in(data_dir)?;
    temp_file
        .as_file()
        .set_permissions(fs::Permissions::from_mode(0o600))?;
    temp_file.write_all(uuid_string.as_bytes())?;
    temp_file.flush()?;
    temp_file.as_file().sync_all()?;

    if !file_existed {
        match temp_file.persist_noclobber(&replica_id_path) {
            Ok(_) => {
                sync_dir_best_effort(data_dir);
                return Ok(new_uuid);
            }
            Err(err) if err.error.kind() == io::ErrorKind::AlreadyExists => {
                let (_, raced_uuid) = read_existing_replica_id(&replica_id_path);
                if let Some(uuid) = raced_uuid {
                    return Ok(uuid);
                }
                err.file
                    .persist(&replica_id_path)
                    .map_err(|persist_err| persist_err.error)?;
                sync_dir_best_effort(data_dir);
                return Ok(new_uuid);
            }
            Err(err) => return Err(err.error),
        }
    }

    temp_file
        .persist(&replica_id_path)
        .map_err(|err| err.error)?;
    sync_dir_best_effort(data_dir);

    Ok(new_uuid)
}

/// Load or create a persistent replica ID, falling back to an ephemeral UUID on I/O failure.
///
/// Returning a unique ephemeral UUID instead of `None` keeps OAuth refresh locking and lazy
/// refresh active when `data_dir` is read-only or temporarily unwritable.
pub fn load_or_create_replica_id_or_ephemeral(data_dir: &Path) -> Uuid {
    match load_or_create_replica_id(data_dir) {
        Ok(id) => id,
        Err(error) => {
            let ephemeral = Uuid::new_v4();
            tracing::warn!(
                path = %data_dir.display(),
                error = %error,
                ephemeral_replica_id = %ephemeral,
                "failed to load or persist replica_id; using ephemeral replica UUID"
            );
            ephemeral
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_when_missing() {
        let dir = tempfile::TempDir::new().unwrap();
        let data_dir = dir.path();

        let uuid1 = load_or_create_replica_id(data_dir).unwrap();
        assert!(!uuid1.is_nil());

        let replica_id_path = data_dir.join("replica_id");
        assert!(replica_id_path.exists());
    }

    #[test]
    fn reads_existing() {
        let dir = tempfile::TempDir::new().unwrap();
        let data_dir = dir.path();
        let replica_id_path = data_dir.join("replica_id");

        // Pre-create a replica_id file with a known UUID
        let known_uuid = Uuid::new_v4();
        fs::write(&replica_id_path, format!("{}\n", known_uuid)).unwrap();

        let loaded_uuid = load_or_create_replica_id(data_dir).unwrap();
        assert_eq!(loaded_uuid, known_uuid);
    }

    #[test]
    fn corrupt_file_regenerates_and_logs() {
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
    fn permissions_0600() {
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
    fn stable_across_restarts() {
        let dir = tempfile::TempDir::new().unwrap();
        let data_dir = dir.path();

        let uuid1 = load_or_create_replica_id(data_dir).unwrap();
        let uuid2 = load_or_create_replica_id(data_dir).unwrap();

        assert_eq!(uuid1, uuid2);
    }

    #[test]
    fn concurrent_initial_creation_converges_to_single_uuid() {
        let dir = tempfile::TempDir::new().unwrap();
        let data_dir = dir.path().to_path_buf();
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(8));
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let data_dir = data_dir.clone();
                let barrier = std::sync::Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    load_or_create_replica_id(&data_dir).expect("concurrent replica_id load")
                })
            })
            .collect();
        let ids: Vec<Uuid> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        assert!(ids.iter().all(|id| *id == ids[0] && !id.is_nil()));
    }

    #[test]
    fn unwritable_dir_falls_back_to_ephemeral_uuid() {
        let dir = tempfile::TempDir::new().unwrap();
        let missing = dir.path().join("nonexistent-subdir");
        let id1 = load_or_create_replica_id_or_ephemeral(&missing);
        let id2 = load_or_create_replica_id_or_ephemeral(&missing);
        assert!(!id1.is_nil());
        assert!(!id2.is_nil());
        assert_ne!(id1, id2);
    }
}
