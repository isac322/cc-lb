use std::env;
use std::fs::{File, OpenOptions, create_dir_all};
use std::path::{Path, PathBuf};

// trybuild's heartbeat lock can expire while its owner is descheduled.
// Hold this OS lock until TestCases has dropped and finished its Cargo invocations.
pub(crate) fn lock() -> File {
    let target_dir = target_dir();
    create_dir_all(&target_dir).expect("create shared trybuild target directory");
    let lock_file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(target_dir.join("trybuild.lock"))
        .expect("open shared trybuild lock file");
    lock_file.lock().expect("lock shared trybuild target");
    lock_file
}

fn target_dir() -> PathBuf {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    assert!(
        manifest_dir.is_absolute(),
        "CARGO_MANIFEST_DIR must be absolute"
    );
    let Some(workspace_root) = manifest_dir.parent().and_then(Path::parent) else {
        panic!("CARGO_MANIFEST_DIR must have a workspace root two levels above");
    };

    match env::var_os("CARGO_TARGET_DIR") {
        Some(value) => {
            let target_dir = Path::new(&value);
            if target_dir.is_absolute() {
                target_dir.to_path_buf()
            } else {
                workspace_root.join(target_dir)
            }
        }
        None => workspace_root.join("target"),
    }
}
