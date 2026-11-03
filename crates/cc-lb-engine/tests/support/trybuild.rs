use std::env;
use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
use std::process::Command;

const TRYBUILD_CHILD_MARKER: &str = "CC_LB_TRYBUILD_CHILD";
const COMPILER_ENVIRONMENT_VARIABLES: [&str; 4] = [
    "RUSTFLAGS",
    "CARGO_ENCODED_RUSTFLAGS",
    "CARGO_BUILD_RUSTC_WRAPPER",
    "RUSTC_WRAPPER",
];

pub(crate) fn run(test_name: &'static str, body: impl FnOnce()) {
    if env::var_os(TRYBUILD_CHILD_MARKER).is_some() {
        let lock_path = target_dir().join("trybuild.lock");
        let lock_file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(lock_path)
            .expect("open shared trybuild lock file");
        lock_file.lock().expect("lock shared trybuild target");
        body();
        return;
    }

    let target_dir = target_dir();
    let executable = env::current_exe().expect("locate integration test executable");
    let mut child = Command::new(executable);
    child
        .args(["--exact", test_name, "--nocapture", "--test-threads=1"])
        .env(TRYBUILD_CHILD_MARKER, "1")
        .env("CARGO_TARGET_DIR", target_dir);
    for variable in COMPILER_ENVIRONMENT_VARIABLES {
        child.env_remove(variable);
    }

    let status = child.status().expect("spawn isolated trybuild child");
    assert!(
        status.success(),
        "isolated trybuild child failed for {test_name}: {status}"
    );
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
