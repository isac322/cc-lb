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

pub(crate) fn run_without_coverage_instrumentation(test_name: &str, f: impl FnOnce()) {
    let needs_scrubbing = env::var_os("CC_LB_TRYBUILD_SCRUBBED").is_none()
        && (env::var("RUSTFLAGS").is_ok_and(|value| value.contains("instrument-coverage"))
            || env::var("CARGO_ENCODED_RUSTFLAGS")
                .is_ok_and(|value| value.contains("instrument-coverage")));
    if needs_scrubbing {
        let exe = env::current_exe().expect("resolve current test executable");
        let mut cmd = std::process::Command::new(exe);
        cmd.arg("--exact")
            .arg(test_name)
            .arg("--nocapture")
            .env("CC_LB_TRYBUILD_SCRUBBED", "1");
        if let Ok(flags) = env::var("RUSTFLAGS") {
            let scrubbed = scrub_flags(flags.split_whitespace(), " ");
            if scrubbed.is_empty() {
                cmd.env_remove("RUSTFLAGS");
            } else {
                cmd.env("RUSTFLAGS", scrubbed);
            }
        }
        if let Ok(flags) = env::var("CARGO_ENCODED_RUSTFLAGS") {
            let scrubbed = scrub_flags(flags.split('\x1f'), "\x1f");
            if scrubbed.is_empty() {
                cmd.env_remove("CARGO_ENCODED_RUSTFLAGS");
            } else {
                cmd.env("CARGO_ENCODED_RUSTFLAGS", scrubbed);
            }
        }
        let status = cmd.status().expect("spawn scrubbed trybuild subprocess");
        assert!(
            status.success(),
            "scrubbed trybuild subprocess failed: {status}"
        );
        return;
    }

    let _lock = lock();
    f();
}

fn scrub_flags<'a>(flags: impl IntoIterator<Item = &'a str>, separator: &str) -> String {
    let flags = flags.into_iter().collect::<Vec<_>>();
    let mut scrubbed = Vec::with_capacity(flags.len());
    let mut index = 0;
    while index < flags.len() {
        if flags[index] == "-Cinstrument-coverage" {
            index += 1;
        } else if flags[index] == "-C" && flags.get(index + 1) == Some(&"instrument-coverage") {
            index += 2;
        } else {
            scrubbed.push(flags[index]);
            index += 1;
        }
    }
    scrubbed.join(separator)
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
