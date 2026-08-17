use std::{
    env, fs,
    path::{Path, PathBuf},
};

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    let manifest_dir =
        PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR is required"));
    emit_rerun_tree(&manifest_dir, Path::new("migrations"));
}

fn emit_rerun_tree(manifest_dir: &Path, relative_path: &Path) {
    let path = manifest_dir.join(relative_path);
    println!("cargo:rerun-if-changed={}", relative_path.display());

    if path.is_file() {
        return;
    }
    if !path.is_dir() {
        panic!("Postgres migration input is missing: {}", path.display());
    }

    let mut entries = fs::read_dir(&path)
        .unwrap_or_else(|error| {
            panic!(
                "failed to read Postgres migration directory {}: {error}",
                path.display()
            )
        })
        .map(|entry| {
            entry.unwrap_or_else(|error| {
                panic!(
                    "failed to read an entry in Postgres migration directory {}: {error}",
                    path.display()
                )
            })
        })
        .collect::<Vec<_>>();
    entries.sort_by_key(|entry| entry.file_name());

    for entry in entries {
        emit_rerun_tree(manifest_dir, &relative_path.join(entry.file_name()));
    }
}
