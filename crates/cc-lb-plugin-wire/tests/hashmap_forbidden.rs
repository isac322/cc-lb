use std::fs;
use std::path::{Path, PathBuf};

const FORBIDDEN_SYMBOLS: [&str; 2] = ["HashMap", "HashSet"];

#[test]
fn canonical_and_handshake_modules_do_not_use_hash_collections() {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let source_files = rust_source_files(&manifest_dir.join("src"));
    let target_files: Vec<_> = source_files
        .into_iter()
        .filter(|source_file| is_target_module(source_file))
        .collect();

    assert_eq!(
        target_files.len(),
        2,
        "expected to scan handshake/mod.rs and handshake/canonical.rs"
    );

    for target_file in target_files {
        let content = fs::read_to_string(&target_file)
            .unwrap_or_else(|error| panic!("failed to read {}: {error}", target_file.display()));

        for forbidden_symbol in FORBIDDEN_SYMBOLS {
            assert_no_forbidden_symbol(&target_file, &content, forbidden_symbol);
        }
    }
}

fn rust_source_files(directory: &Path) -> Vec<PathBuf> {
    let mut source_files = Vec::new();
    collect_rust_source_files(directory, &mut source_files);
    source_files
}

fn collect_rust_source_files(directory: &Path, source_files: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(directory)
        .unwrap_or_else(|error| panic!("failed to read directory {}: {error}", directory.display()))
    {
        let path = entry
            .unwrap_or_else(|error| {
                panic!(
                    "failed to read directory entry in {}: {error}",
                    directory.display()
                )
            })
            .path();

        if path.is_dir() {
            collect_rust_source_files(&path, source_files);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            source_files.push(path);
        }
    }
}

fn is_target_module(source_file: &Path) -> bool {
    source_file.ends_with("handshake/canonical.rs") || source_file.ends_with("handshake/mod.rs")
}

fn assert_no_forbidden_symbol(target_file: &Path, content: &str, forbidden_symbol: &str) {
    for (line_index, line) in content.lines().enumerate() {
        assert!(
            !line.contains(forbidden_symbol),
            "{}:{} must not import or use {forbidden_symbol}: {line}",
            target_file.display(),
            line_index + 1,
        );
    }
}
