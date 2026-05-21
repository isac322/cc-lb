use std::{collections::BTreeSet, path::PathBuf, process::Command};

#[test]
fn workspace_contains_all_expected_crates() {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let expected_path = manifest_dir.join("tests/scaffold/expected-crates.txt");
    let workspace_root = manifest_dir;

    let expected = std::fs::read_to_string(expected_path).expect("expected crate list");
    let expected: BTreeSet<_> = expected
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_owned)
        .collect();

    let output = Command::new("cargo")
        .args(["metadata", "--no-deps", "--format-version", "1"])
        .current_dir(workspace_root)
        .output()
        .expect("cargo metadata");

    assert!(output.status.success());
    let metadata = String::from_utf8(output.stdout).expect("metadata json");

    for crate_name in expected {
        assert!(metadata.contains(&format!("\"name\":\"{}\"", crate_name)));
    }
}
