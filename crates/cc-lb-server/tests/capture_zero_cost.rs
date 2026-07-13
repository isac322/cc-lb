/// Test: Verify that the capture feature is zero-cost when disabled.
///
/// This test runs the fast dependency-graph checks programmatically:
/// 1. Default build: cargo tree should NOT include cc-lb-capture
/// 2. Feature-enabled build: cargo tree SHOULD include cc-lb-capture
///
/// The symbol check (nm -C) is slower and requires an unstripped build,
/// so it is documented as a manual/CI-only verification step in the evidence file.
///
/// ACCEPTANCE CRITERIA:
/// - Default build: `cargo tree -p cc-lb-server -e no-dev | grep -c cc-lb-capture` == 0
/// - Feature build: `cargo tree -p cc-lb-server -e no-dev --features capture | grep -c cc-lb-capture` > 0
use std::process::Command;

#[test]
fn test_capture_feature_off_zero_cost_dependency_graph() {
    // Check 1: Default build should NOT include cc-lb-capture in dependency tree
    let output = Command::new("cargo")
        .args(["tree", "-p", "cc-lb-server", "-e", "no-dev"])
        .output()
        .expect("Failed to run cargo tree (default)");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let capture_count = stdout.matches("cc-lb-capture").count();

    assert_eq!(
        capture_count, 0,
        "Default build should have 0 cc-lb-capture dependencies, but found {}",
        capture_count
    );
}

#[test]
fn test_capture_feature_on_includes_dependency() {
    // Check 2: Feature-enabled build SHOULD include cc-lb-capture in dependency tree
    let output = Command::new("cargo")
        .args([
            "tree",
            "-p",
            "cc-lb-server",
            "-e",
            "no-dev",
            "--features",
            "capture",
        ])
        .output()
        .expect("Failed to run cargo tree (with --features capture)");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let capture_count = stdout.matches("cc-lb-capture").count();

    assert!(
        capture_count > 0,
        "Feature-enabled build should have >0 cc-lb-capture dependencies, but found {}",
        capture_count
    );
}
