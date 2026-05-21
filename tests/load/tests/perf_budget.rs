use std::path::PathBuf;
use std::process::Command;

use cc_lb_load_tests::assert_evidence_against_baseline;

#[test]
fn perf_budget_evidence_passes() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let evidence = root.join(cc_lb_load_tests::EVIDENCE_PATH);
    if !evidence.exists() {
        let status = Command::new("bash")
            .arg("tests/load/bench.sh")
            .arg("all")
            .current_dir(&root)
            .status()
            .expect("run load benchmark harness");
        assert!(status.success(), "bench.sh all failed with {status}");
    }

    assert_evidence_against_baseline(&root).expect("perf budget evidence should pass");
}
