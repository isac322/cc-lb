use std::path::PathBuf;
use std::process::Command;

use cc_lb_load_tests::{LiveTailSoakEvidence, SoakProfile, evaluate_live_tail_soak};

#[test]
#[allow(non_snake_case)]
fn tx__live_tail_soak_smoke_profile_passes() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let status = Command::new("bash")
        .arg("tests/load/live-tail-soak.sh")
        .arg("smoke")
        .current_dir(&root)
        .status()
        .expect("run live-tail soak smoke harness");
    assert!(
        status.success(),
        "live-tail smoke harness failed with {status}"
    );

    let evidence_path = root.join("tests/load/evidence/live-tail-soak.smoke.json");
    let text = std::fs::read_to_string(&evidence_path)
        .unwrap_or_else(|error| panic!("read smoke evidence {}: {error}", evidence_path.display()));
    let evidence: LiveTailSoakEvidence = serde_json::from_str(&text).unwrap_or_else(|error| {
        panic!("parse smoke evidence {}: {error}", evidence_path.display())
    });
    evaluate_live_tail_soak(&evidence, SoakProfile::Smoke)
        .unwrap_or_else(|failures| panic!("live-tail smoke assertions failed: {failures:#?}"));
}
