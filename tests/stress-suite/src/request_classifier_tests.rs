use crate::request_classifier::{Classification, ClassifyFixture, EvidenceLabel, classify_fixture};

#[test]
fn expected_label_classifier() {
    // Given: the fixture matrix of expected and unexpected request outcomes.
    let fixture: ClassifyFixture =
        serde_json::from_str(include_str!("../fixtures/classify-cases.json"))
            .expect("fixture parses");

    // When: each observed outcome is classified against its typed expected label.
    let report = classify_fixture(&fixture);

    // Then: expected 529/cancel/malformed rows pass and a healthy 500 is unexpected.
    assert!(report.all_correct);
    assert!(report.items.iter().any(|item| {
        item.case_id == "expected-529" && item.classified_as == Classification::Expected
    }));
    assert!(report.items.iter().any(|item| {
        item.expected_label == EvidenceLabel::Unexpected5xx
            && item.classified_as == Classification::Unexpected
    }));
}
