use cc_lb_domain::{InternalError, TtlClass};

const REQUEST_EVENT_FIXTURE: &[u8] =
    include_bytes!("../../../tests/fixtures/reassembly/request_event.json");

fn fixture_first_array_object_after(key: &str) -> &[u8] {
    let marker = format!("\"{key}\":[");
    let start = REQUEST_EVENT_FIXTURE
        .windows(marker.len())
        .position(|window| window == marker.as_bytes())
        .expect("fixture array key exists")
        + marker.len();
    let suffix = &REQUEST_EVENT_FIXTURE[start..];
    let object_start = suffix
        .iter()
        .position(|byte| *byte == b'{')
        .expect("fixture array contains an object");
    let object_bytes = &suffix[object_start..];
    let mut depth = 0_u32;
    for (index, byte) in object_bytes.iter().copied().enumerate() {
        match byte {
            b'{' => depth += 1,
            b'}' if depth == 1 => return &object_bytes[..=index],
            b'}' => depth -= 1,
            _ => {}
        }
    }
    panic!("fixture array object is terminated")
}

#[test]
fn internal_error_matches_t0_fixture_bytes_when_reserialized() {
    // Given: exact committed T0 RequestEvent subobject bytes.
    let error_bytes = fixture_first_array_object_after("internal_errors");

    // When: domain values deserialize and reserialize those subobjects.
    let error: InternalError = serde_json::from_slice(error_bytes).expect("deserialize error");

    // Then: serde emits byte-identical JSON in the original field order.
    assert_eq!(
        serde_json::to_vec(&error).expect("serialize error"),
        error_bytes
    );
}

#[test]
fn ttl_class_keeps_canonical_snake_case_strings() {
    // Given: both canonical prompt-cache TTL classes.
    let values = [TtlClass::Ephemeral5m, TtlClass::Ephemeral1h];

    // When: the domain values are serialized.
    let bytes = serde_json::to_vec(&values).expect("serialize ttl classes");

    // Then: the historical storage/plugin strings remain exact.
    assert_eq!(bytes, br#"["ephemeral5m","ephemeral1h"]"#);
}
