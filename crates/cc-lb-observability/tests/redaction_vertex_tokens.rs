use cc_lb_observability::{REDACTED, RedactionPolicy};

#[test]
fn qa_sec_2_redacts_vertex_tokens_and_credentials() {
    let output = RedactionPolicy::default().redact_text(
        r#"google_credential={"type":"service_account","private_key":"-----BEGIN PRIVATE KEY-----secret-----END PRIVATE KEY-----"} ya29.access-token AQ.Ab8abcdefghijklmnopqrst eyJheader.eyJpayload.signature AQUA"#,
    );
    assert!(!output.contains("ya29.access-token"));
    assert!(!output.contains("AQ.Ab8abcdefghijklmnopqrst"));
    assert!(!output.contains("eyJheader.eyJpayload.signature"));
    assert!(output.contains("AQUA"));
    assert!(output.contains(REDACTED));
}

#[test]
fn qa_sec_2_redacts_google_credential_field() {
    let output = RedactionPolicy::default()
        .redact_field_value("google_credential", "uploaded credential JSON");
    assert_eq!(output, REDACTED);
}

#[test]
fn qa_sec_3_redacts_vertex_secret_fields_in_text() {
    let output = RedactionPolicy::default().redact_text(
        "google_credential: secret refresh_token=refresh client_secret=client access_token=access",
    );
    assert!(!output.contains(": secret"));
    assert!(!output.contains("=refresh"));
    assert!(!output.contains("=client"));
    assert!(!output.contains("=access"));
}
