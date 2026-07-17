use std::net::SocketAddr;

use crate::multi_replica_http::request;

pub fn issue_key(admin: SocketAddr) -> Result<String, String> {
    let response = request(
        admin,
        "POST",
        "/admin/v1/principals/stress-principal/keys",
        &[
            ("Authorization", "Bearer stress-admin"),
            ("Content-Type", "application/json"),
        ],
        r#"{"label":"stress-wave"}"#,
    )?;
    if response.status != 201 {
        return Err(format!("issue managed key returned {}", response.status));
    }
    json_field(&response.body, "plaintext_key")
}

pub fn json_field(body: &str, field: &str) -> Result<String, String> {
    serde_json::from_str::<serde_json::Value>(body)
        .map_err(|error| error.to_string())?
        .get(field)
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| format!("missing JSON field {field}"))
}
