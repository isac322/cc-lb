use async_stream::stream;
use bytes::Bytes;
use multer::Multipart;
use serde_json::Value;

use super::CompatReject;

pub const REALTIME_ALPHA_HEADER: (&str, &str) = ("openai-alpha", "quicksilver=v2");
pub const REALTIME_CALL_QUERY: &str = "intent=quicksilver&architecture=avas";

pub async fn call_request_to_backend_json(
    content_type: &str,
    body: Bytes,
) -> Result<Vec<u8>, CompatReject> {
    if content_type
        .to_ascii_lowercase()
        .starts_with("application/sdp")
    {
        return Ok(
            serde_json::json!({"sdp": String::from_utf8_lossy(&body), "session": {}})
                .to_string()
                .into_bytes(),
        );
    }
    let boundary = multer::parse_boundary(content_type)
        .map_err(|_| CompatReject::unsupported_parameter("content_type"))?;
    let s = stream! { yield Ok::<Bytes, std::convert::Infallible>(body); };
    let mut mp = Multipart::new(s, boundary);
    let mut sdp = None;
    let mut session = Value::Object(Default::default());
    while let Some(field) = mp
        .next_field()
        .await
        .map_err(|_| CompatReject::unsupported_parameter("multipart"))?
    {
        let name = field.name().unwrap_or("").to_string();
        let bytes = field
            .bytes()
            .await
            .map_err(|_| CompatReject::unsupported_parameter(name.clone()))?;
        match name.as_str() {
            "sdp" => sdp = Some(String::from_utf8_lossy(&bytes).into_owned()),
            "session" => {
                session = sonic_rs::from_slice(&bytes)
                    .map_err(|_| CompatReject::unsupported_value("session", "invalid JSON"))?
            }
            _ => {}
        }
    }
    if session.get("type").and_then(Value::as_str) == Some("transcription") {
        return Err(CompatReject::unsupported_value(
            "session.type",
            "transcription",
        ));
    }
    if let Some(obj) = session.as_object_mut() {
        obj.remove("type");
    }
    Ok(
        serde_json::json!({"sdp": sdp.unwrap_or_default(), "session": session})
            .to_string()
            .into_bytes(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn sdp_request_gets_empty_session() {
        let out = call_request_to_backend_json("application/sdp", Bytes::from("v=0"))
            .await
            .unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&out).unwrap(),
            serde_json::json!({"sdp":"v=0","session":{}})
        );
    }

    #[tokio::test]
    async fn multipart_request_removes_session_type() {
        let b = "rt";
        let body = format!(
            "--{b}\r\nContent-Disposition: form-data; name=\"sdp\"\r\n\r\nv=0\r\n\
             --{b}\r\nContent-Disposition: form-data; name=\"session\"\r\n\r\n{{\"type\":\"realtime\",\"model\":\"gpt-realtime\"}}\r\n--{b}--\r\n",
            b = b
        );
        let out = call_request_to_backend_json(
            &format!("multipart/form-data; boundary={b}"),
            Bytes::from(body),
        )
        .await
        .unwrap();
        let value: Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(value["sdp"], "v=0");
        assert_eq!(
            value["session"],
            serde_json::json!({"model":"gpt-realtime"})
        );
    }

    #[tokio::test]
    async fn transcription_session_type_is_rejected() {
        let b = "rt";
        let body = format!(
            "--{b}\r\nContent-Disposition: form-data; name=\"sdp\"\r\n\r\nv=0\r\n--{b}\r\nContent-Disposition: form-data; name=\"session\"\r\n\r\n{{\"type\":\"transcription\"}}\r\n--{b}--\r\n",
            b = b
        );
        let err = call_request_to_backend_json(
            &format!("multipart/form-data; boundary={b}"),
            Bytes::from(body),
        )
        .await
        .unwrap_err();
        assert_eq!(err.code, "unsupported_value");
    }
}
