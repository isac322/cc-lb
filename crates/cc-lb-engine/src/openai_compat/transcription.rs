use async_stream::stream;
use bytes::Bytes;
use multer::Multipart;
use serde_json::Value;
use url::Url;

use super::CompatReject;

pub fn transcribe_url(codex_base: &Url) -> Url {
    let mut u = codex_base.clone();
    let mut path = u.path().trim_end_matches('/').to_string();
    if path.ends_with("/codex") {
        path.truncate(path.len() - 6);
    }
    u.set_path(&format!("{}/transcribe", path.trim_end_matches('/')));
    u.set_query(None);
    u.set_fragment(None);
    u
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TranscriptFormat {
    Json,
    Text,
}

pub async fn inspect_transcription_request(
    content_type: &str,
    body: Bytes,
) -> Result<TranscriptFormat, CompatReject> {
    let boundary = multer::parse_boundary(content_type)
        .map_err(|_| CompatReject::unsupported_parameter("content_type"))?;
    let s = stream! { yield Ok::<Bytes, std::convert::Infallible>(body); };
    let mut mp = Multipart::new(s, boundary);
    let mut format = TranscriptFormat::Json;
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
        let value = String::from_utf8_lossy(&bytes);
        match name.as_str() {
            "response_format" => match value.as_ref() {
                "json" | "" => format = TranscriptFormat::Json,
                "text" => format = TranscriptFormat::Text,
                "srt" | "vtt" | "verbose_json" | "diarized_json" => {
                    return Err(CompatReject::unsupported_value("response_format", &value));
                }
                _ => return Err(CompatReject::unsupported_value("response_format", &value)),
            },
            "timestamp_granularities[]" | "timestamp_granularities" => {
                return Err(CompatReject::unsupported_parameter(
                    "timestamp_granularities[]",
                ));
            }
            "stream" if value == "true" => {
                return Err(CompatReject::unsupported_parameter("stream"));
            }
            _ => {}
        }
    }
    Ok(format)
}

pub fn transcription_response(
    format: TranscriptFormat,
    backend_body: &[u8],
) -> Option<(&'static str, Vec<u8>)> {
    let value: Value = serde_json::from_slice(backend_body).ok()?;
    let text = value.get("text")?.as_str()?.to_owned();
    match format {
        TranscriptFormat::Json => Some((
            "application/json",
            serde_json::to_vec(&serde_json::json!({"text": text})).ok()?,
        )),
        TranscriptFormat::Text => Some(("text/plain; charset=utf-8", text.into_bytes())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn multipart(name: &str, value: &str) -> (String, Bytes) {
        let b = "tr";
        let body = format!(
            "--{b}\r\nContent-Disposition: form-data; name=\"{name}\"\r\n\r\n{value}\r\n--{b}--\r\n"
        );
        (
            format!("multipart/form-data; boundary={b}"),
            Bytes::from(body),
        )
    }

    #[tokio::test]
    async fn format_inspection_accepts_text_and_rejects_unsupported_fields() {
        let (ct, body) = multipart("response_format", "text");
        assert_eq!(
            inspect_transcription_request(&ct, body).await.unwrap(),
            TranscriptFormat::Text
        );
        let (ct, body) = multipart("response_format", "vtt");
        assert_eq!(
            inspect_transcription_request(&ct, body)
                .await
                .unwrap_err()
                .code,
            "unsupported_value"
        );
        let (ct, body) = multipart("timestamp_granularities[]", "word");
        assert_eq!(
            inspect_transcription_request(&ct, body)
                .await
                .unwrap_err()
                .code,
            "unsupported_parameter"
        );
        let (ct, body) = multipart("stream", "true");
        assert_eq!(
            inspect_transcription_request(&ct, body)
                .await
                .unwrap_err()
                .code,
            "unsupported_parameter"
        );
    }

    #[test]
    fn transcribe_url_strips_codex() {
        let url = transcribe_url(&Url::parse("https://chatgpt.com/backend-api/codex").unwrap());
        assert_eq!(url.as_str(), "https://chatgpt.com/backend-api/transcribe");
    }

    #[test]
    fn response_conversion_drops_asset_fields() {
        let body = br#"{"text":"hello","asset_pointer":"x","asset_ttl":"30d"}"#;
        assert_eq!(
            transcription_response(TranscriptFormat::Json, body).unwrap(),
            ("application/json", br#"{"text":"hello"}"#.to_vec())
        );
        assert_eq!(
            transcription_response(TranscriptFormat::Text, body).unwrap(),
            ("text/plain; charset=utf-8", b"hello".to_vec())
        );
    }
}
