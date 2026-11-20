use async_stream::stream;
use base64::Engine;
use bytes::Bytes;
use multer::Multipart;
use serde_json::{Value, json};

use super::CompatReject;

#[derive(Debug, Clone, PartialEq)]
pub struct ImageEditJson {
    pub body: Value,
    pub stream: bool,
}

fn boundary(content_type: &str) -> Result<String, CompatReject> {
    multer::parse_boundary(content_type)
        .map_err(|_| CompatReject::unsupported_parameter("content_type"))
}

pub async fn multipart_edit_to_json(
    content_type: &str,
    body: Bytes,
) -> Result<ImageEditJson, CompatReject> {
    let b = boundary(content_type)?;
    let input = body.clone();
    let s = stream! { yield Ok::<Bytes, std::convert::Infallible>(input); };
    let mut mp = Multipart::new(s, b);
    let mut out = serde_json::Map::new();
    let mut images = Vec::new();
    let mut stream_flag = false;
    while let Some(field) = mp
        .next_field()
        .await
        .map_err(|_| CompatReject::unsupported_parameter("multipart"))?
    {
        let name = field.name().unwrap_or("").to_string();
        let ctype = field
            .content_type()
            .map(|x| x.to_string())
            .unwrap_or_else(|| "image/png".into());
        let data = field
            .bytes()
            .await
            .map_err(|_| CompatReject::unsupported_parameter(name.clone()))?;
        if name == "image" || name == "image[]" {
            let encoded = base64::engine::general_purpose::STANDARD.encode(&data);
            images.push(json!({"image_url": format!("data:{ctype};base64,{encoded}")}));
        } else if name == "mask" {
            let encoded = base64::engine::general_purpose::STANDARD.encode(&data);
            out.insert(
                "mask".into(),
                json!({"image_url": format!("data:{ctype};base64,{encoded}")}),
            );
        } else {
            let text = String::from_utf8_lossy(&data).into_owned();
            if name == "stream" {
                stream_flag = text == "true";
                continue;
            }
            let value = match name.as_str() {
                "n" | "partial_images" => text
                    .parse::<i64>()
                    .map(Value::from)
                    .unwrap_or(Value::String(text)),
                "response_format" => Value::String(text),
                _ => Value::String(text),
            };
            out.insert(name, value);
        }
    }
    out.insert("images".into(), Value::Array(images));
    Ok(ImageEditJson {
        body: Value::Object(out),
        stream: stream_flag,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImageStreamKind {
    Generation,
    Edit,
}

pub fn image_json_to_sse(kind: ImageStreamKind, response: &Value) -> Vec<u8> {
    let event = match kind {
        ImageStreamKind::Generation => "image_generation.completed",
        ImageStreamKind::Edit => "image_edit.completed",
    };
    let created = response.get("created").and_then(Value::as_i64).unwrap_or(0);
    let defaults = ["size", "quality", "background", "output_format", "usage"];
    let mut out = Vec::new();
    if let Some(data) = response.get("data").and_then(Value::as_array) {
        for item in data {
            let mut obj = serde_json::Map::new();
            obj.insert("type".into(), Value::String(event.into()));
            obj.insert(
                "b64_json".into(),
                item.get("b64_json").cloned().unwrap_or(Value::Null),
            );
            obj.insert("created_at".into(), Value::from(created));
            for key in defaults {
                obj.insert(
                    key.into(),
                    response
                        .get(key)
                        .cloned()
                        .or_else(|| item.get(key).cloned())
                        .unwrap_or(Value::Null),
                );
            }
            let line = serde_json::to_vec(&Value::Object(obj)).unwrap_or_default();
            out.extend_from_slice(format!("event: {event}\ndata: ").as_bytes());
            out.extend_from_slice(&line);
            out.extend_from_slice(b"\n\n");
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn multipart_edit_converts_images_mask_and_stream() {
        let boundary = "test-boundary";
        let body = format!(
            "--{b}\r\nContent-Disposition: form-data; name=\"image\"; filename=\"a.png\"\r\nContent-Type: image/png\r\n\r\na\r\n\
             --{b}\r\nContent-Disposition: form-data; name=\"image[]\"; filename=\"b.jpg\"\r\nContent-Type: image/jpeg\r\n\r\nbb\r\n\
             --{b}\r\nContent-Disposition: form-data; name=\"mask\"; filename=\"m.png\"\r\nContent-Type: image/png\r\n\r\nm\r\n\
             --{b}\r\nContent-Disposition: form-data; name=\"n\"\r\n\r\n2\r\n\
             --{b}\r\nContent-Disposition: form-data; name=\"stream\"\r\n\r\ntrue\r\n--{b}--\r\n",
            b = boundary
        );
        let result = multipart_edit_to_json(
            &format!("multipart/form-data; boundary={boundary}"),
            Bytes::from(body),
        )
        .await
        .unwrap();
        assert!(result.stream);
        assert_eq!(result.body["n"], 2);
        assert_eq!(
            result.body["images"][0]["image_url"],
            "data:image/png;base64,YQ=="
        );
        assert_eq!(
            result.body["images"][1]["image_url"],
            "data:image/jpeg;base64,YmI="
        );
        assert_eq!(
            result.body["mask"]["image_url"],
            "data:image/png;base64,bQ=="
        );
        assert!(result.body.get("stream").is_none());
    }

    #[test]
    fn image_response_wraps_each_entry_as_sse() {
        let response = json!({"created": 123, "size": "1024x1024", "quality": "high",
            "background": "auto", "output_format": "png", "usage": {"total_tokens": 1},
            "data": [{"b64_json": "one"}, {"b64_json": "two"}]});
        let sse =
            String::from_utf8(image_json_to_sse(ImageStreamKind::Generation, &response)).unwrap();
        assert_eq!(sse.matches("event: image_generation.completed").count(), 2);
        assert!(sse.contains("\"b64_json\":\"one\""));
        assert!(sse.contains("\"created_at\":123"));
    }
}
