use bytes::{Bytes, BytesMut};
use serde_json::json;

pub fn make_error_frame(error_type: &str, message: &str) -> Bytes {
    let json = json!({
        "type": "error",
        "error": {
            "type": error_type,
            "message": message,
        }
    });
    make_error_frame_from_json(&json)
}

pub fn make_error_frame_from_json(json: &serde_json::Value) -> Bytes {
    let mut frame = BytesMut::new();
    frame.extend_from_slice(b"event: error\n");
    frame.extend_from_slice(b"data: ");
    frame.extend_from_slice(json.to_string().as_bytes());
    frame.extend_from_slice(b"\n\n");
    frame.freeze()
}

#[cfg(test)]
#[allow(non_snake_case)]
mod tests {
    use super::*;

    #[test]
    fn t1__make_error_frame_format_exact_bytes() {
        assert_eq!(
        make_error_frame("api_error", "upstream failed"),
        Bytes::from_static(
            b"event: error\ndata: {\"error\":{\"message\":\"upstream failed\",\"type\":\"api_error\"},\"type\":\"error\"}\n\n"
        )
    );
    }
}
