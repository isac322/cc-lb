#![forbid(unsafe_code)]

use std::collections::BTreeMap;

const PRELUDE_LEN: usize = 12;
const MESSAGE_CRC_LEN: usize = 4;
const MIN_MESSAGE_LEN: usize = PRELUDE_LEN + MESSAGE_CRC_LEN;
const HEADER_STRING_TYPE: u8 = 7;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DecodedMessage {
    pub headers: BTreeMap<String, HeaderValue>,
    pub payload: Vec<u8>,
}

impl DecodedMessage {
    pub fn header_str(&self, name: &str) -> Option<&str> {
        self.headers.get(name).and_then(HeaderValue::as_str)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HeaderValue {
    String(String),
}

impl HeaderValue {
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(value) => Some(value.as_str()),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    #[error("message is shorter than the AWS event-stream minimum length")]
    TooShort,
    #[error("declared total length exceeds available bytes")]
    Truncated,
    #[error("declared headers length exceeds message bounds")]
    InvalidHeadersLength,
    #[error("prelude CRC mismatch")]
    PreludeCrcMismatch,
    #[error("message CRC mismatch")]
    MessageCrcMismatch,
    #[error("header is truncated")]
    TruncatedHeader,
    #[error("unsupported header value type {0}")]
    UnsupportedHeaderType(u8),
    #[error("header text is not valid UTF-8")]
    InvalidUtf8,
    #[error("trailing bytes do not form a complete event-stream message")]
    TrailingBytes,
}

pub fn encode_message(event_type: &str, payload: &[u8]) -> Vec<u8> {
    encode_message_with_headers(
        &[
            (":event-type", event_type),
            (":content-type", "application/json"),
            (":message-type", "event"),
        ],
        payload,
    )
}

pub fn encode_message_with_headers(headers: &[(&str, &str)], payload: &[u8]) -> Vec<u8> {
    let mut encoded_headers = Vec::new();
    for (name, value) in headers {
        push_string_header(&mut encoded_headers, name, value);
    }

    let total_len = PRELUDE_LEN + encoded_headers.len() + payload.len() + MESSAGE_CRC_LEN;
    let total_len = match u32::try_from(total_len) {
        Ok(value) => value,
        Err(_) => panic!("AWS event-stream message exceeds u32 length"),
    };
    let headers_len = match u32::try_from(encoded_headers.len()) {
        Ok(value) => value,
        Err(_) => panic!("AWS event-stream headers exceed u32 length"),
    };

    let mut frame = Vec::with_capacity(total_len as usize);
    frame.extend_from_slice(&total_len.to_be_bytes());
    frame.extend_from_slice(&headers_len.to_be_bytes());
    let prelude_crc = crc32fast::hash(&frame);
    frame.extend_from_slice(&prelude_crc.to_be_bytes());
    frame.extend_from_slice(&encoded_headers);
    frame.extend_from_slice(payload);
    let message_crc = crc32fast::hash(&frame);
    frame.extend_from_slice(&message_crc.to_be_bytes());
    frame
}

pub fn decode_message(input: &[u8]) -> Result<(DecodedMessage, usize), DecodeError> {
    if input.len() < MIN_MESSAGE_LEN {
        return Err(DecodeError::TooShort);
    }

    let total_len = read_u32(input, 0)? as usize;
    let headers_len = read_u32(input, 4)? as usize;
    if total_len < MIN_MESSAGE_LEN {
        return Err(DecodeError::TooShort);
    }
    if input.len() < total_len {
        return Err(DecodeError::Truncated);
    }
    if headers_len > total_len - MIN_MESSAGE_LEN {
        return Err(DecodeError::InvalidHeadersLength);
    }

    let declared_prelude_crc = read_u32(input, 8)?;
    let actual_prelude_crc = crc32fast::hash(&input[..8]);
    if declared_prelude_crc != actual_prelude_crc {
        return Err(DecodeError::PreludeCrcMismatch);
    }

    let declared_message_crc = read_u32(input, total_len - MESSAGE_CRC_LEN)?;
    let actual_message_crc = crc32fast::hash(&input[..total_len - MESSAGE_CRC_LEN]);
    if declared_message_crc != actual_message_crc {
        return Err(DecodeError::MessageCrcMismatch);
    }

    let headers_start = PRELUDE_LEN;
    let payload_start = headers_start + headers_len;
    let payload_end = total_len - MESSAGE_CRC_LEN;
    let headers = decode_headers(&input[headers_start..payload_start])?;
    let payload = input[payload_start..payload_end].to_vec();

    Ok((DecodedMessage { headers, payload }, total_len))
}

pub fn decode_messages(mut input: &[u8]) -> Result<Vec<DecodedMessage>, DecodeError> {
    let mut messages = Vec::new();
    while !input.is_empty() {
        if input.len() < MIN_MESSAGE_LEN {
            return Err(DecodeError::TrailingBytes);
        }
        let (message, consumed) = decode_message(input)?;
        messages.push(message);
        input = &input[consumed..];
    }
    Ok(messages)
}

fn push_string_header(output: &mut Vec<u8>, name: &str, value: &str) {
    let name_len = match u8::try_from(name.len()) {
        Ok(value) => value,
        Err(_) => panic!("AWS event-stream header name exceeds u8 length"),
    };
    let value_len = match u16::try_from(value.len()) {
        Ok(value) => value,
        Err(_) => panic!("AWS event-stream header value exceeds u16 length"),
    };
    output.push(name_len);
    output.extend_from_slice(name.as_bytes());
    output.push(HEADER_STRING_TYPE);
    output.extend_from_slice(&value_len.to_be_bytes());
    output.extend_from_slice(value.as_bytes());
}

fn decode_headers(mut input: &[u8]) -> Result<BTreeMap<String, HeaderValue>, DecodeError> {
    let mut headers = BTreeMap::new();
    while !input.is_empty() {
        let name_len = *input.first().ok_or(DecodeError::TruncatedHeader)? as usize;
        input = &input[1..];
        if input.len() < name_len + 1 {
            return Err(DecodeError::TruncatedHeader);
        }
        let name = std::str::from_utf8(&input[..name_len]).map_err(|_| DecodeError::InvalidUtf8)?;
        input = &input[name_len..];

        let value_type = input[0];
        input = &input[1..];
        if value_type != HEADER_STRING_TYPE {
            return Err(DecodeError::UnsupportedHeaderType(value_type));
        }
        if input.len() < 2 {
            return Err(DecodeError::TruncatedHeader);
        }
        let value_len = u16::from_be_bytes([input[0], input[1]]) as usize;
        input = &input[2..];
        if input.len() < value_len {
            return Err(DecodeError::TruncatedHeader);
        }
        let value =
            std::str::from_utf8(&input[..value_len]).map_err(|_| DecodeError::InvalidUtf8)?;
        input = &input[value_len..];
        headers.insert(name.to_owned(), HeaderValue::String(value.to_owned()));
    }
    Ok(headers)
}

fn read_u32(input: &[u8], offset: usize) -> Result<u32, DecodeError> {
    let bytes = input
        .get(offset..offset + 4)
        .ok_or(DecodeError::Truncated)?;
    Ok(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
}

#[cfg(test)]
mod tests {
    use super::{decode_messages, encode_message};

    #[test]
    fn roundtrip_single_message() {
        let frame = encode_message("chunk", br#"{"type":"message_stop"}"#);
        let decoded = decode_messages(&frame).expect("frame decodes");
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].header_str(":event-type"), Some("chunk"));
        assert_eq!(decoded[0].payload, br#"{"type":"message_stop"}"#);
    }
}
