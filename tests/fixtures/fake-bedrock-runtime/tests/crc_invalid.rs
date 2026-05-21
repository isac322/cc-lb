use aws_eventstream_codec::{decode_messages, DecodeError};

#[test]
fn payload_mutation_returns_crc_mismatch() {
    let mut frame = aws_eventstream_codec::encode_message("chunk", br#"{"type":"message_stop"}"#);
    let headers_len = u32::from_be_bytes([frame[4], frame[5], frame[6], frame[7]]) as usize;
    let payload_index = 12 + headers_len;
    frame[payload_index] ^= 0x01;

    let error = decode_messages(&frame).expect_err("mutated frame fails CRC");
    assert!(matches!(error, DecodeError::MessageCrcMismatch));
}
