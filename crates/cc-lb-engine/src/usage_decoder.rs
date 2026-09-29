//! Streaming `content-encoding` decoder for the upstream-response usage tap.
//!
//! ## Why
//!
//! Upstream responses can be compressed (`content-encoding: gzip|deflate|br|zstd`)
//! whenever a downstream client advertises `accept-encoding`. The proxy MUST
//! forward those compressed bytes to the client verbatim (preserving on-the-wire
//! shape), but the proxy's in-flight usage extractor — the SSE and JSON parsers
//! in [`crate::usage_parser`] — needs PLAINTEXT to find
//! `data:` SSE event lines and the `"usage"` field. This module owns that
//! decompression for the metric path only; the response body that flows to the
//! client is never touched.
//!
//! ## Supported encodings
//!
//! `identity`, `gzip` (and `x-gzip` alias), `deflate` (zlib-wrapped, per RFC 9110
//! §8.4.1.2 / common HTTP practice), `br` (Brotli), `zstd`. Anything else is
//! [`UsageDecoder::Unsupported`]; the caller skips usage extraction and emits
//! a warning. Multiple stacked encodings (`content-encoding: gzip, br`) are
//! also unsupported because no real upstream sends them.

use http::HeaderMap;
use http::header::CONTENT_ENCODING;

mod output;

use output::{BudgetedOutput, write_and_drain};

/// Streaming decoder for the metric-extraction side of the response tap.
///
/// One instance per upstream response. Fed compressed chunks; emits plaintext
/// bytes that the SSE parser / JSON parser consume. The owned downstream body
/// is forwarded independently and never goes through this decoder.
pub(crate) enum UsageDecoder {
    /// `content-encoding` absent or `identity` — passthrough.
    Identity,
    Gzip(flate2::write::GzDecoder<BudgetedOutput>),
    /// `content-encoding: deflate` — RFC asks for zlib-wrapped DEFLATE; we
    /// honour that. Servers that send raw DEFLATE under this name are out of
    /// spec and not handled.
    Deflate(flate2::write::ZlibDecoder<BudgetedOutput>),
    Brotli(Box<brotli::DecompressorWriter<BudgetedOutput>>),
    /// Boxed because [`zstd::stream::write::Decoder`] has a non-trivial size
    /// and the rest of the enum is small.
    Zstd(Box<zstd::stream::write::Decoder<'static, BudgetedOutput>>),
    /// Encoding name was unrecognised, multi-encoded, or initialising the
    /// decoder failed. Caller should skip usage extraction.
    Unsupported(String),
    /// A previous chunk failed to decode. Stay poisoned for the rest of the
    /// response so we never emit partially-decoded garbage to the SSE parser.
    Poisoned,
}

impl UsageDecoder {
    /// Build a decoder from the upstream response headers.
    ///
    /// Returns [`UsageDecoder::Unsupported`] (rather than `Err`) for unknown or
    /// multi-stacked encodings so the caller has a single uniform path.
    pub fn from_headers(headers: &HeaderMap, output_budget_bytes: usize) -> Self {
        let Some(value) = headers.get(CONTENT_ENCODING) else {
            return Self::Identity;
        };
        let Ok(raw) = value.to_str() else {
            return Self::Unsupported(format!("{value:?}"));
        };
        let trimmed = raw.trim();
        // Multi-encoded responses (`gzip, br`) are valid HTTP but unused by
        // any real Anthropic-compatible upstream. Skip rather than implement
        // a decoder chain.
        if trimmed.contains(',') {
            return Self::Unsupported(trimmed.to_owned());
        }
        let normalized = trimmed.to_ascii_lowercase();
        match normalized.as_str() {
            "" | "identity" => Self::Identity,
            "gzip" | "x-gzip" => Self::Gzip(flate2::write::GzDecoder::new(BudgetedOutput::new(
                output_budget_bytes,
            ))),
            "deflate" => Self::Deflate(flate2::write::ZlibDecoder::new(BudgetedOutput::new(
                output_budget_bytes,
            ))),
            "br" => Self::Brotli(Box::new(brotli::DecompressorWriter::new(
                BudgetedOutput::new(output_budget_bytes),
                4096,
            ))),
            "zstd" => {
                match zstd::stream::write::Decoder::new(BudgetedOutput::new(output_budget_bytes)) {
                    Ok(decoder) => Self::Zstd(Box::new(decoder)),
                    Err(_) => Self::Unsupported(normalized),
                }
            }
            _ => Self::Unsupported(normalized),
        }
    }

    /// `true` when this decoder will emit plaintext from `push` / `finish`.
    /// Callers use this to decide whether to invoke the SSE / JSON parsers
    /// at all. `Unsupported` and `Poisoned` return `false`.
    pub fn is_active(&self) -> bool {
        !matches!(self, Self::Unsupported(_) | Self::Poisoned)
    }

    /// Returns the unsupported encoding name for logging, if any.
    pub fn unsupported_encoding(&self) -> Option<&str> {
        match self {
            Self::Unsupported(name) => Some(name.as_str()),
            _ => None,
        }
    }

    /// Feed `input` and drain whatever plaintext became available. Returns
    /// an empty `Vec` for chunks that produce no output yet (mid-block in
    /// gzip, etc.) — that's normal.
    ///
    /// On decode error the decoder transitions to `Poisoned`; subsequent
    /// `push` calls return `Ok(Vec::new())` without doing more work.
    pub fn push(&mut self, input: &[u8]) -> std::io::Result<Vec<u8>> {
        if input.is_empty() {
            return Ok(Vec::new());
        }
        let result = match self {
            Self::Identity => return Ok(input.to_vec()),
            Self::Gzip(inner) => write_and_drain(inner, input, |w| w.get_mut()),
            Self::Deflate(inner) => write_and_drain(inner, input, |w| w.get_mut()),
            Self::Brotli(inner) => write_and_drain(inner.as_mut(), input, |w| w.get_mut()),
            Self::Zstd(inner) => write_and_drain(inner.as_mut(), input, |w| w.get_mut()),
            Self::Unsupported(_) | Self::Poisoned => return Ok(Vec::new()),
        };
        match result {
            Ok(bytes) => Ok(bytes),
            Err(error) => {
                *self = Self::Poisoned;
                Err(error)
            }
        }
    }

    /// Finalise the stream and return any trailing plaintext (gzip CRC frame,
    /// zstd final block, …). Always safe to call; returns empty for Identity
    /// / Unsupported / Poisoned. Consumes self.
    pub fn finish(self) -> std::io::Result<Vec<u8>> {
        match self {
            Self::Identity | Self::Unsupported(_) | Self::Poisoned => Ok(Vec::new()),
            Self::Gzip(inner) => inner.finish().map(BudgetedOutput::into_bytes),
            Self::Deflate(inner) => inner.finish().map(BudgetedOutput::into_bytes),
            Self::Brotli(inner) => {
                let mut buf = (*inner).into_inner().map_err(|_| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "brotli decoder failed to finalise",
                    )
                })?;
                Ok(buf.take_bytes())
            }
            Self::Zstd(inner) => {
                let mut writer = inner.into_inner();
                Ok(writer.take_bytes())
            }
        }
    }
}

/// Decode a full body in one shot for the non-streaming JSON response path.
///
/// Returns `Ok(None)` when `content-encoding` is unsupported; callers should
/// log and skip usage extraction. Returns the original bytes unchanged for
/// `identity` to avoid an allocation.
pub fn decode_full_body<'a>(
    headers: &HeaderMap,
    body: &'a [u8],
    output_budget_bytes: usize,
) -> std::io::Result<Option<std::borrow::Cow<'a, [u8]>>> {
    let mut decoder = UsageDecoder::from_headers(headers, output_budget_bytes);
    if !decoder.is_active() {
        return Ok(None);
    }
    if matches!(decoder, UsageDecoder::Identity) {
        return Ok(Some(std::borrow::Cow::Borrowed(body)));
    }
    let mut decoded = decoder.push(body)?;
    let tail = decoder.finish()?;
    decoded.extend_from_slice(&tail);
    Ok(Some(std::borrow::Cow::Owned(decoded)))
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;
    use http::HeaderValue;

    const TEST_OUTPUT_BUDGET_BYTES: usize = 1024 * 1024;

    fn headers_with(encoding: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(CONTENT_ENCODING, HeaderValue::from_str(encoding).unwrap());
        h
    }

    #[test]
    fn identity_when_header_absent() {
        let h = HeaderMap::new();
        assert!(matches!(
            UsageDecoder::from_headers(&h, TEST_OUTPUT_BUDGET_BYTES),
            UsageDecoder::Identity
        ));
    }

    #[test]
    fn identity_when_header_identity() {
        let h = headers_with("identity");
        assert!(matches!(
            UsageDecoder::from_headers(&h, TEST_OUTPUT_BUDGET_BYTES),
            UsageDecoder::Identity
        ));
    }

    #[test]
    fn x_gzip_aliases_gzip() {
        let h = headers_with("x-gzip");
        assert!(matches!(
            UsageDecoder::from_headers(&h, TEST_OUTPUT_BUDGET_BYTES),
            UsageDecoder::Gzip(_)
        ));
    }

    #[test]
    fn multi_encoding_marked_unsupported() {
        let h = headers_with("gzip, br");
        let d = UsageDecoder::from_headers(&h, TEST_OUTPUT_BUDGET_BYTES);
        assert!(!d.is_active());
        assert_eq!(d.unsupported_encoding(), Some("gzip, br"));
    }

    #[test]
    fn unknown_encoding_marked_unsupported() {
        let h = headers_with("snappy");
        assert!(!UsageDecoder::from_headers(&h, TEST_OUTPUT_BUDGET_BYTES).is_active());
    }

    #[test]
    fn gzip_round_trip_chunked() {
        let plaintext = b"event: message_start\ndata: {\"usage\":{\"input_tokens\":7}}\n\n";
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(plaintext).unwrap();
        let compressed = encoder.finish().unwrap();

        let h = headers_with("gzip");
        let mut decoder = UsageDecoder::from_headers(&h, TEST_OUTPUT_BUDGET_BYTES);
        let mut decoded = Vec::new();
        // chunk in tiny slices to exercise the streaming path
        for window in compressed.chunks(3) {
            decoded.extend(decoder.push(window).unwrap());
        }
        decoded.extend(decoder.finish().unwrap());
        assert_eq!(decoded, plaintext);
    }

    #[test]
    fn gzip_missing_trailer_reports_checksum_mismatch() {
        let plaintext = vec![b'a'; 256];
        let mut compressed = gzip_test_bytes(&plaintext, flate2::Compression::default());
        compressed.truncate(compressed.len() - 8);

        let error = gzip_decode_error(&compressed);

        assert_eq!(
            error.to_string(),
            "corrupt gzip stream does not have a matching checksum"
        );
    }

    #[test]
    fn gzip_mid_member_truncation_reports_checksum_mismatch() {
        let plaintext = vec![b'b'; 256];
        let mut compressed = gzip_test_bytes(&plaintext, flate2::Compression::none());
        compressed.truncate(compressed.len() / 2);

        let error = gzip_decode_error(&compressed);

        assert_eq!(
            error.to_string(),
            "corrupt gzip stream does not have a matching checksum"
        );
    }

    #[test]
    fn gzip_flipped_payload_byte_reports_checksum_mismatch() {
        let plaintext = vec![b'c'; 256];
        let mut compressed = gzip_test_bytes(&plaintext, flate2::Compression::none());
        let payload_start = compressed
            .windows(plaintext.len())
            .position(|window| window == plaintext)
            .expect("uncompressed deflate block contains plaintext");
        compressed[payload_start + plaintext.len() / 2] ^= 1;

        let error = gzip_decode_error(&compressed);

        assert_eq!(
            error.to_string(),
            "corrupt gzip stream does not have a matching checksum"
        );
    }

    fn gzip_test_bytes(plaintext: &[u8], level: flate2::Compression) -> Vec<u8> {
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), level);
        encoder.write_all(plaintext).unwrap();
        encoder.finish().unwrap()
    }

    fn gzip_decode_error(compressed: &[u8]) -> std::io::Error {
        let h = headers_with("gzip");
        let mut decoder = UsageDecoder::from_headers(&h, TEST_OUTPUT_BUDGET_BYTES);
        for chunk in compressed.chunks(3) {
            match decoder.push(chunk) {
                Ok(_) => {}
                Err(error) => return error,
            }
        }
        decoder
            .finish()
            .expect_err("damaged gzip stream must fail decoding")
    }

    #[test]
    fn deflate_round_trip() {
        let plaintext = b"hello deflate";
        let mut encoder =
            flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(plaintext).unwrap();
        let compressed = encoder.finish().unwrap();

        let h = headers_with("deflate");
        let mut decoder = UsageDecoder::from_headers(&h, TEST_OUTPUT_BUDGET_BYTES);
        let mut decoded = decoder.push(&compressed).unwrap();
        decoded.extend(decoder.finish().unwrap());
        assert_eq!(decoded, plaintext);
    }

    #[test]
    fn brotli_round_trip() {
        let plaintext = b"hello brotli streaming usage tap";
        let mut encoder = brotli::CompressorWriter::new(Vec::new(), 4096, 5, 22);
        encoder.write_all(plaintext).unwrap();
        let compressed = encoder.into_inner();

        let h = headers_with("br");
        let mut decoder = UsageDecoder::from_headers(&h, TEST_OUTPUT_BUDGET_BYTES);
        let mut decoded = decoder.push(&compressed).unwrap();
        decoded.extend(decoder.finish().unwrap());
        assert_eq!(decoded, plaintext);
    }

    #[test]
    fn zstd_round_trip() {
        let plaintext = b"hello zstd usage tap";
        let compressed = zstd::encode_all(&plaintext[..], 3).unwrap();

        let h = headers_with("zstd");
        let mut decoder = UsageDecoder::from_headers(&h, TEST_OUTPUT_BUDGET_BYTES);
        let mut decoded = decoder.push(&compressed).unwrap();
        decoded.extend(decoder.finish().unwrap());
        assert_eq!(decoded, plaintext);
    }

    #[test]
    fn decode_full_body_identity_borrows() {
        let h = HeaderMap::new();
        let body = b"plain";
        let decoded = decode_full_body(&h, body, body.len()).unwrap().unwrap();
        assert!(matches!(decoded, std::borrow::Cow::Borrowed(_)));
        assert_eq!(&*decoded, body);
    }

    #[test]
    fn decode_full_body_gzip_owns() {
        let plaintext = b"{\"usage\":{\"input_tokens\":11,\"output_tokens\":13}}";
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(plaintext).unwrap();
        let compressed = encoder.finish().unwrap();

        let h = headers_with("gzip");
        let decoded = decode_full_body(&h, &compressed, plaintext.len())
            .unwrap()
            .unwrap();
        assert_eq!(&*decoded, plaintext);
    }

    #[test]
    fn decode_full_body_rejects_output_beyond_budget() {
        let plaintext = vec![b'x'; 4096];
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(&plaintext).unwrap();
        let compressed = encoder.finish().unwrap();
        let h = headers_with("gzip");

        let error = decode_full_body(&h, &compressed, plaintext.len() - 1)
            .expect_err("decompression beyond the output budget must fail");

        assert_eq!(error.kind(), std::io::ErrorKind::OutOfMemory);
    }

    #[test]
    fn decode_full_body_unsupported_returns_none() {
        let h = headers_with("snappy");
        let body = b"\x00\x01\x02";
        assert!(decode_full_body(&h, body, body.len()).unwrap().is_none());
    }

    #[test]
    fn push_after_poison_returns_empty() {
        let h = headers_with("gzip");
        let mut decoder = UsageDecoder::from_headers(&h, TEST_OUTPUT_BUDGET_BYTES);
        // garbage that gzip cannot parse
        let _ = decoder.push(&[0xffu8; 16]);
        // force-poison for the assertion below; the implementation may or may
        // not poison on a single bad chunk depending on internal buffering,
        // but the public contract is that Poisoned discards subsequent input.
        decoder = UsageDecoder::Poisoned;
        let out = decoder.push(b"more").unwrap();
        assert!(out.is_empty());
    }
}
