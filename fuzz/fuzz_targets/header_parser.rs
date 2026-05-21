#![no_main]

use cc_lb_core::strip_hop_by_hop;
use http::{HeaderMap, HeaderName, HeaderValue};
use libfuzzer_sys::fuzz_target;

const MAX_HEADER_LINES: usize = 64;
const MAX_HEADER_VALUE_LEN: usize = 4096;

fuzz_target!(|data: &[u8]| {
    let mut headers = HeaderMap::new();
    for line in data.split(|byte| *byte == b'\n').take(MAX_HEADER_LINES) {
        let line = trim_ascii_cr(line);
        let Some(colon) = line.iter().position(|byte| *byte == b':') else {
            continue;
        };
        let (name, value) = line.split_at(colon);
        let value = trim_ascii_space(&value[1..]);
        if value.len() > MAX_HEADER_VALUE_LEN {
            continue;
        }
        let Ok(name) = HeaderName::from_bytes(trim_ascii_space(name)) else {
            continue;
        };
        let Ok(value) = HeaderValue::from_bytes(value) else {
            continue;
        };
        headers.append(name, value);
    }
    strip_hop_by_hop(&mut headers);
});

fn trim_ascii_cr(input: &[u8]) -> &[u8] {
    input.strip_suffix(b"\r").unwrap_or(input)
}

fn trim_ascii_space(mut input: &[u8]) -> &[u8] {
    while let Some((first, rest)) = input.split_first() {
        if !first.is_ascii_whitespace() {
            break;
        }
        input = rest;
    }
    while let Some((last, rest)) = input.split_last() {
        if !last.is_ascii_whitespace() {
            break;
        }
        input = rest;
    }
    input
}
