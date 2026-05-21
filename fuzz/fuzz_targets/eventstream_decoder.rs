#![no_main]

use aws_eventstream_codec::{decode_message, decode_messages};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _single = decode_message(data);
    let _batch = decode_messages(data);
});
