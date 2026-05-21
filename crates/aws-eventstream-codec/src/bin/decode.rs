#![forbid(unsafe_code)]

use std::env;
use std::fs;

use aws_eventstream_codec::decode_messages;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let Some(path) = env::args().nth(1) else {
        eprintln!("usage: decode <eventstream.bin>");
        std::process::exit(2);
    };

    let bytes = fs::read(path)?;
    let messages = decode_messages(&bytes)?;
    for (index, message) in messages.iter().enumerate() {
        let event_type = message.header_str(":event-type").unwrap_or("");
        let message_type = message.header_str(":message-type").unwrap_or("");
        let exception_type = message.header_str(":exception-type").unwrap_or("");
        let payload = String::from_utf8_lossy(&message.payload);
        println!(
            "frame={} :event-type={} :message-type={} :exception-type={} payload-bytes={} payload={}",
            index + 1,
            event_type,
            message_type,
            exception_type,
            message.payload.len(),
            payload
        );
    }
    println!("frames={}", messages.len());

    Ok(())
}
