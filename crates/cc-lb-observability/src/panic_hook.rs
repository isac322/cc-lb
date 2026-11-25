use std::io::{self, Write};
use std::panic;
use std::sync::Once;

use crate::init::increment_panic_total;
use crate::redaction::RedactionPolicy;

static INSTALL_PANIC_HOOK: Once = Once::new();

pub(crate) fn write_redacted_panic_line(
    writer: &mut impl Write,
    location: &str,
    redacted_message: &str,
) -> io::Result<()> {
    writeln!(
        writer,
        "panic_observed target=cc_lb_observability::panic panic_location={location} panic_message={redacted_message}"
    )?;
    writer.flush()
}

pub fn install_panic_hook(policy: RedactionPolicy) {
    INSTALL_PANIC_HOOK.call_once(move || {
        panic::set_hook(Box::new(move |panic_info| {
            increment_panic_total();

            let message = panic_message(panic_info);
            let redacted_message = policy.redact_text(&message);
            let location = panic_info
                .location()
                .map(|location| {
                    format!(
                        "{}:{}:{}",
                        location.file(),
                        location.line(),
                        location.column()
                    )
                })
                .unwrap_or_else(|| "unknown".to_owned());

            // Write synchronously to stderr before returning because release builds
            // use `panic = "abort"` and the non-blocking stdout log worker may not
            // drain an enqueued tracing event before termination.
            let _ = {
                let mut stderr = io::stderr().lock();
                write_redacted_panic_line(&mut stderr, &location, &redacted_message)
            };

            tracing::error!(
                target: "cc_lb_observability::panic",
                panic_message = redacted_message.as_str(),
                panic_location = location.as_str(),
                "panic_observed"
            );
        }));
    });
}

fn panic_message(panic_info: &panic::PanicHookInfo<'_>) -> String {
    if let Some(message) = panic_info.payload().downcast_ref::<&str>() {
        (*message).to_owned()
    } else if let Some(message) = panic_info.payload().downcast_ref::<String>() {
        message.clone()
    } else {
        "non-string panic payload".to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_redacted_panic_line_synchronously_emits_location_and_redacted_message() {
        let policy = RedactionPolicy::new(false);
        let redacted = policy.redact_text("boom bearer sk-ant-api03-abcdefghijklmnop");
        let mut sink = Vec::new();
        write_redacted_panic_line(&mut sink, "src/lib.rs:42:5", &redacted)
            .expect("stderr write succeeds");
        let line = String::from_utf8(sink).expect("utf-8");
        assert!(line.contains("panic_observed"), "{line}");
        assert!(line.contains("panic_location=src/lib.rs:42:5"), "{line}");
        assert!(!line.contains("sk-ant-api03-abcdefghijklmnop"), "{line}");
    }
}
