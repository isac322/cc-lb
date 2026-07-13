use std::panic;
use std::sync::Once;

use crate::init::increment_panic_total;
use crate::redaction::RedactionPolicy;

static INSTALL_PANIC_HOOK: Once = Once::new();

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

            tracing::error!(
                target: "cc_lb_observability::panic",
                panic_message = redacted_message.as_ref(),
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
