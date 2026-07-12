use http::HeaderMap;
use http::header::{HeaderName, HeaderValue};

pub(crate) fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for (name, value) in pairs {
        headers.insert(
            HeaderName::from_bytes(name.as_bytes()).expect("test header name parses"),
            HeaderValue::from_str(value).expect("test header value parses"),
        );
    }
    headers
}

#[path = "rate_limit_headers/standard.rs"]
mod standard;
#[path = "rate_limit_headers/unified_top_level.rs"]
mod unified_top_level;
#[path = "rate_limit_headers/unified_windows.rs"]
mod unified_windows;
