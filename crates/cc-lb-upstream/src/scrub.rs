//! Error-path masking of a static upstream secret.

use std::borrow::Cow;
use std::collections::HashSet;

/// Replacement text for masked secret material.
const REDACTED: &str = "[REDACTED]";

/// Shortest secret fragment that is masked on its own.
const MIN_FRAGMENT_BYTES: usize = 12;

/// Masks the full `secret` and every maximal run of at least 12 bytes that is
/// a substring of `secret` with `"[REDACTED]"`.
///
/// No state is precomputed; the work happens only when called, i.e. only on
/// error paths. Returns [`Cow::Borrowed`] when nothing matched.
pub fn scrub_secret<'a>(secret: &str, text: &'a str) -> Cow<'a, str> {
    if secret.is_empty() || text.is_empty() {
        return Cow::Borrowed(text);
    }
    let spans = secret_spans(secret.as_bytes(), text);
    if spans.is_empty() {
        return Cow::Borrowed(text);
    }
    let mut scrubbed = String::with_capacity(text.len());
    let mut cursor = 0;
    for (start, end) in spans {
        if let Some(kept) = text.get(cursor..start) {
            scrubbed.push_str(kept);
        }
        scrubbed.push_str(REDACTED);
        cursor = end;
    }
    if let Some(rest) = text.get(cursor..) {
        scrubbed.push_str(rest);
    }
    Cow::Owned(scrubbed)
}

/// Sorted, merged, char-boundary-aligned byte spans of `text` to mask.
fn secret_spans(secret: &[u8], text: &str) -> Vec<(usize, usize)> {
    let bytes = text.as_bytes();
    let mut covered: Vec<(usize, usize)> = Vec::new();
    if secret.len() < MIN_FRAGMENT_BYTES {
        let mut search_from = 0;
        while let Some(found) = bytes
            .get(search_from..)
            .and_then(|haystack| find_subslice(haystack, secret))
        {
            let start = search_from + found;
            let end = start + secret.len();
            covered.push((start, end));
            search_from = end;
        }
    } else {
        let windows: HashSet<&[u8]> = secret.windows(MIN_FRAGMENT_BYTES).collect();
        for (start, window) in bytes.windows(MIN_FRAGMENT_BYTES).enumerate() {
            if windows.contains(window) {
                push_merged(&mut covered, (start, start + MIN_FRAGMENT_BYTES));
            }
        }
    }
    let mut aligned: Vec<(usize, usize)> = Vec::with_capacity(covered.len());
    for (mut start, mut end) in covered {
        while start > 0 && !text.is_char_boundary(start) {
            start -= 1;
        }
        while end < text.len() && !text.is_char_boundary(end) {
            end += 1;
        }
        push_merged(&mut aligned, (start, end));
    }
    aligned
}

/// Appends `span`, merging it into the last span when they overlap or touch.
/// Callers push spans in non-decreasing start order.
fn push_merged(spans: &mut Vec<(usize, usize)>, span: (usize, usize)) {
    if let Some(last) = spans.last_mut()
        && span.0 <= last.1
    {
        last.1 = last.1.max(span.1);
        return;
    }
    spans.push(span);
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY: &str = "sk-ant-api03-AbCdEfGhIjKlMnOpQrStUvWxYz0123456789-xyz_AA";

    #[test]
    fn masks_key_at_start_middle_and_end() {
        assert_eq!(
            scrub_secret(KEY, &format!("{KEY} rejected")),
            "[REDACTED] rejected"
        );
        assert_eq!(
            scrub_secret(KEY, &format!("bad key {KEY} here")),
            "bad key [REDACTED] here"
        );
        assert_eq!(
            scrub_secret(KEY, &format!("invalid: {KEY}")),
            "invalid: [REDACTED]"
        );
    }

    #[test]
    fn masks_every_repeated_occurrence() {
        assert_eq!(
            scrub_secret(KEY, &format!("{KEY},{KEY}")),
            "[REDACTED],[REDACTED]"
        );
    }

    #[test]
    fn masks_fragments_of_twelve_bytes_or_more() {
        let fragment = "AbCdEfGhIjKl";
        let short = "AbCdEfGhIjK";
        assert_eq!(
            scrub_secret(KEY, &format!("x {fragment} y {short} z")),
            format!("x [REDACTED] y {short} z")
        );
    }

    #[test]
    fn masks_url_encoded_key_runs() {
        let secret = "sk-ant-api03-AbCdEfGhIjKlMn+OpQrStUvWxYz/0123456789";
        let encoded = "sk-ant-api03-AbCdEfGhIjKlMn%2BOpQrStUvWxYz%2F0123456789";
        assert_eq!(
            scrub_secret(secret, &format!("url=https://h/?k={encoded}&v=1")),
            "url=https://h/?k=[REDACTED]%2B[REDACTED]%2F0123456789&v=1"
        );
        assert_eq!(
            scrub_secret(KEY, &format!("?key={KEY}&other=1")),
            "?key=[REDACTED]&other=1"
        );
    }

    #[test]
    fn borrows_when_nothing_matches() {
        let text = "upstream said: invalid x-api-key";
        assert!(matches!(scrub_secret(KEY, text), Cow::Borrowed(t) if t == text));
        assert!(matches!(scrub_secret("", text), Cow::Borrowed(_)));
    }

    #[test]
    fn short_secret_masks_only_full_occurrences() {
        assert_eq!(
            scrub_secret("abc", "xabcyab abc"),
            "x[REDACTED]yab [REDACTED]"
        );
    }

    #[test]
    fn keeps_multibyte_text_intact() {
        assert_eq!(scrub_secret(KEY, &format!("é{KEY}ü")), "é[REDACTED]ü");
    }
}
