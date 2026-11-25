use std::borrow::Cow;
use std::io::{self, Write};

use cc_lb_domain::{InternalError, MAX_ERROR_MESSAGE_LEN};
use once_cell::sync::Lazy;
use regex::{Captures, Regex, RegexSet};
use tracing::Metadata;
use tracing_subscriber::fmt::MakeWriter;

pub const REDACTED: &str = "[REDACTED]";
pub const ROUTING_REASON_MAX_BYTES: usize = MAX_ERROR_MESSAGE_LEN;

const TRUNCATED_SUFFIX: &str = "...[truncated]";

/// Masks case-insensitive `key` query parameter values in a URL.
#[must_use]
pub fn sanitize_url_for_log(url: &str) -> Cow<'_, str> {
    let Some(query_start) = url.find('?') else {
        return Cow::Borrowed(url);
    };
    let query_with_fragment = &url[query_start + 1..];
    let (query, fragment, has_fragment) = query_with_fragment
        .split_once('#')
        .map_or((query_with_fragment, "", false), |(query, fragment)| {
            (query, fragment, true)
        });
    let mut changed = false;
    let mut sanitized = String::with_capacity(url.len());
    sanitized.push_str(&url[..query_start + 1]);
    for (index, segment) in query.split('&').enumerate() {
        if index > 0 {
            sanitized.push('&');
        }
        if let Some((name, _value)) = segment.split_once('=')
            && name.eq_ignore_ascii_case("key")
        {
            sanitized.push_str(name);
            sanitized.push('=');
            sanitized.push_str(REDACTED);
            changed = true;
        } else {
            sanitized.push_str(segment);
        }
    }
    if has_fragment {
        sanitized.push('#');
        sanitized.push_str(fragment);
    }
    if changed {
        Cow::Owned(sanitized)
    } else {
        Cow::Borrowed(url)
    }
}
const SECRET_PATTERNS: [&str; 17] = [
    r"sk-ant-[a-zA-Z0-9_-]+",
    r"sk-ant-ort01-[a-zA-Z0-9_-]+",
    r"Bearer\s+[a-zA-Z0-9._~\-/+]+",
    r"x-api-key:\s*\S+",
    r"x-goog-api-key:\s*\S+",
    r"api-key:\s*\S+",
    r"x-openai-actor-authorization:\s*\S+",
    r"AIza[0-9A-Za-z_-]{20,}",
    r"\bya29\.[a-zA-Z0-9._~\-/+]+",
    r"\bAQ\.[A-Za-z0-9_-]{20,}",
    r"eyJ[a-zA-Z0-9_-]+\.eyJ[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+",
    r#"[?&][kK][eE][yY]=[^&#\s"']+"#,
    r"AKIA[A-Z0-9]{16}",
    r"ASIA[A-Z0-9]{16}",
    r#"aws_secret_access_key\s*=\s*['"]\S+['"]"#,
    r"-----BEGIN (?:.* )?PRIVATE KEY-----[\s\S]*?-----END (?:.* )?PRIVATE KEY-----",
    r#""google_credential"\s*:\s*(?:"(?:[^"\\]|\\.)*"|\{[^}]*\})"#,
];

const SENSITIVE_FIELDS: [&str; 13] = [
    "authorization",
    "x-api-key",
    "x-goog-api-key",
    "api-key",
    "x-openai-actor-authorization",
    "api_key",
    "google_credential",
    "token",
    "refresh_token",
    "access_token",
    "client_secret",
    "private_key",
    "aws_secret_access_key",
];

const USER_PROMPT_FIELDS: [&str; 4] = ["prompt", "messages", "system", "content"];

static SECRET_REGEX_SET: Lazy<Result<RegexSet, regex::Error>> =
    Lazy::new(|| RegexSet::new(SECRET_PATTERNS));

static SECRET_REGEXES: Lazy<Result<Vec<Regex>, regex::Error>> = Lazy::new(|| {
    SECRET_PATTERNS
        .iter()
        .map(|pattern| Regex::new(pattern))
        .collect()
});

static SENSITIVE_JSON_FIELD_REGEX: Lazy<Result<Regex, regex::Error>> = Lazy::new(|| {
    Regex::new(
        r#"(?i)("(?:authorization|x-api-key|x-goog-api-key|api-key|x-openai-actor-authorization|api_key|google_credential|token|refresh_token|access_token|client_secret|private_key|aws_secret_access_key)"\s*:\s*)("(?:[^"\\]|\\.)*"|null|true|false|-?\d+(?:\.\d+)?)"#,
    )
});

static SENSITIVE_TEXT_FIELD_REGEX: Lazy<Result<Regex, regex::Error>> = Lazy::new(|| {
    Regex::new(
        r#"(?i)\b(authorization|x-api-key|x-goog-api-key|api-key|x-openai-actor-authorization|api_key|google_credential|token|refresh_token|access_token|client_secret|private_key|aws_secret_access_key)\b(\s*[:=]\s*)("[^"]*"|'[^']*'|\S+)"#,
    )
});

static USER_PROMPT_JSON_FIELD_REGEX: Lazy<Result<Regex, regex::Error>> = Lazy::new(|| {
    Regex::new(
        r#"(?i)("(?:prompt|messages|system|content)"\s*:\s*)("(?:[^"\\]|\\.)*"|\[(?:[^\[\]]|\[[^\[\]]*\])*\]|\{(?:[^{}]|\{[^{}]*\})*\})"#,
    )
});

static USER_PROMPT_TEXT_FIELD_REGEX: Lazy<Result<Regex, regex::Error>> = Lazy::new(|| {
    Regex::new(r#"(?i)\b(prompt|messages|system|content)\b(\s*[:=]\s*)("[^"]*"|'[^']*'|\S+)"#)
});

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RedactionPolicy {
    redact_user_prompt: bool,
}

impl RedactionPolicy {
    pub fn new(redact_user_prompt: bool) -> Self {
        Self { redact_user_prompt }
    }

    pub fn redact_user_prompt(&self) -> bool {
        self.redact_user_prompt
    }

    pub fn redact_field_value(&self, field_name: &str, value: &str) -> String {
        if is_sensitive_field(field_name)
            || (self.redact_user_prompt && is_user_prompt_field(field_name))
        {
            REDACTED.to_owned()
        } else {
            self.redact_text(value)
        }
    }

    pub fn redact_text(&self, value: &str) -> String {
        let mut redacted = redact_known_secret_patterns(value);
        redacted = redact_json_fields(&redacted, &SENSITIVE_JSON_FIELD_REGEX);
        redacted = redact_text_fields(&redacted, &SENSITIVE_TEXT_FIELD_REGEX);

        if self.redact_user_prompt {
            redacted = redact_json_fields(&redacted, &USER_PROMPT_JSON_FIELD_REGEX);
            redacted = redact_text_fields(&redacted, &USER_PROMPT_TEXT_FIELD_REGEX);
        }

        redacted
    }
}

impl Default for RedactionPolicy {
    fn default() -> Self {
        Self::new(false)
    }
}

pub fn redact_internal_errors(errors: &[InternalError]) -> Vec<InternalError> {
    let policy = RedactionPolicy::default();

    errors
        .iter()
        .cloned()
        .map(|mut error| {
            if let Some(message) = &error.message {
                error.message = Some(truncate_reason(&policy.redact_text(message)));
            }
            error
        })
        .collect()
}

pub fn truncate_reason(reason: &str) -> String {
    truncate_with_suffix(reason, ROUTING_REASON_MAX_BYTES)
}

#[derive(Clone, Debug)]
pub struct RedactingMakeWriter<Inner> {
    inner: Inner,
    policy: RedactionPolicy,
}

impl<Inner> RedactingMakeWriter<Inner> {
    pub fn new(inner: Inner, policy: RedactionPolicy) -> Self {
        Self { inner, policy }
    }
}

impl<'writer, Inner> MakeWriter<'writer> for RedactingMakeWriter<Inner>
where
    Inner: MakeWriter<'writer>,
{
    type Writer = RedactingWriter<Inner::Writer>;

    fn make_writer(&'writer self) -> Self::Writer {
        RedactingWriter::new(self.inner.make_writer(), self.policy)
    }

    fn make_writer_for(&'writer self, meta: &Metadata<'_>) -> Self::Writer {
        RedactingWriter::new(self.inner.make_writer_for(meta), self.policy)
    }
}

#[derive(Debug)]
pub struct RedactingWriter<Inner> {
    inner: Inner,
    policy: RedactionPolicy,
    buffer: Vec<u8>,
}

impl<Inner> RedactingWriter<Inner> {
    pub(crate) fn new(inner: Inner, policy: RedactionPolicy) -> Self {
        Self {
            inner,
            policy,
            buffer: Vec::new(),
        }
    }
}

impl<Inner> RedactingWriter<Inner>
where
    Inner: Write,
{
    fn write_redacted(&mut self, bytes: &[u8]) -> io::Result<()> {
        let text = String::from_utf8_lossy(bytes);
        let redacted = self.policy.redact_text(&text);
        self.inner.write_all(redacted.as_bytes())
    }

    fn flush_buffer(&mut self) -> io::Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        let bytes = std::mem::take(&mut self.buffer);
        self.write_redacted(&bytes)
    }
}

impl<Inner> Write for RedactingWriter<Inner>
where
    Inner: Write,
{
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.buffer.extend_from_slice(buf);

        while let Some(newline_position) = self.buffer.iter().position(|byte| *byte == b'\n') {
            let line = self.buffer.drain(..=newline_position).collect::<Vec<u8>>();
            self.write_redacted(&line)?;
        }

        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.flush_buffer()?;
        self.inner.flush()
    }
}

fn redact_known_secret_patterns(value: &str) -> String {
    if let Ok(regex_set) = SECRET_REGEX_SET.as_ref()
        && !regex_set.is_match(value)
    {
        return value.to_owned();
    }

    let mut redacted = Cow::Borrowed(value);

    if let Ok(regexes) = SECRET_REGEXES.as_ref() {
        for regex in regexes {
            redacted = Cow::Owned(regex.replace_all(&redacted, REDACTED).into_owned());
        }
    }

    redacted.into_owned()
}

fn redact_json_fields(value: &str, regex: &Lazy<Result<Regex, regex::Error>>) -> String {
    match regex.as_ref() {
        Ok(regex) => regex
            .replace_all(value, |captures: &Captures<'_>| {
                format!("{}\"{}\"", &captures[1], REDACTED)
            })
            .into_owned(),
        Err(_) => value.to_owned(),
    }
}

fn redact_text_fields(value: &str, regex: &Lazy<Result<Regex, regex::Error>>) -> String {
    match regex.as_ref() {
        Ok(regex) => regex
            .replace_all(value, |captures: &Captures<'_>| {
                format!("{}{}{}", &captures[1], &captures[2], REDACTED)
            })
            .into_owned(),
        Err(_) => value.to_owned(),
    }
}

fn truncate_with_suffix(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.to_owned();
    }

    if max_bytes <= TRUNCATED_SUFFIX.len() {
        return TRUNCATED_SUFFIX[..max_bytes].to_owned();
    }

    let value_limit = max_bytes - TRUNCATED_SUFFIX.len();
    let end = floor_char_boundary(value, value_limit);
    format!("{}{}", &value[..end], TRUNCATED_SUFFIX)
}

fn floor_char_boundary(value: &str, max_bytes: usize) -> usize {
    let mut end = max_bytes.min(value.len());
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    end
}

fn is_sensitive_field(field_name: &str) -> bool {
    SENSITIVE_FIELDS
        .iter()
        .any(|sensitive_field| sensitive_field.eq_ignore_ascii_case(field_name))
}

fn is_user_prompt_field(field_name: &str) -> bool {
    USER_PROMPT_FIELDS
        .iter()
        .any(|prompt_field| prompt_field.eq_ignore_ascii_case(field_name))
}
