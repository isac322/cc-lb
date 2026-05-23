use std::borrow::Cow;
use std::fmt;
use std::io::{self, Write};

use once_cell::sync::Lazy;
use regex::{Captures, Regex, RegexSet};
use tracing::field::{Field, Visit};
use tracing::{Event, Id, Metadata, Subscriber};
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::registry::LookupSpan;

pub const REDACTED: &str = "[REDACTED]";

const SECRET_PATTERNS: [&str; 8] = [
    r"sk-ant-[a-zA-Z0-9_-]+",
    r"sk-ant-ort01-[a-zA-Z0-9_-]+",
    r"Bearer\s+[a-zA-Z0-9._~\-/+]+",
    r"x-api-key:\s*\S+",
    r"AKIA[A-Z0-9]{16}",
    r"ASIA[A-Z0-9]{16}",
    r#"aws_secret_access_key\s*=\s*['"]\S+['"]"#,
    r"-----BEGIN (?:.* )?PRIVATE KEY-----[\s\S]*?-----END (?:.* )?PRIVATE KEY-----",
];

const SENSITIVE_FIELDS: [&str; 8] = [
    "authorization",
    "x-api-key",
    "api_key",
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
        r#"(?i)("(?:authorization|x-api-key|api_key|refresh_token|access_token|client_secret|private_key|aws_secret_access_key)"\s*:\s*)("(?:[^"\\]|\\.)*"|null|true|false|-?\d+(?:\.\d+)?)"#,
    )
});

static SENSITIVE_TEXT_FIELD_REGEX: Lazy<Result<Regex, regex::Error>> = Lazy::new(|| {
    Regex::new(
        r#"(?i)\b(authorization|x-api-key|api_key|refresh_token|access_token|client_secret|private_key|aws_secret_access_key)\b(\s*[:=]\s*)("[^"]*"|'[^']*'|\S+)"#,
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

#[derive(Clone, Debug)]
pub struct RedactionLayer {
    policy: RedactionPolicy,
}

impl RedactionLayer {
    pub fn new(policy: RedactionPolicy) -> Self {
        Self { policy }
    }

    pub fn policy(&self) -> RedactionPolicy {
        self.policy
    }
}

impl Default for RedactionLayer {
    fn default() -> Self {
        Self::new(RedactionPolicy::default())
    }
}

impl<S> Layer<S> for RedactionLayer
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    fn on_new_span(&self, attrs: &tracing::span::Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let mut visitor = RedactingVisitor::new(self.policy);
        attrs.record(&mut visitor);

        if let Some(span) = ctx.span(id) {
            span.extensions_mut().insert(visitor.into_fields());
        }
    }

    fn on_record(&self, span_id: &Id, values: &tracing::span::Record<'_>, ctx: Context<'_, S>) {
        let mut visitor = RedactingVisitor::new(self.policy);
        values.record(&mut visitor);

        if let Some(span) = ctx.span(span_id) {
            let mut extensions = span.extensions_mut();
            if let Some(fields) = extensions.get_mut::<RedactedFields>() {
                fields.extend(visitor.into_fields());
            } else {
                extensions.insert(visitor.into_fields());
            }
        }
    }

    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let mut visitor = RedactingVisitor::new(self.policy);
        event.record(&mut visitor);
        let _redacted_fields = visitor.into_fields();
    }
}

#[derive(Clone, Debug, Default)]
pub struct RedactedFields {
    values: Vec<(String, String)>,
}

impl RedactedFields {
    fn extend(&mut self, other: RedactedFields) {
        self.values.extend(other.values);
    }
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
    fn new(inner: Inner, policy: RedactionPolicy) -> Self {
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

struct RedactingVisitor {
    policy: RedactionPolicy,
    fields: RedactedFields,
}

impl RedactingVisitor {
    fn new(policy: RedactionPolicy) -> Self {
        Self {
            policy,
            fields: RedactedFields::default(),
        }
    }

    fn into_fields(self) -> RedactedFields {
        self.fields
    }

    fn record_value(&mut self, field: &Field, value: &str) {
        let redacted = self.policy.redact_field_value(field.name(), value);
        self.fields.values.push((field.name().to_owned(), redacted));
    }
}

impl Visit for RedactingVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.record_value(field, value);
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.record_value(field, &format!("{value:?}"));
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
