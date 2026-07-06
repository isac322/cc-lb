use std::borrow::Cow;
use std::fmt;
use std::io::{self, Write};

use cc_lb_plugin_api::types::{
    CacheAffinityCandidate, CacheAffinityTrace, CandidateUrgency, MAX_ERROR_MESSAGE_LEN,
    MAX_ROUTING_TRACE_STAGES, MAX_STAGE_NAME_LEN, StageDecision, SubscriptionPreferenceTrace,
    SubscriptionTier, TerminalDecision, WrhKeySource,
};
use cc_lb_plugin_api::{InternalError, RoutingTrace, TerminalStrategy};
use once_cell::sync::Lazy;
use regex::{Captures, Regex, RegexSet};
use tracing::field::{Field, Visit};
use tracing::{Event, Id, Metadata, Subscriber};
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::registry::LookupSpan;

pub const REDACTED: &str = "[REDACTED]";
pub const ROUTING_TRACE_SIZE_CAP_BYTES: usize = 4 * 1024;
pub const ROUTING_REASON_MAX_BYTES: usize = MAX_ERROR_MESSAGE_LEN;

const TRUNCATED_SUFFIX: &str = "...[truncated]";
const ROUTING_TRACE_TRUNCATED_STAGE: &str = "routing_trace_truncated";

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

const SENSITIVE_FIELDS: [&str; 9] = [
    "authorization",
    "x-api-key",
    "api_key",
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
        r#"(?i)("(?:authorization|x-api-key|api_key|token|refresh_token|access_token|client_secret|private_key|aws_secret_access_key)"\s*:\s*)("(?:[^"\\]|\\.)*"|null|true|false|-?\d+(?:\.\d+)?)"#,
    )
});

static SENSITIVE_TEXT_FIELD_REGEX: Lazy<Result<Regex, regex::Error>> = Lazy::new(|| {
    Regex::new(
        r#"(?i)\b(authorization|x-api-key|api_key|token|refresh_token|access_token|client_secret|private_key|aws_secret_access_key)\b(\s*[:=]\s*)("[^"]*"|'[^']*'|\S+)"#,
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

pub fn redact_routing_trace(trace: &RoutingTrace) -> RoutingTrace {
    let policy = RedactionPolicy::default();
    let mut redacted = trace.clone();

    for stage in &mut redacted.stages {
        stage.stage_name = truncate_plain_text(&stage.stage_name, MAX_STAGE_NAME_LEN);
        if let Some(reason) = &stage.reason {
            stage.reason = Some(truncate_reason(&policy.redact_text(reason)));
        }
    }

    redacted
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

pub fn enforce_routing_trace_caps(trace: &RoutingTrace) -> RoutingTrace {
    let mut capped = trace.clone();
    normalize_trace_fields(&mut capped);

    let mut removed_stages = 0;
    if capped.stages.len() > MAX_ROUTING_TRACE_STAGES {
        removed_stages += capped.stages.len() - MAX_ROUTING_TRACE_STAGES + 1;
        capped
            .stages
            .truncate(MAX_ROUTING_TRACE_STAGES.saturating_sub(1));
        upsert_truncation_marker(&mut capped, removed_stages);
    }

    while routing_trace_json_len(&capped) > ROUTING_TRACE_SIZE_CAP_BYTES {
        if !remove_last_original_stage(&mut capped) {
            break;
        }
        removed_stages += 1;
        upsert_truncation_marker(&mut capped, removed_stages);
    }

    capped
}

pub fn truncate_reason(reason: &str) -> String {
    truncate_with_suffix(reason, ROUTING_REASON_MAX_BYTES)
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

fn normalize_trace_fields(trace: &mut RoutingTrace) {
    for stage in &mut trace.stages {
        stage.stage_name = truncate_plain_text(&stage.stage_name, MAX_STAGE_NAME_LEN);
        if let Some(reason) = &stage.reason {
            stage.reason = Some(truncate_reason(reason));
        }
    }
}

fn upsert_truncation_marker(trace: &mut RoutingTrace, removed_stages: usize) {
    let marker = StageDecision {
        stage_name: ROUTING_TRACE_TRUNCATED_STAGE.to_owned(),
        upstream_id: None,
        reason: Some(format!(
            "routing trace truncated; removed {removed_stages} stage(s)"
        )),
        duration_us: 0,
        subscription_preference: None,
        cache_affinity: None,
    };

    if trace
        .stages
        .last()
        .is_some_and(|stage| stage.stage_name == ROUTING_TRACE_TRUNCATED_STAGE)
    {
        let last_index = trace.stages.len() - 1;
        trace.stages[last_index] = marker;
    } else {
        trace.stages.push(marker);
    }
}

fn remove_last_original_stage(trace: &mut RoutingTrace) -> bool {
    if trace.stages.is_empty() {
        return false;
    }

    if trace
        .stages
        .last()
        .is_some_and(|stage| stage.stage_name == ROUTING_TRACE_TRUNCATED_STAGE)
    {
        if trace.stages.len() == 1 {
            return false;
        }
        trace.stages.remove(trace.stages.len() - 2);
        true
    } else {
        trace.stages.pop();
        true
    }
}

fn routing_trace_json_len(trace: &RoutingTrace) -> usize {
    let mut len = "{\"stages\":[".len();

    for (index, stage) in trace.stages.iter().enumerate() {
        if index > 0 {
            len += 1;
        }
        len += stage_json_len(stage);
    }

    len += "]".len();
    if let Some(terminal_decision) = &trace.terminal_decision {
        len += ",\"terminal_decision\":".len() + terminal_decision_json_len(terminal_decision);
    }
    len + "}".len()
}

fn stage_json_len(stage: &StageDecision) -> usize {
    let mut len = "{\"stage_name\":".len() + json_string_len(&stage.stage_name);

    if let Some(upstream_id) = stage.upstream_id {
        len += ",\"upstream_id\":".len() + json_string_len(&upstream_id.to_string());
    }
    if let Some(reason) = &stage.reason {
        len += ",\"reason\":".len() + json_string_len(reason);
    }
    if stage.duration_us != 0 {
        len += ",\"duration_us\":".len() + stage.duration_us.to_string().len();
    }
    if let Some(subscription_preference) = &stage.subscription_preference {
        len += ",\"subscription_preference\":".len()
            + subscription_preference_json_len(subscription_preference);
    }
    if let Some(cache_affinity) = &stage.cache_affinity {
        len += ",\"cache_affinity\":".len() + cache_affinity_json_len(cache_affinity);
    }

    len + "}".len()
}

fn subscription_preference_json_len(trace: &SubscriptionPreferenceTrace) -> usize {
    let mut len =
        "{\"chosen_tier\":".len() + tier_json_len(trace.chosen_tier) + ",\"candidates\":[".len();
    for (index, candidate) in trace.candidates.iter().enumerate() {
        if index > 0 {
            len += 1;
        }
        len += candidate_urgency_json_len(candidate);
    }
    len += "]".len();
    len += ",\"wrh_key_source\":".len() + wrh_key_source_json_len(trace.wrh_key_source);
    if let Some(previous_tier) = trace.previous_tier {
        len += ",\"previous_tier\":".len() + tier_json_len(previous_tier);
    }
    if let Some(salt_version) = &trace.rendezvous_salt_version {
        len += ",\"rendezvous_salt_version\":".len() + json_string_len(salt_version);
    }
    len + "}".len()
}

fn cache_affinity_json_len(trace: &CacheAffinityTrace) -> usize {
    let mut len = "{\"candidates\":[".len();
    for (index, candidate) in trace.candidates.iter().enumerate() {
        if index > 0 {
            len += 1;
        }
        len += cache_affinity_candidate_json_len(candidate);
    }
    len + "]}".len()
}

fn cache_affinity_candidate_json_len(candidate: &CacheAffinityCandidate) -> usize {
    let mut len = "{\"upstream_id\":".len()
        + json_string_len(&candidate.upstream_id.to_string())
        + ",\"kept\":".len()
        + if candidate.kept { "true".len() } else { "false".len() };
    if let Some(tokens) = candidate.predicted_cache_read_tokens {
        len += ",\"predicted_cache_read_tokens\":".len() + tokens.to_string().len();
    }
    if let Some(expires_at) = candidate.predicted_expires_at_unix_secs {
        len += ",\"predicted_expires_at_unix_secs\":".len() + expires_at.to_string().len();
    }
    len + "}".len()
}

fn candidate_urgency_json_len(candidate: &CandidateUrgency) -> usize {
    "{\"upstream_id\":".len()
        + json_string_len(&candidate.upstream_id.to_string())
        + ",\"tier\":".len()
        + tier_json_len(candidate.tier)
        + ",\"urgency\":".len()
        + f64_json_len(candidate.urgency)
        + "}".len()
}

fn tier_json_len(tier: SubscriptionTier) -> usize {
    match tier {
        SubscriptionTier::KnownBase => "\"known_base\"".len(),
        SubscriptionTier::PartialBase => "\"partial_base\"".len(),
        SubscriptionTier::Overage => "\"overage\"".len(),
        SubscriptionTier::UnknownProbe => "\"unknown_probe\"".len(),
    }
}

fn wrh_key_source_json_len(source: WrhKeySource) -> usize {
    match source {
        WrhKeySource::ThreadId => "\"thread_id\"".len(),
        WrhKeySource::RequestId => "\"request_id\"".len(),
    }
}

fn f64_json_len(value: f64) -> usize {
    if value.is_finite() { 24 } else { 4 }
}

fn terminal_decision_json_len(terminal_decision: &TerminalDecision) -> usize {
    let mut len = "{".len();
    let mut needs_comma = false;

    if let Some(upstream_id) = terminal_decision.upstream_id {
        len += "\"upstream_id\":".len() + json_string_len(&upstream_id.to_string());
        needs_comma = true;
    }

    if needs_comma {
        len += 1;
    }
    len += "\"strategy\":".len()
        + json_string_len(terminal_strategy_json(&terminal_decision.strategy));

    len + "}".len()
}

fn terminal_strategy_json(strategy: &TerminalStrategy) -> &'static str {
    match strategy {
        TerminalStrategy::FirstPick => "first-pick",
        TerminalStrategy::Random => "random",
    }
}

fn json_string_len(value: &str) -> usize {
    value.chars().fold(2, |len, ch| {
        len + match ch {
            '"' | '\\' => 2,
            '\u{08}' | '\u{0c}' | '\n' | '\r' | '\t' => 2,
            '\u{00}'..='\u{1f}' => 6,
            ch => ch.len_utf8(),
        }
    })
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

fn truncate_plain_text(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.to_owned();
    }

    let end = floor_char_boundary(value, max_bytes);
    value[..end].to_owned()
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
