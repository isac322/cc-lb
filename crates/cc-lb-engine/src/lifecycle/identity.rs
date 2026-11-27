use sha2::{Digest, Sha256};

use super::*;
use cc_lb_storage_api::{DEFAULT_THREAD_STICKY_SUBAGENT_TTL_SECS, ThreadStickySubagentStore};

pub(super) const THREAD_STICKY_SUBAGENT_SHARD_COUNT: usize = 16;
pub(super) const MAX_THREAD_STICKY_SUBAGENTS_PER_SHARD: usize = 512;
const NEGATIVE_LATCH_TTL_SECS: u64 = 60;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) struct ThreadStickySubagentKey {
    pub(super) principal_id: String,
    pub(super) observed_session_id: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StickyLatchState {
    Latched,
    NotLatched,
}

#[derive(Clone, Copy, Debug)]
struct StickyLatchEntry {
    state: StickyLatchState,
    expires_at_unix_secs: u64,
    sequence: u64,
}

#[derive(Clone)]
pub struct ThreadStickySubagentLatch {
    pub(super) next_sequence: Arc<AtomicU64>,
    pub(super) seed_epoch: Arc<AtomicU64>,
    shards: Arc<
        [Mutex<HashMap<ThreadStickySubagentKey, StickyLatchEntry>>;
            THREAD_STICKY_SUBAGENT_SHARD_COUNT],
    >,
}

impl ThreadStickySubagentLatch {
    pub fn new() -> Self {
        Self {
            next_sequence: Arc::new(AtomicU64::new(0)),
            seed_epoch: Arc::new(AtomicU64::new(0)),
            shards: Arc::new(std::array::from_fn(|_| Mutex::new(HashMap::new()))),
        }
    }

    pub(super) fn shard_index(key: &ThreadStickySubagentKey) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % THREAD_STICKY_SUBAGENT_SHARD_COUNT
    }

    pub fn seed_local(
        &self,
        principal_id: &str,
        observed_session_id: &str,
        expires_at_unix_secs: u64,
    ) {
        self.seed_epoch.fetch_add(1, Ordering::AcqRel);
        let key = ThreadStickySubagentKey {
            principal_id: principal_id.to_owned(),
            observed_session_id: observed_session_id.to_owned(),
        };
        let shard = &self.shards[Self::shard_index(&key)];
        let mut shard = shard
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let sequence = self.next_sequence.fetch_add(1, Ordering::Relaxed) + 1;
        let next_expires = match shard.get(&key) {
            Some(existing) if existing.state == StickyLatchState::Latched => {
                existing.expires_at_unix_secs.max(expires_at_unix_secs)
            }
            _ => expires_at_unix_secs,
        };
        if !shard.contains_key(&key) && shard.len() >= MAX_THREAD_STICKY_SUBAGENTS_PER_SHARD {
            evict_oldest(&mut shard);
        }
        shard.insert(
            key,
            StickyLatchEntry {
                state: StickyLatchState::Latched,
                expires_at_unix_secs: next_expires,
                sequence,
            },
        );
    }

    pub fn invalidate_local(&self, principal_id: &str, observed_session_id: &str) {
        self.seed_epoch.fetch_add(1, Ordering::AcqRel);
        let key = ThreadStickySubagentKey {
            principal_id: principal_id.to_owned(),
            observed_session_id: observed_session_id.to_owned(),
        };
        let shard = &self.shards[Self::shard_index(&key)];
        let mut shard = shard
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        shard.remove(&key);
    }

    pub fn clear_negative_entries(&self) {
        self.seed_epoch.fetch_add(1, Ordering::AcqRel);
        for shard in self.shards.iter() {
            let mut shard = shard
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            shard.retain(|_, entry| entry.state == StickyLatchState::Latched);
        }
    }

    pub(super) fn seed_epoch(&self) -> u64 {
        self.seed_epoch.load(Ordering::Acquire)
    }

    pub(super) fn check_local(
        &self,
        key: &ThreadStickySubagentKey,
        now_unix_secs: u64,
    ) -> Option<bool> {
        let shard = &self.shards[Self::shard_index(key)];
        let mut shard = shard
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match shard.get(key).copied() {
            Some(entry) if entry.expires_at_unix_secs > now_unix_secs => {
                Some(entry.state == StickyLatchState::Latched)
            }
            Some(_) => {
                shard.remove(key);
                None
            }
            None => None,
        }
    }

    pub(super) fn record_negative_local(
        &self,
        key: ThreadStickySubagentKey,
        expires_at_unix_secs: u64,
        now_unix_secs: u64,
    ) {
        let shard = &self.shards[Self::shard_index(&key)];
        let mut shard = shard
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(existing) = shard.get(&key)
            && existing.state == StickyLatchState::Latched
            && existing.expires_at_unix_secs > now_unix_secs
        {
            return;
        }
        let sequence = self.next_sequence.fetch_add(1, Ordering::Relaxed) + 1;
        if !shard.contains_key(&key) && shard.len() >= MAX_THREAD_STICKY_SUBAGENTS_PER_SHARD {
            evict_oldest(&mut shard);
        }
        shard.insert(
            key,
            StickyLatchEntry {
                state: StickyLatchState::NotLatched,
                expires_at_unix_secs,
                sequence,
            },
        );
    }

    #[cfg(test)]
    pub(super) fn observe(&self, key: ThreadStickySubagentKey, seed: bool) -> bool {
        let shard = &self.shards[Self::shard_index(&key)];
        let mut shard = shard
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if seed {
            self.seed_epoch.fetch_add(1, Ordering::AcqRel);
            let sequence = self.next_sequence.fetch_add(1, Ordering::Relaxed) + 1;
            if !shard.contains_key(&key) && shard.len() >= MAX_THREAD_STICKY_SUBAGENTS_PER_SHARD {
                evict_oldest(&mut shard);
            }
            shard.insert(
                key,
                StickyLatchEntry {
                    state: StickyLatchState::Latched,
                    expires_at_unix_secs: u64::MAX,
                    sequence,
                },
            );
            true
        } else {
            shard
                .get(&key)
                .is_some_and(|entry| entry.state == StickyLatchState::Latched)
        }
    }
}

fn evict_oldest(shard: &mut HashMap<ThreadStickySubagentKey, StickyLatchEntry>) {
    if let Some(oldest_key) = shard
        .iter()
        .min_by_key(|(_, entry)| entry.sequence)
        .map(|(key, _)| key.clone())
    {
        shard.remove(&oldest_key);
    }
}

impl Default for ThreadStickySubagentLatch {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
pub(super) fn apply_thread_sticky_subagent_latch(
    latch: &ThreadStickySubagentLatch,
    principal_id: &str,
    value: Option<&Value>,
    metadata: &mut RequestCacheMetadata,
) {
    if metadata.claude_agent_id.is_some() || metadata.explicit_request_class.is_some() {
        return;
    }
    let Some(observed_session_id) = metadata
        .observed_session_id
        .as_deref()
        .or(metadata.thread_id.as_deref())
    else {
        return;
    };
    let key = ThreadStickySubagentKey {
        principal_id: principal_id.to_owned(),
        observed_session_id: observed_session_id.to_owned(),
    };
    if metadata.request_kind.as_deref() == Some("subagent")
        && value.is_some_and(|value| {
            is_thread_sticky_subagent_seed(
                value,
                Some(observed_session_id),
                metadata.parent_session_id.as_deref(),
            )
        })
    {
        latch.observe(key, true);
    } else if matches!(metadata.request_kind.as_deref(), Some("main" | "unknown"))
        && latch.observe(key, false)
    {
        metadata.request_kind = Some("subagent".to_owned());
    }
}

pub(super) async fn apply_thread_sticky_subagent_latch_async(
    latch: &ThreadStickySubagentLatch,
    store: Option<&dyn ThreadStickySubagentStore>,
    clock: &dyn cc_lb_clock::Clock,
    principal_id: &str,
    value: Option<&Value>,
    metadata: &mut RequestCacheMetadata,
) {
    if metadata.claude_agent_id.is_some() || metadata.explicit_request_class.is_some() {
        return;
    }
    let Some(observed_session_id) = metadata
        .observed_session_id
        .as_deref()
        .or(metadata.thread_id.as_deref())
    else {
        return;
    };
    let now_unix_secs = unix_secs(clock.now());
    if metadata.request_kind.as_deref() == Some("subagent")
        && value.is_some_and(|value| {
            is_thread_sticky_subagent_seed(
                value,
                Some(observed_session_id),
                metadata.parent_session_id.as_deref(),
            )
        })
    {
        latch.seed_local(
            principal_id,
            observed_session_id,
            now_unix_secs.saturating_add(DEFAULT_THREAD_STICKY_SUBAGENT_TTL_SECS),
        );
        if let Some(store) = store
            && let Err(error) = store
                .seed_thread_sticky_subagent(
                    principal_id,
                    observed_session_id,
                    now_unix_secs,
                    DEFAULT_THREAD_STICKY_SUBAGENT_TTL_SECS,
                )
                .await
        {
            tracing::warn!(
                %error,
                principal_id,
                observed_session_id,
                "failed to persist thread sticky subagent seed"
            );
        }
    } else if matches!(metadata.request_kind.as_deref(), Some("main" | "unknown")) {
        let key = ThreadStickySubagentKey {
            principal_id: principal_id.to_owned(),
            observed_session_id: observed_session_id.to_owned(),
        };
        match latch.check_local(&key, now_unix_secs) {
            Some(true) => {
                metadata.request_kind = Some("subagent".to_owned());
            }
            Some(false) => {}
            None => {
                let Some(store) = store else {
                    return;
                };
                let seed_epoch = latch.seed_epoch();
                match store
                    .is_thread_sticky_subagent(principal_id, observed_session_id, now_unix_secs)
                    .await
                {
                    Ok(Some(expires_at_unix_secs)) => {
                        latch.seed_local(principal_id, observed_session_id, expires_at_unix_secs);
                        metadata.request_kind = Some("subagent".to_owned());
                    }
                    Ok(None) => {
                        if latch.seed_epoch() == seed_epoch {
                            latch.record_negative_local(
                                key,
                                now_unix_secs.saturating_add(NEGATIVE_LATCH_TTL_SECS),
                                now_unix_secs,
                            );
                        }
                    }
                    Err(error) => {
                        tracing::warn!(
                            %error,
                            principal_id,
                            observed_session_id,
                            "failed to query thread sticky subagent store"
                        );
                    }
                }
            }
        }
    }
}
pub(super) const HERMES_PRODUCT_TAG: &str = "product=hermes-agent";
pub(super) const HERMES_CONVERSATION_TAG_PREFIX: &str = "conversation=";
pub(crate) const MAX_IDENTITY_VALUE_BYTES: usize = 512;

#[derive(Default)]
pub(super) struct MetadataIdentity {
    pub(super) session_id: Option<String>,
    pub(super) parent_session_id: Option<String>,
}

#[derive(Default)]
pub(super) struct RequestIdentityMetadata {
    pub(super) thread_id: Option<String>,
    pub(super) observed_session_id: Option<String>,
    pub(super) request_kind: Option<String>,
    pub(super) explicit_request_class: Option<String>,
    pub(super) explicit_auxiliary: bool,
    pub(super) claude_agent_id: Option<String>,
    pub(super) claude_parent_agent_id: Option<String>,
    pub(super) parent_session_id: Option<String>,
    pub(super) client_app: Option<String>,
    pub(super) session_id_source: Option<String>,
}

pub(super) fn metadata_identity(value: &Value) -> MetadataIdentity {
    let Some(user_id) = value
        .get("metadata")
        .and_then(|metadata| metadata.get("user_id"))
        .and_then(Value::as_str)
    else {
        return MetadataIdentity::default();
    };

    if let Ok(parsed) = sonic_rs::from_str::<Value>(user_id)
        && parsed.is_object()
    {
        return MetadataIdentity {
            session_id: parsed
                .get("session_id")
                .and_then(Value::as_str)
                .and_then(bounded_identity),
            parent_session_id: parsed
                .get("parent_session_id")
                .and_then(Value::as_str)
                .and_then(bounded_identity),
        };
    }
    if let Some((_, rest)) = user_id.rsplit_once("_session_")
        && let Some(id) = bounded_identity(rest.trim())
    {
        return MetadataIdentity {
            session_id: Some(id),
            parent_session_id: None,
        };
    }
    if uuid::Uuid::parse_str(user_id.trim()).is_ok() {
        return MetadataIdentity {
            session_id: bounded_identity(user_id.trim()),
            parent_session_id: None,
        };
    }
    MetadataIdentity::default()
}

pub(super) fn bounded_identity(value: &str) -> Option<String> {
    (!value.is_empty() && value.len() <= MAX_IDENTITY_VALUE_BYTES).then(|| value.to_owned())
}

pub(super) fn header_identity(headers: &HeaderMap, name: &str) -> Option<String> {
    header_to_string(headers, name).and_then(|value| bounded_identity(&value))
}

pub(super) fn client_metadata_session_identity(value: &Value) -> Option<(String, &'static str)> {
    ["/client_metadata/session_id", "/client_metadata/session-id"]
        .into_iter()
        .find_map(|pointer| {
            value
                .pointer(pointer)
                .and_then(Value::as_str)
                .and_then(bounded_identity)
                .map(|session_id| (session_id, "client_metadata.session_id"))
        })
}

pub(super) fn request_session_identity(
    headers: &HeaderMap,
    metadata: &MetadataIdentity,
    value: Option<&Value>,
) -> Option<(String, &'static str)> {
    const SESSION_HEADERS: [&str; 12] = [
        "x-claude-code-session-id",
        "x-claude-session-id",
        "x-hermes-session-id",
        "x-aside-provider-session-id",
        "x-aside-session-id",
        "x-session-affinity",
        "x-opencode-session-id",
        "x-opencode-session",
        "x-session-id",
        "session_id",
        "session-id",
        "conversation_id",
    ];

    SESSION_HEADERS
        .into_iter()
        .find_map(|header| header_identity(headers, header).map(|value| (value, header)))
        .or_else(|| {
            let raw = header_to_string(headers, "x-codex-turn-metadata")?;
            (raw.len() <= 4096)
                .then(|| sonic_rs::from_str::<Value>(&raw).ok())?
                .and_then(|metadata| {
                    metadata
                        .get("session_id")
                        .and_then(Value::as_str)
                        .and_then(bounded_identity)
                        .map(|session_id| (session_id, "x-codex-turn-metadata"))
                })
        })
        .or_else(|| value.and_then(hermes_body_session_identity))
        .or_else(|| value.and_then(client_metadata_session_identity))
        .or_else(|| {
            metadata
                .session_id
                .clone()
                .map(|session_id| (session_id, "metadata.user_id"))
        })
}

pub(super) fn derived_parent_session_id(session_id: &str) -> Option<String> {
    if let Some(rest) = session_id.strip_prefix("aside:worker:")
        && let Some((parent, profile)) = rest.split_once(':')
        && !parent.is_empty()
        && !profile.is_empty()
    {
        return bounded_identity(parent);
    }
    for marker in [":side:", ":spec:", ":btw:"] {
        if let Some((parent, suffix)) = session_id.split_once(marker)
            && !parent.is_empty()
            && !suffix.is_empty()
        {
            return bounded_identity(parent);
        }
    }
    None
}

pub(super) fn request_identity_metadata(
    headers: &HeaderMap,
    value: Option<&Value>,
    capability: Option<ProviderCapability>,
) -> RequestIdentityMetadata {
    let metadata = value.map(metadata_identity).unwrap_or_default();
    let (observed_session_id, session_id_source) =
        match request_session_identity(headers, &metadata, value) {
            Some((session_id, source)) => (Some(session_id), Some(source.to_owned())),
            None => (None, None),
        };
    let claude_agent_id = header_identity(headers, "x-claude-code-agent-id");
    let claude_parent_agent_id = header_identity(headers, "x-claude-code-parent-agent-id");
    let originator = header_identity(headers, "originator");
    let client_app = header_identity(headers, "x-app").or_else(|| {
        originator.as_ref().and_then(|value| {
            let trimmed = value.trim();
            (trimmed.len() >= 5 && trimmed.as_bytes()[..5].eq_ignore_ascii_case(b"codex"))
                .then(|| trimmed.to_owned())
        })
    });
    let codex_parent_thread_id = header_identity(headers, "x-codex-parent-thread-id");
    let parent_session_id = header_identity(headers, "x-parent-session-id")
        .or_else(|| header_identity(headers, "x-opencode-parent-session-id"))
        .or_else(|| codex_parent_thread_id.clone())
        .or_else(|| metadata.parent_session_id.clone())
        .or_else(|| {
            observed_session_id
                .as_deref()
                .and_then(derived_parent_session_id)
        })
        .filter(|parent| observed_session_id.as_deref() != Some(parent.as_str()));
    let claude_request_class = header_identity(headers, "x-claude-code-request-class");
    let explicit_request_class = claude_request_class.clone();
    let explicit_auxiliary = claude_request_class
        .as_deref()
        .is_some_and(|class| class.trim().eq_ignore_ascii_case("auxiliary"));
    let subagent_header = header_identity(headers, "x-openai-subagent");
    let user_agent = header_identity(headers, "user-agent");
    let codex_signals = CodexRequestSignals {
        subagent_header: subagent_header.as_deref(),
        guardian_header: header_identity(headers, "x-codex-guardian").is_some(),
        has_parent_thread: codex_parent_thread_id.is_some(),
        originator: originator.as_deref(),
        user_agent: user_agent.as_deref(),
        has_codex_turn_metadata: header_identity(headers, "x-codex-turn-metadata").is_some(),
        is_compact_endpoint: capability == Some(ProviderCapability::OpenaiResponsesCompact),
    };
    let request_kind = classify_client_request_kind_with_signals(
        value,
        observed_session_id.as_deref(),
        parent_session_id.as_deref(),
        claude_agent_id.is_some(),
        claude_request_class.as_deref(),
        Some(&codex_signals),
    )
    .map(str::to_owned);

    RequestIdentityMetadata {
        thread_id: observed_session_id.clone(),
        observed_session_id,
        request_kind,
        explicit_request_class,
        explicit_auxiliary,
        claude_agent_id,
        claude_parent_agent_id,
        parent_session_id,
        client_app,
        session_id_source,
    }
}

pub(super) fn hermes_body_session_identity(value: &Value) -> Option<(String, &'static str)> {
    let tags = value.get("tags").and_then(Value::as_array)?;
    if !tags
        .iter()
        .any(|tag| tag.as_str() == Some(HERMES_PRODUCT_TAG))
    {
        return None;
    }

    value
        .get("session_id")
        .and_then(Value::as_str)
        .and_then(bounded_identity)
        .map(|session_id| (session_id, "session_id"))
        .or_else(|| {
            tags.iter().filter_map(Value::as_str).find_map(|tag| {
                tag.strip_prefix(HERMES_CONVERSATION_TAG_PREFIX)
                    .and_then(bounded_identity)
                    .map(|session_id| (session_id, "tags.conversation"))
            })
        })
}

pub(super) fn is_openai_prewarm_request(body: &Value) -> bool {
    body.get("generate") == Some(&Value::Bool(false))
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct OpenaiSessionHintKey {
    principal_id: String,
    key_sha256: [u8; 32],
}

#[derive(Clone, Copy, Debug)]
struct OpenaiSessionHintEntry {
    upstream_id: Uuid,
    observed_at_unix_secs: u64,
    sequence: u64,
}

#[derive(Clone)]
pub(super) struct OpenaiSessionHintCache {
    next_sequence: Arc<AtomicU64>,
    shards: Arc<
        [Mutex<HashMap<OpenaiSessionHintKey, OpenaiSessionHintEntry>>;
            THREAD_STICKY_SUBAGENT_SHARD_COUNT],
    >,
}

impl OpenaiSessionHintCache {
    pub(super) fn new() -> Self {
        Self {
            next_sequence: Arc::new(AtomicU64::new(0)),
            shards: Arc::new(std::array::from_fn(|_| Mutex::new(HashMap::new()))),
        }
    }

    fn key(principal_id: &str, raw_key: &str) -> Option<OpenaiSessionHintKey> {
        if raw_key.is_empty() || raw_key.len() > MAX_IDENTITY_VALUE_BYTES {
            return None;
        }
        let key_sha256: [u8; 32] = Sha256::digest(raw_key.as_bytes()).into();
        Some(OpenaiSessionHintKey {
            principal_id: principal_id.to_owned(),
            key_sha256,
        })
    }

    fn shard_index(key: &OpenaiSessionHintKey) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % THREAD_STICKY_SUBAGENT_SHARD_COUNT
    }

    pub(super) fn record(
        &self,
        principal_id: &str,
        raw_key: &str,
        upstream_id: Uuid,
        now_unix_secs: u64,
    ) {
        let Some(key) = Self::key(principal_id, raw_key) else {
            return;
        };
        let shard = &self.shards[Self::shard_index(&key)];
        let mut shard = shard
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let sequence = self.next_sequence.fetch_add(1, Ordering::Relaxed) + 1;
        if !shard.contains_key(&key)
            && shard.len() >= MAX_THREAD_STICKY_SUBAGENTS_PER_SHARD
            && let Some(oldest_key) = shard
                .iter()
                .min_by_key(|(_, entry)| entry.sequence)
                .map(|(k, _)| k.clone())
        {
            shard.remove(&oldest_key);
        }
        shard.insert(
            key,
            OpenaiSessionHintEntry {
                upstream_id,
                observed_at_unix_secs: now_unix_secs,
                sequence,
            },
        );
    }

    pub(super) fn lookup(
        &self,
        principal_id: &str,
        raw_key: &str,
        now_unix_secs: u64,
        ttl_secs: u64,
    ) -> Option<Uuid> {
        let key = Self::key(principal_id, raw_key)?;
        let shard = &self.shards[Self::shard_index(&key)];
        let shard = shard
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let entry = shard.get(&key)?;
        (now_unix_secs.saturating_sub(entry.observed_at_unix_secs) <= ttl_secs)
            .then_some(entry.upstream_id)
    }
}

pub(crate) const DEFAULT_WS_RATE_LIMIT_COOLDOWN_SECS: u64 = 60;
pub(super) const MAX_WS_RATE_LIMIT_COOLDOWN_SECS: u64 = 18_000;
const MAX_WS_RATE_LIMIT_COOLDOWN_ENTRIES: usize = 4096;

#[derive(Clone, Default)]
pub(crate) struct WsRateLimitCooldownCache {
    active_count: Arc<std::sync::atomic::AtomicUsize>,
    entries: Arc<Mutex<HashMap<Uuid, u64>>>,
}

impl WsRateLimitCooldownCache {
    pub(crate) fn has_any(&self) -> bool {
        self.active_count.load(Ordering::Relaxed) > 0
    }

    pub(crate) fn is_blocked(&self, upstream_id: Uuid, now_unix_secs: u64) -> bool {
        if !self.has_any() {
            return false;
        }
        let entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        entries
            .get(&upstream_id)
            .is_some_and(|&blocked_until| blocked_until > now_unix_secs)
    }

    pub(crate) fn record(
        &self,
        upstream_id: Uuid,
        blocked_until_unix_secs: u64,
        now_unix_secs: u64,
    ) {
        if blocked_until_unix_secs <= now_unix_secs {
            return;
        }
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        entries.retain(|_, until| *until > now_unix_secs);
        if !entries.contains_key(&upstream_id)
            && entries.len() >= MAX_WS_RATE_LIMIT_COOLDOWN_ENTRIES
            && let Some(earliest_id) = entries
                .iter()
                .min_by_key(|(_, until)| **until)
                .map(|(id, _)| *id)
        {
            entries.remove(&earliest_id);
        }
        entries
            .entry(upstream_id)
            .and_modify(|existing| *existing = (*existing).max(blocked_until_unix_secs))
            .or_insert(blocked_until_unix_secs);
        self.active_count.store(entries.len(), Ordering::Relaxed);
    }

    pub(crate) fn clear(&self, upstream_id: Uuid) {
        if !self.has_any() {
            return;
        }
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if entries.remove(&upstream_id).is_some() {
            self.active_count.store(entries.len(), Ordering::Relaxed);
        }
    }
}

pub(crate) fn ws_rate_limit_cooldown_secs(
    payload: Option<&Value>,
    retry_after_secs: Option<u64>,
    now_unix_secs: u64,
) -> u64 {
    if let Some(resets_at) = payload.and_then(|value| {
        value
            .pointer("/error/resets_at")
            .or_else(|| value.pointer("/response/error/resets_at"))
            .and_then(Value::as_u64)
    }) && resets_at > now_unix_secs
    {
        return (resets_at - now_unix_secs).clamp(1, MAX_WS_RATE_LIMIT_COOLDOWN_SECS);
    }
    if let Some(resets_in) = payload.and_then(|value| {
        value
            .pointer("/error/resets_in_seconds")
            .or_else(|| value.pointer("/response/error/resets_in_seconds"))
            .and_then(Value::as_u64)
    }) && resets_in > 0
    {
        return resets_in.clamp(1, MAX_WS_RATE_LIMIT_COOLDOWN_SECS);
    }
    if let Some(retry_after) = retry_after_secs.filter(|secs| *secs > 0) {
        return retry_after.clamp(1, MAX_WS_RATE_LIMIT_COOLDOWN_SECS);
    }
    DEFAULT_WS_RATE_LIMIT_COOLDOWN_SECS
}

impl Default for OpenaiSessionHintCache {
    fn default() -> Self {
        Self::new()
    }
}
