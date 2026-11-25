use cc_lb_control::event_bus::RequestEventUpdate;
use cc_lb_request_log::RequestEventPartial;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

use super::DEFAULT_PG_NOTIFY_CHANNEL;
use super::metrics::{record_notify_dropped, record_notify_sent};
use crate::live_tail::metrics_labels::{NotifyDropReason, NotifySentOutcome};

const NOTIFY_PAYLOAD_LIMIT_BYTES: usize = 7500;
const MAX_TRIMMED_LABEL_CHARS: usize = 128;
const MAX_NOTIFY_BATCH: usize = 64;

pub struct PgNotifier {
    pub pg_pool: sqlx::PgPool,
    pub in_rx: mpsc::Receiver<RequestEventUpdate>,
    pub producer_id: String,
    channel: String,
}

impl PgNotifier {
    pub fn spawn(
        pg_pool: sqlx::PgPool,
        in_rx: mpsc::Receiver<RequestEventUpdate>,
        producer_id: String,
        shutdown_rx: watch::Receiver<bool>,
    ) -> JoinHandle<()> {
        Self::spawn_with_channel(
            pg_pool,
            in_rx,
            producer_id,
            DEFAULT_PG_NOTIFY_CHANNEL.to_owned(),
            shutdown_rx,
        )
    }

    pub fn spawn_with_channel(
        pg_pool: sqlx::PgPool,
        in_rx: mpsc::Receiver<RequestEventUpdate>,
        producer_id: String,
        channel: String,
        shutdown_rx: watch::Receiver<bool>,
    ) -> JoinHandle<()> {
        let notifier = Self {
            pg_pool,
            in_rx,
            producer_id,
            channel,
        };
        tokio::spawn(notifier.run(shutdown_rx))
    }

    async fn run(mut self, mut shutdown_rx: watch::Receiver<bool>) {
        const MAX_SHUTDOWN_DRAIN: usize = 4096;
        let mut batch = Vec::with_capacity(MAX_NOTIFY_BATCH);
        loop {
            batch.clear();
            tokio::select! {
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        let mut drained = 0usize;
                        while drained < MAX_SHUTDOWN_DRAIN {
                            batch.clear();
                            while batch.len() < MAX_NOTIFY_BATCH && drained < MAX_SHUTDOWN_DRAIN {
                                match self.in_rx.try_recv() {
                                    Ok(update) => {
                                        batch.push(update);
                                        drained += 1;
                                    }
                                    Err(_) => break,
                                }
                            }
                            if batch.is_empty() {
                                break;
                            }
                            self.publish_batch(&mut batch).await;
                        }
                        return;
                    }
                }
                count = self.in_rx.recv_many(&mut batch, MAX_NOTIFY_BATCH) => {
                    if count == 0 {
                        return;
                    }
                    self.publish_batch(&mut batch).await;
                }
            }
        }
    }

    async fn publish_batch(&self, batch: &mut Vec<RequestEventUpdate>) {
        let mut payloads = Vec::with_capacity(batch.len());
        let mut outcomes = Vec::with_capacity(batch.len());

        for update in batch.drain(..) {
            let RequestEventUpdate::Partial(partial) = update else {
                continue;
            };
            let full_update = RequestEventUpdate::Partial(partial.clone());
            let inline_payload = match serde_json::to_vec(&super::protocol::InlinePartialNotify {
                update: &full_update,
                producer_id: self.producer_id.as_str(),
            }) {
                Ok(payload) => payload,
                Err(error) => {
                    record_notify_dropped(NotifyDropReason::SerializeError);
                    tracing::warn!(%error, "partial notify serialization failed");
                    continue;
                }
            };

            let (notify_payload, outcome) = if notify_payload_fits_inline(inline_payload.len()) {
                (inline_payload, NotifySentOutcome::Sent)
            } else {
                match fit_inline_partial(partial, self.producer_id.as_str()) {
                    Ok(Some(fitted)) => fitted,
                    Ok(None) => {
                        record_notify_dropped(NotifyDropReason::PayloadTooLarge);
                        tracing::warn!(
                            "pg_notify partial payload exceeds {}B even after trimming; dropping",
                            NOTIFY_PAYLOAD_LIMIT_BYTES
                        );
                        continue;
                    }
                    Err(error) => {
                        record_notify_dropped(NotifyDropReason::SerializeError);
                        tracing::warn!(%error, "trimmed partial notify serialization failed");
                        continue;
                    }
                }
            };

            payloads.push(String::from_utf8_lossy(&notify_payload).into_owned());
            outcomes.push(outcome);
        }

        if payloads.is_empty() {
            return;
        }

        match sqlx::query("SELECT pg_notify($1, payload) FROM unnest($2::text[]) AS t(payload)")
            .bind(&self.channel)
            .bind(&payloads)
            .execute(&self.pg_pool)
            .await
        {
            Ok(_) => {
                for outcome in outcomes {
                    record_notify_sent(outcome);
                }
            }
            Err(error) => {
                for _ in outcomes {
                    record_notify_dropped(NotifyDropReason::PgError);
                    record_notify_sent(NotifySentOutcome::Failed);
                }
                tracing::warn!(%error, count = payloads.len(), "pg_notify partial batch publish failed");
            }
        }
    }
}

pub(crate) fn notify_payload_fits_inline(payload_len: usize) -> bool {
    payload_len < NOTIFY_PAYLOAD_LIMIT_BYTES
}

/// Progressively trim heavy optional fields so an oversized partial fits inside
/// Postgres NOTIFY's 8KB wire limit without HTTP callbacks.
fn fit_inline_partial(
    mut partial: RequestEventPartial,
    producer_id: &str,
) -> Result<Option<(Vec<u8>, NotifySentOutcome)>, serde_json::Error> {
    // Pass 1: drop non-filter identity/classification metadata, preserving
    // `thread_id` because `?thread_id=` is an active admin SSE stream filter.
    partial.observed_session_id = None;
    partial.parent_session_id = None;
    partial.claude_agent_id = None;
    partial.claude_parent_agent_id = None;
    partial.client_app = None;
    partial.session_id_source = None;
    partial.source_ref_id = None;
    partial.request_kind = None;
    partial.service_tier = None;
    partial.reasoning_effort = None;
    partial.thinking_level = None;

    let serialize = |snapshot: &RequestEventPartial| -> Result<Vec<u8>, serde_json::Error> {
        let update = RequestEventUpdate::Partial(snapshot.clone());
        serde_json::to_vec(&super::protocol::InlinePartialNotify {
            update: &update,
            producer_id,
        })
    };

    let bytes = serialize(&partial)?;
    if notify_payload_fits_inline(bytes.len()) {
        return Ok(Some((bytes, NotifySentOutcome::TrimmedSent)));
    }

    // Pass 2: trim verbose labels and stage timings while still preserving `thread_id`.
    truncate_chars_in_place(&mut partial.model, MAX_TRIMMED_LABEL_CHARS);
    truncate_chars_in_place(&mut partial.upstream_name, MAX_TRIMMED_LABEL_CHARS);
    partial.request_body_read_ms = None;
    partial.request_body_first_chunk_ms = None;
    partial.request_body_receive_ms = None;
    partial.request_body_wait_ms = None;
    partial.request_body_process_ms = None;
    partial.request_body_chunk_count = None;
    partial.request_body_bytes = None;
    partial.auth_ms = None;
    partial.route_ms = None;
    partial.limit_reserve_ms = None;
    partial.json_parse_ms = None;
    partial.cache_structure_ms = None;
    partial.cache_token_key_ms = None;
    partial.cache_count_lookup_ms = None;
    partial.cache_tokenizer_queue_ms = None;
    partial.cache_serialize_ms = None;
    partial.cache_tokenize_ms = None;
    partial.prepare_signer_ms = None;
    partial.bulkhead_wait_ms = None;
    partial.dns_ms = None;
    partial.connect_ms = None;
    partial.connection_reused = None;
    partial.shape_ms = None;
    partial.sign_ms = None;
    partial.upstream_ttfb_ms = None;
    partial.upstream_body_ms = None;
    partial.response_body_wait_ms = None;
    partial.response_body_process_ms = None;
    partial.response_body_downstream_poll_gap_ms = None;
    partial.retry_overhead_ms = None;
    partial.finalize_ms = None;

    let bytes = serialize(&partial)?;
    if notify_payload_fits_inline(bytes.len()) {
        return Ok(Some((bytes, NotifySentOutcome::TrimmedSent)));
    }

    // Pass 3 (last resort): an oversized `thread_id` itself is pushing the
    // payload past Postgres's 8KB wire ceiling.
    partial.thread_id = None;
    let bytes = serialize(&partial)?;
    if notify_payload_fits_inline(bytes.len()) {
        return Ok(Some((bytes, NotifySentOutcome::TrimmedSent)));
    }

    Ok(None)
}

fn truncate_chars_in_place(value: &mut Option<String>, max_chars: usize) {
    let Some(text) = value.as_mut() else {
        return;
    };
    if let Some((cutoff, _)) = text.char_indices().nth(max_chars) {
        text.truncate(cutoff);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_partial() -> RequestEventPartial {
        RequestEventPartial {
            event_id: "evt-1".to_owned(),
            request_id: "req-1".to_owned(),
            ts: 1_800_000_000,
            ts_ms: 1_800_000_000_000,
            last_update_ms: 1_800_000_000_010,
            elapsed_ms: 10,
            stream: true,
            principal_id: Some("principal-a".to_owned()),
            upstream_name: Some("upstream-a".to_owned()),
            model: Some("claude-sonnet-4-5".to_owned()),
            input_tokens: Some(123),
            output_tokens: Some(45),
            cost_usd_micros: Some(678),
            ..RequestEventPartial::default()
        }
    }

    #[test]
    fn oversize_metadata_partial_trims_under_limit_and_preserves_identity_usage() {
        let mut partial = base_partial();
        partial.thread_id = Some("x".repeat(8_192));
        partial.observed_session_id = Some("y".repeat(2_048));
        partial.request_body_read_ms = Some(15);

        let (bytes, outcome) = fit_inline_partial(partial, "prod-1")
            .expect("trim serializes")
            .expect("trimmed partial fits inline");

        assert_eq!(outcome, NotifySentOutcome::TrimmedSent);
        assert!(notify_payload_fits_inline(bytes.len()));

        let decoded: RequestEventUpdate =
            serde_json::from_slice(&bytes).expect("valid update JSON");
        let RequestEventUpdate::Partial(trimmed) = decoded else {
            panic!("expected partial update");
        };
        assert_eq!(trimmed.event_id, "evt-1");
        assert_eq!(trimmed.request_id, "req-1");
        assert_eq!(trimmed.input_tokens, Some(123));
        assert_eq!(trimmed.output_tokens, Some(45));
        assert_eq!(trimmed.cost_usd_micros, Some(678));
        assert!(trimmed.thread_id.is_none());
        assert!(trimmed.observed_session_id.is_none());
    }

    #[test]
    fn oversize_label_and_timing_partial_trims_on_second_pass() {
        let mut partial = base_partial();
        partial.thread_id = Some("x".repeat(8_192));
        partial.model = Some("m".repeat(4_000));
        partial.upstream_name = Some("u".repeat(4_000));
        partial.request_body_read_ms = Some(15);
        partial.finalize_ms = Some(5);

        let (bytes, outcome) = fit_inline_partial(partial, "prod-1")
            .expect("trim serializes")
            .expect("second pass fits inline");

        assert_eq!(outcome, NotifySentOutcome::TrimmedSent);
        assert!(notify_payload_fits_inline(bytes.len()));

        let decoded: RequestEventUpdate =
            serde_json::from_slice(&bytes).expect("valid update JSON");
        let RequestEventUpdate::Partial(trimmed) = decoded else {
            panic!("expected partial update");
        };
        assert_eq!(
            trimmed.model.as_deref().map(str::len),
            Some(MAX_TRIMMED_LABEL_CHARS)
        );
        assert_eq!(
            trimmed.upstream_name.as_deref().map(str::len),
            Some(MAX_TRIMMED_LABEL_CHARS)
        );
        assert!(trimmed.request_body_read_ms.is_none());
        assert!(trimmed.finalize_ms.is_none());
    }

    #[test]
    fn irreducible_oversize_partial_returns_none() {
        let mut partial = base_partial();
        partial.event_id = "e".repeat(8_000);

        let fitted = fit_inline_partial(partial, "prod-1").expect("serialization succeeds");
        assert!(fitted.is_none());
    }

    #[test]
    fn oversize_metadata_partial_preserves_short_thread_id_for_sse_filter() {
        let mut partial = base_partial();
        partial.thread_id = Some("thread-123".to_owned());
        partial.observed_session_id = Some("y".repeat(4_000));
        partial.model = Some("m".repeat(4_000));
        partial.upstream_name = Some("u".repeat(4_000));
        partial.request_body_read_ms = Some(15);
        partial.finalize_ms = Some(5);

        let (bytes, outcome) = fit_inline_partial(partial, "prod-1")
            .expect("trim serializes")
            .expect("pass 2 trimmed partial fits inline");

        assert_eq!(outcome, NotifySentOutcome::TrimmedSent);
        assert!(notify_payload_fits_inline(bytes.len()));

        let decoded: RequestEventUpdate =
            serde_json::from_slice(&bytes).expect("valid update JSON");
        let RequestEventUpdate::Partial(trimmed) = decoded else {
            panic!("expected partial update");
        };
        assert_eq!(trimmed.thread_id.as_deref(), Some("thread-123"));
        assert!(trimmed.observed_session_id.is_none());
        assert_eq!(
            trimmed.model.as_deref().map(str::len),
            Some(MAX_TRIMMED_LABEL_CHARS)
        );
    }
}
