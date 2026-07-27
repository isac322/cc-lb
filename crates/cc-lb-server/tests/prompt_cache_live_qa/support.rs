use std::time::Duration;

use sqlx::{Row, SqlitePool, sqlite::SqlitePoolOptions};

use super::common;

#[derive(Debug)]
pub struct CacheEvent {
    pub upstream_id: String,
    pub matched_key: String,
    pub breakpoint_index: i64,
    pub matched_index: i64,
    pub lookback_distance: i64,
    pub predicted_read_tokens: i64,
    pub predicted_creation_tokens_5m: i64,
    pub predicted_creation_tokens_1h: i64,
    pub token_estimate_source: String,
}

#[derive(Debug)]
pub struct CacheObservation {
    pub ttl_class: String,
    pub expires_at: i64,
    pub hash_schema_version: i64,
    pub prefix_content_block_index: i64,
    pub estimated_prefix_tokens: i64,
    pub token_estimate_source: String,
}

#[derive(Debug)]
pub struct UpstreamRow {
    pub id: String,
    pub name: String,
}

pub async fn open_sqlite_pool(server: &common::TestServer) -> SqlitePool {
    SqlitePoolOptions::new()
        .max_connections(2)
        .acquire_timeout(Duration::from_secs(5))
        .connect(&format!("sqlite://{}", server.sqlite_path.display()))
        .await
        .expect("open test sqlite")
}

pub async fn fetch_upstreams(pool: &SqlitePool) -> Vec<UpstreamRow> {
    sqlx::query(
        "SELECT id, name FROM upstream_spec_v1 WHERE enabled = 1 AND deleted_at IS NULL ORDER BY name",
    )
    .fetch_all(pool)
    .await
    .expect("query seeded upstreams")
    .into_iter()
    .map(|row| UpstreamRow {
        id: row.get("id"),
        name: row.get("name"),
    })
    .collect()
}

pub async fn wait_for_initial_observations(pool: &SqlitePool, model: &str) -> String {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut interval = tokio::time::interval(Duration::from_millis(20));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        interval.tick().await;
        let row = sqlx::query(
            "SELECT upstream_id, COUNT(*) AS observation_count, \
             MIN(estimated_prefix_tokens) AS min_estimated_tokens, \
             SUM(CASE WHEN ttl_class = '0' THEN 1 ELSE 0 END) AS five_minute_count, \
             SUM(CASE WHEN ttl_class = '1' THEN 1 ELSE 0 END) AS one_hour_count \
             FROM prompt_cache_observations WHERE canonical_model_id = ? \
             GROUP BY upstream_id",
        )
        .bind(model)
        .fetch_optional(pool)
        .await
        .expect("query initial prompt-cache observations");
        if let Some(row) = row
            && row.get::<i64, _>("observation_count") == 2
            && row.get::<i64, _>("min_estimated_tokens") > 1024
            && row.get::<i64, _>("five_minute_count") == 1
            && row.get::<i64, _>("one_hour_count") == 1
        {
            return row.get("upstream_id");
        }
        if tokio::time::Instant::now() >= deadline {
            let rows = sqlx::query(
                "SELECT upstream_id, ttl_class, prefix_content_block_index, estimated_prefix_tokens \
                 FROM prompt_cache_observations ORDER BY upstream_id, prefix_content_block_index",
            )
            .fetch_all(pool)
            .await
            .expect("query prompt-cache observation timeout diagnostics");
            let diagnostics = rows
                .iter()
                .map(|row| {
                    format!(
                        "upstream={} ttl={} index={} tokens={}",
                        row.get::<String, _>("upstream_id"),
                        row.get::<String, _>("ttl_class"),
                        row.get::<i64, _>("prefix_content_block_index"),
                        row.get::<i64, _>("estimated_prefix_tokens")
                    )
                })
                .collect::<Vec<_>>();
            panic!(
                "timed out waiting 10s for one upstream to persist exactly one 1h and one 5m observation above 1024 tokens; rows={diagnostics:?}"
            );
        }
    }
}

pub async fn wait_for_lookback_event(pool: &SqlitePool, model: &str) -> CacheEvent {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut interval = tokio::time::interval(Duration::from_millis(20));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        interval.tick().await;
        let row = sqlx::query(
            "SELECT upstream_id, matched_v3_cache_key, breakpoint_content_block_index, \
             matched_content_block_index, lookback_distance, predicted_cache_read_tokens, \
             predicted_cache_creation_tokens_5m, predicted_cache_creation_tokens_1h, \
             token_estimate_source FROM request_events_v1 \
             WHERE model = ? AND cache_read_input_tokens > 0 ORDER BY id DESC LIMIT 1",
        )
        .bind(model)
        .fetch_optional(pool)
        .await
        .expect("query lookback request event");
        if let Some(row) = row {
            return CacheEvent {
                upstream_id: row.get("upstream_id"),
                matched_key: row.get("matched_v3_cache_key"),
                breakpoint_index: row.get("breakpoint_content_block_index"),
                matched_index: row.get("matched_content_block_index"),
                lookback_distance: row.get("lookback_distance"),
                predicted_read_tokens: row.get("predicted_cache_read_tokens"),
                predicted_creation_tokens_5m: row.get("predicted_cache_creation_tokens_5m"),
                predicted_creation_tokens_1h: row.get("predicted_cache_creation_tokens_1h"),
                token_estimate_source: row.get("token_estimate_source"),
            };
        }
        if tokio::time::Instant::now() >= deadline {
            let latest = sqlx::query(
                "SELECT id, upstream_id, cache_read_input_tokens, matched_v3_cache_key, \
                 lookback_distance FROM request_events_v1 ORDER BY id DESC LIMIT 1",
            )
            .fetch_optional(pool)
            .await
            .expect("query request-event timeout diagnostic")
            .map(|row| {
                format!(
                    "id={} upstream={:?} read={:?} key={:?} distance={:?}",
                    row.get::<i64, _>("id"),
                    row.try_get::<String, _>("upstream_id").ok(),
                    row.try_get::<i64, _>("cache_read_input_tokens").ok(),
                    row.try_get::<String, _>("matched_v3_cache_key").ok(),
                    row.try_get::<i64, _>("lookback_distance").ok()
                )
            });
            panic!(
                "timed out waiting 10s for the cache-read request event with populated lookback telemetry; latest={latest:?}"
            );
        }
    }
}

pub async fn fetch_matched_observation(
    pool: &SqlitePool,
    model: &str,
    event: &CacheEvent,
) -> CacheObservation {
    let row = sqlx::query(
        "SELECT ttl_class, expires_at, hash_schema_version, prefix_content_block_index, \
         estimated_prefix_tokens, token_estimate_source FROM prompt_cache_observations \
         WHERE upstream_id = ? AND canonical_model_id = ? AND v3_prefix_key = ?",
    )
    .bind(&event.upstream_id)
    .bind(model)
    .bind(&event.matched_key)
    .fetch_one(pool)
    .await
    .expect("matched prompt-cache observation exists");
    CacheObservation {
        ttl_class: row.get("ttl_class"),
        expires_at: row.get("expires_at"),
        hash_schema_version: row.get("hash_schema_version"),
        prefix_content_block_index: row.get("prefix_content_block_index"),
        estimated_prefix_tokens: row.get("estimated_prefix_tokens"),
        token_estimate_source: row.get("token_estimate_source"),
    }
}

/// One settled `request_events_v1` row, with the cache-analysis fields the proxy recorded for it.
#[derive(Debug)]
pub struct SettledEvent {
    pub id: i64,
    pub upstream_id: String,
    pub cache_prefix_hash: Option<String>,
    pub cache_control_block_count: Option<i64>,
    pub matched_v3_cache_key: Option<String>,
    pub predicted_read_tokens: Option<i64>,
    cache_breakpoints: String,
}

impl SettledEvent {
    /// `prefix_hash` of the breakpoint recorded against `source`, if the request had one.
    ///
    /// Used to prove salt scoping at the proxy path: a `tools`-source breakpoint must stay
    /// byte-stable across request-level invalidator changes, or cache affinity fragments.
    pub fn breakpoint_prefix_hash(&self, source: &str) -> Option<String> {
        let breakpoints: serde_json::Value = serde_json::from_str(&self.cache_breakpoints)
            .expect("cache_breakpoints column holds JSON");
        breakpoints
            .as_array()?
            .iter()
            .find(|breakpoint| breakpoint["source"].as_str() == Some(source))
            .and_then(|breakpoint| breakpoint["prefix_hash"].as_str())
            .map(ToOwned::to_owned)
    }
}

/// Waits for the next settled proxy request event after `after_id`.
///
/// Settled means the terminal row: `upstream_id` is populated, so the request actually routed.
pub async fn wait_for_settled_event(pool: &SqlitePool, model: &str, after_id: i64) -> SettledEvent {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut interval = tokio::time::interval(Duration::from_millis(20));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        interval.tick().await;
        let row = sqlx::query(
            "SELECT id, upstream_id, cache_prefix_hash, cache_control_block_count, \
             matched_v3_cache_key, predicted_cache_read_tokens, cache_breakpoints \
             FROM request_events_v1 \
             WHERE model = ? AND id > ? AND upstream_id IS NOT NULL \
             ORDER BY id ASC LIMIT 1",
        )
        .bind(model)
        .bind(after_id)
        .fetch_optional(pool)
        .await
        .expect("query settled request event");
        if let Some(row) = row {
            return SettledEvent {
                id: row.get("id"),
                upstream_id: row.get("upstream_id"),
                cache_prefix_hash: row.try_get("cache_prefix_hash").ok(),
                cache_control_block_count: row.try_get("cache_control_block_count").ok(),
                matched_v3_cache_key: row.try_get("matched_v3_cache_key").ok(),
                predicted_read_tokens: row.try_get("predicted_cache_read_tokens").ok(),
                cache_breakpoints: row
                    .try_get::<String, _>("cache_breakpoints")
                    .unwrap_or_else(|_| "[]".to_owned()),
            };
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("timed out waiting 10s for a settled request event after id={after_id}");
        }
    }
}

/// `(count, max estimated_prefix_tokens)` of persisted observations for `model`.
///
/// Polls because observation persistence is asynchronous; returns whatever is present once the
/// count stops being zero, or `(0, 0)` if nothing lands inside `settle`.
pub async fn observation_stats(pool: &SqlitePool, model: &str, settle: Duration) -> (i64, i64) {
    let deadline = tokio::time::Instant::now() + settle;
    let mut interval = tokio::time::interval(Duration::from_millis(20));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        interval.tick().await;
        let row = sqlx::query(
            "SELECT COUNT(*) AS observation_count, \
             COALESCE(MAX(estimated_prefix_tokens), 0) AS max_estimated_tokens \
             FROM prompt_cache_observations WHERE canonical_model_id = ?",
        )
        .bind(model)
        .fetch_one(pool)
        .await
        .expect("query prompt-cache observation stats");
        let stats = (
            row.get::<i64, _>("observation_count"),
            row.get::<i64, _>("max_estimated_tokens"),
        );
        if stats.0 > 0 || tokio::time::Instant::now() >= deadline {
            return stats;
        }
    }
}
