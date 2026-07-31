-- Request-log model filtering moved from `model = $n` to a case-insensitive
-- prefix match (`lower(model) LIKE $n ESCAPE '\'`), which cannot use the plain
-- `(model, ...)` btree. Replace it with a matching expression index;
-- `text_pattern_ops` is what makes a left-anchored LIKE index-usable under a
-- non-C collation. No query filters on raw `model` any more, so the old index
-- would only add write amplification on the busiest table.
CREATE INDEX request_events_v1_lower_model_list_order_idx
    ON request_events_v1 (lower(model) text_pattern_ops, list_ts_ms DESC, list_event_key DESC);

DROP INDEX IF EXISTS request_events_v1_model_list_order_idx;
