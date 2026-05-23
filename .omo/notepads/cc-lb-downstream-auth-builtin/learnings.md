
- Added `PRICE_CATALOG_V1` as a singleton redb table keyed by `litellm_snapshot` and stored snapshots as bincode-encoded `{json_bytes, fetched_at_ms}` payloads.
- `Storage::open` initializes the new table through `migration::initialize_schema`, so `get_price_snapshot()` returns `None` on a fresh database.
- Roundtrip tests should compare the raw JSON bytes directly to preserve byte equality for the cached LiteLLM payload.

- Task 1: `KEY_INDEX_BY_HASH_V1` stores the composite row key encrypted with the existing `Storage::encrypt_value` helper using `index_hash` as AAD, and `Storage::get_composite_by_index` decrypts it back to the `principal_id + NUL + key_id` bytes.
- Task 1: Integration tests must reuse the same `Storage` handle for read/write transactions; opening a second `redb::Database` on the same path while the storage handle is live can hit `DatabaseAlreadyOpen`.

- Task 4: `api_keys::types` now defines the planned serde/bincode-friendly enums and structs, and `Limit::is_subset_of` only checks kind, window, and cap ordering.
- Task 4: `UsageRow` mirrors the planned request-event shape with explicit token, cost, status, and timestamp fields for downstream usage aggregation.
- Task 4: `cargo build -p cc-lb-core` and `cargo test -p cc-lb-core api_keys::types` both passed after adding the new module and `serde`/`bincode` dependencies.
