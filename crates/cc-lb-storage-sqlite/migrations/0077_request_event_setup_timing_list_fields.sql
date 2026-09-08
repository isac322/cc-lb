ALTER TABLE request_events_v1 ADD COLUMN list_json_parse_ms REAL NULL;
ALTER TABLE request_events_v1 ADD COLUMN list_cache_structure_ms REAL NULL;
ALTER TABLE request_events_v1 ADD COLUMN list_cache_token_key_ms REAL NULL;
ALTER TABLE request_events_v1 ADD COLUMN list_cache_count_lookup_ms REAL NULL;
ALTER TABLE request_events_v1 ADD COLUMN list_cache_tokenizer_queue_ms REAL NULL;
ALTER TABLE request_events_v1 ADD COLUMN list_cache_serialize_ms REAL NULL;
ALTER TABLE request_events_v1 ADD COLUMN list_cache_tokenize_ms REAL NULL;
ALTER TABLE request_events_v1 ADD COLUMN list_prepare_signer_ms REAL NULL;
