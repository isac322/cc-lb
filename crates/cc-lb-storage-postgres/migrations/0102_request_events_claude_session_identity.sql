ALTER TABLE request_events_v1
    ADD COLUMN claude_agent_id TEXT,
    ADD COLUMN claude_parent_agent_id TEXT,
    ADD COLUMN parent_session_id TEXT,
    ADD COLUMN client_app TEXT,
    ADD COLUMN session_id_source TEXT;
