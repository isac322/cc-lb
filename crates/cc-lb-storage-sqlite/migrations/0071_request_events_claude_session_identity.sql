ALTER TABLE request_events_v1 ADD COLUMN claude_agent_id TEXT;
ALTER TABLE request_events_v1 ADD COLUMN claude_parent_agent_id TEXT;
ALTER TABLE request_events_v1 ADD COLUMN parent_session_id TEXT;
ALTER TABLE request_events_v1 ADD COLUMN client_app TEXT;
ALTER TABLE request_events_v1 ADD COLUMN session_id_source TEXT;
