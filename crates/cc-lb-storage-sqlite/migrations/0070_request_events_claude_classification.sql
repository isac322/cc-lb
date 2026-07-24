ALTER TABLE request_events_v1 ADD COLUMN claude_agent_id TEXT;
ALTER TABLE request_events_v1 ADD COLUMN claude_parent_agent_id TEXT;
ALTER TABLE request_events_v1 ADD COLUMN claude_auxiliary_kind TEXT;
