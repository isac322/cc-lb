ALTER TABLE principals_v1
ADD COLUMN IF NOT EXISTS allowed_upstreams UUID[] NOT NULL DEFAULT '{}'::uuid[];
