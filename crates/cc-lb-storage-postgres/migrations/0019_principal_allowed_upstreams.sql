ALTER TABLE principals_v1
ADD COLUMN IF NOT EXISTS allowed_upstreams JSONB NOT NULL DEFAULT '[]' CHECK (jsonb_typeof(allowed_upstreams) = 'array');
