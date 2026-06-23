ALTER TABLE organization_metadata_v1
  ADD COLUMN IF NOT EXISTS seat_tier TEXT;
