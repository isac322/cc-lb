CREATE TABLE IF NOT EXISTS upstream_subscription_metadata_v1 (
  upstream_id              UUID PRIMARY KEY,
  organization_uuid        TEXT,
  organization_role        TEXT,
  workspace_role           TEXT,
  observed_at_unix_millis  BIGINT NOT NULL,
  last_error               TEXT,
  raw_roles                TEXT,
  raw_bootstrap            TEXT
);

CREATE INDEX IF NOT EXISTS upstream_subscription_metadata_v1_org_idx
  ON upstream_subscription_metadata_v1 (organization_uuid);

CREATE TABLE IF NOT EXISTS organization_metadata_v1 (
  organization_uuid               TEXT PRIMARY KEY,
  organization_name               TEXT,
  organization_type               TEXT,
  rate_limit_tier                 TEXT,
  has_extra_usage_enabled         BOOLEAN,
  billing_type                    TEXT,
  subscription_created_at_unix_secs BIGINT,
  account_email                   TEXT,
  account_display_name            TEXT,
  account_uuid                    TEXT,
  overage_credit_amount_minor_units BIGINT,
  overage_credit_currency         TEXT,
  overage_credit_granted          BOOLEAN,
  overage_credit_eligible         BOOLEAN,
  observed_at_unix_millis         BIGINT NOT NULL,
  last_error                      TEXT,
  raw_profile                     TEXT,
  raw_overage_grant               TEXT
);
