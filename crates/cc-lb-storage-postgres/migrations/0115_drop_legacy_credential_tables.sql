SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '90s';

DROP TABLE IF EXISTS oauth_credentials_v1;
DROP TABLE IF EXISTS api_keys_v1;
DROP TABLE IF EXISTS anthropic_api_keys_v1;
