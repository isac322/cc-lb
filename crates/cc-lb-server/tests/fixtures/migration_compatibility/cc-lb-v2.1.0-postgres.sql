--
-- PostgreSQL database dump
--

\restrict 5uYxoijXCQkZCd74Dciyp0Q6TrzVke1nbqfDzSHe4JFyFNw8223kVYApT5R960w

-- Dumped from database version 15.19 (Debian 15.19-1.pgdg13+2)
-- Dumped by pg_dump version 15.19 (Debian 15.19-1.pgdg13+2)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: apalis; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA apalis;


--
-- Name: cc_lb_scheduler; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA cc_lb_scheduler;


--
-- Name: pgcrypto; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA public;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: jobs; Type: TABLE; Schema: apalis; Owner: -
--

CREATE TABLE apalis.jobs (
    job bytea NOT NULL,
    id text NOT NULL,
    job_type text NOT NULL,
    status text DEFAULT 'Pending'::text NOT NULL,
    attempts integer DEFAULT 0 NOT NULL,
    max_attempts integer DEFAULT 25 NOT NULL,
    run_at timestamp with time zone DEFAULT now() NOT NULL,
    last_result jsonb,
    lock_at timestamp with time zone,
    lock_by text,
    done_at timestamp with time zone,
    priority integer DEFAULT 0,
    metadata jsonb,
    idempotency_key text
);


--
-- Name: get_jobs(text, text, integer); Type: FUNCTION; Schema: apalis; Owner: -
--

CREATE FUNCTION apalis.get_jobs(worker_id text, v_job_type text, v_job_count integer DEFAULT 5) RETURNS SETOF apalis.jobs
    LANGUAGE plpgsql
    AS $$ BEGIN RETURN QUERY
UPDATE apalis.jobs
SET status = 'Queued',
    lock_by = worker_id,
    lock_at = now()
WHERE id IN (
        SELECT id
        FROM apalis.jobs
        WHERE (status='Pending' OR (status = 'Failed' AND attempts < max_attempts))
            AND run_at < now()
            AND job_type = v_job_type
        ORDER BY priority DESC, run_at ASC
        LIMIT v_job_count FOR
        UPDATE SKIP LOCKED
    )
returning *;
END;
$$;


--
-- Name: notify_new_jobs(); Type: FUNCTION; Schema: apalis; Owner: -
--

CREATE FUNCTION apalis.notify_new_jobs() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF NEW.run_at <= now() THEN
        PERFORM pg_notify(
            'apalis::job::insert',
            json_build_object(
                'job_type', NEW.job_type,
                'id', NEW.id,
                'run_at', NEW.run_at
            )::text
        );
    END IF;
    RETURN NEW;
END;
$$;


--
-- Name: push_job(text, json, text, timestamp with time zone, integer, integer); Type: FUNCTION; Schema: apalis; Owner: -
--

CREATE FUNCTION apalis.push_job(job_type text, job json DEFAULT NULL::json, status text DEFAULT 'Pending'::text, run_at timestamp with time zone DEFAULT now(), max_attempts integer DEFAULT 25, priority integer DEFAULT 0) RETURNS apalis.jobs
    LANGUAGE plpgsql
    AS $$ 

        DECLARE 
            v_job_row apalis.jobs;
            v_job_id text;

        BEGIN
        IF job_type is not NULL and length(job_type) > 512 THEN raise exception 'Job_type is too long (max length: 512).' USING errcode = 'APAJT';
        END IF;

        IF max_attempts < 1 THEN raise exception 'Job maximum attempts must be at least 1.' USING errcode = 'APAMA';
        end IF;

        SELECT
            generate_ulid() INTO v_job_id;
        INSERT INTO
            apalis.jobs
        VALUES
            (
                job,
                v_job_id,
                job_type,
                status,
                0,
                max_attempts,
                run_at,
                NULL,
                NULL,
                NULL,
                NULL,
                priority
            )
            returning * INTO v_job_row;
            RETURN v_job_row;
END;
$$;


--
-- Name: generate_ulid(); Type: FUNCTION; Schema: cc_lb_scheduler; Owner: -
--

CREATE FUNCTION cc_lb_scheduler.generate_ulid() RETURNS text
    LANGUAGE plpgsql
    AS $$
DECLARE
  -- Crockford's Base32
  encoding   BYTEA = '0123456789ABCDEFGHJKMNPQRSTVWXYZ';
  timestamp  BYTEA = E'\\000\\000\\000\\000\\000\\000';
  output     TEXT = '';

  unix_time  BIGINT;
  ulid       BYTEA;
BEGIN
  -- 6 timestamp bytes
  unix_time = (EXTRACT(EPOCH FROM CLOCK_TIMESTAMP()) * 1000)::BIGINT;
  timestamp = SET_BYTE(timestamp, 0, (unix_time >> 40)::BIT(8)::INTEGER);
  timestamp = SET_BYTE(timestamp, 1, (unix_time >> 32)::BIT(8)::INTEGER);
  timestamp = SET_BYTE(timestamp, 2, (unix_time >> 24)::BIT(8)::INTEGER);
  timestamp = SET_BYTE(timestamp, 3, (unix_time >> 16)::BIT(8)::INTEGER);
  timestamp = SET_BYTE(timestamp, 4, (unix_time >> 8)::BIT(8)::INTEGER);
  timestamp = SET_BYTE(timestamp, 5, unix_time::BIT(8)::INTEGER);

  -- 10 entropy bytes
  ulid = timestamp || gen_random_bytes(10);

  -- Encode the timestamp
  output = output || CHR(GET_BYTE(encoding, (GET_BYTE(ulid, 0) & 224) >> 5));
  output = output || CHR(GET_BYTE(encoding, (GET_BYTE(ulid, 0) & 31)));
  output = output || CHR(GET_BYTE(encoding, (GET_BYTE(ulid, 1) & 248) >> 3));
  output = output || CHR(GET_BYTE(encoding, ((GET_BYTE(ulid, 1) & 7) << 2) | ((GET_BYTE(ulid, 2) & 192) >> 6)));
  output = output || CHR(GET_BYTE(encoding, (GET_BYTE(ulid, 2) & 62) >> 1));
  output = output || CHR(GET_BYTE(encoding, ((GET_BYTE(ulid, 2) & 1) << 4) | ((GET_BYTE(ulid, 3) & 240) >> 4)));
  output = output || CHR(GET_BYTE(encoding, ((GET_BYTE(ulid, 3) & 15) << 1) | ((GET_BYTE(ulid, 4) & 128) >> 7)));
  output = output || CHR(GET_BYTE(encoding, (GET_BYTE(ulid, 4) & 124) >> 2));
  output = output || CHR(GET_BYTE(encoding, ((GET_BYTE(ulid, 4) & 3) << 3) | ((GET_BYTE(ulid, 5) & 224) >> 5)));
  output = output || CHR(GET_BYTE(encoding, (GET_BYTE(ulid, 5) & 31)));

  -- Encode the entropy
  output = output || CHR(GET_BYTE(encoding, (GET_BYTE(ulid, 6) & 248) >> 3));
  output = output || CHR(GET_BYTE(encoding, ((GET_BYTE(ulid, 6) & 7) << 2) | ((GET_BYTE(ulid, 7) & 192) >> 6)));
  output = output || CHR(GET_BYTE(encoding, (GET_BYTE(ulid, 7) & 62) >> 1));
  output = output || CHR(GET_BYTE(encoding, ((GET_BYTE(ulid, 7) & 1) << 4) | ((GET_BYTE(ulid, 8) & 240) >> 4)));
  output = output || CHR(GET_BYTE(encoding, ((GET_BYTE(ulid, 8) & 15) << 1) | ((GET_BYTE(ulid, 9) & 128) >> 7)));
  output = output || CHR(GET_BYTE(encoding, (GET_BYTE(ulid, 9) & 124) >> 2));
  output = output || CHR(GET_BYTE(encoding, ((GET_BYTE(ulid, 9) & 3) << 3) | ((GET_BYTE(ulid, 10) & 224) >> 5)));
  output = output || CHR(GET_BYTE(encoding, (GET_BYTE(ulid, 10) & 31)));
  output = output || CHR(GET_BYTE(encoding, (GET_BYTE(ulid, 11) & 248) >> 3));
  output = output || CHR(GET_BYTE(encoding, ((GET_BYTE(ulid, 11) & 7) << 2) | ((GET_BYTE(ulid, 12) & 192) >> 6)));
  output = output || CHR(GET_BYTE(encoding, (GET_BYTE(ulid, 12) & 62) >> 1));
  output = output || CHR(GET_BYTE(encoding, ((GET_BYTE(ulid, 12) & 1) << 4) | ((GET_BYTE(ulid, 13) & 240) >> 4)));
  output = output || CHR(GET_BYTE(encoding, ((GET_BYTE(ulid, 13) & 15) << 1) | ((GET_BYTE(ulid, 14) & 128) >> 7)));
  output = output || CHR(GET_BYTE(encoding, (GET_BYTE(ulid, 14) & 124) >> 2));
  output = output || CHR(GET_BYTE(encoding, ((GET_BYTE(ulid, 14) & 3) << 3) | ((GET_BYTE(ulid, 15) & 224) >> 5)));
  output = output || CHR(GET_BYTE(encoding, (GET_BYTE(ulid, 15) & 31)));

  RETURN output;
END
$$;


--
-- Name: workers; Type: TABLE; Schema: apalis; Owner: -
--

CREATE TABLE apalis.workers (
    id text NOT NULL,
    worker_type text NOT NULL,
    storage_name text NOT NULL,
    layers text DEFAULT ''::text NOT NULL,
    last_seen timestamp with time zone DEFAULT now() NOT NULL,
    started_at timestamp with time zone
);


--
-- Name: _sqlx_migrations; Type: TABLE; Schema: cc_lb_scheduler; Owner: -
--

CREATE TABLE cc_lb_scheduler._sqlx_migrations (
    version bigint NOT NULL,
    description text NOT NULL,
    installed_on timestamp with time zone DEFAULT now() NOT NULL,
    success boolean NOT NULL,
    checksum bytea NOT NULL,
    execution_time bigint NOT NULL
);


--
-- Name: anthropic_compat_etags; Type: TABLE; Schema: cc_lb_scheduler; Owner: -
--

CREATE TABLE cc_lb_scheduler.anthropic_compat_etags (
    key text NOT NULL,
    last_applied_at_unix_secs bigint NOT NULL,
    last_value_hash text NOT NULL,
    CONSTRAINT anthropic_compat_etags_last_applied_at_unix_secs_check CHECK ((last_applied_at_unix_secs >= 0))
);


--
-- Name: oauth_usage_poll_cursors; Type: TABLE; Schema: cc_lb_scheduler; Owner: -
--

CREATE TABLE cc_lb_scheduler.oauth_usage_poll_cursors (
    upstream_id uuid NOT NULL,
    last_status integer,
    attempt_count integer DEFAULT 0 NOT NULL,
    last_observed_at_unix_secs bigint,
    CONSTRAINT oauth_usage_poll_cursors_attempt_count_check CHECK ((attempt_count >= 0)),
    CONSTRAINT oauth_usage_poll_cursors_last_observed_at_unix_secs_check CHECK (((last_observed_at_unix_secs IS NULL) OR (last_observed_at_unix_secs >= 0)))
);


--
-- Name: price_catalog_versions; Type: TABLE; Schema: cc_lb_scheduler; Owner: -
--

CREATE TABLE cc_lb_scheduler.price_catalog_versions (
    source text NOT NULL,
    fingerprint text NOT NULL,
    fetched_at_unix_secs bigint NOT NULL,
    CONSTRAINT price_catalog_versions_fetched_at_unix_secs_check CHECK ((fetched_at_unix_secs >= 0))
);


--
-- Name: _sqlx_migrations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public._sqlx_migrations (
    version bigint NOT NULL,
    description text NOT NULL,
    installed_on timestamp with time zone DEFAULT now() NOT NULL,
    success boolean NOT NULL,
    checksum bytea NOT NULL,
    execution_time bigint NOT NULL
);


--
-- Name: anthropic_compatibility_kv_v1; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.anthropic_compatibility_kv_v1 (
    key text NOT NULL,
    value text NOT NULL,
    last_updated_at_unix_secs bigint NOT NULL,
    last_attempt_at_unix_secs bigint NOT NULL,
    last_error text,
    source_url text,
    CONSTRAINT anthropic_compatibility_kv_v1_last_attempt_at_unix_secs_check CHECK ((last_attempt_at_unix_secs >= 0)),
    CONSTRAINT anthropic_compatibility_kv_v1_last_updated_at_unix_secs_check CHECK ((last_updated_at_unix_secs >= 0))
);


--
-- Name: api_key_concurrency_holds_v1; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.api_key_concurrency_holds_v1 (
    hold_id uuid NOT NULL,
    key_id text NOT NULL,
    writer_epoch uuid NOT NULL,
    acquired_at_unix_secs bigint NOT NULL,
    CONSTRAINT api_key_concurrency_holds_v1_acquired_at_unix_secs_check CHECK ((acquired_at_unix_secs >= 0))
);


--
-- Name: api_key_usage_buckets_v1; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.api_key_usage_buckets_v1 (
    writer_epoch uuid NOT NULL,
    key_id text NOT NULL,
    bucket_width_secs bigint NOT NULL,
    bucket_start_unix_secs bigint NOT NULL,
    requests bigint NOT NULL,
    input_tokens bigint NOT NULL,
    output_tokens bigint NOT NULL,
    cost_usd_micros bigint NOT NULL,
    CONSTRAINT api_key_usage_buckets_v1_bucket_start_unix_secs_check CHECK ((bucket_start_unix_secs >= 0)),
    CONSTRAINT api_key_usage_buckets_v1_bucket_width_secs_check CHECK ((bucket_width_secs > 0))
);


--
-- Name: api_key_usage_writers_v1; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.api_key_usage_writers_v1 (
    writer_epoch uuid NOT NULL,
    lease_until_unix_secs bigint NOT NULL,
    last_flush_id uuid,
    CONSTRAINT api_key_usage_writers_v1_lease_until_unix_secs_check CHECK ((lease_until_unix_secs >= 0))
);


--
-- Name: audit_log_v1; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.audit_log_v1 (
    seq bigint NOT NULL,
    ts timestamp with time zone NOT NULL,
    request_id text,
    principal_id text,
    route text,
    upstream text,
    model text,
    status integer,
    input_tokens bigint,
    output_tokens bigint,
    duration_ms bigint,
    agent_label text,
    payload bytea,
    api_key_id text,
    cost_usd_micros bigint,
    limit_violation text,
    admin_action text,
    actor text,
    actor_authority text,
    actor_subject text,
    actor_kind text,
    actor_email text,
    CONSTRAINT audit_log_v1_cost_usd_micros_check CHECK (((cost_usd_micros IS NULL) OR (cost_usd_micros >= 0)))
);


--
-- Name: audit_log_v1_seq_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.audit_log_v1_seq_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: audit_log_v1_seq_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.audit_log_v1_seq_seq OWNED BY public.audit_log_v1.seq;


--
-- Name: cache_keepalive_decisions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.cache_keepalive_decisions (
    source_ref_id text NOT NULL,
    decision text NOT NULL,
    reason text NOT NULL,
    generation bigint NOT NULL,
    ts bigint NOT NULL,
    principal_id text,
    session_key_hash text,
    upstream_id uuid,
    error text,
    ttl text,
    config_snapshot text,
    last_message_at_ms bigint NOT NULL
);


--
-- Name: cache_keepalive_sessions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.cache_keepalive_sessions (
    session_key_hash text NOT NULL,
    principal_id text NOT NULL,
    upstream_id uuid NOT NULL,
    generation bigint NOT NULL,
    refresh_count bigint NOT NULL,
    first_scheduled_at bigint NOT NULL,
    cache_anchor_at bigint NOT NULL,
    run_at bigint NOT NULL,
    ttl text NOT NULL,
    status text NOT NULL,
    enqueue_state text NOT NULL,
    current_job_key text NOT NULL,
    encrypted_payload bytea NOT NULL,
    terminal_reason text,
    expires_at bigint NOT NULL,
    created_at bigint NOT NULL,
    updated_at bigint NOT NULL,
    accounting_key_id text,
    running_since_unix_secs bigint,
    display_reason text DEFAULT ''::text NOT NULL,
    error text,
    config_snapshot text,
    last_message_at_ms bigint DEFAULT 0 NOT NULL,
    CONSTRAINT cache_keepalive_sessions_enqueue_state_check CHECK ((enqueue_state = ANY (ARRAY['pending'::text, 'enqueued'::text, 'running'::text]))),
    CONSTRAINT cache_keepalive_sessions_status_check CHECK ((status = ANY (ARRAY['active'::text, 'terminal'::text])))
);


--
-- Name: cache_keepalive_turns; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.cache_keepalive_turns (
    source_ref_id text NOT NULL,
    session_key_hash text NOT NULL,
    principal_id text NOT NULL,
    accounting_key_id text,
    upstream_id uuid NOT NULL,
    model text NOT NULL,
    input_tokens bigint NOT NULL,
    output_tokens bigint NOT NULL,
    cache_creation_input_tokens bigint NOT NULL,
    cache_creation_input_tokens_5m bigint NOT NULL,
    cache_creation_input_tokens_1h bigint NOT NULL,
    cache_read_input_tokens bigint NOT NULL,
    cost_micros bigint NOT NULL,
    hit_miss text NOT NULL,
    ts bigint NOT NULL
);


--
-- Name: config_draft_v1; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.config_draft_v1 (
    id text NOT NULL,
    config jsonb NOT NULL,
    revision bigint NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    CONSTRAINT config_draft_v1_id_check CHECK ((id = 'singleton'::text))
);


--
-- Name: config_history_v1; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.config_history_v1 (
    revision bigint NOT NULL,
    created_at timestamp with time zone NOT NULL,
    applied_at_unix_secs bigint NOT NULL
);


--
-- Name: managed_api_key_index_v1; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.managed_api_key_index_v1 (
    index_hash bytea NOT NULL,
    principal_id text NOT NULL,
    key_id text NOT NULL
);


--
-- Name: managed_api_keys_v1; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.managed_api_keys_v1 (
    principal_id text NOT NULL,
    key_id text NOT NULL,
    label text NOT NULL,
    issued_at_unix_secs bigint NOT NULL,
    revoked_at_unix_secs bigint,
    key_hash_b64 text NOT NULL,
    verify_hash bytea NOT NULL,
    secret_salt bytea NOT NULL,
    limit_overrides jsonb DEFAULT '[]'::jsonb NOT NULL,
    status text NOT NULL,
    expires_at_unix_secs bigint,
    last_4 text NOT NULL,
    description text,
    index_hash bytea NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT managed_api_keys_v1_status_check CHECK ((status = ANY (ARRAY['active'::text, 'revoked'::text])))
);


--
-- Name: meta; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.meta (
    key text NOT NULL,
    value text NOT NULL
);


--
-- Name: metadata_tier_mapping_override_v1; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.metadata_tier_mapping_override_v1 (
    organization_type text NOT NULL,
    rate_limit_tier text NOT NULL,
    seat_tier text NOT NULL,
    tier_key text NOT NULL,
    effective_from_unix_millis bigint NOT NULL,
    effective_to_unix_millis bigint,
    provenance text NOT NULL,
    created_at_unix_millis bigint NOT NULL,
    CONSTRAINT metadata_tier_mapping_override_effective_from_unix_millis_check CHECK ((effective_from_unix_millis >= 0)),
    CONSTRAINT metadata_tier_mapping_override_v1_check CHECK (((effective_to_unix_millis IS NULL) OR (effective_to_unix_millis > effective_from_unix_millis))),
    CONSTRAINT metadata_tier_mapping_override_v1_created_at_unix_millis_check CHECK ((created_at_unix_millis >= 0)),
    CONSTRAINT metadata_tier_mapping_override_v1_provenance_check CHECK ((length(btrim(provenance)) > 0)),
    CONSTRAINT metadata_tier_mapping_override_v1_tier_key_check CHECK ((tier_key = ANY (ARRAY['pro'::text, 'team_standard'::text, 'max_5x'::text, 'team_premium'::text, 'max_20x'::text])))
);


--
-- Name: oauth_pkce_flows_v1; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.oauth_pkce_flows_v1 (
    state_token text NOT NULL,
    encrypted_payload bytea NOT NULL,
    created_at_unix_secs bigint NOT NULL,
    expires_at_unix_secs bigint NOT NULL,
    CONSTRAINT oauth_pkce_flows_v1_created_at_unix_secs_check CHECK ((created_at_unix_secs >= 0)),
    CONSTRAINT oauth_pkce_flows_v1_expires_at_unix_secs_check CHECK ((expires_at_unix_secs >= 0))
);


--
-- Name: organization_metadata_v1; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.organization_metadata_v1 (
    organization_uuid text NOT NULL,
    organization_name text,
    organization_type text,
    rate_limit_tier text,
    has_extra_usage_enabled boolean,
    billing_type text,
    subscription_created_at_unix_secs bigint,
    account_email text,
    account_display_name text,
    account_uuid text,
    overage_credit_amount_minor_units bigint,
    overage_credit_currency text,
    overage_credit_granted boolean,
    overage_credit_eligible boolean,
    observed_at_unix_millis bigint NOT NULL,
    last_error text,
    raw_profile text,
    raw_overage_grant text,
    seat_tier text
);


--
-- Name: overview_excluded_error_rollups_v1; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.overview_excluded_error_rollups_v1 (
    resolution text NOT NULL,
    bucket_start_unix_secs bigint NOT NULL,
    error_count bigint NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    CONSTRAINT overview_excluded_error_rollups_v1_bucket_start_unix_secs_check CHECK ((bucket_start_unix_secs >= 0)),
    CONSTRAINT overview_excluded_error_rollups_v1_error_count_check CHECK ((error_count >= 0)),
    CONSTRAINT overview_excluded_error_rollups_v1_resolution_check CHECK ((resolution = ANY (ARRAY['minute'::text, 'hour'::text])))
);


--
-- Name: plan_tier_ratio_history_v1; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.plan_tier_ratio_history_v1 (
    tier_key text NOT NULL,
    pro_relative_ratio double precision NOT NULL,
    effective_from_unix_millis bigint NOT NULL,
    effective_to_unix_millis bigint,
    provenance text NOT NULL,
    created_at_unix_millis bigint NOT NULL,
    CONSTRAINT plan_tier_ratio_history_v1_check CHECK (((effective_to_unix_millis IS NULL) OR (effective_to_unix_millis > effective_from_unix_millis))),
    CONSTRAINT plan_tier_ratio_history_v1_created_at_unix_millis_check CHECK ((created_at_unix_millis >= 0)),
    CONSTRAINT plan_tier_ratio_history_v1_effective_from_unix_millis_check CHECK ((effective_from_unix_millis >= 0)),
    CONSTRAINT plan_tier_ratio_history_v1_pro_relative_ratio_check CHECK (((pro_relative_ratio > (0.0)::double precision) AND (pro_relative_ratio < 'Infinity'::double precision))),
    CONSTRAINT plan_tier_ratio_history_v1_provenance_check CHECK ((length(btrim(provenance)) > 0)),
    CONSTRAINT plan_tier_ratio_history_v1_tier_key_check CHECK ((tier_key = ANY (ARRAY['pro'::text, 'team_standard'::text, 'max_5x'::text, 'team_premium'::text, 'max_20x'::text])))
);


--
-- Name: plugin_chains_v2; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.plugin_chains_v2 (
    id uuid NOT NULL,
    principal_id uuid NOT NULL,
    slot text NOT NULL,
    order_value bigint NOT NULL,
    wasm_registry_id uuid NOT NULL,
    config jsonb NOT NULL,
    revision bigint DEFAULT 0 NOT NULL,
    CONSTRAINT plugin_chains_v2_revision_check CHECK ((revision >= 0)),
    CONSTRAINT plugin_chains_v2_slot_check CHECK ((slot = ANY (ARRAY['router'::text, 'shape'::text])))
);


--
-- Name: pool_subscription_quota_history_v1; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.pool_subscription_quota_history_v1 (
    snapshot_at_unix_secs bigint NOT NULL,
    quota_window text NOT NULL,
    utilization double precision,
    weighted_utilization_sum double precision NOT NULL,
    capacity_ratio_sum double precision NOT NULL,
    eligible_upstreams bigint NOT NULL,
    contributing_upstreams bigint NOT NULL,
    stale_upstreams bigint NOT NULL,
    missing_observation_upstreams bigint NOT NULL,
    missing_metadata_upstreams bigint NOT NULL,
    header_contributing_upstreams bigint NOT NULL,
    api_contributing_upstreams bigint NOT NULL,
    max_observed_at_unix_millis bigint,
    computed_at_unix_millis bigint NOT NULL,
    policy_version integer DEFAULT 1 NOT NULL,
    CONSTRAINT pool_subscription_quota_history_v1_quota_window_check_fable CHECK ((quota_window = ANY (ARRAY['5h'::text, '7d'::text, '7d_sonnet'::text, '7d_opus'::text, '7d_fable'::text, 'overage'::text, 'unified'::text])))
);


--
-- Name: price_catalog_snapshots_v1; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.price_catalog_snapshots_v1 (
    id bigint NOT NULL,
    payload text NOT NULL,
    payload_hash text NOT NULL,
    fetched_at_ms bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: price_catalog_snapshots_v1_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.price_catalog_snapshots_v1_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: price_catalog_snapshots_v1_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.price_catalog_snapshots_v1_id_seq OWNED BY public.price_catalog_snapshots_v1.id;


--
-- Name: principals_v1; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.principals_v1 (
    id uuid NOT NULL,
    name text NOT NULL,
    kind text NOT NULL,
    allowed_models jsonb NOT NULL,
    default_limits jsonb NOT NULL,
    enabled boolean NOT NULL,
    deleted_at timestamp with time zone,
    revision bigint NOT NULL,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    allowed_upstreams uuid[] DEFAULT '{}'::uuid[] NOT NULL,
    router_terminal_strategy text DEFAULT 'first-pick'::text NOT NULL,
    cache_keepalive jsonb,
    CONSTRAINT principals_v1_allowed_models_check CHECK ((jsonb_typeof(allowed_models) = 'array'::text)),
    CONSTRAINT principals_v1_default_limits_check CHECK ((jsonb_typeof(default_limits) = 'array'::text)),
    CONSTRAINT principals_v1_kind_check CHECK ((kind = ANY (ARRAY['machine'::text, 'human'::text, 'admin'::text]))),
    CONSTRAINT principals_v1_revision_check CHECK ((revision >= 0)),
    CONSTRAINT principals_v1_router_terminal_strategy_check CHECK ((router_terminal_strategy = ANY (ARRAY['random'::text, 'first-pick'::text])))
);


--
-- Name: prompt_cache_observations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.prompt_cache_observations (
    upstream_id uuid NOT NULL,
    canonical_model_id text NOT NULL,
    v3_prefix_key text NOT NULL,
    ttl_class smallint NOT NULL,
    expires_at bigint NOT NULL,
    last_observed_at bigint NOT NULL,
    hash_schema_version smallint NOT NULL,
    prefix_content_block_index bigint NOT NULL,
    estimated_prefix_tokens bigint NOT NULL,
    token_estimate_source text NOT NULL,
    CONSTRAINT prompt_cache_observations_estimated_prefix_tokens_check CHECK ((estimated_prefix_tokens >= 0)),
    CONSTRAINT prompt_cache_observations_prefix_content_block_index_check CHECK ((prefix_content_block_index >= 0))
);


--
-- Name: request_events_v1; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.request_events_v1 (
    seq bigint NOT NULL,
    ts timestamp with time zone NOT NULL,
    principal_id text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    upstream_id uuid,
    key_id text,
    model text,
    upstream_name text,
    thread_id text,
    input_tokens bigint,
    output_tokens bigint,
    cache_creation_input_tokens bigint,
    cache_read_input_tokens bigint,
    event_id text NOT NULL,
    error_code text,
    upstream_error_type text,
    upstream_error_message text,
    thinking_tokens bigint,
    service_tier text,
    cache_creation_input_tokens_5m bigint,
    cache_creation_input_tokens_1h bigint,
    tx_id xid8 DEFAULT pg_current_xact_id() NOT NULL,
    token_estimate_source text,
    thinking_budget_tokens bigint,
    reasoning_effort text,
    source_kind text,
    source_ref_id text,
    list_ts_ms bigint DEFAULT 0 NOT NULL,
    list_event_key text DEFAULT ''::text NOT NULL,
    list_status integer,
    observed_session_id text,
    request_kind text,
    claude_agent_id text,
    claude_parent_agent_id text,
    parent_session_id text,
    client_app text,
    session_id_source text,
    list_cost_usd_micros bigint,
    list_cost_input_micros bigint,
    list_cost_output_micros bigint,
    list_cost_cache_creation_5m_micros bigint,
    list_cost_cache_creation_1h_micros bigint,
    list_cost_cache_read_micros bigint,
    event_kind text,
    request_id text DEFAULT ''::text NOT NULL,
    principal_kind text,
    duration_ms bigint DEFAULT 0 NOT NULL,
    auth_ms bigint,
    route_ms bigint,
    limit_reserve_ms bigint,
    bulkhead_wait_ms bigint,
    dns_ms bigint,
    connect_ms bigint,
    connection_reused boolean,
    proxy_setup_ms bigint,
    shape_ms bigint,
    sign_ms bigint,
    upstream_ttfb_ms bigint,
    upstream_body_ms bigint,
    finalize_ms bigint,
    json_parse_ms double precision,
    cache_structure_ms double precision,
    cache_token_key_ms double precision,
    cache_count_lookup_ms double precision,
    cache_tokenizer_queue_ms double precision,
    cache_serialize_ms double precision,
    cache_tokenize_ms double precision,
    prepare_signer_ms double precision,
    request_body_read_ms bigint,
    request_body_bytes bigint,
    request_body_first_chunk_ms double precision,
    request_body_receive_ms double precision,
    request_body_wait_ms double precision,
    request_body_process_ms double precision,
    request_body_chunk_count bigint,
    response_body_wait_ms double precision,
    response_body_process_ms double precision,
    response_body_downstream_poll_gap_ms double precision,
    retry_overhead_ms double precision,
    body_chunk_count bigint,
    body_bytes bigint,
    stream_message_start_ms bigint,
    stream_content_block_start_ms bigint,
    stream_first_content_delta_ms bigint,
    stream_last_content_delta_ms bigint,
    stream_message_stop_ms bigint,
    stream_last_chunk_ms bigint,
    stream_total_ms bigint,
    sse_event_count bigint,
    content_delta_count bigint,
    ping_count bigint,
    inter_token_avg_ms bigint,
    upstream_http_version text,
    upstream_request_id text,
    upstream_content_encoding text,
    upstream_content_length bigint,
    upstream_body_bytes bigint,
    upstream_body_end text,
    upstream_body_error_cause text,
    upstream_body_error_io_kind text,
    upstream_body_error_h2_reason text,
    upstream_stream_warning_type text,
    upstream_stream_warning_message text,
    CONSTRAINT request_events_v1_cache_creation_input_tokens_check CHECK (((cache_creation_input_tokens IS NULL) OR (cache_creation_input_tokens >= 0))),
    CONSTRAINT request_events_v1_cache_read_input_tokens_check CHECK (((cache_read_input_tokens IS NULL) OR (cache_read_input_tokens >= 0))),
    CONSTRAINT request_events_v1_input_tokens_check CHECK (((input_tokens IS NULL) OR (input_tokens >= 0))),
    CONSTRAINT request_events_v1_output_tokens_check CHECK (((output_tokens IS NULL) OR (output_tokens >= 0)))
);


--
-- Name: request_events_v1_seq_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.request_events_v1_seq_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: request_events_v1_seq_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.request_events_v1_seq_seq OWNED BY public.request_events_v1.seq;


--
-- Name: upstream_affinity_v1; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.upstream_affinity_v1 (
    principal_id text NOT NULL,
    provider text NOT NULL,
    kind text NOT NULL,
    value_sha256 bytea NOT NULL,
    upstream_id uuid NOT NULL,
    observed_at_unix_secs bigint NOT NULL,
    expires_at_unix_secs bigint,
    CONSTRAINT upstream_affinity_v1_expires_at_unix_secs_check CHECK (((expires_at_unix_secs IS NULL) OR (expires_at_unix_secs >= 0))),
    CONSTRAINT upstream_affinity_v1_kind_check CHECK ((kind = ANY (ARRAY['anthropic_web_search_encrypted_content'::text, 'anthropic_file_id'::text, 'anthropic_container_id'::text, 'openai_response_id'::text, 'openai_conversation_id'::text, 'gemini_cached_content'::text]))),
    CONSTRAINT upstream_affinity_v1_observed_at_unix_secs_check CHECK ((observed_at_unix_secs >= 0)),
    CONSTRAINT upstream_affinity_v1_value_sha256_check CHECK ((octet_length(value_sha256) = 32))
);


--
-- Name: upstream_api_key_details_v1; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.upstream_api_key_details_v1 (
    upstream_id uuid NOT NULL,
    verified boolean NOT NULL,
    key_class text NOT NULL,
    key_hint text NOT NULL,
    api_key_id text,
    needs_workspace boolean NOT NULL,
    capabilities_json text NOT NULL,
    remediation_json text DEFAULT '[]'::text NOT NULL,
    usage_report_state text,
    last_error text,
    checked_at_unix_secs bigint,
    last_attempt_at_unix_secs bigint,
    access_revision bigint DEFAULT 0 NOT NULL,
    usage_poll_failures integer DEFAULT 0 NOT NULL,
    usage_next_poll_after_unix_secs bigint,
    usage_last_success_at_unix_secs bigint,
    usage_last_attempt_at_unix_secs bigint,
    usage_poll_claim_token text,
    usage_poll_claim_lease_until_unix_secs bigint,
    created_at_unix_secs bigint NOT NULL,
    updated_at_unix_secs bigint NOT NULL,
    CONSTRAINT upstream_api_key_details_v1_access_revision_check CHECK ((access_revision >= 0)),
    CONSTRAINT upstream_api_key_details_v1_checked_at_unix_secs_check CHECK (((checked_at_unix_secs IS NULL) OR (checked_at_unix_secs >= 0))),
    CONSTRAINT upstream_api_key_details_v1_created_at_unix_secs_check CHECK ((created_at_unix_secs >= 0)),
    CONSTRAINT upstream_api_key_details_v1_key_class_check CHECK ((key_class = ANY (ARRAY['admin'::text, 'enterprise'::text, 'oauth_token'::text, 'workspace_bound'::text, 'org_scoped'::text, 'unknown'::text]))),
    CONSTRAINT upstream_api_key_details_v1_last_attempt_at_unix_secs_check CHECK (((last_attempt_at_unix_secs IS NULL) OR (last_attempt_at_unix_secs >= 0))),
    CONSTRAINT upstream_api_key_details_v1_updated_at_unix_secs_check CHECK ((updated_at_unix_secs >= 0)),
    CONSTRAINT upstream_api_key_details_v1_usage_last_attempt_at_unix_se_check CHECK (((usage_last_attempt_at_unix_secs IS NULL) OR (usage_last_attempt_at_unix_secs >= 0))),
    CONSTRAINT upstream_api_key_details_v1_usage_last_success_at_unix_se_check CHECK (((usage_last_success_at_unix_secs IS NULL) OR (usage_last_success_at_unix_secs >= 0))),
    CONSTRAINT upstream_api_key_details_v1_usage_next_poll_after_unix_se_check CHECK (((usage_next_poll_after_unix_secs IS NULL) OR (usage_next_poll_after_unix_secs >= 0))),
    CONSTRAINT upstream_api_key_details_v1_usage_poll_claim_lease_until__check CHECK (((usage_poll_claim_lease_until_unix_secs IS NULL) OR (usage_poll_claim_lease_until_unix_secs >= 0))),
    CONSTRAINT upstream_api_key_details_v1_usage_poll_failures_check CHECK ((usage_poll_failures >= 0)),
    CONSTRAINT upstream_api_key_details_v1_usage_report_state_check CHECK (((usage_report_state IS NULL) OR (usage_report_state = ANY (ARRAY['ok'::text, 'denied'::text, 'error'::text, 'not_applicable'::text]))))
);


--
-- Name: upstream_api_key_secret_v1; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.upstream_api_key_secret_v1 (
    upstream_id uuid NOT NULL,
    api_key_ciphertext bytea,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL
);


--
-- Name: upstream_api_usage_v1; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.upstream_api_usage_v1 (
    upstream_id uuid NOT NULL,
    attribution text NOT NULL,
    subject_id text NOT NULL,
    bucket_width_secs integer NOT NULL,
    bucket_start_unix_secs bigint NOT NULL,
    model text NOT NULL,
    uncached_input_tokens bigint DEFAULT 0 NOT NULL,
    cache_creation_5m_input_tokens bigint DEFAULT 0 NOT NULL,
    cache_creation_1h_input_tokens bigint DEFAULT 0 NOT NULL,
    cache_read_input_tokens bigint DEFAULT 0 NOT NULL,
    output_tokens bigint DEFAULT 0 NOT NULL,
    list_cost_micros bigint DEFAULT 0 NOT NULL,
    observed_at_unix_secs bigint NOT NULL,
    CONSTRAINT upstream_api_usage_v1_attribution_check CHECK ((attribution = ANY (ARRAY['api_key'::text, 'workspace'::text]))),
    CONSTRAINT upstream_api_usage_v1_bucket_start_unix_secs_check CHECK ((bucket_start_unix_secs >= 0)),
    CONSTRAINT upstream_api_usage_v1_bucket_width_secs_check CHECK ((bucket_width_secs = ANY (ARRAY[3600, 86400]))),
    CONSTRAINT upstream_api_usage_v1_cache_creation_1h_input_tokens_check CHECK ((cache_creation_1h_input_tokens >= 0)),
    CONSTRAINT upstream_api_usage_v1_cache_creation_5m_input_tokens_check CHECK ((cache_creation_5m_input_tokens >= 0)),
    CONSTRAINT upstream_api_usage_v1_cache_read_input_tokens_check CHECK ((cache_read_input_tokens >= 0)),
    CONSTRAINT upstream_api_usage_v1_list_cost_micros_check CHECK ((list_cost_micros >= 0)),
    CONSTRAINT upstream_api_usage_v1_observed_at_unix_secs_check CHECK ((observed_at_unix_secs >= 0)),
    CONSTRAINT upstream_api_usage_v1_output_tokens_check CHECK ((output_tokens >= 0)),
    CONSTRAINT upstream_api_usage_v1_uncached_input_tokens_check CHECK ((uncached_input_tokens >= 0))
);


--
-- Name: upstream_oauth_token_v1; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.upstream_oauth_token_v1 (
    upstream_id uuid NOT NULL,
    oauth_credentials_ciphertext bytea,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    oauth_token_generation bigint DEFAULT 0 NOT NULL,
    never_refresh boolean DEFAULT false NOT NULL
);


--
-- Name: upstream_plan_tier_history_v1; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.upstream_plan_tier_history_v1 (
    upstream_id uuid NOT NULL,
    organization_uuid text,
    organization_type text,
    rate_limit_tier text,
    seat_tier text,
    tier_key text,
    resolution_source text NOT NULL,
    resolved_ratio_snapshot double precision,
    observed_at_unix_millis bigint NOT NULL,
    effective_from_unix_millis bigint NOT NULL,
    effective_to_unix_millis bigint,
    provenance text NOT NULL,
    created_at_unix_millis bigint NOT NULL,
    CONSTRAINT upstream_plan_tier_history_v1_check CHECK (((effective_to_unix_millis IS NULL) OR (effective_to_unix_millis > effective_from_unix_millis))),
    CONSTRAINT upstream_plan_tier_history_v1_check1 CHECK (((tier_key IS NULL) = (resolution_source = 'unknown'::text))),
    CONSTRAINT upstream_plan_tier_history_v1_check2 CHECK (((resolution_source <> 'unknown'::text) OR (resolved_ratio_snapshot IS NULL))),
    CONSTRAINT upstream_plan_tier_history_v1_created_at_unix_millis_check CHECK ((created_at_unix_millis >= 0)),
    CONSTRAINT upstream_plan_tier_history_v1_effective_from_unix_millis_check CHECK ((effective_from_unix_millis >= 0)),
    CONSTRAINT upstream_plan_tier_history_v1_observed_at_unix_millis_check CHECK ((observed_at_unix_millis >= 0)),
    CONSTRAINT upstream_plan_tier_history_v1_provenance_check CHECK ((length(btrim(provenance)) > 0)),
    CONSTRAINT upstream_plan_tier_history_v1_resolution_source_check CHECK ((resolution_source = ANY (ARRAY['override'::text, 'builtin'::text, 'unknown'::text]))),
    CONSTRAINT upstream_plan_tier_history_v1_resolved_ratio_snapshot_check CHECK (((resolved_ratio_snapshot IS NULL) OR ((resolved_ratio_snapshot > (0.0)::double precision) AND (resolved_ratio_snapshot < 'Infinity'::double precision)))),
    CONSTRAINT upstream_plan_tier_history_v1_tier_key_check CHECK (((tier_key IS NULL) OR (tier_key = ANY (ARRAY['pro'::text, 'team_standard'::text, 'max_5x'::text, 'team_premium'::text, 'max_20x'::text]))))
);


--
-- Name: upstream_rate_limit_state_v1; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.upstream_rate_limit_state_v1 (
    upstream_id uuid NOT NULL,
    "window" text NOT NULL,
    kind text NOT NULL,
    limit_value bigint,
    remaining bigint,
    reset text,
    observed_at_unix_secs bigint NOT NULL,
    CONSTRAINT upstream_rate_limit_state_v1_kind_check CHECK ((kind = ANY (ARRAY['requests'::text, 'tokens'::text, 'input_tokens'::text, 'output_tokens'::text])))
);


--
-- Name: upstream_spec_v1; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.upstream_spec_v1 (
    id uuid NOT NULL,
    name text NOT NULL,
    kind text NOT NULL,
    base_url text,
    enabled boolean DEFAULT true NOT NULL,
    warmup_enabled boolean DEFAULT false NOT NULL,
    warmup_dialect_plugin jsonb,
    spec_revision bigint NOT NULL,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    deleted_at timestamp with time zone,
    workspace_id text,
    CONSTRAINT upstream_spec_v1_kind_check CHECK ((kind = ANY (ARRAY['anthropic_api_key'::text, 'anthropic_oauth'::text, 'openai_api_key'::text, 'openai_oauth'::text, 'gemini_api_key'::text, 'gemini_oauth'::text]))),
    CONSTRAINT upstream_spec_v1_spec_revision_check CHECK ((spec_revision >= 0))
);


--
-- Name: upstream_status_v1; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.upstream_status_v1 (
    upstream_id uuid NOT NULL,
    last_apply_error text,
    last_apply_at timestamp with time zone,
    updated_at timestamp with time zone NOT NULL,
    last_warmup_at bigint
);


--
-- Name: upstream_subscription_metadata_v1; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.upstream_subscription_metadata_v1 (
    upstream_id uuid NOT NULL,
    organization_uuid text,
    organization_role text,
    workspace_role text,
    observed_at_unix_millis bigint NOT NULL,
    last_error text,
    raw_roles text,
    raw_bootstrap text
);


--
-- Name: upstream_subscription_quota_checkpoints_v1; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.upstream_subscription_quota_checkpoints_v1 (
    upstream_id uuid NOT NULL,
    "window" text NOT NULL,
    source text NOT NULL,
    changed_at_unix_millis bigint NOT NULL,
    sample_id uuid NOT NULL,
    semantic_fingerprint bytea NOT NULL,
    sample_kind text DEFAULT 'sample'::text NOT NULL,
    representative_claim text,
    utilization double precision,
    status text,
    resets_at_unix_secs bigint,
    surpassed_threshold double precision,
    fallback_percentage double precision,
    fallback_available boolean,
    overage_in_use boolean,
    overage_period_monthly_utilization double precision,
    upgrade_paths text,
    disabled_reason text,
    extra_usage_enabled boolean,
    extra_usage_monthly_limit double precision,
    extra_usage_used_credits double precision,
    ingested_at_unix_millis bigint NOT NULL,
    CONSTRAINT upstream_subscription_quota__overage_period_monthly_util_check2 CHECK (((overage_period_monthly_utilization IS NULL) OR ((overage_period_monthly_utilization >= (0.0)::double precision) AND (overage_period_monthly_utilization <= (1.0)::double precision)))),
    CONSTRAINT upstream_subscription_quota_check_ingested_at_unix_millis_check CHECK ((ingested_at_unix_millis >= 0)),
    CONSTRAINT upstream_subscription_quota_checkp_changed_at_unix_millis_check CHECK ((changed_at_unix_millis >= 0)),
    CONSTRAINT upstream_subscription_quota_checkpoi_semantic_fingerprint_check CHECK ((octet_length(semantic_fingerprint) = 32)),
    CONSTRAINT upstream_subscription_quota_checkpoin_fallback_percentage_check CHECK (((fallback_percentage IS NULL) OR ((fallback_percentage >= (0.0)::double precision) AND (fallback_percentage <= (1.0)::double precision)))),
    CONSTRAINT upstream_subscription_quota_checkpoin_resets_at_unix_secs_check CHECK (((resets_at_unix_secs IS NULL) OR (resets_at_unix_secs >= 0))),
    CONSTRAINT upstream_subscription_quota_checkpoin_surpassed_threshold_check CHECK (((surpassed_threshold IS NULL) OR ((surpassed_threshold >= (0.0)::double precision) AND (surpassed_threshold <= (1.0)::double precision)))),
    CONSTRAINT upstream_subscription_quota_checkpoints_v1_sample_kind_check CHECK ((sample_kind = ANY (ARRAY['sample'::text, 'process_start'::text]))),
    CONSTRAINT upstream_subscription_quota_checkpoints_v1_source_check CHECK ((source = ANY (ARRAY['header'::text, 'api'::text]))),
    CONSTRAINT upstream_subscription_quota_checkpoints_v1_status_check CHECK (((status IS NULL) OR (status = ANY (ARRAY['allowed'::text, 'allowed_warning'::text, 'rejected'::text])))),
    CONSTRAINT upstream_subscription_quota_checkpoints_v1_utilization_check CHECK (((utilization IS NULL) OR ((utilization >= (0.0)::double precision) AND (utilization <= (1.0)::double precision)))),
    CONSTRAINT upstream_subscription_quota_checkpoints_v1_window_check CHECK (("window" = ANY (ARRAY['5h'::text, '7d'::text, '7d_sonnet'::text, '7d_opus'::text, '7d_fable'::text, 'overage'::text, 'unified'::text])))
);


--
-- Name: upstream_subscription_quota_latest_v1; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.upstream_subscription_quota_latest_v1 (
    upstream_id uuid NOT NULL,
    "window" text NOT NULL,
    source text NOT NULL,
    sample_kind text DEFAULT 'sample'::text NOT NULL,
    observed_at_unix_millis bigint NOT NULL,
    sample_id uuid NOT NULL,
    utilization double precision,
    status text,
    resets_at_unix_secs bigint,
    surpassed_threshold double precision,
    representative_claim text,
    disabled_reason text,
    extra_usage_enabled boolean,
    extra_usage_monthly_limit double precision,
    extra_usage_used_credits double precision,
    ingested_at_unix_millis bigint NOT NULL,
    fallback_percentage double precision,
    fallback_available boolean,
    overage_in_use boolean,
    overage_period_monthly_utilization double precision,
    upgrade_paths text,
    CONSTRAINT upstream_subscription_quota__overage_period_monthly_util_check1 CHECK (((overage_period_monthly_utilization IS NULL) OR ((overage_period_monthly_utilization >= (0.0)::double precision) AND (overage_period_monthly_utilization <= (1.0)::double precision)))),
    CONSTRAINT upstream_subscription_quota_lates_ingested_at_unix_millis_check CHECK ((ingested_at_unix_millis >= 0)),
    CONSTRAINT upstream_subscription_quota_lates_observed_at_unix_millis_check CHECK ((observed_at_unix_millis >= 0)),
    CONSTRAINT upstream_subscription_quota_latest_surpassed_threshold_range CHECK (((surpassed_threshold IS NULL) OR ((surpassed_threshold >= (0.0)::double precision) AND (surpassed_threshold <= (1.0)::double precision)))),
    CONSTRAINT upstream_subscription_quota_latest_v1_fallback_percentage_check CHECK (((fallback_percentage IS NULL) OR ((fallback_percentage >= (0.0)::double precision) AND (fallback_percentage <= (1.0)::double precision)))),
    CONSTRAINT upstream_subscription_quota_latest_v1_resets_at_unix_secs_check CHECK (((resets_at_unix_secs IS NULL) OR (resets_at_unix_secs >= 0))),
    CONSTRAINT upstream_subscription_quota_latest_v1_sample_kind_check CHECK ((sample_kind = ANY (ARRAY['sample'::text, 'absent'::text, 'process_start'::text]))),
    CONSTRAINT upstream_subscription_quota_latest_v1_source_check CHECK ((source = ANY (ARRAY['header'::text, 'api'::text]))),
    CONSTRAINT upstream_subscription_quota_latest_v1_status_check CHECK (((status IS NULL) OR (status = ANY (ARRAY['allowed'::text, 'allowed_warning'::text, 'rejected'::text])))),
    CONSTRAINT upstream_subscription_quota_latest_v1_utilization_check CHECK (((utilization IS NULL) OR ((utilization >= (0.0)::double precision) AND (utilization <= (1.0)::double precision)))),
    CONSTRAINT upstream_subscription_quota_latest_v1_window_check CHECK (("window" = ANY (ARRAY['5h'::text, '7d'::text, '7d_sonnet'::text, '7d_opus'::text, '7d_fable'::text, 'overage'::text, 'unified'::text])))
);


--
-- Name: usage_rollup_checkpoints_v1; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.usage_rollup_checkpoints_v1 (
    id text NOT NULL,
    value bigint NOT NULL,
    value_xid xid8 DEFAULT '0'::xid8 NOT NULL
);


--
-- Name: usage_rollups_v2; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.usage_rollups_v2 (
    resolution text NOT NULL,
    bucket_start_unix_secs bigint NOT NULL,
    principal_id text NOT NULL,
    upstream_id uuid NOT NULL,
    upstream_name text NOT NULL,
    model text NOT NULL,
    request_count bigint DEFAULT 0 NOT NULL,
    input_tokens bigint DEFAULT 0 NOT NULL,
    output_tokens bigint DEFAULT 0 NOT NULL,
    cache_creation_input_tokens bigint DEFAULT 0 NOT NULL,
    cache_read_input_tokens bigint DEFAULT 0 NOT NULL,
    error_count bigint DEFAULT 0 NOT NULL,
    latency_count bigint DEFAULT 0 NOT NULL,
    latency_ms_sum bigint DEFAULT 0 NOT NULL,
    latency_ms_min bigint,
    latency_ms_max bigint,
    proxy_setup_ms_count bigint DEFAULT 0 NOT NULL,
    proxy_setup_ms_sum bigint DEFAULT 0 NOT NULL,
    shape_ms_count bigint DEFAULT 0 NOT NULL,
    shape_ms_sum bigint DEFAULT 0 NOT NULL,
    sign_ms_count bigint DEFAULT 0 NOT NULL,
    sign_ms_sum bigint DEFAULT 0 NOT NULL,
    upstream_ttfb_ms_count bigint DEFAULT 0 NOT NULL,
    upstream_ttfb_ms_sum bigint DEFAULT 0 NOT NULL,
    upstream_body_ms_count bigint DEFAULT 0 NOT NULL,
    upstream_body_ms_sum bigint DEFAULT 0 NOT NULL,
    virtual_cost_micros bigint DEFAULT 0 NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    CONSTRAINT usage_rollups_v2_bucket_start_unix_secs_check CHECK ((bucket_start_unix_secs >= 0)),
    CONSTRAINT usage_rollups_v2_cache_creation_input_tokens_check CHECK ((cache_creation_input_tokens >= 0)),
    CONSTRAINT usage_rollups_v2_cache_read_input_tokens_check CHECK ((cache_read_input_tokens >= 0)),
    CONSTRAINT usage_rollups_v2_error_count_check CHECK ((error_count >= 0)),
    CONSTRAINT usage_rollups_v2_input_tokens_check CHECK ((input_tokens >= 0)),
    CONSTRAINT usage_rollups_v2_latency_count_check CHECK ((latency_count >= 0)),
    CONSTRAINT usage_rollups_v2_latency_ms_max_check CHECK ((latency_ms_max >= 0)),
    CONSTRAINT usage_rollups_v2_latency_ms_min_check CHECK ((latency_ms_min >= 0)),
    CONSTRAINT usage_rollups_v2_latency_ms_sum_check CHECK ((latency_ms_sum >= 0)),
    CONSTRAINT usage_rollups_v2_output_tokens_check CHECK ((output_tokens >= 0)),
    CONSTRAINT usage_rollups_v2_proxy_setup_ms_count_check CHECK ((proxy_setup_ms_count >= 0)),
    CONSTRAINT usage_rollups_v2_proxy_setup_ms_sum_check CHECK ((proxy_setup_ms_sum >= 0)),
    CONSTRAINT usage_rollups_v2_request_count_check CHECK ((request_count >= 0)),
    CONSTRAINT usage_rollups_v2_resolution_check CHECK ((resolution = ANY (ARRAY['minute'::text, 'hour'::text]))),
    CONSTRAINT usage_rollups_v2_shape_ms_count_check CHECK ((shape_ms_count >= 0)),
    CONSTRAINT usage_rollups_v2_shape_ms_sum_check CHECK ((shape_ms_sum >= 0)),
    CONSTRAINT usage_rollups_v2_sign_ms_count_check CHECK ((sign_ms_count >= 0)),
    CONSTRAINT usage_rollups_v2_sign_ms_sum_check CHECK ((sign_ms_sum >= 0)),
    CONSTRAINT usage_rollups_v2_upstream_body_ms_count_check CHECK ((upstream_body_ms_count >= 0)),
    CONSTRAINT usage_rollups_v2_upstream_body_ms_sum_check CHECK ((upstream_body_ms_sum >= 0)),
    CONSTRAINT usage_rollups_v2_upstream_ttfb_ms_count_check CHECK ((upstream_ttfb_ms_count >= 0)),
    CONSTRAINT usage_rollups_v2_upstream_ttfb_ms_sum_check CHECK ((upstream_ttfb_ms_sum >= 0)),
    CONSTRAINT usage_rollups_v2_virtual_cost_micros_check CHECK ((virtual_cost_micros >= 0))
);


--
-- Name: warmup_attempts_v1; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.warmup_attempts_v1 (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    upstream_id uuid NOT NULL,
    attempted_at_unix_secs bigint NOT NULL,
    completed_at_unix_secs bigint,
    scheduled_for_unix_secs bigint NOT NULL,
    trigger text NOT NULL,
    outcome text NOT NULL,
    reason text NOT NULL,
    http_status integer,
    cycle_key bigint,
    expected_cycle_key bigint,
    idle_secs_since_prev_window bigint,
    replica_id uuid,
    lease_holder text,
    upstream_spec_revision bigint NOT NULL,
    dialect_plugin_snapshot jsonb,
    error_detail text,
    dispatch_kind text,
    CONSTRAINT warmup_attempts_v1_check CHECK (((completed_at_unix_secs IS NULL) OR (completed_at_unix_secs >= attempted_at_unix_secs))),
    CONSTRAINT warmup_attempts_v1_dispatch_kind_check CHECK (((dispatch_kind IS NULL) OR (dispatch_kind = ANY (ARRAY['not_dispatched'::text, 'http'::text, 'dialect_plugin'::text])))),
    CONSTRAINT warmup_attempts_v1_http_status_check CHECK (((http_status IS NULL) OR ((http_status >= 100) AND (http_status <= 599)))),
    CONSTRAINT warmup_attempts_v1_idle_secs_since_prev_window_check CHECK (((idle_secs_since_prev_window IS NULL) OR (idle_secs_since_prev_window >= 0))),
    CONSTRAINT warmup_attempts_v1_outcome_check CHECK ((outcome = ANY (ARRAY['success'::text, 'skipped'::text, 'transient_failure'::text, 'permanent_failure'::text]))),
    CONSTRAINT warmup_attempts_v1_reason_check CHECK ((reason = ANY (ARRAY['cycle_advanced'::text, 'window_already_active'::text, 'seven_day_quota_exhausted'::text, 'upstream_disabled'::text, 'upstream_deleted'::text, 'rate_limited_cycle_key_missing'::text, 'upstream_5xx'::text, 'network_error'::text, 'request_timeout'::text, 'dialect_plugin_transient'::text, 'oauth_credentials_missing'::text, 'request_build_failed'::text, 'oauth_refresh_failed'::text, 'credential_decrypt_failed'::text, 'auth_failed'::text, 'forbidden'::text, 'bad_request'::text, 'not_found'::text, 'dialect_plugin_failed'::text]))),
    CONSTRAINT warmup_attempts_v1_trigger_check CHECK ((trigger = ANY (ARRAY['scheduled'::text, 'manual'::text])))
);


--
-- Name: wasm_blobs_v2; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.wasm_blobs_v2 (
    sha256 bytea NOT NULL,
    bytes bytea NOT NULL,
    size_bytes bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT wasm_blobs_v2_sha256_check CHECK ((length(sha256) = 32)),
    CONSTRAINT wasm_blobs_v2_size_bytes_check CHECK (((size_bytes >= 0) AND (size_bytes <= 33554432)))
);


--
-- Name: wasm_registry_v2; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.wasm_registry_v2 (
    id uuid NOT NULL,
    sha256 bytea NOT NULL,
    name text NOT NULL,
    original_filename text NOT NULL,
    label text,
    uploaded_at timestamp with time zone NOT NULL,
    uploaded_by_admin_id uuid NOT NULL,
    revision bigint DEFAULT 0 NOT NULL,
    supported_slots text[] DEFAULT ARRAY[]::text[] NOT NULL,
    description text DEFAULT ''::text NOT NULL,
    usage text DEFAULT ''::text NOT NULL,
    hook_metadata text DEFAULT '{}'::text NOT NULL,
    plugin_version text,
    CONSTRAINT wasm_registry_v2_revision_check CHECK ((revision >= 0)),
    CONSTRAINT wasm_registry_v2_sha256_check CHECK ((length(sha256) = 32))
);


--
-- Name: audit_log_v1 seq; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.audit_log_v1 ALTER COLUMN seq SET DEFAULT nextval('public.audit_log_v1_seq_seq'::regclass);


--
-- Name: price_catalog_snapshots_v1 id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.price_catalog_snapshots_v1 ALTER COLUMN id SET DEFAULT nextval('public.price_catalog_snapshots_v1_id_seq'::regclass);


--
-- Name: request_events_v1 seq; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.request_events_v1 ALTER COLUMN seq SET DEFAULT nextval('public.request_events_v1_seq_seq'::regclass);


--
-- Data for Name: jobs; Type: TABLE DATA; Schema: apalis; Owner: -
--

INSERT INTO apalis.jobs (job, id, job_type, status, attempts, max_attempts, run_at, last_result, lock_at, lock_by, done_at, priority, metadata, idempotency_key) VALUES ('\x7b2274797065223a2270726963655f636174616c6f675f72656672657368227d', 'fixture-job', 'adaptive', 'Pending', 0, 25, '2026-11-05 12:46:39.604613+00', NULL, NULL, NULL, NULL, 0, '{"traceparent": "00-fixture"}', 'adaptive:fixture:1');


--
-- Data for Name: workers; Type: TABLE DATA; Schema: apalis; Owner: -
--



--
-- Data for Name: _sqlx_migrations; Type: TABLE DATA; Schema: cc_lb_scheduler; Owner: -
--

INSERT INTO cc_lb_scheduler._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (20220530084123, 'jobs workers', '2026-11-05 12:35:46.135506+00', true, '\x0e2ae5fd117c32925d7ff4ba4f26368269fba7706bf1c3e9578a763a3e2acc5fd44c8edf2aa25e7c0ac1c6ebc8e95a9a', 222293226);
INSERT INTO cc_lb_scheduler._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (20220709210445, 'add job fn', '2026-11-05 12:35:46.373578+00', true, '\x14f906e53a627f1b27996e74e8e69dbc29ee70519e7af4f0032a27c75ed706a34af8bafecc8f8572855f314cbc5a7ba9', 9279430);
INSERT INTO cc_lb_scheduler._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (20230330210841, 'replace add job fn', '2026-11-05 12:35:46.403215+00', true, '\xd9b6e57e6de60db1647e2447cf36bf0eafbec266f1d482bd196affc97617efbc4c07bad826706c7b3a0b3960cf9f727b', 19374485);
INSERT INTO cc_lb_scheduler._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (20230408110421, 'drop old push job', '2026-11-05 12:35:46.435216+00', true, '\x3656a91fe9c652ca23df775a601409a9252357f47088d6bcf2025fda2dd67e50c99ad94d1d2ca569c91c343189c58e33', 13570019);
INSERT INTO cc_lb_scheduler._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (20230408234928, 'add get jobs fn ', '2026-11-05 12:35:46.467381+00', true, '\x176c3acaf1097ff45da76d4c746af766640027b6721a056b3fc3fcc4ab8192344920b4415e8dae46f5136ee223ed5f6b', 19930819);
INSERT INTO cc_lb_scheduler._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (20240225141841, 'replace add job fn remove jid', '2026-11-05 12:35:46.504755+00', true, '\xc53560b7a312915d696ed6c7e18bda478d9bf1278c4d59e3c53b47c45d8a903f089c728a5ff2e131cd5b629cdfe7d4cb', 17093524);
INSERT INTO cc_lb_scheduler._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (20250210092135, 'include failed in get jobs', '2026-11-05 12:35:46.531909+00', true, '\x810786bd8f1007bf523bec8ed9de0a9ae789e634e5434a1a5cc9c1b766bd758b4d37912ef390dcf95e8de23910eeef55', 8207845);
INSERT INTO cc_lb_scheduler._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (20250223193249, 'fix get jobs conditional', '2026-11-05 12:35:46.553883+00', true, '\xcfda3f3d62b4122b6321af0e45ba162fd3ad5d07f95e4eefe858600551167e31454ebd57d45b62e880336fcfef3ef2a7', 12044350);
INSERT INTO cc_lb_scheduler._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (20250307001101, 'add job priority', '2026-11-05 12:35:46.580628+00', true, '\xb5d318f2da5e909d66db190f58b76ae52f6808733d2a88d180555430d28518fee783fb0ff1e1c37ed1dc79627abd9694', 15365855);
INSERT INTO cc_lb_scheduler._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (20250404160441, 'cleanup old push jobs fn', '2026-11-05 12:35:46.609023+00', true, '\x962242ef0971de54905cafbdefe26a3c1f32c1c52ef13b6937c2920445b1ce97cd2dcf7db9c293becc95dc55412e7a15', 12166975);
INSERT INTO cc_lb_scheduler._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (20250722071207, 'improve notify', '2026-11-05 12:35:46.632588+00', true, '\xb3a1f712f8fbdfe38004134d8f55e49857b764f3277f38dd9d13299d5e0afbc895005b62fa0355b8a0d8595a38be6db7', 15090146);
INSERT INTO cc_lb_scheduler._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (20251018164839, 'update result', '2026-11-05 12:35:46.665534+00', true, '\xe4e3d1efc77746e4db164b49017c90785613b35520da84f9a5a3f8981d60420d9dec3c94af9e8632ab711ffac7c1e199', 87681831);
INSERT INTO cc_lb_scheduler._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (20251018164912, 'add metadata', '2026-11-05 12:35:46.761216+00', true, '\x803a4510201392a36e20ef81c9b05230ffa734a292c875f87ea65bc73944642b1d260a38741c4bdb88c0096338c152c0', 8592554);
INSERT INTO cc_lb_scheduler._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (20251018165007, 'move to bytes', '2026-11-05 12:35:46.780911+00', true, '\xdcbb8181c56cc4b9511ddc940f81f5bb9420436d0f4edf1bc9978011324e6b8d5437c50830b8651e2168608d04977272', 107851692);
INSERT INTO cc_lb_scheduler._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (20251018165033, 'add started at', '2026-11-05 12:35:46.897682+00', true, '\x21b1264f3881a4cf4fb209ff2cc75c21946441691745d7cbc9ab1dd812a1e1c08648e47ea8b4de060784da581dab2246', 14016728);
INSERT INTO cc_lb_scheduler._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (20251018165056, 'queue jobs', '2026-11-05 12:35:46.923153+00', true, '\xdd02b7e9c23a7da0f71bff73b469c51b7305d1830e7f4bd6de083c11315e8492801e242755d9981381760a5ace6d8fe7', 13661311);
INSERT INTO cc_lb_scheduler._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (20251018165121, 'notify run at', '2026-11-05 12:35:46.948113+00', true, '\xa99e6c60687ec15c1bf8132763db63a0d9d18e1ee814527254fc95ee733e65bdf2f0ae64ca86cae0d1f232654d189fda', 17964941);
INSERT INTO cc_lb_scheduler._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (20251225090252, 'include primary keys', '2026-11-05 12:35:46.975324+00', true, '\xfd9f16bd772c5682ecb3f6b0a21efed5581a85316770ab8d620b38346e9319ae239ef784e8eb46735d6be735e7b3b5be', 33557005);
INSERT INTO cc_lb_scheduler._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (20260508093314, 'idempotency key', '2026-11-05 12:35:47.021821+00', true, '\xf2686534e1abbba2fcde715c1ca724e7937a4e1f49d6a9a462ed4f1c2e8b83518f4f78868a111d6451afba770207c2d0', 62712087);


--
-- Data for Name: anthropic_compat_etags; Type: TABLE DATA; Schema: cc_lb_scheduler; Owner: -
--



--
-- Data for Name: oauth_usage_poll_cursors; Type: TABLE DATA; Schema: cc_lb_scheduler; Owner: -
--



--
-- Data for Name: price_catalog_versions; Type: TABLE DATA; Schema: cc_lb_scheduler; Owner: -
--



--
-- Data for Name: _sqlx_migrations; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (1, 'meta', '2026-11-05 12:35:36.551376+00', true, '\x3d2d8b01333a96dd9a2e06482dfdc00663abf06704dfec70f4874ef71ce6bab07d11ce0e24e2c942837f0bfd31229830', 152285837);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (2, 'killswitch', '2026-11-05 12:35:36.722+00', true, '\x91e5c2c756ce9d5fceb8a5255af2e751e8fe7931753156b96ea716cdc4c6e37531fbd572ca92027df36d99d64a95b168', 80269611);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (3, 'audit log', '2026-11-05 12:35:36.823934+00', true, '\xa2e2569943ae8e0584d5af4a4826a970f560a9d7ff2c3e11bbceb88bb847a428a4931b0146477a3c4c3b06e77c4cb7be', 87164496);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (4, 'request events', '2026-11-05 12:35:36.919306+00', true, '\xa2ebe2eacb709be8d734cb96ca93b38a73487f6456a916b21df830de93fe6b25e8be8a8d352a8a0f281b9ec3eba94707', 59105082);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (5, 'quotas', '2026-11-05 12:35:36.99093+00', true, '\x8bf11adfaca2686da73dedfa59c038bef4dc5e7751172edb0dafb221eff013462b7e6313cba8478548135ab5bfdc38f2', 66236050);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (6, 'principal limit states', '2026-11-05 12:35:37.072713+00', true, '\x86a8170d48d2c4b9f030ce5e3b82a2107135a662b8236ae1e712ea655e33272bbad039f83d93282aaca4e08433c641b3', 55153702);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (7, 'oauth credentials', '2026-11-05 12:35:37.141378+00', true, '\x5f30c7c48016a74a58f96a7f8a5060108a76f4a42b452d5e95e014237b17da4fb315dc4d0d72a05d2fffe4aab07b2c33', 76483939);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (8, 'api keys', '2026-11-05 12:35:37.235517+00', true, '\xc5faebfb80c90bec45cd46c0e247963a03c6e3a8e39b9155b548f448f803b07a24cc521c47adb9252c72da7c4033784d', 39256055);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (9, 'anthropic api keys', '2026-11-05 12:35:37.281461+00', true, '\x4aef7a1d861dd78a1302dbabe11c796da4d1d9b71d03e943486ba9589fabdd6f1dac81b0cca8bcae1717ff419e7c971b', 22272115);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (10, 'usage rollups', '2026-11-05 12:35:37.310468+00', true, '\x4557d8cef64403ecb4cf601c61bbc07adee48bf3ee4283d6bb378196de34819b301502de144bda4f8cbff60e4896540a', 68843512);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (11, 'config draft', '2026-11-05 12:35:37.390089+00', true, '\x3c412d0111069ae9fa4a252ae8f49eee3b575084dd49f9a3979c68a7dbb84c6478ebc35518113e774ff2962a4f1053b3', 45893814);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (12, 'config history', '2026-11-05 12:35:37.444756+00', true, '\xce7d63379b2583ce7f7a09ecc7715274d9a2e5fb618f278c21e4a95cc6aef8fafba6a5e95f06cc058f50b0af17af5ef7', 73841853);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (13, 'managed api keys', '2026-11-05 12:35:37.530877+00', true, '\x6529a166cd80edd4a6e52dd8c55c7c2f60e2749ed1a6cf0549457c131b698ed38d43f00c9f380508f07cad7cbf3bfc3e', 133940353);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (14, 'managed api key index', '2026-11-05 12:35:37.702426+00', true, '\x3b3b3a030bc8ce695d9e113c112f9b65a2f8d491e956f5c252717bc387a04242d132d61fb1a2b673eba7d59025c18334', 90417625);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (15, 'upstreams', '2026-11-05 12:35:37.808386+00', true, '\xc59228823ea86fdc4713c82f9c8f4bb6fc7ca2473c2eec46dc93f9381dbc479c6c32f1e3821564db14d49e0c48e544fa', 155555508);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (16, 'principals', '2026-11-05 12:35:37.981506+00', true, '\x1b10f8ef7d40c609c3aa933837a26fdc6b3164aa5a7c6276c4dbfe30a60fe19c68e8cd0c062ec9744bcd656f114cecd4', 114534242);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (17, 'plugin registry', '2026-11-05 12:35:38.107164+00', true, '\xe2bc2ecc3215b6af31d1ac917e360d30e5f14f205b033859b224c802a53dfaa3d2d88b049fd9196d679a178b477aded5', 224821563);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (18, 'upstream rate limit state', '2026-11-05 12:35:38.354445+00', true, '\x2ff206bc2af17fdb2df86d49660790f3a9b551ae3109fdcc9a7a077ee2407691707d72d95438af1033e9d8ab3f769ad8', 76955981);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (19, 'principal allowed upstreams', '2026-11-05 12:35:38.440534+00', true, '\x9d9c9234857c4bf41daaf692868851ae5d332208e957f14f1309de315680e052d4aab5f665828bce630ad3e32d74fe59', 7286594);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (20, 'drop per key pin', '2026-11-05 12:35:38.456765+00', true, '\xaaaef65c6b9dab2f9adf451473c94a05be71e37fbc51a1464041801fff6d5c4c3147ab5b7f9c7161e3c66c5d8aa25bbc', 9635097);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (21, 'plugin slot shape', '2026-11-05 12:35:38.473873+00', true, '\xc73a1d7c0c04043b1652f78e0fa62978248daa33912156b30ce9acf272ae71d3eae82ea1ee9815099c663b8b1f4799d1', 9249513);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (22, 'plugin registry', '2026-11-05 12:35:38.492888+00', true, '\xfa7ed09b9d5e9b97196db2d9b29dcb6a323993b8dde43e03b305c0a4ef34f5e23ee556a18d3ef02516e3dd0f0d84959e', 167633150);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (23, 'anthropic compatibility kv', '2026-11-05 12:35:38.675108+00', true, '\x3212d0e773cc9318227db33bd4d583d73be915da7af9119628f338525d20fe93fb16f5ed03e98596688ffd10d367003f', 51863155);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (24, 'upstream subscription quota', '2026-11-05 12:35:38.737519+00', true, '\xf5ecbf84bbec6f90ed5ba84041f606fe6de987ce62a932385f6d0c84a5eb105266b81809e1c6fb523a606d8e689f9f9a', 175226744);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (25, 'usage rollups v2', '2026-11-05 12:35:38.925994+00', true, '\xab2614b6a898d7e297741f8e6bb010d575b82e0b901a51839d0df7115ba80f83faa652fce9482387ea0a95a56b3b01ca', 138072817);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (26, 'usage rollup checkpoint reset', '2026-11-05 12:35:39.080264+00', true, '\xad94290a539329700a8bda1bdebae7a216af8d8004b492fa21e03b918b2dbae10be606d8d1cc6fc2a14a62b6614844d9', 16205856);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (27, 'cache analysis fields', '2026-11-05 12:35:39.118665+00', true, '\x7ec407664617fcaf89ac3c76b657f731b21714baef6395f7be931b3d7ab7b91428cc0ed3ac48c34f6d95002d0964b8c2', 83318075);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (28, 'cache breakpoints', '2026-11-05 12:35:39.239084+00', true, '\xad775e7176f4a509da6fc27a1c34d159721948a7fd380b1c6d6ada67fdd04f07860b8650f679d46c6cae922c248f17fb', 30454959);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (29, 'subscription and org metadata', '2026-11-05 12:35:39.305825+00', true, '\x801b6863964b50b81a7b1e8c9ecb74d912f3793ccb760c14330e9459f235a871a105c22eed0f4f9977d09e953f7d3f31', 197858900);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (30, 'prompt cache observation', '2026-11-05 12:35:39.525869+00', true, '\x11d7db3da3e6aca4c04ab21a2e979bfdd4503c4eab096e763cd4c373d6b10cfa3f20b5e8fab6daa95ac3d08ae7df669d', 108587151);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (31, 'plugin chain wire version', '2026-11-05 12:35:39.645676+00', true, '\x3264dfbbd883aa04b4d3135182b26761c6ca5b19223b8c4741e9640d77bd4106a1f464b4b63b2bfe3d6cb555e626d6d4', 39271430);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (32, 'drop custom upstream kind', '2026-11-05 12:35:39.698325+00', true, '\xd1d1f4e2de1d070e46829caca8b0c6dc52c01f6cf4ecbd1f57be0e3e391f138616ce2552f3b5bc79d0ee5cc5fd64eb80', 21063737);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (33, 'router pipeline', '2026-11-05 12:35:39.739716+00', true, '\xbc112a5eaaf7ab68a4a5cba9d71fe6a634a87a66c722beb8c98861bd65ed9b83ba5279b1d65d6ad5913382913f31cf02', 20339570);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (34, 'drop incompatible router chain', '2026-11-05 12:35:39.77691+00', true, '\x0c909282f772e3234fa713211127599354de9bf5ed90962ba8e7e0889db34e6ae5de74eec05a5b7761ccbe79471e1886', 19949570);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (35, 'drop incompatible router chain', '2026-11-05 12:35:39.813655+00', true, '\x34b83813aa9f649aa56be587ebc09d0d30ed55581d6578f1711f1ae21dc6b639127de31ea6fef38a1d07031874d152e2', 35519383);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (36, 'seed builtin cache affinity', '2026-11-05 12:35:39.865661+00', true, '\x7811a7ceabe1708f2536711c8e73397ccf5e1e30aaa2c354ba4cf09685e9d65737b12470dc8060f27e88c7a28d9b0973', 23623324);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (37, 'upstream warmup', '2026-11-05 12:35:39.909677+00', true, '\x010f298a1c96c9e1575fb08dfaef096df08fc986f785968e0bf7c73ccc3507297f0d3c26d66e391a1a65983c776f5fec', 51087196);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (38, 'warmup dialect plugin', '2026-11-05 12:35:39.976665+00', true, '\xa5df12a9c098c11c9a9a4411f354354fd8caea4af319613d21d7b6546d4fbf59a3838f3c5944dbab380936de67bfd2cb', 20419612);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (39, 'wasm registry supported slots', '2026-11-05 12:35:40.017487+00', true, '\xc425dc31a6e5724d390f61c4751018dbd5a369fdc76f756de30e2301ebe3d77761b7c7a5199835272bfb5697cc44cde3', 17554358);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (40, 'wasm registry wire version', '2026-11-05 12:35:40.058008+00', true, '\xb8879672da9fbcaab4dd7e06000cb3eb98c9355ac69144a35f4ef9ca8216f2b1fed7f811d37d8fe438ffba7ffc10c351', 17294316);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (41, 'upstream spec split', '2026-11-05 12:35:40.101826+00', true, '\x028e28c962c9847409af8e1259109ff47507795be7816bc703d8a9373c364fb09f62ff4ca3d456c5f469dfa544cdf176', 746665662);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (42, 'price catalog', '2026-11-05 12:35:40.883633+00', true, '\x66ccb17da4f6ce82f1a97e8a0ec907d5dc6e5417941ac44a42322135843c8716c7cfbefceb4bb796d25c8265d32071a9', 194194437);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (44, 'deprecate quota limit state tables', '2026-11-05 12:35:41.100805+00', true, '\x6a87f530313e21e626eece383e072b395e31d888335acdd00b1d28c02a98539370c44f74755a44416f711d1f74e83743', 14964312);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (45, 'audit log wider columns', '2026-11-05 12:35:41.128269+00', true, '\x2e3f8face6a7ba79b4584ece2ae81d7c42ae706d6a3c942d7330f5203a95aaa1d547054c091709111f3d26c8c27a098e', 18366401);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (46, 'subscription quota fallback percentage', '2026-11-05 12:35:41.157943+00', true, '\xeaca7819babf642c15e1414a2c619d09325e294813ca68e03476ae1a6cf30ab92269acec2a801ecf0d590906a642cac0', 12735601);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (47, 'subscription quota unified signals', '2026-11-05 12:35:41.188142+00', true, '\x9c9a0bc92b0220631798c507da50767d1f36b6fe7c3ff01abae8caf84d5f6660c625e62d5c762196a73276a0d53ddb60', 165494521);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (48, 'add upstream oauth token generation', '2026-11-05 12:35:41.36484+00', true, '\xd7d4fdaeff8f0b1bba3b13062e498e655decd6f703450e7caec0a75db5e9f9082a107febe73a95c09687aec8cd2b3781', 13324686);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (49, 'upstream status last warmup at', '2026-11-05 12:35:41.387168+00', true, '\x0d548c02577bbf80fbfd893df6f176d9335457c9823cad30bb21a9510d75414248063c6610226a92944eb51d3a603930', 11513058);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (50, 'drop upstream lease v1', '2026-11-05 12:35:41.411549+00', true, '\xaad9b691da58ded63fd23f37bf6417ccdc6fb22e507704b8271fc36f5b7dbeec0ce016f361b4ae713cf345d54b76c056', 19729736);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (51, 'drop upstream status warmup columns', '2026-11-05 12:35:41.446025+00', true, '\x3e7b18d0356bb8ccc9b04d4855fad2ca6465c28cf979c6934c92e338033225688f722369ff0cc63b705ad7b94071938e', 11150474);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (52, 'organization metadata seat tier', '2026-11-05 12:35:41.467295+00', true, '\xfded299f5560f4062a705e58c1936c4d0eb1c7bee38a74b1410a7b9ffbd8bc8b1b06efe49df9798b8b3b8c90bc4c5963', 12059934);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (53, 'price catalog payload hash', '2026-11-05 12:35:41.492014+00', true, '\x8ddca2d473b44a5751f3bf7d5fb084eee7d013bdc6f6bc6432a9058d02c77c9c34e15c894377f3e54024c6094e8cfdb0', 111531655);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (54, 'pool quota history', '2026-11-05 12:35:41.615471+00', true, '\xae4de83a7d6dac277b208f02a4a06b292476c40a1c90a56f1c4b8ab14097546a66654eb4dfd70884317de824f2537e22', 60092375);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (55, 'add warmup attempts v1', '2026-11-05 12:35:41.68762+00', true, '\x2ecf35e7314e68d363c37047a8f7244f92d424f373b932e55cfcc4730f7626b571749a5e7253f414a89bbf63f4b92fa6', 121288211);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (56, 'warmup attempts seven day quota reason', '2026-11-05 12:35:41.828904+00', true, '\xf094557a44e36efc6d614a346d48e1b790128db719efef2f1707137aa31509548574f848673abe75f8af753a75d5b6d4', 11108182);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (57, 'seed builtin subscription preference', '2026-11-05 12:35:41.849344+00', true, '\x640fc0a74a882e9e28f82963897d53109026af978bc6d5885bb64e4901d1ad5cc7c05f142cba900a9995b4ce1a0865c6', 12842768);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (58, 'warmup attempts nested outcomes', '2026-11-05 12:35:41.868779+00', true, '\x1429b815b76cb5c5c77649b5fb4094cbe963696d128f46324cb4fb6f6476f3adf4ae7b813ea10907cf723ae3bfb4aa03', 10070764);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (59, 'request events terminal observation', '2026-11-05 12:35:41.891311+00', true, '\x31f31be927465b8b0c054e341003d1d5564bea2cd16a6715a6dc3785025d9071cbd8a57ebf28a7f02ba08be2423ba511', 30006958);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (60, 'wasm registry schema hash', '2026-11-05 12:35:41.930911+00', true, '\xa76733607a2b59fca2434b2b9227d7e0874db5d0c3f7a56857011f2f96534201c5203393ab4e07560fa25f97dbd6462c', 7219802);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (61, 'request events shadow event id', '2026-11-05 12:35:41.946256+00', true, '\xd8611d44e73a486177bb89fcbdf91dd6dc0b58b55c76ddd4867b18af3f26f3dc0e2d3a4c86598b9176d502a9bd6723a2', 20741237);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (62, 'normalize plugin v1 baseline', '2026-11-05 12:35:41.982259+00', true, '\xfc2ba728527c95e3984caa22f89b0aef8a0ab1840b0cd1a522ce393b9ad7b85e74e70006aee02eb7bede228a7cea2405', 10094723);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (63, 'drop request events shadow event id', '2026-11-05 12:35:42.004782+00', true, '\xd5710be1b64889018a90870e69a313487a2d4177457f22a7748cc65aca12113d5cf5307e657489d6ec657e06970c7532', 20319862);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (64, 'wasm registry hook metadata', '2026-11-05 12:35:42.035282+00', true, '\x827c89073f3d8001b9c358e7d56ecb46554a54e6e40df70efce456998d316bca95b12db856a5a05872b1e8d87e590d1f', 11775558);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (65, 'request events add tx id nullable', '2026-11-05 12:35:42.058458+00', true, '\xc4e9f2fe5ec61324af295c41c9105c70d9022133f9ed44fc0a7d9642670059527e9bc236067176f8a1f1fa221d7b3ff1', 16038898);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (66, 'request events backfill tx id', '2026-11-05 12:35:42.084985+00', true, '\xf5a8209e5c9b0c241ec71cd08f4329ce4cbf63133a93fad421e9c513694fba1fb6975e34398f180a248d7ce9875b9fce', 13514893);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (67, 'request events tx id not null', '2026-11-05 12:35:42.114651+00', true, '\xd93d796d732141c9eebeabe3976041d46cbe864f8218aab5e292fc60e741ea1d6d3d8519b8954c3aada88c6706791462', 16535773);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (68, 'request events event id not null', '2026-11-05 12:35:42.148241+00', true, '\x4c2fb9036f68119d9aee33abadfff824aa04cbb73fc34e4ecbf13bd00b20a28be9db639919ebeaa0e8ca782e6b5232c3', 45574021);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (69, 'plan tier history', '2026-11-05 12:35:42.209312+00', true, '\xe49a2b393957d2bfe57d2bde02a613f8f869a98831e535ca1d2183d96601729264e938a93775a986b5146ca9c87a82a5', 256230856);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (70, 'plan tier backfill source', '2026-11-05 12:35:42.475256+00', true, '\x5a9509dcb115ab1f4061aef142d0ade73bed935b0c834189344ef345fa7796042051fb7b934d8351bd1d89df368e16dd', 10546723);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (71, 'drop pool quota contributors json', '2026-11-05 12:35:42.500752+00', true, '\x5db8cd8075fffde71a7fc0689370e853d64727e775c41d417d074cb4953144b258efaf5272e3b9f842a48588c3d25ab3', 10538848);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (72, 'v3 prompt cache observations', '2026-11-05 12:35:42.526678+00', true, '\xf423ae488b269ec241b1e68961e3d50c15c4e7a6d2e6d85bb836bd9f1c400106488aab220ec73a58fa5f36f3688235a3', 71196766);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (73, 'request events v3 cache fields', '2026-11-05 12:35:42.613988+00', true, '\xe9e6d93d43190f86f79ba4f90018e4ce0ef33d872538714cb45887e3df838921a008b8bfcb7e10d3e38ac483ba517e32', 30133500);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (74, 'subscription quota checkpoints', '2026-11-05 12:35:42.654185+00', true, '\x0d3d9521bbdf214f3995fd8db3d13197af4b451dc975d662ea37c1e1a7ff2408dc987bd72c052582ae7d67cd8843645d', 79044235);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (75, 'plugin slot response transforms', '2026-11-05 12:35:42.76288+00', true, '\xdf58f850290aa247f31eac7acbd92b793a1815af0545c2d0cc4905f07928428920327128c998df82f0c0b65f375f9e22', 16777356);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (76, 'remove response transform plugin slots', '2026-11-05 12:35:42.795669+00', true, '\x92a80f193f1fa121b1bf27cc8fc2dfd3a99c15822a619a4980252669ff6d7579d8daa66dc76a2638e0ae5104066592b3', 18838776);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (77, 'drop subscription quota observations', '2026-11-05 12:35:42.827716+00', true, '\x143d90698f2a7e31828248d3d79e570f6e08869eac2c2e72c156a30b811cbca6137c99f376e5c14ca2c2e5e562dd84c3', 20082445);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (78, 'request events quota urgency', '2026-11-05 12:35:42.858797+00', true, '\x1aa762c3e6b3117c5cf74619343da5a1b2528c22d7ac8dbf8960f7a47b374fcbd7b2a15a184a23f3a6cf078f88e83441', 15384563);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (79, 'principals cache keepalive', '2026-11-05 12:35:42.883379+00', true, '\x3e1140d44570ffe43ec6e733304d8fff0de2dca3b8e5600616618223a46b13eec4b6e8bae06d9cf9ed1b7ba114ac162b', 16495440);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (80, 'cache keepalive sessions', '2026-11-05 12:35:42.917794+00', true, '\x5cc1deba4ea45e3b849bdbae2277844422bc24940b60d4fc2dde6d2fe4a1ef14bd94a86838fbeded7d3efe19de2fae25', 82148530);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (81, 'subscription quota checkpoints fable', '2026-11-05 12:35:43.008734+00', true, '\x079660a545d57415597350253f3dcdb6ae13320a69afb8fe922956908de0370b837c3e240587d91914c4c7ed6a453961', 20084236);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (82, 'usage rollup checkpoint xid', '2026-11-05 12:35:43.040238+00', true, '\x80f6746ec7e4d3d37806cbc6fdf126560adb6c0d6a52d720859508fc2cddf5ee1f61327bd1786aa5c948b9787d2fe78a', 36213758);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (83, 'request events thinking budget tokens', '2026-11-05 12:35:43.091936+00', true, '\x492f18ef702c80680cbc4b892c0bb25f6e6e5b7f7c06c702d88a9389f28341623c62c05917d7dd9334ac6399c843d840', 8832304);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (84, 'price catalog payload text', '2026-11-05 12:35:43.113494+00', true, '\xacc71be2afc326d0c2fd29069a7e35ec92a71224e4f68e15d40d817373efe59c8c34aa7b13194392424a392811b62a64', 79546277);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (85, 'quota aggregate covering indexes', '2026-11-05 12:35:43.209618+00', true, '\xea8d82b04fcaa3f42846c77675a738f25898d627622f32d297e0faa500330f432c561e6f45703b0a785e3d2e042b4742', 48886943);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (86, 'usage interval token covering index', '2026-11-05 12:35:43.274362+00', true, '\x7fad9055a5d09f456124ac57e90b7d5fdb0be0c25312db48697cf5ba4bd59a52717b6100abce0aeb710935803ebe6ee9', 38469762);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (87, 'request events reasoning effort', '2026-11-05 12:35:43.329652+00', true, '\xb7c394a3fafb899f5ed517fc0f919a41458798608367e8d15b932bab13847e9ebe4e680764c717522c5d8a74c800d356', 12564309);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (88, 'plugin registry references', '2026-11-05 12:35:43.355676+00', true, '\x97d7f5927ea7ddf168a801791de4515428bd4d14865c00c1a654f8a40ea43f6d60d6c1efb379c1de15e8567bd43881b4', 30364417);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (89, 'request events principal key usage', '2026-11-05 12:35:43.398142+00', true, '\x72ae3adc4c60c62f1d4027cc9a52f33088bc4698f9c4a0dec22e4dd8e8b984f8c14f343852e6216df4fabe23dacd0506', 30108625);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (90, 'drop request events wrh weight columns', '2026-11-05 12:35:43.442256+00', true, '\x78ca49bae6460c9cfc5788e3d4847b7f297449b41f4414686cd309d483895aed764794f8442e7afae6196b01795125ed', 19474027);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (91, 'remove builtin router filter', '2026-11-05 12:35:43.473041+00', true, '\x3753d6e1514bb8202cf020396ff7948d9552e017d665d7996d6f65faa0fbb2bb36d4d3260da741220dbdf3d4825ed409', 14093520);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (92, 'request events source kind', '2026-11-05 12:35:43.495778+00', true, '\xb2399d6e82fe96855b7500c352547976befe07436fd9c4529d6cae19ce5a51be2ee9581ef72b1cd3c919494084e52917', 14803646);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (93, 'cache keepalive running lease', '2026-11-05 12:35:43.52361+00', true, '\x844f9feaa6f2dbfc9562f2b8aae52c577d04c81d72d37573662f7b21e9567268668e7f0ec09a64dd74c677bf5a10ca5a', 15838938);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (94, 'cache keepalive projections', '2026-11-05 12:35:43.556279+00', true, '\x5c7b7094045073e82746de7a2b8cd183bd185149b6e1d5a9dfc714d9024be3b1fc5f5e84de3b7b9d4b20f575e40588e0', 60050376);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (95, 'cache keepalive principal read model', '2026-11-05 12:35:43.625937+00', true, '\x186c96a429752bb0379f9e3d362448fc12480935a0a9af9bc55fe1efce6777f98946de8b4f6f309ba8c962d3463f81ea', 75458063);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (96, 'cache keepalive frozen message timestamp', '2026-11-05 12:35:43.722266+00', true, '\xc555437f4306f199e072e4e0f1722c8d30164137fa1a39939c86a1399daf7413e2b0687f1a885e92be50a1beb8fdb956', 39044637);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (97, 'pool quota history fable', '2026-11-05 12:35:43.787873+00', true, '\xfc5cec36bbcfd5e4365098ee97f2b7247c4a84ee727eaeb152b7b52d4fd64695f9e8d21b8bd2908a83dd90b0e5ca7562', 8522762);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (98, 'validate pool quota history fable', '2026-11-05 12:35:43.810696+00', true, '\x01ae7721a2f1d5c89c209cc832d35633038ef793721afb1b2094d83675b8a02796dadfc9b854b86629e5f163e6a4395f', 10150223);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (99, 'request event list materialized fields', '2026-11-05 12:35:43.829865+00', true, '\xd276b1b0ec6c63913fc80acf44163255b4746b513a94cdbd5bf3ac3fb5f4c00bc57cc1e46f1d5bd25d669bf6afb4aafd', 86204828);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (100, 'live log pagination and overview errors', '2026-11-05 12:35:43.929757+00', true, '\x7d6418c89cb2653f56e17ceef0720535d259a9fc2cd37a2c97f58c19bc750c4ce84a5e21d0f69177d5158ce7a33e50b2', 89588541);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (101, 'request events claude classification', '2026-11-05 12:35:44.035103+00', true, '\x2155c8f1ee336e4e7aab648aa8698fc0709851dce1dca6ea8510d33fb2cafd04b51b8e319e817824ac9be90dd2afef70', 13353436);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (102, 'request events claude session identity', '2026-11-05 12:35:44.063947+00', true, '\x9250657cd22b2f1e6aa6bf1062394e1031968fd6248371080c8b82c469e4a4ca6b353c2f93cd768a88d409c501baadee', 9871389);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (103, 'subscription quota absent samples', '2026-11-05 12:35:44.085689+00', true, '\x1c5262cf8b9ea15cd633789e116fb7aba68ee5173b46b76757c1a50b2ae14e092ebcf2b90505d0a69cc6ab54baf3b71b', 11485182);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (104, 'api key usage buckets', '2026-11-05 12:35:44.111656+00', true, '\xd632eb89627d561e1937b269d760042e54d4938193931eacc3c790b70f1a3304df0a0824e00d2d9680b50dd02411367e', 82302864);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (105, 'request events model prefix index', '2026-11-05 12:35:44.202214+00', true, '\xc766ab1c189086f30ab89f91687b47a40af6e3444a1fef87f6c394aa2115d2087d435c92ef07b628020f9e654a40978e', 26826079);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (106, 'request event principal cost indexes', '2026-11-05 12:35:44.247613+00', true, '\xa50e191e280646d2845dd0bbc515c0939acdfffd0fefdded851f9f7d0903fa78c36f68e3d4040a6c845770d63aedc6f5', 31776253);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (107, 'upstream name active unique', '2026-11-05 12:35:44.292782+00', true, '\x7f6e36c446a7a88900422adda72055faf248b351da09557322df3d8fd4e7d48431af67a10a3231d84854087419a81d96', 21221155);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (108, 'request event cost component columns', '2026-11-05 12:35:44.324057+00', true, '\xc21822cab91766a37f092f7dfdfc1736efe4d093fffcf4b23820fa62e70b8935b4ae489ea4d58b8876d0b03c782cb6d0', 17886774);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (109, 'request event cost component backfill', '2026-11-05 12:35:44.348715+00', true, '\x21214f08d571c8da35d1f833ba1633453cd149a4a5d343f9eed8c9017024664e153792750c8c45e916d22afa9466d1ee', 27044537);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (110, 'request event principal cost covering index', '2026-11-05 12:35:44.387059+00', true, '\x418c1679cce4bad99003b638938169cc51be0cd42d3b4f22b71d194983be5d565f7997e7d2a38d981cb199f1aee63fe3', 52300947);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (111, 'pool quota history chart covering index', '2026-11-05 12:35:44.453812+00', true, '\x5eabd76325417b4d644424c317a8cdbe70f7db7c6ad1d6e59f0da6b4f3be5c14f46e276acd7faf09bdba9a7a790d3eb0', 32586129);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (112, 'upstream affinity', '2026-11-05 12:35:44.506221+00', true, '\x3179f688d4b81f0f414aa4aff2973c9bec626e3dc657246fc25687f2055ae8bad6c68a798e80eb42cb0b140ff6d68192', 76524523);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (113, 'upstream affinity observed at index', '2026-11-05 12:35:44.596245+00', true, '\xbfded52aab5aba089733b9d7323f8b04ae0eea4a4096468ef7b764ac02317c3b6495495b70b1663694b470f9b60df722', 24365659);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (114, 'drop prompt cache last provider columns', '2026-11-05 12:35:44.627155+00', true, '\xce57857568a47c38f1362b192a2bcb8cd1141e1dba045d8ea08af797e9ee3e42a466947568b37e5f0966b022038fde1a', 9420930);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (115, 'drop legacy credential tables', '2026-11-05 12:35:44.644198+00', true, '\x4acf52ea15c8b625633e4c87d048ea4f42153905e797402d42bad75c33ea3db52f1f02902e6a548ac3df52760e39cf46', 11182266);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (116, 'audit log actor identity', '2026-11-05 12:35:44.663621+00', true, '\xfbe9cbd24f8e58154487c9f27a48a861b3e1a7fb3361759794652dd024b7627552cadfe63f017768eaf50052e0a89320', 23224658);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (117, 'drop managed key kind columns', '2026-11-05 12:35:44.698206+00', true, '\x6a221d17e1eb16ba35ea69373a223497e70abd04418db7c7d82146e1c6c33fc8eeaa51f1172f6fe28af335b548724777', 19577902);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (118, 'cache keepalive read indexes', '2026-11-05 12:35:44.73177+00', true, '\x4dd84eb0a6d67f37f58cf553e8e51f1c3f0a5e3c6e36039c8146712bc27f0ee7dcf1ec31d04d3ca1bedfe096d5a5aeee', 37939844);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (119, 'purge config history payload', '2026-11-05 12:35:44.779301+00', true, '\x34a785127f5cbcba8bf39927fc5f6e7a9927c14db8b2dd704405ca12f8b7ec097e2fb0a6ee74c0554caef832cad5034c', 16085939);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (120, 'oauth pkce flows', '2026-11-05 12:35:44.809047+00', true, '\x93ff015e7d8dc7f0b744c0bace517f9af506e39b9e163c83b837093421925a812d484863bea3f0922380261bf5bd5906', 71210891);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (121, 'api key concurrency holds', '2026-11-05 12:35:44.892719+00', true, '\xdbf1e1c50476cf4326ec5b44da1745413558ed9d78766badc09b2524b165e1302dbb5e67dc90707a6c5435e4456f371d', 73196102);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (122, 'audit log actor ts seq index', '2026-11-05 12:35:44.975919+00', true, '\x8e3e87018feaa0e2c302f79cfc643247b0727c2f111fd6b113060a806d9cfb3008bdcf1f9357a1b8498115f732582ecb', 24166200);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (123, 'audit log admin ts seq index', '2026-11-05 12:35:45.016886+00', true, '\x057fbf6693657edf6b3cfb9b08be6a82afaf58457866d8dd2c84b04a405604101f5e0ccc1a32283e4ba74872be00e949', 33693713);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (124, 'audit log admin principal ts seq index', '2026-11-05 12:35:45.061394+00', true, '\xb538150551d41b02cedf866c0c8df7062bdb34675dc20954efbf78300c8d0ecf6b644e89756cee38828c5a5acf3bf1e8', 35521633);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (125, 'audit log admin actor ts seq index', '2026-11-05 12:35:45.111276+00', true, '\x85f6cbbfd4b9b0efa3283707fcbc9589c7db8f265ffab17437656ec2d16672a4b7ded68f8b7026fb91f3400f5c000f90', 48825110);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (126, 'audit log principal ts seq index', '2026-11-05 12:35:45.17028+00', true, '\x81ba3574a67846b326121c03cf685cf211de90f04b01570dc43bb1da8a68ae08aaaaff77be5b50cc15746d606c18bfb9', 24405868);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (127, 'request events event kind', '2026-11-05 12:35:45.212033+00', true, '\xfbd2e3c8984b415dcd95d7b2983df157a8ea2da1bd6c36d33fa447cb14d761415a1bc5fac69fd8f6b29feed8afa9a311', 30394959);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (128, 'upstream oauth never refresh', '2026-11-05 12:35:45.259413+00', true, '\xdf205a2aa3749ee63dac89b5a1f596aaf2c723d7feb45dcbf84a2396233444f5a59bca4e030e896ca422f23ce00dead5', 11648974);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (129, 'remove observability hook plugin slot', '2026-11-05 12:35:45.283017+00', true, '\x085b9d9d250383c351c4eef7a29d74111911c6c44da6919aa2b693c883770ffcc4713faa096b657b5edad2aac7088f2f', 25091910);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (130, 'drop legacy schema', '2026-11-05 12:35:45.31837+00', true, '\xbc72355c4893b9f4e66e0a334602d28a6329b427c92bb6c461d3b607ef90f3161390ae680460d146b5b2f3e63106aae8', 102617059);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (131, 'drop upstream spec name index', '2026-11-05 12:35:45.432806+00', true, '\xe7968db13d432e8a525f2af38fa6691abfe1fbfeb61b0930f73fcec1a3b7201a95e51513a465ec607df6c790cd030edb', 15741813);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (132, 'revoke disabled managed keys', '2026-11-05 12:35:45.4622+00', true, '\xb109fa139304f0f78879dd8b13cb1d2ad77f0f843cfcf2025d00852423527af09d14424f279eacb196b45c05d36ceea2', 8801345);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (133, 'upstream workspace and key details', '2026-11-05 12:35:45.484282+00', true, '\x80697f1a06ea96f1efe28d1c656e6b66ed9a0ae68db0556ff8cc261a65169747e7e8e6ad1e96e0461cacd3405624a199', 62548796);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (134, 'drop request events thread ts index', '2026-11-05 12:35:45.562441+00', true, '\x43e915206fe447c79591d4adb7fc0fd18d340090843aa14b61ae8192fc750eefb972c7a0e672f44b252e7ccccd318cdf', 8350095);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (135, 'drop quota checkpoint duplicate indexes', '2026-11-05 12:35:45.582045+00', true, '\x47a6d99d0d7a78601d6386499d44b1c9209380f1474161f9e4c8208184277e9a218d4deba63483028fcba62d8217fcde', 9411638);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (136, 'request events payload storage main', '2026-11-05 12:35:45.601294+00', true, '\x0b0ef83aa96fdb153fa5e33576f8052d8e8ac590807f120b12ffc92369610f8f8b5467c5f826f525bf68f9fedc11c2b1', 12740226);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (137, 'drop request events lower model index', '2026-11-05 12:35:45.626299+00', true, '\x9edb827d27225ae7fd0b3cf506d8cb67514f120379ae15546d2d94d469c375dbf63374249f721b527f9b99ed885fe183', 17126815);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (138, 'request events payload storage extended', '2026-11-05 12:35:45.656607+00', true, '\xcf3fd89390372b3253d0baf0930efa5cd41fa4a37633b046f473a62f3d755a4a5c055fac25bc3ca6036034ae290f6066', 11413933);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (139, 'request events relational columns', '2026-11-05 12:35:45.734615+00', true, '\x1106a93d43130de82420e878e755e94759884eb5cee03ecb4e91d2404d851686e48b79f97d491d80fbc8d2a8f942df95', 71110932);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (140, 'drop redundant auxiliary indexes', '2026-11-05 12:35:45.768979+00', true, '\x0169c01127c72d3dbb576fabb2c1c113beadc6611f23aaa1e597f7e9be78075a3da5766c0be07a058174623eb12ff43e', 12357850);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (141, 'cache keepalive decisions ts index', '2026-11-05 12:35:45.792189+00', true, '\xfed10ab87ff62ddeccc123c8bf505a88447dff25d3a6c71390deac6cc545d84b2bbff4855536280c4a16554e2f8311d6', 29658750);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (142, 'cache keepalive turns ts index', '2026-11-05 12:35:45.838078+00', true, '\xf217b1225f97c9b4f3b5ec3671d310fd45c1aa98faaabf914a2e7e7cf9c3e9d38c5e36dca40b4c7ad3984ee7cb8ba4b4', 29737583);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (143, 'provider upstream kinds', '2026-11-05 12:35:45.883342+00', true, '\x40452f83cbf5d2f58b6092bb6058f87f304076f5daf0ead548d2e7ce4851c74c62c248fce264adeb913d6f9c5e893f14', 13308769);
INSERT INTO public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (144, 'upstream affinity provider state kinds', '2026-11-05 12:35:45.909396+00', true, '\x2ca10e8a5df0249c2d1d073067e59ed3eef27adbb2a46f0822c9b5ee47d1728f043c800f93c5bbaae653bc95398807a9', 40067764);


--
-- Data for Name: anthropic_compatibility_kv_v1; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: api_key_concurrency_holds_v1; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: api_key_usage_buckets_v1; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: api_key_usage_writers_v1; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.api_key_usage_writers_v1 (writer_epoch, lease_until_unix_secs, last_flush_id) VALUES ('00000000-0000-0000-0000-000000000000', 9223372036854775807, NULL);
INSERT INTO public.api_key_usage_writers_v1 (writer_epoch, lease_until_unix_secs, last_flush_id) VALUES ('01a1abb5-4a05-75b7-9a37-0ae99862980b', 1793882672, '01a1abbc-d7af-7731-97d7-8cda7f450af7');


--
-- Data for Name: audit_log_v1; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: cache_keepalive_decisions; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: cache_keepalive_sessions; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: cache_keepalive_turns; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: config_draft_v1; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: config_history_v1; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: managed_api_key_index_v1; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: managed_api_keys_v1; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: meta; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.meta (key, value) VALUES ('backend_kind', 'postgres');


--
-- Data for Name: metadata_tier_mapping_override_v1; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: oauth_pkce_flows_v1; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: organization_metadata_v1; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: overview_excluded_error_rollups_v1; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: plan_tier_ratio_history_v1; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.plan_tier_ratio_history_v1 (tier_key, pro_relative_ratio, effective_from_unix_millis, effective_to_unix_millis, provenance, created_at_unix_millis) VALUES ('pro', 1, 0, NULL, 'migration:0069_plan_tier_history', 1793882142209);
INSERT INTO public.plan_tier_ratio_history_v1 (tier_key, pro_relative_ratio, effective_from_unix_millis, effective_to_unix_millis, provenance, created_at_unix_millis) VALUES ('team_standard', 1.25, 0, NULL, 'migration:0069_plan_tier_history', 1793882142209);
INSERT INTO public.plan_tier_ratio_history_v1 (tier_key, pro_relative_ratio, effective_from_unix_millis, effective_to_unix_millis, provenance, created_at_unix_millis) VALUES ('max_5x', 5, 0, NULL, 'migration:0069_plan_tier_history', 1793882142209);
INSERT INTO public.plan_tier_ratio_history_v1 (tier_key, pro_relative_ratio, effective_from_unix_millis, effective_to_unix_millis, provenance, created_at_unix_millis) VALUES ('team_premium', 6.25, 0, NULL, 'migration:0069_plan_tier_history', 1793882142209);
INSERT INTO public.plan_tier_ratio_history_v1 (tier_key, pro_relative_ratio, effective_from_unix_millis, effective_to_unix_millis, provenance, created_at_unix_millis) VALUES ('max_20x', 20, 0, NULL, 'migration:0069_plan_tier_history', 1793882142209);


--
-- Data for Name: plugin_chains_v2; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: pool_subscription_quota_history_v1; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.pool_subscription_quota_history_v1 (snapshot_at_unix_secs, quota_window, utilization, weighted_utilization_sum, capacity_ratio_sum, eligible_upstreams, contributing_upstreams, stale_upstreams, missing_observation_upstreams, missing_metadata_upstreams, header_contributing_upstreams, api_contributing_upstreams, max_observed_at_unix_millis, computed_at_unix_millis, policy_version) VALUES (1793882208, '5h', NULL, -0, -0, 0, 0, 0, 0, 0, 0, 0, NULL, 1793882208155, 1);
INSERT INTO public.pool_subscription_quota_history_v1 (snapshot_at_unix_secs, quota_window, utilization, weighted_utilization_sum, capacity_ratio_sum, eligible_upstreams, contributing_upstreams, stale_upstreams, missing_observation_upstreams, missing_metadata_upstreams, header_contributing_upstreams, api_contributing_upstreams, max_observed_at_unix_millis, computed_at_unix_millis, policy_version) VALUES (1793882208, '7d', NULL, -0, -0, 0, 0, 0, 0, 0, 0, 0, NULL, 1793882208155, 1);
INSERT INTO public.pool_subscription_quota_history_v1 (snapshot_at_unix_secs, quota_window, utilization, weighted_utilization_sum, capacity_ratio_sum, eligible_upstreams, contributing_upstreams, stale_upstreams, missing_observation_upstreams, missing_metadata_upstreams, header_contributing_upstreams, api_contributing_upstreams, max_observed_at_unix_millis, computed_at_unix_millis, policy_version) VALUES (1793882208, '7d_fable', NULL, -0, -0, 0, 0, 0, 0, 0, 0, 0, NULL, 1793882208155, 1);


--
-- Data for Name: price_catalog_snapshots_v1; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: principals_v1; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: prompt_cache_observations; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: request_events_v1; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: upstream_affinity_v1; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: upstream_api_key_details_v1; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: upstream_api_key_secret_v1; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: upstream_api_usage_v1; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: upstream_oauth_token_v1; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: upstream_plan_tier_history_v1; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: upstream_rate_limit_state_v1; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: upstream_spec_v1; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: upstream_status_v1; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: upstream_subscription_metadata_v1; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: upstream_subscription_quota_checkpoints_v1; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: upstream_subscription_quota_latest_v1; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: usage_rollup_checkpoints_v1; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: usage_rollups_v2; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: warmup_attempts_v1; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: wasm_blobs_v2; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.wasm_blobs_v2 (sha256, bytes, size_bytes, created_at) VALUES ('\x737562736372697074696f6e2d707265666572656e6365000000000000000000', '\x', 0, '1970-01-01 00:00:00+00');


--
-- Data for Name: wasm_registry_v2; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.wasm_registry_v2 (id, sha256, name, original_filename, label, uploaded_at, uploaded_by_admin_id, revision, supported_slots, description, usage, hook_metadata, plugin_version) VALUES ('00000000-0000-0000-0000-000000000002', '\x737562736372697074696f6e2d707265666572656e6365000000000000000000', 'subscription-preference', 'builtin://subscription-preference', 'Built-in subscription preference filter', '1970-01-01 00:00:00+00', '00000000-0000-0000-0000-000000000000', 0, '{router}', '', '', '{}', NULL);


--
-- Name: audit_log_v1_seq_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.audit_log_v1_seq_seq', 1, false);


--
-- Name: price_catalog_snapshots_v1_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.price_catalog_snapshots_v1_id_seq', 1, false);


--
-- Name: request_events_v1_seq_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.request_events_v1_seq_seq', 1, false);


--
-- Name: jobs jobs_pkey; Type: CONSTRAINT; Schema: apalis; Owner: -
--

ALTER TABLE ONLY apalis.jobs
    ADD CONSTRAINT jobs_pkey PRIMARY KEY (id);


--
-- Name: workers workers_pkey; Type: CONSTRAINT; Schema: apalis; Owner: -
--

ALTER TABLE ONLY apalis.workers
    ADD CONSTRAINT workers_pkey PRIMARY KEY (id);


--
-- Name: _sqlx_migrations _sqlx_migrations_pkey; Type: CONSTRAINT; Schema: cc_lb_scheduler; Owner: -
--

ALTER TABLE ONLY cc_lb_scheduler._sqlx_migrations
    ADD CONSTRAINT _sqlx_migrations_pkey PRIMARY KEY (version);


--
-- Name: anthropic_compat_etags anthropic_compat_etags_pkey; Type: CONSTRAINT; Schema: cc_lb_scheduler; Owner: -
--

ALTER TABLE ONLY cc_lb_scheduler.anthropic_compat_etags
    ADD CONSTRAINT anthropic_compat_etags_pkey PRIMARY KEY (key);


--
-- Name: oauth_usage_poll_cursors oauth_usage_poll_cursors_pkey; Type: CONSTRAINT; Schema: cc_lb_scheduler; Owner: -
--

ALTER TABLE ONLY cc_lb_scheduler.oauth_usage_poll_cursors
    ADD CONSTRAINT oauth_usage_poll_cursors_pkey PRIMARY KEY (upstream_id);


--
-- Name: price_catalog_versions price_catalog_versions_pkey; Type: CONSTRAINT; Schema: cc_lb_scheduler; Owner: -
--

ALTER TABLE ONLY cc_lb_scheduler.price_catalog_versions
    ADD CONSTRAINT price_catalog_versions_pkey PRIMARY KEY (source);


--
-- Name: _sqlx_migrations _sqlx_migrations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public._sqlx_migrations
    ADD CONSTRAINT _sqlx_migrations_pkey PRIMARY KEY (version);


--
-- Name: anthropic_compatibility_kv_v1 anthropic_compatibility_kv_v1_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.anthropic_compatibility_kv_v1
    ADD CONSTRAINT anthropic_compatibility_kv_v1_pkey PRIMARY KEY (key);


--
-- Name: api_key_concurrency_holds_v1 api_key_concurrency_holds_v1_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.api_key_concurrency_holds_v1
    ADD CONSTRAINT api_key_concurrency_holds_v1_pkey PRIMARY KEY (hold_id);


--
-- Name: api_key_usage_buckets_v1 api_key_usage_buckets_v1_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.api_key_usage_buckets_v1
    ADD CONSTRAINT api_key_usage_buckets_v1_pkey PRIMARY KEY (writer_epoch, key_id, bucket_width_secs, bucket_start_unix_secs);


--
-- Name: api_key_usage_writers_v1 api_key_usage_writers_v1_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.api_key_usage_writers_v1
    ADD CONSTRAINT api_key_usage_writers_v1_pkey PRIMARY KEY (writer_epoch);


--
-- Name: audit_log_v1 audit_log_v1_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.audit_log_v1
    ADD CONSTRAINT audit_log_v1_pkey PRIMARY KEY (seq);


--
-- Name: cache_keepalive_decisions cache_keepalive_decisions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.cache_keepalive_decisions
    ADD CONSTRAINT cache_keepalive_decisions_pkey PRIMARY KEY (source_ref_id);


--
-- Name: cache_keepalive_sessions cache_keepalive_sessions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.cache_keepalive_sessions
    ADD CONSTRAINT cache_keepalive_sessions_pkey PRIMARY KEY (session_key_hash);


--
-- Name: cache_keepalive_turns cache_keepalive_turns_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.cache_keepalive_turns
    ADD CONSTRAINT cache_keepalive_turns_pkey PRIMARY KEY (source_ref_id);


--
-- Name: config_draft_v1 config_draft_v1_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.config_draft_v1
    ADD CONSTRAINT config_draft_v1_pkey PRIMARY KEY (id);


--
-- Name: config_history_v1 config_history_v1_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.config_history_v1
    ADD CONSTRAINT config_history_v1_pkey PRIMARY KEY (revision);


--
-- Name: managed_api_key_index_v1 managed_api_key_index_v1_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.managed_api_key_index_v1
    ADD CONSTRAINT managed_api_key_index_v1_pkey PRIMARY KEY (index_hash);


--
-- Name: managed_api_keys_v1 managed_api_keys_v1_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.managed_api_keys_v1
    ADD CONSTRAINT managed_api_keys_v1_pkey PRIMARY KEY (principal_id, key_id);


--
-- Name: meta meta_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.meta
    ADD CONSTRAINT meta_pkey PRIMARY KEY (key);


--
-- Name: metadata_tier_mapping_override_v1 metadata_tier_mapping_override_v1_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.metadata_tier_mapping_override_v1
    ADD CONSTRAINT metadata_tier_mapping_override_v1_pkey PRIMARY KEY (organization_type, rate_limit_tier, seat_tier, effective_from_unix_millis);


--
-- Name: oauth_pkce_flows_v1 oauth_pkce_flows_v1_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.oauth_pkce_flows_v1
    ADD CONSTRAINT oauth_pkce_flows_v1_pkey PRIMARY KEY (state_token);


--
-- Name: organization_metadata_v1 organization_metadata_v1_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.organization_metadata_v1
    ADD CONSTRAINT organization_metadata_v1_pkey PRIMARY KEY (organization_uuid);


--
-- Name: overview_excluded_error_rollups_v1 overview_excluded_error_rollups_v1_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.overview_excluded_error_rollups_v1
    ADD CONSTRAINT overview_excluded_error_rollups_v1_pkey PRIMARY KEY (resolution, bucket_start_unix_secs);


--
-- Name: plan_tier_ratio_history_v1 plan_tier_ratio_history_v1_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.plan_tier_ratio_history_v1
    ADD CONSTRAINT plan_tier_ratio_history_v1_pkey PRIMARY KEY (tier_key, effective_from_unix_millis);


--
-- Name: plugin_chains_v2 plugin_chains_v2_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.plugin_chains_v2
    ADD CONSTRAINT plugin_chains_v2_pkey PRIMARY KEY (id);


--
-- Name: pool_subscription_quota_history_v1 pool_subscription_quota_history_v1_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.pool_subscription_quota_history_v1
    ADD CONSTRAINT pool_subscription_quota_history_v1_pkey PRIMARY KEY (snapshot_at_unix_secs, quota_window);


--
-- Name: price_catalog_snapshots_v1 price_catalog_snapshots_v1_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.price_catalog_snapshots_v1
    ADD CONSTRAINT price_catalog_snapshots_v1_pkey PRIMARY KEY (id);


--
-- Name: principals_v1 principals_v1_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.principals_v1
    ADD CONSTRAINT principals_v1_pkey PRIMARY KEY (id);


--
-- Name: prompt_cache_observations prompt_cache_observations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.prompt_cache_observations
    ADD CONSTRAINT prompt_cache_observations_pkey PRIMARY KEY (upstream_id, canonical_model_id, v3_prefix_key, ttl_class);


--
-- Name: request_events_v1 request_events_v1_event_id_unique; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.request_events_v1
    ADD CONSTRAINT request_events_v1_event_id_unique UNIQUE (event_id);


--
-- Name: request_events_v1 request_events_v1_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.request_events_v1
    ADD CONSTRAINT request_events_v1_pkey PRIMARY KEY (seq);


--
-- Name: upstream_affinity_v1 upstream_affinity_v1_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.upstream_affinity_v1
    ADD CONSTRAINT upstream_affinity_v1_pkey PRIMARY KEY (principal_id, provider, kind, value_sha256);


--
-- Name: upstream_api_key_details_v1 upstream_api_key_details_v1_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.upstream_api_key_details_v1
    ADD CONSTRAINT upstream_api_key_details_v1_pkey PRIMARY KEY (upstream_id);


--
-- Name: upstream_api_key_secret_v1 upstream_api_key_secret_v1_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.upstream_api_key_secret_v1
    ADD CONSTRAINT upstream_api_key_secret_v1_pkey PRIMARY KEY (upstream_id);


--
-- Name: upstream_api_usage_v1 upstream_api_usage_v1_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.upstream_api_usage_v1
    ADD CONSTRAINT upstream_api_usage_v1_pkey PRIMARY KEY (upstream_id, attribution, subject_id, bucket_width_secs, bucket_start_unix_secs, model);


--
-- Name: upstream_oauth_token_v1 upstream_oauth_token_v1_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.upstream_oauth_token_v1
    ADD CONSTRAINT upstream_oauth_token_v1_pkey PRIMARY KEY (upstream_id);


--
-- Name: upstream_plan_tier_history_v1 upstream_plan_tier_history_v1_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.upstream_plan_tier_history_v1
    ADD CONSTRAINT upstream_plan_tier_history_v1_pkey PRIMARY KEY (upstream_id, effective_from_unix_millis);


--
-- Name: upstream_rate_limit_state_v1 upstream_rate_limit_state_v1_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.upstream_rate_limit_state_v1
    ADD CONSTRAINT upstream_rate_limit_state_v1_pkey PRIMARY KEY (upstream_id, "window", kind);


--
-- Name: upstream_spec_v1 upstream_spec_v1_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.upstream_spec_v1
    ADD CONSTRAINT upstream_spec_v1_pkey PRIMARY KEY (id);


--
-- Name: upstream_status_v1 upstream_status_v1_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.upstream_status_v1
    ADD CONSTRAINT upstream_status_v1_pkey PRIMARY KEY (upstream_id);


--
-- Name: upstream_subscription_metadata_v1 upstream_subscription_metadata_v1_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.upstream_subscription_metadata_v1
    ADD CONSTRAINT upstream_subscription_metadata_v1_pkey PRIMARY KEY (upstream_id);


--
-- Name: upstream_subscription_quota_checkpoints_v1 upstream_subscription_quota_checkpoints_v1_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.upstream_subscription_quota_checkpoints_v1
    ADD CONSTRAINT upstream_subscription_quota_checkpoints_v1_pkey PRIMARY KEY (upstream_id, "window", source, changed_at_unix_millis, sample_id);


--
-- Name: upstream_subscription_quota_latest_v1 upstream_subscription_quota_latest_v1_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.upstream_subscription_quota_latest_v1
    ADD CONSTRAINT upstream_subscription_quota_latest_v1_pkey PRIMARY KEY (upstream_id, "window", source);


--
-- Name: usage_rollup_checkpoints_v1 usage_rollup_checkpoints_v1_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.usage_rollup_checkpoints_v1
    ADD CONSTRAINT usage_rollup_checkpoints_v1_pkey PRIMARY KEY (id);


--
-- Name: usage_rollups_v2 usage_rollups_v2_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.usage_rollups_v2
    ADD CONSTRAINT usage_rollups_v2_pkey PRIMARY KEY (resolution, bucket_start_unix_secs, principal_id, upstream_id, model);


--
-- Name: warmup_attempts_v1 warmup_attempts_v1_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.warmup_attempts_v1
    ADD CONSTRAINT warmup_attempts_v1_pkey PRIMARY KEY (id);


--
-- Name: wasm_blobs_v2 wasm_blobs_v2_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.wasm_blobs_v2
    ADD CONSTRAINT wasm_blobs_v2_pkey PRIMARY KEY (sha256);


--
-- Name: wasm_registry_v2 wasm_registry_v2_name_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.wasm_registry_v2
    ADD CONSTRAINT wasm_registry_v2_name_key UNIQUE (name);


--
-- Name: wasm_registry_v2 wasm_registry_v2_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.wasm_registry_v2
    ADD CONSTRAINT wasm_registry_v2_pkey PRIMARY KEY (id);


--
-- Name: idx; Type: INDEX; Schema: apalis; Owner: -
--

CREATE INDEX idx ON apalis.workers USING btree (id);


--
-- Name: idx_jobs_idempotency_key; Type: INDEX; Schema: apalis; Owner: -
--

CREATE UNIQUE INDEX idx_jobs_idempotency_key ON apalis.jobs USING btree (job_type, idempotency_key);


--
-- Name: jtidx; Type: INDEX; Schema: apalis; Owner: -
--

CREATE INDEX jtidx ON apalis.jobs USING btree (job_type);


--
-- Name: lidx; Type: INDEX; Schema: apalis; Owner: -
--

CREATE INDEX lidx ON apalis.jobs USING btree (lock_by);


--
-- Name: lsidx; Type: INDEX; Schema: apalis; Owner: -
--

CREATE INDEX lsidx ON apalis.workers USING btree (last_seen);


--
-- Name: sidx; Type: INDEX; Schema: apalis; Owner: -
--

CREATE INDEX sidx ON apalis.jobs USING btree (status);


--
-- Name: unique_job_id; Type: INDEX; Schema: apalis; Owner: -
--

CREATE UNIQUE INDEX unique_job_id ON apalis.jobs USING btree (id);


--
-- Name: unique_worker_id; Type: INDEX; Schema: apalis; Owner: -
--

CREATE UNIQUE INDEX unique_worker_id ON apalis.workers USING btree (id);


--
-- Name: wtidx; Type: INDEX; Schema: apalis; Owner: -
--

CREATE INDEX wtidx ON apalis.workers USING btree (worker_type);


--
-- Name: anthropic_compatibility_kv_v1_last_updated_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX anthropic_compatibility_kv_v1_last_updated_idx ON public.anthropic_compatibility_kv_v1 USING btree (last_updated_at_unix_secs);


--
-- Name: api_key_concurrency_holds_v1_key_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX api_key_concurrency_holds_v1_key_id_idx ON public.api_key_concurrency_holds_v1 USING btree (key_id);


--
-- Name: api_key_usage_buckets_v1_expiry_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX api_key_usage_buckets_v1_expiry_idx ON public.api_key_usage_buckets_v1 USING btree (((bucket_start_unix_secs + bucket_width_secs)));


--
-- Name: api_key_usage_buckets_v1_key_time_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX api_key_usage_buckets_v1_key_time_idx ON public.api_key_usage_buckets_v1 USING btree (key_id, bucket_width_secs, bucket_start_unix_secs);


--
-- Name: api_key_usage_writers_v1_lease_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX api_key_usage_writers_v1_lease_idx ON public.api_key_usage_writers_v1 USING btree (lease_until_unix_secs);


--
-- Name: audit_log_v1_actor_ts_seq; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX audit_log_v1_actor_ts_seq ON public.audit_log_v1 USING btree (actor_authority, actor_subject, ts DESC, seq DESC) WHERE (actor_authority IS NOT NULL);


--
-- Name: audit_log_v1_admin_action_actor_ts_seq; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX audit_log_v1_admin_action_actor_ts_seq ON public.audit_log_v1 USING btree (actor_authority, actor_subject, ts DESC, seq DESC) WHERE (admin_action IS NOT NULL);


--
-- Name: audit_log_v1_admin_action_principal_ts_seq; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX audit_log_v1_admin_action_principal_ts_seq ON public.audit_log_v1 USING btree (principal_id, ts DESC, seq DESC) WHERE (admin_action IS NOT NULL);


--
-- Name: audit_log_v1_admin_action_ts_seq; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX audit_log_v1_admin_action_ts_seq ON public.audit_log_v1 USING btree (ts DESC, seq DESC) WHERE (admin_action IS NOT NULL);


--
-- Name: audit_log_v1_principal_ts_seq; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX audit_log_v1_principal_ts_seq ON public.audit_log_v1 USING btree (principal_id, ts DESC, seq DESC);


--
-- Name: audit_log_v1_ts; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX audit_log_v1_ts ON public.audit_log_v1 USING btree (ts);


--
-- Name: idx_cache_keepalive_decisions_principal_last_message; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_cache_keepalive_decisions_principal_last_message ON public.cache_keepalive_decisions USING btree (principal_id, last_message_at_ms DESC, (('decision:'::text || source_ref_id)));


--
-- Name: idx_cache_keepalive_decisions_ts; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_cache_keepalive_decisions_ts ON public.cache_keepalive_decisions USING btree (ts);


--
-- Name: idx_cache_keepalive_sessions_expiry; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_cache_keepalive_sessions_expiry ON public.cache_keepalive_sessions USING btree (expires_at);


--
-- Name: idx_cache_keepalive_sessions_pending_updated; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_cache_keepalive_sessions_pending_updated ON public.cache_keepalive_sessions USING btree (status, enqueue_state, updated_at);


--
-- Name: idx_cache_keepalive_sessions_principal_effective_message; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_cache_keepalive_sessions_principal_effective_message ON public.cache_keepalive_sessions USING btree (principal_id, last_message_at_ms DESC, (('session:'::text || session_key_hash)));


--
-- Name: idx_cache_keepalive_sessions_run_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_cache_keepalive_sessions_run_at ON public.cache_keepalive_sessions USING btree (status, run_at);


--
-- Name: idx_cache_keepalive_turns_session_ts; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_cache_keepalive_turns_session_ts ON public.cache_keepalive_turns USING btree (principal_id, session_key_hash, ts DESC);


--
-- Name: idx_cache_keepalive_turns_ts; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_cache_keepalive_turns_ts ON public.cache_keepalive_turns USING btree (ts);


--
-- Name: idx_warmup_attempts_v1_upstream_attempted; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_warmup_attempts_v1_upstream_attempted ON public.warmup_attempts_v1 USING btree (upstream_id, attempted_at_unix_secs DESC, id DESC);


--
-- Name: managed_api_key_index_v1_composite_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX managed_api_key_index_v1_composite_idx ON public.managed_api_key_index_v1 USING btree (principal_id, key_id);


--
-- Name: metadata_tier_mapping_override_v1_one_open_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX metadata_tier_mapping_override_v1_one_open_idx ON public.metadata_tier_mapping_override_v1 USING btree (organization_type, rate_limit_tier, seat_tier) WHERE (effective_to_unix_millis IS NULL);


--
-- Name: oauth_pkce_flows_v1_expires_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX oauth_pkce_flows_v1_expires_at_idx ON public.oauth_pkce_flows_v1 USING btree (expires_at_unix_secs);


--
-- Name: plan_tier_ratio_history_v1_asof_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX plan_tier_ratio_history_v1_asof_idx ON public.plan_tier_ratio_history_v1 USING btree (tier_key, effective_from_unix_millis DESC, effective_to_unix_millis);


--
-- Name: plan_tier_ratio_history_v1_one_open_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX plan_tier_ratio_history_v1_one_open_idx ON public.plan_tier_ratio_history_v1 USING btree (tier_key) WHERE (effective_to_unix_millis IS NULL);


--
-- Name: plugin_chains_v2_principal_slot_order_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX plugin_chains_v2_principal_slot_order_idx ON public.plugin_chains_v2 USING btree (principal_id, slot, order_value);


--
-- Name: plugin_chains_v2_wasm_registry_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX plugin_chains_v2_wasm_registry_idx ON public.plugin_chains_v2 USING btree (wasm_registry_id);


--
-- Name: pool_subscription_quota_history_v1_window_time_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX pool_subscription_quota_history_v1_window_time_idx ON public.pool_subscription_quota_history_v1 USING btree (quota_window, snapshot_at_unix_secs DESC) INCLUDE (utilization);


--
-- Name: price_catalog_snapshots_v1_fetched_at_ms_desc_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX price_catalog_snapshots_v1_fetched_at_ms_desc_idx ON public.price_catalog_snapshots_v1 USING btree (fetched_at_ms DESC);


--
-- Name: price_catalog_snapshots_v1_payload_hash_uidx; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX price_catalog_snapshots_v1_payload_hash_uidx ON public.price_catalog_snapshots_v1 USING btree (payload_hash);


--
-- Name: principals_v1_name_active_uniq; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX principals_v1_name_active_uniq ON public.principals_v1 USING btree (name) WHERE (deleted_at IS NULL);


--
-- Name: principals_v1_name_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX principals_v1_name_idx ON public.principals_v1 USING btree (name);


--
-- Name: prompt_cache_observations_v3_prefix_lookup_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prompt_cache_observations_v3_prefix_lookup_idx ON public.prompt_cache_observations USING btree (upstream_id, canonical_model_id, v3_prefix_key, ttl_class, expires_at);


--
-- Name: request_events_v1_effective_kind_list_order_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX request_events_v1_effective_kind_list_order_idx ON public.request_events_v1 USING btree ((
CASE
    WHEN (source_kind = 'renewal'::text) THEN 'renewal'::text
    ELSE COALESCE(event_kind, 'unclassified'::text)
END), list_ts_ms DESC, list_event_key DESC);


--
-- Name: request_events_v1_list_order_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX request_events_v1_list_order_idx ON public.request_events_v1 USING btree (list_ts_ms DESC, list_event_key DESC);


--
-- Name: request_events_v1_principal_key_ts_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX request_events_v1_principal_key_ts_idx ON public.request_events_v1 USING btree (principal_id, key_id, ts);


--
-- Name: request_events_v1_principal_list_order_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX request_events_v1_principal_list_order_idx ON public.request_events_v1 USING btree (principal_id, list_ts_ms DESC, list_event_key DESC) INCLUDE (list_cost_usd_micros, list_cost_input_micros, list_cost_output_micros, list_cost_cache_creation_5m_micros, list_cost_cache_creation_1h_micros, list_cost_cache_read_micros);


--
-- Name: request_events_v1_source_kind_list_order_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX request_events_v1_source_kind_list_order_idx ON public.request_events_v1 USING btree (source_kind, list_ts_ms DESC, list_event_key DESC);


--
-- Name: request_events_v1_thread_list_order_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX request_events_v1_thread_list_order_idx ON public.request_events_v1 USING btree (thread_id, list_ts_ms DESC, list_event_key DESC);


--
-- Name: request_events_v1_ts; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX request_events_v1_ts ON public.request_events_v1 USING btree (ts);


--
-- Name: request_events_v1_tx_id_seq_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX request_events_v1_tx_id_seq_idx ON public.request_events_v1 USING btree (tx_id, seq);


--
-- Name: request_events_v1_upstream_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX request_events_v1_upstream_id_idx ON public.request_events_v1 USING btree (upstream_id, principal_id, list_ts_ms DESC) INCLUDE (list_cost_usd_micros, list_cost_input_micros, list_cost_output_micros, list_cost_cache_creation_5m_micros, list_cost_cache_creation_1h_micros, list_cost_cache_read_micros);


--
-- Name: request_events_v1_upstream_list_order_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX request_events_v1_upstream_list_order_idx ON public.request_events_v1 USING btree (upstream_id, list_ts_ms DESC, list_event_key DESC);


--
-- Name: upstream_affinity_v1_expires_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX upstream_affinity_v1_expires_at_idx ON public.upstream_affinity_v1 USING btree (expires_at_unix_secs) WHERE (expires_at_unix_secs IS NOT NULL);


--
-- Name: upstream_affinity_v1_observed_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX upstream_affinity_v1_observed_at_idx ON public.upstream_affinity_v1 USING btree (observed_at_unix_secs);


--
-- Name: upstream_affinity_v1_upstream_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX upstream_affinity_v1_upstream_id_idx ON public.upstream_affinity_v1 USING btree (upstream_id);


--
-- Name: upstream_api_usage_v1_bucket_start_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX upstream_api_usage_v1_bucket_start_idx ON public.upstream_api_usage_v1 USING btree (bucket_start_unix_secs);


--
-- Name: upstream_plan_tier_history_v1_asof_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX upstream_plan_tier_history_v1_asof_idx ON public.upstream_plan_tier_history_v1 USING btree (upstream_id, effective_from_unix_millis DESC, effective_to_unix_millis);


--
-- Name: upstream_plan_tier_history_v1_one_open_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX upstream_plan_tier_history_v1_one_open_idx ON public.upstream_plan_tier_history_v1 USING btree (upstream_id) WHERE (effective_to_unix_millis IS NULL);


--
-- Name: upstream_plan_tier_history_v1_tier_time_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX upstream_plan_tier_history_v1_tier_time_idx ON public.upstream_plan_tier_history_v1 USING btree (tier_key, effective_from_unix_millis DESC);


--
-- Name: upstream_rate_limit_state_v1_observed_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX upstream_rate_limit_state_v1_observed_at_idx ON public.upstream_rate_limit_state_v1 USING btree (observed_at_unix_secs);


--
-- Name: upstream_rate_limit_state_v1_upstream_kind_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX upstream_rate_limit_state_v1_upstream_kind_idx ON public.upstream_rate_limit_state_v1 USING btree (upstream_id, kind);


--
-- Name: upstream_spec_v1_active_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX upstream_spec_v1_active_idx ON public.upstream_spec_v1 USING btree (id) WHERE (deleted_at IS NULL);


--
-- Name: upstream_spec_v1_name_active_uniq; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX upstream_spec_v1_name_active_uniq ON public.upstream_spec_v1 USING btree (name) WHERE (deleted_at IS NULL);


--
-- Name: upstream_spec_v1_warmup_wasm_registry_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX upstream_spec_v1_warmup_wasm_registry_id_idx ON public.upstream_spec_v1 USING btree ((((warmup_dialect_plugin ->> 'wasm_registry_id'::text))::uuid)) WHERE ((deleted_at IS NULL) AND (warmup_dialect_plugin IS NOT NULL));


--
-- Name: upstream_subscription_metadata_v1_org_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX upstream_subscription_metadata_v1_org_idx ON public.upstream_subscription_metadata_v1 USING btree (organization_uuid);


--
-- Name: upstream_subscription_quota_checkpoints_slim_cover_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX upstream_subscription_quota_checkpoints_slim_cover_idx ON public.upstream_subscription_quota_checkpoints_v1 USING btree (upstream_id, "window", source, changed_at_unix_millis, sample_id) INCLUDE (utilization, status, resets_at_unix_secs);


--
-- Name: upstream_subscription_quota_latest_lookup_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX upstream_subscription_quota_latest_lookup_idx ON public.upstream_subscription_quota_latest_v1 USING btree (upstream_id, "window", observed_at_unix_millis DESC);


--
-- Name: usage_rollups_v2_upstream_resolution_bucket_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX usage_rollups_v2_upstream_resolution_bucket_idx ON public.usage_rollups_v2 USING btree (upstream_id, resolution, bucket_start_unix_secs);


--
-- Name: wasm_registry_v2_name_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX wasm_registry_v2_name_idx ON public.wasm_registry_v2 USING btree (name);


--
-- Name: wasm_registry_v2_sha_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX wasm_registry_v2_sha_idx ON public.wasm_registry_v2 USING btree (sha256);


--
-- Name: jobs notify_workers; Type: TRIGGER; Schema: apalis; Owner: -
--

CREATE TRIGGER notify_workers AFTER INSERT ON apalis.jobs FOR EACH ROW EXECUTE FUNCTION apalis.notify_new_jobs();


--
-- Name: jobs fk_worker_lock_by; Type: FK CONSTRAINT; Schema: apalis; Owner: -
--

ALTER TABLE ONLY apalis.jobs
    ADD CONSTRAINT fk_worker_lock_by FOREIGN KEY (lock_by) REFERENCES apalis.workers(id);


--
-- Name: api_key_usage_buckets_v1 api_key_usage_buckets_v1_writer_epoch_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.api_key_usage_buckets_v1
    ADD CONSTRAINT api_key_usage_buckets_v1_writer_epoch_fkey FOREIGN KEY (writer_epoch) REFERENCES public.api_key_usage_writers_v1(writer_epoch) ON DELETE CASCADE;


--
-- Name: managed_api_key_index_v1 managed_api_key_index_v1_principal_id_key_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.managed_api_key_index_v1
    ADD CONSTRAINT managed_api_key_index_v1_principal_id_key_id_fkey FOREIGN KEY (principal_id, key_id) REFERENCES public.managed_api_keys_v1(principal_id, key_id) ON DELETE CASCADE;


--
-- Name: plugin_chains_v2 plugin_chains_v2_wasm_registry_fk; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.plugin_chains_v2
    ADD CONSTRAINT plugin_chains_v2_wasm_registry_fk FOREIGN KEY (wasm_registry_id) REFERENCES public.wasm_registry_v2(id) ON DELETE RESTRICT;


--
-- Name: upstream_affinity_v1 upstream_affinity_v1_upstream_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.upstream_affinity_v1
    ADD CONSTRAINT upstream_affinity_v1_upstream_id_fkey FOREIGN KEY (upstream_id) REFERENCES public.upstream_spec_v1(id) ON DELETE CASCADE;


--
-- Name: upstream_api_key_details_v1 upstream_api_key_details_v1_upstream_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.upstream_api_key_details_v1
    ADD CONSTRAINT upstream_api_key_details_v1_upstream_id_fkey FOREIGN KEY (upstream_id) REFERENCES public.upstream_spec_v1(id) ON DELETE CASCADE;


--
-- Name: upstream_api_key_secret_v1 upstream_api_key_secret_v1_upstream_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.upstream_api_key_secret_v1
    ADD CONSTRAINT upstream_api_key_secret_v1_upstream_id_fkey FOREIGN KEY (upstream_id) REFERENCES public.upstream_spec_v1(id) ON DELETE CASCADE;


--
-- Name: upstream_api_usage_v1 upstream_api_usage_v1_upstream_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.upstream_api_usage_v1
    ADD CONSTRAINT upstream_api_usage_v1_upstream_id_fkey FOREIGN KEY (upstream_id) REFERENCES public.upstream_spec_v1(id) ON DELETE CASCADE;


--
-- Name: upstream_oauth_token_v1 upstream_oauth_token_v1_upstream_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.upstream_oauth_token_v1
    ADD CONSTRAINT upstream_oauth_token_v1_upstream_id_fkey FOREIGN KEY (upstream_id) REFERENCES public.upstream_spec_v1(id) ON DELETE CASCADE;


--
-- Name: upstream_status_v1 upstream_status_v1_upstream_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.upstream_status_v1
    ADD CONSTRAINT upstream_status_v1_upstream_id_fkey FOREIGN KEY (upstream_id) REFERENCES public.upstream_spec_v1(id) ON DELETE CASCADE;


--
-- Name: warmup_attempts_v1 warmup_attempts_v1_upstream_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.warmup_attempts_v1
    ADD CONSTRAINT warmup_attempts_v1_upstream_id_fkey FOREIGN KEY (upstream_id) REFERENCES public.upstream_spec_v1(id) ON DELETE CASCADE;


--
-- Name: wasm_registry_v2 wasm_registry_v2_sha_fk; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.wasm_registry_v2
    ADD CONSTRAINT wasm_registry_v2_sha_fk FOREIGN KEY (sha256) REFERENCES public.wasm_blobs_v2(sha256) ON DELETE RESTRICT;


--
-- PostgreSQL database dump complete
--

\unrestrict 5uYxoijXCQkZCd74Dciyp0Q6TrzVke1nbqfDzSHe4JFyFNw8223kVYApT5R960w

