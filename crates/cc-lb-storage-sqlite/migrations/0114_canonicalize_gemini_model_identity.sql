-- Reconcile historical Gemini model identities after the provider started
-- storing the wire `models/` prefix separately from the internal model key.
-- The upstream kind join is intentionally required: a model string alone does
-- not prove provider ownership, and unknown/unrelated rows must remain intact.
CREATE TEMP TABLE _canonicalize_gemini_model_identity_needed (
    needed INTEGER NOT NULL CHECK (needed = 1)
);
INSERT INTO _canonicalize_gemini_model_identity_needed (needed)
SELECT 1
WHERE EXISTS (
    SELECT 1
    FROM request_events_v1 AS events
    JOIN upstream_spec_v1 AS upstream ON upstream.id = events.upstream_id
    WHERE upstream.kind IN ('gemini_api_key', 'gemini_oauth')
      AND events.model LIKE 'models/%'
);

UPDATE request_events_v1
SET model = substr(model, 8)
WHERE model IS NOT NULL
  AND substr(model, 1, 7) = 'models/'
  AND EXISTS (
      SELECT 1
      FROM upstream_spec_v1 AS upstream
      WHERE upstream.id = request_events_v1.upstream_id
        AND upstream.kind IN ('gemini_api_key', 'gemini_oauth')
  )
  AND EXISTS (
      SELECT 1
      FROM _canonicalize_gemini_model_identity_needed
  );


-- Rollups are additive. Replaying the rewritten event history on top of the
-- existing aggregates would double-count it, so clear every derived rollup
-- and every cursor used by the rollup worker in the same migration transaction.
DELETE FROM usage_rollups_v2
WHERE EXISTS (
    SELECT 1
    FROM _canonicalize_gemini_model_identity_needed
);
DELETE FROM overview_excluded_error_rollups_v1
WHERE EXISTS (
    SELECT 1
    FROM _canonicalize_gemini_model_identity_needed
);
DELETE FROM usage_rollup_checkpoints_v1
WHERE id IN (
    'high_water',
    'cost_components_backfill_upper',
    'cost_components_backfill_cursor'
)
  AND EXISTS (
      SELECT 1
      FROM _canonicalize_gemini_model_identity_needed
  );

DROP TABLE _canonicalize_gemini_model_identity_needed;
