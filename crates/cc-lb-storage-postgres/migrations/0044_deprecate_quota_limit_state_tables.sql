-- Mark quota and principal_limit_states tables as deprecated
-- QuotaStore and LimitStateStore were removed in T20
-- Tables will be dropped in the next major release

COMMENT ON TABLE quotas_by_principal_v1 IS 'DEPRECATED: QuotaStore removed in T20; table to be dropped in next major release';
COMMENT ON TABLE principal_limit_states_v1 IS 'DEPRECATED: LimitStateStore removed in T20; table to be dropped in next major release';
