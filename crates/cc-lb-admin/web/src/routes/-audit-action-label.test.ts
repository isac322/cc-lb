import { expect, test } from 'vitest';
import { humanizeAuditAction } from '../components/audit/auditEntry';

test.each([
  ['plugin_chain_update', 'Plugin chain updated'],
  ['principal_key_issue', 'Principal key issued'],
  ['principal_keys_list', 'Principal keys listed'],
  ['config_draft_put', 'Config draft saved'],
  ['upstream_create_from_oauth_draft', 'Upstream created from OAuth draft'],
  ['config_save_failed', 'Config save failed'],
  ['upstream_oauth_refresh_failure', 'Upstream OAuth refresh failed'],
  ['upstream_oauth_refresh_success', 'Upstream OAuth refresh succeeded'],
  ['plugin_registry_upload_attempt', 'Plugin registry upload attempt'],
  ['auth_rejected', 'Authentication rejected'],
  ['authz_denied', 'Authorization denied'],
  ['unknown', 'Unknown'],
])('humanizeAuditAction(%s) is "%s"', (raw, label) => {
  expect(humanizeAuditAction(raw)).toBe(label);
});
