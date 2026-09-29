import { expect, test } from 'vitest';
import {
  auditTarget,
  humanizeAuditAction,
  type NameMaps,
  parseAuditAction,
} from '../components/audit/auditEntry';

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

const UUID = '6f3b2a10-9c4e-4d7a-8b5f-2e1c0a9d8f7b';

function nameMaps(upstreams: Array<[string, string]>): NameMaps {
  return { principals: new Map(), upstreams: new Map(upstreams) };
}

// Entries like upstream_subscription_metadata_refresh record the upstream by
// name and carry no id param; the target still needs the id for its link.
test('auditTarget resolves a name-only upstream entry back to its id', () => {
  const entry = {
    request_id: 'req-1',
    status: 200,
    upstream: 'isac-max',
    admin_action: 'upstream_subscription_metadata_refresh',
  };
  const maps = nameMaps([
    [UUID, 'isac-max'],
    ['isac-max', 'isac-max'],
  ]);

  expect(auditTarget(entry, parseAuditAction(entry), maps)).toEqual({
    kind: 'upstream',
    name: 'isac-max',
    id: UUID,
    resolved: true,
  });
});

test('auditTarget leaves the id null for an unknown upstream name', () => {
  const entry = {
    request_id: 'req-2',
    status: 200,
    upstream: 'ghost',
    admin_action: 'upstream_subscription_metadata_refresh',
  };

  const target = auditTarget(
    entry,
    parseAuditAction(entry),
    nameMaps([[UUID, 'isac-max']]),
  );
  expect(target?.id).toBeNull();
  expect(target?.name).toBe('ghost');
});
