interface AuditFilterBarProps {
  selectedKind: string;
  onKindChange: (kind: string) => void;
}

const AUDIT_KINDS = [
  'config_apply',
  'api_key_issue',
  'api_key_revoke',
  'credential_rotate',
  'credential_revoke',
  'principal_create',
  'principal_update',
  'principal_disable',
  'quota_override',
  'killswitch_set',
  'oauth_complete',
];

export function AuditFilterBar({
  selectedKind,
  onKindChange,
}: AuditFilterBarProps) {
  return (
    <div className="flex items-center gap-4 bg-graphite-900 p-4 rounded-lg border border-graphite-800 shadow-sm">
      <div className="flex items-center gap-2">
        <label
          htmlFor="kind-filter"
          className="text-sm font-medium text-gray-700"
        >
          Action Kind:
        </label>
        <select
          id="kind-filter"
          value={selectedKind}
          onChange={(e) => onKindChange(e.target.value)}
          className="block w-48 rounded-md border-graphite-700 shadow-sm focus:border-blue-500 focus:ring-blue-500 sm:text-sm"
        >
          <option value="">All Actions</option>
          {AUDIT_KINDS.map((kind) => (
            <option key={kind} value={kind}>
              {kind}
            </option>
          ))}
        </select>
      </div>
    </div>
  );
}
