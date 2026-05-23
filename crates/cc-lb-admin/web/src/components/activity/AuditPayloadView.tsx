interface AuditPayloadViewProps {
  payload: Record<string, unknown>;
}

const SENSITIVE_KEYS = [
  /token/i,
  /secret/i,
  /plaintext/i,
  /key_hash_b64/i,
  /client_secret/i,
  /password/i,
];

function isSensitive(key: string): boolean {
  return SENSITIVE_KEYS.some((regex) => regex.test(key));
}

export function AuditPayloadView({ payload }: AuditPayloadViewProps) {
  const entries = Object.entries(payload);

  if (entries.length === 0) {
    return <span className="text-sm text-gray-400 italic">empty</span>;
  }

  return (
    <div className="flex flex-wrap gap-x-4 gap-y-1">
      {entries.map(([key, value]) => {
        const displayValue = isSensitive(key)
          ? '<redacted>'
          : JSON.stringify(value);
        return (
          <div key={key} className="flex items-baseline gap-1 text-sm">
            <span className="text-graphite-400">{key}:</span>
            <span
              className={`font-mono ${isSensitive(key) ? 'text-red-500 font-bold' : 'text-graphite-50'}`}
            >
              {displayValue}
            </span>
          </div>
        );
      })}
    </div>
  );
}
