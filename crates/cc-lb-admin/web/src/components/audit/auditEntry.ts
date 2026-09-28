export interface AuditEntryLike {
  request_id: string;
  ts?: number | null;
  ts_ms?: number | null;
  principal_id?: string | null;
  route?: string | null;
  upstream?: string | null;
  status: number;
  actor?: string | null;
  actor_authority?: string | null;
  actor_subject?: string | null;
  actor_kind?: string | null;
  actor_email?: string | null;
  admin_action?: string | null;
  kind?: string | null;
  payload?: Record<string, unknown> | null;
  [k: string]: unknown;
}

export type AuditCategory = 'write' | 'read' | 'auth';
export type AuditTypeFilter = AuditCategory | 'all';

export const AUDIT_TYPE_FILTERS = ['write', 'read', 'auth', 'all'] as const;

/** Singular and plural noun for counts, e.g. "3 writes". */
export const AUDIT_TYPE_NOUN: Record<AuditTypeFilter, [string, string]> = {
  write: ['write', 'writes'],
  read: ['read', 'reads'],
  auth: ['auth event', 'auth events'],
  all: ['entry', 'entries'],
};

// Audit action names come from the server (`crates/cc-lb-admin/src`,
// `cc-lb-control/src/audit_payload.rs`). The API has no read/write flag, so
// the category is derived from the naming convention: auth rejections and
// denials start with `auth`, and reads end in a read-style verb.
const AUTH_ACTION = /^authz?_/;
const READ_ACTION = /_(read|list|query|export|download|validate|poll)$/;

export interface ParsedAction {
  name: string;
  params: Array<[string, string]>;
}

/** Splits `upstream_update(id=…, fields=a,b)` into its name and parameters. */
export function parseAuditAction(entry: AuditEntryLike): ParsedAction {
  const raw = entry.admin_action ?? entry.kind ?? '';
  const match = /^([^(]+)\((.*)\)$/s.exec(raw);
  if (!match) return { name: raw, params: [] };
  const params: Array<[string, string]> = [];
  for (const part of (match[2] ?? '').split(/, (?=[A-Za-z_]+=)/)) {
    const eq = part.indexOf('=');
    if (eq <= 0) continue;
    params.push([part.slice(0, eq), part.slice(eq + 1)]);
  }
  return { name: match[1] ?? raw, params };
}

export function auditCategory(name: string): AuditCategory {
  if (AUTH_ACTION.test(name)) return 'auth';
  if (READ_ACTION.test(name)) return 'read';
  return 'write';
}

export const AUDIT_CATEGORY_LABEL: Record<AuditCategory, string> = {
  write: 'Write',
  read: 'Read',
  auth: 'Auth',
};

// Past tense for the verbs the server uses in action names.
const PAST_TENSE: Record<string, string> = {
  add: 'added',
  apply: 'applied',
  attempt: 'attempted',
  claim: 'claimed',
  clean: 'cleaned',
  complete: 'completed',
  create: 'created',
  delete: 'deleted',
  disable: 'disabled',
  discard: 'discarded',
  download: 'downloaded',
  enable: 'enabled',
  export: 'exported',
  fire: 'fired',
  import: 'imported',
  install: 'installed',
  issue: 'issued',
  list: 'listed',
  poll: 'polled',
  put: 'saved',
  query: 'queried',
  read: 'read',
  refresh: 'refreshed',
  remove: 'removed',
  rename: 'renamed',
  reorder: 'reordered',
  reset: 'reset',
  restore: 'restored',
  revoke: 'revoked',
  rotate: 'rotated',
  save: 'saved',
  start: 'started',
  update: 'updated',
  upload: 'uploaded',
  validate: 'validated',
};

// A trailing outcome word already carries the tense ("Config save failed").
const OUTCOME: Record<string, string> = {
  attempt: 'attempt',
  failed: 'failed',
  failure: 'failed',
  success: 'succeeded',
};

const WORD: Record<string, string> = {
  api: 'API',
  auth: 'Authentication',
  authz: 'Authorization',
  id: 'ID',
  oauth: 'OAuth',
  pnl: 'PnL',
  sha256: 'SHA-256',
  wasm: 'WASM',
};

/**
 * Sentence-case label for an action name: `plugin_chain_update` becomes
 * "Plugin chain updated". The raw name stays visible as the machine ID.
 */
export function humanizeAuditAction(name: string): string {
  const words = name.split('_').filter(Boolean);
  if (words.length === 0) return name;
  const last = words[words.length - 1] ?? '';
  const outcome = words.length > 1 ? OUTCOME[last] : undefined;
  if (outcome) {
    words[words.length - 1] = outcome;
  } else {
    const verb = words.findIndex(
      (word, index) => index > 0 && PAST_TENSE[word],
    );
    if (verb > 0) words[verb] = PAST_TENSE[words[verb] ?? ''] ?? '';
  }
  const text = words
    .map((word, index) => {
      const known = WORD[word];
      if (!known) return word;
      // Acronyms keep their case; the auth words are ordinary words.
      return index > 0 && (word === 'auth' || word === 'authz')
        ? known.toLowerCase()
        : known;
    })
    .join(' ');
  return text.charAt(0).toUpperCase() + text.slice(1);
}

export interface NameMaps {
  principals: ReadonlyMap<string, string>;
  upstreams: ReadonlyMap<string, string>;
}

export interface AuditTarget {
  kind: 'upstream' | 'principal';
  /** Resolved display name, or the raw id when the entity is unknown. */
  name: string;
  id: string | null;
  resolved: boolean;
}

const UUID = /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/gi;

function lookup(
  kind: AuditTarget['kind'],
  id: string,
  maps: NameMaps,
): AuditTarget {
  const name = (kind === 'upstream' ? maps.upstreams : maps.principals).get(id);
  return { kind, name: name ?? id, id, resolved: name != null };
}

/**
 * Some entries record the upstream by name rather than id. The upstream map
 * holds both `id -> name` and an identity `name -> name`, so the id is the key
 * that maps to the name without being the name itself. With no such key the
 * id stays null so the target renders as text instead of a broken link.
 */
function upstreamByRecordedValue(
  recorded: string,
  maps: NameMaps,
): AuditTarget {
  const mapped = maps.upstreams.get(recorded);
  if (mapped != null && mapped !== recorded) {
    return { kind: 'upstream', name: mapped, id: recorded, resolved: true };
  }
  let id: string | null = null;
  for (const [key, value] of maps.upstreams) {
    if (value === recorded && key !== recorded) {
      id = key;
      break;
    }
  }
  return { kind: 'upstream', name: recorded, id, resolved: true };
}

/** The entity an entry acted on, named where the lists know the id. */
export function auditTarget(
  entry: AuditEntryLike,
  parsed: ParsedAction,
  maps: NameMaps,
): AuditTarget | null {
  const param = new Map(parsed.params);
  const id = param.get('id');
  if (id && parsed.name.startsWith('upstream_')) {
    const target = lookup('upstream', id, maps);
    const recordedName = param.get('name');
    return !target.resolved && recordedName
      ? { ...target, name: recordedName, resolved: true }
      : target;
  }
  if (id && parsed.name.startsWith('principal_')) {
    return lookup('principal', id, maps);
  }
  const principalParam = param.get('principal');
  if (principalParam) return lookup('principal', principalParam, maps);
  if (entry.principal_id) return lookup('principal', entry.principal_id, maps);
  // Admin entries without an upstream target store the placeholder "admin".
  if (entry.upstream && entry.upstream !== 'admin') {
    return upstreamByRecordedValue(entry.upstream, maps);
  }
  return null;
}

/** Replaces id path segments with entity names the lists know. */
export function readableRoute(route: string, maps: NameMaps): string {
  return route.replace(
    UUID,
    (id) => maps.upstreams.get(id) ?? maps.principals.get(id) ?? id,
  );
}

/** Replaces ids inside a parameter value with known entity names. */
export function readableValue(value: string, maps: NameMaps): string {
  return value.replace(UUID, (id) => {
    const name = maps.upstreams.get(id) ?? maps.principals.get(id);
    return name ? `${name} (${id.slice(0, 8)})` : id;
  });
}

const CHANGED_NOUN: Record<string, [string, string]> = {
  fields: ['field', 'fields'],
  slots: ['slot', 'slots'],
};

/** One recorded fact for the list's Details column. */
export interface AuditDetailPart {
  label: string;
  value: string;
}

/**
 * The entry's recorded details in list form: changed field names first
 * ("3 fields: name, enabled, weight"), then the other parameters and payload
 * values. Values that only restate the target (its id or name) are left out;
 * the target has its own column.
 */
export function auditDetailParts(
  entry: AuditEntryLike,
  parsed: ParsedAction,
  maps: NameMaps,
  target: AuditTarget | null,
): AuditDetailPart[] {
  // The target's own id or name adds nothing next to the Target column.
  const targetValues = new Set<unknown>(target ? [target.id, target.name] : []);
  const parts: AuditDetailPart[] = [];
  for (const [key, value] of parsed.params) {
    const noun = CHANGED_NOUN[key];
    if (!noun) continue;
    const names = value.split(',').filter(Boolean);
    parts.push({
      label: `${names.length} ${names.length === 1 ? noun[0] : noun[1]}`,
      value: names.join(', '),
    });
  }
  for (const [key, value] of parsed.params) {
    if (CHANGED_NOUN[key] || targetValues.has(value)) continue;
    parts.push({
      label: key.replaceAll('_', ' '),
      value: readableValue(value, maps),
    });
  }
  for (const [key, value] of Object.entries(entry.payload ?? {})) {
    if (value == null || targetValues.has(value)) continue;
    // An integer past 2^53 lost its digits in JSON parsing (an unbounded
    // `until` is u64::MAX); showing the rounded number would misstate it.
    if (typeof value === 'number' && value > Number.MAX_SAFE_INTEGER) continue;
    parts.push({
      label: key.replaceAll('_', ' '),
      value:
        typeof value === 'string'
          ? readableValue(value, maps)
          : typeof value === 'object'
            ? JSON.stringify(value)
            : String(value),
    });
  }
  return parts;
}

export function auditActorLabel(entry: AuditEntryLike): string {
  return entry.actor_email || entry.actor || '—';
}

/** Healthy by omission: only failures carry a tone; successes stay muted. */
export function statusTextClass(status: number): string {
  if (status >= 500) return 'text-danger-text';
  if (status >= 400) return 'text-warn-text';
  return 'text-text-muted';
}

const RAW_DROP: Record<string, true> = {
  model: true,
  input_tokens: true,
  output_tokens: true,
  duration_ms: true,
  body_bytes: true,
  cost_usd_micros: true,
  cache_creation_input_tokens: true,
  cache_read_input_tokens: true,
  agent_label: true,
  api_key_id: true,
};

/** The stored entry without proxy-only columns that are always empty here. */
export function cleanAuditPayload(
  entry: AuditEntryLike,
): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(entry)) {
    if (RAW_DROP[k]) continue;
    if (v == null) continue;
    if (typeof v === 'number' && v === 0 && k !== 'status') continue;
    out[k] = v;
  }
  return out;
}
