import { expect, test, type Page, type Route } from '@playwright/test';
import fs from 'node:fs';

const fixture = JSON.parse(
  fs.readFileSync('/tmp/cclb-1210-capture/fixtures.json', 'utf8'),
) as {
  fixedNow: number;
  principalId: string;
  principal: Record<string, unknown>;
  upstream: Record<string, unknown>;
};

const SIDE = process.env.SIDE === 'before' ? 'before' : 'after';
const OUT = `/tmp/cclb-1210-capture/out/${SIDE}`;
const BASE = fixture.fixedNow - 5 * 60 * 60 * 1000;
const id = (name: string) => `${fixture.principalId}/thread:${name}`;

type Gap = {
  cycle_id: string | null;
  pause_at_ms: number;
  cached_tokens: number;
  pulses: Array<{
    pulse_number: number;
    time_ms: number;
    offset_sec: number;
    since_prev_renewal_sec: number | null;
    spent_usd: number | null;
    hit: boolean;
  }>;
  ending_kind:
    | 'followup_hit'
    | 'scheduled_waiting'
    | 'capped_no_followup'
    | 'ttl_expired_no_followup';
  ending_at_ms: number;
  total_duration_sec: number;
  since_last_renewal_sec: number | null;
  spent_usd: number;
  avoided_usd: number | null;
  net_pnl_usd: number | null;
};

type Row = {
  id: string;
  last_message_at_ms: number;
  relative_time: string;
  state: 'renewed' | 'scheduled' | 'capped' | 'expired' | 'not_tracked';
  is_active: boolean;
  ttl: '5m' | '1h' | '30m' | '12h' | null;
  attempts: number | null;
  max_attempts: number;
  reason: string;
  display_reason?: string | null;
  generation: number;
  upstream: string | null;
  error: string | null;
  net_pnl: number | null;
  total_spent: number;
  total_avoided: number | null;
  evidence_status: 'observed' | 'none' | 'pending' | 'unknown';
  session_key_hash: string;
  retained_renewals: number;
};

type Detail = Row & {
  renewal_tokens: number;
  total_renewals_current_cycle: number;
  is_last_pending: boolean;
  turns: unknown[];
  config_snapshot: {
    lead_5m: number;
    lead_1h: number;
    max_renewals: number;
    max_duration: number;
    snapshot_bytes: number;
  } | null;
  raw_record: {
    session_key_hash: string;
    principal_id: string;
    upstream_id: string | null;
    generation: number;
    renewal_count: number;
    ttl: Row['ttl'];
    status: string;
    enqueue_state?: string | null;
  };
  gaps: Gap[];
};

function pulses(offsets: number[], pause: number, spent: number): Gap['pulses'] {
  return offsets.map((offset, index) => ({
    pulse_number: index + 1,
    time_ms: pause + offset * 1000,
    offset_sec: offset,
    since_prev_renewal_sec: index === 0 ? null : offset - offsets[index - 1],
    spent_usd: spent,
    hit: true,
  }));
}

function gap(
  offsets: number[],
  pause: number,
  ending_kind: Gap['ending_kind'],
  spent: number,
  net: number | null,
  cycle_id: string | null = null,
): Gap {
  const ps = pulses(offsets, pause, spent / Math.max(offsets.length, 1));
  const endingAt = pause + (offsets.at(-1) ?? 0) * 1000;
  return {
    cycle_id,
    pause_at_ms: pause,
    cached_tokens: 604506,
    pulses: ps,
    ending_kind,
    ending_at_ms: endingAt,
    total_duration_sec: Math.round((endingAt - pause) / 1000),
    since_last_renewal_sec: 0,
    spent_usd: spent,
    avoided_usd: net != null && net > 0 ? net + spent : 0,
    net_pnl_usd: net,
  };
}

function row(
  name: string,
  state: Row['state'],
  reason: string,
  attempts: number,
  net: number | null,
  spent: number,
  evidence: Row['evidence_status'] = 'observed',
): Row {
  const sessionId = id(name);
  const last = BASE + 3_600_000;
  return {
    id: sessionId,
    last_message_at_ms: last,
    relative_time: '1h ago',
    state,
    is_active: false,
    ttl: '5m',
    attempts,
    max_attempts: 12,
    reason,
    display_reason: null,
    generation: 1,
    upstream: 'demo-upstream',
    error: null,
    net_pnl: net,
    total_spent: spent,
    total_avoided: net != null && net > 0 ? net + spent : 0,
    evidence_status: evidence,
    session_key_hash: `hash-${name}`,
    retained_renewals: attempts,
  };
}

const capRow = row(
  'capped-5a1c0e7b',
  'capped',
  'agent-in-turn',
  12,
  -2.314164,
  2.314164,
);
const pendingNet = SIDE === 'after' ? -1.088085 : null;
const pendingGapNet = SIDE === 'after' ? -1.088085 : null;
const pendingRow = row(
  'pending-2b7d9f01',
  'capped',
  'agent-in-turn',
  8,
  pendingNet,
  1.088085,
  'pending',
);
const legacyRow = row(
  'legacy-3c9e1a22',
  'expired',
  'agent-in-turn',
  24,
  -2.4,
  2.4,
);
const bashRow = row(
  'bash-4d2f0a11',
  'renewed',
  'agent-in-turn (tool_use: `Bash`) — first renewal in 4m 30s',
  1,
  1.25,
  0.12,
);
const writeRow = row(
  'write-5e3a0b22',
  'renewed',
  'agent-in-turn (tool_use: `Write`) — first renewal in 4m 30s',
  1,
  2.1,
  0.12,
);
const sqlRow = row(
  'sql-6f4b0c33',
  'renewed',
  'agent-in-turn (tool_use: `mcp__plugin_supabase_supabase__execute_sql`) — first renewal in 4m 30s',
  1,
  4.4889,
  0.12,
);
const agentRow = row(
  'agent-7a5c0d44',
  'renewed',
  'agent-in-turn',
  1,
  0.25,
  0.12,
);

const capPause = BASE;
const pendingPause = BASE + 5_000_000;
const legacyPause = BASE + 10_000_000;
const capGap = gap(
  Array.from({ length: 12 }, (_, i) => (i + 1) * 270),
  capPause,
  'capped_no_followup',
  2.314164,
  -2.314164,
  'cap-cycle',
);
const pendingGap = {
  ...gap(
    Array.from({ length: 8 }, (_, i) => (i + 1) * 270),
    pendingPause,
    'ttl_expired_no_followup',
    1.088085,
    pendingGapNet,
    'pending-cycle',
  ),
  ending_at_ms: pendingPause + 2160_000,
};

const legacyOffsets = [
  270, 540, 810, 1080, 1350, 1620, 1890, 2160, 2430, 2700, 2970, 3240,
  4200, 4470, 4740, 5010, 5280, 5550, 6750, 7020, 7290, 7560, 7830, 8100,
];
const legacyAllGap = gap(
  legacyOffsets,
  legacyPause,
  'ttl_expired_no_followup',
  2.4,
  -2.4,
  null,
);
const legacyAfterGaps = [
  gap(legacyOffsets.slice(0, 12), legacyPause, 'capped_no_followup', 1.2, -1.2),
  // Backend pause_at_ms = first pulse - lead window (270s for a 5m TTL).
  gap(legacyOffsets.slice(12, 18).map((n) => n - 4200 + 270), legacyPause + (4200 - 270) * 1000, 'ttl_expired_no_followup', 0.6, -0.6),
  gap(legacyOffsets.slice(18).map((n) => n - 6750 + 270), legacyPause + (6750 - 270) * 1000, 'ttl_expired_no_followup', 0.6, -0.6),
];

function detail(r: Row, gaps: Gap[]): Detail {
  return {
    ...r,
    renewal_tokens: 604506,
    total_renewals_current_cycle: r.attempts ?? 0,
    is_last_pending: r.evidence_status === 'pending',
    turns: [],
    config_snapshot: {
      lead_5m: 30,
      lead_1h: 300,
      max_renewals: 12,
      max_duration: 14400,
      snapshot_bytes: 8388608,
    },
    raw_record: {
      session_key_hash: r.session_key_hash,
      principal_id: fixture.principalId,
      upstream_id: 'demo-upstream',
      generation: 1,
      renewal_count: r.attempts ?? 0,
      ttl: r.ttl,
      status: r.state,
      enqueue_state: 'enqueued',
    },
    gaps,
  };
}

const rows: Row[] = [capRow, pendingRow, legacyRow, bashRow, writeRow, sqlRow, agentRow];
const details = new Map<string, Detail>([
  [capRow.id, detail(capRow, [capGap])],
  [pendingRow.id, detail(pendingRow, [pendingGap])],
  [legacyRow.id, detail(legacyRow, SIDE === 'after' ? legacyAfterGaps : [legacyAllGap])],
  [bashRow.id, detail(bashRow, [gap([270], BASE, 'followup_hit', 0.12, 1.25)])],
  [writeRow.id, detail(writeRow, [gap([270], BASE, 'followup_hit', 0.12, 2.1)])],
  [sqlRow.id, detail(sqlRow, [gap([270], BASE, 'followup_hit', 0.12, 4.4889)])],
  [agentRow.id, detail(agentRow, [gap([270], BASE, 'followup_hit', 0.12, 0.25)])],
]);

const summary = {
  renewing_now: 0,
  sessions_last_5m: 7,
  renewals_fired: 51,
  retained_successful_hits: 51,
  cost_saved: 4.4889,
  resolved_cost_saved: SIDE === 'after' ? 4.4889 : null,
  total_spent: 8.052249,
  total_avoided: 12.541149,
  resolved_total_avoided: SIDE === 'after' ? 12.541149 : null,
  evidence_status: 'pending',
  unknown_cycles: 0,
  retained_from_ms: BASE,
};

async function json(route: Route, body: unknown, status = 200) {
  await route.fulfill({
    status,
    contentType: 'application/json',
    body: JSON.stringify(body),
  });
}

async function installRoutes(page: Page) {
  await page.route('**/admin/**', async (route) => {
    const request = route.request();
    const url = new URL(request.url());
    const path = url.pathname;
    if (path === '/admin/health') {
      return json(route, { status: 'ok', version: 'fixture', git_sha: SIDE, uptime_secs: 1 });
    }
    if (path === '/admin/v1/auth/session') {
      return json(route, {
        authority: 'fixture',
        subject: 'demo-operator',
        kind: 'service',
        provider_id: 'fixture-provider',
        email: null,
        display_name: 'Synthetic operator',
        expires_at_unix_secs: null,
        auth_mode: 'static_token',
      });
    }
    if (path === '/admin/v1/principals') {
      return json(route, { principals: [fixture.principal] });
    }
    if (path === '/admin/v1/upstreams') {
      return json(route, { upstreams: [fixture.upstream] });
    }
    if (path === '/admin/v1/dashboard/usage') {
      return json(route, {
        window_start_unix_secs: Math.floor((fixture.fixedNow - 86_400_000) / 1000),
        window_end_unix_secs: Math.floor(fixture.fixedNow / 1000),
        step: 'hour',
        group_by: 'principal',
        series: [],
        observed: true,
      });
    }
    if (path === '/admin/v1/events/recent') {
      return json(route, { events: [], observed: true, count: 0, limit: 5 });
    }
    if (/\/admin\/v1\/principals\/[^/]+\/plugin-chain$/.test(path)) {
      return json(route, { entries: [] });
    }
    if (/\/admin\/v1\/principals\/[^/]+\/router-terminal$/.test(path)) {
      return json(route, { strategy: 'first-pick', revision: 7 });
    }
    if (/\/admin\/v1\/principals\/[^/]+\/keys$/.test(path)) {
      return json(route, { keys: [] });
    }
    if (path.includes('/cache-keepalive/detail')) {
      const sessionId = url.searchParams.get('id');
      const found = sessionId ? details.get(sessionId) : undefined;
      return found ? json(route, found) : json(route, { error: 'not_found' }, 404);
    }
    if (path.includes('/cache-keepalive')) {
      return json(route, { summary, rows: url.searchParams.get('limit') === '0' ? [] : rows, next_cursor: null });
    }
    if (path === '/admin/v1/plugins/registry') {
      return json(route, { entries: [] });
    }
    if (path === '/admin/v1/events/stream') {
      return route.fulfill({ status: 200, contentType: 'text/event-stream', body: ': idle\n\n' });
    }
    console.log(`UNHANDLED ${request.method()} ${path}${url.search}`);
    return json(route, {});
  });
}

async function openDrawer(page: Page) {
  await page.addInitScript(({ token, now }) => {
    window.localStorage.setItem('cc-lb-admin-token', token);
    const OriginalDate = Date;
    // Keep app-relative displays deterministic without changing timer behavior.
    // @ts-expect-error browser global override
    window.Date = class extends OriginalDate {
      constructor(...args: ConstructorParameters<typeof Date>) {
        if (args.length === 0) super(now);
        else super(...args);
      }
      static now() { return now; }
    } as DateConstructor;
  }, { token: 'synthetic-token', now: fixture.fixedNow });
  await page.clock.install({ time: fixture.fixedNow });
  page.on('pageerror', (err) => console.log(`PAGEERROR ${err.message}\n${err.stack ?? ''}`));
  await installRoutes(page);
  await page.goto('/principals?selectedId=demo-principal');
  await expect(page.getByTestId('principal-detail-pane')).toBeVisible();
  await expect(page.getByTestId('cache-keepalive-card')).toBeVisible();
  await page.getByRole('button', { name: 'Sessions' }).last().evaluate((el) => {
    (el as HTMLButtonElement).click();
  });
  await expect(page.getByRole('dialog', { name: /Cache keepalive sessions/i })).toBeVisible();
  await expect(page.getByTestId('cache-keepalive-session-list-region')).toBeVisible();
  await expect(page.locator('[data-session-id]').first()).toBeVisible();
}

async function bodyClip(page: Page) {
  const body = page.getByTestId('cache-keepalive-sessions-body');
  await expect(body).toBeVisible();
  const box = await body.boundingBox();
  if (!box) throw new Error('drawer body has no bounding box');
  return { x: box.x, y: box.y, width: box.width, height: box.height };
}

async function selectSession(page: Page, sessionId: string) {
  await page.locator(`[data-session-id="${sessionId}"]`).click();
  await expect(page.getByTestId('session-detail-time-rail')).toBeVisible();
}

test(`capture ${SIDE} cache keepalive evidence`, async ({ page }) => {
  fs.mkdirSync(OUT, { recursive: true });
  await openDrawer(page);

  const list = page.getByTestId('cache-keepalive-session-list-region');
  await list.evaluate((el) => {
    const node = el as HTMLElement;
    node.style.maxHeight = 'none';
    node.style.height = `${node.scrollHeight}px`;
    node.style.overflow = 'visible';
  });
  await list.screenshot({ path: `${OUT}/drawer-groups.png` });

  await selectSession(page, capRow.id);
  await page.screenshot({ path: `${OUT}/rail-capped.png`, clip: await bodyClip(page) });

  await selectSession(page, pendingRow.id);
  await page.screenshot({ path: `${OUT}/rail-pending-spend.png`, clip: await bodyClip(page) });

  await selectSession(page, legacyRow.id);
  await page.screenshot({ path: `${OUT}/rail-legacy-split.png`, clip: await bodyClip(page) });

  await page.setViewportSize({ width: 390, height: 844 });
  await page.reload();
  await openDrawer(page);
  const mobileList = page.getByTestId('cache-keepalive-session-list-region');
  await mobileList.screenshot({ path: `${OUT}/drawer-groups-mobile.png` });
});
