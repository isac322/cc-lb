import { expect, test, type Page, type Route } from '@playwright/test';
import fs from 'node:fs';

const fixture = JSON.parse(
  fs.readFileSync('/tmp/cclb-kafix-capture/fixtures.json', 'utf8'),
) as {
  fixedNow: number;
  principalId: string;
  principal: Record<string, unknown>;
  upstream: Record<string, unknown>;
};

const SIDE = process.env.SIDE === 'before' ? 'before' : 'after';
const OUT = `/tmp/cclb-kafix-capture/out/${SIDE}`;
const BASE = fixture.fixedNow - 5 * 60 * 60 * 1000;
const id = (name: string) => `${fixture.principalId}/thread:${name}`;
const detailRequests: string[] = [];

type State =
  | 'renewed'
  | 'scheduled'
  | 'capped'
  | 'expired'
  | 'cancelled'
  | 'failed'
  | 'not_tracked';
type Evidence = 'observed' | 'none' | 'pending' | 'unknown';
type Ending =
  | 'followup_hit'
  | 'scheduled_waiting'
  | 'capped_no_followup'
  | 'ttl_expired_no_followup'
  | 'followup_unknown';

function pulses(offsets: number[], pause: number, spent: number, hit = true) {
  return offsets.map((offset, index) => ({
    pulse_number: index + 1,
    time_ms: pause + offset * 1000,
    offset_sec: offset,
    since_prev_renewal_sec: index === 0 ? null : offset - offsets[index - 1],
    spent_usd: spent,
    hit,
  }));
}

function gap(
  offsets: number[],
  pause: number,
  ending_kind: Ending,
  spent: number,
  avoided: number,
  cycle_id: string | null = null,
) {
  const endingAt = pause + (offsets.at(-1) ?? 0) * 1000;
  return {
    cycle_id,
    pause_at_ms: pause,
    cached_tokens: 604506,
    pulses: pulses(offsets, pause, spent / Math.max(offsets.length, 1)),
    ending_kind,
    ending_at_ms: endingAt,
    total_duration_sec: Math.round((endingAt - pause) / 1000),
    since_last_renewal_sec: 0,
    spent_usd: spent,
    avoided_usd: avoided,
    net_pnl_usd: Number((avoided - spent).toFixed(6)),
  };
}

function row(
  name: string,
  state: State,
  reason: string,
  attempts: number,
  spent: number,
  avoided: number,
  evidence: Evidence = 'observed',
  error: string | null = null,
) {
  return {
    id: id(name),
    last_message_at_ms: BASE + 3_600_000,
    relative_time: '1h ago',
    state,
    is_active: state === 'renewed' || state === 'scheduled',
    ttl: '5m' as const,
    attempts,
    max_attempts: 12,
    reason,
    display_reason: reason,
    generation: 1,
    upstream: 'demo-upstream',
    error,
    net_pnl: Number((avoided - spent).toFixed(6)),
    total_spent: spent,
    total_avoided: avoided,
    evidence_status: evidence,
    session_key_hash: `hash-${name}`,
    retained_renewals: attempts,
    group_key: reason.includes('execute_sql') ? 'tool:mcp__plugin_supabase_supabase__execute_sql' : 'tool:Bash',
    group_label: reason.includes('execute_sql') ? 'mcp__plugin_supabase_supabase__execute_sql' : 'Bash',
    group_kind: 'tool' as const,
  };
}

const bash = 'agent-in-turn (tool_use: `Bash`) — first renewal in 4m 30s';
const sql =
  'agent-in-turn (tool_use: `mcp__plugin_supabase_supabase__execute_sql`) — first renewal in 4m 30s';
const rows = [
  row('pending-2b7d9f01', 'renewed', bash, 3, 0.32, 0, 'pending'),
  row('unknown-01a14228', 'capped', bash, 12, 1.088085, 0, 'unknown'),
  row('cancelled-3c5cd0f1', 'cancelled', bash, 3, 0.395, 9.875),
  row('failed-01a1c484', 'failed', sql, 1, 0.12, 0, 'observed', 'renewal timed out after 30s'),
  row('capped-5a1c0e7b', 'capped', bash, 12, 2.314164, 0),
  row('sql-6f4b0c33', 'renewed', sql, 1, 0.12, 4.6089),
];

const pause = BASE;

const details = new Map<string, unknown>(
  rows.map((r) => {
    const offsets = Array.from({ length: r.attempts }, (_, i) => (i + 1) * 270);
    const ending: Ending =
      r.evidence_status === 'pending'
        ? 'scheduled_waiting'
        : r.evidence_status === 'unknown'
          ? 'followup_unknown'
          : r.total_avoided > 0
            ? 'followup_hit'
            : r.state === 'capped'
              ? 'capped_no_followup'
              : 'ttl_expired_no_followup';
    return [
      r.id,
      {
        ...r,
        renewal_tokens: 604506,
        total_renewals_current_cycle: r.attempts,
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
          renewal_count: r.attempts,
          ttl: r.ttl,
          status: r.is_active ? 'active' : 'terminal',
          enqueue_state: 'pending',
        },
        gaps: [gap(offsets, pause, ending, r.total_spent, r.total_avoided, 'cycle')],
      },
    ];
  }),
);

const unlistedSpent = 69.03;
const totalSpent = rows.reduce((a, r) => a + r.total_spent, 0) + unlistedSpent;
const totalAvoided = rows.reduce((a, r) => a + r.total_avoided, 0);
const summary: Record<string, unknown> = {
  renewing_now: 1,
  sessions_last_5m: 2,
  retained_successful_hits: 32,
  cost_saved: Number((totalAvoided - totalSpent).toFixed(6)),
  total_spent: Number(totalSpent.toFixed(6)),
  total_avoided: Number(totalAvoided.toFixed(6)),
  unlisted_sessions: 1595,
  unlisted_renewals: 12943,
  unlisted_spent: unlistedSpent,
  unlisted_net: -unlistedSpent,
  evidence_status: 'pending',
  unknown_cycles: 3,
  retained_from_ms: BASE,
};

const groups = (() => {
  const by = new Map<string, { key: string; label: string; kind: 'tool'; sessions: number; net_pnl: number; total_spent: number; total_avoided: number }>();
  for (const r of rows) {
    const g = by.get(r.group_key) ?? { key: r.group_key, label: r.group_label, kind: 'tool' as const, sessions: 0, net_pnl: 0, total_spent: 0, total_avoided: 0 };
    g.sessions += 1; g.net_pnl = Number((g.net_pnl + r.net_pnl).toFixed(6)); g.total_spent += r.total_spent; g.total_avoided += r.total_avoided;
    by.set(r.group_key, g);
  }
  return [...by.values()];
})();
const listRequests: string[] = [];

async function json(route: Route, body: unknown, status = 200) {
  await route.fulfill({ status, contentType: 'application/json', body: JSON.stringify(body) });
}

async function installRoutes(page: Page) {
  await page.route('**/admin/**', async (route) => {
    const url = new URL(route.request().url());
    const path = url.pathname;
    if (path === '/admin/health') return json(route, { status: 'ok', version: 'fixture', git_sha: 'x', uptime_secs: 1 });
    if (path === '/admin/v1/auth/session')
      return json(route, { authority: 'fixture', subject: 'demo', kind: 'service', provider_id: 'p', email: null, display_name: 'Synthetic operator', expires_at_unix_secs: null, auth_mode: 'static_token' });
    if (path === '/admin/v1/principals') return json(route, { principals: [fixture.principal] });
    if (path === '/admin/v1/upstreams') return json(route, { upstreams: [fixture.upstream] });
    if (path === '/admin/v1/dashboard/usage')
      return json(route, { window_start_unix_secs: 1, window_end_unix_secs: 2, step: 'hour', group_by: 'principal', series: [], observed: true });
    if (path === '/admin/v1/events/recent') return json(route, { events: [], observed: true, count: 0, limit: 5 });
    if (/plugin-chain$/.test(path)) return json(route, { entries: [] });
    if (/router-terminal$/.test(path)) return json(route, { strategy: 'first-pick', revision: 7 });
    if (/\/keys$/.test(path)) return json(route, { keys: [] });
    if (path.includes('/cache-keepalive/detail')) {
      detailRequests.push(url.search);
      const found = details.get(url.searchParams.get('id') ?? '');
      return found ? json(route, found) : json(route, { error: 'unknown_cache_keepalive_entry' }, 404);
    }
    if (path.includes('/cache-keepalive')) {
      listRequests.push(url.search);
      if (url.searchParams.get('limit') === '0') return json(route, { summary, rows: [], next_cursor: null });
      const cursor = url.searchParams.get('cursor');
      // Two pages: proves the drawer loads every page before showing group totals.
      if (SIDE === 'before') {
        // Master requests status=all: 20 pages of hidden not_tracked decisions sit between the visible rows.
        const hidden = (n: number) => Array.from({ length: 50 }, (_, i) => ({ ...row(`decision-${n}-${i}`, 'not_tracked', 'user turn (stop_reason=end_turn)', 0, 0, 0, 'none'), id: `decision:${n}-${i}`, net_pnl: 0 }));
        if (!cursor) return json(route, { summary, rows: rows.slice(0, 3), next_cursor: 'p-1' });
        const n = Number(cursor.slice(2));
        if (n <= 20) return json(route, { summary, rows: hidden(n), next_cursor: `p-${n + 1}` });
        return json(route, { summary, rows: rows.slice(3), next_cursor: null });
      }
      if (!cursor) return json(route, { summary, groups, rows: rows.slice(0, 3), next_cursor: 'page-2' });
      return json(route, { summary: null, groups: null, rows: rows.slice(3), next_cursor: null });
    }
    if (path === '/admin/v1/plugins/registry') return json(route, { entries: [] });
    if (path === '/admin/v1/events/stream') return route.fulfill({ status: 200, contentType: 'text/event-stream', body: ': idle\n\n' });
    return json(route, {});
  });
}

async function openDrawer(page: Page) {
  await page.addInitScript(({ token }) => window.localStorage.setItem('cc-lb-admin-token', token), { token: 'synthetic-token' });
  await page.clock.install({ time: fixture.fixedNow });
  page.on('pageerror', (err) => console.log(`PAGEERROR ${err.message}`));
  await installRoutes(page);
  await page.goto('/principals?selectedId=demo-principal');
  await expect(page.getByTestId('cache-keepalive-card')).toBeVisible();
  await page.getByRole('button', { name: 'Sessions' }).last().click();
  await expect(page.getByRole('dialog', { name: /Cache keepalive sessions/i })).toBeVisible();
  await expect(page.locator('[data-session-id]').first()).toBeVisible();
}

test('capture keepalive consistency fix', async ({ page }) => {
  fs.mkdirSync(OUT, { recursive: true });
  await page.setViewportSize({ width: 1440, height: 1000 });
  await openDrawer(page);
  await page.waitForTimeout(1500);
  console.log(`LIST_REQUESTS_${SIDE}`, JSON.stringify(listRequests));
  if (SIDE === 'after') {
    await expect(page.getByTestId('cache-keepalive-deleted-sessions')).toBeVisible();
    await page.waitForTimeout(800);
    console.log('LIST_REQUESTS_AFTER_OPEN', JSON.stringify(listRequests));
    await page.screenshot({ path: `${OUT}/drawer-first-page.png` });
    const footer = page.getByRole('button', { name: /Load more sessions/i });
    await footer.scrollIntoViewIfNeeded();
    console.log('FOOTER_BOX', JSON.stringify(await footer.boundingBox()), await page.getByText(/of \d+ sessions shown/).textContent());
    await page.getByRole('button', { name: /Load more sessions/i }).click();
    await expect(page.locator('[data-session-id]')).toHaveCount(rows.length);
  }
  await page.waitForTimeout(500);
  await page.getByTestId('cache-keepalive-card').screenshot({ path: `${OUT}/card.png` });
  await page.getByRole('dialog', { name: /Cache keepalive sessions/i }).screenshot({ path: `${OUT}/drawer.png` });
  for (const r of rows.slice(1, 4)) {
    await page.locator(`[data-session-id="${r.id}"]`).click();
    await expect(page.getByTestId('session-detail-time-rail')).toBeVisible();
    await page.getByRole('dialog', { name: /Cache keepalive sessions/i }).screenshot({ path: `${OUT}/detail-${r.state}-${r.evidence_status}.png` });
  }
  // Horizon persists across close and reopen (B2).
  if (SIDE === 'after') {
    await page.getByRole('radio', { name: '7d' }).click().catch(async () => page.getByRole('button', { name: '7d' }).click());
    await page.keyboard.press('Escape');
    await page.getByRole('button', { name: 'Sessions' }).last().click();
    await expect(page.getByText('Showing Last 7d')).toBeVisible();
  }
  console.log('DETAIL_REQUESTS', JSON.stringify(detailRequests));

  await page.setViewportSize({ width: 390, height: 844 });
  await page.reload();
  await openDrawer(page);
  await page.waitForTimeout(800);
  await page.getByRole('dialog', { name: /Cache keepalive sessions/i }).screenshot({ path: `${OUT}/mobile-list.png` });
  await page.locator(`[data-session-id="${rows[2].id}"]`).click();
  await page.getByRole('dialog', { name: /Cache keepalive sessions/i }).screenshot({ path: `${OUT}/mobile-detail.png` });
});
