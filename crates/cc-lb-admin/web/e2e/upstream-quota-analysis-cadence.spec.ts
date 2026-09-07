import { expect, type Page, test } from '@playwright/test';

const UPSTREAM_ID = 'oauth-cadence';
const INITIAL_TIME = new Date('2026-06-18T00:00:01.000Z');
const RANGE_SECS = 7 * 24 * 60 * 60;

type RequestBounds = {
  sinceUnixSecs: number;
  untilUnixSecs: number;
};

function requestBounds(url: URL): RequestBounds {
  return {
    sinceUnixSecs: Number(url.searchParams.get('since_unix_secs')),
    untilUnixSecs: Number(url.searchParams.get('until_unix_secs')),
  };
}

async function installAppFixtures(page: Page) {
  const seriesRequests: RequestBounds[] = [];
  const analysisRequests: RequestBounds[] = [];
  let updatedAnalysis = false;

  await page.clock.install({ time: INITIAL_TIME });
  await page.addInitScript(() => {
    window.localStorage.setItem('cc-lb-admin-token', 'mock-token');
  });

  const upstream = {
    id: UPSTREAM_ID,
    name: 'Cadence OAuth',
    kind: 'anthropic_oauth',
    enabled: true,
    spec_revision: 1,
    base_url: 'https://api.anthropic.com',
    api_key_env: null,
    warmup_enabled: false,
    warmup_dialect_plugin: null,
    status: {
      last_apply_error: null,
      last_apply_at_unix_secs: null,
      last_warmup_at_unix_secs: null,
    },
  };

  await page.route('**/admin/**', async (route) => {
    const request = route.request();
    const url = new URL(request.url());
    const { pathname } = url;
    const method = request.method();
    const json = (status: number, body: unknown) =>
      route.fulfill({
        status,
        contentType: 'application/json',
        body: JSON.stringify(body),
      });

    if (pathname === '/admin/v1/upstreams' && method === 'GET') {
      return json(200, { upstreams: [upstream] });
    }
    if (pathname === '/admin/v1/principals' && method === 'GET') {
      return json(200, { principals: [] });
    }
    if (pathname === '/admin/v1/plugins/registry' && method === 'GET') {
      return json(200, { entries: [] });
    }
    if (pathname === `/admin/v1/upstreams/${UPSTREAM_ID}` && method === 'GET') {
      return route.fulfill({
        status: 200,
        contentType: 'application/json',
        headers: { etag: '"1"' },
        body: JSON.stringify(upstream),
      });
    }
    if (pathname === `/admin/v1/upstreams/${UPSTREAM_ID}/oauth/status`) {
      return json(200, {
        upstream_id: UPSTREAM_ID,
        kind: 'anthropic_oauth',
        has_credentials: true,
        status: 'active',
        expires_at_unix_secs: Math.floor(INITIAL_TIME.getTime() / 1000) + 3600,
        refresh_token_present: true,
        scopes: ['user:inference'],
      });
    }
    if (
      pathname ===
      `/admin/v1/upstreams/${UPSTREAM_ID}/subscription-metadata`
    ) {
      return json(200, {
        upstream_id: UPSTREAM_ID,
        subscription_metadata: null,
        organization_metadata: null,
      });
    }
    if (
      pathname === `/admin/v1/upstreams/${UPSTREAM_ID}/warmup` &&
      method === 'GET'
    ) {
      return json(200, {
        upstream_id: UPSTREAM_ID,
        last_attempt: null,
        recent_attempts: [],
        next_scheduled_at_unix_secs: null,
        recent_summary_7d: {
          success: 0,
          skipped: 0,
          transient_failure: 0,
          permanent_failure: 0,
        },
        dialect_plugin: null,
      });
    }
    if (pathname === '/admin/v1/status') {
      return json(200, {
        version: 'mock',
        git_sha: 'mock',
        uptime_secs: 1,
        build: { rust_version: 'mock', profile: 'debug', target: 'mock' },
        generation: 1,
        upstreams: [
          {
            id: UPSTREAM_ID,
            name: upstream.name,
            status: 'active',
            last_apply_at_unix_secs: null,
            last_apply_error: null,
          },
        ],
        principals: [],
        plugin_chain_summary: {
          principal_count_with_chain: 0,
          total_entries: 0,
        },
        killswitch: false,
        last_reload_status: null,
        restart_required_changes: [],
      });
    }
    if (pathname === '/admin/usage') {
      const untilUnixSecs = Math.floor(INITIAL_TIME.getTime() / 1000);
      return json(200, {
        range: url.searchParams.get('range') ?? '24h',
        step: url.searchParams.get('step') ?? 'hour',
        group_by: url.searchParams.get('group_by') ?? 'model',
        window_start_unix_secs: untilUnixSecs - 3600,
        window_end_unix_secs: untilUnixSecs,
        observed: true,
        series: [],
      });
    }
    if (pathname === '/admin/events/recent') {
      return json(200, { events: [], observed: true, count: 0, limit: 5 });
    }
    if (pathname === '/admin/v1/subscription-quotas/latest') {
      const nowUnixSecs = Math.floor(INITIAL_TIME.getTime() / 1000);
      return json(200, {
        now_unix_secs: nowUnixSecs,
        max_staleness_secs: 300,
        upstreams: [
          {
            upstream_id: UPSTREAM_ID,
            upstream_name: upstream.name,
            windows: [
              {
                window: '5h',
                state: 'fresh',
                source: 'api',
                utilization: 0.25,
                status: null,
                resets_at_unix_secs: nowUnixSecs + 3600,
                surpassed_threshold: false,
                representative_claim: null,
                disabled_reason: null,
                extra_usage_enabled: null,
                extra_usage_monthly_limit: null,
                extra_usage_used_credits: null,
                observed_at_unix_millis: nowUnixSecs * 1000,
                age_secs: 0,
              },
            ],
          },
        ],
      });
    }
    if (pathname === '/admin/v1/subscription-quotas/series') {
      const bounds = requestBounds(url);
      seriesRequests.push(bounds);
      return json(200, {
        since_unix_secs: bounds.sinceUnixSecs,
        until_unix_secs: bounds.untilUnixSecs,
        bucket_secs: Number(url.searchParams.get('bucket_secs')),
        source: 'merged',
        series: [
          {
            upstream_id: UPSTREAM_ID,
            upstream_name: upstream.name,
            window: '5h',
            buckets: [
              {
                bucket_start_unix_secs: bounds.sinceUnixSecs,
                utilization_last: 0.2,
              },
              {
                bucket_start_unix_secs: bounds.untilUnixSecs,
                utilization_last: 0.25,
              },
            ],
            markers: [],
          },
        ],
      });
    }
    if (pathname === '/admin/v1/subscription-quotas/analysis') {
      const bounds = requestBounds(url);
      analysisRequests.push(bounds);
      return json(200, {
        since_unix_secs: bounds.sinceUnixSecs,
        until_unix_secs: bounds.untilUnixSecs,
        now_unix_secs: bounds.untilUnixSecs,
        max_staleness_secs: 300,
        upstreams: [
          {
            upstream_id: UPSTREAM_ID,
            upstream_name: upstream.name,
            windows: [
              {
                window: '5h',
                current_utilization: 0.25,
                resets_at_unix_secs: bounds.untilUnixSecs + 3600,
                data_state: 'fresh',
                actual_account_burn: {
                  utilization_per_second: 0.0001,
                  utilization_per_hour: 0.36,
                  eta_to_limit_secs: 7500,
                  resets_before_limit: false,
                  confidence: 'high',
                  sample_count: 3,
                  reason: null,
                },
                proxy_projected_burn: {
                  proxy_tokens_per_second: 10,
                  proxy_tokens_per_hour: 36000,
                  effective_limit_tokens_estimate: 100000,
                  utilization_per_hour: 0.36,
                  eta_to_limit_secs: 7500,
                  resets_before_limit: false,
                  confidence: 'high',
                  sample_count: 3,
                  reason: null,
                },
                deficit: updatedAnalysis
                  ? {
                      projected_proxy_tokens_window: 101234,
                      effective_limit_tokens_estimate: 100000,
                      shortfall_tokens: 1234,
                      recommended_multiplier: 1.25,
                      confidence: 'high',
                    }
                  : null,
                caveats: updatedAnalysis ? ['Cadence QA caveat'] : [],
              },
            ],
          },
        ],
      });
    }
    return json(200, {});
  });

  return {
    analysisRequests,
    seriesRequests,
    showUpdatedAnalysis: () => {
      updatedAnalysis = true;
    },
  };
}

test.describe('Upstream quota analysis cadence', () => {
  test('keeps the key stable for 60 seconds and refetches exact bounds at 120 seconds', async ({
    page,
  }) => {
    const fixtures = await installAppFixtures(page);
    const pageErrors: string[] = [];
    page.on('pageerror', (error) => pageErrors.push(error.message));
    await page.setViewportSize({ width: 1280, height: 800 });
    await page.goto(`/upstreams?selectedId=${UPSTREAM_ID}`);

    await expect.poll(() => fixtures.seriesRequests.length).toBeGreaterThan(0);
    await expect.poll(() => fixtures.analysisRequests.length).toBeGreaterThan(0);
    await expect(page.getByTestId('quota-history-range-control')).toBeVisible();
    const quotaSection = page
      .getByRole('heading', { name: 'Subscription Quota' })
      .locator('xpath=ancestor::section');
    await expect(quotaSection.locator('.animate-pulse')).toHaveCount(0);
    await expect(quotaSection.getByText(/error/i)).toHaveCount(0);
    await expect(page.locator('.recharts-responsive-container')).toBeVisible();
    await expect(page.getByText('No data in range')).toHaveCount(0);

    const initialBounds = fixtures.seriesRequests[0];
    if (!initialBounds) {
      throw new Error('initial series request was not observed');
    }
    expect(initialBounds.untilUnixSecs - initialBounds.sinceUnixSecs).toBe(
      RANGE_SECS,
    );
    expect(initialBounds.untilUnixSecs % 120).not.toBe(0);
    expect(fixtures.analysisRequests[0]).toEqual(initialBounds);
    const initialAnalysisRequestCount = fixtures.analysisRequests.length;

    await page.clock.runFor(60_000);

    const sixtySecondBounds = {
      sinceUnixSecs: initialBounds.sinceUnixSecs + 60,
      untilUnixSecs: initialBounds.untilUnixSecs + 60,
    };
    await expect
      .poll(() =>
        fixtures.seriesRequests.some(
          (bounds) =>
            bounds.sinceUnixSecs === sixtySecondBounds.sinceUnixSecs &&
            bounds.untilUnixSecs === sixtySecondBounds.untilUnixSecs,
        ),
      )
      .toBe(true);
    expect(fixtures.analysisRequests).toHaveLength(initialAnalysisRequestCount);
    await expect(page.locator('.recharts-responsive-container')).toBeVisible();

    fixtures.showUpdatedAnalysis();
    await page.clock.runFor(60_000);

    const oneHundredTwentySecondBounds = {
      sinceUnixSecs: initialBounds.sinceUnixSecs + 120,
      untilUnixSecs: initialBounds.untilUnixSecs + 120,
    };
    expect(oneHundredTwentySecondBounds.untilUnixSecs % 120).toBe(
      initialBounds.untilUnixSecs % 120,
    );
    expect(oneHundredTwentySecondBounds.untilUnixSecs % 120).not.toBe(0);
    await expect
      .poll(() =>
        fixtures.analysisRequests.some(
          (bounds) =>
            bounds.sinceUnixSecs === oneHundredTwentySecondBounds.sinceUnixSecs &&
            bounds.untilUnixSecs === oneHundredTwentySecondBounds.untilUnixSecs,
        ),
      )
      .toBe(true);
    expect(fixtures.analysisRequests.at(-1)).toEqual(
      oneHundredTwentySecondBounds,
    );
    await expect(page.getByText('Quota deficit')).toBeVisible();
    await expect(page.getByText('1,234 tokens')).toBeVisible();
    await expect(page.getByText('Cadence QA caveat')).toBeVisible();
    await expect(page.locator('.recharts-responsive-container')).toBeVisible();
    await expect(page.getByText('No data in range')).toHaveCount(0);
    await expect(quotaSection.locator('.animate-pulse')).toHaveCount(0);
    await expect(quotaSection.getByText(/error/i)).toHaveCount(0);
    expect(pageErrors).toEqual([]);
  });
});
