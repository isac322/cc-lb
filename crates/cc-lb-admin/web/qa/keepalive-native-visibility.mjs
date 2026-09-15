#!/usr/bin/env node

import { chromium } from '@playwright/test';
import { createHash, randomUUID } from 'node:crypto';
import {
  closeSync,
  mkdirSync,
  openSync,
  readFileSync,
  statSync,
  writeFileSync,
} from 'node:fs';
import path from 'node:path';
import { performance } from 'node:perf_hooks';

const AUTH_TOKEN_KEY = 'cc-lb-admin-token';
const CASE_ID = 'PERF-06';
const DEFAULT_VISIBLE_MS = 30_000;
const DEFAULT_HIDDEN_MS = 600_000;
const MIN_VISIBLE_MS = 30_000;
const MIN_HIDDEN_MS = 600_000;
const VISIBILITY_TIMEOUT_MS = 10_000;
const READY_TIMEOUT_MS = 20_000;
const IN_FLIGHT_TIMEOUT_MS = 30_000;
const TARGET_DEFINITIONS = [
  { dataset: 'sqlite:baseline', engine: 'sqlite', phase: 'baseline' },
  { dataset: 'sqlite:candidate', engine: 'sqlite', phase: 'candidate' },
  { dataset: 'postgres:baseline', engine: 'postgres', phase: 'baseline' },
  { dataset: 'postgres:candidate', engine: 'postgres', phase: 'candidate' },
];
const KNOWN_OPTIONS = new Set([
  'manifest',
  'scratch-dir',
  'token-file',
  'cdp-endpoint',
  'output',
  'visible-ms',
  'hidden-ms',
]);

class NativeProofError extends Error {
  constructor(message, details = null) {
    super(message);
    this.name = 'NativeProofError';
    this.details = details;
  }
}

class NativeProofBlockedError extends NativeProofError {
  constructor(message, details = null) {
    super(message, details);
    this.name = 'NativeProofBlockedError';
  }
}

function usage() {
  return `Usage:
  node qa/keepalive-native-visibility.mjs \\
    --manifest <fixture-manifest.json> \\
    --scratch-dir <scratch-directory> \\
    --token-file <mode-0600-admin-token-file> \\
    --cdp-endpoint <loopback-http-origin>

Required options may instead use KEEPALIVE_MANIFEST, KEEPALIVE_SCRATCH_DIR,
KEEPALIVE_ADMIN_TOKEN_FILE, and KEEPALIVE_CDP_ENDPOINT. Optional
KEEPALIVE_NATIVE_VISIBLE_MS and KEEPALIVE_NATIVE_HIDDEN_MS values cannot be
lower than 30000 and 600000. The harness connects to an already-running
Chromium instance and never starts or closes the browser.`;
}

function parseArgs(argv) {
  const parsed = new Map();
  for (let index = 0; index < argv.length; index += 1) {
    const argument = argv[index];
    if (argument === '--help' || argument === '-h') {
      process.stdout.write(`${usage()}\n`);
      process.exit(0);
    }
    if (!argument.startsWith('--')) {
      throw new NativeProofError(`unexpected positional argument: ${argument}`);
    }
    const equalsAt = argument.indexOf('=');
    const name = argument.slice(2, equalsAt === -1 ? undefined : equalsAt);
    if (!KNOWN_OPTIONS.has(name)) {
      throw new NativeProofError(`unknown option --${name}`);
    }
    if (parsed.has(name)) {
      throw new NativeProofError(`duplicate option --${name}`);
    }
    const value =
      equalsAt === -1 ? argv[(index += 1)] : argument.slice(equalsAt + 1);
    if (!value || value.startsWith('--')) {
      throw new NativeProofError(`--${name} requires a value`);
    }
    parsed.set(name, value);
  }
  return parsed;
}

function requiredOption(options, optionName, environmentName) {
  const value = options.get(optionName) ?? process.env[environmentName];
  if (!value?.trim()) {
    throw new NativeProofError(
      `--${optionName} or ${environmentName} is required`,
    );
  }
  return value.trim();
}

function optionalOption(options, optionName, environmentName) {
  return options.get(optionName) ?? process.env[environmentName]?.trim() ?? null;
}

function durationOption(options, optionName, environmentName, fallback, minimum) {
  const raw = optionalOption(options, optionName, environmentName);
  const value = raw === null ? fallback : Number(raw);
  if (!Number.isSafeInteger(value) || value < minimum) {
    throw new NativeProofError(
      `${environmentName}/--${optionName} must be an integer >= ${minimum}`,
    );
  }
  return value;
}

function loopbackOrigin(raw, label) {
  let url;
  try {
    url = new URL(raw);
  } catch (error) {
    throw new NativeProofError(`${label} is not a valid URL`, String(error));
  }
  if (!['http:', 'https:'].includes(url.protocol)) {
    throw new NativeProofError(`${label} must use http or https`);
  }
  if (!['127.0.0.1', 'localhost', '::1'].includes(url.hostname)) {
    throw new NativeProofError(`${label} must use a loopback host`);
  }
  if (
    url.username ||
    url.password ||
    url.pathname !== '/' ||
    url.search ||
    url.hash
  ) {
    throw new NativeProofError(
      `${label} must be a credential-free loopback origin`,
    );
  }
  return url.origin;
}

function sha256(value) {
  return createHash('sha256').update(value).digest('hex');
}

function canonical(value) {
  if (Array.isArray(value)) return `[${value.map(canonical).join(',')}]`;
  if (value !== null && typeof value === 'object') {
    return `{${Object.entries(value)
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([key, item]) => `${JSON.stringify(key)}:${canonical(item)}`)
      .join(',')}}`;
  }
  return JSON.stringify(value) ?? 'null';
}

function iso(epochMs = Date.now()) {
  return new Date(epochMs).toISOString();
}

function delay(milliseconds) {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

function readToken(tokenFile) {
  const mode = statSync(tokenFile).mode & 0o777;
  if ((mode & 0o077) !== 0) {
    throw new NativeProofError('admin token file must have mode 0600');
  }
  const token = readFileSync(tokenFile, 'utf8').trim();
  if (!token) throw new NativeProofError('admin token file is empty');
  return token;
}

function readManifest(manifestPath) {
  let manifest;
  try {
    manifest = JSON.parse(readFileSync(manifestPath, 'utf8'));
  } catch (error) {
    throw new NativeProofError('failed to read fixture manifest', String(error));
  }
  if (![3, 4].includes(manifest.fixture_version)) {
    throw new NativeProofError('fixture manifest version must be 3 or 4');
  }
  const requiredPrincipals = [
    'empty',
    'low',
    'high_ui',
    'disabled',
    'd100k',
    'st_scale',
  ];
  for (const key of requiredPrincipals) {
    if (typeof manifest.principals?.[key] !== 'string') {
      throw new NativeProofError(`manifest principals.${key} is required`);
    }
  }
  if (typeof manifest.principal_names?.low !== 'string') {
    throw new NativeProofError('manifest principal_names.low is required');
  }
  if (typeof manifest.low_ids?.active !== 'string') {
    throw new NativeProofError('manifest low_ids.active is required');
  }
  const targets = TARGET_DEFINITIONS.map((definition) => {
    const dataset = manifest.datasets?.[definition.dataset];
    if (!dataset || typeof dataset !== 'object') {
      throw new NativeProofError(
        `manifest datasets.${definition.dataset} is required`,
      );
    }
    if (dataset.engine !== definition.engine || dataset.phase !== definition.phase) {
      throw new NativeProofError(
        `manifest dataset ${definition.dataset} has mismatched engine/phase`,
      );
    }
    if (typeof dataset.server_url !== 'string') {
      throw new NativeProofError(
        `manifest dataset ${definition.dataset} has no server_url`,
      );
    }
    return {
      ...definition,
      origin: loopbackOrigin(
        dataset.server_url,
        `manifest datasets.${definition.dataset}.server_url`,
      ),
      runtime_profile:
        typeof dataset.runtime_profile === 'string'
          ? dataset.runtime_profile
          : null,
    };
  });
  if (new Set(targets.map((target) => target.origin)).size !== targets.length) {
    throw new NativeProofError('the four target server origins must be distinct');
  }
  return { manifest, targets };
}

function redactUrl(raw) {
  const url = new URL(raw);
  const segments = url.pathname.split('/');
  const principalsAt = segments.indexOf('principals');
  if (principalsAt >= 0 && segments[principalsAt + 1]) {
    segments[principalsAt + 1] = `<sha256:${sha256(
      decodeURIComponent(segments[principalsAt + 1]),
    )}>`;
  }
  const keepaliveAt = segments.indexOf('cache-keepalive');
  if (keepaliveAt >= 0 && segments[keepaliveAt + 1]) {
    segments[keepaliveAt + 1] = `<sha256:${sha256(
      decodeURIComponent(segments[keepaliveAt + 1]),
    )}>`;
  }
  url.pathname = segments.join('/');
  for (const key of ['cursor', 'selectedId']) {
    const value = url.searchParams.get(key);
    if (value !== null) {
      url.searchParams.set(key, `<sha256:${sha256(value)}>`);
    }
  }
  return url.toString();
}

function keepaliveKind(raw) {
  const url = new URL(raw);
  const prefix = '/admin/v1/principals/';
  if (
    !url.pathname.startsWith(prefix) ||
    !url.pathname.includes('/cache-keepalive')
  ) {
    return null;
  }
  const suffix = url.pathname.split('/cache-keepalive')[1];
  if (suffix && suffix !== '/') return 'detail';
  return url.searchParams.get('limit') === '0' ? 'summary' : 'list';
}

class RawLedger {
  constructor(runId) {
    this.runId = runId;
    this.records = [];
  }

  push(type, target, payload) {
    const record = {
      record_id: randomUUID(),
      run_id: this.runId,
      case_id: CASE_ID,
      record_type: type,
      target,
      recorded_at_utc: iso(),
      ...payload,
    };
    this.records.push(record);
    return record;
  }

  validateUniqueIds() {
    const ids = this.records.map((record) => record.record_id);
    if (new Set(ids).size !== ids.length) {
      throw new NativeProofError('raw JSONL record IDs are not unique');
    }
    return ids;
  }
}

class NetworkRecorder {
  constructor(page, target, ledger) {
    this.page = page;
    this.target = target;
    this.ledger = ledger;
    this.started = new Map();
    this.completed = [];
    this.captureTasks = new Set();
    this.captureFailures = [];
    this.timingUnsupported = [];
    this.onRequest = this.onRequest.bind(this);
    this.onResponse = this.onResponse.bind(this);
    this.onRequestFailed = this.onRequestFailed.bind(this);
    page.on('request', this.onRequest);
    page.on('response', this.onResponse);
    page.on('requestfailed', this.onRequestFailed);
  }

  onRequest(request) {
    const kind = keepaliveKind(request.url());
    if (!kind) return;
    this.started.set(request, {
      request_id: randomUUID(),
      kind,
      method: request.method(),
      started_epoch_ms: Date.now(),
      started_monotonic_ms: performance.now(),
      redacted_url: redactUrl(request.url()),
    });
  }

  onResponse(response) {
    const started = this.started.get(response.request());
    if (!started) return;
    const task = this.captureResponse(response, started)
      .catch((error) => {
        this.captureFailures.push(error);
      })
      .finally(() => {
        this.captureTasks.delete(task);
        this.started.delete(response.request());
      });
    this.captureTasks.add(task);
  }

  onRequestFailed(request) {
    const started = this.started.get(request);
    if (!started) return;
    const ended = Date.now();
    const record = this.ledger.push('network', this.target.dataset, {
      request_id: started.request_id,
      started_at_utc: iso(started.started_epoch_ms),
      ended_at_utc: iso(ended),
      request: {
        method: started.method,
        kind: started.kind,
        redacted_url: started.redacted_url,
      },
      response: {
        http_status: null,
        body_bytes: null,
        canonical_body_sha256: null,
        failure: request.failure()?.errorText ?? 'request failed',
        ttfb_ms: null,
        wall_ms: null,
      },
      _started_epoch_ms: started.started_epoch_ms,
      _ended_epoch_ms: ended,
    });
    this.completed.push(record);
    this.started.delete(request);
  }

  async captureResponse(response, started) {
    const body = Buffer.from(await response.body());
    const ended = Date.now();
    const timing = response.request().timing();
    const startTime = timing.startTime;
    const responseStart = timing.responseStart;
    const responseEnd = timing.responseEnd;
    if (
      !Number.isFinite(startTime) ||
      startTime <= 0 ||
      !Number.isFinite(responseStart) ||
      responseStart < 0 ||
      !Number.isFinite(responseEnd) ||
      responseEnd < responseStart
    ) {
      this.timingUnsupported.push({
        request_id: started.request_id,
        timing,
      });
    }
    let bodyValue;
    try {
      bodyValue = JSON.parse(body.toString('utf8'));
    } catch {
      bodyValue = {
        non_json_body_sha256: sha256(body),
        body_bytes: body.length,
      };
    }
    const record = this.ledger.push('network', this.target.dataset, {
      request_id: started.request_id,
      started_at_utc: iso(started.started_epoch_ms),
      ended_at_utc: iso(ended),
      request: {
        method: started.method,
        kind: started.kind,
        redacted_url: started.redacted_url,
      },
      response: {
        http_status: response.status(),
        body_bytes: body.length,
        canonical_body_sha256: sha256(canonical(bodyValue)),
        failure: null,
        ttfb_ms: responseStart,
        wall_ms: responseEnd,
      },
      _started_epoch_ms: started.started_epoch_ms,
      _ended_epoch_ms: ended,
    });
    this.completed.push(record);
  }

  countCompletedAfter(epochMs) {
    return this.completed.filter(
      (record) =>
        record._started_epoch_ms >= epochMs &&
        record.response.http_status !== null,
    ).length;
  }

  firstCompletedAfter(epochMs) {
    return this.completed.find(
      (record) =>
        record._started_epoch_ms >= epochMs &&
        record.response.http_status !== null,
    );
  }

  preHiddenInFlight(hiddenAt) {
    return [...this.started.values()].filter(
      (request) => request.started_epoch_ms <= hiddenAt,
    );
  }

  async flush() {
    await Promise.all([...this.captureTasks]);
    if (this.captureFailures.length > 0) {
      const failures = this.captureFailures.splice(0);
      throw new AggregateError(failures, 'native network evidence capture failed');
    }
  }

  detach() {
    this.page.off('request', this.onRequest);
    this.page.off('response', this.onResponse);
    this.page.off('requestfailed', this.onRequestFailed);
  }
}

async function installNativeProbe(page) {
  await page.addInitScript(() => {
    const descriptorSummary = (descriptor) =>
      descriptor
        ? {
            configurable: descriptor.configurable,
            enumerable: descriptor.enumerable,
            has_getter: typeof descriptor.get === 'function',
            has_setter: typeof descriptor.set === 'function',
            has_value: Object.hasOwn(descriptor, 'value'),
            writable: Object.hasOwn(descriptor, 'writable')
              ? descriptor.writable
              : null,
          }
        : null;
    const initialHidden = Object.getOwnPropertyDescriptor(
      Document.prototype,
      'hidden',
    );
    const initialVisibility = Object.getOwnPropertyDescriptor(
      Document.prototype,
      'visibilityState',
    );
    const events = [];
    const record = (source) => {
      events.push({
        source,
        state: document.visibilityState,
        hidden: document.hidden,
        at_utc: new Date().toISOString(),
        epoch_ms: Date.now(),
        performance_ms: performance.now(),
      });
    };
    document.addEventListener('visibilitychange', () => record('event'));
    globalThis.__keepaliveNativeVisibilityProbe = {
      initial: {
        document_own_hidden: Object.hasOwn(document, 'hidden'),
        document_own_visibility_state: Object.hasOwn(
          document,
          'visibilityState',
        ),
        hidden: descriptorSummary(initialHidden),
        visibility_state: descriptorSummary(initialVisibility),
      },
      descriptors: {
        hidden: initialHidden,
        visibilityState: initialVisibility,
      },
      events,
      record,
      validate() {
        const currentHidden = Object.getOwnPropertyDescriptor(
          Document.prototype,
          'hidden',
        );
        const currentVisibility = Object.getOwnPropertyDescriptor(
          Document.prototype,
          'visibilityState',
        );
        return {
          unchanged:
            !Object.hasOwn(document, 'hidden') &&
            !Object.hasOwn(document, 'visibilityState') &&
            currentHidden?.get === initialHidden?.get &&
            currentHidden?.set === initialHidden?.set &&
            currentHidden?.configurable === initialHidden?.configurable &&
            currentHidden?.enumerable === initialHidden?.enumerable &&
            currentVisibility?.get === initialVisibility?.get &&
            currentVisibility?.set === initialVisibility?.set &&
            currentVisibility?.configurable === initialVisibility?.configurable &&
            currentVisibility?.enumerable === initialVisibility?.enumerable,
          current: {
            document_own_hidden: Object.hasOwn(document, 'hidden'),
            document_own_visibility_state: Object.hasOwn(
              document,
              'visibilityState',
            ),
            hidden: descriptorSummary(currentHidden),
            visibility_state: descriptorSummary(currentVisibility),
          },
        };
      },
    };
    record('initial');
  });
}

async function readNativeProbe(page) {
  return page.evaluate(() => {
    const probe = globalThis.__keepaliveNativeVisibilityProbe;
    if (!probe) return null;
    return {
      state: document.visibilityState,
      hidden: document.hidden,
      initial: probe.initial,
      validation: probe.validate(),
      events: [...probe.events],
    };
  });
}

async function waitForNativeVisibility(page, expected, target) {
  const deadline = Date.now() + VISIBILITY_TIMEOUT_MS;
  while (Date.now() < deadline) {
    const probe = await readNativeProbe(page);
    if (!probe) {
      throw new NativeProofBlockedError(
        `${target}: native visibility probe did not initialize`,
      );
    }
    if (!probe.validation.unchanged) {
      throw new NativeProofError(
        `${target}: document visibility descriptors were modified`,
        probe.validation,
      );
    }
    if (
      probe.state === expected &&
      probe.hidden === (expected === 'hidden')
    ) {
      return { at: Date.now(), probe };
    }
    await delay(100);
  }
  throw new NativeProofError(
    `${target}: native visibility did not become ${expected}`,
  );
}

async function waitForNewResponse(recorder, afterEpochMs, target) {
  const deadline = Date.now() + READY_TIMEOUT_MS;
  while (Date.now() < deadline) {
    await recorder.flush();
    const record = recorder.firstCompletedAfter(afterEpochMs);
    if (record) {
      if (record.request.method !== 'GET' || record.response.http_status !== 200) {
        throw new NativeProofError(
          `${target}: restored ready request was not GET 200`,
          record,
        );
      }
      return record;
    }
    await delay(100);
  }
  throw new NativeProofError(
    `${target}: no new keepalive response after native restore`,
  );
}

async function waitForPreHiddenInFlight(targetState, hiddenAt) {
  const deadline = Date.now() + IN_FLIGHT_TIMEOUT_MS;
  while (Date.now() < deadline) {
    if (targetState.recorder.preHiddenInFlight(hiddenAt).length === 0) return;
    await delay(100);
  }
  throw new NativeProofError(
    `${targetState.target.dataset}: pre-hidden requests did not settle`,
    targetState.recorder.preHiddenInFlight(hiddenAt),
  );
}

async function observeAllTargetsHidden(targetStates, durationMs, ledger) {
  const startedAt = Date.now();
  let checks = 0;
  const sampleRecordIds = [];
  while (Date.now() - startedAt < durationMs) {
    const observations = await Promise.all(
      targetStates.map(async (state) => ({
        state,
        probe: await readNativeProbe(state.page),
      })),
    );
    checks += 1;
    for (const { state, probe } of observations) {
      if (!probe) {
        throw new NativeProofBlockedError(
          `${state.target.dataset}: native visibility probe disappeared`,
        );
      }
      if (!probe.validation.unchanged) {
        throw new NativeProofError(
          `${state.target.dataset}: document visibility descriptors changed while hidden`,
          probe.validation,
        );
      }
      if (probe.state !== 'hidden' || probe.hidden !== true) {
        throw new NativeProofError(
          `${state.target.dataset}: target did not remain natively hidden`,
          { state: probe.state, hidden: probe.hidden },
        );
      }
    }
    if (checks === 1 || checks % 30 === 0) {
      const record = ledger.push('native_visibility_sample', 'all', {
        phase: 'hidden-steady',
        checked_at_utc: iso(),
        states: Object.fromEntries(
          observations.map(({ state, probe }) => [
            state.target.dataset,
            {
              state: probe.state,
              hidden: probe.hidden,
              descriptors_unchanged: probe.validation.unchanged,
            },
          ]),
        ),
      });
      sampleRecordIds.push(record.record_id);
    }
    const remaining = durationMs - (Date.now() - startedAt);
    if (remaining > 0) await delay(Math.min(1_000, remaining));
  }
  return {
    started_at_ms: startedAt,
    ended_at_ms: Date.now(),
    checks,
    sample_record_ids: sampleRecordIds,
  };
}

async function sampleHeap(targetState, label, ledger) {
  let metrics;
  try {
    metrics = await targetState.cdp.send('Performance.getMetrics');
  } catch (error) {
    throw new NativeProofBlockedError(
      `${targetState.target.dataset}: CDP Performance.getMetrics unsupported`,
      String(error),
    );
  }
  const heap = metrics.metrics?.find(
    (metric) => metric.name === 'JSHeapUsedSize',
  )?.value;
  if (!Number.isFinite(heap)) {
    throw new NativeProofBlockedError(
      `${targetState.target.dataset}: CDP JSHeapUsedSize is unavailable`,
      metrics,
    );
  }
  const sample = {
    label,
    at_utc: iso(),
    bytes: heap,
  };
  targetState.heap_samples.push(sample);
  ledger.push('heap', targetState.target.dataset, sample);
  return sample;
}

async function waitForPaintFrames(page, target, ledger, label) {
  const frames = await page.evaluate(
    () =>
      new Promise((resolve) => {
        requestAnimationFrame((first) => {
          requestAnimationFrame((second) => {
            resolve({
              first_performance_ms: first,
              second_performance_ms: second,
              at_utc: new Date().toISOString(),
            });
          });
        });
      }),
  );
  ledger.push('paint_frames', target, { label, ...frames });
  return frames;
}

async function domReadySnapshot(targetState, label, ledger) {
  const snapshot = await targetState.page.evaluate(
    ({ activeId }) => {
      const drawer = document.querySelector(
        '[data-testid="cache-keepalive-sessions-drawer"]',
      );
      const detail = document.querySelector(
        '[data-testid="session-detail-content"]',
      );
      const rowIds = drawer
        ? [...drawer.querySelectorAll('li[data-key]')].map(
            (node) => node.getAttribute('data-key') ?? '',
          )
        : [];
      return {
        drawer_ready: drawer !== null,
        detail_ready: detail !== null,
        selected_row_present: rowIds.includes(activeId),
        visible_row_ids: rowIds,
        detail_heading: detail
          ?.querySelector('[role="heading"], h4')
          ?.textContent?.trim(),
      };
    },
    { activeId: targetState.activeId },
  );
  if (
    !snapshot.drawer_ready ||
    !snapshot.detail_ready ||
    !snapshot.selected_row_present
  ) {
    throw new NativeProofError(
      `${targetState.target.dataset}: DOM was not ready during ${label}`,
      snapshot,
    );
  }
  const evidence = {
    label,
    at_utc: iso(),
    drawer_ready: snapshot.drawer_ready,
    detail_ready: snapshot.detail_ready,
    selected_row_present: snapshot.selected_row_present,
    selected_row_id_sha256: sha256(targetState.activeId),
    visible_row_ids_sha256: sha256(canonical(snapshot.visible_row_ids)),
    visible_row_count: snapshot.visible_row_ids.length,
    detail_heading: snapshot.detail_heading ?? null,
  };
  targetState.dom_snapshots.push(evidence);
  ledger.push('dom_ready', targetState.target.dataset, evidence);
  return evidence;
}
async function restoreAdminToken(targetState) {
  if (!targetState.tokenInjected) return;
  await targetState.page.evaluate(
    ({ key, hadPreviousValue, previousValue }) => {
      if (hadPreviousValue) localStorage.setItem(key, previousValue);
      else localStorage.removeItem(key);
    },
    {
      key: AUTH_TOKEN_KEY,
      hadPreviousValue: targetState.hadPreviousAdminToken,
      previousValue: targetState.previousAdminToken,
    },
  );
  targetState.previousAdminToken = null;
  targetState.hadPreviousAdminToken = false;
  targetState.tokenInjected = false;
}


async function prepareTarget(context, target, manifest, token, ledger) {
  const page = await context.newPage();
  await installNativeProbe(page);
  const recorder = new NetworkRecorder(page, target, ledger);
  const state = {
    target,
    page,
    recorder,
    cdp: null,
    activeId: manifest.low_ids.active,
    heap_samples: [],
    dom_snapshots: [],
    paint_frames: [],
    previousAdminToken: null,
    hadPreviousAdminToken: false,
    tokenInjected: false,
    intervals: {},
    native: {},
  };
  try {
    const health = await page.goto(`${target.origin}/admin/health`, {
      waitUntil: 'domcontentloaded',
    });
    if (!health || health.status() !== 200) {
      throw new NativeProofError(
        `${target.dataset}: /admin/health did not return 200`,
        health?.status() ?? null,
      );
    }
    state.previousAdminToken = await page.evaluate(
      (key) => localStorage.getItem(key),
      AUTH_TOKEN_KEY,
    );
    state.hadPreviousAdminToken = state.previousAdminToken !== null;
    state.tokenInjected = true;
    await page.evaluate(
      ({ key, value }) => localStorage.setItem(key, value),
      { key: AUTH_TOKEN_KEY, value: token },
    );
    const principalUrl = `${target.origin}/principals?selectedId=${encodeURIComponent(
      manifest.principals.low,
    )}`;
    await page.goto(principalUrl, { waitUntil: 'domcontentloaded' });
    await page
      .getByRole('heading', { name: manifest.principal_names.low })
      .waitFor({ state: 'visible', timeout: READY_TIMEOUT_MS });
    const card = page.getByTestId('cache-keepalive-card');
    await card.waitFor({ state: 'visible', timeout: READY_TIMEOUT_MS });
    const listResponsePromise = page.waitForResponse((response) => {
      const url = new URL(response.url());
      return (
        response.request().method() === 'GET' &&
        url.pathname ===
          `/admin/v1/principals/${manifest.principals.low}/cache-keepalive` &&
        !url.searchParams.has('limit')
      );
    });
    await card.getByRole('button', { name: 'Sessions' }).click();
    const listResponse = await listResponsePromise;
    if (listResponse.status() !== 200) {
      throw new NativeProofError(
        `${target.dataset}: sessions GET returned ${listResponse.status()}`,
      );
    }
    const drawer = page.getByTestId('cache-keepalive-sessions-drawer');
    await drawer.waitFor({ state: 'visible', timeout: READY_TIMEOUT_MS });
    const encoded = encodeURIComponent(manifest.low_ids.active);
    const detailPath = `/admin/v1/principals/${manifest.principals.low}/cache-keepalive/${encoded}`;
    const detailResponsePromise = page.waitForResponse((response) => {
      const url = new URL(response.url());
      return (
        response.request().method() === 'GET' && url.pathname === detailPath
      );
    });
    await drawer
      .locator(`li[data-key=${JSON.stringify(manifest.low_ids.active)}]`)
      .getByRole('button')
      .click();
    const detailResponse = await detailResponsePromise;
    if (detailResponse.status() !== 200) {
      throw new NativeProofError(
        `${target.dataset}: detail GET returned ${detailResponse.status()}`,
      );
    }
    await page
      .getByTestId('session-detail-content')
      .waitFor({ state: 'visible', timeout: READY_TIMEOUT_MS });
    await recorder.flush();
    try {
      state.cdp = await context.newCDPSession(page);
      await state.cdp.send('Performance.enable');
    } catch (error) {
      throw new NativeProofBlockedError(
        `${target.dataset}: CDP Performance domain unsupported`,
        String(error),
      );
    }
    return state;
  } catch (error) {
    let restoreFailure = null;
    try {
      await restoreAdminToken(state);
    } catch (restoreError) {
      restoreFailure = restoreError;
    }
    recorder.detach();
    await page.close().catch(() => {});
    if (restoreFailure) {
      throw new NativeProofError(
        `${target.dataset}: setup failed and the prior admin token could not be restored`,
        {
          setup_error: error instanceof Error ? error.message : String(error),
          restore_error:
            restoreFailure instanceof Error
              ? restoreFailure.message
              : String(restoreFailure),
        },
      );
    }
    throw error;
  }
}

function networkRecords(ledger, target) {
  return ledger.records.filter(
    (record) => record.record_type === 'network' && record.target === target,
  );
}

function recordsStartingBetween(records, start, end) {
  return records.filter(
    (record) =>
      record._started_epoch_ms >= start && record._started_epoch_ms < end,
  );
}

function summarizeRecords(records) {
  const byKind = { summary: 0, list: 0, detail: 0 };
  const byStatus = {};
  for (const record of records) {
    byKind[record.request.kind] += 1;
    const status = String(record.response.http_status ?? 'failed');
    byStatus[status] = (byStatus[status] ?? 0) + 1;
  }
  return {
    count: records.length,
    by_kind: byKind,
    by_status: byStatus,
    record_ids: records.map((record) => record.record_id),
  };
}

function verifyNetwork(targetStates, ledger, hiddenSteadyStart, hiddenSteadyEnd) {
  const summaries = {};
  for (const state of targetStates) {
    const records = networkRecords(ledger, state.target.dataset);
    if (state.recorder.timingUnsupported.length > 0) {
      throw new NativeProofBlockedError(
        `${state.target.dataset}: browser request timing is unsupported`,
        state.recorder.timingUnsupported,
      );
    }
    const nonGetOrNon200 = records.filter(
      (record) =>
        record.request.method !== 'GET' || record.response.http_status !== 200,
    );
    if (nonGetOrNon200.length > 0) {
      throw new NativeProofError(
        `${state.target.dataset}: keepalive network contained non-GET or non-200 records`,
        nonGetOrNon200.map((record) => record.record_id),
      );
    }
    const before = recordsStartingBetween(
      records,
      state.intervals.visible_before.started_at_ms,
      state.intervals.visible_before.ended_at_ms,
    );
    const hidden = recordsStartingBetween(
      records,
      hiddenSteadyStart,
      hiddenSteadyEnd,
    );
    const after = recordsStartingBetween(
      records,
      state.intervals.visible_after.started_at_ms,
      state.intervals.visible_after.ended_at_ms,
    );
    const newWhileNativeHidden = records.filter(
      (record) =>
        record._started_epoch_ms > state.native.hidden_at_ms &&
        record._started_epoch_ms < state.native.restored_at_ms,
    );
    const allowedInitialInflight = records.filter(
      (record) =>
        record._started_epoch_ms <= state.native.hidden_at_ms &&
        record._ended_epoch_ms > state.native.hidden_at_ms,
    );
    if (hidden.length > 0 || newWhileNativeHidden.length > 0) {
      throw new NativeProofError(
        `${state.target.dataset}: new keepalive request started while natively hidden`,
        {
          hidden_steady_record_ids: hidden.map((record) => record.record_id),
          native_hidden_record_ids: newWhileNativeHidden.map(
            (record) => record.record_id,
          ),
        },
      );
    }
    if (before.length === 0 || after.length === 0) {
      throw new NativeProofError(
        `${state.target.dataset}: visible phase did not collect keepalive responses`,
      );
    }
    summaries[state.target.dataset] = {
      visible_before: summarizeRecords(before),
      hidden_steady: summarizeRecords(hidden),
      visible_after: summarizeRecords(after),
      hidden_grace: {
        allowed_initial_inflight: summarizeRecords(allowedInitialInflight),
        new_requests_after_hidden: summarizeRecords(newWhileNativeHidden),
      },
    };
  }
  return summaries;
}

function verifyCrossClientDom(targetStates) {
  const beforeKeys = targetStates.map((state) => {
    const snapshot = state.dom_snapshots.find(
      (item) => item.label === 'visible-before',
    );
    return canonical({
      drawer_ready: snapshot.drawer_ready,
      detail_ready: snapshot.detail_ready,
      selected_row_present: snapshot.selected_row_present,
      selected_row_id_sha256: snapshot.selected_row_id_sha256,
      visible_row_ids_sha256: snapshot.visible_row_ids_sha256,
      visible_row_count: snapshot.visible_row_count,
      detail_heading: snapshot.detail_heading,
    });
  });
  const afterKeys = targetStates.map((state) => {
    const snapshot = state.dom_snapshots.find(
      (item) => item.label === 'visible-after',
    );
    return canonical({
      drawer_ready: snapshot.drawer_ready,
      detail_ready: snapshot.detail_ready,
      selected_row_present: snapshot.selected_row_present,
      selected_row_id_sha256: snapshot.selected_row_id_sha256,
      visible_row_ids_sha256: snapshot.visible_row_ids_sha256,
      visible_row_count: snapshot.visible_row_count,
      detail_heading: snapshot.detail_heading,
    });
  });
  const beforeMatch = new Set(beforeKeys).size === 1;
  const afterMatch = new Set(afterKeys).size === 1;
  const restoredMatchesInitial = targetStates.every(
    (_, index) => beforeKeys[index] === afterKeys[index],
  );
  if (!beforeMatch || !afterMatch || !restoredMatchesInitial) {
    throw new NativeProofError('manual DOM readiness did not match across clients', {
      before_match: beforeMatch,
      after_match: afterMatch,
      restored_matches_initial: restoredMatchesInitial,
    });
  }
  return {
    before_match: beforeMatch,
    after_match: afterMatch,
    restored_matches_initial: restoredMatchesInitial,
    canonical_sha256: sha256(beforeKeys[0]),
  };
}

function stripInternalFields(value) {
  if (Array.isArray(value)) return value.map(stripInternalFields);
  if (value !== null && typeof value === 'object') {
    return Object.fromEntries(
      Object.entries(value)
        .filter(([key]) => !key.startsWith('_'))
        .map(([key, item]) => [key, stripInternalFields(item)]),
    );
  }
  return value;
}

function writeEvidence(outputPath, evidence) {
  mkdirSync(path.dirname(outputPath), { recursive: true });
  const descriptor = openSync(outputPath, 'wx', 0o600);
  try {
    writeFileSync(descriptor, `${JSON.stringify(evidence, null, 2)}\n`, 'utf8');
  } finally {
    closeSync(descriptor);
  }
}

async function run() {
  const options = parseArgs(process.argv.slice(2));
  const manifestPath = path.resolve(
    requiredOption(options, 'manifest', 'KEEPALIVE_MANIFEST'),
  );
  const scratchDir = path.resolve(
    requiredOption(options, 'scratch-dir', 'KEEPALIVE_SCRATCH_DIR'),
  );
  const tokenFile = path.resolve(
    requiredOption(options, 'token-file', 'KEEPALIVE_ADMIN_TOKEN_FILE'),
  );
  const cdpEndpoint = loopbackOrigin(
    requiredOption(options, 'cdp-endpoint', 'KEEPALIVE_CDP_ENDPOINT'),
    'CDP endpoint',
  );
  const visibleMs = durationOption(
    options,
    'visible-ms',
    'KEEPALIVE_NATIVE_VISIBLE_MS',
    DEFAULT_VISIBLE_MS,
    MIN_VISIBLE_MS,
  );
  const hiddenMs = durationOption(
    options,
    'hidden-ms',
    'KEEPALIVE_NATIVE_HIDDEN_MS',
    DEFAULT_HIDDEN_MS,
    MIN_HIDDEN_MS,
  );
  const runId = randomUUID();
  const outputPath = path.resolve(
    optionalOption(options, 'output', 'KEEPALIVE_NATIVE_OUTPUT') ??
      path.join(
        scratchDir,
        'evidence',
        `keepalive-native-visibility-${runId}.json`,
      ),
  );
  const token = readToken(tokenFile);
  const manifestBytes = readFileSync(manifestPath);
  const { manifest, targets } = readManifest(manifestPath);
  const ledger = new RawLedger(runId);
  const startedAt = Date.now();
  const createdPages = [];
  const targetStates = [];
  let controlPage = null;
  let hiddenSteadyStart = null;
  let hiddenSteadyEnd = null;
  let hiddenObservation = null;
  let browser = null;
  let playwrightConnectionClosed = false;
  let status = 'PASS';
  let failure = null;
  let phaseCounts = null;
  let crossClientDom = null;
  const cleanupFailures = [];
  let closedTabs = 0;

  try {
    browser = await chromium.connectOverCDP(cdpEndpoint, {
      noDefaults: true,
    });
    const contexts = browser.contexts();
    if (contexts.length === 0) {
      throw new NativeProofBlockedError(
        'CDP browser has no default context; an incognito context is not allowed',
      );
    }
    const context = contexts[0];
    for (const target of targets) {
      const state = await prepareTarget(context, target, manifest, token, ledger);
      targetStates.push(state);
      createdPages.push(state.page);
    }
    controlPage = await context.newPage();
    createdPages.push(controlPage);
    await controlPage.goto('about:blank');

    for (const state of targetStates) {
      await state.page.bringToFront();
      const visible = await waitForNativeVisibility(
        state.page,
        'visible',
        state.target.dataset,
      );
      state.intervals.visible_before = {
        started_at_ms: visible.at,
        started_at_utc: iso(visible.at),
      };
      const visibleEvent = [...visible.probe.events]
        .reverse()
        .find((event) => event.state === 'visible');
      if (!visibleEvent) {
        throw new NativeProofError(
          `${state.target.dataset}: no native visible event before collection`,
        );
      }
      state.native.visible_before_at_ms = visibleEvent.epoch_ms;
      state.native.visible_before_at_utc = visibleEvent.at_utc;
      ledger.push('native_visibility', state.target.dataset, {
        phase: 'visible-before',
        event: visibleEvent,
        observed_at_utc: iso(visible.at),
      });
      await sampleHeap(state, 'visible-before', ledger);
      const paint = await waitForPaintFrames(
        state.page,
        state.target.dataset,
        ledger,
        'visible-before',
      );
      state.paint_frames.push({ label: 'visible-before', ...paint });
      await domReadySnapshot(state, 'visible-before', ledger);
      await delay(visibleMs);
      state.intervals.visible_before.ended_at_ms = Date.now();
      state.intervals.visible_before.ended_at_utc = iso();
    }

    for (const state of targetStates) {
      state.native.pre_control_inflight_request_ids = [
        ...state.recorder.started.values(),
      ].map((request) => request.request_id);
    }
    await controlPage.bringToFront();
    const hiddenObservations = await Promise.all(
      targetStates.map((state) =>
        waitForNativeVisibility(
          state.page,
          'hidden',
          state.target.dataset,
        ),
      ),
    );
    for (let index = 0; index < targetStates.length; index += 1) {
      const state = targetStates[index];
      const observation = hiddenObservations[index];
      const hiddenEvent = [...observation.probe.events]
        .reverse()
        .find((event) => event.state === 'hidden');
      if (!hiddenEvent) {
        throw new NativeProofError(
          `${state.target.dataset}: no native hidden visibilitychange event`,
        );
      }
      state.native.hidden_at_ms = hiddenEvent.epoch_ms;
      state.native.hidden_at_utc = hiddenEvent.at_utc;
      state.native.hidden_observed_at_utc = iso(observation.at);
      ledger.push('native_visibility', state.target.dataset, {
        phase: 'hidden',
        event: hiddenEvent,
        observed_at_utc: iso(observation.at),
      });
      await waitForPreHiddenInFlight(state, hiddenEvent.epoch_ms);
    }

    hiddenObservation = await observeAllTargetsHidden(
      targetStates,
      hiddenMs,
      ledger,
    );
    hiddenSteadyStart = hiddenObservation.started_at_ms;
    hiddenSteadyEnd = hiddenObservation.ended_at_ms;
    ledger.push('phase_boundary', 'all', {
      phase: 'hidden-steady-end',
      at_utc: iso(hiddenSteadyEnd),
      duration_ms: hiddenSteadyEnd - hiddenSteadyStart,
      checks: hiddenObservation.checks,
      sample_record_ids: hiddenObservation.sample_record_ids,
      initial_inflight_policy:
        'only requests whose recorder start predates the native hidden event are allowed to finish',
    });
    if (hiddenSteadyEnd - hiddenSteadyStart < MIN_HIDDEN_MS) {
      throw new NativeProofError(
        'native hidden observation was shorter than 600000ms',
      );
    }

    for (const state of targetStates) {
      await sampleHeap(state, 'hidden-end', ledger);
      const restoreRequestedAt = Date.now();
      await state.page.bringToFront();
      const restored = await waitForNativeVisibility(
        state.page,
        'visible',
        state.target.dataset,
      );
      const restoredEvent = [...restored.probe.events]
        .reverse()
        .find(
          (event) =>
            event.state === 'visible' &&
            event.epoch_ms >= restoreRequestedAt,
        );
      if (!restoredEvent) {
        throw new NativeProofError(
          `${state.target.dataset}: no native restored visibilitychange event`,
        );
      }
      state.native.restored_at_ms = restoredEvent.epoch_ms;
      state.native.restored_at_utc = restoredEvent.at_utc;
      state.native.restored_observed_at_utc = iso(restored.at);
      state.intervals.visible_after = {
        started_at_ms: restoredEvent.epoch_ms,
        started_at_utc: restoredEvent.at_utc,
      };
      ledger.push('native_visibility', state.target.dataset, {
        phase: 'restored',
        event: restoredEvent,
        observed_at_utc: iso(restored.at),
      });
      const readyResponse = await waitForNewResponse(
        state.recorder,
        restoredEvent.epoch_ms,
        state.target.dataset,
      );
      state.native.restored_ready_record_id = readyResponse.record_id;
      const paint = await waitForPaintFrames(
        state.page,
        state.target.dataset,
        ledger,
        'visible-after',
      );
      state.paint_frames.push({ label: 'visible-after', ...paint });
      await domReadySnapshot(state, 'visible-after', ledger);
      await delay(visibleMs);
      state.intervals.visible_after.ended_at_ms = Date.now();
      state.intervals.visible_after.ended_at_utc = iso();
      await sampleHeap(state, 'visible-after', ledger);
    }

    await Promise.all(targetStates.map((state) => state.recorder.flush()));
    phaseCounts = verifyNetwork(
      targetStates,
      ledger,
      hiddenSteadyStart,
      hiddenSteadyEnd,
    );
    crossClientDom = verifyCrossClientDom(targetStates);
    for (const state of targetStates) {
      const probe = await readNativeProbe(state.page);
      if (!probe?.validation.unchanged) {
        throw new NativeProofError(
          `${state.target.dataset}: document visibility descriptors changed`,
          probe?.validation ?? null,
        );
      }
      state.native.descriptor_validation = probe.validation;
      state.native.events = probe.events;
      const hiddenEvent = probe.events.find(
        (event) =>
          event.state === 'hidden' &&
          event.epoch_ms === state.native.hidden_at_ms,
      );
      const restoredEvent = probe.events.find(
        (event) =>
          event.state === 'visible' &&
          event.epoch_ms === state.native.restored_at_ms,
      );
      if (!hiddenEvent || !restoredEvent) {
        throw new NativeProofError(
          `${state.target.dataset}: native event ledger is incomplete`,
        );
      }
    }
  } catch (error) {
    status = error instanceof NativeProofBlockedError ? 'BLOCKED' : 'FAIL';
    failure = {
      name: error instanceof Error ? error.name : 'Error',
      message: error instanceof Error ? error.message : String(error),
      details: error instanceof NativeProofError ? error.details : null,
    };
  } finally {
    await Promise.all(
      targetStates.map(async (state) => {
        try {
          await state.recorder.flush();
        } catch (error) {
          cleanupFailures.push({
            target: state.target.dataset,
            operation: 'network_capture_flush',
            name: error instanceof Error ? error.name : 'Error',
            message: error instanceof Error ? error.message : String(error),
          });
        }
        try {
          await restoreAdminToken(state);
        } catch (error) {
          cleanupFailures.push({
            target: state.target.dataset,
            operation: 'admin_token_restore',
            name: error instanceof Error ? error.name : 'Error',
            message: error instanceof Error ? error.message : String(error),
          });
        }
        state.recorder.detach();
        if (state.cdp) await state.cdp.detach().catch(() => {});
      }),
    );
    for (const page of [...createdPages].reverse()) {
      try {
        await page.close();
        closedTabs += 1;
      } catch (error) {
        cleanupFailures.push({
          target: 'browser',
          operation: 'owned_tab_close',
          name: error instanceof Error ? error.name : 'Error',
          message: error instanceof Error ? error.message : String(error),
        });
      }
    }
    if (browser) {
      try {
        // In Playwright 1.63 connectOverCDP(), Browser.close() closes the
        // Playwright/CDP transport created for this attachment. The connected
        // Chromium browserProcess.close hook is transport.closeAndWait(), not
        // a Browser.close CDP command, so Main's raw Chromium process remains.
        await browser.close();
        playwrightConnectionClosed = true;
      } catch (error) {
        cleanupFailures.push({
          target: 'browser',
          operation: 'playwright_cdp_disconnect',
          name: error instanceof Error ? error.name : 'Error',
          message: error instanceof Error ? error.message : String(error),
        });
      }
    }
    if (cleanupFailures.length > 0) {
      status = 'FAIL';
      failure ??= {
        name: 'CleanupError',
        message:
          'native evidence capture, token restoration, owned-tab cleanup, or CDP disconnect failed',
        details: cleanupFailures,
      };
    }
  }

  let rawRecordIds = [];
  try {
    rawRecordIds = ledger.validateUniqueIds();
  } catch (error) {
    status = 'FAIL';
    failure ??= {
      name: error.name,
      message: error.message,
      details: error.details,
    };
  }
  const endedAt = Date.now();
  const evidence = stripInternalFields({
    schema_version: 1,
    case_id: CASE_ID,
    run_id: runId,
    status,
    failure,
    mechanism: 'native_tab_switch',
    visibility_probe: 'browser_native',
    browser_ownership: {
      connected_to_existing_browser: browser !== null,
      launched_browser_child: false,
      playwright_connection_closed: playwrightConnectionClosed,
      raw_browser_process_closed: false,
      context_policy: 'browser.contexts()[0] default context only',
      created_tabs_total: createdPages.length,
      created_tabs_closed: closedTabs,
    },
    cleanup_failures: cleanupFailures,
    started_at_utc: iso(startedAt),
    ended_at_utc: iso(endedAt),
    duration_ms: endedAt - startedAt,
    inputs: {
      manifest_path_sha256: sha256(manifestPath),
      manifest_sha256: sha256(manifestBytes),
      fixture_version: manifest.fixture_version,
      scratch_path_sha256: sha256(scratchDir),
      token_file_path_sha256: sha256(tokenFile),
      cdp_endpoint: cdpEndpoint,
      target_origins: Object.fromEntries(
        targets.map((target) => [target.dataset, target.origin]),
      ),
    },
    observation_contract: {
      visible_each_ms: visibleMs,
      hidden_all_ms: hiddenMs,
      minimum_visible_each_ms: MIN_VISIBLE_MS,
      minimum_hidden_all_ms: MIN_HIDDEN_MS,
      hidden_polling: 'Node timer plus page.evaluate; no requestAnimationFrame',
      hidden_inflight_policy:
        'allow completion only when the recorder proves request start <= native hidden event',
      descriptor_policy:
        'no own document hidden/visibilityState properties and unchanged Document.prototype accessors',
    },
    hidden_steady: {
      started_at_utc:
        hiddenSteadyStart === null ? null : iso(hiddenSteadyStart),
      ended_at_utc: hiddenSteadyEnd === null ? null : iso(hiddenSteadyEnd),
      duration_ms:
        hiddenSteadyStart === null || hiddenSteadyEnd === null
          ? null
          : hiddenSteadyEnd - hiddenSteadyStart,
    },
    per_phase_counts: phaseCounts,
    manual_dom_cross_client_match: crossClientDom,
    targets: targetStates.map((state) => ({
      dataset: state.target.dataset,
      engine: state.target.engine,
      phase: state.target.phase,
      runtime_profile: state.target.runtime_profile,
      origin: state.target.origin,
      page_url: redactUrl(state.page.url()),
      intervals: state.intervals,
      native: state.native,
      v8_heap_samples: state.heap_samples,
      manual_dom_snapshots: state.dom_snapshots,
      paint_frames: state.paint_frames,
    })),
    raw_jsonl: {
      embedded: true,
      unique_record_ids: new Set(rawRecordIds).size === rawRecordIds.length,
      record_ids: rawRecordIds,
      records: ledger.records,
    },
  });
  writeEvidence(outputPath, evidence);
  process.stdout.write(`${outputPath}\n`);
  if (status === 'BLOCKED') process.exitCode = 2;
  else if (status !== 'PASS') process.exitCode = 1;
}

run().catch((error) => {
  process.stderr.write(
    `${error instanceof Error ? error.stack ?? error.message : String(error)}\n`,
  );
  process.exitCode = 1;
});
