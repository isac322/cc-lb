(() => {
  'use strict';

  /*
   * Admin Web browser evidence recorder, schema version 1.
   *
   * Evaluate this whole file in the Admin Web page, then use:
   *
   *   const recorder = globalThis.__ccLbAdminWebQaRecorder;
   *   recorder.install({ max_records: 2_000, max_body_bytes: 262_144 });
   *   recorder.beginAction({
   *     source_id: 'UI-PR-01:list',
   *     environment: 'local',
   *     resource_alias: 'principals.list',
   *     expected_requests: [{
   *       method: 'GET',
   *       route_template: '/admin/v1/principals',
   *       allowed_query_params: ['cursor', 'limit'],
   *     }],
   *     correlation_headers: ['x-request-id', 'traceparent'],
   *   });
   *   // The harness performs a native Camofox click. This script never clicks.
   *   await recorder.endAction({
   *     oracle_id: 'UI-PR-01:list:oracle',
   *     observer: 'main_agent',
   *     expected_state_sha256: '<64 lowercase hex chars>',
   *     observed_state_sha256: '<64 lowercase hex chars>',
   *     screen_sha256: '<64 lowercase hex chars>',
   *     visibility: 'visible',
   *     reason: null,
   *   });
   *   const evidence = await recorder.drain();
   *
   * A read-only independent request is started exactly once:
   *
   *   const direct = recorder.independentRead({
   *     source_id: 'UI-PR-01:list:direct',
   *     environment: 'local',
   *     resource_alias: 'principals.list.direct',
   *     expected_requests: [{
   *       method: 'GET',
   *       route_template: '/admin/v1/principals',
   *       allowed_query_params: ['cursor', 'limit'],
   *     }],
   *     correlation_headers: ['x-request-id'],
   *     request: { input: '/admin/v1/principals?limit=25', init: {} },
   *   });
   *   const response = await direct.response; // the original native fetch promise
   *   const directEvidence = await direct.evidence;
   *
   * snapshot()/drain() return JSON-safe data shaped as:
   * {
   *   schema_version, collector_version, installed, capabilities,
   *   capture: { complete, incomplete_reasons, overflow, observer_dropped_entries },
   *   clocks: { time_origin_epoch_ms, captured_epoch_ms, captured_monotonic_ms },
   *   records: Array<action_window|fetch_observation|resource_timing|independent_read|collector_notice>
   * }
   *
   * No record contains a raw request URL, query value, request/response body,
   * Authorization/Cookie/Set-Cookie value, or fixture credential. URL and body
   * equality is represented only by SHA-256 plus route templates and allowed
   * query parameter names. The fetch wrapper returns the exact Promise returned
   * by native fetch and never substitutes or consumes the application's Response.
   * Bounded response cloning is an observer side effect and is timed explicitly.
   * A Server-Timing rid metric description is recorded verbatim only when it
   * matches the server-generated req_admin_<UUIDv7> format; every other
   * description is discarded and never collected.
   */

  const GLOBAL_KEY = '__ccLbAdminWebQaRecorder';
  const COLLECTOR_VERSION = '1.0.7';
  const SCHEMA_VERSION = 1;
  const DEFAULT_MAX_RECORDS = 2_000;
  const DEFAULT_MAX_BODY_BYTES = 256 * 1024;
  const MAX_MAX_RECORDS = 20_000;
  const MAX_MAX_BODY_BYTES = 4 * 1024 * 1024;
  const MAX_EXPECTED_REQUESTS = 32;
  const MAX_CORRELATION_HEADERS = 16;
  const MAX_SECRET_SCAN_NODES = 4_096;
  const URL_MATCH_EPSILON_MS = 2;
  const SOURCE_ID = /^[A-Za-z0-9][A-Za-z0-9._:/#-]{0,255}$/;
  const RESOURCE_ALIAS = /^[A-Za-z0-9][A-Za-z0-9._:/-]{0,159}$/;
  const ENVIRONMENT = /^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/;
  const HEADER_NAME = /^[!#$%&'*+.^_`|~0-9A-Za-z-]+$/;
  const QUERY_NAME = /^[A-Za-z0-9][A-Za-z0-9._~-]{0,127}$/;
  const SHA256 = /^[a-f0-9]{64}$/;
  const PROD_ENVIRONMENT = /(?:prod(?:uction)?|live)/i;
  const SERVER_REQUEST_ID =
    /^req_admin_[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-7[0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$/;
  const SECRET_KEY =
    /(?:authorization|cookie|credential|password|passwd|secret|token|api[_-]?key|private[_-]?key|client[_-]?secret|access[_-]?token|refresh[_-]?token)/i;
  const SECRET_VALUE =
    /(?:\bBearer\s+[A-Za-z0-9._~+\/-]+=*|\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b|-----BEGIN [A-Z ]*PRIVATE KEY-----)/i;
  const FORBIDDEN_HEADERS = new Set([
    'authorization',
    'proxy-authorization',
    'cookie',
    'set-cookie',
    'x-api-key',
    'api-key',
  ]);
  const DIRECT_INIT_FIELDS = new Set([
    'cache',
    'headers',
    'integrity',
    'keepalive',
    'method',
    'mode',
    'priority',
    'redirect',
    'referrer',
    'referrerPolicy',
    'signal',
  ]);
  const ORACLE_OBSERVERS = new Set([
    'main_agent',
    'camofox_accessibility',
    'camofox_screenshot',
    'browser_dom_hash',
  ]);
  const ORACLE_VISIBILITY = new Set(['visible', 'hidden', 'absent', 'unknown']);

  const prior = globalThis[GLOBAL_KEY];
  if (prior && prior.collector_version === COLLECTOR_VERSION) {
    return prior.install();
  }
  if (prior?.installed) {
    throw new Error(
      `${GLOBAL_KEY} ${prior.collector_version ?? 'unknown'} is already installed`,
    );
  }

  const state = {
    installed: false,
    installed_epoch_ms: null,
    installed_monotonic_ms: null,
    original_fetch: null,
    original_fetch_own_descriptor: null,
    fetch_had_own_property: false,
    wrapped_fetch: null,
    observer: null,
    observer_mode: null,
    observer_error: null,
    observer_dropped_entries: 0,
    observer_dropped_entries_supported: null,
    max_records: DEFAULT_MAX_RECORDS,
    max_body_bytes: DEFAULT_MAX_BODY_BYTES,
    sequence: 0,
    records: [],
    overflow: {
      dropped_records: 0,
      first_dropped_sequence: null,
      last_dropped_sequence: null,
    },
    active_action: null,
    actions: new Map(),
    request_private: new Map(),
    pending_tasks: new Set(),
    notices: new Set(),
  };

  function nowClock() {
    return {
      epoch_ms: Date.now(),
      monotonic_ms: performance.now(),
      time_origin_epoch_ms: Number.isFinite(performance.timeOrigin)
        ? performance.timeOrigin
        : null,
    };
  }

  function uuid() {
    if (typeof globalThis.crypto?.randomUUID !== 'function') {
      throw new Error(
        'crypto.randomUUID is required; no synthetic ID fallback exists',
      );
    }
    return globalThis.crypto.randomUUID();
  }

  function finiteInteger(value, name, minimum, maximum) {
    if (!Number.isSafeInteger(value) || value < minimum || value > maximum) {
      throw new TypeError(
        `${name} must be an integer from ${minimum} to ${maximum}`,
      );
    }
    return value;
  }

  function requiredString(value, pattern, name) {
    if (typeof value !== 'string' || !pattern.test(value)) {
      throw new TypeError(`${name} is invalid`);
    }
    return value;
  }

  function optionalHash(value, name) {
    if (value === null || value === undefined) return null;
    if (typeof value !== 'string' || !SHA256.test(value)) {
      throw new TypeError(
        `${name} must be null or a lowercase SHA-256 hex string`,
      );
    }
    return value;
  }

  function jsonCopy(value) {
    return JSON.parse(JSON.stringify(value));
  }

  function addRecord(record) {
    const clock = nowClock();
    const complete = {
      schema_version: SCHEMA_VERSION,
      record_id: uuid(),
      sequence: ++state.sequence,
      recorded_epoch_ms: clock.epoch_ms,
      recorded_monotonic_ms: clock.monotonic_ms,
      ...record,
    };
    state.records.push(complete);
    if (state.records.length > state.max_records) {
      const dropped = state.records.shift();
      state.overflow.dropped_records += 1;
      state.overflow.first_dropped_sequence ??= dropped.sequence;
      state.overflow.last_dropped_sequence = dropped.sequence;
    }
    return complete;
  }

  function noticeOnce(code, detail) {
    if (state.notices.has(code)) return;
    state.notices.add(code);
    addRecord({ record_type: 'collector_notice', code, detail });
  }

  function trackTask(promise) {
    const task = Promise.resolve(promise)
      .catch((error) => {
        noticeOnce('observer_task_failed', errorSummary(error));
      })
      .finally(() => state.pending_tasks.delete(task));
    state.pending_tasks.add(task);
    return task;
  }

  function errorSummary(error) {
    return {
      name:
        error && typeof error === 'object' && typeof error.name === 'string'
          ? error.name
          : 'Error',
      message_sha256: null,
      message_length:
        error && typeof error === 'object' && typeof error.message === 'string'
          ? error.message.length
          : String(error).length,
    };
  }

  async function sha256Bytes(bytes) {
    if (typeof globalThis.crypto?.subtle?.digest !== 'function') {
      return { value: null, reason: 'crypto.subtle.digest_unavailable' };
    }
    const digest = await globalThis.crypto.subtle.digest('SHA-256', bytes);
    const view = new DataView(digest);
    let value = '';
    for (let index = 0; index < view.byteLength; index += 1)
      value += view.getUint8(index).toString(16).padStart(2, '0');
    return { value, reason: null };
  }

  async function sha256Text(value) {
    return sha256Bytes(new TextEncoder().encode(value));
  }

  function isSecretValue(value) {
    return typeof value === 'string' && SECRET_VALUE.test(value);
  }

  function scanJsonForSecrets(value) {
    let visited = 0;
    const stack = [value];
    while (stack.length > 0) {
      const item = stack.pop();
      visited += 1;
      if (visited > MAX_SECRET_SCAN_NODES) {
        return { secret: null, reason: 'secret_scan_node_cap_exceeded' };
      }
      if (isSecretValue(item)) {
        return { secret: true, reason: 'secret_like_value_detected' };
      }
      if (Array.isArray(item)) {
        for (const child of item) stack.push(child);
      } else if (item && typeof item === 'object') {
        for (const [key, child] of Object.entries(item)) {
          if (SECRET_KEY.test(key)) {
            return { secret: true, reason: 'secret_like_key_detected' };
          }
          stack.push(child);
        }
      }
    }
    return { secret: false, reason: null };
  }

  function scanTextForSecrets(text, contentKind) {
    if (SECRET_VALUE.test(text)) {
      return { secret: true, reason: 'secret_like_value_detected' };
    }
    const trimmed = text.trimStart();
    if (
      contentKind === 'json' ||
      (contentKind === 'text' &&
        (trimmed.startsWith('{') || trimmed.startsWith('[')))
    ) {
      try {
        return scanJsonForSecrets(JSON.parse(text));
      } catch {
        return {
          secret: null,
          reason:
            contentKind === 'json'
              ? 'declared_json_was_not_parseable'
              : 'json_like_text_was_not_parseable',
        };
      }
    }
    if (contentKind === 'form') {
      const params = new URLSearchParams(text);
      let formSecret = false;
      params.forEach((value, name) => {
        if (!formSecret && (SECRET_KEY.test(name) || isSecretValue(value)))
          formSecret = true;
      });
      if (formSecret)
        return { secret: true, reason: 'secret_like_form_field_detected' };
    }
    return { secret: false, reason: null };
  }

  function contentKind(contentType) {
    const essence = String(contentType ?? '')
      .split(';', 1)[0]
      .trim()
      .toLowerCase();
    if (essence === 'text/event-stream') return 'sse';
    if (essence === 'application/json' || essence.endsWith('+json'))
      return 'json';
    if (essence === 'application/x-www-form-urlencoded') return 'form';
    if (essence.startsWith('text/')) return 'text';
    return essence ? 'binary' : 'unknown';
  }

  function bodyBytes(body) {
    if (body === undefined || body === null) {
      return { bytes: new Uint8Array(0), kind: 'empty', reason: null };
    }
    if (typeof body === 'string') {
      return {
        bytes: new TextEncoder().encode(body),
        kind: 'text',
        reason: null,
      };
    }
    if (body instanceof URLSearchParams) {
      return {
        bytes: new TextEncoder().encode(body.toString()),
        kind: 'form',
        reason: null,
      };
    }
    if (body instanceof ArrayBuffer) {
      return { bytes: new Uint8Array(body), kind: 'binary', reason: null };
    }
    if (ArrayBuffer.isView(body)) {
      return {
        bytes: new Uint8Array(body.buffer, body.byteOffset, body.byteLength),
        kind: 'binary',
        reason: null,
      };
    }
    return {
      bytes: null,
      kind: body instanceof Blob ? 'blob' : 'stream_or_structured',
      reason: 'body_type_not_read_to_avoid_consuming_or_serializing_it',
    };
  }

  async function inspectRequestBody(input, init, output) {
    const clockStart = nowClock();
    let result;
    if (init && typeof init === 'object') {
      let descriptor;
      try {
        descriptor = Object.getOwnPropertyDescriptor(init, 'body');
      } catch {
        descriptor = null;
      }
      if (descriptor && Object.hasOwn(descriptor, 'value')) {
        result = bodyBytes(descriptor.value);
      } else if (descriptor) {
        result = {
          bytes: null,
          kind: 'accessor',
          reason: 'request_init_body_accessor_not_invoked_by_observer',
        };
      }
    }
    if (!result && typeof Request !== 'undefined' && input instanceof Request) {
      output.request_body = {
        sha256: null,
        bytes: null,
        reason: 'request_object_body_not_cloned_or_consumed',
        secret_scan: 'not_performed',
        observer_overhead_ms: performance.now() - clockStart.monotonic_ms,
      };
      return;
    }
    result ??= bodyBytes(null);

    if (result.bytes === null) {
      output.request_body = {
        sha256: null,
        bytes: null,
        reason: result.reason,
        secret_scan: 'not_performed',
        observer_overhead_ms: performance.now() - clockStart.monotonic_ms,
      };
      return;
    }
    if (result.bytes.byteLength > state.max_body_bytes) {
      output.request_body = {
        sha256: null,
        bytes: result.bytes.byteLength,
        reason: 'body_exceeds_capture_cap',
        secret_scan: 'not_performed',
        observer_overhead_ms: performance.now() - clockStart.monotonic_ms,
      };
      return;
    }

    if (result.kind === 'binary' || result.kind === 'stream_or_structured') {
      output.request_body = {
        sha256: null,
        bytes: result.bytes?.byteLength ?? null,
        reason: 'body_kind_not_safely_secret_scannable',
        secret_scan: 'not_performed',
        observer_overhead_ms: performance.now() - clockStart.monotonic_ms,
      };
      return;
    }
    let secretScan = { secret: false, reason: null };
    if (result.kind === 'text' || result.kind === 'form') {
      const text = new TextDecoder().decode(result.bytes);
      secretScan = scanTextForSecrets(text, result.kind);
    }
    if (secretScan.secret !== false) {
      output.request_body = {
        sha256: null,
        bytes: result.bytes.byteLength,
        reason: secretScan.reason,
        secret_scan:
          secretScan.secret === true ? 'secret_detected' : 'inconclusive',
        observer_overhead_ms: performance.now() - clockStart.monotonic_ms,
      };
      return;
    }
    const digest = await sha256Bytes(result.bytes);
    output.request_body = {
      sha256: digest.value,
      bytes: result.bytes.byteLength,
      reason: digest.reason,
      secret_scan: 'clear_within_bounded_scan',
      observer_overhead_ms: performance.now() - clockStart.monotonic_ms,
    };
  }

  function compileRouteTemplate(template) {
    if (
      typeof template !== 'string' ||
      !template.startsWith('/') ||
      template.includes('?') ||
      template.includes('#') ||
      template.includes('://') ||
      template.length > 512 ||
      !/^\/[A-Za-z0-9._~/{|}*-]*$/.test(template)
    ) {
      throw new TypeError('route_template must be a safe path-only template');
    }
    for (const segment of template.split('/')) {
      if (
        !segment.includes('{') &&
        (/[a-f0-9]{24,}/i.test(segment) || /^[A-Za-z0-9_-]{65,}$/.test(segment))
      ) {
        throw new TypeError(
          'route_template contains a likely raw identifier; use a placeholder',
        );
      }
    }
    let expression = '^';
    for (let index = 0; index < template.length; index += 1) {
      const character = template[index];
      if (character === '{') {
        const close = template.indexOf('}', index + 1);
        if (close === -1)
          throw new TypeError('route_template has an unclosed placeholder');
        const placeholder = template.slice(index + 1, close);
        if (!/^[A-Za-z0-9_-]+(?:\|[A-Za-z0-9_-]+)*$/.test(placeholder)) {
          throw new TypeError('route_template placeholder is invalid');
        }
        const parts = placeholder.split('|');
        expression +=
          parts.length === 1
            ? '[^/]+'
            : `(?:${parts.map(escapeRegExp).join('|')})`;
        index = close;
      } else if (character === '*') {
        expression += '.*';
      } else {
        expression += escapeRegExp(character);
      }
    }
    return new RegExp(`${expression}$`);
  }

  function escapeRegExp(value) {
    return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  }

  function normalizeExpectedRequest(value, index) {
    if (!value || typeof value !== 'object' || Array.isArray(value)) {
      throw new TypeError(`expected_requests[${index}] must be an object`);
    }
    const method = requiredString(
      String(value.method ?? '').toUpperCase(),
      /^[A-Z]+$/,
      `expected_requests[${index}].method`,
    );
    const routeTemplate = value.route_template;
    const matcher = compileRouteTemplate(routeTemplate);
    const queryNames = value.allowed_query_params ?? [];
    if (!Array.isArray(queryNames) || queryNames.length > 64) {
      throw new TypeError(
        `expected_requests[${index}].allowed_query_params must be an array of at most 64 names`,
      );
    }
    const allowedQueryParams = [
      ...new Set(
        queryNames.map((name) =>
          requiredString(
            name,
            QUERY_NAME,
            `expected_requests[${index}].allowed_query_params`,
          ),
        ),
      ),
    ].sort();
    return {
      method,
      route_template: routeTemplate,
      allowed_query_params: allowedQueryParams,
      _matcher: matcher,
    };
  }

  function normalizeCorrelationHeaders(value) {
    const headers = value ?? [];
    if (!Array.isArray(headers) || headers.length > MAX_CORRELATION_HEADERS) {
      throw new TypeError(
        `correlation_headers must be an array of at most ${MAX_CORRELATION_HEADERS} names`,
      );
    }
    return [
      ...new Set(
        headers.map((header) => {
          const normalized = requiredString(
            String(header).toLowerCase(),
            HEADER_NAME,
            'correlation_headers[]',
          );
          if (FORBIDDEN_HEADERS.has(normalized)) {
            throw new TypeError(`forbidden correlation header: ${normalized}`);
          }
          return normalized;
        }),
      ),
    ].sort();
  }

  function normalizeDescriptor(value, mode) {
    if (!value || typeof value !== 'object' || Array.isArray(value)) {
      throw new TypeError('descriptor must be an object');
    }
    const sourceId = requiredString(value.source_id, SOURCE_ID, 'source_id');
    const environment = requiredString(
      value.environment,
      ENVIRONMENT,
      'environment',
    );
    const resourceAlias = requiredString(
      value.resource_alias,
      RESOURCE_ALIAS,
      'resource_alias',
    );
    if (
      !Array.isArray(value.expected_requests) ||
      value.expected_requests.length === 0 ||
      value.expected_requests.length > MAX_EXPECTED_REQUESTS
    ) {
      throw new TypeError(
        `expected_requests must contain 1-${MAX_EXPECTED_REQUESTS} entries`,
      );
    }
    const expectedRequests = value.expected_requests.map(
      normalizeExpectedRequest,
    );
    const descriptor = {
      source_id: sourceId,
      environment,
      resource_alias: resourceAlias,
      expected_requests: expectedRequests,
      correlation_headers: normalizeCorrelationHeaders(
        value.correlation_headers,
      ),
    };
    if (mode === 'direct') {
      if (!value.request || typeof value.request !== 'object') {
        throw new TypeError('independentRead descriptor.request is required');
      }
      if (!Object.hasOwn(value.request, 'input')) {
        throw new TypeError(
          'independentRead descriptor.request.input is required',
        );
      }
      if (
        typeof Request !== 'undefined' &&
        value.request.input instanceof Request
      ) {
        throw new TypeError(
          'independentRead requires a string or URL input, not a Request with hidden state',
        );
      }
      const init = value.request.init;
      if (init !== undefined && (!init || typeof init !== 'object')) {
        throw new TypeError('independentRead request.init must be an object');
      }
      if (
        init &&
        (propertyDescriptorInChain(init, 'body') ||
          propertyDescriptorInChain(init, 'credentials'))
      ) {
        throw new TypeError(
          'independentRead rejects own or inherited body and credentials overrides',
        );
      }
      if (init) {
        const ownMethod = Object.getOwnPropertyDescriptor(init, 'method');
        const inheritedMethod =
          !ownMethod &&
          propertyDescriptorInChain(Object.getPrototypeOf(init), 'method');
        if (
          inheritedMethod ||
          (ownMethod && !Object.hasOwn(ownMethod, 'value'))
        ) {
          throw new TypeError(
            'independentRead rejects inherited or accessor method values',
          );
        }
      }
      const safeInit = sanitizeDirectInit(init);
      const method = requestMethod(value.request.input, safeInit);
      if (method === null) {
        throw new TypeError(
          'independentRead requires a plain data method or an implicit GET',
        );
      }
      if (method !== 'GET' && method !== 'HEAD') {
        throw new TypeError('independentRead permits only GET or HEAD');
      }
      const rawUrl = requestUrl(value.request.input);
      const parsed = new URL(rawUrl);
      if (
        !['http:', 'https:'].includes(parsed.protocol) ||
        parsed.username ||
        parsed.password ||
        parsed.hash ||
        parsed.origin !== globalThis.location?.origin
      ) {
        throw new TypeError(
          'independentRead requires a credential-free same-origin HTTP(S) URL',
        );
      }
      const matched = matchExpected(descriptor, method, rawUrl);
      if (!matched.matched || matched.reason !== null) {
        throw new TypeError(
          `independentRead URL is outside the validated route/query allowlist: ${matched.reason}`,
        );
      }
      const productionLabel = PROD_ENVIRONMENT.test(environment);
      const loopbackOrigin = new Set([
        '127.0.0.1',
        'localhost',
        '::1',
        '[::1]',
      ]).has(parsed.hostname);
      const productionOptInRequired = productionLabel || !loopbackOrigin;
      if (productionOptInRequired && value.allow_production_read !== true) {
        throw new TypeError(
          'production-labelled or non-loopback independentRead requires allow_production_read:true',
        );
      }
      const readSafety = value.read_safety ?? 'read_only';
      if (!['read_only', 'known_read_with_audit'].includes(readSafety)) {
        throw new TypeError(
          'read_safety must be read_only or known_read_with_audit',
        );
      }
      if (
        readSafety === 'known_read_with_audit' &&
        value.allow_known_read_with_audit !== true
      ) {
        throw new TypeError(
          'known_read_with_audit requires allow_known_read_with_audit:true',
        );
      }
      descriptor.direct_safety = {
        allow_production_read:
          productionOptInRequired && value.allow_production_read === true,
        production_label: productionLabel,
        non_loopback_origin: !loopbackOrigin,
        read_safety: readSafety,
        allow_known_read_with_audit:
          readSafety === 'known_read_with_audit' &&
          value.allow_known_read_with_audit === true,
      };
      descriptor.request = { input: value.request.input, init: safeInit };
    }
    return descriptor;
  }

  function publicDescriptor(descriptor) {
    return {
      source_id: descriptor.source_id,
      environment: descriptor.environment,
      resource_alias: descriptor.resource_alias,
      expected_requests: descriptor.expected_requests.map((expected) => ({
        method: expected.method,
        route_template: expected.route_template,
        allowed_query_params: [...expected.allowed_query_params],
      })),
      correlation_headers: [...descriptor.correlation_headers],
      direct_safety: descriptor.direct_safety
        ? { ...descriptor.direct_safety }
        : null,
    };
  }

  function propertyDescriptorInChain(value, name) {
    for (
      let current = value;
      current !== null;
      current = Object.getPrototypeOf(current)
    ) {
      const descriptor = Object.getOwnPropertyDescriptor(current, name);
      if (descriptor) return descriptor;
    }
    return null;
  }
  function sanitizeDirectInit(init) {
    if (init === undefined) return undefined;
    const safe = {};
    for (const key of Reflect.ownKeys(init)) {
      if (typeof key !== 'string' || !DIRECT_INIT_FIELDS.has(key)) {
        throw new TypeError(
          `independentRead request.init field is not allowed: ${String(key)}`,
        );
      }
      const descriptor = Object.getOwnPropertyDescriptor(init, key);
      if (!descriptor || !Object.hasOwn(descriptor, 'value')) {
        throw new TypeError(
          `independentRead request.init accessor is not allowed: ${key}`,
        );
      }
      safe[key] = descriptor.value;
    }
    return safe;
  }

  function requestMethod(input, init) {
    if (init && typeof init === 'object') {
      let descriptor;
      try {
        descriptor = propertyDescriptorInChain(init, 'method');
      } catch {
        return null;
      }
      if (descriptor) {
        if (!Object.hasOwn(descriptor, 'value')) return null;
        return String(descriptor.value).toUpperCase();
      }
    }
    if (typeof Request !== 'undefined' && input instanceof Request) {
      return input.method.toUpperCase();
    }
    return 'GET';
  }

  function requestUrl(input) {
    if (typeof Request !== 'undefined' && input instanceof Request)
      return input.url;
    if (input instanceof URL) return input.href;
    if (typeof input === 'string') {
      return new URL(input, globalThis.location?.href).href;
    }
    throw new TypeError('observer does not coerce non-string fetch inputs');
  }

  function matchExpected(descriptor, method, rawUrl) {
    let parsed;
    try {
      parsed = new URL(rawUrl, globalThis.location?.href);
    } catch {
      return {
        matched: null,
        reason: 'request_url_could_not_be_parsed',
        allowed_query_param_names: [],
        unexpected_query_param_count: null,
      };
    }
    const candidates = descriptor.expected_requests.filter(
      (expected) =>
        expected.method === method && expected._matcher.test(parsed.pathname),
    );
    if (candidates.length !== 1) {
      return {
        matched: null,
        reason:
          candidates.length === 0
            ? 'no_expected_method_route_match'
            : 'multiple_expected_method_route_matches',
        allowed_query_param_names: [],
        unexpected_query_param_count: parsed.searchParams.size,
      };
    }
    const expected = candidates[0];
    const actualNameSet = new Set();
    parsed.searchParams.forEach((_value, name) => actualNameSet.add(name));
    const actualNames = [...actualNameSet].sort();
    const allowed = actualNames.filter((name) =>
      expected.allowed_query_params.includes(name),
    );
    return {
      matched: expected,
      reason:
        actualNames.length === allowed.length
          ? null
          : 'request_has_non_allowlisted_query_parameter_names',
      allowed_query_param_names: allowed,
      unexpected_query_param_count: actualNames.length - allowed.length,
    };
  }

  function assignSameUrlOverlap(privateRecord) {
    const overlapping = [...state.request_private.values()].filter(
      (candidate) =>
        candidate !== privateRecord &&
        candidate.raw_url === privateRecord.raw_url &&
        candidate.settled_monotonic_ms === null,
    );
    if (overlapping.length === 0) return;
    const groupId =
      overlapping.find((candidate) => candidate.overlap_group_id)
        ?.overlap_group_id ?? uuid();
    privateRecord.overlap_group_id = groupId;
    privateRecord.public_record.same_url_concurrency = {
      state: 'overlap_observed',
      overlap_group_id: groupId,
      peer_request_ids: overlapping.map((candidate) => candidate.request_id),
    };
    for (const candidate of overlapping) {
      candidate.overlap_group_id = groupId;
      const peers = new Set(
        candidate.public_record.same_url_concurrency?.peer_request_ids ?? [],
      );
      peers.add(privateRecord.request_id);
      candidate.public_record.same_url_concurrency = {
        state: 'overlap_observed',
        overlap_group_id: groupId,
        peer_request_ids: [...peers],
      };
    }
  }
  function statusObservation(status) {
    if (!Number.isFinite(status)) {
      return { value: null, reason: 'status_value_unavailable' };
    }
    return status === 0
      ? { value: null, reason: 'opaque_response_status_zero' }
      : { value: status, reason: null };
  }

  async function correlationHeaders(response, names) {
    const observed = [];
    for (const name of names) {
      const value = response.headers.get(name);
      if (value === null) continue;
      const digest = await sha256Text(value);
      observed.push({
        name,
        value_sha256: digest.value,
        value_length: value.length,
        reason: digest.reason,
      });
    }
    return {
      observed,
      blocker:
        observed.length === 0
          ? 'no_allowlisted_http_correlation_header_observed'
          : null,
    };
  }

  async function readBoundedResponseBody(response) {
    if (!response.body || typeof response.body.getReader !== 'function') {
      return { bytes: new Uint8Array(0), exceeded: false };
    }
    const reader = response.body.getReader();
    const chunks = [];
    let length = 0;
    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        length += value.byteLength;
        if (length > state.max_body_bytes) {
          await reader.cancel('qa recorder body cap reached');
          return { bytes: null, exceeded: true, observed_bytes: length };
        }
        chunks.push(value);
      }
    } finally {
      reader.releaseLock();
    }
    const bytes = new Uint8Array(length);
    let offset = 0;
    for (const chunk of chunks) {
      bytes.set(chunk, offset);
      offset += chunk.byteLength;
    }
    return { bytes, exceeded: false };
  }

  async function inspectResponseBody(response, method, output) {
    const started = nowClock();
    const kind = contentKind(response.headers.get('content-type'));
    output.streaming = {
      kind: kind === 'sse' ? 'sse' : 'not_identified_as_sse',
      opened_epoch_ms: kind === 'sse' ? started.epoch_ms : null,
      opened_monotonic_ms: kind === 'sse' ? started.monotonic_ms : null,
      closed_epoch_ms: null,
      closed_monotonic_ms: null,
      close_source: null,
      reason:
        kind === 'sse'
          ? 'SSE_body_is_never_cloned_or_read; close_is_only_known_if_resource_timing_arrives'
          : null,
    };
    if (kind === 'sse') {
      output.response_body = {
        sha256: null,
        bytes: null,
        reason: 'sse_stream_not_cloned_or_read',
        secret_scan: 'not_performed',
        observer_overhead_ms: performance.now() - started.monotonic_ms,
      };
      return;
    }
    if (method === 'HEAD' || response.body === null) {
      const digest = await sha256Bytes(new Uint8Array(0));
      output.response_body = {
        sha256: digest.value,
        bytes: 0,
        reason: digest.reason,
        secret_scan: 'not_applicable',
        observer_overhead_ms: performance.now() - started.monotonic_ms,
      };
      return;
    }
    if (kind === 'binary' || kind === 'unknown') {
      output.response_body = {
        sha256: null,
        bytes: null,
        reason: 'content_type_not_safely_secret_scannable',
        secret_scan: 'not_performed',
        observer_overhead_ms: performance.now() - started.monotonic_ms,
      };
      return;
    }

    const contentLength = response.headers.get('content-length');
    const declaredBytes = contentLength === null ? null : Number(contentLength);
    if (
      declaredBytes === null ||
      !Number.isSafeInteger(declaredBytes) ||
      declaredBytes < 0
    ) {
      output.response_body = {
        sha256: null,
        bytes: null,
        reason: 'bounded_clone_requires_valid_content_length',
        secret_scan: 'not_performed',
        observer_overhead_ms: performance.now() - started.monotonic_ms,
      };
      return;
    }
    if (declaredBytes > state.max_body_bytes) {
      output.response_body = {
        sha256: null,
        bytes: null,
        reason: 'declared_body_exceeds_capture_cap',
        secret_scan: 'not_performed',
        observer_overhead_ms: performance.now() - started.monotonic_ms,
      };
      return;
    }

    let clone;
    try {
      clone = response.clone();
    } catch (error) {
      output.response_body = {
        sha256: null,
        bytes: null,
        reason: `response_clone_failed:${errorSummary(error).name}`,
        secret_scan: 'not_performed',
        observer_overhead_ms: performance.now() - started.monotonic_ms,
      };
      return;
    }
    try {
      const bounded = await readBoundedResponseBody(clone);
      if (bounded.exceeded) {
        output.response_body = {
          sha256: null,
          bytes: bounded.observed_bytes,
          reason: 'actual_body_exceeds_capture_cap',
          secret_scan: 'not_performed',
          observer_overhead_ms: performance.now() - started.monotonic_ms,
        };
        return;
      }
      const bytes = bounded.bytes;
      let secretScan = { secret: false, reason: null };
      if (kind === 'json' || kind === 'text' || kind === 'form') {
        secretScan = scanTextForSecrets(new TextDecoder().decode(bytes), kind);
      }
      if (secretScan.secret !== false) {
        output.response_body = {
          sha256: null,
          bytes: bytes.byteLength,
          reason: secretScan.reason,
          secret_scan:
            secretScan.secret === true ? 'secret_detected' : 'inconclusive',
          observer_overhead_ms: performance.now() - started.monotonic_ms,
        };
        return;
      }
      const digest = await sha256Bytes(bytes);
      output.response_body = {
        sha256: digest.value,
        bytes: bytes.byteLength,
        reason: digest.reason,
        secret_scan: 'clear_within_bounded_scan',
        observer_overhead_ms: performance.now() - started.monotonic_ms,
      };
    } catch (error) {
      output.response_body = {
        sha256: null,
        bytes: null,
        reason: `bounded_clone_read_failed:${errorSummary(error).name}`,
        secret_scan: 'not_performed',
        observer_overhead_ms: performance.now() - started.monotonic_ms,
      };
    }
  }

  function registerFetch(input, init, context, clock) {
    const requestId = uuid();
    const method = requestMethod(input, init);
    let rawUrl = null;
    let urlReason = null;
    try {
      rawUrl = requestUrl(input);
    } catch {
      urlReason =
        'request_url_could_not_be_read_without_reconstructing_request';
    }
    const match = rawUrl
      ? matchExpected(context.descriptor, method, rawUrl)
      : {
          matched: null,
          reason: urlReason,
          allowed_query_param_names: [],
          unexpected_query_param_count: null,
        };
    const publicRecord = addRecord({
      record_type: 'fetch_observation',
      request_id: requestId,
      capture_origin: context.capture_origin,
      action_id: context.action_id,
      independent_read_id: context.independent_read_id,
      source_id: context.descriptor.source_id,
      environment: context.descriptor.environment,
      resource_alias: context.descriptor.resource_alias,
      request: {
        method,
        method_reason:
          method === null
            ? 'method_accessor_or_input_coercion_not_repeated_by_observer'
            : null,
        route_template: match.matched?.route_template ?? null,
        route_match_reason: match.reason,
        allowed_query_param_names: match.allowed_query_param_names,
        unexpected_query_param_count: match.unexpected_query_param_count,
        url_sha256: null,
        url_sha256_reason: rawUrl ? 'pending' : urlReason,
        body: {
          sha256: null,
          bytes: null,
          reason: 'pending',
          secret_scan: 'pending',
          observer_overhead_ms: null,
        },
      },
      javascript_fetch: {
        call_epoch_ms: clock.epoch_ms,
        call_monotonic_ms: clock.monotonic_ms,
        settled_epoch_ms: null,
        settled_monotonic_ms: null,
        settle_latency_ms: null,
        settle_layer: 'javascript_fetch_promise; not network TTFB',
        returned_original_promise: true,
        response_object_replaced: false,
        response_status: { value: null, reason: 'pending' },
        response_type: null,
        redirected: null,
        failure: null,
        aborted: false,
      },
      response: {
        correlation_headers: [],
        correlation_blocker: 'pending',
        body: {
          sha256: null,
          bytes: null,
          reason: 'pending',
          secret_scan: 'pending',
          observer_overhead_ms: null,
        },
      },
      same_url_concurrency: {
        state: 'none_observed_at_call',
        overlap_group_id: null,
        peer_request_ids: [],
      },
      resource_timing_link: {
        state: 'pending',
        source_entry_ids: [],
        candidate_source_entry_ids: [],
        reason: null,
      },
      cache: {
        state: 'pending_resource_timing',
        basis: null,
      },
      inflight_reuse: {
        state: 'not_observable_yet',
        reason: 'Resource Timing has no request reuse identifier',
      },
    });
    const privateRecord = {
      request_id: requestId,
      public_record: publicRecord,
      descriptor: context.descriptor,
      raw_url: rawUrl,
      call_monotonic_ms: clock.monotonic_ms,
      settled_monotonic_ms: null,
      overlap_group_id: null,
      matched_source_entry_ids: new Set(),
      candidate_source_entry_ids: new Set(),
      monitor_task: null,
    };
    state.request_private.set(requestId, privateRecord);
    assignSameUrlOverlap(privateRecord);
    if (context.action_record)
      context.action_record.request_ids.push(requestId);
    if (context.direct_record) context.direct_record.request_id = requestId;

    if (rawUrl) {
      trackTask(
        sha256Text(rawUrl).then((digest) => {
          publicRecord.request.url_sha256 = digest.value;
          publicRecord.request.url_sha256_reason = digest.reason;
        }),
      );
    }
    trackTask(
      inspectRequestBody(input, init, publicRecord.request).then(() => {
        publicRecord.request.body = publicRecord.request.request_body;
        delete publicRecord.request.request_body;
      }),
    );
    return privateRecord;
  }

  function monitorFetchPromise(promise, privateRecord) {
    const publicRecord = privateRecord.public_record;
    const monitorTask = Promise.resolve(promise).then(
      (response) =>
        trackTask(
          (async () => {
            const clock = nowClock();
            privateRecord.settled_monotonic_ms = clock.monotonic_ms;
            publicRecord.javascript_fetch.settled_epoch_ms = clock.epoch_ms;
            publicRecord.javascript_fetch.settled_monotonic_ms =
              clock.monotonic_ms;
            publicRecord.javascript_fetch.settle_latency_ms =
              clock.monotonic_ms -
              publicRecord.javascript_fetch.call_monotonic_ms;
            if (
              typeof Response !== 'undefined' &&
              response instanceof Response
            ) {
              publicRecord.javascript_fetch.response_status = statusObservation(
                response.status,
              );
              publicRecord.javascript_fetch.response_type = response.type;
              publicRecord.javascript_fetch.redirected = response.redirected;
              const bodyTask = inspectResponseBody(
                response,
                publicRecord.request.method,
                publicRecord.response,
              );
              const headerEvidence = await correlationHeaders(
                response,
                privateRecord.descriptor.correlation_headers,
              );
              publicRecord.response.correlation_headers =
                headerEvidence.observed;
              publicRecord.response.correlation_blocker =
                headerEvidence.blocker;
              await bodyTask;
              publicRecord.response.body = publicRecord.response.response_body;
              delete publicRecord.response.response_body;
            } else {
              publicRecord.javascript_fetch.response_status = {
                value: null,
                reason: 'fulfilled_value_is_not_a_Response',
              };
              publicRecord.response.correlation_blocker =
                'fulfilled_value_is_not_a_Response';
              publicRecord.response.body.reason =
                'fulfilled_value_is_not_a_Response';
            }
          })(),
        ),
      (error) => {
        const clock = nowClock();
        privateRecord.settled_monotonic_ms = clock.monotonic_ms;
        publicRecord.javascript_fetch.settled_epoch_ms = clock.epoch_ms;
        publicRecord.javascript_fetch.settled_monotonic_ms = clock.monotonic_ms;
        publicRecord.javascript_fetch.settle_latency_ms =
          clock.monotonic_ms - publicRecord.javascript_fetch.call_monotonic_ms;
        publicRecord.javascript_fetch.failure = errorSummary(error);
        publicRecord.javascript_fetch.aborted = error?.name === 'AbortError';
        publicRecord.javascript_fetch.response_status = {
          value: null,
          reason: publicRecord.javascript_fetch.aborted
            ? 'fetch_aborted_before_response'
            : 'fetch_rejected',
        };
        publicRecord.response.correlation_blocker =
          'no_response_headers_from_rejected_fetch';
        publicRecord.response.body.reason =
          'no_response_body_from_rejected_fetch';
      },
    );
    privateRecord.monitor_task = monitorTask.catch((error) => {
      noticeOnce('fetch_observer_monitor_failed', errorSummary(error));
    });
    return privateRecord.monitor_task;
  }

  function invokeFetch(receiver, args, context) {
    const [input, init] = args;
    const callClock = nowClock();
    const promise = Reflect.apply(state.original_fetch, receiver, args);
    let privateRecord;
    try {
      privateRecord = registerFetch(input, init, context, callClock);
      privateRecord.public_record.javascript_fetch.native_call_returned_monotonic_ms =
        performance.now();
      privateRecord.public_record.javascript_fetch.registration_overhead_ms =
        performance.now() - callClock.monotonic_ms;
      monitorFetchPromise(promise, privateRecord);
    } catch (error) {
      noticeOnce('fetch_observer_registration_failed', errorSummary(error));
      if (context.direct_record) {
        context.direct_record.capture_blocker =
          'fetch_started_but_observer_registration_failed';
      }
      return { promise, private_record: null };
    }
    return { promise, private_record: privateRecord };
  }

  function activeFetchContext() {
    if (!state.active_action) return null;
    return {
      capture_origin: 'ui',
      action_id: state.active_action.action_id,
      independent_read_id: null,
      descriptor: state.active_action.descriptor,
      action_record: state.active_action.public_record,
      direct_record: null,
    };
  }

  function installFetchWrapper() {
    const original = globalThis.fetch;
    if (typeof original !== 'function') {
      throw new Error('global fetch is unavailable');
    }
    const ownDescriptor = Object.getOwnPropertyDescriptor(globalThis, 'fetch');
    let inheritedDescriptor = null;
    for (
      let owner = Object.getPrototypeOf(globalThis);
      owner && inheritedDescriptor === null;
      owner = Object.getPrototypeOf(owner)
    ) {
      inheritedDescriptor =
        Object.getOwnPropertyDescriptor(owner, 'fetch') ?? null;
    }
    state.original_fetch = original;
    state.original_fetch_own_descriptor = ownDescriptor ?? null;
    state.fetch_had_own_property = ownDescriptor !== undefined;
    const wrapped = function fetch(...args) {
      const context = activeFetchContext();
      if (!context) return Reflect.apply(state.original_fetch, this, args);
      return invokeFetch(this, args, context).promise;
    };
    try {
      Object.defineProperty(wrapped, 'name', {
        value: original.name,
        configurable: true,
      });
      Object.defineProperty(wrapped, 'length', {
        value: original.length,
        configurable: true,
      });
    } catch {
      noticeOnce(
        'fetch_metadata_not_preserved',
        'function name/length metadata could not be mirrored',
      );
    }
    Object.defineProperty(globalThis, 'fetch', {
      value: wrapped,
      configurable: true,
      enumerable:
        ownDescriptor?.enumerable ?? inheritedDescriptor?.enumerable ?? true,
      writable: true,
    });
    state.wrapped_fetch = wrapped;
  }

  function observedNumber(raw, options = {}) {
    if (!Number.isFinite(raw)) {
      return { value: null, raw_value: null, reason: 'property_unavailable' };
    }
    if (raw === 0 && options.zeroUnavailable) {
      return {
        value: null,
        raw_value: 0,
        reason: options.zeroReason ?? 'zero_is_unavailable_or_ambiguous',
      };
    }
    return { value: raw, raw_value: raw, reason: null };
  }

  function durationObservation(end, start, reason) {
    if (end.value === null || start.value === null) {
      return { value: null, reason };
    }
    if (end.value < start.value) {
      return { value: null, reason: 'native_timestamps_are_not_ordered' };
    }
    return { value: end.value - start.value, reason: null };
  }

  function resourceTimingFields(entry, sameOrigin) {
    const opaqueReason = sameOrigin
      ? 'zero_is_unavailable_cancelled_or_instant_cache'
      : 'zero_may_be_cross_origin_timing_masking_cache_or_cancel';
    const start = observedNumber(entry.startTime);
    const fetchStart = observedNumber(entry.fetchStart);
    const requestStart = observedNumber(entry.requestStart, {
      zeroUnavailable: true,
      zeroReason: opaqueReason,
    });
    const finalHeaders = observedNumber(entry.finalResponseHeadersStart, {
      zeroUnavailable: true,
      zeroReason: 'unsupported_or_no_observable_final_response_headers_start',
    });
    const interim = observedNumber(entry.firstInterimResponseStart, {
      zeroUnavailable: true,
      zeroReason: 'no_interim_response_or_property_unsupported',
    });
    const responseStart = observedNumber(entry.responseStart, {
      zeroUnavailable: true,
      zeroReason: opaqueReason,
    });
    const responseEnd = observedNumber(entry.responseEnd, {
      zeroUnavailable: true,
      zeroReason: 'response_not_completed_or_timing_unavailable',
    });
    let ttfb;
    if (requestStart.value === null) {
      ttfb = { value: null, basis: null, reason: requestStart.reason };
    } else if (finalHeaders.value !== null) {
      const value = durationObservation(
        finalHeaders,
        requestStart,
        'final_response_headers_or_request_start_unavailable',
      );
      ttfb = {
        value: value.value,
        basis:
          value.value === null
            ? null
            : 'finalResponseHeadersStart-requestStart',
        reason: value.reason,
      };
    } else if (responseStart.value !== null && interim.value === null) {
      const value = durationObservation(
        responseStart,
        requestStart,
        'response_or_request_start_unavailable',
      );
      ttfb = {
        value: value.value,
        basis:
          value.value === null
            ? null
            : 'responseStart-requestStart; no interim response observed',
        reason: value.reason,
      };
    } else {
      ttfb = {
        value: null,
        basis: null,
        reason: 'final_headers_unavailable_and_responseStart_may_be_interim',
      };
    }
    let download;
    if (interim.value !== null) {
      download = {
        value: null,
        basis: null,
        reason:
          'interim_response_makes_responseStart_an_invalid_download_boundary',
      };
    } else {
      const value = durationObservation(
        responseEnd,
        responseStart,
        'responseStart_or_responseEnd_unavailable',
      );
      download = {
        ...value,
        basis: value.value === null ? null : 'responseEnd-responseStart',
      };
    }
    const sizeReason = sameOrigin
      ? 'zero_may_be_empty_cache_or_unavailable; not coerced to a byte count'
      : 'zero_may_be_cross_origin_privacy_masking_cache_or_empty';
    const size = (raw) =>
      observedNumber(raw, {
        zeroUnavailable: true,
        zeroReason: sizeReason,
      });
    return {
      time_origin_epoch_ms: Number.isFinite(performance.timeOrigin)
        ? performance.timeOrigin
        : null,
      start_time_ms: start,
      fetch_start_ms: fetchStart,
      request_start_ms: requestStart,
      first_interim_response_start_ms: interim,
      final_response_headers_start_ms: finalHeaders,
      response_start_ms: responseStart,
      response_end_ms: responseEnd,
      start_epoch_ms:
        start.value === null || !Number.isFinite(performance.timeOrigin)
          ? null
          : performance.timeOrigin + start.value,
      response_end_epoch_ms:
        responseEnd.value === null || !Number.isFinite(performance.timeOrigin)
          ? null
          : performance.timeOrigin + responseEnd.value,
      ttfb_ms: ttfb,
      download_ms: download,
      response_header_end_ms: {
        value: null,
        reason: 'Resource Timing does not expose response header end',
      },
      source_bytes: {
        transfer_size: size(entry.transferSize),
        encoded_body_size: size(entry.encodedBodySize),
        decoded_body_size: size(entry.decodedBodySize),
      },
    };
  }

  function serverRequestId(entry) {
    const base = {
      state: null,
      value: null,
      value_sha256: null,
      source: null,
      distinct_value_count: 0,
      verification: 'unverified',
      matched_header_name: null,
      reason: null,
    };
    let metrics;
    try {
      metrics = entry.serverTiming;
    } catch {
      return {
        ...base,
        state: 'unavailable',
        reason: 'serverTiming_property_read_failed',
      };
    }
    if (!Array.isArray(metrics)) {
      return {
        ...base,
        state: 'unavailable',
        reason: 'serverTiming_property_unsupported',
      };
    }
    const ridMetrics = metrics.filter(
      (metric) => metric && typeof metric === 'object' && metric.name === 'rid',
    );
    if (ridMetrics.length === 0) {
      return {
        ...base,
        state: 'absent',
        reason: 'no_rid_metric_in_serverTiming',
      };
    }
    const valid = [
      ...new Set(
        ridMetrics
          .map((metric) => metric.description)
          .filter(
            (description) =>
              typeof description === 'string' &&
              SERVER_REQUEST_ID.test(description),
          ),
      ),
    ];
    if (valid.length === 1) {
      return {
        ...base,
        state: 'observed',
        value: valid[0],
        source: 'PerformanceResourceTiming.serverTiming rid description',
        distinct_value_count: 1,
      };
    }
    if (valid.length > 1) {
      return {
        ...base,
        state: 'ambiguous_multiple_distinct_values',
        distinct_value_count: valid.length,
        reason:
          'multiple rid metrics carried different server request IDs; none is authoritative',
      };
    }
    return {
      ...base,
      state: 'invalid_format',
      reason:
        'rid metric description did not match req_admin_<UUIDv7>; the description was not recorded',
    };
  }

  function attributeResourceEntry(candidate, record) {
    candidate.matched_source_entry_ids.add(record.source_entry_id);
    candidate.public_record.cache = { ...record.cache };
    if (candidate.public_record.response.streaming?.kind === 'sse') {
      candidate.public_record.response.streaming.closed_epoch_ms =
        record.native.response_end_epoch_ms;
      candidate.public_record.response.streaming.closed_monotonic_ms =
        record.native.response_end_ms.value;
      candidate.public_record.response.streaming.close_source =
        record.native.response_end_ms.value === null
          ? null
          : 'PerformanceResourceTiming.responseEnd';
    }
  }

  function xRequestIdHashes(request) {
    const headers = request.public_record.response?.correlation_headers;
    if (!Array.isArray(headers)) return new Set();
    return new Set(
      headers
        .filter((header) => header?.name === 'x-request-id')
        .map((header) => header.value_sha256)
        .filter((value) => typeof value === 'string'),
    );
  }

  function retractResourceEntry(candidate) {
    candidate.public_record.cache = { state: 'not_observed', basis: null };
    const streaming = candidate.public_record.response.streaming;
    if (
      streaming?.kind === 'sse' &&
      streaming.close_source === 'PerformanceResourceTiming.responseEnd'
    ) {
      streaming.closed_epoch_ms = null;
      streaming.closed_monotonic_ms = null;
      streaming.close_source = null;
    }
  }

  async function processResourceEntry(entry) {
    const processingStarted = performance.now();
    if (entry.entryType !== 'resource' || typeof entry.name !== 'string')
      return;
    if (
      Number.isFinite(state.installed_monotonic_ms) &&
      entry.startTime + URL_MATCH_EPSILON_MS < state.installed_monotonic_ms
    ) {
      return;
    }
    const entryEnd =
      Number.isFinite(entry.responseEnd) && entry.responseEnd > 0
        ? entry.responseEnd
        : Number.POSITIVE_INFINITY;
    const candidates = [...state.request_private.values()].filter(
      (candidate) => {
        if (candidate.raw_url !== entry.name) return false;
        if (candidate.call_monotonic_ms > entryEnd + URL_MATCH_EPSILON_MS) {
          return false;
        }
        if (
          candidate.settled_monotonic_ms !== null &&
          candidate.settled_monotonic_ms + URL_MATCH_EPSILON_MS <
            entry.startTime
        ) {
          return false;
        }
        return true;
      },
    );
    const serverRequestIdEvidence = serverRequestId(entry);
    const ridMetricPresent =
      serverRequestIdEvidence.state !== 'unavailable' &&
      serverRequestIdEvidence.state !== 'absent';
    if (candidates.length === 0 && !ridMetricPresent) return;
    if (serverRequestIdEvidence.state === 'observed') {
      const digest = await sha256Text(serverRequestIdEvidence.value);
      serverRequestIdEvidence.value_sha256 = digest.value;
      serverRequestIdEvidence.reason ??= digest.reason;
    }

    const entryId = uuid();
    const entryUrlHash = await sha256Text(entry.name);
    let parsed = null;
    try {
      parsed = new URL(entry.name);
    } catch {
      // The URL remains private; parse failure is represented below.
    }
    const sameOrigin = parsed
      ? parsed.origin === globalThis.location?.origin
      : null;
    const unique = candidates.length === 1;
    const record = addRecord({
      record_type: 'resource_timing',
      source_entry_id: entryId,
      candidate_request_ids: candidates.map(
        (candidate) => candidate.request_id,
      ),
      correlation_state:
        candidates.length === 0
          ? 'no_wrapped_fetch_candidate'
          : unique
            ? 'unique_url_time_window_candidate'
            : 'ambiguous_same_url_time_window',
      correlation_reason:
        candidates.length === 0
          ? 'no wrapped fetch shared the URL and time window; the entry is kept because a Server-Timing rid metric was present'
          : unique
            ? 'one wrapped fetch shared the URL and overlapped the native entry window'
            : 'Resource Timing exposes no request ID or method; concurrent/recent same-URL requests cannot be separated',
      server_request_id: serverRequestIdEvidence,
      initiator_type: entry.initiatorType || null,
      url_sha256: entryUrlHash.value,
      url_sha256_reason: entryUrlHash.reason,
      same_origin: sameOrigin,
      native: resourceTimingFields(entry, sameOrigin === true),
      native_status:
        'responseStatus' in entry
          ? statusObservation(entry.responseStatus)
          : { value: null, reason: 'responseStatus_property_unsupported' },
      cache: {
        state:
          'deliveryType' in entry && entry.deliveryType === 'cache'
            ? 'cache'
            : 'not_reported',
        basis:
          'deliveryType' in entry && entry.deliveryType === 'cache'
            ? 'PerformanceResourceTiming.deliveryType'
            : null,
        heuristic:
          entry.transferSize === 0 && entry.decodedBodySize > 0
            ? 'possible_cache_or_privacy_masking; not authoritative'
            : null,
      },
      captured_epoch_ms: Date.now(),
      captured_monotonic_ms: performance.now(),
      observer_processing_ms: performance.now() - processingStarted,
    });

    for (const candidate of candidates) {
      candidate.candidate_source_entry_ids.add(entryId);
      candidate.public_record.resource_timing_link.candidate_source_entry_ids =
        [...candidate.candidate_source_entry_ids];
    }
    if (unique) {
      const candidate = candidates[0];
      attributeResourceEntry(candidate, record);
      candidate.public_record.resource_timing_link = {
        state: 'unique_url_time_window_candidate',
        source_entry_ids: [...candidate.matched_source_entry_ids],
        candidate_source_entry_ids: [...candidate.candidate_source_entry_ids],
        reason:
          'not a protocol request ID; uniqueness is limited to wrapped fetch URL/time overlap',
      };
    }
  }

  function observerCallback(list, _observer, options) {
    if (options && Object.hasOwn(options, 'droppedEntriesCount')) {
      state.observer_dropped_entries_supported = true;
      if (Number.isFinite(options.droppedEntriesCount)) {
        state.observer_dropped_entries += options.droppedEntriesCount;
      }
    } else if (state.observer_dropped_entries_supported === null) {
      state.observer_dropped_entries_supported = false;
    }
    for (const entry of list.getEntries())
      trackTask(processResourceEntry(entry));
  }

  function installObserver() {
    if (
      typeof PerformanceObserver !== 'function' ||
      !Array.isArray(PerformanceObserver.supportedEntryTypes) ||
      !PerformanceObserver.supportedEntryTypes.includes('resource')
    ) {
      state.observer_error = 'PerformanceObserver resource entries unsupported';
      noticeOnce('resource_timing_observer_unavailable', state.observer_error);
      return;
    }
    let observer;
    try {
      observer = new PerformanceObserver(observerCallback);
    } catch (error) {
      state.observer_error = `resource observer construction failed: ${errorSummary(error).name}`;
      noticeOnce('resource_timing_observer_unavailable', state.observer_error);
      return;
    }
    try {
      observer.observe({ type: 'resource', buffered: true });
      state.observer_mode = 'type_resource_buffered';
    } catch {
      try {
        observer.observe({ entryTypes: ['resource'] });
        state.observer_mode = 'entryTypes_resource_future_only';
        state.observer_error =
          'buffered resource observation unsupported; only post-install entries are observed';
        noticeOnce(
          'resource_timing_buffered_unavailable',
          state.observer_error,
        );
      } catch (entryTypesError) {
        observer.disconnect();
        state.observer_error = `resource observer registration failed: ${errorSummary(entryTypesError).name}`;
        noticeOnce(
          'resource_timing_observer_unavailable',
          state.observer_error,
        );
        return;
      }
    }
    state.observer = observer;
  }

  function collectObserverRecords() {
    if (!state.observer) return;
    for (const entry of state.observer.takeRecords()) {
      trackTask(processResourceEntry(entry));
    }
  }

  function reconcileRequests() {
    const groups = new Map();
    for (const request of state.request_private.values()) {
      if (request.overlap_group_id) {
        const group = groups.get(request.overlap_group_id) ?? [];
        group.push(request);
        groups.set(request.overlap_group_id, group);
      }
      const link = request.public_record.resource_timing_link;
      if (
        link.candidate_source_entry_ids.length > 0 &&
        link.state !== 'unique_url_time_window_candidate' &&
        link.state !== 'unique_server_request_id_match'
      ) {
        link.state = 'ambiguous_same_url_time_window';
        link.reason =
          'Resource Timing entries overlapped this URL/time window but could not be attributed to one wrapped fetch';
      } else if (link.state === 'pending') {
        link.state =
          state.observer === null ? 'unavailable' : 'not_observed_in_snapshot';
        link.reason =
          state.observer_error ??
          (request.public_record.response.streaming?.kind === 'sse'
            ? 'SSE may remain open, so Resource Timing responseEnd may not exist in this observation window'
            : 'no Resource Timing entry was queued before snapshot');
      }
      if (request.public_record.cache.state === 'pending_resource_timing') {
        request.public_record.cache = {
          state: 'not_observed',
          basis: null,
        };
      }
    }
    for (const [groupId, requests] of groups) {
      const sourceEntries = new Set();
      for (const request of requests) {
        for (const entryId of request.candidate_source_entry_ids) {
          sourceEntries.add(entryId);
        }
      }
      const possibleReuse = sourceEntries.size < requests.length;
      for (const request of requests) {
        request.public_record.inflight_reuse = {
          state: possibleReuse ? 'possible_not_proven' : 'not_observable',
          reason: possibleReuse
            ? 'fewer Resource Timing candidates than overlapping same-URL fetch calls; cache/privacy/drop can produce the same symptom'
            : 'Resource Timing has no connection/request coalescing identifier',
          overlap_group_id: groupId,
          overlapping_fetch_count: requests.length,
          resource_timing_candidate_count: sourceEntries.size,
        };
      }
    }
    // Server request ID correlation: a Server-Timing rid description that
    // matches req_admin_<UUIDv7> is compared by SHA-256 against the observed
    // x-request-id response header (stored hashed, never raw). Attribution is
    // re-evaluated from current evidence on every reconcile: a fetch is
    // promoted over the URL/time guess only while exactly one of its
    // candidate entries carries its ID, a candidate entry whose observed ID
    // differs from the fetch x-request-id is retracted even without a
    // replacement, and a reused ID on several candidate entries or several
    // candidate fetches stays ambiguous. Other correlation headers (CF-Ray,
    // traceparent, ...) never contradict an attribution.
    const requestsById = new Map(
      [...state.request_private.values()].map((request) => [
        request.request_id,
        request,
      ]),
    );
    const xRequestIdHashesByRequestId = new Map(
      [...state.request_private.values()].map((request) => [
        request.request_id,
        xRequestIdHashes(request),
      ]),
    );
    const entriesById = new Map();
    for (const record of state.records) {
      if (
        record.record_type === 'resource_timing' &&
        typeof record.source_entry_id === 'string'
      ) {
        entriesById.set(record.source_entry_id, record);
      }
    }
    // Reset prior verification on entries that are candidates of live
    // requests so a late-arriving entry demotes an earlier conclusion instead
    // of accumulating on top of it.
    for (const request of state.request_private.values()) {
      for (const entryId of request.candidate_source_entry_ids) {
        const evidence = entriesById.get(entryId)?.server_request_id;
        if (
          evidence?.state === 'observed' &&
          typeof evidence.value_sha256 === 'string'
        ) {
          evidence.verification = 'unverified';
          evidence.matched_header_name = null;
          evidence.reason = null;
        }
      }
    }
    const entryMatches = new Map();
    for (const record of entriesById.values()) {
      const evidence = record.server_request_id;
      if (
        !evidence ||
        evidence.state !== 'observed' ||
        typeof evidence.value_sha256 !== 'string'
      ) {
        continue;
      }
      const matches = (record.candidate_request_ids ?? [])
        .map((requestId) => requestsById.get(requestId))
        .filter(
          (request) =>
            request !== undefined &&
            xRequestIdHashesByRequestId
              .get(request.request_id)
              ?.has(evidence.value_sha256),
        );
      entryMatches.set(record.source_entry_id, matches);
      if (matches.length > 1) {
        evidence.verification =
          'ambiguous_same_id_on_multiple_candidate_fetches';
        evidence.reason =
          'the same server request ID matched several candidate fetches; a reused ID cannot prove a fresh server execution';
      }
    }
    for (const request of state.request_private.values()) {
      const candidateEntries = [...request.candidate_source_entry_ids]
        .map((entryId) => entriesById.get(entryId))
        .filter((record) => record !== undefined);
      if (candidateEntries.length === 0) continue;
      const link = request.public_record.resource_timing_link;
      const xRequestIds =
        xRequestIdHashesByRequestId.get(request.request_id) ?? new Set();
      const idMatched = candidateEntries.filter((record) => {
        const matches = entryMatches.get(record.source_entry_id);
        return matches?.length === 1 && matches[0] === request;
      });
      const uncontradicted = candidateEntries.filter((record) => {
        const evidence = record.server_request_id;
        const contradicted =
          evidence?.state === 'observed' &&
          typeof evidence.value_sha256 === 'string' &&
          xRequestIds.size > 0 &&
          !xRequestIds.has(evidence.value_sha256);
        if (contradicted) {
          if (evidence.verification === 'unverified') {
            evidence.verification =
              'server_request_id_contradicts_url_time_candidate';
            evidence.reason =
              'this entry carried a different server request ID than the fetch x-request-id; the earlier URL/time guess was retracted';
          }
          return false;
        }
        return true;
      });
      const nextMatched = new Set();
      if (idMatched.length === 1) {
        nextMatched.add(idMatched[0].source_entry_id);
      } else if (idMatched.length === 0) {
        for (const record of uncontradicted) {
          if (request.matched_source_entry_ids.has(record.source_entry_id)) {
            nextMatched.add(record.source_entry_id);
          }
        }
      }
      // idMatched.length > 1: the ID is reused across candidate entries, so
      // no entry is authoritative and every prior attribution is dropped.
      request.matched_source_entry_ids = nextMatched;
      link.source_entry_ids = [...nextMatched];
      link.candidate_source_entry_ids = [...request.candidate_source_entry_ids];
      if (idMatched.length === 1) {
        const matchedEvidence = idMatched[0].server_request_id;
        matchedEvidence.verification = 'matched_fetch_correlation_header';
        matchedEvidence.matched_header_name = 'x-request-id';
        matchedEvidence.reason =
          'the only candidate entry carrying this server request ID; the ID is a backend join key, not proof of a fresh server execution';
        link.state = 'unique_server_request_id_match';
        link.reason =
          'the Server-Timing rid matched this fetch x-request-id and no other candidate entry carried that ID; the ID is a backend join key, not proof of a fresh server execution';
      } else if (idMatched.length > 1) {
        for (const record of idMatched) {
          record.server_request_id.verification =
            'ambiguous_same_id_on_multiple_candidate_entries';
          record.server_request_id.reason =
            'the same server request ID appeared on several candidate entries; a reused ID cannot prove a fresh server execution';
        }
        link.state = 'ambiguous_same_url_time_window';
        link.reason =
          'the same server request ID appeared on several candidate entries; a reused ID cannot prove a fresh server execution';
      } else if (nextMatched.size === 1) {
        link.state = 'unique_url_time_window_candidate';
        link.reason =
          'not a protocol request ID; uniqueness is limited to wrapped fetch URL/time overlap';
      } else if (nextMatched.size > 1 || uncontradicted.length > 0) {
        link.state = 'ambiguous_same_url_time_window';
        link.reason =
          uncontradicted.length < candidateEntries.length
            ? 'candidate entries carrying a different server request ID than the fetch x-request-id were retracted; the remaining candidates cannot be attributed to one wrapped fetch'
            : 'Resource Timing entries overlapped this URL/time window but could not be attributed to one wrapped fetch';
      } else {
        link.state = 'not_observed_in_snapshot';
        link.reason =
          'every candidate Resource Timing entry carried a server request ID that contradicts the fetch x-request-id; earlier URL/time guesses were retracted';
      }
      if (nextMatched.size === 1) {
        attributeResourceEntry(request, entriesById.get([...nextMatched][0]));
      } else {
        retractResourceEntry(request);
      }
    }
    for (const record of entriesById.values()) {
      const evidence = record.server_request_id;
      if (
        !evidence ||
        evidence.state !== 'observed' ||
        typeof evidence.value_sha256 !== 'string' ||
        evidence.verification !== 'unverified'
      ) {
        continue;
      }
      const candidates = record.candidate_request_ids ?? [];
      if (candidates.length === 0) {
        evidence.verification = 'no_wrapped_fetch_candidate';
        evidence.reason =
          'no wrapped fetch shared the URL/time window; the ID is preserved as unlinked backend join evidence';
      } else {
        const anyHeader = candidates.some(
          (requestId) =>
            (xRequestIdHashesByRequestId.get(requestId)?.size ?? 0) > 0,
        );
        evidence.verification = 'no_candidate_correlation_header_match';
        evidence.reason = anyHeader
          ? 'no candidate fetch x-request-id matched this server request ID'
          : 'no candidate fetch observed an x-request-id correlation header to compare against';
      }
    }
  }

  function validateOracle(value) {
    if (!value || typeof value !== 'object' || Array.isArray(value)) {
      throw new TypeError('UI oracle must be an object');
    }
    const reason = value.reason ?? null;
    if (
      reason !== null &&
      (typeof reason !== 'string' || reason.length > 240)
    ) {
      throw new TypeError(
        'UI oracle reason must be null or at most 240 characters',
      );
    }
    const observer = requiredString(
      value.observer,
      SOURCE_ID,
      'UI oracle observer',
    );
    if (!ORACLE_OBSERVERS.has(observer)) {
      throw new TypeError(`unsupported UI oracle observer: ${observer}`);
    }
    const visibility = value.visibility ?? 'unknown';
    if (!ORACLE_VISIBILITY.has(visibility)) {
      throw new TypeError('UI oracle visibility is invalid');
    }
    return {
      oracle_id: requiredString(
        value.oracle_id,
        SOURCE_ID,
        'UI oracle oracle_id',
      ),
      observer,
      visibility,
      expected_state_sha256: optionalHash(
        value.expected_state_sha256,
        'UI oracle expected_state_sha256',
      ),
      observed_state_sha256: optionalHash(
        value.observed_state_sha256,
        'UI oracle observed_state_sha256',
      ),
      screen_sha256: optionalHash(
        value.screen_sha256,
        'UI oracle screen_sha256',
      ),
      reason,
    };
  }

  function beginAction(descriptorValue) {
    ensureInstalled();
    if (state.active_action) {
      throw new Error(
        `action ${state.active_action.action_id} is still active`,
      );
    }
    const descriptor = normalizeDescriptor(descriptorValue, 'action');
    const clock = nowClock();
    const actionId = uuid();
    const record = addRecord({
      record_type: 'action_window',
      action_id: actionId,
      descriptor: publicDescriptor(descriptor),
      begin: clock,
      end: null,
      ui_oracle: null,
      request_ids: [],
      recorder_assessment: null,
    });
    state.active_action = {
      action_id: actionId,
      descriptor,
      public_record: record,
    };
    state.actions.set(actionId, state.active_action);
    return actionId;
  }

  async function endAction(oracleValue) {
    ensureInstalled();
    if (!state.active_action) throw new Error('no action is active');
    const oracle = validateOracle(oracleValue);
    const action = state.active_action;
    action.public_record.end = nowClock();
    action.public_record.ui_oracle = {
      ...oracle,
      supplied_epoch_ms: action.public_record.end.epoch_ms,
      supplied_monotonic_ms: action.public_record.end.monotonic_ms,
      render_observation_ms:
        action.public_record.end.monotonic_ms -
        action.public_record.begin.monotonic_ms,
      measurement_layer:
        'UI oracle/action window; separate from Resource Timing network fields',
    };
    action.public_record.recorder_assessment =
      'not_generated; equality and PASS/FAIL belong to the Main QA oracle';
    state.active_action = null;
    await flush();
    return jsonCopy(action.public_record);
  }

  function independentRead(descriptorValue) {
    ensureInstalled();
    const descriptor = normalizeDescriptor(descriptorValue, 'direct');
    const clock = nowClock();
    const readId = uuid();
    const directRecord = addRecord({
      record_type: 'independent_read',
      independent_read_id: readId,
      descriptor: publicDescriptor(descriptor),
      started: clock,
      request_id: null,
      safety: {
        same_origin_http_only: true,
        production_read_opt_in: descriptor.direct_safety.allow_production_read,
        read_safety: descriptor.direct_safety.read_safety,
        known_read_with_audit_opt_in:
          descriptor.direct_safety.allow_known_read_with_audit,
        permitted_methods: ['GET', 'HEAD'],
        mutation_replay_supported: false,
        fixture_restore_owner: 'FullQaFixtureOwner',
      },
    });
    const context = {
      capture_origin: 'independent_read',
      action_id: null,
      independent_read_id: readId,
      descriptor,
      action_record: null,
      direct_record: directRecord,
    };
    const invocation = invokeFetch(
      globalThis,
      [descriptor.request.input, descriptor.request.init],
      context,
    );
    const evidence = (async () => {
      if (invocation.private_record === null) {
        await flush();
        return {
          request_id: null,
          records: [jsonCopy(directRecord)],
          blocker: 'fetch_started_but_observer_registration_failed',
        };
      }
      await invocation.private_record.monitor_task;
      await flush();
      return evidenceForRequest(invocation.private_record.request_id);
    })();
    return {
      independent_read_id: readId,
      request_id: invocation.private_record?.request_id ?? null,
      response: invocation.promise,
      evidence,
    };
  }

  function evidenceForRequest(requestId) {
    const records = state.records.filter(
      (record) =>
        record.request_id === requestId ||
        record.candidate_request_ids?.includes(requestId),
    );
    return jsonCopy({ request_id: requestId, records });
  }

  async function flush() {
    ensureInstalled();
    collectObserverRecords();
    while (state.pending_tasks.size > 0) {
      await Promise.all([...state.pending_tasks]);
      collectObserverRecords();
    }
    reconcileRequests();
  }

  function capabilities() {
    const resourcePrototype = globalThis.PerformanceResourceTiming?.prototype;
    const has = (name) =>
      Boolean(resourcePrototype && name in resourcePrototype);
    return {
      browser_user_agent_sha256: {
        value: null,
        reason:
          'user agent is not collected because it is not required evidence',
      },
      crypto_random_uuid: typeof globalThis.crypto?.randomUUID === 'function',
      crypto_subtle_sha256:
        typeof globalThis.crypto?.subtle?.digest === 'function',
      performance_time_origin: Number.isFinite(performance.timeOrigin),
      performance_observer: typeof PerformanceObserver === 'function',
      performance_observer_supported_entry_types:
        typeof PerformanceObserver === 'function' &&
        Array.isArray(PerformanceObserver.supportedEntryTypes)
          ? [...PerformanceObserver.supportedEntryTypes]
          : null,
      resource_timing_observer_mode: state.observer_mode,
      resource_timing_observer_error: state.observer_error,
      observer_dropped_entries_count:
        state.observer_dropped_entries_supported === true
          ? 'supported'
          : state.observer_dropped_entries_supported === false
            ? 'not_reported_by_callback'
            : 'unknown_until_callback',
      resource_timing_properties: {
        deliveryType: has('deliveryType'),
        responseStatus: has('responseStatus'),
        contentType: has('contentType'),
        firstInterimResponseStart: has('firstInterimResponseStart'),
        finalResponseHeadersStart: has('finalResponseHeadersStart'),
        transferSize: has('transferSize'),
        encodedBodySize: has('encodedBodySize'),
        decodedBodySize: has('decodedBodySize'),
        serverTiming: has('serverTiming'),
      },
      fetch_wrapper: {
        installed: state.installed && globalThis.fetch === state.wrapped_fetch,
        returns_exact_native_promise: true,
        returns_exact_native_response: true,
        attaches_observer_reactions: true,
        caveat:
          'attaching a rejection reaction can affect unhandledrejection reporting; fetch fulfillment/rejection, Promise identity, and Response identity are not replaced',
      },
      response_body_observer: {
        clone_cap_bytes: state.max_body_bytes,
        requires_content_length: true,
        sse_clone_or_read: false,
        caveat:
          'a bounded Response.clone plus hashing can add CPU, allocation, tee buffering, and microtask work; per-record observer_overhead_ms is the script cost visible on this thread, not total browser cost',
      },
      resource_timing_authority:
        'native Resource Timing owns network TTFB/download/source-byte fields; JavaScript fetch settle latency is never copied into them',
      request_id_authority:
        'crypto.randomUUID per wrapped fetch; a Server-Timing rid description matching req_admin_<UUIDv7> is recorded verbatim and compared by SHA-256 against observed response correlation headers; a unique same-ID candidate promotes the link, while a reused or unmatched ID stays a candidate and never proves a fresh server execution',
    };
  }

  function incompleteReasons() {
    const reasons = [];
    for (const code of [
      'observer_task_failed',
      'fetch_observer_registration_failed',
      'fetch_observer_monitor_failed',
    ])
      if (state.notices.has(code)) reasons.push(code);
    if (state.overflow.dropped_records > 0)
      reasons.push('bounded_record_ring_overflow');
    if (state.observer_dropped_entries > 0)
      reasons.push('performance_observer_dropped_entries');
    if (state.observer === null)
      reasons.push('resource_timing_observer_unavailable');
    if (state.active_action) reasons.push('action_window_still_open');
    if (
      [...state.request_private.values()].some(
        (request) => request.settled_monotonic_ms === null,
      )
    ) {
      reasons.push('wrapped_fetch_promise_still_pending');
    }
    if (
      [...state.request_private.values()].some(
        (request) =>
          request.public_record.response.streaming?.kind === 'sse' &&
          request.public_record.response.streaming.closed_epoch_ms === null,
      )
    ) {
      reasons.push('sse_close_or_resource_timing_not_observed');
    }
    return reasons;
  }

  async function snapshot() {
    ensureInstalled();
    await flush();
    const clock = nowClock();
    const reasons = incompleteReasons();
    return jsonCopy({
      schema_version: SCHEMA_VERSION,
      collector_version: COLLECTOR_VERSION,
      installed: state.installed,
      capabilities: capabilities(),
      capture: {
        complete: reasons.length === 0,
        incomplete_reasons: reasons,
        overflow: { ...state.overflow },
        observer_dropped_entries: state.observer_dropped_entries,
      },
      clocks: {
        time_origin_epoch_ms: clock.time_origin_epoch_ms,
        captured_epoch_ms: clock.epoch_ms,
        captured_monotonic_ms: clock.monotonic_ms,
      },
      records: state.records,
      interpretation_limits: [
        'Exact value equality does not prove copied or fabricated evidence.',
        'The same request_id or source_entry_id reused across UI and direct observations is invalid; a URL/time candidate link is not a protocol correlation ID.',
        'Missing native timestamps, ambiguous concurrent same-URL candidates, observer drops, and ring overflow make the affected evidence incomplete.',
        'Resource Timing does not expose response header end, connection reuse identity, HTTP cache key, or in-flight request coalescing identity.',
        'A zero Resource Timing field can mean cache, cancellation, privacy masking, an empty body, or unsupported data; ambiguous zeroes remain null with a reason.',
        'UI render observation is supplied by the Main agent and is never converted into recorder PASS/FAIL.',
        'A Server-Timing rid description carries the server-generated request ID (req_admin_<UUIDv7>), not a time value; the metric duration is not a latency measurement and is never read as one.',
        'A matched server_request_id is a join key to backend evidence, not proof of a fresh server execution; a cached or reused ID stays ambiguous, and backend matching belongs to the outer QA stage.',
      ],
    });
  }

  async function drain() {
    const result = await snapshot();
    const retainedRequestIds = new Set();
    for (const request of state.request_private.values()) {
      const pending = request.settled_monotonic_ms === null;
      const openSse =
        request.public_record.response.streaming?.kind === 'sse' &&
        request.public_record.response.streaming.closed_epoch_ms === null;
      if (pending || openSse) {
        retainedRequestIds.add(request.request_id);
        request.public_record.retained_after_drain = true;
        request.public_record.retained_reason = pending
          ? 'fetch_promise_still_pending'
          : 'sse_close_or_resource_timing_pending';
      }
    }
    state.records = state.records.filter((record) => {
      if (
        record.record_type === 'fetch_observation' &&
        retainedRequestIds.has(record.request_id)
      ) {
        return true;
      }
      if (
        record.record_type === 'independent_read' &&
        retainedRequestIds.has(record.request_id)
      ) {
        return true;
      }
      if (record.record_type === 'action_window') {
        return (
          record === state.active_action?.public_record ||
          record.request_ids.some((requestId) =>
            retainedRequestIds.has(requestId),
          )
        );
      }
      return false;
    });
    state.overflow = {
      dropped_records: 0,
      first_dropped_sequence: null,
      last_dropped_sequence: null,
    };
    state.notices.clear();
    for (const [actionId, action] of state.actions) {
      if (action !== state.active_action) state.actions.delete(actionId);
    }
    for (const [requestId, request] of state.request_private) {
      if (
        request.settled_monotonic_ms !== null &&
        !retainedRequestIds.has(requestId)
      ) {
        state.request_private.delete(requestId);
      }
    }
    return result;
  }

  function install(options = {}) {
    if (!options || typeof options !== 'object' || Array.isArray(options)) {
      throw new TypeError('install options must be an object');
    }
    const maxRecords = finiteInteger(
      options.max_records ?? state.max_records,
      'max_records',
      32,
      MAX_MAX_RECORDS,
    );
    const maxBodyBytes = finiteInteger(
      options.max_body_bytes ?? state.max_body_bytes,
      'max_body_bytes',
      0,
      MAX_MAX_BODY_BYTES,
    );
    state.max_records = maxRecords;
    state.max_body_bytes = maxBodyBytes;
    while (state.records.length > state.max_records) {
      const dropped = state.records.shift();
      state.overflow.dropped_records += 1;
      state.overflow.first_dropped_sequence ??= dropped.sequence;
      state.overflow.last_dropped_sequence = dropped.sequence;
    }
    if (state.installed) return capabilities();
    if (typeof globalThis.crypto?.randomUUID !== 'function') {
      throw new Error(
        'crypto.randomUUID is required; recorder was not installed',
      );
    }
    state.installed_epoch_ms = Date.now();
    state.installed_monotonic_ms = performance.now();
    installFetchWrapper();
    state.installed = true;
    installObserver();
    addRecord({
      record_type: 'collector_notice',
      code: 'installed',
      detail: {
        installed_epoch_ms: state.installed_epoch_ms,
        installed_monotonic_ms: state.installed_monotonic_ms,
        observer_mode: state.observer_mode,
      },
    });
    return capabilities();
  }

  async function uninstall() {
    if (!state.installed) {
      return {
        installed: false,
        fetch_restored: true,
        observer_disconnected: true,
      };
    }
    await flush();
    let fetchRestored = false;
    let fetchRestoreReason = null;
    if (globalThis.fetch === state.wrapped_fetch) {
      if (state.fetch_had_own_property) {
        Object.defineProperty(
          globalThis,
          'fetch',
          state.original_fetch_own_descriptor,
        );
      } else {
        delete globalThis.fetch;
      }
      fetchRestored = globalThis.fetch === state.original_fetch;
      if (!fetchRestored) fetchRestoreReason = 'restored_fetch_value_mismatch';
    } else {
      fetchRestoreReason = 'global_fetch_changed_after_recorder_install';
    }
    if (state.observer) state.observer.disconnect();
    state.observer = null;
    state.installed = false;
    state.request_private.clear();
    state.actions.clear();
    state.pending_tasks.clear();
    state.active_action = null;
    return {
      installed: false,
      fetch_restored: fetchRestored,
      fetch_restore_reason: fetchRestoreReason,
      observer_disconnected: true,
    };
  }

  function ensureInstalled() {
    if (!state.installed) throw new Error('recorder is not installed');
  }

  const api = Object.freeze({
    collector_version: COLLECTOR_VERSION,
    schema_version: SCHEMA_VERSION,
    get installed() {
      return state.installed;
    },
    install,
    uninstall,
    beginAction,
    endAction,
    independentRead,
    snapshot,
    drain,
  });
  Object.defineProperty(globalThis, GLOBAL_KEY, {
    value: api,
    configurable: true,
    enumerable: false,
    writable: false,
  });
  return api.install();
})();
