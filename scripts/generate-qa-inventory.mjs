import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const repoRoot = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  '..',
);
const contractsDir = process.env.CC_LB_QA_CONTRACTS_DIR;
const outputDir = process.env.CC_LB_QA_OUTPUT_DIR;
if (!contractsDir || !outputDir) {
  console.error(
    'QA inventory requires CC_LB_QA_CONTRACTS_DIR and CC_LB_QA_OUTPUT_DIR.',
  );
  process.exit(1);
}
const outputPaths = {
  yaml: path.join(outputDir, 'api-query-inventory.yaml'),
  markdown: path.join(outputDir, 'api-query-inventory.md'),
  reconciliation: path.join(outputDir, 'source-reconciliation.json'),
};
const contractPaths = {
  ui: path.join(contractsDir, 'ui.json'),
  read: path.join(contractsDir, 'api-read.json'),
  write: path.join(contractsDir, 'api-write.json'),
  deployedDelta: path.join(contractsDir, 'deployed-delta.json'),
};
const checkOnly = process.argv.includes('--check');
const RISK_TIERS = new Set([
  'read',
  'read_with_audit',
  'reversible_write',
  'destructive_write',
  'external_action',
]);
const HTTP_METHODS = new Set([
  'GET',
  'POST',
  'PUT',
  'PATCH',
  'DELETE',
  'HEAD',
  'OPTIONS',
]);
const failures = [];

function fail(message) {
  failures.push(message);
}

function readJson(file, label) {
  if (!fs.existsSync(file)) {
    fail(`${label} is missing: ${path.relative(repoRoot, file)}`);
    return null;
  }
  try {
    return JSON.parse(fs.readFileSync(file, 'utf8'));
  } catch (error) {
    fail(`${label} is not valid JSON: ${error.message}`);
    return null;
  }
}

function sha256(value) {
  return crypto.createHash('sha256').update(value).digest('hex');
}

function stableId(prefix, value) {
  return `${prefix}-${sha256(value).slice(0, 12).toUpperCase()}`;
}

function normalizeWirePath(value) {
  return value
    .replaceAll('${principal_id}', '{principal_id}')
    .replaceAll('${principalId}', '{principal_id}')
    .replaceAll('${session_id}', '{session_id}')
    .replaceAll('${sessionId}', '{session_id}')
    .replaceAll('${key_id}', '{key_id}')
    .replaceAll('${eventId}', '{event_id}')
    .replaceAll('{eventId}', '{event_id}')
    .replaceAll('${id}', '{id}')
    .replace(/\$\{([A-Za-z_][A-Za-z0-9_]*)\}/g, '{$1}');
}

function routeShape(value) {
  return normalizeWirePath(value).replace(/\{[^}]+\}/g, '{}');
}

function routeKey(method, routePath) {
  return `${method.toUpperCase()} ${routeShape(routePath)}`;
}

function relativeFile(file) {
  return path.relative(repoRoot, file).split(path.sep).join('/');
}

function walkFiles(root, predicate) {
  const result = [];
  function walk(current) {
    for (const entry of fs.readdirSync(current, { withFileTypes: true })) {
      const absolute = path.join(current, entry.name);
      if (entry.isDirectory()) walk(absolute);
      else if (predicate(absolute, entry.name))
        result.push(relativeFile(absolute));
    }
  }
  walk(root);
  return result.sort();
}

function isProductionUiSource(file, name) {
  const normalized = file.split(path.sep).join('/');
  if (name.includes('.test.')) return false;
  if (normalized.includes('/__tests__/')) return false;
  if (normalized.includes('/__fixtures__/')) return false;
  if (normalized.includes('/test-utils/')) return false;
  if (!(name.endsWith('.ts') || name.endsWith('.tsx'))) return false;
  if (name.endsWith('.d.ts') || name.endsWith('.gen.ts')) return false;
  return true;
}

function discoverUiSources() {
  const root = path.join(repoRoot, 'crates/cc-lb-admin/web/src');
  return walkFiles(root, isProductionUiSource).map((file) => ({
    path: file,
    sha256: sha256(fs.readFileSync(path.join(repoRoot, file))),
  }));
}

function discoverUiRoutes(sourceFiles) {
  return sourceFiles
    .filter(({ path: file }) =>
      /^crates\/cc-lb-admin\/web\/src\/routes\/[^/]+\.tsx$/.test(file),
    )
    .map(({ path: file, sha256: hash }) => {
      const text = fs.readFileSync(path.join(repoRoot, file), 'utf8');
      const match = text.match(/createFileRoute\(['"]([^'"]+)['"]\)/);
      return {
        path: file,
        route: match?.[1] ?? (file.endsWith('/__root.tsx') ? '__root' : null),
        sha256: hash,
      };
    });
}

function lineNumberAt(text, index) {
  return text.slice(0, index).split('\n').length;
}

function routeCallAt(text, routeIndex) {
  const open = text.indexOf('(', routeIndex);
  if (open < 0) return null;
  let depth = 0;
  let quote = null;
  let escaped = false;
  for (let index = open; index < text.length; index += 1) {
    const char = text[index];
    if (quote) {
      if (escaped) escaped = false;
      else if (char === '\\') escaped = true;
      else if (char === quote) quote = null;
      continue;
    }
    if (char === '"' || char === "'") {
      quote = char;
      continue;
    }
    if (char === '(') depth += 1;
    if (char === ')') {
      depth -= 1;
      if (depth === 0) return text.slice(open + 1, index);
    }
  }
  return null;
}

function scanRouteRegistrations(text, file, baseLine, routes) {
  let offset = 0;
  while (true) {
    const index = text.indexOf('.route', offset);
    if (index < 0) break;
    offset = index + 6;
    const call = routeCallAt(text, index);
    if (!call) continue;
    const pathMatch = call.match(/^\s*"([^"]+)"\s*,/);
    if (!pathMatch) continue;
    const methods = [
      ...call.matchAll(
        /(?:^|[.\s])(get|post|put|patch|delete|head|options)\s*\(/g,
      ),
    ].map((match) => match[1].toUpperCase());
    for (const method of new Set(methods)) {
      routes.push({
        method,
        path: pathMatch[1],
        source: { file, line: baseLine + lineNumberAt(text, index) - 1 },
      });
    }
  }
}

function extractRustFunction(text, name) {
  const match = new RegExp(`\\bfn\\s+${name}\\s*\\(`).exec(text);
  if (!match) return null;
  const open = text.indexOf('{', match.index);
  if (open < 0) return null;
  let depth = 0;
  let quote = false;
  let escaped = false;
  for (let index = open; index < text.length; index += 1) {
    const char = text[index];
    if (quote) {
      if (escaped) escaped = false;
      else if (char === '\\') escaped = true;
      else if (char === '"') quote = false;
      continue;
    }
    if (char === '"') {
      quote = true;
      continue;
    }
    if (char === '{') depth += 1;
    if (char === '}') {
      depth -= 1;
      if (depth === 0) {
        return {
          text: text.slice(open + 1, index),
          startLine: lineNumberAt(text, open + 1),
        };
      }
    }
  }
  return null;
}

function discoverBackendRoutes() {
  const adminRoot = path.join(repoRoot, 'crates/cc-lb-admin/src');
  const adminRustFiles = walkFiles(adminRoot, (_file, name) =>
    name.endsWith('.rs'),
  );
  const routes = [];
  for (const file of adminRustFiles) {
    const fullText = fs.readFileSync(path.join(repoRoot, file), 'utf8');
    const testBoundary = fullText.search(/\n#\[cfg\(test\)\]/);
    const text = testBoundary >= 0 ? fullText.slice(0, testBoundary) : fullText;
    scanRouteRegistrations(text, file, 1, routes);
  }

  const serverFile = 'crates/cc-lb-server/src/app.rs';
  const serverText = fs.readFileSync(path.join(repoRoot, serverFile), 'utf8');
  const adminMount = extractRustFunction(serverText, 'admin_router');
  if (!adminMount) {
    fail(`${serverFile} no longer defines admin_router`);
  } else {
    const localRouters = [
      ...adminMount.text.matchAll(/\.merge\(\s*([A-Za-z_][A-Za-z0-9_]*)\s*\(/g),
    ].map((match) => match[1]);
    const visited = new Set();
    const queue = [...localRouters];
    while (queue.length > 0) {
      const functionName = queue.shift();
      if (visited.has(functionName)) continue;
      visited.add(functionName);
      const body = extractRustFunction(serverText, functionName);
      if (!body) {
        fail(
          `${serverFile} admin_router merges unresolved local router function ${functionName}`,
        );
        continue;
      }
      scanRouteRegistrations(body.text, serverFile, body.startLine, routes);
      for (const match of body.text.matchAll(
        /\.merge\(\s*([A-Za-z_][A-Za-z0-9_]*)\s*\(/g,
      )) {
        if (!visited.has(match[1])) queue.push(match[1]);
      }
    }
  }
  return routes.sort((a, b) =>
    routeKey(a.method, a.path).localeCompare(routeKey(b.method, b.path)),
  );
}

function requiredFields(value, fields, context) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    fail(`${context} must be an object`);
    return;
  }
  for (const field of fields) {
    if (!(field in value)) fail(`${context}.${field} is required`);
  }
}

function requireString(value, context) {
  if (typeof value !== 'string' || value.length === 0)
    fail(`${context} must be a non-empty string`);
}

function validateSourceState(contract, label, reference) {
  if (!contract) return;
  const state = contract.source_state;
  const fields = ['kind', 'base_commit', 'note'];
  requiredFields(state, fields, `${label}.source_state`);
  if (!state || typeof state !== 'object' || Array.isArray(state)) return;
  for (const field of fields) {
    requireString(state[field], `${label}.source_state.${field}`);
    if (reference && state[field] !== reference[field])
      fail(`${label}.source_state.${field} differs from the UI source state`);
  }
  if (!/^[0-9a-f]{40}$/.test(state.base_commit ?? ''))
    fail(`${label}.source_state.base_commit must be a full 40-character hexadecimal commit`);
  if (!/^[0-9a-f]{7,40}$/.test(contract.source_commit ?? ''))
    fail(`${label}.source_commit must contain 7 to 40 hexadecimal characters`);
  if (
    typeof state.base_commit === 'string' &&
    typeof contract.source_commit === 'string' &&
    !state.base_commit.startsWith(contract.source_commit)
  )
    fail(`${label}.source_state.base_commit does not match its source_commit`);
}

function requireStringArray(value, context) {
  if (!Array.isArray(value) || value.some((item) => typeof item !== 'string')) {
    fail(`${context} must be an array of strings`);
  }
}
function parameterNames(value, context) {
  if (!Array.isArray(value)) {
    fail(`${context} must be an array`);
    return [];
  }
  return value.map((parameter, index) => {
    if (typeof parameter === 'string') return parameter;
    if (
      !parameter ||
      typeof parameter !== 'object' ||
      Array.isArray(parameter)
    ) {
      fail(
        `${context}[${index}] must be a parameter name or descriptor object`,
      );
      return '';
    }
    requireString(parameter.name, `${context}[${index}].name`);
    return parameter.name;
  });
}

function validateSourceReference(source, context, sourceManifest) {
  requiredFields(source, ['file', 'symbol', 'line'], context);
  if (!source || typeof source !== 'object') return;
  requireString(source.file, `${context}.file`);
  requireString(source.symbol, `${context}.symbol`);
  if (!Number.isInteger(source.line) || source.line < 1)
    fail(`${context}.line must be a positive integer`);
  const absolute = path.join(repoRoot, source.file ?? '');
  if (!fs.existsSync(absolute)) {
    fail(`${context}.file does not exist: ${source.file}`);
    return;
  }
  if (!sourceManifest.has(source.file))
    fail(
      `${context}.file is outside the UI production-source denominator: ${source.file}`,
    );
  const text = fs.readFileSync(absolute, 'utf8');
  const lines = text.split('\n');
  if (source.line > lines.length)
    fail(
      `${context}.line ${source.line} exceeds ${source.file} line count ${lines.length}`,
    );
  if (!text.includes(source.symbol))
    fail(`${context}.symbol was not found in ${source.file}: ${source.symbol}`);
}

function validateUi(ui, discoveredSources, discoveredRoutes) {
  if (!ui) return;
  requiredFields(
    ui,
    [
      'schema_version',
      'source_commit',
      'metadata',
      'source_files',
      'route_denominator',
      'coverage_dimensions',
      'legacy_id_mapping',
      'items',
      'gaps',
    ],
    'ui',
  );
  if (ui.schema_version !== 1) fail('ui.schema_version must be 1');
  requireString(ui.source_commit, 'ui.source_commit');
  requiredFields(
    ui.metadata,
    [
      'source_reconciled',
      'runtime_status',
      'production_applicability',
      'static_catalog_limitations',
    ],
    'ui.metadata',
  );
  if (ui.metadata.canonical_action_count !== (ui.items ?? []).length)
    fail('ui.metadata.canonical_action_count does not match items.length');
  const canonicalRequestCount = (ui.items ?? []).reduce(
    (sum, item) => sum + (item.requests?.length ?? 0),
    0,
  );
  if (ui.metadata.canonical_request_occurrence_count !== canonicalRequestCount)
    fail(
      'ui.metadata.canonical_request_occurrence_count does not match atomic request occurrences',
    );
  if (ui.metadata.runtime_status !== 'runtime_pending')
    fail(
      'ui.metadata.runtime_status must remain runtime_pending until browser execution',
    );
  requireStringArray(
    ui.metadata.static_catalog_limitations,
    'ui.metadata.static_catalog_limitations',
  );

  const expectedSources = JSON.stringify(discoveredSources);
  if (JSON.stringify(ui.source_files) !== expectedSources) {
    const expected = new Map(
      discoveredSources.map((entry) => [entry.path, entry.sha256]),
    );
    const actual = new Map(
      (ui.source_files ?? []).map((entry) => [entry.path, entry.sha256]),
    );
    for (const [file, hash] of expected) {
      if (!actual.has(file))
        fail(
          `ui source denominator is missing new production source file: ${file}`,
        );
      else if (actual.get(file) !== hash)
        fail(`ui source hash is stale: ${file}`);
    }
    for (const file of actual.keys()) {
      if (!expected.has(file))
        fail(
          `ui source denominator contains removed production source file: ${file}`,
        );
    }
  }
  if (
    JSON.stringify(ui.route_denominator) !== JSON.stringify(discoveredRoutes)
  ) {
    fail(
      'ui.route_denominator does not match the independently discovered routes tree',
    );
  }
  const sourceManifest = new Set(discoveredSources.map((entry) => entry.path));
  const ids = new Map();
  const sourceIds = new Set();
  const requestKeys = [];
  const legacyClaims = new Map();
  for (const [index, item] of (ui.items ?? []).entries()) {
    const context = `ui.items[${index}]`;
    requiredFields(
      item,
      [
        'id',
        'source_id',
        'legacy_ids',
        'page',
        'component',
        'source',
        'name',
        'trigger_steps',
        'preconditions',
        'variants',
        'scope',
        'iteration_source',
        'coverage',
        'risk_tier',
        'expected_ui',
        'polling',
        'client_only_reason',
        'atomic_request_ids',
        'requests',
        'applicability',
      ],
      context,
    );
    requireString(item.id, `${context}.id`);
    requireString(item.source_id, `${context}.source_id`);
    if (sourceIds.has(item.source_id))
      fail(`duplicate UI source_id: ${item.source_id}`);
    sourceIds.add(item.source_id);
    if (ids.has(item.id))
      fail(
        `duplicate inventory id ${item.id} at ${context} and ${ids.get(item.id)}`,
      );
    ids.set(item.id, context);
    requireStringArray(item.trigger_steps, `${context}.trigger_steps`);
    requireStringArray(item.preconditions, `${context}.preconditions`);
    requireStringArray(item.variants, `${context}.variants`);
    requireStringArray(
      item.atomic_request_ids,
      `${context}.atomic_request_ids`,
    );
    requireStringArray(item.legacy_ids, `${context}.legacy_ids`);
    for (const legacyId of item.legacy_ids ?? []) {
      if (legacyClaims.has(legacyId))
        fail(
          `legacy ID ${legacyId} is claimed by both ${legacyClaims.get(legacyId)} and ${item.source_id}`,
        );
      legacyClaims.set(legacyId, item.source_id);
    }
    if (!RISK_TIERS.has(item.risk_tier))
      fail(`${context}.risk_tier is invalid: ${item.risk_tier}`);
    requiredFields(
      item.applicability,
      ['latest_source', 'production'],
      `${context}.applicability`,
    );
    if (item.applicability?.latest_source !== true)
      fail(`${context}.applicability.latest_source must be true`);
    if (item.applicability?.production !== 'pending_deployed_delta')
      fail(
        `${context}.applicability.production must remain pending_deployed_delta in the source catalog`,
      );
    validateSourceReference(item.source, `${context}.source`, sourceManifest);
    if (!Array.isArray(item.requests))
      fail(`${context}.requests must be an array`);
    const childIds = [];
    for (const [requestIndex, request] of (item.requests ?? []).entries()) {
      const requestContext = `${context}.requests[${requestIndex}]`;
      requiredFields(
        request,
        [
          'id',
          'parent_action_id',
          'method',
          'path',
          'query_fields',
          'body_fields',
          'client_symbol',
          'source',
          'side_effect_notes',
          'path_bindings',
          'risk_tier',
          'applicability',
        ],
        requestContext,
      );
      if (ids.has(request.id))
        fail(
          `duplicate inventory id ${request.id} at ${requestContext} and ${ids.get(request.id)}`,
        );
      ids.set(request.id, requestContext);
      childIds.push(request.id);
      if (request.parent_action_id !== item.id)
        fail(`${requestContext}.parent_action_id must equal ${item.id}`);
      if (!HTTP_METHODS.has(request.method))
        fail(`${requestContext}.method is invalid: ${request.method}`);
      requireString(request.path, `${requestContext}.path`);
      if (request.path.includes('${'))
        fail(
          `${requestContext}.path contains a JavaScript template instead of a wire template`,
        );
      if (request.path.includes('?'))
        fail(`${requestContext}.path must not embed query parameters`);
      requireStringArray(
        request.query_fields,
        `${requestContext}.query_fields`,
      );
      requireStringArray(request.body_fields, `${requestContext}.body_fields`);
      if (!Array.isArray(request.path_bindings))
        fail(`${requestContext}.path_bindings must be an array`);
      const variables = [...request.path.matchAll(/\{([^}]+)\}/g)]
        .map((match) => match[1])
        .sort();
      const bindings = (request.path_bindings ?? [])
        .map((binding) => binding.name)
        .sort();
      if (JSON.stringify(variables) !== JSON.stringify(bindings))
        fail(`${requestContext}.path_bindings do not match path variables`);
      if (!RISK_TIERS.has(request.risk_tier))
        fail(`${requestContext}.risk_tier is invalid: ${request.risk_tier}`);
      requiredFields(
        request.applicability,
        ['latest_source', 'production'],
        `${requestContext}.applicability`,
      );
      if (request.applicability?.latest_source !== true)
        fail(`${requestContext}.applicability.latest_source must be true`);
      if (request.applicability?.production !== 'pending_deployed_delta')
        fail(
          `${requestContext}.applicability.production must remain pending_deployed_delta in the source catalog`,
        );
      validateSourceReference(
        request.source,
        `${requestContext}.source`,
        sourceManifest,
      );
      requestKeys.push({
        item,
        request,
        key: routeKey(request.method, request.path),
      });
    }
    if (JSON.stringify(item.atomic_request_ids) !== JSON.stringify(childIds))
      fail(`${context}.atomic_request_ids must match requests in order`);
    if (item.requests.length === 0 && !item.client_only_reason)
      fail(`${context} has no request and must state client_only_reason`);
    if (
      item.requests.length > 0 &&
      item.client_only_reason &&
      !String(item.client_only_reason).includes('leads to')
    ) {
      fail(
        `${context} has requests but is marked client-only without documenting the request transition`,
      );
    }
  }
  const legacyIds = new Set();
  for (const [index, mapping] of (ui.legacy_id_mapping ?? []).entries()) {
    requiredFields(
      mapping,
      ['legacy_id', 'status'],
      `ui.legacy_id_mapping[${index}]`,
    );
    if (legacyIds.has(mapping.legacy_id))
      fail(`duplicate legacy ID mapping: ${mapping.legacy_id}`);
    legacyIds.add(mapping.legacy_id);
    if (
      ![
        'preserved',
        'preserved_alias',
        'moved_to_api_catalog',
        'retired',
      ].includes(mapping.status)
    ) {
      fail(
        `invalid legacy mapping status for ${mapping.legacy_id}: ${mapping.status}`,
      );
    }
    if (mapping.status === 'retired' && !mapping.retired_reason)
      fail(`retired legacy ID lacks retired_reason: ${mapping.legacy_id}`);
    if (['preserved', 'preserved_alias'].includes(mapping.status)) {
      if (!mapping.canonical_id || !ids.has(mapping.canonical_id))
        fail(
          `legacy mapping ${mapping.legacy_id} references an unknown canonical_id`,
        );
      if (!mapping.source_id || !sourceIds.has(mapping.source_id))
        fail(
          `legacy mapping ${mapping.legacy_id} references an unknown source_id`,
        );
    }
    if (mapping.status === 'moved_to_api_catalog')
      requiredFields(
        mapping.endpoint,
        ['method', 'path'],
        `ui.legacy_id_mapping[${index}].endpoint`,
      );
  }
  for (const [legacyId, sourceId] of legacyClaims) {
    const mapping = (ui.legacy_id_mapping ?? []).find(
      (entry) => entry.legacy_id === legacyId,
    );
    if (!mapping)
      fail(
        `item ${sourceId} claims legacy ID ${legacyId} without a legacy_id_mapping entry`,
      );
    else if (mapping.source_id !== sourceId)
      fail(
        `item ${sourceId} claims legacy ID ${legacyId}, but mapping assigns it to ${mapping.source_id}`,
      );
  }
  const dimensionIds = new Set();
  const gapIds = new Set();
  for (const [index, gap] of (ui.gaps ?? []).entries()) {
    const context = `ui.gaps[${index}]`;
    requireString(gap.gap_id, `${context}.gap_id`);
    if (gapIds.has(gap.gap_id)) fail(`duplicate UI gap ID: ${gap.gap_id}`);
    gapIds.add(gap.gap_id);
    if (!(gap.description || gap.title || gap.observation))
      fail(`${context} must include description, title, or observation`);
  }
  for (const [index, dimension] of (ui.coverage_dimensions ?? []).entries()) {
    const context = `ui.coverage_dimensions[${index}]`;
    requiredFields(dimension, ['id', 'kind', 'source'], context);
    if (dimensionIds.has(dimension.id))
      fail(`duplicate coverage dimension: ${dimension.id}`);
    dimensionIds.add(dimension.id);
    requiredFields(dimension.source, ['file', 'symbol'], `${context}.source`);
    const sourceFile = dimension.source?.file;
    if (!sourceManifest.has(sourceFile))
      fail(
        `${context}.source.file is outside the UI production-source denominator: ${sourceFile}`,
      );
    else {
      const text = fs.readFileSync(path.join(repoRoot, sourceFile), 'utf8');
      if (!text.includes(dimension.source.symbol))
        fail(
          `${context}.source.symbol was not found in ${sourceFile}: ${dimension.source.symbol}`,
        );
    }
    if (
      dimension.kind.includes('enum') &&
      !Array.isArray(dimension.values) &&
      !dimension.resolver
    ) {
      fail(
        `finite dimension must list every value or an exact runtime resolver: ${dimension.id}`,
      );
    }
    if (
      Array.isArray(dimension.values) &&
      new Set(dimension.values).size !== dimension.values.length
    ) {
      fail(`finite dimension contains duplicate values: ${dimension.id}`);
    }
    if (
      dimension.kind === 'equivalence_classes' &&
      (!Array.isArray(dimension.classes) || dimension.classes.length < 2)
    ) {
      fail(
        `infinite-input dimension must define at least two equivalence classes: ${dimension.id}`,
      );
    }
  }
  return { ids, sourceIds, requestKeys };
}

function validateApiSourceObject(source, context, sourceFiles, requireHash) {
  const fields = ['file', 'symbol', 'line_start', 'line_end'];
  if (requireHash) fields.push('sha256');
  requiredFields(source, fields, context);
  if (!source || typeof source !== 'object') return null;
  if (!sourceFiles.has(source.file))
    fail(
      `${context}.file is absent from the contract source_files manifest: ${source.file}`,
    );
  const absolute = path.join(repoRoot, source.file ?? '');
  if (!fs.existsSync(absolute)) {
    fail(`${context}.file does not exist: ${source.file}`);
    return null;
  }
  const bytes = fs.readFileSync(absolute);
  const text = bytes.toString('utf8');
  const lines = text.split('\n');
  const lineCount = lines.length;
  let region = '';
  if (
    !Number.isInteger(source.line_start) ||
    !Number.isInteger(source.line_end) ||
    source.line_start < 1 ||
    source.line_end < source.line_start ||
    source.line_end > lineCount
  ) {
    fail(
      `${context} has an invalid source line range ${source.line_start}-${source.line_end} for ${source.file}`,
    );
  } else {
    region = lines.slice(source.line_start - 1, source.line_end).join('\n');
    // A symbol identifies the enclosing declaration; a statement range may
    // select only its SQL expression. Literal SQL is checked against that range.
    const leafSymbol = source.symbol.split('::').at(-1);
    const symbolFound =
      text.includes(source.symbol) ||
      text.includes(leafSymbol) ||
      path.basename(source.file) === leafSymbol;
    if (!symbolFound)
      fail(
        `${context}.symbol was not found in ${source.file}: ${source.symbol}`,
      );
  }
  if (requireHash && sha256(bytes) !== source.sha256)
    fail(`${context}.sha256 is stale: ${source.file}`);
  return { region, text };
}

function validateEndpointSource(source, context, sourceFiles) {
  requireString(source, context);
  if (typeof source !== 'string') return;
  const match = source.match(/^(.+):(\d+)(?:-(\d+))?$/);
  if (!match) {
    fail(`${context} must be file:line or file:start-end`);
    return;
  }
  const [, file, startText, endText] = match;
  if (!sourceFiles.has(file))
    fail(
      `${context} file is absent from the contract source_files manifest: ${file}`,
    );
  const absolute = path.join(repoRoot, file);
  if (!fs.existsSync(absolute)) {
    fail(`${context} file does not exist: ${file}`);
    return;
  }
  const lineCount = fs.readFileSync(absolute, 'utf8').split('\n').length;
  const start = Number(startText);
  const end = Number(endText ?? startText);
  if (start < 1 || end < start || end > lineCount)
    fail(
      `${context} has an invalid source line range ${start}-${end} for ${file}`,
    );
}

function validateApiContract(contract, label, uiSourceCommit) {
  if (!contract)
    return { endpoints: [], operations: [], sourceFiles: [], gaps: [] };
  requiredFields(
    contract,
    [
      'schema_version',
      'source_commit',
      'source_files',
      'endpoints',
      'operations',
      'gaps',
    ],
    label,
  );
  if (contract.schema_version !== 1) fail(`${label}.schema_version must be 1`);
  if (contract.source_commit !== uiSourceCommit)
    fail(
      `${label}.source_commit ${contract.source_commit} does not match UI source commit ${uiSourceCommit}`,
    );
  const sourceFiles = new Map();
  for (const [index, source] of (contract.source_files ?? []).entries()) {
    requiredFields(
      source,
      ['path', 'sha256'],
      `${label}.source_files[${index}]`,
    );
    const absolute = path.join(repoRoot, source.path ?? '');
    if (!fs.existsSync(absolute))
      fail(`${label}.source_files[${index}] does not exist: ${source.path}`);
    else {
      const actual = sha256(fs.readFileSync(absolute));
      if (actual !== source.sha256)
        fail(`${label}.source_files[${index}] hash is stale: ${source.path}`);
      sourceFiles.set(source.path, source.sha256);
    }
  }
  const operationIds = new Set();
  for (const [index, operation] of (contract.operations ?? []).entries()) {
    const context = `${label}.operations[${index}]`;
    requiredFields(
      operation,
      ['id', 'engine', 'trait_method', 'source', 'statements'],
      context,
    );
    if (operationIds.has(operation.id))
      fail(`duplicate storage operation id: ${operation.id}`);
    operationIds.add(operation.id);
    validateApiSourceObject(
      operation.source,
      `${context}.source`,
      sourceFiles,
      true,
    );
    if (
      !Array.isArray(operation.statements) ||
      operation.statements.length === 0
    )
      fail(`${context}.statements must not be empty`);
    for (const [statementIndex, statement] of (
      operation.statements ?? []
    ).entries()) {
      const statementContext = `${context}.statements[${statementIndex}]`;
      requiredFields(
        statement,
        ['kind', 'source', 'branches', 'bind_parameters'],
        statementContext,
      );
      if (!['literal', 'builder', 'delegated'].includes(statement.kind))
        fail(`${statementContext}.kind is invalid: ${statement.kind}`);
      if (!Array.isArray(statement.branches))
        fail(`${statementContext}.branches must be an array`);
      if (!Array.isArray(statement.bind_parameters))
        fail(`${statementContext}.bind_parameters must be an array`);
      if (statement.kind !== 'literal' && statement.branches.length === 0)
        fail(
          `${statementContext}.branches must describe the complete ${statement.kind} construction or delegation`,
        );
      const statementEvidence = validateApiSourceObject(
        statement.source,
        `${statementContext}.source`,
        sourceFiles,
        false,
      );
      if (statement.kind === 'literal') {
        requireString(statement.text, `${statementContext}.text`);
        if (statement.text?.includes('...'))
          fail(
            `${statementContext}.text contains an ellipsis instead of exact source SQL`,
          );
        if (statementEvidence) {
          const normalizeSql = (value) =>
            value.replace(/\\\s*/g, '').replace(/\s+/g, ' ').trim();
          if (
            !normalizeSql(statementEvidence.region).includes(
              normalizeSql(statement.text),
            )
          ) {
            fail(
              `${statementContext}.text does not match the exact source SQL in ${statement.source.file}:${statement.source.line_start}-${statement.source.line_end}`,
            );
          }
        }
      }
      if (statement.kind !== 'literal' && statement.text?.includes('...'))
        fail(
          `${statementContext}.text invents incomplete SQL for a ${statement.kind} statement`,
        );
    }
  }
  const endpointKeys = new Set();
  for (const [index, endpoint] of (contract.endpoints ?? []).entries()) {
    const context = `${label}.endpoints[${index}]`;
    requiredFields(
      endpoint,
      [
        'method',
        'path',
        'source',
        'handler',
        'auth',
        'ui_clients',
        'storage_operation_ids',
        'cache_or_no_query',
        'side_effects',
        'risk_tier',
        'parameters',
      ],
      context,
    );
    validateEndpointSource(endpoint.source, `${context}.source`, sourceFiles);
    if (!HTTP_METHODS.has(endpoint.method))
      fail(`${context}.method is invalid: ${endpoint.method}`);
    if (endpoint.path.includes('${'))
      fail(`${context}.path contains a JavaScript template`);
    const key = routeKey(endpoint.method, endpoint.path);
    if (endpointKeys.has(key))
      fail(
        `duplicate endpoint route in ${label}: ${endpoint.method} ${endpoint.path}`,
      );
    endpointKeys.add(key);
    requireStringArray(endpoint.ui_clients, `${context}.ui_clients`);
    requireStringArray(
      endpoint.storage_operation_ids,
      `${context}.storage_operation_ids`,
    );
    for (const operationId of endpoint.storage_operation_ids ?? []) {
      if (!operationIds.has(operationId))
        fail(`${context} references unknown storage operation ${operationId}`);
    }
    if (!RISK_TIERS.has(endpoint.risk_tier))
      fail(`${context}.risk_tier is invalid: ${endpoint.risk_tier}`);
    requiredFields(
      endpoint.parameters,
      ['path', 'query', 'header', 'body'],
      `${context}.parameters`,
    );
    const parameterNameSets = {};
    for (const kind of ['path', 'query', 'header', 'body']) {
      parameterNameSets[kind] = parameterNames(
        endpoint.parameters?.[kind],
        `${context}.parameters.${kind}`,
      );
    }
    const pathVariables = [...endpoint.path.matchAll(/\{([^}]+)\}/g)]
      .map((match) => match[1].replace(/^\*/, ''))
      .sort();
    const pathParameters = parameterNameSets.path
      .map((name) => name.replace(/^\*/, ''))
      .sort();
    if (JSON.stringify(pathVariables) !== JSON.stringify(pathParameters))
      fail(`${context}.parameters.path does not match URI variables`);
  }
  return {
    endpoints: contract.endpoints ?? [],
    operations: contract.operations ?? [],
    sourceFiles: [...sourceFiles.keys()],
    gaps: contract.gaps ?? [],
  };
}

function validateBackendDenominator(discovered, endpoints) {
  const discoveredByKey = new Map();
  for (const route of discovered) {
    const key = routeKey(route.method, route.path);
    if (!discoveredByKey.has(key)) discoveredByKey.set(key, route);
  }
  const catalogByKey = new Map();
  for (const endpoint of endpoints) {
    const key = routeKey(endpoint.method, endpoint.path);
    if (catalogByKey.has(key))
      fail(
        `API catalogs duplicate registered route ${endpoint.method} ${endpoint.path}`,
      );
    catalogByKey.set(key, endpoint);
  }
  const missingFromCatalog = [...discoveredByKey]
    .filter(([key]) => !catalogByKey.has(key))
    .map(([, route]) => route);
  const missingFromRegistration = [...catalogByKey]
    .filter(([key]) => !discoveredByKey.has(key))
    .map(([, endpoint]) => ({
      method: endpoint.method,
      path: endpoint.path,
      source: endpoint.source,
    }));
  for (const route of missingFromCatalog)
    fail(
      `registered backend route missing from API catalog: ${route.method} ${route.path} (${route.source.file}:${route.source.line})`,
    );
  for (const endpoint of missingFromRegistration)
    fail(
      `API catalog route missing from independent registration scan: ${endpoint.method} ${endpoint.path}`,
    );
  return { missingFromCatalog, missingFromRegistration };
}

function parseDeployedDelta(delta, ui) {
  if (!delta) {
    const pending = () => ({
      status: 'pending_deployed_delta',
      reason: 'deployed-delta.json not supplied',
    });
    return {
      delta: null,
      statusFor: pending,
      latestOnlyUi: [],
      latestOnlyApi: [],
      unknownLatestOnlySourceIds: [],
      unknownLatestOnlyEndpoints: [],
      deployedOnlyItems: [],
      deployedOnlyEndpoints: [],
      classificationNotes: [],
    };
  }
  requiredFields(
    delta,
    [
      'deployed_commit',
      'source_commit',
      'added_since_deploy',
      'removed_since_deploy',
      'changed_contracts',
      'deployed_only_items',
      'deployed_only_endpoints',
      'source_files',
      'unread_files',
      'gaps',
    ],
    'deployed_delta',
  );
  requireString(delta.deployed_commit, 'deployed_delta.deployed_commit');
  requireString(delta.source_commit, 'deployed_delta.source_commit');
  if (!delta.source_commit.startsWith(ui.source_commit))
    fail(
      `deployed_delta.source_commit ${delta.source_commit} does not match UI source commit ${ui.source_commit}`,
    );
  for (const field of [
    'added_since_deploy',
    'removed_since_deploy',
    'changed_contracts',
    'deployed_only_items',
    'deployed_only_endpoints',
    'source_files',
    'unread_files',
    'gaps',
  ]) {
    if (!Array.isArray(delta[field]))
      fail(`deployed_delta.${field} must be an array`);
  }
  if ((delta.unread_files ?? []).length > 0)
    fail(
      `deployed delta has unread source files: ${delta.unread_files.join(', ')}`,
    );
  if ((delta.gaps ?? []).length > 0)
    fail('deployed delta contains unresolved gaps');
  const deltaSourceFiles = new Set(delta.source_files ?? []);
  for (const [index, entry] of (delta.added_since_deploy ?? []).entries()) {
    const sources = [
      ...(entry.source && typeof entry.source === 'object'
        ? [entry.source]
        : []),
      ...(entry.sources ?? []),
    ];
    for (const [sourceIndex, source] of sources.entries()) {
      const context = `deployed_delta.added_since_deploy[${index}].sources[${sourceIndex}]`;
      if (source && Object.hasOwn(source, 'line'))
        validateSourceReference(source, context, deltaSourceFiles);
      else validateApiSourceObject(source, context, deltaSourceFiles, true);
    }
  }
  const aliases = new Map(
    (ui.normalization?.deployed_delta_source_aliases ?? []).map((entry) => [
      entry.delta_source_id,
      entry.canonical_source_id,
    ]),
  );
  const latestOnlyUi = (delta.added_since_deploy ?? []).filter(
    (entry) => entry.type === 'ui',
  );
  const latestOnlyApi = (delta.added_since_deploy ?? []).filter(
    (entry) => entry.type === 'api',
  );
  const unavailableSourceIds = new Set(
    latestOnlyUi.map(
      (entry) => aliases.get(entry.source_id) ?? entry.source_id,
    ),
  );
  const unavailableRouteKeys = new Set(
    latestOnlyApi.map((entry) => routeKey(entry.method, entry.path)),
  );
  const overrides = new Map();
  for (const entry of delta.items ?? []) {
    if (entry.source_id)
      overrides.set(
        `source:${aliases.get(entry.source_id) ?? entry.source_id}`,
        entry,
      );
    if (entry.method && entry.path)
      overrides.set(`route:${routeKey(entry.method, entry.path)}`, entry);
  }
  const statusFor = (item, request) => {
    const sourceOverride = overrides.get(`source:${item.source_id}`);
    const routeOverride = request
      ? overrides.get(
          `route:${routeKey(request.method, request.path ?? request.endpoint)}`,
        )
      : null;
    const override = sourceOverride ?? routeOverride;
    if (override)
      return {
        status:
          override.status ??
          (override.applicable === false ||
          override.production_applicability === false
            ? 'not_deployed'
            : 'available'),
        reason: override.reason ?? null,
      };
    if (unavailableSourceIds.has(item.source_id))
      return {
        status: 'not_deployed',
        reason: `Added after deployed commit ${delta.deployed_commit.slice(0, 8)}`,
      };
    if (
      request &&
      unavailableRouteKeys.has(
        routeKey(request.method, request.path ?? request.endpoint),
      )
    )
      return {
        status: 'not_deployed',
        reason: `Endpoint added after deployed commit ${delta.deployed_commit.slice(0, 8)}`,
      };
    return { status: 'available', reason: null };
  };
  const currentSourceIds = new Set(ui.items.map((item) => item.source_id));
  const currentEndpointKeys = new Set();
  const unknownLatestOnlySourceIds = [...unavailableSourceIds].filter(
    (sourceId) => !currentSourceIds.has(sourceId),
  );
  const unknownLatestOnlyEndpoints = [];
  return {
    delta,
    statusFor,
    latestOnlyUi,
    latestOnlyApi,
    unknownLatestOnlySourceIds,
    unknownLatestOnlyEndpoints,
    deployedOnlyItems: delta.deployed_only_items ?? [],
    deployedOnlyEndpoints: delta.deployed_only_endpoints ?? [],
    classificationNotes: [
      ...(delta.changed_contracts ?? []).map((entry) => ({
        classification: 'changed_since_deploy',
        ...entry,
      })),
      ...(delta.removed_since_deploy ?? []).map((entry) => ({
        classification: 'removed_since_deploy',
        ...entry,
      })),
    ],
    aliases,
    currentEndpointKeys,
  };
}

function isBlockingApiGap(gap) {
  return (
    gap.blocking === true ||
    gap.classification === 'extraction_gap' ||
    String(gap.area ?? gap.id ?? '').startsWith('unresolved_')
  );
}

function normalizeRiskTier(value) {
  if (value === 'write') return 'reversible_write';
  if (value === 'destructive') return 'destructive_write';
  return RISK_TIERS.has(value) ? value : 'read';
}

function buildInventory(
  ui,
  apiEndpoints,
  apiOperations,
  apiGaps,
  deployment,
  sourceStatus,
  responseObservability,
) {
  const blockingApiGaps = apiGaps.filter(isBlockingApiGap);
  const apiClassificationNotes = apiGaps.filter(
    (gap) => !isBlockingApiGap(gap),
  );
  const endpointByKey = new Map(
    apiEndpoints.map((endpoint) => [
      routeKey(endpoint.method, endpoint.path),
      endpoint,
    ]),
  );
  const endpointClients = new Map(
    apiEndpoints.map((endpoint) => [
      routeKey(endpoint.method, endpoint.path),
      [],
    ]),
  );
  const deployedEndpointByKey = new Map(
    deployment.deployedOnlyEndpoints.map((endpoint) => [
      routeKey(endpoint.method, endpoint.path),
      endpoint,
    ]),
  );
  const deployedEndpointClients = new Map(
    deployment.deployedOnlyEndpoints.map((endpoint) => [
      routeKey(endpoint.method, endpoint.path),
      [],
    ]),
  );
  const aliasesByCanonical = new Map();
  for (const mapping of ui.legacy_id_mapping) {
    if (mapping.status !== 'preserved_alias' || !mapping.canonical_id) continue;
    const aliases = aliasesByCanonical.get(mapping.canonical_id) ?? [];
    aliases.push(mapping.legacy_id);
    aliasesByCanonical.set(mapping.canonical_id, aliases);
  }
  const interactions = [];
  for (const item of ui.items) {
    const requestDeployments = item.requests.map((request) =>
      deployment.statusFor(item, request),
    );
    let actionDeployment = deployment.statusFor(item, null);
    if (
      deployment.delta &&
      actionDeployment.status === 'available' &&
      requestDeployments.length > 0
    ) {
      if (
        requestDeployments.every((status) => status.status === 'not_deployed')
      ) {
        actionDeployment = {
          status: 'not_deployed',
          reason:
            'Every atomic request for this action is absent from the deployed API.',
        };
      } else if (
        requestDeployments.some((status) => status.status === 'not_deployed')
      ) {
        actionDeployment = {
          status: 'partially_deployed',
          reason:
            'At least one atomic request branch is absent from the deployed API.',
        };
      }
    }
    interactions.push({
      id: item.id,
      aliases: aliasesByCanonical.get(item.id) ?? [],
      entry_type: 'ui_action',
      source_id: item.source_id,
      category: item.contract_area,
      page: item.page,
      component: item.component,
      name: item.name,
      trigger_steps: item.trigger_steps,
      preconditions: item.preconditions,
      variants: item.variants,
      scope: item.scope,
      iteration_source: item.iteration_source,
      coverage: item.coverage,
      atomic_request_ids: item.atomic_request_ids,
      risk_tier: item.risk_tier,
      expected_ui: item.expected_ui,
      polling: item.polling,
      client_only_reason: item.client_only_reason,
      source: item.source,
      source_reconciled: true,
      runtime_status: 'runtime_pending',
      production_applicability: actionDeployment,
    });
    for (const [requestIndex, request] of item.requests.entries()) {
      const key = routeKey(request.method, request.path);
      const endpoint = endpointByKey.get(key);
      if (endpoint) endpointClients.get(key).push(item.source_id);
      const requestDeployment = requestDeployments[requestIndex];
      interactions.push({
        id: request.id,
        aliases: aliasesByCanonical.get(request.id) ?? [],
        entry_type: 'network_request',
        parent_action_id: item.id,
        source_id: item.source_id,
        category: item.contract_area,
        page: item.page,
        component: item.component,
        name: `${item.name} — ${request.branch ?? request.client_symbol}`,
        trigger_steps: item.trigger_steps,
        preconditions: item.preconditions,
        variants: item.variants,
        scope: item.scope,
        iteration_source: item.iteration_source,
        risk_tier: endpoint?.risk_tier ?? request.risk_tier,
        expected_ui: item.expected_ui,
        polling: item.polling,
        source: request.source,
        http: {
          method: request.method,
          path: request.path,
          path_bindings: request.path_bindings,
          query_fields: request.query_fields,
          body_fields: request.body_fields,
          header_fields: parameterNames(
            endpoint?.parameters?.header ?? [],
            `joined endpoint ${request.method} ${request.path} headers`,
          ),
          condition: request.condition ?? null,
        },
        client_symbol: request.client_symbol,
        api_handler: endpoint?.handler ?? null,
        auth: endpoint?.auth ?? null,
        storage_operation_ids: endpoint?.storage_operation_ids ?? [],
        cache_or_no_query: endpoint?.cache_or_no_query ?? null,
        side_effects: endpoint?.side_effects ?? request.side_effect_notes,
        source_reconciled: Boolean(endpoint),
        runtime_status: 'runtime_pending',
        production_applicability: requestDeployment,
      });
    }
  }
  for (const item of deployment.deployedOnlyItems) {
    const actionId = stableId('PROD-UI', item.source_id);
    const requestIds = (item.requests ?? []).map((request, index) =>
      stableId(
        'PROD-REQ',
        `${item.source_id}|${index}|${request.method}|${request.endpoint}`,
      ),
    );
    interactions.push({
      id: actionId,
      aliases: [],
      entry_type: 'ui_action',
      source_catalog: 'deployed_only',
      source_commit: deployment.delta.deployed_commit,
      source_id: item.source_id,
      category: 'production_only',
      page: item.page,
      component: item.component,
      name: item.name,
      trigger_steps: item.trigger_steps ?? [],
      preconditions: item.preconditions ?? [],
      variants: item.variants ?? [],
      scope: item.scope,
      iteration_source: item.iteration_source,
      atomic_request_ids: requestIds,
      risk_tier: normalizeRiskTier(item.risk_tier),
      expected_ui: item.expected_ui,
      polling: item.polling ?? 'not_applicable',
      client_only_reason: item.client_only_reason ?? null,
      source: item.source,
      source_reconciled: true,
      runtime_status: 'runtime_pending',
      production_applicability: {
        status: 'available',
        reason: 'Present only in the deployed production commit.',
      },
    });
    for (const [index, request] of (item.requests ?? []).entries()) {
      const requestPath = normalizeWirePath(request.endpoint);
      const key = routeKey(request.method, requestPath);
      const deployedEndpoint = deployedEndpointByKey.get(key);
      const currentEndpoint = endpointByKey.get(key);
      if (deployedEndpoint)
        deployedEndpointClients.get(key).push(item.source_id);
      interactions.push({
        id: requestIds[index],
        aliases: [],
        entry_type: 'network_request',
        source_catalog: 'deployed_only',
        source_commit: deployment.delta.deployed_commit,
        parent_action_id: actionId,
        source_id: item.source_id,
        category: 'production_only',
        page: item.page,
        component: item.component,
        name: `${item.name} — ${request.client_symbol}`,
        trigger_steps: item.trigger_steps ?? [],
        preconditions: item.preconditions ?? [],
        variants: item.variants ?? [],
        scope: item.scope,
        iteration_source: item.iteration_source,
        risk_tier: normalizeRiskTier(item.risk_tier),
        expected_ui: item.expected_ui,
        polling: item.polling ?? 'not_applicable',
        source: request.source,
        http: {
          method: request.method,
          path: requestPath,
          path_bindings: [...requestPath.matchAll(/\{([^}]+)\}/g)].map(
            (match) => ({
              name: match[1],
              source: `deployed_runtime.${match[1]}`,
              format: 'source_identifier',
            }),
          ),
          query_fields: request.query_fields ?? [],
          body_fields: request.body_fields ?? [],
          header_fields: currentEndpoint
            ? parameterNames(
                currentEndpoint.parameters.header,
                `deployed join ${request.method} ${requestPath} headers`,
              )
            : [],
          condition: null,
        },
        client_symbol: request.client_symbol,
        api_handler:
          deployedEndpoint?.handler ?? currentEndpoint?.handler ?? null,
        auth: deployedEndpoint?.auth ?? currentEndpoint?.auth ?? null,
        storage_operation_ids: currentEndpoint?.storage_operation_ids ?? [],
        cache_or_no_query:
          deployedEndpoint?.storage?.join?.('; ') ??
          currentEndpoint?.cache_or_no_query ??
          null,
        side_effects:
          request.side_effect_notes ??
          deployedEndpoint?.description ??
          currentEndpoint?.side_effects ??
          null,
        source_reconciled: Boolean(deployedEndpoint || currentEndpoint),
        runtime_status: 'runtime_pending',
        production_applicability: {
          status: 'available',
          reason: 'Present only in the deployed production commit.',
        },
      });
    }
  }
  const occupiedIds = new Set(
    interactions.map((interaction) => interaction.id),
  );
  const movedLegacy = new Map(
    ui.legacy_id_mapping
      .filter(
        (mapping) =>
          mapping.status === 'moved_to_api_catalog' && mapping.endpoint,
      )
      .map((mapping) => [
        routeKey(mapping.endpoint.method, mapping.endpoint.path),
        mapping.legacy_id,
      ]),
  );
  for (const endpoint of apiEndpoints) {
    const key = routeKey(endpoint.method, endpoint.path);
    if ((endpointClients.get(key) ?? []).length > 0) continue;
    const preferred = movedLegacy.get(key);
    const id =
      preferred && !occupiedIds.has(preferred)
        ? preferred
        : stableId('API-SRC', `${endpoint.method} ${endpoint.path}`);
    occupiedIds.add(id);
    const endpointDeployment = deployment.statusFor(
      { source_id: null },
      endpoint,
    );
    interactions.push({
      id,
      aliases: aliasesByCanonical.get(id) ?? [],
      entry_type: 'backend_endpoint',
      category: 'backend_only',
      page: 'Backend-Only',
      component: endpoint.handler,
      name: `${endpoint.method} ${endpoint.path}`,
      http: {
        method: endpoint.method,
        path: endpoint.path,
        parameters: endpoint.parameters,
      },
      trigger_steps: [
        'Direct API, operator, scheduler, health probe, or internal service caller',
      ],
      preconditions: [endpoint.auth],
      variants: [],
      scope: 'backend_only',
      iteration_source: 'not_applicable',
      risk_tier: endpoint.risk_tier,
      expected_ui:
        'No Admin Web caller. Validate through the authorized backend-only QA path.',
      source: endpoint.source,
      api_handler: endpoint.handler,
      auth: endpoint.auth,
      storage_operation_ids: endpoint.storage_operation_ids,
      cache_or_no_query: endpoint.cache_or_no_query,
      side_effects: endpoint.side_effects,
      source_reconciled: true,
      runtime_status: 'runtime_pending',
      production_applicability: endpointDeployment,
    });
  }
  for (const endpoint of deployment.deployedOnlyEndpoints) {
    const key = routeKey(endpoint.method, endpoint.path);
    if ((deployedEndpointClients.get(key) ?? []).length > 0) continue;
    const id = stableId('PROD-API', `${endpoint.method} ${endpoint.path}`);
    occupiedIds.add(id);
    const destructive = /killswitch|revoke/i.test(
      `${endpoint.path} ${endpoint.description}`,
    );
    interactions.push({
      id,
      aliases: [],
      entry_type: 'backend_endpoint',
      source_catalog: 'deployed_only',
      source_commit: deployment.delta.deployed_commit,
      category: 'production_only',
      page: 'Production Backend-Only',
      component: endpoint.handler,
      name: `${endpoint.method} ${endpoint.path}`,
      http: {
        method: endpoint.method,
        path: normalizeWirePath(endpoint.path),
        parameters: null,
      },
      trigger_steps: ['Direct deployed API or operator caller'],
      preconditions: [endpoint.auth],
      variants: [],
      scope: 'backend_only',
      iteration_source: 'not_applicable',
      risk_tier: destructive ? 'destructive_write' : 'read',
      expected_ui:
        'No deployed Admin Web caller was reconciled for this endpoint.',
      source: endpoint.reference,
      api_handler: endpoint.handler,
      auth: endpoint.auth,
      storage_operation_ids: [],
      cache_or_no_query:
        endpoint.storage?.join?.('; ') ?? String(endpoint.storage ?? ''),
      side_effects: endpoint.description,
      source_reconciled: true,
      runtime_status: 'runtime_pending',
      production_applicability: {
        status: 'available',
        reason: 'Present only in the deployed production commit.',
      },
    });
  }
  const productionMatrix = deployment.delta
    ? interactions.filter(
        (interaction) =>
          interaction.production_applicability.status !== 'not_deployed',
      )
    : [];
  const counts = interactions.reduce(
    (result, interaction) => {
      result.total += 1;
      result[interaction.entry_type] =
        (result[interaction.entry_type] ?? 0) + 1;
      return result;
    },
    { total: 0 },
  );
  return {
    version: '3.0.0',
    system: 'cc-lb Admin Web & API Surface',
    source_commit: ui.source_commit,
    metadata: {
      source_state: ui.source_state,
      response_observability: responseObservability,
      source_reconciled: sourceStatus === 'source_reconciled',
      runtime_status: 'runtime_pending',
      production_applicability: deployment.delta
        ? 'reconciled_from_deployed_delta'
        : 'pending_deployed_delta',
      count_basis:
        'Rows are rendered from source contracts and independently checked source/route denominators; counts alone are not completion evidence.',
      static_catalog_limitations: ui.metadata.static_catalog_limitations,
      counts,
    },
    coverage_dimensions: ui.coverage_dimensions,
    interactions,
    production_matrix_ids: productionMatrix.map(
      (interaction) => interaction.id,
    ),
    storage_operations: apiOperations,
    classification_notes: [
      ...apiClassificationNotes,
      ...deployment.classificationNotes,
    ],
    gaps: [...ui.gaps, ...blockingApiGaps],
  };
}

function buildReconciliation(
  ui,
  uiValidation,
  apiEndpoints,
  apiGaps,
  discoveredBackendRoutes,
  backendDenominator,
  deployment,
) {
  const blockingApiGaps = apiGaps.filter(isBlockingApiGap);
  const apiClassificationNotes = apiGaps.filter(
    (gap) => !isBlockingApiGap(gap),
  );
  const endpointByKey = new Map(
    apiEndpoints.map((endpoint) => [
      routeKey(endpoint.method, endpoint.path),
      endpoint,
    ]),
  );
  const unknownUiRequests = uiValidation.requestKeys
    .filter(({ key }) => !endpointByKey.has(key))
    .map(({ item, request }) => ({
      source_id: item.source_id,
      request_id: request.id,
      method: request.method,
      path: request.path,
    }));
  for (const request of unknownUiRequests)
    fail(
      `UI request is not registered in the API catalogs: ${request.method} ${request.path} (${request.source_id})`,
    );
  const movedLegacyMissing = ui.legacy_id_mapping
    .filter((mapping) => mapping.status === 'moved_to_api_catalog')
    .filter(
      (mapping) =>
        !endpointByKey.has(
          routeKey(mapping.endpoint.method, mapping.endpoint.path),
        ),
    )
    .map((mapping) => ({ legacy_id: mapping.legacy_id, ...mapping.endpoint }));
  for (const mapping of movedLegacyMissing) {
    fail(
      `legacy ID ${mapping.legacy_id} moved to an API endpoint absent from the catalogs: ${mapping.method} ${mapping.path}`,
    );
  }
  const unknownLatestOnlySourceIds = deployment.unknownLatestOnlySourceIds;
  const unknownLatestOnlyEndpoints = deployment.latestOnlyApi
    .filter((entry) => !endpointByKey.has(routeKey(entry.method, entry.path)))
    .map((entry) => ({ method: entry.method, path: entry.path }));
  for (const sourceId of unknownLatestOnlySourceIds)
    fail(
      `deployed delta latest-only UI source_id does not resolve to the canonical UI catalog: ${sourceId}`,
    );
  for (const endpoint of unknownLatestOnlyEndpoints)
    fail(
      `deployed delta latest-only endpoint does not resolve to the API catalogs: ${endpoint.method} ${endpoint.path}`,
    );
  const applicabilityGaps = [
    ...unknownLatestOnlySourceIds.map((source_id) => ({
      gap_id: stableId('GAP-DELTA-UI', source_id),
      classification: 'production_applicability',
      source_id,
      description:
        'Latest-only UI source_id could not be mapped to the canonical catalog.',
    })),
    ...unknownLatestOnlyEndpoints.map((endpoint) => ({
      gap_id: stableId('GAP-DELTA-API', `${endpoint.method} ${endpoint.path}`),
      classification: 'production_applicability',
      ...endpoint,
      description:
        'Latest-only API endpoint could not be mapped to the canonical catalog.',
    })),
  ];
  const uiClientsByEndpoint = new Map(
    apiEndpoints.map((endpoint) => [
      routeKey(endpoint.method, endpoint.path),
      [],
    ]),
  );
  for (const { item, request, key } of uiValidation.requestKeys) {
    if (uiClientsByEndpoint.has(key))
      uiClientsByEndpoint
        .get(key)
        .push({ source_id: item.source_id, request_id: request.id });
  }
  const backendOnly = apiEndpoints
    .filter(
      (endpoint) =>
        uiClientsByEndpoint.get(routeKey(endpoint.method, endpoint.path))
          .length === 0,
    )
    .map((endpoint) => ({
      method: endpoint.method,
      path: endpoint.path,
      reason:
        endpoint.ui_clients.length === 0
          ? 'No Admin Web caller in current source.'
          : `API catalog declares non-UI clients: ${endpoint.ui_clients.join(', ')}`,
    }));
  const notDeployed = [];
  for (const item of ui.items) {
    const actionStatus = deployment.statusFor(item, null);
    if (actionStatus.status === 'not_deployed')
      notDeployed.push({
        source_id: item.source_id,
        reason: actionStatus.reason,
      });
    for (const request of item.requests) {
      const status = deployment.statusFor(item, request);
      if (status.status === 'not_deployed')
        notDeployed.push({
          source_id: item.source_id,
          request_id: request.id,
          method: request.method,
          path: request.path,
          reason: status.reason,
        });
    }
  }
  return {
    schema_version: 1,
    source_commit: ui.source_commit,
    source_state: ui.source_state,
    deployed_commit: deployment.delta?.deployed_commit ?? null,
    source_reconciliation: {
      status:
        unknownUiRequests.length === 0 &&
        movedLegacyMissing.length === 0 &&
        unknownLatestOnlySourceIds.length === 0 &&
        unknownLatestOnlyEndpoints.length === 0 &&
        backendDenominator.missingFromCatalog.length === 0 &&
        backendDenominator.missingFromRegistration.length === 0 &&
        blockingApiGaps.length === 0
          ? 'source_reconciled'
          : 'incomplete',
      ui_actions: ui.items.length,
      ui_request_occurrences: uiValidation.requestKeys.length,
      api_endpoints: apiEndpoints.length,
      registered_backend_routes: discoveredBackendRoutes.length,
      production_ui_source_files: ui.source_files.length,
      ui_routes: ui.route_denominator.length,
      unknown_ui_requests: unknownUiRequests,
      moved_legacy_ids_missing_from_api_catalog: movedLegacyMissing,
      unknown_latest_only_source_ids: unknownLatestOnlySourceIds,
      unknown_latest_only_endpoints: unknownLatestOnlyEndpoints,
      registered_routes_missing_from_catalog:
        backendDenominator.missingFromCatalog,
      catalog_routes_missing_from_registration:
        backendDenominator.missingFromRegistration,
      backend_only_endpoints: backendOnly,
      api_contract_gaps: blockingApiGaps,
      classification_notes: [
        ...apiClassificationNotes,
        ...deployment.classificationNotes,
      ],
    },
    runtime_reconciliation: {
      status: 'runtime_pending',
      browser_execution: 'not_run',
      entity_populations: 'not_resolved',
      pagination_termination: 'not_observed',
      polling_and_stream_cycles: 'not_observed',
    },
    production_applicability: {
      status:
        deployment.delta && applicabilityGaps.length === 0
          ? 'reconciled'
          : deployment.delta
            ? 'incomplete'
            : 'pending_deployed_delta',
      not_deployed: notDeployed,
      deployed_only_items: deployment.deployedOnlyItems.map(
        (item) => item.source_id,
      ),
      deployed_only_endpoints: deployment.deployedOnlyEndpoints.map(
        (endpoint) => ({ method: endpoint.method, path: endpoint.path }),
      ),
      rule: 'Latest-only rows are excluded; deployed-only rows are explicit production catalog entries and remain in production_matrix_ids.',
    },
    normalization: ui.normalization,
    legacy_id_mapping: ui.legacy_id_mapping,
    classification_notes: [
      ...apiClassificationNotes,
      ...deployment.classificationNotes,
    ],
    gaps: [...ui.gaps, ...blockingApiGaps, ...applicabilityGaps],
  };
}

function yamlScalar(value) {
  if (value === null) return 'null';
  if (typeof value === 'boolean' || typeof value === 'number')
    return String(value);
  return JSON.stringify(String(value));
}

function formatYaml(value, indent = 0) {
  const pad = ' '.repeat(indent);
  if (Array.isArray(value)) {
    if (value.length === 0) return '[]';
    return value
      .map((item) => {
        if (item && typeof item === 'object') {
          const rendered = formatYaml(item, indent + 2);
          const [first, ...rest] = rendered.split('\n');
          return `${pad}- ${first.trimStart()}${rest.length ? `\n${rest.join('\n')}` : ''}`;
        }
        return `${pad}- ${yamlScalar(item)}`;
      })
      .join('\n');
  }
  if (value && typeof value === 'object') {
    const entries = Object.entries(value);
    if (entries.length === 0) return '{}';
    return entries
      .map(([rawKey, item]) => {
        const key = yamlScalar(rawKey);
        if (item && typeof item === 'object') {
          const rendered = formatYaml(item, indent + 2);
          if (rendered === '[]' || rendered === '{}')
            return `${pad}${key}: ${rendered}`;
          return `${pad}${key}:\n${rendered}`;
        }
        return `${pad}${key}: ${yamlScalar(item)}`;
      })
      .join('\n');
  }
  return `${pad}${yamlScalar(value)}`;
}

function escapeCell(value) {
  return String(value ?? '')
    .replaceAll('|', '\\|')
    .replaceAll('\n', '<br>');
}

function formatList(values) {
  return values.length
    ? values
        .map((value) => `\`${typeof value === 'string' ? value : value.name}\``)
        .join(', ')
    : 'None';
}

function renderMarkdown(inventory, reconciliation) {
  let markdown = '# cc-lb Admin Web & API/Query QA Inventory\n\n';
  markdown += `> **Source base commit:** \`${inventory.source_commit}\`  \n`;
  markdown += `> **Source state:** \`${inventory.metadata.source_state.kind}\` — ${inventory.metadata.source_state.note}  \n`;
  markdown += `> **Source reconciliation:** \`${reconciliation.source_reconciliation.status}\`  \n`;
  markdown += `> **Runtime status:** \`${inventory.metadata.runtime_status}\`  \n`;
  markdown += `> **Production applicability:** \`${inventory.metadata.production_applicability}\`  \n`;
  markdown += `> **Count basis:** ${inventory.metadata.count_basis}\n\n`;
  markdown +=
    'Static reconciliation is not browser proof. Runtime entities, cursor termination, poll cycles, SSE behavior, production availability, and latency remain pending until the execution harness records them.\n\n';
  markdown += `Candidate server Admin responses expose \`${inventory.metadata.response_observability.response_header}\`; \`${inventory.metadata.response_observability.event}\` records response-head generation, not stream completion. This does not assert that the deployed production version has this instrumentation.\n\n`;
  markdown += `\`Server-Timing\` metric \`${inventory.metadata.response_observability.server_timing_metric}\` exposes the same opaque ID to Resource Timing. ${inventory.metadata.response_observability.correlation_limits}\n\n`;
  markdown += '## 1. Reconciliation Summary\n\n';
  markdown += '| Dimension | Count / Status |\n|---|---|\n';
  markdown += `| UI parent actions | ${reconciliation.source_reconciliation.ui_actions} |\n`;
  markdown += `| UI atomic request occurrences | ${reconciliation.source_reconciliation.ui_request_occurrences} |\n`;
  markdown += `| Registered API method/path rows | ${reconciliation.source_reconciliation.api_endpoints} |\n`;
  markdown += `| Independent backend route scan | ${reconciliation.source_reconciliation.registered_backend_routes} |\n`;
  markdown += `| Production UI source denominator | ${reconciliation.source_reconciliation.production_ui_source_files} files |\n`;
  markdown += `| UI route denominator | ${reconciliation.source_reconciliation.ui_routes} routes |\n`;
  markdown += `| Unknown UI requests | ${reconciliation.source_reconciliation.unknown_ui_requests.length} |\n`;
  markdown += `| Runtime | ${reconciliation.runtime_reconciliation.status} |\n\n`;
  markdown += '## 2. Risk Tiers\n\n';
  markdown += '- `read`: no source-backed side effect.\n';
  markdown +=
    '- `read_with_audit`: read path that also appends an audit record.\n';
  markdown +=
    '- `reversible_write`: fixture-scoped mutation with a source-backed restore path.\n';
  markdown +=
    '- `destructive_write`: deletion, revocation, or irreversible mutation requiring explicit isolation.\n';
  markdown +=
    '- `external_action`: calls an external system, reloads a service, performs OAuth exchange, or can consume billable upstream resources.\n\n';
  markdown += '## 3. Master Interaction Index\n\n';
  markdown +=
    '| ID | Type | Parent | Page / Component | Action or HTTP | Risk | Production |\n';
  markdown += '|---|---|---|---|---|---|---|\n';
  for (const item of inventory.interactions) {
    const action =
      item.entry_type === 'ui_action'
        ? item.name
        : `${item.http.method} ${item.http.path}`;
    markdown += `| **${escapeCell(item.id)}** | \`${item.entry_type}\` | ${escapeCell(item.parent_action_id ?? '—')} | ${escapeCell(`${item.page} / ${item.component}`)} | ${escapeCell(action)} | \`${item.risk_tier}\` | \`${item.production_applicability.status}\` |\n`;
  }
  markdown += '\n## 4. Detailed Execution Specifications\n\n';
  for (const item of inventory.interactions) {
    markdown += `### [${item.id}] ${item.page} — ${item.name}\n\n`;
    markdown += `- **Entry type:** \`${item.entry_type}\`\n`;
    if (item.parent_action_id)
      markdown += `- **Parent action:** \`${item.parent_action_id}\`\n`;
    if (item.atomic_request_ids)
      markdown += `- **Atomic requests:** ${formatList(item.atomic_request_ids)}\n`;
    markdown += `- **Source:** \`${typeof item.source === 'string' ? item.source : `${item.source.file}:${item.source.line ?? item.source.line_start ?? '?'}#${item.source.symbol ?? ''}`}\`\n`;
    markdown += `- **Preconditions:** ${item.preconditions?.length ? item.preconditions.join('; ') : 'None'}\n`;
    markdown += `- **Steps:** ${item.trigger_steps?.join(' → ') ?? 'Direct backend invocation'}\n`;
    markdown += `- **Scope:** \`${item.scope}\` — ${item.iteration_source}\n`;
    markdown += `- **Risk:** \`${item.risk_tier}\`\n`;
    markdown += `- **Production applicability:** \`${item.production_applicability.status}\`${item.production_applicability.reason ? ` — ${item.production_applicability.reason}` : ''}\n`;
    if (item.http) {
      markdown += `- **HTTP:** \`${item.http.method} ${item.http.path}\`\n`;
      const query = item.http.query_fields ?? item.http.parameters?.query ?? [];
      const body = item.http.body_fields ?? item.http.parameters?.body ?? [];
      const headers =
        item.http.header_fields ?? item.http.parameters?.header ?? [];
      markdown += `  - Query: ${formatList(query)}\n`;
      markdown += `  - Body: ${formatList(body)}\n`;
      markdown += `  - Headers: ${formatList(headers)}\n`;
      if (item.http.condition)
        markdown += `  - Branch condition: \`${item.http.condition}\`\n`;
    }
    if (item.api_handler)
      markdown += `- **Handler:** \`${item.api_handler}\`\n`;
    if (item.storage_operation_ids)
      markdown += `- **Storage operations:** ${formatList(item.storage_operation_ids)}\n`;
    if (item.cache_or_no_query)
      markdown += `- **Cache / no-query path:** ${item.cache_or_no_query}\n`;
    if (item.side_effects)
      markdown += `- **Side effects:** ${item.side_effects}\n`;
    if (item.expected_ui)
      markdown += `- **Expected UI:** ${item.expected_ui}\n`;
    if (item.client_only_reason)
      markdown += `- **Client-only reason:** ${item.client_only_reason}\n`;
    markdown += '- **Runtime result:** `PENDING`\n\n';
  }
  markdown += '## 5. Explicit Gaps\n\n';
  for (const gap of inventory.gaps) {
    markdown += `- **${gap.gap_id ?? gap.id ?? 'unnamed-gap'}** (${gap.classification ?? gap.severity ?? gap.risk ?? 'documented'}): ${gap.description ?? gap.title ?? JSON.stringify(gap)}\n`;
  }
  markdown += '\n## 6. Legacy ID Mapping\n\n';
  markdown += '| Legacy ID | Status | Canonical Target / Endpoint | Reason |\n';
  markdown += '|---|---|---|---|\n';
  for (const mapping of reconciliation.legacy_id_mapping) {
    const target =
      mapping.canonical_id ??
      (mapping.endpoint
        ? `${mapping.endpoint.method} ${mapping.endpoint.path}`
        : '—');
    const reason = mapping.reason ?? mapping.retired_reason ?? '—';
    markdown += `| **${escapeCell(mapping.legacy_id)}** | \`${mapping.status}\` | ${escapeCell(target)} | ${escapeCell(reason)} |\n`;
  }
  return markdown;
}

function compareOutput(file, expected, label) {
  if (!fs.existsSync(file)) {
    fail(`${label} is missing: ${relativeFile(file)}`);
    return;
  }
  const actual = fs.readFileSync(file, 'utf8');
  if (actual !== expected)
    fail(`${label} is stale; run bun scripts/generate-qa-inventory.mjs`);
}

const ui = readJson(contractPaths.ui, 'UI source contract');
const apiRead = readJson(contractPaths.read, 'API read source contract');
const apiWrite = readJson(contractPaths.write, 'API write source contract');
const deployedDelta = fs.existsSync(contractPaths.deployedDelta)
  ? readJson(contractPaths.deployedDelta, 'deployed delta')
  : null;
validateSourceState(ui, 'ui', null);
validateSourceState(apiRead, 'api_read', ui?.source_state);
validateSourceState(apiWrite, 'api_write', ui?.source_state);
validateSourceState(deployedDelta, 'deployed_delta', ui?.source_state);
const responseObservability = apiRead?.response_observability;
const observabilityFields = [
  'scope',
  'response_header',
  'server_timing_metric',
  'correlation_limits',
  'id_issuer',
  'id_format',
  'span',
  'event',
  'handler_timing',
  'source',
];
requiredFields(
  responseObservability,
  observabilityFields,
  'api_read.response_observability',
);
for (const field of observabilityFields)
  requireString(
    responseObservability?.[field],
    `api_read.response_observability.${field}`,
  );
validateEndpointSource(
  responseObservability?.source,
  'api_read.response_observability.source',
  new Set((apiRead?.source_files ?? []).map((entry) => entry.path)),
);
const discoveredUiSources = discoverUiSources();
function mergeOperations(...operationGroups) {
  const byId = new Map();
  for (const operation of operationGroups.flat()) {
    if (!byId.has(operation.id)) {
      byId.set(operation.id, operation);
      continue;
    }
    if (JSON.stringify(byId.get(operation.id)) !== JSON.stringify(operation)) {
      fail(
        `API contracts define conflicting storage operation ${operation.id}`,
      );
    }
  }
  return [...byId.values()];
}

const discoveredUiRoutes = discoverUiRoutes(discoveredUiSources);
const uiValidation = validateUi(ui, discoveredUiSources, discoveredUiRoutes);
const readValidation = validateApiContract(
  apiRead,
  'api_read',
  ui?.source_commit,
);
const writeValidation = validateApiContract(
  apiWrite,
  'api_write',
  ui?.source_commit,
);
const apiEndpoints = [
  ...readValidation.endpoints,
  ...writeValidation.endpoints,
];
const apiOperations = mergeOperations(
  readValidation.operations,
  writeValidation.operations,
);
const apiGaps = [
  ...readValidation.gaps.map((gap) => ({
    source_contract: 'api-read.json',
    ...gap,
  })),
  ...writeValidation.gaps.map((gap) => ({
    source_contract: 'api-write.json',
    ...gap,
  })),
];
const discoveredBackendRoutes = discoverBackendRoutes();
const backendDenominator = validateBackendDenominator(
  discoveredBackendRoutes,
  apiEndpoints,
);
const deployment = parseDeployedDelta(
  deployedDelta,
  ui ?? { source_commit: null },
);
const reconciliation =
  ui && uiValidation
    ? buildReconciliation(
        ui,
        uiValidation,
        apiEndpoints,
        apiGaps,
        discoveredBackendRoutes,
        backendDenominator,
        deployment,
      )
    : null;
const inventory =
  ui && reconciliation && failures.length === 0
    ? buildInventory(
        ui,
        apiEndpoints,
        apiOperations,
        apiGaps,
        deployment,
        reconciliation.source_reconciliation.status,
        responseObservability,
      )
    : null;
if (
  reconciliation &&
  reconciliation.source_reconciliation.status !== 'source_reconciled'
) {
  fail(
    'source reconciliation is incomplete; generated rows cannot be accepted as a complete source catalog',
  );
}
const yamlOutput = inventory
  ? `# cc-lb Admin Web & API Surface Inventory\n# Generated from source contracts; runtime execution remains pending.\n\n${formatYaml(inventory)}\n`
  : '';
const markdownOutput =
  inventory && reconciliation ? renderMarkdown(inventory, reconciliation) : '';
const reconciliationOutput = reconciliation
  ? `${JSON.stringify(reconciliation, null, 2)}\n`
  : '';

if (checkOnly && inventory && reconciliation) {
  compareOutput(outputPaths.yaml, yamlOutput, 'generated YAML');
  compareOutput(outputPaths.markdown, markdownOutput, 'generated Markdown');
  compareOutput(
    outputPaths.reconciliation,
    reconciliationOutput,
    'source reconciliation',
  );
}

if (failures.length > 0) {
  console.error(
    `QA inventory ${checkOnly ? 'check' : 'generation'} failed with ${failures.length} error(s):`,
  );
  for (const message of failures) console.error(`- ${message}`);
  process.exit(1);
}

if (!checkOnly) {
  fs.writeFileSync(outputPaths.yaml, yamlOutput, 'utf8');
  fs.writeFileSync(outputPaths.markdown, markdownOutput, 'utf8');
  fs.writeFileSync(outputPaths.reconciliation, reconciliationOutput, 'utf8');
}

console.log(
  `${checkOnly ? 'Checked' : 'Generated'} ${inventory.metadata.counts.total} rows from ${ui.items.length} UI actions, ${uiValidation.requestKeys.length} UI request occurrences, and ${apiEndpoints.length} API endpoints; source=${reconciliation.source_reconciliation.status}, runtime=${reconciliation.runtime_reconciliation.status}.`,
);
