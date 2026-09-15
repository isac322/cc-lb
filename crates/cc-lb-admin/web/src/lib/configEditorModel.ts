import type {
  ConfigEditorResponse,
  ConfigOverrideInfo,
  ConfigValidationIssue,
} from './api';

export type ConfigPathSegment = string | number;
export type ConfigPath = readonly ConfigPathSegment[];
export type ConfigSchema = Record<string, unknown>;

export const OPAQUE_STORAGE_URL_SENTINEL =
  '__CC_LB_STORAGE_URL_UNCHANGED__' as const;

export type ConfigEditorLeafKind =
  | 'string'
  | 'integer'
  | 'number'
  | 'boolean'
  | 'enum'
  | 'array'
  | 'unknown';

export interface ConfigEditorCategoryMetadata {
  id: string;
  label: string;
  description: string;
  roots: readonly string[];
}

export const CONFIG_EDITOR_CATEGORIES = [
  {
    id: 'network',
    label: 'Network & requests',
    description: 'Listeners, request bodies, and request deadlines.',
    roots: ['listener', 'body', 'timeouts'],
  },
  {
    id: 'data',
    label: 'Storage & data',
    description: 'Primary storage, encryption, event transport, and retention.',
    roots: ['storage', 'aead', 'event_bus', 'request_event_retention_days'],
  },
  {
    id: 'scheduling',
    label: 'Scheduling',
    description: 'Worker pools, recurring jobs, and scheduler concurrency.',
    roots: ['scheduler'],
  },
  {
    id: 'routing',
    label: 'Routing & resilience',
    description:
      'Affinity, circuit breaking, bulkheads, caching, and reservations.',
    roots: [
      'upstream_affinity',
      'circuit_breaker',
      'bulkhead',
      'prompt_cache_shadow',
      'limit_reservation_ttl',
    ],
  },
  {
    id: 'identity',
    label: 'Identity & access',
    description: 'Admin authentication, OAuth, and cluster identity.',
    roots: ['admin', 'oauth', 'cluster'],
  },
  {
    id: 'quota',
    label: 'Pricing & quotas',
    description: 'Price catalog and subscription quota collection.',
    roots: ['price_catalog', 'subscription_quota'],
  },
  {
    id: 'runtime',
    label: 'Runtime & observability',
    description: 'Process runtime and observability behavior.',
    roots: ['runtime', 'observability'],
  },
] as const satisfies readonly ConfigEditorCategoryMetadata[];

export interface ConfigSchemaVariant {
  schema: ConfigSchema;
  label: string;
  tag: { property: string; value: string } | null;
}

export interface ConfigSchemaLeaf {
  path: ConfigPath;
  pathString: string;
  schema: ConfigSchema;
  kind: ConfigEditorLeafKind;
  required: boolean;
  nullable: boolean;
  unknown: boolean;
}

export interface ConfigValueResolution {
  editorValue: unknown;
  fileValue: unknown;
  defaultValue: unknown;
  effectiveValue: unknown;
  resolvedFileValue: unknown;
  origin: 'draft' | 'file' | 'default' | 'unset';
  override: ConfigOverrideInfo | null;
}

export interface ConfigEditorLeaf
  extends ConfigSchemaLeaf,
    ConfigValueResolution {
  modified: boolean;
  overridden: boolean;
  issues: ConfigValidationIssue[];
}

export interface ConfigEditorCounts {
  modified: number;
  overrides: number;
  errors: number;
  warnings: number;
}

export interface ConfigEditorCategory extends ConfigEditorCategoryMetadata {
  leaves: ConfigEditorLeaf[];
  counts: ConfigEditorCounts;
}

export interface ConfigEditorModel {
  source: Record<string, unknown>;
  leaves: ConfigEditorLeaf[];
  categories: ConfigEditorCategory[];
  counts: ConfigEditorCounts;
}

type SchemaWalkContext = {
  root: ConfigSchema;
  sources: readonly unknown[];
  leaves: Map<string, ConfigSchemaLeaf>;
  resolving: ReadonlySet<string>;
};

function objectValue(value: unknown): Record<string, unknown> | null {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : null;
}

function schemaRecord(value: unknown): ConfigSchema | null {
  return objectValue(value);
}

function schemaList(value: unknown): ConfigSchema[] {
  if (!Array.isArray(value)) return [];
  const schemas: ConfigSchema[] = [];
  for (const item of value) {
    const schema = schemaRecord(item);
    if (schema) schemas.push(schema);
  }
  return schemas;
}

function schemaTypes(schema: ConfigSchema): string[] {
  const type = schema.type;
  if (typeof type === 'string') return [type];
  return Array.isArray(type)
    ? type.filter((item): item is string => typeof item === 'string')
    : [];
}

function localRefTarget(root: ConfigSchema, ref: string): ConfigSchema | null {
  if (!ref.startsWith('#/')) return null;
  let value: unknown = root;
  for (const encodedSegment of ref.slice(2).split('/')) {
    const object = objectValue(value);
    if (!object) return null;
    const segment = encodedSegment.replaceAll('~1', '/').replaceAll('~0', '~');
    value = object[segment];
  }
  return schemaRecord(value);
}

export function resolveConfigSchema(
  root: ConfigSchema,
  schema: ConfigSchema,
  resolving: ReadonlySet<string> = new Set(),
): ConfigSchema {
  const ref = typeof schema.$ref === 'string' ? schema.$ref : null;
  if (!ref || resolving.has(ref)) return schema;
  const target = localRefTarget(root, ref);
  if (!target) return schema;

  const nextResolving = new Set(resolving);
  nextResolving.add(ref);
  const resolved = resolveConfigSchema(root, target, nextResolving);
  const siblings = Object.fromEntries(
    Object.entries(schema).filter(([key]) => key !== '$ref'),
  );
  return { ...resolved, ...siblings };
}

export function isNullableConfigSchema(
  root: ConfigSchema,
  schema: ConfigSchema,
): boolean {
  const resolved = resolveConfigSchema(root, schema);
  if (schemaTypes(resolved).includes('null')) return true;
  return [...schemaList(resolved.oneOf), ...schemaList(resolved.anyOf)].some(
    (branch) => schemaTypes(resolveConfigSchema(root, branch)).includes('null'),
  );
}

function nonNullVariants(
  root: ConfigSchema,
  schema: ConfigSchema,
): ConfigSchema[] {
  const resolved = resolveConfigSchema(root, schema);
  const variants = [
    ...schemaList(resolved.oneOf),
    ...schemaList(resolved.anyOf),
  ].filter(
    (branch) =>
      !schemaTypes(resolveConfigSchema(root, branch)).includes('null'),
  );
  return variants.length > 0 ? variants : [resolved];
}

function variantTag(root: ConfigSchema, schema: ConfigSchema) {
  const resolved = resolveConfigSchema(root, schema);
  const properties = schemaRecord(resolved.properties);
  if (!properties) return null;
  for (const [property, propertyValue] of Object.entries(properties)) {
    const propertySchema = schemaRecord(propertyValue);
    if (!propertySchema) continue;
    const concrete = resolveConfigSchema(root, propertySchema);
    const value =
      typeof concrete.const === 'string'
        ? concrete.const
        : Array.isArray(concrete.enum) &&
            concrete.enum.length === 1 &&
            typeof concrete.enum[0] === 'string'
          ? concrete.enum[0]
          : null;
    if (value !== null) return { property, value };
  }
  return null;
}

export function getConfigSchemaVariants(
  root: ConfigSchema,
  schema: ConfigSchema,
): ConfigSchemaVariant[] {
  return nonNullVariants(root, schema).map((branch, index) => {
    const resolved = resolveConfigSchema(root, branch);
    const tag = variantTag(root, resolved);
    return {
      schema: resolved,
      label:
        (typeof resolved.title === 'string' && resolved.title) ||
        tag?.value ||
        `Option ${index + 1}`,
      tag,
    };
  });
}

export function configPathToString(path: ConfigPath): string {
  return path.reduce<string>((result, segment) => {
    if (typeof segment === 'number') return `${result}[${segment}]`;
    if (segment === '*') return result ? `${result}.*` : '*';
    return result ? `${result}.${segment}` : segment;
  }, '');
}

export function parseConfigPath(
  path: string | ConfigPath,
): ConfigPathSegment[] {
  if (typeof path !== 'string') return [...path];
  if (!path) return [];
  const segments: ConfigPathSegment[] = [];
  for (const part of path.split('.')) {
    const property = part.match(/^[^[]+/)?.[0];
    if (property) segments.push(property);
    for (const match of part.matchAll(/\[(\d+)]/g)) {
      segments.push(Number(match[1]));
    }
  }
  return segments;
}

export function getConfigValue(
  value: unknown,
  path: string | ConfigPath,
): unknown {
  let current = value;
  for (const segment of parseConfigPath(path)) {
    if (segment === '*') return undefined;
    if (typeof segment === 'number') {
      if (!Array.isArray(current)) return undefined;
      current = current[segment];
    } else {
      const object = objectValue(current);
      if (!object) return undefined;
      current = object[segment];
    }
  }
  return current;
}

export function setConfigValue(
  value: unknown,
  path: string | ConfigPath,
  nextValue: unknown,
): unknown {
  const segments = parseConfigPath(path);
  if (segments.length === 0) return nextValue;

  const update = (current: unknown, index: number): unknown => {
    const segment = segments[index];
    if (segment === undefined || segment === '*') return current;
    const last = index === segments.length - 1;
    const object = objectValue(current);
    const clone = Array.isArray(current)
      ? [...current]
      : object
        ? { ...object }
        : typeof segment === 'number'
          ? []
          : {};
    if (typeof segment === 'number') {
      if (!Array.isArray(clone)) return current;
      clone[segment] = last ? nextValue : update(clone[segment], index + 1);
    } else {
      if (Array.isArray(clone)) return current;
      clone[segment] = last ? nextValue : update(clone[segment], index + 1);
    }
    return clone;
  };

  return update(value, 0);
}

export function unsetConfigValue(
  value: unknown,
  path: string | ConfigPath,
): unknown {
  const segments = parseConfigPath(path);
  if (segments.length === 0) return {};

  const update = (current: unknown, index: number): unknown => {
    const segment = segments[index];
    if (segment === undefined || segment === '*') return current;
    const last = index === segments.length - 1;
    if (typeof segment === 'number') {
      if (!Array.isArray(current)) return current;
      const clone = [...current];
      if (last) clone.splice(segment, 1);
      else clone[segment] = update(clone[segment], index + 1);
      return clone;
    }
    const object = objectValue(current);
    if (!object || !(segment in object)) return current;
    const clone = { ...object };
    if (last) delete clone[segment];
    else clone[segment] = update(clone[segment], index + 1);
    return clone;
  };

  return update(value, 0);
}

function schemaForProperty(
  root: ConfigSchema,
  schema: ConfigSchema,
  property: string,
): ConfigSchema | null {
  for (const variant of nonNullVariants(root, schema)) {
    const resolved = resolveConfigSchema(root, variant);
    const properties = schemaRecord(resolved.properties);
    const propertySchema = properties && schemaRecord(properties[property]);
    if (propertySchema) return propertySchema;
    const additional = schemaRecord(resolved.additionalProperties);
    if (additional) return additional;
  }
  return null;
}

function schemaForArrayItem(
  root: ConfigSchema,
  schema: ConfigSchema,
): ConfigSchema | null {
  for (const variant of nonNullVariants(root, schema)) {
    const resolved = resolveConfigSchema(root, variant);
    const items = schemaRecord(resolved.items);
    if (items) return items;
  }
  return null;
}

function normalizeValue(
  root: ConfigSchema,
  schema: ConfigSchema,
  value: unknown,
): unknown {
  if (value === null || value === undefined) return undefined;
  if (Array.isArray(value)) {
    const itemSchema = schemaForArrayItem(root, schema) ?? {};
    const normalized = value
      .map((item) => normalizeValue(root, itemSchema, item))
      .filter((item) => item !== undefined);
    return normalized;
  }
  const object = objectValue(value);
  if (!object) return value;

  const normalized: Record<string, unknown> = {};
  for (const [key, child] of Object.entries(object)) {
    const childSchema = schemaForProperty(root, schema, key) ?? {};
    const normalizedChild = normalizeValue(root, childSchema, child);
    if (normalizedChild !== undefined) normalized[key] = normalizedChild;
  }
  return normalized;
}

export function normalizeConfigDraft(
  schema: ConfigSchema,
  draft: Record<string, unknown>,
): Record<string, unknown> {
  return normalizeValue(schema, schema, draft) as Record<string, unknown>;
}

function inferredSchema(value: unknown): ConfigSchema {
  if (Array.isArray(value)) return { type: 'array' };
  if (objectValue(value)) return { type: 'object' };
  if (value === null) return { type: ['null', 'string'] };
  return { type: typeof value };
}

function valuesAt(sources: readonly unknown[], path: ConfigPath): unknown[] {
  return sources
    .map((source) => getConfigValue(source, path))
    .filter((value) => value !== undefined && value !== null);
}

function leafKind(
  root: ConfigSchema,
  schema: ConfigSchema,
): ConfigEditorLeafKind {
  const candidates = nonNullVariants(root, schema).map((candidate) =>
    resolveConfigSchema(root, candidate),
  );
  for (const candidate of candidates) {
    if ('const' in candidate || Array.isArray(candidate.enum)) return 'enum';
  }
  for (const candidate of candidates) {
    const type = schemaTypes(candidate).find((item) => item !== 'null');
    if (
      type === 'string' ||
      type === 'integer' ||
      type === 'number' ||
      type === 'boolean' ||
      type === 'array'
    ) {
      return type;
    }
  }
  return 'unknown';
}

function addLeaf(
  context: SchemaWalkContext,
  path: ConfigPath,
  schema: ConfigSchema,
  required: boolean,
  unknown: boolean,
): void {
  const pathString = configPathToString(path);
  const existing = context.leaves.get(pathString);
  if (existing) {
    const schemas = schemaList(existing.schema.oneOf);
    context.leaves.set(pathString, {
      ...existing,
      schema: {
        oneOf: [...(schemas.length > 0 ? schemas : [existing.schema]), schema],
      },
      required: existing.required || required,
      nullable:
        existing.nullable || isNullableConfigSchema(context.root, schema),
      unknown: existing.unknown && unknown,
    });
    return;
  }
  context.leaves.set(pathString, {
    path: [...path],
    pathString,
    schema,
    kind: leafKind(context.root, schema),
    required,
    nullable: isNullableConfigSchema(context.root, schema),
    unknown,
  });
}

function walkSchema(
  context: SchemaWalkContext,
  schema: ConfigSchema,
  path: ConfigPath,
  required: boolean,
  unknown: boolean,
): void {
  const ref = typeof schema.$ref === 'string' ? schema.$ref : null;
  if (ref && context.resolving.has(ref)) {
    addLeaf(context, path, schema, required, unknown);
    return;
  }
  const activeContext =
    ref === null
      ? context
      : {
          ...context,
          resolving: new Set(context.resolving).add(ref),
        };
  const resolved = resolveConfigSchema(activeContext.root, schema);
  const declaredVariants = [
    ...schemaList(resolved.oneOf),
    ...schemaList(resolved.anyOf),
  ].filter(
    (branch) =>
      !schemaTypes(resolveConfigSchema(activeContext.root, branch)).includes(
        'null',
      ),
  );
  if (declaredVariants.length > 0) {
    const containsObject = declaredVariants.some((variant) => {
      const concrete = resolveConfigSchema(activeContext.root, variant);
      if (
        schemaTypes(concrete).includes('object') ||
        schemaRecord(concrete.properties) !== null
      ) {
        return true;
      }
      const itemSchema = schemaRecord(concrete.items);
      return (
        schemaTypes(concrete).includes('array') &&
        itemSchema !== null &&
        nonNullVariants(activeContext.root, itemSchema).some((item) => {
          const resolvedItem = resolveConfigSchema(activeContext.root, item);
          return (
            schemaTypes(resolvedItem).includes('object') ||
            schemaRecord(resolvedItem.properties) !== null
          );
        })
      );
    });
    if (!containsObject) {
      addLeaf(activeContext, path, resolved, required, unknown);
      return;
    }
    for (const variant of declaredVariants) {
      walkSchema(activeContext, variant, path, required, unknown);
    }
    return;
  }

  const concrete = resolved;
  const types = schemaTypes(concrete).filter((type) => type !== 'null');
  const properties = schemaRecord(concrete.properties);
  const isObject = types.includes('object') || properties !== null;
  if (isObject) {
    const requiredProperties = new Set(
      Array.isArray(concrete.required)
        ? concrete.required.filter(
            (item): item is string => typeof item === 'string',
          )
        : [],
    );
    const knownProperties = properties ?? {};
    for (const [property, propertyValue] of Object.entries(knownProperties)) {
      const propertySchema = schemaRecord(propertyValue);
      if (!propertySchema) continue;
      walkSchema(
        activeContext,
        propertySchema,
        [...path, property],
        requiredProperties.has(property),
        unknown,
      );
    }

    const sourceKeys = new Set<string>();
    for (const value of valuesAt(activeContext.sources, path)) {
      const object = objectValue(value);
      if (!object) continue;
      for (const key of Object.keys(object)) sourceKeys.add(key);
    }
    const additionalSchema = schemaRecord(concrete.additionalProperties);
    for (const key of sourceKeys) {
      if (key in knownProperties) continue;
      const samples = valuesAt(activeContext.sources, [...path, key]);
      walkSchema(
        activeContext,
        additionalSchema ?? inferredSchema(samples[0]),
        [...path, key],
        false,
        additionalSchema === null,
      );
    }
    if (additionalSchema && sourceKeys.size === 0) {
      walkSchema(activeContext, additionalSchema, [...path, '*'], false, false);
    }
    if (
      Object.keys(knownProperties).length === 0 &&
      !additionalSchema &&
      sourceKeys.size === 0
    ) {
      addLeaf(activeContext, path, concrete, required, unknown);
    }
    return;
  }

  if (types.includes('array')) {
    const itemSchema = schemaRecord(concrete.items);
    if (!itemSchema) {
      addLeaf(activeContext, path, concrete, required, unknown);
      return;
    }
    const itemTypes = nonNullVariants(activeContext.root, itemSchema).flatMap(
      (item) => schemaTypes(resolveConfigSchema(activeContext.root, item)),
    );
    const itemHasProperties = nonNullVariants(
      activeContext.root,
      itemSchema,
    ).some(
      (item) =>
        schemaRecord(
          resolveConfigSchema(activeContext.root, item).properties,
        ) !== null,
    );
    if (!itemTypes.includes('object') && !itemHasProperties) {
      addLeaf(activeContext, path, concrete, required, unknown);
      return;
    }

    const indexes = new Set<number>();
    for (const value of valuesAt(activeContext.sources, path)) {
      if (!Array.isArray(value)) continue;
      for (let index = 0; index < value.length; index += 1) indexes.add(index);
    }
    if (indexes.size === 0) {
      walkSchema(activeContext, itemSchema, [...path, '*'], false, unknown);
    } else {
      for (const index of indexes) {
        walkSchema(activeContext, itemSchema, [...path, index], false, unknown);
      }
    }
    return;
  }

  addLeaf(activeContext, path, concrete, required, unknown);
}

export function expandConfigSchema(
  schema: ConfigSchema,
  ...sources: readonly unknown[]
): ConfigSchemaLeaf[] {
  const leaves = new Map<string, ConfigSchemaLeaf>();
  walkSchema(
    { root: schema, sources, leaves, resolving: new Set() },
    schema,
    [],
    true,
    false,
  );
  return [...leaves.values()].sort((left, right) =>
    left.pathString.localeCompare(right.pathString),
  );
}

function valuesEqual(left: unknown, right: unknown): boolean {
  if (Object.is(left, right)) return true;
  if (Array.isArray(left) && Array.isArray(right)) {
    return (
      left.length === right.length &&
      left.every((item, index) => valuesEqual(item, right[index]))
    );
  }
  const leftObject = objectValue(left);
  const rightObject = objectValue(right);
  if (leftObject && rightObject) {
    const leftKeys = Object.keys(leftObject);
    const rightKeys = Object.keys(rightObject);
    return (
      leftKeys.length === rightKeys.length &&
      leftKeys.every(
        (key) =>
          key in rightObject && valuesEqual(leftObject[key], rightObject[key]),
      )
    );
  }
  return false;
}

function overrideForPath(
  overrides: readonly ConfigOverrideInfo[],
  pathString: string,
): ConfigOverrideInfo | null {
  return (
    overrides.find(
      (override) =>
        override.path === pathString ||
        override.path.startsWith(`${pathString}.`) ||
        override.path.startsWith(`${pathString}[`) ||
        pathString.startsWith(`${override.path}.`) ||
        pathString.startsWith(`${override.path}[`),
    ) ?? null
  );
}

export function resolveConfigValue(
  path: string | ConfigPath,
  values: {
    draft: Record<string, unknown> | null;
    editor?: Record<string, unknown>;
    file: Record<string, unknown>;
    defaults: Record<string, unknown>;
    effective: Record<string, unknown>;
    overrides?: readonly ConfigOverrideInfo[];
  },
): ConfigValueResolution {
  const parsedPath = parseConfigPath(path);
  const fileValue = getConfigValue(values.file, parsedPath);
  const defaultValue = getConfigValue(values.defaults, parsedPath);
  const effectiveValue = getConfigValue(values.effective, parsedPath);
  const editorSource = values.editor ?? values.draft ?? values.file;
  const editorValue = getConfigValue(editorSource, parsedPath);
  const editorSet = editorValue !== undefined && editorValue !== null;
  const edited =
    (values.draft !== null || values.editor !== undefined) &&
    !valuesEqual(editorValue, fileValue);
  return {
    editorValue,
    fileValue,
    defaultValue,
    effectiveValue,
    resolvedFileValue: editorSet ? editorValue : defaultValue,
    origin: editorSet
      ? edited
        ? 'draft'
        : 'file'
      : defaultValue !== undefined
        ? 'default'
        : 'unset',
    override: overrideForPath(
      values.overrides ?? [],
      configPathToString(parsedPath),
    ),
  };
}

function allValidationIssues(
  response: ConfigEditorResponse,
): ConfigValidationIssue[] {
  const report = response.last_validation;
  return report
    ? [
        ...report.file.issues,
        ...report.effective.issues,
        ...report.filesystem,
        ...report.overrides,
      ]
    : [];
}

function issuesForPath(
  issues: readonly ConfigValidationIssue[],
  pathString: string,
): ConfigValidationIssue[] {
  return issues.filter(
    (issue) =>
      issue.path === pathString ||
      issue.path.startsWith(`${pathString}.`) ||
      pathString.startsWith(`${issue.path}.`) ||
      issue.path.startsWith(`${pathString}[`) ||
      pathString.startsWith(`${issue.path}[`),
  );
}

export function countConfigValidationIssues(
  issues: readonly ConfigValidationIssue[],
): Pick<ConfigEditorCounts, 'errors' | 'warnings'> {
  const issueKeys = new Set<string>();
  let errors = 0;
  let warnings = 0;
  for (const issue of issues) {
    const key = `${issue.severity}\u0000${issue.path}\u0000${issue.code}\u0000${issue.message}`;
    if (issueKeys.has(key)) continue;
    issueKeys.add(key);
    if (issue.severity === 'error') errors += 1;
    else warnings += 1;
  }
  return { errors, warnings };
}

export function countConfigEditorLeaves(
  leaves: readonly ConfigEditorLeaf[],
): ConfigEditorCounts {
  const issues = leaves.flatMap((leaf) => leaf.issues);
  const validationCounts = countConfigValidationIssues(issues);
  return {
    modified: leaves.filter((leaf) => leaf.modified).length,
    overrides: new Set(
      leaves
        .map((leaf) => leaf.override?.path)
        .filter((path): path is string => path !== undefined),
    ).size,
    ...validationCounts,
  };
}

export function buildConfigEditorModel(
  response: ConfigEditorResponse,
  draft: Record<string, unknown> = response.draft ?? response.file_config,
): ConfigEditorModel {
  const source = draft;
  const issues = allValidationIssues(response);
  const leaves = expandConfigSchema(
    response.schema,
    response.default_config,
    response.file_config,
    response.effective_config,
    source,
  ).map<ConfigEditorLeaf>((leaf) => {
    const resolution = resolveConfigValue(leaf.path, {
      draft: response.draft,
      editor: source,
      file: response.file_config,
      defaults: response.default_config,
      effective: response.effective_config,
      overrides: response.overrides,
    });
    return {
      ...leaf,
      ...resolution,
      modified: !valuesEqual(resolution.editorValue, resolution.fileValue),
      overridden: resolution.override !== null,
      issues: issuesForPath(issues, leaf.pathString),
    };
  });
  const categories = CONFIG_EDITOR_CATEGORIES.map((metadata) => {
    const roots: readonly string[] = metadata.roots;
    const categoryLeaves = leaves.filter(
      (leaf) =>
        typeof leaf.path[0] === 'string' && roots.includes(leaf.path[0]),
    );
    return {
      ...metadata,
      leaves: categoryLeaves,
      counts: countConfigEditorLeaves(categoryLeaves),
    };
  });
  const counts = countConfigEditorLeaves(leaves);
  const validationCounts = countConfigValidationIssues(issues);
  return {
    source,
    leaves,
    categories,
    counts: {
      ...counts,
      overrides: new Set(response.overrides.map((override) => override.path))
        .size,
      ...validationCounts,
    },
  };
}
