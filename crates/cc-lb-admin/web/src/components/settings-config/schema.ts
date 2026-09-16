import {
  type ConfigSchema,
  getConfigSchemaVariants,
  resolveConfigSchema,
} from '../../lib/configEditorModel';

export type JsonObject = Record<string, unknown>;

export function isJsonObject(value: unknown): value is JsonObject {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

export function cloneJson<T>(value: T): T {
  return structuredClone(value);
}

export function isSameJson(left: unknown, right: unknown): boolean {
  return JSON.stringify(left) === JSON.stringify(right);
}

export function titleForKey(key: string): string {
  return key
    .replaceAll('_', ' ')
    .replace(/\b\w/g, (letter) => letter.toUpperCase());
}

export function objectProperties(
  root: ConfigSchema,
  input: unknown,
): Record<string, ConfigSchema> {
  if (!isJsonObject(input)) return {};
  const resolved = resolveConfigSchema(root, input);
  const variants = getConfigSchemaVariants(root, resolved);
  const concrete =
    !isJsonObject(resolved.properties) && variants.length === 1
      ? variants[0]?.schema
      : resolved;
  if (!concrete || !isJsonObject(concrete.properties)) return {};
  const properties: Record<string, ConfigSchema> = {};
  for (const [key, value] of Object.entries(concrete.properties)) {
    if (isJsonObject(value)) {
      properties[key] = resolveConfigSchema(root, value);
    }
  }
  return properties;
}
