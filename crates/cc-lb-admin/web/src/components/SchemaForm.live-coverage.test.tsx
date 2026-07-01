import { render } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import liveSchemaFixture from '../lib/test-utils/runtime-schema.fixture.json';
import { SchemaForm } from './SchemaForm';

type Schema = Record<string, unknown>;

function resolveRef(schema: Schema, defs: Record<string, Schema>): Schema {
  if (schema && typeof schema === 'object' && '$ref' in schema) {
    const ref = (schema as { $ref: string }).$ref;
    if (ref.startsWith('#/$defs/')) {
      const name = ref.split('/').pop()!;
      return defs[name] ? resolveRef(defs[name], defs) : schema;
    }
  }
  return schema;
}

function collectLeafPaths(
  schema: Schema,
  defs: Record<string, Schema>,
  prefix: string,
): string[] {
  const node = resolveRef(schema, defs);
  if (!node || typeof node !== 'object') {
    return prefix ? [prefix] : [];
  }
  const props = (node as { properties?: Record<string, Schema> }).properties;
  if (props) {
    const out: string[] = [];
    for (const [key, child] of Object.entries(props)) {
      const path = prefix ? `${prefix}.${key}` : key;
      const sub = resolveRef(child as Schema, defs);
      if (sub && typeof sub === 'object') {
        const subProps = (sub as { properties?: Record<string, Schema> })
          .properties;
        const additional = (sub as { additionalProperties?: unknown })
          .additionalProperties;
        if (subProps) {
          out.push(...collectLeafPaths(sub, defs, path));
          continue;
        }
        if (additional !== undefined) {
          out.push(`${path}:map`);
          continue;
        }
      }
      out.push(path);
    }
    return out;
  }
  return prefix ? [prefix] : [];
}

describe('SchemaForm against live runtime schema', () => {
  it('every leaf path in the live schema gets a data-config-path input', () => {
    const fixture = liveSchemaFixture as unknown as {
      schema: Schema;
      coverage_checklist: string[];
    };
    const defs =
      (fixture.schema as { $defs?: Record<string, Schema> }).$defs ?? {};
    const expectedLeaves = collectLeafPaths(fixture.schema, defs, '');
    expect(expectedLeaves.length).toBeGreaterThan(40);

    const seed: Record<string, unknown> = {};
    const { container } = render(
      <SchemaForm
        schema={fixture.schema}
        value={seed}
        onChange={() => {}}
        path=""
      />,
    );

    const renderedPaths = new Set(
      Array.from(container.querySelectorAll('[data-config-path]')).map(
        (el) => el.getAttribute('data-config-path')!,
      ),
    );

    const missing: string[] = [];
    for (const leaf of expectedLeaves) {
      const base = leaf.replace(/:map$/, '');
      const exact = renderedPaths.has(base);
      const isMapHeader = leaf.endsWith(':map') && renderedPaths.has(base);
      const hasDescendant =
        !exact &&
        Array.from(renderedPaths).some(
          (p) =>
            p === base || p.startsWith(`${base}.`) || p.startsWith(`${base}[`),
        );
      if (!exact && !isMapHeader && !hasDescendant) {
        missing.push(leaf);
      }
    }
    expect(missing, `missing leaves: ${missing.join(', ')}`).toEqual([]);
  });

  it('coverage_checklist top-level keys all appear as schema sections', () => {
    const fixture = liveSchemaFixture as unknown as {
      schema: { properties: Record<string, unknown> };
      coverage_checklist: string[];
    };
    const topLevel = Object.keys(fixture.schema.properties);
    for (const item of fixture.coverage_checklist) {
      expect(topLevel, `missing ${item} from schema`).toContain(item);
    }
  });
});
