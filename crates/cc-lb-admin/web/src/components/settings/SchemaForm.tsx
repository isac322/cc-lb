import { MaskedSecretField } from './MaskedSecretField';
import { FormField } from '../primitives/FormField';
import { AllowedModelsEditor } from '../management/AllowedModelsEditor';

interface SchemaFormProps {
  schema: Record<string, unknown>;
  rootSchema: Record<string, unknown>;
  value: unknown;
  onChange: (value: unknown) => void;
  path: string;
  error?: string;
}

function resolveRef(ref: string, rootSchema: Record<string, unknown>): Record<string, unknown> {
  if (!ref.startsWith('#/')) return {};
  const parts = ref.split('/').slice(1);
  let current: unknown = rootSchema;
  for (const part of parts) {
    if (!current || typeof current !== 'object') return {};
    current = (current as Record<string, unknown>)[part];
  }
  return (current as Record<string, unknown>) || {};
}

function isSecretPath(path: string): boolean {
  const lower = path.toLowerCase();
  return lower.includes('token') ||
         lower.includes('secret') ||
         lower.includes('api_key') ||
         lower.includes('aead_master_key');
}

export function SchemaForm({ schema, rootSchema, value, onChange, path, error }: SchemaFormProps) {
  if (!schema) return null;

  let resolvedSchema = schema;
  if (typeof schema.$ref === 'string') {
    resolvedSchema = resolveRef(schema.$ref, rootSchema);
  } else if (Array.isArray(schema.anyOf) || Array.isArray(schema.oneOf)) {
    const variants = (schema.anyOf || schema.oneOf) as Record<string, unknown>[];
    const nonNullVariant = variants.find((v) => v.type !== 'null');
    if (nonNullVariant) {
      if (typeof nonNullVariant.$ref === 'string') {
        resolvedSchema = resolveRef(nonNullVariant.$ref, rootSchema);
      } else {
        resolvedSchema = nonNullVariant;
      }
    }
  }

  const type = resolvedSchema.type as string | undefined;
  const title = (resolvedSchema.title as string) || path.split('.').pop() || '';
  const description = resolvedSchema.description as string | undefined;

  if (isSecretPath(path)) {
    return (
      <MaskedSecretField
        label={title}
        value={value as string | null}
        onChange={onChange}
        error={error}
        helpText={description}
      />
    );
  }

  if (type === 'string') {
    if (Array.isArray(resolvedSchema.enum)) {
      return (
        <FormField label={title} error={error} help={description}>
          <select
            value={(value as string) || ''}
            onChange={(e) => onChange(e.target.value)}
            className="w-full bg-graphite-900 border border-graphite-700 rounded-md px-3 py-2 text-sm text-graphite-50 focus:outline-none focus:border-blue-500 focus:ring-1 focus:ring-blue-500"
          >
            <option value="">Select...</option>
            {resolvedSchema.enum.map((opt: unknown) => (
              <option key={String(opt)} value={String(opt)}>{String(opt)}</option>
            ))}
          </select>
        </FormField>
      );
    }
    return (
      <FormField label={title} error={error} help={description}>
        <input
          type="text"
          value={(value as string) || ''}
          onChange={(e) => onChange(e.target.value)}
          className="w-full bg-graphite-900 border border-graphite-700 rounded-md px-3 py-2 text-sm text-graphite-50 focus:outline-none focus:border-blue-500 focus:ring-1 focus:ring-blue-500"
        />
      </FormField>
    );
  }

  if (type === 'integer' || type === 'number') {
    return (
      <FormField label={title} error={error} help={description}>
        <input
          type="number"
          value={(value as number) ?? ''}
          onChange={(e) => onChange(e.target.value === '' ? null : Number(e.target.value))}
          min={resolvedSchema.minimum as number | undefined}
          max={resolvedSchema.maximum as number | undefined}
          className="w-full bg-graphite-900 border border-graphite-700 rounded-md px-3 py-2 text-sm text-graphite-50 focus:outline-none focus:border-blue-500 focus:ring-1 focus:ring-blue-500"
        />
      </FormField>
    );
  }

  if (type === 'boolean') {
    return (
      <FormField label={title} error={error} help={description}>
        <label className="flex items-center space-x-3">
          <input
            type="checkbox"
            checked={!!value}
            onChange={(e) => onChange(e.target.checked)}
            className="h-4 w-4 rounded border-graphite-700 bg-graphite-900 text-blue-500 focus:ring-blue-500 focus:ring-offset-graphite-900"
          />
          <span className="text-sm font-medium text-graphite-200">{title}</span>
        </label>
      </FormField>
    );
  }

  if (type === 'array' && (resolvedSchema.items as Record<string, unknown>)?.type === 'string') {
    return (
      <FormField label={title} error={error} help={description}>
        <AllowedModelsEditor
          models={(value as string[]) || []}
          onChange={onChange}
        />
      </FormField>
    );
  }

  if (type === 'object' && resolvedSchema.properties) {
    const props = resolvedSchema.properties as Record<string, Record<string, unknown>>;
    const valObj = (value as Record<string, unknown>) || {};
    return (
      <div className="space-y-4">
        {Object.entries(props).map(([key, propSchema]) => (
          <SchemaForm
            key={key}
            schema={propSchema}
            rootSchema={rootSchema}
            value={valObj[key]}
            onChange={(newVal) => onChange({ ...valObj, [key]: newVal })}
            path={`${path}.${key}`}
            error={error?.startsWith(`${path}.${key}`) ? error : undefined}
          />
        ))}
      </div>
    );
  }

  // Fallback for unknown types or complex objects
  return (
    <FormField label={title} error={error} help={description}>
      <textarea
        value={value ? JSON.stringify(value, null, 2) : ''}
        onChange={(e) => {
          try {
            onChange(JSON.parse(e.target.value));
          } catch {
            // Ignore parse errors while typing
          }
        }}
        className="w-full bg-graphite-900 border border-graphite-700 rounded-md px-3 py-2 text-sm text-graphite-50 focus:outline-none focus:border-blue-500 focus:ring-1 focus:ring-blue-500 font-mono h-32"
      />
    </FormField>
  );
}
