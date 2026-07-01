import { Plus, Trash2 } from 'lucide-react';
import { Button, Field, IconButton, INPUT_CLASS } from './ui/primitives';

export interface SchemaFormProps {
  schema: any;
  value: any;
  onChange: (value: any) => void;
  path: string;
  rootSchema?: any;
}

function resolveRef(ref: string, rootSchema: any) {
  if (!ref.startsWith('#/')) return {};
  const parts = ref.split('/').slice(1);
  let current = rootSchema;
  for (const part of parts) {
    if (!current) return {};
    current = current[part];
  }
  return current || {};
}

export function SchemaForm({
  schema,
  value,
  onChange,
  path,
  rootSchema,
}: SchemaFormProps) {
  const actualRootSchema = rootSchema || schema;
  let currentSchema = schema;

  if (currentSchema.$ref) {
    currentSchema = {
      ...resolveRef(currentSchema.$ref, actualRootSchema),
      ...currentSchema,
    };
  }

  if (currentSchema.anyOf) {
    const nonNullSchemas = currentSchema.anyOf.filter(
      (s: any) => s.type !== 'null',
    );
    if (nonNullSchemas.length === 1) {
      currentSchema = { ...nonNullSchemas[0], ...currentSchema };
      delete currentSchema.anyOf;
    }
  }

  if (currentSchema.$ref) {
    currentSchema = {
      ...resolveRef(currentSchema.$ref, actualRootSchema),
      ...currentSchema,
    };
    delete currentSchema.$ref;
  }

  if (Array.isArray(currentSchema.type)) {
    const nonNull = currentSchema.type.filter((t: string) => t !== 'null');
    if (nonNull.length === 1) {
      currentSchema = { ...currentSchema, type: nonNull[0] };
    }
  }

  const type = currentSchema.type;
  const description = currentSchema.description;
  const defaultValue = currentSchema.default;

  const renderField = (content: React.ReactNode) => {
    if (!description) return content;
    return (
      <div className="flex flex-col gap-1">
        {content}
        <span className="text-[10px] text-text-faint">{description}</span>
      </div>
    );
  };

  if (currentSchema.enum) {
    return renderField(
      <select
        className={INPUT_CLASS}
        value={value ?? ''}
        onChange={(e) => onChange(e.target.value)}
        data-config-path={path}
      >
        <option value="" disabled>
          Select...
        </option>
        {currentSchema.enum.map((opt: string) => (
          <option key={opt} value={opt}>
            {opt}
          </option>
        ))}
      </select>,
    );
  }

  if (type === 'string') {
    return renderField(
      <input
        type="text"
        className={INPUT_CLASS}
        value={value ?? ''}
        onChange={(e) => onChange(e.target.value)}
        placeholder={defaultValue !== undefined ? String(defaultValue) : ''}
        data-config-path={path}
      />,
    );
  }

  if (type === 'number' || type === 'integer') {
    // u64/i64 might be large, use text input with pattern
    return renderField(
      <input
        type="text"
        pattern="[0-9]*"
        className={INPUT_CLASS}
        value={value ?? ''}
        onChange={(e) => {
          const val = e.target.value;
          if (val === '') onChange(undefined);
          else if (!isNaN(Number(val))) onChange(Number(val));
          else onChange(val); // keep string if it's a huge number or invalid
        }}
        placeholder={defaultValue !== undefined ? String(defaultValue) : ''}
        data-config-path={path}
      />,
    );
  }

  if (type === 'boolean') {
    return renderField(
      <input
        type="checkbox"
        checked={value ?? false}
        onChange={(e) => onChange(e.target.checked)}
        data-config-path={path}
        className="w-4 h-4 rounded-sm border-subtle bg-bg text-accent focus:ring-accent focus:ring-offset-bg"
      />,
    );
  }

  if (type === 'array') {
    const itemsSchema = currentSchema.items || {};
    const arr = Array.isArray(value) ? value : [];
    return renderField(
      <div
        className="flex flex-col gap-2 border border-subtle p-3 rounded-sm bg-overlay-1"
        data-config-path={path}
      >
        {arr.map((item, idx) => (
          <div key={idx} className="flex items-start gap-2">
            <div className="flex-1">
              <SchemaForm
                schema={itemsSchema}
                value={item}
                onChange={(newVal) => {
                  const newArr = [...arr];
                  newArr[idx] = newVal;
                  onChange(newArr);
                }}
                path={`${path}[${idx}]`}
                rootSchema={actualRootSchema}
              />
            </div>
            <IconButton
              label="Remove item"
              onClick={() => {
                const newArr = [...arr];
                newArr.splice(idx, 1);
                onChange(newArr);
              }}
            >
              <Trash2 className="w-4 h-4" />
            </IconButton>
          </div>
        ))}
        <Button
          size="sm"
          variant="ghost"
          iconLeft={<Plus className="w-3 h-3" />}
          onClick={() => onChange([...arr, undefined])}
          className="self-start"
        >
          Add item
        </Button>
      </div>,
    );
  }

  if (
    type === 'object' ||
    currentSchema.properties ||
    currentSchema.additionalProperties
  ) {
    // HashMap case
    if (currentSchema.additionalProperties && !currentSchema.properties) {
      const obj = value || {};
      const entries = Object.entries(obj);
      return renderField(
        <div
          className="flex flex-col gap-2 border border-subtle p-3 rounded-sm bg-overlay-1"
          data-config-path={path}
        >
          {entries.map(([k, v], idx) => (
            <div key={idx} className="flex items-start gap-2">
              <input
                type="text"
                className={INPUT_CLASS}
                style={{ width: '150px' }}
                value={k}
                onChange={(e) => {
                  const newKey = e.target.value;
                  const newObj = { ...obj };
                  delete newObj[k];
                  newObj[newKey] = v;
                  onChange(newObj);
                }}
                placeholder="Key"
              />
              <div className="flex-1">
                <SchemaForm
                  schema={currentSchema.additionalProperties}
                  value={v}
                  onChange={(newVal) => {
                    const newObj = { ...obj };
                    newObj[k] = newVal;
                    onChange(newObj);
                  }}
                  path={`${path}.${k}`}
                  rootSchema={actualRootSchema}
                />
              </div>
              <IconButton
                label="Remove key"
                onClick={() => {
                  const newObj = { ...obj };
                  delete newObj[k];
                  onChange(newObj);
                }}
              >
                <Trash2 className="w-4 h-4" />
              </IconButton>
            </div>
          ))}
          <Button
            size="sm"
            variant="ghost"
            iconLeft={<Plus className="w-3 h-3" />}
            onClick={() => {
              const newObj = { ...obj };
              let newKey = 'new_key';
              let i = 1;
              while (newKey in newObj) {
                newKey = `new_key_${i}`;
                i++;
              }
              newObj[newKey] = undefined;
              onChange(newObj);
            }}
            className="self-start"
          >
            Add key
          </Button>
        </div>,
      );
    }

    // Regular object
    const props = currentSchema.properties || {};
    const obj = value || {};
    return renderField(
      <div className="flex flex-col gap-3">
        {Object.entries(props).map(([k, propSchema]: [string, any]) => (
          <Field key={k} label={k}>
            <SchemaForm
              schema={propSchema}
              value={obj[k]}
              onChange={(newVal) => {
                const newObj = { ...obj };
                if (newVal === undefined) {
                  delete newObj[k];
                } else {
                  newObj[k] = newVal;
                }
                onChange(newObj);
              }}
              path={path ? `${path}.${k}` : k}
              rootSchema={actualRootSchema}
            />
          </Field>
        ))}
      </div>,
    );
  }

  return renderField(
    <div className="text-xs text-red-400">Unsupported schema type: {type}</div>,
  );
}
