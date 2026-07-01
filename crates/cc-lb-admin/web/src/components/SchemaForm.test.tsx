import { render, screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import { SchemaForm } from './SchemaForm';

describe('SchemaForm', () => {
  it('renders_input_for_every_schema_leaf_with_data_config_path', () => {
    const schema = {
      type: 'object',
      properties: {
        str_field: { type: 'string' },
        num_field: { type: 'number' },
        bool_field: { type: 'boolean' },
        enum_field: { enum: ['a', 'b'] },
        nested: {
          type: 'object',
          properties: {
            inner_str: { type: 'string' },
          },
        },
      },
    };

    const value = {
      str_field: 'hello',
      num_field: 42,
      bool_field: true,
      enum_field: 'a',
      nested: {
        inner_str: 'world',
      },
    };

    const onChange = vi.fn();

    render(
      <SchemaForm
        schema={schema}
        value={value}
        onChange={onChange}
        path="test_root"
      />,
    );

    const strInput = screen.getByDisplayValue('hello');
    expect(strInput.getAttribute('data-config-path')).toBe(
      'test_root.str_field',
    );

    const numInput = screen.getByDisplayValue('42');
    expect(numInput.getAttribute('data-config-path')).toBe(
      'test_root.num_field',
    );

    const boolInput = screen.getByRole('checkbox');
    expect(boolInput.getAttribute('data-config-path')).toBe(
      'test_root.bool_field',
    );

    const enumInput = screen.getByDisplayValue('a');
    expect(enumInput.getAttribute('data-config-path')).toBe(
      'test_root.enum_field',
    );

    const innerStrInput = screen.getByDisplayValue('world');
    expect(innerStrInput.getAttribute('data-config-path')).toBe(
      'test_root.nested.inner_str',
    );
  });
});
