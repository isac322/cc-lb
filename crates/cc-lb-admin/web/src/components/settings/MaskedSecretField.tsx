import { useState } from 'react';
import { FormField } from '../primitives/FormField';

interface MaskedSecretFieldProps {
  label: string;
  value: string | null;
  onChange: (value: string) => void;
  error?: string;
  helpText?: string;
}

export function MaskedSecretField({
  label,
  value,
  onChange,
  error,
  helpText,
}: MaskedSecretFieldProps) {
  const [show, setShow] = useState(false);
  const isRedacted = value === '<redacted>' || value === '[REDACTED]';

  return (
    <FormField label={label} error={error} help={helpText}>
      <div className="relative">
        {isRedacted ? (
          <div className="flex items-center space-x-2">
            <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-graphite-800 text-graphite-300">
              redacted
            </span>
            <button
              type="button"
              onClick={() => onChange('')}
              className="text-xs text-blue-400 hover:text-blue-300"
            >
              Replace
            </button>
          </div>
        ) : (
          <>
            <input
              type={show ? 'text' : 'password'}
              value={value || ''}
              onChange={(e) => onChange(e.target.value)}
              className="w-full bg-graphite-900 border border-graphite-700 rounded-md px-3 py-2 text-sm text-graphite-50 focus:outline-none focus:border-blue-500 focus:ring-1 focus:ring-blue-500 pr-10"
              placeholder="${ENV_VAR_NAME}"
            />
            <button
              type="button"
              onClick={() => setShow(!show)}
              className="absolute inset-y-0 right-0 pr-3 flex items-center text-graphite-400 hover:text-graphite-300"
            >
              {show ? (
                <svg
                  xmlns="http://www.w3.org/2000/svg"
                  width="16"
                  height="16"
                  viewBox="0 0 24 24"
                  fill="none"
                  stroke="currentColor"
                  strokeWidth="2"
                  strokeLinecap="round"
                  strokeLinejoin="round"
                >
                  <path d="M17.94 17.94A10.07 10.07 0 0 1 12 20c-7 0-11-8-11-8a18.45 18.45 0 0 1 5.06-5.94M9.9 4.24A9.12 9.12 0 0 1 12 4c7 0 11 8 11 8a18.5 18.5 0 0 1-2.16 3.19m-6.72-1.07a3 3 0 1 1-4.24-4.24" />
                  <line x1="1" y1="1" x2="23" y2="23" />
                </svg>
              ) : (
                <svg
                  xmlns="http://www.w3.org/2000/svg"
                  width="16"
                  height="16"
                  viewBox="0 0 24 24"
                  fill="none"
                  stroke="currentColor"
                  strokeWidth="2"
                  strokeLinecap="round"
                  strokeLinejoin="round"
                >
                  <path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z" />
                  <circle cx="12" cy="12" r="3" />
                </svg>
              )}
            </button>
          </>
        )}
      </div>
    </FormField>
  );
}
