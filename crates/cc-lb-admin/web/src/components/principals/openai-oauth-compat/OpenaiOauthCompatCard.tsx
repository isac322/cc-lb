import { useEffect, useMemo, useState } from 'react';
import { toast } from 'sonner';
import { ApiError } from '../../../lib/api';
import type {
  OpenaiOauthCompatOverride,
  OpenaiOauthCompatPolicy,
  Principal,
} from '../../../lib/queries';
import {
  useConfigEditor,
  usePrincipalWritePending,
  useUpdatePrincipalOpenaiOauthCompat,
} from '../../../lib/queries';
import { DetailSection } from '../../ui/DetailPane';
import { Button, ConfirmDialog } from '../../ui/primitives';
import { Select, type SelectOption } from '../../ui/Select';

const SERVER_DEFAULTS: OpenaiOauthCompatPolicy = {
  store_true: 'coerce',
  sampling: 'drop',
  logprobs: 'drop',
  truncation_auto: 'emulate',
  prompt_cache_in_memory: 'drop',
  service_tier: 'coerce',
  web_search_preview: 'coerce',
  metadata: 'drop_echo',
  max_tool_calls: 'drop',
  background: 'drop',
  hosted_tools: 'drop_tool',
  compact: 'emulate',
  chat_n: 'emulate',
  chat_stop: 'emulate',
  chat_seed_penalties: 'drop',
  transcription: 'disabled',
};

type CompatKey = keyof OpenaiOauthCompatPolicy;
type CompatValue = OpenaiOauthCompatPolicy[CompatKey];

type CompatField = {
  key: CompatKey;
  label: string;
  description: string;
  values: readonly CompatValue[];
};

const FIELDS: readonly CompatField[] = [
  {
    key: 'store_true',
    label: 'Store requests marked true',
    description: 'Controls requests that ask OpenAI to store a response.',
    values: ['coerce', 'reject'],
  },
  {
    key: 'sampling',
    label: 'Sampling controls',
    description: 'Handles temperature and top-p on subscription upstreams.',
    values: ['drop', 'reject'],
  },
  {
    key: 'logprobs',
    label: 'Log probabilities',
    description: 'Handles log-probability fields in responses requests.',
    values: ['drop', 'reject'],
  },
  {
    key: 'truncation_auto',
    label: 'Automatic truncation',
    description: 'Handles truncation: auto when context is too large.',
    values: ['emulate', 'drop', 'reject'],
  },
  {
    key: 'prompt_cache_in_memory',
    label: 'In-memory prompt cache retention',
    description: 'Handles requests that ask for in-memory prompt caching.',
    values: ['drop', 'reject'],
  },
  {
    key: 'service_tier',
    label: 'Service tier',
    description: 'Handles auto and flex service-tier requests.',
    values: ['coerce', 'reject'],
  },
  {
    key: 'web_search_preview',
    label: 'Web search preview tool',
    description: 'Handles the preview web-search tool on responses.',
    values: ['coerce', 'reject'],
  },
  {
    key: 'metadata',
    label: 'Request metadata',
    description: 'Controls metadata, user, and safety identifier fields.',
    values: ['drop_echo', 'drop', 'reject'],
  },
  {
    key: 'max_tool_calls',
    label: 'Maximum tool calls',
    description: 'Handles the max tool calls request limit.',
    values: ['drop', 'reject'],
  },
  {
    key: 'background',
    label: 'Background responses',
    description: 'Handles background responses without an API-key upstream.',
    values: ['drop', 'reject'],
  },
  {
    key: 'hosted_tools',
    label: 'Hosted tools',
    description: 'Handles file search and code interpreter tools.',
    values: ['drop_tool', 'reject'],
  },
  {
    key: 'compact',
    label: 'Compact responses',
    description: 'Handles the responses compact endpoint.',
    values: ['emulate', 'reject'],
  },
  {
    key: 'chat_n',
    label: 'Multiple chat choices',
    description: 'Handles Chat Completions requests with n greater than one.',
    values: ['emulate', 'reject'],
  },
  {
    key: 'chat_stop',
    label: 'Chat stop sequences',
    description: 'Handles stop sequences in Chat Completions requests.',
    values: ['emulate', 'drop', 'reject'],
  },
  {
    key: 'chat_seed_penalties',
    label: 'Chat seed and penalties',
    description: 'Handles seed, frequency penalty, and presence penalty.',
    values: ['drop', 'reject'],
  },
  {
    key: 'transcription',
    label: 'Audio transcription',
    description:
      'Controls audio transcription through the subscription dictation endpoint. The upstream keeps submitted audio for 30 days.',
    values: ['disabled', 'enabled'],
  },
];

const VALUE_LABELS: Record<string, string> = {
  coerce: 'Coerce to a supported value',
  reject: 'Reject the request',
  drop: 'Ignore the unsupported option',
  emulate: 'Emulate support',
  drop_echo: 'Remove upstream, echo in response',
  drop_tool: 'Remove the hosted tool',
  disabled: 'Disabled',
  enabled: 'Enabled',
};

function valueLabel(value: string): string {
  return VALUE_LABELS[value] ?? value;
}

function copyOverride(
  value: Principal['openai_oauth_compat'],
): OpenaiOauthCompatOverride {
  return value ? { ...value } : {};
}

function overridesEqual(
  left: OpenaiOauthCompatOverride,
  right: OpenaiOauthCompatOverride,
): boolean {
  return FIELDS.every((field) => left[field.key] === right[field.key]);
}

export function OpenaiOauthCompatCard({ principal }: { principal: Principal }) {
  const updateSettings = useUpdatePrincipalOpenaiOauthCompat();
  const principalWritePending = usePrincipalWritePending(principal.id) > 0;
  const configQuery = useConfigEditor();
  const [transcriptionConfirmOpen, setTranscriptionConfirmOpen] =
    useState(false);
  const [draft, setDraft] = useState<OpenaiOauthCompatOverride>(() =>
    copyOverride(principal.openai_oauth_compat),
  );
  const [resetOpen, setResetOpen] = useState(false);

  useEffect(() => {
    setDraft(copyOverride(principal.openai_oauth_compat));
  }, [principal.openai_oauth_compat]);

  const serverPolicy = useMemo(() => {
    const configured = configQuery.data?.effective_config?.openai_oauth_compat;
    if (!configured || typeof configured !== 'object') return SERVER_DEFAULTS;
    return {
      ...SERVER_DEFAULTS,
      ...(configured as Partial<OpenaiOauthCompatPolicy>),
    };
  }, [configQuery.data?.effective_config]);

  const changed = !overridesEqual(
    draft,
    copyOverride(principal.openai_oauth_compat),
  );
  const controlsDisabled =
    updateSettings.isPending || principalWritePending || configQuery.isLoading;

  const handleSave = () => {
    if (!changed || controlsDisabled) return;
    updateSettings.mutate(
      {
        id: principal.id,
        expected_revision: principal.revision,
        openai_oauth_compat: draft,
      },
      {
        onSuccess: () => toast.success('OpenAI compatibility settings saved'),
        onError: (error) => {
          if (
            error instanceof ApiError &&
            (error.status === 409 || error.status === 412)
          ) {
            toast.error(
              'Principal was modified by another user. Please refresh and try again.',
            );
          } else {
            toast.error(
              error instanceof Error
                ? error.message
                : 'Failed to update compatibility settings',
            );
          }
        },
      },
    );
  };

  const handleReset = () => {
    if (controlsDisabled) return;
    updateSettings.mutate(
      {
        id: principal.id,
        expected_revision: principal.revision,
        openai_oauth_compat: null,
      },
      {
        onSuccess: () => {
          setResetOpen(false);
          setDraft({});
          toast.success('OpenAI compatibility settings reset');
        },
        onError: (error) => {
          if (
            error instanceof ApiError &&
            (error.status === 409 || error.status === 412)
          ) {
            toast.error(
              'Principal was modified by another user. Please refresh and try again.',
            );
          } else {
            toast.error(
              error instanceof Error
                ? error.message
                : 'Failed to reset compatibility settings',
            );
          }
        },
      },
    );
  };
  const applyDraftValue = (field: CompatField, value: string) => {
    setDraft((current) => {
      const next = { ...current };
      if (value === '') delete next[field.key];
      else next[field.key] = value as never;
      return next;
    });
  };

  return (
    <>
      <DetailSection
        span="full"
        data-testid="openai-oauth-compat-card"
        title="Public OpenAI API compatibility"
        description="Choose how public OpenAI API options behave on ChatGPT subscription upstreams."
        action={
          <div className="flex flex-wrap items-center gap-2">
            <Button
              variant="secondary"
              size="sm"
              disabled={controlsDisabled}
              onClick={() => setResetOpen(true)}
            >
              Reset all to server defaults
            </Button>
            <Button
              variant="primary"
              size="sm"
              disabled={controlsDisabled || !changed}
              onClick={handleSave}
            >
              Save changes
            </Button>
          </div>
        }
      >
        <div className="divide-y divide-row border-y border-row">
          {FIELDS.map((field) => {
            const serverValue = serverPolicy[field.key] as string;
            const overrideValue = draft[field.key] as string | undefined;
            const effectiveValue = overrideValue ?? serverValue;
            const options: readonly SelectOption[] = field.values.map(
              (value) => ({
                value,
                label: valueLabel(value),
              }),
            );
            return (
              <div
                key={field.key}
                data-testid={`openai-oauth-compat-row-${field.key}`}
                className="grid gap-3 py-3 md:grid-cols-[minmax(0,1fr)_minmax(14rem,22rem)] md:items-center"
              >
                <div className="min-w-0">
                  <div className="text-label text-text">{field.label}</div>
                  <p className="mt-0.5 text-caption text-text-muted">
                    {field.description}
                  </p>
                  <p className="mt-1 text-caption text-text-faint">
                    Effective:{' '}
                    <span className="text-text-muted">
                      {valueLabel(effectiveValue)}
                    </span>{' '}
                    ·{' '}
                    {overrideValue === undefined
                      ? 'Inherited from server'
                      : 'Overridden for this principal'}
                  </p>
                </div>
                <Select
                  value={overrideValue ?? ''}
                  options={options}
                  allLabel={`Inherit (server: ${valueLabel(serverValue)})`}
                  aria-label={field.label}
                  disabled={controlsDisabled}
                  data-testid={`openai-oauth-compat-select-${field.key}`}
                  onChange={(value) => {
                    if (
                      field.key === 'transcription' &&
                      value === 'enabled' &&
                      effectiveValue !== 'enabled'
                    ) {
                      setTranscriptionConfirmOpen(true);
                      return;
                    }
                    applyDraftValue(field, value);
                  }}
                />
              </div>
            );
          })}
        </div>
      </DetailSection>
      <ConfirmDialog
        open={resetOpen}
        onOpenChange={setResetOpen}
        title="Reset OpenAI compatibility settings?"
        description="This removes every per-principal override and makes all rows inherit the current server policy."
        confirmLabel="Reset all"
        destructive
        pending={updateSettings.isPending}
        closeOnConfirm={false}
        onConfirm={handleReset}
      />
      <ConfirmDialog
        open={transcriptionConfirmOpen}
        onOpenChange={setTranscriptionConfirmOpen}
        title="Enable audio transcription?"
        description="The ChatGPT subscription upstream keeps submitted audio for 30 days. Enable transcription only if that retention is acceptable."
        confirmLabel="Enable transcription"
        onConfirm={() => {
          setDraft((current) => ({ ...current, transcription: 'enabled' }));
          setTranscriptionConfirmOpen(false);
        }}
      />
    </>
  );
}
