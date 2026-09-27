import { Radio as BaseRadio } from '@base-ui/react/radio';
import { RadioGroup as BaseRadioGroup } from '@base-ui/react/radio-group';
import { useRef, useState } from 'react';
import { toast } from 'sonner';
import { DEFAULT_ANTHROPIC_BASE_URL } from '../../lib/constants';
import {
  type UpdateUpstreamRequest,
  type Upstream,
  useUpdateUpstream,
} from '../../lib/queries';
import { DetailRow, DetailRows, DetailSection } from '../ui/DetailPane';
import { Button, Field, INPUT_CLASS } from '../ui/primitives';

type Props = {
  upstream: Upstream;
};

export function SettingsCard({ upstream }: Props) {
  const [editing, setEditing] = useState(false);
  const [baseUrl, setBaseUrl] = useState(upstream.base_url || '');
  const [useLiteral, setUseLiteral] = useState(!upstream.api_key_env);
  const [envVar, setEnvVar] = useState(upstream.api_key_env || '');
  const [literalKey, setLiteralKey] = useState('');
  // Base URL value captured when editing began. Save intent is judged
  // against this baseline, not live props, so a background refetch can
  // never turn an untouched field into a clear.
  const baseUrlBaseline = useRef(upstream.base_url || '');

  const update = useUpdateUpstream();

  if (upstream.kind === 'anthropic_oauth') {
    return null;
  }

  const handleSave = () => {
    const trimmedBase = baseUrl.trim();
    const trimmedEnv = envVar.trim();
    const trimmedLiteral = literalKey.trim();

    const body: UpdateUpstreamRequest = {};
    if (trimmedBase !== baseUrlBaseline.current) {
      if (trimmedBase === '') {
        body.clear_base_url = true;
      } else {
        body.base_url = trimmedBase;
      }
    }
    if (upstream.kind === 'anthropic_api_key') {
      body.api_key_value = useLiteral ? trimmedLiteral || null : null;
      body.api_key_env = !useLiteral ? trimmedEnv || null : null;
    }

    update.mutate(
      {
        id: upstream.id,
        body,
        spec_revision: upstream.spec_revision,
      },
      {
        onSuccess: (updated) => {
          // An explicit clear is only done when the server confirms
          // base_url is null. Older backends ignore clear_base_url and
          // still return the stored URL (or omit the field); treat that
          // as unconfirmed and keep the editor open.
          if (body.clear_base_url === true && updated.base_url !== null) {
            toast.error(
              'Server did not confirm the Base URL clear; the stored URL may still be set.',
            );
            return;
          }
          toast.success('Settings updated');
          setEditing(false);
        },
      },
    );
  };

  const handleEdit = () => {
    setBaseUrl(upstream.base_url || '');
    baseUrlBaseline.current = upstream.base_url || '';
    setUseLiteral(!upstream.api_key_env);
    setEnvVar(upstream.api_key_env || '');
    setLiteralKey('');
    setEditing(true);
  };

  const handleCancel = () => {
    setBaseUrl(upstream.base_url || '');
    setUseLiteral(!upstream.api_key_env);
    setEnvVar(upstream.api_key_env || '');
    setLiteralKey('');
    setEditing(false);
  };

  return (
    <DetailSection
      title="Settings"
      description="Where requests go and which key they carry"
      action={
        !editing && (
          <Button size="sm" onClick={handleEdit}>
            Edit
          </Button>
        )
      }
    >
      <div>
        {editing ? (
          <div
            aria-busy={update.isPending}
            className="flex max-w-2xl flex-col gap-4"
            data-testid="upstream-settings-edit-form"
          >
            <Field label="Base URL">
              <input
                type="text"
                className={INPUT_CLASS}
                value={baseUrl}
                onChange={(e) => setBaseUrl(e.target.value)}
                placeholder={`e.g. ${DEFAULT_ANTHROPIC_BASE_URL}`}
              />
            </Field>

            {upstream.kind === 'anthropic_api_key' && (
              <Field label="API key">
                <div className="flex flex-col gap-2">
                  <BaseRadioGroup
                    className="flex items-center gap-4"
                    onValueChange={(value) =>
                      setUseLiteral(value === 'literal')
                    }
                    value={useLiteral ? 'literal' : 'env'}
                  >
                    <label className="flex items-center gap-1.5 text-body-sm text-text">
                      <BaseRadio.Root
                        className="flex h-4 w-4 items-center justify-center rounded-full border border-subtle-strong bg-input-bg data-[checked]:border-accent"
                        value="env"
                      >
                        <BaseRadio.Indicator className="h-2 w-2 rounded-full bg-accent" />
                      </BaseRadio.Root>
                      Environment variable
                    </label>
                    <label className="flex items-center gap-1.5 text-body-sm text-text">
                      <BaseRadio.Root
                        className="flex h-4 w-4 items-center justify-center rounded-full border border-subtle-strong bg-input-bg data-[checked]:border-accent"
                        value="literal"
                      >
                        <BaseRadio.Indicator className="h-2 w-2 rounded-full bg-accent" />
                      </BaseRadio.Root>
                      Literal value
                    </label>
                  </BaseRadioGroup>
                  {!useLiteral ? (
                    <input
                      type="text"
                      className={INPUT_CLASS}
                      value={envVar}
                      onChange={(e) => setEnvVar(e.target.value)}
                      placeholder="e.g. ANTHROPIC_API_KEY"
                    />
                  ) : (
                    <input
                      type="password"
                      className={INPUT_CLASS}
                      value={literalKey}
                      onChange={(e) => setLiteralKey(e.target.value)}
                      placeholder="Enter new API key to replace stored value"
                    />
                  )}
                </div>
              </Field>
            )}

            <div className="flex justify-end gap-2 mt-2">
              <Button onClick={handleCancel} disabled={update.isPending}>
                Cancel
              </Button>
              <Button
                variant="primary"
                loading={update.isPending}
                disabled={update.isPending}
                onClick={handleSave}
              >
                {update.isPending ? 'Saving...' : 'Save'}
              </Button>
            </div>
          </div>
        ) : (
          <DetailRows>
            <DetailRow label="Base URL">
              {upstream.base_url ? (
                <span className="break-all font-mono text-data">
                  {upstream.base_url}
                </span>
              ) : (
                <span className="text-text-muted">
                  Default{' '}
                  <span className="break-all font-mono text-data">
                    {DEFAULT_ANTHROPIC_BASE_URL}
                  </span>
                </span>
              )}
            </DetailRow>
            {upstream.kind === 'anthropic_api_key' && (
              <DetailRow label="API key">
                {upstream.api_key_env ? (
                  <span className="break-all font-mono text-data">
                    env:{upstream.api_key_env}
                  </span>
                ) : (
                  <span className="text-text-muted">
                    Literal value (stored)
                  </span>
                )}
              </DetailRow>
            )}
          </DetailRows>
        )}
      </div>
    </DetailSection>
  );
}
