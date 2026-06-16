import { useState } from 'react';
import { toast } from 'sonner';
import { DEFAULT_ANTHROPIC_BASE_URL } from '../../lib/constants';
import {
  type UpdateUpstreamRequest,
  type Upstream,
  useUpdateUpstream,
} from '../../lib/queries';
import {
  Button,
  Card,
  CardBody,
  CardHeader,
  Field,
  INPUT_CLASS,
} from '../ui/primitives';

type Props = {
  upstream: Upstream;
};

export function SettingsCard({ upstream }: Props) {
  const [editing, setEditing] = useState(false);
  const [baseUrl, setBaseUrl] = useState(upstream.base_url || '');
  const [useLiteral, setUseLiteral] = useState(!upstream.api_key_env);
  const [envVar, setEnvVar] = useState(upstream.api_key_env || '');
  const [literalKey, setLiteralKey] = useState('');

  const update = useUpdateUpstream();

  if (upstream.kind === 'anthropic_oauth') {
    return null;
  }

  const handleSave = () => {
    const trimmedBase = baseUrl.trim();
    const trimmedEnv = envVar.trim();
    const trimmedLiteral = literalKey.trim();

    const body: UpdateUpstreamRequest = {
      base_url: trimmedBase === '' ? null : trimmedBase,
      ...(upstream.kind === 'anthropic_api_key' && {
        api_key_value: useLiteral ? trimmedLiteral || null : null,
        api_key_env: !useLiteral ? trimmedEnv || null : null,
      }),
    };

    update.mutate(
      {
        id: upstream.id,
        body,
        spec_revision: upstream.spec_revision,
      },
      {
        onSuccess: () => {
          toast.success('Settings updated');
          setEditing(false);
        },
      },
    );
  };

  const handleCancel = () => {
    setBaseUrl(upstream.base_url || '');
    setUseLiteral(!upstream.api_key_env);
    setEnvVar(upstream.api_key_env || '');
    setLiteralKey('');
    setEditing(false);
  };

  return (
    <Card>
      <CardHeader
        title="Settings"
        action={
          !editing && (
            <Button size="sm" onClick={() => setEditing(true)}>
              Edit
            </Button>
          )
        }
      />
      <CardBody>
        {editing ? (
          <div className="flex flex-col gap-4">
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
              <Field label="API Key">
                <div className="flex flex-col gap-2">
                  <div className="flex items-center gap-4">
                    <label className="flex items-center gap-1.5 text-sm text-text">
                      <input
                        type="radio"
                        checked={!useLiteral}
                        onChange={() => setUseLiteral(false)}
                      />
                      Environment Variable
                    </label>
                    <label className="flex items-center gap-1.5 text-sm text-text">
                      <input
                        type="radio"
                        checked={useLiteral}
                        onChange={() => setUseLiteral(true)}
                      />
                      Literal Value
                    </label>
                  </div>
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
              <Button
                variant="ghost"
                onClick={handleCancel}
                disabled={update.isPending}
              >
                Cancel
              </Button>
              <Button
                variant="primary"
                onClick={handleSave}
                disabled={update.isPending}
              >
                {update.isPending ? 'Saving...' : 'Save'}
              </Button>
            </div>
          </div>
        ) : (
          <div className="grid grid-cols-[120px_1fr] gap-y-3 gap-x-4 text-sm">
            <div className="text-text-faint">Base URL</div>
            <div className="text-text">{upstream.base_url || '—'}</div>

            {upstream.kind === 'anthropic_api_key' && (
              <>
                <div className="text-text-faint">API Key</div>
                <div className="text-text">
                  {upstream.api_key_env ? (
                    <span className="font-mono text-xs">
                      env:{upstream.api_key_env}
                    </span>
                  ) : (
                    <span className="text-text-muted italic">
                      literal value (stored)
                    </span>
                  )}
                </div>
              </>
            )}
          </div>
        )}
      </CardBody>
    </Card>
  );
}
