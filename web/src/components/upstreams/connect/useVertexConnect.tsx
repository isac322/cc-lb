import { RadioGroup } from '@base-ui/react/radio-group';
import { KeyRound, UserRound } from 'lucide-react';
import { type ReactNode, useMemo, useState } from 'react';
import { toast } from 'sonner';
import {
  ApiError,
  type PreflightResponse,
  type Upstream,
} from '../../../lib/api';
import {
  useCreateUpstream,
  usePreflightApiKey,
  useUpdateUpstream,
} from '../../../lib/queries';
import { Button, cx, Field, INPUT_CLASS, Notice } from '../../ui/primitives';
import { KindOption, StepHeading } from './parts';

export type VertexConnectMode = 'express' | 'cloud_api_key' | 'credential';

type VertexStep = 'mode' | 'input' | 'finish';

export interface VertexConnect {
  rail: ReadonlyArray<{ title: string; hint: string }>;
  activeIndex: number;
  body: ReactNode;
  footer: ReactNode;
  busy: boolean;
}

function errorReason(error: unknown, field: string): string | null {
  if (
    !(error instanceof ApiError) ||
    !error.body ||
    typeof error.body !== 'object'
  ) {
    return null;
  }
  const body = error.body as Record<string, unknown>;
  const nested =
    body.error && typeof body.error === 'object'
      ? (body.error as Record<string, unknown>)
      : body;
  return nested.field === field && typeof nested.reason === 'string'
    ? nested.reason
    : null;
}

function credentialHint(value: Record<string, unknown>): string | null {
  const email = value.client_email;
  if (typeof email === 'string' && email.trim()) return email;
  const quota = value.quota_project_id;
  return typeof quota === 'string' && quota.trim() ? quota : null;
}

export function useVertexConnect({
  replace,
  onCancel,
  onDone,
}: {
  replace: Upstream | null;
  onCancel: () => void;
  onDone: (upstream: Upstream) => void;
}): VertexConnect {
  const preflight = usePreflightApiKey();
  const create = useCreateUpstream();
  const update = useUpdateUpstream({ inlineError: true });
  const [mode, setMode] = useState<VertexConnectMode>(
    replace?.kind === 'vertex_credential'
      ? 'credential'
      : replace?.vertex?.express
        ? 'express'
        : 'cloud_api_key',
  );
  const [step, setStep] = useState<VertexStep>('mode');
  const [key, setKey] = useState('');
  const [project, setProject] = useState(replace?.vertex?.project_id ?? '');
  const [location, setLocation] = useState(
    replace?.vertex?.location ?? 'global',
  );
  const [credential, setCredential] = useState('');
  const [credentialMeta, setCredentialMeta] = useState<{
    type: string;
    hint: string | null;
    name: string | null;
  } | null>(null);
  const [name, setName] = useState(replace?.name ?? '');
  const [checked, setChecked] = useState<PreflightResponse | null>(null);
  const [fieldError, setFieldError] = useState<string | null>(null);
  const [requestError, setRequestError] = useState<string | null>(null);

  const busy = preflight.isPending || create.isPending || update.isPending;
  const isCredential = mode === 'credential';
  const isExpress = mode === 'express';
  const rail = [
    { title: 'Google Cloud', hint: 'Choose a connection' },
    { title: isCredential ? 'Credential' : 'API key', hint: 'Add access' },
    { title: 'Name & save', hint: 'Confirm the upstream' },
  ];

  const parseCredential = (
    raw: string,
    source: 'file' | 'paste',
    name: string | null = null,
  ) => {
    const trimmed = raw.trim();
    if (!trimmed.startsWith('{')) {
      setFieldError(
        source === 'file'
          ? 'Choose a JSON credential file.'
          : 'Paste a JSON object.',
      );
      setCredentialMeta(null);
      return;
    }
    try {
      const parsed: unknown = JSON.parse(trimmed);
      if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed))
        throw new Error();
      const object = parsed as Record<string, unknown>;
      if (typeof object.type !== 'string' || !object.type.trim()) {
        setFieldError('Credential JSON must include a type.');
        setCredentialMeta(null);
        return;
      }
      setCredential(raw);
      setCredentialMeta({
        type: object.type,
        hint: credentialHint(object),
        name,
      });
      setFieldError(null);
      setRequestError(null);
    } catch {
      setFieldError('Choose a valid JSON credential file.');
      setCredentialMeta(null);
    }
  };

  const check = async () => {
    setRequestError(null);
    setFieldError(null);
    if (isCredential) {
      if (!credentialMeta || credential.trim() === '') {
        setFieldError(
          'Add a Google credential JSON file or paste its contents.',
        );
        return;
      }
      setStep('finish');
      return;
    }
    const parsedKey = key.trim();
    if (parsedKey.startsWith('{') || parsedKey.startsWith('[')) {
      setFieldError('Use a Vertex API key, not credential JSON.');
      return;
    }
    if (!parsedKey) {
      setFieldError('Enter an API key.');
      return;
    }
    if (!isExpress && !project.trim()) {
      setFieldError('Enter a project ID.');
      return;
    }
    try {
      const result = await preflight.mutateAsync({
        kind: 'vertex_api_key',
        api_key_value: parsedKey,
        ...(isExpress
          ? { express: true }
          : {
              project_id: project.trim(),
              location: location.trim() || 'global',
            }),
      });
      setChecked(result);
      if (result.verdict === 'ok') setStep('finish');
      else {
        const code = result.remediation[0]?.code;
        setRequestError(
          code === 'key_forbidden'
            ? 'Vertex denied this key. Check roles/aiplatform.user and quota-project permissions, then try again.'
            : code === 'probe_unreachable'
              ? 'Could not reach Vertex AI. Try again to check the key.'
              : 'Vertex rejected this key. Check the key, project and location.',
        );
      }
    } catch (error) {
      setRequestError(
        error instanceof Error
          ? error.message
          : 'The key could not be checked.',
      );
    }
  };

  const save = async () => {
    setRequestError(null);
    try {
      const body = isCredential
        ? {
            google_credential: credential,
            project_id: project.trim() || undefined,
            location: location.trim() || 'global',
          }
        : {
            api_key_value: key.trim(),
            ...(isExpress
              ? { express: true }
              : {
                  project_id: project.trim(),
                  location: location.trim() || 'global',
                }),
          };
      const result = replace
        ? await update.mutateAsync({
            id: replace.id,
            spec_revision: replace.spec_revision,
            body,
          })
        : await create.mutateAsync({
            name:
              name.trim() ||
              (isCredential ? 'Vertex credential' : 'Vertex API key'),
            kind: isCredential ? 'vertex_credential' : 'vertex_api_key',
            ...body,
          });
      toast.success(`${result.name} ${replace ? 'updated' : 'added'}`);
      onDone(result);
    } catch (error) {
      const reason = errorReason(error, 'google_credential');
      if (reason) {
        setFieldError(reason);
        setStep('input');
      } else {
        setRequestError(
          error instanceof Error
            ? error.message
            : 'The upstream could not be saved.',
        );
      }
    }
  };

  // The rendered body intentionally follows the current local draft state;
  // mutation callbacks are recreated with the hook state on each render.
  // biome-ignore lint/correctness/useExhaustiveDependencies: local draft state is listed explicitly below.
  const body = useMemo(() => {
    if (step === 'mode') {
      return (
        <div className="flex flex-col gap-4">
          <StepHeading
            title="Connect Google Cloud"
            subtitle="Use Vertex AI through the Gemini-compatible API."
          />
          <RadioGroup
            name="vertex-mode"
            value={mode}
            onValueChange={(value) => {
              setMode(value as VertexConnectMode);
              setFieldError(null);
            }}
            className="flex flex-col gap-2"
          >
            <KindOption
              value="express"
              selected={mode === 'express'}
              icon={<KeyRound className="w-4 h-4" />}
              title="Express key"
              description="Generate and count content only."
              onActivate={() => setStep('input')}
            />
            <KindOption
              value="cloud_api_key"
              selected={mode === 'cloud_api_key'}
              icon={<KeyRound className="w-4 h-4" />}
              title="Cloud API key"
              description="A project-bound key for generation, streaming, and token counting. Cache, model catalog, and Live use a credential."
              onActivate={() => setStep('input')}
            />
            <KindOption
              value="credential"
              selected={mode === 'credential'}
              icon={<UserRound className="w-4 h-4" />}
              title="Credential file"
              description="A Google service account or authorized-user credential for generation, streaming, token counting, cache, model catalog, and Live."
              onActivate={() => setStep('input')}
            />
          </RadioGroup>
        </div>
      );
    }
    if (step === 'finish') {
      return (
        <div className="flex flex-col gap-4">
          <StepHeading
            title="Name & save"
            subtitle="Nothing is saved until you confirm the upstream."
          />
          <Field
            label="Upstream name"
            hint="Only used inside cc-lb — pick something recognizable."
          >
            <input
              className={INPUT_CLASS}
              value={name}
              autoFocus
              onChange={(event) => setName(event.target.value)}
            />
          </Field>
          {checked ? (
            <p className="text-body-sm text-text-muted">
              Vertex preflight passed.
            </p>
          ) : null}
          {credentialMeta ? (
            <p className="text-body-sm text-text-muted">
              {credentialMeta.type}
              {credentialMeta.hint ? ` · ${credentialMeta.hint}` : ''}
            </p>
          ) : null}
          {requestError ? (
            <p role="alert" className="text-caption text-danger-text">
              {requestError}
            </p>
          ) : null}
        </div>
      );
    }
    return (
      <div className="flex flex-col gap-4">
        <StepHeading
          title={
            isCredential
              ? 'Add credential'
              : isExpress
                ? 'Add Express key'
                : 'Add Cloud API key'
          }
        />
        {isCredential ? (
          <>
            <Field
              label="Credential JSON"
              hint="Stored encrypted. cc-lb never shows it again."
            >
              <input
                type="file"
                accept=".json,application/json"
                aria-label="Credential JSON file"
                onChange={(event) => {
                  const file = event.target.files?.[0];
                  if (!file) return;
                  const reader = new FileReader();
                  reader.addEventListener('load', () =>
                    parseCredential(
                      String(reader.result ?? ''),
                      'file',
                      file.name,
                    ),
                  );
                  reader.readAsText(file);
                }}
              />
              <textarea
                className={cx(INPUT_CLASS, 'mt-2 min-h-28 font-mono text-data')}
                aria-label="Paste credential JSON"
                value=""
                onPaste={(event) => {
                  event.preventDefault();
                  parseCredential(event.clipboardData.getData('text'), 'paste');
                }}
                readOnly
                placeholder="Paste credential JSON; contents stay hidden"
              />
            </Field>
            {credentialMeta ? (
              <Notice>
                {credentialMeta.name ? `${credentialMeta.name} · ` : ''}
                Type: {credentialMeta.type}
                {credentialMeta.hint ? ` · ${credentialMeta.hint}` : ''}
              </Notice>
            ) : null}
          </>
        ) : (
          <>
            <Field label="API key">
              <input
                className={INPUT_CLASS}
                type="password"
                value={key}
                autoFocus
                onChange={(event) => {
                  setKey(event.target.value);
                  setFieldError(null);
                }}
              />
            </Field>
            {!isExpress ? (
              <>
                <Field label="Project ID">
                  <input
                    className={INPUT_CLASS}
                    value={project}
                    onChange={(event) => setProject(event.target.value)}
                  />
                </Field>
                <Field label="Location" hint="Defaults to global.">
                  <input
                    className={INPUT_CLASS}
                    value={location}
                    onChange={(event) => setLocation(event.target.value)}
                  />
                </Field>
              </>
            ) : null}
          </>
        )}
        {fieldError ? (
          <p role="alert" className="text-caption text-danger-text">
            {fieldError}
          </p>
        ) : null}
        {requestError ? (
          <p role="alert" className="text-caption text-danger-text">
            {requestError}
          </p>
        ) : null}
      </div>
    );
  }, [
    checked,
    credential,
    credentialMeta,
    fieldError,
    isCredential,
    isExpress,
    key,
    location,
    mode,
    name,
    project,
    requestError,
    step,
  ]);

  const footer =
    step === 'mode' ? (
      <>
        <Button disabled={busy} onClick={onCancel}>
          Cancel
        </Button>
        <Button
          variant="primary"
          disabled={busy}
          onClick={() => setStep('input')}
        >
          Continue
        </Button>
      </>
    ) : step === 'input' ? (
      <>
        <Button disabled={busy} onClick={() => setStep('mode')}>
          Back
        </Button>
        <Button disabled={busy} onClick={onCancel}>
          Cancel
        </Button>
        <Button variant="primary" loading={busy} onClick={() => void check()}>
          Continue
        </Button>
      </>
    ) : (
      <>
        <Button disabled={busy} onClick={() => setStep('input')}>
          Back
        </Button>
        <Button disabled={busy} onClick={onCancel}>
          Cancel
        </Button>
        <Button variant="primary" loading={busy} onClick={() => void save()}>
          Add upstream
        </Button>
      </>
    );

  return {
    rail,
    activeIndex: step === 'mode' ? 0 : step === 'input' ? 1 : 2,
    body,
    footer,
    busy,
  };
}
