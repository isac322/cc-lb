import { Field as BaseField } from '@base-ui/react/field';
import { Form as BaseForm } from '@base-ui/react/form';
import { Input as BaseInput } from '@base-ui/react/input';
import { Lock } from 'lucide-react';
import { type ReactNode, useEffect, useState } from 'react';
import { AUTH_TOKEN_KEY, getAdminToken, setAdminToken } from '../lib/auth';
import { Button, Card, cx, INPUT_CLASS } from './ui/primitives';

const AUTH_REQUIRED_EVENT = 'cclb:auth-required';

export function AuthRequiredGate({ children }: { children: ReactNode }) {
  const [hasToken, setHasToken] = useState<boolean>(() => !!getAdminToken());
  const [value, setValue] = useState('');
  const [submitting, setSubmitting] = useState(false);
  const [errors, setErrors] = useState<Record<string, string>>({});

  useEffect(() => {
    const onStorage = (e: StorageEvent) => {
      if (e.key === AUTH_TOKEN_KEY || e.key === null) {
        setHasToken(!!getAdminToken());
      }
    };
    const onAuthRequired = () => {
      setHasToken(false);
    };
    window.addEventListener('storage', onStorage);
    window.addEventListener(AUTH_REQUIRED_EVENT, onAuthRequired);
    return () => {
      window.removeEventListener('storage', onStorage);
      window.removeEventListener(AUTH_REQUIRED_EVENT, onAuthRequired);
    };
  }, []);

  if (hasToken) {
    return <>{children}</>;
  }

  function handleSubmit(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const trimmed = value.trim();
    if (!trimmed) {
      setErrors({ token: 'Token cannot be empty' });
      return;
    }
    setSubmitting(true);
    try {
      setAdminToken(trimmed);
      setValue('');
      setErrors({});
      setHasToken(true);
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div className="min-h-screen w-full flex items-center justify-center bg-bg text-text px-4 py-8">
      <div className="w-full max-w-sm">
        <Card className="p-6">
          <div className="flex flex-col gap-2 mb-5">
            <div className="flex items-center gap-2 text-text-faint">
              <Lock className="w-3.5 h-3.5" />
              <span className="text-[11px] uppercase tracking-wider font-mono">
                cc-lb admin
              </span>
            </div>
            <h1 className="text-base font-medium text-text leading-tight">
              Admin token required
            </h1>
            <p className="text-xs text-text-faint">
              Paste the admin Bearer token to continue.
            </p>
          </div>
          <BaseForm
            className="flex flex-col gap-4"
            errors={errors}
            onSubmit={handleSubmit}
          >
            <BaseField.Root className="flex flex-col gap-1.5" name="token">
              <BaseField.Label className="text-[11px] uppercase tracking-wider text-text-faint">
                Bearer token
              </BaseField.Label>
              <BaseField.Control
                autoComplete="current-password"
                autoFocus
                className={cx(INPUT_CLASS, 'font-mono')}
                onChange={(event) => setValue(event.target.value)}
                placeholder="paste token"
                render={<BaseInput />}
                spellCheck={false}
                type="password"
                value={value}
              />
              <BaseField.Error className="text-[11px] text-red-400" />
            </BaseField.Root>
            <Button
              disabled={submitting || !value.trim()}
              fullWidth
              type="submit"
              variant="primary"
            >
              Sign in
            </Button>
          </BaseForm>
        </Card>
      </div>
    </div>
  );
}
