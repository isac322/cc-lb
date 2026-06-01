import { useEffect, useState, type FormEvent, type ReactNode } from 'react';
import { toast } from 'sonner';
import { Lock } from 'lucide-react';
import { Button, Card, Field, INPUT_CLASS, cx } from './ui/primitives';
import { AUTH_TOKEN_KEY, getAdminToken, setAdminToken } from '../lib/auth';

const AUTH_REQUIRED_EVENT = 'cclb:auth-required';

export function AuthRequiredGate({ children }: { children: ReactNode }) {
  const [hasToken, setHasToken] = useState<boolean>(() => !!getAdminToken());
  const [value, setValue] = useState('');
  const [submitting, setSubmitting] = useState(false);

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

  function handleSubmit(e: FormEvent<HTMLFormElement>) {
    e.preventDefault();
    const trimmed = value.trim();
    if (!trimmed) {
      toast.error('Token cannot be empty');
      return;
    }
    setSubmitting(true);
    try {
      setAdminToken(trimmed);
      setValue('');
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
              <span className="text-[11px] uppercase tracking-wider font-mono">cc-lb admin</span>
            </div>
            <h1 className="text-base font-medium text-text leading-tight">Admin token required</h1>
            <p className="text-xs text-text-faint">Paste the admin Bearer token to continue.</p>
          </div>
          <form onSubmit={handleSubmit} className="flex flex-col gap-4">
            <Field label="Bearer token">
              <input
                type="password"
                autoComplete="current-password"
                autoFocus
                spellCheck={false}
                value={value}
                onChange={(e) => setValue(e.target.value)}
                placeholder="paste token"
                className={cx(INPUT_CLASS, 'font-mono')}
              />
            </Field>
            <Button
              type="submit"
              variant="primary"
              fullWidth
              disabled={submitting || !value.trim()}
            >
              Sign in
            </Button>
          </form>
        </Card>
      </div>
    </div>
  );
}
