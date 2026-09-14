import { Field as BaseField } from '@base-ui/react/field';
import { Form as BaseForm } from '@base-ui/react/form';
import { Input as BaseInput } from '@base-ui/react/input';
import { Lock } from 'lucide-react';
import {
  type ReactNode,
  useCallback,
  useEffect,
  useRef,
  useState,
} from 'react';
import {
  ApiError,
  AUTH_REQUIRED_EVENT,
  type AuthRequiredEventDetail,
  type AuthSession,
  getAuthSession,
  getUnauthorizedAuthMode,
} from '../lib/api';
import {
  AUTH_TOKEN_KEY,
  clearAdminToken,
  getAdminToken,
  setAdminToken,
} from '../lib/auth';
import { AuthSessionProvider } from '../lib/authSession';
import { Button, Card, cx, INPUT_CLASS, Spinner } from './ui/primitives';

type SessionState =
  | { status: 'loading' }
  | { status: 'authenticated'; session: AuthSession }
  | { status: 'required' }
  | { status: 'external_required' }
  | { status: 'error' };

export function AuthRequiredGate({ children }: { children: ReactNode }) {
  const [sessionState, setSessionState] = useState<SessionState>({
    status: 'loading',
  });
  const [value, setValue] = useState('');
  const [submitting, setSubmitting] = useState(false);
  const [errors, setErrors] = useState<Record<string, string>>({});
  const requestSequence = useRef(0);

  const loadSession = useCallback(
    async (showLoading: boolean): Promise<SessionState['status']> => {
      const requestId = ++requestSequence.current;
      let sentStoredToken = Boolean(getAdminToken());
      let retriedWithoutToken = false;
      if (showLoading) {
        setSessionState({ status: 'loading' });
      }

      for (;;) {
        try {
          const session = await getAuthSession();
          if (requestId !== requestSequence.current) return 'loading';
          setSessionState({ status: 'authenticated', session });
          return 'authenticated';
        } catch (error) {
          if (requestId !== requestSequence.current) return 'loading';
          if (error instanceof ApiError && error.status === 401) {
            const authMode = getUnauthorizedAuthMode(error);
            if (
              authMode === 'external' &&
              sentStoredToken &&
              !retriedWithoutToken
            ) {
              clearAdminToken();
              sentStoredToken = false;
              retriedWithoutToken = true;
              continue;
            }

            clearAdminToken();
            if (authMode === 'static_token') {
              setSessionState({ status: 'required' });
              return 'required';
            }
            if (authMode === 'external') {
              setSessionState({ status: 'external_required' });
              return 'external_required';
            }
          }
          setSessionState({ status: 'error' });
          return 'error';
        }
      }
    },
    [],
  );

  useEffect(() => {
    const onStorage = (event: StorageEvent) => {
      if (event.key === AUTH_TOKEN_KEY || event.key === null) {
        void loadSession(true);
      }
    };
    const onAuthRequired = (event: Event) => {
      const detail = (event as CustomEvent<AuthRequiredEventDetail>).detail;
      requestSequence.current += 1;
      clearAdminToken();
      if (detail?.authMode === 'external') {
        if (detail.hadToken) {
          void loadSession(true);
        } else {
          setSessionState({ status: 'external_required' });
        }
        return;
      }
      if (detail?.authMode === 'static_token') {
        setSessionState({ status: 'required' });
        return;
      }
      setSessionState({ status: 'error' });
    };

    window.addEventListener('storage', onStorage);
    window.addEventListener(AUTH_REQUIRED_EVENT, onAuthRequired);
    void loadSession(true);

    return () => {
      requestSequence.current += 1;
      window.removeEventListener('storage', onStorage);
      window.removeEventListener(AUTH_REQUIRED_EVENT, onAuthRequired);
    };
  }, [loadSession]);

  if (sessionState.status === 'authenticated') {
    return (
      <AuthSessionProvider session={sessionState.session}>
        {children}
      </AuthSessionProvider>
    );
  }

  if (sessionState.status === 'loading') {
    return (
      <AuthGateFrame>
        <Card
          aria-label="Checking admin session"
          className="p-6 flex items-center gap-3 text-sm text-text-muted"
          role="status"
        >
          <Spinner />
          Checking admin session…
        </Card>
      </AuthGateFrame>
    );
  }

  if (sessionState.status === 'error') {
    return (
      <AuthGateFrame>
        <Card className="p-6">
          <div className="flex flex-col gap-2 mb-5">
            <h1 className="text-base font-medium text-text leading-tight">
              Unable to verify admin session
            </h1>
            <p className="text-xs text-text-faint">
              Check the admin service connection, then try again.
            </p>
          </div>
          <Button fullWidth onClick={() => void loadSession(true)}>
            Retry
          </Button>
        </Card>
      </AuthGateFrame>
    );
  }

  if (sessionState.status === 'external_required') {
    return (
      <AuthGateFrame>
        <Card className="p-6">
          <div className="flex flex-col gap-2 mb-5">
            <h1 className="text-base font-medium text-text leading-tight">
              External authentication required
            </h1>
            <p className="text-xs text-text-faint">
              Sign in with the configured identity provider, then try again.
            </p>
          </div>
          <Button fullWidth onClick={() => void loadSession(true)}>
            Retry
          </Button>
        </Card>
      </AuthGateFrame>
    );
  }

  async function handleSubmit(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const trimmed = value.trim();
    if (!trimmed) {
      setErrors({ token: 'Token cannot be empty' });
      return;
    }

    setSubmitting(true);
    try {
      setErrors({});
      setAdminToken(trimmed);
      const result = await loadSession(false);
      if (result === 'authenticated') {
        setValue('');
      } else if (result === 'required' || result === 'loading') {
        setErrors({ token: 'Admin token was rejected' });
      }
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <AuthGateFrame>
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
            loading={submitting}
            type="submit"
            variant="primary"
          >
            Sign in
          </Button>
        </BaseForm>
      </Card>
    </AuthGateFrame>
  );
}

function AuthGateFrame({ children }: { children: ReactNode }) {
  return (
    <div className="min-h-screen w-full flex items-center justify-center bg-bg text-text px-4 py-8">
      <div className="w-full max-w-sm">{children}</div>
    </div>
  );
}
