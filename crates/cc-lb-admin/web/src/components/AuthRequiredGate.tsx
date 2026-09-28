import { Field as BaseField } from '@base-ui/react/field';
import { Form as BaseForm } from '@base-ui/react/form';
import { Input as BaseInput } from '@base-ui/react/input';
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
import { BrandMark } from './layout/Sidebar';
import { Button, cx, INPUT_CLASS, Spinner } from './ui/primitives';

/**
 * The signed-out panel: a flat card from `sm`; phones drop the frame and its
 * padding so the copy and the token field use the full width.
 */
const GATE_PANEL_CLASS = 'rounded-md sm:glass sm:p-6';

/** Phones: the one action spans the panel width. */
const GATE_ACTION_CLASS = 'max-sm:w-full';

type SessionState =
  | { status: 'loading' }
  | { status: 'authenticated'; session: AuthSession }
  /** `storedTokenRejected`: a saved token existed and the server refused it. */
  | { status: 'required'; storedTokenRejected: boolean }
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

  const showRequired = useCallback((storedTokenRejected: boolean) => {
    setSessionState({ status: 'required', storedTokenRejected });
    if (storedTokenRejected) {
      setValue('');
      setErrors({ token: 'Saved token no longer works' });
    }
  }, []);

  const loadSession = useCallback(
    async (
      showLoading: boolean,
      tokenSource: 'stored' | 'submitted' = 'stored',
    ): Promise<SessionState['status']> => {
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
              showRequired(sentStoredToken && tokenSource === 'stored');
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
    [showRequired],
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
        showRequired(detail.hadToken);
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
  }, [loadSession, showRequired]);

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
        <div
          aria-label="Checking admin session"
          className={cx(
            GATE_PANEL_CLASS,
            'flex items-center gap-3 text-body text-text-muted',
          )}
          role="status"
        >
          <Spinner />
          Checking admin session…
        </div>
      </AuthGateFrame>
    );
  }

  if (sessionState.status === 'error') {
    return (
      <AuthGateFrame>
        <div className={GATE_PANEL_CLASS}>
          <h1 className="text-title-section text-text">
            Unable to verify admin session
          </h1>
          <p className="mt-1.5 text-body text-text-muted">
            Check the admin service connection, then try again.
          </p>
          <div className="mt-5 flex justify-end">
            <Button
              variant="primary"
              className={GATE_ACTION_CLASS}
              onClick={() => void loadSession(true)}
            >
              Retry
            </Button>
          </div>
        </div>
      </AuthGateFrame>
    );
  }

  if (sessionState.status === 'external_required') {
    return (
      <AuthGateFrame>
        <div className={GATE_PANEL_CLASS}>
          <h1 className="text-title-section text-text">
            External authentication required
          </h1>
          <p className="mt-1.5 text-body text-text-muted">
            Sign in with the configured identity provider, then try again.
          </p>
          <div className="mt-5 flex justify-end">
            <Button
              variant="primary"
              className={GATE_ACTION_CLASS}
              onClick={() => void loadSession(true)}
            >
              Retry
            </Button>
          </div>
        </div>
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
      const result = await loadSession(false, 'submitted');
      if (result === 'authenticated') {
        setValue('');
      } else if (result === 'required' || result === 'loading') {
        setErrors({ token: 'Admin token was rejected' });
      }
    } finally {
      setSubmitting(false);
    }
  }

  const rejected = sessionState.storedTokenRejected;
  return (
    <AuthGateFrame>
      <div className={GATE_PANEL_CLASS}>
        <h1 className="text-title-section text-text">
          {rejected ? 'Saved admin token was rejected' : 'Admin token required'}
        </h1>
        <p className="mt-1.5 text-body text-text-muted">
          {rejected
            ? 'The admin token saved in this browser no longer works. It may have been rotated. Paste the current admin Bearer token to continue.'
            : 'Paste the admin Bearer token to continue.'}
        </p>
        <BaseForm
          className="mt-5 flex flex-col gap-5"
          errors={errors}
          onSubmit={handleSubmit}
        >
          <BaseField.Root className="flex flex-col gap-1.5" name="token">
            <BaseField.Label className="text-label text-text-muted">
              Bearer token
            </BaseField.Label>
            <BaseField.Control
              autoComplete="current-password"
              autoFocus
              className={cx(INPUT_CLASS, 'font-mono max-sm:h-11')}
              onChange={(event) => setValue(event.target.value)}
              placeholder="paste token"
              render={<BaseInput />}
              spellCheck={false}
              type="password"
              value={value}
            />
            <BaseField.Error className="text-caption text-danger-text" />
          </BaseField.Root>
          <div className="flex justify-end">
            <Button
              disabled={submitting || !value.trim()}
              loading={submitting}
              className={GATE_ACTION_CLASS}
              type="submit"
              variant="primary"
            >
              Sign in
            </Button>
          </div>
        </BaseForm>
      </div>
    </AuthGateFrame>
  );
}

/**
 * Signed-out frame: the brand mark and name on the ground, then one flat
 * panel. The page's single primary action lives inside the panel.
 */
function AuthGateFrame({ children }: { children: ReactNode }) {
  return (
    <div className="min-h-dvh w-full flex items-center justify-center bg-bg text-text px-4 py-8">
      <div className="w-full max-w-sm">
        <div className="mb-6 flex items-center justify-center gap-2.5">
          <BrandMark size={28} />
          <span className="text-title-section text-text">cc-lb admin</span>
        </div>
        {children}
      </div>
    </div>
  );
}
