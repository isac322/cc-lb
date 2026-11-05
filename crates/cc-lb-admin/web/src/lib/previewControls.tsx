import { useNavigate, useRouterState } from '@tanstack/react-router';
import { Eye } from 'lucide-react';
import {
  createContext,
  type ReactNode,
  useContext,
  useEffect,
  useMemo,
  useState,
} from 'react';
import {
  type Installation,
  PROVIDER_FAMILIES,
  PROVIDER_LABELS,
  type ProviderFamily,
  parseProviders,
  providerOf,
  useInstallation,
} from './providers';
import { useUpstreams } from './queries';
import { TIME_PRESETS, type TimePreset } from './timePresets';

export const PREVIEW_ENABLED =
  import.meta.env.DEV || import.meta.env.VITE_CC_LB_PREVIEW === '1';

export type ProviderScope = 'all' | ProviderFamily;

export interface ProviderQueryOptions {
  providers?: readonly ProviderFamily[];
  /** Optional principal filter for dashboard usage ledgers. */
  principalId?: string;
}

export interface PreviewSearch {
  previewProviders?: string;
  provider?: ProviderScope;
  range?: TimePreset;
}

export function validatePreviewSearch(
  search: Record<string, unknown>,
): PreviewSearch {
  const preview =
    typeof search.previewProviders === 'string'
      ? parseProviders(search.previewProviders)
      : undefined;
  const family = (value: unknown) =>
    PROVIDER_FAMILIES.find((id) => id === value);
  const range = TIME_PRESETS.find((preset) => preset === search.range);
  return {
    previewProviders:
      PREVIEW_ENABLED && preview?.length ? preview.join(',') : undefined,
    provider: search.provider === 'all' ? 'all' : family(search.provider),
    range,
  };
}

/** Undefined is unrestricted; [] is deliberately empty and must never become an unfiltered request. */
export function intersectProviders(
  ...scopes: (readonly ProviderFamily[] | undefined)[]
): ProviderFamily[] | undefined {
  const present = scopes.filter(
    (scope): scope is readonly ProviderFamily[] => scope !== undefined,
  );
  return present.length
    ? PROVIDER_FAMILIES.filter((provider) =>
        present.every((scope) => scope.includes(provider)),
      )
    : undefined;
}

interface PreviewContextValue {
  previewProviders: readonly ProviderFamily[] | undefined;
  scope: ProviderScope;
  explicit: boolean;
  providers: readonly ProviderFamily[] | undefined;
  setScope: (scope: ProviderScope) => void;
}
const PreviewContext = createContext<PreviewContextValue>({
  previewProviders: undefined,
  scope: 'all',
  explicit: false,
  providers: undefined,
  setScope: () => {},
});

/** Mount once inside the authenticated root. Each query captures its own effective providers. */
export function PreviewProvider({ children }: { children: ReactNode }) {
  const search = useRouterState({
    select: (state) => state.location.search,
  }) as PreviewSearch;
  const navigate = useNavigate();
  const inventory = useUpstreams();
  const previewProviders = useMemo(
    () =>
      PREVIEW_ENABLED ? parseProviders(search.previewProviders) : undefined,
    [search.previewProviders],
  );
  const configuredProviders = PROVIDER_FAMILIES.filter(
    (provider) =>
      (previewProviders === undefined || previewProviders.includes(provider)) &&
      inventory.data?.upstreams.some(
        (upstream) => providerOf(upstream.kind) === provider,
      ),
  );
  const automatic =
    configuredProviders.length === 1 ? configuredProviders[0] : 'all';
  const scope: ProviderScope = search.provider ?? automatic;
  const value = useMemo<PreviewContextValue>(
    () => ({
      previewProviders,
      scope,
      explicit: search.provider !== undefined,
      providers: intersectProviders(
        previewProviders,
        scope === 'all' ? undefined : [scope],
      ),
      setScope: (next) => {
        void navigate({
          to: '.',
          search: (previous) => ({
            ...previous,
            provider: next === 'all' ? undefined : next,
            selectedId: undefined,
            upstream_id: undefined,
            event_kind: undefined,
          }),
        });
      },
    }),
    [navigate, previewProviders, scope, search.provider],
  );
  return (
    <PreviewContext.Provider value={value}>{children}</PreviewContext.Provider>
  );
}

export function usePreviewControls() {
  return useContext(PreviewContext);
}

/** Installation mode after applying the owner-only provider subset. */
export function usePreviewInstallation(): Installation {
  const installation = useInstallation();
  const { previewProviders } = usePreviewControls();
  return useMemo(() => {
    if (installation.mode === 'unknown' || previewProviders === undefined) {
      return installation;
    }
    const providers = installation.providers.filter((provider) =>
      previewProviders.includes(provider),
    );
    const mode =
      providers.length === 0
        ? 'empty'
        : providers.length === 1
          ? 'single'
          : 'multi';
    return { ...installation, mode, providers };
  }, [installation, previewProviders]);
}
/** Shared provider lens for data hooks that support provider filtering. */
export function useProviderScope() {
  return useContext(PreviewContext);
}

export function useEffectiveProviders(providers?: readonly ProviderFamily[]) {
  return intersectProviders(useContext(PreviewContext).providers, providers);
}

/** Shared by history, delta, SSE and histogram callers; filtering enters their cache identities too. */
export function useProviderFilters(
  filters: Record<string, string | undefined>,
  providers?: readonly ProviderFamily[],
) {
  const effective = useEffectiveProviders(
    intersectProviders(parseProviders(filters.providers), providers),
  );
  const key = effective?.join(',');
  return useMemo(
    () => ({ ...filters, ...(key !== undefined ? { providers: key } : {}) }),
    [filters, key],
  );
}

const SUBSETS = [
  { value: '', label: 'All' },
  { value: 'anthropic', label: 'Anthropic only' },
  { value: 'openai', label: 'OpenAI only' },
  { value: 'gemini', label: 'Gemini only' },
  { value: 'anthropic,openai', label: 'Anthropic + OpenAI' },
] as const;
const SELECT_CLASS =
  'h-10 md:h-9 min-w-0 rounded-sm border border-subtle-strong bg-bg px-2 text-body-sm text-text focus-visible:outline-2 focus-visible:outline-accent';

/** Owner-only preview chrome. Production without the build flag renders nothing. */
export function PreviewControls({ embedded = false }: { embedded?: boolean }) {
  const { previewProviders } = usePreviewControls();
  const navigate = useNavigate();
  const [open, setOpen] = useState(() => {
    if (typeof window === 'undefined') return false;
    try {
      return window.sessionStorage.getItem('cclb.preview.open') === '1';
    } catch {
      return false;
    }
  });
  useEffect(() => {
    if (!PREVIEW_ENABLED) return;
    try {
      window.sessionStorage.setItem('cclb.preview.open', open ? '1' : '0');
    } catch {
      // Session storage is optional in restricted browser contexts.
    }
  }, [open]);
  if (!PREVIEW_ENABLED) return null;
  const subset = previewProviders?.join(',') ?? '';
  const label =
    previewProviders
      ?.map((provider) => PROVIDER_LABELS[provider])
      .join(' + ') ?? 'All configured';
  const setPreviewSearch = (changes: Record<string, unknown>) => {
    void navigate({
      to: '.',
      search: (previous) => ({ ...previous, ...changes }),
    });
  };
  return (
    <aside
      aria-label="Preview controls"
      className={
        embedded
          ? `max-w-full rounded-md border border-subtle-strong bg-bg-sub shadow-overlay ${
              open ? 'p-3' : ''
            }`
          : `fixed bottom-3 right-3 z-40 max-w-[calc(100vw_-_1.5rem)] rounded-md border border-subtle-strong bg-bg-sub shadow-overlay max-lg:hidden ${
              open ? 'p-3' : ''
            }`
      }
    >
      {open ? (
        <>
          <button
            type="button"
            className="flex min-h-10 items-center text-body-sm font-medium text-text focus-visible:outline-2 focus-visible:outline-accent md:min-h-6"
            onClick={() => setOpen(false)}
          >
            Preview
          </button>
          <div className="mt-3 grid grid-cols-2 gap-2">
            <label className="grid gap-1 text-caption text-text-muted">
              Inventory
              <select
                className={SELECT_CLASS}
                value={subset}
                onChange={(event) => {
                  const next = event.target.value || undefined;
                  setPreviewSearch({
                    previewProviders: next,
                    provider: undefined,
                    selectedId: undefined,
                    upstream_id: undefined,
                  });
                }}
              >
                {SUBSETS.map((option) => (
                  <option key={option.value} value={option.value}>
                    {option.label}
                  </option>
                ))}
                {subset &&
                !SUBSETS.some((option) => option.value === subset) ? (
                  <option value={subset}>{label}</option>
                ) : null}
              </select>
            </label>
            <div aria-hidden="true" />
          </div>
          <p className="mt-2 text-caption text-text-muted">
            Preview: {label} inventory · live backend
          </p>
        </>
      ) : (
        <button
          type="button"
          className={
            embedded
              ? 'inline-flex min-h-10 items-center justify-center gap-2 px-3 text-caption font-medium text-text focus-visible:outline-2 focus-visible:outline-accent'
              : 'inline-flex min-h-10 items-center justify-center gap-2 text-caption font-medium text-text focus-visible:outline-2 focus-visible:outline-accent max-lg:size-10 max-lg:p-0 lg:px-3'
          }
          aria-label="Open preview controls"
          onClick={() => setOpen(true)}
        >
          <Eye
            className={embedded ? 'size-4' : 'size-4 lg:hidden'}
            aria-hidden="true"
          />
          <span className={embedded ? undefined : 'max-lg:sr-only'}>
            Preview
          </span>
        </button>
      )}
    </aside>
  );
}
