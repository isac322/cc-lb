import { Radio as BaseRadio } from '@base-ui/react/radio';
import { RadioGroup as BaseRadioGroup } from '@base-ui/react/radio-group';
import {
  ChevronDown,
  ChevronRight,
  ChevronUp,
  Copy,
  Download,
  FileCheck2,
  FileWarning,
  Plus,
  RotateCcw,
  Save,
  Search,
  Trash2,
  X,
} from 'lucide-react';
import {
  createContext,
  type ReactNode,
  type Ref,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
} from 'react';
import { toast } from 'sonner';
import type {
  ConfigEditorResponse,
  ConfigHistoryResponse,
  ConfigOverrideInfo,
  ConfigValidationIssue,
  ConfigValidationReport,
} from '../../lib/api';
import { ApiError, downloadConfigDraft } from '../../lib/api';
import {
  buildConfigEditorModel,
  CONFIG_EDITOR_CATEGORIES,
  CONFIG_EDITOR_UNASSIGNED_LABEL,
  CONFIG_EDITOR_UNASSIGNED_SECTION_ID,
  type ConfigEditorCounts,
  type ConfigEditorLeaf,
  type ConfigEditorLeafKind,
  type ConfigEditorModel,
  type ConfigEditorSection as ConfigEditorSectionModel,
  type ConfigFieldGuidance,
  type ConfigSchema,
  type ConfigSearchResult,
  classifyConfigLeaf,
  configPathToString,
  countConfigEditorLeaves,
  getConfigSchemaVariants,
  getConfigValue,
  humanizeConfigValue,
  isNullableConfigSchema,
  normalizeConfigDraft,
  parseConfigPath,
  recurringJobMetadata,
  resolveConfigFieldGuidance,
  resolveConfigSchema,
  searchConfigLeaves,
  setConfigValue,
  unsetConfigValue,
} from '../../lib/configEditorModel';
import {
  useConfigDraft,
  useConfigEditor,
  useSaveConfigFile,
  useSaveDraft,
  useStatus,
  useValidateConfig,
} from '../../lib/queries';
import {
  Badge,
  Button,
  Card,
  CardBody,
  CardHeader,
  ConfirmDialog,
  cx,
  INPUT_CLASS,
  Notice,
  Section,
  Skeleton,
  ToggleSwitch,
} from '../ui/primitives';
import { RelativeTime } from '../ui/RelativeTime';
import {
  cloneJson,
  isJsonObject,
  isSameJson,
  type JsonObject,
  objectProperties,
  titleForKey,
} from './schema';

interface EditorState {
  value: JsonObject;
  saved: JsonObject;
  revision: number;
  savedAtUnixSecs: number | null;
  hasSavedDraft: boolean;
}

export interface ConfigEditorNavigateTarget {
  category?: string;
  field?: string;
  q?: string;
}

interface ConfigEditorSectionProps {
  history: {
    data?: ConfigHistoryResponse;
    isError: boolean;
  };
  category?: string;
  field?: string;
  query?: string;
  onNavigate: (next: ConfigEditorNavigateTarget) => void;
}

const INPUT_WITH_ERROR_CLASS = `${INPUT_CLASS} aria-[invalid=true]:border-red-400`;
const OPAQUE_STORAGE_URL_PATH = 'storage.url';
const ADMIN_PROVIDERS_PATH = 'admin.auth.providers';
const RECURRING_JOBS_PATH = 'scheduler.recurring_jobs';
const CONFIG_CATEGORY_HEADING_ID = 'config-category-heading';
const CONFIG_SEARCH_RESULTS_ID = 'config-search-results';
const STORAGE_KIND_LABELS: Record<string, string> = {
  sqlite: 'SQLite',
  postgres: 'PostgreSQL',
};
const DESTRUCTIVE_ACTION_LABELS = [
  'Reset',
  'Unset',
  'Remove',
  'Cancel replacement',
];

/**
 * Whether the leaf's path resolves through declared schema properties under
 * the union variants the draft currently selects. The model marks a leaf
 * active when any walked alternative matches, but drifted file keys are also
 * walked under variants that never declared them — this check pins each
 * tagged union to the draft's discriminator value so stale-variant fields
 * stay hidden (and show up in the review list as removals instead).
 */
function leafDeclaredInActiveVariant(
  leaf: ConfigEditorLeaf,
  rootSchema: ConfigSchema,
  source: unknown,
): boolean {
  if (leaf.unknown) return true;
  let candidates: ConfigSchema[] = [rootSchema];
  let prefix: (string | number)[] = [];
  for (const segment of leaf.path) {
    const next: ConfigSchema[] = [];
    for (const candidate of candidates) {
      const resolved = resolveConfigSchema(rootSchema, candidate);
      const variants = getConfigSchemaVariants(rootSchema, resolved);
      const pinned = variants.filter((variant) => {
        if (!variant.tag) return true;
        const tagValue = getConfigValue(source, [
          ...prefix,
          variant.tag.property,
        ]);
        return tagValue === undefined || tagValue === variant.tag.value;
      });
      for (const option of [resolved, ...pinned.map((v) => v.schema)]) {
        const concrete = resolveConfigSchema(rootSchema, option);
        if (typeof segment === 'number') {
          if (isJsonObject(concrete.items)) next.push(concrete.items);
          continue;
        }
        if (segment === '*') {
          if (isJsonObject(concrete.additionalProperties)) {
            next.push(concrete.additionalProperties);
          } else if (isJsonObject(concrete.items)) {
            next.push(concrete.items);
          }
          continue;
        }
        const properties = isJsonObject(concrete.properties)
          ? concrete.properties
          : null;
        if (properties && isJsonObject(properties[segment])) {
          next.push(properties[segment]);
        } else if (isJsonObject(concrete.additionalProperties)) {
          next.push(concrete.additionalProperties);
        }
      }
    }
    if (next.length === 0) return false;
    candidates = next;
    prefix = [...prefix, segment];
  }
  return true;
}

/**
 * Per-model memo of leaf visibility. leafDeclaredInActiveVariant descends the
 * schema for every leaf on every filter pass, but the result depends only on
 * the leaf path, the root schema, and model.source — all fixed for the life
 * of a model instance — so each path is resolved once and reused until the
 * model is rebuilt (draft edits, new editor data). The WeakMap key keeps the
 * cache scoped to the model instance and lets it be collected with it.
 */
const leafVisibilityCache = new WeakMap<
  ConfigEditorModel,
  { schema: ConfigSchema; results: Map<string, boolean> }
>();

function isLeafVisible(
  leaf: ConfigEditorLeaf,
  model: ConfigEditorModel,
  rootSchema: ConfigSchema,
): boolean {
  let entry = leafVisibilityCache.get(model);
  if (!entry || entry.schema !== rootSchema) {
    entry = { schema: rootSchema, results: new Map() };
    leafVisibilityCache.set(model, entry);
  }
  const cached = entry.results.get(leaf.pathString);
  if (cached !== undefined) return cached;
  const visible =
    leaf.active && leafDeclaredInActiveVariant(leaf, rootSchema, model.source);
  entry.results.set(leaf.pathString, visible);
  return visible;
}

function activeDescendants(
  model: ConfigEditorModel,
  rootSchema: ConfigSchema,
  path: string,
): ConfigEditorLeaf[] {
  return model.leaves.filter(
    (leaf) =>
      isLeafVisible(leaf, model, rootSchema) &&
      pathPrefixMatches(path, leaf.pathString),
  );
}

function sectionGroupHomogeneous(
  section: ConfigEditorSectionModel,
  advanced: boolean,
  path: string,
  model: ConfigEditorModel,
  rootSchema: ConfigSchema,
): boolean {
  return section.matchedLeaves
    .filter(
      (leaf) =>
        isLeafVisible(leaf, model, rootSchema) &&
        pathPrefixMatches(path, leaf.pathString),
    )
    .every((leaf) => leaf.advanced === advanced);
}

const StorageUrlReplacementContext = createContext<{
  value: string | null;
  onChange: (value: string | null) => void;
}>({
  value: null,
  onChange: () => undefined,
});

const ConfigSourcesContext = createContext<{
  fileConfig: JsonObject;
}>({
  fileConfig: {},
});

function responseError(error: unknown, fallback: string): string {
  if (error instanceof ApiError) return error.message || fallback;
  if (error instanceof Error) return error.message;
  return fallback;
}

function validationReportFromError(
  error: unknown,
): ConfigValidationReport | null {
  if (!(error instanceof ApiError) || !isJsonObject(error.body)) return null;
  for (const key of ['validation', 'report']) {
    const candidate = error.body[key];
    if (
      isJsonObject(candidate) &&
      typeof candidate.revision === 'number' &&
      isJsonObject(candidate.file) &&
      isJsonObject(candidate.effective) &&
      Array.isArray(candidate.filesystem) &&
      Array.isArray(candidate.overrides)
    ) {
      return candidate as unknown as ConfigValidationReport;
    }
  }
  return null;
}

function reportIssues(report: ConfigValidationReport | null | undefined) {
  if (!report) return [];
  const unique = new Map<string, ConfigValidationIssue>();
  for (const issue of [
    ...report.file.issues,
    ...report.effective.issues,
    ...report.filesystem,
    ...report.overrides,
  ]) {
    const key = [issue.severity, issue.path, issue.code, issue.message].join(
      '\u0000',
    );
    if (!unique.has(key)) unique.set(key, issue);
  }
  return [...unique.values()];
}

function reportIsValid(report: ConfigValidationReport | null | undefined) {
  return Boolean(
    report?.file.valid &&
      report.effective.valid &&
      !report.filesystem.some((issue) => issue.severity === 'error'),
  );
}

function taggedVariants(root: ConfigSchema, schema: ConfigSchema) {
  return getConfigSchemaVariants(root, schema).flatMap((variant) =>
    variant.tag
      ? [
          {
            kind: variant.tag.value,
            property: variant.tag.property,
            label: variant.label,
            schema: variant.schema,
          },
        ]
      : [],
  );
}

function displayValue(value: unknown, sensitive = false): string {
  if (sensitive && value !== undefined) return 'Hidden';
  if (value === undefined) return 'Not set';
  if (value === null) return 'Unset';
  if (typeof value === 'string') return value || 'Empty string';
  if (typeof value === 'boolean' || typeof value === 'number') {
    return String(value);
  }
  if (Array.isArray(value))
    return `${value.length} item${value.length === 1 ? '' : 's'}`;
  return 'Configured object';
}

function overrideForPath(
  overrides: ConfigOverrideInfo[],
  path: string,
): ConfigOverrideInfo | undefined {
  return overrides.find(
    (override) =>
      override.path === path ||
      path.startsWith(`${override.path}.`) ||
      path.startsWith(`${override.path}[`) ||
      override.path.startsWith(`${path}.`) ||
      override.path.startsWith(`${path}[`),
  );
}

function sourceLabel(override: ConfigOverrideInfo | undefined): string {
  if (!override) return 'File / default';
  return override.source === 'cli' ? 'CLI' : 'Environment';
}

function validationTone(issue: ConfigValidationIssue): 'danger' | 'warn' {
  return issue.severity === 'error' ? 'danger' : 'warn';
}

function unitSuffix(unit: 'ms' | 'secs' | 'days' | 'bytes' | null): string {
  switch (unit) {
    case 'ms':
      return 'ms';
    case 'secs':
      return 'sec';
    case 'days':
      return 'days';
    case 'bytes':
      return 'bytes';
    default:
      return '';
  }
}

function pathPrefixMatches(prefix: string, path: string): boolean {
  return (
    path === prefix ||
    path.startsWith(`${prefix}.`) ||
    path.startsWith(`${prefix}[`)
  );
}

function schemaAtConfigPath(
  rootSchema: ConfigSchema,
  path: string,
): ConfigSchema | null {
  const segments = parseConfigPath(path);
  let current: ConfigSchema = rootSchema;
  for (const segment of segments) {
    const resolved = resolveConfigSchema(rootSchema, current);
    const candidates = [
      resolved,
      ...getConfigSchemaVariants(rootSchema, resolved).map(
        (variant) => variant.schema,
      ),
    ];
    let next: ConfigSchema | null = null;
    for (const candidate of candidates) {
      const concrete = resolveConfigSchema(rootSchema, candidate);
      if (typeof segment === 'number') {
        if (isJsonObject(concrete.items)) {
          next = concrete.items;
          break;
        }
        continue;
      }
      if (segment === '*') {
        if (isJsonObject(concrete.additionalProperties)) {
          next = concrete.additionalProperties;
          break;
        }
        if (isJsonObject(concrete.items)) {
          next = concrete.items;
          break;
        }
        continue;
      }
      const properties = isJsonObject(concrete.properties)
        ? concrete.properties
        : null;
      if (properties && isJsonObject(properties[segment])) {
        next = properties[segment];
        break;
      }
      if (isJsonObject(concrete.additionalProperties)) {
        next = concrete.additionalProperties;
        break;
      }
    }
    if (!next) return null;
    current = next;
  }
  return current;
}

type CompoundKind =
  | 'specialized'
  | 'union'
  | 'nullable-object'
  | 'object'
  | 'scalar';

function compoundKindAt(
  rootSchema: ConfigSchema,
  schemaNode: ConfigSchema,
  path: string,
): CompoundKind {
  if (path === ADMIN_PROVIDERS_PATH || path === RECURRING_JOBS_PATH) {
    return 'specialized';
  }
  const resolved = resolveConfigSchema(rootSchema, schemaNode);
  if (taggedVariants(rootSchema, resolved).length) return 'union';
  const hasProperties =
    Object.keys(objectProperties(rootSchema, resolved)).length > 0 ||
    getConfigSchemaVariants(rootSchema, resolved).some(
      (variant) =>
        Object.keys(objectProperties(rootSchema, variant.schema)).length > 0,
    );
  if (!hasProperties) return 'scalar';
  return isNullableConfigSchema(rootSchema, schemaNode)
    ? 'nullable-object'
    : 'object';
}

interface RenderRoot {
  path: string;
  schema: ConfigSchema;
  childKeys?: Set<string>;
}

function unionChildKeys(
  unionPath: string,
  leaves: readonly ConfigEditorLeaf[],
): Set<string> {
  const keys = new Set<string>();
  for (const leaf of leaves) {
    if (!leaf.pathString.startsWith(`${unionPath}.`)) continue;
    const rest = leaf.pathString.slice(unionPath.length + 1);
    const first = rest.split('.')[0]?.split('[')[0];
    if (first) keys.add(first);
  }
  return keys;
}

function renderRootForLeaf(
  leaf: ConfigEditorLeaf,
  section: ConfigEditorSectionModel,
  advanced: boolean,
  model: ConfigEditorModel,
  rootSchema: ConfigSchema,
): RenderRoot {
  const activeSectionLeaves = section.matchedLeaves.filter(
    (candidate) =>
      isLeafVisible(candidate, model, rootSchema) &&
      candidate.advanced === advanced,
  );

  // 1. Longest declared non-glob section path that contains this leaf and is a
  //    compound container wins over generic ancestor collapsing.
  let declared: string | null = null;
  for (const pattern of section.paths) {
    if (pattern.includes('*')) continue;
    if (!pathPrefixMatches(pattern, leaf.pathString)) continue;
    if (!declared || pattern.length > declared.length) declared = pattern;
  }
  if (declared) {
    const node = schemaAtConfigPath(rootSchema, declared);
    if (node) {
      const kind = compoundKindAt(rootSchema, node, declared);
      if (kind === 'union') {
        if (
          sectionGroupHomogeneous(
            section,
            advanced,
            declared,
            model,
            rootSchema,
          )
        ) {
          return {
            path: declared,
            schema: node,
            childKeys: unionChildKeys(declared, activeSectionLeaves),
          };
        }
      } else if (kind === 'specialized') {
        return { path: declared, schema: node };
      } else if (kind === 'nullable-object') {
        if (
          sectionGroupHomogeneous(
            section,
            advanced,
            declared,
            model,
            rootSchema,
          )
        ) {
          return { path: declared, schema: node };
        }
      } else if (kind === 'object') {
        const descendants = activeDescendants(model, rootSchema, declared);
        if (
          descendants.length > 1 &&
          descendants.every(
            (candidate) =>
              candidate.sectionId === section.id &&
              candidate.advanced === advanced,
          )
        ) {
          return { path: declared, schema: node };
        }
      }
    }
  }

  // 2. Nearest specialized ancestor wins over union collapsing so the
  //    dedicated editors keep their add/remove/reorder controls.
  const segments = parseConfigPath(leaf.pathString);
  for (let index = segments.length - 1; index >= 1; index -= 1) {
    if (segments[index - 1] === '*') continue;
    const ancestorPath = configPathToString(segments.slice(0, index));
    const node = schemaAtConfigPath(rootSchema, ancestorPath);
    if (!node) continue;
    if (compoundKindAt(rootSchema, node, ancestorPath) === 'specialized') {
      return { path: ancestorPath, schema: node };
    }
  }

  // 3. Nearest union / nullable-object ancestor — only when every active leaf
  //    this section claims under that ancestor belongs to the same group, so
  //    primary and advanced groups never render the same compound editor.
  for (let index = segments.length - 1; index >= 1; index -= 1) {
    if (segments[index - 1] === '*') continue;
    const ancestorPath = configPathToString(segments.slice(0, index));
    const node = schemaAtConfigPath(rootSchema, ancestorPath);
    if (!node) continue;
    const kind = compoundKindAt(rootSchema, node, ancestorPath);
    if (kind === 'union') {
      if (
        !sectionGroupHomogeneous(
          section,
          advanced,
          ancestorPath,
          model,
          rootSchema,
        )
      )
        continue;
      return {
        path: ancestorPath,
        schema: node,
        childKeys: unionChildKeys(ancestorPath, activeSectionLeaves),
      };
    }
    if (kind === 'nullable-object') {
      if (
        !sectionGroupHomogeneous(
          section,
          advanced,
          ancestorPath,
          model,
          rootSchema,
        )
      )
        continue;
      return { path: ancestorPath, schema: node };
    }
  }
  // 4. Nearest plain object whose active leaves all live in this section+group.
  for (let index = segments.length - 1; index >= 1; index -= 1) {
    if (segments[index - 1] === '*') continue;
    const ancestorPath = configPathToString(segments.slice(0, index));
    const node = schemaAtConfigPath(rootSchema, ancestorPath);
    if (!node) continue;
    if (compoundKindAt(rootSchema, node, ancestorPath) !== 'object') continue;
    const descendants = activeDescendants(model, rootSchema, ancestorPath);
    if (
      descendants.length > 1 &&
      descendants.every(
        (candidate) =>
          candidate.sectionId === section.id && candidate.advanced === advanced,
      )
    ) {
      return { path: ancestorPath, schema: node };
    }
  }

  return { path: leaf.pathString, schema: leaf.schema };
}

function sectionRenderRoots(
  section: ConfigEditorSectionModel,
  advanced: boolean,
  model: ConfigEditorModel,
  rootSchema: ConfigSchema,
): RenderRoot[] {
  const roots = new Map<string, RenderRoot>();
  const sectionLeaves = section.matchedLeaves.filter(
    (leaf) => leaf.advanced === advanced,
  );
  const activeLeaves = sectionLeaves.filter((leaf) =>
    isLeafVisible(leaf, model, rootSchema),
  );

  // Declared container paths render even with no active leaves so specialized
  // editors (admin providers, recurring jobs) and nullable toggles stay
  // reachable.
  for (const pattern of section.paths) {
    if (pattern.includes('*')) continue;
    const node = schemaAtConfigPath(rootSchema, pattern);
    if (!node) continue;
    const kind = compoundKindAt(rootSchema, node, pattern);
    if (kind === 'scalar') continue;
    const hasLeaves = section.matchedLeaves.some((leaf) =>
      pathPrefixMatches(pattern, leaf.pathString),
    );
    if (!hasLeaves && kind !== 'specialized' && kind !== 'nullable-object') {
      continue;
    }
    if (kind === 'object') {
      const descendants = activeDescendants(model, rootSchema, pattern);
      if (
        !descendants.every(
          (candidate) =>
            candidate.sectionId === section.id &&
            candidate.advanced === advanced,
        )
      ) {
        continue;
      }
    }
    if (kind === 'union') {
      if (
        !sectionGroupHomogeneous(section, advanced, pattern, model, rootSchema)
      )
        continue;
      roots.set(pattern, {
        path: pattern,
        schema: node,
        childKeys: unionChildKeys(pattern, activeLeaves),
      });
    } else if (
      kind === 'nullable-object' &&
      !sectionGroupHomogeneous(section, advanced, pattern, model, rootSchema)
    ) {
    } else {
      roots.set(pattern, { path: pattern, schema: node });
    }
  }

  for (const leaf of activeLeaves) {
    const root = renderRootForLeaf(leaf, section, advanced, model, rootSchema);
    const existing = roots.get(root.path);
    if (existing) {
      if (root.childKeys) {
        existing.childKeys = new Set([
          ...(existing.childKeys ?? []),
          ...root.childKeys,
        ]);
      }
      continue;
    }
    roots.set(root.path, root);
  }

  return [...roots.values()].sort((left, right) =>
    left.path.localeCompare(right.path),
  );
}

export function ConfigEditorSection({
  history,
  category,
  field,
  query,
  onNavigate,
}: ConfigEditorSectionProps) {
  const editorQuery = useConfigEditor();
  const draftQuery = useConfigDraft();
  const saveDraft = useSaveDraft();
  const validate = useValidateConfig();
  const saveFile = useSaveConfigFile();
  const [state, setState] = useState<EditorState | null>(null);
  const status = useStatus();
  const [localValidation, setLocalValidation] = useState<{
    revision: number;
    report: ConfigValidationReport;
    storageUrlReplacement: string | null;
  } | null>(null);
  const [selectedCategory, setSelectedCategory] = useState<string>(
    CONFIG_EDITOR_CATEGORIES.some((entry) => entry.id === category)
      ? (category as string)
      : 'network',
  );
  const [searchOpen, setSearchOpen] = useState(false);
  const searchWrapperRef = useRef<HTMLDivElement | null>(null);
  const [openAdvanced, setOpenAdvanced] = useState<Record<string, boolean>>({});
  const [searchText, setSearchText] = useState(query ?? '');
  const [confirmationOpen, setConfirmationOpen] = useState(false);
  const [selfLockoutRequired, setSelfLockoutRequired] = useState(false);
  const [selfLockoutAcknowledged, setSelfLockoutAcknowledged] = useState(false);
  const [downloadPending, setDownloadPending] = useState(false);
  const [downloadError, setDownloadError] = useState<string | null>(null);
  const [actionError, setActionError] = useState<string | null>(null);
  const [storageUrlReplacement, setStorageUrlReplacement] = useState<
    string | null
  >(null);
  const downloadLock = useRef(false);
  const validationSummaryRef = useRef<HTMLDivElement | null>(null);
  const handledFieldRef = useRef<string | null>(null);

  const editorData = editorQuery.data;
  const draftData = draftQuery.data ?? editorData;

  useEffect(() => {
    if (!editorData || !draftData) return;
    const source = draftData.draft ?? editorData.file_config ?? {};
    if (!isJsonObject(source)) return;
    const revision = draftData.revision;
    setState((current) => {
      if (current && !isSameJson(current.value, current.saved)) return current;
      if (current && current.revision > revision) return current;
      const nextSaved = cloneJson(source);
      if (
        current &&
        current.revision === revision &&
        current.hasSavedDraft === (draftData.draft !== null) &&
        current.savedAtUnixSecs === draftData.saved_at_unix_secs &&
        isSameJson(current.value, nextSaved) &&
        isSameJson(current.saved, nextSaved)
      ) {
        return current;
      }
      return {
        value: nextSaved,
        saved: cloneJson(nextSaved),
        revision,
        savedAtUnixSecs: draftData.saved_at_unix_secs,
        hasSavedDraft: draftData.draft !== null,
      };
    });
  }, [draftData, editorData]);

  // URL → local state sync (tolerant: unknown categories fall back, deep-link
  // fields open their category and Advanced disclosure before focusing).
  useEffect(() => {
    if (
      category &&
      CONFIG_EDITOR_CATEGORIES.some((entry) => entry.id === category)
    ) {
      setSelectedCategory(category);
    }
  }, [category]);

  useEffect(() => {
    setSearchText(query ?? '');
  }, [query]);

  // The search overlay is a floating panel: outside pointerdown closes it
  // without clearing the query, matching the Escape behavior on the input.
  useEffect(() => {
    if (!searchOpen) return;
    const onPointerDown = (event: PointerEvent) => {
      if (
        event.target instanceof Node &&
        !searchWrapperRef.current?.contains(event.target)
      ) {
        setSearchOpen(false);
      }
    };
    document.addEventListener('pointerdown', onPointerDown);
    return () => document.removeEventListener('pointerdown', onPointerDown);
  }, [searchOpen]);

  const dirty = state ? !isSameJson(state.value, state.saved) : false;
  const serverRevision = draftData?.revision ?? null;
  const stale =
    state !== null &&
    serverRevision !== null &&
    state.revision !== serverRevision;
  const localValidationMatches =
    localValidation !== null &&
    state !== null &&
    localValidation.revision === state.revision &&
    localValidation.storageUrlReplacement === storageUrlReplacement;
  const storedValidation = draftData?.last_validation ?? null;
  const storedValidatedRevision = draftData?.last_validated_revision ?? null;
  const serverReportMatches =
    storageUrlReplacement === null &&
    state !== null &&
    storedValidation?.revision === state.revision;
  const validation = localValidationMatches
    ? localValidation.report
    : serverReportMatches
      ? storedValidation
      : null;
  const validatedRevision = localValidationMatches
    ? localValidation.revision
    : storedValidatedRevision === state?.revision
      ? storedValidatedRevision
      : null;
  const validationReportCurrent = localValidationMatches || serverReportMatches;
  const issues = useMemo(() => reportIssues(validation), [validation]);
  const validationCurrent =
    state !== null && !dirty && !stale && validatedRevision === state.revision;
  const fileValid = Boolean(validation?.file.valid);
  const effectiveValid = Boolean(validation?.effective.valid);
  const filesystemValid = !(validation?.filesystem ?? []).some(
    (issue) => issue.severity === 'error',
  );
  const fileWritable = editorData?.file.mode === 'writable';
  const canSaveDraft = Boolean(
    state && (dirty || !state.hasSavedDraft) && !stale,
  );
  const canValidate = Boolean(state?.hasSavedDraft && !dirty && !stale);
  const canSaveFile = Boolean(
    validationCurrent &&
      fileValid &&
      effectiveValid &&
      filesystemValid &&
      fileWritable,
  );
  const canDownload = Boolean(validationCurrent && fileValid);
  const adminProvidersChanged = Boolean(
    state &&
      editorData &&
      !isSameJson(
        getConfigValue(state.value, ADMIN_PROVIDERS_PATH),
        getConfigValue(editorData.file_config, ADMIN_PROVIDERS_PATH),
      ),
  );
  const latestHistorySavedAtUnixSecs = useMemo(
    () =>
      history.data?.entries.reduce(
        (latest, entry) => Math.max(latest, entry.saved_at_unix_secs),
        0,
      ) || null,
    [history.data],
  );
  const latestSavedAtUnixSecs = Math.max(
    state?.savedAtUnixSecs ?? 0,
    latestHistorySavedAtUnixSecs ?? 0,
  );
  const savedAfterStart = Boolean(
    latestSavedAtUnixSecs &&
      status.data &&
      latestSavedAtUnixSecs >
        Math.floor(Date.now() / 1000) - status.data.uptime_secs,
  );
  const requiresSelfLockoutConfirmation =
    adminProvidersChanged || selfLockoutRequired;

  const model = useMemo(
    () =>
      editorData && state
        ? buildConfigEditorModel(
            {
              ...editorData,
              last_validation: validation ?? null,
              last_validated_revision: validatedRevision,
            },
            state.value,
          )
        : null,
    [editorData, state, validation, validatedRevision],
  );
  useEffect(() => {
    if (!field) {
      handledFieldRef.current = null;
      return;
    }
    if (!model || handledFieldRef.current === field) return;
    handledFieldRef.current = field;
    const classification = classifyConfigLeaf(field);
    setSelectedCategory(classification.categoryId);
    if (classification.advanced && classification.sectionId) {
      const sectionId = classification.sectionId;
      setOpenAdvanced((current) => ({ ...current, [sectionId]: true }));
    }
    focusConfigPath(field);
  }, [field, model]);

  const categories = useMemo(() => {
    if (!model || !editorData) return [];
    const rootSchema = editorData.schema as ConfigSchema;
    return model.categories.map((entry) => ({
      ...entry,
      // Counts reflect the fields this category actually displays: visible
      // section leaves plus the unassigned leaves routed to this category.
      counts: countConfigEditorLeaves([
        ...entry.leaves.filter((leaf) =>
          isLeafVisible(leaf, model, rootSchema),
        ),
        ...model.unassigned.leaves.filter(
          (leaf) =>
            isLeafVisible(leaf, model, rootSchema) &&
            leaf.categoryId === entry.id,
        ),
      ]),
    }));
  }, [model, editorData]);

  const activeCategory =
    categories.find((entry) => entry.id === selectedCategory) ??
    categories[0] ??
    null;

  const searchResults = useMemo(() => {
    const trimmed = searchText.trim();
    if (!model || !editorData || !trimmed) return [];
    const rootSchema = editorData.schema as ConfigSchema;
    return searchConfigLeaves(model, trimmed)
      .filter((result) => isLeafVisible(result.leaf, model, rootSchema))
      .slice(0, 24);
  }, [model, editorData, searchText]);

  const reviewEntries = useMemo(() => {
    if (!model || !editorData) return [];
    const entries: {
      pathString: string;
      categoryId: string;
      dangerous: boolean;
      dangerImpact: string | null;
      sensitive: boolean;
      oldValue: unknown;
      newValue: unknown;
    }[] = model.leaves
      // Inactive-but-modified leaves are included: switching a union variant
      // deletes every path the old variant owned, and those removals must
      // show up as old → Not set rows.
      .filter((leaf) => leaf.modified)
      .map((leaf) => ({
        pathString: leaf.pathString,
        categoryId: leaf.categoryId,
        dangerous: leaf.dangerous,
        dangerImpact: leaf.dangerImpact,
        sensitive:
          Boolean(leaf.override?.sensitive) ||
          leaf.pathString === OPAQUE_STORAGE_URL_PATH,
        oldValue: leaf.fileValue,
        newValue: leaf.editorValue,
      }));
    // Container-level diffs for the specialized editors are added directly,
    // but only when no item leaf under the container already describes the
    // change — otherwise the row would repeat what the item rows say.
    for (const containerPath of [ADMIN_PROVIDERS_PATH, RECURRING_JOBS_PATH]) {
      const oldValue = getConfigValue(editorData.file_config, containerPath);
      const newValue = getConfigValue(model.source, containerPath);
      if (isSameJson(oldValue, newValue)) continue;
      const covered = entries.some((entry) =>
        pathPrefixMatches(containerPath, entry.pathString),
      );
      if (covered) continue;
      const classification = classifyConfigLeaf(containerPath);
      entries.push({
        pathString: containerPath,
        categoryId: classification.categoryId,
        dangerous: classification.dangerous,
        dangerImpact: classification.dangerImpact,
        sensitive: false,
        oldValue,
        newValue,
      });
    }
    const categoryOrder = new Map(
      CONFIG_EDITOR_CATEGORIES.map((entry, index) => [
        entry.id as string,
        index,
      ]),
    );
    return entries.sort((left, right) => {
      if (left.dangerous !== right.dangerous) return left.dangerous ? -1 : 1;
      const order =
        (categoryOrder.get(left.categoryId) ?? 99) -
        (categoryOrder.get(right.categoryId) ?? 99);
      return order || left.pathString.localeCompare(right.pathString);
    });
  }, [model, editorData]);

  const updateValue = (next: unknown) => {
    if (!isJsonObject(next)) return;
    setState((current) => (current ? { ...current, value: next } : current));
    setLocalValidation(null);
    setActionError(null);
    setDownloadError(null);
  };
  const selectCategory = (id: string) => {
    setSelectedCategory(id);
    // Category switches keep the active search text but drop the field
    // deep-link — the field belongs to the previous category.
    onNavigate({ category: id, q: searchText.trim() || undefined });
  };

  const revealConfigPath = (path: string) => {
    const classification = classifyConfigLeaf(path);
    setSelectedCategory(classification.categoryId);
    if (classification.advanced && classification.sectionId) {
      const sectionId = classification.sectionId;
      setOpenAdvanced((current) => ({ ...current, [sectionId]: true }));
    }
    focusConfigPath(path);
  };

  const activateSearchResult = (path: string) => {
    const classification = classifyConfigLeaf(path);
    setSelectedCategory(classification.categoryId);
    if (classification.advanced && classification.sectionId) {
      const sectionId = classification.sectionId;
      setOpenAdvanced((current) => ({ ...current, [sectionId]: true }));
    }
    onNavigate({
      category: classification.categoryId,
      field: path,
      q: searchText.trim() || undefined,
    });
    focusConfigPath(path);
  };

  const handleSaveDraft = () => {
    if (!state || !editorData || !canSaveDraft) return;
    const editorSnapshot = cloneJson(state.value);
    const submitted = normalizeConfigDraft(editorData.schema, state.value);
    const submittedRevision = state.revision;
    setActionError(null);
    saveDraft.mutate(
      { draft: submitted, expected_revision: submittedRevision },
      {
        onSuccess: (response) => {
          setState((current) => {
            if (!current || current.revision !== submittedRevision)
              return current;
            const valueUnchanged = isSameJson(current.value, editorSnapshot);
            return {
              ...current,
              value: valueUnchanged ? cloneJson(submitted) : current.value,
              saved: cloneJson(submitted),
              revision: response.revision,
              savedAtUnixSecs: response.saved_at_unix_secs,
              hasSavedDraft: true,
            };
          });
          setLocalValidation(null);
          toast.success('Configuration draft saved');
        },
        onError: (error) => {
          setActionError(
            responseError(error, 'Failed to save the configuration draft.'),
          );
        },
      },
    );
  };

  const handleValidate = () => {
    if (!state || !canValidate) return;
    setActionError(null);
    validate.mutate(
      {
        expected_revision: state.revision,
        storage_url_replacement: storageUrlReplacement ?? undefined,
      },
      {
        onSuccess: (report) => {
          setLocalValidation({
            revision: report.revision,
            report,
            storageUrlReplacement,
          });
          if (reportIsValid(report)) {
            toast.success('Configuration is valid');
          } else {
            toast.error('Configuration needs attention');
            requestAnimationFrame(() => validationSummaryRef.current?.focus());
          }
        },
        onError: (error) => {
          setActionError(
            responseError(error, 'Configuration validation failed.'),
          );
        },
      },
    );
  };

  const persistConfigFile = () => {
    if (!canSaveFile || !editorData || !state) return;
    setActionError(null);
    saveFile.mutate(
      {
        expected_revision: state.revision,
        expected_fingerprint: editorData.file.fingerprint,
        storage_url_replacement: storageUrlReplacement ?? undefined,
        confirm_self_lockout: requiresSelfLockoutConfirmation,
      },
      {
        onSuccess: (response) => {
          setConfirmationOpen(false);
          setSelfLockoutRequired(false);
          setSelfLockoutAcknowledged(false);
          setStorageUrlReplacement(null);
          toast.success('Configuration file saved');
          setState((current) =>
            current
              ? {
                  ...current,
                  savedAtUnixSecs:
                    response.saved_at_unix_secs ?? current.savedAtUnixSecs,
                }
              : current,
          );
        },
        onError: (error) => {
          const report = validationReportFromError(error);
          if (report) {
            setLocalValidation({
              revision: report.revision,
              report,
              storageUrlReplacement,
            });
          }
          if (
            error instanceof ApiError &&
            error.code === 'self_lockout_confirmation_required'
          ) {
            setSelfLockoutRequired(true);
            setConfirmationOpen(true);
            return;
          }
          setConfirmationOpen(false);
          setActionError(
            responseError(error, 'Failed to save the configuration file.'),
          );
        },
      },
    );
  };

  const handleSaveFileRequest = () => {
    if (!canSaveFile || !editorData || !state) return;
    if (editorData.file.exists || requiresSelfLockoutConfirmation) {
      setConfirmationOpen(true);
      return;
    }
    persistConfigFile();
  };

  const handleDownload = async () => {
    if (!state || !canDownload || downloadLock.current) return;
    downloadLock.current = true;
    setDownloadPending(true);
    setDownloadError(null);
    try {
      await downloadConfigDraft(
        state.revision,
        storageUrlReplacement ?? undefined,
      );
      toast.success('Validated TOML downloaded');
    } catch (error) {
      setDownloadError(responseError(error, 'Validated TOML download failed.'));
    } finally {
      downloadLock.current = false;
      setDownloadPending(false);
    }
  };

  const loading = !editorData || !draftData || !state;
  const loadError =
    (!editorData && editorQuery.isError) || (!draftData && draftQuery.isError);

  return (
    <Section
      title="Configuration"
      subtitle="Edit the startup configuration, validate it, then save or download TOML."
    >
      <div data-testid="config-status-zone" className="space-y-2">
        <div className="flex flex-wrap items-baseline gap-x-4 gap-y-1 text-xs text-text-faint">
          <EditorMetadata
            loading={loading}
            revision={state?.revision ?? null}
            savedAtUnixSecs={state?.savedAtUnixSecs ?? null}
            validatedRevision={validatedRevision}
            filePath={editorData?.file.path}
          />
          {editorData ? <RunningSummary data={editorData} /> : null}
        </div>

        {loadError ? (
          <Notice
            tone="danger"
            title="Configuration editor unavailable"
            action={
              <Button
                size="sm"
                loading={editorQuery.isFetching || draftQuery.isFetching}
                onClick={() => {
                  if (!editorData) void editorQuery.refetch();
                  if (!draftData) void draftQuery.refetch();
                }}
              >
                Retry
              </Button>
            }
          >
            The editor or saved draft could not be loaded. Existing runtime
            configuration is unchanged.
          </Notice>
        ) : null}

        {savedAfterStart ? (
          <Notice
            tone="warning"
            title={
              <span data-testid="restart-drift-banner">
                Saved at{' '}
                <RelativeTime ts={new Date(latestSavedAtUnixSecs * 1000)} />,
                not applied yet — restart cc-lb
              </span>
            }
          >
            cc-lb is still using its startup configuration. If this is only a
            draft, validate and save it to the config file before restarting.
          </Notice>
        ) : null}
        {history.isError ? (
          <Notice tone="warning" title="Restart status may be incomplete">
            Saved config history could not be loaded, so the editor cannot
            confirm whether a saved revision is still waiting for a restart.
            Retry the history request below.
          </Notice>
        ) : null}
        {stale ? (
          <Notice tone="danger" title="Draft revision changed">
            This editor is pinned to revision {state?.revision}, but the latest
            draft on the server is revision {serverRevision}. Copy any unsaved
            values, then refresh the page to load the latest draft before
            saving.
          </Notice>
        ) : null}

        {dirty ? (
          <Notice tone="warning" title="Draft has unsaved changes">
            Save the draft before validation. The config file and download
            actions remain locked until this exact revision passes validation.
          </Notice>
        ) : !state?.hasSavedDraft && state ? (
          <Notice tone="info" title="Start with a saved draft">
            Save this file configuration as a draft before validating it.
          </Notice>
        ) : null}

        {actionError ? (
          <Notice tone="danger" title="Configuration action failed">
            {actionError}
          </Notice>
        ) : null}

        {downloadError ? (
          <Notice
            tone="danger"
            title="Download failed"
            action={
              <div className="flex gap-2">
                <Button
                  size="sm"
                  iconLeft={<Copy className="h-3.5 w-3.5" />}
                  onClick={() => {
                    if (!state) return;
                    void navigator.clipboard
                      ?.writeText(JSON.stringify(state.value, null, 2))
                      .then(() => toast.success('Draft JSON copied'))
                      .catch(() => toast.error('Could not copy draft JSON'));
                  }}
                >
                  Copy draft JSON
                </Button>
                <Button size="sm" onClick={() => void handleDownload()}>
                  Retry
                </Button>
              </div>
            }
          >
            {downloadError} The validated draft remains stored on the server;
            retry the TOML download or copy the draft JSON as a recovery
            fallback.
          </Notice>
        ) : null}

        <ValidationSummary
          ref={validationSummaryRef}
          report={validation}
          current={validationReportCurrent}
          issues={issues}
          onIssueClick={revealConfigPath}
        />
      </div>

      <div>
        {activeCategory ? (
          <nav
            aria-label="Configuration categories"
            data-testid="config-category-nav"
            className="overflow-hidden rounded-t-sm border border-subtle border-b-0"
          >
            <CategoryNavList
              categories={categories}
              activeCategoryId={activeCategory.id}
              onSelect={selectCategory}
            />
          </nav>
        ) : null}
        <Card
          data-testid="config-editor-card"
          className={activeCategory ? 'rounded-t-none border-t-0!' : undefined}
        >
          <CardHeader
            titleId={CONFIG_CATEGORY_HEADING_ID}
            title={activeCategory?.label ?? 'Configuration'}
            subtitle={
              activeCategory ? (
                <span className="flex flex-wrap items-center gap-x-2 gap-y-1">
                  <span>{activeCategory.description}</span>
                  <CategoryStatusBadges counts={activeCategory.counts} />
                </span>
              ) : (
                <Skeleton className="h-3 w-56 max-w-full" />
              )
            }
            action={
              <div className="flex w-full flex-wrap items-center justify-end gap-2">
                <ConfigSearch
                  searchText={searchText}
                  results={searchResults}
                  open={searchOpen}
                  wrapperRef={searchWrapperRef}
                  onOpenChange={setSearchOpen}
                  onSearchTextChange={setSearchText}
                  onActivate={activateSearchResult}
                />
                <Button
                  size="sm"
                  iconLeft={<Save className="h-3.5 w-3.5" />}
                  loading={saveDraft.isPending}
                  disabled={!canSaveDraft}
                  onClick={handleSaveDraft}
                >
                  Save draft
                </Button>
                <Button
                  size="sm"
                  iconLeft={<FileCheck2 className="h-3.5 w-3.5" />}
                  loading={validate.isPending}
                  disabled={!canValidate}
                  onClick={handleValidate}
                >
                  Validate
                </Button>
                <Button
                  size="sm"
                  variant="primary"
                  iconLeft={<Save className="h-3.5 w-3.5" />}
                  loading={saveFile.isPending}
                  disabled={!canSaveFile}
                  onClick={handleSaveFileRequest}
                >
                  Save to config file
                </Button>
                <Button
                  size="sm"
                  iconLeft={<Download className="h-3.5 w-3.5" />}
                  loading={downloadPending}
                  disabled={!canDownload}
                  onClick={() => void handleDownload()}
                >
                  Download TOML
                </Button>
              </div>
            }
          />
          <CardBody className="space-y-4">
            {loading ? (
              <ConfigEditorSkeleton />
            ) : editorData && state && model && activeCategory ? (
              <StorageUrlReplacementContext.Provider
                value={{
                  value: storageUrlReplacement,
                  onChange: (next) => {
                    setStorageUrlReplacement(next);
                    setLocalValidation(null);
                    setActionError(null);
                    setDownloadError(null);
                  },
                }}
              >
                <ConfigSourcesContext.Provider
                  value={{ fileConfig: editorData.file_config }}
                >
                  <div
                    className="space-y-3"
                    data-testid="structured-config-editor"
                  >
                    <CategoryPanel
                      category={activeCategory}
                      model={model}
                      rootSchema={editorData.schema as ConfigSchema}
                      value={state.value}
                      defaultConfig={editorData.default_config}
                      effectiveConfig={editorData.effective_config}
                      overrides={editorData.overrides ?? []}
                      issues={issues}
                      openAdvanced={openAdvanced}
                      onToggleAdvanced={(sectionId, open) =>
                        setOpenAdvanced((current) => ({
                          ...current,
                          [sectionId]: open,
                        }))
                      }
                      onChange={updateValue}
                    />
                  </div>
                </ConfigSourcesContext.Provider>
              </StorageUrlReplacementContext.Provider>
            ) : (
              <div
                className="min-h-[420px]"
                data-testid="config-editor-reserved"
              />
            )}
          </CardBody>
        </Card>
      </div>

      <ConfirmDialog
        open={confirmationOpen}
        onOpenChange={(open) => {
          setConfirmationOpen(open);
          if (!open) setSelfLockoutAcknowledged(false);
        }}
        title={
          requiresSelfLockoutConfirmation
            ? editorData?.file.exists
              ? 'Confirm config overwrite and admin access changes'
              : 'Confirm admin access changes'
            : 'Overwrite the current config file?'
        }
        description={
          <span className="block space-y-3">
            {editorData?.file.exists ? (
              <span className="block">
                This atomically replaces {editorData.file.path}. Comments and
                formatting in the current TOML file are not preserved.
              </span>
            ) : null}
            {reviewEntries.length ? (
              <span
                data-testid="config-review-list"
                className="block space-y-2"
              >
                <span className="block text-[10px] uppercase tracking-wider text-text-faint">
                  Changes in this save
                </span>
                {(() => {
                  const dangerous = reviewEntries.filter(
                    (entry) => entry.dangerous,
                  );
                  const groups: {
                    key: string;
                    label: string;
                    entries: typeof reviewEntries;
                  }[] = [];
                  if (dangerous.length) {
                    groups.push({
                      key: 'dangerous',
                      label: 'Dangerous changes',
                      entries: dangerous,
                    });
                  }
                  for (const categoryMeta of CONFIG_EDITOR_CATEGORIES) {
                    const entries = reviewEntries.filter(
                      (entry) =>
                        !entry.dangerous &&
                        entry.categoryId === categoryMeta.id,
                    );
                    if (entries.length) {
                      groups.push({
                        key: categoryMeta.id,
                        label: categoryMeta.label,
                        entries,
                      });
                    }
                  }
                  return groups.map((group) => (
                    <span key={group.key} className="block">
                      <span className="block text-xs font-medium text-text">
                        {group.label}
                      </span>
                      {group.entries.map((entry) => (
                        <span
                          key={entry.pathString}
                          className={cx(
                            'block break-all py-0.5 text-xs',
                            entry.dangerous
                              ? 'text-[color:var(--color-warn-text)]'
                              : 'text-text-muted',
                          )}
                        >
                          <span className="font-mono">{entry.pathString}</span>:{' '}
                          {displayValue(entry.oldValue, entry.sensitive)} →{' '}
                          {displayValue(entry.newValue, entry.sensitive)}
                          {entry.dangerous && entry.dangerImpact ? (
                            <span className="block text-[10px]">
                              {entry.dangerImpact}
                            </span>
                          ) : null}
                        </span>
                      ))}
                    </span>
                  ));
                })()}
              </span>
            ) : null}
            {requiresSelfLockoutConfirmation ? (
              <span className="block space-y-2">
                <span className="block font-medium text-[color:var(--color-warn-text)]">
                  Admin authentication providers changed. A wrong provider kind,
                  ID, token environment variable, domain, or audience can lock
                  you out after restart.
                </span>
                <label className="flex items-start gap-2 text-xs text-text">
                  <input
                    type="checkbox"
                    data-testid="self-lockout-ack"
                    className="mt-0.5"
                    checked={selfLockoutAcknowledged}
                    onChange={(event) =>
                      setSelfLockoutAcknowledged(event.target.checked)
                    }
                  />
                  <span>
                    I confirmed another valid admin access path exists before
                    saving.
                  </span>
                </label>
              </span>
            ) : null}
          </span>
        }
        confirmLabel={
          requiresSelfLockoutConfirmation
            ? 'Save and accept lockout risk'
            : 'Overwrite file'
        }
        destructive={requiresSelfLockoutConfirmation}
        confirmDisabled={
          requiresSelfLockoutConfirmation && !selfLockoutAcknowledged
        }
        pending={saveFile.isPending}
        closeOnConfirm={false}
        onConfirm={persistConfigFile}
      />
    </Section>
  );
}

function RunningSummary({ data }: { data: ConfigEditorResponse }) {
  const effective = data.effective_config;
  const proxyAddr = getConfigValue(effective, 'listener.proxy_addr');
  const adminAddr = getConfigValue(effective, 'listener.admin_addr');
  const storageKind = getConfigValue(effective, 'storage.kind');
  const providers = getConfigValue(effective, ADMIN_PROVIDERS_PATH);
  const providerCount = Array.isArray(providers) ? providers.length : 0;
  const fileReason =
    data.file.reason ??
    'This process cannot atomically replace the config file. You can still save and validate a draft, then download TOML for manual deployment.';
  return (
    <div data-testid="config-running-summary" className="contents">
      <span className="font-medium text-text-muted">Running configuration</span>
      <span>
        proxy{' '}
        <span className="font-mono text-text">
          {typeof proxyAddr === 'string' ? proxyAddr : '—'}
        </span>
      </span>
      <span>
        admin{' '}
        <span className="font-mono text-text">
          {typeof adminAddr === 'string' ? adminAddr : '—'}
        </span>
      </span>
      <span>
        storage{' '}
        <span className="font-mono text-text">
          {typeof storageKind === 'string' ? storageKind : '—'}
        </span>
      </span>
      <span>
        {providerCount} admin provider{providerCount === 1 ? '' : 's'}
      </span>
      {data.file.mode === 'read_only' ? (
        <>
          <Badge tone="warn">
            {data.file.exists
              ? 'Config file read-only'
              : 'Config file missing — read-only'}
          </Badge>
          <span className="basis-full">{fileReason}</span>
        </>
      ) : !data.file.exists ? (
        <>
          <Badge tone="neutral">Config file missing</Badge>
          <span className="basis-full">
            Saving will create {data.file.path}.
          </span>
        </>
      ) : null}
    </div>
  );
}

function EditorMetadata({
  loading,
  revision,
  validatedRevision,
  savedAtUnixSecs,
  filePath,
}: {
  loading: boolean;
  revision: number | null;
  validatedRevision: number | null;
  savedAtUnixSecs: number | null;
  filePath?: string;
}) {
  return (
    <div data-testid="config-editor-metadata" className="contents">
      <MetadataValue
        label="Draft revision"
        loading={loading}
        value={revision ?? '—'}
      />
      <MetadataValue
        label="Validated revision"
        loading={loading}
        value={validatedRevision ?? '—'}
      />
      <MetadataValue
        label="Draft saved"
        loading={loading}
        value={
          savedAtUnixSecs ? (
            <RelativeTime ts={new Date(savedAtUnixSecs * 1000)} />
          ) : (
            '—'
          )
        }
      />
      <MetadataValue
        label="Config file"
        loading={loading}
        value={<span className="break-all font-mono">{filePath ?? '—'}</span>}
      />
    </div>
  );
}

function MetadataValue({
  label,
  loading,
  value,
}: {
  label: string;
  loading: boolean;
  value: ReactNode;
}) {
  return (
    <span className="inline-flex min-w-0 items-baseline gap-1.5">
      <span>{label}</span>
      <span className="min-w-0 text-text-muted">
        {loading ? (
          <Skeleton as="span" className="inline-block h-3 w-16 max-w-full" />
        ) : (
          value
        )}
      </span>
    </span>
  );
}

function ValidationSummary({
  ref,
  report,
  current,
  issues,
  onIssueClick,
}: {
  ref?: Ref<HTMLDivElement>;
  report: ConfigValidationReport | null | undefined;
  current: boolean;
  issues: ConfigValidationIssue[];
  onIssueClick: (path: string) => void;
}) {
  const validationSucceeded =
    current && reportIsValid(report) && issues.length === 0;
  if (validationSucceeded) {
    return (
      <Notice tone="success" title="Configuration validated">
        This draft passed file and effective validation with no issues.
      </Notice>
    );
  }
  if (!report) return null;
  const errors = issues.filter((issue) => issue.severity === 'error');
  const warnings = issues.filter((issue) => issue.severity !== 'error');
  return (
    <div
      ref={ref}
      tabIndex={-1}
      data-testid="config-validation-summary"
      className="outline-none focus-visible:ring-2 focus-visible:ring-[color:var(--color-accent)]/60"
    >
      <details
        open={errors.length > 0}
        className={cx(
          'rounded-sm border',
          errors.length
            ? 'border-red-500/35 bg-red-500/5'
            : 'border-subtle bg-panel-strong',
        )}
      >
        <summary className="flex cursor-pointer list-none items-center justify-between gap-3 px-3 py-2.5 text-sm focus-visible:outline focus-visible:outline-2 focus-visible:outline-[color:var(--color-accent)] focus-visible:outline-offset-2">
          <span className="inline-flex items-center gap-2">
            {errors.length ? (
              <FileWarning className="h-4 w-4 text-[color:var(--color-danger-text)]" />
            ) : (
              <FileCheck2 className="h-4 w-4 text-[color:var(--color-ok)]" />
            )}
            Validation summary
          </span>
          <span className="flex flex-wrap justify-end gap-1.5">
            {!current ? <Badge tone="warn">Stale</Badge> : null}
            <Badge tone={report.file.valid ? 'ok' : 'danger'}>
              File {report.file.valid ? 'valid' : 'invalid'}
            </Badge>
            <Badge tone={report.effective.valid ? 'ok' : 'danger'}>
              Effective {report.effective.valid ? 'valid' : 'invalid'}
            </Badge>
            {errors.length ? (
              <Badge tone="danger">{errors.length} errors</Badge>
            ) : null}
            {warnings.length ? (
              <Badge tone="warn">{warnings.length} warnings</Badge>
            ) : null}
          </span>
        </summary>
        {issues.length ? (
          <div className="space-y-1 border-t border-subtle px-3 py-2">
            {issues.map((issue, index) => (
              <button
                key={`${issue.path}-${issue.code}-${index}`}
                type="button"
                className="flex w-full min-w-0 items-start gap-2 rounded-sm px-2 py-1.5 text-left text-xs hover:bg-overlay-5 focus-visible:outline focus-visible:outline-2 focus-visible:outline-[color:var(--color-accent)]"
                onClick={() => issue.path && onIssueClick(issue.path)}
              >
                <Badge tone={validationTone(issue)}>{issue.severity}</Badge>
                <span className="min-w-0">
                  <span className="block break-all font-mono text-text">
                    {issue.path || 'configuration'} · {issue.code}
                  </span>
                  <span className="block text-text-muted">{issue.message}</span>
                </span>
              </button>
            ))}
          </div>
        ) : (
          <div className="border-t border-subtle px-3 py-2 text-xs text-text-muted">
            No validation issues.
          </div>
        )}
      </details>
    </div>
  );
}

/**
 * Full status badges shown in the CardHeader subtitle for the active
 * category. Counts come from the visible-leaf tally computed for each
 * category; the nav grid uses the compact CategoryStatusDots instead.
 */
function CategoryStatusBadges({ counts }: { counts: ConfigEditorCounts }) {
  if (!counts.modified && !counts.overrides && !counts.errors) return null;
  return (
    <span className="flex flex-wrap items-center gap-1">
      {counts.modified ? (
        <Badge tone="accent">{counts.modified} modified</Badge>
      ) : null}
      {counts.overrides ? (
        <Badge tone="neutral">{counts.overrides} overridden</Badge>
      ) : null}
      {counts.errors ? (
        <Badge tone="danger">{counts.errors} invalid</Badge>
      ) : null}
    </span>
  );
}

/**
 * Compact per-category status for the inline nav grid: one dot per non-empty
 * tally (modified / overridden / invalid) plus a screen-reader summary. The
 * row keeps a fixed height so badge changes never re-layout the grid.
 */
function CategoryStatusDots({ counts }: { counts: ConfigEditorCounts }) {
  const summary = [
    counts.modified ? `${counts.modified} modified` : null,
    counts.overrides ? `${counts.overrides} overridden` : null,
    counts.errors ? `${counts.errors} invalid` : null,
  ]
    .filter(Boolean)
    .join(', ');
  return (
    <span className="flex h-2 items-center gap-1">
      {counts.modified ? (
        <span
          aria-hidden="true"
          className="h-1.5 w-1.5 rounded-full bg-accent"
        />
      ) : null}
      {counts.overrides ? (
        <span
          aria-hidden="true"
          className="h-1.5 w-1.5 rounded-full bg-[color:var(--color-text-faint)]"
        />
      ) : null}
      {counts.errors ? (
        <span
          aria-hidden="true"
          className="h-1.5 w-1.5 rounded-full bg-[color:var(--color-danger)]"
        />
      ) : null}
      {summary ? <span className="sr-only">{summary}</span> : null}
    </span>
  );
}

/**
 * Flat category grid rendered once inside the inline nav — no drawer, sheet,
 * or per-viewport copies. This is navigation, not a tab widget: items carry
 * aria-current and no tablist/tab roles.
 */
function CategoryNavList({
  categories,
  activeCategoryId,
  onSelect,
}: {
  categories: ConfigEditorModel['categories'];
  activeCategoryId: string;
  onSelect: (id: string) => void;
}) {
  return (
    <ul className="grid grid-cols-2 gap-px bg-[color:var(--color-border)] sm:grid-cols-3 lg:grid-cols-4 xl:grid-cols-7">
      {categories.map((entry) => {
        const active = entry.id === activeCategoryId;
        const statusSummary = [
          entry.counts.modified ? `${entry.counts.modified} modified` : null,
          entry.counts.overrides
            ? `${entry.counts.overrides} overridden`
            : null,
          entry.counts.errors ? `${entry.counts.errors} invalid` : null,
        ]
          .filter(Boolean)
          .join(', ');
        return (
          <li
            key={entry.id}
            className="min-w-0 last:col-span-2 sm:last:col-span-3 lg:last:col-span-2 xl:last:col-span-1"
          >
            <button
              type="button"
              data-config-category={entry.id}
              aria-current={active ? 'page' : undefined}
              title={statusSummary || undefined}
              className={cx(
                'flex h-full min-h-[44px] w-full min-w-0 items-start border-t-2 px-3 py-2.5 text-left text-sm focus-visible:outline focus-visible:outline-2 focus-visible:-outline-offset-2 focus-visible:outline-[color:var(--color-accent)]',
                active
                  ? 'border-accent bg-bg-sub font-medium text-text'
                  : 'border-transparent bg-bg text-text-muted hover:bg-bg-sub hover:text-[color:var(--color-text)]',
              )}
              onClick={() => onSelect(entry.id)}
            >
              <span className="min-w-0 flex-1">
                <span className="line-clamp-2">{entry.label}</span>
                <span className="sr-only">{entry.description}</span>
                <span className="mt-1 block">
                  <CategoryStatusDots counts={entry.counts} />
                </span>
              </span>
            </button>
          </li>
        );
      })}
    </ul>
  );
}

/**
 * Compact header search: the input lives in the CardHeader action row and the
 * result list is an absolute overlay anchored to the wrapper, so opening or
 * closing it never changes the panel's document flow or height. Escape,
 * outside pointerdown, and focus leaving the wrapper close the overlay but
 * keep the query; the clear button resets both and returns focus to the input.
 */
function ConfigSearch({
  searchText,
  results,
  open,
  wrapperRef,
  onOpenChange,
  onSearchTextChange,
  onActivate,
}: {
  searchText: string;
  results: ConfigSearchResult[];
  open: boolean;
  wrapperRef: Ref<HTMLDivElement>;
  onOpenChange: (open: boolean) => void;
  onSearchTextChange: (text: string) => void;
  onActivate: (path: string) => void;
}) {
  const hasQuery = Boolean(searchText.trim());
  const showOverlay = open && hasQuery;
  const inputRef = useRef<HTMLInputElement | null>(null);
  // Programmatic focus returns (Escape, clear) must not reopen the overlay:
  // the next focus event after one is suppressed exactly once.
  const suppressNextFocusOpenRef = useRef(false);
  const returnFocusToInput = () => {
    const input = inputRef.current;
    if (!input) return;
    // Suppress only when focus actually moves — a no-op focus() emits no
    // focus event, so an unconsumed flag would swallow a later genuine focus.
    if (document.activeElement !== input)
      suppressNextFocusOpenRef.current = true;
    input.focus();
  };
  // Combobox active descendant: index into `results`, or null when no option
  // is active. Kept in state so ArrowUp/ArrowDown moves re-render the highlight.
  const [activeIndex, setActiveIndex] = useState<number | null>(null);
  const keyboardNavigationRef = useRef(false);
  const optionId = (path: string) =>
    `${CONFIG_SEARCH_RESULTS_ID}-option-${path}`;
  // Render-clamped index: a results update can shrink the list before the
  // reset effect below runs, so aria-activedescendant never sees a stale id.
  const activeOptionIndex =
    showOverlay && activeIndex !== null && activeIndex < results.length
      ? activeIndex
      : null;
  const activeOptionId =
    activeOptionIndex !== null
      ? optionId(results[activeOptionIndex].path)
      : null;
  // Close or any results change drops the active option — a new query must not
  // leave a highlight (or a stale descendant id) on a different result set.
  // Arrow-key/hover updates only touch activeIndex, which is not a dep here,
  // so they never trigger the reset.
  const prevResultsRef = useRef(results);
  useEffect(() => {
    const resultsChanged = prevResultsRef.current !== results;
    prevResultsRef.current = results;
    if (!showOverlay || resultsChanged) setActiveIndex(null);
  }, [showOverlay, results]);
  // Keep the active option visible only after keyboard navigation. Pointer hover
  // updates the highlight without changing the list's scroll position.
  // scrollIntoView is absent in jsdom, so guard the call itself.
  useEffect(() => {
    if (!keyboardNavigationRef.current) return;
    keyboardNavigationRef.current = false;
    if (activeOptionId === null) return;
    document
      .getElementById(activeOptionId)
      ?.scrollIntoView?.({ block: 'nearest' });
  }, [activeOptionId]);
  const moveActive = (delta: 1 | -1) => {
    if (!hasQuery) return;
    if (!open) onOpenChange(true);
    if (!results.length) return;
    keyboardNavigationRef.current = true;
    setActiveIndex((current) =>
      current === null
        ? delta === 1
          ? 0
          : results.length - 1
        : (current + delta + results.length) % results.length,
    );
  };
  const activateOption = (index: number) => {
    const result = results[index];
    if (!result) return;
    onOpenChange(false);
    onActivate(result.path);
  };
  return (
    <div
      ref={wrapperRef}
      className="relative w-full sm:w-64 lg:w-80"
      onKeyDown={(event) => {
        if (event.key === 'Escape') {
          event.preventDefault();
          onOpenChange(false);
          returnFocusToInput();
        }
      }}
      onBlurCapture={(event) => {
        // relatedTarget null means focus went nowhere — only pointer-induced
        // blurs produce it (Tab-out always has a real target, and outside
        // clicks already close via the document pointerdown listener). Keep
        // the overlay mounted so a pending option click can still land.
        if (event.relatedTarget === null) return;
        const next = event.relatedTarget;
        if (!(next instanceof Node && event.currentTarget.contains(next))) {
          onOpenChange(false);
        }
      }}
    >
      <Search
        aria-hidden="true"
        className="pointer-events-none absolute left-2.5 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-text-faint"
      />
      <input
        ref={inputRef}
        type="search"
        role="combobox"
        aria-autocomplete="list"
        aria-haspopup="listbox"
        aria-expanded={showOverlay}
        aria-controls={showOverlay ? CONFIG_SEARCH_RESULTS_ID : undefined}
        aria-activedescendant={activeOptionId ?? undefined}
        aria-label="Search settings"
        data-testid="config-search"
        className={cx(
          INPUT_CLASS,
          '!h-8 pl-8 pr-9 [&::-webkit-search-cancel-button]:appearance-none',
        )}
        placeholder="Search settings"
        value={searchText}
        onChange={(event) => {
          onSearchTextChange(event.target.value);
          onOpenChange(Boolean(event.target.value.trim()));
        }}
        onFocus={() => {
          if (suppressNextFocusOpenRef.current) {
            suppressNextFocusOpenRef.current = false;
            return;
          }
          if (hasQuery) onOpenChange(true);
        }}
        onKeyDown={(event) => {
          switch (event.key) {
            case 'ArrowDown':
              event.preventDefault();
              moveActive(1);
              break;
            case 'ArrowUp':
              event.preventDefault();
              moveActive(-1);
              break;
            case 'Enter':
              if (activeOptionIndex !== null) {
                event.preventDefault();
                activateOption(activeOptionIndex);
              }
              break;
          }
        }}
      />
      {hasQuery ? (
        <button
          type="button"
          aria-label="Clear search"
          className="absolute right-0.5 top-1/2 flex h-8 w-8 -translate-y-1/2 items-center justify-center rounded-sm text-text-faint hover:bg-overlay-5 hover:text-[color:var(--color-text)] focus-visible:outline focus-visible:outline-2 focus-visible:outline-[color:var(--color-accent)]"
          onClick={() => {
            onSearchTextChange('');
            onOpenChange(false);
            requestAnimationFrame(returnFocusToInput);
          }}
        >
          <X className="h-3.5 w-3.5" />
        </button>
      ) : null}
      {showOverlay ? (
        <div
          role="status"
          className="sr-only"
          data-testid="config-search-status"
        >
          {results.length
            ? `${results.length} ${results.length === 1 ? 'setting matches' : 'settings match'} this search.`
            : 'No settings match this search.'}
        </div>
      ) : null}
      {showOverlay ? (
        <div
          id={CONFIG_SEARCH_RESULTS_ID}
          role="listbox"
          aria-label="Search results"
          data-testid="config-search-results"
          className="glass-strong absolute left-0 right-0 top-full z-50 mt-1 max-h-72 overflow-y-auto rounded-sm shadow-2xl"
        >
          {results.length ? (
            results.map((result, index) => (
              <button
                key={result.path}
                id={optionId(result.path)}
                type="button"
                role="option"
                aria-selected={index === activeOptionIndex}
                tabIndex={-1}
                className={cx(
                  'flex min-h-[44px] w-full min-w-0 items-center justify-between gap-3 px-3 py-2 text-left hover:bg-overlay-5 focus-visible:outline focus-visible:outline-2 focus-visible:outline-[color:var(--color-accent)]',
                  index === activeOptionIndex && 'bg-overlay-5',
                )}
                // Keep focus on the input: preventing the default mousedown
                // focus shift means the option is still mounted when the
                // click arrives, so pointer selection never races a
                // focusout-driven unmount.
                onMouseDown={(event) => event.preventDefault()}
                onMouseMove={() => {
                  if (index !== activeIndex) setActiveIndex(index);
                }}
                onClick={() => activateOption(index)}
              >
                <span className="min-w-0">
                  <span className="block truncate text-sm text-text">
                    {result.label}
                  </span>
                  <span className="block truncate text-[10px] text-text-faint">
                    {result.breadcrumb} ·{' '}
                    <span className="font-mono">{result.path}</span>
                  </span>
                </span>
                {result.leaf.advanced ? (
                  <Badge tone="neutral">Advanced</Badge>
                ) : null}
              </button>
            ))
          ) : (
            <div
              role="option"
              aria-selected={false}
              aria-disabled="true"
              className="px-3 py-2 text-xs text-text-faint"
              onMouseDown={(event) => event.preventDefault()}
            >
              No settings match this search.
            </div>
          )}
        </div>
      ) : null}
    </div>
  );
}

function ConfigEditorSkeleton() {
  return (
    <div
      data-testid="config-editor-skeleton"
      className="min-h-[560px] space-y-3"
      aria-hidden="true"
    >
      <Skeleton className="h-9 w-full" />
      <div className="grid grid-cols-2 gap-px bg-[color:var(--color-border)] sm:grid-cols-3 lg:grid-cols-4 xl:grid-cols-7">
        {CONFIG_EDITOR_CATEGORIES.map((category) => (
          <Skeleton
            key={category.id}
            className="h-14 w-full last:col-span-2 sm:last:col-span-3 lg:last:col-span-2 xl:last:col-span-1"
          />
        ))}
      </div>
      <div className="space-y-6">
        <div className="space-y-1.5">
          <Skeleton className="h-5 w-40" />
          <Skeleton className="h-3 w-64" />
        </div>
        {[0, 1, 2].map((index) => (
          <div
            key={index}
            className={index === 0 ? undefined : 'border-t border-subtle pt-5'}
          >
            <Skeleton className={cx('h-4', index % 2 ? 'w-36' : 'w-44')} />
            <div className="mt-4 grid grid-cols-1 gap-3 md:grid-cols-2 2xl:grid-cols-3">
              <Skeleton className="h-16 w-full" />
              <Skeleton className="h-16 w-full" />
              <Skeleton className="hidden h-16 w-full 2xl:block" />
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

function CategoryPanel({
  category,
  model,
  rootSchema,
  value,
  defaultConfig,
  effectiveConfig,
  overrides,
  issues,
  openAdvanced,
  onToggleAdvanced,
  onChange,
}: {
  category: ConfigEditorModel['categories'][number];
  model: ConfigEditorModel;
  rootSchema: ConfigSchema;
  value: JsonObject;
  defaultConfig: JsonObject;
  effectiveConfig: JsonObject;
  overrides: ConfigOverrideInfo[];
  issues: ConfigValidationIssue[];
  openAdvanced: Record<string, boolean>;
  onToggleAdvanced: (sectionId: string, open: boolean) => void;
  onChange: (value: unknown) => void;
}) {
  const unassignedLeaves = model.unassigned.leaves.filter(
    (leaf) =>
      isLeafVisible(leaf, model, rootSchema) && leaf.categoryId === category.id,
  );
  const hasUnknownKeys = unassignedLeaves.some((leaf) => leaf.unknown);
  const hasSchemaKnown = unassignedLeaves.some((leaf) => !leaf.unknown);
  return (
    <section
      className="space-y-6"
      data-config-category-panel={category.id}
      aria-labelledby={CONFIG_CATEGORY_HEADING_ID}
    >
      {category.sections.map((section) => (
        <SectionCard
          key={section.id}
          section={section}
          model={model}
          rootSchema={rootSchema}
          value={value}
          defaultConfig={defaultConfig}
          effectiveConfig={effectiveConfig}
          overrides={overrides}
          issues={issues}
          open={Boolean(openAdvanced[section.id])}
          onToggleAdvanced={(open) => onToggleAdvanced(section.id, open)}
          onChange={onChange}
        />
      ))}
      {unassignedLeaves.length ? (
        <section
          data-testid="config-section-card"
          data-config-section={CONFIG_EDITOR_UNASSIGNED_SECTION_ID}
          className="border-t border-subtle pt-5 first:border-t-0 first:pt-0"
        >
          <div className="flex flex-wrap items-center gap-2">
            <h4 className="text-sm font-medium text-text">
              {CONFIG_EDITOR_UNASSIGNED_LABEL}
            </h4>
            <Badge tone="warn">
              {hasUnknownKeys ? 'Unknown keys' : 'Unassigned'}
            </Badge>
          </div>
          {hasUnknownKeys ? (
            <p className="mt-0.5 text-xs text-text-faint">
              These file keys are not recognized by the schema. They are
              preserved for review — remove or correct them before validation.
            </p>
          ) : null}
          {hasSchemaKnown ? (
            <p className="mt-0.5 text-xs text-text-faint">
              These settings are recognized by the schema but not covered by a
              settings section.
            </p>
          ) : null}
          <div className="mt-3 grid grid-cols-1 gap-3 md:grid-cols-2 2xl:grid-cols-3">
            {unassignedLeaves.map((leaf) => (
              <ScalarField
                key={leaf.pathString}
                rootSchema={rootSchema}
                schema={leaf.schema}
                nullable={leaf.nullable}
                path={leaf.pathString}
                value={value}
                defaultValue={leaf.defaultValue}
                effectiveValue={leaf.effectiveValue}
                override={leaf.override ?? undefined}
                issues={leaf.issues}
                onChange={onChange}
              />
            ))}
          </div>
        </section>
      ) : null}
    </section>
  );
}

function SectionCard({
  section,
  model,
  rootSchema,
  value,
  defaultConfig,
  effectiveConfig,
  overrides,
  issues,
  open,
  onToggleAdvanced,
  onChange,
}: {
  section: ConfigEditorSectionModel;
  model: ConfigEditorModel;
  rootSchema: ConfigSchema;
  value: JsonObject;
  defaultConfig: JsonObject;
  effectiveConfig: JsonObject;
  overrides: ConfigOverrideInfo[];
  issues: ConfigValidationIssue[];
  open: boolean;
  onToggleAdvanced: (open: boolean) => void;
  onChange: (value: unknown) => void;
}) {
  const primaryRoots = sectionRenderRoots(section, false, model, rootSchema);
  const advancedRoots = sectionRenderRoots(section, true, model, rootSchema);
  // A lone primary root repeats the section h4 — suppress its own heading so
  // the section owns the label. Multi-root sections keep per-root headings.
  const hideRootHeading =
    primaryRoots.length === 1 && advancedRoots.length === 0;
  // A lone nullable root (e.g. listener.tls) hoists its enable switch into the
  // section header row so the h4/description and the switch share one line.
  const singleNullableRoot = hideRootHeading ? (primaryRoots[0] ?? null) : null;
  const singleNullablePath =
    singleNullableRoot &&
    compoundKindAt(
      rootSchema,
      singleNullableRoot.schema,
      singleNullableRoot.path,
    ) === 'nullable-object'
      ? singleNullableRoot.path
      : null;
  const renderRoot = (root: RenderRoot) => (
    <ConfigNode
      key={root.path}
      rootSchema={rootSchema}
      schema={root.schema}
      path={root.path}
      value={value}
      defaultConfig={defaultConfig}
      effectiveConfig={effectiveConfig}
      overrides={overrides}
      issues={issues}
      onChange={onChange}
      depth={0}
      childKeys={root.childKeys}
      hideHeading={hideRootHeading}
      suppressNullableToggle={root.path === singleNullablePath}
    />
  );
  return (
    <section
      data-testid="config-section-card"
      data-config-section={section.id}
      className="border-t border-subtle pt-5 first:border-t-0 first:pt-0"
    >
      <div className="flex flex-wrap items-center justify-between gap-x-3 gap-y-2">
        <div className="min-w-0">
          <h4 className="text-sm font-medium text-text">{section.label}</h4>
          {section.description ? (
            <p className="mt-0.5 text-xs text-text-faint">
              {section.description}
            </p>
          ) : null}
        </div>
        {singleNullablePath ? (
          <ToggleSwitch
            variant="compact"
            className="shrink-0"
            data-field-control
            data-config-path={singleNullablePath}
            checked={isJsonObject(getConfigValue(value, singleNullablePath))}
            label="Enabled"
            onChange={(event) => {
              onChange(
                event.target.checked
                  ? (setConfigValue(
                      value,
                      singleNullablePath,
                      {},
                    ) as JsonObject)
                  : (unsetConfigValue(value, singleNullablePath) as JsonObject),
              );
            }}
          />
        ) : null}
      </div>
      {primaryRoots.length ? (
        <div className="mt-3 grid grid-cols-1 gap-3 md:grid-cols-2 2xl:grid-cols-3">
          {primaryRoots.map(renderRoot)}
        </div>
      ) : null}
      {advancedRoots.length ? (
        <details
          data-testid="config-advanced"
          open={open}
          onToggle={(event) => onToggleAdvanced(event.currentTarget.open)}
          className="mt-3 rounded-sm border border-subtle bg-panel-strong"
        >
          <summary className="flex min-h-[44px] cursor-pointer list-none items-center gap-2 px-3 py-2 text-xs text-text-muted focus-visible:outline focus-visible:outline-2 focus-visible:outline-[color:var(--color-accent)]">
            <ChevronDown className="h-3.5 w-3.5" />
            Advanced ({advancedRoots.length})
          </summary>
          <div className="grid grid-cols-1 gap-3 border-t border-subtle px-3 py-3 md:grid-cols-2 2xl:grid-cols-3">
            {advancedRoots.map(renderRoot)}
          </div>
        </details>
      ) : null}
      {!primaryRoots.length && !advancedRoots.length ? (
        <p className="mt-3 text-xs text-text-faint">
          These settings only apply to a different configuration variant.
        </p>
      ) : null}
    </section>
  );
}

function ConfigNode({
  rootSchema,
  schema: inputSchema,
  path,
  value,
  defaultConfig,
  effectiveConfig,
  overrides,
  issues,
  onChange,
  depth,
  childKeys,
  hideHeading = false,
  suppressNullableToggle = false,
  embedded = false,
}: {
  rootSchema: ConfigSchema;
  schema: ConfigSchema;
  path: string;
  value: JsonObject;
  defaultConfig: JsonObject;
  effectiveConfig: JsonObject;
  overrides: ConfigOverrideInfo[];
  issues: ConfigValidationIssue[];
  onChange: (value: unknown) => void;
  depth: number;
  childKeys?: Set<string>;
  /** Suppress the root h5/description when the section h4 already names it. */
  hideHeading?: boolean;
  /** Drop the in-root nullable switch when the section header owns it. */
  suppressNullableToggle?: boolean;
  /** Render scalar leaves without their card chrome (inside a provider card). */
  embedded?: boolean;
}) {
  const resolvedSchema = resolveConfigSchema(rootSchema, inputSchema);
  const nullable = isNullableConfigSchema(rootSchema, inputSchema);
  const variants = taggedVariants(rootSchema, resolvedSchema);
  const schema =
    variants.length > 0
      ? resolvedSchema
      : (getConfigSchemaVariants(rootSchema, resolvedSchema)[0]?.schema ??
        resolvedSchema);
  const properties = objectProperties(rootSchema, schema);
  const currentValue = getConfigValue(value, path);
  const defaultValue = getConfigValue(defaultConfig, path);
  const effectiveValue = getConfigValue(effectiveConfig, path);

  if (path === ADMIN_PROVIDERS_PATH) {
    return (
      <AdminProvidersEditor
        rootSchema={rootSchema}
        schema={schema}
        path={path}
        value={value}
        defaultConfig={defaultConfig}
        effectiveConfig={effectiveConfig}
        overrides={overrides}
        issues={issues}
        onChange={onChange}
        hideHeading={hideHeading}
      />
    );
  }
  if (path === RECURRING_JOBS_PATH) {
    return (
      <RecurringJobsEditor
        rootSchema={rootSchema}
        schema={schema}
        path={path}
        value={value}
        defaultConfig={defaultConfig}
        effectiveConfig={effectiveConfig}
        overrides={overrides}
        issues={issues}
        onChange={onChange}
      />
    );
  }
  if (variants.length) {
    return (
      <TaggedUnionEditor
        rootSchema={rootSchema}
        schema={schema}
        path={path}
        value={value}
        defaultConfig={defaultConfig}
        effectiveConfig={effectiveConfig}
        overrides={overrides}
        issues={issues}
        onChange={onChange}
        depth={depth}
        childKeys={childKeys}
        hideHeading={hideHeading}
      />
    );
  }
  if (Object.keys(properties).length) {
    const configured = isJsonObject(currentValue);
    const showChildren = !nullable || configured;
    const showNullableToggle = nullable && !suppressNullableToggle;
    return (
      <div
        className={cx(
          'col-span-full space-y-3',
          depth === 0
            ? ''
            : 'rounded-sm border border-subtle bg-panel-strong p-3',
        )}
        data-config-path={path}
        tabIndex={-1}
      >
        {!hideHeading || showNullableToggle ? (
          <div
            className={cx(
              'flex min-w-0 flex-wrap items-center gap-x-3 gap-y-2',
              hideHeading ? 'justify-end' : 'justify-between',
            )}
          >
            {!hideHeading ? (
              <div className="min-w-0 flex-1">
                <h5
                  className={cx(
                    'font-medium text-text',
                    depth === 0 ? 'text-sm' : 'text-xs',
                  )}
                >
                  {titleForKey(path.split('.').at(-1) ?? path)}
                </h5>
                {typeof schema.description === 'string' ? (
                  <p className="mt-0.5 text-xs leading-relaxed text-text-faint">
                    {schema.description}
                  </p>
                ) : null}
              </div>
            ) : null}
            {showNullableToggle ? (
              <ToggleSwitch
                variant="compact"
                className="shrink-0"
                data-field-control
                checked={configured}
                label="Enabled"
                onChange={(event) => {
                  onChange(
                    event.target.checked
                      ? (setConfigValue(value, path, {}) as JsonObject)
                      : (unsetConfigValue(value, path) as JsonObject),
                  );
                }}
              />
            ) : null}
          </div>
        ) : null}
        {showChildren ? (
          <div className="grid grid-cols-1 gap-3 md:grid-cols-2 2xl:grid-cols-3">
            {Object.entries(properties).map(([key, childSchema]) =>
              childKeys && !childKeys.has(key) ? null : (
                <ConfigNode
                  key={key}
                  rootSchema={rootSchema}
                  schema={childSchema}
                  path={`${path}.${key}`}
                  value={value}
                  defaultConfig={defaultConfig}
                  effectiveConfig={effectiveConfig}
                  overrides={overrides}
                  issues={issues}
                  onChange={onChange}
                  depth={depth + 1}
                  embedded={embedded}
                />
              ),
            )}
          </div>
        ) : null}
      </div>
    );
  }

  return (
    <ScalarField
      rootSchema={rootSchema}
      schema={resolvedSchema}
      nullable={nullable}
      path={path}
      value={value}
      defaultValue={defaultValue}
      effectiveValue={effectiveValue}
      override={overrideForPath(overrides, path)}
      issues={issues.filter(
        (issue) => issue.path === path || issue.path.startsWith(`${path}.`),
      )}
      onChange={onChange}
      embedded={embedded}
    />
  );
}

/**
 * Compact trade-off block for a scalar field: what moving a numeric value in
 * either direction does, or what toggling a boolean does, plus any
 * operational recommendation. Rendered as a dl so the labels stay visible.
 */
function GuidanceTradeoffs({ guidance }: { guidance: ConfigFieldGuidance }) {
  const rows: { label: string; text: string }[] = [];
  if (guidance.lower) rows.push({ label: 'Lower', text: guidance.lower });
  if (guidance.higher) rows.push({ label: 'Higher', text: guidance.higher });
  if (guidance.enabled) rows.push({ label: 'On', text: guidance.enabled });
  if (guidance.disabled) rows.push({ label: 'Off', text: guidance.disabled });
  if (guidance.recommendation) {
    rows.push({ label: 'Recommendation', text: guidance.recommendation });
  }
  if (!rows.length) return null;
  return (
    <dl className="mt-2 space-y-1">
      {rows.map((row) => (
        <div
          key={row.label}
          className="flex gap-1.5 text-[10px] leading-relaxed"
        >
          <dt className="shrink-0 font-medium uppercase tracking-wide text-text-muted">
            {row.label}
          </dt>
          <dd className="min-w-0 text-text-faint">{row.text}</dd>
        </div>
      ))}
    </dl>
  );
}

function ScalarField({
  rootSchema,
  schema,
  nullable,
  path,
  value,
  defaultValue,
  effectiveValue,
  override,
  issues,
  onChange,
  embedded = false,
  suppressActions = false,
}: {
  rootSchema: ConfigSchema;
  schema: ConfigSchema;
  nullable: boolean;
  path: string;
  value: JsonObject;
  defaultValue: unknown;
  effectiveValue: unknown;
  override?: ConfigOverrideInfo;
  issues: ConfigValidationIssue[];
  onChange: (value: unknown) => void;
  /** Render without the outer card chrome for use inside a composite row. */
  embedded?: boolean;
  /** Hide the Reset/Unset actions when a parent row header owns them. */
  suppressActions?: boolean;
}) {
  const { fileConfig } = useContext(ConfigSourcesContext);
  const storageUrlReplacement = useContext(StorageUrlReplacementContext);
  const [replacingOpaque, setReplacingOpaque] = useState(false);
  const [clearedWhileEditing, setClearedWhileEditing] = useState(false);
  const current = getConfigValue(value, path);
  const fileValue = getConfigValue(fileConfig, path);
  const configured = current !== undefined && current !== null;
  const modified = configured && !isSameJson(current, fileValue);
  const error = issues.find((issue) => issue.severity === 'error');
  const isSensitive =
    Boolean(override?.sensitive) || path === OPAQUE_STORAGE_URL_PATH;
  const label = titleForKey(path.split('.').at(-1) ?? path);
  const resolvedInput = resolveConfigSchema(rootSchema, schema);
  const schemaVariants = getConfigSchemaVariants(rootSchema, resolvedInput);
  // Union-merged leaf schemas put the inferred branch first (e.g. a field
  // declared with bounds in one tagged variant but inferred as a bare number
  // in another) — prefer the variant that actually declares numeric bounds so
  // the range hint and input type stay accurate.
  const concrete =
    schemaVariants.find(
      (variant) =>
        typeof variant.schema.minimum === 'number' ||
        typeof variant.schema.maximum === 'number',
    )?.schema ??
    schemaVariants[0]?.schema ??
    resolvedInput;
  const description =
    typeof concrete.description === 'string'
      ? concrete.description
      : typeof resolvedInput.description === 'string'
        ? resolvedInput.description
        : undefined;
  const schemaTypes = (
    Array.isArray(concrete.type) ? concrete.type : [concrete.type]
  ).filter(
    (type): type is string => typeof type === 'string' && type !== 'null',
  );
  const numeric =
    schemaTypes.includes('integer') || schemaTypes.includes('number');
  const boolean = schemaTypes.includes('boolean');
  const enumValues = [
    ...new Set(
      [concrete, ...schemaVariants.map((variant) => variant.schema)].flatMap(
        (candidate) => [
          ...(Array.isArray(candidate.enum) ? candidate.enum : []),
          ...(typeof candidate.const === 'string' ||
          typeof candidate.const === 'number'
            ? [candidate.const]
            : []),
        ],
      ),
    ),
  ].filter((item): item is string | number =>
    ['string', 'number'].includes(typeof item),
  );
  const arrayItems = isJsonObject(concrete.items)
    ? resolveConfigSchema(rootSchema, concrete.items)
    : {};
  const isStringArray =
    schemaTypes.includes('array') && arrayItems.type === 'string';
  const leafKind: ConfigEditorLeafKind = boolean
    ? 'boolean'
    : enumValues.length
      ? 'enum'
      : schemaTypes.includes('integer')
        ? 'integer'
        : numeric
          ? 'number'
          : isStringArray
            ? 'array'
            : schemaTypes.includes('string')
              ? 'string'
              : 'unknown';
  const classification = classifyConfigLeaf(path, leafKind);
  const guidance = resolveConfigFieldGuidance(path, description, leafKind);
  const inputId = `config-${path.replaceAll('.', '-')}`;
  const humanized =
    classification.presentation === 'duration' ||
    classification.presentation === 'bytes'
      ? humanizeConfigValue(
          configured ? current : effectiveValue,
          classification.unit,
        )
      : null;
  const minimum =
    typeof concrete.minimum === 'number' ? concrete.minimum : undefined;
  const maximum =
    typeof concrete.maximum === 'number' ? concrete.maximum : undefined;

  useEffect(() => {
    if (storageUrlReplacement.value === null) {
      setReplacingOpaque(false);
    }
  }, [storageUrlReplacement.value]);

  const reset = () => {
    if (defaultValue !== undefined) {
      onChange(setConfigValue(value, path, cloneJson(defaultValue)));
    } else {
      onChange(unsetConfigValue(value, path));
    }
  };

  return (
    <div
      data-config-path={path}
      data-field-embedded={embedded ? '' : undefined}
      tabIndex={-1}
      className={cx(
        'min-w-0 outline-none focus:ring-2 focus:ring-[color:var(--color-accent)]/60',
        isStringArray ? 'col-span-full' : undefined,
        embedded
          ? 'rounded-sm'
          : cx(
              'rounded-sm border bg-panel-strong p-3',
              error ? 'border-red-500/45' : 'border-subtle',
            ),
      )}
    >
      <div className="mb-2 flex min-w-0 flex-wrap items-start justify-between gap-2">
        <div className="flex min-w-0 flex-wrap items-center gap-x-1.5 gap-y-1">
          <label
            htmlFor={inputId}
            className="min-w-0 text-xs font-medium text-text"
          >
            {label}
          </label>
          {modified ? <Badge tone="accent">Modified</Badge> : null}
          {classification.dangerous ? (
            <Badge tone="warn">Operational risk</Badge>
          ) : null}
        </div>
        {suppressActions ? null : (
          <div className="flex shrink-0 flex-wrap justify-end gap-1">
            {path === OPAQUE_STORAGE_URL_PATH ? (
              <>
                {replacingOpaque || storageUrlReplacement.value !== null ? (
                  <Button
                    size="sm"
                    variant="ghost"
                    iconLeft={<X className="h-3 w-3" />}
                    onClick={() => {
                      storageUrlReplacement.onChange(null);
                      setReplacingOpaque(false);
                    }}
                  >
                    Cancel replacement
                  </Button>
                ) : null}
                {configured ? (
                  <Button
                    size="sm"
                    variant="ghost"
                    iconLeft={<X className="h-3 w-3" />}
                    onClick={() => {
                      storageUrlReplacement.onChange(null);
                      setReplacingOpaque(false);
                      onChange(unsetConfigValue(value, path));
                    }}
                  >
                    Unset
                  </Button>
                ) : null}
              </>
            ) : (
              <>
                <Button
                  size="sm"
                  variant="ghost"
                  iconLeft={<RotateCcw className="h-3 w-3" />}
                  onClick={reset}
                >
                  Reset
                </Button>
                <Button
                  size="sm"
                  variant="ghost"
                  iconLeft={<X className="h-3 w-3" />}
                  onClick={() => onChange(unsetConfigValue(value, path))}
                >
                  Unset
                </Button>
              </>
            )}
          </div>
        )}
      </div>
      <p className="mb-2 text-xs leading-relaxed text-text-faint">
        {guidance.description}
      </p>
      {nullable && !configured && !(numeric && clearedWhileEditing) ? (
        <Button
          size="sm"
          iconLeft={<Plus className="h-3 w-3" />}
          onClick={() => {
            const initial =
              defaultValue !== undefined && defaultValue !== null
                ? cloneJson(defaultValue)
                : effectiveValue !== undefined && effectiveValue !== null
                  ? cloneJson(effectiveValue)
                  : boolean
                    ? false
                    : numeric
                      ? 0
                      : schemaTypes.includes('array')
                        ? []
                        : '';
            onChange(setConfigValue(value, path, initial));
          }}
        >
          Set value
        </Button>
      ) : path === OPAQUE_STORAGE_URL_PATH &&
        storageUrlReplacement.value === null &&
        !replacingOpaque ? (
        <div className="flex flex-wrap items-center gap-2">
          <Badge tone="mono">
            {configured
              ? 'Stored value hidden'
              : override
                ? `Supplied by ${override.name}; not stored in file`
                : 'No stored URL'}
          </Badge>
          <Button size="sm" onClick={() => setReplacingOpaque(true)}>
            Replace URL
          </Button>
        </div>
      ) : isStringArray ? (
        <StringArrayControl
          id={inputId}
          values={
            Array.isArray(current)
              ? current.filter(
                  (item): item is string => typeof item === 'string',
                )
              : []
          }
          error={error?.message}
          onChange={(next) => onChange(setConfigValue(value, path, next))}
        />
      ) : boolean ? (
        <ToggleSwitch
          id={inputId}
          data-field-control
          checked={configured ? current === true : effectiveValue === true}
          label={
            (configured ? current : effectiveValue) === true
              ? 'Enabled'
              : 'Disabled'
          }
          onChange={(event) =>
            onChange(setConfigValue(value, path, event.target.checked))
          }
        />
      ) : enumValues.length ? (
        <select
          id={inputId}
          data-field-control
          className={INPUT_WITH_ERROR_CLASS}
          aria-invalid={Boolean(error)}
          value={
            typeof current === 'string' || typeof current === 'number'
              ? String(current)
              : ''
          }
          onChange={(event) => {
            const raw = event.target.value;
            onChange(setConfigValue(value, path, numeric ? Number(raw) : raw));
          }}
        >
          {!configured ? <option value="">Inherited / not set</option> : null}
          {enumValues.map((option) => (
            <option key={String(option)} value={String(option)}>
              {titleForKey(String(option))}
            </option>
          ))}
        </select>
      ) : (
        <div>
          <div className="flex items-center gap-2">
            <input
              id={inputId}
              data-field-control
              className={INPUT_WITH_ERROR_CLASS}
              type={
                path === OPAQUE_STORAGE_URL_PATH
                  ? 'password'
                  : numeric
                    ? 'number'
                    : 'text'
              }
              step={schemaTypes.includes('integer') ? 1 : undefined}
              min={minimum}
              max={maximum}
              inputMode={
                classification.presentation === 'count' || numeric
                  ? 'numeric'
                  : undefined
              }
              aria-invalid={Boolean(error)}
              value={
                path === OPAQUE_STORAGE_URL_PATH
                  ? (storageUrlReplacement.value ?? '')
                  : typeof current === 'string' || typeof current === 'number'
                    ? String(current)
                    : ''
              }
              placeholder={
                path === OPAQUE_STORAGE_URL_PATH
                  ? 'Enter a replacement URL'
                  : `Inherited: ${displayValue(effectiveValue, isSensitive)}`
              }
              onChange={(event) => {
                if (path === OPAQUE_STORAGE_URL_PATH) {
                  const replacement = event.target.value;
                  storageUrlReplacement.onChange(replacement || null);
                  if (!replacement) setReplacingOpaque(false);
                  return;
                }
                if (numeric && event.target.value === '') {
                  setClearedWhileEditing(true);
                  onChange(unsetConfigValue(value, path));
                  return;
                }
                if (numeric) setClearedWhileEditing(false);
                const next = numeric
                  ? Number(event.target.value)
                  : event.target.value;
                onChange(setConfigValue(value, path, next));
              }}
              onBlur={() => {
                if (numeric) setClearedWhileEditing(false);
              }}
            />
            {classification.unit ? (
              <span className="shrink-0 text-xs text-text-faint">
                {unitSuffix(classification.unit)}
              </span>
            ) : null}
          </div>
          {humanized ? (
            <div className="mt-1 text-[10px] text-text-faint">
              ≈ {humanized}
            </div>
          ) : null}
          {numeric && (minimum !== undefined || maximum !== undefined) ? (
            <div className="mt-1 text-[10px] text-text-faint">
              Range: {minimum ?? 'no min'} – {maximum ?? 'no max'}
            </div>
          ) : null}
          {classification.presentation === 'env' ? (
            <div className="mt-1 text-[10px] text-text-faint">
              Environment variable name — the secret value itself is never
              stored or shown here.
            </div>
          ) : classification.presentation === 'address' ? (
            <div className="mt-1 text-[10px] text-text-faint">
              Listen address in host:port form.
            </div>
          ) : classification.presentation === 'url' ? (
            <div className="mt-1 text-[10px] text-text-faint">
              Connection or endpoint URL.
            </div>
          ) : classification.presentation === 'path' ? (
            <div className="mt-1 text-[10px] text-text-faint">
              Filesystem path on the cc-lb host.
            </div>
          ) : null}
        </div>
      )}

      <GuidanceTradeoffs guidance={guidance} />

      <div className="mt-2 flex min-w-0 flex-wrap gap-x-3 gap-y-1 text-[10px] text-text-faint">
        <span>Draft: {displayValue(current, isSensitive)}</span>
        <span>Effective: {displayValue(effectiveValue, isSensitive)}</span>
        {!configured ? <Badge tone="neutral">Inherited</Badge> : null}
      </div>
      <details data-testid="config-value-details" className="group mt-1.5">
        <summary className="inline-flex min-h-[44px] w-fit cursor-pointer list-none items-center gap-1 text-[10px] text-text-faint focus-visible:outline focus-visible:outline-2 focus-visible:outline-[color:var(--color-accent)]">
          <ChevronRight className="h-3 w-3 transition-transform group-open:rotate-90" />
          Value details
        </summary>
        <div className="mt-1 space-y-0.5 text-[10px] text-text-faint">
          <div>File: {displayValue(fileValue, isSensitive)}</div>
          <div>Default: {displayValue(defaultValue, isSensitive)}</div>
          <div>
            Source:{' '}
            {override
              ? `${sourceLabel(override)} · ${override.name}`
              : 'Config file / defaults'}
          </div>
        </div>
      </details>
      {override ? (
        <div className="mt-1.5 text-[10px] leading-relaxed text-text-muted">
          Effective value comes from {sourceLabel(override)} · {override.name}.
          The file value applies only if the override is removed.
        </div>
      ) : null}
      {classification.dangerous && classification.dangerImpact ? (
        <div className="mt-1.5 text-[10px] leading-relaxed text-[color:var(--color-warn-text)]">
          {classification.dangerImpact}
        </div>
      ) : null}
      {issues.map((issue, index) => (
        <div
          key={`${issue.code}-${index}`}
          className={cx(
            'mt-1.5 text-[11px]',
            issue.severity === 'error'
              ? 'text-[color:var(--color-danger-text)]'
              : 'text-[color:var(--color-warn-text)]',
          )}
        >
          {issue.message}
        </div>
      ))}
    </div>
  );
}

function StringArrayControl({
  id,
  values,
  error,
  onChange,
}: {
  id: string;
  values: string[];
  error?: string;
  onChange: (values: string[]) => void;
}) {
  return (
    <div className="space-y-2">
      {values.map((item, index) => (
        <div key={`${id}-${index}`} className="flex min-w-0 gap-1.5">
          <input
            id={index === 0 ? id : undefined}
            data-field-control={index === 0 ? true : undefined}
            className={INPUT_WITH_ERROR_CLASS}
            aria-invalid={Boolean(error)}
            value={item}
            onChange={(event) => {
              const next = [...values];
              next[index] = event.target.value;
              onChange(next);
            }}
          />
          <Button
            size="sm"
            variant="ghost"
            aria-label={`Move item ${index + 1} up`}
            disabled={index === 0}
            onClick={() => {
              const next = [...values];
              [next[index - 1], next[index]] = [next[index], next[index - 1]];
              onChange(next);
            }}
          >
            <ChevronUp className="h-3.5 w-3.5" />
          </Button>
          <Button
            size="sm"
            variant="ghost"
            aria-label={`Move item ${index + 1} down`}
            disabled={index === values.length - 1}
            onClick={() => {
              const next = [...values];
              [next[index], next[index + 1]] = [next[index + 1], next[index]];
              onChange(next);
            }}
          >
            <ChevronDown className="h-3.5 w-3.5" />
          </Button>
          <Button
            size="sm"
            variant="ghost"
            aria-label={`Remove item ${index + 1}`}
            onClick={() =>
              onChange(values.filter((_, itemIndex) => itemIndex !== index))
            }
          >
            <Trash2 className="h-3.5 w-3.5" />
          </Button>
        </div>
      ))}
      <Button
        size="sm"
        iconLeft={<Plus className="h-3 w-3" />}
        onClick={() => onChange([...values, ''])}
      >
        Add item
      </Button>
    </div>
  );
}

function TaggedUnionEditor({
  rootSchema,
  schema,
  path,
  value,
  defaultConfig,
  effectiveConfig,
  overrides,
  issues,
  onChange,
  depth,
  childKeys,
  hideHeading = false,
}: {
  rootSchema: ConfigSchema;
  schema: ConfigSchema;
  path: string;
  value: JsonObject;
  defaultConfig: JsonObject;
  effectiveConfig: JsonObject;
  overrides: ConfigOverrideInfo[];
  issues: ConfigValidationIssue[];
  onChange: (value: unknown) => void;
  depth: number;
  childKeys?: Set<string>;
  /** Suppress the root h5/description when the section h4 already names it. */
  hideHeading?: boolean;
}) {
  const storageUrlReplacement = useContext(StorageUrlReplacementContext);
  const variants = taggedVariants(rootSchema, schema);
  const current = getConfigValue(value, path);
  const effective = getConfigValue(effectiveConfig, path);
  const currentKind =
    isJsonObject(current) && typeof current.kind === 'string'
      ? current.kind
      : isJsonObject(effective) && typeof effective.kind === 'string'
        ? effective.kind
        : variants[0]?.kind;
  const selected =
    variants.find((variant) => variant.kind === currentKind) ?? variants[0];
  if (!selected) {
    return (
      <ScalarField
        rootSchema={rootSchema}
        schema={{ type: 'string' }}
        nullable={false}
        path={path}
        value={value}
        defaultValue={getConfigValue(defaultConfig, path)}
        effectiveValue={effective}
        override={overrideForPath(overrides, path)}
        issues={issues.filter((issue) => issue.path === path)}
        onChange={onChange}
      />
    );
  }
  const properties = objectProperties(rootSchema, selected.schema);
  const showDiscriminator = !childKeys || childKeys.has(selected.property);
  const selectKind = (kind: string) => {
    const nextVariant = variants.find((variant) => variant.kind === kind);
    if (!nextVariant) return;
    const nextValue = {
      [nextVariant.property]: nextVariant.kind,
    };
    if (path === 'storage') {
      storageUrlReplacement.onChange(null);
    }
    onChange(setConfigValue(value, path, nextValue));
  };
  return (
    <div
      className="col-span-full space-y-3 rounded-sm border border-subtle bg-panel-strong p-3"
      data-config-path={path}
      tabIndex={-1}
    >
      {!hideHeading || showDiscriminator ? (
        <div
          className={cx(
            'flex flex-col gap-2 sm:flex-row',
            hideHeading
              ? 'sm:items-center sm:justify-end'
              : 'sm:items-end sm:justify-between',
          )}
        >
          {!hideHeading ? (
            <div className="min-w-0">
              <h5
                className={cx(
                  'font-medium text-text',
                  depth === 0 ? 'text-sm' : 'text-xs',
                )}
              >
                {titleForKey(path.split('.').at(-1) ?? path)}
              </h5>
              {typeof schema.description === 'string' ? (
                <p className="mt-0.5 text-xs text-text-faint">
                  {schema.description}
                </p>
              ) : null}
            </div>
          ) : null}
          {showDiscriminator ? (
            path === 'storage' ? (
              <BaseRadioGroup
                aria-label="Storage backend"
                data-config-path={`${path}.${selected.property}`}
                required
                className="inline-flex w-fit items-center gap-0.5 rounded-sm border border-subtle bg-bg p-0.5"
                value={selected.kind}
                onValueChange={(kind) => selectKind(kind)}
              >
                {variants.map((variant) => (
                  <BaseRadio.Root
                    key={variant.kind}
                    value={variant.kind}
                    className="cursor-pointer rounded-sm px-2.5 py-1 text-xs text-text-muted transition-colors hover:text-[color:var(--color-text)] focus-visible:outline focus-visible:outline-2 focus-visible:outline-[color:var(--color-accent)] data-[checked]:bg-[color:var(--color-panel-strong)] data-[checked]:font-medium data-[checked]:text-[color:var(--color-text)]"
                  >
                    {STORAGE_KIND_LABELS[variant.kind] ??
                      titleForKey(variant.kind)}
                  </BaseRadio.Root>
                ))}
              </BaseRadioGroup>
            ) : (
              <label className="min-w-40 text-[10px] uppercase tracking-wider text-text-faint">
                Kind
                <select
                  data-config-path={`${path}.${selected.property}`}
                  data-field-control
                  className={cx(INPUT_CLASS, 'mt-1')}
                  value={selected.kind}
                  onChange={(event) => selectKind(event.target.value)}
                >
                  {variants.map((variant) => (
                    <option key={variant.kind} value={variant.kind}>
                      {titleForKey(variant.kind)}
                    </option>
                  ))}
                </select>
              </label>
            )
          ) : null}
        </div>
      ) : null}
      <div className="grid grid-cols-1 gap-3 md:grid-cols-2 2xl:grid-cols-3">
        {Object.entries(properties).map(([key, child]) =>
          key === selected.property ||
          (childKeys && !childKeys.has(key)) ? null : (
            <ConfigNode
              key={key}
              rootSchema={rootSchema}
              schema={child}
              path={`${path}.${key}`}
              value={value}
              defaultConfig={defaultConfig}
              effectiveConfig={effectiveConfig}
              overrides={overrides}
              issues={issues}
              onChange={onChange}
              depth={depth + 1}
            />
          ),
        )}
      </div>
    </div>
  );
}

function AdminProvidersEditor({
  rootSchema,
  schema,
  path,
  value,
  defaultConfig,
  effectiveConfig,
  overrides,
  issues,
  onChange,
  hideHeading = false,
}: {
  rootSchema: ConfigSchema;
  schema: ConfigSchema;
  path: string;
  value: JsonObject;
  defaultConfig: JsonObject;
  effectiveConfig: JsonObject;
  overrides: ConfigOverrideInfo[];
  issues: ConfigValidationIssue[];
  onChange: (value: unknown) => void;
  /** Suppress the root h5 when the section h4 already names it; the token-handling note always renders. */
  hideHeading?: boolean;
}) {
  const itemSchema = isJsonObject(schema.items)
    ? resolveConfigSchema(rootSchema, schema.items)
    : {};
  const variants = taggedVariants(rootSchema, itemSchema);
  const providers = Array.isArray(getConfigValue(value, path))
    ? (getConfigValue(value, path) as unknown[]).filter(isJsonObject)
    : [];
  const providerKeyCounter = useRef(0);
  const providerKeys = useRef<string[]>([]);
  while (providerKeys.current.length < providers.length) {
    providerKeys.current.push(`provider-${providerKeyCounter.current}`);
    providerKeyCounter.current += 1;
  }
  if (providerKeys.current.length > providers.length) {
    providerKeys.current.length = providers.length;
  }

  return (
    <div
      className="col-span-full space-y-3"
      data-config-path={path}
      tabIndex={-1}
    >
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          {!hideHeading ? (
            <h5 className="text-xs font-medium text-text">Admin Providers</h5>
          ) : null}
          <p className="mt-0.5 text-xs text-text-faint">
            Provider order is stable. Environment-backed tokens are referenced
            by name and never displayed.
          </p>
        </div>
        <Button
          size="sm"
          iconLeft={<Plus className="h-3 w-3" />}
          onClick={() => {
            const variant = variants[0];
            const provider = {
              [variant?.property ?? 'kind']: variant?.kind ?? 'static_token',
            };
            providerKeys.current.push(`provider-${providerKeyCounter.current}`);
            providerKeyCounter.current += 1;
            onChange(setConfigValue(value, path, [...providers, provider]));
          }}
        >
          Add provider
        </Button>
      </div>
      {providers.length ? (
        <div className="space-y-3">
          {providers.map((provider, index) => {
            const kind =
              typeof provider.kind === 'string'
                ? provider.kind
                : variants[0]?.kind;
            const variant =
              variants.find((candidate) => candidate.kind === kind) ??
              variants[0];
            const properties = variant
              ? objectProperties(rootSchema, variant.schema)
              : {};
            const providerRoot = `${path}[${index}]`;
            return (
              <div
                key={providerKeys.current[index]}
                data-config-path={providerRoot}
                tabIndex={-1}
                className="rounded-sm border border-subtle bg-panel-strong p-3"
              >
                <div className="mb-3 flex flex-wrap items-end justify-between gap-2">
                  <label className="min-w-44 text-[10px] uppercase tracking-wider text-text-faint">
                    Provider kind
                    <select
                      data-config-path={`${providerRoot}.${variant?.property ?? 'kind'}`}
                      data-field-control
                      className={cx(INPUT_CLASS, 'mt-1')}
                      value={kind}
                      onChange={(event) => {
                        const nextVariant = variants.find(
                          (candidate) => candidate.kind === event.target.value,
                        );
                        if (!nextVariant) return;
                        const nextProvider = {
                          [nextVariant.property]: nextVariant.kind,
                        };
                        const next = [...providers];
                        next[index] = nextProvider;
                        onChange(setConfigValue(value, path, next));
                        requestAnimationFrame(() =>
                          focusConfigPath(`${providerRoot}.id`),
                        );
                      }}
                    >
                      {variants.map((candidate) => (
                        <option key={candidate.kind} value={candidate.kind}>
                          {titleForKey(candidate.kind)}
                        </option>
                      ))}
                    </select>
                  </label>
                  <div className="flex items-center gap-1">
                    <Button
                      size="sm"
                      variant="ghost"
                      aria-label={`Move provider ${index + 1} up`}
                      disabled={index === 0}
                      onClick={() => {
                        const next = [...providers];
                        [next[index - 1], next[index]] = [
                          next[index],
                          next[index - 1],
                        ];
                        [
                          providerKeys.current[index - 1],
                          providerKeys.current[index],
                        ] = [
                          providerKeys.current[index],
                          providerKeys.current[index - 1],
                        ];
                        onChange(setConfigValue(value, path, next));
                      }}
                    >
                      <ChevronUp className="h-3.5 w-3.5" />
                    </Button>
                    <Button
                      size="sm"
                      variant="ghost"
                      aria-label={`Move provider ${index + 1} down`}
                      disabled={index === providers.length - 1}
                      onClick={() => {
                        const next = [...providers];
                        [next[index], next[index + 1]] = [
                          next[index + 1],
                          next[index],
                        ];
                        [
                          providerKeys.current[index + 1],
                          providerKeys.current[index],
                        ] = [
                          providerKeys.current[index],
                          providerKeys.current[index + 1],
                        ];
                        onChange(setConfigValue(value, path, next));
                      }}
                    >
                      <ChevronDown className="h-3.5 w-3.5" />
                    </Button>
                    <Button
                      size="sm"
                      variant="danger"
                      iconLeft={<Trash2 className="h-3 w-3" />}
                      onClick={() => {
                        providerKeys.current.splice(index, 1);
                        onChange(
                          setConfigValue(
                            value,
                            path,
                            providers.filter(
                              (_, providerIndex) => providerIndex !== index,
                            ),
                          ),
                        );
                      }}
                    >
                      Remove
                    </Button>
                  </div>
                </div>
                <div className="grid grid-cols-1 gap-3 md:grid-cols-2 2xl:grid-cols-3">
                  {Object.entries(properties).map(([key, child]) =>
                    key === (variant?.property ?? 'kind') ? null : (
                      <ConfigNode
                        key={key}
                        rootSchema={rootSchema}
                        schema={child}
                        path={`${providerRoot}.${key}`}
                        value={value}
                        defaultConfig={defaultConfig}
                        effectiveConfig={effectiveConfig}
                        overrides={overrides}
                        issues={issues}
                        onChange={onChange}
                        depth={2}
                        embedded
                      />
                    ),
                  )}
                </div>
              </div>
            );
          })}
        </div>
      ) : (
        <Notice tone="warning" title="No admin providers in the file">
          Ensure an environment or other deployment access path exists before
          saving and restarting.
        </Notice>
      )}
    </div>
  );
}

function RecurringJobsEditor({
  rootSchema,
  schema,
  path,
  value,
  defaultConfig,
  effectiveConfig,
  overrides,
  issues,
  onChange,
}: {
  rootSchema: ConfigSchema;
  schema: ConfigSchema;
  path: string;
  value: JsonObject;
  defaultConfig: JsonObject;
  effectiveConfig: JsonObject;
  overrides: ConfigOverrideInfo[];
  issues: ConfigValidationIssue[];
  onChange: (value: unknown) => void;
}) {
  const configured = isJsonObject(getConfigValue(value, path))
    ? (getConfigValue(value, path) as JsonObject)
    : {};
  const defaults = isJsonObject(getConfigValue(defaultConfig, path))
    ? (getConfigValue(defaultConfig, path) as JsonObject)
    : {};
  const effective = isJsonObject(getConfigValue(effectiveConfig, path))
    ? (getConfigValue(effectiveConfig, path) as JsonObject)
    : {};
  const keys = [
    ...new Set([
      ...Object.keys(defaults),
      ...Object.keys(configured),
      ...Object.keys(effective),
    ]),
  ].sort();
  const jobSchema = isJsonObject(schema.additionalProperties)
    ? resolveConfigSchema(rootSchema, schema.additionalProperties)
    : {};
  return (
    <div
      className="col-span-full space-y-2"
      data-config-path={path}
      tabIndex={-1}
    >
      {keys.length ? (
        keys.map((key) => (
          <RecurringJobRow
            key={key}
            rootSchema={rootSchema}
            jobSchema={jobSchema}
            jobKey={key}
            path={path}
            value={value}
            defaultConfig={defaultConfig}
            effectiveConfig={effectiveConfig}
            overrides={overrides}
            issues={issues}
            hasDefault={Object.hasOwn(defaults, key)}
            defaultJobValue={defaults[key]}
            onChange={onChange}
          />
        ))
      ) : (
        <p className="text-xs text-text-faint">
          No recurring jobs are configured.
        </p>
      )}
    </div>
  );
}

/**
 * One flattened row per recurring job: the section card already supplies the
 * "Recurring jobs" title, so each job is a single subtle row whose header
 * carries the friendly label, key, status, the compact enabled switch, and
 * reset/unset actions. Interval/jitter render as embedded scalar fields.
 */
function RecurringJobRow({
  rootSchema,
  jobSchema,
  jobKey,
  path,
  value,
  defaultConfig,
  effectiveConfig,
  overrides,
  issues,
  hasDefault,
  defaultJobValue,
  onChange,
}: {
  rootSchema: ConfigSchema;
  jobSchema: ConfigSchema;
  jobKey: string;
  path: string;
  value: JsonObject;
  defaultConfig: JsonObject;
  effectiveConfig: JsonObject;
  overrides: ConfigOverrideInfo[];
  issues: ConfigValidationIssue[];
  hasDefault: boolean;
  defaultJobValue: unknown;
  onChange: (value: unknown) => void;
}) {
  const { fileConfig } = useContext(ConfigSourcesContext);
  const jobPath = `${path}.${jobKey}`;
  const enabledPath = `${jobPath}.enabled`;
  const meta = recurringJobMetadata(jobKey);
  const jobGuidance = resolveConfigFieldGuidance(
    jobPath,
    typeof jobSchema.description === 'string'
      ? jobSchema.description
      : undefined,
  );
  const enabledGuidance = resolveConfigFieldGuidance(
    enabledPath,
    undefined,
    'boolean',
  );
  const jobCurrent = getConfigValue(value, jobPath);
  const jobFile = getConfigValue(fileConfig, jobPath);
  const jobModified =
    jobCurrent !== undefined && !isSameJson(jobCurrent, jobFile);
  const dangerous = classifyConfigLeaf(jobPath).dangerous;
  const enabledCurrent = getConfigValue(value, enabledPath);
  const enabledEffective = getConfigValue(effectiveConfig, enabledPath);
  const enabledConfigured =
    enabledCurrent !== undefined && enabledCurrent !== null;
  const enabledChecked = enabledConfigured
    ? enabledCurrent === true
    : enabledEffective === true;
  const enabledOverride = overrideForPath(overrides, enabledPath);
  const jobIssues = issues.filter(
    (issue) => issue.path === jobPath || issue.path === enabledPath,
  );
  const fieldProperties = Object.entries(
    objectProperties(rootSchema, jobSchema),
  ).filter(([field]) => field !== 'enabled');
  return (
    <div
      className="rounded-sm border border-subtle bg-panel-strong p-3"
      data-config-path={jobPath}
      tabIndex={-1}
    >
      <div className="flex min-w-0 flex-wrap items-start justify-between gap-x-3 gap-y-2">
        <div className="flex min-w-0 flex-wrap items-center gap-x-2 gap-y-1">
          <span className="text-xs font-medium text-text">
            {meta?.label ?? titleForKey(jobKey)}
          </span>
          <span className="break-all font-mono text-[10px] text-text-faint">
            {jobKey}
          </span>
          {!hasDefault ? <Badge tone="warn">Unknown key</Badge> : null}
          {dangerous ? <Badge tone="warn">Operational risk</Badge> : null}
          {jobModified ? <Badge tone="accent">Modified</Badge> : null}
        </div>
        <div className="flex shrink-0 flex-wrap items-center gap-1">
          <ToggleSwitch
            variant="compact"
            data-config-path={enabledPath}
            data-field-control
            checked={enabledChecked}
            aria-invalid={jobIssues.some(
              (issue) =>
                issue.path === enabledPath && issue.severity === 'error',
            )}
            label="Enabled"
            onChange={(event) =>
              onChange(setConfigValue(value, enabledPath, event.target.checked))
            }
          />
          <Button
            size="sm"
            variant="ghost"
            iconLeft={<RotateCcw className="h-3 w-3" />}
            disabled={!hasDefault}
            onClick={() =>
              onChange(
                setConfigValue(value, jobPath, cloneJson(defaultJobValue)),
              )
            }
          >
            Reset
          </Button>
          <Button
            size="sm"
            variant="ghost"
            iconLeft={<X className="h-3 w-3" />}
            onClick={() => onChange(unsetConfigValue(value, jobPath))}
          >
            Unset
          </Button>
        </div>
      </div>
      <p className="mt-1.5 text-xs leading-relaxed text-text-faint">
        {jobGuidance.description}
      </p>
      <GuidanceTradeoffs guidance={jobGuidance} />
      <GuidanceTradeoffs guidance={enabledGuidance} />
      <div className="mt-1.5 flex min-w-0 flex-wrap items-center gap-x-3 gap-y-1 text-[10px] text-text-faint">
        <span className="font-medium text-text-muted">Enabled</span>
        <span>Draft: {displayValue(enabledCurrent)}</span>
        <span>Effective: {displayValue(enabledEffective)}</span>
        {!enabledConfigured ? <Badge tone="neutral">Inherited</Badge> : null}
      </div>
      {enabledOverride ? (
        <div className="mt-1 text-[10px] leading-relaxed text-text-muted">
          Enabled state comes from {sourceLabel(enabledOverride)} ·{' '}
          {enabledOverride.name}. The file value applies only if the override is
          removed.
        </div>
      ) : null}
      {jobIssues.map((issue, index) => (
        <div
          key={`${issue.code}-${index}`}
          className={cx(
            'mt-1.5 text-[11px]',
            issue.severity === 'error'
              ? 'text-[color:var(--color-danger-text)]'
              : 'text-[color:var(--color-warn-text)]',
          )}
        >
          {issue.message}
        </div>
      ))}
      {fieldProperties.length ? (
        <div className="mt-2 grid grid-cols-1 gap-3 sm:grid-cols-2">
          {fieldProperties.map(([field, child]) => {
            const fieldPath = `${jobPath}.${field}`;
            return (
              <ScalarField
                key={field}
                embedded
                suppressActions
                rootSchema={rootSchema}
                schema={child}
                nullable={isNullableConfigSchema(rootSchema, child)}
                path={fieldPath}
                value={value}
                defaultValue={getConfigValue(defaultConfig, fieldPath)}
                effectiveValue={getConfigValue(effectiveConfig, fieldPath)}
                override={overrideForPath(overrides, fieldPath)}
                issues={issues.filter(
                  (issue) =>
                    issue.path === fieldPath ||
                    issue.path.startsWith(`${fieldPath}.`),
                )}
                onChange={onChange}
              />
            );
          })}
        </div>
      ) : null}
    </div>
  );
}

function focusConfigPath(path: string) {
  const focus = () => {
    const targets =
      document.querySelectorAll<HTMLElement>('[data-config-path]');
    const exact = [...targets].find(
      (target) => target.dataset.configPath === path,
    );
    const parent = [...targets]
      .filter((target) => {
        const candidate = target.dataset.configPath;
        return Boolean(
          candidate &&
            (path.startsWith(`${candidate}.`) ||
              path.startsWith(`${candidate}[`)),
        );
      })
      .sort(
        (left, right) =>
          (right.dataset.configPath?.length ?? 0) -
          (left.dataset.configPath?.length ?? 0),
      )[0];
    const target = exact ?? parent;
    target?.scrollIntoView({ block: 'center', behavior: 'smooth' });
    const explicit = target?.matches('[data-field-control]')
      ? target
      : target?.querySelector<HTMLElement>('[data-field-control]');
    const fieldControlSelector =
      'input:not([type="hidden"]):not(:disabled), select:not(:disabled), textarea:not(:disabled), [role="switch"]:not([aria-disabled="true"]), [role="radio"]:not([aria-disabled="true"])';
    const fieldControl =
      explicit ??
      // A checked radio is the meaningful control inside a segmented group;
      // fall back to the first enabled radio when nothing is checked yet.
      (target?.matches(fieldControlSelector)
        ? target
        : (target?.querySelector<HTMLElement>(
            '[role="radio"][aria-checked="true"]',
          ) ?? target?.querySelector<HTMLElement>(fieldControlSelector)));
    const actions = target?.matches('button:not(:disabled)')
      ? [target]
      : [
          ...(target?.querySelectorAll<HTMLElement>('button:not(:disabled)') ??
            []),
        ];
    const primaryAction = actions.find(
      (action) =>
        !DESTRUCTIVE_ACTION_LABELS.includes(action.textContent?.trim() ?? ''),
    );
    (fieldControl ?? primaryAction ?? actions[0] ?? target)?.focus();
  };
  requestAnimationFrame(focus);
}
