# cc-lb Admin Web Design System

## 1. Atmosphere & Identity

cc-lb admin is a quiet infrastructure command center: dense enough for operators, but calm enough to scan during incidents. The signature is glass-on-black operational depth: subdued surfaces, thin borders, mono labels, and status color used only for meaning.

## 2. Color

### Palette

| Role | Token | Light | Dark | Usage |
|------|-------|-------|------|-------|
| Surface/base | `--color-bg` | `#ffffff` | `#000000` | App background |
| Surface/subtle | `--color-bg-sub` | `#fafafa` | `#050505` | Secondary page background |
| Surface/panel | `--color-panel` | `rgba(0,0,0,0.018)` | `rgba(255,255,255,0.02)` | Card surfaces via `.glass` |
| Surface/strong | `--color-panel-strong` | `rgba(0,0,0,0.04)` | `rgba(255,255,255,0.04)` | Buttons, table headers, elevated rows |
| Border/default | `--color-border` | `rgba(0,0,0,0.10)` | `rgba(255,255,255,0.08)` | Cards and dividers |
| Border/strong | `--color-border-strong` | `rgba(0,0,0,0.18)` | `rgba(255,255,255,0.14)` | Strong separators and active frames |
| Text/primary | `--color-text` | `#0a0a0a` | `#ededed` | Primary text |
| Text/muted | `--color-text-muted` | `#4b5563` | `#9ca3af` | Secondary text |
| Text/faint | `--color-text-faint` | `#6b7280` | `#6b7280` | Overlines, hints, metadata |
| Accent | `--color-accent` | `#0086d4` | `#00d4ff` | Focus, selected states, rare primary accents |
| Success | `--color-ok` | `#059669` | `#10b981` | Healthy status |
| Warning | `--color-warn` | `#d97706` | `#f59e0b` | Degraded or retrying status |
| Danger | `--color-danger` | `#dc2626` | `#ef4444` | Down, failed, destructive status |
| Neutral | `--color-neutral` | `#6b7280` | `#6b7280` | Pending, skipped, disabled status |

### Rules

- Use semantic status colors only for status dots, badges, and incident affordances.
- Do not introduce decorative gradients or brand-color fills in admin cards.
- Prefer existing utility classes such as `text-text`, `text-text-muted`, `text-text-faint`, `bg-overlay-*`, `border-subtle`, and `glass`.

## 3. Typography

### Scale

| Level | Class / Size | Weight | Usage |
|-------|--------------|--------|-------|
| Page title | `text-lg` to `text-xl` | 600 | Route-level headings |
| Card title | `text-sm` | 500 | `CardHeader` titles |
| Body/sm | `text-sm` | 400 | Main card copy |
| Caption | `text-xs` | 400-500 | Secondary copy, buttons, table cells |
| Overline | `text-[11px] uppercase tracking-wider` | 500-600 | Section labels and metadata keys |
| Mono metadata | `font-mono text-[11px]` | 400-600 | IDs, status labels, compact counters |

### Font Stack

- Primary: `Geist Variable`, `Geist`, `Inter`, `ui-sans-serif`, `system-ui`, `-apple-system`, `Segoe UI`, `sans-serif`.
- Mono: `Geist Mono Variable`, `Geist Mono`, `ui-monospace`, `JetBrains Mono`, `monospace`.

### Rules

- Use mono type for machine-readable labels and status codes, not for long prose.
- Keep dashboard copy short; detailed debug text belongs in drawers or detail panels.

## 4. Spacing & Layout

### Base Unit

All spacing derives from a 4px base through Tailwind spacing utilities.

| Token | Value | Usage |
|-------|-------|-------|
| `gap-1` | 4px | Dot-to-label, tight inline groups |
| `gap-2` | 8px | Button groups, compact row items |
| `gap-3` | 12px | Header action wrapping, stacked fields |
| `gap-4` / `p-4` | 16px | Default card body spacing |
| `gap-6` | 24px | Page section rhythm |

### Grid

- Use `PageContainer` for route width and `Section` for page grouping.
- Cards should collapse to one column on mobile and avoid forced `flex-row` headers unless content is proven to fit.
- Mockups must be visually checked at 375px, 768px, and 1280px.

### Rules

- Prefer fewer groups with clear hierarchy over dense metric grids.
- When a card has a drawer, keep raw logs and verbose diagnostics in the drawer.

## 5. Components

### Card

- **Structure**: `Card` + optional `CardHeader` + `CardBody`.
- **Spacing**: `CardHeader` owns `px-4 py-3`; `CardBody` owns `p-4`.
- **States**: card itself is passive; controls inside carry focus and disabled states.
- **Accessibility**: headers should stay text-based and actions should be real buttons.

### StatusBadge / Badge

- **Structure**: dot plus mono uppercase label for `StatusBadge`; bordered compact label for `Badge`.
- **Variants**: `ok`, `warn`, `danger`, `neutral`, `accent`, `mono`.
- **Usage**: one dominant status badge per card; secondary outcomes use smaller badges or dots.

### Button

- **Variants**: `primary`, `secondary`, `ghost`, `danger`, `accent`.
- **Rules**: use `secondary` for operational actions, `ghost` for navigation/detail, `danger` only for destructive operations.

## 6. Motion & Interaction

### Timing

| Type | Duration | Usage |
|------|----------|-------|
| Micro | 150-200ms | Hover, toggle, focus state |
| Standard | 200-300ms | Drawer, dialog, disclosure |

### Rules

- Use existing transition utilities and animate only color, opacity, or transform.
- Every interactive element must remain keyboard-focusable with the existing accent focus ring.
- Do not use emoji as icons; use Lucide or text-only labels.

## 7. Depth & Surface

### Strategy

Depth uses a mixed glass-and-border system already present in `index.css`:

| Level | Token / Utility | Usage |
|-------|-----------------|-------|
| Base | `--color-bg` | Page background |
| Card | `.glass` | Main panels |
| Nested subtle | `bg-overlay-2` / `bg-overlay-3` | Inline callouts and grouped controls |
| Nested strong | `bg-overlay-5` / `border-subtle-strong` | Active or selected affordances |

### Rules

- Avoid heavy shadows; surface separation should come from glass, borders, and overlay tokens.
- A simplified warmup card should show one primary status story, one primary action cluster, and one drawer entry point. Everything else should be summarized or moved behind disclosure.

## 8. Time Controls & Bounded History

### Time UX Contract
- **Compact Controls**: Time ranges use a dense preset selector (`All time`, `1h`, `6h`, `24h`, `7d`, `Custom`) alongside optional `Since` and `Until` inputs.
- **Timezone Awareness**: Custom bounds use dashboard-styled `YYYY-MM-DD HH:mm` text inputs interpreted in the user's effective configured timezone (via `useTimezone()`), not blindly in the browser's local timezone. Each field pairs the validated text entry with a date-only `react-day-picker` calendar: a Base UI popover at desktop width and the existing Base UI Modal surface below 1024px. Picking a date preserves an existing time or supplies the bound's start/end-of-day default. Since resolves to the first second of its displayed minute; inclusive Until resolves to the final second.
- **Validation**: Invalid calendar values, nonexistent spring-forward times, and ambiguous fall-back times are deterministically distinguished with programmatically linked inline errors. URL bounds outside the JavaScript Date range or inverted Custom bounds are removed before rendering or querying.
- **Draft & Persistence**: Presets compute a canonical `since_unix_secs` at selection time. Custom edits remain local until explicit Apply; Cancel or Escape restores the applied range without changing the URL. Custom ranges require and persist both `since_unix_secs` and `until_unix_secs`, and URL reloads reproduce the exact mode and applied bounds.

### Bounded History & Client Pagination
- **Request Rows & Client Pagination**: The client retains at most 500 finalized events plus 500 in-flight partials (and up to 500 eviction tombstones) in memory, which remains unchanged. To prevent CPU lag and long tasks, the log table uses client-side pagination to mount at most 50 data rows per page in the DOM. Table and row components retain `React.memo` optimization, and merged rows keep referential identity for unchanged events so live-tail updates do not re-render the whole page.
- **Filtering & Pagination Rules**: Filters apply to the retained events before client pagination is calculated. Any route or filter change resets the current page to page 1. Shrink clamps are applied to prevent out-of-bounds pages.
- **Status Classes**: Status filtering uses the backend's `status_class` contract (`2xx`, `3xx`, `4xx`, `5xx`) for recent history and live SSE, then applies the same class predicate to retained rows so stale live or placeholder data cannot leak across transitions.
- **In-Progress Cost**: Once usage is observed for a priced model, partial rows and drawers show a token-derived `Estimated Cost`. This is display-only; authoritative final cost remains sourced from the termination-time `Priced` event used by billing and limit reconciliation.
- **Export Behavior**: The Export action includes all filtered retained rows, not just the current page.
- **Sentinel Behavior**: The backend infinite-scroll sentinel is only shown on the final client page, and it is still disabled when a session filter is active.
- **Pagination Controls**: Controls are accessible, featuring labeled Prev and Next buttons and an `aria-live` region for screen readers.
- **Backend Paging**: The exact 200, 200, 100 backend paging limits remain unchanged.
- **Live Tailing**: Tailing is automatically paused when a fixed custom time range is selected, and resumes based on user preference when returning to a relative preset or "All time".
