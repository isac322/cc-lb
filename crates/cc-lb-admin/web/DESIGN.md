# cc-lb Admin Web Design System

## 1. Atmosphere & Identity

cc-lb admin is a quiet operations console: dense enough for operators, calm enough to scan during an incident. The identity is a near-black canvas (a soft gray one in light mode), hairline borders, one cyan accent, and status color used only for meaning. Craft comes from hierarchy, restraint and consistent geometry, not from decoration.

- Sans (Geist) is the voice of the product, including every number. Mono (Geist Mono) is reserved for strings an operator copies or compares character by character.
- Color carries state, never ornament. Red appears only where something is broken or destructive.
- Show exceptions; let healthy states speak by omission.

## 2. Color

### Palette

| Role | Token | Light | Dark | Usage |
|------|-------|-------|------|-------|
| Canvas | `--color-bg` | `#f7f7f8` | `#000000` | Page background, topbar |
| Overlay surface | `--color-bg-sub` | `#ffffff` | `#0a0a0a` | Dialogs, drawers, popovers, menus, sticky table headers, sidebar |
| Panel | `--color-panel` | `#ffffff` | `rgba(255,255,255,0.02)` | Card fill (`.glass`) |
| Panel strong | `--color-panel-strong` | `rgba(0,0,0,0.04)` | `rgba(255,255,255,0.04)` | Secondary button fill |
| Input | `--color-input-bg` | `#ffffff` | `rgba(255,255,255,0.03)` | Inputs and select triggers |
| Border | `--color-border` | `rgba(0,0,0,0.10)` | `rgba(255,255,255,0.08)` | Cards, dividers between sub-sections |
| Border strong | `--color-border-strong` | `rgba(0,0,0,0.18)` | `rgba(255,255,255,0.14)` | Inputs, overlays, table header rule |
| Border row | `--color-border-row` | `rgba(0,0,0,0.05)` | `rgba(255,255,255,0.04)` | Table and list row dividers, chart gridlines |
| Overlay steps | `--color-overlay-{1..6}` | 2–8% black | 2–8% white | Wells (`overlay-2`), hover (`overlay-2`/`-5`), selected segments (`overlay-6`) |
| Text | `--color-text` | `#0a0a0a` | `#ededed` | Primary text |
| Text muted | `--color-text-muted` | `#4b5563` | `#9ca3af` | Secondary text, labels, notice bodies |
| Text faint | `--color-text-faint` | `#5b636f` | `#7d858f` | Captions, table headers, chart ticks |
| Accent | `--color-accent` | `#0086d4` | `#00d4ff` | Focus rings, active tab bar, switch on, live dot |
| Accent text | `--color-accent-text` | `#00629b` | `#00d4ff` | Links and accent-colored text (AA in light) |
| Accent dim | `--color-accent-dim` | `rgba(0,134,212,0.10)` | `rgba(0,212,255,0.12)` | Selected table row, text selection |
| Success | `--color-ok` | `#059669` | `#10b981` | Dots, fills, borders |
| Warning | `--color-warn` | `#d97706` | `#f59e0b` | Dots, fills, borders; meters from 80% |
| Danger | `--color-danger` | `#dc2626` | `#ef4444` | Dots, fills, borders; meters from 95% |
| Danger solid | `--color-danger-solid` / `-hover` | `#dc2626` / `#b91c1c` | `#dc2626` / `#b91c1c` | White-text fill of `danger-solid` buttons (AA in both themes) |
| Neutral | `--color-neutral` | `#6b7280` | `#6b7280` | Pending, skipped, disabled dots |
| Status text | `--color-{success,warn,danger}-text` | `#046049` / `#92400e` / `#a51b1b` | `#6ee7b7` / `#fcd34d` / `#fca5a5` | Any status-colored *text*; AA on `tone/8`–`/15` tints, including over hover and active fills |
| Window series | `--color-series-{5h,7d,fable,sonnet,opus,overage,unified}` | OKLCH L≈0.55, C≈0.13 | OKLCH L≈0.72, C≈0.10 | Quota window and model series in charts and legends |
| Token series | `--color-series-{input,output,cache-create-5m,cache-create-1h,cache-read}` | 600–700 hues | 400 hues | Token/cost slices |
| Chart fill | `--chart-fill-opacity` | `0.10` | `0.12` | Flat area fill under a series stroke |

Window series hues: 5h sky (235), 7d violet (290), Fable green (155), Sonnet teal (195), Opus rose (350); overage and unified are neutral grays. `lib/colors.ts#getWindowColor` returns these as `var()` references so charts follow the theme; unknown keys fall back to `unified`.

### Rules

- Status colors mark state only: dots, badges, severity, incident affordances, destructive actions. KPIs, sparklines and links never borrow them. Error rate turns danger only above zero.
- Status-colored text always uses the `*-text` tokens. Never use raw Tailwind palette classes (`text-red-400`, `zinc-*`) or hex literals in components.
- There is no blue and no "info" hue: informational notices are neutral.
- Series never use amber, orange or red hues; those mean severity.
- Quota severity comes from `lib/quotaSeverity.ts`: below 80% neutral, 80% and above `warn`, 95% and above `danger`. Window identity colors appear only in chart series and legends, never as the fill of a utilization bar.
- No gradients, glows or brand-color fills. No side-stripe borders; mark a special row with a leading dot or icon.
- Tokens are registered through `@theme inline` in `index.css`, so every utility (`text-text`, `bg-overlay-3`, `border-subtle`, `bg-series-5h`) supports `hover:`, `focus-visible:`, `dark:` and `/opacity` variants. `dark:` follows the app theme (`data-theme`), not only the OS.
- For data colors computed in JS and used as text, use CSS `light-dark(<light>, <dark>)`.

## 3. Typography

### Type roles

Each role is one Tailwind utility (`@theme` in `index.css`) that sets size, line height, weight and tracking together. All sizes are `rem`, so the browser font-size preference scales them.

| Utility | Size / line height | Weight | Tracking | Use |
|---------|--------------------|--------|----------|-----|
| `text-display` | 28 / 32 | 500 | -0.02em | Hero metrics only: KPI values, the quota % on window cards |
| `text-title-page` | 22 / 28 | 600 | -0.01em | `PageHeader` h1; entity titles on detail panes (upstream, principal, plugin name). Sans, never mono |
| `text-title-section` | 16 / 24 | 600 | -0.005em | `Section` h2 outside cards; Modal and Drawer titles |
| `text-title-card` | 14 / 20 | 600 | 0 | `CardHeader`, confirm-dialog title, empty-state title |
| `text-body` | 14 / 22 | 400 | 0 | Primary copy, dialog descriptions |
| `text-body-sm` | 13 / 20 | 400 | 0 | Table cells, list secondary lines, field descriptions, toolbar copy, notices |
| `text-label` | 12 / 16 | 500 | 0.005em | Form labels, metric labels, table headers, `dl` keys, badges. Sentence case |
| `text-caption` | 12 / 16 | 400 | 0 | Hints, timestamps, sublines (`text-muted` / `text-faint`) |
| `text-overline` | 11 / 16, uppercase | 500 | 0.06em | **Only** sidebar and command-palette group headings |
| `font-mono text-data` | 12 / 16 | 400 | 0 | IDs, hashes, key IDs, model IDs, paths, env vars, request IDs, raw config keys, code |

`text-2xs` (11px) remains the floor for compact chrome (sidebar footer, sidebar counts, chart ticks). Nothing renders below 11px.

### Font stack

- Sans: `Geist Variable`, `Geist`, `Inter`, `ui-sans-serif`, `system-ui`, `-apple-system`, `Segoe UI`, `sans-serif`.
- Mono: `Geist Mono Variable`, `Geist Mono`, `ui-monospace`, `JetBrains Mono`, `monospace`.

### Rules

- Weights are 400, 500 and 600 only. 600 is for titles; 500 for labels, buttons and emphasized numbers. No 700 (the base layer resets the UA `th` bold).
- Adjacent levels differ by at least 2px or one weight step: page 22/600 > section 16/600 > card 14/600 > body 14 or 13/400 > label 12/500 > caption 12/400.
- Numbers (%, $, ms, counts, durations) are sans with `tabular-nums` (the body default). Units sit at the same size in `text-muted` ("669 ms"), not smaller.
- Mono only for strings an operator copies or compares character by character. Never for names people chose, prose, labels, badges, dates or durations.
- Uppercase only through `text-overline`. Never uppercase a string that contains a unit, version or identifier (5h, 7d, 5m, v0.6.0).
- Sentence case for every heading, button, tab and label ("Recent requests", "Delete plugin", "Back to catalog"). Use plain words ("Admin address", not "Admin Addr").
- No eyebrow or kicker text above a heading.
- Every route renders exactly one `h1` through `PageHeader`. `CardHeader` defaults to `h2`; cards nested under a titled section pass `headingLevel={3}`. Do not skip levels.
- Keep dashboard copy short; detailed debug text belongs in drawers.

## 4. Spacing & Layout

### Scale

All spacing is on a 4px base: 4, 8, 12, 16, 20, 24, 32, 40 (`1, 2, 3, 4, 5, 6, 8, 10`).

| Context | Value |
|---------|-------|
| Page padding | 24 at `md`+ (`md:p-6`), 16 on mobile (`p-4`) |
| Page header → first content | 24 (`PageHeader` carries `mb-6`) |
| Between page sections | 32 (`PageContainer` is `space-y-8`) |
| Section title → content | 12 (`Section` is `gap-3`) |
| Card header / body | `px-4 py-3` / `p-4` |
| List row | `px-4 py-2.5` (40px) |
| Table row | 40 default, 32 dense |
| Form | label → control 6, control → help 4, field → field 20 vertical, 24 horizontal gutter; field groups separated by a divider plus 24 |
| Heights | topbar 48; controls 32 (`sm`) / 36 (`md`); segmented control 32 outer / 26 inner |

### Layout rules

- Use `PageContainer` for route width and `Section` for page grouping. Cards collapse to one column on mobile; avoid forced `flex-row` headers unless the content fits.
- A card is as tall as its content. No `min-h`, `h-[50vh]` or stretched grid cells to fill space.
- Prefer fewer groups with clear hierarchy over dense metric grids. Put raw logs and verbose diagnostics in drawers.
- Check layouts at 390px, 768px and 1440px.

### Radii

| Token | Value | Use |
|-------|-------|-----|
| `rounded-xs` (`--radius-xs`) | 2px | Meters, bars, tiny tags, skeletons |
| `rounded-sm` (`--radius-sm`) | 4px | Buttons, inputs, badges, segments, menu items, in-card wells and notices |
| `rounded-md` (`--radius-md`) | 6px | Cards, dialogs, drawers' inner panels, popovers, page banners |
| `rounded-full` | — | Status dots, switches, avatars, the sidebar count pill. Never bars or tracks |

## 5. Components

Primitives live in `src/components/ui/`: `primitives.tsx` (Card, Button, Badge, Notice, Field, …), `Select.tsx`, `Table.tsx`, `Tabs.tsx`, `charts.tsx`.

### Card and Section

- **Card** (`Card` + optional `CardHeader` + `CardBody`): a self-contained object with its own header or actions — a chart, a table, a form group, a detail block. `CardHeader` owns `px-4 py-3` and a `text-title-card` heading with an optional `text-caption` subtitle; `CardBody` owns `p-4`.
- **Section** (`Section`): a flat page region with a `text-title-section` h2 and no box. Use it for groups of peers (window cards, "Used by" lists) and around page-level tables that already have a frame.
- A section title and a card title never share a size.

### Button

| Variant | Look | Use |
|---------|------|-----|
| `primary` | Solid inverted neutral (`bg-text text-bg`) | One per view: the commit action (Save, Create, Continue). Not "New" in list headers |
| `secondary` (default) | `panel-strong` fill + hairline border | Toolbars, paired actions, most actions |
| `ghost` | No fill or border; `text-muted` → `text` on hover | Tertiary actions, in-row links |
| `danger` | Outline: `danger/45` border, `danger-text`, hover `danger/10` | Page-level Delete / Revoke |
| `danger-solid` | `danger-solid` fill, white text, darker fill on hover | Only the confirm button of a destructive `ConfirmDialog` |

- Sizes: `sm` 28px / 12px text (tables, toolbars), `md` 32px / 13px (default), `lg` 36px / 14px (dialog footers, mobile).
- Icons are 14px in `sm`/`md` and 16px in `lg`, enforced by the Button; use lucide at `strokeWidth={1.75}`.
- Disabled is 40% opacity of the same variant. A disabled `primary` renders as a disabled `secondary` until it becomes actionable.
- Paired or sibling actions share a variant and size. No full-width buttons inside desktop cards.
- A toggled toolbar action ("Stop tail") is `secondary` with `aria-pressed`, plus a live dot while active.

### IconButton

Every icon-only control. `label` is required and becomes the accessible name. 44×44 below `md`, 32×32 from `md`; the glyph is 16px.

### Badge and StatusBadge

- `Badge`: 20px tall, `text-label` sans sentence case, `px-1.5`, `rounded-sm`, no border. Tones: `neutral` (`overlay-4`, `text-muted`); `accent`, `ok`, `warn`, `danger` (tone/12 fill, tone text); `mono` (`font-mono text-data` on `overlay-3`, for model IDs and key IDs only).
- `StatusBadge`: a status dot plus a `text-label` phrase ("Enabled", "Reconnect required"). Healthy tones stay `text-muted`; `warn`/`danger` use their `*-text`. No chip, no mono, no uppercase.
- At most one status badge per object header. In lists, status is a dot plus a short `*-text` phrase, not a filled chip. Healthy states are shown by omission (no "Active" ×12).
- Status dot: `.status-dot` 6px (`.status-dot.lg` 8px in lists), flat. Only `live` pulses, and it is static under reduced motion.
- Sidebar count: a 16px `tone/15` pill with a `*-text` numeral; a 6px dot when the sidebar is collapsed.

### Tables

Use `Table`, `TableHead`, `TableHeadCell`, `TableRow`, `TableCell`, `TableEmptyRow` and `EmptyValue` from `Table.tsx`, or the `table-header` (on `thead`) and `table-th` (on `th`) utilities for existing markup.

- Header: `text-label` sans sentence case in `text-faint`, 32px, opaque sticky `bg-sub` with a `border-strong` rule. No uppercase, no tracking, no blur.
- Body: `text-body-sm` 13px sans. Mono (`TableCell mono`) only for ID, model, path and hash columns. Numbers right-aligned and tabular (`numeric`), units muted at the same size.
- Rows: 40px (32px `dense`), `border-row` divider, `overlay-2` hover, `accent-dim` when selected.
- No per-cell bars. If a magnitude cue is needed, one 2px neutral bar in a single "share" column; breakdowns go in the drawer.
- Hide a column when every visible row is null; show a null cell as `—` in `text-faint`.
- A table inside a card drops the card body padding; the first and last cells keep a 16px inset so the header aligns with the card title.

### Charts (Recharts)

- Series colors come from `--color-series-*` via `getWindowColor`. Areas use a flat fill at `SERIES_FILL_OPACITY` (12% dark / 10% light), never a gradient, and a 1.5px stroke. When series overlap, fill only the primary or hovered series and draw the rest as lines.
- Grid: `CartesianGrid vertical={false} stroke="var(--color-border-row)"`. Axis and tick lines are hidden globally.
- Ticks: 11px sans tabular in `text-faint` (global rule in `index.css`); about 4 x-ticks; 0/50/100 on a % axis.
- Thresholds: 1px dashed (`3 3`) `warn`/`danger` lines at 50% opacity with "80%"/"95%" labels at the right end in 11px `*-text`. No tinted `ReferenceArea` bands.
- Legend: in the chart header row, right-aligned: a 10×2px line swatch, a 12px `text-muted` label and a tabular value. Clicking toggles.
- Tooltip: `bg-sub`, `border-strong`, `rounded-md`, overlay shadow, 12px sans; swatch and label left, tabular value right.
- Histograms: neutral `text-faint`/60% bars with errors stacked in `danger`.
- No donuts: use an 8px stacked bar plus a value table.
- `Sparkline` draws a 1.5px line over a flat fill and renders nothing when every value is zero.

### Meters and bars

- 6px tall in lists, 8px on a hero; `rounded-xs`; track `bg-progress-track` (flat, no border).
- Fill is `text-muted` below 80%, `warn` from 80%, `danger` from 95%.
- Stacked segments sit in a `flex gap-px` row so the track shows between them; each segment is square with `min-width: 2px`. Only the track is rounded.
- The % label sits 8px right of the bar (or above-right), never across the row.

### Forms

- `Field`: `text-label` sentence-case label in `text-muted` above the control, `text-caption` hint in `text-faint` below, `text-caption` error in `danger-text`. Native `input`/`textarea` children get `required`, `aria-invalid` and `aria-describedby` automatically.
- Inputs: `INPUT_CLASS` (36px, `md`) or `INPUT_SM_CLASS` (32px, `sm`): `rounded-sm`, `bg-input-bg`, `border-strong`, 2px accent focus outline at 1px offset.
- `Select` (`Select.tsx`) is the only select control; never a native `<select>`. `allLabel` adds the `''` "all" item for filter bars; otherwise `placeholder` shows until a value is chosen. Width comes from `className`; the popup matches the trigger. Pass `aria-label` when no `Field` names it.
- Disclosure: a custom `<summary>` with `list-style: none` and a 12px rotating lucide chevron.
- `ToggleSwitch`: 36×20 accent switch over a native checkbox with an accessible name. Label is `text-body-sm` sans. The default `card` variant is an in-card well (fill, no border); `compact` sits inline in a header row.
- `SegmentedControl`: at most 5 short options. Radiogroup with roving focus; selected is `overlay-6` + `text`. Use it for every small exclusive choice (time presets, window toggles); do not hand-roll chip rows.
- `Tabs` (`Tabs.tsx`): underline tabs, 13/500, 36px row, 2px accent bar under the active item, one horizontally scrolling line with an edge fade. `mode="tabs"` is an ARIA tablist (pair panels with `tabPanelProps`); `mode="nav"` renders a labelled `<nav>` with `aria-current="page"`. No grids of bordered cells, no wrapping.

### Banners and notices

`Notice` has two variants:

- `variant="banner"`: the page-level incident line, at most one per page. `rounded-md`, `tone/30` hairline, `tone/8` fill, 16px icon, one line: a 600-weight title plus a short summary, and at most one action on the right. Lists collapse into a sentence ("3 upstreams can't authenticate: isac-max, bear-max, bh322yoo-max").
- `variant="inline"` (default): a well inside a card, `tone/8` fill with no border, a 14px icon in the tone text color, body in `text-muted`.

Tones are `info` (neutral: overlay fill, muted icon), `success`, `warning` and `danger`. The object that owns the problem shows the full notice; everywhere else shows a dot and a short phrase.

### Empty states

- `EmptyState` has no frame of its own. In a card it is one centered 13px `text-muted` line with `py-8`; with a description or action the title steps up to `text-title-card`.
- The containing card collapses to its content. Dashed borders are only for drop zones.
- Metrics with no traffic show `—`, not a fake-precise `0 ms` or `0.00%`.

### Dialogs and drawers

- `Modal`: overlay surface, `text-title-section` title, `text-body-sm` muted description, body `p-4`, footer right-aligned.
- `ConfirmDialog`: `text-title-card` title, `text-body` muted description, `secondary` Cancel and `primary` confirm, or `danger-solid` when `destructive` (focus then lands on Cancel).
- `Drawer`: right-hand sheet (`width="md" | "lg" | "xl"`), full width below `sm`, `text-title-section` title. It fades instead of sliding under reduced motion.

### Changes that affect the pool

- Enabling or disabling an upstream or principal opens a `ConfirmDialog` that names the object and states the impact before the mutation runs.
- Other reversible pool-affecting settings (router strategy, cache-warm, keepalive) apply immediately and show `undoToast`, whose Undo re-applies the previous value through the same mutation.
- Deletion keeps the destructive `ConfirmDialog`.

## 6. Motion & Interaction

### Timing

| Type | Duration | Usage |
|------|----------|-------|
| Micro | 150–200ms | Hover, toggle, focus state |
| Standard | 200–300ms | Drawer, dialog, disclosure |

### Rules

- Animate only color, opacity or transform. The one looping animation is the 2s `live` status-dot pulse.
- Every interactive element stays keyboard-focusable with the 2px accent focus outline.
- No emoji or Unicode glyphs as icons; use lucide at `strokeWidth={1.75}` or text-only labels.
- Honor `prefers-reduced-motion`: pulses, shimmer and live-row flashes become static, drawers fade. Never use a global 0.01ms kill.
- Mobile (below `md`): request tables stack into three-line cards, filter panels sit behind a "Filters (n)" disclosure, and quota comes before traffic KPIs.

## 7. Depth & Surface

| Level | Treatment | When |
|-------|-----------|------|
| Canvas | `bg-bg` | The page |
| Flat section | No box: `text-title-section` title + content; 32px apart | Groups of peers; page-level tables with their own frame |
| Card | `.glass` (`panel` fill + 1px `border`) + `rounded-md` | A self-contained object with its own header or actions |
| Well | `.well` (`overlay-2` fill, `rounded-sm`, no border) + `p-3` | A sub-group inside a card: an inline notice, a credit strip, toggle rows |
| Divider | 1px `border-row` (`divide-y divide-row`, `border-t border-subtle`) | Between rows or sub-sections inside a card |
| Overlay | `.glass-strong` (`bg-sub` + 1px `border-strong` + `shadow-overlay`) + `rounded-md` | Popovers, menus, select popups. Dialogs and drawers use the same fill, border and shadow |

### Rules

- A bordered box never sits inside another bordered box. Inside a card, group with wells and dividers.
- Elevation is declared once: `shadow-overlay` (`0 12px 32px -12px`, 60% black in dark, 18% in light) and only on overlays. No glows, no blur.
- In light mode cards separate from the `#f7f7f8` canvas by their white fill as well as their hairline.
- A simplified warmup card shows one status story, one action cluster and one drawer entry point; everything else is summarized or moved behind disclosure.

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
- **Model Filter**: The Model input is a case-insensitive prefix match (`claude-sonnet` matches `claude-sonnet-4-5-20250929`), enforced identically by the historical SQL query, the live SSE fan-out and the retained-row filter. Typing is debounced so the URL, the history query and the SSE connection update only once input settles.
- **In-Progress Cost**: Once usage is observed for a priced model, partial rows and drawers show a token-derived `Estimated Cost`. This is display-only; authoritative final cost remains sourced from the termination-time `Priced` event used by billing and limit reconciliation.
- **Export Behavior**: The Export action includes all filtered retained rows, not just the current page.
- **Sentinel Behavior**: The backend infinite-scroll sentinel is only shown on the final client page, and it is still disabled when a session filter is active.
- **Pagination Controls**: Controls are accessible, featuring labeled Prev and Next buttons and an `aria-live` region for screen readers.
- **Backend Paging**: The exact 200, 200, 100 backend paging limits remain unchanged.
- **Live Tailing**: Tailing is automatically paused when a fixed custom time range is selected, and resumes based on user preference when returning to a relative preset or "All time".
