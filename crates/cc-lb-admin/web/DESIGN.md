# cc-lb Admin Web Design System

## 1. Identity

cc-lb admin is an instrument cluster for a quota pool. The question it answers first is how much is left: per window, what runs out first, and when it refills. Everything else (traffic, upstreams, principals, logs) is a drill-down from that reading.

- Headroom is the instrument. Every quota figure is written as what is **left**; used is secondary text.
- Readability comes before atmosphere. When a choice trades legibility for character, legibility wins.
- The ground is graphite at night and cool daylight gray by day. Surfaces sit one flat step above it, drawn with 1px lines. No blur, no glow, no decorative gradient.
- One brand hue, Ion violet, used at instrument scale only. Warn and danger are the only other hues; healthy is neutral ink.
- Hanken Grotesk is the voice of the product, including every number. Geist Mono is only for strings an operator copies or compares character by character.
- Show exceptions; let healthy states speak by omission.

## 2. Color

### Palette

Night is the default (`:root`, `[data-theme="dark"]`). Day applies with `[data-theme="light"]`, or with `[data-theme="system"]` when the OS prefers light. Both are defined in `@layer base` in `index.css` and registered through `@theme inline`.

| Role | Token | Night | Day | Usage |
|------|-------|-------|-----|-------|
| Ground | `--color-bg` | `#15171a` | `#e6e7ea` | Page, rail, top bar, tab bar, "More" sheet |
| Raised surface | `--color-bg-sub` | `#23272c` | `#f8f8f9` | Dialogs, drawers, popovers, menus, select popups, tooltips |
| Panel | `--color-panel` | `#1c1f23` | `#f1f2f4` | Card fill (`.glass`), sticky table header, nav hover |
| Panel strong | `--color-panel-strong` | `#23272c` | `#f8f8f9` | Active nav item, selected segment, secondary button hover |
| Input | `--color-input-bg` | `#15171a` | `#f8f8f9` | Inputs and select triggers |
| Toast | `--color-toast-bg` | `#23272c` | `#f8f8f9` | Sonner toasts |
| Line | `--color-border` (`border-subtle`) | `#2f343a` | `#c9ccd2` | Card edges, card header rule, top bar and rail edges, chart gridlines |
| Line strong | `--color-border-strong` (`border-subtle-strong`) | `#666d76` | `#7e838b` | Inputs, secondary buttons, overlays, gauge minor ticks, chart cursor |
| Row line | `--color-border-row` (`border-row`) | `#272b30` | `#d7d9de` | Table and list row dividers, dividers between peers |
| Meter track | `--color-progress-track` | `#2c3036` | `#cfd2d8` | Gauge and meter tracks, switch off |
| Overlay steps | `--color-overlay-{1..6}` | 2–8% white | 2–8% ink (`#17191c`) | Wells (`overlay-3`), neutral badges (`overlay-5`), cmdk selection |
| Hover | `--color-hover-bg` | 4% white | 4% ink | Row and ghost-button hover |
| Text | `--color-text` | `#eceef0` | `#17191c` | Primary text, headroom numerals |
| Text muted | `--color-text-muted` | `#b3b9c1` | `#464b52` | Secondary text, labels, used %, notice bodies |
| Text faint | `--color-text-faint` | `#959ca5` | `#565a61` | Captions, table headers, chart ticks, placeholders |
| Accent (Ion violet) | `--color-accent` | `#a493ff` | `#5a3fd6` | See "Brand violet" below |
| Accent dim | `--color-accent-dim` | `#a493ff` at 14% | `#5a3fd6` at 10% | Selected table row, text selection, live-row flash |
| Accent text | `--color-accent-text` | `#b3a6ff` | `#4c31c4` | Links, accent badge text, select check marks |
| Accent ink | `--color-accent-ink` | `#15171a` | `#ffffff` | Text and the switch knob on a solid accent fill |
| Healthy | `--color-ok` / `--color-success-text` | `#b3b9c1` | `#464b52` | Healthy dots and text: neutral ink, not green |
| Warning | `--color-warn` / `--color-warn-text` | `#f0b429` / `#f0b429` | `#8a5a00` / `#7a4f00` | Fills, dots, lines / text |
| Danger | `--color-danger` / `--color-danger-text` | `#ff6f61` / `#ff8a7e` | `#a61e30` / `#a61e30` | Fills, dots, lines / text |
| Danger solid | `--color-danger-solid` / `-hover` | `#c7372c` / `#b02f25` | `#a61e30` / `#8e1828` | White-text fill of `danger-solid` buttons |
| Neutral | `--color-neutral` | `#8f969f` | `#6b6f76` | Pending, skipped, disabled, connecting dots |
| Backdrops | `--color-modal-backdrop` / `--color-drawer-backdrop` | `#0a0b0d` at 72% / 60% | ink at 40% / 32% | Behind dialogs and drawers |
| Overlay shadow | `--color-overlay-shadow` | black at 50% | ink at 16% | The one elevation shadow |
| Scrollbar | `--color-scrollbar` / `-hover` | `#2f343a` / `#666d76` | `#c9ccd2` / `#7e838b` | WebKit scrollbar thumb |
| Window series | `--color-series-{5h,7d,fable,sonnet,opus,overage,unified}` | OKLCH L 0.58–0.88 | OKLCH L 0.40–0.62 | Quota window series in charts and legends |
| Token series | `--color-series-{input,output,cache-create-5m,cache-create-1h,cache-read}` | OKLCH L 0.60–0.78 | OKLCH L 0.40–0.56 | Token and cost slices |
| Chart fill | `--chart-fill-opacity` | `0.12` | `0.10` | Flat area fill under a series stroke |

Contrast (WCAG) holds on every surface in both themes: text at least 12.9:1, muted at least 6.9:1, faint at least 4.9:1; accent, warn and danger text at least 4.5:1 on the ground, on surfaces, on their own `/12` badge fill and on the selected-row fill; white on `danger-solid` at least 5.2:1. `border-strong` stays at least 3:1 against the ground as a non-text boundary.

Window series hues: 5h blue (235 night / 240 day), 7d teal (180 / 185), Fable green (145), Sonnet pale cyan (215 / 210), Opus magenta (330 / 350); overage and unified are neutral grays. No series sits within OKLab ΔE 0.08 of the accent, warn or danger in either theme. `lib/colors.ts#getWindowColor` returns them as `var()` references so charts follow the theme; unknown keys fall back to `unified`. Session chips hash into hue bands that stay clear of the same three tokens.

### Brand violet

Ion violet marks the instrument and the one thing to do next. It appears only as:

- gauge sweeps and headroom meter fills (while severity is healthy),
- the active nav rule (rail, bottom tab bar, command palette selection),
- the selected table row (`accent-dim` fill plus a 2px inset accent rule),
- the one `primary` button per view,
- focus rings, the switch "on" track, the live connection dot, tab underlines, accent badges and links (as `accent-text`).

It is never a KPI color, a sparkline color, a page decoration or a card fill. The KPI series and sparklines are `text-muted`.

### Rules

- Status colors mark state only: dots, badges, severity, incident notices, destructive actions. Healthy is neutral ink; there is no green and no blue. Informational notices are neutral.
- Status-colored text always uses the `*-text` tokens. Never use raw Tailwind palette classes (`text-red-400`, `zinc-*`) or hex literals in components.
- Series never use amber, orange or red hues; those mean severity.
- Window identity colors appear only in chart series and legends, never on a gauge, a meter or a quota numeral.
- No gradients, glows or brand-color surfaces.
- Tokens are registered through `@theme inline`, so every utility (`text-text`, `bg-overlay-3`, `border-subtle`, `bg-series-5h`) supports `hover:`, `focus-visible:`, `dark:` and `/opacity` variants. `dark:` follows the app theme (`data-theme`), not only the OS.
- For data colors computed in JS and used as text, use CSS `light-dark(<light>, <dark>)`.

### Headroom and severity

`lib/quotaSeverity.ts` is the only source of quota wording and color.

- The primary figure is headroom, written `N% left` (`formatHeadroom`). A bare percentage that could mean used is never shown. `formatHeadroomValue` gives the bare number for layouts that set `% left` apart, such as a gauge numeral.
- Used appears only as secondary text, `M% used` (`formatQuotaPercent` plus " used"), in `text-muted`.
- Rounding never lies: `0% left` only once the window is exhausted, `<1% left` for a sliver, never `100% left` once anything is used. No reading is `—`.
- Severity is judged on **used**: below 80% healthy, 80% and up `warn`, 95% and up `danger` (`QUOTA_WARN_PCT`, `QUOTA_DANGER_PCT`). In headroom terms that is warn below 20% left and danger below 5% left.
- Healthy headroom numerals are `text-text`; warn and danger numerals use `QUOTA_SEVERITY_TEXT_CLASS` (`warn-text`, `danger-text`). The sweep or fill turns `warn` / `danger` at the same thresholds.
- Rankings of quota ("Closest to limit") order by least left.

## 3. Typography

### Type roles

Each role is one Tailwind utility (`@theme` in `index.css`) that sets size, line height, weight and tracking together. All sizes are `rem`, so the browser font-size preference scales them. The body default is 14px / 1.5 sans with tabular lining numerals.

| Utility | Size / line height | Weight | Tracking | Use |
|---------|--------------------|--------|----------|-----|
| `text-display-hero` | 56 / 60 | 300 | -0.03em | The hero headroom numeral (pool dial, the top "Closest to limit" row) |
| `text-display` | 32 / 36 | 300 | -0.02em | Sub-gauge numerals and KPI values |
| `text-title-page` | 24 / 32 | 600 | -0.01em | `PageHeader` h1 when it is not already the top bar's page name; entity titles on detail panes. Sans, never mono |
| `text-title-section` | 15 / 22 | 600 | 0 | `Section` h2, top bar page name, sub-gauge labels, Modal and Drawer titles |
| `text-title-card` | 14 / 20 | 600 | 0 | `CardHeader`, confirm-dialog title, empty-state title, `N% left` on a `md` meter |
| `text-body` | 14 / 21 | 400 | 0 | Primary copy, table cells, dialog descriptions, notices |
| `text-body-sm` | 13 / 20 | 400 | 0 | Section and card subtitles, gauge captions, field-adjacent copy |
| `text-label` | 13 / 18 | 500 | 0 | Form labels, metric labels, `dl` keys, identity name. Sentence case |
| `text-caption` | 12 / 16 | 400 | 0 | Hints, timestamps, used %, badges (at 500), tab bar labels |
| `text-overline` | 12 / 16, uppercase | 500 | 0.08em | **Only** rail and command-palette group headings |
| `font-mono text-data` | 12 / 16 | 400 | 0 | IDs, hashes, key IDs, model IDs, error codes, paths, env vars, request IDs, code |
| `text-2xs` | 11 / 16 | — | — | **Only** the sidebar footer (version line) |

Table headers use the `table-header` utility: 12 / 16, 500, `text-faint`, sentence case.

### Font stack

- Sans: `Hanken Grotesk Variable`, `Hanken Grotesk`, `ui-sans-serif`, `system-ui`, `-apple-system`, `Segoe UI`, `sans-serif`.
- Mono: `Geist Mono Variable`, `Geist Mono`, `ui-monospace`, `JetBrains Mono`, `monospace`.

### Readability rules

- 12px is the floor. The one exception is the 11px sidebar footer; chart ticks, badges, captions and tab bar labels are all 12px or larger.
- Weights are 300, 400, 500 and 600. 300 is for display numerals only; 600 for titles; 500 for labels, buttons and emphasized numbers. No 700 (the base layer resets the UA `th` bold).
- Numbers (%, $, ms, counts, durations) are sans with tabular figures everywhere. Units sit in `text-muted`, at the same size in running text ("669 ms") and smaller only beside a display numeral (`59` + `% left`).
- Mono only for machine strings an operator copies or compares character by character. Never for times, dates, durations, names people chose, prose, labels or badges.
- Uppercase only through `text-overline`. Never uppercase a string that contains a unit, version or identifier (5h, 7d, 5m, v0.6.0).
- Sentence case for every heading, button, tab and label ("Recent requests", "Delete plugin", "Pool headroom"). Use plain words ("Admin address", not "Admin Addr").
- No eyebrow or kicker text above a heading.
- Every route renders exactly one `h1` through `PageHeader`. When its string title equals the page name the top bar already shows (`ShellPageTitleProvider`), the h1 stays for assistive tech but is visually hidden, so the name is never printed twice. `CardHeader` defaults to `h2`; cards nested under a titled section pass `headingLevel={3}`. Do not skip levels.
- Keep dashboard copy short; detailed debug text belongs in drawers.

## 4. Spacing & Layout

### Scale

All spacing is on a 4px base: 4, 8, 12, 16, 20, 24, 32, 40, 48, 64 (`1, 2, 3, 4, 5, 6, 8, 10, 12, 16`).

| Context | Value |
|---------|-------|
| Page padding (`PageContainer`) | 16 sides, 20 top, 40 bottom on phones; 32 sides and top from `md`; 40 sides and top, 64 bottom from `lg`. Max width 1440 |
| Viewport-fit consoles (`FullPage`) | 16 / 32 sides, 16 / 24 top; `h-shell` height from `md`; max width 1920 |
| Page header → first content | 32 (`PageHeader` carries `mb-8`) |
| Between page sections | 48, 64 from `lg` (`PageContainer` is `space-y-12 lg:space-y-16`) |
| Section title → content | 16 (`Section` is `gap-4`) |
| Card header / body | `px-4 py-3` with a 1px rule below / `p-4` |
| Table | header 36; rows 40 default, 32 dense; cells `px-3 py-2`, first and last cells 16 inset |
| Form | label → control 6, control → hint or error 4 |
| Shell | top bar 48; rail 208 (56 collapsed); bottom tab bar 56 plus the safe-area inset |
| Controls | buttons 28 (`sm`) / 32 (`md`) / 36 (`lg`); inputs 32 (`sm`) / 36 (`md`); nav items 36; segmented control 28 / 32 outer on desktop, taller below `md` |
| Touch | icon buttons and top bar controls are 44×44 below `md` |

### Layout rules

- Use `PageContainer` for route width and `Section` for page grouping. Separate sections with space, not rules or boxes.
- Cards collapse to one column on phones; avoid forced `flex-row` headers unless the content fits.
- A card is as tall as its content. No `min-h`, `h-[50vh]` or stretched grid cells to fill space.
- Prefer fewer groups with clear hierarchy over dense metric grids. Put raw logs and verbose diagnostics in drawers.
- Phones stay short: gauges become linear meters and repeated cards collapse into rows, so no page turns into a multi-thousand-pixel scroll.
- Check layouts at 390px, 768px and 1440px.

### Radii

| Token | Value | Use |
|-------|-------|-----|
| `rounded-xs` (`--radius-xs`) | 2px | Stacked bars, tiny tags, skeletons |
| `rounded-sm` (`--radius-sm`) | 4px | Buttons, inputs, badges, segments, nav items, menu items, in-card wells and inline notices |
| `rounded-md` (`--radius-md`) | 6px | Cards, dialogs, popovers, toasts, tooltips, page banners |
| `rounded-full` | — | Status dots, switches, the theme pill and the sidebar count pill. Never bars or tracks |

Headroom meter tracks and gauge strokes are square (butt caps, no radius).

## 5. Components

Primitives live in `src/components/ui/`: `primitives.tsx` (Card, Button, Badge, Notice, Field, …), `Gauge.tsx`, `charts.tsx`, `StackedBar.tsx`, `Select.tsx`, `Table.tsx`, `Tabs.tsx`. The shell lives in `src/components/layout/`.

### Shell

- **Rail** (`lg`+): 208px on the ground with a 1px right line, sticky to the viewport; collapses to 56px with ⌘B (the width snaps, labels fade). Brand mark (a small 240° headroom dial) and "cc-lb" at the top, nav groups with `text-overline` headings, then the footer (Docs link and the 11px version line).
- **Nav item**: 36px, 14/500 `text-muted`; hover `panel` fill. Active (`data-status="active"`) is `panel-strong` fill, full ink and a 2px accent rule on the rail's left edge. The Upstreams item carries the reconnect count pill (`warn` / `danger` at 15% with `*-text` numerals; a 6px dot when collapsed).
- **Top bar**: 48px, sticky, on the ground with a 1px bottom line. Left: the rail toggle (`lg`+) or the brand mark (below `lg`), then the page name in `text-title-section`. Right: Search (⌘K) field, identity (name and kind from `xl`, a popover below), API connection status (dot + word; the healthy "Live" word hides on phones, other states always show their word), and the Night / Day theme pill.
- **Bottom tab bar** (below `lg`): fixed, 56px plus the safe-area inset, on the ground with a 1px top line. Overview, Upstreams, Principals, Logs and More; 16px icons over 12px labels. The active tab is full ink with a 2px accent rule on its top edge. More opens the full navigation as a left sheet. Page titles stay in the top bar only; `main` pads by `--shell-bottom` and toasts sit above the bar.
- `h-shell` is the viewport minus the top bar and, below `lg`, the tab bar; use it for viewport-fit consoles.

### Gauges and meters

`Gauge.tsx` holds the two headroom instruments. Both draw what is **left**, in the accent, switching to `warn` / `danger` by used (80 / 95). Both are static SVG or CSS; nothing animates.

- **`ArcGauge`**: a 240° dial opening at the bottom. 1px ticks every 10% (major at 0, 50 and 100 in `text-faint`, minor in `border-strong`), 2px warn and danger zones just outside the dial at the low-headroom end, the sweep from zero to the headroom, and a 1.5px marker at the sweep's end. The numeral is a light display number with a small `% left`.
  - `size="hero"`: 264px, label and caption inside the dial. One per page: the pool dial on Overview.
  - `size="sub"`: 128px, label (`text-title-section`) and caption (`text-body-sm` muted) below. Per-window gauges.
  - One `role="img"` with the name `"<label>: N% left"` plus the caption when it is a string; pass `ariaLabel` when the caption is not plain text ("5h: 59% left, resets Thu 10-01 09:00").
- **`HeadroomMeter`**: the linear sibling for lists and phones. Fill = headroom in the accent (warn / danger by used) on the meter track, with a 1px `text-faint` mark where the warn zone starts (20% left). `sm` is a 4px bar for dense rows; `md` is a 6px bar with `N% left` (`text-title-card`) and `M% used` (`text-caption` muted) above it. `role="meter"` valued as headroom, so assistive tech hears "59% left".
- Below `md`, every dial becomes a `HeadroomMeter`.
- Quota is only ever shown through these two components. Window identity colors never fill them.
- **`StackedBar`**: breakdowns (latency attribution, token usage), not quota. 6px (`sm`) or 8px (`md`), `rounded-xs` track, square segments in a `gap-px` row with `min-width: 2px`, so the track shows between them. Renders nothing when every segment is zero. Breakdowns live in drawers and popovers, never in table cells.

### Card and Section

- **Card** (`Card` + optional `CardHeader` + `CardBody`): a self-contained object with its own header or actions — a chart, a table, a form group, a detail block. Flat `panel` fill, 1px line, `rounded-md`. `CardHeader` owns `px-4 py-3`, a 1px rule below, and a `text-title-card` heading with an optional `text-caption` subtitle; `CardBody` owns `p-4`.
- **Section** (`Section`): a flat page region on the ground with a `text-title-section` h2, optional `text-body-sm` muted subtitle, and no box. Use it for groups of peers (window gauges, the upstream strip, "Used by" lists) and around page-level tables that already have a frame.
- A section title and a card title never share a size.

### Button

| Variant | Look | Use |
|---------|------|-----|
| `primary` | Solid accent with `accent-ink` text; brighter on hover | One per view: the commit action (Save, Create, Continue). Not "New" in list headers |
| `secondary` (default) | Transparent with a 1px `border-strong` line; `panel-strong` on hover | Toolbars, paired actions, most actions |
| `ghost` | No fill or line; `text-muted` → `text` with a hover fill | Tertiary actions, in-row links |
| `danger` | Outline: 1px `danger` line, `danger-text`, hover `danger/10` | Page-level Delete / Revoke |
| `danger-solid` | `danger-solid` fill, white text, darker fill on hover | Only the confirm button of a destructive `ConfirmDialog` |

- Sizes: `sm` 28px / 12px text (tables, toolbars), `md` 32px / 13px (default), `lg` 36px / 14px (dialog footers, mobile).
- Icons are 14px in `sm`/`md` and 16px in `lg`, enforced by the Button; use lucide at `strokeWidth={1.75}`.
- Disabled is 40% opacity of the same variant. A disabled `primary` renders as a disabled `secondary` until it becomes actionable.
- Paired or sibling actions share a variant and size. No full-width buttons inside desktop cards.
- A toggled toolbar action ("Stop tail") is `secondary` with `aria-pressed`, plus a live dot while active.

### IconButton

Every icon-only control. `label` is required and becomes the accessible name. 44×44 below `md`, 32×32 from `md`; the glyph is 16px.

### Badge and StatusBadge

- `Badge`: 20px tall, 12/500 sans sentence case, `px-1.5`, `rounded-sm`. Tones: `neutral` and `ok` (`overlay-5` fill, neutral text); `warn`, `danger` (tone/12 fill, `*-text`); `accent` (an outlined chip: 1px accent line, `accent-text`, no fill — e.g. "binds pool"); `mono` (`font-mono text-data` on `overlay-4`, for model IDs and key IDs only).
- `StatusBadge`: a status dot plus a `text-label` phrase ("Enabled", "Reconnect required"). Healthy tones stay `text-muted`; `warn`/`danger` use their `*-text`. No chip, no mono, no uppercase.
- At most one status badge per object header. In lists, status is a dot plus a short `*-text` phrase, not a filled chip. Healthy states are shown by omission (no "Active" ×12).
- Status dot: `.status-dot` 6px (`.status-dot.lg` 8px in lists), flat and static. `live` is an accent dot that does not pulse; state is always carried by text as well.

### Tables

Use `Table`, `TableHead`, `TableHeadCell`, `TableRow`, `TableCell`, `TableEmptyRow` and `EmptyValue` from `Table.tsx`, or the `table-header` (on `thead`) and `table-th` (on `th`) utilities for existing markup.

- Header: 12/500 sans sentence case in `text-faint`, 36px, opaque sticky `panel` with a 1px line. No uppercase, no tracking, no blur.
- Body: `text-body` 14px sans. Mono (`TableCell mono`) only for ID, model, path and hash columns. Numbers right-aligned and tabular (`numeric`), units muted at the same size.
- Rows: 40px (32px `dense`), `border-row` divider, `hover-bg` on interactive rows. Selected is `accent-dim` with a 2px inset accent rule on the left edge — the one place a row carries a colored edge.
- No per-cell bars except a `HeadroomMeter sm` in a headroom column. Breakdowns go in the drawer.
- Hide a column when every visible row is null; show a null cell as `—` in `text-faint` (`EmptyValue`, with a screen-reader label).
- A table inside a card drops the card body padding; the first and last cells keep a 16px inset so the header aligns with the card title.

### Charts (Recharts)

`charts.tsx` exports the shared chrome; spread it into Recharts parts rather than restyling per chart.

- `CHART_GRID` (`vertical: false`, stroke `border`): 1px crisp horizontal gridlines only. `index.css` enforces the stroke and hides axis and tick lines everywhere.
- `CHART_AXIS`: no axis or tick lines, 12px sans tabular ticks in `text-faint`. About 4 x-ticks.
- `CHART_CURSOR`: a 1px `border-strong` hover rule.
- Tooltip: `bg-sub`, 1px `border-strong`, `rounded-md`, the overlay shadow, 12px sans; label `text-muted`, values `text`, tabular.
- **Quota history plots headroom**, the same way every gauge reads: each bucket is `100 − used`, clamped to 0–100 (`buildPoolQuotaChartData`, the upstream quota chart). The y axis is `0–100` with ticks at 0, 25, 50, 75 and 100, labeled `N%`; higher is better.
- Thresholds (`CHART_THRESHOLD`): 1px dashed (`3 3`) `warn` and `danger` reference lines at **20% left** and **5% left** (`100 − QUOTA_WARN_PCT`, `100 − QUOTA_DANGER_PCT`), labeled "20% left" / "5% left" or listed in the legend as "Warn below 20% left" / "Danger below 5% left". No tinted `ReferenceArea` bands.
- Series colors come from `--color-series-*` via `getWindowColor`; 1.5px strokes. Quota windows draw as lines without fill (7d (Fable) dashed `5 3` where it shares a plot with 7d); areas elsewhere use a flat fill at `SERIES_FILL_OPACITY`, never a gradient. When series overlap, fill only the primary or hovered series.
- Legend: in the chart header row or directly under the plot: a short line swatch (dashed for thresholds), a `text-muted` label and a tabular value. A legend entry may isolate its window.
- `Sparkline`: a 1.25px `text-muted` line over a flat fill; renders nothing when every value is zero. `QuotaMiniChart`: 1.5px step lines for 5h and 7d, no fill. Chart animation is off.
- Histograms: neutral bars with errors stacked in `danger`. No donuts: use a `StackedBar` plus a value table.
- A chart is one static image to assistive tech: pass an `ariaLabel` summary, or hide it when the surrounding element already names it.

### Forms

- `Field`: `text-label` sentence-case label in `text-muted` above the control (6px), `text-caption` hint in `text-faint` or error in `danger-text` below (4px). A required field shows a `danger-text` asterisk. Native `input`/`select`/`textarea` children get `required`, `aria-invalid` and `aria-describedby` automatically.
- Inputs: `INPUT_CLASS` (36px) or `INPUT_SM_CLASS` (32px): `rounded-sm`, `input-bg`, 1px `border-strong`, accent line on focus plus the 2px accent focus outline at 1px offset. Placeholder in `text-faint`.
- `Select` (`Select.tsx`) is the only select control; never a native `<select>`. `allLabel` adds the `''` "all" item for filter bars; otherwise `placeholder` shows until a value is chosen. Width comes from `className`; the popup matches the trigger and uses the overlay surface, with an `accent-text` check column reserved so labels never shift. Pass `aria-label` when no `Field` names it.
- Disclosure: a custom `<summary>` with `list-style: none` and a 12px rotating lucide chevron.
- `ToggleSwitch`: 36×20 switch over a native checkbox with an accessible name. Off is the meter track with a `border-strong` line and a muted knob; on is a solid accent track with an `accent-ink` knob. Label is `text-body-sm` sans. The default `card` variant is an in-card well (fill, no border); `compact` sits inline in a header row.
- `SegmentedControl`: at most 5 short options. Radiogroup with roving focus; 1px `border-strong` outline, selected segment is `panel-strong` fill with full ink. Use it for every small exclusive choice (time presets, window toggles); do not hand-roll chip rows.
- `Tabs` (`Tabs.tsx`): underline tabs, 14/500, 36px row over a 1px line, a square 2px accent rule under the active item, one horizontally scrolling line with an edge fade. `mode="tabs"` is an ARIA tablist (pair panels with `tabPanelProps`); `mode="nav"` renders a labelled `<nav>` with `aria-current="page"`. No grids of bordered cells, no wrapping.

### Banners and notices

`Notice` has two variants:

- `variant="banner"`: the page-level incident line, at most one per page. `rounded-md`, 1px `tone/45` line whose left edge is drawn in the full tone, `tone/8` fill, 16px icon, one line: a 600-weight title plus a short summary, and at most one action on the right. Lists collapse into a sentence ("3 upstreams can't authenticate: isac-max, bear-max, bh322yoo-max").
- `variant="inline"` (default): a well inside a card, `tone/8` fill with no border, a 14px icon in the tone text color, body in `text-muted`.

Tones are `info` and `success` (both neutral: overlay fill, muted icon), `warning` and `danger`. The object that owns the problem shows the full notice; everywhere else shows a dot and a short phrase.

### Empty states

- `EmptyState` has no frame of its own. In a card it is one centered 13px `text-muted` line with `py-8`; with a description or action the title steps up to `text-title-card`.
- The containing card collapses to its content. Dashed borders are only for drop zones.
- Metrics with no traffic show `—`, not a fake-precise `0 ms` or `0.00%`. A quota with no reading shows `—` and an empty dial or track, never `0% left`.

### Dialogs and drawers

- `Modal`: raised surface (`bg-sub`, 1px `border-strong`, overlay shadow, `rounded-md`), `text-title-section` title, `text-body-sm` muted description, body `p-4`, footer right-aligned.
- `ConfirmDialog`: `text-title-card` title, `text-body` muted description, `secondary` Cancel and `primary` confirm, or `danger-solid` when `destructive` (focus then lands on Cancel). While the confirmed action is pending, backdrop clicks, Escape and Cancel cannot close it.
- `Drawer`: right-hand sheet (`width="md" | "lg" | "xl"`) on the raised surface with a 1px `border-strong` left line, full width below `sm`, `text-title-section` title. It fades instead of sliding under reduced motion.

### Changes that affect the pool

- Enabling or disabling an upstream or principal opens a `ConfirmDialog` that names the object and states the impact before the mutation runs.
- Other reversible pool-affecting settings (router strategy, cache-warm, keepalive) apply immediately and show `undoToast`, whose Undo re-applies the previous value through the same mutation.
- Deletion keeps the destructive `ConfirmDialog`.

## 6. Motion & Interaction

### Timing

| Type | Duration | Usage |
|------|----------|-------|
| Micro | 150ms | Hover, focus, theme switch, rail label fade, dialog open (opacity + scale) |
| Standard | 200ms | Drawer slide, backdrop fade |
| Attention | 1–1.8s | Live-row flash (`flash-in`), text settle, skeleton shimmer |

### Rules

- Animate only color, opacity or transform. Width never animates: the rail width snaps and only its labels fade.
- Instruments are still. Gauges, meters and charts do not animate, and the live dot does not pulse; nothing loops except the skeleton shimmer while loading.
- Every interactive element stays keyboard-focusable with the 2px accent focus outline.
- No emoji or Unicode glyphs as icons; use lucide at `strokeWidth={1.75}` or text-only labels.
- Honor `prefers-reduced-motion`: shimmer and live-row flashes become static, drawers fade instead of sliding, switches and the "More" sheet stop transitioning. Never use a global 0.01ms kill.
- Mobile (below `md`): request tables stack into three-line cards, filter panels sit behind a "Filters (n)" disclosure, dials become linear meters, and quota comes before traffic KPIs.

## 7. Depth & Surface

| Level | Treatment | When |
|-------|-----------|------|
| Ground | `bg-bg` | The page, rail, top bar and tab bar |
| Flat section | No box: `text-title-section` title + content, separated from the next section by space | Groups of peers; the cluster band; page-level tables with their own frame |
| Card | `.glass` (`panel` fill + 1px `border`) + `rounded-md` | A self-contained object with its own header or actions |
| Well | `.well` (`overlay-3` fill, `rounded-sm`, no border) + `p-3` | A sub-group inside a card: an inline notice, a credit strip, toggle rows |
| Divider | 1px `border-row` (`divide-y divide-row`, `border-t border-row`) | Between rows, between peer gauges in a band, or between sub-sections inside a card |
| Overlay | `.glass-strong` (`bg-sub` + 1px `border-strong` + `shadow-overlay`) + `rounded-md` | Popovers, menus, select popups, tooltips, toasts. Dialogs and drawers use the same fill, line and shadow |

### Rules

- Everything is flat. Surfaces step up from the ground by fill and a 1px line, never by blur, glow, gradient or a drop shadow.
- Elevation is declared once: `shadow-overlay` (`0 8px 24px -12px` in the overlay-shadow color) and only on overlays.
- Separate sections with space, not borders. Page regions are not boxed unless they are a self-contained object.
- A bordered box never sits inside another bordered box. Inside a card, group with wells and dividers.
- No side-stripe borders to mark a card, row or list item as special; use a leading dot, an icon or a badge. The only colored edges are the 2px accent rules on the active nav item, the active tab and the selected table row, and the full-tone 1px left edge of a page banner.
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
