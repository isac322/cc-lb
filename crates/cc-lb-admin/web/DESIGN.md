# cc-lb Admin Web Design System

## 1. Identity

cc-lb admin is an instrument cluster for a quota pool. The question it answers first is how quota usage is moving: per window, how much is used, what is closest to the limit, and when it resets. Everything else (traffic, upstreams, principals, logs) is a drill-down from that reading.

- Usage is the instrument. Every quota figure is written as what is **used** (`N% used`), the same utilization Claude reports, so it compares with Claude's own screens without conversion. Usage over time comes before any single current value.
- Readability comes before atmosphere. When a choice trades legibility for character, legibility wins.
- The ground is a neutral graphite at night and a pale graphite by day. Surfaces sit one flat step above it, drawn with 1px lines. No blur, no glow, no decorative gradient.
- One accent, near-monochrome: a silver with a faint blue whisper at night, a deep ink by day, used at instrument scale only. Hue lives in the data; explicit healthy/success status is green, warn and danger are the severity hues, and healthy-by-default states may remain omitted.
- Hanken Grotesk is the voice of the product, including every number. Geist Mono is only for strings an operator copies or compares character by character.
- Show exceptions; omit repeated healthy-by-default labels, while explicit success and live states use their semantic tones.

### Brand

The brand mark is the let-gate: two squared bracket "c" shapes facing each other so they form a gate, with one lane passing straight through it. It replaces the old open dial and is drawn by `BrandMark` (`components/layout/Sidebar.tsx`) in the rail, the top bar and the sign-in frame. The repository brand kit lives in `assets/brand/` (master marks, favicon set, app icons, lockups, README hero, social preview) with geometry and palette in `assets/brand/tools/brand.py`; the usage rules are in `assets/brand/README.md`.

- The brackets draw in `text` ink; only the lane draws in `accent` — it is the one place the accent appears as part of identity.
- Flat graphite always: no gradients, glows, shadows or decorative noise, in any asset.
- Below 24px the mark uses the 16-grid snapped variant (`mark/cc-lb-mark-16-*.svg`), which is what the favicon ships; 16px is the minimum rendered size.

## 2. Color

### Palette

Graphite is the one scheme: a near-achromatic graphite ground, a near-monochrome accent (silver with a faint blue whisper at night, deep ink by day), with hue carried only by the data — severity, series and categorical chips. Night is the default (`:root`, `[data-theme="dark"]`). Day applies with `[data-theme="light"]`, or with `[data-theme="system"]` when the OS prefers light. Both are defined in `@layer base` in `index.css` and registered through `@theme inline reference`.

| Role | Token | Night | Day | Usage |
|------|-------|-------|-----|-------|
| Ground | `--color-bg` | `#161719` | `#e3e5e7` | Page, rail, top bar, tab bar, "More" sheet |
| Raised surface | `--color-bg-sub` | `#232527` | `#f7f8f9` | Dialogs, drawers, popovers, menus, select popups, tooltips |
| Panel | `--color-panel` | `#1c1e20` | `#eeeff1` | Card fill (`.glass`), sticky table header, nav hover |
| Panel strong | `--color-panel-strong` | `#232527` | `#f7f8f9` | Selected segment, secondary button and theme pill hover, pressed toolbar toggle |
| Input | `--color-input-bg` | `#161719` | `#f7f8f9` | Inputs and select triggers |
| Toast | `--color-toast-bg` | `#232527` | `#f7f8f9` | Sonner toasts |
| Line | `--color-border` (`border-subtle`) | `#2f3033` | `#cacccf` | Card edges, card header rule, top bar and rail edges, chart gridlines |
| Line strong | `--color-border-strong` (`border-subtle-strong`) | `#6a6c6f` | `#7e8084` | Inputs, secondary buttons, overlays, chart cursor |
| Row line | `--color-border-row` (`border-row`) | `#26282a` | `#d7d9dc` | Table and list row dividers, dividers between peers |
| Meter track | `--color-progress-track` | `#2c2d2f` | `#d1d3d5` | Usage meter and stacked-bar tracks, switch off |
| Overlay steps | `--color-overlay-{1..6}` | 2–8% white | 2–8% ink (`#18191c`) | Wells (`overlay-3`), neutral badges (`overlay-5`), list-row and nav hover (`overlay-2`) |
| Hover | `--color-hover-bg` | 4% white | 4% ink | Interactive table row hover |
| Selected | `--color-selected` (`bg-selected`) | 8% white | `#18191c` at 7% | Selected list and table rows, active nav item and tab bar item, command-palette selection |
| Text | `--color-text` | `#edeeee` | `#18191c` | Primary text, healthy quota figures |
| Text muted | `--color-text-muted` | `#c0c1c3` | `#484a4e` | Secondary text, labels, used %, notice bodies |
| Text faint | `--color-text-faint` | `#9d9ea0` | `#595b5e` | Captions, table headers, chart ticks, placeholders |
| Text disabled | `--color-text-disabled` | `#626365` | `#8a8c90` | Disabled control labels and icons (`control-disabled`) |
| Border disabled | `--color-border-disabled` | `#4a4c4e` | `#afb1b4` | The dashed line of a disabled bordered control |
| Accent (silver / ink) | `--color-accent` | `#ccddee` | `#1f2c3d` | See "Accent" below |
| Accent hover | `--color-accent-hover` | `#dceaf8` | `#142030` | Fill and line of a hovered `primary` button |
| Accent dim | `--color-accent-dim` | `#ccddee` at 14% | `#1f2c3d` at 10% | Text selection, live-row flash, calendar day selection, time-strip wash |
| Accent text | `--color-accent-text` | `#779cc2` | `#0a345a` | Links, accent badge text, select check marks |
| Accent ink | `--color-accent-ink` | `#181a1c` | `#ffffff` | Text and the switch knob on a solid accent fill |
| Healthy status | `--color-traffic-success` / `--color-traffic-success-text` | green | green | Displayed success and healthy dots/text; `--color-ok` remains graphite for metric fills such as healthy quota and pace bars |
| Warning | `--color-warn` / `--color-warn-text` | `#f0b429` / `#f0b429` | `#8a5a00` / `#7a4f00` | Fills, dots, lines / text |
| Danger | `--color-danger` / `--color-danger-text` | `#ff6f61` / `#ff8a7e` | `#a61e30` / `#a61e30` | Fills, dots, lines / text |
| Danger solid | `--color-danger-solid` / `-hover` | `#c7372c` / `#b02f25` | `#a61e30` / `#8e1828` | White-text fill of `danger-solid` buttons |
| Neutral | `--color-neutral` | `#939495` | `#6d6f72` | Pending, skipped, disabled, connecting and unknown dots when no explicit success/live tone applies |
| Backdrops | `--color-modal-backdrop` / `--color-drawer-backdrop` | `#060708` at 72% / 60% | ink at 40% / 32% | Behind dialogs and drawers |
| Overlay shadow | `--color-overlay-shadow` | black at 50% | ink at 16% | The one elevation shadow |
| Scrollbar | `--color-scrollbar` / `-hover` | `#2f3033` / `#6a6c6f` | `#cacccf` / `#7e8084` | WebKit scrollbar thumb |
| Window series | `--color-series-{5h,7d,fable,sonnet,opus,overage}` | `#63b1f9` / `#cd5cac` / `#bbe75f` / `#629092` / `#91745d` / `#a3a5a8` | `#0c82bf` / `#8d1071` / `#486a00` / `#39555c` / `#6a5647` / `#616366` | Quota window series in charts, legends and window swatches |
| Token series | `--color-series-{cache-read,cache-create-5m,cache-create-1h,input,output}` | `#24c27d` / `#e48d28` / `#cc612e` / `#96a6bb` / `#997fde` | `#278a47` / `#cc8d16` / `#bf5b0d` / `#657689` / `#613ea6` | Token and cost slices, the Tokens KPI's cache-miss line |
| Latency series | `--color-series-latency-{downstream,cclb,net,wait}` | `#c56f9b` / `#5d98f4` / `#6f9436` / `#6dd9d6` | `#6b2f57` / `#234f99` / `#547d2b` / `#279592` | Latency responsibility segments (`bg-series-latency-*`) |
| Chart fill | `--chart-fill-opacity` | `0.12` | `0.10` | Sparkline area fill; quota gradients start at 2× this |

Contrast (WCAG) holds on every surface in both themes: text at least 13:1, muted at least 6.9:1, faint at least 5.4:1; accent, warn and danger text at least 4.5:1 on the ground, on surfaces, on their own `/12` badge fill and on the selected-row fill; white on `danger-solid` at least 5.2:1. `border-strong` stays at least 3:1 against the ground as a non-text boundary. `text-disabled` is deliberately lower — about 2.9:1 — legible but a clear step below `text-faint`, so an unavailable control reads as such at a glance.

**Token composition** (`components/ui/usage/sliceColors.ts#USAGE_CATEGORIES`) is the one category → color list for every token and cost bar, popover and breakdown: request-table cells, the request drawer, and the Overview principal cost popovers. Cache read is green, the good outcome (the prompt came from cache); cache create is amber (5m) and burnt orange (1h), the costly miss — conspicuous but never red, because it is not an error; both sit deeper than the `warn` amber so a cache write never reads as a warning. Uncached input is a quiet slate; output is violet. Segments always draw in that fixed order: cache read, cache create 5m, cache create 1h, input, output. Cache read leads so the green run from the left edge reads directly as the hit, and both cache outcomes sit side by side. A mostly green bar is a high cache hit; amber or orange is cache writes. Cost bars use the same categories, colors and order (`costCategories.ts`); cost no category accounts for is a neutral `Unattributed` slice.

**Quota windows** (`lib/colors.ts#getWindowColor`, returned as `var()` references so charts follow the theme): the three live windows are 5h sky, 7d magenta and 7d (Fable) lime; the legacy 7d (Sonnet) and 7d (Opus) windows are muted teal-gray and brown-gray; `overage` (Extra usage) is a neutral gray, and unknown keys fall back to `--color-neutral`. The 5h sky sits bluer than before, away from the warm severity band. The live three must stay apart under protan, deutan and tritan simulation (target OKLab ΔE ≥ 0.12 pairwise) and clear of the warn and danger tokens, so a window line never reads as a threshold. `ApiUsageCard` reuses these tokens for model families (Opus → opus, Sonnet → sonnet, Haiku → 5h, Fable → fable). The backend's `unified` account-restriction envelope is not a web-visible window — it is never requested, listed or drawn here — so it has no series token; its provider signals still feed routing behind the API.

**Latency** responsibility segments (Downstream, cc-lb, Upstream net, Upstream wait; renewals use cc-lb) have their own `--color-series-latency-*` tokens, used only by latency bars and breakdowns: downstream magenta, cc-lb blue, upstream net lime, upstream wait teal. The four differ in lightness as well as hue, so any two stay at least OKLab ΔE 0.13 apart, and 0.1 under protan, deutan and tritan simulation. In the drawer's request timeline, internal stages take the cc-lb hue and upstream stages the upstream-wait hue; stages within one group step through a fixed lightness × hue offset table (`STAGE_SHADE_OFFSETS`) so chronological neighbours stay at least ΔE 0.13 apart.

**Categorical chips** use `categoricalColor(hue)` (`lib/colors.ts`): text at OKLCH L 0.42 by day / 0.83 at night (≥ 4.5:1 on the chip's own tint over every table surface, including the selected row), a series-strength dot, and a 14% / 18% tint. The accent is near-achromatic, so chips can use most of the wheel: session chips draw from a fixed palette of nine hues (`SESSION_HUES`: 110, 135, 200, 230, 255, 280, 305, 330, 355; ≥ 21° apart) that skips only the warm band ~5–100 (danger, warn and the cache-write series, and the khaki tint they create next to warn), with 355 as the pink end nearest danger. A session keeps the slot it was first given: a new session takes its hashed slot (FNV-1a), or the least-used slot along the probe order when that one is taken — the first nine sessions seen never share a hue, and past that each hue carries an even share. Assignment is per page session, so a session's chip is stable for a sitting but may change on reload. Request kinds (`lib/requestKind.ts`) have fixed hues ≥ 21° apart, every badge color at least OKLab ΔE 0.08 from warn, danger and the cache-write series in both themes — side 112, session title 135, notification 200, subagent 235, look at 258, compaction 281, auto thinking 304, advisor 327, recap 350; custom kinds hash into the same safe arcs (108–170 and 195–358). `main`, the bulk of traffic, is the quiet neutral badge (`overlay-5`, muted text) and `unknown` an outlined dashed neutral badge.

### Accent

The accent is near-monochrome — a silver with a faint blue whisper at night, a deep ink by day — so it marks the instrument by fill and position, not by hue: the solid fills (the `primary` button, the switch "on" track) and the focus ring. It appears only as:

- the brand mark,
- the one `primary` button per view,
- focus rings, the switch "on" track, tab underlines, accent badges and links (as `accent-text`). Displayed connection and feed `Live` indicators use the semantic `ok` green instead of the accent.

Selection stays neutral (`--color-selected`), never an accent tint. The accent is never a KPI color, a sparkline color, a chart series, a page decoration or a card fill. The KPI series and sparklines are `text-muted`; the Tokens KPI's second series is the cache-create amber, and the error-rate sparkline turns `danger` only with errors.

### Rules

- Status colors mark state only: dots, badges, severity, incident notices, destructive actions. Explicit healthy or successful status uses the green traffic-success tokens; `warn` is amber and `danger` is red. Quota and pace metrics remain graphite when healthy or on pace, and categorical, disabled, revoked and unknown states remain neutral.
- Status-colored text always uses the `*-text` tokens. Never use raw Tailwind palette classes (`text-red-400`, `zinc-*`) or hex literals in components.
- Amber, orange and red mean severity. The one deliberate exception is cache create in the token series (amber 5m, orange 1h): a cache write is the costly outcome, so it should stand out, but it is never red. Quota window series, session chips and request-kind badges stay clear of warn and danger.
- Window identity colors appear only in chart series and legends, never on a meter or a quota numeral.
- No glows or brand-color surfaces. The only gradient is data: the vertical fill under a quota chart's live windows (`SeriesFillGradient`).
- Tokens are registered through `@theme inline reference`, so every utility (`text-text`, `bg-overlay-3`, `border-subtle`, `bg-series-5h`) supports `hover:`, `focus-visible:`, `dark:` and `/opacity` variants. `dark:` follows the app theme (`data-theme`), not only the OS.
- For data colors computed in JS and used as text, use CSS `light-dark(<light>, <dark>)`.

### Usage and severity

`lib/quotaSeverity.ts` is the only source of quota wording and color.

- The primary figure is what is used, written `N% used`: `formatQuotaPercent` gives the number and the layout adds "used" (as a muted word, or `sr-only` where a compact row shows only `N%`). `left` and `headroom` are never used for quota, and charts never invert usage.
- Rounding never lies (`formatQuotaPercent`): whole percent everywhere, lists, cards and chart tooltips alike. A non-zero reading below 1% is `<1%`, never `0%`; a reading from 99% up to 100% is `99%`, so `100%` means exhausted. No reading (null or non-finite) is `—`.
- Severity (`quotaSeverity`) is pace-relative: usage far ahead of even pace reads worse than the same figure on pace. `used` at 95% and up is always `danger` (`QUOTA_DANGER_PCT`). Without a pace reading — untimed windows such as Extra usage, or a window not running — severity falls back to the absolute rule: `warn` from 80% (`QUOTA_WARN_PCT`), `danger` from 95%. With pace, `danger` when used runs 30+ points ahead (`QUOTA_PACE_DANGER_GAP`), `warn` when used is 90%+ (`QUOTA_PACE_WARN_PCT`) or 10+ points ahead (`QUOTA_PACE_WARN_GAP`), `ok` otherwise.
- Numerals: healthy is `text-text`; warn and danger use `QUOTA_SEVERITY_TEXT_CLASS` (`text-warn-text`, `text-danger-text`); no reading is `text-text-faint`. Compact per-window facts in a list caption stay `text-muted` while healthy.
- Meter fills follow the same severity: `text-muted` ink while healthy, the `warn` and `danger` fills past those marks. Never the accent, never a window color.
- Rankings order by the most-used window, highest first; a row without a reading sorts last in its group.
- API-key upstreams have no subscription quota. The Overview usage table spans the window columns with `No subscription quota` (`text-body-sm` muted); the upstream list shows no figure and no window facts, only `API key` as the caption.

## 3. Typography

### Type roles

Each role is one Tailwind utility (`@theme` in `index.css`) that sets size, line height, weight and tracking together. All sizes are `rem`, so the browser font-size preference scales them. The body default is 14px / 1.5 sans with tabular lining numerals.

| Utility | Size / line height | Weight | Tracking | Use |
|---------|--------------------|--------|----------|-----|
| `text-display` | 32 / 36 | 300 | -0.02em | KPI values and single headline figures in detail panes |
| `text-title-page` | 24 / 32 | 600 | -0.01em | `PageHeader` h1 when it is not already the top bar's page name; entity titles on detail panes. Sans, never mono |
| `text-title-section` | 15 / 22 | 600 | 0 | `Section` h2, top bar page name, quota window names on upstream detail, Modal and Drawer titles |
| `text-title-card` | 14 / 20 | 600 | 0 | `CardHeader`, confirm-dialog title, empty-state title, `N% used` on a `md` meter |
| `text-body` | 14 / 21 | 400 | 0 | Primary copy, table cells, dialog descriptions, notices |
| `text-body-sm` | 13 / 20 | 400 | 0 | Section and card subtitles, window captions, field-adjacent copy |
| `text-label` | 13 / 18 | 500 | 0 | Form labels, metric labels, `dl` keys, identity name. Sentence case |
| `text-caption` | 12 / 16 | 400 | 0 | Hints, timestamps, reset captions, list-row captions, badges (at 500), tab bar labels |
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
- Numbers (%, $, ms, counts, durations) are sans with tabular figures everywhere. Units sit in `text-muted`, at the same size in running text ("669 ms") and one step smaller only beside an emphasized figure (`87%` + `used`).
- Mono only for machine strings an operator copies or compares character by character. Never for times, dates, durations, names people chose, prose, labels or badges.
- Uppercase only through `text-overline`. Never uppercase a string that contains a unit, version or identifier (5h, 7d, 5m, v0.6.0).
- Sentence case for every heading, button, tab and label ("Recent requests", "Delete plugin", "Pool quota usage"). Use plain words ("Admin address", not "Admin Addr").
- No eyebrow or kicker text above a heading.
- Every route renders exactly one `h1` through `PageHeader`. When its string title equals the page name the top bar already shows (`ShellPageTitleProvider`), the h1 stays for assistive tech but is visually hidden, so the name is never printed twice. `CardHeader` defaults to `h2`; cards nested under a titled section pass `headingLevel={3}`. Do not skip levels.
- Keep dashboard copy short; detailed debug text belongs in drawers.

## 4. Spacing & Layout

### Scale

All spacing is on a 4px base: 4, 8, 12, 16, 20, 24, 32, 40, 48, 64 (`1, 2, 3, 4, 5, 6, 8, 10, 12, 16`).

| Context | Value |
|---------|-------|
| Page padding (`PageContainer`) | 16 sides, 20 top, 40 bottom on phones; 32 sides and top from `md`; 40 sides and top, 64 bottom from `lg`. Max width 90rem (1440px at the default font size) |
| Viewport-fit consoles (`FullPage`) | 16 / 32 sides, 16 / 24 top; `h-shell` height from `md`; max width 1920. Logs and Audit narrow it to 90rem with 40 sides from `lg` (`lg:max-w-[90rem] lg:px-10`), so every page shares one content width and edge |
| Page header → first content | 32 (`PageHeader` carries `mb-8`) |
| Between page sections | 48, 64 from `lg` (`PageContainer` is `space-y-12 lg:space-y-16`) |
| Section title → content | 16 (`Section` is `gap-4`) |
| Card header / body | `px-4 py-3` with a 1px rule below / `p-4` |
| Table | header 36; rows 40 default, 32 dense; cells `px-3 py-2`, first and last cells 16 inset |
| Form | label → control 6, control → hint or error 4 |
| Shell | top bar 48; rail 208 (56 collapsed); bottom tab bar 56 plus the safe-area inset |
| Controls | buttons 28 (`sm`) / 32 (`md`) / 36 (`lg`), at least 40 tall below `md`; inputs 32 (`sm`) / 36 (`md`); nav items 36; segmented control 28 / 32 outer on desktop, 40px segments below `md` |
| Touch | icon buttons and top bar controls are 44×44 below `md` |

### Layout rules

- Use `PageContainer` for route width and `Section` for page grouping. Separate sections with space, not rules or boxes.
- Cards collapse to one column on phones; avoid forced `flex-row` headers unless the content fits.
- A card is as tall as its content. No `min-h`, `h-[50vh]` or stretched grid cells to fill space.
- Prefer fewer groups with clear hierarchy over dense metric grids. Put raw logs and verbose diagnostics in drawers.
- Phones stay short: usage table rows stack and repeated cards collapse into rows, so no page turns into a multi-thousand-pixel scroll.
- A composite gets its own phone arrangement, not the desktop one squeezed: a phone is tall and narrow, so the pleasant internal composition differs. Below `md`, framed groups drop their frame and read as sections separated by space (Overview usage group, Latest requests); list-like tables become rows that run edge to edge (`-mx-4`) between two `border-row` rules (audit entries, the plugin catalog as two-line rows: name and use count, then where it runs and what it is; plugin hooks); wide tables reflow into one card per item (API keys below a 42rem section: checkbox, label and revoke, then the key ID, then last 4 · issued · last used); window grids read one line per window (Quota rows, the Overview upstream usage table).
- Check layouts at 390px, 768px and 1440px.

### Radii

| Token | Value | Use |
|-------|-------|-----|
| `rounded-xs` (`--radius-xs`) | 2px | Stacked bars, tiny tags, skeletons |
| `rounded-sm` (`--radius-sm`) | 4px | Buttons, inputs, badges, segments, nav items, menu items, in-card wells and inline notices |
| `rounded-md` (`--radius-md`) | 6px | Cards, dialogs, popovers, toasts, tooltips, page banners |
| `rounded-full` | — | Status dots, switches, the theme pill and the sidebar count pill. Never bars or tracks |

Usage meter tracks and fills are square (no radius).

## 5. Components

Primitives live in `src/components/ui/`: `primitives.tsx` (Card, Button, Badge, Notice, Field, …), `UsageMeter.tsx`, `StackedBar.tsx` + `MetricCell.tsx`, `EntityList.tsx`, `DetailPane.tsx`, `charts.tsx`, `Select.tsx`, `Table.tsx`, `Tabs.tsx`. The shell lives in `src/components/layout/`.

### Shell

- **Rail** (`lg`+): 208px on the ground with a 1px right line, sticky to the viewport; collapses to 56px with ⌘B or the header toggle (the width snaps, labels fade). The brand row at the top is 48px so it lines up with the top bar: brand mark (the let-gate — two squared bracket "c" shapes forming a gate, one accent lane through it) and "cc-lb", with the collapse/expand `IconButton` (`PanelLeft`; "Collapse sidebar" / "Expand sidebar", `aria-expanded`, ⌘B in the tooltip) right-aligned beside the wordmark when expanded and directly under the mark when collapsed. Then nav groups with `text-overline` headings, then the footer: the 11px version line. The phone "More" sheet omits the toggle.
- **Nav item**: 36px, 14/500 `text-muted`; hover `overlay-2` fill and full ink. Active (`data-status="active"`, `aria-current="page"`) is the `selected` fill with full ink, and stays filled on hover; no edge rule. The Upstreams item carries the reconnect count pill (`warn` / `danger` at 15% with `*-text` numerals; a 6px dot when collapsed).
- **Top bar**: 48px, sticky, on the ground with a 1px bottom line. Left: the page name in `text-title-section`, joined by the brand mark below `lg`. Right: Search (⌘K) field, identity (name and kind from `xl`, a popover below), API connection status (dot + word; the healthy "Live" word hides on phones, other states always show their word), and the Night / Day theme pill. The rail toggle lives in the rail's brand row, not here.
- **Bottom tab bar** (below `lg`): fixed, 56px plus the safe-area inset, on the ground with a 1px top line. Overview, Upstreams, Principals, Logs and More; 16px icons over 12px labels. The active tab (and More while one of its pages is open) takes the `selected` fill and full ink; no edge rule. More opens the full navigation as a left sheet. Page titles stay in the top bar only; `main` pads by `--shell-bottom` and toasts sit above the bar.
- `h-shell` is the viewport minus the top bar and, below `lg`, the tab bar; use it for viewport-fit consoles.

### Meters

`UsageMeter` (`UsageMeter.tsx`) is the one quota instrument. There are no arc gauges or dials. It is static CSS; nothing animates.

- The fill is what is **used** of the window, clamped to 0–100, on the meter track, colored by `quotaSeverity(used, pace)`: `text-muted` ink while healthy, `warn` and `danger` fills at the pace-relative marks (danger always at 95%+ used). A non-zero reading draws at least 1% wide; no reading or 0% draws an empty track. The track's one mark is the pace tick: a 2px full-ink (`text`) line centred on `pacePct`, 2px taller than the track on each side, plain ink rather than a severity or window color so it reads on any fill.
- `size="sm"`: a 4px bar for dense rows (the Overview usage table). `size="md"`: a 6px bar with `N% used` (`text-title-card`, severity ink) above it (upstream detail window blocks), or `—` without a reading.
- `role="meter"` named by `label` (the window), valued 0–100 as used, with `aria-valuetext` "N% used" or "No reading", plus "even pace N%" when the tick is drawn.
- **Pace tick** (`pacePct`, from `quotaPacePct` in `lib/quotaSeverity.ts`): the share of the window already elapsed — `(now − (resets_at − length)) / length`, length from `WINDOW_DURATION_SECS` (5h or 7d) — so the eye can read the used fill as ahead of or behind even pace. Drawn only while a timed window runs: a reading and a reset still ahead, within one window length of it. `quotaPacePct` returns `null` for windows without a fixed length (Extra usage), a missing or expired reset, or a reset more than one length ahead (a future window not yet open), so those tracks draw no tick; the mark never appears on the history charts — it belongs to the bar, not the plot. Everywhere quota bars appear the tick does too: the Overview upstream usage table (its `WindowCell` meters) and the upstream detail Quota rows. A `PaceLegend` key — a tick glyph and "even pace", the rule in its `title` — sits once per surface: the usage table's footer and the Quota section's description line.
- Quota bars are only ever this component. Window identity colors never fill it.
- **`StackedBar`**: breakdowns (latency attribution, token usage), not quota. 4px (`xs`), 6px (`sm`) or 8px (`md`), `rounded-xs` track, square segments in a `gap-px` row with `min-width: 2px`, so the track shows between them. Segments always scale within their own bar, never across rows. Renders nothing when every segment is zero. The `xs` bar is the in-cell composition strip under a `MetricCell` number; larger sizes live in drawers and popovers.

### Card and Section

- **Card** (`Card` + optional `CardHeader` + `CardBody`): a self-contained object with its own header or actions — a chart, a table, a form group, a detail block. Flat `panel` fill, 1px line, `rounded-md`. `CardHeader` owns `px-4 py-3`, a 1px rule below, and a `text-title-card` heading with an optional `text-caption` subtitle; `CardBody` owns `p-4`.
- **Section** (`Section`): a flat page region on the ground with a `text-title-section` h2, optional `text-body-sm` muted subtitle, and no box. Use it for groups of peers (quota windows, the upstream usage table, "Used by" lists) and around page-level tables that already have a frame.
- A section title and a card title never share a size.

### Button

| Variant | Look | Use |
|---------|------|-----|
| `primary` | Solid accent with `accent-ink` text; `accent-hover` fill and line on hover (no brightness filter) | One per view: the commit action (Save, Create, Continue). Not "New" in list headers |
| `secondary` (default) | Transparent with a 1px `border-strong` line; `panel-strong` on hover | Toolbars, paired actions, most actions |
| `ghost` | No fill or line; `text-muted` → `text` with the icon-button `overlay-5` hover fill | Tertiary actions, in-row links |
| `danger` | Outline: 1px `danger` line, `danger-text`, hover `danger/10` | Page-level Delete / Revoke |
| `danger-solid` | `danger-solid` fill, white text, darker fill on hover | Only the confirm button of a destructive `ConfirmDialog` |

- Sizes: `sm` 28px / 12px text (tables, toolbars), `md` 32px / 13px (default), `lg` 36px / 14px (dialog footers, mobile). Below `md` every Button is at least 40px tall (`max-md:min-h-10`), whatever its size.
- Icons are 14px in `sm`/`md` and 16px in `lg`, enforced by the Button; use lucide at `strokeWidth={1.75}`.
- Disabled is the shared `control-disabled` utility (`index.css`), never opacity: `text-disabled` ink (about 2.9:1 — legible but a clear step below `text-faint`, so the control reads unavailable at a glance), transparent fill, a dashed 1px `border-disabled` line where the control draws a border, `cursor-not-allowed`, and no hover response. Native controls take `disabled:control-disabled`; elements that carry `aria-disabled` take it as a plain class. A busy (`aria-busy`) Button keeps its normal look (spinner, `cursor-progress`); `control-disabled` applies only to a button that is unavailable. A disabled `primary` renders as a disabled `secondary` until it becomes actionable.
- Paired or sibling actions share a variant and size. No full-width buttons inside desktop cards.
- A toggled toolbar action ("Stop tail") is `secondary` with `aria-pressed`, plus a live dot while active.

### IconButton

Every icon-only control. `label` is required and becomes the accessible name. 44×44 below `md`, 32×32 from `md`; the glyph is 16px. Hover is the one icon/ghost fill, `bg-overlay-5`, with `text-muted` → `text` — shared by `IconButton`, the `ghost` Button, the shell's `TOPBAR_ICON_BUTTON` and the 16px `?` help glyphs; disabled drops both the fill and the text change.

### Badge and StatusBadge

- `Badge`: 20px tall, 12/500 sans sentence case, `px-1.5`, `rounded-sm`. Tones: `neutral` (`overlay-5` fill, neutral text), `ok` (traffic-success text and dot), `warn`, `danger` (tone/12 fill, `*-text`), `accent` (an outlined chip: 1px accent line, `accent-text`, no fill — e.g. "binds pool"), `mono` (`font-mono text-data` on `overlay-4`, for model IDs and key IDs only).
- `StatusBadge`: a status dot plus a `text-label` phrase ("Enabled", "Reconnect required"). Explicit healthy and success states use green traffic-success text and dots; `warn`/`danger` use their `*-text`. No chip, no mono, no uppercase.
- At most one status badge per object header. In lists, status is a dot plus a short `*-text` phrase, not a filled chip. Omit repeated healthy-by-default labels where presence is intentionally conveyed by the surrounding object (for example, enabled rows); do not suppress an explicit success or live state.
- Status dot: `.status-dot` 6px (`.status-dot.lg` 8px in lists), flat and static. Pending warm-up uses the accent `live` dot; connection and request-feed `Live` indicators use the green `ok` dot. State is always carried by text as well.

### Selection

One rule for every list, table and nav:

- Selected: the neutral `selected` fill (`bg-selected`, a clear step above hover) with the name in full ink, `font-medium` in entity lists. `aria-current="true"` on list rows; `aria-current="page"` on nav items and nav tabs. The fill stays on hover.
- Hover: `bg-overlay-2` on list rows and nav items (`hover-bg` on interactive table rows).
- Focus-visible: the standard 2px accent outline. Focus is not selection.
- Never a side stripe: no colored left border, no inset box-shadow, no pseudo-element bar, and never an accent tint as the only cue.
- Applies to the rail nav item, the bottom tab bar, the command palette, `EntityList` rows, `TableRow selected` and the keepalive sessions drawer.

### Tables

Use `Table`, `TableHead`, `TableHeadCell`, `TableRow`, `TableCell`, `TableEmptyRow` and `EmptyValue` from `Table.tsx`, or the `table-header` (on `thead`) and `table-th` (on `th`) utilities for existing markup.

- Header: 12/500 sans sentence case in `text-faint`, 36px, opaque sticky `panel` with a 1px line. No uppercase, no tracking, no blur.
- Body: `text-body` 14px sans. Mono (`TableCell mono`) only for ID, model, path and hash columns. Numbers right-aligned and tabular (`numeric`), units muted at the same size.
- Rows: 40px (32px `dense`), `border-row` divider, `hover-bg` on interactive rows. Selected (`TableRow selected`) is the neutral `selected` fill, kept on hover; no edge rule, no inset shadow.
- No per-cell bars except a `UsageMeter sm` in a quota window cell and the `MetricCell` composition bar. In request tables (Overview preview, Logs, per-upstream and per-principal recent requests) latency, tokens and cost are each one right-aligned cell via `MetricCell`: the tabular number on top, an `xs` `StackedBar` of the metric's composition under it (the 4px slot is always reserved, so an empty bar keeps the baseline), and a breakdown `Hint` popover on hover — the cell is a button, so Enter/Space opens the same breakdown. A metric the request never recorded is `EmptyMetricCell`: `—` over the empty bar slot.
- **Tokens cell** (`TokenCell`): `in → out   N% hit` in fixed slots so every row's figures line up down the column: prompt tokens in (cache read + cache create + input) and output tokens out, each a right-aligned 5.5ch slot with its unit in a fixed trailing 1.5ch slot, a faint `→` between, then the cache hit of the prompt (cache read ÷ prompt, rounded down so a partial hit never reads `100%`) in a 4.5ch slot with a faint `hit` word (kept in the cell because the card layout has no header). Every row with prompt tokens shows its hit, `0%` included; only a row with no prompt tokens yet shows a faint `—`. Its bar and popover follow the token composition order; the popover footer repeats `Cache hit of prompt`.
- Compact counts use `splitNum` (`lib/format.ts`): the figure and a separate unit — no unit under 1,000, then `k`, `M`, `B` (lowercase `k`; `M` / `B` capitals so they never read as milli or minutes); three significant digits at most (`2.8k`, `12.5k`, `120k`, `1.2M`). Latency uses `fmtMsCompact` (`ms` under 1s, then `s`, then `m`); cost is `formatCostMicros`, right-aligned so every row ends on the same edge.
- `RequestEventsTable` column order: Timestamp, Principal, Upstream, Session, Kind, Model, Status, Latency, Tokens, Cost. Session (`SessionChip`) and Kind (`RequestKindBadge`) are always shown, even when every row is null: the dashes say the requests carried none, and the columns do not jump as rows stream in. It has two width modes, and in both Session is the one flexible column — it takes the spare width, drops its own right inset (`pr-0`, the Kind cell's left inset is the separator), and middle-truncates inside its track (the head span takes the ellipsis, the tail span keeps the last 4 characters; the full id in `title`). Model is a fixed content column capped at 10rem (`max-w-40`), enough for a family and version. Full-page tables (Logs, Overview latest requests) pass `minWidthClass` (`min-w-[1080px]`) and keep every column — session still keeps a 7rem floor once the floor scrolls — with horizontal scroll as a last resort. Detail-pane tables omit it and fit their container (`@container/events`): from 60rem they are a table, session again absorbing the slack; between 46rem and 60rem each row takes two lines on fixed tracks (`4rem 3.25rem minmax(6rem,1fr) auto auto auto`), headed by the cells themselves — time, kind, session, the principal or upstream, model and status, then latency under time and kind, tokens under session and cost at the right edge — and when both Principal and Upstream are shown they sit out that band; below 46rem each row becomes a three-line card — time, kind, session, status; principal → upstream, model; latency, tokens, cost — on a four-track grid whose first track is pinned at 3.5rem (`min-w-14`, fits "23h ago" / "100 ms") so kind badges line up from card to card, with `gap-x-2` and the metric cells' 72px floor dropped; principal and model cap at 40cqw. Timestamp, Session, Kind, Status, Latency, Tokens and Cost never drop.
- Latency and Cost cells hold 7.5rem (`min-w-30`) from a 46rem table and 9rem (`min-w-36`) from 80rem, a little narrower than the Tokens cell (≈ 11.7rem), so their composition bars read.
- The Model cell drops a leading `claude-` in every mode (`sonnet-4-6`, `opus-4-7`) so the family and version survive; the full id stays in the cell's `title` and accessible name.
- Hide a column when every visible row is null, except Session and Kind in request tables (above); show a null cell as `—` in `text-faint` (`EmptyValue`, with a screen-reader label).
- A table inside a card drops the card body padding; the first and last cells keep a 16px inset so the header aligns with the card title.

### Lists at any size

`EntityList` (`EntityList.tsx`) is the one list pane behind Upstreams and Principals. It reads the same with 1 item and with dozens.

- **Split pane**: inside `h-shell`, capped at 90rem and centred like `PageContainer`, the list pane is 360px from `md` (400px from `xl`) with a 1px right line and its own scroll; the detail pane takes the rest and scrolls on its own. Below `md` the page shows the list or the selected item's detail, never both, and the detail carries a back button. From `md` the first visible row (under the current sort and filter) is selected automatically, so the detail pane is never blank.
- **Pane header**: the pane title lives in the top bar (the `PageHeader` h1 is visually hidden), so the header is one row: a faint `text-caption` count line (`15 upstreams · 3 need reconnect`, `37 principals · 2 disabled`) on the left and the Add / New `primary` button on the right. Its outer edge follows `PageContainer`'s gutter (16 / 32 / 40, `pl-4 md:pl-8 lg:pl-10`), and so do the result line and the rows, so list content lines up with every other page's content edge.
- **Toolbar** only from 8 items (`ENTITY_LIST_TOOLBAR_MIN_ITEMS`); below that it is noise. A search field (`Search upstreams`, Escape clears it), one filter `Select` and one sort `Select`, all `sm`. On phones the search is 40px tall and the two selects read as one quiet line under it, filter left and sort right: borderless, transparent, `text-body-sm` muted, 40px tall and sized to their label, so the view controls never outweigh the list. Search is client-side over the name, id and kind or plan. Below 8 items search and filter do not apply; sort always does.
- **URL state**: `q`, `filter` and `sort` live in the route search params with zod defaults, and defaults stay out of the address bar. Changing them replaces history and keeps `selectedId`; a selected item the filter hides stays open in the detail pane.
- **Result line** while search or a non-default filter narrows the list: `Showing 4 of 37` (`role="status"`) and a ghost `Clear` that resets search and filter, not sort.
- **No match vs first run**: when items exist but none match, a centered `No principals match "abc"` (or `No principals match this filter`) with `Clear filters`. When nothing exists at all, the page's own first-run empty state shows instead.
- **Keyboard**: the list is one tab stop (rows are `tabIndex=-1`). ArrowUp, ArrowDown, Home and End move the selection from `md`, scrolling it into view within the pane only. On phones they move a highlight (`overlay-2` + accent outline) and Enter or Space opens the detail. `/` is not bound; ⌘K is the palette.
- **Rows**: full-width buttons, min 52px, `rounded-sm px-3 py-2`, two lines. Line 1: the name (`text-body`, truncated) with one tabular figure on the right. Line 2: a faint `text-caption` caption with compact tabular facts on the right. The full value sits in `title`. Disabled items read muted until selected.

Upstream rows:

- Line 1, right: the highest used % across the subscription windows, in severity ink; nothing for API-key upstreams.
- Line 2: `Disabled` (muted) for a disabled upstream; otherwise, when there is a reconnect nudge or health problem, a status dot and phrase in `warn-text` / `danger-text`; otherwise the plan (`Claude Max 20x`) or `API key`. Right: the per-window facts `5h 12% · 7d 87% · Fable 10%` rendered as three fixed columns (`5h`, `7d`, `Fable`), each a constant faint label over a right-aligned 4.5ch figure slot, muted while healthy and severity-colored by `quotaSeverity` — so every row puts each window's figure at the same x. A missing window keeps its (invisible) cell rather than collapsing the column.
- **Filter:** All upstreams / Needs attention / Disabled / Subscription (OAuth) / API key. Sort: Needs attention first (default) / Highest usage / Name. An enabled upstream needs attention when it has a status problem or any window has warn or danger quota severity; a disabled OAuth upstream still needs attention when it has a reconnect nudge. With a valid window pace, quota warns at a 10-point pace gap or 90% used and becomes danger at a 30-point gap or 95% used. Without pace it falls back to the absolute 80% warn and 95% danger boundaries. The default sort ranks terminal OAuth reconnect nudges first, then danger status or quota, then warn status or quota, then healthy, then disabled; ties go to the most-used window, then name.

Principal rows:

- Line 1, right: requests in the selected shared range (muted, tabular; `—` when none), from one per-principal usage query for the whole list — the same `1h` / `6h` / `24h` / `7d` preset Overview's Top principals uses.
- Line 2: `Disabled · ` (muted) when disabled, then kind · `Any model` / `N models` · `No limits` / `N limits`. The revision lives in the detail, not the row.
- Filter: All principals / Enabled / Disabled / Human / Machine / Admin. Sort: Name (default) / Most active. There is no "recently updated": principals carry no update timestamp.

### Detail pane

`DetailPane.tsx` is the one selected-item detail behind Upstreams and Principals; both pages compose the same exported pieces, identically.

- **`DetailPane`**: the pane's own `@container` scroll area (`min-w-0 flex-1 overflow-y-auto`). From `md` the `header` stays pinned at the top; the body (`@container/detail`, 40 / 64px bottom padding) stacks an optional `notices` block (incident notices, page-level actions) then the sections at 24px (`md`: 32px) intervals. Header and body share a 16 / 32px horizontal inset, with `PageContainer`'s 40px on the right from `lg`, so the pane's content ends on the same edge as every other page's content.
- **`DetailHeader`**: identical shape for every entity; sticky from `md` only (on phones a pinned header would hold a quarter of the screen, so it scrolls away with the body). Narrow panes (phones, and split panes under `@2xl` / 42rem): the first row holds the 44px `All upstreams` / `All principals` back button (phones only) on the left and the actions on the right, then the entity name gets a full-width line. Wide panes: the name on the left, the actions on the right. The name is the pane's one `h2` (`text-title-page`) with its kind/plan `Badge` and at most one exception `StatusBadge`; the actions are the `Enabled` compact `ToggleSwitch` (with an `Enabling...` / `Disabling...` pending status) and a `danger` `sm` Delete button, in that order on both pages. Then one muted `·`-joined meta line ending in the mono ID with a copy `IconButton`. It sits over `bg-bg` and gains a 1px `border-subtle` bottom line once the body scrolls under it. `DetailHeaderSkeleton` is the loading placeholder with the same geometry.
- **`DetailSection`**: one titled region per pane section — a full-width 1px `border-subtle` rule, 20px (`md`: 24px) above the title, then a `text-title-section` h3 with a one-line muted `description` and an optional `action` (on the title's line while both fit, wrapping under it otherwise), then the content 12px below. `collapsible` makes a `details` disclosure closed unless `defaultOpen`, with a Show/Hide + chevron summary. Each section is its own size container.
- **`DetailSectionGrid`**: lays sections out in reading order — one column, then two equal columns (48px gap) once the pane body is 56rem wide. Sections in one row share its top so their rules line up; `span="full"` takes the whole row. The row gap equals the body's section interval, so the rhythm never changes. Pair only sections of similar height; anything wide or much taller spans full.
- **`DetailRows` / `DetailRow`**: settings as label / value / action rows between `border-row` dividers (bordered top and bottom). From the pane's `@lg` width a row is a three-column grid: 12rem `text-label` muted label (with a faint caption `description`), then the value, then the action.
- **`DetailFacts` / `DetailFact`**: a `dl` of small related facts in 2 columns, 3 from `@2xl`, 4 from `@4xl` — muted label over value with an optional faint `hint`.

The upstream detail reads: notices (reconnect / apply failure / billing), then for OAuth upstreams **Quota** and **Quota history** (with its own range control) — API-key upstreams get **API usage** instead — then **Recent requests**, all full width. OAuth continues with **Credential** and **Account metadata** side by side (Account metadata is collapsible, open whenever a credential is bound so the pair has no hole; its facts include a humanized billing type, `stripe_subscription` → "Stripe subscription"), then **Warm-up** full width; Credential and Warm-up use status tones to distinguish enabled or healthy, attention or retrying, and failed or reconnect-required states. A disabled OAuth upstream still shows its reconnect notice. API-key ends with **Settings** full width.

The **Quota** section's `Reset quota` action (`LimitResetAction`) is always visible. Without a stored OAuth credential it is disabled — resets are claimed with that credential, so its status query never runs — and the disabled button is wrapped in a focusable popover trigger that explains why ("Connect the account first: resets are claimed with its stored credentials").

**Quota** rows (`QuotaWindowRows`) list every web-visible window the API reports for the upstream, in `QUOTA_WINDOW_ORDER`: 5h, 7d, 7d (Fable), 7d (Sonnet), 7d (Opus), Extra usage. `absent` windows are hidden; 5h and 7d show an unobserved placeholder before the first quota lookup, the others need an observation, and Extra usage is shown only when enabled with a finite positive monthly budget. Timed windows use a `UsageMeter` with `N% used` (severity ink) and reset facts; Extra usage shows its credit amount and budget. The backend's `unified` account-restriction envelope is not a web-visible window: it stays on the wire and in admin payloads (its provider signals feed routing and overage decisions), but the UI neither requests it nor renders a row or series for it. The rows share one subgrid — window swatch and name, meter, then facts — and from a 40rem section each window is one line; narrower, a row takes two lines.

The principal detail reads: **Cache keepalive**, **Recent requests**, **Router**, **Shape**, **API keys**, and **Access**, each full width. Shape is a plain section, always expanded.

### Overview

The Overview leads with how usage moved, then who is using it. Top to bottom (`routes/index.tsx`): the incident banners (`LiveTailFailureBanner`, `OAuthReconnectSummary`), `PageHeader`, then either the first-run checklist alone or the sections below.

1. **Usage**: one group that binds the page's single range control to everything it scopes, framed from `md` (`rounded-md`, 1px `border-subtle`). Its header row holds the `Usage` h2, the subtitle `Pool quota usage, traffic and top principals over the selected range` and the `SegmentedControl` range (`?range=`, shared non-Logs default `7d`; on phones the four presets share the full width as equal segments, one row under the title). Three blocks (`UsageBlock`: `text-title-card` h3 whose subtitle ends in the range words, e.g. `last 7d`) stack inside it between 1px `border-subtle` rules from `md`. Below `md` the group has no frame and no rules: the blocks are sections separated by space at the page inset, and the Top principals table runs to the screen edges with its cells kept readable.
   - **Pool quota usage**: pool used % per window (5h, 7d, 7d (Fable)) over the range, with the 95% danger line. 200px tall on phones, 240px from `md`, 320px from `lg`. The legend row above the plot gives each window's chip and name, its current `N% used` (`font-medium`, severity ink) and `resets in 3h 12m` (absolute time in `title`), then the threshold entry. Captions under the plot keep the honesty notes: plan weighting, upstreams with unknown capacity, and the provider reading's age with a `Stale reading` badge.
   - **Traffic**: five KPI readouts with sparklines (Requests/s, Tokens, Cost at list price, Avg latency, Error rate) as cells of one grid (2 columns, 3 from `sm`, 5 from `xl`) separated by 1px left rules, sharing one hover index and a hover tooltip. Values never truncate: Requests/s reads `<0.01` for a nonzero rate below 0.01 (`formatRate`); Tokens is compact (`fmtTokens`); Requests/s and Cost switch to `splitNum` units at or past a million. Whenever the visible figure is compacted, the exact figure is its `title` and screen-reader text. Tokens is the one two-series tile: total tokens in the neutral KPI ink and the cache-miss % (0–100 on its own scale) as a dashed amber (`--color-series-cache-create-5m`) line, named by a legend under the value (`Tokens 45.2k`, `Cache miss 12.3%`). A range without requests shows one line instead of the five readouts.
   - **Top principals** (`Ranked by virtual cost · selected range`, `View all principals` link to `/principals?sort=active`): a table of every principal with requests in the range, initially showing the top 10 and expanding to the full result set with an optional name filter — Principal (links to its detail), Requests, Tokens, Cache hit, Cost and Share. Cache hit is the window's cache read over every prompt token (`cacheHitRatio`: cache read + cache create + uncached input), rounded down; it is neutral at 90% or more, amber from 80% to below 90%, red below 80%, and muted when there are no prompt tokens. Token units (`k`, `M`, `B`) use categorical colors while the number stays neutral. Cost is a request-table `MetricCell` with a bar relative to the largest cost in the full result set, a fixed two-decimal table value, and the exact amount in its details; cost breakdowns list only positive categories. Both Top principal and request cost figures mute only right-padding fractional zeros; the decimal point, significant digits, and whole-number zeros stay primary, and request cost keeps its existing precision.
2. **Upstreams** (`Section`, full width, outside the Usage group because it reads "as last observed", `Manage upstreams` link): `UpstreamUsageTable`, one row per upstream with columns Upstream, 5h, 7d and 7d (Fable). The name cell adds a dot and status phrase only when something is wrong. Each window cell is `N%` with a muted `used`, a `UsageMeter sm` and a muted reset caption (`resets in 3h 12m`, absolute time in `title`; `No reading` without a snapshot); no limit-status text. Order: needs attention, subscription, API key, disabled; most used first inside each group. Up to 8 rows show; past 8 the rest sit behind an inline `Show all N` / `Show fewer` toggle (`aria-expanded`). Rows link to `/upstreams?selectedId=`. The row composes by the container's width: narrow (phones), the name line, then one line per window — its label (4.5rem), `N% used` and the reset caption on the right — over a full-width meter; from `@xl` the three windows sit side by side under the name, each repeating its label; from `@4xl` one table row under a header.
3. **Latest requests (any time)**: a shared `RequestEventsFeed` presents newest message requests with live status, failure and retry state, plus pagination. Overview starts with 500 historical rows and mounts 50 data rows per page; cursor paging loads older history beyond that initial batch. Overview keeps both Principal and Upstream columns; entity-scoped panes omit only their own entity column. A card from `md`; on phones the rows run to the screen edges.

### Charts (Recharts)

`charts.tsx` exports the shared chrome; spread it into Recharts parts rather than restyling per chart.

- `CHART_GRID` (`vertical: false`, stroke `border`): 1px crisp horizontal gridlines only. `index.css` enforces the stroke and hides axis and tick lines everywhere.
- `CHART_AXIS`: no axis or tick lines, 12px sans tabular ticks in `text-faint`. About 4 x-ticks.
- `CHART_CURSOR`: a 1px `border-strong` hover rule.
- Tooltip: `bg-sub`, 1px `border-strong`, `rounded-md`, the overlay shadow, 12px sans; label `text-muted`, values `text`, tabular.
- **Quota history plots used**, the same figure every meter reads. The y axis is `0–100` with ticks at 0, 25, 50, 75 and 100, labeled `N%`; higher is closer to the limit.
- Thresholds (`CHART_THRESHOLD`): a 1px dashed (`3 3`) `danger` reference line at **95%** used (`QUOTA_DANGER_PCT`). Severity color is pace-relative now, so the only absolute boundary a chart can draw is the danger line — there is no warn line. The Overview labels it `95%` inside the right edge in `danger-text`; legends list "Danger at 95% used" after the series. No tinted `ReferenceArea` bands.
- Quota charts draw each window in its `getWindowColor` series with a 1.5px stroke. The live windows (5h, 7d, 7d (Fable)) are filled with `SeriesFillGradient`: the series color at 2× `--chart-fill-opacity` along the line fading to nothing at the baseline, so overlapping areas stay legible under each other's strokes. Legacy (Sonnet, Opus) and overage are strokes only — and the unified envelope never draws, as it is not a web-visible window — so five-plus windows never stack into a tinted wash; with more than three windows visible and none isolated, the gradients drop to 1× strength. The Overview pool chart paints back to front — 7d, 7d (Fable), then 5h (the fastest mover) on top — fills first and every stroke in a second pass, so no fill covers another window's line; its tooltip shows one row per window in legend order (`poolTooltipRows` drops the fill pass's entry, which Recharts also hands custom tooltip content).
- Legend: in the chart header row: a square window chip (or a short dashed line swatch for the threshold), a `text-muted` label and a tabular value. Series come first, then the threshold; a series entry toggles isolating its window.
- `Sparkline`: a 1.25px line over a flat fill at `--chart-fill-opacity` (neutral `text-muted` by default); an optional `secondary` series draws as a 1.5px dashed line without fill on its own scale (`SPARKLINE_SECONDARY_DASH` 3 2), always named by a caller legend; a null secondary value is a gap, not a zero. Renders nothing when every value is zero. Chart animation is off.

- Histograms: neutral bars with errors stacked in `danger`. No donuts: use a `StackedBar` plus a value table.
- A chart is one static image to assistive tech: pass an `ariaLabel` summary, or hide it when the surrounding element already names it.

### Audit table

The audit trail (`routes/audit.tsx`, a `FullPage` console capped at 90rem with 40px sides from `lg`, like `PageContainer` pages) lists admin API actions newest first in a card table. Time is `RelativeTime compact` (`5m ago`) with the absolute timestamp in a hover tooltip (`Hint`) and `title`. Columns (Time, Action, Target, Details, Actor, Request, Open) size to the table's own width by container query, so a collapsed rail counts: below `@4xl` (56rem) Action is the one flexible column, the change summary rides under it (line-clamped to 2), Details is hidden and the Request column is a right-aligned Status; from `@4xl` Action and Target are fixed, a line-clamped Details column appears, and the route joins the status in the Request column — Details and Request are the two flexible columns and split the rest evenly, with Details' full text in the cell `title` and the drawer; from `@7xl` (80rem) the fixed columns widen a step and the actor kind shows. A row click opens an `AuditEntryDrawer`. Below `md` the card drops its frame: one row per entry runs edge to edge between two `border-row` rules — the action and its time, then the target, then the change summary (line-clamped to 2), then actor · status.

### Settings

`routes/settings.tsx` shows no visible page header: the top bar names the page and the `PageHeader` h1 stays for assistive tech only, with no description. Sections: Localization, the configuration editor, configuration history, Data & backups; there is no Version card. The configuration editor's facts strip (`ConfigStatusFacts`) shows only the **Draft** facts — revision, validated revision, saved time and the config file path — because the editor fields below already show each running value.

### Forms

- `Field`: `text-label` sentence-case label in `text-muted` above the control (6px), `text-caption` hint in `text-faint` or error in `danger-text` below (4px). A required field shows a `danger-text` asterisk. Native `input`/`select`/`textarea` children get `required`, `aria-invalid` and `aria-describedby` automatically.
- Inputs: `INPUT_CLASS` (36px) or `INPUT_SM_CLASS` (32px): `rounded-sm`, `input-bg`, 1px `border-strong`, accent line on focus plus the 2px accent focus outline at 1px offset. Placeholder in `text-faint`.
- `Select` (`Select.tsx`) is the only select control; never a native `<select>`. `allLabel` adds the `''` "all" item for filter bars; otherwise `placeholder` shows until a value is chosen. Width comes from `className`; the popup matches the trigger and uses the overlay surface, with an `accent-text` check column reserved so labels never shift. Pass `aria-label` when no `Field` names it.
- Disclosure: a custom `<summary>` with `list-style: none` and a 12px rotating lucide chevron.
- `ToggleSwitch`: 36×20 switch over a native checkbox with an accessible name. Off is the meter track with a `border-strong` line and a muted knob; on is a solid accent track with an `accent-ink` knob. Disabled keeps the switch faint instead of accent: off draws a dashed `border-disabled` line over a transparent track, on keeps the same dashed line over the plain meter track — either way the knob is `text-disabled` and the label `text-disabled`. Label is `text-body-sm` sans. The default `card` variant is an in-card well (fill, no border); `compact` sits inline in a header row.
- `SegmentedControl`: at most 5 short options. Radiogroup with roving focus; 1px `border-strong` outline, selected segment is `panel-strong` fill with full ink. Segments are 40px tall below `md` (`h-10`) and 22 / 26px from `md`. Use it for every small exclusive choice (time presets, window toggles); do not hand-roll chip rows.
- `Tabs` (`Tabs.tsx`): underline tabs, 14/500, 36px row over a 1px line, a square 2px accent rule under the active item, one horizontally scrolling line with an edge fade. `mode="tabs"` is an ARIA tablist (pair panels with `tabPanelProps`); `mode="nav"` renders a labelled `<nav>` with `aria-current="page"`. No grids of bordered cells, no wrapping.

### Banners and notices

`Notice` has two variants:

- `variant="banner"`: the page-level incident line, at most one per page. `rounded-md`, a uniform 1px `tone/45` line (`border-subtle` for neutral tones), `tone/8` fill, 16px icon, one line: a 600-weight title plus a short summary, and at most one action on the right. Lists collapse into a sentence ("3 upstreams can't authenticate: isac-max, example-org, example-secondary-max").
- `variant="inline"` (default): a well inside a card, `tone/8` fill with no border, a 14px icon in the tone text color, body in `text-muted`.

- Status tones are `ok` (green traffic-success dot and text), `warn` (amber), `danger` (red), `neutral` (muted), and `live` (accent when used for a pending warm-up state). The admin connection and request-feed `Live` indicators use the green `ok` treatment, as does the success notice icon. `info` notice surfaces remain neutral; explicit status badges use the semantic status tones. The object that owns the problem shows the full notice; everywhere else shows a dot and a short phrase.

### Empty states

- `EmptyState` has no frame of its own. In a card it is one centered 13px `text-muted` line with `py-8`; with a description or action the title steps up to `text-title-card`.
- The containing card collapses to its content. Dashed borders are only for drop zones.
- Metrics with no traffic show `—`, not a fake-precise `0 ms` or `0.00%`. A quota with no reading shows `—` and an empty track, never `0% used`.

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
- Instruments are still. Meters and charts do not animate, and the live dot does not pulse; nothing loops except the skeleton shimmer while loading.
- Every interactive element shows focus only on keyboard focus: `focus-visible:outline-2 outline-accent` (offset per element), never `focus:` rings or `ring-*`. Disabled is the `control-disabled` utility (see Button), never an opacity dim, and a disabled `primary` Button renders as a disabled `secondary`. Anchors that must keep link semantics take `buttonClassName(variant, size)` for Button chrome.
- No emoji or Unicode glyphs as icons; use lucide at `strokeWidth={1.75}` or text-only labels.
- Honor `prefers-reduced-motion`: shimmer and live-row flashes become static, drawers fade instead of sliding, switches and the "More" sheet stop transitioning. Never use a global 0.01ms kill.
- Mobile (below `md`): request tables stack into three-line cards, filter panels sit behind a "Filters (n)" disclosure, entity pages show the list or the detail, and quota comes before traffic KPIs.

## 7. Depth & Surface

| Level | Treatment | When |
|-------|-----------|------|
| Ground | `bg-bg` | The page, rail, top bar and tab bar |
| Flat section | No box: `text-title-section` title + content, separated from the next section by space | Groups of peers; page-level tables with their own frame |
| Card | `.glass` (`panel` fill + 1px `border`) + `rounded-md` | A self-contained object with its own header or actions |
| Well | `.well` (`overlay-3` fill, `rounded-sm`, no border) + `p-3` | A sub-group inside a card: an inline notice, a credit strip, toggle rows |
| Divider | 1px `border-row` (`divide-y divide-row`, `border-t border-row`) | Between rows, or between sub-sections inside a card |
| Overlay | `.glass-strong` (`bg-sub` + 1px `border-strong` + `shadow-overlay`) + `rounded-md` | Popovers, menus, select popups, tooltips, toasts. Dialogs and drawers use the same fill, line and shadow |

### Rules

- Everything is flat. Surfaces step up from the ground by fill and a 1px line, never by blur, glow, gradient or a drop shadow.
- Elevation is declared once: `shadow-overlay` (`0 8px 24px -12px` in the overlay-shadow color) and only on overlays.
- Separate sections with space, not borders. Page regions are not boxed unless they are a self-contained object.
- A bordered box never sits inside another bordered box. Inside a card, group with wells and dividers.
- No side-stripe borders to mark a card, row or list item as special or selected; use a leading dot, an icon, a badge, or the `selected` fill. The only colored edge is the 2px accent rule under the active tab.
- A simplified warmup card shows one status story, one action cluster and one drawer entry point; everything else is summarized or moved behind disclosure.

## 8. Time Controls & Bounded History

### Time UX Contract
- **Compact Controls**: Time ranges use a dense preset selector (`All time`, `1h`, `6h`, `24h`, `7d`, `Custom`) alongside optional `Since` and `Until` inputs.
- **Range changes keep the reader's place**: a range, preset or filter change is a search-only `navigate()` and passes `resetScroll: false` — the router runs with `scrollRestoration`, which otherwise snaps the page to the top on every navigation. Data keeps its previous response on screen until the new one lands (`keepPreviousData` is the query client default), so the region never collapses to skeletons under the reader.
- **Timezone Awareness**: Custom bounds use dashboard-styled `YYYY-MM-DD HH:mm` text inputs interpreted in the user's effective configured timezone (via `useTimezone()`), not blindly in the browser's local timezone. Each field pairs the validated text entry with a date-only `react-day-picker` calendar: a Base UI popover at desktop width and the existing Base UI Modal surface below 1024px. Picking a date preserves an existing time or supplies the bound's start/end-of-day default. Since resolves to the first second of its displayed minute; inclusive Until resolves to the final second.
- **Validation**: Invalid calendar values, nonexistent spring-forward times, and ambiguous fall-back times are deterministically distinguished with programmatically linked inline errors. URL bounds outside the JavaScript Date range or inverted Custom bounds are removed before rendering or querying.
- **Shared relative range:** Embedded charts and drawers use the shared `cclb.timeRange` selection, persisted across mounted consumers with a `7d` default. Logs and other URL-owned pages keep their own range state; Logs defaults independently to `All time`.
- **Warm-up status tones:** Warm-up uses `Healthy`/green for success, `Idle`/neutral for skipped work, `Degraded`/warning for transient failure, and `Down`/danger for permanent failure or reconnect-required credentials. The same semantic colors apply to the last-run summary, history counts, and outcome filters.

### Bounded History & Client Pagination
- **Request Rows & Client Pagination:** The shared `RequestEventsFeed`/`useRequestEventsFeed` owns historical rows, SSE state, filtering, live flash/status/failure/retry state, and cursor page snapshots. Overview retains an initial 500 historical rows and mounts at most 50 data rows per page; Next loads older history pages by cursor. Entity-scoped feeds use a bounded infinite scroll slot with at most 500 retained rows and 50-row growth pages. SSE state is bounded separately at 500 finalized events, 500 partials, and up to 500 eviction tombstones. Table and row components retain `React.memo` optimization, and merged rows keep referential identity for unchanged events so live-tail updates do not re-render the whole page.
- **Filtering & Pagination Rules**: Filters apply to the retained events before client pagination is calculated. Any route or filter change resets the current page to page 1. Shrink clamps are applied to prevent out-of-bounds pages.
- **Status Classes**: Status filtering uses the backend's `status_class` contract (`2xx`, `3xx`, `4xx`, `5xx`) for recent history and live SSE, then applies the same class predicate to retained rows so stale live or placeholder data cannot leak across transitions.
- **Model Filter**: The Model input is case-insensitive and trims an optional leading `claude-`. It matches a model when the normalized id starts with the remaining text, or when a `claude-` model contains that text after its vendor prefix (`sonnet` matches `claude-sonnet-4-5` and `claude-3-5-sonnet-…`). Historical SQL, the live SSE fan-out and the retained-row filter use the same predicate. Typing is debounced so the URL, history query and SSE connection update only once input settles.
- **Kind Filter**: Logs lists `messages` (model traffic, `/v1/messages`) when the URL names no `event_kind` — the default kind is not an applied filter and stays out of the address bar. `event_kind=all` ("All kinds") is the explicit opt-out that lists every endpoint category; any other kind filters to it and shows a Kind chip. The default kind filters history, the live SSE fan-out and the retained rows alike.
- **In-Progress Cost**: Once usage is observed for a priced model, partial rows and drawers show a token-derived `Estimated Cost`. This is display-only; authoritative final cost remains sourced from the termination-time `Priced` event used by billing and limit reconciliation.
- **Export Behavior**: The Export action includes all filtered retained rows, not just the current page.
- **Older History:** Next loads a cursor page from the current final row when local page snapshots and retained history are exhausted; the shared feed does not use a backend infinite-scroll sentinel.
- **Pagination Controls**: Controls are accessible, featuring labeled Prev and Next buttons and an `aria-live` region for screen readers.
- **Backend Paging**: The exact 200, 200, 100 backend paging limits remain unchanged.
- **Live Tailing**: Tailing is automatically paused when a fixed custom time range is selected, and resumes based on user preference when returning to a relative preset or "All time".
