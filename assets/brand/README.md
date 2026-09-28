# cc-lb brand assets

The cc-lb mark is the **let-gate**: two squared bracket "c" shapes face each
other so they form a gate, and one lane (the accent bar) passes straight
through it. Wordmark: `cc-lb`. Tagline: *Anthropic-compatible load balancer
for Claude Code*.

Everything here is generated from `tools/brand.py` — geometry, palette,
wordmark and tagline. To change the mark, edit that file and rebuild (below).

## Files by use case

| Use case | File(s) |
|---|---|
| README hero (top of `README.md`) | `readme/cc-lb-hero-dark.svg`, `readme/cc-lb-hero-light.svg` — embed via `<picture>` with a `prefers-color-scheme: dark` source |
| Website header / product lockup | `lockup/cc-lb-lockup-horizontal-on-{dark,light}.svg`, `lockup/cc-lb-lockup-stacked-on-{dark,light}.svg` |
| Wordmark only | `lockup/cc-lb-wordmark-on-{dark,light}.svg` (glyphs outlined to paths — no font dependency) |
| Standalone mark | `mark/cc-lb-mark-on-{dark,light}.svg` |
| Mark under 24px / favicons | `mark/cc-lb-mark-16-on-{dark,light}.svg` (geometry snapped to a 16-unit grid) |
| Favicon set | `favicon/favicon.svg` (adapts to `prefers-color-scheme`), `favicon/favicon.ico` (16/32/48), `favicon/apple-touch-icon.png` (180px, opaque — iOS rounds it) |
| Favicon wiring | The admin web copies of these three live in `crates/cc-lb-admin/web/public/` and are linked from `crates/cc-lb-admin/web/index.html`:<br>`<link rel="icon" href="/favicon.ico" sizes="32x32">`<br>`<link rel="icon" href="/favicon.svg" type="image/svg+xml">`<br>`<link rel="apple-touch-icon" href="/apple-touch-icon.png">` |
| App icon / PWA | `app-icon/cc-lb-app-icon-{dark,light}.svg`, `app-icon/cc-lb-app-icon-maskable.svg` (mark inside the maskable safe zone); PNG renders in `app-icon/png/` (16–512px, maskable at 192/512) |
| Social preview / `og:image` | `social/cc-lb-social-preview.svg`, `social/cc-lb-social-preview.png` (1280×640) |
| In-app inline mark | `mark/cc-lb-mark-currentcolor.svg` — fills use `currentColor`, so it inherits `color` from its context. In the admin web, use `BrandMark` (`src/components/layout/Sidebar.tsx`) instead: brackets fill `var(--color-text)`, the lane fills `var(--color-accent)` |
| Print / single-colour | `mark/cc-lb-mark-mono-black.svg`, `mark/cc-lb-mark-mono-white.svg` |

All SVGs are self-contained: explicit hex colours, no CSS `var()`, no live
`<text>` (glyphs are outlined paths), no external references.

## Palette (Graphite)

| Role | Night (dark) | Day (light) |
|---|---|---|
| Background | `#161719` | `#e3e5e7` |
| Panel | `#1c1e20` | `#eeeff1` |
| Border | `#2f3033` | `#cacccf` |
| Ink (brackets) | `#edeeee` | `#18191c` |
| Accent (lane) | `#ccddee` | `#1f2c3d` |
| Mid | `#6a6c6f` | `#7e8084` |
| Muted | `#c0c1c3` | `#484a4e` |

Mono variants use `#000000` / `#ffffff`. In product UI the mark always follows
the theme tokens: brackets in text ink, the lane in accent — never hard-code
hex inside the app.

## Rules

- **Clear space:** keep at least 25% of the mark's rendered width empty on all
  four sides (on the 64-unit master grid, 16 units).
- **Minimum size:** 16px. Below 24px always use the 16-grid snapped variant
  (`mark/cc-lb-mark-16-*.svg`); it is what the favicon ships.
- **Flat only:** no gradients, glows, shadows, outlines or decorative noise.
  Don't rotate, skew, recolor per-context, or put the mark on a busy image;
  if a single colour is required, use the mono variants.

## Rebuilding

```sh
assets/brand/tools/build_all.sh
```

Runs the generators in order: `build_marks.py` (mark, app-icon and favicon
SVGs, PNG renders, `favicon.ico`, and copies into `crates/cc-lb-admin/web/public/`)
then `build_compositions.py` (lockups, README hero, social preview). The
generators are Python 3 stdlib only; PNG rendering goes through
`tools/render_png.mjs` with playwright's headless Chromium from
`crates/cc-lb-admin/web/node_modules` (install the web package deps first if
needed). `favicon.ico` is packed from PNGs by `tools/build_ico.py`.
