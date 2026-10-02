# cc-lb documentation site

This site uses Astro, Starlight, and Bun. It serves the public landing page at `/` and the curated documentation tree at `/docs/`.

## Commands

```bash
bun install --frozen-lockfile
bun run dev
bun run build
bun run preview
```

`bun run build` first runs `scripts/sync-brand.ts` and `scripts/sync-docs.ts`. Brand sync copies fixed assets from `assets/brand/` into the public output and Starlight source assets. Docs sync copies only the selected canonical source sections into `src/content/docs/reference/source/`, adds Starlight frontmatter, rewrites repository links, and rejects blocked terms or local paths.

The GitHub Pages workflow is `.github/workflows/site-pages.yml`. Pages and DNS settings are managed separately from this worktree.
