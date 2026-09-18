---
name: freshen-deps
description: Project-wide dependency and tool version freshness sweep for the cc-lb repo. Use whenever the user asks to bump, refresh, upgrade, modernize, or "make everything latest" — across Rust toolchain, Cargo workspace deps, web (bun) deps, GitHub Actions, Docker base images, external CLI tools pinned in scripts, and any version reference embedded in CI or Docker. Slash invocation is `/freshen-deps`. Natural-language triggers include "bump everything", "update all dependencies", "make all deps latest", "전부 최신화", "버저닝 다 올려", "version bump 전수검사", any variation of "is there anything else outdated?", and follow-up pushback like "did you really get everything?" or "/freshen-deps". Even if the user only names one tool (e.g., "bump Rust"), use this skill — partial bumps usually expose adjacent stale pins that should also move. Do NOT use for adding new dependencies, removing dependencies, or migrating to a different package (those are separate refactors).
---

# Freshen all versionable dependencies (cc-lb)

End-to-end sweep that drives every versioned external surface in this repo to its current latest stable, with PR + CI watch, stopping before merge.

## When to use this skill

Activate on any request to "bump", "refresh", "upgrade everything", "최신화", or similar phrasing — including narrow framings ("bump Rust") because a real freshness pass always finds adjacent stale pins. Activate again on follow-up pushback ("really? all of them?") — that pushback is the user telling you the first sweep missed surfaces.

## Core principle: methodology beats the checklist

This skill ships with a snapshot of cc-lb's known versioned surfaces (see `## Surface checklist`). Treat it as a starting hint, **not** a finishing line.

The cc-lb repo evolves: workflows get added, new Dockerfiles appear, scripts start pinning external tools, vendored deps come and go. Any checklist you can write down today will be incomplete tomorrow. So the methodology — "enumerate every file that can carry a version string and audit it" — is the contract; the checklist is just a memory aid.

The user has caught missed surfaces three separate times in past runs (`tests/real-client/install.sh`, stale yml comments, freshly-released `tower-http 0.7.0`). Plan for at least one full re-sweep after the first pass, and stay receptive to "is that everything?" prompts.

## Workflow

### Phase 1: Discover every versionable surface

Run all of these (use the `command` prefix in bash to bypass aliases per AGENTS.md):

```bash
command find . -name "Cargo.toml" -not -path "*/target/*" -not -path "*/.opencode/*"
command find . -name "package.json" -not -path "*/node_modules/*" -not -path "*/.opencode/*"
command find . -iname "Dockerfile*" -not -path "*/target/*"
command find . -name "*.sh" -not -path "*/target/*" -not -path "*/.opencode/*" -not -path "*/.git/*"
command find . -name "*.toml" -not -name "Cargo.toml" -not -path "*/target/*"
command find .github -type f
```

Then grep every found file for version-shaped strings:

```bash
command grep -rEn 'uses:\s*\S+@v?[0-9]' .github/
command grep -rEn '^\s*(image|FROM|ARG)\s*[:=]' .github/ compose.yaml
command grep -rEn '(bun-version|node-version|RUSTUP_TOOLCHAIN|rust-musl-cross|messense/|falcondev|ghcr\.io)' .github/ compose.yaml docs/
command grep -rEn '\b[0-9]+\.[0-9]+\.[0-9]+\b' tests/ scripts/ docs/ examples/ | command grep -v target/
command grep -rEn 'install_client|npm install|bun add|pip install|cargo install' tests/ scripts/
```

Anything that contains a version-looking string is a candidate. Compare against the surface checklist below; any *new* surface not in the checklist gets flagged in your PR body so future runs know to look for it.

### Phase 2: Look up latest stable upstream

Use these APIs directly — no extra tools to install. Pipe through `python3` for JSON:

**Rust toolchain**
```bash
curl -sSL https://static.rust-lang.org/dist/channel-rust-stable.toml | grep -A1 -E '\[pkg\.(rust|rustc|cargo)\]'
```
`[pkg.rust] version` is the canonical latest stable rustc.

**Cargo crates**
```bash
curl -sSL "https://crates.io/api/v1/crates/<name>" -H "User-Agent: check"
```
`crate.max_stable_version` is the latest stable. **Sanity-check** before bumping a major version: compare `downloads` of the new version vs the old. A "latest" with 60K downloads vs an "old" with 33M is suspect — see Anti-patterns / bincode 3.0.0.

**npm packages**
```bash
curl -sSL "https://registry.npmjs.org/<package>"
```
`dist-tags.latest` is the answer. Scoped names (`@anthropic-ai/claude-code`) work as-is.

**GitHub Actions**
```bash
curl -sSL "https://api.github.com/repos/<owner>/<repo>/releases/latest"
```
`tag_name` is the latest release. cc-lb policy is to use major-pin tags (`@v6`) which auto-track latest patch — so the action is "already at latest" if `cur_major == latest_major`.

**Docker images**
```bash
curl -sSL "https://hub.docker.com/v2/repositories/library/<image>/tags?page_size=20"
curl -sSL "https://hub.docker.com/v2/repositories/<org>/<image>/tags?page_size=20"
```
cc-lb uses rolling major tags (`postgres:18`, `messense/rust-musl-cross:aarch64-musl`) — already-latest if the tag is being rebuilt recently. Confirm via `last_updated`.

**Bun**
```bash
npm view bun version
```

**Node**
```bash
curl -sSL https://nodejs.org/dist/index.json
```
Filter by `lts: "Krypton"` (or whichever LTS line is current). cc-lb uses `node-version: "24"` which is rolling LTS 24.x.

### Phase 3: Classify and apply

Three buckets:

| Bucket | Action |
|---|---|
| Workspace dep already on `^semver` of latest | `cargo update --workspace --recursive --verbose`, done |
| Workspace dep needs major bump (0.x → 0.y or 1 → 2) | Edit `Cargo.toml` workspace.dependencies, then `cargo update -p <name>` |
| Transitive dep behind latest but blocked by an upstream constraint | Try `cargo update -p <name>@<old> --precise <new>`. If it fails, that's a real upstream block — diagnose per Phase 4. |

Web:
1. Edit `crates/cc-lb-admin/web/package.json` literal version values. Preserve the existing pin style of each entry (`^x.y.z`, `~x.y.z`, exact `x.y.z`).
2. `bun install` from the web dir to refresh `bun.lock`.
3. Sanity: `bun outdated` should print nothing.

Workflow YAML tool pins (literal strings):
1. `RUSTUP_TOOLCHAIN: 1.X.Y` in `musl-static.yml`.
2. `bun-version: x.y.z` in `web.yml`.
3. `node-version: "NN"` in `real-client-e2e.yml`.
4. **Comments that mention the old version** — also update. Drift between comments and code burns the next reader. The musl-static.yml comment about "messense image ships rustc X.Y.Z" must move when the image's actual ship version moves, separately from the workspace pin.

External CLI tool pins in scripts:
1. `tests/real-client/install.sh` pins three npm CLIs by exact version.
2. `tests/real-client/VERSIONS.md` mirrors the same table — update **both**.

The plugin-trio (`cc-lb-pdk`, `cc-lb-plugin-wire`, `cc-lb-plugin-api`) versions in workspace.dependencies are **internal lockstep pins**, not external versions — never bump these as part of a freshness sweep; they move via `release-plz`.

### Phase 4: Diagnose upstream blocks honestly

When `cargo update` reports "Unchanged X v.. (available: v..)" and a forced `--precise` fails:

```bash
cargo update -p <dep>@<old> --precise <new> 2>&1
```
The error message names the upstream pinner. Then:

```bash
cargo tree --invert <dep>@<ver>
```
gives the dependency chain. Check the upstream crate's latest stable on crates.io:
- If a newer upstream release exists that relaxes the pin → bump the upstream too (chain bump).
- If the upstream's latest stable still has the pin → genuine block. Move on.

Always cross-reference `deny.toml` `[advisories] ignore` — the cc-lb team already records known unfixable upstream blocks (`rustls-pemfile` via `rustls`, `bincode` 2.x unmaintained status). These are not "I missed it" — they are tracked and gated. Quote the deny.toml `reason` field in your PR body.

Known durable blocks (snapshot — re-verify each run):
- `bincode 2.0.1 → 3.0.0` — 3.0.0 is an [xkcd 2347](https://xkcd.com/2347/) joke release containing `compile_error!`. Real latest stable production is 2.0.1. Confirmed both by attempting build (immediate compile error) and by deny.toml note ("upstream team ceased development permanently").
- `generic-array 0.14.x → 0.14.9` — pinned via `chacha20poly1305 0.10.x → aead 0.5.x → crypto-common 0.1.x`. Unblocks when `chacha20poly1305 0.11` stable lands (currently `-rc.*` / `-pre.*`).
- `matchit 0.8.4 → 0.8.6` — exact-pinned by `axum 0.8.9`. Unblocks when axum cuts a release that relaxes the pin.

If any of those clear up between runs (newer chacha20poly1305 / axum / bincode replacement), bump them and remove the corresponding entry from deny.toml `ignore`.

### Phase 5: Verify locally (light)

CI runs the full matrix (`cargo nextest`, `cargo llvm-cov`, `musl-static verify`, real-client E2E, bun vitest). Locally just confirm nothing is structurally broken before pushing:

```bash
export CC_LB_ADMIN_SKIP_SPA=1
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
```

For any workspace dep that crossed a major boundary (e.g., `tower-http 0.6 → 0.7`), additionally:
```bash
cargo build -p <crate-that-imports-it>
```

Web (fast, locally meaningful — biome + tsgo + vite all run in seconds):
```bash
cd crates/cc-lb-admin/web
bun install
bun run lint
bun run typecheck
bun run build
```

Skip `cargo nextest`, `cargo llvm-cov`, real-client E2E locally. They're slow and CI is authoritative.

### Phase 6: PR + CI watch (stop before merge)

Branch:
```bash
git checkout -b chore/upgrade-toolchain-and-deps  # or chore/freshen-deps-YYYY-MM-DD
git add <only the files you changed; ignore untracked conformance-plugin-* Cargo.lock artifacts>
```

Commit (plain body — per AGENTS.md, **never** add Sisyphus / Co-authored-by trailers):
```
chore: bump toolchain and dependencies to latest

Tooling
- Rust X.Y.Z -> A.B.C (rust-toolchain.toml, Cargo.toml rust-version, musl-static.yml RUSTUP_TOOLCHAIN)

Cargo
- cargo update --workspace --recursive applied
- <any force-bumped transitive deps>
- <any major-version workspace dep bumps>

Web (bun)
- <name> X.Y.Z -> A.B.C
...

Real-client E2E pins
- <name> X.Y.Z -> A.B.C
...

Blocked upstream (no actionable fix)
- <name>: <root cause + upstream evidence>

Verification (local)
- cargo fmt --check: pass
- cargo clippy --workspace --all-targets -- -D warnings: pass
- bun run lint/typecheck/build: pass
```

Push + PR:
```bash
git push -u origin <branch>
gh pr create --base master --head <branch> --title "chore: bump toolchain and dependencies to latest" --body "$(cat <<'EOF'
<markdown body — no column limit per AGENTS.md, write naturally; GitHub wraps>
EOF
)"
```

PR body sections to include (this is what the cc-lb maintainer expects):
1. **Tooling / Cargo / Web / Real-client E2E pins** — every bump with `from -> to`.
2. **Already at latest** — what you checked and confirmed is current. Saves the reviewer asking.
3. **Blocked upstream** — table with `Dep | Block | Evidence`, citing deny.toml `reason` where applicable.
4. **Verification (local)** — what you ran and the result.

Watch CI (per AGENTS.md PR completion contract):
```bash
gh pr checks <num> --watch --interval 30
```
or poll:
```bash
gh pr view <num> --json statusCheckRollup,mergeStateStatus
```

The cc-lb self-hosted runner (`cc-lb`) can be backed up by concurrent master pushes. Be patient — checks often sit `QUEUED` for minutes. As long as `failed=0` and progress is happening, keep watching.

If any required check fails: open `gh run view <id> --log-failed`, fix the root cause, push, resume watch. Do NOT report "done" while anything is red.

**Stop here.** Do NOT merge unless the user explicitly says so. When `mergeStateStatus: CLEAN` and `mergeable: MERGEABLE`, report the PR URL + check summary and wait.

If the user does say "merge it":
```bash
gh pr merge <num> --squash --delete-branch
```
The `failed to run git: ... worktree at ...` warning after merge is benign — it's just gh trying to switch local HEAD to master, which is checked out in a different worktree. The merge on GitHub succeeded; verify via `gh pr view <num> --json state,mergedAt`.

## Surface checklist (project snapshot — verify and extend each run)

Treat this as a hint, not a contract. Re-discover every run per Phase 1.

### Toolchain pins
- `rust-toolchain.toml` — `[toolchain] channel`
- `Cargo.toml` — `workspace.package.rust-version`
- `.github/workflows/musl-static.yml` — `env: RUSTUP_TOOLCHAIN` **and** the comment block above the install step that names the messense image's shipped rustc + the channel pin

### Cargo
- `Cargo.toml` — `workspace.dependencies` (the big block)
- All per-crate `Cargo.toml` under `crates/`, `tests/`, `tests/fixtures/`, `plugins/`, `benches/`, `fuzz/`
- Workspace-excluded Cargo.tomls each have their own `Cargo.lock` and need `cargo update` run from inside the crate dir:
  - `fuzz/`
  - `crates/cc-lb-plugin-conformance/tests/fixtures/conformance-plugin-observe/`
  - `crates/cc-lb-plugin-conformance/tests/fixtures/conformance-plugin-router/`
  - `crates/cc-lb-plugin-conformance/tests/fixtures/conformance-plugin-shape/`
- `deny.toml` — `[advisories] ignore` cross-reference (don't claim a wasmtime/rustls-pemfile/bincode "miss" if it's already documented)

### Web (bun)
- `crates/cc-lb-admin/web/package.json` — `dependencies` + `devDependencies`
- `crates/cc-lb-admin/web/bun.lock` — refreshed by `bun install`

### CI / Docker
- `.github/workflows/*.yml` — `uses: <action>@v<major>`, `image:`, `bun-version`, `node-version`, `RUSTUP_TOOLCHAIN`, and any version-shaped string in comments
- `.github/actions/setup-rust-cache/action.yml` — composite action's own `uses:` pins
- `.github/runner-image/Dockerfile` — `ARG BASE_IMAGE`, `ARG BASE_TAG`
- `compose.yaml` — `image: postgres:NN-alpine`

### External CLI pins (npm via shell)
- `tests/real-client/install.sh` — three `install_client <name> <pkg> <ver> ...` lines
- `tests/real-client/VERSIONS.md` — mirror table; the Pinned version column + Output column both need bumping

### Explicitly not in scope
- `.opencode/package.json` — gitignored, local-only opencode runtime
- `docs/cc-lb-guide.html`, `docs/plugin-author-guide.md` — example command lines, not version pins
- `release-plz.toml`, `.cargo/config.toml`, `.cargo/llvm-cov.toml` — no version strings; behavioral config only

## Why this skill exists (why naive sweeps fail)

The first pass on a freshness sweep is reliably incomplete because:

1. **Surfaces hide.** `tests/real-client/install.sh` pinning npm CLIs looks like a test script, not a version manifest. `tests/real-client/VERSIONS.md` looks like documentation, not a versioned artifact. Only enumerating every file with a version-shaped string catches these.

2. **Comments drift.** A comment explaining "image ships rustc X.Y" must move when the image and the workspace pin both move, but `grep` for `RUSTUP_TOOLCHAIN: ` won't find it. Re-grep for the old version numbers as a final sweep.

3. **Upstream lies.** crates.io's `max_stable_version` is whatever the upstream published — `bincode 3.0.0` is a joke release, but the API doesn't say that. Sanity-check by download count and by attempting a real build before declaring "latest".

4. **The registry moves while you work.** `tower-http 0.7.0` was released mid-session in a prior run. A 30-minute sweep with a 2-hour CI cycle can race a new release; re-query latest right before pushing.

5. **Pushback is signal.** When the user asks "is that really all?" they have specific evidence that something was missed. Treat it as a hint to widen the net (different file extensions, different surfaces, deeper grep patterns), not a request to repeat the same query harder.

## Anti-patterns

- **Skipping the re-sweep.** "I checked everything once" is the failure mode that triggers the user's "really?" follow-up. Always plan two passes minimum, and especially after any user pushback.
- **Trusting `cargo outdated` / `bun outdated` exclusively.** They only see direct deps already in the manifest. They don't catch yml comments, shell scripts, or doc tables.
- **Silent stale.** If a version reference is stale and you can't bump it (upstream block, explicit pin policy), document it in the PR body. Never leave a stale comment without an explanation.
- **Major bump without build verify.** Even `cargo check` is enough — but verify before pushing. Saved this skill from shipping `tower-http 0.7` and `bincode 3.0.0` blind.
- **Adding `--no-verify` / `--force` to push past CI failures.** Per AGENTS.md, never. Diagnose and fix the root cause.
- **Sisyphus footer / Co-authored-by on commits.** Per AGENTS.md, plain commit bodies only. Applies to rebases / squashes / amends too.
- **Auto-merging.** Even when CLEAN, wait for the user. AGENTS.md is strict: "Do not merge the PR unless the user explicitly asked for merging."
- **Adding line breaks at 80 columns in the PR body.** AGENTS.md: PR title/body have no column limit; renderers wrap. Forced breaks only between semantic paragraphs.
