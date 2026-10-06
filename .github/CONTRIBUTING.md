# Contributing to cc-lb

Contributions to the Rust proxy, operator documentation, documentation site, Helm chart, and Wasmtime plugin crates are welcome. Keep changes focused and describe the user-visible behavior they affect.

To run cc-lb rather than change it, follow the [documentation site](https://cc-lb.bhyoo.com/docs/getting-started/) or the README quick start. The commands below are for development.

## Development setup

- Use Rust 1.98.1 from `rust-toolchain.toml`.
- Install Bun 1.3.14 or newer; the workspace build compiles the admin SPA.
- Install the Wasm fixture target:

  ```bash
  rustup target add wasm32-unknown-unknown
  ```

- Build the workspace with `cargo build --workspace`.
- For a server-only build without Wasm fixture compilation, set `CC_LB_SKIP_WASM_FIXTURE_BUILD=1`; the admin SPA still requires Bun unless a prebuilt SPA is supplied.
- For plugin work, use the `wasm32-unknown-unknown` target and start with [the plugin author guide](../docs/plugin-author-guide.md).
- Read [runtime management](../docs/runtime-management.md) before changing operator-facing API or configuration documentation.
- For documentation site changes, run `bun install --frozen-lockfile` and `bun run build` in `site/`. The build syncs repository docs and brand assets into the site before Astro renders it.

Run the narrowest relevant checks for your change and report the commands and results in the pull request. Do not include credentials, production data, or generated session artifacts.

[`AGENTS.md`](../AGENTS.md) records repository rules that apply to every change, including the authentication ordering in the request path, the SQLite storage backend, CI failure handling, and the JSON library policy.

## Documentation and public claims

Public wording must be English, direct, and evidence-backed. Use [`positioning.yml`](../positioning.yml) as the positioning source and [`PRODUCT.md`](../PRODUCT.md) for product context. The supported upstream kinds are Anthropic API-key and Anthropic OAuth; do not add claims for providers or guarantees that are not present in the source evidence.

When a change alters behavior, configuration, or a supported claim, update the code, the repository docs, the documentation site, and the registry and repository listings together so they describe the same contract.

## Pull requests

A pull request should include:

- a concise problem statement and scope;
- the relevant tests, checks, or smoke scenario and their results;
- documentation updates when behavior or an operator contract changes; and
- any release or infrastructure dependency that remains.

The pull request template contains a short checklist for these items.

For release changes, follow [the server release runbook](../docs/runbook/server-release.md). Do not publish packages, images, or charts from a development branch.
