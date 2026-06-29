# Real Client Versions

Pinned versions are installed by `tests/real-client/install.sh`. The script prefers `bun add` when available and falls back to `npm install` (both run with `cwd = target/test-bins/<client>`), so the resulting `node_modules/.bin/<binary>` layout is identical either way.

| Client | npm package | Binary | Pinned version | Confirmation command | Output |
| --- | --- | --- | --- | --- | --- |
| `claude-code` | `@anthropic-ai/claude-code` | `claude` | `2.1.195` | `npm view @anthropic-ai/claude-code version` | `2.1.195` |
| `opencode` | `opencode-ai` | `opencode` | `1.17.11` | `npm view opencode-ai version` | `1.17.11` |
| `pi` | `@earendil-works/pi-coding-agent` | `pi` | `0.80.2` | `npm view @earendil-works/pi-coding-agent version` | `0.80.2` |

`@earendil-works/pi-ai` was also publicly resolvable at `0.80.2`, but the executable client harness pins `@earendil-works/pi-coding-agent` because it exposes the `pi` binary.
